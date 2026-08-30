"""Local-only CLI and hardened Unix-socket server bootstrap for the worker."""

from __future__ import annotations

import argparse
import errno
import json
import os
import secrets
import signal
import socket
import stat
import threading
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import cast

import uvicorn

from seo_orchestrator.api.app import create_app
from seo_orchestrator.api.auth import load_api_token, load_hmac_key
from seo_orchestrator.canonical import (
    MAX_CANONICAL_BYTES,
    JsonValue,
    canonical_json,
)
from seo_orchestrator.db.connection import connect, require_unaliased_absolute_path
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.executors.base import Executor
from seo_orchestrator.executors.staged_mock import StagedMockExecutor
from seo_orchestrator.runner import Runner
from seo_orchestrator.services.artifacts import ArtifactStore
from seo_orchestrator.services.jobs import JobService
from seo_orchestrator.services.supervised_subscription import (
    authoritative_supervised_state_path,
    prepare_supervised_packet,
)
from seo_orchestrator.settings import Settings
from seo_orchestrator.supervised_rail import (
    ObservedCompletion,
    OperatorAttestation,
    StagePacket,
    SupervisedRail,
    SupervisedStatus,
    packet_identity_mapping,
)

_WORKER_UID = 10000
_SOCKET_BIND_UMASK_LOCK = threading.Lock()


class _SocketCleanupSignal(SystemExit):
    """Unwind Uvicorn's post-shutdown SIGTERM through socket cleanup."""


def _unwind_after_sigterm(_signum: int, _frame: FrameType | None) -> None:
    raise _SocketCleanupSignal()


class SocketPathError(RuntimeError):
    """Raised when a configured Unix socket target cannot be handled safely."""


class _OwnedUnixListener(socket.socket):
    """Unix listener carrying the exact filesystem identity created by bind."""

    path_identity: tuple[int, int] | None


def _validate_socket_mode(mode: object) -> int:
    if (
        type(mode) is not int
        or not 0 <= mode <= 0o777
        or mode & 0o700 != 0o600
        or mode & 0o111
        or mode & 0o007
    ):
        raise SocketPathError("worker socket mode is unsafe")
    return mode


def _validate_socket_parent(socket_path: Path, owner_uid: int) -> None:
    try:
        metadata = os.lstat(socket_path.parent)
    except OSError as exc:
        raise SocketPathError("worker socket parent is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != owner_uid
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise SocketPathError("worker socket parent is unsafe")


def _owned_socket_metadata(socket_path: Path, owner_uid: int) -> os.stat_result | None:
    try:
        metadata = os.lstat(socket_path)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise SocketPathError("worker socket target cannot be inspected") from exc
    if not stat.S_ISSOCK(metadata.st_mode):
        raise SocketPathError("worker socket target is not a socket")
    if metadata.st_uid != owner_uid:
        raise SocketPathError("worker socket target has an unexpected owner")
    return metadata


def _remove_owned_socket(
    socket_path: Path,
    owner_uid: int,
    *,
    expected_identity: tuple[int, int] | None = None,
) -> None:
    metadata = _owned_socket_metadata(socket_path, owner_uid)
    if metadata is None:
        return
    if (
        expected_identity is not None
        and (
            metadata.st_dev,
            metadata.st_ino,
        )
        != expected_identity
    ):
        raise SocketPathError("worker socket target changed before removal")
    if expected_identity is not None and metadata.st_nlink != 1:
        raise SocketPathError("worker socket target has unexpected aliases")
    try:
        os.unlink(socket_path)
    except OSError as exc:
        raise SocketPathError("worker socket target cannot be removed") from exc


def _remove_owned_stale_socket(socket_path: Path, owner_uid: int) -> None:
    metadata = _owned_socket_metadata(socket_path, owner_uid)
    if metadata is None:
        return
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.settimeout(1)
        result = probe.connect_ex(str(socket_path))
    except OSError as exc:
        raise SocketPathError("worker socket activity cannot be determined") from exc
    finally:
        probe.close()
    if result == 0:
        raise SocketPathError("worker socket is already active")
    if result == errno.ENOENT:
        return
    if result != errno.ECONNREFUSED:
        raise SocketPathError("worker socket activity cannot be determined")
    _remove_owned_socket(
        socket_path,
        owner_uid,
        expected_identity=(metadata.st_dev, metadata.st_ino),
    )


def _verify_socket_path_references_listener(
    listener: socket.socket,
    socket_path: Path,
) -> None:
    """Prove a pathname-routed connection reaches this exact listener."""
    challenge = secrets.token_bytes(32)
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    accepted: socket.socket | None = None
    previous_timeout = listener.gettimeout()
    try:
        listener.settimeout(1)
        probe.settimeout(1)
        probe.connect(str(socket_path))
        probe.sendall(challenge)
        accepted, _address = listener.accept()
        accepted.settimeout(1)
        received = bytearray()
        while len(received) < len(challenge):
            chunk = accepted.recv(len(challenge) - len(received))
            if not chunk:
                break
            received.extend(chunk)
        if not secrets.compare_digest(received, challenge):
            raise SocketPathError("worker socket path does not reference the bound listener")
    except SocketPathError:
        raise
    except OSError as exc:
        raise SocketPathError("worker socket path does not reference the bound listener") from exc
    finally:
        if accepted is not None:
            accepted.close()
        probe.close()
        listener.settimeout(previous_timeout)


def _bind_unix_socket_with_mode(
    listener: socket.socket,
    socket_path: Path,
    mode: int,
) -> None:
    """Create the socket pathname with its final mode as part of bind."""
    creation_umask = 0o777 & ~mode
    with _SOCKET_BIND_UMASK_LOCK:
        previous_umask = os.umask(creation_umask)
        try:
            listener.bind(str(socket_path))
        finally:
            os.umask(previous_umask)


def prepare_unix_socket(socket_path: Path, *, owner_uid: int, mode: int) -> _OwnedUnixListener:
    """Bind a fresh AF_UNIX listener without replacing arbitrary filesystem objects."""
    if not isinstance(socket_path, Path) or not socket_path.is_absolute():
        raise SocketPathError("worker socket path must be absolute")
    if type(owner_uid) is not int or owner_uid < 0:
        raise SocketPathError("worker socket owner is invalid")
    mode = _validate_socket_mode(mode)
    _validate_socket_parent(socket_path, owner_uid)
    _remove_owned_stale_socket(socket_path, owner_uid)

    listener = _OwnedUnixListener(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.path_identity = None
    bound = False
    candidate_identity: tuple[int, int] | None = None
    try:
        _bind_unix_socket_with_mode(listener, socket_path, mode)
        bound = True
        metadata = os.lstat(socket_path)
        candidate_identity = (metadata.st_dev, metadata.st_ino)
        if (
            not stat.S_ISSOCK(metadata.st_mode)
            or metadata.st_uid != owner_uid
            or stat.S_IMODE(metadata.st_mode) != mode
            or metadata.st_nlink != 1
            or (metadata.st_dev, metadata.st_ino) != candidate_identity
        ):
            raise SocketPathError("worker socket post-bind validation failed")
        listener.listen(socket.SOMAXCONN)
        _verify_socket_path_references_listener(listener, socket_path)
        metadata = os.lstat(socket_path)
        if (
            not stat.S_ISSOCK(metadata.st_mode)
            or metadata.st_uid != owner_uid
            or stat.S_IMODE(metadata.st_mode) != mode
            or metadata.st_nlink != 1
            or (metadata.st_dev, metadata.st_ino) != candidate_identity
        ):
            raise SocketPathError("worker socket post-bind validation failed")
        listener.path_identity = candidate_identity
        return listener
    except BaseException as exc:
        path_identity = listener.path_identity
        listener.close()
        if bound and path_identity is not None:
            _remove_owned_socket(
                socket_path,
                owner_uid,
                expected_identity=path_identity,
            )
        elif bound and candidate_identity is not None:
            current = _owned_socket_metadata(socket_path, owner_uid)
            if (
                current is not None
                and (
                    current.st_dev,
                    current.st_ino,
                )
                != candidate_identity
            ):
                raise SocketPathError("worker socket target changed before removal") from exc
        raise


def serve_worker(settings: Settings, *, owner_uid: int = _WORKER_UID) -> None:
    """Serve ASGI only through a checked pre-bound Unix domain socket FD."""
    socket_path = settings.socket_path
    listener: _OwnedUnixListener | None = None
    previous_sigterm_handler = None
    previous_sigterm_mask: set[signal.Signals] | None = None
    try:
        if threading.current_thread() is threading.main_thread():
            previous_sigterm_mask = cast(
                set[signal.Signals],
                signal.pthread_sigmask(
                    signal.SIG_BLOCK,
                    {signal.SIGTERM},
                ),
            )
            previous_sigterm_handler = signal.signal(
                signal.SIGTERM,
                _unwind_after_sigterm,
            )
        try:
            listener = prepare_unix_socket(
                socket_path,
                owner_uid=owner_uid,
                mode=settings.worker_socket_mode,
            )
            if previous_sigterm_mask is not None:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_sigterm_mask)
                previous_sigterm_mask = None
            # Passing the FD preserves our mode; Uvicorn's uds= path would reset it.
            uvicorn.run(create_app(settings), fd=listener.fileno(), access_log=False)
        except _SocketCleanupSignal:
            pass
    finally:
        try:
            if listener is not None:
                path_identity = listener.path_identity
                listener.close()
                if path_identity is None:
                    raise SocketPathError("worker socket identity is unavailable")
                _remove_owned_socket(
                    socket_path,
                    owner_uid,
                    expected_identity=path_identity,
                )
        finally:
            if previous_sigterm_handler is not None:
                signal.signal(signal.SIGTERM, previous_sigterm_handler)
            if previous_sigterm_mask is not None:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_sigterm_mask)


def run_migrations(settings: Settings) -> None:
    """Apply durable SQLite migrations through the explicit configured database path."""
    connection = connect(settings.db_path)
    try:
        migrate(connection)
    finally:
        connection.close()


def run_worker(
    settings: Settings,
    *,
    executor: Executor,
    poll_interval_seconds: float = 1.0,
    runner_id: str | None = None,
) -> None:
    """Run bounded ticks until SIGTERM, finishing the active tick before exit."""
    if type(poll_interval_seconds) not in {int, float} or poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be positive")
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("worker loop must run in the main thread")
    selected_runner_id = runner_id or f"runner-{secrets.token_hex(16)}"
    if type(selected_runner_id) is not str or not selected_runner_id.strip():
        raise ValueError("runner_id must be a non-empty string")
    stop_requested = threading.Event()

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        stop_requested.set()

    previous_sigterm_handler = signal.signal(signal.SIGTERM, request_stop)
    connection = None
    try:
        connection = connect(settings.db_path)
        migrate(connection)
        runner = Runner(
            connection,
            executor=executor,
            artifact_store=ArtifactStore(settings.artifact_root),
            runner_id=selected_runner_id,
            lease_token_factory=lambda: secrets.token_hex(32),
        )
        while not stop_requested.is_set():
            runner.tick(limit=1)
            if not stop_requested.is_set():
                stop_requested.wait(float(poll_interval_seconds))
    finally:
        try:
            if connection is not None:
                connection.close()
        finally:
            signal.signal(signal.SIGTERM, previous_sigterm_handler)


def doctor(settings: Settings, *, owner_uid: int = _WORKER_UID) -> None:
    """Check local worker configuration and protected key readability without disclosure."""
    load_api_token(settings.api_token_path)
    load_hmac_key(settings.callback_hmac_key_path)
    _validate_socket_parent(settings.socket_path, owner_uid)


def _packet_output(packet: StagePacket) -> dict[str, JsonValue]:
    return {**packet_identity_mapping(packet), "input_hash": packet.input_hash}


def _emit_json(value: dict[str, JsonValue]) -> None:
    print(canonical_json(value).decode("utf-8"))


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("completion JSON contains a duplicate object key")
        value[key] = item
    return value


def _reject_json_constant(_value: str) -> object:
    raise ValueError("completion JSON contains a non-finite number")


def _private_completion_metadata(metadata: os.stat_result) -> bool:
    return (
        stat.S_ISREG(metadata.st_mode)
        and metadata.st_uid == os.getuid()
        and stat.S_IMODE(metadata.st_mode) == 0o600
        and metadata.st_nlink == 1
        and metadata.st_size <= MAX_CANONICAL_BYTES
    )


def _completion_metadata_signature(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_uid,
        stat.S_IMODE(metadata.st_mode),
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_private_json(path: Path) -> JsonValue:
    if (
        not path.is_absolute()
        or any(part in {".", ".."} for part in path.parts)
        or "\x00" in str(path)
    ):
        raise ValueError("completion file path must be absolute and normalized")
    try:
        require_unaliased_absolute_path(path)
    except ValueError as exc:
        raise ValueError("completion file is not a private bounded regular file") from exc
    try:
        before = os.lstat(path)
    except OSError as exc:
        raise ValueError("completion file is unavailable") from exc
    if not _private_completion_metadata(before):
        raise ValueError("completion file is not a private bounded regular file")
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ValueError("completion file cannot be opened safely") from exc
    try:
        opened = os.fstat(descriptor)
        if not _private_completion_metadata(opened) or _completion_metadata_signature(
            opened
        ) != _completion_metadata_signature(before):
            raise ValueError("completion file changed while opening")
        payload = bytearray()
        while len(payload) <= MAX_CANONICAL_BYTES:
            chunk = os.read(descriptor, min(65_536, MAX_CANONICAL_BYTES + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) > MAX_CANONICAL_BYTES:
            raise ValueError("completion file exceeds the maximum JSON size")
        after = os.fstat(descriptor)
        if not _private_completion_metadata(after) or _completion_metadata_signature(
            after
        ) != _completion_metadata_signature(opened):
            raise ValueError("completion file changed while reading")
    finally:
        os.close(descriptor)
    try:
        value = json.loads(
            payload,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
        normalized = json.loads(canonical_json(cast(JsonValue, value)))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError, RecursionError) as exc:
        raise ValueError("completion file is not valid bounded JSON") from exc
    return cast(JsonValue, normalized)


def _read_observed_completion(
    path: Path,
    *,
    attestation: OperatorAttestation,
) -> ObservedCompletion:
    value = _read_private_json(path)
    if type(value) is not dict or set(value) != {
        "company_id",
        "job_id",
        "stage_id",
        "input_hash",
        "payload",
    }:
        raise ValueError("completion file must be an exact identity-bound envelope")
    try:
        return ObservedCompletion(
            job_id=cast(str, value["job_id"]),
            company_id=cast(str, value["company_id"]),
            stage_id=cast(str, value["stage_id"]),
            input_hash=cast(str, value["input_hash"]),
            payload=value["payload"],
            attestation=attestation,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("completion file identity envelope is invalid") from exc


def _supervised_status_output(
    rail: SupervisedRail,
    *,
    company_id: str,
    job_id: str,
) -> dict[str, JsonValue]:
    status = rail.status(company_id=company_id, job_id=job_id)
    output: dict[str, JsonValue] = {"status": status.value}
    if status is SupervisedStatus.AWAITING_OPERATOR_EXECUTION:
        output["packet"] = _packet_output(
            rail.outstanding_packet(company_id=company_id, job_id=job_id)
        )
    elif status is SupervisedStatus.ARTIFACT_FROZEN:
        binding = rail.artifact_binding(company_id=company_id, job_id=job_id)
        output["artifact"] = {
            "manifest_path": binding.manifest_path,
            "manifest_hash": binding.manifest_hash,
        }
    return output


def _run_supervised_command(
    settings: Settings,
    *,
    command: str,
    arguments: argparse.Namespace,
) -> None:
    company_id = cast(str, arguments.company_id)
    job_id = cast(str, arguments.job_id)
    rail = SupervisedRail(state_path=authoritative_supervised_state_path(settings.db_path))

    if command == "supervised-status":
        _emit_json(_supervised_status_output(rail, company_id=company_id, job_id=job_id))
        return
    if command == "supervised-bind":
        completion = _read_observed_completion(
            cast(Path, arguments.completion_file),
            attestation=OperatorAttestation(
                session_ref=cast(str, arguments.session_ref),
                provider_id=cast(str, arguments.provider_id),
                model_id=cast(str, arguments.model_id),
                operator_id=cast(str, arguments.operator_id),
                observed_at=datetime.now(UTC),
            ),
        )
        if completion.company_id != company_id or completion.job_id != job_id:
            raise ValueError("completion file is outside the requested authority scope")
        if cast(bool, arguments.resolve_recovery):
            rail.resolve_recovery_for_exact_binding(
                company_id=company_id,
                job_id=job_id,
                expected_input_hash=completion.input_hash,
                operator_id=cast(str, arguments.operator_id),
            )
        outcome = rail.bind_completion(completion)
        if isinstance(outcome, StagePacket):
            _emit_json(_packet_output(outcome))
        else:
            _emit_json({"status": outcome.value})
        return
    if command != "supervised-packet":
        raise ValueError("unsupported supervised command")

    connection = connect(settings.db_path)
    try:
        packet = prepare_supervised_packet(
            rail=rail,
            job_service=JobService(connection, company_id=company_id),
            job_id=job_id,
            designated_session_ref=cast(str, arguments.session_ref),
        )
        _emit_json(_packet_output(packet))
    finally:
        connection.close()


def _supervised_parser(
    subcommands: argparse._SubParsersAction[argparse.ArgumentParser],
    command: str,
) -> argparse.ArgumentParser:
    parser = subcommands.add_parser(command)
    parser.add_argument("--company-id", required=True)
    parser.add_argument("--job-id", required=True)
    return parser


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="seo-orchestrator")
    subcommands = parser.add_subparsers(dest="command", required=True)
    for command in ("migrate", "serve", "doctor"):
        subcommands.add_parser(command)
    _supervised_parser(subcommands, "supervised-status")
    supervised_packet = _supervised_parser(subcommands, "supervised-packet")
    supervised_packet.add_argument("--session-ref", required=True)
    supervised_bind = _supervised_parser(subcommands, "supervised-bind")
    supervised_bind.add_argument("--completion-file", required=True, type=Path)
    supervised_bind.add_argument("--operator-id", required=True)
    supervised_bind.add_argument("--session-ref", required=True)
    supervised_bind.add_argument("--provider-id", required=True)
    supervised_bind.add_argument("--model-id", required=True)
    supervised_bind.add_argument("--resolve-recovery", action="store_true")
    worker = subcommands.add_parser("worker")
    worker.add_argument(
        "--mock",
        action="store_true",
        help="run the deterministic local executor (development/test only)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Run one explicit local command; never default to network or TCP serving."""
    arguments = _parser().parse_args(argv)
    command = cast(str, arguments.command)
    settings = Settings.from_env(os.environ)
    if command == "migrate":
        run_migrations(settings)
        return
    if command == "serve":
        serve_worker(settings)
        return
    if command == "doctor":
        doctor(settings)
        return
    if command in {
        "supervised-packet",
        "supervised-status",
        "supervised-bind",
    }:
        _run_supervised_command(settings, command=command, arguments=arguments)
        return
    if not cast(bool, getattr(arguments, "mock", False)):
        raise SystemExit("worker requires explicit executor selection")
    if settings.environment == "production":
        raise SystemExit("production executor is unavailable until Task 12")
    mock_state_path = settings.db_path.with_name(f"{settings.db_path.name}.mock-executor")
    run_worker(settings, executor=StagedMockExecutor(state_path=mock_state_path))
