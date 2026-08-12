"""Local-only CLI and hardened Unix-socket server bootstrap for the worker."""

from __future__ import annotations

import argparse
import errno
import os
import secrets
import signal
import socket
import stat
import threading
from collections.abc import Sequence
from pathlib import Path
from types import FrameType
from typing import cast

import uvicorn

from seo_orchestrator.api.app import create_app
from seo_orchestrator.api.auth import load_api_token, load_hmac_key
from seo_orchestrator.db.connection import connect
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.settings import Settings

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
    if expected_identity is not None and (
        metadata.st_dev,
        metadata.st_ino,
    ) != expected_identity:
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
            raise SocketPathError(
                "worker socket path does not reference the bound listener"
            )
    except SocketPathError:
        raise
    except OSError as exc:
        raise SocketPathError(
            "worker socket path does not reference the bound listener"
        ) from exc
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


def prepare_unix_socket(
    socket_path: Path, *, owner_uid: int, mode: int
) -> _OwnedUnixListener:
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
            if current is not None and (
                current.st_dev,
                current.st_ino,
            ) != candidate_identity:
                raise SocketPathError(
                    "worker socket target changed before removal"
                ) from exc
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


def doctor(settings: Settings, *, owner_uid: int = _WORKER_UID) -> None:
    """Check local worker configuration and protected key readability without disclosure."""
    load_api_token(settings.api_token_path)
    load_hmac_key(settings.callback_hmac_key_path)
    _validate_socket_parent(settings.socket_path, owner_uid)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="seo-orchestrator")
    subcommands = parser.add_subparsers(dest="command", required=True)
    for command in ("migrate", "serve", "worker", "doctor"):
        subcommands.add_parser(command)
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
    raise SystemExit("worker runner is unavailable until Task 10")
