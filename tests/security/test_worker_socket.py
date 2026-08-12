import os
import signal
import socket
import stat
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pytest

from seo_orchestrator.settings import Settings

_TOKEN_HEX = "0f" * 32
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _settings(tmp_path: Path, socket_path: Path) -> Settings:
    token_path = tmp_path / "worker-api.token"
    token_path.write_text(_TOKEN_HEX, encoding="ascii")
    token_path.chmod(0o600)
    callback_key_path = tmp_path / "n8n-callback.key"
    callback_key_path.write_text(_TOKEN_HEX, encoding="ascii")
    callback_key_path.chmod(0o600)
    return Settings(
        environment="test",
        db_path=tmp_path / "worker.db",
        artifact_root=tmp_path / "artifacts",
        listen=f"unix:{socket_path}",
        api_token_path=token_path,
        callback_hmac_key_path=callback_key_path,
    )


@contextmanager
def _short_socket_path() -> Iterator[Path]:
    with TemporaryDirectory(dir="/opt/data/cache", prefix="uds-") as directory:
        yield Path(directory) / "worker.sock"


def _stale_socket(path: Path) -> None:
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        listener.bind(str(path))
    finally:
        listener.close()


def test_prepare_unix_socket_refuses_regular_file_without_removing_it(tmp_path: Path) -> None:
    from seo_orchestrator.cli import SocketPathError, prepare_unix_socket

    path = tmp_path / "worker.sock"
    path.write_text("not a socket", encoding="utf-8")

    with pytest.raises(SocketPathError):
        prepare_unix_socket(path, owner_uid=os.geteuid(), mode=0o660)

    assert path.read_text(encoding="utf-8") == "not a socket"


def test_prepare_unix_socket_refuses_foreign_owned_socket_without_removing_it(
    tmp_path: Path,
) -> None:
    from seo_orchestrator.cli import SocketPathError, prepare_unix_socket

    with _short_socket_path() as path:
        _stale_socket(path)

        with pytest.raises(SocketPathError):
            prepare_unix_socket(path, owner_uid=os.geteuid() + 1, mode=0o660)

        assert stat.S_ISSOCK(os.lstat(path).st_mode)


def test_prepare_unix_socket_replaces_only_owned_stale_socket_and_sets_mode(tmp_path: Path) -> None:
    from seo_orchestrator.cli import prepare_unix_socket

    with _short_socket_path() as path:
        _stale_socket(path)

        listener = prepare_unix_socket(path, owner_uid=os.geteuid(), mode=0o660)
        try:
            metadata = os.lstat(path)
            assert stat.S_ISSOCK(metadata.st_mode)
            assert stat.S_IMODE(metadata.st_mode) == 0o660
            assert metadata.st_uid == os.geteuid()
        finally:
            listener.close()
            path.unlink()


def test_prepare_unix_socket_refuses_active_owned_listener_without_replacing_it() -> None:
    from seo_orchestrator.cli import SocketPathError, prepare_unix_socket

    with _short_socket_path() as path:
        active_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            active_listener.bind(str(path))
            active_listener.listen(socket.SOMAXCONN)
            original = os.lstat(path)

            with pytest.raises(SocketPathError, match="already active"):
                prepare_unix_socket(path, owner_uid=os.geteuid(), mode=0o660)

            current = os.lstat(path)
            assert (current.st_dev, current.st_ino) == (original.st_dev, original.st_ino)
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                probe.settimeout(1)
                probe.connect(str(path))
            finally:
                probe.close()
        finally:
            active_listener.close()
            path.unlink(missing_ok=True)


def test_serve_worker_rejects_hard_link_alias_replacement_before_identity_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from seo_orchestrator import cli

    replacement: socket.socket | None = None
    replacement_identity: tuple[int, int] | None = None
    owned_listener_alias: Path | None = None
    path_lstat_calls = 0
    uvicorn_called = False
    real_lstat = cli.os.lstat

    with _short_socket_path() as path:
        owned_listener_alias = path.with_name("owned-listener-alias.sock")

        def replace_before_identity_capture(
            target: os.PathLike[str] | str,
            *,
            dir_fd: int | None = None,
        ) -> os.stat_result:
            nonlocal path_lstat_calls, replacement, replacement_identity
            if dir_fd is None and os.fspath(target) == os.fspath(path):
                path_lstat_calls += 1
                if path_lstat_calls == 2:
                    os.link(path, owned_listener_alias, follow_symlinks=False)
                    path.unlink()
                    replacement = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    replacement.bind(str(path))
                    os.chmod(path, 0o660)
                    replacement.listen(socket.SOMAXCONN)
                    metadata = real_lstat(path)
                    replacement_identity = (metadata.st_dev, metadata.st_ino)
            return real_lstat(target, dir_fd=dir_fd)

        def fake_run(_app: Any, **_kwargs: Any) -> None:
            nonlocal uvicorn_called
            uvicorn_called = True

        monkeypatch.setattr(cli.os, "lstat", replace_before_identity_capture)
        monkeypatch.setattr(cli.uvicorn, "run", fake_run)

        try:
            with pytest.raises(
                cli.SocketPathError,
                match="does not reference the bound listener",
            ):
                cli.serve_worker(_settings(tmp_path, path), owner_uid=os.geteuid())

            assert not uvicorn_called
            assert replacement is not None
            assert replacement_identity is not None
            current = real_lstat(path)
            assert (current.st_dev, current.st_ino) == replacement_identity
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                probe.settimeout(1)
                probe.connect(str(path))
            finally:
                probe.close()
        finally:
            if replacement is not None:
                replacement.close()
            path.unlink(missing_ok=True)
            if owned_listener_alias is not None:
                owned_listener_alias.unlink(missing_ok=True)


def test_prepare_unix_socket_rejects_hard_link_alias_before_ownership_acceptance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from seo_orchestrator import cli

    listener: socket.socket | None = None
    with _short_socket_path() as path:
        alias = path.with_name("pre-ownership-alias.sock")
        real_verification = cli._verify_socket_path_references_listener

        def add_alias_after_verification(
            bound_listener: socket.socket,
            target: Path,
        ) -> None:
            real_verification(bound_listener, target)
            os.link(path, alias, follow_symlinks=False)

        monkeypatch.setattr(
            cli,
            "_verify_socket_path_references_listener",
            add_alias_after_verification,
        )
        try:
            with pytest.raises(cli.SocketPathError, match="post-bind validation"):
                listener = cli.prepare_unix_socket(
                    path,
                    owner_uid=os.geteuid(),
                    mode=0o660,
                )

            current = os.lstat(path)
            linked = os.lstat(alias)
            assert (current.st_dev, current.st_ino) == (
                linked.st_dev,
                linked.st_ino,
            )
            assert current.st_nlink == 2
        finally:
            if listener is not None:
                listener.close()
            path.unlink(missing_ok=True)
            alias.unlink(missing_ok=True)


def test_prepare_unix_socket_never_chmods_replacement_after_identity_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from seo_orchestrator import cli

    with _short_socket_path() as path:
        victim = path.with_name("permission-victim")
        victim.write_text("must remain private", encoding="utf-8")
        victim.chmod(0o600)
        real_lstat = cli.os.lstat
        path_lstat_calls = 0

        def replace_after_identity_check(
            target: os.PathLike[str] | str,
            *,
            dir_fd: int | None = None,
        ) -> os.stat_result:
            nonlocal path_lstat_calls

            target_path = os.fspath(target)
            if dir_fd is None and target_path == os.fspath(path):
                path_lstat_calls += 1
                metadata = real_lstat(target)
                if path_lstat_calls == 2:
                    path.unlink()
                    path.symlink_to(victim)
                return metadata
            return real_lstat(target, dir_fd=dir_fd)

        monkeypatch.setattr(cli.os, "lstat", replace_after_identity_check)
        try:
            with pytest.raises(cli.SocketPathError):
                cli.prepare_unix_socket(
                    path,
                    owner_uid=os.geteuid(),
                    mode=0o660,
                )

            assert stat.S_IMODE(os.stat(victim).st_mode) == 0o600
        finally:
            path.unlink(missing_ok=True)
            victim.unlink(missing_ok=True)


def test_serve_worker_preserves_replaced_active_socket_during_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from seo_orchestrator import cli

    replacement: socket.socket | None = None
    with _short_socket_path() as path:

        def replace_listener(_app: Any, **_kwargs: Any) -> None:
            nonlocal replacement
            path.unlink()
            replacement = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            replacement.bind(str(path))
            replacement.listen(socket.SOMAXCONN)

        monkeypatch.setattr(cli.uvicorn, "run", replace_listener)
        try:
            with pytest.raises(cli.SocketPathError, match="changed before removal"):
                cli.serve_worker(_settings(tmp_path, path), owner_uid=os.geteuid())

            assert replacement is not None
            current = os.lstat(path)
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                probe.settimeout(1)
                probe.connect(str(path))
            finally:
                probe.close()
            assert stat.S_ISSOCK(current.st_mode)
        finally:
            if replacement is not None:
                replacement.close()
            path.unlink(missing_ok=True)


def test_serve_worker_preserves_hard_link_alias_during_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from seo_orchestrator import cli

    with _short_socket_path() as path:
        alias = path.with_name("worker-renamed.sock")

        def replace_name_with_alias(_app: Any, **_kwargs: Any) -> None:
            path.rename(alias)
            os.link(alias, path, follow_symlinks=False)

        monkeypatch.setattr(cli.uvicorn, "run", replace_name_with_alias)
        try:
            with pytest.raises(cli.SocketPathError, match="unexpected aliases"):
                cli.serve_worker(_settings(tmp_path, path), owner_uid=os.geteuid())

            current = os.lstat(path)
            renamed = os.lstat(alias)
            assert (current.st_dev, current.st_ino) == (
                renamed.st_dev,
                renamed.st_ino,
            )
            assert current.st_nlink == 2
            assert renamed.st_nlink == 2
        finally:
            path.unlink(missing_ok=True)
            alias.unlink(missing_ok=True)


def test_prepare_unix_socket_preserves_replacement_during_error_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from seo_orchestrator import cli

    replacement: socket.socket | None = None
    with _short_socket_path() as path:

        def replace_before_failure(
            _listener: socket.socket,
            _socket_path: Path,
        ) -> None:
            nonlocal replacement
            path.unlink()
            replacement = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            replacement.bind(str(path))
            replacement.listen(socket.SOMAXCONN)
            raise OSError("forced verification failure")

        monkeypatch.setattr(
            cli,
            "_verify_socket_path_references_listener",
            replace_before_failure,
        )
        try:
            with pytest.raises(cli.SocketPathError, match="changed before removal"):
                cli.prepare_unix_socket(path, owner_uid=os.geteuid(), mode=0o660)

            assert replacement is not None
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                probe.settimeout(1)
                probe.connect(str(path))
            finally:
                probe.close()
        finally:
            if replacement is not None:
                replacement.close()
            path.unlink(missing_ok=True)


def test_serve_worker_passes_only_a_prebound_unix_fd_to_uvicorn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from seo_orchestrator import cli

    captured: dict[str, Any] = {}
    with _short_socket_path() as path:

        def fake_run(app: Any, **kwargs: Any) -> None:
            captured["app"] = app
            captured.update(kwargs)
            bound = socket.fromfd(kwargs["fd"], socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                assert bound.getsockname() == str(path)
            finally:
                bound.close()

        monkeypatch.setattr(cli.uvicorn, "run", fake_run)

        cli.serve_worker(_settings(tmp_path, path), owner_uid=os.geteuid())

        assert captured["fd"] >= 0
        assert "uds" not in captured
        assert "host" not in captured
        assert "port" not in captured
        assert not path.exists()


def test_sigterm_probe_does_not_embed_a_specific_worktree_path() -> None:
    forbidden_worktree = (
        f"/{'opt'}/{'data'}/{'seo-content-orchestrator'}/"
        f"{'.worktrees'}/{'stage-b-mvp'}"
    )

    assert forbidden_worktree not in Path(__file__).read_text(encoding="utf-8")


def test_serve_worker_removes_its_socket_after_sigterm(tmp_path: Path) -> None:
    with _short_socket_path() as socket_path:
        settings = _settings(tmp_path, socket_path)
        script = """
import os
import sys
from pathlib import Path

from seo_orchestrator.cli import serve_worker
from seo_orchestrator.settings import Settings

root = Path(sys.argv[1])
serve_worker(
    Settings(
        environment="test",
        db_path=root / "worker.db",
        artifact_root=root / "artifacts",
        listen=sys.argv[2],
        api_token_path=root / "worker-api.token",
        callback_hmac_key_path=root / "n8n-callback.key",
    ),
    owner_uid=os.geteuid(),
)
"""
        process = subprocess.Popen(
            [sys.executable, "-c", script, str(tmp_path), settings.listen],
            cwd=_PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            deadline = time.monotonic() + 10
            while not socket_path.exists() and time.monotonic() < deadline:
                if process.poll() is not None:
                    pytest.fail("worker exited before binding its socket")
                time.sleep(0.05)
            assert socket_path.exists()

            os.kill(process.pid, signal.SIGTERM)
            assert process.wait(timeout=10) == 0
            assert not socket_path.exists()
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
            process.communicate()
