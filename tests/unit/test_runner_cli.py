"""Fail-closed CLI selection for the Task 10 runner."""

import ast
from pathlib import Path

import pytest

from seo_orchestrator import cli
from seo_orchestrator.canonical import JsonValue, canonical_json
from seo_orchestrator.executors.staged_mock import StagedMockExecutor
from seo_orchestrator.settings import Settings


def _settings(environment: str = "development") -> Settings:
    return Settings(
        environment=environment,
        db_path=Path("/tmp/runner-cli.db"),
        artifact_root=Path("/tmp/runner-cli-artifacts"),
        listen="unix:/tmp/runner-cli.sock",
    )


def test_supervised_modules_import_no_transport_or_process_clients() -> None:
    root = Path(__file__).parents[2]
    forbidden = {"httpx", "requests", "socket", "subprocess", "urllib", "webbrowser"}
    observed: set[str] = set()
    for relative_path in (
        "src/seo_orchestrator/supervised_rail.py",
        "src/seo_orchestrator/services/supervised_subscription.py",
    ):
        tree = ast.parse((root / relative_path).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                observed.update(alias.name.split(".", maxsplit=1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                observed.add(node.module.split(".", maxsplit=1)[0])
    assert observed.isdisjoint(forbidden)


def test_completion_file_boundary_is_private_strict_and_no_follow(tmp_path: Path) -> None:
    value: JsonValue = {"outline": ["bounded"]}
    completion_path = tmp_path / "completion.json"
    completion_path.write_bytes(canonical_json(value))
    completion_path.chmod(0o600)

    assert cli._read_private_json(completion_path) == value

    completion_path.chmod(0o644)
    with pytest.raises(ValueError, match="private bounded regular file"):
        cli._read_private_json(completion_path)
    completion_path.chmod(0o600)

    symlink_path = tmp_path / "completion-link.json"
    symlink_path.symlink_to(completion_path)
    with pytest.raises(ValueError, match="private bounded regular file"):
        cli._read_private_json(symlink_path)

    duplicate_path = tmp_path / "duplicate.json"
    duplicate_path.write_bytes(b'{"key":1,"key":2}')
    duplicate_path.chmod(0o600)
    with pytest.raises(ValueError, match="valid bounded JSON"):
        cli._read_private_json(duplicate_path)


def test_completion_file_boundary_rejects_hardlink(tmp_path: Path) -> None:
    completion_path = tmp_path / "completion.json"
    completion_path.write_bytes(canonical_json({"outline": ["bounded"]}))
    completion_path.chmod(0o600)
    alias_path = tmp_path / "completion-alias.json"
    alias_path.hardlink_to(completion_path)

    with pytest.raises(ValueError, match="private bounded regular file"):
        cli._read_private_json(completion_path)


def test_completion_file_boundary_rechecks_mode_after_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completion_path = tmp_path / "completion.json"
    completion_path.write_bytes(canonical_json({"outline": ["bounded"]}))
    completion_path.chmod(0o600)
    raw_open = cli.os.open

    def open_after_mode_change(path: Path, flags: int) -> int:
        completion_path.chmod(0o644)
        return raw_open(path, flags)

    monkeypatch.setattr(cli.os, "open", open_after_mode_change)

    with pytest.raises(ValueError, match="changed while opening"):
        cli._read_private_json(completion_path)


def test_completion_file_boundary_rechecks_metadata_after_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completion_path = tmp_path / "completion.json"
    completion_path.write_bytes(canonical_json({"outline": ["bounded"]}))
    completion_path.chmod(0o600)
    raw_read = cli.os.read
    changed = False

    def read_then_change(descriptor: int, size: int) -> bytes:
        nonlocal changed
        chunk = raw_read(descriptor, size)
        if not changed:
            changed = True
            with completion_path.open("ab") as file:
                file.write(b" ")
        return chunk

    monkeypatch.setattr(cli.os, "read", read_then_change)

    with pytest.raises(ValueError, match="changed while reading"):
        cli._read_private_json(completion_path)


def test_supervised_cli_allowlist_contains_no_execution_command() -> None:
    parser = cli._parser()
    shared = [
        "--company-id",
        "company-1",
        "--job-id",
        "job-1",
    ]
    allowed = {
        "supervised-packet": ["--session-ref", "session-1"],
        "supervised-status": [],
        "supervised-bind": [
            "--completion-file",
            "/private/completion.json",
            "--operator-id",
            "operator-1",
            "--session-ref",
            "session-1",
            "--provider-id",
            "openai-codex",
            "--model-id",
            "gpt-5.6-terra",
        ],
    }

    for command, extra in allowed.items():
        assert parser.parse_args([command, *shared, *extra]).command == command

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "supervised-status",
                *shared,
                "--state-path",
                "/private/alternate.sqlite",
            ]
        )

    for forbidden in (
        "supervised-recovery",
        "supervised-cancel",
        "supervised-complete",
        "supervised-execute",
        "provider",
        "hermes",
    ):
        with pytest.raises(SystemExit):
            parser.parse_args([forbidden])


def test_worker_without_explicit_executor_selection_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli.Settings, "from_env", lambda _env: _settings())
    monkeypatch.setattr(
        cli,
        "run_worker",
        lambda *_args, **_kwargs: pytest.fail("worker must not start"),
    )

    with pytest.raises(SystemExit, match="explicit executor selection"):
        cli.main(["worker"])


def test_worker_mock_selection_is_explicit_and_non_production_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[object] = []
    monkeypatch.setattr(cli.Settings, "from_env", lambda _env: _settings("test"))
    monkeypatch.setattr(
        cli,
        "run_worker",
        lambda _settings, *, executor: observed.append(executor),
    )

    cli.main(["worker", "--mock"])

    assert len(observed) == 1
    assert isinstance(observed[0], StagedMockExecutor)
    assert observed[0].durable_semantic_idempotency is True


def test_production_rejects_mock_worker_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli.Settings, "from_env", lambda _env: _settings("production"))
    monkeypatch.setattr(
        cli,
        "run_worker",
        lambda *_args, **_kwargs: pytest.fail("worker must not start"),
    )

    with pytest.raises(SystemExit, match="unavailable until Task 12"):
        cli.main(["worker", "--mock"])
