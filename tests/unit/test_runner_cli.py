"""Fail-closed CLI selection for the Task 10 runner."""

from pathlib import Path

import pytest

from seo_orchestrator import cli
from seo_orchestrator.executors.mock import MockExecutor
from seo_orchestrator.settings import Settings


def _settings(environment: str = "development") -> Settings:
    return Settings(
        environment=environment,
        db_path=Path("/tmp/runner-cli.db"),
        artifact_root=Path("/tmp/runner-cli-artifacts"),
        listen="unix:/tmp/runner-cli.sock",
    )


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
    assert isinstance(observed[0], MockExecutor)
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
