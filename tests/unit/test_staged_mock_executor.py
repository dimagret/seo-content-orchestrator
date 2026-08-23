"""Deterministic staged mock executor tests."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from seo_orchestrator.canonical import JsonValue, sha256_fingerprint
from seo_orchestrator.domain import ExecutionSnapshot, JobState, SeoJob
from seo_orchestrator.executors.base import (
    ExternalStatus,
    execution_result_bytes,
    execution_result_from_bytes,
)
from seo_orchestrator.executors.staged_mock import (
    STAGE_IDS,
    StagedMockExecutor,
    build_staged_mock_result,
)

NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)


def _snapshot(*, context: JsonValue | None = None) -> ExecutionSnapshot:
    compiled_context = context if context is not None else {
        "schema_version": 1,
        "company": {"name": "Example Company"},
        "direction": {"name": "Paint Repair"},
        "audience": {"name": "Car owners"},
        "brief": {
            "primary_keyword": "покраска автомобиля",
            "goal": "Объяснить проверяемый порядок работ",
            "page_structure": ["Диагностика", "Подготовка", "Контроль"],
        },
        "prompt_set_version": 3,
    }
    return ExecutionSnapshot(
        snapshot_id="snapshot-one",
        brief_id="brief-one",
        company_id="company-one",
        company_profile_version=1,
        direction_id="direction-one",
        direction_version=1,
        audience_segment_id="audience-one",
        audience_version=1,
        prompt_set_version=3,
        compiled_context=compiled_context,
        snapshot_hash=sha256_fingerprint(compiled_context),
        created_at=NOW,
    )


def _job(*, attempt: int = 1) -> SeoJob:
    return SeoJob(
        job_id="job-one",
        brief_id="brief-one",
        brief_fingerprint="b" * 64,
        snapshot_id="snapshot-one",
        snapshot_hash=_snapshot().snapshot_hash,
        company_id="company-one",
        direction_id="direction-one",
        audience_segment_id="audience-one",
        state=JobState.QUEUED,
        current_stage=None,
        approved_plan_fingerprint="c" * 64,
        approval_record_id="approval-one",
        attempt=attempt,
        created_at=NOW,
        started_at=None,
        finished_at=None,
        error_code=None,
        error_summary=None,
        artifact_manifest_path=None,
        company_profile_version=1,
        direction_version=1,
        audience_version=1,
        prompt_set_version=3,
    )


def test_result_builder_is_deterministic_and_truthful() -> None:
    first = build_staged_mock_result(
        _snapshot(),
        model_id="writer-model-v1",
        provider_id="mock-provider",
    )
    second = build_staged_mock_result(
        _snapshot(),
        model_id="writer-model-v1",
        provider_id="mock-provider",
    )

    assert execution_result_bytes(first) == execution_result_bytes(second)
    assert "покраска автомобиля" in first.content_markdown.casefold()
    assert len(first.titles) == len(first.descriptions) == 5
    assert first.sources == ()
    assert first.warnings == ("mock execution did not fetch external sources",)
    assert first.model_usage == {
        "models": [
            {
                "model_id": "writer-model-v1",
                "provider_id": "mock-provider",
                "input_tokens": 0,
                "output_tokens": 0,
            }
        ]
    }
    assert first.stage_timings == {stage_id: 0 for stage_id in STAGE_IDS}
    assert first.prompt_versions == {"pipeline": "staged-mock-v1"}


@pytest.mark.parametrize(
    "context",
    [
        {},
        {"brief": {}},
        {"brief": {"primary_keyword": "test", "goal": "goal", "page_structure": []}},
        {
            "brief": {
                "primary_keyword": "test",
                "goal": "goal",
                "page_structure": ["section"],
            },
            "company": {"name": "company"},
            "direction": {"name": "direction"},
        },
    ],
)
def test_result_builder_rejects_incomplete_snapshot_context(context: JsonValue) -> None:
    with pytest.raises(ValueError, match="snapshot context"):
        build_staged_mock_result(
            _snapshot(context=context),
            model_id="writer-model-v1",
            provider_id="mock-provider",
        )


def test_result_builder_rejects_context_that_does_not_match_frozen_hash() -> None:
    context = _snapshot().thawed_compiled_context()
    assert type(context) is dict
    snapshot = _snapshot().model_copy(
        update={
            "compiled_context": {
                **context,
                "prompt_set_version": 999,
            }
        }
    )

    with pytest.raises(ValueError, match="snapshot context hash"):
        build_staged_mock_result(
            snapshot,
            model_id="writer-model-v1",
            provider_id="mock-provider",
        )


def test_executor_deduplicates_and_completes_fixed_stages(tmp_path: Path) -> None:
    executor = StagedMockExecutor(state_path=tmp_path / "mock.db", clock=lambda: NOW)

    run = executor.submit(_job(), _snapshot())
    assert executor.submit(_job(), _snapshot()) == run

    statuses = [executor.poll(run) for _ in STAGE_IDS]
    assert [status.stage_id for status in statuses] == list(STAGE_IDS)
    assert all(status.status is ExternalStatus.RUNNING for status in statuses)

    terminal = executor.poll(run)
    assert terminal.status is ExternalStatus.SUCCEEDED
    assert terminal.stage_id == "complete"
    assert terminal.result is not None
    assert terminal.result.sources == ()
    assert executor.poll(run) == terminal


def test_executor_resumes_exact_next_stage_after_restart(tmp_path: Path) -> None:
    state_path = tmp_path / "mock.db"
    first = StagedMockExecutor(state_path=state_path, clock=lambda: NOW)
    run = first.submit(_job(), _snapshot())
    assert first.poll(run).stage_id == STAGE_IDS[0]
    assert first.poll(run).stage_id == STAGE_IDS[1]

    restarted = StagedMockExecutor(state_path=state_path, clock=lambda: NOW)
    assert restarted.lookup(_job(), _snapshot()) == run
    assert restarted.poll(run).stage_id == STAGE_IDS[2]


def test_executor_preserves_terminal_cancel_across_restart(tmp_path: Path) -> None:
    state_path = tmp_path / "mock.db"
    first = StagedMockExecutor(state_path=state_path, clock=lambda: NOW)
    run = first.submit(_job(), _snapshot())
    first.poll(run)
    first.cancel(run)

    restarted = StagedMockExecutor(state_path=state_path, clock=lambda: NOW)
    assert restarted.cancel(run).status is ExternalStatus.CANCELED
    assert restarted.poll(run).status is ExternalStatus.CANCELED


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("pipeline_version", "wrong-version"),
        ("result_hash", "0" * 64),
        ("next_stage_index", 1.5),
        ("result_json", b"{}"),
    ],
)
def test_executor_rejects_tampered_durable_state(
    tmp_path: Path,
    column: str,
    value: object,
) -> None:
    state_path = tmp_path / f"{column}.db"
    executor = StagedMockExecutor(state_path=state_path, clock=lambda: NOW)
    run = executor.submit(_job(), _snapshot())
    connection = sqlite3.connect(state_path)
    try:
        if column == "next_stage_index":
            connection.execute(
                f"UPDATE staged_mock_runs SET {column} = ? WHERE idempotency_key = ?",
                (value, run.idempotency_key),
            )
            connection.commit()
        else:
            with pytest.raises(sqlite3.IntegrityError, match="immutable staged mock result"):
                connection.execute(
                    f"UPDATE staged_mock_runs SET {column} = ? WHERE idempotency_key = ?",
                    (value, run.idempotency_key),
                )
            return
    finally:
        connection.close()

    with pytest.raises(ValueError, match="staged mock state"):
        executor.poll(run)


def test_executor_rejects_self_consistent_result_replacement(tmp_path: Path) -> None:
    state_path = tmp_path / "self-consistent.db"
    executor = StagedMockExecutor(state_path=state_path, clock=lambda: NOW)
    run = executor.submit(_job(), _snapshot())
    connection = sqlite3.connect(state_path)
    try:
        original_payload = connection.execute(
            "SELECT result_json FROM staged_mock_runs WHERE idempotency_key = ?",
            (run.idempotency_key,),
        ).fetchone()[0]
        original = execution_result_from_bytes(original_payload)
        altered_payload = execution_result_bytes(
            replace(original, content_markdown=f"{original.content_markdown}\nALTERED")
        )
        with pytest.raises(sqlite3.IntegrityError, match="immutable staged mock result"):
            connection.execute(
                """UPDATE staged_mock_runs SET result_json = ?, result_hash = ?
                   WHERE idempotency_key = ?""",
                (
                    altered_payload,
                    hashlib.sha256(altered_payload).hexdigest(),
                    run.idempotency_key,
                ),
            )
    finally:
        connection.close()


def test_executor_fails_closed_when_integrity_guard_is_removed(tmp_path: Path) -> None:
    state_path = tmp_path / "missing-guard.db"
    executor = StagedMockExecutor(state_path=state_path, clock=lambda: NOW)
    run = executor.submit(_job(), _snapshot())
    connection = sqlite3.connect(state_path)
    try:
        connection.execute("DROP TRIGGER staged_mock_result_immutable")
        connection.execute(
            """CREATE TRIGGER staged_mock_result_immutable
               BEFORE UPDATE ON staged_mock_runs BEGIN SELECT 1; END"""
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(ValueError, match="integrity guards are missing"):
        executor.poll(run)


def test_executor_rejects_replace_of_existing_result(tmp_path: Path) -> None:
    state_path = tmp_path / "replace.db"
    executor = StagedMockExecutor(state_path=state_path, clock=lambda: NOW)
    executor.submit(_job(), _snapshot())
    connection = sqlite3.connect(state_path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="immutable staged mock result"):
            connection.execute(
                "INSERT OR REPLACE INTO staged_mock_runs SELECT * FROM staged_mock_runs"
            )
    finally:
        connection.close()


def test_attempts_have_independent_pipeline_state(tmp_path: Path) -> None:
    executor = StagedMockExecutor(state_path=tmp_path / "mock.db", clock=lambda: NOW)
    first = executor.submit(_job(), _snapshot())
    second = executor.submit(replace(_job(), attempt=2), _snapshot())

    assert first != second
    assert executor.poll(first).stage_id == STAGE_IDS[0]
    assert executor.poll(first).stage_id == STAGE_IDS[1]
    assert executor.poll(second).stage_id == STAGE_IDS[0]
