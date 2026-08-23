"""End-to-end acceptance for the restart-safe staged mock pipeline."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from seo_orchestrator.executors.base import execution_result_bytes, execution_result_from_bytes
from seo_orchestrator.executors.staged_mock import (
    PIPELINE_VERSION,
    STAGE_IDS,
    StagedMockExecutor,
)
from tests.e2e.support import (
    api_request,
    app_for,
    assert_succeeded,
    create_company_card,
    load_company_fixture,
    make_runner,
    make_settings,
    plan_flow,
    read_manifest,
)


def _current_stage(connection: object, company_id: str, job_id: str) -> str | None:
    row = connection.execute(  # type: ignore[attr-defined]
        """SELECT current_stage FROM job_execution_runs
           WHERE company_id = ? AND job_id = ?""",
        (company_id, job_id),
    ).fetchone()
    assert row is not None
    value = row[0]
    assert value is None or isinstance(value, str)
    return value


def _replacement_commitment(identity_json: str, payload: bytes) -> str:
    digest = hashlib.sha256()
    for value in (identity_json.encode("utf-8"), PIPELINE_VERSION.encode("ascii"), payload):
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.hexdigest()


def test_staged_mock_restarts_and_publishes_truthful_immutable_artifact(
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    app = app_for(settings)
    fixture = load_company_fixture("avtomalyar")
    create_company_card(app, settings, fixture)
    flow = plan_flow(app, settings, fixture, pipeline_version=PIPELINE_VERSION)
    state_path = tmp_path / "staged-mock.db"

    current_time = [datetime.now(UTC)]

    def clock() -> datetime:
        return current_time[0]

    def advance() -> None:
        current_time[0] += timedelta(seconds=30)

    first_executor = StagedMockExecutor(state_path=state_path, clock=clock)
    first_connection, first_runner = make_runner(
        settings,
        first_executor,
        runner_id="staged-runner-before-restart",
        lease_token="staged-lease-before-restart",
        clock=clock,
    )
    try:
        assert first_runner.tick() == 1  # durable submission
        provider_state = sqlite3.connect(state_path)
        try:
            payload = provider_state.execute(
                "SELECT result_json FROM staged_mock_runs"
            ).fetchone()[0]
            original = execution_result_from_bytes(payload)
            altered_payload = execution_result_bytes(
                replace(original, content_markdown=f"{original.content_markdown}\nALTERED")
            )
            with pytest.raises(
                sqlite3.IntegrityError, match="immutable staged mock result"
            ):
                provider_state.execute(
                    "UPDATE staged_mock_runs SET result_json = ?, result_hash = ?",
                    (altered_payload, hashlib.sha256(altered_payload).hexdigest()),
                )
        finally:
            provider_state.close()
        for expected_stage in STAGE_IDS[:2]:
            advance()
            assert first_runner.tick() == 1
            assert _current_stage(first_connection, flow.company_id, flow.job_id) == expected_stage
    finally:
        first_connection.close()

    restarted_executor = StagedMockExecutor(state_path=state_path, clock=clock)
    restarted_connection, restarted_runner = make_runner(
        settings,
        restarted_executor,
        runner_id="staged-runner-after-restart",
        lease_token="staged-lease-after-restart",
        clock=clock,
    )
    observed: list[str | None] = list(STAGE_IDS[:2])
    try:
        for expected_stage in STAGE_IDS[2:]:
            advance()
            assert restarted_runner.tick() == 1
            observed.append(
                _current_stage(restarted_connection, flow.company_id, flow.job_id)
            )
            assert observed[-1] == expected_stage
        advance()
        assert restarted_runner.tick() == 1
    finally:
        restarted_connection.close()

    assert observed == list(STAGE_IDS)
    assert_succeeded(settings, flow)
    assert api_request(
        app,
        "GET",
        f"/v1/jobs/{flow.job_id}",
        params={"company_id": flow.company_id},
    ).json()["state"] == "SUCCEEDED"

    manifest = read_manifest(settings, flow)
    assert manifest["warnings"] == ["mock execution did not fetch external sources"]
    assert manifest["source_provenance"] == []
    assert manifest["prompt_versions"] == {"pipeline": "staged-mock-v1"}
    assert manifest["stage_timings"] == {stage_id: 0 for stage_id in STAGE_IDS}
    bundle = settings.artifact_root / "companies" / flow.company_id / "jobs" / flow.job_id
    assert json.loads((bundle / "sources.json").read_bytes()) == []
    metadata = json.loads((bundle / "metadata.json").read_bytes())
    assert metadata["warnings"] == ["mock execution did not fetch external sources"]
    assert "ALTERED" not in (bundle / "content.md").read_text(encoding="utf-8")


def test_staged_mock_rejects_different_approved_pipeline_before_submit(
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    app = app_for(settings)
    fixture = load_company_fixture("avtomalyar")
    create_company_card(app, settings, fixture)
    flow = plan_flow(app, settings, fixture, pipeline_version="different-pipeline-v1")
    state_path = tmp_path / "staged-mock.db"
    executor = StagedMockExecutor(state_path=state_path)
    connection, runner = make_runner(
        settings,
        executor,
        runner_id="pipeline-mismatch-runner",
        lease_token="pipeline-mismatch-lease",
    )
    try:
        assert runner.tick() == 1
        assert connection.execute(
            """SELECT state, current_stage, error_code FROM jobs
               WHERE company_id = ? AND job_id = ?""",
            (flow.company_id, flow.job_id),
        ).fetchone() == ("QUEUED", "reconciliation_required", "EXECUTOR_MISMATCH")
    finally:
        connection.close()
    provider_state = sqlite3.connect(state_path)
    try:
        assert provider_state.execute("SELECT COUNT(*) FROM mock_executor_runs").fetchone()[0] == 0
    finally:
        provider_state.close()


def test_runner_rejects_replacement_after_exact_guard_restoration(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    app = app_for(settings)
    fixture = load_company_fixture("avtomalyar")
    create_company_card(app, settings, fixture)
    flow = plan_flow(app, settings, fixture, pipeline_version=PIPELINE_VERSION)
    state_path = tmp_path / "staged-mock.db"
    current_time = [datetime.now(UTC)]

    def clock() -> datetime:
        return current_time[0]

    executor = StagedMockExecutor(state_path=state_path, clock=clock)
    connection, runner = make_runner(
        settings,
        executor,
        runner_id="replacement-runner",
        lease_token="replacement-lease",
        clock=clock,
    )
    try:
        assert runner.tick() == 1
        provider_state = sqlite3.connect(state_path)
        try:
            trigger_sql = provider_state.execute(
                """SELECT sql FROM sqlite_master
                   WHERE type = 'trigger' AND name = 'staged_mock_result_immutable'"""
            ).fetchone()[0]
            identity_json, payload = provider_state.execute(
                """SELECT lifecycle.identity_json, staged.result_json
                   FROM staged_mock_runs AS staged
                   JOIN mock_executor_runs AS lifecycle USING (idempotency_key)"""
            ).fetchone()
            original = execution_result_from_bytes(payload)
            altered_payload = execution_result_bytes(
                replace(original, content_markdown=f"{original.content_markdown}\nALTERED")
            )
            provider_state.execute("DROP TRIGGER staged_mock_result_immutable")
            provider_state.execute(
                """UPDATE staged_mock_runs
                   SET result_json = ?, result_hash = ?, result_commitment = ?""",
                (
                    altered_payload,
                    hashlib.sha256(altered_payload).hexdigest(),
                    _replacement_commitment(identity_json, altered_payload),
                ),
            )
            provider_state.execute(trigger_sql)
            provider_state.commit()
        finally:
            provider_state.close()

        for _ in range(len(STAGE_IDS) + 1):
            current_time[0] += timedelta(seconds=30)
            assert runner.tick() == 1

        assert connection.execute(
            """SELECT state, current_stage, error_code FROM jobs
               WHERE company_id = ? AND job_id = ?""",
            (flow.company_id, flow.job_id),
        ).fetchone() == (
            "RUNNING",
            "reconciliation_required",
            "EXECUTOR_POLL_OUTPUT_INVALID",
        )
    finally:
        connection.close()

    bundle = settings.artifact_root / "companies" / flow.company_id / "jobs" / flow.job_id
    assert not bundle.exists()
