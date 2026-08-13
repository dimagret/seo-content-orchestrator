"""Deterministic executor contract tests."""

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from seo_orchestrator.domain import ExecutionSnapshot, JobState, SeoJob
from seo_orchestrator.executors.base import ExecutionStatus, ExecutorError, ExternalStatus
from seo_orchestrator.executors.mock import MockExecutor
from seo_orchestrator.services.artifacts import ExecutionResult

NOW = datetime(2026, 8, 12, 10, tzinfo=UTC)


def _job(*, job_id: str = "job-one", snapshot_hash: str = "a" * 64) -> SeoJob:
    return SeoJob(
        job_id=job_id,
        brief_id="brief-one",
        brief_fingerprint="b" * 64,
        snapshot_id="snapshot-one",
        snapshot_hash=snapshot_hash,
        company_id="company-one",
        direction_id="direction-one",
        audience_segment_id="audience-one",
        state=JobState.QUEUED,
        current_stage=None,
        approved_plan_fingerprint="c" * 64,
        approval_record_id="approval-one",
        attempt=1,
        created_at=NOW,
        started_at=None,
        finished_at=None,
        error_code=None,
        error_summary=None,
        artifact_manifest_path=None,
        company_profile_version=1,
        direction_version=1,
        audience_version=1,
        prompt_set_version=1,
    )


def _snapshot(*, snapshot_hash: str = "a" * 64) -> ExecutionSnapshot:
    return ExecutionSnapshot(
        snapshot_id="snapshot-one",
        brief_id="brief-one",
        company_id="company-one",
        company_profile_version=1,
        direction_id="direction-one",
        direction_version=1,
        audience_segment_id="audience-one",
        audience_version=1,
        prompt_set_version=1,
        compiled_context={
            "schema_version": 1,
            "company": {},
            "direction": {},
            "audience": {},
            "brief": {},
            "prompt_set_version": 1,
        },
        snapshot_hash=snapshot_hash,
        created_at=NOW,
    )


def _result() -> ExecutionResult:
    return ExecutionResult(
        content_markdown="# Deterministic result\n",
        titles=("One", "Two", "Three", "Four", "Five"),
        descriptions=("One", "Two", "Three", "Four", "Five"),
        keyword_qa={"primary_keyword": "test", "occurrences": 1, "passed": True},
        text_metrics={"characters": 23},
        sources=(),
        warnings=(),
        model_usage={
            "models": [
                {
                    "model_id": "mock-model",
                    "provider_id": "mock-provider",
                    "input_tokens": 1,
                    "output_tokens": 1,
                }
            ]
        },
        stage_timings={"mock": 1},
        prompt_versions={"writer": "v1"},
    )


def test_submit_is_semantically_idempotent_and_poll_is_scripted() -> None:
    executor = MockExecutor(
        outcomes=(
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.RUNNING,
                stage_id="research",
                retry_after_seconds=2,
                error_code=None,
                error_summary=None,
                result=None,
            ),
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.SUCCEEDED,
                stage_id="complete",
                retry_after_seconds=None,
                error_code=None,
                error_summary=None,
                result=_result(),
            ),
        ),
        clock=lambda: NOW,
        run_id_factory=lambda number: ("external-one", "external-two")[number - 1],
    )

    first = executor.submit(_job(), _snapshot())
    duplicate = executor.submit(_job(), _snapshot())
    resumed = executor.submit(replace(_job(), attempt=2), _snapshot())

    assert duplicate == first
    assert resumed != first
    assert first.idempotency_key == "company-one:job-one:1"
    assert resumed.idempotency_key == "company-one:job-one:2"
    assert executor.submission_count == 2
    assert executor.poll(first).status is ExternalStatus.RUNNING
    assert executor.poll(first).status is ExternalStatus.SUCCEEDED


def test_submit_rejects_reused_key_with_different_immutable_identity() -> None:
    executor = MockExecutor(clock=lambda: NOW)
    executor.submit(_job(), _snapshot())

    with pytest.raises(ValueError, match="idempotency key"):
        executor.submit(
            _job(snapshot_hash="d" * 64),
            _snapshot(snapshot_hash="d" * 64),
        )

    with pytest.raises(ValueError, match="idempotency key"):
        executor.submit(
            replace(_job(), approved_plan_fingerprint="d" * 64),
            _snapshot(),
        )

    with pytest.raises(ValueError, match="idempotency key"):
        executor.submit(
            replace(_job(), approval_record_id="approval-two"),
            _snapshot(),
        )


@pytest.mark.parametrize(
    "error_code",
    ["NETWORK_ERROR", "HTTP_429", "HTTP_502", "HTTP_503", "HTTP_504"],
)
def test_submit_scripts_frozen_transient_error_matrix(error_code: str) -> None:
    executor = MockExecutor(
        submit_errors=(
            ExecutorError(error_code=error_code, error_summary="transient failure"),
        ),
        clock=lambda: NOW,
    )

    with pytest.raises(ExecutorError) as failure:
        executor.submit(_job(), _snapshot())

    assert failure.value.error_code == error_code
    assert executor.submission_count == 0


def test_poll_scripts_model_output_invalid_terminal_status() -> None:
    executor = MockExecutor(
        outcomes=(
            ExecutionStatus(
                external_run_id="mock-run-1",
                status=ExternalStatus.FAILED_FINAL,
                stage_id="writer",
                retry_after_seconds=None,
                error_code="MODEL_OUTPUT_INVALID",
                error_summary="result schema is invalid",
                result=None,
            ),
        ),
        clock=lambda: NOW,
    )
    run = executor.submit(_job(), _snapshot())

    status = executor.poll(run)

    assert status.status is ExternalStatus.FAILED_FINAL
    assert status.error_code == "MODEL_OUTPUT_INVALID"


@pytest.mark.parametrize(
    "error_code",
    ["NETWORK_ERROR", "HTTP_429", "HTTP_502", "HTTP_503", "HTTP_504"],
)
def test_poll_scripts_frozen_transient_error_matrix(error_code: str) -> None:
    executor = MockExecutor(
        outcomes=(
            ExecutorError(error_code=error_code, error_summary="transient failure"),
        ),
        clock=lambda: NOW,
    )
    run = executor.submit(_job(), _snapshot())

    with pytest.raises(ExecutorError) as failure:
        executor.poll(run)

    assert failure.value.error_code == error_code
    assert executor.poll_call_count == 1


def test_authority_deadline_is_exclusive_before_provider_side_effect() -> None:
    executor = MockExecutor(clock=lambda: NOW)

    with pytest.raises(ExecutorError) as exc_info:
        executor.submit_authorized(
            _job(),
            _snapshot(),
            authority_expires_at=NOW,
            approved_model_ids=("writer-model-v1",),
            approved_provider_ids=("mock-provider",),
        )

    assert exc_info.value.error_code == "APPROVAL_EXPIRED"
    assert executor.submit_call_count == 1
    assert executor.submission_count == 0


def test_cancel_is_idempotent_and_poll_reports_canceled() -> None:
    executor = MockExecutor(clock=lambda: NOW)
    run = executor.submit(_job(), _snapshot())

    first_confirmation = executor.cancel(run)
    duplicate_confirmation = executor.cancel(run)

    assert executor.cancellation_count == 1
    assert first_confirmation.status is ExternalStatus.CANCELED
    assert duplicate_confirmation.status is ExternalStatus.CANCELED
    assert executor.poll(run).status is ExternalStatus.CANCELED


def test_durable_mock_deduplicates_and_preserves_cancel_across_instances(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "mock-provider.db"
    first = MockExecutor(clock=lambda: NOW, state_path=state_path)
    run = first.submit(_job(), _snapshot())
    first.cancel(run)

    restarted = MockExecutor(clock=lambda: NOW, state_path=state_path)
    duplicate = restarted.submit(_job(), _snapshot())

    assert duplicate == run
    assert restarted.submission_count == 1
    assert restarted.poll(run).status is ExternalStatus.CANCELED
