"""Restart-safe runner submission and stale reconciliation tests."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from seo_orchestrator.db.connection import connect
from seo_orchestrator.db.repositories import RunnerRepository
from seo_orchestrator.domain import ExecutionSnapshot, JobState, SeoJob
from seo_orchestrator.domain.approvals import fingerprint_plan
from seo_orchestrator.executors.base import (
    ExecutionStatus,
    ExecutorError,
    ExternalRun,
    ExternalStatus,
    execution_result_bytes,
)
from seo_orchestrator.executors.mock import MockExecutor
from seo_orchestrator.runner import RetryPolicy, Runner
from seo_orchestrator.services.approvals import ApprovalService
from seo_orchestrator.services.artifacts import ArtifactStore, ExecutionResult
from seo_orchestrator.services.jobs import JobService
from tests.integration.test_approval_invalidation import (
    NOW,
    _bind_persisted_paid_approval,
    _open_seeded,
    _plan,
    _prepare_awaiting_job,
)


class _SimulatedCrash(BaseException):
    pass


class _SubmitOverrideMixin(MockExecutor):
    def submit_authorized(
        self,
        job: SeoJob,
        snapshot: ExecutionSnapshot,
        *,
        authority_expires_at: datetime | None,
        approved_model_ids: tuple[str, ...],
        approved_provider_ids: tuple[str, ...],
    ) -> ExternalRun:
        if authority_expires_at is not None:
            MockExecutor._validate_authority(self._clock(), authority_expires_at)
        if self.model_ids != approved_model_ids or self.provider_ids != approved_provider_ids:
            raise ExecutorError(
                error_code="EXECUTOR_CONFIGURATION_UNAUTHORIZED",
                error_summary="executor provider/model configuration is not approved",
            )
        return self.submit(job, snapshot)


class _TransactionCheckingExecutor(_SubmitOverrideMixin):
    def __init__(
        self,
        connection: sqlite3.Connection,
        outcomes: tuple[ExecutionStatus | ExecutorError, ...] = (),
        cancel_errors: tuple[ExecutorError, ...] = (),
        clock=lambda: NOW,
    ) -> None:
        super().__init__(
            outcomes=outcomes,
            cancel_errors=cancel_errors,
            clock=clock,
            run_id_factory=lambda number: ("external-one", "external-two")[number - 1],
        )
        self._connection = connection

    def submit(self, job: object, snapshot: object):  # type: ignore[no-untyped-def]
        assert not self._connection.in_transaction
        return super().submit(job, snapshot)  # type: ignore[arg-type]

    def lookup(self, job: object, snapshot: object):  # type: ignore[no-untyped-def]
        assert not self._connection.in_transaction
        return super().lookup(job, snapshot)  # type: ignore[arg-type]

    def poll(self, run: object):  # type: ignore[no-untyped-def]
        assert not self._connection.in_transaction
        return super().poll(run)  # type: ignore[arg-type]

    def cancel(self, run: object) -> ExecutionStatus:
        assert not self._connection.in_transaction
        return super().cancel(run)  # type: ignore[arg-type]


class _NoExternalIoExecutor:
    name = "mock"
    model_ids = ("writer-model-v1",)
    provider_ids = ("mock-provider",)
    durable_semantic_idempotency = True
    side_effect_free_lookup = True
    idempotent_cancel = True
    cancel_confirms_terminal = True
    authority_deadline_enforced = True
    configuration_authorization_enforced = True

    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        del job, snapshot
        raise AssertionError("terminal recovery must not submit")

    def submit_authorized(
        self,
        job: SeoJob,
        snapshot: ExecutionSnapshot,
        *,
        authority_expires_at: datetime | None,
        approved_model_ids: tuple[str, ...],
        approved_provider_ids: tuple[str, ...],
    ) -> ExternalRun:
        del authority_expires_at, approved_model_ids, approved_provider_ids
        return self.submit(job, snapshot)

    def lookup(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun | None:
        del job, snapshot
        raise AssertionError("terminal recovery must not look up")

    def poll(self, run: ExternalRun) -> ExecutionStatus:
        del run
        raise AssertionError("terminal recovery must not poll")

    def cancel(self, run: ExternalRun) -> ExecutionStatus:
        del run
        raise AssertionError("terminal recovery must not cancel")


class _CapabilityDowngradeExecutor(MockExecutor):
    def __init__(self) -> None:
        self.configuration_authorized = True
        super().__init__()

    @property
    def configuration_authorization_enforced(self) -> bool:
        return self.configuration_authorized


class _NoDurableIdempotencyExecutor:
    name = "mock"
    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        del job, snapshot
        raise AssertionError("executor without durable idempotency must not submit")

    def submit_authorized(
        self,
        job: SeoJob,
        snapshot: ExecutionSnapshot,
        *,
        authority_expires_at: datetime | None,
        approved_model_ids: tuple[str, ...],
        approved_provider_ids: tuple[str, ...],
    ) -> ExternalRun:
        del authority_expires_at, approved_model_ids, approved_provider_ids
        return self.submit(job, snapshot)

    def lookup(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun | None:
        del job, snapshot
        raise AssertionError("executor without durable idempotency must not look up")

    def poll(self, run: ExternalRun) -> ExecutionStatus:
        del run
        raise AssertionError("unexpected poll")

    def cancel(self, run: ExternalRun) -> ExecutionStatus:
        del run
        raise AssertionError("unexpected cancel")


class _WrongSubmitIdentityExecutor(_SubmitOverrideMixin):
    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        accepted = super().submit(job, snapshot)
        return ExternalRun(
            external_run_id=accepted.external_run_id,
            idempotency_key="wrong-key",
            accepted_at=accepted.accepted_at,
        )


class _WrongPollIdentityExecutor(MockExecutor):
    def poll(self, run: ExternalRun) -> ExecutionStatus:
        self.poll_call_count += 1
        return ExecutionStatus(
            external_run_id="wrong-run",
            status=ExternalStatus.RUNNING,
            stage_id="research",
            retry_after_seconds=1,
            error_code=None,
            error_summary=None,
            result=None,
        )


class _MalformedPollOutputExecutor(MockExecutor):
    def poll(self, run: ExternalRun) -> ExecutionStatus:
        self.poll_call_count += 1
        raise ValueError(f"malformed poll response for {run.external_run_id}")


class _MalformedSubmitOutputExecutor(_SubmitOverrideMixin):
    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        super().submit(job, snapshot)
        return object()  # type: ignore[return-value]


class _UnexpectedSubmitExceptionExecutor(_SubmitOverrideMixin):
    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        super().submit(job, snapshot)
        raise RuntimeError("connection dropped after submit")


class _NaiveAcceptedAtExecutor(_SubmitOverrideMixin):
    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        run = super().submit(job, snapshot)
        object.__setattr__(run, "accepted_at", NOW.replace(tzinfo=None))
        return run


class _FutureAcceptedAtExecutor(_SubmitOverrideMixin):
    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        run = super().submit(job, snapshot)
        object.__setattr__(run, "accepted_at", NOW + timedelta(minutes=20))
        return run


class _ExpiryBoundaryAcceptedAtExecutor(_SubmitOverrideMixin):
    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        run = super().submit(job, snapshot)
        object.__setattr__(run, "accepted_at", NOW + timedelta(minutes=15))
        return run


class _NameChangingSubmitExecutor(_SubmitOverrideMixin):
    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        run = super().submit(job, snapshot)
        self.name = "wrong-executor"
        return run


class _NameChangingPollExecutor(MockExecutor):
    def poll(self, run: ExternalRun) -> ExecutionStatus:
        status = super().poll(run)
        self.name = "wrong-executor"
        return status


class _TimeoutAfterSubmitExecutor(_SubmitOverrideMixin):
    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        run = super().submit(job, snapshot)
        time.sleep(1)
        return run


class _TimeoutOnceAfterPollExecutor(MockExecutor):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self._delayed = False
        self._status: ExecutionStatus | None = None

    def poll(self, run: ExternalRun) -> ExecutionStatus:
        if self._status is None:
            self._status = super().poll(run)
        if not self._delayed:
            self._delayed = True
            time.sleep(1)
        return self._status


class _DelayedSubmitExecutor(_SubmitOverrideMixin):
    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        self.entered_count = getattr(self, "entered_count", 0) + 1
        time.sleep(1)
        return super().submit(job, snapshot)


class _NameChangingSubmitErrorExecutor(_SubmitOverrideMixin):
    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        super().submit(job, snapshot)
        self.name = "wrong-executor"
        raise ExecutorError(
            error_code="HTTP_503",
            error_summary="retryable provider error",
        )


class _NameChangingPollErrorExecutor(MockExecutor):
    def poll(self, run: ExternalRun) -> ExecutionStatus:
        self.name = "wrong-executor"
        raise ExecutorError(
            error_code="HTTP_503",
            error_summary="retryable provider error",
        )


def _queue_approved_job(
    database_path: Path,
    *,
    maximum_retries: int = 2,
    executor_name: str = "mock",
) -> None:
    connection, snapshot_hash, _context = _open_seeded(database_path)
    plan = _plan(
        executor_name=executor_name,
        model_ids=("writer-model-v1",),
        provider_ids=("mock-provider",),
        maximum_retries=maximum_retries,
    )
    try:
        _prepare_awaiting_job(connection, plan)
        ApprovalService(
            connection,
            company_id="company-one",
            clock=lambda: NOW,
            id_factory=lambda: "approval-one",
        ).approve_job(
            "job-one",
            "approver-one",
            snapshot_hash,
            fingerprint_plan(plan),
        )
    finally:
        connection.close()


def _runner(
    connection: sqlite3.Connection,
    executor: MockExecutor,
    artifact_root: Path,
    *,
    after_submit=lambda: None,
    now=NOW,
    clock=None,
    lease_duration: timedelta = timedelta(seconds=30),
    external_call_timeout: timedelta = timedelta(seconds=20),
    lease_safety_margin: timedelta = timedelta(seconds=5),
    approval_submission_margin: timedelta = timedelta(seconds=1),
    retry_policy: RetryPolicy | None = None,
) -> Runner:
    if isinstance(executor, MockExecutor) and not executor.durable_semantic_idempotency:
        executor.configure_durable_state(artifact_root.parent / "mock-executor.db")
    return Runner(
        connection,
        executor=executor,
        artifact_store=ArtifactStore(artifact_root, clock=lambda: now),
        clock=clock or (lambda: now),
        runner_id="runner-one",
        lease_token_factory=lambda: "lease-one",
        after_submit=after_submit,
        stale_after=timedelta(minutes=5),
        lease_duration=lease_duration,
        external_call_timeout=external_call_timeout,
        lease_safety_margin=lease_safety_margin,
        approval_submission_margin=approval_submission_margin,
        retry_policy=retry_policy,
    )


def _execution_result() -> ExecutionResult:
    fixture_path = Path(__file__).parents[2] / "fixtures" / "executions" / "success-result.json"
    value = json.loads(fixture_path.read_text(encoding="utf-8"))
    return ExecutionResult(
        content_markdown=value["content_markdown"],
        titles=tuple(value["titles"]),
        descriptions=tuple(value["descriptions"]),
        keyword_qa=value["keyword_qa"],
        text_metrics=value["text_metrics"],
        sources=tuple(value["sources"]),
        warnings=tuple(value["warnings"]),
        model_usage=value["model_usage"],
        stage_timings=value["stage_timings"],
        prompt_versions=value["prompt_versions"],
    )


def test_tick_submits_one_approved_job_outside_transaction_and_persists_running(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "runner.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _TransactionCheckingExecutor(connection)
    try:
        assert _runner(connection, executor, tmp_path / "artifacts").tick() == 1
        assert not connection.in_transaction
    finally:
        connection.close()

    reopened = connect(database_path)
    try:
        assert JobService(reopened, company_id="company-one").get_job("job-one").state is JobState.RUNNING
        assert reopened.execute(
            """SELECT attempt, idempotency_key, external_run_id, external_status,
                      submission_attempted_at, lease_token
               FROM job_execution_runs
               WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            1,
            "company-one:job-one:1",
            "external-one",
            "ACCEPTED",
            NOW.isoformat(),
            None,
        )
        assert reopened.execute(
            "SELECT from_state, to_state FROM job_transitions ORDER BY transition_id"
        ).fetchall()[-1] == ("QUEUED", "RUNNING")
        assert reopened.execute(
            "SELECT heartbeat_at FROM runner_heartbeats WHERE runner_id = ?",
            ("runner-one",),
        ).fetchone() == (NOW.isoformat(),)
    finally:
        reopened.close()
    assert executor.submission_count == 1


def test_expired_approval_is_quarantined_without_executor_io(tmp_path: Path) -> None:
    database_path = tmp_path / "expired-approval.db"
    connection, _snapshot_hash, _context = _open_seeded(database_path)
    try:
        _prepare_awaiting_job(connection, _plan(
            executor_name="mock",
            model_ids=("writer-model-v1",),
            provider_ids=("mock-provider",),
        ))
        _bind_persisted_paid_approval(
            connection,
            approved_at=NOW - timedelta(seconds=2),
            expires_at=NOW - timedelta(seconds=1),
        )
    finally:
        connection.close()

    reopened = connect(database_path)
    executor = MockExecutor(clock=lambda: NOW)
    try:
        runner = _runner(reopened, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        quarantined = JobService(reopened, company_id="company-one").get_job("job-one")
        assert quarantined.state is JobState.QUEUED
        assert quarantined.current_stage == "reconciliation_required"
        assert quarantined.error_code == "APPROVAL_INVALID"
        assert quarantined.error_summary == "approval is not valid at dispatch time"
        assert reopened.execute(
            """SELECT current_stage, error_code, error_summary, external_run_id,
                      external_status, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "reconciliation_required",
            "APPROVAL_INVALID",
            "approval is not valid at dispatch time",
            None,
            None,
            None,
        )
        assert runner.tick() == 0
    finally:
        reopened.close()

    assert executor.submit_call_count == 0


def test_corrupt_plan_is_quarantined_without_executor_io(tmp_path: Path) -> None:
    database_path = tmp_path / "corrupt-plan-quarantine.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    try:
        connection.execute("DROP TRIGGER jobs_execution_plan_immutable")
        connection.execute(
            "UPDATE jobs SET plan_fingerprint = ? WHERE company_id = ? AND job_id = ?",
            ("f" * 64, "company-one", "job-one"),
        )
        connection.commit()
    finally:
        connection.close()

    reopened = connect(database_path)
    executor = MockExecutor(clock=lambda: NOW)
    try:
        runner = _runner(reopened, executor, tmp_path / "artifacts")

        assert runner.tick() == 1
        assert runner.tick() == 0
        assert reopened.execute(
            """SELECT state, current_stage, error_code, error_summary
               FROM jobs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "QUEUED",
            "reconciliation_required",
            "DATA_INTEGRITY",
            "execution provenance failed integrity verification",
        )
    finally:
        reopened.close()

    assert executor.submit_call_count == 0


def test_approved_executor_mismatch_is_quarantined_without_io(tmp_path: Path) -> None:
    database_path = tmp_path / "executor-mismatch.db"
    _queue_approved_job(database_path, executor_name="isolated-n8n")
    connection = connect(database_path)
    executor = MockExecutor(clock=lambda: NOW)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT state, current_stage, error_code FROM jobs
               WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "QUEUED",
            "reconciliation_required",
            "EXECUTOR_MISMATCH",
        )
    finally:
        connection.close()

    assert executor.submit_call_count == 0
    assert executor.lookup_call_count == 0
    assert executor.poll_call_count == 0
    assert executor.cancel_call_count == 0


def test_executor_name_change_after_runner_construction_is_quarantined(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "executor-name-change.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = MockExecutor(clock=lambda: NOW)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        executor.name = "wrong-executor"
        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            "SELECT current_stage, error_code FROM jobs WHERE job_id = ?",
            ("job-one",),
        ).fetchone() == ("reconciliation_required", "EXECUTOR_MISMATCH")
    finally:
        connection.close()

    assert executor.submit_call_count == 0
    assert executor.lookup_call_count == 0


def test_runner_rejects_executor_without_durable_semantic_idempotency(
    tmp_path: Path,
) -> None:
    connection = connect(tmp_path / "executor-capability.db")
    try:
        with pytest.raises(TypeError, match="durable semantic idempotency"):
            Runner(
                connection,
                executor=_NoDurableIdempotencyExecutor(),  # type: ignore[arg-type]
                artifact_store=ArtifactStore(tmp_path / "artifacts"),
                runner_id="runner-one",
                lease_token_factory=lambda: "lease-one",
            )
    finally:
        connection.close()


def test_mismatched_submit_response_is_quarantined_without_resubmission(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "ambiguous-submit-output.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _WrongSubmitIdentityExecutor(clock=lambda: NOW)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")

        assert runner.tick() == 1
        assert runner.tick() == 0
        job = JobService(connection, company_id="company-one").get_job("job-one")
        assert job.state is JobState.RUNNING
        assert job.current_stage == "reconciliation_required"
        assert job.error_code == "EXECUTOR_SUBMIT_OUTPUT_INVALID"
        assert connection.execute(
            """SELECT external_run_id, external_status, current_stage,
                      reconciliation_count, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (None, None, "reconciliation_required", 1, None)
    finally:
        connection.close()

    assert executor.submit_call_count == 1
    assert executor.submission_count == 1


def test_submit_deadline_expires_before_lease_and_never_replays(tmp_path: Path) -> None:
    database_path = tmp_path / "submit-timeout.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _TimeoutAfterSubmitExecutor(clock=lambda: NOW)
    started = time.monotonic()
    try:
        runner = _runner(
            connection,
            executor,
            tmp_path / "artifacts-timeout",
            lease_duration=timedelta(milliseconds=300),
            external_call_timeout=timedelta(milliseconds=50),
            lease_safety_margin=timedelta(milliseconds=100),
        )
        assert runner.tick() == 1
        assert time.monotonic() - started < 0.5
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT external_run_id, executor_name, current_stage, error_code, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            None,
            "mock",
            "reconciliation_required",
            "EXECUTOR_SUBMIT_OUTPUT_INVALID",
            None,
        )
    finally:
        connection.close()

    assert executor.submission_count == 1
    assert executor.submit_call_count == 1


def test_submit_deadline_is_bounded_by_remaining_approval_authority(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "submit-approval-deadline.db"
    connection, _snapshot_hash, _context = _open_seeded(database_path)
    try:
        _prepare_awaiting_job(connection, _plan(
            executor_name="mock",
            model_ids=("writer-model-v1",),
            provider_ids=("mock-provider",),
        ))
        _bind_persisted_paid_approval(
            connection,
            approved_at=NOW - timedelta(seconds=1),
            expires_at=NOW + timedelta(milliseconds=200),
        )
    finally:
        connection.close()

    connection = connect(database_path)
    executor = _DelayedSubmitExecutor(clock=lambda: NOW)
    started = time.monotonic()
    try:
        runner = _runner(
            connection,
            executor,
            tmp_path / "artifacts-approval-deadline",
            lease_duration=timedelta(seconds=2),
            external_call_timeout=timedelta(seconds=1),
            lease_safety_margin=timedelta(milliseconds=500),
            approval_submission_margin=timedelta(milliseconds=50),
        )
        assert runner.tick() == 1
        assert time.monotonic() - started < 0.5
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT external_run_id, current_stage, error_code
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            None,
            "reconciliation_required",
            "EXECUTOR_SUBMIT_OUTPUT_INVALID",
        )
    finally:
        connection.close()

    assert executor.entered_count == 1
    assert executor.submission_count == 0


def test_executor_name_change_with_submit_error_cannot_schedule_retry(tmp_path: Path) -> None:
    database_path = tmp_path / "submit-error-name-change.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _NameChangingSubmitErrorExecutor(clock=lambda: NOW)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts-submit-error-name")
        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT current_stage, error_code, next_action_at
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("reconciliation_required", "EXECUTOR_MISMATCH", None)
    finally:
        connection.close()


def test_executor_name_change_with_poll_error_cannot_schedule_retry(tmp_path: Path) -> None:
    database_path = tmp_path / "poll-error-name-change.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _NameChangingPollErrorExecutor(clock=lambda: NOW)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts-poll-error-name")
        assert runner.tick() == 1
        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT external_status, current_stage, error_code, next_action_at
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "ACCEPTED",
            "reconciliation_required",
            "EXECUTOR_MISMATCH",
            None,
        )
    finally:
        connection.close()


def test_malformed_submit_output_is_quarantined_without_resubmission(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "malformed-submit-output.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _MalformedSubmitOutputExecutor(clock=lambda: NOW)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")

        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT current_stage, error_code, lease_token, external_run_id
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "reconciliation_required",
            "EXECUTOR_SUBMIT_OUTPUT_INVALID",
            None,
            None,
        )
    finally:
        connection.close()

    assert executor.submission_count == 1


def test_naive_submit_acceptance_time_is_quarantined_without_replay(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "naive-submit-accepted-at.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _NaiveAcceptedAtExecutor(clock=lambda: NOW)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT external_run_id, external_status, current_stage, error_code
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            None,
            None,
            "reconciliation_required",
            "EXECUTOR_SUBMIT_OUTPUT_INVALID",
        )
    finally:
        connection.close()

    assert executor.submit_call_count == 1
    assert executor.submission_count == 1


@pytest.mark.parametrize(
    ("executor_factory", "database_name"),
    [
        (_FutureAcceptedAtExecutor, "future-accepted-at.db"),
        (_ExpiryBoundaryAcceptedAtExecutor, "expiry-boundary-accepted-at.db"),
        (_NameChangingSubmitExecutor, "submit-name-change.db"),
    ],
)
def test_submit_provenance_violation_is_quarantined_without_acceptance(
    tmp_path: Path,
    executor_factory: type[MockExecutor],
    database_name: str,
) -> None:
    database_path = tmp_path / database_name
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = executor_factory(clock=lambda: NOW)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT external_run_id, external_status, current_stage, error_code
               FROM job_execution_runs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == (
            None,
            None,
            "reconciliation_required",
            "EXTERNAL_ACCEPTANCE_PROVENANCE_INVALID",
        )
    finally:
        connection.close()


def test_executor_provider_model_configuration_must_match_approved_plan(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "executor-config-mismatch.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = MockExecutor(
        model_ids=("unapproved-model",),
        provider_ids=("unapproved-provider",),
    )
    try:
        assert _runner(connection, executor, tmp_path / "config-artifacts").tick() == 1
        assert executor.submit_call_count == 0
        assert connection.execute(
            """SELECT current_stage, error_code FROM job_execution_runs
               WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("reconciliation_required", "EXECUTOR_MISMATCH")
    finally:
        connection.close()


def test_executor_capability_downgrade_is_quarantined_before_io(tmp_path: Path) -> None:
    database_path = tmp_path / "executor-capability-downgrade.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _CapabilityDowngradeExecutor()
    runner = _runner(connection, executor, tmp_path / "capability-artifacts")
    executor.configuration_authorized = False
    try:
        assert runner.tick() == 1
        assert executor.submit_call_count == 0
        assert connection.execute(
            """SELECT current_stage, error_code FROM job_execution_runs
               WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("reconciliation_required", "EXECUTOR_MISMATCH")
    finally:
        connection.close()


def test_terminal_model_usage_must_match_approved_plan(tmp_path: Path) -> None:
    database_path = tmp_path / "terminal-model-usage-mismatch.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    result = _execution_result()
    object.__setattr__(
        result,
        "model_usage",
        {
            "models": [
                {
                    "model_id": "unapproved-model",
                    "provider_id": "unapproved-provider",
                    "input_tokens": 1,
                    "output_tokens": 1,
                }
            ]
        },
    )
    executor = MockExecutor(
        outcomes=(
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.SUCCEEDED,
                stage_id="complete",
                retry_after_seconds=None,
                error_code=None,
                error_summary=None,
                result=result,
            ),
        ),
        run_id_factory=lambda _number: "external-one",
        clock=lambda: NOW,
    )
    try:
        runner = _runner(connection, executor, tmp_path / "usage-artifacts")
        assert runner.tick() == 1
        assert runner.tick() == 1
        assert connection.execute(
            """SELECT current_stage, error_code, result_json
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "reconciliation_required",
            "EXECUTOR_POLL_OUTPUT_INVALID",
            None,
        )
    finally:
        connection.close()


def test_executor_name_change_during_poll_is_quarantined_before_persistence(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "poll-name-change.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _NameChangingPollExecutor(
        outcomes=(
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.RUNNING,
                stage_id="writer",
                retry_after_seconds=1,
                error_code=None,
                error_summary=None,
                result=None,
            ),
        ),
        clock=lambda: NOW,
        run_id_factory=lambda _number: "external-one",
    )
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT external_status, current_stage, error_code
               FROM job_execution_runs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == (
            "ACCEPTED",
            "reconciliation_required",
            "EXECUTOR_MISMATCH",
        )
    finally:
        connection.close()


def test_unexpected_submit_exception_is_quarantined_without_resubmission(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "unexpected-submit-exception.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _UnexpectedSubmitExceptionExecutor(clock=lambda: NOW)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")

        assert runner.tick() == 1
        assert runner.tick() == 0
        job = JobService(connection, company_id="company-one").get_job("job-one")
        assert job.current_stage == "reconciliation_required"
        assert job.error_code == "EXECUTOR_SUBMIT_OUTPUT_INVALID"
    finally:
        connection.close()

    assert executor.submit_call_count == 1


def test_acceptance_persistence_failure_is_quarantined_without_resubmission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "acceptance-persistence-failure.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = MockExecutor(clock=lambda: NOW)

    def fail_acceptance(*_args: object, **_kwargs: object) -> bool:
        raise sqlite3.IntegrityError("external identity collision")

    monkeypatch.setattr(RunnerRepository, "record_external_acceptance", fail_acceptance)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")

        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT current_stage, error_code, lease_token, external_run_id
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "reconciliation_required",
            "EXECUTOR_SUBMIT_OUTPUT_INVALID",
            None,
            None,
        )
    finally:
        connection.close()

    assert executor.submission_count == 1


def test_mismatched_poll_response_is_quarantined_without_repolling(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "invalid-poll-output.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _WrongPollIdentityExecutor(clock=lambda: NOW)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")

        assert runner.tick() == 1
        assert runner.tick() == 1
        assert runner.tick() == 0
        job = JobService(connection, company_id="company-one").get_job("job-one")
        assert job.state is JobState.RUNNING
        assert job.current_stage == "reconciliation_required"
        assert job.error_code == "EXECUTOR_POLL_OUTPUT_INVALID"
        assert connection.execute(
            """SELECT external_status, current_stage, next_action_at,
                      reconciliation_count, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "ACCEPTED",
            "reconciliation_required",
            None,
            1,
            None,
        )
    finally:
        connection.close()

    assert executor.poll_call_count == 1


def test_malformed_poll_exception_is_quarantined_without_repolling(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "malformed-poll-exception.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _MalformedPollOutputExecutor(clock=lambda: NOW)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT current_stage, error_code, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "reconciliation_required",
            "EXECUTOR_POLL_OUTPUT_INVALID",
            None,
        )
    finally:
        connection.close()

    assert executor.poll_call_count == 1


def test_dispatch_recovery_uses_lookup_only_after_approval_expires(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "approval-expiry-after-dispatch.db"
    connection, _snapshot_hash, _context = _open_seeded(database_path)
    try:
        _prepare_awaiting_job(connection, _plan(
            executor_name="mock",
            model_ids=("writer-model-v1",),
            provider_ids=("mock-provider",),
        ))
        _bind_persisted_paid_approval(
            connection,
            approved_at=NOW - timedelta(seconds=1),
            expires_at=NOW + timedelta(seconds=2),
        )
    finally:
        connection.close()

    first_connection = connect(database_path)
    executor = MockExecutor(
        clock=lambda: NOW,
        run_id_factory=lambda _number: "external-one",
    )
    try:
        with pytest.raises(_SimulatedCrash):
            _runner(
                first_connection,
                executor,
                tmp_path / "artifacts",
                after_submit=lambda: (_ for _ in ()).throw(_SimulatedCrash),
            ).tick()
    finally:
        first_connection.close()

    restarted = connect(database_path)
    try:
        assert _runner(
            restarted,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(seconds=31),
        ).tick() == 1
        assert restarted.execute(
            """SELECT idempotency_key, external_run_id, external_status, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("company-one:job-one:1", "external-one", "ACCEPTED", None)
    finally:
        restarted.close()

    assert executor.submit_call_count == 1
    assert executor.lookup_call_count == 1
    assert executor.submission_count == 1


def test_expired_dispatch_lookup_miss_never_replays_submit(tmp_path: Path) -> None:
    database_path = tmp_path / "approval-expiry-lookup-miss.db"
    connection, _snapshot_hash, _context = _open_seeded(database_path)
    try:
        _prepare_awaiting_job(connection, _plan(
            executor_name="mock",
            model_ids=("writer-model-v1",),
            provider_ids=("mock-provider",),
        ))
        _bind_persisted_paid_approval(
            connection,
            approved_at=NOW - timedelta(seconds=1),
            expires_at=NOW + timedelta(seconds=2),
        )
    finally:
        connection.close()

    first_connection = connect(database_path)
    executor = MockExecutor(clock=lambda: NOW)
    try:
        with pytest.raises(_SimulatedCrash):
            _runner(
                first_connection,
                executor,
                tmp_path / "artifacts",
                after_submit=lambda: (_ for _ in ()).throw(_SimulatedCrash()),
            ).tick()
    finally:
        first_connection.close()

    provider = sqlite3.connect(tmp_path / "mock-executor.db")
    try:
        provider.execute("DELETE FROM mock_executor_runs")
        provider.commit()
    finally:
        provider.close()

    restarted = connect(database_path)
    try:
        runner = _runner(
            restarted,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(seconds=31),
        )
        assert runner.tick() == 1
        assert runner.tick() == 0
        assert restarted.execute(
            """SELECT external_run_id, external_status, current_stage, error_code
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            None,
            None,
            "reconciliation_required",
            "APPROVAL_EXPIRED_LOOKUP_FAILED",
        )
    finally:
        restarted.close()

    assert executor.submit_call_count == 1
    assert executor.lookup_call_count == 1


def test_approval_expiring_between_claim_and_submit_never_starts_external_work(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "approval-expiry-claim-race.db"
    connection, _snapshot_hash, _context = _open_seeded(database_path)
    try:
        _prepare_awaiting_job(connection, _plan(
            executor_name="mock",
            model_ids=("writer-model-v1",),
            provider_ids=("mock-provider",),
        ))
        _bind_persisted_paid_approval(
            connection,
            approved_at=NOW - timedelta(seconds=1),
            expires_at=NOW + timedelta(seconds=1),
        )
    finally:
        connection.close()

    calls = iter((NOW, NOW, NOW + timedelta(seconds=2)))
    executor = MockExecutor(clock=lambda: NOW)
    reopened = connect(database_path)
    try:
        runner = _runner(
            reopened,
            executor,
            tmp_path / "artifacts",
            clock=lambda: next(calls, NOW + timedelta(seconds=2)),
        )
        assert runner.tick() == 1
        assert runner.tick() == 0
        assert reopened.execute(
            """SELECT external_run_id, external_status, current_stage, error_code
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            None,
            None,
            "reconciliation_required",
            "APPROVAL_EXPIRED_LOOKUP_FAILED",
        )
    finally:
        reopened.close()

    assert executor.submit_call_count == 0
    assert executor.lookup_call_count == 1


def test_restart_after_remote_acceptance_recovers_same_run_without_duplicate_execution(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "crash.db"
    _queue_approved_job(database_path)
    first_connection = connect(database_path)
    executor = MockExecutor(clock=lambda: NOW, run_id_factory=lambda _number: "external-one")

    def crash_after_submit() -> None:
        raise _SimulatedCrash

    try:
        try:
            _runner(
                first_connection,
                executor,
                tmp_path / "artifacts",
                after_submit=crash_after_submit,
            ).tick()
        except _SimulatedCrash:
            pass
        else:
            raise AssertionError("simulated crash was not raised")
        assert not first_connection.in_transaction
        assert first_connection.execute(
            "SELECT state, current_stage FROM jobs WHERE job_id = ?", ("job-one",)
        ).fetchone() == ("RUNNING", "dispatching")
        assert first_connection.execute(
            """SELECT submission_attempted_at, external_run_id
               FROM job_execution_runs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == (NOW.isoformat(), None)
        assert first_connection.execute(
            """SELECT from_state, to_state FROM job_transitions
               WHERE job_id = ? ORDER BY transition_id DESC LIMIT 1""",
            ("job-one",),
        ).fetchone() == ("QUEUED", "RUNNING")
    finally:
        first_connection.close()

    restarted_connection = connect(database_path)
    try:
        assert _runner(
            restarted_connection,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(seconds=31),
        ).tick() == 1
        assert JobService(
            restarted_connection, company_id="company-one"
        ).get_job("job-one").state is JobState.RUNNING
        assert restarted_connection.execute(
            "SELECT external_run_id FROM job_execution_runs WHERE job_id = ?",
            ("job-one",),
        ).fetchone() == ("external-one",)
    finally:
        restarted_connection.close()

    assert executor.submission_count == 1


def test_stale_running_without_execution_ledger_is_marked_for_reconciliation_not_resubmitted(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "stale.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    try:
        JobService(connection, company_id="company-one", clock=lambda: NOW).transition(
            "job-one", JobState.QUEUED, JobState.RUNNING, "simulate pre-runner active job"
        )
    finally:
        connection.close()

    reopened = connect(database_path)
    executor = MockExecutor(clock=lambda: NOW)
    stale_time = NOW + timedelta(minutes=10)
    try:
        assert _runner(
            reopened,
            executor,
            tmp_path / "artifacts",
            now=stale_time,
        ).tick() == 1
        assert JobService(reopened, company_id="company-one").get_job("job-one").state is JobState.RUNNING
        assert reopened.execute(
            "SELECT current_stage FROM jobs WHERE job_id = ?", ("job-one",)
        ).fetchone() == ("reconciliation_required",)
        assert reopened.execute(
            """SELECT submission_attempted_at, external_run_id, reconciliation_count,
                      current_stage, lease_token
               FROM job_execution_runs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == (None, None, 1, "reconciliation_required", None)
    finally:
        reopened.close()

    assert executor.submission_count == 0


def test_legacy_invalid_scheduler_timestamp_is_quarantined_before_due_query(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "legacy-invalid-scheduler.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _TransactionCheckingExecutor(connection)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            """UPDATE job_execution_runs
               SET next_action_at = 'not-a-time'
               WHERE company_id = ? AND job_id = ? AND attempt = ?""",
            ("company-one", "job-one", 1),
        )
        connection.commit()
        connection.execute("PRAGMA ignore_check_constraints = OFF")

        assert runner.tick() == 1
        assert connection.execute(
            """SELECT current_stage, next_action_at, lease_token,
                      lease_expires_at, error_code
               FROM job_execution_runs
               WHERE company_id = ? AND job_id = ? AND attempt = ?""",
            ("company-one", "job-one", 1),
        ).fetchone() == (
            "reconciliation_required",
            None,
            None,
            None,
            "SCHEDULER_TIMESTAMP_INVALID",
        )
        assert executor.poll_call_count == 0
        assert runner.tick() == 0
    finally:
        connection.execute("PRAGMA ignore_check_constraints = OFF")
        connection.close()


def test_malformed_persisted_external_identity_is_quarantined_without_poll(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "malformed-external-identity.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _TransactionCheckingExecutor(connection)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        connection.execute("DROP TRIGGER job_execution_runs_external_identity_immutable")
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            """UPDATE job_execution_runs
               SET external_accepted_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?""",
            (NOW.replace(tzinfo=None).isoformat(), "company-one", "job-one", 1),
        )
        connection.commit()
        connection.execute("PRAGMA ignore_check_constraints = OFF")

        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT current_stage, error_code, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "reconciliation_required",
            "EXTERNAL_ACCEPTANCE_PROVENANCE_INVALID",
            None,
        )
        assert executor.poll_call_count == 0
    finally:
        connection.execute("PRAGMA ignore_check_constraints = OFF")
        connection.close()


def test_poll_success_publishes_and_binds_immutable_artifact_bundle(tmp_path: Path) -> None:
    database_path = tmp_path / "success.db"
    artifact_root = tmp_path / "artifacts"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _TransactionCheckingExecutor(
        connection,
        outcomes=(
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.SUCCEEDED,
                stage_id="complete",
                retry_after_seconds=None,
                error_code=None,
                error_summary=None,
                result=_execution_result(),
            ),
        ),
    )
    try:
        runner = _runner(connection, executor, artifact_root)
        assert runner.tick() == 1
        assert runner.tick() == 1
        succeeded = JobService(connection, company_id="company-one").get_job("job-one")
        assert succeeded.state is JobState.SUCCEEDED
        assert succeeded.artifact_manifest_path == str(
            artifact_root / "companies" / "company-one" / "jobs" / "job-one" / "manifest.json"
        )
        assert connection.execute(
            """SELECT external_status, current_stage, completion_observed_at,
                      next_action_at, lease_token
               FROM job_execution_runs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == ("SUCCEEDED", "complete", NOW.isoformat(), None, None)
        assert not connection.in_transaction
    finally:
        connection.close()
    assert (artifact_root / "companies" / "company-one" / "jobs" / "job-one" / "content.md").is_file()


def test_executor_name_change_before_poll_is_quarantined_without_poll(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "executor-name-change-poll.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _TransactionCheckingExecutor(connection)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        executor.name = "wrong-executor"
        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT external_status, current_stage, error_code
               FROM job_execution_runs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == (
            "ACCEPTED",
            "reconciliation_required",
            "EXECUTOR_MISMATCH",
        )
    finally:
        connection.close()

    assert executor.poll_call_count == 0


def test_running_poll_persists_stage_and_respects_remote_poll_deadline(tmp_path: Path) -> None:
    database_path = tmp_path / "running.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _TransactionCheckingExecutor(
        connection,
        outcomes=(
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.RUNNING,
                stage_id="research",
                retry_after_seconds=10**10,
                error_code=None,
                error_summary=None,
                result=None,
            ),
        ),
    )
    try:
        assert _runner(connection, executor, tmp_path / "artifacts").tick() == 1
        assert _runner(connection, executor, tmp_path / "artifacts").tick() == 1
        running = JobService(connection, company_id="company-one").get_job("job-one")
        assert running.state is JobState.RUNNING
        assert running.current_stage == "research"
        assert connection.execute(
            """SELECT external_status, current_stage, next_action_at
               FROM job_execution_runs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == (
            "RUNNING",
            "research",
            (NOW + timedelta(seconds=60)).isoformat(),
        )
        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(seconds=2),
        ).tick() == 0
    finally:
        connection.close()


def test_transient_submit_error_retries_same_attempt_and_key(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "submit-retry.db"
    _queue_approved_job(database_path, maximum_retries=2)
    connection = connect(database_path)
    executor = MockExecutor(
        submit_errors=(
            ExecutorError(
                error_code="HTTP_429",
                error_summary="submission rate limited",
                retry_after_seconds=3,
            ),
        ),
        clock=lambda: NOW,
        run_id_factory=lambda _number: "external-one",
    )
    policy = RetryPolicy(
        base_delay_seconds=2,
        maximum_delay_seconds=30,
        jitter=lambda delay: delay,
    )
    try:
        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts",
            retry_policy=policy,
        ).tick() == 1
        running = JobService(connection, company_id="company-one").get_job("job-one")
        assert running.state is JobState.RUNNING
        assert running.current_stage == "dispatching"
        assert running.attempt == 1
        assert connection.execute(
            """SELECT idempotency_key, external_run_id, external_status,
                      next_action_at, transient_failure_count, error_code
               FROM job_execution_runs WHERE job_id = ? AND attempt = 1""",
            ("job-one",),
        ).fetchone() == (
            "company-one:job-one:1",
            None,
            None,
            (NOW + timedelta(seconds=3)).isoformat(),
            1,
            "HTTP_429",
        )
        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(seconds=2),
            retry_policy=policy,
        ).tick() == 0
        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(seconds=3),
            retry_policy=policy,
        ).tick() == 1
        assert JobService(connection, company_id="company-one").get_job(
            "job-one"
        ).state is JobState.RUNNING
        assert connection.execute(
            """SELECT idempotency_key, external_run_id, external_status
               FROM job_execution_runs WHERE job_id = ? AND attempt = 1""",
            ("job-one",),
        ).fetchone() == ("company-one:job-one:1", "external-one", "ACCEPTED")
        assert executor.submission_count == 1
        assert executor.submit_call_count == 2
    finally:
        connection.close()


def test_runner_poll_timeout_is_retryable_and_resets_after_success(tmp_path: Path) -> None:
    database_path = tmp_path / "poll-deadline-retry.db"
    _queue_approved_job(database_path, maximum_retries=1)
    connection = connect(database_path)
    executor = _TimeoutOnceAfterPollExecutor(
        outcomes=(
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.SUCCEEDED,
                stage_id="complete",
                retry_after_seconds=None,
                error_code=None,
                error_summary=None,
                result=_execution_result(),
            ),
        ),
        clock=lambda: NOW,
        run_id_factory=lambda _number: "external-one",
    )
    try:
        runner = _runner(
            connection,
            executor,
            tmp_path / "artifacts-poll-deadline",
            lease_duration=timedelta(seconds=1),
            external_call_timeout=timedelta(milliseconds=50),
            lease_safety_margin=timedelta(milliseconds=100),
        )
        assert runner.tick() == 1
        assert runner.tick() == 1
        assert connection.execute(
            """SELECT external_status, error_code, transient_failure_count
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("ACCEPTED", "NETWORK_ERROR", 1)
        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts-poll-deadline",
            now=NOW + timedelta(seconds=1),
            lease_duration=timedelta(seconds=1),
            external_call_timeout=timedelta(milliseconds=50),
            lease_safety_margin=timedelta(milliseconds=100),
        ).tick() == 1
        assert connection.execute(
            """SELECT external_status, error_code, transient_failure_count
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("SUCCEEDED", None, 0)
    finally:
        connection.close()


def test_successful_progress_in_same_stage_does_not_reset_retry_budget(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "same-stage-budget.db"
    _queue_approved_job(database_path, maximum_retries=1)
    connection = connect(database_path)
    transient = ExecutorError(
        error_code="HTTP_503",
        error_summary="retryable provider error",
    )
    executor = _TransactionCheckingExecutor(
        connection,
        outcomes=(
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.RUNNING,
                stage_id="research",
                retry_after_seconds=1,
                error_code=None,
                error_summary=None,
                result=None,
            ),
            transient,
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.RUNNING,
                stage_id="research",
                retry_after_seconds=1,
                error_code=None,
                error_summary=None,
                result=None,
            ),
        ),
    )
    try:
        assert _runner(connection, executor, tmp_path / "same-stage-artifacts").tick() == 1
        assert _runner(connection, executor, tmp_path / "same-stage-artifacts").tick() == 1
        assert _runner(
            connection,
            executor,
            tmp_path / "same-stage-artifacts",
            now=NOW + timedelta(seconds=1),
        ).tick() == 1
        assert _runner(
            connection,
            executor,
            tmp_path / "same-stage-artifacts",
            now=NOW + timedelta(seconds=2),
        ).tick() == 1
        assert connection.execute(
            """SELECT current_stage, transient_failure_count
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("research", 1)
    finally:
        connection.close()


def test_stage_retry_budget_survives_progress_through_other_stages(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "interleaved-stage-budget.db"
    _queue_approved_job(database_path, maximum_retries=1)
    connection = connect(database_path)
    transient = ExecutorError(
        error_code="HTTP_503",
        error_summary="retryable provider error",
    )
    executor = _TransactionCheckingExecutor(
        connection,
        outcomes=(
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.RUNNING,
                stage_id="stage-a",
                retry_after_seconds=1,
                error_code=None,
                error_summary=None,
                result=None,
            ),
            transient,
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.RUNNING,
                stage_id="stage-b",
                retry_after_seconds=1,
                error_code=None,
                error_summary=None,
                result=None,
            ),
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.RUNNING,
                stage_id="stage-a",
                retry_after_seconds=1,
                error_code=None,
                error_summary=None,
                result=None,
            ),
            transient,
        ),
    )
    try:
        runner_path = tmp_path / "interleaved-stage-artifacts"
        assert _runner(connection, executor, runner_path).tick() == 1
        assert _runner(connection, executor, runner_path).tick() == 1
        for seconds in range(1, 5):
            assert _runner(
                connection,
                executor,
                runner_path,
                now=NOW + timedelta(seconds=seconds),
            ).tick() == 1

        assert connection.execute(
            """SELECT current_stage, next_action_at, error_code
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("reconciliation_required", None, "HTTP_503")
        assert connection.execute(
            """SELECT retry_stage_id, failure_count
               FROM job_stage_retry_budgets ORDER BY retry_stage_id"""
        ).fetchall() == [("poll:stage-a", 1)]
    finally:
        connection.close()


def test_transient_poll_error_retries_same_run_without_stopping_worker(tmp_path: Path) -> None:
    database_path = tmp_path / "poll-retry.db"
    _queue_approved_job(database_path, maximum_retries=2)
    connection = connect(database_path)
    executor = _TransactionCheckingExecutor(
        connection,
        outcomes=(
            ExecutorError(
                error_code="HTTP_503",
                error_summary="status endpoint unavailable",
                retry_after_seconds=3,
            ),
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.SUCCEEDED,
                stage_id="complete",
                retry_after_seconds=None,
                error_code=None,
                error_summary=None,
                result=_execution_result(),
            ),
        ),
    )
    policy = RetryPolicy(base_delay_seconds=2, maximum_delay_seconds=30, jitter=lambda delay: delay)
    try:
        assert _runner(connection, executor, tmp_path / "artifacts", retry_policy=policy).tick() == 1
        assert _runner(connection, executor, tmp_path / "artifacts", retry_policy=policy).tick() == 1
        running = JobService(connection, company_id="company-one").get_job("job-one")
        assert running.state is JobState.RUNNING
        assert connection.execute(
            """SELECT external_status, next_action_at, transient_failure_count, error_code
               FROM job_execution_runs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == (
            "ACCEPTED",
            (NOW + timedelta(seconds=3)).isoformat(),
            1,
            "HTTP_503",
        )
        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(seconds=3),
            retry_policy=policy,
        ).tick() == 1
        assert JobService(connection, company_id="company-one").get_job("job-one").state is JobState.SUCCEEDED
        assert executor.submission_count == 1
        assert executor.poll_call_count == 2
    finally:
        connection.close()


def test_exhausted_submit_transport_error_requires_reconciliation(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "submit-exhausted.db"
    _queue_approved_job(database_path, maximum_retries=0)
    connection = connect(database_path)
    executor = MockExecutor(
        submit_errors=(
            ExecutorError(
                error_code="HTTP_503",
                error_summary="submission endpoint unavailable",
            ),
        ),
        clock=lambda: NOW,
    )
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        assert runner.tick() == 0
        job = JobService(connection, company_id="company-one").get_job("job-one")
        assert job.state is JobState.RUNNING
        assert job.current_stage == "reconciliation_required"
        assert connection.execute(
            """SELECT idempotency_key, external_run_id, external_status,
                      current_stage, error_code, lease_token
               FROM job_execution_runs WHERE job_id = ? AND attempt = 1""",
            ("job-one",),
        ).fetchone() == (
            "company-one:job-one:1",
            None,
            None,
            "reconciliation_required",
            "HTTP_503",
            None,
        )
        assert executor.submit_call_count == 1
        assert executor.submission_count == 0
    finally:
        connection.close()


def test_exhausted_poll_transport_retries_require_reconciliation(tmp_path: Path) -> None:
    database_path = tmp_path / "poll-exhausted.db"
    _queue_approved_job(database_path, maximum_retries=1)
    connection = connect(database_path)
    executor = _TransactionCheckingExecutor(
        connection,
        outcomes=(
            ExecutorError(
                error_code="HTTP_503",
                error_summary="status endpoint unavailable",
                retry_after_seconds=2,
            ),
            ExecutorError(
                error_code="HTTP_503",
                error_summary="status endpoint still unavailable",
                retry_after_seconds=2,
            ),
        ),
    )
    policy = RetryPolicy(base_delay_seconds=1, maximum_delay_seconds=30, jitter=lambda delay: delay)
    try:
        assert _runner(connection, executor, tmp_path / "artifacts", retry_policy=policy).tick() == 1
        assert _runner(connection, executor, tmp_path / "artifacts", retry_policy=policy).tick() == 1
        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(seconds=2),
            retry_policy=policy,
        ).tick() == 1
        running = JobService(connection, company_id="company-one").get_job("job-one")
        assert running.state is JobState.RUNNING
        assert running.current_stage == "reconciliation_required"
        assert connection.execute(
            """SELECT external_status, current_stage, next_action_at,
                      transient_failure_count, reconciliation_count, error_code
               FROM job_execution_runs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == (
            "ACCEPTED",
            "reconciliation_required",
            None,
            2,
            1,
            "HTTP_503",
        )
        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(minutes=10),
            retry_policy=policy,
        ).tick() == 0
        assert executor.poll_call_count == 2
        assert executor.submission_count == 1
    finally:
        connection.close()


def test_transient_poll_failure_waits_until_due_then_requeues_next_attempt(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "retry.db"
    _queue_approved_job(database_path, maximum_retries=2)
    connection = connect(database_path)
    accepted_times = iter((NOW, NOW + timedelta(seconds=4)))
    executor = _TransactionCheckingExecutor(
        connection,
        outcomes=(
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.FAILED_RETRYABLE,
                stage_id="research",
                retry_after_seconds=None,
                error_code="HTTP_503",
                error_summary="temporary upstream failure",
                result=None,
            ),
            ExecutionStatus(
                external_run_id="external-two",
                status=ExternalStatus.SUCCEEDED,
                stage_id="complete",
                retry_after_seconds=None,
                error_code=None,
                error_summary=None,
                result=_execution_result(),
            ),
        ),
        clock=lambda: next(accepted_times),
    )
    policy = RetryPolicy(base_delay_seconds=4, maximum_delay_seconds=30, jitter=lambda delay: delay)
    try:
        assert _runner(connection, executor, tmp_path / "artifacts", retry_policy=policy).tick() == 1
        assert _runner(connection, executor, tmp_path / "artifacts", retry_policy=policy).tick() == 1
        failed = JobService(connection, company_id="company-one").get_job("job-one")
        assert failed.state is JobState.FAILED_RETRYABLE
        assert failed.attempt == 1
        assert connection.execute(
            """SELECT external_status, current_stage, next_action_at
               FROM job_execution_runs WHERE job_id = ? AND attempt = 1""",
            ("job-one",),
        ).fetchone() == (
            "FAILED_RETRYABLE",
            "research",
            (NOW + timedelta(seconds=4)).isoformat(),
        )

        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(seconds=3),
            retry_policy=policy,
        ).tick() == 0
        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(seconds=4),
            retry_policy=policy,
        ).tick() == 1
        queued = JobService(connection, company_id="company-one").get_job("job-one")
        assert queued.state is JobState.QUEUED
        assert queued.attempt == 2
        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(seconds=4),
            retry_policy=policy,
        ).tick() == 1
        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(seconds=4),
            retry_policy=policy,
        ).tick() == 1
        succeeded = JobService(connection, company_id="company-one").get_job("job-one")
        assert succeeded.state is JobState.SUCCEEDED
        assert succeeded.attempt == 2
        assert connection.execute(
            """SELECT attempt, idempotency_key, external_run_id
               FROM job_execution_runs ORDER BY attempt"""
        ).fetchall() == [
            (1, "company-one:job-one:1", "external-one"),
            (2, "company-one:job-one:2", "external-two"),
        ]
        assert not connection.in_transaction
    finally:
        connection.close()
    assert executor.submission_count == 2


def test_provider_retry_budget_survives_requeue_for_same_stage(tmp_path: Path) -> None:
    database_path = tmp_path / "provider-stage-budget.db"
    _queue_approved_job(database_path, maximum_retries=1)
    connection = connect(database_path)
    accepted_times = iter((NOW, NOW + timedelta(seconds=1)))
    executor = _TransactionCheckingExecutor(
        connection,
        outcomes=(
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.FAILED_RETRYABLE,
                stage_id="research",
                retry_after_seconds=1,
                error_code="HTTP_503",
                error_summary="temporary upstream failure",
                result=None,
            ),
            ExecutionStatus(
                external_run_id="external-two",
                status=ExternalStatus.FAILED_RETRYABLE,
                stage_id="research",
                retry_after_seconds=1,
                error_code="HTTP_503",
                error_summary="temporary upstream failure again",
                result=None,
            ),
        ),
        clock=lambda: next(accepted_times),
    )
    try:
        assert _runner(connection, executor, tmp_path / "provider-budget-artifacts").tick() == 1
        assert _runner(connection, executor, tmp_path / "provider-budget-artifacts").tick() == 1
        for _step in range(3):
            assert _runner(
                connection,
                executor,
                tmp_path / "provider-budget-artifacts",
                now=NOW + timedelta(seconds=1),
            ).tick() == 1

        job = JobService(connection, company_id="company-one").get_job("job-one")
        assert job.state is JobState.FAILED_FINAL
        assert connection.execute(
            """SELECT attempt, retry_stage_id, transient_failure_count, external_status
               FROM job_execution_runs ORDER BY attempt"""
        ).fetchall() == [
            (1, "poll:research", 1, "FAILED_RETRYABLE"),
            (2, "poll:research", 1, "FAILED_FINAL"),
        ]
    finally:
        connection.close()


def test_non_retryable_poll_failure_transitions_directly_to_failed_final(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "final.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _TransactionCheckingExecutor(
        connection,
        outcomes=(
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.FAILED_FINAL,
                stage_id="writer",
                retry_after_seconds=None,
                error_code="MODEL_OUTPUT_INVALID",
                error_summary="result schema is invalid",
                result=None,
            ),
        ),
    )
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        assert runner.tick() == 1
        failed = JobService(connection, company_id="company-one").get_job("job-one")
        assert failed.state is JobState.FAILED_FINAL
        assert failed.error_code == "MODEL_OUTPUT_INVALID"
        assert failed.error_summary == "result schema is invalid"
        assert connection.execute(
            "SELECT external_status, current_stage FROM job_execution_runs WHERE job_id = ?",
            ("job-one",),
        ).fetchone() == ("FAILED_FINAL", "writer")
        assert not connection.in_transaction
    finally:
        connection.close()


def test_restart_finalizes_durable_success_without_repolling_external_executor(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "durable-success.db"
    artifact_root = tmp_path / "artifacts"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = MockExecutor(clock=lambda: NOW, run_id_factory=lambda _number: "external-one")
    try:
        assert _runner(connection, executor, artifact_root).tick() == 1
        payload = execution_result_bytes(_execution_result())
        connection.execute(
            """UPDATE job_execution_runs
               SET external_status = 'SUCCEEDED', current_stage = 'complete',
                   completion_observed_at = ?, result_json = ?, result_hash = ?,
                   next_action_at = NULL, updated_at = ?
               WHERE job_id = ?""",
            (
                NOW.isoformat(),
                payload,
                hashlib.sha256(payload).hexdigest(),
                NOW.isoformat(),
                "job-one",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    restarted = connect(database_path)
    try:
        runner = Runner(
            restarted,
            executor=_NoExternalIoExecutor(),
            artifact_store=ArtifactStore(artifact_root, clock=lambda: NOW),
            clock=lambda: NOW,
            runner_id="runner-restarted",
            lease_token_factory=lambda: "lease-restarted",
        )
        assert runner.tick() == 1
        succeeded = JobService(restarted, company_id="company-one").get_job("job-one")
        assert succeeded.state is JobState.SUCCEEDED
        assert succeeded.artifact_manifest_path is not None
    finally:
        restarted.close()


def test_restart_rejects_durable_success_with_unapproved_model_usage(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "durable-usage-mismatch.db"
    artifact_root = tmp_path / "artifacts"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = MockExecutor(clock=lambda: NOW, run_id_factory=lambda _number: "external-one")
    try:
        assert _runner(connection, executor, artifact_root).tick() == 1
        result = _execution_result()
        object.__setattr__(
            result,
            "model_usage",
            {
                "models": [
                    {
                        "model_id": "unapproved-model",
                        "provider_id": "unapproved-provider",
                        "input_tokens": 1,
                        "output_tokens": 1,
                    }
                ]
            },
        )
        payload = execution_result_bytes(result)
        connection.execute(
            """UPDATE job_execution_runs
               SET external_status = 'SUCCEEDED', current_stage = 'complete',
                   completion_observed_at = ?, result_json = ?, result_hash = ?,
                   next_action_at = NULL, updated_at = ? WHERE job_id = ?""",
            (
                NOW.isoformat(),
                payload,
                hashlib.sha256(payload).hexdigest(),
                NOW.isoformat(),
                "job-one",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    restarted = connect(database_path)
    try:
        runner = Runner(
            restarted,
            executor=_NoExternalIoExecutor(),
            artifact_store=ArtifactStore(artifact_root, clock=lambda: NOW),
            clock=lambda: NOW,
            runner_id="runner-restarted",
            lease_token_factory=lambda: "lease-restarted",
        )
        assert runner.tick() == 1
        assert JobService(restarted, company_id="company-one").get_job(
            "job-one"
        ).state is JobState.RUNNING
        assert restarted.execute(
            """SELECT current_stage, error_code FROM job_execution_runs
               WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("reconciliation_required", "TERMINAL_PROVENANCE_INVALID")
        assert not artifact_root.exists()
    finally:
        restarted.close()


def test_restart_finalizes_durable_failure_without_repolling_external_executor(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "durable-failure.db"
    artifact_root = tmp_path / "artifacts"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = MockExecutor(clock=lambda: NOW, run_id_factory=lambda _number: "external-one")
    try:
        assert _runner(connection, executor, artifact_root).tick() == 1
        connection.execute(
            """UPDATE job_execution_runs
               SET external_status = 'FAILED_FINAL', current_stage = 'writer',
                   completion_observed_at = ?, error_code = 'MODEL_OUTPUT_INVALID',
                   error_summary = 'result schema is invalid', next_action_at = NULL,
                   updated_at = ?
               WHERE job_id = ?""",
            (NOW.isoformat(), NOW.isoformat(), "job-one"),
        )
        connection.commit()
    finally:
        connection.close()

    restarted = connect(database_path)
    try:
        runner = Runner(
            restarted,
            executor=_NoExternalIoExecutor(),
            artifact_store=ArtifactStore(artifact_root, clock=lambda: NOW),
            clock=lambda: NOW,
            runner_id="runner-restarted",
            lease_token_factory=lambda: "lease-restarted",
        )
        assert runner.tick() == 1
        failed = JobService(restarted, company_id="company-one").get_job("job-one")
        assert failed.state is JobState.FAILED_FINAL
        assert failed.error_code == "MODEL_OUTPUT_INVALID"
        assert failed.error_summary == "result schema is invalid"
    finally:
        restarted.close()


def test_corrupt_durable_terminal_result_is_quarantined_without_repoll(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "corrupt-terminal-result.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    try:
        executor = _TransactionCheckingExecutor(connection)
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        payload = execution_result_bytes(_execution_result())
        connection.execute(
            """UPDATE job_execution_runs
               SET external_status = 'SUCCEEDED', current_stage = 'complete',
                   completion_observed_at = ?, result_json = ?, result_hash = ?,
                   next_action_at = NULL, updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?""",
            (
                NOW.isoformat(),
                payload,
                hashlib.sha256(payload).hexdigest(),
                NOW.isoformat(),
                "company-one",
                "job-one",
                1,
            ),
        )
        connection.commit()
        connection.execute("DROP TRIGGER job_execution_runs_result_immutable")
        connection.execute(
            """UPDATE job_execution_runs
               SET result_hash = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?""",
            ("f" * 64, "company-one", "job-one", 1),
        )
        connection.commit()

        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT current_stage, error_code, error_summary, lease_token
               FROM job_execution_runs
               WHERE company_id = ? AND job_id = ? AND attempt = ?""",
            ("company-one", "job-one", 1),
        ).fetchone() == (
            "reconciliation_required",
            "TERMINAL_PROVENANCE_INVALID",
            "durable terminal result requires reconciliation",
            None,
        )
        assert executor.poll_call_count == 0
    finally:
        connection.close()


def test_terminal_recovery_quarantines_naive_external_acceptance_timestamp(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "terminal-naive-acceptance.db"
    artifact_root = tmp_path / "artifacts"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    try:
        executor = _TransactionCheckingExecutor(connection)
        runner = _runner(connection, executor, artifact_root)
        assert runner.tick() == 1
        payload = execution_result_bytes(_execution_result())
        connection.execute(
            """UPDATE job_execution_runs
               SET external_status = 'SUCCEEDED', current_stage = 'complete',
                   completion_observed_at = ?, result_json = ?, result_hash = ?,
                   next_action_at = NULL, updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?""",
            (
                NOW.isoformat(),
                payload,
                hashlib.sha256(payload).hexdigest(),
                NOW.isoformat(),
                "company-one",
                "job-one",
                1,
            ),
        )
        connection.commit()
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute("DROP TRIGGER job_execution_runs_external_identity_immutable")
        connection.execute(
            """UPDATE job_execution_runs SET external_accepted_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?""",
            (NOW.replace(tzinfo=None).isoformat(), "company-one", "job-one", 1),
        )
        connection.commit()
        connection.execute("PRAGMA ignore_check_constraints = OFF")

        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT current_stage, error_code FROM job_execution_runs
               WHERE company_id = ? AND job_id = ? AND attempt = ?""",
            ("company-one", "job-one", 1),
        ).fetchone() == (
            "reconciliation_required",
            "TERMINAL_PROVENANCE_INVALID",
        )
        assert not (
            artifact_root / "companies" / "company-one" / "jobs" / "job-one"
        ).exists()
    finally:
        connection.close()


def test_terminal_recovery_lease_excludes_a_second_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "terminal-recovery-race.db"
    artifact_root = tmp_path / "artifacts"
    _queue_approved_job(database_path)
    setup_connection = connect(database_path)
    executor = MockExecutor(clock=lambda: NOW, run_id_factory=lambda _number: "external-one")
    try:
        assert _runner(setup_connection, executor, artifact_root).tick() == 1
        setup_connection.execute(
            """UPDATE job_execution_runs
               SET external_status = 'FAILED_FINAL', current_stage = 'writer',
                   completion_observed_at = ?, error_code = 'MODEL_OUTPUT_INVALID',
                   error_summary = 'result schema is invalid', next_action_at = NULL,
                   updated_at = ?
               WHERE job_id = ?""",
            (NOW.isoformat(), NOW.isoformat(), "job-one"),
        )
        setup_connection.commit()
    finally:
        setup_connection.close()

    first_connection = connect(database_path)
    second_connection = connect(database_path)
    first_runner = Runner(
        first_connection,
        executor=_NoExternalIoExecutor(),
        artifact_store=ArtifactStore(artifact_root, clock=lambda: NOW),
        clock=lambda: NOW,
        runner_id="runner-first",
        lease_token_factory=lambda: "lease-first",
    )
    second_runner = Runner(
        second_connection,
        executor=_NoExternalIoExecutor(),
        artifact_store=ArtifactStore(artifact_root, clock=lambda: NOW),
        clock=lambda: NOW,
        runner_id="runner-second",
        lease_token_factory=lambda: "lease-second",
    )
    original_transition = JobService.transition
    competing_ticks: list[int] = []
    entered = False

    def race_transition(self, *args, **kwargs):
        nonlocal entered
        if not entered:
            entered = True
            competing_ticks.append(second_runner.tick())
        return original_transition(self, *args, **kwargs)

    monkeypatch.setattr(JobService, "transition", race_transition)
    try:
        assert first_runner.tick() == 1
        assert competing_ticks == [0]
        failed = JobService(first_connection, company_id="company-one").get_job("job-one")
        assert failed.state is JobState.FAILED_FINAL
        assert first_connection.execute(
            "SELECT lease_token FROM job_execution_runs WHERE job_id = ?",
            ("job-one",),
        ).fetchone() == (None,)
    finally:
        first_connection.close()
        second_connection.close()


def test_worker_loop_handles_sigterm_after_current_transition_and_records_heartbeat(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "worker-loop.db"
    artifact_root = tmp_path / "artifacts"
    marker_path = tmp_path / "submit-started"
    _queue_approved_job(database_path)
    script = """
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from seo_orchestrator.cli import run_worker
from seo_orchestrator.executors.base import ExecutionStatus, ExternalRun, submission_idempotency_key
from seo_orchestrator.settings import Settings


class SlowExecutor:
    name = "mock"
    model_ids = ("writer-model-v1",)
    provider_ids = ("mock-provider",)
    durable_semantic_idempotency = True
    side_effect_free_lookup = True
    idempotent_cancel = True
    cancel_confirms_terminal = True
    authority_deadline_enforced = True
    configuration_authorization_enforced = True

    def __init__(self, marker: Path) -> None:
        self.marker = marker

    def submit(self, job, snapshot):
        del snapshot
        self.marker.write_text("started", encoding="ascii")
        time.sleep(0.4)
        return ExternalRun(
            external_run_id="external-sigterm",
            idempotency_key=submission_idempotency_key(job),
            accepted_at=datetime.now(UTC),
        )

    def submit_authorized(
        self,
        job,
        snapshot,
        *,
        authority_expires_at,
        approved_model_ids,
        approved_provider_ids,
    ):
        if authority_expires_at is not None and datetime.now(UTC) >= authority_expires_at:
            raise AssertionError("submit started after authority expiry")
        if self.model_ids != approved_model_ids or self.provider_ids != approved_provider_ids:
            raise AssertionError("submit configuration is not approved")
        return self.submit(job, snapshot)

    def lookup(self, job, snapshot):
        raise AssertionError(f"unexpected lookup: {job.job_id}:{snapshot.snapshot_id}")

    def poll(self, run) -> ExecutionStatus:
        raise AssertionError(f"unexpected poll: {run.external_run_id}")

    def cancel(self, run) -> None:
        raise AssertionError(f"unexpected cancel: {run.external_run_id}")


root = Path(sys.argv[1])
run_worker(
    Settings(
        environment="test",
        db_path=Path(sys.argv[2]),
        artifact_root=Path(sys.argv[3]),
        listen="unix:/run/seo-orchestrator/test-worker.sock",
        api_token_path=root / "api.token",
        callback_hmac_key_path=root / "callback.key",
    ),
    executor=SlowExecutor(Path(sys.argv[4])),
    poll_interval_seconds=0.01,
    runner_id="sigterm-runner",
)
"""
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            script,
            str(tmp_path),
            str(database_path),
            str(artifact_root),
            str(marker_path),
        ],
        cwd=Path(__file__).parents[2],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not marker_path.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker_path.exists()
        os.kill(process.pid, signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)

    assert process.returncode == 0, (stdout, stderr)
    connection = connect(database_path)
    try:
        assert JobService(connection, company_id="company-one").get_job("job-one").state is JobState.RUNNING
        assert connection.execute(
            """SELECT external_run_id, external_status, lease_token
               FROM job_execution_runs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == ("external-sigterm", "ACCEPTED", None)
        assert connection.execute(
            "SELECT COUNT(*) FROM runner_heartbeats WHERE runner_id = ?",
            ("sigterm-runner",),
        ).fetchone() == (1,)
        assert not connection.in_transaction
    finally:
        connection.close()
