"""Cooperative and restart-safe runner cancellation tests."""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from seo_orchestrator.db.connection import connect
from seo_orchestrator.domain import ExecutionSnapshot, JobState, SeoJob
from seo_orchestrator.executors.base import (
    ExecutionStatus,
    ExecutorError,
    ExternalRun,
    ExternalStatus,
)
from seo_orchestrator.executors.mock import MockExecutor
from seo_orchestrator.runner import RetryPolicy
from seo_orchestrator.services.jobs import JobService
from tests.integration.test_approval_invalidation import NOW
from tests.integration.test_runner_recovery import (
    _execution_result,
    _queue_approved_job,
    _runner,
    _SimulatedCrash,
    _TransactionCheckingExecutor,
)


class _CancelDuringSubmitExecutor(MockExecutor):
    def __init__(self, database_path: Path) -> None:
        super().__init__(
            clock=lambda: NOW,
            run_id_factory=lambda _number: "external-one",
        )
        self._database_path = database_path

    def submit_authorized(
        self,
        job: SeoJob,
        snapshot: ExecutionSnapshot,
        *,
        authority_expires_at: datetime | None,
        approved_model_ids: tuple[str, ...],
        approved_provider_ids: tuple[str, ...],
    ) -> ExternalRun:
        competing_connection = connect(self._database_path)
        try:
            observed = JobService(
                competing_connection,
                company_id="company-one",
                clock=lambda: NOW,
            ).get_job("job-one")
            assert observed.state is JobState.RUNNING
            canceled = JobService(
                competing_connection,
                company_id="company-one",
                clock=lambda: NOW,
            ).cancel_job("job-one", JobState.RUNNING)
            assert canceled.state is JobState.CANCELED
        finally:
            competing_connection.close()
        return super().submit_authorized(
            job,
            snapshot,
            authority_expires_at=authority_expires_at,
            approved_model_ids=approved_model_ids,
            approved_provider_ids=approved_provider_ids,
        )


class _CancelDuringPollExecutor(MockExecutor):
    def __init__(
        self,
        database_path: Path,
        *,
        status: ExternalStatus = ExternalStatus.SUCCEEDED,
    ) -> None:
        result = _execution_result() if status is ExternalStatus.SUCCEEDED else None
        super().__init__(
            outcomes=(
                ExecutionStatus(
                    external_run_id="external-one",
                    status=status,
                    stage_id="complete" if status is ExternalStatus.SUCCEEDED else "research",
                    retry_after_seconds=None if status is ExternalStatus.SUCCEEDED else 5,
                    error_code=None,
                    error_summary=None,
                    result=result,
                ),
            ),
            clock=lambda: NOW,
            run_id_factory=lambda _number: "external-one",
        )
        self._database_path = database_path

    def poll(self, run: ExternalRun) -> ExecutionStatus:
        competing_connection = connect(self._database_path)
        try:
            canceled = JobService(
                competing_connection,
                company_id="company-one",
                clock=lambda: NOW,
            ).cancel_job("job-one", JobState.RUNNING)
            assert canceled.state is JobState.CANCELED
        finally:
            competing_connection.close()
        return super().poll(run)


class _CancelDuringPollErrorExecutor(MockExecutor):
    def __init__(self, database_path: Path, error: ExecutorError) -> None:
        super().__init__(
            clock=lambda: NOW,
            run_id_factory=lambda _number: "external-one",
        )
        self._database_path = database_path
        self._error = error

    def poll(self, run: ExternalRun) -> ExecutionStatus:
        self.poll_call_count += 1
        competing_connection = connect(self._database_path)
        try:
            canceled = JobService(
                competing_connection,
                company_id="company-one",
                clock=lambda: NOW,
            ).cancel_job("job-one", JobState.RUNNING)
            assert canceled.state is JobState.CANCELED
        finally:
            competing_connection.close()
        raise self._error


class _MalformedCancelOutputExecutor(MockExecutor):
    def cancel(self, run: ExternalRun) -> ExecutionStatus:
        self.cancel_call_count += 1
        raise ValueError(f"malformed cancel response for {run.external_run_id}")


class _NonNoneCancelResultExecutor(MockExecutor):
    def cancel(self, run: ExternalRun) -> ExecutionStatus:
        super().cancel(run)
        return object()  # type: ignore[return-value]


class _TimeoutAfterCancelExecutor(MockExecutor):
    def cancel(self, run: ExternalRun) -> ExecutionStatus:
        confirmation = super().cancel(run)
        time.sleep(1)
        return confirmation


class _NameChangingCancelErrorExecutor(MockExecutor):
    def cancel(self, run: ExternalRun) -> ExecutionStatus:
        del run
        self.name = "wrong-executor"
        raise ExecutorError(
            error_code="HTTP_503",
            error_summary="retryable provider error",
        )


def test_dispatch_intent_linearizes_submit_before_concurrent_cancel(tmp_path: Path) -> None:
    database_path = tmp_path / "submit-cancel-race.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _CancelDuringSubmitExecutor(database_path)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")

        assert runner.tick() == 1
        canceled = JobService(connection, company_id="company-one").get_job("job-one")
        assert canceled.state is JobState.CANCELED
        assert connection.execute(
            """SELECT external_run_id, external_status, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("external-one", "ACCEPTED", None)
        assert connection.execute(
            """SELECT from_state, to_state FROM job_transitions
               WHERE job_id = ? ORDER BY transition_id DESC LIMIT 2""",
            ("job-one",),
        ).fetchall() == [("RUNNING", "CANCELED"), ("QUEUED", "RUNNING")]

        assert runner.tick() == 1
        assert runner.tick() == 0
        assert JobService(connection, company_id="company-one").get_job(
            "job-one"
        ).state is JobState.CANCELED
        assert connection.execute(
            """SELECT external_status, completion_observed_at, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("CANCELED", NOW.isoformat(), None)
    finally:
        connection.close()

    assert executor.submission_count == 1
    assert executor.cancellation_count == 1


def test_late_poll_success_cannot_resurrect_concurrently_canceled_job(tmp_path: Path) -> None:
    database_path = tmp_path / "poll-cancel-race.db"
    artifact_root = tmp_path / "artifacts"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _CancelDuringPollExecutor(database_path)
    try:
        runner = _runner(connection, executor, artifact_root)

        assert runner.tick() == 1
        assert runner.tick() == 1
        canceled = JobService(connection, company_id="company-one").get_job("job-one")
        assert canceled.state is JobState.CANCELED
        assert canceled.artifact_manifest_path is None
        external_status, current_stage, result_hash, lease_token = connection.execute(
            """SELECT external_status, current_stage, result_hash, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone()
        assert external_status == "SUCCEEDED"
        assert current_stage == "cancel_reconciliation_required"
        assert isinstance(result_hash, str) and len(result_hash) == 64
        assert lease_token is None
        assert runner.tick() == 0
    finally:
        connection.close()

    assert executor.cancellation_count == 0
    assert not (artifact_root / "companies" / "company-one" / "jobs" / "job-one").exists()


def test_cancel_between_terminal_observation_and_local_transition_wins(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "terminal-transition-cancel-race.db"
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
    original_transition = JobService.transition
    canceled_once = False

    def cancel_before_success_transition(self, job_id, expected_state, target_state, *args, **kwargs):
        nonlocal canceled_once
        if target_state is JobState.SUCCEEDED and not canceled_once:
            canceled_once = True
            competing_connection = connect(database_path)
            try:
                JobService(
                    competing_connection,
                    company_id="company-one",
                    clock=lambda: NOW,
                ).cancel_job("job-one", JobState.RUNNING)
            finally:
                competing_connection.close()
        return original_transition(
            self,
            job_id,
            expected_state,
            target_state,
            *args,
            **kwargs,
        )

    try:
        runner = _runner(connection, executor, artifact_root)
        assert runner.tick() == 1
        monkeypatch.setattr(JobService, "transition", cancel_before_success_transition)

        assert runner.tick() == 1
        job = JobService(connection, company_id="company-one").get_job("job-one")
        assert job.state is JobState.CANCELED
        assert job.artifact_manifest_path is None
        external_status, current_stage, result_hash = connection.execute(
            """SELECT external_status, current_stage, result_hash
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone()
        assert external_status == "SUCCEEDED"
        assert current_stage == "cancel_reconciliation_required"
        assert isinstance(result_hash, str) and len(result_hash) == 64
    finally:
        monkeypatch.setattr(JobService, "transition", original_transition)
        connection.close()

    assert not (artifact_root / "companies" / "company-one" / "jobs" / "job-one").exists()


def test_late_poll_progress_cannot_block_concurrent_remote_cancel(tmp_path: Path) -> None:
    database_path = tmp_path / "active-poll-cancel-race.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _CancelDuringPollExecutor(database_path, status=ExternalStatus.RUNNING)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")

        assert runner.tick() == 1
        assert runner.tick() == 1
        assert JobService(connection, company_id="company-one").get_job(
            "job-one"
        ).state is JobState.CANCELED
        assert connection.execute(
            """SELECT external_status, current_stage, next_action_at, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("RUNNING", "research", None, None)

        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT external_status, lease_token FROM job_execution_runs
               WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("CANCELED", None)
    finally:
        connection.close()

    assert executor.cancellation_count == 1


def test_transient_poll_error_cannot_delay_concurrent_remote_cancel(tmp_path: Path) -> None:
    database_path = tmp_path / "poll-error-cancel-race.db"
    _queue_approved_job(database_path, maximum_retries=2)
    connection = connect(database_path)
    executor = _CancelDuringPollErrorExecutor(
        database_path,
        ExecutorError(
            error_code="HTTP_503",
            error_summary="poll endpoint unavailable",
            retry_after_seconds=30,
        ),
    )
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")

        assert runner.tick() == 1
        assert runner.tick() == 1
        assert JobService(connection, company_id="company-one").get_job(
            "job-one"
        ).state is JobState.CANCELED
        assert connection.execute(
            """SELECT external_status, next_action_at, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("ACCEPTED", None, None)

        assert runner.tick() == 1
        assert runner.tick() == 0
    finally:
        connection.close()

    assert executor.cancellation_count == 1


def test_final_poll_error_cannot_override_concurrent_local_cancel(tmp_path: Path) -> None:
    database_path = tmp_path / "poll-final-cancel-race.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _CancelDuringPollErrorExecutor(
        database_path,
        ExecutorError(
            error_code="MODEL_OUTPUT_INVALID",
            error_summary="result schema is invalid",
        ),
    )
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")

        assert runner.tick() == 1
        assert runner.tick() == 1
        assert JobService(connection, company_id="company-one").get_job(
            "job-one"
        ).state is JobState.CANCELED
        assert connection.execute(
            """SELECT external_status, current_stage, next_action_at,
                      error_code, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "FAILED_FINAL",
            "cancel_reconciliation_required",
            None,
            "MODEL_OUTPUT_INVALID",
            None,
        )
        assert runner.tick() == 0
    finally:
        connection.close()

    assert executor.cancellation_count == 0


def test_exhausted_poll_error_cannot_override_concurrent_local_cancel(tmp_path: Path) -> None:
    database_path = tmp_path / "poll-exhausted-cancel-race.db"
    _queue_approved_job(database_path, maximum_retries=0)
    connection = connect(database_path)
    executor = _CancelDuringPollErrorExecutor(
        database_path,
        ExecutorError(
            error_code="HTTP_503",
            error_summary="poll endpoint unavailable",
        ),
    )
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")

        assert runner.tick() == 1
        assert runner.tick() == 1
        assert JobService(connection, company_id="company-one").get_job(
            "job-one"
        ).state is JobState.CANCELED
        assert connection.execute(
            """SELECT external_status, current_stage, next_action_at,
                      error_code, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "ACCEPTED",
            None,
            None,
            "HTTP_503",
            None,
        )

        assert runner.tick() == 1
        assert runner.tick() == 0
    finally:
        connection.close()

    assert executor.cancellation_count == 1


def test_cancel_after_durable_terminal_poll_preserves_remote_truth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "terminal-cancel-race.db"
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
    original_transition = JobService.transition

    def crash_before_local_terminal_transition(self, *args, **kwargs):
        raise _SimulatedCrash

    try:
        runner = _runner(connection, executor, artifact_root)
        assert runner.tick() == 1
        monkeypatch.setattr(
            JobService,
            "transition",
            crash_before_local_terminal_transition,
        )
        with pytest.raises(_SimulatedCrash):
            runner.tick()
        monkeypatch.setattr(JobService, "transition", original_transition)

        canceled = JobService(connection, company_id="company-one", clock=lambda: NOW).cancel_job(
            "job-one", JobState.RUNNING
        )
        assert canceled.state is JobState.CANCELED
        assert runner.tick() == 1
        external_status, current_stage, result_hash = connection.execute(
            """SELECT external_status, current_stage, result_hash
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone()
        assert external_status == "SUCCEEDED"
        assert current_stage == "cancel_reconciliation_required"
        assert isinstance(result_hash, str) and len(result_hash) == 64
        assert runner.tick() == 0
    finally:
        monkeypatch.setattr(JobService, "transition", original_transition)
        connection.close()

    assert executor.cancellation_count == 0
    assert not (artifact_root / "companies" / "company-one" / "jobs" / "job-one").exists()


def test_cancel_claim_cannot_race_a_new_terminal_external_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "terminal-before-cancel-claim.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _TransactionCheckingExecutor(connection)
    runner = _runner(connection, executor, tmp_path / "artifacts")
    original_get_run = type(runner._runs).get_run
    first_read = True

    def finish_after_stale_read(repository, company_id: str, job_id: str, attempt: int):
        nonlocal first_read
        run = original_get_run(repository, company_id, job_id, attempt)
        if first_read:
            first_read = False
            with connection:
                connection.execute(
                    """UPDATE job_execution_runs
                       SET external_status = 'FAILED_FINAL',
                           completion_observed_at = ?,
                           error_code = 'MODEL_OUTPUT_INVALID',
                           error_summary = 'result schema is invalid'
                       WHERE company_id = ? AND job_id = ? AND attempt = ?""",
                    (NOW.isoformat(), company_id, job_id, attempt),
                )
        return run

    try:
        assert runner.tick() == 1
        JobService(connection, company_id="company-one", clock=lambda: NOW).cancel_job(
            "job-one", JobState.RUNNING
        )
        monkeypatch.setattr(type(runner._runs), "get_run", finish_after_stale_read)

        assert runner.tick() == 0
        monkeypatch.setattr(type(runner._runs), "get_run", original_get_run)
        assert runner.tick() == 1
        assert connection.execute(
            """SELECT external_status, current_stage, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("FAILED_FINAL", "cancel_reconciliation_required", None)
    finally:
        monkeypatch.setattr(type(runner._runs), "get_run", original_get_run)
        connection.close()

    assert executor.cancel_call_count == 0


def test_known_run_cancel_remains_available_after_launch_approval_expires(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "cancel-after-approval-expiry.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _TransactionCheckingExecutor(connection)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        connection.execute("DROP TRIGGER approval_records_immutable_update")
        connection.execute(
            "UPDATE approval_records SET expires_at = ? WHERE approval_record_id = ?",
            ((NOW - timedelta(seconds=1)).isoformat(), "approval-record-one"),
        )
        connection.commit()
        JobService(
            connection,
            company_id="company-one",
            clock=lambda: NOW + timedelta(seconds=1),
        ).cancel_job("job-one", JobState.RUNNING)

        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(seconds=1),
        ).tick() == 1
        assert executor.cancellation_count == 1
        assert connection.execute(
            "SELECT state FROM jobs WHERE company_id = ? AND job_id = ?",
            ("company-one", "job-one"),
        ).fetchone() == ("CANCELED",)
    finally:
        connection.close()


def test_cancel_deadline_expires_before_lease_and_never_repeats(tmp_path: Path) -> None:
    database_path = tmp_path / "cancel-timeout.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _TimeoutAfterCancelExecutor(clock=lambda: NOW)
    started = time.monotonic()
    try:
        runner = _runner(
            connection,
            executor,
            tmp_path / "artifacts-cancel-timeout",
            lease_duration=timedelta(milliseconds=300),
            external_call_timeout=timedelta(milliseconds=50),
            lease_safety_margin=timedelta(milliseconds=100),
        )
        assert runner.tick() == 1
        JobService(
            connection,
            company_id="company-one",
            clock=lambda: NOW,
        ).cancel_job("job-one", JobState.RUNNING)
        assert runner.tick() == 1
        assert time.monotonic() - started < 0.5
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT external_status, current_stage, error_code, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "ACCEPTED",
            "cancel_reconciliation_required",
            "EXECUTOR_CANCEL_OUTPUT_INVALID",
            None,
        )
    finally:
        connection.close()

    assert executor.cancellation_count == 1
    assert executor.cancel_call_count == 1


def test_executor_name_change_with_cancel_error_cannot_schedule_retry(tmp_path: Path) -> None:
    database_path = tmp_path / "cancel-error-name-change.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _NameChangingCancelErrorExecutor(clock=lambda: NOW)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts-cancel-error-name")
        assert runner.tick() == 1
        JobService(
            connection,
            company_id="company-one",
            clock=lambda: NOW,
        ).cancel_job("job-one", JobState.RUNNING)
        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT current_stage, error_code, next_action_at
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "cancel_reconciliation_required",
            "EXECUTOR_MISMATCH",
            None,
        )
    finally:
        connection.close()


def test_running_cancel_is_forwarded_once_outside_transaction_and_never_resurrects(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "cancel.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _TransactionCheckingExecutor(
        connection,
        outcomes=(
            ExecutionStatus(
                external_run_id="external-one",
                status=ExternalStatus.RUNNING,
                stage_id="late-progress",
                retry_after_seconds=1,
                error_code=None,
                error_summary=None,
                result=None,
            ),
        ),
    )
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        canceled = JobService(connection, company_id="company-one", clock=lambda: NOW).cancel_job(
            "job-one", JobState.RUNNING
        )
        assert canceled.state is JobState.CANCELED

        assert runner.tick() == 1
        assert runner.tick() == 0
        stored = JobService(connection, company_id="company-one").get_job("job-one")
        assert stored.state is JobState.CANCELED
        assert connection.execute(
            """SELECT external_status, completion_observed_at, lease_token
               FROM job_execution_runs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == ("CANCELED", NOW.isoformat(), None)
        assert not connection.in_transaction
    finally:
        connection.close()

    assert executor.cancellation_count == 1


def test_queued_cancel_without_external_submission_never_calls_executor(tmp_path: Path) -> None:
    database_path = tmp_path / "queued-cancel.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _TransactionCheckingExecutor(connection)
    try:
        JobService(connection, company_id="company-one", clock=lambda: NOW).cancel_job(
            "job-one", JobState.QUEUED
        )

        assert _runner(connection, executor, tmp_path / "artifacts").tick() == 0
        assert connection.execute("SELECT COUNT(*) FROM job_execution_runs").fetchone() == (0,)
    finally:
        connection.close()

    assert executor.submission_count == 0
    assert executor.cancellation_count == 0


def test_cancel_retry_budget_is_independent_from_poll_failures(tmp_path: Path) -> None:
    database_path = tmp_path / "cancel-stage-budget.db"
    _queue_approved_job(database_path, maximum_retries=1)
    connection = connect(database_path)
    transient = ExecutorError(
        error_code="HTTP_503",
        error_summary="retryable provider error",
    )
    executor = _TransactionCheckingExecutor(
        connection,
        outcomes=(transient,),
        cancel_errors=(transient,),
    )
    try:
        assert _runner(connection, executor, tmp_path / "artifacts-budget").tick() == 1
        assert _runner(connection, executor, tmp_path / "artifacts-budget").tick() == 1
        JobService(
            connection,
            company_id="company-one",
            clock=lambda: NOW,
        ).cancel_job("job-one", JobState.RUNNING)

        assert _runner(connection, executor, tmp_path / "artifacts-budget").tick() == 1
        assert connection.execute(
            """SELECT current_stage, transient_failure_count
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("cancel_retry_pending", 1)
        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts-budget",
            now=NOW + timedelta(milliseconds=500),
        ).tick() == 0
        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts-budget",
            now=NOW + timedelta(seconds=1),
        ).tick() == 1
        assert connection.execute(
            """SELECT external_status FROM job_execution_runs
               WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("CANCELED",)
    finally:
        connection.close()


def test_transient_remote_cancel_error_retries_after_deadline_without_resurrection(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "cancel-retry.db"
    _queue_approved_job(database_path, maximum_retries=2)
    connection = connect(database_path)
    executor = _TransactionCheckingExecutor(connection)
    executor.script_cancel_errors(
        ExecutorError(
            error_code="HTTP_503",
            error_summary="cancel endpoint unavailable",
            retry_after_seconds=3,
        )
    )
    policy = RetryPolicy(base_delay_seconds=1, maximum_delay_seconds=30, jitter=lambda delay: delay)
    try:
        assert _runner(connection, executor, tmp_path / "artifacts", retry_policy=policy).tick() == 1
        JobService(connection, company_id="company-one", clock=lambda: NOW).cancel_job(
            "job-one", JobState.RUNNING
        )

        assert _runner(connection, executor, tmp_path / "artifacts", retry_policy=policy).tick() == 1
        assert JobService(connection, company_id="company-one").get_job("job-one").state is JobState.CANCELED
        assert connection.execute(
            """SELECT external_status, next_action_at, transient_failure_count,
                      error_code, lease_token
               FROM job_execution_runs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == (
            "ACCEPTED",
            (NOW + timedelta(seconds=3)).isoformat(),
            1,
            "HTTP_503",
            None,
        )
        assert _runner(connection, executor, tmp_path / "artifacts", retry_policy=policy).tick() == 0
        assert _runner(
            connection,
            executor,
            tmp_path / "artifacts",
            now=NOW + timedelta(seconds=3),
            retry_policy=policy,
        ).tick() == 1
        assert connection.execute(
            "SELECT external_status, next_action_at FROM job_execution_runs WHERE job_id = ?",
            ("job-one",),
        ).fetchone() == ("CANCELED", None)
        assert not connection.in_transaction
    finally:
        connection.close()

    assert executor.cancel_call_count == 2
    assert executor.cancellation_count == 1


def test_malformed_cancel_exception_is_quarantined_without_recancel(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "malformed-cancel-exception.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _MalformedCancelOutputExecutor(clock=lambda: NOW)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        JobService(connection, company_id="company-one", clock=lambda: NOW).cancel_job(
            "job-one", JobState.RUNNING
        )

        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT external_status, current_stage, error_code, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "ACCEPTED",
            "cancel_reconciliation_required",
            "EXECUTOR_CANCEL_OUTPUT_INVALID",
            None,
        )
    finally:
        connection.close()

    assert executor.cancel_call_count == 1


def test_non_none_cancel_result_is_quarantined_without_false_completion(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "non-none-cancel-result.db"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    executor = _NonNoneCancelResultExecutor(clock=lambda: NOW)
    try:
        runner = _runner(connection, executor, tmp_path / "artifacts")
        assert runner.tick() == 1
        JobService(connection, company_id="company-one", clock=lambda: NOW).cancel_job(
            "job-one", JobState.RUNNING
        )

        assert runner.tick() == 1
        assert runner.tick() == 0
        assert connection.execute(
            """SELECT external_status, current_stage, error_code, lease_token
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (
            "ACCEPTED",
            "cancel_reconciliation_required",
            "EXECUTOR_CANCEL_OUTPUT_INVALID",
            None,
        )
    finally:
        connection.close()

    assert executor.cancel_call_count == 1


def test_cancel_after_ambiguous_submit_uses_lookup_then_remote_cancel(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "ambiguous-cancel.db"
    artifact_root = tmp_path / "artifacts"
    _queue_approved_job(database_path)
    first_connection = connect(database_path)
    first_executor = MockExecutor(
        clock=lambda: NOW,
        run_id_factory=lambda _number: "external-one",
    )

    def crash_after_submit() -> None:
        raise _SimulatedCrash

    try:
        try:
            _runner(
                first_connection,
                first_executor,
                artifact_root,
                after_submit=crash_after_submit,
            ).tick()
        except _SimulatedCrash:
            pass
        else:
            raise AssertionError("simulated crash was not raised")
        JobService(first_connection, company_id="company-one", clock=lambda: NOW).cancel_job(
            "job-one", JobState.RUNNING
        )
    finally:
        first_connection.close()

    restarted = connect(database_path)
    restarted_executor = MockExecutor(
        clock=lambda: NOW,
        run_id_factory=lambda _number: "must-not-create-a-second-run",
    )
    try:
        runner = _runner(
            restarted,
            restarted_executor,
            artifact_root,
            now=NOW + timedelta(seconds=31),
        )
        assert runner.tick() == 1
        assert restarted.execute(
            """SELECT external_run_id, external_status FROM job_execution_runs
               WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("external-one", "ACCEPTED")
        assert runner.tick() == 1
        assert runner.tick() == 0
        assert restarted.execute(
            """SELECT external_status, lease_token FROM job_execution_runs
               WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == ("CANCELED", None)
    finally:
        restarted.close()

    assert first_executor.submission_count == 1
    assert restarted_executor.submission_count == 1
    assert restarted_executor.submit_call_count == 0
    assert restarted_executor.lookup_call_count == 1
    assert restarted_executor.cancellation_count == 1


def test_cancel_lookup_miss_never_resubmits_external_work(tmp_path: Path) -> None:
    database_path = tmp_path / "ambiguous-cancel-miss.db"
    artifact_root = tmp_path / "artifacts"
    _queue_approved_job(database_path)
    connection = connect(database_path)
    first_executor = MockExecutor(clock=lambda: NOW)

    def crash_after_submit() -> None:
        raise _SimulatedCrash

    try:
        try:
            _runner(
                connection,
                first_executor,
                artifact_root,
                after_submit=crash_after_submit,
            ).tick()
        except _SimulatedCrash:
            pass
        else:
            raise AssertionError("simulated crash was not raised")
        JobService(connection, company_id="company-one", clock=lambda: NOW).cancel_job(
            "job-one", JobState.RUNNING
        )
    finally:
        connection.close()

    provider = sqlite3.connect(tmp_path / "mock-executor.db")
    try:
        provider.execute("DELETE FROM mock_executor_runs")
        provider.commit()
    finally:
        provider.close()

    restarted = connect(database_path)
    restarted_executor = MockExecutor(clock=lambda: NOW)
    try:
        runner = _runner(
            restarted,
            restarted_executor,
            artifact_root,
            now=NOW + timedelta(seconds=31),
        )
        assert runner.tick() == 1
        assert runner.tick() == 0
        assert restarted.execute(
            """SELECT external_run_id, external_status, current_stage
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            ("company-one", "job-one"),
        ).fetchone() == (None, None, "cancel_reconciliation_required")
    finally:
        restarted.close()

    assert restarted_executor.lookup_call_count == 1
    assert restarted_executor.submit_call_count == 0
    assert restarted_executor.cancel_call_count == 0
