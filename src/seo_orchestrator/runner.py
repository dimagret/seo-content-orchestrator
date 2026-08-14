"""Bounded durable job runner primitives."""

from __future__ import annotations

import hashlib
import math
import signal
import sqlite3
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import FrameType

from seo_orchestrator.db.connection import transaction
from seo_orchestrator.db.repositories import ExecutionRunRecord, RunnerCandidate, RunnerRepository
from seo_orchestrator.domain import JobState
from seo_orchestrator.domain.approvals import ExecutionPlan
from seo_orchestrator.errors import ApprovalInvalid, DataIntegrityError, StateConflict
from seo_orchestrator.executors.base import (
    ExecutionStatus,
    Executor,
    ExecutorError,
    ExternalRun,
    ExternalStatus,
    execution_result_bytes,
    execution_result_from_bytes,
    submission_idempotency_key,
)
from seo_orchestrator.services.artifacts import ArtifactStore, ExecutionResult
from seo_orchestrator.services.jobs import JobService


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Classify retryable errors and compute bounded deterministic delays."""

    _TRANSIENT_ERRORS = frozenset(
        {"NETWORK_ERROR", "HTTP_429", "HTTP_502", "HTTP_503", "HTTP_504"}
    )

    base_delay_seconds: float = 1.0
    maximum_delay_seconds: float = 60.0
    jitter: Callable[[float], float] = field(default=lambda delay: delay)

    def __post_init__(self) -> None:
        if type(self.base_delay_seconds) not in {int, float} or type(
            self.maximum_delay_seconds
        ) not in {int, float}:
            raise ValueError("retry delays must be positive and bounded")
        base_delay = float(self.base_delay_seconds)
        maximum_delay = float(self.maximum_delay_seconds)
        if (
            not math.isfinite(base_delay)
            or not math.isfinite(maximum_delay)
            or base_delay <= 0
            or maximum_delay <= 0
            or base_delay > maximum_delay
        ):
            raise ValueError("retry delays must be positive and bounded")
        if not callable(self.jitter):
            raise TypeError("jitter must be callable")

    def delay_for(self, error_code: str, *, retry_number: int) -> float:
        """Return a bounded delay for one transient failure."""
        if not self.is_retryable(error_code):
            raise ValueError("error is not retryable")
        if type(retry_number) is not int or retry_number < 1:
            raise ValueError("retry_number must be a positive integer")
        maximum = float(self.maximum_delay_seconds)
        base = float(self.base_delay_seconds)
        remaining_doublings = retry_number - 1
        while remaining_doublings > 0 and base < maximum:
            base = min(maximum, base * 2)
            remaining_doublings -= 1
        jittered = self.jitter(base)
        if (
            type(jittered) not in {int, float}
            or not math.isfinite(float(jittered))
            or jittered < 0
        ):
            raise ValueError("jitter must return a non-negative number")
        return min(maximum, float(jittered))

    def is_retryable(self, error_code: str) -> bool:
        """Classify only the frozen transient transport failures as retryable."""
        return type(error_code) is str and error_code in self._TRANSIENT_ERRORS


def _aware_utc(value: object, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")
    return value.astimezone(UTC)


class Runner:
    """Process a bounded number of durable jobs without network I/O in transactions."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        executor: Executor,
        artifact_store: ArtifactStore,
        clock: Callable[[], datetime] | None = None,
        runner_id: str,
        lease_token_factory: Callable[[], str],
        after_submit: Callable[[], None] | None = None,
        stale_after: timedelta = timedelta(minutes=5),
        lease_duration: timedelta = timedelta(seconds=30),
        external_call_timeout: timedelta = timedelta(seconds=20),
        lease_safety_margin: timedelta = timedelta(seconds=5),
        approval_submission_margin: timedelta = timedelta(seconds=1),
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        if not isinstance(conn, sqlite3.Connection):
            raise TypeError("conn must be a sqlite3.Connection")
        for method_name in ("submit", "submit_authorized", "lookup", "poll", "cancel"):
            if not callable(getattr(executor, method_name, None)):
                raise TypeError("executor must implement submit, lookup, poll, and cancel")
        executor_name = getattr(executor, "name", None)
        if type(executor_name) is not str or not executor_name.strip():
            raise TypeError("executor must expose a non-empty stable name")
        if getattr(executor, "durable_semantic_idempotency", None) is not True:
            raise TypeError("executor must guarantee durable semantic idempotency")
        if getattr(executor, "side_effect_free_lookup", None) is not True:
            raise TypeError("executor must guarantee side-effect-free lookup")
        if getattr(executor, "idempotent_cancel", None) is not True:
            raise TypeError("executor must guarantee idempotent cancellation")
        if getattr(executor, "cancel_confirms_terminal", None) is not True:
            raise TypeError("executor cancel must confirm terminal cancellation")
        if getattr(executor, "authority_deadline_enforced", None) is not True:
            raise TypeError("executor must enforce the exclusive authority deadline")
        if getattr(executor, "configuration_authorization_enforced", None) is not True:
            raise TypeError("executor must enforce approved provider/model configuration")
        if not isinstance(artifact_store, ArtifactStore):
            raise TypeError("artifact_store must be an ArtifactStore")
        if type(runner_id) is not str or not runner_id.strip():
            raise ValueError("runner_id must be a non-empty string")
        if not callable(lease_token_factory):
            raise TypeError("lease_token_factory must be callable")
        if after_submit is not None and not callable(after_submit):
            raise TypeError("after_submit must be callable")
        if (
            type(stale_after) is not timedelta
            or type(lease_duration) is not timedelta
            or stale_after <= timedelta(0)
            or lease_duration <= timedelta(0)
            or type(external_call_timeout) is not timedelta
            or external_call_timeout <= timedelta(0)
            or type(lease_safety_margin) is not timedelta
            or lease_safety_margin <= timedelta(0)
            or external_call_timeout + lease_safety_margin > lease_duration
            or type(approval_submission_margin) is not timedelta
            or approval_submission_margin <= timedelta(0)
        ):
            raise ValueError(
                "runner durations must be positive and leave lease persistence margin"
            )
        self._conn = conn
        self._executor = executor
        self._executor_name = executor_name
        self._artifact_store = artifact_store
        self._clock = clock or (lambda: datetime.now(UTC))
        self._runner_id = runner_id
        self._lease_token_factory = lease_token_factory
        self._after_submit = after_submit or (lambda: None)
        self._stale_after = stale_after
        self._lease_duration = lease_duration
        self._external_call_timeout = external_call_timeout
        self._lease_safety_margin = lease_safety_margin
        self._approval_submission_margin = approval_submission_margin
        self._retry_policy = retry_policy or RetryPolicy()
        self._runs = RunnerRepository(conn)

    def _now(self) -> datetime:
        return _aware_utc(self._clock(), "runner clock")

    def _lease_token(self) -> str:
        token = self._lease_token_factory()
        if type(token) is not str or not token.strip():
            raise ValueError("lease token factory must return a non-empty string")
        return token

    @contextmanager
    def _external_call(self, timeout: timedelta | None = None) -> Iterator[None]:
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("external calls require the runner main thread")
        previous_handler = signal.getsignal(signal.SIGALRM)
        previous_timer = signal.getitimer(signal.ITIMER_REAL)
        if previous_timer[0] > 0:
            raise RuntimeError("runner external deadline conflicts with an active timer")

        def timeout_handler(_signum: int, _frame: FrameType | None) -> None:
            raise TimeoutError("executor call exceeded the durable lease deadline")

        signal.signal(signal.SIGALRM, timeout_handler)
        selected_timeout = timeout or self._external_call_timeout
        if selected_timeout <= timedelta(0) or selected_timeout > self._external_call_timeout:
            raise ValueError("external call timeout is outside the configured bound")
        signal.setitimer(signal.ITIMER_REAL, selected_timeout.total_seconds())
        try:
            yield
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous_handler)

    def _lease_call_timeout(self, run: ExecutionRunRecord) -> timedelta:
        if run.lease_expires_at is None:
            raise DataIntegrityError
        try:
            lease_expires_at = _aware_utc(
                datetime.fromisoformat(run.lease_expires_at),
                name="lease_expires_at",
            )
        except (TypeError, ValueError) as exc:
            raise DataIntegrityError from exc
        available = lease_expires_at - self._now() - self._lease_safety_margin
        if available <= timedelta(0):
            raise TimeoutError("insufficient durable lease remains for external call")
        return min(self._external_call_timeout, available)

    def _record_heartbeat(self, now: datetime) -> None:
        with transaction(self._conn):
            self._runs.record_heartbeat(self._runner_id, now.isoformat())

    def _retry_delay(
        self,
        error_code: str,
        *,
        retry_number: int,
        requested_delay: int | None,
    ) -> float:
        policy_delay = self._retry_policy.delay_for(
            error_code, retry_number=retry_number
        )
        maximum = float(self._retry_policy.maximum_delay_seconds)
        requested = requested_delay or 0
        if requested >= maximum:
            return maximum
        return max(policy_delay, float(requested))

    def _poll_delay(self, requested_delay: int | None) -> float:
        requested = requested_delay or 1
        maximum = float(self._retry_policy.maximum_delay_seconds)
        if requested >= maximum:
            return maximum
        return float(requested)

    def _mark_stale(self, candidate: RunnerCandidate, now: datetime) -> None:
        key = f"{candidate.company_id}:{candidate.job_id}:{candidate.attempt}"
        with transaction(self._conn):
            self._runs.mark_reconciliation_required(
                candidate,
                idempotency_key=key,
                now=now.isoformat(),
            )

    def _mark_claimed_reconciliation(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        error_code: str,
        error_summary: str,
        now: datetime,
    ) -> bool:
        with transaction(self._conn):
            marked = self._runs.mark_claimed_reconciliation_required(
                candidate,
                lease_token=lease_token,
                error_code=error_code,
                error_summary=error_summary,
                now=now.isoformat(),
            )
        if not marked:
            raise StateConflict
        return True

    def _current_executor_name(self) -> str:
        capabilities = (
            "durable_semantic_idempotency",
            "side_effect_free_lookup",
            "idempotent_cancel",
            "cancel_confirms_terminal",
            "authority_deadline_enforced",
            "configuration_authorization_enforced",
        )
        if any(getattr(self._executor, capability, None) is not True for capability in capabilities):
            raise DataIntegrityError
        current_name = getattr(self._executor, "name", None)
        if type(current_name) is not str or current_name != self._executor_name:
            raise DataIntegrityError
        return current_name

    def _executor_matches_plan(self, service: JobService, job_id: str) -> bool:
        current_name = self._current_executor_name()
        current_models = getattr(self._executor, "model_ids", None)
        current_providers = getattr(self._executor, "provider_ids", None)
        if (
            type(current_models) is not tuple
            or type(current_providers) is not tuple
            or any(type(value) is not str or not value for value in current_models)
            or any(type(value) is not str or not value for value in current_providers)
        ):
            raise DataIntegrityError
        plan = service.execution_plan(job_id)
        return (
            plan.executor_name == current_name
            and plan.model_ids == current_models
            and plan.provider_ids == current_providers
        )

    @staticmethod
    def _result_usage_matches_plan(result: ExecutionResult, plan: ExecutionPlan) -> bool:
        usage = result.model_usage
        if type(usage) is not dict or set(usage) != {"models"}:
            return False
        models = usage.get("models")
        if type(models) is not list or not models:
            return False
        approved_models = set(plan.model_ids)
        approved_providers = set(plan.provider_ids)
        for entry in models:
            if type(entry) is not dict:
                return False
            model_id = entry.get("model_id")
            provider_id = entry.get("provider_id")
            if model_id not in approved_models or provider_id not in approved_providers:
                return False
        return True

    def _executor_identity_still_matches(
        self,
        service: JobService,
        job_id: str,
    ) -> bool:
        try:
            return self._executor_matches_plan(service, job_id)
        except (DataIntegrityError, TypeError, ValueError):
            return False

    def _durable_executor_matches(
        self,
        service: JobService,
        candidate: RunnerCandidate,
        run: ExecutionRunRecord,
    ) -> bool:
        return (
            run.executor_name is not None
            and run.executor_name == self._executor_name
            and self._executor_identity_still_matches(service, candidate.job_id)
        )

    def _acceptance_provenance_valid(
        self,
        service: JobService,
        candidate: RunnerCandidate,
        run: ExecutionRunRecord,
        *,
        accepted_at: datetime,
        observed_at: datetime,
    ) -> bool:
        if run.submission_attempted_at is None:
            return False
        try:
            submitted_at = datetime.fromisoformat(run.submission_attempted_at)
            submitted_at = _aware_utc(submitted_at, "submission attempted_at")
            approved_at, expires_at = service.launch_approval_window(candidate.job_id)
        except (ApprovalInvalid, DataIntegrityError, TypeError, ValueError):
            return False
        return (
            approved_at <= submitted_at <= accepted_at <= observed_at
            and (expires_at is None or accepted_at < expires_at)
        )

    def _durable_acceptance_provenance_valid(
        self,
        service: JobService,
        candidate: RunnerCandidate,
        run: ExecutionRunRecord,
    ) -> bool:
        if run.external_accepted_at is None or run.acceptance_observed_at is None:
            return False
        try:
            accepted_at = _aware_utc(
                datetime.fromisoformat(run.external_accepted_at),
                "external accepted_at",
            )
            observed_at = _aware_utc(
                datetime.fromisoformat(run.acceptance_observed_at),
                "acceptance observed_at",
            )
        except (TypeError, ValueError):
            return False
        return self._acceptance_provenance_valid(
            service,
            candidate,
            run,
            accepted_at=accepted_at,
            observed_at=observed_at,
        )

    def _reconcile_terminal_cancel(
        self,
        candidate: RunnerCandidate,
        now: datetime,
    ) -> bool:
        if self._runs.job_state(candidate) != JobState.CANCELED.value:
            return False
        with transaction(self._conn):
            reconciled = self._runs.reconcile_poll_after_local_cancel(
                candidate,
                terminal=True,
                now=now.isoformat(),
            )
        if not reconciled:
            raise StateConflict
        return True

    def _recover_dispatch_without_submit(
        self,
        candidate: RunnerCandidate,
        now: datetime,
        service: JobService,
    ) -> bool:
        try:
            job, snapshot = service.recover_dispatch_identity(candidate.job_id)
        except DataIntegrityError:
            with transaction(self._conn):
                return self._runs.quarantine_running_dispatch(
                    candidate,
                    error_code="DATA_INTEGRITY",
                    error_summary="execution provenance failed integrity verification",
                    now=now.isoformat(),
                )
        except StateConflict:
            return False
        if job.attempt != candidate.attempt or job.state is not JobState.RUNNING:
            raise StateConflict
        if not self._executor_identity_still_matches(service, candidate.job_id):
            with transaction(self._conn):
                return self._runs.quarantine_running_dispatch(
                    candidate,
                    error_code="EXECUTOR_MISMATCH",
                    error_summary="selected executor does not match the approved plan",
                    now=now.isoformat(),
                )
        idempotency_key = submission_idempotency_key(job)
        claim_now = self._now()
        lease_token = self._lease_token()
        with transaction(self._conn):
            claimed = self._runs.claim_submission(
                candidate,
                idempotency_key=idempotency_key,
                executor_name=self._executor_name,
                now=claim_now.isoformat(),
                lease_token=lease_token,
                lease_expires_at=(claim_now + self._lease_duration).isoformat(),
            )
        if claimed is None:
            return False
        try:
            with self._external_call(self._lease_call_timeout(claimed)):
                external_run = self._executor.lookup(job, snapshot)
            if external_run is None or not isinstance(external_run, ExternalRun):
                raise DataIntegrityError
            external_run.__post_init__()
            if external_run.idempotency_key != idempotency_key:
                raise DataIntegrityError
            external_accepted_at = _aware_utc(
                external_run.accepted_at,
                "external accepted_at",
            )
        except Exception:  # noqa: BLE001 - lookup never creates external work.
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="APPROVAL_EXPIRED_LOOKUP_FAILED",
                error_summary="expired approval prevents dispatch replay; lookup requires reconciliation",
                now=self._now(),
            )
        observed_at = self._now()
        if (
            not self._executor_identity_still_matches(service, candidate.job_id)
            or not self._acceptance_provenance_valid(
                service,
                candidate,
                claimed,
                accepted_at=external_accepted_at,
                observed_at=observed_at,
            )
        ):
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXTERNAL_ACCEPTANCE_PROVENANCE_INVALID",
                error_summary="external acceptance provenance requires reconciliation",
                now=observed_at,
            )
        try:
            with transaction(self._conn):
                recorded = self._runs.record_external_acceptance(
                    candidate,
                    lease_token=lease_token,
                    external_run_id=external_run.external_run_id,
                    executor_name=self._executor_name,
                    idempotency_key=external_run.idempotency_key,
                    accepted_at=external_accepted_at.isoformat(),
                    now=observed_at.isoformat(),
                )
        except sqlite3.Error:
            recorded = False
        if not recorded:
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="APPROVAL_EXPIRED_LOOKUP_PERSIST_FAILED",
                error_summary="expired approval lookup persistence requires reconciliation",
                now=observed_at,
            )
        with transaction(self._conn):
            updated = self._runs.update_running_stage(
                candidate,
                current_stage="accepted",
            )
        if not updated:
            raise StateConflict
        return True

    def _submit(self, candidate: RunnerCandidate, now: datetime) -> bool:
        service = JobService(
            self._conn,
            company_id=candidate.company_id,
            clock=lambda: now,
        )
        if candidate.state == JobState.QUEUED.value:
            try:
                job, snapshot = service.prepare_execution(candidate.job_id)
            except ApprovalInvalid:
                with transaction(self._conn):
                    quarantined = self._runs.quarantine_queued_job(
                        candidate,
                        error_code="APPROVAL_INVALID",
                        error_summary="approval is not valid at dispatch time",
                        now=now.isoformat(),
                    )
                return quarantined
            except DataIntegrityError:
                with transaction(self._conn):
                    quarantined = self._runs.quarantine_queued_job(
                        candidate,
                        error_code="DATA_INTEGRITY",
                        error_summary="execution provenance failed integrity verification",
                        now=now.isoformat(),
                    )
                return quarantined
            except StateConflict:
                return False
        elif candidate.state == JobState.RUNNING.value:
            try:
                job, snapshot = service.resume_dispatch(candidate.job_id)
            except ApprovalInvalid:
                return self._recover_dispatch_without_submit(candidate, now, service)
            except DataIntegrityError:
                with transaction(self._conn):
                    quarantined = self._runs.quarantine_running_dispatch(
                        candidate,
                        error_code="DATA_INTEGRITY",
                        error_summary="execution provenance failed integrity verification",
                        now=now.isoformat(),
                    )
                return quarantined
            except StateConflict:
                return False
        else:
            raise StateConflict
        if job.attempt != candidate.attempt or job.state.value != candidate.state:
            raise StateConflict
        if not self._executor_identity_still_matches(service, candidate.job_id):
            with transaction(self._conn):
                if candidate.state == JobState.QUEUED.value:
                    return self._runs.quarantine_queued_job(
                        candidate,
                        error_code="EXECUTOR_MISMATCH",
                        error_summary="selected executor does not match the approved plan",
                        now=now.isoformat(),
                    )
                return self._runs.quarantine_running_dispatch(
                    candidate,
                    error_code="EXECUTOR_MISMATCH",
                    error_summary="selected executor does not match the approved plan",
                    now=now.isoformat(),
                )
        idempotency_key = submission_idempotency_key(job)
        claim_now = self._now()
        lease_token = self._lease_token()
        with transaction(self._conn):
            claimed = self._runs.claim_submission(
                candidate,
                idempotency_key=idempotency_key,
                executor_name=self._executor_name,
                now=claim_now.isoformat(),
                lease_token=lease_token,
                lease_expires_at=(claim_now + self._lease_duration).isoformat(),
            )
        if claimed is None:
            return False

        fresh_now = self._now()
        fresh_service = JobService(
            self._conn,
            company_id=candidate.company_id,
            clock=lambda: fresh_now,
        )
        current = fresh_service.get_job(candidate.job_id)
        if current.state is JobState.CANCELED:
            with transaction(self._conn):
                released = self._runs.release_lease(
                    candidate,
                    lease_token=lease_token,
                    now=fresh_now.isoformat(),
                )
            if not released:
                raise StateConflict
            return self._cancel(
                RunnerCandidate(
                    company_id=candidate.company_id,
                    job_id=candidate.job_id,
                    attempt=candidate.attempt,
                    state=JobState.CANCELED.value,
                ),
                fresh_now,
            )
        try:
            job, snapshot = fresh_service.resume_dispatch(candidate.job_id)
        except ApprovalInvalid:
            with transaction(self._conn):
                released = self._runs.release_lease(
                    candidate,
                    lease_token=lease_token,
                    now=fresh_now.isoformat(),
                )
            if not released:
                raise StateConflict
            return self._recover_dispatch_without_submit(
                RunnerCandidate(
                    company_id=candidate.company_id,
                    job_id=candidate.job_id,
                    attempt=candidate.attempt,
                    state=JobState.RUNNING.value,
                ),
                fresh_now,
                fresh_service,
            )
        except DataIntegrityError:
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="DATA_INTEGRITY",
                error_summary="execution provenance failed integrity verification",
                now=fresh_now,
            )
        except StateConflict:
            with transaction(self._conn):
                self._runs.release_lease(
                    candidate,
                    lease_token=lease_token,
                    now=fresh_now.isoformat(),
                )
            return False

        if not self._executor_identity_still_matches(fresh_service, candidate.job_id):
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXECUTOR_MISMATCH",
                error_summary="selected executor does not match the approved plan",
                now=fresh_now,
            )

        submit_now = self._now()
        _approved_at, expires_at = fresh_service.launch_approval_window(candidate.job_id)
        submit_timeout = self._external_call_timeout
        if expires_at is not None:
            remaining_authority = (
                expires_at - submit_now - self._approval_submission_margin
            )
            if remaining_authority <= timedelta(0):
                with transaction(self._conn):
                    released = self._runs.release_lease(
                        candidate,
                        lease_token=lease_token,
                        now=submit_now.isoformat(),
                    )
                if not released:
                    raise StateConflict
                return self._recover_dispatch_without_submit(
                    RunnerCandidate(
                        company_id=candidate.company_id,
                        job_id=candidate.job_id,
                        attempt=candidate.attempt,
                        state=JobState.RUNNING.value,
                    ),
                    submit_now,
                    fresh_service,
                )
            submit_timeout = min(submit_timeout, remaining_authority)

        try:
            submit_timeout = min(submit_timeout, self._lease_call_timeout(claimed))
        except (DataIntegrityError, TimeoutError):
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXTERNAL_CALL_DEADLINE_INVALID",
                error_summary="insufficient durable lease remains for submission",
                now=self._now(),
            )

        try:
            approved_plan = service.execution_plan(candidate.job_id)
            with self._external_call(submit_timeout):
                external_run = self._executor.submit_authorized(
                    job,
                    snapshot,
                    authority_expires_at=expires_at,
                    approved_model_ids=approved_plan.model_ids,
                    approved_provider_ids=approved_plan.provider_ids,
                )
            self._after_submit()
            if not isinstance(external_run, ExternalRun):
                raise TypeError("executor submit returned an invalid result")
        except ExecutorError as exc:
            observed_at = self._now()
            if not self._executor_identity_still_matches(
                service,
                candidate.job_id,
            ):
                return self._mark_claimed_reconciliation(
                    candidate,
                    lease_token=lease_token,
                    error_code="EXECUTOR_MISMATCH",
                    error_summary="executor identity changed during submission",
                    now=observed_at,
                )
            try:
                plan = service.execution_plan(candidate.job_id)
            except DataIntegrityError:
                return self._mark_claimed_reconciliation(
                    candidate,
                    lease_token=lease_token,
                    error_code="DATA_INTEGRITY",
                    error_summary="execution provenance failed integrity verification",
                    now=observed_at,
                )
            retry_number = self._runs.retry_count_for_stage(
                candidate.company_id,
                candidate.job_id,
                "submission",
            ) + 1
            retryable = (
                self._retry_policy.is_retryable(exc.error_code)
                and retry_number <= plan.maximum_retries
            )
            if not retryable:
                return self._mark_claimed_reconciliation(
                    candidate,
                    lease_token=lease_token,
                    error_code=exc.error_code,
                    error_summary="submission outcome requires manual reconciliation",
                    now=observed_at,
                )
            delay = self._retry_delay(
                exc.error_code,
                retry_number=retry_number,
                requested_delay=exc.retry_after_seconds,
            )
            with transaction(self._conn):
                recorded = self._runs.record_submit_transport_failure(
                    candidate,
                    lease_token=lease_token,
                    next_action_at=(
                        observed_at + timedelta(seconds=delay)
                    ).isoformat(),
                    error_code=exc.error_code,
                    error_summary=exc.error_summary,
                    now=observed_at.isoformat(),
                )
            if not recorded:
                raise StateConflict
            return True
        except Exception:  # noqa: BLE001 - submit outcome may be ambiguous.
            observed_at = self._now()
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXECUTOR_SUBMIT_OUTPUT_INVALID",
                error_summary="external submission outcome requires reconciliation",
                now=observed_at,
            )
        if external_run.idempotency_key != idempotency_key:
            observed_at = self._now()
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXECUTOR_SUBMIT_OUTPUT_INVALID",
                error_summary="external submission outcome requires reconciliation",
                now=observed_at,
            )

        try:
            external_accepted_at = _aware_utc(
                external_run.accepted_at,
                "external accepted_at",
            )
        except (TypeError, ValueError):
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXECUTOR_SUBMIT_OUTPUT_INVALID",
                error_summary="external submission outcome requires reconciliation",
                now=self._now(),
            )
        observed_at = self._now()
        if (
            not self._executor_identity_still_matches(
                fresh_service,
                candidate.job_id,
            )
            or not self._acceptance_provenance_valid(
                fresh_service,
                candidate,
                claimed,
                accepted_at=external_accepted_at,
                observed_at=observed_at,
            )
        ):
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXTERNAL_ACCEPTANCE_PROVENANCE_INVALID",
                error_summary="external acceptance provenance requires reconciliation",
                now=observed_at,
            )
        try:
            with transaction(self._conn):
                recorded = self._runs.record_external_acceptance(
                    candidate,
                    lease_token=lease_token,
                    external_run_id=external_run.external_run_id,
                    executor_name=self._executor_name,
                    idempotency_key=external_run.idempotency_key,
                    accepted_at=external_accepted_at.isoformat(),
                    now=observed_at.isoformat(),
                )
        except sqlite3.Error:
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXECUTOR_SUBMIT_OUTPUT_INVALID",
                error_summary="external submission outcome requires reconciliation",
                now=observed_at,
            )
        if not recorded:
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXECUTOR_SUBMIT_OUTPUT_INVALID",
                error_summary="external submission outcome requires reconciliation",
                now=observed_at,
            )
        current = service.get_job(candidate.job_id)
        if current.state is JobState.RUNNING:
            with transaction(self._conn):
                updated = self._runs.update_running_stage(
                    RunnerCandidate(
                        company_id=candidate.company_id,
                        job_id=candidate.job_id,
                        attempt=candidate.attempt,
                        state=JobState.RUNNING.value,
                    ),
                    current_stage="accepted",
                )
            if not updated:
                raise StateConflict
        elif current.state is not JobState.CANCELED:
            raise StateConflict
        return True

    @staticmethod
    def _external_run(run: ExecutionRunRecord) -> ExternalRun:
        if run.external_run_id is None or run.external_accepted_at is None:
            raise DataIntegrityError
        try:
            accepted_at = datetime.fromisoformat(run.external_accepted_at)
        except ValueError as exc:
            raise DataIntegrityError from exc
        try:
            return ExternalRun(
                external_run_id=run.external_run_id,
                idempotency_key=run.idempotency_key,
                accepted_at=accepted_at,
            )
        except (TypeError, ValueError) as exc:
            raise DataIntegrityError from exc

    def _claim_existing(
        self,
        candidate: RunnerCandidate,
        now: datetime,
        *,
        expected_statuses: tuple[str, ...],
    ) -> tuple[ExecutionRunRecord, str] | None:
        del now
        claim_now = self._now()
        lease_token = self._lease_token()
        with transaction(self._conn):
            claimed = self._runs.claim_existing_run(
                candidate,
                expected_statuses=expected_statuses,
                now=claim_now.isoformat(),
                lease_token=lease_token,
                lease_expires_at=(claim_now + self._lease_duration).isoformat(),
            )
        if claimed is None:
            return None
        return claimed, lease_token

    def _poll(self, candidate: RunnerCandidate, now: datetime) -> bool:
        claimed = self._claim_existing(
            candidate,
            now,
            expected_statuses=(ExternalStatus.ACCEPTED.value, ExternalStatus.RUNNING.value),
        )
        if claimed is None:
            return False
        run, lease_token = claimed
        service = JobService(self._conn, company_id=candidate.company_id)
        try:
            executor_matches = self._durable_executor_matches(service, candidate, run)
        except DataIntegrityError:
            executor_matches = False
        if not executor_matches:
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXECUTOR_MISMATCH",
                error_summary="selected executor does not match the approved plan",
                now=now,
            )
        if not self._durable_acceptance_provenance_valid(service, candidate, run):
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXTERNAL_ACCEPTANCE_PROVENANCE_INVALID",
                error_summary="durable external acceptance requires reconciliation",
                now=now,
            )
        try:
            external_run = self._external_run(run)
        except DataIntegrityError:
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXTERNAL_RUN_IDENTITY_INVALID",
                error_summary="durable external identity requires reconciliation",
                now=now,
            )
        try:
            try:
                with self._external_call(self._lease_call_timeout(run)):
                    status = self._executor.poll(external_run)
            except TimeoutError as exc:
                raise ExecutorError(
                    error_code="NETWORK_ERROR",
                    error_summary="external poll timed out",
                ) from exc
        except ExecutorError as exc:
            observed_at = self._now()
            error_service = JobService(
                self._conn, company_id=candidate.company_id
            )
            if not self._executor_identity_still_matches(
                error_service,
                candidate.job_id,
            ):
                return self._mark_claimed_reconciliation(
                    candidate,
                    lease_token=lease_token,
                    error_code="EXECUTOR_MISMATCH",
                    error_summary="executor identity changed during polling",
                    now=observed_at,
                )
            plan = error_service.execution_plan(candidate.job_id)
            retry_stage_id = run.retry_stage_id
            retry_number = self._runs.retry_count_for_stage(
                candidate.company_id,
                candidate.job_id,
                retry_stage_id,
            ) + 1
            retryable = (
                self._retry_policy.is_retryable(exc.error_code)
                and retry_number <= plan.maximum_retries
            )
            if retryable:
                delay = self._retry_delay(
                    exc.error_code,
                    retry_number=retry_number,
                    requested_delay=exc.retry_after_seconds,
                )
                with transaction(self._conn):
                    recorded = self._runs.record_poll_transport_failure(
                        candidate,
                        lease_token=lease_token,
                        next_action_at=(
                            observed_at + timedelta(seconds=delay)
                        ).isoformat(),
                        current_stage=run.current_stage,
                        retry_stage_id=retry_stage_id,
                        error_code=exc.error_code,
                        error_summary=exc.error_summary,
                        now=observed_at.isoformat(),
                    )
                if not recorded:
                    raise StateConflict
                if self._runs.job_state(candidate) == JobState.CANCELED.value:
                    with transaction(self._conn):
                        reconciled = self._runs.reconcile_poll_after_local_cancel(
                            candidate,
                            terminal=False,
                            now=observed_at.isoformat(),
                        )
                    if not reconciled:
                        raise StateConflict
                return True
            if self._retry_policy.is_retryable(exc.error_code):
                canceled = self._runs.job_state(candidate) == JobState.CANCELED.value
                with transaction(self._conn):
                    if canceled:
                        marked = self._runs.record_exhausted_poll_after_local_cancel(
                            candidate,
                            lease_token=lease_token,
                            error_code=exc.error_code,
                            error_summary=exc.error_summary,
                            now=observed_at.isoformat(),
                        )
                    else:
                        marked = self._runs.mark_poll_reconciliation_required(
                            candidate,
                            lease_token=lease_token,
                            error_code=exc.error_code,
                            error_summary=exc.error_summary,
                            now=observed_at.isoformat(),
                        )
                if not marked:
                    raise StateConflict
                return True
            with transaction(self._conn):
                recorded = self._runs.record_poll_status(
                    candidate,
                    lease_token=lease_token,
                    external_status=ExternalStatus.FAILED_FINAL.value,
                    current_stage=run.current_stage,
                    retry_stage_id=retry_stage_id,
                    consume_retry=False,
                    next_action_at=None,
                    completion_observed_at=observed_at.isoformat(),
                    error_code=exc.error_code,
                    error_summary=exc.error_summary,
                    result_json=None,
                    result_hash=None,
                    now=observed_at.isoformat(),
                )
            if not recorded:
                raise StateConflict
            if self._runs.job_state(candidate) == JobState.CANCELED.value:
                with transaction(self._conn):
                    reconciled = self._runs.reconcile_poll_after_local_cancel(
                        candidate,
                        terminal=True,
                        now=observed_at.isoformat(),
                    )
                if not reconciled:
                    raise StateConflict
                return True
            JobService(
                self._conn,
                company_id=candidate.company_id,
                clock=lambda: observed_at,
            ).transition(
                candidate.job_id,
                JobState.RUNNING,
                JobState.FAILED_FINAL,
                exc.error_summary,
                error_code=exc.error_code,
                current_stage=run.current_stage,
            )
            return True
        except Exception:  # noqa: BLE001 - external outcome may be ambiguous.
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXECUTOR_POLL_OUTPUT_INVALID",
                error_summary="external poll outcome requires reconciliation",
                now=self._now(),
            )
        observed_at = self._now()
        if not self._durable_executor_matches(service, candidate, run):
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXECUTOR_MISMATCH",
                error_summary="executor identity changed during poll",
                now=observed_at,
            )
        next_action_at: str | None = None
        completion_at: str | None = None
        result_json: bytes | None = None
        result_hash: str | None = None
        target_state: JobState | None = None
        retry_stage_id = "poll:unknown"
        consume_retry = False
        try:
            if not isinstance(status, ExecutionStatus):
                raise DataIntegrityError
            status.__post_init__()
            retry_stage_id = f"poll:{status.stage_id or 'unknown'}"
            if status.external_run_id != external_run.external_run_id:
                raise DataIntegrityError
            error_code = status.error_code
            error_summary = status.error_summary
            if status.status in {ExternalStatus.ACCEPTED, ExternalStatus.RUNNING}:
                poll_delay = self._poll_delay(status.retry_after_seconds)
                next_action_at = (
                    observed_at + timedelta(seconds=poll_delay)
                ).isoformat()
            elif status.status is ExternalStatus.SUCCEEDED:
                if status.result is None:
                    raise DataIntegrityError
                plan = service.execution_plan(candidate.job_id)
                if not self._result_usage_matches_plan(status.result, plan):
                    raise DataIntegrityError
                result_json = execution_result_bytes(status.result)
                result_hash = hashlib.sha256(result_json).hexdigest()
                completion_at = observed_at.isoformat()
                target_state = JobState.SUCCEEDED
            elif status.status in {
                ExternalStatus.FAILED_RETRYABLE,
                ExternalStatus.FAILED_FINAL,
            }:
                if error_code is None or error_summary is None:
                    raise DataIntegrityError
                plan = JobService(
                    self._conn, company_id=candidate.company_id
                ).execution_plan(candidate.job_id)
                retry_number = self._runs.retry_count_for_stage(
                    candidate.company_id,
                    candidate.job_id,
                    retry_stage_id,
                ) + 1
                transient = self._retry_policy.is_retryable(error_code)
                retryable = (
                    status.status is ExternalStatus.FAILED_RETRYABLE
                    and transient
                    and retry_number <= plan.maximum_retries
                )
                consume_retry = status.status is ExternalStatus.FAILED_RETRYABLE and transient
                if retryable:
                    retry_delay = self._retry_delay(
                        error_code,
                        retry_number=retry_number,
                        requested_delay=status.retry_after_seconds,
                    )
                    next_action_at = (
                        observed_at + timedelta(seconds=retry_delay)
                    ).isoformat()
                    target_state = JobState.FAILED_RETRYABLE
                else:
                    completion_at = observed_at.isoformat()
                    target_state = JobState.FAILED_FINAL
            elif status.status is ExternalStatus.CANCELED:
                completion_at = observed_at.isoformat()
                target_state = JobState.CANCELED
            else:
                raise DataIntegrityError
        except (AttributeError, DataIntegrityError, TypeError, ValueError):
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXECUTOR_POLL_OUTPUT_INVALID",
                error_summary="external poll outcome requires reconciliation",
                now=observed_at,
            )

        with transaction(self._conn):
            recorded = self._runs.record_poll_status(
                candidate,
                lease_token=lease_token,
                external_status=(
                    target_state.value if target_state is not None else status.status.value
                ),
                current_stage=status.stage_id,
                retry_stage_id=retry_stage_id,
                consume_retry=consume_retry,
                next_action_at=next_action_at,
                completion_observed_at=completion_at,
                error_code=error_code,
                error_summary=error_summary,
                result_json=result_json,
                result_hash=result_hash,
                now=observed_at.isoformat(),
            )
            if (
                target_state is None
                and not self._runs.update_running_stage(
                    candidate, current_stage=status.stage_id
                )
                and self._runs.job_state(candidate) != JobState.CANCELED.value
            ):
                raise StateConflict
        if not recorded:
            raise StateConflict
        local_job = JobService(
            self._conn, company_id=candidate.company_id
        ).get_job(candidate.job_id)
        if local_job.state is JobState.CANCELED:
            with transaction(self._conn):
                reconciled = self._runs.reconcile_poll_after_local_cancel(
                    candidate,
                    terminal=target_state is not None,
                    now=observed_at.isoformat(),
                )
            if not reconciled:
                raise StateConflict
            return True
        if target_state is None:
            return True

        service = JobService(
            self._conn,
            company_id=candidate.company_id,
            clock=lambda: observed_at,
            artifact_store=self._artifact_store,
        )
        if target_state is JobState.SUCCEEDED:
            try:
                succeeded = service.transition(
                    candidate.job_id,
                    JobState.RUNNING,
                    JobState.SUCCEEDED,
                    "external execution succeeded",
                    current_stage=status.stage_id,
                )
            except StateConflict:
                if self._reconcile_terminal_cancel(candidate, observed_at):
                    return True
                raise
            if status.result is None:
                raise DataIntegrityError
            self._artifact_store.write_bundle(succeeded, status.result)
            service.bind_artifact_manifest(candidate.job_id)
        elif target_state in {JobState.FAILED_RETRYABLE, JobState.FAILED_FINAL}:
            if error_code is None or error_summary is None:
                raise DataIntegrityError
            try:
                service.transition(
                    candidate.job_id,
                    JobState.RUNNING,
                    target_state,
                    error_summary,
                    error_code=error_code,
                    current_stage=status.stage_id,
                )
            except StateConflict:
                if self._reconcile_terminal_cancel(candidate, observed_at):
                    return True
                raise
        else:
            service.transition(
                candidate.job_id,
                JobState.RUNNING,
                JobState.CANCELED,
                "external execution canceled",
                current_stage=status.stage_id,
            )
        return True

    def _requeue(self, candidate: RunnerCandidate, now: datetime) -> bool:
        claimed = self._claim_existing(
            candidate,
            now,
            expected_statuses=(ExternalStatus.FAILED_RETRYABLE.value,),
        )
        if claimed is None:
            return False
        _run, lease_token = claimed
        service = JobService(self._conn, company_id=candidate.company_id, clock=lambda: now)
        service.retry_job(candidate.job_id)
        with transaction(self._conn):
            consumed = self._runs.mark_retry_consumed(
                candidate,
                lease_token=lease_token,
                now=now.isoformat(),
            )
        if not consumed:
            raise StateConflict
        return True

    def _cancel(self, candidate: RunnerCandidate, now: datetime) -> bool:
        run = self._runs.get_run(
            candidate.company_id, candidate.job_id, candidate.attempt
        )
        if run.external_run_id is None:
            claim_now = self._now()
            lease_token = self._lease_token()
            lease_expires_at = claim_now + self._lease_duration
            with transaction(self._conn):
                claimed_lookup = self._runs.claim_canceled_lookup(
                    candidate,
                    lease_token=lease_token,
                    lease_expires_at=lease_expires_at.isoformat(),
                    now=claim_now.isoformat(),
                )
            if claimed_lookup is None:
                return False
            service = JobService(self._conn, company_id=candidate.company_id)
            try:
                job, snapshot = service.recover_canceled_dispatch(candidate.job_id)
                if not self._executor_identity_still_matches(service, candidate.job_id):
                    raise DataIntegrityError
                with self._external_call(self._lease_call_timeout(claimed_lookup)):
                    looked_up = self._executor.lookup(job, snapshot)
                if looked_up is None or not isinstance(looked_up, ExternalRun):
                    raise DataIntegrityError
                looked_up.__post_init__()
                if looked_up.idempotency_key != submission_idempotency_key(job):
                    raise DataIntegrityError
                accepted_at = _aware_utc(looked_up.accepted_at, name="accepted_at")
            except Exception:  # noqa: BLE001 - lookup never creates work; failure is terminal.
                with transaction(self._conn):
                    marked = self._runs.mark_claimed_cancel_lookup_reconciliation(
                        candidate,
                        lease_token=lease_token,
                        error_code="EXTERNAL_RUN_LOOKUP_FAILED",
                        error_summary="external run lookup requires reconciliation",
                        now=self._now().isoformat(),
                    )
                if not marked:
                    raise StateConflict
                return True
            observed_at = self._now()
            if (
                not self._executor_identity_still_matches(service, candidate.job_id)
                or not self._acceptance_provenance_valid(
                    service,
                    candidate,
                    claimed_lookup,
                    accepted_at=accepted_at,
                    observed_at=observed_at,
                )
            ):
                with transaction(self._conn):
                    marked = self._runs.mark_claimed_cancel_lookup_reconciliation(
                        candidate,
                        lease_token=lease_token,
                        error_code="EXTERNAL_ACCEPTANCE_PROVENANCE_INVALID",
                        error_summary="external acceptance provenance requires reconciliation",
                        now=observed_at.isoformat(),
                    )
                if not marked:
                    raise StateConflict
                return True
            try:
                with transaction(self._conn):
                    recorded = self._runs.record_lookup_acceptance_for_canceled(
                        candidate,
                        lease_token=lease_token,
                        external_run_id=looked_up.external_run_id,
                        executor_name=self._executor_name,
                        idempotency_key=looked_up.idempotency_key,
                        accepted_at=accepted_at.isoformat(),
                        now=observed_at.isoformat(),
                    )
            except sqlite3.Error:
                recorded = False
            if not recorded:
                with transaction(self._conn):
                    marked = self._runs.mark_claimed_cancel_lookup_reconciliation(
                        candidate,
                        lease_token=lease_token,
                        error_code="EXTERNAL_RUN_LOOKUP_PERSIST_FAILED",
                        error_summary="external run lookup persistence requires reconciliation",
                        now=self._now().isoformat(),
                    )
                if not marked:
                    raise StateConflict
                return True
            return True

        if run.external_status in {
            ExternalStatus.SUCCEEDED.value,
            ExternalStatus.FAILED_RETRYABLE.value,
            ExternalStatus.FAILED_FINAL.value,
        }:
            with transaction(self._conn):
                reconciled = self._runs.reconcile_terminal_after_local_cancel(
                    candidate,
                    now=now.isoformat(),
                )
            if not reconciled:
                raise StateConflict
            return True

        claim = self._claim_existing(
            candidate,
            now,
            expected_statuses=(
                ExternalStatus.ACCEPTED.value,
                ExternalStatus.RUNNING.value,
            ),
        )
        if claim is None:
            return False
        claimed_run, lease_token = claim
        authority_now = self._now()
        with transaction(self._conn):
            began_cancel = self._runs.begin_cancel_stage(
                candidate,
                lease_token=lease_token,
                now=authority_now.isoformat(),
            )
        if not began_cancel:
            raise StateConflict
        claimed_run = self._runs.get_run(
            candidate.company_id,
            candidate.job_id,
            candidate.attempt,
        )
        authority_service = JobService(
            self._conn,
            company_id=candidate.company_id,
            clock=lambda: authority_now,
        )
        try:
            authority_service.validate_external_cancellation(candidate.job_id)
            executor_matches = (
                self._durable_executor_matches(
                    authority_service,
                    candidate,
                    claimed_run,
                )
                and self._durable_acceptance_provenance_valid(
                    authority_service,
                    candidate,
                    claimed_run,
                )
            )
        except ApprovalInvalid:
            with transaction(self._conn):
                marked = self._runs.mark_known_cancel_reconciliation_required(
                    candidate,
                    lease_token=lease_token,
                    error_code="APPROVAL_INVALID",
                    error_summary="paid approval binding is not valid for external cancellation",
                    now=authority_now.isoformat(),
                )
            if not marked:
                raise StateConflict
            return True
        except (DataIntegrityError, StateConflict):
            executor_matches = False
        if not executor_matches:
            with transaction(self._conn):
                marked = self._runs.mark_known_cancel_reconciliation_required(
                    candidate,
                    lease_token=lease_token,
                    error_code="EXECUTOR_MISMATCH",
                    error_summary="selected executor does not match the approved plan",
                    now=authority_now.isoformat(),
                )
            if not marked:
                raise StateConflict
            return True
        try:
            external_run = self._external_run(claimed_run)
        except DataIntegrityError:
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="EXTERNAL_RUN_IDENTITY_INVALID",
                error_summary="durable external identity requires reconciliation",
                now=now,
            )
        try:
            with self._external_call(self._lease_call_timeout(claimed_run)):
                cancel_result = self._executor.cancel(external_run)
            confirmed = ExecutionStatus(
                external_run_id=cancel_result.external_run_id,
                status=cancel_result.status,
                stage_id=cancel_result.stage_id,
                retry_after_seconds=cancel_result.retry_after_seconds,
                error_code=cancel_result.error_code,
                error_summary=cancel_result.error_summary,
                result=cancel_result.result,
            )
            if (
                confirmed.external_run_id != external_run.external_run_id
                or confirmed.status is not ExternalStatus.CANCELED
            ):
                raise ValueError("executor cancel returned an invalid confirmation")
        except ExecutorError as exc:
            observed_at = self._now()
            error_service = JobService(
                self._conn, company_id=candidate.company_id
            )
            if not self._executor_identity_still_matches(
                error_service,
                candidate.job_id,
            ):
                with transaction(self._conn):
                    marked = self._runs.mark_known_cancel_reconciliation_required(
                        candidate,
                        lease_token=lease_token,
                        error_code="EXECUTOR_MISMATCH",
                        error_summary="executor identity changed during cancellation",
                        now=observed_at.isoformat(),
                    )
                if not marked:
                    raise StateConflict
                return True
            plan = error_service.execution_plan(candidate.job_id)
            retry_number = self._runs.retry_count_for_stage(
                candidate.company_id,
                candidate.job_id,
                "cancel",
            ) + 1
            retryable = (
                self._retry_policy.is_retryable(exc.error_code)
                and retry_number <= plan.maximum_retries
            )
            if retryable:
                delay = self._retry_delay(
                    exc.error_code,
                    retry_number=retry_number,
                    requested_delay=exc.retry_after_seconds,
                )
                with transaction(self._conn):
                    recorded = self._runs.record_cancel_transport_failure(
                        candidate,
                        lease_token=lease_token,
                        next_action_at=(
                            observed_at + timedelta(seconds=delay)
                        ).isoformat(),
                        error_code=exc.error_code,
                        error_summary=exc.error_summary,
                        now=observed_at.isoformat(),
                    )
                if not recorded:
                    raise StateConflict
                return True
            with transaction(self._conn):
                marked = self._runs.mark_known_cancel_reconciliation_required(
                    candidate,
                    lease_token=lease_token,
                    error_code=exc.error_code,
                    error_summary=exc.error_summary,
                    now=observed_at.isoformat(),
                )
            if not marked:
                raise StateConflict
            return True
        except Exception:  # noqa: BLE001 - external outcome may be ambiguous.
            observed_at = self._now()
            with transaction(self._conn):
                marked = self._runs.mark_known_cancel_reconciliation_required(
                    candidate,
                    lease_token=lease_token,
                    error_code="EXECUTOR_CANCEL_OUTPUT_INVALID",
                    error_summary="external cancellation outcome requires reconciliation",
                    now=observed_at.isoformat(),
                )
            if not marked:
                raise StateConflict
            return True
        canceled_at = self._now()
        if not self._durable_executor_matches(
            authority_service,
            candidate,
            claimed_run,
        ):
            with transaction(self._conn):
                marked = self._runs.mark_known_cancel_reconciliation_required(
                    candidate,
                    lease_token=lease_token,
                    error_code="EXECUTOR_MISMATCH",
                    error_summary="executor identity changed during cancellation",
                    now=canceled_at.isoformat(),
                )
            if not marked:
                raise StateConflict
            return True
        with transaction(self._conn):
            recorded = self._runs.record_remote_canceled(
                candidate,
                lease_token=lease_token,
                now=canceled_at.isoformat(),
            )
        if not recorded:
            raise StateConflict
        return True

    def _finalize_terminal(self, candidate: RunnerCandidate, now: datetime) -> bool:
        claim = self._claim_existing(
            candidate,
            now,
            expected_statuses=(
                ExternalStatus.SUCCEEDED.value,
                ExternalStatus.FAILED_RETRYABLE.value,
                ExternalStatus.FAILED_FINAL.value,
                ExternalStatus.CANCELED.value,
            ),
        )
        if claim is None:
            return False
        run, lease_token = claim
        try:
            finalized = self._finalize_claimed_terminal(candidate, run, now)
        except DataIntegrityError:
            return self._mark_claimed_reconciliation(
                candidate,
                lease_token=lease_token,
                error_code="TERMINAL_PROVENANCE_INVALID",
                error_summary="durable terminal result requires reconciliation",
                now=now,
            )
        except StateConflict:
            if self._runs.job_state(candidate) == JobState.CANCELED.value:
                with transaction(self._conn):
                    reconciled = self._runs.reconcile_claimed_terminal_after_local_cancel(
                        candidate,
                        lease_token=lease_token,
                        now=now.isoformat(),
                    )
                if not reconciled:
                    raise StateConflict
                return True
            with transaction(self._conn):
                self._runs.release_lease(
                    candidate,
                    lease_token=lease_token,
                    now=self._now().isoformat(),
                )
            raise
        except Exception:
            with transaction(self._conn):
                self._runs.release_lease(
                    candidate,
                    lease_token=lease_token,
                    now=self._now().isoformat(),
                )
            raise
        with transaction(self._conn):
            released = self._runs.release_lease(
                candidate,
                lease_token=lease_token,
                now=self._now().isoformat(),
            )
        if not released:
            raise StateConflict
        return finalized

    def _finalize_claimed_terminal(
        self,
        candidate: RunnerCandidate,
        run: ExecutionRunRecord,
        now: datetime,
    ) -> bool:
        self._external_run(run)
        service = JobService(self._conn, company_id=candidate.company_id)
        if not (
            self._durable_executor_matches(service, candidate, run)
            and self._durable_acceptance_provenance_valid(service, candidate, run)
        ):
            raise DataIntegrityError
        if run.external_status is None:
            raise DataIntegrityError
        try:
            status = ExternalStatus(run.external_status)
        except (TypeError, ValueError) as exc:
            raise DataIntegrityError from exc
        service = JobService(
            self._conn,
            company_id=candidate.company_id,
            clock=lambda: now,
            artifact_store=self._artifact_store,
        )
        job = service.get_job(candidate.job_id)
        if status is ExternalStatus.SUCCEEDED:
            if run.result_json is None or run.result_hash is None:
                raise DataIntegrityError
            payload = bytes(run.result_json)
            if hashlib.sha256(payload).hexdigest() != run.result_hash:
                raise DataIntegrityError
            try:
                result = execution_result_from_bytes(payload)
            except (TypeError, ValueError) as exc:
                raise DataIntegrityError from exc
            if not self._result_usage_matches_plan(
                result, service.execution_plan(candidate.job_id)
            ):
                raise DataIntegrityError
            if job.state is JobState.RUNNING:
                job = service.transition(
                    candidate.job_id,
                    JobState.RUNNING,
                    JobState.SUCCEEDED,
                    "recover durable external success",
                    current_stage=run.current_stage,
                )
            elif job.state is not JobState.SUCCEEDED:
                raise StateConflict
            self._artifact_store.write_bundle(job, result)
            service.bind_artifact_manifest(candidate.job_id)
            return True
        if status in {ExternalStatus.FAILED_RETRYABLE, ExternalStatus.FAILED_FINAL}:
            if run.error_code is None or run.error_summary is None:
                raise DataIntegrityError
            if job.state is JobState.QUEUED:
                if status is ExternalStatus.FAILED_RETRYABLE:
                    service.transition(
                        candidate.job_id,
                        JobState.QUEUED,
                        JobState.FAILED_RETRYABLE,
                        run.error_summary,
                        error_code=run.error_code,
                        current_stage=run.current_stage,
                    )
                    return True
                service.transition(
                    candidate.job_id,
                    JobState.QUEUED,
                    JobState.RUNNING,
                    "recover durable submission rejection",
                )
            elif job.state is not JobState.RUNNING:
                raise StateConflict
            service.transition(
                candidate.job_id,
                JobState.RUNNING,
                JobState(status.value),
                run.error_summary,
                error_code=run.error_code,
                current_stage=run.current_stage,
            )
            return True
        if job.state is not JobState.RUNNING:
            raise StateConflict
        if status is ExternalStatus.CANCELED:
            service.transition(
                candidate.job_id,
                JobState.RUNNING,
                JobState.CANCELED,
                "recover durable external cancellation",
                current_stage=run.current_stage,
            )
            return True
        raise DataIntegrityError

    def tick(self, limit: int = 1) -> int:
        """Process at most ``limit`` jobs and return the number durably advanced."""
        if type(limit) is not int or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        heartbeat_at = self._now()
        self._record_heartbeat(heartbeat_at)
        processed = 0
        for _index in range(limit):
            now = self._now()
            invalid_scheduler_candidate = self._runs.next_invalid_scheduler_candidate()
            if invalid_scheduler_candidate is not None:
                with transaction(self._conn):
                    quarantined = self._runs.quarantine_invalid_scheduler_timestamps(
                        invalid_scheduler_candidate,
                        now=now.isoformat(),
                    )
                if quarantined:
                    processed += 1
                    continue
            cancel_candidate = self._runs.next_cancel_candidate(now=now.isoformat())
            if cancel_candidate is not None:
                if not self._cancel(cancel_candidate, now):
                    break
                processed += 1
                continue
            terminal_candidate = self._runs.next_terminal_recovery_candidate(
                now=now.isoformat()
            )
            if terminal_candidate is not None:
                if not self._finalize_terminal(terminal_candidate, now):
                    break
                processed += 1
                continue
            poll_candidate = self._runs.next_poll_candidate(now=now.isoformat())
            if poll_candidate is not None:
                if not self._poll(poll_candidate, now):
                    break
                processed += 1
                continue
            retry_candidate = self._runs.next_retry_candidate(now=now.isoformat())
            if retry_candidate is not None:
                if not self._requeue(retry_candidate, now):
                    break
                processed += 1
                continue
            candidate = self._runs.next_recovery_candidate(
                now=now.isoformat(),
                stale_before=(now - self._stale_after).isoformat(),
            )
            if candidate is None:
                break
            if candidate.state == JobState.RUNNING.value and (
                JobService(self._conn, company_id=candidate.company_id)
                .get_job(candidate.job_id)
                .current_stage
                != "dispatching"
            ):
                self._mark_stale(candidate, now)
                processed += 1
                continue
            if candidate.state not in {
                JobState.QUEUED.value,
                JobState.RUNNING.value,
            }:
                raise DataIntegrityError
            if not self._submit(candidate, now):
                break
            processed += 1
        return processed
