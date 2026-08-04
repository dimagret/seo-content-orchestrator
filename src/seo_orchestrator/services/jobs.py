"""Company-scoped durable SEO job planning and state changes."""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from seo_orchestrator.canonical import sha256_fingerprint
from seo_orchestrator.db.connection import transaction
from seo_orchestrator.db.repositories import JobRecord, JobRepository, SnapshotRepository
from seo_orchestrator.domain import ExecutionPlan, JobState, PlannedJob, SeoJob
from seo_orchestrator.domain.approvals import canonical_plan_bytes, fingerprint_plan
from seo_orchestrator.domain.jobs import is_transition_allowed
from seo_orchestrator.errors import ApprovalInvalid, InvalidTransition, StateConflict

_FINISHED_STATES = frozenset(
    {JobState.SUCCEEDED, JobState.FAILED_FINAL, JobState.CANCELED, JobState.EXPORTED}
)


def _stored_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("stored job timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _domain_job(record: JobRecord) -> SeoJob:
    created_at = _stored_datetime(record.created_at)
    if created_at is None:
        raise ValueError("stored job creation timestamp is missing")
    return SeoJob(
        job_id=record.job_id,
        brief_id=record.brief_id,
        brief_fingerprint=record.brief_fingerprint,
        snapshot_id=record.snapshot_id,
        snapshot_hash=record.snapshot_hash,
        company_id=record.company_id,
        direction_id=record.direction_id,
        audience_segment_id=record.audience_segment_id,
        state=JobState(record.state),
        current_stage=record.current_stage,
        approved_plan_fingerprint=record.approved_plan_fingerprint,
        approval_record_id=record.approval_record_id,
        attempt=record.attempt,
        created_at=created_at,
        started_at=_stored_datetime(record.started_at),
        finished_at=_stored_datetime(record.finished_at),
        error_code=record.error_code,
        error_summary=record.error_summary,
        artifact_manifest_path=record.artifact_manifest_path,
    )


class JobService:
    """Own job mutations inside one authenticated company boundary."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        company_id: str,
        clock: Callable[[], datetime] | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._conn = conn
        self._company_id = company_id
        self._jobs = JobRepository(conn)
        self._snapshots = SnapshotRepository(conn)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._id_factory = id_factory or (lambda: f"job-{uuid.uuid4().hex}")

    def plan_job(self, snapshot_id: str, execution_plan: ExecutionPlan) -> PlannedJob:
        """Persist the exact plan and a PLANNED creation audit in one transaction."""
        plan_json = canonical_plan_bytes(execution_plan)
        plan_fingerprint = fingerprint_plan(execution_plan)
        now = self._clock()
        job_id = self._id_factory()
        with transaction(self._conn):
            snapshot = self._snapshots.get_snapshot(self._company_id, snapshot_id)
            context = snapshot.thawed_compiled_context()
            brief_value = context.get("brief") if type(context) is dict else None
            if type(brief_value) is not dict:
                raise ValueError("snapshot compiled context has no canonical brief")
            brief = brief_value
            created_by = brief.get("created_by")
            if type(created_by) is not str:
                raise ValueError("snapshot compiled brief has no creator")
            identifiers = {
                "brief_id": snapshot.brief_id,
                "company_id": snapshot.company_id,
                "direction_id": snapshot.direction_id,
                "audience_segment_id": snapshot.audience_segment_id,
            }
            if any(brief.get(name) != value for name, value in identifiers.items()):
                raise ValueError("snapshot metadata does not match its compiled brief")
            job = JobRecord(
                job_id=job_id,
                brief_id=snapshot.brief_id,
                brief_fingerprint=sha256_fingerprint(brief),
                snapshot_id=snapshot.snapshot_id,
                snapshot_hash=snapshot.snapshot_hash,
                company_id=snapshot.company_id,
                direction_id=snapshot.direction_id,
                audience_segment_id=snapshot.audience_segment_id,
                state=JobState.PLANNED.value,
                current_stage=None,
                approved_plan_fingerprint=None,
                approval_record_id=None,
                attempt=1,
                created_by=created_by,
                created_at=now.isoformat(),
                started_at=None,
                finished_at=None,
                error_code=None,
                error_summary=None,
                artifact_manifest_path=None,
                plan_json=plan_json,
                plan_fingerprint=plan_fingerprint,
            )
            self._jobs.add_job(job)
            self._jobs.append_transition(
                self._company_id,
                job_id,
                from_state=None,
                to_state=JobState.PLANNED.value,
                occurred_at=now.isoformat(),
                reason_summary=None,
            )
        return PlannedJob(
            job_id=job_id,
            snapshot_id=snapshot_id,
            snapshot_hash=snapshot.snapshot_hash,
            plan=execution_plan,
            plan_fingerprint=plan_fingerprint,
            state=JobState.PLANNED,
        )

    def transition(
        self,
        job_id: str,
        expected_state: JobState,
        target_state: JobState,
        reason: str,
    ) -> SeoJob:
        """Apply one scoped state CAS and append its audit in the same write lock."""
        with transaction(self._conn):
            record = self._jobs.get_job(self._company_id, job_id)
            actual_state = JobState(record.state)
            if (
                record.plan_json is None
                and record.plan_fingerprint is None
                and actual_state not in {JobState.DRAFT, JobState.VALIDATED}
            ):
                raise ApprovalInvalid
            try:
                expected = JobState(expected_state)
                target = JobState(target_state)
            except ValueError as exc:
                raise InvalidTransition from exc
            if actual_state is JobState.CANCELED and target is JobState.CANCELED:
                result = _domain_job(record)
            else:
                if actual_state is not expected:
                    raise StateConflict
                if not is_transition_allowed(actual_state, target):
                    raise InvalidTransition
                now = self._clock().isoformat()
                attempt = record.attempt + int(
                    actual_state is JobState.FAILED_RETRYABLE and target is JobState.QUEUED
                )
                started_at = record.started_at
                if target is JobState.RUNNING and started_at is None:
                    started_at = now
                finished_at = record.finished_at
                if target in _FINISHED_STATES and finished_at is None:
                    finished_at = now
                error_code = record.error_code
                error_summary = record.error_summary
                if target in {JobState.FAILED_RETRYABLE, JobState.FAILED_FINAL}:
                    error_summary = reason
                elif (
                    actual_state is JobState.FAILED_RETRYABLE
                    and target is JobState.QUEUED
                ):
                    error_code = None
                    error_summary = None
                changed = self._jobs.compare_and_swap_state(
                    self._company_id,
                    job_id,
                    expected_state=actual_state.value,
                    target_state=target.value,
                    attempt=attempt,
                    started_at=started_at,
                    finished_at=finished_at,
                    error_code=error_code,
                    error_summary=error_summary,
                )
                if not changed:
                    raise StateConflict
                self._jobs.append_transition(
                    self._company_id,
                    job_id,
                    from_state=actual_state.value,
                    to_state=target.value,
                    occurred_at=now,
                    reason_summary=reason,
                )
                result = _domain_job(self._jobs.get_job(self._company_id, job_id))
        return result
