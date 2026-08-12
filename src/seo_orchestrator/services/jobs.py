"""Company-scoped durable SEO job planning and state changes."""

from __future__ import annotations

import hashlib
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from typing import BinaryIO

from seo_orchestrator.canonical import sha256_fingerprint
from seo_orchestrator.db.connection import transaction
from seo_orchestrator.db.repositories import (
    ApprovalRepository,
    JobRecord,
    JobRepository,
    SnapshotRepository,
)
from seo_orchestrator.domain import (
    ApprovalRecord,
    ExecutionPlan,
    ExecutionSnapshot,
    JobState,
    PlannedJob,
    SeoJob,
)
from seo_orchestrator.domain.approvals import (
    canonical_plan_bytes,
    deserialize_plan,
    fingerprint_plan,
)
from seo_orchestrator.domain.jobs import is_transition_allowed
from seo_orchestrator.errors import (
    ApprovalInvalid,
    DataIntegrityError,
    InvalidTransition,
    NotFound,
    StateConflict,
)
from seo_orchestrator.services.artifacts import ArtifactStore

_FINISHED_STATES = frozenset(
    {JobState.SUCCEEDED, JobState.FAILED_FINAL, JobState.CANCELED, JobState.EXPORTED}
)
_RESERVED_APPROVAL_EDGES = frozenset(
    {
        (JobState.AWAITING_PAID_APPROVAL, JobState.QUEUED),
        (JobState.AWAITING_EXPORT_APPROVAL, JobState.EXPORTED),
    }
)


def _require_plan_integrity(record: JobRecord) -> ExecutionPlan:
    payload = record.plan_json
    fingerprint = record.plan_fingerprint
    if type(payload) is not bytes or type(fingerprint) is not str:
        raise DataIntegrityError
    if hashlib.sha256(payload).hexdigest() != fingerprint:
        raise DataIntegrityError
    try:
        plan = deserialize_plan(payload)
    except (TypeError, ValueError) as exc:
        raise DataIntegrityError from exc
    if canonical_plan_bytes(plan) != payload:
        raise DataIntegrityError
    return plan


def _require_job_snapshot_integrity(
    record: JobRecord, snapshot: ExecutionSnapshot
) -> None:
    context = snapshot.thawed_compiled_context()
    brief = context.get("brief") if type(context) is dict else None
    if type(brief) is not dict:
        raise DataIntegrityError
    if (
        record.snapshot_id != snapshot.snapshot_id
        or record.snapshot_hash != snapshot.snapshot_hash
        or record.brief_id != snapshot.brief_id
        or record.company_id != snapshot.company_id
        or record.direction_id != snapshot.direction_id
        or record.audience_segment_id != snapshot.audience_segment_id
        or record.created_by != brief.get("created_by")
        or record.brief_fingerprint != sha256_fingerprint(brief)
    ):
        raise DataIntegrityError


def _stored_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if type(value) is not str:
        raise DataIntegrityError
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise DataIntegrityError from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DataIntegrityError
    return parsed.astimezone(UTC)


def _clock_datetime(value: object) -> datetime:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock must return a timezone-aware datetime")
    return value.astimezone(UTC)


def _domain_job(record: JobRecord) -> SeoJob:
    try:
        for required_value in (
            record.job_id,
            record.brief_id,
            record.snapshot_id,
            record.company_id,
            record.direction_id,
            record.audience_segment_id,
            record.created_by,
        ):
            if type(required_value) is not str or not required_value.strip():
                raise DataIntegrityError
        for hash_value in (record.brief_fingerprint, record.snapshot_hash):
            if (
                type(hash_value) is not str
                or len(hash_value) != 64
                or any(
                    character not in "0123456789abcdef" for character in hash_value
                )
            ):
                raise DataIntegrityError
        for optional_value in (
            record.current_stage,
            record.error_code,
            record.error_summary,
            record.artifact_manifest_path,
        ):
            if optional_value is not None and type(optional_value) is not str:
                raise DataIntegrityError
        if (
            record.approved_plan_fingerprint is not None
            and (
                type(record.approved_plan_fingerprint) is not str
                or len(record.approved_plan_fingerprint) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in record.approved_plan_fingerprint
                )
            )
        ):
            raise DataIntegrityError
        if record.approval_record_id is not None and (
            type(record.approval_record_id) is not str
            or not record.approval_record_id.strip()
        ):
            raise DataIntegrityError
        if record.superseded_by_job_id is not None and (
            type(record.superseded_by_job_id) is not str
            or not record.superseded_by_job_id.strip()
        ):
            raise DataIntegrityError
        if type(record.attempt) is not int or record.attempt < 1:
            raise DataIntegrityError
        created_at = _stored_datetime(record.created_at)
        if created_at is None:
            raise DataIntegrityError
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
    except DataIntegrityError:
        raise
    except (TypeError, ValueError) as exc:
        raise DataIntegrityError from exc


def _domain_job_with_snapshot(
    record: JobRecord, snapshot: ExecutionSnapshot
) -> SeoJob:
    _require_job_snapshot_integrity(record, snapshot)
    return replace(
        _domain_job(record),
        company_profile_version=snapshot.company_profile_version,
        direction_version=snapshot.direction_version,
        audience_version=snapshot.audience_version,
        prompt_set_version=snapshot.prompt_set_version,
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
        artifact_store: ArtifactStore | None = None,
    ) -> None:
        self._conn = conn
        self._company_id = company_id
        self._jobs = JobRepository(conn)
        self._snapshots = SnapshotRepository(conn)
        self._approvals = ApprovalRepository(conn)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._id_factory = id_factory or (lambda: f"job-{uuid.uuid4().hex}")
        if artifact_store is not None and not isinstance(artifact_store, ArtifactStore):
            raise TypeError("artifact_store must be an ArtifactStore")
        self._artifact_store = artifact_store

    def get_job(self, job_id: str) -> SeoJob:
        """Return one scoped job only after its immutable snapshot verifies."""
        record = self._jobs.get_job(self._company_id, job_id)
        snapshot = self._snapshots.get_snapshot(self._company_id, record.snapshot_id)
        return _domain_job_with_snapshot(record, snapshot)

    def bind_artifact_manifest(self, job_id: str) -> SeoJob:
        """Durably attach one published manifest to a succeeded job exactly once."""
        if self._artifact_store is None:
            raise ValueError("artifact_store is required to bind an artifact manifest")
        with transaction(self._conn):
            record = self._jobs.get_job(self._company_id, job_id)
            snapshot = self._snapshots.get_snapshot(self._company_id, record.snapshot_id)
            job = _domain_job_with_snapshot(record, snapshot)
            if job.state is not JobState.SUCCEEDED:
                raise StateConflict
            manifest_path_text = str(
                self._artifact_store.manifest_path_for_job(self._company_id, job_id)
            )
            if record.artifact_manifest_path is None:
                verified_path_text = str(self._artifact_store.verify_manifest_for_job(job))
                if verified_path_text != manifest_path_text:
                    raise DataIntegrityError
                if not self._jobs.bind_artifact_manifest(
                    self._company_id, job_id, verified_path_text
                ):
                    raise StateConflict
                record = self._jobs.get_job(self._company_id, job_id)
            elif record.artifact_manifest_path != manifest_path_text:
                raise DataIntegrityError
            return _domain_job_with_snapshot(record, snapshot)

    def open_artifact(self, job_id: str, name: str) -> BinaryIO:
        """Open one durably bound artifact through the job authority boundary."""
        if self._artifact_store is None:
            raise ValueError("artifact_store is required to open an artifact")
        job = self.get_job(job_id)
        if job.state is not JobState.SUCCEEDED or job.artifact_manifest_path is None:
            raise NotFound
        return self._artifact_store.open_artifact_for_job(job, name)

    def request_paid_approval(self, job_id: str) -> SeoJob:
        """Move one verified planned job to its explicit manual approval gate."""
        return self.transition(
            job_id,
            JobState.PLANNED,
            JobState.AWAITING_PAID_APPROVAL,
            "paid execution approval requested",
        )

    def cancel_job(self, job_id: str, expected_state: JobState) -> SeoJob:
        """Cancel only a queued or running job, preserving durable CAS semantics."""
        if expected_state not in {JobState.QUEUED, JobState.RUNNING}:
            raise InvalidTransition
        return self.transition(
            job_id,
            expected_state,
            JobState.CANCELED,
            "job canceled through Worker API",
        )

    def retry_job(self, job_id: str) -> SeoJob:
        """Requeue only a durable retryable failure and increment its attempt."""
        return self.transition(
            job_id,
            JobState.FAILED_RETRYABLE,
            JobState.QUEUED,
            "job retry requested through Worker API",
        )

    def _paid_approval(self, record: JobRecord) -> ApprovalRecord:
        approval_id = record.approval_record_id
        if (
            type(approval_id) is not str
            or record.approved_plan_fingerprint != record.plan_fingerprint
        ):
            raise DataIntegrityError
        try:
            approval = self._approvals.get_approval(
                self._company_id, record.job_id, approval_id
            )
        except NotFound as exc:
            raise DataIntegrityError from exc
        if (
            approval.approval_record_id != approval_id
            or approval.job_id != record.job_id
            or approval.approval_type != "paid_execution"
            or approval.snapshot_hash != record.snapshot_hash
            or approval.plan_fingerprint != record.plan_fingerprint
        ):
            raise DataIntegrityError
        return approval

    def plan_job(self, snapshot_id: str, execution_plan: ExecutionPlan) -> PlannedJob:
        """Persist the exact plan and a PLANNED creation audit in one transaction."""
        plan_json = canonical_plan_bytes(execution_plan)
        plan_fingerprint = fingerprint_plan(execution_plan)
        now = _clock_datetime(self._clock())
        job_id = self._id_factory()
        with transaction(self._conn):
            snapshot = self._snapshots.get_snapshot(self._company_id, snapshot_id)
            context = snapshot.thawed_compiled_context()
            brief_value = context.get("brief") if type(context) is dict else None
            if type(brief_value) is not dict:
                raise DataIntegrityError
            brief = brief_value
            created_by = brief.get("created_by")
            if type(created_by) is not str:
                raise DataIntegrityError
            identifiers = {
                "brief_id": snapshot.brief_id,
                "company_id": snapshot.company_id,
                "direction_id": snapshot.direction_id,
                "audience_segment_id": snapshot.audience_segment_id,
            }
            if any(brief.get(name) != value for name, value in identifiers.items()):
                raise DataIntegrityError
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
            self._jobs.supersede_pending_jobs(
                self._company_id, created_by, replacement_job_id=job_id
            )
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
            hydrated_record = _domain_job(record)
            actual_state = hydrated_record.state
            try:
                expected = JobState(expected_state)
                target = JobState(target_state)
            except ValueError as exc:
                raise InvalidTransition from exc
            if actual_state is JobState.CANCELED and target is JobState.CANCELED:
                result = hydrated_record
            else:
                if record.superseded_by_job_id is not None:
                    raise StateConflict
                if actual_state is not expected:
                    raise StateConflict
                if (actual_state, target) in _RESERVED_APPROVAL_EDGES:
                    raise InvalidTransition
                if not is_transition_allowed(actual_state, target):
                    raise InvalidTransition
                if not (
                    actual_state is JobState.DRAFT and target is JobState.VALIDATED
                ):
                    _require_plan_integrity(record)
                    snapshot = self._snapshots.get_snapshot(
                        self._company_id, record.snapshot_id
                    )
                    _require_job_snapshot_integrity(record, snapshot)
                transition_time: datetime | None = None
                approval: ApprovalRecord | None = None
                if target in {
                    JobState.RUNNING,
                    JobState.SUCCEEDED,
                    JobState.AWAITING_EXPORT_APPROVAL,
                    JobState.EXPORTED,
                }:
                    approval = self._paid_approval(record)
                if target is JobState.RUNNING:
                    transition_time = _clock_datetime(self._clock())
                    if approval is None:
                        raise DataIntegrityError
                    if (
                        approval.approved_at > transition_time
                        or (
                            approval.expires_at is not None
                            and (
                                approval.expires_at <= approval.approved_at
                                or approval.expires_at <= transition_time
                            )
                        )
                    ):
                        raise ApprovalInvalid
                if transition_time is None:
                    transition_time = _clock_datetime(self._clock())
                now = transition_time.isoformat()
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
                result_record = self._jobs.get_job(self._company_id, job_id)
                result_snapshot = self._snapshots.get_snapshot(
                    self._company_id, result_record.snapshot_id
                )
                result = _domain_job_with_snapshot(result_record, result_snapshot)
        return result
