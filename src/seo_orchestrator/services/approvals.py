"""Company-scoped atomic paid-execution approvals."""

from __future__ import annotations

import hashlib
import sqlite3
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from seo_orchestrator.db.connection import transaction
from seo_orchestrator.db.repositories import ApprovalRepository, JobRepository
from seo_orchestrator.domain import ApprovalRecord, JobState
from seo_orchestrator.domain.approvals import canonical_plan_bytes, deserialize_plan
from seo_orchestrator.errors import ApprovalInvalid, StateConflict


class ApprovalService:
    """Validate and bind approvals inside one authenticated company boundary."""

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
        self._approvals = ApprovalRepository(conn)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._id_factory = id_factory or (lambda: f"approval-{uuid.uuid4().hex}")

    def approve_job(
        self,
        job_id: str,
        actor_id: str,
        snapshot_hash: str,
        plan_fingerprint: str,
    ) -> ApprovalRecord:
        """Validate durable fingerprints, bind one approval, and queue atomically."""
        with transaction(self._conn):
            job = self._jobs.get_job(self._company_id, job_id)
            if job.state != JobState.AWAITING_PAID_APPROVAL.value:
                raise StateConflict
            payload = job.plan_json
            stored_fingerprint = job.plan_fingerprint
            if type(payload) is not bytes or type(stored_fingerprint) is not str:
                raise ApprovalInvalid
            recomputed_fingerprint = hashlib.sha256(payload).hexdigest()
            if recomputed_fingerprint != stored_fingerprint:
                raise ApprovalInvalid
            try:
                stored_plan = deserialize_plan(payload)
                canonical_payload = canonical_plan_bytes(stored_plan)
            except (TypeError, ValueError) as exc:
                raise ApprovalInvalid from exc
            if canonical_payload != payload:
                raise ApprovalInvalid
            if snapshot_hash != job.snapshot_hash:
                raise ApprovalInvalid
            if plan_fingerprint != stored_fingerprint:
                raise ApprovalInvalid

            now = self._clock()
            approval = ApprovalRecord(
                approval_record_id=self._id_factory(),
                job_id=job.job_id,
                approval_type="paid_execution",
                snapshot_hash=job.snapshot_hash,
                plan_fingerprint=stored_fingerprint,
                approved_by=actor_id,
                approved_at=now,
                expires_at=None,
            )
            self._approvals.add_approval(self._company_id, approval)
            bound = self._jobs.bind_paid_approval(
                self._company_id,
                job.job_id,
                expected_state=JobState.AWAITING_PAID_APPROVAL.value,
                plan_fingerprint=stored_fingerprint,
                approval_record_id=approval.approval_record_id,
            )
            if not bound:
                raise StateConflict
            self._jobs.append_transition(
                self._company_id,
                job.job_id,
                from_state=JobState.AWAITING_PAID_APPROVAL.value,
                to_state=JobState.QUEUED.value,
                occurred_at=now.isoformat(),
                reason_summary=None,
            )
        return approval
