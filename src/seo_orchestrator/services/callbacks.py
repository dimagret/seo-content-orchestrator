"""Durable, company-scoped acceptance of authenticated executor callbacks."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

from seo_orchestrator.db.connection import transaction
from seo_orchestrator.db.repositories import (
    WebhookCallbackReceiptRepository,
    WebhookNonceRepository,
)
from seo_orchestrator.domain import JobState
from seo_orchestrator.errors import CallbackRejected, NotFound
from seo_orchestrator.security.signatures import MAX_TIMESTAMP_SKEW_SECONDS
from seo_orchestrator.services.jobs import JobService


class CallbackService:
    """Verify durable callback correlation and reject replay before any later processing."""

    def __init__(self, conn: sqlite3.Connection, *, company_id: str) -> None:
        self._conn = conn
        self._company_id = company_id
        self._nonces = WebhookNonceRepository(conn)
        self._receipts = WebhookCallbackReceiptRepository(conn)

    def accept_callback(
        self,
        *,
        job_id: str,
        snapshot_hash: str,
        idempotency_key: str,
        nonce: str,
        signed_timestamp: int,
        received_at: datetime,
    ) -> bool:
        """Accept one active scoped callback exactly once inside a write transaction."""
        if type(idempotency_key) is not str or idempotency_key != job_id:
            raise CallbackRejected
        if (
            type(received_at) is not datetime
            or received_at.tzinfo is None
            or received_at.utcoffset() is None
        ):
            raise ValueError("callback receipt time must be timezone-aware")
        if type(signed_timestamp) is not int or signed_timestamp < 0:
            raise ValueError("callback signature timestamp must be a non-negative integer")
        received_at = received_at.astimezone(UTC)
        try:
            signed_at = datetime.fromtimestamp(signed_timestamp, UTC)
        except (OverflowError, OSError, ValueError) as exc:
            raise ValueError("callback signature timestamp is out of range") from exc
        # Signatures are valid at the inclusive whole-second skew boundary. Keep the
        # nonce one further second after that window so repository pruning cannot
        # reopen a replay at its final valid instant.
        expires_at = max(received_at, signed_at) + timedelta(
            seconds=MAX_TIMESTAMP_SKEW_SECONDS + 1
        )
        with transaction(self._conn):
            try:
                job = JobService(self._conn, company_id=self._company_id).get_job(job_id)
            except NotFound as exc:
                raise CallbackRejected from exc
            if job.snapshot_hash != snapshot_hash:
                raise CallbackRejected
            if not self._nonces.consume_nonce(
                nonce,
                received_at,
                expires_at,
            ):
                raise CallbackRejected
            inserted = self._receipts.record_receipt(
                company_id=self._company_id,
                job_id=job_id,
                snapshot_hash=snapshot_hash,
                idempotency_key=idempotency_key,
                received_at=received_at,
            )
            if not inserted:
                return False
            if job.state not in {JobState.QUEUED, JobState.RUNNING}:
                raise CallbackRejected
            return True
