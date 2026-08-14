"""Deterministic in-memory executor for local recovery and lifecycle tests."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from seo_orchestrator.domain import ExecutionSnapshot, SeoJob
from seo_orchestrator.executors.base import (
    ExecutionStatus,
    ExecutorError,
    ExternalRun,
    ExternalStatus,
    submission_idempotency_key,
)

_Identity = tuple[str, str, int, str, str, str, str, str]


class MockExecutor:
    """Scriptable executor with optional provider-side durable idempotency state."""

    name = "mock"

    def __init__(
        self,
        outcomes: Iterable[ExecutionStatus | ExecutorError] = (),
        *,
        submit_errors: Iterable[ExecutorError] = (),
        cancel_errors: Iterable[ExecutorError] = (),
        clock: Callable[[], datetime] | None = None,
        run_id_factory: Callable[[int], str] | None = None,
        state_path: Path | None = None,
        model_ids: tuple[str, ...] = ("writer-model-v1",),
        provider_ids: tuple[str, ...] = ("mock-provider",),
    ) -> None:
        if not model_ids or any(type(value) is not str or not value for value in model_ids):
            raise ValueError("model_ids must contain non-empty strings")
        if not provider_ids or any(
            type(value) is not str or not value for value in provider_ids
        ):
            raise ValueError("provider_ids must contain non-empty strings")
        self.model_ids = model_ids
        self.provider_ids = provider_ids
        self._outcomes = list(outcomes)
        self._submit_errors = list(submit_errors)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._run_id_factory = run_id_factory or (lambda number: f"mock-run-{number}")
        self._runs: dict[str, tuple[ExternalRun, _Identity]] = {}
        self._canceled: set[str] = set()
        self._submission_count = 0
        self.submit_call_count = 0
        self.lookup_call_count = 0
        self.poll_call_count = 0
        self.cancel_call_count = 0
        self.cancellation_count = 0
        self._cancel_errors = list(cancel_errors)
        self._state_path: Path | None = None
        if state_path is not None:
            self.configure_durable_state(state_path)

    @property
    def durable_semantic_idempotency(self) -> bool:
        return self._state_path is not None

    @property
    def side_effect_free_lookup(self) -> bool:
        return self._state_path is not None

    @property
    def idempotent_cancel(self) -> bool:
        return self._state_path is not None

    @property
    def cancel_confirms_terminal(self) -> bool:
        return self._state_path is not None

    @property
    def authority_deadline_enforced(self) -> bool:
        return self._state_path is not None

    @property
    def configuration_authorization_enforced(self) -> bool:
        return self._state_path is not None

    @property
    def submission_count(self) -> int:
        if self._state_path is None:
            return self._submission_count
        with self._state_connection() as connection:
            return int(connection.execute("SELECT COUNT(*) FROM mock_executor_runs").fetchone()[0])

    def configure_durable_state(self, state_path: Path) -> None:
        """Bind this mock to provider-side SQLite state before its first submission."""
        if not isinstance(state_path, Path):
            raise TypeError("state_path must be a Path")
        if self._runs or self._submission_count:
            raise RuntimeError("durable state must be configured before submit")
        selected = state_path.resolve(strict=False)
        if self._state_path is not None and self._state_path != selected:
            raise RuntimeError("mock durable state cannot be rebound")
        selected.parent.mkdir(parents=True, exist_ok=True)
        self._state_path = selected
        with self._state_connection() as connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS mock_executor_runs (
                       idempotency_key TEXT PRIMARY KEY,
                       identity_json TEXT NOT NULL,
                       external_run_id TEXT NOT NULL UNIQUE,
                       accepted_at TEXT NOT NULL,
                       canceled INTEGER NOT NULL DEFAULT 0 CHECK (canceled IN (0, 1))
                   )"""
            )

    @contextmanager
    def _state_connection(self) -> Iterator[sqlite3.Connection]:
        if self._state_path is None:
            raise RuntimeError("mock durable state is not configured")
        connection = sqlite3.connect(self._state_path, isolation_level=None)
        connection.execute("PRAGMA busy_timeout = 5000")
        try:
            yield connection
            if connection.in_transaction:
                connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _identity_json(identity: _Identity) -> str:
        return json.dumps(identity, ensure_ascii=True, separators=(",", ":"))

    def script_cancel_errors(self, *errors: ExecutorError) -> None:
        """Append deterministic normalized cancel transport failures."""
        if any(not isinstance(error, ExecutorError) for error in errors):
            raise TypeError("cancel errors must be ExecutorError instances")
        self._cancel_errors.extend(errors)

    @staticmethod
    def _identity(job: SeoJob, snapshot: ExecutionSnapshot) -> _Identity:
        if (
            job.snapshot_id != snapshot.snapshot_id
            or job.snapshot_hash != snapshot.snapshot_hash
            or job.brief_id != snapshot.brief_id
            or job.company_id != snapshot.company_id
            or job.direction_id != snapshot.direction_id
            or job.audience_segment_id != snapshot.audience_segment_id
        ):
            raise ValueError("job and snapshot identity do not match")
        return (
            job.company_id,
            job.job_id,
            job.attempt,
            job.brief_fingerprint,
            job.snapshot_id,
            job.snapshot_hash,
            job.approved_plan_fingerprint or "",
            job.approval_record_id or "",
        )

    def _submit(
        self,
        job: SeoJob,
        snapshot: ExecutionSnapshot,
        *,
        authority_expires_at: datetime | None,
    ) -> ExternalRun:
        self.submit_call_count += 1
        key = submission_idempotency_key(job)
        identity = self._identity(job, snapshot)
        if self._state_path is None:
            existing = self._runs.get(key)
            if existing is not None:
                run, original_identity = existing
                if original_identity != identity:
                    raise ValueError(
                        "idempotency key was reused with different immutable identity"
                    )
                return run
            if self._submit_errors:
                raise self._submit_errors.pop(0)
            accepted_at = self._accepted_at()
            self._validate_authority(accepted_at, authority_expires_at)
            self._submission_count += 1
            run = ExternalRun(
                external_run_id=self._run_id_factory(self._submission_count),
                idempotency_key=key,
                accepted_at=accepted_at,
            )
            self._runs[key] = (run, identity)
            return run

        identity_json = self._identity_json(identity)
        with self._state_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT identity_json, external_run_id, accepted_at
                   FROM mock_executor_runs WHERE idempotency_key = ?""",
                (key,),
            ).fetchone()
            if row is not None:
                if row[0] != identity_json:
                    raise ValueError(
                        "idempotency key was reused with different immutable identity"
                    )
                return ExternalRun(
                    external_run_id=row[1],
                    idempotency_key=key,
                    accepted_at=datetime.fromisoformat(row[2]),
                )
            if self._submit_errors:
                raise self._submit_errors.pop(0)
            sequence = int(
                connection.execute("SELECT COUNT(*) FROM mock_executor_runs").fetchone()[0]
            ) + 1
            accepted_at = self._accepted_at()
            self._validate_authority(accepted_at, authority_expires_at)
            run = ExternalRun(
                external_run_id=self._run_id_factory(sequence),
                idempotency_key=key,
                accepted_at=accepted_at,
            )
            connection.execute(
                """INSERT INTO mock_executor_runs(
                       idempotency_key, identity_json, external_run_id, accepted_at
                   ) VALUES (?, ?, ?, ?)""",
                (key, identity_json, run.external_run_id, run.accepted_at.isoformat()),
            )
            return run

    def _accepted_at(self) -> datetime:
        accepted_at = self._clock()
        if (
            type(accepted_at) is not datetime
            or accepted_at.tzinfo is None
            or accepted_at.utcoffset() is None
        ):
            raise ValueError("clock must return a timezone-aware datetime")
        return accepted_at

    @staticmethod
    def _validate_authority(
        accepted_at: datetime,
        authority_expires_at: datetime | None,
    ) -> None:
        if (
            authority_expires_at is not None
            and accepted_at.astimezone(UTC) >= authority_expires_at.astimezone(UTC)
        ):
            raise ExecutorError(
                error_code="APPROVAL_EXPIRED",
                error_summary="provider rejected submission after authority expiry",
            )

    def _stored_run(self, run: ExternalRun) -> tuple[ExternalRun, bool]:
        if self._state_path is None:
            stored = self._runs.get(run.idempotency_key)
            if stored is None or stored[0] != run:
                raise ValueError("unknown external run")
            return stored[0], run.external_run_id in self._canceled
        with self._state_connection() as connection:
            row = connection.execute(
                """SELECT external_run_id, accepted_at, canceled
                   FROM mock_executor_runs WHERE idempotency_key = ?""",
                (run.idempotency_key,),
            ).fetchone()
        if row is None:
            raise ValueError("unknown external run")
        stored_run = ExternalRun(
            external_run_id=row[0],
            idempotency_key=run.idempotency_key,
            accepted_at=datetime.fromisoformat(row[1]),
        )
        if stored_run != run:
            raise ValueError("unknown external run")
        return stored_run, bool(row[2])

    def poll(self, run: ExternalRun) -> ExecutionStatus:
        self.poll_call_count += 1
        _stored, canceled = self._stored_run(run)
        if canceled:
            return ExecutionStatus(
                external_run_id=run.external_run_id,
                status=ExternalStatus.CANCELED,
                stage_id=None,
                retry_after_seconds=None,
                error_code=None,
                error_summary=None,
                result=None,
            )
        if self._outcomes:
            outcome = self._outcomes.pop(0)
            if isinstance(outcome, ExecutorError):
                raise outcome
            if outcome.external_run_id != run.external_run_id:
                raise ValueError("scripted status belongs to a different external run")
            return outcome
        return ExecutionStatus(
            external_run_id=run.external_run_id,
            status=ExternalStatus.RUNNING,
            stage_id=None,
            retry_after_seconds=None,
            error_code=None,
            error_summary=None,
            result=None,
        )

    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        return self._submit(job, snapshot, authority_expires_at=None)

    def submit_authorized(
        self,
        job: SeoJob,
        snapshot: ExecutionSnapshot,
        *,
        authority_expires_at: datetime | None,
        approved_model_ids: tuple[str, ...],
        approved_provider_ids: tuple[str, ...],
    ) -> ExternalRun:
        if self.model_ids != approved_model_ids or self.provider_ids != approved_provider_ids:
            raise ExecutorError(
                error_code="EXECUTOR_CONFIGURATION_UNAUTHORIZED",
                error_summary="executor provider/model configuration is not approved",
            )
        return self._submit(
            job,
            snapshot,
            authority_expires_at=authority_expires_at,
        )

    def lookup(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun | None:
        """Return an existing attempt without creating a new mock execution."""
        self.lookup_call_count += 1
        key = submission_idempotency_key(job)
        identity = self._identity(job, snapshot)
        if self._state_path is None:
            stored = self._runs.get(key)
            if stored is None:
                return None
            if stored[1] != identity:
                raise ValueError("idempotency key reused with different execution identity")
            return stored[0]
        with self._state_connection() as connection:
            row = connection.execute(
                """SELECT identity_json, external_run_id, accepted_at
                   FROM mock_executor_runs WHERE idempotency_key = ?""",
                (key,),
            ).fetchone()
        if row is None:
            return None
        if row[0] != self._identity_json(identity):
            raise ValueError("idempotency key reused with different execution identity")
        return ExternalRun(
            external_run_id=row[1],
            idempotency_key=key,
            accepted_at=datetime.fromisoformat(row[2]),
        )

    @staticmethod
    def _cancel_confirmation(run: ExternalRun) -> ExecutionStatus:
        return ExecutionStatus(
            external_run_id=run.external_run_id,
            status=ExternalStatus.CANCELED,
            stage_id=None,
            retry_after_seconds=None,
            error_code=None,
            error_summary=None,
            result=None,
        )

    def cancel(self, run: ExternalRun) -> ExecutionStatus:
        self.cancel_call_count += 1
        self._stored_run(run)
        if self._cancel_errors:
            raise self._cancel_errors.pop(0)
        if self._state_path is None:
            if run.external_run_id not in self._canceled:
                self._canceled.add(run.external_run_id)
                self.cancellation_count += 1
            return self._cancel_confirmation(run)
        with self._state_connection() as connection:
            cursor = connection.execute(
                """UPDATE mock_executor_runs SET canceled = 1
                   WHERE idempotency_key = ? AND external_run_id = ? AND canceled = 0""",
                (run.idempotency_key, run.external_run_id),
            )
        if cursor.rowcount == 1:
            self.cancellation_count += 1
        return self._cancel_confirmation(run)
