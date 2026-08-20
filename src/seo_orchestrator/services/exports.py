"""Approval-gated, provider-neutral Google Sheets export contracts."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Protocol, cast
from urllib.parse import quote

from seo_orchestrator.canonical import MAX_CANONICAL_BYTES, JsonValue, canonical_json
from seo_orchestrator.db.connection import transaction
from seo_orchestrator.db.repositories import (
    ApprovalRepository,
    JobRecord,
    JobRepository,
)
from seo_orchestrator.domain import ApprovalRecord, JobState
from seo_orchestrator.errors import ApprovalInvalid, DataIntegrityError, NotFound, StateConflict

_SHA256 = frozenset("0123456789abcdef")


def _non_empty_text(value: object, field_name: str, *, maximum: int = 256) -> str:
    if type(value) is not str or not value.strip() or value != value.strip():
        raise ValueError(f"{field_name} must be non-empty trimmed text")
    if len(value) > maximum or "\x00" in value:
        raise ValueError(f"{field_name} is invalid")
    return value


def _sha256(value: object, field_name: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in _SHA256 for character in value)
    ):
        raise ValueError(f"{field_name} must be a lowercase SHA-256")
    return value


def _frozen_string_mapping(
    value: object,
    field_name: str,
    *,
    maximum_items: int = 64,
    maximum_value: int = 64,
) -> dict[str, str]:
    if type(value) is not dict or not value or len(value) > maximum_items:
        raise ValueError(f"{field_name} must be a non-empty bounded dictionary")
    copied: dict[str, str] = {}
    for key, item in value.items():
        copied[_non_empty_text(key, f"{field_name} key", maximum=64)] = _non_empty_text(
            item, f"{field_name} value", maximum=maximum_value
        )
    return cast(dict[str, str], MappingProxyType(copied))


def _frozen_artifact_values(value: object) -> dict[str, str]:
    if type(value) is not dict or not value or len(value) > 64:
        raise ValueError("artifact values must be a non-empty bounded dictionary")
    copied: dict[str, str] = {}
    for key, item in value.items():
        canonical_key = _non_empty_text(key, "artifact values key", maximum=64)
        if (
            type(item) is not str
            or len(item) > MAX_CANONICAL_BYTES
            or "\x00" in item
        ):
            raise ValueError("artifact cell value is invalid")
        copied[canonical_key] = item
    canonical_json(cast(JsonValue, copied))
    return cast(dict[str, str], MappingProxyType(copied))


@dataclass(frozen=True, slots=True)
class SheetExportTarget:
    spreadsheet_id: str
    sheet_name: str
    row_selector: str
    column_map: dict[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "spreadsheet_id",
            _non_empty_text(self.spreadsheet_id, "spreadsheet_id"),
        )
        object.__setattr__(
            self, "sheet_name", _non_empty_text(self.sheet_name, "sheet_name")
        )
        object.__setattr__(
            self,
            "row_selector",
            _non_empty_text(self.row_selector, "row_selector", maximum=512),
        )
        object.__setattr__(
            self,
            "column_map",
            _frozen_string_mapping(self.column_map, "column_map"),
        )


@dataclass(frozen=True, slots=True)
class ExportArtifact:
    artifact_hash: str
    values: dict[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "artifact_hash", _sha256(self.artifact_hash, "artifact_hash")
        )
        object.__setattr__(
            self,
            "values",
            _frozen_artifact_values(self.values),
        )


@dataclass(frozen=True, slots=True)
class ExportPlan:
    job_id: str
    artifact_hash: str
    spreadsheet_id: str
    sheet_name: str
    row_selector: str
    column_map: dict[str, str]
    plan_fingerprint: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "job_id", _non_empty_text(self.job_id, "job_id"))
        object.__setattr__(
            self, "artifact_hash", _sha256(self.artifact_hash, "artifact_hash")
        )
        object.__setattr__(
            self,
            "spreadsheet_id",
            _non_empty_text(self.spreadsheet_id, "spreadsheet_id"),
        )
        object.__setattr__(
            self, "sheet_name", _non_empty_text(self.sheet_name, "sheet_name")
        )
        object.__setattr__(
            self,
            "row_selector",
            _non_empty_text(self.row_selector, "row_selector", maximum=512),
        )
        object.__setattr__(
            self,
            "column_map",
            _frozen_string_mapping(dict(self.column_map), "column_map"),
        )
        object.__setattr__(
            self,
            "plan_fingerprint",
            _sha256(self.plan_fingerprint, "plan_fingerprint"),
        )


@dataclass(frozen=True, slots=True)
class ExportReceipt:
    job_id: str
    export_key: str
    destination_reference: str
    exported_at: datetime


@dataclass(frozen=True, slots=True)
class SheetWriteRequest:
    job_id: str
    export_key: str
    spreadsheet_id: str
    sheet_name: str
    row_selector: str
    destination_reference: str
    cells: dict[str, str]


@dataclass(frozen=True, slots=True)
class SheetWriteReceipt:
    export_key: str
    spreadsheet_id: str
    sheet_name: str
    row_selector: str
    destination_reference: str
    exported_at: datetime


@dataclass(frozen=True, slots=True)
class _ClaimedWrite:
    request: SheetWriteRequest
    attempt_id: str
    write_allowed: bool


class ExportArtifactSource(Protocol):
    def load_export_artifact(self, job: JobRecord) -> ExportArtifact: ...


class SheetExportBackend(Protocol):
    def write(self, request: SheetWriteRequest) -> SheetWriteReceipt: ...

    def lookup(self, request: SheetWriteRequest) -> SheetWriteReceipt | None: ...


class MockSheetExportBackend:
    """Local-only backend. Preparing a plan never invokes a write."""

    def __init__(self, *, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock or (lambda: datetime.now(UTC))
        self._rows: dict[str, tuple[SheetWriteRequest, SheetWriteReceipt]] = {}

    @property
    def write_count(self) -> int:
        return len(self._rows)

    @property
    def row_count(self) -> int:
        return len(self._rows)

    def write(self, request: SheetWriteRequest) -> SheetWriteReceipt:
        existing = self._rows.get(request.export_key)
        if existing is not None:
            if existing[0] != request:
                raise StateConflict
            return existing[1]
        exported_at = self._clock()
        if exported_at.tzinfo is None or exported_at.utcoffset() is None:
            raise ValueError("sheet backend clock must be timezone-aware")
        receipt = SheetWriteReceipt(
            export_key=request.export_key,
            spreadsheet_id=request.spreadsheet_id,
            sheet_name=request.sheet_name,
            row_selector=request.row_selector,
            destination_reference=request.destination_reference,
            exported_at=exported_at.astimezone(UTC),
        )
        self._rows[request.export_key] = (request, receipt)
        return receipt

    def lookup(self, request: SheetWriteRequest) -> SheetWriteReceipt | None:
        existing = self._rows.get(request.export_key)
        if existing is None:
            return None
        if existing[0] != request:
            raise StateConflict
        return existing[1]


def _plan_value(job_id: str, artifact: ExportArtifact, target: SheetExportTarget) -> JsonValue:
    return {
        "job_id": job_id,
        "artifact_hash": artifact.artifact_hash,
        "spreadsheet_id": target.spreadsheet_id,
        "sheet_name": target.sheet_name,
        "row_selector": target.row_selector,
        "column_map": dict(target.column_map),
    }


def _destination_reference(target: SheetExportTarget) -> str:
    reference = (
        f"sheet://{quote(target.spreadsheet_id, safe='')}/"
        f"{quote(target.sheet_name, safe='')}"
        f"?row={quote(target.row_selector, safe='')}"
    )
    return _non_empty_text(reference, "destination_reference", maximum=2048)


class ExportService:
    """Prepare durable export intent without crossing the external-write gate."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        company_id: str,
        artifact_source: ExportArtifactSource,
        backend: SheetExportBackend,
        clock: Callable[[], datetime] | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._conn = conn
        self._company_id = _non_empty_text(company_id, "company_id")
        self._jobs = JobRepository(conn)
        self._approvals = ApprovalRepository(conn)
        self._artifact_source = artifact_source
        self._backend = backend
        self._clock = clock or (lambda: datetime.now(UTC))
        self._id_factory = id_factory or (lambda: f"approval-{uuid.uuid4().hex}")

    def prepare_export(self, job_id: str, target: SheetExportTarget) -> ExportPlan:
        job_id = _non_empty_text(job_id, "job_id")
        if type(target) is not SheetExportTarget:
            raise TypeError("target must be a SheetExportTarget")
        _destination_reference(target)
        with transaction(self._conn):
            job = self._jobs.get_job(self._company_id, job_id)
            if job.state != JobState.SUCCEEDED.value or job.artifact_manifest_path is None:
                raise StateConflict
            artifact = self._artifact_source.load_export_artifact(job)
            missing = set(target.column_map.values()) - set(artifact.values)
            if missing:
                raise ValueError("column_map references an unavailable artifact value")
            plan_value = _plan_value(job.job_id, artifact, target)
            plan_fingerprint = hashlib.sha256(canonical_json(plan_value)).hexdigest()
            claimed = self._conn.execute(
                """SELECT plan_fingerprint FROM sheet_export_claims
                   WHERE company_id = ? AND job_id = ?""",
                (self._company_id, job.job_id),
            ).fetchone()
            if claimed is not None and claimed[0] != plan_fingerprint:
                raise StateConflict
            cells = {
                column: artifact.values[source]
                for column, source in target.column_map.items()
            }
            if any(value.startswith(("=", "+", "-", "@")) for value in cells.values()):
                raise ValueError("formula-like sheet cell requires explicit escaping")
            prepared_at = self._clock()
            if prepared_at.tzinfo is None or prepared_at.utcoffset() is None:
                raise ValueError("export clock must be timezone-aware")
            self._conn.execute(
                """INSERT OR IGNORE INTO sheet_export_plans(
                       company_id, job_id, artifact_hash, spreadsheet_id,
                       sheet_name, row_selector, column_map_json, payload_json,
                       plan_fingerprint, prepared_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    self._company_id,
                    job.job_id,
                    artifact.artifact_hash,
                    target.spreadsheet_id,
                    target.sheet_name,
                    target.row_selector,
                    canonical_json(cast(JsonValue, dict(target.column_map))),
                    canonical_json(cast(JsonValue, {"cells": cells})),
                    plan_fingerprint,
                    prepared_at.astimezone(UTC).isoformat(),
                ),
            )
            _, durable_target, durable_cells, durable_fingerprint = (
                self._validated_current_plan_locked(job)
            )
            if (
                durable_target != target
                or durable_cells != cells
                or durable_fingerprint != plan_fingerprint
            ):
                raise StateConflict
        return ExportPlan(
            job_id=job.job_id,
            artifact_hash=artifact.artifact_hash,
            spreadsheet_id=target.spreadsheet_id,
            sheet_name=target.sheet_name,
            row_selector=target.row_selector,
            column_map=dict(target.column_map),
            plan_fingerprint=plan_fingerprint,
        )

    def _validated_current_plan_locked(
        self, job: JobRecord
    ) -> tuple[ExportArtifact, SheetExportTarget, dict[str, str], str]:
        latest = self._conn.execute(
            """SELECT artifact_hash, spreadsheet_id, sheet_name, row_selector,
                      column_map_json, payload_json, plan_fingerprint
               FROM sheet_export_plans
               WHERE company_id = ? AND job_id = ?
               ORDER BY rowid DESC LIMIT 1""",
            (self._company_id, job.job_id),
        ).fetchone()
        if latest is None:
            raise ApprovalInvalid
        try:
            if type(latest[4]) is not bytes or type(latest[5]) is not bytes:
                raise DataIntegrityError
            column_map_value = json.loads(latest[4])
            if (
                type(column_map_value) is not dict
                or canonical_json(cast(JsonValue, column_map_value)) != latest[4]
            ):
                raise DataIntegrityError
            target = SheetExportTarget(
                spreadsheet_id=latest[1],
                sheet_name=latest[2],
                row_selector=latest[3],
                column_map=cast(dict[str, str], column_map_value),
            )
            _destination_reference(target)
            artifact = self._artifact_source.load_export_artifact(job)
            if artifact.artifact_hash != latest[0]:
                raise DataIntegrityError
            missing = set(target.column_map.values()) - set(artifact.values)
            if missing:
                raise DataIntegrityError
            cells = {
                column: artifact.values[source]
                for column, source in target.column_map.items()
            }
            if any(
                value.startswith(("=", "+", "-", "@"))
                for value in cells.values()
            ):
                raise DataIntegrityError
            if canonical_json(cast(JsonValue, {"cells": cells})) != latest[5]:
                raise DataIntegrityError
            expected_fingerprint = hashlib.sha256(
                canonical_json(_plan_value(job.job_id, artifact, target))
            ).hexdigest()
            if latest[6] != expected_fingerprint:
                raise DataIntegrityError
        except (TypeError, ValueError, UnicodeDecodeError) as exc:
            raise DataIntegrityError from exc
        return artifact, target, cells, expected_fingerprint

    def approve_export(
        self, job_id: str, plan_fingerprint: str, actor_id: str
    ) -> ApprovalRecord:
        job_id = _non_empty_text(job_id, "job_id")
        plan_fingerprint = _sha256(plan_fingerprint, "plan_fingerprint")
        try:
            actor_id = _non_empty_text(actor_id, "actor_id")
        except ValueError as exc:
            raise ApprovalInvalid from exc
        with transaction(self._conn):
            job = self._jobs.get_job(self._company_id, job_id)
            if (
                job.state != JobState.SUCCEEDED.value
                or job.artifact_manifest_path is None
            ):
                raise ApprovalInvalid
            _, _, _, current_fingerprint = self._validated_current_plan_locked(job)
            if current_fingerprint != plan_fingerprint:
                raise ApprovalInvalid
            existing = self._conn.execute(
                """SELECT approval_record_id
                   FROM approval_records
                   WHERE job_id = ? AND approval_type = 'sheet_export'
                     AND plan_fingerprint = ?
                   ORDER BY approved_at, approval_record_id LIMIT 1""",
                (job_id, plan_fingerprint),
            ).fetchone()
            if existing is not None:
                return self._approvals.get_approval(
                    self._company_id, job_id, existing[0]
                )
            now = self._clock()
            approval = ApprovalRecord(
                approval_record_id=self._id_factory(),
                job_id=job_id,
                approval_type="sheet_export",
                snapshot_hash=job.snapshot_hash,
                plan_fingerprint=plan_fingerprint,
                approved_by=actor_id,
                approved_at=now,
                expires_at=None,
            )
            self._approvals.add_approval(self._company_id, approval)
            return approval

    def execute_export(self, approval_id: str) -> ExportReceipt:
        approval_id = _non_empty_text(approval_id, "approval_id")
        with transaction(self._conn):
            claimed = self._claim_export_locked(approval_id)
        if isinstance(claimed, ExportReceipt):
            return claimed
        if claimed.write_allowed:
            try:
                backend_receipt = self._backend.write(claimed.request)
            except Exception:
                recovered_receipt = self._backend.lookup(claimed.request)
                if recovered_receipt is None:
                    with transaction(self._conn):
                        self._release_attempt_locked(claimed)
                    raise
                backend_receipt = recovered_receipt
        else:
            recovered_receipt = self._backend.lookup(claimed.request)
            if recovered_receipt is None:
                raise StateConflict
            backend_receipt = recovered_receipt
        backend_receipt = self._validate_backend_receipt(
            claimed.request, backend_receipt
        )
        with transaction(self._conn):
            return self._persist_receipt_locked(
                approval_id, claimed, backend_receipt
            )

    @staticmethod
    def _validate_backend_receipt(
        request: SheetWriteRequest, backend_receipt: SheetWriteReceipt
    ) -> SheetWriteReceipt:
        if type(backend_receipt) is not SheetWriteReceipt:
            raise DataIntegrityError
        if any(
            type(value) is not str
            for value in (
                backend_receipt.export_key,
                backend_receipt.spreadsheet_id,
                backend_receipt.sheet_name,
                backend_receipt.row_selector,
                backend_receipt.destination_reference,
            )
        ):
            raise DataIntegrityError
        if (
            backend_receipt.export_key != request.export_key
            or backend_receipt.spreadsheet_id != request.spreadsheet_id
            or backend_receipt.sheet_name != request.sheet_name
            or backend_receipt.row_selector != request.row_selector
            or backend_receipt.destination_reference != request.destination_reference
        ):
            raise DataIntegrityError
        if type(backend_receipt.exported_at) is not datetime:
            raise DataIntegrityError
        try:
            if backend_receipt.exported_at.tzinfo is None:
                raise DataIntegrityError
            offset = backend_receipt.exported_at.utcoffset()
            if type(offset) is not timedelta:
                raise DataIntegrityError
            exported_at = (
                backend_receipt.exported_at.replace(tzinfo=None) - offset
            ).replace(tzinfo=UTC)
            _non_empty_text(
                backend_receipt.destination_reference,
                "destination_reference",
                maximum=2048,
            )
        except DataIntegrityError:
            raise
        except Exception as exc:
            raise DataIntegrityError from exc
        return replace(backend_receipt, exported_at=exported_at)

    def _claim_export_locked(
        self, approval_id: str
    ) -> ExportReceipt | _ClaimedWrite:
        identity = self._conn.execute(
            """SELECT approval.job_id
               FROM approval_records AS approval
               JOIN jobs ON jobs.job_id = approval.job_id
               WHERE jobs.company_id = ? AND approval.approval_record_id = ?""",
            (self._company_id, approval_id),
        ).fetchone()
        if identity is None:
            raise NotFound
        approval = self._approvals.get_approval(
            self._company_id, identity[0], approval_id
        )
        job = self._jobs.get_job(self._company_id, approval.job_id)
        if (
            approval.approval_type != "sheet_export"
            or approval.snapshot_hash != job.snapshot_hash
            or job.state != JobState.SUCCEEDED.value
            or job.artifact_manifest_path is None
        ):
            raise ApprovalInvalid
        artifact, target, cells, current_fingerprint = (
            self._validated_current_plan_locked(job)
        )
        if current_fingerprint != approval.plan_fingerprint:
            raise ApprovalInvalid
        export_key = hashlib.sha256(
            canonical_json(
                {"job_id": approval.job_id, "artifact_hash": artifact.artifact_hash}
            )
        ).hexdigest()
        request = SheetWriteRequest(
            job_id=approval.job_id,
            export_key=export_key,
            spreadsheet_id=target.spreadsheet_id,
            sheet_name=target.sheet_name,
            row_selector=target.row_selector,
            destination_reference=_destination_reference(target),
            cells=cast(dict[str, str], MappingProxyType(cells)),
        )
        existing = self._conn.execute(
            """SELECT job_id, export_key, destination_reference, exported_at
               FROM sheet_export_receipts
               WHERE company_id = ? AND approval_record_id = ?""",
            (self._company_id, approval_id),
        ).fetchone()
        if existing is not None:
            if existing[0] != request.job_id or existing[1] != request.export_key:
                raise DataIntegrityError
            try:
                persisted_at = datetime.fromisoformat(existing[3])
                if persisted_at.tzinfo is None or persisted_at.utcoffset() is None:
                    raise ValueError
                exported_at = persisted_at.astimezone(UTC)
            except (TypeError, ValueError) as exc:
                raise DataIntegrityError from exc
            backend_receipt = self._backend.lookup(request)
            if backend_receipt is None:
                raise DataIntegrityError
            backend_receipt = self._validate_backend_receipt(
                request, backend_receipt
            )
            if (
                existing[2] != backend_receipt.destination_reference
                or exported_at != backend_receipt.exported_at
            ):
                raise DataIntegrityError
            return ExportReceipt(
                job_id=request.job_id,
                export_key=request.export_key,
                destination_reference=existing[2],
                exported_at=exported_at,
            )
        claimed_at = self._clock()
        if claimed_at.tzinfo is None or claimed_at.utcoffset() is None:
            raise DataIntegrityError
        self._conn.execute(
            """INSERT OR IGNORE INTO sheet_export_claims(
                   company_id, job_id, approval_record_id, plan_fingerprint,
                   claimed_at
               ) VALUES (?, ?, ?, ?, ?)""",
            (
                self._company_id,
                approval.job_id,
                approval_id,
                approval.plan_fingerprint,
                claimed_at.astimezone(UTC).isoformat(),
            ),
        )
        durable_claim = self._conn.execute(
            """SELECT approval_record_id, plan_fingerprint
               FROM sheet_export_claims
               WHERE company_id = ? AND job_id = ?""",
            (self._company_id, approval.job_id),
        ).fetchone()
        if durable_claim != (approval_id, approval.plan_fingerprint):
            raise ApprovalInvalid
        attempt_id = f"export-attempt-{uuid.uuid4().hex}"
        inserted = self._conn.execute(
            """INSERT OR IGNORE INTO sheet_export_attempts(
                   company_id, job_id, approval_record_id, attempt_id, started_at
               ) VALUES (?, ?, ?, ?, ?)""",
            (
                self._company_id,
                approval.job_id,
                approval_id,
                attempt_id,
                claimed_at.astimezone(UTC).isoformat(),
            ),
        )
        if inserted.rowcount == 1:
            return _ClaimedWrite(
                request=request, attempt_id=attempt_id, write_allowed=True
            )
        existing_attempt = self._conn.execute(
            """SELECT approval_record_id, attempt_id
               FROM sheet_export_attempts
               WHERE company_id = ? AND job_id = ?""",
            (self._company_id, approval.job_id),
        ).fetchone()
        if existing_attempt is None:
            raise DataIntegrityError
        if existing_attempt[0] != approval_id:
            raise ApprovalInvalid
        return _ClaimedWrite(
            request=request,
            attempt_id=existing_attempt[1],
            write_allowed=False,
        )

    def _persist_receipt_locked(
        self,
        approval_id: str,
        claimed: _ClaimedWrite,
        backend_receipt: SheetWriteReceipt,
    ) -> ExportReceipt:
        request = claimed.request
        claim = self._conn.execute(
            """SELECT approval_record_id FROM sheet_export_claims
               WHERE company_id = ? AND job_id = ?""",
            (self._company_id, request.job_id),
        ).fetchone()
        if claim != (approval_id,):
            raise ApprovalInvalid
        durable = self._conn.execute(
            """SELECT job_id, export_key, destination_reference, exported_at
               FROM sheet_export_receipts
               WHERE company_id = ? AND approval_record_id = ?""",
            (self._company_id, approval_id),
        ).fetchone()
        if durable is not None:
            if durable[:3] != (
                request.job_id,
                request.export_key,
                backend_receipt.destination_reference,
            ):
                raise DataIntegrityError
            try:
                persisted_at = datetime.fromisoformat(durable[3])
                if persisted_at.tzinfo is None or persisted_at.utcoffset() is None:
                    raise ValueError
                exported_at = persisted_at.astimezone(UTC)
            except (TypeError, ValueError) as exc:
                raise DataIntegrityError from exc
            if exported_at != backend_receipt.exported_at:
                raise DataIntegrityError
            return ExportReceipt(
                job_id=request.job_id,
                export_key=request.export_key,
                destination_reference=backend_receipt.destination_reference,
                exported_at=exported_at,
            )
        attempt = self._conn.execute(
            """SELECT approval_record_id, attempt_id
               FROM sheet_export_attempts
               WHERE company_id = ? AND job_id = ?""",
            (self._company_id, request.job_id),
        ).fetchone()
        if attempt != (approval_id, claimed.attempt_id):
            raise DataIntegrityError
        self._conn.execute(
            """INSERT INTO sheet_export_receipts(
                   company_id, job_id, approval_record_id, export_key,
                   destination_reference, exported_at
               ) VALUES (?, ?, ?, ?, ?, ?)""",
            (
                self._company_id,
                request.job_id,
                approval_id,
                request.export_key,
                backend_receipt.destination_reference,
                backend_receipt.exported_at.isoformat(),
            ),
        )
        durable = self._conn.execute(
            """SELECT job_id, export_key, destination_reference, exported_at
               FROM sheet_export_receipts
               WHERE company_id = ? AND approval_record_id = ?""",
            (self._company_id, approval_id),
        ).fetchone()
        if durable is None:
            raise DataIntegrityError
        if durable[:3] != (
            request.job_id,
            request.export_key,
            backend_receipt.destination_reference,
        ):
            raise DataIntegrityError
        try:
            persisted_at = datetime.fromisoformat(durable[3])
            if persisted_at.tzinfo is None or persisted_at.utcoffset() is None:
                raise ValueError
            exported_at = persisted_at.astimezone(UTC)
        except (TypeError, ValueError) as exc:
            raise DataIntegrityError from exc
        if exported_at != backend_receipt.exported_at:
            raise DataIntegrityError
        removed = self._conn.execute(
            """DELETE FROM sheet_export_attempts
               WHERE company_id = ? AND job_id = ? AND attempt_id = ?""",
            (self._company_id, request.job_id, claimed.attempt_id),
        )
        if removed.rowcount != 1:
            raise DataIntegrityError
        return ExportReceipt(
            job_id=request.job_id,
            export_key=request.export_key,
            destination_reference=backend_receipt.destination_reference,
            exported_at=exported_at,
        )

    def _release_attempt_locked(self, claimed: _ClaimedWrite) -> None:
        removed = self._conn.execute(
            """DELETE FROM sheet_export_attempts
               WHERE company_id = ? AND job_id = ? AND attempt_id = ?""",
            (self._company_id, claimed.request.job_id, claimed.attempt_id),
        )
        if removed.rowcount != 1:
            raise DataIntegrityError
