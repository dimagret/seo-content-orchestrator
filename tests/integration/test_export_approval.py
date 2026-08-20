from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta, tzinfo
from pathlib import Path
from threading import Event, Lock
from typing import cast

import pytest

from seo_orchestrator.canonical import JsonValue, canonical_json, sha256_fingerprint
from seo_orchestrator.db.connection import connect
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.db.repositories import JobRecord
from seo_orchestrator.errors import (
    ApprovalInvalid,
    DataIntegrityError,
    NotFound,
    StateConflict,
)
from seo_orchestrator.services.exports import (
    ExportArtifact,
    ExportReceipt,
    ExportService,
    MockSheetExportBackend,
    SheetExportTarget,
    SheetWriteReceipt,
    SheetWriteRequest,
)

_NOW = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)


class _ArtifactSource:
    def load_export_artifact(self, job: JobRecord) -> ExportArtifact:
        return ExportArtifact(
            artifact_hash="a" * 64,
            values={"job_id": job.job_id, "content": "Safe content"},
        )


class _FailOnceBackend:
    def __init__(self) -> None:
        self._failed = False
        self.delegate = MockSheetExportBackend(clock=lambda: _NOW)

    def write(self, request: SheetWriteRequest) -> SheetWriteReceipt:
        if not self._failed:
            self._failed = True
            self.delegate.write(request)
            raise RuntimeError("simulated backend interruption")
        return self.delegate.write(request)

    def lookup(self, request: SheetWriteRequest) -> SheetWriteReceipt | None:
        return self.delegate.lookup(request)


class _BlockingBackend:
    def __init__(self) -> None:
        self.delegate = MockSheetExportBackend(clock=lambda: _NOW)
        self.entered = Event()
        self.release = Event()
        self._lock = Lock()
        self.calls = 0

    def write(self, request: SheetWriteRequest) -> SheetWriteReceipt:
        with self._lock:
            self.calls += 1
        self.entered.set()
        if not self.release.wait(timeout=5):
            raise RuntimeError("blocking backend timed out")
        return self.delegate.write(request)

    def lookup(self, request: SheetWriteRequest) -> SheetWriteReceipt | None:
        return self.delegate.lookup(request)


class _FailBeforeWriteOnceBackend:
    def __init__(self) -> None:
        self._failed = False
        self.calls = 0
        self.delegate = MockSheetExportBackend(clock=lambda: _NOW)

    def write(self, request: SheetWriteRequest) -> SheetWriteReceipt:
        self.calls += 1
        if not self._failed:
            self._failed = True
            raise RuntimeError("simulated pre-write failure")
        return self.delegate.write(request)

    def lookup(self, request: SheetWriteRequest) -> SheetWriteReceipt | None:
        return self.delegate.lookup(request)


class _CrashAfterEffect(BaseException):
    pass


class _CrashAfterEffectOnceBackend:
    def __init__(self) -> None:
        self._crashed = False
        self.lookup_calls = 0
        self.delegate = MockSheetExportBackend(clock=lambda: _NOW)

    def write(self, request: SheetWriteRequest) -> SheetWriteReceipt:
        receipt = self.delegate.write(request)
        if not self._crashed:
            self._crashed = True
            raise _CrashAfterEffect
        return receipt

    def lookup(self, request: SheetWriteRequest) -> SheetWriteReceipt | None:
        self.lookup_calls += 1
        return self.delegate.lookup(request)


class _TamperedDestinationBackend:
    def __init__(self) -> None:
        self.delegate = MockSheetExportBackend(clock=lambda: _NOW)

    def write(self, request: SheetWriteRequest) -> SheetWriteReceipt:
        valid = self.delegate.write(request)
        return replace(
            valid,
            destination_reference="https://attacker.invalid/not-the-requested-sheet",
        )

    def lookup(self, request: SheetWriteRequest) -> SheetWriteReceipt | None:
        return self.delegate.lookup(request)


class _MalformedTimeBackend:
    def __init__(self) -> None:
        self.delegate = MockSheetExportBackend(clock=lambda: _NOW)

    def write(self, request: SheetWriteRequest) -> SheetWriteReceipt:
        valid = self.delegate.write(request)
        return replace(valid, exported_at=cast(datetime, "not-a-datetime"))

    def lookup(self, request: SheetWriteRequest) -> SheetWriteReceipt | None:
        return self.delegate.lookup(request)


class _BrokenTimezone(tzinfo):
    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        raise RuntimeError("hostile timezone")

    def dst(self, dt: datetime | None) -> timedelta | None:
        return timedelta(0)

    def tzname(self, dt: datetime | None) -> str | None:
        return "broken"


class _BrokenTimezoneBackend:
    def __init__(self) -> None:
        self.delegate = MockSheetExportBackend(clock=lambda: _NOW)

    def write(self, request: SheetWriteRequest) -> SheetWriteReceipt:
        valid = self.delegate.write(request)
        broken = datetime(2026, 8, 20, 12, 0, tzinfo=_BrokenTimezone())
        return replace(valid, exported_at=broken)

    def lookup(self, request: SheetWriteRequest) -> SheetWriteReceipt | None:
        return self.delegate.lookup(request)


class _StatefulTimezone(tzinfo):
    def __init__(self) -> None:
        self.calls = 0

    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        self.calls += 1
        if self.calls > 1:
            raise RuntimeError("second-call hostile timezone")
        return timedelta(0)

    def dst(self, dt: datetime | None) -> timedelta | None:
        return timedelta(0)

    def tzname(self, dt: datetime | None) -> str | None:
        return "stateful"


class _StatefulTimezoneBackend:
    def __init__(self) -> None:
        self.delegate = MockSheetExportBackend(clock=lambda: _NOW)
        self.timezone = _StatefulTimezone()

    def write(self, request: SheetWriteRequest) -> SheetWriteReceipt:
        valid = self.delegate.write(request)
        stateful = datetime(2026, 8, 20, 12, 0, tzinfo=self.timezone)
        return replace(valid, exported_at=stateful)

    def lookup(self, request: SheetWriteRequest) -> SheetWriteReceipt | None:
        return self.delegate.lookup(request)


class _NoneOffsetTimezone(tzinfo):
    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        return None

    def dst(self, dt: datetime | None) -> timedelta | None:
        return None

    def tzname(self, dt: datetime | None) -> str | None:
        return "none-offset"


class _NoneOffsetTimezoneBackend:
    def __init__(self) -> None:
        self.delegate = MockSheetExportBackend(clock=lambda: _NOW)

    def write(self, request: SheetWriteRequest) -> SheetWriteReceipt:
        valid = self.delegate.write(request)
        malformed = datetime(2026, 8, 20, 12, 0, tzinfo=_NoneOffsetTimezone())
        return replace(valid, exported_at=malformed)

    def lookup(self, request: SheetWriteRequest) -> SheetWriteReceipt | None:
        return self.delegate.lookup(request)


def _connection(path: Path) -> sqlite3.Connection:
    connection = connect(path)
    migrate(connection)
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute(
        """INSERT INTO jobs(
               job_id, brief_id, brief_fingerprint, snapshot_id, snapshot_hash,
               company_id, direction_id, audience_segment_id, state,
               created_by, created_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "job-one",
            "brief-one",
            "b" * 64,
            "snapshot-one",
            "c" * 64,
            "company-one",
            "direction-one",
            "audience-one",
            "SUCCEEDED",
            "actor-one",
            _NOW.isoformat(),
        ),
    )
    connection.execute(
        "UPDATE jobs SET artifact_manifest_path = ? WHERE job_id = ?",
        ("/artifacts/company-one/job-one/manifest.json", "job-one"),
    )
    connection.commit()
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def _insert_plan(
    connection: sqlite3.Connection,
    tab: str,
    *,
    fingerprint_override: str | None = None,
    payload_override: bytes | None = None,
) -> str:
    artifact_hash = "a" * 64
    column_map = {"A": "job_id", "B": "content"}
    fingerprint = fingerprint_override or sha256_fingerprint(
        cast(JsonValue, {
            "job_id": "job-one",
            "artifact_hash": artifact_hash,
            "spreadsheet_id": "spreadsheet-one",
            "sheet_name": tab,
            "row_selector": "job_id=job-one",
            "column_map": column_map,
        })
    )
    connection.execute(
        """INSERT INTO sheet_export_plans(
               company_id, job_id, artifact_hash, spreadsheet_id, sheet_name,
               row_selector, column_map_json, payload_json, plan_fingerprint,
               prepared_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "company-one",
            "job-one",
            artifact_hash,
            "spreadsheet-one",
            tab,
            "job_id=job-one",
            canonical_json(cast(JsonValue, column_map)),
            payload_override
            or canonical_json({"cells": {"A": "job-one", "B": "Safe content"}}),
            fingerprint,
            _NOW.isoformat(),
        ),
    )
    connection.commit()
    return fingerprint


def test_destination_change_invalidates_existing_export_approval(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "approval.sqlite3")
    backend = MockSheetExportBackend(clock=lambda: _NOW)
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
        id_factory=lambda: "approval-export-one",
    )
    first_fingerprint = _insert_plan(connection, "SEO Export")

    approval = service.approve_export("job-one", first_fingerprint, "reviewer-one")
    duplicate = service.approve_export("job-one", first_fingerprint, "reviewer-one")

    assert duplicate == approval
    assert approval.approval_type == "sheet_export"
    assert approval.plan_fingerprint == first_fingerprint
    assert approval.snapshot_hash == "c" * 64
    assert backend.write_count == 0

    _insert_plan(connection, "Changed destination")

    with pytest.raises(ApprovalInvalid):
        service.execute_export(approval.approval_record_id)

    assert backend.write_count == 0


def test_approve_rejects_persisted_plan_with_forged_fingerprint(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "forged-plan.sqlite3")
    backend = MockSheetExportBackend(clock=lambda: _NOW)
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
    )
    forged_fingerprint = "d" * 64
    _insert_plan(
        connection,
        "Attacker destination",
        fingerprint_override=forged_fingerprint,
    )

    with pytest.raises(DataIntegrityError):
        service.approve_export("job-one", forged_fingerprint, "reviewer-one")

    assert backend.write_count == 0


def test_approve_rejects_persisted_payload_not_derived_from_artifact(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "forged-payload.sqlite3")
    backend = MockSheetExportBackend(clock=lambda: _NOW)
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
    )
    fingerprint = _insert_plan(
        connection,
        "SEO Export",
        payload_override=canonical_json(
            cast(JsonValue, {"cells": {f"C{index}": "x" for index in range(65)}})
        ),
    )

    with pytest.raises(DataIntegrityError):
        service.approve_export("job-one", fingerprint, "reviewer-one")

    assert backend.write_count == 0


def test_prepare_rejects_target_whose_canonical_destination_is_too_long(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "long-destination.sqlite3")
    backend = MockSheetExportBackend(clock=lambda: _NOW)
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
    )
    target = SheetExportTarget(
        spreadsheet_id="💣" * 256,
        sheet_name="💣" * 256,
        row_selector="💣" * 512,
        column_map={"A": "job_id"},
    )

    with pytest.raises(ValueError, match="destination_reference"):
        service.prepare_export("job-one", target)

    assert connection.execute("SELECT count(*) FROM sheet_export_plans").fetchone()[0] == 0
    assert backend.write_count == 0


def test_execute_export_is_idempotent_across_service_retries(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "execute.sqlite3")
    backend = MockSheetExportBackend(clock=lambda: _NOW)
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
        id_factory=lambda: "approval-export-one",
    )
    fingerprint = _insert_plan(connection, "SEO Export")
    approval = service.approve_export("job-one", fingerprint, "reviewer-one")

    first = service.execute_export(approval.approval_record_id)
    restarted_service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
    )
    duplicate = restarted_service.execute_export(approval.approval_record_id)

    assert duplicate == first
    assert first.job_id == "job-one"
    assert first.export_key == sha256_fingerprint(
        {"job_id": "job-one", "artifact_hash": "a" * 64}
    )
    assert first.exported_at == _NOW
    assert first.destination_reference == (
        "sheet://spreadsheet-one/SEO%20Export?row=job_id%3Djob-one"
    )
    assert backend.write_count == 1
    assert backend.row_count == 1
    assert connection.execute("SELECT count(*) FROM sheet_export_receipts").fetchone()[0] == 1

    with pytest.raises(StateConflict):
        service.prepare_export(
            "job-one",
            SheetExportTarget(
                spreadsheet_id="spreadsheet-one",
                sheet_name="Changed destination",
                row_selector="job_id=job-one",
                column_map={"A": "job_id", "B": "content"},
            ),
        )


def test_concurrent_duplicate_does_not_cross_backend_write_boundary(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "concurrent.sqlite3"
    connection = _connection(database_path)
    backend = _BlockingBackend()
    setup_service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
        id_factory=lambda: "approval-export-one",
    )
    fingerprint = _insert_plan(connection, "SEO Export")
    approval = setup_service.approve_export(
        "job-one", fingerprint, "reviewer-one"
    )

    def execute_in_worker() -> ExportReceipt:
        worker_connection = connect(database_path)
        try:
            worker = ExportService(
                worker_connection,
                company_id="company-one",
                artifact_source=_ArtifactSource(),
                backend=backend,
                clock=lambda: _NOW,
            )
            return worker.execute_export(approval.approval_record_id)
        finally:
            worker_connection.close()

    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(execute_in_worker)
        assert backend.entered.wait(timeout=5)
        competing_connection = connect(database_path)
        try:
            competing = ExportService(
                competing_connection,
                company_id="company-one",
                artifact_source=_ArtifactSource(),
                backend=backend,
                clock=lambda: _NOW,
            )
            with pytest.raises(StateConflict):
                competing.execute_export(approval.approval_record_id)
        finally:
            competing_connection.close()
            backend.release.set()
        receipt = first.result(timeout=5)

    assert receipt.job_id == "job-one"
    assert backend.calls == 1
    assert backend.delegate.write_count == 1


def test_export_reconciles_partial_backend_failure_without_duplicate_write(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "partial.sqlite3")
    backend = _FailOnceBackend()
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
        id_factory=lambda: "approval-export-one",
    )
    fingerprint = _insert_plan(connection, "SEO Export")
    approval = service.approve_export("job-one", fingerprint, "reviewer-one")

    first = service.execute_export(approval.approval_record_id)

    assert connection.execute("SELECT count(*) FROM sheet_export_claims").fetchone()[0] == 1
    assert connection.execute("SELECT count(*) FROM sheet_export_receipts").fetchone()[0] == 1
    assert connection.execute("SELECT count(*) FROM sheet_export_attempts").fetchone()[0] == 0
    assert backend.delegate.write_count == 1

    receipt = service.execute_export(approval.approval_record_id)

    assert receipt == first
    assert receipt.job_id == "job-one"
    assert backend.delegate.write_count == 1
    assert connection.execute("SELECT count(*) FROM sheet_export_receipts").fetchone()[0] == 1


def test_export_releases_attempt_after_backend_proves_no_write(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "pre-write-failure.sqlite3")
    backend = _FailBeforeWriteOnceBackend()
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
        id_factory=lambda: "approval-export-one",
    )
    fingerprint = _insert_plan(connection, "SEO Export")
    approval = service.approve_export("job-one", fingerprint, "reviewer-one")

    with pytest.raises(RuntimeError, match="pre-write"):
        service.execute_export(approval.approval_record_id)

    assert connection.execute("SELECT count(*) FROM sheet_export_attempts").fetchone()[0] == 0
    assert connection.execute("SELECT count(*) FROM sheet_export_receipts").fetchone()[0] == 0

    receipt = service.execute_export(approval.approval_record_id)

    assert receipt.job_id == "job-one"
    assert backend.calls == 2
    assert backend.delegate.write_count == 1


def test_restart_reconciles_post_effect_crash_without_second_write(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "post-effect-crash.sqlite3")
    backend = _CrashAfterEffectOnceBackend()
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
        id_factory=lambda: "approval-export-one",
    )
    fingerprint = _insert_plan(connection, "SEO Export")
    approval = service.approve_export("job-one", fingerprint, "reviewer-one")

    with pytest.raises(_CrashAfterEffect):
        service.execute_export(approval.approval_record_id)

    assert connection.execute("SELECT count(*) FROM sheet_export_attempts").fetchone()[0] == 1
    assert connection.execute("SELECT count(*) FROM sheet_export_receipts").fetchone()[0] == 0
    assert backend.delegate.write_count == 1

    restarted = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
    )
    receipt = restarted.execute_export(approval.approval_record_id)

    assert receipt.job_id == "job-one"
    assert receipt.destination_reference == (
        "sheet://spreadsheet-one/SEO%20Export?row=job_id%3Djob-one"
    )
    assert backend.lookup_calls == 1
    assert backend.delegate.write_count == 1
    assert connection.execute("SELECT count(*) FROM sheet_export_attempts").fetchone()[0] == 0
    assert connection.execute("SELECT count(*) FROM sheet_export_receipts").fetchone()[0] == 1


def test_first_call_rejects_tampered_backend_destination(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "tampered-destination.sqlite3")
    backend = _TamperedDestinationBackend()
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
        id_factory=lambda: "approval-export-one",
    )
    fingerprint = _insert_plan(connection, "SEO Export")
    approval = service.approve_export("job-one", fingerprint, "reviewer-one")

    with pytest.raises(DataIntegrityError):
        service.execute_export(approval.approval_record_id)

    assert connection.execute("SELECT count(*) FROM sheet_export_attempts").fetchone()[0] == 1
    assert connection.execute("SELECT count(*) FROM sheet_export_receipts").fetchone()[0] == 0
    assert backend.delegate.write_count == 1

    receipt = service.execute_export(approval.approval_record_id)

    assert receipt.destination_reference == (
        "sheet://spreadsheet-one/SEO%20Export?row=job_id%3Djob-one"
    )
    assert backend.delegate.write_count == 1


def test_malformed_backend_time_is_reported_as_data_integrity_error(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "malformed-time.sqlite3")
    backend = _MalformedTimeBackend()
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
        id_factory=lambda: "approval-export-one",
    )
    fingerprint = _insert_plan(connection, "SEO Export")
    approval = service.approve_export("job-one", fingerprint, "reviewer-one")

    with pytest.raises(DataIntegrityError):
        service.execute_export(approval.approval_record_id)

    assert connection.execute("SELECT count(*) FROM sheet_export_attempts").fetchone()[0] == 1
    assert connection.execute("SELECT count(*) FROM sheet_export_receipts").fetchone()[0] == 0
    assert backend.delegate.write_count == 1

    receipt = service.execute_export(approval.approval_record_id)

    assert receipt.exported_at == _NOW
    assert backend.delegate.write_count == 1


def test_broken_timezone_is_reported_as_data_integrity_error(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "broken-timezone.sqlite3")
    backend = _BrokenTimezoneBackend()
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
        id_factory=lambda: "approval-export-one",
    )
    fingerprint = _insert_plan(connection, "SEO Export")
    approval = service.approve_export("job-one", fingerprint, "reviewer-one")

    with pytest.raises(DataIntegrityError):
        service.execute_export(approval.approval_record_id)

    assert connection.execute("SELECT count(*) FROM sheet_export_attempts").fetchone()[0] == 1
    assert connection.execute("SELECT count(*) FROM sheet_export_receipts").fetchone()[0] == 0
    assert backend.delegate.write_count == 1

    receipt = service.execute_export(approval.approval_record_id)

    assert receipt.exported_at == _NOW
    assert backend.delegate.write_count == 1


def test_backend_time_is_normalized_once_before_persistence(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "stateful-timezone.sqlite3")
    backend = _StatefulTimezoneBackend()
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
        id_factory=lambda: "approval-export-one",
    )
    fingerprint = _insert_plan(connection, "SEO Export")
    approval = service.approve_export("job-one", fingerprint, "reviewer-one")

    receipt = service.execute_export(approval.approval_record_id)

    assert receipt.exported_at == _NOW
    assert receipt.exported_at.tzinfo is UTC
    assert backend.timezone.calls == 1
    assert backend.delegate.write_count == 1
    assert connection.execute("SELECT count(*) FROM sheet_export_attempts").fetchone()[0] == 0
    assert connection.execute("SELECT count(*) FROM sheet_export_receipts").fetchone()[0] == 1


def test_timezone_with_none_offset_is_rejected(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "none-offset-timezone.sqlite3")
    backend = _NoneOffsetTimezoneBackend()
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
        id_factory=lambda: "approval-export-one",
    )
    fingerprint = _insert_plan(connection, "SEO Export")
    approval = service.approve_export("job-one", fingerprint, "reviewer-one")

    with pytest.raises(DataIntegrityError):
        service.execute_export(approval.approval_record_id)

    assert connection.execute("SELECT count(*) FROM sheet_export_attempts").fetchone()[0] == 1
    assert connection.execute("SELECT count(*) FROM sheet_export_receipts").fetchone()[0] == 0
    assert backend.delegate.write_count == 1

    receipt = service.execute_export(approval.approval_record_id)

    assert receipt.exported_at == _NOW
    assert backend.delegate.write_count == 1


def test_execute_rejects_persisted_receipt_without_backend_provenance(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "forged-receipt.sqlite3")
    backend = MockSheetExportBackend(clock=lambda: _NOW)
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
        id_factory=lambda: "approval-export-one",
    )
    fingerprint = _insert_plan(connection, "SEO Export")
    approval = service.approve_export("job-one", fingerprint, "reviewer-one")
    export_key = sha256_fingerprint(
        {"job_id": "job-one", "artifact_hash": "a" * 64}
    )
    connection.execute(
        """INSERT INTO sheet_export_claims(
               company_id, job_id, approval_record_id, plan_fingerprint, claimed_at
           ) VALUES (?, ?, ?, ?, ?)""",
        (
            "company-one",
            "job-one",
            approval.approval_record_id,
            fingerprint,
            _NOW.isoformat(),
        ),
    )
    connection.execute(
        """INSERT INTO sheet_export_receipts(
               company_id, job_id, approval_record_id, export_key,
               destination_reference, exported_at
           ) VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "company-one",
            "job-one",
            approval.approval_record_id,
            export_key,
            "mock://attacker-controlled",
            _NOW.isoformat(),
        ),
    )
    connection.commit()

    with pytest.raises(DataIntegrityError):
        service.execute_export(approval.approval_record_id)

    assert backend.write_count == 0


def test_export_approval_and_execution_are_company_scoped(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "isolation.sqlite3")
    owner_backend = MockSheetExportBackend(clock=lambda: _NOW)
    owner = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=owner_backend,
        clock=lambda: _NOW,
        id_factory=lambda: "approval-export-one",
    )
    fingerprint = _insert_plan(connection, "SEO Export")
    approval = owner.approve_export("job-one", fingerprint, "reviewer-one")
    foreign_backend = MockSheetExportBackend(clock=lambda: _NOW)
    foreign = ExportService(
        connection,
        company_id="company-two",
        artifact_source=_ArtifactSource(),
        backend=foreign_backend,
        clock=lambda: _NOW,
    )

    with pytest.raises(NotFound):
        foreign.approve_export("job-one", fingerprint, "reviewer-two")
    with pytest.raises(NotFound):
        foreign.execute_export(approval.approval_record_id)

    assert owner_backend.write_count == 0
    assert foreign_backend.write_count == 0


def test_claim_schema_rejects_approval_belonging_to_another_job(
    tmp_path: Path,
) -> None:
    connection = _connection(tmp_path / "claim-binding.sqlite3")
    fingerprint = _insert_plan(connection, "SEO Export")
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute(
        """INSERT INTO jobs(
               job_id, brief_id, brief_fingerprint, snapshot_id, snapshot_hash,
               company_id, direction_id, audience_segment_id, state,
               created_by, created_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "job-two",
            "brief-two",
            "e" * 64,
            "snapshot-two",
            "f" * 64,
            "company-one",
            "direction-one",
            "audience-one",
            "SUCCEEDED",
            "actor-one",
            _NOW.isoformat(),
        ),
    )
    connection.commit()
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute(
        """INSERT INTO approval_records(
               approval_record_id, job_id, approval_type, snapshot_hash,
               plan_fingerprint, approved_by, approved_at, expires_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)""",
        (
            "approval-for-job-two",
            "job-two",
            "sheet_export",
            "f" * 64,
            fingerprint,
            "reviewer-one",
            _NOW.isoformat(),
        ),
    )
    connection.commit()

    with pytest.raises(
        sqlite3.IntegrityError, match="sheet export claim approval mismatch"
    ):
        connection.execute(
            """INSERT INTO sheet_export_claims(
                   company_id, job_id, approval_record_id,
                   plan_fingerprint, claimed_at
               ) VALUES (?, ?, ?, ?, ?)""",
            (
                "company-one",
                "job-one",
                "approval-for-job-two",
                fingerprint,
                _NOW.isoformat(),
            ),
        )


def test_export_durable_records_are_immutable(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "immutable.sqlite3")
    backend = MockSheetExportBackend(clock=lambda: _NOW)
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
        id_factory=lambda: "approval-export-one",
    )
    fingerprint = _insert_plan(connection, "SEO Export")
    approval = service.approve_export("job-one", fingerprint, "reviewer-one")
    service.execute_export(approval.approval_record_id)

    for table in (
        "sheet_export_plans",
        "sheet_export_claims",
        "sheet_export_receipts",
    ):
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(f"UPDATE {table} SET job_id = job_id")
        connection.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(f"DELETE FROM {table}")
        connection.rollback()
