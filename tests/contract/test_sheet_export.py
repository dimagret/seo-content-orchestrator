from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from seo_orchestrator.db.connection import connect
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.db.repositories import JobRecord
from seo_orchestrator.services.exports import (
    ExportArtifact,
    ExportService,
    MockSheetExportBackend,
    SheetExportTarget,
)

_NOW = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)


class _ArtifactSource:
    def load_export_artifact(self, job: JobRecord) -> ExportArtifact:
        assert job.job_id == "job-one"
        return ExportArtifact(
            artifact_hash="a" * 64,
            values={
                "job_id": job.job_id,
                "content": "Safe content",
                "metadata": '{"title":"Safe title"}',
            },
        )


class _ContentArtifactSource:
    def __init__(self, content: str) -> None:
        self._content = content

    def load_export_artifact(self, job: JobRecord) -> ExportArtifact:
        return ExportArtifact(
            artifact_hash="a" * 64,
            values={"job_id": job.job_id, "content": self._content},
        )


def _connection(path: Path) -> sqlite3.Connection:
    connection = connect(path)
    migrate(connection)
    connection.execute(
        "INSERT INTO companies(company_id, created_at, updated_at) VALUES (?, ?, ?)",
        ("company-one", _NOW.isoformat(), _NOW.isoformat()),
    )
    connection.execute(
        """INSERT INTO company_profile_versions(
               company_id, version, company_profile_id, profile_json,
               created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "company-one",
            1,
            "profile-one",
            "{}",
            _NOW.isoformat(),
            _NOW.isoformat(),
        ),
    )
    connection.execute(
        """INSERT INTO business_direction_versions(
               company_id, direction_id, version, company_profile_version,
               direction_json, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "company-one",
            "direction-one",
            1,
            1,
            "{}",
            _NOW.isoformat(),
            _NOW.isoformat(),
        ),
    )
    connection.execute(
        """INSERT INTO audience_segment_versions(
               company_id, direction_id, audience_segment_id, version,
               direction_version, audience_json, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "company-one",
            "direction-one",
            "audience-one",
            1,
            1,
            "{}",
            _NOW.isoformat(),
            _NOW.isoformat(),
        ),
    )
    connection.execute(
        """INSERT INTO brief_drafts(
               brief_id, company_id, company_profile_version, direction_id,
               direction_version, audience_segment_id, audience_version,
               brief_json, status, created_by, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "brief-one",
            "company-one",
            1,
            "direction-one",
            1,
            "audience-one",
            1,
            "{}",
            "validated",
            "actor-one",
            _NOW.isoformat(),
            _NOW.isoformat(),
        ),
    )
    connection.execute(
        """INSERT INTO execution_snapshots(
               snapshot_id, brief_id, company_id, company_profile_version,
               direction_id, direction_version, audience_segment_id,
               audience_version, prompt_set_version, compiled_context,
               snapshot_hash, created_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "snapshot-one",
            "brief-one",
            "company-one",
            1,
            "direction-one",
            1,
            "audience-one",
            1,
            1,
            b"{}",
            "b" * 64,
            _NOW.isoformat(),
        ),
    )
    connection.execute(
        """INSERT INTO jobs(
               job_id, brief_id, brief_fingerprint, snapshot_id, snapshot_hash,
               company_id, direction_id, audience_segment_id, state,
               created_by, created_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "job-one",
            "brief-one",
            "c" * 64,
            "snapshot-one",
            "b" * 64,
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
    return connection


def test_prepare_export_binds_destination_without_writing_backend(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "export.sqlite3")
    backend = MockSheetExportBackend(clock=lambda: _NOW)
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ArtifactSource(),
        backend=backend,
        clock=lambda: _NOW,
    )
    target = SheetExportTarget(
        spreadsheet_id="spreadsheet-one",
        sheet_name="SEO Export",
        row_selector="job_id=job-one",
        column_map={"A": "job_id", "B": "content", "C": "metadata"},
    )

    plan = service.prepare_export("job-one", target)

    assert plan.job_id == "job-one"
    assert plan.artifact_hash == "a" * 64
    assert plan.spreadsheet_id == "spreadsheet-one"
    assert plan.sheet_name == "SEO Export"
    assert plan.row_selector == "job_id=job-one"
    assert dict(plan.column_map) == {"A": "job_id", "B": "content", "C": "metadata"}
    assert len(plan.plan_fingerprint) == 64
    assert backend.write_count == 0
    stored = connection.execute(
        """SELECT artifact_hash, spreadsheet_id, sheet_name, row_selector,
                  column_map_json, plan_fingerprint
           FROM sheet_export_plans
           WHERE company_id = ? AND job_id = ?""",
        ("company-one", "job-one"),
    ).fetchone()
    assert stored == (
        "a" * 64,
        "spreadsheet-one",
        "SEO Export",
        "job_id=job-one",
        b'{"A":"job_id","B":"content","C":"metadata"}',
        plan.plan_fingerprint,
    )

    changed = service.prepare_export(
        "job-one",
        SheetExportTarget(
            spreadsheet_id="spreadsheet-one",
            sheet_name="Other tab",
            row_selector="job_id=job-one",
            column_map={"A": "job_id", "B": "content", "C": "metadata"},
        ),
    )

    assert changed.plan_fingerprint != plan.plan_fingerprint
    assert backend.write_count == 0


@pytest.mark.parametrize("dangerous_prefix", ["=", "+", "-", "@"])
def test_prepare_export_rejects_formula_cells(
    tmp_path: Path, dangerous_prefix: str
) -> None:
    connection = _connection(tmp_path / "formula.sqlite3")
    backend = MockSheetExportBackend(clock=lambda: _NOW)
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ContentArtifactSource(f"{dangerous_prefix}IMPORTDATA('x')"),
        backend=backend,
        clock=lambda: _NOW,
    )
    target = SheetExportTarget(
        spreadsheet_id="spreadsheet-one",
        sheet_name="SEO Export",
        row_selector="job_id=job-one",
        column_map={"A": "job_id", "B": "content"},
    )

    with pytest.raises(ValueError, match="formula"):
        service.prepare_export("job-one", target)

    assert backend.write_count == 0
    assert connection.execute("SELECT count(*) FROM sheet_export_plans").fetchone()[0] == 0


def test_prepare_export_allows_explicitly_escaped_formula_text(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "escaped.sqlite3")
    backend = MockSheetExportBackend(clock=lambda: _NOW)
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ContentArtifactSource("'=SUM(1,2)"),
        backend=backend,
        clock=lambda: _NOW,
    )

    plan = service.prepare_export(
        "job-one",
        SheetExportTarget(
            spreadsheet_id="spreadsheet-one",
            sheet_name="SEO Export",
            row_selector="job_id=job-one",
            column_map={"A": "content"},
        ),
    )

    assert len(plan.plan_fingerprint) == 64
    assert backend.write_count == 0


def test_prepare_export_preserves_multiline_artifact_text(tmp_path: Path) -> None:
    connection = _connection(tmp_path / "multiline.sqlite3")
    backend = MockSheetExportBackend(clock=lambda: _NOW)
    service = ExportService(
        connection,
        company_id="company-one",
        artifact_source=_ContentArtifactSource("Paragraph one.\n\nParagraph two.\n"),
        backend=backend,
        clock=lambda: _NOW,
    )

    service.prepare_export(
        "job-one",
        SheetExportTarget(
            spreadsheet_id="spreadsheet-one",
            sheet_name="SEO Export",
            row_selector="job_id=job-one",
            column_map={"A": "content"},
        ),
    )

    payload = connection.execute("SELECT payload_json FROM sheet_export_plans").fetchone()[0]
    assert payload == b'{"cells":{"A":"Paragraph one.\\n\\nParagraph two.\\n"}}'
