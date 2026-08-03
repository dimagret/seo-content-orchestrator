"""Company-scoped SQLite repository boundaries.

Repositories deliberately leave transaction ownership to their caller.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from seo_orchestrator.canonical import canonical_json
from seo_orchestrator.domain import (
    AudienceSegment,
    BusinessDirection,
    CompanyProfile,
    ExecutionSnapshot,
    SeoBrief,
)
from seo_orchestrator.errors import NotFound


@dataclass(frozen=True, slots=True)
class JobRecord:
    job_id: str
    brief_id: str
    brief_fingerprint: str
    snapshot_id: str
    snapshot_hash: str
    company_id: str
    direction_id: str
    audience_segment_id: str
    state: str
    current_stage: str | None
    approved_plan_fingerprint: str | None
    approval_record_id: str | None
    attempt: int
    created_by: str
    created_at: str
    started_at: str | None
    finished_at: str | None
    error_code: str | None
    error_summary: str | None
    artifact_manifest_path: str | None


@dataclass(frozen=True, slots=True)
class ApprovalRecord:
    approval_record_id: str
    job_id: str
    approval_type: str
    snapshot_hash: str
    plan_fingerprint: str
    approved_by: str
    approved_at: str
    expires_at: str | None


class CompanyRepository:
    """Persist and retrieve versioned company-owned configuration."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def add_company(
        self, company_id: str, created_at: datetime, updated_at: datetime
    ) -> None:
        self._conn.execute(
            """INSERT INTO companies(company_id, created_at, updated_at)
               VALUES (?, ?, ?)""",
            (company_id, created_at.isoformat(), updated_at.isoformat()),
        )

    def add_profile(self, profile: CompanyProfile) -> None:
        self._conn.execute(
            """INSERT INTO company_profile_versions(
                   company_id, version, company_profile_id, profile_json,
                   created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?)""",
            (
                profile.company_id,
                profile.company_profile_version,
                profile.company_profile_id,
                profile.model_dump_json(),
                profile.created_at.isoformat(),
                profile.updated_at.isoformat(),
            ),
        )

    def get_profile(self, company_id: str, version: int) -> CompanyProfile:
        row = self._conn.execute(
            """SELECT profile_json
               FROM company_profile_versions
               WHERE company_id = ? AND version = ?""",
            (company_id, version),
        ).fetchone()
        if row is None:
            raise NotFound
        values: dict[str, Any] = json.loads(row[0])
        values["created_at"] = datetime.fromisoformat(values["created_at"])
        values["updated_at"] = datetime.fromisoformat(values["updated_at"])
        return CompanyProfile.model_validate(values)

    def add_direction(self, direction: BusinessDirection) -> None:
        self._conn.execute(
            """INSERT INTO business_direction_versions(
                   company_id, direction_id, version, company_profile_version,
                   direction_json, created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                direction.company_id,
                direction.direction_id,
                direction.direction_version,
                direction.company_profile_version,
                direction.model_dump_json(),
                direction.created_at.isoformat(),
                direction.updated_at.isoformat(),
            ),
        )

    def get_direction(
        self, company_id: str, direction_id: str, version: int
    ) -> BusinessDirection:
        row = self._conn.execute(
            """SELECT direction_json
               FROM business_direction_versions
               WHERE company_id = ? AND direction_id = ? AND version = ?""",
            (company_id, direction_id, version),
        ).fetchone()
        if row is None:
            raise NotFound
        values: dict[str, Any] = json.loads(row[0])
        values["created_at"] = datetime.fromisoformat(values["created_at"])
        values["updated_at"] = datetime.fromisoformat(values["updated_at"])
        return BusinessDirection.model_validate(values)

    def add_audience(self, audience: AudienceSegment) -> None:
        self._conn.execute(
            """INSERT INTO audience_segment_versions(
                   company_id, direction_id, audience_segment_id, version,
                   direction_version, audience_json, created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                audience.company_id,
                audience.direction_id,
                audience.audience_segment_id,
                audience.audience_version,
                audience.direction_version,
                audience.model_dump_json(),
                audience.created_at.isoformat(),
                audience.updated_at.isoformat(),
            ),
        )

    def get_audience(
        self,
        company_id: str,
        direction_id: str,
        audience_segment_id: str,
        version: int,
    ) -> AudienceSegment:
        row = self._conn.execute(
            """SELECT audience_json
               FROM audience_segment_versions
               WHERE company_id = ? AND direction_id = ?
                 AND audience_segment_id = ? AND version = ?""",
            (company_id, direction_id, audience_segment_id, version),
        ).fetchone()
        if row is None:
            raise NotFound
        values: dict[str, Any] = json.loads(row[0])
        values["created_at"] = datetime.fromisoformat(values["created_at"])
        values["updated_at"] = datetime.fromisoformat(values["updated_at"])
        return AudienceSegment.model_validate(values)


class BriefRepository:
    """Persist complete brief records through company-explicit APIs."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def add_brief(self, brief: SeoBrief) -> None:
        self._conn.execute(
            """INSERT INTO brief_drafts(
                   brief_id, company_id, company_profile_version, direction_id,
                   direction_version, audience_segment_id, audience_version,
                   brief_json, created_by, created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                brief.brief_id,
                brief.company_id,
                brief.company_profile_version,
                brief.direction_id,
                brief.direction_version,
                brief.audience_segment_id,
                brief.audience_version,
                brief.model_dump_json(),
                brief.created_by,
                brief.created_at.isoformat(),
                brief.updated_at.isoformat(),
            ),
        )

    def get_brief(self, company_id: str, brief_id: str) -> SeoBrief:
        row = self._conn.execute(
            """SELECT brief_json FROM brief_drafts
               WHERE company_id = ? AND brief_id = ?""",
            (company_id, brief_id),
        ).fetchone()
        if row is None:
            raise NotFound
        values: dict[str, Any] = json.loads(row[0])
        values["created_at"] = datetime.fromisoformat(values["created_at"])
        values["updated_at"] = datetime.fromisoformat(values["updated_at"])
        return SeoBrief.model_validate(values)


class SnapshotRepository:
    """Persist immutable execution snapshots through company-explicit APIs."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def add_snapshot(self, snapshot: ExecutionSnapshot) -> None:
        context = canonical_json(snapshot.thawed_compiled_context())
        cursor = self._conn.execute(
            """INSERT INTO execution_snapshots(
                   snapshot_id, brief_id, company_id, company_profile_version,
                   direction_id, direction_version, audience_segment_id, audience_version,
                   prompt_set_version, compiled_context, snapshot_hash, created_at
               )
               SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
               FROM brief_drafts
               WHERE company_id = ? AND brief_id = ?""",
            (
                snapshot.snapshot_id,
                snapshot.brief_id,
                snapshot.company_id,
                snapshot.company_profile_version,
                snapshot.direction_id,
                snapshot.direction_version,
                snapshot.audience_segment_id,
                snapshot.audience_version,
                snapshot.prompt_set_version,
                context,
                snapshot.snapshot_hash,
                snapshot.created_at.isoformat(),
                snapshot.company_id,
                snapshot.brief_id,
            ),
        )
        if cursor.rowcount != 1:
            raise NotFound

    def get_snapshot(self, company_id: str, snapshot_id: str) -> ExecutionSnapshot:
        row = self._conn.execute(
            """SELECT snapshot_id, brief_id, company_id, company_profile_version,
                      direction_id, direction_version, audience_segment_id, audience_version,
                      prompt_set_version, compiled_context, snapshot_hash, created_at
               FROM execution_snapshots
               WHERE company_id = ? AND snapshot_id = ?""",
            (company_id, snapshot_id),
        ).fetchone()
        if row is None:
            raise NotFound
        context_bytes = bytes(row[9])
        return ExecutionSnapshot(
            snapshot_id=row[0],
            brief_id=row[1],
            company_id=row[2],
            company_profile_version=row[3],
            direction_id=row[4],
            direction_version=row[5],
            audience_segment_id=row[6],
            audience_version=row[7],
            prompt_set_version=row[8],
            compiled_context=json.loads(context_bytes),
            snapshot_hash=row[10],
            created_at=datetime.fromisoformat(row[11]),
        )


class JobRepository:
    """Retrieve jobs only through explicit company scope."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def add_job(self, job: JobRecord) -> None:
        cursor = self._conn.execute(
            """INSERT INTO jobs(
                   job_id, brief_id, brief_fingerprint, snapshot_id, snapshot_hash,
                   company_id, direction_id, audience_segment_id, state, current_stage,
                   approved_plan_fingerprint, approval_record_id, attempt, created_by,
                   created_at, started_at, finished_at, error_code, error_summary,
                   artifact_manifest_path
               )
               SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
               FROM brief_drafts AS brief
               JOIN execution_snapshots AS snapshot
                 ON snapshot.company_id = brief.company_id
                AND snapshot.brief_id = brief.brief_id
               WHERE brief.company_id = ? AND brief.brief_id = ?
                 AND snapshot.snapshot_id = ?""",
            (
                job.job_id,
                job.brief_id,
                job.brief_fingerprint,
                job.snapshot_id,
                job.snapshot_hash,
                job.company_id,
                job.direction_id,
                job.audience_segment_id,
                job.state,
                job.current_stage,
                job.approved_plan_fingerprint,
                job.approval_record_id,
                job.attempt,
                job.created_by,
                job.created_at,
                job.started_at,
                job.finished_at,
                job.error_code,
                job.error_summary,
                job.artifact_manifest_path,
                job.company_id,
                job.brief_id,
                job.snapshot_id,
            ),
        )
        if cursor.rowcount != 1:
            raise NotFound

    def get_job(self, company_id: str, job_id: str) -> JobRecord:
        row = self._conn.execute(
            """SELECT job_id, brief_id, brief_fingerprint, snapshot_id, snapshot_hash,
                      company_id, direction_id, audience_segment_id, state, current_stage,
                      approved_plan_fingerprint, approval_record_id, attempt, created_by,
                      created_at, started_at, finished_at, error_code, error_summary,
                      artifact_manifest_path
               FROM jobs
               WHERE company_id = ? AND job_id = ?""",
            (company_id, job_id),
        ).fetchone()
        if row is None:
            raise NotFound
        return JobRecord(*row)


class ApprovalRepository:
    """Persist approvals only through the owning job's company scope."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def add_approval(self, company_id: str, approval: ApprovalRecord) -> None:
        cursor = self._conn.execute(
            """INSERT INTO approval_records(
                   approval_record_id, job_id, approval_type, snapshot_hash,
                   plan_fingerprint, approved_by, approved_at, expires_at
               )
               SELECT ?, ?, ?, ?, ?, ?, ?, ?
               FROM jobs
               WHERE company_id = ? AND job_id = ?""",
            (
                approval.approval_record_id,
                approval.job_id,
                approval.approval_type,
                approval.snapshot_hash,
                approval.plan_fingerprint,
                approval.approved_by,
                approval.approved_at,
                approval.expires_at,
                company_id,
                approval.job_id,
            ),
        )
        if cursor.rowcount != 1:
            raise NotFound

    def get_approval(
        self, company_id: str, job_id: str, approval_record_id: str
    ) -> ApprovalRecord:
        row = self._conn.execute(
            """SELECT approval.approval_record_id, approval.job_id,
                      approval.approval_type, approval.snapshot_hash,
                      approval.plan_fingerprint, approval.approved_by,
                      approval.approved_at, approval.expires_at
               FROM approval_records AS approval
               JOIN jobs ON jobs.job_id = approval.job_id
               WHERE jobs.company_id = ? AND jobs.job_id = ?
                 AND approval.approval_record_id = ?""",
            (company_id, job_id, approval_record_id),
        ).fetchone()
        if row is None:
            raise NotFound
        return ApprovalRecord(*row)
