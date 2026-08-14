"""Company-scoped SQLite repository boundaries.

Repositories deliberately leave transaction ownership to their caller.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from seo_orchestrator.canonical import canonical_json, sha256_fingerprint
from seo_orchestrator.domain import (
    ApprovalRecord,
    AudienceSegment,
    BusinessDirection,
    CompanyProfile,
    ExecutionSnapshot,
    SeoBrief,
)
from seo_orchestrator.errors import CompanyArchived, DataIntegrityError, NotFound

_SNAPSHOT_CONTEXT_KEYS = frozenset(
    {
        "schema_version",
        "company",
        "direction",
        "audience",
        "brief",
        "prompt_set_version",
    }
)


def _storage_timestamp(value: object) -> str | None:
    if value is None:
        return None
    if type(value) is not datetime:
        raise TypeError("approval timestamp must be a datetime or None")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("approval timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _domain_timestamp(value: object, *, optional: bool) -> datetime | None:
    if value is None and optional:
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
    plan_json: bytes | None = None
    plan_fingerprint: str | None = None
    superseded_by_job_id: str | None = None


@dataclass(frozen=True, slots=True)
class RunnerCandidate:
    """One globally discovered job whose identity remains explicitly company-scoped."""

    company_id: str
    job_id: str
    attempt: int
    state: str


@dataclass(frozen=True, slots=True)
class ExecutionRunRecord:
    """Durable executor identity and lease state for one job attempt."""

    company_id: str
    job_id: str
    attempt: int
    idempotency_key: str
    external_run_id: str | None
    executor_name: str | None
    external_accepted_at: str | None
    acceptance_observed_at: str | None
    external_status: str | None
    current_stage: str | None
    next_action_at: str | None
    submission_attempted_at: str | None
    completion_observed_at: str | None
    error_code: str | None
    error_summary: str | None
    result_json: bytes | None
    result_hash: str | None
    heartbeat_at: str
    lease_token: str | None
    lease_expires_at: str | None
    reconciliation_count: int
    retry_stage_id: str
    transient_failure_count: int
    created_at: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class BriefDraftRecord:
    brief_id: str
    company_id: str
    company_profile_version: int
    direction_id: str | None
    direction_version: int | None
    audience_segment_id: str | None
    audience_version: int | None
    brief_json: bytes
    status: str
    created_by: str
    created_at: str
    updated_at: str


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

    def require_active(self, company_id: str) -> None:
        row = self._conn.execute(
            "SELECT status FROM companies WHERE company_id = ?", (company_id,)
        ).fetchone()
        if row is None:
            raise NotFound
        if row[0] == "archived":
            raise CompanyArchived

    def archive_company(self, company_id: str, archived_at: datetime) -> None:
        self.require_active(company_id)
        self._conn.execute(
            """UPDATE companies
               SET status = 'archived', updated_at = ?, archived_at = ?
               WHERE company_id = ?""",
            (archived_at.isoformat(), archived_at.isoformat(), company_id),
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

    def get_current_profile(self, company_id: str) -> CompanyProfile:
        row = self._conn.execute(
            """SELECT profile_json
               FROM company_profile_versions
               WHERE company_id = ?
               ORDER BY version DESC
               LIMIT 1""",
            (company_id,),
        ).fetchone()
        if row is None:
            raise NotFound
        values: dict[str, Any] = json.loads(row[0])
        values["created_at"] = datetime.fromisoformat(values["created_at"])
        values["updated_at"] = datetime.fromisoformat(values["updated_at"])
        return CompanyProfile.model_validate(values)

    def list_current_profiles(self) -> tuple[CompanyProfile, ...]:
        """Return current profiles for active companies in deterministic company order."""
        rows = self._conn.execute(
            """SELECT profile.profile_json
               FROM companies AS company
               JOIN company_profile_versions AS profile
                 ON profile.company_id = company.company_id
               WHERE company.status = 'active'
                 AND profile.version = (
                     SELECT MAX(current_profile.version)
                     FROM company_profile_versions AS current_profile
                     WHERE current_profile.company_id = company.company_id
                 )
               ORDER BY company.company_id ASC"""
        ).fetchall()
        result: list[CompanyProfile] = []
        for row in rows:
            values: dict[str, Any] = json.loads(row[0])
            values["created_at"] = datetime.fromisoformat(values["created_at"])
            values["updated_at"] = datetime.fromisoformat(values["updated_at"])
            result.append(CompanyProfile.model_validate(values))
        return tuple(result)

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

    def get_current_direction(
        self, company_id: str, direction_id: str
    ) -> BusinessDirection:
        row = self._conn.execute(
            """SELECT direction_json
               FROM business_direction_versions
               WHERE company_id = ? AND direction_id = ?
               ORDER BY version DESC
               LIMIT 1""",
            (company_id, direction_id),
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

    def add_draft(self, record: BriefDraftRecord) -> None:
        self._conn.execute(
            """INSERT INTO brief_drafts(
                   brief_id, company_id, company_profile_version, direction_id,
                   direction_version, audience_segment_id, audience_version,
                   brief_json, status, created_by, created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                record.brief_id,
                record.company_id,
                record.company_profile_version,
                record.direction_id,
                record.direction_version,
                record.audience_segment_id,
                record.audience_version,
                record.brief_json,
                record.status,
                record.created_by,
                record.created_at,
                record.updated_at,
            ),
        )

    def get_draft(
        self, company_id: str, brief_id: str, actor_id: str
    ) -> BriefDraftRecord:
        row = self._conn.execute(
            """SELECT brief_id, company_id, company_profile_version, direction_id,
                      direction_version, audience_segment_id, audience_version,
                      brief_json, status, created_by, created_at, updated_at
               FROM brief_drafts
               WHERE company_id = ? AND brief_id = ? AND created_by = ?""",
            (company_id, brief_id, actor_id),
        ).fetchone()
        if row is None:
            raise NotFound
        return BriefDraftRecord(
            brief_id=row[0],
            company_id=row[1],
            company_profile_version=row[2],
            direction_id=row[3],
            direction_version=row[4],
            audience_segment_id=row[5],
            audience_version=row[6],
            brief_json=bytes(row[7]),
            status=row[8],
            created_by=row[9],
            created_at=row[10],
            updated_at=row[11],
        )


    def get_validated_draft(self, company_id: str, brief_id: str) -> BriefDraftRecord:
        row = self._conn.execute(
            """SELECT brief_id, company_id, company_profile_version, direction_id,
                      direction_version, audience_segment_id, audience_version,
                      brief_json, status, created_by, created_at, updated_at
               FROM brief_drafts
               WHERE company_id = ? AND brief_id = ? AND status = 'validated'""",
            (company_id, brief_id),
        ).fetchone()
        if row is None:
            raise NotFound
        return BriefDraftRecord(
            brief_id=row[0],
            company_id=row[1],
            company_profile_version=row[2],
            direction_id=row[3],
            direction_version=row[4],
            audience_segment_id=row[5],
            audience_version=row[6],
            brief_json=bytes(row[7]),
            status=row[8],
            created_by=row[9],
            created_at=row[10],
            updated_at=row[11],
        )

    def update_draft(self, scoped_company_id: str, record: BriefDraftRecord) -> None:
        cursor = self._conn.execute(
            """UPDATE brief_drafts
               SET company_id = ?, company_profile_version = ?, direction_id = ?,
                   direction_version = ?, audience_segment_id = ?, audience_version = ?,
                   brief_json = ?, status = ?, updated_at = ?
               WHERE company_id = ? AND brief_id = ? AND created_by = ?""",
            (
                record.company_id,
                record.company_profile_version,
                record.direction_id,
                record.direction_version,
                record.audience_segment_id,
                record.audience_version,
                record.brief_json,
                record.status,
                record.updated_at,
                scoped_company_id,
                record.brief_id,
                record.created_by,
            ),
        )
        if cursor.rowcount != 1:
            raise NotFound

    def add_brief(self, brief: SeoBrief) -> None:
        """Persist an already complete domain brief as validated."""
        self._conn.execute(
            """INSERT INTO brief_drafts(
                   brief_id, company_id, company_profile_version, direction_id,
                   direction_version, audience_segment_id, audience_version,
                   brief_json, status, created_by, created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'validated', ?, ?, ?)""",
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
               WHERE company_id = ? AND brief_id = ? AND status = 'validated'
                 AND company_profile_version = ? AND direction_id = ?
                 AND direction_version = ? AND audience_segment_id = ?
                 AND audience_version = ?""",
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
                snapshot.company_profile_version,
                snapshot.direction_id,
                snapshot.direction_version,
                snapshot.audience_segment_id,
                snapshot.audience_version,
            ),
        )
        if cursor.rowcount != 1:
            raise NotFound

    def _hydrate_verified(self, row: tuple[Any, ...]) -> ExecutionSnapshot:
        try:
            if type(row[9]) is not bytes:
                raise DataIntegrityError
            context_bytes = row[9]
            context = json.loads(context_bytes)
            if canonical_json(context) != context_bytes:
                raise DataIntegrityError
            if sha256_fingerprint(context) != row[10]:
                raise DataIntegrityError
            snapshot = ExecutionSnapshot(
                snapshot_id=row[0],
                brief_id=row[1],
                company_id=row[2],
                company_profile_version=row[3],
                direction_id=row[4],
                direction_version=row[5],
                audience_segment_id=row[6],
                audience_version=row[7],
                prompt_set_version=row[8],
                compiled_context=context,
                snapshot_hash=row[10],
                created_at=datetime.fromisoformat(row[11]),
            )
        except DataIntegrityError:
            raise
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DataIntegrityError from exc

        if type(context) is not dict or set(context) != _SNAPSHOT_CONTEXT_KEYS:
            raise DataIntegrityError
        company = context.get("company")
        direction = context.get("direction")
        audience = context.get("audience")
        brief = context.get("brief")
        if any(type(value) is not dict for value in (company, direction, audience, brief)):
            raise DataIntegrityError
        company = cast(dict[str, Any], company)
        direction = cast(dict[str, Any], direction)
        audience = cast(dict[str, Any], audience)
        brief = cast(dict[str, Any], brief)
        expected_values = (
            (context.get("schema_version"), 1),
            (context.get("prompt_set_version"), snapshot.prompt_set_version),
            (company.get("company_id"), snapshot.company_id),
            (company.get("company_profile_version"), snapshot.company_profile_version),
            (direction.get("company_id"), snapshot.company_id),
            (direction.get("company_profile_version"), snapshot.company_profile_version),
            (direction.get("direction_id"), snapshot.direction_id),
            (direction.get("direction_version"), snapshot.direction_version),
            (audience.get("company_id"), snapshot.company_id),
            (audience.get("direction_id"), snapshot.direction_id),
            (audience.get("direction_version"), snapshot.direction_version),
            (audience.get("audience_segment_id"), snapshot.audience_segment_id),
            (audience.get("audience_version"), snapshot.audience_version),
            (brief.get("brief_id"), snapshot.brief_id),
            (brief.get("company_id"), snapshot.company_id),
            (brief.get("company_profile_version"), snapshot.company_profile_version),
            (brief.get("direction_id"), snapshot.direction_id),
            (brief.get("direction_version"), snapshot.direction_version),
            (brief.get("audience_segment_id"), snapshot.audience_segment_id),
            (brief.get("audience_version"), snapshot.audience_version),
        )
        if any(
            type(actual) is not type(expected) or actual != expected
            for actual, expected in expected_values
        ):
            raise DataIntegrityError
        for model_type, payload in (
            (CompanyProfile, company),
            (BusinessDirection, direction),
            (AudienceSegment, audience),
            (SeoBrief, brief),
        ):
            try:
                model_values = dict(payload)
                for timestamp_field in ("created_at", "updated_at"):
                    timestamp = model_values.get(timestamp_field)
                    if type(timestamp) is not str:
                        raise DataIntegrityError
                    parsed_timestamp = datetime.fromisoformat(timestamp)
                    if (
                        parsed_timestamp.tzinfo is None
                        or parsed_timestamp.utcoffset() is None
                    ):
                        raise DataIntegrityError
                    model_values[timestamp_field] = parsed_timestamp
                normalized = model_type.model_validate(model_values).model_dump(mode="json")
            except DataIntegrityError:
                raise
            except (TypeError, ValueError) as exc:
                raise DataIntegrityError from exc
            if canonical_json(normalized) != canonical_json(payload):
                raise DataIntegrityError
        return snapshot

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
        return self._hydrate_verified(row)

    def get_snapshot_by_hash(
        self, company_id: str, snapshot_hash: str
    ) -> ExecutionSnapshot | None:
        row = self._conn.execute(
            """SELECT snapshot_id, brief_id, company_id, company_profile_version,
                      direction_id, direction_version, audience_segment_id, audience_version,
                      prompt_set_version, compiled_context, snapshot_hash, created_at
               FROM execution_snapshots
               WHERE company_id = ? AND snapshot_hash = ?""",
            (company_id, snapshot_hash),
        ).fetchone()
        if row is None:
            return None
        return self._hydrate_verified(row)


class JobRepository:
    """Retrieve jobs only through explicit company scope."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def add_job(self, job: JobRecord) -> None:
        authoritative = self._conn.execute(
            """SELECT snapshot.snapshot_hash, snapshot.direction_id,
                      snapshot.audience_segment_id, brief.created_by
               FROM execution_snapshots AS snapshot
               JOIN brief_drafts AS brief
                 ON brief.company_id = snapshot.company_id
                AND brief.brief_id = snapshot.brief_id
               WHERE snapshot.company_id = ? AND snapshot.snapshot_id = ?
                 AND brief.brief_id = ?""",
            (job.company_id, job.snapshot_id, job.brief_id),
        ).fetchone()
        if authoritative is None:
            raise NotFound
        if authoritative != (
            job.snapshot_hash,
            job.direction_id,
            job.audience_segment_id,
            job.created_by,
        ):
            raise DataIntegrityError

        self._conn.execute(
            """INSERT INTO jobs(
                   job_id, brief_id, brief_fingerprint, snapshot_id, snapshot_hash,
                   company_id, direction_id, audience_segment_id, state, current_stage,
                   approved_plan_fingerprint, approval_record_id, attempt, created_by,
                   created_at, started_at, finished_at, error_code, error_summary,
                   artifact_manifest_path, plan_json, plan_fingerprint,
                   superseded_by_job_id
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                job.plan_json,
                job.plan_fingerprint,
                job.superseded_by_job_id,
            ),
        )

    def get_job(self, company_id: str, job_id: str) -> JobRecord:
        row = self._conn.execute(
            """SELECT job_id, brief_id, brief_fingerprint, snapshot_id, snapshot_hash,
                      company_id, direction_id, audience_segment_id, state, current_stage,
                      approved_plan_fingerprint, approval_record_id, attempt, created_by,
                      created_at, started_at, finished_at, error_code, error_summary,
                      artifact_manifest_path, plan_json, plan_fingerprint,
                      superseded_by_job_id
               FROM jobs
               WHERE company_id = ? AND job_id = ?""",
            (company_id, job_id),
        ).fetchone()
        if row is None:
            raise NotFound
        return JobRecord(*row)

    def supersede_pending_jobs(
        self, company_id: str, created_by: str, *, replacement_job_id: str
    ) -> None:
        self._conn.execute(
            """UPDATE jobs
               SET superseded_by_job_id = ?
               WHERE company_id = ? AND created_by = ? AND job_id != ?
                 AND superseded_by_job_id IS NULL
                 AND state IN (
                     'DRAFT', 'VALIDATED', 'PLANNED', 'AWAITING_PAID_APPROVAL'
                 )""",
            (replacement_job_id, company_id, created_by, replacement_job_id),
        )

    def append_transition(
        self,
        company_id: str,
        job_id: str,
        *,
        from_state: str | None,
        to_state: str,
        occurred_at: str,
        reason_summary: str | None,
    ) -> None:
        cursor = self._conn.execute(
            """INSERT INTO job_transitions(
                   job_id, from_state, to_state, reason_summary, occurred_at
               )
               SELECT job_id, ?, ?, ?, ?
               FROM jobs
               WHERE company_id = ? AND job_id = ?""",
            (
                from_state,
                to_state,
                reason_summary,
                occurred_at,
                company_id,
                job_id,
            ),
        )
        if cursor.rowcount != 1:
            raise NotFound

    def compare_and_swap_state(
        self,
        company_id: str,
        job_id: str,
        *,
        expected_state: str,
        target_state: str,
        attempt: int,
        started_at: str | None,
        finished_at: str | None,
        current_stage: str | None,
        error_code: str | None,
        error_summary: str | None,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE jobs
               SET state = ?, attempt = ?, started_at = ?, finished_at = ?,
                   current_stage = ?, error_code = ?, error_summary = ?
               WHERE company_id = ? AND job_id = ? AND state = ?
                 AND superseded_by_job_id IS NULL""",
            (
                target_state,
                attempt,
                started_at,
                finished_at,
                current_stage,
                error_code,
                error_summary,
                company_id,
                job_id,
                expected_state,
            ),
        )
        return cursor.rowcount == 1

    def bind_artifact_manifest(
        self, company_id: str, job_id: str, manifest_path: str
    ) -> bool:
        """Attach one manifest to one succeeded job without permitting replacement."""
        if type(manifest_path) is not str or not manifest_path:
            raise ValueError("manifest path must be a non-empty string")
        cursor = self._conn.execute(
            """UPDATE jobs
               SET artifact_manifest_path = ?
               WHERE company_id = ? AND job_id = ? AND state = 'SUCCEEDED'
                 AND artifact_manifest_path IS NULL
                 AND superseded_by_job_id IS NULL""",
            (manifest_path, company_id, job_id),
        )
        return cursor.rowcount == 1

    def bind_paid_approval(
        self,
        company_id: str,
        job_id: str,
        *,
        expected_state: str,
        plan_fingerprint: str,
        approval_record_id: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE OR IGNORE jobs
               SET state = 'QUEUED', approved_plan_fingerprint = ?,
                   approval_record_id = ?
               WHERE company_id = ? AND job_id = ? AND state = ?
                 AND plan_fingerprint = ?
                 AND approved_plan_fingerprint IS NULL
                 AND approval_record_id IS NULL
                 AND superseded_by_job_id IS NULL""",
            (
                plan_fingerprint,
                approval_record_id,
                company_id,
                job_id,
                expected_state,
                plan_fingerprint,
            ),
        )
        return cursor.rowcount == 1


class RunnerRepository:
    """Lease and persist executor work without owning transaction boundaries."""

    _RUN_COLUMNS = """company_id, job_id, attempt, idempotency_key,
                       external_run_id, executor_name, external_accepted_at,
                       acceptance_observed_at, external_status,
                       current_stage, next_action_at, submission_attempted_at,
                       completion_observed_at, error_code, error_summary,
                       result_json, result_hash, heartbeat_at, lease_token,
                       lease_expires_at, reconciliation_count,
                       retry_stage_id, transient_failure_count, created_at, updated_at"""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def record_heartbeat(self, runner_id: str, now: str) -> None:
        self._conn.execute(
            """INSERT INTO runner_heartbeats(runner_id, started_at, heartbeat_at)
               VALUES (?, ?, ?)
               ON CONFLICT(runner_id) DO UPDATE SET heartbeat_at = excluded.heartbeat_at""",
            (runner_id, now, now),
        )

    def next_invalid_scheduler_candidate(self) -> RunnerCandidate | None:
        row = self._conn.execute(
            """SELECT job.company_id, job.job_id, job.attempt, job.state
               FROM jobs AS job
               LEFT JOIN job_execution_runs AS run
                 ON run.company_id = job.company_id
                AND run.job_id = job.job_id
                AND run.attempt = job.attempt
               WHERE job.superseded_by_job_id IS NULL
                 AND (
                     (job.started_at IS NOT NULL
                      AND is_utc_timestamp(job.started_at) != 1)
                     OR (run.next_action_at IS NOT NULL
                         AND is_utc_timestamp(run.next_action_at) != 1)
                     OR (run.lease_expires_at IS NOT NULL
                         AND is_utc_timestamp(run.lease_expires_at) != 1)
                 )
               ORDER BY job.company_id, job.job_id
               LIMIT 1"""
        ).fetchone()
        return None if row is None else RunnerCandidate(*row)

    def quarantine_invalid_scheduler_timestamps(
        self, candidate: RunnerCandidate, *, now: str
    ) -> bool:
        current = self._conn.execute(
            """SELECT 1
               FROM jobs AS job
               LEFT JOIN job_execution_runs AS run
                 ON run.company_id = job.company_id
                AND run.job_id = job.job_id
                AND run.attempt = job.attempt
               WHERE job.company_id = ? AND job.job_id = ? AND job.attempt = ?
                 AND job.superseded_by_job_id IS NULL
                 AND (
                     (job.started_at IS NOT NULL
                      AND is_utc_timestamp(job.started_at) != 1)
                     OR (run.next_action_at IS NOT NULL
                         AND is_utc_timestamp(run.next_action_at) != 1)
                     OR (run.lease_expires_at IS NOT NULL
                         AND is_utc_timestamp(run.lease_expires_at) != 1)
                 )""",
            (candidate.company_id, candidate.job_id, candidate.attempt),
        ).fetchone()
        if current is None:
            return False
        self._conn.execute(
            """UPDATE job_execution_runs
               SET current_stage = 'reconciliation_required',
                   next_action_at = NULL,
                   lease_token = NULL,
                   lease_expires_at = NULL,
                   error_code = 'SCHEDULER_TIMESTAMP_INVALID',
                   error_summary = 'scheduler timestamp requires reconciliation',
                   reconciliation_count = reconciliation_count + 1,
                   heartbeat_at = ?, updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?""",
            (
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
            ),
        )
        job_cursor = self._conn.execute(
            """UPDATE jobs
               SET current_stage = 'reconciliation_required',
                   error_code = 'SCHEDULER_TIMESTAMP_INVALID',
                   error_summary = 'scheduler timestamp requires reconciliation'
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND superseded_by_job_id IS NULL""",
            (
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
            ),
        )
        return job_cursor.rowcount == 1

    def next_recovery_candidate(
        self, *, now: str, stale_before: str
    ) -> RunnerCandidate | None:
        row = self._conn.execute(
            """SELECT job.company_id, job.job_id, job.attempt, job.state
               FROM jobs AS job
               LEFT JOIN job_execution_runs AS run
                 ON run.company_id = job.company_id
                AND run.job_id = job.job_id
                AND run.attempt = job.attempt
               WHERE job.superseded_by_job_id IS NULL
                 AND (
                     (
                         job.state = 'QUEUED'
                         AND COALESCE(job.current_stage, '') != 'reconciliation_required'
                         AND (run.lease_token IS NULL OR run.lease_expires_at <= ?)
                     )
                     OR (
                         job.state = 'RUNNING'
                         AND job.current_stage = 'dispatching'
                         AND run.external_run_id IS NULL
                         AND run.external_status IS NULL
                         AND (run.next_action_at IS NULL OR run.next_action_at <= ?)
                         AND (run.lease_token IS NULL OR run.lease_expires_at <= ?)
                     )
                     OR (
                         job.state = 'RUNNING'
                         AND run.job_id IS NULL
                         AND job.started_at IS NOT NULL
                         AND job.started_at <= ?
                         AND COALESCE(job.current_stage, '') != 'reconciliation_required'
                     )
                 )
               ORDER BY CASE job.state WHEN 'QUEUED' THEN 0 ELSE 1 END,
                        job.created_at, job.company_id, job.job_id
               LIMIT 1""",
            (now, now, now, stale_before),
        ).fetchone()
        return None if row is None else RunnerCandidate(*row)

    def next_poll_candidate(self, *, now: str) -> RunnerCandidate | None:
        row = self._conn.execute(
            """SELECT job.company_id, job.job_id, job.attempt, job.state
               FROM jobs AS job
               JOIN job_execution_runs AS run
                 ON run.company_id = job.company_id
                AND run.job_id = job.job_id
                AND run.attempt = job.attempt
               WHERE job.state = 'RUNNING'
                 AND job.superseded_by_job_id IS NULL
                 AND run.external_run_id IS NOT NULL
                 AND run.external_status IN ('ACCEPTED', 'RUNNING')
                 AND COALESCE(run.current_stage, '') != 'reconciliation_required'
                 AND (run.next_action_at IS NULL OR run.next_action_at <= ?)
                 AND (run.lease_token IS NULL OR run.lease_expires_at <= ?)
               ORDER BY COALESCE(run.next_action_at, run.updated_at),
                        job.company_id, job.job_id
               LIMIT 1""",
            (now, now),
        ).fetchone()
        return None if row is None else RunnerCandidate(*row)

    def next_retry_candidate(self, *, now: str) -> RunnerCandidate | None:
        row = self._conn.execute(
            """SELECT job.company_id, job.job_id, job.attempt, job.state
               FROM jobs AS job
               JOIN job_execution_runs AS run
                 ON run.company_id = job.company_id
                AND run.job_id = job.job_id
                AND run.attempt = job.attempt
               WHERE job.state = 'FAILED_RETRYABLE'
                 AND job.superseded_by_job_id IS NULL
                 AND run.external_status = 'FAILED_RETRYABLE'
                 AND run.next_action_at IS NOT NULL
                 AND run.next_action_at <= ?
                 AND (run.lease_token IS NULL OR run.lease_expires_at <= ?)
               ORDER BY run.next_action_at, job.company_id, job.job_id
               LIMIT 1""",
            (now, now),
        ).fetchone()
        return None if row is None else RunnerCandidate(*row)

    def next_cancel_candidate(self, *, now: str) -> RunnerCandidate | None:
        row = self._conn.execute(
            """SELECT job.company_id, job.job_id, job.attempt, job.state
               FROM jobs AS job
               JOIN job_execution_runs AS run
                 ON run.company_id = job.company_id
                AND run.job_id = job.job_id
                AND run.attempt = job.attempt
               WHERE job.state = 'CANCELED'
                 AND job.superseded_by_job_id IS NULL
                 AND run.external_status IS NOT 'CANCELED'
                 AND COALESCE(run.current_stage, '') != 'cancel_reconciliation_required'
                 AND (
                     COALESCE(run.current_stage, '') != 'cancel_retry_pending'
                     OR run.next_action_at IS NULL
                     OR run.next_action_at <= ?
                 )
                 AND (run.lease_token IS NULL OR run.lease_expires_at <= ?)
               ORDER BY job.finished_at, job.company_id, job.job_id
               LIMIT 1""",
            (now, now),
        ).fetchone()
        return None if row is None else RunnerCandidate(*row)

    def next_terminal_recovery_candidate(self, *, now: str) -> RunnerCandidate | None:
        row = self._conn.execute(
            """SELECT job.company_id, job.job_id, job.attempt, job.state
               FROM jobs AS job
               JOIN job_execution_runs AS run
                 ON run.company_id = job.company_id
                AND run.job_id = job.job_id
                AND run.attempt = job.attempt
               WHERE job.superseded_by_job_id IS NULL
                 AND COALESCE(run.current_stage, '') != 'reconciliation_required'
                 AND (
                     (
                         job.state = 'RUNNING'
                         AND run.external_status IN (
                             'SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_FINAL', 'CANCELED'
                         )
                     )
                     OR (
                         job.state = 'QUEUED'
                         AND run.external_run_id IS NULL
                         AND run.external_status IN ('FAILED_RETRYABLE', 'FAILED_FINAL')
                     )
                     OR (
                         job.state = 'SUCCEEDED'
                         AND run.external_status = 'SUCCEEDED'
                         AND job.artifact_manifest_path IS NULL
                     )
                 )
                 AND (run.lease_token IS NULL OR run.lease_expires_at <= ?)
               ORDER BY run.completion_observed_at, job.company_id, job.job_id
               LIMIT 1""",
            (now,),
        ).fetchone()
        return None if row is None else RunnerCandidate(*row)

    def claim_existing_run(
        self,
        candidate: RunnerCandidate,
        *,
        expected_statuses: tuple[str, ...],
        now: str,
        lease_token: str,
        lease_expires_at: str,
    ) -> ExecutionRunRecord | None:
        if not expected_statuses:
            raise ValueError("expected_statuses cannot be empty")
        placeholders = ", ".join("?" for _status in expected_statuses)
        cursor = self._conn.execute(
            f"""UPDATE job_execution_runs
                SET lease_token = ?, lease_expires_at = ?, heartbeat_at = ?, updated_at = ?
                WHERE company_id = ? AND job_id = ? AND attempt = ?
                  AND external_status IN ({placeholders})
                  AND (lease_token IS NULL OR lease_expires_at <= ? OR lease_token = ?)
                  AND EXISTS (
                      SELECT 1 FROM jobs
                      WHERE jobs.company_id = job_execution_runs.company_id
                        AND jobs.job_id = job_execution_runs.job_id
                        AND jobs.attempt = job_execution_runs.attempt
                        AND jobs.state = ?
                        AND jobs.superseded_by_job_id IS NULL
                  )""",
            (
                lease_token,
                lease_expires_at,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                *expected_statuses,
                now,
                lease_token,
                candidate.state,
            ),
        )
        if cursor.rowcount != 1:
            return None
        return self.get_run(candidate.company_id, candidate.job_id, candidate.attempt)

    def claim_submission(
        self,
        candidate: RunnerCandidate,
        *,
        idempotency_key: str,
        executor_name: str,
        now: str,
        lease_token: str,
        lease_expires_at: str,
    ) -> ExecutionRunRecord | None:
        if candidate.state not in {"QUEUED", "RUNNING"}:
            raise ValueError("submission candidate must be queued or dispatching")
        self._conn.execute(
            """INSERT INTO job_execution_runs(
                   company_id, job_id, attempt, idempotency_key, executor_name,
                   submission_attempted_at, heartbeat_at, lease_token,
                   lease_expires_at, created_at, updated_at
               ) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                 FROM jobs
                 WHERE company_id = ? AND job_id = ? AND attempt = ?
                   AND state = ? AND superseded_by_job_id IS NULL
                   AND (? != 'RUNNING' OR current_stage = 'dispatching')
               ON CONFLICT(company_id, job_id, attempt) DO NOTHING""",
            (
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                idempotency_key,
                executor_name,
                now,
                now,
                lease_token,
                lease_expires_at,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                candidate.state,
                candidate.state,
            ),
        )
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET executor_name = COALESCE(executor_name, ?),
                   lease_token = ?, lease_expires_at = ?, heartbeat_at = ?, updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND idempotency_key = ?
                 AND (executor_name IS NULL OR executor_name = ?)
                 AND (lease_token IS NULL OR lease_expires_at <= ? OR lease_token = ?)
                 AND EXISTS (
                     SELECT 1 FROM jobs
                     WHERE jobs.company_id = job_execution_runs.company_id
                       AND jobs.job_id = job_execution_runs.job_id
                       AND jobs.attempt = job_execution_runs.attempt
                       AND jobs.state = ?
                       AND (? != 'RUNNING' OR jobs.current_stage = 'dispatching')
                       AND jobs.superseded_by_job_id IS NULL
                 )""",
            (
                executor_name,
                lease_token,
                lease_expires_at,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                idempotency_key,
                executor_name,
                now,
                lease_token,
                candidate.state,
                candidate.state,
            ),
        )
        if cursor.rowcount != 1:
            return None
        if candidate.state == "QUEUED":
            job_cursor = self._conn.execute(
                """UPDATE jobs
                   SET state = 'RUNNING', current_stage = 'dispatching',
                       started_at = COALESCE(started_at, ?)
                   WHERE company_id = ? AND job_id = ? AND attempt = ?
                     AND state = 'QUEUED' AND superseded_by_job_id IS NULL""",
                (now, candidate.company_id, candidate.job_id, candidate.attempt),
            )
            if job_cursor.rowcount != 1:
                raise DataIntegrityError
            transition_cursor = self._conn.execute(
                """INSERT INTO job_transitions(
                       job_id, from_state, to_state, reason_summary, occurred_at
                   ) SELECT job_id, 'QUEUED', 'RUNNING', 'external dispatch claimed', ?
                     FROM jobs
                     WHERE company_id = ? AND job_id = ? AND attempt = ?
                       AND state = 'RUNNING' AND current_stage = 'dispatching'""",
                (now, candidate.company_id, candidate.job_id, candidate.attempt),
            )
            if transition_cursor.rowcount != 1:
                raise DataIntegrityError
        return self.get_run(candidate.company_id, candidate.job_id, candidate.attempt)

    def get_run(self, company_id: str, job_id: str, attempt: int) -> ExecutionRunRecord:
        row = self._conn.execute(
            f"""SELECT {self._RUN_COLUMNS}
                FROM job_execution_runs
                WHERE company_id = ? AND job_id = ? AND attempt = ?""",
            (company_id, job_id, attempt),
        ).fetchone()
        if row is None:
            raise NotFound
        return ExecutionRunRecord(*row)

    def retry_count_for_stage(
        self, company_id: str, job_id: str, retry_stage_id: str
    ) -> int:
        row = self._conn.execute(
            """SELECT failure_count
               FROM job_stage_retry_budgets
               WHERE company_id = ? AND job_id = ? AND retry_stage_id = ?""",
            (company_id, job_id, retry_stage_id),
        ).fetchone()
        if row is None:
            return 0
        if type(row[0]) is not int or row[0] < 0:
            raise DataIntegrityError
        return row[0]

    def _consume_stage_retry(
        self, candidate: RunnerCandidate, *, retry_stage_id: str, now: str
    ) -> None:
        self._conn.execute(
            """INSERT INTO job_stage_retry_budgets(
                   company_id, job_id, retry_stage_id, failure_count, created_at, updated_at
               ) VALUES (?, ?, ?, 1, ?, ?)
               ON CONFLICT(company_id, job_id, retry_stage_id) DO UPDATE SET
                   failure_count = failure_count + 1,
                   updated_at = excluded.updated_at""",
            (
                candidate.company_id,
                candidate.job_id,
                retry_stage_id,
                now,
                now,
            ),
        )

    def record_external_acceptance(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        external_run_id: str,
        executor_name: str,
        idempotency_key: str,
        accepted_at: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET external_run_id = ?, executor_name = ?, external_accepted_at = ?,
                   acceptance_observed_at = ?, external_status = 'ACCEPTED',
                   next_action_at = NULL, error_code = NULL, error_summary = NULL,
                   retry_stage_id = 'poll:unknown', transient_failure_count = 0,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND lease_token = ? AND idempotency_key = ?
                 AND external_run_id IS NULL""",
            (
                external_run_id,
                executor_name,
                accepted_at,
                now,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
                idempotency_key,
            ),
        )
        return cursor.rowcount == 1

    def claim_canceled_lookup(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        lease_expires_at: str,
        now: str,
    ) -> ExecutionRunRecord | None:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET lease_token = ?, lease_expires_at = ?, heartbeat_at = ?, updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND external_run_id IS NULL AND external_status IS NULL
                 AND (lease_token IS NULL OR lease_expires_at <= ? OR lease_token = ?)
                 AND EXISTS (
                     SELECT 1 FROM jobs
                     WHERE jobs.company_id = job_execution_runs.company_id
                       AND jobs.job_id = job_execution_runs.job_id
                       AND jobs.attempt = job_execution_runs.attempt
                       AND jobs.state = 'CANCELED'
                       AND jobs.current_stage = 'dispatching'
                       AND jobs.superseded_by_job_id IS NULL
                 )""",
            (
                lease_token,
                lease_expires_at,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                now,
                lease_token,
            ),
        )
        if cursor.rowcount != 1:
            return None
        return self.get_run(candidate.company_id, candidate.job_id, candidate.attempt)

    def record_lookup_acceptance_for_canceled(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        external_run_id: str,
        executor_name: str,
        idempotency_key: str,
        accepted_at: str,
        now: str,
    ) -> bool:
        """Attach one side-effect-free lookup result to a canceled dispatch."""
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET external_run_id = ?, executor_name = ?, external_accepted_at = ?,
                   acceptance_observed_at = ?, external_status = 'ACCEPTED',
                   next_action_at = NULL, error_code = NULL, error_summary = NULL,
                   retry_stage_id = 'poll:unknown', transient_failure_count = 0,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND idempotency_key = ? AND lease_token = ? AND external_run_id IS NULL
                 AND EXISTS (
                     SELECT 1 FROM jobs
                     WHERE jobs.company_id = job_execution_runs.company_id
                       AND jobs.job_id = job_execution_runs.job_id
                       AND jobs.attempt = job_execution_runs.attempt
                       AND jobs.state = 'CANCELED'
                       AND jobs.current_stage = 'dispatching'
                       AND jobs.superseded_by_job_id IS NULL
                 )""",
            (
                external_run_id,
                executor_name,
                accepted_at,
                now,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                idempotency_key,
                lease_token,
            ),
        )
        return cursor.rowcount == 1

    def mark_claimed_cancel_lookup_reconciliation(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        error_code: str,
        error_summary: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET current_stage = 'cancel_reconciliation_required', next_action_at = NULL,
                   error_code = ?, error_summary = ?,
                   reconciliation_count = reconciliation_count + 1,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND lease_token = ? AND external_run_id IS NULL
                 AND external_status IS NULL""",
            (
                error_code,
                error_summary,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
            ),
        )
        if cursor.rowcount != 1:
            return False
        job_cursor = self._conn.execute(
            """UPDATE jobs SET current_stage = 'cancel_reconciliation_required'
               WHERE company_id = ? AND job_id = ? AND attempt = ? AND state = 'CANCELED'""",
            (candidate.company_id, candidate.job_id, candidate.attempt),
        )
        return job_cursor.rowcount == 1

    def record_submit_transport_failure(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        next_action_at: str | None,
        error_code: str,
        error_summary: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET next_action_at = ?,
                   error_code = ?, error_summary = ?,
                   transient_failure_count = transient_failure_count + 1,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND lease_token = ? AND external_run_id IS NULL
                 AND external_status IS NULL""",
            (
                next_action_at,
                error_code,
                error_summary,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
            ),
        )
        if cursor.rowcount != 1:
            return False
        self._consume_stage_retry(candidate, retry_stage_id="submission", now=now)
        return True

    def record_poll_transport_failure(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        next_action_at: str | None,
        current_stage: str | None,
        retry_stage_id: str,
        error_code: str,
        error_summary: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET next_action_at = ?, current_stage = COALESCE(?, current_stage),
                   retry_stage_id = ?, error_code = ?, error_summary = ?,
                   transient_failure_count = transient_failure_count + 1,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND lease_token = ? AND external_run_id IS NOT NULL
                 AND external_status IN ('ACCEPTED', 'RUNNING')""",
            (
                next_action_at,
                current_stage,
                retry_stage_id,
                error_code,
                error_summary,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
            ),
        )
        if cursor.rowcount != 1:
            return False
        self._consume_stage_retry(
            candidate, retry_stage_id=retry_stage_id, now=now
        )
        return True

    def record_poll_status(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        external_status: str,
        current_stage: str | None,
        retry_stage_id: str,
        consume_retry: bool,
        next_action_at: str | None,
        completion_observed_at: str | None,
        error_code: str | None,
        error_summary: str | None,
        result_json: bytes | None,
        result_hash: str | None,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET external_status = ?, current_stage = ?, next_action_at = ?,
                   completion_observed_at = ?, error_code = ?, error_summary = ?,
                   result_json = COALESCE(result_json, ?),
                   result_hash = COALESCE(result_hash, ?),
                   transient_failure_count = CASE
                       WHEN retry_stage_id != ? THEN ?
                       WHEN ? THEN transient_failure_count + 1
                       ELSE transient_failure_count END,
                   retry_stage_id = ?,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND lease_token = ?
                 AND external_status IN ('ACCEPTED', 'RUNNING')
                 AND (result_json IS NULL OR (result_json = ? AND result_hash = ?))""",
            (
                external_status,
                current_stage,
                next_action_at,
                completion_observed_at,
                error_code,
                error_summary,
                result_json,
                result_hash,
                retry_stage_id,
                int(consume_retry),
                int(consume_retry),
                retry_stage_id,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
                result_json,
                result_hash,
            ),
        )
        if cursor.rowcount != 1:
            return False
        if consume_retry:
            self._consume_stage_retry(
                candidate, retry_stage_id=retry_stage_id, now=now
            )
        return True

    def reconcile_poll_after_local_cancel(
        self,
        candidate: RunnerCandidate,
        *,
        terminal: bool,
        now: str,
    ) -> bool:
        if terminal:
            cursor = self._conn.execute(
                """UPDATE job_execution_runs
                   SET current_stage = 'cancel_reconciliation_required',
                       next_action_at = NULL,
                       reconciliation_count = reconciliation_count + 1,
                       heartbeat_at = ?, updated_at = ?
                   WHERE company_id = ? AND job_id = ? AND attempt = ?
                     AND lease_token IS NULL
                     AND external_status IN (
                         'SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_FINAL'
                     )
                     AND EXISTS (
                         SELECT 1 FROM jobs
                         WHERE company_id = ? AND job_id = ? AND attempt = ?
                           AND state = 'CANCELED'
                     )""",
                (
                    now,
                    now,
                    candidate.company_id,
                    candidate.job_id,
                    candidate.attempt,
                    candidate.company_id,
                    candidate.job_id,
                    candidate.attempt,
                ),
            )
        else:
            cursor = self._conn.execute(
                """UPDATE job_execution_runs
                   SET next_action_at = NULL, heartbeat_at = ?, updated_at = ?
                   WHERE company_id = ? AND job_id = ? AND attempt = ?
                     AND lease_token IS NULL
                     AND external_status IN ('ACCEPTED', 'RUNNING')
                     AND EXISTS (
                         SELECT 1 FROM jobs
                         WHERE company_id = ? AND job_id = ? AND attempt = ?
                           AND state = 'CANCELED'
                     )""",
                (
                    now,
                    now,
                    candidate.company_id,
                    candidate.job_id,
                    candidate.attempt,
                    candidate.company_id,
                    candidate.job_id,
                    candidate.attempt,
                ),
            )
        return cursor.rowcount == 1

    def reconcile_terminal_after_local_cancel(
        self,
        candidate: RunnerCandidate,
        *,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET current_stage = 'cancel_reconciliation_required',
                   next_action_at = NULL,
                   reconciliation_count = reconciliation_count + 1,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND external_status IN ('SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_FINAL')
                 AND (lease_token IS NULL OR lease_expires_at <= ?)
                 AND EXISTS (
                     SELECT 1 FROM jobs
                     WHERE company_id = ? AND job_id = ? AND attempt = ?
                       AND state = 'CANCELED'
                 )""",
            (
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
            ),
        )
        return cursor.rowcount == 1

    def reconcile_claimed_terminal_after_local_cancel(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET current_stage = 'cancel_reconciliation_required',
                   next_action_at = NULL,
                   reconciliation_count = reconciliation_count + 1,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND lease_token = ?
                 AND external_status IN (
                     'SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_FINAL', 'CANCELED'
                 )
                 AND EXISTS (
                     SELECT 1 FROM jobs
                     WHERE company_id = ? AND job_id = ? AND attempt = ?
                       AND state = 'CANCELED'
                 )""",
            (
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
            ),
        )
        return cursor.rowcount == 1

    def record_exhausted_poll_after_local_cancel(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        error_code: str,
        error_summary: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET next_action_at = NULL, error_code = ?, error_summary = ?,
                   transient_failure_count = transient_failure_count + 1,
                   reconciliation_count = reconciliation_count + 1,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND lease_token = ?
                 AND external_status IN ('ACCEPTED', 'RUNNING')
                 AND EXISTS (
                     SELECT 1 FROM jobs
                     WHERE company_id = ? AND job_id = ? AND attempt = ?
                       AND state = 'CANCELED'
                 )""",
            (
                error_code,
                error_summary,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
            ),
        )
        return cursor.rowcount == 1

    def mark_poll_reconciliation_required(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        error_code: str,
        error_summary: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET current_stage = 'reconciliation_required', next_action_at = NULL,
                   error_code = ?, error_summary = ?,
                   transient_failure_count = transient_failure_count + 1,
                   reconciliation_count = reconciliation_count + 1,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND lease_token = ? AND external_run_id IS NOT NULL
                 AND external_status IN ('ACCEPTED', 'RUNNING')""",
            (
                error_code,
                error_summary,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
            ),
        )
        if cursor.rowcount != 1:
            return False
        job_cursor = self._conn.execute(
            """UPDATE jobs SET current_stage = 'reconciliation_required'
               WHERE company_id = ? AND job_id = ? AND attempt = ? AND state = 'RUNNING'""",
            (candidate.company_id, candidate.job_id, candidate.attempt),
        )
        return job_cursor.rowcount == 1

    def mark_retry_consumed(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET next_action_at = NULL, heartbeat_at = ?, lease_token = NULL,
                   lease_expires_at = NULL, updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND lease_token = ? AND external_status = 'FAILED_RETRYABLE'""",
            (
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
            ),
        )
        return cursor.rowcount == 1

    def update_running_stage(
        self,
        candidate: RunnerCandidate,
        *,
        current_stage: str | None,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE jobs SET current_stage = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND state = 'RUNNING' AND superseded_by_job_id IS NULL""",
            (
                current_stage,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
            ),
        )
        return cursor.rowcount == 1

    def mark_claimed_reconciliation_required(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        error_code: str,
        error_summary: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET current_stage = 'reconciliation_required', next_action_at = NULL,
                   error_code = ?, error_summary = ?,
                   reconciliation_count = reconciliation_count + 1,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND lease_token = ?""",
            (
                error_code,
                error_summary,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
            ),
        )
        if cursor.rowcount != 1:
            return False
        job_cursor = self._conn.execute(
            """UPDATE jobs
               SET current_stage = 'reconciliation_required',
                   error_code = ?, error_summary = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND state IN (
                     'QUEUED', 'RUNNING', 'SUCCEEDED',
                     'FAILED_RETRYABLE', 'FAILED_FINAL', 'CANCELED'
                 )""",
            (
                error_code,
                error_summary,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
            ),
        )
        return job_cursor.rowcount == 1

    def mark_observed_reconciliation_required(
        self,
        candidate: RunnerCandidate,
        *,
        error_code: str,
        error_summary: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET current_stage = 'reconciliation_required', next_action_at = NULL,
                   error_code = ?, error_summary = ?,
                   reconciliation_count = reconciliation_count + 1,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND external_run_id IS NOT NULL""",
            (
                error_code,
                error_summary,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
            ),
        )
        if cursor.rowcount != 1:
            return False
        job_cursor = self._conn.execute(
            """UPDATE jobs
               SET current_stage = 'reconciliation_required',
                   error_code = ?, error_summary = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND state IN ('RUNNING', 'SUCCEEDED', 'CANCELED')""",
            (
                error_code,
                error_summary,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
            ),
        )
        return job_cursor.rowcount == 1

    def quarantine_running_dispatch(
        self,
        candidate: RunnerCandidate,
        *,
        error_code: str,
        error_summary: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET current_stage = 'reconciliation_required', next_action_at = NULL,
                   error_code = ?, error_summary = ?,
                   reconciliation_count = reconciliation_count + 1,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND external_run_id IS NULL AND external_status IS NULL
                 AND (lease_token IS NULL OR lease_expires_at <= ?)""",
            (
                error_code,
                error_summary,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                now,
            ),
        )
        if cursor.rowcount != 1:
            return False
        job_cursor = self._conn.execute(
            """UPDATE jobs
               SET current_stage = 'reconciliation_required',
                   error_code = ?, error_summary = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND state = 'RUNNING' AND current_stage = 'dispatching'""",
            (
                error_code,
                error_summary,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
            ),
        )
        return job_cursor.rowcount == 1

    def job_state(self, candidate: RunnerCandidate) -> str:
        row = self._conn.execute(
            """SELECT state FROM jobs
               WHERE company_id = ? AND job_id = ? AND attempt = ?""",
            (candidate.company_id, candidate.job_id, candidate.attempt),
        ).fetchone()
        if row is None:
            raise NotFound
        state = row[0]
        if type(state) is not str:
            raise DataIntegrityError
        return state

    def begin_cancel_stage(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET transient_failure_count = CASE
                       WHEN retry_stage_id = 'cancel'
                       THEN transient_failure_count ELSE 0 END,
                   retry_stage_id = 'cancel',
                   current_stage = CASE
                       WHEN current_stage = 'cancel_retry_pending'
                       THEN current_stage ELSE 'canceling' END,
                   heartbeat_at = ?, updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND lease_token = ? AND external_status IN ('ACCEPTED', 'RUNNING')""",
            (
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
            ),
        )
        return cursor.rowcount == 1

    def record_cancel_transport_failure(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        next_action_at: str,
        error_code: str,
        error_summary: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET current_stage = 'cancel_retry_pending', next_action_at = ?,
                   error_code = ?, error_summary = ?,
                   transient_failure_count = transient_failure_count + 1,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND lease_token = ? AND external_run_id IS NOT NULL
                 AND external_status IS NOT 'CANCELED'""",
            (
                next_action_at,
                error_code,
                error_summary,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
            ),
        )
        if cursor.rowcount != 1:
            return False
        self._consume_stage_retry(candidate, retry_stage_id="cancel", now=now)
        return True

    def mark_known_cancel_reconciliation_required(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        error_code: str,
        error_summary: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET current_stage = 'cancel_reconciliation_required', next_action_at = NULL,
                   error_code = ?, error_summary = ?,
                   transient_failure_count = transient_failure_count + 1,
                   reconciliation_count = reconciliation_count + 1,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND lease_token = ? AND external_run_id IS NOT NULL
                 AND external_status IS NOT 'CANCELED'""",
            (
                error_code,
                error_summary,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
            ),
        )
        if cursor.rowcount != 1:
            return False
        job_cursor = self._conn.execute(
            """UPDATE jobs SET current_stage = 'cancel_reconciliation_required'
               WHERE company_id = ? AND job_id = ? AND attempt = ? AND state = 'CANCELED'""",
            (candidate.company_id, candidate.job_id, candidate.attempt),
        )
        return job_cursor.rowcount == 1

    def record_remote_canceled(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET external_status = 'CANCELED', next_action_at = NULL,
                   completion_observed_at = COALESCE(completion_observed_at, ?),
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND lease_token = ? AND external_run_id IS NOT NULL""",
            (
                now,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
            ),
        )
        return cursor.rowcount == 1

    def mark_cancel_reconciliation_required(
        self,
        candidate: RunnerCandidate,
        *,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET current_stage = 'cancel_reconciliation_required',
                   reconciliation_count = reconciliation_count + 1,
                   heartbeat_at = ?, lease_token = NULL, lease_expires_at = NULL,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND external_run_id IS NULL
                 AND (lease_token IS NULL OR lease_expires_at <= ?)""",
            (
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                now,
            ),
        )
        if cursor.rowcount != 1:
            return False
        job_cursor = self._conn.execute(
            """UPDATE jobs SET current_stage = 'cancel_reconciliation_required'
               WHERE company_id = ? AND job_id = ? AND attempt = ? AND state = 'CANCELED'""",
            (candidate.company_id, candidate.job_id, candidate.attempt),
        )
        return job_cursor.rowcount == 1

    def release_lease(
        self,
        candidate: RunnerCandidate,
        *,
        lease_token: str,
        now: str,
    ) -> bool:
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET lease_token = NULL, lease_expires_at = NULL, heartbeat_at = ?, updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ? AND lease_token = ?""",
            (
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                lease_token,
            ),
        )
        return cursor.rowcount == 1

    def mark_reconciliation_required(
        self, candidate: RunnerCandidate, *, idempotency_key: str, now: str
    ) -> None:
        self._conn.execute(
            """INSERT INTO job_execution_runs(
                   company_id, job_id, attempt, idempotency_key, current_stage,
                   heartbeat_at, reconciliation_count, created_at, updated_at
               ) VALUES (?, ?, ?, ?, 'reconciliation_required', ?, 1, ?, ?)
               ON CONFLICT(company_id, job_id, attempt) DO NOTHING""",
            (
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                idempotency_key,
                now,
                now,
                now,
            ),
        )
        cursor = self._conn.execute(
            """UPDATE jobs
               SET current_stage = 'reconciliation_required'
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND state = 'RUNNING' AND superseded_by_job_id IS NULL""",
            (candidate.company_id, candidate.job_id, candidate.attempt),
        )
        if cursor.rowcount != 1:
            raise NotFound


    def quarantine_queued_job(
        self,
        candidate: RunnerCandidate,
        *,
        error_code: str,
        error_summary: str,
        now: str,
    ) -> bool:
        job_row = self._conn.execute(
            """SELECT 1 FROM jobs
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND state = 'QUEUED' AND superseded_by_job_id IS NULL""",
            (
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
            ),
        ).fetchone()
        if job_row is None:
            return False
        self._conn.execute(
            """INSERT OR IGNORE INTO job_execution_runs(
                   company_id, job_id, attempt, idempotency_key, current_stage,
                   error_code, error_summary, heartbeat_at,
                   reconciliation_count, created_at, updated_at
               ) VALUES (?, ?, ?, ?, 'reconciliation_required', ?, ?, ?, 1, ?, ?)""",
            (
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
                f"{candidate.company_id}:{candidate.job_id}:{candidate.attempt}",
                error_code,
                error_summary,
                now,
                now,
                now,
            ),
        )
        cursor = self._conn.execute(
            """UPDATE job_execution_runs
               SET current_stage = 'reconciliation_required',
                   error_code = ?, error_summary = ?, heartbeat_at = ?,
                   reconciliation_count = CASE
                       WHEN current_stage = 'reconciliation_required'
                       THEN reconciliation_count
                       ELSE reconciliation_count + 1
                   END,
                   updated_at = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND external_run_id IS NULL AND external_status IS NULL
                 AND lease_token IS NULL""",
            (
                error_code,
                error_summary,
                now,
                now,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
            ),
        )
        if cursor.rowcount != 1:
            return False
        job_cursor = self._conn.execute(
            """UPDATE jobs
               SET current_stage = 'reconciliation_required',
                   error_code = ?, error_summary = ?
               WHERE company_id = ? AND job_id = ? AND attempt = ?
                 AND state = 'QUEUED' AND superseded_by_job_id IS NULL""",
            (
                error_code,
                error_summary,
                candidate.company_id,
                candidate.job_id,
                candidate.attempt,
            ),
        )
        return job_cursor.rowcount == 1


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
                _storage_timestamp(approval.approved_at),
                _storage_timestamp(approval.expires_at),
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
        approved_at = _domain_timestamp(row[6], optional=False)
        if approved_at is None:
            raise DataIntegrityError
        expires_at = _domain_timestamp(row[7], optional=True)
        try:
            return ApprovalRecord(
                approval_record_id=row[0],
                job_id=row[1],
                approval_type=row[2],
                snapshot_hash=row[3],
                plan_fingerprint=row[4],
                approved_by=row[5],
                approved_at=approved_at,
                expires_at=expires_at,
            )
        except (TypeError, ValueError) as exc:
            raise DataIntegrityError from exc


class WebhookNonceRepository:
    """Persist replay nonces with transaction ownership left to the caller."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def consume_nonce(self, nonce: str, received_at: datetime, expires_at: datetime) -> bool:
        """Atomically record one fresh nonce and prune entries expired at receipt time."""
        if type(nonce) is not str or not nonce:
            raise ValueError("nonce must be a non-empty string")
        if type(received_at) is not datetime or type(expires_at) is not datetime:
            raise TypeError("nonce timestamps must be datetimes")
        if (
            received_at.tzinfo is None
            or received_at.utcoffset() is None
            or expires_at.tzinfo is None
            or expires_at.utcoffset() is None
            or expires_at <= received_at
        ):
            raise ValueError("nonce timestamps must be ordered and timezone-aware")
        received_text = _storage_timestamp(received_at)
        expires_text = _storage_timestamp(expires_at)
        if received_text is None or expires_text is None:
            raise DataIntegrityError
        self._conn.execute("DELETE FROM webhook_nonces WHERE expires_at <= ?", (received_text,))
        cursor = self._conn.execute(
            """INSERT INTO webhook_nonces(nonce, received_at, expires_at)
               VALUES (?, ?, ?)
               ON CONFLICT(nonce) DO NOTHING""",
            (nonce, received_text, expires_text),
        )
        return cursor.rowcount == 1


class WebhookCallbackReceiptRepository:
    """Persist one immutable semantic callback receipt per authenticated delivery key."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def record_receipt(
        self,
        *,
        company_id: str,
        job_id: str,
        snapshot_hash: str,
        idempotency_key: str,
        received_at: datetime,
    ) -> bool:
        if any(
            type(value) is not str or not value
            for value in (company_id, job_id, snapshot_hash, idempotency_key)
        ):
            raise ValueError("callback receipt identifiers must be non-empty strings")
        accepted_at = _storage_timestamp(received_at)
        if accepted_at is None:
            raise DataIntegrityError
        cursor = self._conn.execute(
            """INSERT INTO webhook_callback_receipts(
                   company_id, job_id, snapshot_hash, idempotency_key, accepted_at
               ) VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(company_id, job_id, snapshot_hash, idempotency_key) DO NOTHING""",
            (company_id, job_id, snapshot_hash, idempotency_key, accepted_at),
        )
        return cursor.rowcount == 1
