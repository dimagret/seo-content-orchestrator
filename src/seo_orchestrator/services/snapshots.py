"""Deterministic compiler for immutable, exact-version execution snapshots."""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from seo_orchestrator.canonical import JsonValue, sha256_fingerprint
from seo_orchestrator.db.repositories import BriefRepository, CompanyRepository, SnapshotRepository
from seo_orchestrator.domain import ExecutionSnapshot
from seo_orchestrator.errors import NotFound
from seo_orchestrator.services.briefs import ValidatedBrief


class SnapshotCompiler:
    """Compile within an explicit company scope without consulting latest card state."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        company_id: str,
        clock: Callable[[], datetime] | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._company_id = company_id
        self._companies = CompanyRepository(conn)
        self._briefs = BriefRepository(conn)
        self._snapshots = SnapshotRepository(conn)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._id_factory = id_factory or (lambda: f"snapshot-{uuid.uuid4().hex}")

    def compile_snapshot(
        self, brief_id: str, prompt_set_version: int
    ) -> ExecutionSnapshot:
        if type(prompt_set_version) is not int or prompt_set_version <= 0:
            raise ValueError("prompt_set_version must be a positive integer")
        record = self._briefs.get_validated_draft(self._company_id, brief_id)
        values = json.loads(record.brief_json)
        values.pop("version", None)
        values.pop("category_context")
        values.pop("status")
        values["created_at"] = datetime.fromisoformat(values["created_at"])
        values["updated_at"] = datetime.fromisoformat(values["updated_at"])
        brief = ValidatedBrief.model_validate(values)

        company = self._companies.get_profile(
            self._company_id, brief.company_profile_version
        )
        direction = self._companies.get_direction(
            self._company_id, brief.direction_id, brief.direction_version
        )
        audience = self._companies.get_audience(
            self._company_id,
            brief.direction_id,
            brief.audience_segment_id,
            brief.audience_version,
        )
        if (
            direction.company_profile_version != company.company_profile_version
            or audience.direction_version != direction.direction_version
        ):
            raise NotFound

        context: JsonValue = {
            "schema_version": 1,
            "company": company.model_dump(mode="json"),
            "direction": direction.model_dump(mode="json"),
            "audience": audience.model_dump(mode="json"),
            "brief": brief.model_dump(mode="json"),
            "prompt_set_version": prompt_set_version,
        }
        snapshot_hash = sha256_fingerprint(context)
        existing = self._snapshots.get_snapshot_by_hash(
            self._company_id, snapshot_hash
        )
        if existing is not None:
            return existing
        snapshot = ExecutionSnapshot(
            snapshot_id=self._id_factory(),
            brief_id=brief.brief_id,
            company_id=self._company_id,
            company_profile_version=brief.company_profile_version,
            direction_id=brief.direction_id,
            direction_version=brief.direction_version,
            audience_segment_id=brief.audience_segment_id,
            audience_version=brief.audience_version,
            prompt_set_version=prompt_set_version,
            compiled_context=context,
            snapshot_hash=snapshot_hash,
            created_at=self._clock(),
        )
        self._snapshots.add_snapshot(snapshot)
        return snapshot
