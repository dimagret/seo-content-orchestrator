"""Durable, resumable SEO brief drafts."""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, field_validator

from seo_orchestrator.canonical import canonical_json
from seo_orchestrator.db.repositories import BriefDraftRecord, BriefRepository, CompanyRepository
from seo_orchestrator.domain.models import DomainModel, Identifier, SeoBrief, StrictPositiveInt
from seo_orchestrator.errors import NotFound


class SeoBriefDraft(BaseModel):
    """Persisted wizard state; incomplete values remain explicit and resumable."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    brief_id: Identifier
    company_id: Identifier
    company_profile_version: StrictPositiveInt
    direction_id: Identifier | None = None
    direction_version: StrictPositiveInt | None = None
    audience_segment_id: Identifier | None = None
    audience_version: StrictPositiveInt | None = None
    page_type: str | None = None
    goal: str | None = None
    target_language: str | None = None
    locale: str | None = None
    page_structure: tuple[str, ...] | None = None
    category_context: str | None = None
    primary_keyword: str | None = None
    keywords: tuple[str, ...] | None = None
    lsi_terms: tuple[str, ...] | None = None
    competitor_urls: tuple[str, ...] | None = None
    current_page_url: str | None = None
    current_page_context: str | None = None
    output_sheet_target: str | None = None
    created_by: Identifier
    created_at: datetime
    updated_at: datetime
    status: str = "draft"

    @field_validator("created_at", "updated_at", mode="before")
    @classmethod
    def normalize_datetime(cls, value: object) -> datetime:
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be a timezone-aware datetime")
        return value.astimezone(UTC)


class UpdateBrief(DomainModel):
    """One wizard update scoped to the draft's current company and actor."""

    brief_id: Identifier
    actor_id: Identifier
    company_id: Identifier
    replacement_company_id: Identifier | None = None
    direction_id: Identifier | None = None
    direction_version: StrictPositiveInt | None = None
    audience_segment_id: Identifier | None = None
    audience_version: StrictPositiveInt | None = None
    page_type: str | None = None
    goal: str | None = None
    target_language: str | None = None
    locale: str | None = None
    page_structure: tuple[str, ...] | None = None
    category_context: str | None = None
    primary_keyword: str | None = None
    keywords: tuple[str, ...] | None = None
    lsi_terms: tuple[str, ...] | None = None
    competitor_urls: tuple[str, ...] | None = None
    current_page_url: str | None = None
    current_page_context: str | None = None
    output_sheet_target: str | None = None


class ValidatedBrief(SeoBrief):
    """A complete brief whose exact version ownership has been checked."""


class BriefService:
    """Persist wizard progress while leaving transaction ownership to the caller."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        clock: Callable[[], datetime] | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._companies = CompanyRepository(conn)
        self._briefs = BriefRepository(conn)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._id_factory = id_factory or (lambda: f"brief-{uuid.uuid4().hex}")

    def _record(self, draft: SeoBriefDraft) -> BriefDraftRecord:
        return BriefDraftRecord(
            brief_id=draft.brief_id,
            company_id=draft.company_id,
            company_profile_version=draft.company_profile_version,
            direction_id=draft.direction_id,
            direction_version=draft.direction_version,
            audience_segment_id=draft.audience_segment_id,
            audience_version=draft.audience_version,
            brief_json=canonical_json(draft.model_dump(mode="json")),
            status=draft.status,
            created_by=draft.created_by,
            created_at=draft.created_at.isoformat(),
            updated_at=draft.updated_at.isoformat(),
        )

    def start_brief(self, actor_id: str, company_id: str) -> SeoBriefDraft:
        self._companies.require_active(company_id)
        profile = self._companies.get_current_profile(company_id)
        now = self._clock()
        draft = SeoBriefDraft(
            brief_id=self._id_factory(),
            company_id=company_id,
            company_profile_version=profile.company_profile_version,
            created_by=actor_id,
            created_at=now,
            updated_at=now,
        )
        self._briefs.add_draft(self._record(draft))
        return draft

    def get_brief(self, company_id: str, brief_id: str, actor_id: str) -> SeoBriefDraft:
        record = self._briefs.get_draft(company_id, brief_id, actor_id)
        values = json.loads(record.brief_json)
        values["created_at"] = datetime.fromisoformat(values["created_at"])
        values["updated_at"] = datetime.fromisoformat(values["updated_at"])
        return SeoBriefDraft.model_validate(values)

    def update_brief(self, command: UpdateBrief) -> SeoBriefDraft:
        current = self.get_brief(command.company_id, command.brief_id, command.actor_id)
        fields = command.model_fields_set
        for identifier, version in (
            ("direction_id", "direction_version"),
            ("audience_segment_id", "audience_version"),
        ):
            supplied = identifier in fields, version in fields
            pair_values = getattr(command, identifier), getattr(command, version)
            if supplied[0] != supplied[1] or (
                (pair_values[0] is None) != (pair_values[1] is None)
            ):
                raise ValueError(f"{identifier} and {version} must be supplied together")

        values = current.model_dump(mode="python")
        selected_company = command.replacement_company_id or current.company_id
        company_changed = selected_company != current.company_id
        if company_changed:
            self._companies.require_active(selected_company)
            profile = self._companies.get_current_profile(selected_company)
            values.update(
                company_id=selected_company,
                company_profile_version=profile.company_profile_version,
                direction_id=None,
                direction_version=None,
                audience_segment_id=None,
                audience_version=None,
                page_structure=None,
                category_context=None,
            )

        fields = command.model_fields_set
        if not company_changed and (
            "direction_id" in fields or "direction_version" in fields
        ):
            if command.direction_id is None or command.direction_version is None:
                values.update(
                    direction_id=None,
                    direction_version=None,
                    audience_segment_id=None,
                    audience_version=None,
                    page_structure=None,
                    category_context=None,
                )
            else:
                direction = self._companies.get_direction(
                    selected_company, command.direction_id, command.direction_version
                )
                if direction.company_profile_version != values["company_profile_version"]:
                    raise NotFound
                if (
                    command.direction_id != values["direction_id"]
                    or command.direction_version != values["direction_version"]
                ):
                    values.update(
                        audience_segment_id=None,
                        audience_version=None,
                        page_structure=None,
                        category_context=None,
                    )
                values.update(
                    direction_id=command.direction_id,
                    direction_version=command.direction_version,
                )

        if not company_changed and (
            "audience_segment_id" in fields or "audience_version" in fields
        ):
            if command.audience_segment_id is None or command.audience_version is None:
                values.update(audience_segment_id=None, audience_version=None)
            elif values["direction_id"] is None or values["direction_version"] is None:
                raise NotFound
            else:
                audience = self._companies.get_audience(
                    selected_company,
                    values["direction_id"],
                    command.audience_segment_id,
                    command.audience_version,
                )
                if audience.direction_version != values["direction_version"]:
                    raise NotFound
                values.update(
                    audience_segment_id=command.audience_segment_id,
                    audience_version=command.audience_version,
                )

        for field_name in (
            "page_type",
            "goal",
            "target_language",
            "locale",
            "page_structure",
            "category_context",
            "primary_keyword",
            "keywords",
            "lsi_terms",
            "competitor_urls",
            "current_page_url",
            "current_page_context",
            "output_sheet_target",
        ):
            if field_name in fields and not (
                company_changed and field_name in {"page_structure", "category_context"}
            ):
                values[field_name] = getattr(command, field_name)
        values["updated_at"] = self._clock()
        values["status"] = "draft"
        updated = SeoBriefDraft.model_validate(values)
        self._briefs.update_draft(command.company_id, self._record(updated))
        return updated

    def validate_brief(
        self, company_id: str, brief_id: str, actor_id: str
    ) -> ValidatedBrief:
        draft = self.get_brief(company_id, brief_id, actor_id)
        if (
            draft.direction_id is None
            or draft.direction_version is None
            or draft.audience_segment_id is None
            or draft.audience_version is None
        ):
            raise ValueError("brief is incomplete")
        profile = self._companies.get_profile(
            draft.company_id, draft.company_profile_version
        )
        direction = self._companies.get_direction(
            draft.company_id, draft.direction_id, draft.direction_version
        )
        audience = self._companies.get_audience(
            draft.company_id,
            draft.direction_id,
            draft.audience_segment_id,
            draft.audience_version,
        )
        if (
            direction.company_profile_version != profile.company_profile_version
            or audience.direction_version != direction.direction_version
        ):
            raise NotFound
        values = draft.model_dump(mode="python")
        values.pop("category_context")
        values.pop("status")
        validated = ValidatedBrief.model_validate(values)
        saved = SeoBriefDraft.model_validate(
            {
                **validated.model_dump(mode="python"),
                "category_context": draft.category_context,
                "status": "validated",
            }
        )
        self._briefs.update_draft(draft.company_id, self._record(saved))
        return validated
