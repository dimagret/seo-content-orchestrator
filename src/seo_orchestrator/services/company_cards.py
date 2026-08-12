"""Versioned, company-scoped company card operations."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime

from seo_orchestrator.db.repositories import CompanyRepository
from seo_orchestrator.domain import AudienceSegment, BusinessDirection, CompanyProfile
from seo_orchestrator.domain.models import (
    DomainModel,
    Identifier,
    NonEmptyStr,
    NonEmptyStrings,
    StrictPositiveInt,
)
from seo_orchestrator.errors import VersionConflict


class CompanyProfileData(DomainModel):
    """Complete replacement content for one company profile version."""

    name: NonEmptyStr
    brand_summary: NonEmptyStr
    products_services_overview: NonEmptyStr
    commercial_model: NonEmptyStr
    pricing_overview: NonEmptyStr
    service_geography: NonEmptyStr
    value_propositions: NonEmptyStrings
    proof_points: NonEmptyStrings
    certifications: NonEmptyStrings
    case_references: NonEmptyStrings
    tools_and_process: NonEmptyStrings
    tone_of_voice: NonEmptyStr
    positive_voice_examples: NonEmptyStrings
    negative_voice_examples: NonEmptyStrings
    reading_level: NonEmptyStr
    allowed_claims: NonEmptyStrings
    forbidden_claims: NonEmptyStrings
    compliance_requirements: NonEmptyStrings
    default_language: NonEmptyStr
    default_locale: NonEmptyStr


class CreateCompany(DomainModel):
    company_id: Identifier
    company_profile_id: Identifier
    actor_id: Identifier
    replacement: CompanyProfileData


class ReviseCompany(DomainModel):
    company_id: Identifier
    actor_id: Identifier
    expected_current_version: StrictPositiveInt
    replacement: CompanyProfileData


class DirectionData(DomainModel):
    """Complete replacement content for one business direction version."""

    name: NonEmptyStr
    offerings: NonEmptyStrings
    category_context: NonEmptyStr
    prices_and_tariffs: NonEmptyStr
    direction_value_propositions: NonEmptyStrings
    direction_proof_points: NonEmptyStrings
    direction_cases: NonEmptyStrings
    internal_link_catalog: NonEmptyStrings
    default_page_structure: NonEmptyStrings
    default_language: NonEmptyStr
    default_locale: NonEmptyStr
    allowed_claims: NonEmptyStrings
    forbidden_claims: NonEmptyStrings


class CreateDirection(DomainModel):
    company_id: Identifier
    company_profile_version: StrictPositiveInt
    direction_id: Identifier
    actor_id: Identifier
    replacement: DirectionData


class ReviseDirection(DomainModel):
    company_id: Identifier
    company_profile_version: StrictPositiveInt
    direction_id: Identifier
    actor_id: Identifier
    expected_current_version: StrictPositiveInt
    replacement: DirectionData


class AudienceData(DomainModel):
    """Complete content for one audience segment version."""

    name: NonEmptyStr
    buyer_roles: NonEmptyStrings
    industry: NonEmptyStr
    company_or_customer_size: NonEmptyStr
    geography: NonEmptyStr
    jobs_to_be_done: NonEmptyStrings
    pains_and_risks: NonEmptyStrings
    objections: NonEmptyStrings
    objection_responses: NonEmptyStrings
    selection_criteria: NonEmptyStrings
    minimum_expectations: NonEmptyStrings
    purchase_triggers: NonEmptyStrings
    budget_range: NonEmptyStr
    decision_cycle: NonEmptyStr
    decision_participants: NonEmptyStrings
    preferred_content_formats: NonEmptyStrings


class CreateAudience(DomainModel):
    company_id: Identifier
    direction_id: Identifier
    direction_version: StrictPositiveInt
    audience_segment_id: Identifier
    actor_id: Identifier
    replacement: AudienceData


class CompanyCardService:
    """Create immutable company-card versions without committing caller transactions."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._repository = CompanyRepository(conn)
        self._clock = clock or (lambda: datetime.now(UTC))

    def create_company(self, command: CreateCompany) -> CompanyProfile:
        now = self._clock()
        profile = CompanyProfile(
            company_id=command.company_id,
            company_profile_id=command.company_profile_id,
            company_profile_version=1,
            **command.replacement.model_dump(),
            created_at=now,
            updated_at=now,
        )
        self._repository.add_company(command.company_id, now, now)
        self._repository.add_profile(profile)
        return profile

    def revise_company(self, command: ReviseCompany) -> CompanyProfile:
        self._repository.require_active(command.company_id)
        current = self._repository.get_current_profile(command.company_id)
        if current.company_profile_version != command.expected_current_version:
            raise VersionConflict
        now = self._clock()
        profile = CompanyProfile(
            company_id=command.company_id,
            company_profile_id=current.company_profile_id,
            company_profile_version=current.company_profile_version + 1,
            **command.replacement.model_dump(),
            created_at=current.created_at,
            updated_at=now,
        )
        self._repository.add_profile(profile)
        return profile

    def get_company_profile(self, company_id: str, version: int) -> CompanyProfile:
        """Retrieve one exact historical profile within explicit company scope."""
        return self._repository.get_profile(company_id, version)

    def list_current_company_profiles(self) -> tuple[CompanyProfile, ...]:
        """Return current profiles for active companies without exposing persistence."""
        return self._repository.list_current_profiles()

    def create_direction(self, command: CreateDirection) -> BusinessDirection:
        self._repository.require_active(command.company_id)
        self._repository.get_profile(command.company_id, command.company_profile_version)
        now = self._clock()
        direction = BusinessDirection(
            company_id=command.company_id,
            company_profile_version=command.company_profile_version,
            direction_id=command.direction_id,
            direction_version=1,
            **command.replacement.model_dump(),
            created_at=now,
            updated_at=now,
        )
        self._repository.add_direction(direction)
        return direction

    def revise_direction(self, command: ReviseDirection) -> BusinessDirection:
        self._repository.require_active(command.company_id)
        self._repository.get_profile(command.company_id, command.company_profile_version)
        current = self._repository.get_current_direction(
            command.company_id, command.direction_id
        )
        if current.direction_version != command.expected_current_version:
            raise VersionConflict
        now = self._clock()
        direction = BusinessDirection(
            company_id=command.company_id,
            company_profile_version=command.company_profile_version,
            direction_id=command.direction_id,
            direction_version=current.direction_version + 1,
            **command.replacement.model_dump(),
            created_at=current.created_at,
            updated_at=now,
        )
        self._repository.add_direction(direction)
        return direction

    def get_direction(
        self, company_id: str, direction_id: str, version: int
    ) -> BusinessDirection:
        """Retrieve one exact historical direction within explicit company scope."""
        return self._repository.get_direction(company_id, direction_id, version)

    def create_audience(self, command: CreateAudience) -> AudienceSegment:
        self._repository.require_active(command.company_id)
        self._repository.get_direction(
            command.company_id, command.direction_id, command.direction_version
        )
        now = self._clock()
        audience = AudienceSegment(
            company_id=command.company_id,
            direction_id=command.direction_id,
            direction_version=command.direction_version,
            audience_segment_id=command.audience_segment_id,
            audience_version=1,
            **command.replacement.model_dump(),
            created_at=now,
            updated_at=now,
        )
        self._repository.add_audience(audience)
        return audience

    def get_audience(
        self,
        company_id: str,
        direction_id: str,
        audience_segment_id: str,
        version: int,
    ) -> AudienceSegment:
        """Retrieve one exact historical audience within explicit company scope."""
        return self._repository.get_audience(
            company_id, direction_id, audience_segment_id, version
        )

    def archive_company(self, company_id: str, actor_id: str) -> None:
        """Archive a company while retaining all immutable historical rows."""
        del actor_id
        self._repository.archive_company(company_id, self._clock())
