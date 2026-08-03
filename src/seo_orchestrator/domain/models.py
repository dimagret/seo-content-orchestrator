"""Validated, immutable SEO orchestration domain contracts."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Annotated, Any, cast
from urllib.parse import SplitResult, urlsplit, urlunsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from seo_orchestrator.canonical import JsonValue, canonical_json
from seo_orchestrator.errors import CanonicalizationError

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]
Identifier = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9-]{1,63}$")]
Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
NonEmptyStrings = Annotated[tuple[NonEmptyStr, ...], Field(min_length=1)]
Strings = tuple[NonEmptyStr, ...]

_COLLECTION_FIELDS = {
    "value_propositions",
    "proof_points",
    "certifications",
    "case_references",
    "tools_and_process",
    "positive_voice_examples",
    "negative_voice_examples",
    "allowed_claims",
    "forbidden_claims",
    "compliance_requirements",
    "offerings",
    "category_context",
    "direction_value_propositions",
    "direction_proof_points",
    "direction_cases",
    "internal_link_catalog",
    "default_page_structure",
    "buyer_roles",
    "jobs_to_be_done",
    "pains_and_risks",
    "objections",
    "objection_responses",
    "selection_criteria",
    "minimum_expectations",
    "purchase_triggers",
    "decision_participants",
    "preferred_content_formats",
    "page_structure",
    "keywords",
    "lsi_terms",
}


def _normalize_url(value: str) -> str:
    raw = value.strip()
    if not raw:
        raise ValueError("URL must not be empty")
    if "#" in raw:
        raise ValueError("URL fragments are not allowed")
    try:
        parsed = urlsplit(raw)
        port = parsed.port
        hostname = parsed.hostname
    except ValueError as exc:
        raise ValueError("URL has an invalid host or port") from exc
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise ValueError("URL scheme must be http or https")
    if hostname is None:
        raise ValueError("URL must include a host")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("URL credentials are not allowed")
    try:
        ascii_host = hostname.encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise ValueError("URL hostname is invalid") from exc
    if ":" in ascii_host:
        ascii_host = f"[{ascii_host}]"
    include_port = port is not None and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    )
    netloc = f"{ascii_host}:{port}" if include_port else ascii_host
    normalized = SplitResult(scheme, netloc, parsed.path or "/", parsed.query, "")
    return urlunsplit(normalized)


def _freeze_json(value: JsonValue) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: object) -> JsonValue:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return cast(JsonValue, value)


class DomainModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    @field_validator("*", mode="before")
    @classmethod
    def normalize_string_collections(cls, value: object, info: Any) -> object:
        if info.field_name not in _COLLECTION_FIELDS or not isinstance(value, (list, tuple)):
            return value
        result: list[str] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, str):
                return value
            normalized = item.strip()
            if not normalized:
                raise ValueError(f"{info.field_name} entries must not be empty")
            if normalized not in seen:
                seen.add(normalized)
                result.append(normalized)
        return tuple(result)

    @field_validator("created_at", "updated_at", mode="before", check_fields=False)
    @classmethod
    def normalize_datetime(cls, value: object) -> datetime:
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be a timezone-aware datetime")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_timestamp_order(self) -> DomainModel:
        created_at = getattr(self, "created_at", None)
        updated_at = getattr(self, "updated_at", None)
        if (
            isinstance(created_at, datetime)
            and isinstance(updated_at, datetime)
            and updated_at < created_at
        ):
            raise ValueError("updated_at must be greater than or equal to created_at")
        return self


class CompanyProfile(DomainModel):
    company_id: Identifier
    company_profile_id: Identifier
    company_profile_version: PositiveInt
    name: NonEmptyStr
    brand_summary: NonEmptyStr
    products_services_overview: NonEmptyStr
    commercial_model: NonEmptyStr
    pricing_overview: NonEmptyStr
    service_geography: NonEmptyStr
    value_propositions: NonEmptyStrings
    proof_points: Strings
    certifications: Strings
    case_references: Strings
    tools_and_process: NonEmptyStrings
    tone_of_voice: NonEmptyStr
    positive_voice_examples: Strings
    negative_voice_examples: Strings
    reading_level: NonEmptyStr
    allowed_claims: Strings
    forbidden_claims: Strings
    compliance_requirements: Strings
    default_language: NonEmptyStr
    default_locale: NonEmptyStr
    created_at: datetime
    updated_at: datetime


class BusinessDirection(DomainModel):
    direction_id: Identifier
    company_id: Identifier
    company_profile_version: PositiveInt
    direction_version: PositiveInt
    name: NonEmptyStr
    offerings: NonEmptyStrings
    category_context: NonEmptyStrings
    prices_and_tariffs: NonEmptyStr
    direction_value_propositions: NonEmptyStrings
    direction_proof_points: Strings
    direction_cases: Strings
    internal_link_catalog: Strings
    default_page_structure: NonEmptyStrings
    default_language: NonEmptyStr
    default_locale: NonEmptyStr
    allowed_claims: Strings
    forbidden_claims: Strings
    created_at: datetime
    updated_at: datetime


class AudienceSegment(DomainModel):
    audience_segment_id: Identifier
    company_id: Identifier
    direction_id: Identifier
    direction_version: PositiveInt
    audience_version: PositiveInt
    name: NonEmptyStr
    buyer_roles: NonEmptyStrings
    industry: NonEmptyStr
    company_or_customer_size: NonEmptyStr
    geography: NonEmptyStr
    jobs_to_be_done: NonEmptyStrings
    pains_and_risks: NonEmptyStrings
    objections: Strings
    objection_responses: Strings
    selection_criteria: NonEmptyStrings
    minimum_expectations: NonEmptyStrings
    purchase_triggers: NonEmptyStrings
    budget_range: NonEmptyStr
    decision_cycle: NonEmptyStr
    decision_participants: NonEmptyStrings
    preferred_content_formats: NonEmptyStrings
    created_at: datetime
    updated_at: datetime


class SeoBrief(DomainModel):
    brief_id: Identifier
    company_id: Identifier
    company_profile_version: PositiveInt
    direction_id: Identifier
    direction_version: PositiveInt
    audience_segment_id: Identifier
    audience_version: PositiveInt
    page_type: NonEmptyStr
    goal: NonEmptyStr
    target_language: NonEmptyStr
    locale: NonEmptyStr
    page_structure: NonEmptyStrings
    primary_keyword: NonEmptyStr
    keywords: NonEmptyStrings
    lsi_terms: Strings
    competitor_urls: tuple[NonEmptyStr, ...]
    current_page_url: NonEmptyStr | None
    current_page_context: NonEmptyStr | None
    output_sheet_target: NonEmptyStr | None
    created_by: Identifier
    created_at: datetime
    updated_at: datetime

    @field_validator("competitor_urls", mode="before")
    @classmethod
    def normalize_competitor_urls(cls, value: object) -> object:
        if not isinstance(value, (list, tuple)):
            return value
        result: list[str] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, str):
                return value
            normalized = _normalize_url(item)
            if normalized not in seen:
                seen.add(normalized)
                result.append(normalized)
        return tuple(result)

    @field_validator("current_page_url", mode="before")
    @classmethod
    def normalize_current_page_url(cls, value: object) -> object:
        if value is None or not isinstance(value, str):
            return value
        return _normalize_url(value)

    @model_validator(mode="after")
    def validate_current_page_source(self) -> SeoBrief:
        if (self.current_page_url is None) == (self.current_page_context is None):
            raise ValueError("exactly one current-page source must be present")
        return self


class ExecutionSnapshot(DomainModel):
    snapshot_id: Identifier
    brief_id: Identifier
    company_id: Identifier
    company_profile_version: PositiveInt
    direction_id: Identifier
    direction_version: PositiveInt
    audience_segment_id: Identifier
    audience_version: PositiveInt
    prompt_set_version: PositiveInt
    compiled_context: object
    snapshot_hash: Sha256Hex
    created_at: datetime

    @field_validator("compiled_context", mode="before")
    @classmethod
    def validate_and_freeze_context(cls, value: object) -> object:
        try:
            canonical_json(cast(JsonValue, value))
        except CanonicalizationError as exc:
            raise ValueError("compiled_context must contain only canonical JSON values") from exc
        return _freeze_json(cast(JsonValue, value))

    @field_serializer("compiled_context", return_type=Any)
    def serialize_compiled_context(self, value: object) -> Any:
        return _thaw_json(value)

    def thawed_compiled_context(self) -> JsonValue:
        """Return an independent mutable JSON representation for serialization."""
        return _thaw_json(self.compiled_context)
