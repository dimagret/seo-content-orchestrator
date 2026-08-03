from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from seo_orchestrator.domain import (
    AudienceSegment,
    BusinessDirection,
    CompanyProfile,
    ExecutionSnapshot,
    SeoBrief,
)

CREATED = datetime(2026, 8, 3, 10, 0, tzinfo=UTC)
UPDATED = datetime(2026, 8, 3, 11, 0, tzinfo=UTC)


def company_values() -> dict[str, object]:
    return {
        "company_id": "company-1",
        "company_profile_id": "profile-1",
        "company_profile_version": 1,
        "name": " Example Company ",
        "brand_summary": "Trusted specialists",
        "products_services_overview": "SEO services",
        "commercial_model": "Fixed fee",
        "pricing_overview": "From 1000 EUR",
        "service_geography": "Europe",
        "value_propositions": ["Fast", "Measurable", "Fast"],
        "proof_points": [],
        "certifications": [],
        "case_references": [],
        "tools_and_process": ["Research", "Review"],
        "tone_of_voice": "Clear",
        "positive_voice_examples": ["Direct and useful"],
        "negative_voice_examples": [],
        "reading_level": "General business",
        "allowed_claims": ["Evidence-backed"],
        "forbidden_claims": [],
        "compliance_requirements": [],
        "default_language": "en",
        "default_locale": "en-GB",
        "created_at": CREATED,
        "updated_at": UPDATED,
    }


def direction_values() -> dict[str, object]:
    return {
        "direction_id": "direction-1",
        "company_id": "company-1",
        "company_profile_version": 1,
        "direction_version": 2,
        "name": "SEO consulting",
        "offerings": ["Audit"],
        "category_context": ["B2B SEO"],
        "prices_and_tariffs": "From 1000 EUR",
        "direction_value_propositions": ["Senior specialists"],
        "direction_proof_points": [],
        "direction_cases": [],
        "internal_link_catalog": ["https://example.com/services"],
        "default_page_structure": ["Hero", "Benefits"],
        "default_language": "en",
        "default_locale": "en-GB",
        "allowed_claims": [],
        "forbidden_claims": [],
        "created_at": CREATED,
        "updated_at": UPDATED,
    }


def audience_values() -> dict[str, object]:
    return {
        "audience_segment_id": "audience-1",
        "company_id": "company-1",
        "direction_id": "direction-1",
        "direction_version": 2,
        "audience_version": 3,
        "name": "Marketing leaders",
        "buyer_roles": ["CMO"],
        "industry": "Technology",
        "company_or_customer_size": "50-500 employees",
        "geography": "Europe",
        "jobs_to_be_done": ["Grow qualified traffic"],
        "pains_and_risks": ["Unpredictable pipeline"],
        "objections": [],
        "objection_responses": [],
        "selection_criteria": ["Relevant experience"],
        "minimum_expectations": ["Monthly reporting"],
        "purchase_triggers": ["Traffic decline"],
        "budget_range": "1000-5000 EUR",
        "decision_cycle": "One month",
        "decision_participants": ["CMO", "CEO"],
        "preferred_content_formats": ["Guides"],
        "created_at": CREATED,
        "updated_at": UPDATED,
    }


def brief_values() -> dict[str, object]:
    return {
        "brief_id": "brief-1",
        "company_id": "company-1",
        "company_profile_version": 1,
        "direction_id": "direction-1",
        "direction_version": 2,
        "audience_segment_id": "audience-1",
        "audience_version": 3,
        "page_type": "Landing page",
        "goal": "Generate leads",
        "target_language": "en",
        "locale": "en-GB",
        "page_structure": ["Hero", "Proof"],
        "primary_keyword": "seo consulting",
        "keywords": ["seo agency", "seo consulting"],
        "lsi_terms": [],
        "competitor_urls": [
            "HTTPS://EXAMPLE.COM:443",
            "https://example.com/",
            "http://BÜCHER.example:80/path?q=1",
        ],
        "current_page_url": "HTTP://EXAMPLE.COM:80/current?draft=1",
        "current_page_context": None,
        "output_sheet_target": None,
        "created_by": "user-1",
        "created_at": CREATED,
        "updated_at": UPDATED,
    }


def snapshot_values() -> dict[str, object]:
    return {
        "snapshot_id": "snapshot-1",
        "brief_id": "brief-1",
        "company_id": "company-1",
        "company_profile_version": 1,
        "direction_id": "direction-1",
        "direction_version": 2,
        "audience_segment_id": "audience-1",
        "audience_version": 3,
        "prompt_set_version": 4,
        "compiled_context": {"sections": ["hero", {"proof": True}], "count": 2},
        "snapshot_hash": "a" * 64,
        "created_at": CREATED,
    }


def build(model_type: type[object], values: dict[str, object]) -> object:
    return model_type(**values)  # type: ignore[call-arg]


def test_company_profile_strips_text_deduplicates_and_freezes_collections() -> None:
    profile = CompanyProfile(**company_values())  # type: ignore[arg-type]

    assert profile.name == "Example Company"
    assert profile.value_propositions == ("Fast", "Measurable")
    assert isinstance(profile.tools_and_process, tuple)
    with pytest.raises(ValidationError):
        profile.name = "Changed"  # type: ignore[misc]


def test_domain_models_forbid_extra_fields() -> None:
    values = company_values() | {"secret": "must not enter the model"}
    with pytest.raises(ValidationError, match="Extra inputs"):
        CompanyProfile(**values)  # type: ignore[arg-type]


def test_aware_timestamps_are_normalized_to_utc_in_json_dump() -> None:
    values = company_values()
    values["created_at"] = datetime(2026, 8, 3, 12, 0, tzinfo=timezone(timedelta(hours=2)))
    profile = CompanyProfile(**values)  # type: ignore[arg-type]

    assert profile.created_at == CREATED
    assert profile.model_dump(mode="json")["created_at"] == "2026-08-03T10:00:00Z"


@pytest.mark.parametrize("field", ["created_at", "updated_at"])
def test_naive_timestamps_are_rejected(field: str) -> None:
    values = company_values()
    values[field] = CREATED.replace(tzinfo=None)
    with pytest.raises(ValidationError, match="timezone-aware"):
        CompanyProfile(**values)  # type: ignore[arg-type]


def test_updated_at_cannot_precede_created_at() -> None:
    values = company_values()
    values["updated_at"] = CREATED - timedelta(seconds=1)
    with pytest.raises(ValidationError, match="updated_at"):
        CompanyProfile(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize("model_type,values", [(CompanyProfile, company_values()), (BusinessDirection, direction_values()), (AudienceSegment, audience_values()), (SeoBrief, brief_values()), (ExecutionSnapshot, snapshot_values())])
def test_invalid_ids_are_rejected(model_type: type[object], values: dict[str, object]) -> None:
    id_field = next(key for key in values if key.endswith("_id"))
    values[id_field] = "Invalid_ID"
    with pytest.raises(ValidationError):
        build(model_type, values)


@pytest.mark.parametrize("model_type,values", [(CompanyProfile, company_values()), (BusinessDirection, direction_values()), (AudienceSegment, audience_values()), (SeoBrief, brief_values()), (ExecutionSnapshot, snapshot_values())])
def test_versions_must_be_positive(model_type: type[object], values: dict[str, object]) -> None:
    version_field = next(key for key in values if key.endswith("_version"))
    values[version_field] = 0
    with pytest.raises(ValidationError):
        build(model_type, values)


def test_audience_contains_explicit_ownership_and_direction_version() -> None:
    audience = AudienceSegment(**audience_values())  # type: ignore[arg-type]
    assert (audience.company_id, audience.direction_id, audience.direction_version) == (
        "company-1",
        "direction-1",
        2,
    )


@pytest.mark.parametrize("url,context", [(None, None), ("https://example.com", "source")])
def test_brief_requires_exactly_one_current_page_source(
    url: str | None, context: str | None
) -> None:
    values = brief_values() | {"current_page_url": url, "current_page_context": context}
    with pytest.raises(ValidationError, match="exactly one"):
        SeoBrief(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "model_type,values",
    [
        (CompanyProfile, company_values() | {"default_language": " "}),
        (CompanyProfile, company_values() | {"default_locale": ""}),
        (BusinessDirection, direction_values() | {"category_context": []}),
        (SeoBrief, brief_values() | {"page_structure": []}),
        (AudienceSegment, audience_values() | {"buyer_roles": []}),
        (AudienceSegment, audience_values() | {"jobs_to_be_done": []}),
        (CompanyProfile, company_values() | {"value_propositions": ["valid", " "]}),
    ],
)
def test_required_text_and_collection_content_cannot_be_empty(
    model_type: type[object], values: dict[str, object]
) -> None:
    with pytest.raises(ValidationError):
        build(model_type, values)


def test_urls_are_normalized_and_competitors_are_deduplicated() -> None:
    brief = SeoBrief(**brief_values())  # type: ignore[arg-type]

    assert brief.competitor_urls == (
        "https://example.com/",
        "http://xn--bcher-kva.example/path?q=1",
    )
    assert brief.current_page_url == "http://example.com/current?draft=1"


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/path",
        "https:///missing-host",
        "https://user:pass@example.com/",
        "https://example.com/path#fragment",
        "https://example.com:invalid/",
        "https://example.com:99999/",
    ],
)
def test_invalid_urls_are_rejected(url: str) -> None:
    values = brief_values() | {"competitor_urls": [url]}
    with pytest.raises(ValidationError):
        SeoBrief(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "context",
    [{"score": 1.5}, {1: "bad key"}, {"nested": [float("nan")]}],
)
def test_compiled_context_rejects_non_json_values(context: object) -> None:
    values = snapshot_values() | {"compiled_context": context}
    with pytest.raises(ValidationError):
        ExecutionSnapshot(**values)  # type: ignore[arg-type]


def test_compiled_context_is_deeply_immutable_and_can_be_thawed() -> None:
    original = {"sections": ["hero", {"proof": True}], "count": 2}
    snapshot = ExecutionSnapshot(**(snapshot_values() | {"compiled_context": original}))  # type: ignore[arg-type]
    original["sections"] = []

    assert snapshot.thawed_compiled_context() == {
        "sections": ["hero", {"proof": True}],
        "count": 2,
    }
    assert snapshot.model_dump(mode="json")["compiled_context"] == snapshot.thawed_compiled_context()
    with pytest.raises(TypeError):
        snapshot.compiled_context["count"] = 3  # type: ignore[index]
    with pytest.raises(TypeError):
        snapshot.compiled_context["sections"][1]["proof"] = False  # type: ignore[index]


def test_compiled_context_tuples_are_recursively_frozen() -> None:
    context = {"items": ({"mutable": "before"},)}
    snapshot = ExecutionSnapshot(**(snapshot_values() | {"compiled_context": context}))  # type: ignore[arg-type]

    with pytest.raises(TypeError):
        snapshot.compiled_context["items"][0]["mutable"] = "after"  # type: ignore[index]
    assert snapshot.thawed_compiled_context() == {"items": [{"mutable": "before"}]}


@pytest.mark.parametrize("snapshot_hash", ["A" * 64, "a" * 63, "g" * 64])
def test_snapshot_hash_must_be_lowercase_sha256(snapshot_hash: str) -> None:
    values = snapshot_values() | {"snapshot_hash": snapshot_hash}
    with pytest.raises(ValidationError):
        ExecutionSnapshot(**values)  # type: ignore[arg-type]
