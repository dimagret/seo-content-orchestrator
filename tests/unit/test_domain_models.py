from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from seo_orchestrator.canonical import (
    MAX_CANONICAL_BYTES,
    MAX_CANONICAL_DEPTH,
    MAX_CANONICAL_NODES,
    MAX_SAFE_INTEGER,
    canonical_json,
)
from seo_orchestrator.domain import (
    AudienceSegment,
    BusinessDirection,
    CompanyProfile,
    ExecutionSnapshot,
    SeoBrief,
)
from seo_orchestrator.domain import models as domain_models

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
        "proof_points": ["Published results"],
        "certifications": ["ISO 9001"],
        "case_references": ["Case study A"],
        "tools_and_process": ["Research", "Review"],
        "tone_of_voice": "Clear",
        "positive_voice_examples": ["Direct and useful"],
        "negative_voice_examples": ["Vague hype"],
        "reading_level": "General business",
        "allowed_claims": ["Evidence-backed"],
        "forbidden_claims": ["Guaranteed rankings"],
        "compliance_requirements": ["Cite evidence"],
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
        "category_context": "B2B SEO",
        "prices_and_tariffs": "From 1000 EUR",
        "direction_value_propositions": ["Senior specialists"],
        "direction_proof_points": ["20% traffic growth"],
        "direction_cases": ["SaaS case"],
        "internal_link_catalog": ["https://example.com/services"],
        "default_page_structure": ["Hero", "Benefits"],
        "default_language": "en",
        "default_locale": "en-GB",
        "allowed_claims": ["Evidence-backed growth"],
        "forbidden_claims": ["Guaranteed results"],
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
        "objections": ["Too expensive"],
        "objection_responses": ["Show ROI model"],
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
        "lsi_terms": ["organic search consulting"],
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


MODEL_VERSION_FIELDS = [
    (CompanyProfile, company_values, "company_profile_version"),
    (BusinessDirection, direction_values, "company_profile_version"),
    (BusinessDirection, direction_values, "direction_version"),
    (AudienceSegment, audience_values, "direction_version"),
    (AudienceSegment, audience_values, "audience_version"),
    (SeoBrief, brief_values, "company_profile_version"),
    (SeoBrief, brief_values, "direction_version"),
    (SeoBrief, brief_values, "audience_version"),
    (ExecutionSnapshot, snapshot_values, "company_profile_version"),
    (ExecutionSnapshot, snapshot_values, "direction_version"),
    (ExecutionSnapshot, snapshot_values, "audience_version"),
    (ExecutionSnapshot, snapshot_values, "prompt_set_version"),
]

MODEL_IDENTIFIER_FIELDS = [
    (CompanyProfile, company_values, "company_id"),
    (CompanyProfile, company_values, "company_profile_id"),
    (BusinessDirection, direction_values, "direction_id"),
    (BusinessDirection, direction_values, "company_id"),
    (AudienceSegment, audience_values, "audience_segment_id"),
    (AudienceSegment, audience_values, "company_id"),
    (AudienceSegment, audience_values, "direction_id"),
    (SeoBrief, brief_values, "brief_id"),
    (SeoBrief, brief_values, "company_id"),
    (SeoBrief, brief_values, "direction_id"),
    (SeoBrief, brief_values, "audience_segment_id"),
    (SeoBrief, brief_values, "created_by"),
    (ExecutionSnapshot, snapshot_values, "snapshot_id"),
    (ExecutionSnapshot, snapshot_values, "brief_id"),
    (ExecutionSnapshot, snapshot_values, "company_id"),
    (ExecutionSnapshot, snapshot_values, "direction_id"),
    (ExecutionSnapshot, snapshot_values, "audience_segment_id"),
]


MODEL_SCALAR_TEXT_FIELDS = [
    *[
        (CompanyProfile, company_values, field)
        for field in (
            "name",
            "brand_summary",
            "products_services_overview",
            "commercial_model",
            "pricing_overview",
            "service_geography",
            "tone_of_voice",
            "reading_level",
            "default_language",
            "default_locale",
        )
    ],
    *[
        (BusinessDirection, direction_values, field)
        for field in (
            "name",
            "category_context",
            "prices_and_tariffs",
            "default_language",
            "default_locale",
        )
    ],
    *[
        (AudienceSegment, audience_values, field)
        for field in (
            "name",
            "industry",
            "company_or_customer_size",
            "geography",
            "budget_range",
            "decision_cycle",
        )
    ],
    *[
        (SeoBrief, brief_values, field)
        for field in (
            "page_type",
            "goal",
            "target_language",
            "locale",
            "primary_keyword",
            "current_page_context",
            "output_sheet_target",
        )
    ],
]


MODEL_COLLECTION_FIELDS = [
    *[
        (CompanyProfile, company_values, field)
        for field in (
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
        )
    ],
    *[
        (BusinessDirection, direction_values, field)
        for field in (
            "offerings",
            "direction_value_propositions",
            "direction_proof_points",
            "direction_cases",
            "internal_link_catalog",
            "default_page_structure",
            "allowed_claims",
            "forbidden_claims",
        )
    ],
    *[
        (AudienceSegment, audience_values, field)
        for field in (
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
        )
    ],
    *[
        (SeoBrief, brief_values, field)
        for field in ("page_structure", "keywords", "lsi_terms", "competitor_urls")
    ],
]


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


@pytest.mark.parametrize("model_type,values_factory,id_field", MODEL_IDENTIFIER_FIELDS)
def test_every_identifier_field_rejects_regex_valid_bytes(
    model_type: type[object], values_factory: object, id_field: str
) -> None:
    values = values_factory()  # type: ignore[operator]
    values[id_field] = b"company-1"

    with pytest.raises(ValidationError):
        build(model_type, values)


@pytest.mark.parametrize(
    "invalid_identifier",
    [
        pytest.param(bytearray(b"company-1"), id="bytearray"),
        pytest.param(123, id="int"),
        pytest.param(True, id="bool"),
        pytest.param(object(), id="object"),
    ],
)
def test_identifier_rejects_representative_non_string_values(
    invalid_identifier: object,
) -> None:
    values = company_values()
    values["company_id"] = invalid_identifier

    with pytest.raises(ValidationError):
        CompanyProfile(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "invalid_text",
    [
        pytest.param(b"valid text", id="bytes"),
        pytest.param(bytearray(b"valid text"), id="bytearray"),
    ],
)
@pytest.mark.parametrize("model_type,values_factory,text_field", MODEL_SCALAR_TEXT_FIELDS)
def test_every_scalar_text_field_rejects_bytes_like_input(
    model_type: type[object],
    values_factory: object,
    text_field: str,
    invalid_text: object,
) -> None:
    values = values_factory()  # type: ignore[operator]
    if text_field == "current_page_context":
        values["current_page_url"] = None
    values[text_field] = invalid_text

    with pytest.raises(ValidationError):
        build(model_type, values)


@pytest.mark.parametrize("invalid_version", [True, "1", 1.0, 0, -1])
@pytest.mark.parametrize("model_type,values_factory,version_field", MODEL_VERSION_FIELDS)
def test_every_version_field_requires_a_strict_positive_integer(
    model_type: type[object],
    values_factory: object,
    version_field: str,
    invalid_version: object,
) -> None:
    values = values_factory()  # type: ignore[operator]
    values[version_field] = invalid_version
    with pytest.raises(ValidationError):
        build(model_type, values)


@pytest.mark.parametrize("model_type,values_factory,version_field", MODEL_VERSION_FIELDS)
def test_every_version_field_accepts_positive_integers(
    model_type: type[object], values_factory: object, version_field: str
) -> None:
    values = values_factory()  # type: ignore[operator]
    values[version_field] = 1

    model = build(model_type, values)

    assert getattr(model, version_field) == 1


def test_audience_contains_explicit_ownership_and_direction_version() -> None:
    audience = AudienceSegment(**audience_values())  # type: ignore[arg-type]
    assert (audience.company_id, audience.direction_id, audience.direction_version) == (
        "company-1",
        "direction-1",
        2,
    )


def test_direction_category_context_is_a_non_empty_string() -> None:
    direction = BusinessDirection(**direction_values())  # type: ignore[arg-type]
    assert direction.category_context == "B2B SEO"

    with pytest.raises(ValidationError):
        BusinessDirection(**(direction_values() | {"category_context": " "}))  # type: ignore[arg-type]


@pytest.mark.parametrize("url,context", [(None, None), ("https://example.com", "source")])
def test_brief_requires_exactly_one_current_page_source(
    url: str | None, context: str | None
) -> None:
    values = brief_values() | {"current_page_url": url, "current_page_context": context}
    with pytest.raises(ValidationError, match="exactly one"):
        SeoBrief(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "present_field,present_value,omitted_field",
    [
        ("current_page_url", "https://example.com/current", "current_page_context"),
        ("current_page_context", "Existing page copy", "current_page_url"),
    ],
)
def test_brief_allows_omitting_unused_current_page_alternative(
    present_field: str, present_value: str, omitted_field: str
) -> None:
    values = brief_values()
    values[present_field] = present_value
    values.pop(omitted_field)
    brief = SeoBrief(**values)  # type: ignore[arg-type]
    assert getattr(brief, present_field) is not None
    assert getattr(brief, omitted_field) is None


def test_brief_allows_omitting_output_sheet_target() -> None:
    values = brief_values()
    values.pop("output_sheet_target")
    assert SeoBrief(**values).output_sheet_target is None  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "model_type,values",
    [
        (CompanyProfile, company_values() | {"default_language": " "}),
        (CompanyProfile, company_values() | {"default_locale": ""}),
        (BusinessDirection, direction_values() | {"category_context": ""}),
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


@pytest.mark.parametrize(
    "model_type,values,field",
    [
        *[
            (CompanyProfile, company_values(), field)
            for field in (
                "value_propositions", "proof_points", "certifications", "case_references",
                "tools_and_process", "positive_voice_examples", "negative_voice_examples",
                "allowed_claims", "forbidden_claims", "compliance_requirements",
            )
        ],
        *[
            (BusinessDirection, direction_values(), field)
            for field in (
                "offerings", "direction_value_propositions", "direction_proof_points",
                "direction_cases", "internal_link_catalog", "default_page_structure",
                "allowed_claims", "forbidden_claims",
            )
        ],
        *[
            (AudienceSegment, audience_values(), field)
            for field in (
                "buyer_roles", "jobs_to_be_done", "pains_and_risks", "objections",
                "objection_responses", "selection_criteria", "minimum_expectations",
                "purchase_triggers", "decision_participants", "preferred_content_formats",
            )
        ],
        *[
            (SeoBrief, brief_values(), field)
            for field in ("page_structure", "keywords", "lsi_terms", "competitor_urls")
        ],
    ],
)
def test_every_required_collection_rejects_empty(
    model_type: type[object], values: dict[str, object], field: str
) -> None:
    with pytest.raises(ValidationError):
        build(model_type, values | {field: []})


@pytest.mark.parametrize(
    "invalid_collection",
    [
        pytest.param(lambda: (item for item in ["first", "second"]), id="generator"),
        pytest.param(lambda: {"first", "second"}, id="set"),
        pytest.param(lambda: frozenset({"first", "second"}), id="frozenset"),
        pytest.param(lambda: {"first": "second"}, id="dict"),
        pytest.param(lambda: "first", id="scalar"),
    ],
)
@pytest.mark.parametrize("model_type,values_factory,field", MODEL_COLLECTION_FIELDS)
def test_every_string_collection_accepts_only_list_or_tuple_input(
    model_type: type[object],
    values_factory: object,
    field: str,
    invalid_collection: object,
) -> None:
    values = values_factory()  # type: ignore[operator]
    values[field] = invalid_collection()  # type: ignore[operator]

    with pytest.raises(ValidationError):
        build(model_type, values)


@pytest.mark.parametrize(
    "invalid_entry",
    [
        pytest.param(b"entry", id="bytes"),
        pytest.param(bytearray(b"entry"), id="bytearray"),
        pytest.param(123, id="int"),
        pytest.param(True, id="bool"),
        pytest.param(object(), id="object"),
    ],
)
@pytest.mark.parametrize("model_type,values_factory,field", MODEL_COLLECTION_FIELDS)
def test_every_string_collection_rejects_non_string_entries(
    model_type: type[object],
    values_factory: object,
    field: str,
    invalid_entry: object,
) -> None:
    values = values_factory()  # type: ignore[operator]
    values[field] = [invalid_entry, "valid"]

    with pytest.raises(ValidationError, match="entries must be strings"):
        build(model_type, values)


@pytest.mark.parametrize("input_type", [list, tuple])
def test_competitor_urls_normalize_deduplicate_and_freeze_allowed_input_types(
    input_type: type[list[str]] | type[tuple[str, ...]],
) -> None:
    values = brief_values()
    values["competitor_urls"] = input_type(
        ["HTTPS://EXAMPLE.COM:443", "https://example.com/"]
    )

    brief = SeoBrief(**values)  # type: ignore[arg-type]

    assert brief.competitor_urls == ("https://example.com/",)
    assert isinstance(brief.competitor_urls, tuple)


def test_urls_are_normalized_and_competitors_are_deduplicated() -> None:
    brief = SeoBrief(**brief_values())  # type: ignore[arg-type]

    assert brief.competitor_urls == (
        "https://example.com/",
        "http://xn--bcher-kva.example/path?q=1",
    )
    assert brief.current_page_url == "http://example.com/current?draft=1"


@pytest.mark.parametrize(
    "invalid_url",
    [
        pytest.param(b"ftp://example.com/", id="bytes"),
        pytest.param(bytearray(b"https://example.com/"), id="bytearray"),
        pytest.param(123, id="int"),
        pytest.param(True, id="bool"),
        pytest.param(object(), id="object"),
    ],
)
def test_current_page_url_rejects_non_string_input_before_url_validation(
    invalid_url: object,
) -> None:
    values = brief_values() | {"current_page_url": invalid_url}

    with pytest.raises(ValidationError, match="current_page_url must be a string"):
        SeoBrief(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "invalid_url",
    [b"https://example.com/", bytearray(b"https://example.com/"), 123, True, object()],
)
def test_private_url_normalizer_rejects_non_string_input(invalid_url: object) -> None:
    with pytest.raises(ValueError, match="URL must be a string"):
        domain_models._normalize_url(invalid_url)


def test_url_hosts_normalize_idna_ipv4_and_ipv6() -> None:
    values = brief_values() | {
        "competitor_urls": [
            "https://BÜCHER.example/path",
            "http://192.168.1.1:80/path",
            "HTTPS://[2001:0DB8:0:0:0:0:0:1]:443/path",
        ]
    }
    brief = SeoBrief(**values)  # type: ignore[arg-type]
    assert brief.competitor_urls == (
        "https://xn--bcher-kva.example/path",
        "http://192.168.1.1/path",
        "https://[2001:db8::1]/path",
    )


def test_url_hosts_use_uts46_idna2008_authorities() -> None:
    values = brief_values() | {
        "competitor_urls": ["https://faß.de/path", "https://οδός.gr/path"]
    }

    brief = SeoBrief(**values)  # type: ignore[arg-type]

    assert brief.competitor_urls == (
        "https://xn--fa-hia.de/path",
        "https://xn--pxavk3b.gr/path",
    )


@pytest.mark.parametrize("hostname", [f"{chr(0xD800)}.example", f"{chr(0xDC00)}.example"])
def test_invalid_idna_hosts_raise_controlled_validation_errors(hostname: str) -> None:
    values = brief_values() | {"competitor_urls": [f"https://{hostname}/"]}

    with pytest.raises(ValidationError, match="hostname is invalid"):
        SeoBrief(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/path",
        "https:///missing-host",
        "https://user:pass@example.com/",
        "https://example.com/path#fragment",
        "https://example.com:invalid/",
        "https://example.com:99999/",
        "https://example.com./",
        "https://two..dots.example/",
        "https://under_score.example/",
        "https://-leading.example/",
        "https://trailing-.example/",
        "https://host%20name.example/",
        "https://host name.example/",
        "https://host\nname.example/",
        "https://[2001:db8:::1]/",
        "https://999.999.999.999/",
        f"https://{'a' * 64}.example/",
        f"https://{'a.' * 126}aa/",
    ],
)
def test_invalid_urls_are_rejected(url: str) -> None:
    values = brief_values() | {"competitor_urls": [url]}
    with pytest.raises(ValidationError):
        SeoBrief(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "url",
    [
        "\nhttps://example.com/",
        "\thttps://example.com/",
        " https://example.com/",
        "https://host name.example/",
        "https://example.com/path with-space",
        "https://example.com/path?query=with space",
        "https://host\nname.example/",
        "https://example.com/path\nsegment",
        "https://example.com/path?query=with\nnewline",
        "https://example.com/path?query=with\x00nul",
        "https://example.com/path\x7fsegment",
        "https://example.com/path?query=with\x1fcontrol",
    ],
)
def test_urls_reject_raw_whitespace_and_controls_before_parsing(url: str) -> None:
    values = brief_values() | {"competitor_urls": [url]}

    with pytest.raises(ValidationError):
        SeoBrief(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize("url", ["https://example.com:/", "http://[::1]:/"])
def test_urls_reject_lexically_explicit_empty_ports(url: str) -> None:
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
    dumped_context = snapshot.model_dump(mode="json")["compiled_context"]
    thawed_context = snapshot.thawed_compiled_context()
    assert dumped_context == thawed_context
    expected_canonical = b'{"count":2,"sections":["hero",{"proof":true}]}'
    assert canonical_json(dumped_context) == expected_canonical
    assert canonical_json(thawed_context) == expected_canonical
    with pytest.raises(TypeError):
        snapshot.compiled_context["count"] = 3  # type: ignore[index]
    with pytest.raises(TypeError):
        snapshot.compiled_context["sections"][1]["proof"] = False  # type: ignore[index]


@pytest.mark.parametrize(
    "context",
    [
        ("top-level",),
        [("nested-in-list",)],
        {"nested": ("nested-in-dict",)},
        {"nested": [{"deeply_nested": (1, 2)}]},
    ],
    ids=["top-level", "list", "dict", "deeply-nested"],
)
def test_compiled_context_rejects_tuples_anywhere(context: object) -> None:
    values = snapshot_values() | {"compiled_context": context}

    with pytest.raises(ValidationError):
        ExecutionSnapshot(**values)  # type: ignore[arg-type]


def _nested_context(container_depth: int) -> list[object]:
    root: list[object] = []
    current = root
    for _ in range(container_depth):
        child: list[object] = []
        current.append(child)
        current = child
    return root


def _invalid_compiled_contexts() -> list[object]:
    cyclic_list: list[object] = []
    cyclic_list.append(cyclic_list)
    cyclic_dict: dict[str, object] = {}
    cyclic_dict["self"] = cyclic_dict
    return [
        "\ud800",
        "\udc00",
        {"\ud800": "value"},
        cyclic_list,
        cyclic_dict,
        _nested_context(MAX_CANONICAL_DEPTH + 1),
        [None] * MAX_CANONICAL_NODES,
        "a" * (MAX_CANONICAL_BYTES + 1),
        MAX_SAFE_INTEGER + 1,
        -MAX_SAFE_INTEGER - 1,
    ]


@pytest.mark.parametrize(
    "context",
    _invalid_compiled_contexts(),
    ids=[
        "high-surrogate",
        "low-surrogate",
        "surrogate-key",
        "list-cycle",
        "dict-cycle",
        "depth",
        "nodes",
        "bytes",
        "positive-integer",
        "negative-integer",
    ],
)
def test_compiled_context_converts_all_canonical_failures_to_validation_errors(
    context: object,
) -> None:
    values = snapshot_values() | {"compiled_context": context}

    with pytest.raises(ValidationError, match="compiled_context"):
        ExecutionSnapshot(**values)  # type: ignore[arg-type]


def test_compiled_context_allows_repeated_acyclic_aliases() -> None:
    alias: list[object] = [{"value": 1}]
    snapshot = ExecutionSnapshot(
        **(snapshot_values() | {"compiled_context": [alias, alias]})  # type: ignore[arg-type]
    )

    assert snapshot.thawed_compiled_context() == [[{"value": 1}], [{"value": 1}]]


@pytest.mark.parametrize("snapshot_hash", ["A" * 64, "a" * 63, "g" * 64])
def test_snapshot_hash_must_be_lowercase_sha256(snapshot_hash: str) -> None:
    values = snapshot_values() | {"snapshot_hash": snapshot_hash}
    with pytest.raises(ValidationError):
        ExecutionSnapshot(**values)  # type: ignore[arg-type]


def test_snapshot_hash_rejects_bytes() -> None:
    values = snapshot_values() | {"snapshot_hash": b"a" * 64}

    with pytest.raises(ValidationError):
        ExecutionSnapshot(**values)  # type: ignore[arg-type]
