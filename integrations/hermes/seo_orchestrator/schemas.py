"""Strict model-facing JSON schemas for the SEO orchestrator toolset."""

from __future__ import annotations

from typing import Any

_IDENTIFIER = {
    "type": "string",
    "minLength": 2,
    "maxLength": 64,
    "pattern": "^[a-z0-9][a-z0-9-]{1,63}$",
}
_SHA256 = {"type": "string", "pattern": "^[0-9a-f]{64}$"}
_VERSION = {"type": "integer", "minimum": 1}
_TEXT = {"type": "string", "minLength": 1, "maxLength": 32_768}
_OPTIONAL_TEXT = {"type": ["string", "null"], "maxLength": 32_768}
_TEXTS = {
    "type": "array",
    "items": _TEXT,
    "minItems": 1,
    "maxItems": 256,
}
_OPTIONAL_TEXTS = {
    "type": ["array", "null"],
    "items": _TEXT,
    "maxItems": 256,
}

_COMPANY_PROFILE_FIELDS = (
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
_COMPANY_PROFILE_LIST_FIELDS = (
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
_COMPANY_PROFILE = {
    "type": "object",
    "properties": {
        **{field: _TEXT for field in _COMPANY_PROFILE_FIELDS},
        **{field: _TEXTS for field in _COMPANY_PROFILE_LIST_FIELDS},
    },
    "required": [*_COMPANY_PROFILE_FIELDS, *_COMPANY_PROFILE_LIST_FIELDS],
    "additionalProperties": False,
}
_BRIEF_REPLACEMENT = {
    "type": "object",
    "properties": {

        "direction_id": {**_IDENTIFIER, "type": ["string", "null"]},
        "direction_version": {"type": ["integer", "null"], "minimum": 1},
        "audience_segment_id": {**_IDENTIFIER, "type": ["string", "null"]},
        "audience_version": {"type": ["integer", "null"], "minimum": 1},
        "page_type": _OPTIONAL_TEXT,
        "goal": _OPTIONAL_TEXT,
        "target_language": _OPTIONAL_TEXT,
        "locale": _OPTIONAL_TEXT,
        "page_structure": _OPTIONAL_TEXTS,
        "category_context": _OPTIONAL_TEXT,
        "primary_keyword": _OPTIONAL_TEXT,
        "keywords": _OPTIONAL_TEXTS,
        "lsi_terms": _OPTIONAL_TEXTS,
        "competitor_urls": _OPTIONAL_TEXTS,
        "current_page_url": _OPTIONAL_TEXT,
        "current_page_context": _OPTIONAL_TEXT,
        "output_sheet_target": _OPTIONAL_TEXT,
    },
    "minProperties": 1,
    "additionalProperties": False,
}
_EXECUTION_PLAN = {
    "type": "object",
    "properties": {
        "pipeline_version": _TEXT,
        "executor_name": _TEXT,
        "model_ids": _TEXTS,
        "provider_ids": _TEXTS,
        "maximum_retries": {"type": "integer", "minimum": 0},
        "cost_currency": {"type": ["string", "null"], "maxLength": 64},
        "cost_min_decimal": {"type": ["string", "null"], "maxLength": 128},
        "cost_max_decimal": {"type": ["string", "null"], "maxLength": 128},
        "unknown_cost_reasons": {
            "type": "array",
            "items": _TEXT,
            "maxItems": 256,
        },
        "result_destination": _TEXT,
    },
    "required": [
        "pipeline_version",
        "executor_name",
        "model_ids",
        "provider_ids",
        "maximum_retries",
        "cost_currency",
        "cost_min_decimal",
        "cost_max_decimal",
        "unknown_cost_reasons",
        "result_destination",
    ],
    "additionalProperties": False,
}


def _schema(name: str, description: str, properties: dict[str, Any], required: tuple[str, ...]) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "parameters": {
            "type": "object",
            "properties": properties,
            "required": list(required),
            "additionalProperties": False,
        },
    }


TOOL_SCHEMAS = {
    "seo_company_list": _schema("seo_company_list", "List current active company cards.", {}, ()),
    "seo_company_get": _schema(
        "seo_company_get",
        "Get one exact company card version.",
        {"company_id": _IDENTIFIER, "version": _VERSION},
        ("company_id", "version"),
    ),
    "seo_company_save_draft": _schema(
        "seo_company_save_draft",
        "Save a version-checked company-card draft without changing n8n.",
        {
            "company_id": _IDENTIFIER,
            "actor_id": _IDENTIFIER,
            "expected_version": _VERSION,
            "replacement": _COMPANY_PROFILE,
        },
        ("company_id", "actor_id", "expected_version", "replacement"),
    ),
    "seo_brief_start": _schema(
        "seo_brief_start",
        "Start a resumable company-scoped SEO brief draft.",
        {"company_id": _IDENTIFIER, "actor_id": _IDENTIFIER},
        ("company_id", "actor_id"),
    ),
    "seo_brief_update": _schema(
        "seo_brief_update",
        "Update one explicit brief draft version.",
        {
            "company_id": _IDENTIFIER,
            "brief_id": _IDENTIFIER,
            "actor_id": _IDENTIFIER,
            "expected_version": _VERSION,
            "expected_profile_version": _VERSION,
            "replacement": _BRIEF_REPLACEMENT,
        },
        (
            "company_id",
            "brief_id",
            "actor_id",
            "expected_version",
            "expected_profile_version",
            "replacement",
        ),
    ),
    "seo_brief_validate": _schema(
        "seo_brief_validate",
        "Validate and freeze one company-scoped brief.",
        {
            "company_id": _IDENTIFIER,
            "brief_id": _IDENTIFIER,
            "actor_id": _IDENTIFIER,
            "expected_version": _VERSION,
            "expected_profile_version": _VERSION,
        },
        (
            "company_id",
            "brief_id",
            "actor_id",
            "expected_version",
            "expected_profile_version",
        ),
    ),
    "seo_job_plan": _schema(
        "seo_job_plan",
        "Create a non-executing immutable job plan.",
        {
            "company_id": _IDENTIFIER,
            "snapshot_id": _IDENTIFIER,
            "execution_plan": _EXECUTION_PLAN,
        },
        ("company_id", "snapshot_id", "execution_plan"),
    ),
    "seo_job_approve": _schema(
        "seo_job_approve",
        "Approve one exact paid execution plan and snapshot.",
        {
            "company_id": _IDENTIFIER,
            "job_id": _IDENTIFIER,
            "actor_id": _IDENTIFIER,
            "snapshot_hash": _SHA256,
            "plan_fingerprint": _SHA256,
        },
        ("company_id", "job_id", "actor_id", "snapshot_hash", "plan_fingerprint"),
    ),
    "seo_job_status": _schema(
        "seo_job_status",
        "Read one explicitly company-scoped job status.",
        {"company_id": _IDENTIFIER, "job_id": _IDENTIFIER},
        ("company_id", "job_id"),
    ),
    "seo_job_cancel": _schema(
        "seo_job_cancel",
        "Cancel a job using compare-and-swap expected state.",
        {
            "company_id": _IDENTIFIER,
            "job_id": _IDENTIFIER,
            "expected_state": {"type": "string", "enum": ["QUEUED", "RUNNING"]},
        },
        ("company_id", "job_id", "expected_state"),
    ),
    "seo_job_retry": _schema(
        "seo_job_retry",
        "Retry one explicitly company-scoped failed job.",
        {"company_id": _IDENTIFIER, "job_id": _IDENTIFIER},
        ("company_id", "job_id"),
    ),
    "seo_job_artifact": _schema(
        "seo_job_artifact",
        "Read the succeeded content artifact for one company-scoped job.",
        {"company_id": _IDENTIFIER, "job_id": _IDENTIFIER},
        ("company_id", "job_id"),
    ),
    "seo_export_approve": _schema(
        "seo_export_approve",
        "Approve one exact artifact export destination contract.",
        {
            "company_id": _IDENTIFIER,
            "job_id": _IDENTIFIER,
            "actor_id": _IDENTIFIER,
            "artifact_hash": _SHA256,
            "plan_fingerprint": _SHA256,
            "destination": {"type": "object", "additionalProperties": False},
        },
        ("company_id", "job_id", "actor_id", "artifact_hash", "plan_fingerprint", "destination"),
    ),
}
