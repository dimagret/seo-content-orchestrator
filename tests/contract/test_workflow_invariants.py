"""Fail-closed invariants for the sanitized n8n source fixture."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from integrations.n8n.transform import transform_workflow
from integrations.n8n.validate import validate_sanitized_source, validate_universal_workflow

ROOT = Path(__file__).parents[2]
SOURCE_PATH = ROOT / "integrations" / "n8n" / "source-workflow.json"


def _load_source() -> dict[str, Any]:
    loaded: object = json.loads(SOURCE_PATH.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def test_sanitized_source_fixture_passes_secret_scan() -> None:
    report = validate_sanitized_source(_load_source())

    assert report.is_valid, report.errors


def test_source_validator_rejects_unredacted_sensitive_field() -> None:
    unsafe = copy.deepcopy(_load_source())
    unsafe["nodes"][1]["parameters"]["documentId"] = "real-document-id"

    report = validate_sanitized_source(unsafe)

    assert not report.is_valid
    assert any("documentId" in error for error in report.errors)


def test_source_validator_rejects_credential_fields_and_secret_patterns() -> None:
    unsafe = copy.deepcopy(_load_source())
    unsafe["nodes"][0]["credentials"] = {"example": "[REDACTED]"}
    unsafe["nodes"][0]["parameters"]["authorization"] = "Bearer abcdefghijklmnop"

    report = validate_sanitized_source(unsafe)

    assert not report.is_valid
    assert any("credential fields" in error for error in report.errors)
    assert any("authorization" in error for error in report.errors)


def test_source_validator_rejects_sensitive_urls_embedded_in_multiline_text() -> None:
    unsafe = copy.deepcopy(_load_source())
    unsafe["nodes"][0]["parameters"]["notes"] = (
        "References:\n"
        "https://docs.google.com/spreadsheets/d/private-document-id/edit\n"
        "https://example.com/course/abcdefghijklmnopqrstuv"
    )

    report = validate_sanitized_source(unsafe)

    assert not report.is_valid
    assert sum("URL contains" in error for error in report.errors) == 2


def test_source_validator_rejects_normalized_credential_names_and_embedded_jwt() -> None:
    unsafe = copy.deepcopy(_load_source())
    parameters = unsafe["nodes"][0]["parameters"]
    parameters["api_token"] = "unsafe-api-token"
    parameters["credentialId"] = "unsafe-credential-id"
    parameters["clientSecret"] = "unsafe-client-secret"
    parameters["notes"] = (
        "embedded eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signaturevalue token"
    )

    report = validate_sanitized_source(unsafe)

    assert not report.is_valid
    assert any("api_token" in error for error in report.errors)
    assert any("credentialId" in error for error in report.errors)
    assert any("clientSecret" in error for error in report.errors)
    assert any("secret pattern" in error for error in report.errors)


def test_source_validator_rejects_token_in_url_fragment() -> None:
    unsafe = copy.deepcopy(_load_source())
    unsafe["nodes"][0]["parameters"]["url"] = (
        "https://example.com/callback#access_token=unsafe-token-value"
    )

    report = validate_sanitized_source(unsafe)

    assert not report.is_valid
    assert any("URL contains" in error for error in report.errors)


def test_source_validator_rejects_common_prefixed_provider_tokens() -> None:
    unsafe = copy.deepcopy(_load_source())
    suffix = "abcdefghijklmnopqrst"
    long_suffix = suffix + "uvwxyz123456"
    assemble = "".join
    unsafe["nodes"][0]["parameters"]["provider_values"] = [
        assemble(("pplx", "-", suffix)),
        assemble(("fc", "-", suffix)),
        assemble(("ghp", "_", long_suffix)),
        assemble(("xoxb", "-", "123456789012", "-", suffix)),
        assemble(("hf", "_", long_suffix)),
        assemble(("glpat", "-", long_suffix)),
        assemble(("npm", "_", long_suffix)),
        assemble(("AIza", "Sy", "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567")),
        assemble(("AKIA", "ABCDEFGHIJKLMNOP")),
        assemble(("sk", "_", "live", "_", long_suffix)),
        assemble(("SG", ".", suffix, ".", long_suffix)),
    ]

    report = validate_sanitized_source(unsafe)

    assert not report.is_valid
    assert sum("secret pattern" in error for error in report.errors) == 11


def test_transformed_universal_workflow_passes_all_invariants() -> None:
    transformed = transform_workflow(_load_source())

    report = validate_universal_workflow(transformed)

    assert report.is_valid, report.errors


def test_universal_validator_rejects_dangling_connection() -> None:
    transformed = transform_workflow(_load_source())
    transformed["connections"]["Оценщик"]["main"][0][0]["node"] = "missing-node"

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("dangling" in error for error in report.errors)


def test_universal_validator_rejects_company_specific_fallback() -> None:
    transformed = transform_workflow(_load_source())
    transformed["nodes"][0]["parameters"]["fallback"] = "ResultUP"

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("company-specific fallback" in error for error in report.errors)


def test_universal_validator_rejects_unresolved_redacted_placeholder() -> None:
    transformed = transform_workflow(_load_source())
    firecrawl = next(node for node in transformed["nodes"] if node["name"] == "FireCrawl")
    firecrawl["parameters"]["headerParameters"]["parameters"][0]["value"] = "[REDACTED]"

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("unresolved redacted placeholder" in error for error in report.errors)


def test_universal_validator_rejects_changed_ingress_contract() -> None:
    transformed = transform_workflow(_load_source())
    ingress = next(
        node for node in transformed["nodes"] if node["name"] == "When clicking ‘Execute workflow’"
    )
    ingress["parameters"]["workflowInputs"]["values"].pop()

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("ingress" in error for error in report.errors)


def test_universal_validator_rejects_unknown_empty_connection_source() -> None:
    transformed = transform_workflow(_load_source())
    transformed["connections"]["missing-node"] = {"main": [[]]}

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("connection source" in error for error in report.errors)


def test_universal_validator_rejects_unknown_empty_connection_group() -> None:
    transformed = transform_workflow(_load_source())
    transformed["connections"]["Оценщик"]["unexpected"] = [[]]

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("connection group" in error for error in report.errors)


def test_universal_validator_rejects_duplicate_ingress_fields() -> None:
    transformed = transform_workflow(_load_source())
    ingress = next(
        node for node in transformed["nodes"] if node["name"] == "When clicking ‘Execute workflow’"
    )
    fields = ingress["parameters"]["workflowInputs"]["values"]
    fields.append(copy.deepcopy(fields[0]))

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("duplicate ingress" in error for error in report.errors)


def test_universal_validator_rejects_duplicate_context_assignments() -> None:
    transformed = transform_workflow(_load_source())
    context = next(node for node in transformed["nodes"] if node["name"] == "Для кого пишем")
    assignments = context["parameters"]["assignments"]["assignments"]
    assignments.append(copy.deepcopy(assignments[0]))

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("duplicate assignment" in error for error in report.errors)


def test_universal_validator_fails_closed_on_non_string_field_names() -> None:
    transformed = transform_workflow(_load_source())
    ingress = next(
        node for node in transformed["nodes"] if node["name"] == "When clicking ‘Execute workflow’"
    )
    ingress["parameters"]["workflowInputs"]["values"][0]["name"] = []

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("ingress" in error for error in report.errors)


def test_universal_validator_rejects_retained_node_type_drift() -> None:
    transformed = transform_workflow(_load_source())
    evaluator = next(node for node in transformed["nodes"] if node["name"] == "Оценщик")
    evaluator["type"] = "n8n-nodes-base.set"

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("retained node type" in error for error in report.errors)


def test_universal_validator_rejects_hard_coded_model_id() -> None:
    transformed = transform_workflow(_load_source())
    model = next(node for node in transformed["nodes"] if ".lmChat" in node["type"])
    model["parameters"]["model"] = "openai/gpt-4o"

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("approved model/provider" in error for error in report.errors)


def test_universal_validator_rejects_retained_position_drift() -> None:
    transformed = transform_workflow(_load_source())
    evaluator = next(node for node in transformed["nodes"] if node["name"] == "Оценщик")
    evaluator["position"] = [0, 0]

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("position" in error for error in report.errors)


def test_universal_validator_rejects_extra_root_structure() -> None:
    transformed = transform_workflow(_load_source())
    transformed["unexpected"] = {}

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("root structure" in error for error in report.errors)


def test_universal_validator_rejects_changed_connection_target_type() -> None:
    transformed = transform_workflow(_load_source())
    transformed["connections"]["Оценщик"]["main"][0][0]["type"] = "ai_tool"

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("connection target" in error for error in report.errors)


def test_universal_validator_rejects_extra_executable_parameter() -> None:
    transformed = transform_workflow(_load_source())
    evaluator = next(node for node in transformed["nodes"] if node["name"] == "Оценщик")
    evaluator["parameters"]["unexpected"] = True

    report = validate_universal_workflow(transformed)

    assert not report.is_valid
    assert any("parameter map" in error for error in report.errors)
