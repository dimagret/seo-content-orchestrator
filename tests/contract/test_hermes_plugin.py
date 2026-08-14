"""Contract tests for the narrow profile-local Hermes plugin."""

from __future__ import annotations

import importlib
import inspect
from pathlib import Path
from typing import Any

EXPECTED_TOOLS = {
    "seo_company_list",
    "seo_company_get",
    "seo_company_save_draft",
    "seo_brief_start",
    "seo_brief_update",
    "seo_brief_validate",
    "seo_job_plan",
    "seo_job_approve",
    "seo_job_status",
    "seo_job_cancel",
    "seo_job_retry",
    "seo_job_artifact",
    "seo_export_approve",
}

MUTATING_TOOLS = EXPECTED_TOOLS - {
    "seo_company_list",
    "seo_company_get",
    "seo_job_status",
    "seo_job_artifact",
}


class FakeContext:
    """Capture plugin registrations without touching the live Hermes registry."""

    def __init__(self) -> None:
        self.registrations: list[dict[str, Any]] = []

    def register_tool(self, **kwargs: Any) -> None:
        self.registrations.append(kwargs)


def test_registers_exact_narrow_toolset_without_overrides() -> None:
    plugin = importlib.import_module("integrations.hermes.seo_orchestrator")
    context = FakeContext()

    plugin.register(context)

    by_name = {registration["name"]: registration for registration in context.registrations}
    assert set(by_name) == EXPECTED_TOOLS
    assert len(context.registrations) == len(EXPECTED_TOOLS)
    for name, registration in by_name.items():
        assert registration["toolset"] == "seo_orchestrator"
        assert registration["override"] is False
        assert registration.get("is_async", False) is False
        schema = registration["schema"]
        assert schema["name"] == name
        assert schema["description"]
        parameters = schema["parameters"]
        assert parameters["type"] == "object"
        assert parameters["additionalProperties"] is False
        assert set(parameters["required"]) <= set(parameters["properties"])
        signature = inspect.signature(registration["handler"])
        assert list(signature.parameters) == ["args", "kwargs"]


def test_mutating_tools_require_explicit_scope_and_concurrency_inputs() -> None:
    plugin = importlib.import_module("integrations.hermes.seo_orchestrator")
    context = FakeContext()

    plugin.register(context)

    by_name = {registration["name"]: registration for registration in context.registrations}
    for name in MUTATING_TOOLS:
        required = set(by_name[name]["schema"]["parameters"]["required"])
        assert "company_id" in required, name

    assert "expected_version" in set(
        by_name["seo_company_save_draft"]["schema"]["parameters"]["required"]
    )
    assert "brief_id" in set(by_name["seo_brief_update"]["schema"]["parameters"]["required"])
    assert "expected_version" in set(
        by_name["seo_brief_update"]["schema"]["parameters"]["required"]
    )
    assert "job_id" in set(by_name["seo_job_cancel"]["schema"]["parameters"]["required"])
    assert "expected_state" in set(
        by_name["seo_job_cancel"]["schema"]["parameters"]["required"]
    )
    assert "plan_fingerprint" in set(
        by_name["seo_job_approve"]["schema"]["parameters"]["required"]
    )
    assert "artifact_hash" in set(
        by_name["seo_export_approve"]["schema"]["parameters"]["required"]
    )


def test_manifest_and_runtime_paths_match_the_stage_b_contract() -> None:
    from integrations.hermes.seo_orchestrator.client import (
        DEFAULT_SOCKET_PATH,
        DEFAULT_TOKEN_PATH,
    )

    manifest = Path(
        "integrations/hermes/seo_orchestrator/plugin.yaml"
    ).read_text(encoding="utf-8")

    assert manifest.startswith("name: seo-orchestrator\n")
    assert DEFAULT_SOCKET_PATH == Path("/opt/data/seo-runtime/worker.sock")
    assert DEFAULT_TOKEN_PATH == Path("/opt/data/seo-runtime/worker-api.token")


def test_identifier_schema_exactly_matches_worker_domain() -> None:
    from integrations.hermes.seo_orchestrator.schemas import TOOL_SCHEMAS

    identifier = TOOL_SCHEMAS["seo_company_get"]["parameters"]["properties"]["company_id"]
    assert identifier == {
        "type": "string",
        "minLength": 2,
        "maxLength": 64,
        "pattern": "^[a-z0-9][a-z0-9-]{1,63}$",
    }
