"""Contract tests for the pure deterministic n8n workflow transformer."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from integrations.n8n.transform import (
    WorkflowInvariantError,
    render_workflow_json,
    transform_workflow,
)
from seo_orchestrator.canonical import canonical_json
from seo_orchestrator.executors.base import execution_result_from_bytes

ROOT = Path(__file__).parents[2]
SOURCE_PATH = ROOT / "integrations" / "n8n" / "source-workflow.json"
EXPECTED_MAP_PATH = ROOT / "integrations" / "n8n" / "expected-node-map.json"
AUTHORITATIVE_EXPORT_PATH = Path("/opt/data/tmp/n8n_first_workflow_details.json")

REMOVED_DISCONNECTED = {
    "Embeddings OpenAI",
    "Supabase Vector Store",
    "Reranker Cohere",
    "Scrape a url and get its content1",
    "/scrape in Firecrawl",
}


def _load_source() -> dict[str, Any]:
    loaded: object = json.loads(SOURCE_PATH.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _node(workflow: dict[str, Any], name: str) -> dict[str, Any]:
    return next(node for node in workflow["nodes"] if node["name"] == name)


def _canonical_graph(workflow: dict[str, Any]) -> dict[str, Any]:
    edges = []
    for source, groups in workflow["connections"].items():
        for kind, outputs in groups.items():
            for output_index, targets in enumerate(outputs):
                for target in targets:
                    edges.append(
                        {
                            "source": source,
                            "kind": kind,
                            "output_index": output_index,
                            "target": target["node"],
                            "target_type": target["type"],
                            "target_input_index": target.get("index", 0),
                        }
                    )
    edges.sort(
        key=lambda edge: (
            edge["source"],
            edge["kind"],
            edge["output_index"],
            edge["target"],
            edge["target_type"],
            edge["target_input_index"],
        )
    )
    nodes = sorted(
        [
            {
                "id": node["id"],
                "name": node["name"],
                "type": node["type"],
                "position": node["position"],
            }
            for node in workflow["nodes"]
        ],
        key=lambda node: node["name"],
    )
    return {"nodes": nodes, "edges": edges}


def test_transform_is_deterministic_and_preserves_source_object() -> None:
    source = _load_source()
    original = copy.deepcopy(source)

    first = transform_workflow(source)
    second = transform_workflow(source)

    assert source == original
    assert first == second
    assert first is not source


def test_transform_fails_closed_when_source_node_type_changes() -> None:
    source = _load_source()
    _node(source, "Оценщик")["type"] = "n8n-nodes-base.set"

    with pytest.raises(WorkflowInvariantError, match="source node/type map"):
        transform_workflow(source)


def test_transform_fails_closed_when_source_node_id_changes() -> None:
    source = _load_source()
    _node(source, "When clicking ‘Execute workflow’")["id"] = "tampered-id"

    with pytest.raises(WorkflowInvariantError, match="source node/ID map"):
        transform_workflow(source)


def test_transform_fails_closed_when_source_node_position_changes() -> None:
    source = _load_source()
    _node(source, "Оценщик")["position"] = [0, 0]

    with pytest.raises(WorkflowInvariantError, match="source node position map"):
        transform_workflow(source)


def test_transform_fails_closed_when_critical_connection_changes() -> None:
    source = _load_source()
    source["connections"]["Оценщик"]["main"][0][0]["node"] = "Цикл"

    with pytest.raises(WorkflowInvariantError, match="critical connections"):
        transform_workflow(source)


def test_transform_fails_closed_when_connection_target_type_changes() -> None:
    source = _load_source()
    source["connections"]["Оценщик"]["main"][0][0]["type"] = "ai_tool"

    with pytest.raises(WorkflowInvariantError, match="critical connections"):
        transform_workflow(source)


def test_transform_fails_closed_when_empty_connection_group_is_added() -> None:
    source = _load_source()
    source["connections"]["Оценщик"]["unexpected"] = [[]]

    with pytest.raises(WorkflowInvariantError, match="connection group structure"):
        transform_workflow(source)


def test_transform_fails_closed_when_source_root_key_is_added() -> None:
    source = _load_source()
    source["unexpected"] = {}

    with pytest.raises(WorkflowInvariantError, match="source root structure"):
        transform_workflow(source)


def test_transform_fails_closed_when_source_parameter_is_added() -> None:
    source = _load_source()
    _node(source, "Оценщик")["parameters"]["unexpected"] = True

    with pytest.raises(WorkflowInvariantError, match="source parameter map"):
        transform_workflow(source)


def test_sanitized_graph_matches_recorded_authoritative_digest() -> None:
    source = _load_source()
    expected: object = json.loads(EXPECTED_MAP_PATH.read_text(encoding="utf-8"))
    assert isinstance(expected, dict)
    canonical = json.dumps(
        _canonical_graph(source),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()

    assert hashlib.sha256(canonical).hexdigest() == expected["source_graph_sha256"]
    assert {node["name"]: node["position"] for node in source["nodes"]} == expected[
        "source_node_positions"
    ]


def test_local_authoritative_export_matches_recorded_provenance() -> None:
    if not AUTHORITATIVE_EXPORT_PATH.is_file():
        pytest.skip("authoritative local export is not available in this environment")
    export_bytes = AUTHORITATIVE_EXPORT_PATH.read_bytes()
    export: object = json.loads(export_bytes)
    expected: object = json.loads(EXPECTED_MAP_PATH.read_text(encoding="utf-8"))
    assert isinstance(export, dict)
    assert isinstance(export.get("workflow"), dict)
    assert isinstance(expected, dict)
    original_graph = _canonical_graph(export["workflow"])
    canonical_graph = json.dumps(
        original_graph,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()

    assert hashlib.sha256(export_bytes).hexdigest() == expected["source_export_sha256"]
    assert hashlib.sha256(canonical_graph).hexdigest() == expected["source_graph_sha256"]


def test_transform_and_renderer_are_local_and_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("network access is forbidden")

    monkeypatch.setattr("socket.socket.connect", reject_network)
    transformed = transform_workflow(_load_source())

    rendered = render_workflow_json(transformed)

    assert rendered == render_workflow_json(transformed)
    assert rendered.endswith("\n")
    assert json.loads(rendered) == transformed
    assert rendered.index('"active"') < rendered.index('"connections"')


def test_transform_replaces_ingress_and_source_reads_with_snapshot_data() -> None:
    transformed = transform_workflow(_load_source())

    ingress = _node(transformed, "When clicking ‘Execute workflow’")
    assert ingress["type"] == "n8n-nodes-base.executeWorkflowTrigger"
    input_fields = {
        value["name"]: value["type"]
        for value in ingress["parameters"]["workflowInputs"]["values"]
    }
    assert input_fields == {
        "job_id": "string",
        "brief_id": "string",
        "brief_fingerprint": "string",
        "snapshot_hash": "string",
        "attempt": "number",
        "approved_plan_fingerprint": "string",
        "approval_record_id": "string",
        "authority_expires_at": "string",
        "approved_model_ids": "array",
        "approved_provider_ids": "array",
        "execution_snapshot": "object",
    }

    context = _node(transformed, "Для кого пишем")
    assignments = {
        assignment["name"]: assignment["value"]
        for assignment in context["parameters"]["assignments"]["assignments"]
    }
    assert set(assignments) == {
        "company",
        "target",
        "stucture",
        "person",
        "podrajanie",
        "language",
        "LOCALE",
        "year",
    }
    assert all(value.startswith("={{") for value in assignments.values())
    assert "JSON.stringify" in assignments["company"]
    assert "compiled_context.company" in assignments["company"]
    assert "compiled_context.brief.goal" in assignments["target"]
    assert "compiled_context.brief.page_structure" in assignments["stucture"]
    assert "JSON.stringify" in assignments["person"]
    assert "compiled_context.audience" in assignments["person"]
    assert "compiled_context.company.positive_voice_examples" in assignments["podrajanie"]
    assert "compiled_context.brief.target_language" in assignments["language"]
    assert "compiled_context.brief.locale" in assignments["LOCALE"]
    assert "execution_snapshot.created_at" in assignments["year"]

    for name in ("SEO ТЗ из таблицы", "Сбор кейсов", "Ссылки для линковки1"):
        node = _node(transformed, name)
        assert node["type"] != "n8n-nodes-base.googleSheets"
        serialized = json.dumps(node["parameters"], ensure_ascii=False)
        assert "$('When clicking ‘Execute workflow’').first().json" in serialized
        assert "$input.first()" not in serialized


def test_transform_parameterizes_every_retained_model_from_approved_ids() -> None:
    transformed = transform_workflow(_load_source())
    model_nodes = [
        node for node in transformed["nodes"] if ".lmChat" in node["type"]
    ]

    assert model_nodes
    for node in model_nodes:
        expression = node["parameters"]["model"]
        assert expression.startswith("={{")
        assert "approved_model_ids" in expression
        assert "approved_provider_ids" in expression
        assert "providers.includes('openrouter')" in expression
        assert "models[0]" in expression

    firecrawl = _node(transformed, "FireCrawl")
    authorization = firecrawl["parameters"]["headerParameters"]["parameters"][0]["value"]
    assert "approved_provider_ids" in authorization
    assert "providers.includes('firecrawl')" in authorization
    assert "$vars.FIRECRAWL_API_KEY" in authorization


def test_transform_preserves_ids_except_explicit_disconnected_removals() -> None:
    source = _load_source()
    transformed = transform_workflow(source)
    removed_ids = {
        node["id"] for node in source["nodes"] if node["name"] in REMOVED_DISCONNECTED
    }

    assert {node["id"] for node in transformed["nodes"]} == {
        node["id"] for node in source["nodes"]
    } - removed_ids
    assert not (REMOVED_DISCONNECTED & {node["name"] for node in transformed["nodes"]})


def test_transform_emits_structured_versioned_result_without_fallbacks() -> None:
    source = _load_source()
    transformed = transform_workflow(source)
    final = _node(transformed, "Финальный вывод")

    assert final["type"] == "n8n-nodes-base.set"
    output_assignments = {
        assignment["name"]: assignment["value"]
        for assignment in final["parameters"]["assignments"]["assignments"]
    }
    result_fixture: object = json.loads(
        (ROOT / "fixtures" / "executions" / "success-result.json").read_text(encoding="utf-8")
    )
    assert isinstance(result_fixture, dict)
    assert set(output_assignments) == set(result_fixture)
    parser_schema = json.loads(_node(source, "Structured Output Parser")["parameters"]["inputSchema"])
    assert set(parser_schema["properties"]) == {
        "title_1",
        "title_2",
        "title_3",
        "title_4",
        "title_5",
        "desc_1",
        "desc_2",
        "desc_3",
        "desc_4",
        "desc_5",
    }
    assert "Редактура и проверка вписывания ссылок" in output_assignments["content_markdown"]
    assert all(f"$json.output.title_{index}" in output_assignments["titles"] for index in range(1, 6))
    assert all(
        f"$json.output.desc_{index}" in output_assignments["descriptions"]
        for index in range(1, 6)
    )
    assert "compiled_context.brief.primary_keywords[0]" in output_assignments["keyword_qa"]
    assert "passed: false" in output_assignments["keyword_qa"]
    assert "Подсчет параметров текста" in output_assignments["text_metrics"]
    assert output_assignments["sources"] == "={{ [] }}"
    assert "legacy workflow does not emit source provenance" in output_assignments["warnings"]
    assert "approved_model_ids[0]" in output_assignments["model_usage"]
    assert "provider_id: 'openrouter'" in output_assignments["model_usage"]
    assert output_assignments["stage_timings"] == "={{ {} }}"
    assert "stage-b-v1" in output_assignments["prompt_versions"]
    assert "snapshot_hash" in output_assignments["prompt_versions"]
    assert "String(" in output_assignments["prompt_versions"]
    assert "execution_snapshot.prompt_set_version" in output_assignments["prompt_versions"]
    representative = copy.deepcopy(result_fixture)
    representative["sources"] = []
    representative["warnings"] = ["legacy workflow does not emit source provenance"]
    representative["model_usage"] = {
        "models": [{"model_id": "writer-model-v1", "provider_id": "openrouter"}]
    }
    representative["stage_timings"] = {}
    representative["prompt_versions"] = {
        "pipeline_version": "stage-b-v1",
        "snapshot_hash": "a" * 64,
        "prompt_set_version": "1",
    }
    assert execution_result_from_bytes(canonical_json(representative)).content_markdown

    numeric_version = copy.deepcopy(result_fixture)
    numeric_version["prompt_versions"]["prompt_set_version"] = 1
    with pytest.raises(ValueError, match="prompt_versions version"):
        execution_result_from_bytes(canonical_json(numeric_version))
    numeric_version["prompt_versions"]["prompt_set_version"] = "1"
    parsed_result = execution_result_from_bytes(canonical_json(numeric_version))
    versions = parsed_result.prompt_versions
    assert isinstance(versions, dict)
    assert versions["prompt_set_version"] == "1"

    executable_parameters = json.dumps(
        [node.get("parameters", {}) for node in transformed["nodes"]],
        ensure_ascii=False,
    )
    assert "ResultUP" not in executable_parameters
    assert "[REDACTED]" not in executable_parameters
    assert "$vars.FIRECRAWL_API_KEY" in executable_parameters
    assert "pinData" not in transformed
