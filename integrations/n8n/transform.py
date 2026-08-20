"""Pure deterministic transformation of the frozen n8n source workflow."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

from integrations.n8n.validate import validate_sanitized_source

_EXPECTED_MAP_PATH = Path(__file__).with_name("expected-node-map.json")

_INGRESS_NAME = "When clicking ‘Execute workflow’"
_CONTEXT_NAME = "Для кого пишем"
_REMOVED_NODES = frozenset(
    {
        "Embeddings OpenAI",
        "Supabase Vector Store",
        "Reranker Cohere",
        "Scrape a url and get its content1",
        "/scrape in Firecrawl",
    }
)


class WorkflowInvariantError(ValueError):
    """Raised when the frozen source graph no longer matches its contract."""


def _expected_map() -> dict[str, Any]:
    loaded: object = json.loads(_EXPECTED_MAP_PATH.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise WorkflowInvariantError("expected node map must be an object")
    return loaded


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _node_types(workflow: dict[str, Any]) -> dict[str, str]:
    try:
        pairs = [(node["name"], node["type"]) for node in workflow["nodes"]]
    except (KeyError, TypeError) as exc:
        raise WorkflowInvariantError("source nodes have an invalid shape") from exc
    if len({name for name, _ in pairs}) != len(pairs):
        raise WorkflowInvariantError("source node names must be unique")
    return dict(pairs)


def _node_ids(workflow: dict[str, Any]) -> dict[str, str]:
    try:
        pairs = [(node["name"], node["id"]) for node in workflow["nodes"]]
    except (KeyError, TypeError) as exc:
        raise WorkflowInvariantError("source node IDs have an invalid shape") from exc
    if not all(isinstance(name, str) and isinstance(node_id, str) for name, node_id in pairs):
        raise WorkflowInvariantError("source node IDs have an invalid shape")
    return dict(pairs)


def _node_positions(workflow: dict[str, Any]) -> dict[str, Any]:
    try:
        pairs = [(node["name"], node["position"]) for node in workflow["nodes"]]
    except (KeyError, TypeError) as exc:
        raise WorkflowInvariantError("source node positions have an invalid shape") from exc
    if not all(isinstance(name, str) for name, _ in pairs):
        raise WorkflowInvariantError("source node positions have an invalid shape")
    return dict(pairs)


def _edges(workflow: dict[str, Any]) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    try:
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
    except (AttributeError, KeyError, TypeError) as exc:
        raise WorkflowInvariantError("source connections have an invalid shape") from exc
    return sorted(
        edges,
        key=lambda edge: (
            edge["source"],
            edge["kind"],
            edge["output_index"],
            edge["target"],
            edge["target_type"],
            edge["target_input_index"],
        ),
    )


def _connection_groups(workflow: dict[str, Any]) -> dict[str, dict[str, int]]:
    connections = workflow.get("connections")
    if not isinstance(connections, dict):
        raise WorkflowInvariantError("source connection groups have an invalid shape")
    result: dict[str, dict[str, int]] = {}
    for source, groups in connections.items():
        if not isinstance(source, str) or not isinstance(groups, dict):
            raise WorkflowInvariantError("source connection groups have an invalid shape")
        group_counts: dict[str, int] = {}
        for kind, outputs in groups.items():
            if not isinstance(kind, str) or not isinstance(outputs, list):
                raise WorkflowInvariantError("source connection groups have an invalid shape")
            group_counts[kind] = len(outputs)
        result[source] = group_counts
    return result


def _assert_source(workflow: dict[str, Any]) -> None:
    report = validate_sanitized_source(workflow)
    if not report.is_valid:
        raise WorkflowInvariantError("source sanitizer violations: " + "; ".join(report.errors))
    expected = _expected_map()
    if sorted(workflow) != expected["source_root_keys"]:
        raise WorkflowInvariantError("source root structure differs from expected map")
    if _node_types(workflow) != expected["source_node_types"]:
        raise WorkflowInvariantError("source node/type map differs from expected map")
    if _node_ids(workflow) != expected["source_node_ids"]:
        raise WorkflowInvariantError("source node/ID map differs from expected map")
    if _node_positions(workflow) != expected["source_node_positions"]:
        raise WorkflowInvariantError("source node position map differs from expected map")
    if _connection_groups(workflow) != expected["source_connection_groups"]:
        raise WorkflowInvariantError("source connection group structure differs from expected map")
    if _edges(workflow) != expected["source_edges"]:
        raise WorkflowInvariantError("source critical connections differ from expected map")
    parameter_hashes = {
        node["name"]: _canonical_sha256(node.get("parameters")) for node in workflow["nodes"]
    }
    if parameter_hashes != expected["source_parameter_sha256"]:
        raise WorkflowInvariantError("source parameter map differs from expected map")
    if _canonical_sha256(workflow) != expected["source_workflow_sha256"]:
        raise WorkflowInvariantError("source workflow differs from frozen fixture")


def _node(workflow: dict[str, Any], name: str) -> dict[str, Any]:
    return next(node for node in workflow["nodes"] if node["name"] == name)


def _assignments(values: dict[str, str]) -> dict[str, Any]:
    return {
        "assignments": {
            "assignments": [
                {
                    "id": f"stage-b-{name.lower()}",
                    "name": name,
                    "type": "string" if name != "result" else "object",
                    "value": value,
                }
                for name, value in values.items()
            ]
        },
        "includeOtherFields": True,
        "options": {},
    }


def _typed_assignments(
    values: dict[str, str],
    field_types: dict[str, str],
) -> dict[str, Any]:
    return {
        "assignments": {
            "assignments": [
                {
                    "id": f"stage-b-result-{name.lower()}",
                    "name": name,
                    "type": field_types[name],
                    "value": value,
                }
                for name, value in values.items()
            ]
        },
        "includeOtherFields": False,
        "options": {},
    }


def _replace_company_literal(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _replace_company_literal(child) for key, child in value.items()}
    if isinstance(value, list):
        return [_replace_company_literal(child) for child in value]
    if isinstance(value, str):
        return value.replace("ResultUP", "current company")
    return value


def _remove_disconnected_nodes(workflow: dict[str, Any]) -> None:
    workflow["nodes"] = [
        node for node in workflow["nodes"] if node["name"] not in _REMOVED_NODES
    ]
    connections: dict[str, Any] = {}
    for source, groups in workflow["connections"].items():
        if source in _REMOVED_NODES:
            continue
        retained_groups: dict[str, Any] = {}
        for kind, outputs in groups.items():
            retained_groups[kind] = [
                [target for target in targets if target["node"] not in _REMOVED_NODES]
                for targets in outputs
            ]
        connections[source] = retained_groups
    workflow["connections"] = connections


def _parameterize_ingress(workflow: dict[str, Any]) -> None:
    ingress = _node(workflow, _INGRESS_NAME)
    ingress["type"] = "n8n-nodes-base.executeWorkflowTrigger"
    ingress["typeVersion"] = 1.1
    ingress["parameters"] = {
        "workflowInputs": {
            "values": [
                {"name": "job_id", "type": "string"},
                {"name": "brief_id", "type": "string"},
                {"name": "brief_fingerprint", "type": "string"},
                {"name": "snapshot_hash", "type": "string"},
                {"name": "attempt", "type": "number"},
                {"name": "approved_plan_fingerprint", "type": "string"},
                {"name": "approval_record_id", "type": "string"},
                {"name": "authority_expires_at", "type": "string"},
                {"name": "approved_model_ids", "type": "array"},
                {"name": "approved_provider_ids", "type": "array"},
                {"name": "execution_snapshot", "type": "object"},
            ]
        }
    }

    ingress_ref = f"$('{_INGRESS_NAME}').item.json"
    context = _node(workflow, _CONTEXT_NAME)
    context["parameters"] = _assignments(
        {
            "company": (
                f"={{{{ JSON.stringify({ingress_ref}.execution_snapshot.compiled_context.company) }}}}"
            ),
            "target": f"={{{{ {ingress_ref}.execution_snapshot.compiled_context.brief.goal }}}}",
            "stucture": (
                f"={{{{ JSON.stringify({ingress_ref}.execution_snapshot.compiled_context.brief.page_structure) }}}}"
            ),
            "person": (
                f"={{{{ JSON.stringify({ingress_ref}.execution_snapshot.compiled_context.audience) }}}}"
            ),
            "podrajanie": (
                f"={{{{ {ingress_ref}.execution_snapshot.compiled_context.company.positive_voice_examples.join('\\n') }}}}"
            ),
            "language": (
                f"={{{{ {ingress_ref}.execution_snapshot.compiled_context.brief.target_language }}}}"
            ),
            "LOCALE": f"={{{{ {ingress_ref}.execution_snapshot.compiled_context.brief.locale }}}}",
            "year": f"={{{{ {ingress_ref}.execution_snapshot.created_at.slice(0, 4) }}}}",
        }
    )


def _replace_source_reads(workflow: dict[str, Any]) -> None:
    input_expression = f"$('{_INGRESS_NAME}').first().json"
    scripts = {
        "SEO ТЗ из таблицы": (
            f"const input = {input_expression};\n"
            "const values = input.execution_snapshot.compiled_context.brief.competitor_urls;\n"
            "return values.map((target_url) => ({ json: { ...input, target_url } }));"
        ),
        "Сбор кейсов": (
            f"const input = {input_expression};\n"
            "const values = input.execution_snapshot.compiled_context.direction.direction_cases;\n"
            "return values.map((case_reference) => ({ json: { ...input, case_reference } }));"
        ),
        "Ссылки для линковки1": (
            f"const input = {input_expression};\n"
            "const values = input.execution_snapshot.compiled_context.direction.internal_link_catalog;\n"
            "return values.map((internal_url) => ({ json: { ...input, internal_url } }));"
        ),
    }
    for name, script in scripts.items():
        node = _node(workflow, name)
        node["type"] = "n8n-nodes-base.code"
        node["typeVersion"] = 2
        node["parameters"] = {"jsCode": script, "mode": "runOnceForAllItems"}


def _replace_final_output(workflow: dict[str, Any]) -> None:
    final = _node(workflow, "Финальный вывод")
    final["type"] = "n8n-nodes-base.set"
    final["typeVersion"] = 3.4
    ingress_ref = f"$('{_INGRESS_NAME}').item.json"
    content_ref = "$('Редактура и проверка вписывания ссылок').item.json.output"
    metrics_ref = "$('Подсчет параметров текста').item.json"
    values = {
        "content_markdown": f"={{{{ {content_ref} }}}}",
        "titles": (
            "={{ [$json.output.title_1, $json.output.title_2, $json.output.title_3, "
            "$json.output.title_4, $json.output.title_5] }}"
        ),
        "descriptions": (
            "={{ [$json.output.desc_1, $json.output.desc_2, $json.output.desc_3, "
            "$json.output.desc_4, $json.output.desc_5] }}"
        ),
        "keyword_qa": (
            "={{ (() => { "
            f"const content = String({content_ref} ?? ''); "
            f"const primary = String({ingress_ref}.execution_snapshot.compiled_context.brief.primary_keywords[0]); "
            "const occurrences = primary ? content.toLocaleLowerCase().split("
            "primary.toLocaleLowerCase()).length - 1 : 0; "
            "return { primary_keyword: primary, occurrences, passed: false }; })() }}"
        ),
        "text_metrics": (
            "={{ { "
            f"words: Number({metrics_ref}.totalWords ?? 0), "
            f"characters: String({content_ref} ?? '').length "
            "} }}"
        ),
        "sources": "={{ [] }}",
        "warnings": "={{ ['legacy workflow does not emit source provenance'] }}",
        "model_usage": (
            "={{ { models: [{ "
            f"model_id: {ingress_ref}.approved_model_ids[0], "
            "provider_id: 'openrouter' }] } }}"
        ),
        "stage_timings": "={{ {} }}",
        "prompt_versions": (
            "={{ { "
            "pipeline_version: 'stage-b-v1', "
            f"snapshot_hash: {ingress_ref}.snapshot_hash, "
            f"prompt_set_version: String({ingress_ref}.execution_snapshot.prompt_set_version) }} }}"
        ),
    }
    field_types = {
        "content_markdown": "string",
        "titles": "array",
        "descriptions": "array",
        "keyword_qa": "object",
        "text_metrics": "object",
        "sources": "array",
        "warnings": "array",
        "model_usage": "object",
        "stage_timings": "object",
        "prompt_versions": "object",
    }
    final["parameters"] = _typed_assignments(
        values,
        field_types,
    )


def _parameterize_provider_secrets(workflow: dict[str, Any]) -> None:
    firecrawl = _node(workflow, "FireCrawl")
    try:
        headers = firecrawl["parameters"]["headerParameters"]["parameters"]
        redacted = [header for header in headers if header.get("value") == "[REDACTED]"]
    except (AttributeError, KeyError, TypeError) as exc:
        raise WorkflowInvariantError("FireCrawl authorization placeholder is missing") from exc
    if len(redacted) != 1 or redacted[0].get("name") != "Authorization":
        raise WorkflowInvariantError("FireCrawl authorization placeholder differs from expected")
    ingress = f"$('{_INGRESS_NAME}').item.json"
    redacted[0]["value"] = (
        "={{ (() => { "
        f"const providers = {ingress}.approved_provider_ids; "
        "if (!Array.isArray(providers) || !providers.includes('firecrawl')) { "
        "throw new Error('approved firecrawl provider required'); "
        "} return 'Bearer ' + $vars.FIRECRAWL_API_KEY; })() }}"
    )


def _parameterize_models(workflow: dict[str, Any]) -> None:
    ingress = f"$('{_INGRESS_NAME}').item.json"
    expression = (
        "={{ (() => { "
        f"const providers = {ingress}.approved_provider_ids; "
        f"const models = {ingress}.approved_model_ids; "
        "if (!Array.isArray(providers) || !providers.includes('openrouter') || "
        "!Array.isArray(models) || models.length === 0) { "
        "throw new Error('approved provider/model IDs required'); "
        "} return models[0]; })() }}"
    )
    for node in workflow["nodes"]:
        if ".lmChat" in node["type"]:
            node["parameters"]["model"] = expression


def transform_workflow(source: dict[str, Any]) -> dict[str, Any]:
    """Validate and transform the frozen source without mutation or network calls."""

    _assert_source(source)
    workflow = copy.deepcopy(source)
    _remove_disconnected_nodes(workflow)
    _parameterize_ingress(workflow)
    _replace_source_reads(workflow)
    _replace_final_output(workflow)
    _parameterize_provider_secrets(workflow)
    _parameterize_models(workflow)
    for node in workflow["nodes"]:
        node["parameters"] = _replace_company_literal(node.get("parameters", {}))
    workflow.pop("pinData", None)
    workflow["name"] = "Stage B Universal SEO Workflow"
    workflow["active"] = False
    return workflow


def render_workflow_json(workflow: dict[str, Any]) -> str:
    """Render deterministic UTF-8 JSON for local review artifacts."""

    return json.dumps(workflow, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
