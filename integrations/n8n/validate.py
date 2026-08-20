"""Fail-closed validation for sanitized and transformed n8n workflows."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit

_REDACTED = "[REDACTED]"
_CREDENTIAL_KEYS = {"credential", "credentials"}
_SENSITIVE_KEYS = {
    "apikey",
    "apitoken",
    "accesstoken",
    "authorization",
    "clientsecret",
    "credentialid",
    "credentialname",
    "documentid",
    "password",
    "privatekey",
    "refreshtoken",
    "secret",
    "sheetid",
    "spreadsheetid",
    "token",
}
_SECRET_QUERY_KEY = re.compile(r"(token|key|secret|auth|signature|password)", re.IGNORECASE)
_SECRET_PATTERNS = (
    re.compile(r"\bBearer\s+[A-Za-z0-9._~+/-]+=*", re.IGNORECASE),
    re.compile(r"\b(?:sk|pk|rk)-[A-Za-z0-9_-]{12,}\b"),
    re.compile(r"\b(?:pplx|fc)-[A-Za-z0-9_-]{12,}\b", re.IGNORECASE),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9_-]{20,}\b", re.IGNORECASE),
    re.compile(r"\bxox[baprs]-[A-Za-z0-9_-]{12,}\b", re.IGNORECASE),
    re.compile(r"\bhf_[A-Za-z0-9_-]{20,}\b", re.IGNORECASE),
    re.compile(r"\bglpat-[A-Za-z0-9_-]{20,}\b", re.IGNORECASE),
    re.compile(r"\bnpm_[A-Za-z0-9_-]{20,}\b", re.IGNORECASE),
    re.compile(r"\bAIza[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    re.compile(r"\b(?:sk|pk)_(?:live|test)_[A-Za-z0-9_-]{16,}\b", re.IGNORECASE),
    re.compile(r"\bSG\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}(?![A-Za-z0-9_-])"),
)
_URL_PATTERN = re.compile(r"https?://[^\s<>\"`]+")
_OPAQUE_PATH_SEGMENT = re.compile(r"[A-Za-z0-9_-]{20,}")
_EXPECTED_MAP_PATH = Path(__file__).with_name("expected-node-map.json")
_INGRESS_NAME = "When clicking ‘Execute workflow’"
_CONTEXT_NAME = "Для кого пишем"


@dataclass(frozen=True, slots=True)
class ValidationReport:
    """Immutable result returned by local workflow validators."""

    errors: tuple[str, ...]

    @property
    def is_valid(self) -> bool:
        """Return whether no invariant violations were found."""

        return not self.errors


def _unsafe_url(value: str) -> bool:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return True
    if parsed.scheme not in {"http", "https"}:
        return False
    if parsed.username is not None or parsed.password is not None:
        return True
    query_keys = [key for key, _ in parse_qsl(parsed.query, keep_blank_values=True)]
    fragment_keys = [key for key, _ in parse_qsl(parsed.fragment, keep_blank_values=True)]
    if any(_SECRET_QUERY_KEY.search(key) for key in (*query_keys, *fragment_keys)):
        return True
    if parsed.hostname == "docs.google.com" and "/d/" in parsed.path:
        return True
    segments = [segment for segment in parsed.path.split("/") if segment]
    return any(_OPAQUE_PATH_SEGMENT.fullmatch(segment) for segment in segments)


def _unsafe_urls_in_text(value: str) -> tuple[str, ...]:
    urls = (
        match.group(0).rstrip("),.;]")
        for match in _URL_PATTERN.finditer(value)
    )
    return tuple(url for url in urls if _unsafe_url(url))


def _secret_errors(value: Any, *, path: str = "$") -> list[str]:
    errors: list[str] = []
    if isinstance(value, dict):
        for raw_key, child in value.items():
            key = str(raw_key)
            normalized_key = re.sub(r"[^a-z0-9]", "", key.casefold())
            child_path = f"{path}.{key}"
            if normalized_key in _CREDENTIAL_KEYS:
                errors.append(f"{child_path}: credential fields are forbidden")
                continue
            if normalized_key in _SENSITIVE_KEYS and child != _REDACTED:
                errors.append(f"{child_path}: sensitive field must be redacted")
                continue
            if key == "pinData":
                errors.append(f"{child_path}: pinned data is forbidden")
                continue
            errors.extend(_secret_errors(child, path=child_path))
        return errors
    if isinstance(value, list):
        for index, child in enumerate(value):
            errors.extend(_secret_errors(child, path=f"{path}[{index}]"))
        return errors
    if isinstance(value, str) and value != _REDACTED:
        for _ in _unsafe_urls_in_text(value):
            errors.append(f"{path}: URL contains a secret or document identifier")
        if any(pattern.search(value) for pattern in _SECRET_PATTERNS):
            errors.append(f"{path}: value contains a secret pattern")
    return errors


def validate_sanitized_source(workflow: dict[str, Any]) -> ValidationReport:
    """Reject secret-bearing fields, URLs, patterns, and pinned data."""

    return ValidationReport(errors=tuple(_secret_errors(workflow)))


def _load_expected_map() -> dict[str, Any]:
    loaded: object = json.loads(_EXPECTED_MAP_PATH.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise TypeError("expected node map must be an object")
    return loaded


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _canonical_edges(workflow: dict[str, Any]) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    connections = workflow.get("connections")
    if not isinstance(connections, dict):
        return edges
    try:
        for source, groups in connections.items():
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
    except (AttributeError, KeyError, TypeError):
        return []
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
        return {}
    result: dict[str, dict[str, int]] = {}
    for source, groups in connections.items():
        if not isinstance(source, str) or not isinstance(groups, dict):
            return {}
        group_counts: dict[str, int] = {}
        for kind, outputs in groups.items():
            if not isinstance(kind, str) or not isinstance(outputs, list):
                return {}
            group_counts[kind] = len(outputs)
        result[source] = group_counts
    return result


def _assignment_values(node: dict[str, Any]) -> dict[str, Any]:
    try:
        assignments = node["parameters"]["assignments"]["assignments"]
        return {assignment["name"]: assignment["value"] for assignment in assignments}
    except (KeyError, TypeError):
        return {}


def _assignment_list(node: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        value = node["parameters"]["assignments"]["assignments"]
    except (KeyError, TypeError):
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        return []
    return value


def validate_universal_workflow(workflow: dict[str, Any]) -> ValidationReport:
    """Validate the deterministic universal workflow without executing it."""

    errors = _secret_errors(workflow)
    expected = _load_expected_map()
    if sorted(workflow) != expected["transformed_root_keys"]:
        errors.append("$: root structure differs from frozen universal contract")
    nodes_value = workflow.get("nodes")
    if not isinstance(nodes_value, list):
        return ValidationReport(errors=(*errors, "$.nodes: nodes must be a list"))
    nodes = [node for node in nodes_value if isinstance(node, dict)]
    if len(nodes) != len(nodes_value):
        errors.append("$.nodes: every node must be an object")
    names = [node.get("name") for node in nodes]
    ids = [node.get("id") for node in nodes]
    valid_names = all(isinstance(name, str) for name in names)
    valid_ids = all(isinstance(node_id, str) for node_id in ids)
    if not valid_names or len(set(names)) != len(names):
        errors.append("$.nodes: node names must be unique strings")
    if not valid_ids or len(set(ids)) != len(ids):
        errors.append("$.nodes: node IDs must be unique strings")
    by_name = {
        node["name"]: node for node in nodes if isinstance(node.get("name"), str)
    }
    for node in nodes:
        if not isinstance(node.get("parameters"), dict):
            errors.append(f"$.nodes.{node.get('name')}: parameters must be an object")

    removed = set(expected["removed_disconnected_nodes"])
    if removed & set(by_name):
        errors.append("$.nodes: disconnected removal set is still present")
    expected_ids = {
        name: node_id
        for name, node_id in expected["source_node_ids"].items()
        if name not in removed
    }
    actual_ids = {name: node.get("id") for name, node in by_name.items()}
    if actual_ids != expected_ids:
        errors.append("$.nodes: retained source node IDs differ from expected map")
    expected_types = {
        name: node_type
        for name, node_type in expected["source_node_types"].items()
        if name not in removed
    }
    expected_types[_INGRESS_NAME] = "n8n-nodes-base.executeWorkflowTrigger"
    for name in expected["source_read_nodes"]:
        expected_types[name] = "n8n-nodes-base.code"
    expected_types["Финальный вывод"] = "n8n-nodes-base.set"
    actual_types = {name: node.get("type") for name, node in by_name.items()}
    if actual_types != expected_types:
        errors.append("$.nodes: retained node type map differs from expected contract")
    actual_positions = {name: node.get("position") for name, node in by_name.items()}
    if actual_positions != expected["transformed_node_positions"]:
        errors.append("$.nodes: retained node position map differs from expected contract")
    actual_parameter_hashes = {
        name: _canonical_sha256(node.get("parameters")) for name, node in by_name.items()
    }
    if actual_parameter_hashes != expected["transformed_parameter_sha256"]:
        errors.append("$.nodes: executable parameter map differs from expected contract")

    connections = workflow.get("connections")
    if isinstance(connections, dict):
        unknown_sources = set(connections) - set(by_name)
        if unknown_sources:
            errors.append("$.connections: connection source references a missing node")
    else:
        errors.append("$.connections: connections must be an object")
    expected_connection_groups = {
        source: groups
        for source, groups in expected["source_connection_groups"].items()
        if source not in removed
    }
    if _connection_groups(workflow) != expected_connection_groups:
        errors.append("$.connections: connection group structure differs from expected map")
    if isinstance(connections, dict):
        invalid_targets = False
        try:
            for groups in connections.values():
                for kind, outputs in groups.items():
                    for targets in outputs:
                        for target in targets:
                            if (
                                set(target) != {"node", "type", "index"}
                                or target.get("type") != kind
                                or not isinstance(target.get("index"), int)
                            ):
                                invalid_targets = True
        except (AttributeError, TypeError):
            invalid_targets = True
        if invalid_targets:
            errors.append("$.connections: connection target structure differs from expected map")
    actual_edges = _canonical_edges(workflow)
    dangling = [
        edge
        for edge in actual_edges
        if edge["source"] not in by_name or edge["target"] not in by_name
    ]
    if dangling:
        errors.append("$.connections: dangling connection references a missing node")
    expected_edges = [
        edge
        for edge in expected["source_edges"]
        if edge["source"] not in removed and edge["target"] not in removed
    ]
    if actual_edges != expected_edges:
        errors.append("$.connections: retained processing order differs from expected map")

    ingress = by_name.get(_INGRESS_NAME, {})
    try:
        ingress_values = ingress["parameters"]["workflowInputs"]["values"]
    except (KeyError, TypeError):
        ingress_values = []
    if not isinstance(ingress_values, list) or not all(
        isinstance(value, dict) for value in ingress_values
    ):
        ingress_values = []
    ingress_names = [value.get("name") for value in ingress_values]
    valid_ingress_names = all(isinstance(name, str) for name in ingress_names)
    if not valid_ingress_names or len(ingress_names) != len(set(ingress_names)):
        errors.append("$.nodes: duplicate ingress fields are forbidden")
    ingress_fields = (
        {value.get("name"): value.get("type") for value in ingress_values}
        if valid_ingress_names
        else {}
    )
    if ingress.get("type") != "n8n-nodes-base.executeWorkflowTrigger" or ingress_fields != {
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
    }:
        errors.append("$.nodes: ingress contract is not the validated Stage B schema")

    context = by_name.get(_CONTEXT_NAME, {})
    context_assignments = _assignment_list(context)
    context_names = [assignment.get("name") for assignment in context_assignments]
    valid_context_names = all(isinstance(name, str) for name in context_names)
    if not valid_context_names or len(context_names) != len(set(context_names)):
        errors.append("$.nodes: duplicate assignment names are forbidden")
    context_values = _assignment_values(context) if valid_context_names else {}
    ingress_ref = f"$('{_INGRESS_NAME}').item.json"
    expected_context_values = {
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
    if context_values != expected_context_values:
        errors.append("$.nodes: snapshot context fields are incomplete or literal")

    for name in expected["source_read_nodes"]:
        node = by_name.get(name, {})
        serialized = json.dumps(node.get("parameters", {}), ensure_ascii=False)
        if (
            node.get("type") != "n8n-nodes-base.code"
            or f"$('{_INGRESS_NAME}').first().json" not in serialized
            or "$input.first()" in serialized
        ):
            errors.append(f"$.nodes.{name}: source read is not snapshot-backed")

    for name in expected["profile_dependent_nodes"]:
        node = by_name.get(name, {})
        serialized = json.dumps(node.get("parameters", {}), ensure_ascii=False)
        if _CONTEXT_NAME not in serialized:
            errors.append(f"$.nodes.{name}: profile-dependent parameters lack snapshot context")

    model_ingress = f"$('{_INGRESS_NAME}').item.json"
    expected_model_expression = (
        "={{ (() => { "
        f"const providers = {model_ingress}.approved_provider_ids; "
        f"const models = {model_ingress}.approved_model_ids; "
        "if (!Array.isArray(providers) || !providers.includes('openrouter') || "
        "!Array.isArray(models) || models.length === 0) { "
        "throw new Error('approved provider/model IDs required'); "
        "} return models[0]; })() }}"
    )
    for node in nodes:
        parameters = node.get("parameters")
        model = parameters.get("model") if isinstance(parameters, dict) else None
        if ".lmChat" in str(node.get("type")) and model != expected_model_expression:
            errors.append(
                f"$.nodes.{node.get('name')}: approved model/provider expression differs"
            )

    serialized_parameters = json.dumps(
        [node.get("parameters", {}) for node in nodes], ensure_ascii=False
    )
    if "resultup" in serialized_parameters.lower():
        errors.append("$.nodes: company-specific fallback is forbidden")
    if "[REDACTED]" in serialized_parameters:
        errors.append("$.nodes: unresolved redacted placeholder is forbidden")
    if "$vars.FIRECRAWL_API_KEY" not in serialized_parameters:
        errors.append("$.nodes: FireCrawl provider authorization is not parameterized")
    firecrawl_ingress = f"$('{_INGRESS_NAME}').item.json"
    expected_firecrawl_authorization = (
        "={{ (() => { "
        f"const providers = {firecrawl_ingress}.approved_provider_ids; "
        "if (!Array.isArray(providers) || !providers.includes('firecrawl')) { "
        "throw new Error('approved firecrawl provider required'); "
        "} return 'Bearer ' + $vars.FIRECRAWL_API_KEY; })() }}"
    )
    try:
        firecrawl_authorization = by_name["FireCrawl"]["parameters"]["headerParameters"][
            "parameters"
        ][0]["value"]
    except (IndexError, KeyError, TypeError):
        firecrawl_authorization = None
    if firecrawl_authorization != expected_firecrawl_authorization:
        errors.append("$.nodes.FireCrawl: approved provider expression differs")

    final = by_name.get("Финальный вывод", {})
    final_assignments = _assignment_list(final)
    final_names = [assignment.get("name") for assignment in final_assignments]
    valid_final_names = all(isinstance(name, str) for name in final_names)
    if not valid_final_names or len(final_names) != len(set(final_names)):
        errors.append("$.nodes.Финальный вывод: duplicate assignment names are forbidden")
    final_values = _assignment_values(final) if valid_final_names else {}
    final_types = {
        assignment.get("name"): assignment.get("type")
        for assignment in final_assignments
        if isinstance(assignment.get("name"), str)
    }
    expected_final_types = {
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
    content_ref = "$('Редактура и проверка вписывания ссылок').item.json.output"
    metrics_ref = "$('Подсчет параметров текста').item.json"
    expected_final_values = {
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
    if (
        final.get("type") != "n8n-nodes-base.set"
        or final_values != expected_final_values
        or final_types != expected_final_types
    ):
        errors.append("$.nodes.Финальный вывод: structured result contract is incomplete")

    if workflow.get("active") is not False:
        errors.append("$.active: local transformed workflow must remain inactive")
    if _canonical_sha256(workflow) != expected["transformed_workflow_sha256"]:
        errors.append("$: workflow bytes differ from frozen universal contract")
    return ValidationReport(errors=tuple(errors))
