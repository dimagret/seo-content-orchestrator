"""Deterministic local-only Stage B n8n deployment bundle builder."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from itertools import pairwise
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from integrations.n8n.transform import transform_workflow
from integrations.n8n.validate import validate_universal_workflow

_DEPLOYMENT_STATE = "LOCAL_MOCK_ONLY_NOT_CLOUD_VERIFIED"
_CRYPTO_CREDENTIAL = "__SELECT_CRYPTO_HMAC_CREDENTIAL__"
_DATA_TABLE_ID = "__CREATE_SEO_WRAPPER_REQUESTS_TABLE__"

_ROUTES = (
    ("Submit", "POST", "v1/executions", "upsert"),
    ("Lookup", "POST", "v1/executions/lookup", "get"),
    ("Poll", "GET", "v1/executions/:external_run_id", "get"),
    ("Cancel", "POST", "v1/executions/:external_run_id/cancel", "update"),
)


@dataclass(frozen=True)
class BundleValidationReport:
    """Static local validation result; deployability also requires Cloud gates."""

    errors: tuple[str, ...]
    unresolved_gates: tuple[str, ...]

    @property
    def is_valid(self) -> bool:
        return not self.errors

    @property
    def is_deployable(self) -> bool:
        return self.is_valid and not self.unresolved_gates


def _node_id(name: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"verexela:stage-b-wrapper:{name}"))


def _preflight_code(route: str, method: str, path: str) -> str:
    path_expression = (
        "'/v1/executions/' + request.params.external_run_id"
        if path.endswith(":external_run_id")
        else (
            "'/v1/executions/' + request.params.external_run_id + '/cancel'"
            if path.endswith(":external_run_id/cancel")
            else repr(f"/{path}")
        )
    )
    route_validation = {
        "Lookup": """if (!isPlainObject(body) || !exactKeys(body, ['idempotency_key']) || body.idempotency_key !== idempotencyKey) invalid();""",
        "Poll": """if (!isPlainObject(body) || Object.keys(body).length !== 0) invalid();""",
        "Cancel": """if (!isPlainObject(body) || Object.keys(body).length !== 0) invalid();""",
        "Submit": """const submitKeys = ['approval_record_id','approved_model_ids','approved_plan_fingerprint','approved_provider_ids','attempt','authority_expires_at','brief_fingerprint','brief_id','execution_snapshot','job_id','snapshot_hash'];
if (!isPlainObject(body) || !exactKeys(body, submitKeys)) invalid();
const identifier = value => typeof value === 'string' && /^[a-z0-9][a-z0-9-]{1,63}$/.test(value);
const sha256 = value => typeof value === 'string' && /^[0-9a-f]{64}$/.test(value);
const positiveInteger = value => Number.isSafeInteger(value) && value >= 1;
const identifierList = value => Array.isArray(value) && value.length > 0 && value.length === new Set(value).size && value.every(item => typeof item === 'string' && item.length > 0 && item.length <= 128);
if (!identifier(body.job_id) || !identifier(body.brief_id) || !sha256(body.brief_fingerprint) || !sha256(body.snapshot_hash) || !positiveInteger(body.attempt) || !sha256(body.approved_plan_fingerprint) || !identifier(body.approval_record_id) || !identifierList(body.approved_model_ids) || !identifierList(body.approved_provider_ids)) invalid();
if (!rfc3339DateTime(body.authority_expires_at)) invalid();
const authorityExpiresAt = Date.parse(body.authority_expires_at);
if (!Number.isFinite(authorityExpiresAt) || authorityExpiresAt <= Date.now()) invalid();
const expectedModelIds = ['__BIND_APPROVED_MODEL_IDS__'];
const expectedProviderIds = ['__BIND_APPROVED_PROVIDER_IDS__'];
if (JSON.stringify(body.approved_model_ids) !== JSON.stringify(expectedModelIds) || JSON.stringify(body.approved_provider_ids) !== JSON.stringify(expectedProviderIds)) invalid();
const snapshot = body.execution_snapshot;
const snapshotKeys = ['audience_segment_id','audience_version','brief_id','company_id','company_profile_version','compiled_context','created_at','direction_id','direction_version','prompt_set_version','snapshot_hash','snapshot_id'];
if (!isPlainObject(snapshot) || !exactKeys(snapshot, snapshotKeys)) invalid();
if (!identifier(snapshot.snapshot_id) || !identifier(snapshot.brief_id) || !identifier(snapshot.company_id) || !positiveInteger(snapshot.company_profile_version) || !identifier(snapshot.direction_id) || !positiveInteger(snapshot.direction_version) || !identifier(snapshot.audience_segment_id) || !positiveInteger(snapshot.audience_version) || !positiveInteger(snapshot.prompt_set_version) || !sha256(snapshot.snapshot_hash) || !rfc3339DateTime(snapshot.created_at)) invalid();
const context = snapshot.compiled_context;
const contextKeys = ['audience','brief','company','direction','prompt_set_version','schema_version'];
if (!isPlainObject(context) || !exactKeys(context, contextKeys) || context.schema_version !== 1 || context.prompt_set_version !== snapshot.prompt_set_version) invalid();
const {company, direction, audience, brief} = context;
if (![company, direction, audience, brief].every(isPlainObject)) invalid();
const contextHash = crypto.createHash('sha256').update(stableJson(context), 'utf8').digest('hex');
const briefHash = crypto.createHash('sha256').update(stableJson(brief), 'utf8').digest('hex');
const expectedIdempotencyKey = snapshot.company_id + ':' + body.job_id + ':' + body.attempt;
const authorized = idempotencyKey === expectedIdempotencyKey && body.brief_id === snapshot.brief_id && snapshot.brief_id === brief.brief_id && body.snapshot_hash === snapshot.snapshot_hash && snapshot.snapshot_hash === contextHash && body.brief_fingerprint === briefHash && company.company_id === snapshot.company_id && company.company_profile_version === snapshot.company_profile_version && direction.direction_id === snapshot.direction_id && direction.company_id === snapshot.company_id && direction.company_profile_version === snapshot.company_profile_version && direction.direction_version === snapshot.direction_version && audience.audience_segment_id === snapshot.audience_segment_id && audience.company_id === snapshot.company_id && audience.direction_id === snapshot.direction_id && audience.direction_version === snapshot.direction_version && audience.audience_version === snapshot.audience_version && brief.company_id === snapshot.company_id && brief.company_profile_version === snapshot.company_profile_version && brief.direction_id === snapshot.direction_id && brief.direction_version === snapshot.direction_version && brief.audience_segment_id === snapshot.audience_segment_id && brief.audience_version === snapshot.audience_version;
if (!authorized) throw new Error('unauthorized execution');""",
    }[route]
    template = """const crypto = require('crypto');
const request = $input.first().json;
const headers = Object.fromEntries(Object.entries(request.headers ?? {{}}).map(([key, value]) => [key.toLowerCase(), value]));
const invalid = () => { throw new Error('invalid request'); };
const isPlainObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const exactKeys = (value, expected) => isPlainObject(value) && JSON.stringify(Object.keys(value).sort()) === JSON.stringify([...expected].sort());
const rfc3339DateTime = value => {
  if (typeof value !== 'string') return false;
  const match = /^(\\d{4})-(\\d{2})-(\\d{2})[Tt](\\d{2}):(\\d{2}):(\\d{2})(?:\\.\\d+)?(?:[Zz]|[+-](\\d{2}):(\\d{2}))$/.exec(value);
  if (!match) return false;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const offsetHour = match[7] === undefined ? 0 : Number(match[7]);
  const offsetMinute = match[8] === undefined ? 0 : Number(match[8]);
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return year >= 1 && month >= 1 && month <= 12 && day >= 1 && day <= days[month - 1] && hour <= 23 && minute <= 59 && second <= 59 && offsetHour <= 23 && offsetMinute <= 59;
};
const stableValue = value => {
  if (Array.isArray(value)) return value.map(stableValue);
  if (isPlainObject(value)) return Object.fromEntries(Object.keys(value).sort().map(key => [key, stableValue(value[key])]));
  if (value === null || typeof value === 'string' || typeof value === 'boolean' || (typeof value === 'number' && Number.isSafeInteger(value))) return value;
  invalid();
};
const stableJson = value => JSON.stringify(stableValue(value));
const body = isPlainObject(request.body) ? request.body : {};
const timestampText = headers['x-seo-timestamp'];
const nonce = headers['x-seo-nonce'];
const idempotencyKey = headers['x-seo-idempotency-key'];
const providedSignature = headers['x-seo-signature'];
if (!/^\\d{10}$/.test(timestampText ?? '')) invalid();
const timestamp = Number(timestampText);
if (Math.abs(Math.floor(Date.now() / 1000) - timestamp) > 300) invalid();
if (!/^[A-Za-z0-9_-]{20,128}$/.test(nonce ?? '')) invalid();
if (typeof idempotencyKey !== 'string' || idempotencyKey.length < 3 || idempotencyKey.length > 192) invalid();
if (!/^[0-9a-f]{64}$/.test(providedSignature ?? '')) invalid();
if (!/^[0-9a-f]{64}$/.test(request.body_hash ?? '')) invalid();
__ROUTE_VALIDATION__
const requestHash = request.body_hash;
const nonceHash = crypto.createHash('sha256').update(nonce, 'utf8').digest('hex');
const idempotencyKeyHash = crypto.createHash('sha256').update(idempotencyKey, 'utf8').digest('hex');
const requestPath = __PATH_EXPRESSION__;
const canonicalInput = __METHOD__ + '\\n' + requestPath + '\\n' + timestampText + '\\n' + nonce + '\\n' + requestHash;
return [{json: {request_hash: requestHash, nonce_hash: nonceHash, idempotency_key: idempotencyKey, idempotency_key_hash: idempotencyKeyHash, provided_signature: providedSignature, canonical_input: canonicalInput, external_run_id: request.params?.external_run_id ?? null, accepted_at: new Date().toISOString(), retention_at: new Date(Date.now() + 86400000).toISOString()}}];"""
    return (
        template.replace("__ROUTE_VALIDATION__", route_validation)
        .replace("__PATH_EXPRESSION__", path_expression)
        .replace("__METHOD__", repr(method))
        .replace("{{}}", "{}")
    )


_VERIFY_CODE = """const crypto = require('crypto');
const item = $input.first().json;
const expected = Buffer.from(item.expected_signature ?? '', 'hex');
const provided = Buffer.from(item.provided_signature ?? '', 'hex');
if (expected.length !== 32 || provided.length !== 32 || !crypto.timingSafeEqual(expected, provided)) throw new Error('invalid request');
delete item.expected_signature;
delete item.provided_signature;
delete item.canonical_input;
delete item.request_body;
return [{json: item}];"""


_TABLE_FIELDS = (
    ("nonce_hash", "string"),
    ("idempotency_key_hash", "string"),
    ("request_hash", "string"),
    ("external_run_id", "string"),
    ("status", "string"),
    ("accepted_at", "dateTime"),
    ("retention_at", "dateTime"),
)


def _table_parameters(
    operation: str,
    *,
    conditions: tuple[tuple[str, str], ...] = (),
    values: dict[str, str] | None = None,
) -> dict[str, Any]:
    parameters: dict[str, Any] = {
        "resource": "row",
        "operation": operation,
        "dataTableId": {"__rl": True, "mode": "id", "value": _DATA_TABLE_ID},
    }
    if conditions:
        parameters.update(
            {
                "matchType": "allConditions",
                "filters": {
                    "conditions": [
                        {"keyName": name, "condition": "eq", "keyValue": expression}
                        for name, expression in conditions
                    ]
                },
            }
        )
    if values is not None:
        match_names = {name for name, _expression in conditions}
        parameters["columns"] = {
            "mappingMode": "defineBelow",
            "value": values,
            "schema": [
                {
                    "id": name,
                    "displayName": name,
                    "required": False,
                    "defaultMatch": name in match_names,
                    "display": True,
                    "type": field_type,
                    "canBeUsedToMatch": True,
                }
                for name, field_type in _TABLE_FIELDS
            ],
        }
    if operation == "get":
        parameters.update({"returnAll": False, "limit": 1})
    return parameters


def _preflight_ref(route: str, field: str) -> str:
    return "={{ $('" + route + " Preflight').first().json." + field + " }}"


def _nonce_nodes(route: str) -> list[dict[str, Any]]:
    return [
        {
            "name": f"{route} Nonce Lookup",
            "type": "n8n-nodes-base.dataTable",
            "typeVersion": 1.1,
            "alwaysOutputData": True,
            "parameters": _table_parameters(
                "get",
                conditions=(("nonce_hash", _preflight_ref(route, "nonce_hash")),),
            ),
        },
        {
            "name": f"{route} Nonce Gate",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "parameters": {
                "mode": "runOnceForAllItems",
                "jsCode": f"""const existing = $input.first().json;
if (existing.nonce_hash) throw new Error('nonce replay');
return [{{json: $('{route} Preflight').first().json}}];""",
            },
        },
        {
            "name": f"{route} Nonce Insert",
            "type": "n8n-nodes-base.dataTable",
            "typeVersion": 1.1,
            "parameters": _table_parameters(
                "insert",
                values={
                    "nonce_hash": "={{ $json.nonce_hash }}",
                    "accepted_at": "={{ $json.accepted_at }}",
                    "retention_at": "={{ $json.retention_at }}",
                },
            ),
        },
    ]


def _run_lookup(route: str, *, correlate_external_id: bool) -> dict[str, Any]:
    conditions = (
        [("external_run_id", _preflight_ref(route, "external_run_id"))]
        if correlate_external_id
        else [("idempotency_key_hash", _preflight_ref(route, "idempotency_key_hash"))]
    )
    return {
        "name": f"{route} Run Lookup",
        "type": "n8n-nodes-base.dataTable",
        "typeVersion": 1.1,
        "alwaysOutputData": True,
        "parameters": _table_parameters("get", conditions=tuple(conditions)),
    }


def _run_correlation(route: str) -> dict[str, Any]:
    return {
        "name": f"{route} Run Correlation",
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "parameters": {
            "mode": "runOnceForAllItems",
            "jsCode": f"""const row = $input.first().json;
if (!row.external_run_id) return [{{json: row}}];
const request = $('{route} Preflight').first().json;
if (row.idempotency_key_hash !== request.idempotency_key_hash) throw new Error('run correlation failed');
return [{{json: row}}];""",
        },
    }


def _response_code(route: str) -> str:
    if route == "Submit":
        return """const state = $('Submit Resolve Run').first().json;
if (state.response_code === 409) return [{json: {response_code: 409, response_body: {error: 'idempotency_conflict'}}}];
return [{json: {response_code: 202, response_body: {external_run_id: state.external_run_id, idempotency_key: $('Submit Preflight').first().json.idempotency_key, accepted_at: state.accepted_at}}}];"""
    if route == "Lookup":
        return """const row = $input.first().json;
if (!row.external_run_id) return [{json: {response_code: 404, response_body: {error: 'not_found'}}}];
return [{json: {response_code: 200, response_body: {external_run_id: row.external_run_id, idempotency_key: $('Lookup Preflight').first().json.idempotency_key, accepted_at: row.accepted_at}}}];"""
    if route == "Cancel":
        return """const state = $('Cancel Resolve Run').first().json;
if (!state.found) return [{json: {response_code: 404, response_body: {error: 'not_found'}}}];
return [{json: {response_code: 200, response_body: {external_run_id: state.external_run_id, status: 'CANCELED', stage_id: null, retry_after_seconds: null, error_code: null, error_summary: null, result: null}}}];"""
    return """const row = $input.first().json;
if (!row.external_run_id) return [{json: {response_code: 404, response_body: {error: 'not_found'}}}];
return [{json: {response_code: 200, response_body: {external_run_id: row.external_run_id, status: row.status, stage_id: null, retry_after_seconds: null, error_code: null, error_summary: null, result: null}}}];"""


def _route_tail(route: str) -> tuple[list[dict[str, Any]], list[tuple[str, str]]]:
    response = {
        "name": f"{route} Mock Response",
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "parameters": {"mode": "runOnceForAllItems", "jsCode": _response_code(route)},
    }
    respond = {
        "name": f"{route} Respond",
        "type": "n8n-nodes-base.respondToWebhook",
        "typeVersion": 1.5,
        "parameters": {
            "respondWith": "json",
            "responseBody": "={{ $json.response_body }}",
            "options": {"responseCode": "={{ $json.response_code }}"},
        },
    }
    lookup = _run_lookup(route, correlate_external_id=route in {"Poll", "Cancel"})
    if route == "Submit":
        resolve = {
            "name": "Submit Resolve Run",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "parameters": {
                "mode": "runOnceForAllItems",
                "jsCode": """const existing = $input.first().json;
const request = $('Submit Preflight').first().json;
if (existing.external_run_id && existing.request_hash !== request.request_hash) return [{json: {...existing, should_write: false, response_code: 409}}];
if (existing.external_run_id) return [{json: {...existing, should_write: false, response_code: 202}}];
return [{json: {...request, external_run_id: String($execution.id), status: 'RUNNING', should_write: true, response_code: 202}}];
// idempotency conflict; existing.external_run_id is preserved""",
            },
        }
        gate = {
            "name": "Submit Write?",
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.2,
            "parameters": {
                "conditions": {
                    "options": {"caseSensitive": True, "typeValidation": "strict"},
                    "conditions": [
                        {
                            "leftValue": "={{ $json.should_write }}",
                            "operator": {"type": "boolean", "operation": "true", "singleValue": True},
                        }
                    ],
                    "combinator": "and",
                }
            },
        }
        store = {
            "name": "Submit Request Store",
            "type": "n8n-nodes-base.dataTable",
            "typeVersion": 1.1,
            "parameters": _table_parameters(
                "upsert",
                conditions=(("idempotency_key_hash", "={{ $json.idempotency_key_hash }}"),),
                values={
                    "idempotency_key_hash": "={{ $json.idempotency_key_hash }}",
                    "request_hash": "={{ $json.request_hash }}",
                    "external_run_id": "={{ $json.external_run_id }}",
                    "status": "={{ $json.status }}",
                    "accepted_at": "={{ $json.accepted_at }}",
                    "retention_at": "={{ $json.retention_at }}",
                },
            ),
        }
        provider_gate = {
            "name": "Submit Provider Calls Allowed?",
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.2,
            "parameters": {
                "conditions": {
                    "options": {"caseSensitive": True, "typeValidation": "strict"},
                    "conditions": [
                        {
                            "leftValue": False,
                            "operator": {"type": "boolean", "operation": "true", "singleValue": True},
                        }
                    ],
                    "combinator": "and",
                }
            },
        }
        prepare = {
            "name": "Submit Prepare Universal Input",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "parameters": {
                "mode": "runOnceForAllItems",
                "jsCode": "return [{json: $('Submit Webhook').first().json.body}];",
            },
        }
        execute = {
            "name": "Submit Execute Universal (G4)",
            "type": "n8n-nodes-base.executeWorkflow",
            "typeVersion": 1.1,
            "parameters": {
                "source": "database",
                "workflowId": {
                    "__rl": True,
                    "mode": "id",
                    "value": "__IMPORT_UNIVERSAL_WORKFLOW_FIRST__",
                },
                "mode": "once",
                "options": {"waitForSubWorkflow": False},
            },
        }
        return [
            lookup,
            resolve,
            gate,
            store,
            provider_gate,
            prepare,
            execute,
            response,
            respond,
        ], [
            ("Submit Nonce Insert", "Submit Run Lookup"),
            ("Submit Run Lookup", "Submit Resolve Run"),
            ("Submit Resolve Run", "Submit Write?"),
            ("Submit Request Store", "Submit Provider Calls Allowed?"),
            ("Submit Prepare Universal Input", "Submit Execute Universal (G4)"),
            ("Submit Execute Universal (G4)", "Submit Mock Response"),
            ("Submit Mock Response", "Submit Respond"),
        ]
    if route == "Cancel":
        correlation = _run_correlation(route)
        resolve = {
            "name": "Cancel Resolve Run",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "parameters": {
                "mode": "runOnceForAllItems",
                "jsCode": """const row = $input.first().json;
if (!row.external_run_id) return [{json: {found: false}}];
return [{json: {...row, found: true, status: 'CANCELED'}}];""",
            },
        }
        gate = {
            "name": "Cancel Found?",
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.2,
            "parameters": {
                "conditions": {
                    "options": {"caseSensitive": True, "typeValidation": "strict"},
                    "conditions": [
                        {
                            "leftValue": "={{ $json.found }}",
                            "operator": {"type": "boolean", "operation": "true", "singleValue": True},
                        }
                    ],
                    "combinator": "and",
                }
            },
        }
        store = {
            "name": "Cancel Request Store",
            "type": "n8n-nodes-base.dataTable",
            "typeVersion": 1.1,
            "parameters": _table_parameters(
                "update",
                conditions=(
                    ("idempotency_key_hash", "={{ $json.idempotency_key_hash }}"),
                    ("external_run_id", "={{ $json.external_run_id }}"),
                ),
                values={
                    "external_run_id": "={{ $json.external_run_id }}",
                    "status": "CANCELED",
                },
            ),
        }
        return [lookup, correlation, resolve, gate, store, response, respond], [
            ("Cancel Nonce Insert", "Cancel Run Lookup"),
            ("Cancel Run Lookup", "Cancel Run Correlation"),
            ("Cancel Run Correlation", "Cancel Resolve Run"),
            ("Cancel Resolve Run", "Cancel Found?"),
            ("Cancel Request Store", "Cancel Mock Response"),
            ("Cancel Mock Response", "Cancel Respond"),
        ]
    if route == "Poll":
        correlation = _run_correlation(route)
        return [lookup, correlation, response, respond], [
            ("Poll Nonce Insert", "Poll Run Lookup"),
            ("Poll Run Lookup", "Poll Run Correlation"),
            ("Poll Run Correlation", "Poll Mock Response"),
            ("Poll Mock Response", "Poll Respond"),
        ]
    return [lookup, response, respond], [
        (f"{route} Nonce Insert", f"{route} Run Lookup"),
        (f"{route} Run Lookup", f"{route} Mock Response"),
        (f"{route} Mock Response", f"{route} Respond"),
    ]


def _connect(connections: dict[str, Any], source: str, target: str, output: int = 0) -> None:
    main = connections.setdefault(source, {"main": []})["main"]
    while len(main) <= output:
        main.append([])
    main[output].append({"node": target, "type": "main", "index": 0})


def _wrapper_graph() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    connections: dict[str, Any] = {}
    for row, (route, method, path, _table_operation) in enumerate(_ROUTES):
        y = row * 360
        definitions: list[dict[str, Any]] = [
            {
                "name": f"{route} Webhook",
                "type": "n8n-nodes-base.webhook",
                "typeVersion": 2,
                "parameters": {
                    "httpMethod": method,
                    "path": path,
                    "responseMode": "responseNode",
                    "options": {"rawBody": True},
                },
                "webhookId": _node_id(f"{route} webhook"),
            },
            {
                "name": f"{route} Body Hash",
                "type": "n8n-nodes-base.crypto",
                "typeVersion": 2,
                "parameters": {
                    "action": "hash",
                    "binaryData": True,
                    "binaryPropertyName": "data",
                    "type": "SHA256",
                    "dataPropertyName": "body_hash",
                    "encoding": "hex",
                },
            },
            {
                "name": f"{route} Preflight",
                "type": "n8n-nodes-base.code",
                "typeVersion": 2,
                "parameters": {
                    "mode": "runOnceForAllItems",
                    "jsCode": _preflight_code(route, method, path),
                },
            },
            {
                "name": f"{route} HMAC",
                "type": "n8n-nodes-base.crypto",
                "typeVersion": 2,
                "parameters": {
                    "action": "hmac",
                    "binaryData": False,
                    "type": "SHA256",
                    "value": "={{ $json.canonical_input }}",
                    "dataPropertyName": "expected_signature",
                    "encoding": "hex",
                },
                "credentials": {
                    "crypto": {"id": _CRYPTO_CREDENTIAL, "name": _CRYPTO_CREDENTIAL}
                },
            },
            {
                "name": f"{route} Verify",
                "type": "n8n-nodes-base.code",
                "typeVersion": 2,
                "parameters": {"mode": "runOnceForAllItems", "jsCode": _VERIFY_CODE},
            },
        ]
        definitions.extend(_nonce_nodes(route))
        tail, route_connections = _route_tail(route)
        definitions.extend(tail)
        for column, definition in enumerate(definitions):
            definition["id"] = _node_id(definition["name"])
            definition["position"] = [column * 240, y]
            nodes.append(definition)
        common_names = [definition["name"] for definition in definitions[:8]]
        for source, target in pairwise(common_names):
            _connect(connections, source, target)
        for source, target in route_connections:
            _connect(connections, source, target)
        if route == "Submit":
            _connect(connections, "Submit Write?", "Submit Request Store", 0)
            _connect(connections, "Submit Write?", "Submit Mock Response", 1)
            _connect(
                connections,
                "Submit Provider Calls Allowed?",
                "Submit Prepare Universal Input",
                0,
            )
            _connect(
                connections,
                "Submit Provider Calls Allowed?",
                "Submit Mock Response",
                1,
            )
        elif route == "Cancel":
            _connect(connections, "Cancel Found?", "Cancel Request Store", 0)
            _connect(connections, "Cancel Found?", "Cancel Mock Response", 1)
    return nodes, connections


def _wrapper_workflow(contract_version: int) -> dict[str, Any]:
    nodes, connections = _wrapper_graph()
    return {
        "name": "SEO STAGE B WRAPPER (G3 MOCK ONLY)",
        "active": False,
        "nodes": nodes,
        "connections": connections,
        "settings": {"executionOrder": "v1"},
        "pinData": {},
        "meta": {
            "x-seo-wrapper": {
                "contract_version": contract_version,
                "mode": "mock",
                "routes": [
                    {"method": "POST", "path": "v1/executions"},
                    {"method": "POST", "path": "v1/executions/lookup"},
                    {"method": "GET", "path": "v1/executions/:external_run_id"},
                    {
                        "method": "POST",
                        "path": "v1/executions/:external_run_id/cancel",
                    },
                ],
            }
        },
    }


def _data_table_contract() -> dict[str, Any]:
    return {
        "name": "seo_wrapper_requests",
        "columns": {
            "nonce_hash": "string",
            "idempotency_key_hash": "string",
            "request_hash": "string",
            "external_run_id": "string",
            "status": "string",
            "accepted_at": "dateTime",
            "retention_at": "dateTime",
        },
        "unique_constraints_required": [
            ["nonce_hash"],
            ["idempotency_key_hash"],
            ["external_run_id"],
        ],
        "privacy": "request identifiers and hashes only",
    }


def build_stage_b_bundle(
    source_workflow: dict[str, Any],
    universal_contract: dict[str, Any],
) -> dict[str, Any]:
    """Build an inactive, mock-only deployment bundle without mutating inputs."""

    source = copy.deepcopy(source_workflow)
    contract = copy.deepcopy(universal_contract)
    universal = transform_workflow(source)
    wrapper = _wrapper_workflow(contract["x-seo-wrapper"]["version"])
    table = _data_table_contract()
    return {
        "schema_version": 1,
        "deployment_state": _DEPLOYMENT_STATE,
        "universal_workflow": universal,
        "wrapper_workflow": wrapper,
        "data_table": table,
        "required_bindings": {
            "crypto_hmac_credential_id": "__SELECT_CRYPTO_HMAC_CREDENTIAL__",
            "data_table_id": "__CREATE_SEO_WRAPPER_REQUESTS_TABLE__",
            "universal_workflow_id": "__IMPORT_UNIVERSAL_WORKFLOW_FIRST__",
            "universal_workflow_import_name": "SEO UNIVERSAL STAGE B (G3)",
            "approved_model_ids": ["__BIND_APPROVED_MODEL_IDS__"],
            "approved_provider_ids": ["__BIND_APPROVED_PROVIDER_IDS__"],
        },
        "limitations": {
            "cloud_import_verified": False,
            "data_table_atomic_uniqueness_verified": False,
            "free_mock_probe_completed": False,
            "paid_provider_calls_allowed": False,
        },
    }


def validate_stage_b_bundle(bundle: dict[str, Any]) -> BundleValidationReport:
    """Validate the exact local-only bundle and report unresolved Cloud gates."""

    errors: list[str] = []
    expected_keys = {
        "schema_version",
        "deployment_state",
        "universal_workflow",
        "wrapper_workflow",
        "data_table",
        "required_bindings",
        "limitations",
    }
    if set(bundle) != expected_keys:
        errors.append("$: unexpected bundle keys")
    if bundle.get("schema_version") != 1:
        errors.append("$.schema_version: unsupported value")
    if bundle.get("deployment_state") != _DEPLOYMENT_STATE:
        errors.append("$.deployment_state: local fail-closed state required")

    universal = bundle.get("universal_workflow")
    if not isinstance(universal, dict):
        errors.append("$.universal_workflow: object required")
    else:
        universal_report = validate_universal_workflow(universal)
        errors.extend(f"$.universal_workflow: {error}" for error in universal_report.errors)

    expected_wrapper = _wrapper_workflow(contract_version=1)
    if bundle.get("wrapper_workflow") != expected_wrapper:
        errors.append("$.wrapper_workflow: bytes differ from local mock-only contract")
    if bundle.get("data_table") != _data_table_contract():
        errors.append("$.data_table: privacy-bounded schema differs")

    expected_bindings = {
        "crypto_hmac_credential_id": _CRYPTO_CREDENTIAL,
        "data_table_id": _DATA_TABLE_ID,
        "universal_workflow_id": "__IMPORT_UNIVERSAL_WORKFLOW_FIRST__",
        "universal_workflow_import_name": "SEO UNIVERSAL STAGE B (G3)",
        "approved_model_ids": ["__BIND_APPROVED_MODEL_IDS__"],
        "approved_provider_ids": ["__BIND_APPROVED_PROVIDER_IDS__"],
    }
    if bundle.get("required_bindings") != expected_bindings:
        errors.append("$.required_bindings: exact fail-closed placeholders required")

    expected_limitations = {
        "cloud_import_verified": False,
        "data_table_atomic_uniqueness_verified": False,
        "free_mock_probe_completed": False,
        "paid_provider_calls_allowed": False,
    }
    if bundle.get("limitations") != expected_limitations:
        errors.append("$.limitations: local-only safety gates differ")

    unresolved = (
        "cloud_import_verified",
        "data_table_atomic_uniqueness_verified",
        "free_mock_probe_completed",
        "crypto_hmac_credential_id",
        "data_table_id",
        "universal_workflow_id",
        "approved_model_ids",
        "approved_provider_ids",
    )
    return BundleValidationReport(errors=tuple(errors), unresolved_gates=unresolved)
