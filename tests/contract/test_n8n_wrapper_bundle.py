from __future__ import annotations

import copy
import hashlib
import json
import shutil
import subprocess
import time
from itertools import pairwise
from pathlib import Path
from typing import Any, cast

import pytest

from integrations.n8n.build_wrapper_bundle import render_bundle_json
from integrations.n8n.transform import transform_workflow
from integrations.n8n.wrapper import build_stage_b_bundle, validate_stage_b_bundle
from seo_orchestrator.canonical import JsonValue, canonical_json, sha256_fingerprint
from seo_orchestrator.security.signatures import sign_request

_ROOT = Path(__file__).parents[2]
_SOURCE = _ROOT / "integrations/n8n/source-workflow.json"
_CONTRACT = _ROOT / "integrations/n8n/universal-contract.json"
_BUNDLE = _ROOT / "integrations/n8n/stage-b-local-bundle.json"
_TEST_HMAC_KEY = bytes.fromhex("2b" * 32)


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_bytes())
    assert isinstance(value, dict)
    return value


def _submit_request() -> dict[str, Any]:
    context: dict[str, Any] = {
        "schema_version": 1,
        "company": {"company_id": "company-one", "company_profile_version": 1},
        "direction": {
            "direction_id": "direction-one",
            "company_id": "company-one",
            "company_profile_version": 1,
            "direction_version": 1,
        },
        "audience": {
            "audience_segment_id": "audience-one",
            "company_id": "company-one",
            "direction_id": "direction-one",
            "direction_version": 1,
            "audience_version": 1,
        },
        "brief": {
            "brief_id": "brief-one",
            "company_id": "company-one",
            "company_profile_version": 1,
            "direction_id": "direction-one",
            "direction_version": 1,
            "audience_segment_id": "audience-one",
            "audience_version": 1,
        },
        "prompt_set_version": 1,
    }
    snapshot_hash = sha256_fingerprint(context)
    body = {
        "job_id": "job-one",
        "brief_id": "brief-one",
        "brief_fingerprint": sha256_fingerprint(context["brief"]),
        "snapshot_hash": snapshot_hash,
        "attempt": 1,
        "approved_plan_fingerprint": "a" * 64,
        "approval_record_id": "approval-one",
        "authority_expires_at": "2099-01-01T00:00:00Z",
        "approved_model_ids": ["__BIND_APPROVED_MODEL_IDS__"],
        "approved_provider_ids": ["__BIND_APPROVED_PROVIDER_IDS__"],
        "execution_snapshot": {
            "snapshot_id": "snapshot-one",
            "brief_id": "brief-one",
            "company_id": "company-one",
            "company_profile_version": 1,
            "direction_id": "direction-one",
            "direction_version": 1,
            "audience_segment_id": "audience-one",
            "audience_version": 1,
            "prompt_set_version": 1,
            "compiled_context": context,
            "snapshot_hash": snapshot_hash,
            "created_at": "2026-08-20T00:00:00Z",
        },
    }
    raw = canonical_json(cast(JsonValue, body))
    timestamp = int(time.time())
    nonce = "nonce-wrapper-0123456789"
    return {
        "headers": {
            "x-seo-timestamp": str(timestamp),
            "x-seo-nonce": nonce,
            "x-seo-idempotency-key": "company-one:job-one:1",
            "x-seo-signature": sign_request(
                "POST", "/v1/executions", timestamp, nonce, raw, _TEST_HMAC_KEY
            ),
        },
        "body": body,
        "body_hash": hashlib.sha256(raw).hexdigest(),
        "params": {},
    }


def _run_submit_preflight(request: dict[str, Any]) -> subprocess.CompletedProcess[str]:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required to execute generated n8n Code node vectors")
    wrapper = build_stage_b_bundle(_load(_SOURCE), _load(_CONTRACT))["wrapper_workflow"]
    code = next(
        node["parameters"]["jsCode"]
        for node in wrapper["nodes"]
        if node["name"] == "Submit Preflight"
    )
    harness = (
        "global.$input={first:()=>({json:JSON.parse(process.argv[1])})};"
        f"(async()=>{{{code}\n}})()"
        ".then(value=>process.stdout.write(JSON.stringify(value)))"
        ".catch(error=>{console.error(error.message);process.exit(7);});"
    )
    return subprocess.run(
        [node, "-e", harness, json.dumps(request, separators=(",", ":"))],
        capture_output=True,
        check=False,
        text=True,
    )


def _run_code_node(
    name: str,
    input_json: dict[str, Any],
    *,
    references: dict[str, dict[str, Any]] | None = None,
) -> subprocess.CompletedProcess[str]:
    node_path = shutil.which("node")
    if node_path is None:
        pytest.skip("Node.js is required to execute generated n8n Code node vectors")
    wrapper = build_stage_b_bundle(_load(_SOURCE), _load(_CONTRACT))["wrapper_workflow"]
    code = next(
        node["parameters"]["jsCode"]
        for node in wrapper["nodes"]
        if node["name"] == name
    )
    context = json.dumps(
        {"input": input_json, "references": references or {}}, separators=(",", ":")
    )
    harness = (
        "const context=JSON.parse(process.argv[1]);"
        "global.$input={first:()=>({json:context.input})};"
        "global.$execution={id:'wrapper-execution-one'};"
        "global.$=name=>({first:()=>({json:context.references[name]})});"
        f"(async()=>{{{code}\n}})()"
        ".then(value=>process.stdout.write(JSON.stringify(value)))"
        ".catch(error=>{console.error(error.message);process.exit(7);});"
    )
    return subprocess.run(
        [node_path, "-e", harness, context],
        capture_output=True,
        check=False,
        text=True,
    )


def test_builder_is_deterministic_mock_only_and_preserves_inputs() -> None:
    source = _load(_SOURCE)
    contract = _load(_CONTRACT)
    source_before = copy.deepcopy(source)
    contract_before = copy.deepcopy(contract)

    first = build_stage_b_bundle(source, contract)
    second = build_stage_b_bundle(source, contract)

    assert first == second
    assert source == source_before
    assert contract == contract_before
    assert set(first) == {
        "schema_version",
        "deployment_state",
        "universal_workflow",
        "wrapper_workflow",
        "data_table",
        "required_bindings",
        "limitations",
    }
    assert first["schema_version"] == 1
    assert first["deployment_state"] == "LOCAL_MOCK_ONLY_NOT_CLOUD_VERIFIED"
    assert first["universal_workflow"] == transform_workflow(source)
    assert first["required_bindings"]["universal_workflow_import_name"] == (
        "SEO UNIVERSAL STAGE B (G3)"
    )
    assert first["universal_workflow"]["active"] is False
    assert first["wrapper_workflow"]["name"] == "SEO STAGE B WRAPPER (G3 MOCK ONLY)"
    assert first["wrapper_workflow"]["active"] is False

    routes = first["wrapper_workflow"]["meta"]["x-seo-wrapper"]["routes"]
    assert routes == [
        {"method": "POST", "path": "v1/executions"},
        {"method": "POST", "path": "v1/executions/lookup"},
        {"method": "GET", "path": "v1/executions/:external_run_id"},
        {"method": "POST", "path": "v1/executions/:external_run_id/cancel"},
    ]

    table = first["data_table"]
    assert table["name"] == "seo_wrapper_requests"
    assert table["unique_constraints_required"] == [
        ["nonce_hash"],
        ["idempotency_key_hash"],
        ["external_run_id"],
    ]
    assert set(table["columns"]) == {
        "nonce_hash",
        "idempotency_key_hash",
        "request_hash",
        "external_run_id",
        "status",
        "accepted_at",
        "retention_at",
    }
    assert first["limitations"] == {
        "cloud_import_verified": False,
        "data_table_atomic_uniqueness_verified": False,
        "free_mock_probe_completed": False,
        "paid_provider_calls_allowed": False,
    }


def test_wrapper_graph_enforces_auth_nonce_and_route_semantics() -> None:
    bundle = build_stage_b_bundle(_load(_SOURCE), _load(_CONTRACT))
    wrapper = bundle["wrapper_workflow"]
    nodes = {node["name"]: node for node in wrapper["nodes"]}

    route_names = ("Submit", "Lookup", "Poll", "Cancel")
    for route in route_names:
        webhook = nodes[f"{route} Webhook"]
        assert webhook["type"] == "n8n-nodes-base.webhook"
        assert webhook["typeVersion"] == 2
        assert webhook["parameters"]["responseMode"] == "responseNode"
        assert webhook["parameters"]["options"]["rawBody"] is True

        body_hash = nodes[f"{route} Body Hash"]
        assert body_hash["type"] == "n8n-nodes-base.crypto"
        assert body_hash["typeVersion"] == 2
        assert body_hash["parameters"] == {
            "action": "hash",
            "binaryData": True,
            "binaryPropertyName": "data",
            "type": "SHA256",
            "dataPropertyName": "body_hash",
            "encoding": "hex",
        }
        assert "credentials" not in body_hash

        preflight = nodes[f"{route} Preflight"]
        assert preflight["type"] == "n8n-nodes-base.code"
        preflight_code = preflight["parameters"]["jsCode"]
        assert "request.body_hash" in preflight_code
        assert "x-seo-timestamp" in preflight_code
        assert "x-seo-nonce" in preflight_code
        assert "x-seo-idempotency-key" in preflight_code
        assert "x-seo-signature" in preflight_code
        assert "300" in preflight_code
        assert "canonical_input" in preflight_code
        assert "idempotency_key" in preflight_code

        hmac = nodes[f"{route} HMAC"]
        assert hmac["type"] == "n8n-nodes-base.crypto"
        assert hmac["typeVersion"] == 2
        assert hmac["parameters"] == {
            "action": "hmac",
            "binaryData": False,
            "type": "SHA256",
            "value": "={{ $json.canonical_input }}",
            "dataPropertyName": "expected_signature",
            "encoding": "hex",
        }
        assert hmac["credentials"] == {
            "crypto": {
                "id": "__SELECT_CRYPTO_HMAC_CREDENTIAL__",
                "name": "__SELECT_CRYPTO_HMAC_CREDENTIAL__",
            }
        }

        verify = nodes[f"{route} Verify"]
        verify_code = verify["parameters"]["jsCode"]
        assert "timingSafeEqual" in verify_code
        assert "expected_signature" in verify_code
        assert "provided_signature" in verify_code

        nonce_lookup = nodes[f"{route} Nonce Lookup"]
        assert nonce_lookup["type"] == "n8n-nodes-base.dataTable"
        assert nonce_lookup["parameters"]["operation"] == "get"
        assert nonce_lookup["alwaysOutputData"] is True
        assert nonce_lookup["parameters"]["filters"]["conditions"][0][
            "keyName"
        ] == "nonce_hash"

        nonce_gate_code = nodes[f"{route} Nonce Gate"]["parameters"]["jsCode"]
        assert "nonce replay" in nonce_gate_code
        assert f"$('{route} Preflight')" in nonce_gate_code

        nonce_insert = nodes[f"{route} Nonce Insert"]
        assert nonce_insert["parameters"]["operation"] == "insert"
        assert nonce_insert["parameters"]["dataTableId"] == {
            "__rl": True,
            "mode": "id",
            "value": "__CREATE_SEO_WRAPPER_REQUESTS_TABLE__",
        }

        response = nodes[f"{route} Respond"]
        assert response["type"] == "n8n-nodes-base.respondToWebhook"
        assert response["parameters"]["respondWith"] == "json"

        common_chain = [
            f"{route} Webhook",
            f"{route} Body Hash",
            f"{route} Preflight",
            f"{route} HMAC",
            f"{route} Verify",
            f"{route} Nonce Lookup",
            f"{route} Nonce Gate",
            f"{route} Nonce Insert",
        ]
        for source, target in pairwise(common_chain):
            assert wrapper["connections"][source]["main"][0] == [
                {"node": target, "type": "main", "index": 0}
            ]

    submit_code = nodes["Submit Preflight"]["parameters"]["jsCode"]
    for marker in (
        "execution_snapshot",
        "snapshot_hash",
        "compiled_context",
        "brief_fingerprint",
        "authority_expires_at",
        "approved_model_ids",
        "approved_provider_ids",
        "expectedIdempotencyKey",
        "stableJson",
    ):
        assert marker in submit_code

    submit_resolve = nodes["Submit Resolve Run"]["parameters"]["jsCode"]
    assert "idempotency conflict" in submit_resolve
    assert "existing.external_run_id" in submit_resolve
    assert "$execution.id" in submit_resolve

    lookup_response = nodes["Lookup Mock Response"]["parameters"]["jsCode"]
    assert "response_code: 404" in lookup_response
    assert "response_code: 200" in lookup_response
    assert "__ECHO_REQUEST_HEADER_AT_BIND_TIME__" not in lookup_response

    for route in ("Poll", "Cancel"):
        conditions = nodes[f"{route} Run Lookup"]["parameters"]["filters"][
            "conditions"
        ]
        assert {condition["keyName"] for condition in conditions} == {
            "external_run_id",
        }
        correlation_code = nodes[f"{route} Run Correlation"]["parameters"][
            "jsCode"
        ]
        assert "run correlation failed" in correlation_code
        assert "idempotency_key_hash" in correlation_code
        assert wrapper["connections"][f"{route} Run Lookup"]["main"][0] == [
            {"node": f"{route} Run Correlation", "type": "main", "index": 0}
        ]
        correlation_target = (
            "Cancel Resolve Run" if route == "Cancel" else "Poll Mock Response"
        )
        assert wrapper["connections"][f"{route} Run Correlation"]["main"][0] == [
            {"node": correlation_target, "type": "main", "index": 0}
        ]

    table_expressions = [
        condition["keyValue"]
        for node in wrapper["nodes"]
        if node["type"] == "n8n-nodes-base.dataTable"
        for condition in node["parameters"].get("filters", {}).get("conditions", [])
    ]
    assert table_expressions
    assert all(
        expression.startswith("={{ ") and expression.endswith(" }}")
        for expression in table_expressions
    )

    cancel_columns = nodes["Cancel Request Store"]["parameters"]["columns"][
        "value"
    ]
    assert cancel_columns["external_run_id"] == "={{ $json.external_run_id }}"
    assert cancel_columns["status"] == "CANCELED"

    provider_gate = nodes["Submit Provider Calls Allowed?"]
    assert provider_gate["parameters"]["conditions"]["conditions"][0][
        "leftValue"
    ] is False
    execute_universal = nodes["Submit Execute Universal (G4)"]
    assert execute_universal["type"] == "n8n-nodes-base.executeWorkflow"
    assert execute_universal["parameters"]["workflowId"]["value"] == (
        "__IMPORT_UNIVERSAL_WORKFLOW_FIRST__"
    )
    assert wrapper["connections"]["Submit Provider Calls Allowed?"]["main"] == [
        [{"node": "Submit Prepare Universal Input", "type": "main", "index": 0}],
        [{"node": "Submit Mock Response", "type": "main", "index": 0}],
    ]


def test_submit_preflight_executes_schema_authority_and_snapshot_checks() -> None:
    request = _submit_request()

    valid = _run_submit_preflight(request)

    assert valid.returncode == 0, valid.stderr
    output = json.loads(valid.stdout)[0]["json"]
    assert output["idempotency_key"] == "company-one:job-one:1"
    assert output["request_hash"] == request["body_hash"]
    assert output["canonical_input"] == "\n".join(
        (
            "POST",
            "/v1/executions",
            request["headers"]["x-seo-timestamp"],
            request["headers"]["x-seo-nonce"],
            request["body_hash"],
        )
    )
    assert output["provided_signature"] == request["headers"]["x-seo-signature"]

    mutations: list[dict[str, Any]] = []

    changed_key = copy.deepcopy(request)
    changed_key["headers"]["x-seo-idempotency-key"] = "company-one:other-job:1"
    mutations.append(changed_key)

    mismatched_snapshot = copy.deepcopy(request)
    mismatched_snapshot["body"]["snapshot_hash"] = "b" * 64
    mutations.append(mismatched_snapshot)

    expired = copy.deepcopy(request)
    expired["body"]["authority_expires_at"] = "2020-01-01T00:00:00Z"
    mutations.append(expired)

    for invalid_date_time in (
        "2099-01-01",
        "01/01/2099",
        "2099-01-01T00:00:00",
    ):
        invalid_authority = copy.deepcopy(request)
        invalid_authority["body"]["authority_expires_at"] = invalid_date_time
        mutations.append(invalid_authority)

    invalid_created_at = copy.deepcopy(request)
    invalid_created_at["body"]["execution_snapshot"]["created_at"] = "2026-08-20"
    mutations.append(invalid_created_at)

    unapproved_model = copy.deepcopy(request)
    unapproved_model["body"]["approved_model_ids"] = ["other-model"]
    mutations.append(unapproved_model)

    extra_field = copy.deepcopy(request)
    extra_field["body"]["unexpected"] = True
    mutations.append(extra_field)

    for mutation in mutations:
        raw = canonical_json(mutation["body"])
        mutation["body_hash"] = hashlib.sha256(raw).hexdigest()
        rejected = _run_submit_preflight(mutation)
        assert rejected.returncode == 7


def test_generated_nonce_and_idempotency_code_executes_contract_vectors() -> None:
    preflight = {
        "idempotency_key": "company-one:job-one:1",
        "idempotency_key_hash": "1" * 64,
        "request_hash": "2" * 64,
        "nonce_hash": "3" * 64,
        "accepted_at": "2026-08-20T00:00:00Z",
        "retention_at": "2026-08-21T00:00:00Z",
    }

    fresh_nonce = _run_code_node(
        "Submit Nonce Gate",
        {},
        references={"Submit Preflight": preflight},
    )
    assert fresh_nonce.returncode == 0, fresh_nonce.stderr
    assert json.loads(fresh_nonce.stdout)[0]["json"] == preflight

    replay = _run_code_node(
        "Submit Nonce Gate",
        {"nonce_hash": preflight["nonce_hash"]},
        references={"Submit Preflight": preflight},
    )
    assert replay.returncode == 7
    assert "nonce replay" in replay.stderr

    first = _run_code_node(
        "Submit Resolve Run",
        {},
        references={"Submit Preflight": preflight},
    )
    assert first.returncode == 0, first.stderr
    first_state = json.loads(first.stdout)[0]["json"]
    assert first_state["external_run_id"] == "wrapper-execution-one"
    assert first_state["should_write"] is True
    assert first_state["response_code"] == 202

    existing = {
        **preflight,
        "external_run_id": "n8n-run-one",
        "status": "RUNNING",
    }
    duplicate = _run_code_node(
        "Submit Resolve Run",
        existing,
        references={"Submit Preflight": preflight},
    )
    duplicate_state = json.loads(duplicate.stdout)[0]["json"]
    assert duplicate_state["external_run_id"] == "n8n-run-one"
    assert duplicate_state["should_write"] is False
    assert duplicate_state["response_code"] == 202

    changed_request = {**preflight, "request_hash": "4" * 64}
    conflict = _run_code_node(
        "Submit Resolve Run",
        existing,
        references={"Submit Preflight": changed_request},
    )
    conflict_state = json.loads(conflict.stdout)[0]["json"]
    assert conflict_state["external_run_id"] == "n8n-run-one"
    assert conflict_state["should_write"] is False
    assert conflict_state["response_code"] == 409

    lookup_missing = _run_code_node(
        "Lookup Mock Response",
        {},
        references={"Lookup Preflight": preflight},
    )
    assert json.loads(lookup_missing.stdout)[0]["json"]["response_code"] == 404

    lookup_found = _run_code_node(
        "Lookup Mock Response",
        existing,
        references={"Lookup Preflight": preflight},
    )
    lookup_body = json.loads(lookup_found.stdout)[0]["json"]
    assert lookup_body["response_code"] == 200
    assert lookup_body["response_body"] == {
        "external_run_id": "n8n-run-one",
        "idempotency_key": "company-one:job-one:1",
        "accepted_at": "2026-08-20T00:00:00Z",
    }

    for route in ("Poll", "Cancel"):
        correlated = _run_code_node(
            f"{route} Run Correlation",
            existing,
            references={f"{route} Preflight": preflight},
        )
        assert correlated.returncode == 0, correlated.stderr

        wrong_header = _run_code_node(
            f"{route} Run Correlation",
            existing,
            references={
                f"{route} Preflight": {
                    **preflight,
                    "idempotency_key_hash": "9" * 64,
                }
            },
        )
        assert wrong_header.returncode == 7
        assert "run correlation failed" in wrong_header.stderr


def test_local_bundle_is_valid_but_not_deployable_before_cloud_bind_and_probe() -> None:
    bundle = build_stage_b_bundle(_load(_SOURCE), _load(_CONTRACT))

    report = validate_stage_b_bundle(bundle)

    assert report.is_valid is True
    assert report.is_deployable is False
    assert report.errors == ()
    assert report.unresolved_gates == (
        "cloud_import_verified",
        "data_table_atomic_uniqueness_verified",
        "free_mock_probe_completed",
        "crypto_hmac_credential_id",
        "data_table_id",
        "universal_workflow_id",
        "approved_model_ids",
        "approved_provider_ids",
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "activate_wrapper",
        "enable_paid_calls",
        "remove_raw_body",
        "replace_credential_placeholder",
        "store_company_id",
        "drift_universal_graph",
    ],
)
def test_validator_rejects_unsafe_or_drifted_bundle(mutation: str) -> None:
    bundle = build_stage_b_bundle(_load(_SOURCE), _load(_CONTRACT))
    if mutation == "activate_wrapper":
        bundle["wrapper_workflow"]["active"] = True
    elif mutation == "enable_paid_calls":
        bundle["limitations"]["paid_provider_calls_allowed"] = True
    elif mutation == "remove_raw_body":
        bundle["wrapper_workflow"]["nodes"][0]["parameters"]["options"][
            "rawBody"
        ] = False
    elif mutation == "replace_credential_placeholder":
        hmac_node = next(
            node
            for node in bundle["wrapper_workflow"]["nodes"]
            if node["name"] == "Submit HMAC"
        )
        hmac_node["credentials"]["crypto"]["id"] = (
            "credential-from-another-workspace"
        )
    elif mutation == "store_company_id":
        bundle["data_table"]["columns"]["company_id"] = "string"
    else:
        bundle["universal_workflow"]["nodes"][0]["position"][0] += 1

    report = validate_stage_b_bundle(bundle)

    assert report.is_valid is False
    assert report.is_deployable is False
    assert report.errors


def test_committed_bundle_is_byte_exact_reproducible_artifact() -> None:
    expected = render_bundle_json(_SOURCE, _CONTRACT)

    assert _BUNDLE.read_bytes() == expected
    committed = json.loads(expected)
    assert validate_stage_b_bundle(committed).is_valid is True
