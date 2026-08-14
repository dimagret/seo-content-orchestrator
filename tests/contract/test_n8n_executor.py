from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

from seo_orchestrator.canonical import JsonValue, sha256_fingerprint
from seo_orchestrator.domain import ExecutionSnapshot, JobState, SeoJob
from seo_orchestrator.executors.base import ExecutorError, ExternalRun, ExternalStatus
from seo_orchestrator.executors.n8n import N8nExecutor
from seo_orchestrator.security.signatures import sign_request, verify_request
from tests.contract.mock_n8n_app import MockN8nApp

NOW = datetime(2026, 8, 14, 8, tzinfo=UTC)
KEY = bytes.fromhex("2b" * 32)
NONCE = "nonce-executor-0123456789"


def _compiled_context() -> dict[str, Any]:
    return {
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
            "goal": "test",
        },
        "prompt_set_version": 1,
    }


def _job() -> SeoJob:
    return SeoJob(
        job_id="job-one",
        brief_id="brief-one",
        brief_fingerprint=sha256_fingerprint(
            cast(JsonValue, _compiled_context()["brief"])
        ),
        snapshot_id="snapshot-one",
        snapshot_hash=_snapshot().snapshot_hash,
        company_id="company-one",
        direction_id="direction-one",
        audience_segment_id="audience-one",
        state=JobState.QUEUED,
        current_stage=None,
        approved_plan_fingerprint="c" * 64,
        approval_record_id="approval-one",
        attempt=1,
        created_at=NOW,
        started_at=None,
        finished_at=None,
        error_code=None,
        error_summary=None,
        artifact_manifest_path=None,
        company_profile_version=1,
        direction_version=1,
        audience_version=1,
        prompt_set_version=1,
    )


def _snapshot() -> ExecutionSnapshot:
    context = _compiled_context()
    return ExecutionSnapshot(
        snapshot_id="snapshot-one",
        brief_id="brief-one",
        company_id="company-one",
        company_profile_version=1,
        direction_id="direction-one",
        direction_version=1,
        audience_segment_id="audience-one",
        audience_version=1,
        prompt_set_version=1,
        compiled_context=context,
        snapshot_hash=sha256_fingerprint(cast(JsonValue, context)),
        created_at=NOW,
    )


def _executor(handler: Any) -> N8nExecutor:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return N8nExecutor(
        "https://n8n.test",
        KEY,
        client,
        clock=lambda: NOW,
        nonce_factory=lambda: NONCE,
        model_ids=("writer-model-v1",),
        provider_ids=("n8n-provider",),
    )


def _submit_bounded(executor: N8nExecutor) -> ExternalRun:
    return executor.submit_authorized(
        _job(),
        _snapshot(),
        authority_expires_at=datetime(2026, 8, 14, 8, 5, tzinfo=UTC),
        approved_model_ids=("writer-model-v1",),
        approved_provider_ids=("n8n-provider",),
    )


def test_submit_sends_signed_complete_execution_snapshot() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/v1/executions"
        assert request.headers["x-seo-idempotency-key"] == "company-one:job-one:1"
        body = request.content
        verify_request(
            "POST",
            "/v1/executions",
            int(request.headers["x-seo-timestamp"]),
            request.headers["x-seo-nonce"],
            body,
            KEY,
            request.headers["x-seo-signature"],
            now=int(NOW.timestamp()),
            nonce_consumer=lambda _nonce: True,
        )
        payload = json.loads(body)
        assert payload["job_id"] == "job-one"
        assert payload["brief_fingerprint"] == _job().brief_fingerprint
        assert payload["snapshot_hash"] == _snapshot().snapshot_hash
        assert payload["execution_snapshot"] == _snapshot().model_dump(mode="json")
        assert payload["authority_expires_at"] == "2026-08-14T08:05:00Z"
        assert payload["approved_model_ids"] == ["writer-model-v1"]
        assert payload["approved_provider_ids"] == ["n8n-provider"]
        return httpx.Response(
            202,
            json={
                "external_run_id": "n8n-run-one",
                "idempotency_key": "company-one:job-one:1",
                "accepted_at": "2026-08-14T08:00:01Z",
            },
        )

    run = _executor(handler).submit_authorized(
        _job(),
        _snapshot(),
        authority_expires_at=datetime(2026, 8, 14, 8, 5, tzinfo=UTC),
        approved_model_ids=("writer-model-v1",),
        approved_provider_ids=("n8n-provider",),
    )

    assert run.external_run_id == "n8n-run-one"
    assert run.accepted_at == datetime(2026, 8, 14, 8, 0, 1, tzinfo=UTC)


def test_executor_declares_required_durable_capabilities() -> None:
    executor = _executor(lambda _request: httpx.Response(500))

    assert executor.name == "n8n"
    assert executor.durable_semantic_idempotency is True
    assert executor.side_effect_free_lookup is True
    assert executor.idempotent_cancel is True
    assert executor.cancel_confirms_terminal is True
    assert executor.authority_deadline_enforced is True
    assert executor.configuration_authorization_enforced is True
    assert ExternalStatus.SUCCEEDED.value == "SUCCEEDED"


def _result_value() -> dict[str, object]:
    return {
        "content_markdown": "# Result\n",
        "titles": ["One", "Two", "Three", "Four", "Five"],
        "descriptions": ["One", "Two", "Three", "Four", "Five"],
        "keyword_qa": {"primary_keyword": "test", "occurrences": 1, "passed": True},
        "text_metrics": {"characters": 9},
        "sources": [],
        "warnings": [],
        "model_usage": {
            "models": [{"model_id": "writer-model-v1", "provider_id": "n8n-provider"}]
        },
        "stage_timings": {"writer": 1},
        "prompt_versions": {"writer": "v1"},
    }


def test_lookup_poll_and_cancel_are_signed_and_strictly_parsed() -> None:
    run_value = {
        "external_run_id": "n8n-run-one",
        "idempotency_key": "company-one:job-one:1",
        "accepted_at": "2026-08-14T08:00:01Z",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        verify_request(
            request.method,
            request.url.path,
            int(request.headers["x-seo-timestamp"]),
            request.headers["x-seo-nonce"],
            request.content,
            KEY,
            request.headers["x-seo-signature"],
            now=int(NOW.timestamp()),
            nonce_consumer=lambda _nonce: True,
        )
        if request.url.path == "/v1/executions/lookup":
            assert json.loads(request.content) == {
                "idempotency_key": "company-one:job-one:1"
            }
            return httpx.Response(200, json=run_value)
        if request.url.path == "/v1/executions/n8n-run-one":
            return httpx.Response(
                200,
                json={
                    "external_run_id": "n8n-run-one",
                    "status": "SUCCEEDED",
                    "stage_id": "complete",
                    "retry_after_seconds": None,
                    "error_code": None,
                    "error_summary": None,
                    "result": _result_value(),
                },
            )
        if request.url.path == "/v1/executions/n8n-run-one/cancel":
            return httpx.Response(
                200,
                json={
                    "external_run_id": "n8n-run-one",
                    "status": "CANCELED",
                    "stage_id": None,
                    "retry_after_seconds": None,
                    "error_code": None,
                    "error_summary": None,
                    "result": None,
                },
            )
        raise AssertionError(request.url.path)

    executor = _executor(handler)
    run = executor.lookup(_job(), _snapshot())
    assert run is not None
    assert executor.poll(run).status is ExternalStatus.SUCCEEDED
    assert executor.cancel(run).status is ExternalStatus.CANCELED


@pytest.mark.parametrize(
    "mutation",
    [
        {"status": "UNKNOWN"},
        {"hidden_reasoning": "do not expose"},
        {"result": {**_result_value(), "titles": ["Only one"]}},
        {"result": {**_result_value(), "descriptions": ["Only one"]}},
    ],
)
def test_poll_rejects_invalid_or_hidden_result_fields(mutation: dict[str, object]) -> None:
    payload: dict[str, object] = {
        "external_run_id": "n8n-run-one",
        "status": "SUCCEEDED",
        "stage_id": "complete",
        "retry_after_seconds": None,
        "error_code": None,
        "error_summary": None,
        "result": _result_value(),
    }
    payload.update(mutation)
    executor = _executor(lambda _request: httpx.Response(200, json=payload))
    run = ExternalRun("n8n-run-one", "company-one:job-one:1", NOW)

    with pytest.raises(ExecutorError) as failure:
        executor.poll(run)

    assert failure.value.error_code == "INVALID_RESPONSE"


def test_stateful_mock_proves_idempotency_lookup_and_terminal_cancel(tmp_path: Path) -> None:
    state_path = tmp_path / "mock-n8n.db"
    app = MockN8nApp(KEY, now=NOW, state_path=state_path)
    sequence = iter(f"nonce-sequence-{number:016d}" for number in range(10))
    executor = N8nExecutor(
        "https://n8n.test",
        KEY,
        httpx.Client(transport=httpx.MockTransport(app)),
        clock=lambda: NOW,
        nonce_factory=lambda: next(sequence),
    )

    first = executor.submit_authorized(
        _job(),
        _snapshot(),
        authority_expires_at=datetime(2026, 8, 14, 8, 5, tzinfo=UTC),
        approved_model_ids=("writer-model-v1",),
        approved_provider_ids=("n8n-provider",),
    )
    duplicate = executor.submit_authorized(
        _job(),
        _snapshot(),
        authority_expires_at=datetime(2026, 8, 14, 8, 5, tzinfo=UTC),
        approved_model_ids=("writer-model-v1",),
        approved_provider_ids=("n8n-provider",),
    )

    assert duplicate == first
    assert executor.lookup(_job(), _snapshot()) == first
    assert executor.poll(first).status is ExternalStatus.RUNNING

    cancel_path = "/v1/executions/n8n-run-1/cancel"
    for method, idempotency_key, nonce in (
        ("GET", first.idempotency_key, "nonce-wrong-method-0000000001"),
        ("POST", "company-one:other-job:1", "nonce-wrong-key-0000000000001"),
    ):
        timestamp = int(NOW.timestamp())
        response = app(
            httpx.Request(
                method,
                f"https://n8n.test{cancel_path}",
                headers={
                    "x-seo-timestamp": str(timestamp),
                    "x-seo-nonce": nonce,
                    "x-seo-idempotency-key": idempotency_key,
                    "x-seo-signature": sign_request(
                        method, cancel_path, timestamp, nonce, b"", KEY
                    ),
                },
                content=b"",
            )
        )
        assert response.status_code in {403, 405}
        assert executor.poll(first).status is ExternalStatus.RUNNING

    assert executor.cancel(first).status is ExternalStatus.CANCELED
    assert executor.cancel(first).status is ExternalStatus.CANCELED


def test_mock_restart_rejects_replay_and_expired_fresh_submit(tmp_path: Path) -> None:
    state_path = tmp_path / "mock-n8n-restart.db"
    deadline = datetime(2026, 8, 14, 8, 0, 1, tzinfo=UTC)
    first_app = MockN8nApp(KEY, now=NOW, state_path=state_path)
    first_executor = _executor(first_app)
    first = first_executor.submit_authorized(
        _job(),
        _snapshot(),
        authority_expires_at=deadline,
        approved_model_ids=("writer-model-v1",),
        approved_provider_ids=("n8n-provider",),
    )

    restarted = MockN8nApp(
        KEY,
        now=datetime(2026, 8, 14, 8, 0, 2, tzinfo=UTC),
        state_path=state_path,
    )
    with pytest.raises(ExecutorError):
        _executor(restarted).submit_authorized(
            _job(),
            _snapshot(),
            authority_expires_at=deadline,
            approved_model_ids=("writer-model-v1",),
            approved_provider_ids=("n8n-provider",),
        )
    fresh_nonce_executor = N8nExecutor(
        "https://n8n.test",
        KEY,
        httpx.Client(transport=httpx.MockTransport(restarted)),
        clock=lambda: NOW,
        nonce_factory=lambda: "nonce-fresh-after-restart-0001",
    )
    with pytest.raises(ExecutorError):
        fresh_nonce_executor.submit_authorized(
            _job(),
            _snapshot(),
            authority_expires_at=deadline,
            approved_model_ids=("writer-model-v1",),
            approved_provider_ids=("n8n-provider",),
        )

    lookup_executor = N8nExecutor(
        "https://n8n.test",
        KEY,
        httpx.Client(transport=httpx.MockTransport(restarted)),
        clock=lambda: datetime(2026, 8, 14, 8, 0, 2, tzinfo=UTC),
        nonce_factory=lambda: "nonce-lookup-after-restart-0001",
    )
    assert lookup_executor.lookup(_job(), _snapshot()) == first


@pytest.mark.parametrize(
    "forgery",
    [
        "missing_brief_id",
        "outer_hash",
        "compiled_context",
        "type_confusion",
        "recomputed_context_hash",
    ],
)
def test_mock_rejects_signed_schema_and_snapshot_forgeries(
    tmp_path: Path, forgery: str
) -> None:
    app = MockN8nApp(KEY, now=NOW, state_path=tmp_path / f"{forgery}.db")

    def forged_handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        if forgery == "missing_brief_id":
            payload.pop("brief_id")
        elif forgery == "outer_hash":
            payload["snapshot_hash"] = "0" * 64
        elif forgery == "compiled_context":
            payload["execution_snapshot"]["compiled_context"]["brief"]["goal"] = "forged"
        elif forgery == "type_confusion":
            payload["job_id"] = "x"
            payload["attempt"] = True
            payload["approval_record_id"] = "!"
        else:
            context = payload["execution_snapshot"]["compiled_context"]
            context["brief"]["goal"] = "forged"
            snapshot_hash = sha256_fingerprint(cast(JsonValue, context))
            payload["execution_snapshot"]["snapshot_hash"] = snapshot_hash
            payload["snapshot_hash"] = snapshot_hash
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        timestamp = int(request.headers["x-seo-timestamp"])
        nonce = request.headers["x-seo-nonce"]
        headers = dict(request.headers)
        headers["x-seo-signature"] = sign_request(
            request.method, request.url.path, timestamp, nonce, body, KEY
        )
        return app(httpx.Request(request.method, request.url, headers=headers, content=body))

    with pytest.raises(ExecutorError):
        _executor(forged_handler).submit_authorized(
            _job(),
            _snapshot(),
            authority_expires_at=datetime(2026, 8, 14, 8, 5, tzinfo=UTC),
            approved_model_ids=("writer-model-v1",),
            approved_provider_ids=("n8n-provider",),
        )


def test_universal_contract_freezes_security_capabilities_and_snapshot_fields() -> None:
    contract_path = Path(__file__).parents[2] / "integrations/n8n/universal-contract.json"
    contract = json.loads(contract_path.read_bytes())

    assert contract["additionalProperties"] is False
    assert set(contract["required"]) == set(contract["properties"])
    snapshot = contract["$defs"]["executionSnapshot"]
    assert snapshot["additionalProperties"] is False
    assert set(snapshot["required"]) == set(_snapshot().model_dump(mode="json"))
    wrapper = contract["x-seo-wrapper"]
    assert wrapper["canonicalInput"] == (
        "METHOD\\nPATH\\nUNIX_TIMESTAMP\\nNONCE\\nSHA256_HEX(BODY)"
    )
    assert wrapper["headers"] == [
        "X-SEO-Timestamp",
        "X-SEO-Nonce",
        "X-SEO-Idempotency-Key",
        "X-SEO-Signature",
    ]
    assert wrapper["maxTimestampSkewSeconds"] == 300
    assert wrapper["replayedNonceRejected"] is True
    assert wrapper["nonceStoreDurable"] is True
    assert wrapper["durableSemanticIdempotency"] is True
    assert wrapper["authorityDeadlineEnforced"] is True
    assert wrapper["cancelConfirmsTerminal"] is True


def test_executor_rejects_non_https_origin() -> None:
    with pytest.raises(ValueError, match="HTTPS origin"):
        N8nExecutor("http://n8n.test", KEY, httpx.Client())


@pytest.mark.parametrize("status_code", [301, 302, 307, 308])
def test_executor_never_follows_redirects_or_exposes_remote_body(status_code: int) -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            status_code,
            headers={"location": "https://attacker.test/steal"},
            content=b'{"secret":"must-not-leak"}',
        )

    with pytest.raises(ExecutorError) as failure:
        _submit_bounded(_executor(handler))

    assert calls == 1
    assert failure.value.error_code == "N8N_HTTP_ERROR"
    assert "must-not-leak" not in failure.value.error_summary


def test_executor_rejects_oversized_response() -> None:
    executor = _executor(
        lambda _request: httpx.Response(
            200,
            headers={"content-type": "application/json"},
            content=b"x" * (1_048_576 + 1),
        )
    )
    run = ExternalRun("n8n-run-one", "company-one:job-one:1", NOW)

    with pytest.raises(ExecutorError) as failure:
        executor.poll(run)

    assert failure.value.error_code == "RESPONSE_TOO_LARGE"


def test_submit_rejects_configuration_and_expired_authority_before_transport() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    executor = _executor(handler)
    with pytest.raises(ExecutorError) as unauthorized:
        executor.submit_authorized(
            _job(),
            _snapshot(),
            authority_expires_at=datetime(2026, 8, 14, 8, 5, tzinfo=UTC),
            approved_model_ids=("other-model",),
            approved_provider_ids=("n8n-provider",),
        )
    with pytest.raises(ExecutorError) as expired:
        executor.submit_authorized(
            _job(),
            _snapshot(),
            authority_expires_at=NOW,
            approved_model_ids=("writer-model-v1",),
            approved_provider_ids=("n8n-provider",),
        )

    assert unauthorized.value.error_code == "EXECUTOR_CONFIGURATION_UNAUTHORIZED"
    assert expired.value.error_code == "APPROVAL_EXPIRED"
    assert calls == 0


def test_n8n_submit_requires_bounded_authority_before_transport() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    executor = _executor(handler)
    with pytest.raises(ExecutorError) as unbounded:
        executor.submit(_job(), _snapshot())
    with pytest.raises(ExecutorError) as missing_deadline:
        executor.submit_authorized(
            _job(),
            _snapshot(),
            authority_expires_at=None,
            approved_model_ids=("writer-model-v1",),
            approved_provider_ids=("n8n-provider",),
        )

    assert unbounded.value.error_code == "APPROVAL_REQUIRED"
    assert missing_deadline.value.error_code == "APPROVAL_REQUIRED"
    assert calls == 0


def test_submit_rejects_naive_authority_deadline_before_transport() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    with pytest.raises(ValueError, match="timezone-aware"):
        _executor(handler).submit_authorized(
            _job(),
            _snapshot(),
            authority_expires_at=datetime(2026, 8, 14, 8, 5),  # noqa: DTZ001 - Negative test.
            approved_model_ids=("writer-model-v1",),
            approved_provider_ids=("n8n-provider",),
        )
    assert calls == 0


def test_executor_requires_json_response_content_type() -> None:
    executor = _executor(
        lambda _request: httpx.Response(
            200,
            content=b'{"external_run_id":"n8n-run-one"}',
        )
    )
    run = ExternalRun("n8n-run-one", "company-one:job-one:1", NOW)

    with pytest.raises(ExecutorError) as failure:
        executor.poll(run)

    assert failure.value.error_code == "INVALID_RESPONSE"


def test_run_response_rejects_duplicate_json_keys() -> None:
    body = (
        b'{"external_run_id":"n8n-run-one",'
        b'"external_run_id":"n8n-run-two",'
        b'"idempotency_key":"company-one:job-one:1",'
        b'"accepted_at":"2026-08-14T08:00:01Z"}'
    )
    executor = _executor(
        lambda _request: httpx.Response(
            202,
            headers={"content-type": "application/json"},
            content=body,
        )
    )

    with pytest.raises(ExecutorError) as failure:
        _submit_bounded(executor)

    assert failure.value.error_code == "INVALID_RESPONSE"


@pytest.mark.parametrize("payload", [b"\xff", b"[" * 2_000 + b"]" * 2_000])
@pytest.mark.parametrize("operation", ["submit", "poll"])
def test_malformed_or_deep_remote_json_is_normalized(
    payload: bytes, operation: str
) -> None:
    status_code = 202 if operation == "submit" else 200

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code,
            content=payload,
            headers={"content-type": "application/json"},
        )

    executor = _executor(handler)
    with pytest.raises(ExecutorError) as failure:
        if operation == "submit":
            _submit_bounded(executor)
        else:
            executor.poll(
                ExternalRun(
                    external_run_id="run-one",
                    idempotency_key="company-one:job-one:1",
                    accepted_at=NOW,
                )
            )
    assert failure.value.error_code == "INVALID_RESPONSE"
    assert "\\xff" not in str(failure.value)
    assert "[[[[" not in str(failure.value)
