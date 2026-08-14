"""Exact Worker API mapping for Hermes SEO tool handlers."""

from __future__ import annotations

import json
from typing import Any


class FakeClient:
    def __init__(self, result: Any = None) -> None:
        self.result = {"safe": True} if result is None else result
        self.calls: list[tuple[str, str, str, Any, Any]] = []

    def request_json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        query: dict[str, str] | None = None,
    ) -> Any:
        self.calls.append(("json", method, path, payload, query))
        return self.result

    def request_text(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        accepted_content_types: frozenset[str],
    ) -> str:
        self.calls.append(("text", method, path, None, query))
        assert accepted_content_types == frozenset({"text/markdown"})
        return "# safe artifact"


ROUTE_CASES = (
    ("seo_company_list", {}, ("json", "GET", "/v1/companies", None, None)),
    (
        "seo_company_get",
        {"company_id": "company-1", "version": 2},
        ("json", "GET", "/v1/companies/company-1", None, {"version": "2"}),
    ),
    (
        "seo_company_save_draft",
        {
            "company_id": "company-1",
            "actor_id": "actor-1",
            "expected_version": 2,
            "replacement": {"name": "Replacement"},
        },
        (
            "json",
            "PATCH",
            "/v1/companies/company-1",
            {
                "company_id": "company-1",
                "actor_id": "actor-1",
                "expected_current_version": 2,
                "replacement": {"name": "Replacement"},
            },
            None,
        ),
    ),
    (
        "seo_brief_start",
        {"company_id": "company-1", "actor_id": "actor-1"},
        (
            "json",
            "POST",
            "/v1/briefs",
            {"company_id": "company-1", "actor_id": "actor-1"},
            None,
        ),
    ),
    (
        "seo_brief_update",
        {
            "company_id": "company-1",
            "brief_id": "brief-1",
            "actor_id": "actor-1",
            "expected_version": 7,
            "expected_profile_version": 3,
            "replacement": {"goal": "Improve discovery"},
        },
        (
            "json",
            "PATCH",
            "/v1/briefs/brief-1",
            {
                "company_id": "company-1",
                "brief_id": "brief-1",
                "actor_id": "actor-1",
                "expected_version": 7,
                "expected_profile_version": 3,
                "goal": "Improve discovery",
            },
            None,
        ),
    ),
    (
        "seo_brief_validate",
        {
            "company_id": "company-1",
            "brief_id": "brief-1",
            "actor_id": "actor-1",
            "expected_version": 2,
            "expected_profile_version": 1,
        },
        (
            "json",
            "POST",
            "/v1/briefs/brief-1/validate",
            {
                "company_id": "company-1",
                "actor_id": "actor-1",
                "expected_version": 2,
                "expected_profile_version": 1,
            },
            None,
        ),
    ),
    (
        "seo_job_plan",
        {"company_id": "company-1", "snapshot_id": "snapshot-1", "execution_plan": {}},
        (
            "json",
            "POST",
            "/v1/jobs/plan",
            {"company_id": "company-1", "snapshot_id": "snapshot-1", "execution_plan": {}},
            None,
        ),
    ),
    (
        "seo_job_approve",
        {
            "company_id": "company-1",
            "job_id": "job-1",
            "actor_id": "actor-1",
            "snapshot_hash": "a" * 64,
            "plan_fingerprint": "b" * 64,
        },
        (
            "json",
            "POST",
            "/v1/jobs/job-1/approve",
            {
                "company_id": "company-1",
                "actor_id": "actor-1",
                "snapshot_hash": "a" * 64,
                "plan_fingerprint": "b" * 64,
            },
            None,
        ),
    ),
    (
        "seo_job_status",
        {"company_id": "company-1", "job_id": "job-1"},
        ("json", "GET", "/v1/jobs/job-1", None, {"company_id": "company-1"}),
    ),
    (
        "seo_job_cancel",
        {"company_id": "company-1", "job_id": "job-1", "expected_state": "QUEUED"},
        (
            "json",
            "POST",
            "/v1/jobs/job-1/cancel",
            {"company_id": "company-1", "expected_state": "QUEUED"},
            None,
        ),
    ),
    (
        "seo_job_retry",
        {"company_id": "company-1", "job_id": "job-1"},
        (
            "json",
            "POST",
            "/v1/jobs/job-1/retry",
            {"company_id": "company-1"},
            None,
        ),
    ),
    (
        "seo_job_artifact",
        {"company_id": "company-1", "job_id": "job-1"},
        (
            "text",
            "GET",
            "/v1/jobs/job-1/artifacts/content",
            None,
            {"company_id": "company-1"},
        ),
    ),
)


def test_handlers_map_to_exact_worker_routes() -> None:
    from integrations.hermes.seo_orchestrator.tools import build_handler

    for tool_name, args, expected_call in ROUTE_CASES:
        client = FakeClient()
        handler = build_handler(tool_name, client_factory=lambda client=client: client)

        rendered = json.loads(handler(args))

        expected_data = "# safe artifact" if tool_name == "seo_job_artifact" else client.result
        assert rendered == {"ok": True, "data": expected_data}
        assert client.calls == [expected_call]


def test_future_export_tool_is_explicitly_unavailable_without_worker_call() -> None:
    from integrations.hermes.seo_orchestrator.tools import build_handler

    client = FakeClient()
    rendered = json.loads(
        build_handler("seo_export_approve", client_factory=lambda: client)(
            {
                "company_id": "company-1",
                "job_id": "job-1",
                "actor_id": "actor-1",
                "artifact_hash": "a" * 64,
                "plan_fingerprint": "b" * 64,
                "destination": {},
            }
        )
    )

    assert rendered["ok"] is False
    assert rendered["error"]["code"] == "CAPABILITY_UNAVAILABLE"
    assert client.calls == []


def test_handler_rejects_sensitive_worker_json() -> None:
    from integrations.hermes.seo_orchestrator.tools import build_handler

    client = FakeClient({"nested": {"raw_provider_response": "must not escape"}})
    rendered = json.loads(build_handler("seo_company_list", client_factory=lambda: client)({}))

    assert rendered["ok"] is False
    assert rendered["error"]["code"] == "UNSAFE_WORKER_RESPONSE"
    assert "must not escape" not in json.dumps(rendered)


def test_handler_rejects_sensitive_worker_keys_and_values() -> None:
    from integrations.hermes.seo_orchestrator.tools import build_handler

    for payload in (
        {"api_key": "credential-value"},
        {"api_key_value": "credential-value"},
        {"note": "Authorization: Bearer ***"},
        {"note": "sk-testcredential123"},
        {"note": "ghp_0123456789abcdefghijklmnopqrst"},
        {"note": "AKIA0123456789ABCDEF"},
        {"note": "-----BEGIN PRIVATE KEY-----"},
        {"note": "GOCSPX-0123456789abcdefghijkl"},
        {"note": "xapp-1-A1234567890-B1234567890-C1234567890"},
        {"note": "<analysis>private reasoning</analysis>"},
    ):
        client = FakeClient(payload)
        rendered = json.loads(
            build_handler(
                "seo_company_list", client_factory=lambda client=client: client
            )({})
        )
        assert rendered["ok"] is False
        assert rendered["error"]["code"] == "UNSAFE_WORKER_RESPONSE"
        assert "credential-value" not in json.dumps(rendered)


def test_job_handler_allows_prompt_set_version_metadata() -> None:
    from integrations.hermes.seo_orchestrator.tools import build_handler

    client = FakeClient(
        {
            "company_id": "company-1",
            "job_id": "job-1",
            "prompt_set_version": 7,
        }
    )
    output = build_handler("seo_job_status", client_factory=lambda: client)(
        {"company_id": "company-1", "job_id": "job-1"}
    )

    assert json.loads(output)["data"]["prompt_set_version"] == 7


def test_brief_handler_rejects_cross_company_replacement() -> None:
    from integrations.hermes.seo_orchestrator.tools import build_handler

    client = FakeClient()
    rendered = json.loads(
        build_handler("seo_brief_update", client_factory=lambda: client)(
            {
                "company_id": "company-1",
                "brief_id": "brief-1",
                "actor_id": "actor-1",
                "expected_version": 1,
                "replacement": {"replacement_company_id": "company-2"},
            }
        )
    )
    assert rendered["ok"] is False
    assert rendered["error"]["code"] == "INVALID_ARGUMENTS"
    assert client.calls == []


def test_brief_replacement_cannot_override_explicit_scope_or_version() -> None:
    from integrations.hermes.seo_orchestrator.tools import build_handler

    client = FakeClient()
    rendered = json.loads(
        build_handler("seo_brief_update", client_factory=lambda: client)(
            {
                "company_id": "company-1",
                "brief_id": "brief-1",
                "actor_id": "actor-1",
                "expected_version": 7,
                "expected_profile_version": 3,
                "replacement": {
                    "company_id": "company-2",
                    "brief_id": "brief-2",
                    "actor_id": "actor-2",
                    "expected_version": 99,
                    "expected_profile_version": 99,
                    "goal": "Safe goal",
                },
            }
        )
    )

    assert rendered["ok"] is True
    payload = client.calls[0][3]
    assert payload["company_id"] == "company-1"
    assert payload["brief_id"] == "brief-1"
    assert payload["actor_id"] == "actor-1"
    assert payload["expected_version"] == 7
    assert payload["expected_profile_version"] == 3


def test_handler_normalizes_client_errors_without_response_content() -> None:
    from integrations.hermes.seo_orchestrator.client import WorkerClientError
    from integrations.hermes.seo_orchestrator.tools import build_handler

    class FailingClient(FakeClient):
        def request_json(self, *args: Any, **kwargs: Any) -> Any:
            del args, kwargs
            raise WorkerClientError(
                "WORKER_HTTP_ERROR",
                "worker rejected the request",
                status_code=409,
                request_id="safe-request-id",
            )

    rendered = json.loads(
        build_handler("seo_company_list", client_factory=FailingClient)({})
    )

    assert rendered == {
        "ok": False,
        "error": {
            "code": "WORKER_HTTP_ERROR",
            "message": "worker rejected the request",
            "request_id": "safe-request-id",
            "status_code": 409,
        },
    }
