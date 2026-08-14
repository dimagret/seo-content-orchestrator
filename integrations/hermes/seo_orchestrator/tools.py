"""Hermes handlers for the isolated SEO worker API."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any, Protocol
from urllib.parse import quote

from .client import WorkerClient, WorkerClientError


class _Client(Protocol):
    def request_json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        query: dict[str, str] | None = None,
    ) -> Any: ...

    def request_text(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        accepted_content_types: frozenset[str],
    ) -> str: ...


_SENSITIVE_KEYS = frozenset(
    {
        "accesstoken",
        "apikey",
        "authorization",
        "bearertoken",
        "chainofthought",
        "clientsecret",
        "cookie",
        "credential",
        "credentials",
        "hiddenreasoning",
        "idtoken",
        "password",
        "passwd",
        "privatekey",
        "rawproviderresponse",
        "reasoning",
        "refreshtoken",
        "secret",
        "setcookie",
        "systemprompt",
    }
)
_SENSITIVE_EXACT_KEYS = frozenset({"prompt", "userprompt"})
_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9-]{1,63}$")
_SENSITIVE_TEXT = re.compile(
    r"\bbearer\s+[A-Za-z0-9._~+/-]{6,}|"
    r"(?:sk|pk|rk|ak)[_-][A-Za-z0-9_-]{8,}|"
    r"(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})|"
    r"(?:AKIA|ASIA|AGPA|AIDA|ANPA|ANVA|AROA)[A-Z0-9]{16}|"
    r"AIza[0-9A-Za-z_-]{35}|GOCSPX-[A-Za-z0-9_-]{16,}|"
    r"(?:xox[baprs]-|xapp-)[A-Za-z0-9-]{10,}|"
    r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}|"
    r"-----BEGIN [A-Z0-9 ]*(?:PRIVATE KEY|CERTIFICATE)-----|"
    r"<\s*/?\s*(?:think|analysis|reasoning)\s*>|"
    r"\b(?:hidden[ _-]?reasoning|chain[ _-]?of[ _-]?thought)\s*[:=]|"
    r"\b(?:authorization|api[ _-]?(?:key|token)|(?:access|refresh|id|bearer)[ _-]?token|"
    r"client[ _-]?secret|credentials?|password|passwd|secret|set[ _-]?cookie|cookie|token)"
    r"\s*[:=]\s*(?:bearer\s+)?[^\s,;]{6,}",
    re.IGNORECASE,
)


def _safe_identifier(value: Any) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError("identifier is required")
    return quote(value, safe="")


def _contains_sensitive_key(value: Any) -> bool:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
            if (
                normalized in _SENSITIVE_EXACT_KEYS
                or any(marker in normalized for marker in _SENSITIVE_KEYS)
                or normalized.endswith(("token", "secret"))
                or _contains_sensitive_key(child)
            ):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_sensitive_key(child) for child in value)
    return isinstance(value, str) and _SENSITIVE_TEXT.search(value) is not None


def _execute(tool_name: str, args: dict[str, Any], client: _Client) -> Any:
    company_id = args.get("company_id")
    if tool_name == "seo_company_list":
        return client.request_json("GET", "/v1/companies")
    if tool_name == "seo_company_get":
        return client.request_json(
            "GET",
            f"/v1/companies/{_safe_identifier(company_id)}",
            query={"version": str(args["version"])},
        )
    if tool_name == "seo_company_save_draft":
        return client.request_json(
            "PATCH",
            f"/v1/companies/{_safe_identifier(company_id)}",
            payload={
                "company_id": company_id,
                "actor_id": args["actor_id"],
                "expected_current_version": args["expected_version"],
                "replacement": args["replacement"],
            },
        )
    if tool_name == "seo_brief_start":
        return client.request_json(
            "POST",
            "/v1/briefs",
            payload={"company_id": company_id, "actor_id": args["actor_id"]},
        )
    if tool_name == "seo_brief_update":
        replacement = args["replacement"]
        if not isinstance(replacement, dict):
            raise ValueError("replacement must be an object")
        if "replacement_company_id" in replacement:
            raise ValueError("cross-company brief replacement is not available")
        return client.request_json(
            "PATCH",
            f"/v1/briefs/{_safe_identifier(args.get('brief_id'))}",
            payload={
                **replacement,
                "company_id": company_id,
                "brief_id": args["brief_id"],
                "actor_id": args["actor_id"],
                "expected_version": args["expected_version"],
                "expected_profile_version": args["expected_profile_version"],
            },
        )
    if tool_name == "seo_brief_validate":
        return client.request_json(
            "POST",
            f"/v1/briefs/{_safe_identifier(args.get('brief_id'))}/validate",
            payload={
                "company_id": company_id,
                "actor_id": args["actor_id"],
                "expected_version": args["expected_version"],
                "expected_profile_version": args["expected_profile_version"],
            },
        )
    if tool_name == "seo_job_plan":
        return client.request_json(
            "POST",
            "/v1/jobs/plan",
            payload={
                "company_id": company_id,
                "snapshot_id": args["snapshot_id"],
                "execution_plan": args["execution_plan"],
            },
        )
    if tool_name == "seo_job_approve":
        return client.request_json(
            "POST",
            f"/v1/jobs/{_safe_identifier(args.get('job_id'))}/approve",
            payload={
                "company_id": company_id,
                "actor_id": args["actor_id"],
                "snapshot_hash": args["snapshot_hash"],
                "plan_fingerprint": args["plan_fingerprint"],
            },
        )
    if tool_name == "seo_job_status":
        return client.request_json(
            "GET",
            f"/v1/jobs/{_safe_identifier(args.get('job_id'))}",
            query={"company_id": str(company_id)},
        )
    if tool_name == "seo_job_cancel":
        return client.request_json(
            "POST",
            f"/v1/jobs/{_safe_identifier(args.get('job_id'))}/cancel",
            payload={"company_id": company_id, "expected_state": args["expected_state"]},
        )
    if tool_name == "seo_job_retry":
        return client.request_json(
            "POST",
            f"/v1/jobs/{_safe_identifier(args.get('job_id'))}/retry",
            payload={"company_id": company_id},
        )
    if tool_name == "seo_job_artifact":
        return client.request_text(
            "GET",
            f"/v1/jobs/{_safe_identifier(args.get('job_id'))}/artifacts/content",
            query={"company_id": str(company_id)},
            accepted_content_types=frozenset({"text/markdown"}),
        )
    raise WorkerClientError(
        "CAPABILITY_UNAVAILABLE", "worker capability is not available in this stage"
    )


def _render_error(error: WorkerClientError) -> str:
    details: dict[str, Any] = {"code": error.error_code, "message": str(error)}
    if error.status_code is not None:
        details["status_code"] = error.status_code
    if error.request_id is not None:
        details["request_id"] = error.request_id
    return json.dumps(
        {"ok": False, "error": details},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def build_handler(
    tool_name: str, *, client_factory: Callable[[], _Client] = WorkerClient
) -> Callable[..., str]:
    """Return one synchronous, bounded, fail-closed Hermes tool handler."""

    def handler(args: dict[str, Any], **kwargs: Any) -> str:
        del kwargs
        try:
            result = _execute(tool_name, args, client_factory())
            if _contains_sensitive_key(result):
                raise WorkerClientError(
                    "UNSAFE_WORKER_RESPONSE", "worker response was withheld by policy"
                )
        except WorkerClientError as error:
            return _render_error(error)
        except (KeyError, TypeError, ValueError):
            return _render_error(
                WorkerClientError("INVALID_ARGUMENTS", "tool arguments are invalid")
            )
        return json.dumps(
            {"ok": True, "data": result},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    return handler
