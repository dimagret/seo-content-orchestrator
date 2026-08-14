"""Signed, bounded HTTP adapter for the universal n8n wrapper contract."""

from __future__ import annotations

import json
import re
import secrets
from collections.abc import Callable
from datetime import UTC, datetime
from typing import cast
from urllib.parse import urlsplit

import httpx

from seo_orchestrator.canonical import MAX_CANONICAL_BYTES, JsonValue, canonical_json
from seo_orchestrator.domain import ExecutionSnapshot, SeoJob
from seo_orchestrator.executors.base import (
    ExecutionStatus,
    ExecutorError,
    ExternalRun,
    ExternalStatus,
    execution_result_from_bytes,
    submission_idempotency_key,
)
from seo_orchestrator.security.signatures import sign_request

_MAX_RESPONSE_BYTES = MAX_CANONICAL_BYTES
_RESPONSE_FIELDS = frozenset({"external_run_id", "idempotency_key", "accepted_at"})
_STATUS_FIELDS = frozenset(
    {
        "external_run_id",
        "status",
        "stage_id",
        "retry_after_seconds",
        "error_code",
        "error_summary",
        "result",
    }
)
_EXTERNAL_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


def _reject_json_constant(_value: str) -> object:
    raise ValueError("non-finite JSON number")


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON object key")
        value[key] = item
    return value


def _strict_json(payload: bytes) -> object:
    return json.loads(
        payload,
        object_pairs_hook=_unique_json_object,
        parse_constant=_reject_json_constant,
    )


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


class N8nExecutor:
    """Executor backed by a provider wrapper that enforces the frozen universal contract."""

    name = "n8n"

    def __init__(
        self,
        base_url: str,
        key: bytes,
        client: httpx.Client,
        *,
        clock: Callable[[], datetime] | None = None,
        nonce_factory: Callable[[], str] | None = None,
        model_ids: tuple[str, ...] = ("writer-model-v1",),
        provider_ids: tuple[str, ...] = ("n8n-provider",),
        timeout: httpx.Timeout | None = None,
    ) -> None:
        if type(base_url) is not str:
            raise TypeError("base_url must be a string")
        parsed = urlsplit(base_url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError("base_url must be an HTTPS origin")
        if type(key) is not bytes or len(key) != 32:
            raise ValueError("key must contain exactly 32 bytes")
        if not isinstance(client, httpx.Client):
            raise TypeError("client must be an httpx.Client")
        if not model_ids or any(type(value) is not str or not value for value in model_ids):
            raise ValueError("model_ids must contain non-empty strings")
        if not provider_ids or any(type(value) is not str or not value for value in provider_ids):
            raise ValueError("provider_ids must contain non-empty strings")
        self._base_url = base_url.rstrip("/")
        self._key = key
        self._client = client
        self._clock = clock or (lambda: datetime.now(UTC))
        self._nonce_factory = nonce_factory or (lambda: secrets.token_urlsafe(24))
        self.model_ids = model_ids
        self.provider_ids = provider_ids
        self._timeout = timeout or httpx.Timeout(connect=5.0, read=30.0, write=30.0, pool=5.0)

    durable_semantic_idempotency = True
    side_effect_free_lookup = True
    idempotent_cancel = True
    cancel_confirms_terminal = True
    authority_deadline_enforced = True
    configuration_authorization_enforced = True

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return value.astimezone(UTC)

    def _request(
        self,
        method: str,
        path: str,
        body: bytes,
        idempotency_key: str,
        *,
        expected_statuses: frozenset[int],
    ) -> tuple[int, bytes]:
        now = self._now()
        timestamp = int(now.timestamp())
        nonce = self._nonce_factory()
        signature = sign_request(method, path, timestamp, nonce, body, self._key)
        headers = {
            "content-type": "application/json",
            "accept": "application/json",
            "x-seo-timestamp": str(timestamp),
            "x-seo-nonce": nonce,
            "x-seo-idempotency-key": idempotency_key,
            "x-seo-signature": signature,
        }
        try:
            with self._client.stream(
                method,
                f"{self._base_url}{path}",
                content=body,
                headers=headers,
                timeout=self._timeout,
                follow_redirects=False,
            ) as response:
                if response.status_code not in expected_statuses:
                    self._raise_http_error(response.status_code)
                media_type = response.headers.get("content-type", "").split(";", 1)[0].lower()
                if response.status_code != 404 and media_type != "application/json":
                    raise ExecutorError(
                        error_code="INVALID_RESPONSE",
                        error_summary="n8n returned a non-JSON response",
                    )
                chunks: list[bytes] = []
                total = 0
                for chunk in response.iter_bytes():
                    total += len(chunk)
                    if total > _MAX_RESPONSE_BYTES:
                        raise ExecutorError(
                            error_code="RESPONSE_TOO_LARGE",
                            error_summary="n8n response exceeded the size limit",
                        )
                    chunks.append(chunk)
                return response.status_code, b"".join(chunks)
        except ExecutorError:
            raise
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise ExecutorError(
                error_code="NETWORK_ERROR",
                error_summary="n8n transport failed",
            ) from exc
        except httpx.HTTPError as exc:
            raise ExecutorError(
                error_code="NETWORK_ERROR",
                error_summary="n8n transport failed",
            ) from exc

    @staticmethod
    def _raise_http_error(status_code: int) -> None:
        retryable = status_code in {429, 502, 503, 504}
        raise ExecutorError(
            error_code=f"HTTP_{status_code}" if retryable else "N8N_HTTP_ERROR",
            error_summary="n8n wrapper rejected the request",
        )

    @staticmethod
    def _parse_run(payload: bytes, expected_key: str) -> ExternalRun:
        try:
            value = _strict_json(payload)
        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
            RecursionError,
            ValueError,
        ) as exc:
            raise ExecutorError(
                error_code="INVALID_RESPONSE",
                error_summary="n8n returned invalid JSON",
            ) from exc
        if type(value) is not dict or set(value) != _RESPONSE_FIELDS:
            raise ExecutorError(
                error_code="INVALID_RESPONSE",
                error_summary="n8n returned an invalid run response",
            )
        mapping = cast(dict[str, object], value)
        if mapping["idempotency_key"] != expected_key:
            raise ExecutorError(
                error_code="INVALID_RESPONSE",
                error_summary="n8n returned a mismatched run identity",
            )
        try:
            accepted_at = datetime.fromisoformat(cast(str, mapping["accepted_at"]))
            external_run_id = cast(str, mapping["external_run_id"])
            if _EXTERNAL_RUN_ID.fullmatch(external_run_id) is None:
                raise ValueError
            return ExternalRun(
                external_run_id=external_run_id,
                idempotency_key=expected_key,
                accepted_at=accepted_at,
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise ExecutorError(
                error_code="INVALID_RESPONSE",
                error_summary="n8n returned an invalid run response",
            ) from exc

    @staticmethod
    def _parse_status(payload: bytes, expected_run_id: str) -> ExecutionStatus:
        try:
            value = _strict_json(payload)
            if type(value) is not dict or set(value) != _STATUS_FIELDS:
                raise ValueError
            mapping = cast(dict[str, object], value)
            if mapping["external_run_id"] != expected_run_id:
                raise ValueError
            raw_result = mapping["result"]
            result = (
                execution_result_from_bytes(canonical_json(cast(JsonValue, raw_result)))
                if raw_result is not None
                else None
            )
            return ExecutionStatus(
                external_run_id=expected_run_id,
                status=ExternalStatus(cast(str, mapping["status"])),
                stage_id=cast(str | None, mapping["stage_id"]),
                retry_after_seconds=cast(int | None, mapping["retry_after_seconds"]),
                error_code=cast(str | None, mapping["error_code"]),
                error_summary=cast(str | None, mapping["error_summary"]),
                result=result,
            )
        except (KeyError, RecursionError, TypeError, UnicodeDecodeError, ValueError) as exc:
            raise ExecutorError(
                error_code="INVALID_RESPONSE",
                error_summary="n8n returned an invalid status response",
            ) from exc

    @staticmethod
    def _validate_identity(job: SeoJob, snapshot: ExecutionSnapshot) -> None:
        if (
            job.snapshot_id != snapshot.snapshot_id
            or job.snapshot_hash != snapshot.snapshot_hash
            or job.brief_id != snapshot.brief_id
            or job.company_id != snapshot.company_id
            or job.direction_id != snapshot.direction_id
            or job.audience_segment_id != snapshot.audience_segment_id
        ):
            raise ValueError("job and snapshot identity do not match")

    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        del job, snapshot
        raise ExecutorError(
            error_code="APPROVAL_REQUIRED",
            error_summary="n8n submission requires bounded approval authority",
        )

    def submit_authorized(
        self,
        job: SeoJob,
        snapshot: ExecutionSnapshot,
        *,
        authority_expires_at: datetime | None,
        approved_model_ids: tuple[str, ...],
        approved_provider_ids: tuple[str, ...],
    ) -> ExternalRun:
        self._validate_identity(job, snapshot)
        if authority_expires_at is None:
            raise ExecutorError(
                error_code="APPROVAL_REQUIRED",
                error_summary="n8n submission requires bounded approval authority",
            )
        if job.approved_plan_fingerprint is None or job.approval_record_id is None:
            raise ExecutorError(
                error_code="APPROVAL_REQUIRED",
                error_summary="n8n submission requires an approved immutable plan",
            )
        if self.model_ids != approved_model_ids or self.provider_ids != approved_provider_ids:
            raise ExecutorError(
                error_code="EXECUTOR_CONFIGURATION_UNAUTHORIZED",
                error_summary="executor provider/model configuration is not approved",
            )
        if self._now() >= authority_expires_at.astimezone(UTC):
            raise ExecutorError(
                error_code="APPROVAL_EXPIRED",
                error_summary="provider submission authority expired",
            )
        key = submission_idempotency_key(job)
        value: JsonValue = {
            "job_id": job.job_id,
            "brief_id": job.brief_id,
            "brief_fingerprint": job.brief_fingerprint,
            "snapshot_hash": job.snapshot_hash,
            "attempt": job.attempt,
            "approved_plan_fingerprint": job.approved_plan_fingerprint,
            "approval_record_id": job.approval_record_id,
            "authority_expires_at": _utc_text(authority_expires_at),
            "approved_model_ids": list(approved_model_ids),
            "approved_provider_ids": list(approved_provider_ids),
            "execution_snapshot": cast(JsonValue, snapshot.model_dump(mode="json")),
        }
        _status_code, response = self._request(
            "POST",
            "/v1/executions",
            canonical_json(value),
            key,
            expected_statuses=frozenset({202}),
        )
        run = self._parse_run(response, key)
        if run.accepted_at >= authority_expires_at.astimezone(UTC):
            raise ExecutorError(
                error_code="APPROVAL_EXPIRED",
                error_summary="provider accepted submission after authority expiry",
            )
        return run

    def lookup(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun | None:
        self._validate_identity(job, snapshot)
        key = submission_idempotency_key(job)
        status_code, response = self._request(
            "POST",
            "/v1/executions/lookup",
            canonical_json({"idempotency_key": key}),
            key,
            expected_statuses=frozenset({200, 404}),
        )
        return None if status_code == 404 else self._parse_run(response, key)

    def poll(self, run: ExternalRun) -> ExecutionStatus:
        if _EXTERNAL_RUN_ID.fullmatch(run.external_run_id) is None:
            raise ValueError("external run id is not canonical")
        _status_code, response = self._request(
            "GET",
            f"/v1/executions/{run.external_run_id}",
            b"",
            run.idempotency_key,
            expected_statuses=frozenset({200}),
        )
        return self._parse_status(response, run.external_run_id)

    def cancel(self, run: ExternalRun) -> ExecutionStatus:
        if _EXTERNAL_RUN_ID.fullmatch(run.external_run_id) is None:
            raise ValueError("external run id is not canonical")
        _status_code, response = self._request(
            "POST",
            f"/v1/executions/{run.external_run_id}/cancel",
            b"",
            run.idempotency_key,
            expected_statuses=frozenset({200}),
        )
        confirmation = self._parse_status(response, run.external_run_id)
        if confirmation.status is not ExternalStatus.CANCELED:
            raise ExecutorError(
                error_code="INVALID_RESPONSE",
                error_summary="n8n did not confirm terminal cancellation",
            )
        return confirmation
