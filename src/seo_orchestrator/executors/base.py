"""Executor boundary shared by local mocks and external adapters."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol, cast

from seo_orchestrator.canonical import JsonValue, canonical_json
from seo_orchestrator.domain import ExecutionSnapshot, SeoJob
from seo_orchestrator.services.artifacts import ExecutionResult

_RESULT_FIELDS = frozenset(
    {
        "content_markdown",
        "titles",
        "descriptions",
        "keyword_qa",
        "text_metrics",
        "sources",
        "warnings",
        "model_usage",
        "stage_timings",
        "prompt_versions",
    }
)


class ExecutorError(RuntimeError):
    """Safe normalized executor failure suitable for durable retry decisions."""

    def __init__(
        self,
        *,
        error_code: str,
        error_summary: str,
        retry_after_seconds: int | None = None,
    ) -> None:
        for field_name, value in (
            ("error_code", error_code),
            ("error_summary", error_summary),
        ):
            if type(value) is not str or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        if retry_after_seconds is not None and (
            type(retry_after_seconds) is not int or retry_after_seconds < 0
        ):
            raise ValueError("retry_after_seconds must be a non-negative integer or None")
        super().__init__(error_summary)
        self.error_code = error_code
        self.error_summary = error_summary
        self.retry_after_seconds = retry_after_seconds


class ExternalStatus(StrEnum):
    """Normalized status values exposed by every executor."""

    ACCEPTED = "ACCEPTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_FINAL = "FAILED_FINAL"
    CANCELED = "CANCELED"


@dataclass(frozen=True, slots=True)
class ExternalRun:
    """Stable identity returned after an executor accepts a submission."""

    external_run_id: str
    idempotency_key: str
    accepted_at: datetime

    def __post_init__(self) -> None:
        for field_name in ("external_run_id", "idempotency_key"):
            value = getattr(self, field_name)
            if type(value) is not str or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        if (
            type(self.accepted_at) is not datetime
            or self.accepted_at.tzinfo is None
            or self.accepted_at.utcoffset() is None
        ):
            raise ValueError("accepted_at must be a timezone-aware datetime")
        object.__setattr__(self, "accepted_at", self.accepted_at.astimezone(UTC))


@dataclass(frozen=True, slots=True)
class ExecutionStatus:
    """Normalized poll response with an optional validated terminal result."""

    external_run_id: str
    status: ExternalStatus
    stage_id: str | None
    retry_after_seconds: int | None
    error_code: str | None
    error_summary: str | None
    result: ExecutionResult | None

    def __post_init__(self) -> None:
        if type(self.external_run_id) is not str or not self.external_run_id.strip():
            raise ValueError("external_run_id must be a non-empty string")
        if not isinstance(self.status, ExternalStatus):
            raise TypeError("status must be an ExternalStatus")
        for field_name in ("stage_id", "error_code", "error_summary"):
            value = getattr(self, field_name)
            if value is not None and (type(value) is not str or not value.strip()):
                raise ValueError(f"{field_name} must be None or a non-empty string")
        if self.retry_after_seconds is not None and (
            type(self.retry_after_seconds) is not int or self.retry_after_seconds < 0
        ):
            raise ValueError("retry_after_seconds must be a non-negative integer or None")
        if self.status is ExternalStatus.SUCCEEDED:
            if not isinstance(self.result, ExecutionResult):
                raise ValueError("SUCCEEDED status requires an execution result")
        elif self.result is not None:
            raise ValueError("only SUCCEEDED status may contain an execution result")
        if self.status in {ExternalStatus.FAILED_RETRYABLE, ExternalStatus.FAILED_FINAL}:
            if self.error_code is None or self.error_summary is None:
                raise ValueError("failed status requires an error code and summary")
        elif self.error_code is not None or self.error_summary is not None:
            raise ValueError("non-failed status cannot contain an error")


class Executor(Protocol):
    """Minimal execution boundary; implementations own transport details."""

    @property
    def name(self) -> str:
        """Immutable adapter identity matched against the approved execution plan."""
        ...

    @property
    def model_ids(self) -> tuple[str, ...]:
        """Immutable model identities authorized for this adapter configuration."""
        ...

    @property
    def provider_ids(self) -> tuple[str, ...]:
        """Immutable provider identities authorized for this adapter configuration."""
        ...

    @property
    def durable_semantic_idempotency(self) -> bool:
        """Whether submission dedupe survives adapter/process restart."""
        ...

    @property
    def side_effect_free_lookup(self) -> bool:
        """Whether lookup is guaranteed never to create external work."""
        ...

    @property
    def idempotent_cancel(self) -> bool:
        """Whether repeated cancel requests are safe after crash or timeout."""
        ...

    @property
    def cancel_confirms_terminal(self) -> bool:
        """Whether successful cancel confirms provider-terminal CANCELED."""
        ...

    @property
    def authority_deadline_enforced(self) -> bool:
        """Whether submit rejects provider acceptance at/after the supplied deadline."""
        ...

    @property
    def configuration_authorization_enforced(self) -> bool:
        """Whether submit enforces approved provider/model IDs at the side-effect boundary."""
        ...

    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun: ...

    def submit_authorized(
        self,
        job: SeoJob,
        snapshot: ExecutionSnapshot,
        *,
        authority_expires_at: datetime | None,
        approved_model_ids: tuple[str, ...],
        approved_provider_ids: tuple[str, ...],
    ) -> ExternalRun:
        """Submit with a provider-enforced exclusive authority deadline."""
        ...

    def lookup(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun | None:
        """Look up an existing attempt without creating external work."""
        ...

    def poll(self, run: ExternalRun) -> ExecutionStatus: ...

    def cancel(self, run: ExternalRun) -> ExecutionStatus:
        """Return only after provider-terminal CANCELED confirmation."""
        ...


def submission_idempotency_key(job: SeoJob) -> str:
    """Return one stable provider-global identity for one local execution attempt."""
    return f"{job.company_id}:{job.job_id}:{job.attempt}"


def execution_result_bytes(result: ExecutionResult) -> bytes:
    """Serialize one already-validated result to canonical durable bytes."""
    if not isinstance(result, ExecutionResult):
        raise TypeError("result must be an ExecutionResult")
    result.__post_init__()
    value: JsonValue = {
        "content_markdown": result.content_markdown,
        "titles": list(result.titles),
        "descriptions": list(result.descriptions),
        "keyword_qa": result.keyword_qa,
        "text_metrics": result.text_metrics,
        "sources": list(result.sources),
        "warnings": list(result.warnings),
        "model_usage": result.model_usage,
        "stage_timings": result.stage_timings,
        "prompt_versions": result.prompt_versions,
    }
    return canonical_json(value)


def execution_result_from_bytes(payload: bytes) -> ExecutionResult:
    """Hydrate exact canonical result bytes and re-run all artifact validators."""
    if type(payload) is not bytes:
        raise TypeError("result payload must be exact bytes")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("result payload is not valid UTF-8 JSON") from exc
    if type(value) is not dict or set(value) != _RESULT_FIELDS:
        raise ValueError("result payload must contain exactly the frozen result fields")
    mapping = cast(dict[str, object], value)
    for field_name in ("titles", "descriptions", "sources", "warnings"):
        if type(mapping[field_name]) is not list:
            raise ValueError(f"{field_name} must be a JSON array")
    result = ExecutionResult(
        content_markdown=cast(str, mapping["content_markdown"]),
        titles=tuple(cast(list[str], mapping["titles"])),  # type: ignore[arg-type]
        descriptions=tuple(cast(list[str], mapping["descriptions"])),  # type: ignore[arg-type]
        keyword_qa=cast(JsonValue, mapping["keyword_qa"]),
        text_metrics=cast(JsonValue, mapping["text_metrics"]),
        sources=tuple(cast(list[JsonValue], mapping["sources"])),
        warnings=tuple(cast(list[str], mapping["warnings"])),
        model_usage=cast(JsonValue, mapping["model_usage"]),
        stage_timings=cast(JsonValue, mapping["stage_timings"]),
        prompt_versions=cast(JsonValue, mapping["prompt_versions"]),
    )
    if execution_result_bytes(result) != payload:
        raise ValueError("result payload is not canonical")
    return result
