"""Paid-execution plan contracts and deterministic fingerprints."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import cast

from seo_orchestrator.canonical import JsonValue, canonical_json
from seo_orchestrator.domain.jobs import JobState

_PLAN_FIELDS = frozenset(
    {
        "pipeline_version",
        "executor_name",
        "model_ids",
        "provider_ids",
        "maximum_retries",
        "cost_currency",
        "cost_min_decimal",
        "cost_max_decimal",
        "unknown_cost_reasons",
        "result_destination",
    }
)


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    pipeline_version: str
    executor_name: str
    model_ids: tuple[str, ...]
    provider_ids: tuple[str, ...]
    maximum_retries: int
    cost_currency: str | None
    cost_min_decimal: str | None
    cost_max_decimal: str | None
    unknown_cost_reasons: tuple[str, ...]
    result_destination: str

    def __post_init__(self) -> None:
        for field_name in ("pipeline_version", "executor_name", "result_destination"):
            if type(getattr(self, field_name)) is not str:
                raise TypeError(f"{field_name} must be a string")
        for field_name in ("model_ids", "provider_ids", "unknown_cost_reasons"):
            value = getattr(self, field_name)
            if type(value) is not tuple or any(type(item) is not str for item in value):
                raise TypeError(f"{field_name} must be a tuple of strings")
        if type(self.maximum_retries) is not int:
            raise TypeError("maximum_retries must be an integer")
        if self.maximum_retries < 0:
            raise ValueError("maximum_retries must be non-negative")
        for field_name in ("cost_currency", "cost_min_decimal", "cost_max_decimal"):
            value = getattr(self, field_name)
            if value is not None and type(value) is not str:
                raise TypeError(f"{field_name} must be a string or None")


@dataclass(frozen=True, slots=True)
class PlannedJob:
    job_id: str
    snapshot_id: str
    snapshot_hash: str
    plan: ExecutionPlan
    plan_fingerprint: str
    state: JobState


@dataclass(frozen=True, slots=True)
class ApprovalRecord:
    approval_record_id: str
    job_id: str
    approval_type: str
    snapshot_hash: str
    plan_fingerprint: str
    approved_by: str
    approved_at: datetime
    expires_at: datetime | None


def plan_mapping(plan: ExecutionPlan) -> dict[str, JsonValue]:
    """Return the exact frozen plan contract as canonical-JSON-compatible values."""
    return {
        "pipeline_version": plan.pipeline_version,
        "executor_name": plan.executor_name,
        "model_ids": list(plan.model_ids),
        "provider_ids": list(plan.provider_ids),
        "maximum_retries": plan.maximum_retries,
        "cost_currency": plan.cost_currency,
        "cost_min_decimal": plan.cost_min_decimal,
        "cost_max_decimal": plan.cost_max_decimal,
        "unknown_cost_reasons": list(plan.unknown_cost_reasons),
        "result_destination": plan.result_destination,
    }


def canonical_plan_bytes(plan: ExecutionPlan) -> bytes:
    """Serialize an execution plan to its exact canonical UTF-8 JSON bytes."""
    return canonical_json(plan_mapping(plan))


def fingerprint_plan(plan: ExecutionPlan) -> str:
    """Return the lowercase SHA-256 of the exact canonical plan bytes."""
    return hashlib.sha256(canonical_plan_bytes(plan)).hexdigest()


def _string_tuple(value: object, field_name: str) -> tuple[str, ...]:
    if type(value) is not list or any(type(item) is not str for item in value):
        raise ValueError(f"{field_name} must be a JSON array of strings")
    return tuple(cast(list[str], value))


def _required_string(value: object, field_name: str) -> str:
    if type(value) is not str:
        raise ValueError(f"{field_name} must be a string")
    return value


def _optional_string(value: object, field_name: str) -> str | None:
    if value is not None and type(value) is not str:
        raise ValueError(f"{field_name} must be a string or null")
    return value


def deserialize_plan(payload: bytes) -> ExecutionPlan:
    """Parse only the exact, unwrapped frozen plan payload contract."""
    if type(payload) is not bytes:
        raise TypeError("plan payload must be exact bytes")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("plan payload is not valid UTF-8 JSON") from exc
    if type(value) is not dict or set(value) != _PLAN_FIELDS:
        raise ValueError("plan payload must contain exactly the frozen plan fields")
    mapping = cast(dict[str, object], value)
    retries = mapping["maximum_retries"]
    if type(retries) is not int or retries < 0:
        raise ValueError("maximum_retries must be a non-negative integer")
    return ExecutionPlan(
        pipeline_version=_required_string(mapping["pipeline_version"], "pipeline_version"),
        executor_name=_required_string(mapping["executor_name"], "executor_name"),
        model_ids=_string_tuple(mapping["model_ids"], "model_ids"),
        provider_ids=_string_tuple(mapping["provider_ids"], "provider_ids"),
        maximum_retries=retries,
        cost_currency=_optional_string(mapping["cost_currency"], "cost_currency"),
        cost_min_decimal=_optional_string(mapping["cost_min_decimal"], "cost_min_decimal"),
        cost_max_decimal=_optional_string(mapping["cost_max_decimal"], "cost_max_decimal"),
        unknown_cost_reasons=_string_tuple(
            mapping["unknown_cost_reasons"], "unknown_cost_reasons"
        ),
        result_destination=_required_string(
            mapping["result_destination"], "result_destination"
        ),
    )
