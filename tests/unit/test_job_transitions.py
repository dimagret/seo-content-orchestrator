import hashlib
from dataclasses import FrozenInstanceError

import pytest

from seo_orchestrator.domain.approvals import (
    ExecutionPlan,
    canonical_plan_bytes,
    deserialize_plan,
    fingerprint_plan,
    plan_mapping,
)
from seo_orchestrator.domain.jobs import ALLOWED_TRANSITIONS, JobState, is_transition_allowed

EXPECTED_STATES = (
    JobState.DRAFT,
    JobState.VALIDATED,
    JobState.PLANNED,
    JobState.AWAITING_PAID_APPROVAL,
    JobState.QUEUED,
    JobState.RUNNING,
    JobState.FAILED_RETRYABLE,
    JobState.FAILED_FINAL,
    JobState.CANCELED,
    JobState.SUCCEEDED,
    JobState.AWAITING_EXPORT_APPROVAL,
    JobState.EXPORTED,
)
EXPECTED_EDGES = frozenset(
    {
        (JobState.DRAFT, JobState.VALIDATED),
        (JobState.VALIDATED, JobState.PLANNED),
        (JobState.PLANNED, JobState.AWAITING_PAID_APPROVAL),
        (JobState.AWAITING_PAID_APPROVAL, JobState.QUEUED),
        (JobState.QUEUED, JobState.RUNNING),
        (JobState.RUNNING, JobState.SUCCEEDED),
        (JobState.SUCCEEDED, JobState.AWAITING_EXPORT_APPROVAL),
        (JobState.AWAITING_EXPORT_APPROVAL, JobState.EXPORTED),
        (JobState.QUEUED, JobState.FAILED_RETRYABLE),
        (JobState.RUNNING, JobState.FAILED_RETRYABLE),
        (JobState.FAILED_RETRYABLE, JobState.QUEUED),
        (JobState.QUEUED, JobState.CANCELED),
        (JobState.RUNNING, JobState.CANCELED),
        (JobState.RUNNING, JobState.FAILED_FINAL),
    }
)


def _plan(**changes: object) -> ExecutionPlan:
    values: dict[str, object] = {
        "pipeline_version": "pipeline-v1",
        "executor_name": "isolated-n8n",
        "model_ids": ("research-model", "writer-model"),
        "provider_ids": ("provider-a", "provider-b"),
        "maximum_retries": 2,
        "cost_currency": "USD",
        "cost_min_decimal": "1.25",
        "cost_max_decimal": "2.50",
        "unknown_cost_reasons": ("scrape volume varies",),
        "result_destination": "local-artifacts",
    }
    values.update(changes)
    return ExecutionPlan(**values)  # type: ignore[arg-type]


def test_job_state_contract_and_full_transition_oracle_are_exact() -> None:
    assert tuple(JobState) == EXPECTED_STATES
    assert ALLOWED_TRANSITIONS == EXPECTED_EDGES

    outcomes = {
        (source, target): is_transition_allowed(source, target)
        for source in JobState
        for target in JobState
    }

    assert len(outcomes) == 144
    assert sum(outcomes.values()) == 14
    assert {edge for edge, allowed in outcomes.items() if allowed} == EXPECTED_EDGES
    assert sum(not allowed for allowed in outcomes.values()) == 130


@pytest.mark.parametrize("terminal", [JobState.FAILED_FINAL, JobState.CANCELED, JobState.EXPORTED])
def test_terminal_states_have_no_outgoing_graph_edges(terminal: JobState) -> None:
    assert not any(is_transition_allowed(terminal, target) for target in JobState)


def test_succeeded_can_only_await_export_approval() -> None:
    allowed_targets = {
        target for target in JobState if is_transition_allowed(JobState.SUCCEEDED, target)
    }
    assert allowed_targets == {JobState.AWAITING_EXPORT_APPROVAL}
    assert not is_transition_allowed(JobState.SUCCEEDED, JobState.RUNNING)
    assert not is_transition_allowed(JobState.SUCCEEDED, JobState.FAILED_RETRYABLE)
    assert not is_transition_allowed(JobState.SUCCEEDED, JobState.FAILED_FINAL)


def test_execution_plan_uses_exact_canonical_mapping_and_round_trips() -> None:
    plan = _plan()

    assert plan_mapping(plan) == {
        "pipeline_version": "pipeline-v1",
        "executor_name": "isolated-n8n",
        "model_ids": ["research-model", "writer-model"],
        "provider_ids": ["provider-a", "provider-b"],
        "maximum_retries": 2,
        "cost_currency": "USD",
        "cost_min_decimal": "1.25",
        "cost_max_decimal": "2.50",
        "unknown_cost_reasons": ["scrape volume varies"],
        "result_destination": "local-artifacts",
    }
    payload = canonical_plan_bytes(plan)
    assert payload == (
        b'{"cost_currency":"USD","cost_max_decimal":"2.50",'
        b'"cost_min_decimal":"1.25","executor_name":"isolated-n8n",'
        b'"maximum_retries":2,"model_ids":["research-model","writer-model"],'
        b'"pipeline_version":"pipeline-v1","provider_ids":["provider-a","provider-b"],'
        b'"result_destination":"local-artifacts",'
        b'"unknown_cost_reasons":["scrape volume varies"]}'
    )
    assert fingerprint_plan(plan) == hashlib.sha256(payload).hexdigest()
    assert deserialize_plan(payload) == plan
    assert canonical_plan_bytes(deserialize_plan(payload)) == payload


def test_tuple_order_is_fingerprint_sensitive() -> None:
    plan = _plan()

    assert fingerprint_plan(_plan(model_ids=tuple(reversed(plan.model_ids)))) != fingerprint_plan(plan)
    assert fingerprint_plan(
        _plan(provider_ids=tuple(reversed(plan.provider_ids)))
    ) != fingerprint_plan(plan)
    assert fingerprint_plan(
        _plan(unknown_cost_reasons=("second", "first"))
    ) != fingerprint_plan(_plan(unknown_cost_reasons=("first", "second")))


@pytest.mark.parametrize("invalid", [True, -1, 1.0, "1", None])
def test_maximum_retries_is_a_strict_non_negative_integer(invalid: object) -> None:
    with pytest.raises((TypeError, ValueError), match="maximum_retries"):
        _plan(maximum_retries=invalid)


def test_execution_plan_is_frozen() -> None:
    plan = _plan()

    with pytest.raises(FrozenInstanceError):
        plan.pipeline_version = "changed"  # type: ignore[misc]
