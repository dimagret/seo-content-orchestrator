"""Deterministic retry policy tests for the durable runner."""

import pytest

from seo_orchestrator.runner import RetryPolicy


def test_transient_failure_uses_exponential_backoff_with_injected_jitter() -> None:
    observed: list[float] = []

    def jitter(delay: float) -> float:
        observed.append(delay)
        return delay + 0.25

    policy = RetryPolicy(
        base_delay_seconds=2.0,
        maximum_delay_seconds=30.0,
        jitter=jitter,
    )

    assert [
        policy.delay_for("HTTP_503", retry_number=number)
        for number in (1, 2, 3, 4, 5, 6)
    ] == [2.25, 4.25, 8.25, 16.25, 30.0, 30.0]
    assert observed == [2.0, 4.0, 8.0, 16.0, 30.0, 30.0]


@pytest.mark.parametrize(
    ("error_code", "expected"),
    [
        ("NETWORK_ERROR", True),
        ("HTTP_429", True),
        ("HTTP_502", True),
        ("HTTP_503", True),
        ("HTTP_504", True),
        ("MODEL_OUTPUT_INVALID", False),
        ("BRIEF_INVALID", False),
        ("PROFILE_INVALID", False),
        ("URL_BLOCKED", False),
        ("APPROVAL_INVALID", False),
        ("SCHEMA_INVALID", False),
    ],
)
def test_retry_policy_classifies_only_frozen_transient_failures(
    error_code: str, expected: bool
) -> None:
    assert RetryPolicy().is_retryable(error_code) is expected


def test_huge_retry_number_is_bounded_before_exponentiation() -> None:
    policy = RetryPolicy(
        base_delay_seconds=2,
        maximum_delay_seconds=30,
        jitter=lambda delay: delay,
    )

    assert policy.delay_for("HTTP_503", retry_number=2000) == 30.0
