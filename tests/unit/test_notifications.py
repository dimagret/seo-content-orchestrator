from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from seo_orchestrator.services.notifications import (
    DeliveryReceipt,
    NotificationConfig,
    NotificationEvent,
    NullNotificationSink,
    build_notification_sink,
)


def _event_payload() -> dict[str, object]:
    return {
        "event_id": "event-1",
        "job_id": "job-1",
        "event_type": "progress",
        "company_display_name": "АвтоМаляр",
        "direction_display_name": "Покраска автомобилей",
        "stage_id": "research",
        "completed_stage_count": 2,
        "attempt": 1,
        "elapsed_seconds": 17,
        "artifact_ready": False,
    }


def test_null_sink_suppresses_minimized_event_without_delivery() -> None:
    event = NotificationEvent.model_validate(_event_payload())

    receipt = NullNotificationSink().send(event)

    assert event.event_key == (
        "job-1",
        "progress",
        "research",
        1,
    )
    assert receipt.event_id == event.event_id
    assert receipt.destination == "null"
    assert receipt.delivered_at.tzinfo is UTC
    assert set(DeliveryReceipt.model_fields) == {
        "event_id",
        "destination",
        "delivered_at",
    }

@pytest.mark.parametrize(
    "unsafe_value",
    [
        "Authorization: Bearer ***",
        "Authorization: Basic ***",
        "Cookie: session=abcdef123456",
        "eyJhbG...ture",
        "«redacted:sk-…»",
        "postgresql://user:***@host/database",
        "<analysis>private reasoning</analysis>",
        "-----BEGIN PRIVATE KEY-----",
        "x" * 121,
    ],
)
def test_event_rejects_sensitive_or_unbounded_display_values(unsafe_value: str) -> None:
    payload = _event_payload()
    payload["company_display_name"] = unsafe_value

    with pytest.raises(ValidationError):
        NotificationEvent.model_validate(payload)


@pytest.mark.parametrize("safe_value", ["Secret Garden", "Token Studio"])
def test_event_allows_business_names_containing_ordinary_security_words(
    safe_value: str,
) -> None:
    payload = _event_payload()
    payload["company_display_name"] = safe_value

    event = NotificationEvent.model_validate(payload)

    assert event.company_display_name == safe_value


@pytest.mark.parametrize(
    "forbidden_field",
    ["prompt", "scraped_text", "credentials", "model_reasoning", "provider_output", "raw_source_body"],
)
def test_event_rejects_non_allowlisted_content_fields(forbidden_field: str) -> None:
    payload = _event_payload()
    payload[forbidden_field] = "must not cross boundary"

    with pytest.raises(ValidationError):
        NotificationEvent.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("artifact_ready", 0),
        ("artifact_ready", "false"),
        ("completed_stage_count", "2"),
        ("attempt", True),
        ("attempt", 101),
        ("elapsed_seconds", 17.0),
        ("company_display_name", " АвтоМаляр "),
    ],
)
def test_event_rejects_coercion_and_whitespace_normalization(
    field: str,
    value: object,
) -> None:
    payload = _event_payload()
    payload[field] = value

    with pytest.raises(ValidationError):
        NotificationEvent.model_validate(payload)


def test_completion_has_one_canonical_event_key_per_job() -> None:
    payload = {
        **_event_payload(),
        "event_type": "completed",
        "stage_id": None,
        "completed_stage_count": 5,
        "artifact_ready": True,
    }

    event = NotificationEvent.model_validate(payload)

    assert event.event_key == ("job-1", "completed", None, 1)
    with pytest.raises(ValidationError):
        NotificationEvent.model_validate({**payload, "stage_id": "research"})
    with pytest.raises(ValidationError):
        NotificationEvent.model_validate({**payload, "attempt": 2})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("destination", " seo-progress "),
        ("destination", "x" * 129),
        ("delivered_at", "2026-08-14T16:00:00Z"),
    ],
)
def test_delivery_receipt_rejects_coercion_and_string_stripping(
    field: str,
    value: object,
) -> None:
    payload: dict[str, object] = {
        "event_id": "event-1",
        "destination": "seo-progress",
        "delivered_at": datetime(2026, 8, 14, 16, tzinfo=UTC),
    }
    payload[field] = value

    with pytest.raises(ValidationError):
        DeliveryReceipt.model_validate(payload)


def test_default_notification_configuration_builds_null_sink() -> None:
    config = NotificationConfig()

    sink = build_notification_sink(config)

    assert isinstance(sink, NullNotificationSink)


def test_enabled_configuration_requires_fixed_secret_file_configuration() -> None:
    with pytest.raises(ValidationError):
        NotificationConfig(enabled=True)
    with pytest.raises(ValidationError):
        NotificationConfig.model_validate(
            {
                "enabled": False,
                "company_profile": {"webhook_url": "https://unsafe.invalid"},
            }
        )
