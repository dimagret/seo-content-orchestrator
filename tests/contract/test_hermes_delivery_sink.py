from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

from seo_orchestrator.canonical import canonical_json
from seo_orchestrator.errors import CanonicalizationError
from seo_orchestrator.security.signatures import sign_request
from seo_orchestrator.services.notifications import (
    EventType,
    HermesWebhookSink,
    NotificationCapacityError,
    NotificationConfig,
    NotificationConflictError,
    NotificationDeliveryError,
    NotificationEvent,
    NotificationPrivacyError,
    build_notification_sink,
)

_KEY = bytes.fromhex("11" * 32)


def _key_file(tmp_path: Path) -> Path:
    path = tmp_path / "delivery-hmac.key"
    path.write_text(_KEY.hex(), encoding="ascii")
    os.chmod(path, 0o600)
    return path


def _event(*, event_type: EventType = "progress") -> NotificationEvent:
    return NotificationEvent(
        event_id="event-1",
        job_id="job-1",
        event_type=event_type,
        company_display_name="АвтоМаляр",
        direction_display_name="Покраска автомобилей",
        completed_stage_count=2,
        stage_id=None if event_type == "completed" else "research",
        attempt=1,
        elapsed_seconds=17,
        artifact_ready=False,
    )


class _Catalog:
    def __init__(self, *events: NotificationEvent) -> None:
        self._events = {event.event_id: event for event in events}

    def approve(self, event: NotificationEvent) -> None:
        self._events[event.event_id] = event

    def resolve(self, event_id: str) -> NotificationEvent | None:
        return self._events.get(event_id)


def _catalog(*events: NotificationEvent) -> _Catalog:
    return _Catalog(*(events or (_event(),)))


class _Resolver:
    def __init__(self, *answers: str) -> None:
        self.answers = answers
        self.queries: list[str] = []

    def resolve(self, hostname: str) -> tuple[str, ...]:
        self.queries.append(hostname)
        return self.answers


def test_webhook_sink_sends_one_fixed_signed_deliver_only_payload(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(202, json={"provider_detail": "must not enter receipt"})

    client = httpx.Client(transport=httpx.MockTransport(deliver))
    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=client,
        clock=lambda: 1_700_000_000,
        nonce_factory=lambda: "0123456789abcdef0123456789abcdef",
    )

    receipt = sink.send(_event())

    assert receipt.event_id == "event-1"
    assert receipt.delivered_at.isoformat() == "2023-11-14T22:13:20+00:00"
    assert receipt.destination == "seo-progress"
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert str(request.url) == "https://hermes.invalid/v1/deliver"
    assert request.headers["content-type"] == "application/json"
    assert request.headers["x-seo-timestamp"] == "1700000000"
    assert request.headers["x-seo-nonce"] == "0123456789abcdef0123456789abcdef"
    assert request.headers["idempotency-key"]
    assert request.headers["x-seo-payload-sha256"] == hashlib.sha256(
        request.content
    ).hexdigest()
    assert request.headers["x-seo-signature"] == sign_request(
        "POST",
        "/v1/deliver",
        1_700_000_000,
        "0123456789abcdef0123456789abcdef",
        request.content,
        _KEY,
    )
    payload = json.loads(request.content)
    assert set(payload) == {"destination", "event", "event_key"}
    assert set(payload["event"]) == {
        "event_id",
        "job_id",
        "event_type",
        "company_display_name",
        "direction_display_name",
        "completed_stage_count",
        "stage_id",
        "attempt",
        "elapsed_seconds",
        "artifact_ready",
    }
    assert "provider_detail" not in receipt.model_dump_json()
    assert _KEY.hex() not in request.content.decode("utf-8")


def test_sink_requires_webhook_host_in_explicit_trusted_allowlist(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="^notification webhook URL is invalid$"):
        HermesWebhookSink(
            webhook_url="https://hermes.invalid/v1/deliver",
            allowed_origins=("https://delivery.example",),
            hmac_key_path=_key_file(tmp_path),
            destination="seo-progress",
            approved_event_catalog=_catalog(),
            client=httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(204))),
        )


def test_sink_requires_approved_event_catalog_even_for_mock_destination(
    tmp_path: Path,
) -> None:
    with pytest.raises(TypeError, match="approved_event_catalog"):
        HermesWebhookSink(  # type: ignore[call-arg]
            webhook_url="https://hermes.invalid/v1/deliver",
            hmac_key_path=_key_file(tmp_path),
            destination="seo-progress",
        )


@pytest.mark.parametrize(
    ("webhook_url", "allowed_origins"),
    [
        ("https://delivery.example/v1/deliver", None),
        ("https://127.0.0.1/v1/deliver", ("https://127.0.0.1",)),
        ("https://2130706433/v1/deliver", ("https://2130706433",)),
        ("https://127.1/v1/deliver", ("https://127.1",)),
        ("https://0x7f000001/v1/deliver", ("https://0x7f000001",)),
        ("https://gateway.internal/v1/deliver", ("https://gateway.internal",)),
    ],
)
def test_sink_rejects_untrusted_or_internal_production_origins(
    tmp_path: Path,
    webhook_url: str,
    allowed_origins: tuple[str, ...] | None,
) -> None:
    class Catalog:
        def resolve(self, event_id: str) -> NotificationEvent | None:
            return None

    with pytest.raises(ValueError, match="^notification webhook URL is invalid$"):
        HermesWebhookSink(
            webhook_url=webhook_url,
            allowed_origins=allowed_origins,
            hmac_key_path=_key_file(tmp_path),
            destination="seo-progress",
            approved_event_catalog=Catalog(),
        )


@pytest.mark.parametrize(
    "answers",
    [
        ("127.0.0.1",),
        ("93.184.216.34", "10.0.0.8"),
    ],
)
def test_production_sink_rejects_private_or_mixed_dns_answers(
    tmp_path: Path,
    answers: tuple[str, ...],
) -> None:
    with pytest.raises(ValueError, match="^notification webhook URL is invalid$"):
        HermesWebhookSink(
            webhook_url="https://delivery.example/v1/deliver",
            allowed_origins=("https://delivery.example",),
            hmac_key_path=_key_file(tmp_path),
            destination="seo-progress",
            approved_event_catalog=_catalog(),
            resolver=_Resolver(*answers),
        )


def test_production_transport_pins_public_ip_and_preserves_host_and_sni(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []
    resolver = _Resolver("93.184.216.34")

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://delivery.example/v1/deliver",
        allowed_origins=("https://delivery.example",),
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        resolver=resolver,
        transport=httpx.MockTransport(deliver),
    )

    resolver.answers = ("127.0.0.1",)
    sink.send(_event())

    assert resolver.queries == ["delivery.example"]
    assert len(requests) == 1
    request = requests[0]
    assert request.url.host == "93.184.216.34"
    assert request.headers["host"] == "delivery.example"
    assert request.extensions["sni_hostname"] == "delivery.example"


def test_enabled_factory_uses_fixed_config_without_network_delivery(tmp_path: Path) -> None:
    class Catalog:
        def resolve(self, event_id: str) -> NotificationEvent | None:
            return None

    config = NotificationConfig(
        enabled=True,
        webhook_url="https://delivery.example/v1/deliver",
        allowed_origins=("https://delivery.example",),
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
    )

    with pytest.raises(
        ValueError,
        match="^enabled notifications require approved event catalog$",
    ):
        build_notification_sink(config)

    sink = build_notification_sink(
        config,
        approved_event_catalog=Catalog(),
        resolver=_Resolver("93.184.216.34"),
    )

    assert isinstance(sink, HermesWebhookSink)
    sink.close()


def test_close_waits_for_active_delivery_and_post_close_send_is_generic(
    tmp_path: Path,
) -> None:
    event = _event()
    entered = threading.Event()
    release = threading.Event()
    close_started = threading.Event()
    close_finished = threading.Event()

    class Catalog:
        def resolve(self, event_id: str) -> NotificationEvent | None:
            return event if event_id == event.event_id else None

    def blocked_delivery(request: httpx.Request) -> httpx.Response:
        entered.set()
        assert release.wait(timeout=2)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://delivery.example/v1/deliver",
        allowed_origins=("https://delivery.example",),
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=Catalog(),
        resolver=_Resolver("93.184.216.34"),
        transport=httpx.MockTransport(blocked_delivery),
    )

    def close_sink() -> None:
        close_started.set()
        sink.close()
        close_finished.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        delivery = executor.submit(sink.send, event)
        assert entered.wait(timeout=1)
        closing = executor.submit(close_sink)
        assert close_started.wait(timeout=1)
        assert not close_finished.wait(timeout=0.05)
        release.set()
        assert delivery.result(timeout=2).event_id == event.event_id
        closing.result(timeout=2)

    with pytest.raises(
        NotificationDeliveryError,
        match="^notification delivery failed$",
    ) as captured:
        sink.send(event)
    assert captured.value.__cause__ is None


def test_sink_rejects_event_not_matching_trusted_catalog(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    class Catalog:
        def resolve(self, event_id: str) -> NotificationEvent | None:
            assert event_id == "event-1"
            return NotificationEvent.model_validate(
                {
                    **_event().model_dump(mode="python"),
                    "company_display_name": "Approved company",
                    "direction_display_name": "Approved direction",
                }
            )

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        client=httpx.Client(
            transport=httpx.MockTransport(deliver)
        ),
        approved_event_catalog=Catalog(),
    )

    with pytest.raises(NotificationPrivacyError, match="^notification event not approved$"):
        sink.send(_event())

    assert requests == []


def test_sink_rejects_type_confusion_before_reading_event_methods(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    class AdversarialEvent:
        event_id = "event-1"
        event_key = ("job-1", "progress", "research", 1)

        def __ne__(self, other: object) -> bool:
            return False

        def model_dump(self, *, mode: str) -> dict[str, object]:
            return {"company_display_name": "TOP_SECRET"}

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(deliver)),
    )

    with pytest.raises(
        NotificationPrivacyError,
        match="^notification event not approved$",
    ):
        sink.send(AdversarialEvent())  # type: ignore[arg-type]

    assert requests == []


def test_sink_revalidates_exact_event_instances_before_catalog_comparison(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    class AdversarialStr(str):
        def __eq__(self, other: object) -> bool:
            raise AssertionError("reflected equality executed")

    event = _event()
    hostile = NotificationEvent.model_construct(
        **{
            **event.model_dump(mode="python"),
            "company_display_name": AdversarialStr("АвтоМаляр"),
        }
    )

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(event),
        client=httpx.Client(transport=httpx.MockTransport(deliver)),
    )

    with pytest.raises(
        NotificationPrivacyError,
        match="^notification event not approved$",
    ) as captured:
        sink.send(hostile)
    assert captured.value.__cause__ is None
    assert "reflected equality executed" not in repr(captured.value)
    assert requests == []


def test_sink_accepts_event_only_when_exact_trusted_catalog_record_matches(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []
    approved = _event()

    class Catalog:
        def resolve(self, event_id: str) -> NotificationEvent | None:
            assert event_id == "event-1"
            return approved

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        client=httpx.Client(transport=httpx.MockTransport(deliver)),
        approved_event_catalog=Catalog(),
    )

    receipt = sink.send(approved)

    assert receipt.event_id == approved.event_id
    assert receipt.destination == "seo-progress"
    assert len(requests) == 1


def test_catalog_failure_is_generic_and_never_reaches_transport(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    class Catalog:
        def resolve(self, event_id: str) -> NotificationEvent | None:
            raise RuntimeError("private catalog detail")

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        client=httpx.Client(transport=httpx.MockTransport(deliver)),
        approved_event_catalog=Catalog(),
    )

    with pytest.raises(
        NotificationPrivacyError,
        match="^notification event not approved$",
    ) as captured:
        sink.send(_event())

    assert captured.value.__cause__ is None
    assert "private catalog detail" not in repr(captured.value)
    assert requests == []


def test_successful_completion_is_delivered_once_per_destination(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []
    event = _event(event_type="completed")

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(event),
        client=httpx.Client(transport=httpx.MockTransport(deliver)),
        clock=lambda: 1_700_000_000,
        nonce_factory=lambda: "0123456789abcdef0123456789abcdef",
    )

    first = sink.send(event)
    second = sink.send(event)

    assert first.event_id == event.event_id
    assert second.event_id == event.event_id
    assert first.destination == second.destination == "seo-progress"
    assert len(requests) == 1


def test_same_event_key_rejects_a_changed_payload(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []
    event = _event()
    catalog = _catalog(event)

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=catalog,
        client=httpx.Client(transport=httpx.MockTransport(deliver)),
        clock=lambda: 1_700_000_000,
        nonce_factory=lambda: "0123456789abcdef0123456789abcdef",
    )
    changed = NotificationEvent.model_validate(
        {**event.model_dump(), "elapsed_seconds": event.elapsed_seconds + 1}
    )

    sink.send(event)
    catalog.approve(changed)
    with pytest.raises(NotificationConflictError, match="^notification event key conflict$"):
        sink.send(changed)

    assert len(requests) == 1


def test_receiver_conflict_preserves_payload_binding_across_sink_restart(
    tmp_path: Path,
) -> None:
    seen: dict[str, str] = {}
    user_visible_deliveries = 0
    lose_first_response = True
    event = _event()
    changed_event = NotificationEvent.model_validate(
        {
            **event.model_dump(mode="python"),
            "elapsed_seconds": 18,
        }
    )

    def receiver(request: httpx.Request) -> httpx.Response:
        nonlocal lose_first_response, user_visible_deliveries
        payload = json.loads(request.content)
        key = hashlib.sha256(
            canonical_json(
                {
                    "destination": payload["destination"],
                    "event_key": payload["event_key"],
                }
            )
        ).hexdigest()
        payload_hash = hashlib.sha256(request.content).hexdigest()
        assert request.headers["idempotency-key"] == key
        assert request.headers["x-seo-payload-sha256"] == payload_hash
        assert request.headers["x-seo-signature"] == sign_request(
            "POST",
            "/v1/deliver",
            int(request.headers["x-seo-timestamp"]),
            request.headers["x-seo-nonce"],
            request.content,
            _KEY,
        )
        existing = seen.get(key)
        if existing is not None:
            if existing == payload_hash:
                return httpx.Response(204)
            return httpx.Response(409)
        seen[key] = payload_hash
        user_visible_deliveries += 1
        if lose_first_response:
            lose_first_response = False
            raise httpx.ReadTimeout("response lost after durable acceptance", request=request)
        return httpx.Response(204)

    transport = httpx.MockTransport(receiver)
    first_sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(event),
        client=httpx.Client(transport=transport),
    )
    restarted_sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(event),
        client=httpx.Client(transport=transport),
    )
    changed_sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(changed_event),
        client=httpx.Client(transport=transport),
    )

    with pytest.raises(NotificationDeliveryError, match="^notification delivery failed$"):
        first_sink.send(event)
    restarted_sink.send(event)
    with pytest.raises(NotificationConflictError, match="^notification event payload conflict$"):
        changed_sink.send(changed_event)

    assert user_visible_deliveries == 1
    assert len(seen) == 1


def test_event_tracking_is_bounded_and_fails_before_delivery(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []
    first = _event()
    second = NotificationEvent.model_validate(
        {**first.model_dump(), "event_id": "event-2", "job_id": "job-2"}
    )

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(first, second),
        client=httpx.Client(transport=httpx.MockTransport(deliver)),
        clock=lambda: 1_700_000_000,
        nonce_factory=lambda: "0123456789abcdef0123456789abcdef",
        max_tracked_events=1,
    )

    sink.send(first)
    with pytest.raises(NotificationCapacityError, match="^notification capacity exhausted$"):
        sink.send(second)

    assert len(requests) == 1


def test_sink_generates_unique_csprng_nonces_by_default(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []
    first = _event()
    second = NotificationEvent.model_validate(
        {**first.model_dump(), "event_id": "event-2", "job_id": "job-2"}
    )

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(first, second),
        client=httpx.Client(transport=httpx.MockTransport(deliver)),
        clock=lambda: 1_700_000_000,
    )

    sink.send(first)
    sink.send(second)

    nonces = [request.headers["x-seo-nonce"] for request in requests]
    assert all(re.fullmatch(r"[0-9a-f]{32}", nonce) for nonce in nonces)
    assert len(set(nonces)) == 2


def test_retry_after_timeout_reuses_idempotency_key(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []
    nonces = iter(
        [
            "0123456789abcdef0123456789abcdef",
            "fedcba9876543210fedcba9876543210",
        ]
    )

    def timeout_then_deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            raise httpx.ReadTimeout("private provider timeout detail", request=request)
        return httpx.Response(202)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(timeout_then_deliver)),
        clock=lambda: 1_700_000_000,
        nonce_factory=lambda: next(nonces),
    )
    event = _event()

    with pytest.raises(
        NotificationDeliveryError,
        match="^notification delivery failed$",
    ) as captured:
        sink.send(event)
    assert captured.value.__cause__ is None
    assert "private provider timeout detail" not in repr(captured.value)
    receipt = sink.send(event)

    assert receipt.event_id == event.event_id
    assert receipt.destination == "seo-progress"
    assert len(requests) == 2
    assert requests[0].headers["idempotency-key"] == requests[1].headers["idempotency-key"]
    assert requests[0].headers["x-seo-nonce"] != requests[1].headers["x-seo-nonce"]


def test_cached_receipt_clock_failure_is_generic(tmp_path: Path) -> None:
    clock_values = iter([1_700_000_000])

    def clock() -> int:
        try:
            return next(clock_values)
        except StopIteration:
            raise RuntimeError("private cached clock detail") from None

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(
            transport=httpx.MockTransport(lambda _: httpx.Response(204))
        ),
        clock=clock,
    )
    sink.send(_event())

    with pytest.raises(
        NotificationDeliveryError,
        match="^notification delivery failed$",
    ) as captured:
        sink.send(_event())

    assert captured.value.__cause__ is None
    assert "private cached clock detail" not in repr(captured.value)


def test_waiter_receipt_clock_failure_is_generic(tmp_path: Path) -> None:
    transport_entered = threading.Event()
    release_transport = threading.Event()
    wait_entered = threading.Event()
    clock_values = iter([1_700_000_000])

    class ObservedSignal:
        def __init__(self) -> None:
            self._event = threading.Event()

        def set(self) -> None:
            self._event.set()

        def wait(self, timeout: float | None = None) -> bool:
            wait_entered.set()
            return self._event.wait(timeout)

    def clock() -> int:
        try:
            return next(clock_values)
        except StopIteration:
            raise RuntimeError("private waiter clock detail") from None

    def blocked_delivery(request: httpx.Request) -> httpx.Response:
        state = next(iter(sink._events.values()))
        state.done = ObservedSignal()  # type: ignore[assignment]
        transport_entered.set()
        assert release_transport.wait(timeout=2)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(blocked_delivery)),
        clock=clock,
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        owner = executor.submit(sink.send, _event())
        assert transport_entered.wait(timeout=1)
        waiter = executor.submit(sink.send, _event())
        assert wait_entered.wait(timeout=1)
        release_transport.set()
        assert owner.result(timeout=2).event_id == "event-1"
        with pytest.raises(
            NotificationDeliveryError,
            match="^notification delivery failed$",
        ) as captured:
            waiter.result(timeout=2)

    assert captured.value.__cause__ is None
    assert "private waiter clock detail" not in repr(captured.value)


def test_waiter_signal_failure_is_generic_without_duplicate_delivery(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []
    transport_entered = threading.Event()
    release_transport = threading.Event()
    wait_entered = threading.Event()

    class FaultingWait:
        def __init__(self) -> None:
            self._event = threading.Event()

        def set(self) -> None:
            self._event.set()

        def wait(self, timeout: float | None = None) -> bool:
            wait_entered.set()
            raise RuntimeError("private signal wait detail")

    def blocked_delivery(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        state = next(iter(sink._events.values()))
        state.done = FaultingWait()  # type: ignore[assignment]
        transport_entered.set()
        assert release_transport.wait(timeout=2)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(blocked_delivery)),
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        owner = executor.submit(sink.send, _event())
        assert transport_entered.wait(timeout=1)
        waiter = executor.submit(sink.send, _event())
        assert wait_entered.wait(timeout=1)
        with pytest.raises(
            NotificationDeliveryError,
            match="^notification delivery failed$",
        ) as captured:
            waiter.result(timeout=2)
        release_transport.set()
        assert owner.result(timeout=2).event_id == "event-1"

    assert captured.value.__cause__ is None
    assert "private signal wait detail" not in repr(captured.value)
    assert len(requests) == 1


def test_unexpected_transport_exception_terminates_state_and_allows_retry(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def fail_then_deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            raise RuntimeError("private closed-client detail")
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(fail_then_deliver)),
    )
    event = _event()

    with pytest.raises(
        NotificationDeliveryError,
        match="^notification delivery failed$",
    ) as captured:
        sink.send(event)
    assert captured.value.__cause__ is None
    assert "private closed-client detail" not in repr(captured.value)

    receipt = sink.send(event)

    assert receipt.event_id == event.event_id
    assert len(requests) == 2
    assert requests[0].headers["idempotency-key"] == requests[1].headers["idempotency-key"]


def test_retry_generation_allocation_failure_does_not_poison_single_flight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    def fail_then_deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            raise RuntimeError("initial transport fault")
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(fail_then_deliver)),
    )
    event = _event()
    with pytest.raises(NotificationDeliveryError):
        sink.send(event)

    def fail_state_allocation(*args: object, **kwargs: object) -> object:
        raise MemoryError("private allocation detail")

    monkeypatch.setattr(
        "seo_orchestrator.services.notifications._DeliveryState",
        fail_state_allocation,
    )
    with pytest.raises(
        NotificationDeliveryError,
        match="^notification delivery failed$",
    ) as captured:
        sink.send(event)
    assert captured.value.__cause__ is None
    assert "private allocation detail" not in repr(captured.value)
    monkeypatch.undo()

    assert sink.send(event).event_id == event.event_id
    assert len(requests) == 2


def test_transport_admission_exception_does_not_poison_single_flight(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def fail_then_deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            raise RuntimeError("initial transport fault")
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(fail_then_deliver)),
    )
    event = _event()
    with pytest.raises(NotificationDeliveryError):
        sink.send(event)

    transport_slots = sink._transport_slots

    class FaultingAdmission:
        def acquire(self, *, blocking: bool) -> bool:
            raise MemoryError("private admission detail")

        def release(self) -> None:
            raise AssertionError("unacquired transport slot was released")

    sink._transport_slots = FaultingAdmission()  # type: ignore[assignment]
    with pytest.raises(
        NotificationDeliveryError,
        match="^notification delivery failed$",
    ) as captured:
        sink.send(event)
    assert captured.value.__cause__ is None
    assert "private admission detail" not in repr(captured.value)
    sink._transport_slots = transport_slots

    assert sink.send(event).event_id == event.event_id
    assert len(requests) == 2


def test_canonicalization_failure_is_generic_and_never_reaches_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    def fail_canonicalization(value: object) -> bytes:
        raise CanonicalizationError("private canonicalization detail")

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    monkeypatch.setattr(
        "seo_orchestrator.services.notifications.canonical_json",
        fail_canonicalization,
    )
    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(deliver)),
    )

    with pytest.raises(
        NotificationDeliveryError,
        match="^notification delivery failed$",
    ):
        sink.send(_event())

    assert requests == []


def test_sink_never_follows_provider_redirects(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    def redirect(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(307, headers={"location": "https://redirect.invalid/collect"})

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(
            transport=httpx.MockTransport(redirect),
            follow_redirects=True,
        ),
        clock=lambda: 1_700_000_000,
        nonce_factory=lambda: "0123456789abcdef0123456789abcdef",
    )

    with pytest.raises(NotificationDeliveryError, match="^notification delivery failed$"):
        sink.send(_event())

    assert len(requests) == 1
    assert requests[0].url.host == "hermes.invalid"


def test_error_response_body_is_never_read(tmp_path: Path) -> None:
    class ExplodingStream(httpx.SyncByteStream):
        def __iter__(self) -> Iterator[bytes]:
            raise AssertionError("private response body was read")

    def reject(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, stream=ExplodingStream())

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(reject)),
    )

    with pytest.raises(
        NotificationDeliveryError,
        match="^notification delivery failed$",
    ) as captured:
        sink.send(_event())

    assert captured.value.__cause__ is None
    assert "private response body was read" not in repr(captured.value)


def test_concurrent_completion_delivery_is_exactly_once(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []
    event = _event(event_type="completed")

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(event),
        client=httpx.Client(transport=httpx.MockTransport(deliver)),
        clock=lambda: 1_700_000_000,
        nonce_factory=lambda: "0123456789abcdef0123456789abcdef",
    )

    with ThreadPoolExecutor(max_workers=8) as executor:
        receipts = list(executor.map(sink.send, [event] * 8))

    assert len(requests) == 1
    assert all(receipt.event_id == event.event_id for receipt in receipts)
    assert all(receipt.destination == "seo-progress" for receipt in receipts)


def test_unrelated_event_fails_fast_when_transport_admission_is_full(
    tmp_path: Path,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    first_event = _event()
    second_event = NotificationEvent.model_validate(
        {
            **first_event.model_dump(mode="python"),
            "event_id": "event-2",
            "job_id": "job-2",
        }
    )

    def blocked_delivery(request: httpx.Request) -> httpx.Response:
        entered.set()
        assert release.wait(timeout=2)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(first_event, second_event),
        client=httpx.Client(transport=httpx.MockTransport(blocked_delivery)),
        max_concurrent_deliveries=1,
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(sink.send, first_event)
        assert entered.wait(timeout=1)
        with pytest.raises(
            NotificationCapacityError,
            match="^notification transport capacity exhausted$",
        ):
            sink.send(second_event)
        release.set()
        assert first.result(timeout=2).event_id == "event-1"


def test_send_fails_fast_when_total_pending_admission_is_full(
    tmp_path: Path,
) -> None:
    entered = threading.Event()
    release = threading.Event()

    def blocked_delivery(request: httpx.Request) -> httpx.Response:
        entered.set()
        assert release.wait(timeout=2)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(blocked_delivery)),
        max_concurrent_deliveries=1,
        max_pending_deliveries=1,
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(sink.send, _event())
        assert entered.wait(timeout=1)
        with pytest.raises(
            NotificationCapacityError,
            match="^notification request capacity exhausted$",
        ):
            sink.send(_event())
        release.set()
        assert first.result(timeout=2).event_id == "event-1"


def test_success_finalization_does_not_reacquire_dictionary_lock(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(deliver)),
    )

    class FailSecondLock:
        acquisitions = 0

        def __enter__(self) -> None:
            self.acquisitions += 1
            if self.acquisitions == 2:
                raise MemoryError("private finalization lock detail")

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            traceback: object,
        ) -> None:
            return None

    sink._delivery_lock = FailSecondLock()  # type: ignore[assignment]

    receipt = sink.send(_event())

    assert receipt.event_id == "event-1"
    assert len(requests) == 1


def test_signaling_fault_leaves_terminal_state_and_prevents_duplicate(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    class FaultingSignal:
        def set(self) -> None:
            raise MemoryError("private signaling detail")

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        state = next(iter(sink._events.values()))
        state.done = FaultingSignal()  # type: ignore[assignment]
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(deliver)),
    )

    with pytest.raises(
        NotificationDeliveryError,
        match="^notification delivery failed$",
    ) as captured:
        sink.send(_event())
    assert captured.value.__cause__ is None
    assert "private signaling detail" not in repr(captured.value)
    assert next(iter(sink._events.values())).status == "delivered"

    receipt = sink.send(_event())

    assert receipt.event_id == "event-1"
    assert len(requests) == 1


def test_transport_release_fault_after_success_is_not_retryable(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(deliver)),
    )

    class FaultingRelease:
        def acquire(self, *, blocking: bool) -> bool:
            return True

        def release(self) -> None:
            raise MemoryError("private transport release detail")

    sink._transport_slots = FaultingRelease()  # type: ignore[assignment]
    with pytest.raises(
        NotificationDeliveryError,
        match="^notification delivery failed$",
    ) as captured:
        sink.send(_event())
    assert captured.value.__cause__ is None
    assert next(iter(sink._events.values())).status == "delivered"

    receipt = sink.send(_event())

    assert receipt.event_id == "event-1"
    assert len(requests) == 1


def test_retry_installs_a_new_generation_state(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []
    entered_retry = threading.Event()
    release_retry = threading.Event()

    def fail_then_block(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            raise RuntimeError("initial transport fault")
        entered_retry.set()
        assert release_retry.wait(timeout=2)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(fail_then_block)),
    )
    event = _event()
    with pytest.raises(NotificationDeliveryError):
        sink.send(event)
    old_state = next(iter(sink._events.values()))

    with ThreadPoolExecutor(max_workers=1) as executor:
        retry = executor.submit(sink.send, event)
        assert entered_retry.wait(timeout=1)
        new_state = next(iter(sink._events.values()))
        assert new_state is not old_state
        assert old_state.status == "failed"
        assert old_state.done.is_set()
        release_retry.set()
        assert retry.result(timeout=2).event_id == event.event_id


@pytest.mark.parametrize("fault_phase", ["acquire", "release"])
def test_request_admission_fault_cleans_lifecycle_and_is_generic(
    tmp_path: Path,
    fault_phase: str,
) -> None:
    requests: list[httpx.Request] = []

    def deliver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    sink = HermesWebhookSink(
        webhook_url="https://hermes.invalid/v1/deliver",
        hmac_key_path=_key_file(tmp_path),
        destination="seo-progress",
        approved_event_catalog=_catalog(),
        client=httpx.Client(transport=httpx.MockTransport(deliver)),
    )

    class FaultingAdmission:
        def acquire(self, *, blocking: bool) -> bool:
            if fault_phase == "acquire":
                raise MemoryError("private request acquire detail")
            return True

        def release(self) -> None:
            if fault_phase == "release":
                raise MemoryError("private request release detail")

    sink._request_slots = FaultingAdmission()  # type: ignore[assignment]
    with pytest.raises(
        NotificationDeliveryError,
        match="^notification delivery failed$",
    ) as captured:
        sink.send(_event())

    assert captured.value.__cause__ is None
    assert "private request" not in repr(captured.value)
    assert sink._active_sends == 0
    assert len(requests) == (0 if fault_phase == "acquire" else 1)
    sink.close()
    with pytest.raises(NotificationDeliveryError):
        sink.send(_event())
