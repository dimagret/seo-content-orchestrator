"""Privacy-bounded progress events and disabled-by-default delivery sinks."""

from __future__ import annotations

import hashlib
import ipaddress
import re
import secrets
import socket
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal, Protocol

import httpx
from pydantic import ConfigDict, Field, StrictBool, TypeAdapter, field_validator, model_validator

from seo_orchestrator.api.auth import load_hmac_key
from seo_orchestrator.canonical import JsonValue, canonical_json
from seo_orchestrator.domain.models import DomainModel, Identifier, NonEmptyStr
from seo_orchestrator.errors import CanonicalizationError
from seo_orchestrator.security.signatures import sign_request
from seo_orchestrator.security.url_policy import (
    NormalizedUrl,
    Resolver,
    UrlPolicyError,
    normalize_public_http_url,
)

EventType = Literal["progress", "retry", "completed"]
NotificationAttempt = Annotated[int, Field(strict=True, ge=1, le=100)]
EventKey = tuple[Identifier, EventType, Identifier | None, NotificationAttempt]
_ALLOWED_DISPLAY_PUNCTUATION = frozenset(" ,&'()«»№+-–—")
_CANONICAL_DNS_HOST = re.compile(
    r"(?=.{1,253}\Z)(?:[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?"
)


def _has_long_opaque_run(value: str) -> bool:
    run_length = 0
    for character in value:
        run_length = run_length + 1 if character.isalnum() else 0
        if run_length > 20:
            return True
    return False


@dataclass
class _DeliveryState:
    payload_hash: str
    status: Literal["in_flight", "failed", "delivered"]
    done: threading.Event = field(default_factory=threading.Event)


class _SystemResolver:
    def resolve(self, hostname: str) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    str(address[4][0])
                    for address in socket.getaddrinfo(
                        hostname,
                        None,
                        type=socket.SOCK_STREAM,
                    )
                }
            )
        )


class _PinnedTransport(httpx.BaseTransport):
    def __init__(
        self,
        normalized_url: NormalizedUrl,
        inner: httpx.BaseTransport | None = None,
    ) -> None:
        self._hostname = normalized_url.hostname
        self._port = normalized_url.port
        self._address = normalized_url.resolved_addresses[0]
        self._inner = inner or httpx.HTTPTransport(retries=0)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        headers = request.headers.copy()
        authority = self._hostname
        if self._port != 443:
            authority = f"{authority}:{self._port}"
        headers["host"] = authority
        extensions = dict(request.extensions)
        extensions["sni_hostname"] = self._hostname
        pinned_request = httpx.Request(
            request.method,
            request.url.copy_with(host=self._address, port=self._port),
            headers=headers,
            stream=request.stream,
            extensions=extensions,
        )
        return self._inner.handle_request(pinned_request)

    def close(self) -> None:
        self._inner.close()


class NotificationEvent(DomainModel):
    """The complete allowlisted payload that may cross the notification boundary."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        strict=True,
        str_strip_whitespace=False,
    )

    event_id: Identifier
    job_id: Identifier
    event_type: EventType
    company_display_name: NonEmptyStr = Field(max_length=120)
    direction_display_name: NonEmptyStr = Field(max_length=120)
    completed_stage_count: int = Field(strict=True, ge=0, le=1000)
    stage_id: Identifier | None
    attempt: NotificationAttempt
    elapsed_seconds: int = Field(strict=True, ge=0, le=2_592_000)
    artifact_ready: StrictBool

    @field_validator("company_display_name", "direction_display_name")
    @classmethod
    def reject_sensitive_display_text(cls, value: str) -> str:
        if (
            value != value.strip()
            or "  " in value
            or "--" in value
            or _has_long_opaque_run(value)
            or any(
                not character.isalnum()
                and character not in _ALLOWED_DISPLAY_PUNCTUATION
                for character in value
            )
        ):
            raise ValueError("notification display text violates safe grammar")
        return value

    @model_validator(mode="after")
    def enforce_event_key_invariants(self) -> NotificationEvent:
        if self.event_type == "completed":
            if self.stage_id is not None or self.attempt != 1:
                raise ValueError("completion event key must be canonical")
        elif self.stage_id is None:
            raise ValueError("non-completion events require stage_id")
        return self

    @property
    def event_key(self) -> EventKey:
        return (self.job_id, self.event_type, self.stage_id, self.attempt)


_EVENT_FIELD_NAMES = (
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
)


def _strict_event_copy(value: object) -> NotificationEvent | None:
    if type(value) is not NotificationEvent:
        return None
    try:
        raw_values = tuple(
            object.__getattribute__(value, field_name)
            for field_name in _EVENT_FIELD_NAMES
        )
    except Exception:  # noqa: BLE001 - malformed instances stay untrusted.
        return None
    expected_types = (
        str,
        str,
        str,
        str,
        str,
        int,
        (str, type(None)),
        int,
        int,
        bool,
    )
    for raw_value, expected_type in zip(raw_values, expected_types, strict=True):
        if isinstance(expected_type, tuple):
            if type(raw_value) not in expected_type:
                return None
        elif type(raw_value) is not expected_type:
            return None
    try:
        return NotificationEvent.model_validate(
            dict(zip(_EVENT_FIELD_NAMES, raw_values, strict=True))
        )
    except (TypeError, ValueError):
        return None


def _event_values(event: NotificationEvent) -> tuple[object, ...]:
    return tuple(
        object.__getattribute__(event, field_name)
        for field_name in _EVENT_FIELD_NAMES
    )


class DeliveryReceipt(DomainModel):
    """Safe local acknowledgement without provider response material."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        strict=True,
        str_strip_whitespace=False,
    )

    event_id: Identifier
    destination: Identifier
    delivered_at: datetime

    @field_validator("destination")
    @classmethod
    def reject_destination_normalization(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("receipt destination must be canonical")
        return value

    @field_validator("delivered_at")
    @classmethod
    def require_aware_delivery_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("receipt delivery time must be timezone-aware")
        return value


class NotificationSink(Protocol):
    def send(self, event: NotificationEvent) -> DeliveryReceipt:
        """Deliver or suppress one bounded event."""

        ...

    def close(self) -> None:
        """Release sink-owned resources."""

        ...


class ApprovedEventCatalog(Protocol):
    """Trusted server-side lookup for an exact approved notification event."""

    def resolve(self, event_id: Identifier) -> NotificationEvent | None: ...


class NullNotificationSink:
    """Default sink: acknowledge suppression and perform no external action."""

    def send(self, event: NotificationEvent) -> DeliveryReceipt:
        validated_event = _strict_event_copy(event)
        if validated_event is None:
            raise NotificationPrivacyError("notification event not approved")
        return DeliveryReceipt(
            event_id=validated_event.event_id,
            destination="null",
            delivered_at=datetime.now(UTC),
        )

    def close(self) -> None:
        return None


class NotificationConfig(DomainModel):
    """Disabled-by-default delivery configuration independent of company data."""

    enabled: bool = Field(default=False, strict=True)
    webhook_url: NonEmptyStr | None = None
    allowed_origins: tuple[NonEmptyStr, ...] | None = None
    hmac_key_path: Path | None = None
    destination: Identifier | None = None

    @model_validator(mode="after")
    def validate_delivery_configuration(self) -> NotificationConfig:
        configured = (
            self.webhook_url is not None,
            self.allowed_origins is not None,
            self.hmac_key_path is not None,
            self.destination is not None,
        )
        if self.enabled and not all(configured):
            raise ValueError("enabled notifications require fixed delivery configuration")
        if not self.enabled and any(configured):
            raise ValueError("disabled notifications must not retain delivery configuration")
        if self.hmac_key_path is not None and not self.hmac_key_path.is_absolute():
            raise ValueError("notification HMAC key path must be absolute")
        return self


def build_notification_sink(
    config: NotificationConfig,
    *,
    approved_event_catalog: ApprovedEventCatalog | None = None,
    resolver: Resolver | None = None,
    clock: Callable[[], int] = lambda: int(time.time()),
) -> NotificationSink:
    """Build the disabled sink unless an explicit complete config is supplied."""
    if not config.enabled:
        return NullNotificationSink()
    if approved_event_catalog is None:
        raise ValueError("enabled notifications require approved event catalog")
    assert config.webhook_url is not None
    assert config.allowed_origins is not None
    assert config.hmac_key_path is not None
    assert config.destination is not None
    return HermesWebhookSink(
        webhook_url=config.webhook_url,
        allowed_origins=config.allowed_origins,
        hmac_key_path=config.hmac_key_path,
        destination=config.destination,
        approved_event_catalog=approved_event_catalog,
        resolver=resolver,
        clock=clock,
    )


class NotificationDeliveryError(RuntimeError):
    """Safe delivery failure without transport or provider details."""


class NotificationConflictError(RuntimeError):
    """One logical event key was reused with different immutable content."""


class NotificationCapacityError(RuntimeError):
    """The bounded event identity store cannot accept another key."""


class NotificationPrivacyError(RuntimeError):
    """Notification content was not approved by the trusted local catalog."""


class HermesWebhookSink:
    """Send one minimized event to one fixed HMAC-authenticated endpoint."""

    def __init__(
        self,
        *,
        webhook_url: str,
        allowed_origins: tuple[str, ...] | None = None,
        hmac_key_path: Path,
        destination: str,
        approved_event_catalog: ApprovedEventCatalog,
        client: httpx.Client | None = None,
        transport: httpx.BaseTransport | None = None,
        resolver: Resolver | None = None,
        clock: Callable[[], int] = lambda: int(time.time()),
        nonce_factory: Callable[[], str] | None = None,
        timeout_seconds: float = 5.0,
        max_tracked_events: int = 10_000,
        max_concurrent_deliveries: int = 8,
        max_pending_deliveries: int = 64,
    ) -> None:
        url = httpx.URL(webhook_url)
        raw_host = (url.host or "").lower()
        host = raw_host.rstrip(".")
        port = url.port
        origin = f"https://{host}" + (
            f":{port}" if port not in {None, 443} else ""
        )
        if allowed_origins is None:
            allowed_origins = (origin,) if host.endswith(".invalid") else ()
        try:
            ipaddress.ip_address(host)
        except ValueError:
            is_ip_literal = False
        else:
            is_ip_literal = True
        if (
            (client is not None and transport is not None)
            or
            url.scheme != "https"
            or not url.host
            or bool(url.username)
            or bool(url.password)
            or url.query
            or url.fragment
            or is_ip_literal
            or raw_host != host
            or _CANONICAL_DNS_HOST.fullmatch(host) is None
            or host == "localhost"
            or host.endswith((".localhost", ".local", ".internal"))
            or (not host.endswith(".invalid") and client is not None)
            or (not host.endswith(".invalid") and nonce_factory is not None)
            or not 1 <= len(allowed_origins) <= 32
            or origin not in allowed_origins
            or any(
                not item.startswith("https://")
                or "*" in item
                or item.rstrip("/") != item
                for item in allowed_origins
            )
        ):
            raise ValueError("notification webhook URL is invalid")
        if not 0.1 <= timeout_seconds <= 10.0:
            raise ValueError("notification timeout is outside allowed bounds")
        if type(max_tracked_events) is not int or not 1 <= max_tracked_events <= 100_000:
            raise ValueError("max_tracked_events must be between 1 and 100000")
        if (
            type(max_concurrent_deliveries) is not int
            or not 1 <= max_concurrent_deliveries <= 64
        ):
            raise ValueError("max_concurrent_deliveries must be between 1 and 64")
        if (
            type(max_pending_deliveries) is not int
            or not max_concurrent_deliveries <= max_pending_deliveries <= 1024
        ):
            raise ValueError(
                "max_pending_deliveries must be between max_concurrent_deliveries and 1024"
            )
        if not hmac_key_path.is_absolute():
            raise ValueError("notification HMAC key path must be absolute")
        normalized_url: NormalizedUrl | None = None
        if not host.endswith(".invalid"):
            try:
                normalized_url = normalize_public_http_url(
                    webhook_url,
                    resolver or _SystemResolver(),
                )
            except UrlPolicyError:
                raise ValueError("notification webhook URL is invalid") from None
        self._url = url
        self._path = url.raw_path.decode("ascii")
        self._key = load_hmac_key(hmac_key_path)
        self._destination = TypeAdapter(Identifier).validate_python(destination)
        if normalized_url is None:
            self._client = client or httpx.Client(
                follow_redirects=False,
                trust_env=False,
                transport=transport,
            )
            self._owns_client = client is None
        else:
            self._client = httpx.Client(
                follow_redirects=False,
                trust_env=False,
                transport=_PinnedTransport(normalized_url, transport),
            )
            self._owns_client = True
        self._approved_event_catalog = approved_event_catalog
        self._clock = clock
        self._nonce_factory = nonce_factory or (lambda: secrets.token_hex(16))
        self._timeout_seconds = timeout_seconds
        self._max_tracked_events = max_tracked_events
        self._events: dict[str, _DeliveryState] = {}
        self._delivery_lock = threading.Lock()
        self._transport_slots = threading.BoundedSemaphore(max_concurrent_deliveries)
        self._request_slots = threading.BoundedSemaphore(max_pending_deliveries)
        self._lifecycle = "open"
        self._active_sends = 0
        self._lifecycle_condition = threading.Condition()

    def close(self) -> None:
        with self._lifecycle_condition:
            if self._lifecycle == "closed":
                return
            if self._lifecycle == "closing":
                while self._lifecycle != "closed":
                    self._lifecycle_condition.wait()
                return
            self._lifecycle = "closing"
            while self._active_sends:
                self._lifecycle_condition.wait()
        close_failed = False
        try:
            if self._owns_client:
                self._client.close()
        except Exception:  # noqa: BLE001 - lifecycle boundary hides client internals.
            close_failed = True
        finally:
            with self._lifecycle_condition:
                self._lifecycle = "closed"
                self._lifecycle_condition.notify_all()
        if close_failed:
            raise NotificationDeliveryError("notification delivery failed")

    def send(self, event: NotificationEvent) -> DeliveryReceipt:
        validated_event = _strict_event_copy(event)
        if validated_event is None:
            raise NotificationPrivacyError("notification event not approved")
        with self._lifecycle_condition:
            if self._lifecycle != "open":
                raise NotificationDeliveryError("notification delivery failed")
            self._active_sends += 1
        request_admitted = False
        try:
            try:
                request_admitted = self._request_slots.acquire(blocking=False)
            except Exception:  # noqa: BLE001 - admission faults stay generic.
                raise NotificationDeliveryError(
                    "notification delivery failed"
                ) from None
            if not request_admitted:
                raise NotificationCapacityError(
                    "notification request capacity exhausted"
                )
            return self._send_admitted(validated_event)
        finally:
            release_failed = False
            try:
                if request_admitted:
                    self._request_slots.release()
            except Exception:  # noqa: BLE001 - release faults stay generic.
                release_failed = True
            finally:
                with self._lifecycle_condition:
                    self._active_sends -= 1
                    self._lifecycle_condition.notify_all()
            if release_failed:
                raise NotificationDeliveryError(
                    "notification delivery failed"
                ) from None

    def _send_admitted(self, event: NotificationEvent) -> DeliveryReceipt:
        try:
            approved_event = self._approved_event_catalog.resolve(event.event_id)
        except Exception:  # noqa: BLE001 - privacy boundary hides catalog internals.
            raise NotificationPrivacyError(
                "notification event not approved"
            ) from None
        validated_approved_event = _strict_event_copy(approved_event)
        if (
            validated_approved_event is None
            or _event_values(validated_approved_event) != _event_values(event)
        ):
            raise NotificationPrivacyError(
                "notification event not approved"
            )
        event = validated_approved_event
        payload: dict[str, JsonValue] = {
            "destination": self._destination,
            "event": event.model_dump(mode="json"),
            "event_key": list(event.event_key),
        }
        identity_payload: dict[str, JsonValue] = {
            "destination": self._destination,
            "event_key": list(event.event_key),
        }
        try:
            body = canonical_json(payload)
            idempotency_key = hashlib.sha256(
                canonical_json(identity_payload)
            ).hexdigest()
        except CanonicalizationError:
            raise NotificationDeliveryError("notification delivery failed") from None
        payload_hash = hashlib.sha256(body).hexdigest()
        wait_state: _DeliveryState | None = None
        with self._delivery_lock:
            existing = self._events.get(idempotency_key)
            if existing is not None and existing.payload_hash != payload_hash:
                raise NotificationConflictError("notification event key conflict")
            if existing is not None and existing.status == "delivered":
                return DeliveryReceipt(
                    event_id=event.event_id,
                    destination=self._destination,
                    delivered_at=self._delivery_time(self._read_timestamp()),
                )
            if existing is None:
                if len(self._events) >= self._max_tracked_events:
                    raise NotificationCapacityError("notification capacity exhausted")
                try:
                    existing = _DeliveryState(
                        payload_hash=payload_hash,
                        status="in_flight",
                    )
                except Exception:  # noqa: BLE001 - allocation faults stay generic.
                    raise NotificationDeliveryError(
                        "notification delivery failed"
                    ) from None
                self._events[idempotency_key] = existing
            elif existing.status == "in_flight":
                wait_state = existing
            else:
                try:
                    next_state = _DeliveryState(
                        payload_hash=payload_hash,
                        status="in_flight",
                    )
                except Exception:  # noqa: BLE001 - allocation faults stay generic.
                    raise NotificationDeliveryError(
                        "notification delivery failed"
                    ) from None
                self._events[idempotency_key] = next_state
                existing = next_state

        if wait_state is not None:
            try:
                wait_completed = wait_state.done.wait(timeout=self._timeout_seconds)
            except Exception:  # noqa: BLE001 - signaling details stay generic.
                raise NotificationDeliveryError(
                    "notification delivery failed"
                ) from None
            if not wait_completed:
                raise NotificationDeliveryError("notification delivery failed")
            if wait_state.status != "delivered":
                raise NotificationDeliveryError("notification delivery failed")
            return DeliveryReceipt(
                event_id=event.event_id,
                destination=self._destination,
                delivered_at=self._delivery_time(self._read_timestamp()),
            )

        transport_acquired = False
        delivery_succeeded = False
        delivered_timestamp = 0
        delivery_error: Exception | None = None
        cleanup_failed = False
        try:
            try:
                if not self._transport_slots.acquire(blocking=False):
                    raise NotificationCapacityError(
                        "notification transport capacity exhausted"
                    )
                transport_acquired = True
                delivered_timestamp = self._deliver(body, idempotency_key)
                delivery_succeeded = True
            except Exception as exc:  # noqa: BLE001 - classified below.
                delivery_error = exc
        finally:
            try:
                if transport_acquired:
                    self._transport_slots.release()
            except Exception:  # noqa: BLE001 - cleanup faults stay generic.
                cleanup_failed = True

        if delivery_succeeded:
            self._finish_state(existing, "delivered")
            if cleanup_failed:
                raise NotificationDeliveryError(
                    "notification delivery failed"
                ) from None
        else:
            self._finish_state(existing, "failed")
            if isinstance(
                delivery_error,
                (
                    NotificationCapacityError,
                    NotificationConflictError,
                    NotificationDeliveryError,
                ),
            ):
                raise delivery_error
            raise NotificationDeliveryError("notification delivery failed") from None
        return DeliveryReceipt(
            event_id=event.event_id,
            destination=self._destination,
            delivered_at=self._delivery_time(delivered_timestamp),
        )

    def _read_timestamp(self) -> int:
        try:
            timestamp = self._clock()
        except Exception:  # noqa: BLE001 - clock details must not cross boundary.
            raise NotificationDeliveryError("notification delivery failed") from None
        if type(timestamp) is not int or not 0 <= timestamp <= 4_102_444_800:
            raise NotificationDeliveryError("notification delivery failed")
        return timestamp

    @staticmethod
    def _finish_state(
        state: _DeliveryState,
        status: Literal["failed", "delivered"],
    ) -> None:
        state.status = status
        try:
            state.done.set()
        except Exception:  # noqa: BLE001 - signaling faults stay generic.
            raise NotificationDeliveryError("notification delivery failed") from None

    @staticmethod
    def _delivery_time(timestamp: int) -> datetime:
        return datetime.fromtimestamp(timestamp, UTC)

    def _deliver(self, body: bytes, idempotency_key: str) -> int:
        timestamp = self._read_timestamp()
        nonce = self._nonce_factory()
        if re.fullmatch(r"[0-9a-f]{32}", nonce) is None:
            raise NotificationDeliveryError("notification delivery failed")
        signature = sign_request("POST", self._path, timestamp, nonce, body, self._key)
        headers = {
            "content-type": "application/json",
            "idempotency-key": idempotency_key,
            "x-seo-payload-sha256": hashlib.sha256(body).hexdigest(),
            "x-seo-nonce": nonce,
            "x-seo-signature": signature,
            "x-seo-timestamp": str(timestamp),
        }
        try:
            with self._client.stream(
                "POST",
                self._url,
                content=body,
                headers=headers,
                timeout=self._timeout_seconds,
                follow_redirects=False,
            ) as response:
                if response.status_code == 409:
                    raise NotificationConflictError(
                        "notification event payload conflict"
                    )
                if response.status_code not in {202, 204}:
                    raise NotificationDeliveryError("notification delivery failed")
        except httpx.HTTPError:
            raise NotificationDeliveryError("notification delivery failed") from None
        return timestamp
