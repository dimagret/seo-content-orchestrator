from __future__ import annotations

import pytest

from seo_orchestrator.security.signatures import (
    SignatureVerificationError,
    sign_request,
    verify_request,
)

_KEY = bytes.fromhex("1a" * 32)
_BODY = b'{"job_id":"job-one"}'
_TIMESTAMP = 1_722_772_800
_NONCE = "nonce-contract-0123456789"


class ReplayGuard:
    def __init__(self) -> None:
        self.seen: set[str] = set()

    def consume(self, nonce: str) -> bool:
        if nonce in self.seen:
            return False
        self.seen.add(nonce)
        return True


def test_verify_request_consumes_authenticated_nonce_once() -> None:
    guard = ReplayGuard()
    signature = sign_request("POST", "/v1/executions", _TIMESTAMP, _NONCE, _BODY, _KEY)
    assert signature == "08da8941f48195977540def4599b86c05860eb87a6f1db7eace85331144c67ed"

    verify_request(
        "POST",
        "/v1/executions",
        _TIMESTAMP,
        _NONCE,
        _BODY,
        _KEY,
        signature,
        now=_TIMESTAMP,
        nonce_consumer=guard.consume,
    )

    with pytest.raises(SignatureVerificationError, match="invalid request signature"):
        verify_request(
            "POST",
            "/v1/executions",
            _TIMESTAMP,
            _NONCE,
            _BODY,
            _KEY,
            signature,
            now=_TIMESTAMP,
            nonce_consumer=guard.consume,
        )


def test_invalid_signature_does_not_burn_nonce() -> None:
    guard = ReplayGuard()
    valid = sign_request("POST", "/v1/executions", _TIMESTAMP, _NONCE, _BODY, _KEY)

    with pytest.raises(SignatureVerificationError):
        verify_request(
            "POST",
            "/v1/executions",
            _TIMESTAMP,
            _NONCE,
            _BODY,
            _KEY,
            "0" * 64,
            now=_TIMESTAMP,
            nonce_consumer=guard.consume,
        )

    verify_request(
        "POST",
        "/v1/executions",
        _TIMESTAMP,
        _NONCE,
        _BODY,
        _KEY,
        valid,
        now=_TIMESTAMP,
        nonce_consumer=guard.consume,
    )


def test_timestamp_skew_is_inclusive_at_300_seconds_and_rejects_301() -> None:
    signature = sign_request("POST", "/v1/executions", _TIMESTAMP, _NONCE, _BODY, _KEY)
    verify_request(
        "POST",
        "/v1/executions",
        _TIMESTAMP,
        _NONCE,
        _BODY,
        _KEY,
        signature,
        now=_TIMESTAMP - 300,
        nonce_consumer=lambda _nonce: True,
    )
    with pytest.raises(SignatureVerificationError):
        verify_request(
            "POST",
            "/v1/executions",
            _TIMESTAMP,
            _NONCE,
            _BODY,
            _KEY,
            signature,
            now=_TIMESTAMP - 301,
            nonce_consumer=lambda _nonce: True,
        )
