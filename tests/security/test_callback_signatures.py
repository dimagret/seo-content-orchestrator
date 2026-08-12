import pytest

_KEY = bytes.fromhex("0f" * 32)
_BODY = b'{"job_id":"job-one"}'
_TIMESTAMP = 1_722_772_800
_NONCE = "nonce-test-0123456789"
_SIGNATURE = "452f5e6acb8fa0f33e8b0db4f2463485348e6fb6090c28786656a4591f9d7f2d"


def test_signature_canonical_input_is_frozen_and_verifies() -> None:
    from seo_orchestrator.security.signatures import sign_request, verify_request

    signature = sign_request(
        "POST",
        "/v1/callbacks/n8n",
        _TIMESTAMP,
        _NONCE,
        _BODY,
        _KEY,
    )

    assert signature == _SIGNATURE
    verify_request(
        "POST",
        "/v1/callbacks/n8n",
        _TIMESTAMP,
        _NONCE,
        _BODY,
        _KEY,
        signature,
        now=_TIMESTAMP + 300,
    )


@pytest.mark.parametrize(
    ("timestamp", "body", "signature", "now"),
    [
        (_TIMESTAMP - 301, _BODY, _SIGNATURE, _TIMESTAMP),
        (_TIMESTAMP, b'{"job_id":"other"}', _SIGNATURE, _TIMESTAMP),
        (_TIMESTAMP, _BODY, "0" * 64, _TIMESTAMP),
    ],
)
def test_signature_verification_rejects_stale_or_tampered_values(
    timestamp: int, body: bytes, signature: str, now: int
) -> None:
    from seo_orchestrator.security.signatures import SignatureVerificationError, verify_request

    with pytest.raises(SignatureVerificationError):
        verify_request(
            "POST",
            "/v1/callbacks/n8n",
            timestamp,
            _NONCE,
            body,
            _KEY,
            signature,
            now=now,
        )


def test_signature_rejects_line_break_in_signing_component() -> None:
    from seo_orchestrator.security.signatures import SignatureVerificationError, sign_request

    with pytest.raises(SignatureVerificationError):
        sign_request(
            "POST\nGET",
            "/v1/callbacks/n8n",
            _TIMESTAMP,
            _NONCE,
            _BODY,
            _KEY,
        )
