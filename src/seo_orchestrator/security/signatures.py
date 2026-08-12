"""Pure HMAC request-signing primitives shared by n8n callbacks and executor calls."""

from __future__ import annotations

import hashlib
import hmac
import re

MAX_TIMESTAMP_SKEW_SECONDS = 300
_NONCE_PATTERN = re.compile(r"[A-Za-z0-9_-]{16,128}\Z")
_LOWER_HEX = frozenset("0123456789abcdef")


class SignatureVerificationError(ValueError):
    """Raised for every invalid or unauthenticated signing input."""


def _invalid() -> SignatureVerificationError:
    return SignatureVerificationError("invalid request signature")


def _validated_method(value: object) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.upper()
        or not value.isascii()
        or any(character in value for character in "\r\n")
    ):
        raise _invalid()
    return value


def _validated_path(value: object) -> str:
    if (
        type(value) is not str
        or not value.startswith("/")
        or not value.isascii()
        or any(character in value for character in "\r\n")
    ):
        raise _invalid()
    return value


def _validated_timestamp(value: object) -> int:
    if type(value) is not int or value < 0:
        raise _invalid()
    return value


def _validated_nonce(value: object) -> str:
    if type(value) is not str or _NONCE_PATTERN.fullmatch(value) is None:
        raise _invalid()
    return value


def _validated_body(value: object) -> bytes:
    if type(value) is not bytes:
        raise _invalid()
    return value


def _validated_key(value: object) -> bytes:
    if type(value) is not bytes or len(value) != 32:
        raise _invalid()
    return value


def _canonical_input(method: object, path: object, timestamp: object, nonce: object, body: object) -> bytes:
    verified_method = _validated_method(method)
    verified_path = _validated_path(path)
    verified_timestamp = _validated_timestamp(timestamp)
    verified_nonce = _validated_nonce(nonce)
    verified_body = _validated_body(body)
    body_digest = hashlib.sha256(verified_body).hexdigest()
    return (
        f"{verified_method}\n{verified_path}\n{verified_timestamp}\n{verified_nonce}\n{body_digest}"
    ).encode("ascii")


def sign_request(
    method: str,
    path: str,
    timestamp: int,
    nonce: str,
    body: bytes,
    key: bytes,
) -> str:
    """Return HMAC-SHA256 hex for the frozen five-line canonical request input."""
    canonical = _canonical_input(method, path, timestamp, nonce, body)
    return hmac.new(_validated_key(key), canonical, hashlib.sha256).hexdigest()


def verify_request(
    method: str,
    path: str,
    timestamp: int,
    nonce: str,
    body: bytes,
    key: bytes,
    signature: str,
    *,
    now: int,
) -> None:
    """Fail closed unless signature and bounded timestamp are both valid."""
    verified_timestamp = _validated_timestamp(timestamp)
    verified_now = _validated_timestamp(now)
    if (
        type(signature) is not str
        or len(signature) != 64
        or any(character not in _LOWER_HEX for character in signature)
        or abs(verified_now - verified_timestamp) > MAX_TIMESTAMP_SKEW_SECONDS
    ):
        raise _invalid()
    expected = sign_request(method, path, verified_timestamp, nonce, body, key)
    if not hmac.compare_digest(expected, signature):
        raise _invalid()
