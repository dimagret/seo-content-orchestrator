"""Local Worker API token loading and bearer authentication."""

from __future__ import annotations

import hmac
import os
import stat
from pathlib import Path

_TOKEN_BYTES = 32
_TOKEN_HEX_LENGTH = _TOKEN_BYTES * 2
_MAX_AUTHORIZATION_HEADER_CHARS = 512


class ApiAuthenticationError(RuntimeError):
    """Raised when a request does not prove possession of the local API token."""


class ApiTokenConfigurationError(RuntimeError):
    """Raised when the local API token file is unsafe or malformed."""


def load_api_token(path: Path) -> bytes:
    """Load exactly 32 random bytes encoded as hex from a protected regular file."""
    flags = (
        os.O_RDONLY
        | os.O_CLOEXEC
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ApiTokenConfigurationError("local API token file is unavailable") from error

    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ApiTokenConfigurationError("local API token file is not regular")
        if stat.S_IMODE(metadata.st_mode) != 0o600:
            raise ApiTokenConfigurationError("local API token file mode must be 0600")
        if metadata.st_uid != os.geteuid():
            raise ApiTokenConfigurationError("local API token file has an unexpected owner")
        if metadata.st_nlink != 1:
            raise ApiTokenConfigurationError("local API token file must not be linked")

        encoded = os.read(descriptor, _TOKEN_HEX_LENGTH + 1)
        if len(encoded) != _TOKEN_HEX_LENGTH:
            raise ApiTokenConfigurationError("local API token file has an invalid length")
        try:
            token = bytes.fromhex(encoded.decode("ascii"))
        except (UnicodeDecodeError, ValueError) as error:
            raise ApiTokenConfigurationError("local API token file is not hexadecimal") from error
        if len(token) != _TOKEN_BYTES:
            raise ApiTokenConfigurationError("local API token file has an invalid length")
        return token
    finally:
        os.close(descriptor)


def load_hmac_key(path: Path) -> bytes:
    """Load an independent callback HMAC key under the same file safety rules."""
    return load_api_token(path)


def verify_bearer_authorization(authorization: str | None, expected_token: bytes) -> None:
    """Require one bounded Bearer token and compare decoded bytes in constant time."""
    if authorization is None or len(authorization) > _MAX_AUTHORIZATION_HEADER_CHARS:
        raise ApiAuthenticationError("unauthorized")
    if not authorization.startswith("Bearer "):
        raise ApiAuthenticationError("unauthorized")

    encoded = authorization.removeprefix("Bearer ")
    if len(encoded) != _TOKEN_HEX_LENGTH:
        raise ApiAuthenticationError("unauthorized")
    try:
        presented_token = bytes.fromhex(encoded)
    except ValueError as error:
        raise ApiAuthenticationError("unauthorized") from error
    if len(presented_token) != _TOKEN_BYTES:
        raise ApiAuthenticationError("unauthorized")
    if not hmac.compare_digest(presented_token, expected_token):
        raise ApiAuthenticationError("unauthorized")
