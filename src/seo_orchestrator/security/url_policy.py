"""Pure URL normalization and SSRF policy with injected DNS resolution."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address, ip_address
from typing import Protocol
from urllib.parse import SplitResult, urljoin, urlsplit, urlunsplit

import idna


class UrlPolicyError(ValueError):
    """Raised when a URL or resolved address violates the public-network policy."""


class Resolver(Protocol):
    """Side-effect boundary for hostname resolution."""

    def resolve(self, hostname: str) -> Iterable[str]: ...


@dataclass(frozen=True, slots=True)
class NormalizedUrl:
    """Canonical URL plus the exact public addresses observed during validation."""

    url: str
    scheme: str
    hostname: str
    port: int
    resolved_addresses: tuple[str, ...]


_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_IPV4_NUMBER = r"(?:0x[0-9a-f]+|0[0-7]*|[0-9]+)"
_LEGACY_IPV4 = re.compile(rf"{_IPV4_NUMBER}(?:\.{_IPV4_NUMBER}){{0,3}}", re.IGNORECASE)


def _canonical_hostname(hostname: str) -> tuple[str, IPv4Address | IPv6Address | None]:
    try:
        literal = ip_address(hostname)
    except ValueError:
        if ":" in hostname or _LEGACY_IPV4.fullmatch(hostname) is not None:
            raise UrlPolicyError("URL contains a malformed IP literal") from None
        try:
            ascii_host = idna.encode(hostname, uts46=True, std3_rules=True).decode("ascii").lower()
        except (idna.IDNAError, UnicodeError) as exc:
            raise UrlPolicyError("URL hostname is invalid") from exc
        try:
            ip_address(ascii_host)
        except ValueError:
            if _LEGACY_IPV4.fullmatch(ascii_host) is not None:
                raise UrlPolicyError("URL hostname normalizes to an IP literal") from None
        else:
            raise UrlPolicyError("URL hostname normalizes to an IP literal")
        if len(ascii_host) > 253 or any(
            _DNS_LABEL.fullmatch(label) is None for label in ascii_host.split(".")
        ):
            raise UrlPolicyError("URL hostname is invalid")
        return ascii_host, None
    return str(literal), literal


def _public_address(raw: str) -> IPv4Address | IPv6Address:
    if not isinstance(raw, str) or "%" in raw:
        raise UrlPolicyError("resolver returned an invalid IP address")
    try:
        address = ip_address(raw)
    except ValueError as exc:
        raise UrlPolicyError("resolver returned an invalid IP address") from exc
    if (
        not address.is_global
        or address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    ):
        raise UrlPolicyError("resolved addresses must all be public")
    return address


def _resolved_addresses(
    hostname: str,
    literal: IPv4Address | IPv6Address | None,
    resolver: Resolver,
) -> tuple[str, ...]:
    if literal is not None:
        raw_addresses: Iterable[str] = (str(literal),)
    else:
        try:
            raw_addresses = tuple(resolver.resolve(hostname))
        except Exception as exc:
            raise UrlPolicyError("URL hostname resolution failed") from exc
    parsed_addresses = {_public_address(raw) for raw in raw_addresses}
    addresses = tuple(
        str(address)
        for address in sorted(parsed_addresses, key=lambda address: (address.version, address.packed))
    )
    if not addresses:
        raise UrlPolicyError("URL hostname did not resolve to an address")
    return addresses


def normalize_public_http_url(raw: str, resolver: Resolver) -> NormalizedUrl:
    """Normalize an HTTP(S) URL and reject every non-public DNS answer."""

    if not isinstance(raw, str):
        raise UrlPolicyError("URL must be a string")
    if not raw or raw != raw.strip():
        raise UrlPolicyError("URL must be non-empty without surrounding whitespace")
    if any(char.isspace() or ord(char) <= 31 or ord(char) == 127 for char in raw):
        raise UrlPolicyError("URL contains whitespace or control characters")
    if "\\" in raw:
        raise UrlPolicyError("URL backslashes are not allowed")
    if "#" in raw:
        raise UrlPolicyError("URL fragments are not allowed")
    authority = raw.partition("://")[2].split("/", 1)[0].split("?", 1)[0]
    if "%" in authority:
        raise UrlPolicyError("URL authority contains invalid escaping")
    try:
        parsed = urlsplit(raw)
        if parsed.netloc.endswith(":"):
            raise UrlPolicyError("URL must not contain an empty port")
        port = parsed.port
    except ValueError as exc:
        raise UrlPolicyError("URL has an invalid host or port") from exc
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise UrlPolicyError("URL scheme must be http or https")
    if parsed.hostname is None:
        raise UrlPolicyError("URL must include a host")
    if parsed.username is not None or parsed.password is not None:
        raise UrlPolicyError("URL credentials are not allowed")
    if port == 0:
        raise UrlPolicyError("URL port must be between 1 and 65535")
    hostname, literal = _canonical_hostname(parsed.hostname)
    effective_port = port if port is not None else (80 if scheme == "http" else 443)
    addresses = _resolved_addresses(hostname, literal, resolver)
    display_host = f"[{hostname}]" if literal is not None and literal.version == 6 else hostname
    include_port = effective_port != (80 if scheme == "http" else 443)
    netloc = f"{display_host}:{effective_port}" if include_port else display_host
    normalized = urlunsplit(SplitResult(scheme, netloc, parsed.path or "/", parsed.query, ""))
    return NormalizedUrl(
        url=normalized,
        scheme=scheme,
        hostname=hostname,
        port=effective_port,
        resolved_addresses=addresses,
    )


def validate_redirect(
    source: NormalizedUrl,
    target: str,
    resolver: Resolver,
) -> NormalizedUrl:
    """Resolve a Location value and apply the full URL and DNS policy again."""

    if not isinstance(source, NormalizedUrl):
        raise UrlPolicyError("redirect source must be a normalized URL")
    if not isinstance(target, str) or not target or target != target.strip():
        raise UrlPolicyError("redirect target must be a non-empty string")
    return normalize_public_http_url(urljoin(source.url, target), resolver)
