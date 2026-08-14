from __future__ import annotations

from collections.abc import Mapping, Sequence

import pytest

from seo_orchestrator.security.url_policy import (
    UrlPolicyError,
    normalize_public_http_url,
    validate_redirect,
)


class SequencedResolver:
    def __init__(self, answers: Mapping[str, Sequence[Sequence[str]]]) -> None:
        self.answers = {host: list(values) for host, values in answers.items()}
        self.queries: list[str] = []

    def resolve(self, hostname: str) -> tuple[str, ...]:
        self.queries.append(hostname)
        values = self.answers.get(hostname, [])
        if not values:
            return ()
        return tuple(values.pop(0))


def test_relative_redirect_is_joined_and_revalidated() -> None:
    resolver = SequencedResolver(
        {"example.com": (("93.184.216.34",), ("93.184.216.35",))}
    )
    source = normalize_public_http_url("https://example.com/a/start", resolver)

    target = validate_redirect(source, "../next?q=1", resolver)

    assert target.url == "https://example.com/next?q=1"
    assert target.resolved_addresses == ("93.184.216.35",)
    assert resolver.queries == ["example.com", "example.com"]


def test_absolute_cross_host_redirect_is_revalidated() -> None:
    resolver = SequencedResolver(
        {
            "example.com": (("93.184.216.34",),),
            "www.iana.org": (("192.0.43.8",),),
        }
    )
    source = normalize_public_http_url("https://example.com/start", resolver)

    target = validate_redirect(source, "https://www.iana.org/domains", resolver)

    assert target.url == "https://www.iana.org/domains"
    assert resolver.queries == ["example.com", "www.iana.org"]


def test_same_host_dns_rebinding_to_private_address_is_rejected() -> None:
    resolver = SequencedResolver(
        {"example.com": (("93.184.216.34",), ("127.0.0.1",))}
    )
    source = normalize_public_http_url("https://example.com/start", resolver)

    with pytest.raises(UrlPolicyError, match="public"):
        validate_redirect(source, "/admin", resolver)


def test_redirect_to_private_cross_host_is_rejected() -> None:
    resolver = SequencedResolver(
        {
            "example.com": (("93.184.216.34",),),
            "metadata.internal": (("169.254.169.254",),),
        }
    )
    source = normalize_public_http_url("https://example.com/start", resolver)

    with pytest.raises(UrlPolicyError):
        validate_redirect(source, "http://metadata.internal/latest", resolver)


@pytest.mark.parametrize(
    "target",
    (
        "file:///etc/passwd",
        "//user:pass@example.com/private",
        "/next#fragment",
        "http://127.0.0.1/admin",
        "javascript:alert(1)",
        "\\\\127.0.0.1/private",
    ),
)
def test_redirect_bypass_targets_are_rejected(target: str) -> None:
    resolver = SequencedResolver({"example.com": (("93.184.216.34",),)})
    source = normalize_public_http_url("https://example.com/start", resolver)

    with pytest.raises(UrlPolicyError):
        validate_redirect(source, target, resolver)
