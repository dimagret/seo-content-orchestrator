from __future__ import annotations

from collections.abc import Mapping, Sequence

import pytest

from seo_orchestrator.security.url_policy import (
    NormalizedUrl,
    UrlPolicyError,
    normalize_public_http_url,
)


class StaticResolver:
    def __init__(self, answers: Mapping[str, Sequence[str]]) -> None:
        self.answers = answers
        self.queries: list[str] = []

    def resolve(self, hostname: str) -> tuple[str, ...]:
        self.queries.append(hostname)
        return tuple(self.answers.get(hostname, ()))


@pytest.mark.parametrize(
    "raw",
    (
        "file:///etc/passwd",
        "http://127.0.0.1",
        "http://[::1]",
        "http://169.254.169.254",
        "http://0x7f000001",
        "http://2130706433",
        "http://017700000001",
        "http://0x7f.0.0.1",
        "http://２１３０７０６４３３",
        "http://０１７７０００００００１",
        "http://０x７f０００００１",
        "http://１２７。０。０。１",
        "http://0x7f。0。0。1",
        "http://localhost",
        "http://user:pass@example.com",
        "http://example.com#secret",
        "http://example.com\\@127.0.0.1/",
        "http://example.com/path\\segment",
        "http://example.com:0/",
    ),
)
def test_bypass_corpus_is_rejected(raw: str) -> None:
    resolver = StaticResolver(
        {
            "example.com": ("93.184.216.34",),
            "localhost": ("127.0.0.1",),
            "0x7f000001": ("93.184.216.34",),
            "2130706433": ("93.184.216.34",),
            "017700000001": ("93.184.216.34",),
            "0x7f.0.0.1": ("93.184.216.34",),
            "127.0.0.1": ("93.184.216.34",),
        }
    )

    with pytest.raises(UrlPolicyError):
        normalize_public_http_url(raw, resolver)


@pytest.mark.parametrize(
    "address",
    (
        "10.0.0.1",
        "127.0.0.1",
        "169.254.1.1",
        "224.0.0.1",
        "192.0.2.1",
        "0.0.0.0",
        "::",
        "::1",
        "fc00::1",
        "fe80::1",
        "ff02::1",
        "2001:db8::1",
        "::ffff:127.0.0.1",
    ),
)
def test_any_non_public_dns_answer_rejects_the_host(address: str) -> None:
    resolver = StaticResolver({"example.com": ("93.184.216.34", address)})

    with pytest.raises(UrlPolicyError, match="public"):
        normalize_public_http_url("https://example.com/path", resolver)


def test_normalizes_idna_default_port_path_and_dns_answers() -> None:
    resolver = StaticResolver(
        {"xn--e1afmkfd.xn--p1ai": ("2606:2800:220:1:248:1893:25c8:1946", "93.184.216.34")}
    )

    result = normalize_public_http_url("HTTPS://Пример.РФ:443/a/../b?q=1", resolver)

    assert result == NormalizedUrl(
        url="https://xn--e1afmkfd.xn--p1ai/a/../b?q=1",
        scheme="https",
        hostname="xn--e1afmkfd.xn--p1ai",
        port=443,
        resolved_addresses=("93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946"),
    )
    assert resolver.queries == ["xn--e1afmkfd.xn--p1ai"]


def test_explicit_non_default_port_is_preserved() -> None:
    resolver = StaticResolver({"example.com": ("93.184.216.34",)})

    result = normalize_public_http_url("http://EXAMPLE.com:8080", resolver)

    assert result.url == "http://example.com:8080/"
    assert result.port == 8080


def test_empty_or_malformed_dns_answers_fail_closed() -> None:
    with pytest.raises(UrlPolicyError, match="resolve"):
        normalize_public_http_url("https://example.com", StaticResolver({}))

    with pytest.raises(UrlPolicyError, match="address"):
        normalize_public_http_url(
            "https://example.com", StaticResolver({"example.com": ("not-an-ip",)})
        )

    with pytest.raises(UrlPolicyError, match="address"):
        normalize_public_http_url(
            "https://example.com", StaticResolver({"example.com": (None,)})  # type: ignore[arg-type]
        )

    with pytest.raises(UrlPolicyError, match="address"):
        normalize_public_http_url(
            "https://example.com",
            StaticResolver({"example.com": ("2606:2800:220:1:248:1893:25c8:1946%lo",)}),
        )


def test_resolver_failure_is_wrapped_as_policy_error() -> None:
    class FailingResolver:
        def resolve(self, hostname: str) -> tuple[str, ...]:
            raise OSError(f"DNS unavailable for {hostname}")

    with pytest.raises(UrlPolicyError, match="resolution"):
        normalize_public_http_url("https://example.com", FailingResolver())
