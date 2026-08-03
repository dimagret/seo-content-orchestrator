from datetime import UTC, datetime

import pytest
from pydantic import BaseModel

from seo_orchestrator.canonical import canonical_json, sha256_fingerprint
from seo_orchestrator.errors import CanonicalizationError


def test_canonical_json_is_compact_unicode_and_key_order_independent() -> None:
    first = {"я": "да", "a": 1}
    second = {"a": 1, "я": "да"}

    expected = '{"a":1,"я":"да"}'.encode()
    assert canonical_json(first) == expected
    assert canonical_json(second) == expected
    assert sha256_fingerprint(first) == (
        "454728b41cd2490909785c31e10dd37ab4d9f5c03d7b5c3917fde6e9e20547b5"
    )


def test_array_order_is_stable_and_tuples_become_arrays() -> None:
    assert canonical_json({"items": ("second", "first")}) == (
        b'{"items":["second","first"]}'
    )
    assert canonical_json([2, 1]) != canonical_json([1, 2])


class ExampleModel(BaseModel):
    value: str


@pytest.mark.parametrize(
    "unsupported",
    [
        1.0,
        float("nan"),
        float("inf"),
        b"bytes",
        {"set"},
        frozenset({"frozen"}),
        object(),
        ExampleModel(value="model"),
        datetime.now(UTC),
    ],
)
def test_unsupported_values_are_rejected(unsupported: object) -> None:
    with pytest.raises(CanonicalizationError):
        canonical_json(unsupported)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "unsupported",
    [
        {"nested": [1, 2.0]},
        {"nested": {"bad": b"bytes"}},
        {1: "non-string key"},
    ],
)
def test_nested_unsupported_values_and_non_string_keys_are_rejected(
    unsupported: object,
) -> None:
    with pytest.raises(CanonicalizationError):
        canonical_json(unsupported)  # type: ignore[arg-type]


def test_canonicalization_error_is_a_type_error() -> None:
    assert issubclass(CanonicalizationError, TypeError)
