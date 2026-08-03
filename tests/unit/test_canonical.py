from datetime import UTC, datetime

import pytest
from pydantic import BaseModel

from seo_orchestrator.canonical import (
    MAX_CANONICAL_BYTES,
    MAX_CANONICAL_DEPTH,
    MAX_CANONICAL_NODES,
    MAX_SAFE_INTEGER,
    JsonValue,
    canonical_json,
    sha256_fingerprint,
)
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


def test_array_order_is_stable() -> None:
    assert canonical_json([2, 1]) != canonical_json([1, 2])


def test_public_json_value_alias_excludes_tuples() -> None:
    assert "tuple" not in str(JsonValue.__value__)


def test_top_level_tuple_is_rejected() -> None:
    with pytest.raises(CanonicalizationError):
        canonical_json((1, 2))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "value",
    [[("nested",)], {"nested": (1, 2)}],
    ids=["list", "dict"],
)
def test_nested_tuple_is_rejected(value: object) -> None:
    with pytest.raises(CanonicalizationError):
        canonical_json(value)  # type: ignore[arg-type]


def test_sha256_fingerprint_rejects_tuples() -> None:
    with pytest.raises(CanonicalizationError):
        sha256_fingerprint({"nested": (1, 2)})  # type: ignore[dict-item]


class ExampleModel(BaseModel):
    value: str


@pytest.mark.parametrize(
    "unsupported",
    [
        1.0,
        float("nan"),
        float("inf"),
        b"bytes",
        bytearray(b"bytes"),
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


@pytest.mark.parametrize(
    "unsupported",
    [
        1.0,
        float("nan"),
        float("inf"),
        b"bytes",
        bytearray(b"bytes"),
        {"set"},
        frozenset({"frozen"}),
        object(),
        ExampleModel(value="model"),
        datetime.now(UTC),
    ],
)
@pytest.mark.parametrize(
    "container",
    [lambda value: [value], lambda value: {"nested": value}],
    ids=["list", "dict"],
)
def test_every_unsupported_type_is_rejected_in_every_nested_container(
    unsupported: object, container: object
) -> None:
    nested = container(unsupported)  # type: ignore[operator]
    with pytest.raises(CanonicalizationError):
        canonical_json(nested)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "value",
    [{1: "value"}, [{1: "value"}], {"nested": {1: "value"}}],
    ids=["top-level-dict", "list", "dict"],
)
def test_non_string_keys_are_rejected_at_top_level_and_nested(value: object) -> None:
    with pytest.raises(CanonicalizationError):
        canonical_json(value)  # type: ignore[arg-type]


def test_canonicalization_error_is_a_type_error() -> None:
    assert issubclass(CanonicalizationError, TypeError)


def _nested_lists(container_depth: int) -> list[object]:
    root: list[object] = []
    current = root
    for _ in range(container_depth):
        child: list[object] = []
        current.append(child)
        current = child
    return root


@pytest.mark.parametrize("container_type", [list, dict], ids=["list", "dict"])
def test_direct_container_cycles_are_rejected(container_type: type[object]) -> None:
    if container_type is list:
        value: object = []
        value.append(value)  # type: ignore[attr-defined]
    else:
        value = {}
        value["self"] = value  # type: ignore[index]

    with pytest.raises(CanonicalizationError, match="cycle"):
        canonical_json(value)  # type: ignore[arg-type]


@pytest.mark.parametrize("container_type", [list, dict], ids=["list", "dict"])
def test_nested_container_cycles_are_rejected(container_type: type[object]) -> None:
    if container_type is list:
        cyclic: object = []
        value = {"outer": [cyclic]}
        cyclic.append(value)  # type: ignore[attr-defined]
    else:
        cyclic = {}
        value = [cyclic]
        cyclic["outer"] = value  # type: ignore[index]

    with pytest.raises(CanonicalizationError, match="cycle"):
        canonical_json(value)  # type: ignore[arg-type]


def test_canonical_container_depth_boundary() -> None:
    accepted = _nested_lists(MAX_CANONICAL_DEPTH)
    rejected = _nested_lists(MAX_CANONICAL_DEPTH + 1)

    assert canonical_json(accepted).startswith(b"[")
    with pytest.raises(CanonicalizationError, match="depth"):
        canonical_json(rejected)


@pytest.mark.parametrize(
    "value",
    [chr(0xD800), chr(0xDC00), {"key": chr(0xD800)}, {chr(0xDC00): "value"}],
    ids=["high-value", "low-value", "nested-value", "dict-key"],
)
def test_lone_surrogates_raise_controlled_errors(value: object) -> None:
    with pytest.raises(CanonicalizationError, match="UTF-8"):
        canonical_json(value)  # type: ignore[arg-type]


def test_canonical_node_count_boundary_counts_root_and_list_values() -> None:
    accepted = [None] * (MAX_CANONICAL_NODES - 1)
    rejected = [None] * MAX_CANONICAL_NODES

    assert canonical_json(accepted).startswith(b"[")
    with pytest.raises(CanonicalizationError, match="nodes"):
        canonical_json(rejected)


def test_canonical_node_count_includes_dictionary_keys() -> None:
    # root object + each key + each value = 1 + 2n visited nodes
    accepted = {str(index): None for index in range((MAX_CANONICAL_NODES - 1) // 2)}
    rejected = accepted | {"overflow": None}

    assert canonical_json(accepted).startswith(b"{")
    with pytest.raises(CanonicalizationError, match="nodes"):
        canonical_json(rejected)


def test_final_canonical_byte_budget_boundary_includes_json_quotes() -> None:
    accepted = "a" * (MAX_CANONICAL_BYTES - 2)
    rejected = accepted + "a"

    assert len(canonical_json(accepted)) == MAX_CANONICAL_BYTES
    with pytest.raises(CanonicalizationError, match="bytes"):
        canonical_json(rejected)


def test_oversized_single_string_is_rejected_by_preflight_budget() -> None:
    with pytest.raises(CanonicalizationError, match="string byte budget"):
        canonical_json("a" * (MAX_CANONICAL_BYTES + 1))


def test_oversized_cumulative_strings_and_keys_are_rejected_by_preflight_budget() -> None:
    first = "a" * (MAX_CANONICAL_BYTES // 2)
    second = "b" * (MAX_CANONICAL_BYTES // 2 + 1)

    with pytest.raises(CanonicalizationError, match="string byte budget"):
        canonical_json({first: second})


@pytest.mark.parametrize("value", [-MAX_SAFE_INTEGER, MAX_SAFE_INTEGER])
def test_json_safe_integer_boundaries_are_accepted(value: int) -> None:
    assert canonical_json(value) == str(value).encode()


@pytest.mark.parametrize("value", [-MAX_SAFE_INTEGER - 1, MAX_SAFE_INTEGER + 1])
def test_integers_outside_json_safe_range_are_rejected(value: int) -> None:
    with pytest.raises(CanonicalizationError, match="safe range"):
        canonical_json(value)


def test_bool_remains_accepted_separately_from_integer_limits() -> None:
    assert canonical_json([False, True]) == b"[false,true]"


def test_repeated_acyclic_aliases_are_allowed_and_deterministic() -> None:
    alias: list[JsonValue] = [{"value": 1}]
    value = [alias, alias]

    assert canonical_json(value) == b'[[{"value":1}],[{"value":1}]]'
    assert canonical_json(value) == canonical_json(value)


@pytest.mark.parametrize(
    "residual_error",
    [
        ValueError("dumps failed"),
        TypeError("dumps failed"),
        UnicodeEncodeError("utf-8", "\ud800", 0, 1, "surrogate"),
        RecursionError("dumps failed"),
        OverflowError("dumps failed"),
    ],
    ids=["value", "type", "unicode", "recursion", "overflow"],
)
def test_residual_serialization_errors_are_wrapped(
    monkeypatch: pytest.MonkeyPatch, residual_error: Exception
) -> None:
    def fail_dumps(*args: object, **kwargs: object) -> str:
        raise residual_error

    monkeypatch.setattr("seo_orchestrator.canonical.json.dumps", fail_dumps)

    with pytest.raises(CanonicalizationError, match="serialize"):
        canonical_json({"valid": True})


def test_sha256_fingerprint_converts_every_guard_failure_to_canonicalization_error() -> None:
    cyclic_list: list[object] = []
    cyclic_list.append(cyclic_list)
    cyclic_dict: dict[str, object] = {}
    cyclic_dict["self"] = cyclic_dict
    invalid_values = [
        "\ud800",
        "\udc00",
        {"\ud800": "value"},
        cyclic_list,
        cyclic_dict,
        _nested_lists(MAX_CANONICAL_DEPTH + 1),
        [None] * MAX_CANONICAL_NODES,
        "a" * (MAX_CANONICAL_BYTES + 1),
        MAX_SAFE_INTEGER + 1,
        -MAX_SAFE_INTEGER - 1,
    ]

    for value in invalid_values:
        with pytest.raises(CanonicalizationError):
            sha256_fingerprint(value)  # type: ignore[arg-type]
