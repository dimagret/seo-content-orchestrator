"""Deterministic, resource-bounded JSON serialization and fingerprints."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import cast

from seo_orchestrator.errors import CanonicalizationError

type JsonScalar = None | bool | int | str
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]

MAX_CANONICAL_DEPTH = 64
MAX_CANONICAL_NODES = 10_000
MAX_CANONICAL_BYTES = 1_048_576
MAX_SAFE_INTEGER = 2**53 - 1
_UTF8_CHUNK_CHARACTERS = 65_536
_RESIDUAL_ERRORS = (ValueError, TypeError, UnicodeEncodeError, RecursionError, OverflowError)


@dataclass
class _ValidationState:
    nodes: int = 0
    string_bytes: int = 0
    active_containers: set[int] = field(default_factory=set)

    def visit_node(self) -> None:
        self.nodes += 1
        if self.nodes > MAX_CANONICAL_NODES:
            raise CanonicalizationError("canonical JSON exceeds maximum visited nodes")

    def visit_string(self, value: str) -> None:
        self.visit_node()
        for offset in range(0, len(value), _UTF8_CHUNK_CHARACTERS):
            try:
                encoded = value[offset : offset + _UTF8_CHUNK_CHARACTERS].encode("utf-8")
            except UnicodeEncodeError as exc:
                raise CanonicalizationError(
                    "canonical JSON strings and keys must be valid UTF-8"
                ) from exc
            self.string_bytes += len(encoded)
            if self.string_bytes > MAX_CANONICAL_BYTES:
                raise CanonicalizationError(
                    "canonical JSON exceeds preflight string byte budget"
                )


def _normalize(value: object, *, depth: int, state: _ValidationState) -> JsonValue:
    value_type = type(value)
    if value_type is str:
        string_value = cast(str, value)
        state.visit_string(string_value)
        return string_value

    state.visit_node()
    if value is None:
        return None
    if value_type is bool:
        return cast(bool, value)
    if value_type is int:
        integer_value = cast(int, value)
        if not -MAX_SAFE_INTEGER <= integer_value <= MAX_SAFE_INTEGER:
            raise CanonicalizationError("canonical JSON integer is outside the safe range")
        return integer_value
    if value_type is not list and value_type is not dict:
        raise CanonicalizationError("unsupported canonical JSON value type")
    if depth > MAX_CANONICAL_DEPTH:
        raise CanonicalizationError("canonical JSON exceeds maximum container depth")

    identity = id(value)
    if identity in state.active_containers:
        raise CanonicalizationError("canonical JSON contains a container cycle")
    state.active_containers.add(identity)
    try:
        if value_type is list:
            normalized_list: list[JsonValue] = []
            for item in cast(list[object], value):
                normalized_list.append(
                    _normalize(item, depth=depth + 1, state=state)
                )
            return normalized_list
        normalized_dict: dict[str, JsonValue] = {}
        for key, item in cast(dict[object, object], value).items():
            if type(key) is not str:
                raise CanonicalizationError(
                    "canonical JSON object keys must be exact strings"
                )
            string_key = key
            state.visit_string(string_key)
            normalized_dict[string_key] = _normalize(
                item, depth=depth + 1, state=state
            )
        return normalized_dict
    finally:
        state.active_containers.remove(identity)


def canonical_json(value: JsonValue) -> bytes:
    """Serialize a supported value to deterministic, bounded UTF-8 JSON bytes.

    Accounting visits the root, every list/dict value, and every dictionary key once.
    Container depth starts at zero for a container root. String and key UTF-8 bytes are
    cumulatively preflighted before serialization; JSON syntax/escaping is covered by the
    final encoded-payload limit.
    """
    try:
        normalized = _normalize(value, depth=0, state=_ValidationState())
        payload = json.dumps(
            normalized,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except CanonicalizationError:
        raise
    except _RESIDUAL_ERRORS as exc:
        raise CanonicalizationError("failed to serialize canonical JSON") from exc
    if len(payload) > MAX_CANONICAL_BYTES:
        raise CanonicalizationError("canonical JSON exceeds maximum payload bytes")
    return payload


def sha256_fingerprint(value: JsonValue) -> str:
    """Return the lowercase SHA-256 fingerprint of canonical JSON bytes."""
    return hashlib.sha256(canonical_json(value)).hexdigest()
