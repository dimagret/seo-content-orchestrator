"""Deterministic JSON serialization and fingerprints."""

from __future__ import annotations

import hashlib
import json
from typing import cast

from seo_orchestrator.errors import CanonicalizationError

type JsonScalar = None | bool | int | str
type JsonValue = (
    JsonScalar | list[JsonValue] | tuple[JsonValue, ...] | dict[str, JsonValue]
)


def _normalize(value: object) -> JsonValue:
    if value is None or isinstance(value, (bool, str)):
        return cast(JsonScalar, value)
    if isinstance(value, int):
        return value
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if isinstance(value, dict):
        normalized: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise CanonicalizationError("canonical JSON object keys must be strings")
            normalized[key] = _normalize(item)
        return normalized
    raise CanonicalizationError(
        f"unsupported canonical JSON value type: {type(value).__name__}"
    )


def canonical_json(value: JsonValue) -> bytes:
    """Serialize a supported value to deterministic UTF-8 JSON bytes."""
    normalized = _normalize(value)
    return json.dumps(
        normalized,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_fingerprint(value: JsonValue) -> str:
    """Return the lowercase SHA-256 fingerprint of canonical JSON bytes."""
    return hashlib.sha256(canonical_json(value)).hexdigest()
