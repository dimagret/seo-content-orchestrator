"""Immutable artifact bundles for successful SEO jobs.

Atomic no-replace publication relies on Linux ``renameat2(RENAME_NOREPLACE)``;
an unavailable syscall fails closed instead of falling back to replacing rename.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import os
import re
import secrets
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
from ipaddress import ip_address
from pathlib import Path
from types import MappingProxyType
from typing import BinaryIO, cast
from urllib.parse import parse_qsl, urlsplit

from seo_orchestrator.canonical import MAX_CANONICAL_BYTES, JsonValue, canonical_json
from seo_orchestrator.domain import JobState, SeoJob
from seo_orchestrator.domain.models import _normalize_url
from seo_orchestrator.errors import CanonicalizationError, DataIntegrityError, NotFound

_ARTIFACT_NAMES = frozenset(
    {"content.md", "metadata.json", "qa.json", "sources.json", "manifest.json"}
)
_PAYLOAD_NAMES = frozenset({"content.md", "metadata.json", "qa.json", "sources.json"})
_KEYWORD_QA_KEYS = frozenset({"primary_keyword", "occurrences", "passed"})
_SOURCE_PROVENANCE_KEYS = frozenset({"url", "content_hash", "fetched_at"})
_MODEL_USAGE_KEYS = frozenset({"models"})
_MODEL_USAGE_MODEL_REQUIRED_KEYS = frozenset({"model_id", "provider_id"})
_MODEL_USAGE_MODEL_OPTIONAL_KEYS = frozenset({"input_tokens", "output_tokens"})
_MODEL_USAGE_MODEL_KEYS = _MODEL_USAGE_MODEL_REQUIRED_KEYS | _MODEL_USAGE_MODEL_OPTIONAL_KEYS
_METADATA_KEYS = frozenset(
    {
        "titles",
        "descriptions",
        "text_metrics",
        "warnings",
        "model_usage",
        "stage_timings",
        "prompt_versions",
    }
)
_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "job_id",
        "company_id",
        "brief_id",
        "brief_fingerprint",
        "snapshot_id",
        "snapshot_hash",
        "company_profile_version",
        "direction_id",
        "direction_version",
        "audience_segment_id",
        "audience_version",
        "prompt_set_version",
        "approved_plan_fingerprint",
        "approval_record_id",
        "attempt",
        "status",
        "job_created_at",
        "started_at",
        "finished_at",
        "prompt_versions",
        "model_usage",
        "stage_timings",
        "warnings",
        "source_provenance",
        "artifact_hashes",
        "created_at",
    }
)
_FORBIDDEN_ARTIFACT_FIELDS = frozenset(
    {
        "accesstoken",
        "apikey",
        "authorization",
        "chainofthought",
        "clientsecret",
        "cookie",
        "credential",
        "credentials",
        "hiddenreasoning",
        "idtoken",
        "password",
        "passwordhash",
        "passwd",
        "privatekey",
        "reasoning",
        "refreshtoken",
        "secret",
        "setcookie",
        "thinking",
    }
)
_FORBIDDEN_FIELD_SUFFIXES = (
    "accesstoken",
    "apikey",
    "authorization",
    "chainofthought",
    "clientsecret",
    "cookie",
    "credential",
    "credentials",
    "hiddenreasoning",
    "idtoken",
    "password",
    "passwordhash",
    "privatekey",
    "refreshtoken",
    "secret",
    "setcookie",
    "token",
)
_FORBIDDEN_FIELD_SUBSTRINGS = _FORBIDDEN_ARTIFACT_FIELDS | frozenset(
    {"tokenvalue"}
)
_SENSITIVE_TEXT_PATTERNS = (
    re.compile(r"-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----", re.IGNORECASE),
    re.compile(
        r"\b(?:authorization|api[ _-]?(?:key|token)|"
        r"(?:access|refresh|id|bearer)[ _-]?token|client[ _-]?secret|"
        r"credentials?|password|passwd|secret|set[ _-]?cookie|cookie|token)\s*[:=]\s*"
        r"(?:bearer\s+)?[^\s,;]{6,}",
        re.IGNORECASE,
    ),
    re.compile(
        r"<\s*/?\s*(?:think|analysis|reasoning)\s*>|"
        r"\b(?:hidden[ _-]?reasoning|chain[ _-]?of[ _-]?thought|"
        r"(?:reasoning|thinking)[ _-]?content)\s*[\"']?\s*[:=]",
        re.IGNORECASE,
    ),
)
_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9-]{1,63}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ALTERNATE_IP_COMPONENT = re.compile(r"0x[0-9a-f]+", re.IGNORECASE)
_STRUCTURED_FIELD_NAME = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_COMPACT_LABEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_EMBEDDED_JSON_START = re.compile(r'[\[{\"]')
_CREDENTIAL_LIKE_CORE = (
    r"(?:"
    r"(?:sk|pk|rk|ak)[_-][A-Za-z0-9_-]{8,}"
    r"|AIza[A-Za-z0-9_-]{20,}"
    r"|AKIA[0-9A-Z]{16}"
    r"|gh[pousr]_[A-Za-z0-9]{20,}"
    r"|github_pat_[A-Za-z0-9_]{20,}"
    r"|xox[a-z]-[A-Za-z0-9-]{10,}"
    r"|eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"
    r"|(?:api[-_]?key|credential|secret)[_-][A-Za-z0-9_-]{8,}"
    r"|(?:access|refresh|id)[_-]?token[-_][A-Za-z0-9_-]{8,}"
    r")"
)
_CREDENTIAL_LIKE_LABEL = re.compile(
    rf"^(?:{_CREDENTIAL_LIKE_CORE}|token[_-][A-Za-z0-9_-]{{8,}})$",
    re.IGNORECASE,
)
_CREDENTIAL_LIKE_TEXT = re.compile(
    rf"(?<![A-Za-z0-9._-]){_CREDENTIAL_LIKE_CORE}(?![A-Za-z0-9._-])",
    re.IGNORECASE,
)
_SENSITIVE_SOURCE_QUERY_KEYS = frozenset(
    {
        "apikey",
        "auth",
        "authorization",
        "credential",
        "credentials",
        "hmac",
        "key",
        "sig",
        "sign",
        "signature",
    }
)
_MAX_ARTIFACT_BYTES = MAX_CANONICAL_BYTES
_MAX_PRIMARY_KEYWORD_BYTES = 256
_MAX_EMBEDDED_JSON_FRAGMENTS = 64
_MAX_JSON_STRING_WRAPPERS = 8
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_READ_FLAGS = os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC
_WRITE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
_RENAME_NOREPLACE = 1
_LIBC = ctypes.CDLL(None, use_errno=True)
_RENAMEAT2: Callable[[int, bytes, int, bytes, int], int] | None
try:
    renameat2 = _LIBC.renameat2
except AttributeError:
    _RENAMEAT2 = None
else:
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    _RENAMEAT2 = cast(Callable[[int, bytes, int, bytes, int], int], renameat2)


def _validate_identifier(value: object, field_name: str) -> str:
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a canonical identifier")
    return value


def _path_error(exc: OSError, *, missing_is_not_found: bool) -> Exception:
    if missing_is_not_found and isinstance(exc, FileNotFoundError):
        return NotFound()
    return DataIntegrityError()


def _open_directory_at(parent_fd: int, name: str, *, create: bool) -> int:
    created = False
    if create:
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_fd)
            created = True
        except FileExistsError:
            pass
        except OSError as exc:
            raise DataIntegrityError from exc
    try:
        descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except OSError as exc:
        if created:
            try:
                os.rmdir(name, dir_fd=parent_fd)
                os.fsync(parent_fd)
            except OSError as cleanup_exc:
                exc.add_note(f"failed to remove unopenable directory: {cleanup_exc!r}")
        raise _path_error(exc, missing_is_not_found=not create) from exc
    if created:
        try:
            os.fsync(descriptor)
            os.fsync(parent_fd)
        except OSError as exc:
            os.close(descriptor)
            raise DataIntegrityError from exc
    return descriptor


def _validate_directory(descriptor: int, mode: int) -> os.stat_result:
    try:
        directory_stat = os.fstat(descriptor)
    except OSError as exc:
        raise DataIntegrityError from exc
    if (
        not stat.S_ISDIR(directory_stat.st_mode)
        or stat.S_IMODE(directory_stat.st_mode) != mode
        or directory_stat.st_uid != os.geteuid()
        or directory_stat.st_gid != os.getegid()
    ):
        raise DataIntegrityError
    return directory_stat


def _aware_datetime(value: object, field_name: str) -> datetime:
    if type(value) is not datetime or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _parse_aware_datetime(value: object) -> datetime:
    if type(value) is not str:
        raise ValueError
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError
    return parsed.astimezone(UTC)


def _utc_iso(value: datetime | None) -> JsonValue:
    if value is None:
        return None
    return _aware_datetime(value, "timestamp").isoformat()


def _compact_field_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


def _is_forbidden_field(value: str) -> bool:
    compact = _compact_field_name(value)
    return (
        compact in _FORBIDDEN_ARTIFACT_FIELDS
        or compact.endswith(_FORBIDDEN_FIELD_SUFFIXES)
        or any(fragment in compact for fragment in _FORBIDDEN_FIELD_SUBSTRINGS)
    )


def _reject_raw_json_artifact_text(value: str) -> None:
    candidate = value.strip()
    for _ in range(_MAX_JSON_STRING_WRAPPERS):
        if len(candidate) < 2 or candidate[0] not in "[{\"":
            return
        try:
            decoded = json.loads(candidate)
        except json.JSONDecodeError:
            return
        except (RecursionError, UnicodeDecodeError, ValueError) as exc:
            raise ValueError("artifact text contains invalid structured output") from exc
        if type(decoded) is dict or type(decoded) is list:
            raise ValueError("artifact text must not contain raw structured provider output")
        if type(decoded) is not str:
            return
        candidate = decoded.strip()

    if len(candidate) < 2 or candidate[0] not in "[{\"":
        return
    try:
        json.loads(candidate)
    except json.JSONDecodeError:
        return
    except (RecursionError, UnicodeDecodeError, ValueError) as exc:
        raise ValueError("artifact text contains invalid structured output") from exc
    raise ValueError("artifact text contains too deeply nested structured output")


def _reject_embedded_json_artifact_text(value: str, *, _string_depth: int = 0) -> None:
    if _string_depth > _MAX_JSON_STRING_WRAPPERS:
        raise ValueError("artifact text contains too deeply nested structured output")
    decoder = json.JSONDecoder()
    for fragment_index, match in enumerate(_EMBEDDED_JSON_START.finditer(value), start=1):
        if fragment_index > _MAX_EMBEDDED_JSON_FRAGMENTS:
            raise ValueError("artifact text contains too many embedded JSON fragments")
        start = match.start()
        try:
            decoded, _ = decoder.raw_decode(value, start)
        except json.JSONDecodeError:
            continue
        except (RecursionError, UnicodeDecodeError, ValueError) as exc:
            raise ValueError("artifact text contains invalid structured output") from exc
        if type(decoded) is dict or type(decoded) is list:
            raise ValueError("artifact text must not contain embedded structured output")
        if type(decoded) is str:
            _reject_raw_json_artifact_text(decoded)
            _reject_embedded_json_artifact_text(
                decoded,
                _string_depth=_string_depth + 1,
            )


def _reject_sensitive_text(value: str) -> None:
    _reject_raw_json_artifact_text(value)
    _reject_embedded_json_artifact_text(value)
    if _CREDENTIAL_LIKE_TEXT.search(value) is not None:
        raise ValueError("artifact text contains a credential")
    if any(pattern.search(value) is not None for pattern in _SENSITIVE_TEXT_PATTERNS):
        raise ValueError("artifact text contains a credential or hidden reasoning")


def _reject_forbidden_artifact_fields(value: JsonValue) -> None:
    if type(value) is dict:
        for key, nested in value.items():
            if _is_forbidden_field(key):
                raise ValueError(f"forbidden artifact field: {key}")
            _reject_forbidden_artifact_fields(nested)
    elif type(value) is list:
        for nested in value:
            _reject_forbidden_artifact_fields(nested)
    elif type(value) is str:
        _reject_sensitive_text(value)


def _is_alternate_ip_literal(hostname: str) -> bool:
    components = hostname.split(".")
    return bool(components) and all(
        component.isdecimal() or _ALTERNATE_IP_COMPONENT.fullmatch(component) is not None
        for component in components
    )


def _validated_source_url(value: object) -> str:
    if type(value) is not str:
        raise ValueError("source URL must be a string")
    try:
        normalized = _normalize_url(value)
        query = parse_qsl(
            urlsplit(normalized).query,
            keep_blank_values=True,
            max_num_fields=128,
        )
    except ValueError as exc:
        raise ValueError("source URL is invalid") from exc
    if normalized != value:
        raise ValueError("source URL must use canonical form")
    hostname = urlsplit(normalized).hostname
    if hostname is None:
        raise ValueError("source URL is invalid")
    normalized_hostname = hostname.rstrip(".").casefold()
    if normalized_hostname == "localhost" or normalized_hostname.endswith(".localhost"):
        raise ValueError("source URL must not use a local host")
    try:
        literal_host = ip_address(hostname)
    except ValueError:
        if _is_alternate_ip_literal(hostname):
            raise ValueError("source URL must not use an alternate IP literal") from None
    else:
        if not literal_host.is_global:
            raise ValueError("source URL must not use a non-public literal host")
    for key, _ in query:
        compact = _compact_field_name(key)
        if (
            _is_forbidden_field(key)
            or compact in _SENSITIVE_SOURCE_QUERY_KEYS
            or "token" in compact
            or "signature" in compact
        ):
            raise ValueError("source URL must not contain credential parameters")
    _reject_sensitive_text(value)
    return value


def _source_provenance(source: JsonValue) -> dict[str, JsonValue]:
    if type(source) is not dict:
        raise ValueError("source must be a JSON object")
    if set(source) != _SOURCE_PROVENANCE_KEYS:
        raise ValueError("source must contain only url, content_hash, and fetched_at")
    url = source["url"]
    content_hash = source["content_hash"]
    fetched_at = source["fetched_at"]
    url = _validated_source_url(url)
    if type(content_hash) is not str or _SHA256.fullmatch(content_hash) is None:
        raise ValueError("source content_hash must be a lowercase SHA-256 digest")
    try:
        normalized_fetched_at = _parse_aware_datetime(fetched_at).isoformat()
    except ValueError as exc:
        raise ValueError(
            "source fetched_at must be a timezone-aware ISO timestamp"
        ) from exc
    return {
        "url": url,
        "content_hash": content_hash,
        "fetched_at": normalized_fetched_at,
    }


def _normalized_source(source: JsonValue) -> JsonValue:
    return cast(JsonValue, _source_provenance(source))


def _validate_five_strings(value: object, field_name: str) -> None:
    if (
        type(value) is not tuple
        or len(value) != 5
        or any(type(item) is not str or not item.strip() for item in value)
    ):
        raise ValueError(f"{field_name} must be a five-item string tuple")
    for item in value:
        _reject_sensitive_text(item)


def _validate_structured_field_name(value: object, field_name: str) -> None:
    if type(value) is not str or _STRUCTURED_FIELD_NAME.fullmatch(value) is None:
        raise ValueError(f"{field_name} must use canonical field names")


def _validate_compact_label(value: object, field_name: str) -> None:
    if type(value) is not str or _COMPACT_LABEL.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a compact identifier")
    if _CREDENTIAL_LIKE_LABEL.fullmatch(value) is not None:
        raise ValueError(f"{field_name} must not contain credential-shaped data")


def _validate_keyword_qa(value: JsonValue) -> None:
    if type(value) is not dict or set(value) != _KEYWORD_QA_KEYS:
        raise ValueError("keyword_qa must contain primary_keyword, occurrences, and passed")
    primary_keyword = value["primary_keyword"]
    if type(primary_keyword) is not str or not primary_keyword.strip():
        raise ValueError("keyword_qa primary_keyword must be a non-empty string")
    try:
        primary_keyword_bytes = primary_keyword.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("keyword_qa primary_keyword must be valid UTF-8") from exc
    if len(primary_keyword_bytes) > _MAX_PRIMARY_KEYWORD_BYTES or "\n" in primary_keyword:
        raise ValueError("keyword_qa primary_keyword is not a compact keyword")
    _reject_sensitive_text(primary_keyword)
    occurrences = value["occurrences"]
    if type(occurrences) is not int or occurrences < 0:
        raise ValueError("keyword_qa occurrences must be a non-negative integer")
    if type(value["passed"]) is not bool:
        raise ValueError("keyword_qa passed must be a boolean")


def _validate_nonnegative_integer_mapping(value: JsonValue, field_name: str) -> None:
    if type(value) is not dict:
        raise ValueError(f"{field_name} must be a JSON object")
    for key, integer_value in value.items():
        _validate_structured_field_name(key, field_name)
        if type(integer_value) is not int or integer_value < 0:
            raise ValueError(f"{field_name} must map field names to non-negative integers")


def _validate_model_usage(value: JsonValue) -> None:
    if type(value) is not dict or set(value) != _MODEL_USAGE_KEYS:
        raise ValueError("model_usage must contain only models")
    models = value["models"]
    if type(models) is not list or not models:
        raise ValueError("model_usage models must be a non-empty list")
    for model in models:
        if type(model) is not dict or not _MODEL_USAGE_MODEL_REQUIRED_KEYS.issubset(model):
            raise ValueError("model_usage models must contain JSON objects")
        if not set(model).issubset(_MODEL_USAGE_MODEL_KEYS):
            raise ValueError("model_usage model contains unsupported fields")
        for field_name in ("model_id", "provider_id"):
            _validate_compact_label(model[field_name], f"model_usage {field_name}")
        for field_name in ("input_tokens", "output_tokens"):
            if field_name not in model:
                continue
            token_count = model[field_name]
            if (
                type(token_count) is not int or token_count < 0
            ):
                raise ValueError(f"model_usage {field_name} must be a non-negative integer")


def _validate_prompt_versions(value: JsonValue) -> None:
    if type(value) is not dict or not value:
        raise ValueError("prompt_versions must be a non-empty JSON object")
    for key, version in value.items():
        _validate_structured_field_name(key, "prompt_versions")
        _validate_compact_label(version, "prompt_versions version")


def _clone_json(value: JsonValue) -> JsonValue:
    return cast(JsonValue, json.loads(canonical_json(value)))


def _rename_noreplace(parent_fd: int, source: str, target: str) -> None:
    if _RENAMEAT2 is None:
        raise OSError(errno.ENOSYS, os.strerror(errno.ENOSYS), target)
    ctypes.set_errno(0)
    result = _RENAMEAT2(
        parent_fd,
        os.fsencode(source),
        parent_fd,
        os.fsencode(target),
        _RENAME_NOREPLACE,
    )
    if result != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number), target)


def _validate_job_provenance(job: SeoJob) -> None:
    for field_name in (
        "brief_id",
        "snapshot_id",
        "direction_id",
        "audience_segment_id",
    ):
        _validate_identifier(getattr(job, field_name), field_name)
    for field_name in ("brief_fingerprint", "snapshot_hash"):
        value = getattr(job, field_name)
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    if type(job.attempt) is not int or job.attempt < 1:
        raise ValueError("attempt must be a positive integer")
    for field_name in (
        "company_profile_version",
        "direction_version",
        "audience_version",
        "prompt_set_version",
    ):
        value = getattr(job, field_name)
        if type(value) is not int or value < 1:
            raise ValueError(f"{field_name} must be a positive integer")
    if type(job.state) is not JobState:
        raise ValueError("state must be a JobState")
    if job.state is not JobState.SUCCEEDED:
        raise ValueError("state must be SUCCEEDED to publish artifacts")
    if (
        type(job.approved_plan_fingerprint) is not str
        or _SHA256.fullmatch(job.approved_plan_fingerprint) is None
    ):
        raise ValueError(
            "approved_plan_fingerprint must be a lowercase SHA-256 digest"
        )
    _validate_identifier(job.approval_record_id, "approval_record_id")
    created_at = _aware_datetime(job.created_at, "created_at")
    if job.started_at is None:
        raise ValueError("started_at is required for a successful job")
    started_at = _aware_datetime(job.started_at, "started_at")
    if started_at < created_at:
        raise ValueError("started_at cannot precede created_at")
    if job.finished_at is None:
        raise ValueError("finished_at is required for a successful job")
    finished_at = _aware_datetime(job.finished_at, "finished_at")
    if finished_at < started_at:
        raise ValueError("finished_at cannot precede started_at")


def _expected_job_manifest_provenance(job: SeoJob) -> dict[str, JsonValue]:
    return {
        "job_id": job.job_id,
        "company_id": job.company_id,
        "brief_id": job.brief_id,
        "brief_fingerprint": job.brief_fingerprint,
        "snapshot_id": job.snapshot_id,
        "snapshot_hash": job.snapshot_hash,
        "direction_id": job.direction_id,
        "audience_segment_id": job.audience_segment_id,
        "approval_record_id": job.approval_record_id,
        "approved_plan_fingerprint": job.approved_plan_fingerprint,
        "attempt": job.attempt,
        "company_profile_version": job.company_profile_version,
        "direction_version": job.direction_version,
        "audience_version": job.audience_version,
        "prompt_set_version": job.prompt_set_version,
        "status": job.state.value,
        "job_created_at": _utc_iso(job.created_at),
        "started_at": _utc_iso(job.started_at),
        "finished_at": _utc_iso(job.finished_at),
    }


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    content_markdown: str
    titles: tuple[str, str, str, str, str]
    descriptions: tuple[str, str, str, str, str]
    keyword_qa: JsonValue
    text_metrics: JsonValue
    sources: tuple[JsonValue, ...]
    warnings: tuple[str, ...]
    model_usage: JsonValue
    stage_timings: JsonValue
    prompt_versions: JsonValue

    def __post_init__(self) -> None:
        if type(self.content_markdown) is not str:
            raise ValueError("content_markdown must be a string")
        try:
            content_bytes = self.content_markdown.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError("content_markdown must be valid UTF-8") from exc
        if not self.content_markdown.strip() or len(content_bytes) > _MAX_ARTIFACT_BYTES:
            raise ValueError("content_markdown must be non-empty and at most 1 MiB")
        _reject_sensitive_text(self.content_markdown)
        _validate_five_strings(self.titles, "titles")
        _validate_five_strings(self.descriptions, "descriptions")
        if type(self.sources) is not tuple:
            raise ValueError("sources must be a tuple")
        if type(self.warnings) is not tuple or any(
            type(warning) is not str or not warning.strip() for warning in self.warnings
        ):
            raise ValueError("warnings must be a string tuple")
        for warning in self.warnings:
            _reject_sensitive_text(warning)
        structured_values = (
            self.keyword_qa,
            self.text_metrics,
            self.model_usage,
            self.stage_timings,
            self.prompt_versions,
        )
        for value in structured_values:
            canonical_json(value)
            _reject_forbidden_artifact_fields(value)
        source_values = list(self.sources)
        canonical_json(source_values)
        _reject_forbidden_artifact_fields(source_values)
        for source in self.sources:
            _source_provenance(source)
        _validate_keyword_qa(self.keyword_qa)
        _validate_nonnegative_integer_mapping(self.text_metrics, "text_metrics")
        _validate_model_usage(self.model_usage)
        _validate_nonnegative_integer_mapping(self.stage_timings, "stage_timings")
        _validate_prompt_versions(self.prompt_versions)


def _snapshot_execution_result(result: ExecutionResult) -> ExecutionResult:
    result.__post_init__()
    return ExecutionResult(
        content_markdown=result.content_markdown,
        titles=result.titles,
        descriptions=result.descriptions,
        keyword_qa=_clone_json(result.keyword_qa),
        text_metrics=_clone_json(result.text_metrics),
        sources=tuple(_clone_json(source) for source in result.sources),
        warnings=tuple(result.warnings),
        model_usage=_clone_json(result.model_usage),
        stage_timings=_clone_json(result.stage_timings),
        prompt_versions=_clone_json(result.prompt_versions),
    )


@dataclass(frozen=True, slots=True)
class ArtifactManifest:
    job_id: str
    company_id: str
    snapshot_hash: str
    artifact_hashes: dict[str, str]
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_hashes",
            cast(dict[str, str], MappingProxyType(dict(self.artifact_hashes))),
        )


class ArtifactStore:
    """Persist and open one immutable bundle under an explicit artifact root."""

    def __init__(
        self,
        artifact_root: Path,
        *,
        clock: Callable[[], datetime] | None = None,
        before_rename: Callable[[], None] | None = None,
        after_validation: Callable[[], None] | None = None,
    ) -> None:
        if (
            not isinstance(artifact_root, Path)
            or not artifact_root.is_absolute()
            or artifact_root.anchor != "/"
            or len(artifact_root.parts) < 2
            or any(part in {".", ".."} for part in artifact_root.parts)
        ):
            raise ValueError("artifact_root must be an absolute dedicated directory")
        self._artifact_root = artifact_root
        self._clock = clock or (lambda: datetime.now(UTC))
        self._before_rename = before_rename or (lambda: None)
        self._after_validation = after_validation or (lambda: None)

    def _manifest_path_for_job(self, company_id: str, job_id: str) -> Path:
        return (
            self._artifact_root
            / "companies"
            / company_id
            / "jobs"
            / job_id
            / "manifest.json"
        )

    def manifest_path_for_job(self, company_id: str, job_id: str) -> Path:
        """Return the one canonical manifest location for an artifact identity."""
        return self._manifest_path_for_job(
            _validate_identifier(company_id, "company_id"),
            _validate_identifier(job_id, "job_id"),
        )

    @contextmanager
    def _jobs_directory(self, company_id: str, *, create: bool) -> Iterator[int]:
        root_fd: int | None = None
        try:
            root_fd = os.open("/", _DIRECTORY_FLAGS)
            for component in self._artifact_root.parts[1:]:
                next_fd = _open_directory_at(root_fd, component, create=create)
                os.close(root_fd)
                root_fd = next_fd
        except OSError as exc:
            if root_fd is not None:
                os.close(root_fd)
            raise DataIntegrityError from exc
        except BaseException:
            if root_fd is not None:
                os.close(root_fd)
            raise
        if root_fd is None:
            raise DataIntegrityError
        descriptors = [root_fd]
        try:
            _validate_directory(root_fd, 0o700)
            companies_fd = _open_directory_at(root_fd, "companies", create=create)
            descriptors.append(companies_fd)
            _validate_directory(companies_fd, 0o700)
            company_fd = _open_directory_at(companies_fd, company_id, create=create)
            descriptors.append(company_fd)
            _validate_directory(company_fd, 0o700)
            jobs_fd = _open_directory_at(company_fd, "jobs", create=create)
            descriptors.append(jobs_fd)
            _validate_directory(jobs_fd, 0o700)
            yield jobs_fd
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)

    @staticmethod
    def _write_new_file(bundle_fd: int, name: str, payload: bytes) -> None:
        try:
            descriptor = os.open(name, _WRITE_FLAGS, mode=0o600, dir_fd=bundle_fd)
        except OSError as exc:
            raise DataIntegrityError from exc
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as artifact:
                artifact.write(payload)
                artifact.flush()
            os.fchmod(descriptor, 0o440)
            os.fsync(descriptor)
        except OSError as exc:
            raise DataIntegrityError from exc
        finally:
            os.close(descriptor)

    @staticmethod
    def _read_file(bundle_fd: int, name: str, owner: tuple[int, int]) -> bytes:
        try:
            descriptor = os.open(name, _READ_FLAGS, dir_fd=bundle_fd)
        except OSError as exc:
            raise DataIntegrityError from exc
        try:
            file_stat = os.fstat(descriptor)
            if (
                not stat.S_ISREG(file_stat.st_mode)
                or stat.S_IMODE(file_stat.st_mode) != 0o440
                or file_stat.st_nlink != 1
                or (file_stat.st_uid, file_stat.st_gid) != owner
                or file_stat.st_size > _MAX_ARTIFACT_BYTES
            ):
                raise DataIntegrityError
            with os.fdopen(descriptor, "rb", closefd=False) as artifact:
                payload = artifact.read(_MAX_ARTIFACT_BYTES + 1)
            if len(payload) > _MAX_ARTIFACT_BYTES:
                raise DataIntegrityError
            return payload
        finally:
            os.close(descriptor)

    @staticmethod
    def _payloads(result: ExecutionResult) -> tuple[dict[str, bytes], dict[str, str]]:
        result.__post_init__()
        normalized_sources = [_normalized_source(source) for source in result.sources]
        metadata: JsonValue = {
            "titles": list(result.titles),
            "descriptions": list(result.descriptions),
            "text_metrics": result.text_metrics,
            "warnings": list(result.warnings),
            "model_usage": result.model_usage,
            "stage_timings": result.stage_timings,
            "prompt_versions": result.prompt_versions,
        }
        payloads = {
            "content.md": result.content_markdown.encode("utf-8"),
            "metadata.json": canonical_json(metadata),
            "qa.json": canonical_json(result.keyword_qa),
            "sources.json": canonical_json(normalized_sources),
        }
        artifact_hashes = {
            name: hashlib.sha256(payload).hexdigest() for name, payload in payloads.items()
        }
        return payloads, artifact_hashes

    @staticmethod
    def _manifest_value(
        job: SeoJob,
        result: ExecutionResult,
        artifact_hashes: dict[str, str],
        created_at: datetime,
    ) -> JsonValue:
        source_provenance: list[JsonValue] = [
            _source_provenance(source) for source in result.sources
        ]
        return {
            "schema_version": 1,
            "job_id": job.job_id,
            "company_id": job.company_id,
            "brief_id": job.brief_id,
            "brief_fingerprint": job.brief_fingerprint,
            "snapshot_id": job.snapshot_id,
            "snapshot_hash": job.snapshot_hash,
            "company_profile_version": job.company_profile_version,
            "direction_id": job.direction_id,
            "direction_version": job.direction_version,
            "audience_segment_id": job.audience_segment_id,
            "audience_version": job.audience_version,
            "prompt_set_version": job.prompt_set_version,
            "approved_plan_fingerprint": job.approved_plan_fingerprint,
            "approval_record_id": job.approval_record_id,
            "attempt": job.attempt,
            "status": JobState.SUCCEEDED.value,
            "job_created_at": _utc_iso(job.created_at),
            "started_at": _utc_iso(job.started_at),
            "finished_at": _utc_iso(job.finished_at),
            "prompt_versions": result.prompt_versions,
            "model_usage": result.model_usage,
            "stage_timings": result.stage_timings,
            "warnings": list(result.warnings),
            "source_provenance": source_provenance,
            "artifact_hashes": cast(JsonValue, dict(artifact_hashes)),
            "created_at": _utc_iso(created_at),
        }

    def _verified_existing_manifest(
        self,
        bundle_fd: int,
        job: SeoJob,
        result: ExecutionResult,
        expected_payloads: dict[str, bytes],
        artifact_hashes: dict[str, str],
    ) -> ArtifactManifest:
        actual_payloads, _, manifest = self._validated_bundle(
            bundle_fd, job.company_id, job.job_id
        )
        for name, expected_payload in expected_payloads.items():
            if actual_payloads[name] != expected_payload:
                raise DataIntegrityError
        expected_manifest = self._manifest_value(
            job, result, artifact_hashes, manifest.created_at
        )
        if actual_payloads["manifest.json"] != canonical_json(expected_manifest):
            raise DataIntegrityError
        return manifest

    def _validated_bundle(
        self,
        bundle_fd: int,
        company_id: str,
        job_id: str,
    ) -> tuple[dict[str, bytes], dict[str, JsonValue], ArtifactManifest]:
        bundle_stat = _validate_directory(bundle_fd, 0o550)
        owner = (bundle_stat.st_uid, bundle_stat.st_gid)
        try:
            names = set(os.listdir(bundle_fd))
        except OSError as exc:
            raise DataIntegrityError from exc
        if names != _ARTIFACT_NAMES:
            raise DataIntegrityError
        actual_payloads = {
            name: self._read_file(bundle_fd, name, owner) for name in _ARTIFACT_NAMES
        }
        try:
            content_markdown = actual_payloads["content.md"].decode("utf-8")
            if not content_markdown.strip():
                raise ValueError
            _reject_sensitive_text(content_markdown)
            json_values = {
                name: json.loads(actual_payloads[name])
                for name in _ARTIFACT_NAMES
                if name.endswith(".json")
            }
            for name, value in json_values.items():
                if actual_payloads[name] != canonical_json(value):
                    raise ValueError
                _reject_forbidden_artifact_fields(value)
            metadata = json_values["metadata.json"]
            sources = json_values["sources.json"]
            manifest_value = json_values["manifest.json"]
            if (
                type(metadata) is not dict
                or set(metadata) != _METADATA_KEYS
                or type(sources) is not list
                or type(manifest_value) is not dict
                or set(manifest_value) != _MANIFEST_KEYS
            ):
                raise ValueError
            for field_name in ("titles", "descriptions"):
                field = metadata[field_name]
                if (
                    type(field) is not list
                    or len(field) != 5
                    or any(type(item) is not str or not item.strip() for item in field)
                ):
                    raise ValueError
                for item in field:
                    _reject_sensitive_text(item)
            warnings = metadata["warnings"]
            if type(warnings) is not list or any(
                type(warning) is not str or not warning.strip() for warning in warnings
            ):
                raise ValueError
            for warning in warnings:
                _reject_sensitive_text(warning)
            _validate_keyword_qa(cast(JsonValue, json_values["qa.json"]))
            _validate_nonnegative_integer_mapping(
                cast(JsonValue, metadata["text_metrics"]), "text_metrics"
            )
            _validate_model_usage(cast(JsonValue, metadata["model_usage"]))
            _validate_nonnegative_integer_mapping(
                cast(JsonValue, metadata["stage_timings"]), "stage_timings"
            )
            _validate_prompt_versions(cast(JsonValue, metadata["prompt_versions"]))
            schema_version = manifest_value["schema_version"]
            if type(schema_version) is not int or schema_version != 1:
                raise ValueError
            for field_name in (
                "job_id",
                "company_id",
                "brief_id",
                "snapshot_id",
                "direction_id",
                "audience_segment_id",
                "approval_record_id",
            ):
                _validate_identifier(manifest_value[field_name], field_name)
            if (
                manifest_value["job_id"] != job_id
                or manifest_value["company_id"] != company_id
            ):
                raise ValueError
            for field_name in (
                "brief_fingerprint",
                "snapshot_hash",
                "approved_plan_fingerprint",
            ):
                value = manifest_value[field_name]
                if type(value) is not str or _SHA256.fullmatch(value) is None:
                    raise ValueError
            attempt = manifest_value["attempt"]
            if type(attempt) is not int or attempt < 1:
                raise ValueError
            for field_name in (
                "company_profile_version",
                "direction_version",
                "audience_version",
                "prompt_set_version",
            ):
                value = manifest_value[field_name]
                if type(value) is not int or value < 1:
                    raise ValueError
            status_value = manifest_value["status"]
            if status_value != JobState.SUCCEEDED.value:
                raise ValueError
            timestamps: dict[str, datetime] = {}
            for field_name in ("job_created_at", "started_at", "finished_at"):
                timestamp = manifest_value[field_name]
                if type(timestamp) is not str:
                    raise ValueError
                parsed_timestamp = _parse_aware_datetime(timestamp)
                if parsed_timestamp.isoformat() != timestamp:
                    raise ValueError
                timestamps[field_name] = parsed_timestamp
            created_at = _parse_aware_datetime(manifest_value["created_at"])
            if created_at.isoformat() != manifest_value["created_at"]:
                raise ValueError
            if not (
                timestamps["job_created_at"]
                <= timestamps["started_at"]
                <= timestamps["finished_at"]
                <= created_at
            ):
                raise ValueError
            artifact_hashes_value = manifest_value["artifact_hashes"]
            if (
                type(artifact_hashes_value) is not dict
                or set(artifact_hashes_value) != _PAYLOAD_NAMES
            ):
                raise ValueError
            artifact_hashes: dict[str, str] = {}
            for artifact_name in _PAYLOAD_NAMES:
                digest = artifact_hashes_value[artifact_name]
                if type(digest) is not str or _SHA256.fullmatch(digest) is None:
                    raise ValueError
                if digest != hashlib.sha256(actual_payloads[artifact_name]).hexdigest():
                    raise ValueError
                artifact_hashes[artifact_name] = digest
            for field_name in ("prompt_versions", "model_usage", "stage_timings"):
                if canonical_json(cast(JsonValue, manifest_value[field_name])) != canonical_json(
                    cast(JsonValue, metadata[field_name])
                ):
                    raise ValueError
            if manifest_value["warnings"] != warnings:
                raise ValueError
            source_provenance = manifest_value["source_provenance"]
            if type(source_provenance) is not list or len(source_provenance) != len(
                sources
            ):
                raise ValueError
            for source, provenance in zip(sources, source_provenance, strict=True):
                expected_provenance = _source_provenance(cast(JsonValue, source))
                if (
                    type(provenance) is not dict
                    or set(provenance) != {"url", "content_hash", "fetched_at"}
                    or canonical_json(cast(JsonValue, provenance))
                    != canonical_json(cast(JsonValue, expected_provenance))
                    or canonical_json(cast(JsonValue, source))
                    != canonical_json(_normalized_source(cast(JsonValue, source)))
                ):
                    raise ValueError
        except (
            CanonicalizationError,
            KeyError,
            RecursionError,
            TypeError,
            ValueError,
            UnicodeDecodeError,
        ) as exc:
            raise DataIntegrityError from exc
        manifest = ArtifactManifest(
            job_id=job_id,
            company_id=company_id,
            snapshot_hash=cast(str, manifest_value["snapshot_hash"]),
            artifact_hashes=artifact_hashes,
            created_at=created_at,
        )
        return actual_payloads, cast(dict[str, JsonValue], manifest_value), manifest

    @staticmethod
    def _create_staging_directory(
        jobs_fd: int,
        job_id: str,
        bundle_digest: str,
    ) -> tuple[str, int]:
        for _ in range(16):
            name = f".{job_id}.{bundle_digest}.{secrets.token_hex(16)}.tmp"
            try:
                os.mkdir(name, mode=0o700, dir_fd=jobs_fd)
            except FileExistsError:
                continue
            except OSError as exc:
                raise DataIntegrityError from exc
            try:
                descriptor = _open_directory_at(jobs_fd, name, create=False)
            except BaseException as exc:
                try:
                    os.rmdir(name, dir_fd=jobs_fd)
                    os.fsync(jobs_fd)
                except OSError as cleanup_exc:
                    exc.add_note(
                        f"failed to remove unopenable staging directory: {cleanup_exc!r}"
                    )
                raise
            return name, descriptor
        raise DataIntegrityError

    @staticmethod
    def _staging_name_matches_descriptor(
        jobs_fd: int,
        staging_fd: int,
        staging_name: str,
    ) -> bool:
        try:
            named_stat = os.stat(
                staging_name,
                dir_fd=jobs_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise DataIntegrityError from exc
        try:
            staging_stat = os.fstat(staging_fd)
        except OSError as exc:
            raise DataIntegrityError from exc
        return (
            stat.S_ISDIR(named_stat.st_mode)
            and stat.S_ISDIR(staging_stat.st_mode)
            and (named_stat.st_dev, named_stat.st_ino)
            == (staging_stat.st_dev, staging_stat.st_ino)
        )

    @staticmethod
    def _cleanup_staging(jobs_fd: int, staging_fd: int, staging_name: str) -> None:
        if not ArtifactStore._staging_name_matches_descriptor(
            jobs_fd,
            staging_fd,
            staging_name,
        ):
            return
        try:
            os.fchmod(staging_fd, 0o700)
            for name in _ARTIFACT_NAMES:
                try:
                    os.unlink(name, dir_fd=staging_fd)
                except FileNotFoundError:
                    pass
            os.rmdir(staging_name, dir_fd=jobs_fd)
            os.fsync(jobs_fd)
        except OSError as exc:
            raise DataIntegrityError from exc

    @staticmethod
    def _bundle_digest(payloads: dict[str, bytes]) -> str:
        digest = hashlib.sha256()
        for name in sorted(payloads):
            name_bytes = name.encode("ascii")
            payload = payloads[name]
            digest.update(len(name_bytes).to_bytes(8, "big"))
            digest.update(name_bytes)
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
        return digest.hexdigest()

    def write_bundle(self, job: SeoJob, result: ExecutionResult) -> ArtifactManifest:
        company_id = _validate_identifier(job.company_id, "company_id")
        job_id = _validate_identifier(job.job_id, "job_id")
        _validate_job_provenance(job)
        result = _snapshot_execution_result(result)
        payloads, artifact_hashes = self._payloads(result)
        with self._jobs_directory(company_id, create=True) as jobs_fd:
            try:
                bundle_fd = _open_directory_at(jobs_fd, job_id, create=False)
            except NotFound:
                bundle_fd = None
            if bundle_fd is not None:
                try:
                    return self._verified_existing_manifest(
                        bundle_fd, job, result, payloads, artifact_hashes
                    )
                finally:
                    os.close(bundle_fd)

            created_at = _aware_datetime(self._clock(), "artifact clock")
            finished_at = _aware_datetime(job.finished_at, "finished_at")
            if created_at < finished_at:
                raise ValueError("artifact clock cannot precede finished_at")
            manifest_value = self._manifest_value(
                job, result, artifact_hashes, created_at
            )
            staged_payloads = dict(payloads)
            staged_payloads["manifest.json"] = canonical_json(manifest_value)
            staging_name, staging_fd = self._create_staging_directory(
                jobs_fd,
                job_id,
                self._bundle_digest(staged_payloads),
            )
            published = False
            target_race = False
            rename_outcome = "not_attempted"
            try:
                try:
                    try:
                        for name, payload in staged_payloads.items():
                            self._write_new_file(staging_fd, name, payload)
                        os.fchmod(staging_fd, 0o550)
                        os.fsync(staging_fd)
                        self._before_rename()
                        actual_staged, _, _ = self._validated_bundle(
                            staging_fd,
                            company_id,
                            job_id,
                        )
                        if actual_staged != staged_payloads:
                            raise DataIntegrityError
                        self._after_validation()
                        actual_staged, _, _ = self._validated_bundle(
                            staging_fd,
                            company_id,
                            job_id,
                        )
                        if actual_staged != staged_payloads:
                            raise DataIntegrityError
                        if not self._staging_name_matches_descriptor(
                            jobs_fd, staging_fd, staging_name
                        ):
                            raise DataIntegrityError
                        rename_outcome = "unknown"
                        try:
                            _rename_noreplace(jobs_fd, staging_name, job_id)
                        except OSError as exc:
                            rename_outcome = "not_published"
                            if exc.errno not in {errno.EEXIST, errno.ENOTEMPTY}:
                                raise
                            target_race = True
                        else:
                            published = True
                            rename_outcome = "published"
                            os.fsync(jobs_fd)
                    except OSError as exc:
                        raise DataIntegrityError from exc
                except BaseException:
                    if not published and rename_outcome != "unknown":
                        with suppress(BaseException):
                            self._cleanup_staging(jobs_fd, staging_fd, staging_name)
                    raise
                else:
                    if not published:
                        self._cleanup_staging(jobs_fd, staging_fd, staging_name)
            finally:
                os.close(staging_fd)

            if target_race:
                bundle_fd = _open_directory_at(jobs_fd, job_id, create=False)
                try:
                    return self._verified_existing_manifest(
                        bundle_fd, job, result, payloads, artifact_hashes
                    )
                finally:
                    os.close(bundle_fd)

            if published:
                bundle_fd = _open_directory_at(jobs_fd, job_id, create=False)
                try:
                    return self._verified_existing_manifest(
                        bundle_fd, job, result, payloads, artifact_hashes
                    )
                finally:
                    os.close(bundle_fd)

        return ArtifactManifest(
            job_id=job.job_id,
            company_id=job.company_id,
            snapshot_hash=job.snapshot_hash,
            artifact_hashes=artifact_hashes,
            created_at=created_at,
        )

    def _validated_payloads_for_job(self, job: SeoJob) -> dict[str, bytes]:
        try:
            _validate_job_provenance(job)
        except ValueError as exc:
            raise DataIntegrityError from exc
        with self._jobs_directory(job.company_id, create=False) as jobs_fd:
            bundle_fd = _open_directory_at(jobs_fd, job.job_id, create=False)
            try:
                payloads, manifest_value, _ = self._validated_bundle(
                    bundle_fd, job.company_id, job.job_id
                )
            finally:
                os.close(bundle_fd)
        expected_provenance = _expected_job_manifest_provenance(job)
        if any(
            manifest_value[field_name] != value
            for field_name, value in expected_provenance.items()
        ):
            raise DataIntegrityError
        return payloads

    def verify_manifest_for_job(self, job: SeoJob) -> Path:
        """Verify a durable bundle before returning its canonical manifest path."""
        self._validated_payloads_for_job(job)
        return self._manifest_path_for_job(job.company_id, job.job_id)

    def open_artifact_for_job(self, job: SeoJob, name: str) -> BinaryIO:
        """Open one artifact only when its validated manifest exactly binds to ``job``."""
        if name not in _ARTIFACT_NAMES:
            raise ValueError("artifact name is not allowed")
        if job.artifact_manifest_path != str(
            self._manifest_path_for_job(job.company_id, job.job_id)
        ):
            raise DataIntegrityError
        payloads = self._validated_payloads_for_job(job)
        return cast(BinaryIO, BytesIO(payloads[name]))

    def open_artifact(self, company_id: str, job_id: str, name: str) -> BinaryIO:
        company_id = _validate_identifier(company_id, "company_id")
        job_id = _validate_identifier(job_id, "job_id")
        if name not in _ARTIFACT_NAMES:
            raise ValueError("artifact name is not allowed")
        with self._jobs_directory(company_id, create=False) as jobs_fd:
            bundle_fd = _open_directory_at(jobs_fd, job_id, create=False)
            try:
                payloads, _, _ = self._validated_bundle(
                    bundle_fd, company_id, job_id
                )
            finally:
                os.close(bundle_fd)
        return cast(BinaryIO, BytesIO(payloads[name]))
