from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import stat
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier
from typing import Any, cast

import pytest

import seo_orchestrator.services.artifacts as artifacts_module
from seo_orchestrator.canonical import canonical_json
from seo_orchestrator.domain import JobState, SeoJob
from seo_orchestrator.errors import DataIntegrityError
from seo_orchestrator.services.artifacts import ArtifactStore, ExecutionResult

NOW = datetime(2026, 8, 4, 10, 0, tzinfo=UTC)
FIXTURE = Path("fixtures/executions/success-result.json")
_SYNTHETIC_CREDENTIAL_TEXT = "sk" + "-synthetic-credential-marker-12345678"
_PREFIXED_PROVIDER_ENVELOPE_TEXT = (
    'Preamble: {"choices":[{"message":{"reasoning_content":"private marker"}}]}'
)
_PREFIXED_RAW_PROVIDER_ENVELOPE_TEXT = (
    'Preamble: {"choices":[{"message":{"content":"response marker"}}]}'
)
_PREFIXED_LEGACY_PROVIDER_ENVELOPE_TEXT = (
    'Preamble: {"choices":[{"text":"response marker"}]}'
)


def _job() -> SeoJob:
    return SeoJob(
        job_id="job-success-one",
        brief_id="brief-painting",
        brief_fingerprint="b" * 64,
        snapshot_id="snapshot-painting",
        snapshot_hash="a" * 64,
        company_id="avtomalyar",
        direction_id="car-painting",
        audience_segment_id="private-car-owners",
        state=JobState.SUCCEEDED,
        current_stage=None,
        approved_plan_fingerprint="c" * 64,
        approval_record_id="approval-one",
        attempt=1,
        created_at=NOW - timedelta(minutes=5),
        started_at=NOW - timedelta(minutes=4),
        finished_at=NOW - timedelta(minutes=1),
        error_code=None,
        error_summary=None,
        artifact_manifest_path=None,
        company_profile_version=3,
        direction_version=5,
        audience_version=7,
        prompt_set_version=11,
    )


def _result() -> ExecutionResult:
    value = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return ExecutionResult(
        content_markdown=value["content_markdown"],
        titles=tuple(value["titles"]),
        descriptions=tuple(value["descriptions"]),
        keyword_qa=value["keyword_qa"],
        text_metrics=value["text_metrics"],
        sources=tuple(value["sources"]),
        warnings=tuple(value["warnings"]),
        model_usage=value["model_usage"],
        stage_timings=value["stage_timings"],
        prompt_versions=value["prompt_versions"],
    )


def _result_with_text_channel(field_name: str, text: str) -> ExecutionResult:
    result = _result()
    if field_name == "content_markdown":
        return replace(result, content_markdown=text)
    if field_name == "titles":
        return replace(result, titles=(text,) * 5)
    if field_name == "descriptions":
        return replace(result, descriptions=(text,) * 5)
    if field_name == "keyword_qa":
        return replace(
            result,
            keyword_qa={"primary_keyword": text, "occurrences": 3, "passed": True},
        )
    if field_name == "warnings":
        return replace(result, warnings=(text,))
    raise AssertionError(f"unsupported text channel: {field_name}")


def _self_consistent_text_payload(
    bundle: Path, field_name: str, text: str
) -> tuple[str, bytes]:
    if field_name == "content_markdown":
        return "content.md", text.encode("utf-8")
    if field_name == "keyword_qa":
        qa_value = json.loads((bundle / "qa.json").read_bytes())
        qa_value["primary_keyword"] = text
        return "qa.json", canonical_json(qa_value)
    metadata_value = json.loads((bundle / "metadata.json").read_bytes())
    if field_name == "titles":
        metadata_value["titles"] = [text] * 5
    elif field_name == "descriptions":
        metadata_value["descriptions"] = [text] * 5
    elif field_name == "warnings":
        metadata_value["warnings"] = [text]
    else:
        raise AssertionError(f"unsupported text channel: {field_name}")
    return "metadata.json", canonical_json(metadata_value)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def test_write_bundle_persists_exact_canonical_payloads_and_manifest(tmp_path: Path) -> None:
    job = _job()
    result = _result()
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)

    manifest = store.write_bundle(job, result)

    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    assert {entry.name for entry in bundle.iterdir()} == {
        "content.md",
        "metadata.json",
        "qa.json",
        "sources.json",
        "manifest.json",
    }
    expected_metadata = {
        "titles": list(result.titles),
        "descriptions": list(result.descriptions),
        "text_metrics": result.text_metrics,
        "warnings": list(result.warnings),
        "model_usage": result.model_usage,
        "stage_timings": result.stage_timings,
        "prompt_versions": result.prompt_versions,
    }
    expected_payloads = {
        "content.md": result.content_markdown.encode(),
        "metadata.json": canonical_json(expected_metadata),
        "qa.json": canonical_json(result.keyword_qa),
        "sources.json": canonical_json(list(result.sources)),
    }
    expected_hashes = {
        name: _sha256(payload) for name, payload in expected_payloads.items()
    }
    for name, payload in expected_payloads.items():
        assert (bundle / name).read_bytes() == payload

    expected_manifest_json = {
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
        "status": "SUCCEEDED",
        "job_created_at": job.created_at.isoformat(),
        "started_at": (NOW - timedelta(minutes=4)).isoformat(),
        "finished_at": (NOW - timedelta(minutes=1)).isoformat(),
        "prompt_versions": result.prompt_versions,
        "model_usage": result.model_usage,
        "stage_timings": result.stage_timings,
        "warnings": list(result.warnings),
        "source_provenance": [
            {
                "url": "https://example.com/paint-preparation",
                "content_hash": "a" * 64,
                "fetched_at": "2026-08-04T09:00:00+00:00",
            }
        ],
        "artifact_hashes": expected_hashes,
        "created_at": NOW.isoformat(),
    }
    assert (bundle / "manifest.json").read_bytes() == canonical_json(
        expected_manifest_json
    )
    assert manifest.job_id == job.job_id
    assert manifest.company_id == job.company_id
    assert manifest.snapshot_hash == job.snapshot_hash
    assert manifest.artifact_hashes == expected_hashes
    assert manifest.created_at == NOW

    with store.open_artifact(job.company_id, job.job_id, "content.md") as artifact:
        assert artifact.read() == expected_payloads["content.md"]


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("company_profile_version", None),
        ("direction_version", 0),
        ("audience_version", True),
        ("prompt_set_version", "11"),
    ],
)
def test_write_bundle_requires_positive_authoritative_profile_versions(
    tmp_path: Path,
    field_name: str,
    invalid_value: object,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)

    with pytest.raises(ValueError):
        store.write_bundle(
            replace(_job(), **cast(Any, {field_name: invalid_value})),
            _result(),
        )


class _BeforeRenameFailure(RuntimeError):
    pass


def test_failure_before_rename_leaves_no_partial_or_staging_bundle(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"

    def fail_before_rename() -> None:
        raise _BeforeRenameFailure

    store = ArtifactStore(
        artifact_root,
        clock=lambda: NOW,
        before_rename=fail_before_rename,
    )

    with pytest.raises(_BeforeRenameFailure):
        store.write_bundle(_job(), _result())

    jobs = artifact_root / "companies" / "avtomalyar" / "jobs"
    assert jobs.is_dir()
    assert list(jobs.iterdir()) == []


def test_repeating_same_bundle_is_idempotent_without_rewriting_files(tmp_path: Path) -> None:
    times = iter((NOW, NOW + timedelta(hours=1)))
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: next(times))
    job = _job()
    result = _result()

    first = store.write_bundle(job, result)
    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    first_files = {
        entry.name: (entry.read_bytes(), entry.stat().st_mtime_ns)
        for entry in bundle.iterdir()
    }

    second = store.write_bundle(job, result)

    assert second == first
    assert {
        entry.name: (entry.read_bytes(), entry.stat().st_mtime_ns)
        for entry in bundle.iterdir()
    } == first_files


def test_different_result_for_existing_job_fails_closed_without_mutation(
    tmp_path: Path,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, _result())
    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    before = {entry.name: entry.read_bytes() for entry in bundle.iterdir()}
    changed = replace(_result(), content_markdown="# Different result")

    with pytest.raises(DataIntegrityError):
        store.write_bundle(job, changed)

    assert {entry.name: entry.read_bytes() for entry in bundle.iterdir()} == before


def test_open_rejects_tampered_payload_even_when_another_file_is_requested(
    tmp_path: Path,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, _result())
    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    metadata = bundle / "metadata.json"
    bundle.chmod(0o750)
    metadata.chmod(0o640)
    value = json.loads(metadata.read_bytes())
    value["warnings"] = ["hostile rewrite"]
    metadata.write_bytes(canonical_json(value))

    with pytest.raises(DataIntegrityError):
        store.open_artifact(job.company_id, job.job_id, "content.md")


def test_open_rejects_manifest_with_recomputed_but_inconsistent_hash(
    tmp_path: Path,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, _result())
    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    manifest = bundle / "manifest.json"
    bundle.chmod(0o750)
    manifest.chmod(0o640)
    value = json.loads(manifest.read_bytes())
    value["artifact_hashes"]["content.md"] = "d" * 64
    manifest.write_bytes(canonical_json(value))

    with pytest.raises(DataIntegrityError):
        store.open_artifact(job.company_id, job.job_id, "manifest.json")


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("company_profile_version", True),
        ("direction_version", 0),
        ("audience_version", None),
        ("prompt_set_version", "11"),
    ],
)
def test_open_rejects_invalid_manifest_profile_versions(
    tmp_path: Path,
    field_name: str,
    invalid_value: object,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, _result())
    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    manifest = bundle / "manifest.json"
    bundle.chmod(0o750)
    manifest.chmod(0o640)
    value = json.loads(manifest.read_bytes())
    value[field_name] = invalid_value
    manifest.write_bytes(canonical_json(value))
    manifest.chmod(0o440)
    bundle.chmod(0o550)

    with pytest.raises(DataIntegrityError):
        store.open_artifact(job.company_id, job.job_id, "content.md")


def test_open_rejects_self_consistent_provider_envelope_in_qa_payload(
    tmp_path: Path,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, _result())
    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    qa = bundle / "qa.json"
    manifest = bundle / "manifest.json"
    bundle.chmod(0o750)
    qa.chmod(0o640)
    manifest.chmod(0o640)
    qa_value = json.loads(qa.read_bytes())
    qa_value["primary_keyword"] = (
        '{"choices":[{"message":{"reasoning_content":"private marker"}}]}'
    )
    qa_payload = canonical_json(qa_value)
    qa.write_bytes(qa_payload)
    manifest_value = json.loads(manifest.read_bytes())
    manifest_value["artifact_hashes"]["qa.json"] = _sha256(qa_payload)
    manifest.write_bytes(canonical_json(manifest_value))
    qa.chmod(0o440)
    manifest.chmod(0o440)
    bundle.chmod(0o550)

    with pytest.raises(DataIntegrityError):
        store.open_artifact(job.company_id, job.job_id, "content.md")


@pytest.mark.parametrize(
    ("field_name", "text"),
    [
        ("content_markdown", _SYNTHETIC_CREDENTIAL_TEXT),
        ("titles", _SYNTHETIC_CREDENTIAL_TEXT),
        ("descriptions", _SYNTHETIC_CREDENTIAL_TEXT),
        ("keyword_qa", _SYNTHETIC_CREDENTIAL_TEXT),
        ("warnings", _SYNTHETIC_CREDENTIAL_TEXT),
        ("content_markdown", _PREFIXED_PROVIDER_ENVELOPE_TEXT),
        ("titles", _PREFIXED_PROVIDER_ENVELOPE_TEXT),
        ("descriptions", _PREFIXED_PROVIDER_ENVELOPE_TEXT),
        ("keyword_qa", _PREFIXED_PROVIDER_ENVELOPE_TEXT),
        ("warnings", _PREFIXED_PROVIDER_ENVELOPE_TEXT),
    ],
)
def test_open_rejects_self_consistent_forbidden_text_in_all_channels(
    tmp_path: Path,
    field_name: str,
    text: str,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, _result())
    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    manifest = bundle / "manifest.json"
    artifact_name, payload = _self_consistent_text_payload(bundle, field_name, text)
    artifact = bundle / artifact_name
    bundle.chmod(0o750)
    artifact.chmod(0o640)
    manifest.chmod(0o640)
    try:
        artifact.write_bytes(payload)
        manifest_value = json.loads(manifest.read_bytes())
        manifest_value["artifact_hashes"][artifact_name] = _sha256(payload)
        if field_name == "warnings":
            manifest_value["warnings"] = [text]
        manifest.write_bytes(canonical_json(manifest_value))
    finally:
        artifact.chmod(0o440)
        manifest.chmod(0o440)
        bundle.chmod(0o550)

    with pytest.raises(DataIntegrityError):
        store.open_artifact(job.company_id, job.job_id, "content.md")


@pytest.mark.parametrize(
    "text",
    [
        _PREFIXED_RAW_PROVIDER_ENVELOPE_TEXT,
        _PREFIXED_LEGACY_PROVIDER_ENVELOPE_TEXT,
    ],
)
def test_open_rejects_self_consistent_prefixed_raw_provider_envelope_without_hidden_reasoning(
    tmp_path: Path,
    text: str,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, _result())
    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    manifest = bundle / "manifest.json"
    artifact = bundle / "content.md"
    payload = text.encode("utf-8")
    bundle.chmod(0o750)
    artifact.chmod(0o640)
    manifest.chmod(0o640)
    try:
        artifact.write_bytes(payload)
        manifest_value = json.loads(manifest.read_bytes())
        manifest_value["artifact_hashes"]["content.md"] = _sha256(payload)
        manifest.write_bytes(canonical_json(manifest_value))
    finally:
        artifact.chmod(0o440)
        manifest.chmod(0o440)
        bundle.chmod(0o550)

    with pytest.raises(DataIntegrityError):
        store.open_artifact(job.company_id, job.job_id, "content.md")


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("content_markdown", b"not text"),
        ("titles", ("one", "two", "three", "four")),
        ("descriptions", ["one", "two", "three", "four", "five"]),
        ("sources", []),
        ("warnings", ["warning"]),
    ],
)
def test_execution_result_rejects_invalid_runtime_shapes(
    field_name: str,
    invalid_value: object,
) -> None:
    with pytest.raises(ValueError, match=field_name):
        replace(_result(), **cast(Any, {field_name: invalid_value}))


def test_naive_artifact_clock_fails_before_publishing_bundle(tmp_path: Path) -> None:
    store = ArtifactStore(
        tmp_path / "artifacts",
        clock=lambda: NOW.replace(tzinfo=None),
    )

    with pytest.raises(ValueError, match="timezone-aware"):
        store.write_bundle(_job(), _result())

    assert not (
        tmp_path
        / "artifacts"
        / "companies"
        / "avtomalyar"
        / "jobs"
        / "job-success-one"
    ).exists()


class _CancellationLike(BaseException):
    pass


def test_cancellation_like_failure_before_rename_cleans_staging(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"

    def cancel_before_rename() -> None:
        raise _CancellationLike

    store = ArtifactStore(
        artifact_root,
        clock=lambda: NOW,
        before_rename=cancel_before_rename,
    )

    with pytest.raises(_CancellationLike):
        store.write_bundle(_job(), _result())

    jobs = artifact_root / "companies" / "avtomalyar" / "jobs"
    assert list(jobs.iterdir()) == []


def test_post_rename_cancellation_preserves_published_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_rename = artifacts_module._rename_noreplace

    def rename_then_cancel(parent_fd: int, source: str, target: str) -> None:
        real_rename(parent_fd, source, target)
        raise _CancellationLike

    monkeypatch.setattr(artifacts_module, "_rename_noreplace", rename_then_cancel)
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()

    with pytest.raises(_CancellationLike):
        store.write_bundle(job, _result())

    with store.open_artifact(job.company_id, job.job_id, "content.md") as artifact:
        assert artifact.read() == _result().content_markdown.encode("utf-8")


def test_concurrent_identical_publishers_converge_on_one_bundle(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    barrier = Barrier(2)

    def wait_before_rename() -> None:
        barrier.wait(timeout=5)

    stores = [
        ArtifactStore(
            artifact_root,
            clock=lambda: NOW,
            before_rename=wait_before_rename,
        )
        for _ in range(2)
    ]

    with ThreadPoolExecutor(max_workers=2) as pool:
        manifests = list(pool.map(lambda store: store.write_bundle(_job(), _result()), stores))

    assert manifests[0] == manifests[1]
    bundle = artifact_root / "companies" / "avtomalyar" / "jobs" / "job-success-one"
    assert {entry.name for entry in bundle.iterdir()} == {
        "content.md",
        "metadata.json",
        "qa.json",
        "sources.json",
        "manifest.json",
    }
    assert stat.S_IMODE(bundle.stat().st_mode) == 0o550
    assert all(stat.S_IMODE(entry.stat().st_mode) == 0o440 for entry in bundle.iterdir())


def test_concurrent_conflicting_publishers_allow_exactly_one_result(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    barrier = Barrier(2)

    def wait_before_rename() -> None:
        barrier.wait(timeout=5)

    stores = [
        ArtifactStore(
            artifact_root,
            clock=lambda: NOW,
            before_rename=wait_before_rename,
        )
        for _ in range(2)
    ]
    results = (
        _result(),
        replace(_result(), content_markdown="# Conflicting result"),
    )

    def publish(index: int) -> object:
        try:
            return stores[index].write_bundle(_job(), results[index])
        except DataIntegrityError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(publish, range(2)))

    assert sum(isinstance(value, DataIntegrityError) for value in outcomes) == 1
    bundle = artifact_root / "companies" / "avtomalyar" / "jobs" / "job-success-one"
    assert (bundle / "content.md").read_text(encoding="utf-8") in {
        results[0].content_markdown,
        results[1].content_markdown,
    }
    assert {entry.name for entry in bundle.parent.iterdir()} == {"job-success-one"}


def test_staging_directory_name_contains_bundle_content_digest(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    observed_names: list[str] = []

    def inspect_staging_name() -> None:
        jobs = artifact_root / "companies" / "avtomalyar" / "jobs"
        observed_names.extend(entry.name for entry in jobs.iterdir())

    ArtifactStore(
        artifact_root,
        clock=lambda: NOW,
        before_rename=inspect_staging_name,
    ).write_bundle(_job(), _result())

    assert len(observed_names) == 1
    assert re.fullmatch(
        r"\.job-success-one\.[0-9a-f]{64}\.[0-9a-f]{32}\.tmp",
        observed_names[0],
    )


def test_publish_never_replaces_preexisting_empty_final_directory(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    final = (
        artifact_root
        / "companies"
        / "avtomalyar"
        / "jobs"
        / "job-success-one"
    )

    def inject_empty_final() -> None:
        final.mkdir()

    store = ArtifactStore(
        artifact_root,
        clock=lambda: NOW,
        before_rename=inject_empty_final,
    )

    with pytest.raises(DataIntegrityError):
        store.write_bundle(_job(), _result())

    assert final.is_dir()
    assert list(final.iterdir()) == []
    assert {entry.name for entry in final.parent.iterdir()} == {"job-success-one"}


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("brief_fingerprint", "not-a-hash"),
        ("snapshot_hash", "A" * 64),
        ("attempt", True),
        ("attempt", 0),
        ("attempt", -1),
        ("created_at", NOW.replace(tzinfo=None)),
        ("started_at", NOW - timedelta(minutes=6)),
        ("finished_at", NOW - timedelta(minutes=5)),
        ("state", "SUCCEEDED"),
    ],
)
def test_write_rejects_invalid_job_provenance_before_publishing(
    tmp_path: Path,
    field_name: str,
    invalid_value: object,
) -> None:
    job = replace(_job(), **cast(Any, {field_name: invalid_value}))
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)

    with pytest.raises(ValueError, match=field_name):
        store.write_bundle(job, _result())

    assert not (
        tmp_path
        / "artifacts"
        / "companies"
        / "avtomalyar"
        / "jobs"
        / "job-success-one"
    ).exists()


@pytest.mark.parametrize("fetched_at", [123, "2026-08-04T09:00:00"])
def test_execution_result_rejects_malformed_source_fetch_timestamp(
    fetched_at: object,
) -> None:
    source = {
        "url": "https://example.com/source",
        "content_hash": "d" * 64,
        "fetched_at": fetched_at,
    }

    with pytest.raises(ValueError, match="fetched_at"):
        replace(_result(), sources=(cast(Any, source),))


@pytest.mark.parametrize("state", [state for state in JobState if state is not JobState.SUCCEEDED])
def test_only_succeeded_job_can_create_artifact_bundle(
    tmp_path: Path,
    state: JobState,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)

    with pytest.raises(ValueError, match="state"):
        store.write_bundle(replace(_job(), state=state), _result())

    assert not (tmp_path / "artifacts").exists()


def test_manifest_normalizes_all_timestamps_to_utc(tmp_path: Path) -> None:
    plus_three = timezone(timedelta(hours=3))
    job = replace(
        _job(),
        created_at=_job().created_at.astimezone(plus_three),
        started_at=cast(datetime, _job().started_at).astimezone(plus_three),
        finished_at=cast(datetime, _job().finished_at).astimezone(plus_three),
    )
    result = replace(
        _result(),
        sources=(
            {
                "url": "https://example.com/source",
                "content_hash": "d" * 64,
                "fetched_at": "2026-08-04T12:00:00+03:00",
            },
        ),
    )
    store = ArtifactStore(
        tmp_path / "artifacts",
        clock=lambda: NOW.astimezone(plus_three),
    )

    manifest = store.write_bundle(job, result)

    assert manifest.created_at == NOW
    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    value = json.loads((bundle / "manifest.json").read_bytes())
    assert value["created_at"] == "2026-08-04T10:00:00+00:00"
    assert value["job_created_at"] == "2026-08-04T09:55:00+00:00"
    assert value["started_at"] == "2026-08-04T09:56:00+00:00"
    assert value["finished_at"] == "2026-08-04T09:59:00+00:00"
    assert value["source_provenance"][0]["fetched_at"] == (
        "2026-08-04T09:00:00+00:00"
    )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("keyword_qa", {"nested": {"authorization": "Bearer secret"}}),
        ("text_metrics", {"api_key": "secret"}),
        ("model_usage", {"hidden_reasoning": "private chain"}),
        ("stage_timings", {"access_token": "secret"}),
        ("prompt_versions", {"credentials": {"password": "secret"}}),
    ],
)
def test_execution_result_rejects_nested_secrets_and_hidden_reasoning(
    field_name: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="forbidden artifact field"):
        replace(_result(), **cast(Any, {field_name: value}))


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("text_metrics", {"api_token": "credential-marker-123456"}),
        ("stage_timings", {"client_secret": "credential-marker-123456"}),
        ("content_markdown", "secret=credential-marker-123456"),
        ("warnings", ("cookie=credential-marker-123456",)),
    ],
)
def test_execution_result_rejects_unlisted_credential_key_and_text_channels(
    field_name: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="credential|forbidden"):
        replace(_result(), **cast(Any, {field_name: value}))


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("text_metrics", {"secret_note": "credential-marker-123456"}),
        ("text_metrics", {"cookie_value": "credential-marker-123456"}),
        ("text_metrics", {"api_token_value": "credential-marker-123456"}),
        ("keyword_qa", {"thinking_notes": "private reasoning"}),
        (
            "model_usage",
            {
                "models": [
                    {
                        "model_id": "writer-model-v1",
                        "provider_id": "mock-provider",
                    }
                ],
                "model_reasoning": "private reasoning",
            },
        ),
    ],
)
def test_execution_result_rejects_sensitive_embedded_field_names(
    field_name: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="forbidden artifact field"):
        replace(_result(), **cast(Any, {field_name: value}))


def test_execution_result_allows_non_secret_token_usage_metric() -> None:
    result = replace(_result(), text_metrics={"token_count": 42})

    assert result.text_metrics == {"token_count": 42}


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        (
            "keyword_qa",
            {"assessment": {"choices": [{"message": "provider-envelope-marker"}]}},
        ),
        ("keyword_qa", {"assessment": "provider-envelope-marker"}),
        (
            "text_metrics",
            {"measurements": {"choices": [{"message": "provider-envelope-marker"}]}},
        ),
        (
            "stage_timings",
            {"writer": {"choices": [{"message": "provider-envelope-marker"}]}},
        ),
        (
            "model_usage",
            {
                "models": [
                    {
                        "model_id": "writer-model-v1",
                        "provider_id": "mock-provider",
                    }
                ],
                "telemetry": {"choices": [{"message": "provider-envelope-marker"}]},
            },
        ),
        (
            "model_usage",
            {
                "models": [
                    {
                        "model_id": "writer-model-v1",
                        "provider_id": "mock-provider",
                        "telemetry": "provider-envelope-marker",
                    }
                ]
            },
        ),
        ("prompt_versions", {"writer": "provider envelope marker"}),
        (
            "sources",
            (
                {
                    "url": "https://example.com/source",
                    "content_hash": "d" * 64,
                    "fetched_at": "2026-08-04T09:00:00+00:00",
                    "evidence": {
                        "choices": [{"message": "provider-envelope-marker"}]
                    },
                },
            ),
        ),
    ],
)
def test_execution_result_rejects_unstructured_provider_envelopes(
    field_name: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        replace(_result(), **cast(Any, {field_name: value}))


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        (
            "content_markdown",
            '{"choices":[{"message":{"reasoning_content":"private marker"}}]}',
        ),
        (
            "content_markdown",
            '"{\\"choices\\":[{\\"message\\":{\\"reasoning_content\\":\\"private marker\\"}}]}"',
        ),
        (
            "keyword_qa",
            {
                "primary_keyword": '{"choices":[{"message":{"reasoning_content":"private marker"}}]}',
                "occurrences": 3,
                "passed": True,
            },
        ),
        ("warnings", ("<reasoning>private marker</reasoning>",)),
        ("warnings", ("reasoning_content: private marker",)),
    ],
)
def test_execution_result_rejects_provider_envelopes_in_text_fields(
    field_name: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        replace(_result(), **cast(Any, {field_name: value}))


@pytest.mark.parametrize(
    ("field_name", "text"),
    [
        ("content_markdown", _SYNTHETIC_CREDENTIAL_TEXT),
        ("titles", _SYNTHETIC_CREDENTIAL_TEXT),
        ("descriptions", _SYNTHETIC_CREDENTIAL_TEXT),
        ("keyword_qa", _SYNTHETIC_CREDENTIAL_TEXT),
        ("warnings", _SYNTHETIC_CREDENTIAL_TEXT),
        ("content_markdown", _PREFIXED_PROVIDER_ENVELOPE_TEXT),
        ("titles", _PREFIXED_PROVIDER_ENVELOPE_TEXT),
        ("descriptions", _PREFIXED_PROVIDER_ENVELOPE_TEXT),
        ("keyword_qa", _PREFIXED_PROVIDER_ENVELOPE_TEXT),
        ("warnings", _PREFIXED_PROVIDER_ENVELOPE_TEXT),
    ],
)
def test_execution_result_rejects_credentials_and_prefixed_provider_envelopes_in_all_text_channels(
    field_name: str,
    text: str,
) -> None:
    with pytest.raises(ValueError):
        _result_with_text_channel(field_name, text)


@pytest.mark.parametrize(
    "text",
    [
        _PREFIXED_RAW_PROVIDER_ENVELOPE_TEXT,
        _PREFIXED_LEGACY_PROVIDER_ENVELOPE_TEXT,
    ],
)
def test_execution_result_rejects_prefixed_raw_provider_envelope_without_hidden_reasoning(
    text: str,
) -> None:
    with pytest.raises(ValueError):
        _result_with_text_channel("content_markdown", text)


@pytest.mark.parametrize(
    "model_id",
    [_SYNTHETIC_CREDENTIAL_TEXT, "api-key-synthetic-marker-123456"],
)
def test_execution_result_rejects_credential_shaped_model_label(model_id: str) -> None:
    with pytest.raises(ValueError):
        replace(
            _result(),
            model_usage={
                "models": [
                    {
                        "model_id": model_id,
                        "provider_id": "mock-provider",
                    }
                ]
            },
        )


def test_execution_result_rejects_signed_source_url() -> None:
    source = {
        "url": "https://example.com/source?sig=synthetic-marker-123456",
        "content_hash": "e" * 64,
        "fetched_at": "2026-08-04T09:00:00+00:00",
    }

    with pytest.raises(ValueError, match="credential"):
        replace(_result(), sources=(source,))


def test_write_bundle_rejects_staging_name_swap_after_validation(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"

    def swap_staging_name() -> None:
        jobs = artifact_root / "companies" / "avtomalyar" / "jobs"
        staging = next(entry for entry in jobs.iterdir() if entry.name.startswith("."))
        displaced = jobs / f"{staging.name}.displaced"
        staging.rename(displaced)
        (jobs / staging.name).mkdir(mode=0o700)

    store = ArtifactStore(
        artifact_root,
        clock=lambda: NOW,
        after_validation=swap_staging_name,
    )

    with pytest.raises(DataIntegrityError):
        store.write_bundle(_job(), _result())

    jobs = artifact_root / "companies" / "avtomalyar" / "jobs"
    assert not (jobs / "job-success-one").exists()


def test_staged_bytes_are_revalidated_before_publish(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"

    def corrupt_staged_qa() -> None:
        jobs = artifact_root / "companies" / "avtomalyar" / "jobs"
        staging = next(entry for entry in jobs.iterdir() if entry.name.startswith("."))
        qa = staging / "qa.json"
        qa.chmod(0o600)
        qa.write_bytes(b"{")
        qa.chmod(0o440)

    store = ArtifactStore(
        artifact_root,
        clock=lambda: NOW,
        before_rename=corrupt_staged_qa,
    )

    with pytest.raises(DataIntegrityError):
        store.write_bundle(_job(), _result())

    jobs = artifact_root / "companies" / "avtomalyar" / "jobs"
    assert list(jobs.iterdir()) == []


def test_open_rejects_boolean_manifest_schema_version(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, _result())
    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    manifest = bundle / "manifest.json"
    bundle.chmod(0o750)
    manifest.chmod(0o640)
    value = json.loads(manifest.read_bytes())
    value["schema_version"] = True
    manifest.write_bytes(canonical_json(value))
    manifest.chmod(0o440)
    bundle.chmod(0o550)

    with pytest.raises(DataIntegrityError):
        store.open_artifact(job.company_id, job.job_id, "content.md")


def test_manifest_comparison_does_not_coerce_boolean_to_integer(tmp_path: Path) -> None:
    result = replace(
        _result(),
        model_usage={
            "models": [
                {
                    "model_id": "writer-model-v1",
                    "provider_id": "mock-provider",
                    "input_tokens": 1,
                    "output_tokens": 80,
                }
            ]
        },
    )
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, result)
    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    manifest = bundle / "manifest.json"
    bundle.chmod(0o750)
    manifest.chmod(0o640)
    value = json.loads(manifest.read_bytes())
    value["model_usage"]["models"][0]["input_tokens"] = True
    manifest.write_bytes(canonical_json(value))
    manifest.chmod(0o440)
    bundle.chmod(0o550)

    with pytest.raises(DataIntegrityError):
        store.open_artifact(job.company_id, job.job_id, "content.md")


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("model_usage", {}),
        ("model_usage", {"models": []}),
        ("model_usage", {"models": [{"model_id": "writer-model-v1"}]}),
        ("prompt_versions", {}),
    ],
)
def test_execution_result_requires_model_provider_and_prompt_provenance(
    field_name: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match=field_name):
        replace(_result(), **cast(Any, {field_name: value}))


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("text_metrics", {"accessToken": "token-marker-123456"}),
        ("keyword_qa", {"chainOfThought": "private reasoning"}),
        ("content_markdown", "password=credential-marker-123456"),
    ],
)
def test_execution_result_rejects_camel_case_and_text_secret_channels(
    field_name: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="credential|reasoning|forbidden"):
        replace(_result(), **cast(Any, {field_name: value}))


@pytest.mark.parametrize(
    "url",
    [
        "https://user:password@example.com/source",
        "https://example.com/source?accessToken=token-marker-123456",
    ],
)
def test_execution_result_rejects_credentials_in_source_urls(url: str) -> None:
    source = {
        "url": url,
        "content_hash": "d" * 64,
        "fetched_at": "2026-08-04T09:00:00+00:00",
    }

    with pytest.raises(ValueError, match="URL|credential|forbidden"):
        replace(_result(), sources=(source,))


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/private",
        "http://[::1]/private",
        "http://[::ffff:127.0.0.1]/private",
        "http://169.254.169.254/metadata",
        "http://localhost/private",
        "http://internal.localhost/private",
    ],
)
def test_execution_result_rejects_literal_unsafe_source_urls(url: str) -> None:
    source = {
        "url": url,
        "content_hash": "d" * 64,
        "fetched_at": "2026-08-04T09:00:00+00:00",
    }

    with pytest.raises(ValueError, match="source URL"):
        replace(_result(), sources=(source,))


@pytest.mark.parametrize(
    "url",
    [
        "http://2130706433/private",
        "http://0x7f000001/private",
        "http://0x7f.0x0.0x0.0x1/private",
    ],
)
def test_execution_result_rejects_alternate_numeric_loopback_source_urls(url: str) -> None:
    source = {
        "url": url,
        "content_hash": "d" * 64,
        "fetched_at": "2026-08-04T09:00:00+00:00",
    }

    with pytest.raises(ValueError, match="source URL"):
        replace(_result(), sources=(source,))


def test_execution_result_rejects_oversized_content() -> None:
    with pytest.raises(ValueError, match="content_markdown"):
        replace(_result(), content_markdown="x" * 1_048_577)


def test_artifact_clock_cannot_precede_job_completion(tmp_path: Path) -> None:
    store = ArtifactStore(
        tmp_path / "artifacts",
        clock=lambda: NOW - timedelta(minutes=2),
    )

    with pytest.raises(ValueError, match="artifact clock"):
        store.write_bundle(_job(), _result())


def test_open_maps_excessive_json_nesting_to_integrity_error(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, _result())
    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    qa = bundle / "qa.json"
    bundle.chmod(0o750)
    qa.chmod(0o640)
    qa.write_bytes(b"[" * 10_000 + b"0" + b"]" * 10_000)
    qa.chmod(0o440)
    bundle.chmod(0o550)

    with pytest.raises(DataIntegrityError):
        store.open_artifact(job.company_id, job.job_id, "content.md")


def test_write_uses_consistent_snapshot_if_original_result_mutates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _result()
    original_payloads = ArtifactStore._payloads

    def mutate_after_payloads(
        candidate: ExecutionResult,
    ) -> tuple[dict[str, bytes], dict[str, str]]:
        payloads = original_payloads(candidate)
        model_usage = cast(dict[str, Any], result.model_usage)
        models = cast(list[dict[str, Any]], model_usage["models"])
        models[0]["model_id"] = "mutated-after-payloads"
        return payloads

    monkeypatch.setattr(ArtifactStore, "_payloads", staticmethod(mutate_after_payloads))
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()

    store.write_bundle(job, result)

    with store.open_artifact(job.company_id, job.job_id, "content.md") as artifact:
        assert artifact.read() == _result().content_markdown.encode()


def test_staging_open_failure_removes_created_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_open = artifacts_module._open_directory_at

    def fail_staging_open(parent_fd: int, name: str, *, create: bool) -> int:
        if name.endswith(".tmp"):
            raise DataIntegrityError
        return original_open(parent_fd, name, create=create)

    monkeypatch.setattr(artifacts_module, "_open_directory_at", fail_staging_open)
    artifact_root = tmp_path / "artifacts"

    with pytest.raises(DataIntegrityError):
        ArtifactStore(artifact_root, clock=lambda: NOW).write_bundle(_job(), _result())

    jobs = artifact_root / "companies" / "avtomalyar" / "jobs"
    assert list(jobs.iterdir()) == []


@pytest.mark.parametrize("failure_call", (1, 2, 3, 4))
def test_jobs_directory_closes_each_fd_when_directory_validation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_call: int,
) -> None:
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir(mode=0o700)
    artifact_root.chmod(0o700)
    real_validate = artifacts_module._validate_directory
    observed_descriptors: list[int] = []
    calls = 0

    def fail_on_requested_validation(descriptor: int, mode: int) -> os.stat_result:
        nonlocal calls
        calls += 1
        if calls == failure_call:
            observed_descriptors.append(descriptor)
            raise DataIntegrityError
        return real_validate(descriptor, mode)

    monkeypatch.setattr(
        artifacts_module,
        "_validate_directory",
        fail_on_requested_validation,
    )
    try:
        with pytest.raises(DataIntegrityError):
            ArtifactStore(artifact_root, clock=lambda: NOW).write_bundle(_job(), _result())

        assert len(observed_descriptors) == 1
        with pytest.raises(OSError):
            os.fstat(observed_descriptors[0])
    finally:
        for descriptor in observed_descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass


def test_cleanup_failure_preserves_primary_error_and_closes_staging_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_fds: list[int] = []

    def fail_cleanup(jobs_fd: int, staging_fd: int, staging_name: str) -> None:
        del jobs_fd, staging_name
        observed_fds.append(staging_fd)
        raise DataIntegrityError

    monkeypatch.setattr(ArtifactStore, "_cleanup_staging", staticmethod(fail_cleanup))
    artifact_root = tmp_path / "artifacts"

    def cancel() -> None:
        raise _CancellationLike

    store = ArtifactStore(
        artifact_root,
        clock=lambda: NOW,
        before_rename=cancel,
    )

    with pytest.raises(_CancellationLike):
        store.write_bundle(_job(), _result())

    assert len(observed_fds) == 1
    with pytest.raises(OSError):
        os.fstat(observed_fds[0])
    jobs = artifact_root / "companies" / "avtomalyar" / "jobs"
    for staging in jobs.iterdir():
        staging.chmod(0o700)
        for artifact in staging.iterdir():
            artifact.unlink()
        staging.rmdir()


def test_fchmod_is_durable_before_descriptor_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[str, int, int | None]] = []
    real_fchmod = os.fchmod
    real_fsync = os.fsync
    real_close = os.close

    def record_fchmod(descriptor: int, mode: int) -> None:
        events.append(("chmod", descriptor, mode))
        real_fchmod(descriptor, mode)

    def record_fsync(descriptor: int) -> None:
        events.append(("fsync", descriptor, None))
        real_fsync(descriptor)

    def record_close(descriptor: int) -> None:
        events.append(("close", descriptor, None))
        real_close(descriptor)

    monkeypatch.setattr(os, "fchmod", record_fchmod)
    monkeypatch.setattr(os, "fsync", record_fsync)
    monkeypatch.setattr(os, "close", record_close)

    ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW).write_bundle(_job(), _result())

    for index, (operation, descriptor, mode) in enumerate(events):
        if operation != "chmod" or mode not in {0o440, 0o550}:
            continue
        remaining = events[index + 1 :]
        close_index = next(
            offset
            for offset, event in enumerate(remaining)
            if event[0] == "close" and event[1] == descriptor
        )
        assert any(
            event[0] == "fsync" and event[1] == descriptor
            for event in remaining[:close_index]
        )


def test_new_directory_entry_fsyncs_child_and_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_fd = os.open(tmp_path, artifacts_module._DIRECTORY_FLAGS)
    fsynced: list[int] = []
    real_fsync = os.fsync

    def record_fsync(descriptor: int) -> None:
        fsynced.append(descriptor)
        real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", record_fsync)
    child_fd: int | None = None
    try:
        child_fd = artifacts_module._open_directory_at(parent_fd, "new-directory", create=True)
        assert child_fd in fsynced
        assert parent_fd in fsynced
    finally:
        if child_fd is not None:
            os.close(child_fd)
        os.close(parent_fd)


def test_artifact_manifest_hashes_are_immutable(tmp_path: Path) -> None:
    manifest = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW).write_bundle(
        _job(), _result()
    )

    with pytest.raises(TypeError):
        manifest.artifact_hashes["content.md"] = "0" * 64


def test_missing_renameat2_fails_closed_without_publishing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(artifacts_module, "_RENAMEAT2", None)
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()

    with pytest.raises(DataIntegrityError):
        store.write_bundle(job, _result())

    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    assert not bundle.exists()


def test_post_rename_fsync_error_is_controlled_and_bundle_remains_readable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    renamed = False
    real_rename = artifacts_module._rename_noreplace
    real_fsync = os.fsync

    def record_rename(parent_fd: int, source: str, target: str) -> None:
        nonlocal renamed
        real_rename(parent_fd, source, target)
        renamed = True

    def fail_after_rename(descriptor: int) -> None:
        if renamed:
            raise OSError(errno.EIO, "injected fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(artifacts_module, "_rename_noreplace", record_rename)
    monkeypatch.setattr(os, "fsync", fail_after_rename)
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()

    with pytest.raises(DataIntegrityError):
        store.write_bundle(job, _result())

    with store.open_artifact(job.company_id, job.job_id, "content.md") as artifact:
        assert artifact.read() == _result().content_markdown.encode("utf-8")


@pytest.mark.parametrize(
    "text",
    [
        'Preamble: {"error":{"message":"provider failure","type":"api_error"}}',
        'Preamble: {"choices":[]}',
        'Preamble: {"analysis":"private marker"}',
    ],
)
@pytest.mark.parametrize(
    "field_name",
    ["content_markdown", "titles", "descriptions", "keyword_qa", "warnings"],
)
def test_execution_result_rejects_prefixed_embedded_structured_output_in_all_text_channels(
    field_name: str,
    text: str,
) -> None:
    with pytest.raises(ValueError, match="structured|provider"):
        _result_with_text_channel(field_name, text)


@pytest.mark.parametrize("prefix", ["", "Preamble: "])
def test_execution_result_rejects_triple_json_wrapped_provider_envelope(prefix: str) -> None:
    raw_provider_envelope = (
        '{"choices":[{"message":{"reasoning_content":"private marker"}}]}'
    )
    text = prefix + json.dumps(json.dumps(json.dumps(raw_provider_envelope)))

    with pytest.raises(ValueError, match="structured|provider"):
        _result_with_text_channel("content_markdown", text)


@pytest.mark.parametrize(
    "text",
    [
        'Preamble: {"error":{"message":"provider failure","type":"api_error"}}',
        "Preamble: "
        + json.dumps(
            json.dumps(
                json.dumps(
                    '{"choices":[{"message":{"reasoning_content":"private marker"}}]}'
                )
            )
        ),
    ],
)
def test_open_rejects_self_consistent_prefixed_embedded_structured_output(
    tmp_path: Path,
    text: str,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, _result())
    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    manifest = bundle / "manifest.json"
    artifact_name, payload = _self_consistent_text_payload(bundle, "content_markdown", text)
    artifact = bundle / artifact_name
    bundle.chmod(0o750)
    artifact.chmod(0o640)
    manifest.chmod(0o640)
    try:
        artifact.write_bytes(payload)
        manifest_value = json.loads(manifest.read_bytes())
        manifest_value["artifact_hashes"][artifact_name] = _sha256(payload)
        manifest.write_bytes(canonical_json(manifest_value))
    finally:
        artifact.chmod(0o440)
        manifest.chmod(0o440)
        bundle.chmod(0o550)

    with pytest.raises(DataIntegrityError):
        store.open_artifact(job.company_id, job.job_id, "content.md")


def test_post_validation_payload_mutation_never_publishes_bundle(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"

    def corrupt_staged_qa() -> None:
        jobs = artifact_root / "companies" / "avtomalyar" / "jobs"
        staging = next(entry for entry in jobs.iterdir() if entry.name.startswith("."))
        qa = staging / "qa.json"
        qa.chmod(0o640)
        qa.write_bytes(b"{}")
        qa.chmod(0o440)

    store = ArtifactStore(
        artifact_root,
        clock=lambda: NOW,
        after_validation=corrupt_staged_qa,
    )

    with pytest.raises(DataIntegrityError):
        store.write_bundle(_job(), _result())

    jobs = artifact_root / "companies" / "avtomalyar" / "jobs"
    assert not (jobs / "job-success-one").exists()
