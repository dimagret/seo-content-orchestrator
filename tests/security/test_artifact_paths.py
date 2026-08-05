from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from seo_orchestrator.domain import JobState, SeoJob
from seo_orchestrator.errors import DataIntegrityError, NotFound
from seo_orchestrator.services.artifacts import ArtifactStore, ExecutionResult

NOW = datetime(2026, 8, 4, 10, 0, tzinfo=UTC)
FIXTURE = Path("fixtures/executions/success-result.json")


def _job(
    *,
    company_id: str = "avtomalyar",
    job_id: str = "job-success-one",
) -> SeoJob:
    return SeoJob(
        job_id=job_id,
        brief_id="brief-painting",
        brief_fingerprint="b" * 64,
        snapshot_id="snapshot-painting",
        snapshot_hash="a" * 64,
        company_id=company_id,
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


@pytest.mark.parametrize(
    "value",
    [
        "/absolute",
        ".",
        "..",
        "../escape",
        "nested/value",
        r"nested\value",
        "nested∕value",
        "nested／value",
        "сompany-one",
        "company_one",
        "a",
        "a" * 65,
    ],
)
def test_open_artifact_rejects_noncanonical_company_and_job_ids(
    tmp_path: Path,
    value: str,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts")

    with pytest.raises(ValueError, match="identifier"):
        store.open_artifact(value, "job-success-one", "content.md")
    with pytest.raises(ValueError, match="identifier"):
        store.open_artifact("avtomalyar", value, "content.md")


@pytest.mark.parametrize(
    "name",
    [
        "/etc/passwd",
        "../content.md",
        "nested/content.md",
        r"..\content.md",
        "CONTENT.md",
        "сontent.md",
        "content．md",
        "metadata.json/extra",
    ],
)
def test_open_artifact_rejects_names_outside_exact_allowlist(
    tmp_path: Path,
    name: str,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts")

    with pytest.raises(ValueError, match="name"):
        store.open_artifact("avtomalyar", "job-success-one", name)


def test_write_bundle_rejects_noncanonical_job_scope_before_filesystem_write(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    store = ArtifactStore(artifact_root, clock=lambda: NOW)

    with pytest.raises(ValueError, match="identifier"):
        store.write_bundle(_job(company_id="../outside"), _result())
    with pytest.raises(ValueError, match="identifier"):
        store.write_bundle(_job(job_id=r"..\outside"), _result())

    assert not artifact_root.exists()


def test_cross_company_job_lookup_is_non_oracular_not_found(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, _result())

    with pytest.raises(NotFound, match="record not found"):
        store.open_artifact("sweet-world", job.job_id, "content.md")


def test_artifact_root_symlink_is_rejected_without_writing_target(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    artifact_root = tmp_path / "artifacts"
    artifact_root.symlink_to(outside, target_is_directory=True)

    with pytest.raises(DataIntegrityError):
        ArtifactStore(artifact_root, clock=lambda: NOW).write_bundle(_job(), _result())

    assert list(outside.iterdir()) == []


def test_intermediate_company_symlink_cannot_escape_artifact_root(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    companies = artifact_root / "companies"
    companies.mkdir(parents=True)
    artifact_root.chmod(0o700)
    companies.chmod(0o700)
    outside = tmp_path / "outside"
    secret = outside / "jobs" / "job-success-one"
    secret.mkdir(parents=True)
    (secret / "content.md").write_bytes(b"outside-secret")
    (companies / "avtomalyar").symlink_to(outside, target_is_directory=True)
    store = ArtifactStore(artifact_root)

    with pytest.raises(DataIntegrityError):
        store.open_artifact("avtomalyar", "job-success-one", "content.md")


def test_intermediate_jobs_symlink_cannot_redirect_bundle_write(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    company = artifact_root / "companies" / "avtomalyar"
    company.mkdir(parents=True)
    for directory in (artifact_root, artifact_root / "companies", company):
        directory.chmod(0o700)
    outside = tmp_path / "outside"
    outside.mkdir()
    (company / "jobs").symlink_to(outside, target_is_directory=True)

    with pytest.raises(DataIntegrityError):
        ArtifactStore(artifact_root, clock=lambda: NOW).write_bundle(_job(), _result())

    assert list(outside.iterdir()) == []


def test_artifact_file_symlink_is_rejected_instead_of_opened(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, _result())
    bundle = tmp_path / "artifacts" / "companies" / job.company_id / "jobs" / job.job_id
    outside = tmp_path / "outside-secret.txt"
    outside.write_bytes(b"outside-secret")
    bundle.chmod(0o750)
    (bundle / "content.md").unlink()
    (bundle / "content.md").symlink_to(outside)
    bundle.chmod(0o550)
    seen_names: list[str] = []
    original_read = ArtifactStore._read_file

    def record_read(bundle_fd: int, name: str, owner: tuple[int, int]) -> bytes:
        seen_names.append(name)
        return original_read(bundle_fd, name, owner)

    monkeypatch.setattr(ArtifactStore, "_read_file", staticmethod(record_read))

    with pytest.raises(DataIntegrityError):
        store.open_artifact(job.company_id, job.job_id, "content.md")

    assert "content.md" in seen_names


@pytest.mark.parametrize(
    "artifact_root",
    [Path("relative-artifacts"), Path("/"), Path("//")],
)
def test_artifact_root_must_be_absolute_and_dedicated(artifact_root: Path) -> None:
    with pytest.raises(ValueError, match="artifact_root"):
        ArtifactStore(artifact_root)


def test_artifact_root_ancestor_symlink_cannot_redirect_creation(tmp_path: Path) -> None:
    trusted_parent = tmp_path / "trusted"
    outside = tmp_path / "outside"
    trusted_parent.mkdir()
    outside.mkdir()
    (trusted_parent / "redirect").symlink_to(outside, target_is_directory=True)
    store = ArtifactStore(
        trusted_parent / "redirect" / "artifacts",
        clock=lambda: NOW,
    )

    with pytest.raises(DataIntegrityError):
        store.write_bundle(_job(), _result())

    assert not (outside / "artifacts").exists()


def test_hardlinked_artifact_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, _result())
    bundle = (
        tmp_path
        / "artifacts"
        / "companies"
        / job.company_id
        / "jobs"
        / job.job_id
    )
    content = (bundle / "content.md").read_bytes()
    bundle.chmod(0o750)
    (bundle / "content.md").unlink()
    outside = tmp_path / "outside-content.md"
    outside.write_bytes(content)
    outside.chmod(0o440)
    os.link(outside, bundle / "content.md")
    bundle.chmod(0o550)
    seen_names: list[str] = []
    original_read = ArtifactStore._read_file

    def record_read(bundle_fd: int, name: str, owner: tuple[int, int]) -> bytes:
        seen_names.append(name)
        return original_read(bundle_fd, name, owner)

    monkeypatch.setattr(ArtifactStore, "_read_file", staticmethod(record_read))

    with pytest.raises(DataIntegrityError):
        store.open_artifact(job.company_id, job.job_id, "content.md")

    assert "content.md" in seen_names


def test_writable_bundle_and_artifact_modes_are_rejected(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "artifacts", clock=lambda: NOW)
    job = _job()
    store.write_bundle(job, _result())
    bundle = (
        tmp_path
        / "artifacts"
        / "companies"
        / job.company_id
        / "jobs"
        / job.job_id
    )
    bundle.chmod(0o777)
    for artifact in bundle.iterdir():
        artifact.chmod(0o666)

    with pytest.raises(DataIntegrityError):
        store.open_artifact(job.company_id, job.job_id, "content.md")
