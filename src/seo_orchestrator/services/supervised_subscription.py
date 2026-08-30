"""Deterministic finalization for the local supervised subscription rail."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import cast

from seo_orchestrator.canonical import JsonValue, canonical_json
from seo_orchestrator.domain import JobState, SeoJob
from seo_orchestrator.errors import StateConflict
from seo_orchestrator.services.artifacts import (
    ArtifactManifest,
    ArtifactStore,
    ExecutionResult,
    validate_source_provenance,
)
from seo_orchestrator.services.jobs import (
    JobService,
    verified_authoritative_database_path,
)
from seo_orchestrator.supervised_rail import (
    SUPERVISED_MODEL_ID,
    SUPERVISED_PIPELINE_VERSION,
    SUPERVISED_PROVIDER_ID,
    FrozenRunBinding,
    StagePacket,
    SupervisedRail,
    build_stage_packet,
)

_SUPERVISED_EXECUTOR = "supervised-subscription"
_RESULT_DESTINATION = "local-artifacts"
_REVISION_FIELDS = frozenset({"content_markdown", "titles", "descriptions", "sources", "warnings"})


def authoritative_supervised_state_path(database_path: Path) -> Path:
    """Derive the only supervised ledger path from the authoritative SQLite file."""
    resolved = verified_authoritative_database_path(database_path)
    return resolved.parent / f".{resolved.name}.supervised" / "ledger.sqlite"


def _require_authoritative_rail(
    rail: SupervisedRail,
    job_service: JobService,
) -> None:
    expected = authoritative_supervised_state_path(job_service.database_path)
    if rail.state_path != expected:
        raise ValueError("rail is not the authoritative supervised ledger")


def _mapping(value: object, field_name: str) -> dict[str, JsonValue]:
    if type(value) is not dict:
        raise ValueError(f"{field_name} must be a JSON object")
    return cast(dict[str, JsonValue], value)


def _string(value: object, field_name: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _five_strings(value: object, field_name: str) -> tuple[str, str, str, str, str]:
    if (
        type(value) is not list
        or len(value) != 5
        or any(type(item) is not str or not item.strip() for item in value)
    ):
        raise ValueError(f"{field_name} must be an exact five-item string array")
    items = cast(list[str], value)
    return items[0], items[1], items[2], items[3], items[4]


def _string_tuple(value: object, field_name: str) -> tuple[str, ...]:
    if type(value) is not list or any(type(item) is not str or not item.strip() for item in value):
        raise ValueError(f"{field_name} must be a string array")
    return tuple(cast(list[str], value))


def _source_tuple(value: object) -> tuple[JsonValue, ...]:
    if type(value) is not list:
        raise ValueError("sources must be a JSON array")
    sources = tuple(cast(list[JsonValue], value))
    for source in sources:
        normalized = validate_source_provenance(source)
        if canonical_json(normalized) != canonical_json(source):
            raise ValueError("source provenance must already be canonical")
    return sources


def _frozen_source_commitments(context: dict[str, JsonValue]) -> frozenset[bytes]:
    evidence = context.get("evidence")
    if evidence is None:
        return frozenset()
    evidence_mapping = _mapping(evidence, "frozen evidence")
    sources = evidence_mapping.get("sources")
    if type(sources) is not list:
        raise ValueError("frozen evidence sources must be a JSON array")
    commitments: list[bytes] = []
    for source in sources:
        normalized = validate_source_provenance(source)
        if canonical_json(normalized) != canonical_json(source):
            raise ValueError("frozen source provenance must already be canonical")
        commitments.append(canonical_json(source))
    if len(set(commitments)) != len(commitments):
        raise ValueError("frozen evidence sources must be unique")
    return frozenset(commitments)


_CITATION = re.compile(r"\[S([1-9][0-9]*)\]")
_CITATION_LIKE = re.compile(r"\[S[^\]]*\]")


def _validate_content_citations(content: str, source_count: int) -> None:
    expected = set(range(1, source_count + 1))
    citation_numbers = _CITATION.findall(content)
    citations = {int(number) for number in citation_numbers}
    canonical_markers = [f"[S{number}]" for number in citation_numbers]
    if _CITATION_LIKE.findall(content) != canonical_markers or citations != expected:
        raise ValueError("content citation set must exactly reference frozen sources")
    for block in re.split(r"\n\s*\n", content):
        stripped = block.strip()
        if not stripped:
            continue
        lines = [line.strip() for line in stripped.splitlines() if line.strip()]
        if lines and all(line.startswith("#") for line in lines):
            continue
        if _CITATION.search(stripped) is None:
            raise ValueError("each non-heading content block must carry a source citation")


def _validate_plan(job_service: JobService, job_id: str) -> None:
    plan = job_service.execution_plan(job_id)
    if (
        plan.pipeline_version != SUPERVISED_PIPELINE_VERSION
        or plan.executor_name != _SUPERVISED_EXECUTOR
        or plan.model_ids != (SUPERVISED_MODEL_ID,)
        or plan.provider_ids != (SUPERVISED_PROVIDER_ID,)
        or plan.maximum_retries != 0
        or plan.cost_currency is not None
        or plan.cost_min_decimal is not None
        or plan.cost_max_decimal is not None
        or not plan.unknown_cost_reasons
        or plan.result_destination != _RESULT_DESTINATION
    ):
        raise ValueError("approved execution plan is not the supervised subscription plan")


def _validate_binding(job: SeoJob, binding: FrozenRunBinding) -> None:
    if (
        binding.company_id != job.company_id
        or binding.job_id != job.job_id
        or binding.approval_record_id != job.approval_record_id
        or binding.approved_plan_fingerprint != job.approved_plan_fingerprint
        or binding.snapshot_hash != job.snapshot_hash
    ):
        raise ValueError("supervised rail binding does not match the authoritative job")


def _execution_result(payload: JsonValue, context_value: JsonValue) -> ExecutionResult:
    payload_mapping = _mapping(payload, "revision payload")
    if set(payload_mapping) != _REVISION_FIELDS:
        raise ValueError("revision payload must contain exactly the frozen result fields")
    context = _mapping(context_value, "frozen context")
    brief = _mapping(context.get("brief"), "frozen brief")
    primary_keyword = _string(brief.get("primary_keyword"), "primary_keyword")
    content = _string(payload_mapping["content_markdown"], "content_markdown")
    titles = _five_strings(payload_mapping["titles"], "titles")
    descriptions = _five_strings(payload_mapping["descriptions"], "descriptions")
    sources = _source_tuple(payload_mapping["sources"])
    warnings = _string_tuple(payload_mapping["warnings"], "warnings")

    allowed_sources = _frozen_source_commitments(context)
    if not allowed_sources:
        raise ValueError("frozen evidence set must contain at least one source")
    source_commitments = [canonical_json(source) for source in sources]
    if not source_commitments:
        raise ValueError("revision sources must contain at least one frozen source")
    if len(set(source_commitments)) != len(source_commitments):
        raise ValueError("revision sources must be unique")
    for commitment in source_commitments:
        if commitment not in allowed_sources:
            raise ValueError("revision source is absent from the frozen evidence set")
    _validate_content_citations(content, len(sources))

    occurrences = content.casefold().count(primary_keyword.casefold())
    if occurrences == 0:
        raise ValueError("content must contain the frozen primary keyword")
    return ExecutionResult(
        content_markdown=content,
        titles=titles,
        descriptions=descriptions,
        keyword_qa={
            "primary_keyword": primary_keyword,
            "occurrences": occurrences,
            "passed": occurrences > 0,
        },
        text_metrics={
            "characters": len(content),
            "words": len(content.split()),
        },
        sources=sources,
        warnings=warnings,
        model_usage={
            "models": [
                {
                    "model_id": SUPERVISED_MODEL_ID,
                    "provider_id": SUPERVISED_PROVIDER_ID,
                    "input_tokens": 0,
                    "output_tokens": 0,
                }
            ]
        },
        stage_timings={
            "outline_ms": 0,
            "draft_ms": 0,
            "critic_ms": 0,
            "revision_ms": 0,
        },
        prompt_versions={"pipeline": SUPERVISED_PIPELINE_VERSION},
    )


def prepare_supervised_packet(
    *,
    rail: SupervisedRail,
    job_service: JobService,
    job_id: str,
    designated_session_ref: str,
) -> StagePacket:
    """Open or resume one approved local supervised session without provider I/O."""
    if not isinstance(rail, SupervisedRail):
        raise TypeError("rail must be a SupervisedRail")
    if not isinstance(job_service, JobService):
        raise TypeError("job_service must be a JobService")
    if type(job_id) is not str or not job_id.strip():
        raise ValueError("job_id must be a non-empty string")
    if type(designated_session_ref) is not str or not designated_session_ref.strip():
        raise ValueError("designated_session_ref must be a non-empty string")

    _require_authoritative_rail(rail, job_service)
    job = job_service.get_job(job_id)
    _validate_plan(job_service, job_id)
    if job.state is JobState.QUEUED:
        try:
            queued_job, queued_snapshot = job_service.prepare_execution(job_id)
            build_stage_packet(
                queued_job,
                queued_snapshot,
                stage_id="outline",
                sequence=0,
                designated_session_ref=designated_session_ref,
            )
            job = job_service.transition(
                job_id,
                JobState.QUEUED,
                JobState.RUNNING,
                "supervised operator execution started",
                current_stage="outline",
            )
        except StateConflict:
            job = job_service.get_job(job_id)
    if job.state is not JobState.RUNNING:
        raise StateConflict
    job, snapshot = job_service.running_execution_identity(job_id)
    build_stage_packet(
        job,
        snapshot,
        stage_id="outline",
        sequence=0,
        designated_session_ref=designated_session_ref,
    )
    return rail.prepare_packet(
        job,
        snapshot,
        designated_session_ref=designated_session_ref,
    )


class SupervisedSubscriptionFinalizer:
    """Finalize one operator-observed result without provider or Hermes I/O."""

    def __init__(
        self,
        *,
        rail: SupervisedRail,
        job_service: JobService,
        artifact_store: ArtifactStore,
    ) -> None:
        if not isinstance(rail, SupervisedRail):
            raise TypeError("rail must be a SupervisedRail")
        if not isinstance(job_service, JobService):
            raise TypeError("job_service must be a JobService")
        if not isinstance(artifact_store, ArtifactStore):
            raise TypeError("artifact_store must be an ArtifactStore")
        _require_authoritative_rail(rail, job_service)
        self._rail = rail
        self._jobs = job_service
        self._artifacts = artifact_store

    def finalize(self, job_id: str) -> ArtifactManifest:
        if type(job_id) is not str or not job_id.strip():
            raise ValueError("job_id must be a non-empty string")
        _require_authoritative_rail(self._rail, self._jobs)
        job = self._jobs.get_job(job_id)
        _validate_plan(self._jobs, job_id)
        binding = self._rail.frozen_binding(
            company_id=job.company_id,
            job_id=job.job_id,
        )
        _validate_binding(job, binding)
        completion = self._rail.final_completion(
            company_id=job.company_id,
            job_id=job.job_id,
        )
        result = _execution_result(
            completion.payload,
            self._rail.frozen_context(
                company_id=job.company_id,
                job_id=job.job_id,
            ),
        )

        if job.state is JobState.RUNNING:
            _require_authoritative_rail(self._rail, self._jobs)
            try:
                job = self._jobs.transition(
                    job_id,
                    JobState.RUNNING,
                    JobState.SUCCEEDED,
                    "supervised subscription final QA accepted",
                    current_stage="revision",
                )
            except StateConflict:
                job = self._jobs.get_job(job_id)
        if job.state is not JobState.SUCCEEDED:
            raise StateConflict
        _validate_binding(job, binding)

        _require_authoritative_rail(self._rail, self._jobs)
        manifest = self._artifacts.write_bundle(job, result)
        _require_authoritative_rail(self._rail, self._jobs)
        bound_job = self._jobs.bind_artifact_manifest(job_id)
        manifest_path = bound_job.artifact_manifest_path
        if manifest_path is None:
            raise StateConflict
        with self._jobs.open_artifact(job_id, "manifest.json") as manifest_file:
            manifest_hash = hashlib.sha256(manifest_file.read()).hexdigest()
        _require_authoritative_rail(self._rail, self._jobs)
        self._rail.record_artifact(
            company_id=job.company_id,
            job_id=job.job_id,
            manifest_path=manifest_path,
            manifest_hash=manifest_hash,
        )
        return manifest
