"""Integration coverage for the local-only supervised subscription rail."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from argparse import Namespace
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from seo_orchestrator import cli
from seo_orchestrator.canonical import JsonValue, canonical_json, sha256_fingerprint
from seo_orchestrator.db import connection as connection_module
from seo_orchestrator.db.connection import connect
from seo_orchestrator.domain import JobState
from seo_orchestrator.errors import DataIntegrityError, NotFound
from seo_orchestrator.services.approvals import ApprovalService
from seo_orchestrator.services.artifacts import ArtifactManifest, ArtifactStore
from seo_orchestrator.services.jobs import JobService
from seo_orchestrator.services.supervised_subscription import (
    SupervisedSubscriptionFinalizer,
    authoritative_supervised_state_path,
    prepare_supervised_packet,
)
from seo_orchestrator.settings import Settings
from seo_orchestrator.supervised_rail import (
    ObservedCompletion,
    OperatorAttestation,
    StagePacket,
    SupervisedRail,
    SupervisedStatus,
)
from tests.e2e.support import (
    PlannedFlow,
    app_for,
    create_company_card,
    load_company_fixture,
    make_settings,
    plan_flow,
)

EVIDENCE_FETCHED_AT = datetime(2026, 8, 27, 9, 0, tzinfo=UTC)
EVIDENCE_SOURCE: dict[str, JsonValue] = {
    "url": "https://example.test/evidence",
    "content_hash": "e" * 64,
    "fetched_at": EVIDENCE_FETCHED_AT.isoformat(),
}
_FLOW_TIMES: dict[str, datetime] = {}


def _flow_time(job_id: str) -> datetime:
    try:
        return _FLOW_TIMES[job_id]
    except KeyError as exc:
        raise AssertionError("supervised fixture time is unavailable") from exc


def _plan_supervised_flow(
    app: Any,
    settings: Settings,
    fixture: dict[str, Any],
    *,
    pipeline_version: str,
    executor_name: str,
    model_ids: tuple[str, ...],
    provider_ids: tuple[str, ...],
    maximum_retries: int,
    evidence_sources: tuple[dict[str, Any], ...],
) -> PlannedFlow:
    flow = plan_flow(
        app,
        settings,
        fixture,
        approve=False,
        pipeline_version=pipeline_version,
        executor_name=executor_name,
        model_ids=model_ids,
        provider_ids=provider_ids,
        maximum_retries=maximum_retries,
        evidence_sources=evidence_sources,
    )
    connection = connect(settings.db_path)
    try:
        approval = ApprovalService(
            connection,
            company_id=flow.company_id,
            id_factory=lambda: f"approval-{flow.job_id}",
        ).approve_job(
            flow.job_id,
            "local-supervised-approver",
            flow.snapshot_hash,
            flow.plan_fingerprint,
        )
        _FLOW_TIMES[flow.job_id] = approval.approved_at
    finally:
        connection.close()
    return flow


def _captured_cli_json(capsys: pytest.CaptureFixture[str]) -> dict[str, JsonValue]:
    output = capsys.readouterr().out
    value = json.loads(output)
    assert type(value) is dict
    assert output == canonical_json(value).decode("utf-8") + "\n"
    return value


def _completion(
    packet: StagePacket,
    payload: JsonValue,
    *,
    sequence_offset: int,
) -> ObservedCompletion:
    return ObservedCompletion(
        job_id=packet.job_id,
        company_id=packet.company_id,
        stage_id=packet.stage_id,
        input_hash=packet.input_hash,
        payload=payload,
        attestation=OperatorAttestation(
            operator_id="operator-one",
            session_ref=packet.designated_session_ref,
            provider_id=packet.provider_id,
            model_id=packet.model_id,
            observed_at=_flow_time(packet.job_id) + timedelta(seconds=sequence_offset),
        ),
    )


def _revision_payload(primary_keyword: str) -> dict[str, JsonValue]:
    content = (
        f"# {primary_keyword}\n\nA deterministic supervised result about {primary_keyword}. [S1]"
    )
    return {
        "content_markdown": content,
        "titles": [f"{primary_keyword} title {index}" for index in range(1, 6)],
        "descriptions": [
            f"Deterministic {primary_keyword} description {index}." for index in range(1, 6)
        ],
        "sources": [EVIDENCE_SOURCE],
        "warnings": [],
    }


def _revision_without_primary_keyword(_primary_keyword: str) -> JsonValue:
    return _revision_payload("unrelated phrase")


def _revision_without_citation(primary_keyword: str) -> JsonValue:
    payload = _revision_payload(primary_keyword)
    content = payload["content_markdown"]
    assert type(content) is str
    payload["content_markdown"] = content.replace(" [S1]", "")
    return payload


def _revision_with_unknown_citation(primary_keyword: str) -> JsonValue:
    payload = _revision_payload(primary_keyword)
    content = payload["content_markdown"]
    assert type(content) is str
    payload["content_markdown"] = content.replace("[S1]", "[S2]")
    return payload


def _complete_four_stages(
    rail: SupervisedRail,
    first_packet: StagePacket,
    *,
    primary_keyword: str,
    revision_payload: JsonValue | None = None,
) -> None:
    packet = first_packet
    intermediate_payloads: tuple[JsonValue, JsonValue, JsonValue] = (
        {"sections": ["overview", "process", "decision"]},
        {"content_markdown": f"Draft about {primary_keyword}."},
        {"issues": [], "decision": "revise"},
    )
    for sequence_offset, payload in enumerate(intermediate_payloads, start=1):
        next_packet = rail.bind_completion(
            _completion(packet, payload, sequence_offset=sequence_offset)
        )
        assert isinstance(next_packet, StagePacket)
        packet = next_packet
    terminal = rail.bind_completion(
        _completion(
            packet,
            revision_payload
            if revision_payload is not None
            else _revision_payload(primary_keyword),
            sequence_offset=4,
        )
    )
    assert terminal is SupervisedStatus.FINAL_QA_READY


def _prepared_final_qa(
    tmp_path: Path,
    *,
    model_ids: tuple[str, ...] = ("gpt-5.6-terra",),
    provider_ids: tuple[str, ...] = ("openai-codex",),
    maximum_retries: int = 0,
    revision_payload: JsonValue | None = None,
    revision_payload_factory: Callable[[str], JsonValue] | None = None,
) -> tuple[
    Settings,
    PlannedFlow,
    sqlite3.Connection,
    JobService,
    ArtifactStore,
    SupervisedRail,
]:
    settings = make_settings(tmp_path)
    app = app_for(settings)
    fixture = load_company_fixture("avtomalyar")
    create_company_card(app, settings, fixture)
    flow = _plan_supervised_flow(
        app,
        settings,
        fixture,
        pipeline_version="supervised-subscription-v1",
        executor_name="supervised-subscription",
        model_ids=model_ids,
        provider_ids=provider_ids,
        maximum_retries=maximum_retries,
        evidence_sources=(EVIDENCE_SOURCE,),
    )
    store = ArtifactStore(
        settings.artifact_root,
        clock=lambda: _flow_time(flow.job_id) + timedelta(seconds=1),
    )
    connection = connect(settings.db_path)
    service = JobService(
        connection,
        company_id=flow.company_id,
        clock=lambda: _flow_time(flow.job_id),
        artifact_store=store,
    )
    service.transition(
        flow.job_id,
        JobState.QUEUED,
        JobState.RUNNING,
        "supervised operator execution started",
        current_stage="outline",
    )
    job, snapshot = service.running_execution_identity(flow.job_id)
    context = snapshot.thawed_compiled_context()
    if type(context) is not dict:
        raise AssertionError("fixture snapshot is invalid")
    brief = context.get("brief")
    if type(brief) is not dict:
        raise AssertionError("fixture brief is invalid")
    primary_keyword = brief.get("primary_keyword")
    if type(primary_keyword) is not str:
        raise AssertionError("fixture primary keyword is invalid")
    rail = SupervisedRail(state_path=authoritative_supervised_state_path(settings.db_path))
    first_packet = rail.prepare_packet(
        job,
        snapshot,
        designated_session_ref="local-session-designated",
    )
    _complete_four_stages(
        rail,
        first_packet,
        primary_keyword=primary_keyword,
        revision_payload=(
            revision_payload_factory(primary_keyword)
            if revision_payload_factory is not None
            else revision_payload
        ),
    )
    return settings, flow, connection, service, store, rail


def test_finalizer_rejects_materialized_completion_not_bound_to_authenticated_event(
    tmp_path: Path,
) -> None:
    settings, flow, connection, service, store, rail = _prepared_final_qa(tmp_path)
    rail_path = authoritative_supervised_state_path(settings.db_path)
    try:
        with sqlite3.connect(rail_path) as ledger:
            row = ledger.execute(
                "SELECT completion.completion_json FROM supervised_completions AS completion "
                "JOIN supervised_packets AS packet ON packet.company_id=completion.company_id "
                "AND packet.job_id=completion.job_id AND packet.input_hash=completion.input_hash "
                "WHERE completion.company_id=? AND completion.job_id=? AND packet.stage_id='revision'",
                (flow.company_id, flow.job_id),
            ).fetchone()
            assert row is not None
            altered = json.loads(row[0])
            altered["payload"]["titles"][0] = "Authenticated chain bypass"
            altered_text = canonical_json(altered).decode("utf-8")
            ledger.execute("DROP TRIGGER supervised_completions_no_update")
            ledger.execute(
                "UPDATE supervised_completions SET completion_json=?, completion_hash=? "
                "WHERE company_id=? AND job_id=? AND input_hash=("
                "SELECT input_hash FROM supervised_packets WHERE company_id=? AND job_id=? "
                "AND stage_id='revision')",
                (
                    altered_text,
                    sha256_fingerprint(altered),
                    flow.company_id,
                    flow.job_id,
                    flow.company_id,
                    flow.job_id,
                ),
            )

        finalizer = SupervisedSubscriptionFinalizer(
            rail=rail,
            job_service=service,
            artifact_store=store,
        )
        with pytest.raises(DataIntegrityError):
            finalizer.finalize(flow.job_id)

        unchanged = service.get_job(flow.job_id)
        assert unchanged.state is JobState.RUNNING
        assert unchanged.artifact_manifest_path is None
        assert not (
            settings.artifact_root
            / "companies"
            / flow.company_id
            / "jobs"
            / flow.job_id
            / "manifest.json"
        ).exists()
    finally:
        connection.close()


def test_finalizer_freezes_one_manifest_and_replays_after_restart(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    app = app_for(settings)
    fixture = load_company_fixture("avtomalyar")
    create_company_card(app, settings, fixture)
    flow = _plan_supervised_flow(
        app,
        settings,
        fixture,
        pipeline_version="supervised-subscription-v1",
        executor_name="supervised-subscription",
        model_ids=("gpt-5.6-terra",),
        provider_ids=("openai-codex",),
        maximum_retries=0,
        evidence_sources=(EVIDENCE_SOURCE,),
    )
    rail_path = authoritative_supervised_state_path(settings.db_path)
    store = ArtifactStore(
        settings.artifact_root,
        clock=lambda: _flow_time(flow.job_id) + timedelta(seconds=1),
    )

    connection = connect(settings.db_path)
    try:
        service = JobService(
            connection,
            company_id=flow.company_id,
            clock=lambda: _flow_time(flow.job_id),
            artifact_store=store,
        )
        running = service.transition(
            flow.job_id,
            JobState.QUEUED,
            JobState.RUNNING,
            "supervised operator execution started",
            current_stage="outline",
        )
        assert running.attempt == 1
        job, snapshot = service.running_execution_identity(flow.job_id)
        context = snapshot.thawed_compiled_context()
        assert isinstance(context, dict)
        brief = context["brief"]
        assert isinstance(brief, dict)
        primary_keyword = brief["primary_keyword"]
        assert isinstance(primary_keyword, str)

        rail = SupervisedRail(state_path=rail_path)
        first_packet = rail.prepare_packet(
            job,
            snapshot,
            designated_session_ref="local-session-designated",
        )
        _complete_four_stages(
            rail,
            first_packet,
            primary_keyword=primary_keyword,
        )
        finalizer = SupervisedSubscriptionFinalizer(
            rail=rail,
            job_service=service,
            artifact_store=store,
        )
        first_manifest = finalizer.finalize(flow.job_id)
        succeeded = service.get_job(flow.job_id)

        assert succeeded.state is JobState.SUCCEEDED
        assert succeeded.current_stage == "revision"
        assert succeeded.artifact_manifest_path == str(
            settings.artifact_root
            / "companies"
            / flow.company_id
            / "jobs"
            / flow.job_id
            / "manifest.json"
        )
        assert rail.status(company_id=flow.company_id, job_id=flow.job_id) is (
            SupervisedStatus.ARTIFACT_FROZEN
        )
        frozen = rail.artifact_binding(company_id=flow.company_id, job_id=flow.job_id)
        assert frozen.manifest_path == succeeded.artifact_manifest_path
        assert len(frozen.manifest_hash) == 64
    finally:
        connection.close()

    reopened = connect(settings.db_path)
    try:
        reopened_store = ArtifactStore(
            settings.artifact_root,
            clock=lambda: _flow_time(flow.job_id) + timedelta(minutes=2),
        )
        reopened_service = JobService(
            reopened,
            company_id=flow.company_id,
            clock=lambda: _flow_time(flow.job_id) + timedelta(minutes=1),
            artifact_store=reopened_store,
        )
        reopened_finalizer = SupervisedSubscriptionFinalizer(
            rail=SupervisedRail(state_path=rail_path),
            job_service=reopened_service,
            artifact_store=reopened_store,
        )
        replayed_manifest = reopened_finalizer.finalize(flow.job_id)

        assert replayed_manifest == first_manifest
        assert reopened_service.get_job(flow.job_id).state is JobState.SUCCEEDED
        manifest_path = Path(reopened_service.get_job(flow.job_id).artifact_manifest_path or "")
        manifest_value = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert manifest_value["model_usage"] == {
            "models": [
                {
                    "input_tokens": 0,
                    "model_id": "gpt-5.6-terra",
                    "output_tokens": 0,
                    "provider_id": "openai-codex",
                }
            ]
        }
        assert manifest_value["stage_timings"] == {
            "critic_ms": 0,
            "draft_ms": 0,
            "outline_ms": 0,
            "revision_ms": 0,
        }
        assert manifest_value["prompt_versions"] == {"pipeline": "supervised-subscription-v1"}

        manifest_path.chmod(0o600)
        manifest_path.write_text("{}", encoding="utf-8")
        manifest_path.chmod(0o440)
        with pytest.raises(DataIntegrityError):
            reopened_finalizer.finalize(flow.job_id)
    finally:
        reopened.close()


def test_finalizer_recovers_after_artifact_write_before_job_bind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, flow, connection, service, store, rail = _prepared_final_qa(tmp_path)
    original_bind = service.bind_artifact_manifest

    def fail_after_artifact_write(_job_id: str) -> None:
        raise RuntimeError("injected post-artifact failure")

    try:
        monkeypatch.setattr(service, "bind_artifact_manifest", fail_after_artifact_write)
        finalizer = SupervisedSubscriptionFinalizer(
            rail=rail,
            job_service=service,
            artifact_store=store,
        )
        with pytest.raises(RuntimeError, match="post-artifact"):
            finalizer.finalize(flow.job_id)

        interrupted = service.get_job(flow.job_id)
        assert interrupted.state is JobState.SUCCEEDED
        assert interrupted.artifact_manifest_path is None
        assert rail.status(company_id=flow.company_id, job_id=flow.job_id) is (
            SupervisedStatus.FINAL_QA_READY
        )
        monkeypatch.setattr(service, "bind_artifact_manifest", original_bind)

        manifest = finalizer.finalize(flow.job_id)
        assert manifest.job_id == flow.job_id
        assert service.get_job(flow.job_id).artifact_manifest_path is not None
        assert rail.status(company_id=flow.company_id, job_id=flow.job_id) is (
            SupervisedStatus.ARTIFACT_FROZEN
        )
    finally:
        connection.close()


def test_finalizer_recovers_after_job_bind_before_rail_freeze(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, flow, connection, service, store, rail = _prepared_final_qa(tmp_path)
    original_record = rail.record_artifact

    def fail_after_job_bind(**_kwargs: object) -> None:
        raise RuntimeError("injected pre-rail-freeze failure")

    try:
        monkeypatch.setattr(rail, "record_artifact", fail_after_job_bind)
        finalizer = SupervisedSubscriptionFinalizer(
            rail=rail,
            job_service=service,
            artifact_store=store,
        )
        with pytest.raises(RuntimeError, match="pre-rail-freeze"):
            finalizer.finalize(flow.job_id)

        interrupted = service.get_job(flow.job_id)
        assert interrupted.state is JobState.SUCCEEDED
        assert interrupted.artifact_manifest_path is not None
        assert rail.status(company_id=flow.company_id, job_id=flow.job_id) is (
            SupervisedStatus.FINAL_QA_READY
        )
        monkeypatch.setattr(rail, "record_artifact", original_record)

        manifest = finalizer.finalize(flow.job_id)
        assert manifest.job_id == flow.job_id
        assert rail.status(company_id=flow.company_id, job_id=flow.job_id) is (
            SupervisedStatus.ARTIFACT_FROZEN
        )
    finally:
        connection.close()


def test_competing_finalizers_converge_on_one_manifest(tmp_path: Path) -> None:
    settings, flow, connection, _, _, _ = _prepared_final_qa(tmp_path)
    connection.close()
    barrier = threading.Barrier(2)

    def finalize_once() -> ArtifactManifest:
        local_connection = connect(settings.db_path)
        try:
            local_store = ArtifactStore(
                settings.artifact_root,
                clock=lambda: _flow_time(flow.job_id) + timedelta(seconds=1),
            )
            local_service = JobService(
                local_connection,
                company_id=flow.company_id,
                clock=lambda: _flow_time(flow.job_id),
                artifact_store=local_store,
            )
            finalizer = SupervisedSubscriptionFinalizer(
                rail=SupervisedRail(
                    state_path=authoritative_supervised_state_path(settings.db_path)
                ),
                job_service=local_service,
                artifact_store=local_store,
            )
            barrier.wait(timeout=10)
            return finalizer.finalize(flow.job_id)
        finally:
            local_connection.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(finalize_once) for _ in range(2)]
        manifests = [future.result(timeout=30) for future in futures]

    assert manifests[0] == manifests[1]
    verify_connection = connect(settings.db_path)
    try:
        verify_service = JobService(
            verify_connection,
            company_id=flow.company_id,
            clock=lambda: _flow_time(flow.job_id),
            artifact_store=ArtifactStore(
                settings.artifact_root, clock=lambda: _flow_time(flow.job_id)
            ),
        )
        job = verify_service.get_job(flow.job_id)
        assert job.state is JobState.SUCCEEDED
        assert job.artifact_manifest_path is not None
    finally:
        verify_connection.close()


@pytest.mark.parametrize(
    ("revision_payload_factory", "message"),
    [
        (_revision_without_primary_keyword, "primary keyword"),
        (_revision_without_citation, "citation"),
        (_revision_with_unknown_citation, "citation"),
    ],
)
def test_finalizer_rejects_failed_keyword_or_evidence_qa_without_side_effects(
    tmp_path: Path,
    revision_payload_factory: Callable[[str], JsonValue],
    message: str,
) -> None:
    _, flow, connection, service, store, rail = _prepared_final_qa(
        tmp_path,
        revision_payload_factory=revision_payload_factory,
    )
    try:
        finalizer = SupervisedSubscriptionFinalizer(
            rail=rail,
            job_service=service,
            artifact_store=store,
        )

        with pytest.raises(ValueError, match=message):
            finalizer.finalize(flow.job_id)

        job = service.get_job(flow.job_id)
        assert job.state is JobState.RUNNING
        assert job.artifact_manifest_path is None
        assert (
            rail.status(company_id=flow.company_id, job_id=flow.job_id)
            is SupervisedStatus.FINAL_QA_READY
        )
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("model_ids", "provider_ids", "maximum_retries"),
    [
        (("wrong-model",), ("openai-codex",), 0),
        (("gpt-5.6-terra",), ("wrong-provider",), 0),
        (("gpt-5.6-terra",), ("openai-codex",), 1),
    ],
)
def test_finalizer_rejects_non_supervised_approved_plan_before_transition(
    tmp_path: Path,
    model_ids: tuple[str, ...],
    provider_ids: tuple[str, ...],
    maximum_retries: int,
) -> None:
    _, flow, connection, service, store, rail = _prepared_final_qa(
        tmp_path,
        model_ids=model_ids,
        provider_ids=provider_ids,
        maximum_retries=maximum_retries,
    )
    try:
        finalizer = SupervisedSubscriptionFinalizer(
            rail=rail,
            job_service=service,
            artifact_store=store,
        )
        with pytest.raises(ValueError, match="approved execution plan"):
            finalizer.finalize(flow.job_id)
        job = service.get_job(flow.job_id)
        assert job.state is JobState.RUNNING
        assert job.artifact_manifest_path is None
    finally:
        connection.close()


def test_revision_binding_rejects_non_frozen_source_before_persistence(
    tmp_path: Path,
) -> None:
    revision_payload = {
        **_revision_payload("invented car-painting service"),
        "sources": [
            {
                "url": "https://example.com/not-frozen",
                "fetched_at": "2026-08-27T12:00:00+00:00",
                "content_hash": "a" * 64,
            }
        ],
    }

    with pytest.raises(ValueError, match="frozen evidence"):
        _prepared_final_qa(tmp_path, revision_payload=revision_payload)


def test_revision_binding_rejects_credential_text_before_success_or_artifact(
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    app = app_for(settings)
    fixture = load_company_fixture("avtomalyar")
    create_company_card(app, settings, fixture)
    flow = _plan_supervised_flow(
        app,
        settings,
        fixture,
        pipeline_version="supervised-subscription-v1",
        executor_name="supervised-subscription",
        model_ids=("gpt-5.6-terra",),
        provider_ids=("openai-codex",),
        maximum_retries=0,
        evidence_sources=(EVIDENCE_SOURCE,),
    )
    store = ArtifactStore(settings.artifact_root, clock=lambda: _flow_time(flow.job_id))
    connection = connect(settings.db_path)
    service = JobService(
        connection,
        company_id=flow.company_id,
        clock=lambda: _flow_time(flow.job_id),
        artifact_store=store,
    )
    rail = SupervisedRail(state_path=authoritative_supervised_state_path(settings.db_path))
    payload = _revision_payload("покраска авто")
    payload["content_markdown"] = "# покраска авто\n\n-----BEGIN PRIVATE KEY-----\nnot-a-real-key"
    try:
        packet = prepare_supervised_packet(
            job_id=flow.job_id,
            designated_session_ref="visible-session-designated",
            job_service=service,
            rail=rail,
        )
        intermediate_payloads: tuple[JsonValue, ...] = (
            {"sections": ["overview", "process", "decision"]},
            {"content_markdown": "Bounded draft."},
            {"issues": [], "decision": "revise"},
        )
        for sequence_offset, stage_payload in enumerate(
            intermediate_payloads,
            start=1,
        ):
            outcome = rail.bind_completion(
                _completion(
                    packet,
                    stage_payload,
                    sequence_offset=sequence_offset,
                )
            )
            assert isinstance(outcome, StagePacket)
            packet = outcome
        assert packet.stage_id == "revision"
        with pytest.raises(ValueError, match="credential"):
            rail.bind_completion(_completion(packet, payload, sequence_offset=4))
        job = service.get_job(flow.job_id)
        assert job.state is JobState.RUNNING
        assert job.artifact_manifest_path is None
        assert rail.status(company_id=flow.company_id, job_id=flow.job_id) is (
            SupervisedStatus.AWAITING_OPERATOR_EXECUTION
        )
    finally:
        connection.close()


def test_finalizer_preserves_company_scope_before_rail_lookup(tmp_path: Path) -> None:
    _, flow, connection, service, store, rail = _prepared_final_qa(tmp_path)
    try:
        wrong_company_service = JobService(
            connection,
            company_id="different-company",
            clock=lambda: _flow_time(flow.job_id),
            artifact_store=store,
        )
        finalizer = SupervisedSubscriptionFinalizer(
            rail=rail,
            job_service=wrong_company_service,
            artifact_store=store,
        )
        with pytest.raises(NotFound):
            finalizer.finalize(flow.job_id)
        assert service.get_job(flow.job_id).state is JobState.RUNNING
    finally:
        connection.close()


def test_prepare_supervised_packet_starts_once_and_replays_same_packet(
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    app = app_for(settings)
    fixture = load_company_fixture("avtomalyar")
    create_company_card(app, settings, fixture)
    flow = _plan_supervised_flow(
        app,
        settings,
        fixture,
        pipeline_version="supervised-subscription-v1",
        executor_name="supervised-subscription",
        model_ids=("gpt-5.6-terra",),
        provider_ids=("openai-codex",),
        maximum_retries=0,
        evidence_sources=(EVIDENCE_SOURCE,),
    )
    connection = connect(settings.db_path)
    try:
        service = JobService(
            connection, company_id=flow.company_id, clock=lambda: _flow_time(flow.job_id)
        )
        state_path = authoritative_supervised_state_path(settings.db_path)
        first = prepare_supervised_packet(
            rail=SupervisedRail(state_path=state_path),
            job_service=service,
            job_id=flow.job_id,
            designated_session_ref="visible-session-designated",
        )
        replayed = prepare_supervised_packet(
            rail=SupervisedRail(state_path=state_path),
            job_service=service,
            job_id=flow.job_id,
            designated_session_ref="visible-session-designated",
        )

        assert first == replayed
        assert first.stage_id == "outline"
        assert service.get_job(flow.job_id).state is JobState.RUNNING
    finally:
        connection.close()


def test_prepare_supervised_packet_rejects_non_authoritative_rail_before_transition(
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    app = app_for(settings)
    fixture = load_company_fixture("avtomalyar")
    create_company_card(app, settings, fixture)
    flow = _plan_supervised_flow(
        app,
        settings,
        fixture,
        pipeline_version="supervised-subscription-v1",
        executor_name="supervised-subscription",
        model_ids=("gpt-5.6-terra",),
        provider_ids=("openai-codex",),
        maximum_retries=0,
        evidence_sources=(EVIDENCE_SOURCE,),
    )
    connection = connect(settings.db_path)
    try:
        service = JobService(
            connection, company_id=flow.company_id, clock=lambda: _flow_time(flow.job_id)
        )
        alternate = SupervisedRail(state_path=tmp_path / "alternate-private" / "supervised.sqlite")

        with pytest.raises(ValueError, match="authoritative supervised ledger"):
            prepare_supervised_packet(
                rail=alternate,
                job_service=service,
                job_id=flow.job_id,
                designated_session_ref="visible-session-designated",
            )

        assert service.get_job(flow.job_id).state is JobState.QUEUED
    finally:
        connection.close()


def test_prepare_supervised_packet_rejects_invalid_evidence_before_transition(
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    app = app_for(settings)
    fixture = load_company_fixture("avtomalyar")
    create_company_card(app, settings, fixture)
    flow = _plan_supervised_flow(
        app,
        settings,
        fixture,
        pipeline_version="supervised-subscription-v1",
        executor_name="supervised-subscription",
        model_ids=("gpt-5.6-terra",),
        provider_ids=("openai-codex",),
        maximum_retries=0,
        evidence_sources=(),
    )
    connection = connect(settings.db_path)
    try:
        service = JobService(
            connection, company_id=flow.company_id, clock=lambda: _flow_time(flow.job_id)
        )
        rail = SupervisedRail(state_path=authoritative_supervised_state_path(settings.db_path))

        with pytest.raises(ValueError, match="frozen evidence"):
            prepare_supervised_packet(
                rail=rail,
                job_service=service,
                job_id=flow.job_id,
                designated_session_ref="visible-session-designated",
            )

        rejected = service.get_job(flow.job_id)
        assert rejected.state is JobState.QUEUED
        assert rejected.current_stage is None
        with pytest.raises(LookupError, match="supervised run was not found"):
            rail.status(company_id=flow.company_id, job_id=flow.job_id)
    finally:
        connection.close()


def test_authoritative_main_database_rejects_postcheck_hardlink(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    app_for(settings)
    alias = tmp_path / "worker-hardlink.db"
    os.link(settings.db_path, alias)

    with pytest.raises(ValueError, match="single-link"):
        authoritative_supervised_state_path(settings.db_path)

    connection = connect(settings.db_path)
    try:
        service = JobService(connection, company_id="company-avtomalyar")
        with pytest.raises(ValueError, match="single-link"):
            _ = service.database_path
    finally:
        connection.close()


def test_main_database_connect_rejects_symlink_alias(tmp_path: Path) -> None:
    real_path = tmp_path / "real.sqlite"
    connection = connect(real_path)
    connection.close()
    alias_path = tmp_path / "alias.sqlite"
    alias_path.symlink_to(real_path)

    with pytest.raises(ValueError, match="alias"):
        connect(alias_path)


def test_supervised_rail_rejects_symlinked_ancestor_alias(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir(mode=0o700)
    state_parent = real_parent / "private"
    state_parent.mkdir(mode=0o700)
    alias_parent = tmp_path / "alias"
    alias_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(ValueError, match="alias"):
        SupervisedRail(state_path=alias_parent / "private" / "ledger.sqlite")


def test_new_main_database_rejects_inode_swap_during_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "main.sqlite"
    moved_path = tmp_path / "opened-inode.sqlite"
    raw_connect = sqlite3.connect

    def replace_after_open(
        path: str | Path,
        *args: Any,
        **kwargs: Any,
    ) -> sqlite3.Connection:
        opened = raw_connect(path, *args, **kwargs)
        opened.execute("CREATE TABLE opened_marker(value TEXT NOT NULL)")
        opened.execute("INSERT INTO opened_marker VALUES ('inode-a')")
        opened.commit()
        database_path.replace(moved_path)
        replacement = raw_connect(database_path)
        replacement.close()
        return opened

    monkeypatch.setattr(connection_module.sqlite3, "connect", replace_after_open)

    with pytest.raises(ValueError, match="identity changed while opening"):
        connect(database_path)


def test_authoritative_main_database_rejects_path_replacement_after_connect(
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    app_for(settings)
    connection = connect(settings.db_path)
    connection.execute("CREATE TABLE connection_identity_marker(value TEXT NOT NULL)")
    connection.execute(
        "INSERT INTO connection_identity_marker(value) VALUES ('original-open-connection')"
    )
    connection.commit()
    moved_path = settings.db_path.with_name("worker-original.sqlite")
    try:
        settings.db_path.replace(moved_path)
        replacement = sqlite3.connect(settings.db_path)
        replacement.close()
        assert connection.execute("SELECT value FROM connection_identity_marker").fetchone() == (
            "original-open-connection",
        )

        service = JobService(connection, company_id="avtomalyar")
        with pytest.raises(ValueError, match="opened database identity"):
            _ = service.database_path
    finally:
        connection.close()


def test_preconstructed_finalizer_rejects_main_database_path_replacement_before_effects(
    tmp_path: Path,
) -> None:
    settings, flow, connection, service, store, rail = _prepared_final_qa(tmp_path)
    finalizer = SupervisedSubscriptionFinalizer(
        rail=rail,
        job_service=service,
        artifact_store=store,
    )
    moved_path = settings.db_path.with_name("worker-original.sqlite")
    try:
        settings.db_path.replace(moved_path)
        replacement = sqlite3.connect(settings.db_path)
        replacement.close()

        with pytest.raises(ValueError, match="opened database identity"):
            finalizer.finalize(flow.job_id)

        detached_job = service.get_job(flow.job_id)
        assert detached_job.state is JobState.RUNNING
        assert detached_job.artifact_manifest_path is None
        assert not (settings.artifact_root / flow.company_id / flow.job_id).exists()
        assert rail.status(company_id=flow.company_id, job_id=flow.job_id) is (
            SupervisedStatus.FINAL_QA_READY
        )
    finally:
        connection.close()


def test_supervised_cli_runs_complete_offline_lifecycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = make_settings(tmp_path)
    app = app_for(settings)
    fixture = load_company_fixture("avtomalyar")
    create_company_card(app, settings, fixture)
    flow = _plan_supervised_flow(
        app,
        settings,
        fixture,
        pipeline_version="supervised-subscription-v1",
        executor_name="supervised-subscription",
        model_ids=("gpt-5.6-terra",),
        provider_ids=("openai-codex",),
        maximum_retries=0,
        evidence_sources=(EVIDENCE_SOURCE,),
    )
    shared = [
        "--company-id",
        flow.company_id,
        "--job-id",
        flow.job_id,
    ]
    monkeypatch.setattr(cli.Settings, "from_env", lambda _env: settings)
    monkeypatch.setattr(
        cli,
        "serve_worker",
        lambda *_args, **_kwargs: pytest.fail("supervised CLI must not serve"),
    )
    monkeypatch.setattr(
        cli,
        "run_worker",
        lambda *_args, **_kwargs: pytest.fail("supervised CLI must not run worker"),
    )

    cli.main(
        [
            "supervised-packet",
            *shared,
            "--session-ref",
            "visible-session-designated",
        ]
    )
    output = _captured_cli_json(capsys)
    assert output["stage_id"] == "outline"
    assert output["provider_id"] == "openai-codex"
    assert output["model_id"] == "gpt-5.6-terra"
    packet_value = output
    prompt = packet_value.get("prompt")
    assert type(prompt) is str
    frozen_context = json.loads(prompt.split("\nfrozen-context=", maxsplit=1)[1])
    assert type(frozen_context) is dict
    frozen_brief = frozen_context.get("brief")
    assert type(frozen_brief) is dict
    primary_keyword = frozen_brief.get("primary_keyword")
    assert type(primary_keyword) is str

    payloads: tuple[JsonValue, ...] = (
        {"sections": ["scope", "proof", "cta"]},
        {"content_markdown": "bounded draft"},
        {"issues": ["keep evidence exact"], "decision": "revise"},
        _revision_payload(primary_keyword),
    )
    for sequence, payload in enumerate(payloads):
        completion_path = tmp_path / f"completion-{sequence}.json"
        completion_envelope: dict[str, JsonValue] = {
            "company_id": flow.company_id,
            "job_id": flow.job_id,
            "stage_id": output["stage_id"],
            "input_hash": output["input_hash"],
            "payload": payload,
        }
        completion_path.write_bytes(canonical_json(completion_envelope))
        completion_path.chmod(0o600)
        cli.main(
            [
                "supervised-bind",
                *shared,
                "--completion-file",
                str(completion_path),
                "--operator-id",
                "operator-cli",
                "--session-ref",
                "visible-session-designated",
                "--provider-id",
                "openai-codex",
                "--model-id",
                "gpt-5.6-terra",
            ]
        )
        output = _captured_cli_json(capsys)
        if sequence < 3:
            packet_value = output
            assert packet_value["stage_id"] == ("draft", "critic", "revision")[sequence]
        else:
            assert output == {"status": "FINAL_QA_READY"}

    cli.main(["supervised-status", *shared])
    assert _captured_cli_json(capsys) == {"status": "FINAL_QA_READY"}



def test_supervised_finalize_cli_freezes_artifact(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    settings, flow, connection, service, store, rail = _prepared_final_qa(tmp_path)
    try:
        cli._run_supervised_command(
            settings,
            command="supervised-finalize",
            arguments=Namespace(company_id=flow.company_id, job_id=flow.job_id),
        )
        output = _captured_cli_json(capsys)
        assert output["status"] == "ARTIFACT_FROZEN"
        manifest = output["manifest"]
        assert manifest["company_id"] == flow.company_id
        assert manifest["job_id"] == flow.job_id
        assert service.get_job(flow.job_id).state is JobState.SUCCEEDED
        assert rail.status(company_id=flow.company_id, job_id=flow.job_id).value == "ARTIFACT_FROZEN"
        assert store.manifest_path_for_job(flow.company_id, flow.job_id).is_file()
    finally:
        connection.close()
