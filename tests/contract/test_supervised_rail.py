"""Contracts for the local-only supervised subscription rail."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from seo_orchestrator.canonical import JsonValue, canonical_json, sha256_fingerprint
from seo_orchestrator.domain import ExecutionSnapshot, JobState, SeoJob
from seo_orchestrator.errors import DataIntegrityError
from seo_orchestrator.supervised_rail import (
    ObservedCompletion,
    OperatorAttestation,
    StagePacket,
    SupervisedRail,
    SupervisedStatus,
    build_stage_packet,
    packet_identity_mapping,
)

NOW = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
EVIDENCE_SOURCE: dict[str, JsonValue] = {
    "url": "https://example.test/evidence",
    "content_hash": "e" * 64,
    "fetched_at": NOW.isoformat(),
}
CONTEXT: JsonValue = {
    "brief": {
        "primary_keyword": "покраска авто",
        "goal": "подготовить проверяемый материал",
        "page_structure": ["Введение", "Контроль качества"],
    },
    "evidence": {"sources": [EVIDENCE_SOURCE]},
}


def _snapshot() -> ExecutionSnapshot:
    return ExecutionSnapshot(
        snapshot_id="snapshot-1",
        brief_id="brief-1",
        company_id="company-1",
        company_profile_version=1,
        direction_id="direction-1",
        direction_version=1,
        audience_segment_id="audience-1",
        audience_version=1,
        prompt_set_version=1,
        compiled_context=CONTEXT,
        snapshot_hash=sha256_fingerprint(CONTEXT),
        created_at=NOW,
    )


def _job(snapshot: ExecutionSnapshot) -> SeoJob:
    return SeoJob(
        job_id="job-1",
        brief_id=snapshot.brief_id,
        brief_fingerprint="b" * 64,
        snapshot_id=snapshot.snapshot_id,
        snapshot_hash=snapshot.snapshot_hash,
        company_id=snapshot.company_id,
        direction_id=snapshot.direction_id,
        audience_segment_id=snapshot.audience_segment_id,
        state=JobState.RUNNING,
        current_stage=None,
        approved_plan_fingerprint="a" * 64,
        approval_record_id="approval-1",
        attempt=0,
        created_at=NOW,
        started_at=NOW,
        finished_at=None,
        error_code=None,
        error_summary=None,
        artifact_manifest_path=None,
        company_profile_version=snapshot.company_profile_version,
        direction_version=snapshot.direction_version,
        audience_version=snapshot.audience_version,
        prompt_set_version=snapshot.prompt_set_version,
    )


def test_private_rail_creates_single_link_owner_only_database(tmp_path: Path) -> None:
    state_path = tmp_path / "private" / "supervised.sqlite"

    SupervisedRail(state_path=state_path)

    parent = os.lstat(state_path.parent)
    database = os.lstat(state_path)
    key_path = state_path.with_name(f".{state_path.name}.integrity-key")
    integrity_key = os.lstat(key_path)
    assert stat.S_ISDIR(parent.st_mode)
    assert stat.S_IMODE(parent.st_mode) == 0o700
    assert stat.S_ISREG(database.st_mode)
    assert stat.S_IMODE(database.st_mode) == 0o600
    assert database.st_uid == os.getuid()
    assert database.st_nlink == 1
    assert stat.S_ISREG(integrity_key.st_mode)
    assert stat.S_IMODE(integrity_key.st_mode) == 0o600
    assert integrity_key.st_uid == os.getuid()
    assert integrity_key.st_nlink == 1
    assert integrity_key.st_size == 32


def test_integrity_key_replacement_and_hardlink_fail_closed(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "private" / "supervised.sqlite"
    rail = SupervisedRail(state_path=state_path)
    rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    key_path = state_path.with_name(f".{state_path.name}.integrity-key")
    key_alias = state_path.parent / "integrity-key-alias"
    os.link(key_path, key_alias)

    with pytest.raises(ValueError, match="private and single-link"):
        rail.status(company_id=job.company_id, job_id=job.job_id)
    key_alias.unlink()
    key_path.write_bytes(b"x" * 32)
    key_path.chmod(0o600)

    with pytest.raises(DataIntegrityError):
        rail.status(company_id=job.company_id, job_id=job.job_id)
    with pytest.raises(DataIntegrityError):
        SupervisedRail(state_path=state_path)


def test_integrity_key_commitment_is_immutable(tmp_path: Path) -> None:
    state_path = tmp_path / "private" / "supervised.sqlite"
    SupervisedRail(state_path=state_path)

    with sqlite3.connect(state_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="metadata is immutable"):
            connection.execute(
                "UPDATE supervised_ledger_metadata SET integrity_key_hash=? WHERE singleton=1",
                ("f" * 64,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="metadata is immutable"):
            connection.execute("DELETE FROM supervised_ledger_metadata WHERE singleton=1")


def test_private_rail_rejects_symlink_and_hardlink_state_paths(tmp_path: Path) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    target = private / "target.sqlite"
    target.touch(mode=0o600)
    symlink_path = private / "symlink.sqlite"
    symlink_path.symlink_to(target)
    with pytest.raises(ValueError, match="aliases"):
        SupervisedRail(state_path=symlink_path)

    state_path = private / "state.sqlite"
    SupervisedRail(state_path=state_path)
    os.link(state_path, private / "alias.sqlite")
    with pytest.raises(ValueError, match="single-link"):
        SupervisedRail(state_path=state_path)


def test_private_rail_rejects_hardlink_added_after_construction(tmp_path: Path) -> None:
    state_path = tmp_path / "private" / "state.sqlite"
    rail = SupervisedRail(state_path=state_path)
    os.link(state_path, state_path.parent / "alias.sqlite")

    with pytest.raises(ValueError, match="identity or private mode changed"):
        rail.status(company_id="company-1", job_id="job-1")


def test_packet_is_identical_for_the_same_frozen_job_and_stage() -> None:
    snapshot = _snapshot()
    job = _job(snapshot)

    packet_one = build_stage_packet(
        job,
        snapshot,
        stage_id="outline",
        sequence=0,
        designated_session_ref="session-1",
    )
    packet_two = build_stage_packet(
        job,
        snapshot,
        stage_id="outline",
        sequence=0,
        designated_session_ref="session-1",
    )

    assert packet_one == packet_two
    assert packet_one.input_hash == sha256_fingerprint(packet_identity_mapping(packet_one))
    assert packet_one.provider_id == "openai-codex"
    assert packet_one.model_id == "gpt-5.6-terra"


def test_packet_exposes_separate_frozen_evidence_commitment() -> None:
    context: JsonValue = {
        "brief": {
            "primary_keyword": "покраска авто",
            "goal": "подготовить проверяемый материал",
            "page_structure": ["Введение", "Контроль качества"],
        },
        "evidence": {
            "sources": [
                {
                    "url": "https://example.test/evidence",
                    "content_hash": "e" * 64,
                    "fetched_at": NOW.isoformat(),
                }
            ]
        },
    }
    snapshot = _snapshot().model_copy(
        update={
            "compiled_context": context,
            "snapshot_hash": sha256_fingerprint(context),
        }
    )
    packet = build_stage_packet(
        _job(snapshot),
        snapshot,
        stage_id="outline",
        sequence=0,
        designated_session_ref="session-1",
    )

    assert packet.evidence_hash == sha256_fingerprint(context["evidence"])
    assert packet_identity_mapping(packet)["evidence_hash"] == packet.evidence_hash


def test_packet_rejects_unsupported_stage_and_mismatched_snapshot() -> None:
    snapshot = _snapshot()
    job = _job(snapshot)

    with pytest.raises(ValueError, match="stage_id"):
        build_stage_packet(
            job,
            snapshot,
            stage_id="publish",
            sequence=0,
            designated_session_ref="session-1",
        )
    with pytest.raises(ValueError, match="frozen snapshot"):
        build_stage_packet(
            job,
            snapshot.model_copy(update={"snapshot_id": "snapshot-2"}),
            stage_id="outline",
            sequence=0,
            designated_session_ref="session-1",
        )


def test_packet_rejects_missing_approval_binding() -> None:
    snapshot = _snapshot()
    job = replace(_job(snapshot), approval_record_id=None)

    with pytest.raises(ValueError, match="approval_record_id"):
        build_stage_packet(
            job,
            snapshot,
            stage_id="outline",
            sequence=0,
            designated_session_ref="session-1",
        )


def test_observed_completion_rejects_malformed_attestation() -> None:
    with pytest.raises(ValueError, match="session_ref"):
        OperatorAttestation(
            session_ref=" ",
            provider_id="openai-codex",
            model_id="gpt-5.6-terra",
            operator_id="operator-1",
            observed_at=NOW,
        )

    with pytest.raises(ValueError, match="input_hash"):
        ObservedCompletion(
            job_id="job-1",
            company_id="company-1",
            stage_id="outline",
            input_hash="not-a-hash",
            payload={"outline": "ok"},
            attestation=OperatorAttestation(
                session_ref="session-1",
                provider_id="openai-codex",
                model_id="gpt-5.6-terra",
                operator_id="operator-1",
                observed_at=NOW,
            ),
        )


def test_private_ledger_recovers_the_same_outstanding_packet_after_reopen(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "supervised.sqlite"

    first_packet = SupervisedRail(state_path=state_path).prepare_packet(
        job, snapshot, designated_session_ref="session-1"
    )
    reopened = SupervisedRail(state_path=state_path)

    assert (
        reopened.status(company_id=job.company_id, job_id=job.job_id)
        is SupervisedStatus.AWAITING_OPERATOR_EXECUTION
    )
    assert reopened.outstanding_packet(company_id=job.company_id, job_id=job.job_id) == first_packet


def _valid_stage_payload(stage_id: str) -> JsonValue:
    if stage_id == "outline":
        return {"sections": ["scope", "proof", "decision"]}
    if stage_id == "draft":
        return {"content_markdown": "# Bounded draft\n\nEvidence-led copy."}
    if stage_id == "critic":
        return {"issues": [], "decision": "revise"}
    if stage_id == "revision":
        return {
            "content_markdown": "# Final revision\n\nEvidence-led result. [S1]",
            "titles": [f"Title {index}" for index in range(5)],
            "descriptions": [f"Description {index}" for index in range(5)],
            "sources": [EVIDENCE_SOURCE],
            "warnings": [],
        }
    raise AssertionError("unexpected stage")


def _completion(
    packet: StagePacket,
    *,
    payload: JsonValue | None = None,
    session_ref: str = "session-1",
) -> ObservedCompletion:
    assert hasattr(packet, "job_id")
    assert hasattr(packet, "company_id")
    assert hasattr(packet, "stage_id")
    assert hasattr(packet, "input_hash")
    return ObservedCompletion(
        job_id=packet.job_id,
        company_id=packet.company_id,
        stage_id=packet.stage_id,
        input_hash=packet.input_hash,
        payload=payload if payload is not None else _valid_stage_payload(packet.stage_id),
        attestation=OperatorAttestation(
            session_ref=session_ref,
            provider_id="openai-codex",
            model_id="gpt-5.6-terra",
            operator_id="operator-1",
            observed_at=NOW,
        ),
    )


def test_completion_requires_designated_session_and_frozen_provider_model(
    tmp_path: Path,
) -> None:
    rail = SupervisedRail(state_path=tmp_path / "private" / "supervised.sqlite")
    snapshot = _snapshot()
    job = _job(snapshot)
    packet = rail.prepare_packet(
        job,
        snapshot,
        designated_session_ref="visible-session-designated",
    )

    def completion(*, session_ref: str, provider_id: str, model_id: str) -> ObservedCompletion:
        return ObservedCompletion(
            job_id=packet.job_id,
            company_id=packet.company_id,
            stage_id=packet.stage_id,
            input_hash=packet.input_hash,
            payload=_valid_stage_payload(packet.stage_id),
            attestation=OperatorAttestation(
                session_ref=session_ref,
                provider_id=provider_id,
                model_id=model_id,
                operator_id="operator-1",
                observed_at=NOW,
            ),
        )

    for candidate in (
        completion(
            session_ref="visible-session-wrong",
            provider_id="openai-codex",
            model_id="gpt-5.6-terra",
        ),
        completion(
            session_ref="visible-session-designated",
            provider_id="wrong-provider",
            model_id="gpt-5.6-terra",
        ),
        completion(
            session_ref="visible-session-designated",
            provider_id="openai-codex",
            model_id="wrong-model",
        ),
    ):
        with pytest.raises(ValueError, match="attestation"):
            rail.bind_completion(candidate)

    accepted = rail.bind_completion(
        completion(
            session_ref="visible-session-designated",
            provider_id="openai-codex",
            model_id="gpt-5.6-terra",
        )
    )
    assert isinstance(accepted, StagePacket)


def test_rail_rejects_second_completion_for_the_same_packet(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    rail = SupervisedRail(state_path=tmp_path / "rail.sqlite")
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")

    accepted = rail.bind_completion(_completion(packet))

    assert isinstance(accepted, StagePacket)
    assert accepted.stage_id == "draft"
    with pytest.raises(ValueError, match="already bound"):
        rail.bind_completion(_completion(packet))
    with pytest.raises(ValueError, match="already bound"):
        rail.bind_completion(_completion(packet, payload={"changed": True}))


def test_next_packet_identity_commits_accepted_stage_payload(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    packets: list[StagePacket] = []

    variants: tuple[tuple[str, JsonValue], ...] = (
        ("one", {"sections": ["scope"]}),
        ("two", {"sections": ["scope", "proof"]}),
    )
    for name, payload in variants:
        rail = SupervisedRail(state_path=tmp_path / name / "rail.sqlite")
        outline = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
        draft = rail.bind_completion(_completion(outline, payload=payload))
        assert isinstance(draft, StagePacket)
        packets.append(draft)

    first, second = packets
    assert first.input_hash != second.input_hash
    assert first.previous_completion_hash != second.previous_completion_hash
    assert "previous-stage=outline\n" in first.prompt
    assert f"previous-completion-hash={first.previous_completion_hash}\n" in first.prompt
    assert 'previous-payload={"sections":["scope"]}\n' in first.prompt


@pytest.mark.parametrize(
    ("stage_id", "first_payload", "second_payload"),
    [
        ("outline", {"sections": ["scope"]}, {"sections": ["scope", "proof"]}),
        (
            "draft",
            {"content_markdown": "First bounded draft."},
            {"content_markdown": "Second bounded draft."},
        ),
        (
            "critic",
            {"issues": [], "decision": "revise"},
            {"issues": ["Add source proof."], "decision": "revise"},
        ),
    ],
)
def test_every_preceding_completion_changes_next_packet_after_restart(
    tmp_path: Path,
    stage_id: str,
    first_payload: JsonValue,
    second_payload: JsonValue,
) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    next_packets: list[StagePacket] = []
    for name, payload in (("first", first_payload), ("second", second_payload)):
        state_path = tmp_path / name / "rail.sqlite"
        rail = SupervisedRail(state_path=state_path)
        packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
        while packet.stage_id != stage_id:
            outcome = rail.bind_completion(_completion(packet))
            assert isinstance(outcome, StagePacket)
            packet = outcome
        next_packet = rail.bind_completion(_completion(packet, payload=payload))
        assert isinstance(next_packet, StagePacket)
        reopened = SupervisedRail(state_path=state_path)
        assert (
            reopened.outstanding_packet(
                company_id=job.company_id,
                job_id=job.job_id,
            )
            == next_packet
        )
        next_packets.append(next_packet)

    assert next_packets[0].input_hash != next_packets[1].input_hash
    assert next_packets[0].previous_completion_hash != next_packets[1].previous_completion_hash


@pytest.mark.parametrize(
    ("stage_id", "malformed_payload"),
    [
        ("outline", {"sections": []}),
        ("draft", {"draft": "wrong field"}),
        ("critic", {"issues": [], "decision": "accept"}),
        (
            "revision",
            {
                "content_markdown": "missing exact final fields",
                "titles": [],
            },
        ),
    ],
)
def test_malformed_stage_payload_cannot_be_persisted_or_advance(
    tmp_path: Path,
    stage_id: str,
    malformed_payload: JsonValue,
) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / stage_id / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    while packet.stage_id != stage_id:
        outcome = rail.bind_completion(_completion(packet))
        assert isinstance(outcome, StagePacket)
        packet = outcome

    with sqlite3.connect(state_path) as connection:
        count_before = connection.execute("SELECT COUNT(*) FROM supervised_completions").fetchone()
    with pytest.raises(ValueError, match="stage payload"):
        rail.bind_completion(_completion(packet, payload=malformed_payload))
    with sqlite3.connect(state_path) as connection:
        count_after = connection.execute("SELECT COUNT(*) FROM supervised_completions").fetchone()

    assert count_after == count_before
    assert rail.outstanding_packet(company_id=job.company_id, job_id=job.job_id) == packet


def test_revision_source_is_validated_against_frozen_evidence_before_persistence(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "rail.sqlite"
    snapshot = _snapshot()
    job = _job(snapshot)
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    while packet.stage_id != "revision":
        outcome = rail.bind_completion(_completion(packet))
        assert isinstance(outcome, StagePacket)
        packet = outcome
    payload = _valid_stage_payload("revision")
    assert type(payload) is dict
    payload["sources"] = [
        {
            "url": "https://example.test/not-frozen",
            "content_hash": "f" * 64,
            "fetched_at": NOW.isoformat(),
        }
    ]
    with sqlite3.connect(state_path) as connection:
        count_before = connection.execute("SELECT COUNT(*) FROM supervised_completions").fetchone()

    with pytest.raises(ValueError, match="frozen evidence"):
        rail.bind_completion(_completion(packet, payload=payload))

    with sqlite3.connect(state_path) as connection:
        count_after = connection.execute("SELECT COUNT(*) FROM supervised_completions").fetchone()
    assert count_after == count_before
    assert rail.outstanding_packet(company_id=job.company_id, job_id=job.job_id) == packet


def test_rejected_completion_has_durable_payload_free_state_evidence(tmp_path: Path) -> None:
    state_path = tmp_path / "rail.sqlite"
    snapshot = _snapshot()
    job = _job(snapshot)
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    secret_text = "api_key=abcdef123456"

    with pytest.raises(ValueError, match="credential"):
        rail.bind_completion(_completion(packet, payload={"sections": [secret_text]}))

    with sqlite3.connect(state_path) as connection:
        completions = connection.execute("SELECT COUNT(*) FROM supervised_completions").fetchone()
        events = connection.execute(
            "SELECT event_type, event_json FROM supervised_events "
            "WHERE company_id=? AND job_id=? ORDER BY event_id DESC LIMIT 2",
            (job.company_id, job.job_id),
        ).fetchall()
    assert completions == (0,)
    assert [event[0] for event in reversed(events)] == [
        "COMPLETION_SUBMITTED",
        "COMPLETION_REJECTED",
    ]
    serialized_events = "".join(event[1] for event in events)
    assert secret_text not in serialized_events
    assert '"payload"' not in serialized_events
    assert "PAYLOAD_INVALID" in serialized_events
    assert (
        rail.status(company_id=job.company_id, job_id=job.job_id)
        is SupervisedStatus.AWAITING_OPERATOR_EXECUTION
    )


@pytest.mark.parametrize(
    ("stage_id", "sensitive_payload"),
    [
        ("outline", {"sections": ["api_key=abcdef123456"]}),
        ("draft", {"content_markdown": "password=abcdef123456"}),
        (
            "critic",
            {"issues": ["<analysis>hidden text</analysis>"], "decision": "revise"},
        ),
        (
            "revision",
            {
                "content_markdown": "secret=abcdef123456",
                "titles": ["one", "two", "three", "four", "five"],
                "descriptions": ["one", "two", "three", "four", "five"],
                "sources": [],
                "warnings": [],
            },
        ),
    ],
)
def test_sensitive_stage_payload_cannot_be_persisted_or_advance(
    tmp_path: Path,
    stage_id: str,
    sensitive_payload: JsonValue,
) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    rail = SupervisedRail(state_path=tmp_path / "rail.sqlite")
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    while packet.stage_id != stage_id:
        outcome = rail.bind_completion(_completion(packet))
        assert isinstance(outcome, StagePacket)
        packet = outcome

    with pytest.raises(
        ValueError,
        match="credential|hidden reasoning|forbidden",
    ):
        rail.bind_completion(_completion(packet, payload=sensitive_payload))

    assert rail.outstanding_packet(company_id=job.company_id, job_id=job.job_id) == packet


def test_completion_row_requires_an_existing_packet(tmp_path: Path) -> None:
    state_path = tmp_path / "rail.sqlite"
    SupervisedRail(state_path=state_path)

    with sqlite3.connect(state_path) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            connection.execute(
                "INSERT INTO supervised_completions"
                "(company_id, job_id, input_hash, completion_hash, completion_json) "
                "VALUES (?, ?, ?, ?, ?)",
                ("company-1", "job-1", "f" * 64, "e" * 64, "{}"),
            )


def test_binding_rejects_cross_company_completion_without_writing(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    completion = replace(_completion(packet), company_id="company-2")

    with pytest.raises(LookupError, match="not found"):
        rail.bind_completion(completion)

    with sqlite3.connect(state_path) as connection:
        count = connection.execute("SELECT COUNT(*) FROM supervised_completions").fetchone()
    assert count == (0,)


def test_accepted_completion_stores_its_canonical_commitment(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    rail.bind_completion(_completion(packet))

    with sqlite3.connect(state_path) as connection:
        row = connection.execute(
            "SELECT completion_json, completion_hash FROM supervised_completions "
            "WHERE input_hash=?",
            (packet.input_hash,),
        ).fetchone()

    assert row is not None
    completion_value = json.loads(row[0])
    assert row[1] == sha256_fingerprint(completion_value)


def test_ledger_events_are_append_only_and_cover_packet_binding(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    rail.bind_completion(_completion(packet))

    with sqlite3.connect(state_path) as connection:
        event_types = connection.execute(
            "SELECT event_type FROM supervised_events ORDER BY event_id"
        ).fetchall()
        assert event_types == [
            ("PACKET_PREPARED",),
            ("COMPLETION_SUBMITTED",),
            ("COMPLETION_ACCEPTED",),
            ("PACKET_PREPARED",),
        ]
        with pytest.raises(sqlite3.IntegrityError, match="event is immutable"):
            connection.execute(
                "UPDATE supervised_events SET event_type='TAMPERED' WHERE event_id=1"
            )
        with pytest.raises(sqlite3.IntegrityError, match="event is immutable"):
            connection.execute("DELETE FROM supervised_events WHERE event_id=1")


def test_accepted_completion_rejects_update_and_delete_tampering(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    rail.bind_completion(_completion(packet))

    with sqlite3.connect(state_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="completion is immutable"):
            connection.execute(
                "UPDATE supervised_completions SET completion_json='{}' WHERE input_hash=?",
                (packet.input_hash,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="completion is immutable"):
            connection.execute(
                "DELETE FROM supervised_completions WHERE input_hash=?",
                (packet.input_hash,),
            )


def test_completion_advances_only_through_fixed_four_stage_order(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")

    for sequence, expected_stage in enumerate(("draft", "critic", "revision"), start=1):
        outcome = rail.bind_completion(_completion(packet))
        assert isinstance(outcome, StagePacket)
        assert outcome.stage_id == expected_stage
        assert outcome.sequence == sequence
        packet = outcome

    terminal = rail.bind_completion(_completion(packet))

    assert terminal is SupervisedStatus.FINAL_QA_READY
    assert rail.status(company_id=job.company_id, job_id=job.job_id) is terminal
    with pytest.raises(LookupError, match="no outstanding packet"):
        rail.outstanding_packet(company_id=job.company_id, job_id=job.job_id)
    with pytest.raises(ValueError, match="FINAL_QA_READY"):
        rail.prepare_packet(job, snapshot, designated_session_ref="session-1")

    with sqlite3.connect(state_path) as connection:
        counts = connection.execute(
            "SELECT "
            "(SELECT COUNT(*) FROM supervised_packets), "
            "(SELECT COUNT(*) FROM supervised_completions)"
        ).fetchone()
        final_event = connection.execute(
            "SELECT event_type FROM supervised_events ORDER BY event_id DESC LIMIT 1"
        ).fetchone()
    assert counts == (4, 4)
    assert final_event == ("FINAL_QA_READY",)


def test_authenticated_completion_binding_blocks_materialized_row_replacement(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    while packet.stage_id != "revision":
        outcome = rail.bind_completion(_completion(packet))
        assert isinstance(outcome, StagePacket)
        packet = outcome
    accepted_revision = _completion(packet)
    assert rail.bind_completion(accepted_revision) is SupervisedStatus.FINAL_QA_READY

    altered_payload = cast(dict[str, JsonValue], _valid_stage_payload("revision"))
    altered_payload = dict(altered_payload)
    altered_payload["titles"] = ["CHAIN BYPASS TITLE", *[f"Title {index}" for index in range(1, 5)]]
    altered_completion = replace(accepted_revision, payload=altered_payload)
    altered_value: dict[str, JsonValue] = {
        "job_id": altered_completion.job_id,
        "company_id": altered_completion.company_id,
        "stage_id": altered_completion.stage_id,
        "input_hash": altered_completion.input_hash,
        "payload": altered_completion.payload,
        "attestation": {
            "session_ref": altered_completion.attestation.session_ref,
            "provider_id": altered_completion.attestation.provider_id,
            "model_id": altered_completion.attestation.model_id,
            "operator_id": altered_completion.attestation.operator_id,
            "observed_at": altered_completion.attestation.observed_at.isoformat(),
        },
    }
    altered_text = canonical_json(altered_value).decode("utf-8")
    altered_hash = sha256_fingerprint(altered_value)
    with sqlite3.connect(state_path) as connection:
        connection.execute("DROP TRIGGER supervised_completions_no_update")
        connection.execute(
            "UPDATE supervised_completions SET completion_json=?, completion_hash=? "
            "WHERE company_id=? AND job_id=? AND input_hash=?",
            (
                altered_text,
                altered_hash,
                job.company_id,
                job.job_id,
                packet.input_hash,
            ),
        )

    with pytest.raises(DataIntegrityError):
        rail.status(company_id=job.company_id, job_id=job.job_id)
    with pytest.raises(DataIntegrityError):
        rail.final_completion(company_id=job.company_id, job_id=job.job_id)


def test_authenticated_packet_binding_blocks_materialized_row_replacement(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    with sqlite3.connect(state_path) as connection:
        connection.execute("DROP TRIGGER supervised_packets_no_update")
        connection.execute(
            "UPDATE supervised_packets SET packet_json='{}' WHERE input_hash=?",
            (packet.input_hash,),
        )

    with pytest.raises(DataIntegrityError):
        rail.status(company_id=job.company_id, job_id=job.job_id)


def test_authenticated_context_binding_blocks_materialized_row_replacement(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    altered_context: JsonValue = {"brief": {"goal": "altered"}, "evidence": {"sources": []}}
    with sqlite3.connect(state_path) as connection:
        connection.execute("DROP TRIGGER supervised_jobs_binding_no_update")
        connection.execute(
            "UPDATE supervised_jobs SET context_json=?, snapshot_hash=? "
            "WHERE company_id=? AND job_id=?",
            (
                canonical_json(altered_context).decode("utf-8"),
                sha256_fingerprint(altered_context),
                job.company_id,
                job.job_id,
            ),
        )

    with pytest.raises(DataIntegrityError):
        rail.frozen_context(company_id=job.company_id, job_id=job.job_id)


def test_authenticated_artifact_binding_blocks_projection_replacement(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    for _ in range(4):
        outcome = rail.bind_completion(_completion(packet))
        if isinstance(outcome, StagePacket):
            packet = outcome
    rail.record_artifact(
        company_id=job.company_id,
        job_id=job.job_id,
        manifest_path="/private/manifest.json",
        manifest_hash="a" * 64,
    )
    with sqlite3.connect(state_path) as connection:
        connection.execute("DROP TRIGGER supervised_jobs_artifact_no_rebind")
        connection.execute(
            "UPDATE supervised_jobs SET artifact_manifest_path=?, artifact_manifest_hash=? "
            "WHERE company_id=? AND job_id=?",
            ("/private/altered.json", "b" * 64, job.company_id, job.job_id),
        )

    with pytest.raises(DataIntegrityError):
        rail.artifact_binding(company_id=job.company_id, job_id=job.job_id)


def test_unknown_operator_execution_blocks_prepare_and_bind_after_restart(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")

    status = rail.mark_operator_recovery_required(
        company_id=job.company_id,
        job_id=job.job_id,
    )
    reopened = SupervisedRail(state_path=state_path)

    assert status is SupervisedStatus.OPERATOR_RECOVERY_REQUIRED
    assert reopened.status(company_id=job.company_id, job_id=job.job_id) is status
    with pytest.raises(ValueError, match="OPERATOR_RECOVERY_REQUIRED"):
        reopened.prepare_packet(job, snapshot, designated_session_ref="session-1")
    with pytest.raises(ValueError, match="OPERATOR_RECOVERY_REQUIRED"):
        reopened.bind_completion(_completion(packet))
    with pytest.raises(LookupError, match="no outstanding packet"):
        reopened.outstanding_packet(company_id=job.company_id, job_id=job.job_id)


@pytest.mark.parametrize("blocked_state", ["recovery", "cancel"])
def test_status_projection_cannot_bypass_recovery_or_cancel_by_direct_update(
    tmp_path: Path,
    blocked_state: str,
) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    if blocked_state == "recovery":
        rail.mark_operator_recovery_required(company_id=job.company_id, job_id=job.job_id)
    else:
        rail.request_cancel(
            company_id=job.company_id,
            job_id=job.job_id,
            operator_id="operator-1",
        )

    with sqlite3.connect(state_path) as connection:
        connection.execute(
            "UPDATE supervised_jobs SET status=? WHERE company_id=? AND job_id=?",
            (
                SupervisedStatus.AWAITING_OPERATOR_EXECUTION.value,
                job.company_id,
                job.job_id,
            ),
        )

    with pytest.raises(DataIntegrityError):
        rail.status(company_id=job.company_id, job_id=job.job_id)
    with pytest.raises(DataIntegrityError):
        rail.bind_completion(_completion(packet))


@pytest.mark.parametrize("blocked_state", ["recovery", "cancel"])
def test_forged_event_append_cannot_reopen_recovery_or_cancel(
    tmp_path: Path,
    blocked_state: str,
) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    if blocked_state == "recovery":
        rail.mark_operator_recovery_required(company_id=job.company_id, job_id=job.job_id)
    else:
        rail.request_cancel(
            company_id=job.company_id,
            job_id=job.job_id,
            operator_id="operator-1",
        )

    with sqlite3.connect(state_path) as connection:
        head = connection.execute(
            "SELECT event_count, event_head_hash FROM supervised_jobs "
            "WHERE company_id=? AND job_id=?",
            (job.company_id, job.job_id),
        ).fetchone()
        assert head is not None
        event_sequence = head[0]
        predecessor_hash = head[1]
        recorded_at = NOW.isoformat()
        forged_event: dict[str, JsonValue] = {
            "company_id": job.company_id,
            "job_id": job.job_id,
            "event_sequence": event_sequence,
            "predecessor_hash": predecessor_hash,
            "event_type": "RECOVERY_RESOLVED_FOR_EXACT_BINDING",
            "recorded_at": recorded_at,
            "details": {
                "operator_id": "forged-operator",
                "outstanding_input_hash": packet.input_hash,
                "replacement_attempt": False,
                "maximum_retries": 0,
            },
        }
        forged_event_hash = sha256_fingerprint(forged_event)
        forged_event_mac = "f" * 64
        connection.create_function(
            "supervised_event_mac",
            1,
            lambda _event_text: forged_event_mac,
            deterministic=True,
        )
        connection.execute(
            "INSERT INTO supervised_events"
            "(company_id, job_id, event_sequence, predecessor_hash, event_type, "
            "recorded_at, event_hash, event_mac, event_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                job.company_id,
                job.job_id,
                event_sequence,
                predecessor_hash,
                forged_event["event_type"],
                recorded_at,
                forged_event_hash,
                forged_event_mac,
                canonical_json(forged_event).decode("utf-8"),
            ),
        )
        connection.execute(
            "UPDATE supervised_jobs SET status=?, event_count=?, event_head_hash=?, "
            "event_head_mac=? WHERE company_id=? AND job_id=?",
            (
                SupervisedStatus.AWAITING_OPERATOR_EXECUTION.value,
                event_sequence + 1,
                forged_event_hash,
                forged_event_mac,
                job.company_id,
                job.job_id,
            ),
        )

    with pytest.raises(DataIntegrityError):
        rail.status(company_id=job.company_id, job_id=job.job_id)
    with pytest.raises(DataIntegrityError):
        rail.bind_completion(_completion(packet))


def test_operator_recovery_can_resolve_for_exact_original_completion(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    rail.mark_operator_recovery_required(company_id=job.company_id, job_id=job.job_id)
    reopened = SupervisedRail(state_path=state_path)

    with pytest.raises(ValueError, match="outstanding packet"):
        reopened.resolve_recovery_for_exact_binding(
            company_id=job.company_id,
            job_id=job.job_id,
            expected_input_hash="0" * 64,
            operator_id="operator-1",
        )
    assert (
        reopened.status(company_id=job.company_id, job_id=job.job_id)
        is SupervisedStatus.OPERATOR_RECOVERY_REQUIRED
    )

    status = reopened.resolve_recovery_for_exact_binding(
        company_id=job.company_id,
        job_id=job.job_id,
        expected_input_hash=packet.input_hash,
        operator_id="operator-1",
    )
    assert status is SupervisedStatus.AWAITING_OPERATOR_EXECUTION
    with pytest.raises(ValueError, match="operator identity"):
        reopened.resolve_recovery_for_exact_binding(
            company_id=job.company_id,
            job_id=job.job_id,
            expected_input_hash=packet.input_hash,
            operator_id="operator-2",
        )
    replayed_status = reopened.resolve_recovery_for_exact_binding(
        company_id=job.company_id,
        job_id=job.job_id,
        expected_input_hash=packet.input_hash,
        operator_id="operator-1",
    )
    assert replayed_status is SupervisedStatus.AWAITING_OPERATOR_EXECUTION
    assert reopened.outstanding_packet(company_id=job.company_id, job_id=job.job_id) == packet
    next_packet = reopened.bind_completion(_completion(packet))
    assert isinstance(next_packet, StagePacket)
    with sqlite3.connect(state_path) as connection:
        event = connection.execute(
            "SELECT event_type, event_json FROM supervised_events "
            "WHERE company_id=? AND job_id=? "
            "AND event_type='RECOVERY_RESOLVED_FOR_EXACT_BINDING' "
            "ORDER BY event_id DESC LIMIT 1",
            (job.company_id, job.job_id),
        ).fetchone()
    assert event is not None
    assert event[0] == "RECOVERY_RESOLVED_FOR_EXACT_BINDING"
    assert json.loads(event[1])["details"]["outstanding_input_hash"] == packet.input_hash


def test_recovery_replay_rejects_self_consistent_semantic_event_tamper(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    rail.mark_operator_recovery_required(company_id=job.company_id, job_id=job.job_id)
    rail.resolve_recovery_for_exact_binding(
        company_id=job.company_id,
        job_id=job.job_id,
        expected_input_hash=packet.input_hash,
        operator_id="operator-1",
    )

    with sqlite3.connect(state_path) as connection:
        row = connection.execute(
            "SELECT event_json FROM supervised_events "
            "WHERE event_type='RECOVERY_RESOLVED_FOR_EXACT_BINDING'"
        ).fetchone()
        assert row is not None
        event = json.loads(row[0])
        event["details"]["replacement_attempt"] = True
        connection.execute("DROP TRIGGER supervised_events_no_update")
        connection.execute(
            "UPDATE supervised_events SET event_json=?, event_hash=? "
            "WHERE event_type='RECOVERY_RESOLVED_FOR_EXACT_BINDING'",
            (canonical_json(event).decode("utf-8"), sha256_fingerprint(event)),
        )

    with pytest.raises(DataIntegrityError):
        rail.resolve_recovery_for_exact_binding(
            company_id=job.company_id,
            job_id=job.job_id,
            expected_input_hash=packet.input_hash,
            operator_id="operator-1",
        )


def test_operator_recovery_can_be_resolved_by_explicit_local_cancel(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    rail.mark_operator_recovery_required(
        company_id=job.company_id,
        job_id=job.job_id,
    )

    status = rail.request_cancel(
        company_id=job.company_id,
        job_id=job.job_id,
        operator_id="operator-1",
    )

    assert status is SupervisedStatus.CANCEL_REQUESTED
    assert rail.status(company_id=job.company_id, job_id=job.job_id) is status
    with pytest.raises(ValueError, match="CANCEL_REQUESTED"):
        rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    with sqlite3.connect(state_path) as connection:
        events = [
            row[0]
            for row in connection.execute(
                "SELECT event_type FROM supervised_events "
                "WHERE company_id=? AND job_id=? ORDER BY event_id",
                (job.company_id, job.job_id),
            ).fetchall()
        ]
    assert events[-2:] == ["OPERATOR_RECOVERY_REQUIRED", "CANCEL_REQUESTED"]


def test_cancel_request_is_local_persisted_and_never_claims_upstream_cancellation(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")

    status = rail.request_cancel(
        company_id=job.company_id,
        job_id=job.job_id,
        operator_id="operator-1",
    )
    reopened = SupervisedRail(state_path=state_path)

    assert status is SupervisedStatus.CANCEL_REQUESTED
    assert "CANCELED" not in {candidate.value for candidate in SupervisedStatus}
    assert reopened.status(company_id=job.company_id, job_id=job.job_id) is status
    assert (
        reopened.request_cancel(
            company_id=job.company_id,
            job_id=job.job_id,
            operator_id="operator-1",
        )
        is status
    )
    with pytest.raises(ValueError, match="CANCEL_REQUESTED"):
        reopened.prepare_packet(job, snapshot, designated_session_ref="session-1")
    with pytest.raises(ValueError, match="CANCEL_REQUESTED"):
        reopened.bind_completion(_completion(packet))
    with pytest.raises(LookupError, match="no outstanding packet"):
        reopened.outstanding_packet(company_id=job.company_id, job_id=job.job_id)

    with sqlite3.connect(state_path) as connection:
        events = connection.execute(
            "SELECT event_type, event_json FROM supervised_events "
            "WHERE event_type='CANCEL_REQUESTED'"
        ).fetchall()
    assert len(events) == 1
    assert json.loads(events[0][1])["details"]["operator_id"] == "operator-1"


def test_packet_and_completion_advance_roll_back_together_on_head_failure(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    outline = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    with sqlite3.connect(state_path) as connection:
        connection.execute(
            "CREATE TRIGGER injected_head_failure "
            "BEFORE UPDATE OF packet_json ON supervised_jobs "
            "BEGIN SELECT RAISE(ABORT, 'injected head failure'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected head failure"):
        rail.bind_completion(_completion(outline))

    with sqlite3.connect(state_path) as connection:
        counts = connection.execute(
            "SELECT (SELECT COUNT(*) FROM supervised_packets), "
            "(SELECT COUNT(*) FROM supervised_completions), "
            "(SELECT COUNT(*) FROM supervised_events)"
        ).fetchone()
        connection.execute("DROP TRIGGER injected_head_failure")
    assert counts == (1, 0, 1)
    assert rail.outstanding_packet(company_id=job.company_id, job_id=job.job_id) == outline
    assert isinstance(rail.bind_completion(_completion(outline)), StagePacket)


def test_mutable_packet_head_must_reference_next_immutable_packet(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    outline = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")

    with (
        sqlite3.connect(state_path) as connection,
        pytest.raises(sqlite3.IntegrityError, match="packet head"),
    ):
        connection.execute(
            "UPDATE supervised_jobs SET packet_json=? WHERE company_id=? AND job_id=?",
            ('{"tampered":true}', job.company_id, job.job_id),
        )

    draft = rail.bind_completion(_completion(outline))
    assert isinstance(draft, StagePacket)
    with sqlite3.connect(state_path) as connection:
        outline_json = connection.execute(
            "SELECT packet_json FROM supervised_packets "
            "WHERE company_id=? AND job_id=? AND sequence=0",
            (job.company_id, job.job_id),
        ).fetchone()
        assert outline_json is not None
        with pytest.raises(sqlite3.IntegrityError, match="packet head"):
            connection.execute(
                "UPDATE supervised_jobs SET packet_json=? WHERE company_id=? AND job_id=?",
                (outline_json[0], job.company_id, job.job_id),
            )


def test_artifact_pointer_is_write_once_in_sqlite(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    for _ in range(4):
        outcome = rail.bind_completion(_completion(packet))
        if isinstance(outcome, StagePacket):
            packet = outcome
    rail.record_artifact(
        company_id=job.company_id,
        job_id=job.job_id,
        manifest_path="/private/manifest.json",
        manifest_hash="a" * 64,
    )

    with (
        sqlite3.connect(state_path) as connection,
        pytest.raises(sqlite3.IntegrityError, match="artifact binding"),
    ):
        connection.execute(
            "UPDATE supervised_jobs SET artifact_manifest_path=?, "
            "artifact_manifest_hash=? WHERE company_id=? AND job_id=?",
            (
                "/private/other.json",
                "b" * 64,
                job.company_id,
                job.job_id,
            ),
        )


def test_frozen_job_binding_rejects_direct_update_and_delete(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    SupervisedRail(state_path=state_path).prepare_packet(
        job, snapshot, designated_session_ref="session-1"
    )

    with sqlite3.connect(state_path) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        with pytest.raises(sqlite3.IntegrityError, match="binding is immutable"):
            connection.execute(
                "UPDATE supervised_jobs SET snapshot_hash=? WHERE company_id=? AND job_id=?",
                ("b" * 64, job.company_id, job.job_id),
            )
        with pytest.raises(sqlite3.IntegrityError, match="job record is immutable"):
            connection.execute(
                "DELETE FROM supervised_jobs WHERE company_id=? AND job_id=?",
                (job.company_id, job.job_id),
            )


def test_packet_history_rejects_update_and_delete_tampering(tmp_path: Path) -> None:
    snapshot = _snapshot()
    job = _job(snapshot)
    state_path = tmp_path / "rail.sqlite"
    packet = SupervisedRail(state_path=state_path).prepare_packet(
        job, snapshot, designated_session_ref="session-1"
    )

    with sqlite3.connect(state_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="packet is immutable"):
            connection.execute(
                "UPDATE supervised_packets SET packet_json='{}' WHERE input_hash=?",
                (packet.input_hash,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="packet is immutable"):
            connection.execute(
                "DELETE FROM supervised_packets WHERE input_hash=?",
                (packet.input_hash,),
            )

# helpers for terminal-state guard hardening


def _reach_final_qa(
    tmp_path: Path, state_path: Path
) -> tuple[SeoJob, ExecutionSnapshot, SupervisedRail]:
    snapshot = _snapshot()
    job = _job(snapshot)
    rail = SupervisedRail(state_path=state_path)
    packet = rail.prepare_packet(job, snapshot, designated_session_ref="session-1")
    while packet.stage_id != "revision":
        outcome = rail.bind_completion(_completion(packet))
        assert isinstance(outcome, StagePacket)
        packet = outcome
    terminal = rail.bind_completion(_completion(packet))
    assert terminal is SupervisedStatus.FINAL_QA_READY
    return job, snapshot, rail


def _reach_artifact_frozen(
    tmp_path: Path, state_path: Path
) -> tuple[SeoJob, ExecutionSnapshot, SupervisedRail]:
    job, snapshot, rail = _reach_final_qa(tmp_path, state_path)
    binding = rail.record_artifact(
        company_id=job.company_id,
        job_id=job.job_id,
        manifest_path=str(tmp_path / "manifest.json"),
        manifest_hash="a" * 64,
    )
    assert binding.manifest_path == str(tmp_path / "manifest.json")
    assert binding.manifest_hash == "a" * 64
    return job, snapshot, rail


def test_mark_operator_recovery_required_is_rejected_after_final_qa_ready(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "rail.sqlite"
    job, _, rail = _reach_final_qa(tmp_path, state_path)
    assert rail.status(company_id=job.company_id, job_id=job.job_id) is SupervisedStatus.FINAL_QA_READY

    with pytest.raises(ValueError, match="FINAL_QA_READY"):
        rail.mark_operator_recovery_required(
            company_id=job.company_id,
            job_id=job.job_id,
        )

    reopened = SupervisedRail(state_path=state_path)
    with pytest.raises(ValueError, match="FINAL_QA_READY"):
        reopened.mark_operator_recovery_required(
            company_id=job.company_id,
            job_id=job.job_id,
        )

    with sqlite3.connect(state_path) as connection:
        events = connection.execute(
            "SELECT event_type FROM supervised_events "
            "WHERE company_id=? AND job_id=? AND event_type='OPERATOR_RECOVERY_REQUIRED'",
            (job.company_id, job.job_id),
        ).fetchall()
    assert events == []


def test_request_cancel_is_rejected_after_final_qa_ready(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "rail.sqlite"
    job, _, rail = _reach_final_qa(tmp_path, state_path)
    assert rail.status(company_id=job.company_id, job_id=job.job_id) is SupervisedStatus.FINAL_QA_READY

    with pytest.raises(ValueError, match="FINAL_QA_READY"):
        rail.request_cancel(
            company_id=job.company_id,
            job_id=job.job_id,
            operator_id="operator-1",
        )

    reopened = SupervisedRail(state_path=state_path)
    with pytest.raises(ValueError, match="FINAL_QA_READY"):
        reopened.request_cancel(
            company_id=job.company_id,
            job_id=job.job_id,
            operator_id="operator-1",
        )

    with sqlite3.connect(state_path) as connection:
        events = connection.execute(
            "SELECT event_type FROM supervised_events "
            "WHERE company_id=? AND job_id=? AND event_type='CANCEL_REQUESTED'",
            (job.company_id, job.job_id),
        ).fetchall()
    assert events == []


def test_mark_operator_recovery_required_is_rejected_after_artifact_frozen(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "rail.sqlite"
    job, _, rail = _reach_artifact_frozen(tmp_path, state_path)
    assert rail.status(company_id=job.company_id, job_id=job.job_id) is SupervisedStatus.ARTIFACT_FROZEN

    with pytest.raises(ValueError, match="ARTIFACT_FROZEN"):
        rail.mark_operator_recovery_required(
            company_id=job.company_id,
            job_id=job.job_id,
        )

    reopened = SupervisedRail(state_path=state_path)
    with pytest.raises(ValueError, match="ARTIFACT_FROZEN"):
        reopened.mark_operator_recovery_required(
            company_id=job.company_id,
            job_id=job.job_id,
        )

    with sqlite3.connect(state_path) as connection:
        events = connection.execute(
            "SELECT event_type FROM supervised_events "
            "WHERE company_id=? AND job_id=? AND event_type='OPERATOR_RECOVERY_REQUIRED'",
            (job.company_id, job.job_id),
        ).fetchall()
    assert events == []


def test_request_cancel_is_rejected_after_artifact_frozen(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "rail.sqlite"
    job, _, rail = _reach_artifact_frozen(tmp_path, state_path)
    assert rail.status(company_id=job.company_id, job_id=job.job_id) is SupervisedStatus.ARTIFACT_FROZEN

    with pytest.raises(ValueError, match="ARTIFACT_FROZEN"):
        rail.request_cancel(
            company_id=job.company_id,
            job_id=job.job_id,
            operator_id="operator-1",
        )

    reopened = SupervisedRail(state_path=state_path)
    with pytest.raises(ValueError, match="ARTIFACT_FROZEN"):
        reopened.request_cancel(
            company_id=job.company_id,
            job_id=job.job_id,
            operator_id="operator-1",
        )

    with sqlite3.connect(state_path) as connection:
        events = connection.execute(
            "SELECT event_type FROM supervised_events "
            "WHERE company_id=? AND job_id=? AND event_type='CANCEL_REQUESTED'",
            (job.company_id, job.job_id),
        ).fetchall()
    assert events == []