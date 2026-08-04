from __future__ import annotations

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier

import pytest

from seo_orchestrator.canonical import JsonValue, canonical_json, sha256_fingerprint
from seo_orchestrator.db.connection import connect
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.domain import ApprovalRecord, ExecutionPlan, JobState, SeoJob
from seo_orchestrator.domain.approvals import canonical_plan_bytes, fingerprint_plan
from seo_orchestrator.errors import (
    ApprovalInvalid,
    InvalidTransition,
    NotFound,
    StateConflict,
)
from seo_orchestrator.services.approvals import ApprovalService
from seo_orchestrator.services.jobs import JobService

NOW = datetime(2026, 8, 4, 9, 30, tzinfo=UTC)


def _plan(**changes: object) -> ExecutionPlan:
    values: dict[str, object] = {
        "pipeline_version": "pipeline-v1",
        "executor_name": "isolated-n8n",
        "model_ids": ("research-model", "writer-model"),
        "provider_ids": ("provider-a", "provider-b"),
        "maximum_retries": 2,
        "cost_currency": "USD",
        "cost_min_decimal": "1.25",
        "cost_max_decimal": "2.50",
        "unknown_cost_reasons": ("scrape volume varies",),
        "result_destination": "local-artifacts",
    }
    values.update(changes)
    return ExecutionPlan(**values)  # type: ignore[arg-type]


def _seed_snapshot(
    conn: sqlite3.Connection,
    *,
    company_id: str = "company-one",
    direction_id: str = "direction-one",
    audience_id: str = "audience-one",
    brief_id: str = "brief-one",
    snapshot_id: str = "snapshot-one",
    actor_id: str = "actor-one",
    prompt_set_version: int = 1,
) -> tuple[str, dict[str, JsonValue]]:
    if conn.execute(
        "SELECT 1 FROM companies WHERE company_id = ?", (company_id,)
    ).fetchone() is None:
        conn.execute(
            "INSERT INTO companies(company_id, created_at, updated_at) VALUES (?, ?, ?)",
            (company_id, NOW.isoformat(), NOW.isoformat()),
        )
        conn.execute(
            """INSERT INTO company_profile_versions(
                   company_id, version, company_profile_id, profile_json,
                   created_at, updated_at
               ) VALUES (?, 1, ?, ?, ?, ?)""",
            (company_id, f"{company_id}-profile", "{}", NOW.isoformat(), NOW.isoformat()),
        )
        conn.execute(
            """INSERT INTO business_direction_versions(
                   company_id, direction_id, version, company_profile_version,
                   direction_json, created_at, updated_at
               ) VALUES (?, ?, 1, 1, ?, ?, ?)""",
            (company_id, direction_id, "{}", NOW.isoformat(), NOW.isoformat()),
        )
        conn.execute(
            """INSERT INTO audience_segment_versions(
                   company_id, direction_id, audience_segment_id, version,
                   direction_version, audience_json, created_at, updated_at
               ) VALUES (?, ?, ?, 1, 1, ?, ?, ?)""",
            (
                company_id,
                direction_id,
                audience_id,
                "{}",
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )

    brief: dict[str, JsonValue] = {
        "brief_id": brief_id,
        "company_id": company_id,
        "company_profile_version": 1,
        "direction_id": direction_id,
        "direction_version": 1,
        "audience_segment_id": audience_id,
        "audience_version": 1,
        "page_type": "service-page",
        "goal": "Explain an invented service",
        "target_language": "en",
        "locale": "en-US",
        "page_structure": ["Overview"],
        "primary_keyword": "invented service",
        "keywords": ["invented service"],
        "lsi_terms": ["invented example"],
        "competitor_urls": ["https://example.test/reference"],
        "current_page_url": None,
        "current_page_context": "Invented current-page context",
        "output_sheet_target": None,
        "created_by": actor_id,
        "created_at": NOW.isoformat().replace("+00:00", "Z"),
        "updated_at": NOW.isoformat().replace("+00:00", "Z"),
    }
    conn.execute(
        """INSERT INTO brief_drafts(
               brief_id, company_id, company_profile_version, direction_id,
               direction_version, audience_segment_id, audience_version,
               brief_json, status, created_by, created_at, updated_at
           ) VALUES (?, ?, 1, ?, 1, ?, 1, ?, 'validated', ?, ?, ?)""",
        (
            brief_id,
            company_id,
            direction_id,
            audience_id,
            canonical_json(brief),
            actor_id,
            NOW.isoformat(),
            NOW.isoformat(),
        ),
    )
    context: dict[str, JsonValue] = {
        "schema_version": 1,
        "company": {"company_id": company_id, "company_profile_version": 1},
        "direction": {"direction_id": direction_id, "direction_version": 1},
        "audience": {"audience_segment_id": audience_id, "audience_version": 1},
        "brief": brief,
        "prompt_set_version": prompt_set_version,
    }
    snapshot_hash = sha256_fingerprint(context)
    conn.execute(
        """INSERT INTO execution_snapshots(
               snapshot_id, brief_id, company_id, company_profile_version,
               direction_id, direction_version, audience_segment_id, audience_version,
               prompt_set_version, compiled_context, snapshot_hash, created_at
           ) VALUES (?, ?, ?, 1, ?, 1, ?, 1, ?, ?, ?, ?)""",
        (
            snapshot_id,
            brief_id,
            company_id,
            direction_id,
            audience_id,
            prompt_set_version,
            canonical_json(context),
            snapshot_hash,
            NOW.isoformat(),
        ),
    )
    return snapshot_hash, context


def _open_seeded(database_path: Path) -> tuple[sqlite3.Connection, str, dict[str, JsonValue]]:
    conn = connect(database_path)
    migrate(conn)
    snapshot_hash, context = _seed_snapshot(conn)
    conn.commit()
    return conn, snapshot_hash, context


def _job_service(
    conn: sqlite3.Connection,
    *,
    company_id: str = "company-one",
    job_id: str = "job-one",
) -> JobService:
    return JobService(
        conn,
        company_id=company_id,
        clock=lambda: NOW,
        id_factory=lambda: job_id,
    )


def test_plan_job_persists_exact_plan_and_derived_scope_with_creation_audit(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "plan.db"
    conn, snapshot_hash, context = _open_seeded(database_path)
    plan = _plan()
    try:
        planned = _job_service(conn).plan_job("snapshot-one", plan)

        assert planned.job_id == "job-one"
        assert planned.snapshot_id == "snapshot-one"
        assert planned.snapshot_hash == snapshot_hash
        assert planned.plan == plan
        assert planned.plan_fingerprint == fingerprint_plan(plan)
        assert planned.state is JobState.PLANNED
        row = conn.execute(
            """SELECT brief_id, brief_fingerprint, snapshot_id, snapshot_hash,
                      company_id, direction_id, audience_segment_id, created_by,
                      state, plan_json, plan_fingerprint,
                      approved_plan_fingerprint, approval_record_id
               FROM jobs WHERE job_id = ?""",
            (planned.job_id,),
        ).fetchone()
        assert row == (
            "brief-one",
            sha256_fingerprint(context["brief"]),
            "snapshot-one",
            snapshot_hash,
            "company-one",
            "direction-one",
            "audience-one",
            "actor-one",
            "PLANNED",
            canonical_plan_bytes(plan),
            fingerprint_plan(plan),
            None,
            None,
        )
        assert conn.execute(
            """SELECT from_state, to_state, reason_summary, occurred_at
               FROM job_transitions WHERE job_id = ?""",
            (planned.job_id,),
        ).fetchall() == [(None, "PLANNED", None, NOW.isoformat())]
        assert not conn.in_transaction
    finally:
        conn.close()

    reopened = connect(database_path)
    try:
        stored = reopened.execute(
            "SELECT plan_json, plan_fingerprint FROM jobs WHERE job_id = ?", ("job-one",)
        ).fetchone()
        assert bytes(stored[0]) == canonical_plan_bytes(plan)
        assert stored[1] == fingerprint_plan(plan)
        assert json.loads(bytes(stored[0])) == json.loads(canonical_plan_bytes(plan))
    finally:
        reopened.close()


@pytest.mark.parametrize("scope", ["company-two", "missing-company"])
def test_plan_job_hides_foreign_and_missing_snapshot_with_identical_error(
    tmp_path: Path, scope: str
) -> None:
    conn, _, _ = _open_seeded(tmp_path / f"plan-isolation-{scope}.db")
    try:
        with pytest.raises(NotFound, match="record not found"):
            _job_service(conn, company_id=scope).plan_job("snapshot-one", _plan())

        assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone() == (0,)
        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == (0,)
        assert not conn.in_transaction
    finally:
        conn.close()


SERVICE_ALLOWED_EDGES = (
    (JobState.DRAFT, JobState.VALIDATED),
    (JobState.VALIDATED, JobState.PLANNED),
    (JobState.PLANNED, JobState.AWAITING_PAID_APPROVAL),
    (JobState.AWAITING_PAID_APPROVAL, JobState.QUEUED),
    (JobState.QUEUED, JobState.RUNNING),
    (JobState.RUNNING, JobState.SUCCEEDED),
    (JobState.SUCCEEDED, JobState.AWAITING_EXPORT_APPROVAL),
    (JobState.AWAITING_EXPORT_APPROVAL, JobState.EXPORTED),
    (JobState.QUEUED, JobState.FAILED_RETRYABLE),
    (JobState.RUNNING, JobState.FAILED_RETRYABLE),
    (JobState.FAILED_RETRYABLE, JobState.QUEUED),
    (JobState.QUEUED, JobState.CANCELED),
    (JobState.RUNNING, JobState.CANCELED),
    (JobState.RUNNING, JobState.FAILED_FINAL),
)


def _force_job_state(
    conn: sqlite3.Connection,
    state: JobState,
    *,
    attempt: int = 1,
    started_at: str | None = None,
    finished_at: str | None = None,
    error_code: str | None = None,
    error_summary: str | None = None,
) -> None:
    conn.execute(
        """UPDATE jobs
           SET state = ?, attempt = ?, started_at = ?, finished_at = ?,
               error_code = ?, error_summary = ?
           WHERE job_id = ?""",
        (
            state.value,
            attempt,
            started_at,
            finished_at,
            error_code,
            error_summary,
            "job-one",
        ),
    )
    conn.commit()


@pytest.mark.parametrize("source,target", SERVICE_ALLOWED_EDGES)
def test_transition_applies_each_allowed_edge_once_and_only_retry_requeues_increment_attempt(
    tmp_path: Path, source: JobState, target: JobState
) -> None:
    conn, _, _ = _open_seeded(tmp_path / f"edge-{source}-{target}.db")
    try:
        service = _job_service(conn)
        service.plan_job("snapshot-one", _plan())
        _force_job_state(conn, source, attempt=4)

        result = service.transition("job-one", source, target, "test edge")

        assert isinstance(result, SeoJob)
        assert result.state is target
        assert result.attempt == (5 if source is JobState.FAILED_RETRYABLE else 4)
        assert conn.execute(
            """SELECT from_state, to_state, reason_summary
               FROM job_transitions WHERE job_id = ? ORDER BY transition_id""",
            ("job-one",),
        ).fetchall() == [(None, "PLANNED", None), (source.value, target.value, "test edge")]
        assert not conn.in_transaction
    finally:
        conn.close()


def test_transition_distinguishes_stale_cas_from_forbidden_actual_edge_without_mutation(
    tmp_path: Path,
) -> None:
    conn, _, _ = _open_seeded(tmp_path / "transition-errors.db")
    try:
        service = _job_service(conn)
        service.plan_job("snapshot-one", _plan())

        with pytest.raises(StateConflict) as stale:
            service.transition(
                "job-one", JobState.VALIDATED, JobState.PLANNED, "stale caller"
            )
        assert stale.value.code == "STATE_CONFLICT"

        with pytest.raises(InvalidTransition) as forbidden:
            service.transition(
                "job-one", JobState.PLANNED, JobState.RUNNING, "skip approval"
            )
        assert forbidden.value.code == "INVALID_TRANSITION"
        assert conn.execute(
            "SELECT state, attempt FROM jobs WHERE job_id = ?", ("job-one",)
        ).fetchone() == ("PLANNED", 1)
        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == (1,)
        assert not conn.in_transaction
    finally:
        conn.close()


def test_duplicate_cancel_is_noop_even_with_stale_expected_state_and_adds_no_audit(
    tmp_path: Path,
) -> None:
    conn, _, _ = _open_seeded(tmp_path / "duplicate-cancel.db")
    try:
        service = _job_service(conn)
        service.plan_job("snapshot-one", _plan())
        _force_job_state(conn, JobState.CANCELED, attempt=3, finished_at=NOW.isoformat())
        before = conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone()

        result = service.transition(
            "job-one", JobState.DRAFT, JobState.CANCELED, "duplicate cancel"
        )

        assert result.state is JobState.CANCELED
        assert result.attempt == 3
        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == before
    finally:
        conn.close()


@pytest.mark.parametrize("scope", ["company-two", "missing-company"])
def test_transition_hides_foreign_and_missing_job_before_duplicate_cancel_logic(
    tmp_path: Path, scope: str
) -> None:
    conn, _, _ = _open_seeded(tmp_path / f"transition-isolation-{scope}.db")
    try:
        _job_service(conn).plan_job("snapshot-one", _plan())
        _force_job_state(conn, JobState.CANCELED)

        with pytest.raises(NotFound, match="record not found"):
            _job_service(conn, company_id=scope).transition(
                "job-one", JobState.DRAFT, JobState.CANCELED, "hidden cancel"
            )

        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == (1,)
    finally:
        conn.close()


def test_transition_rolls_back_state_timestamps_errors_and_attempt_when_audit_insert_fails(
    tmp_path: Path,
) -> None:
    conn, _, _ = _open_seeded(tmp_path / "transition-rollback.db")
    try:
        service = _job_service(conn)
        service.plan_job("snapshot-one", _plan())
        _force_job_state(
            conn,
            JobState.RUNNING,
            attempt=7,
            started_at="2026-08-04T09:00:00+00:00",
            error_code="OLD_ERROR",
            error_summary="old summary",
        )
        before = conn.execute(
            """SELECT state, attempt, started_at, finished_at, error_code, error_summary
               FROM jobs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone()
        conn.execute(
            """CREATE TRIGGER reject_transition_audit
               BEFORE INSERT ON job_transitions
               WHEN NEW.from_state = 'RUNNING'
               BEGIN
                   SELECT RAISE(ABORT, 'audit rejected');
               END"""
        )

        with pytest.raises(sqlite3.IntegrityError, match="audit rejected"):
            service.transition(
                "job-one", JobState.RUNNING, JobState.FAILED_FINAL, "new failure"
            )

        assert conn.execute(
            """SELECT state, attempt, started_at, finished_at, error_code, error_summary
               FROM jobs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == before
        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == (1,)
        assert not conn.in_transaction
    finally:
        conn.close()


def _concurrent_transition(database_path: Path, barrier: Barrier) -> str:
    conn = connect(database_path)
    try:
        barrier.wait(timeout=10)
        try:
            JobService(conn, company_id="company-one", clock=lambda: NOW).transition(
                "job-one",
                JobState.PLANNED,
                JobState.AWAITING_PAID_APPROVAL,
                "concurrent caller",
            )
        except StateConflict:
            return "conflict"
        return "won"
    finally:
        conn.close()


def test_concurrent_cas_transition_has_one_winner_and_one_audit_row(tmp_path: Path) -> None:
    database_path = tmp_path / "transition-concurrency.db"
    conn, _, _ = _open_seeded(database_path)
    try:
        _job_service(conn).plan_job("snapshot-one", _plan())
    finally:
        conn.close()

    barrier = Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(
            executor.map(lambda _: _concurrent_transition(database_path, barrier), range(2))
        )

    assert sorted(outcomes) == ["conflict", "won"]
    verification = connect(database_path)
    try:
        assert verification.execute(
            "SELECT state FROM jobs WHERE job_id = ?", ("job-one",)
        ).fetchone() == ("AWAITING_PAID_APPROVAL",)
        assert verification.execute(
            """SELECT COUNT(*) FROM job_transitions
               WHERE from_state = ? AND to_state = ?""",
            ("PLANNED", "AWAITING_PAID_APPROVAL"),
        ).fetchone() == (1,)
    finally:
        verification.close()


def _prepare_awaiting_job(
    conn: sqlite3.Connection, plan: ExecutionPlan | None = None
) -> tuple[str, str]:
    selected_plan = plan or _plan()
    service = _job_service(conn)
    planned = service.plan_job("snapshot-one", selected_plan)
    service.transition(
        planned.job_id,
        JobState.PLANNED,
        JobState.AWAITING_PAID_APPROVAL,
        "request paid approval",
    )
    return planned.snapshot_hash, planned.plan_fingerprint


def _approval_service(
    conn: sqlite3.Connection,
    *,
    company_id: str = "company-one",
    approval_id: str = "approval-one",
) -> ApprovalService:
    return ApprovalService(
        conn,
        company_id=company_id,
        clock=lambda: NOW,
        id_factory=lambda: approval_id,
    )


def _assert_unapproved(conn: sqlite3.Connection) -> None:
    assert conn.execute(
        """SELECT state, approved_plan_fingerprint, approval_record_id
           FROM jobs WHERE job_id = ?""",
        ("job-one",),
    ).fetchone() == ("AWAITING_PAID_APPROVAL", None, None)
    assert conn.execute("SELECT COUNT(*) FROM approval_records").fetchone() == (0,)
    assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == (2,)
    assert not conn.in_transaction


def test_approve_job_atomically_binds_one_paid_approval_and_queues_job(
    tmp_path: Path,
) -> None:
    conn, snapshot_hash, _ = _open_seeded(tmp_path / "approval.db")
    plan = _plan()
    try:
        _prepare_awaiting_job(conn, plan)

        approval = _approval_service(conn).approve_job(
            "job-one", "approver-one", snapshot_hash, fingerprint_plan(plan)
        )

        assert isinstance(approval, ApprovalRecord)
        assert approval.approval_record_id == "approval-one"
        assert approval.job_id == "job-one"
        assert approval.approval_type == "paid_execution"
        assert approval.snapshot_hash == snapshot_hash
        assert approval.plan_fingerprint == fingerprint_plan(plan)
        assert approval.approved_by == "approver-one"
        assert approval.approved_at == NOW
        assert approval.expires_at is None
        assert conn.execute(
            """SELECT state, approved_plan_fingerprint, approval_record_id, attempt
               FROM jobs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == ("QUEUED", fingerprint_plan(plan), "approval-one", 1)
        assert conn.execute(
            """SELECT approval_record_id, job_id, approval_type, snapshot_hash,
                      plan_fingerprint, approved_by, approved_at, expires_at
               FROM approval_records"""
        ).fetchall() == [
            (
                "approval-one",
                "job-one",
                "paid_execution",
                snapshot_hash,
                fingerprint_plan(plan),
                "approver-one",
                NOW.isoformat(),
                None,
            )
        ]
        assert conn.execute(
            """SELECT from_state, to_state FROM job_transitions
               WHERE job_id = ? ORDER BY transition_id""",
            ("job-one",),
        ).fetchall() == [
            (None, "PLANNED"),
            ("PLANNED", "AWAITING_PAID_APPROVAL"),
            ("AWAITING_PAID_APPROVAL", "QUEUED"),
        ]
        assert not conn.in_transaction
    finally:
        conn.close()


PLAN_INVALIDATIONS = (
    pytest.param(_plan(pipeline_version="pipeline-v2"), id="pipeline-version"),
    pytest.param(_plan(executor_name="worker-v2"), id="executor"),
    pytest.param(_plan(model_ids=("different-model",)), id="model-ids"),
    pytest.param(
        _plan(model_ids=("writer-model", "research-model")), id="model-order"
    ),
    pytest.param(_plan(provider_ids=("different-provider",)), id="provider-ids"),
    pytest.param(
        _plan(provider_ids=("provider-b", "provider-a")), id="provider-order"
    ),
    pytest.param(_plan(maximum_retries=3), id="maximum-retries"),
    pytest.param(_plan(cost_currency="EUR"), id="cost-currency"),
    pytest.param(_plan(cost_min_decimal="1.26"), id="cost-min-decimal"),
    pytest.param(_plan(cost_max_decimal="2.51"), id="cost-max-decimal"),
    pytest.param(
        _plan(unknown_cost_reasons=("unknown model price",)),
        id="unknown-cost-reasons",
    ),
    pytest.param(_plan(result_destination="different-destination"), id="destination"),
)


@pytest.mark.parametrize("changed_plan", PLAN_INVALIDATIONS)
def test_approval_rejects_every_changed_plan_field_without_mutation(
    tmp_path: Path, changed_plan: ExecutionPlan
) -> None:
    conn, snapshot_hash, _ = _open_seeded(
        tmp_path / f"approval-plan-{fingerprint_plan(changed_plan)}.db"
    )
    try:
        _prepare_awaiting_job(conn)

        with pytest.raises(ApprovalInvalid) as invalid:
            _approval_service(conn).approve_job(
                "job-one",
                "approver-one",
                snapshot_hash,
                fingerprint_plan(changed_plan),
            )

        assert invalid.value.code == "APPROVAL_INVALID"
        _assert_unapproved(conn)
    finally:
        conn.close()


def test_approval_rejects_changed_snapshot_hash_without_mutation(tmp_path: Path) -> None:
    conn, _, _ = _open_seeded(tmp_path / "approval-snapshot.db")
    try:
        _, plan_fingerprint = _prepare_awaiting_job(conn)

        with pytest.raises(ApprovalInvalid):
            _approval_service(conn).approve_job(
                "job-one", "approver-one", "f" * 64, plan_fingerprint
            )

        _assert_unapproved(conn)
    finally:
        conn.close()


def _add_prompt_variant_snapshot(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        """SELECT brief_id, company_id, company_profile_version, direction_id,
                  direction_version, audience_segment_id, audience_version,
                  compiled_context
           FROM execution_snapshots WHERE snapshot_id = ?""",
        ("snapshot-one",),
    ).fetchone()
    context = json.loads(bytes(row[7]))
    context["prompt_set_version"] = 2
    snapshot_hash = sha256_fingerprint(context)
    conn.execute(
        """INSERT INTO execution_snapshots(
               snapshot_id, brief_id, company_id, company_profile_version,
               direction_id, direction_version, audience_segment_id, audience_version,
               prompt_set_version, compiled_context, snapshot_hash, created_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 2, ?, ?, ?)""",
        (
            "snapshot-prompt-two",
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            canonical_json(context),
            snapshot_hash,
            NOW.isoformat(),
        ),
    )
    conn.commit()
    return snapshot_hash


def test_prompt_set_change_via_new_snapshot_invalidates_approval(tmp_path: Path) -> None:
    conn, _, _ = _open_seeded(tmp_path / "approval-prompt.db")
    try:
        changed_snapshot_hash = _add_prompt_variant_snapshot(conn)
        _, plan_fingerprint = _prepare_awaiting_job(conn)

        with pytest.raises(ApprovalInvalid):
            _approval_service(conn).approve_job(
                "job-one", "approver-one", changed_snapshot_hash, plan_fingerprint
            )

        assert conn.execute(
            """SELECT prompt_set_version FROM execution_snapshots
               WHERE snapshot_id = ?""",
            ("snapshot-prompt-two",),
        ).fetchone() == (2,)
        _assert_unapproved(conn)
    finally:
        conn.close()


@pytest.mark.parametrize("tamper", ["missing-plan", "fingerprint", "noncanonical-bytes"])
def test_approval_fails_closed_for_legacy_or_tampered_durable_plan(
    tmp_path: Path, tamper: str
) -> None:
    conn, snapshot_hash, _ = _open_seeded(tmp_path / f"approval-tamper-{tamper}.db")
    try:
        _, submitted_fingerprint = _prepare_awaiting_job(conn)
        if tamper == "missing-plan":
            conn.execute(
                """UPDATE jobs SET plan_json = NULL, plan_fingerprint = NULL
                   WHERE job_id = ?""",
                ("job-one",),
            )
        elif tamper == "fingerprint":
            submitted_fingerprint = "0" * 64
            conn.execute(
                "UPDATE jobs SET plan_fingerprint = ? WHERE job_id = ?",
                (submitted_fingerprint, "job-one"),
            )
        else:
            payload = canonical_plan_bytes(_plan())
            tampered_payload = b"\n" + payload
            submitted_fingerprint = hashlib.sha256(tampered_payload).hexdigest()
            conn.execute(
                """UPDATE jobs SET plan_json = ?, plan_fingerprint = ?
                   WHERE job_id = ?""",
                (tampered_payload, submitted_fingerprint, "job-one"),
            )
        conn.commit()

        with pytest.raises(ApprovalInvalid) as invalid:
            _approval_service(conn).approve_job(
                "job-one",
                "approver-one",
                snapshot_hash,
                submitted_fingerprint,
            )

        assert invalid.value.code == "APPROVAL_INVALID"
        _assert_unapproved(conn)
    finally:
        conn.close()


@pytest.mark.parametrize("scope", ["company-two", "missing-company"])
def test_approve_job_hides_foreign_and_missing_job_without_mutation(
    tmp_path: Path, scope: str
) -> None:
    conn, snapshot_hash, _ = _open_seeded(tmp_path / f"approval-isolation-{scope}.db")
    try:
        _, plan_fingerprint = _prepare_awaiting_job(conn)

        with pytest.raises(NotFound, match="record not found"):
            _approval_service(conn, company_id=scope).approve_job(
                "job-one", "approver-one", snapshot_hash, plan_fingerprint
            )

        _assert_unapproved(conn)
    finally:
        conn.close()


def test_approval_requires_actual_awaiting_state_and_duplicate_does_not_rebind(
    tmp_path: Path,
) -> None:
    conn, snapshot_hash, _ = _open_seeded(tmp_path / "approval-state.db")
    plan = _plan()
    try:
        _job_service(conn).plan_job("snapshot-one", plan)
        service = _approval_service(conn)
        with pytest.raises(StateConflict):
            service.approve_job(
                "job-one", "approver-one", snapshot_hash, fingerprint_plan(plan)
            )
        assert conn.execute("SELECT COUNT(*) FROM approval_records").fetchone() == (0,)

        _job_service(conn).transition(
            "job-one",
            JobState.PLANNED,
            JobState.AWAITING_PAID_APPROVAL,
            "request paid approval",
        )
        first = service.approve_job(
            "job-one", "approver-one", snapshot_hash, fingerprint_plan(plan)
        )
        with pytest.raises(StateConflict):
            _approval_service(conn, approval_id="approval-two").approve_job(
                "job-one", "approver-two", snapshot_hash, fingerprint_plan(plan)
            )

        assert conn.execute("SELECT COUNT(*) FROM approval_records").fetchone() == (1,)
        assert conn.execute(
            "SELECT approval_record_id FROM jobs WHERE job_id = ?", ("job-one",)
        ).fetchone() == (first.approval_record_id,)
    finally:
        conn.close()


def test_approval_rolls_back_record_binding_state_and_audit_when_audit_insert_fails(
    tmp_path: Path,
) -> None:
    conn, snapshot_hash, _ = _open_seeded(tmp_path / "approval-rollback.db")
    try:
        _, plan_fingerprint = _prepare_awaiting_job(conn)
        conn.execute(
            """CREATE TRIGGER reject_approval_audit
               BEFORE INSERT ON job_transitions
               WHEN NEW.from_state = 'AWAITING_PAID_APPROVAL'
               BEGIN
                   SELECT RAISE(ABORT, 'approval audit rejected');
               END"""
        )

        with pytest.raises(sqlite3.IntegrityError, match="approval audit rejected"):
            _approval_service(conn).approve_job(
                "job-one", "approver-one", snapshot_hash, plan_fingerprint
            )

        _assert_unapproved(conn)
    finally:
        conn.close()


def _concurrent_approval(
    database_path: Path,
    barrier: Barrier,
    contender: int,
    snapshot_hash: str,
    plan_fingerprint: str,
) -> str:
    conn = connect(database_path)
    try:
        barrier.wait(timeout=10)
        try:
            approval = ApprovalService(
                conn,
                company_id="company-one",
                clock=lambda: NOW,
                id_factory=lambda: f"approval-{contender}",
            ).approve_job(
                "job-one",
                f"approver-{contender}",
                snapshot_hash,
                plan_fingerprint,
            )
        except StateConflict:
            return "conflict"
        return approval.approval_record_id
    finally:
        conn.close()


def test_concurrent_approval_creates_one_record_and_one_unambiguous_binding(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "approval-concurrency.db"
    conn, snapshot_hash, _ = _open_seeded(database_path)
    try:
        _, plan_fingerprint = _prepare_awaiting_job(conn)
    finally:
        conn.close()

    barrier = Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(
            executor.map(
                lambda contender: _concurrent_approval(
                    database_path,
                    barrier,
                    contender,
                    snapshot_hash,
                    plan_fingerprint,
                ),
                range(2),
            )
        )

    assert outcomes.count("conflict") == 1
    winner = next(outcome for outcome in outcomes if outcome != "conflict")
    verification = connect(database_path)
    try:
        assert verification.execute(
            "SELECT COUNT(*) FROM approval_records"
        ).fetchone() == (1,)
        assert verification.execute(
            """SELECT state, approval_record_id, approved_plan_fingerprint
               FROM jobs WHERE job_id = ?""",
            ("job-one",),
        ).fetchone() == ("QUEUED", winner, plan_fingerprint)
        assert verification.execute(
            """SELECT COUNT(*) FROM job_transitions
               WHERE from_state = ? AND to_state = ?""",
            ("AWAITING_PAID_APPROVAL", "QUEUED"),
        ).fetchone() == (1,)
    finally:
        verification.close()
