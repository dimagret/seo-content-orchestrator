from __future__ import annotations

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier

import pytest

from seo_orchestrator.canonical import JsonValue, canonical_json, sha256_fingerprint
from seo_orchestrator.db.connection import connect
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.db.repositories import JobRecord, JobRepository
from seo_orchestrator.domain import ApprovalRecord, ExecutionPlan, JobState, SeoJob
from seo_orchestrator.domain.approvals import canonical_plan_bytes, fingerprint_plan
from seo_orchestrator.errors import (
    ApprovalInvalid,
    DataIntegrityError,
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
    timestamp = NOW.isoformat().replace("+00:00", "Z")
    company_context: dict[str, JsonValue] = {
        "company_id": company_id,
        "company_profile_id": f"{company_id}-profile",
        "company_profile_version": 1,
        "name": "Example Company",
        "brand_summary": "A test-only company profile",
        "products_services_overview": "An invented service",
        "commercial_model": "Fixed price",
        "pricing_overview": "Published test prices",
        "service_geography": "Test region",
        "value_propositions": ["Predictable delivery"],
        "proof_points": ["Documented test proof"],
        "certifications": ["Test certification"],
        "case_references": ["Test case"],
        "tools_and_process": ["Documented process"],
        "tone_of_voice": "Clear",
        "positive_voice_examples": ["Direct and factual"],
        "negative_voice_examples": ["Unsupported hype"],
        "reading_level": "General",
        "allowed_claims": ["Documented claims"],
        "forbidden_claims": ["Guaranteed outcomes"],
        "compliance_requirements": ["Use verified facts"],
        "default_language": "en",
        "default_locale": "en-US",
        "created_at": timestamp,
        "updated_at": timestamp,
    }
    direction_context: dict[str, JsonValue] = {
        "company_id": company_id,
        "company_profile_version": 1,
        "direction_id": direction_id,
        "direction_version": 1,
        "name": "Example Direction",
        "offerings": ["Invented offering"],
        "category_context": "Test category",
        "prices_and_tariffs": "Fixed test tariff",
        "direction_value_propositions": ["Auditable workflow"],
        "direction_proof_points": ["Test evidence"],
        "direction_cases": ["Test direction case"],
        "internal_link_catalog": ["/example"],
        "default_page_structure": ["Overview"],
        "default_language": "en",
        "default_locale": "en-US",
        "allowed_claims": ["Verified claim"],
        "forbidden_claims": ["Guaranteed result"],
        "created_at": timestamp,
        "updated_at": timestamp,
    }
    audience_context: dict[str, JsonValue] = {
        "company_id": company_id,
        "direction_id": direction_id,
        "direction_version": 1,
        "audience_segment_id": audience_id,
        "audience_version": 1,
        "name": "Example Audience",
        "buyer_roles": ["Owner"],
        "industry": "Test industry",
        "company_or_customer_size": "Small",
        "geography": "Test region",
        "jobs_to_be_done": ["Evaluate an invented service"],
        "pains_and_risks": ["Unclear scope"],
        "objections": ["Insufficient evidence"],
        "objection_responses": ["Show documented evidence"],
        "selection_criteria": ["Transparent process"],
        "minimum_expectations": ["Clear deliverable"],
        "purchase_triggers": ["Need for a controlled test"],
        "budget_range": "Test budget",
        "decision_cycle": "One week",
        "decision_participants": ["Owner"],
        "preferred_content_formats": ["Service page"],
        "created_at": timestamp,
        "updated_at": timestamp,
    }
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
            (
                company_id,
                f"{company_id}-profile",
                canonical_json(company_context),
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )
        conn.execute(
            """INSERT INTO business_direction_versions(
                   company_id, direction_id, version, company_profile_version,
                   direction_json, created_at, updated_at
               ) VALUES (?, ?, 1, 1, ?, ?, ?)""",
            (
                company_id,
                direction_id,
                canonical_json(direction_context),
                NOW.isoformat(),
                NOW.isoformat(),
            ),
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
                canonical_json(audience_context),
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
        "created_at": timestamp,
        "updated_at": timestamp,
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
        "company": company_context,
        "direction": direction_context,
        "audience": audience_context,
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


SERVICE_GENERIC_EDGES = (
    (JobState.DRAFT, JobState.VALIDATED),
    (JobState.VALIDATED, JobState.PLANNED),
    (JobState.PLANNED, JobState.AWAITING_PAID_APPROVAL),
    (JobState.RUNNING, JobState.SUCCEEDED),
    (JobState.SUCCEEDED, JobState.AWAITING_EXPORT_APPROVAL),
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
    job_id: str = "job-one",
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
            job_id,
        ),
    )
    conn.commit()


@pytest.mark.parametrize("source,target", SERVICE_GENERIC_EDGES)
def test_transition_applies_each_generic_edge_once_and_only_retry_requeues_increment_attempt(
    tmp_path: Path, source: JobState, target: JobState
) -> None:
    conn, _, _ = _open_seeded(tmp_path / f"edge-{source}-{target}.db")
    try:
        service = _job_service(conn)
        service.plan_job("snapshot-one", _plan())
        if target in {JobState.SUCCEEDED, JobState.AWAITING_EXPORT_APPROVAL}:
            _force_job_state(conn, JobState.AWAITING_PAID_APPROVAL, attempt=4)
            _bind_persisted_paid_approval(
                conn,
                approved_at=NOW,
                expires_at=None,
            )
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


@pytest.mark.parametrize("actor_id", ["", "   "])
def test_approve_job_rejects_blank_actor_without_mutation(
    tmp_path: Path,
    actor_id: str,
) -> None:
    conn, snapshot_hash, _ = _open_seeded(tmp_path / f"blank-actor-{len(actor_id)}.db")
    try:
        _, plan_fingerprint = _prepare_awaiting_job(conn)

        with pytest.raises(ApprovalInvalid) as invalid:
            _approval_service(conn).approve_job(
                "job-one", actor_id, snapshot_hash, plan_fingerprint
            )

        assert invalid.value.code == "APPROVAL_INVALID"
        _assert_unapproved(conn)
    finally:
        conn.close()


def _bind_persisted_paid_approval(
    conn: sqlite3.Connection,
    *,
    approved_at: datetime,
    expires_at: datetime | None,
) -> None:
    plan_fingerprint = fingerprint_plan(_plan())
    snapshot_hash = conn.execute(
        "SELECT snapshot_hash FROM jobs WHERE job_id = 'job-one'"
    ).fetchone()[0]
    conn.execute(
        """INSERT INTO approval_records(
               approval_record_id, job_id, approval_type, snapshot_hash,
               plan_fingerprint, approved_by, approved_at, expires_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "temporal-approval",
            "job-one",
            "paid_execution",
            snapshot_hash,
            plan_fingerprint,
            "approver-one",
            approved_at.isoformat(),
            expires_at.isoformat() if expires_at is not None else None,
        ),
    )
    conn.execute(
        """UPDATE jobs
           SET state = 'QUEUED', approved_plan_fingerprint = ?, approval_record_id = ?
           WHERE job_id = 'job-one'""",
        (plan_fingerprint, "temporal-approval"),
    )
    conn.commit()


@pytest.mark.parametrize(
    ("approved_at", "expires_at"),
    [
        (NOW + timedelta(seconds=1), None),
        (NOW - timedelta(seconds=2), NOW - timedelta(seconds=1)),
    ],
)
def test_running_rejects_temporally_invalid_paid_approval(
    tmp_path: Path,
    approved_at: datetime,
    expires_at: datetime | None,
) -> None:
    conn, _, _ = _open_seeded(tmp_path / "temporal-approval.db")
    try:
        _prepare_awaiting_job(conn)
        _bind_persisted_paid_approval(
            conn,
            approved_at=approved_at,
            expires_at=expires_at,
        )

        with pytest.raises(ApprovalInvalid) as invalid:
            _job_service(conn).transition(
                "job-one", JobState.QUEUED, JobState.RUNNING, "paid execution"
            )

        assert invalid.value.code == "APPROVAL_INVALID"
        assert conn.execute(
            "SELECT state FROM jobs WHERE job_id = 'job-one'"
        ).fetchone() == ("QUEUED",)
        assert not conn.in_transaction
    finally:
        conn.close()


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE approval_records SET approved_by = 'rogue-actor'",
        "DELETE FROM approval_records",
    ],
)
def test_paid_approval_evidence_is_append_only(
    tmp_path: Path,
    statement: str,
) -> None:
    conn, snapshot_hash, _ = _open_seeded(tmp_path / "approval-append-only.db")
    try:
        _, plan_fingerprint = _prepare_awaiting_job(conn)
        _approval_service(conn).approve_job(
            "job-one", "approver-one", snapshot_hash, plan_fingerprint
        )

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(statement)
        conn.rollback()

        assert conn.execute(
            "SELECT approved_by FROM approval_records"
        ).fetchone() == ("approver-one",)
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
def test_execution_plan_storage_rejects_post_planning_tamper(
    tmp_path: Path, tamper: str
) -> None:
    conn, _, _ = _open_seeded(tmp_path / f"approval-tamper-{tamper}.db")
    try:
        _prepare_awaiting_job(conn)
        original = conn.execute(
            "SELECT plan_json, plan_fingerprint FROM jobs WHERE job_id = 'job-one'"
        ).fetchone()
        with pytest.raises(sqlite3.IntegrityError):
            if tamper == "missing-plan":
                conn.execute(
                    """UPDATE jobs SET plan_json = NULL, plan_fingerprint = NULL
                       WHERE job_id = ?""",
                    ("job-one",),
                )
            elif tamper == "fingerprint":
                conn.execute(
                    "UPDATE jobs SET plan_fingerprint = ? WHERE job_id = ?",
                    ("0" * 64, "job-one"),
                )
            else:
                payload = canonical_plan_bytes(_plan())
                tampered_payload = b"\n" + payload
                conn.execute(
                    """UPDATE jobs SET plan_json = ?, plan_fingerprint = ?
                       WHERE job_id = ?""",
                    (
                        tampered_payload,
                        hashlib.sha256(tampered_payload).hexdigest(),
                        "job-one",
                    ),
                )
        conn.rollback()

        assert conn.execute(
            "SELECT plan_json, plan_fingerprint FROM jobs WHERE job_id = 'job-one'"
        ).fetchone() == original
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


def _assert_data_integrity(error: pytest.ExceptionInfo[RuntimeError]) -> None:
    assert getattr(error.value, "code", None) == "DATA_INTEGRITY"


@pytest.mark.parametrize(
    "source,target",
    [
        (JobState.AWAITING_PAID_APPROVAL, JobState.QUEUED),
        (JobState.AWAITING_EXPORT_APPROVAL, JobState.EXPORTED),
    ],
)
def test_task7_a_generic_transition_reserves_approval_gates_without_mutation(
    tmp_path: Path, source: JobState, target: JobState
) -> None:
    conn, _, _ = _open_seeded(tmp_path / f"reserved-gate-{source}.db")
    try:
        service = _job_service(conn)
        service.plan_job("snapshot-one", _plan())
        _force_job_state(conn, source)
        before = conn.execute(
            "SELECT state, approved_plan_fingerprint, approval_record_id FROM jobs"
        ).fetchone()
        audit_before = conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone()

        with pytest.raises(InvalidTransition) as invalid:
            service.transition("job-one", source, target, "raw gated edge")

        assert invalid.value.code == "INVALID_TRANSITION"
        assert conn.execute(
            "SELECT state, approved_plan_fingerprint, approval_record_id FROM jobs"
        ).fetchone() == before
        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == audit_before
        assert conn.execute("SELECT COUNT(*) FROM approval_records").fetchone() == (0,)
        assert not conn.in_transaction
    finally:
        conn.close()


def test_task7_a_queued_job_without_persisted_paid_approval_cannot_start(
    tmp_path: Path,
) -> None:
    conn, _, _ = _open_seeded(tmp_path / "run-without-approval.db")
    try:
        service = _job_service(conn)
        service.plan_job("snapshot-one", _plan())
        _force_job_state(conn, JobState.QUEUED)
        audit_before = conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone()

        with pytest.raises(RuntimeError) as invalid:
            service.transition(
                "job-one", JobState.QUEUED, JobState.RUNNING, "start paid execution"
            )

        _assert_data_integrity(invalid)
        assert conn.execute("SELECT state, started_at FROM jobs").fetchone() == (
            "QUEUED",
            None,
        )
        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == audit_before
    finally:
        conn.close()


def test_task7_a_paid_approval_is_the_successful_path_to_running(tmp_path: Path) -> None:
    conn, snapshot_hash, _ = _open_seeded(tmp_path / "approved-run.db")
    try:
        _, plan_fingerprint = _prepare_awaiting_job(conn)
        _approval_service(conn).approve_job(
            "job-one", "approver-one", snapshot_hash, plan_fingerprint
        )

        running = _job_service(conn).transition(
            "job-one", JobState.QUEUED, JobState.RUNNING, "start paid execution"
        )

        assert running.state is JobState.RUNNING
        assert running.approval_record_id == "approval-one"
        assert running.approved_plan_fingerprint == plan_fingerprint
        assert conn.execute("SELECT COUNT(*) FROM approval_records").fetchone() == (1,)
    finally:
        conn.close()


@pytest.mark.parametrize(
    "tamper_sql,parameters",
    [
        ("UPDATE approval_records SET approval_type = 'sheet_export'", ()),
        ("UPDATE approval_records SET snapshot_hash = ?", ("f" * 64,)),
        ("UPDATE approval_records SET plan_fingerprint = ?", ("f" * 64,)),
        ("UPDATE approval_records SET approved_at = 'malformed'", ()),
        ("UPDATE approval_records SET approved_by = ''", ()),
        ("UPDATE approval_records SET approved_by = '   '", ()),
    ],
)
def test_task7_a_approval_record_storage_rejects_binding_tamper(
    tmp_path: Path, tamper_sql: str, parameters: tuple[str, ...]
) -> None:
    digest = hashlib.sha256(tamper_sql.encode()).hexdigest()
    conn, snapshot_hash, _ = _open_seeded(tmp_path / f"approval-storage-{digest}.db")
    try:
        _, plan_fingerprint = _prepare_awaiting_job(conn)
        _approval_service(conn).approve_job(
            "job-one", "approver-one", snapshot_hash, plan_fingerprint
        )
        original = conn.execute("SELECT * FROM approval_records").fetchone()

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(tamper_sql, parameters)
        conn.rollback()

        assert conn.execute("SELECT * FROM approval_records").fetchone() == original
        assert conn.execute("SELECT state FROM jobs").fetchone() == ("QUEUED",)
    finally:
        conn.close()


@pytest.mark.parametrize(
    "tamper_sql,parameters",
    [
        ("UPDATE jobs SET approved_plan_fingerprint = ?", ("f" * 64,)),
        ("UPDATE jobs SET approval_record_id = 'missing-approval'", ()),
    ],
)
def test_task7_a_running_revalidates_complete_paid_approval_binding(
    tmp_path: Path, tamper_sql: str, parameters: tuple[str, ...]
) -> None:
    digest = hashlib.sha256(tamper_sql.encode()).hexdigest()
    conn, snapshot_hash, _ = _open_seeded(tmp_path / f"run-binding-{digest}.db")
    try:
        _, plan_fingerprint = _prepare_awaiting_job(conn)
        _approval_service(conn).approve_job(
            "job-one", "approver-one", snapshot_hash, plan_fingerprint
        )
        conn.execute("DROP TRIGGER jobs_approval_binding_write_once")
        conn.execute(tamper_sql, parameters)
        conn.commit()
        audit_before = conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone()

        with pytest.raises(RuntimeError) as invalid:
            _job_service(conn).transition(
                "job-one", JobState.QUEUED, JobState.RUNNING, "start tampered job"
            )

        _assert_data_integrity(invalid)
        assert conn.execute("SELECT state, started_at FROM jobs").fetchone() == (
            "QUEUED",
            None,
        )
        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == audit_before
    finally:
        conn.close()


@pytest.mark.parametrize(
    "old_state",
    [
        JobState.DRAFT,
        JobState.VALIDATED,
        JobState.PLANNED,
        JobState.AWAITING_PAID_APPROVAL,
    ],
)
def test_task7_b_replan_supersedes_every_same_company_pending_variant(
    tmp_path: Path, old_state: JobState
) -> None:
    conn, snapshot_hash, _ = _open_seeded(tmp_path / f"replan-{old_state}.db")
    try:
        first_service = _job_service(conn)
        first_service.plan_job("snapshot-one", _plan())
        _force_job_state(conn, old_state)
        _seed_snapshot(
            conn,
            brief_id="brief-two",
            snapshot_id="snapshot-two",
            actor_id="actor-one",
        )
        conn.commit()

        replacement = _job_service(conn, job_id="job-two").plan_job(
            "snapshot-two", _plan(pipeline_version="pipeline-v2")
        )

        assert replacement.job_id == "job-two"
        assert conn.execute(
            "SELECT superseded_by_job_id FROM jobs WHERE job_id = 'job-one'"
        ).fetchone() == ("job-two",)
        assert conn.execute(
            "SELECT superseded_by_job_id FROM jobs WHERE job_id = 'job-two'"
        ).fetchone() == (None,)
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE jobs SET superseded_by_job_id = NULL WHERE job_id = 'job-one'"
            )
        conn.rollback()
        assert conn.execute(
            "SELECT superseded_by_job_id FROM jobs WHERE job_id = 'job-one'"
        ).fetchone() == ("job-two",)
        with pytest.raises(StateConflict):
            first_service.transition(
                "job-one", old_state, JobState.CANCELED, "superseded job is read-only"
            )
        if old_state is JobState.AWAITING_PAID_APPROVAL:
            with pytest.raises(StateConflict):
                _approval_service(conn).approve_job(
                    "job-one", "approver-one", snapshot_hash, fingerprint_plan(_plan())
                )
        assert conn.execute("SELECT COUNT(*) FROM approval_records").fetchone() == (0,)
    finally:
        conn.close()


def test_task7_b_global_paid_slot_conflict_is_typed_and_rolls_back_second_approval(
    tmp_path: Path,
) -> None:
    conn, first_hash, _ = _open_seeded(tmp_path / "global-slot.db")
    try:
        first_plan = _job_service(conn).plan_job("snapshot-one", _plan())
        _job_service(conn).transition(
            first_plan.job_id,
            JobState.PLANNED,
            JobState.AWAITING_PAID_APPROVAL,
            "first approval",
        )
        second_hash, _ = _seed_snapshot(
            conn,
            company_id="company-two",
            direction_id="direction-two",
            audience_id="audience-two",
            brief_id="brief-two",
            snapshot_id="snapshot-two",
            actor_id="actor-one",
        )
        conn.commit()
        second_service = _job_service(conn, company_id="company-two", job_id="job-two")
        second_plan = second_service.plan_job("snapshot-two", _plan())
        second_service.transition(
            second_plan.job_id,
            JobState.PLANNED,
            JobState.AWAITING_PAID_APPROVAL,
            "second approval",
        )
        assert conn.execute(
            "SELECT superseded_by_job_id FROM jobs WHERE job_id = 'job-one'"
        ).fetchone() == (None,)

        _approval_service(conn).approve_job(
            "job-one", "approver-one", first_hash, first_plan.plan_fingerprint
        )
        audit_before = conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone()
        with pytest.raises(StateConflict) as conflict:
            _approval_service(
                conn, company_id="company-two", approval_id="approval-two"
            ).approve_job(
                "job-two", "approver-two", second_hash, second_plan.plan_fingerprint
            )

        assert conflict.value.code == "STATE_CONFLICT"
        assert conn.execute(
            "SELECT state, approval_record_id FROM jobs WHERE job_id = 'job-two'"
        ).fetchone() == ("AWAITING_PAID_APPROVAL", None)
        assert conn.execute(
            "SELECT approval_record_id FROM approval_records ORDER BY approval_record_id"
        ).fetchall() == [("approval-one",)]
        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == audit_before
        assert not conn.in_transaction
    finally:
        conn.close()


def test_task7_b_active_slot_identity_cannot_be_mutated_after_planning(
    tmp_path: Path,
) -> None:
    conn, first_hash, _ = _open_seeded(tmp_path / "immutable-slot-identity.db")
    try:
        first_plan = _job_service(conn).plan_job("snapshot-one", _plan())
        _job_service(conn).transition(
            first_plan.job_id,
            JobState.PLANNED,
            JobState.AWAITING_PAID_APPROVAL,
            "paid approval",
        )
        _approval_service(conn).approve_job(
            first_plan.job_id,
            "approver-one",
            first_hash,
            first_plan.plan_fingerprint,
        )
        _job_service(conn).transition(
            first_plan.job_id,
            JobState.QUEUED,
            JobState.RUNNING,
            "start paid execution",
        )

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE jobs SET created_by = ? WHERE job_id = ?",
                ("rogue-actor", first_plan.job_id),
            )

        assert conn.execute(
            "SELECT state, created_by FROM jobs WHERE job_id = ?",
            (first_plan.job_id,),
        ).fetchone() == ("RUNNING", "actor-one")
    finally:
        conn.close()


@pytest.mark.parametrize("column", ["plan_json", "plan_fingerprint"])
def test_task7_b_active_slot_plan_predicate_cannot_be_cleared(
    tmp_path: Path,
    column: str,
) -> None:
    conn, first_hash, _ = _open_seeded(tmp_path / f"immutable-slot-{column}.db")
    try:
        first_plan = _job_service(conn).plan_job("snapshot-one", _plan())
        _job_service(conn).transition(
            first_plan.job_id,
            JobState.PLANNED,
            JobState.AWAITING_PAID_APPROVAL,
            "paid approval",
        )
        _approval_service(conn).approve_job(
            first_plan.job_id,
            "approver-one",
            first_hash,
            first_plan.plan_fingerprint,
        )
        statements = {
            "plan_json": "UPDATE jobs SET plan_json = NULL WHERE job_id = ?",
            "plan_fingerprint": (
                "UPDATE jobs SET plan_fingerprint = NULL WHERE job_id = ?"
            ),
        }

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(statements[column], (first_plan.job_id,))

        assert conn.execute(
            "SELECT plan_json IS NOT NULL, plan_fingerprint IS NOT NULL "
            "FROM jobs WHERE job_id = ?",
            (first_plan.job_id,),
        ).fetchone() == (1, 1)
    finally:
        conn.close()


@pytest.mark.parametrize("column", ["plan_json", "plan_fingerprint"])
@pytest.mark.parametrize("awaiting_approval", [False, True])
def test_task7_b_execution_plan_is_immutable_from_initial_planning(
    tmp_path: Path,
    column: str,
    awaiting_approval: bool,
) -> None:
    conn, _, _ = _open_seeded(
        tmp_path / f"immutable-plan-{column}-{awaiting_approval}.db"
    )
    try:
        planned = _job_service(conn).plan_job("snapshot-one", _plan())
        if awaiting_approval:
            _job_service(conn).transition(
                planned.job_id,
                JobState.PLANNED,
                JobState.AWAITING_PAID_APPROVAL,
                "request paid approval",
            )
        statements = {
            "plan_json": "UPDATE jobs SET plan_json = X'7B7D' WHERE job_id = ?",
            "plan_fingerprint": (
                "UPDATE jobs SET plan_fingerprint = '"
                + ("f" * 64)
                + "' WHERE job_id = ?"
            ),
        }

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(statements[column], (planned.job_id,))
        conn.rollback()

        assert conn.execute(
            "SELECT plan_fingerprint FROM jobs WHERE job_id = ?",
            (planned.job_id,),
        ).fetchone() == (planned.plan_fingerprint,)
    finally:
        conn.close()


def test_task7_e_malformed_job_timestamp_is_typed_data_integrity(
    tmp_path: Path,
) -> None:
    conn, _, _ = _open_seeded(tmp_path / "malformed-job-timestamp.db")
    try:
        _job_service(conn).plan_job("snapshot-one", _plan())
        conn.execute("DROP TRIGGER jobs_execution_provenance_immutable")
        conn.execute("UPDATE jobs SET created_at = 'not-a-timestamp'")
        conn.commit()

        with pytest.raises(DataIntegrityError) as invalid:
            _job_service(conn).transition(
                "job-one",
                JobState.PLANNED,
                JobState.AWAITING_PAID_APPROVAL,
                "request paid approval",
            )

        assert invalid.value.code == "DATA_INTEGRITY"
        assert conn.execute("SELECT state FROM jobs").fetchone() == ("PLANNED",)
        assert not conn.in_transaction
    finally:
        conn.close()


@pytest.mark.parametrize(
    "column,value",
    [
        ("snapshot_hash", "f" * 64),
        ("brief_fingerprint", "f" * 64),
        ("created_at", "2027-01-01T00:00:00+00:00"),
    ],
)
def test_task7_b_job_execution_provenance_is_immutable_after_planning(
    tmp_path: Path,
    column: str,
    value: str,
) -> None:
    conn, _, _ = _open_seeded(tmp_path / f"immutable-job-provenance-{column}.db")
    try:
        _job_service(conn).plan_job("snapshot-one", _plan())
        select_sql = {
            "snapshot_hash": "SELECT snapshot_hash FROM jobs WHERE job_id = ?",
            "brief_fingerprint": "SELECT brief_fingerprint FROM jobs WHERE job_id = ?",
            "created_at": "SELECT created_at FROM jobs WHERE job_id = ?",
        }[column]
        update_sql = {
            "snapshot_hash": "UPDATE jobs SET snapshot_hash = ? WHERE job_id = ?",
            "brief_fingerprint": "UPDATE jobs SET brief_fingerprint = ? WHERE job_id = ?",
            "created_at": "UPDATE jobs SET created_at = ? WHERE job_id = ?",
        }[column]
        original = conn.execute(select_sql, ("job-one",)).fetchone()

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(update_sql, (value, "job-one"))
        conn.rollback()

        assert conn.execute(select_sql, ("job-one",)).fetchone() == original
    finally:
        conn.close()


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE jobs SET approved_plan_fingerprint = NULL WHERE job_id = 'job-one'",
        "UPDATE jobs SET approval_record_id = 'replacement' WHERE job_id = 'job-one'",
    ],
)
def test_task7_a_paid_approval_binding_is_write_once(
    tmp_path: Path,
    statement: str,
) -> None:
    conn, snapshot_hash, _ = _open_seeded(tmp_path / "immutable-job-approval.db")
    try:
        _, plan_fingerprint = _prepare_awaiting_job(conn)
        _approval_service(conn).approve_job(
            "job-one", "approver-one", snapshot_hash, plan_fingerprint
        )
        original = conn.execute(
            """SELECT approved_plan_fingerprint, approval_record_id
               FROM jobs WHERE job_id = 'job-one'"""
        ).fetchone()

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(statement)
        conn.rollback()

        assert conn.execute(
            """SELECT approved_plan_fingerprint, approval_record_id
               FROM jobs WHERE job_id = 'job-one'"""
        ).fetchone() == original
    finally:
        conn.close()


def test_task7_e_malformed_job_attempt_is_typed_data_integrity(tmp_path: Path) -> None:
    conn, _, _ = _open_seeded(tmp_path / "malformed-job-attempt.db")
    try:
        _job_service(conn).plan_job("snapshot-one", _plan())
        conn.execute("UPDATE jobs SET attempt = 'not-an-integer'")
        conn.commit()

        with pytest.raises(DataIntegrityError) as invalid:
            _job_service(conn).transition(
                "job-one",
                JobState.PLANNED,
                JobState.AWAITING_PAID_APPROVAL,
                "request paid approval",
            )

        assert invalid.value.code == "DATA_INTEGRITY"
        assert conn.execute("SELECT state FROM jobs").fetchone() == ("PLANNED",)
        assert not conn.in_transaction
    finally:
        conn.close()


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE job_transitions SET reason_summary = 'rewritten'",
        "DELETE FROM job_transitions",
    ],
)
def test_task7_b_transition_audit_is_append_only(
    tmp_path: Path,
    statement: str,
) -> None:
    conn, _, _ = _open_seeded(tmp_path / "immutable-transition-audit.db")
    try:
        _job_service(conn).plan_job("snapshot-one", _plan())
        original = conn.execute("SELECT * FROM job_transitions").fetchall()

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(statement)
        conn.rollback()

        assert conn.execute("SELECT * FROM job_transitions").fetchall() == original
    finally:
        conn.close()


@pytest.mark.parametrize(
    "corruption",
    ["snapshot-binding", "approval-deleted", "plan-rewritten"],
)
def test_task7_e_pre_v3_provenance_corruption_blocks_success_transition(
    tmp_path: Path,
    corruption: str,
) -> None:
    conn, snapshot_hash, _ = _open_seeded(tmp_path / f"legacy-{corruption}.db")
    try:
        _, plan_fingerprint = _prepare_awaiting_job(conn)
        _approval_service(conn).approve_job(
            "job-one", "approver-one", snapshot_hash, plan_fingerprint
        )
        _job_service(conn).transition(
            "job-one", JobState.QUEUED, JobState.RUNNING, "start valid job"
        )
        audit_before = conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone()

        if corruption == "snapshot-binding":
            conn.execute("DROP TRIGGER jobs_execution_provenance_immutable")
            conn.execute("UPDATE jobs SET snapshot_hash = ?", ("f" * 64,))
        elif corruption == "approval-deleted":
            conn.execute("DROP TRIGGER approval_records_immutable_delete")
            conn.execute("DELETE FROM approval_records")
        else:
            replacement_plan = _plan(pipeline_version="pipeline-v2")
            replacement_payload = canonical_plan_bytes(replacement_plan)
            conn.execute("DROP TRIGGER jobs_execution_plan_immutable")
            conn.execute(
                "UPDATE jobs SET plan_json = ?, plan_fingerprint = ?",
                (replacement_payload, fingerprint_plan(replacement_plan)),
            )
        conn.commit()

        with pytest.raises(DataIntegrityError) as invalid:
            _job_service(conn).transition(
                "job-one", JobState.RUNNING, JobState.SUCCEEDED, "finish corrupt job"
            )

        assert invalid.value.code == "DATA_INTEGRITY"
        assert conn.execute("SELECT state FROM jobs").fetchone() == ("RUNNING",)
        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == audit_before
        assert not conn.in_transaction
    finally:
        conn.close()


@pytest.mark.parametrize(
    "tamper",
    [
        "noncanonical-context",
        "snapshot-hash",
        "context-metadata",
        "prompt-metadata",
        "wrong-storage-type",
        "extra-top-level",
        "nested-source-divergence",
        "boolean-version-metadata",
    ],
)
def test_task7_c_snapshot_storage_rejects_post_insert_tamper_without_job(
    tmp_path: Path, tamper: str
) -> None:
    conn, _, _ = _open_seeded(tmp_path / f"snapshot-before-plan-{tamper}.db")
    try:
        row = conn.execute(
            """SELECT compiled_context, snapshot_hash, prompt_set_version
               FROM execution_snapshots WHERE snapshot_id = 'snapshot-one'"""
        ).fetchone()
        with pytest.raises(sqlite3.IntegrityError):
            if tamper == "noncanonical-context":
                conn.execute(
                    "UPDATE execution_snapshots SET compiled_context = ?",
                    (b"\n" + bytes(row[0]),),
                )
            elif tamper == "snapshot-hash":
                conn.execute(
                    "UPDATE execution_snapshots SET snapshot_hash = ?", ("f" * 64,)
                )
            elif tamper == "context-metadata":
                context = json.loads(bytes(row[0]))
                context["direction"]["direction_version"] = 2
                conn.execute(
                    "UPDATE execution_snapshots SET compiled_context = ?, snapshot_hash = ?",
                    (canonical_json(context), sha256_fingerprint(context)),
                )
            elif tamper == "prompt-metadata":
                conn.execute("UPDATE execution_snapshots SET prompt_set_version = 2")
            elif tamper == "wrong-storage-type":
                conn.execute("UPDATE execution_snapshots SET compiled_context = 123")
            elif tamper == "extra-top-level":
                context = json.loads(bytes(row[0]))
                context["rogue"] = "not part of the frozen snapshot envelope"
                conn.execute(
                    "UPDATE execution_snapshots SET compiled_context = ?, snapshot_hash = ?",
                    (canonical_json(context), sha256_fingerprint(context)),
                )
            elif tamper == "boolean-version-metadata":
                context = json.loads(bytes(row[0]))
                context["schema_version"] = True
                context["company"]["company_profile_version"] = True
                conn.execute(
                    "UPDATE execution_snapshots SET compiled_context = ?, snapshot_hash = ?",
                    (canonical_json(context), sha256_fingerprint(context)),
                )
            else:
                context = json.loads(bytes(row[0]))
                context["brief"]["rogue_nested"] = "not compiled from source"
                conn.execute(
                    "UPDATE execution_snapshots SET compiled_context = ?, snapshot_hash = ?",
                    (canonical_json(context), sha256_fingerprint(context)),
                )
        conn.rollback()

        assert conn.execute(
            """SELECT compiled_context, snapshot_hash, prompt_set_version
               FROM execution_snapshots WHERE snapshot_id = 'snapshot-one'"""
        ).fetchone() == row
        assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone() == (0,)
        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == (0,)
        assert not conn.in_transaction
    finally:
        conn.close()


@pytest.mark.parametrize("corruption", ["rogue-nested-key", "boolean-version"])
def test_task7_c_preexisting_snapshot_corruption_fails_strict_hydration(
    tmp_path: Path,
    corruption: str,
) -> None:
    conn, _, _ = _open_seeded(tmp_path / f"preexisting-corruption-{corruption}.db")
    try:
        conn.execute("DROP TRIGGER execution_snapshots_immutable_update")
        row = conn.execute("SELECT compiled_context FROM execution_snapshots").fetchone()
        context = json.loads(bytes(row[0]))
        if corruption == "rogue-nested-key":
            context["brief"]["rogue_nested"] = "pre-v3 corruption"
        else:
            context["schema_version"] = True
            context["company"]["company_profile_version"] = True
        conn.execute(
            "UPDATE execution_snapshots SET compiled_context = ?, snapshot_hash = ?",
            (canonical_json(context), sha256_fingerprint(context)),
        )
        conn.commit()

        with pytest.raises(RuntimeError) as invalid:
            _job_service(conn).plan_job("snapshot-one", _plan())

        _assert_data_integrity(invalid)
        assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone() == (0,)
        assert not conn.in_transaction
    finally:
        conn.close()


def test_task7_c_snapshot_tamper_between_planning_and_approval_is_rejected(
    tmp_path: Path,
) -> None:
    conn, snapshot_hash, _ = _open_seeded(tmp_path / "snapshot-before-approval.db")
    try:
        _, plan_fingerprint = _prepare_awaiting_job(conn)
        row = conn.execute("SELECT compiled_context FROM execution_snapshots").fetchone()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE execution_snapshots SET compiled_context = ?",
                (b"\n" + bytes(row[0]),),
            )
        conn.rollback()

        approved = _approval_service(conn).approve_job(
            "job-one", "approver-one", snapshot_hash, plan_fingerprint
        )
        assert approved.job_id == "job-one"
        assert conn.execute("SELECT state FROM jobs").fetchone() == ("QUEUED",)
    finally:
        conn.close()


def test_task7_c_snapshot_tamper_after_approval_is_rejected_before_running(
    tmp_path: Path,
) -> None:
    conn, snapshot_hash, _ = _open_seeded(tmp_path / "snapshot-before-running.db")
    try:
        _, plan_fingerprint = _prepare_awaiting_job(conn)
        _approval_service(conn).approve_job(
            "job-one", "approver-one", snapshot_hash, plan_fingerprint
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE execution_snapshots SET prompt_set_version = 2")
        conn.rollback()

        running = _job_service(conn).transition(
            "job-one", JobState.QUEUED, JobState.RUNNING, "start approved snapshot"
        )

        assert running.state is JobState.RUNNING
        assert running.started_at == NOW
    finally:
        conn.close()


@pytest.mark.parametrize(
    "field,value",
    [
        ("snapshot_hash", "f" * 64),
        ("direction_id", "rogue-direction"),
        ("audience_segment_id", "rogue-audience"),
        ("created_by", "rogue-actor"),
    ],
)
def test_task7_c_add_job_rejects_caller_metadata_inconsistent_with_snapshot(
    tmp_path: Path, field: str, value: str
) -> None:
    conn, snapshot_hash, context = _open_seeded(tmp_path / f"rogue-job-{field}.db")
    try:
        values: dict[str, object] = {
            "job_id": "rogue-job",
            "brief_id": "brief-one",
            "brief_fingerprint": sha256_fingerprint(context["brief"]),
            "snapshot_id": "snapshot-one",
            "snapshot_hash": snapshot_hash,
            "company_id": "company-one",
            "direction_id": "direction-one",
            "audience_segment_id": "audience-one",
            "state": JobState.PLANNED.value,
            "current_stage": None,
            "approved_plan_fingerprint": None,
            "approval_record_id": None,
            "attempt": 1,
            "created_by": "actor-one",
            "created_at": NOW.isoformat(),
            "started_at": None,
            "finished_at": None,
            "error_code": None,
            "error_summary": None,
            "artifact_manifest_path": None,
            "plan_json": canonical_plan_bytes(_plan()),
            "plan_fingerprint": fingerprint_plan(_plan()),
        }
        values[field] = value

        with pytest.raises(RuntimeError) as invalid:
            JobRepository(conn).add_job(JobRecord(**values))  # type: ignore[arg-type]

        _assert_data_integrity(invalid)
        assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone() == (0,)
    finally:
        conn.close()


def test_task7_c_foreign_scope_gets_not_found_before_snapshot_integrity_disclosure(
    tmp_path: Path,
) -> None:
    conn, _, _ = _open_seeded(tmp_path / "corrupt-snapshot-isolation.db")
    try:
        conn.execute("DROP TRIGGER execution_snapshots_immutable_update")
        conn.execute("UPDATE execution_snapshots SET compiled_context = X'00'")
        conn.commit()

        with pytest.raises(NotFound, match="record not found"):
            _job_service(conn, company_id="company-two").plan_job(
                "snapshot-one", _plan()
            )
    finally:
        conn.close()


def test_task7_e_approval_rejects_empty_actor_without_mutation(tmp_path: Path) -> None:
    conn, snapshot_hash, _ = _open_seeded(tmp_path / "empty-approval-actor.db")
    try:
        _, plan_fingerprint = _prepare_awaiting_job(conn)

        with pytest.raises(ApprovalInvalid):
            _approval_service(conn).approve_job(
                "job-one", "", snapshot_hash, plan_fingerprint
            )

        _assert_unapproved(conn)
    finally:
        conn.close()


@pytest.mark.parametrize(
    "tamper",
    ["missing-json", "missing-fingerprint", "noncanonical-json", "hash-mismatch"],
)
def test_task7_e_plan_trigger_rejects_every_partial_or_invalid_mutation(
    tmp_path: Path, tamper: str
) -> None:
    conn, _, _ = _open_seeded(tmp_path / f"transition-plan-integrity-{tamper}.db")
    try:
        _job_service(conn).plan_job("snapshot-one", _plan())
        before = conn.execute("SELECT state, plan_json, plan_fingerprint FROM jobs").fetchone()

        with pytest.raises(sqlite3.IntegrityError):
            if tamper == "missing-json":
                conn.execute("UPDATE jobs SET plan_json = NULL")
            elif tamper == "missing-fingerprint":
                conn.execute("UPDATE jobs SET plan_fingerprint = NULL")
            elif tamper == "noncanonical-json":
                payload = b"\n" + canonical_plan_bytes(_plan())
                conn.execute(
                    "UPDATE jobs SET plan_json = ?, plan_fingerprint = ?",
                    (payload, hashlib.sha256(payload).hexdigest()),
                )
            else:
                conn.execute("UPDATE jobs SET plan_fingerprint = ?", ("0" * 64,))
        conn.rollback()

        assert conn.execute("SELECT state, plan_json, plan_fingerprint FROM jobs").fetchone() == before
        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == (1,)
    finally:
        conn.close()


def test_task7_e_malformed_persisted_job_state_is_a_stable_integrity_error(
    tmp_path: Path,
) -> None:
    conn, _, _ = _open_seeded(tmp_path / "malformed-job-state.db")
    try:
        _job_service(conn).plan_job("snapshot-one", _plan())
        conn.execute("UPDATE jobs SET state = 'NOT_A_JOB_STATE'")
        conn.commit()

        with pytest.raises(RuntimeError) as invalid:
            _job_service(conn).transition(
                "job-one",
                JobState.PLANNED,
                JobState.AWAITING_PAID_APPROVAL,
                "malformed persisted state",
            )

        _assert_data_integrity(invalid)
        assert conn.execute("SELECT state FROM jobs").fetchone() == ("NOT_A_JOB_STATE",)
        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == (1,)
    finally:
        conn.close()
