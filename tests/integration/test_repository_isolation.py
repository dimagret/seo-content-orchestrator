from datetime import UTC, datetime
from pathlib import Path

from seo_orchestrator.db.connection import connect
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.db.repositories import (
    ApprovalRecord,
    ApprovalRepository,
    BriefRepository,
    CompanyRepository,
    JobRecord,
    JobRepository,
    SnapshotRepository,
)
from seo_orchestrator.domain import (
    AudienceSegment,
    BusinessDirection,
    CompanyProfile,
    ExecutionSnapshot,
    SeoBrief,
)

NOW = datetime(2026, 8, 3, tzinfo=UTC)


def _profile(company_id: str, name: str) -> CompanyProfile:
    return CompanyProfile(
        company_id=company_id,
        company_profile_id=f"{company_id}-profile",
        company_profile_version=1,
        name=name,
        brand_summary=f"Invented profile for {name}",
        products_services_overview="Invented services",
        commercial_model="Direct service",
        pricing_overview="Quoted per project",
        service_geography="Local region",
        value_propositions=["Careful work"],
        proof_points=["Invented portfolio"],
        certifications=["Invented training"],
        case_references=["Invented case"],
        tools_and_process=["Documented process"],
        tone_of_voice="Clear and helpful",
        positive_voice_examples=["Plain explanation"],
        negative_voice_examples=["Unsupported promise"],
        reading_level="General audience",
        allowed_claims=["Transparent process"],
        forbidden_claims=["Guaranteed outcome"],
        compliance_requirements=["Use invented data"],
        default_language="en",
        default_locale="en-US",
        created_at=NOW,
        updated_at=NOW,
    )


def _direction(company_id: str, direction_id: str) -> BusinessDirection:
    return BusinessDirection(
        direction_id=direction_id,
        company_id=company_id,
        company_profile_version=1,
        direction_version=1,
        name=direction_id.replace("-", " ").title(),
        offerings=("Invented offering",),
        category_context="Invented category",
        prices_and_tariffs="Quoted per project",
        direction_value_propositions=("Careful service",),
        direction_proof_points=("Invented case",),
        direction_cases=("Invented example",),
        internal_link_catalog=("/invented",),
        default_page_structure=("Overview",),
        default_language="en",
        default_locale="en-US",
        allowed_claims=("Transparent process",),
        forbidden_claims=("Guaranteed outcome",),
        created_at=NOW,
        updated_at=NOW,
    )


def _audience(company_id: str, direction_id: str, audience_id: str) -> AudienceSegment:
    return AudienceSegment(
        audience_segment_id=audience_id,
        company_id=company_id,
        direction_id=direction_id,
        direction_version=1,
        audience_version=1,
        name=audience_id.replace("-", " ").title(),
        buyer_roles=("Buyer",),
        industry="Invented market",
        company_or_customer_size="Individual",
        geography="Local region",
        jobs_to_be_done=("Choose a provider",),
        pains_and_risks=("Unclear quality",),
        objections=("Price uncertainty",),
        objection_responses=("Explain scope",),
        selection_criteria=("Documented process",),
        minimum_expectations=("Clear estimate",),
        purchase_triggers=("Upcoming need",),
        budget_range="Quoted",
        decision_cycle="One week",
        decision_participants=("Buyer",),
        preferred_content_formats=("Guide",),
        created_at=NOW,
        updated_at=NOW,
    )


def _brief(company_id: str, direction_id: str, audience_id: str) -> SeoBrief:
    return SeoBrief(
        brief_id=f"{company_id}-brief",
        company_id=company_id,
        company_profile_version=1,
        direction_id=direction_id,
        direction_version=1,
        audience_segment_id=audience_id,
        audience_version=1,
        page_type="service page",
        goal="Explain an invented service",
        target_language="en",
        locale="en-US",
        page_structure=("Overview",),
        primary_keyword="invented service",
        keywords=("invented service",),
        lsi_terms=("invented example",),
        competitor_urls=("https://example.com/",),
        current_page_context="Invented current-page context",
        created_by="invented-actor",
        created_at=NOW,
        updated_at=NOW,
    )


def _snapshot(brief: SeoBrief) -> ExecutionSnapshot:
    return ExecutionSnapshot(
        snapshot_id=f"{brief.company_id}-snapshot",
        brief_id=brief.brief_id,
        company_id=brief.company_id,
        company_profile_version=brief.company_profile_version,
        direction_id=brief.direction_id,
        direction_version=brief.direction_version,
        audience_segment_id=brief.audience_segment_id,
        audience_version=brief.audience_version,
        prompt_set_version=1,
        compiled_context={"schema_version": 1, "company": brief.company_id},
        snapshot_hash="a" * 64,
        created_at=NOW,
    )


def test_brief_and_snapshot_repositories_round_trip_company_scoped_models(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "repository.db")
    try:
        migrate(conn)
        company_repo = CompanyRepository(conn)
        company_repo.add_company("avtomalyar", NOW, NOW)
        company_repo.add_profile(_profile("avtomalyar", "Avtomalyar"))
        company_repo.add_direction(_direction("avtomalyar", "car-painting"))
        company_repo.add_audience(
            _audience("avtomalyar", "car-painting", "private-car-owners")
        )
        brief = _brief("avtomalyar", "car-painting", "private-car-owners")
        snapshot = _snapshot(brief)

        BriefRepository(conn).add_brief(brief)
        SnapshotRepository(conn).add_snapshot(snapshot)

        assert BriefRepository(conn).get_brief("avtomalyar", brief.brief_id) == brief
        assert (
            SnapshotRepository(conn).get_snapshot("avtomalyar", snapshot.snapshot_id)
            == snapshot
        )
    finally:
        conn.close()


def test_job_and_approval_repositories_round_trip_through_company_scope(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "repository.db")
    try:
        migrate(conn)
        company_repo = CompanyRepository(conn)
        company_repo.add_company("avtomalyar", NOW, NOW)
        company_repo.add_profile(_profile("avtomalyar", "Avtomalyar"))
        company_repo.add_direction(_direction("avtomalyar", "car-painting"))
        company_repo.add_audience(
            _audience("avtomalyar", "car-painting", "private-car-owners")
        )
        brief = _brief("avtomalyar", "car-painting", "private-car-owners")
        snapshot = _snapshot(brief)
        BriefRepository(conn).add_brief(brief)
        SnapshotRepository(conn).add_snapshot(snapshot)
        conn.commit()
        job = JobRecord(
            job_id="avtomalyar-job",
            brief_id=brief.brief_id,
            brief_fingerprint="b" * 64,
            snapshot_id=snapshot.snapshot_id,
            snapshot_hash=snapshot.snapshot_hash,
            company_id="avtomalyar",
            direction_id="car-painting",
            audience_segment_id="private-car-owners",
            state="PLANNED",
            current_stage=None,
            approved_plan_fingerprint=None,
            approval_record_id=None,
            attempt=1,
            created_by="invented-actor",
            created_at=NOW.isoformat(),
            started_at=None,
            finished_at=None,
            error_code=None,
            error_summary=None,
            artifact_manifest_path=None,
        )
        approval = ApprovalRecord(
            approval_record_id="avtomalyar-approval",
            job_id=job.job_id,
            approval_type="paid_execution",
            snapshot_hash=snapshot.snapshot_hash,
            plan_fingerprint="c" * 64,
            approved_by="invented-approver",
            approved_at=NOW.isoformat(),
            expires_at=None,
        )

        conn.execute("BEGIN IMMEDIATE")
        JobRepository(conn).add_job(job)
        ApprovalRepository(conn).add_approval("avtomalyar", approval)

        assert JobRepository(conn).get_job("avtomalyar", job.job_id) == job
        assert (
            ApprovalRepository(conn).get_approval(
                "avtomalyar", job.job_id, approval.approval_record_id
            )
            == approval
        )
        assert conn.in_transaction
        conn.rollback()
        assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone() == (0,)
        assert conn.execute("SELECT COUNT(*) FROM approval_records").fetchone() == (0,)
    finally:
        conn.close()


def test_company_repository_round_trips_exact_direction_and_audience_versions(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "repository.db")
    try:
        migrate(conn)
        repo = CompanyRepository(conn)
        profile = _profile("avtomalyar", "Avtomalyar")
        direction = _direction("avtomalyar", "car-painting")
        audience = _audience("avtomalyar", "car-painting", "private-car-owners")
        repo.add_company("avtomalyar", NOW, NOW)
        repo.add_profile(profile)
        repo.add_direction(direction)
        repo.add_audience(audience)

        assert repo.get_direction("avtomalyar", "car-painting", 1) == direction
        assert (
            repo.get_audience("avtomalyar", "car-painting", "private-car-owners", 1)
            == audience
        )
    finally:
        conn.close()


def test_company_repository_round_trips_profile_without_committing(tmp_path: Path) -> None:
    conn = connect(tmp_path / "repository.db")
    try:
        migrate(conn)
        repo = CompanyRepository(conn)
        profile = _profile("avtomalyar", "Avtomalyar")

        conn.execute("BEGIN IMMEDIATE")
        repo.add_company("avtomalyar", NOW, NOW)
        repo.add_profile(profile)

        assert repo.get_profile("avtomalyar", 1) == profile
        assert conn.in_transaction
        conn.rollback()
        assert conn.execute("SELECT COUNT(*) FROM companies").fetchone() == (0,)
    finally:
        conn.close()


def test_scoped_lookup_query_plans_use_expected_composite_indexes(tmp_path: Path) -> None:
    conn = connect(tmp_path / "repository.db")
    try:
        migrate(conn)
        plans = {
            "direction": conn.execute(
                """EXPLAIN QUERY PLAN
                   SELECT direction_json FROM business_direction_versions
                   WHERE company_id = ? AND direction_id = ? AND version = ?""",
                ("avtomalyar", "car-painting", 1),
            ).fetchall(),
            "audience": conn.execute(
                """EXPLAIN QUERY PLAN
                   SELECT audience_json FROM audience_segment_versions
                   WHERE company_id = ? AND direction_id = ?
                     AND audience_segment_id = ? AND version = ?""",
                ("avtomalyar", "car-painting", "private-car-owners", 1),
            ).fetchall(),
            "job": conn.execute(
                """EXPLAIN QUERY PLAN
                   SELECT job_id FROM jobs
                   WHERE company_id = ? AND job_id = ?""",
                ("avtomalyar", "avtomalyar-job"),
            ).fetchall(),
        }

        expected_indexes = {
            "direction": "sqlite_autoindex_business_direction_versions_1",
            "audience": "sqlite_autoindex_audience_segment_versions_1",
            "job": "idx_jobs_company_job_id",
        }
        for lookup, index_name in expected_indexes.items():
            assert any(index_name in str(row[3]) for row in plans[lookup]), plans[lookup]
    finally:
        conn.close()
