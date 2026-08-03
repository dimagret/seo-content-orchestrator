import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest

from seo_orchestrator.db.connection import connect, transaction
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.db.repositories import JobRecord, JobRepository
from seo_orchestrator.errors import CompanyArchived, NotFound, VersionConflict
from seo_orchestrator.services.company_cards import (
    AudienceData,
    CompanyCardService,
    CompanyProfileData,
    CreateAudience,
    CreateCompany,
    CreateDirection,
    DirectionData,
    ReviseCompany,
    ReviseDirection,
)

NOW = datetime(2026, 8, 3, tzinfo=UTC)
FIXTURE_ROOT = Path(__file__).parents[2] / "fixtures" / "companies"


def _load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _create_fixture_card(
    service: CompanyCardService, fixture: dict[str, Any]
) -> tuple[str, str, str]:
    company_id = fixture["company_id"]
    direction_id = fixture["direction"]["direction_id"]
    audience_id = fixture["direction"]["audience"]["audience_segment_id"]
    service.create_company(
        CreateCompany(
            company_id=company_id,
            company_profile_id=fixture["company_profile_id"],
            actor_id="fixture-actor",
            replacement=CompanyProfileData.model_validate(fixture["profile"]),
        )
    )
    service.create_direction(
        CreateDirection(
            company_id=company_id,
            company_profile_version=1,
            direction_id=direction_id,
            actor_id="fixture-actor",
            replacement=DirectionData.model_validate(fixture["direction"]["data"]),
        )
    )
    service.create_audience(
        CreateAudience(
            company_id=company_id,
            direction_id=direction_id,
            direction_version=1,
            audience_segment_id=audience_id,
            actor_id="fixture-actor",
            replacement=AudienceData.model_validate(
                fixture["direction"]["audience"]["data"]
            ),
        )
    )
    return company_id, direction_id, audience_id


def test_fixture_cards_allocate_monotonic_versions_without_mutating_history(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        service = CompanyCardService(conn, clock=lambda: NOW)
        avtomalyar = _load_fixture("avtomalyar.json")
        sweet_world = _load_fixture("sweet-world.json")
        assert avtomalyar["fixture_notice"].startswith("Invented non-secret")
        assert sweet_world["fixture_notice"].startswith("Invented non-secret")

        with transaction(conn):
            _create_fixture_card(service, avtomalyar)
            _create_fixture_card(service, sweet_world)
            original_profile_json = conn.execute(
                """SELECT profile_json FROM company_profile_versions
                   WHERE company_id = ? AND version = 1""",
                ("avtomalyar",),
            ).fetchone()[0]
            profile_two = service.revise_company(
                ReviseCompany(
                    company_id="avtomalyar",
                    actor_id="fixture-editor",
                    expected_current_version=1,
                    replacement=CompanyProfileData.model_validate(
                        {**avtomalyar["profile"], "name": "Avtomalyar Workshop"}
                    ),
                )
            )
            profile_three = service.revise_company(
                ReviseCompany(
                    company_id="avtomalyar",
                    actor_id="fixture-editor",
                    expected_current_version=2,
                    replacement=CompanyProfileData.model_validate(
                        {**avtomalyar["profile"], "name": "Avtomalyar Lab"}
                    ),
                )
            )
            direction_two = service.revise_direction(
                ReviseDirection(
                    company_id="avtomalyar",
                    company_profile_version=3,
                    direction_id="car-painting",
                    actor_id="fixture-editor",
                    expected_current_version=1,
                    replacement=DirectionData.model_validate(avtomalyar["direction"]["data"]),
                )
            )
            direction_three = service.revise_direction(
                ReviseDirection(
                    company_id="avtomalyar",
                    company_profile_version=3,
                    direction_id="car-painting",
                    actor_id="fixture-editor",
                    expected_current_version=2,
                    replacement=DirectionData.model_validate(
                        {**avtomalyar["direction"]["data"], "name": "Paint Refinishing"}
                    ),
                )
            )

        assert (profile_two.company_profile_version, profile_three.company_profile_version) == (2, 3)
        assert (direction_two.direction_version, direction_three.direction_version) == (2, 3)
        assert conn.execute(
            """SELECT profile_json FROM company_profile_versions
               WHERE company_id = ? AND version = 1""",
            ("avtomalyar",),
        ).fetchone()[0] == original_profile_json
        assert service.get_company_profile("avtomalyar", 1).name == "Avtomalyar Studio"
        with pytest.raises(NotFound, match="record not found"):
            service.get_direction("avtomalyar", "wedding-cakes", 1)
        with pytest.raises(NotFound, match="record not found"):
            service.get_audience("sweet-world", "wedding-cakes", "private-car-owners", 1)
    finally:
        conn.close()


def test_service_never_commits_caller_owned_transaction(tmp_path: Path) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        service = CompanyCardService(conn, clock=lambda: NOW)
        conn.execute("BEGIN IMMEDIATE")
        _create_fixture_card(service, _load_fixture("avtomalyar.json"))
        service.archive_company("avtomalyar", "fixture-archivist")
        assert conn.in_transaction
        conn.rollback()

        assert conn.execute("SELECT COUNT(*) FROM companies").fetchone() == (0,)
        assert conn.execute("SELECT COUNT(*) FROM company_profile_versions").fetchone() == (0,)
        assert conn.execute("SELECT COUNT(*) FROM business_direction_versions").fetchone() == (0,)
        assert conn.execute("SELECT COUNT(*) FROM audience_segment_versions").fetchone() == (0,)
    finally:
        conn.close()


def test_archival_preserves_historical_job_and_exact_card_readability(tmp_path: Path) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        service = CompanyCardService(conn, clock=lambda: NOW)
        fixture = _load_fixture("avtomalyar.json")
        with transaction(conn):
            _create_fixture_card(service, fixture)
            conn.execute(
                """INSERT INTO brief_drafts(
                       brief_id, company_id, company_profile_version, direction_id,
                       direction_version, audience_segment_id, audience_version,
                       brief_json, created_by, created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    "historical-brief", "avtomalyar", 1, "car-painting", 1,
                    "private-car-owners", 1, "{}", "fixture-actor",
                    NOW.isoformat(), NOW.isoformat(),
                ),
            )
            conn.execute(
                """INSERT INTO execution_snapshots(
                       snapshot_id, brief_id, company_id, company_profile_version,
                       direction_id, direction_version, audience_segment_id, audience_version,
                       prompt_set_version, compiled_context, snapshot_hash, created_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    "historical-snapshot", "historical-brief", "avtomalyar", 1,
                    "car-painting", 1, "private-car-owners", 1, 1, b"{}", "a" * 64,
                    NOW.isoformat(),
                ),
            )
            historical_job = JobRecord(
                job_id="historical-job",
                brief_id="historical-brief",
                brief_fingerprint="b" * 64,
                snapshot_id="historical-snapshot",
                snapshot_hash="a" * 64,
                company_id="avtomalyar",
                direction_id="car-painting",
                audience_segment_id="private-car-owners",
                state="SUCCEEDED",
                current_stage=None,
                approved_plan_fingerprint=None,
                approval_record_id=None,
                attempt=1,
                created_by="fixture-actor",
                created_at=NOW.isoformat(),
                started_at=NOW.isoformat(),
                finished_at=NOW.isoformat(),
                error_code=None,
                error_summary=None,
                artifact_manifest_path="/invented/historical-manifest.json",
            )
            JobRepository(conn).add_job(historical_job)
            service.archive_company("avtomalyar", "fixture-archivist")

        assert JobRepository(conn).get_job("avtomalyar", "historical-job") == historical_job
        assert service.get_company_profile("avtomalyar", 1).name == "Avtomalyar Studio"
        assert service.get_direction("avtomalyar", "car-painting", 1).direction_version == 1
        assert service.get_audience(
            "avtomalyar", "car-painting", "private-car-owners", 1
        ).audience_version == 1
        with pytest.raises(CompanyArchived) as caught, transaction(conn):
            service.revise_company(
                ReviseCompany(
                    company_id="avtomalyar",
                    actor_id="fixture-editor",
                    expected_current_version=1,
                    replacement=CompanyProfileData.model_validate(fixture["profile"]),
                )
            )
        assert caught.value.code == "COMPANY_ARCHIVED"
    finally:
        conn.close()


def test_concurrent_expected_version_writers_yield_one_update_and_one_conflict(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "cards.db"
    seed_conn = connect(db_path)
    try:
        migrate(seed_conn)
        with transaction(seed_conn):
            CompanyCardService(seed_conn, clock=lambda: NOW).create_company(
                CreateCompany(
                    company_id="avtomalyar",
                    company_profile_id="avtomalyar-profile",
                    actor_id="fixture-actor",
                    replacement=CompanyProfileData.model_validate(
                        _load_fixture("avtomalyar.json")["profile"]
                    ),
                )
            )
    finally:
        seed_conn.close()

    barrier = Barrier(2)

    def revise(name: str, actor_id: str) -> str:
        conn = connect(db_path)
        try:
            barrier.wait()
            try:
                with transaction(conn):
                    CompanyCardService(conn, clock=lambda: NOW).revise_company(
                        ReviseCompany(
                            company_id="avtomalyar",
                            actor_id=actor_id,
                            expected_current_version=1,
                            replacement=CompanyProfileData.model_validate(
                                {**_load_fixture("avtomalyar.json")["profile"], "name": name}
                            ),
                        )
                    )
            except VersionConflict as exc:
                return exc.code
            return "UPDATED"
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (
            executor.submit(revise, "Writer One", "editor-one"),
            executor.submit(revise, "Writer Two", "editor-two"),
        )
        outcomes = sorted(future.result() for future in futures)

    assert outcomes == ["UPDATED", "VERSION_CONFLICT"]
    verify_conn = connect(db_path)
    try:
        assert CompanyCardService(verify_conn).get_company_profile(
            "avtomalyar", 2
        ).company_profile_version == 2
        assert verify_conn.execute(
            "SELECT COUNT(*) FROM company_profile_versions WHERE company_id = ?",
            ("avtomalyar",),
        ).fetchone() == (2,)
    finally:
        verify_conn.close()
