import sqlite3
from pathlib import Path

import pytest

from seo_orchestrator.db.connection import connect
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.db.repositories import (
    ApprovalRepository,
    BriefRepository,
    CompanyRepository,
    JobRepository,
    SnapshotRepository,
)
from seo_orchestrator.errors import NotFound


def _seed_two_companies(conn: sqlite3.Connection) -> None:
    for company_id in ("avtomalyar", "sweet-world"):
        conn.execute(
            "INSERT INTO companies(company_id, created_at, updated_at) VALUES (?, ?, ?)",
            (company_id, "2026-08-03T00:00:00Z", "2026-08-03T00:00:00Z"),
        )
        conn.execute(
            """INSERT INTO company_profile_versions(
                   company_id, version, company_profile_id, profile_json,
                   created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?)""",
            (company_id, 1, f"{company_id}-profile", "{}", "now", "now"),
        )

    direction_rows = (
        ("avtomalyar", "car-painting"),
        ("sweet-world", "wedding-cakes"),
    )
    for company_id, direction_id in direction_rows:
        conn.execute(
            """INSERT INTO business_direction_versions(
                   company_id, direction_id, version, company_profile_version,
                   direction_json, created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (company_id, direction_id, 1, 1, "{}", "now", "now"),
        )

    audience_rows = (
        ("avtomalyar", "car-painting", "private-car-owners"),
        ("sweet-world", "wedding-cakes", "newlyweds"),
    )
    for company_id, direction_id, audience_id in audience_rows:
        conn.execute(
            """INSERT INTO audience_segment_versions(
                   company_id, direction_id, audience_segment_id, version,
                   direction_version, audience_json, created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (company_id, direction_id, audience_id, 1, 1, "{}", "now", "now"),
        )

    conn.execute(
        """INSERT INTO brief_drafts(
               brief_id, company_id, company_profile_version, direction_id,
               direction_version, audience_segment_id, audience_version,
               brief_json, created_by, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "avtomalyar-brief", "avtomalyar", 1, "car-painting", 1,
            "private-car-owners", 1, "{}", "invented-actor", "now", "now",
        ),
    )
    conn.execute(
        """INSERT INTO execution_snapshots(
               snapshot_id, brief_id, company_id, company_profile_version,
               direction_id, direction_version, audience_segment_id, audience_version,
               prompt_set_version, compiled_context, snapshot_hash, created_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "avtomalyar-snapshot", "avtomalyar-brief", "avtomalyar", 1,
            "car-painting", 1, "private-car-owners", 1, 1, b"{}", "a" * 64, "now",
        ),
    )
    conn.execute(
        """INSERT INTO jobs(
               job_id, brief_id, brief_fingerprint, snapshot_id, snapshot_hash,
               company_id, direction_id, audience_segment_id, state,
               created_by, created_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "avtomalyar-job", "avtomalyar-brief", "b" * 64, "avtomalyar-snapshot",
            "a" * 64, "avtomalyar", "car-painting", "private-car-owners",
            "PLANNED", "invented-actor", "now",
        ),
    )
    conn.execute(
        """INSERT INTO approval_records(
               approval_record_id, job_id, approval_type, snapshot_hash,
               plan_fingerprint, approved_by, approved_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "avtomalyar-approval", "avtomalyar-job", "paid_execution",
            "a" * 64, "c" * 64, "invented-approver", "now",
        ),
    )


def test_wedding_cakes_is_hidden_under_avtomalyar(tmp_path: Path) -> None:
    conn = connect(tmp_path / "security.db")
    try:
        migrate(conn)
        _seed_two_companies(conn)

        with pytest.raises(NotFound, match="record not found"):
            CompanyRepository(conn).get_direction("avtomalyar", "wedding-cakes", 1)
    finally:
        conn.close()


def test_private_car_owners_is_hidden_under_sweet_world_direction(tmp_path: Path) -> None:
    conn = connect(tmp_path / "security.db")
    try:
        migrate(conn)
        _seed_two_companies(conn)

        with pytest.raises(NotFound, match="record not found"):
            CompanyRepository(conn).get_audience(
                "sweet-world", "wedding-cakes", "private-car-owners", 1
            )
    finally:
        conn.close()


def test_avtomalyar_job_is_hidden_under_sweet_world(tmp_path: Path) -> None:
    conn = connect(tmp_path / "security.db")
    try:
        migrate(conn)
        _seed_two_companies(conn)

        with pytest.raises(NotFound, match="record not found"):
            JobRepository(conn).get_job("sweet-world", "avtomalyar-job")
    finally:
        conn.close()


def test_avtomalyar_profile_is_hidden_under_sweet_world(tmp_path: Path) -> None:
    conn = connect(tmp_path / "security.db")
    try:
        migrate(conn)
        _seed_two_companies(conn)

        with pytest.raises(NotFound, match="record not found"):
            CompanyRepository(conn).get_profile("missing-company", 1)
    finally:
        conn.close()


def test_avtomalyar_brief_is_hidden_under_sweet_world(tmp_path: Path) -> None:
    conn = connect(tmp_path / "security.db")
    try:
        migrate(conn)
        _seed_two_companies(conn)

        with pytest.raises(NotFound, match="record not found"):
            BriefRepository(conn).get_brief("sweet-world", "avtomalyar-brief")
    finally:
        conn.close()


def test_avtomalyar_snapshot_is_hidden_under_sweet_world(tmp_path: Path) -> None:
    conn = connect(tmp_path / "security.db")
    try:
        migrate(conn)
        _seed_two_companies(conn)

        with pytest.raises(NotFound, match="record not found"):
            SnapshotRepository(conn).get_snapshot("sweet-world", "avtomalyar-snapshot")
    finally:
        conn.close()


def test_avtomalyar_approval_is_hidden_under_sweet_world(tmp_path: Path) -> None:
    conn = connect(tmp_path / "security.db")
    try:
        migrate(conn)
        _seed_two_companies(conn)

        with pytest.raises(NotFound, match="record not found"):
            ApprovalRepository(conn).get_approval(
                "sweet-world", "avtomalyar-job", "avtomalyar-approval"
            )
    finally:
        conn.close()
