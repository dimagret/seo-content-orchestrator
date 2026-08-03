import sqlite3
from pathlib import Path

import pytest

from seo_orchestrator.db.connection import connect, transaction
from seo_orchestrator.db.migrations import migrate

EXPECTED_TABLES = {
    "companies",
    "company_profile_versions",
    "business_direction_versions",
    "audience_segment_versions",
    "brief_drafts",
    "execution_snapshots",
    "jobs",
    "job_transitions",
    "approval_records",
    "artifact_manifests",
    "webhook_nonces",
    "schema_migrations",
}


def _insert_version_graph(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO companies(company_id, created_at, updated_at) VALUES (?, ?, ?)",
        ("company-a", "2026-08-03T00:00:00Z", "2026-08-03T00:00:00Z"),
    )
    conn.execute(
        """INSERT INTO company_profile_versions(
               company_id, version, company_profile_id, profile_json, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?)""",
        ("company-a", 1, "profile-a", "{}", "now", "now"),
    )
    conn.execute(
        """INSERT INTO business_direction_versions(
               company_id, direction_id, version, company_profile_version,
               direction_json, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
        ("company-a", "direction-a", 1, 1, "{}", "now", "now"),
    )
    conn.execute(
        """INSERT INTO audience_segment_versions(
               company_id, direction_id, audience_segment_id, version,
               direction_version, audience_json, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        ("company-a", "direction-a", "audience-a", 1, 1, "{}", "now", "now"),
    )


def _insert_brief_and_snapshot(conn: sqlite3.Connection) -> None:
    _insert_version_graph(conn)
    conn.execute(
        """INSERT INTO brief_drafts(
               brief_id, company_id, company_profile_version, direction_id,
               direction_version, audience_segment_id, audience_version,
               brief_json, created_by, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "brief-a", "company-a", 1, "direction-a", 1, "audience-a", 1,
            "{}", "actor-a", "now", "now",
        ),
    )
    conn.execute(
        """INSERT INTO execution_snapshots(
               snapshot_id, brief_id, company_id, company_profile_version,
               direction_id, direction_version, audience_segment_id, audience_version,
               prompt_set_version, compiled_context, snapshot_hash, created_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "snapshot-a", "brief-a", "company-a", 1, "direction-a", 1,
            "audience-a", 1, 1, b"{}", "a" * 64, "now",
        ),
    )


def _insert_job(conn: sqlite3.Connection, job_id: str, state: str) -> None:
    conn.execute(
        """INSERT INTO jobs(
               job_id, brief_id, brief_fingerprint, snapshot_id, snapshot_hash,
               company_id, direction_id, audience_segment_id, state,
               created_by, created_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            job_id, "brief-a", "b" * 64, "snapshot-a", "a" * 64,
            "company-a", "direction-a", "audience-a", state, "actor-a", "now",
        ),
    )


def test_connect_enables_wal_and_foreign_keys(tmp_path: Path) -> None:
    conn = connect(tmp_path / "orchestrator.db")
    try:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    finally:
        conn.close()


def test_connect_configures_durability_and_busy_timeout(tmp_path: Path) -> None:
    conn = connect(tmp_path / "orchestrator.db")
    try:
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
    finally:
        conn.close()


def test_migrate_applies_version_one_idempotently(tmp_path: Path) -> None:
    conn = connect(tmp_path / "orchestrator.db")
    try:
        assert migrate(conn) == 1
        assert migrate(conn) == 1
        assert conn.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall() == [(1,)]
    finally:
        conn.close()


def test_migration_creates_exact_authoritative_tables(tmp_path: Path) -> None:
    conn = connect(tmp_path / "orchestrator.db")
    try:
        migrate(conn)
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_schema WHERE type = ? AND name NOT LIKE ?",
                ("table", "sqlite_%"),
            )
        }
        assert tables == EXPECTED_TABLES
    finally:
        conn.close()


def test_version_scopes_are_unique_and_owned_by_exact_parent_versions(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "orchestrator.db")
    try:
        migrate(conn)
        _insert_version_graph(conn)

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO company_profile_versions(
                       company_id, version, company_profile_id, profile_json,
                       created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?)""",
                ("company-a", 1, "other-profile", "{}", "now", "now"),
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO business_direction_versions(
                       company_id, direction_id, version, company_profile_version,
                       direction_json, created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                ("company-a", "orphan-direction", 1, 2, "{}", "now", "now"),
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO audience_segment_versions(
                       company_id, direction_id, audience_segment_id, version,
                       direction_version, audience_json, created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                ("company-a", "direction-a", "orphan-audience", 1, 2, "{}", "now", "now"),
            )
    finally:
        conn.close()


def test_brief_requires_exact_company_direction_and_audience_versions(tmp_path: Path) -> None:
    conn = connect(tmp_path / "orchestrator.db")
    try:
        migrate(conn)
        _insert_version_graph(conn)
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO brief_drafts(
                       brief_id, company_id, company_profile_version, direction_id,
                       direction_version, audience_segment_id, audience_version,
                       brief_json, created_by, created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    "brief-a", "company-a", 1, "direction-a", 1, "audience-a", 2,
                    "{}", "actor-a", "now", "now",
                ),
            )
    finally:
        conn.close()


def test_transaction_uses_begin_immediate_and_commits(tmp_path: Path) -> None:
    conn = connect(tmp_path / "orchestrator.db")
    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        conn.execute("CREATE TABLE values_table(value TEXT NOT NULL)")
        with transaction(conn) as active_conn:
            assert active_conn is conn
            active_conn.execute("INSERT INTO values_table(value) VALUES (?)", ("kept",))
        assert conn.execute("SELECT value FROM values_table").fetchall() == [("kept",)]
        assert "BEGIN IMMEDIATE" in statements
        assert "COMMIT" in statements
    finally:
        conn.close()


def test_transaction_rolls_back_and_propagates_ordinary_exception(tmp_path: Path) -> None:
    conn = connect(tmp_path / "orchestrator.db")
    try:
        conn.execute("CREATE TABLE values_table(value TEXT NOT NULL)")
        with pytest.raises(RuntimeError, match="stop"), transaction(conn):
            conn.execute("INSERT INTO values_table(value) VALUES (?)", ("discarded",))
            raise RuntimeError("stop")
        assert conn.execute("SELECT value FROM values_table").fetchall() == []
    finally:
        conn.close()


def test_transaction_begin_failure_does_not_rollback_existing_transaction(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "orchestrator.db")
    try:
        conn.execute("CREATE TABLE values_table(value TEXT NOT NULL)")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO values_table(value) VALUES (?)", ("outer",))
        with pytest.raises(
            sqlite3.OperationalError, match="within a transaction"
        ), transaction(conn):
            pass
        assert conn.in_transaction
        assert conn.execute("SELECT value FROM values_table").fetchall() == [("outer",)]
        conn.rollback()
    finally:
        conn.close()


def test_snapshot_hash_and_active_job_creator_are_unique(tmp_path: Path) -> None:
    conn = connect(tmp_path / "orchestrator.db")
    try:
        migrate(conn)
        _insert_brief_and_snapshot(conn)
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO execution_snapshots(
                       snapshot_id, brief_id, company_id, company_profile_version,
                       direction_id, direction_version, audience_segment_id,
                       audience_version, prompt_set_version, compiled_context,
                       snapshot_hash, created_at
                   ) SELECT ?, brief_id, company_id, company_profile_version,
                            direction_id, direction_version, audience_segment_id,
                            audience_version, prompt_set_version, compiled_context,
                            snapshot_hash, created_at
                     FROM execution_snapshots WHERE snapshot_id = ?""",
                ("snapshot-b", "snapshot-a"),
            )
        _insert_job(conn, "job-a", "RUNNING")
        with pytest.raises(sqlite3.IntegrityError):
            _insert_job(conn, "job-b", "QUEUED")
        conn.execute("UPDATE jobs SET state = ? WHERE job_id = ?", ("SUCCEEDED", "job-a"))
        _insert_job(conn, "job-b", "QUEUED")
    finally:
        conn.close()


def test_transition_ids_are_never_reused(tmp_path: Path) -> None:
    conn = connect(tmp_path / "orchestrator.db")
    try:
        migrate(conn)
        _insert_brief_and_snapshot(conn)
        _insert_job(conn, "job-a", "RUNNING")
        first_id = conn.execute(
            """INSERT INTO job_transitions(job_id, to_state, occurred_at)
               VALUES (?, ?, ?)""",
            ("job-a", "RUNNING", "now"),
        ).lastrowid
        conn.execute("DELETE FROM job_transitions WHERE transition_id = ?", (first_id,))
        second_id = conn.execute(
            """INSERT INTO job_transitions(job_id, to_state, occurred_at)
               VALUES (?, ?, ?)""",
            ("job-a", "SUCCEEDED", "later"),
        ).lastrowid
        assert first_id is not None
        assert second_id is not None
        assert second_id > first_id
    finally:
        conn.close()


@pytest.mark.parametrize("table", ["execution_snapshots", "jobs"])
def test_historical_snapshot_and_job_foreign_keys_do_not_cascade(
    tmp_path: Path, table: str
) -> None:
    conn = connect(tmp_path / "orchestrator.db")
    try:
        migrate(conn)
        delete_actions = {
            row[6].upper() for row in conn.execute(f"PRAGMA foreign_key_list({table})")
        }
        assert "CASCADE" not in delete_actions
    finally:
        conn.close()
