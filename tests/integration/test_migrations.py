import sqlite3
from pathlib import Path
from typing import cast

import pytest

from seo_orchestrator.db import migrations as migration_module
from seo_orchestrator.db.connection import connect, transaction
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.domain import JobState
from seo_orchestrator.services.jobs import JobService

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


def _insert_job(
    conn: sqlite3.Connection,
    job_id: str,
    state: str,
    *,
    durable_plan: bool = True,
) -> None:
    job_columns = {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}
    if durable_plan and {"plan_json", "plan_fingerprint"} <= job_columns:
        conn.execute(
            """INSERT INTO jobs(
                   job_id, brief_id, brief_fingerprint, snapshot_id, snapshot_hash,
                   company_id, direction_id, audience_segment_id, state,
                   created_by, created_at, plan_json, plan_fingerprint
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                job_id,
                "brief-a",
                "b" * 64,
                "snapshot-a",
                "a" * 64,
                "company-a",
                "direction-a",
                "audience-a",
                state,
                "actor-a",
                "now",
                b"{}",
                "c" * 64,
            ),
        )
        return
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


def test_migrate_applies_ordered_versions_through_three_idempotently(tmp_path: Path) -> None:
    conn = connect(tmp_path / "orchestrator.db")
    try:
        assert migrate(conn) == 3
        assert migrate(conn) == 3
        assert conn.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall() == [(1,), (2,), (3,)]
        job_columns = {
            row[1]: (row[2], row[3]) for row in conn.execute("PRAGMA table_info(jobs)")
        }
        assert job_columns["plan_json"] == ("BLOB", 0)
        assert job_columns["plan_fingerprint"] == ("TEXT", 0)
        assert job_columns["superseded_by_job_id"] == ("TEXT", 0)
    finally:
        conn.close()


def _create_version_one_database(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("BEGIN IMMEDIATE")
    for statement in migration_module._MIGRATION_0001:
        conn.execute(statement)
    conn.execute("INSERT INTO schema_migrations(version) VALUES (1)")
    conn.commit()


def _create_committed_version_two_database(conn: sqlite3.Connection) -> None:
    _create_version_one_database(conn)
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("ALTER TABLE jobs ADD COLUMN plan_json BLOB")
    conn.execute("ALTER TABLE jobs ADD COLUMN plan_fingerprint TEXT")
    conn.execute("INSERT INTO schema_migrations(version) VALUES (2)")
    conn.commit()


def test_task7_d_migrate_upgrades_committed_v2_to_v3_without_rewriting_history(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "upgrade-v2.db")
    try:
        _create_committed_version_two_database(conn)
        _insert_brief_and_snapshot(conn)
        _insert_job(conn, "existing-v2-job", "RUNNING")
        conn.commit()

        assert migrate(conn) == 3
        assert conn.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall() == [(1,), (2,), (3,)]
        assert "superseded_by_job_id" in {
            row[1] for row in conn.execute("PRAGMA table_info(jobs)")
        }
        assert conn.execute(
            "SELECT state, created_by FROM jobs WHERE job_id = ?",
            ("existing-v2-job",),
        ).fetchone() == ("RUNNING", "actor-a")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE jobs SET created_by = ? WHERE job_id = ?",
                ("rogue-actor", "existing-v2-job"),
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE jobs SET plan_json = NULL WHERE job_id = ?",
                ("existing-v2-job",),
            )
    finally:
        conn.close()


def test_migrate_upgrades_v1_without_replaying_it_and_preserves_existing_rows(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "upgrade.db")
    try:
        _create_version_one_database(conn)
        _insert_brief_and_snapshot(conn)
        _insert_job(conn, "legacy-job", "PLANNED")
        conn.execute(
            """INSERT INTO job_transitions(job_id, from_state, to_state, occurred_at)
               VALUES (?, ?, ?, ?)""",
            ("legacy-job", None, "PLANNED", "before-upgrade"),
        )
        conn.commit()

        assert migrate(conn) == 3
        assert conn.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall() == [(1,), (2,), (3,)]
        assert conn.execute(
            """SELECT job_id, state, plan_json, plan_fingerprint
               FROM jobs WHERE job_id = ?""",
            ("legacy-job",),
        ).fetchone() == ("legacy-job", "PLANNED", None, None)
        assert conn.execute(
            """SELECT from_state, to_state, occurred_at
               FROM job_transitions WHERE job_id = ?""",
            ("legacy-job",),
        ).fetchall() == [(None, "PLANNED", "before-upgrade")]

        assert migrate(conn) == 3
        assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone() == (1,)
        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == (1,)
    finally:
        conn.close()


def test_transition_rejects_upgraded_v1_job_without_plan_and_preserves_storage(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "legacy-transition.db")
    try:
        _create_version_one_database(conn)
        _insert_brief_and_snapshot(conn)
        _insert_job(conn, "legacy-job", "PLANNED")
        conn.execute(
            "UPDATE jobs SET created_at = ? WHERE job_id = ?",
            ("2026-08-04T09:30:00+00:00", "legacy-job"),
        )
        conn.execute(
            """INSERT INTO job_transitions(job_id, from_state, to_state, occurred_at)
               VALUES (?, ?, ?, ?)""",
            ("legacy-job", None, "PLANNED", "before-upgrade"),
        )
        conn.commit()
        migrate(conn)
        job_before = conn.execute(
            """SELECT state, attempt, started_at, finished_at, error_code, error_summary,
                      plan_json, plan_fingerprint
               FROM jobs WHERE company_id = ? AND job_id = ?""",
            ("company-a", "legacy-job"),
        ).fetchone()
        audit_before = conn.execute(
            """SELECT transition_id, from_state, to_state, reason_summary, occurred_at
               FROM job_transitions WHERE job_id = ? ORDER BY transition_id""",
            ("legacy-job",),
        ).fetchall()

        with pytest.raises(RuntimeError) as invalid:
            JobService(conn, company_id="company-a").transition(
                "legacy-job",
                JobState.PLANNED,
                JobState.AWAITING_PAID_APPROVAL,
                "request paid approval",
            )

        assert getattr(invalid.value, "code", None) == "DATA_INTEGRITY"
        assert conn.execute(
            """SELECT state, attempt, started_at, finished_at, error_code, error_summary,
                      plan_json, plan_fingerprint
               FROM jobs WHERE company_id = ? AND job_id = ?""",
            ("company-a", "legacy-job"),
        ).fetchone() == job_before
        assert conn.execute(
            """SELECT transition_id, from_state, to_state, reason_summary, occurred_at
               FROM job_transitions WHERE job_id = ? ORDER BY transition_id""",
            ("legacy-job",),
        ).fetchall() == audit_before
        assert not conn.in_transaction
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
        conn.execute("DROP TRIGGER job_transitions_immutable_delete")
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


def test_task7_d_migrate_rejects_caller_transaction_without_rolling_it_back(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "nested-migrate.db")
    try:
        conn.execute("CREATE TABLE caller_values(value TEXT NOT NULL)")
        conn.execute("INSERT INTO caller_values(value) VALUES ('survives')")
        assert conn.in_transaction

        with pytest.raises(sqlite3.OperationalError):
            migrate(conn)

        assert conn.in_transaction
        assert conn.execute("SELECT value FROM caller_values").fetchall() == [("survives",)]
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'schema_migrations'"
        ).fetchone() == (0,)
        conn.rollback()
    finally:
        conn.close()


class _SimulatedMigrationCancellation(BaseException):
    pass


class _ControlledMigrationConnection:
    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        cancel_statement: str | None = None,
        fail_commit: bool = False,
    ) -> None:
        self._conn = conn
        self._cancel_statement = cancel_statement
        self._fail_commit = fail_commit
        self.rollback_calls = 0

    @property
    def in_transaction(self) -> bool:
        return self._conn.in_transaction

    def execute(
        self, statement: str, parameters: tuple[object, ...] = ()
    ) -> sqlite3.Cursor:
        if statement == self._cancel_statement:
            raise _SimulatedMigrationCancellation
        return self._conn.execute(statement, parameters)

    def commit(self) -> None:
        if self._fail_commit:
            self._fail_commit = False
            raise sqlite3.IntegrityError("simulated migration commit failure")
        self._conn.commit()

    def rollback(self) -> None:
        self.rollback_calls += 1
        self._conn.rollback()


def test_task7_d_migrate_rolls_back_cancellation_and_partial_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = sqlite3.connect(tmp_path / "migration-cancellation.db")
    controlled = _ControlledMigrationConnection(
        conn, cancel_statement="SIMULATE_CANCELLATION"
    )
    monkeypatch.setattr(
        migration_module,
        "_MIGRATIONS",
        ((1, ("CREATE TABLE partial_migration(value TEXT)", "SIMULATE_CANCELLATION")),),
    )
    try:
        with pytest.raises(_SimulatedMigrationCancellation):
            migrate(cast(sqlite3.Connection, controlled))

        assert controlled.rollback_calls == 1
        assert not conn.in_transaction
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'partial_migration'"
        ).fetchone() == (0,)
        assert conn.execute("SELECT version FROM schema_migrations").fetchall() == []
        conn.execute("CREATE TABLE connection_reused(value TEXT)")
        conn.commit()
    finally:
        conn.close()


def test_task7_d_migrate_rolls_back_commit_failure_and_schema_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = sqlite3.connect(tmp_path / "migration-commit-failure.db")
    controlled = _ControlledMigrationConnection(conn, fail_commit=True)
    monkeypatch.setattr(
        migration_module,
        "_MIGRATIONS",
        ((1, ("CREATE TABLE partial_migration(value TEXT)",)),),
    )
    try:
        with pytest.raises(sqlite3.IntegrityError, match="commit failure"):
            migrate(cast(sqlite3.Connection, controlled))

        assert controlled.rollback_calls == 1
        assert not conn.in_transaction
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'partial_migration'"
        ).fetchone() == (0,)
        assert conn.execute("SELECT version FROM schema_migrations").fetchall() == []
        conn.execute("CREATE TABLE connection_reused(value TEXT)")
        conn.commit()
    finally:
        conn.close()


def _create_migration_history(
    conn: sqlite3.Connection, versions: tuple[int, ...]
) -> None:
    conn.execute(
        """CREATE TABLE schema_migrations (
               version INTEGER PRIMARY KEY,
               applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
           )"""
    )
    conn.executemany(
        "INSERT INTO schema_migrations(version) VALUES (?)",
        ((version,) for version in versions),
    )
    conn.commit()


@pytest.mark.parametrize(
    "versions",
    [
        pytest.param((2,), id="v2-without-v1"),
        pytest.param((1, 3), id="gap"),
        pytest.param((1, 2, 3, 99), id="unknown-future"),
    ],
)
def test_task7_d_migrate_rejects_noncontiguous_or_unknown_history(
    tmp_path: Path, versions: tuple[int, ...]
) -> None:
    conn = connect(tmp_path / f"invalid-history-{versions[-1]}.db")
    try:
        _create_migration_history(conn, versions)

        with pytest.raises(RuntimeError) as invalid:
            migrate(conn)

        assert getattr(invalid.value, "code", None) == "MIGRATION_INVALID"
        assert conn.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall() == [(version,) for version in versions]
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'jobs'"
        ).fetchone() == (0,)
        assert not conn.in_transaction
    finally:
        conn.close()


def test_task7_b_migration_execution_slot_excludes_pending_and_covers_all_active_jobs(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "execution-slot-index.db")
    try:
        migrate(conn)
        _insert_brief_and_snapshot(conn)
        _insert_job(conn, "planned-one", "PLANNED")
        _insert_job(conn, "planned-two", "AWAITING_PAID_APPROVAL")
        _insert_job(conn, "paid-active", "QUEUED")

        with pytest.raises(sqlite3.IntegrityError):
            _insert_job(conn, "second-paid-active", "RUNNING")
        with pytest.raises(sqlite3.IntegrityError):
            _insert_job(conn, "legacy-running", "RUNNING", durable_plan=False)
        assert conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE created_by = 'actor-a'"
        ).fetchone() == (3,)
        index_sql = conn.execute(
            "SELECT sql FROM sqlite_schema WHERE name = 'one_active_job_per_creator'"
        ).fetchone()[0]
        assert "'QUEUED', 'RUNNING', 'FAILED_RETRYABLE'" in index_sql
        assert "plan_json IS NOT NULL" not in index_sql
        assert "plan_fingerprint IS NOT NULL" not in index_sql
    finally:
        conn.close()


def test_task7_b_upgraded_plan_null_active_row_blocks_second_paid_slot(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "legacy-active-slot.db")
    try:
        _create_version_one_database(conn)
        _insert_brief_and_snapshot(conn)
        _insert_job(conn, "legacy-running", "RUNNING")
        conn.commit()

        migrate(conn)
        with pytest.raises(sqlite3.IntegrityError):
            _insert_job(conn, "current-queued", "QUEUED")

        assert conn.execute(
            "SELECT job_id, plan_json, plan_fingerprint FROM jobs ORDER BY job_id"
        ).fetchall() == [("legacy-running", None, None)]
    finally:
        conn.close()


def test_task7_b_new_job_cannot_be_inserted_pre_superseded(tmp_path: Path) -> None:
    conn = connect(tmp_path / "pre-superseded-job.db")
    try:
        migrate(conn)
        _insert_brief_and_snapshot(conn)
        _insert_job(conn, "replacement", "PLANNED")
        conn.commit()

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO jobs(
                       job_id, brief_id, brief_fingerprint, snapshot_id, snapshot_hash,
                       company_id, direction_id, audience_segment_id, state,
                       created_by, created_at, plan_json, plan_fingerprint,
                       superseded_by_job_id
                   )
                   SELECT 'pre-superseded', brief_id, brief_fingerprint,
                          snapshot_id, snapshot_hash, company_id, direction_id,
                          audience_segment_id, state, created_by, created_at,
                          plan_json, plan_fingerprint, 'replacement'
                   FROM jobs WHERE job_id = 'replacement'"""
            )
        conn.rollback()

        assert conn.execute("SELECT job_id FROM jobs").fetchall() == [("replacement",)]
    finally:
        conn.close()


def test_task7_e_duplicate_cancel_precedes_legacy_plan_guard_after_scoped_lookup(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "legacy-canceled.db")
    try:
        _create_version_one_database(conn)
        _insert_brief_and_snapshot(conn)
        _insert_job(conn, "legacy-canceled", "CANCELED")
        conn.execute(
            "UPDATE jobs SET created_at = ?, finished_at = ? WHERE job_id = ?",
            (
                "2026-08-04T09:30:00+00:00",
                "2026-08-04T09:31:00+00:00",
                "legacy-canceled",
            ),
        )
        conn.commit()
        migrate(conn)

        result = JobService(conn, company_id="company-a").transition(
            "legacy-canceled", JobState.DRAFT, JobState.CANCELED, "duplicate cancel"
        )

        assert result.state is JobState.CANCELED
        assert conn.execute("SELECT COUNT(*) FROM job_transitions").fetchone() == (0,)
        assert not conn.in_transaction
    finally:
        conn.close()
