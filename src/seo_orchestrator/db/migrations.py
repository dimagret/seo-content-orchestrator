"""Versioned SQLite schema migrations."""

import sqlite3

_LATEST_VERSION = 1

_MIGRATION_0001 = (
    """
    CREATE TABLE companies (
        company_id TEXT PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'archived')),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        archived_at TEXT
    )
    """,
    """
    CREATE TABLE company_profile_versions (
        company_id TEXT NOT NULL,
        version INTEGER NOT NULL CHECK (version > 0),
        company_profile_id TEXT NOT NULL,
        profile_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (company_id, version),
        UNIQUE (company_id, company_profile_id, version),
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
    )
    """,
    """
    CREATE TABLE business_direction_versions (
        company_id TEXT NOT NULL,
        direction_id TEXT NOT NULL,
        version INTEGER NOT NULL CHECK (version > 0),
        company_profile_version INTEGER NOT NULL CHECK (company_profile_version > 0),
        direction_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (company_id, direction_id, version),
        FOREIGN KEY (company_id, company_profile_version)
            REFERENCES company_profile_versions(company_id, version)
    )
    """,
    """
    CREATE TABLE audience_segment_versions (
        company_id TEXT NOT NULL,
        direction_id TEXT NOT NULL,
        audience_segment_id TEXT NOT NULL,
        version INTEGER NOT NULL CHECK (version > 0),
        direction_version INTEGER NOT NULL CHECK (direction_version > 0),
        audience_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (company_id, direction_id, audience_segment_id, version),
        FOREIGN KEY (company_id, direction_id, direction_version)
            REFERENCES business_direction_versions(company_id, direction_id, version)
    )
    """,
    """
    CREATE TABLE brief_drafts (
        brief_id TEXT PRIMARY KEY,
        company_id TEXT NOT NULL,
        company_profile_version INTEGER,
        direction_id TEXT,
        direction_version INTEGER,
        audience_segment_id TEXT,
        audience_version INTEGER,
        brief_json TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'draft',
        created_by TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (company_id) REFERENCES companies(company_id),
        FOREIGN KEY (company_id, company_profile_version)
            REFERENCES company_profile_versions(company_id, version),
        FOREIGN KEY (company_id, direction_id, direction_version)
            REFERENCES business_direction_versions(company_id, direction_id, version),
        FOREIGN KEY (company_id, direction_id, audience_segment_id, audience_version)
            REFERENCES audience_segment_versions(
                company_id, direction_id, audience_segment_id, version
            )
    )
    """,
    """
    CREATE TABLE execution_snapshots (
        snapshot_id TEXT PRIMARY KEY,
        brief_id TEXT NOT NULL,
        company_id TEXT NOT NULL,
        company_profile_version INTEGER NOT NULL,
        direction_id TEXT NOT NULL,
        direction_version INTEGER NOT NULL,
        audience_segment_id TEXT NOT NULL,
        audience_version INTEGER NOT NULL,
        prompt_set_version INTEGER NOT NULL CHECK (prompt_set_version > 0),
        compiled_context BLOB NOT NULL,
        snapshot_hash TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL,
        FOREIGN KEY (brief_id) REFERENCES brief_drafts(brief_id),
        FOREIGN KEY (company_id, company_profile_version)
            REFERENCES company_profile_versions(company_id, version),
        FOREIGN KEY (company_id, direction_id, direction_version)
            REFERENCES business_direction_versions(company_id, direction_id, version),
        FOREIGN KEY (company_id, direction_id, audience_segment_id, audience_version)
            REFERENCES audience_segment_versions(
                company_id, direction_id, audience_segment_id, version
            )
    )
    """,
    """
    CREATE TABLE jobs (
        job_id TEXT PRIMARY KEY,
        brief_id TEXT NOT NULL,
        brief_fingerprint TEXT NOT NULL,
        snapshot_id TEXT NOT NULL,
        snapshot_hash TEXT NOT NULL,
        company_id TEXT NOT NULL,
        direction_id TEXT NOT NULL,
        audience_segment_id TEXT NOT NULL,
        state TEXT NOT NULL,
        current_stage TEXT,
        approved_plan_fingerprint TEXT,
        approval_record_id TEXT,
        attempt INTEGER NOT NULL DEFAULT 1 CHECK (attempt > 0),
        created_by TEXT NOT NULL,
        created_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT,
        error_code TEXT,
        error_summary TEXT,
        artifact_manifest_path TEXT,
        FOREIGN KEY (brief_id) REFERENCES brief_drafts(brief_id),
        FOREIGN KEY (snapshot_id) REFERENCES execution_snapshots(snapshot_id),
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
    )
    """,
    """
    CREATE UNIQUE INDEX idx_jobs_company_job_id
    ON jobs(company_id, job_id)
    """,
    """
    CREATE UNIQUE INDEX one_active_job_per_creator
    ON jobs(created_by)
    WHERE state IN (
        'DRAFT', 'VALIDATED', 'PLANNED', 'AWAITING_PAID_APPROVAL',
        'QUEUED', 'RUNNING', 'FAILED_RETRYABLE', 'AWAITING_EXPORT_APPROVAL'
    )
    """,
    """
    CREATE TABLE job_transitions (
        transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT NOT NULL,
        from_state TEXT,
        to_state TEXT NOT NULL,
        stage TEXT,
        reason_code TEXT,
        reason_summary TEXT,
        occurred_at TEXT NOT NULL,
        FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )
    """,
    """
    CREATE TABLE approval_records (
        approval_record_id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL,
        approval_type TEXT NOT NULL CHECK (
            approval_type IN ('paid_execution', 'sheet_export', 'final_publication')
        ),
        snapshot_hash TEXT NOT NULL,
        plan_fingerprint TEXT NOT NULL,
        approved_by TEXT NOT NULL,
        approved_at TEXT NOT NULL,
        expires_at TEXT,
        FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )
    """,
    """
    CREATE TABLE artifact_manifests (
        artifact_manifest_id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL,
        company_id TEXT NOT NULL,
        manifest_path TEXT NOT NULL,
        manifest_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY (job_id) REFERENCES jobs(job_id),
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
    )
    """,
    """
    CREATE TABLE webhook_nonces (
        nonce TEXT PRIMARY KEY,
        received_at TEXT NOT NULL,
        expires_at TEXT NOT NULL
    )
    """,
)


def migrate(conn: sqlite3.Connection) -> int:
    """Apply all known migrations and return the latest schema version."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    applied = conn.execute(
        "SELECT 1 FROM schema_migrations WHERE version = ?", (_LATEST_VERSION,)
    ).fetchone()
    if applied is not None:
        return _LATEST_VERSION

    try:
        conn.execute("BEGIN IMMEDIATE")
        for statement in _MIGRATION_0001:
            conn.execute(statement)
        conn.execute(
            "INSERT INTO schema_migrations(version) VALUES (?)",
            (_LATEST_VERSION,),
        )
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    return _LATEST_VERSION
