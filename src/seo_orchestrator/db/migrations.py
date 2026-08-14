"""Versioned SQLite schema migrations."""

import sqlite3

from seo_orchestrator.errors import MigrationError

_LATEST_VERSION = 6

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

_MIGRATION_0002 = (
    "ALTER TABLE jobs ADD COLUMN plan_json BLOB",
    "ALTER TABLE jobs ADD COLUMN plan_fingerprint TEXT",
)

_MIGRATION_0003 = (
    "ALTER TABLE jobs ADD COLUMN superseded_by_job_id TEXT REFERENCES jobs(job_id)",
    "DROP INDEX one_active_job_per_creator",
    """
    CREATE UNIQUE INDEX one_active_job_per_creator
    ON jobs(created_by)
    WHERE state IN ('QUEUED', 'RUNNING', 'FAILED_RETRYABLE')
    """,
    """
    CREATE TRIGGER jobs_created_by_immutable
    BEFORE UPDATE OF created_by ON jobs
    FOR EACH ROW
    WHEN NEW.created_by IS NOT OLD.created_by
    BEGIN
        SELECT RAISE(ABORT, 'jobs.created_by is immutable');
    END
    """,
    """
    CREATE TRIGGER jobs_execution_provenance_immutable
    BEFORE UPDATE OF
        job_id, brief_id, brief_fingerprint, snapshot_id, snapshot_hash,
        company_id, direction_id, audience_segment_id, created_at
    ON jobs
    FOR EACH ROW
    WHEN NEW.job_id IS NOT OLD.job_id
      OR NEW.brief_id IS NOT OLD.brief_id
      OR NEW.brief_fingerprint IS NOT OLD.brief_fingerprint
      OR NEW.snapshot_id IS NOT OLD.snapshot_id
      OR NEW.snapshot_hash IS NOT OLD.snapshot_hash
      OR NEW.company_id IS NOT OLD.company_id
      OR NEW.direction_id IS NOT OLD.direction_id
      OR NEW.audience_segment_id IS NOT OLD.audience_segment_id
      OR NEW.created_at IS NOT OLD.created_at
    BEGIN
        SELECT RAISE(ABORT, 'job execution provenance is immutable');
    END
    """,
    """
    CREATE TRIGGER jobs_execution_plan_immutable
    BEFORE UPDATE OF plan_json, plan_fingerprint ON jobs
    FOR EACH ROW
    WHEN (
         NEW.plan_json IS NOT OLD.plan_json
         OR NEW.plan_fingerprint IS NOT OLD.plan_fingerprint
     )
    BEGIN
        SELECT RAISE(ABORT, 'job execution plan is immutable');
    END
    """,
    """
    CREATE TRIGGER jobs_supersession_append_only
    BEFORE UPDATE OF superseded_by_job_id ON jobs
    FOR EACH ROW
    WHEN OLD.superseded_by_job_id IS NOT NULL
     AND NEW.superseded_by_job_id IS NOT OLD.superseded_by_job_id
    BEGIN
        SELECT RAISE(ABORT, 'job supersession is append-only');
    END
    """,
    """
    CREATE TRIGGER jobs_supersession_initially_null
    BEFORE INSERT ON jobs
    FOR EACH ROW
    WHEN NEW.superseded_by_job_id IS NOT NULL
    BEGIN
        SELECT RAISE(ABORT, 'new job cannot be pre-superseded');
    END
    """,
    """
    CREATE TRIGGER jobs_supersession_same_lineage
    BEFORE UPDATE OF superseded_by_job_id ON jobs
    FOR EACH ROW
    WHEN NEW.superseded_by_job_id IS NOT NULL
     AND NOT EXISTS (
         SELECT 1
         FROM jobs AS replacement
         WHERE replacement.job_id = NEW.superseded_by_job_id
           AND replacement.company_id = OLD.company_id
           AND replacement.created_by = OLD.created_by
     )
    BEGIN
        SELECT RAISE(ABORT, 'job supersession must preserve lineage');
    END
    """,
    """
    CREATE TRIGGER jobs_approval_binding_initially_null
    BEFORE INSERT ON jobs
    FOR EACH ROW
    WHEN NEW.approved_plan_fingerprint IS NOT NULL
      OR NEW.approval_record_id IS NOT NULL
    BEGIN
        SELECT RAISE(ABORT, 'new job cannot be pre-approved');
    END
    """,
    """
    CREATE TRIGGER jobs_approval_binding_write_once
    BEFORE UPDATE OF approved_plan_fingerprint, approval_record_id ON jobs
    FOR EACH ROW
    WHEN (
        NEW.approved_plan_fingerprint IS NOT OLD.approved_plan_fingerprint
        OR NEW.approval_record_id IS NOT OLD.approval_record_id
    )
    AND NOT (
        OLD.approved_plan_fingerprint IS NULL
        AND OLD.approval_record_id IS NULL
        AND OLD.state = 'AWAITING_PAID_APPROVAL'
        AND NEW.state = 'QUEUED'
        AND NEW.plan_fingerprint IS NOT NULL
        AND NEW.approved_plan_fingerprint = NEW.plan_fingerprint
        AND NEW.approval_record_id IS NOT NULL
        AND EXISTS (
            SELECT 1
            FROM approval_records AS approval
            WHERE approval.approval_record_id = NEW.approval_record_id
              AND approval.job_id = OLD.job_id
              AND approval.approval_type = 'paid_execution'
              AND approval.snapshot_hash = OLD.snapshot_hash
              AND approval.plan_fingerprint = NEW.plan_fingerprint
        )
    )
    BEGIN
        SELECT RAISE(ABORT, 'job approval binding is write-once');
    END
    """,
    """
    CREATE TRIGGER execution_snapshots_immutable_update
    BEFORE UPDATE ON execution_snapshots
    FOR EACH ROW
    BEGIN
        SELECT RAISE(ABORT, 'execution snapshots are immutable');
    END
    """,
    """
    CREATE TRIGGER execution_snapshots_immutable_delete
    BEFORE DELETE ON execution_snapshots
    FOR EACH ROW
    BEGIN
        SELECT RAISE(ABORT, 'execution snapshots are append-only');
    END
    """,
    """
    CREATE TRIGGER approval_records_immutable_update
    BEFORE UPDATE ON approval_records
    FOR EACH ROW
    BEGIN
        SELECT RAISE(ABORT, 'approval records are immutable');
    END
    """,
    """
    CREATE TRIGGER approval_records_immutable_delete
    BEFORE DELETE ON approval_records
    FOR EACH ROW
    BEGIN
        SELECT RAISE(ABORT, 'approval records are append-only');
    END
    """,
    """
    CREATE TRIGGER job_transitions_immutable_update
    BEFORE UPDATE ON job_transitions
    FOR EACH ROW
    BEGIN
        SELECT RAISE(ABORT, 'job transitions are immutable');
    END
    """,
    """
    CREATE TRIGGER job_transitions_immutable_delete
    BEFORE DELETE ON job_transitions
    FOR EACH ROW
    BEGIN
        SELECT RAISE(ABORT, 'job transitions are append-only');
    END
    """,
)

_MIGRATION_0004 = (
    """
    CREATE TABLE webhook_callback_receipts (
        company_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        snapshot_hash TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        accepted_at TEXT NOT NULL,
        PRIMARY KEY (company_id, job_id, snapshot_hash, idempotency_key),
        FOREIGN KEY (company_id, job_id) REFERENCES jobs(company_id, job_id)
    )
    """,
    """
    CREATE TRIGGER webhook_callback_receipts_immutable_update
    BEFORE UPDATE ON webhook_callback_receipts
    FOR EACH ROW
    BEGIN
        SELECT RAISE(ABORT, 'callback receipts are immutable');
    END
    """,
    """
    CREATE TRIGGER webhook_callback_receipts_immutable_delete
    BEFORE DELETE ON webhook_callback_receipts
    FOR EACH ROW
    BEGIN
        SELECT RAISE(ABORT, 'callback receipts are append-only');
    END
    """,
)

_MIGRATION_0005 = (
    """
    CREATE TRIGGER jobs_artifact_manifest_binding_initially_null
    BEFORE INSERT ON jobs
    FOR EACH ROW
    WHEN NEW.artifact_manifest_path IS NOT NULL
    BEGIN
        SELECT RAISE(ABORT, 'new job cannot be pre-bound to an artifact manifest');
    END
    """,
    """
    CREATE TRIGGER jobs_artifact_manifest_binding_write_once
    BEFORE UPDATE OF artifact_manifest_path ON jobs
    FOR EACH ROW
    WHEN NOT (
        OLD.artifact_manifest_path IS NULL
        AND NEW.artifact_manifest_path IS NOT NULL
        AND OLD.state = 'SUCCEEDED'
        AND NEW.state = 'SUCCEEDED'
    )
    BEGIN
        SELECT RAISE(ABORT, 'job artifact manifest binding is write-once');
    END
    """,
)

_MIGRATION_0006 = (
    """
    CREATE TABLE job_execution_runs (
        company_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        attempt INTEGER NOT NULL CHECK (attempt > 0),
        idempotency_key TEXT NOT NULL,
        external_run_id TEXT,
        executor_name TEXT,
        external_accepted_at TEXT,
        acceptance_observed_at TEXT,
        external_status TEXT CHECK (
            external_status IN (
                'ACCEPTED', 'RUNNING', 'SUCCEEDED',
                'FAILED_RETRYABLE', 'FAILED_FINAL', 'CANCELED'
            )
        ),
        current_stage TEXT,
        next_action_at TEXT,
        submission_attempted_at TEXT,
        completion_observed_at TEXT,
        error_code TEXT,
        error_summary TEXT,
        result_json BLOB,
        result_hash TEXT,
        heartbeat_at TEXT NOT NULL,
        lease_token TEXT,
        lease_expires_at TEXT,
        reconciliation_count INTEGER NOT NULL DEFAULT 0
            CHECK (reconciliation_count >= 0),
        retry_stage_id TEXT NOT NULL DEFAULT 'submission'
            CHECK (retry_stage_id = trim(retry_stage_id) AND retry_stage_id != ''),
        transient_failure_count INTEGER NOT NULL DEFAULT 0
            CHECK (transient_failure_count >= 0),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (company_id, job_id, attempt),
        FOREIGN KEY (company_id, job_id) REFERENCES jobs(company_id, job_id),
        CHECK (
            (
                external_run_id IS NULL
                AND external_accepted_at IS NULL AND acceptance_observed_at IS NULL
            ) OR (
                external_run_id IS NOT NULL AND executor_name IS NOT NULL
                AND external_accepted_at IS NOT NULL AND acceptance_observed_at IS NOT NULL
            )
        ),
        CHECK (
            (lease_token IS NULL AND lease_expires_at IS NULL)
            OR (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
        ),
        CHECK (
            (result_json IS NULL AND result_hash IS NULL)
            OR (result_json IS NOT NULL AND result_hash IS NOT NULL)
        ),
        CHECK (
            idempotency_key = company_id || ':' || job_id || ':' || CAST(attempt AS TEXT)
        ),
        CHECK (
            (submission_attempted_at IS NULL AND executor_name IS NULL)
            OR (submission_attempted_at IS NOT NULL AND executor_name IS NOT NULL)
        ),
        CHECK (executor_name IS NULL OR (executor_name = trim(executor_name) AND executor_name != '')),
        CHECK (external_accepted_at IS NULL OR is_utc_timestamp(external_accepted_at) = 1),
        CHECK (acceptance_observed_at IS NULL OR is_utc_timestamp(acceptance_observed_at) = 1),
        CHECK (
            external_accepted_at IS NULL OR (
                submission_attempted_at IS NOT NULL
                AND submission_attempted_at <= external_accepted_at
                AND external_accepted_at <= acceptance_observed_at
            )
        ),
        CHECK (next_action_at IS NULL OR is_utc_timestamp(next_action_at) = 1),
        CHECK (submission_attempted_at IS NULL OR is_utc_timestamp(submission_attempted_at) = 1),
        CHECK (completion_observed_at IS NULL OR is_utc_timestamp(completion_observed_at) = 1),
        CHECK (is_utc_timestamp(heartbeat_at) = 1),
        CHECK (lease_expires_at IS NULL OR is_utc_timestamp(lease_expires_at) = 1),
        CHECK (is_utc_timestamp(created_at) = 1),
        CHECK (is_utc_timestamp(updated_at) = 1)
    )
    """,
    """
    CREATE INDEX idx_jobs_runner_state_created
    ON jobs(state, created_at, job_id)
    """,
    """
    CREATE INDEX idx_job_execution_runs_due
    ON job_execution_runs(next_action_at, company_id, job_id, attempt)
    """,
    """
    CREATE INDEX idx_job_execution_runs_idempotency
    ON job_execution_runs(company_id, idempotency_key)
    """,
    """
    CREATE TABLE job_stage_retry_budgets (
        company_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        retry_stage_id TEXT NOT NULL
            CHECK (retry_stage_id = trim(retry_stage_id) AND retry_stage_id != ''),
        failure_count INTEGER NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
        created_at TEXT NOT NULL CHECK (is_utc_timestamp(created_at) = 1),
        updated_at TEXT NOT NULL CHECK (is_utc_timestamp(updated_at) = 1),
        PRIMARY KEY (company_id, job_id, retry_stage_id),
        FOREIGN KEY (company_id, job_id) REFERENCES jobs(company_id, job_id)
    )
    """,
    """
    CREATE TRIGGER job_stage_retry_budget_identity_immutable
    BEFORE UPDATE OF company_id, job_id, retry_stage_id, created_at
    ON job_stage_retry_budgets
    FOR EACH ROW
    WHEN NEW.company_id IS NOT OLD.company_id
      OR NEW.job_id IS NOT OLD.job_id
      OR NEW.retry_stage_id IS NOT OLD.retry_stage_id
      OR NEW.created_at IS NOT OLD.created_at
    BEGIN
        SELECT RAISE(ABORT, 'stage retry budget identity is immutable');
    END
    """,
    """
    CREATE TRIGGER job_stage_retry_budget_monotonic
    BEFORE UPDATE OF failure_count ON job_stage_retry_budgets
    FOR EACH ROW
    WHEN NEW.failure_count < OLD.failure_count
    BEGIN
        SELECT RAISE(ABORT, 'stage retry budget cannot decrease');
    END
    """,
    """
    CREATE UNIQUE INDEX idx_job_execution_runs_external_identity
    ON job_execution_runs(executor_name, external_run_id)
    WHERE external_run_id IS NOT NULL;
    """,
    """
    CREATE TRIGGER jobs_runner_timestamp_valid_insert
    BEFORE INSERT ON jobs
    FOR EACH ROW
    WHEN NEW.started_at IS NOT NULL AND is_utc_timestamp(NEW.started_at) != 1
    BEGIN
        SELECT RAISE(ABORT, 'job scheduler timestamp must be canonical UTC');
    END
    """,
    """
    CREATE TRIGGER jobs_runner_timestamp_valid_update
    BEFORE UPDATE OF started_at ON jobs
    FOR EACH ROW
    WHEN NEW.started_at IS NOT NULL AND is_utc_timestamp(NEW.started_at) != 1
    BEGIN
        SELECT RAISE(ABORT, 'job scheduler timestamp must be canonical UTC');
    END
    """,
    """
    CREATE TRIGGER job_execution_runs_identity_immutable
    BEFORE UPDATE OF company_id, job_id, attempt, idempotency_key, created_at
    ON job_execution_runs
    FOR EACH ROW
    WHEN NEW.company_id IS NOT OLD.company_id
      OR NEW.job_id IS NOT OLD.job_id
      OR NEW.attempt IS NOT OLD.attempt
      OR NEW.idempotency_key IS NOT OLD.idempotency_key
      OR NEW.created_at IS NOT OLD.created_at
    BEGIN
        SELECT RAISE(ABORT, 'execution run identity is immutable');
    END
    """,
    """
    CREATE TRIGGER job_execution_runs_result_immutable
    BEFORE UPDATE OF result_json, result_hash ON job_execution_runs
    FOR EACH ROW
    WHEN OLD.result_json IS NOT NULL
      AND (NEW.result_json IS NOT OLD.result_json OR NEW.result_hash IS NOT OLD.result_hash)
    BEGIN
        SELECT RAISE(ABORT, 'execution result is immutable');
    END
    """,
    """
    CREATE TRIGGER job_execution_runs_external_status_monotonic
    BEFORE UPDATE OF external_status ON job_execution_runs
    FOR EACH ROW
    WHEN (
        OLD.external_status IS NOT NULL
        AND NEW.external_status IS NULL
    ) OR (
        OLD.external_status IN (
            'SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_FINAL', 'CANCELED'
        )
        AND NEW.external_status IS NOT OLD.external_status
    ) OR (
        OLD.external_status = 'RUNNING'
        AND NEW.external_status = 'ACCEPTED'
    )
    BEGIN
        SELECT RAISE(ABORT, 'external status cannot regress');
    END
    """,
    """
    CREATE TRIGGER job_execution_runs_external_identity_immutable
    BEFORE UPDATE OF external_run_id, executor_name, external_accepted_at,
                     acceptance_observed_at ON job_execution_runs
    FOR EACH ROW
    WHEN (
        OLD.executor_name IS NOT NULL
        AND NEW.executor_name IS NOT OLD.executor_name
    ) OR (
        OLD.external_run_id IS NOT NULL
        AND (
          NEW.external_run_id IS NOT OLD.external_run_id
          OR NEW.external_accepted_at IS NOT OLD.external_accepted_at
          OR NEW.acceptance_observed_at IS NOT OLD.acceptance_observed_at
        )
    )
    BEGIN
        SELECT RAISE(ABORT, 'external run identity is immutable');
    END
    """,
    """
    CREATE TRIGGER job_execution_runs_external_identity_owned_insert
    BEFORE INSERT ON job_execution_runs
    FOR EACH ROW
    WHEN NEW.external_run_id IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM job_execution_runs AS existing
          WHERE existing.executor_name = NEW.executor_name
            AND existing.external_run_id = NEW.external_run_id
            AND (
                existing.company_id != NEW.company_id
                OR existing.job_id != NEW.job_id
                OR existing.attempt != NEW.attempt
            )
      )
    BEGIN
        SELECT RAISE(ABORT, 'external run belongs to another job');
    END
    """,
    """
    CREATE TRIGGER job_execution_runs_external_identity_owned_update
    BEFORE UPDATE OF external_run_id ON job_execution_runs
    FOR EACH ROW
    WHEN NEW.external_run_id IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM job_execution_runs AS existing
          WHERE existing.executor_name = NEW.executor_name
            AND existing.external_run_id = NEW.external_run_id
            AND (
                existing.company_id != NEW.company_id
                OR existing.job_id != NEW.job_id
                OR existing.attempt != NEW.attempt
            )
      )
    BEGIN
        SELECT RAISE(ABORT, 'external run belongs to another job');
    END
    """,
    """
    CREATE TRIGGER job_execution_runs_append_only
    BEFORE DELETE ON job_execution_runs
    FOR EACH ROW
    BEGIN
        SELECT RAISE(ABORT, 'execution runs are append-only');
    END
    """,
    """
    CREATE TABLE runner_heartbeats (
        runner_id TEXT PRIMARY KEY,
        started_at TEXT NOT NULL,
        heartbeat_at TEXT NOT NULL
    )
    """,
)

_MIGRATIONS = (
    (1, _MIGRATION_0001),
    (2, _MIGRATION_0002),
    (3, _MIGRATION_0003),
    (4, _MIGRATION_0004),
    (5, _MIGRATION_0005),
    (6, _MIGRATION_0006),
)


def _validated_applied_versions(conn: sqlite3.Connection) -> tuple[int, ...]:
    applied = tuple(
        row[0]
        for row in conn.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        )
    )
    known = tuple(version for version, _ in _MIGRATIONS)
    if applied != known[: len(applied)]:
        raise MigrationError
    return applied


def migrate(conn: sqlite3.Connection) -> int:
    """Apply all known migrations and return the latest schema version."""
    if conn.in_transaction:
        raise sqlite3.OperationalError("migrate cannot run inside a caller transaction")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    _validated_applied_versions(conn)
    for version, statements in _MIGRATIONS:
        conn.execute("BEGIN IMMEDIATE")
        try:
            applied_versions = _validated_applied_versions(conn)
            if version not in applied_versions:
                for statement in statements:
                    conn.execute(statement)
                conn.execute(
                    "INSERT INTO schema_migrations(version) VALUES (?)",
                    (version,),
                )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
    return _LATEST_VERSION
