"""Restart-safe deterministic staged executor for explicit local mock runs."""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import cast

from seo_orchestrator.canonical import JsonValue, sha256_fingerprint
from seo_orchestrator.domain import ExecutionSnapshot, SeoJob
from seo_orchestrator.executors.base import (
    ExecutionStatus,
    ExternalRun,
    ExternalStatus,
    execution_result_bytes,
    execution_result_from_bytes,
)
from seo_orchestrator.executors.mock import MockExecutor
from seo_orchestrator.services.artifacts import ExecutionResult

PIPELINE_VERSION = "staged-mock-v1"
STAGE_IDS = (
    "freeze_context",
    "acquire_sources",
    "normalize_sources",
    "analyze_competitors",
    "research_facts",
    "merge_evidence",
    "build_outline",
    "write_draft",
    "deterministic_qa",
    "critic_review",
    "reader_review",
    "revise_draft",
    "build_metadata",
    "freeze_artifact",
)
_STAGED_UPDATE_GUARD = "staged_mock_result_immutable"
_STAGED_DELETE_GUARD = "staged_mock_result_delete_guard"
_STAGED_INSERT_GUARD = "staged_mock_result_insert_guard"
_LIFECYCLE_UPDATE_GUARD = "staged_mock_identity_immutable"
_LIFECYCLE_DELETE_GUARD = "staged_mock_identity_delete_guard"
_LIFECYCLE_INSERT_GUARD = "staged_mock_identity_insert_guard"
_GUARD_DEFINITIONS = {
    _STAGED_UPDATE_GUARD: f"""CREATE TRIGGER {_STAGED_UPDATE_GUARD}
        BEFORE UPDATE OF idempotency_key, external_run_id, pipeline_version,
                         identity_hash, result_json, result_hash, result_commitment
        ON staged_mock_runs
        BEGIN
            SELECT RAISE(ABORT, 'immutable staged mock result');
        END""",
    _STAGED_DELETE_GUARD: f"""CREATE TRIGGER {_STAGED_DELETE_GUARD}
        BEFORE DELETE ON staged_mock_runs
        BEGIN
            SELECT RAISE(ABORT, 'immutable staged mock result');
        END""",
    _STAGED_INSERT_GUARD: f"""CREATE TRIGGER {_STAGED_INSERT_GUARD}
        BEFORE INSERT ON staged_mock_runs
        WHEN EXISTS (
            SELECT 1 FROM staged_mock_runs
            WHERE idempotency_key = NEW.idempotency_key
               OR external_run_id = NEW.external_run_id
        )
        BEGIN
            SELECT RAISE(ABORT, 'immutable staged mock result');
        END""",
    _LIFECYCLE_UPDATE_GUARD: f"""CREATE TRIGGER {_LIFECYCLE_UPDATE_GUARD}
        BEFORE UPDATE OF idempotency_key, identity_json, external_run_id, accepted_at
        ON mock_executor_runs
        BEGIN
            SELECT RAISE(ABORT, 'immutable staged mock identity');
        END""",
    _LIFECYCLE_DELETE_GUARD: f"""CREATE TRIGGER {_LIFECYCLE_DELETE_GUARD}
        BEFORE DELETE ON mock_executor_runs
        BEGIN
            SELECT RAISE(ABORT, 'immutable staged mock identity');
        END""",
    _LIFECYCLE_INSERT_GUARD: f"""CREATE TRIGGER {_LIFECYCLE_INSERT_GUARD}
        BEFORE INSERT ON mock_executor_runs
        WHEN EXISTS (
            SELECT 1 FROM mock_executor_runs
            WHERE idempotency_key = NEW.idempotency_key
               OR external_run_id = NEW.external_run_id
        )
        BEGIN
            SELECT RAISE(ABORT, 'immutable staged mock identity');
        END""",
}


def _mapping(value: object) -> dict[str, JsonValue]:
    if type(value) is not dict:
        raise ValueError("snapshot context must contain JSON objects")
    return cast(dict[str, JsonValue], value)


def _text(mapping: dict[str, JsonValue], field_name: str) -> str:
    value = mapping.get(field_name)
    if type(value) is not str or not value.strip():
        raise ValueError(f"snapshot context {field_name} must be a non-empty string")
    return value.strip()


def _sections(mapping: dict[str, JsonValue]) -> tuple[str, ...]:
    value = mapping.get("page_structure")
    if type(value) is not list or not value:
        raise ValueError("snapshot context page_structure must be a non-empty array")
    if any(type(section) is not str or not section.strip() for section in value):
        raise ValueError("snapshot context page_structure must contain non-empty strings")
    return tuple(cast(str, section).strip() for section in value)


def build_staged_mock_result(
    snapshot: ExecutionSnapshot,
    *,
    model_id: str,
    provider_id: str,
) -> ExecutionResult:
    """Build a truthful deterministic artifact without performing external work."""
    if not isinstance(snapshot, ExecutionSnapshot):
        raise TypeError("snapshot must be an ExecutionSnapshot")
    for field_name, value in (("model_id", model_id), ("provider_id", provider_id)):
        if type(value) is not str or not value.strip():
            raise ValueError(f"{field_name} must be a non-empty string")

    context = _mapping(snapshot.thawed_compiled_context())
    if sha256_fingerprint(context) != snapshot.snapshot_hash:
        raise ValueError("snapshot context hash does not match frozen snapshot")
    company = _mapping(context.get("company"))
    direction = _mapping(context.get("direction"))
    audience = _mapping(context.get("audience"))
    brief = _mapping(context.get("brief"))
    company_name = _text(company, "name")
    direction_name = _text(direction, "name")
    audience_name = _text(audience, "name")
    keyword = _text(brief, "primary_keyword")
    goal = _text(brief, "goal")
    sections = _sections(brief)

    outline = "\n".join(f"- {section}" for section in sections)
    content = (
        f"# {keyword.capitalize()}\n\n"
        f"Локальный mock-черновик для {company_name}. "
        f"Направление: {direction_name}. Аудитория: {audience_name}.\n\n"
        f"Цель материала: {goal}.\n\n"
        f"## Структура\n\n{outline}\n\n"
        f"## Контроль\n\n"
        f"Материал по теме «{keyword}» требует проверки фактов и источников "
        "перед внешним использованием. В этом запуске сеть и внешние провайдеры "
        "не использовались.\n"
    )
    titles = cast(
        tuple[str, str, str, str, str],
        tuple(
            f"{keyword}: {suffix}"
            for suffix in (
                "проверяемый порядок",
                "структура материала",
                "ключевые этапы",
                "контроль результата",
                "практический план",
            )
        ),
    )
    descriptions = cast(
        tuple[str, str, str, str, str],
        tuple(
            f"{keyword.capitalize()}: {suffix}. Локальный mock-результат без внешних источников."
            for suffix in (
                "последовательность действий",
                "структура и критерии",
                "этапы подготовки",
                "проверка результата",
                "практический подход",
            )
        ),
    )
    occurrences = content.casefold().count(keyword.casefold())
    return ExecutionResult(
        content_markdown=content,
        titles=titles,
        descriptions=descriptions,
        keyword_qa={
            "primary_keyword": keyword,
            "occurrences": occurrences,
            "passed": occurrences > 0,
        },
        text_metrics={
            "characters": len(content),
            "words": len(content.split()),
        },
        sources=(),
        warnings=("mock execution did not fetch external sources",),
        model_usage={
            "models": [
                {
                    "model_id": model_id,
                    "provider_id": provider_id,
                    "input_tokens": 0,
                    "output_tokens": 0,
                }
            ]
        },
        stage_timings={stage_id: 0 for stage_id in STAGE_IDS},
        prompt_versions={"pipeline": PIPELINE_VERSION},
    )


class StagedMockExecutor:
    """Durable local-only stage progression over the trusted mock lifecycle boundary."""

    name = "mock"
    pipeline_version = PIPELINE_VERSION

    def __init__(
        self,
        *,
        state_path: Path,
        clock: Callable[[], datetime] | None = None,
        run_id_factory: Callable[[int], str] | None = None,
        model_ids: tuple[str, ...] = ("writer-model-v1",),
        provider_ids: tuple[str, ...] = ("mock-provider",),
    ) -> None:
        if not isinstance(state_path, Path):
            raise TypeError("state_path must be a Path")
        self._state_path = state_path.resolve(strict=False)
        self._lifecycle = MockExecutor(
            clock=clock,
            run_id_factory=run_id_factory,
            state_path=self._state_path,
            model_ids=model_ids,
            provider_ids=provider_ids,
        )
        self.model_ids = self._lifecycle.model_ids
        self.provider_ids = self._lifecycle.provider_ids
        with self._connection() as connection:
            connection.execute(
                f"""CREATE TABLE IF NOT EXISTS staged_mock_runs (
                       idempotency_key TEXT PRIMARY KEY,
                       external_run_id TEXT NOT NULL UNIQUE,
                       pipeline_version TEXT NOT NULL,
                       identity_hash TEXT NOT NULL,
                       result_json BLOB NOT NULL,
                       result_hash TEXT NOT NULL,
                       result_commitment TEXT NOT NULL,
                       next_stage_index INTEGER NOT NULL DEFAULT 0
                           CHECK (next_stage_index >= 0 AND next_stage_index <= {len(STAGE_IDS)})
                   )"""
            )
            for trigger_sql in _GUARD_DEFINITIONS.values():
                connection.execute(trigger_sql.replace("CREATE TRIGGER", "CREATE TRIGGER IF NOT EXISTS", 1))
            self._verify_guards(connection)

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self._state_path, isolation_level=None)
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA recursive_triggers = ON")
        try:
            yield connection
            if connection.in_transaction:
                connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    @property
    def durable_semantic_idempotency(self) -> bool:
        return self._lifecycle.durable_semantic_idempotency

    @property
    def side_effect_free_lookup(self) -> bool:
        return self._lifecycle.side_effect_free_lookup

    @property
    def idempotent_cancel(self) -> bool:
        return self._lifecycle.idempotent_cancel

    @property
    def cancel_confirms_terminal(self) -> bool:
        return self._lifecycle.cancel_confirms_terminal

    @property
    def authority_deadline_enforced(self) -> bool:
        return self._lifecycle.authority_deadline_enforced

    @property
    def configuration_authorization_enforced(self) -> bool:
        return self._lifecycle.configuration_authorization_enforced

    def _result_bytes(self, snapshot: ExecutionSnapshot) -> bytes:
        result = build_staged_mock_result(
            snapshot,
            model_id=self.model_ids[0],
            provider_id=self.provider_ids[0],
        )
        return execution_result_bytes(result)

    def validate_result(
        self,
        job: SeoJob,
        snapshot: ExecutionSnapshot,
        result: ExecutionResult,
    ) -> None:
        """Rebuild the exact result from the authoritative runner snapshot."""
        if (
            job.company_id != snapshot.company_id
            or job.snapshot_id != snapshot.snapshot_id
            or job.snapshot_hash != snapshot.snapshot_hash
            or execution_result_bytes(result) != self._result_bytes(snapshot)
        ):
            raise ValueError("staged mock result does not match authoritative snapshot")

    @staticmethod
    def _identity_commitment(identity_json: str, payload: bytes) -> tuple[str, str]:
        identity_bytes = identity_json.encode("utf-8")
        identity_hash = hashlib.sha256(identity_bytes).hexdigest()
        digest = hashlib.sha256()
        for value in (identity_bytes, PIPELINE_VERSION.encode("ascii"), payload):
            digest.update(len(value).to_bytes(8, "big"))
            digest.update(value)
        return identity_hash, digest.hexdigest()

    @staticmethod
    def _verify_guards(connection: sqlite3.Connection) -> None:
        rows = connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'",
        ).fetchall()
        actual = {
            name: " ".join(sql.split())
            for name, sql in rows
            if name in _GUARD_DEFINITIONS and type(sql) is str
        }
        expected = {
            name: " ".join(sql.split()) for name, sql in _GUARD_DEFINITIONS.items()
        }
        if actual != expected:
            raise ValueError("staged mock state integrity guards are missing")

    def _initialize(self, run: ExternalRun, snapshot: ExecutionSnapshot) -> None:
        payload = self._result_bytes(snapshot)
        result_hash = hashlib.sha256(payload).hexdigest()
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._verify_guards(connection)
            lifecycle = connection.execute(
                """SELECT identity_json FROM mock_executor_runs
                   WHERE idempotency_key = ? AND external_run_id = ?""",
                (run.idempotency_key, run.external_run_id),
            ).fetchone()
            if lifecycle is None or type(lifecycle[0]) is not str or not lifecycle[0]:
                raise ValueError("staged mock lifecycle identity is invalid")
            identity_hash, result_commitment = self._identity_commitment(
                lifecycle[0], payload
            )
            row = connection.execute(
                """SELECT external_run_id, pipeline_version, identity_hash, result_json,
                          result_hash, result_commitment, next_stage_index
                   FROM staged_mock_runs WHERE idempotency_key = ?""",
                (run.idempotency_key,),
            ).fetchone()
            if row is None:
                connection.execute(
                    """INSERT INTO staged_mock_runs(
                           idempotency_key, external_run_id, pipeline_version, identity_hash,
                           result_json, result_hash, result_commitment, next_stage_index
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, 0)""",
                    (
                        run.idempotency_key,
                        run.external_run_id,
                        PIPELINE_VERSION,
                        identity_hash,
                        payload,
                        result_hash,
                        result_commitment,
                    ),
                )
                return
            if (
                row[0] != run.external_run_id
                or row[1] != PIPELINE_VERSION
                or row[2] != identity_hash
                or row[3] != payload
                or row[4] != result_hash
                or row[5] != result_commitment
                or type(row[6]) is not int
                or not 0 <= row[6] <= len(STAGE_IDS)
            ):
                raise ValueError("staged mock state does not match immutable execution")

    def submit(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun:
        run = self._lifecycle.submit(job, snapshot)
        self._initialize(run, snapshot)
        return run

    def submit_authorized(
        self,
        job: SeoJob,
        snapshot: ExecutionSnapshot,
        *,
        authority_expires_at: datetime | None,
        approved_model_ids: tuple[str, ...],
        approved_provider_ids: tuple[str, ...],
    ) -> ExternalRun:
        run = self._lifecycle.submit_authorized(
            job,
            snapshot,
            authority_expires_at=authority_expires_at,
            approved_model_ids=approved_model_ids,
            approved_provider_ids=approved_provider_ids,
        )
        self._initialize(run, snapshot)
        return run

    def lookup(self, job: SeoJob, snapshot: ExecutionSnapshot) -> ExternalRun | None:
        run = self._lifecycle.lookup(job, snapshot)
        if run is not None:
            self._initialize(run, snapshot)
        return run

    @staticmethod
    def _invalid_state(exc: Exception | None = None) -> ValueError:
        error = ValueError("staged mock state is invalid")
        if exc is not None:
            error.__cause__ = exc
        return error

    def poll(self, run: ExternalRun) -> ExecutionStatus:
        lifecycle_status = self._lifecycle.poll(run)
        if lifecycle_status.status is ExternalStatus.CANCELED:
            return lifecycle_status
        if lifecycle_status.status is not ExternalStatus.RUNNING:
            raise self._invalid_state()

        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._verify_guards(connection)
            row = connection.execute(
                """SELECT staged.external_run_id, staged.pipeline_version,
                          staged.identity_hash, staged.result_json, staged.result_hash,
                          staged.result_commitment, staged.next_stage_index,
                          lifecycle.identity_json
                   FROM staged_mock_runs AS staged
                   JOIN mock_executor_runs AS lifecycle USING (idempotency_key)
                   WHERE staged.idempotency_key = ?""",
                (run.idempotency_key,),
            ).fetchone()
            if row is None:
                raise self._invalid_state()
            (
                external_run_id,
                pipeline_version,
                identity_hash,
                payload,
                result_hash,
                result_commitment,
                cursor,
                identity_json,
            ) = row
            if type(identity_json) is not str or not identity_json:
                raise self._invalid_state()
            expected_identity_hash, expected_commitment = self._identity_commitment(
                identity_json, payload
            )
            if (
                external_run_id != run.external_run_id
                or pipeline_version != PIPELINE_VERSION
                or identity_hash != expected_identity_hash
                or type(payload) is not bytes
                or type(result_hash) is not str
                or result_commitment != expected_commitment
                or type(cursor) is not int
                or not 0 <= cursor <= len(STAGE_IDS)
                or hashlib.sha256(payload).hexdigest() != result_hash
            ):
                raise self._invalid_state()
            try:
                result = execution_result_from_bytes(payload)
            except (TypeError, ValueError) as exc:
                raise self._invalid_state(exc) from exc
            if cursor < len(STAGE_IDS):
                stage_id = STAGE_IDS[cursor]
                updated = connection.execute(
                    """UPDATE staged_mock_runs SET next_stage_index = ?
                       WHERE idempotency_key = ? AND next_stage_index = ?""",
                    (cursor + 1, run.idempotency_key, cursor),
                )
                if updated.rowcount != 1:
                    raise self._invalid_state()
                return ExecutionStatus(
                    external_run_id=run.external_run_id,
                    status=ExternalStatus.RUNNING,
                    stage_id=stage_id,
                    retry_after_seconds=0,
                    error_code=None,
                    error_summary=None,
                    result=None,
                )
            return ExecutionStatus(
                external_run_id=run.external_run_id,
                status=ExternalStatus.SUCCEEDED,
                stage_id="complete",
                retry_after_seconds=None,
                error_code=None,
                error_summary=None,
                result=result,
            )

    def cancel(self, run: ExternalRun) -> ExecutionStatus:
        return self._lifecycle.cancel(run)
