"""Stateful local implementation of the frozen n8n wrapper contract for tests."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, cast

import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
)

from seo_orchestrator.canonical import JsonValue, sha256_fingerprint
from seo_orchestrator.domain import ExecutionSnapshot
from seo_orchestrator.security.signatures import SignatureVerificationError, verify_request

Identifier = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9-]{1,63}$", strict=True)]
Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$", strict=True)]
ConfigId = Annotated[str, StringConstraints(min_length=1, max_length=128, strict=True)]


class SubmitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    job_id: Identifier
    brief_id: Identifier
    brief_fingerprint: Sha256Hex
    snapshot_hash: Sha256Hex
    attempt: Annotated[int, Field(strict=True, ge=1)]
    approved_plan_fingerprint: Sha256Hex
    approval_record_id: Identifier
    authority_expires_at: datetime
    approved_model_ids: tuple[ConfigId, ...]
    approved_provider_ids: tuple[ConfigId, ...]
    execution_snapshot: dict[str, object]

    @field_validator("approved_model_ids", "approved_provider_ids")
    @classmethod
    def validate_unique_nonempty_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(set(value)) != len(value):
            raise ValueError("approved configuration IDs must be nonempty and unique")
        return value


class MockN8nApp:
    def __init__(
        self,
        key: bytes,
        *,
        now: datetime,
        state_path: Path,
        model_ids: tuple[str, ...] = ("writer-model-v1",),
        provider_ids: tuple[str, ...] = ("n8n-provider",),
    ) -> None:
        self._key = key
        self._now = now.astimezone(UTC)
        self._state_path = state_path
        self._model_ids = model_ids
        self._provider_ids = provider_ids
        with self._connection() as connection:
            connection.executescript(
                """CREATE TABLE IF NOT EXISTS nonces (nonce TEXT PRIMARY KEY);
                   CREATE TABLE IF NOT EXISTS runs (
                       idempotency_key TEXT PRIMARY KEY,
                       request_body BLOB NOT NULL,
                       external_run_id TEXT NOT NULL UNIQUE,
                       accepted_at TEXT NOT NULL,
                       canceled INTEGER NOT NULL DEFAULT 0 CHECK (canceled IN (0, 1))
                   );"""
            )

    def _connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._state_path)
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def _consume_nonce(self, nonce: str) -> bool:
        with self._connection() as connection:
            cursor = connection.execute(
                "INSERT INTO nonces(nonce) VALUES (?) ON CONFLICT(nonce) DO NOTHING",
                (nonce,),
            )
            return cursor.rowcount == 1

    def __call__(self, request: httpx.Request) -> httpx.Response:
        try:
            verify_request(
                request.method,
                request.url.path,
                int(request.headers["x-seo-timestamp"]),
                request.headers["x-seo-nonce"],
                request.content,
                self._key,
                request.headers["x-seo-signature"],
                now=int(self._now.timestamp()),
                nonce_consumer=self._consume_nonce,
            )
        except (KeyError, ValueError, SignatureVerificationError):
            return httpx.Response(401, json={"error": "unauthorized"})
        key = request.headers["x-seo-idempotency-key"]
        path = request.url.path
        if request.method == "POST" and path == "/v1/executions":
            try:
                payload = SubmitRequest.model_validate_json(request.content)
                snapshot_value = payload.execution_snapshot
                snapshot_input = dict(snapshot_value)
                snapshot_input["created_at"] = datetime.fromisoformat(
                    cast(str, snapshot_input["created_at"])
                )
                snapshot = ExecutionSnapshot.model_validate(snapshot_input)
                normalized_snapshot = snapshot.model_dump(mode="json")
                context = cast(dict[str, object], normalized_snapshot["compiled_context"])
                if set(context) != {
                    "schema_version",
                    "company",
                    "direction",
                    "audience",
                    "brief",
                    "prompt_set_version",
                }:
                    raise ValueError("unexpected compiled-context fields")
                company = cast(dict[str, object], context["company"])
                direction = cast(dict[str, object], context["direction"])
                audience = cast(dict[str, object], context["audience"])
                brief = cast(dict[str, object], context["brief"])
                expected_key = f"{snapshot.company_id}:{payload.job_id}:{payload.attempt}"
                expires_at = payload.authority_expires_at
                authorized = (
                    key == expected_key
                    and payload.brief_id == snapshot.brief_id == brief.get("brief_id")
                    and payload.snapshot_hash == snapshot.snapshot_hash
                    and snapshot.snapshot_hash
                    == sha256_fingerprint(
                        cast(JsonValue, normalized_snapshot["compiled_context"])
                    )
                    and payload.brief_fingerprint
                    == sha256_fingerprint(cast(JsonValue, brief))
                    and context["schema_version"] == 1
                    and context["prompt_set_version"] == snapshot.prompt_set_version
                    and company.get("company_id") == snapshot.company_id
                    and company.get("company_profile_version")
                    == snapshot.company_profile_version
                    and direction.get("direction_id") == snapshot.direction_id
                    and direction.get("company_id") == snapshot.company_id
                    and direction.get("company_profile_version")
                    == snapshot.company_profile_version
                    and direction.get("direction_version") == snapshot.direction_version
                    and audience.get("audience_segment_id")
                    == snapshot.audience_segment_id
                    and audience.get("company_id") == snapshot.company_id
                    and audience.get("direction_id") == snapshot.direction_id
                    and audience.get("direction_version") == snapshot.direction_version
                    and audience.get("audience_version") == snapshot.audience_version
                    and brief.get("company_id") == snapshot.company_id
                    and brief.get("company_profile_version")
                    == snapshot.company_profile_version
                    and brief.get("direction_id") == snapshot.direction_id
                    and brief.get("direction_version") == snapshot.direction_version
                    and brief.get("audience_segment_id") == snapshot.audience_segment_id
                    and brief.get("audience_version") == snapshot.audience_version
                    and expires_at.tzinfo is not None
                    and expires_at.utcoffset() is not None
                    and self._now < expires_at.astimezone(UTC)
                    and payload.approved_model_ids == self._model_ids
                    and payload.approved_provider_ids == self._provider_ids
                )
            except (KeyError, TypeError, ValidationError, ValueError):
                authorized = False
            if not authorized:
                return httpx.Response(403, json={"error": "unauthorized_execution"})
            with self._connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """SELECT request_body, external_run_id, accepted_at
                       FROM runs WHERE idempotency_key = ?""",
                    (key,),
                ).fetchone()
                if existing is not None:
                    if existing[0] != request.content:
                        return httpx.Response(409, json={"error": "idempotency_conflict"})
                    run = {
                        "external_run_id": existing[1],
                        "idempotency_key": key,
                        "accepted_at": existing[2],
                    }
                    return httpx.Response(202, json=run)
                sequence = int(connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0]) + 1
                run = {
                    "external_run_id": f"n8n-run-{sequence}",
                    "idempotency_key": key,
                    "accepted_at": self._now.isoformat().replace("+00:00", "Z"),
                }
                connection.execute(
                    """INSERT INTO runs(
                           idempotency_key, request_body, external_run_id, accepted_at
                       ) VALUES (?, ?, ?, ?)""",
                    (key, request.content, run["external_run_id"], run["accepted_at"]),
                )
            return httpx.Response(202, json=run)
        if request.method == "POST" and path == "/v1/executions/lookup":
            value = json.loads(request.content)
            lookup_key = cast(dict[str, object], value).get("idempotency_key")
            with self._connection() as connection:
                existing = connection.execute(
                    """SELECT external_run_id, accepted_at FROM runs
                       WHERE idempotency_key = ?""",
                    (lookup_key,),
                ).fetchone()
            if existing is None:
                return httpx.Response(404)
            return httpx.Response(
                200,
                json={
                    "external_run_id": existing[0],
                    "idempotency_key": lookup_key,
                    "accepted_at": existing[1],
                },
            )
        prefix = "/v1/executions/"
        if not path.startswith(prefix):
            return httpx.Response(404)
        suffix = path.removeprefix(prefix)
        cancel = suffix.endswith("/cancel")
        run_id = suffix.removesuffix("/cancel") if cancel else suffix
        if (cancel and request.method != "POST") or (
            not cancel and request.method != "GET"
        ):
            return httpx.Response(405, json={"error": "method_not_allowed"})
        with self._connection() as connection:
            row = connection.execute(
                """SELECT idempotency_key, canceled FROM runs
                   WHERE external_run_id = ?""",
                (run_id,),
            ).fetchone()
        if row is None:
            return httpx.Response(404)
        if row[0] != key:
            return httpx.Response(403, json={"error": "run_correlation_failed"})
        if cancel:
            with self._connection() as connection:
                connection.execute(
                    "UPDATE runs SET canceled = 1 WHERE external_run_id = ?", (run_id,)
                )
            row = (row[0], 1)
        status = "CANCELED" if row[1] else "RUNNING"
        return httpx.Response(
            200,
            json={
                "external_run_id": run_id,
                "status": status,
                "stage_id": None,
                "retry_after_seconds": None,
                "error_code": None,
                "error_summary": None,
                "result": None,
            },
        )
