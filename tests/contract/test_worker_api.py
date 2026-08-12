import asyncio
import json
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any

import httpx
import pytest

from seo_orchestrator.api import callback_routes
from seo_orchestrator.api.app import create_app
from seo_orchestrator.db.connection import connect, transaction
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.domain import JobState
from seo_orchestrator.errors import DataIntegrityError, NotFound
from seo_orchestrator.security.signatures import MAX_TIMESTAMP_SKEW_SECONDS, sign_request
from seo_orchestrator.services.artifacts import ArtifactStore, ExecutionResult
from seo_orchestrator.services.company_cards import (
    AudienceData,
    CompanyCardService,
    CompanyProfileData,
    CreateAudience,
    CreateCompany,
    CreateDirection,
    DirectionData,
)
from seo_orchestrator.services.jobs import JobService
from seo_orchestrator.services.snapshots import SnapshotCompiler
from seo_orchestrator.settings import Settings

_TOKEN_HEX = "0f" * 32
FIXTURE_ROOT = Path(__file__).parents[2] / "fixtures" / "companies"
SUCCESS_RESULT_FIXTURE = Path(__file__).parents[2] / "fixtures" / "executions" / "success-result.json"


def _settings(tmp_path: Path) -> Settings:
    token_path = tmp_path / "worker-api.token"
    token_path.write_text(_TOKEN_HEX, encoding="ascii")
    token_path.chmod(0o600)
    callback_key_path = tmp_path / "n8n-callback.key"
    callback_key_path.write_text(_TOKEN_HEX, encoding="ascii")
    callback_key_path.chmod(0o600)
    return Settings(
        environment="test",
        db_path=tmp_path / "worker.db",
        artifact_root=tmp_path / "artifacts",
        listen="unix:/run/seo-orchestrator/worker.sock",
        api_token_path=token_path,
        callback_hmac_key_path=callback_key_path,
    )


def _execution_result() -> ExecutionResult:
    value = json.loads(SUCCESS_RESULT_FIXTURE.read_text(encoding="utf-8"))
    return ExecutionResult(
        content_markdown=value["content_markdown"],
        titles=tuple(value["titles"]),
        descriptions=tuple(value["descriptions"]),
        keyword_qa=value["keyword_qa"],
        text_metrics=value["text_metrics"],
        sources=tuple(value["sources"]),
        warnings=tuple(value["warnings"]),
        model_usage=value["model_usage"],
        stage_timings=value["stage_timings"],
        prompt_versions=value["prompt_versions"],
    )


def _seed_company(database_path: Path) -> None:
    fixture = json.loads((FIXTURE_ROOT / "avtomalyar.json").read_text(encoding="utf-8"))
    profile = fixture["profile"]
    assert isinstance(profile, dict)
    conn = connect(database_path)
    try:
        migrate(conn)
        with transaction(conn):
            CompanyCardService(conn).create_company(
                CreateCompany(
                    company_id=fixture["company_id"],
                    company_profile_id=fixture["company_profile_id"],
                    actor_id="fixture-actor",
                    replacement=CompanyProfileData.model_validate(profile),
                )
            )
    finally:
        conn.close()


def _seed_company_card(database_path: Path) -> None:
    fixture = json.loads((FIXTURE_ROOT / "avtomalyar.json").read_text(encoding="utf-8"))
    direction = fixture["direction"]
    assert isinstance(direction, dict)
    audience = direction["audience"]
    assert isinstance(audience, dict)
    conn = connect(database_path)
    try:
        migrate(conn)
        with transaction(conn):
            cards = CompanyCardService(conn)
            cards.create_company(
                CreateCompany(
                    company_id=fixture["company_id"],
                    company_profile_id=fixture["company_profile_id"],
                    actor_id="fixture-actor",
                    replacement=CompanyProfileData.model_validate(fixture["profile"]),
                )
            )
            cards.create_direction(
                CreateDirection(
                    company_id=fixture["company_id"],
                    company_profile_version=1,
                    direction_id=direction["direction_id"],
                    actor_id="fixture-actor",
                    replacement=DirectionData.model_validate(direction["data"]),
                )
            )
            cards.create_audience(
                CreateAudience(
                    company_id=fixture["company_id"],
                    direction_id=direction["direction_id"],
                    direction_version=1,
                    audience_segment_id=audience["audience_segment_id"],
                    actor_id="fixture-actor",
                    replacement=AudienceData.model_validate(audience["data"]),
                )
            )
    finally:
        conn.close()


def _compile_snapshot(database_path: Path, brief_id: str) -> str:
    conn = connect(database_path)
    try:
        with transaction(conn):
            return SnapshotCompiler(conn, company_id="avtomalyar").compile_snapshot(
                brief_id, prompt_set_version=1
            ).snapshot_id
    finally:
        conn.close()


async def _list_companies(app: Any) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.get("/v1/companies", headers={"authorization": f"Bearer {_TOKEN_HEX}"})


async def _create_company(app: Any, payload: dict[str, Any]) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.post(
            "/v1/companies",
            headers={"authorization": f"Bearer {_TOKEN_HEX}"},
            json=payload,
        )


async def _start_brief(app: Any, company_id: str, actor_id: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.post(
            "/v1/briefs",
            headers={"authorization": f"Bearer {_TOKEN_HEX}"},
            json={"company_id": company_id, "actor_id": actor_id},
        )


async def _update_brief(app: Any, brief_id: str, payload: dict[str, Any]) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.patch(
            f"/v1/briefs/{brief_id}",
            headers={"authorization": f"Bearer {_TOKEN_HEX}"},
            json=payload,
        )


async def _validate_brief(app: Any, brief_id: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.post(
            f"/v1/briefs/{brief_id}/validate",
            headers={"authorization": f"Bearer {_TOKEN_HEX}"},
            json={"company_id": "avtomalyar", "actor_id": "fixture-actor"},
        )


async def _plan_job(app: Any, snapshot_id: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.post(
            "/v1/jobs/plan",
            headers={"authorization": f"Bearer {_TOKEN_HEX}"},
            json={
                "company_id": "avtomalyar",
                "snapshot_id": snapshot_id,
                "execution_plan": {
                    "pipeline_version": "test-pipeline-v1",
                    "executor_name": "test-executor",
                    "model_ids": ["test-model"],
                    "provider_ids": ["test-provider"],
                    "maximum_retries": 1,
                    "cost_currency": None,
                    "cost_min_decimal": None,
                    "cost_max_decimal": None,
                    "unknown_cost_reasons": ["test-only"],
                    "result_destination": "local-artifacts",
                },
            },
        )


async def _get_job(app: Any, job_id: str, company_id: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.get(
            f"/v1/jobs/{job_id}",
            headers={"authorization": f"Bearer {_TOKEN_HEX}"},
            params={"company_id": company_id},
        )


async def _request_paid_approval(app: Any, job_id: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.post(
            f"/v1/jobs/{job_id}/request-paid-approval",
            headers={"authorization": f"Bearer {_TOKEN_HEX}"},
            json={"company_id": "avtomalyar"},
        )


async def _approve_job(app: Any, job_id: str, planned: dict[str, Any]) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.post(
            f"/v1/jobs/{job_id}/approve",
            headers={"authorization": f"Bearer {_TOKEN_HEX}"},
            json={
                "company_id": "avtomalyar",
                "actor_id": "fixture-approver",
                "snapshot_hash": planned["snapshot_hash"],
                "plan_fingerprint": planned["plan_fingerprint"],
            },
        )


async def _cancel_job(app: Any, job_id: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.post(
            f"/v1/jobs/{job_id}/cancel",
            headers={"authorization": f"Bearer {_TOKEN_HEX}"},
            json={"company_id": "avtomalyar", "expected_state": "QUEUED"},
        )


async def _retry_job(app: Any, job_id: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.post(
            f"/v1/jobs/{job_id}/retry",
            headers={"authorization": f"Bearer {_TOKEN_HEX}"},
            json={"company_id": "avtomalyar"},
        )


async def _artifact_content(app: Any, job_id: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.get(
            f"/v1/jobs/{job_id}/artifacts/content",
            headers={"authorization": f"Bearer {_TOKEN_HEX}"},
            params={"company_id": "avtomalyar"},
        )


def _complete_brief_payload(brief_id: str) -> dict[str, Any]:
    return {
        "brief_id": brief_id,
        "actor_id": "fixture-actor",
        "company_id": "avtomalyar",
        "direction_id": "car-painting",
        "direction_version": 1,
        "audience_segment_id": "private-car-owners",
        "audience_version": 1,
        "page_type": "service-page",
        "goal": "Explain an invented vehicle refinishing service.",
        "target_language": "en",
        "locale": "en-US",
        "page_structure": ["Overview", "Process", "Estimate"],
        "primary_keyword": "invented car painting",
        "keywords": ["vehicle refinishing"],
        "lsi_terms": ["surface preparation"],
        "competitor_urls": ["https://example.test/reference"],
        "current_page_context": "Invented current-page source text.",
    }


def _queued_job(tmp_path: Path) -> tuple[Settings, Any, dict[str, Any]]:
    settings = _settings(tmp_path)
    _seed_company_card(settings.db_path)
    app = create_app(settings)
    started = asyncio.run(_start_brief(app, "avtomalyar", "fixture-actor"))
    assert started.status_code == 201
    brief_id = started.json()["brief_id"]
    assert asyncio.run(_update_brief(app, brief_id, _complete_brief_payload(brief_id))).status_code == 200
    assert asyncio.run(_validate_brief(app, brief_id)).status_code == 200
    planned_response = asyncio.run(_plan_job(app, _compile_snapshot(settings.db_path, brief_id)))
    assert planned_response.status_code == 201
    planned = planned_response.json()
    assert asyncio.run(_request_paid_approval(app, planned["job_id"])).status_code == 200
    assert asyncio.run(_approve_job(app, planned["job_id"], planned)).status_code == 200
    return settings, app, planned


async def _post_n8n_callback(
    app: Any, payload: dict[str, Any], nonce: str, *, timestamp: int | None = None
) -> httpx.Response:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    if timestamp is None:
        timestamp = int(time.time())
    signature = sign_request(
        "POST",
        "/v1/callbacks/n8n",
        timestamp,
        nonce,
        body,
        bytes.fromhex(_TOKEN_HEX),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.post(
            "/v1/callbacks/n8n",
            content=body,
            headers={
                "content-type": "application/json",
                "x-seo-timestamp": str(timestamp),
                "x-seo-nonce": nonce,
                "x-seo-idempotency-key": payload["job_id"],
                "x-seo-signature": signature,
            },
        )


async def _post_oversized_n8n_callback(app: Any) -> httpx.Response:
    body = b"x" * (64 * 1024 + 1)
    timestamp = int(time.time())
    nonce = "nonce-oversized-0123456789"
    signature = sign_request(
        "POST",
        "/v1/callbacks/n8n",
        timestamp,
        nonce,
        body,
        bytes.fromhex(_TOKEN_HEX),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.post(
            "/v1/callbacks/n8n",
            content=body,
            headers={
                "content-type": "application/json",
                "x-seo-timestamp": str(timestamp),
                "x-seo-nonce": nonce,
                "x-seo-idempotency-key": "job-one",
                "x-seo-signature": signature,
            },
        )


def test_company_collection_projects_current_active_company_cards(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _seed_company(settings.db_path)

    response = asyncio.run(_list_companies(create_app(settings)))

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["company_id"] == "avtomalyar"
    assert body[0]["company_profile_version"] == 1
    assert body[0]["name"] == "Avtomalyar Studio"
    assert response.headers["x-request-id"]


def test_company_create_validates_and_persists_through_service(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    conn = connect(settings.db_path)
    try:
        migrate(conn)
    finally:
        conn.close()
    fixture = json.loads((FIXTURE_ROOT / "sweet-world.json").read_text(encoding="utf-8"))
    profile = fixture["profile"]
    assert isinstance(profile, dict)

    response = asyncio.run(
        _create_company(
            create_app(settings),
            {
                "company_id": fixture["company_id"],
                "company_profile_id": fixture["company_profile_id"],
                "actor_id": "fixture-actor",
                "replacement": profile,
            },
        )
    )

    assert response.status_code == 201
    assert response.json()["company_id"] == "sweet-world"
    assert response.json()["company_profile_version"] == 1


def test_brief_start_creates_a_company_scoped_draft(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _seed_company(settings.db_path)

    response = asyncio.run(_start_brief(create_app(settings), "avtomalyar", "fixture-actor"))

    assert response.status_code == 201
    body = response.json()
    assert body["company_id"] == "avtomalyar"
    assert body["company_profile_version"] == 1
    assert body["created_by"] == "fixture-actor"
    assert body["status"] == "draft"


def test_brief_update_then_validate_returns_exact_company_scoped_brief(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    _seed_company_card(settings.db_path)
    app = create_app(settings)
    started = asyncio.run(_start_brief(app, "avtomalyar", "fixture-actor"))
    assert started.status_code == 201
    brief_id = started.json()["brief_id"]

    updated = asyncio.run(
        _update_brief(
            app,
            brief_id,
            {
                "brief_id": brief_id,
                "actor_id": "fixture-actor",
                "company_id": "avtomalyar",
                "direction_id": "car-painting",
                "direction_version": 1,
                "audience_segment_id": "private-car-owners",
                "audience_version": 1,
                "page_type": "service-page",
                "goal": "Explain an invented vehicle refinishing service.",
                "target_language": "en",
                "locale": "en-US",
                "page_structure": ["Overview", "Process", "Estimate"],
                "primary_keyword": "invented car painting",
                "keywords": ["vehicle refinishing"],
                "lsi_terms": ["surface preparation"],
                "competitor_urls": ["https://example.test/reference"],
                "current_page_context": "Invented current-page source text.",
            },
        )
    )
    assert updated.status_code == 200
    assert updated.json()["audience_segment_id"] == "private-car-owners"

    validated = asyncio.run(_validate_brief(app, brief_id))

    assert validated.status_code == 200
    assert validated.json()["brief_id"] == brief_id
    assert validated.json()["company_id"] == "avtomalyar"
    assert validated.json()["audience_version"] == 1


def test_job_plan_then_get_is_company_scoped_and_snapshot_hydrated(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _seed_company_card(settings.db_path)
    app = create_app(settings)
    started = asyncio.run(_start_brief(app, "avtomalyar", "fixture-actor"))
    assert started.status_code == 201
    brief_id = started.json()["brief_id"]
    updated = asyncio.run(
        _update_brief(
            app,
            brief_id,
            {
                "brief_id": brief_id,
                "actor_id": "fixture-actor",
                "company_id": "avtomalyar",
                "direction_id": "car-painting",
                "direction_version": 1,
                "audience_segment_id": "private-car-owners",
                "audience_version": 1,
                "page_type": "service-page",
                "goal": "Explain an invented vehicle refinishing service.",
                "target_language": "en",
                "locale": "en-US",
                "page_structure": ["Overview", "Process", "Estimate"],
                "primary_keyword": "invented car painting",
                "keywords": ["vehicle refinishing"],
                "lsi_terms": ["surface preparation"],
                "competitor_urls": ["https://example.test/reference"],
                "current_page_context": "Invented current-page source text.",
            },
        )
    )
    assert updated.status_code == 200
    assert asyncio.run(_validate_brief(app, brief_id)).status_code == 200
    snapshot_id = _compile_snapshot(settings.db_path, brief_id)

    planned = asyncio.run(_plan_job(app, snapshot_id))

    assert planned.status_code == 201
    planned_body = planned.json()
    assert planned_body["snapshot_id"] == snapshot_id
    assert planned_body["state"] == "PLANNED"

    fetched = asyncio.run(_get_job(app, planned_body["job_id"], "avtomalyar"))

    assert fetched.status_code == 200
    assert fetched.json()["job_id"] == planned_body["job_id"]
    assert fetched.json()["company_profile_version"] == 1
    assert fetched.json()["prompt_set_version"] == 1


def test_job_request_paid_approval_then_approve_queues_exact_fingerprints(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    _seed_company_card(settings.db_path)
    app = create_app(settings)
    started = asyncio.run(_start_brief(app, "avtomalyar", "fixture-actor"))
    assert started.status_code == 201
    brief_id = started.json()["brief_id"]
    updated = asyncio.run(
        _update_brief(
            app,
            brief_id,
            {
                "brief_id": brief_id,
                "actor_id": "fixture-actor",
                "company_id": "avtomalyar",
                "direction_id": "car-painting",
                "direction_version": 1,
                "audience_segment_id": "private-car-owners",
                "audience_version": 1,
                "page_type": "service-page",
                "goal": "Explain an invented vehicle refinishing service.",
                "target_language": "en",
                "locale": "en-US",
                "page_structure": ["Overview", "Process", "Estimate"],
                "primary_keyword": "invented car painting",
                "keywords": ["vehicle refinishing"],
                "lsi_terms": ["surface preparation"],
                "competitor_urls": ["https://example.test/reference"],
                "current_page_context": "Invented current-page source text.",
            },
        )
    )
    assert updated.status_code == 200
    assert asyncio.run(_validate_brief(app, brief_id)).status_code == 200
    snapshot_id = _compile_snapshot(settings.db_path, brief_id)
    planned_response = asyncio.run(_plan_job(app, snapshot_id))
    assert planned_response.status_code == 201
    planned = planned_response.json()

    requested = asyncio.run(_request_paid_approval(app, planned["job_id"]))

    assert requested.status_code == 200
    assert requested.json()["state"] == "AWAITING_PAID_APPROVAL"

    approved = asyncio.run(_approve_job(app, planned["job_id"], planned))

    assert approved.status_code == 200
    assert approved.json()["job_id"] == planned["job_id"]
    assert approved.json()["snapshot_hash"] == planned["snapshot_hash"]
    assert approved.json()["plan_fingerprint"] == planned["plan_fingerprint"]
    queued = asyncio.run(_get_job(app, planned["job_id"], "avtomalyar"))
    assert queued.status_code == 200
    assert queued.json()["state"] == "QUEUED"


def test_job_cancel_is_queued_state_aware_and_idempotent(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _seed_company_card(settings.db_path)
    app = create_app(settings)
    started = asyncio.run(_start_brief(app, "avtomalyar", "fixture-actor"))
    assert started.status_code == 201
    brief_id = started.json()["brief_id"]
    updated = asyncio.run(
        _update_brief(
            app,
            brief_id,
            {
                "brief_id": brief_id,
                "actor_id": "fixture-actor",
                "company_id": "avtomalyar",
                "direction_id": "car-painting",
                "direction_version": 1,
                "audience_segment_id": "private-car-owners",
                "audience_version": 1,
                "page_type": "service-page",
                "goal": "Explain an invented vehicle refinishing service.",
                "target_language": "en",
                "locale": "en-US",
                "page_structure": ["Overview", "Process", "Estimate"],
                "primary_keyword": "invented car painting",
                "keywords": ["vehicle refinishing"],
                "lsi_terms": ["surface preparation"],
                "competitor_urls": ["https://example.test/reference"],
                "current_page_context": "Invented current-page source text.",
            },
        )
    )
    assert updated.status_code == 200
    assert asyncio.run(_validate_brief(app, brief_id)).status_code == 200
    planned_response = asyncio.run(_plan_job(app, _compile_snapshot(settings.db_path, brief_id)))
    assert planned_response.status_code == 201
    planned = planned_response.json()
    assert asyncio.run(_request_paid_approval(app, planned["job_id"])).status_code == 200
    assert asyncio.run(_approve_job(app, planned["job_id"], planned)).status_code == 200

    canceled = asyncio.run(_cancel_job(app, planned["job_id"]))

    assert canceled.status_code == 200
    assert canceled.json()["state"] == "CANCELED"
    duplicate = asyncio.run(_cancel_job(app, planned["job_id"]))
    assert duplicate.status_code == 200
    assert duplicate.json()["state"] == "CANCELED"


def test_job_retry_requeues_only_a_retryable_failure(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _seed_company_card(settings.db_path)
    app = create_app(settings)
    started = asyncio.run(_start_brief(app, "avtomalyar", "fixture-actor"))
    assert started.status_code == 201
    brief_id = started.json()["brief_id"]
    updated = asyncio.run(
        _update_brief(
            app,
            brief_id,
            {
                "brief_id": brief_id,
                "actor_id": "fixture-actor",
                "company_id": "avtomalyar",
                "direction_id": "car-painting",
                "direction_version": 1,
                "audience_segment_id": "private-car-owners",
                "audience_version": 1,
                "page_type": "service-page",
                "goal": "Explain an invented vehicle refinishing service.",
                "target_language": "en",
                "locale": "en-US",
                "page_structure": ["Overview", "Process", "Estimate"],
                "primary_keyword": "invented car painting",
                "keywords": ["vehicle refinishing"],
                "lsi_terms": ["surface preparation"],
                "competitor_urls": ["https://example.test/reference"],
                "current_page_context": "Invented current-page source text.",
            },
        )
    )
    assert updated.status_code == 200
    assert asyncio.run(_validate_brief(app, brief_id)).status_code == 200
    planned_response = asyncio.run(_plan_job(app, _compile_snapshot(settings.db_path, brief_id)))
    assert planned_response.status_code == 201
    planned = planned_response.json()
    assert asyncio.run(_request_paid_approval(app, planned["job_id"])).status_code == 200
    assert asyncio.run(_approve_job(app, planned["job_id"], planned)).status_code == 200
    connection = connect(settings.db_path)
    try:
        failed = JobService(connection, company_id="avtomalyar").transition(
            planned["job_id"],
            JobState.QUEUED,
            JobState.FAILED_RETRYABLE,
            "test retryable failure",
        )
    finally:
        connection.close()

    retried = asyncio.run(_retry_job(app, planned["job_id"]))

    assert retried.status_code == 200
    assert retried.json()["state"] == "QUEUED"
    assert retried.json()["attempt"] == failed.attempt + 1
    assert retried.json()["error_summary"] is None


def test_succeeded_job_does_not_serve_artifacts_before_durable_manifest_binding(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    _seed_company_card(settings.db_path)
    app = create_app(settings)
    started = asyncio.run(_start_brief(app, "avtomalyar", "fixture-actor"))
    assert started.status_code == 201
    brief_id = started.json()["brief_id"]
    updated = asyncio.run(
        _update_brief(
            app,
            brief_id,
            {
                "brief_id": brief_id,
                "actor_id": "fixture-actor",
                "company_id": "avtomalyar",
                "direction_id": "car-painting",
                "direction_version": 1,
                "audience_segment_id": "private-car-owners",
                "audience_version": 1,
                "page_type": "service-page",
                "goal": "Explain an invented vehicle refinishing service.",
                "target_language": "en",
                "locale": "en-US",
                "page_structure": ["Overview", "Process", "Estimate"],
                "primary_keyword": "invented car painting",
                "keywords": ["vehicle refinishing"],
                "lsi_terms": ["surface preparation"],
                "competitor_urls": ["https://example.test/reference"],
                "current_page_context": "Invented current-page source text.",
            },
        )
    )
    assert updated.status_code == 200
    assert asyncio.run(_validate_brief(app, brief_id)).status_code == 200
    planned_response = asyncio.run(_plan_job(app, _compile_snapshot(settings.db_path, brief_id)))
    assert planned_response.status_code == 201
    planned = planned_response.json()
    assert asyncio.run(_request_paid_approval(app, planned["job_id"])).status_code == 200
    assert asyncio.run(_approve_job(app, planned["job_id"], planned)).status_code == 200
    connection = connect(settings.db_path)
    try:
        service = JobService(connection, company_id="avtomalyar")
        running = service.transition(
            planned["job_id"], JobState.QUEUED, JobState.RUNNING, "start test execution"
        )
        succeeded = service.transition(
            running.job_id, JobState.RUNNING, JobState.SUCCEEDED, "finish test execution"
        )
    finally:
        connection.close()
    ArtifactStore(settings.artifact_root).write_bundle(succeeded, _execution_result())

    artifact = asyncio.run(_artifact_content(app, succeeded.job_id))

    assert artifact.status_code == 404
    assert artifact.json()["code"] == "NOT_FOUND"


def test_succeeded_job_binding_requires_a_configured_artifact_store(tmp_path: Path) -> None:
    settings, _app, planned = _queued_job(tmp_path)
    connection = connect(settings.db_path)
    try:
        service = JobService(connection, company_id="avtomalyar")
        running = service.transition(
            planned["job_id"], JobState.QUEUED, JobState.RUNNING, "start test execution"
        )
        succeeded = service.transition(
            running.job_id, JobState.RUNNING, JobState.SUCCEEDED, "finish test execution"
        )

        with pytest.raises(ValueError, match="artifact_store"):
            service.bind_artifact_manifest(succeeded.job_id)

        assert service.get_job(succeeded.job_id).artifact_manifest_path is None
    finally:
        connection.close()


def test_artifact_route_delegates_retrieval_to_job_service(
    tmp_path: Path, monkeypatch: Any
) -> None:
    _settings, app, planned = _queued_job(tmp_path)
    calls: list[tuple[str, str]] = []

    def open_artifact(_service: JobService, job_id: str, name: str) -> BytesIO:
        calls.append((job_id, name))
        return BytesIO(b"# Service-owned artifact\n")

    monkeypatch.setattr(JobService, "open_artifact", open_artifact, raising=False)

    response = asyncio.run(_artifact_content(app, planned["job_id"]))

    assert response.status_code == 200
    assert response.text == "# Service-owned artifact\n"
    assert calls == [(planned["job_id"], "content.md")]


def test_succeeded_job_binding_verifies_manifest_provenance_before_write_once(
    tmp_path: Path,
) -> None:
    settings, _app, planned = _queued_job(tmp_path)
    connection = connect(settings.db_path)
    try:
        service = JobService(connection, company_id="avtomalyar")
        running = service.transition(
            planned["job_id"], JobState.QUEUED, JobState.RUNNING, "start test execution"
        )
        succeeded = service.transition(
            running.job_id, JobState.RUNNING, JobState.SUCCEEDED, "finish test execution"
        )
    finally:
        connection.close()
    artifact_store = ArtifactStore(settings.artifact_root)
    artifact_store.write_bundle(
        replace(succeeded, snapshot_hash="d" * 64),
        _execution_result(),
    )
    connection = connect(settings.db_path)
    try:
        service = JobService(
            connection,
            company_id="avtomalyar",
            artifact_store=artifact_store,
        )

        with pytest.raises(DataIntegrityError):
            service.bind_artifact_manifest(succeeded.job_id)

        assert service.get_job(succeeded.job_id).artifact_manifest_path is None
    finally:
        connection.close()


def test_succeeded_job_binding_rejects_missing_manifest_before_write_once(
    tmp_path: Path,
) -> None:
    settings, _app, planned = _queued_job(tmp_path)
    connection = connect(settings.db_path)
    try:
        service = JobService(connection, company_id="avtomalyar")
        running = service.transition(
            planned["job_id"], JobState.QUEUED, JobState.RUNNING, "start test execution"
        )
        succeeded = service.transition(
            running.job_id, JobState.RUNNING, JobState.SUCCEEDED, "finish test execution"
        )
        binding_service = JobService(
            connection,
            company_id="avtomalyar",
            artifact_store=ArtifactStore(settings.artifact_root),
        )

        with pytest.raises(NotFound):
            binding_service.bind_artifact_manifest(succeeded.job_id)

        assert binding_service.get_job(succeeded.job_id).artifact_manifest_path is None
    finally:
        connection.close()


def test_succeeded_job_serves_only_its_durably_bound_artifact_content(tmp_path: Path) -> None:
    settings, app, planned = _queued_job(tmp_path)
    connection = connect(settings.db_path)
    try:
        service = JobService(connection, company_id="avtomalyar")
        running = service.transition(
            planned["job_id"], JobState.QUEUED, JobState.RUNNING, "start test execution"
        )
        succeeded = service.transition(
            running.job_id, JobState.RUNNING, JobState.SUCCEEDED, "finish test execution"
        )
    finally:
        connection.close()
    artifact_store = ArtifactStore(settings.artifact_root)
    artifact_store.write_bundle(succeeded, _execution_result())
    manifest_path = (
        settings.artifact_root
        / "companies"
        / succeeded.company_id
        / "jobs"
        / succeeded.job_id
        / "manifest.json"
    )
    connection = connect(settings.db_path)
    try:
        binding_service = JobService(
            connection,
            company_id="avtomalyar",
            artifact_store=artifact_store,
        )
        bound = binding_service.bind_artifact_manifest(succeeded.job_id)
        rebound = binding_service.bind_artifact_manifest(succeeded.job_id)
    finally:
        connection.close()

    artifact = asyncio.run(_artifact_content(app, succeeded.job_id))

    assert bound.artifact_manifest_path == str(manifest_path)
    assert rebound == bound
    assert artifact.status_code == 200
    assert artifact.headers["content-type"].startswith("text/markdown")
    assert artifact.text == _execution_result().content_markdown


def test_n8n_callback_requires_valid_hmac_correlation_and_durable_nonce(tmp_path: Path) -> None:
    _settings, app, planned = _queued_job(tmp_path)
    payload = {
        "company_id": "avtomalyar",
        "job_id": planned["job_id"],
        "snapshot_hash": planned["snapshot_hash"],
    }
    nonce = "nonce-callback-0123456789"

    accepted = asyncio.run(_post_n8n_callback(app, payload, nonce))

    assert accepted.status_code == 202
    assert accepted.json() == {"status": "accepted"}
    replayed = asyncio.run(_post_n8n_callback(app, payload, nonce))

    assert replayed.status_code == 401
    assert replayed.json()["code"] == "UNAUTHORIZED"


def test_n8n_callback_deduplicates_distinct_valid_nonces_by_idempotency_key(
    tmp_path: Path,
) -> None:
    settings, app, planned = _queued_job(tmp_path)
    payload = {
        "company_id": "avtomalyar",
        "job_id": planned["job_id"],
        "snapshot_hash": planned["snapshot_hash"],
    }

    first = asyncio.run(_post_n8n_callback(app, payload, "nonce-semantic-first-0123456789"))
    second = asyncio.run(_post_n8n_callback(app, payload, "nonce-semantic-second-0123456789"))

    assert first.status_code == 202
    assert second.status_code == 202
    connection = connect(settings.db_path)
    try:
        assert connection.execute(
            """SELECT company_id, job_id, snapshot_hash, idempotency_key
               FROM webhook_callback_receipts"""
        ).fetchall() == [
            (
                "avtomalyar",
                planned["job_id"],
                planned["snapshot_hash"],
                planned["job_id"],
            )
        ]
        assert connection.execute("SELECT COUNT(*) FROM webhook_nonces").fetchone() == (2,)
    finally:
        connection.close()


def test_n8n_callback_deduplicates_after_job_reaches_terminal_state(
    tmp_path: Path,
) -> None:
    settings, app, planned = _queued_job(tmp_path)
    payload = {
        "company_id": "avtomalyar",
        "job_id": planned["job_id"],
        "snapshot_hash": planned["snapshot_hash"],
    }

    first = asyncio.run(_post_n8n_callback(app, payload, "nonce-terminal-first-0123456789"))
    connection = connect(settings.db_path)
    try:
        service = JobService(connection, company_id="avtomalyar")
        running = service.transition(
            planned["job_id"], JobState.QUEUED, JobState.RUNNING, "start test execution"
        )
        service.transition(
            running.job_id, JobState.RUNNING, JobState.SUCCEEDED, "finish test execution"
        )
    finally:
        connection.close()
    duplicate = asyncio.run(
        _post_n8n_callback(app, payload, "nonce-terminal-second-0123456789")
    )

    assert first.status_code == 202
    assert duplicate.status_code == 202
    connection = connect(settings.db_path)
    try:
        assert connection.execute(
            "SELECT COUNT(*) FROM webhook_callback_receipts"
        ).fetchone() == (1,)
        assert connection.execute("SELECT COUNT(*) FROM webhook_nonces").fetchone() == (2,)
    finally:
        connection.close()


def test_n8n_callback_rejects_first_receipt_for_terminal_job(tmp_path: Path) -> None:
    settings, app, planned = _queued_job(tmp_path)
    connection = connect(settings.db_path)
    try:
        service = JobService(connection, company_id="avtomalyar")
        running = service.transition(
            planned["job_id"], JobState.QUEUED, JobState.RUNNING, "start test execution"
        )
        service.transition(
            running.job_id, JobState.RUNNING, JobState.SUCCEEDED, "finish test execution"
        )
    finally:
        connection.close()
    payload = {
        "company_id": "avtomalyar",
        "job_id": planned["job_id"],
        "snapshot_hash": planned["snapshot_hash"],
    }

    rejected = asyncio.run(
        _post_n8n_callback(app, payload, "nonce-terminal-new-0123456789")
    )

    assert rejected.status_code == 401
    connection = connect(settings.db_path)
    try:
        assert connection.execute(
            "SELECT COUNT(*) FROM webhook_callback_receipts"
        ).fetchone() == (0,)
        assert connection.execute("SELECT COUNT(*) FROM webhook_nonces").fetchone() == (0,)
    finally:
        connection.close()


def test_n8n_callback_nonce_covers_the_full_future_signature_window(
    tmp_path: Path, monkeypatch: Any
) -> None:
    _settings, app, planned = _queued_job(tmp_path)
    received_at = datetime(2030, 1, 1, tzinfo=UTC)
    signed_timestamp = int(
        (received_at + timedelta(seconds=MAX_TIMESTAMP_SKEW_SECONDS)).timestamp()
    )
    final_valid_instant = datetime.fromtimestamp(
        signed_timestamp + MAX_TIMESTAMP_SKEW_SECONDS, UTC
    )
    payload = {
        "company_id": "avtomalyar",
        "job_id": planned["job_id"],
        "snapshot_hash": planned["snapshot_hash"],
    }
    nonce = "nonce-future-window-0123456789"

    class FixedClock:
        current = received_at

        @classmethod
        def now(cls, tz: object) -> datetime:
            assert tz is UTC
            return cls.current

    monkeypatch.setattr(callback_routes, "datetime", FixedClock)
    monkeypatch.setattr(callback_routes.time, "time", lambda: received_at.timestamp())
    accepted = asyncio.run(
        _post_n8n_callback(app, payload, nonce, timestamp=signed_timestamp)
    )

    assert accepted.status_code == 202
    FixedClock.current = final_valid_instant
    monkeypatch.setattr(
        callback_routes.time, "time", lambda: final_valid_instant.timestamp()
    )
    replayed = asyncio.run(
        _post_n8n_callback(app, payload, nonce, timestamp=signed_timestamp)
    )

    assert replayed.status_code == 401
    assert replayed.json()["code"] == "UNAUTHORIZED"


def test_n8n_callback_rejects_wrong_correlation_without_burning_nonce(tmp_path: Path) -> None:
    _settings, app, planned = _queued_job(tmp_path)
    nonce = "nonce-correlation-0123456789"
    mismatched = asyncio.run(
        _post_n8n_callback(
            app,
            {
                "company_id": "avtomalyar",
                "job_id": planned["job_id"],
                "snapshot_hash": "0" * 64,
            },
            nonce,
        )
    )
    assert mismatched.status_code == 401

    accepted = asyncio.run(
        _post_n8n_callback(
            app,
            {
                "company_id": "avtomalyar",
                "job_id": planned["job_id"],
                "snapshot_hash": planned["snapshot_hash"],
            },
            nonce,
        )
    )

    assert accepted.status_code == 202


def test_n8n_callback_rejects_oversized_body_before_parsing(tmp_path: Path) -> None:
    response = asyncio.run(_post_oversized_n8n_callback(create_app(_settings(tmp_path))))

    assert response.status_code == 401
    assert response.json()["code"] == "UNAUTHORIZED"
