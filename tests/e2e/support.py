"""Shared local-only harness for Stage B end-to-end acceptance tests."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from seo_orchestrator.api.app import create_app
from seo_orchestrator.db.connection import connect, transaction
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.domain import JobState
from seo_orchestrator.executors.base import ExecutionStatus, ExternalStatus
from seo_orchestrator.executors.mock import MockExecutor
from seo_orchestrator.runner import Runner
from seo_orchestrator.services.artifacts import ArtifactStore, ExecutionResult
from seo_orchestrator.services.company_cards import (
    AudienceData,
    CompanyCardService,
    CreateAudience,
    CreateDirection,
    DirectionData,
)
from seo_orchestrator.services.jobs import JobService
from seo_orchestrator.services.notifications import (
    DeliveryReceipt,
    NotificationConfig,
    NotificationEvent,
    NullNotificationSink,
    build_notification_sink,
)
from seo_orchestrator.services.snapshots import SnapshotCompiler
from seo_orchestrator.settings import Settings

NOW = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
TOKEN_HEX = "3a" * 32
FIXTURE_ROOT = Path(__file__).parents[2] / "fixtures" / "companies"
PIPELINE_VERSION = "local-e2e-pipeline-v1"


@dataclass(frozen=True, slots=True)
class PlannedFlow:
    company_id: str
    direction_id: str
    audience_id: str
    company_name: str
    direction_name: str
    brief_id: str
    snapshot_id: str
    snapshot_hash: str
    job_id: str
    plan_fingerprint: str


class MutationGuardExecutor(MockExecutor):
    """Durable mock that counts calls to its real configuration boundary."""

    def __init__(
        self,
        *,
        state_path: Path,
        outcomes: tuple[ExecutionStatus, ...],
    ) -> None:
        self.configuration_call_count = 0
        super().__init__(
            outcomes=outcomes,
            clock=lambda: datetime.now(UTC),
            run_id_factory=lambda number: f"local-run-{number}",
            state_path=state_path,
        )
    def configure_durable_state(self, state_path: Path) -> None:
        self.configuration_call_count += 1
        super().configure_durable_state(state_path)


def load_company_fixture(company_id: str) -> dict[str, Any]:
    value = json.loads((FIXTURE_ROOT / f"{company_id}.json").read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError("company fixture must be an object")
    return value


def make_settings(tmp_path: Path) -> Settings:
    token_path = tmp_path / "worker-api.token"
    token_path.write_text(TOKEN_HEX, encoding="ascii")
    token_path.chmod(0o600)
    callback_key_path = tmp_path / "callback-hmac.key"
    callback_key_path.write_text(TOKEN_HEX, encoding="ascii")
    callback_key_path.chmod(0o600)
    settings = Settings(
        environment="test",
        db_path=tmp_path / "worker.db",
        artifact_root=tmp_path / "artifacts",
        listen="unix:/run/seo-orchestrator/worker.sock",
        api_token_path=token_path,
        callback_hmac_key_path=callback_key_path,
    )
    connection = connect(settings.db_path)
    try:
        migrate(connection)
    finally:
        connection.close()
    return settings


async def _request_async(
    app: Any,
    method: str,
    path: str,
    *,
    json_body: dict[str, Any] | None = None,
    params: dict[str, str] | None = None,
) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://worker.test",
    ) as client:
        return await client.request(
            method,
            path,
            headers={"authorization": f"Bearer {TOKEN_HEX}"},
            json=json_body,
            params=params,
        )


def api_request(
    app: Any,
    method: str,
    path: str,
    *,
    json_body: dict[str, Any] | None = None,
    params: dict[str, str] | None = None,
    expected_status: int = 200,
) -> httpx.Response:
    response = asyncio.run(
        _request_async(app, method, path, json_body=json_body, params=params)
    )
    assert response.status_code == expected_status, response.text
    return response


def create_company_card(app: Any, settings: Settings, fixture: dict[str, Any]) -> None:
    company_id = str(fixture["company_id"])
    api_request(
        app,
        "POST",
        "/v1/companies",
        json_body={
            "company_id": company_id,
            "company_profile_id": fixture["company_profile_id"],
            "actor_id": "local-e2e-actor",
            "replacement": fixture["profile"],
        },
        expected_status=201,
    )
    direction = fixture["direction"]
    assert isinstance(direction, dict)
    audience = direction["audience"]
    assert isinstance(audience, dict)
    connection = connect(settings.db_path)
    try:
        with transaction(connection):
            cards = CompanyCardService(connection, clock=lambda: NOW)
            cards.create_direction(
                CreateDirection(
                    company_id=company_id,
                    company_profile_version=1,
                    direction_id=direction["direction_id"],
                    actor_id="local-e2e-actor",
                    replacement=DirectionData.model_validate(direction["data"]),
                )
            )
            cards.create_audience(
                CreateAudience(
                    company_id=company_id,
                    direction_id=direction["direction_id"],
                    direction_version=1,
                    audience_segment_id=audience["audience_segment_id"],
                    actor_id="local-e2e-actor",
                    replacement=AudienceData.model_validate(audience["data"]),
                )
            )
    finally:
        connection.close()


def _brief_payload(
    brief_id: str,
    fixture: dict[str, Any],
    *,
    direction_version: int = 1,
    audience_version: int = 1,
) -> dict[str, Any]:
    company_id = str(fixture["company_id"])
    direction = fixture["direction"]
    assert isinstance(direction, dict)
    audience = direction["audience"]
    assert isinstance(audience, dict)
    primary_keyword = f"invented {direction['direction_id']} service"
    return {
        "brief_id": brief_id,
        "actor_id": "local-e2e-actor",
        "company_id": company_id,
        "direction_id": direction["direction_id"],
        "direction_version": direction_version,
        "audience_segment_id": audience["audience_segment_id"],
        "audience_version": audience_version,
        "page_type": "service-page",
        "goal": f"Explain the invented {direction['direction_id']} service.",
        "target_language": "en",
        "locale": "en-US",
        "page_structure": ["Overview", "Process", "Decision"],
        "primary_keyword": primary_keyword,
        "keywords": [primary_keyword],
        "lsi_terms": [f"invented {company_id} example"],
        "competitor_urls": [f"https://example.test/{company_id}-reference"],
        "current_page_context": f"Invented source context for {company_id} only.",
    }


def plan_flow(
    app: Any,
    settings: Settings,
    fixture: dict[str, Any],
    *,
    direction_version: int = 1,
    audience_version: int = 1,
    approve: bool = True,
) -> PlannedFlow:
    company_id = str(fixture["company_id"])
    direction = fixture["direction"]
    assert isinstance(direction, dict)
    audience = direction["audience"]
    assert isinstance(audience, dict)
    started = api_request(
        app,
        "POST",
        "/v1/briefs",
        json_body={"company_id": company_id, "actor_id": "local-e2e-actor"},
        expected_status=201,
    ).json()
    brief_id = str(started["brief_id"])
    api_request(
        app,
        "PATCH",
        f"/v1/briefs/{brief_id}",
        json_body={
            "expected_version": 1,
            "expected_profile_version": 1,
            **_brief_payload(
                brief_id,
                fixture,
                direction_version=direction_version,
                audience_version=audience_version,
            ),
        },
    )
    api_request(
        app,
        "POST",
        f"/v1/briefs/{brief_id}/validate",
        json_body={
            "company_id": company_id,
            "actor_id": "local-e2e-actor",
            "expected_version": 2,
            "expected_profile_version": 1,
        },
    )
    connection = connect(settings.db_path)
    try:
        with transaction(connection):
            snapshot = SnapshotCompiler(connection, company_id=company_id).compile_snapshot(
                brief_id,
                prompt_set_version=1,
            )
    finally:
        connection.close()
    planned = api_request(
        app,
        "POST",
        "/v1/jobs/plan",
        json_body={
            "company_id": company_id,
            "snapshot_id": snapshot.snapshot_id,
            "execution_plan": {
                "pipeline_version": PIPELINE_VERSION,
                "executor_name": "mock",
                "model_ids": ["writer-model-v1"],
                "provider_ids": ["mock-provider"],
                "maximum_retries": 1,
                "cost_currency": None,
                "cost_min_decimal": None,
                "cost_max_decimal": None,
                "unknown_cost_reasons": ["local deterministic mock"],
                "result_destination": "local-artifacts",
            },
        },
        expected_status=201,
    ).json()
    job_id = str(planned["job_id"])
    api_request(
        app,
        "POST",
        f"/v1/jobs/{job_id}/request-paid-approval",
        json_body={"company_id": company_id},
    )
    if approve:
        api_request(
            app,
            "POST",
            f"/v1/jobs/{job_id}/approve",
            json_body={
                "company_id": company_id,
                "actor_id": "local-e2e-approver",
                "snapshot_hash": planned["snapshot_hash"],
                "plan_fingerprint": planned["plan_fingerprint"],
            },
        )
    profile = fixture["profile"]
    assert isinstance(profile, dict)
    data = direction["data"]
    assert isinstance(data, dict)
    return PlannedFlow(
        company_id=company_id,
        direction_id=str(direction["direction_id"]),
        audience_id=str(audience["audience_segment_id"]),
        company_name=str(profile["name"]),
        direction_name=str(data["name"]),
        brief_id=brief_id,
        snapshot_id=snapshot.snapshot_id,
        snapshot_hash=snapshot.snapshot_hash,
        job_id=job_id,
        plan_fingerprint=str(planned["plan_fingerprint"]),
    )


def execution_result(company_id: str, keyword: str) -> ExecutionResult:
    title_base = f"Invented guide for {company_id}"
    description_base = f"Deterministic local-only description for {company_id}"
    content = f"# {title_base}\n\nA local test result about {keyword} for {company_id}."
    return ExecutionResult(
        content_markdown=content,
        titles=tuple(f"{title_base} {index}" for index in range(1, 6)),  # type: ignore[arg-type]
        descriptions=tuple(
            f"{description_base} {index}." for index in range(1, 6)
        ),  # type: ignore[arg-type]
        keyword_qa={"primary_keyword": keyword, "occurrences": 1, "passed": True},
        text_metrics={"characters": len(content), "words": len(content.split())},
        sources=(
            {
                "url": f"https://example.com/{company_id}-source",
                "content_hash": ("a" if company_id == "avtomalyar" else "b") * 64,
                "fetched_at": NOW.isoformat(),
            },
        ),
        warnings=(),
        model_usage={
            "models": [
                {
                    "model_id": "writer-model-v1",
                    "provider_id": "mock-provider",
                    "input_tokens": 10,
                    "output_tokens": 20,
                }
            ]
        },
        stage_timings={"complete_ms": 1},
        prompt_versions={"writer": "writer-v1"},
    )


def succeeded_status(run_number: int, result: ExecutionResult) -> ExecutionStatus:
    return ExecutionStatus(
        external_run_id=f"local-run-{run_number}",
        status=ExternalStatus.SUCCEEDED,
        stage_id="complete",
        retry_after_seconds=None,
        error_code=None,
        error_summary=None,
        result=result,
    )


def make_runner(
    settings: Settings,
    executor: MockExecutor,
    *,
    runner_id: str,
    lease_token: str,
    clock: Callable[[], datetime] | None = None,
) -> tuple[Any, Runner]:
    connection = connect(settings.db_path)
    selected_clock = clock or (lambda: datetime.now(UTC))
    return connection, Runner(
        connection,
        executor=executor,
        artifact_store=ArtifactStore(settings.artifact_root, clock=selected_clock),
        clock=selected_clock,
        runner_id=runner_id,
        lease_token_factory=lambda: lease_token,
    )


def assert_succeeded(settings: Settings, flow: PlannedFlow) -> None:
    connection = connect(settings.db_path)
    try:
        job = JobService(connection, company_id=flow.company_id).get_job(flow.job_id)
        assert job.state is JobState.SUCCEEDED
        assert job.artifact_manifest_path == str(
            settings.artifact_root
            / "companies"
            / flow.company_id
            / "jobs"
            / flow.job_id
            / "manifest.json"
        )
    finally:
        connection.close()


def read_manifest(settings: Settings, flow: PlannedFlow) -> dict[str, Any]:
    path = (
        settings.artifact_root
        / "companies"
        / flow.company_id
        / "jobs"
        / flow.job_id
        / "manifest.json"
    )
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError("manifest must be an object")
    return value


def suppress_completion(flow: PlannedFlow) -> DeliveryReceipt:
    sink = build_notification_sink(NotificationConfig())
    assert isinstance(sink, NullNotificationSink)
    try:
        return sink.send(
            NotificationEvent(
                event_id=f"event-{flow.job_id}",
                job_id=flow.job_id,
                event_type="completed",
                company_display_name=flow.company_name,
                direction_display_name=flow.direction_name,
                completed_stage_count=1,
                stage_id=None,
                attempt=1,
                elapsed_seconds=1,
                artifact_ready=True,
            )
        )
    finally:
        sink.close()


def app_for(settings: Settings) -> Any:
    return create_app(settings)
