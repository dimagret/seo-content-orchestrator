"""Paid approval invalidation when versioned company context changes."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from seo_orchestrator.db.connection import connect, transaction
from seo_orchestrator.services.company_cards import (
    AudienceData,
    CompanyCardService,
    DirectionData,
    ReviseAudience,
    ReviseDirection,
)
from seo_orchestrator.services.jobs import JobService
from tests.e2e.support import (
    MutationGuardExecutor,
    api_request,
    app_for,
    create_company_card,
    load_company_fixture,
    make_runner,
    make_settings,
    plan_flow,
)


def test_direction_revision_prevents_old_paid_approval_from_starting_new_snapshot(
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    app = app_for(settings)
    fixture = load_company_fixture("avtomalyar")
    create_company_card(app, settings, fixture)
    executor = MutationGuardExecutor(
        state_path=tmp_path / "unused-executor.db",
        outcomes=(),
    )

    old_flow = plan_flow(app, settings, fixture, approve=True)
    old_job = api_request(
        app,
        "GET",
        f"/v1/jobs/{old_flow.job_id}",
        params={"company_id": old_flow.company_id},
    ).json()
    assert old_job["state"] == "QUEUED"
    assert old_job["approval_record_id"] is not None

    direction = fixture["direction"]
    assert isinstance(direction, dict)
    audience = direction["audience"]
    assert isinstance(audience, dict)
    revised_data = dict(direction["data"])
    revised_data["name"] = f"{revised_data['name']} Revised"
    connection = connect(settings.db_path)
    try:
        with transaction(connection):
            cards = CompanyCardService(connection)
            revised = cards.revise_direction(
                ReviseDirection(
                    company_id=old_flow.company_id,
                    company_profile_version=1,
                    direction_id=old_flow.direction_id,
                    actor_id="local-e2e-actor",
                    expected_current_version=1,
                    replacement=DirectionData.model_validate(revised_data),
                )
            )
            assert revised.direction_version == 2
            revised_audience = cards.revise_audience(
                ReviseAudience(
                    company_id=old_flow.company_id,
                    direction_id=old_flow.direction_id,
                    direction_version=2,
                    audience_segment_id=old_flow.audience_id,
                    actor_id="local-e2e-actor",
                    expected_current_version=1,
                    replacement=AudienceData.model_validate(audience["data"]),
                )
            )
            assert revised_audience.audience_version == 2
    finally:
        connection.close()

    new_flow = plan_flow(
        app,
        settings,
        fixture,
        direction_version=2,
        audience_version=2,
        approve=False,
    )
    assert new_flow.snapshot_hash != old_flow.snapshot_hash
    assert new_flow.plan_fingerprint == old_flow.plan_fingerprint

    rejected = api_request(
        app,
        "POST",
        f"/v1/jobs/{new_flow.job_id}/approve",
        json_body={
            "company_id": new_flow.company_id,
            "actor_id": "local-e2e-approver",
            "snapshot_hash": old_flow.snapshot_hash,
            "plan_fingerprint": old_flow.plan_fingerprint,
        },
        expected_status=409,
    )
    assert rejected.json()["code"] == "APPROVAL_INVALID"

    new_job = api_request(
        app,
        "GET",
        f"/v1/jobs/{new_flow.job_id}",
        params={"company_id": new_flow.company_id},
    ).json()
    assert new_job["state"] == "AWAITING_PAID_APPROVAL"
    assert new_job["approval_record_id"] is None
    connection = connect(settings.db_path)
    try:
        domain_job = JobService(
            connection,
            company_id=new_flow.company_id,
        ).get_job(new_flow.job_id)
        assert domain_job.snapshot_id == new_flow.snapshot_id
        assert domain_job.snapshot_hash == new_flow.snapshot_hash
    finally:
        connection.close()

    runner_connection, runner = make_runner(
        settings,
        executor,
        runner_id="runner-after-invalid-approval",
        lease_token="lease-after-invalid-approval",
    )
    try:
        tick_results: list[int] = []
        for _ in range(5):
            tick_results.append(runner.tick())
            if tick_results[-1] == 0:
                break
        assert tick_results[-1] == 0
        assert tick_results[:-1] == [1, 1]
    finally:
        runner_connection.close()

    still_awaiting = api_request(
        app,
        "GET",
        f"/v1/jobs/{new_flow.job_id}",
        params={"company_id": new_flow.company_id},
    ).json()
    assert still_awaiting["state"] == "AWAITING_PAID_APPROVAL"
    assert still_awaiting["approval_record_id"] is None
    assert executor.submit_call_count == 1
    assert executor.submission_count == 1
    executor_connection = sqlite3.connect(tmp_path / "unused-executor.db")
    try:
        submissions = executor_connection.execute(
            "SELECT idempotency_key, identity_json FROM mock_executor_runs"
        ).fetchall()
    finally:
        executor_connection.close()
    assert len(submissions) == 1
    idempotency_key, identity_json = submissions[0]
    submitted_identity = json.loads(identity_json)
    assert idempotency_key == f"{old_flow.company_id}:{old_flow.job_id}:1"
    assert submitted_identity[1] == old_flow.job_id
    assert submitted_identity[4] == old_flow.snapshot_id
    assert submitted_identity[5] == old_flow.snapshot_hash
    assert new_flow.job_id not in identity_json
    assert new_flow.snapshot_id not in identity_json
    assert executor.cancel_call_count == 0
    assert executor.configuration_call_count == 1
