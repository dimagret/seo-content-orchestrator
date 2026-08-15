"""Two-company local Stage B acceptance flow with contamination checks."""

from __future__ import annotations

import json
from pathlib import Path

from seo_orchestrator.db.connection import connect, transaction
from seo_orchestrator.executors.base import Executor
from seo_orchestrator.services.company_cards import (
    AudienceData,
    CompanyCardService,
    DirectionData,
    ReviseAudience,
    ReviseDirection,
)
from seo_orchestrator.services.jobs import JobService
from tests.e2e.support import (
    PIPELINE_VERSION,
    MutationGuardExecutor,
    api_request,
    app_for,
    assert_succeeded,
    create_company_card,
    execution_result,
    load_company_fixture,
    make_runner,
    make_settings,
    plan_flow,
    read_manifest,
    succeeded_status,
    suppress_completion,
)


def test_two_companies_share_pipeline_without_context_or_workflow_contamination(
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    app = app_for(settings)
    avtomalyar = load_company_fixture("avtomalyar")
    sweet_world = load_company_fixture("sweet-world")
    executor = MutationGuardExecutor(
        state_path=tmp_path / "mock-executor.db",
        outcomes=(
            succeeded_status(
                1,
                execution_result("avtomalyar", "invented car painting service"),
            ),
            succeeded_status(
                2,
                execution_result("sweet-world", "invented wedding cake service"),
            ),
        ),
    )
    executor_state = tmp_path / "mock-executor.db"
    configuration_baseline = executor.configuration_call_count
    durable_state_baseline = executor_state.read_bytes()
    assert configuration_baseline == 1
    for absent_workflow_api in ("configure_workflow", "mutate_workflow"):
        assert not hasattr(Executor, absent_workflow_api)
        assert not hasattr(type(executor), absent_workflow_api)

    create_company_card(app, settings, avtomalyar)
    create_company_card(app, settings, sweet_world)
    assert executor.submit_call_count == 0
    assert executor.lookup_call_count == 0
    assert executor.poll_call_count == 0
    assert executor.cancel_call_count == 0
    assert executor.configuration_call_count == configuration_baseline
    assert executor_state.read_bytes() == durable_state_baseline

    first = plan_flow(app, settings, avtomalyar)
    first_connection, first_runner = make_runner(
        settings,
        executor,
        runner_id="runner-first",
        lease_token="lease-first",
    )
    try:
        assert first_runner.tick() == 1
        second_tick = first_runner.tick()
        assert second_tick == 1, first_connection.execute(
            """SELECT external_status, current_stage, next_action_at,
                      acceptance_observed_at, updated_at
               FROM job_execution_runs WHERE company_id = ? AND job_id = ?""",
            (first.company_id, first.job_id),
        ).fetchone()
    finally:
        first_connection.close()
    assert_succeeded(settings, first)
    assert suppress_completion(first).destination == "null"

    second = plan_flow(app, settings, sweet_world)
    second_connection, second_runner = make_runner(
        settings,
        executor,
        runner_id="runner-second",
        lease_token="lease-second",
    )
    try:
        assert second_runner.tick() == 1
        assert second_runner.tick() == 1
    finally:
        second_connection.close()
    assert_succeeded(settings, second)
    assert suppress_completion(second).destination == "null"

    assert first.snapshot_hash != second.snapshot_hash
    connection = connect(settings.db_path)
    try:
        first_plan = JobService(connection, company_id=first.company_id).execution_plan(
            first.job_id
        )
        second_plan = JobService(connection, company_id=second.company_id).execution_plan(
            second.job_id
        )
    finally:
        connection.close()
    assert first_plan.pipeline_version == PIPELINE_VERSION
    assert second_plan.pipeline_version == PIPELINE_VERSION
    assert first_plan.executor_name == second_plan.executor_name == "mock"
    assert executor.submission_count == 2
    assert executor.configuration_call_count == configuration_baseline

    first_manifest = read_manifest(settings, first)
    second_manifest = read_manifest(settings, second)
    assert first_manifest["company_id"] == "avtomalyar"
    assert first_manifest["direction_id"] == "car-painting"
    assert first_manifest["audience_segment_id"] == "private-car-owners"
    assert second_manifest["company_id"] == "sweet-world"
    assert second_manifest["direction_id"] == "wedding-cakes"
    assert second_manifest["audience_segment_id"] == "newlyweds"

    first_manifest_text = json.dumps(first_manifest, sort_keys=True)
    second_manifest_text = json.dumps(second_manifest, sort_keys=True)
    for foreign_value in ("sweet-world", "Sweet World Atelier", "wedding-cakes", "newlyweds"):
        assert foreign_value not in first_manifest_text
    for foreign_value in (
        "avtomalyar",
        "Avtomalyar Studio",
        "car-painting",
        "private-car-owners",
    ):
        assert foreign_value not in second_manifest_text

    first_content = api_request(
        app,
        "GET",
        f"/v1/jobs/{first.job_id}/artifacts/content",
        params={"company_id": first.company_id},
    ).text
    second_content = api_request(
        app,
        "GET",
        f"/v1/jobs/{second.job_id}/artifacts/content",
        params={"company_id": second.company_id},
    ).text
    assert "avtomalyar" in first_content
    assert "sweet-world" not in first_content
    assert "sweet-world" in second_content
    assert "avtomalyar" not in second_content

    assert (
        settings.artifact_root
        / "companies"
        / first.company_id
        / "jobs"
        / first.job_id
    ).is_dir()
    assert (
        settings.artifact_root
        / "companies"
        / second.company_id
        / "jobs"
        / second.job_id
    ).is_dir()

    executor_calls_before_card_lifecycle = (
        executor.submit_call_count,
        executor.lookup_call_count,
        executor.poll_call_count,
        executor.cancel_call_count,
        executor.cancellation_count,
        executor.configuration_call_count,
        executor.submission_count,
    )
    executor_state_before_card_lifecycle = executor_state.read_bytes()

    listed = api_request(app, "GET", "/v1/companies").json()
    assert {item["company_id"] for item in listed} == {"avtomalyar", "sweet-world"}
    original_profile = api_request(
        app,
        "GET",
        "/v1/companies/sweet-world",
        params={"version": "1"},
    ).json()
    assert original_profile["company_profile_version"] == 1
    revised_profile_data = dict(sweet_world["profile"])
    revised_profile_data["name"] = "Sweet World Atelier Revised"
    revised_profile = api_request(
        app,
        "PATCH",
        "/v1/companies/sweet-world",
        json_body={
            "company_id": "sweet-world",
            "actor_id": "local-e2e-actor",
            "expected_current_version": 1,
            "replacement": revised_profile_data,
        },
    ).json()
    assert revised_profile["company_profile_version"] == 2
    current_profiles = {
        item["company_id"]: item
        for item in api_request(app, "GET", "/v1/companies").json()
    }
    assert current_profiles["sweet-world"]["company_profile_version"] == 2

    direction_fixture = sweet_world["direction"]
    assert isinstance(direction_fixture, dict)
    audience_fixture = direction_fixture["audience"]
    assert isinstance(audience_fixture, dict)
    card_connection = connect(settings.db_path)
    try:
        with transaction(card_connection):
            cards = CompanyCardService(card_connection)
            revised_direction = cards.revise_direction(
                ReviseDirection(
                    company_id="sweet-world",
                    company_profile_version=2,
                    direction_id="wedding-cakes",
                    actor_id="local-e2e-actor",
                    expected_current_version=1,
                    replacement=DirectionData.model_validate(direction_fixture["data"]),
                )
            )
            revised_audience = cards.revise_audience(
                ReviseAudience(
                    company_id="sweet-world",
                    direction_id="wedding-cakes",
                    direction_version=2,
                    audience_segment_id="newlyweds",
                    actor_id="local-e2e-actor",
                    expected_current_version=1,
                    replacement=AudienceData.model_validate(audience_fixture["data"]),
                )
            )
            assert revised_direction.direction_version == 2
            assert revised_audience.audience_version == 2
            assert cards.get_direction("sweet-world", "wedding-cakes", 1).direction_version == 1
            assert cards.get_direction("sweet-world", "wedding-cakes", 2) == revised_direction
            assert cards.get_audience(
                "sweet-world", "wedding-cakes", "newlyweds", 1
            ).audience_version == 1
            assert cards.get_audience(
                "sweet-world", "wedding-cakes", "newlyweds", 2
            ) == revised_audience
            cards.archive_company("sweet-world", "local-e2e-actor")
    finally:
        card_connection.close()
    assert {item["company_id"] for item in api_request(app, "GET", "/v1/companies").json()} == {
        "avtomalyar"
    }
    assert (
        executor.submit_call_count,
        executor.lookup_call_count,
        executor.poll_call_count,
        executor.cancel_call_count,
        executor.cancellation_count,
        executor.configuration_call_count,
        executor.submission_count,
    ) == executor_calls_before_card_lifecycle
    assert executor_state.read_bytes() == executor_state_before_card_lifecycle
