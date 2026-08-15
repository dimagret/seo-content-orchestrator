"""Restart recovery across queued, running, and succeeded boundaries."""

from __future__ import annotations

from pathlib import Path

from tests.e2e.support import (
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
    succeeded_status,
    suppress_completion,
)


def test_app_and_runner_restart_resume_one_durable_job_without_duplicate_submit(
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    initial_app = app_for(settings)
    fixture = load_company_fixture("avtomalyar")
    create_company_card(initial_app, settings, fixture)
    flow = plan_flow(initial_app, settings, fixture)
    assert api_request(
        initial_app,
        "GET",
        f"/v1/jobs/{flow.job_id}",
        params={"company_id": flow.company_id},
    ).json()["state"] == "QUEUED"

    executor_state = tmp_path / "durable-mock-executor.db"
    outcome = succeeded_status(
        1,
        execution_result("avtomalyar", "invented restart-safe painting service"),
    )

    pre_restart_executor = MutationGuardExecutor(
        state_path=executor_state,
        outcomes=(outcome,),
    )
    pre_restart_connection, pre_restart_runner = make_runner(
        settings,
        pre_restart_executor,
        runner_id="runner-queued-before-restart",
        lease_token="lease-queued-before-restart",
    )
    pre_restart_connection.close()
    assert pre_restart_executor.submit_call_count == 0
    assert pre_restart_executor.submission_count == 0

    queued_restart_app = app_for(settings)
    assert api_request(
        queued_restart_app,
        "GET",
        f"/v1/jobs/{flow.job_id}",
        params={"company_id": flow.company_id},
    ).json()["state"] == "QUEUED"
    first_executor = MutationGuardExecutor(
        state_path=executor_state,
        outcomes=(outcome,),
    )
    first_connection, first_runner = make_runner(
        settings,
        first_executor,
        runner_id="runner-before-restart",
        lease_token="lease-before-restart",
    )
    assert first_runner is not pre_restart_runner
    assert first_executor is not pre_restart_executor
    try:
        assert first_runner.tick() == 1
    finally:
        first_connection.close()
    assert first_executor.submission_count == 1
    assert first_executor.submit_call_count == 1
    assert api_request(
        queued_restart_app,
        "GET",
        f"/v1/jobs/{flow.job_id}",
        params={"company_id": flow.company_id},
    ).json()["state"] == "RUNNING"

    running_restart_app = app_for(settings)
    restarted_executor = MutationGuardExecutor(
        state_path=executor_state,
        outcomes=(outcome,),
    )
    restarted_connection, restarted_runner = make_runner(
        settings,
        restarted_executor,
        runner_id="runner-after-restart",
        lease_token="lease-after-restart",
    )
    try:
        assert restarted_runner.tick() == 1
    finally:
        restarted_connection.close()

    assert restarted_executor.submit_call_count == 0
    assert restarted_executor.submission_count == 1
    assert restarted_executor.poll_call_count == 1
    assert_succeeded(settings, flow)
    assert api_request(
        running_restart_app,
        "GET",
        f"/v1/jobs/{flow.job_id}",
        params={"company_id": flow.company_id},
    ).json()["state"] == "SUCCEEDED"
    assert suppress_completion(flow).destination == "null"
