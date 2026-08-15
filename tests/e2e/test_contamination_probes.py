"""Explicit cross-company contamination and disclosure probes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from seo_orchestrator.db.connection import connect
from seo_orchestrator.db.repositories import SnapshotRepository
from tests.e2e.support import (
    MutationGuardExecutor,
    api_request,
    app_for,
    create_company_card,
    execution_result,
    load_company_fixture,
    make_runner,
    make_settings,
    plan_flow,
    read_manifest,
    succeeded_status,
)


def _stable_error(response_json: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in response_json.items() if key != "request_id"}


def test_foreign_company_jobs_snapshots_and_artifacts_are_indistinguishable_from_missing(
    tmp_path: Path,
) -> None:
    settings = make_settings(tmp_path)
    app = app_for(settings)
    avtomalyar = load_company_fixture("avtomalyar")
    sweet_world = load_company_fixture("sweet-world")
    create_company_card(app, settings, avtomalyar)
    create_company_card(app, settings, sweet_world)

    first = plan_flow(app, settings, avtomalyar)
    executor = MutationGuardExecutor(
        state_path=tmp_path / "mock-executor.db",
        outcomes=(
            succeeded_status(
                1,
                execution_result("avtomalyar", "invented isolated painting service"),
            ),
        ),
    )
    connection, runner = make_runner(
        settings,
        executor,
        runner_id="contamination-runner",
        lease_token="contamination-lease",
    )
    try:
        assert runner.tick() == 1
        assert runner.tick() == 1
    finally:
        connection.close()
    second = plan_flow(app, settings, sweet_world, approve=False)

    foreign_job = api_request(
        app,
        "GET",
        f"/v1/jobs/{first.job_id}",
        params={"company_id": second.company_id},
        expected_status=404,
    )
    missing_job = api_request(
        app,
        "GET",
        "/v1/jobs/missing-local-job",
        params={"company_id": second.company_id},
        expected_status=404,
    )
    assert _stable_error(foreign_job.json()) == _stable_error(missing_job.json()) == {
        "code": "NOT_FOUND",
        "message": "not found",
    }

    foreign_artifact = api_request(
        app,
        "GET",
        f"/v1/jobs/{first.job_id}/artifacts/content",
        params={"company_id": second.company_id},
        expected_status=404,
    )
    missing_artifact = api_request(
        app,
        "GET",
        "/v1/jobs/missing-local-job/artifacts/content",
        params={"company_id": second.company_id},
        expected_status=404,
    )
    assert _stable_error(foreign_artifact.json()) == _stable_error(
        missing_artifact.json()
    ) == {"code": "NOT_FOUND", "message": "not found"}

    reverse_foreign_job = api_request(
        app,
        "GET",
        f"/v1/jobs/{second.job_id}",
        params={"company_id": first.company_id},
        expected_status=404,
    )
    assert _stable_error(reverse_foreign_job.json()) == {
        "code": "NOT_FOUND",
        "message": "not found",
    }

    db = connect(settings.db_path)
    try:
        snapshots = SnapshotRepository(db)
        first_snapshot = snapshots.get_snapshot(first.company_id, first.snapshot_id)
        second_snapshot = snapshots.get_snapshot(second.company_id, second.snapshot_id)
    finally:
        db.close()
    first_snapshot_text = first_snapshot.model_dump_json()
    second_snapshot_text = second_snapshot.model_dump_json()
    assert "sweet-world" not in first_snapshot_text
    assert "wedding-cakes" not in first_snapshot_text
    assert "newlyweds" not in first_snapshot_text
    assert "avtomalyar" not in second_snapshot_text
    assert "car-painting" not in second_snapshot_text
    assert "private-car-owners" not in second_snapshot_text

    first_manifest_text = json.dumps(read_manifest(settings, first), sort_keys=True)
    assert "sweet-world" not in first_manifest_text
    assert "wedding-cakes" not in first_manifest_text
    assert "newlyweds" not in first_manifest_text
    assert not (
        settings.artifact_root
        / "companies"
        / second.company_id
        / "jobs"
        / second.job_id
    ).exists()
