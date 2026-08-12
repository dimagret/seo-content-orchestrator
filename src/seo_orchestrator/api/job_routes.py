"""Durable job routes for the authenticated local Worker API."""

from __future__ import annotations

from collections.abc import Callable
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Request, status
from pydantic import Field, StringConstraints

from seo_orchestrator.db.connection import connect
from seo_orchestrator.domain import ApprovalRecord, ExecutionPlan, JobState, PlannedJob, SeoJob
from seo_orchestrator.domain.models import DomainModel, Identifier, Sha256Hex
from seo_orchestrator.services.approvals import ApprovalService
from seo_orchestrator.services.jobs import JobService
from seo_orchestrator.settings import Settings

_StrictString = Annotated[str, StringConstraints(strict=True)]
_StrictStrings = tuple[_StrictString, ...]
_StrictNonNegativeInt = Annotated[int, Field(strict=True, ge=0)]
_CancelableJobState = Literal[JobState.QUEUED, JobState.RUNNING]


class ExecutionPlanRequest(DomainModel):
    """JSON representation of the exact immutable paid-execution plan."""

    pipeline_version: _StrictString
    executor_name: _StrictString
    model_ids: _StrictStrings
    provider_ids: _StrictStrings
    maximum_retries: _StrictNonNegativeInt
    cost_currency: _StrictString | None
    cost_min_decimal: _StrictString | None
    cost_max_decimal: _StrictString | None
    unknown_cost_reasons: _StrictStrings
    result_destination: _StrictString

    def to_domain(self) -> ExecutionPlan:
        """Convert validated transport values into the frozen domain contract."""
        return ExecutionPlan(
            pipeline_version=self.pipeline_version,
            executor_name=self.executor_name,
            model_ids=self.model_ids,
            provider_ids=self.provider_ids,
            maximum_retries=self.maximum_retries,
            cost_currency=self.cost_currency,
            cost_min_decimal=self.cost_min_decimal,
            cost_max_decimal=self.cost_max_decimal,
            unknown_cost_reasons=self.unknown_cost_reasons,
            result_destination=self.result_destination,
        )


class PlanJobRequest(DomainModel):
    """Company-scoped plan request bound to a pre-existing immutable snapshot."""

    company_id: Identifier
    snapshot_id: Identifier
    execution_plan: ExecutionPlanRequest


class CompanyScopeRequest(DomainModel):
    """Explicit company scope for an authenticated job lifecycle action."""

    company_id: Identifier


class ApproveJobRequest(CompanyScopeRequest):
    """User-approved immutable fingerprints for one paid execution."""

    actor_id: Identifier
    snapshot_hash: Sha256Hex
    plan_fingerprint: Sha256Hex


class CancelJobRequest(CompanyScopeRequest):
    """Caller-observed legal source state for a cancel compare-and-swap."""

    expected_state: _CancelableJobState


def create_job_router(settings: Settings, require_bearer: Callable[[Request], None]) -> APIRouter:
    """Build job routes with service-owned state transitions and provenance checks."""
    router = APIRouter(dependencies=[Depends(require_bearer)])

    @router.post("/v1/jobs/plan", status_code=status.HTTP_201_CREATED)
    def plan_job(command: PlanJobRequest) -> PlannedJob:
        connection = connect(settings.db_path)
        try:
            return JobService(connection, company_id=command.company_id).plan_job(
                command.snapshot_id, command.execution_plan.to_domain()
            )
        finally:
            connection.close()

    @router.get("/v1/jobs/{job_id}")
    def get_job(job_id: Identifier, company_id: Identifier) -> SeoJob:
        connection = connect(settings.db_path)
        try:
            return JobService(connection, company_id=company_id).get_job(job_id)
        finally:
            connection.close()

    @router.post("/v1/jobs/{job_id}/request-paid-approval")
    def request_paid_approval(job_id: Identifier, scope: CompanyScopeRequest) -> SeoJob:
        connection = connect(settings.db_path)
        try:
            return JobService(
                connection, company_id=scope.company_id
            ).request_paid_approval(job_id)
        finally:
            connection.close()

    @router.post("/v1/jobs/{job_id}/approve")
    def approve_job(job_id: Identifier, command: ApproveJobRequest) -> ApprovalRecord:
        connection = connect(settings.db_path)
        try:
            return ApprovalService(
                connection, company_id=command.company_id
            ).approve_job(
                job_id,
                command.actor_id,
                command.snapshot_hash,
                command.plan_fingerprint,
            )
        finally:
            connection.close()

    @router.post("/v1/jobs/{job_id}/cancel")
    def cancel_job(job_id: Identifier, command: CancelJobRequest) -> SeoJob:
        connection = connect(settings.db_path)
        try:
            return JobService(connection, company_id=command.company_id).cancel_job(
                job_id, command.expected_state
            )
        finally:
            connection.close()

    @router.post("/v1/jobs/{job_id}/retry")
    def retry_job(job_id: Identifier, scope: CompanyScopeRequest) -> SeoJob:
        connection = connect(settings.db_path)
        try:
            return JobService(connection, company_id=scope.company_id).retry_job(job_id)
        finally:
            connection.close()

    return router
