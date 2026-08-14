"""Brief-draft routes for the authenticated local Worker API."""

from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, Depends, Request, status
from pydantic import Field, model_validator

from seo_orchestrator.db.connection import connect, transaction
from seo_orchestrator.domain.models import DomainModel, Identifier, StrictPositiveInt
from seo_orchestrator.services.briefs import (
    BriefService,
    SeoBriefDraft,
    UpdateBrief,
    ValidatedBrief,
)
from seo_orchestrator.settings import Settings


class StartBriefRequest(DomainModel):
    """Actor and company scope required to begin a resumable brief draft."""

    company_id: Identifier
    actor_id: Identifier


class BriefScope(DomainModel):
    """Explicit actor and company scope for one existing brief."""

    company_id: Identifier
    actor_id: Identifier


class ValidateBriefRequest(BriefScope):
    """Explicit optimistic versions required for the validating transition."""

    expected_version: StrictPositiveInt
    expected_profile_version: StrictPositiveInt


class UpdateBriefRequest(UpdateBrief):
    """Worker update contract requires both optimistic concurrency versions."""

    expected_version: StrictPositiveInt | None = Field(...)
    expected_profile_version: StrictPositiveInt | None = Field(...)

    @model_validator(mode="after")
    def require_versions(self) -> UpdateBriefRequest:
        if self.expected_version is None or self.expected_profile_version is None:
            raise ValueError("both expected versions are required")
        return self


def create_brief_router(
    settings: Settings, require_bearer: Callable[[Request], None]
) -> APIRouter:
    """Build company-scoped brief routes with one SQLite connection per request."""
    router = APIRouter(dependencies=[Depends(require_bearer)])

    @router.post("/v1/briefs", status_code=status.HTTP_201_CREATED)
    def start_brief(command: StartBriefRequest) -> SeoBriefDraft:
        connection = connect(settings.db_path)
        try:
            with transaction(connection):
                return BriefService(connection).start_brief(
                    actor_id=command.actor_id, company_id=command.company_id
                )
        finally:
            connection.close()

    @router.patch("/v1/briefs/{brief_id}")
    def update_brief(brief_id: Identifier, command: UpdateBriefRequest) -> SeoBriefDraft:
        if command.brief_id != brief_id:
            raise ValueError("brief_id path and body must match")
        connection = connect(settings.db_path)
        try:
            with transaction(connection):
                return BriefService(connection).update_brief(command)
        finally:
            connection.close()

    @router.post("/v1/briefs/{brief_id}/validate")
    def validate_brief(
        brief_id: Identifier, scope: ValidateBriefRequest
    ) -> ValidatedBrief:
        connection = connect(settings.db_path)
        try:
            with transaction(connection):
                return BriefService(connection).validate_brief(
                    company_id=scope.company_id,
                    brief_id=brief_id,
                    actor_id=scope.actor_id,
                    expected_version=scope.expected_version,
                    expected_profile_version=scope.expected_profile_version,
                )
        finally:
            connection.close()

    return router
