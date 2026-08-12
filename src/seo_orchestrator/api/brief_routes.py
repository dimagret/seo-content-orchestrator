"""Brief-draft routes for the authenticated local Worker API."""

from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, Depends, Request, status

from seo_orchestrator.db.connection import connect, transaction
from seo_orchestrator.domain.models import DomainModel, Identifier
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
    def update_brief(brief_id: Identifier, command: UpdateBrief) -> SeoBriefDraft:
        if command.brief_id != brief_id:
            raise ValueError("brief_id path and body must match")
        connection = connect(settings.db_path)
        try:
            with transaction(connection):
                return BriefService(connection).update_brief(command)
        finally:
            connection.close()

    @router.post("/v1/briefs/{brief_id}/validate")
    def validate_brief(brief_id: Identifier, scope: BriefScope) -> ValidatedBrief:
        connection = connect(settings.db_path)
        try:
            with transaction(connection):
                return BriefService(connection).validate_brief(
                    company_id=scope.company_id,
                    brief_id=brief_id,
                    actor_id=scope.actor_id,
                )
        finally:
            connection.close()

    return router
