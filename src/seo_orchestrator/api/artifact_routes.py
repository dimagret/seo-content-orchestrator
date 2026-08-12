"""Artifact-content routes for the authenticated local Worker API."""

from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, Depends, Request
from fastapi.responses import PlainTextResponse

from seo_orchestrator.db.connection import connect
from seo_orchestrator.domain.models import Identifier
from seo_orchestrator.services.artifacts import ArtifactStore
from seo_orchestrator.services.jobs import JobService
from seo_orchestrator.settings import Settings


def create_artifact_router(
    settings: Settings, require_bearer: Callable[[Request], None]
) -> APIRouter:
    """Build strictly scoped, succeeded-only artifact content retrieval."""
    router = APIRouter(dependencies=[Depends(require_bearer)])

    @router.get("/v1/jobs/{job_id}/artifacts/content")
    def artifact_content(job_id: Identifier, company_id: Identifier) -> PlainTextResponse:
        connection = connect(settings.db_path)
        try:
            artifact = JobService(
                connection,
                company_id=company_id,
                artifact_store=ArtifactStore(settings.artifact_root),
            ).open_artifact(job_id, "content.md")
            try:
                content = artifact.read().decode("utf-8")
            finally:
                artifact.close()
        finally:
            connection.close()
        return PlainTextResponse(content, media_type="text/markdown")

    return router
