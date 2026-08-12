"""Company-card routes for the authenticated local Worker API."""

from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, Depends, Request, status

from seo_orchestrator.db.connection import connect, transaction
from seo_orchestrator.domain import CompanyProfile
from seo_orchestrator.services.company_cards import (
    CompanyCardService,
    CreateCompany,
)
from seo_orchestrator.settings import Settings


def create_company_router(
    settings: Settings, require_bearer: Callable[[Request], None]
) -> APIRouter:
    """Build company-card routes with one SQLite connection per request."""
    router = APIRouter(dependencies=[Depends(require_bearer)])

    @router.get("/v1/companies")
    def list_companies() -> list[CompanyProfile]:
        connection = connect(settings.db_path)
        try:
            service = CompanyCardService(connection)
            return list(service.list_current_company_profiles())
        finally:
            connection.close()

    @router.post("/v1/companies", status_code=status.HTTP_201_CREATED)
    def create_company(command: CreateCompany) -> CompanyProfile:
        connection = connect(settings.db_path)
        try:
            with transaction(connection):
                return CompanyCardService(connection).create_company(command)
        finally:
            connection.close()

    return router
