"""ASGI application factory for the local Worker API."""

from __future__ import annotations

import sqlite3
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from pydantic import ValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import RequestResponseEndpoint
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from seo_orchestrator.api.artifact_routes import create_artifact_router
from seo_orchestrator.api.auth import (
    ApiAuthenticationError,
    load_api_token,
    verify_bearer_authorization,
)
from seo_orchestrator.api.brief_routes import create_brief_router
from seo_orchestrator.api.callback_routes import create_callback_router
from seo_orchestrator.api.company_routes import create_company_router
from seo_orchestrator.api.job_routes import create_job_router
from seo_orchestrator.errors import (
    ApprovalInvalid,
    CallbackRejected,
    CompanyArchived,
    DataIntegrityError,
    InvalidTransition,
    NotFound,
    StateConflict,
    VersionConflict,
)
from seo_orchestrator.settings import Settings

MAX_REQUEST_BODY_BYTES = 64 * 1024
_CALLBACK_PATH = "/v1/callbacks/n8n"


def _declared_body_exceeds_limit(scope: Scope, max_body_bytes: int) -> bool:
    values = [
        value for name, value in scope["headers"] if name.lower() == b"content-length"
    ]
    if len(values) > 1:
        return True
    if not values:
        return False
    value = values[0]
    if not value.isascii() or not value.isdigit():
        return True
    try:
        return int(value) > max_body_bytes
    except ValueError:
        return True


async def _send_request_too_large(
    scope: Scope, receive: Receive, send: Send
) -> None:
    request_id = uuid4().hex
    response = JSONResponse(
        status_code=413,
        headers={"X-Request-ID": request_id},
        content={
            "code": "REQUEST_TOO_LARGE",
            "message": "request body exceeds limit",
            "request_id": request_id,
        },
    )
    await response(scope, receive, send)


class RequestBodyLimitMiddleware:
    """Bound all non-callback HTTP bodies before FastAPI can buffer them."""

    def __init__(self, app: ASGIApp, *, max_body_bytes: int) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] == _CALLBACK_PATH:
            await self.app(scope, receive, send)
            return
        if _declared_body_exceeds_limit(scope, self.max_body_bytes):
            await _send_request_too_large(scope, receive, send)
            return

        buffered_messages: list[Message] = []
        received_bytes = 0
        while True:
            message = await receive()
            buffered_messages.append(message)
            if message["type"] == "http.request":
                received_bytes += len(message.get("body", b""))
                if received_bytes > self.max_body_bytes:
                    await _send_request_too_large(scope, receive, send)
                    return
                if not message.get("more_body", False):
                    break
            elif message["type"] == "http.disconnect":
                break

        async def receive_buffered() -> Message:
            if buffered_messages:
                return buffered_messages.pop(0)
            return {"type": "http.disconnect"}

        await self.app(scope, receive_buffered, send)


def _request_id(request: Request) -> str:
    request_id = getattr(request.state, "request_id", None)
    return request_id if isinstance(request_id, str) else uuid4().hex


def _error_response(
    request: Request,
    *,
    status_code: int,
    code: str,
    message: str,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    request_id = _request_id(request)
    response_headers = {**(headers or {}), "X-Request-ID": request_id}
    return JSONResponse(
        status_code=status_code,
        headers=response_headers,
        content={
            "code": code,
            "message": message,
            "request_id": request_id,
        },
    )


def _unauthorized_response(request: Request) -> JSONResponse:
    return _error_response(
        request,
        status_code=401,
        code="UNAUTHORIZED",
        message="unauthorized",
        headers={"WWW-Authenticate": "Bearer"},
    )


def _authorization_header(request: Request) -> str | None:
    values = request.headers.getlist("authorization")
    if len(values) != 1:
        return None
    return values[0]


def create_app(settings: Settings) -> FastAPI:
    """Create the local ASGI API without opening a shared SQLite connection."""
    expected_token = load_api_token(settings.api_token_path)
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def attach_request_id(
        request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        request.state.request_id = uuid4().hex
        response = await call_next(request)
        response.headers["X-Request-ID"] = _request_id(request)
        return response

    app.add_middleware(
        RequestBodyLimitMiddleware,
        max_body_bytes=MAX_REQUEST_BODY_BYTES,
    )

    @app.exception_handler(ApiAuthenticationError)
    async def handle_authentication_error(
        request: Request, _error: ApiAuthenticationError
    ) -> JSONResponse:
        return _unauthorized_response(request)

    @app.exception_handler(CallbackRejected)
    async def handle_callback_rejection(
        request: Request, _error: CallbackRejected
    ) -> JSONResponse:
        return _unauthorized_response(request)

    @app.exception_handler(RequestValidationError)
    async def handle_request_validation_error(
        request: Request, _error: RequestValidationError
    ) -> JSONResponse:
        return _error_response(
            request,
            status_code=422,
            code="VALIDATION_ERROR",
            message="invalid request",
        )

    @app.exception_handler(ValidationError)
    async def handle_domain_validation_error(
        request: Request, _error: ValidationError
    ) -> JSONResponse:
        return _error_response(
            request,
            status_code=422,
            code="VALIDATION_ERROR",
            message="invalid request",
        )

    @app.exception_handler(ValueError)
    async def handle_value_error(request: Request, _error: ValueError) -> JSONResponse:
        return _error_response(
            request,
            status_code=422,
            code="VALIDATION_ERROR",
            message="invalid request",
        )

    @app.exception_handler(NotFound)
    async def handle_not_found(request: Request, _error: NotFound) -> JSONResponse:
        return _error_response(
            request,
            status_code=404,
            code="NOT_FOUND",
            message="not found",
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_framework_http_exception(
        request: Request, error: StarletteHTTPException
    ) -> JSONResponse:
        if error.status_code == 404:
            code = "NOT_FOUND"
            message = "not found"
        elif error.status_code == 405:
            code = "METHOD_NOT_ALLOWED"
            message = "method not allowed"
        else:
            code = "HTTP_ERROR"
            message = "request rejected"
        return _error_response(
            request,
            status_code=error.status_code,
            code=code,
            message=message,
        )

    @app.exception_handler(ApprovalInvalid)
    @app.exception_handler(CompanyArchived)
    @app.exception_handler(InvalidTransition)
    @app.exception_handler(StateConflict)
    @app.exception_handler(VersionConflict)
    async def handle_conflict(request: Request, error: Exception) -> JSONResponse:
        if isinstance(
            error,
            (VersionConflict, StateConflict, InvalidTransition, CompanyArchived, ApprovalInvalid),
        ):
            code = error.code
        else:
            code = "CONFLICT"
        return _error_response(
            request,
            status_code=409,
            code=code,
            message="request conflicts with current state",
        )

    @app.exception_handler(sqlite3.IntegrityError)
    async def handle_database_conflict(
        request: Request, _error: sqlite3.IntegrityError
    ) -> JSONResponse:
        return _error_response(
            request,
            status_code=409,
            code="CONFLICT",
            message="request conflicts with current state",
        )

    @app.exception_handler(DataIntegrityError)
    async def handle_data_integrity_error(
        request: Request, _error: DataIntegrityError
    ) -> JSONResponse:
        return _error_response(
            request,
            status_code=500,
            code="INTERNAL_ERROR",
            message="internal server error",
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, _error: Exception) -> JSONResponse:
        return _error_response(
            request,
            status_code=500,
            code="INTERNAL_ERROR",
            message="internal server error",
        )

    def require_bearer(request: Request) -> None:
        verify_bearer_authorization(_authorization_header(request), expected_token)

    @app.get("/v1/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    app.include_router(create_company_router(settings, require_bearer))
    app.include_router(create_brief_router(settings, require_bearer))
    app.include_router(create_job_router(settings, require_bearer))
    app.include_router(create_artifact_router(settings, require_bearer))
    app.include_router(create_callback_router(settings))

    return app
