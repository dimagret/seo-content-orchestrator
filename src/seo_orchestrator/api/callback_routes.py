"""Authenticated n8n callback route with bounded raw-body handling."""

from __future__ import annotations

import re
import time
from datetime import UTC, datetime

from fastapi import APIRouter, Request, status
from pydantic import ValidationError

from seo_orchestrator.api.auth import ApiAuthenticationError, load_hmac_key
from seo_orchestrator.db.connection import connect
from seo_orchestrator.domain.models import DomainModel, Identifier, Sha256Hex
from seo_orchestrator.security.signatures import SignatureVerificationError, verify_request
from seo_orchestrator.services.callbacks import CallbackService
from seo_orchestrator.settings import Settings

_MAX_CALLBACK_BYTES = 64 * 1024
_TIMESTAMP_PATTERN = re.compile(r"[0-9]{1,16}\Z")


class CallbackPayload(DomainModel):
    """Minimal signed correlation payload; later stages process callback details."""

    company_id: Identifier
    job_id: Identifier
    snapshot_hash: Sha256Hex


def _required_single_header(request: Request, name: str) -> str:
    values = request.headers.getlist(name)
    if len(values) != 1:
        raise ApiAuthenticationError("unauthorized")
    return values[0]


def _timestamp(value: str) -> int:
    if _TIMESTAMP_PATTERN.fullmatch(value) is None:
        raise ApiAuthenticationError("unauthorized")
    return int(value)


async def _bounded_raw_body(request: Request) -> bytes:
    lengths = request.headers.getlist("content-length")
    if len(lengths) > 1:
        raise ApiAuthenticationError("unauthorized")
    if lengths and (
        _TIMESTAMP_PATTERN.fullmatch(lengths[0]) is None
        or int(lengths[0]) > _MAX_CALLBACK_BYTES
    ):
        raise ApiAuthenticationError("unauthorized")
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > _MAX_CALLBACK_BYTES:
            raise ApiAuthenticationError("unauthorized")
        chunks.append(chunk)
    return b"".join(chunks)


def create_callback_router(settings: Settings) -> APIRouter:
    """Build the HMAC-authenticated callback endpoint without bearer credentials."""
    router = APIRouter()

    @router.post("/v1/callbacks/n8n", status_code=status.HTTP_202_ACCEPTED)
    async def accept_n8n_callback(request: Request) -> dict[str, str]:
        content_type = request.headers.get("content-type", "")
        if content_type.split(";", 1)[0].strip().lower() != "application/json":
            raise ApiAuthenticationError("unauthorized")
        body = await _bounded_raw_body(request)
        timestamp = _timestamp(_required_single_header(request, "x-seo-timestamp"))
        nonce = _required_single_header(request, "x-seo-nonce")
        idempotency_key = _required_single_header(request, "x-seo-idempotency-key")
        signature = _required_single_header(request, "x-seo-signature")
        try:
            verify_request(
                request.method,
                request.url.path,
                timestamp,
                nonce,
                body,
                load_hmac_key(settings.callback_hmac_key_path),
                signature,
                now=int(time.time()),
            )
        except SignatureVerificationError as exc:
            raise ApiAuthenticationError("unauthorized") from exc
        try:
            payload = CallbackPayload.model_validate_json(body)
        except ValidationError as exc:
            raise ApiAuthenticationError("unauthorized") from exc
        if idempotency_key != payload.job_id:
            raise ApiAuthenticationError("unauthorized")
        connection = connect(settings.db_path)
        try:
            CallbackService(connection, company_id=payload.company_id).accept_callback(
                job_id=payload.job_id,
                snapshot_hash=payload.snapshot_hash,
                idempotency_key=idempotency_key,
                nonce=nonce,
                signed_timestamp=timestamp,
                received_at=datetime.now(UTC),
            )
        finally:
            connection.close()
        return {"status": "accepted"}

    return router
