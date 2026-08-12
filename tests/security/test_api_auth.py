import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from seo_orchestrator.settings import Settings

_TOKEN_HEX = "0f" * 32
_WRONG_TOKEN_HEX = "f0" * 32
_MAX_REQUEST_BODY_BYTES = 64 * 1024


def _token_file(tmp_path: Path) -> Path:
    path = tmp_path / "worker-api.token"
    path.write_text(_TOKEN_HEX, encoding="ascii")
    path.chmod(0o600)
    return path


def _app(tmp_path: Path) -> Any:
    from seo_orchestrator.api.app import create_app

    return create_app(
        Settings(
            environment="test",
            db_path=tmp_path / "worker.db",
            artifact_root=tmp_path / "artifacts",
            listen="unix:/run/seo-orchestrator/worker.sock",
            api_token_path=_token_file(tmp_path),
        )
    )


async def _get(app: Any, path: str, authorization: str | None = None) -> httpx.Response:
    headers = {} if authorization is None else {"authorization": authorization}
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.get(path, headers=headers)


async def _post_without_authorization(app: Any, path: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.post(path, json={})


async def _post_with_authorization(app: Any, path: str, payload: dict[str, object]) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
        return await client.post(
            path,
            headers={"authorization": f"Bearer {_TOKEN_HEX}"},
            json=payload,
        )


def test_health_is_anonymous_and_contains_no_runtime_detail(tmp_path: Path) -> None:
    response = asyncio.run(_get(_app(tmp_path), "/v1/health"))

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.parametrize(
    "authorization",
    [
        None,
        "Basic unsupported",
        "Bearer",
        f"Bearer {_WRONG_TOKEN_HEX}",
        f"Bearer {'x' * 513}",
    ],
)
def test_company_collection_rejects_invalid_bearer_authorization(
    tmp_path: Path, authorization: str | None
) -> None:
    response = asyncio.run(_get(_app(tmp_path), "/v1/companies", authorization))

    assert response.status_code == 401
    body = response.json()
    assert body["code"] == "UNAUTHORIZED"
    assert body["message"] == "unauthorized"
    assert isinstance(body["request_id"], str)
    assert body["request_id"]


def test_company_create_requires_bearer_before_body_validation(tmp_path: Path) -> None:
    response = asyncio.run(_post_without_authorization(_app(tmp_path), "/v1/companies"))

    assert response.status_code == 401
    assert response.json()["code"] == "UNAUTHORIZED"


def test_malformed_authenticated_request_uses_a_stable_non_leaking_error_envelope(
    tmp_path: Path,
) -> None:
    response = asyncio.run(_post_with_authorization(_app(tmp_path), "/v1/companies", {}))

    assert response.status_code == 422
    body = response.json()
    assert body["code"] == "VALIDATION_ERROR"
    assert body["message"] == "invalid request"
    assert body["request_id"]
    assert "detail" not in body


def test_callback_hmac_key_uses_the_same_fd_safe_secret_loader(tmp_path: Path) -> None:
    from seo_orchestrator.api.auth import ApiTokenConfigurationError, load_hmac_key

    key_path = _token_file(tmp_path)

    assert load_hmac_key(key_path) == bytes.fromhex(_TOKEN_HEX)
    key_path.chmod(0o640)
    with pytest.raises(ApiTokenConfigurationError):
        load_hmac_key(key_path)


def test_secret_loader_rejects_a_fifo_without_blocking(tmp_path: Path) -> None:
    fifo = tmp_path / "worker-api.token"
    os.mkfifo(fifo, 0o600)
    script = """
import sys
from pathlib import Path

from seo_orchestrator.api.auth import ApiTokenConfigurationError, load_api_token

try:
    load_api_token(Path(sys.argv[1]))
except ApiTokenConfigurationError:
    raise SystemExit(0)
raise SystemExit(1)
"""
    try:
        result = subprocess.run(
            [sys.executable, "-c", script, str(fifo)],
            check=False,
            capture_output=True,
            text=True,
            timeout=1,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("secret loader blocked on FIFO")

    assert result.returncode == 0


def test_unexpected_exception_uses_a_stable_non_leaking_error_envelope(tmp_path: Path) -> None:
    app = _app(tmp_path)

    @app.get("/v1/testing/unexpected-error")
    def unexpected_error() -> None:
        raise RuntimeError("internal diagnostic must not reach the API response")

    async def request() -> httpx.Response:
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
            return await client.get("/v1/testing/unexpected-error")

    response = asyncio.run(request())

    assert response.status_code == 500
    assert response.json()["code"] == "INTERNAL_ERROR"
    assert response.json()["message"] == "internal server error"
    assert response.json()["request_id"]
    assert "diagnostic" not in response.text
    assert response.headers["x-request-id"] == response.json()["request_id"]


def test_framework_http_errors_use_stable_correlated_error_envelopes(tmp_path: Path) -> None:
    app = _app(tmp_path)

    async def requests() -> tuple[httpx.Response, httpx.Response]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
            return (
                await client.get("/v1/does-not-exist"),
                await client.post("/v1/health"),
            )

    not_found, method_not_allowed = asyncio.run(requests())

    for response, status_code, code, message in (
        (not_found, 404, "NOT_FOUND", "not found"),
        (method_not_allowed, 405, "METHOD_NOT_ALLOWED", "method not allowed"),
    ):
        assert response.status_code == status_code
        assert response.json() == {
            "code": code,
            "message": message,
            "request_id": response.headers["x-request-id"],
        }


@pytest.mark.parametrize("streamed", [False, True])
def test_request_body_limit_rejects_oversized_input_before_route_processing(
    tmp_path: Path, streamed: bool
) -> None:
    app = _app(tmp_path)

    async def oversized_stream() -> AsyncIterator[bytes]:
        yield b"x" * (_MAX_REQUEST_BODY_BYTES + 1)

    async def request() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        content: bytes | AsyncIterator[bytes]
        content = oversized_stream() if streamed else b"x" * (_MAX_REQUEST_BODY_BYTES + 1)
        async with httpx.AsyncClient(transport=transport, base_url="http://worker.test") as client:
            return await client.post(
                "/v1/companies",
                content=content,
                headers={"content-type": "application/json"},
            )

    response = asyncio.run(request())

    assert response.status_code == 413
    assert response.json() == {
        "code": "REQUEST_TOO_LARGE",
        "message": "request body exceeds limit",
        "request_id": response.headers["x-request-id"],
    }
