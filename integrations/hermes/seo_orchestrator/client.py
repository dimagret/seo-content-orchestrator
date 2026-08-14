"""Bounded stdlib HTTP client for the isolated SEO worker Unix socket."""

from __future__ import annotations

import http.client
import json
import math
import re
import socket
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

DEFAULT_SOCKET_PATH = Path("/opt/data/seo-runtime/worker.sock")
DEFAULT_TOKEN_PATH = Path("/opt/data/seo-runtime/worker-api.token")
_REQUEST_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")


class WorkerClientError(RuntimeError):
    """Safe model-facing worker failure without response or credential content."""

    def __init__(
        self,
        error_code: str,
        message: str,
        *,
        status_code: int | None = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.status_code = status_code
        self.request_id = request_id


class _DeadlineSocket(socket.socket):
    def __init__(self, deadline: float) -> None:
        super().__init__(socket.AF_UNIX, socket.SOCK_STREAM)
        self._deadline = deadline

    def _arm(self) -> None:
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError
        self.settimeout(remaining)


    def recv_into(self, buffer: Any, nbytes: int = 0, flags: int = 0) -> int:
        self._arm()
        return super().recv_into(buffer, nbytes, flags)

    def send(self, data: Any, flags: int = 0) -> int:
        self._arm()
        return super().send(data, flags)

    def sendall(self, data: Any, flags: int = 0) -> None:
        self._arm()
        super().sendall(data, flags)


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: Path, timeout: float) -> None:
        super().__init__("localhost", timeout=timeout)
        self._socket_path = socket_path

    def connect(self) -> None:
        if self.timeout is None:
            raise ValueError("timeout is required")
        connection = _DeadlineSocket(time.monotonic() + self.timeout)
        try:
            connection._arm()
            connection.connect(str(self._socket_path))
        except BaseException:
            connection.close()
            raise
        self.sock = connection


class WorkerClient:
    """Synchronous bounded client used by Hermes tool handlers."""

    def __init__(
        self,
        *,
        socket_path: Path = DEFAULT_SOCKET_PATH,
        token_path: Path = DEFAULT_TOKEN_PATH,
        timeout_seconds: float = 5.0,
        max_request_bytes: int = 64 * 1024,
        max_response_bytes: int = 1024 * 1024,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_request_bytes <= 0 or max_response_bytes <= 0:
            raise ValueError("request and response byte limits must be positive")
        self._socket_path = socket_path
        self._token_path = token_path
        self._timeout_seconds = timeout_seconds
        self._max_request_bytes = max_request_bytes
        self._max_response_bytes = max_response_bytes

    def _bearer_token(self) -> str:
        try:
            with self._token_path.open("rb") as token_file:
                raw_token = token_file.read(1025)
            if len(raw_token) > 1024:
                raise ValueError("token file exceeded limit")
            token = raw_token.decode("utf-8").strip()
        except (OSError, UnicodeError, ValueError) as exc:
            raise WorkerClientError(
                "WORKER_AUTH_UNAVAILABLE", "worker authentication is unavailable"
            ) from exc
        if not token or len(token) > 1024 or any(character.isspace() for character in token):
            raise WorkerClientError(
                "WORKER_AUTH_UNAVAILABLE", "worker authentication is unavailable"
            )
        return token

    @staticmethod
    def _target(path: str, query: dict[str, str] | None) -> str:
        if not path.startswith("/") or "\r" in path or "\n" in path or "?" in path:
            raise ValueError("path must be an origin-form path without a query")
        if not query:
            return path
        return f"{path}?{urlencode(query)}"

    def _preflight_payload(self, payload: Any) -> None:
        remaining = self._max_request_bytes
        stack: list[tuple[Any, int]] = [(payload, 0)]
        nodes = 0
        while stack:
            value, depth = stack.pop()
            nodes += 1
            if nodes > 4096 or depth > 32:
                raise WorkerClientError("INVALID_ARGUMENT", "worker request payload is invalid")
            if isinstance(value, str):
                if len(value) > remaining:
                    raise WorkerClientError("REQUEST_TOO_LARGE", "worker request exceeded limit")
                remaining -= len(value.encode("utf-8"))
            elif isinstance(value, dict):
                stack.extend((key, depth + 1) for key in value)
                stack.extend((child, depth + 1) for child in value.values())
            elif isinstance(value, (list, tuple)):
                stack.extend((child, depth + 1) for child in value)
            elif isinstance(value, float) and not math.isfinite(value):
                raise WorkerClientError("INVALID_ARGUMENT", "worker request payload is invalid")
            remaining -= 2
            if remaining < 0:
                raise WorkerClientError("REQUEST_TOO_LARGE", "worker request exceeded limit")

    @staticmethod
    def _contains_exact_token(value: Any, token: str) -> bool:
        stack = [value]
        while stack:
            current = stack.pop()
            if isinstance(current, str) and token in current:
                return True
            if isinstance(current, dict):
                stack.extend(current.keys())
                stack.extend(current.values())
            elif isinstance(current, list):
                stack.extend(current)
        return False

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        query: dict[str, str] | None = None,
    ) -> tuple[http.client.HTTPResponse, bytes, str]:
        if method not in {"GET", "POST", "PATCH"}:
            raise ValueError("unsupported worker HTTP method")
        body = None
        token = self._bearer_token()
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {token}",
            "connection": "close",
        }
        if payload is not None:
            self._preflight_payload(payload)
            try:
                body = json.dumps(
                    payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            except (RecursionError, TypeError, ValueError) as exc:
                raise WorkerClientError(
                    "INVALID_ARGUMENT", "worker request payload is invalid"
                ) from exc
            if len(body) > self._max_request_bytes:
                raise WorkerClientError(
                    "REQUEST_TOO_LARGE", "worker request exceeded limit"
                )
            headers["content-type"] = "application/json"
        connection = _UnixHTTPConnection(self._socket_path, self._timeout_seconds)
        try:
            connection.request(method, self._target(path, query), body=body, headers=headers)
            response = connection.getresponse()
            content = response.read(self._max_response_bytes + 1)
        except TimeoutError as exc:
            raise WorkerClientError("WORKER_TIMEOUT", "worker request timed out") from exc
        except OSError as exc:
            raise WorkerClientError("WORKER_UNAVAILABLE", "worker is unavailable") from exc
        finally:
            connection.close()
        if len(content) > self._max_response_bytes:
            raise WorkerClientError("RESPONSE_TOO_LARGE", "worker response exceeded limit")
        request_id_header = response.getheader("x-request-id")
        request_id = (
            request_id_header
            if request_id_header is not None
            and _REQUEST_ID_PATTERN.fullmatch(request_id_header) is not None
            else None
        )
        if token.encode("utf-8") in content:
            raise WorkerClientError(
                "INVALID_RESPONSE",
                "worker returned an invalid response",
                request_id=request_id,
            )
        if not 200 <= response.status < 300:
            raise WorkerClientError(
                "WORKER_HTTP_ERROR",
                "worker rejected the request",
                status_code=response.status,
                request_id=request_id,
            )
        return response, content, token

    def request_json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        query: dict[str, str] | None = None,
    ) -> Any:
        """Return one strict JSON response, rejecting parser differentials."""
        response, content, token = self._request(
            method, path, payload=payload, query=query
        )
        content_type, charset = self._content_type(response)
        if content_type != "application/json" or charset not in {None, "utf-8"}:
            raise WorkerClientError("INVALID_RESPONSE", "worker returned an invalid response")

        def reject_duplicate(key_values: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in key_values:
                if key in result:
                    raise ValueError("duplicate JSON key")
                result[key] = value
            return result

        def reject_constant(_value: str) -> None:
            raise ValueError("non-finite JSON number")

        def reject_nonfinite_float(value: str) -> float:
            parsed = float(value)
            if not math.isfinite(parsed):
                raise ValueError("non-finite JSON number")
            return parsed

        try:
            result = json.loads(
                content.decode("utf-8"),
                object_pairs_hook=reject_duplicate,
                parse_constant=reject_constant,
                parse_float=reject_nonfinite_float,
            )
            if not isinstance(result, (dict, list)):
                raise TypeError("JSON root must be an object or array")
            if self._contains_exact_token(result, token):
                raise ValueError("response contained authentication material")
            return result
        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
            RecursionError,
            TypeError,
            ValueError,
        ) as exc:
            raise WorkerClientError(
                "INVALID_RESPONSE", "worker returned an invalid response"
            ) from exc

    def request_text(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        accepted_content_types: frozenset[str],
    ) -> str:
        """Return bounded UTF-8 text for explicitly allowlisted media types."""
        response, content, _token = self._request(method, path, query=query)
        content_type, charset = self._content_type(response)
        if content_type not in accepted_content_types or charset not in {None, "utf-8"}:
            raise WorkerClientError("INVALID_RESPONSE", "worker returned an invalid response")
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise WorkerClientError(
                "INVALID_RESPONSE", "worker returned an invalid response"
            ) from exc

    @staticmethod
    def _content_type(response: http.client.HTTPResponse) -> tuple[str, str | None]:
        parts = [
            part.strip().lower()
            for part in response.getheader("content-type", "").split(";")
        ]
        charset: str | None = None
        for parameter in parts[1:]:
            if parameter.startswith("charset="):
                if charset is not None:
                    raise WorkerClientError(
                        "INVALID_RESPONSE", "worker returned an invalid response"
                    )
                charset = parameter.removeprefix("charset=").strip('"')
        return parts[0], charset
