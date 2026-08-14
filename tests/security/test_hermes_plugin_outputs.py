"""Security contract for the profile-local Hermes Unix-socket client."""

from __future__ import annotations

import json
import math
import socketserver
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

import pytest


class _ThreadingUnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


class _WorkerHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    seen_authorization: str | None = None

    def do_GET(self) -> None:
        type(self).seen_authorization = self.headers.get("authorization")
        if self.path == "/slow":
            time.sleep(0.2)
        if self.path == "/drip":
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", "2")
            self.end_headers()
            try:
                for byte in b"{}":
                    time.sleep(0.075)
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
            except BrokenPipeError:
                pass
            return
        if self.path == "/large":
            self._reply(200, b"x" * 2_048)
            return
        if self.path == "/wrong-type":
            self._reply(200, b"not-json", content_type="text/plain")
            return
        if self.path == "/duplicate":
            self._reply(200, b'{"job_id":"one","job_id":"two"}')
            return
        if self.path == "/nan":
            self._reply(200, b'{"value": NaN}')
            return
        if self.path == "/overflow":
            self._reply(200, b'{"value": 1e400}')
            return
        if self.path == "/echo-token":
            self._reply(200, b'{"note":"test-bearer-secret"}')
            return
        if self.path == "/echo-token-escaped":
            self._reply(200, b'{"note":"\\u0074est-bearer-secret"}')
            return
        if self.path == "/scalar":
            self._reply(200, b'"unexpected scalar"')
            return
        if self.path == "/utf16":
            self._reply(200, '{"ok":true}'.encode("utf-16"))
            return
        if self.path == "/wrong-charset":
            self._reply(
                200,
                b'{"ok":true}',
                content_type="application/json; charset=iso-8859-1",
            )
            return
        if self.path == "/markdown":
            self._reply(200, "# Результат".encode(), content_type="text/markdown")
            return
        if self.path == "/bad-utf8":
            self._reply(200, b"\xff", content_type="text/markdown")
            return
        if self.path == "/error":
            secret = self.headers.get("authorization", "missing")
            self._reply(500, f"upstream echoed {secret}".encode())
            return
        if self.path == "/unsafe-request-id":
            self._reply(500, b"error", request_id="Bearer leaked-value")
            return
        self._reply(200, json.dumps({"ok": True}).encode())

    def _reply(
        self,
        status: int,
        body: bytes,
        *,
        content_type: str = "application/json",
        request_id: str = "0123456789abcdef0123456789abcdef",
    ) -> None:
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(body)))
        self.send_header("x-request-id", request_id)
        self.end_headers()
        try:
            self.wfile.write(body)
        except BrokenPipeError:
            pass

    def log_message(self, format: str, *args: Any) -> None:
        del format, args


@contextmanager
def _worker_server(tmp_path: Path) -> Iterator[Path]:
    socket_path = tmp_path / "worker.sock"
    _WorkerHandler.seen_authorization = None
    server = _ThreadingUnixServer(str(socket_path), _WorkerHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield socket_path
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


def _client(tmp_path: Path, socket_path: Path, *, timeout: float = 0.1, cap: int = 1_024) -> Any:
    from integrations.hermes.seo_orchestrator.client import WorkerClient

    token_path = tmp_path / "worker.token"
    token_path.write_text("test-bearer-secret\n", encoding="utf-8")
    return WorkerClient(
        socket_path=socket_path,
        token_path=token_path,
        timeout_seconds=timeout,
        max_response_bytes=cap,
    )


def test_client_uses_bearer_file_over_unix_socket(tmp_path: Path) -> None:
    with _worker_server(tmp_path) as socket_path:
        result = _client(tmp_path, socket_path).request_json("GET", "/ok")

    assert result == {"ok": True}
    assert _WorkerHandler.seen_authorization == "Bearer test-bearer-secret"


@pytest.mark.parametrize(
    ("path", "error_code"),
    [
        ("/slow", "WORKER_TIMEOUT"),
        ("/drip", "WORKER_TIMEOUT"),
        ("/large", "RESPONSE_TOO_LARGE"),
    ],
)
def test_client_bounds_time_and_response_size(
    tmp_path: Path, path: str, error_code: str
) -> None:
    from integrations.hermes.seo_orchestrator.client import WorkerClientError

    with _worker_server(tmp_path) as socket_path, pytest.raises(
        WorkerClientError
    ) as failure:
        _client(tmp_path, socket_path).request_json("GET", path)

    assert failure.value.error_code == error_code


@pytest.mark.parametrize(
    "path",
    [
        "/wrong-type",
        "/duplicate",
        "/nan",
        "/overflow",
        "/echo-token",
        "/echo-token-escaped",
        "/scalar",
        "/utf16",
        "/wrong-charset",
    ],
)
def test_client_requires_strict_json_response(tmp_path: Path, path: str) -> None:
    from integrations.hermes.seo_orchestrator.client import WorkerClientError

    with _worker_server(tmp_path) as socket_path, pytest.raises(
        WorkerClientError
    ) as failure:
        _client(tmp_path, socket_path).request_json("GET", path)

    assert failure.value.error_code == "INVALID_RESPONSE"


def test_client_bounds_and_strictly_serializes_requests(tmp_path: Path) -> None:
    from integrations.hermes.seo_orchestrator.client import WorkerClient, WorkerClientError

    token_path = tmp_path / "worker.token"
    token_path.write_text("test-bearer-secret\n", encoding="utf-8")
    client = WorkerClient(
        socket_path=tmp_path / "unused.sock",
        token_path=token_path,
        max_request_bytes=32,
    )
    with pytest.raises(WorkerClientError) as oversized:
        client.request_json("POST", "/echo", payload={"value": "x" * 64})
    with pytest.raises(WorkerClientError) as nonfinite:
        client.request_json("POST", "/echo", payload={"value": math.nan})
    with pytest.raises(WorkerClientError) as structurally_large:
        client.request_json("POST", "/echo", payload={"items": ["x"] * 100})
    assert oversized.value.error_code == "REQUEST_TOO_LARGE"
    assert nonfinite.value.error_code == "INVALID_ARGUMENT"
    assert structurally_large.value.error_code == "REQUEST_TOO_LARGE"


def test_client_errors_do_not_expose_authorization_or_worker_body(tmp_path: Path) -> None:
    from integrations.hermes.seo_orchestrator.client import WorkerClientError

    with _worker_server(tmp_path) as socket_path:
        client = _client(tmp_path, socket_path)
        with pytest.raises(WorkerClientError) as failure:
            client.request_json("GET", "/error")

    rendered = str(failure.value)
    assert failure.value.error_code == "INVALID_RESPONSE"
    assert failure.value.request_id == "0123456789abcdef0123456789abcdef"
    assert "test-bearer-secret" not in rendered
    assert "authorization" not in rendered.lower()
    assert "upstream echoed" not in rendered


def test_client_accepts_only_valid_utf8_for_allowlisted_text(tmp_path: Path) -> None:
    from integrations.hermes.seo_orchestrator.client import WorkerClientError

    with _worker_server(tmp_path) as socket_path:
        client = _client(tmp_path, socket_path)
        assert client.request_text(
            "GET",
            "/markdown",
            accepted_content_types=frozenset({"text/markdown"}),
        ) == "# Результат"
        with pytest.raises(WorkerClientError) as failure:
            client.request_text(
                "GET",
                "/bad-utf8",
                accepted_content_types=frozenset({"text/markdown"}),
            )

    assert failure.value.error_code == "INVALID_RESPONSE"


def test_client_omits_untrusted_request_id_header(tmp_path: Path) -> None:
    from integrations.hermes.seo_orchestrator.client import WorkerClientError

    with _worker_server(tmp_path) as socket_path, pytest.raises(
        WorkerClientError
    ) as failure:
        _client(tmp_path, socket_path).request_json("GET", "/unsafe-request-id")

    assert failure.value.error_code == "WORKER_HTTP_ERROR"
    assert failure.value.request_id is None
    assert "leaked-value" not in str(failure.value)
