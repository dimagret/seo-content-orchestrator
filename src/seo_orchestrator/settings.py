"""Environment-backed settings for the isolated orchestrator worker."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

_DEFAULT_DB_PATH = "/opt/data/seo-orchestrator-data/seo.db"
_DEFAULT_ARTIFACT_ROOT = "/opt/data/seo-orchestrator-data/artifacts"
_DEFAULT_LISTEN = "unix:/run/seo-orchestrator/worker.sock"
_ALLOWED_ENVIRONMENTS = frozenset({"development", "test", "production"})


@dataclass(frozen=True)
class Settings:
    """Validated runtime settings loaded from an explicit environment mapping."""

    environment: str
    db_path: Path
    artifact_root: Path
    listen: str
    worker_socket_mode: int = 0o660
    max_active_jobs_per_user: int = 1

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Settings:
        """Build settings from environment variables without reading global process state."""
        environment = env.get("SEO_ENV", "development")
        db_path = Path(env.get("SEO_DB_PATH", _DEFAULT_DB_PATH))
        artifact_root = Path(env.get("SEO_ARTIFACT_ROOT", _DEFAULT_ARTIFACT_ROOT))
        listen = env.get("SEO_LISTEN", _DEFAULT_LISTEN)

        if environment not in _ALLOWED_ENVIRONMENTS:
            allowed = ", ".join(sorted(_ALLOWED_ENVIRONMENTS))
            raise ValueError(f"SEO environment must be one of: {allowed}")
        if not db_path.is_absolute():
            raise ValueError("SEO_DB_PATH must be absolute")
        if not artifact_root.is_absolute():
            raise ValueError("SEO_ARTIFACT_ROOT must be absolute")
        if environment == "production" and not listen.startswith("unix:"):
            raise ValueError("Production SEO_LISTEN must use a Unix socket")

        socket_mode_text = env.get("SEO_WORKER_SOCKET_MODE", "0660")
        try:
            worker_socket_mode = int(socket_mode_text, 8)
        except ValueError as error:
            raise ValueError("SEO_WORKER_SOCKET_MODE must be an octal string") from error

        max_jobs_text = env.get("SEO_MAX_ACTIVE_JOBS_PER_USER", "1")
        try:
            max_active_jobs_per_user = int(max_jobs_text)
        except ValueError as error:
            raise ValueError("SEO_MAX_ACTIVE_JOBS_PER_USER must be a positive integer") from error
        if max_active_jobs_per_user <= 0:
            raise ValueError("SEO_MAX_ACTIVE_JOBS_PER_USER must be positive")

        return cls(
            environment=environment,
            db_path=db_path,
            artifact_root=artifact_root,
            listen=listen,
            worker_socket_mode=worker_socket_mode,
            max_active_jobs_per_user=max_active_jobs_per_user,
        )
