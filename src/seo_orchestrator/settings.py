"""Environment-backed settings for the isolated orchestrator worker."""

# Settings intentionally expose ValueError for every caller-visible validation failure.
# ruff: noqa: TRY004

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

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

    def __post_init__(self) -> None:
        """Enforce the same runtime invariants for every construction path."""
        if not isinstance(self.environment, str):
            raise ValueError("SEO_ENV must be a string")
        if self.environment not in _ALLOWED_ENVIRONMENTS:
            allowed = ", ".join(sorted(_ALLOWED_ENVIRONMENTS))
            raise ValueError(f"SEO environment must be one of: {allowed}")
        if not isinstance(self.db_path, Path):
            raise ValueError("SEO_DB_PATH must be a pathlib.Path")
        if not self.db_path.is_absolute():
            raise ValueError("SEO_DB_PATH must be absolute")
        if not isinstance(self.artifact_root, Path):
            raise ValueError("SEO_ARTIFACT_ROOT must be a pathlib.Path")
        if not self.artifact_root.is_absolute():
            raise ValueError("SEO_ARTIFACT_ROOT must be absolute")
        if not isinstance(self.listen, str):
            raise ValueError("SEO_LISTEN must be a string")

        if self.environment == "production":
            socket_path = self.listen.removeprefix("unix:")
            path_segments = socket_path.split("/")
            if (
                not self.listen.startswith("unix:")
                or not socket_path
                or "\0" in socket_path
                or not PurePosixPath(socket_path).is_absolute()
                or socket_path == "/"
                or socket_path.endswith("/")
                or "." in path_segments
                or ".." in path_segments
            ):
                raise ValueError("Production SEO_LISTEN must use an absolute Unix socket path")

        mode = self.worker_socket_mode
        if (
            not isinstance(mode, int)
            or isinstance(mode, bool)
            or not 0 <= mode <= 0o777
            or mode & 0o700 != 0o600
            or mode & 0o111
            or mode & 0o007
        ):
            raise ValueError("SEO_WORKER_SOCKET_MODE has an unsafe socket mode")

        job_limit = self.max_active_jobs_per_user
        if not isinstance(job_limit, int) or isinstance(job_limit, bool) or job_limit <= 0:
            raise ValueError("SEO_MAX_ACTIVE_JOBS_PER_USER must be a positive integer")

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Settings:
        """Build settings from environment variables without reading global process state."""
        environment = env.get("SEO_ENV", "development")
        db_path_text = env.get("SEO_DB_PATH", _DEFAULT_DB_PATH)
        artifact_root_text = env.get("SEO_ARTIFACT_ROOT", _DEFAULT_ARTIFACT_ROOT)
        listen = env.get("SEO_LISTEN", _DEFAULT_LISTEN)

        for name, value in (
            ("SEO_ENV", environment),
            ("SEO_DB_PATH", db_path_text),
            ("SEO_ARTIFACT_ROOT", artifact_root_text),
            ("SEO_LISTEN", listen),
        ):
            if not isinstance(value, str):
                raise ValueError(f"{name} must be a string")

        try:
            db_path = Path(db_path_text)
        except (TypeError, ValueError) as error:
            raise ValueError("SEO_DB_PATH must be a valid path string") from error
        try:
            artifact_root = Path(artifact_root_text)
        except (TypeError, ValueError) as error:
            raise ValueError("SEO_ARTIFACT_ROOT must be a valid path string") from error

        socket_mode_text = env.get("SEO_WORKER_SOCKET_MODE", "0660")
        if not isinstance(socket_mode_text, str):
            raise ValueError("SEO_WORKER_SOCKET_MODE must be an octal string")
        try:
            worker_socket_mode = int(socket_mode_text, 8)
        except (TypeError, ValueError) as error:
            raise ValueError("SEO_WORKER_SOCKET_MODE must be an octal string") from error

        max_jobs_text = env.get("SEO_MAX_ACTIVE_JOBS_PER_USER", "1")
        if not isinstance(max_jobs_text, str):
            raise ValueError("SEO_MAX_ACTIVE_JOBS_PER_USER must be a positive integer")
        try:
            max_active_jobs_per_user = int(max_jobs_text)
        except (TypeError, ValueError) as error:
            raise ValueError("SEO_MAX_ACTIVE_JOBS_PER_USER must be a positive integer") from error
        return cls(
            environment=environment,
            db_path=db_path,
            artifact_root=artifact_root,
            listen=listen,
            worker_socket_mode=worker_socket_mode,
            max_active_jobs_per_user=max_active_jobs_per_user,
        )
