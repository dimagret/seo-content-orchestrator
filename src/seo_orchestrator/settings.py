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
_DEFAULT_API_TOKEN_PATH = "/opt/data/seo-orchestrator-data/worker-api.token"
_DEFAULT_CALLBACK_HMAC_KEY_PATH = "/opt/data/seo-orchestrator-data/n8n-callback-hmac.key"
_ALLOWED_ENVIRONMENTS = frozenset({"development", "test", "production"})


def _unix_socket_path(listen: object) -> Path:
    if type(listen) is not str:
        raise ValueError("SEO_LISTEN must be a string")
    socket_text = listen.removeprefix("unix:")
    path_segments = socket_text.split("/")
    if (
        not listen.startswith("unix:")
        or not socket_text
        or "\0" in socket_text
        or not PurePosixPath(socket_text).is_absolute()
        or socket_text == "/"
        or socket_text.endswith("/")
        or "." in path_segments
        or ".." in path_segments
    ):
        raise ValueError("SEO_LISTEN must use an absolute Unix socket path")
    return Path(socket_text)


@dataclass(frozen=True)
class Settings:
    """Validated runtime settings loaded from an explicit environment mapping."""

    environment: str
    db_path: Path
    artifact_root: Path
    listen: str
    api_token_path: Path = Path(_DEFAULT_API_TOKEN_PATH)
    callback_hmac_key_path: Path = Path(_DEFAULT_CALLBACK_HMAC_KEY_PATH)
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
        if not isinstance(self.api_token_path, Path):
            raise ValueError("SEO_API_TOKEN_PATH must be a pathlib.Path")
        if not self.api_token_path.is_absolute():
            raise ValueError("SEO_API_TOKEN_PATH must be absolute")
        if not isinstance(self.callback_hmac_key_path, Path):
            raise ValueError("SEO_CALLBACK_HMAC_KEY_PATH must be a pathlib.Path")
        if not self.callback_hmac_key_path.is_absolute():
            raise ValueError("SEO_CALLBACK_HMAC_KEY_PATH must be absolute")
        if not isinstance(self.listen, str):
            raise ValueError("SEO_LISTEN must be a string")

        if self.environment == "production":
            try:
                _unix_socket_path(self.listen)
            except ValueError as error:
                raise ValueError(
                    "Production SEO_LISTEN must use an absolute Unix socket path"
                ) from error

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

    @property
    def socket_path(self) -> Path:
        """Return the validated Unix socket target required by the serve command."""
        return _unix_socket_path(self.listen)

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Settings:
        """Build settings from environment variables without reading global process state."""
        environment = env.get("SEO_ENV", "development")
        db_path_text = env.get("SEO_DB_PATH", _DEFAULT_DB_PATH)
        artifact_root_text = env.get("SEO_ARTIFACT_ROOT", _DEFAULT_ARTIFACT_ROOT)
        listen = env.get("SEO_LISTEN", _DEFAULT_LISTEN)
        api_token_path_text = env.get("SEO_API_TOKEN_PATH", _DEFAULT_API_TOKEN_PATH)
        callback_hmac_key_path_text = env.get(
            "SEO_CALLBACK_HMAC_KEY_PATH", _DEFAULT_CALLBACK_HMAC_KEY_PATH
        )

        for name, value in (
            ("SEO_ENV", environment),
            ("SEO_DB_PATH", db_path_text),
            ("SEO_ARTIFACT_ROOT", artifact_root_text),
            ("SEO_LISTEN", listen),
            ("SEO_API_TOKEN_PATH", api_token_path_text),
            ("SEO_CALLBACK_HMAC_KEY_PATH", callback_hmac_key_path_text),
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
        try:
            api_token_path = Path(api_token_path_text)
        except (TypeError, ValueError) as error:
            raise ValueError("SEO_API_TOKEN_PATH must be a valid path string") from error
        try:
            callback_hmac_key_path = Path(callback_hmac_key_path_text)
        except (TypeError, ValueError) as error:
            raise ValueError("SEO_CALLBACK_HMAC_KEY_PATH must be a valid path string") from error

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
            api_token_path=api_token_path,
            callback_hmac_key_path=callback_hmac_key_path,
            worker_socket_mode=worker_socket_mode,
            max_active_jobs_per_user=max_active_jobs_per_user,
        )
