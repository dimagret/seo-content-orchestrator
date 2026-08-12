from pathlib import Path

import pytest

from seo_orchestrator.settings import Settings


def make_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "environment": "development",
        "db_path": Path("/var/lib/seo-orchestrator/seo.db"),
        "artifact_root": Path("/var/lib/seo-orchestrator/artifacts"),
        "listen": "unix:/run/seo-orchestrator/worker.sock",
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def test_relative_db_path_is_rejected() -> None:
    with pytest.raises(ValueError, match="absolute"):
        Settings.from_env({"SEO_DB_PATH": "relative/seo.db"})


def test_relative_artifact_root_is_rejected() -> None:
    with pytest.raises(ValueError, match="absolute"):
        Settings.from_env({"SEO_ARTIFACT_ROOT": "relative/artifacts"})


def test_production_rejects_tcp_listener() -> None:
    env = {"SEO_ENV": "production", "SEO_LISTEN": "127.0.0.1:8787"}

    with pytest.raises(ValueError, match="Unix socket"):
        Settings.from_env(env)


def test_production_accepts_unix_socket() -> None:
    settings = Settings.from_env(
        {
            "SEO_ENV": "production",
            "SEO_LISTEN": "unix:/run/seo-orchestrator/worker.sock",
        }
    )

    assert settings.listen == "unix:/run/seo-orchestrator/worker.sock"


def test_worker_limits_have_safe_defaults() -> None:
    settings = Settings.from_env({})

    assert settings.worker_socket_mode == 0o660
    assert settings.max_active_jobs_per_user == 1


def test_required_settings_have_development_defaults() -> None:
    settings = Settings.from_env({})

    assert settings.environment == "development"
    assert settings.db_path == Path("/opt/data/seo-orchestrator-data/seo.db")
    assert settings.artifact_root == Path("/opt/data/seo-orchestrator-data/artifacts")
    assert settings.listen == "unix:/run/seo-orchestrator/worker.sock"


def test_unknown_environment_is_rejected() -> None:
    with pytest.raises(ValueError, match="environment"):
        Settings.from_env({"SEO_ENV": "staging"})


def test_max_active_jobs_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        Settings.from_env({"SEO_MAX_ACTIVE_JOBS_PER_USER": "0"})


def test_max_active_jobs_can_be_configured() -> None:
    settings = Settings.from_env({"SEO_MAX_ACTIVE_JOBS_PER_USER": "3"})

    assert settings.max_active_jobs_per_user == 3


def test_socket_mode_is_parsed_as_octal() -> None:
    settings = Settings.from_env({"SEO_WORKER_SOCKET_MODE": "0640"})

    assert settings.worker_socket_mode == 0o640


def test_invalid_socket_mode_is_rejected() -> None:
    with pytest.raises(ValueError, match="octal"):
        Settings.from_env({"SEO_WORKER_SOCKET_MODE": "invalid"})


def test_direct_constructor_rejects_unknown_environment() -> None:
    with pytest.raises(ValueError, match="environment"):
        make_settings(environment="staging")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("environment", object()),
        ("db_path", "/var/lib/seo-orchestrator/seo.db"),
        ("artifact_root", object()),
        ("listen", object()),
        ("worker_socket_mode", "0660"),
        ("max_active_jobs_per_user", "1"),
    ],
)
def test_direct_constructor_rejects_wrong_field_types(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        make_settings(**{field: value})


@pytest.mark.parametrize(
    "env",
    [
        {"SEO_ENV": []},
        {"SEO_DB_PATH": object()},
        {"SEO_ARTIFACT_ROOT": object()},
        {"SEO_LISTEN": object()},
        {"SEO_WORKER_SOCKET_MODE": object()},
        {"SEO_MAX_ACTIVE_JOBS_PER_USER": object()},
    ],
)
def test_from_env_rejects_wrong_mapping_value_types(env: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        Settings.from_env(env)  # type: ignore[arg-type]


@pytest.mark.parametrize("field", ["db_path", "artifact_root"])
def test_direct_constructor_rejects_relative_paths(field: str) -> None:
    with pytest.raises(ValueError, match="absolute"):
        make_settings(**{field: Path("relative/path")})


@pytest.mark.parametrize(
    "listen",
    ["", "tcp:127.0.0.1:8787", "127.0.0.1:8787", "unix:", "unix:worker.sock"],
)
def test_production_rejects_non_absolute_unix_listeners(listen: str) -> None:
    with pytest.raises(ValueError, match="Unix socket"):
        make_settings(environment="production", listen=listen)


@pytest.mark.parametrize(
    "listen",
    [
        "unix:/",
        "unix:/run/seo/",
        "unix:/run/seo/worker\0.sock",
        "unix:run/seo/worker.sock",
        "unix:/run/./seo/worker.sock",
        "unix:/run/seo/../worker.sock",
    ],
)
def test_direct_constructor_rejects_invalid_production_socket_targets(listen: str) -> None:
    with pytest.raises(ValueError, match="Unix socket"):
        make_settings(environment="production", listen=listen)


@pytest.mark.parametrize(
    "listen",
    [
        "unix:/",
        "unix:/run/seo/",
        "unix:/run/seo/worker\0.sock",
        "unix:run/seo/worker.sock",
        "unix:/run/./seo/worker.sock",
        "unix:/run/seo/../worker.sock",
    ],
)
def test_from_env_rejects_invalid_production_socket_targets(listen: str) -> None:
    with pytest.raises(ValueError, match="Unix socket"):
        Settings.from_env({"SEO_ENV": "production", "SEO_LISTEN": listen})


def test_development_accepts_tcp_listener() -> None:
    settings = Settings.from_env({"SEO_LISTEN": "127.0.0.1:8787"})

    assert settings.listen == "127.0.0.1:8787"


@pytest.mark.parametrize("value", [0, -1, True, False])
def test_direct_constructor_rejects_invalid_job_limits(value: object) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        make_settings(max_active_jobs_per_user=value)


@pytest.mark.parametrize("value", ["0", "-1"])
def test_environment_rejects_non_positive_job_limits(value: str) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        Settings.from_env({"SEO_MAX_ACTIVE_JOBS_PER_USER": value})


@pytest.mark.parametrize("mode", [0o600, 0o640, 0o660])
def test_direct_constructor_accepts_safe_socket_modes(mode: int) -> None:
    assert make_settings(worker_socket_mode=mode).worker_socket_mode == mode


@pytest.mark.parametrize("mode", [-1, 0o1660, 0o666, 0o770, True, False])
def test_direct_constructor_rejects_unsafe_socket_modes(mode: object) -> None:
    with pytest.raises(ValueError, match="socket mode"):
        make_settings(worker_socket_mode=mode)


@pytest.mark.parametrize("mode", ["-1", "1660", "0666", "0770", "not-octal", "0890"])
def test_environment_rejects_unsafe_or_malformed_socket_modes(mode: str) -> None:
    with pytest.raises(ValueError, match="SOCKET_MODE"):
        Settings.from_env({"SEO_WORKER_SOCKET_MODE": mode})


def test_callback_hmac_key_path_is_absolute_and_independently_configurable() -> None:
    settings = Settings.from_env(
        {"SEO_CALLBACK_HMAC_KEY_PATH": "/run/seo-orchestrator/n8n-callback.key"}
    )

    assert settings.callback_hmac_key_path == Path("/run/seo-orchestrator/n8n-callback.key")
    with pytest.raises(ValueError, match="CALLBACK_HMAC_KEY_PATH"):
        Settings.from_env({"SEO_CALLBACK_HMAC_KEY_PATH": "relative/callback.key"})
