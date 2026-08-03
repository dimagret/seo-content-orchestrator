from pathlib import Path

import pytest

from seo_orchestrator.settings import Settings


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
