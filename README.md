# SEO Content Orchestrator

An isolated Python worker for the Telegram SEO content orchestration system.

## Architecture boundary

This repository owns the local SEO orchestration worker, its domain logic, and its tests. It
keeps runtime data under the configured database and artifact paths and communicates through a
Unix socket in production. The worker does not own or modify n8n, Hermes Agent, deployment
services, or third-party systems.

No external integrations are implemented yet.

## Local setup

Python 3.13 or newer and [uv](https://docs.astral.sh/uv/) are required.

```bash
uv sync --frozen
uv run pytest
uv run ruff check .
uv run mypy src
```

Default development settings are defined in `seo_orchestrator.settings.Settings` and can be
overridden by passing an explicit environment mapping to `Settings.from_env`.

## Approval-gated work

Any n8n or Hermes configuration, plugin installation, external API access, Google Sheets write,
or deployment/systemd change is outside this scaffold. Such work must be separately approved
before it is performed.
