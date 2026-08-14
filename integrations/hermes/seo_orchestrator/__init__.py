"""Profile-local Hermes plugin exposing narrow SEO orchestrator controls."""

from __future__ import annotations

from typing import Any, Protocol

from .schemas import TOOL_SCHEMAS
from .tools import build_handler


class PluginContext(Protocol):
    """Minimal current Hermes registration surface used by this plugin."""

    def register_tool(self, **kwargs: Any) -> None: ...


def register(ctx: PluginContext) -> None:
    """Register only the reviewed SEO toolset without hooks or overrides."""
    for name, schema in TOOL_SCHEMAS.items():
        ctx.register_tool(
            name=name,
            toolset="seo_orchestrator",
            schema=schema,
            handler=build_handler(name),
            override=False,
        )
