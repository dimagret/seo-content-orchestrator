"""Execution adapter contracts."""

from seo_orchestrator.executors.base import (
    ExecutionStatus,
    Executor,
    ExternalRun,
    ExternalStatus,
)
from seo_orchestrator.executors.mock import MockExecutor

__all__ = [
    "ExecutionStatus",
    "Executor",
    "ExternalRun",
    "ExternalStatus",
    "MockExecutor",
]
