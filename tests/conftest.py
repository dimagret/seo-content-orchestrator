"""Shared test hygiene for filesystem-backed artifact fixtures."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


def _restore_artifact_directory_modes(root: Path) -> None:
    """Make only test-owned directories removable after immutable-bundle tests."""
    if root.is_symlink() or not root.exists():
        return

    try:
        os.chmod(root, 0o700, follow_symlinks=False)
    except (FileNotFoundError, PermissionError):
        return

    for directory, child_directories, _ in os.walk(root, topdown=True, followlinks=False):
        for child_name in child_directories:
            child = Path(directory, child_name)
            if child.is_symlink():
                continue
            try:
                os.chmod(child, 0o700, follow_symlinks=False)
            except (FileNotFoundError, PermissionError):
                continue


def pytest_runtest_teardown(item: pytest.Item, nextitem: pytest.Item | None) -> None:
    """Restore removable modes before pytest removes a test's tmp_path."""
    if not isinstance(item, pytest.Function):
        return
    tmp_path = item.funcargs.get("tmp_path")
    if isinstance(tmp_path, Path):
        _restore_artifact_directory_modes(tmp_path / "artifacts")
