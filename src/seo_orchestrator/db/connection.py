"""SQLite connection and transaction helpers."""

import os
import sqlite3
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path


@dataclass(frozen=True, slots=True)
class OpenedDatabaseIdentity:
    """Filesystem identity captured while SQLite opens its main database."""

    path: Path
    device: int
    inode: int


class TrackedSQLiteConnection(sqlite3.Connection):
    """SQLite connection carrying the immutable main-file identity it opened."""

    opened_database_identity: OpenedDatabaseIdentity


def require_unaliased_absolute_path(path: Path) -> Path:
    """Reject lexical parent traversal and symlinks in every existing component."""
    if (
        not isinstance(path, Path)
        or not path.is_absolute()
        or "\x00" in str(path)
        or any(part == ".." for part in path.parts)
    ):
        raise ValueError("database path must be an absolute normalized path")
    current = Path(path.anchor)
    components = path.parts[1:]
    for index, component in enumerate(components):
        current /= component
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            return path
        except OSError as exc:
            raise ValueError("database path component is unavailable") from exc
        if stat.S_ISLNK(metadata.st_mode):
            raise ValueError("database path aliases are not allowed")
        if index < len(components) - 1 and not stat.S_ISDIR(metadata.st_mode):
            raise ValueError("database parent component must be a directory")
    return path


def _identity_from_metadata(
    path: Path,
    metadata: os.stat_result,
) -> OpenedDatabaseIdentity:
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError("database path must be a regular file")
    return OpenedDatabaseIdentity(
        path=path,
        device=metadata.st_dev,
        inode=metadata.st_ino,
    )


def _database_identity(path: Path) -> OpenedDatabaseIdentity:
    require_unaliased_absolute_path(path)
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise ValueError("database path identity is unavailable") from exc
    return _identity_from_metadata(path, metadata)


def _pin_database_file(path: Path) -> tuple[int, OpenedDatabaseIdentity]:
    flags = os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        try:
            descriptor = os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise ValueError("database file cannot be created safely") from exc
    except OSError as exc:
        raise ValueError("database file cannot be opened safely") from exc
    try:
        identity = _identity_from_metadata(path, os.fstat(descriptor))
        if _database_identity(path) != identity:
            raise ValueError("database path identity changed before SQLite open")
        return descriptor, identity
    except BaseException:
        os.close(descriptor)
        raise


def opened_database_identity(connection: sqlite3.Connection) -> OpenedDatabaseIdentity:
    """Return the open-time identity or fail closed for an untracked connection."""
    if not isinstance(connection, TrackedSQLiteConnection):
        raise TypeError("authoritative database connection has no opened database identity")
    return connection.opened_database_identity


def _is_utc_timestamp(value: object) -> int:
    """Return one only for canonical aware UTC ``datetime.isoformat`` text."""
    if type(value) is not str:
        return 0
    try:
        from datetime import datetime

        parsed = datetime.fromisoformat(value)
    except ValueError:
        return 0
    return int(
        parsed.tzinfo is not None
        and parsed.utcoffset() == timedelta(0)
        and parsed.isoformat() == value
    )


def connect(path: Path) -> sqlite3.Connection:
    """Open a SQLite connection with required durability settings."""
    if not isinstance(path, Path):
        raise TypeError("path must be a pathlib.Path")
    requested_path = require_unaliased_absolute_path(path)
    descriptor, pinned_identity = _pin_database_file(requested_path)
    try:
        conn = sqlite3.connect(requested_path, factory=TrackedSQLiteConnection)
        try:
            if _database_identity(requested_path) != pinned_identity:
                raise ValueError("database path identity changed while opening connection")
            conn.opened_database_identity = pinned_identity
            conn.create_function("is_utc_timestamp", 1, _is_utc_timestamp, deterministic=True)
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA busy_timeout=5000")
            if _database_identity(requested_path) != pinned_identity:
                raise ValueError("database path identity changed while configuring connection")
            return conn
        except BaseException:
            conn.close()
            raise
    finally:
        os.close(descriptor)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run one non-nested transaction with an eagerly acquired write lock."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
