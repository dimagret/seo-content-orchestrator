"""SQLite connection and transaction helpers."""

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path


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
    conn = sqlite3.connect(path)
    conn.create_function("is_utc_timestamp", 1, _is_utc_timestamp, deterministic=True)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


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
