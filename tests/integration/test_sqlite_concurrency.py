from __future__ import annotations

import sqlite3
from multiprocessing import get_context
from pathlib import Path
from time import monotonic
from typing import Any, cast

from seo_orchestrator.db.connection import connect, transaction


def _hold_uncommitted_write(
    database_path: str,
    write_started: Any,
    release_write: Any,
    result_queue: Any,
) -> None:
    conn = connect(Path(database_path))
    try:
        with transaction(conn):
            conn.execute("UPDATE concurrency_probe SET value = ?", ("after",))
            write_started.set()
            if not release_write.wait(timeout=10):
                raise TimeoutError("writer release event was not set")
        result_queue.put(("writer", "committed"))
    except (sqlite3.Error, TimeoutError) as exc:
        result_queue.put(("writer-error", repr(exc)))
    finally:
        conn.close()


def _read_during_write(database_path: str, write_started: Any, result_queue: Any) -> None:
    try:
        if not write_started.wait(timeout=10):
            raise TimeoutError("writer did not start")
        conn = connect(Path(database_path))
        try:
            value = conn.execute("SELECT value FROM concurrency_probe").fetchone()[0]
            result_queue.put(("reader", value))
        finally:
            conn.close()
    except (sqlite3.Error, TimeoutError) as exc:
        result_queue.put(("reader-error", repr(exc)))


def _hold_write_lock(database_path: str, lock_acquired: Any, release_lock: Any) -> None:
    conn = connect(Path(database_path))
    try:
        with transaction(conn):
            conn.execute("UPDATE concurrency_probe SET value = ?", ("locked",))
            lock_acquired.set()
            if not release_lock.wait(timeout=10):
                raise TimeoutError("lock release event was not set")
    finally:
        conn.close()


def _attempt_busy_write(database_path: str, lock_acquired: Any, result_queue: Any) -> None:
    if not lock_acquired.wait(timeout=10):
        result_queue.put(("contender-error", "holder did not acquire lock", 0.0))
        return
    conn = connect(Path(database_path))
    started = monotonic()
    try:
        with transaction(conn):
            conn.execute("UPDATE concurrency_probe SET value = ?", ("contender",))
    except sqlite3.OperationalError as exc:
        result_queue.put(("busy", str(exc), monotonic() - started))
    except sqlite3.Error as exc:
        result_queue.put(("contender-error", repr(exc), monotonic() - started))
    else:
        result_queue.put(("unexpected-success", "", monotonic() - started))
    finally:
        conn.close()


def _create_probe(database_path: Path) -> None:
    conn = connect(database_path)
    try:
        conn.execute("CREATE TABLE concurrency_probe(value TEXT NOT NULL)")
        conn.execute("INSERT INTO concurrency_probe(value) VALUES (?)", ("before",))
        conn.commit()
    finally:
        conn.close()


def test_wal_allows_process_reader_while_process_writer_has_uncommitted_change(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "orchestrator.db"
    _create_probe(database_path)
    context = get_context("spawn")
    write_started = context.Event()
    release_write = context.Event()
    result_queue = context.Queue()
    writer = context.Process(
        target=_hold_uncommitted_write,
        args=(str(database_path), write_started, release_write, result_queue),
    )
    reader = context.Process(
        target=_read_during_write,
        args=(str(database_path), write_started, result_queue),
    )

    writer.start()
    reader.start()
    try:
        reader_result = cast(tuple[str, str], result_queue.get(timeout=10))
        assert reader_result == ("reader", "before")
        assert writer.is_alive()
        release_write.set()
        writer_result = cast(tuple[str, str], result_queue.get(timeout=10))
        assert writer_result == ("writer", "committed")
    finally:
        release_write.set()
        reader.join(timeout=10)
        writer.join(timeout=10)
        if reader.is_alive():
            reader.terminate()
        if writer.is_alive():
            writer.terminate()
    assert reader.exitcode == 0
    assert writer.exitcode == 0


def test_busy_timeout_waits_before_process_writer_fails(tmp_path: Path) -> None:
    database_path = tmp_path / "orchestrator.db"
    _create_probe(database_path)
    context = get_context("spawn")
    lock_acquired = context.Event()
    release_lock = context.Event()
    result_queue = context.Queue()
    holder = context.Process(
        target=_hold_write_lock,
        args=(str(database_path), lock_acquired, release_lock),
    )
    contender = context.Process(
        target=_attempt_busy_write,
        args=(str(database_path), lock_acquired, result_queue),
    )

    holder.start()
    contender.start()
    try:
        result = cast(tuple[str, str, float], result_queue.get(timeout=10))
        assert result[0] == "busy"
        assert "locked" in result[1]
        assert 4.5 <= result[2] < 8.0
    finally:
        release_lock.set()
        contender.join(timeout=10)
        holder.join(timeout=10)
        if contender.is_alive():
            contender.terminate()
        if holder.is_alive():
            holder.terminate()
    assert contender.exitcode == 0
    assert holder.exitcode == 0
