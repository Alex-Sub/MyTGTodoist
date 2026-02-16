from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator


DB_PATH = os.getenv("DB_PATH", "/data/organizer.db")


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_utc(value: str) -> datetime:
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _add_days_iso_utc(value: str, days: int) -> str:
    dt = _parse_iso_utc(value) + timedelta(days=days)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


@contextmanager
def connect(db_path: str | None = None) -> Iterator[sqlite3.Connection]:
    path = db_path or DB_PATH
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
    finally:
        conn.close()


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _safe_count(conn: sqlite3.Connection, table: str) -> int:
    if not _table_exists(conn, table):
        return 0
    row = conn.execute(f"SELECT COUNT(1) AS c FROM {table}").fetchone()
    return int(row["c"] if row is not None else 0)


@dataclass(frozen=True, slots=True)
class StateSnapshot:
    tasks_total: int
    subtasks_total: int
    time_blocks_total: int
    regulations_total: int
    regulation_runs_total: int
    queue_total: int
    cycles_total: int
    goals_total: int
    nudges_total: int


def get_state(conn: sqlite3.Connection) -> StateSnapshot:
    return StateSnapshot(
        tasks_total=_safe_count(conn, "tasks"),
        subtasks_total=_safe_count(conn, "subtasks"),
        time_blocks_total=_safe_count(conn, "time_blocks"),
        regulations_total=_safe_count(conn, "regulations"),
        regulation_runs_total=_safe_count(conn, "regulation_runs"),
        queue_total=_safe_count(conn, "inbox_queue"),
        cycles_total=_safe_count(conn, "cycles"),
        goals_total=_safe_count(conn, "cycle_goals"),
        nudges_total=_safe_count(conn, "user_nudges"),
    )


def create_task(
    conn: sqlite3.Connection,
    *,
    title: str,
    status: str = "NEW",
    state: str = "NEW",
    planned_at: str | None = None,
    source_msg_id: str | None = None,
    parent_type: str | None = None,
    parent_id: int | None = None,
) -> int:
    now = _now_iso_utc()
    cur = conn.execute(
        """
        INSERT INTO tasks (title, status, state, planned_at, source_msg_id, parent_type, parent_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (title, status, state, planned_at, source_msg_id, parent_type, parent_id, now, now),
    )
    conn.commit()
    return int(cur.lastrowid)


def update_task(
    conn: sqlite3.Connection,
    *,
    task_id: int,
    title: str | None = None,
    status: str | None = None,
    state: str | None = None,
    planned_at: str | None = None,
    parent_type: str | None = None,
    parent_id: int | None = None,
) -> None:
    now = _now_iso_utc()
    existing = conn.execute("SELECT id FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if existing is None:
        raise ValueError("task not found")

    fields: list[str] = ["updated_at = ?"]
    params: list[Any] = [now]

    if title is not None:
        fields.append("title = ?")
        params.append(title)
    if status is not None:
        fields.append("status = ?")
        params.append(status)
    if state is not None:
        fields.append("state = ?")
        params.append(state)
    if planned_at is not None:
        fields.append("planned_at = ?")
        params.append(planned_at)
    if parent_type is not None:
        fields.append("parent_type = ?")
        params.append(parent_type)
    if parent_id is not None:
        fields.append("parent_id = ?")
        params.append(parent_id)

    params.append(task_id)
    conn.execute(f"UPDATE tasks SET {', '.join(fields)} WHERE id = ?", params)
    conn.commit()


def find_task_candidates(conn: sqlite3.Connection, *, task_ref: str, limit: int = 5) -> list[dict[str, Any]]:
    ref = (task_ref or "").strip()
    if not ref:
        return []
    rows = conn.execute(
        """
        SELECT id, title, status, state, planned_at
        FROM tasks
        WHERE CAST(id AS TEXT) = ?
           OR title LIKE ?
        ORDER BY
            CASE WHEN CAST(id AS TEXT) = ? THEN 0 ELSE 1 END,
            id DESC
        LIMIT ?
        """,
        (ref, f"%{ref}%", ref, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def complete_task(conn: sqlite3.Connection, *, task_id: int) -> None:
    now = _now_iso_utc()
    row = conn.execute("SELECT id FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if row is None:
        raise ValueError("task not found")
    conn.execute(
        "UPDATE tasks SET status = ?, state = ?, completed_at = ?, updated_at = ? WHERE id = ?",
        ("DONE", "DONE", now, now, task_id),
    )
    conn.commit()


def create_subtask(
    conn: sqlite3.Connection,
    *,
    task_id: int,
    title: str,
    status: str = "NEW",
    source_msg_id: str | None = None,
) -> int:
    now = _now_iso_utc()
    cur = conn.execute(
        """
        INSERT INTO subtasks (task_id, title, status, source_msg_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (task_id, title, status, source_msg_id, now, now),
    )
    conn.commit()
    return int(cur.lastrowid)


def complete_subtask(conn: sqlite3.Connection, *, subtask_id: int) -> None:
    now = _now_iso_utc()
    row = conn.execute("SELECT id FROM subtasks WHERE id = ?", (subtask_id,)).fetchone()
    if row is None:
        raise ValueError("subtask not found")
    conn.execute(
        "UPDATE subtasks SET status = ?, completed_at = ?, updated_at = ? WHERE id = ?",
        ("DONE", now, now, subtask_id),
    )
    conn.commit()


def create_time_block(
    conn: sqlite3.Connection,
    *,
    task_id: int,
    start_at: str,
    end_at: str,
) -> int:
    now = _now_iso_utc()
    cur = conn.execute(
        """
        INSERT INTO time_blocks (task_id, start_at, end_at, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (task_id, start_at, end_at, now),
    )
    conn.commit()
    return int(cur.lastrowid)


def move_time_block(
    conn: sqlite3.Connection,
    *,
    time_block_id: int,
    start_at: str | None = None,
    end_at: str | None = None,
    task_id: int | None = None,
) -> None:
    row = conn.execute("SELECT id FROM time_blocks WHERE id = ?", (time_block_id,)).fetchone()
    if row is None:
        raise ValueError("time block not found")

    fields: list[str] = []
    params: list[Any] = []
    if task_id is not None:
        fields.append("task_id = ?")
        params.append(task_id)
    if start_at is not None:
        fields.append("start_at = ?")
        params.append(start_at)
    if end_at is not None:
        fields.append("end_at = ?")
        params.append(end_at)

    if not fields:
        return

    params.append(time_block_id)
    conn.execute(f"UPDATE time_blocks SET {', '.join(fields)} WHERE id = ?", params)
    conn.commit()


def delete_time_block(conn: sqlite3.Connection, *, time_block_id: int) -> None:
    conn.execute("DELETE FROM time_blocks WHERE id = ?", (time_block_id,))
    conn.commit()


def upsert_regulation_run(
    conn: sqlite3.Connection,
    *,
    regulation_id: int,
    period_key: str,
    status: str,
    due_date: str,
    due_time_local: str | None = None,
    done_at: str | None = None,
) -> int:
    now = _now_iso_utc()
    existing = conn.execute(
        "SELECT id FROM regulation_runs WHERE regulation_id = ? AND period_key = ?",
        (regulation_id, period_key),
    ).fetchone()

    if existing is None:
        cur = conn.execute(
            """
            INSERT INTO regulation_runs (
              regulation_id, period_key, status, due_date, due_time_local, done_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (regulation_id, period_key, status, due_date, due_time_local, done_at, now, now),
        )
        conn.commit()
        return int(cur.lastrowid)

    run_id = int(existing["id"])
    conn.execute(
        """
        UPDATE regulation_runs
        SET status = ?, due_date = ?, due_time_local = ?, done_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (status, due_date, due_time_local, done_at, now, run_id),
    )
    conn.commit()
    return run_id


def list_regulation_runs(
    conn: sqlite3.Connection,
    *,
    regulation_id: int | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    if not _table_exists(conn, "regulation_runs"):
        return []

    if regulation_id is None:
        rows = conn.execute(
            """
            SELECT id, regulation_id, period_key, status, due_date, due_time_local, done_at, created_at, updated_at
            FROM regulation_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, regulation_id, period_key, status, due_date, due_time_local, done_at, created_at, updated_at
            FROM regulation_runs
            WHERE regulation_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (regulation_id, limit),
        ).fetchall()

    return [dict(r) for r in rows]


def cycles_create(
    conn: sqlite3.Connection,
    *,
    title: str,
    date_from: str | None,
    date_to: str | None,
) -> int:
    now = _now_iso_utc()
    cur = conn.execute(
        """
        INSERT INTO cycles (type, period_key, period_start, period_end, status, summary, source_msg_id, created_at, updated_at, closed_at)
        VALUES (?, NULL, ?, ?, ?, ?, NULL, ?, ?, NULL)
        """,
        ("CUSTOM", date_from, date_to, "OPEN", title, now, now),
    )
    conn.commit()
    return int(cur.lastrowid)


def cycles_set_active(conn: sqlite3.Connection, *, cycle_id: int) -> None:
    now = _now_iso_utc()
    row = conn.execute("SELECT id FROM cycles WHERE id = ?", (cycle_id,)).fetchone()
    if row is None:
        raise ValueError("cycle not found")
    # Default: enforce a single OPEN cycle.
    conn.execute("UPDATE cycles SET status = ?, updated_at = ? WHERE status = ? AND id != ?", ("DONE", now, "OPEN", cycle_id))
    conn.execute("UPDATE cycles SET status = ?, updated_at = ? WHERE id = ?", ("OPEN", now, cycle_id))
    conn.commit()


def cycles_close(conn: sqlite3.Connection, *, cycle_id: int) -> None:
    now = _now_iso_utc()
    row = conn.execute("SELECT id FROM cycles WHERE id = ?", (cycle_id,)).fetchone()
    if row is None:
        raise ValueError("cycle not found")
    conn.execute("UPDATE cycles SET status = ?, closed_at = ?, updated_at = ? WHERE id = ?", ("DONE", now, now, cycle_id))
    conn.commit()


def goals_create(
    conn: sqlite3.Connection,
    *,
    cycle_id: int,
    title: str,
    target: str | None = None,
) -> int:
    _ = target  # Reserved for future schema.
    now = _now_iso_utc()
    row = conn.execute("SELECT id FROM cycles WHERE id = ?", (cycle_id,)).fetchone()
    if row is None:
        raise ValueError("cycle not found")
    cur = conn.execute(
        """
        INSERT INTO cycle_goals (cycle_id, text, status, continued_from_goal_id, created_at, updated_at)
        VALUES (?, ?, ?, NULL, ?, ?)
        """,
        (cycle_id, title, "ACTIVE", now, now),
    )
    conn.commit()
    return int(cur.lastrowid)


def goals_update(conn: sqlite3.Connection, *, goal_id: int, fields: dict[str, Any]) -> None:
    now = _now_iso_utc()
    existing = conn.execute("SELECT id FROM cycle_goals WHERE id = ?", (goal_id,)).fetchone()
    if existing is None:
        raise ValueError("goal not found")

    sql_fields: list[str] = ["updated_at = ?"]
    params: list[Any] = [now]

    if "title" in fields and fields["title"] is not None:
        sql_fields.append("text = ?")
        params.append(str(fields["title"]))
    if "text" in fields and fields["text"] is not None:
        sql_fields.append("text = ?")
        params.append(str(fields["text"]))
    if "status" in fields and fields["status"] is not None:
        sql_fields.append("status = ?")
        params.append(str(fields["status"]).upper())

    if len(sql_fields) == 1:
        return

    params.append(goal_id)
    conn.execute(f"UPDATE cycle_goals SET {', '.join(sql_fields)} WHERE id = ?", params)
    conn.commit()


def goals_close(conn: sqlite3.Connection, *, goal_id: int) -> None:
    now = _now_iso_utc()
    existing = conn.execute("SELECT id FROM cycle_goals WHERE id = ?", (goal_id,)).fetchone()
    if existing is None:
        raise ValueError("goal not found")
    conn.execute("UPDATE cycle_goals SET status = ?, updated_at = ? WHERE id = ?", ("ACHIEVED", now, goal_id))
    conn.commit()


def nudges_list(conn: sqlite3.Connection, *, user_id: str) -> list[dict[str, Any]]:
    if not _table_exists(conn, "user_nudges"):
        return []
    now = _now_iso_utc()
    rows = conn.execute(
        """
        SELECT user_id, nudge_key, next_at, last_shown_at, created_at, updated_at
        FROM user_nudges
        WHERE user_id = ? AND next_at <= ?
        ORDER BY next_at ASC, nudge_key ASC
        """,
        (user_id, now),
    ).fetchall()
    return [dict(r) for r in rows]


def nudges_ack(conn: sqlite3.Connection, *, user_id: str, nudge_id: str) -> None:
    # nudge_id is a public alias for nudge_key.
    now = _now_iso_utc()
    next_at = _add_days_iso_utc(now, 1)
    row = conn.execute(
        "SELECT user_id FROM user_nudges WHERE user_id = ? AND nudge_key = ?",
        (user_id, nudge_id),
    ).fetchone()
    if row is None:
        raise ValueError("nudge not found")
    conn.execute(
        """
        UPDATE user_nudges
        SET last_shown_at = ?, next_at = ?, updated_at = ?
        WHERE user_id = ? AND nudge_key = ?
        """,
        (now, next_at, now, user_id, nudge_id),
    )
    conn.commit()
