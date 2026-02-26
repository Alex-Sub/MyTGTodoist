from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
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


def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if not _table_exists(conn, table):
        return False
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    for r in rows:
        name = r["name"] if isinstance(r, sqlite3.Row) else r[1]
        if str(name) == column:
            return True
    return False


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
    goals_table = "goals" if _table_exists(conn, "goals") else "cycle_goals"
    return StateSnapshot(
        tasks_total=_safe_count(conn, "tasks"),
        subtasks_total=_safe_count(conn, "subtasks"),
        time_blocks_total=_safe_count(conn, "time_blocks"),
        regulations_total=_safe_count(conn, "regulations"),
        regulation_runs_total=_safe_count(conn, "regulation_runs"),
        queue_total=_safe_count(conn, "inbox_queue"),
        cycles_total=_safe_count(conn, "cycles"),
        goals_total=_safe_count(conn, goals_table),
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


def find_goal_candidates(conn: sqlite3.Connection, *, goal_ref: str, limit: int = 5) -> list[dict[str, Any]]:
    ref = (goal_ref or "").strip()
    if not ref or not _table_exists(conn, "goals"):
        return []
    rows = conn.execute(
        """
        SELECT id, title, planned_end_date, status
        FROM goals
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


# Strategic Layer v1
def create_cycle(conn: sqlite3.Connection, *, name: str, start_date: str, end_date: str) -> int:
    now = _now_iso_utc()
    has_name = _table_has_column(conn, "cycles", "name")
    has_start = _table_has_column(conn, "cycles", "start_date")
    has_end = _table_has_column(conn, "cycles", "end_date")

    if has_name and has_start and has_end:
        cur = conn.execute(
            """
            INSERT INTO cycles (name, start_date, end_date, type, period_key, period_start, period_end, status, summary, source_msg_id, created_at, updated_at, closed_at)
            VALUES (?, ?, ?, ?, NULL, ?, ?, ?, NULL, NULL, ?, ?, NULL)
            """,
            (name, start_date, end_date, "CUSTOM", start_date, end_date, "OPEN", now, now),
        )
    else:
        cur = conn.execute(
            """
            INSERT INTO cycles (type, period_key, period_start, period_end, status, summary, source_msg_id, created_at, updated_at, closed_at)
            VALUES (?, NULL, ?, ?, ?, ?, NULL, ?, ?, NULL)
            """,
            ("CUSTOM", start_date, end_date, "OPEN", name, now, now),
        )
    conn.commit()
    return int(cur.lastrowid)


def get_active_cycle(conn: sqlite3.Connection) -> dict[str, Any] | None:
    if not _table_exists(conn, "cycles"):
        return None
    row = conn.execute(
        """
        SELECT *
        FROM cycles
        WHERE closed_at IS NULL
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    return dict(row) if row is not None else None


def close_cycle(conn: sqlite3.Connection, *, cycle_id: int) -> dict[str, Any]:
    now = _now_iso_utc()
    row = conn.execute("SELECT id FROM cycles WHERE id = ?", (cycle_id,)).fetchone()
    if row is None:
        raise ValueError("cycle not found")
    conn.execute("UPDATE cycles SET status = ?, closed_at = ?, updated_at = ? WHERE id = ?", ("DONE", now, now, cycle_id))

    today = datetime.now(timezone.utc).date().isoformat()
    summary = {"goals_total": 0, "goals_done": 0, "goals_overdue": 0}
    if _table_exists(conn, "goals"):
        g_total = conn.execute("SELECT COUNT(1) AS c FROM goals WHERE cycle_id = ?", (cycle_id,)).fetchone()
        g_done = conn.execute(
            "SELECT COUNT(1) AS c FROM goals WHERE cycle_id = ? AND status = 'DONE'",
            (cycle_id,),
        ).fetchone()
        g_overdue = conn.execute(
            """
            SELECT COUNT(1) AS c
            FROM goals
            WHERE cycle_id = ?
              AND status = 'ACTIVE'
              AND planned_end_date < ?
            """,
            (cycle_id, today),
        ).fetchone()
        summary = {
            "goals_total": int(g_total["c"] if g_total else 0),
            "goals_done": int(g_done["c"] if g_done else 0),
            "goals_overdue": int(g_overdue["c"] if g_overdue else 0),
        }
    conn.commit()
    return summary


def create_goal(
    conn: sqlite3.Connection,
    *,
    cycle_id: int,
    title: str,
    success_criteria: str,
    planned_end_date: str,
) -> int:
    row = conn.execute("SELECT id FROM cycles WHERE id = ?", (cycle_id,)).fetchone()
    if row is None:
        raise ValueError("cycle not found")
    cur = conn.execute(
        """
        INSERT INTO goals (cycle_id, title, success_criteria, planned_end_date, status, created_at, completed_at)
        VALUES (?, ?, ?, ?, 'ACTIVE', datetime('now'), NULL)
        """,
        (cycle_id, title, success_criteria, planned_end_date),
    )
    conn.commit()
    return int(cur.lastrowid)


def update_goal(conn: sqlite3.Connection, *, goal_id: int, fields: dict[str, Any]) -> None:
    row = conn.execute("SELECT id FROM goals WHERE id = ?", (goal_id,)).fetchone()
    if row is None:
        raise ValueError("goal not found")
    sql_fields: list[str] = []
    params: list[Any] = []
    for key in ("title", "success_criteria", "planned_end_date", "status", "completed_at"):
        if key in fields and fields[key] is not None:
            sql_fields.append(f"{key} = ?")
            params.append(fields[key])
    if not sql_fields:
        return
    params.append(goal_id)
    conn.execute(f"UPDATE goals SET {', '.join(sql_fields)} WHERE id = ?", params)
    conn.commit()


def close_goal(conn: sqlite3.Connection, *, goal_id: int, close_as: str = "DONE") -> None:
    status = str(close_as or "DONE").upper()
    if status not in {"DONE", "DROPPED"}:
        raise ValueError("close_as must be DONE or DROPPED")
    row = conn.execute("SELECT id FROM goals WHERE id = ?", (goal_id,)).fetchone()
    if row is None:
        raise ValueError("goal not found")
    completed_at = _now_iso_utc() if status == "DONE" else None
    conn.execute("UPDATE goals SET status = ?, completed_at = ? WHERE id = ?", (status, completed_at, goal_id))
    conn.commit()


def reschedule_goal(conn: sqlite3.Connection, *, goal_id: int, new_end_date: str) -> int:
    row = conn.execute("SELECT planned_end_date FROM goals WHERE id = ?", (goal_id,)).fetchone()
    if row is None:
        raise ValueError("goal not found")
    old_end_date = str(row["planned_end_date"])
    cur = conn.execute(
        """
        INSERT INTO goal_reschedule_events (goal_id, old_end_date, new_end_date, changed_at)
        VALUES (?, ?, ?, datetime('now'))
        """,
        (goal_id, old_end_date, new_end_date),
    )
    conn.execute("UPDATE goals SET planned_end_date = ? WHERE id = ?", (new_end_date, goal_id))
    conn.commit()
    return int(cur.lastrowid)


def link_task_to_goal(conn: sqlite3.Connection, *, task_id: int, goal_id: int) -> None:
    t_row = conn.execute("SELECT id FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if t_row is None:
        raise ValueError("task not found")
    g_row = conn.execute("SELECT id FROM goals WHERE id = ?", (goal_id,)).fetchone()
    if g_row is None:
        raise ValueError("goal not found")
    now = _now_iso_utc()
    conn.execute("UPDATE tasks SET goal_id = ?, updated_at = ? WHERE id = ?", (goal_id, now, task_id))
    conn.commit()


def list_nudges(conn: sqlite3.Connection, *, user_id: str, today: str) -> list[dict[str, Any]]:
    digest = compute_daily_digest(conn, today=today, tomorrow=(date.fromisoformat(today) + timedelta(days=1)).isoformat(), user_id=user_id)
    out: list[dict[str, Any]] = []
    if int(digest["goals_overdue"]) > 0:
        out.append({"nudge_type": "goals.overdue", "entity_type": "goal", "entity_id": 0, "payload": {"count": digest["goals_overdue"]}})
    if int(digest["goals_due_soon"]) > 0:
        out.append({"nudge_type": "goals.due_soon", "entity_type": "goal", "entity_id": 0, "payload": {"count": digest["goals_due_soon"]}})
    if int(digest["goals_at_risk"]) > 0:
        out.append({"nudge_type": "goals.at_risk", "entity_type": "goal", "entity_id": 0, "payload": {"count": digest["goals_at_risk"]}})
    return out


def ack_nudge(conn: sqlite3.Connection, *, user_id: str, nudge_type: str, entity_type: str, entity_id: int) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO nudge_ack (user_id, nudge_type, entity_type, entity_id, acked_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        """,
        (user_id, nudge_type, entity_type, entity_id),
    )
    conn.commit()


def _goal_has_movement_last3d(conn: sqlite3.Connection, goal_id: int, today: date) -> bool:
    from_day = (today - timedelta(days=3)).isoformat()
    to_day = today.isoformat()
    task_rows = conn.execute(
        """
        SELECT id, updated_at, completed_at
        FROM tasks
        WHERE goal_id = ?
        """,
        (goal_id,),
    ).fetchall()
    if not task_rows:
        return False
    task_ids = [int(r["id"]) for r in task_rows]
    for r in task_rows:
        upd = str(r["updated_at"] or "")[:10]
        comp = str(r["completed_at"] or "")[:10]
        if (upd and from_day <= upd <= to_day) or (comp and from_day <= comp <= to_day):
            return True
    placeholders = ",".join("?" for _ in task_ids)
    tb_row = conn.execute(
        f"""
        SELECT COUNT(1) AS c
        FROM time_blocks
        WHERE task_id IN ({placeholders})
          AND substr(created_at, 1, 10) >= ?
          AND substr(created_at, 1, 10) <= ?
        """,
        (*task_ids, from_day, to_day),
    ).fetchone()
    return int(tb_row["c"] if tb_row else 0) > 0


def _goal_has_time_blocks_next2d(conn: sqlite3.Connection, goal_id: int, today: date) -> bool:
    task_rows = conn.execute("SELECT id FROM tasks WHERE goal_id = ?", (goal_id,)).fetchall()
    if not task_rows:
        return False
    task_ids = [int(r["id"]) for r in task_rows]
    from_day = today.isoformat()
    to_day = (today + timedelta(days=2)).isoformat()
    placeholders = ",".join("?" for _ in task_ids)
    row = conn.execute(
        f"""
        SELECT COUNT(1) AS c
        FROM time_blocks
        WHERE task_id IN ({placeholders})
          AND substr(start_at, 1, 10) >= ?
          AND substr(start_at, 1, 10) <= ?
        """,
        (*task_ids, from_day, to_day),
    ).fetchone()
    return int(row["c"] if row else 0) > 0


def list_goals_overdue(conn: sqlite3.Connection, *, today: str, limit: int = 20) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            g.id,
            g.cycle_id,
            g.title,
            g.success_criteria,
            g.planned_end_date,
            g.status,
            COALESCE((SELECT COUNT(1) FROM goal_reschedule_events e WHERE e.goal_id = g.id), 0) AS reschedule_count
        FROM goals g
        WHERE g.status = 'ACTIVE'
          AND g.planned_end_date < ?
        ORDER BY g.planned_end_date ASC, g.id ASC
        LIMIT ?
        """,
        (today, int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]


def list_goals_due_soon(
    conn: sqlite3.Connection,
    *,
    today: str,
    tomorrow: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            g.id,
            g.cycle_id,
            g.title,
            g.success_criteria,
            g.planned_end_date,
            g.status,
            COALESCE((SELECT COUNT(1) FROM goal_reschedule_events e WHERE e.goal_id = g.id), 0) AS reschedule_count
        FROM goals g
        WHERE g.status = 'ACTIVE'
          AND g.planned_end_date IN (?, ?)
        ORDER BY g.planned_end_date ASC, g.id ASC
        LIMIT ?
        """,
        (today, tomorrow, int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]


def list_goals_at_risk(conn: sqlite3.Connection, *, today: str, limit: int = 20) -> list[dict[str, Any]]:
    today_d = date.fromisoformat(today)
    goal_rows = conn.execute(
        """
        SELECT id, cycle_id, title, success_criteria, planned_end_date, status
        FROM goals
        WHERE status = 'ACTIVE'
        ORDER BY planned_end_date ASC, id ASC
        """
    ).fetchall()

    out: list[dict[str, Any]] = []
    for g in goal_rows:
        planned = date.fromisoformat(str(g["planned_end_date"]))
        due_in = (planned - today_d).days
        if due_in > 3:
            continue
        goal_id = int(g["id"])
        movement = _goal_has_movement_last3d(conn, goal_id, today_d)
        blocks_next2 = _goal_has_time_blocks_next2d(conn, goal_id, today_d)
        res_row = conn.execute("SELECT COUNT(1) AS c FROM goal_reschedule_events WHERE goal_id = ?", (goal_id,)).fetchone()
        res_cnt = int(res_row["c"] if res_row else 0)
        if (not movement) or (not blocks_next2) or (res_cnt >= 3):
            item = dict(g)
            item["reschedule_count"] = res_cnt
            out.append(item)
        if len(out) >= int(limit):
            break
    return out


def list_tasks_today(conn: sqlite3.Connection, *, today: str, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, title, planned_at, status, state, goal_id, parent_id, parent_type
        FROM tasks
        WHERE substr(COALESCE(planned_at,''), 1, 10) = ?
        ORDER BY planned_at ASC, id ASC
        LIMIT ?
        """,
        (today, int(limit)),
    ).fetchall()
    return _with_task_levels(conn, [dict(r) for r in rows])


def list_tasks_tomorrow(conn: sqlite3.Connection, *, tomorrow: str, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, title, planned_at, status, state, goal_id, parent_id, parent_type
        FROM tasks
        WHERE substr(COALESCE(planned_at,''), 1, 10) = ?
        ORDER BY planned_at ASC, id ASC
        LIMIT ?
        """,
        (tomorrow, int(limit)),
    ).fetchall()
    return _with_task_levels(conn, [dict(r) for r in rows])


def list_tasks_active(conn: sqlite3.Connection, *, limit: int = 100) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, title, planned_at, status, state, goal_id, parent_id, parent_type
        FROM tasks
        WHERE UPPER(COALESCE(status, '')) NOT IN ('DONE', 'ARCHIVED', 'CANCELED', 'CANCELLED')
        ORDER BY
            CASE WHEN planned_at IS NULL OR planned_at = '' THEN 1 ELSE 0 END,
            planned_at ASC,
            id ASC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    return _with_task_levels(conn, [dict(r) for r in rows])


def _with_task_levels(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def _safe_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None

    cache: dict[int, tuple[str | None, int | None]] = {}

    def _task_parent(task_id: int) -> tuple[str | None, int | None]:
        if task_id in cache:
            return cache[task_id]
        row = conn.execute(
            "SELECT parent_type, parent_id FROM tasks WHERE id = ?",
            (int(task_id),),
        ).fetchone()
        if row is None:
            cache[task_id] = (None, None)
            return cache[task_id]
        ptype = str(row["parent_type"]).strip().lower() if row["parent_type"] is not None else None
        pid = _safe_int(row["parent_id"])
        cache[task_id] = (ptype, pid)
        return cache[task_id]

    out: list[dict[str, Any]] = []
    for item in rows:
        row = dict(item)
        task_id = _safe_int(row.get("id"))
        level = 0
        seen: set[int] = set()
        cur_id = task_id
        for _ in range(4):
            if cur_id is None or cur_id in seen:
                break
            seen.add(cur_id)
            ptype, pid = _task_parent(cur_id)
            if ptype != "task" or pid is None:
                break
            level += 1
            cur_id = pid
        row["parent_id"] = _safe_int(row.get("parent_id"))
        row["level"] = min(level, 3)
        row.pop("parent_type", None)
        out.append(row)
    return out


def compute_daily_digest(conn: sqlite3.Connection, *, today: str, tomorrow: str, user_id: str) -> dict[str, int]:
    _ = user_id  # reserved for per-user filters when multi-tenant user scoping is applied.
    if not _table_exists(conn, "goals"):
        return {
            "goals_active": 0,
            "goals_overdue": 0,
            "goals_due_soon": 0,
            "goals_at_risk": 0,
            "tasks_today": 0,
            "tasks_tomorrow": 0,
            "tasks_active_total": 0,
        }

    today_d = date.fromisoformat(today)
    goal_rows = conn.execute(
        """
        SELECT id, planned_end_date, status
        FROM goals
        WHERE status = 'ACTIVE'
        """
    ).fetchall()
    goals_active = len(goal_rows)
    goals_overdue = 0
    goals_due_soon = 0
    goals_at_risk = 0
    for g in goal_rows:
        goal_id = int(g["id"])
        planned = date.fromisoformat(str(g["planned_end_date"]))
        due_in = (planned - today_d).days
        if due_in < 0:
            goals_overdue += 1
        if str(g["planned_end_date"]) in {today, tomorrow}:
            goals_due_soon += 1

        if due_in <= 3:
            movement = _goal_has_movement_last3d(conn, goal_id, today_d)
            blocks_next2 = _goal_has_time_blocks_next2d(conn, goal_id, today_d)
            res_row = conn.execute("SELECT COUNT(1) AS c FROM goal_reschedule_events WHERE goal_id = ?", (goal_id,)).fetchone()
            res_cnt = int(res_row["c"] if res_row else 0)
            r1 = not movement
            r2 = not blocks_next2
            r3 = res_cnt >= 3
            if r1 or r2 or r3:
                goals_at_risk += 1

    tasks_today_row = conn.execute(
        "SELECT COUNT(1) AS c FROM tasks WHERE substr(COALESCE(planned_at,''), 1, 10) = ?",
        (today,),
    ).fetchone()
    tasks_tom_row = conn.execute(
        "SELECT COUNT(1) AS c FROM tasks WHERE substr(COALESCE(planned_at,''), 1, 10) = ?",
        (tomorrow,),
    ).fetchone()
    tasks_active_row = conn.execute(
        """
        SELECT COUNT(1) AS c
        FROM tasks
        WHERE UPPER(COALESCE(status, '')) NOT IN ('DONE', 'ARCHIVED', 'CANCELED', 'CANCELLED')
        """
    ).fetchone()

    return {
        "goals_active": goals_active,
        "goals_overdue": goals_overdue,
        "goals_due_soon": goals_due_soon,
        "goals_at_risk": goals_at_risk,
        "tasks_today": int(tasks_today_row["c"] if tasks_today_row else 0),
        "tasks_tomorrow": int(tasks_tom_row["c"] if tasks_tom_row else 0),
        "tasks_active_total": int(tasks_active_row["c"] if tasks_active_row else 0),
    }
