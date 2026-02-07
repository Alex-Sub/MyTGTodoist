import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

DB_PATH = os.getenv("DB_PATH", "/data/organizer.db")
P2_ENFORCE_STATUS = os.getenv("P2_ENFORCE_STATUS", "0") == "1"

_STATUS_ALIASES = {
    "INBOX": "NEW",
    "TODO": "NEW",
}


def normalize_status(status: str) -> str:
    if not status:
        return "NEW"
    s = status.strip().upper()
    return _STATUS_ALIASES.get(s, s)


@dataclass(frozen=True)
class Task:
    id: int
    title: str
    status: str
    state: str
    planned_at: str | None
    calendar_event_id: str | None
    source_msg_id: str | None


@dataclass(frozen=True)
class Subtask:
    id: int
    task_id: int
    title: str
    status: str
    source_msg_id: str | None


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _enforce_or_warn(message: str) -> None:
    if P2_ENFORCE_STATUS:
        raise ValueError(message)
    logging.warning("P2_INVARIANT_VIOLATION: %s", message)


def create_task(title: str, status: str = "INBOX", source_msg_id: str | None = None) -> Task:
    status = normalize_status(status)
    with _get_conn() as conn:
        if source_msg_id:
            row = conn.execute(
                """
                SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id
                FROM tasks
                WHERE source_msg_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (source_msg_id,),
            ).fetchone()
            if row:
                return Task(
                    id=int(row["id"]),
                    title=str(row["title"]),
                    status=str(row["status"]),
                    state=str(row["state"] or ""),
                    planned_at=row["planned_at"],
                    calendar_event_id=row["calendar_event_id"],
                    source_msg_id=row["source_msg_id"],
                )
        cur = conn.execute(
            """
            INSERT INTO tasks (title, status, state, planned_at, created_at, updated_at, completed_at, source_msg_id)
            VALUES (?, ?, 'NEW', NULL, ?, ?, NULL, ?)
            """,
            (title, status, _now_iso(), _now_iso(), source_msg_id),
        )
        conn.commit()
        task_id = int(cur.lastrowid)
        row = conn.execute(
            "SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id FROM tasks WHERE id = ?",
            (task_id,),
        ).fetchone()
    return Task(
        id=int(row["id"]),
        title=str(row["title"]),
        status=str(row["status"]),
        state=str(row["state"] or ""),
        planned_at=row["planned_at"],
        calendar_event_id=row["calendar_event_id"],
        source_msg_id=row["source_msg_id"],
    )


def create_subtask(
    task_id: int,
    title: str,
    status: str = "TODO",
    source_msg_id: str | None = None,
) -> Subtask:
    status = normalize_status(status)
    with _get_conn() as conn:
        if source_msg_id:
            row = conn.execute(
                """
                SELECT id, task_id, title, status, source_msg_id
                FROM subtasks
                WHERE source_msg_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (source_msg_id,),
            ).fetchone()
            if row:
                if int(row["task_id"]) != int(task_id):
                    _enforce_or_warn("source_msg_id collision across tasks")
                return Subtask(
                    id=int(row["id"]),
                    task_id=int(row["task_id"]),
                    title=str(row["title"]),
                    status=str(row["status"]),
                    source_msg_id=row["source_msg_id"],
                )
        task = conn.execute(
            "SELECT id, status FROM tasks WHERE id = ?",
            (int(task_id),),
        ).fetchone()
        if not task:
            raise ValueError("task not found")
        task_status = str(task["status"])
        if task_status in {"DONE", "FAILED"}:
            _enforce_or_warn("cannot create subtask for DONE/FAILED task")

        cur = conn.execute(
            """
            INSERT INTO subtasks (task_id, title, status, created_at, updated_at, completed_at, source_msg_id)
            VALUES (?, ?, ?, ?, ?, NULL, ?)
            """,
            (int(task_id), title, status, _now_iso(), _now_iso(), source_msg_id),
        )
        conn.commit()
        sub_id = int(cur.lastrowid)
        row = conn.execute(
            "SELECT id, task_id, title, status, source_msg_id FROM subtasks WHERE id = ?",
            (sub_id,),
        ).fetchone()
    return Subtask(
        id=int(row["id"]),
        task_id=int(row["task_id"]),
        title=str(row["title"]),
        status=str(row["status"]),
        source_msg_id=row["source_msg_id"],
    )


def complete_subtask(subtask_id: int) -> Subtask:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT id, task_id, title, status, source_msg_id FROM subtasks WHERE id = ?",
            (int(subtask_id),),
        ).fetchone()
        if not row:
            raise ValueError("subtask not found")
        if str(row["status"]) == "DONE":
            return Subtask(
                id=int(row["id"]),
                task_id=int(row["task_id"]),
                title=str(row["title"]),
                status=str(row["status"]),
                source_msg_id=row["source_msg_id"],
            )
        conn.execute(
            """
            UPDATE subtasks
            SET status = 'DONE',
                updated_at = ?,
                completed_at = COALESCE(completed_at, ?)
            WHERE id = ?
            """,
            (_now_iso(), _now_iso(), int(subtask_id)),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id, task_id, title, status, source_msg_id FROM subtasks WHERE id = ?",
            (int(subtask_id),),
        ).fetchone()
    return Subtask(
        id=int(row["id"]),
        task_id=int(row["task_id"]),
        title=str(row["title"]),
        status=str(row["status"]),
        source_msg_id=row["source_msg_id"],
    )


def complete_task(task_id: int) -> Task:
    with _get_conn() as conn:
        task = conn.execute(
            "SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id FROM tasks WHERE id = ?",
            (int(task_id),),
        ).fetchone()
        if not task:
            raise ValueError("task not found")
        if str(task["status"]) == "DONE":
            return Task(
                id=int(task["id"]),
                title=str(task["title"]),
                status=str(task["status"]),
                state=str(task["state"] or ""),
                planned_at=task["planned_at"],
                calendar_event_id=task["calendar_event_id"],
                source_msg_id=task["source_msg_id"],
            )

        open_cnt = conn.execute(
            "SELECT COUNT(*) AS cnt FROM subtasks WHERE task_id = ? AND status != 'DONE'",
            (int(task_id),),
        ).fetchone()
        open_subtasks = int(open_cnt["cnt"] or 0)
        if open_subtasks > 0:
            _enforce_or_warn("cannot complete task with open subtasks")

        conn.execute(
            """
            UPDATE tasks
            SET status = 'DONE',
                state = 'DONE',
                updated_at = ?,
                completed_at = COALESCE(completed_at, ?)
            WHERE id = ?
            """,
            (_now_iso(), _now_iso(), int(task_id)),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id FROM tasks WHERE id = ?",
            (int(task_id),),
        ).fetchone()
    return Task(
        id=int(row["id"]),
        title=str(row["title"]),
        status=str(row["status"]),
        state=str(row["state"] or ""),
        planned_at=row["planned_at"],
        calendar_event_id=row["calendar_event_id"],
        source_msg_id=row["source_msg_id"],
    )


def plan_task(task_id: int, planned_at_iso: str) -> Task:
    # FSM (canonical): states NEW, PLANNED, SCHEDULED, DONE, FAILED, CANCELLED.
    # Allowed: NEW->PLANNED (plan_task), PLANNED->SCHEDULED (calendar create/patch success),
    # SCHEDULED->PLANNED (plan_task re-plan), SCHEDULED->DONE (complete_task),
    # SCHEDULED->FAILED (terminal failure, reserved), SCHEDULED->CANCELLED (P4 scaffold),
    # PLANNED->DONE (complete_task), PLANNED->CANCELLED (P4 scaffold).
    # Forbidden: DONE/FAILED/CANCELLED -> NEW/PLANNED/SCHEDULED; no Calendar -> Task autologic.
    with _get_conn() as conn:
        task = conn.execute(
            "SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id FROM tasks WHERE id = ?",
            (int(task_id),),
        ).fetchone()
        if not task:
            raise ValueError("task not found")
        status = str(task["status"] or "")
        state = str(task["state"] or "")
        if status in {"DONE", "FAILED", "CANCELLED"} or state in {"DONE", "FAILED", "CANCELLED"}:
            new_state = state
        else:
            new_state = "PLANNED"
        conn.execute(
            """
            UPDATE tasks
            SET planned_at = ?,
                state = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (planned_at_iso, new_state, _now_iso(), int(task_id)),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id FROM tasks WHERE id = ?",
            (int(task_id),),
        ).fetchone()
    return Task(
        id=int(row["id"]),
        title=str(row["title"]),
        status=str(row["status"]),
        state=str(row["state"] or ""),
        planned_at=row["planned_at"],
        calendar_event_id=row["calendar_event_id"],
        source_msg_id=row["source_msg_id"],
    )
