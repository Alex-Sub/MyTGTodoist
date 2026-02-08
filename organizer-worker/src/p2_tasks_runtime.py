import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, date, timedelta
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "data" / "p2_runtime.sqlite3"
DB_PATH = os.getenv("DB_PATH", "")
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
    parent_type: str | None
    parent_id: int | None


@dataclass(frozen=True)
class Subtask:
    id: int
    task_id: int
    title: str
    status: str
    source_msg_id: str | None


@dataclass(frozen=True)
class Direction:
    id: int
    title: str
    note: str | None
    status: str
    source_msg_id: str | None


@dataclass(frozen=True)
class Project:
    id: int
    direction_id: int | None
    title: str
    status: str
    source_msg_id: str | None


@dataclass(frozen=True)
class Cycle:
    id: int
    type: str
    period_key: str | None
    period_start: str | None
    period_end: str | None
    status: str
    summary: str | None
    source_msg_id: str | None


@dataclass(frozen=True)
class CycleOutcome:
    id: int
    cycle_id: int
    kind: str
    text: str
    created_at: str


@dataclass(frozen=True)
class CycleGoal:
    id: int
    cycle_id: int
    text: str
    status: str
    continued_from_goal_id: int | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class Regulation:
    id: int
    title: str
    note: str | None
    status: str
    day_of_month: int
    due_time_local: str | None
    source_msg_id: str | None


@dataclass(frozen=True)
class RegulationRun:
    id: int
    regulation_id: int
    period_key: str
    status: str
    due_date: str
    due_time_local: str | None
    done_at: str | None
    created_at: str
    updated_at: str


_ALLOWED_CYCLE_GOAL_STATUS = {"ACTIVE", "ACHIEVED", "DROPPED"}


def _resolve_db_path() -> str:
    env_path = os.getenv("P2_DB_PATH")
    if env_path:
        return env_path
    if DB_PATH:
        return DB_PATH
    return str(DEFAULT_DB_PATH)


def _get_conn() -> sqlite3.Connection:
    db_path = _resolve_db_path()
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_lastrowid(cur: sqlite3.Cursor) -> int:
    lastrowid = cur.lastrowid
    if lastrowid is None:
        raise RuntimeError("insert failed: no rowid")
    return int(lastrowid)


def _enforce_or_warn(message: str) -> None:
    if P2_ENFORCE_STATUS:
        raise ValueError(message)
    logging.warning("P2_INVARIANT_VIOLATION: %s", message)

_ALLOWED_PARENT_TYPES = {"project", "cycle", "regulation_run"}
_ALLOWED_CYCLE_OUTCOME_KINDS = {"GOAL"}


def _normalize_parent_type(parent_type: str | None) -> str | None:
    if not parent_type:
        return None
    s = parent_type.strip().lower()
    if s == "none":
        return None
    if s not in _ALLOWED_PARENT_TYPES:
        raise ValueError("invalid parent_type")
    return s


def create_task(
    title: str,
    status: str = "INBOX",
    source_msg_id: str | None = None,
    parent_type: str | None = None,
    parent_id: int | None = None,
) -> Task:
    status = normalize_status(status)
    parent_type_norm = _normalize_parent_type(parent_type)
    parent_id_use = int(parent_id) if parent_id is not None else None
    if parent_type_norm and parent_id_use is None:
        raise ValueError("parent_id is required")
    with _get_conn() as conn:
        if source_msg_id:
            row = conn.execute(
                """
                SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id,
                       parent_type, parent_id
                FROM tasks
                WHERE source_msg_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (source_msg_id,),
            ).fetchone()
            if row:
                existing_parent_type = row["parent_type"]
                existing_parent_id = row["parent_id"]
                if (
                    (parent_type_norm or None) != (existing_parent_type or None)
                    or (parent_id_use or None) != (existing_parent_id or None)
                ):
                    _enforce_or_warn("source_msg_id collision across tasks")
                return Task(
                    id=int(row["id"]),
                    title=str(row["title"]),
                    status=str(row["status"]),
                    state=str(row["state"] or ""),
                    planned_at=row["planned_at"],
                    calendar_event_id=row["calendar_event_id"],
                    source_msg_id=row["source_msg_id"],
                    parent_type=row["parent_type"],
                    parent_id=row["parent_id"],
                )
        if parent_type_norm == "project":
            if parent_id_use is None:
                raise ValueError("parent_id is required")
            row = conn.execute(
                "SELECT id FROM projects WHERE id = ?",
                (parent_id_use,),
            ).fetchone()
            if not row:
                raise ValueError("project not found")
        elif parent_type_norm == "cycle":
            if parent_id_use is None:
                raise ValueError("parent_id is required")
            row = conn.execute(
                "SELECT id FROM cycles WHERE id = ?",
                (parent_id_use,),
            ).fetchone()
            if not row:
                raise ValueError("cycle not found")
        elif parent_type_norm == "regulation_run":
            if parent_id_use is None:
                raise ValueError("parent_id is required")
            row = conn.execute(
                "SELECT id FROM regulation_runs WHERE id = ?",
                (parent_id_use,),
            ).fetchone()
            if not row:
                raise ValueError("regulation_run not found")
        cur = conn.execute(
            """
            INSERT INTO tasks (
                title, status, state, planned_at,
                created_at, updated_at, completed_at,
                source_msg_id, parent_type, parent_id
            )
            VALUES (?, ?, 'NEW', NULL, ?, ?, NULL, ?, ?, ?)
            """,
            (title, status, _now_iso(), _now_iso(), source_msg_id, parent_type_norm, parent_id_use),
        )
        conn.commit()
        task_id = _require_lastrowid(cur)
        row = conn.execute(
            """
            SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id,
                   parent_type, parent_id
            FROM tasks
            WHERE id = ?
            """,
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
        parent_type=row["parent_type"],
        parent_id=row["parent_id"],
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
        sub_id = _require_lastrowid(cur)
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
            """
            SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id,
                   parent_type, parent_id
            FROM tasks
            WHERE id = ?
            """,
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
                parent_type=task["parent_type"],
                parent_id=task["parent_id"],
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
            """
            SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id,
                   parent_type, parent_id
            FROM tasks
            WHERE id = ?
            """,
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
        parent_type=row["parent_type"],
        parent_id=row["parent_id"],
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
            """
            SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id,
                   parent_type, parent_id
            FROM tasks
            WHERE id = ?
            """,
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
            """
            SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id,
                   parent_type, parent_id
            FROM tasks
            WHERE id = ?
            """,
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
        parent_type=row["parent_type"],
        parent_id=row["parent_id"],
    )


def create_direction(title: str, note: str | None = None, source_msg_id: str | None = None) -> Direction:
    with _get_conn() as conn:
        if source_msg_id:
            row = conn.execute(
                """
                SELECT id, title, note, status, source_msg_id
                FROM directions
                WHERE source_msg_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (source_msg_id,),
            ).fetchone()
            if row:
                return Direction(
                    id=int(row["id"]),
                    title=str(row["title"]),
                    note=row["note"],
                    status=str(row["status"]),
                    source_msg_id=row["source_msg_id"],
                )
        cur = conn.execute(
            """
            INSERT INTO directions (title, note, status, source_msg_id, created_at, updated_at)
            VALUES (?, ?, 'ACTIVE', ?, ?, ?)
            """,
            (title, note, source_msg_id, _now_iso(), _now_iso()),
        )
        direction_id = _require_lastrowid(cur)
        conn.commit()
        row = conn.execute(
            "SELECT id, title, note, status, source_msg_id FROM directions WHERE id = ?",
            (direction_id,),
        ).fetchone()
    return Direction(
        id=int(row["id"]),
        title=str(row["title"]),
        note=row["note"],
        status=str(row["status"]),
        source_msg_id=row["source_msg_id"],
    )


def create_project(
    title: str,
    direction_id: int | None = None,
    source_msg_id: str | None = None,
) -> Project:
    with _get_conn() as conn:
        if source_msg_id:
            row = conn.execute(
                """
                SELECT id, direction_id, title, status, source_msg_id
                FROM projects
                WHERE source_msg_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (source_msg_id,),
            ).fetchone()
            if row:
                return Project(
                    id=int(row["id"]),
                    direction_id=int(row["direction_id"]) if row["direction_id"] is not None else None,
                    title=str(row["title"]),
                    status=str(row["status"]),
                    source_msg_id=row["source_msg_id"],
                )
        direction_id_use = int(direction_id) if direction_id is not None else None
        if direction_id_use is not None:
            row = conn.execute(
                "SELECT id FROM directions WHERE id = ?",
                (direction_id_use,),
            ).fetchone()
            if not row:
                raise ValueError("direction not found")
        cur = conn.execute(
            """
            INSERT INTO projects (direction_id, title, status, source_msg_id, created_at, updated_at, closed_at)
            VALUES (?, ?, 'ACTIVE', ?, ?, ?, NULL)
            """,
            (direction_id_use, title, source_msg_id, _now_iso(), _now_iso()),
        )
        project_id = _require_lastrowid(cur)
        conn.commit()
        row = conn.execute(
            "SELECT id, direction_id, title, status, source_msg_id FROM projects WHERE id = ?",
            (project_id,),
        ).fetchone()
    return Project(
        id=int(row["id"]),
        direction_id=int(row["direction_id"]) if row["direction_id"] is not None else None,
        title=str(row["title"]),
        status=str(row["status"]),
        source_msg_id=row["source_msg_id"],
    )


def convert_direction_to_project(
    direction_id: int,
    title: str | None = None,
    source_msg_id: str | None = None,
) -> Project:
    with _get_conn() as conn:
        direction = conn.execute(
            "SELECT id, title FROM directions WHERE id = ?",
            (int(direction_id),),
        ).fetchone()
        if not direction:
            raise ValueError("direction not found")
        if source_msg_id:
            row = conn.execute(
                """
                SELECT id, direction_id, title, status, source_msg_id
                FROM projects
                WHERE source_msg_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (source_msg_id,),
            ).fetchone()
            if row:
                return Project(
                    id=int(row["id"]),
                    direction_id=int(row["direction_id"]) if row["direction_id"] is not None else None,
                    title=str(row["title"]),
                    status=str(row["status"]),
                    source_msg_id=row["source_msg_id"],
                )
        existing = conn.execute(
            """
            SELECT id, direction_id, title, status, source_msg_id
            FROM projects
            WHERE direction_id = ?
              AND status != 'ARCHIVED'
            ORDER BY id ASC
            LIMIT 1
            """,
            (int(direction_id),),
        ).fetchone()
        if existing:
            return Project(
                id=int(existing["id"]),
                direction_id=int(existing["direction_id"]) if existing["direction_id"] is not None else None,
                title=str(existing["title"]),
                status=str(existing["status"]),
                source_msg_id=existing["source_msg_id"],
            )
        title_use = title if title is not None and title.strip() else str(direction["title"])
        cur = conn.execute(
            """
            INSERT INTO projects (direction_id, title, status, source_msg_id, created_at, updated_at, closed_at)
            VALUES (?, ?, 'ACTIVE', ?, ?, ?, NULL)
            """,
            (int(direction_id), title_use, source_msg_id, _now_iso(), _now_iso()),
        )
        project_id = _require_lastrowid(cur)
        conn.commit()
        row = conn.execute(
            "SELECT id, direction_id, title, status, source_msg_id FROM projects WHERE id = ?",
            (project_id,),
        ).fetchone()
    return Project(
        id=int(row["id"]),
        direction_id=int(row["direction_id"]) if row["direction_id"] is not None else None,
        title=str(row["title"]),
        status=str(row["status"]),
        source_msg_id=row["source_msg_id"],
    )


def start_cycle(
    type: str,
    period_key: str | None = None,
    source_msg_id: str | None = None,
) -> Cycle:
    type_norm = (type or "").strip()
    if not type_norm:
        raise ValueError("type is required")
    if type_norm not in {"MONTHLY", "QUARTERLY"}:
        raise ValueError("invalid cycle type")
    period_key_use = period_key if period_key is not None else None
    if type_norm in {"MONTHLY", "QUARTERLY"} and not period_key_use:
        raise ValueError("period_key is required for MONTHLY/QUARTERLY")
    with _get_conn() as conn:
        if source_msg_id:
            row = conn.execute(
                """
                SELECT id, type, period_key, period_start, period_end, status, summary, source_msg_id
                FROM cycles
                WHERE source_msg_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (source_msg_id,),
            ).fetchone()
            if row:
                return Cycle(
                    id=int(row["id"]),
                    type=str(row["type"]),
                    period_key=row["period_key"],
                    period_start=row["period_start"],
                    period_end=row["period_end"],
                    status=str(row["status"]),
                    summary=row["summary"],
                    source_msg_id=row["source_msg_id"],
                )
        if period_key_use is None:
            row = conn.execute(
                """
                SELECT id, type, period_key, period_start, period_end, status, summary, source_msg_id
                FROM cycles
                WHERE status = 'OPEN'
                  AND type = ?
                  AND period_key IS NULL
                ORDER BY id ASC
                LIMIT 1
                """,
                (type_norm,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT id, type, period_key, period_start, period_end, status, summary, source_msg_id
                FROM cycles
                WHERE status = 'OPEN'
                  AND type = ?
                  AND period_key = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (type_norm, period_key_use),
            ).fetchone()
        if row:
            return Cycle(
                id=int(row["id"]),
                type=str(row["type"]),
                period_key=row["period_key"],
                period_start=row["period_start"],
                period_end=row["period_end"],
                status=str(row["status"]),
                summary=row["summary"],
                source_msg_id=row["source_msg_id"],
            )
        cur = conn.execute(
            """
            INSERT INTO cycles (
                type, period_key, period_start, period_end,
                status, summary, source_msg_id,
                created_at, updated_at, closed_at
            )
            VALUES (?, ?, NULL, NULL, 'OPEN', NULL, ?, ?, ?, NULL)
            """,
            (type_norm, period_key_use, source_msg_id, _now_iso(), _now_iso()),
        )
        cycle_id = _require_lastrowid(cur)
        conn.commit()
        row = conn.execute(
            """
            SELECT id, type, period_key, period_start, period_end, status, summary, source_msg_id
            FROM cycles
            WHERE id = ?
            """,
            (cycle_id,),
        ).fetchone()
    return Cycle(
        id=int(row["id"]),
        type=str(row["type"]),
        period_key=row["period_key"],
        period_start=row["period_start"],
        period_end=row["period_end"],
        status=str(row["status"]),
        summary=row["summary"],
        source_msg_id=row["source_msg_id"],
    )


def close_cycle(
    cycle_id: int,
    status: str,
    summary: str | None = None,
) -> Cycle:
    status_norm = (status or "").strip().upper()
    if status_norm not in {"DONE", "SKIPPED"}:
        raise ValueError("invalid cycle status")
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, type, period_key, period_start, period_end, status, summary, source_msg_id
            FROM cycles
            WHERE id = ?
            """,
            (int(cycle_id),),
        ).fetchone()
        if not row:
            raise ValueError("cycle not found")
        if str(row["status"] or "") in {"DONE", "SKIPPED"}:
            return Cycle(
                id=int(row["id"]),
                type=str(row["type"]),
                period_key=row["period_key"],
                period_start=row["period_start"],
                period_end=row["period_end"],
                status=str(row["status"]),
                summary=row["summary"],
                source_msg_id=row["source_msg_id"],
            )
        summary_use = summary if summary is not None else row["summary"]
        conn.execute(
            """
            UPDATE cycles
            SET status = ?,
                summary = ?,
                updated_at = ?,
                closed_at = COALESCE(closed_at, ?)
            WHERE id = ?
            """,
            (status_norm, summary_use, _now_iso(), _now_iso(), int(cycle_id)),
        )
        conn.commit()
        row = conn.execute(
            """
            SELECT id, type, period_key, period_start, period_end, status, summary, source_msg_id
            FROM cycles
            WHERE id = ?
            """,
            (int(cycle_id),),
        ).fetchone()
    return Cycle(
        id=int(row["id"]),
        type=str(row["type"]),
        period_key=row["period_key"],
        period_start=row["period_start"],
        period_end=row["period_end"],
        status=str(row["status"]),
        summary=row["summary"],
        source_msg_id=row["source_msg_id"],
    )


def add_cycle_outcome(
    cycle_id: int,
    kind: str,
    text: str,
) -> CycleOutcome:
    kind_norm = (kind or "").strip().upper()
    if kind_norm not in _ALLOWED_CYCLE_OUTCOME_KINDS:
        raise ValueError("invalid cycle outcome kind")
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM cycles WHERE id = ?",
            (int(cycle_id),),
        ).fetchone()
        if not row:
            raise ValueError("cycle not found")
        cur = conn.execute(
            """
            INSERT INTO cycle_outcomes (cycle_id, kind, text, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (int(cycle_id), kind_norm, text, _now_iso()),
        )
        outcome_id = _require_lastrowid(cur)
        conn.commit()
        row = conn.execute(
            """
            SELECT id, cycle_id, kind, text, created_at
            FROM cycle_outcomes
            WHERE id = ?
            """,
            (outcome_id,),
        ).fetchone()
    return CycleOutcome(
        id=int(row["id"]),
        cycle_id=int(row["cycle_id"]),
        kind=str(row["kind"]),
        text=str(row["text"]),
        created_at=str(row["created_at"]),
    )


def add_cycle_goal(cycle_id: int, text: str) -> CycleGoal:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM cycles WHERE id = ?",
            (int(cycle_id),),
        ).fetchone()
        if not row:
            raise ValueError("cycle not found")
        cur = conn.execute(
            """
            INSERT INTO cycle_goals (cycle_id, text, status, continued_from_goal_id, created_at, updated_at)
            VALUES (?, ?, 'ACTIVE', NULL, ?, ?)
            """,
            (int(cycle_id), text, _now_iso(), _now_iso()),
        )
        goal_id = _require_lastrowid(cur)
        conn.commit()
        row = conn.execute(
            """
            SELECT id, cycle_id, text, status, continued_from_goal_id, created_at, updated_at
            FROM cycle_goals
            WHERE id = ?
            """,
            (goal_id,),
        ).fetchone()
    return CycleGoal(
        id=int(row["id"]),
        cycle_id=int(row["cycle_id"]),
        text=str(row["text"]),
        status=str(row["status"]),
        continued_from_goal_id=int(row["continued_from_goal_id"]) if row["continued_from_goal_id"] is not None else None,
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def continue_cycle_goal(goal_id: int, target_cycle_id: int) -> CycleGoal:
    with _get_conn() as conn:
        goal = conn.execute(
            """
            SELECT id, text
            FROM cycle_goals
            WHERE id = ?
            """,
            (int(goal_id),),
        ).fetchone()
        if not goal:
            raise ValueError("goal not found")
        row = conn.execute(
            "SELECT id FROM cycles WHERE id = ?",
            (int(target_cycle_id),),
        ).fetchone()
        if not row:
            raise ValueError("cycle not found")
        cur = conn.execute(
            """
            INSERT INTO cycle_goals (cycle_id, text, status, continued_from_goal_id, created_at, updated_at)
            VALUES (?, ?, 'ACTIVE', ?, ?, ?)
            """,
            (int(target_cycle_id), str(goal["text"]), int(goal_id), _now_iso(), _now_iso()),
        )
        new_goal_id = _require_lastrowid(cur)
        conn.commit()
        row = conn.execute(
            """
            SELECT id, cycle_id, text, status, continued_from_goal_id, created_at, updated_at
            FROM cycle_goals
            WHERE id = ?
            """,
            (new_goal_id,),
        ).fetchone()
    return CycleGoal(
        id=int(row["id"]),
        cycle_id=int(row["cycle_id"]),
        text=str(row["text"]),
        status=str(row["status"]),
        continued_from_goal_id=int(row["continued_from_goal_id"]) if row["continued_from_goal_id"] is not None else None,
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def update_cycle_goal_status(goal_id: int, status: str) -> CycleGoal:
    status_norm = (status or "").strip().upper()
    if status_norm not in _ALLOWED_CYCLE_GOAL_STATUS:
        raise ValueError("invalid goal status")
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, cycle_id, text, status, continued_from_goal_id, created_at, updated_at
            FROM cycle_goals
            WHERE id = ?
            """,
            (int(goal_id),),
        ).fetchone()
        if not row:
            raise ValueError("goal not found")
        if str(row["status"] or "") != status_norm:
            conn.execute(
                """
                UPDATE cycle_goals
                SET status = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (status_norm, _now_iso(), int(goal_id)),
            )
            conn.commit()
            row = conn.execute(
                """
                SELECT id, cycle_id, text, status, continued_from_goal_id, created_at, updated_at
                FROM cycle_goals
                WHERE id = ?
                """,
                (int(goal_id),),
            ).fetchone()
    return CycleGoal(
        id=int(row["id"]),
        cycle_id=int(row["cycle_id"]),
        text=str(row["text"]),
        status=str(row["status"]),
        continued_from_goal_id=int(row["continued_from_goal_id"]) if row["continued_from_goal_id"] is not None else None,
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _clamp_day(y: int, m: int, d: int) -> int:
    if d < 1:
        return 1
    if m < 1 or m > 12:
        m = 12 if m > 12 else 1
    if m == 12:
        next_m = date(y + 1, 1, 1)
    else:
        next_m = date(y, m + 1, 1)
    last = (next_m - timedelta(days=1)).day
    return min(d, last)


def _parse_period_key(period_key: str) -> tuple[int, int]:
    if not period_key:
        raise ValueError("period_key is required")
    parts = period_key.split("-")
    if len(parts) != 2:
        raise ValueError("invalid period_key")
    y = int(parts[0])
    m = int(parts[1])
    if m < 1 or m > 12:
        raise ValueError("invalid period_key")
    return y, m


def _normalize_due_time_local(due_time_local: str | None) -> str | None:
    if due_time_local is None:
        return None
    s = due_time_local.strip()
    if not s:
        return None
    if not re.match(r"^\d{2}:\d{2}$", s):
        raise ValueError("invalid due_time_local")
    hh = int(s[:2])
    mm = int(s[3:])
    if hh > 23 or mm > 59:
        raise ValueError("invalid due_time_local")
    return s


def _validate_day_of_month(day_of_month: int) -> int:
    day = int(day_of_month)
    if day < 1 or day > 31:
        raise ValueError("invalid day_of_month")
    return day


def create_regulation(
    title: str,
    day_of_month: int | None = None,
    note: str | None = None,
    due_time_local: str | None = None,
    source_msg_id: str | None = None,
) -> Regulation:
    if not title:
        raise ValueError("title is required")
    day_val = _validate_day_of_month(day_of_month if day_of_month is not None else 1)
    due_time_use = _normalize_due_time_local(due_time_local) or "10:00"
    with _get_conn() as conn:
        if source_msg_id:
            row = conn.execute(
                """
                SELECT id, title, note, status, day_of_month, due_time_local, source_msg_id
                FROM regulations
                WHERE source_msg_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (source_msg_id,),
            ).fetchone()
            if row:
                return Regulation(
                    id=int(row["id"]),
                    title=str(row["title"]),
                    note=row["note"],
                    status=str(row["status"]),
                    day_of_month=int(row["day_of_month"]),
                    due_time_local=row["due_time_local"],
                    source_msg_id=row["source_msg_id"],
                )
        cur = conn.execute(
            """
            INSERT INTO regulations (
                title, note, status, day_of_month, due_time_local,
                source_msg_id, created_at, updated_at
            )
            VALUES (?, ?, 'ACTIVE', ?, ?, ?, ?, ?)
            """,
            (title, note, day_val, due_time_use, source_msg_id, _now_iso(), _now_iso()),
        )
        regulation_id = _require_lastrowid(cur)
        conn.commit()
        row = conn.execute(
            """
            SELECT id, title, note, status, day_of_month, due_time_local, source_msg_id
            FROM regulations
            WHERE id = ?
            """,
            (regulation_id,),
        ).fetchone()
    return Regulation(
        id=int(row["id"]),
        title=str(row["title"]),
        note=row["note"],
        status=str(row["status"]),
        day_of_month=int(row["day_of_month"]),
        due_time_local=row["due_time_local"],
        source_msg_id=row["source_msg_id"],
    )


def archive_regulation(regulation_id: int) -> Regulation:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, title, note, status, day_of_month, due_time_local, source_msg_id
            FROM regulations
            WHERE id = ?
            """,
            (int(regulation_id),),
        ).fetchone()
        if not row:
            raise ValueError("regulation not found")
        if str(row["status"] or "") != "ARCHIVED":
            conn.execute(
                """
                UPDATE regulations
                SET status = 'ARCHIVED',
                    updated_at = ?
                WHERE id = ?
                """,
                (_now_iso(), int(regulation_id)),
            )
            conn.commit()
            row = conn.execute(
                """
                SELECT id, title, note, status, day_of_month, due_time_local, source_msg_id
                FROM regulations
                WHERE id = ?
                """,
                (int(regulation_id),),
            ).fetchone()
    return Regulation(
        id=int(row["id"]),
        title=str(row["title"]),
        note=row["note"],
        status=str(row["status"]),
        day_of_month=int(row["day_of_month"]),
        due_time_local=row["due_time_local"],
        source_msg_id=row["source_msg_id"],
    )


def update_regulation_schedule(
    regulation_id: int,
    day_of_month: int | None = None,
    due_time_local: str | None = None,
) -> Regulation:
    due_time_use = _normalize_due_time_local(due_time_local) if due_time_local is not None else None
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, title, note, status, day_of_month, due_time_local, source_msg_id
            FROM regulations
            WHERE id = ?
            """,
            (int(regulation_id),),
        ).fetchone()
        if not row:
            raise ValueError("regulation not found")
        day_val = int(row["day_of_month"])
        if day_of_month is not None:
            day_val = _validate_day_of_month(day_of_month)
        time_val = row["due_time_local"]
        if due_time_local is not None:
            time_val = due_time_use
        if day_val != int(row["day_of_month"]) or (time_val or None) != (row["due_time_local"] or None):
            conn.execute(
                """
                UPDATE regulations
                SET day_of_month = ?,
                    due_time_local = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (day_val, time_val, _now_iso(), int(regulation_id)),
            )
            conn.commit()
            row = conn.execute(
                """
                SELECT id, title, note, status, day_of_month, due_time_local, source_msg_id
                FROM regulations
                WHERE id = ?
                """,
                (int(regulation_id),),
            ).fetchone()
    return Regulation(
        id=int(row["id"]),
        title=str(row["title"]),
        note=row["note"],
        status=str(row["status"]),
        day_of_month=int(row["day_of_month"]),
        due_time_local=row["due_time_local"],
        source_msg_id=row["source_msg_id"],
    )


def ensure_regulation_runs(period_key: str) -> list[RegulationRun]:
    y, m = _parse_period_key(period_key)
    runs: list[RegulationRun] = []
    with _get_conn() as conn:
        regs = conn.execute(
            """
            SELECT id, title, note, status, day_of_month, due_time_local, source_msg_id
            FROM regulations
            WHERE status = 'ACTIVE'
            ORDER BY id ASC
            """
        ).fetchall()
        for reg in regs:
            reg_id = int(reg["id"])
            row = conn.execute(
                """
                SELECT id, regulation_id, period_key, status, due_date, due_time_local,
                       done_at, created_at, updated_at
                FROM regulation_runs
                WHERE regulation_id = ? AND period_key = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (reg_id, period_key),
            ).fetchone()
            if not row:
                day = _clamp_day(y, m, int(reg["day_of_month"]))
                due_date = date(y, m, day).isoformat()
                cur = conn.execute(
                    """
                    INSERT INTO regulation_runs (
                        regulation_id, period_key, status, due_date, due_time_local,
                        done_at, created_at, updated_at
                    )
                    VALUES (?, ?, 'OPEN', ?, ?, NULL, ?, ?)
                    """,
                    (reg_id, period_key, due_date, reg["due_time_local"], _now_iso(), _now_iso()),
                )
                run_id = _require_lastrowid(cur)
                conn.commit()
                row = conn.execute(
                    """
                    SELECT id, regulation_id, period_key, status, due_date, due_time_local,
                           done_at, created_at, updated_at
                    FROM regulation_runs
                    WHERE id = ?
                    """,
                    (run_id,),
                ).fetchone()
            # ensure task exists for run
            task = conn.execute(
                """
                SELECT id FROM tasks
                WHERE parent_type = 'regulation_run' AND parent_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (int(row["id"]),),
            ).fetchone()
            if not task:
                create_task(
                    title=str(reg["title"]),
                    status="NEW",
                    source_msg_id=None,
                    parent_type="regulation_run",
                    parent_id=int(row["id"]),
                )
            runs.append(
                RegulationRun(
                    id=int(row["id"]),
                    regulation_id=int(row["regulation_id"]),
                    period_key=str(row["period_key"]),
                    status=str(row["status"]),
                    due_date=str(row["due_date"]),
                    due_time_local=row["due_time_local"],
                    done_at=row["done_at"],
                    created_at=str(row["created_at"]),
                    updated_at=str(row["updated_at"]),
                )
            )
    return runs


def mark_regulation_done(run_id: int, done_at: str | None = None) -> RegulationRun:
    done_at_use = done_at if done_at is not None else _now_iso()
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, regulation_id, period_key, status, due_date, due_time_local,
                   done_at, created_at, updated_at
            FROM regulation_runs
            WHERE id = ?
            """,
            (int(run_id),),
        ).fetchone()
        if not row:
            raise ValueError("regulation_run not found")
        status = str(row["status"] or "")
        if status == "OPEN":
            conn.execute(
                """
                UPDATE regulation_runs
                SET status = 'DONE',
                    done_at = COALESCE(done_at, ?),
                    updated_at = ?
                WHERE id = ?
                """,
                (done_at_use, _now_iso(), int(run_id)),
            )
            conn.commit()
        elif status == "MISSED":
            conn.execute(
                """
                UPDATE regulation_runs
                SET done_at = COALESCE(done_at, ?),
                    updated_at = ?
                WHERE id = ?
                """,
                (done_at_use, _now_iso(), int(run_id)),
            )
            conn.commit()
        row = conn.execute(
            """
            SELECT id, regulation_id, period_key, status, due_date, due_time_local,
                   done_at, created_at, updated_at
            FROM regulation_runs
            WHERE id = ?
            """,
            (int(run_id),),
        ).fetchone()
    return RegulationRun(
        id=int(row["id"]),
        regulation_id=int(row["regulation_id"]),
        period_key=str(row["period_key"]),
        status=str(row["status"]),
        due_date=str(row["due_date"]),
        due_time_local=row["due_time_local"],
        done_at=row["done_at"],
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def monthly_regulation_tick(current_date: date) -> list[RegulationRun]:
    if not isinstance(current_date, date):
        raise ValueError("current_date is required")
    period_key = f"{current_date.year:04d}-{current_date.month:02d}"
    y, m = _parse_period_key(period_key)
    created: list[RegulationRun] = []
    with _get_conn() as conn:
        regs = conn.execute(
            """
            SELECT id, title, note, status, day_of_month, due_time_local, source_msg_id
            FROM regulations
            WHERE status = 'ACTIVE'
            ORDER BY id ASC
            """
        ).fetchall()
        for reg in regs:
            reg_id = int(reg["id"])
            exists = conn.execute(
                """
                SELECT id FROM regulation_runs
                WHERE regulation_id = ? AND period_key = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (reg_id, period_key),
            ).fetchone()
            if exists:
                continue
            day = _clamp_day(y, m, int(reg["day_of_month"]))
            due_date = date(y, m, day).isoformat()
            cur = conn.execute(
                """
                INSERT INTO regulation_runs (
                    regulation_id, period_key, status, due_date, due_time_local,
                    done_at, created_at, updated_at
                )
                VALUES (?, ?, 'OPEN', ?, ?, NULL, ?, ?)
                """,
                (reg_id, period_key, due_date, reg["due_time_local"], _now_iso(), _now_iso()),
            )
            run_id = _require_lastrowid(cur)
            conn.commit()
            row = conn.execute(
                """
                SELECT id, regulation_id, period_key, status, due_date, due_time_local,
                       done_at, created_at, updated_at
                FROM regulation_runs
                WHERE id = ?
                """,
                (run_id,),
            ).fetchone()
            # ensure task exists for run
            task = conn.execute(
                """
                SELECT id FROM tasks
                WHERE parent_type = 'regulation_run' AND parent_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (int(row["id"]),),
            ).fetchone()
            if not task:
                create_task(
                    title=str(reg["title"]),
                    status="NEW",
                    source_msg_id=None,
                    parent_type="regulation_run",
                    parent_id=int(row["id"]),
                )
            created.append(
                RegulationRun(
                    id=int(row["id"]),
                    regulation_id=int(row["regulation_id"]),
                    period_key=str(row["period_key"]),
                    status=str(row["status"]),
                    due_date=str(row["due_date"]),
                    due_time_local=row["due_time_local"],
                    done_at=row["done_at"],
                    created_at=str(row["created_at"]),
                    updated_at=str(row["updated_at"]),
                )
            )
    return created


def complete_regulation_run(run_id: int, done_at: str | None = None) -> RegulationRun:
    done_at_use = done_at if done_at is not None else _now_iso()
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, regulation_id, period_key, status, due_date, due_time_local,
                   done_at, created_at, updated_at
            FROM regulation_runs
            WHERE id = ?
            """,
            (int(run_id),),
        ).fetchone()
        if not row:
            raise ValueError("regulation_run not found")
        status = str(row["status"] or "")
        if status == "OPEN":
            conn.execute(
                """
                UPDATE regulation_runs
                SET status = 'DONE',
                    done_at = COALESCE(done_at, ?),
                    updated_at = ?
                WHERE id = ?
                """,
                (done_at_use, _now_iso(), int(run_id)),
            )
            conn.commit()
        row = conn.execute(
            """
            SELECT id, regulation_id, period_key, status, due_date, due_time_local,
                   done_at, created_at, updated_at
            FROM regulation_runs
            WHERE id = ?
            """,
            (int(run_id),),
        ).fetchone()
    return RegulationRun(
        id=int(row["id"]),
        regulation_id=int(row["regulation_id"]),
        period_key=str(row["period_key"]),
        status=str(row["status"]),
        due_date=str(row["due_date"]),
        due_time_local=row["due_time_local"],
        done_at=row["done_at"],
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def skip_regulation_run(run_id: int) -> RegulationRun:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, regulation_id, period_key, status, due_date, due_time_local,
                   done_at, created_at, updated_at
            FROM regulation_runs
            WHERE id = ?
            """,
            (int(run_id),),
        ).fetchone()
        if not row:
            raise ValueError("regulation_run not found")
        status = str(row["status"] or "")
        if status == "OPEN":
            conn.execute(
                """
                UPDATE regulation_runs
                SET status = 'SKIPPED',
                    updated_at = ?
                WHERE id = ?
                """,
                (_now_iso(), int(run_id)),
            )
            conn.commit()
        row = conn.execute(
            """
            SELECT id, regulation_id, period_key, status, due_date, due_time_local,
                   done_at, created_at, updated_at
            FROM regulation_runs
            WHERE id = ?
            """,
            (int(run_id),),
        ).fetchone()
    return RegulationRun(
        id=int(row["id"]),
        regulation_id=int(row["regulation_id"]),
        period_key=str(row["period_key"]),
        status=str(row["status"]),
        due_date=str(row["due_date"]),
        due_time_local=row["due_time_local"],
        done_at=row["done_at"],
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def disable_regulation(regulation_id: int) -> Regulation:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, title, note, status, day_of_month, due_time_local, source_msg_id
            FROM regulations
            WHERE id = ?
            """,
            (int(regulation_id),),
        ).fetchone()
        if not row:
            raise ValueError("regulation not found")
        if str(row["status"] or "") != "DISABLED":
            conn.execute(
                """
                UPDATE regulations
                SET status = 'DISABLED',
                    updated_at = ?
                WHERE id = ?
                """,
                (_now_iso(), int(regulation_id)),
            )
            conn.commit()
            row = conn.execute(
                """
                SELECT id, title, note, status, day_of_month, due_time_local, source_msg_id
                FROM regulations
                WHERE id = ?
                """,
                (int(regulation_id),),
            ).fetchone()
    return Regulation(
        id=int(row["id"]),
        title=str(row["title"]),
        note=row["note"],
        status=str(row["status"]),
        day_of_month=int(row["day_of_month"]),
        due_time_local=row["due_time_local"],
        source_msg_id=row["source_msg_id"],
    )
