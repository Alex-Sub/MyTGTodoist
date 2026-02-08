import csv
import io
import os
import sqlite3
import zipfile
from datetime import date, datetime, timezone, timedelta
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response

DB_PATH = os.getenv("DB_PATH", "/data/organizer.db")
P7_MODE = (os.getenv("P7_MODE", "off") or "off").strip().lower()
LOCAL_TZ_OFFSET_MIN = int(os.getenv("LOCAL_TZ_OFFSET_MIN", "180"))

app = FastAPI()


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _normalize_status(status: str | None) -> str | None:
    if not status:
        return None
    s = status.strip().upper()
    if s == "INBOX" or s == "TODO":
        return "NEW"
    return s


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    return dict(row)

def _p7_enabled() -> bool:
    return P7_MODE == "on"

def _require_p7() -> None:
    if not _p7_enabled():
        raise HTTPException(status_code=404, detail="not found")

def _local_day_bounds_utc(day: date) -> tuple[datetime, datetime]:
    tz = timezone(timedelta(minutes=LOCAL_TZ_OFFSET_MIN))
    start_local = datetime(day.year, day.month, day.day, tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

def _local_tz_label() -> str:
    offset_min = LOCAL_TZ_OFFSET_MIN
    sign = "+" if offset_min >= 0 else "-"
    total_min = abs(offset_min)
    hh = total_min // 60
    mm = total_min % 60
    return f"UTC{sign}{hh:02d}:{mm:02d}"


TASK_COLUMNS = [
    "id",
    "title",
    "status",
    "state",
    "planned_at",
    "calendar_event_id",
    "source_msg_id",
    "parent_type",
    "parent_id",
    "created_at",
    "updated_at",
    "completed_at",
]

SUBTASK_COLUMNS = [
    "id",
    "task_id",
    "title",
    "status",
    "source_msg_id",
    "created_at",
    "updated_at",
    "completed_at",
]

REGULATION_COLUMNS = [
    "id",
    "title",
    "note",
    "status",
    "day_of_month",
    "due_time_local",
    "source_msg_id",
    "created_at",
    "updated_at",
]

REGULATION_RUN_COLUMNS = [
    "id",
    "regulation_id",
    "period_key",
    "status",
    "due_date",
    "due_time_local",
    "done_at",
    "created_at",
    "updated_at",
]


def _parse_date_param(value: Any, name: str) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"{name} must be YYYY-MM-DD")


def _build_date_filter(
    column: str,
    from_date: date | None,
    to_date: date | None,
    params: list[Any],
) -> str:
    where = []
    if from_date:
        where.append(f"date({column}) >= ?")
        params.append(from_date.isoformat())
    if to_date:
        where.append(f"date({column}) <= ?")
        params.append(to_date.isoformat())
    return " AND ".join(where)


def _select_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
    from_date: date | None,
    to_date: date | None,
    extra_where: list[str] | None = None,
    extra_params: list[Any] | None = None,
) -> list[dict[str, Any]]:
    params: list[Any] = []
    where = []
    date_where = _build_date_filter("created_at", from_date, to_date, params)
    if date_where:
        where.append(date_where)
    if extra_where:
        where.extend(extra_where)
    if extra_params:
        params.extend(extra_params)
    where_sql = f" WHERE {' AND '.join(where)}" if where else ""
    col_sql = ", ".join(columns)
    rows = conn.execute(
        f"""
        SELECT {col_sql}
        FROM {table}
        {where_sql}
        ORDER BY id ASC
        """,
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def _rows_to_csv(columns: list[str], rows: list[dict[str, Any]]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=",", lineterminator="\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow([row.get(col) if row.get(col) is not None else "" for col in columns])
    return buf.getvalue()


def _export_zip(payloads: dict[str, str]) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in payloads.items():
            zf.writestr(name, content)
    return out.getvalue()


def _detect_overload(conn: sqlite3.Connection) -> bool:
    unplanned = conn.execute(
        """
        SELECT 1
        FROM tasks
        WHERE planned_at IS NULL
          AND (
            state = 'NEW'
            OR ((state IS NULL OR state = '') AND status = 'NEW')
          )
        LIMIT 1
        """
    ).fetchone()
    if not unplanned:
        return False
    commitments = conn.execute(
        """
        SELECT 1
        FROM tasks
        WHERE state IN ('IN_PROGRESS', 'PLANNED', 'SCHEDULED')
           OR ((state IS NULL OR state = '') AND status = 'IN_PROGRESS')
        LIMIT 1
        """
    ).fetchone()
    return commitments is not None


def _detect_drift(conn: sqlite3.Connection) -> bool:
    unlinked_tasks = conn.execute(
        """
        SELECT 1
        FROM tasks
        WHERE parent_type IS NULL
          AND (
            state IN ('NEW', 'IN_PROGRESS', 'PLANNED', 'SCHEDULED')
            OR ((state IS NULL OR state = '') AND status IN ('NEW', 'IN_PROGRESS'))
          )
        LIMIT 1
        """
    ).fetchone()
    if not unlinked_tasks:
        return False
    has_management = conn.execute(
        """
        SELECT 1
        FROM directions
        WHERE status = 'ACTIVE'
        UNION ALL
        SELECT 1
        FROM projects
        WHERE status = 'ACTIVE'
        UNION ALL
        SELECT 1
        FROM cycles
        WHERE status = 'OPEN'
        LIMIT 1
        """
    ).fetchone()
    return has_management is not None


def _p2_state_payload(user_id: str | None) -> dict[str, Any]:
    with _get_conn() as conn:
        overload_enabled = 0
        drift_enabled = 0
        if user_id:
            row = conn.execute(
                "SELECT overload_enabled, drift_enabled FROM user_settings WHERE user_id = ?",
                (str(user_id),),
            ).fetchone()
            if row:
                overload_enabled = int(row["overload_enabled"] or 0)
                drift_enabled = int(row["drift_enabled"] or 0)
        overload = _detect_overload(conn)
        drift = False if overload else _detect_drift(conn)
    payload: dict[str, Any] = {
        "execution": "ok",
        "overload": None,
        "drift": None,
    }
    if overload and overload_enabled:
        payload["overload"] = {"active": True, "signal": "OVERLOAD"}
    if drift and drift_enabled:
        payload["drift"] = {"active": True, "signal": "DRIFT"}
    return payload

@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True}


@app.get("/p2/state")
def get_p2_state(
    user_id: str | None = Query(default=None),
) -> dict[str, Any]:
    return _p2_state_payload(user_id)


@app.get("/p2/user_settings")
def get_user_settings(
    user_id: str | None = Query(default=None),
) -> dict[str, Any]:
    if not user_id:
        raise HTTPException(status_code=400, detail="user_id is required")
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT user_id, overload_enabled, drift_enabled FROM user_settings WHERE user_id = ?",
            (str(user_id),),
        ).fetchone()
    if not row:
        return {"user_id": str(user_id), "overload_enabled": 0, "drift_enabled": 0}
    return {
        "user_id": str(row["user_id"]),
        "overload_enabled": int(row["overload_enabled"] or 0),
        "drift_enabled": int(row["drift_enabled"] or 0),
    }


@app.get("/p2/user_nudges")
def get_user_nudges(
    user_id: str | None = Query(default=None),
    nudge_key: str | None = Query(default=None),
) -> dict[str, Any]:
    if not user_id or not nudge_key:
        raise HTTPException(status_code=400, detail="user_id and nudge_key are required")
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT user_id, nudge_key, next_at, last_shown_at
            FROM user_nudges
            WHERE user_id = ? AND nudge_key = ?
            """,
            (str(user_id), str(nudge_key)),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="nudge not found")
    return {
        "user_id": str(row["user_id"]),
        "nudge_key": str(row["nudge_key"]),
        "next_at": str(row["next_at"]),
        "last_shown_at": row["last_shown_at"],
    }

@app.get("/p2/tasks")
def list_tasks(
    status: str | None = Query(default=None),
    source_msg_id: str | None = Query(default=None),
) -> list[dict[str, Any]]:
    status_norm = _normalize_status(status)
    with _get_conn() as conn:
        where = []
        params: list[Any] = []
        if status_norm:
            where.append("status = ?")
            params.append(status_norm)
        if source_msg_id:
            where.append("source_msg_id = ?")
            params.append(source_msg_id)
        where_sql = " WHERE " + " AND ".join(where) if where else ""
        rows = conn.execute(
            f"""
            SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id,
                   parent_type, parent_id,
                   created_at, updated_at, completed_at
            FROM tasks
            {where_sql}
            ORDER BY id ASC
            """,
            params,
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/p2/tasks/{task_id}")
def get_task(task_id: int) -> dict[str, Any]:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id,
                   parent_type, parent_id,
                   created_at, updated_at, completed_at
            FROM tasks
            WHERE id = ?
            """,
            (int(task_id),),
        ).fetchone()
    data = _row_dict(row)
    if not data:
        raise HTTPException(status_code=404, detail="task not found")
    return data


@app.get("/p2/directions")
def list_directions() -> list[dict[str, Any]]:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, title, note, status, source_msg_id, created_at, updated_at
            FROM directions
            ORDER BY id ASC
            """
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/p2/directions/{direction_id}")
def get_direction(direction_id: int) -> dict[str, Any]:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, title, note, status, source_msg_id, created_at, updated_at
            FROM directions
            WHERE id = ?
            """,
            (int(direction_id),),
        ).fetchone()
    data = _row_dict(row)
    if not data:
        raise HTTPException(status_code=404, detail="direction not found")
    return data


@app.get("/p2/directions/{direction_id}/projects")
def list_direction_projects(direction_id: int) -> list[dict[str, Any]]:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, direction_id, title, status, source_msg_id, created_at, updated_at, closed_at
            FROM projects
            WHERE direction_id = ?
            ORDER BY id ASC
            """,
            (int(direction_id),),
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/p2/projects")
def list_projects(
    inbox: int | None = Query(default=None),
) -> list[dict[str, Any]]:
    with _get_conn() as conn:
        if inbox:
            rows = conn.execute(
                """
                SELECT p.id, p.direction_id, p.title, p.status, p.source_msg_id,
                       p.created_at, p.updated_at, p.closed_at
                FROM projects p
                WHERE p.status = 'ACTIVE'
                  AND NOT EXISTS (
                    SELECT 1
                    FROM tasks t
                    WHERE t.parent_type = 'project'
                      AND t.parent_id = p.id
                      AND (
                        t.state IN ('NEW', 'IN_PROGRESS', 'PLANNED', 'SCHEDULED')
                        OR ((t.state IS NULL OR t.state = '') AND t.status IN ('NEW', 'IN_PROGRESS'))
                      )
                  )
                ORDER BY p.id ASC
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, direction_id, title, status, source_msg_id, created_at, updated_at, closed_at
                FROM projects
                ORDER BY id ASC
                """
            ).fetchall()
    return [dict(r) for r in rows]


@app.get("/p2/projects/{project_id}")
def get_project(project_id: int) -> dict[str, Any]:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, direction_id, title, status, source_msg_id, created_at, updated_at, closed_at
            FROM projects
            WHERE id = ?
            """,
            (int(project_id),),
        ).fetchone()
    data = _row_dict(row)
    if not data:
        raise HTTPException(status_code=404, detail="project not found")
    return data


@app.get("/p2/projects/{project_id}/tasks")
def list_project_tasks(project_id: int) -> list[dict[str, Any]]:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id,
                   parent_type, parent_id,
                   created_at, updated_at, completed_at
            FROM tasks
            WHERE parent_type = 'project' AND parent_id = ?
            ORDER BY id ASC
            """,
            (int(project_id),),
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/p2/cycles")
def list_cycles() -> list[dict[str, Any]]:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, type, period_key, period_start, period_end, status, summary,
                   source_msg_id, created_at, updated_at, closed_at
            FROM cycles
            ORDER BY id ASC
            """
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/p2/cycles/{cycle_id}")
def get_cycle(cycle_id: int) -> dict[str, Any]:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, type, period_key, period_start, period_end, status, summary,
                   source_msg_id, created_at, updated_at, closed_at
            FROM cycles
            WHERE id = ?
            """,
            (int(cycle_id),),
        ).fetchone()
    data = _row_dict(row)
    if not data:
        raise HTTPException(status_code=404, detail="cycle not found")
    return data


@app.get("/p2/cycles/{cycle_id}/outcomes")
def list_cycle_outcomes(cycle_id: int) -> list[dict[str, Any]]:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, cycle_id, kind, text, created_at
            FROM cycle_outcomes
            WHERE cycle_id = ?
            ORDER BY id ASC
            """,
            (int(cycle_id),),
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/p2/cycles/{cycle_id}/tasks")
def list_cycle_tasks(cycle_id: int) -> list[dict[str, Any]]:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, title, status, state, planned_at, calendar_event_id, source_msg_id,
                   parent_type, parent_id,
                   created_at, updated_at, completed_at
            FROM tasks
            WHERE parent_type = 'cycle' AND parent_id = ?
            ORDER BY id ASC
            """,
            (int(cycle_id),),
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/p2/cycles/{cycle_id}/previous_goals")
def list_previous_cycle_goals(cycle_id: int) -> list[dict[str, Any]]:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT id, type FROM cycles WHERE id = ?",
            (int(cycle_id),),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="cycle not found")
        cycle_type = row["type"]
        prev = conn.execute(
            """
            SELECT id
            FROM cycles
            WHERE type = ?
              AND id < ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (cycle_type, int(cycle_id)),
        ).fetchone()
        if not prev:
            return []
        rows = conn.execute(
            """
            SELECT id, cycle_id, text, status, continued_from_goal_id, created_at, updated_at
            FROM cycle_goals
            WHERE cycle_id = ?
              AND status = 'ACTIVE'
            ORDER BY id ASC
            """,
            (int(prev["id"]),),
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/p2/cycles/{cycle_id}/goals")
def list_cycle_goals(cycle_id: int) -> list[dict[str, Any]]:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, cycle_id, text, status, continued_from_goal_id, created_at
            FROM cycle_goals
            WHERE cycle_id = ?
            ORDER BY id ASC
            """,
            (int(cycle_id),),
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/p2/tasks/{task_id}/subtasks")
def list_subtasks(task_id: int) -> list[dict[str, Any]]:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, task_id, title, status, source_msg_id,
                   created_at, updated_at, completed_at
            FROM subtasks
            WHERE task_id = ?
            ORDER BY id ASC
            """,
            (int(task_id),),
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/p2/subtasks")
def list_all_subtasks(
    source_msg_id: str | None = Query(default=None),
) -> list[dict[str, Any]]:
    with _get_conn() as conn:
        if source_msg_id:
            rows = conn.execute(
                """
                SELECT id, task_id, title, status, source_msg_id,
                       created_at, updated_at, completed_at
                FROM subtasks
                WHERE source_msg_id = ?
                ORDER BY id ASC
                """,
                (source_msg_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, task_id, title, status, source_msg_id,
                       created_at, updated_at, completed_at
                FROM subtasks
                ORDER BY id ASC
                """
            ).fetchall()
    return [dict(r) for r in rows]


@app.get("/p2/subtasks/{subtask_id}")
def get_subtask(subtask_id: int) -> dict[str, Any]:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, task_id, title, status, source_msg_id,
                   created_at, updated_at, completed_at
            FROM subtasks
            WHERE id = ?
            """,
            (int(subtask_id),),
        ).fetchone()
    data = _row_dict(row)
    if not data:
        raise HTTPException(status_code=404, detail="subtask not found")
    return data


@app.get("/export/json")
def export_json(
    from_: str | None = Query(default=None, alias="from"),
    to: str | None = Query(default=None),
) -> dict[str, Any]:
    from_date = _parse_date_param(from_, "from")
    to_date = _parse_date_param(to, "to")
    if from_date and to_date and from_date > to_date:
        raise HTTPException(status_code=400, detail="from must be <= to")

    with _get_conn() as conn:
        tasks = _select_rows(conn, "tasks", TASK_COLUMNS, from_date, to_date)
        task_ids = [t["id"] for t in tasks]
        if task_ids:
            id_placeholders = ", ".join(["?"] * len(task_ids))
            subtasks = _select_rows(
                conn,
                "subtasks",
                SUBTASK_COLUMNS,
                from_date,
                to_date,
                extra_where=[f"task_id IN ({id_placeholders})"],
                extra_params=task_ids,
            )
        else:
            subtasks = []
        regs = _select_rows(conn, "regulations", REGULATION_COLUMNS, from_date, to_date)
        runs = _select_rows(conn, "regulation_runs", REGULATION_RUN_COLUMNS, from_date, to_date)

    by_task: dict[int, list[dict[str, Any]]] = {}
    for sub in subtasks:
        task_id = int(sub["task_id"])
        by_task.setdefault(task_id, []).append(sub)
    for task in tasks:
        task["subtasks"] = by_task.get(int(task["id"]), [])

    return {
        "metadata": {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "timezone": "UTC",
        },
        "tasks": tasks,
        "regulations": regs,
        "regulation_runs": runs,
    }


@app.get("/export/csv")
def export_csv(
    from_: str | None = Query(default=None, alias="from"),
    to: str | None = Query(default=None),
) -> Response:
    from_date = _parse_date_param(from_, "from")
    to_date = _parse_date_param(to, "to")
    if from_date and to_date and from_date > to_date:
        raise HTTPException(status_code=400, detail="from must be <= to")

    with _get_conn() as conn:
        tasks = _select_rows(conn, "tasks", TASK_COLUMNS, from_date, to_date)
        subtasks = _select_rows(conn, "subtasks", SUBTASK_COLUMNS, from_date, to_date)
        regs = _select_rows(conn, "regulations", REGULATION_COLUMNS, from_date, to_date)
        runs = _select_rows(conn, "regulation_runs", REGULATION_RUN_COLUMNS, from_date, to_date)

    payloads = {
        "tasks.csv": _rows_to_csv(TASK_COLUMNS, tasks),
        "subtasks.csv": _rows_to_csv(SUBTASK_COLUMNS, subtasks),
        "regulations.csv": _rows_to_csv(REGULATION_COLUMNS, regs),
        "regulation_runs.csv": _rows_to_csv(REGULATION_RUN_COLUMNS, runs),
    }
    archive = _export_zip(payloads)
    headers = {"Content-Disposition": "attachment; filename=export.zip"}
    return Response(content=archive, media_type="application/zip", headers=headers)


@app.get("/p7/day")
def get_p7_day(
    date: str | None = Query(default=None),
) -> dict[str, Any]:
    _require_p7()
    if not date:
        raise HTTPException(status_code=400, detail="date is required")
    day = _parse_date_param(date, "date")
    if not day:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")
    start_utc, end_utc = _local_day_bounds_utc(day)
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, task_id, start_at, end_at, created_at
            FROM time_blocks
            WHERE start_at < ? AND end_at > ?
            ORDER BY start_at ASC, id ASC
            """,
            (end_utc.isoformat(), start_utc.isoformat()),
        ).fetchall()
    return {
        "date": day.isoformat(),
        "timezone": _local_tz_label(),
        "blocks": [dict(r) for r in rows],
    }


@app.get("/p4/regulations")
def list_regulations() -> list[dict[str, Any]]:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, title, note, status, day_of_month, due_time_local, source_msg_id,
                   created_at, updated_at
            FROM regulations
            ORDER BY id ASC
            """
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/p4/regulations/{regulation_id}")
def get_regulation(regulation_id: int) -> dict[str, Any]:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, title, note, status, day_of_month, due_time_local, source_msg_id,
                   created_at, updated_at
            FROM regulations
            WHERE id = ?
            """,
            (int(regulation_id),),
        ).fetchone()
    data = _row_dict(row)
    if not data:
        raise HTTPException(status_code=404, detail="regulation not found")
    return data


@app.get("/p4/runs")
def list_regulation_runs(
    period_key: str | None = Query(default=None),
) -> list[dict[str, Any]]:
    if not period_key:
        raise HTTPException(status_code=400, detail="period_key is required")
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, regulation_id, period_key, status, due_date, due_time_local,
                   done_at, created_at, updated_at
            FROM regulation_runs
            WHERE period_key = ?
            ORDER BY id ASC
            """,
            (str(period_key),),
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/p4/runs/{run_id}")
def get_regulation_run(run_id: int) -> dict[str, Any]:
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
    data = _row_dict(row)
    if not data:
        raise HTTPException(status_code=404, detail="regulation_run not found")
    return data


@app.get("/p4/regulations/{regulation_id}/runs")
def list_regulation_runs_for_reg(
    regulation_id: int,
    period: str | None = Query(default=None),
) -> list[dict[str, Any]]:
    if not period:
        raise HTTPException(status_code=400, detail="period is required")
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, regulation_id, period_key, status, due_date, due_time_local,
                   done_at, created_at, updated_at
            FROM regulation_runs
            WHERE regulation_id = ? AND period_key = ?
            ORDER BY id ASC
            """,
            (int(regulation_id), str(period)),
        ).fetchall()
    return [dict(r) for r in rows]
