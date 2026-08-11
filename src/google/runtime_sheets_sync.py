from __future__ import annotations

import argparse
import asyncio
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from src.google.unified_calendar_export import collect_unified_calendar_rows

try:
    from loguru import logger
except Exception:  # pragma: no cover - local test fallback when deps are not installed
    import logging

    logger = logging.getLogger(__name__)

_TASKS_TAB = "Задачи"
_INBOX_TAB = "InBox"
_CALENDAR_TAB = "Календарь"
_LOGS_TAB = "Логи"
_TASKS_HEADER = [
    "№",
    "ID",
    "Уровень",
    "Родитель ID",
    "Родитель",
    "Задача",
    "Статус",
    "Состояние",
    "План",
    "Создано",
    "Обновлено",
    "Комментарий",
    "Кол-во блоков времени",
]
_INBOX_HEADER = [
    "№",
    "ID",
    "Уровень",
    "Родитель ID",
    "Родитель",
    "Задача",
    "Создано",
    "Источник",
    "Комментарий",
]
_CALENDAR_HEADER = [
    "Тип",
    "Название",
    "Дата",
    "День недели",
    "Начало",
    "Конец",
    "Время",
    "Длительность, мин",
    "Связанная задача",
    "Комментарий",
    "ID",
    "Calendar Event ID",
    "Источник",
]
_LOGS_HEADER = [
    "Время",
    "Событие",
    "Лист",
    "Строк",
    "Создано",
    "Обновлено",
    "Пропущено",
    "Ошибка",
]
_LOG_RETENTION_DAYS = 7
_LEGACY_TAB_RENAMES = {
    "Inbox": "Inbox_legacy_deprecated",
}
_FORMATTABLE_TABS = {_TASKS_TAB, _INBOX_TAB, _CALENDAR_TAB, _LOGS_TAB}
_CANONICAL_TABS = [_TASKS_TAB, _INBOX_TAB, _CALENDAR_TAB, _LOGS_TAB]
_SHEETS_EPOCH = datetime(1899, 12, 30, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class SyncStats:
    created: int = 0
    updated: int = 0
    skipped: int = 0
    stale: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "created": int(self.created),
            "updated": int(self.updated),
            "skipped": int(self.skipped),
            "stale": int(self.stale),
        }


def _db_path() -> str:
    settings_obj = _settings()
    default_path = getattr(settings_obj, "sqlite_path", "/data/organizer.db") if settings_obj is not None else "/data/organizer.db"
    return (os.getenv("SQLITE_PATH", "") or default_path or "/data/organizer.db").strip()


def _spreadsheet_id() -> str:
    settings_obj = _settings()
    default_id = getattr(settings_obj, "google_sheets_spreadsheet_id", "") if settings_obj is not None else ""
    return (os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "") or default_id or "").strip()


def _sync_interval_sec() -> int:
    raw = os.getenv("GOOGLE_SHEETS_SYNC_INTERVAL_SEC")
    settings_obj = _settings()
    default_interval = getattr(settings_obj, "google_sheets_sync_interval_sec", 300) if settings_obj is not None else 300
    if raw is None or not str(raw).strip():
        return max(30, int(default_interval))
    try:
        return max(30, int(str(raw).strip()))
    except Exception:
        return max(30, int(default_interval))


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def _settings():
    try:
        from src.config import settings as settings_obj

        return settings_obj
    except Exception:
        return None


def _connect(db_path: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or _db_path(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(str(row["name"] if isinstance(row, sqlite3.Row) else row[1]) == column for row in rows)


def _task_origin(source_msg_id: Any) -> str:
    return "telegram" if str(source_msg_id or "").strip() else "runtime"


def _task_priority(conn: sqlite3.Connection, task_id: int) -> str:
    if _table_has_column(conn, "tasks", "priority"):
        row = conn.execute("SELECT priority FROM tasks WHERE id = ?", (int(task_id),)).fetchone()
        return "" if row is None or row["priority"] is None else str(row["priority"])
    return ""


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _parse_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _format_datetime(value: Any) -> str:
    dt = _parse_datetime(value)
    if dt is None:
        return ""
    return dt.strftime("%d.%m.%Y %H:%M")


def _format_date(value: Any) -> str:
    dt = _parse_datetime(value)
    if dt is None:
        return ""
    return dt.strftime("%d.%m.%Y")


def _format_weekday_ru(value: Any) -> str:
    dt = _parse_datetime(value)
    if dt is None:
        return ""
    labels = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    return labels[dt.weekday()]


def _format_time_range(start_at: Any, end_at: Any) -> str:
    start_dt = _parse_datetime(start_at)
    end_dt = _parse_datetime(end_at)
    if start_dt is None:
        return ""
    if end_dt is None:
        return start_dt.strftime("%H:%M")
    return f"{start_dt.strftime('%H:%M')} - {end_dt.strftime('%H:%M')}"


def _iso_sort_ord(value: Any) -> float:
    dt = _parse_datetime(value)
    return 0.0 if dt is None else dt.timestamp()


def _task_tree_sort_key(row: sqlite3.Row | dict[str, Any]) -> tuple[float, int]:
    return (_iso_sort_ord(row["created_at"]), int(row["id"]))


def _sort_task_rows_hierarchical(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    normalized = [dict(row) for row in rows]
    by_id = {int(row["id"]): row for row in normalized}
    children_by_parent: dict[int, list[dict[str, Any]]] = {}
    roots: list[dict[str, Any]] = []

    for row in normalized:
        parent_task_id = _safe_int(row.get("parent_task_id"))
        if parent_task_id is not None and parent_task_id in by_id:
            children_by_parent.setdefault(parent_task_id, []).append(row)
        else:
            roots.append(row)

    ordered: list[dict[str, Any]] = []
    emitted: set[int] = set()

    def _emit_tree(row: dict[str, Any], stack: set[int]) -> None:
        row_id = int(row["id"])
        if row_id in emitted:
            return
        ordered.append(row)
        emitted.add(row_id)
        if row_id in stack:
            return
        next_stack = set(stack)
        next_stack.add(row_id)
        children = children_by_parent.get(row_id, [])
        for child in sorted(children, key=_task_tree_sort_key):
            _emit_tree(child, next_stack)

    for root in sorted(roots, key=_task_tree_sort_key):
        _emit_tree(root, set())

    orphans = [row for row in normalized if int(row["id"]) not in emitted]
    for row in sorted(orphans, key=_task_tree_sort_key):
        _emit_tree(row, set())
    return ordered


def _annotate_task_tree(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id = {int(row["id"]): row for row in rows}
    children_by_parent: dict[int, list[int]] = {}
    roots: list[int] = []

    for row in rows:
        row_id = int(row["id"])
        parent_task_id = _safe_int(row.get("parent_task_id"))
        if parent_task_id is not None and parent_task_id in by_id:
            children_by_parent.setdefault(parent_task_id, []).append(row_id)
        else:
            roots.append(row_id)

    level_by_id: dict[int, int] = {}
    number_by_id: dict[int, str] = {}

    def _assign(task_id: int, *, level: int, number: str, seen: set[int] | None = None) -> None:
        if seen is None:
            seen = set()
        if task_id in seen:
            return
        next_seen = set(seen)
        next_seen.add(task_id)
        level_by_id[task_id] = level
        number_by_id[task_id] = number
        for index, child_id in enumerate(children_by_parent.get(task_id, []), start=1):
            _assign(child_id, level=level + 1, number=f"{number}.{index}", seen=next_seen)

    for root_id in roots:
        _assign(root_id, level=1, number=str(root_id))

    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item_id = int(item["id"])
        item["tree_number"] = number_by_id.get(item_id, str(item_id))
        item["level"] = level_by_id.get(item_id, 1)
        item["depth"] = item["level"]
        out.append(item)
    return out


def _calendar_sort_key(row: sqlite3.Row) -> tuple[str, int]:
    return (str(row["start_at"] or ""), int(row["id"]))


def _is_inbox_task(row: sqlite3.Row) -> bool:
    state = str(row["state"] or "").strip().upper()
    status = str(row["status"] or "").strip().upper()
    if state in {"DONE", "ARCHIVED", "CANCELED", "CANCELLED"} or status in {"DONE", "ARCHIVED", "CANCELED", "CANCELLED"}:
        return False
    if str(row["planned_at"] or "").strip():
        return False
    if row["parent_task_id"] is not None and str(row["parent_task_id"]).strip():
        return False
    if row["parent_id"] is not None and str(row["parent_id"]).strip():
        return False
    try:
        linked_count = int(row["linked_timeblocks_count"] or 0)
    except Exception:
        linked_count = 0
    if linked_count > 0:
        return False
    try:
        child_count = int(row["child_tasks_count"] or 0)
    except Exception:
        child_count = 0
    if child_count > 0:
        return False
    return True


def _duration_minutes(start_at: Any, end_at: Any) -> str:
    start_dt = _parse_datetime(start_at)
    end_dt = _parse_datetime(end_at)
    if start_dt is None or end_dt is None:
        return ""
    minutes = int((end_dt - start_dt).total_seconds() // 60)
    return str(minutes) if minutes >= 0 else ""


def _read_task_projection_rows(conn: sqlite3.Connection, *, user_id: str | None = None) -> list[sqlite3.Row]:
    has_comment = _table_has_column(conn, "tasks", "comment")
    has_user_id = _table_has_column(conn, "time_blocks", "user_id")
    has_source_msg_id = _table_has_column(conn, "tasks", "source_msg_id")
    has_parent_task_id = _table_has_column(conn, "tasks", "parent_task_id")

    select_source_msg_id = "t.source_msg_id" if has_source_msg_id else "'' AS source_msg_id"
    select_parent_task_id = "t.parent_task_id" if has_parent_task_id else "NULL AS parent_task_id"
    join_parent = "LEFT JOIN tasks tp ON tp.id = t.parent_task_id" if has_parent_task_id else ""
    select_parent_title = "COALESCE(tp.title, '') AS parent_title" if has_parent_task_id else "'' AS parent_title"
    if user_id:
        if not has_user_id:
            raise RuntimeError("single-user sheets sync is not supported: time_blocks.user_id is missing")
        return conn.execute(
            f"""
            SELECT
                t.id, t.title, t.status, t.state, t.planned_at, t.calendar_event_id,
                t.parent_type, t.parent_id,
                {select_parent_task_id},
                {select_parent_title},
                {"t.comment" if has_comment else "'' AS comment"},
                t.created_at, t.updated_at, t.completed_at,
                {select_source_msg_id},
                (
                    SELECT COUNT(1)
                FROM time_blocks tb
                WHERE tb.task_id = t.id
            ) AS linked_timeblocks_count,
                (
                    SELECT COUNT(1)
                    FROM tasks tc
                    WHERE tc.parent_task_id = t.id
                ) AS child_tasks_count
            FROM tasks t
            {join_parent}
            WHERE t.id IN (
                SELECT DISTINCT tb.task_id
                FROM time_blocks tb
                WHERE tb.user_id = ?
            )
            """,
            (str(user_id),),
        ).fetchall()
    return conn.execute(
        f"""
        SELECT
            id, title, status, state, planned_at, calendar_event_id,
            parent_type, parent_id,
            {"parent_task_id" if has_parent_task_id else "NULL AS parent_task_id"},
            {("COALESCE((SELECT title FROM tasks tp WHERE tp.id = tasks.parent_task_id), '') AS parent_title") if has_parent_task_id else "'' AS parent_title"},
            {"comment" if has_comment else "'' AS comment"},
            created_at, updated_at, completed_at,
            {"source_msg_id" if has_source_msg_id else "'' AS source_msg_id"},
            (
                SELECT COUNT(1)
                FROM time_blocks tb
                WHERE tb.task_id = tasks.id
            ) AS linked_timeblocks_count,
            (
                SELECT COUNT(1)
                FROM tasks tc
                WHERE tc.parent_task_id = tasks.id
            ) AS child_tasks_count
        FROM tasks
        """
    ).fetchall()


def _read_tasks_rows(conn: sqlite3.Connection, *, user_id: str | None = None) -> list[list[Any]]:
    rows = _annotate_task_tree(_sort_task_rows_hierarchical(_read_task_projection_rows(conn, user_id=user_id)))
    out: list[list[Any]] = []
    for row in rows:
        if _is_inbox_task(row):
            continue
        task_id = int(row["id"])
        out.append(
            [
                str(row.get("tree_number") or task_id),
                str(task_id),
                str(row.get("level") or (2 if _safe_int(row.get("parent_task_id")) is not None else 1)),
                str(row.get("parent_task_id") or ""),
                str(row.get("parent_title") or ""),
                str(row["title"] or ""),
                str(row["status"] or ""),
                str(row["state"] or ""),
                _format_datetime(row["planned_at"]),
                _format_datetime(row["created_at"]),
                _format_datetime(row["updated_at"]),
                str(row["comment"] or ""),
                str(row["linked_timeblocks_count"] or 0),
            ]
        )
    return out


def _read_inbox_rows(conn: sqlite3.Connection, *, user_id: str | None = None) -> list[list[Any]]:
    rows = _annotate_task_tree(_sort_task_rows_hierarchical(_read_task_projection_rows(conn, user_id=user_id)))
    out: list[list[Any]] = []
    for row in rows:
        if not _is_inbox_task(row):
            continue
        out.append(
            [
                str(row.get("tree_number") or row["id"]),
                str(row["id"]),
                str(row.get("level") or (2 if _safe_int(row.get("parent_task_id")) is not None else 1)),
                str(row.get("parent_task_id") or ""),
                str(row.get("parent_title") or ""),
                str(row["title"] or ""),
                _format_datetime(row["created_at"]),
                _task_origin(row["source_msg_id"]),
                str(row["comment"] or ""),
            ]
        )
    return out


def _read_calendar_rows(
    conn: sqlite3.Connection,
    *,
    user_id: str | None = None,
    calendar_client: Any | None = None,
    include_external_calendar: bool = False,
) -> list[list[Any]]:
    if not include_external_calendar:
        has_comment = _table_has_column(conn, "time_blocks", "comment")
        has_user_id = _table_has_column(conn, "time_blocks", "user_id")
        where_sql = "WHERE tb.user_id = ?" if user_id and has_user_id else ""
        params: tuple[Any, ...] = (str(user_id),) if user_id and has_user_id else ()
        rows = conn.execute(
            f"""
            SELECT
                tb.id,
                tb.task_id,
                COALESCE(t.title, '') AS task_title,
                tb.start_at,
                tb.end_at,
                {"tb.comment" if has_comment else "'' AS comment"},
                COALESCE(t.calendar_event_id, '') AS calendar_event_id
            FROM time_blocks tb
            LEFT JOIN tasks t ON t.id = tb.task_id
            {where_sql}
            ORDER BY tb.start_at ASC, tb.id ASC
            """,
            params,
        ).fetchall()
        rows_sorted = sorted(rows, key=_calendar_sort_key)
        out: list[list[Any]] = []
        for row in rows_sorted:
            out.append(
                [
                    "Блок времени",
                    str(row["task_title"] or ""),
                    _format_date(row["start_at"]),
                    _format_weekday_ru(row["start_at"]),
                    _format_datetime(row["start_at"]),
                    _format_datetime(row["end_at"]),
                    _format_time_range(row["start_at"], row["end_at"]),
                    _duration_minutes(row["start_at"], row["end_at"]),
                    str(row["task_title"] or ""),
                    str(row["comment"] or ""),
                    f"timeblock:{str(row['id'])}",
                    str(row["calendar_event_id"] or ""),
                    "runtime_timeblock",
                ]
            )
        return out

    unified_rows = collect_unified_calendar_rows(
        conn,
        user_id=user_id,
        calendar_client=calendar_client,
    )
    return [row.to_sheet_row(format_datetime=_format_datetime) for row in unified_rows]


def build_runtime_sheet_payloads(
    conn: sqlite3.Connection,
    *,
    user_id: str | None = None,
    calendar_client: Any | None = None,
    include_external_calendar: bool = False,
) -> dict[str, tuple[list[str], list[list[Any]]]]:
    return {
        _TASKS_TAB: (_TASKS_HEADER, _read_tasks_rows(conn, user_id=user_id)),
        _INBOX_TAB: (_INBOX_HEADER, _read_inbox_rows(conn, user_id=user_id)),
        _CALENDAR_TAB: (
            _CALENDAR_HEADER,
            _read_calendar_rows(
                conn,
                user_id=user_id,
                calendar_client=calendar_client,
                include_external_calendar=include_external_calendar,
            ),
        ),
    }


def _normalize_row(row: list[Any], width: int) -> list[str]:
    values = ["" if value is None else str(value) for value in row[:width]]
    if len(values) < width:
        values.extend([""] * (width - len(values)))
    return values


def _datetime_to_sheets_serial(dt: datetime) -> float:
    normalized = dt.astimezone(timezone.utc) if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    return (normalized - _SHEETS_EPOCH).total_seconds() / 86400.0


def _parse_display_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for pattern in ("%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            return datetime.strptime(text, pattern).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _typed_cell_value(sheet_name: str, column_name: str, value: Any) -> Any:
    raw = "" if value is None else value
    text = str(raw).strip()
    if not text:
        return ""
    if sheet_name == _TASKS_TAB and column_name in {"План", "Создано", "Обновлено"}:
        dt = _parse_datetime(text) or _parse_display_datetime(text)
        return _datetime_to_sheets_serial(dt) if dt is not None else raw
    if sheet_name == _INBOX_TAB and column_name == "Создано":
        dt = _parse_datetime(text) or _parse_display_datetime(text)
        return _datetime_to_sheets_serial(dt) if dt is not None else raw
    if sheet_name == _CALENDAR_TAB and column_name == "Дата":
        dt = _parse_datetime(text) or _parse_display_datetime(text)
        if dt is not None:
            return _datetime_to_sheets_serial(dt.replace(hour=0, minute=0, second=0, microsecond=0))
        return raw
    if sheet_name == _CALENDAR_TAB and column_name in {"Начало", "Конец"}:
        dt = _parse_datetime(text) or _parse_display_datetime(text)
        return _datetime_to_sheets_serial(dt) if dt is not None else raw
    if sheet_name == _LOGS_TAB and column_name == "Время":
        dt = _parse_datetime(text) or _parse_display_datetime(text)
        return _datetime_to_sheets_serial(dt) if dt is not None else raw
    return raw


def _typed_rows_for_sheet(sheet_name: str, header: list[str], rows: list[list[Any]]) -> list[list[Any]]:
    typed_rows: list[list[Any]] = []
    width = len(header)
    for row in rows:
        padded = list(row[:width])
        if len(padded) < width:
            padded.extend([""] * (width - len(padded)))
        typed_rows.append([
            _typed_cell_value(sheet_name, header[col_index], padded[col_index])
            for col_index in range(width)
        ])
    return typed_rows


def _column_number_formats(sheet_name: str, header: list[str]) -> list[dict[str, Any]]:
    if sheet_name == _TASKS_TAB:
        format_map = {
            "План": ("DATE_TIME", "dd.mm.yyyy hh:mm"),
            "Создано": ("DATE_TIME", "dd.mm.yyyy hh:mm"),
            "Обновлено": ("DATE_TIME", "dd.mm.yyyy hh:mm"),
        }
    elif sheet_name == _INBOX_TAB:
        format_map = {"Создано": ("DATE_TIME", "dd.mm.yyyy hh:mm")}
    elif sheet_name == _CALENDAR_TAB:
        format_map = {
            "Дата": ("DATE", "dd.mm.yyyy"),
            "Начало": ("DATE_TIME", "dd.mm.yyyy hh:mm"),
            "Конец": ("DATE_TIME", "dd.mm.yyyy hh:mm"),
        }
    elif sheet_name == _LOGS_TAB:
        format_map = {"Время": ("DATE_TIME", "dd.mm.yyyy hh:mm")}
    else:
        format_map = {}
    return [
        {
            "startIndex": index,
            "endIndex": index + 1,
            "type": fmt[0],
            "pattern": fmt[1],
        }
        for index, name in enumerate(header)
        if str(name) in format_map
        for fmt in [format_map[str(name)]]
    ]


def sync_sheet_tab_by_entity_id(
    client: Any,
    spreadsheet_id: str,
    *,
    sheet_name: str,
    header: list[str],
    rows: list[list[Any]],
    dry_run: bool = False,
) -> SyncStats:
    try:
        entity_col_index = header.index("ID")
    except ValueError as exc:
        raise RuntimeError(f"sheet `{sheet_name}` header must contain `ID` column") from exc

    wanted_rows = [_normalize_row(row, len(header)) for row in rows]
    wanted_rows_typed = _typed_rows_for_sheet(sheet_name, header, rows)
    existing = client.read_range(spreadsheet_id, f"'{sheet_name}'!A1:Z")
    existing_header = _normalize_row(existing[0], len(header)) if existing else []
    existing_rows = existing[1:] if len(existing) > 1 else []
    existing_map: dict[str, tuple[int, list[str]]] = {}
    duplicate_existing_ids = 0

    for row_index, row in enumerate(existing_rows, start=2):
        normalized = _normalize_row(row, len(header))
        entity_id = normalized[entity_col_index].strip()
        if entity_id:
            if entity_id in existing_map:
                duplicate_existing_ids += 1
            existing_map[entity_id] = (row_index, normalized)

    create_rows: list[list[Any]] = []
    update_rows: list[tuple[int, list[Any]]] = []
    skipped = 0
    seen_ids: set[str] = set()

    for position, row in enumerate(wanted_rows):
        entity_id = row[entity_col_index].strip()
        if not entity_id:
            continue
        seen_ids.add(entity_id)
        existing_entry = existing_map.get(entity_id)
        if existing_entry is None:
            create_rows.append(wanted_rows_typed[position])
            continue
        existing_row_index, existing_row = existing_entry
        if existing_row == row:
            skipped += 1
            continue
        update_rows.append((existing_row_index, wanted_rows_typed[position]))

    stale = sum(1 for entity_id in existing_map if entity_id not in seen_ids)

    if dry_run:
        return SyncStats(created=len(create_rows), updated=len(update_rows), skipped=skipped, stale=stale)

    if not existing:
        client.write_table(spreadsheet_id, sheet_name, header, wanted_rows_typed)
        return SyncStats(created=len(wanted_rows), updated=0, skipped=0, stale=0)

    if existing_header != header:
        client.write_table(spreadsheet_id, sheet_name, header, wanted_rows_typed)
        return SyncStats(created=0, updated=len(wanted_rows), skipped=0, stale=stale)

    if stale > 0 or duplicate_existing_ids > 0:
        client.write_table(spreadsheet_id, sheet_name, header, wanted_rows_typed)
        return SyncStats(created=0, updated=len(wanted_rows), skipped=0, stale=stale)

    for row_index, row in update_rows:
        client.write_range(spreadsheet_id, f"'{sheet_name}'!A{row_index}", [row])

    if create_rows:
        client.append_rows(spreadsheet_id, f"'{sheet_name}'!A1", create_rows)

    return SyncStats(created=len(create_rows), updated=len(update_rows), skipped=skipped, stale=stale)


def _log_cutoff(now: datetime) -> datetime:
    return now - timedelta(days=_LOG_RETENTION_DAYS)


def _parse_log_row_time(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%d.%m.%Y %H:%M").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _bounded_log_rows(existing_rows: list[list[Any]], new_rows: list[list[Any]], *, now: datetime) -> list[list[str]]:
    cutoff = _log_cutoff(now)
    rows: list[list[str]] = []
    for row in existing_rows:
        normalized = _normalize_row(row, len(_LOGS_HEADER))
        parsed = _parse_log_row_time(normalized[0])
        if parsed is not None and parsed >= cutoff:
            rows.append(normalized)
    rows.extend(_normalize_row(row, len(_LOGS_HEADER)) for row in new_rows)
    rows.sort(key=lambda row: _parse_log_row_time(row[0]) or datetime.min.replace(tzinfo=timezone.utc))
    return rows


def _write_logs_sheet(
    client: Any,
    spreadsheet_id: str,
    *,
    dry_run: bool,
    rows: list[list[Any]],
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    existing = client.read_range(spreadsheet_id, f"'{_LOGS_TAB}'!A1:Z")
    existing_rows = existing[1:] if len(existing) > 1 else []
    bounded_rows = _bounded_log_rows(existing_rows, rows, now=now)
    bounded_rows_typed = _typed_rows_for_sheet(_LOGS_TAB, _LOGS_HEADER, bounded_rows)
    if not dry_run:
        client.write_table(spreadsheet_id, _LOGS_TAB, _LOGS_HEADER, bounded_rows_typed)
    return {
        "rows": len(bounded_rows),
        "appended": len(rows),
        "retained": len(bounded_rows) - len(rows),
    }


def _apply_canonical_tab_formatting(
    client: Any,
    spreadsheet_id: str,
    *,
    tabs: dict[str, int],
    tabs_meta: dict[str, list[str]],
) -> None:
    formatter = getattr(client, "apply_basic_tab_formatting", None)
    if not callable(formatter):
        return
    for sheet_name, column_count in tabs.items():
        if sheet_name not in _FORMATTABLE_TABS:
            continue
        try:
            formatter(
                spreadsheet_id,
                sheet_name=sheet_name,
                column_count=int(column_count),
                freeze_rows=1,
                enable_filter=True,
                auto_resize=True,
                number_formats=_column_number_formats(sheet_name, tabs_meta.get(sheet_name, [])),
            )
        except Exception as exc:
            logger.warning(
                f"runtime_sheets_sync formatting_failed tab={sheet_name} err={str(exc)[:300]}"
            )


def _prepare_runtime_tabs(client: Any, spreadsheet_id: str, sheet_names: list[str]) -> None:
    try:
        existing = client.list_tabs(spreadsheet_id)
    except Exception:
        existing = []
    existing_by_title = {str(row.get("title") or ""): row for row in existing}
    for legacy_title, renamed_title in _LEGACY_TAB_RENAMES.items():
        if legacy_title not in existing_by_title:
            continue
        if renamed_title in existing_by_title:
            continue
        canonical_conflict = any(str(name or "") == "InBox" for name in sheet_names) and legacy_title == "Inbox"
        if not canonical_conflict:
            continue
        if "InBox" in existing_by_title:
            continue
        sheet_id = existing_by_title[legacy_title].get("sheetId")
        if sheet_id is None:
            continue
        client.rename_sheet(spreadsheet_id, sheet_id=int(sheet_id), new_title=renamed_title)
        logger.warning(
            "runtime_sheets_sync renamed legacy_tab={} new_title={} reason=canonical_name_conflict",
            legacy_title,
            renamed_title,
        )


def cleanup_deprecated_tabs(
    *,
    spreadsheet_id: str | None = None,
    client: Any | None = None,
    delete_deprecated: bool = False,
) -> dict[str, Any]:
    spreadsheet = (spreadsheet_id or _spreadsheet_id()).strip()
    if not spreadsheet:
        raise RuntimeError("GOOGLE_SHEETS_SPREADSHEET_ID is required for Sheets tab cleanup")
    if client is None:
        from src.google.sheets_client import SheetsClient

        client = SheetsClient()
    found_tabs = list(client.list_tabs(spreadsheet))
    found_titles = [str(row.get("title") or "").strip() for row in found_tabs]
    missing_canonical = [name for name in _CANONICAL_TABS if name not in found_titles]
    if missing_canonical:
        raise RuntimeError(
            f"refusing cleanup: canonical sheets missing: {', '.join(missing_canonical)}"
        )
    delete_candidates = [
        row for row in found_tabs
        if str(row.get("title") or "").strip() not in _CANONICAL_TABS
    ]
    if len(_CANONICAL_TABS) <= 0:
        raise RuntimeError("refusing cleanup: canonical tab list is empty")
    result = {
        "ok": True,
        "spreadsheet_id": spreadsheet,
        "mode": "cleanup_delete" if delete_deprecated else "cleanup_preview",
        "found_sheets": found_titles,
        "kept_sheets": list(_CANONICAL_TABS),
        "deleted_sheets": [],
    }
    if not delete_deprecated:
        result["deleted_sheets"] = [str(row.get("title") or "").strip() for row in delete_candidates]
        return result
    if len(found_tabs) - len(delete_candidates) <= 0:
        raise RuntimeError("refusing cleanup: deletion would leave zero sheets")
    for row in delete_candidates:
        title = str(row.get("title") or "").strip()
        sheet_id = row.get("sheetId")
        if title in _CANONICAL_TABS:
            continue
        if sheet_id is None:
            raise RuntimeError(f"refusing cleanup: sheet `{title}` has no sheetId")
        client.delete_sheet(spreadsheet, sheet_id=int(sheet_id))
        result["deleted_sheets"].append(title)
    logger.info(
        f"runtime_sheets_sync cleanup found={len(found_titles)} kept={len(_CANONICAL_TABS)} deleted={len(result['deleted_sheets'])}"
    )
    return result


def run_runtime_sheets_sync_once(
    *,
    db_path: str | None = None,
    spreadsheet_id: str | None = None,
    dry_run: bool = False,
    user_id: str | None = None,
    client: Any | None = None,
    calendar_client: Any | None = None,
) -> dict[str, Any]:
    spreadsheet = (spreadsheet_id or _spreadsheet_id()).strip()
    if not spreadsheet:
        raise RuntimeError("GOOGLE_SHEETS_SPREADSHEET_ID is required for runtime Sheets sync")

    if client is None:
        from src.google.sheets_client import SheetsClient

        client = SheetsClient()
    result: dict[str, Any] = {
        "ok": True,
        "mode": "dry_run" if dry_run else "apply",
        "spreadsheet_id": spreadsheet,
        "db_path": db_path or _db_path(),
        "user_id": str(user_id or ""),
        "tabs": {},
    }

    with _connect(db_path) as conn:
        payloads = build_runtime_sheet_payloads(
            conn,
            user_id=user_id,
            calendar_client=calendar_client,
            include_external_calendar=True,
        )

    _prepare_runtime_tabs(client, spreadsheet, [*payloads.keys(), _LOGS_TAB])
    client.ensure_tabs(spreadsheet, [*payloads.keys(), _LOGS_TAB])
    total = SyncStats()
    log_timestamp = _format_datetime(datetime.now(timezone.utc).isoformat())
    log_rows: list[list[Any]] = []
    for sheet_name, (header, rows) in payloads.items():
        stats = sync_sheet_tab_by_entity_id(
            client,
            spreadsheet,
            sheet_name=sheet_name,
            header=header,
            rows=rows,
            dry_run=dry_run,
        )
        result["tabs"][sheet_name] = stats.to_dict() | {"rows": len(rows)}
        total = SyncStats(
            created=total.created + stats.created,
            updated=total.updated + stats.updated,
            skipped=total.skipped + stats.skipped,
            stale=total.stale + stats.stale,
        )
        log_rows.append(
            [
                log_timestamp,
                "runtime_sheets_sync",
                sheet_name,
                str(len(rows)),
                str(stats.created),
                str(stats.updated),
                str(stats.skipped),
                "",
            ]
        )
        logger.info(
            "runtime_sheets_sync tab={} user_id={} dry_run={} rows={} created={} updated={} skipped={} stale={}",
            sheet_name,
            str(user_id or ""),
            bool(dry_run),
            len(rows),
            stats.created,
            stats.updated,
            stats.skipped,
            stats.stale,
        )

    result["tabs"][_LOGS_TAB] = _write_logs_sheet(
        client,
        spreadsheet,
        dry_run=dry_run,
        rows=log_rows,
    )
    if not dry_run:
        _apply_canonical_tab_formatting(
            client,
            spreadsheet,
            tabs={
                **{sheet_name: len(header) for sheet_name, (header, _) in payloads.items()},
                _LOGS_TAB: len(_LOGS_HEADER),
            },
            tabs_meta={
                **{sheet_name: list(header) for sheet_name, (header, _) in payloads.items()},
                _LOGS_TAB: list(_LOGS_HEADER),
            },
        )
    result["totals"] = total.to_dict()
    logger.info(
        "runtime_sheets_sync done user_id={} dry_run={} created={} updated={} skipped={} stale={}",
        str(user_id or ""),
        bool(dry_run),
        total.created,
        total.updated,
        total.skipped,
        total.stale,
    )
    return result


async def run_runtime_sheets_sync_scheduler() -> None:
    spreadsheet = _spreadsheet_id()
    if not spreadsheet:
        logger.warning("runtime_sheets_sync disabled spreadsheet_id=empty")
        while True:
            await asyncio.sleep(_sync_interval_sec())
    settings_obj = _settings()
    dry_run = _env_bool(
        "GOOGLE_SHEETS_SYNC_DRY_RUN",
        default=bool(getattr(settings_obj, "google_sheets_sync_dry_run", False)) if settings_obj is not None else False,
    )
    user_id = (os.getenv("GOOGLE_SHEETS_SYNC_USER_ID", "") or "").strip() or None
    interval = _sync_interval_sec()
    logger.info(
        "runtime_sheets_sync scheduler started spreadsheet_id={} dry_run={} interval_sec={} user_id={}",
        spreadsheet,
        bool(dry_run),
        interval,
        str(user_id or ""),
    )
    while True:
        try:
            await asyncio.to_thread(
                run_runtime_sheets_sync_once,
                dry_run=dry_run,
                user_id=user_id,
            )
        except Exception as exc:
            logger.error("runtime_sheets_sync failed err={}", str(exc)[:300])
        await asyncio.sleep(interval)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="One-way runtime DB -> Google Sheets sync")
    parser.add_argument("--dry-run", action="store_true", help="calculate changes but do not write to Sheets")
    parser.add_argument("--user-id", default="", help="optional single-user export when runtime schema supports it")
    parser.add_argument("--db-path", default="", help="override runtime SQLite path")
    parser.add_argument("--spreadsheet-id", default="", help="override Google Sheets spreadsheet id")
    parser.add_argument("--cleanup-tabs", action="store_true", help="inspect workbook tabs and prepare deprecated-tab cleanup")
    parser.add_argument("--delete-deprecated", action="store_true", help="delete all non-canonical workbook tabs")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.cleanup_tabs or args.delete_deprecated:
        result = cleanup_deprecated_tabs(
            spreadsheet_id=args.spreadsheet_id or None,
            delete_deprecated=bool(args.delete_deprecated),
        )
        print(f"Found sheets: {', '.join(result['found_sheets'])}")
        print(f"Kept sheets: {', '.join(result['kept_sheets'])}")
        print(f"Deleted sheets: {', '.join(result['deleted_sheets']) if result['deleted_sheets'] else '-'}")
        return
    run_runtime_sheets_sync_once(
        db_path=args.db_path or None,
        spreadsheet_id=args.spreadsheet_id or None,
        dry_run=bool(args.dry_run),
        user_id=args.user_id or None,
    )


if __name__ == "__main__":
    main()
