from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from httpx import HTTPStatusError
from loguru import logger
from sqlalchemy import func, select, text

from src.config import settings
from src.db.models import CalendarSyncState, Conflict, Item, SyncOutbox
from src.db.session import get_session
from src.exports.vitrina_tasks import build_vitrina
from src.google.calendar_client import CalendarClient
from src.google.google_sync import (
    pull_google_tasks_with_conflicts,
    sync_task_completed,
    sync_task_created,
    sync_task_updated,
)
from src.google.review_http import start_review_http_server
from src.google.sheets_client import SheetsClient
from src.google.sheet_pull import pull_google_sheet_apply_rows
from src.google.sync_in import sync_in_calendar_window
from src.google.sync_out import sync_out_meeting
from src.google.runtime_sheets_sync import run_runtime_sheets_sync_scheduler

_lock = asyncio.Lock()
_tabs_ensured = False
_SYNC_STATE_KEY = "__global_sync_policy__"
_OUTBOX_BATCH_SIZE = 50
_OUTBOX_BASE_BACKOFF_SEC = 30
_last_calendar_pull_at: datetime | None = None
_last_tasks_pull_at: datetime | None = None
_last_sheets_pull_at: datetime | None = None
_last_calendar_pull_error: str | None = None
_last_tasks_pull_error: str | None = None
_last_sheets_pull_error: str | None = None
_last_vitrina_error: str | None = None
_last_sheets_push_sig: str | None = None
_SHEETS_TASKS_TAB = "Tasks"
_SHEETS_INBOX_TAB = "Inbox"
_SHEETS_CALENDAR_TAB = "Calendar"
_SHEETS_TASKS_INBOX_HEADER = ["ID", "Родитель", "Уровень", "Название", "Дата", "Время", "Статус", "Обновлено"]
_SHEETS_CALENDAR_HEADER = ["ID", "Название", "Заметки", "Дата", "Начало", "Конец", "Статус", "Обновлено"]
_GARBAGE_LIST_PREFIXES = ("список ", "покажи ", "выведи ", "дай мне список")
_SHEETS_TIMEZONE = ZoneInfo("Europe/Moscow")
_CALENDAR_PULL_STATE_KEY = "__calendar_pull_sync__"
_BIDIR_CALENDAR_STATE_SUFFIX = ":bidir"
_SYNC_STATUS_CANCELLED = {"cancelled", "canceled", "отменено", "cancel"}
_RUNTIME_SHEETS_MODES = {"runtime_sheets", "runtime_sheets_export", "runtime_sheets_read_model"}
_LEGACY_ITEMS_SHEETS_MODES = {"full_bidir", "sheets", "sheets_push", "sheets-only", "calendar_pull_and_sheets_push"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_dt(value: datetime | None) -> str:
    return value.isoformat() if value is not None else ""


def _dt_to_iso_utc(value: datetime | None) -> str:
    if value is None:
        return ""
    dt = value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value or "").strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _value_to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _dt_to_iso_utc(value)
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = _parse_iso_datetime(raw)
    if parsed is not None:
        return parsed.isoformat()
    return raw


def _as_local_datetime(value: Any) -> datetime | None:
    dt = _parse_iso_datetime(value)
    if dt is None:
        return None
    return dt.astimezone(_SHEETS_TIMEZONE)


def _local_date_from_iso(value: Any) -> str:
    dt = _as_local_datetime(value)
    return dt.strftime("%Y-%m-%d") if dt is not None else ""


def _local_time_from_iso(value: Any) -> str:
    dt = _as_local_datetime(value)
    return dt.strftime("%H:%M") if dt is not None else ""


def _local_datetime_label(value: Any) -> str:
    dt = _as_local_datetime(value)
    return dt.strftime("%Y-%m-%d %H:%M") if dt is not None else ""


def planned_at_from_item(row: dict[str, Any]) -> str | None:
    return _value_to_iso(row.get("start_at"))


def _start_at_from_item(row: dict[str, Any]) -> str | None:
    return _value_to_iso(row.get("start_at"))


def _calendar_event_id_from_item(row: dict[str, Any]) -> str:
    return str(row.get("calendar_event_id") or row.get("event_id") or "").strip()


def _end_at_from_item(row: dict[str, Any], start_at: str | None) -> str | None:
    explicit = _value_to_iso(row.get("end_at"))
    if explicit:
        return explicit
    start_dt = _parse_iso_datetime(start_at)
    if start_dt is None:
        return None
    duration_raw = row.get("duration_minutes")
    if duration_raw is None:
        duration_raw = row.get("duration_min")
    try:
        duration = int(duration_raw or 0)
    except Exception:
        duration = 0
    if duration <= 0:
        return None
    return (start_dt + timedelta(minutes=duration)).isoformat()


def _updated_at_iso_from_item(row: dict[str, Any]) -> str:
    updated = _value_to_iso(row.get("updated_at"))
    if updated:
        return updated
    created = _value_to_iso(row.get("created_at"))
    return created or ""


def _updated_at_from_item(row: dict[str, Any]) -> str:
    return _local_datetime_label(_updated_at_iso_from_item(row))


def _is_garbage_list_title(title: str) -> bool:
    normalized = " ".join(str(title or "").strip().lower().split())
    return any(normalized.startswith(prefix) for prefix in _GARBAGE_LIST_PREFIXES)


def _load_items_rows(session) -> list[dict[str, Any]]:
    rows = session.execute(text("SELECT * FROM items")).mappings().all()
    return [dict(row) for row in rows]


def _partition_sheets_rows(items_rows: list[dict[str, Any]]) -> tuple[list[list[Any]], list[list[Any]], list[list[Any]]]:
    tasks_raw: list[dict[str, Any]] = []
    inbox_raw: list[dict[str, Any]] = []
    calendar_raw: list[dict[str, Any]] = []

    for row in items_rows:
        item_id = str(row.get("id") or "").strip()
        if not item_id:
            continue
        item_type = str(row.get("type") or "").strip().lower()
        status = str(row.get("status") or "").strip().lower()
        title = str(row.get("title") or "").strip()
        if _is_garbage_list_title(title):
            continue
        parent_id = str(row.get("parent_id") or "").strip()
        level_raw = row.get("level")
        if level_raw is None:
            level_raw = row.get("depth")
        try:
            level = int(level_raw or 0)
        except Exception:
            level = 0
        updated_at = _updated_at_from_item(row)
        updated_sort = _updated_at_iso_from_item(row)
        start_at = _start_at_from_item(row) or ""
        calendar_event_id = _calendar_event_id_from_item(row)
        notes = str(row.get("notes") or row.get("description") or "").strip()

        if item_type == "task":
            planned = planned_at_from_item(row) or ""
            entry = {
                "id": item_id,
                "parent_id": parent_id,
                "level": level,
                "title": title,
                "start_at": start_at,
                "planned_at": planned,
                "date": _local_date_from_iso(planned),
                "time": _local_time_from_iso(planned),
                "status": status,
                "updated_sort": updated_sort,
                "updated_at": updated_at,
            }
            if status == "inbox":
                inbox_raw.append(entry)
            elif status != "archived":
                tasks_raw.append(entry)

        if item_type in {"timeblock", "meeting", "event"} or calendar_event_id:
            end_at = _end_at_from_item(row, start_at)
            calendar_raw.append(
                {
                    "id": item_id,
                    "title": title,
                    "notes": notes,
                    "start_at": start_at or "",
                    "end_at": end_at or "",
                    "date": _local_date_from_iso(start_at),
                    "start_time": _local_time_from_iso(start_at),
                    "end_time": _local_time_from_iso(end_at),
                    "status": status,
                    "updated_sort": updated_sort,
                    "updated_at": updated_at,
                }
            )

    def _task_sort_key(entry: dict[str, Any]) -> tuple[float, float, str]:
        start_dt = _parse_iso_datetime(entry.get("start_at"))
        updated_dt = _parse_iso_datetime(entry.get("updated_sort"))
        start_ord = start_dt.timestamp() if start_dt is not None else float("inf")
        updated_ord = -(updated_dt.timestamp() if updated_dt is not None else 0.0)
        return (start_ord, updated_ord, str(entry.get("id") or ""))

    def _inbox_sort_key(entry: dict[str, Any]) -> tuple[float, str]:
        updated_dt = _parse_iso_datetime(entry.get("updated_sort"))
        updated_ord = -(updated_dt.timestamp() if updated_dt is not None else 0.0)
        return (updated_ord, str(entry.get("id") or ""))

    def _calendar_sort_key(entry: dict[str, Any]) -> tuple[float, str]:
        start_dt = _parse_iso_datetime(entry.get("start_at"))
        start_ord = start_dt.timestamp() if start_dt is not None else float("inf")
        return (start_ord, str(entry.get("id") or ""))

    tasks_sorted = sorted(tasks_raw, key=_task_sort_key)
    inbox_sorted = sorted(inbox_raw, key=_inbox_sort_key)
    calendar_sorted = sorted(calendar_raw, key=_calendar_sort_key)

    tasks_rows: list[list[Any]] = []
    for entry in tasks_sorted:
        tasks_rows.append(
            [
                str(entry.get("id") or ""),
                str(entry.get("parent_id") or ""),
                int(entry.get("level") or 0),
                str(entry.get("title") or ""),
                str(entry.get("date") or ""),
                str(entry.get("time") or ""),
                str(entry.get("status") or ""),
                str(entry.get("updated_at") or ""),
            ]
        )

    inbox_rows: list[list[Any]] = []
    for entry in inbox_sorted:
        inbox_rows.append(
            [
                str(entry.get("id") or ""),
                str(entry.get("parent_id") or ""),
                int(entry.get("level") or 0),
                str(entry.get("title") or ""),
                str(entry.get("date") or ""),
                str(entry.get("time") or ""),
                str(entry.get("status") or ""),
                str(entry.get("updated_at") or ""),
            ]
        )

    calendar_rows: list[list[Any]] = []
    for entry in calendar_sorted:
        calendar_rows.append(
            [
                str(entry.get("id") or ""),
                str(entry.get("title") or ""),
                str(entry.get("notes") or ""),
                str(entry.get("date") or ""),
                str(entry.get("start_time") or ""),
                str(entry.get("end_time") or ""),
                str(entry.get("status") or ""),
                str(entry.get("updated_at") or ""),
            ]
        )
    return tasks_rows, inbox_rows, calendar_rows


def _build_sheets_push_rows(session) -> tuple[list[list[Any]], list[list[Any]], list[list[Any]]]:
    return _partition_sheets_rows(_load_items_rows(session))


def _sheets_push_signature(tasks_rows: list[list[Any]], inbox_rows: list[list[Any]], calendar_rows: list[list[Any]]) -> str:
    digest = hashlib.sha1(
        json.dumps([tasks_rows, inbox_rows, calendar_rows], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"tasks={len(tasks_rows)};inbox={len(inbox_rows)};calendar={len(calendar_rows)};sha1={digest}"


def _ensure_expected_db_has_items() -> None:
    db_path = os.getenv("SQLITE_PATH", "").strip() or settings.sqlite_path
    try:
        with get_session() as session:
            items_table_exists = bool(
                session.execute(
                    text(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='items' LIMIT 1"
                    )
                ).scalar()
            )
            if not items_table_exists:
                logger.error("wrong db file: table items is missing sqlite_path={}", db_path)
                raise SystemExit(1)

            has_items = int(session.execute(text("SELECT COUNT(1) FROM items")).scalar() or 0)
            if has_items <= 0:
                logger.error("wrong db file: has_items=0 sqlite_path={}", db_path)
                raise SystemExit(1)
    except SystemExit:
        raise
    except Exception as exc:
        logger.error("wrong db file: sanity_check_failed sqlite_path={} err={}", db_path, type(exc).__name__)
        raise SystemExit(1)


def _items_columns(session) -> set[str]:
    rows = session.execute(text("PRAGMA table_info(items)")).fetchall()
    return {str(row[1]) for row in rows if len(row) > 1}


def _collapse_spaces(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _normalize_notes(value: Any) -> str:
    text_value = str(value or "")
    text_value = text_value.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.strip() for line in text_value.split("\n")]
    return "\n".join(lines).strip()


def _normalize_status(value: Any) -> str:
    return _collapse_spaces(value).lower()


def _normalize_iso_utc(value: Any) -> str:
    dt = _parse_iso_datetime(value)
    if dt is None:
        return ""
    return dt.isoformat()


def _canonical_payload(
    *,
    title: Any,
    notes: Any,
    start_at: Any,
    end_at: Any,
    status: Any,
) -> dict[str, str]:
    return {
        "title": _collapse_spaces(title),
        "notes": _normalize_notes(notes),
        "start_at": _normalize_iso_utc(start_at),
        "end_at": _normalize_iso_utc(end_at),
        "status": _normalize_status(status),
    }


def _payload_hash(payload: dict[str, str] | None) -> str:
    if not payload:
        return ""
    return hashlib.sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _parse_sheet_date_time(date_raw: Any, time_raw: Any) -> str:
    date_text = str(date_raw or "").strip()
    time_text = str(time_raw or "").strip()
    if not date_text:
        return ""
    date_formats = ("%Y-%m-%d", "%d.%m.%Y")
    parsed_date: datetime | None = None
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_text, fmt)
            break
        except Exception:
            continue
    if parsed_date is None:
        return ""

    hour = 0
    minute = 0
    if time_text:
        try:
            time_parts = time_text.split(":", 1)
            hour = int(time_parts[0])
            minute = int(time_parts[1] if len(time_parts) > 1 else 0)
        except Exception:
            hour = 0
            minute = 0
    local_dt = datetime(parsed_date.year, parsed_date.month, parsed_date.day, hour, minute, tzinfo=_SHEETS_TIMEZONE)
    return local_dt.astimezone(timezone.utc).isoformat()


def _header_key(value: Any) -> str:
    return "".join(ch for ch in str(value or "").strip().lower() if ch not in {" ", "_", "-", ":"})


def _sheet_calendar_header_index(headers: list[Any]) -> dict[str, int]:
    idx = {_header_key(name): i for i, name in enumerate(headers)}
    aliases = {
        "id": {"id", "ид"},
        "title": {"название", "title"},
        "notes": {"заметки", "описание", "notes", "description"},
        "date": {"дата", "date"},
        "start": {"начало", "время", "start", "starttime", "времяначала"},
        "end": {"конец", "end", "endtime", "времяокончания"},
        "status": {"статус", "status"},
    }
    out: dict[str, int] = {}
    for logical, names in aliases.items():
        for name in names:
            key = _header_key(name)
            if key in idx:
                out[logical] = idx[key]
                break
    return out


def _sheet_cell(row: list[Any], col: int | None) -> Any:
    if col is None:
        return None
    if col < 0 or col >= len(row):
        return None
    return row[col]


def _read_sheet_calendar_snapshot(client: SheetsClient, spreadsheet_id: str) -> dict[str, dict[str, str]]:
    values = client.read_range(spreadsheet_id, f"'{_SHEETS_CALENDAR_TAB}'!A1:Z")
    if not values:
        return {}
    headers = list(values[0]) if values else []
    index = _sheet_calendar_header_index(headers)
    out: dict[str, dict[str, str]] = {}
    for row in values[1:]:
        item_id = str(_sheet_cell(row, index.get("id")) or "").strip()
        if not item_id:
            continue
        start_at = _parse_sheet_date_time(_sheet_cell(row, index.get("date")), _sheet_cell(row, index.get("start")))
        end_at = _parse_sheet_date_time(_sheet_cell(row, index.get("date")), _sheet_cell(row, index.get("end")))
        payload = _canonical_payload(
            title=_sheet_cell(row, index.get("title")),
            notes=_sheet_cell(row, index.get("notes")),
            start_at=start_at,
            end_at=end_at,
            status=_sheet_cell(row, index.get("status")),
        )
        out[item_id] = payload
    return out


def _sync_cols(cols: set[str]) -> dict[str, str | None]:
    def _pick(*names: str) -> str | None:
        for name in names:
            if name in cols:
                return name
        return None

    return {
        "title": _pick("title"),
        "notes": _pick("notes", "description"),
        "status": _pick("status"),
        "start": _pick("start_at", "scheduled_at", "due_at"),
        "end": _pick("end_at"),
        "duration": _pick("duration_minutes", "duration_min"),
        "event_id": _pick("calendar_event_id", "event_id"),
        "etag": _pick("etag"),
        "ical_uid": _pick("ical_uid"),
        "g_updated": _pick("g_updated"),
        "updated_at": _pick("updated_at"),
        "item_type": _pick("type"),
    }


def _db_payload_from_row(row: dict[str, Any], colmap: dict[str, str | None]) -> dict[str, str]:
    start_at = _value_to_iso(row.get(colmap["start"])) if colmap.get("start") else None
    end_at = _value_to_iso(row.get(colmap["end"])) if colmap.get("end") else None
    if not end_at and start_at and colmap.get("duration"):
        try:
            duration = int(row.get(colmap["duration"]) or 0)
        except Exception:
            duration = 0
        if duration > 0:
            start_dt = _parse_iso_datetime(start_at)
            if start_dt is not None:
                end_at = (start_dt + timedelta(minutes=duration)).isoformat()
    return _canonical_payload(
        title=row.get(colmap["title"]) if colmap.get("title") else "",
        notes=row.get(colmap["notes"]) if colmap.get("notes") else "",
        start_at=start_at,
        end_at=end_at,
        status=row.get(colmap["status"]) if colmap.get("status") else "",
    )


def _read_db_sync_snapshot(session) -> tuple[dict[str, dict[str, Any]], dict[str, str | None], set[str]]:
    cols = _items_columns(session)
    colmap = _sync_cols(cols)
    rows = session.execute(text("SELECT * FROM items")).mappings().all()
    out: dict[str, dict[str, Any]] = {}
    for mapped in rows:
        row = dict(mapped)
        item_id = str(row.get("id") or "").strip()
        if not item_id:
            continue
        event_id = str(row.get(colmap["event_id"]) or "").strip() if colmap.get("event_id") else ""
        item_type = str(row.get(colmap["item_type"]) or "").strip().lower() if colmap.get("item_type") else ""
        has_start = bool(row.get(colmap["start"])) if colmap.get("start") else False
        if not (event_id or item_type in {"meeting", "event", "timeblock"} or has_start):
            continue
        out[item_id] = {
            "payload": _db_payload_from_row(row, colmap),
            "row": row,
            "event_id": event_id,
            "etag": str(row.get(colmap["etag"]) or "").strip() if colmap.get("etag") else "",
            "ical_uid": str(row.get(colmap["ical_uid"]) or "").strip() if colmap.get("ical_uid") else "",
        }
    return out, colmap, cols


def _ensure_bidir_tables(session, *, cols: set[str]) -> None:
    if "notes" not in cols:
        try:
            session.execute(text("ALTER TABLE items ADD COLUMN notes TEXT"))
            cols.add("notes")
        except Exception:
            pass
    session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS calendar_sync_state (
              id TEXT PRIMARY KEY,
              calendar_id TEXT NOT NULL UNIQUE,
              sync_token TEXT NULL,
              channel_id TEXT NULL,
              resource_id TEXT NULL,
              expiration TEXT NULL,
              last_sync_status TEXT NULL,
              last_sync_error TEXT NULL,
              active_until TEXT NULL,
              created_at TEXT NULL,
              updated_at TEXT NULL
            )
            """
        )
    )
    session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS oauth_tokens (
              id TEXT PRIMARY KEY,
              provider TEXT NOT NULL UNIQUE,
              access_token TEXT NOT NULL,
              refresh_token TEXT NULL,
              token_type TEXT NOT NULL,
              expiry_ts INTEGER NOT NULL,
              scope TEXT NULL,
              created_at TEXT NULL,
              updated_at TEXT NULL
            )
            """
        )
    )
    session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS sync_state (
              item_id TEXT PRIMARY KEY,
              calendar_event_id TEXT NULL,
              last_db_hash TEXT NULL,
              last_sheet_hash TEXT NULL,
              last_calendar_hash TEXT NULL,
              last_sheet_seen_at TEXT NULL,
              last_calendar_seen_at TEXT NULL,
              last_db_seen_at TEXT NULL,
              last_conflict_at TEXT NULL
            )
            """
        )
    )
    session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS sync_outbox (
              id TEXT PRIMARY KEY,
              entity_type TEXT NOT NULL DEFAULT 'item',
              entity_id TEXT NOT NULL,
              operation TEXT NOT NULL DEFAULT 'upsert',
              payload_json TEXT NULL,
              attempts INTEGER NOT NULL DEFAULT 0,
              next_retry_at TEXT NULL,
              last_error TEXT NULL,
              processed_at TEXT NULL,
              created_at TEXT NULL,
              updated_at TEXT NULL
            )
            """
        )
    )
    session.execute(text("CREATE INDEX IF NOT EXISTS ix_sync_outbox_entity_id ON sync_outbox(entity_id)"))
    session.execute(text("CREATE INDEX IF NOT EXISTS ix_sync_outbox_processed_at ON sync_outbox(processed_at)"))
    session.execute(text("CREATE INDEX IF NOT EXISTS ix_sync_outbox_next_retry_at ON sync_outbox(next_retry_at)"))
    session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS sync_conflicts (
              id TEXT PRIMARY KEY,
              item_id TEXT NOT NULL,
              db_payload_json TEXT NULL,
              sheet_payload_json TEXT NULL,
              calendar_payload_json TEXT NULL,
              status TEXT NOT NULL DEFAULT 'open',
              resolution TEXT NULL,
              created_at TEXT NULL,
              resolved_at TEXT NULL
            )
            """
        )
    )
    session.execute(text("CREATE INDEX IF NOT EXISTS ix_sync_conflicts_item_id ON sync_conflicts(item_id)"))
    session.execute(text("CREATE INDEX IF NOT EXISTS ix_sync_conflicts_status ON sync_conflicts(status)"))


def _load_sync_state_map(session) -> dict[str, dict[str, Any]]:
    rows = session.execute(text("SELECT * FROM sync_state")).mappings().all()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        item_id = str(row.get("item_id") or "").strip()
        if not item_id:
            continue
        out[item_id] = dict(row)
    return out


def _upsert_sync_state(
    session,
    *,
    item_id: str,
    calendar_event_id: str,
    db_hash: str,
    sheet_hash: str,
    calendar_hash: str,
    now_iso: str,
    conflict_at_iso: str | None = None,
) -> None:
    session.execute(
        text(
            """
            INSERT INTO sync_state (
              item_id, calendar_event_id, last_db_hash, last_sheet_hash, last_calendar_hash,
              last_sheet_seen_at, last_calendar_seen_at, last_db_seen_at, last_conflict_at
            )
            VALUES (
              :item_id, :calendar_event_id, :last_db_hash, :last_sheet_hash, :last_calendar_hash,
              :last_sheet_seen_at, :last_calendar_seen_at, :last_db_seen_at, :last_conflict_at
            )
            ON CONFLICT(item_id) DO UPDATE SET
              calendar_event_id = excluded.calendar_event_id,
              last_db_hash = excluded.last_db_hash,
              last_sheet_hash = excluded.last_sheet_hash,
              last_calendar_hash = excluded.last_calendar_hash,
              last_sheet_seen_at = excluded.last_sheet_seen_at,
              last_calendar_seen_at = excluded.last_calendar_seen_at,
              last_db_seen_at = excluded.last_db_seen_at,
              last_conflict_at = COALESCE(excluded.last_conflict_at, sync_state.last_conflict_at)
            """
        ),
        {
            "item_id": item_id,
            "calendar_event_id": calendar_event_id or None,
            "last_db_hash": db_hash or None,
            "last_sheet_hash": sheet_hash or None,
            "last_calendar_hash": calendar_hash or None,
            "last_sheet_seen_at": now_iso,
            "last_calendar_seen_at": now_iso,
            "last_db_seen_at": now_iso,
            "last_conflict_at": conflict_at_iso,
        },
    )


def _create_sync_conflict(
    session,
    *,
    item_id: str,
    db_payload: dict[str, str] | None,
    sheet_payload: dict[str, str] | None,
    calendar_payload: dict[str, str] | None,
) -> str:
    existing = session.execute(
        text("SELECT id FROM sync_conflicts WHERE item_id = :item_id AND status = 'open' ORDER BY created_at DESC LIMIT 1"),
        {"item_id": item_id},
    ).scalar()
    if existing:
        return str(existing)
    conflict_id = str(uuid.uuid4())
    now_iso = _utc_now().isoformat()
    session.execute(
        text(
            """
            INSERT INTO sync_conflicts (
              id, item_id, db_payload_json, sheet_payload_json, calendar_payload_json, status, created_at
            )
            VALUES (:id, :item_id, :db_payload_json, :sheet_payload_json, :calendar_payload_json, 'open', :created_at)
            """
        ),
        {
            "id": conflict_id,
            "item_id": item_id,
            "db_payload_json": json.dumps(db_payload, ensure_ascii=False) if db_payload else None,
            "sheet_payload_json": json.dumps(sheet_payload, ensure_ascii=False) if sheet_payload else None,
            "calendar_payload_json": json.dumps(calendar_payload, ensure_ascii=False) if calendar_payload else None,
            "created_at": now_iso,
        },
    )
    return conflict_id


def _changed_sources(prev_state: dict[str, Any], *, db_hash: str, sheet_hash: str, calendar_hash: str) -> list[str]:
    out: list[str] = []
    prev_db = str(prev_state.get("last_db_hash") or "")
    prev_sheet = str(prev_state.get("last_sheet_hash") or "")
    prev_calendar = str(prev_state.get("last_calendar_hash") or "")
    if (db_hash or prev_db) and db_hash != prev_db:
        out.append("db")
    if (sheet_hash or prev_sheet) and sheet_hash != prev_sheet:
        out.append("sheet")
    if (calendar_hash or prev_calendar) and calendar_hash != prev_calendar:
        out.append("calendar")
    return out


def _row_matches_event(session, colmap: dict[str, str | None], *, event_id: str, ical_uid: str, title: str, start_iso: str) -> dict[str, Any] | None:
    event_col = colmap.get("event_id")
    start_col = colmap.get("start")
    if event_col:
        row = session.execute(
            text(f"SELECT * FROM items WHERE {event_col} = :event_id LIMIT 1"),
            {"event_id": event_id},
        ).mappings().first()
        if row:
            return dict(row)
        if ical_uid:
            row = session.execute(
                text(f"SELECT * FROM items WHERE {event_col} = :ical_uid LIMIT 1"),
                {"ical_uid": ical_uid},
            ).mappings().first()
            if row:
                return dict(row)
    if colmap.get("ical_uid"):
        row = session.execute(
            text(f"SELECT * FROM items WHERE {colmap['ical_uid']} = :event_id LIMIT 1"),
            {"event_id": event_id},
        ).mappings().first()
        if row:
            return dict(row)
        if ical_uid:
            row = session.execute(
                text(f"SELECT * FROM items WHERE {colmap['ical_uid']} = :ical_uid LIMIT 1"),
                {"ical_uid": ical_uid},
            ).mappings().first()
            if row:
                return dict(row)
    if start_col and title and start_iso:
        start_dt = _parse_iso_datetime(start_iso)
        if start_dt is not None:
            low = (start_dt - timedelta(minutes=5)).isoformat()
            high = (start_dt + timedelta(minutes=5)).isoformat()
            order_col = colmap.get("updated_at") or "id"
            row = session.execute(
                text(
                    f"""
                    SELECT * FROM items
                    WHERE lower(title) = lower(:title)
                      AND julianday({start_col}) BETWEEN julianday(:low) AND julianday(:high)
                    ORDER BY {order_col} DESC
                    LIMIT 1
                    """
                ),
                {"title": title, "low": low, "high": high},
            ).mappings().first()
            if row:
                return dict(row)
    return None


def _calendar_event_payload(event: dict[str, Any]) -> dict[str, str]:
    start_at = _event_dt(event.get("start"))
    end_at = _event_dt(event.get("end"))
    status = str(event.get("status") or "confirmed")
    if status.lower() == "cancelled":
        status = "cancelled"
    return _canonical_payload(
        title=event.get("summary"),
        notes=event.get("description"),
        start_at=start_at,
        end_at=end_at,
        status=status,
    )


def _fetch_calendar_delta_for_bidir(session, *, calendar_id: str, colmap: dict[str, str | None]) -> tuple[dict[str, dict[str, Any]], int]:
    state = _get_calendar_pull_state(session, f"{calendar_id}{_BIDIR_CALENDAR_STATE_SUFFIX}")
    client = CalendarClient()
    events: list[dict[str, Any]] = []

    def _poll(sync_token: str | None) -> str | None:
        page_token: str | None = None
        next_sync_token: str | None = None
        while True:
            response = client.list_events(
                calendar_id=calendar_id,
                sync_token=sync_token,
                time_min=None,
                time_max=None,
                page_token=page_token,
            )
            events.extend(list(response.get("items") or []))
            page_token = response.get("nextPageToken")
            if not page_token:
                next_sync_token = response.get("nextSyncToken")
                break
        return next_sync_token

    try:
        next_token = _poll(state.sync_token if state.sync_token else None)
        if next_token:
            state.sync_token = next_token
    except HTTPStatusError as exc:
        if int(getattr(exc.response, "status_code", 0)) != 410:
            raise
        state.sync_token = None
        next_token = _poll(None)
        if next_token:
            state.sync_token = next_token

    out: dict[str, dict[str, Any]] = {}
    for event in events:
        event_id = str(event.get("id") or "").strip()
        if not event_id:
            continue
        ical_uid = str(event.get("iCalUID") or "").strip()
        payload = _calendar_event_payload(event)
        row = _row_matches_event(
            session,
            colmap,
            event_id=event_id,
            ical_uid=ical_uid,
            title=payload.get("title") or "",
            start_iso=payload.get("start_at") or "",
        )
        if not row:
            continue
        item_id = str(row.get("id") or "").strip()
        if not item_id:
            continue
        out[item_id] = {
            "payload": payload,
            "event_id": event_id,
            "ical_uid": ical_uid,
        }
    return out, len(events)


def _apply_payload_to_db(session, *, item_id: str, payload: dict[str, str], colmap: dict[str, str | None]) -> bool:
    sets: list[str] = []
    params: dict[str, Any] = {"id": item_id}
    if colmap.get("title"):
        sets.append(f"{colmap['title']} = :title")
        params["title"] = payload.get("title") or ""
    if colmap.get("notes"):
        sets.append(f"{colmap['notes']} = :notes")
        params["notes"] = payload.get("notes") or ""
    if colmap.get("status") and payload.get("status"):
        sets.append(f"{colmap['status']} = :status")
        params["status"] = payload.get("status")
    if colmap.get("start"):
        sets.append(f"{colmap['start']} = :start_at")
        params["start_at"] = payload.get("start_at") or None
    if colmap.get("end"):
        sets.append(f"{colmap['end']} = :end_at")
        params["end_at"] = payload.get("end_at") or None
    elif colmap.get("duration"):
        start_dt = _parse_iso_datetime(payload.get("start_at"))
        end_dt = _parse_iso_datetime(payload.get("end_at"))
        if start_dt and end_dt and end_dt >= start_dt:
            duration = int((end_dt - start_dt).total_seconds() // 60)
            sets.append(f"{colmap['duration']} = :duration")
            params["duration"] = max(0, duration)
    if colmap.get("updated_at"):
        sets.append(f"{colmap['updated_at']} = :updated_at")
        params["updated_at"] = _utc_now().isoformat()
    if not sets:
        return False
    result = session.execute(text(f"UPDATE items SET {', '.join(sets)} WHERE id = :id"), params)
    return int(getattr(result, "rowcount", 0) or 0) > 0


def _to_local_iso(value: str) -> str:
    dt = _parse_iso_datetime(value)
    if dt is None:
        return value
    return dt.astimezone(ZoneInfo(settings.timezone)).isoformat()


def _build_google_event_payload(item_id: str, payload: dict[str, str]) -> dict[str, Any] | None:
    start_at = payload.get("start_at") or ""
    end_at = payload.get("end_at") or ""
    if not start_at or not end_at:
        return None
    return {
        "summary": payload.get("title") or "(без названия)",
        "description": payload.get("notes") or "",
        "start": {"dateTime": _to_local_iso(start_at), "timeZone": settings.timezone},
        "end": {"dateTime": _to_local_iso(end_at), "timeZone": settings.timezone},
        "extendedProperties": {"private": {"item_id": item_id}},
        "reminders": {"useDefault": True},
    }


def _push_payload_to_calendar(
    session,
    *,
    calendar_id: str,
    item_id: str,
    payload: dict[str, str],
    db_record: dict[str, Any],
    colmap: dict[str, str | None],
    client: CalendarClient,
) -> bool:
    event_id = str(db_record.get("event_id") or "").strip()
    status = _normalize_status(payload.get("status"))
    result: dict[str, Any] | None = None
    if status in _SYNC_STATUS_CANCELLED and event_id:
        result = client.cancel_event(calendar_id, event_id)
    else:
        google_payload = _build_google_event_payload(item_id, payload)
        if not google_payload:
            return False
        if event_id:
            result = client.update_event(calendar_id, event_id, google_payload, if_match_etag=db_record.get("etag") or None)
        else:
            result = client.create_event(calendar_id, google_payload)
    if not result:
        return False

    sets: list[str] = []
    params: dict[str, Any] = {"id": item_id}
    if colmap.get("event_id"):
        sets.append(f"{colmap['event_id']} = :event_id")
        params["event_id"] = str(result.get("event_id") or event_id or "")
    if colmap.get("etag"):
        sets.append(f"{colmap['etag']} = :etag")
        params["etag"] = str(result.get("etag") or db_record.get("etag") or "")
    if colmap.get("ical_uid"):
        sets.append(f"{colmap['ical_uid']} = :ical_uid")
        params["ical_uid"] = str(result.get("ical_uid") or db_record.get("ical_uid") or "")
    if colmap.get("g_updated"):
        sets.append(f"{colmap['g_updated']} = :g_updated")
        params["g_updated"] = str(result.get("updated") or "")
    if colmap.get("updated_at"):
        sets.append(f"{colmap['updated_at']} = :updated_at")
        params["updated_at"] = _utc_now().isoformat()
    if sets:
        session.execute(text(f"UPDATE items SET {', '.join(sets)} WHERE id = :id"), params)
    return True


def _notify_sync_conflict(
    conflict_id: str,
    item_id: str,
    changed_sources: list[str],
    *,
    db_payload: dict[str, str] | None = None,
    sheet_payload: dict[str, str] | None = None,
    calendar_payload: dict[str, str] | None = None,
) -> None:
    bot_token = str(settings.telegram_bot_token or "").strip()
    chat_id = str(os.getenv("ALERT_CHAT_ID", "") or "").strip()
    if not bot_token or not chat_id:
        return
    text_msg = (
        "Обнаружен конфликт синхронизации.\n"
        f"item_id={item_id}\n"
        f"conflict_id={conflict_id}\n"
        f"sources={','.join(changed_sources)}\n\n"
        f"Таблица: {json.dumps(sheet_payload or {}, ensure_ascii=False)[:220]}\n"
        f"Календарь: {json.dumps(calendar_payload or {}, ensure_ascii=False)[:220]}\n"
        f"База: {json.dumps(db_payload or {}, ensure_ascii=False)[:220]}\n\n"
        "Выберите версию:\n"
        "A) как в таблице\n"
        "B) как в календаре\n"
        "C) как в базе"
    )
    try:
        import httpx

        with httpx.Client(timeout=10.0) as client:
            client.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": text_msg,
                    "reply_markup": {
                        "inline_keyboard": [
                            [
                                {"text": "A: Таблица", "callback_data": f"syncconf:{conflict_id}:sheet"},
                                {"text": "B: Календарь", "callback_data": f"syncconf:{conflict_id}:calendar"},
                                {"text": "C: База", "callback_data": f"syncconf:{conflict_id}:db"},
                            ]
                        ]
                    },
                },
            ).raise_for_status()
    except Exception as exc:
        logger.warning("sync conflict notify failed err={}", type(exc).__name__)


def _run_full_bidir_sync_once() -> dict[str, int]:
    spreadsheet_id = (settings.google_sheets_spreadsheet_id or "").strip()
    calendar_id = _effective_calendar_id()
    stats = {"db_only": 0, "sheet_only": 0, "calendar_only": 0, "conflicts": 0, "calendar_fetched": 0, "calendar_pushed": 0}
    if not spreadsheet_id:
        return stats

    client = SheetsClient()
    calendar_client = CalendarClient()
    now_iso = _utc_now().isoformat()
    client.ensure_tabs(spreadsheet_id, [_SHEETS_TASKS_TAB, _SHEETS_INBOX_TAB, _SHEETS_CALENDAR_TAB])

    with get_session() as session:
        db_snapshot, colmap, cols = _read_db_sync_snapshot(session)
        _ensure_bidir_tables(session, cols=cols)
        sync_state = _load_sync_state_map(session)
        calendar_delta, fetched = _fetch_calendar_delta_for_bidir(session, calendar_id=calendar_id, colmap=colmap)
        stats["calendar_fetched"] = int(fetched)
        sheet_snapshot = _read_sheet_calendar_snapshot(client, spreadsheet_id)

        all_item_ids = set(db_snapshot.keys()) | set(sheet_snapshot.keys()) | set(calendar_delta.keys())
        for item_id in sorted(all_item_ids):
            db_record = db_snapshot.get(item_id)
            db_payload = dict((db_record or {}).get("payload") or {})
            sheet_payload = dict(sheet_snapshot.get(item_id) or {})
            calendar_payload = dict((calendar_delta.get(item_id) or {}).get("payload") or {})
            db_hash = _payload_hash(db_payload)
            sheet_hash = _payload_hash(sheet_payload)
            calendar_hash = _payload_hash(calendar_payload)
            prev = sync_state.get(item_id)
            event_id = str((db_record or {}).get("event_id") or (calendar_delta.get(item_id) or {}).get("event_id") or "").strip()

            if prev is None:
                base_hash = db_hash or sheet_hash or calendar_hash
                _upsert_sync_state(
                    session,
                    item_id=item_id,
                    calendar_event_id=event_id,
                    db_hash=base_hash,
                    sheet_hash=sheet_hash or base_hash,
                    calendar_hash=calendar_hash or base_hash,
                    now_iso=now_iso,
                )
                continue

            changed = _changed_sources(prev, db_hash=db_hash, sheet_hash=sheet_hash, calendar_hash=calendar_hash)
            if len(changed) >= 2:
                conflict_id = _create_sync_conflict(
                    session,
                    item_id=item_id,
                    db_payload=db_payload or None,
                    sheet_payload=sheet_payload or None,
                    calendar_payload=calendar_payload or None,
                )
                stats["conflicts"] += 1
                _upsert_sync_state(
                    session,
                    item_id=item_id,
                    calendar_event_id=event_id,
                    db_hash=db_hash,
                    sheet_hash=sheet_hash,
                    calendar_hash=calendar_hash,
                    now_iso=now_iso,
                    conflict_at_iso=now_iso,
                )
                _notify_sync_conflict(
                    conflict_id,
                    item_id,
                    changed,
                    db_payload=db_payload or None,
                    sheet_payload=sheet_payload or None,
                    calendar_payload=calendar_payload or None,
                )
                continue

            if changed == ["calendar"] and db_record:
                if _apply_payload_to_db(session, item_id=item_id, payload=calendar_payload, colmap=colmap):
                    db_snapshot[item_id]["payload"] = dict(calendar_payload)
                    db_hash = _payload_hash(calendar_payload)
                    stats["calendar_only"] += 1
            elif changed == ["sheet"] and db_record:
                if _apply_payload_to_db(session, item_id=item_id, payload=sheet_payload, colmap=colmap):
                    db_snapshot[item_id]["payload"] = dict(sheet_payload)
                    db_hash = _payload_hash(sheet_payload)
                    stats["sheet_only"] += 1
                    try:
                        if _push_payload_to_calendar(
                            session,
                            calendar_id=calendar_id,
                            item_id=item_id,
                            payload=sheet_payload,
                            db_record=db_snapshot[item_id],
                            colmap=colmap,
                            client=calendar_client,
                        ):
                            stats["calendar_pushed"] += 1
                    except Exception as exc:
                        logger.warning("CALENDAR push from sheet failed item_id={} err={}", item_id, type(exc).__name__)
            elif changed == ["db"] and db_record:
                stats["db_only"] += 1
                try:
                    if _push_payload_to_calendar(
                        session,
                        calendar_id=calendar_id,
                        item_id=item_id,
                        payload=db_payload,
                        db_record=db_record,
                        colmap=colmap,
                        client=calendar_client,
                    ):
                        stats["calendar_pushed"] += 1
                except Exception as exc:
                    logger.warning("CALENDAR push from db failed item_id={} err={}", item_id, type(exc).__name__)

            # After applying/pushing, keep hashes aligned with current DB as source of truth.
            current_payload = dict((db_snapshot.get(item_id) or {}).get("payload") or db_payload or sheet_payload or calendar_payload)
            current_hash = _payload_hash(current_payload)
            _upsert_sync_state(
                session,
                item_id=item_id,
                calendar_event_id=event_id,
                db_hash=current_hash,
                sheet_hash=current_hash,
                calendar_hash=current_hash,
                now_iso=now_iso,
            )

    return stats


def _calendar_pull_state_key(calendar_id: str) -> str:
    return f"{_CALENDAR_PULL_STATE_KEY}:{calendar_id}"


def _effective_calendar_id() -> str:
    raw = (os.getenv("GOOGLE_CALENDAR_ID", "") or "").strip()
    if raw:
        return raw
    fallback = (settings.google_calendar_id_default or "primary").strip()
    return fallback or "primary"


def _validate_calendar_pull_mode() -> None:
    sa_file = (os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "") or "").strip()
    if not sa_file:
        return
    calendar_id = (os.getenv("GOOGLE_CALENDAR_ID", "") or "").strip().lower()
    if calendar_id in {"", "primary"}:
        logger.error("service account requires explicit shared calendar id")
        raise SystemExit(1)


def _get_calendar_pull_state(session, calendar_id: str) -> CalendarSyncState:
    key = _calendar_pull_state_key(calendar_id)
    state = session.scalar(select(CalendarSyncState).where(CalendarSyncState.calendar_id == key))
    if state is not None:
        return state
    state = CalendarSyncState(calendar_id=key)
    session.add(state)
    session.flush()
    return state


def _event_dt(value: dict[str, Any] | None) -> datetime | None:
    part = value or {}
    raw = str(part.get("dateTime") or part.get("date") or "").strip()
    if not raw:
        return None
    if len(raw) == 10 and "T" not in raw:
        raw = f"{raw}T00:00:00+00:00"
    return _parse_iso_datetime(raw)


def _calendar_pull_sync_once(calendar_id: str) -> dict[str, int]:
    stats = {"fetched": 0, "updated": 0}
    client = CalendarClient()

    with get_session() as session:
        cols = _items_columns(session)
        lookup_col = "calendar_event_id" if "calendar_event_id" in cols else ("event_id" if "event_id" in cols else None)
        lookup_available = bool(lookup_col or ("ical_uid" in cols))
        start_col = "start_at" if "start_at" in cols else ("scheduled_at" if "scheduled_at" in cols else None)
        end_col = "end_at" if "end_at" in cols else None
        if (not lookup_available) or start_col is None:
            return stats

        state = _get_calendar_pull_state(session, calendar_id)
        now = _utc_now()
        def _fetch_candidate(where_sql: str, params: dict[str, Any]) -> dict[str, Any] | None:
            select_fields = ["id", "title", start_col, "updated_at"]
            if end_col:
                select_fields.append(end_col)
            q = text(f"SELECT {', '.join(select_fields)} FROM items WHERE {where_sql} ORDER BY updated_at DESC LIMIT 1")
            row = session.execute(q, params).mappings().first()
            return dict(row) if row is not None else None

        def _find_item_for_event(event_id: str, ical_uid: str, summary: str, start_iso: str) -> dict[str, Any] | None:
            # Priority 1: calendar_event_id == event.id
            if "calendar_event_id" in cols:
                candidate = _fetch_candidate("calendar_event_id = :value", {"value": event_id})
                if candidate is not None:
                    return candidate

            # Priority 2: calendar_event_id == event.iCalUID
            if ical_uid and "calendar_event_id" in cols:
                candidate = _fetch_candidate("calendar_event_id = :value", {"value": ical_uid})
                if candidate is not None:
                    return candidate

            # Compatibility: legacy columns (event_id/ical_uid)
            for col in ("event_id", "ical_uid"):
                if col in cols:
                    candidate = _fetch_candidate(f"{col} = :value", {"value": event_id})
                    if candidate is not None:
                        return candidate
            if ical_uid:
                for col in ("event_id", "ical_uid"):
                    if col in cols:
                        candidate = _fetch_candidate(f"{col} = :value", {"value": ical_uid})
                        if candidate is not None:
                            return candidate

            if summary and start_iso:
                start_dt = _parse_iso_datetime(start_iso)
                if start_dt is not None:
                    low = (start_dt - timedelta(minutes=5)).isoformat()
                    high = (start_dt + timedelta(minutes=5)).isoformat()
                    fallback = _fetch_candidate(
                        f"lower(title) = lower(:title) AND julianday({start_col}) BETWEEN julianday(:low) AND julianday(:high)",
                        {"title": summary, "low": low, "high": high},
                    )
                    if fallback is not None:
                        return fallback
            return None

        def _process_items(items: list[dict[str, Any]]) -> None:
            stats["fetched"] += len(items)
            for event in items:
                event_id = str(event.get("id") or "").strip()
                if not event_id:
                    continue
                if str(event.get("status") or "").strip().lower() == "cancelled":
                    continue
                summary = str(event.get("summary") or "").strip()
                ical_uid = str(event.get("iCalUID") or "").strip()
                start_dt = _event_dt(event.get("start"))
                end_dt = _event_dt(event.get("end"))
                if start_dt is None or end_dt is None:
                    continue
                start_iso = _dt_to_iso_utc(start_dt)
                end_iso = _dt_to_iso_utc(end_dt)
                row = _find_item_for_event(event_id, ical_uid, summary, start_iso)
                if not row:
                    continue

                current_start = str(row.get(start_col) or "")
                current_end = str(row.get(end_col) or "") if end_col else ""
                current_start_dt = _parse_iso_datetime(current_start)
                current_end_dt = _parse_iso_datetime(current_end) if end_col else None
                same_start = (current_start_dt == start_dt)
                same_end = (not end_col) or (current_end_dt == end_dt)
                if same_start and same_end:
                    continue

                sets: list[str] = []
                params: dict[str, Any] = {"id": row.get("id")}
                sets.append(f"{start_col} = :start_at")
                params["start_at"] = start_iso
                if end_col:
                    sets.append(f"{end_col} = :end_at")
                    params["end_at"] = end_iso
                elif "duration_min" in cols:
                    sets.append("duration_min = :duration_min")
                    params["duration_min"] = int((end_dt - start_dt).total_seconds() / 60)
                if "updated_at" in cols:
                    sets.append("updated_at = :updated_at")
                    params["updated_at"] = now.isoformat()
                if "calendar_event_id" in cols:
                    sets.append("calendar_event_id = :calendar_event_id")
                    params["calendar_event_id"] = event_id
                if "event_id" in cols:
                    sets.append("event_id = :event_id")
                    params["event_id"] = event_id
                if ical_uid and "ical_uid" in cols:
                    sets.append("ical_uid = :ical_uid")
                    params["ical_uid"] = ical_uid
                session.execute(text(f"UPDATE items SET {', '.join(sets)} WHERE id = :id"), params)
                stats["updated"] += 1
                logger.info(
                    "CALENDAR pull apply id={} start_at {} -> {} end_at {} -> {}",
                    row.get("id"),
                    current_start,
                    start_iso,
                    current_end,
                    end_iso,
                )

        def _poll(sync_token: str | None, *, time_min: str | None = None) -> str | None:
            page_token: str | None = None
            next_sync_token: str | None = None
            while True:
                response = client.list_events(
                    calendar_id=calendar_id,
                    sync_token=sync_token,
                    time_min=time_min,
                    time_max=None,
                    page_token=page_token,
                )
                _process_items(list(response.get("items") or []))
                page_token = response.get("nextPageToken")
                if not page_token:
                    next_sync_token = response.get("nextSyncToken")
                    break
            return next_sync_token

        try:
            next_token: str | None
            if state.sync_token:
                next_token = _poll(state.sync_token, time_min=None)
            else:
                next_token = _poll(None, time_min=None)
            if next_token:
                state.sync_token = next_token
        except HTTPStatusError as exc:
            if int(getattr(exc.response, "status_code", 0)) == 410:
                state.sync_token = None
                next_token = _poll(None, time_min=None)
                if next_token:
                    state.sync_token = next_token
            else:
                raise
    return stats


async def _run_calendar_pull_for_sheets() -> int:
    if not settings.sync_in_enabled:
        logger.info("CALENDAR pull no_changes")
        return 0
    calendar_id = _effective_calendar_id()
    try:
        logger.info("CALENDAR pull start calendar_id={}", calendar_id)
        async with _lock:
            stats = await asyncio.to_thread(_calendar_pull_sync_once, calendar_id)
        fetched = int((stats or {}).get("fetched") or 0)
        updated_count = int((stats or {}).get("updated") or 0)
        logger.info("CALENDAR pull fetched={} updated={}", fetched, updated_count)
        if updated_count <= 0:
            logger.info("CALENDAR pull no_changes")
        else:
            pass
        return updated_count
    except Exception as exc:
        logger.error("CALENDAR pull failed err={}", str(exc)[:300])
        return 0


async def _run_sheets_push_once(*, force: bool = False) -> int:
    global _last_sheets_push_sig

    spreadsheet_id = (settings.google_sheets_spreadsheet_id or "").strip()
    if not spreadsheet_id:
        logger.warning("SHEETS disabled spreadsheet_id=empty")
        return 0

    try:
        client = SheetsClient()
        await asyncio.to_thread(
            client.ensure_tabs,
            spreadsheet_id,
            [_SHEETS_TASKS_TAB, _SHEETS_INBOX_TAB, _SHEETS_CALENDAR_TAB],
        )

        with get_session() as session:
            tasks_rows, inbox_rows, calendar_rows = _build_sheets_push_rows(session)

        sig = _sheets_push_signature(tasks_rows, inbox_rows, calendar_rows)
        if not force and sig == _last_sheets_push_sig:
            return len(tasks_rows) + len(inbox_rows) + len(calendar_rows)

        await asyncio.to_thread(client.clear_sheet, spreadsheet_id, _SHEETS_TASKS_TAB)
        await asyncio.to_thread(
            client.write_table,
            spreadsheet_id,
            _SHEETS_TASKS_TAB,
            list(_SHEETS_TASKS_INBOX_HEADER),
            tasks_rows,
            value_input_option="USER_ENTERED",
        )
        await asyncio.to_thread(
            client.apply_datetime_format,
            spreadsheet_id,
            sheet_name=_SHEETS_TASKS_TAB,
            date_col=4,
            time_cols=[5],
            updated_col=7,
            date_col_width_px=95,
        )
        await asyncio.to_thread(client.clear_sheet, spreadsheet_id, _SHEETS_INBOX_TAB)
        await asyncio.to_thread(
            client.write_table,
            spreadsheet_id,
            _SHEETS_INBOX_TAB,
            list(_SHEETS_TASKS_INBOX_HEADER),
            inbox_rows,
            value_input_option="USER_ENTERED",
        )
        await asyncio.to_thread(
            client.apply_datetime_format,
            spreadsheet_id,
            sheet_name=_SHEETS_INBOX_TAB,
            date_col=4,
            time_cols=[5],
            updated_col=7,
            date_col_width_px=95,
        )
        await asyncio.to_thread(client.clear_sheet, spreadsheet_id, _SHEETS_CALENDAR_TAB)
        await asyncio.to_thread(
            client.write_table,
            spreadsheet_id,
            _SHEETS_CALENDAR_TAB,
            list(_SHEETS_CALENDAR_HEADER),
            calendar_rows,
            value_input_option="USER_ENTERED",
        )
        await asyncio.to_thread(
            client.apply_datetime_format,
            spreadsheet_id,
            sheet_name=_SHEETS_CALENDAR_TAB,
            date_col=3,
            time_cols=[4, 5],
            updated_col=7,
            date_col_width_px=95,
        )
        _last_sheets_push_sig = sig
        logger.info("SHEETS export tasks={} inbox={} calendar={}", len(tasks_rows), len(inbox_rows), len(calendar_rows))
        logger.info("SHEETS push ok")
        return len(tasks_rows) + len(inbox_rows) + len(calendar_rows)
    except Exception as exc:
        logger.error("SHEETS push failed err={}", str(exc)[:300])
        return 0


def _get_sync_state(session) -> CalendarSyncState:
    state = session.scalar(select(CalendarSyncState).where(CalendarSyncState.calendar_id == _SYNC_STATE_KEY))
    if state is not None:
        return state
    state = CalendarSyncState(calendar_id=_SYNC_STATE_KEY)
    session.add(state)
    session.flush()
    return state


def _current_poll_interval() -> int:
    now = _utc_now()
    try:
        with get_session() as session:
            state = _get_sync_state(session)
            active_until = state.active_until
            if active_until is not None:
                if active_until.tzinfo is None:
                    active_until = active_until.replace(tzinfo=timezone.utc)
                else:
                    active_until = active_until.astimezone(timezone.utc)
            if active_until is not None and now < active_until:
                return max(5, int(settings.sync_poll_active_sec))
    except Exception as exc:
        logger.warning("sync poll state unavailable, fallback active interval err={}", type(exc).__name__)
        return max(5, int(settings.sync_poll_active_sec))
    return max(30, int(settings.sync_poll_idle_sec))


async def run_calendar_scheduler() -> None:
    vitrina_interval = max(60, int(settings.google_vitrina_refresh_interval_sec))
    logger.info(
        "calendar sync scheduler started (active_window_min={} active_poll={}s idle_poll={}s vitrina={}s)",
        int(settings.sync_active_window_min),
        int(settings.sync_poll_active_sec),
        int(settings.sync_poll_idle_sec),
        vitrina_interval,
    )

    await _ensure_sheets_tabs_once()
    next_vitrina = 0.0
    loop = asyncio.get_running_loop()

    while True:
        now = loop.time()
        # Poll tick always checks pull sources.
        if settings.sync_in_enabled:
            await _run_pull()
        await _run_tasks_pull()
        if settings.google_sheets_spreadsheet_id:
            await _run_sheets_pull()
        if settings.sync_out_enabled:
            await _run_outbox_processor()

        if settings.google_sheets_spreadsheet_id and now >= next_vitrina:
            next_vitrina = now + vitrina_interval
            await _run_vitrina_refresh()
        interval = _current_poll_interval()
        logger.debug("sync_poll_sleep_sec={} mode={}", interval, "active" if interval == int(settings.sync_poll_active_sec) else "idle")
        await asyncio.sleep(interval)


async def run_sheets_push_scheduler(*, calendar_pull_enabled: bool, sheets_push_enabled: bool) -> None:
    spreadsheet_id = (settings.google_sheets_spreadsheet_id or "").strip()
    if sheets_push_enabled and not spreadsheet_id:
        logger.warning("SHEETS disabled spreadsheet_id=empty")
    elif sheets_push_enabled:
        logger.info("SHEETS enabled spreadsheet_id={}", spreadsheet_id)
    logger.info(
        "sync mode calendar_pull_enabled={} sheets_push_enabled={}",
        bool(calendar_pull_enabled),
        bool(sheets_push_enabled),
    )

    if calendar_pull_enabled:
        await _run_calendar_pull_for_sheets()

    if sheets_push_enabled:
        # Bootstrap export: fill table immediately on startup.
        await _run_sheets_push_once(force=True)

    while True:
        interval = _current_poll_interval()
        await asyncio.sleep(interval)
        if calendar_pull_enabled:
            await _run_calendar_pull_for_sheets()
        if sheets_push_enabled:
            await _run_sheets_push_once(force=False)


async def run_full_bidir_scheduler() -> None:
    spreadsheet_id = (settings.google_sheets_spreadsheet_id or "").strip()
    logger.info(
        "sync mode full_bidir spreadsheet_id_set={} calendar_id={}",
        bool(spreadsheet_id),
        _effective_calendar_id(),
    )
    # Bootstrap export so the sheet is always initialized.
    await _run_sheets_push_once(force=True)
    first_cycle = True
    while True:
        stats: dict[str, int] = {}
        try:
            async with _lock:
                stats = await asyncio.to_thread(_run_full_bidir_sync_once)
            logger.info(
                "BIDIR cycle db_only={} sheet_only={} calendar_only={} conflicts={} calendar_fetched={} calendar_pushed={}",
                int(stats.get("db_only") or 0),
                int(stats.get("sheet_only") or 0),
                int(stats.get("calendar_only") or 0),
                int(stats.get("conflicts") or 0),
                int(stats.get("calendar_fetched") or 0),
                int(stats.get("calendar_pushed") or 0),
            )
            logger.info(
                "CALENDAR pull fetched={} updated={}",
                int(stats.get("calendar_fetched") or 0),
                int(stats.get("calendar_only") or 0),
            )
        except Exception as exc:
            logger.error("BIDIR cycle failed err={}", str(exc)[:300])
            continue

        should_push = any(
            int(stats.get(key) or 0) > 0 for key in ("db_only", "sheet_only", "calendar_only", "calendar_pushed")
        )
        if should_push or first_cycle:
            await _run_sheets_push_once(force=first_cycle)
        first_cycle = False
        interval = _current_poll_interval()
        await asyncio.sleep(interval)


async def _run_pull() -> None:
    global _last_calendar_pull_at
    global _last_calendar_pull_error
    try:
        async with _lock:
            tz_name = settings.sync_timezone or settings.timezone
            tz = ZoneInfo(tz_name)
            window_start = datetime.now(tz).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            window_end = window_start + timedelta(
                days=max(1, int(settings.sync_window_days))
            )
            logger.info(
                "auto pull start window_start={} window_end={}",
                window_start.isoformat(),
                window_end.isoformat(),
            )
            with get_session() as session:
                stats = await asyncio.to_thread(
                    sync_in_calendar_window,
                    session,
                    settings.google_calendar_id_default,
                    window_start,
                    window_end,
                )
            if stats.get("token_reset") == 1:
                logger.warning("auto pull token_reset=1")
            logger.info("auto pull done stats={}", stats)
            _last_calendar_pull_at = _utc_now()
            _last_calendar_pull_error = None
    except Exception as exc:
        _last_calendar_pull_error = str(exc)[:300]
        logger.error("auto pull error: {}", exc)


def _outbox_backoff_sec(attempts: int) -> int:
    tries = max(1, int(attempts))
    value = int(_OUTBOX_BASE_BACKOFF_SEC * (2 ** (tries - 1)))
    cap = max(int(settings.sync_poll_idle_sec), _OUTBOX_BASE_BACKOFF_SEC)
    return min(value, cap)


def _process_outbox_item(session, row: SyncOutbox) -> None:
    item = session.get(Item, row.entity_id)
    if item is None:
        row.processed_at = _utc_now()
        row.last_error = "item_not_found"
        return
    if item.type == "meeting":
        sync_out_meeting(session, item.id)
        return
    if item.status == "done":
        sync_task_completed(session, item.id)
        return
    if item.google_task_id:
        sync_task_updated(session, item.id)
        return
    sync_task_created(session, item.id)


async def _run_outbox_processor() -> None:
    stats = {"processed": 0, "success": 0, "failed": 0}
    try:
        async with _lock:
            logger.info("outbox push start")
            now = _utc_now()
            with get_session() as session:
                rows = list(
                    session.scalars(
                        select(SyncOutbox).where(
                            SyncOutbox.processed_at.is_(None),
                            ((SyncOutbox.next_retry_at.is_(None)) | (SyncOutbox.next_retry_at <= now)),
                        ).order_by(SyncOutbox.created_at.asc()).limit(_OUTBOX_BATCH_SIZE)
                    ).all()
                )
                for row in rows:
                    stats["processed"] += 1
                    try:
                        _process_outbox_item(session, row)
                        row.processed_at = _utc_now()
                        row.last_error = None
                        row.next_retry_at = None
                        stats["success"] += 1
                    except Exception as exc:
                        row.attempts = int(row.attempts or 0) + 1
                        row.last_error = str(exc)[:500]
                        row.next_retry_at = _utc_now() + timedelta(seconds=_outbox_backoff_sec(int(row.attempts)))
                        row.payload_json = json.dumps(
                            {
                                "entity_type": row.entity_type,
                                "entity_id": row.entity_id,
                                "operation": row.operation,
                            },
                            ensure_ascii=False,
                        )
                        stats["failed"] += 1
            logger.info("outbox push done stats={}", stats)
    except Exception as exc:
        logger.error("outbox push error: {}", exc)


async def _run_tasks_pull() -> None:
    global _last_tasks_pull_at
    global _last_tasks_pull_error
    try:
        async with _lock:
            with get_session() as session:
                res = await asyncio.to_thread(pull_google_tasks_with_conflicts, session)
            stats = res.get("stats") if isinstance(res, dict) else None
            clarification = res.get("clarification") if isinstance(res, dict) else None
            logger.info(
                "auto tasks pull done stats={} open_conflicts={}",
                stats,
                bool(clarification),
            )
            _last_tasks_pull_at = _utc_now()
            _last_tasks_pull_error = None
    except Exception as exc:
        _last_tasks_pull_error = str(exc)[:300]
        logger.error("auto tasks pull error: {}", exc)


async def _run_sheets_pull() -> None:
    global _last_sheets_pull_at
    global _last_sheets_pull_error
    try:
        async with _lock:
            spreadsheet_id = (settings.google_sheets_spreadsheet_id or "").strip()
            if not spreadsheet_id:
                return
            client = SheetsClient()
            rows, headers_idx, sheet_name = await asyncio.to_thread(
                client.read_apply_rows,
                spreadsheet_id,
                settings.google_sheets_range,
            )
            if not rows:
                return
            with get_session() as session:
                stats, row_updates = await asyncio.to_thread(
                    pull_google_sheet_apply_rows,
                    session,
                    rows,
                )
            written = await asyncio.to_thread(
                client.write_status_updates,
                spreadsheet_id,
                sheet_name=sheet_name,
                headers_idx=headers_idx,
                row_updates=row_updates,
            )
            logger.info("auto sheets pull done stats={} writes={}", stats, written)
            _last_sheets_pull_at = _utc_now()
            _last_sheets_pull_error = None
    except Exception as exc:
        _last_sheets_pull_error = str(exc)[:300]
        logger.error("auto sheets pull error: {}", exc)


async def _ensure_sheets_tabs_once() -> None:
    global _tabs_ensured
    if _tabs_ensured:
        return
    spreadsheet_id = (settings.google_sheets_spreadsheet_id or "").strip()
    if not spreadsheet_id:
        _tabs_ensured = True
        return
    try:
        client = SheetsClient()
        await asyncio.to_thread(
            client.ensure_tabs,
            spreadsheet_id,
            [settings.google_vitrina_sheet_name, settings.google_ops_log_sheet_name],
        )
        _tabs_ensured = True
    except Exception as exc:
        logger.error("ensure tabs failed: {}", exc)


def _parse_ts(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    v = str(raw).strip().lower()
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off"}:
        return False
    return default


def _runtime_sheets_requested(mode: str) -> bool:
    if mode in _RUNTIME_SHEETS_MODES:
        return True
    return _env_bool("GOOGLE_SHEETS_SYNC_ENABLED", default=bool(settings.google_sheets_sync_enabled))


def _legacy_items_mode_allowed() -> bool:
    return _env_bool("GOOGLE_SYNC_ALLOW_LEGACY_ITEMS_MODE", default=False)


def _dispatch_main_mode() -> tuple[str, str]:
    mode = (os.getenv("GOOGLE_SYNC_MODE", "full") or "full").strip().lower()
    runtime_requested = _runtime_sheets_requested(mode)
    if runtime_requested:
        if mode in _LEGACY_ITEMS_SHEETS_MODES:
            logger.warning(
                "legacy items-based sheets mode skipped mode={} canonical=runtime_sheets reason=GOOGLE_SHEETS_SYNC_ENABLED_or_runtime_mode",
                mode,
            )
        logger.info("google sheets sync mode selected canonical=runtime_sheets requested_mode={}", mode)
        return ("runtime_sheets", mode)

    if mode in _LEGACY_ITEMS_SHEETS_MODES:
        if not _legacy_items_mode_allowed():
            logger.warning(
                "legacy items-based sheets mode disabled mode={} canonical=runtime_sheets env_flag=GOOGLE_SYNC_ALLOW_LEGACY_ITEMS_MODE",
                mode,
            )
            return ("legacy_items_blocked", mode)
        logger.warning("legacy items-based sheets mode enabled mode={} status=non_canonical", mode)
        return ("legacy_items_allowed", mode)

    return ("calendar_only", mode)


async def _prune_ops_log(client: SheetsClient, spreadsheet_id: str) -> int:
    retention_days = max(1, int(settings.google_ops_retention_days))
    cutoff = _utc_now() - timedelta(days=retention_days)
    sheet_name = settings.google_ops_log_sheet_name
    values = await asyncio.to_thread(client.read_range, spreadsheet_id, f"'{sheet_name}'!A4:Z")
    if not values:
        return 0
    kept: list[list[Any]] = []
    removed = 0
    for row in values:
        ts = _parse_ts(row[0] if row else "")
        if ts is None or ts >= cutoff:
            kept.append(row)
        else:
            removed += 1
    if removed <= 0:
        return 0
    await asyncio.to_thread(client.clear_range, spreadsheet_id, f"'{sheet_name}'!A4:Z")
    if kept:
        await asyncio.to_thread(client.write_range, spreadsheet_id, f"'{sheet_name}'!A4", kept)
    return removed


async def _run_vitrina_refresh() -> None:
    global _last_vitrina_error
    spreadsheet_id = (settings.google_sheets_spreadsheet_id or "").strip()
    if not spreadsheet_id:
        return
    try:
        async with _lock:
            client = SheetsClient()
            await asyncio.to_thread(
                client.ensure_tabs,
                spreadsheet_id,
                [settings.google_vitrina_sheet_name, settings.google_ops_log_sheet_name],
            )
            with get_session() as session:
                header, rows = build_vitrina(session)
                active_count = int(
                    session.scalar(
                        select(func.count()).where(
                            Item.type == "task",
                            Item.status.notin_(("done", "archived")),
                        )
                    )
                    or 0
                )
                open_conflicts = int(
                    session.scalar(select(func.count()).where(Conflict.status == "open")) or 0
                )
                sync_failed = int(
                    session.scalar(
                        select(func.count()).where(
                            Item.type == "task",
                            Item.google_sync_status == "failed",
                        )
                    )
                    or 0
                )
                sync_pending = int(
                    session.scalar(
                        select(func.count()).where(
                            Item.type == "task",
                            Item.google_sync_status == "pending",
                        )
                    )
                    or 0
                )

            await asyncio.to_thread(client.clear_sheet, spreadsheet_id, settings.google_vitrina_sheet_name)
            await asyncio.to_thread(
                client.write_table,
                spreadsheet_id,
                settings.google_vitrina_sheet_name,
                header,
                rows,
            )

            last_error = _last_vitrina_error or _last_sheets_pull_error or _last_tasks_pull_error or _last_calendar_pull_error or ""
            meta = {
                "now": _fmt_dt(_utc_now()),
                "active_count": active_count,
                "open_conflicts": open_conflicts,
                "sync_failed": sync_failed,
                "sync_pending": sync_pending,
                "last_tasks_pull_at": _fmt_dt(_last_tasks_pull_at),
                "last_sheets_pull_at": _fmt_dt(_last_sheets_pull_at),
                "last_calendar_pull_at": _fmt_dt(_last_calendar_pull_at),
            }
            ops_header = [
                "ts",
                "status",
                "vitrina_rows",
                "open_conflicts",
                "failed_sync",
                "calendar_pull_at",
                "tasks_pull_at",
                "sheets_pull_at",
                "last_error",
            ]
            ops_row = [
                _fmt_dt(_utc_now()),
                "",
                len(rows),
                open_conflicts,
                sync_failed,
                _fmt_dt(_last_calendar_pull_at),
                _fmt_dt(_last_tasks_pull_at),
                _fmt_dt(_last_sheets_pull_at),
                str(last_error or ""),
            ]
            await asyncio.to_thread(
                client.ops_log_upsert,
                spreadsheet_id,
                settings.google_ops_log_sheet_name,
                meta,
                ops_header,
                ops_row,
            )
            pruned = await _prune_ops_log(client, spreadsheet_id)
            logger.info("vitrina refresh done rows={} pruned_ops={}", len(rows), pruned)
            _last_vitrina_error = None
    except Exception as exc:
        _last_vitrina_error = str(exc)[:300]
        logger.error("vitrina refresh error: {}", exc)


def list_open_sync_conflicts(*, limit: int = 20) -> list[dict[str, Any]]:
    with get_session() as session:
        rows = session.execute(
            text(
                """
                SELECT id, item_id, db_payload_json, sheet_payload_json, calendar_payload_json, created_at
                FROM sync_conflicts
                WHERE status = 'open'
                ORDER BY created_at ASC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()
        return [dict(row) for row in rows]


def resolve_sync_conflict(*, conflict_id: str, choice: str) -> dict[str, Any]:
    choice_map = {"a": "sheet", "b": "calendar", "c": "db", "sheet": "sheet", "calendar": "calendar", "db": "db"}
    chosen = choice_map.get(str(choice or "").strip().lower())
    if not chosen:
        raise ValueError("choice must be one of A/B/C or sheet/calendar/db")

    with get_session() as session:
        cols = _items_columns(session)
        _ensure_bidir_tables(session, cols=cols)
        row = session.execute(
            text("SELECT * FROM sync_conflicts WHERE id = :id LIMIT 1"),
            {"id": conflict_id},
        ).mappings().first()
        if not row:
            raise ValueError("sync conflict not found")
        row_dict = dict(row)
        item_id = str(row_dict.get("item_id") or "").strip()
        if not item_id:
            raise ValueError("sync conflict has no item_id")
        if str(row_dict.get("status") or "").lower() != "open":
            return {"ok": True, "status": "already_resolved", "id": conflict_id}

        payload_raw = row_dict.get(f"{chosen}_payload_json")
        payload = json.loads(payload_raw) if payload_raw else {}
        if not isinstance(payload, dict):
            raise ValueError("invalid conflict payload")
        colmap = _sync_cols(cols)
        _apply_payload_to_db(session, item_id=item_id, payload={k: str(v or "") for k, v in payload.items()}, colmap=colmap)

        now_iso = _utc_now().isoformat()
        session.execute(
            text(
                """
                UPDATE sync_conflicts
                SET status = 'resolved', resolution = :resolution, resolved_at = :resolved_at
                WHERE id = :id
                """
            ),
            {"id": conflict_id, "resolution": chosen, "resolved_at": now_iso},
        )
        payload_hash = _payload_hash({k: str(v or "") for k, v in payload.items()})
        session.execute(
            text(
                """
                INSERT INTO sync_state (
                  item_id, last_db_hash, last_sheet_hash, last_calendar_hash, last_db_seen_at, last_sheet_seen_at, last_calendar_seen_at
                )
                VALUES (:item_id, :h, :h, :h, :t, :t, :t)
                ON CONFLICT(item_id) DO UPDATE SET
                  last_db_hash = excluded.last_db_hash,
                  last_sheet_hash = excluded.last_sheet_hash,
                  last_calendar_hash = excluded.last_calendar_hash,
                  last_db_seen_at = excluded.last_db_seen_at,
                  last_sheet_seen_at = excluded.last_sheet_seen_at,
                  last_calendar_seen_at = excluded.last_calendar_seen_at
                """
            ),
            {"item_id": item_id, "h": payload_hash, "t": now_iso},
        )
        return {"ok": True, "status": "resolved", "id": conflict_id, "choice": chosen, "item_id": item_id}


def main() -> None:
    start_review_http_server(port=int(settings.google_sync_http_port))
    selected, mode = _dispatch_main_mode()
    if selected == "runtime_sheets":
        asyncio.run(run_runtime_sheets_sync_scheduler())
        return
    if selected == "legacy_items_blocked":
        raise SystemExit(1)
    if selected != "legacy_items_allowed":
        asyncio.run(run_calendar_scheduler())
        return

    _ensure_expected_db_has_items()

    if mode == "full_bidir":
        _validate_calendar_pull_mode()
        asyncio.run(run_full_bidir_scheduler())
        return
    if mode in {"sheets", "sheets_push", "sheets-only", "calendar_pull_and_sheets_push"}:
        default_calendar_pull = mode == "calendar_pull_and_sheets_push"
        default_sheets_push = True
        calendar_pull_enabled = _env_bool("CALENDAR_PULL_ENABLED", default_calendar_pull)
        sheets_push_enabled = _env_bool("SHEETS_PUSH_ENABLED", default_sheets_push)
        if calendar_pull_enabled:
            _validate_calendar_pull_mode()
        asyncio.run(
            run_sheets_push_scheduler(
                calendar_pull_enabled=calendar_pull_enabled,
                sheets_push_enabled=sheets_push_enabled,
            )
        )
        return
    asyncio.run(run_calendar_scheduler())


if __name__ == "__main__":
    main()
