from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    from loguru import logger
except Exception:  # pragma: no cover
    import logging

    logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class UnifiedCalendarRow:
    entity_id: str
    row_type: str
    title: str
    start_at: str
    end_at: str
    duration_min: str
    linked_task: str
    comment: str
    calendar_event_id: str
    source: str

    def to_sheet_row(self, *, format_datetime: Any) -> list[str]:
        start_label = format_datetime(self.start_at)
        end_label = format_datetime(self.end_at)
        start_dt = _parse_iso(self.start_at)
        return [
            self.row_type,
            self.title,
            _format_date(start_dt),
            _format_weekday_ru(start_dt),
            start_label,
            end_label,
            _format_time_range(start_dt, _parse_iso(self.end_at)),
            self.duration_min,
            self.linked_task,
            self.comment,
            self.entity_id,
            self.calendar_event_id,
            self.source,
        ]


def _settings():
    try:
        from src.config import settings as settings_obj

        return settings_obj
    except Exception:
        return None


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def _calendar_id() -> str:
    settings_obj = _settings()
    default_id = getattr(settings_obj, "google_calendar_id_default", "primary") if settings_obj is not None else "primary"
    return str(
        os.getenv("GOOGLE_CALENDAR_ID")
        or os.getenv("GOOGLE_CALENDAR_ID_DEFAULT")
        or default_id
        or "primary"
    ).strip() or "primary"


def _window_days() -> tuple[int, int]:
    settings_obj = _settings()
    past_default = getattr(settings_obj, "google_sheets_calendar_past_days", 30) if settings_obj is not None else 30
    future_default = getattr(settings_obj, "google_sheets_calendar_future_days", 60) if settings_obj is not None else 60
    return (
        max(0, _env_int("GOOGLE_SHEETS_CALENDAR_PAST_DAYS", int(past_default))),
        max(1, _env_int("GOOGLE_SHEETS_CALENDAR_FUTURE_DAYS", int(future_default))),
    )


def _parse_iso(value: Any) -> datetime | None:
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


def _parse_google_event_bounds(event: dict[str, Any]) -> tuple[str, str]:
    start_payload = (event.get("start") or {}) if isinstance(event.get("start"), dict) else {}
    end_payload = (event.get("end") or {}) if isinstance(event.get("end"), dict) else {}
    start_dt = str(start_payload.get("dateTime") or "").strip()
    end_dt = str(end_payload.get("dateTime") or "").strip()
    if start_dt and end_dt:
        return start_dt, end_dt
    start_date = str(start_payload.get("date") or "").strip()
    end_date = str(end_payload.get("date") or "").strip()
    if start_date:
        start_dt = f"{start_date}T00:00:00+00:00"
    if end_date:
        end_dt = f"{end_date}T00:00:00+00:00"
    return start_dt, end_dt


def _duration_minutes(start_at: str, end_at: str) -> str:
    start_dt = _parse_iso(start_at)
    end_dt = _parse_iso(end_at)
    if start_dt is None or end_dt is None:
        return ""
    minutes = int((end_dt - start_dt).total_seconds() // 60)
    return str(minutes) if minutes >= 0 else ""


def _format_date(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.strftime("%d.%m.%Y")


def _format_weekday_ru(dt: datetime | None) -> str:
    if dt is None:
        return ""
    labels = [
        "Пн",
        "Вт",
        "Ср",
        "Чт",
        "Пт",
        "Сб",
        "Вс",
    ]
    return labels[dt.weekday()]


def _format_time_range(start_dt: datetime | None, end_dt: datetime | None) -> str:
    if start_dt is None:
        return ""
    if end_dt is None:
        return start_dt.strftime("%H:%M")
    return f"{start_dt.strftime('%H:%M')} - {end_dt.strftime('%H:%M')}"


def _window_bounds_utc() -> tuple[datetime, datetime]:
    past_days, future_days = _window_days()
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=past_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = (now + timedelta(days=future_days)).replace(hour=23, minute=59, second=59, microsecond=0)
    return start, end


def _within_window(start_at: str, end_at: str, *, window_start: datetime, window_end: datetime) -> bool:
    start_dt = _parse_iso(start_at)
    end_dt = _parse_iso(end_at)
    if start_dt is None:
        return False
    effective_end = end_dt or start_dt
    return start_dt <= window_end and effective_end >= window_start


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        LIMIT 1
        """,
        (table,),
    ).fetchone()
    return row is not None


def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(str(row["name"] if isinstance(row, sqlite3.Row) else row[1]) == column for row in rows)


def _read_runtime_timeblocks(conn: sqlite3.Connection, *, user_id: str | None, window_start: datetime, window_end: datetime) -> tuple[list[UnifiedCalendarRow], set[str]]:
    has_comment = _table_has_column(conn, "time_blocks", "comment")
    has_user_id = _table_has_column(conn, "time_blocks", "user_id")
    where_parts = [
        "tb.start_at < ?",
        "tb.end_at > ?",
    ]
    params: list[Any] = [window_end.isoformat(), window_start.isoformat()]
    if user_id and has_user_id:
        where_parts.append("tb.user_id = ?")
        params.append(str(user_id))
    rows = conn.execute(
        f"""
        SELECT
            tb.id,
            tb.task_id,
            tb.start_at,
            tb.end_at,
            {"tb.comment" if has_comment else "'' AS comment"},
            COALESCE(t.title, '') AS task_title,
            COALESCE(t.calendar_event_id, '') AS calendar_event_id
        FROM time_blocks tb
        LEFT JOIN tasks t ON t.id = tb.task_id
        WHERE {' AND '.join(where_parts)}
        ORDER BY tb.start_at ASC, tb.id ASC
        """,
        tuple(params),
    ).fetchall()
    out: list[UnifiedCalendarRow] = []
    claimed_event_ids: set[str] = set()
    for row in rows:
        event_id = str(row["calendar_event_id"] or "").strip()
        if event_id:
            claimed_event_ids.add(event_id)
        out.append(
            UnifiedCalendarRow(
                entity_id=f"timeblock:{int(row['id'])}",
                row_type="Блок времени",
                title=str(row["task_title"] or ""),
                start_at=str(row["start_at"] or ""),
                end_at=str(row["end_at"] or ""),
                duration_min=_duration_minutes(str(row["start_at"] or ""), str(row["end_at"] or "")),
                linked_task=str(row["task_title"] or ""),
                comment=str(row["comment"] or ""),
                calendar_event_id=event_id,
                source="runtime_timeblock",
            )
        )
    return out, claimed_event_ids


def _read_runtime_known_meetings(conn: sqlite3.Connection, *, window_start: datetime, window_end: datetime) -> dict[str, dict[str, str]]:
    if not _table_exists(conn, "items"):
        return {}
    columns = {
        row["name"] if isinstance(row, sqlite3.Row) else row[1]
        for row in conn.execute("PRAGMA table_info(items)").fetchall()
    }
    if "type" not in columns or "event_id" not in columns:
        return {}
    select_title = "title" if "title" in columns else "'' AS title"
    select_description = "description" if "description" in columns else "'' AS description"
    select_scheduled_at = "scheduled_at" if "scheduled_at" in columns else "NULL AS scheduled_at"
    select_duration_min = "duration_min" if "duration_min" in columns else "NULL AS duration_min"
    rows = conn.execute(
        f"""
        SELECT
            id,
            {select_title},
            {select_description},
            {select_scheduled_at},
            {select_duration_min},
            event_id,
            status
        FROM items
        WHERE type = 'meeting'
          AND event_id IS NOT NULL
          AND event_id != ''
        """
    ).fetchall()
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        event_id = str(row["event_id"] or "").strip()
        if not event_id:
            continue
        status = str(row["status"] or "").strip().lower()
        if status in {"canceled", "cancelled", "archived", "done"}:
            continue
        scheduled_at = str(row["scheduled_at"] or "").strip()
        duration_min = int(row["duration_min"] or 0) if row["duration_min"] is not None else 0
        end_at = ""
        if scheduled_at and duration_min > 0:
            start_dt = _parse_iso(scheduled_at)
            if start_dt is not None:
                end_at = (start_dt + timedelta(minutes=duration_min)).isoformat()
        if scheduled_at and not _within_window(scheduled_at, end_at or scheduled_at, window_start=window_start, window_end=window_end):
            continue
        out[event_id] = {
            "item_id": str(row["id"] or "").strip(),
            "title": str(row["title"] or "").strip(),
            "comment": str(row["description"] or "").strip(),
            "start_at": scheduled_at,
            "end_at": end_at,
        }
    return out


def _read_google_events(*, calendar_client: Any | None, window_start: datetime, window_end: datetime) -> dict[str, dict[str, str]]:
    client = calendar_client
    if client is None:
        from src.google.calendar_client import CalendarClient

        client = CalendarClient()
    calendar_id = _calendar_id()
    page_token = None
    out: dict[str, dict[str, str]] = {}
    while True:
        response = client.list_events(
            calendar_id=calendar_id,
            sync_token=None,
            time_min=window_start.isoformat(),
            time_max=window_end.isoformat(),
            page_token=page_token,
        )
        for event in list(response.get("items") or []):
            if not isinstance(event, dict):
                continue
            event_id = str(event.get("id") or "").strip()
            if not event_id:
                continue
            if str(event.get("status") or "").strip().lower() == "cancelled":
                continue
            start_at, end_at = _parse_google_event_bounds(event)
            if not _within_window(start_at, end_at or start_at, window_start=window_start, window_end=window_end):
                continue
            out[event_id] = {
                "title": str(event.get("summary") or "").strip(),
                "comment": str(event.get("description") or "").strip(),
                "start_at": start_at,
                "end_at": end_at,
                "calendar_event_id": event_id,
            }
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return out


def collect_unified_calendar_rows(
    conn: sqlite3.Connection,
    *,
    user_id: str | None = None,
    calendar_client: Any | None = None,
) -> list[UnifiedCalendarRow]:
    window_start, window_end = _window_bounds_utc()
    block_rows, claimed_event_ids = _read_runtime_timeblocks(
        conn,
        user_id=user_id,
        window_start=window_start,
        window_end=window_end,
    )
    try:
        google_events = _read_google_events(
            calendar_client=calendar_client,
            window_start=window_start,
            window_end=window_end,
        )
    except Exception as exc:
        logger.warning(f"unified_calendar_export google_read_failed err={str(exc)[:300]}")
        google_events = {}

    meeting_refs = _read_runtime_known_meetings(
        conn,
        window_start=window_start,
        window_end=window_end,
    )
    out: list[UnifiedCalendarRow] = list(block_rows)

    for event_id in sorted(meeting_refs):
        live = google_events.get(event_id)
        if live is None:
            continue
        claimed_event_ids.add(event_id)
        ref = meeting_refs[event_id]
        out.append(
            UnifiedCalendarRow(
                entity_id=f"meeting_event:{event_id}",
                row_type="Встреча",
                title=str(live.get("title") or ref.get("title") or "Встреча"),
                start_at=str(live.get("start_at") or ref.get("start_at") or ""),
                end_at=str(live.get("end_at") or ref.get("end_at") or ""),
                duration_min=_duration_minutes(str(live.get("start_at") or ""), str(live.get("end_at") or "")),
                linked_task="",
                comment=str(live.get("comment") or ref.get("comment") or ""),
                calendar_event_id=event_id,
                source="runtime_known_meeting",
            )
        )

    for event_id in sorted(google_events):
        if event_id in claimed_event_ids:
            continue
        live = google_events[event_id]
        out.append(
            UnifiedCalendarRow(
                entity_id=f"external_event:{event_id}",
                row_type="Событие календаря",
                title=str(live.get("title") or "Событие календаря"),
                start_at=str(live.get("start_at") or ""),
                end_at=str(live.get("end_at") or ""),
                duration_min=_duration_minutes(str(live.get("start_at") or ""), str(live.get("end_at") or "")),
                linked_task="",
                comment=str(live.get("comment") or ""),
                calendar_event_id=event_id,
                source="google_calendar",
            )
        )

    def _sort_key(row: UnifiedCalendarRow) -> tuple[str, str]:
        return (str(row.start_at or ""), str(row.entity_id or ""))

    return sorted(out, key=_sort_key)
