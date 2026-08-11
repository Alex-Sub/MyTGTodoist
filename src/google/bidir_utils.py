from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo


def _collapse_spaces(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _normalize_notes(value: Any) -> str:
    text_value = str(value or "")
    text_value = text_value.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.strip() for line in text_value.split("\n")]
    return "\n".join(lines).strip()


def _normalize_status(value: Any) -> str:
    return _collapse_spaces(value).lower()


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


def _parse_sheet_date_time(date_raw: Any, time_raw: Any, *, timezone_name: str = "Europe/Moscow") -> str:
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
    local_tz = ZoneInfo(timezone_name)
    local_dt = datetime(parsed_date.year, parsed_date.month, parsed_date.day, hour, minute, tzinfo=local_tz)
    return local_dt.astimezone(timezone.utc).isoformat()


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
