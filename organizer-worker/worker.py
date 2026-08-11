import importlib
import importlib.util as importlib_util
import argparse
import json
import logging
import os
import re
import sqlite3
import traceback
import time
import socket
import sys
import threading
import uuid
from pathlib import Path
from datetime import datetime, timedelta, timezone, date
from typing import Any
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.error
import urllib.request

import requests


def _ensure_local_no_proxy() -> None:
    hosts = ("127.0.0.1", "localhost")
    for key in ("NO_PROXY", "no_proxy"):
        raw = os.getenv(key, "")
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        changed = False
        for host in hosts:
            if host not in parts:
                parts.append(host)
                changed = True
        if changed:
            os.environ[key] = ",".join(parts)


_ensure_local_no_proxy()


_ROOT_DIR = Path(__file__).resolve().parent
if str(_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(_ROOT_DIR))
_REPO_ROOT_DIR = _ROOT_DIR.parent
if str(_REPO_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_DIR))
_SRC_DIR = Path(__file__).resolve().parent / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))
from organizer_worker.startup_preflight import ensure_canon_mounted
from organizer_worker.google_calendar_idempotency import build_item_ical_uid, create_or_reuse_event
from organizer_worker import db
_P2_RUNTIME_PATH = _SRC_DIR / "p2_tasks_runtime.py"
_P2_SPEC = importlib_util.spec_from_file_location("p2_tasks_runtime", _P2_RUNTIME_PATH)
if _P2_SPEC is None or _P2_SPEC.loader is None:
    raise ImportError(f"Cannot load p2 runtime module from {_P2_RUNTIME_PATH}")
p2: Any = importlib_util.module_from_spec(_P2_SPEC)
sys.modules.setdefault("p2_tasks_runtime", p2)
_P2_SPEC.loader.exec_module(p2)
try:
    ensure_canon_mounted("/canon/intents_v2.yml")
except RuntimeError as exc:
    local_canon = Path(__file__).resolve().parents[1] / "canon" / "intents_v2.yml"
    if not Path("/.dockerenv").exists() and local_canon.exists():
        logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), force=True)
        logging.warning("canon_local_fallback path=%s", local_canon)
    else:
        logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), force=True)
        logging.error("%s", str(exc))
        raise SystemExit(1)
from organizer_worker.handlers import INTENT_ALIAS_TO_CANON, dispatch_intent
import shared_runtime as _shared_runtime
import runtime_search as _runtime_search_module
import runtime_sync_conflicts as _runtime_sync_conflicts_module
import runtime_calendar as _runtime_calendar_module
import runtime_handlers as _runtime_handlers_module
import runtime_server as _runtime_server_module
import legacy_queue as _legacy_queue_module

DB_PATH = os.getenv("DB_PATH", "/data/organizer.db")
TIMEZONE_NAME = os.getenv("TIMEZONE_NAME", os.getenv("TIMEZONE", "Europe/Moscow"))
DEFAULT_DURATION_MIN = int(os.getenv("DEFAULT_DURATION_MIN", "30"))
WORKER_INTERVAL_SEC = int(os.getenv("WORKER_INTERVAL_SEC", "5"))
WORKER_HEARTBEAT_SEC = int(os.getenv("WORKER_HEARTBEAT_SEC", "7"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "5"))
CALENDAR_MAX_ATTEMPTS = int(os.getenv("CALENDAR_MAX_ATTEMPTS", "5"))
GOOGLE_CALENDAR_ID = os.getenv("GOOGLE_CALENDAR_ID", "")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "")
CALENDAR_DEBUG = os.getenv("CALENDAR_DEBUG", "0") == "1"
CALENDAR_SMOKE_TEST = os.getenv("CALENDAR_SMOKE_TEST", "0") == "1"
_CALENDAR_SYNC_MODE_RAW = os.getenv("CALENDAR_SYNC_MODE", "full")
_CALENDAR_SYNC_MODE_NORM = (_CALENDAR_SYNC_MODE_RAW or "").strip().lower()
CALENDAR_SYNC_MODE = (
    _CALENDAR_SYNC_MODE_NORM
    if _CALENDAR_SYNC_MODE_NORM in {"off", "create", "full"}
    else "full"
)
REG_NUDGES_MODE = (os.getenv("REG_NUDGES_MODE", "off") or "off").strip().lower()
REG_NUDGES_INTERVAL_SEC = int(os.getenv("REG_NUDGES_INTERVAL_SEC", "3600"))
DRIFT_MODE = (os.getenv("DRIFT_MODE", "off") or "off").strip().lower()
OVERLOAD_MODE = (os.getenv("OVERLOAD_MODE", "off") or "off").strip().lower()
P5_TICK_INTERVAL_SEC = int(os.getenv("P5_TICK_INTERVAL_SEC", "3600"))
P5_NUDGES_MODE = (os.getenv("P5_NUDGES_MODE", "off") or "off").strip().lower()
CAPACITY_MINUTES_PER_DAY = int(os.getenv("CAPACITY_MINUTES_PER_DAY", "240"))
CAPACITY_ITEMS_PER_DAY = int(os.getenv("CAPACITY_ITEMS_PER_DAY", "6"))
DUE_TODAY_LIMIT = int(os.getenv("DUE_TODAY_LIMIT", "5"))
BACKLOG_LIMIT = int(os.getenv("BACKLOG_LIMIT", "50"))
P7_MODE = (os.getenv("P7_MODE", "off") or "off").strip().lower()
ASR_DT_SELF_CHECK = os.getenv("ASR_DT_SELF_CHECK", "0") == "1"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
_ML_GATEWAY_URL_RAW = os.getenv("ML_GATEWAY_URL", "").strip()
ML_GATEWAY_URL = _ML_GATEWAY_URL_RAW or "http://host.docker.internal:19000"
TG_HTTP_CONNECT_TIMEOUT = int(os.getenv("TG_HTTP_CONNECT_TIMEOUT", "3"))
TG_HTTP_READ_TIMEOUT = int(os.getenv("TG_HTTP_READ_TIMEOUT", "90"))
TG_HTTP_RETRIES = int(os.getenv("TG_HTTP_RETRIES", "2"))
TG_HTTP_RETRY_SLEEP = float(os.getenv("TG_HTTP_RETRY_SLEEP", "0.3"))
TG_HTTP_429_RETRIES = int(os.getenv("TG_HTTP_429_RETRIES", "2"))
WORKER_TG_TEXT_CHUNK = int(os.getenv("WORKER_TG_TEXT_CHUNK", "3500"))
ASR_HTTP_READ_TIMEOUT = int(os.getenv("ASR_HTTP_READ_TIMEOUT", "180"))
LOG_ML_RAW = os.getenv("LOG_ML_RAW", "0") == "1"
MEETING_DEFAULT_MINUTES = int(os.getenv("MEETING_DEFAULT_MINUTES", "30"))
LOCAL_TZ_OFFSET_MIN = int(os.getenv("LOCAL_TZ_OFFSET_MIN", "180"))  # +03:00 default
ORGANIZER_API_URL = os.getenv("ORGANIZER_API_URL", "http://organizer-api:8000")
DEFAULT_HOUR = int(os.getenv("DT_DEFAULT_HOUR", "10"))
DEFAULT_MINUTE = int(os.getenv("DT_DEFAULT_MINUTE", "0"))
DT_REQUIRE_AMPM_FOR_SHORT_HOURS = os.getenv("DT_REQUIRE_AMPM_FOR_SHORT_HOURS", "1") == "1"
DT_SHORT_HOUR_MAX = int(os.getenv("DT_SHORT_HOUR_MAX", "12"))
DEFAULT_WEEKDAY = int(os.getenv("DT_DEFAULT_WEEKDAY", "0"))  # 0=Mon..6=Sun
DEFAULT_MONTHDAY = int(os.getenv("DT_DEFAULT_MONTHDAY", "1"))  # for month-only refs
DEFAULT_YEAR_MONTH = int(os.getenv("DT_DEFAULT_YEAR_MONTH", "1"))  # 1..12
DEFAULT_YEAR_MONTHDAY = int(os.getenv("DT_DEFAULT_YEAR_MONTHDAY", "1"))  # 1..31
CLARIFY_STATE_PATH = os.getenv("CLARIFY_STATE_PATH", "/data/bot.clarify.json")
CLARIFY_TTL_SEC = int(os.getenv("CLARIFY_TTL_SEC", "180"))

# Notifications back to user after actual creation
WORKER_TG_HTTP_READ_TIMEOUT = int(os.getenv("WORKER_TG_HTTP_READ_TIMEOUT", "90"))
WORKER_TG_SEND_MAX_RETRIES = int(os.getenv("WORKER_TG_SEND_MAX_RETRIES", "2"))
WORKER_NOTIFY_ON_DEAD = os.getenv("WORKER_NOTIFY_ON_DEAD", "1") == "1"

B2_CLAIM_LEASE_SEC = int(os.getenv("B2_CLAIM_LEASE_SEC", "120"))
B2_MAX_ATTEMPTS = int(os.getenv("B2_MAX_ATTEMPTS", "5"))
B2_REQUEUE_FAILED_EVERY_SEC = int(os.getenv("B2_REQUEUE_FAILED_EVERY_SEC", "15"))
B2_REQUEUE_FAILED_BATCH = int(os.getenv("B2_REQUEUE_FAILED_BATCH", "10"))
B2_REPLAY_MAX_AGE_SEC = int(os.getenv("B2_REPLAY_MAX_AGE_SEC", "900"))
B2_REPLAY_SCAN_LIMIT = int(os.getenv("B2_REPLAY_SCAN_LIMIT", "200"))
B2_IDLE_SLEEP_SEC = float(os.getenv("B2_IDLE_SLEEP_SEC", "0.5"))
RUNTIME_TRACE_DEDUP_TTL_SEC = int(os.getenv("RUNTIME_TRACE_DEDUP_TTL_SEC", "86400"))
CALENDAR_DRIFT_FAST_CHECK_SEC = int(os.getenv("CALENDAR_DRIFT_FAST_CHECK_SEC", "900"))
CALENDAR_DRIFT_FAST_LOOKBACK_DAYS = int(os.getenv("CALENDAR_DRIFT_FAST_LOOKBACK_DAYS", "7"))
CALENDAR_DRIFT_FAST_LOOKAHEAD_DAYS = int(os.getenv("CALENDAR_DRIFT_FAST_LOOKAHEAD_DAYS", "30"))
CALENDAR_DRIFT_FULL_LOOKBACK_DAYS = int(os.getenv("CALENDAR_DRIFT_FULL_LOOKBACK_DAYS", "30"))
CALENDAR_DRIFT_FULL_LOOKAHEAD_DAYS = int(os.getenv("CALENDAR_DRIFT_FULL_LOOKAHEAD_DAYS", "180"))
CALENDAR_DRIFT_FULL_HOUR = int(os.getenv("CALENDAR_DRIFT_FULL_HOUR", "3"))
CALENDAR_DRIFT_FULL_MINUTE = int(os.getenv("CALENDAR_DRIFT_FULL_MINUTE", "30"))
SCHEMA_PATH = os.getenv("B2_SCHEMA_PATH", "/app/migrations/001_inbox_queue.sql")
MIGRATIONS_DIR = os.getenv("MIGRATIONS_DIR", "/app/migrations")
P2_ENFORCE_STATUS = os.getenv("P2_ENFORCE_STATUS", "0") == "1"
WORKER_COMMAND_PORT = int(os.getenv("WORKER_COMMAND_PORT", "8002"))
NUDGE_SIGNALS_KEY = "signals_enable_prompt"

WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"

_REG_NUDGE_LAST_SENT: dict[str, float] = {}
_P5_NUDGE_DAY: str | None = None
_P5_DRIFT_COUNT_TODAY: int = 0
_P5_OVERLOAD_COUNT_TODAY: int = 0
_P5_NUDGE_EMITTED: bool = False

def _local_tz() -> timezone:
    return _shared_runtime._local_tz(LOCAL_TZ_OFFSET_MIN)

def as_dict(row: sqlite3.Row | dict | None) -> dict:
    return _shared_runtime.as_dict(row)


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_iso_utc_or_none(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
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
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _queue_item_age_sec(row: dict, now_ts: float) -> float | None:
    base_ts = (
        row.get("ingested_at")
        or row.get("created_at")
        or row.get("updated_at")
    )
    parsed = _parse_iso_utc_or_none(base_ts)
    if parsed is None:
        return None
    return max(0.0, float(now_ts - parsed.timestamp()))


def _is_stale_queue_row(row: dict, now_ts: float) -> tuple[bool, float | None]:
    if B2_REPLAY_MAX_AGE_SEC <= 0:
        return False, None
    age = _queue_item_age_sec(row, now_ts)
    if age is None:
        return False, None
    return age > float(B2_REPLAY_MAX_AGE_SEC), age


# SHARED_RUNTIME_HELPERS_START
def _runtime_trace_dedup_init(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runtime_trace_dedup (
            trace_id TEXT PRIMARY KEY,
            response_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )


def _runtime_sync_conflicts_init(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_conflicts (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            source TEXT NOT NULL,
            entity_ref TEXT NULL,
            calendar_event_id TEXT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            notified_at TEXT NULL,
            last_checked_at TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            resolved_at TEXT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_sync_conflicts_user_status ON sync_conflicts(user_id, status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_sync_conflicts_calendar_event ON sync_conflicts(calendar_event_id)"
    )
    cols = {
        str(as_dict(row).get("name") or "")
        for row in conn.execute("PRAGMA table_info(sync_conflicts)").fetchall()
    }
    if "notified_at" not in cols:
        conn.execute("ALTER TABLE sync_conflicts ADD COLUMN notified_at TEXT NULL")
    if "last_checked_at" not in cols:
        conn.execute("ALTER TABLE sync_conflicts ADD COLUMN last_checked_at TEXT NULL")


def _runtime_trace_dedup_prune(conn: sqlite3.Connection) -> None:
    ttl = int(RUNTIME_TRACE_DEDUP_TTL_SEC)
    if ttl <= 0:
        return
    conn.execute(
        """
        DELETE FROM runtime_trace_dedup
        WHERE created_at < strftime('%Y-%m-%dT%H:%M:%fZ','now', '-' || ? || ' seconds')
        """,
        (str(ttl),),
    )


def _runtime_trace_dedup_get(trace_id: str) -> dict | None:
    key = str(trace_id or "").strip()
    if not key:
        return None
    with _get_conn() as conn:
        _runtime_trace_dedup_init(conn)
        _runtime_trace_dedup_prune(conn)
        row = conn.execute(
            "SELECT response_json FROM runtime_trace_dedup WHERE trace_id = ? LIMIT 1",
            (key,),
        ).fetchone()
        conn.commit()
    if row is None:
        return None
    try:
        payload = json.loads(str(as_dict(row).get("response_json") or "{}"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _runtime_trace_dedup_put(trace_id: str, payload: dict) -> None:
    key = str(trace_id or "").strip()
    if not key or not isinstance(payload, dict):
        return
    try:
        raw = json.dumps(payload, ensure_ascii=False)
    except Exception:
        return
    with _get_conn() as conn:
        _runtime_trace_dedup_init(conn)
        _runtime_trace_dedup_prune(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO runtime_trace_dedup (trace_id, response_json, created_at)
            VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            """,
            (key, raw),
        )
        conn.commit()


def _runtime_trace_dedup_replace(trace_id: str, payload: dict) -> None:
    _runtime_trace_dedup_put(trace_id, payload)


def _runtime_trace_dedup_delete(trace_ids: list[str]) -> int:
    keys = [str(item or "").strip() for item in trace_ids if str(item or "").strip()]
    if not keys:
        return 0
    with _get_conn() as conn:
        _runtime_trace_dedup_init(conn)
        cur = conn.executemany(
            "DELETE FROM runtime_trace_dedup WHERE trace_id = ?",
            [(key,) for key in keys],
        )
        conn.commit()
    return int(cur.rowcount or 0)


def _runtime_trace_list_rows_for_user(user_id: str, *, limit: int = 200) -> list[tuple[str, dict[str, Any]]]:
    uid = str(user_id or "").strip()
    if not uid:
        return []
    with _get_conn() as conn:
        _runtime_trace_dedup_init(conn)
        _runtime_trace_dedup_prune(conn)
        rows = conn.execute(
            """
            SELECT trace_id, response_json
            FROM runtime_trace_dedup
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (int(max(1, limit)),),
        ).fetchall()
    out: list[tuple[str, dict[str, Any]]] = []
    for row in rows:
        raw = str(as_dict(row).get("response_json") or "")
        trace_id = str(as_dict(row).get("trace_id") or "").strip()
        if not raw or not trace_id:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        event_id = str(payload.get("calendar_event_id") or "").strip()
        if not event_id:
            continue
        debug = payload.get("debug")
        debug = debug if isinstance(debug, dict) else {}
        payload_uid = str(debug.get("user_id") or payload.get("user_id") or "").strip()
        if payload_uid != uid:
            continue
        out.append((trace_id, payload))
    return out


def _runtime_trace_list_all_rows(*, limit: int = 5000) -> list[tuple[str, dict[str, Any]]]:
    with _get_conn() as conn:
        _runtime_trace_dedup_init(conn)
        _runtime_trace_dedup_prune(conn)
        rows = conn.execute(
            """
            SELECT trace_id, response_json
            FROM runtime_trace_dedup
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (int(max(1, limit)),),
        ).fetchall()
    out: list[tuple[str, dict[str, Any]]] = []
    for row in rows:
        raw = str(as_dict(row).get("response_json") or "")
        trace_id = str(as_dict(row).get("trace_id") or "").strip()
        if not raw or not trace_id:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        if isinstance(payload, dict):
            out.append((trace_id, payload))
    return out


def _parse_drift_range_bound(value: Any, *, end_of_day: bool) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed_dt = _parse_iso_datetime_to_local(raw)
    if parsed_dt is not None:
        return parsed_dt
    parsed_date = _parse_date_token_ymd(raw)
    if parsed_date is None:
        return None
    if end_of_day:
        return datetime(parsed_date.year, parsed_date.month, parsed_date.day, 23, 59, 59, tzinfo=_local_tz())
    return datetime(parsed_date.year, parsed_date.month, parsed_date.day, 0, 0, 0, tzinfo=_local_tz())


def _runtime_snapshot_in_range(snapshot: dict[str, Any], from_dt: datetime | None, to_dt: datetime | None) -> bool:
    start_at = _parse_iso_datetime_to_local(snapshot.get("start_at"))
    if start_at is None:
        date_raw = str(snapshot.get("start_at_date") or "").strip()
        time_raw = str(snapshot.get("start_at_time") or "").strip() or "00:00"
        date_token = _parse_date_token_ymd(date_raw)
        time_token = _parse_time_token_hhmm(time_raw)
        if date_token is None or time_token is None:
            return True
        start_at = datetime(date_token.year, date_token.month, date_token.day, time_token[0], time_token[1], tzinfo=_local_tz())
    if from_dt is not None and start_at < from_dt:
        return False
    if to_dt is not None and start_at > to_dt:
        return False
    return True


def _runtime_sync_conflict_snapshot_from_payload(trace_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    base = payload.get("__trace_snapshot")
    base = dict(base) if isinstance(base, dict) else {}
    debug = payload.get("debug")
    debug = debug if isinstance(debug, dict) else {}
    summary = {
        "trace_id": trace_id,
        "user_id": str(base.get("user_id") or debug.get("user_id") or payload.get("user_id") or "").strip(),
        "calendar_event_id": str(
            base.get("calendar_event_id") or payload.get("calendar_event_id") or debug.get("calendar_event_id") or ""
        ).strip(),
        "source_msg_id": str(base.get("source_msg_id") or payload.get("source_msg_id") or "").strip(),
        "title": str(base.get("title") or payload.get("title") or "Встреча").strip() or "Встреча",
        "meeting_kind": str(base.get("meeting_kind") or payload.get("meeting_kind") or "").strip(),
        "start_at": str(base.get("start_at") or payload.get("start_at") or "").strip(),
        "start_at_date": str(base.get("start_at_date") or payload.get("start_at_date") or "").strip(),
        "start_at_time": str(base.get("start_at_time") or payload.get("start_at_time") or "").strip(),
        "duration_minutes": base.get("duration_minutes") or payload.get("duration_minutes"),
        "comment_text": str(
            base.get("comment_text")
            or payload.get("comment_text")
            or payload.get("description")
            or payload.get("comment")
            or ""
        ).strip(),
    }
    if summary["start_at"] and (not summary["start_at_date"] or not summary["start_at_time"]):
        parsed = _parse_iso_datetime_to_local(summary["start_at"])
        if parsed is not None:
            if not summary["start_at_date"]:
                summary["start_at_date"] = parsed.date().isoformat()
            if not summary["start_at_time"]:
                summary["start_at_time"] = parsed.strftime("%H:%M")
    if summary["duration_minutes"] in (None, ""):
        summary["duration_minutes"] = DEFAULT_DURATION_MIN
    chat_id, _message_id = _parse_tg_source_msg_id(summary.get("source_msg_id"))
    summary["chat_id"] = str(chat_id or "").strip()
    return summary


def _runtime_sync_conflict_summary_text(conflict: dict[str, Any]) -> str:
    title = str(conflict.get("title") or "Встреча").strip() or "Встреча"
    start_date = str(conflict.get("start_at_date") or "").strip()
    start_time = str(conflict.get("start_at_time") or "").strip()
    duration = str(conflict.get("duration_minutes") or "").strip()
    comment_text = str(conflict.get("comment_text") or "").strip()
    parts = [title]
    if start_date or start_time:
        parts.append(" ".join([p for p in (start_date, start_time) if p]).strip())
    if duration:
        parts.append(f"{duration} мин")
    if comment_text:
        parts.append(f"Комментарий: {comment_text}")
    return "\n".join([part for part in parts if part])


def _runtime_sync_conflict_row_to_payload(row: Any) -> dict[str, Any]:
    item = as_dict(row)
    payload_raw = str(item.get("payload_json") or "{}")
    try:
        payload_json = json.loads(payload_raw)
    except Exception:
        payload_json = {}
    payload_json = payload_json if isinstance(payload_json, dict) else {}
    out = {
        "id": str(item.get("id") or "").strip(),
        "user_id": str(item.get("user_id") or "").strip(),
        "kind": str(item.get("kind") or "").strip(),
        "source": str(item.get("source") or "").strip(),
        "entity_ref": str(item.get("entity_ref") or "").strip(),
        "calendar_event_id": str(item.get("calendar_event_id") or "").strip(),
        "status": str(item.get("status") or "").strip(),
        "notified_at": str(item.get("notified_at") or "").strip(),
        "last_checked_at": str(item.get("last_checked_at") or "").strip(),
        "created_at": str(item.get("created_at") or "").strip(),
        "updated_at": str(item.get("updated_at") or "").strip(),
        "resolved_at": str(item.get("resolved_at") or "").strip(),
        "payload": payload_json,
    }
    out.update({k: v for k, v in payload_json.items() if k not in out})
    out["summary_text"] = _runtime_sync_conflict_summary_text(out)
    return out


def _runtime_sync_conflict_create_or_reopen(
    *,
    user_id: str,
    source: str,
    entity_ref: str,
    calendar_event_id: str,
    payload: dict[str, Any],
    kind: str = "calendar_missing",
) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    entity_key = str(entity_ref or "").strip()
    event_id = str(calendar_event_id or "").strip()
    payload_json = dict(payload if isinstance(payload, dict) else {})
    payload_json["calendar_event_id"] = event_id
    payload_json["user_id"] = uid
    raw = json.dumps(payload_json, ensure_ascii=False)
    lifecycle = "created"
    with _get_conn() as conn:
        _runtime_sync_conflicts_init(conn)
        row = conn.execute(
            """
            SELECT *
            FROM sync_conflicts
            WHERE user_id = ?
              AND source = ?
              AND entity_ref = ?
              AND kind = ?
              AND calendar_event_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (uid, str(source or "").strip(), entity_key, str(kind or "").strip(), event_id),
        ).fetchone()
        if row is None:
            conflict_id = f"sc-{uuid.uuid4().hex[:12]}"
            conn.execute(
                """
                INSERT INTO sync_conflicts (
                    id, user_id, kind, source, entity_ref, calendar_event_id, payload_json,
                    status, notified_at, last_checked_at, created_at, updated_at, resolved_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?,
                    'pending',
                    NULL,
                    strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                    strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                    strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                    NULL
                )
                """,
                (conflict_id, uid, kind, source, entity_key, event_id, raw),
            )
            row = conn.execute("SELECT * FROM sync_conflicts WHERE id = ?", (conflict_id,)).fetchone()
        else:
            row_dict = as_dict(row)
            status = str(row_dict.get("status") or "").strip().lower()
            if status in {"resolved"}:
                lifecycle = "recreated"
                conflict_id = f"sc-{uuid.uuid4().hex[:12]}"
                conn.execute(
                    """
                    INSERT INTO sync_conflicts (
                        id, user_id, kind, source, entity_ref, calendar_event_id, payload_json,
                        status, notified_at, last_checked_at, created_at, updated_at, resolved_at
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?,
                        'pending',
                        NULL,
                        strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                        strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                        strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                        NULL
                    )
                    """,
                    (conflict_id, uid, kind, source, entity_key, event_id, raw),
                )
                row = conn.execute("SELECT * FROM sync_conflicts WHERE id = ?", (conflict_id,)).fetchone()
            elif status == "pending":
                lifecycle = "existing_pending"
                conn.execute(
                    """
                    UPDATE sync_conflicts
                    SET calendar_event_id = ?,
                        payload_json = ?,
                        status = 'pending',
                        last_checked_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                        resolved_at = NULL
                    WHERE id = ?
                    """,
                    (event_id, raw, str(row_dict.get("id") or "").strip()),
                )
                row = conn.execute(
                    "SELECT * FROM sync_conflicts WHERE id = ?",
                    (str(row_dict.get("id") or "").strip(),),
                ).fetchone()
            else:
                lifecycle = "existing_skipped"
                conn.execute(
                    """
                    UPDATE sync_conflicts
                    SET calendar_event_id = ?,
                        payload_json = ?,
                        status = ?,
                        last_checked_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                    WHERE id = ?
                    """,
                    (event_id, raw, status or "skipped", str(row_dict.get("id") or "").strip()),
                )
                row = conn.execute(
                    "SELECT * FROM sync_conflicts WHERE id = ?",
                    (str(row_dict.get("id") or "").strip(),),
                ).fetchone()
        conn.commit()
    assert row is not None
    payload_out = _runtime_sync_conflict_row_to_payload(row)
    payload_out["lifecycle"] = lifecycle
    return payload_out


def _runtime_sync_conflict_get(conflict_id: str) -> dict[str, Any] | None:
    key = str(conflict_id or "").strip()
    if not key:
        return None
    with _get_conn() as conn:
        _runtime_sync_conflicts_init(conn)
        row = conn.execute("SELECT * FROM sync_conflicts WHERE id = ? LIMIT 1", (key,)).fetchone()
    if row is None:
        return None
    return _runtime_sync_conflict_row_to_payload(row)


def _runtime_sync_conflict_set_status(conflict_id: str, status: str) -> dict[str, Any] | None:
    key = str(conflict_id or "").strip()
    if not key:
        return None
    normalized = str(status or "").strip().lower() or "pending"
    resolved_at_sql = "strftime('%Y-%m-%dT%H:%M:%fZ','now')" if normalized == "resolved" else "NULL"
    with _get_conn() as conn:
        _runtime_sync_conflicts_init(conn)
        conn.execute(
            f"""
            UPDATE sync_conflicts
            SET status = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                resolved_at = {resolved_at_sql}
            WHERE id = ?
            """,
            (normalized, key),
        )
        row = conn.execute("SELECT * FROM sync_conflicts WHERE id = ? LIMIT 1", (key,)).fetchone()
        conn.commit()
    if row is None:
        return None
    return _runtime_sync_conflict_row_to_payload(row)


def _runtime_sync_conflict_mark_notified(conflict_id: str) -> dict[str, Any] | None:
    key = str(conflict_id or "").strip()
    if not key:
        return None
    with _get_conn() as conn:
        _runtime_sync_conflicts_init(conn)
        conn.execute(
            """
            UPDATE sync_conflicts
            SET notified_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            (key,),
        )
        row = conn.execute("SELECT * FROM sync_conflicts WHERE id = ? LIMIT 1", (key,)).fetchone()
        conn.commit()
    if row is None:
        return None
    return _runtime_sync_conflict_row_to_payload(row)


def _runtime_sync_conflict_keyboard(conflict_id: str) -> dict[str, Any]:
    cid = str(conflict_id or "").strip()
    return {
        "inline_keyboard": [
            [{"text": "Удалить из базы", "callback_data": f"sync_conflict:delete_db:{cid}"}],
            [{"text": "Восстановить в календаре", "callback_data": f"sync_conflict:restore_calendar:{cid}"}],
            [{"text": "Изменить", "callback_data": f"sync_conflict:edit:{cid}"}],
            [{"text": "Пропустить", "callback_data": f"sync_conflict:skip:{cid}"}],
        ]
    }


def _runtime_sync_conflict_notify_if_needed(conflict: dict[str, Any]) -> bool:
    if not isinstance(conflict, dict):
        return False
    status = str(conflict.get("status") or "").strip().lower()
    lifecycle = str(conflict.get("lifecycle") or "").strip().lower()
    conflict_id = str(conflict.get("id") or "").strip()
    chat_id_raw = conflict.get("chat_id") or (conflict.get("payload") or {}).get("chat_id")
    chat_id = _to_int_or_none(chat_id_raw)
    if status != "pending" or not conflict_id or not chat_id:
        return False
    if str(conflict.get("notified_at") or "").strip():
        logging.info(
            "sync_conflict_existing",
            extra={"conflict_id": conflict_id, "user_id": str(conflict.get("user_id") or ""), "calendar_event_id": str(conflict.get("calendar_event_id") or "")},
        )
        return False
    if lifecycle not in {"created", "recreated"}:
        logging.info(
            "sync_conflict_existing",
            extra={"conflict_id": conflict_id, "user_id": str(conflict.get("user_id") or ""), "calendar_event_id": str(conflict.get("calendar_event_id") or "")},
        )
        return False
    text = (
        "Найдено событие в базе, которого нет в Google Calendar.\n"
        f"{str(conflict.get('summary_text') or '').strip()}\n"
        "Что сделать?"
    ).strip()
    sent = _tg_send_message_with_keyboard(
        int(chat_id),
        text,
        _runtime_sync_conflict_keyboard(conflict_id),
        stage="sync_conflict_notify",
    )
    if sent:
        _runtime_sync_conflict_mark_notified(conflict_id)
        logging.info(
            "sync_conflict_created",
            extra={"conflict_id": conflict_id, "user_id": str(conflict.get("user_id") or ""), "calendar_event_id": str(conflict.get("calendar_event_id") or "")},
        )
        return True
    return False


def _runtime_cleanup_stale_calendar_events_for_user(user_id: str, *, limit: int = 500) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    if not uid:
        return {"ok": False, "reason": "user_id_required", "conflicts": [], "checked": 0}
    rows = _runtime_trace_list_rows_for_user(uid, limit=limit)
    conflicts: list[dict[str, Any]] = []
    checked = 0
    for trace_id, payload in rows:
        snapshot = _runtime_sync_conflict_snapshot_from_payload(trace_id, payload)
        event_id = str(snapshot.get("calendar_event_id") or "").strip()
        if not event_id:
            continue
        checked += 1
        event = _calendar_get_event(event_id)
        if bool(event.get("ok")):
            continue
        if int(event.get("http_status") or 0) == 404:
            conflicts.append(
                _runtime_sync_conflict_create_or_reopen(
                    user_id=uid,
                    source="runtime_trace_dedup",
                    entity_ref=trace_id,
                    calendar_event_id=event_id,
                    payload=snapshot,
                )
            )
    return {
        "ok": True,
        "user_id": uid,
        "checked": checked,
        "stale_found": len(conflicts),
        "created_or_reopened": len(conflicts),
        "conflicts": conflicts,
    }


def _runtime_check_calendar_drift(
    *,
    user_id: str = "",
    from_value: Any = "",
    to_value: Any = "",
    notify: bool = True,
    limit: int = 5000,
) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    from_dt = _parse_drift_range_bound(from_value, end_of_day=False)
    to_dt = _parse_drift_range_bound(to_value, end_of_day=True)
    logging.info(
        "calendar_drift_check_started",
        extra={"user_id": uid, "from": str(from_value or ""), "to": str(to_value or ""), "notify": bool(notify)},
    )
    rows = _runtime_trace_list_rows_for_user(uid, limit=limit) if uid else _runtime_trace_list_all_rows(limit=limit)
    checked = 0
    missing = 0
    conflicts: list[dict[str, Any]] = []
    notified = 0
    for trace_id, payload in rows:
        snapshot = _runtime_sync_conflict_snapshot_from_payload(trace_id, payload)
        event_id = str(snapshot.get("calendar_event_id") or "").strip()
        snapshot_uid = str(snapshot.get("user_id") or "").strip()
        if not event_id:
            continue
        if uid and snapshot_uid != uid:
            continue
        if not _runtime_snapshot_in_range(snapshot, from_dt, to_dt):
            continue
        checked += 1
        event = _calendar_get_event(event_id)
        if bool(event.get("ok")):
            continue
        if int(event.get("http_status") or 0) != 404:
            continue
        missing += 1
        logging.info(
            "calendar_drift_missing_event_found",
            extra={"user_id": snapshot_uid, "trace_id": trace_id, "calendar_event_id": event_id},
        )
        conflict = _runtime_sync_conflict_create_or_reopen(
            user_id=snapshot_uid,
            source="runtime_trace_dedup",
            entity_ref=trace_id,
            calendar_event_id=event_id,
            payload=snapshot,
        )
        conflicts.append(conflict)
        if notify and _runtime_sync_conflict_notify_if_needed(conflict):
            notified += 1
    logging.info(
        "calendar_drift_check_finished",
        extra={"user_id": uid, "checked": checked, "missing": missing, "conflicts": len(conflicts), "notified": notified},
    )
    return {
        "ok": True,
        "user_id": uid,
        "checked": checked,
        "missing": missing,
        "conflicts_created_or_found": len(conflicts),
        "notified": notified,
        "conflicts": conflicts,
        "from": str(from_value or ""),
        "to": str(to_value or ""),
    }


def _runtime_candidate_signature(candidate: dict[str, Any]) -> str:
    return _runtime_search_module._runtime_candidate_signature(candidate)


def _runtime_build_trace_snapshot(
    *,
    trace_id: str,
    intent: str,
    entities: dict[str, Any],
    response_payload: dict[str, Any],
    user_id: str,
) -> dict[str, Any]:
    out = dict(response_payload if isinstance(response_payload, dict) else {})
    event_id = str(out.get("calendar_event_id") or entities.get("calendar_event_id") or "").strip()
    title = str(
        entities.get("title")
        or out.get("title")
        or entities.get("meeting_update_source_title")
        or "Встреча"
    ).strip() or "Встреча"
    start_at = str(
        entities.get("start_at")
        or out.get("start_at")
        or ""
    ).strip()
    start_at_date = str(entities.get("start_at_date") or out.get("start_at_date") or "").strip()
    start_at_time = str(entities.get("start_at_time") or out.get("start_at_time") or "").strip()
    if not start_at and start_at_date and start_at_time:
        try:
            start_at = datetime.fromisoformat(f"{start_at_date}T{start_at_time}").replace(tzinfo=_local_tz()).isoformat()
        except Exception:
            start_at = ""
    snapshot = {
        "trace_id": trace_id,
        "intent": str(intent or "").strip(),
        "user_id": str(user_id or "").strip(),
        "calendar_event_id": event_id,
        "source_msg_id": str(entities.get("source_msg_id") or "").strip(),
        "title": title,
        "meeting_kind": str(entities.get("meeting_kind") or out.get("meeting_kind") or "").strip(),
        "start_at": start_at,
        "start_at_date": start_at_date,
        "start_at_time": start_at_time,
        "duration_minutes": entities.get("duration_minutes") or out.get("duration_minutes") or DEFAULT_DURATION_MIN,
        "comment_text": str(
            entities.get("comment_text")
            or entities.get("description")
            or entities.get("comment")
            or out.get("comment_text")
            or out.get("description")
            or out.get("comment")
            or ""
        ).strip(),
    }
    out["__trace_snapshot"] = snapshot
    out["user_id"] = str(user_id or "").strip()
    if event_id:
        out["calendar_event_id"] = event_id
    if snapshot["start_at"]:
        out["start_at"] = snapshot["start_at"]
    if snapshot["start_at_date"]:
        out["start_at_date"] = snapshot["start_at_date"]
    if snapshot["start_at_time"]:
        out["start_at_time"] = snapshot["start_at_time"]
    out["duration_minutes"] = snapshot["duration_minutes"]
    if snapshot["comment_text"] or "comment_text" in out:
        out["comment_text"] = snapshot["comment_text"]
    if title:
        out["title"] = title
    return out


def _runtime_trace_conflict_restore_from_snapshot(conflict: dict[str, Any], *, override: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = conflict.get("payload") if isinstance(conflict.get("payload"), dict) else {}
    snapshot = dict(payload if isinstance(payload, dict) else {})
    if isinstance(override, dict):
        snapshot.update({k: v for k, v in override.items() if v is not None})
    title = str(snapshot.get("title") or "Встреча").strip() or "Встреча"
    start_at = _parse_iso_datetime_to_local(snapshot.get("start_at"))
    if start_at is None:
        date_raw = str(snapshot.get("start_at_date") or "").strip()
        time_raw = str(snapshot.get("start_at_time") or "").strip() or "10:00"
        time_token = _parse_time_token_hhmm(time_raw)
        date_token = _parse_date_token_ymd(date_raw)
        if date_token is None or time_token is None:
            return {"ok": False, "error": "invalid_conflict_snapshot"}
        start_at = datetime(
            date_token.year,
            date_token.month,
            date_token.day,
            time_token[0],
            time_token[1],
            tzinfo=_local_tz(),
        )
    duration = int(snapshot.get("duration_minutes") or DEFAULT_DURATION_MIN)
    end_at = start_at + timedelta(minutes=max(1, duration))
    comment_text = str(snapshot.get("comment_text") or "").strip() or None
    try:
        new_event_id = _create_event(
            str(snapshot.get("trace_id") or conflict.get("entity_ref") or f"conflict:{conflict.get('id') or ''}"),
            title,
            start_at,
            end_at,
            description=comment_text,
            flow_id=str(conflict.get("id") or ""),
        )
    except Exception as exc:
        return {"ok": False, "error": "calendar_create_failed", "detail": str(exc)[:300]}
    if not new_event_id:
        return {"ok": False, "error": "calendar_event_id_missing"}
    trace_id = str(conflict.get("entity_ref") or "").strip()
    if trace_id:
        payload_row = _runtime_trace_dedup_get(trace_id) or {}
        updated_payload = _runtime_build_trace_snapshot(
            trace_id=trace_id,
            intent=str(snapshot.get("intent") or "meeting.update"),
            entities={
                "calendar_event_id": str(new_event_id),
                "title": title,
                "start_at": start_at.isoformat(),
                "start_at_date": start_at.date().isoformat(),
                "start_at_time": start_at.strftime("%H:%M"),
                "duration_minutes": duration,
                "comment_text": comment_text or "",
                "meeting_kind": str(snapshot.get("meeting_kind") or ""),
            },
            response_payload={**payload_row, "calendar_event_id": str(new_event_id)},
            user_id=str(conflict.get("user_id") or snapshot.get("user_id") or ""),
        )
        _runtime_trace_dedup_replace(trace_id, updated_payload)
    _runtime_sync_conflict_set_status(str(conflict.get("id") or ""), "resolved")
    return {
        "ok": True,
        "calendar_event_id": str(new_event_id),
        "user_message": "Событие восстановлено в календаре.",
        "conflict_id": str(conflict.get("id") or ""),
    }


def _runtime_trace_find_latest_calendar_event_for_user(user_id: str, *, limit: int = 200) -> str | None:
    uid = str(user_id or "").strip()
    if not uid:
        return None
    for trace_id, payload in _runtime_trace_list_rows_for_user(uid, limit=limit):
        snapshot = _runtime_sync_conflict_snapshot_from_payload(trace_id, payload)
        event_id = str(snapshot.get("calendar_event_id") or "").strip()
        if not event_id:
            continue
        event = _calendar_get_event(event_id)
        if bool(event.get("ok")):
            return event_id
        if int(event.get("http_status") or 0) == 404:
            _runtime_sync_conflict_create_or_reopen(
                user_id=uid,
                source="runtime_trace_dedup",
                entity_ref=trace_id,
                calendar_event_id=event_id,
                payload=snapshot,
            )
    return None


def _runtime_trace_list_calendar_events_for_user(user_id: str, *, limit: int = 200) -> list[str]:
    uid = str(user_id or "").strip()
    if not uid:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for trace_id, payload in _runtime_trace_list_rows_for_user(uid, limit=limit):
        snapshot = _runtime_sync_conflict_snapshot_from_payload(trace_id, payload)
        event_id = str(snapshot.get("calendar_event_id") or "").strip()
        if not event_id or event_id in seen:
            continue
        event = _calendar_get_event(event_id)
        if not bool(event.get("ok")):
            if int(event.get("http_status") or 0) == 404:
                _runtime_sync_conflict_create_or_reopen(
                    user_id=uid,
                    source="runtime_trace_dedup",
                    entity_ref=trace_id,
                    calendar_event_id=event_id,
                    payload=snapshot,
                )
            continue
        seen.add(event_id)
        out.append(event_id)
    return out


def _runtime_meeting_source_for_user(user_id: str) -> dict[str, Any]:
    return _runtime_search_module._runtime_meeting_source_for_user(
        user_id,
        trace_list_rows_for_user=_runtime_trace_list_rows_for_user,
        conflict_snapshot_from_payload=_runtime_sync_conflict_snapshot_from_payload,
        calendar_get_event=_calendar_get_event,
        sync_conflict_create_or_reopen=_runtime_sync_conflict_create_or_reopen,
        parse_iso_datetime_to_local=_parse_iso_datetime_to_local,
        default_duration_min=DEFAULT_DURATION_MIN,
        timezone_name=TIMEZONE_NAME,
    )


def _runtime_meeting_search_for_user(user_id: str, target_hint: str = "", meeting_kind: str = "") -> dict[str, Any]:
    return _runtime_search_module._runtime_meeting_search_for_user(
        user_id,
        target_hint,
        meeting_kind,
        normalize_meeting_kind=_normalize_meeting_kind,
        extract_meeting_kind_from_text=_extract_meeting_kind_from_text,
        trace_list_rows_for_user=_runtime_trace_list_rows_for_user,
        conflict_snapshot_from_payload=_runtime_sync_conflict_snapshot_from_payload,
        calendar_get_event=_calendar_get_event,
        sync_conflict_create_or_reopen=_runtime_sync_conflict_create_or_reopen,
        parse_iso_datetime_to_local=_parse_iso_datetime_to_local,
        runtime_candidate_signature=_runtime_candidate_signature,
        default_duration_min=DEFAULT_DURATION_MIN,
        meeting_kind_default=_MEETING_KIND_DEFAULT,
    )


def _runtime_task_search_for_user(user_id: str, target_hint: str = "") -> dict[str, Any]:
    return _runtime_search_module._runtime_task_search_for_user(
        user_id,
        target_hint,
        get_conn=_get_conn,
        as_dict=as_dict,
    )


def _runtime_task_duplicate_check_for_user(
    user_id: str,
    *,
    title: str,
    planned_at: Any = None,
    parent_task_id: Any = None,
) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    title_text = str(title or "").strip()
    if not uid:
        return {"ok": False, "reason": "user_id_required", "duplicate_found": False}
    if not title_text:
        return {"ok": False, "reason": "title_required", "duplicate_found": False}
    try:
        parent_task_id_value = int(parent_task_id) if parent_task_id not in (None, "") else None
    except Exception:
        return {"ok": False, "reason": "invalid_parent_task_id", "duplicate_found": False}
    logging.info(
        "task_duplicate_precheck_start",
        extra={
            "user_id": uid,
            "title": title_text,
            "planned_at": str(planned_at or "").strip(),
            "planned_day": (str(planned_at or "").strip()[:10] or None),
            "undated_scope": not bool(str(planned_at or "").strip()),
            "parent_task_id": str(parent_task_id_value or ""),
            "source": "worker_precheck_endpoint",
        },
    )
    with _get_conn() as conn:
        probe = db.inspect_duplicate_active_task_create_candidate(
            conn,
            title=title_text,
            planned_at=(str(planned_at).strip() if planned_at is not None else None),
            parent_task_id=parent_task_id_value,
        )
    duplicate_scope = "dated"
    if bool(probe.get("undated_scope")):
        duplicate_scope = "subtask_undated" if str(probe.get("parent_scope") or "").strip() else "inbox"
    logging.info(
        "task_duplicate_precheck_result",
        extra={
            "normalized_title": probe.get("normalized_title"),
            "planned_at": (str(planned_at).strip() if planned_at is not None else None),
            "planned_day": probe.get("planned_day"),
            "undated_scope": bool(probe.get("undated_scope")),
            "duplicate_scope": duplicate_scope,
            "parent_task_id": probe.get("parent_scope"),
            "candidate_count": int(probe.get("candidate_count") or 0),
            "duplicate_found": bool(probe.get("duplicate_found")),
            "source": "worker_precheck_endpoint",
        },
    )
    duplicate = probe.get("duplicate") if isinstance(probe.get("duplicate"), dict) else None
    if duplicate is None:
        return {
            "ok": True,
            "duplicate_found": False,
            "normalized_title": str(probe.get("normalized_title") or ""),
            "planned_at": (str(planned_at).strip() if planned_at is not None else None),
            "planned_day": probe.get("planned_day"),
            "undated_scope": bool(probe.get("undated_scope")),
            "duplicate_scope": duplicate_scope,
            "candidate_count": int(probe.get("candidate_count") or 0),
        }
    return {
        "ok": True,
        "duplicate_found": True,
        "normalized_title": str(probe.get("normalized_title") or ""),
        "planned_at": (str(planned_at).strip() if planned_at is not None else None),
        "planned_day": probe.get("planned_day"),
        "undated_scope": bool(probe.get("undated_scope")),
        "duplicate_scope": duplicate_scope,
        "candidate_count": int(probe.get("candidate_count") or 0),
        "existing_task": {
            "id": duplicate.get("id"),
            "title": duplicate.get("title"),
            "planned_at": duplicate.get("planned_at"),
            "parent_task_id": duplicate.get("parent_task_id"),
        },
    }


def _runtime_timeblock_search_for_user(user_id: str, target_hint: str = "") -> dict[str, Any]:
    return _runtime_search_module._runtime_timeblock_search_for_user(
        user_id,
        target_hint,
        get_conn=_get_conn,
        as_dict=as_dict,
    )


def _runtime_sync_conflict_apply_action(conflict_id: str, action: str) -> dict[str, Any]:
    conflict = _runtime_sync_conflict_get(conflict_id)
    if not isinstance(conflict, dict):
        return {"ok": False, "reason": "conflict_not_found"}
    normalized_action = str(action or "").strip().lower()
    if normalized_action == "delete_db":
        trace_id = str(conflict.get("entity_ref") or "").strip()
        removed = _runtime_trace_dedup_delete([trace_id]) if trace_id else 0
        _runtime_sync_conflict_set_status(conflict_id, "resolved")
        return {
            "ok": True,
            "action": "delete_db",
            "removed": removed,
            "conflict_id": conflict_id,
            "user_message": "Событие удалено из внутренней базы и больше не будет предлагаться.",
        }
    if normalized_action == "restore_calendar":
        return _runtime_trace_conflict_restore_from_snapshot(conflict)
    if normalized_action == "skip":
        _runtime_sync_conflict_set_status(conflict_id, "skipped")
        return {
            "ok": True,
            "action": "skip",
            "conflict_id": conflict_id,
            "user_message": "Хорошо, пока пропускаю этот конфликт.",
        }
    if normalized_action == "edit":
        return {"ok": True, "action": "edit", "conflict_id": conflict_id, "conflict": conflict}
    return {"ok": False, "reason": "unsupported_action"}


def _runtime_sync_conflict_deps() -> dict[str, Any]:
    return {
        "get_conn": _get_conn,
        "as_dict": as_dict,
        "runtime_trace_dedup_ttl_sec": RUNTIME_TRACE_DEDUP_TTL_SEC,
        "parse_iso_datetime_to_local": _parse_iso_datetime_to_local,
        "parse_date_token_ymd": _parse_date_token_ymd,
        "parse_time_token_hhmm": _parse_time_token_hhmm,
        "local_tz": _local_tz,
        "default_duration_min": DEFAULT_DURATION_MIN,
        "parse_tg_source_msg_id": _parse_tg_source_msg_id,
        "to_int_or_none": _to_int_or_none,
        "calendar_get_event": _calendar_get_event,
        "create_event": _create_event,
        "tg_send_message_with_keyboard": _tg_send_message_with_keyboard,
    }


def _runtime_trace_dedup_get(trace_id: str) -> dict | None:
    return _runtime_sync_conflicts_module._runtime_trace_dedup_get(
        trace_id,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_trace_dedup_put(trace_id: str, payload: dict) -> None:
    _runtime_sync_conflicts_module._runtime_trace_dedup_put(
        trace_id,
        payload,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_trace_dedup_replace(trace_id: str, payload: dict) -> None:
    _runtime_sync_conflicts_module._runtime_trace_dedup_replace(
        trace_id,
        payload,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_trace_dedup_delete(trace_ids: list[str]) -> int:
    return _runtime_sync_conflicts_module._runtime_trace_dedup_delete(
        trace_ids,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_trace_list_rows_for_user(user_id: str, *, limit: int = 200) -> list[tuple[str, dict[str, Any]]]:
    return _runtime_sync_conflicts_module._runtime_trace_list_rows_for_user(
        user_id,
        limit=limit,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_trace_list_all_rows(*, limit: int = 5000) -> list[tuple[str, dict[str, Any]]]:
    return _runtime_sync_conflicts_module._runtime_trace_list_all_rows(
        limit=limit,
        deps=_runtime_sync_conflict_deps(),
    )


def _parse_drift_range_bound(value: Any, *, end_of_day: bool) -> datetime | None:
    return _runtime_sync_conflicts_module._parse_drift_range_bound(
        value,
        end_of_day=end_of_day,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_snapshot_in_range(snapshot: dict[str, Any], from_dt: datetime | None, to_dt: datetime | None) -> bool:
    return _runtime_sync_conflicts_module._runtime_snapshot_in_range(
        snapshot,
        from_dt,
        to_dt,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_sync_conflict_snapshot_from_payload(trace_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return _runtime_sync_conflicts_module._runtime_sync_conflict_snapshot_from_payload(
        trace_id,
        payload,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_sync_conflict_summary_text(conflict: dict[str, Any]) -> str:
    return _runtime_sync_conflicts_module._runtime_sync_conflict_summary_text(conflict)


def _runtime_sync_conflict_row_to_payload(row: Any) -> dict[str, Any]:
    return _runtime_sync_conflicts_module._runtime_sync_conflict_row_to_payload(
        row,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_sync_conflict_create_or_reopen(
    *,
    user_id: str,
    source: str,
    entity_ref: str,
    calendar_event_id: str,
    payload: dict[str, Any],
    kind: str = "calendar_missing",
) -> dict[str, Any]:
    return _runtime_sync_conflicts_module._runtime_sync_conflict_create_or_reopen(
        user_id=user_id,
        source=source,
        entity_ref=entity_ref,
        calendar_event_id=calendar_event_id,
        payload=payload,
        kind=kind,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_sync_conflict_get(conflict_id: str) -> dict[str, Any] | None:
    return _runtime_sync_conflicts_module._runtime_sync_conflict_get(
        conflict_id,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_sync_conflict_set_status(conflict_id: str, status: str) -> dict[str, Any] | None:
    return _runtime_sync_conflicts_module._runtime_sync_conflict_set_status(
        conflict_id,
        status,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_sync_conflict_mark_notified(conflict_id: str) -> dict[str, Any] | None:
    return _runtime_sync_conflicts_module._runtime_sync_conflict_mark_notified(
        conflict_id,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_sync_conflict_keyboard(conflict_id: str) -> dict[str, Any]:
    return _runtime_sync_conflicts_module._runtime_sync_conflict_keyboard(conflict_id)


def _runtime_sync_conflict_notify_if_needed(conflict: dict[str, Any]) -> bool:
    return _runtime_sync_conflicts_module._runtime_sync_conflict_notify_if_needed(
        conflict,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_cleanup_stale_calendar_events_for_user(user_id: str, *, limit: int = 500) -> dict[str, Any]:
    return _runtime_sync_conflicts_module._runtime_cleanup_stale_calendar_events_for_user(
        user_id,
        limit=limit,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_check_calendar_drift(
    *,
    user_id: str = "",
    from_value: Any = "",
    to_value: Any = "",
    notify: bool = True,
    limit: int = 5000,
) -> dict[str, Any]:
    return _runtime_sync_conflicts_module._runtime_check_calendar_drift(
        user_id=user_id,
        from_value=from_value,
        to_value=to_value,
        notify=notify,
        limit=limit,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_build_trace_snapshot(
    *,
    trace_id: str,
    intent: str,
    entities: dict[str, Any],
    response_payload: dict[str, Any],
    user_id: str,
) -> dict[str, Any]:
    return _runtime_sync_conflicts_module._runtime_build_trace_snapshot(
        trace_id=trace_id,
        intent=intent,
        entities=entities,
        response_payload=response_payload,
        user_id=user_id,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_trace_conflict_restore_from_snapshot(
    conflict: dict[str, Any],
    *,
    override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _runtime_sync_conflicts_module._runtime_trace_conflict_restore_from_snapshot(
        conflict,
        override=override,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_trace_find_latest_calendar_event_for_user(user_id: str, *, limit: int = 200) -> str | None:
    return _runtime_sync_conflicts_module._runtime_trace_find_latest_calendar_event_for_user(
        user_id,
        limit=limit,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_trace_list_calendar_events_for_user(user_id: str, *, limit: int = 200) -> list[str]:
    return _runtime_sync_conflicts_module._runtime_trace_list_calendar_events_for_user(
        user_id,
        limit=limit,
        deps=_runtime_sync_conflict_deps(),
    )


def _runtime_sync_conflict_apply_action(conflict_id: str, action: str) -> dict[str, Any]:
    return _runtime_sync_conflicts_module._runtime_sync_conflict_apply_action(
        conflict_id,
        action,
        deps=_runtime_sync_conflict_deps(),
    )


def _command_dedup_init(conn: sqlite3.Connection) -> None:
    _shared_runtime._command_dedup_init(conn)


def _normalize_intent_for_idempotency(raw_intent: Any) -> str:
    return _shared_runtime._normalize_intent_for_idempotency(raw_intent)


def _parse_tg_source_msg_id(raw: Any) -> tuple[str | None, str | None]:
    text = str(raw or "").strip()
    if not text:
        return None, None
    parts = text.split(":")
    if len(parts) < 3:
        return None, None
    if str(parts[0]).strip().lower() not in {"tg", "telegram"}:
        return None, None
    chat_id = str(parts[1]).strip()
    message_id = str(parts[2]).strip()
    if not (chat_id and message_id):
        return None, None
    return chat_id, message_id


def _source_channel_from_envelope(source: Any) -> str:
    if isinstance(source, dict):
        raw = source.get("channel") or source.get("source") or source.get("name") or ""
    else:
        raw = source
    value = str(raw or "").strip().lower()
    if "telegram" in value or value in {"tg", "telegram"}:
        return "telegram"
    return value


def _runtime_idempotency_key(envelope: dict) -> str | None:
    return _shared_runtime._runtime_idempotency_key(
        envelope,
        normalize_intent=_normalize_intent_for_idempotency,
    )


def _command_dedup_get(idempotency_key: str) -> dict | None:
    return _shared_runtime._command_dedup_get(idempotency_key, db_path=DB_PATH)


def _command_dedup_put(
    idempotency_key: str,
    *,
    intent: str,
    entity_type: str | None,
    entity_id: str | None,
) -> None:
    _shared_runtime._command_dedup_put(
        idempotency_key,
        intent=intent,
        entity_type=entity_type,
        entity_id=entity_id,
        db_path=DB_PATH,
    )


def _command_entity_ref(payload: dict) -> tuple[str | None, str | None]:
    if not isinstance(payload, dict):
        return None, None
    keys_map = (
        ("time_block_id", "timeblock"),
        ("task_id", "task"),
        ("subtask_id", "subtask"),
        ("goal_id", "goal"),
        ("cycle_id", "cycle"),
        ("project_id", "project"),
        ("direction_id", "direction"),
        ("regulation_id", "regulation"),
        ("regulation_run_id", "regulation_run"),
        ("event_id", "calendar_event"),
        ("calendar_event_id", "calendar_event"),
    )
    for key, entity_type in keys_map:
        value = payload.get(key)
        if value is None:
            continue
        entity_id = str(value).strip()
        if entity_id:
            return entity_type, entity_id
    debug = payload.get("debug")
    if isinstance(debug, dict):
        for key, entity_type in keys_map:
            value = debug.get(key)
            if value is None:
                continue
            entity_id = str(value).strip()
            if entity_id:
                return entity_type, entity_id
    return None, None


def _duplicate_execution_response(dedup_row: dict, *, idempotency_key: str) -> dict:
    entity_type = str(dedup_row.get("entity_type") or "").strip()
    entity_id = str(dedup_row.get("entity_id") or "").strip()
    intent = str(dedup_row.get("intent") or "").strip()
    out: dict[str, Any] = {
        "ok": True,
        "already_executed": True,
        "user_message": "Уже выполнено ранее.",
        "debug": {
            "idempotency_key": idempotency_key,
            "intent": intent,
        },
    }
    if entity_type:
        out["entity_type"] = entity_type
        out["debug"]["entity_type"] = entity_type
    if entity_id:
        out["entity_id"] = entity_id
        out["debug"]["entity_id"] = entity_id
        if entity_type == "task" and entity_id.isdigit():
            out["task_id"] = int(entity_id)
        elif entity_type == "timeblock" and entity_id.isdigit():
            out["time_block_id"] = int(entity_id)
    return out


def _runtime_flow_id(envelope: dict[str, Any], trace_id: str) -> str:
    explicit = str(envelope.get("flow_id") or "").strip()
    if explicit:
        return explicit
    source = envelope.get("source")
    if isinstance(source, dict):
        nested = str(source.get("flow_id") or "").strip()
        if nested:
            return nested
    return trace_id


def _runtime_source_timezone(envelope: dict[str, Any]) -> str:
    source = envelope.get("source")
    if isinstance(source, dict):
        tz = str(source.get("timezone") or "").strip()
        if tz:
            return tz
    return TIMEZONE_NAME


def _runtime_handler_deps() -> dict[str, Any]:
    return {
        "runtime_flow_id": _runtime_flow_id,
        "runtime_source_timezone": _runtime_source_timezone,
        "runtime_trace_dedup_get": _runtime_trace_dedup_get,
        "runtime_trace_dedup_put": _runtime_trace_dedup_put,
        "normalize_intent_for_idempotency": _normalize_intent_for_idempotency,
        "runtime_idempotency_key": _runtime_idempotency_key,
        "command_dedup_get": _command_dedup_get,
        "duplicate_execution_response": _duplicate_execution_response,
        "dispatch_intent": dispatch_intent,
        "runtime_commit_temporal_calendar": _runtime_commit_temporal_calendar,
        "runtime_commit_meeting_update_calendar": _runtime_commit_meeting_update_calendar,
        "runtime_commit_meeting_comment_update_calendar": _runtime_commit_meeting_comment_update_calendar,
        "command_entity_ref": _command_entity_ref,
        "command_dedup_put": _command_dedup_put,
        "runtime_build_trace_snapshot": _runtime_build_trace_snapshot,
    }


def _runtime_server_deps() -> dict[str, Any]:
    return {
        "handle_runtime_command": _runtime_handlers_module.handle_runtime_command,
        "runtime_handler_deps": _runtime_handler_deps,
        "runtime_meeting_source_for_user": _runtime_meeting_source_for_user,
        "runtime_meeting_search_for_user": _runtime_meeting_search_for_user,
        "runtime_cleanup_stale_calendar_events_for_user": _runtime_cleanup_stale_calendar_events_for_user,
        "runtime_check_calendar_drift": _runtime_check_calendar_drift,
        "runtime_sync_conflict_apply_action": _runtime_sync_conflict_apply_action,
        "runtime_task_search_for_user": _runtime_task_search_for_user,
        "runtime_task_duplicate_check_for_user": _runtime_task_duplicate_check_for_user,
        "runtime_timeblock_search_for_user": _runtime_timeblock_search_for_user,
    }


_MEETING_KIND_DEFAULT = "встреча"


def _normalize_meeting_kind(raw: Any) -> str:
    value = str(raw or "").strip().lower().replace("ё", "е")
    if not value:
        return ""
    if value.startswith("собран"):
        return "собрание"
    if value.startswith("созвон") or value.startswith("звон"):
        return "созвон"
    if value.startswith("мероприят") or value.startswith("ивент") or value.startswith("событ"):
        return "мероприятие"
    if value.startswith("встреч"):
        return "встреча"
    return ""


def _extract_meeting_kind_from_text(text: Any) -> str:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return ""
    if re.search(r"\bсобран\w*\b", low):
        return "собрание"
    if re.search(r"\b(созвон\w*|звонок|колл)\b", low):
        return "созвон"
    if re.search(r"\b(мероприят\w*|ивент\w*|событ\w*)\b", low):
        return "мероприятие"
    if re.search(r"\bвстреч\w*\b", low):
        return "встреча"
    return ""


def _meeting_kind_from_entities(intent: str, entities: dict[str, Any]) -> str:
    intent_norm = _normalize_intent_for_idempotency(intent)
    explicit = _normalize_meeting_kind(entities.get("meeting_kind") or entities.get("event_kind"))
    if explicit:
        return explicit
    if intent_norm == "meeting.create":
        return _MEETING_KIND_DEFAULT
    inferred = _extract_meeting_kind_from_text(entities.get("text") or entities.get("title"))
    if inferred:
        return inferred
    if "meeting" in intent_norm:
        return _MEETING_KIND_DEFAULT
    return ""


def _meeting_success_text(meeting_kind: Any, action: str) -> str:
    kind = _normalize_meeting_kind(meeting_kind) or _MEETING_KIND_DEFAULT
    action_norm = str(action or "").strip().lower()
    if action_norm == "create":
        by_kind = {
            "встреча": "Встреча создана.",
            "собрание": "Собрание создано.",
            "созвон": "Созвон создан.",
            "мероприятие": "Мероприятие создано.",
        }
    elif action_norm == "reschedule":
        by_kind = {
            "встреча": "Встреча перенесена.",
            "собрание": "Собрание перенесено.",
            "созвон": "Созвон перенесён.",
            "мероприятие": "Мероприятие перенесено.",
        }
    else:
        by_kind = {
            "встреча": "Встреча обновлена.",
            "собрание": "Собрание обновлено.",
            "созвон": "Созвон обновлён.",
            "мероприятие": "Мероприятие обновлено.",
        }
    return by_kind.get(kind, by_kind[_MEETING_KIND_DEFAULT])


def _normalize_meeting_title(raw: Any, meeting_kind: str) -> str:
    kind = _normalize_meeting_kind(meeting_kind) or _MEETING_KIND_DEFAULT
    text = str(raw or "").strip()
    if not text:
        return kind.capitalize()
    s = text.replace("ё", "е").replace("Ё", "Е")
    s = re.sub(
        r"\b(назначь|запланируй|поставь|сделай|создай|добавь|перенеси|сдвинь|поменяй|измени|нужно|надо|хочу)\b",
        " ",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(r"\b(сегодня|завтра|послезавтра)\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\b\d{1,2}[./-]\d{1,2}(?:[./-]\d{4})?\b", " ", s)
    s = re.sub(r"\b(?:в|на|к)\s*\d{1,2}(?::\d{2})?\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\bна\s+\d+\s*(?:мин|минута|минуты|минут|час|часа|часов)\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip(" ,.-")
    if not s:
        return kind.capitalize()
    s = re.sub(r"^встреч(?:у|е|и)\b", "встреча", s, flags=re.IGNORECASE)
    s = re.sub(r"^собрани(?:е|я|ю)\b", "собрание", s, flags=re.IGNORECASE)
    s = re.sub(r"^(?:созвон(?:а|е)?|звонок|колл)\b", "созвон", s, flags=re.IGNORECASE)
    s = re.sub(r"^мероприяти(?:е|я)\b", "мероприятие", s, flags=re.IGNORECASE)
    s = re.sub(
        r"^(встреча|собрание|созвон|мероприятие)\s+(?:встреч(?:а|у|е|и)|собрани(?:е|я|ю)|созвон(?:а|е)?|звонок|колл|мероприяти(?:е|я))\b",
        r"\1",
        s,
        flags=re.IGNORECASE,
    ).strip()
    if re.match(r"^(встреча|собрание|созвон|мероприятие)\b", s, flags=re.IGNORECASE):
        return s[:1].upper() + s[1:]
    return f"{kind.capitalize()} {s}".strip()


def _runtime_temporal_title(intent: str, entities: dict[str, Any]) -> str:
    meeting_kind = _meeting_kind_from_entities(intent, entities)
    intent_norm = _normalize_intent_for_idempotency(intent)
    if meeting_kind or "meeting" in intent_norm:
        explicit_title = str(entities.get("title") or "").strip()
        if explicit_title:
            return explicit_title
        source_text = str(entities.get("text") or "").strip()
        return _normalize_meeting_title(source_text, meeting_kind or _MEETING_KIND_DEFAULT)
    source_text = str(entities.get("text") or "").strip().lower()
    if MEETING_HINT_RE.search(source_text):
        return _normalize_meeting_title(source_text, _MEETING_KIND_DEFAULT)
    explicit = str(entities.get("title") or "").strip()
    if explicit:
        return explicit
    return "Блок времени"


def _runtime_duration_minutes(entities: dict[str, Any]) -> int | None:
    for key in ("duration_minutes", "duration_min", "duration_mins", "duration"):
        raw = entities.get(key)
        if raw is None:
            continue
        try:
            parsed = int(str(raw).strip())
        except Exception:
            continue
        if parsed > 0:
            return parsed
    return None


def _parse_iso_datetime_to_local(value: Any) -> datetime | None:
    return _shared_runtime._parse_iso_datetime_to_local(value, tz_offset_min=LOCAL_TZ_OFFSET_MIN)


def _parse_date_token_ymd(value: Any) -> date | None:
    return _shared_runtime._parse_date_token_ymd(value)


def _parse_time_token_hhmm(value: Any) -> tuple[int, int] | None:
    return _shared_runtime._parse_time_token_hhmm(value)
# SHARED_RUNTIME_HELPERS_END


def _require_int_field(value: Any, field_name: str) -> int:
    if value is None:
        raise ValueError(f"{field_name} is required")
    parsed = _to_int_or_none(value)
    if parsed is None:
        raise ValueError(f"{field_name} must be int")
    return parsed


def _require_lastrowid(cur: sqlite3.Cursor) -> int:
    lastrowid = cur.lastrowid
    if lastrowid is None:
        raise RuntimeError("insert failed: no rowid")
    return int(lastrowid)

def _p7_enabled() -> bool:
    return P7_MODE == "on"

def _require_p7() -> None:
    if not _p7_enabled():
        raise ValueError("P7_MODE is off")

def _parse_iso_dt(value: str) -> datetime:
    if not value:
        raise ValueError("datetime is required")
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        raise ValueError("invalid datetime")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _local_day_bounds_utc(dt_utc: datetime) -> tuple[datetime, datetime]:
    local = dt_utc.astimezone(_local_tz())
    day = local.date()
    start_local = datetime(day.year, day.month, day.day, tzinfo=_local_tz())
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

def _ensure_same_local_day(start_utc: datetime, end_utc: datetime) -> None:
    if start_utc.astimezone(_local_tz()).date() != end_utc.astimezone(_local_tz()).date():
        raise ValueError("block must fit within a single local day")


_RU_MONTHS = {
    "январ": 1, "феврал": 2, "март": 3, "апрел": 4, "ма": 5, "июн": 6, "июл": 7,
    "август": 8, "сентябр": 9, "октябр": 10, "ноябр": 11, "декабр": 12,
}
_RU_WEEKDAYS = {
    "понедельник": 0, "пн": 0,
    "вторник": 1, "вт": 1,
    "среда": 2, "ср": 2,
    "четверг": 3, "чт": 3,
    "пятница": 4, "пт": 4,
    "суббота": 5, "сб": 5,
    "воскресенье": 6, "вс": 6,
}

ORDINAL_GENITIVE_DAY = {
    "первого": 1, "второго": 2, "третьего": 3, "четвертого": 4, "пятого": 5, "шестого": 6,
    "седьмого": 7, "восьмого": 8, "девятого": 9, "десятого": 10, "одиннадцатого": 11,
    "двенадцатого": 12, "тринадцатого": 13, "четырнадцатого": 14, "пятнадцатого": 15,
    "шестнадцатого": 16, "семнадцатого": 17, "восемнадцатого": 18, "девятнадцатого": 19,
    "двадцатого": 20, "двадцать первого": 21, "двадцать второго": 22, "двадцать третьего": 23,
    "двадцать четвертого": 24, "двадцать пятого": 25, "двадцать шестого": 26, "двадцать седьмого": 27,
    "двадцать восьмого": 28, "двадцать девятого": 29, "тридцатого": 30, "тридцать первого": 31,
}
_ORDINAL_GENITIVE_RE = re.compile(
    r"\b(" + "|".join(sorted((re.escape(k) for k in ORDINAL_GENITIVE_DAY.keys()), key=len, reverse=True)) + r")\b"
)
MEETING_HINT_RE = re.compile(r"\b(встреча|созвон|звонок|совещание|митинг)\b", re.IGNORECASE)

def _clamp_day(y: int, m: int, d: int) -> int:
    # clamp day to last day of month (no external deps)
    if d < 1:
        return 1
    # hard-guard month range to avoid crashing worker
    if m < 1 or m > 12:
        # keep deterministic but safe
        m = 12 if m > 12 else 1
    if m == 12:
        next_m = date(y + 1, 1, 1)
    else:
        next_m = date(y, m + 1, 1)
    last = (next_m - timedelta(days=1)).day
    return min(d, last)


def _parse_time_ru(t: str) -> tuple[int, int] | None:
    """
    Returns (hh, mm) or None. Supports:
      'в 11', 'в 11:30', 'в 11 30', 'в 11 утра/вечера', 'в 7 часов'
    """
    if not t:
        return None
    s = t.lower()
    m = re.search(r"\bв\s*(\d{1,2})(?:\s*[:\.]\s*(\d{2})|\s+(\d{2}))?\b", s)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2) or m.group(3) or "0")
    if hh > 23 or mm > 59:
        return None

    # parts of day heuristics
    if re.search(r"\bвечер(а|ом)?\b", s) and 1 <= hh <= 11:
        hh += 12
    if re.search(r"\bдня\b", s) and 1 <= hh <= 7:
        hh += 12
    # explicit "утра" keeps as-is
    return hh, mm


def _is_time_ambiguous(t: str) -> bool:
    if not DT_REQUIRE_AMPM_FOR_SHORT_HOURS:
        return False
    tm = _parse_time_ru(t or "")
    if not tm:
        return False
    hh, _ = tm
    if re.search(r"\b(утра|вечера|дня|ночью)\b", t):
        return False
    if re.search(r"\bчас(ов|а)?\b", t):
        return False
    # B7: 1-8 ambiguous, 9-18 day auto-accept, 19-23 evening auto-accept
    if 1 <= hh <= 8:
        return True
    return False


def _parse_weekday_ru(t: str) -> int | None:
    s = (t or "").lower()
    for k, v in _RU_WEEKDAYS.items():
        if re.search(rf"\b{re.escape(k)}\b", s):
            return v
    return None


def _parse_month_ru(t: str) -> int | None:
    s = (t or "").lower()
    for k, v in _RU_MONTHS.items():
        if k in s:
            return v
    return None


def _week_start(d: date) -> date:
    # Monday as week start
    return d - timedelta(days=d.weekday())


def _add_months(d: date, delta: int) -> date:
    y = d.year
    m = d.month + delta
    while m > 12:
        y += 1
        m -= 12
    while m < 1:
        y -= 1
        m += 12
    day = _clamp_day(y, m, d.day)
    return date(y, m, day)


def _resolve_relative_period(s: str, base: datetime) -> date | None:
    """
    Resolves:
      - сегодня/завтра/послезавтра
      - на этой/прошлой/следующей неделе (+ weekday optional)
      - в этом/прошлом/следующем месяце (+ day optional)
      - в этом/прошлом/следующем году (+ month/day optional)
      - через N дней/недель/месяцев/лет
    Returns date or None.
    """
    txt = s.lower()
    base_date = base.date()

    # через N ...
    m = re.search(r"\bчерез\s+(\d{1,3})\s*(дн(я|ей)?|недел(ю|и|ь)|месяц(а|ев)?|год(а|ов)?)\b", txt)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if unit.startswith("дн"):
            return base_date + timedelta(days=n)
        if unit.startswith("недел"):
            return base_date + timedelta(days=7 * n)
        if unit.startswith("месяц"):
            return _add_months(base_date, n)
        if unit.startswith("год"):
            y = base_date.year + n
            m0 = base_date.month
            d0 = _clamp_day(y, m0, base_date.day)
            return date(y, m0, d0)

    # today/tomorrow/day after
    if "послезавтра" in txt:
        return base_date + timedelta(days=2)
    if "завтра" in txt:
        return base_date + timedelta(days=1)
    if "сегодня" in txt:
        return base_date

    # week refs
    if "недел" in txt and ("эт" in txt or "прошл" in txt or "след" in txt):
        if "прошл" in txt:
            anchor = base_date - timedelta(days=7)
        elif "след" in txt:
            anchor = base_date + timedelta(days=7)
        else:
            anchor = base_date
        ws = _week_start(anchor)
        wd = _parse_weekday_ru(txt)
        if wd is None:
            wd = DEFAULT_WEEKDAY
        return ws + timedelta(days=wd)

    # month refs
    if "месяц" in txt and ("эт" in txt or "прошл" in txt or "след" in txt):
        delta = -1 if "прошл" in txt else (1 if "след" in txt else 0)
        md = _add_months(base_date.replace(day=1), delta)  # first day of target month
        # optional day number: "в следующем месяце 12"
        mday = None
        m2 = re.search(r"\b(\d{1,2})\b", txt)
        if m2:
            mday = int(m2.group(1))
        if mday is None:
            mday = DEFAULT_MONTHDAY
        d = _clamp_day(md.year, md.month, mday)
        return date(md.year, md.month, d)

    # year refs
    if "год" in txt and ("эт" in txt or "прошл" in txt or "след" in txt):
        y = base_date.year + (-1 if "прошл" in txt else (1 if "след" in txt else 0))
        # optional month/day inside: "в следующем году 3 марта"
        mon = _parse_month_ru(txt) or DEFAULT_YEAR_MONTH
        # day: take first number found or default
        mday = None
        m2 = re.search(r"\b(\d{1,2})\b", txt)
        if m2:
            mday = int(m2.group(1))
        if mday is None:
            mday = DEFAULT_YEAR_MONTHDAY
        d = _clamp_day(y, mon, mday)
        return date(y, mon, d)

    return None


def _extract_datetime(text: str, now_local: datetime | None = None) -> datetime | None:
    """
    Minimal RU datetime extractor (deterministic).
    Supports:
      - 'сегодня|завтра|послезавтра в HH[:MM]'
      - 'в HH[:MM]' with optional 'утра|дня|вечера'
    Returns timezone-aware datetime in LOCAL_TZ.

    Examples:
      - "встреча завтра в 9" -> ambiguous -> inbox + 09:00
      - "встреча завтра в 9 утра" -> active + 09:00
      - "встреча завтра в 19" -> active + 19:00
      - "встреча завтра в 7 вечера" -> active + 19:00
      - "на следующей неделе" -> default 10:00 + inbox
      - "завтра" -> default 10:00 + inbox
      - "в 9 третьего" -> ближайшее 03-е число в 09:00
      - "третьего в 9" -> ближайшее 03-е число в 09:00
      - "четвертого в 3" -> ближайшее 04-е число в 03:00
      - "встреча 3-го в 9.30" -> ближайшее 03-е число в 09:30
      - "4-го в 15:30" -> ближайшее 04-е число в 15:30
      - "встреча третьего" -> ближайшее 03-е число в 10:00
      - "в 9.30 четвертого" -> ближайшее 04-е число в 09:30
    """
    if not text:
        return None
    # If ASR returned time as "HH.MM" (e.g. "21.16", "12.00"), treat it as time, not date.
    # This prevents crashes and wrong date parsing.
    m_time_dot = re.fullmatch(r"\s*(\d{1,2})\.(\d{2})\s*", text.strip())
    if m_time_dot:
        hh = int(m_time_dot.group(1))
        mm = int(m_time_dot.group(2))
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            now_local = datetime.now(_local_tz())
            d0 = now_local.date()
            return datetime(d0.year, d0.month, d0.day, hh, mm, tzinfo=_local_tz())

    # Also if inside a longer phrase we see "HH.MM" and it looks like time, normalize to "HH:MM"
    text_norm = re.sub(r"\b(\d{1,2})\.(\d{2})\b", r"\1:\2", text)
    text = text_norm
    t = text.strip().lower()
    t = t.replace("—", "-").replace("–", "-")

    tz = _local_tz()
    if now_local is None:
        now_local = datetime.now(tz)

    tm = _parse_time_ru(t)
    time_ambiguous = False
    if tm:
        hh, mm = tm
        time_ambiguous = _is_time_ambiguous(t)
        if time_ambiguous:
            logging.info("time_ambiguous=True text=%r", text[:200])
    else:
        hh, mm = (DEFAULT_HOUR, DEFAULT_MINUTE)
    time_explicit = tm is not None

    rel_date = _resolve_relative_period(t, now_local)
    if rel_date:
        return datetime(rel_date.year, rel_date.month, rel_date.day, hh, mm, tzinfo=tz)

    # day-of-month without month (e.g., "третьего в 9", "4-го", "4-го в 15:30")
    has_month_name = _parse_month_ru(t) is not None
    has_numeric_date = re.search(r"\b\d{1,2}[./]\d{1,2}\b", t) is not None
    if not has_month_name and not has_numeric_date:
        day = None
        m_dayw = _ORDINAL_GENITIVE_RE.search(t)
        if m_dayw:
            day = ORDINAL_GENITIVE_DAY.get(m_dayw.group(1))
        if day is None:
            m_dayn = re.search(r"\b(?P<day>\d{1,2})\s*(?:-?\s*го|ого)?\b", t)
            if m_dayn:
                day = int(m_dayn.group("day"))
            if day is not None:
                if day < 1 or day > 31:
                    return None
                base = now_local.date()
                target_year = base.year
                target_month = base.month
                if day < base.day:
                    if target_month == 12:
                        target_month = 1
                        target_year += 1
                    else:
                        target_month += 1
                d = _clamp_day(target_year, target_month, day)
                if not time_explicit:
                    return datetime(target_year, target_month, d, DEFAULT_HOUR, DEFAULT_MINUTE, tzinfo=tz)
                return datetime(target_year, target_month, d, hh, mm, tzinfo=tz)

    # explicit date: "3 марта", "12.05", "12/05/2025", etc.
    # month name in RU
    mon = _parse_month_ru(t)
    if mon:
        mday = None
        m2 = re.search(r"\b(\d{1,2})\b", t)
        if m2:
            mday = int(m2.group(1))
        if mday is None:
            mday = DEFAULT_MONTHDAY
        d = _clamp_day(now_local.year, mon, mday)
        dt = datetime(now_local.year, mon, d, hh, mm, tzinfo=tz)
        if dt <= now_local:
            dt = datetime(now_local.year + 1, mon, d, hh, mm, tzinfo=tz)
        return dt

    # numeric date with separators
    m = re.search(r"\b(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\b", t)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y_raw = m.group(3)
        y = int(y_raw) if y_raw else now_local.year
        if y < 100:
            y += 2000
        if mo < 1 or mo > 12:
            # likely not a date (or invalid)
            abs_date = None
        else:
            d = _clamp_day(y, mo, d)
            abs_date = date(y, mo, d)
        if abs_date is None:
            return None
        dt = datetime(abs_date.year, abs_date.month, abs_date.day, hh, mm, tzinfo=tz)
        if y_raw is None and time_explicit and dt <= now_local:
            y2 = now_local.year + 1
            d2 = _clamp_day(y2, mo, d)
            dt = datetime(y2, mo, d2, hh, mm, tzinfo=tz)
        return dt

    # only weekday reference, no "неделя" word
    wd = _parse_weekday_ru(t)
    if wd is not None:
        base_date = now_local.date()
        cur_wd = base_date.weekday()
        delta = (wd - cur_wd) % 7
        if delta == 0:
            delta = 7
        target = base_date + timedelta(days=delta)
        return datetime(target.year, target.month, target.day, hh, mm, tzinfo=tz)

    return None


def _selfcheck_asr_datetime() -> None:
    tz = timezone(timedelta(hours=3))
    now = datetime(2026, 2, 5, 2, 16, tzinfo=tz)
    text = "31.01 14:00 созвон с Иваном"
    dt = _extract_datetime(text, now)
    expected = datetime(2027, 1, 31, 14, 0, tzinfo=tz)
    ok = dt == expected
    logging.info("asr datetime self-check ok=%s got=%s expected=%s", ok, dt, expected)
    if not ok:
        raise RuntimeError("ASR datetime self-check failed")


def _get_conn() -> sqlite3.Connection:
    return _shared_runtime._get_conn(DB_PATH)


def _init_db() -> None:
    with _get_conn() as conn:
        _command_dedup_init(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY,
                type TEXT,
                title TEXT,
                status TEXT,
                parent_id INTEGER NULL,
                parent_id_int INTEGER NULL,
                start_at DATETIME NULL,
                end_at DATETIME NULL,
                source TEXT,
                tg_update_id INTEGER NULL,
                tg_chat_id INTEGER NULL,
                tg_message_id INTEGER NULL,
                tg_voice_file_id TEXT NULL,
                tg_voice_unique_id TEXT NULL,
                tg_voice_duration INTEGER NULL,
                asr_text TEXT NULL,
                created_at DATETIME,
                ingested_at DATETIME NULL,
                calendar_event_id TEXT NULL,
                calendar_ok_at DATETIME NULL,
                attempts INTEGER DEFAULT 0,
                last_error TEXT NULL,
                updated_at DATETIME NULL,
                tg_accepted_sent INTEGER NOT NULL DEFAULT 0,
                tg_result_sent INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        columns = {as_dict(row).get("name") for row in conn.execute("PRAGMA table_info(items)").fetchall()}
        if "parent_id" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN parent_id INTEGER NULL")
            conn.execute("CREATE INDEX IF NOT EXISTS ix_items_parent_id ON items(parent_id)")
        if "parent_id_int" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN parent_id_int INTEGER NULL")
            conn.execute(
                "UPDATE items SET parent_id_int = CAST(parent_id AS INTEGER) WHERE parent_id IS NOT NULL"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS ix_items_parent_id_int ON items(parent_id_int)")
        if "tg_update_id" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN tg_update_id INTEGER NULL")
        if "tg_chat_id" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN tg_chat_id INTEGER NULL")
        if "tg_message_id" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN tg_message_id INTEGER NULL")
        if "tg_voice_file_id" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN tg_voice_file_id TEXT NULL")
        if "tg_voice_unique_id" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN tg_voice_unique_id TEXT NULL")
        if "tg_voice_duration" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN tg_voice_duration INTEGER NULL")
        if "asr_text" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN asr_text TEXT NULL")
        if "ingested_at" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN ingested_at DATETIME NULL")
        if "calendar_event_id" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN calendar_event_id TEXT NULL")
        if "calendar_ok_at" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN calendar_ok_at DATETIME NULL")
        if "attempts" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
        if "last_error" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN last_error TEXT NULL")
        if "updated_at" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN updated_at DATETIME NULL")
        if "tg_accepted_sent" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN tg_accepted_sent INTEGER NOT NULL DEFAULT 0")
        if "tg_result_sent" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN tg_result_sent INTEGER NOT NULL DEFAULT 0")
        conn.commit()
        apply_migrations(conn)
        _assert_required_tables(
            conn,
            required=(
                "tasks",
                "subtasks",
                "time_blocks",
                "cycles",
                "goals",
                "goal_reschedule_events",
            ),
        )
        _log_worker_startup_db_facts(conn)
        columns_q = {as_dict(row).get("name") for row in conn.execute("PRAGMA table_info(inbox_queue)").fetchall()}
        if "ingested_at" not in columns_q:
            conn.execute("ALTER TABLE inbox_queue ADD COLUMN ingested_at TEXT")
            conn.commit()


def _sorted_migration_files(migrations_dir: Path) -> list[Path]:
    files = [p for p in migrations_dir.iterdir() if p.is_file() and p.suffix.lower() == ".sql"]

    def _sort_key(path: Path) -> tuple[int, str]:
        head = path.name.split("_", 1)[0]
        try:
            return (int(head), path.name)
        except Exception:
            return (10**9, path.name)

    return sorted(files, key=_sort_key)


def apply_migrations(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            name TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
        """
    )
    conn.commit()

    migrations_path = Path(MIGRATIONS_DIR)
    if not migrations_path.exists():
        raise RuntimeError(f"migrations directory not found: {migrations_path}")
    if not migrations_path.is_dir():
        raise RuntimeError(f"migrations path is not a directory: {migrations_path}")

    files = _sorted_migration_files(migrations_path)
    if not files:
        raise RuntimeError(f"no *.sql migrations found in {migrations_path}")

    for path in files:
        mig_name = path.name
        row = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE name = ?",
            (mig_name,),
        ).fetchone()
        if row:
            continue
        logging.info("applying migration: %s", mig_name)
        sql = path.read_text(encoding="utf-8")
        conn.executescript(sql)
        conn.execute(
            "INSERT INTO schema_migrations (name, applied_at) VALUES (?, ?)",
            (mig_name, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()


def _assert_required_tables(conn: sqlite3.Connection, required: tuple[str, ...]) -> None:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    existing = {str(as_dict(r).get("name") or "").strip() for r in rows}
    missing = [name for name in required if name not in existing]
    if missing:
        raise RuntimeError(f"missing required tables after migrations: {', '.join(missing)}")


def _log_calendar_sqlite_context(
    scope: str,
    conn: sqlite3.Connection | None = None,
    *,
    conn_source: str = "_get_conn()",
) -> None:
    db_path_env = os.getenv("DB_PATH")
    cwd = os.getcwd()
    logging.warning("%s DB_PATH=%s cwd=%s conn_source=%s", scope, db_path_env, cwd, conn_source)
    used_temp_conn = False
    active_conn = conn
    try:
        if active_conn is None:
            active_conn = _get_conn()
            used_temp_conn = True
        rows = active_conn.execute("PRAGMA database_list").fetchall()
        logging.warning("%s SQLITE database_list=%s used_temp_conn=%s", scope, rows, used_temp_conn)
    except Exception as exc:
        logging.warning("%s SQLITE database_list_err=%s used_temp_conn=%s", scope, str(exc)[:200], used_temp_conn)
    finally:
        if used_temp_conn and active_conn is not None:
            try:
                active_conn.close()
            except Exception:
                pass


def _log_worker_startup_db_facts(conn: sqlite3.Connection) -> None:
    db_path_env = os.getenv("DB_PATH")
    cwd = os.getcwd()
    try:
        db_list = conn.execute("PRAGMA database_list").fetchall()
    except Exception as exc:
        db_list = [f"error:{type(exc).__name__}:{str(exc)[:120]}"]
    try:
        row = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()
        tables_count = int(row[0]) if row else -1
    except Exception:
        tables_count = -1
    required = ("tasks", "subtasks", "time_blocks")
    required_presence: dict[str, bool] = {name: False for name in required}
    try:
        table_rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        existing = {str(as_dict(r).get("name") or "").strip() for r in table_rows}
        required_presence = {name: (name in existing) for name in required}
    except Exception:
        pass
    logging.info(
        "startup_db_facts DB_PATH=%s cwd=%s sqlite_database_list=%s tables_count=%s required_tables=%s",
        db_path_env,
        cwd,
        db_list,
        tables_count,
        required_presence,
    )


def reap_claims(conn: sqlite3.Connection, now_ts: float) -> tuple[int, int]:
    now_iso = datetime.fromtimestamp(now_ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%fZ")
    cur_reclaim = conn.execute(
        """
        UPDATE inbox_queue
        SET status='NEW',
            claimed_by=NULL,
            claimed_at=NULL,
            lease_until=NULL,
            updated_at=?
        WHERE status='CLAIMED'
          AND lease_until IS NOT NULL
          AND lease_until < ?
          AND attempts < ?
        """,
        (now_iso, now_iso, B2_MAX_ATTEMPTS),
    )
    cur_dead = conn.execute(
        """
        UPDATE inbox_queue
        SET status='DEAD',
            claimed_by=NULL,
            claimed_at=NULL,
            lease_until=NULL,
            updated_at=?
        WHERE status='CLAIMED'
          AND lease_until IS NOT NULL
          AND lease_until < ?
          AND attempts >= ?
        """,
        (now_iso, now_iso, B2_MAX_ATTEMPTS),
    )
    reclaimed = int(cur_reclaim.rowcount or 0)
    dead = int(cur_dead.rowcount or 0)
    if reclaimed or dead:
        logging.info("reap_claims reclaimed=%s dead=%s", reclaimed, dead)
    return reclaimed, dead


def _tg_download_voice(file_id: str) -> bytes:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN not set")
    base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    resp = requests.get(f"{base}/getFile", params={"file_id": file_id}, timeout=(3, WORKER_TG_HTTP_READ_TIMEOUT))
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError("telegram getFile failed")
    file_path = data["result"]["file_path"]
    file_resp = requests.get(
        f"{base.replace('/bot', '/file/bot')}/{file_path}",
        timeout=(3, WORKER_TG_HTTP_READ_TIMEOUT),
    )
    file_resp.raise_for_status()
    return file_resp.content


def _tg_split_chunks(text: str, limit: int = WORKER_TG_TEXT_CHUNK) -> list[str]:
    raw = str(text or "")
    if not raw:
        return [""]
    max_len = max(100, int(limit))
    chunks: list[str] = []
    rest = raw
    while len(rest) > max_len:
        cut = rest.rfind("\n", 0, max_len + 1)
        if cut <= max_len // 2:
            cut = max_len
        chunk = rest[:cut].strip()
        if not chunk:
            chunk = rest[:max_len]
            cut = max_len
        chunks.append(chunk)
        rest = rest[cut:].lstrip("\n")
    if rest:
        chunks.append(rest)
    return chunks or [raw[:max_len]]


def _tg_retry_after_seconds(exc: Exception) -> float | None:
    if not isinstance(exc, urllib.error.HTTPError):
        return None
    if int(getattr(exc, "code", 0)) != 429:
        return None
    retry_after = exc.headers.get("Retry-After") if getattr(exc, "headers", None) else None
    if not retry_after:
        return None
    try:
        return max(0.0, float(str(retry_after).strip()))
    except Exception:
        return None


def _item_set_last_error(item_id: int, err_text: str) -> None:
    err = str(err_text or "").strip()
    if not err:
        return
    try:
        with _get_conn() as conn:
            conn.execute(
                """
                UPDATE items
                SET last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (err[:500], datetime.now(timezone.utc).isoformat(), int(item_id)),
            )
            conn.commit()
    except Exception:
        logging.exception("item_last_error_update_failed item_id=%s", item_id)


def _item_clear_last_error(item_id: int) -> None:
    try:
        with _get_conn() as conn:
            conn.execute(
                """
                UPDATE items
                SET last_error = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (datetime.now(timezone.utc).isoformat(), int(item_id)),
            )
            conn.commit()
    except Exception:
        logging.exception("item_last_error_clear_failed item_id=%s", item_id)


def _tg_send_payload(
    payload: dict,
    *,
    chat_id: int,
    item_id: int | None,
    queue_id: int | None,
    stage: str,
) -> tuple[bool, int | None, str | None]:
    if not TELEGRAM_BOT_TOKEN:
        return False, None, "telegram_bot_token_missing"
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    max_attempts = max(1, TG_HTTP_RETRIES) + max(0, TG_HTTP_429_RETRIES)
    last_error = "unknown"
    last_tb = ""
    for attempt in range(1, max_attempts + 1):
        try:
            raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=raw,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=TG_HTTP_READ_TIMEOUT) as resp:
                body = resp.read()
            data = json.loads(body.decode("utf-8")) if body else {}
            if not isinstance(data, dict) or not data.get("ok"):
                last_error = f"telegram_api_not_ok:{str(data)[:200]}"
                if attempt < max_attempts:
                    time.sleep(TG_HTTP_RETRY_SLEEP)
                    continue
                break
            result = data.get("result") if isinstance(data, dict) else None
            message_id = _to_int_or_none(result.get("message_id")) if isinstance(result, dict) else None
            logging.info(
                "send_result_ok queue_id=%s item_id=%s stage=%s chat_id=%s msg_id=%s",
                queue_id,
                item_id,
                stage,
                chat_id,
                message_id,
            )
            return True, message_id, None
        except Exception as exc:
            last_error = f"{type(exc).__name__}:{str(exc)[:200]}"
            last_tb = traceback.format_exc()
            sleep_sec = TG_HTTP_RETRY_SLEEP
            retry_after = _tg_retry_after_seconds(exc)
            if retry_after is not None:
                sleep_sec = max(0.5, retry_after)
            if attempt < max_attempts:
                time.sleep(sleep_sec)
                continue
            break
    logging.error(
        "send_result_fail queue_id=%s item_id=%s stage=%s chat_id=%s err=%s traceback=%s",
        queue_id,
        item_id,
        stage,
        chat_id,
        last_error,
        last_tb[:2000],
    )
    return False, None, last_error


def _tg_send_text(
    chat_id: int,
    text: str,
    *,
    item_id: int | None = None,
    queue_id: int | None = None,
    stage: str = "notify",
    reply_markup: dict | None = None,
) -> tuple[bool, list[int], str | None]:
    chunks = _tg_split_chunks(text)
    sent_ids: list[int] = []
    total = len(chunks)
    for idx, chunk in enumerate(chunks, start=1):
        payload: dict[str, Any] = {"chat_id": chat_id, "text": chunk}
        if idx == 1 and reply_markup is not None:
            payload["reply_markup"] = reply_markup
        step = f"{stage}:{idx}/{total}" if total > 1 else stage
        logging.info(
            "send_result_start queue_id=%s item_id=%s stage=%s chat_id=%s result_len=%s",
            queue_id,
            item_id,
            step,
            chat_id,
            len(chunk),
        )
        ok, msg_id, err = _tg_send_payload(
            payload,
            chat_id=chat_id,
            item_id=item_id,
            queue_id=queue_id,
            stage=step,
        )
        if not ok:
            return False, sent_ids, err
        if msg_id is not None:
            sent_ids.append(int(msg_id))
    return True, sent_ids, None


def _tg_send_message(
    chat_id: int,
    text: str,
    *,
    item_id: int | None = None,
    queue_id: int | None = None,
    stage: str = "notify",
) -> bool:
    ok, _ids, _err = _tg_send_text(
        chat_id,
        text,
        item_id=item_id,
        queue_id=queue_id,
        stage=stage,
    )
    return ok


def _tg_send_message_with_keyboard(
    chat_id: int,
    text: str,
    reply_markup: dict,
    *,
    item_id: int | None = None,
    queue_id: int | None = None,
    stage: str = "notify_keyboard",
) -> bool:
    ok, _ids, _err = _tg_send_text(
        chat_id,
        text,
        item_id=item_id,
        queue_id=queue_id,
        stage=stage,
        reply_markup=reply_markup,
    )
    return ok


_TG_RESULT_SUCCESS = 1
_TG_RESULT_ERROR = 2
_TG_RESULT_DEAD = 4
_TG_RESULT_CREATED = 8
_CAL_NOT_CONFIGURED_REASON: str | None = None


def _tg_mark_result_sent(item_id: int, flag: int) -> bool:
    try:
        with _get_conn() as conn:
            cur = conn.execute(
                """
                UPDATE items
                SET tg_result_sent = COALESCE(tg_result_sent, 0) | ?
                WHERE id = ? AND (COALESCE(tg_result_sent, 0) & ?) = 0
                """,
                (int(flag), int(item_id), int(flag)),
            )
            conn.commit()
        return cur.rowcount == 1
    except Exception:
        return False


def _tg_mark_sent_success(
    item_id: int,
    flag: int,
    *,
    queue_id: int | None,
    chat_id: int,
    stage: str,
) -> bool:
    marked = _tg_mark_result_sent(int(item_id), int(flag))
    if marked:
        _item_clear_last_error(int(item_id))
        logging.info(
            "mark_delivered_ok queue_id=%s item_id=%s stage=%s chat_id=%s flag=%s",
            queue_id,
            item_id,
            stage,
            chat_id,
            int(flag),
        )
    else:
        logging.info(
            "mark_delivered_skip queue_id=%s item_id=%s stage=%s chat_id=%s flag=%s",
            queue_id,
            item_id,
            stage,
            chat_id,
            int(flag),
        )
    return marked


def _tg_notify_created(item_id: int, queue_id: int | None = None) -> None:
    try:
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT tg_chat_id, title, type, status, tg_result_sent FROM items WHERE id = ?",
                (int(item_id),),
            ).fetchone()
        row = as_dict(row)
        chat_id = int(row.get("tg_chat_id") or 0)
        if not chat_id:
            return
        flags = int(row.get("tg_result_sent") or 0)
        if flags & _TG_RESULT_CREATED:
            return
        item_type = str(row.get("type") or "task")
        status = str(row.get("status") or "inbox")
        logging.info(
            "result_prepare queue_id=%s item_id=%s stage=created chat_id=%s status=%s",
            queue_id,
            item_id,
            chat_id,
            status,
        )
        text = f"Создано: #{item_id} ({status})."
        if item_type == "meeting":
            text = f"Создано: #{item_id} ({status}). Поставлю в календарь."
        ok, _ids, err = _tg_send_text(
            chat_id,
            text,
            item_id=int(item_id),
            queue_id=queue_id,
            stage="created",
        )
        if not ok:
            _item_set_last_error(int(item_id), f"tg_send_created_failed:{err}")
            return
        _tg_mark_sent_success(
            int(item_id),
            _TG_RESULT_CREATED,
            queue_id=queue_id,
            chat_id=chat_id,
            stage="created",
        )
    except Exception:
        logging.exception("tg_notify_created_failed item_id=%s queue_id=%s", item_id, queue_id)
        _item_set_last_error(int(item_id), "tg_notify_created_exception")


def _tg_notify_calendar_error(item_id: int) -> None:
    return



def _format_start_at_ru(start_at: str | None) -> str | None:
    """
    Формат для пользователя: '5 фев, 10:00' (без TZ/секунд/года).
    """
    if not start_at:
        return None


def _format_start_at_local(start_at: str | None) -> str | None:
    if not start_at:
        return None
    s = str(start_at).strip()
    if not s:
        return None
    try:
        s_norm = s.replace("Z", "+00:00") if s.endswith("Z") else s
        dt = datetime.fromisoformat(s_norm)
    except Exception:
        return None
    tz = _local_tz()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc).astimezone(tz)
    else:
        dt = dt.astimezone(tz)
    return dt.strftime("%d.%m.%Y %H:%M")
    s = str(start_at).strip()
    if not s:
        return None
    try:
        s_norm = s.replace("Z", "+00:00") if s.endswith("Z") else s
        dt = datetime.fromisoformat(s_norm)
        month = ["янв","фев","мар","апр","мая","июн","июл","авг","сен","окт","ноя","дек"][dt.month - 1]
        return f"{dt.day} {month}, {dt:%H:%M}"
    except Exception:
        mm = re.match(r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})", s)
        if mm:
            day = int(mm.group(3))
            mon = int(mm.group(2))
            hh = mm.group(4)
            mi = mm.group(5)
            month = ["янв","фев","мар","апр","мая","июн","июл","авг","сен","окт","ноя","дек"][mon - 1]
            return f"{day} {month}, {hh}:{mi}"
        return None


def _compute_item_fields_from_text(text: str) -> tuple[str, str, str | None, str | None, datetime | None, bool]:
    dt = _extract_datetime(text)
    item_type = "meeting" if (dt is not None or MEETING_HINT_RE.search(text or "")) else "task"
    time_ambiguous = _is_time_ambiguous(text or "")
    has_time = _parse_time_ru(text or "") is not None
    status = "active" if (dt and has_time and not time_ambiguous) else "inbox"
    # Contract-safe behavior: do not persist implicit marker time (e.g. 06:00/10:00)
    # as actual schedule when explicit time is missing or ambiguous.
    start_at = dt.isoformat() if (dt and has_time and not time_ambiguous) else None
    end_at = (
        (dt + timedelta(minutes=MEETING_DEFAULT_MINUTES)).isoformat()
        if (dt and has_time and not time_ambiguous)
        else None
    )
    return item_type, status, start_at, end_at, dt, time_ambiguous


def _tg_notify_calendar_success(item_id: int, queue_id: int | None = None) -> None:
    try:
        _tg_notify_created(int(item_id), queue_id=queue_id)
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT tg_chat_id, title, start_at, tg_result_sent, type, status FROM items WHERE id = ?",
                (int(item_id),),
            ).fetchone()
        row = as_dict(row)
        chat_id = int(row.get("tg_chat_id") or 0)
        if not chat_id:
            return
        flags = int(row.get("tg_result_sent") or 0)
        created_sent = bool(flags & _TG_RESULT_CREATED)
        success_sent = bool(flags & _TG_RESULT_SUCCESS)
        dead_sent = bool(flags & _TG_RESULT_DEAD)
        logging.info(
            "tg notify order item_id=%s created=%s success=%s dead=%s",
            item_id,
            created_sent,
            success_sent,
            dead_sent,
        )
        if not created_sent:
            _tg_notify_created(int(item_id), queue_id=queue_id)
        if success_sent:
            return
        title = (row.get("title") or "").strip() or "без названия"
        start_at = str(row.get("start_at") or "")
        when_human = _format_start_at_local(start_at) or "без времени"
        text = f"✅ В календаре: {when_human} — {title}"
        ok, _ids, err = _tg_send_text(
            chat_id,
            text,
            item_id=int(item_id),
            queue_id=queue_id,
            stage="calendar_success",
        )
        if not ok:
            _item_set_last_error(int(item_id), f"tg_send_calendar_success_failed:{err}")
            return
        _tg_mark_sent_success(
            int(item_id),
            _TG_RESULT_SUCCESS,
            queue_id=queue_id,
            chat_id=chat_id,
            stage="calendar_success",
        )
    except Exception:
        logging.exception("tg_notify_calendar_success_failed item_id=%s queue_id=%s", item_id, queue_id)
        _item_set_last_error(int(item_id), "tg_notify_calendar_success_exception")


def _tg_notify_calendar_dead(item_id: int, queue_id: int | None = None) -> None:
    try:
        _tg_notify_created(int(item_id), queue_id=queue_id)
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT tg_chat_id, tg_result_sent, type, status FROM items WHERE id = ?",
                (int(item_id),),
            ).fetchone()
        row = as_dict(row)
        chat_id = int(row.get("tg_chat_id") or 0)
        if not chat_id:
            return
        flags = int(row.get("tg_result_sent") or 0)
        created_sent = bool(flags & _TG_RESULT_CREATED)
        success_sent = bool(flags & _TG_RESULT_SUCCESS)
        dead_sent = bool(flags & _TG_RESULT_DEAD)
        logging.info(
            "tg notify order item_id=%s created=%s success=%s dead=%s",
            item_id,
            created_sent,
            success_sent,
            dead_sent,
        )
        if not created_sent:
            _tg_notify_created(int(item_id), queue_id=queue_id)
        if dead_sent:
            return
        ok, _ids, err = _tg_send_text(
            chat_id,
            "⚠️ Не удалось добавить в календарь. Задача сохранена, верну в Inbox. [CAL-DEAD]",
            item_id=int(item_id),
            queue_id=queue_id,
            stage="calendar_dead",
        )
        if not ok:
            _item_set_last_error(int(item_id), f"tg_send_calendar_dead_failed:{err}")
            return
        _tg_mark_sent_success(
            int(item_id),
            _TG_RESULT_DEAD,
            queue_id=queue_id,
            chat_id=chat_id,
            stage="calendar_dead",
        )
    except Exception:
        logging.exception("tg_notify_calendar_dead_failed item_id=%s queue_id=%s", item_id, queue_id)
        _item_set_last_error(int(item_id), "tg_notify_calendar_dead_exception")


def _extract_ml_summary(data: dict[str, Any]) -> dict[str, Any]:
    cmd = data.get("command") if isinstance(data, dict) else None
    intent = str(cmd.get("intent") or "") if isinstance(cmd, dict) else "unknown"
    trace_id = str(data.get("trace_id") or "-")
    has_start_at = False
    duration: int | None = None
    status = "ok"
    if isinstance(cmd, dict):
        has_start_at = bool(str(cmd.get("start_at") or cmd.get("when") or "").strip())
        dur = cmd.get("duration_minutes")
        if dur is None:
            dur = cmd.get("duration_min")
        try:
            duration = int(dur) if dur is not None else None
        except Exception:
            duration = None
    if str(data.get("type") or "").strip().lower() == "clarify":
        status = "clarify"
    return {
        "trace_id": trace_id,
        "intent": intent or "unknown",
        "has_start_at": has_start_at,
        "duration": duration,
        "status": status,
    }


def _log_ml_response(data: dict[str, Any]) -> None:
    summary = _extract_ml_summary(data)
    if LOG_ML_RAW:
        logging.info("ml_raw_response trace_id=%s payload=%s", summary["trace_id"], data)
        return
    logging.info(
        "ml_response_summary trace_id=%s intent=%s has_start_at=%s duration=%s status=%s",
        summary["trace_id"],
        summary["intent"],
        summary["has_start_at"],
        summary["duration"],
        summary["status"],
    )


def _normalize_health_services(payload: dict[str, Any]) -> dict[str, str]:
    services = {"asr": "down", "llm": "down", "embedding": "down"}
    raw_services = payload.get("services")
    if isinstance(raw_services, dict):
        for key in services:
            val = str(raw_services.get(key) or "").strip().lower()
            services[key] = "ok" if val == "ok" else "down"
        return services

    # Backward compatibility with legacy payload format.
    for key in ("asr", "llm", "embedding"):
        if key in payload:
            val = str(payload.get(key) or "").strip().lower()
            services[key] = "ok" if val in {"ok", "configured", "up"} else "down"
    return services


def _ml_health_check() -> tuple[str, dict[str, str]]:
    url = f"{ML_GATEWAY_URL.rstrip('/')}/health"
    fallback = {"asr": "down", "llm": "down", "embedding": "down"}
    try:
        resp = requests.get(url, timeout=(3, 10))
        resp.raise_for_status()
    except Exception as exc:
        logging.warning("ml health check failed url=%s err=%s", url, str(exc)[:200])
        return "down", fallback

    try:
        payload = resp.json() if resp.content else {}
    except Exception as exc:
        logging.warning("ml health invalid json url=%s err=%s", url, str(exc)[:200])
        return "down", fallback

    if not isinstance(payload, dict):
        return "down", fallback

    services = _normalize_health_services(payload)
    status = str(payload.get("status") or "").strip().lower()
    if status not in {"ok", "degraded", "down"}:
        values = list(services.values())
        if all(v == "ok" for v in values):
            status = "ok"
        elif all(v == "down" for v in values):
            status = "down"
        else:
            status = "degraded"
    return status, services


def _ml_asr_transcribe(audio: bytes) -> str:
    logging.info("voice transcribe fallback via ml_gateway /asr url=%s", f"{ML_GATEWAY_URL.rstrip('/')}/asr")
    files = {"file": ("voice.ogg", audio, "audio/ogg")}
    resp = requests.post(f"{ML_GATEWAY_URL.rstrip('/')}/asr", files=files, timeout=(3, ASR_HTTP_READ_TIMEOUT))
    resp.raise_for_status()
    data = resp.json() if resp.content else {}
    if not isinstance(data, dict):
        return ""
    return str(data.get("text") or data.get("transcript") or "").strip()


def _ml_voice_command(audio: bytes) -> str:
    logging.info("voice transcribe via ml_gateway url=%s", f"{ML_GATEWAY_URL.rstrip('/')}/voice-command")
    files = {"file": ("voice.ogg", audio, "audio/ogg")}
    resp = requests.post(f"{ML_GATEWAY_URL.rstrip('/')}/voice-command", files=files, timeout=(3, ASR_HTTP_READ_TIMEOUT))
    resp.raise_for_status()
    data = resp.json() if resp.content else {}
    if isinstance(data, dict):
        _log_ml_response(data)
    if not isinstance(data, dict):
        return ""
    text = (data.get("text") or "").strip()
    if text:
        return text
    command = data.get("command")
    if isinstance(command, dict):
        return str(
            command.get("text")
            or command.get("text_normalized")
            or command.get("utterance")
            or ""
        ).strip()
    return ""

def _api_schedule_item(item_id: int, when: datetime, duration: int) -> dict:
    url = f"{ORGANIZER_API_URL}/items/{item_id}/schedule"
    resp = requests.post(
        url,
        json={"when": when.isoformat(), "duration_min": int(duration)},
        timeout=(3, TG_HTTP_READ_TIMEOUT),
    )
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, dict) else {}

def _load_clarify_state() -> dict:
    if not os.path.exists(CLARIFY_STATE_PATH):
        return {}
    try:
        with open(CLARIFY_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_clarify_state(state: dict) -> None:
    tmp_path = f"{CLARIFY_STATE_PATH}.tmp"
    os.makedirs(os.path.dirname(CLARIFY_STATE_PATH), exist_ok=True)
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)
    os.replace(tmp_path, CLARIFY_STATE_PATH)


def _prune_clarify_state(state: dict, now_ts: float) -> None:
    for cid, st in list(state.items()):
        q = (st or {}).get("queue") or []
        if not isinstance(q, list):
            state.pop(cid, None)
            continue
        new_q = [it for it in q if (it or {}).get("expires_at", 0) > now_ts]
        if new_q:
            st["queue"] = new_q
            state[cid] = st
        else:
            state.pop(cid, None)


def _get_pending_clarify(chat_id: int) -> dict | None:
    state = _load_clarify_state()
    _prune_clarify_state(state, time.time())
    st = state.get(str(chat_id)) or {}
    q = st.get("queue") or []
    return q[0] if q else None


def _clear_pending_clarify(chat_id: int) -> None:
    state = _load_clarify_state()
    st = state.get(str(chat_id)) or {}
    q = st.get("queue") or []
    if q:
        q.pop(0)
    if q:
        st["queue"] = q
        state[str(chat_id)] = st
    else:
        state.pop(str(chat_id), None)
    _save_clarify_state(state)


def _try_apply_clarification(pending: dict, text: str) -> bool:
    t = text.lower()

    if re.search(r"\b(отмена|не надо|отменить)\b", t):
        return False

    if pending.get("mode") == "no_time":
        return False
    tm = _parse_time_ru(t)
    if tm:
        hh, mm = tm
    elif "вечер" in t and pending["hh"] < 12:
        hh, mm = pending["hh"] + 12, pending["mm"]
    elif "утр" in t:
        hh, mm = pending["hh"], pending["mm"]
    else:
        return False

    d = date.fromisoformat(pending["date"])
    when = datetime(d.year, d.month, d.day, hh, mm, tzinfo=_local_tz())
    _api_schedule_item(pending["item_id"], when, pending["duration"])
    return True


def _parse_date_token(token: str, now_local: datetime) -> date | None:
    if not token:
        return None
    t = token.lower()
    if t == "сегодня":
        return now_local.date()
    if t == "завтра":
        return now_local.date() + timedelta(days=1)
    if t == "послезавтра":
        return now_local.date() + timedelta(days=2)
    m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?", t)
    if not m:
        return None
    d = int(m.group(1))
    mo = int(m.group(2))
    if mo < 1 or mo > 12:
        return None
    y = int(m.group(3)) if m.group(3) else now_local.year
    if y < 100:
        y += 2000
    d = _clamp_day(y, mo, d)
    return date(y, mo, d)


def _get_item_start_date(item_id: int) -> date | None:
    try:
        with _get_conn() as conn:
            row = conn.execute("SELECT start_at FROM items WHERE id=?", (int(item_id),)).fetchone()
        if not row:
            return None
        row = as_dict(row)
        start_at = row.get("start_at")
        if not start_at:
            return None
        dt = datetime.fromisoformat(start_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_local_tz())
        return dt.date()
    except Exception:
        return None


def _try_parse_schedule_command(text: str) -> tuple[int, datetime, int] | None:
    if not text:
        return None
    t = text.strip().lower()
    # Scheduling existing item requires explicit item reference:
    #   "#21 16:00" | "номер 21 в 16" | "для 21 завтра 9:30" | "встреча #21 в 16"
    # This avoids false positives like "встреча завтра в 8" where "8" is time, not item id.
    if not re.search(r"(#\s*\d{1,6}\b|\bномер\s+\d{1,6}\b|\bдля\s+\d{1,6}\b|\bвстреча\s+#\s*\d{1,6}\b)", t):
        return None

    m = re.search(
        r"(?:\bдля\b\s+|\bномер\b\s+|\bвстреча\b\s+)?#\s*(?P<id>\d{1,6})\b"
        r"(?:\s+(?P<date>(?:сегодня|завтра|послезавтра|\d{1,2}\.\d{1,2}(?:\.\d{2,4})?))\b)?"
        r"(?:\s+в\b)?\s+(?P<time>\d{1,2}(?::\d{2}|\.\d{2}|\s+\d{2})?)\b",
        t,
    )
    if not m:
        # Also allow "номер 21 16:00" and "для 21 16:00" without '#'
        m = re.search(
            r"(?:\bдля\b\s+|\bномер\b\s+)(?P<id>\d{1,6})\b"
            r"(?:\s+(?P<date>(?:сегодня|завтра|послезавтра|\d{1,2}\.\d{1,2}(?:\.\d{2,4})?))\b)?"
            r"(?:\s+в\b)?\s+(?P<time>\d{1,2}(?::\d{2}|\.\d{2}|\s+\d{2})?)\b",
            t,
        )
        if not m:
            return None
    item_id = int(m.group("id"))
    time_tok = m.group("time")
    if not time_tok:
        return None
    time_tok = time_tok.replace(".", ":").replace(" ", ":")
    hh_mm = time_tok.split(":")
    hh = int(hh_mm[0])
    mm = int(hh_mm[1]) if len(hh_mm) == 2 else 0
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return None

    tz = _local_tz()
    now_local = datetime.now(tz)
    date_tok = m.group("date")
    if date_tok:
        target_date = _parse_date_token(date_tok, now_local)
        if target_date is None:
            return None
    else:
        target_date = _get_item_start_date(item_id) or now_local.date()

    when = datetime(target_date.year, target_date.month, target_date.day, hh, mm, tzinfo=tz)
    return item_id, when, MEETING_DEFAULT_MINUTES


def _looks_like_schedule_intent(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    # IMPORTANT: do NOT treat generic meeting phrases as reschedule-intent.
    # Only explicit existing-item references are reschedule-intent.
    return re.search(r"(#\s*\d{1,6}\b|\bномер\s+\d{1,6}\b|\bдля\s+\d{1,6}\b|\bвстреча\s+#\s*\d{1,6}\b)", t) is not None


def _extract_first_item_ref(text: str) -> int | None:
    t = (text or "").lower()
    m = re.search(r"#\s*(\d{1,6})\b", t)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(?:номер|для)\s+(\d{1,6})\b", t)
    if m:
        return int(m.group(1))
    return None


def _insert_item_from_text(
    text: str,
    source: str,
    ingested_at: str | None,
    tg_chat_id: int | None,
    tg_message_id: int | None,
    queue_id: int | None = None,
    tg_update_id: int | None = None,
    tg_voice_file_id: str | None = None,
    tg_voice_unique_id: str | None = None,
    tg_voice_duration: int | None = None,
    asr_text: str | None = None,
) -> tuple[int, str, str]:
    item_type, status, start_at, end_at, _, _ = _compute_item_fields_from_text(text)

    created_at = datetime.now(timezone.utc).isoformat()
    ingested_at = ingested_at or created_at
    with _get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO items (
                type, title, status, start_at, end_at, source,
                tg_update_id, tg_chat_id, tg_message_id,
                tg_voice_file_id, tg_voice_unique_id, tg_voice_duration,
                asr_text,
                tg_accepted_sent, tg_result_sent,
                created_at, ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
            """,
            (
                item_type,
                text.strip(),
                status,
                start_at,
                end_at,
                source,
                tg_update_id,
                tg_chat_id,
                tg_message_id,
                tg_voice_file_id,
                tg_voice_unique_id,
                tg_voice_duration,
                asr_text,
                created_at,
                ingested_at,
            ),
        )
        conn.commit()
        last_id = cur.lastrowid
        if last_id is None:
            raise RuntimeError("insert failed: no rowid")
        item_id = int(last_id)
        _tg_notify_created(item_id, queue_id=queue_id)
        return item_id, item_type, status


def _insert_voice_placeholder(
    source: str,
    ingested_at: str | None,
    tg_chat_id: int | None,
    tg_message_id: int | None,
    tg_update_id: int | None,
    tg_voice_file_id: str | None,
    tg_voice_unique_id: str | None,
    tg_voice_duration: int | None,
    queue_id: int | None = None,
) -> int:
    created_at = datetime.now(timezone.utc).isoformat()
    ingested_at = ingested_at or created_at
    with _get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO items (
                type, title, status, start_at, end_at, source,
                tg_update_id, tg_chat_id, tg_message_id,
                tg_voice_file_id, tg_voice_unique_id, tg_voice_duration,
                tg_accepted_sent, tg_result_sent,
                created_at, ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
            """,
            (
                "task",
                "",
                "inbox",
                None,
                None,
                source,
                tg_update_id,
                tg_chat_id,
                tg_message_id,
                tg_voice_file_id,
                tg_voice_unique_id,
                tg_voice_duration,
                created_at,
                ingested_at,
            ),
        )
        conn.commit()
        last_id = cur.lastrowid
        if last_id is None:
            raise RuntimeError("insert failed: no rowid")
        item_id = int(last_id)
        _tg_notify_created(item_id, queue_id=queue_id)
        return item_id


def _update_item_from_asr(
    item_id: int,
    text: str,
    item_type: str,
    status: str,
    start_at: str | None,
    end_at: str | None,
) -> None:
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE items
            SET title = ?,
                type = ?,
                status = ?,
                start_at = ?,
                end_at = ?,
                asr_text = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                text.strip(),
                item_type,
                status,
                start_at,
                end_at,
                text.strip(),
                datetime.now(timezone.utc).isoformat(),
                int(item_id),
            ),
        )
        conn.commit()


def _ensure_voice_meta(
    item_id: int,
    tg_update_id: int | None,
    tg_message_id: int | None,
    tg_voice_file_id: str | None,
    tg_voice_unique_id: str | None,
    tg_voice_duration: int | None,
) -> None:
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE items
            SET tg_update_id = COALESCE(tg_update_id, ?),
                tg_message_id = COALESCE(tg_message_id, ?),
                tg_voice_file_id = COALESCE(tg_voice_file_id, ?),
                tg_voice_unique_id = COALESCE(tg_voice_unique_id, ?),
                tg_voice_duration = COALESCE(tg_voice_duration, ?),
                updated_at = ?
            WHERE id = ?
            """,
            (
                tg_update_id,
                tg_message_id,
                tg_voice_file_id,
                tg_voice_unique_id,
                tg_voice_duration,
                datetime.now(timezone.utc).isoformat(),
                int(item_id),
            ),
        )
        conn.commit()


def _mark_item_failed_asr_dedup(item_id: int) -> None:
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE items
            SET status = 'inbox',
                start_at = NULL,
                end_at = NULL,
                calendar_event_id = NULL,
                last_error = 'FAILED_ASR_DEDUP',
                updated_at = ?
            WHERE id = ?
            """,
            (datetime.now(timezone.utc).isoformat(), int(item_id)),
        )
        conn.commit()


def _log_voice_meta(
    label: str,
    item_id: int | None,
    tg_update_id: int | None,
    tg_message_id: int | None,
    tg_voice_unique_id: str | None,
    tg_voice_duration: int | None,
    queue_id: int | None,
) -> None:
    logging.info(
        "%s item_id=%s upd=%s msg=%s uniq=%s dur=%s queue_id=%s",
        label,
        item_id,
        tg_update_id,
        tg_message_id,
        tg_voice_unique_id,
        tg_voice_duration,
        queue_id,
    )


def _get_parent_id_from_row(row: dict) -> int | None:
    parent_id_int = _to_int_or_none(row.get("parent_id_int"))
    if parent_id_int is not None:
        return parent_id_int
    return _to_int_or_none(row.get("parent_id"))


def _p2_task_row(task_id: int) -> dict:
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
    row = as_dict(row)
    if not row:
        raise ValueError("task not found")
    return row


def _p2_subtask_row(subtask_id: int) -> dict:
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
    row = as_dict(row)
    if not row:
        raise ValueError("subtask not found")
    return row


def _p2_direction_row(direction_id: int) -> dict:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, title, note, status, source_msg_id, created_at, updated_at
            FROM directions
            WHERE id = ?
            """,
            (int(direction_id),),
        ).fetchone()
    row = as_dict(row)
    if not row:
        raise ValueError("direction not found")
    return row


def _p2_project_row(project_id: int) -> dict:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, direction_id, title, status, source_msg_id, created_at, updated_at, closed_at
            FROM projects
            WHERE id = ?
            """,
            (int(project_id),),
        ).fetchone()
    row = as_dict(row)
    if not row:
        raise ValueError("project not found")
    return row


def _p2_cycle_row(cycle_id: int) -> dict:
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
    row = as_dict(row)
    if not row:
        raise ValueError("cycle not found")
    return row


def _p2_cycle_outcome_row(outcome_id: int) -> dict:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, cycle_id, kind, text, created_at
            FROM cycle_outcomes
            WHERE id = ?
            """,
            (int(outcome_id),),
        ).fetchone()
    row = as_dict(row)
    if not row:
        raise ValueError("cycle outcome not found")
    return row


def _p2_cycle_goal_row(goal_id: int) -> dict:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, cycle_id, text, status, continued_from_goal_id, created_at, updated_at
            FROM cycle_goals
            WHERE id = ?
            """,
            (int(goal_id),),
        ).fetchone()
    row = as_dict(row)
    if not row:
        raise ValueError("cycle goal not found")
    return row


def _p4_regulation_row(regulation_id: int) -> dict:
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
    row = as_dict(row)
    if not row:
        raise ValueError("regulation not found")
    return row


def _p4_regulation_run_row(run_id: int) -> dict:
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
    row = as_dict(row)
    if not row:
        raise ValueError("regulation_run not found")
    return row


def _p7_time_block_row(block_id: int) -> dict:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, task_id, start_at, end_at, created_at
            FROM time_blocks
            WHERE id = ?
            """,
            (int(block_id),),
        ).fetchone()
    row = as_dict(row)
    if not row:
        raise ValueError("time_block not found")
    return row


def _p7_task_exists(task_id: int) -> None:
    with _get_conn() as conn:
        row = conn.execute("SELECT 1 FROM tasks WHERE id = ?", (int(task_id),)).fetchone()
    if not row:
        raise ValueError("task not found")


def _p7_check_overlap(
    conn: sqlite3.Connection,
    start_utc: datetime,
    end_utc: datetime,
    exclude_id: int | None = None,
) -> None:
    day_start, day_end = _local_day_bounds_utc(start_utc)
    params: list[Any] = [
        end_utc.isoformat(),
        start_utc.isoformat(),
        day_end.isoformat(),
        day_start.isoformat(),
    ]
    sql = (
        """
        SELECT id
        FROM time_blocks
        WHERE start_at < ? AND end_at > ?
          AND start_at < ? AND end_at > ?
        """
    )
    if exclude_id is not None:
        sql += " AND id != ?"
        params.append(int(exclude_id))
    row = conn.execute(sql, params).fetchone()
    if row:
        raise ValueError("time_block overlap")


def _normalize_parent_type(parent_type: str | None) -> str | None:
    if parent_type is None:
        return None
    s = str(parent_type).strip().lower()
    if not s or s == "none":
        return None
    if s not in {"project", "cycle", "regulation_run"}:
        raise ValueError("invalid parent_type")
    return s


def cmd_create_task(
    title: str,
    source_msg_id: str | None = None,
    parent_type: str | None = None,
    parent_id: int | None = None,
) -> dict:
    parent_type_norm = _normalize_parent_type(parent_type)
    if parent_type_norm and parent_id is None:
        raise ValueError("parent_id is required")
    if parent_type_norm is None and parent_id is not None:
        raise ValueError("parent_type is required when parent_id is set")
    task = p2.create_task(
        title,
        status="NEW",
        source_msg_id=source_msg_id,
        parent_type=parent_type_norm,
        parent_id=int(parent_id) if parent_id is not None else None,
    )
    return _p2_task_row(task.id)


def cmd_create_subtask(
    task_id: int,
    title: str,
    status: str,
    source_msg_id: str | None = None,
) -> dict:
    sub = p2.create_subtask(task_id, title, status=status, source_msg_id=source_msg_id)
    return _p2_subtask_row(sub.id)


def cmd_complete_subtask(subtask_id: int) -> dict:
    sub = p2.complete_subtask(subtask_id)
    return _p2_subtask_row(sub.id)


def cmd_complete_task(task_id: int) -> dict:
    task = p2.complete_task(task_id)
    return _p2_task_row(task.id)


def cmd_plan_task(task_id: int, planned_at: str) -> dict:
    task = p2.plan_task(task_id, planned_at)
    return _p2_task_row(task.id)


def cmd_create_direction(title: str, note: str | None, source_msg_id: str | None) -> dict:
    direction = p2.create_direction(title, note=note, source_msg_id=source_msg_id)
    return _p2_direction_row(direction.id)


def cmd_create_project(
    title: str,
    direction_id: int | None,
    source_msg_id: str | None,
) -> dict:
    project = p2.create_project(title, direction_id=direction_id, source_msg_id=source_msg_id)
    return _p2_project_row(project.id)


def cmd_convert_direction_to_project(
    direction_id: int,
    title: str | None,
    source_msg_id: str | None,
) -> dict:
    project = p2.convert_direction_to_project(direction_id, title=title, source_msg_id=source_msg_id)
    return _p2_project_row(project.id)


def cmd_start_cycle(type: str, period_key: str | None, source_msg_id: str | None) -> dict:
    cycle = p2.start_cycle(type, period_key=period_key, source_msg_id=source_msg_id)
    return _p2_cycle_row(cycle.id)


def cmd_close_cycle(
    cycle_id: int,
    status: str,
    summary: str | None,
    source_msg_id: str | None,
) -> dict:
    _ = source_msg_id
    cycle = p2.close_cycle(cycle_id, status=status, summary=summary)
    return _p2_cycle_row(cycle.id)


def cmd_add_cycle_outcome(
    cycle_id: int,
    kind: str,
    text: str,
    source_msg_id: str | None,
) -> dict:
    _ = source_msg_id
    outcome = p2.add_cycle_outcome(cycle_id, kind=kind, text=text)
    return _p2_cycle_outcome_row(outcome.id)


def cmd_add_cycle_goal(
    cycle_id: int,
    text: str,
    source_msg_id: str | None,
) -> dict:
    _ = source_msg_id
    goal = p2.add_cycle_goal(cycle_id, text=text)
    return _p2_cycle_goal_row(goal.id)


def cmd_continue_cycle_goal(
    goal_id: int,
    target_cycle_id: int,
    source_msg_id: str | None,
) -> dict:
    _ = source_msg_id
    goal = p2.continue_cycle_goal(goal_id, target_cycle_id)
    return _p2_cycle_goal_row(goal.id)


def cmd_update_cycle_goal_status(
    goal_id: int,
    status: str,
    source_msg_id: str | None,
) -> dict:
    _ = source_msg_id
    goal = p2.update_cycle_goal_status(goal_id, status=status)
    return _p2_cycle_goal_row(goal.id)


def cmd_create_regulation(
    title: str,
    day_of_month: int,
    note: str | None,
    due_time_local: str | None,
    source_msg_id: str | None,
) -> dict:
    reg = p2.create_regulation(
        title=title,
        day_of_month=day_of_month,
        note=note,
        due_time_local=due_time_local,
        source_msg_id=source_msg_id,
    )
    return _p4_regulation_row(reg.id)


def cmd_archive_regulation(regulation_id: int, source_msg_id: str | None) -> dict:
    _ = source_msg_id
    reg = p2.archive_regulation(regulation_id)
    return _p4_regulation_row(reg.id)


def cmd_update_regulation_schedule(
    regulation_id: int,
    day_of_month: int | None,
    due_time_local: str | None,
    source_msg_id: str | None,
) -> dict:
    _ = source_msg_id
    reg = p2.update_regulation_schedule(
        regulation_id,
        day_of_month=day_of_month,
        due_time_local=due_time_local,
    )
    return _p4_regulation_row(reg.id)


def cmd_ensure_regulation_runs(
    user_id: str | None,
    period_key: str,
    source_msg_id: str | None,
) -> dict:
    _ = user_id
    _ = source_msg_id
    runs = p2.ensure_regulation_runs(period_key)
    return {
        "period_key": period_key,
        "runs": [
            {
                "id": r.id,
                "regulation_id": r.regulation_id,
                "period_key": r.period_key,
                "status": r.status,
                "due_date": r.due_date,
                "due_time_local": r.due_time_local,
                "done_at": r.done_at,
                "created_at": r.created_at,
                "updated_at": r.updated_at,
            }
            for r in runs
        ],
    }


def cmd_mark_regulation_done(
    run_id: int,
    done_at: str | None,
    source_msg_id: str | None,
) -> dict:
    _ = source_msg_id
    run = p2.mark_regulation_done(run_id, done_at=done_at)
    return _p4_regulation_run_row(run.id)


def cmd_complete_reg_run(
    run_id: int,
    done_at: str | None,
    source_msg_id: str | None,
) -> dict:
    _ = source_msg_id
    run = p2.complete_regulation_run(run_id, done_at=done_at)
    return _p4_regulation_run_row(run.id)


def cmd_skip_reg_run(
    run_id: int,
    source_msg_id: str | None,
) -> dict:
    _ = source_msg_id
    run = p2.skip_regulation_run(run_id)
    return _p4_regulation_run_row(run.id)


def cmd_disable_reg(
    regulation_id: int,
    source_msg_id: str | None,
) -> dict:
    _ = source_msg_id
    reg = p2.disable_regulation(regulation_id)
    return _p4_regulation_row(reg.id)


def cmd_add_block(
    task_id: int,
    start_at: str,
    end_at: str,
    source_msg_id: str | None = None,
) -> dict:
    _require_p7()
    _ = source_msg_id
    _p7_task_exists(task_id)
    start_utc = _parse_iso_dt(start_at)
    end_utc = _parse_iso_dt(end_at)
    if end_utc <= start_utc:
        raise ValueError("start_at must be < end_at")
    _ensure_same_local_day(start_utc, end_utc)
    with _get_conn() as conn:
        _p7_check_overlap(conn, start_utc, end_utc)
        created_at = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            """
            INSERT INTO time_blocks (task_id, start_at, end_at, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (int(task_id), start_utc.isoformat(), end_utc.isoformat(), created_at),
        )
        conn.commit()
        block_id = int(cur.lastrowid or 0)
    return _p7_time_block_row(block_id)


def cmd_move_block(
    block_id: int,
    delta_minutes: int,
    source_msg_id: str | None = None,
) -> dict:
    _require_p7()
    _ = source_msg_id
    if delta_minutes not in {-10, 10}:
        raise ValueError("delta_minutes must be -10 or 10")
    block = _p7_time_block_row(block_id)
    start_utc = _parse_iso_dt(str(block.get("start_at") or ""))
    end_utc = _parse_iso_dt(str(block.get("end_at") or ""))
    start_utc = start_utc + timedelta(minutes=delta_minutes)
    end_utc = end_utc + timedelta(minutes=delta_minutes)
    if end_utc <= start_utc:
        raise ValueError("start_at must be < end_at")
    _ensure_same_local_day(start_utc, end_utc)
    with _get_conn() as conn:
        _p7_check_overlap(conn, start_utc, end_utc, exclude_id=block_id)
        conn.execute(
            """
            UPDATE time_blocks
            SET start_at = ?, end_at = ?
            WHERE id = ?
            """,
            (start_utc.isoformat(), end_utc.isoformat(), int(block_id)),
        )
        conn.commit()
    return _p7_time_block_row(block_id)


def cmd_delete_block(
    block_id: int,
    source_msg_id: str | None = None,
) -> dict:
    _require_p7()
    _ = source_msg_id
    _ = _p7_time_block_row(block_id)
    with _get_conn() as conn:
        conn.execute("DELETE FROM time_blocks WHERE id = ?", (int(block_id),))
        conn.commit()
    return {"deleted": True, "block_id": int(block_id)}


def _ensure_user_settings(user_id: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    next_at = (datetime.now(timezone.utc) + timedelta(days=90)).isoformat()
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT user_id, signals_enabled, overload_enabled, drift_enabled, created_at, updated_at "
            "FROM user_settings WHERE user_id = ?",
            (str(user_id),),
        ).fetchone()
        if not row:
            conn.execute(
                """
                INSERT INTO user_settings (user_id, signals_enabled, overload_enabled, drift_enabled, created_at, updated_at)
                VALUES (?, 0, 0, 0, ?, ?)
                """,
                (str(user_id), now, now),
            )
            conn.execute(
                """
                INSERT INTO user_nudges (user_id, nudge_key, next_at, last_shown_at, created_at, updated_at)
                VALUES (?, ?, ?, NULL, ?, ?)
                """,
                (str(user_id), NUDGE_SIGNALS_KEY, next_at, now, now),
            )
            conn.commit()
            row = conn.execute(
                "SELECT user_id, signals_enabled, overload_enabled, drift_enabled, created_at, updated_at "
                "FROM user_settings WHERE user_id = ?",
                (str(user_id),),
            ).fetchone()
        else:
            # migrate from signals_enabled if new fields are missing or zeroed
            try:
                sig = int(row["signals_enabled"] or 0)
            except Exception:
                sig = 0
            try:
                overload_enabled = int(row["overload_enabled"] or 0)
            except Exception:
                overload_enabled = 0
            try:
                drift_enabled = int(row["drift_enabled"] or 0)
            except Exception:
                drift_enabled = 0
            if sig and (overload_enabled == 0 or drift_enabled == 0):
                conn.execute(
                    """
                    UPDATE user_settings
                    SET overload_enabled = ?,
                        drift_enabled = ?,
                        updated_at = ?
                    WHERE user_id = ?
                    """,
                    (sig, sig, now, str(user_id)),
                )
                conn.commit()
            # ensure nudge exists
            nudge = conn.execute(
                """
                SELECT user_id FROM user_nudges WHERE user_id = ? AND nudge_key = ?
                """,
                (str(user_id), NUDGE_SIGNALS_KEY),
            ).fetchone()
            if not nudge:
                conn.execute(
                    """
                    INSERT INTO user_nudges (user_id, nudge_key, next_at, last_shown_at, created_at, updated_at)
                    VALUES (?, ?, ?, NULL, ?, ?)
                    """,
                    (str(user_id), NUDGE_SIGNALS_KEY, next_at, now, now),
                )
                conn.commit()
    row = as_dict(row)
    return {
        "user_id": row.get("user_id"),
        "signals_enabled": int(row.get("signals_enabled") or 0),
        "overload_enabled": int(row.get("overload_enabled") or 0),
        "drift_enabled": int(row.get("drift_enabled") or 0),
    }


def cmd_set_signals_enabled(user_id: str, enabled: int) -> dict:
    enabled_val = 1 if int(enabled) else 0
    now = datetime.now(timezone.utc).isoformat()
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT user_id, signals_enabled, overload_enabled, drift_enabled FROM user_settings WHERE user_id = ?",
            (str(user_id),),
        ).fetchone()
        if not row:
            conn.execute(
                """
                INSERT INTO user_settings (user_id, signals_enabled, overload_enabled, drift_enabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (str(user_id), enabled_val, enabled_val, enabled_val, now, now),
            )
        else:
            if int(row["signals_enabled"] or 0) != enabled_val:
                conn.execute(
                    """
                    UPDATE user_settings
                    SET signals_enabled = ?,
                        overload_enabled = ?,
                        drift_enabled = ?,
                        updated_at = ?
                    WHERE user_id = ?
                    """,
                    (enabled_val, enabled_val, enabled_val, now, str(user_id)),
                )
        conn.commit()
        row = conn.execute(
            "SELECT user_id, signals_enabled, overload_enabled, drift_enabled FROM user_settings WHERE user_id = ?",
            (str(user_id),),
        ).fetchone()
    row = as_dict(row)
    return {
        "user_id": row.get("user_id"),
        "signals_enabled": int(row.get("signals_enabled") or 0),
        "overload_enabled": int(row.get("overload_enabled") or 0),
        "drift_enabled": int(row.get("drift_enabled") or 0),
    }


def cmd_set_module_enabled(user_id: str, module: str, enabled: int) -> dict:
    mod = (module or "").strip().lower()
    if mod not in {"overload", "drift"}:
        raise ValueError("invalid module")
    enabled_val = 1 if int(enabled) else 0
    now = datetime.now(timezone.utc).isoformat()
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT user_id, overload_enabled, drift_enabled FROM user_settings WHERE user_id = ?",
            (str(user_id),),
        ).fetchone()
        if not row:
            overload_val = enabled_val if mod == "overload" else 0
            drift_val = enabled_val if mod == "drift" else 0
            conn.execute(
                """
                INSERT INTO user_settings (user_id, signals_enabled, overload_enabled, drift_enabled, created_at, updated_at)
                VALUES (?, 0, ?, ?, ?, ?)
                """,
                (str(user_id), overload_val, drift_val, now, now),
            )
        else:
            if mod == "overload":
                conn.execute(
                    """
                    UPDATE user_settings
                    SET overload_enabled = ?,
                        updated_at = ?
                    WHERE user_id = ?
                    """,
                    (enabled_val, now, str(user_id)),
                )
            else:
                conn.execute(
                    """
                    UPDATE user_settings
                    SET drift_enabled = ?,
                        updated_at = ?
                    WHERE user_id = ?
                    """,
                    (enabled_val, now, str(user_id)),
                )
        conn.commit()
        row = conn.execute(
            "SELECT user_id, overload_enabled, drift_enabled FROM user_settings WHERE user_id = ?",
            (str(user_id),),
        ).fetchone()
    row = as_dict(row)
    return {
        "user_id": row.get("user_id"),
        "overload_enabled": int(row.get("overload_enabled") or 0),
        "drift_enabled": int(row.get("drift_enabled") or 0),
    }


def cmd_set_modules_enabled_bulk(user_id: str, overload_enabled: int, drift_enabled: int) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT user_id FROM user_settings WHERE user_id = ?",
            (str(user_id),),
        ).fetchone()
        if not row:
            conn.execute(
                """
                INSERT INTO user_settings (user_id, signals_enabled, overload_enabled, drift_enabled, created_at, updated_at)
                VALUES (?, 0, ?, ?, ?, ?)
                """,
                (str(user_id), int(overload_enabled), int(drift_enabled), now, now),
            )
        else:
            conn.execute(
                """
                UPDATE user_settings
                SET overload_enabled = ?,
                    drift_enabled = ?,
                    updated_at = ?
                WHERE user_id = ?
                """,
                (int(overload_enabled), int(drift_enabled), now, str(user_id)),
            )
        conn.commit()
        row = conn.execute(
            "SELECT user_id, overload_enabled, drift_enabled FROM user_settings WHERE user_id = ?",
            (str(user_id),),
        ).fetchone()
    row = as_dict(row)
    return {
        "user_id": row.get("user_id"),
        "overload_enabled": int(row.get("overload_enabled") or 0),
        "drift_enabled": int(row.get("drift_enabled") or 0),
    }


def cmd_snooze_nudge(user_id: str, nudge_key: str, days: int) -> dict:
    now = datetime.now(timezone.utc)
    target = now + timedelta(days=int(days))
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT user_id, nudge_key, next_at
            FROM user_nudges
            WHERE user_id = ? AND nudge_key = ?
            """,
            (str(user_id), str(nudge_key)),
        ).fetchone()
        if not row:
            conn.execute(
                """
                INSERT INTO user_nudges (user_id, nudge_key, next_at, last_shown_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(user_id),
                    str(nudge_key),
                    target.isoformat(),
                    now.isoformat(),
                    now.isoformat(),
                    now.isoformat(),
                ),
            )
        else:
            try:
                existing = datetime.fromisoformat(str(row["next_at"]))
            except Exception:
                existing = None
            next_use = existing if (existing and existing >= target) else target
            conn.execute(
                """
                UPDATE user_nudges
                SET next_at = ?,
                    last_shown_at = ?,
                    updated_at = ?
                WHERE user_id = ? AND nudge_key = ?
                """,
                (
                    next_use.isoformat(),
                    now.isoformat(),
                    now.isoformat(),
                    str(user_id),
                    str(nudge_key),
                ),
            )
        conn.commit()
        row = conn.execute(
            """
            SELECT user_id, nudge_key, next_at, last_shown_at
            FROM user_nudges
            WHERE user_id = ? AND nudge_key = ?
            """,
            (str(user_id), str(nudge_key)),
        ).fetchone()
    row = as_dict(row)
    return {
        "user_id": row.get("user_id"),
        "nudge_key": row.get("nudge_key"),
        "next_at": row.get("next_at"),
        "last_shown_at": row.get("last_shown_at"),
    }


_CommandHandler = _runtime_server_module.make_command_handler(_runtime_server_deps)


def _start_command_server() -> None:
    _runtime_server_module.start_command_server(WORKER_COMMAND_PORT, _CommandHandler)
# ACTIVE_RUNTIME_END


def validate_task_status(item: dict, new_status: str, open_subtasks: int) -> None:
    """
    Validate task/subtask status transitions.
    - task: inbox -> active -> done -> archived
    - subtask: todo -> done
    - task cannot be done if any subtask is not done
    """
    if item.get("type") != "task":
        return
    is_subtask = _get_parent_id_from_row(item) is not None
    if is_subtask:
        allowed = {"todo": {"done"}, "done": set()}
    else:
        allowed = {"inbox": {"active"}, "active": {"done"}, "done": {"archived"}, "archived": set()}

    current = str(item.get("status") or "")
    if new_status == current:
        return
    if current not in allowed or new_status not in allowed[current]:
        raise ValueError(f"invalid status transition: {current} -> {new_status}")

    if not is_subtask and new_status == "done" and open_subtasks > 0:
        raise ValueError("cannot complete task with open subtasks")


# LEGACY_CANDIDATE: queue/items flow below is not part of runtime_core_direct request routing.
# Active production write path is /runtime/command; verify before removal or extraction.
def _process_queue_item(row: dict) -> None:
    row = as_dict(row)
    queue_id = row["id"]
    chat_id = int(row.get("tg_chat_id") or 0)
    message_id = row.get("tg_message_id")
    attempts = int(row.get("attempts") or 0)
    logging.info(
        "queue_process_start queue_id=%s kind=%s status=%s attempts=%s chat_id=%s",
        queue_id,
        row.get("kind"),
        row.get("status"),
        attempts,
        chat_id,
    )
    try:
        payload = json.loads(row.get("payload_json") or "{}")
        kind = row.get("kind")
        if kind in ("text", "clarify_reply"):
            text = (payload.get("text") or "").strip()
            if not text or len(text) < 2:
                raise RuntimeError("empty text")
            time_ambiguous = _is_time_ambiguous(text)
            dt = _extract_datetime(text)
            pending = _get_pending_clarify(chat_id)
            if pending:
                if _try_apply_clarification(pending, text):
                    _clear_pending_clarify(chat_id)
                    _queue_mark(queue_id, "DONE", None)
                    if chat_id:
                        _tg_send_message(chat_id, "Ок, время встречи обновил.")
                    return
                if re.search(r"\b(отмена|не надо|отменить)\b", text.lower()):
                    _clear_pending_clarify(chat_id)
                    _queue_mark(queue_id, "DONE", None)
                    if chat_id:
                        _tg_send_message(chat_id, "Хорошо, отменил уточнение.")
                    return
                _queue_mark(queue_id, "DONE", None)
                if chat_id:
                    _tg_send_message(chat_id, "Есть ожидающее уточнение. Ответь: утро/вечер или /set #ID 16:00.")
                return
            ingested_at = row.get("ingested_at") or row.get("created_at")
            item_id, item_type, item_status = _insert_item_from_text(
                text,
                "telegram",
                ingested_at,
                int(chat_id) if chat_id else None,
                int(message_id) if message_id is not None else None,
                queue_id=int(queue_id),
                tg_update_id=_to_int_or_none(row.get("tg_update_id")),
            )
            logging.info("tg meta kind=%s item_id=%s tg_chat_id=%s", kind, item_id, chat_id)
            try:
                with _get_conn() as conn:
                    row_item = conn.execute(
                        "SELECT id, title, type, start_at, end_at, calendar_event_id, parent_id, parent_id_int FROM items WHERE id=?",
                        (item_id,),
                    ).fetchone()
                row_item = as_dict(row_item)
                if (
                    row_item
                    and row_item.get("type") == "meeting"
                    and row_item.get("start_at")
                    and row_item.get("end_at")
                ):
                    _sync_calendar_for_item(row_item)
            except Exception as exc:
                rid = row_item.get("id") if row_item else None
                logging.warning("calendar sync failed item_id=%s err=%s", rid, str(exc)[:200])
            _queue_mark(queue_id, "DONE", None)
            logging.info("queue done id=%s kind=text attempts=%s", queue_id, attempts)
            _tg_notify_created(int(item_id), queue_id=int(queue_id))
            if item_type == "meeting" and row_item:
                _sync_calendar_for_item(row_item)
            if chat_id and item_type == "meeting" and dt is None:
                logging.info("clarify needed item_id=%s dt=%s ambiguous=%s", item_id, dt, time_ambiguous)
                item = {
                    "chat_id": chat_id,
                    "item_id": int(item_id),
                    "date": "1970-01-01",
                    "hh": 0,
                    "mm": 0,
                    "duration": int(MEETING_DEFAULT_MINUTES),
                    "expires_at": time.time() + CLARIFY_TTL_SEC,
                    "mode": "no_time",
                }
                qlen = _enqueue_clarify(chat_id, item)
                if qlen == 1:
                    reply_markup = {
                        "inline_keyboard": [
                            [
                                {
                                    "text": "Оставить без времени",
                                    "callback_data": (
                                        f"clarify:{chat_id}:{item_id}:1970-01-01:0:0:{MEETING_DEFAULT_MINUTES}:cancel"
                                    ),
                                },
                                {
                                    "text": "Отменить",
                                    "callback_data": (
                                        f"clarify:{chat_id}:{item_id}:1970-01-01:0:0:{MEETING_DEFAULT_MINUTES}:cancel"
                                    ),
                                },
                            ]
                        ]
                    }
                    _tg_send_message_with_keyboard(
                        chat_id,
                        f"Уточни время для встречи #{item_id}. Скажи: \"/set #{item_id} в 9\" или \"#{item_id} 16:00\".",
                        reply_markup,
                    )
            elif chat_id and item_type == "meeting" and time_ambiguous and dt is not None:
                logging.info("clarify needed item_id=%s dt=%s ambiguous=%s", item_id, dt, time_ambiguous)
                tm = _parse_time_ru(text)
                hh, mm = tm if tm else (dt.hour if dt else 0, dt.minute if dt else 0)
                item = {
                    "chat_id": chat_id,
                    "item_id": int(item_id),
                    "date": dt.date().isoformat() if dt else "1970-01-01",
                    "hh": int(hh),
                    "mm": int(mm),
                    "duration": int(MEETING_DEFAULT_MINUTES),
                    "expires_at": time.time() + CLARIFY_TTL_SEC,
                    "mode": "ambiguous",
                }
                qlen = _enqueue_clarify(chat_id, item)
                if qlen == 1:
                    reply_markup = {
                        "inline_keyboard": [
                            [
                                {
                                    "text": "Утро",
                                    "callback_data": (
                                        f"clarify:{chat_id}:{item_id}:"
                                        f"{dt.date().isoformat()}:{hh}:{mm}:{MEETING_DEFAULT_MINUTES}:am"
                                    ),
                                },
                                {
                                    "text": "Вечер",
                                    "callback_data": (
                                        f"clarify:{chat_id}:{item_id}:"
                                        f"{dt.date().isoformat()}:{hh}:{mm}:{MEETING_DEFAULT_MINUTES}:pm"
                                    ),
                                },
                                {
                                    "text": "Отменить",
                                    "callback_data": (
                                        f"clarify:{chat_id}:{item_id}:"
                                        f"{dt.date().isoformat()}:{hh}:{mm}:{MEETING_DEFAULT_MINUTES}:cancel"
                                    ),
                                },
                            ]
                        ]
                    }
                    _tg_send_message_with_keyboard(
                        chat_id,
                        f"Уточни время для встречи #{item_id}: утро или вечер?",
                        reply_markup,
                    )
            return
        if kind != "voice":
            # Unknown kinds are no-op but still complete to avoid queue clogging
            _queue_mark(queue_id, "DONE", None)
            logging.info("queue done id=%s kind=%s attempts=%s (noop)", queue_id, kind, attempts)
            return
        meta = payload.get("_meta") or {}
        tg_update_id = meta.get("tg_update_id") or row.get("tg_update_id")
        tg_message_id = meta.get("tg_message_id") or row.get("tg_message_id")
        file_id = payload.get("file_id")
        voice_unique_id = payload.get("file_unique_id")
        voice_duration = payload.get("duration")
        if not file_id:
            raise RuntimeError("missing file_id")

        ingested_at = row.get("ingested_at") or row.get("created_at")
        existing_item_id: int | None = None
        existing_asr_text: str | None = None
        if voice_unique_id:
            with _get_conn() as conn:
                r = conn.execute(
                    """
                    SELECT id, asr_text
                    FROM items
                    WHERE tg_voice_unique_id = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (str(voice_unique_id),),
                ).fetchone()
            r = as_dict(r)
            existing_item_id = _to_int_or_none(r.get("id"))
            existing_asr_text = (r.get("asr_text") or None) if r else None
        elif tg_update_id is not None and tg_message_id is not None:
            with _get_conn() as conn:
                r = conn.execute(
                    """
                    SELECT id, asr_text
                    FROM items
                    WHERE tg_update_id = ? AND tg_message_id = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (int(tg_update_id), int(tg_message_id)),
                ).fetchone()
            r = as_dict(r)
            existing_item_id = _to_int_or_none(r.get("id"))
            existing_asr_text = (r.get("asr_text") or None) if r else None

        if existing_item_id is not None:
            item_id = existing_item_id
            _ensure_voice_meta(
                item_id,
                int(tg_update_id) if tg_update_id is not None else None,
                int(tg_message_id) if tg_message_id is not None else None,
                str(file_id) if file_id else None,
                str(voice_unique_id) if voice_unique_id else None,
                int(voice_duration) if voice_duration is not None else None,
            )
            _log_voice_meta(
                "voice_dedup hit",
                item_id,
                int(tg_update_id) if tg_update_id is not None else None,
                int(tg_message_id) if tg_message_id is not None else None,
                str(voice_unique_id) if voice_unique_id else None,
                int(voice_duration) if voice_duration is not None else None,
                int(queue_id) if queue_id is not None else None,
            )
        else:
            item_id = _insert_voice_placeholder(
                "telegram",
                ingested_at,
                int(chat_id) if chat_id else None,
                int(tg_message_id) if tg_message_id is not None else None,
                queue_id=int(queue_id),
                tg_update_id=int(tg_update_id) if tg_update_id is not None else None,
                tg_voice_file_id=str(file_id) if file_id else None,
                tg_voice_unique_id=str(voice_unique_id) if voice_unique_id else None,
                tg_voice_duration=int(voice_duration) if voice_duration is not None else None,
            )
            _log_voice_meta(
                "voice_meta",
                item_id,
                int(tg_update_id) if tg_update_id is not None else None,
                int(tg_message_id) if tg_message_id is not None else None,
                str(voice_unique_id) if voice_unique_id else None,
                int(voice_duration) if voice_duration is not None else None,
                int(queue_id) if queue_id is not None else None,
            )

        if voice_unique_id:
            with _get_conn() as conn:
                r_other = conn.execute(
                    """
                    SELECT id, asr_text
                    FROM items
                    WHERE tg_voice_unique_id = ?
                      AND id != ?
                      AND asr_text IS NOT NULL
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (str(voice_unique_id), int(item_id)),
                ).fetchone()
            if r_other:
                _log_voice_meta(
                    "voice_dedup mismatch",
                    item_id,
                    int(tg_update_id) if tg_update_id is not None else None,
                    int(tg_message_id) if tg_message_id is not None else None,
                    str(voice_unique_id) if voice_unique_id else None,
                    int(voice_duration) if voice_duration is not None else None,
                    int(queue_id) if queue_id is not None else None,
                )
                _mark_item_failed_asr_dedup(int(item_id))
                _queue_mark(queue_id, "DONE", None)
                if chat_id:
                    _tg_send_message(
                        chat_id,
                        "⚠️ Похоже, голосовое сообщение обработалось некорректно. Отправь ещё раз voice. [VOICE-GUARD]",
                    )
                return

        health_status, health_services = _ml_health_check()
        logging.info(
            "ml health snapshot status=%s asr=%s llm=%s embedding=%s",
            health_status,
            health_services.get("asr", "down"),
            health_services.get("llm", "down"),
            health_services.get("embedding", "down"),
        )
        if health_services.get("asr") != "ok":
            _queue_mark(queue_id, "DONE", None)
            if chat_id:
                _tg_send_message(chat_id, "⚠️ Голосовой ввод недоступен (ASR не запущен)")
            return

        llm_is_down = health_services.get("llm") != "ok"
        if existing_asr_text:
            text = existing_asr_text
        else:
            audio = _tg_download_voice(file_id)
            if llm_is_down:
                logging.warning("ml health llm=down; using canonical parser fallback via /asr")
                text = _ml_asr_transcribe(audio)
            else:
                text = _ml_voice_command(audio)
        if not text or len(text.strip()) < 3:
            raise RuntimeError("empty text")
        pending = _get_pending_clarify(chat_id)
        if pending:
            if _try_apply_clarification(pending, text):
                _clear_pending_clarify(chat_id)
                _queue_mark(queue_id, "DONE", None)
                _tg_send_message(chat_id, "Ок, время встречи обновил.")
                return
            if re.search(r"\b(отмена|не надо|отменить)\b", text.lower()):
                _clear_pending_clarify(chat_id)
                _queue_mark(queue_id, "DONE", None)
                _tg_send_message(chat_id, "Хорошо, отменил уточнение.")
                return
        cmd = _try_parse_schedule_command(text)
        if cmd:
            item_id, when, duration = cmd
            try:
                data = _api_schedule_item(item_id, when, duration)
                item = (data or {}).get("item") or {}
                _queue_mark(queue_id, "DONE", None)
                logging.info("queue done id=%s kind=voice schedule=%s", queue_id, item_id)
                if chat_id:
                    _tg_send_message(
                        chat_id,
                        f"Ок. Встреча #{item_id} → {item.get('start_at', when.isoformat())} ({item.get('status', 'active')}).",
                    )
                if pending and pending.get("item_id") == item_id:
                    _clear_pending_clarify(chat_id)
                return
            except Exception as exc:
                err = str(exc)[:500]
                status = "DEAD" if attempts >= B2_MAX_ATTEMPTS else "FAILED"
                _queue_mark(queue_id, status, err)
                logging.warning("queue failed id=%s status=%s err=%s", queue_id, status, err)
                if chat_id:
                    _tg_send_message(chat_id, "Не понял формат, скажи: #21 16:00")
                return
        if pending:
            _queue_mark(queue_id, "DONE", None)
            if chat_id:
                _tg_send_message(chat_id, "Есть ожидающее уточнение. Ответь: утро/вечер или /set #ID 16:00.")
            return
        if _looks_like_schedule_intent(text):
            logging.info("schedule intent but parse failed: %r", text[:200])
            _queue_mark(queue_id, "DONE", None)
            if chat_id:
                ref_id = _extract_first_item_ref(text)
                hint_id = ref_id if ref_id is not None else 0
                _tg_send_message(
                    chat_id,
                    (
                        "Не понял уточнение. Скажи так: "
                        f"\"#{hint_id} 16:00\" или \"/set #{hint_id} 16:00\"."
                        if hint_id else
                        "Не понял уточнение. Скажи так: \"#ID 16:00\" или \"/set #ID 16:00\"."
                    ),
                )
            return
        item_type, item_status, start_at, end_at, dt, time_ambiguous = _compute_item_fields_from_text(text)
        logging.info("asr text=%r dt=%r", text[:200], dt)
        _update_item_from_asr(int(item_id), text, item_type, item_status, start_at, end_at)
        _log_voice_meta(
            "voice_asr",
            item_id,
            int(tg_update_id) if tg_update_id is not None else None,
            int(tg_message_id) if tg_message_id is not None else None,
            str(voice_unique_id) if voice_unique_id else None,
            int(voice_duration) if voice_duration is not None else None,
            int(queue_id) if queue_id is not None else None,
        )
        logging.info("tg meta kind=%s item_id=%s tg_chat_id=%s", kind, item_id, chat_id)
        try:
            with _get_conn() as conn:
                row_item = conn.execute(
                    "SELECT id, title, type, start_at, end_at, calendar_event_id, parent_id, parent_id_int FROM items WHERE id=?",
                    (item_id,),
                ).fetchone()
            row_item = as_dict(row_item)
            if (
                row_item
                and row_item.get("type") == "meeting"
                and row_item.get("start_at")
                and row_item.get("end_at")
            ):
                _sync_calendar_for_item(row_item)
        except Exception as exc:
            rid = row_item.get("id") if row_item else None
            logging.warning("calendar sync failed item_id=%s err=%s", rid, str(exc)[:200])
        _queue_mark(queue_id, "DONE", None)
        logging.info("queue done id=%s kind=voice attempts=%s", queue_id, attempts)
        if chat_id:
            # NOTE: report actual status/type to user
            # We read it back quickly to avoid mismatch
            try:
                with _get_conn() as conn:
                    r = conn.execute("select type,status,start_at from items where id=?", (item_id,)).fetchone()
                r = as_dict(r)
                st = str(r.get("status") or "inbox")
                sa = r.get("start_at")
            except Exception:
                st, sa = "inbox", None
            _tg_notify_created(int(item_id), queue_id=int(queue_id))
            if item_type == "meeting" and dt is None:
                reply_markup = {
                    "inline_keyboard": [
                        [
                            {
                                "text": "Скажи время",
                                "callback_data": (
                                    f"clarify:{chat_id}:{item_id}:no_date:no_hh:no_mm:{MEETING_DEFAULT_MINUTES}:cancel"
                                ),
                            },
                            {
                                "text": "Отменить",
                                "callback_data": (
                                    f"clarify:{chat_id}:{item_id}:no_date:no_hh:no_mm:{MEETING_DEFAULT_MINUTES}:cancel"
                                ),
                            },
                        ]
                    ]
                }
                _tg_send_message_with_keyboard(
                    chat_id,
                    f"Уточни время: скажи \"/set #{item_id} в 9\" или \"#{item_id} 16:00\".",
                    reply_markup,
                )
                return
            elif item_type == "meeting" and time_ambiguous and dt is not None:
                logging.info("clarify needed item_id=%s dt=%s ambiguous=%s", item_id, dt, time_ambiguous)
                tm = _parse_time_ru(text)
                hh, mm = tm if tm else (dt.hour if dt else 0, dt.minute if dt else 0)
                item = {
                    "chat_id": chat_id,
                    "item_id": int(item_id),
                    "date": dt.date().isoformat() if dt else "1970-01-01",
                    "hh": int(hh),
                    "mm": int(mm),
                    "duration": int(MEETING_DEFAULT_MINUTES),
                    "expires_at": time.time() + CLARIFY_TTL_SEC,
                    "mode": "ambiguous",
                }
                qlen = _enqueue_clarify(chat_id, item)
                if qlen == 1:
                    reply_markup = {
                        "inline_keyboard": [
                            [
                                {
                                    "text": "Утро",
                                    "callback_data": (
                                        f"clarify:{chat_id}:{item_id}:"
                                        f"{dt.date().isoformat()}:{hh}:{mm}:{MEETING_DEFAULT_MINUTES}:am"
                                    ),
                                },
                                {
                                    "text": "Вечер",
                                    "callback_data": (
                                        f"clarify:{chat_id}:{item_id}:"
                                        f"{dt.date().isoformat()}:{hh}:{mm}:{MEETING_DEFAULT_MINUTES}:pm"
                                    ),
                                },
                                {
                                    "text": "Отменить",
                                    "callback_data": (
                                        f"clarify:{chat_id}:{item_id}:"
                                        f"{dt.date().isoformat()}:{hh}:{mm}:{MEETING_DEFAULT_MINUTES}:cancel"
                                    ),
                                },
                            ]
                        ]
                    }
                    _tg_send_message_with_keyboard(
                        chat_id,
                        f"Уточни время для встречи #{item_id}: утро или вечер?",
                        reply_markup,
                    )
            else:
                if item_type != "meeting":
                    _tg_notify_created(int(item_id), queue_id=int(queue_id))
                    if sa:
                        _tg_send_message(chat_id, f"Время: {sa}")
                    else:
                        _tg_send_message(chat_id, "Время не распознано — останется в Inbox.")
    except Exception as exc:
        err = str(exc)[:500]
        status = "DEAD" if attempts >= B2_MAX_ATTEMPTS else "FAILED"
        _queue_mark(queue_id, status, err)
        logging.warning("queue failed id=%s status=%s err=%s", queue_id, status, err)
        if status == "DEAD" and WORKER_NOTIFY_ON_DEAD and chat_id:
            _tg_send_message(
                chat_id,
                "⚠️ Не удалось добавить в календарь. Задача сохранена, верну в Inbox. [CAL-DEAD]",
            )


def _find_item_for_queue_delivery(queue_row: dict) -> dict:
    queue_id = _to_int_or_none(queue_row.get("id"))
    chat_id = _to_int_or_none(queue_row.get("tg_chat_id"))
    tg_update_id = _to_int_or_none(queue_row.get("tg_update_id"))
    tg_message_id = _to_int_or_none(queue_row.get("tg_message_id"))
    if chat_id is None:
        return {}
    with _get_conn() as conn:
        row = None
        if tg_update_id is not None:
            row = conn.execute(
                """
                SELECT id, tg_chat_id, tg_update_id, tg_message_id, title, type, status, start_at,
                       calendar_event_id, tg_result_sent, last_error
                FROM items
                WHERE tg_chat_id = ? AND tg_update_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(chat_id), int(tg_update_id)),
            ).fetchone()
        if row is None and tg_message_id is not None:
            row = conn.execute(
                """
                SELECT id, tg_chat_id, tg_update_id, tg_message_id, title, type, status, start_at,
                       calendar_event_id, tg_result_sent, last_error
                FROM items
                WHERE tg_chat_id = ? AND tg_message_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(chat_id), int(tg_message_id)),
            ).fetchone()
    item = as_dict(row)
    if item:
        logging.info(
            "re_deliver_item_resolved queue_id=%s item_id=%s chat_id=%s",
            queue_id,
            item.get("id"),
            chat_id,
        )
    return item


def re_deliver_done_items(limit: int = 50) -> dict:
    batch = max(1, int(limit))
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, kind, status, tg_chat_id, tg_update_id, tg_message_id
            FROM inbox_queue
            WHERE status = 'DONE'
            ORDER BY id DESC
            LIMIT ?
            """,
            (batch,),
        ).fetchall()
    scanned = 0
    resolved = 0
    resent = 0
    unchanged = 0
    for raw in rows:
        q = as_dict(raw)
        queue_id = _to_int_or_none(q.get("id"))
        if queue_id is None:
            continue
        scanned += 1
        item = _find_item_for_queue_delivery(q)
        if not item:
            logging.info("re_deliver_skip queue_id=%s reason=item_not_found", queue_id)
            unchanged += 1
            continue
        item_id = _to_int_or_none(item.get("id"))
        if item_id is None:
            unchanged += 1
            continue
        resolved += 1
        flags_before = int(item.get("tg_result_sent") or 0)
        item_type = str(item.get("type") or "")
        calendar_state = str(item.get("calendar_event_id") or "")
        logging.info(
            "re_deliver_check queue_id=%s item_id=%s type=%s flags_before=%s calendar_state=%s",
            queue_id,
            item_id,
            item_type,
            flags_before,
            calendar_state or "-",
        )
        if not (flags_before & _TG_RESULT_CREATED):
            _tg_notify_created(int(item_id), queue_id=int(queue_id))
        if item_type == "meeting":
            if calendar_state == "FAILED" and not (flags_before & _TG_RESULT_DEAD):
                _tg_notify_calendar_dead(int(item_id), queue_id=int(queue_id))
            elif calendar_state and calendar_state not in {"PENDING", "FAILED"} and not (flags_before & _TG_RESULT_SUCCESS):
                _tg_notify_calendar_success(int(item_id), queue_id=int(queue_id))
        with _get_conn() as conn:
            row_after = conn.execute(
                "SELECT tg_result_sent FROM items WHERE id = ?",
                (int(item_id),),
            ).fetchone()
        flags_after = int((as_dict(row_after)).get("tg_result_sent") or 0)
        if flags_after != flags_before:
            resent += 1
            logging.info(
                "re_deliver_marked queue_id=%s item_id=%s flags_before=%s flags_after=%s",
                queue_id,
                item_id,
                flags_before,
                flags_after,
            )
        else:
            unchanged += 1
            logging.info(
                "re_deliver_unchanged queue_id=%s item_id=%s flags=%s",
                queue_id,
                item_id,
                flags_after,
            )
    result = {
        "ok": True,
        "scanned": scanned,
        "resolved": resolved,
        "resent": resent,
        "unchanged": unchanged,
        "limit": batch,
    }
    logging.info(
        "re_deliver_summary scanned=%s resolved=%s resent=%s unchanged=%s limit=%s",
        scanned,
        resolved,
        resent,
        unchanged,
        batch,
    )
    return result


# === P3: Task Domain (pure-ish) =============================================
# Helpers only for logging / context (no behavior changes).
def _is_terminal_state(state: str) -> bool:
    return str(state or "").upper() in {"DONE", "FAILED", "CANCELLED"}


def _can_transition(state_from: str, state_to: str) -> bool:
    sf = str(state_from or "").upper()
    st = str(state_to or "").upper()
    allowed = {
        ("NEW", "PLANNED"),
        ("PLANNED", "SCHEDULED"),
        ("SCHEDULED", "PLANNED"),
        ("SCHEDULED", "DONE"),
        ("SCHEDULED", "FAILED"),
        ("SCHEDULED", "CANCELLED"),
        ("PLANNED", "DONE"),
        ("PLANNED", "CANCELLED"),
    }
    return (sf, st) in allowed


def _describe_transition(state_from: str, state_to: str) -> str:
    sf = str(state_from or "").upper()
    st = str(state_to or "").upper()
    return f"{sf}->{st}"


# === P3: Calendar Adapter (side-effect boundary) ============================
P3_CALENDAR_CREATE = "P3_CALENDAR_CREATE"
P3_CALENDAR_UPDATE = "P3_CALENDAR_UPDATE"
P4_CALENDAR_CANCEL = "P4_CALENDAR_CANCEL"


def _disable_calendar_sync(reason: str, detail: str) -> None:
    global CALENDAR_SYNC_MODE, _CAL_NOT_CONFIGURED_REASON
    _CAL_NOT_CONFIGURED_REASON = reason
    prev_mode = CALENDAR_SYNC_MODE
    CALENDAR_SYNC_MODE = "off"
    logging.error(
        "calendar_sync_disabled reason=%s detail=%s previous_mode=%s current_mode=%s calendar_mode=NOT_CONFIGURED",
        reason,
        detail,
        prev_mode,
        CALENDAR_SYNC_MODE,
    )


def _validate_calendar_service_account_on_startup() -> None:
    if CALENDAR_SYNC_MODE == "off":
        return
    sa_path = (GOOGLE_SERVICE_ACCOUNT_FILE or "").strip()
    if not sa_path:
        _disable_calendar_sync("missing_file", "GOOGLE_SERVICE_ACCOUNT_FILE is empty")
        return
    if not os.path.exists(sa_path):
        _disable_calendar_sync("missing_file", f"service account file not found: {sa_path}")
        return
    if not os.path.isfile(sa_path):
        _disable_calendar_sync("file_is_directory", f"service account path is not a file: {sa_path}")
        return
    try:
        size = os.path.getsize(sa_path)
    except Exception as exc:
        _disable_calendar_sync("file_stat_error", f"failed to stat service account file: {type(exc).__name__}")
        return
    if size <= 0:
        _disable_calendar_sync("empty_file", f"service account file is empty: {sa_path}")
        return
    try:
        with open(sa_path, "r", encoding="utf-8") as f:
            _ = json.load(f)
    except json.JSONDecodeError:
        _disable_calendar_sync(
            "invalid_json",
            "service account json invalid (likely missing leading '{' or extra non-json text)",
        )
        return
    except Exception as exc:
        _disable_calendar_sync("read_error", f"failed to read service account file: {type(exc).__name__}")
        return
    logging.info("calendar_service_account_file ok path=%s", sa_path)


def _smoke_check_google_calendar_deps() -> None:
    if CALENDAR_SYNC_MODE == "off":
        return
    try:
        importlib.import_module("googleapiclient")
        importlib.import_module("google.oauth2")
    except Exception as exc:
        msg = (
            "Google Calendar deps missing; rebuild worker image with "
            "google-api-python-client/google-auth"
        )
        logging.error("%s err=%s", msg, str(exc)[:200])
        _disable_calendar_sync("config_error_missing_google_deps", msg)


def _env_flag_enabled(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _short_event_label(value: str, *, max_len: int, max_words: int) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    words = text.split()
    if len(words) > max_words:
        text = " ".join(words[:max_words])
    if len(text) > max_len:
        text = text[: max_len - 1].rstrip() + "…"
    return text


def _calendar_title_description_from_item(item: dict[str, Any]) -> tuple[str, str | None]:
    base_title = str(item.get("title") or "").strip()
    root_title = str(item.get("root_title") or "").strip()
    parent_title = str(item.get("parent_title") or "").strip()
    task_title = str(item.get("task_title") or base_title).strip()
    calendar_add = _env_flag_enabled(item.get("calendar_add"))

    if not (calendar_add and root_title):
        return base_title, None

    root_short = _short_event_label(root_title, max_len=20, max_words=2)
    task_short = _short_event_label(task_title or base_title, max_len=48, max_words=6)
    event_title = f"{root_short}: {task_short}".strip(": ").strip()
    if not event_title:
        event_title = base_title

    parts: list[str] = []
    for part in (root_title, parent_title, task_title or base_title):
        part_norm = str(part or "").strip()
        if part_norm and part_norm not in parts:
            parts.append(part_norm)
    description = f"Path: {' / '.join(parts)}" if parts else None
    return event_title, description


def _calendar_error_info(exc: Exception) -> tuple[str, bool]:
    status = None
    resp = getattr(exc, "resp", None)
    if resp is not None and getattr(resp, "status", None) is not None:
        try:
            status = int(resp.status)
        except Exception:
            status = None
    if status is not None:
        return f"calendar_http_{status}", status in (429, 500, 502, 503, 504)
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return "calendar_timeout", True
    return f"calendar_error_{type(exc).__name__}", True


def _handle_calendar_not_configured(item_id: int) -> None:
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE items
            SET last_error = ?,
                calendar_event_id = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            ("calendar_not_configured", datetime.now(timezone.utc).isoformat(), item_id),
        )
        conn.commit()
    logging.info("[%s] calendar_state after=NOT_CONFIGURED", item_id)


def _mark_calendar_failed(item_id: int, err_code: str) -> None:
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE items
            SET attempts = ?,
                last_error = ?,
                calendar_event_id = 'FAILED',
                updated_at = ?
            WHERE id = ?
            """,
            (CALENDAR_MAX_ATTEMPTS, err_code[:200], datetime.now(timezone.utc).isoformat(), item_id),
        )
        conn.commit()
    logging.info("[%s] calendar_state after=FAILED", item_id)
    _tg_notify_calendar_dead(item_id)


def _calendar_smoke_test() -> None:
    service = _get_calendar_service()
    if service is None:
        logging.info("calendar_smoke service NONE")
        return
    logging.info("calendar_smoke service OK")
    if not CALENDAR_SMOKE_TEST:
        return
    try:
        start = datetime.now(_local_tz()) + timedelta(minutes=5)
        end = start + timedelta(minutes=15)
        event = {
            "summary": "MyTGTodoist smoke test",
            "start": {"dateTime": start.isoformat(), "timeZone": TIMEZONE_NAME},
            "end": {"dateTime": end.isoformat(), "timeZone": TIMEZONE_NAME},
        }
        created = service.events().insert(calendarId=GOOGLE_CALENDAR_ID, body=event).execute()
        logging.info("calendar_smoke created id=%s", created.get("id"))
    except Exception as exc:
        logging.warning("calendar_smoke failed err=%s", str(exc)[:200])


def _calendar_http_status(exc: Exception) -> int | None:
    resp = getattr(exc, "resp", None)
    status = getattr(resp, "status", None) if resp is not None else None
    try:
        return int(status) if status is not None else None
    except Exception:
        return None


def _calendar_result(
    ok: bool,
    event_id: str | None,
    http_status: int | None,
    err: str | None,
    etag: str | None,
) -> dict:
    return {
        "ok": bool(ok),
        "event_id": event_id,
        "http_status": http_status,
        "err": err,
        "etag": etag,
    }


def _set_calendar_not_configured_reason(value: str | None) -> None:
    global _CAL_NOT_CONFIGURED_REASON
    _CAL_NOT_CONFIGURED_REASON = value


def _runtime_calendar_deps() -> dict[str, Any]:
    return {
        "set_calendar_not_configured_reason": _set_calendar_not_configured_reason,
        "google_service_account_file": GOOGLE_SERVICE_ACCOUNT_FILE,
        "google_calendar_id": GOOGLE_CALENDAR_ID,
        "calendar_debug": CALENDAR_DEBUG,
        "timezone_name": TIMEZONE_NAME,
        "local_tz": _local_tz,
        "build_item_ical_uid": build_item_ical_uid,
        "create_or_reuse_event": create_or_reuse_event,
        "patch_event": _patch_event,
        "patch_event_description": _patch_event_description,
        "get_calendar_service": _get_calendar_service,
        "parse_iso_datetime_to_local": _parse_iso_datetime_to_local,
        "parse_date_token_ymd": _parse_date_token_ymd,
        "parse_time_token_hhmm": _parse_time_token_hhmm,
        "runtime_duration_minutes": _runtime_duration_minutes,
        "meeting_kind_from_entities": _meeting_kind_from_entities,
        "runtime_temporal_title": _runtime_temporal_title,
        "meeting_success_text": _meeting_success_text,
        "meeting_kind_default": _MEETING_KIND_DEFAULT,
        "default_duration_min": DEFAULT_DURATION_MIN,
        "calendar_get_event": _calendar_get_event,
        "runtime_sync_conflict_get": _runtime_sync_conflict_get,
        "runtime_trace_conflict_restore_from_snapshot": _runtime_trace_conflict_restore_from_snapshot,
        "calendar_patch_event": _calendar_patch_event,
        "calendar_patch_event_description": _calendar_patch_event_description,
        "create_event": _create_event,
        "re": re,
    }


def _get_calendar_service():
    return _runtime_calendar_module._get_calendar_service(
        deps=_runtime_calendar_deps(),
    )


def _calendar_normalize_local_dt(value: datetime) -> datetime:
    return _runtime_calendar_module._calendar_normalize_local_dt(
        value,
        deps=_runtime_calendar_deps(),
    )


def _calendar_datetime_payload(start: datetime, end: datetime) -> tuple[dict[str, str], dict[str, str]]:
    return _runtime_calendar_module._calendar_datetime_payload(
        start,
        end,
        deps=_runtime_calendar_deps(),
    )


def _create_event(
    item_id: int | str,
    title: str,
    start: datetime,
    end: datetime,
    description: str | None = None,
    existing_event_id: str | None = None,
    flow_id: str | None = None,
) -> str | None:
    return _runtime_calendar_module._create_event(
        item_id,
        title,
        start,
        end,
        description=description,
        existing_event_id=existing_event_id,
        flow_id=flow_id,
        deps=_runtime_calendar_deps(),
    )


def _patch_event(event_id: str, start: datetime, end: datetime) -> str:
    return _runtime_calendar_module._patch_event(
        event_id,
        start,
        end,
        deps=_runtime_calendar_deps(),
    )


def _patch_event_description(event_id: str, description: str) -> str:
    return _runtime_calendar_module._patch_event_description(
        event_id,
        description,
        deps=_runtime_calendar_deps(),
    )


def _calendar_create_event(
    item_id: int | str,
    title: str,
    start: datetime,
    end: datetime,
    description: str | None = None,
) -> dict:
    return _runtime_calendar_module._calendar_create_event(
        item_id,
        title,
        start,
        end,
        description=description,
        deps=_runtime_calendar_deps(),
    )


def _calendar_patch_event(event_id: str, start: datetime, end: datetime) -> dict:
    return _runtime_calendar_module._calendar_patch_event(
        event_id,
        start,
        end,
        deps=_runtime_calendar_deps(),
    )


def _calendar_patch_event_description(event_id: str, description: str) -> dict:
    return _runtime_calendar_module._calendar_patch_event_description(
        event_id,
        description,
        deps=_runtime_calendar_deps(),
    )


def _calendar_delete_event(event_id: str) -> dict:
    return _runtime_calendar_module._calendar_delete_event(
        event_id,
        deps=_runtime_calendar_deps(),
    )


def _calendar_cancel_event(event_id: str) -> dict:
    return _calendar_delete_event(event_id)


def _calendar_get_event(event_id: str) -> dict:
    return _runtime_calendar_module._calendar_get_event(
        event_id,
        deps=_runtime_calendar_deps(),
    )


def _runtime_temporal_window_local(
    entities: dict[str, Any],
    *,
    fallback_minutes: int,
) -> tuple[datetime | None, datetime | None, int]:
    return _runtime_calendar_module._runtime_temporal_window_local(
        entities,
        fallback_minutes=fallback_minutes,
        deps=_runtime_calendar_deps(),
    )


def _runtime_commit_temporal_calendar(
    *,
    flow_id: str,
    intent: str,
    entities: dict[str, Any],
    execution_result: dict[str, Any],
    source_timezone: str,
) -> dict[str, Any]:
    return _runtime_calendar_module._runtime_commit_temporal_calendar(
        flow_id=flow_id,
        intent=intent,
        entities=entities,
        execution_result=execution_result,
        source_timezone=source_timezone,
        deps=_runtime_calendar_deps(),
    )


def _runtime_commit_meeting_update_calendar(
    *,
    flow_id: str,
    entities: dict[str, Any],
    execution_result: dict[str, Any],
) -> dict[str, Any]:
    return _runtime_calendar_module._runtime_commit_meeting_update_calendar(
        flow_id=flow_id,
        entities=entities,
        execution_result=execution_result,
        deps=_runtime_calendar_deps(),
    )


def _runtime_commit_meeting_comment_update_calendar(
    *,
    flow_id: str,
    entities: dict[str, Any],
    execution_result: dict[str, Any],
) -> dict[str, Any]:
    return _runtime_calendar_module._runtime_commit_meeting_comment_update_calendar(
        flow_id=flow_id,
        entities=entities,
        execution_result=execution_result,
        deps=_runtime_calendar_deps(),
    )


# === P3: Sync Ticks ==========================================================
# TODO(P4): infinite-loop protection concept only (no implementation in P3).
# Suggestion: last_sync_at, sync_counter, or max N state flips per task per hour.
def _p3_calendar_create_tick(limit: int = 10) -> None:
    # P3: for current logic (create_tick).
    conn: sqlite3.Connection | None = None
    try:
        with _get_conn() as conn_local:
            conn = conn_local
            rows = conn_local.execute(
                """
                SELECT id, title, planned_at
                FROM tasks
                WHERE state = 'PLANNED'
                  AND planned_at IS NOT NULL
                  AND calendar_event_id IS NULL
                ORDER BY id ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
    except Exception as exc:
        logging.exception("%s err=%s", P3_CALENDAR_CREATE, str(exc)[:200], exc_info=True)
        _log_calendar_sqlite_context(
            P3_CALENDAR_CREATE,
            conn,
            conn_source="_get_conn() in _p3_calendar_create_tick(fetch rows)",
        )
        return

    for row in rows:
        try:
            task_id = int(row["id"])
            title = str(row["title"] or "").strip()
            planned_at = str(row["planned_at"] or "").strip()
            if not planned_at:
                continue
            try:
                dt = datetime.fromisoformat(planned_at)
            except Exception:
                logging.warning(
                    "%s task_id=%s state_from=PLANNED state_to=PLANNED planned_at=%s calendar_event_id=%s "
                    "err=bad_planned_at",
                    P3_CALENDAR_CREATE,
                    task_id,
                    planned_at,
                    "",
                )
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            claim_id = f"PENDING:{datetime.now(timezone.utc).isoformat()}:{os.getpid()}"
            with _get_conn() as conn:
                cur = conn.execute(
                    """
                    UPDATE tasks
                    SET calendar_event_id = ?
                    WHERE id = ?
                      AND calendar_event_id IS NULL
                    """,
                    (claim_id, task_id),
                )
                conn.commit()
            if cur.rowcount != 1:
                continue

            end = dt + timedelta(minutes=MEETING_DEFAULT_MINUTES)
            res = _calendar_create_event(task_id, f"Task #{task_id}: {title}", dt, end)
            logging.info(
                "%s action=create_attempt task_id=%s planned_at=%s calendar_event_id=%s "
                "ok=%s http_status=%s err=%s",
                P3_CALENDAR_CREATE,
                task_id,
                planned_at,
                claim_id,
                res.get("ok"),
                res.get("http_status"),
                res.get("err"),
            )
            if not res.get("ok"):
                err = res.get("err") or ""
                if err.startswith("exception:"):
                    logging.warning(
                        "%s action=create_attempt task_id=%s planned_at=%s calendar_event_id=%s "
                        "ok=%s http_status=%s err=%s",
                        P3_CALENDAR_CREATE,
                        task_id,
                        planned_at,
                        claim_id,
                        res.get("ok"),
                        res.get("http_status"),
                        res.get("err"),
                    )
                    continue
                with _get_conn() as conn:
                    conn.execute(
                        """
                        UPDATE tasks
                        SET calendar_event_id = NULL,
                            updated_at = ?
                        WHERE id = ?
                          AND calendar_event_id = ?
                        """,
                        (datetime.now(timezone.utc).isoformat(), task_id, claim_id),
                    )
                    conn.commit()
                logging.warning(
                    "%s action=create_attempt task_id=%s planned_at=%s calendar_event_id=%s "
                    "reason=service_unavailable ok=%s http_status=%s err=%s",
                    P3_CALENDAR_CREATE,
                    task_id,
                    planned_at,
                    claim_id,
                    res.get("ok"),
                    res.get("http_status"),
                    res.get("err"),
                )
                break

            event_id = res.get("event_id")
            with _get_conn() as conn:
                conn.execute(
                    """
                    UPDATE tasks
                    SET calendar_event_id = ?,
                        state = 'SCHEDULED',
                        updated_at = ?
                    WHERE id = ?
                      AND calendar_event_id = ?
                    """,
                    (event_id, datetime.now(timezone.utc).isoformat(), task_id, claim_id),
                )
                conn.commit()
            logging.info(
                "%s transition=PLANNED->SCHEDULED reason=create_success task_id=%s planned_at=%s "
                "calendar_event_id=%s ok=%s http_status=%s err=%s",
                P3_CALENDAR_CREATE,
                task_id,
                planned_at,
                event_id,
                res.get("ok"),
                res.get("http_status"),
                res.get("err"),
            )
        except Exception as exc:
            logging.exception(
                "%s action=create_attempt task_id=%s planned_at=%s calendar_event_id=%s",
                P3_CALENDAR_CREATE,
                task_id,
                planned_at,
                "",
            )
            _log_calendar_sqlite_context(P3_CALENDAR_CREATE)
            continue


def _p3_calendar_update_tick(limit: int = 3) -> None:
    # P3: for current logic (update_tick).
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    conn: sqlite3.Connection | None = None
    try:
        with _get_conn() as conn_local:
            conn = conn_local
            rows = conn_local.execute(
                """
                SELECT id, title, planned_at, calendar_event_id, state, updated_at
                FROM tasks
                WHERE calendar_event_id IS NOT NULL
                  AND planned_at IS NOT NULL
                  AND updated_at >= ?
                  AND state = 'PLANNED'
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (cutoff, int(limit)),
            ).fetchall()
    except Exception as exc:
        logging.exception("%s err=%s", P3_CALENDAR_UPDATE, str(exc)[:200], exc_info=True)
        _log_calendar_sqlite_context(
            P3_CALENDAR_UPDATE,
            conn,
            conn_source="_get_conn() in _p3_calendar_update_tick(fetch rows)",
        )
        return

    for row in rows:
        try:
            task_id = int(row["id"])
            title = str(row["title"] or "").strip()
            planned_at = str(row["planned_at"] or "").strip()
            event_id = str(row["calendar_event_id"] or "").strip()
            state = str(row["state"] or "").strip().upper()
            if state == "CANCELLED":
                # TODO(P4): plan cancel_event (do not perform).
                pass
            if not planned_at or not event_id:
                continue
            try:
                dt = datetime.fromisoformat(planned_at)
            except Exception:
                logging.warning(
                    "%s task_id=%s state_from=PLANNED state_to=PLANNED planned_at=%s calendar_event_id=%s "
                    "err=bad_planned_at",
                    P3_CALENDAR_UPDATE,
                    task_id,
                    planned_at,
                    event_id,
                )
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            end = dt + timedelta(minutes=MEETING_DEFAULT_MINUTES)
            res = _calendar_patch_event(event_id, dt, end)
            logging.info(
                "%s action=patch_attempt task_id=%s planned_at=%s calendar_event_id=%s "
                "ok=%s http_status=%s err=%s",
                P3_CALENDAR_UPDATE,
                task_id,
                planned_at,
                event_id,
                res.get("ok"),
                res.get("http_status"),
                res.get("err"),
            )
            if res.get("err") == "not_found":
                with _get_conn() as conn:
                    conn.execute(
                        """
                        UPDATE tasks
                        SET calendar_event_id = NULL,
                            state = 'PLANNED',
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (datetime.now(timezone.utc).isoformat(), task_id),
                    )
                    conn.commit()
                logging.warning(
                    "%s action=patch_attempt task_id=%s planned_at=%s calendar_event_id=%s "
                    "reason=patch_404_reset ok=%s http_status=%s err=%s",
                    P3_CALENDAR_UPDATE,
                    task_id,
                    planned_at,
                    event_id,
                    res.get("ok"),
                    res.get("http_status"),
                    res.get("err"),
                )
                continue
            if not res.get("ok"):
                logging.warning(
                    "%s action=patch_attempt task_id=%s planned_at=%s calendar_event_id=%s "
                    "reason=patch_failed ok=%s http_status=%s err=%s",
                    P3_CALENDAR_UPDATE,
                    task_id,
                    planned_at,
                    event_id,
                    res.get("ok"),
                    res.get("http_status"),
                    res.get("err"),
                )
                continue
            with _get_conn() as conn:
                conn.execute(
                    """
                    UPDATE tasks
                    SET state = 'SCHEDULED',
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (datetime.now(timezone.utc).isoformat(), task_id),
                )
                conn.commit()
            logging.info(
                "%s transition=PLANNED->SCHEDULED reason=patch_success task_id=%s planned_at=%s "
                "calendar_event_id=%s ok=%s http_status=%s err=%s",
                P3_CALENDAR_UPDATE,
                task_id,
                planned_at,
                event_id,
                res.get("ok"),
                res.get("http_status"),
                res.get("err"),
            )
        except Exception as exc:
            logging.exception(
                "%s action=patch_attempt task_id=%s planned_at=%s calendar_event_id=%s",
                P3_CALENDAR_UPDATE,
                row["id"],
                "",
                "",
            )
            _log_calendar_sqlite_context(P3_CALENDAR_UPDATE)
            continue


def _p4_calendar_cancel_tick(limit: int = 3) -> None:
    # P4: for future scaffold (cancel_tick). Do not call yet.
    conn: sqlite3.Connection | None = None
    try:
        with _get_conn() as conn_local:
            conn = conn_local
            rows = conn_local.execute(
                """
                SELECT id, title, planned_at, calendar_event_id, state, updated_at
                FROM tasks
                WHERE state = 'CANCELLED'
                  AND calendar_event_id IS NOT NULL
                  AND calendar_event_id != ''
                ORDER BY updated_at ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
    except Exception as exc:
        logging.exception("%s err=%s", P4_CALENDAR_CANCEL, str(exc)[:200], exc_info=True)
        _log_calendar_sqlite_context(
            P4_CALENDAR_CANCEL,
            conn,
            conn_source="_get_conn() in _p4_calendar_cancel_tick(fetch rows)",
        )
        return
    for row in rows:
        try:
            task_id = int(row["id"])
            event_id = str(row["calendar_event_id"] or "").strip()
            if not event_id:
                continue
            res = _calendar_cancel_event(event_id)
            logging.info(
                "%s action=cancel_attempt task_id=%s calendar_event_id=%s ok=%s http_status=%s err=%s",
                P4_CALENDAR_CANCEL,
                task_id,
                event_id,
                res.get("ok"),
                res.get("http_status"),
                res.get("err"),
            )
            if res.get("ok"):
                reason = "already_missing" if res.get("http_status") == 404 else "delete_success"
                with _get_conn() as conn:
                    conn.execute(
                        """
                        UPDATE tasks
                        SET calendar_event_id = NULL,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (datetime.now(timezone.utc).isoformat(), task_id),
                    )
                    conn.commit()
                logging.info(
                    "%s action=cancel_applied task_id=%s cleared_calendar_event_id=1 reason=%s",
                    P4_CALENDAR_CANCEL,
                    task_id,
                    reason,
                )
                continue
            logging.warning(
                "%s action=cancel_failed task_id=%s calendar_event_id=%s ok=%s http_status=%s err=%s",
                P4_CALENDAR_CANCEL,
                task_id,
                event_id,
                res.get("ok"),
                res.get("http_status"),
                res.get("err"),
            )
        except Exception as exc:
            logging.exception(
                "%s action=cancel_failed task_id=%s calendar_event_id=%s",
                P4_CALENDAR_CANCEL,
                row["id"],
                row.get("calendar_event_id"),
            )
            _log_calendar_sqlite_context(P4_CALENDAR_CANCEL)
            continue

def _p4_reg_nudge_should_emit(mode: str, today: date, due_date: date) -> bool:
    mode_norm = (mode or "off").strip().lower()
    if mode_norm == "off":
        return False
    if mode_norm == "daily":
        return True
    if mode_norm == "due_day":
        return today == due_date
    return False


def _p4_reg_nudge_tick(limit: int = 50) -> None:
    mode = REG_NUDGES_MODE
    if mode not in {"daily", "due_day"}:
        return
    now_local = datetime.now(_local_tz())
    today = now_local.date()
    period_key = f"{today.year:04d}-{today.month:02d}"
    try:
        with _get_conn() as conn:
            rows = conn.execute(
                """
                SELECT rr.id, rr.regulation_id, rr.period_key, rr.status, rr.due_date, r.title
                FROM regulation_runs rr
                JOIN regulations r ON r.id = rr.regulation_id
                WHERE rr.period_key = ?
                  AND rr.status = 'OPEN'
                  AND r.status = 'ACTIVE'
                ORDER BY rr.id ASC
                LIMIT ?
                """,
                (period_key, int(limit)),
            ).fetchall()
    except Exception as exc:
        logging.warning("P4_REG_NUDGE action=fetch_error err=%s", str(exc)[:200])
        return
    for row in rows:
        try:
            due_date = date.fromisoformat(str(row["due_date"]))
        except Exception:
            continue
        if not _p4_reg_nudge_should_emit(mode, today, due_date):
            continue
        key = f"{today.isoformat()}:{period_key}:{int(row['id'])}"
        if _REG_NUDGE_LAST_SENT.get(key):
            continue
        _REG_NUDGE_LAST_SENT[key] = time.time()
        logging.info(
            "P4_REG_NUDGE action=emit reg_run_id=%s regulation_id=%s period_key=%s due_date=%s mode=%s dedup=%s",
            row["id"],
            row["regulation_id"],
            period_key,
            row["due_date"],
            mode,
            key,
        )


def _p5_should_run(mode: str) -> bool:
    return (mode or "off").strip().lower() == "log"


def _p5_drift_calendar_type(
    state: str,
    cal_http_status: int | None,
    planned_at: str | None,
    calendar_start: str | None,
    cal_ok: bool,
) -> str | None:
    state_norm = (state or "").strip().upper()
    if cal_http_status == 404:
        if state_norm == "SCHEDULED":
            return "missing_event"
        return None
    if cal_ok and state_norm in {"DONE", "FAILED", "CANCELLED"}:
        return "unexpected_event"
    if cal_ok and state_norm == "SCHEDULED" and planned_at and calendar_start:
        try:
            pa = planned_at.replace("Z", "+00:00")
            ca = calendar_start.replace("Z", "+00:00")
            if datetime.fromisoformat(pa) != datetime.fromisoformat(ca):
                return "time_mismatch"
        except Exception:
            return "time_mismatch"
    return None


def _p5_drift_tick(limit: int = 50) -> int:
    if not _p5_should_run(DRIFT_MODE):
        return 0
    drift_count = 0
    try:
        with _get_conn() as conn:
            rows = conn.execute(
                """
                SELECT id, state, planned_at, calendar_event_id
                FROM tasks
                WHERE calendar_event_id IS NOT NULL AND calendar_event_id != ''
                ORDER BY id ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
    except Exception as exc:
        logging.warning("P5_DRIFT action=fetch_error err=%s", str(exc)[:200])
        return 0
    for row in rows:
        event_id = str(row["calendar_event_id"] or "").strip()
        if not event_id:
            continue
        cal_res = _calendar_get_event(event_id)
        drift_type = _p5_drift_calendar_type(
            str(row["state"] or ""),
            cal_res.get("http_status"),
            row["planned_at"],
            cal_res.get("event_start"),
            bool(cal_res.get("ok")),
        )
        if drift_type:
            logging.info(
                "P5_DRIFT drift_type=%s entity=task task_id=%s calendar_event_id=%s planned_at=%s calendar_start=%s "
                "http_status=%s err=%s",
                drift_type,
                row["id"],
                event_id,
                row["planned_at"],
                cal_res.get("event_start"),
                cal_res.get("http_status"),
                cal_res.get("err"),
            )
            drift_count += 1
    # Regulations drift: missing run for current month
    today = datetime.now(_local_tz()).date()
    period_key = f"{today.year:04d}-{today.month:02d}"
    try:
        with _get_conn() as conn:
            regs = conn.execute(
                """
                SELECT id FROM regulations WHERE status = 'ACTIVE' ORDER BY id ASC
                """
            ).fetchall()
            for reg in regs:
                reg_id = int(reg["id"])
                run = conn.execute(
                    """
                    SELECT id FROM regulation_runs
                    WHERE regulation_id = ? AND period_key = ?
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (reg_id, period_key),
                ).fetchone()
                if not run:
                    logging.info(
                        "P5_DRIFT drift_type=reg_run_missing_for_month entity=regulation regulation_id=%s period_key=%s",
                        reg_id,
                        period_key,
                    )
                    drift_count += 1
    except Exception as exc:
        logging.warning("P5_DRIFT action=reg_fetch_error err=%s", str(exc)[:200])
    return drift_count


def _p5_overload_signals(
    day: str,
    tasks_today: int,
    regs_due: int,
    backlog: int,
) -> list[tuple[str, int, int, str]]:
    signals: list[tuple[str, int, int, str]] = []
    minutes = int(tasks_today) * int(DEFAULT_DURATION_MIN)
    if minutes > CAPACITY_MINUTES_PER_DAY:
        signals.append(("capacity_minutes", minutes, CAPACITY_MINUTES_PER_DAY, day))
    if tasks_today > CAPACITY_ITEMS_PER_DAY:
        signals.append(("capacity_items", tasks_today, CAPACITY_ITEMS_PER_DAY, day))
    if regs_due > DUE_TODAY_LIMIT:
        signals.append(("due_today", regs_due, DUE_TODAY_LIMIT, day))
    if backlog > BACKLOG_LIMIT:
        signals.append(("backlog", backlog, BACKLOG_LIMIT, day))
    return signals


def _p5_reg_status_is_due(status: str | None) -> bool:
    s = (status or "").strip().upper()
    return s in {"DUE", "OPEN"}


def _p5_regs_due_counts(rows: list[sqlite3.Row] | list[dict]) -> tuple[int, dict[str, int]]:
    total = 0
    counts: dict[str, int] = {}
    for row in rows:
        status = row["status"] if isinstance(row, sqlite3.Row) else row.get("status")
        if not _p5_reg_status_is_due(status):
            continue
        status_norm = (status or "").strip().upper()
        counts[status_norm] = counts.get(status_norm, 0) + 1
        total += 1
    return total, counts


def _p5_nudge_reset_if_new_day(day_str: str) -> None:
    global _P5_NUDGE_DAY, _P5_DRIFT_COUNT_TODAY, _P5_OVERLOAD_COUNT_TODAY, _P5_NUDGE_EMITTED
    if _P5_NUDGE_DAY != day_str:
        _P5_NUDGE_DAY = day_str
        _P5_DRIFT_COUNT_TODAY = 0
        _P5_OVERLOAD_COUNT_TODAY = 0
        _P5_NUDGE_EMITTED = False


def _p5_nudge_should_emit(mode: str, drift_count: int, overload_count: int, emitted: bool) -> bool:
    mode_norm = (mode or "off").strip().lower()
    if mode_norm != "daily":
        return False
    if emitted:
        return False
    return drift_count > 0 or overload_count > 0


def _p5_nudge_emit_if_needed(day_str: str, mode: str | None = None) -> bool:
    global _P5_NUDGE_EMITTED
    mode_use = mode if mode is not None else P5_NUDGES_MODE
    if not _p5_nudge_should_emit(
        mode_use, _P5_DRIFT_COUNT_TODAY, _P5_OVERLOAD_COUNT_TODAY, _P5_NUDGE_EMITTED
    ):
        return False
    logging.info(
        "P5_NUDGE action=emit day=%s drift=%s overload=%s hint=%s",
        day_str,
        _P5_DRIFT_COUNT_TODAY,
        _P5_OVERLOAD_COUNT_TODAY,
        "/regs /list open",
    )
    _P5_NUDGE_EMITTED = True
    return True


def _p5_overload_tick() -> int:
    if not _p5_should_run(OVERLOAD_MODE):
        return 0
    tz = _local_tz()
    now_local = datetime.now(tz)
    day_str = now_local.date().isoformat()
    try:
        with _get_conn() as conn:
            rows = conn.execute(
                """
                SELECT planned_at
                FROM tasks
                WHERE state IN ('PLANNED', 'SCHEDULED')
                  AND planned_at IS NOT NULL
                """
            ).fetchall()
            tasks_today = 0
            for row in rows:
                try:
                    s = str(row["planned_at"]).replace("Z", "+00:00")
                    dt = datetime.fromisoformat(s)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    if dt.astimezone(tz).date().isoformat() == day_str:
                        tasks_today += 1
                except Exception:
                    continue
            reg_rows = conn.execute(
                """
                SELECT status
                FROM regulation_runs
                WHERE due_date = ?
                  AND status IN ('OPEN', 'DUE')
                """,
                (day_str,),
            ).fetchall()
            regs_due, reg_status_counts = _p5_regs_due_counts(reg_rows)
            backlog = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM tasks
                WHERE planned_at IS NULL
                  AND (
                    state = 'NEW'
                    OR ((state IS NULL OR state = '') AND status = 'NEW')
                  )
                """
            ).fetchone()["cnt"]
    except Exception as exc:
        logging.warning("P5_OVERLOAD action=fetch_error err=%s", str(exc)[:200])
        return 0
    for status, count in reg_status_counts.items():
        logging.info(
            "P5_OVERLOAD action=due_today_status run_status=%s value=%s day=%s",
            status,
            count,
            day_str,
        )
    signals = _p5_overload_signals(day_str, int(tasks_today), int(regs_due), int(backlog))
    for sig, val, thr, day in signals:
        logging.info(
            "P5_OVERLOAD signal=%s value=%s threshold=%s day=%s",
            sig,
            val,
            thr,
            day,
        )
    return len(signals)
def _sync_calendar_for_item(item: dict) -> None:
    # accept sqlite3.Row too
    if not isinstance(item, dict):
        item = dict(item)
    item_id = item["id"]
    title = (item.get("title") or "").strip()
    cal_id = item.get("calendar_event_id")  # None | 'PENDING' | 'FAILED' | '<id>'
    attempts = int(item.get("attempts") or 0)
    if _get_parent_id_from_row(item) is not None:
        logging.info("[%s] calendar_state after=SKIP (subtask)", item_id)
        return

    logging.info("[%s] calendar_state before=%s", item_id, cal_id)

    existing_event_id = str(cal_id).strip() if cal_id and cal_id not in ("PENDING", "FAILED") else None

    # Do not auto-retry FAILED
    if cal_id == "FAILED":
        logging.info("[%s] calendar_state after=%s (skip)", item_id, cal_id)
        return

    # Must have schedule
    if not item.get("start_at") or not item.get("end_at"):
        logging.info("[%s] calendar_state after=%s (skip)", item_id, cal_id or "NULL")
        return

    start = datetime.fromisoformat(item["start_at"])
    end = datetime.fromisoformat(item["end_at"])

    try:
        event_title, event_description = _calendar_title_description_from_item(item)
        event_id = _create_event(
            item_id,
            event_title,
            start,
            end,
            description=event_description,
            existing_event_id=existing_event_id,
        )  # must return str|None
    except Exception as e:
        event_id = None
        err_text, err_transient = _calendar_error_info(e)
    else:
        err_text, err_transient = "", True

    if event_id is None and _CAL_NOT_CONFIGURED_REASON is not None:
        _handle_calendar_not_configured(item_id)
        return

    if not event_id:
        if err_text and not err_transient:
            _mark_calendar_failed(item_id, err_text)
            return
        new_attempts = attempts + 1
        new_state = "FAILED" if new_attempts >= CALENDAR_MAX_ATTEMPTS else "PENDING"
        err_text = err_text or "calendar create failed"
        logging.warning("calendar error item_id=%s err=%s", item_id, err_text[:200])

        with _get_conn() as conn:
            conn.execute(
                """
                UPDATE items
                SET attempts = ?,
                    last_error = ?,
                    calendar_event_id = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    new_attempts,
                    err_text[:200],
                    new_state,
                    datetime.now(timezone.utc).isoformat(),
                    item_id,
                ),
            )
            conn.commit()

        logging.info("[%s] calendar_state after=%s", item_id, new_state)
        if new_state == "FAILED":
            _tg_notify_calendar_dead(item_id)
        return

    if existing_event_id:
        with _get_conn() as conn:
            conn.execute(
                """
                UPDATE items
                SET last_error = NULL,
                    updated_at = ?,
                    calendar_ok_at = COALESCE(calendar_ok_at, ?)
                WHERE id = ?
                """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    datetime.now(timezone.utc).isoformat(),
                    item_id,
                ),
            )
            conn.commit()
        logging.info("[%s] calendar_state after=%s (updated)", item_id, event_id)
        return

    # success: store event_id for NULL or PENDING
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE items
            SET calendar_event_id = ?,
                last_error = NULL,
                updated_at = ?,
                calendar_ok_at = COALESCE(calendar_ok_at, ?)
            WHERE id = ?
              AND (calendar_event_id IS NULL OR calendar_event_id = 'PENDING')
            """,
            (
                event_id,
                datetime.now(timezone.utc).isoformat(),
                datetime.now(timezone.utc).isoformat(),
                item_id,
            ),
        )
        conn.commit()

    logging.info("[%s] calendar_state after=%s", item_id, event_id)
    _tg_notify_calendar_success(item_id)


def _process_items() -> None:
    _retry_pending_events()
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, title, type, status, parent_id, parent_id_int
            FROM items
            WHERE status = 'inbox'
              AND (start_at IS NULL OR start_at = '')
              AND (calendar_event_id IS NULL OR calendar_event_id = '')
            ORDER BY id ASC
            LIMIT 20
            """
        ).fetchall()

    for row in rows:
        row = as_dict(row)
        item_id = row.get("id")
        if item_id is None:
            continue
        if _get_parent_id_from_row(row) is not None:
            continue
        title = row.get("title") or ""
        start = _extract_datetime(title)
        if not start or _is_time_ambiguous(title):
            continue
        end = start + timedelta(minutes=MEETING_DEFAULT_MINUTES)
        reserved = False
        with _get_conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if P2_ENFORCE_STATUS:
                validate_task_status(row, "active", 0)
            cur = conn.execute(
                """
                UPDATE items
                SET status = 'active',
                    start_at = ?,
                    end_at = ?,
                    calendar_event_id = 'PENDING'
                WHERE id = ?
                  AND status = 'inbox'
                  AND (start_at IS NULL OR start_at = '')
                  AND (calendar_event_id IS NULL OR calendar_event_id = '')
                """,
                (start.isoformat(), end.isoformat(), item_id),
            )
            reserved = cur.rowcount == 1
            conn.commit()
        if not reserved:
            continue
        with _get_conn() as conn:
            row_state = conn.execute(
                "SELECT calendar_event_id FROM items WHERE id = ?",
                (item_id,),
            ).fetchone()
        row_state = as_dict(row_state) if row_state else None
        cal_before = row_state.get("calendar_event_id") if row_state else None
        logging.info("[%s] calendar_state before=%s", item_id, cal_before)
        if cal_before and cal_before != "PENDING":
            logging.info("[%s] calendar_state after=%s", item_id, cal_before)
            continue
        try:
            event_title, event_description = _calendar_title_description_from_item(row)
            event_id = _create_event(item_id, event_title, start, end, description=event_description)
        except Exception as exc:
            event_id = None
            err_text, err_transient = _calendar_error_info(exc)
        else:
            err_text, err_transient = "", True
        if event_id is None and _CAL_NOT_CONFIGURED_REASON is not None:
            _handle_calendar_not_configured(int(item_id))
            continue
        if not event_id:
            if err_text and not err_transient:
                _mark_calendar_failed(int(item_id), err_text)
                continue
            err_text = err_text or "calendar create failed"
            logging.warning("event create failed for item %s err=%s", item_id, err_text[:200])
            logging.warning("event create failed for item %s", item_id)
            with _get_conn() as conn:
                conn.execute(
                    """
                    UPDATE items
                    SET attempts = attempts + 1,
                        last_error = ?,
                        calendar_event_id = 'PENDING',
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (err_text[:200], datetime.now(timezone.utc).isoformat(), item_id),
                )
                conn.commit()
            logging.info("[%s] calendar_state after=%s", item_id, "PENDING")
            continue
        with _get_conn() as conn:
            conn.execute(
                """
                UPDATE items
                SET calendar_event_id = ?,
                    calendar_ok_at = COALESCE(calendar_ok_at, ?),
                    updated_at = ?
                WHERE id = ? AND calendar_event_id = 'PENDING'
                """,
                (
                    event_id,
                    datetime.now(timezone.utc).isoformat(),
                    datetime.now(timezone.utc).isoformat(),
                    item_id,
                ),
            )
            conn.commit()
        logging.info("[%s] calendar_state after=%s", item_id, event_id)
        _tg_notify_calendar_success(int(item_id))


def _retry_pending_events() -> None:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, title, start_at, end_at, attempts, parent_id, parent_id_int
            FROM items
            WHERE calendar_event_id = 'PENDING'
              AND attempts < ?
              AND status = 'active'
              AND start_at IS NOT NULL
              AND end_at IS NOT NULL
              AND (source IS NULL OR source != 'canceled')
            ORDER BY id ASC
            LIMIT 20
            """,
            (MAX_ATTEMPTS,),
        ).fetchall()

    for row in rows:
        row = as_dict(row)
        item_id = row.get("id")
        if item_id is None:
            continue
        if _get_parent_id_from_row(row) is not None:
            continue
        title = row.get("title") or ""
        start_at = row.get("start_at")
        end_at = row.get("end_at")
        attempts = int(row.get("attempts") or 0)
        logging.info("retry start item_id=%s attempts=%s", item_id, attempts)
        logging.info("[%s] calendar_state before=%s", item_id, "PENDING")

        try:
            if not start_at or not end_at:
                continue
            start = datetime.fromisoformat(str(start_at))
            end = datetime.fromisoformat(str(end_at))
        except ValueError:
            continue

        claimed = False
        with _get_conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                """
                UPDATE items
                SET attempts = attempts + 1,
                    updated_at = ?,
                    last_error = NULL
                WHERE id = ?
                  AND calendar_event_id = 'PENDING'
                  AND attempts < ?
                """,
                (datetime.now(timezone.utc).isoformat(), item_id, MAX_ATTEMPTS),
            )
            claimed = cur.rowcount == 1
            conn.commit()
        if not claimed:
            continue

        try:
            event_title, event_description = _calendar_title_description_from_item(row)
            event_id = _create_event(item_id, event_title, start, end, description=event_description)
        except Exception as exc:
            event_id = None
            err_text, err_transient = _calendar_error_info(exc)
        else:
            err_text, err_transient = "", True

        if event_id is None and _CAL_NOT_CONFIGURED_REASON is not None:
            _handle_calendar_not_configured(int(item_id))
            continue

        if event_id:
            with _get_conn() as conn:
                conn.execute(
                    """
                    UPDATE items
                    SET calendar_event_id = ?,
                        last_error = NULL,
                        updated_at = ?,
                        calendar_ok_at = COALESCE(calendar_ok_at, ?)
                    WHERE id = ? AND calendar_event_id = 'PENDING'
                    """,
                    (
                        event_id,
                        datetime.now(timezone.utc).isoformat(),
                        datetime.now(timezone.utc).isoformat(),
                        item_id,
                    ),
                )
                conn.commit()
            logging.info("retry success item_id=%s event_id=%s", item_id, event_id)
            logging.info("[%s] calendar_state after=%s", item_id, event_id)
            _tg_notify_calendar_success(int(item_id))
            continue

        if err_text and not err_transient:
            _mark_calendar_failed(int(item_id), err_text)
            continue

        logging.warning("retry failed item_id=%s err=%s", item_id, (err_text or "calendar create failed")[:200])
        with _get_conn() as conn:
            conn.execute(
                """
                UPDATE items
                SET last_error = ?,
                    updated_at = ?
                WHERE id = ? AND calendar_event_id = 'PENDING'
                """,
                ((err_text or "calendar create failed")[:200], datetime.now(timezone.utc).isoformat(), item_id),
            )
            conn.commit()
        logging.info("[%s] calendar_state after=%s", item_id, "PENDING")

        with _get_conn() as conn:
            row2 = conn.execute(
                "SELECT attempts FROM items WHERE id = ?", (item_id,)
            ).fetchone()
            row2 = as_dict(row2) if row2 else None
            if not row2:
                continue
            if int(row2.get("attempts") or 0) >= MAX_ATTEMPTS:
                conn.execute(
                    """
                    UPDATE items
                    SET calendar_event_id = 'FAILED',
                        updated_at = ?
                    WHERE id = ? AND calendar_event_id = 'PENDING'
                    """,
                    (datetime.now(timezone.utc).isoformat(), item_id),
                )
                conn.commit()
                logging.info("marked FAILED item_id=%s", item_id)
# LEGACY_QUEUE_ITEMS_END


_legacy_process_queue_item_impl = _process_queue_item
_legacy_find_item_for_queue_delivery_impl = _find_item_for_queue_delivery
_legacy_re_deliver_done_items_impl = re_deliver_done_items
_legacy_sync_calendar_for_item_impl = _sync_calendar_for_item
_legacy_process_items_impl = _process_items
_legacy_retry_pending_events_impl = _retry_pending_events


def _legacy_queue_deps() -> dict[str, Any]:
    return {
        "get_conn": _get_conn,
        "as_dict": as_dict,
        "reap_claims": reap_claims,
        "is_stale_queue_row": _is_stale_queue_row,
        "worker_id": WORKER_ID,
        "b2_replay_scan_limit": B2_REPLAY_SCAN_LIMIT,
        "b2_replay_max_age_sec": B2_REPLAY_MAX_AGE_SEC,
        "b2_claim_lease_sec": B2_CLAIM_LEASE_SEC,
        "b2_max_attempts": B2_MAX_ATTEMPTS,
        "load_clarify_state": _load_clarify_state,
        "save_clarify_state": _save_clarify_state,
        "prune_clarify_state": _prune_clarify_state,
        "validate_task_status": validate_task_status,
        "p2_enforce_status": P2_ENFORCE_STATUS,
        "get_parent_id_from_row": _get_parent_id_from_row,
        "require_lastrowid": _require_lastrowid,
        "impl_process_queue_item": _legacy_process_queue_item_impl,
        "impl_find_item_for_queue_delivery": _legacy_find_item_for_queue_delivery_impl,
        "impl_re_deliver_done_items": _legacy_re_deliver_done_items_impl,
        "impl_sync_calendar_for_item": _legacy_sync_calendar_for_item_impl,
        "impl_process_items": _legacy_process_items_impl,
        "impl_retry_pending_events": _legacy_retry_pending_events_impl,
    }


def _queue_reaper() -> None:
    _legacy_queue_module._queue_reaper(deps=_legacy_queue_deps())


def _queue_claim() -> dict | None:
    return _legacy_queue_module._queue_claim(deps=_legacy_queue_deps())


def _queue_mark(queue_id: int, status: str, last_error: str | None = None) -> None:
    _legacy_queue_module._queue_mark(queue_id, status, last_error, deps=_legacy_queue_deps())


def _queue_requeue_failed(limit: int) -> int:
    return _legacy_queue_module._queue_requeue_failed(limit, deps=_legacy_queue_deps())


def _enqueue_clarify(chat_id: int, item: dict) -> int:
    return _legacy_queue_module._enqueue_clarify(chat_id, item, deps=_legacy_queue_deps())


def _update_item_status(item_id: int, new_status: str) -> None:
    _legacy_queue_module._update_item_status(item_id, new_status, deps=_legacy_queue_deps())


def create_task(
    title: str,
    *,
    status: str = "inbox",
    from_inbox_item_id: int | None = None,
) -> int:
    return _legacy_queue_module.create_task(
        title,
        status=status,
        from_inbox_item_id=from_inbox_item_id,
        deps=_legacy_queue_deps(),
    )


def create_subtask(
    parent_id: int,
    title: str,
    *,
    status: str = "todo",
) -> int:
    return _legacy_queue_module.create_subtask(
        parent_id,
        title,
        status=status,
        deps=_legacy_queue_deps(),
    )


def _process_queue_item(row: dict) -> None:
    _legacy_queue_module._process_queue_item(row, deps=_legacy_queue_deps())


def _find_item_for_queue_delivery(queue_row: dict) -> dict:
    return _legacy_queue_module._find_item_for_queue_delivery(queue_row, deps=_legacy_queue_deps())


def re_deliver_done_items(limit: int = 50) -> dict:
    return _legacy_queue_module.re_deliver_done_items(limit=limit, deps=_legacy_queue_deps())


def _sync_calendar_for_item(item: dict) -> None:
    _legacy_queue_module._sync_calendar_for_item(item, deps=_legacy_queue_deps())


def _process_items() -> None:
    _legacy_queue_module._process_items(deps=_legacy_queue_deps())


def _retry_pending_events() -> None:
    _legacy_queue_module._retry_pending_events(deps=_legacy_queue_deps())


def _legacy_worker_queue_enabled() -> bool:
    value = str(os.getenv("ALLOW_LEGACY_WORKER_QUEUE", "") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def main() -> None:
    _init_db()
    assert hasattr(sqlite3.Row, "__getitem__") and not hasattr(sqlite3.Row, "get")
    os.makedirs("/tmp", exist_ok=True)
    with open("/tmp/worker.ok", "w", encoding="utf-8") as marker:
        marker.write("ok\n")
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), force=True)
    logging.info("organizer-worker started")
    _validate_calendar_service_account_on_startup()
    _smoke_check_google_calendar_deps()
    logging.info(
        "P3_CALENDAR_MODE mode=%s raw=%s",
        CALENDAR_SYNC_MODE,
        _CALENDAR_SYNC_MODE_RAW,
    )
    _start_command_server()
    if ASR_DT_SELF_CHECK:
        _selfcheck_asr_datetime()
    legacy_queue_enabled = _legacy_worker_queue_enabled()
    if legacy_queue_enabled:
        logging.warning("legacy_worker_queue_enabled")
    else:
        logging.warning("legacy_worker_queue_disabled active_runtime_only=true")

    last_heartbeat = 0.0
    last_requeue = 0.0
    last_reg_nudge = 0.0
    last_p5_tick = 0.0
    last_drift_fast = 0.0
    last_drift_full_day = ""
    while True:
        try:
            now = time.time()
            if REG_NUDGES_INTERVAL_SEC > 0 and (now - last_reg_nudge) >= REG_NUDGES_INTERVAL_SEC:
                _p4_reg_nudge_tick()
                last_reg_nudge = now
            if P5_TICK_INTERVAL_SEC > 0 and (now - last_p5_tick) >= P5_TICK_INTERVAL_SEC:
                day_str = datetime.now(_local_tz()).date().isoformat()
                _p5_nudge_reset_if_new_day(day_str)
                drift_count = _p5_drift_tick()
                overload_count = _p5_overload_tick()
                if drift_count:
                    _P5_DRIFT_COUNT_TODAY += int(drift_count)
                if overload_count:
                    _P5_OVERLOAD_COUNT_TODAY += int(overload_count)
                _p5_nudge_emit_if_needed(day_str)
                last_p5_tick = now
            if CALENDAR_DRIFT_FAST_CHECK_SEC > 0 and (now - last_drift_fast) >= CALENDAR_DRIFT_FAST_CHECK_SEC:
                now_local = datetime.now(_local_tz())
                fast_from = (now_local - timedelta(days=CALENDAR_DRIFT_FAST_LOOKBACK_DAYS)).isoformat()
                fast_to = (now_local + timedelta(days=CALENDAR_DRIFT_FAST_LOOKAHEAD_DAYS)).isoformat()
                _runtime_check_calendar_drift(from_value=fast_from, to_value=fast_to, notify=True)
                last_drift_fast = now
            now_local = datetime.now(_local_tz())
            full_day = now_local.date().isoformat()
            full_due = (
                now_local.hour > CALENDAR_DRIFT_FULL_HOUR
                or (now_local.hour == CALENDAR_DRIFT_FULL_HOUR and now_local.minute >= CALENDAR_DRIFT_FULL_MINUTE)
            )
            if full_due and last_drift_full_day != full_day:
                full_from = (now_local - timedelta(days=CALENDAR_DRIFT_FULL_LOOKBACK_DAYS)).isoformat()
                full_to = (now_local + timedelta(days=CALENDAR_DRIFT_FULL_LOOKAHEAD_DAYS)).isoformat()
                _runtime_check_calendar_drift(from_value=full_from, to_value=full_to, notify=True)
                last_drift_full_day = full_day
            if legacy_queue_enabled:
                _queue_reaper()
                if B2_REQUEUE_FAILED_EVERY_SEC > 0 and (now - last_requeue) >= B2_REQUEUE_FAILED_EVERY_SEC:
                    moved = _queue_requeue_failed(limit=B2_REQUEUE_FAILED_BATCH)
                    if moved:
                        logging.info("requeued FAILED->NEW: %s", moved)
                    last_requeue = now
                row = _queue_claim()
                if row:
                    _process_queue_item(row)
                else:
                    time.sleep(B2_IDLE_SLEEP_SEC)
                _process_items()
                if CALENDAR_SYNC_MODE != "off":
                    _p3_calendar_create_tick()
                    if CALENDAR_SYNC_MODE == "full":
                        _p3_calendar_update_tick()
                        _p4_calendar_cancel_tick()
        except Exception as exc:
            logging.exception("worker error: %s", exc)
        now = time.time()
        if now - last_heartbeat >= WORKER_HEARTBEAT_SEC:
            try:
                with open("/tmp/worker.ok", "w", encoding="utf-8") as marker:
                    marker.write("ok\n")
            except Exception:
                pass
            last_heartbeat = now
        time.sleep(WORKER_INTERVAL_SEC)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Organizer worker runtime")
    parser.add_argument("--re-deliver", action="store_true", help="Re-send undelivered Telegram results for recent DONE queue rows")
    parser.add_argument("--limit", type=int, default=50, help="Max DONE queue rows to inspect for re-delivery")
    args = parser.parse_args()
    if args.re_deliver:
        _init_db()
        summary = re_deliver_done_items(limit=max(1, int(args.limit)))
        print(json.dumps(summary, ensure_ascii=False))
    else:
        main()
