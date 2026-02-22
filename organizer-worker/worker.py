import importlib
import importlib.util as importlib_util
import json
import logging
import os
import re
import sqlite3
import time
import socket
import sys
import threading
from pathlib import Path
from datetime import datetime, timedelta, timezone, date
from typing import Any
from http.server import BaseHTTPRequestHandler, HTTPServer
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


_SRC_DIR = Path(__file__).resolve().parent / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))
_P2_RUNTIME_PATH = _SRC_DIR / "p2_tasks_runtime.py"
_P2_SPEC = importlib_util.spec_from_file_location("p2_tasks_runtime", _P2_RUNTIME_PATH)
if _P2_SPEC is None or _P2_SPEC.loader is None:
    raise ImportError(f"Cannot load p2 runtime module from {_P2_RUNTIME_PATH}")
p2: Any = importlib_util.module_from_spec(_P2_SPEC)
sys.modules.setdefault("p2_tasks_runtime", p2)
_P2_SPEC.loader.exec_module(p2)
from organizer_worker.handlers import dispatch_intent

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
ASR_SERVICE_URL = os.getenv("ASR_SERVICE_URL", "http://asr-service:8001")
TG_HTTP_CONNECT_TIMEOUT = int(os.getenv("TG_HTTP_CONNECT_TIMEOUT", "3"))
TG_HTTP_READ_TIMEOUT = int(os.getenv("TG_HTTP_READ_TIMEOUT", "90"))
TG_HTTP_RETRIES = int(os.getenv("TG_HTTP_RETRIES", "2"))
TG_HTTP_RETRY_SLEEP = float(os.getenv("TG_HTTP_RETRY_SLEEP", "0.3"))
ASR_HTTP_READ_TIMEOUT = int(os.getenv("ASR_HTTP_READ_TIMEOUT", "180"))
MEETING_DEFAULT_MINUTES = int(os.getenv("MEETING_DEFAULT_MINUTES", "30"))
LOCAL_TZ_OFFSET_MIN = int(os.getenv("LOCAL_TZ_OFFSET_MIN", "180"))  # +03:00 default
ORGANIZER_API_URL = os.getenv("ORGANIZER_API_URL", "http://organizer-api:8000")
DEFAULT_HOUR = int(os.getenv("DT_DEFAULT_HOUR", "10"))
DEFAULT_MINUTE = int(os.getenv("DT_DEFAULT_MINUTE", "0"))
MARKER_HOUR = int(os.getenv("DT_MARKER_HOUR", "6"))
MARKER_MINUTE = int(os.getenv("DT_MARKER_MINUTE", "0"))
DT_REQUIRE_AMPM_FOR_SHORT_HOURS = os.getenv("DT_REQUIRE_AMPM_FOR_SHORT_HOURS", "1") == "1"
DT_SHORT_HOUR_MAX = int(os.getenv("DT_SHORT_HOUR_MAX", "12"))
DT_AMBIGUOUS_MARKER_HOUR = int(os.getenv("DT_AMBIGUOUS_MARKER_HOUR", "6"))
DT_AMBIGUOUS_MARKER_MINUTE = int(os.getenv("DT_AMBIGUOUS_MARKER_MINUTE", "0"))
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
B2_IDLE_SLEEP_SEC = float(os.getenv("B2_IDLE_SLEEP_SEC", "0.5"))
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
    return timezone(timedelta(minutes=LOCAL_TZ_OFFSET_MIN))

def as_dict(row: sqlite3.Row | dict | None) -> dict:
    if row is None:
        return {}
    return row if isinstance(row, dict) else dict(row)


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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
      - "встреча завтра в 9" -> ambiguous -> inbox + 06:00
      - "встреча завтра в 9 утра" -> active + 09:00
      - "встреча завтра в 19" -> active + 19:00
      - "встреча завтра в 7 вечера" -> active + 19:00
      - "на следующей неделе" -> marker 06:00 + inbox
      - "завтра" -> default 10:00 + inbox
      - "в 9 третьего" -> ближайшее 03-е число в 09:00
      - "третьего в 9" -> ближайшее 03-е число в 09:00
      - "четвертого в 3" -> ближайшее 04-е число в 03:00
      - "встреча 3-го в 9.30" -> ближайшее 03-е число в 09:30
      - "4-го в 15:30" -> ближайшее 04-е число в 15:30
      - "встреча третьего" -> ближайшее 03-е число в 06:00
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

    period_like = any(x in t for x in (
        "на этой неделе", "на прошлой неделе", "на следующей неделе",
        "в этом месяце", "в прошлом месяце", "в следующем месяце",
        "в этом году", "в прошлом году", "в следующем году", "через ",
    ))
    tm = _parse_time_ru(t)
    time_ambiguous = False
    if tm:
        hh, mm = tm
        time_ambiguous = _is_time_ambiguous(t)
        if time_ambiguous:
            logging.info("time_ambiguous=True text=%r", text[:200])
    else:
        hh, mm = (MARKER_HOUR, MARKER_MINUTE) if period_like else (DEFAULT_HOUR, DEFAULT_MINUTE)
    time_explicit = tm is not None

    rel_date = _resolve_relative_period(t, now_local)
    if rel_date:
        hh_use, mm_use = (
            (DT_AMBIGUOUS_MARKER_HOUR, DT_AMBIGUOUS_MARKER_MINUTE) if time_ambiguous else (hh, mm)
        )
        return datetime(rel_date.year, rel_date.month, rel_date.day, hh_use, mm_use, tzinfo=tz)

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
                    return datetime(target_year, target_month, d, MARKER_HOUR, MARKER_MINUTE, tzinfo=tz)

                hh_use, mm_use = (
                    (DT_AMBIGUOUS_MARKER_HOUR, DT_AMBIGUOUS_MARKER_MINUTE) if time_ambiguous else (hh, mm)
                )
                return datetime(target_year, target_month, d, hh_use, mm_use, tzinfo=tz)

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
        hh_use, mm_use = (
            (DT_AMBIGUOUS_MARKER_HOUR, DT_AMBIGUOUS_MARKER_MINUTE) if time_ambiguous else (hh, mm)
        )
        dt = datetime(now_local.year, mon, d, hh_use, mm_use, tzinfo=tz)
        if dt <= now_local:
            dt = datetime(now_local.year + 1, mon, d, hh_use, mm_use, tzinfo=tz)
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
        hh_use, mm_use = (
            (DT_AMBIGUOUS_MARKER_HOUR, DT_AMBIGUOUS_MARKER_MINUTE) if time_ambiguous else (hh, mm)
        )
        dt = datetime(abs_date.year, abs_date.month, abs_date.day, hh_use, mm_use, tzinfo=tz)
        if y_raw is None and time_explicit and dt <= now_local:
            y2 = now_local.year + 1
            d2 = _clamp_day(y2, mo, d)
            dt = datetime(y2, mo, d2, hh_use, mm_use, tzinfo=tz)
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
        hh_use, mm_use = (
            (DT_AMBIGUOUS_MARKER_HOUR, DT_AMBIGUOUS_MARKER_MINUTE) if time_ambiguous else (hh, mm)
        )
        return datetime(target.year, target.month, target.day, hh_use, mm_use, tzinfo=tz)

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
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _init_db() -> None:
    with _get_conn() as conn:
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


def _queue_reaper() -> None:
    with _get_conn() as conn:
        reap_claims(conn, time.time())
        conn.commit()


def _queue_claim() -> dict | None:
    with _get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            """
            UPDATE inbox_queue
            SET status='CLAIMED',
                claimed_by=?,
                claimed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                lease_until=strftime('%Y-%m-%dT%H:%M:%fZ','now', '+' || ? || ' seconds'),
                attempts=attempts+1,
                updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = (
              SELECT id
              FROM inbox_queue
              WHERE status='NEW'
              ORDER BY priority ASC, id ASC
              LIMIT 1
            )
            """,
            (WORKER_ID, str(B2_CLAIM_LEASE_SEC)),
        )
        if cur.rowcount != 1:
            conn.commit()
            return None
        row = conn.execute(
            """
            SELECT *
            FROM inbox_queue
            WHERE status='CLAIMED' AND claimed_by=?
            ORDER BY claimed_at DESC
            LIMIT 1
            """,
            (WORKER_ID,),
        ).fetchone()
        conn.commit()
        return dict(row) if row else None


def _queue_mark(queue_id: int, status: str, last_error: str | None = None) -> None:
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE inbox_queue
            SET status=?,
                last_error=?,
                updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id=?
            """,
            (status, last_error, queue_id),
        )
        conn.commit()


def _queue_requeue_failed(limit: int) -> int:
    """
    Bounded auto-requeue: FAILED -> NEW for tasks with attempts < B2_MAX_ATTEMPTS
    """
    if limit <= 0:
        return 0
    with _get_conn() as conn:
        cur = conn.execute(
            f"""
            UPDATE inbox_queue
            SET status='NEW',
                updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id IN (
              SELECT id
              FROM inbox_queue
              WHERE status='FAILED'
                AND attempts < ?
              ORDER BY id ASC
              LIMIT {int(limit)}
            )
            """,
            (B2_MAX_ATTEMPTS,),
        )
        conn.commit()
        return int(cur.rowcount or 0)

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


def _tg_send_message(chat_id: int, text: str) -> bool:
    """
    Send message to Telegram user from worker. Best-effort with retries.
    """
    if not TELEGRAM_BOT_TOKEN:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    last_exc: Exception | None = None
    for _ in range(max(1, TG_HTTP_RETRIES)):
        try:
            payload = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=TG_HTTP_READ_TIMEOUT) as resp:
                _ = resp.read()
            return True
        except Exception as exc:
            last_exc = exc
            time.sleep(TG_HTTP_RETRY_SLEEP)
    if last_exc:
        logging.warning("tg notify failed chat_id=%s err=%s", chat_id, str(last_exc)[:200])
    return False


def _tg_send_message_with_keyboard(chat_id: int, text: str, reply_markup: dict) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    last_exc: Exception | None = None
    for _ in range(max(1, TG_HTTP_RETRIES)):
        try:
            payload = json.dumps(
                {"chat_id": chat_id, "text": text, "reply_markup": reply_markup}
            ).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=TG_HTTP_READ_TIMEOUT) as resp:
                _ = resp.read()
            return True
        except Exception as exc:
            last_exc = exc
            time.sleep(TG_HTTP_RETRY_SLEEP)
    if last_exc:
        logging.warning("tg notify failed chat_id=%s err=%s", chat_id, str(last_exc)[:200])
    return False


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


def _tg_mark_created_sent(item_id: int) -> bool:
    return _tg_mark_result_sent(item_id, _TG_RESULT_CREATED)

def _tg_notify_created(item_id: int) -> None:
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
        if not _tg_mark_created_sent(int(item_id)):
            return
        item_type = str(row.get("type") or "task")
        status = str(row.get("status") or "inbox")
        logging.info("tg_notify created item_id=%s", item_id)
        if item_type == "meeting":
            _tg_send_message(chat_id, f"Создано: #{item_id} ({status}). Поставлю в календарь.")
        else:
            _tg_send_message(chat_id, f"Создано: #{item_id} ({status}).")
    except Exception as exc:
        logging.warning("tg notify created failed item_id=%s err=%s", item_id, str(exc)[:200])


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
    start_at = dt.isoformat() if dt else None
    end_at = (dt + timedelta(minutes=MEETING_DEFAULT_MINUTES)).isoformat() if dt else None
    return item_type, status, start_at, end_at, dt, time_ambiguous


def _tg_notify_calendar_success(item_id: int) -> None:
    try:
        _tg_notify_created(int(item_id))
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
            _tg_notify_created(int(item_id))
        if success_sent:
            return
        if not _tg_mark_result_sent(int(item_id), _TG_RESULT_SUCCESS):
            return
        title = (row.get("title") or "").strip() or "без названия"
        start_at = str(row.get("start_at") or "")
        when_human = _format_start_at_local(start_at) or "без времени"
        text = f"✅ В календаре: {when_human} — {title}"
        logging.info("tg_notify success item_id=%s", item_id)
        _tg_send_message(chat_id, text)
    except Exception as exc:
        logging.warning("tg notify success failed item_id=%s err=%s", item_id, str(exc)[:200])


def _tg_notify_calendar_dead(item_id: int) -> None:
    try:
        _tg_notify_created(int(item_id))
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
            _tg_notify_created(int(item_id))
        if dead_sent:
            return
        if not _tg_mark_result_sent(int(item_id), _TG_RESULT_DEAD):
            return
        logging.info("tg_notify dead item_id=%s", item_id)
        _tg_send_message(
            chat_id,
            "⚠️ Не удалось добавить в календарь. Задача сохранена, верну в Inbox. [CAL-DEAD]",
        )
    except Exception as exc:
        logging.warning("tg notify dead failed item_id=%s err=%s", item_id, str(exc)[:200])


def _asr_transcribe(audio: bytes) -> str:
    files = {"file": ("voice.ogg", audio, "audio/ogg")}
    resp = requests.post(f"{ASR_SERVICE_URL}/transcribe", files=files, timeout=(3, ASR_HTTP_READ_TIMEOUT))
    resp.raise_for_status()
    data = resp.json()
    return (data.get("text") or "").strip()

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


def _enqueue_clarify(chat_id: int, item: dict) -> int:
    state = _load_clarify_state()
    _prune_clarify_state(state, time.time())
    st = state.get(str(chat_id)) or {"queue": []}
    q = st.get("queue") or []
    q.append(item)
    st["queue"] = q
    state[str(chat_id)] = st
    _save_clarify_state(state)
    return len(q)


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
        _tg_notify_created(item_id)
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
        _tg_notify_created(item_id)
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


class _CommandHandler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, payload: dict) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self) -> dict:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        raw = self.rfile.read(length) if length > 0 else b"{}"
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("invalid json body")
        return data

    def do_GET(self):  # noqa: N802 - stdlib API
        if self.path == "/health":
            self._send_json(200, {"ok": True})
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self):  # noqa: N802 - stdlib API
        try:
            data = self._read_json()
            if self.path == "/runtime/command":
                trace_id = data.get("trace_id")
                if not isinstance(trace_id, str) or not trace_id.strip():
                    raise ValueError("trace_id is required")
                command = data.get("command")
                if not isinstance(command, dict):
                    raise ValueError("command is required")
                intent = command.get("intent")
                if not isinstance(intent, str) or not intent.strip():
                    raise ValueError("command.intent is required")
                entities = command.get("entities")
                if not isinstance(entities, dict):
                    raise ValueError("command.entities must be object")
                res = dispatch_intent(command)
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/create_task":
                res = cmd_create_task(
                    data.get("title") or "",
                    data.get("source_msg_id"),
                    data.get("parent_type"),
                    data.get("parent_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/create_direction":
                if not data.get("title"):
                    raise ValueError("title is required")
                res = cmd_create_direction(
                    data.get("title") or "",
                    data.get("note"),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/create_project":
                if not data.get("title"):
                    raise ValueError("title is required")
                direction_id = data.get("direction_id")
                res = cmd_create_project(
                    data.get("title") or "",
                    int(direction_id) if direction_id is not None else None,
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/convert_direction_to_project":
                if data.get("direction_id") is None:
                    raise ValueError("direction_id is required")
                res = cmd_convert_direction_to_project(
                    _require_int_field(data.get("direction_id"), "direction_id"),
                    data.get("title"),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/start_cycle":
                if not data.get("type"):
                    raise ValueError("type is required")
                res = cmd_start_cycle(
                    data.get("type") or "",
                    data.get("period_key"),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/close_cycle":
                if data.get("cycle_id") is None:
                    raise ValueError("cycle_id is required")
                status = data.get("status") or ""
                status_norm = status.strip().upper()
                if status_norm not in {"DONE", "SKIPPED"}:
                    raise ValueError("status must be DONE or SKIPPED")
                res = cmd_close_cycle(
                    _require_int_field(data.get("cycle_id"), "cycle_id"),
                    status_norm,
                    data.get("summary"),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/add_cycle_outcome":
                if data.get("cycle_id") is None:
                    raise ValueError("cycle_id is required")
                if not data.get("kind"):
                    raise ValueError("kind is required")
                if not data.get("text"):
                    raise ValueError("text is required")
                res = cmd_add_cycle_outcome(
                    _require_int_field(data.get("cycle_id"), "cycle_id"),
                    data.get("kind") or "",
                    data.get("text") or "",
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/add_cycle_goal":
                if data.get("cycle_id") is None:
                    raise ValueError("cycle_id is required")
                if not data.get("text"):
                    raise ValueError("text is required")
                res = cmd_add_cycle_goal(
                    _require_int_field(data.get("cycle_id"), "cycle_id"),
                    data.get("text") or "",
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/continue_cycle_goal":
                if data.get("goal_id") is None:
                    raise ValueError("goal_id is required")
                if data.get("target_cycle_id") is None:
                    raise ValueError("target_cycle_id is required")
                res = cmd_continue_cycle_goal(
                    _require_int_field(data.get("goal_id"), "goal_id"),
                    _require_int_field(data.get("target_cycle_id"), "target_cycle_id"),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/update_cycle_goal_status":
                if data.get("goal_id") is None:
                    raise ValueError("goal_id is required")
                if not data.get("status"):
                    raise ValueError("status is required")
                res = cmd_update_cycle_goal_status(
                    _require_int_field(data.get("goal_id"), "goal_id"),
                    str(data.get("status")),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/ensure_user_settings":
                if not data.get("user_id"):
                    raise ValueError("user_id is required")
                res = _ensure_user_settings(str(data.get("user_id")))
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/set_signals_enabled":
                if not data.get("user_id"):
                    raise ValueError("user_id is required")
                enabled = data.get("enabled")
                if enabled is None:
                    raise ValueError("enabled is required")
                res = cmd_set_signals_enabled(str(data.get("user_id")), int(enabled))
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/snooze_nudge":
                if not data.get("user_id"):
                    raise ValueError("user_id is required")
                nudge_key = data.get("nudge_key") or NUDGE_SIGNALS_KEY
                days = int(data.get("days") or 90)
                res = cmd_snooze_nudge(str(data.get("user_id")), str(nudge_key), days)
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/set_module_enabled":
                if not data.get("user_id"):
                    raise ValueError("user_id is required")
                if not data.get("module"):
                    raise ValueError("module is required")
                enabled = data.get("enabled")
                if enabled is None:
                    raise ValueError("enabled is required")
                res = cmd_set_module_enabled(str(data.get("user_id")), str(data.get("module")), int(enabled))
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/set_modules_enabled_bulk":
                if not data.get("user_id"):
                    raise ValueError("user_id is required")
                if data.get("overload_enabled") is None or data.get("drift_enabled") is None:
                    raise ValueError("overload_enabled and drift_enabled are required")
                res = cmd_set_modules_enabled_bulk(
                    str(data.get("user_id")),
                    _require_int_field(data.get("overload_enabled"), "overload_enabled"),
                    _require_int_field(data.get("drift_enabled"), "drift_enabled"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/create_subtask":
                if data.get("task_id") is None:
                    raise ValueError("task_id is required")
                res = cmd_create_subtask(
                    _require_int_field(data.get("task_id"), "task_id"),
                    data.get("title") or "",
                    data.get("status") or "NEW",
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/complete_task":
                if data.get("task_id") is None:
                    raise ValueError("task_id is required")
                res = cmd_complete_task(_require_int_field(data.get("task_id"), "task_id"))
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/complete_subtask":
                if data.get("subtask_id") is None:
                    raise ValueError("subtask_id is required")
                res = cmd_complete_subtask(_require_int_field(data.get("subtask_id"), "subtask_id"))
                self._send_json(200, res)
                return
            if self.path == "/p2/commands/plan_task":
                if data.get("task_id") is None:
                    raise ValueError("task_id is required")
                if not data.get("planned_at"):
                    raise ValueError("planned_at is required")
                res = cmd_plan_task(_require_int_field(data.get("task_id"), "task_id"), str(data.get("planned_at")))
                self._send_json(200, res)
                return
            if self.path == "/p4/commands/create_regulation":
                if not data.get("title"):
                    raise ValueError("title is required")
                day_val = data.get("day_of_month")
                res = cmd_create_regulation(
                    data.get("title") or "",
                    int(day_val) if day_val is not None else 1,
                    data.get("note"),
                    data.get("due_time_local"),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p4/commands/archive_regulation":
                if data.get("regulation_id") is None:
                    raise ValueError("regulation_id is required")
                res = cmd_archive_regulation(
                    _require_int_field(data.get("regulation_id"), "regulation_id"),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p4/commands/update_regulation_schedule":
                if data.get("regulation_id") is None:
                    raise ValueError("regulation_id is required")
                day_of_month = data.get("day_of_month")
                res = cmd_update_regulation_schedule(
                    _require_int_field(data.get("regulation_id"), "regulation_id"),
                    _require_int_field(day_of_month, "day_of_month") if day_of_month is not None else None,
                    data.get("due_time_local"),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p4/commands/ensure_regulation_runs":
                if not data.get("period_key"):
                    raise ValueError("period_key is required")
                res = cmd_ensure_regulation_runs(
                    data.get("user_id"),
                    str(data.get("period_key")),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p4/commands/mark_regulation_done":
                if data.get("run_id") is None:
                    raise ValueError("run_id is required")
                res = cmd_mark_regulation_done(
                    _require_int_field(data.get("run_id"), "run_id"),
                    data.get("done_at"),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p4/commands/complete_reg_run":
                if data.get("run_id") is None:
                    raise ValueError("run_id is required")
                res = cmd_complete_reg_run(
                    _require_int_field(data.get("run_id"), "run_id"),
                    data.get("done_at"),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p4/commands/skip_reg_run":
                if data.get("run_id") is None:
                    raise ValueError("run_id is required")
                res = cmd_skip_reg_run(
                    _require_int_field(data.get("run_id"), "run_id"),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p4/commands/disable_reg":
                if data.get("regulation_id") is None:
                    raise ValueError("regulation_id is required")
                res = cmd_disable_reg(
                    _require_int_field(data.get("regulation_id"), "regulation_id"),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p7/commands/add_block":
                if data.get("task_id") is None:
                    raise ValueError("task_id is required")
                if not data.get("start_at"):
                    raise ValueError("start_at is required")
                if not data.get("end_at"):
                    raise ValueError("end_at is required")
                res = cmd_add_block(
                    _require_int_field(data.get("task_id"), "task_id"),
                    str(data.get("start_at")),
                    str(data.get("end_at")),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p7/commands/move_block":
                if data.get("block_id") is None:
                    raise ValueError("block_id is required")
                if data.get("delta_minutes") is None:
                    raise ValueError("delta_minutes is required")
                res = cmd_move_block(
                    _require_int_field(data.get("block_id"), "block_id"),
                    _require_int_field(data.get("delta_minutes"), "delta_minutes"),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            if self.path == "/p7/commands/delete_block":
                if data.get("block_id") is None:
                    raise ValueError("block_id is required")
                res = cmd_delete_block(
                    _require_int_field(data.get("block_id"), "block_id"),
                    data.get("source_msg_id"),
                )
                self._send_json(200, res)
                return
            self._send_json(404, {"error": "not found"})
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)[:200]})
        except Exception as exc:
            self._send_json(500, {"error": str(exc)[:200]})

    def log_message(self, format, *args):  # noqa: A003 - stdlib API
        return


def _start_command_server() -> None:
    server = HTTPServer(("0.0.0.0", WORKER_COMMAND_PORT), _CommandHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logging.info("worker_cmd_server started port=%s", WORKER_COMMAND_PORT)


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


def _update_item_status(item_id: int, new_status: str) -> None:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, type, status, parent_id, parent_id_int
            FROM items
            WHERE id = ?
            """,
            (int(item_id),),
        ).fetchone()
        row = as_dict(row)
        if not row:
            raise ValueError("item not found")
        if P2_ENFORCE_STATUS:
            open_subtasks = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM items
                WHERE (parent_id_int = ? OR parent_id = ?)
                  AND status != 'done'
                """,
                (int(item_id), int(item_id)),
            ).fetchone()
            open_cnt = int(as_dict(open_subtasks).get("cnt") or 0)
            validate_task_status(row, new_status, open_cnt)
        conn.execute(
            """
            UPDATE items
            SET status = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (new_status, datetime.now(timezone.utc).isoformat(), int(item_id)),
        )
        conn.commit()


def create_task(
    title: str,
    *,
    status: str = "inbox",
    from_inbox_item_id: int | None = None,
) -> int:
    if from_inbox_item_id is not None:
        with _get_conn() as conn:
            row = conn.execute(
                """
                SELECT id, status, parent_id, parent_id_int
                FROM items
                WHERE id = ?
                """,
                (int(from_inbox_item_id),),
            ).fetchone()
            row = as_dict(row)
            if not row:
                raise ValueError("inbox item not found")
            if _get_parent_id_from_row(row) is not None:
                raise ValueError("cannot promote a subtask to task")
            if str(row.get("status") or "") != "inbox":
                raise ValueError("only inbox items can be promoted to task")
            if P2_ENFORCE_STATUS:
                validate_task_status(row, status, 0)
            conn.execute(
                """
                UPDATE items
                SET type = 'task',
                    status = ?,
                    title = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (status, title, datetime.now(timezone.utc).isoformat(), int(from_inbox_item_id)),
            )
            conn.commit()
        return int(from_inbox_item_id)

    if status not in {"inbox", "active"}:
        raise ValueError("task status must be inbox or active on create")
    with _get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO items (
                type, title, status, parent_id, parent_id_int, created_at
            )
            VALUES ('task', ?, ?, NULL, NULL, ?)
            """,
            (title, status, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        return _require_lastrowid(cur)


def create_subtask(
    parent_id: int,
    title: str,
    *,
    status: str = "todo",
) -> int:
    with _get_conn() as conn:
        parent = conn.execute(
            """
            SELECT id, type, status, parent_id, parent_id_int
            FROM items
            WHERE id = ?
            """,
            (int(parent_id),),
        ).fetchone()
        parent = as_dict(parent)
        if not parent:
            raise ValueError("parent not found")
        if _get_parent_id_from_row(parent) is not None:
            raise ValueError("cannot create subtask under subtask")
        if str(parent.get("type") or "") != "task":
            raise ValueError("parent must be task")
        if str(parent.get("status") or "") == "done":
            raise ValueError("cannot add subtask to done task")
        if status not in {"todo", "done"}:
            raise ValueError("subtask status must be todo or done")

        cur = conn.execute(
            """
            INSERT INTO items (
                type, title, status, parent_id, parent_id_int, created_at
            )
            VALUES ('task', ?, ?, ?, ?, ?)
            """,
            (title, status, int(parent_id), int(parent_id), datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        return _require_lastrowid(cur)
def _process_queue_item(row: dict) -> None:
    row = as_dict(row)
    queue_id = row["id"]
    chat_id = int(row.get("tg_chat_id") or 0)
    message_id = row.get("tg_message_id")
    attempts = int(row.get("attempts") or 0)
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
                _to_int_or_none(row.get("tg_update_id")),
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
            _tg_notify_created(int(item_id))
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
                int(tg_update_id) if tg_update_id is not None else None,
                str(file_id) if file_id else None,
                str(voice_unique_id) if voice_unique_id else None,
                int(voice_duration) if voice_duration is not None else None,
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

        if existing_asr_text:
            text = existing_asr_text
        else:
            audio = _tg_download_voice(file_id)
            text = _asr_transcribe(audio)
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
            _tg_notify_created(int(item_id))
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
                    _tg_notify_created(int(item_id))
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

def _get_calendar_service():
    global _CAL_NOT_CONFIGURED_REASON
    _CAL_NOT_CONFIGURED_REASON = None
    if not GOOGLE_SERVICE_ACCOUNT_FILE:
        logging.warning("calendar_not_configured: missing_file")
        _CAL_NOT_CONFIGURED_REASON = "missing_file"
        return None
    if os.path.isdir(GOOGLE_SERVICE_ACCOUNT_FILE):
        logging.warning("calendar_not_configured: file_is_directory")
        _CAL_NOT_CONFIGURED_REASON = "file_is_directory"
        return None
    if not os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        logging.warning("calendar_not_configured: missing_file")
        _CAL_NOT_CONFIGURED_REASON = "missing_file"
        return None
    if not GOOGLE_CALENDAR_ID:
        logging.warning("calendar_not_configured: missing_calendar_id")
        _CAL_NOT_CONFIGURED_REASON = "missing_calendar_id"
        return None
    try:
        creds_mod = importlib.import_module("google.oauth2.service_account")
        discovery_mod = importlib.import_module("googleapiclient.discovery")
        Credentials = getattr(creds_mod, "Credentials", None)
        build = getattr(discovery_mod, "build", None)
        if Credentials is None or build is None:
            logging.warning("google api libs not available; skipping")
            _CAL_NOT_CONFIGURED_REASON = "missing_libs"
            return None
    except Exception as exc:
        logging.warning("google api libs not available; skipping (%s)", str(exc)[:200])
        _CAL_NOT_CONFIGURED_REASON = "missing_libs"
        return None

    creds = Credentials.from_service_account_file(
        GOOGLE_SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/calendar"],
    )
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)
    if CALENDAR_DEBUG:
        try:
            data = service.calendarList().list().execute()
            for cal in (data.get("items") or []):
                logging.info(
                    "calendar_list id=%s summary=%s",
                    cal.get("id"),
                    cal.get("summary"),
                )
        except Exception as exc:
            logging.warning("calendar_list failed err=%s", str(exc)[:200])
    return service


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


def _create_event(title: str, start: datetime, end: datetime, description: str | None = None) -> str | None:
    service = _get_calendar_service()
    if service is None:
        logging.warning("calendar service not configured; skipping")
        return None

    event = {
        "summary": title,
        "start": {"dateTime": start.isoformat(), "timeZone": TIMEZONE_NAME},
        "end": {"dateTime": end.isoformat(), "timeZone": TIMEZONE_NAME},
    }
    if description:
        event["description"] = description
    created = service.events().insert(calendarId=GOOGLE_CALENDAR_ID, body=event).execute()
    return created.get("id")


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


def _patch_event(event_id: str, start: datetime, end: datetime) -> str:
    service = _get_calendar_service()
    if service is None:
        return "no_service"
    body = {
        "start": {"dateTime": start.isoformat(), "timeZone": TIMEZONE_NAME},
        "end": {"dateTime": end.isoformat(), "timeZone": TIMEZONE_NAME},
    }
    try:
        service.events().patch(calendarId=GOOGLE_CALENDAR_ID, eventId=event_id, body=body).execute()
        return "ok"
    except Exception as exc:
        try:
            err_mod = importlib.import_module("googleapiclient.errors")
            HttpError = getattr(err_mod, "HttpError", None)
        except Exception:
            HttpError = None
        if HttpError is not None and isinstance(exc, HttpError):
            status = getattr(getattr(exc, "resp", None), "status", None)
            if status == 404:
                return "not_found"
            return "error"
        return "error"


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


def _calendar_create_event(title: str, start: datetime, end: datetime, description: str | None = None) -> dict:
    # TODO(P4): store calendar_etag when schema allows.
    try:
        event_id = _create_event(title, start, end, description=description)
    except Exception as exc:
        status = _calendar_http_status(exc)
        return _calendar_result(False, None, status, f"exception:{type(exc).__name__}", None)
    if not event_id:
        return _calendar_result(False, None, None, "no_event_id", None)
    return _calendar_result(True, str(event_id), None, None, None)


def _calendar_patch_event(event_id: str, start: datetime, end: datetime) -> dict:
    # TODO(P4): store calendar_etag when schema allows.
    try:
        res = _patch_event(event_id, start, end)
    except Exception as exc:
        status = _calendar_http_status(exc)
        return _calendar_result(False, None, status, f"exception:{type(exc).__name__}", None)
    if res == "ok":
        return _calendar_result(True, event_id, None, None, None)
    if res == "not_found":
        return _calendar_result(False, None, 404, "not_found", None)
    if res == "no_service":
        return _calendar_result(False, None, None, "no_service", None)
    return _calendar_result(False, None, None, "error", None)


def _calendar_cancel_event(event_id: str) -> dict:
    service = _get_calendar_service()
    if service is None:
        return _calendar_result(False, None, None, "no_service", None)
    try:
        service.events().delete(calendarId=GOOGLE_CALENDAR_ID, eventId=event_id).execute()
        return _calendar_result(True, None, 204, None, None)
    except Exception as exc:
        status = _calendar_http_status(exc)
        if status == 404:
            return _calendar_result(True, None, 404, "not_found", None)
        return _calendar_result(False, None, status, f"exception:{type(exc).__name__}", None)


def _calendar_get_event(event_id: str) -> dict:
    service = _get_calendar_service()
    if service is None:
        return _calendar_result(False, None, None, "no_service", None)
    try:
        event = service.events().get(calendarId=GOOGLE_CALENDAR_ID, eventId=event_id).execute()
        start = (event or {}).get("start") or {}
        start_dt = start.get("dateTime") or start.get("date")
        return {
            "ok": True,
            "event_id": event_id,
            "http_status": None,
            "err": None,
            "event_start": start_dt,
        }
    except Exception as exc:
        status = _calendar_http_status(exc)
        if status == 404:
            return {"ok": False, "event_id": None, "http_status": 404, "err": "not_found", "event_start": None}
        return {
            "ok": False,
            "event_id": None,
            "http_status": status,
            "err": f"exception:{type(exc).__name__}",
            "event_start": None,
        }


# === P3: Sync Ticks ==========================================================
# TODO(P4): infinite-loop protection concept only (no implementation in P3).
# Suggestion: last_sync_at, sync_counter, or max N state flips per task per hour.
def _p3_calendar_create_tick(limit: int = 10) -> None:
    # P3: for current logic (create_tick).
    try:
        with _get_conn() as conn:
            rows = conn.execute(
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
        logging.warning("%s err=%s", P3_CALENDAR_CREATE, str(exc)[:200])
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
            res = _calendar_create_event(f"Task #{task_id}: {title}", dt, end)
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
            logging.warning(
                "%s action=create_attempt task_id=%s planned_at=%s calendar_event_id=%s err=%s",
                P3_CALENDAR_CREATE,
                task_id,
                planned_at,
                "",
                str(exc)[:200],
            )
            continue


def _p3_calendar_update_tick(limit: int = 3) -> None:
    # P3: for current logic (update_tick).
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    try:
        with _get_conn() as conn:
            rows = conn.execute(
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
        logging.warning("%s err=%s", P3_CALENDAR_UPDATE, str(exc)[:200])
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
            logging.warning(
                "%s action=patch_attempt task_id=%s planned_at=%s calendar_event_id=%s err=%s",
                P3_CALENDAR_UPDATE,
                row["id"],
                "",
                "",
                str(exc)[:200],
            )
            continue


def _p4_calendar_cancel_tick(limit: int = 3) -> None:
    # P4: for future scaffold (cancel_tick). Do not call yet.
    try:
        with _get_conn() as conn:
            rows = conn.execute(
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
        logging.warning("%s err=%s", P4_CALENDAR_CANCEL, str(exc)[:200])
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
            logging.warning(
                "%s action=cancel_failed task_id=%s calendar_event_id=%s err=%s",
                P4_CALENDAR_CANCEL,
                row["id"],
                row.get("calendar_event_id"),
                str(exc)[:200],
            )
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

    # Idempotency guard
    if cal_id and cal_id not in ("PENDING", "FAILED"):
        logging.info("[%s] calendar_state after=%s (skip)", item_id, cal_id)
        return

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
        event_id = _create_event(event_title, start, end, description=event_description)  # must return str|None
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
            event_id = _create_event(event_title, start, end, description=event_description)
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
            event_id = _create_event(event_title, start, end, description=event_description)
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


def main() -> None:
    _init_db()
    assert hasattr(sqlite3.Row, "__getitem__") and not hasattr(sqlite3.Row, "get")
    os.makedirs("/tmp", exist_ok=True)
    with open("/tmp/worker.ok", "w", encoding="utf-8") as marker:
        marker.write("ok\n")
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    logging.info("organizer-worker started")
    logging.info(
        "P3_CALENDAR_MODE mode=%s raw=%s",
        CALENDAR_SYNC_MODE,
        _CALENDAR_SYNC_MODE_RAW,
    )
    _start_command_server()
    if ASR_DT_SELF_CHECK:
        _selfcheck_asr_datetime()

    last_heartbeat = 0.0
    last_requeue = 0.0
    last_reg_nudge = 0.0
    last_p5_tick = 0.0
    while True:
        try:
            _queue_reaper()
            now = time.time()
            if B2_REQUEUE_FAILED_EVERY_SEC > 0 and (now - last_requeue) >= B2_REQUEUE_FAILED_EVERY_SEC:
                moved = _queue_requeue_failed(limit=B2_REQUEUE_FAILED_BATCH)
                if moved:
                    logging.info("requeued FAILED->NEW: %s", moved)
                last_requeue = now
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
    main()
