import importlib
import json
import logging
import os
import re
import sqlite3
import time
import socket
from pathlib import Path
from datetime import datetime, timedelta, timezone, date
import urllib.request

import requests


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

WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"

def _local_tz() -> timezone:
    return timezone(timedelta(minutes=LOCAL_TZ_OFFSET_MIN))

def as_dict(row: sqlite3.Row | dict | None) -> dict:
    if row is None:
        return {}
    return row if isinstance(row, dict) else dict(row)


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
                start_at DATETIME NULL,
                end_at DATETIME NULL,
                source TEXT,
                tg_chat_id INTEGER NULL,
                tg_message_id INTEGER NULL,
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
        if "tg_chat_id" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN tg_chat_id INTEGER NULL")
        if "tg_message_id" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN tg_message_id INTEGER NULL")
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
        sql = Path(SCHEMA_PATH).read_text(encoding="utf-8")
        conn.executescript(sql)
        conn.commit()
        columns_q = {as_dict(row).get("name") for row in conn.execute("PRAGMA table_info(inbox_queue)").fetchall()}
        if "ingested_at" not in columns_q:
            conn.execute("ALTER TABLE inbox_queue ADD COLUMN ingested_at TEXT")
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
) -> tuple[int, str, str]:
    dt = _extract_datetime(text)   # важно: должна вернуть datetime или None
    item_type = "meeting" if (dt is not None or MEETING_HINT_RE.search(text or "")) else "task"

    time_ambiguous = _is_time_ambiguous(text or "")
    has_time = _parse_time_ru(text or "") is not None
    status = "active" if (dt and has_time and not time_ambiguous) else "inbox"
    start_at = dt.isoformat() if dt else None
    end_at = (dt + timedelta(minutes=MEETING_DEFAULT_MINUTES)).isoformat() if dt else None

    created_at = datetime.now(timezone.utc).isoformat()
    ingested_at = ingested_at or created_at
    with _get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO items (
                type, title, status, start_at, end_at, source,
                tg_chat_id, tg_message_id,
                tg_accepted_sent, tg_result_sent,
                created_at, ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
            """,
            (
                item_type,
                text.strip(),
                status,
                start_at,
                end_at,
                source,
                tg_chat_id,
                tg_message_id,
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
            )
            logging.info("tg meta kind=%s item_id=%s tg_chat_id=%s", kind, item_id, chat_id)
            try:
                with _get_conn() as conn:
                    row_item = conn.execute(
                        "SELECT id, title, type, start_at, end_at, calendar_event_id FROM items WHERE id=?",
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
        file_id = payload.get("file_id")
        if not file_id:
            raise RuntimeError("missing file_id")
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
        dt = _extract_datetime(text)
        logging.info("asr text=%r dt=%r", text[:200], dt)
        time_ambiguous = _is_time_ambiguous(text)
        ingested_at = row.get("ingested_at") or row.get("created_at")
        item_id, item_type, item_status = _insert_item_from_text(
            text,
            "telegram",
            ingested_at,
            int(chat_id) if chat_id else None,
            int(message_id) if message_id is not None else None,
        )
        logging.info("tg meta kind=%s item_id=%s tg_chat_id=%s", kind, item_id, chat_id)
        try:
            with _get_conn() as conn:
                row_item = conn.execute(
                    "SELECT id, title, type, start_at, end_at, calendar_event_id FROM items WHERE id=?",
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


def _create_event(title: str, start: datetime, end: datetime) -> str | None:
    service = _get_calendar_service()
    if service is None:
        logging.warning("calendar service not configured; skipping")
        return None

    event = {
        "summary": title,
        "start": {"dateTime": start.isoformat(), "timeZone": TIMEZONE_NAME},
        "end": {"dateTime": end.isoformat(), "timeZone": TIMEZONE_NAME},
    }
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

def _sync_calendar_for_item(item: dict) -> None:
    # accept sqlite3.Row too
    if not isinstance(item, dict):
        item = dict(item)
    item_id = item["id"]
    title = (item.get("title") or "").strip()
    cal_id = item.get("calendar_event_id")  # None | 'PENDING' | 'FAILED' | '<id>'
    attempts = int(item.get("attempts") or 0)

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
        event_id = _create_event(title, start, end)  # must return str|None
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
            SELECT id, title
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
        title = row.get("title") or ""
        start = _extract_datetime(title)
        if not start or _is_time_ambiguous(title):
            continue
        end = start + timedelta(minutes=MEETING_DEFAULT_MINUTES)
        reserved = False
        with _get_conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
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
            event_id = _create_event(title, start, end)
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
            SELECT id, title, start_at, end_at, attempts
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
            event_id = _create_event(title, start, end)
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
    if ASR_DT_SELF_CHECK:
        _selfcheck_asr_datetime()

    last_heartbeat = 0.0
    last_requeue = 0.0
    while True:
        try:
            _queue_reaper()
            now = time.time()
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
