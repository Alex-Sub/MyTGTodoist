import json
import logging
import os
import re
import sqlite3
import time
import socket
from pathlib import Path
from datetime import datetime, timedelta, timezone, date

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import requests


DB_PATH = os.getenv("DB_PATH", "/data/organizer.db")
TIMEZONE_NAME = os.getenv("TIMEZONE", "Europe/Moscow")
DEFAULT_DURATION_MIN = int(os.getenv("DEFAULT_DURATION_MIN", "30"))
WORKER_INTERVAL_SEC = int(os.getenv("WORKER_INTERVAL_SEC", "5"))
WORKER_HEARTBEAT_SEC = int(os.getenv("WORKER_HEARTBEAT_SEC", "7"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "5"))
GOOGLE_CALENDAR_ID = os.getenv("GOOGLE_CALENDAR_ID", "")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ASR_SERVICE_URL = os.getenv("ASR_SERVICE_URL", "http://asr-service:8001")
TG_HTTP_READ_TIMEOUT = int(os.getenv("TG_HTTP_READ_TIMEOUT", "90"))
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


_RU_MONTHS = {
    "январ": 1, "феврал": 2, "март": 3, "апрел": 4, "ма": 5, "июн": 6, "июл": 7,
    "август": 8, "сентябр": 9, "октябр": 10, "ноябр": 11, "декабр": 12,
}
_RU_WEEKDAYS = {
    "пон": 0, "пн": 0,
    "втор": 1, "вт": 1,
    "сред": 2, "ср": 2,
    "чет": 3, "чт": 3,
    "пят": 4, "пт": 4,
    "суб": 5, "сб": 5,
    "воск": 6, "вс": 6,
}


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
    if hh > DT_SHORT_HOUR_MAX:
        return False
    if re.search(r"\b(утра|вечера|дня|ночью)\b", t):
        return False
    if re.search(r"\bчас(ов|а)?\b", t):
        return False
    return True


def _parse_weekday_ru(t: str) -> int | None:
    s = (t or "").lower()
    for k, v in _RU_WEEKDAYS.items():
        if k in s:
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


def _extract_datetime(text: str) -> datetime | None:
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
    now_local = datetime.now(tz)

    period_like = any(x in t for x in (
        "на этой неделе", "на прошлой неделе", "на следующей неделе",
        "в этом месяце", "в прошлом месяце", "в следующем месяце",
        "в этом году", "в прошлом году", "в следующем году", "через ",
    ))
    tm = _parse_time_ru(t)
    if tm:
        hh, mm = tm
        time_ambiguous = _is_time_ambiguous(t)
        if time_ambiguous:
            hh, mm = DT_AMBIGUOUS_MARKER_HOUR, DT_AMBIGUOUS_MARKER_MINUTE
            logging.info("time_ambiguous=True text=%r", text[:200])
    else:
        hh, mm = (MARKER_HOUR, MARKER_MINUTE) if period_like else (DEFAULT_HOUR, DEFAULT_MINUTE)

    rel_date = _resolve_relative_period(t, now_local)
    if rel_date:
        return datetime(rel_date.year, rel_date.month, rel_date.day, hh, mm, tzinfo=tz)

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
                created_at DATETIME,
                calendar_event_id TEXT NULL,
                attempts INTEGER DEFAULT 0,
                last_error TEXT NULL,
                updated_at DATETIME NULL
            )
            """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(items)").fetchall()}
        if "calendar_event_id" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN calendar_event_id TEXT NULL")
        if "attempts" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
        if "last_error" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN last_error TEXT NULL")
        if "updated_at" not in columns:
            conn.execute("ALTER TABLE items ADD COLUMN updated_at DATETIME NULL")
        conn.commit()
        sql = Path(SCHEMA_PATH).read_text(encoding="utf-8")
        conn.executescript(sql)
        conn.commit()


def _queue_reaper() -> None:
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE inbox_queue
            SET status='NEW',
                claimed_by=NULL,
                claimed_at=NULL,
                lease_until=NULL,
                updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE status='CLAIMED'
              AND lease_until IS NOT NULL
              AND lease_until < strftime('%Y-%m-%dT%H:%M:%fZ','now')
              AND attempts < ?
            """,
            (B2_MAX_ATTEMPTS,),
        )
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


def _tg_send_message(chat_id: int, text: str) -> None:
    """
    Send message to Telegram user from worker. Best-effort with retries.
    """
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    last_exc: Exception | None = None
    for _ in range(max(1, WORKER_TG_SEND_MAX_RETRIES)):
        try:
            resp = requests.post(
                url,
                json={"chat_id": chat_id, "text": text},
                timeout=(3, WORKER_TG_HTTP_READ_TIMEOUT),
            )
            resp.raise_for_status()
            return
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            time.sleep(0.3)
        except Exception as exc:
            # do not crash worker for notification failures
            last_exc = exc
            break
    if last_exc:
        logging.warning("tg notify failed chat_id=%s err=%s", chat_id, str(last_exc)[:200])


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
        start_at = row[0]
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
    # Only treat as schedule command when intent marker is present
    if not re.search(r"\b(встреча|для|номер|#)\b", t):
        return None
    # Accept: "встреча 21 в 16", "#21 16:00", "для 21 завтра в 9:30"
    m = re.search(
        r"(?:\bдля\b\s+|\bвстреча\b\s+|\bномер\b\s+)?#?(?P<id>\d{1,6})\b"
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
    if not re.search(r"(#|\bвстреча\b|\bномер\b|\bдля\b)", t):
        return False
    return re.search(r"\b\d{1,6}\b", t) is not None


def _insert_item_from_text(text: str, source: str) -> tuple[int, str]:
    dt = _extract_datetime(text)   # важно: должна вернуть datetime или None
    tnorm = (text or "").lower()
    is_meeting_hint = any(w in tnorm for w in ("встреч", "созвон", "звонок", "колл", "колл-"))
    item_type = "meeting" if (dt or is_meeting_hint) else "task"

    time_ambiguous = _is_time_ambiguous(text or "")
    status = "active" if (dt and not time_ambiguous) else "inbox"
    start_at = dt.isoformat() if dt else None
    end_at = (dt + timedelta(minutes=MEETING_DEFAULT_MINUTES)).isoformat() if dt else None

    created_at = datetime.now(timezone.utc).isoformat()
    with _get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO items (type, title, status, start_at, end_at, source, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (item_type, text.strip(), status, start_at, end_at, source, created_at),
        )
        conn.commit()
        return int(cur.lastrowid), item_type


def _process_queue_item(row: dict) -> None:
    queue_id = row["id"]
    chat_id = int(row.get("tg_chat_id") or 0)
    attempts = int(row.get("attempts") or 0)
    try:
        payload = json.loads(row.get("payload_json") or "{}")
        kind = row.get("kind")
        if kind == "text":
            text = (payload.get("text") or "").strip()
            if not text or len(text) < 2:
                raise RuntimeError("empty text")
            time_ambiguous = _is_time_ambiguous(text)
            item_id, item_type = _insert_item_from_text(text, "telegram")
            _queue_mark(queue_id, "DONE", None)
            logging.info("queue done id=%s kind=text attempts=%s", queue_id, attempts)
            if chat_id:
                if time_ambiguous and item_type == "meeting":
                    reply = (
                        f"Создано: #{item_id} (inbox). Время не уточнено (утро/вечер). "
                        "Напиши: \"в 9 утра\" или \"в 9 вечера\"."
                    )
                else:
                    status = "active" if item_type == "meeting" else "inbox"
                    reply = f"Создано: #{item_id} ({status})."
                    if item_type == "meeting":
                        reply += "\nПопробую поставить в календарь."
                _tg_send_message(chat_id, reply)
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
                return
            except Exception as exc:
                err = str(exc)[:500]
                status = "DEAD" if attempts >= B2_MAX_ATTEMPTS else "FAILED"
                _queue_mark(queue_id, status, err)
                logging.warning("queue failed id=%s status=%s err=%s", queue_id, status, err)
                if chat_id:
                    _tg_send_message(chat_id, "Не понял формат, скажи: #21 16:00")
                return
        if _looks_like_schedule_intent(text):
            logging.info("schedule intent but parse failed: %r", text[:200])
            _queue_mark(queue_id, "DONE", None)
            if chat_id:
                _tg_send_message(
                    chat_id,
                    'Не понял уточнение. Скажи так: "#21 16:00" или "/set #21 16:00".',
                )
            return
        dt = _extract_datetime(text)
        logging.info("asr text=%r dt=%r", text[:200], dt)
        time_ambiguous = _is_time_ambiguous(text)
        item_id, item_type = _insert_item_from_text(text, "telegram")
        _queue_mark(queue_id, "DONE", None)
        logging.info("queue done id=%s kind=voice attempts=%s", queue_id, attempts)
        if chat_id:
            # NOTE: report actual status/type to user
            # We read it back quickly to avoid mismatch
            try:
                with _get_conn() as conn:
                    r = conn.execute("select type,status,start_at from items where id=?", (item_id,)).fetchone()
                st = r[1] if r else "inbox"
                sa = r[2] if r else None
            except Exception:
                st, sa = "inbox", None
            if time_ambiguous and item_type == "meeting":
                reply = (
                    f"Создано: #{item_id} (inbox). Время не уточнено (утро/вечер). "
                    "Напиши: \"в 9 утра\" или \"в 9 вечера\"."
                )
            else:
                reply = f"Создано: #{item_id} ({st})."
                if item_type == "meeting":
                    reply += "\nПопробую поставить в календарь."
                if sa:
                    reply += f"\nВремя: {sa}"
                else:
                    reply += "\nВремя не распознано — останется в Inbox."
            _tg_send_message(chat_id, reply)
    except Exception as exc:
        err = str(exc)[:500]
        status = "DEAD" if attempts >= B2_MAX_ATTEMPTS else "FAILED"
        _queue_mark(queue_id, status, err)
        logging.warning("queue failed id=%s status=%s err=%s", queue_id, status, err)
        if status == "DEAD" and WORKER_NOTIFY_ON_DEAD and chat_id:
            _tg_send_message(
                chat_id,
                "Не получилось обработать сообщение после нескольких попыток. Отправь ещё раз позже.",
            )


def _get_calendar_service():
    if not GOOGLE_CALENDAR_ID or not GOOGLE_SERVICE_ACCOUNT_FILE:
        return None
    if not os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        return None
    creds = Credentials.from_service_account_file(
        GOOGLE_SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/calendar"],
    )
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


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
        item_id = row["id"]
        title = row["title"] or ""
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
        event_id = _create_event(title, start, end)
        if not event_id:
            logging.warning("event create failed for item %s", item_id)
            continue
        with _get_conn() as conn:
            conn.execute(
                """
                UPDATE items
                SET calendar_event_id = ?
                WHERE id = ? AND calendar_event_id = 'PENDING'
                """,
                (event_id, item_id),
            )
            conn.commit()
        logging.info("event created for item %s", item_id)


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
        item_id = row["id"]
        title = row["title"] or ""
        start_at = row["start_at"]
        end_at = row["end_at"]
        attempts = int(row["attempts"] or 0)
        logging.info("retry start item_id=%s attempts=%s", item_id, attempts)

        try:
            start = datetime.fromisoformat(start_at)
            end = datetime.fromisoformat(end_at)
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
            err_text = str(exc)
        else:
            err_text = ""

        if event_id:
            with _get_conn() as conn:
                conn.execute(
                    """
                    UPDATE items
                    SET calendar_event_id = ?,
                        last_error = NULL,
                        updated_at = ?
                    WHERE id = ? AND calendar_event_id = 'PENDING'
                    """,
                    (event_id, datetime.now(timezone.utc).isoformat(), item_id),
                )
                conn.commit()
            logging.info("retry success item_id=%s event_id=%s", item_id, event_id)
            continue

        logging.warning("retry failed item_id=%s err=%s", item_id, err_text or "calendar create failed")
        with _get_conn() as conn:
            conn.execute(
                """
                UPDATE items
                SET last_error = ?,
                    updated_at = ?
                WHERE id = ? AND calendar_event_id = 'PENDING'
                """,
                (err_text or "calendar create failed", datetime.now(timezone.utc).isoformat(), item_id),
            )
            conn.commit()

        with _get_conn() as conn:
            row2 = conn.execute(
                "SELECT attempts FROM items WHERE id = ?", (item_id,)
            ).fetchone()
            if not row2:
                continue
            if row2["attempts"] >= MAX_ATTEMPTS:
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
    os.makedirs("/tmp", exist_ok=True)
    with open("/tmp/worker.ok", "w", encoding="utf-8") as marker:
        marker.write("ok\n")
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    logging.info("organizer-worker started")

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
