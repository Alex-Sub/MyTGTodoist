import json
import os
import sqlite3
import threading
import time
import re
import urllib.parse
import urllib.request
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timedelta, timezone, date, tzinfo
from typing import Any
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - fallback for minimal runtime
    ZoneInfo = None

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover - fallback for minimal runtime
    requests = None


def _require_requests() -> Any:
    if requests is None:
        raise RuntimeError("requests is required")
    return requests


def _build_retry_exceptions() -> tuple[type[Exception], ...]:
    ex_types: list[type[Exception]] = [TimeoutError, ConnectionError]
    if requests is not None:
        for name in ("Timeout", "ConnectionError"):
            exc_t = getattr(requests, name, None)
            if isinstance(exc_t, type) and issubclass(exc_t, Exception):
                ex_types.append(exc_t)
    return tuple(ex_types)


_RETRY_EXCEPTIONS = _build_retry_exceptions()


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ORGANIZER_API_URL = os.getenv("ORGANIZER_API_URL", "http://organizer-api:8000")
WORKER_COMMAND_URL = os.getenv("WORKER_COMMAND_URL", "http://organizer-worker:8002")
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8082"))
STATE_PATH = os.getenv("STATE_PATH", "/data/bot.offset")
TG_DRAIN_ON_START = os.getenv("TG_DRAIN_ON_START", "0") == "1"
TG_LONGPOLL_SEC = int(os.getenv("TG_LONGPOLL_SEC", "25"))
TG_HTTP_TIMEOUT = (3, int(os.getenv("TG_HTTP_READ_TIMEOUT", "90")))
DB_PATH = os.getenv("DB_PATH", "/data/organizer.db")
B2_QUEUE_MAX_NEW = int(os.getenv("B2_QUEUE_MAX_NEW", "50"))
B2_QUEUE_MAX_TOTAL = int(os.getenv("B2_QUEUE_MAX_TOTAL", "500"))
B2_BACKPRESSURE_MODE = os.getenv("B2_BACKPRESSURE_MODE", "reject")
SCHEMA_PATH = os.getenv("B2_SCHEMA_PATH", "/app/migrations/001_inbox_queue.sql")
LOCAL_TZ_OFFSET_MIN = int(os.getenv("LOCAL_TZ_OFFSET_MIN", "180"))  # +03:00
TIMEZONE = os.getenv("TIMEZONE", "Europe/Moscow")
DEFAULT_MEETING_MINUTES = int(os.getenv("MEETING_DEFAULT_MINUTES", "30"))
TG_SEND_MAX_RETRIES = int(os.getenv("TG_SEND_MAX_RETRIES", "2"))
CLARIFY_STATE_PATH = os.getenv("CLARIFY_STATE_PATH", "/data/bot.clarify.json")
CLARIFY_TTL_SEC = int(os.getenv("CLARIFY_TTL_SEC", "180"))
NUDGE_SIGNALS_KEY = "signals_enable_prompt"
_P2_PENDING_PATH_ENV = os.getenv("P2_PENDING_PATH")
if _P2_PENDING_PATH_ENV:
    P2_PENDING_PATH = _P2_PENDING_PATH_ENV
else:
    _pending_dir = "/data" if os.path.isdir("/data") else "./data"
    P2_PENDING_PATH = os.path.join(_pending_dir, "bot.p2_pending.json")
P2_PENDING_TTL_SEC = int(os.getenv("P2_PENDING_TTL_SEC", "300"))
DRIFT_MODE = (os.getenv("DRIFT_MODE", "off") or "off").strip().lower()
OVERLOAD_MODE = (os.getenv("OVERLOAD_MODE", "off") or "off").strip().lower()
P5_NUDGES_MODE = (os.getenv("P5_NUDGES_MODE", "off") or "off").strip().lower()
P7_MODE = (os.getenv("P7_MODE", "off") or "off").strip().lower()
DAILY_DIGEST_ENABLED = os.getenv("DAILY_DIGEST_ENABLED", "1") == "1"
DAILY_DIGEST_TIME = os.getenv("DAILY_DIGEST_TIME", "09:00")
DAILY_DIGEST_TIMEZONE = os.getenv("DAILY_DIGEST_TIMEZONE", "Europe/Berlin")
_digest_chats_raw = os.getenv("DAILY_DIGEST_CHAT_IDS", "").strip()
DAILY_DIGEST_CHAT_IDS = [int(x.strip()) for x in _digest_chats_raw.split(",") if x.strip().isdigit()]

_p2_pending_state: dict[int, dict] = {}
_daily_digest_sent_day: dict[int, str] = {}

def _p7_enabled() -> bool:
    return P7_MODE == "on"

def _worker_post(path: str, payload: dict) -> dict | None:
    try:
        if requests is not None:
            resp = requests.post(
                f"{WORKER_COMMAND_URL}{path}",
                json=payload,
                timeout=TG_HTTP_TIMEOUT,
            )
            if resp.status_code >= 300:
                print(f"p2_cmd_http_error status={resp.status_code} path={path} body={resp.text[:200]}")
                return None
            data = _safe_json(resp)
            return data if isinstance(data, dict) else None
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{WORKER_COMMAND_URL}{path}",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=_timeout_seconds()) as resp:
            body = resp.read()
        data = json.loads(body.decode("utf-8"))
        return data if isinstance(data, dict) else None
    except Exception as exc:
        print(f"p2_cmd_http_error path={path} err={str(exc)[:200]}")
        return None


def _worker_post_ex(path: str, payload: dict) -> tuple[bool, dict | None, int | None, str | None]:
    try:
        if requests is not None:
            resp = requests.post(
                f"{WORKER_COMMAND_URL}{path}",
                json=payload,
                timeout=TG_HTTP_TIMEOUT,
            )
            status = int(resp.status_code)
            if status >= 300:
                return False, None, status, (resp.text or "")[:500]
            data = _safe_json(resp)
            return True, data if isinstance(data, dict) else None, status, None
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{WORKER_COMMAND_URL}{path}",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=_timeout_seconds()) as resp:
            status = int(getattr(resp, "status", 200))
            body = resp.read()
        if status >= 300:
            return False, None, status, body.decode("utf-8")[:500]
        parsed = json.loads(body.decode("utf-8"))
        return True, parsed if isinstance(parsed, dict) else None, status, None
    except Exception as exc:
        return False, None, None, str(exc)[:500]


def _worker_runtime_command(intent: str, entities: dict) -> dict | None:
    envelope = {
        "trace_id": f"tg:{int(time.time() * 1000)}:{intent}",
        "source": "telegram-bot",
        "command": {
            "intent": intent,
            "entities": entities,
        },
    }
    return _worker_post("/runtime/command", envelope)


def _digest_tz() -> tzinfo:
    if ZoneInfo is not None:
        try:
            return ZoneInfo(DAILY_DIGEST_TIMEZONE)
        except Exception:
            pass
    return _tz_local()


def _digest_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "⚠️ Просроченные", "callback_data": "digest:overdue"},
                {"text": "⏳ Истекают", "callback_data": "digest:due_soon"},
            ],
            [
                {"text": "🟠 Под риском", "callback_data": "digest:at_risk"},
                {"text": "📌 Сегодня", "callback_data": "digest:today"},
            ],
            [
                {"text": "📌 Завтра", "callback_data": "digest:tomorrow"},
                {"text": "📋 Все активные задачи", "callback_data": "digest:active"},
            ],
        ]
    }


def _format_daily_digest_text(digest: dict, now_local: datetime) -> str:
    day_label = now_local.strftime("%d %b")
    return (
        f"Сегодня, {day_label}\n"
        f"🎯 Активных целей: {int(digest.get('goals_active', 0))}\n"
        f"⚠️ Просрочено: {int(digest.get('goals_overdue', 0))}\n"
        f"⏳ Истекает сегодня/завтра: {int(digest.get('goals_due_soon', 0))}\n"
        f"🟠 Под риском: {int(digest.get('goals_at_risk', 0))}\n"
        f"📌 Задачи на сегодня: {int(digest.get('tasks_today', 0))}\n"
        f"📌 Задачи на завтра: {int(digest.get('tasks_tomorrow', 0))}"
    )


def _send_daily_digest(chat_id: int) -> None:
    now_local = datetime.now(_digest_tz())
    today = now_local.date().isoformat()
    tomorrow = (now_local.date() + timedelta(days=1)).isoformat()
    payload = _worker_runtime_command("digest.daily", {"today": today, "tomorrow": tomorrow, "user_id": str(chat_id)})
    digest = (payload or {}).get("debug", {}).get("digest") if isinstance(payload, dict) else None
    if not isinstance(digest, dict):
        return
    text = _format_daily_digest_text(digest, now_local)
    _send_message(chat_id, text, reply_markup=_digest_keyboard())


def _handle_digest_callback(data: str, chat_id: int | None) -> str:
    if chat_id is None:
        return "Ошибка"
    parts = data.split(":")
    mode = parts[1] if len(parts) > 1 else ""
    today = datetime.now(_digest_tz()).date().isoformat()
    tomorrow = (datetime.now(_digest_tz()).date() + timedelta(days=1)).isoformat()

    if mode == "goal_reschedule" and len(parts) >= 4:
        goal_id = int(parts[2])
        current_due = str(parts[3])
        try:
            base = date.fromisoformat(current_due)
            new_due = (base + timedelta(days=7)).isoformat()
        except Exception:
            new_due = (datetime.now(_digest_tz()).date() + timedelta(days=7)).isoformat()
        payload = _worker_runtime_command("goal.reschedule", {"goal_id": goal_id, "new_end_date": new_due}) or {}
        ok = bool(payload.get("ok")) if isinstance(payload, dict) else False
        if ok:
            _send_message(chat_id, f"Цель #{goal_id}: срок перенесён на {new_due}.")
            return "Готово"
        _send_message(chat_id, f"Не удалось перенести цель #{goal_id}.")
        return "Ошибка"

    if mode == "goal_close_done" and len(parts) >= 3:
        goal_id = int(parts[2])
        payload = _worker_runtime_command("goal.close", {"goal_id": goal_id, "close_as": "DONE"}) or {}
        ok = bool(payload.get("ok")) if isinstance(payload, dict) else False
        _send_message(chat_id, f"Цель #{goal_id} закрыта как DONE." if ok else f"Не удалось закрыть цель #{goal_id}.")
        return "Готово" if ok else "Ошибка"

    if mode == "goal_close_drop" and len(parts) >= 3:
        goal_id = int(parts[2])
        payload = _worker_runtime_command("goal.close", {"goal_id": goal_id, "close_as": "DROPPED"}) or {}
        ok = bool(payload.get("ok")) if isinstance(payload, dict) else False
        _send_message(chat_id, f"Цель #{goal_id} снята (DROPPED)." if ok else f"Не удалось снять цель #{goal_id}.")
        return "Готово" if ok else "Ошибка"

    if mode in {"overdue", "due_soon", "at_risk"}:
        intent_map = {
            "overdue": "goals.list_overdue",
            "due_soon": "goals.list_due_soon",
            "at_risk": "goals.list_at_risk",
        }
        payload = _worker_runtime_command(
            intent_map[mode],
            {"user_id": str(chat_id), "today": today, "tomorrow": tomorrow, "limit": 20},
        ) or {}
        goals = payload.get("debug", {}).get("goals") if isinstance(payload, dict) else []
        if not isinstance(goals, list):
            goals = []
        titles = {"overdue": "⚠️ Просроченные", "due_soon": "⏳ Истекают", "at_risk": "🟠 Под риском"}
        title = titles.get(mode, "Список")
        if not goals:
            _send_message(chat_id, f"{title}: пусто")
            return "Ок"
        _send_message(chat_id, f"{title}: {len(goals)}")
        for g in goals[:10]:
            goal_id = int(g.get("id"))
            due = str(g.get("planned_end_date") or "")
            rs = int(g.get("reschedule_count") or 0)
            text = f"🎯 #{goal_id} {str(g.get('title') or '').strip()}\nСрок: {due}\nПереносов: {rs}"
            _send_message(
                chat_id,
                text,
                reply_markup={
                    "inline_keyboard": [
                        [
                            {"text": "⏭ Перенести +7д", "callback_data": f"digest:goal_reschedule:{goal_id}:{due}"},
                            {"text": "✅ Закрыть", "callback_data": f"digest:goal_close_done:{goal_id}"},
                        ],
                        [
                            {"text": "🗑 Снять", "callback_data": f"digest:goal_close_drop:{goal_id}"},
                        ],
                    ]
                },
            )
        return "Ок"

    if mode in {"today", "tomorrow", "active"}:
        intent_map = {
            "today": "tasks.list_today",
            "tomorrow": "tasks.list_tomorrow",
            "active": "tasks.list_active",
        }
        payload = _worker_runtime_command(
            intent_map[mode],
            {"user_id": str(chat_id), "today": today, "tomorrow": tomorrow, "limit": 50},
        ) or {}
        items = payload.get("debug", {}).get("tasks") if isinstance(payload, dict) else []
        if not isinstance(items, list):
            items = []
        title = {"today": "📌 Сегодня", "tomorrow": "📌 Завтра", "active": "📋 Все активные задачи"}[mode]
        if not items:
            _send_message(chat_id, f"{title}: пусто")
            return "Ок"
        lines = [title]
        for t in items[:20]:
            planned = str(t.get("planned_at") or "")[:16]
            suffix = f" ({planned})" if planned else ""
            lines.append(f"- #{t.get('id')} {str(t.get('title') or '').strip()}{suffix}")
        _send_message(chat_id, "\n".join(lines))
        return "Ок"
    return "Некорректно"


def _daily_digest_loop() -> None:
    while True:
        try:
            if DAILY_DIGEST_ENABLED and DAILY_DIGEST_CHAT_IDS:
                now_local = datetime.now(_digest_tz())
                hh, mm = [int(x) for x in DAILY_DIGEST_TIME.split(":")]
                for chat_id in DAILY_DIGEST_CHAT_IDS:
                    sent_key = _daily_digest_sent_day.get(chat_id)
                    today_key = now_local.date().isoformat()
                    if sent_key == today_key:
                        continue
                    if now_local.hour > hh or (now_local.hour == hh and now_local.minute >= mm):
                        _send_daily_digest(chat_id)
                        _daily_digest_sent_day[chat_id] = today_key
        except Exception as exc:
            print(f"digest_loop_error: {str(exc)[:200]}")
        time.sleep(30)


def _api_get_ex(path: str, params: dict | None = None) -> tuple[bool, dict | list | None, int | None, str | None]:
    try:
        if requests is not None:
            resp = requests.get(f"{ORGANIZER_API_URL}{path}", params=params or {}, timeout=TG_HTTP_TIMEOUT)
            status = int(resp.status_code)
            if status >= 300:
                return False, None, status, (resp.text or "")[:500]
            data = _safe_json(resp)
            return True, data, status, None
        query = urllib.parse.urlencode(params or {})
        url = f"{ORGANIZER_API_URL}{path}"
        if query:
            url = f"{url}?{query}"
        with urllib.request.urlopen(url, timeout=_timeout_seconds()) as resp:
            status = int(getattr(resp, "status", 200))
            body = resp.read()
        if status >= 300:
            return False, None, status, body.decode("utf-8")[:500]
        data = json.loads(body.decode("utf-8"))
        return True, data, status, None
    except Exception as exc:
        return False, None, None, str(exc)[:500]


def _timeout_seconds() -> float:
    t = TG_HTTP_TIMEOUT
    if isinstance(t, (int, float)):
        return float(t)
    if isinstance(t, (tuple, list)) and len(t) >= 2:
        return float(t[1])
    if isinstance(t, (tuple, list)) and len(t) == 1:
        return float(t[0])
    return 10.0


def _normalize_ws(text: str) -> str:
    return " ".join((text or "").split())


def _normalize_due_time_local(text: str) -> str | None:
    s = (text or "").strip()
    if not s:
        return None
    if s in {"-", "нет", "skip"}:
        return None
    if not re.match(r"^\d{2}:\d{2}$", s):
        raise ValueError("invalid due_time_local")
    hh = int(s[:2])
    mm = int(s[3:])
    if hh > 23 or mm > 59:
        raise ValueError("invalid due_time_local")
    return s


_REGS_PAGE_SIZE = 10
_REGS_MONTHS_RU = [
    "Январь",
    "Февраль",
    "Март",
    "Апрель",
    "Май",
    "Июнь",
    "Июль",
    "Август",
    "Сентябрь",
    "Октябрь",
    "Ноябрь",
    "Декабрь",
]


def _regs_period_key(dt: datetime) -> str:
    return f"{dt.year:04d}-{dt.month:02d}"


def _regs_parse_period_key(period_key: str) -> tuple[int, int] | None:
    try:
        parts = (period_key or "").split("-")
        if len(parts) != 2:
            return None
        y = int(parts[0])
        m = int(parts[1])
        if m < 1 or m > 12:
            return None
        return y, m
    except Exception:
        return None


def _regs_shift_period(period_key: str, delta_months: int) -> str | None:
    parsed = _regs_parse_period_key(period_key)
    if not parsed:
        return None
    y, m = parsed
    m = m + int(delta_months)
    while m > 12:
        y += 1
        m -= 12
    while m < 1:
        y -= 1
        m += 12
    return f"{y:04d}-{m:02d}"


def _regs_month_label(period_key: str) -> str:
    parsed = _regs_parse_period_key(period_key)
    if not parsed:
        return period_key
    y, m = parsed
    name = _REGS_MONTHS_RU[m - 1]
    return f"{name} {y}"


def _regs_status_from_run(status: str | None) -> str:
    s = (status or "").strip().upper()
    if s == "DONE":
        return "DONE"
    if s in {"MISSED", "SKIPPED"}:
        return "MISSED"
    if s == "OPEN":
        return "DUE"
    return "DUE"


def _regs_status_icon(label: str) -> str:
    if label == "DONE":
        return "🟢"
    if label == "MISSED":
        return "⚠️"
    return "🔴"


def _regs_sort_key(item: dict) -> tuple[int, str]:
    order = {"DUE": 0, "MISSED": 1, "DONE": 2}
    label = item.get("status_label") or "DUE"
    return (order.get(label, 0), (item.get("title") or "").lower())


def _regs_source_msg_id(chat_id: int, message_id: int, action: str) -> str:
    return f"tg:{chat_id}:{int(message_id)}:regs:{action}"


def _edit_message_with_keyboard(chat_id: int, message_id: int, text: str, reply_markup: dict | None) -> None:
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    try:
        req_mod = _require_requests()
        resp = req_mod.post(_api_url("editMessageText"), json=payload, timeout=TG_HTTP_TIMEOUT)
        resp.raise_for_status()
    except Exception:
        return


def _today_source_msg_id(chat_id: int, message_id: int, action: str) -> str:
    return f"tg:{chat_id}:{int(message_id)}:today:{action}"


def _today_parse_planned_local(iso_text: str) -> tuple[str, str] | None:
    try:
        s = iso_text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local_dt = dt.astimezone(_tz_local())
        return local_dt.date().isoformat(), local_dt.strftime("%H:%M")
    except Exception:
        return None


def _parse_iso_utc(iso_text: str) -> datetime | None:
    if not iso_text:
        return None
    try:
        s = iso_text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _format_local_hhmm(dt_utc: datetime) -> str:
    return dt_utc.astimezone(_tz_local()).strftime("%H:%M")

def _round_up_minutes(dt_local: datetime, step: int) -> datetime:
    if step <= 0:
        return dt_local
    discard = timedelta(minutes=dt_local.minute % step, seconds=dt_local.second, microseconds=dt_local.microsecond)
    if discard == timedelta(0):
        return dt_local
    return dt_local + (timedelta(minutes=step) - discard)


def _today_time_menu_keyboard(task_id: int) -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "30m", "callback_data": f"today:time:add:{task_id}:30"},
                {"text": "60m", "callback_data": f"today:time:add:{task_id}:60"},
                {"text": "90m", "callback_data": f"today:time:add:{task_id}:90"},
            ],
            [
                {"text": "↩️ Назад", "callback_data": "today:refresh"},
            ],
        ]
    }

def _today_blocks_page_keyboard(page: int, total_pages: int) -> list[dict]:
    if total_pages <= 1:
        return []
    prev_page = page - 1
    next_page = page + 1
    buttons: list[dict] = []
    if prev_page >= 1:
        buttons.append({"text": "⬅️", "callback_data": f"today:block:page:p{prev_page}"})
    buttons.append({"text": f"Стр. {page}/{total_pages}", "callback_data": "today:refresh"})
    if next_page <= total_pages:
        buttons.append({"text": "➡️", "callback_data": f"today:block:page:p{next_page}"})
    return buttons

def _today_collect_blocks(day_str: str) -> list[dict]:
    if not _p7_enabled():
        return []
    ok, data, status, err_text = _api_get_ex("/p7/day", {"date": day_str})
    if not ok or not isinstance(data, dict):
        print(f"today_blocks_error status={status} err={err_text}")
        return []
    blocks = data.get("blocks")
    if not isinstance(blocks, list):
        return []
    title_cache: dict[int, str] = {}
    items: list[dict] = []
    for b in blocks:
        if not isinstance(b, dict):
            continue
        block_id = b.get("id")
        task_id = b.get("task_id")
        start_at = b.get("start_at")
        end_at = b.get("end_at")
        if block_id is None or task_id is None or not start_at or not end_at:
            continue
        start_utc = _parse_iso_utc(str(start_at))
        end_utc = _parse_iso_utc(str(end_at))
        if start_utc is None or end_utc is None:
            continue
        title = b.get("title")
        if not title:
            if int(task_id) not in title_cache:
                ok_t, data_t, _, _ = _api_get_ex(f"/p2/tasks/{int(task_id)}")
                if ok_t and isinstance(data_t, dict):
                    title_cache[int(task_id)] = _truncate(data_t.get("title") or "")
                else:
                    title_cache[int(task_id)] = f"#{int(task_id)}"
            title = title_cache[int(task_id)]
        items.append(
            {
                "id": int(block_id),
                "task_id": int(task_id),
                "title": _truncate(str(title)),
                "start_utc": start_utc,
                "end_utc": end_utc,
            }
        )
    items.sort(key=lambda r: (r["start_utc"], r["id"]))
    return items

def _today_blocks_page(blocks: list[dict], page: int, page_size: int = 5) -> tuple[list[dict], int]:
    if not blocks:
        return [], 1
    total_pages = max(1, (len(blocks) + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    return blocks[start:end], total_pages


def _today_collect_tasks() -> list[dict]:
    ok, data, status, err_text = _api_get_ex("/p2/tasks")
    if not ok or not isinstance(data, list):
        print(f"today_tasks_error status={status} err={err_text}")
        return []
    return data


def _today_tasks_planned_today(tasks: list[dict], day_str: str) -> list[dict]:
    items: list[dict] = []
    for t in tasks:
        state = (t.get("state") or "").strip().upper()
        if state not in {"PLANNED", "SCHEDULED"}:
            continue
        planned_at = t.get("planned_at")
        if not planned_at:
            continue
        parsed = _today_parse_planned_local(str(planned_at))
        if not parsed:
            continue
        d, hhmm = parsed
        if d != day_str:
            continue
        items.append(
            {
                "id": int(t.get("id") or 0),
                "title": _truncate(t.get("title") or ""),
                "time": hhmm,
                "state": state,
            }
        )
    items.sort(key=lambda r: r.get("time") or "")
    return items[:5]


def _today_backlog(tasks: list[dict]) -> list[dict]:
    items: list[dict] = []
    for t in tasks:
        state = (t.get("state") or "").strip().upper()
        status = (t.get("status") or "").strip().upper()
        if state not in {"NEW", "IN_PROGRESS"} and not (not state and status in {"NEW", "IN_PROGRESS"}):
            continue
        items.append(
            {
                "id": int(t.get("id") or 0),
                "title": _truncate(t.get("title") or ""),
            }
        )
    items.sort(key=lambda r: r.get("id") or 0, reverse=True)
    return items[:5]


def _today_collect_regs(period_key: str, day_str: str) -> list[dict]:
    ok_regs, data_regs, status_regs, err_regs = _api_get_ex("/p4/regulations")
    if not ok_regs or not isinstance(data_regs, list):
        print(f"today_regs_error status={status_regs} err={err_regs}")
        return []
    regs = [r for r in data_regs if (r.get("status") or "ACTIVE") == "ACTIVE"][:50]
    items: list[dict] = []
    for reg in regs:
        reg_id = reg.get("id")
        if reg_id is None:
            continue
        ok_run, data_run, status_run, err_run = _api_get_ex(
            f"/p4/regulations/{int(reg_id)}/runs", {"period": period_key}
        )
        if not ok_run or not isinstance(data_run, list) or not data_run:
            continue
        run = data_run[0]
        run_status = (run.get("status") or "").strip().upper()
        if run_status not in {"DUE", "OPEN"}:
            continue
        if str(run.get("due_date") or "") != day_str:
            continue
        items.append(
            {
                "run_id": int(run.get("id") or 0),
                "title": _truncate(reg.get("title") or ""),
                "day_of_month": reg.get("day_of_month"),
            }
        )
    items.sort(key=lambda r: (r.get("title") or "").lower())
    return items[:5]


def _today_build_message(
    day_str: str,
    tasks_today: list[dict],
    blocks_today: list[dict],
    blocks_page: int,
    blocks_total_pages: int,
    regs_today: list[dict],
    backlog: list[dict],
) -> tuple[str, dict]:
    d = datetime.fromisoformat(day_str)
    header = f"🧭 Сегодня — {d.strftime('%d.%m.%Y')}"
    tasks_sorted = sorted(tasks_today, key=lambda r: r.get("time") or "")
    lines = [header, "", "📌 Задачи на сегодня"]
    if tasks_sorted:
        for t in tasks_sorted:
            state = t.get("state")
            label = "В календаре" if state == "SCHEDULED" else "Запланирована"
            lines.append(f"• #{t.get('id')} {t.get('title')} ({t.get('time')}) [{label}]")
    else:
        lines.append("• Пусто.")

    lines.append("")
    lines.append("⏱️ Блоки дня")
    if blocks_today:
        for b in blocks_today:
            start_local = _format_local_hhmm(b["start_utc"])
            end_local = _format_local_hhmm(b["end_utc"])
            lines.append(
                f"• [{start_local}–{end_local}] #{b.get('task_id')} {b.get('title')}"
            )
        if blocks_total_pages > 1:
            lines.append(f"Стр. {blocks_page}/{blocks_total_pages}")
    else:
        lines.append("• Пусто.")

    lines.append("")
    lines.append("📅 Регламенты на сегодня")
    if regs_today:
        for r in regs_today:
            lines.append(f"• {r.get('title')} (до {r.get('day_of_month')})")
    else:
        lines.append("• Пусто.")

    lines.append("")
    lines.append("📥 Backlog")
    if backlog:
        for b in backlog:
            lines.append(f"• #{b.get('id')} {b.get('title')}")
    else:
        lines.append("• Пусто.")

    lines.append("")
    lines.append("⚠️ Сигналы")
    lines.append(f"Drift: {DRIFT_MODE}")
    lines.append(f"Overload: {OVERLOAD_MODE}")
    lines.append(f"Nudges: {P5_NUDGES_MODE}")

    kb_rows: list[list[dict]] = []
    for t in tasks_sorted:
        task_id = t.get("id")
        if task_id:
            kb_rows.append(
                [
                    {
                        "text": "✅ Готово",
                        "callback_data": f"today:task:done:{int(task_id)}",
                    }
                ]
            )
            if _p7_enabled():
                kb_rows.append(
                    [
                        {
                            "text": "📍 Выделить время",
                            "callback_data": f"today:time:open:{int(task_id)}",
                        }
                    ]
                )
    if blocks_today:
        for b in blocks_today:
            block_id = b.get("id")
            if block_id:
                kb_rows.append(
                    [
                        {"text": "⬅️ -10", "callback_data": f"today:block:move:{int(block_id)}:-10"},
                        {"text": "➡️ +10", "callback_data": f"today:block:move:{int(block_id)}:10"},
                        {"text": "🗑 Удалить", "callback_data": f"today:block:del:{int(block_id)}"},
                    ]
                )
        page_buttons = _today_blocks_page_keyboard(blocks_page, blocks_total_pages)
        if page_buttons:
            kb_rows.append(page_buttons)
    for r in regs_today:
        run_id = r.get("run_id")
        if run_id:
            kb_rows.append(
                [
                    {
                        "text": "✅ Выполнить",
                        "callback_data": f"today:reg:complete:{int(run_id)}",
                    },
                    {
                        "text": "⏭ Пропустить",
                        "callback_data": f"today:reg:skip:{int(run_id)}",
                    },
                ]
            )
    kb_rows.append(
        [
            {"text": "🔄 Обновить", "callback_data": "today:refresh"},
            {"text": "📅 /regs", "callback_data": "today:open_regs"},
            {"text": "📃 /list open", "callback_data": "today:open_list_open"},
        ]
    )
    return "\n".join(lines), {"inline_keyboard": kb_rows}


def _today_render(chat_id: int, message_id: int | None, page: int = 1) -> None:
    today = datetime.now(_tz_local()).date()
    day_str = today.isoformat()
    tasks = _today_collect_tasks()
    tasks_today = _today_tasks_planned_today(tasks, day_str)
    blocks = _today_collect_blocks(day_str) if _p7_enabled() else []
    blocks_page, total_pages = _today_blocks_page(blocks, page)
    regs_today = _today_collect_regs(_regs_period_key(datetime.now(_tz_local())), day_str)
    backlog = _today_backlog(tasks)
    text, keyboard = _today_build_message(day_str, tasks_today, blocks_page, page, total_pages, regs_today, backlog)
    if message_id is None:
        _send_message_with_keyboard(chat_id, text, keyboard)
        return
    _edit_message_with_keyboard(chat_id, message_id, text, keyboard)


def _render_list_open(chat_id: int) -> None:
    ok_new, data_new, status_new, err_new = _api_get_ex("/p2/tasks", {"status": "NEW"})
    ok_ip, data_ip, status_ip, err_ip = _api_get_ex("/p2/tasks", {"status": "IN_PROGRESS"})
    if (
        not ok_new
        or not ok_ip
        or not isinstance(data_new, list)
        or not isinstance(data_ip, list)
    ):
        print(
            "p2_list_open_error status_new=%s status_ip=%s err_new=%s err_ip=%s",
            status_new,
            status_ip,
            err_new,
            err_ip,
        )
        _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
        return
    items = data_new + data_ip
    merged: dict[int, dict] = {}
    for it in items:
        try:
            iid = int((it or {}).get("id") or 0)
        except Exception:
            continue
        if iid <= 0:
            continue
        if iid not in merged:
            merged[iid] = it
    items_sorted = sorted(merged.values(), key=lambda r: int((r or {}).get("id") or 0), reverse=True)
    page_items = _slice_page(items_sorted, 1)
    if not page_items:
        _send_message(chat_id, "Пусто.")
        return
    lines = []
    for it in page_items:
        iid = it.get("id")
        status = (it.get("state") or "").strip()
        title = _truncate(it.get("title") or "")
        planned_local = _planned_at_local(it.get("planned_at"))
        suffix = f" @ {planned_local}" if planned_local else ""
        lines.append(f"#{iid} [{status}] {title}{suffix}".strip())
    _send_message(chat_id, "\n".join(lines))


def _truncate(text: str, max_len: int = 80) -> str:
    s = _normalize_ws(text)
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def _planned_at_local(iso_text: str | None) -> str:
    if not iso_text:
        return ""
    try:
        s = iso_text.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local_dt = dt.astimezone(_tz_local())
        return local_dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""


def _slice_page(items: list, page: int, page_size: int = 10) -> list:
    if page < 1 or page > 50:
        return []
    start = (page - 1) * page_size
    end = start + page_size
    return items[start:end]


def _source_msg_id(chat_id: int, message_id: int | None) -> str | None:
    if message_id is None:
        return None
    return f"tg:{chat_id}:{int(message_id)}"


def _p2_handle_text(chat_id: int, message_id: int | None, text: str) -> None:
    src_id = _source_msg_id(chat_id, message_id)
    if not src_id:
        return
    raw = text.strip()
    now_ts = time.time()
    _prune_p2_pending_state(_p2_pending_state, now_ts)
    pending = _p2_pending_state.get(chat_id)
    if pending and raw and not raw.startswith("/"):
        mode = pending.get("mode")
        if mode == "task":
            parent_type = pending.get("parent_type")
            parent_id = pending.get("parent_id")
            ok, data, status, err_text = _worker_post_ex(
                "/p2/commands/create_task",
                {
                    "title": raw,
                    "status": "NEW",
                    "source_msg_id": src_id,
                    "parent_type": parent_type,
                    "parent_id": parent_id,
                },
            )
            if ok and data:
                _send_message(chat_id, f"✅ Задача создана: task #{data.get('id')}")
            else:
                print(f"p2_cmd=create_task_pending source_msg_id={src_id} ok=0 status={status} err={err_text}")
                _send_message(chat_id, "⚠️ Не удалось создать задачу.")
        elif mode == "cycle_outcome":
            cycle_id = pending.get("cycle_id")
            kind = pending.get("kind")
            ok, data, status, err_text = _worker_post_ex(
                "/p2/commands/add_cycle_outcome",
                {"cycle_id": cycle_id, "kind": kind, "text": raw, "source_msg_id": src_id},
            )
            if ok and data:
                _send_message(chat_id, f"✅ Результат добавлен: outcome #{data.get('id')}")
            else:
                print(f"p2_cmd=add_cycle_outcome source_msg_id={src_id} ok=0 status={status} err={err_text}")
                _send_message(chat_id, "⚠️ Не удалось добавить результат.")
            _p2_pending_state.pop(chat_id, None)
            _save_p2_pending_state(_p2_pending_state)
            return
        if mode == "project_create":
            ok, data, status, err_text = _worker_post_ex(
                "/p2/commands/create_project",
                {"title": raw, "source_msg_id": src_id},
            )
            if ok and data:
                _send_message(chat_id, f"✅ Проект создан: #{data.get('id')}")
            else:
                print(f"p2_cmd=create_project source_msg_id={src_id} ok=0 status={status} err={err_text}")
                _send_message(chat_id, "⚠️ Не удалось создать проект.")
            _p2_pending_state.pop(chat_id, None)
            _save_p2_pending_state(_p2_pending_state)
            return
        if mode == "reg_add":
            step = int(pending.get("step") or 1)
            if step == 1:
                _p2_pending_state[chat_id] = {
                    "mode": "reg_add",
                    "step": 2,
                    "title": raw,
                    "expires_at": time.time() + P2_PENDING_TTL_SEC,
                }
                _save_p2_pending_state(_p2_pending_state)
                _send_message(chat_id, "День месяца (1-31):")
                return
            if step == 2:
                try:
                    day_of_month = int(raw)
                except Exception:
                    _send_message(chat_id, "Формат: число 1-31.")
                    return
                if day_of_month < 1 or day_of_month > 31:
                    _send_message(chat_id, "Формат: число 1-31.")
                    return
                _p2_pending_state[chat_id] = {
                    "mode": "reg_add",
                    "step": 3,
                    "title": pending.get("title") or "",
                    "day_of_month": day_of_month,
                    "expires_at": time.time() + P2_PENDING_TTL_SEC,
                }
                _save_p2_pending_state(_p2_pending_state)
                _send_message(chat_id, "Время (HH:MM) или '-' если без времени:")
                return
            if step == 3:
                try:
                    due_time_local = _normalize_due_time_local(raw)
                except Exception:
                    _send_message(chat_id, "Формат: HH:MM или '-'")
                    return
                ok, data, status, err_text = _worker_post_ex(
                    "/p4/commands/create_regulation",
                    {
                        "title": pending.get("title") or "",
                        "day_of_month": int(pending.get("day_of_month") or 1),
                        "due_time_local": due_time_local,
                        "source_msg_id": src_id,
                    },
                )
                if ok and data:
                    _send_message(chat_id, f"✅ Регламент создан: #{data.get('id')}")
                else:
                    print(f"p4_cmd=create_regulation source_msg_id={src_id} ok=0 status={status} err={err_text}")
                    _send_message(chat_id, "⚠️ Не удалось создать регламент.")
                _p2_pending_state.pop(chat_id, None)
                _save_p2_pending_state(_p2_pending_state)
                return
        if mode == "cycle_goal":
            cycle_id = pending.get("cycle_id")
            count = int(pending.get("count") or 0)
            if raw.strip().lower() in {"готово", "стоп", "хватит"} and count >= 1:
                _p2_pending_state.pop(chat_id, None)
                _save_p2_pending_state(_p2_pending_state)
                _send_message_with_keyboard(
                    chat_id,
                    f"Цели собраны для цикла #{cycle_id}. Что дальше?",
                    {
                        "inline_keyboard": [
                            [
                                {
                                    "text": "Добавить задачу",
                                    "callback_data": f"p2:cycle:add_task:{cycle_id}",
                                },
                                {
                                    "text": "Добавить проект",
                                    "callback_data": f"p2:cycle:add_project:{cycle_id}",
                                },
                            ],
                            [
                                {
                                    "text": "Закрыть цикл",
                                    "callback_data": f"p2:cycle:close:{cycle_id}",
                                }
                            ],
                        ]
                    },
                )
                return
            ok, data, status, err_text = _worker_post_ex(
                "/p2/commands/add_cycle_goal",
                {"cycle_id": cycle_id, "text": raw, "source_msg_id": src_id},
            )
            if ok and data:
                count += 1
                if count >= 3:
                    _p2_pending_state.pop(chat_id, None)
                    _save_p2_pending_state(_p2_pending_state)
                    _send_message_with_keyboard(
                        chat_id,
                        f"Цели собраны для цикла #{cycle_id}. Что дальше?",
                        {
                            "inline_keyboard": [
                                [
                                    {
                                        "text": "Добавить задачу",
                                        "callback_data": f"p2:cycle:add_task:{cycle_id}",
                                    },
                                    {
                                        "text": "Добавить проект",
                                        "callback_data": f"p2:cycle:add_project:{cycle_id}",
                                    },
                                ],
                                [
                                    {
                                        "text": "Закрыть цикл",
                                        "callback_data": f"p2:cycle:close:{cycle_id}",
                                    }
                                ],
                            ]
                        },
                    )
                    return
                _p2_pending_state[chat_id] = {
                    "mode": "cycle_goal",
                    "cycle_id": cycle_id,
                    "count": count,
                    "expires_at": time.time() + P2_PENDING_TTL_SEC,
                }
                _save_p2_pending_state(_p2_pending_state)
                _send_message(chat_id, f"Цель {count + 1}/3:")
            else:
                print(f"p2_cmd=add_cycle_goal source_msg_id={src_id} ok=0 status={status} err={err_text}")
                _send_message(chat_id, "⚠️ Не удалось добавить цель.")
            return
        if mode == "task_plan":
            task_id = pending.get("task_id")
            try:
                planned_iso, planned_local = _parse_plan_input(raw)
            except Exception:
                _send_message(chat_id, "Формат: YYYY-MM-DD HH:MM")
                return
            ok, data, status, err_text = _worker_post_ex(
                "/p2/commands/plan_task",
                {"task_id": task_id, "planned_at": planned_iso, "source_msg_id": src_id},
            )
            if ok and data:
                _send_message(chat_id, f"Ок, запланировал на {planned_local}.")
                _p2_pending_state.pop(chat_id, None)
                _save_p2_pending_state(_p2_pending_state)
                return
            print(f"p2_cmd=plan_task_pending source_msg_id={src_id} ok=0 status={status} err={err_text}")
            _send_message(chat_id, "⚠️ Не удалось запланировать.")
            return
        _p2_pending_state.pop(chat_id, None)
        _save_p2_pending_state(_p2_pending_state)
        return
    if raw == "/help":
        _send_message(
            chat_id,
            "\n".join(
                [
                    "Команды:",
                    "Inbox:",
                    "/inbox — буфер принятия решений",
                    "/inbox pN — страница N",
                    "Типы: циклы/направления/проекты/unplanned задачи",
                    "/list — последние задачи",
                    "/list open — незавершённые",
                    "/list <task_id> — задача и подзадачи",
                    "/done <task_id> — завершить задачу",
                    "/sdone <subtask_id> — завершить подзадачу",
                    "Подзадача: #<task_id> <текст>",
                    "Направление: !! <текст>",
                    "Проект: @@ <текст>",
                    "Обычный текст — новая задача",
                    "Кнопки задачи в /inbox:",
                    "Запланировать — меню:",
                    "Через 1/3 дней (10:00)",
                    "Выбрать дату… (YYYY-MM-DD HH:MM)",
                    "Оставить без даты",
                    "Напомнить через 7 дней — отдельная быстрая кнопка",
                ]
            ),
        )
        return
    if raw == "/state":
        ok_set, data_set, status_set, err_set = _api_get_ex(
            "/p2/user_settings", {"user_id": str(chat_id)}
        )
        if not ok_set or not isinstance(data_set, dict):
            print(f"p2_state_error user_id={chat_id} status={status_set} err={err_set}")
            _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
            return
        overload_enabled = int((data_set or {}).get("overload_enabled") or 0)
        drift_enabled = int((data_set or {}).get("drift_enabled") or 0)
        signals_on = 1 if overload_enabled or drift_enabled else 0
        text_line = (
            f"Сигналы управления: {'ВКЛ' if signals_on else 'ВЫКЛ'}\n"
            f"Перегрузка: {'ВКЛ' if overload_enabled else 'ВЫКЛ'}\n"
            f"Связность: {'ВКЛ' if drift_enabled else 'ВЫКЛ'}"
        )
        reply_markup = {
            "inline_keyboard": [
                [
                    {
                        "text": "Перегрузка: ВКЛ" if not overload_enabled else "Перегрузка: ВЫКЛ",
                        "callback_data": f"p2:signals:toggle_direct:overload:{chat_id}",
                    },
                    {
                        "text": "Связность: ВКЛ" if not drift_enabled else "Связность: ВЫКЛ",
                        "callback_data": f"p2:signals:toggle_direct:drift:{chat_id}",
                    },
                ],
            ]
        }
        shortcut = _signals_shortcut_keyboard(str(chat_id))
        reply_markup["inline_keyboard"] = shortcut["inline_keyboard"] + reply_markup["inline_keyboard"]
        _send_message_with_keyboard(chat_id, text_line, reply_markup)
        return
    if raw in {"/today", "/status"}:
        _today_render(chat_id, None)
        return
    if raw == "/reg add":
        _p2_pending_state[chat_id] = {
            "mode": "reg_add",
            "step": 1,
            "expires_at": time.time() + P2_PENDING_TTL_SEC,
        }
        _save_p2_pending_state(_p2_pending_state)
        _send_message(chat_id, "Название регламента:")
        return
    if raw in {"/reg", "/regs"}:
        now_local = datetime.now(_tz_local())
        period_key = _regs_period_key(now_local)
        _regs_render(chat_id, None, period_key, page=1)
        return
    m_plan = re.match(r"^/plan(?:\s+|#)(\d+)\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})$", raw)
    if m_plan:
        task_id = int(m_plan.group(1))
        date_part = m_plan.group(2)
        time_part = m_plan.group(3)
        try:
            local_dt = datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M")
            local_dt = local_dt.replace(tzinfo=_tz_local())
            planned_iso = local_dt.astimezone(timezone.utc).isoformat()
        except Exception:
            _send_message(chat_id, "⚠️ Не удалось выполнить команду.")
            return
        ok, data, status, err_text = _worker_post_ex(
            "/p2/commands/plan_task",
            {"task_id": task_id, "planned_at": planned_iso, "source_msg_id": src_id},
        )
        if ok and data:
            print(f"p2_cmd=plan_task source_msg_id={src_id} ok=1 id={data.get('id')}")
            _send_message(chat_id, f"✅ Запланировано: task #{data.get('id')} {date_part} {time_part}")
        else:
            print(f"p2_cmd=plan_task source_msg_id={src_id} ok=0 status={status} err={err_text}")
            if status == 400:
                _send_message(chat_id, "⚠️ Не удалось выполнить команду.")
            else:
                _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
        return
    m_done = re.match(r"^/done(?:\s+|#)(\d+)$", raw)
    if m_done:
        task_id = int(m_done.group(1))
        ok, data, status, err_text = _worker_post_ex("/p2/commands/complete_task", {"task_id": task_id})
        if ok and data:
            print(f"p2_cmd=complete_task source_msg_id={src_id} ok=1 id={data.get('id')}")
            _send_message(chat_id, f"✅ Готово: task #{data.get('id')}")
        else:
            print(f"p2_cmd=complete_task source_msg_id={src_id} ok=0 status={status} err={err_text}")
            if status == 400 and err_text and "open subtasks" in err_text:
                _send_message(chat_id, "⚠️ Нельзя завершить: есть незавершённые подзадачи.")
            elif status == 400:
                _send_message(chat_id, "⚠️ Не удалось выполнить команду.")
            else:
                _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
        return

    m_sdone = re.match(r"^/sdone(?:\s+|#)(\d+)$", raw)
    if m_sdone:
        sub_id = int(m_sdone.group(1))
        ok, data, status, err_text = _worker_post_ex("/p2/commands/complete_subtask", {"subtask_id": sub_id})
        if ok and data:
            print(f"p2_cmd=complete_subtask source_msg_id={src_id} ok=1 id={data.get('id')}")
            _send_message(chat_id, f"✅ Готово: subtask #{data.get('id')}")
        else:
            print(f"p2_cmd=complete_subtask source_msg_id={src_id} ok=0 status={status} err={err_text}")
            if status == 400:
                _send_message(chat_id, "⚠️ Не удалось выполнить команду.")
            else:
                _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
        return

    if raw == "/open":
        raw = "/list open"

    m_inbox_page = re.match(r"^/inbox\s+p(\d+)$", raw)
    if raw == "/inbox" or m_inbox_page:
        page = int(m_inbox_page.group(1)) if m_inbox_page else 1
        _worker_post("/p2/commands/ensure_user_settings", {"user_id": str(chat_id), "source_msg_id": src_id})
        ok_set, data_set, status_set, err_set = _api_get_ex("/p2/user_settings", {"user_id": str(chat_id)})
        ok_state, data_state, status_state, err_state = _api_get_ex("/p2/state", {"user_id": str(chat_id)})
        ok_new, data_new, status_new, err_new = _api_get_ex("/p2/tasks", {"status": "NEW"})
        ok_dir, data_dir, status_dir, err_dir = _api_get_ex("/p2/directions")
        ok_proj, data_proj, status_proj, err_proj = _api_get_ex("/p2/projects", {"inbox": 1})
        ok_cyc, data_cyc, status_cyc, err_cyc = _api_get_ex("/p2/cycles")
        if (
            not ok_new
            or not ok_dir
            or not ok_cyc
            or not isinstance(data_new, list)
            or not isinstance(data_dir, list)
            or not isinstance(data_cyc, list)
        ):
            print(
                "p2_inbox_error source_msg_id=%s status_new=%s status_dir=%s status_proj=%s status_cyc=%s "
                "err_new=%s err_dir=%s err_proj=%s err_cyc=%s",
                src_id,
                status_new,
                status_dir,
                status_proj,
                status_cyc,
                err_new,
                err_dir,
                err_proj,
                err_cyc,
            )
            _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
            return
        overload_enabled = 0
        drift_enabled = 0
        if ok_set and isinstance(data_set, dict):
            overload_enabled = int((data_set or {}).get("overload_enabled") or 0)
            drift_enabled = int((data_set or {}).get("drift_enabled") or 0)
        if overload_enabled or drift_enabled:
            if ok_state and isinstance(data_state, dict):
                line = _state_line_from_payload(data_state)
                if line:
                    _send_message(chat_id, line)
        else:
            ok_nudge, data_nudge, status_nudge, err_nudge = _api_get_ex(
                "/p2/user_nudges", {"user_id": str(chat_id), "nudge_key": NUDGE_SIGNALS_KEY}
            )
            if ok_nudge and isinstance(data_nudge, dict):
                now = datetime.now(timezone.utc)
                if _nudge_is_due(data_nudge.get("next_at"), now):
                    reply_markup = {
                        "inline_keyboard": [
                            [
                                {
                                    "text": "Настроить",
                                    "callback_data": f"p2:signals:configure:{chat_id}",
                                },
                                {
                                    "text": "Не сейчас",
                                    "callback_data": f"p2:signals:snooze:{chat_id}",
                                },
                            ]
                        ]
                    }
                    _send_message_with_keyboard(
                        chat_id,
                        "Хочешь включить сигналы управления?",
                        reply_markup,
                    )

        if not ok_proj or not isinstance(data_proj, list):
            print(
                "p2_inbox_projects_fallback source_msg_id=%s status_proj=%s err_proj=%s",
                src_id,
                status_proj,
                err_proj,
            )
            ok_proj_all, data_proj_all, status_proj_all, err_proj_all = _api_get_ex("/p2/projects")
            if ok_proj_all and isinstance(data_proj_all, list):
                data_proj = data_proj_all
            else:
                data_proj = []
        inbox_items = _build_inbox_items(data_cyc, data_dir, data_proj, data_new)
        page_items = _slice_page(inbox_items, page)
        if not page_items:
            _send_message(chat_id, "Пусто.")
            return

        month_key = datetime.now(_tz_local()).strftime("%Y-%m")
        q = (datetime.now(_tz_local()).month - 1) // 3 + 1
        quarter_key = f"{datetime.now(_tz_local()).year}-Q{q}"
        _send_message_with_keyboard(
            chat_id,
            f"Обзоры: {month_key} / {quarter_key}",
            {
                "inline_keyboard": [
                    [
                        {
                            "text": "Начать месячный обзор",
                            "callback_data": "p2:cycle:start:MONTHLY",
                        }
                    ],
                    [
                        {
                            "text": "Начать квартальный обзор",
                            "callback_data": "p2:cycle:start:QUARTERLY",
                        }
                    ],
                ]
            },
        )

        for it in page_items:
            t = it.get("kind")
            if t == "direction":
                text_line = f"📌 Направление #{it.get('id')} — {_truncate(it.get('title') or '')}"
                reply_markup = {
                    "inline_keyboard": [
                        [
                            {
                                "text": "Сделать проектом",
                                "callback_data": f"p2:direction:convert:{it.get('id')}",
                            }
                        ]
                    ]
                }
                _send_message_with_keyboard(chat_id, text_line, reply_markup)
            elif t == "project":
                text_line = f"📁 Проект #{it.get('id')} — {_truncate(it.get('title') or '')}"
                reply_markup = {
                    "inline_keyboard": [
                        [
                            {
                                "text": "Добавить задачу",
                                "callback_data": f"p2:project:add_task:{it.get('id')}",
                            }
                        ]
                    ]
                }
                _send_message_with_keyboard(chat_id, text_line, reply_markup)
            elif t == "cycle":
                period = it.get("period_key") or "-"
                text_line = f"🔁 Цикл #{it.get('id')} — {it.get('type')} {period}"
                reply_markup = {
                    "inline_keyboard": [
                        [
                            {
                                "text": "Добавить цель",
                                "callback_data": f"p2:cycle:add_goal:{it.get('id')}",
                            },
                        ],
                        [
                            {
                                "text": "Добавить задачу",
                                "callback_data": f"p2:cycle:add_task:{it.get('id')}",
                            }
                        ],
                    ]
                }
                _send_message_with_keyboard(chat_id, text_line, reply_markup)
            else:
                iid = it.get("id")
                title = _truncate(it.get("title") or "")
                reply_markup = {
                    "inline_keyboard": [
                        [
                            {
                                "text": "Запланировать",
                                "callback_data": f"p2:task:plan:{iid}",
                            },
                            {
                                "text": "Напомнить через 7 дней",
                                "callback_data": f"p2:task:plan_later:{iid}",
                            },
                        ],
                    ]
                }
                _send_message_with_keyboard(chat_id, f"🧩 Задача #{iid} — {title}".strip(), reply_markup)
        return

    m_list_open_page = re.match(r"^/list\s+open\s+p(\d+)$", raw)
    if m_list_open_page:
        page = int(m_list_open_page.group(1))
        ok_new, data_new, status_new, err_new = _api_get_ex("/p2/tasks", {"status": "NEW"})
        ok_ip, data_ip, status_ip, err_ip = _api_get_ex("/p2/tasks", {"status": "IN_PROGRESS"})
        if (
            not ok_new
            or not ok_ip
            or not isinstance(data_new, list)
            or not isinstance(data_ip, list)
        ):
            print(
                "p2_list_open_error source_msg_id=%s status_new=%s status_ip=%s err_new=%s err_ip=%s",
                src_id,
                status_new,
                status_ip,
                err_new,
                err_ip,
            )
            _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
            return
        items = data_new + data_ip
        merged: dict[int, dict] = {}
        for it in items:
            try:
                iid = int((it or {}).get("id") or 0)
            except Exception:
                continue
            if iid <= 0:
                continue
            if iid not in merged:
                merged[iid] = it
        items_sorted = sorted(merged.values(), key=lambda r: int((r or {}).get("id") or 0), reverse=True)
        page_items = _slice_page(items_sorted, page)
        if not page_items:
            _send_message(chat_id, "Пусто.")
            return
        lines = []
        for it in page_items:
            iid = it.get("id")
            status = (it.get("state") or "").strip()
            title = _truncate(it.get("title") or "")
            planned_local = _planned_at_local(it.get("planned_at"))
            suffix = f" @ {planned_local}" if planned_local else ""
            lines.append(f"#{iid} [{status}] {title}{suffix}".strip())
        _send_message(chat_id, "\n".join(lines))
        return

    m_list_page = re.match(r"^/list\s+p(\d+)$", raw)
    if m_list_page:
        page = int(m_list_page.group(1))
        ok, data, status, err_text = _api_get_ex("/p2/tasks")
        if not ok or not isinstance(data, list):
            print(f"p2_list_error source_msg_id={src_id} status={status} err={err_text}")
            _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
            return
        items_sorted = sorted(data, key=lambda r: int((r or {}).get("id") or 0), reverse=True)
        page_items = _slice_page(items_sorted, page)
        if not page_items:
            _send_message(chat_id, "Пусто.")
            return
        lines = []
        for it in page_items:
            iid = it.get("id")
            status = (it.get("state") or "").strip()
            title = _truncate(it.get("title") or "")
            planned_local = _planned_at_local(it.get("planned_at"))
            suffix = f" @ {planned_local}" if planned_local else ""
            lines.append(f"#{iid} [{status}] {title}{suffix}".strip())
        _send_message(chat_id, "\n".join(lines))
        return

    if raw == "/list" or raw == "/list open":
        if raw == "/list":
            ok, data, status, err_text = _api_get_ex("/p2/tasks")
            if not ok or not isinstance(data, list):
                print(f"p2_list_error source_msg_id={src_id} status={status} err={err_text}")
                _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
                return
            items = data
        else:
            ok_new, data_new, status_new, err_new = _api_get_ex("/p2/tasks", {"status": "NEW"})
            ok_ip, data_ip, status_ip, err_ip = _api_get_ex("/p2/tasks", {"status": "IN_PROGRESS"})
            if (
                not ok_new
                or not ok_ip
                or not isinstance(data_new, list)
                or not isinstance(data_ip, list)
            ):
                print(
                    "p2_list_open_error source_msg_id=%s status_new=%s status_ip=%s err_new=%s err_ip=%s",
                    src_id,
                    status_new,
                    status_ip,
                    err_new,
                    err_ip,
                )
                _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
                return
            items = data_new + data_ip

        merged: dict[int, dict] = {}
        for it in items:
            try:
                iid = int((it or {}).get("id") or 0)
            except Exception:
                continue
            if iid <= 0:
                continue
            if iid not in merged:
                merged[iid] = it
        items_sorted = sorted(merged.values(), key=lambda r: int((r or {}).get("id") or 0), reverse=True)
        page_items = _slice_page(items_sorted, 1)
        if not page_items:
            _send_message(chat_id, "Пусто.")
            return
        lines = []
        for it in page_items:
            iid = it.get("id")
            status = (it.get("state") or "").strip()
            title = _truncate(it.get("title") or "")
            planned_local = _planned_at_local(it.get("planned_at"))
            suffix = f" @ {planned_local}" if planned_local else ""
            lines.append(f"#{iid} [{status}] {title}{suffix}".strip())
        _send_message(chat_id, "\n".join(lines))
        return

    m_list_one = re.match(r"^/list\s+(\d+)$", raw)
    if m_list_one:
        task_id = int(m_list_one.group(1))
        ok_t, data_t, status_t, err_t = _api_get_ex(f"/p2/tasks/{task_id}")
        if not ok_t:
            if status_t == 404:
                _send_message(chat_id, "⚠️ Не найдено.")
            else:
                print(f"p2_list_task_error source_msg_id={src_id} status={status_t} err={err_t}")
                _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
            return
        if not isinstance(data_t, dict):
            print(f"p2_list_task_bad_payload source_msg_id={src_id} status={status_t}")
            _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
            return
        ok_s, data_s, status_s, err_s = _api_get_ex(f"/p2/tasks/{task_id}/subtasks")
        if not ok_s or not isinstance(data_s, list):
            print(f"p2_list_subtasks_error source_msg_id={src_id} status={status_s} err={err_s}")
            _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
            return
        title = _truncate(data_t.get("title") or "")
        status = (data_t.get("state") or "").strip()
        planned_local = _planned_at_local(data_t.get("planned_at"))
        suffix = f" @ {planned_local}" if planned_local else ""
        lines = [f"task #{task_id} [{status}] {title}{suffix}".strip()]
        if not data_s:
            lines.append("  (нет подзадач)")
        else:
            for sub in data_s:
                sid = sub.get("id")
                sst = (sub.get("status") or "").strip()
                stitle = _truncate(sub.get("title") or "")
                lines.append(f"  - sub #{sid} [{sst}] {stitle}".rstrip())
        _send_message(chat_id, "\n".join(lines))
        return

    m = re.match(r"^#(\d+)\s+(.+)$", raw)
    try:
        m_dir = re.match(r"^!!\s+(.+)$", raw)
        m_proj = re.match(r"^@@\s+(.+)$", raw)
        if m_dir:
            title = m_dir.group(1).strip()
            direction = _worker_post(
                "/p2/commands/create_direction",
                {"title": title, "source_msg_id": src_id},
            )
            if direction:
                print(f"p2_cmd=create_direction source_msg_id={src_id} id={direction.get('id')}")
        elif m_proj:
            title = m_proj.group(1).strip()
            project = _worker_post(
                "/p2/commands/create_project",
                {"title": title, "source_msg_id": src_id},
            )
            if project:
                print(f"p2_cmd=create_project source_msg_id={src_id} id={project.get('id')}")
        elif m:
            task_id = int(m.group(1))
            title = m.group(2).strip()
            sub = _worker_post(
                "/p2/commands/create_subtask",
                {"task_id": task_id, "title": title, "status": "NEW", "source_msg_id": src_id},
            )
            if sub:
                print(f"p2_cmd=create_subtask source_msg_id={src_id} id={sub.get('id')}")
        else:
            task = _worker_post(
                "/p2/commands/create_task",
                {"title": raw, "status": "NEW", "source_msg_id": src_id},
            )
            if task:
                print(f"p2_cmd=create_task source_msg_id={src_id} id={task.get('id')}")
    except Exception as exc:
        print(f"p2_cmd_error source_msg_id={src_id} err={str(exc)[:200]}")


def _p2_handle_voice(chat_id: int, message_id: int | None, voice: dict) -> None:
    src_id = _source_msg_id(chat_id, message_id)
    if not src_id:
        return
    fid = voice.get("file_unique_id") or voice.get("file_id") or ""
    title = f"voice:{fid}" if fid else "voice"
    try:
        task = _worker_post(
            "/p2/commands/create_task",
            {"title": title, "status": "NEW", "source_msg_id": src_id},
        )
        if task:
            print(f"p2_cmd=create_task source_msg_id={src_id} id={task.get('id')}")
    except Exception as exc:
        print(f"p2_cmd_error source_msg_id={src_id} err={str(exc)[:200]}")


def _tz_local() -> tzinfo:
    if ZoneInfo is not None:
        try:
            return ZoneInfo(TIMEZONE)
        except Exception:
            pass
    return timezone(timedelta(minutes=LOCAL_TZ_OFFSET_MIN))


def _api_url(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"


def _safe_json(resp: Any) -> dict | None:
    try:
        data = resp.json()
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _init_queue_schema() -> None:
    with _db_connect() as conn:
        sql = Path(SCHEMA_PATH).read_text(encoding="utf-8")
        conn.executescript(sql)
        conn.commit()


def _queue_depths() -> tuple[int, int]:
    with _db_connect() as conn:
        depth_new = conn.execute(
            "SELECT COUNT(*) AS cnt FROM inbox_queue WHERE status='NEW'"
        ).fetchone()["cnt"]
        depth_total = conn.execute(
            "SELECT COUNT(*) AS cnt FROM inbox_queue"
        ).fetchone()["cnt"]
    return int(depth_new), int(depth_total)

def _enqueue_text(update_id: int, chat_id: int, message_id: int | None, text: str) -> tuple[bool, int]:
    payload = {
        "text": text,
        "_meta": {
            "tg_chat_id": chat_id,
            "tg_update_id": update_id,
            "tg_message_id": message_id,
            "enqueued_at": time.time(),
        },
    }
    with _db_connect() as conn:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO inbox_queue
            (source, tg_chat_id, tg_update_id, tg_message_id, kind, payload_json)
            VALUES (?, ?, ?, ?, 'text', ?)
            """,
            ("telegram", chat_id, update_id, message_id, json.dumps(payload, ensure_ascii=False)),
        )
        conn.commit()
        inserted = cur.rowcount == 1
        depth_new = conn.execute(
            "SELECT COUNT(*) AS cnt FROM inbox_queue WHERE status='NEW'"
        ).fetchone()["cnt"]
    return inserted, int(depth_new)

def _handle_text_message(update_id: int, message: dict, pending_state: dict[int, dict]) -> None:
    chat_id = int(message["chat"]["id"])
    text_msg = (message.get("text") or "").strip()
    if not text_msg:
        return

    depth_new, depth_total = _queue_depths()
    if depth_new >= B2_QUEUE_MAX_NEW or depth_total >= B2_QUEUE_MAX_TOTAL:
        if B2_BACKPRESSURE_MODE == "reject":
            _send_message(chat_id, "Очередь перегружена. Подожди 1–2 минуты и отправь ещё раз.")
            return

    inserted, depth_new_after = _enqueue_text(
        update_id=update_id,
        chat_id=chat_id,
        message_id=message.get("message_id"),
        text=text_msg,
    )
    _p2_handle_text(chat_id, message.get("message_id"), text_msg)
    if inserted:
        _send_message(chat_id, f"Принято. В очереди: {depth_new_after}.")
    else:
        _send_message(chat_id, f"Уже принято ранее. В очереди: {depth_new}.")


def _enqueue_voice(update_id: int, chat_id: int, message_id: int | None, voice: dict) -> tuple[bool, int]:
    payload = {
        "file_id": voice.get("file_id"),
        "file_unique_id": voice.get("file_unique_id"),
        "duration": voice.get("duration"),
        "_meta": {
            "tg_chat_id": chat_id,
            "tg_update_id": update_id,
            "tg_message_id": message_id,
            "enqueued_at": time.time(),
        },
    }
    with _db_connect() as conn:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO inbox_queue
            (source, tg_chat_id, tg_update_id, tg_message_id, kind, payload_json)
            VALUES (?, ?, ?, ?, 'voice', ?)
            """,
            ("telegram", chat_id, update_id, message_id, json.dumps(payload, ensure_ascii=False)),
        )
        conn.commit()
        inserted = cur.rowcount == 1
        depth_new = conn.execute(
            "SELECT COUNT(*) AS cnt FROM inbox_queue WHERE status='NEW'"
        ).fetchone()["cnt"]
    return inserted, int(depth_new)


def _send_message(chat_id: int, text: str) -> None:
    # Telegram sometimes may timeout; avoid spamming user with duplicates.
    last_exc: Exception | None = None
    for _ in range(max(1, TG_SEND_MAX_RETRIES)):
        try:
            req_mod = _require_requests()
            resp = req_mod.post(
                _api_url("sendMessage"),
                json={"chat_id": chat_id, "text": text},
                timeout=TG_HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            return
        except _RETRY_EXCEPTIONS as exc:
            last_exc = exc
            time.sleep(0.3)
    if last_exc:
        raise last_exc


def _send_message_with_keyboard(chat_id: int, text: str, reply_markup: dict) -> None:
    last_exc: Exception | None = None
    for _ in range(max(1, TG_SEND_MAX_RETRIES)):
        try:
            req_mod = _require_requests()
            resp = req_mod.post(
                _api_url("sendMessage"),
                json={"chat_id": chat_id, "text": text, "reply_markup": reply_markup},
                timeout=TG_HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            return
        except _RETRY_EXCEPTIONS as exc:
            last_exc = exc
            time.sleep(0.3)
    if last_exc:
        raise last_exc


def _heartbeat_loop() -> None:
    while True:
        try:
            with open("/tmp/bot.ok", "w", encoding="utf-8") as marker:
                marker.write("ok\n")
        except Exception:
            pass
        time.sleep(5)


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - stdlib API
        if self.path != "/health":
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, format, *args):  # noqa: A003 - stdlib API
        return


def _start_health_server() -> None:
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()


def _get_updates(offset: int) -> list[dict]:
    req_mod = _require_requests()
    resp = req_mod.get(
        _api_url("getUpdates"),
        params={"timeout": TG_LONGPOLL_SEC, "offset": offset},
        timeout=TG_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    data = _safe_json(resp)
    if not data or not data.get("ok"):
        return []
    return data.get("result", [])

def _load_offset() -> int:
    if not os.path.exists(STATE_PATH):
        return 0
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return 0


def _save_offset(offset: int, update_id: int) -> None:
    tmp_path = f"{STATE_PATH}.tmp"
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(str(offset))
    os.replace(tmp_path, STATE_PATH)
    print(f"saved offset={offset} update_id={update_id}")


def _load_clarify_state() -> dict[int, dict]:
    if not os.path.exists(CLARIFY_STATE_PATH):
        return {}
    try:
        with open(CLARIFY_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {int(k): v for k, v in data.items() if isinstance(v, dict)}
    except Exception:
        return {}
    return {}


def _save_clarify_state(state: dict[int, dict]) -> None:
    tmp_path = f"{CLARIFY_STATE_PATH}.tmp"
    os.makedirs(os.path.dirname(CLARIFY_STATE_PATH), exist_ok=True)
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)
    os.replace(tmp_path, CLARIFY_STATE_PATH)


def _prune_clarify_state(state: dict[int, dict], now_ts: float) -> None:
    expired = [cid for cid, st in state.items() if (st or {}).get("expires_at", 0) <= now_ts]
    for cid in expired:
        state.pop(cid, None)


def _load_p2_pending_state() -> dict[int, dict]:
    if not os.path.exists(P2_PENDING_PATH):
        return {}
    try:
        with open(P2_PENDING_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            out: dict[int, dict] = {}
            for k, v in data.items():
                if isinstance(v, dict):
                    try:
                        out[int(k)] = v
                    except Exception:
                        continue
            return out
    except Exception as exc:
        print(f"p2_pending_load_error path={P2_PENDING_PATH} err={str(exc)[:200]}")
        return {}
    return {}


def _save_p2_pending_state(state: dict[int, dict]) -> None:
    tmp_path = f"{P2_PENDING_PATH}.tmp"
    os.makedirs(os.path.dirname(P2_PENDING_PATH), exist_ok=True)
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)
    os.replace(tmp_path, P2_PENDING_PATH)


def _prune_p2_pending_state(state: dict[int, dict], now_ts: float) -> None:
    expired = []
    for cid, st in state.items():
        try:
            if float((st or {}).get("expires_at", 0)) <= now_ts:
                expired.append(cid)
        except Exception:
            expired.append(cid)
    for cid in expired:
        state.pop(cid, None)
    if expired:
        _save_p2_pending_state(state)


def _drain_updates() -> int:
    req_mod = _require_requests()
    resp = req_mod.get(_api_url("getUpdates"), params={"timeout": 0}, timeout=TG_HTTP_TIMEOUT)
    resp.raise_for_status()
    data = _safe_json(resp)
    if not data or not data.get("ok"):
        return 0
    updates = data.get("result", [])
    if not updates:
        return 0
    last_update_id = max(u.get("update_id", 0) for u in updates)
    return int(last_update_id + 1) if last_update_id else 0

def _fetch_stats() -> dict | None:
    req_mod = _require_requests()
    resp = req_mod.get(f"{ORGANIZER_API_URL}/stats", timeout=TG_HTTP_TIMEOUT)
    resp.raise_for_status()
    data = _safe_json(resp)
    return data if isinstance(data, dict) else None


def _api_get(path: str, params: dict | None = None) -> dict | None:
    req_mod = _require_requests()
    resp = req_mod.get(f"{ORGANIZER_API_URL}{path}", params=params or {}, timeout=TG_HTTP_TIMEOUT)
    resp.raise_for_status()
    data = _safe_json(resp)
    return data if isinstance(data, dict) else None


def _api_post(path: str, payload: dict) -> dict | None:
    req_mod = _require_requests()
    resp = req_mod.post(f"{ORGANIZER_API_URL}{path}", json=payload, timeout=TG_HTTP_TIMEOUT)
    resp.raise_for_status()
    data = _safe_json(resp)
    return data if isinstance(data, dict) else None


def _api_answer_callback(callback_query_id: str, text: str = "") -> None:
    if not callback_query_id:
        return
    payload: dict[str, object] = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
        payload["show_alert"] = False
    try:
        req_mod = _require_requests()
        resp = req_mod.post(_api_url("answerCallbackQuery"), json=payload, timeout=TG_HTTP_TIMEOUT)
        resp.raise_for_status()
    except Exception:
        # callback ack failure must not crash bot loop
        return


def _clear_clarify(chat_id: int, clarify_state: dict[int, dict]) -> None:
    if chat_id in clarify_state:
        clarify_state.pop(chat_id, None)
        _save_clarify_state(clarify_state)


def _format_pending(items: list[dict]) -> str:
    if not items:
        return "Нет встреч для уточнения."
    lines = ["Встречи для уточнения:"]
    for it in items[:20]:
        sid = it.get("id")
        title = (it.get("title") or "").strip()
        sa = it.get("start_at") or "-"
        lines.append(f"#{sid} {sa} — {title}")
    lines.append("")
    lines.append("Команда: /set #ID DD.MM HH:MM [мин]")
    lines.append("Примеры: /set #16 завтра 21:00   |   /set 16 02.02 11:30 45")
    return "\n".join(lines)


def _get_item_start_date(item_id: int, tz: tzinfo) -> date | None:
    try:
        with _db_connect() as conn:
            row = conn.execute("SELECT start_at FROM items WHERE id=?", (int(item_id),)).fetchone()
        if not row:
            return None
        start_at = row[0]
        if not start_at:
            return None
        dt = datetime.fromisoformat(start_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        return dt.date()
    except Exception:
        return None


def _is_ambiguous_time(text: str, hh: int) -> bool:
    if hh < 1 or hh > 12:
        return False
    t = text.lower()
    # explicit disambiguation words -> not ambiguous
    if re.search(r"\b(утр\w*|вечер\w*|дн[яе]|ноч\w*)\b", t):
        return False
    return True


def _parse_set_command(text: str) -> tuple:
    """
    Parse: /set #ID [date_token] HH[:MM] [duration_min]
    date_token: сегодня|завтра|послезавтра|DD.MM[.YYYY]
    """
    parts = text.strip().split()
    if len(parts) < 3:
        raise ValueError("usage: /set #ID [date] HH:MM [min]")

    # id
    raw_id = parts[1].lstrip("#")
    if not raw_id.isdigit():
        raise ValueError("bad id")
    item_id = int(raw_id)

    # detect where time token is
    # Accept HH or HH:MM, with optional "в"
    def _is_time(tok: str) -> bool:
        return bool(re.fullmatch(r"\d{1,2}(:\d{2})?", tok))

    date_tok = None
    time_tok = None
    dur_tok = None

    # patterns:
    # /set #16 21:00
    # /set #16 завтра 21:00
    # /set #16 02.02 21:00 45
    idx = 2
    if idx < len(parts) and parts[idx] == "в":
        idx += 1
    if _is_time(parts[idx]):
        time_tok = parts[idx]
        if len(parts) >= idx + 2:
            dur_tok = parts[idx + 1]
    else:
        date_tok = parts[idx]
        if len(parts) < idx + 2:
            raise ValueError("missing time")
        if parts[idx + 1] == "в" and len(parts) >= idx + 3:
            idx += 1
        if not _is_time(parts[idx + 1]):
            raise ValueError("missing time")
        time_tok = parts[idx + 1]
        if len(parts) >= idx + 3:
            dur_tok = parts[idx + 2]

    # duration
    duration = DEFAULT_MEETING_MINUTES
    if dur_tok is not None:
        if not dur_tok.isdigit():
            raise ValueError("bad duration")
        duration = int(dur_tok)

    # date
    tz = _tz_local()
    now = datetime.now(tz)
    target_date = now.date()
    if date_tok:
        dtok = date_tok.lower()
        if dtok == "сегодня":
            target_date = now.date()
        elif dtok == "завтра":
            target_date = now.date() + timedelta(days=1)
        elif dtok == "послезавтра":
            target_date = now.date() + timedelta(days=2)
        else:
            m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?", dtok)
            if not m:
                raise ValueError("bad date token")
            d = int(m.group(1))
            mo = int(m.group(2))
            y = int(m.group(3)) if m.group(3) else now.year
            if y < 100:
                y += 2000
            # clamp day
            if mo < 1 or mo > 12:
                raise ValueError("bad month")
            # last day of month
            if mo == 12:
                nm = date(y + 1, 1, 1)
            else:
                nm = date(y, mo + 1, 1)
            last = (nm - timedelta(days=1)).day
            d = max(1, min(d, last))
            target_date = date(y, mo, d)
    else:
        start_date = _get_item_start_date(item_id, tz)
        if start_date:
            target_date = start_date

    # time
    hh_mm = time_tok.split(":")
    hh = int(hh_mm[0])
    mm = int(hh_mm[1]) if len(hh_mm) == 2 else 0
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        raise ValueError("bad time")
    if _is_ambiguous_time(text, hh):
        return item_id, target_date, hh, mm, duration, True
    when = datetime(target_date.year, target_date.month, target_date.day, hh, mm, tzinfo=tz)
    return item_id, when, duration, False


def _format_latest(latest: list[dict]) -> str:
    lines = []
    for item in latest:
        event_id = (item.get("calendar_event_id") or "")
        event_short = event_id[:8] if event_id and event_id not in ("PENDING", "FAILED") else event_id
        lines.append(
            f'#{item.get("id")} {item.get("type")} {item.get("status")} '
            f'{item.get("start_at") or "-"} {event_short}'
        )
    return "\n".join(lines) if lines else "-"


def _set_pending_task(chat_id: int, parent_type: str, parent_id: int) -> None:
    _p2_pending_state[chat_id] = {
        "mode": "task",
        "parent_type": parent_type,
        "parent_id": int(parent_id),
        "expires_at": time.time() + P2_PENDING_TTL_SEC,
    }
    _save_p2_pending_state(_p2_pending_state)


def _set_pending_cycle_outcome(chat_id: int, cycle_id: int, kind: str) -> None:
    _p2_pending_state[chat_id] = {
        "mode": "cycle_outcome",
        "cycle_id": int(cycle_id),
        "kind": kind,
        "expires_at": time.time() + P2_PENDING_TTL_SEC,
    }
    _save_p2_pending_state(_p2_pending_state)


def _set_pending_task_plan(chat_id: int, task_id: int) -> None:
    _p2_pending_state[chat_id] = {
        "mode": "task_plan",
        "task_id": int(task_id),
        "expires_at": time.time() + P2_PENDING_TTL_SEC,
    }
    _save_p2_pending_state(_p2_pending_state)

def _plan_menu_keyboard(task_id: int) -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "Через 1 день (10:00)", "callback_data": f"p2:task:plan_in:{task_id}:1"},
                {"text": "Через 3 дня (10:00)", "callback_data": f"p2:task:plan_in:{task_id}:3"},
            ],
            [
                {"text": "Выбрать дату…", "callback_data": f"p2:task:plan_choose:{task_id}"},
                {"text": "Оставить без даты", "callback_data": f"p2:task:plan_none:{task_id}"},
            ],
            [
                {"text": "Назад", "callback_data": f"p2:task:plan_back:{task_id}"},
            ],
        ]
    }

def _id_desc(it: dict) -> int:
    try:
        return int(it.get("id") or 0)
    except Exception:
        return 0


def _task_is_in_inbox(task: dict) -> bool:
    if not isinstance(task, dict):
        return False
    if task.get("planned_at"):
        return False
    state = (task.get("state") or "").strip().upper()
    status = (task.get("status") or "").strip().upper()
    if state:
        return state == "NEW"
    return status == "NEW"


def _build_inbox_items(
    cycles: list[dict],
    directions: list[dict],
    projects: list[dict],
    tasks_new: list[dict],
) -> list[dict]:
    cycle_entries: list[dict] = []
    direction_entries: list[dict] = []
    project_entries: list[dict] = []
    task_entries: list[dict] = []

    for c in cycles:
        if not isinstance(c, dict):
            continue
        if (c.get("status") or "").upper() != "OPEN":
            continue
        cycle_entries.append({"kind": "cycle", **c})

    for d in directions:
        if not isinstance(d, dict):
            continue
        if (d.get("status") or "").upper() != "ACTIVE":
            continue
        direction_entries.append({"kind": "direction", **d})

    for p in projects:
        if not isinstance(p, dict):
            continue
        if (p.get("status") or "").upper() != "ACTIVE":
            continue
        project_entries.append({"kind": "project", **p})

    for it in tasks_new:
        if _task_is_in_inbox(it):
            task_entries.append({"kind": "task", **it})

    cycle_sorted = sorted(cycle_entries, key=_id_desc, reverse=True)
    direction_sorted = sorted(direction_entries, key=_id_desc, reverse=True)
    project_sorted = sorted(project_entries, key=_id_desc, reverse=True)
    tasks_sorted = sorted(task_entries, key=_id_desc, reverse=True)
    return cycle_sorted + direction_sorted + project_sorted + tasks_sorted


def _compute_plan_local(days: int) -> datetime:
    tz = _tz_local()
    now = datetime.now(tz)
    later = now + timedelta(days=int(days))
    return datetime(later.year, later.month, later.day, 10, 0, tzinfo=tz)


def _compute_plan_iso(days: int) -> str:
    when = _compute_plan_local(days)
    return when.astimezone(timezone.utc).isoformat()


def _format_local_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")


def _parse_plan_input(text: str) -> tuple[str, str]:
    dt_local = datetime.strptime(text, "%Y-%m-%d %H:%M").replace(tzinfo=_tz_local())
    return dt_local.astimezone(timezone.utc).isoformat(), _format_local_dt(dt_local)


def _state_line_from_payload(payload: dict) -> str | None:
    if not isinstance(payload, dict):
        return None
    overload = payload.get("overload") if isinstance(payload.get("overload"), dict) else None
    drift = payload.get("drift") if isinstance(payload.get("drift"), dict) else None
    if overload and overload.get("active"):
        return "Состояние: • Перегрузка"
    if drift and drift.get("active"):
        return "Состояние: • Связность"
    return None


def _nudge_is_due(next_at: str | None, now: datetime) -> bool:
    if not next_at:
        return False
    try:
        dt = datetime.fromisoformat(str(next_at))
    except Exception:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc) >= dt.astimezone(timezone.utc)


def _signals_config_keyboard(overload_enabled: int, drift_enabled: int, user_id: str) -> dict:
    overload_label = "Перегрузка: ВКЛ" if overload_enabled else "Перегрузка: ВЫКЛ"
    drift_label = "Связность: ВКЛ" if drift_enabled else "Связность: ВЫКЛ"
    return {
        "inline_keyboard": [
            [
                {
                    "text": overload_label,
                    "callback_data": f"p2:signals:toggle:overload:{user_id}",
                }
            ],
            [
                {
                    "text": drift_label,
                    "callback_data": f"p2:signals:toggle:drift:{user_id}",
                }
            ],
            [
                {
                    "text": "Сохранить",
                    "callback_data": f"p2:signals:save:{user_id}",
                },
                {
                    "text": "Отмена",
                    "callback_data": f"p2:signals:cancel:{user_id}",
                },
            ],
        ]
    }


def _signals_shortcut_keyboard(user_id: str) -> dict:
    return {
        "inline_keyboard": [
            [
                {
                    "text": "Сигналы: ВКЛ",
                    "callback_data": f"p2:signals:bulk_on:{user_id}",
                },
                {
                    "text": "Сигналы: ВЫКЛ",
                    "callback_data": f"p2:signals:bulk_off:{user_id}",
                },
            ]
        ]
    }


def _regs_collect_items(period_key: str) -> list[dict]:
    ok_regs, data_regs, status_regs, err_regs = _api_get_ex("/p4/regulations")
    if not ok_regs or not isinstance(data_regs, list):
        print(f"p4_reg_error status={status_regs} err={err_regs}")
        return []
    regs = [r for r in data_regs if (r.get("status") or "ACTIVE") == "ACTIVE"][:50]
    items: list[dict] = []
    for reg in regs:
        reg_id = reg.get("id")
        if reg_id is None:
            continue
        ok_run, data_run, status_run, err_run = _api_get_ex(
            f"/p4/regulations/{int(reg_id)}/runs", {"period": period_key}
        )
        run = None
        if ok_run and isinstance(data_run, list) and data_run:
            run = data_run[0]
        elif not ok_run:
            print(f"p4_runs_error reg_id={reg_id} status={status_run} err={err_run}")
        run_status = (run or {}).get("status")
        status_label = _regs_status_from_run(run_status)
        items.append(
            {
                "reg_id": int(reg_id),
                "run_id": int(run.get("id")) if run and run.get("id") is not None else None,
                "title": _truncate(reg.get("title") or ""),
                "day_of_month": reg.get("day_of_month"),
                "status_label": status_label,
            }
        )
    items.sort(key=_regs_sort_key)
    return items


def _regs_build_message(period_key: str, page: int, items: list[dict]) -> tuple[str, dict, int]:
    items_sorted = sorted(items, key=_regs_sort_key)
    total = len(items_sorted)
    total_pages = max(1, (total + _REGS_PAGE_SIZE - 1) // _REGS_PAGE_SIZE)
    page_use = page if 1 <= page <= total_pages else 1
    page_items = items_sorted[(page_use - 1) * _REGS_PAGE_SIZE : page_use * _REGS_PAGE_SIZE]

    lines = [f"📅 Регламенты — {_regs_month_label(period_key)}", ""]
    for it in page_items:
        label = it.get("status_label") or "DUE"
        icon = _regs_status_icon(label)
        title = it.get("title") or ""
        day = it.get("day_of_month")
        day_part = f" (до {day})" if day else ""
        lines.append(f"{icon} [{label}] {title}{day_part}")

    if not page_items:
        lines.append("Пусто.")

    lines.append("")
    lines.append("Легенда: 🔴 Не выполнен • 🟢 Выполнен • ⚠️ Пропущен")
    if total_pages > 1:
        lines.append(f"Стр. {page_use}/{total_pages}")

    kb_rows: list[list[dict]] = []
    for it in page_items:
        label = it.get("status_label") or "DUE"
        run_id = it.get("run_id")
        reg_id = it.get("reg_id")
        row: list[dict] = []
        if label == "DUE" and run_id is not None:
            row.append(
                {
                    "text": "✅ Выполнить",
                    "callback_data": f"regs:act:complete:{run_id}:{period_key}",
                }
            )
            row.append(
                {
                    "text": "⏭ Пропустить",
                    "callback_data": f"regs:act:skip:{run_id}:{period_key}",
                }
            )
        if reg_id is not None:
            row.append(
                {
                    "text": "⛔ Отключить",
                    "callback_data": f"regs:act:disable:{reg_id}:{period_key}",
                }
            )
        if row:
            kb_rows.append(row)

    prev_key = _regs_shift_period(period_key, -1) or period_key
    next_key = _regs_shift_period(period_key, 1) or period_key
    nav_row = [
        {"text": "◀️ Пред. месяц", "callback_data": f"regs:month:{prev_key}"},
        {"text": "Обновить", "callback_data": f"regs:refresh:{period_key}"},
        {"text": "След. месяц ▶️", "callback_data": f"regs:month:{next_key}"},
    ]
    kb_rows.append(nav_row)

    if total_pages > 1:
        page_row: list[dict] = []
        if page_use > 1:
            page_row.append(
                {
                    "text": "◀️ Стр.",
                    "callback_data": f"regs:page:{period_key}:p{page_use - 1}",
                }
            )
        if page_use < total_pages:
            page_row.append(
                {
                    "text": "Стр. ▶️",
                    "callback_data": f"regs:page:{period_key}:p{page_use + 1}",
                }
            )
        if page_row:
            kb_rows.append(page_row)

    return "\n".join(lines), {"inline_keyboard": kb_rows}, total_pages


def _regs_render(chat_id: int, message_id: int | None, period_key: str, page: int = 1) -> None:
    items = _regs_collect_items(period_key)
    text, keyboard, _ = _regs_build_message(period_key, page, items)
    if message_id is None:
        _send_message_with_keyboard(chat_id, text, keyboard)
        return
    _edit_message_with_keyboard(chat_id, message_id, text, keyboard)


def _default_signals_selection(overload_enabled: int, drift_enabled: int) -> tuple[int, int]:
    if int(overload_enabled or 0) == 0 and int(drift_enabled or 0) == 0:
        return 1, 0
    return int(overload_enabled or 0), int(drift_enabled or 0)


def _handle_p2_callback(data: str, chat_id: int | None) -> str:
    if not chat_id:
        return ""
    parts = data.split(":")
    if len(parts) < 2:
        return ""
    if parts[:3] == ["p2", "direction", "convert"] and len(parts) >= 4:
        direction_id = int(parts[3])
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/convert_direction_to_project",
            {"direction_id": direction_id},
        )
        if ok and payload:
            _send_message(chat_id, f"✅ Проект создан: #{payload.get('id')}")
            return "Готово"
        print(f"p2_cmd=convert_direction_to_project ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось создать проект.")
        return "Ошибка"
    if parts[:3] == ["p2", "project", "add_task"] and len(parts) >= 4:
        project_id = int(parts[3])
        _set_pending_task(chat_id, "project", project_id)
        _send_message(chat_id, f"Пришли текст задачи для проекта #{project_id}.")
        return "Ок"
    if parts[:3] == ["p2", "cycle", "add_task"] and len(parts) >= 4:
        cycle_id = int(parts[3])
        _set_pending_task(chat_id, "cycle", cycle_id)
        _send_message(chat_id, f"Пришли текст задачи для цикла #{cycle_id}.")
        return "Ок"
    if parts[:3] == ["p2", "cycle", "add_goal"] and len(parts) >= 4:
        cycle_id = int(parts[3])
        _p2_pending_state[chat_id] = {
            "mode": "cycle_goal",
            "cycle_id": cycle_id,
            "count": 0,
            "expires_at": time.time() + P2_PENDING_TTL_SEC,
        }
        _save_p2_pending_state(_p2_pending_state)
        _send_message(chat_id, "Цель 1/3:")
        return "Ок"
    if parts[:3] == ["p2", "goal", "continue"] and len(parts) >= 5:
        goal_id = int(parts[3])
        cycle_id = int(parts[4])
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/continue_cycle_goal",
            {"goal_id": goal_id, "target_cycle_id": cycle_id},
        )
        if ok and payload:
            _send_message(chat_id, f"✅ Цель продлена в цикл #{cycle_id}")
            return "Готово"
        print(f"p2_cmd=continue_cycle_goal ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось продлить цель.")
        return "Ошибка"
    if parts[:3] == ["p2", "goal", "update"] and len(parts) >= 5:
        goal_id = int(parts[3])
        status_val = parts[4]
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/update_cycle_goal_status",
            {"goal_id": goal_id, "status": status_val},
        )
        if ok and payload:
            _send_message(chat_id, "✅ Обновил цель.")
            return "Готово"
        print(f"p2_cmd=update_cycle_goal_status ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось обновить цель.")
        return "Ошибка"
    if parts[:3] == ["p2", "cycle", "add_project"] and len(parts) >= 4:
        _p2_pending_state[chat_id] = {
            "mode": "project_create",
            "cycle_id": int(parts[3]),
            "expires_at": time.time() + P2_PENDING_TTL_SEC,
        }
        _save_p2_pending_state(_p2_pending_state)
        _send_message(chat_id, "Пришли название проекта.")
        return "Ок"
    if parts[:3] == ["p2", "cycle", "close"] and len(parts) >= 4:
        cycle_id = int(parts[3])
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/close_cycle",
            {"cycle_id": cycle_id, "status": "DONE"},
        )
        if ok and payload:
            _send_message(chat_id, f"✅ Цикл закрыт: #{payload.get('id')}")
            return "Готово"
        print(f"p2_cmd=close_cycle ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось закрыть цикл.")
        return "Ошибка"
    if parts[:3] == ["p2", "cycle", "start"] and len(parts) >= 4:
        cycle_type = parts[3]
        now = datetime.now(_tz_local())
        if cycle_type == "MONTHLY":
            period_key = now.strftime("%Y-%m")
        elif cycle_type == "QUARTERLY":
            q = (now.month - 1) // 3 + 1
            period_key = f"{now.year}-Q{q}"
        else:
            return "Ошибка"
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/start_cycle",
            {"type": cycle_type, "period_key": period_key},
        )
        if ok and payload:
            cycle_id_raw = payload.get("id")
            if cycle_id_raw is None:
                print("p2_cmd=start_cycle missing cycle_id")
                _send_message(chat_id, "⚠️ Не удалось начать цикл.")
                return "Ошибка"
            try:
                cycle_id = int(cycle_id_raw)
            except Exception:
                print("p2_cmd=start_cycle missing cycle_id")
                _send_message(chat_id, "⚠️ Не удалось начать цикл.")
                return "Ошибка"
            _send_message(chat_id, f"✅ Цикл начат: #{cycle_id} ({period_key})")
            ok_prev, data_prev, status_prev, err_prev = _api_get_ex(
                f"/p2/cycles/{cycle_id}/previous_goals"
            )
            if ok_prev and isinstance(data_prev, list) and data_prev:
                for g in data_prev:
                    gid = g.get("id")
                    text = _truncate(g.get("text") or "")
                    _send_message_with_keyboard(
                        chat_id,
                        f"Прошлая цель #{gid}: {text}",
                        {
                            "inline_keyboard": [
                                [
                                    {
                                        "text": "Продлить",
                                        "callback_data": f"p2:goal:continue:{gid}:{cycle_id}",
                                    },
                                    {
                                        "text": "Достигнута",
                                        "callback_data": f"p2:goal:update:{gid}:ACHIEVED",
                                    },
                                    {
                                        "text": "Снять",
                                        "callback_data": f"p2:goal:update:{gid}:DROPPED",
                                    },
                                ]
                            ]
                        },
                    )
            _p2_pending_state[chat_id] = {
                "mode": "cycle_goal",
                "cycle_id": cycle_id,
                "count": 0,
                "expires_at": time.time() + P2_PENDING_TTL_SEC,
            }
            _save_p2_pending_state(_p2_pending_state)
            _send_message(chat_id, "Цель 1/3:")
            return "Готово"
        print(f"p2_cmd=start_cycle ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось начать цикл.")
        return "Ошибка"
    if parts[:3] == ["p2", "task", "plan"] and len(parts) >= 4:
        task_id = int(parts[3])
        _send_message_with_keyboard(chat_id, f"Выбери срок для task #{task_id}:", _plan_menu_keyboard(task_id))
        return "Ок"
    if parts[:3] == ["p2", "task", "plan_in"] and len(parts) >= 5:
        task_id = int(parts[3])
        days = int(parts[4])
        planned_local = _format_local_dt(_compute_plan_local(days))
        planned_iso = _compute_plan_iso(days)
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/plan_task",
            {"task_id": task_id, "planned_at": planned_iso},
        )
        if ok and payload:
            _send_message(chat_id, f"Ок, запланировал на {planned_local}.")
            return "Готово"
        print(f"p2_cmd=plan_task_in ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось запланировать.")
        return "Ошибка"
    if parts[:3] == ["p2", "task", "plan_choose"] and len(parts) >= 4:
        task_id = int(parts[3])
        _set_pending_task_plan(chat_id, task_id)
        _send_message(chat_id, f"Пришли дату и время для task #{task_id} в формате YYYY-MM-DD HH:MM.")
        return "Ок"
    if parts[:3] == ["p2", "task", "plan_none"] and len(parts) >= 4:
        _send_message(chat_id, "Ок, оставил без даты.")
        return "Ок"
    if parts[:3] == ["p2", "task", "plan_later"] and len(parts) >= 4:
        task_id = int(parts[3])
        planned_local = _format_local_dt(_compute_plan_local(7))
        planned_iso = _compute_plan_iso(7)
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/plan_task",
            {"task_id": task_id, "planned_at": planned_iso},
        )
        if ok and payload:
            _send_message(chat_id, f"Ок, запланировал на {planned_local}.")
            return "Готово"
        print(f"p2_cmd=plan_task_later ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось запланировать.")
        return "Ошибка"
    if parts[:3] == ["p2", "task", "plan_back"] and len(parts) >= 4:
        _send_message(chat_id, "Ок.")
        return "Ок"
    if parts[:3] == ["p2", "signals", "enable"] and len(parts) >= 4:
        user_id = parts[3]
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/set_modules_enabled_bulk",
            {"user_id": user_id, "overload_enabled": 1, "drift_enabled": 1},
        )
        if ok and payload:
            _send_message(chat_id, "Сигналы управления включены.")
            return "Готово"
        print(f"p2_cmd=set_modules_enabled_bulk ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось включить.")
        return "Ошибка"
    if parts[:3] == ["p2", "signals", "disable"] and len(parts) >= 4:
        user_id = parts[3]
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/set_modules_enabled_bulk",
            {"user_id": user_id, "overload_enabled": 0, "drift_enabled": 0},
        )
        if ok and payload:
            _send_message(chat_id, "Сигналы управления выключены.")
            return "Готово"
        print(f"p2_cmd=set_modules_enabled_bulk ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось выключить.")
        return "Ошибка"
    if parts[:3] == ["p2", "signals", "snooze"] and len(parts) >= 4:
        user_id = parts[3]
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/snooze_nudge",
            {"user_id": user_id, "nudge_key": NUDGE_SIGNALS_KEY, "days": 90},
        )
        if ok and payload:
            _send_message(chat_id, "Ок, напомню позже.")
            return "Готово"
        print(f"p2_cmd=snooze_nudge ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось отложить.")
        return "Ошибка"
    if parts[:3] == ["p2", "signals", "configure"] and len(parts) >= 4:
        user_id = parts[3]
        ok_set, data_set, status_set, err_set = _api_get_ex(
            "/p2/user_settings", {"user_id": str(user_id)}
        )
        if not ok_set or not isinstance(data_set, dict):
            _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
            return "Ошибка"
        overload_enabled = int((data_set or {}).get("overload_enabled") or 0)
        drift_enabled = int((data_set or {}).get("drift_enabled") or 0)
        overload_enabled, drift_enabled = _default_signals_selection(overload_enabled, drift_enabled)
        _p2_pending_state[chat_id] = {
            "mode": "signals_config",
            "user_id": str(user_id),
            "overload_enabled": overload_enabled,
            "drift_enabled": drift_enabled,
            "expires_at": time.time() + P2_PENDING_TTL_SEC,
        }
        _save_p2_pending_state(_p2_pending_state)
        _send_message_with_keyboard(
            chat_id,
            "Настройки сигналов:",
            _signals_config_keyboard(overload_enabled, drift_enabled, str(user_id)),
        )
        return "Ок"
    if parts[:3] == ["p2", "signals", "toggle"] and len(parts) >= 5:
        module = parts[3]
        user_id = parts[4]
        st = _p2_pending_state.get(chat_id) or {}
        if st.get("mode") != "signals_config" or st.get("user_id") != str(user_id):
            return "Ок"
        if module == "overload":
            st["overload_enabled"] = 0 if int(st.get("overload_enabled") or 0) else 1
        elif module == "drift":
            st["drift_enabled"] = 0 if int(st.get("drift_enabled") or 0) else 1
        st["expires_at"] = time.time() + P2_PENDING_TTL_SEC
        _p2_pending_state[chat_id] = st
        _save_p2_pending_state(_p2_pending_state)
        _send_message_with_keyboard(
            chat_id,
            "Настройки сигналов:",
            _signals_config_keyboard(int(st.get("overload_enabled") or 0), int(st.get("drift_enabled") or 0), user_id),
        )
        return "Ок"
    if parts[:3] == ["p2", "signals", "save"] and len(parts) >= 4:
        user_id = parts[3]
        st = _p2_pending_state.get(chat_id) or {}
        if st.get("mode") != "signals_config" or st.get("user_id") != str(user_id):
            return "Ок"
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/set_modules_enabled_bulk",
            {
                "user_id": user_id,
                "overload_enabled": int(st.get("overload_enabled") or 0),
                "drift_enabled": int(st.get("drift_enabled") or 0),
            },
        )
        if ok and payload:
            _p2_pending_state.pop(chat_id, None)
            _save_p2_pending_state(_p2_pending_state)
            _send_message(chat_id, "Настройки сохранены.")
            return "Готово"
        print(f"p2_cmd=set_modules_enabled_bulk ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось сохранить.")
        return "Ошибка"
    if parts[:3] == ["p2", "signals", "cancel"] and len(parts) >= 4:
        _p2_pending_state.pop(chat_id, None)
        _save_p2_pending_state(_p2_pending_state)
        _send_message(chat_id, "Ок.")
        return "Ок"
    if parts[:3] == ["p2", "signals", "toggle_direct"] and len(parts) >= 5:
        module = parts[3]
        user_id = parts[4]
        ok_set, data_set, status_set, err_set = _api_get_ex(
            "/p2/user_settings", {"user_id": str(user_id)}
        )
        if not ok_set or not isinstance(data_set, dict):
            _send_message(chat_id, "⚠️ Команда не выполнена. Повтори позже.")
            return "Ошибка"
        if module == "overload":
            enabled = 0 if int((data_set or {}).get("overload_enabled") or 0) else 1
        elif module == "drift":
            enabled = 0 if int((data_set or {}).get("drift_enabled") or 0) else 1
        else:
            return "Ошибка"
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/set_module_enabled",
            {"user_id": user_id, "module": module, "enabled": enabled},
        )
        if ok and payload:
            _send_message(chat_id, "Ок.")
            return "Готово"
        print(f"p2_cmd=set_module_enabled ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось переключить.")
        return "Ошибка"
    if parts[:3] == ["p2", "signals", "bulk_on"] and len(parts) >= 4:
        user_id = parts[3]
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/set_modules_enabled_bulk",
            {"user_id": user_id, "overload_enabled": 1, "drift_enabled": 0},
        )
        if ok and payload:
            _send_message(chat_id, "Сигналы управления включены (перегрузка).")
            return "Готово"
        print(f"p2_cmd=set_modules_enabled_bulk ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось включить.")
        return "Ошибка"
    if parts[:3] == ["p2", "signals", "bulk_off"] and len(parts) >= 4:
        user_id = parts[3]
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/set_modules_enabled_bulk",
            {"user_id": user_id, "overload_enabled": 0, "drift_enabled": 0},
        )
        if ok and payload:
            _send_message(chat_id, "Сигналы управления выключены.")
            return "Готово"
        print(f"p2_cmd=set_modules_enabled_bulk ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось выключить.")
        return "Ошибка"
    if parts[:3] == ["p2", "task", "leave_unplanned"] and len(parts) >= 4:
        _send_message(chat_id, "Ок, оставил без даты.")
        return "Ок"
    return ""


def _handle_p4_callback(data: str, chat_id: int | None) -> str:
    if not chat_id:
        return ""
    parts = data.split(":")
    if len(parts) < 2:
        return ""
    if parts[:3] == ["p4", "reg", "ensure"] and len(parts) >= 4:
        period_key = parts[3]
        ok, payload, status, err_text = _worker_post_ex(
            "/p4/commands/ensure_regulation_runs",
            {"user_id": str(chat_id), "period_key": period_key},
        )
        if ok and payload:
            _send_message(chat_id, f"Ок, запустил регламенты за {period_key}.")
            return "Готово"
        print(f"p4_cmd=ensure_regulation_runs ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось запустить регламенты.")
        return "Ошибка"
    if parts[:3] == ["p4", "run", "done"] and len(parts) >= 4:
        run_id = int(parts[3])
        ok, payload, status, err_text = _worker_post_ex(
            "/p4/commands/mark_regulation_done",
            {"run_id": run_id},
        )
        if ok and payload:
            _send_message(chat_id, f"✅ Отмечено: run #{run_id}")
            return "Готово"
        print(f"p4_cmd=mark_regulation_done ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось отметить.")
        return "Ошибка"
    return ""


def _handle_regs_callback(data: str, chat_id: int | None, message_id: int | None) -> str:
    if not chat_id:
        return ""
    if len(data or "") > 64:
        return "Некорректная команда"
    parts = data.split(":")
    if len(parts) < 2:
        return "Некорректная команда"
    if parts[:2] == ["regs", "month"] and len(parts) == 3:
        period_key = parts[2]
        if not _regs_parse_period_key(period_key):
            return "Некорректная команда"
        _regs_render(chat_id, message_id, period_key, page=1)
        return "Ок"
    if parts[:2] == ["regs", "refresh"] and len(parts) == 3:
        period_key = parts[2]
        if not _regs_parse_period_key(period_key):
            return "Некорректная команда"
        _regs_render(chat_id, message_id, period_key, page=1)
        return "Ок"
    if parts[:2] == ["regs", "page"] and len(parts) == 4:
        period_key = parts[2]
        if not _regs_parse_period_key(period_key):
            return "Некорректная команда"
        m = re.fullmatch(r"p(\d+)", parts[3])
        if not m:
            return "Некорректная команда"
        page = int(m.group(1))
        _regs_render(chat_id, message_id, period_key, page=page)
        return "Ок"
    if parts[:3] == ["regs", "act", "complete"] and len(parts) == 5:
        run_id = int(parts[3])
        period_key = parts[4]
        if not _regs_parse_period_key(period_key):
            return "Некорректная команда"
        msg_id_use = int(message_id or 0)
        ok, payload, status, err_text = _worker_post_ex(
            "/p4/commands/complete_reg_run",
            {
                "run_id": run_id,
                "source_msg_id": _regs_source_msg_id(chat_id, msg_id_use, "complete"),
            },
        )
        if ok and payload:
            _regs_render(chat_id, message_id, period_key, page=1)
            return "Готово"
        print(f"p4_cmd=complete_reg_run ok=0 status={status} err={err_text}")
        return "Ошибка"
    if parts[:3] == ["regs", "act", "skip"] and len(parts) == 5:
        run_id = int(parts[3])
        period_key = parts[4]
        if not _regs_parse_period_key(period_key):
            return "Некорректная команда"
        msg_id_use = int(message_id or 0)
        ok, payload, status, err_text = _worker_post_ex(
            "/p4/commands/skip_reg_run",
            {
                "run_id": run_id,
                "source_msg_id": _regs_source_msg_id(chat_id, msg_id_use, "skip"),
            },
        )
        if ok and payload:
            _regs_render(chat_id, message_id, period_key, page=1)
            return "Пропущено"
        print(f"p4_cmd=skip_reg_run ok=0 status={status} err={err_text}")
        return "Ошибка"
    if parts[:3] == ["regs", "act", "disable"] and len(parts) == 5:
        reg_id = int(parts[3])
        period_key = parts[4]
        if not _regs_parse_period_key(period_key):
            return "Некорректная команда"
        msg_id_use = int(message_id or 0)
        ok, payload, status, err_text = _worker_post_ex(
            "/p4/commands/disable_reg",
            {
                "regulation_id": reg_id,
                "source_msg_id": _regs_source_msg_id(chat_id, msg_id_use, "disable"),
            },
        )
        if ok and payload:
            _regs_render(chat_id, message_id, period_key, page=1)
            return "Отключено"
        print(f"p4_cmd=disable_reg ok=0 status={status} err={err_text}")
        return "Ошибка"
    return "Некорректная команда"


def _handle_today_callback(data: str, chat_id: int | None, message_id: int | None) -> str:
    if not chat_id:
        return ""
    if len(data or "") > 64:
        return "Некорректная команда"
    parts = data.split(":")
    if parts[:2] == ["today", "refresh"] and len(parts) == 2:
        _today_render(chat_id, message_id, page=1)
        return "Ок"
    if parts[:3] == ["today", "block", "page"] and len(parts) == 4:
        if not _p7_enabled():
            return "Ок"
        m = re.fullmatch(r"p(\d+)", parts[3])
        if not m:
            return "Некорректная команда"
        page = int(m.group(1))
        _today_render(chat_id, message_id, page=page)
        return "Ок"
    if parts[:3] == ["today", "block", "move"] and len(parts) == 5:
        if not _p7_enabled():
            return "Ок"
        block_id = int(parts[3])
        delta = int(parts[4])
        if delta not in {-10, 10}:
            return "Некорректная команда"
        msg_id_use = int(message_id or 0)
        ok, payload, status, err_text = _worker_post_ex(
            "/p7/commands/move_block",
            {
                "block_id": block_id,
                "delta_minutes": delta,
                "source_msg_id": _today_source_msg_id(chat_id, msg_id_use, "block:move"),
            },
        )
        if ok and payload:
            _today_render(chat_id, message_id, page=1)
            return "Сдвинуто"
        if status and status < 500 and err_text and "overlap" in err_text.lower():
            return "Нельзя: пересечение"
        return "Ошибка"
    if parts[:3] == ["today", "block", "del"] and len(parts) == 4:
        if not _p7_enabled():
            return "Ок"
        block_id = int(parts[3])
        msg_id_use = int(message_id or 0)
        ok, payload, status, err_text = _worker_post_ex(
            "/p7/commands/delete_block",
            {
                "block_id": block_id,
                "source_msg_id": _today_source_msg_id(chat_id, msg_id_use, "block:del"),
            },
        )
        if ok and payload:
            _today_render(chat_id, message_id, page=1)
            return "Удалено"
        if status and status < 500 and err_text and "overlap" in err_text.lower():
            return "Нельзя: пересечение"
        return "Ошибка"
    if parts[:3] == ["today", "task", "done"] and len(parts) == 4:
        task_id = int(parts[3])
        msg_id_use = int(message_id or 0)
        ok, payload, status, err_text = _worker_post_ex(
            "/p2/commands/complete_task",
            {
                "task_id": task_id,
                "source_msg_id": _today_source_msg_id(chat_id, msg_id_use, "task:done"),
            },
        )
        if ok and payload:
            _today_render(chat_id, message_id, page=1)
            return "Готово"
        print(f"p2_cmd=complete_task_today ok=0 status={status} err={err_text}")
        return "Ошибка"
    if parts[:3] == ["today", "time", "open"] and len(parts) == 4:
        if not _p7_enabled():
            return "Ок"
        task_id = int(parts[3])
        text = f"📍 Выделить время для задачи #{task_id}\nВыбери длительность:"
        keyboard = _today_time_menu_keyboard(task_id)
        if message_id is None:
            _send_message_with_keyboard(chat_id, text, keyboard)
        else:
            _edit_message_with_keyboard(chat_id, message_id, text, keyboard)
        return "Ок"
    if parts[:3] == ["today", "time", "add"] and len(parts) == 5:
        if not _p7_enabled():
            return "Ок"
        task_id = int(parts[3])
        minutes = int(parts[4])
        now_local = datetime.now(_tz_local())
        start_local = _round_up_minutes(now_local, 10)
        start_utc = start_local.astimezone(timezone.utc)
        end_utc = start_utc + timedelta(minutes=minutes)
        msg_id_use = int(message_id or 0)
        ok, payload, status, err_text = _worker_post_ex(
            "/p7/commands/add_block",
            {
                "task_id": task_id,
                "start_at": start_utc.isoformat(),
                "end_at": end_utc.isoformat(),
                "source_msg_id": _today_source_msg_id(chat_id, msg_id_use, f"time:add:{minutes}"),
            },
        )
        if ok and payload:
            start_local = _format_local_hhmm(start_utc)
            end_local = _format_local_hhmm(end_utc)
            _send_message(chat_id, f"✅ Время выделено: {start_local}–{end_local}")
            _today_render(chat_id, message_id, page=1)
            return "Готово"
        print(f"p7_cmd=add_block ok=0 status={status} err={err_text}")
        _send_message(chat_id, "⚠️ Не удалось выделить время.")
        return "Ошибка"
    if parts[:3] == ["today", "reg", "complete"] and len(parts) == 4:
        run_id = int(parts[3])
        msg_id_use = int(message_id or 0)
        ok, payload, status, err_text = _worker_post_ex(
            "/p4/commands/complete_reg_run",
            {
                "run_id": run_id,
                "source_msg_id": _today_source_msg_id(chat_id, msg_id_use, "reg:complete"),
            },
        )
        if ok and payload:
            _today_render(chat_id, message_id, page=1)
            return "Готово"
        print(f"p4_cmd=complete_reg_run_today ok=0 status={status} err={err_text}")
        return "Ошибка"
    if parts[:3] == ["today", "reg", "skip"] and len(parts) == 4:
        run_id = int(parts[3])
        msg_id_use = int(message_id or 0)
        ok, payload, status, err_text = _worker_post_ex(
            "/p4/commands/skip_reg_run",
            {
                "run_id": run_id,
                "source_msg_id": _today_source_msg_id(chat_id, msg_id_use, "reg:skip"),
            },
        )
        if ok and payload:
            _today_render(chat_id, message_id, page=1)
            return "Пропущено"
        print(f"p4_cmd=skip_reg_run_today ok=0 status={status} err={err_text}")
        return "Ошибка"
    if parts[:2] == ["today", "open_regs"] and len(parts) == 2:
        period_key = _regs_period_key(datetime.now(_tz_local()))
        _regs_render(chat_id, message_id, period_key, page=1)
        return "Ок"
    if parts[:2] == ["today", "open_list_open"] and len(parts) == 2:
        _render_list_open(chat_id)
        return "Ок"
    return "Некорректная команда"


def _handle_voice_message(update_id: int, message: dict) -> None:
    chat_id = int(message["chat"]["id"])
    voice = message.get("voice") or {}

    # Validate minimal voice payload early
    file_id = voice.get("file_id")
    if not file_id:
        _send_message(chat_id, "Не удалось принять voice (нет file_id). Отправь ещё раз.")
        return

    depth_new, depth_total = _queue_depths()
    if depth_new >= B2_QUEUE_MAX_NEW or depth_total >= B2_QUEUE_MAX_TOTAL:
        if B2_BACKPRESSURE_MODE == "reject":
            _send_message(chat_id, "Очередь перегружена. Подожди 1–2 минуты и отправь ещё раз.")
            return

    inserted, depth_new_after = _enqueue_voice(
        update_id=update_id,
        chat_id=chat_id,
        message_id=message.get("message_id"),
        voice=voice,
    )
    _p2_handle_voice(chat_id, message.get("message_id"), voice)
    if inserted:
        _send_message(chat_id, f"Принято. В очереди: {depth_new_after}.")
    else:
        _send_message(chat_id, f"Уже принято ранее. В очереди: {depth_new}.")

def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise SystemExit("TELEGRAM_BOT_TOKEN is required")

    os.makedirs("/tmp", exist_ok=True)
    _init_queue_schema()
    _start_health_server()
    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    threading.Thread(target=_daily_digest_loop, daemon=True).start()
    print("bot started")

    offset = _load_offset()
    clarify_state = _load_clarify_state()
    global _p2_pending_state
    _p2_pending_state = _load_p2_pending_state()
    print(f"loaded offset={offset}")
    if TG_DRAIN_ON_START:
        try:
            drained = _drain_updates()
            if drained > 0:
                offset = drained
                _save_offset(offset, offset - 1)
        except Exception as exc:
            print(f"drain failed: {exc}")
    backoff = 2
    last_req_err_ts = 0.0
    while True:
        try:
            backoff = 2
            updates = _get_updates(offset=offset)
            if not updates:
                continue
            for update in updates:
                update_id = update.get("update_id")
                if update_id is None:
                    continue
                if update_id < offset:
                    continue
                callback = update.get("callback_query") or {}
                if callback:
                    cb_id = callback.get("id")
                    data = (callback.get("data") or "").strip()
                    from_user = callback.get("from") or {}
                    chat = (callback.get("message") or {}).get("chat") or {}
                    chat_id_cb = chat.get("id") or from_user.get("id")
                    if data.startswith("clarify:"):
                        try:
                            parts = data.split(":")
                            # clarify:<chat_id>:<item_id>:<YYYY-MM-DD>:<hh>:<mm>:<dur>:am|pm|cancel
                            if len(parts) == 8:
                                _, chat_id_s, item_id_s, date_s, hh_s, mm_s, dur_s, ampm = parts
                                if chat_id_cb is not None and int(chat_id_s) != int(chat_id_cb):
                                    raise ValueError("chat mismatch")
                                # cancel
                                if ampm == "cancel":
                                    if chat_id_cb:
                                        _clear_clarify(int(chat_id_cb), clarify_state)
                                        _send_message(int(chat_id_cb), "Ок, отменено.")
                                    _api_answer_callback(cb_id, "Отменено")
                                    handled_kind = "callback_cancel"
                                    print(f"processed update_id={update_id} kind={handled_kind}")
                                    offset = update_id + 1
                                    _save_offset(offset, update_id)
                                    continue
                                hh = int(hh_s)
                                mm = int(mm_s)
                                if ampm == "pm" and hh < 12:
                                    hh += 12
                                tz = _tz_local()
                                d = date.fromisoformat(date_s)
                                when = datetime(d.year, d.month, d.day, hh, mm, tzinfo=tz)
                                payload = {"when": when.isoformat(), "duration_min": int(dur_s)}
                                data_resp = _api_post(f"/items/{int(item_id_s)}/schedule", payload)
                                it = (data_resp or {}).get("item") or {}
                                if chat_id_cb:
                                    _send_message(
                                        int(chat_id_cb),
                                        f"Ок. Встреча #{it.get('id')} → {it.get('start_at')} ({it.get('status')}).",
                                    )
                                    _clear_clarify(int(chat_id_cb), clarify_state)
                                _api_answer_callback(cb_id, "Готово")
                        except Exception:
                            if chat_id_cb:
                                _send_message(int(chat_id_cb), "Не получилось обновить встречу, попробуйте позже")
                            _api_answer_callback(cb_id, "Ошибка")
                    elif data.startswith("p2:"):
                        try:
                            resp_text = _handle_p2_callback(data, int(chat_id_cb) if chat_id_cb else None)
                            _api_answer_callback(cb_id, resp_text or "Ок")
                        except Exception:
                            _api_answer_callback(cb_id, "Ошибка")
                    elif data.startswith("regs:"):
                        try:
                            msg = callback.get("message") or {}
                            msg_id = msg.get("message_id")
                            resp_text = _handle_regs_callback(
                                data, int(chat_id_cb) if chat_id_cb else None, int(msg_id) if msg_id else None
                            )
                            _api_answer_callback(cb_id, resp_text or "Ок")
                        except Exception:
                            _api_answer_callback(cb_id, "Ошибка")
                    elif data.startswith("today:"):
                        try:
                            msg = callback.get("message") or {}
                            msg_id = msg.get("message_id")
                            resp_text = _handle_today_callback(
                                data, int(chat_id_cb) if chat_id_cb else None, int(msg_id) if msg_id else None
                            )
                            _api_answer_callback(cb_id, resp_text or "Ок")
                        except Exception:
                            _api_answer_callback(cb_id, "Ошибка")
                    elif data.startswith("digest:"):
                        try:
                            resp_text = _handle_digest_callback(data, int(chat_id_cb) if chat_id_cb else None)
                            _api_answer_callback(cb_id, resp_text or "Ок")
                        except Exception:
                            _api_answer_callback(cb_id, "Ошибка")
                    elif data.startswith("p4:"):
                        try:
                            resp_text = _handle_p4_callback(data, int(chat_id_cb) if chat_id_cb else None)
                            _api_answer_callback(cb_id, resp_text or "Ок")
                        except Exception:
                            _api_answer_callback(cb_id, "Ошибка")
                    else:
                        _api_answer_callback(cb_id)
                    handled_kind = "callback"
                    print(f"processed update_id={update_id} kind={handled_kind}")
                    offset = update_id + 1
                    _save_offset(offset, update_id)
                    continue
                message = update.get("message") or update.get("edited_message") or {}
                if not message:
                    offset = update_id + 1
                    _save_offset(offset, update_id)
                    continue
                chat = message.get("chat") or {}
                chat_id_raw = chat.get("id")
                text_msg = (message.get("text") or "").strip()
                handled_kind = "unknown"
                if chat_id_raw is None:
                    handled_kind = "noop"
                else:
                    chat_id = int(chat_id_raw)
                    if text_msg and not text_msg.startswith("/"):
                        now_ts = time.time()
                        _prune_clarify_state(clarify_state, now_ts)
                        st = clarify_state.get(chat_id)
                        if st:
                            t = text_msg.lower()
                            hh = int(st.get("hh", 0))
                            mm = int(st.get("mm", 0))
                            if re.search(r"\bутр\w*\b", t):
                                pass
                            elif re.search(r"\bвечер\w*\b", t):
                                if hh < 12:
                                    hh += 12
                            elif re.search(r"\bдн[яе]\b", t) and 1 <= hh <= 7:
                                hh += 12
                            else:
                                _send_message(chat_id, 'Ответь "утра" или "вечера".')
                                handled_kind = "clarify_wait"
                                print(f"processed update_id={update_id} kind={handled_kind}")
                                offset = update_id + 1
                                _save_offset(offset, update_id)
                                continue
                            try:
                                d = date.fromisoformat(st.get("date", ""))
                                tz = _tz_local()
                                when = datetime(d.year, d.month, d.day, hh, mm, tzinfo=tz)
                                payload = {
                                    "when": when.isoformat(),
                                    "duration_min": int(st.get("duration", DEFAULT_MEETING_MINUTES)),
                                }
                                data = _api_post(f"/items/{int(st.get('item_id'))}/schedule", payload)
                                it = (data or {}).get("item") or {}
                                _send_message(
                                    chat_id,
                                    f"Ок. Встреча #{it.get('id')} → {it.get('start_at')} ({it.get('status')}).",
                                )
                                clarify_state.pop(chat_id, None)
                                _save_clarify_state(clarify_state)
                                handled_kind = "clarify_set"
                                print(f"processed update_id={update_id} kind={handled_kind}")
                                offset = update_id + 1
                                _save_offset(offset, update_id)
                                continue
                            except Exception:
                                _send_message(chat_id, "Не получилось обновить встречу, попробуйте позже")
                                clarify_state.pop(chat_id, None)
                                _save_clarify_state(clarify_state)
                                handled_kind = "clarify_fail"
                                print(f"processed update_id={update_id} kind={handled_kind}")
                                offset = update_id + 1
                                _save_offset(offset, update_id)
                                continue
                    if text_msg == "/start":
                        _send_message(chat_id, "Привет! Отправь voice — я создам запись.")
                        handled_kind = "start"
                    elif text_msg == "/health":
                        _send_message(chat_id, "ok")
                        handled_kind = "health"
                    elif text_msg == "/status":
                        try:
                            stats = _fetch_stats()
                            if not stats:
                                raise ValueError("bad stats")
                            counts = stats.get("counts", {})
                            pending = stats.get("pending_calendar_count", 0)
                            failed = stats.get("failed_calendar_count", 0)
                            latest = stats.get("latest", [])
                            status_text = (
                                f'Inbox: {counts.get("inbox", 0)}\n'
                                f'Active: {counts.get("active", 0)}\n'
                                f'Failed calendar: {failed}\n'
                                f'Pending calendar: {pending}\n'
                                f'Latest:\n{_format_latest(latest)}'
                            )
                            _send_message(chat_id, status_text)
                        except Exception:
                            _send_message(chat_id, "API недоступен, попробуйте позже")
                        handled_kind = "status"
                    elif text_msg == "/pending":
                        try:
                            data = _api_get("/pending_meetings", params={"limit": 20})
                            items = (data or {}).get("items", [])
                            _send_message(chat_id, _format_pending(items))
                        except Exception:
                            _send_message(chat_id, "API недоступен, попробуйте позже")
                        handled_kind = "pending"
                    elif text_msg.startswith("/set"):
                        try:
                            parsed = _parse_set_command(text_msg)
                            if parsed[-1] is True:
                                item_id, target_date, hh, mm, duration, _ = parsed
                                clarify_state[chat_id] = {
                                    "chat_id": chat_id,
                                    "item_id": int(item_id),
                                    "date": target_date.isoformat(),
                                    "hh": int(hh),
                                    "mm": int(mm),
                                    "duration": int(duration),
                                    "expires_at": time.time() + CLARIFY_TTL_SEC,
                                }
                                _save_clarify_state(clarify_state)
                                reply_markup = {
                                    "inline_keyboard": [
                                        [
                                            {
                                                "text": "Утро",
                                                "callback_data": (
                                                    f"clarify:{chat_id}:{item_id}:"
                                                    f"{target_date.isoformat()}:{hh}:{mm}:{duration}:am"
                                                ),
                                            },
                                            {
                                                "text": "Вечер",
                                                "callback_data": (
                                                    f"clarify:{chat_id}:{item_id}:"
                                                    f"{target_date.isoformat()}:{hh}:{mm}:{duration}:pm"
                                                ),
                                            },
                                        ]
                                        ,
                                        [
                                            {
                                                "text": "Отменить",
                                                "callback_data": (
                                                    f"clarify:{chat_id}:{item_id}:"
                                                    f"{target_date.isoformat()}:{hh}:{mm}:{duration}:cancel"
                                                ),
                                            }
                                        ],
                                    ]
                                }
                                req_mod = _require_requests()
                                req_mod.post(
                                    _api_url("sendMessage"),
                                    json={
                                        "chat_id": chat_id,
                                        "text": "Уточни: утро или вечер?",
                                        "reply_markup": reply_markup,
                                    },
                                    timeout=TG_HTTP_TIMEOUT,
                                ).raise_for_status()
                            else:
                                item_id, when, duration, _ = parsed
                                payload = {"when": when.isoformat(), "duration_min": duration}
                                data = _api_post(f"/items/{item_id}/schedule", payload)
                                it = (data or {}).get("item") or {}
                                _send_message(
                                    chat_id,
                                    f"Ок. Встреча #{it.get('id')} → {it.get('start_at')} ({it.get('status')})."
                                )
                        except ValueError:
                            _send_message(chat_id, "Формат: /set #ID [сегодня|завтра|послезавтра|DD.MM] [в] HH[:MM] [мин]")
                        except Exception:
                            _send_message(chat_id, "Не получилось обновить встречу, попробуйте позже")
                        handled_kind = "set"
                    elif "voice" in message:
                        _handle_voice_message(update_id, message)
                        handled_kind = "voice"
                    elif text_msg:
                        # Stage B2.1: enqueue plain text (excluding commands above)
                        _handle_text_message(update_id, message, _p2_pending_state)
                        handled_kind = "text"
                    else:
                        handled_kind = "noop"

                print(f"processed update_id={update_id} kind={handled_kind}")
                offset = update_id + 1
                _save_offset(offset, update_id)
        except _RETRY_EXCEPTIONS as exc:
            now = time.time()
            if now - last_req_err_ts >= 10:
                print(f"bot error: {exc}")
                last_req_err_ts = now
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as exc:
            now = time.time()
            if now - last_req_err_ts >= 10:
                print(f"bot error: {exc}")
                last_req_err_ts = now
            time.sleep(2)


if __name__ == "__main__":
    main()
