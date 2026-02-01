import json
import os
import sqlite3
import threading
import time
import re
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timedelta, timezone, date

import requests


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ORGANIZER_API_URL = os.getenv("ORGANIZER_API_URL", "http://organizer-api:8000")
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
DEFAULT_MEETING_MINUTES = int(os.getenv("MEETING_DEFAULT_MINUTES", "30"))
TG_SEND_MAX_RETRIES = int(os.getenv("TG_SEND_MAX_RETRIES", "2"))
CLARIFY_STATE_PATH = os.getenv("CLARIFY_STATE_PATH", "/data/bot.clarify.json")
CLARIFY_TTL_SEC = int(os.getenv("CLARIFY_TTL_SEC", "180"))


def _tz_local() -> timezone:
    return timezone(timedelta(minutes=LOCAL_TZ_OFFSET_MIN))


def _api_url(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"


def _safe_json(resp: requests.Response) -> dict | None:
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

def _handle_text_message(update_id: int, message: dict) -> None:
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
            resp = requests.post(
                _api_url("sendMessage"),
                json={"chat_id": chat_id, "text": text},
                timeout=TG_HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            return
        except (requests.Timeout, requests.ConnectionError) as exc:
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
    resp = requests.get(
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


def _drain_updates() -> int:
    resp = requests.get(_api_url("getUpdates"), params={"timeout": 0}, timeout=TG_HTTP_TIMEOUT)
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
    resp = requests.get(f"{ORGANIZER_API_URL}/stats", timeout=TG_HTTP_TIMEOUT)
    resp.raise_for_status()
    data = _safe_json(resp)
    return data if isinstance(data, dict) else None


def _api_get(path: str, params: dict | None = None) -> dict | None:
    resp = requests.get(f"{ORGANIZER_API_URL}{path}", params=params or {}, timeout=TG_HTTP_TIMEOUT)
    resp.raise_for_status()
    data = _safe_json(resp)
    return data if isinstance(data, dict) else None


def _api_post(path: str, payload: dict) -> dict | None:
    resp = requests.post(f"{ORGANIZER_API_URL}{path}", json=payload, timeout=TG_HTTP_TIMEOUT)
    resp.raise_for_status()
    data = _safe_json(resp)
    return data if isinstance(data, dict) else None


def _api_answer_callback(callback_query_id: str, text: str = "") -> None:
    if not callback_query_id:
        return
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
        payload["show_alert"] = False
    try:
        resp = requests.post(_api_url("answerCallbackQuery"), json=payload, timeout=TG_HTTP_TIMEOUT)
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


def _get_item_start_date(item_id: int, tz: timezone) -> date | None:
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
    print("bot started")

    offset = _load_offset()
    clarify_state = _load_clarify_state()
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
                                requests.post(
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
                        _handle_text_message(update_id, message)
                        handled_kind = "text"
                    else:
                        handled_kind = "noop"

                print(f"processed update_id={update_id} kind={handled_kind}")
                offset = update_id + 1
                _save_offset(offset, update_id)
        except (requests.Timeout, requests.ConnectionError) as exc:
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
