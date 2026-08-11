import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable


def _local_tz(offset_minutes: int | None = None) -> timezone:
    minutes = int(offset_minutes if offset_minutes is not None else os.getenv("LOCAL_TZ_OFFSET_MIN", "180"))
    return timezone(timedelta(minutes=minutes))


def as_dict(row: sqlite3.Row | dict | None) -> dict:
    if row is None:
        return {}
    return row if isinstance(row, dict) else dict(row)


def _parse_iso_datetime_to_local(
    value: Any,
    *,
    tz_offset_min: int | None = None,
) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    tz = _local_tz(tz_offset_min)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    else:
        dt = dt.astimezone(tz)
    return dt


def _parse_date_token_ymd(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
        try:
            return date.fromisoformat(raw)
        except Exception:
            return None
    parts: list[str] = []
    buf = ""
    for ch in raw:
        if ch.isdigit():
            buf += ch
            continue
        if ch in "./-" and buf:
            parts.append(buf)
            buf = ""
            continue
        return None
    if buf:
        parts.append(buf)
    if len(parts) != 3:
        return None
    try:
        day = int(parts[0])
        month = int(parts[1])
        year = int(parts[2])
        return date(year, month, day)
    except Exception:
        return None


def _parse_time_token_hhmm(value: Any) -> tuple[int, int] | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parts = raw.split(":", 1)
    try:
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) == 2 and parts[1] != "" else 0
    except Exception:
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour, minute


def _normalize_intent_for_idempotency(raw_intent: Any) -> str:
    intent = str(raw_intent or "").strip()
    if not intent:
        return ""
    from organizer_worker.handlers import INTENT_ALIAS_TO_CANON

    return str(INTENT_ALIAS_TO_CANON.get(intent, intent)).strip()


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


def _runtime_idempotency_key(
    envelope: dict,
    *,
    normalize_intent: Callable[[Any], str] | None = None,
) -> str | None:
    if not isinstance(envelope, dict):
        return None
    explicit = str(envelope.get("idempotency_key") or "").strip()
    if explicit:
        return explicit

    command = envelope.get("command")
    if not isinstance(command, dict):
        return None
    normalize = normalize_intent or _normalize_intent_for_idempotency
    intent = normalize(command.get("intent"))
    if not intent:
        return None

    entities = command.get("entities")
    entities = entities if isinstance(entities, dict) else {}

    source_channel = _source_channel_from_envelope(envelope.get("source"))
    src_msg = entities.get("source_msg_id")
    chat_id, message_id = _parse_tg_source_msg_id(src_msg)
    if chat_id is None or message_id is None:
        trace_id = str(envelope.get("trace_id") or "").strip()
        chat_id, message_id = _parse_tg_source_msg_id(trace_id)
    if (chat_id is None or message_id is None) and source_channel != "telegram":
        return None
    if chat_id is None or message_id is None:
        return None
    if source_channel != "telegram":
        source_channel = "telegram"
    return f"{source_channel}:{chat_id}:{message_id}:{intent}"


def _get_conn(db_path: str | None = None) -> sqlite3.Connection:
    path = str(db_path or os.getenv("DB_PATH", "/data/organizer.db"))
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _command_dedup_init(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS command_dedup (
            idempotency_key TEXT PRIMARY KEY,
            intent TEXT NOT NULL,
            entity_type TEXT NULL,
            entity_id TEXT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _command_dedup_get(idempotency_key: str, *, db_path: str | None = None) -> dict | None:
    key = str(idempotency_key or "").strip()
    if not key:
        return None
    with _get_conn(db_path) as conn:
        _command_dedup_init(conn)
        row = conn.execute(
            """
            SELECT idempotency_key, intent, entity_type, entity_id, created_at
            FROM command_dedup
            WHERE idempotency_key = ?
            LIMIT 1
            """,
            (key,),
        ).fetchone()
        conn.commit()
    return as_dict(row) if row is not None else None


def _command_dedup_put(
    idempotency_key: str,
    *,
    intent: str,
    entity_type: str | None,
    entity_id: str | None,
    db_path: str | None = None,
) -> None:
    key = str(idempotency_key or "").strip()
    if not key:
        return
    with _get_conn(db_path) as conn:
        _command_dedup_init(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO command_dedup
            (idempotency_key, intent, entity_type, entity_id, created_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                key,
                str(intent or "").strip(),
                (str(entity_type).strip() if entity_type else None),
                (str(entity_id).strip() if entity_id else None),
            ),
        )
        conn.commit()
