import json
import logging
import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Any


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


def _runtime_sync_conflicts_init(conn: sqlite3.Connection, *, as_dict: Any) -> None:
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


def _runtime_trace_dedup_prune(conn: sqlite3.Connection, *, runtime_trace_dedup_ttl_sec: int) -> None:
    ttl = int(runtime_trace_dedup_ttl_sec)
    if ttl <= 0:
        return
    conn.execute(
        """
        DELETE FROM runtime_trace_dedup
        WHERE created_at < strftime('%Y-%m-%dT%H:%M:%fZ','now', '-' || ? || ' seconds')
        """,
        (str(ttl),),
    )


def _runtime_trace_dedup_get(trace_id: str, *, deps: dict[str, Any]) -> dict | None:
    key = str(trace_id or "").strip()
    if not key:
        return None
    with deps["get_conn"]() as conn:
        _runtime_trace_dedup_init(conn)
        _runtime_trace_dedup_prune(
            conn,
            runtime_trace_dedup_ttl_sec=deps["runtime_trace_dedup_ttl_sec"],
        )
        row = conn.execute(
            "SELECT response_json FROM runtime_trace_dedup WHERE trace_id = ? LIMIT 1",
            (key,),
        ).fetchone()
        conn.commit()
    if row is None:
        return None
    try:
        payload = json.loads(str(deps["as_dict"](row).get("response_json") or "{}"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _runtime_trace_dedup_put(trace_id: str, payload: dict, *, deps: dict[str, Any]) -> None:
    key = str(trace_id or "").strip()
    if not key or not isinstance(payload, dict):
        return
    try:
        raw = json.dumps(payload, ensure_ascii=False)
    except Exception:
        return
    with deps["get_conn"]() as conn:
        _runtime_trace_dedup_init(conn)
        _runtime_trace_dedup_prune(
            conn,
            runtime_trace_dedup_ttl_sec=deps["runtime_trace_dedup_ttl_sec"],
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO runtime_trace_dedup (trace_id, response_json, created_at)
            VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            """,
            (key, raw),
        )
        conn.commit()


def _runtime_trace_dedup_replace(trace_id: str, payload: dict, *, deps: dict[str, Any]) -> None:
    _runtime_trace_dedup_put(trace_id, payload, deps=deps)


def _runtime_trace_dedup_delete(trace_ids: list[str], *, deps: dict[str, Any]) -> int:
    keys = [str(item or "").strip() for item in trace_ids if str(item or "").strip()]
    if not keys:
        return 0
    with deps["get_conn"]() as conn:
        _runtime_trace_dedup_init(conn)
        cur = conn.executemany(
            "DELETE FROM runtime_trace_dedup WHERE trace_id = ?",
            [(key,) for key in keys],
        )
        conn.commit()
    return int(cur.rowcount or 0)


def _runtime_trace_list_rows_for_user(
    user_id: str,
    *,
    limit: int = 200,
    deps: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    uid = str(user_id or "").strip()
    if not uid:
        return []
    with deps["get_conn"]() as conn:
        _runtime_trace_dedup_init(conn)
        _runtime_trace_dedup_prune(
            conn,
            runtime_trace_dedup_ttl_sec=deps["runtime_trace_dedup_ttl_sec"],
        )
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
        raw = str(deps["as_dict"](row).get("response_json") or "")
        trace_id = str(deps["as_dict"](row).get("trace_id") or "").strip()
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


def _runtime_trace_list_all_rows(*, limit: int = 5000, deps: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    with deps["get_conn"]() as conn:
        _runtime_trace_dedup_init(conn)
        _runtime_trace_dedup_prune(
            conn,
            runtime_trace_dedup_ttl_sec=deps["runtime_trace_dedup_ttl_sec"],
        )
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
        raw = str(deps["as_dict"](row).get("response_json") or "")
        trace_id = str(deps["as_dict"](row).get("trace_id") or "").strip()
        if not raw or not trace_id:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        if isinstance(payload, dict):
            out.append((trace_id, payload))
    return out


def _parse_drift_range_bound(value: Any, *, end_of_day: bool, deps: dict[str, Any]) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed_dt = deps["parse_iso_datetime_to_local"](raw)
    if parsed_dt is not None:
        return parsed_dt
    parsed_date = deps["parse_date_token_ymd"](raw)
    if parsed_date is None:
        return None
    if end_of_day:
        return datetime(parsed_date.year, parsed_date.month, parsed_date.day, 23, 59, 59, tzinfo=deps["local_tz"]())
    return datetime(parsed_date.year, parsed_date.month, parsed_date.day, 0, 0, 0, tzinfo=deps["local_tz"]())


def _runtime_snapshot_in_range(
    snapshot: dict[str, Any],
    from_dt: datetime | None,
    to_dt: datetime | None,
    *,
    deps: dict[str, Any],
) -> bool:
    start_at = deps["parse_iso_datetime_to_local"](snapshot.get("start_at"))
    if start_at is None:
        date_raw = str(snapshot.get("start_at_date") or "").strip()
        time_raw = str(snapshot.get("start_at_time") or "").strip() or "00:00"
        date_token = deps["parse_date_token_ymd"](date_raw)
        time_token = deps["parse_time_token_hhmm"](time_raw)
        if date_token is None or time_token is None:
            return True
        start_at = datetime(
            date_token.year,
            date_token.month,
            date_token.day,
            time_token[0],
            time_token[1],
            tzinfo=deps["local_tz"](),
        )
    if from_dt is not None and start_at < from_dt:
        return False
    if to_dt is not None and start_at > to_dt:
        return False
    return True


def _runtime_sync_conflict_snapshot_from_payload(trace_id: str, payload: dict[str, Any], *, deps: dict[str, Any]) -> dict[str, Any]:
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
        parsed = deps["parse_iso_datetime_to_local"](summary["start_at"])
        if parsed is not None:
            if not summary["start_at_date"]:
                summary["start_at_date"] = parsed.date().isoformat()
            if not summary["start_at_time"]:
                summary["start_at_time"] = parsed.strftime("%H:%M")
    if summary["duration_minutes"] in (None, ""):
        summary["duration_minutes"] = deps["default_duration_min"]
    chat_id, _message_id = deps["parse_tg_source_msg_id"](summary.get("source_msg_id"))
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


def _runtime_sync_conflict_row_to_payload(row: Any, *, deps: dict[str, Any]) -> dict[str, Any]:
    item = deps["as_dict"](row)
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
    deps: dict[str, Any],
) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    entity_key = str(entity_ref or "").strip()
    event_id = str(calendar_event_id or "").strip()
    payload_json = dict(payload if isinstance(payload, dict) else {})
    payload_json["calendar_event_id"] = event_id
    payload_json["user_id"] = uid
    raw = json.dumps(payload_json, ensure_ascii=False)
    lifecycle = "created"
    with deps["get_conn"]() as conn:
        _runtime_sync_conflicts_init(conn, as_dict=deps["as_dict"])
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
            row_dict = deps["as_dict"](row)
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
    payload_out = _runtime_sync_conflict_row_to_payload(row, deps=deps)
    payload_out["lifecycle"] = lifecycle
    logging.info(
        "runtime_sync_conflict_upsert trace_id=%s flow_id=%s user_id=%s conflict_id=%s calendar_event_id=%s lifecycle=%s kind=%s source=%s",
        entity_key,
        entity_key,
        uid,
        str(payload_out.get("id") or ""),
        event_id,
        lifecycle,
        str(kind or "").strip(),
        str(source or "").strip(),
    )
    return payload_out


def _runtime_sync_conflict_get(conflict_id: str, *, deps: dict[str, Any]) -> dict[str, Any] | None:
    key = str(conflict_id or "").strip()
    if not key:
        return None
    with deps["get_conn"]() as conn:
        _runtime_sync_conflicts_init(conn, as_dict=deps["as_dict"])
        row = conn.execute("SELECT * FROM sync_conflicts WHERE id = ? LIMIT 1", (key,)).fetchone()
    if row is None:
        return None
    return _runtime_sync_conflict_row_to_payload(row, deps=deps)


def _runtime_sync_conflict_set_status(conflict_id: str, status: str, *, deps: dict[str, Any]) -> dict[str, Any] | None:
    key = str(conflict_id or "").strip()
    if not key:
        return None
    normalized = str(status or "").strip().lower() or "pending"
    resolved_at_sql = "strftime('%Y-%m-%dT%H:%M:%fZ','now')" if normalized == "resolved" else "NULL"
    with deps["get_conn"]() as conn:
        _runtime_sync_conflicts_init(conn, as_dict=deps["as_dict"])
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
    return _runtime_sync_conflict_row_to_payload(row, deps=deps)


def _runtime_sync_conflict_mark_notified(conflict_id: str, *, deps: dict[str, Any]) -> dict[str, Any] | None:
    key = str(conflict_id or "").strip()
    if not key:
        return None
    with deps["get_conn"]() as conn:
        _runtime_sync_conflicts_init(conn, as_dict=deps["as_dict"])
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
    return _runtime_sync_conflict_row_to_payload(row, deps=deps)


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


def _runtime_sync_conflict_notify_if_needed(conflict: dict[str, Any], *, deps: dict[str, Any]) -> bool:
    if not isinstance(conflict, dict):
        return False
    status = str(conflict.get("status") or "").strip().lower()
    lifecycle = str(conflict.get("lifecycle") or "").strip().lower()
    conflict_id = str(conflict.get("id") or "").strip()
    chat_id_raw = conflict.get("chat_id") or (conflict.get("payload") or {}).get("chat_id")
    chat_id = deps["to_int_or_none"](chat_id_raw)
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
    sent = deps["tg_send_message_with_keyboard"](
        int(chat_id),
        text,
        _runtime_sync_conflict_keyboard(conflict_id),
        stage="sync_conflict_notify",
    )
    if sent:
        _runtime_sync_conflict_mark_notified(conflict_id, deps=deps)
        logging.info(
            "sync_conflict_created",
            extra={"conflict_id": conflict_id, "user_id": str(conflict.get("user_id") or ""), "calendar_event_id": str(conflict.get("calendar_event_id") or "")},
        )
        return True
    return False


def _runtime_cleanup_stale_calendar_events_for_user(user_id: str, *, limit: int = 500, deps: dict[str, Any]) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    if not uid:
        return {"ok": False, "reason": "user_id_required", "conflicts": [], "checked": 0}
    rows = _runtime_trace_list_rows_for_user(uid, limit=limit, deps=deps)
    conflicts: list[dict[str, Any]] = []
    checked = 0
    for trace_id, payload in rows:
        snapshot = _runtime_sync_conflict_snapshot_from_payload(trace_id, payload, deps=deps)
        event_id = str(snapshot.get("calendar_event_id") or "").strip()
        if not event_id:
            continue
        checked += 1
        event = deps["calendar_get_event"](event_id)
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
                    deps=deps,
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
    deps: dict[str, Any],
) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    from_dt = _parse_drift_range_bound(from_value, end_of_day=False, deps=deps)
    to_dt = _parse_drift_range_bound(to_value, end_of_day=True, deps=deps)
    logging.info(
        "calendar_drift_check_started",
        extra={"user_id": uid, "from": str(from_value or ""), "to": str(to_value or ""), "notify": bool(notify)},
    )
    rows = (
        _runtime_trace_list_rows_for_user(uid, limit=limit, deps=deps)
        if uid
        else _runtime_trace_list_all_rows(limit=limit, deps=deps)
    )
    checked = 0
    missing = 0
    conflicts: list[dict[str, Any]] = []
    notified = 0
    for trace_id, payload in rows:
        snapshot = _runtime_sync_conflict_snapshot_from_payload(trace_id, payload, deps=deps)
        event_id = str(snapshot.get("calendar_event_id") or "").strip()
        snapshot_uid = str(snapshot.get("user_id") or "").strip()
        if not event_id:
            continue
        if uid and snapshot_uid != uid:
            continue
        if not _runtime_snapshot_in_range(snapshot, from_dt, to_dt, deps=deps):
            continue
        checked += 1
        event = deps["calendar_get_event"](event_id)
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
            deps=deps,
        )
        conflicts.append(conflict)
        if notify and _runtime_sync_conflict_notify_if_needed(conflict, deps=deps):
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


def _runtime_build_trace_snapshot(
    *,
    trace_id: str,
    intent: str,
    entities: dict[str, Any],
    response_payload: dict[str, Any],
    user_id: str,
    deps: dict[str, Any],
) -> dict[str, Any]:
    out = dict(response_payload if isinstance(response_payload, dict) else {})
    event_id = str(out.get("calendar_event_id") or entities.get("calendar_event_id") or "").strip()
    title = str(
        entities.get("title")
        or out.get("title")
        or entities.get("meeting_update_source_title")
        or "Встреча"
    ).strip() or "Встреча"
    start_at = str(entities.get("start_at") or out.get("start_at") or "").strip()
    start_at_date = str(entities.get("start_at_date") or out.get("start_at_date") or "").strip()
    start_at_time = str(entities.get("start_at_time") or out.get("start_at_time") or "").strip()
    if not start_at and start_at_date and start_at_time:
        try:
            start_at = datetime.fromisoformat(f"{start_at_date}T{start_at_time}").replace(
                tzinfo=deps["local_tz"]()
            ).isoformat()
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
        "duration_minutes": entities.get("duration_minutes") or out.get("duration_minutes") or deps["default_duration_min"],
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


def _runtime_trace_conflict_restore_from_snapshot(
    conflict: dict[str, Any],
    *,
    override: dict[str, Any] | None = None,
    deps: dict[str, Any],
) -> dict[str, Any]:
    payload = conflict.get("payload") if isinstance(conflict.get("payload"), dict) else {}
    snapshot = dict(payload if isinstance(payload, dict) else {})
    if isinstance(override, dict):
        snapshot.update({k: v for k, v in override.items() if v is not None})
    title = str(snapshot.get("title") or "Встреча").strip() or "Встреча"
    start_at = deps["parse_iso_datetime_to_local"](snapshot.get("start_at"))
    if start_at is None:
        date_raw = str(snapshot.get("start_at_date") or "").strip()
        time_raw = str(snapshot.get("start_at_time") or "").strip() or "10:00"
        time_token = deps["parse_time_token_hhmm"](time_raw)
        date_token = deps["parse_date_token_ymd"](date_raw)
        if date_token is None or time_token is None:
            return {"ok": False, "error": "invalid_conflict_snapshot"}
        start_at = datetime(
            date_token.year,
            date_token.month,
            date_token.day,
            time_token[0],
            time_token[1],
            tzinfo=deps["local_tz"](),
        )
    duration = int(snapshot.get("duration_minutes") or deps["default_duration_min"])
    end_at = start_at + timedelta(minutes=max(1, duration))
    comment_text = str(snapshot.get("comment_text") or "").strip() or None
    try:
        new_event_id = deps["create_event"](
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
        payload_row = _runtime_trace_dedup_get(trace_id, deps=deps) or {}
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
            deps=deps,
        )
        _runtime_trace_dedup_replace(trace_id, updated_payload, deps=deps)
    _runtime_sync_conflict_set_status(str(conflict.get("id") or ""), "resolved", deps=deps)
    return {
        "ok": True,
        "calendar_event_id": str(new_event_id),
        "user_message": "Событие восстановлено в календаре.",
        "conflict_id": str(conflict.get("id") or ""),
    }


def _runtime_trace_find_latest_calendar_event_for_user(user_id: str, *, limit: int = 200, deps: dict[str, Any]) -> str | None:
    uid = str(user_id or "").strip()
    if not uid:
        return None
    for trace_id, payload in _runtime_trace_list_rows_for_user(uid, limit=limit, deps=deps):
        snapshot = _runtime_sync_conflict_snapshot_from_payload(trace_id, payload, deps=deps)
        event_id = str(snapshot.get("calendar_event_id") or "").strip()
        if not event_id:
            continue
        event = deps["calendar_get_event"](event_id)
        if bool(event.get("ok")):
            return event_id
        if int(event.get("http_status") or 0) == 404:
            _runtime_sync_conflict_create_or_reopen(
                user_id=uid,
                source="runtime_trace_dedup",
                entity_ref=trace_id,
                calendar_event_id=event_id,
                payload=snapshot,
                deps=deps,
            )
    return None


def _runtime_trace_list_calendar_events_for_user(user_id: str, *, limit: int = 200, deps: dict[str, Any]) -> list[str]:
    uid = str(user_id or "").strip()
    if not uid:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for trace_id, payload in _runtime_trace_list_rows_for_user(uid, limit=limit, deps=deps):
        snapshot = _runtime_sync_conflict_snapshot_from_payload(trace_id, payload, deps=deps)
        event_id = str(snapshot.get("calendar_event_id") or "").strip()
        if not event_id or event_id in seen:
            continue
        event = deps["calendar_get_event"](event_id)
        if not bool(event.get("ok")):
            if int(event.get("http_status") or 0) == 404:
                _runtime_sync_conflict_create_or_reopen(
                    user_id=uid,
                    source="runtime_trace_dedup",
                    entity_ref=trace_id,
                    calendar_event_id=event_id,
                    payload=snapshot,
                    deps=deps,
                )
            continue
        seen.add(event_id)
        out.append(event_id)
    return out


def _runtime_sync_conflict_apply_action(conflict_id: str, action: str, *, deps: dict[str, Any]) -> dict[str, Any]:
    conflict = _runtime_sync_conflict_get(conflict_id, deps=deps)
    if not isinstance(conflict, dict):
        return {"ok": False, "reason": "conflict_not_found"}
    normalized_action = str(action or "").strip().lower()
    if normalized_action == "delete_db":
        trace_id = str(conflict.get("entity_ref") or "").strip()
        removed = _runtime_trace_dedup_delete([trace_id], deps=deps) if trace_id else 0
        _runtime_sync_conflict_set_status(conflict_id, "resolved", deps=deps)
        return {
            "ok": True,
            "action": "delete_db",
            "removed": removed,
            "conflict_id": conflict_id,
            "user_message": "Событие удалено из внутренней базы и больше не будет предлагаться.",
        }
    if normalized_action == "restore_calendar":
        return _runtime_trace_conflict_restore_from_snapshot(conflict, deps=deps)
    if normalized_action == "skip":
        _runtime_sync_conflict_set_status(conflict_id, "skipped", deps=deps)
        return {
            "ok": True,
            "action": "skip",
            "conflict_id": conflict_id,
            "user_message": "Хорошо, пока пропускаю этот конфликт.",
        }
    if normalized_action == "edit":
        return {"ok": True, "action": "edit", "conflict_id": conflict_id, "conflict": conflict}
    return {"ok": False, "reason": "unsupported_action"}
