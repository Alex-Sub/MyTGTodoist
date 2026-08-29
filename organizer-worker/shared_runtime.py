"""Small, durable storage primitives for the Organizer runtime ingress.

This module deliberately contains no Telegram, ASR, or queue integration.  It
only records the command identity and the domain result in the Organizer DB so
that the HTTP boundary can safely replay a request after a response loss.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TABLE_NAME = "runtime_command_dedup"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _connect(db_path: str) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Journal mode is a database-level setting.  It is initialized during
    # worker startup; changing it while concurrent request connections open
    # can itself raise ``database is locked`` before busy_timeout applies.
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def ensure_runtime_schema(db_path: str) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                idempotency_key TEXT PRIMARY KEY,
                request_hash TEXT NOT NULL,
                intent TEXT NOT NULL,
                state TEXT NOT NULL,
                response_json TEXT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def request_hash(payload: dict[str, Any]) -> str:
    """Hash only domain-relevant input, not transport-only trace metadata."""

    material = {
        "source": payload.get("source"),
        "command": payload.get("command"),
    }
    return hashlib.sha256(_json(material).encode("utf-8")).hexdigest()


def idempotency_key(payload: dict[str, Any]) -> str:
    explicit = str(payload.get("idempotency_key") or "").strip()
    if explicit:
        return explicit
    command = payload.get("command")
    entities = command.get("entities") if isinstance(command, dict) else None
    if isinstance(entities, dict):
        source_msg_id = str(entities.get("source_msg_id") or "").strip()
        if source_msg_id:
            return source_msg_id
    return str(payload.get("trace_id") or "").strip()


def begin_command(
    db_path: str,
    *,
    key: str,
    intent: str,
    payload_hash: str,
) -> dict[str, Any]:
    """Reserve a key, or return its durable prior state.

    The immediate transaction makes the reservation exclusive across worker
    threads/processes.  A second request cannot enter the domain dispatcher
    while the first request owns a processing reservation.
    """

    if not key:
        raise ValueError("idempotency_key or trace_id is required")
    ensure_runtime_schema(db_path)
    with _connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            f"SELECT idempotency_key, request_hash, intent, state, response_json "
            f"FROM {TABLE_NAME} WHERE idempotency_key = ?",
            (key,),
        ).fetchone()
        if row is None:
            now = _now()
            conn.execute(
                f"""
                INSERT INTO {TABLE_NAME}
                    (idempotency_key, request_hash, intent, state, response_json, created_at, updated_at)
                VALUES (?, ?, ?, 'processing', NULL, ?, ?)
                """,
                (key, payload_hash, intent, now, now),
            )
            conn.commit()
            return {"state": "new", "request_hash": payload_hash}
        conn.commit()
        result = dict(row)
        if result.get("response_json"):
            result["response"] = json.loads(str(result["response_json"]))
        return result


def finish_command(db_path: str, *, key: str, response: dict[str, Any]) -> None:
    ensure_runtime_schema(db_path)
    state = "succeeded" if response.get("ok") else "rejected"
    with _connect(db_path) as conn:
        conn.execute(
            f"""
            UPDATE {TABLE_NAME}
            SET state = ?, response_json = ?, updated_at = ?
            WHERE idempotency_key = ?
            """,
            (state, _json(response), _now(), key),
        )
        conn.commit()


def release_command(db_path: str, *, key: str) -> None:
    """Allow a retry after a transient failure before the domain commit."""

    with _connect(db_path) as conn:
        conn.execute(
            f"DELETE FROM {TABLE_NAME} WHERE idempotency_key = ? AND state = 'processing'",
            (key,),
        )
        conn.commit()
