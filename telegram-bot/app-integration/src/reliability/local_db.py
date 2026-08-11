from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _utc_iso(ts: Optional[float] = None) -> str:
    t = time.time() if ts is None else float(ts)
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


def _utc_ts_from_iso(value: str) -> Optional[float]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return float(dt.timestamp())
    except Exception:
        return None


class LocalMainDb:
    def __init__(self, path: str) -> None:
        self.path = str(path)
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA foreign_keys=ON;")
        return c

    def ensure_schema(self) -> None:
        with self._conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS user_events (
                  id TEXT PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  user_id TEXT NOT NULL,
                  channel TEXT NOT NULL,
                  payload_type TEXT NOT NULL,
                  payload_ref TEXT NOT NULL,
                  transcript TEXT,
                  normalized_text TEXT,
                  request_id TEXT NOT NULL
                )
                """
            )
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_user_events_request_id ON user_events(request_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_user_events_user_created ON user_events(user_id, created_at)")
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS clarification_sessions (
                  context_key TEXT PRIMARY KEY,
                  app_id TEXT,
                  tenant_id TEXT,
                  user_id TEXT NOT NULL,
                  intent TEXT NOT NULL,
                  payload_json TEXT NOT NULL,
                  missing_field TEXT NOT NULL,
                  source_message_id TEXT,
                  idempotency_key TEXT,
                  created_at TEXT NOT NULL
                )
                """
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_clarification_sessions_user_created "
                "ON clarification_sessions(user_id, created_at)"
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS command_dedup (
                  idempotency_key TEXT PRIMARY KEY,
                  intent TEXT NOT NULL,
                  entity_type TEXT,
                  entity_id TEXT,
                  status TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """
            )
            cols = {str(r["name"]) for r in c.execute("PRAGMA table_info(command_dedup)").fetchall()}
            if "status" not in cols:
                c.execute("ALTER TABLE command_dedup ADD COLUMN status TEXT NOT NULL DEFAULT 'completed'")
            if "updated_at" not in cols:
                c.execute("ALTER TABLE command_dedup ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''")
            c.execute("UPDATE command_dedup SET status='completed' WHERE status IS NULL OR TRIM(status)=''")
            c.execute("UPDATE command_dedup SET updated_at=created_at WHERE updated_at IS NULL OR TRIM(updated_at)=''")
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_command_dedup_created "
                "ON command_dedup(created_at)"
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_command_dedup_status_updated "
                "ON command_dedup(status, updated_at)"
            )

    def insert_idempotent(self, row: Dict[str, Any]) -> bool:
        """
        Returns True if inserted, False if duplicate by request_id.
        """
        self.ensure_schema()
        rid = str(row.get("request_id") or "").strip()
        if not rid:
            raise ValueError("request_id required for idempotency")
        with self._conn() as c:
            c.execute("BEGIN")
            try:
                cur = c.execute(
                    """
                    INSERT OR IGNORE INTO user_events(
                      id, created_at, user_id, channel, payload_type, payload_ref,
                      transcript, normalized_text, request_id
                    ) VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        str(row.get("id")),
                        str(row.get("created_at") or _utc_iso()),
                        str(row.get("user_id") or ""),
                        str(row.get("channel") or "unknown"),
                        str(row.get("payload_type") or "text"),
                        str(row.get("payload_ref") or ""),
                        row.get("transcript"),
                        row.get("normalized_text"),
                        rid,
                    ),
                )
                c.execute("COMMIT")
                return int(cur.rowcount or 0) > 0
            except Exception:
                c.execute("ROLLBACK")
                raise

    def count_by_request_id(self, request_id: str) -> int:
        self.ensure_schema()
        with self._conn() as c:
            row = c.execute("SELECT COUNT(1) AS n FROM user_events WHERE request_id=?", (str(request_id),)).fetchone()
            return int(row["n"] if row else 0)

    def upsert_clarification_session(
        self,
        *,
        context_key: str,
        user_id: str,
        intent: str,
        payload: Dict[str, Any],
        missing_field: str,
        app_id: str = "",
        tenant_id: str = "",
        source_message_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        created_at: Optional[str] = None,
    ) -> None:
        self.ensure_schema()
        ck = str(context_key or "").strip()
        uid = str(user_id or "").strip()
        intent_name = str(intent or "").strip()
        mf = str(missing_field or "").strip()
        if not ck:
            raise ValueError("context_key is required")
        if not uid:
            raise ValueError("user_id is required")
        if not intent_name:
            raise ValueError("intent is required")
        if not mf:
            raise ValueError("missing_field is required")

        payload_json = json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=False)
        created = str(created_at or _utc_iso()).strip() or _utc_iso()
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO clarification_sessions(
                  context_key, app_id, tenant_id, user_id, intent, payload_json,
                  missing_field, source_message_id, idempotency_key, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(context_key) DO UPDATE SET
                  app_id=excluded.app_id,
                  tenant_id=excluded.tenant_id,
                  user_id=excluded.user_id,
                  intent=excluded.intent,
                  payload_json=excluded.payload_json,
                  missing_field=excluded.missing_field,
                  source_message_id=excluded.source_message_id,
                  idempotency_key=excluded.idempotency_key,
                  created_at=excluded.created_at
                """,
                (
                    ck,
                    str(app_id or ""),
                    str(tenant_id or ""),
                    uid,
                    intent_name,
                    payload_json,
                    mf,
                    None if source_message_id is None else str(source_message_id),
                    None if idempotency_key is None else str(idempotency_key),
                    created,
                ),
            )

    def get_active_clarification_session(
        self,
        *,
        context_key: str,
        ttl_seconds: int = 300,
        now_ts: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        self.ensure_schema()
        ck = str(context_key or "").strip()
        if not ck:
            return None

        with self._conn() as c:
            row = c.execute(
                """
                SELECT context_key, app_id, tenant_id, user_id, intent, payload_json,
                       missing_field, source_message_id, idempotency_key, created_at
                FROM clarification_sessions
                WHERE context_key=?
                """,
                (ck,),
            ).fetchone()
            if row is None:
                return None

            created_ts = _utc_ts_from_iso(str(row["created_at"] or ""))
            now = float(time.time() if now_ts is None else now_ts)
            ttl = max(0, int(ttl_seconds))
            if created_ts is None or (now - created_ts) > float(ttl):
                c.execute("DELETE FROM clarification_sessions WHERE context_key=?", (ck,))
                return None

            payload: Dict[str, Any] = {}
            try:
                decoded = json.loads(str(row["payload_json"] or "{}"))
                if isinstance(decoded, dict):
                    payload = decoded
            except Exception:
                payload = {}

            return {
                "context_key": str(row["context_key"]),
                "app_id": str(row["app_id"] or ""),
                "tenant_id": str(row["tenant_id"] or ""),
                "user_id": str(row["user_id"]),
                "intent": str(row["intent"]),
                "payload": payload,
                "missing_field": str(row["missing_field"]),
                "source_message_id": row["source_message_id"],
                "idempotency_key": row["idempotency_key"],
                "created_at": str(row["created_at"]),
            }

    def delete_clarification_session(self, context_key: str) -> None:
        self.ensure_schema()
        ck = str(context_key or "").strip()
        if not ck:
            return
        with self._conn() as c:
            c.execute("DELETE FROM clarification_sessions WHERE context_key=?", (ck,))

    def bind_clarification_prompt_message(self, *, context_key: str, prompt_message_id: str) -> None:
        self.ensure_schema()
        ck = str(context_key or "").strip()
        mid = str(prompt_message_id or "").strip()
        if not ck or not mid:
            return
        with self._conn() as c:
            row = c.execute(
                "SELECT payload_json FROM clarification_sessions WHERE context_key=?",
                (ck,),
            ).fetchone()
            if row is None:
                return
            payload: Dict[str, Any] = {}
            try:
                decoded = json.loads(str(row["payload_json"] or "{}"))
                if isinstance(decoded, dict):
                    payload = decoded
            except Exception:
                payload = {}
            payload["__telegram_prompt_message_id"] = mid
            c.execute(
                """
                UPDATE clarification_sessions
                SET source_message_id=?, payload_json=?
                WHERE context_key=?
                """,
                (mid, json.dumps(payload, ensure_ascii=False), ck),
            )

    def get_command_dedup(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        self.ensure_schema()
        key = str(idempotency_key or "").strip()
        if not key:
            return None
        with self._conn() as c:
            row = c.execute(
                """
                SELECT idempotency_key, intent, entity_type, entity_id, status, created_at, updated_at
                FROM command_dedup
                WHERE idempotency_key=?
                """,
                (key,),
            ).fetchone()
            if row is None:
                return None
            return {
                "idempotency_key": str(row["idempotency_key"]),
                "intent": str(row["intent"]),
                "entity_type": str(row["entity_type"] or ""),
                "entity_id": str(row["entity_id"] or ""),
                "status": str(row["status"] or "completed"),
                "created_at": str(row["created_at"]),
                "updated_at": str(row["updated_at"] or ""),
            }

    def reserve_command_dedup(
        self,
        *,
        idempotency_key: str,
        intent: str,
        created_at: Optional[str] = None,
        updated_at: Optional[str] = None,
    ) -> bool:
        """
        Reserve idempotency key before execution.
        Returns True if reservation created, False if already exists.
        """
        self.ensure_schema()
        key = str(idempotency_key or "").strip()
        intent_name = str(intent or "").strip()
        if not key:
            raise ValueError("idempotency_key is required")
        if not intent_name:
            raise ValueError("intent is required")
        ts = str(created_at or _utc_iso()).strip() or _utc_iso()
        upd = str(updated_at or ts).strip() or ts

        with self._conn() as c:
            cur = c.execute(
                """
                INSERT OR IGNORE INTO command_dedup(
                  idempotency_key, intent, entity_type, entity_id, status, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (key, intent_name, "", "", "reserved", ts, upd),
            )
            return int(cur.rowcount or 0) > 0

    def finalize_command_dedup(
        self,
        *,
        idempotency_key: str,
        entity_type: str = "",
        entity_id: str = "",
        updated_at: Optional[str] = None,
    ) -> None:
        self.ensure_schema()
        key = str(idempotency_key or "").strip()
        if not key:
            return
        upd = str(updated_at or _utc_iso()).strip() or _utc_iso()
        with self._conn() as c:
            c.execute(
                """
                UPDATE command_dedup
                SET entity_type=?, entity_id=?, status='completed', updated_at=?
                WHERE idempotency_key=?
                """,
                (str(entity_type or ""), str(entity_id or ""), upd, key),
            )

    def reclaim_command_dedup(
        self,
        *,
        idempotency_key: str,
        intent: str,
        updated_at: Optional[str] = None,
    ) -> bool:
        self.ensure_schema()
        key = str(idempotency_key or "").strip()
        intent_name = str(intent or "").strip()
        if not key:
            return False
        if not intent_name:
            raise ValueError("intent is required")
        upd = str(updated_at or _utc_iso()).strip() or _utc_iso()
        with self._conn() as c:
            cur = c.execute(
                """
                UPDATE command_dedup
                SET intent=?, entity_type='', entity_id='', status='reserved', updated_at=?
                WHERE idempotency_key=?
                """,
                (intent_name, upd, key),
            )
            return int(cur.rowcount or 0) > 0

    def is_command_dedup_stale(
        self,
        record: Optional[Dict[str, Any]],
        *,
        ttl_seconds: int,
        now_ts: Optional[float] = None,
    ) -> bool:
        if not isinstance(record, dict):
            return True
        anchor = str(record.get("updated_at") or record.get("created_at") or "").strip()
        ts = _utc_ts_from_iso(anchor)
        if ts is None:
            return True
        ttl = max(0, int(ttl_seconds))
        now = float(time.time() if now_ts is None else now_ts)
        return (now - ts) > float(ttl)

    def delete_command_dedup(self, idempotency_key: str) -> None:
        self.ensure_schema()
        key = str(idempotency_key or "").strip()
        if not key:
            return
        with self._conn() as c:
            c.execute("DELETE FROM command_dedup WHERE idempotency_key=?", (key,))
