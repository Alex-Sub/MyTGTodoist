from __future__ import annotations

import os
import sqlite3
import datetime as dt
import time
import uuid
from typing import Any, Dict, List, Optional

from .buffer_adapter import BufferEvent


def _utc_iso(ts: Optional[float] = None) -> str:
    t = time.time() if ts is None else float(ts)
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


class SqliteCloudBuffer:
    """
    Dev-only cloud buffer adapter implemented with sqlite3.
    DSN form: sqlite:////abs/path.db or sqlite:///relative/path.db
    """

    def __init__(self, dsn: str) -> None:
        self.path = self._parse_path(dsn)
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)

    def _parse_path(self, dsn: str) -> str:
        s = (dsn or "").strip()
        if not s.startswith("sqlite:///"):
            raise ValueError("sqlite buffer DSN must start with sqlite:///")
        return s[len("sqlite:///") :]

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
                CREATE TABLE IF NOT EXISTS cloud_buffer_events (
                  id TEXT PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  user_id TEXT NOT NULL,
                  channel TEXT NOT NULL,
                  payload_type TEXT NOT NULL,
                  payload_ref TEXT NOT NULL,
                  transcript TEXT,
                  normalized_text TEXT,
                  status TEXT NOT NULL,
                  fail_reason TEXT,
                  delivered_at TEXT,
                  request_id TEXT NOT NULL
                )
                """
            )
            c.execute("CREATE INDEX IF NOT EXISTS idx_cbe_status_created ON cloud_buffer_events(status, created_at)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_cbe_user_created ON cloud_buffer_events(user_id, created_at)")
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_cbe_request_id ON cloud_buffer_events(request_id)")

    def insert_event(self, row: Optional[Dict[str, Any]] = None, **kwargs: Any) -> str:
        """
        Insert an event.

        Supports:
        - insert_event({...})  # canonical internal form
        - insert_event(user_id=..., channel=..., payload_type=..., payload_ref=..., request_id=...)
          (handy for tests / simple callers)
        """
        self.ensure_schema()
        if row is None:
            row = dict(kwargs)
        eid = str(row.get("id") or uuid.uuid4())
        created_at = str(row.get("created_at") or _utc_iso())
        user_id = str(row.get("user_id") or "").strip()
        channel = str(row.get("channel") or "unknown").strip()
        payload_type = str(row.get("payload_type") or "text").strip()
        payload_ref = str(row.get("payload_ref") or "").strip()
        request_id = str(row.get("request_id") or "").strip()
        if not user_id or not payload_ref or not request_id:
            raise ValueError("user_id, payload_ref and request_id are required")

        transcript = row.get("transcript")
        normalized_text = row.get("normalized_text")
        status = str(row.get("status") or "pending").strip()

        with self._conn() as c:
            c.execute("BEGIN")
            try:
                c.execute(
                    """
                    INSERT OR IGNORE INTO cloud_buffer_events(
                      id, created_at, user_id, channel, payload_type, payload_ref,
                      transcript, normalized_text, status, fail_reason, delivered_at, request_id
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        eid,
                        created_at,
                        user_id,
                        channel,
                        payload_type,
                        payload_ref,
                        transcript,
                        normalized_text,
                        status,
                        None,
                        None,
                        request_id,
                    ),
                )
                c.execute("COMMIT")
            except Exception:
                c.execute("ROLLBACK")
                raise
        return eid

    def fetch_pending(self, limit: int) -> List[BufferEvent]:
        self.ensure_schema()
        n = max(1, int(limit))
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT * FROM cloud_buffer_events
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (n,),
            ).fetchall()
        out: List[BufferEvent] = []
        for r in rows:
            out.append(BufferEvent(**dict(r)))
        return out

    def mark_delivered(self, event_id: str, delivered_at: Optional[dt.datetime] = None) -> None:
        if delivered_at is not None:
            if delivered_at.tzinfo is not None:
                delivered_at = delivered_at.astimezone(dt.timezone.utc).replace(tzinfo=None)
            delivered_iso = delivered_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            delivered_iso = _utc_iso()
        with self._conn() as c:
            c.execute(
                """
                UPDATE cloud_buffer_events
                SET status='delivered', delivered_at=?, fail_reason=NULL
                WHERE id = ? AND status != 'delivered'
                """,
                (delivered_iso, str(event_id)),
            )

    def mark_failed(self, event_id: str, reason: str) -> None:
        with self._conn() as c:
            c.execute(
                """
                UPDATE cloud_buffer_events
                SET status='failed', fail_reason=?
                WHERE id = ? AND status != 'delivered'
                """,
                (str(reason)[:400], str(event_id)),
            )

    def mark_needs_review(self, event_id: str, reason: str) -> None:
        with self._conn() as c:
            c.execute(
                """
                UPDATE cloud_buffer_events
                SET status='needs_review', fail_reason=?
                WHERE id = ? AND status != 'delivered'
                """,
                (str(reason)[:400], str(event_id)),
            )

    def cleanup(self, *, delivered_days: int, keep_last_per_user: int) -> int:
        """
        Delete delivered rows older than X days OR keep only last K per user (delivered only).
        """
        days = max(0, int(delivered_days))
        keep = max(0, int(keep_last_per_user))
        deleted = 0
        with self._conn() as c:
            c.execute("BEGIN")
            try:
                if days > 0:
                    cutoff = time.time() - days * 86400
                    cutoff_iso = _utc_iso(cutoff)
                    cur = c.execute(
                        "DELETE FROM cloud_buffer_events WHERE status='delivered' AND delivered_at IS NOT NULL AND delivered_at < ?",
                        (cutoff_iso,),
                    )
                    deleted += int(cur.rowcount or 0)

                if keep > 0:
                    # For each user, keep last K delivered by delivered_at.
                    users = [r[0] for r in c.execute("SELECT DISTINCT user_id FROM cloud_buffer_events").fetchall()]
                    for uid in users:
                        ids = [
                            r[0]
                            for r in c.execute(
                                """
                                SELECT id FROM cloud_buffer_events
                                WHERE status='delivered' AND user_id=?
                                ORDER BY delivered_at DESC
                                """,
                                (uid,),
                            ).fetchall()
                        ]
                        for drop_id in ids[keep:]:
                            cur = c.execute(
                                "DELETE FROM cloud_buffer_events WHERE id=? AND status='delivered'",
                                (drop_id,),
                            )
                            deleted += int(cur.rowcount or 0)

                c.execute("COMMIT")
            except Exception:
                c.execute("ROLLBACK")
                raise
        return deleted

    def list_all(self) -> List[Dict[str, Any]]:
        self.ensure_schema()
        with self._conn() as c:
            rows = c.execute("SELECT * FROM cloud_buffer_events ORDER BY created_at ASC").fetchall()
        return [dict(r) for r in rows]


# Backward/compat alias for tests.
SqliteBuffer = SqliteCloudBuffer
