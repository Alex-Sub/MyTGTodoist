from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional

from .buffer_adapter import BufferEvent


def _utc_iso(ts: Optional[float] = None) -> str:
    t = time.time() if ts is None else float(ts)
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


class PostgresCloudBuffer:
    """
    Minimal Postgres adapter. Requires psycopg (v3).
    DSN: postgres://...
    """

    def __init__(self, dsn: str) -> None:
        self.dsn = str(dsn or "").strip()
        if not self.dsn.startswith("postgres"):
            raise ValueError("postgres buffer DSN must start with postgres:// or postgresql://")

        try:
            import psycopg  # type: ignore
        except Exception as e:
            raise RuntimeError("psycopg is required for PostgresCloudBuffer") from e

        self._psycopg = psycopg

    def _conn(self):
        return self._psycopg.connect(self.dsn)

    def ensure_schema(self) -> None:
        with self._conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cloud_buffer_events (
                      id uuid PRIMARY KEY,
                      created_at timestamptz NOT NULL,
                      user_id text NOT NULL,
                      channel text NOT NULL,
                      payload_type text NOT NULL,
                      payload_ref text NOT NULL,
                      transcript text,
                      normalized_text text,
                      status text NOT NULL,
                      fail_reason text,
                      delivered_at timestamptz,
                      request_id uuid NOT NULL UNIQUE
                    );
                    CREATE INDEX IF NOT EXISTS idx_cbe_status_created ON cloud_buffer_events(status, created_at);
                    CREATE INDEX IF NOT EXISTS idx_cbe_user_created ON cloud_buffer_events(user_id, created_at);
                    """
                )
            c.commit()

    def insert_event(self, row: Dict[str, Any]) -> str:
        self.ensure_schema()
        eid = str(row.get("id") or uuid.uuid4())
        user_id = str(row.get("user_id") or "").strip()
        payload_ref = str(row.get("payload_ref") or "").strip()
        request_id = str(row.get("request_id") or "").strip()
        if not user_id or not payload_ref or not request_id:
            raise ValueError("user_id, payload_ref and request_id are required")

        created_at = row.get("created_at") or _utc_iso()
        channel = str(row.get("channel") or "unknown").strip()
        payload_type = str(row.get("payload_type") or "text").strip()
        transcript = row.get("transcript")
        normalized_text = row.get("normalized_text")
        status = str(row.get("status") or "pending").strip()

        with self._conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cloud_buffer_events(
                      id, created_at, user_id, channel, payload_type, payload_ref,
                      transcript, normalized_text, status, fail_reason, delivered_at, request_id
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,NULL,%s)
                    ON CONFLICT (request_id) DO NOTHING
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
                        request_id,
                    ),
                )
            c.commit()
        return eid

    def fetch_pending(self, limit: int) -> List[BufferEvent]:
        self.ensure_schema()
        n = max(1, int(limit))
        with self._conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      id::text as id,
                      to_char(created_at at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') as created_at,
                      user_id, channel, payload_type, payload_ref,
                      transcript, normalized_text, status, fail_reason,
                      CASE WHEN delivered_at IS NULL THEN NULL ELSE to_char(delivered_at at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') END as delivered_at,
                      request_id::text as request_id
                    FROM cloud_buffer_events
                    WHERE status='pending'
                    ORDER BY created_at ASC
                    LIMIT %s
                    """,
                    (n,),
                )
                rows = cur.fetchall()
                cols = [d.name for d in cur.description]  # type: ignore
        out: List[BufferEvent] = []
        for r in rows:
            out.append(BufferEvent(**dict(zip(cols, r))))
        return out

    def mark_delivered(self, event_id: str) -> None:
        with self._conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cloud_buffer_events
                    SET status='delivered', delivered_at=now(), fail_reason=NULL
                    WHERE id=%s::uuid AND status!='delivered'
                    """,
                    (str(event_id),),
                )
            c.commit()

    def mark_failed(self, event_id: str, reason: str) -> None:
        with self._conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cloud_buffer_events
                    SET status='failed', fail_reason=%s
                    WHERE id=%s::uuid AND status!='delivered'
                    """,
                    (str(reason)[:400], str(event_id)),
                )
            c.commit()

    def mark_needs_review(self, event_id: str, reason: str) -> None:
        with self._conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cloud_buffer_events
                    SET status='needs_review', fail_reason=%s
                    WHERE id=%s::uuid AND status!='delivered'
                    """,
                    (str(reason)[:400], str(event_id)),
                )
            c.commit()

    def cleanup(self, *, delivered_days: int, keep_last_per_user: int) -> int:
        days = max(0, int(delivered_days))
        keep = max(0, int(keep_last_per_user))
        deleted = 0
        with self._conn() as c:
            with c.cursor() as cur:
                if days > 0:
                    cur.execute(
                        """
                        DELETE FROM cloud_buffer_events
                        WHERE status='delivered' AND delivered_at IS NOT NULL AND delivered_at < now() - (%s || ' days')::interval
                        """,
                        (days,),
                    )
                    deleted += int(cur.rowcount or 0)

                if keep > 0:
                    cur.execute(
                        """
                        WITH ranked AS (
                          SELECT id, user_id,
                                 ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY delivered_at DESC) AS rn
                          FROM cloud_buffer_events
                          WHERE status='delivered' AND delivered_at IS NOT NULL
                        )
                        DELETE FROM cloud_buffer_events
                        WHERE id IN (SELECT id FROM ranked WHERE rn > %s)
                        """,
                        (keep,),
                    )
                    deleted += int(cur.rowcount or 0)
            c.commit()
        return deleted
