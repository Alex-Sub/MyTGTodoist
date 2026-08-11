import json
import logging
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any


def _queue_reaper(*, deps: dict[str, Any]) -> None:
    with deps["get_conn"]() as conn:
        deps["reap_claims"](conn, time.time())
        conn.commit()


def _queue_claim(*, deps: dict[str, Any]) -> dict | None:
    with deps["get_conn"]() as conn:
        conn.execute("BEGIN IMMEDIATE")
        now_ts = time.time()
        for _ in range(max(1, int(deps["b2_replay_scan_limit"]))):
            cand = conn.execute(
                """
                SELECT id, ingested_at, created_at, updated_at
                FROM inbox_queue
                WHERE status='NEW'
                ORDER BY priority ASC, id ASC
                LIMIT 1
                """
            ).fetchone()
            if cand is None:
                conn.commit()
                return None
            candidate = deps["as_dict"](cand)
            queue_id = int(candidate.get("id"))
            stale, age_sec = deps["is_stale_queue_row"](candidate, now_ts)
            if stale:
                cur_dead = conn.execute(
                    """
                    UPDATE inbox_queue
                    SET status='DEAD',
                        last_error=?,
                        updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
                    WHERE id=? AND status='NEW'
                    """,
                    ("replay_expired_new", queue_id),
                )
                if cur_dead.rowcount:
                    logging.warning(
                        "queue_replay_guard queue_id=%s action=dead reason=expired_new age_sec=%s max_age_sec=%s",
                        queue_id,
                        f"{age_sec:.1f}" if age_sec is not None else "-",
                        deps["b2_replay_max_age_sec"],
                    )
                continue

            cur_claim = conn.execute(
                """
                UPDATE inbox_queue
                SET status='CLAIMED',
                    claimed_by=?,
                    claimed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                    lease_until=strftime('%Y-%m-%dT%H:%M:%fZ','now', '+' || ? || ' seconds'),
                    attempts=attempts+1,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE id=? AND status='NEW'
                """,
                (deps["worker_id"], str(deps["b2_claim_lease_sec"]), queue_id),
            )
            if cur_claim.rowcount != 1:
                continue
            row = conn.execute(
                """
                SELECT *
                FROM inbox_queue
                WHERE id=? AND status='CLAIMED' AND claimed_by=?
                LIMIT 1
                """,
                (queue_id, deps["worker_id"]),
            ).fetchone()
            conn.commit()
            return dict(row) if row else None
        conn.commit()
        return None


def _queue_mark(queue_id: int, status: str, last_error: str | None = None, *, deps: dict[str, Any]) -> None:
    with deps["get_conn"]() as conn:
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


def _queue_requeue_failed(limit: int, *, deps: dict[str, Any]) -> int:
    if limit <= 0:
        return 0
    moved = 0
    now_ts = time.time()
    with deps["get_conn"]() as conn:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            """
            SELECT id, ingested_at, created_at, updated_at
            FROM inbox_queue
            WHERE status='FAILED'
              AND attempts < ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (deps["b2_max_attempts"], max(int(limit), int(deps["b2_replay_scan_limit"]))),
        ).fetchall()
        for raw in rows:
            if moved >= int(limit):
                break
            row = deps["as_dict"](raw)
            queue_id = int(row.get("id"))
            stale, age_sec = deps["is_stale_queue_row"](row, now_ts)
            if stale:
                cur_dead = conn.execute(
                    """
                    UPDATE inbox_queue
                    SET status='DEAD',
                        last_error=?,
                        updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
                    WHERE id=? AND status='FAILED'
                    """,
                    ("replay_expired_failed", queue_id),
                )
                if cur_dead.rowcount:
                    logging.warning(
                        "queue_replay_guard queue_id=%s action=dead reason=expired_failed age_sec=%s max_age_sec=%s",
                        queue_id,
                        f"{age_sec:.1f}" if age_sec is not None else "-",
                        deps["b2_replay_max_age_sec"],
                    )
                continue
            cur = conn.execute(
                """
                UPDATE inbox_queue
                SET status='NEW',
                    claimed_by=NULL,
                    claimed_at=NULL,
                    lease_until=NULL,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE id=? AND status='FAILED' AND attempts < ?
                """,
                (queue_id, deps["b2_max_attempts"]),
            )
            if cur.rowcount == 1:
                moved += 1
        conn.commit()
    return moved


def _enqueue_clarify(chat_id: int, item: dict, *, deps: dict[str, Any]) -> int:
    state = deps["load_clarify_state"]()
    deps["prune_clarify_state"](state, time.time())
    st = state.get(str(chat_id)) or {"queue": []}
    q = st.get("queue") or []
    q.append(item)
    st["queue"] = q
    state[str(chat_id)] = st
    deps["save_clarify_state"](state)
    return len(q)


def _update_item_status(item_id: int, new_status: str, *, deps: dict[str, Any]) -> None:
    with deps["get_conn"]() as conn:
        row = conn.execute(
            """
            SELECT id, type, status, parent_id, parent_id_int
            FROM items
            WHERE id = ?
            """,
            (int(item_id),),
        ).fetchone()
        row = deps["as_dict"](row)
        if not row:
            raise ValueError("item not found")
        if deps["p2_enforce_status"]:
            open_subtasks = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM items
                WHERE (parent_id_int = ? OR parent_id = ?)
                  AND status != 'done'
                """,
                (int(item_id), int(item_id)),
            ).fetchone()
            open_cnt = int(deps["as_dict"](open_subtasks).get("cnt") or 0)
            deps["validate_task_status"](row, new_status, open_cnt)
        conn.execute(
            """
            UPDATE items
            SET status = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (new_status, datetime.now(timezone.utc).isoformat(), int(item_id)),
        )
        conn.commit()


def create_task(
    title: str,
    *,
    status: str = "inbox",
    from_inbox_item_id: int | None = None,
    deps: dict[str, Any],
) -> int:
    if from_inbox_item_id is not None:
        with deps["get_conn"]() as conn:
            row = conn.execute(
                """
                SELECT id, status, parent_id, parent_id_int
                FROM items
                WHERE id = ?
                """,
                (int(from_inbox_item_id),),
            ).fetchone()
            row = deps["as_dict"](row)
            if not row:
                raise ValueError("inbox item not found")
            if deps["get_parent_id_from_row"](row) is not None:
                raise ValueError("cannot promote a subtask to task")
            if str(row.get("status") or "") != "inbox":
                raise ValueError("only inbox items can be promoted to task")
            if deps["p2_enforce_status"]:
                deps["validate_task_status"](row, status, 0)
            conn.execute(
                """
                UPDATE items
                SET type = 'task',
                    status = ?,
                    title = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (status, title, datetime.now(timezone.utc).isoformat(), int(from_inbox_item_id)),
            )
            conn.commit()
        return int(from_inbox_item_id)

    if status not in {"inbox", "active"}:
        raise ValueError("task status must be inbox or active on create")
    with deps["get_conn"]() as conn:
        cur = conn.execute(
            """
            INSERT INTO items (
                type, title, status, parent_id, parent_id_int, created_at
            )
            VALUES ('task', ?, ?, NULL, NULL, ?)
            """,
            (title, status, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        return deps["require_lastrowid"](cur)


def create_subtask(
    parent_id: int,
    title: str,
    *,
    status: str = "todo",
    deps: dict[str, Any],
) -> int:
    with deps["get_conn"]() as conn:
        parent = conn.execute(
            """
            SELECT id, type, status, parent_id, parent_id_int
            FROM items
            WHERE id = ?
            """,
            (int(parent_id),),
        ).fetchone()
        parent = deps["as_dict"](parent)
        if not parent:
            raise ValueError("parent not found")
        if deps["get_parent_id_from_row"](parent) is not None:
            raise ValueError("cannot create subtask under subtask")
        if str(parent.get("type") or "") != "task":
            raise ValueError("parent must be task")
        if str(parent.get("status") or "") == "done":
            raise ValueError("cannot add subtask to done task")
        if status not in {"todo", "done"}:
            raise ValueError("subtask status must be todo or done")

        cur = conn.execute(
            """
            INSERT INTO items (
                type, title, status, parent_id, parent_id_int, created_at
            )
            VALUES ('task', ?, ?, ?, ?, ?)
            """,
            (title, status, int(parent_id), int(parent_id), datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        return deps["require_lastrowid"](cur)


def _process_queue_item(row: dict, *, deps: dict[str, Any]) -> None:
    deps["impl_process_queue_item"](row)


def _find_item_for_queue_delivery(queue_row: dict, *, deps: dict[str, Any]) -> dict:
    return deps["impl_find_item_for_queue_delivery"](queue_row)


def re_deliver_done_items(limit: int = 50, *, deps: dict[str, Any]) -> dict:
    return deps["impl_re_deliver_done_items"](limit=limit)


def _sync_calendar_for_item(item: dict, *, deps: dict[str, Any]) -> None:
    deps["impl_sync_calendar_for_item"](item)


def _process_items(*, deps: dict[str, Any]) -> None:
    deps["impl_process_items"]()


def _retry_pending_events(*, deps: dict[str, Any]) -> None:
    deps["impl_retry_pending_events"]()
