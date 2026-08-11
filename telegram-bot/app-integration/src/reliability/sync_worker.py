from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .queue_store import BufferEvent, CloudBufferAdapter
from .local_db import LocalMainDb
from .replay_policy import ReplayDecision, classify_replay_type, decide_replay_action


def _is_retryable(reason: str) -> bool:
    s = (reason or "").lower()
    return any(x in s for x in ("connect", "connection", "temporar", "timeout", "unavailable"))


@dataclass
class SyncCfg:
    batch_size: int = 50
    max_backoff_sec: int = 3600
    queue_replay_ttl_seconds: int = 600


class SyncWorker:
    def __init__(self, *, cloud: CloudBufferAdapter, local: LocalMainDb, cfg: Optional[SyncCfg] = None) -> None:
        self.cloud = cloud
        self.local = local
        self.cfg = cfg or SyncCfg()

    def deliver_one(self, ev: BufferEvent) -> Tuple[bool, str]:
        """
        Deliver into local DB idempotently by request_id.
        Returns (delivered, reason).
        """
        try:
            row: Dict[str, Any] = {
                "id": ev.id,
                "created_at": ev.created_at,
                "user_id": ev.user_id,
                "channel": ev.channel,
                "payload_type": ev.payload_type,
                "payload_ref": ev.payload_ref,
                "transcript": ev.transcript,
                "normalized_text": ev.normalized_text,
                "request_id": ev.request_id,
            }
            inserted = self.local.insert_idempotent(row)
            return True, "inserted" if inserted else "duplicate"
        except ValueError as e:
            return False, f"invalid_payload:{e}"
        except Exception as e:
            return False, f"deliver_error:{type(e).__name__}:{e}"

    def classify_replay_type(self, ev: BufferEvent) -> str:
        return classify_replay_type(ev)

    def decide_replay_action(
        self,
        ev: BufferEvent,
        *,
        now_ts: Optional[float] = None,
        ttl_seconds: Optional[int] = None,
    ) -> ReplayDecision:
        ttl = int(self.cfg.queue_replay_ttl_seconds if ttl_seconds is None else ttl_seconds)
        return decide_replay_action(
            ev,
            ttl_seconds=ttl,
            now_ts=now_ts,
        )

    def sync_once(self) -> Dict[str, Any]:
        self.cloud.ensure_schema()
        self.local.ensure_schema()

        batch = self.cloud.fetch_pending(limit=int(self.cfg.batch_size))
        out = {
            "fetched": len(batch),
            "delivered": 0,
            "failed": 0,
            "needs_review": 0,
            "replay_skipped": 0,
            "replay_needs_review": 0,
            "rows": [],
        }  # type: ignore

        for ev in batch:
            decision = self.decide_replay_action(ev)
            if decision.action == "skip_as_stale":
                reason = decision.reason
                self.cloud.mark_failed(ev.id, reason)
                out["failed"] += 1
                out["replay_skipped"] += 1
                out["rows"].append(
                    {
                        "id": ev.id,
                        "request_id": ev.request_id,
                        "ok": False,
                        "reason": reason,
                        "replay_type": decision.replay_type,
                        "replay_action": decision.action,
                    }
                )
                continue

            if decision.action == "mark_needs_review":
                reason = decision.reason
                self.cloud.mark_needs_review(ev.id, reason)
                out["needs_review"] += 1
                out["replay_needs_review"] += 1
                out["rows"].append(
                    {
                        "id": ev.id,
                        "request_id": ev.request_id,
                        "ok": False,
                        "reason": reason,
                        "replay_type": decision.replay_type,
                        "replay_action": decision.action,
                    }
                )
                continue

            ok, reason = self.deliver_one(ev)
            if ok:
                self.cloud.mark_delivered(ev.id)
                out["delivered"] += 1
            else:
                # retryable vs non-retryable: keep "failed" but do not loop here
                self.cloud.mark_failed(ev.id, reason)
                out["failed"] += 1
            out["rows"].append(
                {
                    "id": ev.id,
                    "request_id": ev.request_id,
                    "ok": ok,
                    "reason": reason,
                    "replay_type": decision.replay_type,
                    "replay_action": decision.action,
                }
            )

        return out

    def loop(self, *, sleep_sec: float = 1.0) -> None:
        backoff = 1.0
        while True:
            try:
                r = self.sync_once()
                backoff = 1.0
                time.sleep(float(sleep_sec))
            except Exception as e:
                reason = f"{type(e).__name__}:{e}"
                if _is_retryable(reason):
                    time.sleep(backoff)
                    backoff = min(float(self.cfg.max_backoff_sec), backoff * 2.0)
                    continue
                raise
