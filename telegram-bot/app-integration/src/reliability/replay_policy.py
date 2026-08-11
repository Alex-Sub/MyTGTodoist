from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from .queue_store import BufferEvent


def utc_ts_from_iso(value: str) -> Optional[float]:
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


@dataclass(frozen=True)
class ReplayDecision:
    replay_type: str
    action: str
    reason: str
    age_seconds: Optional[float]


def classify_replay_type(ev: BufferEvent) -> str:
    parts = [
        str(ev.payload_type or ""),
        str(ev.payload_ref or ""),
        str(ev.transcript or ""),
        str(ev.normalized_text or ""),
        str(ev.channel or ""),
    ]
    text = " ".join(parts).strip().lower()

    hard_keywords = (
        "reminder",
        "напомин",
        "notification",
        "уведом",
        "timeblock",
        "meeting",
        "встреч",
        "exact-time",
    )
    soft_keywords = (
        "task",
        "задач",
        "today",
        "сегодня",
        "follow-up",
        "followup",
        "план",
        "planning",
    )
    non_critical_keywords = (
        "sync",
        "mirror",
        "inbox",
        "local-db",
        "local db",
        "delivery",
        "buffer",
    )

    if any(k in text for k in hard_keywords):
        return "hard_time_bound"
    if any(k in text for k in non_critical_keywords):
        return "non_time_critical"
    if any(k in text for k in soft_keywords):
        return "soft_time_bound"
    return "soft_time_bound"


def decide_replay_action(
    ev: BufferEvent,
    *,
    ttl_seconds: int,
    now_ts: Optional[float] = None,
) -> ReplayDecision:
    ttl = max(1, int(ttl_seconds))
    replay_type = classify_replay_type(ev)
    created_ts = utc_ts_from_iso(ev.created_at)
    if created_ts is None:
        return ReplayDecision(
            replay_type=replay_type,
            action="skip_as_stale",
            reason="replay_skipped:invalid_created_at",
            age_seconds=None,
        )

    now = float(time.time() if now_ts is None else now_ts)
    age = max(0.0, now - created_ts)
    ttl_f = float(ttl)

    if replay_type == "hard_time_bound":
        if age <= ttl_f:
            return ReplayDecision(replay_type=replay_type, action="deliver_now", reason="replay:deliver:hard", age_seconds=age)
        return ReplayDecision(
            replay_type=replay_type,
            action="skip_as_stale",
            reason="replay_skipped:stale_pending:hard_time_bound",
            age_seconds=age,
        )

    if replay_type == "non_time_critical":
        if age <= (3.0 * ttl_f):
            return ReplayDecision(
                replay_type=replay_type,
                action="deliver_now",
                reason="replay:deliver:non_time_critical",
                age_seconds=age,
            )
        return ReplayDecision(
            replay_type=replay_type,
            action="mark_needs_review",
            reason="replay_review:non_time_critical",
            age_seconds=age,
        )

    if age <= ttl_f:
        return ReplayDecision(replay_type="soft_time_bound", action="deliver_now", reason="replay:deliver:soft", age_seconds=age)
    if age <= (3.0 * ttl_f):
        return ReplayDecision(
            replay_type="soft_time_bound",
            action="mark_needs_review",
            reason="replay_review:soft_time_bound",
            age_seconds=age,
        )
    return ReplayDecision(
        replay_type="soft_time_bound",
        action="skip_as_stale",
        reason="replay_skipped:stale_pending:soft_time_bound",
        age_seconds=age,
    )
