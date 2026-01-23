from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, desc, select
from sqlalchemy.orm import Session

from src.db.models import PendingAction


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _cleanup_expired(session: Session) -> None:
    session.execute(delete(PendingAction).where(PendingAction.expires_at <= _now_utc()))


def create_pending(
    session: Session,
    *,
    chat_id: int,
    user_id: int,
    intent: str,
    action_type: str | None = None,
    args_dict: dict,
    raw_head: str,
    raw_text: str,
    source: str,
    confidence: float,
    canonical_text: str = "",
    missing: list[str] | None = None,
    stage: str | None = None,
    meta: dict | None = None,
    ttl_sec: int = 600,
) -> PendingAction:
    _cleanup_expired(session)
    now = _now_utc()
    payload = json.dumps(args_dict, ensure_ascii=False, default=str)
    pending = PendingAction(
        chat_id=chat_id,
        user_id=user_id,
        intent=intent,
        action_type=action_type,
        source=source,
        confidence=confidence,
        args_json=payload,
        raw_head=raw_head[:50],
        raw_text=raw_text,
        canonical_text=canonical_text,
        missing_json=json.dumps(missing or [], ensure_ascii=False),
        state="await_confirm",
        stage=stage,
        awaiting_field=None,
        meta_json=json.dumps(meta, ensure_ascii=False) if meta else None,
        created_at=now,
        expires_at=now + timedelta(seconds=ttl_sec),
    )
    session.add(pending)
    session.flush()
    session.refresh(pending)
    return pending


def get_pending(session: Session, pending_id: str) -> PendingAction | None:
    _cleanup_expired(session)
    return session.get(PendingAction, pending_id)


def get_latest_pending(session: Session, chat_id: int, user_id: int) -> PendingAction | None:
    _cleanup_expired(session)
    stmt = (
        select(PendingAction)
        .where(PendingAction.chat_id == chat_id, PendingAction.user_id == user_id)
        .order_by(desc(PendingAction.created_at))
    )
    return session.scalar(stmt)


def set_state(session: Session, pending_id: str, state: str) -> None:
    pending = session.get(PendingAction, pending_id)
    if pending is None:
        return
    pending.state = state
    session.add(pending)


def update_pending(
    session: Session,
    pending_id: str,
    *,
    args_dict: dict | None = None,
    missing: list[str] | None = None,
    state: str | None = None,
    awaiting_field: str | None = None,
    stage: str | None = None,
    meta: dict | None = None,
) -> None:
    pending = session.get(PendingAction, pending_id)
    if pending is None:
        return
    if args_dict is not None:
        pending.args_json = json.dumps(args_dict, ensure_ascii=False)
    if missing is not None:
        pending.missing_json = json.dumps(missing, ensure_ascii=False)
    if state is not None:
        pending.state = state
    pending.awaiting_field = awaiting_field
    if stage is not None:
        pending.stage = stage
    if meta is not None:
        pending.meta_json = json.dumps(meta, ensure_ascii=False)
    session.add(pending)


def delete_pending(session: Session, pending_id: str) -> None:
    pending = session.get(PendingAction, pending_id)
    if pending is None:
        return
    session.delete(pending)


def cleanup_expired(session: Session) -> None:
    _cleanup_expired(session)
