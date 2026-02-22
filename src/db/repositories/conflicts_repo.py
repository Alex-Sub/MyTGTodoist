from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import Conflict, Item


@dataclass(slots=True)
class ConflictDraft:
    item_id: str
    source: str
    field_name: str
    local_value: Any
    remote_value: Any
    remote_patch: dict[str, Any]
    row_ref: str | None = None


def _to_jsonable(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _parse_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _apply_remote_field(item: Item, field_name: str, remote_value: Any) -> None:
    if field_name == "due_at":
        item.due_at = _parse_datetime(remote_value)
        return
    if field_name == "status":
        item.status = str(remote_value or item.status)
        return
    if field_name == "title":
        item.title = str(remote_value or item.title)
        return
    if field_name == "description":
        item.description = None if remote_value is None else str(remote_value)
        return


def create_conflict(session: Session, draft: ConflictDraft) -> Conflict:
    local_value = _to_jsonable(draft.local_value)
    remote_value = _to_jsonable(draft.remote_value)
    existing = session.scalar(
        select(Conflict).where(
            Conflict.item_id == draft.item_id,
            Conflict.source == draft.source,
            Conflict.field_name == draft.field_name,
            Conflict.local_value == local_value,
            Conflict.remote_value == remote_value,
            Conflict.status == "open",
        )
    )
    if existing is not None:
        return existing

    row = Conflict(
        item_id=draft.item_id,
        source=draft.source,
        field_name=draft.field_name,
        local_value=local_value,
        remote_value=remote_value,
        remote_patch_json=_to_jsonable(draft.remote_patch),
        status="open",
        row_ref=draft.row_ref,
    )
    session.add(row)
    session.flush()
    return row


def list_open_conflicts(session: Session, *, limit: int = 20) -> list[Conflict]:
    return list(
        session.scalars(
            select(Conflict).where(Conflict.status == "open").order_by(Conflict.created_at.asc()).limit(int(limit))
        ).all()
    )


def apply_conflict_choice(session: Session, conflict_id: str, choice: str) -> Conflict:
    conflict = session.get(Conflict, conflict_id)
    if conflict is None:
        raise ValueError("Conflict not found")
    if conflict.status != "open":
        return conflict

    choice_norm = (choice or "").strip().lower()
    if choice_norm not in {"keep_local", "accept_remote"}:
        raise ValueError("Unsupported choice")

    if choice_norm == "accept_remote":
        item = session.get(Item, conflict.item_id)
        if item is None:
            raise ValueError("Item not found")
        remote_value: Any = conflict.remote_value
        if conflict.field_name == "due_at":
            remote_value = _parse_datetime(remote_value)
        _apply_remote_field(item, conflict.field_name, remote_value)
        item.updated_at = datetime.now(timezone.utc)
        item.google_sync_status = "synced"
        item.google_sync_error = None
        item.google_synced_at = datetime.now(timezone.utc)

    conflict.status = "resolved"
    conflict.resolution = choice_norm
    conflict.resolved_at = datetime.now(timezone.utc)
    session.flush()
    return conflict
