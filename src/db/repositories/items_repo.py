import json
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from src.db.models import Item, ItemEvent, Project
from src.config import settings


def _json_default(value) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _log_event(session: Session, item_id: str, event_type: str, meta: dict | str | None = None) -> None:
    if isinstance(meta, dict):
        meta_json = json.dumps(meta, ensure_ascii=False, default=_json_default)
    elif meta is None:
        meta_json = None
    else:
        meta_json = str(meta)

    session.add(
        ItemEvent(
            item_id=item_id,
            event_type=event_type,
            ts=datetime.now(timezone.utc),
            meta_json=meta_json,
        )
    )


def create_item(
    session: Session,
    *,
    title: str,
    project_id: str,
    description: str | None = None,
    type: str = "task",
    status: str = "inbox",
    parent_id: str | None = None,
    depth: int = 0,
    **fields,
) -> Item:
    if parent_id:
        parent = session.get(Item, parent_id)
        if parent is None:
            raise ValueError("Parent item not found")
        depth = parent.depth + 1

    if depth > 3:
        raise ValueError("Depth must be <= 3")

    item = Item(
        title=title,
        description=description,
        type=type,
        status=status,
        project_id=project_id,
        parent_id=parent_id,
        depth=depth,
        **fields,
    )
    session.add(item)
    session.flush()
    _log_event(session, item.id, "created")
    session.commit()
    session.refresh(item)
    return item


def update_item(session: Session, item_id: str, **fields) -> Item:
    item = session.get(Item, item_id)
    if item is None:
        raise ValueError("Item not found")

    for key, value in fields.items():
        setattr(item, key, value)

    now = datetime.now(timezone.utc)
    item.updated_at = now
    item.last_touched_at = now
    _log_event(session, item.id, "updated", meta={"fields": list(fields.keys())})
    session.commit()
    session.refresh(item)
    return item


def move_item(
    session: Session,
    item_id: str,
    *,
    due_at: datetime | None = None,
    scheduled_at: datetime | None = None,
) -> Item:
    item = session.get(Item, item_id)
    if item is None:
        raise ValueError("Item not found")

    meta = {
        "due_at": {"from": item.due_at, "to": due_at},
        "scheduled_at": {"from": item.scheduled_at, "to": scheduled_at},
    }
    item.due_at = due_at
    item.scheduled_at = scheduled_at

    now = datetime.now(timezone.utc)
    item.updated_at = now
    item.last_touched_at = now
    _log_event(session, item.id, "moved", meta=meta)
    session.commit()
    session.refresh(item)
    return item


def complete_item(session: Session, item_id: str) -> Item:
    item = session.get(Item, item_id)
    if item is None:
        raise ValueError("Item not found")

    now = datetime.now(timezone.utc)
    item.status = "done"
    item.updated_at = now
    item.last_touched_at = now
    _log_event(session, item.id, "completed")
    session.commit()
    session.refresh(item)
    return item


def set_waiting(session: Session, item_id: str) -> Item:
    item = session.get(Item, item_id)
    if item is None:
        raise ValueError("Item not found")

    now = datetime.now(timezone.utc)
    item.status = "waiting"
    item.updated_at = now
    item.last_touched_at = now
    _log_event(session, item.id, "waiting")
    session.commit()
    session.refresh(item)
    return item


def list_inbox(session: Session) -> list[Item]:
    return list(session.scalars(select(Item).where(Item.status == "inbox")).all())


def _day_range(now: datetime, tz: ZoneInfo) -> tuple[datetime, datetime]:
    local = now.astimezone(tz)
    start = datetime.combine(local.date(), time.min, tzinfo=tz)
    end = start + timedelta(days=1)
    return start, end


def list_today(session: Session) -> list[Item]:
    tz = ZoneInfo(settings.timezone)
    now = datetime.now(tz)
    start, end = _day_range(now, tz)
    return list(
        session.scalars(
            select(Item).where(
                or_(
                    and_(Item.due_at >= start, Item.due_at < end),
                    and_(Item.scheduled_at >= start, Item.scheduled_at < end),
                )
            )
        ).all()
    )


def list_week(session: Session) -> list[Item]:
    tz = ZoneInfo(settings.timezone)
    now = datetime.now(tz)
    start, _ = _day_range(now, tz)
    end = start + timedelta(days=7)
    return list(
        session.scalars(
            select(Item).where(
                or_(
                    and_(Item.due_at >= start, Item.due_at < end),
                    and_(Item.scheduled_at >= start, Item.scheduled_at < end),
                )
            )
        ).all()
    )


def list_overdue(session: Session) -> list[Item]:
    now = datetime.now(timezone.utc)
    return list(
        session.scalars(
            select(Item).where(
                and_(Item.due_at.is_not(None), Item.due_at < now, Item.status == "active")
            )
        ).all()
    )


def list_by_project(
    session: Session,
    *,
    project_id: str | None = None,
    project_name: str | None = None,
) -> list[Item]:
    if project_id:
        stmt = select(Item).where(Item.project_id == project_id)
    elif project_name:
        stmt = select(Item).join(Project).where(Project.name == project_name)
    else:
        raise ValueError("project_id or project_name is required")

    return list(session.scalars(stmt).all())
