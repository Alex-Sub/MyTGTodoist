import json
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import and_, func, or_, select
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


def create_task(
    session: Session,
    *,
    title: str | None = None,
    status: str = "inbox",
    description: str | None = None,
    from_inbox_item_id: str | None = None,
    **fields,
) -> Item:
    """
    Create a root task either directly or by promoting an inbox item.
    - Direct create: status is "inbox" or "active".
    - From inbox: item must be root and status "inbox".
    """
    if from_inbox_item_id:
        item = session.get(Item, from_inbox_item_id)
        if item is None:
            raise ValueError("Inbox item not found")
        if item.parent_id is not None:
            raise ValueError("Cannot promote a subtask to task")
        if item.status != "inbox":
            raise ValueError("Only inbox items can be promoted to task")
        if title is not None:
            item.title = title
        if description is not None:
            item.description = description
        item.type = "task"
        validate_task_status(session, item, status)
        item.status = status
        item.updated_at = datetime.now(timezone.utc)
        item.last_touched_at = item.updated_at
        _log_event(session, item.id, "promoted_to_task")
        session.commit()
        session.refresh(item)
        return item

    if status not in {"inbox", "active"}:
        raise ValueError("Task status must be inbox or active on create")
    item = Item(
        title=title or "",
        description=description,
        type="task",
        status=status,
        project_id=None,
        parent_id=None,
        depth=0,
        **fields,
    )
    session.add(item)
    session.flush()
    _log_event(session, item.id, "created_task")
    session.commit()
    session.refresh(item)
    return item


def create_subtask(
    session: Session,
    *,
    parent_id: str,
    title: str,
    description: str | None = None,
    status: str = "todo",
    **fields,
) -> Item:
    """
    Create a subtask for a root task.
    Rules:
    - parent must exist, be type='task', and be root (parent_id IS NULL)
    - parent cannot be done
    - subtask depth=1, parent_id is required
    """
    parent = session.get(Item, parent_id)
    if parent is None:
        raise ValueError("Parent task not found")
    if parent.parent_id is not None:
        raise ValueError("Cannot create a subtask under another subtask")
    if parent.type != "task":
        raise ValueError("Parent must be a task")
    if parent.status == "done":
        raise ValueError("Cannot add subtask to a done task")
    if status not in {"todo", "done"}:
        raise ValueError("Subtask status must be todo or done on create")

    item = Item(
        title=title,
        description=description,
        type="task",
        status=status,
        project_id=parent.project_id,
        parent_id=parent.id,
        depth=1,
        **fields,
    )
    session.add(item)
    session.flush()
    _log_event(session, item.id, "created_subtask", meta={"parent_id": parent.id})
    session.commit()
    session.refresh(item)
    return item


def validate_task_status(session: Session, item: Item, new_status: str) -> None:
    """
    Validate task/subtask status transitions.
    - task: inbox -> active -> done -> archived
    - subtask: todo -> done
    - task cannot be done if any subtask is not done
    """
    if item.type != "task":
        raise ValueError("Status validation applies only to tasks")

    is_subtask = item.parent_id is not None
    if is_subtask:
        allowed = {"todo": {"done"}, "done": set()}
    else:
        allowed = {"inbox": {"active"}, "active": {"done"}, "done": {"archived"}, "archived": set()}

    current = item.status
    if new_status == current:
        return
    if current not in allowed or new_status not in allowed[current]:
        raise ValueError(f"Invalid status transition: {current} -> {new_status}")

    if not is_subtask and new_status == "done":
        open_subtasks = session.scalar(
            select(func.count()).where(and_(Item.parent_id == item.id, Item.status != "done"))
        )
        if open_subtasks and int(open_subtasks) > 0:
            raise ValueError("Cannot complete task with open subtasks")


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
