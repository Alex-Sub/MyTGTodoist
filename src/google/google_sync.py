from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import settings
from src.db.models import Item
from src.db.session import get_session
from src.google.calendar_client import CalendarClient
from src.google.conflict_engine import open_conflicts_clarification
from src.google.sheet_pull import pull_google_sheet_apply_rows
from src.google.tasks_client import TasksClient
from src.google.tasks_pull import pull_google_tasks

_RETRY_LOCK = asyncio.Lock()


def _short_title(value: str, *, min_len: int, max_len: int, max_words: int) -> str:
    text = " ".join((value or "").split())
    if not text:
        return "Задача"
    words = text.split()
    picked = words[:max_words]
    out = " ".join(picked)
    if len(out) < min_len and len(words) > len(picked):
        for w in words[len(picked) :]:
            if len(out) >= min_len:
                break
            candidate = (out + " " + w).strip()
            if len(candidate) > max_len:
                break
            out = candidate
    if len(out) > max_len:
        out = out[:max_len].rstrip()
    return out


def _build_task_path_in_session(session: Session, task_id: str) -> tuple[str, str]:
    node = session.get(Item, task_id)
    if node is None:
        raise ValueError("Task not found")

    titles: list[str] = []
    current = node
    guard = 0
    while current is not None and guard < 32:
        titles.append((current.title or "").strip() or f"#{current.id}")
        if not current.parent_id:
            break
        current = session.get(Item, current.parent_id)
        guard += 1

    titles.reverse()
    root_title = titles[0] if titles else ((node.title or "").strip() or f"#{node.id}")
    path_label = " / ".join(titles) if titles else root_title
    return root_title, path_label


def build_task_path(task_id: str) -> tuple[str, str]:
    with get_session() as session:
        return _build_task_path_in_session(session, task_id)


def _calendar_title(root_title: str, task_title: str) -> str:
    root_short = _short_title(root_title, min_len=10, max_len=20, max_words=2)
    task_short = _short_title(task_title, min_len=1, max_len=48, max_words=6)
    return f"{root_short}: {task_short}"


def _event_description(path_label: str, item: Item) -> str:
    details = [f"Path: {path_label}", f"Item ID: {item.id}"]
    if item.description:
        details.append("")
        details.append(item.description)
    return "\n".join(details)


def _ensure_due_tz(due_at: datetime) -> datetime:
    tz = ZoneInfo(settings.timezone)
    if due_at.tzinfo is None:
        return due_at.replace(tzinfo=tz)
    return due_at.astimezone(tz)


def _sync_event_for_due(item: Item, *, title: str, description: str) -> None:
    if item.due_at is None:
        if item.event_id:
            CalendarClient().cancel_event(settings.google_calendar_id_default, item.event_id)
            item.event_id = None
            item.etag = None
            item.ical_uid = None
            item.g_updated = None
        return

    start = _ensure_due_tz(item.due_at)
    end = start + timedelta(minutes=max(15, int(item.duration_min or 30)))
    payload = {
        "summary": title,
        "description": description,
        "start": {"dateTime": start.isoformat(), "timeZone": settings.timezone},
        "end": {"dateTime": end.isoformat(), "timeZone": settings.timezone},
        "extendedProperties": {"private": {"item_id": item.id}},
        "reminders": {"useDefault": True},
    }
    calendar = CalendarClient()
    if item.event_id:
        resp = calendar.update_event(
            settings.google_calendar_id_default,
            item.event_id,
            payload,
            if_match_etag=item.etag,
        )
    else:
        resp = calendar.create_event(settings.google_calendar_id_default, payload)
    item.event_id = resp.get("event_id") or item.event_id
    item.etag = resp.get("etag") or item.etag
    item.ical_uid = resp.get("ical_uid") or item.ical_uid
    updated = resp.get("updated")
    if updated:
        if updated.endswith("Z"):
            updated = updated.replace("Z", "+00:00")
        item.g_updated = datetime.fromisoformat(updated)


def _mark_sync_ok(item: Item) -> None:
    item.google_sync_status = "synced"
    item.google_sync_error = None
    item.google_synced_at = datetime.now(ZoneInfo("UTC"))


def _mark_sync_err(item: Item, exc: Exception) -> None:
    item.google_sync_status = "failed"
    item.google_sync_attempts = int(item.google_sync_attempts or 0) + 1
    item.google_sync_error = str(exc)[:500]


def sync_task_created(session: Session, item_id: str) -> Item:
    item = session.get(Item, item_id)
    if item is None:
        raise ValueError("Item not found")
    root_title, path_label = _build_task_path_in_session(session, item.id)
    title = _calendar_title(root_title, item.title or "")
    description = _event_description(path_label, item)
    parent_google_id = None
    if item.parent_id:
        parent = session.get(Item, item.parent_id)
        if parent and parent.google_task_id:
            parent_google_id = parent.google_task_id
    try:
        due = _ensure_due_tz(item.due_at).isoformat() if item.due_at else None
        created = TasksClient().create_task(
            "@default",
            title=title,
            notes=description,
            due=due,
            parent=parent_google_id,
        )
        item.google_task_id = created.get("id") or item.google_task_id
        item.google_parent_task_id = parent_google_id
        _sync_event_for_due(item, title=title, description=description)
        _mark_sync_ok(item)
    except Exception as exc:
        _mark_sync_err(item, exc)
        raise
    return item


def sync_task_updated(session: Session, item_id: str) -> Item:
    item = session.get(Item, item_id)
    if item is None:
        raise ValueError("Item not found")
    if not item.google_task_id:
        return sync_task_created(session, item_id)
    root_title, path_label = _build_task_path_in_session(session, item.id)
    title = _calendar_title(root_title, item.title or "")
    description = _event_description(path_label, item)
    due = _ensure_due_tz(item.due_at).isoformat() if item.due_at else None
    payload = {"title": title, "notes": description, "due": due}
    try:
        TasksClient().update_task("@default", item.google_task_id, payload)
        _sync_event_for_due(item, title=title, description=description)
        _mark_sync_ok(item)
    except Exception as exc:
        _mark_sync_err(item, exc)
        raise
    return item


def sync_task_completed(session: Session, item_id: str) -> Item:
    item = session.get(Item, item_id)
    if item is None:
        raise ValueError("Item not found")
    try:
        if item.google_task_id:
            TasksClient().complete_task("@default", item.google_task_id)
        if item.event_id:
            CalendarClient().cancel_event(settings.google_calendar_id_default, item.event_id)
            item.event_id = None
            item.etag = None
            item.ical_uid = None
            item.g_updated = None
        _mark_sync_ok(item)
    except Exception as exc:
        _mark_sync_err(item, exc)
        raise
    return item


async def run_google_sync_retry_worker(interval_sec: int = 60, max_attempts: int = 10) -> None:
    logger.info("google task sync retry worker started interval={}s", interval_sec)
    while True:
        try:
            async with _RETRY_LOCK:
                with get_session() as session:
                    rows = list(
                        session.scalars(
                            select(Item).where(
                                Item.type == "task",
                                Item.google_sync_status.in_(("pending", "failed")),
                                Item.google_sync_attempts < int(max_attempts),
                            )
                        ).all()
                    )
                    for item in rows:
                        try:
                            if item.status == "done":
                                sync_task_completed(session, item.id)
                            elif item.google_task_id:
                                sync_task_updated(session, item.id)
                            else:
                                sync_task_created(session, item.id)
                        except Exception as exc:
                            logger.warning("google task sync retry failed item={} err={}", item.id, str(exc)[:200])
        except Exception as exc:
            logger.error("google task sync retry worker error: {}", exc)
        await asyncio.sleep(max(10, int(interval_sec)))


def pull_google_tasks_with_conflicts(session: Session, *, tasklist_id: str = "@default") -> dict[str, object]:
    stats = pull_google_tasks(session, tasklist_id=tasklist_id)
    clarification = open_conflicts_clarification(session, limit=5)
    return {"stats": stats, "clarification": clarification}


def pull_google_sheet_with_apply(
    session: Session,
    rows: list[dict[str, object]],
) -> dict[str, object]:
    stats, row_updates = pull_google_sheet_apply_rows(session, rows)
    clarification = open_conflicts_clarification(session, limit=5)
    return {"stats": stats, "rows": row_updates, "clarification": clarification}
