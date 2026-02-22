from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import Item
from src.google.conflict_engine import detect_conflicts, persist_detected_conflicts
from src.google.tasks_client import TasksClient


def _parse_google_due(raw: str | None) -> datetime | None:
    if not raw:
        return None
    text = str(raw).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _remote_patch_from_task(task: dict[str, Any]) -> dict[str, Any]:
    patch: dict[str, Any] = {}
    if "title" in task:
        patch["title"] = task.get("title") or ""
    if "notes" in task:
        patch["description"] = task.get("notes")
    if "due" in task:
        patch["due_at"] = _parse_google_due(task.get("due"))
    if "status" in task:
        patch["status"] = "done" if str(task.get("status")) == "completed" else "active"
    return patch


def _has_local_changes(item: Item) -> bool:
    if str(item.google_sync_status or "") in {"pending", "failed"}:
        return True
    if item.google_synced_at and item.updated_at:
        return bool(item.updated_at > item.google_synced_at)
    return False


def _apply_remote_patch(item: Item, patch: dict[str, Any]) -> bool:
    changed = False
    for key, value in patch.items():
        if not hasattr(item, key):
            continue
        if getattr(item, key) == value:
            continue
        setattr(item, key, value)
        changed = True
    if changed:
        item.updated_at = datetime.now(timezone.utc)
        item.google_sync_status = "synced"
        item.google_sync_error = None
        item.google_synced_at = datetime.now(timezone.utc)
    return changed


def pull_google_tasks(session: Session, *, tasklist_id: str = "@default", max_results: int = 100) -> dict[str, int]:
    client = TasksClient()
    stats = {
        "seen": 0,
        "applied": 0,
        "conflicts": 0,
        "skipped_local_changes": 0,
        "skipped_missing_local": 0,
    }
    page_token: str | None = None
    while True:
        data = client.list_tasks(tasklist_id=tasklist_id, page_token=page_token, max_results=max_results)
        for task in data.get("items") or []:
            stats["seen"] += 1
            remote_id = str(task.get("id") or "").strip()
            if not remote_id:
                continue
            item = session.scalar(select(Item).where(Item.google_task_id == remote_id))
            if item is None:
                stats["skipped_missing_local"] += 1
                continue
            patch = _remote_patch_from_task(task)
            if not patch:
                continue

            local_dirty = _has_local_changes(item)
            conflicts = detect_conflicts(item, patch, source="google_tasks_pull") if local_dirty else []
            if conflicts:
                persist_detected_conflicts(session, conflicts)
                stats["conflicts"] += len(conflicts)
                continue

            if local_dirty:
                stats["skipped_local_changes"] += 1
                continue

            if _apply_remote_patch(item, patch):
                stats["applied"] += 1

        page_token = data.get("nextPageToken")
        if not page_token:
            break
    session.flush()
    return stats
