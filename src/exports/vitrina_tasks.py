from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import Item


VITRINA_HEADER = [
    "id",
    "title",
    "status",
    "due_at",
    "root",
    "parent",
    "path",
    "updated_at",
    "google_task_id",
]

_EXCLUDED_STATUSES = {"done", "archived"}


def _fmt_dt(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.isoformat()


def _build_path(item: Item, index: dict[str, Item]) -> tuple[str, str, str]:
    titles: list[str] = []
    parent_title = ""
    current: Item | None = item
    guard = 0
    while current is not None and guard < 32:
        title = (current.title or "").strip() or f"#{current.id}"
        titles.append(title)
        if current.parent_id and not parent_title and current.id == item.id:
            parent = index.get(current.parent_id)
            parent_title = (parent.title or "").strip() if parent is not None else ""
        if not current.parent_id:
            break
        current = index.get(current.parent_id)
        guard += 1
    titles.reverse()
    root_title = titles[0] if titles else ((item.title or "").strip() or f"#{item.id}")
    path = " / ".join(titles) if titles else root_title
    return root_title, parent_title, path


def build_vitrina(session: Session) -> tuple[list[str], list[list[Any]]]:
    rows = list(
        session.scalars(
            select(Item).where(
                Item.type == "task",
                Item.status.notin_(_EXCLUDED_STATUSES),
            )
        ).all()
    )
    index = {item.id: item for item in rows}
    out: list[list[Any]] = []
    for item in sorted(rows, key=lambda x: (_fmt_dt(x.updated_at), x.id), reverse=True):
        root, parent, path = _build_path(item, index)
        out.append(
            [
                item.id,
                item.title or "",
                item.status or "",
                _fmt_dt(item.due_at),
                root,
                parent,
                path,
                _fmt_dt(item.updated_at),
                item.google_task_id or "",
            ]
        )
    return list(VITRINA_HEADER), out
