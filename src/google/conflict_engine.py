from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from src.db.models import Conflict, Item
from src.db.repositories.conflicts_repo import apply_conflict_choice as repo_apply_conflict_choice
from src.db.repositories.conflicts_repo import create_conflict, list_open_conflicts
from src.db.repositories.conflicts_repo import ConflictDraft


@dataclass(slots=True)
class DetectedConflict:
    item_id: str
    source: str
    field_name: str
    local_value: Any
    remote_value: Any
    remote_patch: dict[str, Any]
    row_ref: str | None = None


_PATCH_FIELDS = ("title", "description", "due_at", "status")


def _norm(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def detect_conflicts(item: Item, remote_patch: dict[str, Any], *, source: str = "google_tasks") -> list[DetectedConflict]:
    out: list[DetectedConflict] = []
    for field_name in _PATCH_FIELDS:
        if field_name not in remote_patch:
            continue
        local_value = getattr(item, field_name, None)
        remote_value = remote_patch.get(field_name)
        if _norm(local_value) == _norm(remote_value):
            continue
        out.append(
            DetectedConflict(
                item_id=item.id,
                source=source,
                field_name=field_name,
                local_value=local_value,
                remote_value=remote_value,
                remote_patch=remote_patch,
            )
        )
    return out


def render_conflict_question(conflicts: list[Conflict | DetectedConflict]) -> tuple[str, list[dict[str, str]]]:
    question = "Найдены конфликты синхронизации. Что разбираем первым?"
    choices: list[dict[str, str]] = []
    for idx, conflict in enumerate(conflicts, start=1):
        cid = str(getattr(conflict, "id", "")) or f"new:{idx}"
        field_name = str(getattr(conflict, "field_name", "field"))
        local_value = str(getattr(conflict, "local_value", "") or "")
        remote_value = str(getattr(conflict, "remote_value", "") or "")
        label = f"#{idx} {field_name}: local='{local_value[:24]}' remote='{remote_value[:24]}'"
        choices.append({"id": cid, "label": label})
    return question, choices


def apply_conflict_choice(session: Session, conflict_id: str, choice: str) -> Conflict:
    return repo_apply_conflict_choice(session, conflict_id, choice)


def persist_detected_conflicts(
    session: Session,
    conflicts: list[DetectedConflict],
    *,
    row_ref: str | None = None,
) -> list[Conflict]:
    rows: list[Conflict] = []
    for c in conflicts:
        rows.append(
            create_conflict(
                session,
                ConflictDraft(
                    item_id=c.item_id,
                    source=c.source,
                    field_name=c.field_name,
                    local_value=c.local_value,
                    remote_value=c.remote_value,
                    remote_patch=c.remote_patch,
                    row_ref=row_ref or c.row_ref,
                ),
            )
        )
    return rows


def open_conflicts_clarification(session: Session, *, limit: int = 5) -> dict[str, Any] | None:
    conflicts = list_open_conflicts(session, limit=limit)
    if not conflicts:
        return None
    question, choices = render_conflict_question(conflicts)
    return {
        "ok": False,
        "user_message": "Нужны уточнения.",
        "clarifying_question": question,
        "choices": choices,
        "debug": {"open_conflicts": len(conflicts)},
    }
