from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from src.db.models import Item
from src.google.conflict_engine import detect_conflicts, persist_detected_conflicts


def _is_apply_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().upper() == "TRUE"


def _parse_due(value: Any) -> datetime | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _build_patch_from_sheet_row(row: dict[str, Any]) -> dict[str, Any]:
    patch: dict[str, Any] = {}
    if "title" in row:
        patch["title"] = row.get("title")
    if "description" in row:
        patch["description"] = row.get("description")
    if "due_at" in row:
        patch["due_at"] = _parse_due(row.get("due_at"))
    if "status" in row:
        patch["status"] = row.get("status")
    return {k: v for k, v in patch.items() if v is not None}


def _apply_patch(item: Item, patch: dict[str, Any]) -> bool:
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


def pull_google_sheet_apply_rows(
    session: Session,
    rows: list[dict[str, Any]],
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    stats = {"seen_apply_true": 0, "applied": 0, "conflicts": 0, "missing_item": 0}
    row_updates: list[dict[str, Any]] = []

    for idx, row in enumerate(rows):
        if not _is_apply_true(row.get("apply")):
            continue
        stats["seen_apply_true"] += 1
        item_id = str(row.get("item_id") or "").strip()
        item = session.get(Item, item_id) if item_id else None
        if item is None:
            stats["missing_item"] += 1
            row_updates.append(
                {
                    "row": idx,
                    "sheet_row": int(row.get("_sheet_row") or 0),
                    "status": "MISSING_ITEM",
                    "apply": True,
                }
            )
            continue

        patch = _build_patch_from_sheet_row(row)
        conflicts = detect_conflicts(item, patch, source="google_sheet_pull")
        if conflicts:
            stored = persist_detected_conflicts(session, conflicts, row_ref=str(row.get("row_id") or idx))
            stats["conflicts"] += len(stored)
            row_updates.append(
                {
                    "row": idx,
                    "sheet_row": int(row.get("_sheet_row") or 0),
                    "status": "CONFLICT",
                    "apply": True,
                    "conflict_ids": [c.id for c in stored],
                }
            )
            continue

        _apply_patch(item, patch)
        stats["applied"] += 1
        row_updates.append(
            {
                "row": idx,
                "sheet_row": int(row.get("_sheet_row") or 0),
                "status": "APPLIED",
                "apply": False,
            }
        )

    session.flush()
    return stats, row_updates
