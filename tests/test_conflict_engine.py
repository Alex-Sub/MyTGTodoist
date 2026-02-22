from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from src.google.conflict_engine import detect_conflicts, render_conflict_question


@dataclass
class _FakeItem:
    id: str
    title: str
    description: str | None
    due_at: datetime | None
    status: str


def test_detect_conflicts_returns_changed_fields() -> None:
    item = _FakeItem(
        id="item-1",
        title="Local title",
        description="Local description",
        due_at=datetime(2026, 2, 22, 10, 0, tzinfo=timezone.utc),
        status="active",
    )
    remote_patch = {
        "title": "Remote title",
        "description": "Remote description",
        "status": "done",
    }
    conflicts = detect_conflicts(item, remote_patch, source="google_tasks_pull")
    assert len(conflicts) == 3
    assert {c.field_name for c in conflicts} == {"title", "description", "status"}


def test_render_conflict_question_has_choices() -> None:
    item = _FakeItem(
        id="item-2",
        title="Local",
        description=None,
        due_at=None,
        status="active",
    )
    conflicts = detect_conflicts(item, {"title": "Remote"}, source="google_sheet_pull")
    question, choices = render_conflict_question(conflicts)
    assert "конфликт" in question.lower()
    assert len(choices) == 1
    assert "title" in choices[0]["label"]
