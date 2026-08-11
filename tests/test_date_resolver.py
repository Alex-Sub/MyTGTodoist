from __future__ import annotations

from datetime import datetime

from common.date_resolver import normalize_temporal_fields, resolve_date_phrase


def test_resolve_date_phrase_tomorrow() -> None:
    result = resolve_date_phrase("завтра", now=datetime(2026, 5, 17, 9, 0, 0))
    assert result.ok is True
    assert result.date == "2026-05-18"


def test_resolve_date_phrase_day_after_tomorrow() -> None:
    result = resolve_date_phrase("послезавтра", now=datetime(2026, 5, 17, 9, 0, 0))
    assert result.ok is True
    assert result.date == "2026-05-19"


def test_resolve_date_phrase_ddmm_uses_current_year() -> None:
    result = resolve_date_phrase("18.05", now=datetime(2026, 5, 17, 9, 0, 0))
    assert result.ok is True
    assert result.date == "2026-05-18"


def test_resolve_date_phrase_ddmmyyyy_keeps_explicit_year() -> None:
    result = resolve_date_phrase("18.05.2026", now=datetime(2026, 5, 17, 9, 0, 0))
    assert result.ok is True
    assert result.date == "2026-05-18"


def test_resolve_date_phrase_next_monday_from_sunday() -> None:
    result = resolve_date_phrase("в понедельник", now=datetime(2026, 5, 17, 9, 0, 0))
    assert result.ok is True
    assert result.date == "2026-05-18"


def test_resolve_date_phrase_following_monday_from_sunday() -> None:
    result = resolve_date_phrase("в следующий понедельник", now=datetime(2026, 5, 17, 9, 0, 0))
    assert result.ok is True
    assert result.date == "2026-05-25"


def test_resolve_date_phrase_unresolved_needs_clarification() -> None:
    result = resolve_date_phrase("на неделе", now=datetime(2026, 5, 17, 9, 0, 0))
    assert result.ok is False
    assert result.needs_clarification is True


def test_normalize_temporal_fields_canonicalizes_due_date_and_planned_at() -> None:
    normalized = normalize_temporal_fields(
        {"due_date": "завтра", "entities": {"text": "создай задачу купить молоко завтра"}},
        now=datetime(2026, 5, 17, 9, 0, 0),
    )
    assert normalized["due_date"] == "2026-05-18"
    assert normalized["planned_at"] == "2026-05-18"
    assert normalized["entities"]["due_date"] == "2026-05-18"
    assert normalized["entities"]["planned_at"] == "2026-05-18"
