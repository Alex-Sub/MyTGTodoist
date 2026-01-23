from __future__ import annotations

from dataclasses import dataclass
from datetime import date as date_type
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import select

from src.config import settings
from src.db.models import Item
from src.db.session import get_session


@dataclass(frozen=True)
class Event:
    item_id: str
    title: str
    start: datetime
    end: datetime


def find_events_for_day(day: date_type) -> list[Event]:
    tz = ZoneInfo(settings.timezone)
    start = datetime.combine(day, time.min, tzinfo=tz)
    end = start + timedelta(days=1)
    events: list[Event] = []
    with get_session() as session:
        items = list(
            session.scalars(
                select(Item).where(
                    Item.type == "meeting",
                    Item.status != "canceled",
                    Item.scheduled_at.is_not(None),
                    Item.scheduled_at >= start,
                    Item.scheduled_at < end,
                )
            ).all()
        )
        for item in items:
            scheduled = item.scheduled_at
            if scheduled is None:
                continue
            if scheduled.tzinfo is None:
                scheduled = scheduled.replace(tzinfo=tz)
            else:
                scheduled = scheduled.astimezone(tz)
            duration = item.duration_min if item.duration_min is not None else 60
            events.append(
                Event(
                    item_id=item.id,
                    title=item.title or "",
                    start=scheduled,
                    end=scheduled + timedelta(minutes=duration),
                )
            )
    return events


def has_time_conflict(
    candidate_start: datetime,
    candidate_end: datetime,
    events: list[Event],
) -> tuple[bool, list[Event]]:
    conflicts: list[Event] = []
    for event in events:
        if event.start < candidate_end and candidate_start < event.end:
            conflicts.append(event)
    return bool(conflicts), conflicts


def _tokenize(text: str) -> set[str]:
    parts = []
    current = []
    for ch in text.lower():
        if ch.isalnum():
            current.append(ch)
        else:
            if current:
                parts.append("".join(current))
                current = []
    if current:
        parts.append("".join(current))
    return {p for p in parts if len(p) >= 3}


def find_similar_title(candidate_title: str, events: list[Event]) -> tuple[Event | None, float]:
    if not candidate_title:
        return None, 0.0
    cand_norm = candidate_title.strip().lower()
    cand_tokens = _tokenize(candidate_title)
    best_event: Event | None = None
    best_score = 0.0
    for event in events:
        title = event.title or ""
        norm = title.strip().lower()
        if not norm:
            continue
        if cand_norm in norm or norm in cand_norm:
            return event, 1.0
        tokens = _tokenize(title)
        if not tokens or not cand_tokens:
            continue
        overlap = len(tokens & cand_tokens)
        score = overlap / max(1, len(tokens | cand_tokens))
        if score > best_score:
            best_score = score
            best_event = event
    return best_event, best_score
