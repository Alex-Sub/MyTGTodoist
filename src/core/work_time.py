from __future__ import annotations

import json
import math
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterable
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import settings
from src.db.models import Item, ItemEvent, Project


def resolve_item(session: Session, token: str) -> Item | None:
    token = token.strip()
    if not token:
        return None
    if len(token) >= 32:
        item = session.get(Item, token)
        if item:
            return item
    return session.scalar(select(Item).where(Item.id.like(f"{token}%")))


def _last_event(session: Session, item_id: str, event_type: str) -> ItemEvent | None:
    return session.scalar(
        select(ItemEvent)
        .where(ItemEvent.item_id == item_id, ItemEvent.event_type == event_type)
        .order_by(ItemEvent.ts.desc())
    )


def get_active_work(session: Session) -> tuple[Item, datetime] | None:
    events = session.scalars(
        select(ItemEvent)
        .where(ItemEvent.event_type.in_(("work_start", "work_stop")))
        .order_by(ItemEvent.ts)
    ).all()

    last_state: dict[str, tuple[str, datetime]] = {}
    for event in events:
        last_state[event.item_id] = (event.event_type, event.ts)

    active = [(item_id, ts) for item_id, (etype, ts) in last_state.items() if etype == "work_start"]
    if not active:
        return None

    item_id, start_ts = sorted(active, key=lambda v: v[1], reverse=True)[0]
    item = session.get(Item, item_id)
    if item is None:
        return None
    return item, start_ts


def is_item_active(session: Session, item_id: str) -> tuple[bool, datetime | None]:
    start = _last_event(session, item_id, "work_start")
    if start is None:
        return False, None

    stop = session.scalar(
        select(ItemEvent)
        .where(
            ItemEvent.item_id == item_id,
            ItemEvent.event_type == "work_stop",
            ItemEvent.ts > start.ts,
        )
        .order_by(ItemEvent.ts.desc())
    )
    if stop:
        return False, None
    return True, start.ts


def _add_work_event(session: Session, item_id: str, event_type: str, source: str) -> None:
    session.add(
        ItemEvent(
            item_id=item_id,
            event_type=event_type,
            ts=datetime.now(timezone.utc),
            meta_json=json.dumps({"source": source}),
        )
    )


def _as_aware_local(value: datetime) -> datetime:
    tz = ZoneInfo(settings.timezone)
    if value.tzinfo is None:
        return value.replace(tzinfo=tz)
    return value.astimezone(tz)


def elapsed_minutes(start_ts: datetime, now: datetime | None = None) -> int:
    now = _as_aware_local(now or datetime.now(timezone.utc))
    start = _as_aware_local(start_ts)
    return _ceil_minutes(now - start)


def calc_fact_for_day(session: Session, day: date, tz: ZoneInfo) -> dict:
    start_day = datetime.combine(day, time.min).replace(tzinfo=tz)
    end_day = start_day + timedelta(days=1)
    now = _as_aware_local(datetime.now(timezone.utc))
    now = now.astimezone(tz)

    events = session.scalars(
        select(ItemEvent)
        .where(ItemEvent.event_type.in_(("work_start", "work_stop")))
        .order_by(ItemEvent.ts)
    ).all()

    seconds_by_item: dict[str, int] = {}
    active_start: dict[str, datetime] = {}

    for event in events:
        ts = _as_aware_local(event.ts).astimezone(tz)
        if event.event_type == "work_start":
            if event.item_id not in active_start:
                active_start[event.item_id] = ts
        elif event.event_type == "work_stop":
            start_ts = active_start.pop(event.item_id, None)
            if start_ts:
                seconds_by_item[event.item_id] = seconds_by_item.get(event.item_id, 0) + _overlap_seconds(
                    start_ts, ts, start_day, end_day
                )

    active_info = None
    for item_id, start_ts in active_start.items():
        seconds_by_item[item_id] = seconds_by_item.get(item_id, 0) + _overlap_seconds(
            start_ts, now, start_day, end_day
        )

    if active_start:
        item_id, start_ts = sorted(active_start.items(), key=lambda it: it[1], reverse=True)[0]
        item = session.get(Item, item_id)
        if item:
            active_info = {
                "id_short": item.id[:8],
                "title": item.title,
                "started_at": start_ts,
                "elapsed_min": max(1, math.ceil((now - start_ts).total_seconds() / 60)),
            }

    items = session.scalars(select(Item).where(Item.id.in_(seconds_by_item.keys()))).all()
    project_map = {
        project.id: project.name for project in session.scalars(select(Project)).all()
    }

    by_project_sec: dict[str, int] = {}
    by_item = []
    total_sec = 0
    for item in items:
        sec = seconds_by_item.get(item.id, 0)
        if sec <= 0:
            continue
        total_sec += sec
        project_name = project_map.get(item.project_id or "", "")
        by_project_sec[project_name] = by_project_sec.get(project_name, 0) + sec
        by_item.append(
            {
                "id_short": item.id[:8],
                "title": item.title,
                "project": project_name,
                "min": math.ceil(sec / 60),
            }
        )

    by_project = [
        {"project": name, "min": math.ceil(sec / 60)}
        for name, sec in by_project_sec.items()
        if sec > 0
    ]

    by_project.sort(key=lambda row: row["min"], reverse=True)
    by_item.sort(key=lambda row: row["min"], reverse=True)

    return {
        "total_min": math.ceil(total_sec / 60) if total_sec > 0 else 0,
        "by_project": by_project,
        "by_item": by_item,
        "active": active_info,
    }


def _overlap_seconds(start: datetime, stop: datetime, window_start: datetime, window_end: datetime) -> int:
    if stop < start:
        return 0
    left = max(start, window_start)
    right = min(stop, window_end)
    delta = (right - left).total_seconds()
    return max(0, int(delta))


def start_work(session: Session, item: Item, source: str = "telegram") -> tuple[bool, str]:
    active, _ = is_item_active(session, item.id)
    if active:
        return False, "already_active"

    _add_work_event(session, item.id, "work_start", source)
    item.working = True
    item.work_started_at = datetime.now(timezone.utc)
    session.commit()
    session.refresh(item)
    return True, "started"


def stop_work(session: Session, item: Item, source: str = "telegram") -> tuple[bool, int, int]:
    active, start_ts = is_item_active(session, item.id)
    if not active or start_ts is None:
        return False, 0, item.actual_min or 0

    now = _as_aware_local(datetime.now(timezone.utc))
    _add_work_event(session, item.id, "work_stop", source)
    session.commit()

    total_min, _ = compute_actuals(session, [item.id], now)
    actual = total_min[item.id]
    session_min = _ceil_minutes(now - _as_aware_local(start_ts))

    item.actual_min = actual
    item.working = False
    item.work_started_at = None
    session.commit()
    session.refresh(item)
    return True, session_min, actual


def _ceil_minutes(delta) -> int:
    seconds = max(0, delta.total_seconds())
    return max(1, int(math.ceil(seconds / 60.0)))


def compute_actuals(
    session: Session,
    item_ids: Iterable[str],
    now: datetime | None = None,
) -> tuple[dict[str, int], dict[str, bool]]:
    now = _as_aware_local(now or datetime.now(timezone.utc))
    item_ids = list(item_ids)
    if not item_ids:
        return {}, {}

    events = session.scalars(
        select(ItemEvent)
        .where(
            ItemEvent.item_id.in_(item_ids),
            ItemEvent.event_type.in_(("work_start", "work_stop")),
        )
        .order_by(ItemEvent.ts)
    ).all()

    totals: dict[str, int] = {item_id: 0 for item_id in item_ids}
    active_start: dict[str, datetime | None] = {item_id: None for item_id in item_ids}

    for event in events:
        if event.event_type == "work_start":
            if active_start[event.item_id] is None:
                active_start[event.item_id] = _as_aware_local(event.ts)
        elif event.event_type == "work_stop":
            start_ts = active_start.get(event.item_id)
            if start_ts is not None:
                totals[event.item_id] += _ceil_minutes(_as_aware_local(event.ts) - start_ts)
                active_start[event.item_id] = None

    working: dict[str, bool] = {}
    for item_id, start_ts in active_start.items():
        if start_ts is not None:
            totals[item_id] += _ceil_minutes(now - start_ts)
            working[item_id] = True
        else:
            working[item_id] = False

    return totals, working
