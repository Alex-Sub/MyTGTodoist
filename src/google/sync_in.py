from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from httpx import HTTPStatusError
from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import settings
from src.db.models import CalendarSyncState, Item, ItemEvent, Project
from src.db.repositories.items_repo import create_item
from src.google.calendar_client import CalendarClient


def _parse_rfc3339(value: str | None) -> datetime | None:
    if not value:
        return None
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)


def resolve_item_id(event: dict) -> str | None:
    extended = event.get("extendedProperties", {}) or {}
    private = extended.get("private", {}) or {}
    item_id = private.get("item_id")
    if item_id:
        return item_id
    return None


def _get_sync_state(session: Session, calendar_id: str) -> CalendarSyncState:
    state = session.scalar(select(CalendarSyncState).where(CalendarSyncState.calendar_id == calendar_id))
    if state:
        return state
    state = CalendarSyncState(calendar_id=calendar_id)
    session.add(state)
    session.commit()
    session.refresh(state)
    return state


def _log_event(session: Session, item_id: str, event_type: str, meta: dict) -> None:
    session.add(
        ItemEvent(
            item_id=item_id,
            event_type=event_type,
            ts=datetime.now(timezone.utc),
            meta_json=json.dumps(meta, ensure_ascii=False),
        )
    )


def _get_default_project(session: Session) -> Project:
    project = session.scalar(select(Project).where(Project.name == "Проекты"))
    if project is None:
        project = session.scalar(select(Project).where(Project.name == "Inbox"))
    if project is None:
        project = Project(name="Inbox", is_system=True, sort_order=0)
        session.add(project)
        session.commit()
        session.refresh(project)
    return project


def _apply_remote_update(item: Item, payload: dict) -> None:
    item.title = payload["title"]
    item.description = payload["description"]
    item.scheduled_at = payload["scheduled_at"]
    item.duration_min = payload["duration_min"]
    item.etag = payload["etag"]
    item.g_updated = payload["g_updated"]
    item.event_id = payload["event_id"]
    item.ical_uid = payload["ical_uid"]
    item.sync_state = "synced"


def sync_in_calendar(session: Session, calendar_id: str = "primary") -> dict:
    stats = {
        "processed": 0,
        "created": 0,
        "updated": 0,
        "cancelled": 0,
        "conflicts": 0,
        "token_reset": 0,
    }
    client = CalendarClient()
    state = _get_sync_state(session, calendar_id)
    tz = ZoneInfo(settings.timezone)
    logger.info("sync_in start calendar_id={}", calendar_id)

    def _full_resync() -> None:
        now = datetime.now(timezone.utc)
        time_min = (now - timedelta(days=90)).isoformat()
        time_max = (now + timedelta(days=365)).isoformat()
        next_sync = _sync_loop(sync_token=None, time_min=time_min, time_max=time_max)
        if not next_sync:
            logger.warning("sync_in missing nextSyncToken, retrying without timeMax")
            next_sync = _sync_loop(sync_token=None, time_min=time_min, time_max=None)
        return next_sync

    def _sync_loop(sync_token: str | None, time_min: str | None, time_max: str | None) -> str | None:
        page_token = None
        next_sync = None
        while True:
            response = client.list_events(
                calendar_id=calendar_id,
                sync_token=sync_token,
                time_min=time_min,
                time_max=time_max,
                page_token=page_token,
            )
            for event in response.get("items", []):
                stats["processed"] += 1
                _process_event(session, calendar_id, event, stats, tz)

            page_token = response.get("nextPageToken")
            if not page_token:
                next_sync = response.get("nextSyncToken")
                logger.debug(
                    "sync_in nextSyncToken present={} calendar_id={}",
                    bool(next_sync),
                    calendar_id,
                )
                break

        if next_sync:
            state.sync_token = next_sync
            session.commit()
        return next_sync

    try:
        if state.sync_token:
            _sync_loop(sync_token=state.sync_token, time_min=None, time_max=None)
        else:
            _full_resync()
        state.last_sync_status = "ok"
        state.last_sync_error = None
        session.commit()
    except HTTPStatusError as exc:
        if exc.response.status_code == 410:
            logger.warning("Sync token expired for calendar={}, resyncing", calendar_id)
            stats["token_reset"] = 1
            state.sync_token = None
            session.commit()
            _full_resync()
            state.last_sync_status = "ok"
            state.last_sync_error = None
            session.commit()
        else:
            state.last_sync_status = "error"
            state.last_sync_error = str(exc)
            session.commit()
            raise

    logger.info("sync_in done calendar_id={} stats={}", calendar_id, stats)
    return stats


def sync_in_calendar_window(
    session: Session,
    calendar_id: str,
    window_start: datetime,
    window_end: datetime,
) -> dict:
    stats = {
        "processed": 0,
        "created": 0,
        "updated": 0,
        "cancelled": 0,
        "conflicts": 0,
        "token_reset": 0,
    }
    client = CalendarClient()
    tz = ZoneInfo(settings.timezone)

    start_iso = window_start.isoformat()
    end_iso = window_end.isoformat()
    logger.info(
        "sync_in window start={} end={} calendar_id={}",
        start_iso,
        end_iso,
        calendar_id,
    )

    page_token = None
    while True:
        response = client.list_events(
            calendar_id=calendar_id,
            sync_token=None,
            time_min=start_iso,
            time_max=end_iso,
            page_token=page_token,
        )
        for event in response.get("items", []):
            stats["processed"] += 1
            _process_event(session, calendar_id, event, stats, tz)

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    logger.info(
        "sync_in window done calendar_id={} count={} stats={}",
        calendar_id,
        stats["processed"],
        stats,
    )
    return stats


def _process_event(session: Session, calendar_id: str, event: dict, stats: dict, tz: ZoneInfo) -> None:
    event_id = event.get("id")
    status = event.get("status")
    item_id = resolve_item_id(event)

    item = None
    if item_id:
        item = session.get(Item, item_id)
    if item is None and event_id:
        item = session.scalar(select(Item).where(Item.event_id == event_id))

    if status == "cancelled":
        if item:
            item.status = "canceled"
            item.sync_state = "synced"
            item.event_id = event_id or item.event_id
            item.g_updated = _parse_rfc3339(event.get("updated"))
            item.etag = event.get("etag")
            _log_event(session, item.id, "sync_in_cancel", {"event_id": event_id})
            stats["cancelled"] += 1
            session.commit()
        return

    start = (event.get("start") or {}).get("dateTime")
    end = (event.get("end") or {}).get("dateTime")
    if not start or not end:
        return

    start_dt = _parse_rfc3339(start)
    end_dt = _parse_rfc3339(end)
    if not start_dt or not end_dt:
        return

    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=tz)
    else:
        start_dt = start_dt.astimezone(tz)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=tz)
    else:
        end_dt = end_dt.astimezone(tz)

    duration_min = int((end_dt - start_dt).total_seconds() / 60)
    summary = event.get("summary") or "(no title)"
    description = event.get("description")
    updated = _parse_rfc3339(event.get("updated"))
    etag = event.get("etag")
    ical_uid = event.get("iCalUID")

    payload = {
        "title": summary,
        "description": description,
        "scheduled_at": start_dt,
        "duration_min": duration_min,
        "etag": etag,
        "g_updated": updated,
        "event_id": event_id,
        "ical_uid": ical_uid,
    }

    if item:
        if item.sync_state == "dirty" and item.g_updated and updated and updated > item.g_updated:
            item.sync_state = "conflict"
            _log_event(
                session,
                item.id,
                "sync_in_conflict",
                {
                    "event_id": event_id,
                    "etag": etag,
                    "updated": updated.isoformat() if updated else None,
                    "remote": {
                        "summary": summary,
                        "description": description,
                        "start": start,
                        "end": end,
                        "timeZone": (event.get("start") or {}).get("timeZone") or settings.timezone,
                    },
                },
            )
            stats["conflicts"] += 1
            session.commit()
            return

        _apply_remote_update(item, payload)
        _log_event(session, item.id, "sync_in_update", {"event_id": event_id})
        stats["updated"] += 1
        session.commit()
        return

    project = _get_default_project(session)
    created = create_item(
        session,
        title=payload["title"],
        description=payload["description"],
        project_id=project.id,
        type="meeting",
        status="active",
        scheduled_at=payload["scheduled_at"],
        duration_min=payload["duration_min"],
        calendar_id=calendar_id,
        event_id=payload["event_id"],
        etag=payload["etag"],
        g_updated=payload["g_updated"],
        ical_uid=payload["ical_uid"],
        sync_state="synced",
    )
    _log_event(session, created.id, "sync_in_create", {"event_id": event_id})
    stats["created"] += 1
    session.commit()
