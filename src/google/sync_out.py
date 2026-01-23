from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from loguru import logger
from httpx import HTTPStatusError
from sqlalchemy.orm import Session

from src.config import settings
from src.db.models import Item, ItemEvent
from src.google.calendar_client import CalendarClient


def _parse_google_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)


def _ensure_tz(dt: datetime, tz: ZoneInfo) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


def _build_event_payload(item: Item) -> dict:
    tz = ZoneInfo(settings.timezone)
    scheduled_at = item.scheduled_at
    if scheduled_at is None:
        raise ValueError("Meeting scheduled_at is required for sync")

    start = _ensure_tz(scheduled_at, tz)
    duration_min = item.duration_min or 60
    end = start + timedelta(minutes=duration_min)

    payload = {
        "summary": item.title,
        "start": {"dateTime": start.isoformat(), "timeZone": settings.timezone},
        "end": {"dateTime": end.isoformat(), "timeZone": settings.timezone},
        "extendedProperties": {"private": {"item_id": item.id}},
        "reminders": {"useDefault": True},
    }
    if item.description:
        payload["description"] = item.description
    return payload


def sync_out_meeting(session: Session, item_id: str) -> Item:
    item = session.get(Item, item_id)
    if item is None:
        raise ValueError("Item not found")
    if item.type != "meeting":
        raise ValueError("Item type is not meeting")

    calendar_id = item.calendar_id or settings.google_calendar_id_default
    client = CalendarClient()
    payload = _build_event_payload(item)

    action = None
    result = None

    try:
        if item.status == "canceled" and item.event_id:
            result = client.cancel_event(calendar_id, item.event_id)
            action = "cancel"
        elif not item.event_id:
            logger.debug("Google event payload start={}, end={}", payload["start"], payload["end"])
            result = client.create_event(calendar_id, payload)
            action = "create"
        elif item.sync_state in {"dirty", "conflict"}:
            logger.debug("Google event payload start={}, end={}", payload["start"], payload["end"])
            result = client.update_event(
                calendar_id,
                item.event_id,
                payload,
                if_match_etag=item.etag,
            )
            action = "update"
        else:
            action = "skip"
    except HTTPStatusError as exc:
        if exc.response.status_code in {409, 412}:
            item.sync_state = "conflict"
            session.commit()
            logger.warning(
                "sync_out_meeting conflict item={} status={}",
                item.id,
                exc.response.status_code,
            )
            return item
        raise

    if result:
        item.event_id = result.get("event_id") or item.event_id
        item.etag = result.get("etag") or item.etag
        item.ical_uid = result.get("ical_uid") or item.ical_uid
        updated = _parse_google_ts(result.get("updated"))
        if updated:
            item.g_updated = updated
        item.sync_state = "synced"

        session.add(
            ItemEvent(
                item_id=item.id,
                event_type="sync_out",
                ts=datetime.now(timezone.utc),
                meta_json=json.dumps({"action": action, "event_id": item.event_id}),
            )
        )
        session.commit()
        session.refresh(item)

        logger.info(
            "sync_out_meeting item={} action={} calendar_id={}",
            item.id,
            action,
            calendar_id,
        )
    return item
