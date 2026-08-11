from __future__ import annotations

import logging
from typing import Any

LOG = logging.getLogger(__name__)


def build_item_ical_uid(item_id: int | str) -> str:
    return f"mytgtodoist-{str(item_id).strip()}@mytgtodoist"


def _first_event_id(payload: dict[str, Any] | None) -> str | None:
    items = (payload or {}).get("items") or []
    if not isinstance(items, list) or not items:
        return None
    event_id = str((items[0] or {}).get("id") or "").strip()
    return event_id or None


def _list_by_ical_uid(service: Any, calendar_id: str, ical_uid: str) -> str | None:
    payload = (
        service.events()
        .list(
            calendarId=calendar_id,
            iCalUID=ical_uid,
            maxResults=1,
            singleEvents=False,
        )
        .execute()
    )
    return _first_event_id(payload)


def _list_by_private_item_id(service: Any, calendar_id: str, item_id: int | str) -> str | None:
    payload = (
        service.events()
        .list(
            calendarId=calendar_id,
            privateExtendedProperty=f"mytgtodoist_item_id={item_id}",
            maxResults=1,
            singleEvents=False,
        )
        .execute()
    )
    return _first_event_id(payload)


def create_or_reuse_event(
    service: Any,
    *,
    calendar_id: str,
    item_id: int | str,
    event: dict[str, Any],
    flow_id: str | None = None,
) -> tuple[str | None, str]:
    ical_uid = build_item_ical_uid(item_id)

    existing_id = _list_by_ical_uid(service, calendar_id, ical_uid)
    if existing_id:
        LOG.info("calendar_adapter_reuse flow_id=%s item_id=%s action=%s event_id=%s", flow_id or "-", item_id, "reuse_icaluid", existing_id)
        return existing_id, "reuse_icaluid"
    existing_id = _list_by_private_item_id(service, calendar_id, item_id)
    if existing_id:
        LOG.info("calendar_adapter_reuse flow_id=%s item_id=%s action=%s event_id=%s", flow_id or "-", item_id, "reuse_private_prop", existing_id)
        return existing_id, "reuse_private_prop"

    body = dict(event)
    body["iCalUID"] = ical_uid
    ext = body.get("extendedProperties")
    private = (ext or {}).get("private") if isinstance(ext, dict) else None
    private_map = dict(private) if isinstance(private, dict) else {}
    private_map["mytgtodoist_item_id"] = str(item_id)
    body["extendedProperties"] = {"private": private_map}

    try:
        created = service.events().insert(calendarId=calendar_id, body=body).execute()
    except Exception as exc:
        LOG.warning(
            "calendar_adapter_commit_failed flow_id=%s item_id=%s err=%s",
            flow_id or "-",
            item_id,
            str(exc)[:300],
        )
        status = getattr(getattr(exc, "resp", None), "status", None)
        if status in (409, 412):
            existing_id = _list_by_ical_uid(service, calendar_id, ical_uid)
            if existing_id:
                return existing_id, "reuse_after_conflict_icaluid"
            existing_id = _list_by_private_item_id(service, calendar_id, item_id)
            if existing_id:
                return existing_id, "reuse_after_conflict_private_prop"
        raise

    created_id = str((created or {}).get("id") or "").strip()
    if created_id:
        created_start = ((created or {}).get("start") or {}).get("dateTime")
        created_end = ((created or {}).get("end") or {}).get("dateTime")
        provider_tz = ((created or {}).get("start") or {}).get("timeZone") or ((created or {}).get("end") or {}).get("timeZone")
        LOG.info(
            "calendar_adapter_commit_success flow_id=%s item_id=%s event_id=%s created_start=%s created_end=%s provider_timezone=%s",
            flow_id or "-",
            item_id,
            created_id,
            created_start,
            created_end,
            provider_tz or "-",
        )
        return created_id, "insert"
    existing_id = _list_by_ical_uid(service, calendar_id, ical_uid)
    if existing_id:
        LOG.info("calendar_adapter_reuse flow_id=%s item_id=%s action=%s event_id=%s", flow_id or "-", item_id, "reuse_post_insert_icaluid", existing_id)
        return existing_id, "reuse_post_insert_icaluid"
    existing_id = _list_by_private_item_id(service, calendar_id, item_id)
    if existing_id:
        LOG.info("calendar_adapter_reuse flow_id=%s item_id=%s action=%s event_id=%s", flow_id or "-", item_id, "reuse_post_insert_private_prop", existing_id)
        return existing_id, "reuse_post_insert_private_prop"
    return None, "none"
