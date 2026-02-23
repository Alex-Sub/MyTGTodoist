from __future__ import annotations

from typing import Any


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
) -> str | None:
    ical_uid = build_item_ical_uid(item_id)

    existing_id = _list_by_ical_uid(service, calendar_id, ical_uid)
    if existing_id:
        return existing_id
    existing_id = _list_by_private_item_id(service, calendar_id, item_id)
    if existing_id:
        return existing_id

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
        status = getattr(getattr(exc, "resp", None), "status", None)
        if status in (409, 412):
            existing_id = _list_by_ical_uid(service, calendar_id, ical_uid)
            if existing_id:
                return existing_id
            existing_id = _list_by_private_item_id(service, calendar_id, item_id)
            if existing_id:
                return existing_id
        raise

    created_id = str((created or {}).get("id") or "").strip()
    if created_id:
        return created_id
    existing_id = _list_by_ical_uid(service, calendar_id, ical_uid)
    if existing_id:
        return existing_id
    return _list_by_private_item_id(service, calendar_id, item_id)
