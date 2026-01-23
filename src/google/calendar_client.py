from __future__ import annotations

from typing import Any, Callable, Optional

import httpx

from src.google.auth import get_access_token

GOOGLE_CALENDAR_API = "https://www.googleapis.com/calendar/v3"

AccessTokenProvider = Callable[[], str]


class CalendarClient:
    def __init__(self, access_token_provider: Optional[AccessTokenProvider] = None) -> None:
        self._access_token_provider = access_token_provider or get_access_token

    def _headers(self, if_match_etag: Optional[str] = None) -> dict[str, str]:
        access_token = self._access_token_provider()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }
        if if_match_etag:
            headers["If-Match"] = if_match_etag
        return headers

    def list_calendars(self) -> dict[str, Any]:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(
                f"{GOOGLE_CALENDAR_API}/users/me/calendarList",
                headers=self._headers(),
            )
            resp.raise_for_status()
            return resp.json()

    def create_event(self, calendar_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(
                f"{GOOGLE_CALENDAR_API}/calendars/{calendar_id}/events",
                headers=self._headers(),
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()

        return {
            "event_id": data.get("id"),
            "etag": data.get("etag"),
            "updated": data.get("updated"),
            "ical_uid": data.get("iCalUID"),
        }

    def update_event(
        self,
        calendar_id: str,
        event_id: str,
        payload: dict[str, Any],
        if_match_etag: Optional[str] = None,
    ) -> dict[str, Any]:
        with httpx.Client(timeout=10.0) as client:
            resp = client.patch(
                f"{GOOGLE_CALENDAR_API}/calendars/{calendar_id}/events/{event_id}",
                headers=self._headers(if_match_etag=if_match_etag),
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()

        return {
            "event_id": data.get("id"),
            "etag": data.get("etag"),
            "updated": data.get("updated"),
            "ical_uid": data.get("iCalUID"),
        }

    def cancel_event(
        self,
        calendar_id: str,
        event_id: str,
    ) -> dict[str, Any]:
        payload = {"status": "cancelled"}
        with httpx.Client(timeout=10.0) as client:
            resp = client.patch(
                f"{GOOGLE_CALENDAR_API}/calendars/{calendar_id}/events/{event_id}",
                headers=self._headers(),
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()

        return {
            "event_id": data.get("id"),
            "etag": data.get("etag"),
            "updated": data.get("updated"),
            "ical_uid": data.get("iCalUID"),
        }

    def get_event(self, calendar_id: str, event_id: str) -> dict[str, Any]:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(
                f"{GOOGLE_CALENDAR_API}/calendars/{calendar_id}/events/{event_id}",
                headers=self._headers(),
            )
            resp.raise_for_status()
            return resp.json()

    def list_events(
        self,
        calendar_id: str,
        sync_token: Optional[str] = None,
        time_min: Optional[str] = None,
        time_max: Optional[str] = None,
        page_token: Optional[str] = None,
    ) -> dict[str, Any]:
        if sync_token and (time_min or time_max):
            raise ValueError("sync_token cannot be combined with timeMin/timeMax")
        params: dict[str, Any] = {
            "singleEvents": "true",
            "showDeleted": "true",
            "maxResults": 250,
        }
        if sync_token:
            params["syncToken"] = sync_token
        if time_min:
            params["timeMin"] = time_min
            params["orderBy"] = "startTime"
        if time_max:
            params["timeMax"] = time_max
        if page_token:
            params["pageToken"] = page_token

        with httpx.Client(timeout=10.0) as client:
            resp = client.get(
                f"{GOOGLE_CALENDAR_API}/calendars/{calendar_id}/events",
                headers=self._headers(),
                params=params,
            )
            resp.raise_for_status()
            return resp.json()
