from __future__ import annotations

from typing import Any

from organizer_worker.google_calendar_idempotency import create_or_reuse_event


class _Req:
    def __init__(self, fn):
        self._fn = fn

    def execute(self):
        return self._fn()


class _FakeEvents:
    def __init__(self) -> None:
        self._rows: list[dict[str, Any]] = []
        self.insert_calls = 0

    def list(
        self,
        *,
        calendarId: str,
        iCalUID: str | None = None,
        privateExtendedProperty: str | None = None,
        maxResults: int | None = None,
        singleEvents: bool | None = None,
    ) -> _Req:
        del calendarId, maxResults, singleEvents

        def _run():
            rows = self._rows
            if iCalUID:
                rows = [r for r in rows if str(r.get("iCalUID") or "") == iCalUID]
            if privateExtendedProperty:
                key, value = privateExtendedProperty.split("=", 1)
                rows = [
                    r
                    for r in rows
                    if str(((r.get("extendedProperties") or {}).get("private") or {}).get(key) or "")
                    == value
                ]
            items = [{"id": r["id"]} for r in rows[:1]]
            return {"items": items}

        return _Req(_run)

    def insert(self, *, calendarId: str, body: dict[str, Any]) -> _Req:
        del calendarId

        def _run():
            self.insert_calls += 1
            event_id = f"evt-{self.insert_calls}"
            row = dict(body)
            row["id"] = event_id
            self._rows.append(row)
            return {"id": event_id}

        return _Req(_run)


class _FakeService:
    def __init__(self) -> None:
        self._events = _FakeEvents()

    def events(self) -> _FakeEvents:
        return self._events


def test_create_or_reuse_event_second_call_reuses_existing() -> None:
    service = _FakeService()
    event = {
        "summary": "Meet",
        "start": {"dateTime": "2026-03-01T10:00:00+00:00", "timeZone": "UTC"},
        "end": {"dateTime": "2026-03-01T10:30:00+00:00", "timeZone": "UTC"},
    }

    first_id = create_or_reuse_event(
        service,
        calendar_id="calendar-1",
        item_id=42,
        event=event,
    )
    second_id = create_or_reuse_event(
        service,
        calendar_id="calendar-1",
        item_id=42,
        event=event,
    )

    assert first_id == "evt-1"
    assert second_id == "evt-1"
    assert service.events().insert_calls == 1
    saved = service.events()._rows[0]
    private = ((saved.get("extendedProperties") or {}).get("private") or {})
    assert private.get("mytgtodoist_item_id") == "42"
