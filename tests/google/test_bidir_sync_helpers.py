from __future__ import annotations

from src.google.bidir_utils import (
    _canonical_payload,
    _changed_sources,
    _parse_sheet_date_time,
    _payload_hash,
)


def test_parse_sheet_date_time_local_to_utc_iso() -> None:
    value = _parse_sheet_date_time("2026-02-28", "10:30")
    assert value.startswith("2026-02-28T07:30:00")
    assert value.endswith("+00:00")


def test_payload_hash_is_stable_for_same_normalized_values() -> None:
    a = _canonical_payload(
        title="  Встреча   с   командой ",
        notes="line1\r\nline2",
        start_at="2026-02-28T07:30:00+00:00",
        end_at="2026-02-28T08:00:00+00:00",
        status="CONFIRMED",
    )
    b = _canonical_payload(
        title="Встреча с командой",
        notes="line1\nline2",
        start_at="2026-02-28T10:30:00+03:00",
        end_at="2026-02-28T11:00:00+03:00",
        status="confirmed",
    )
    assert _payload_hash(a) == _payload_hash(b)


def test_changed_sources_detects_conflict_between_sheet_and_calendar() -> None:
    prev = {
        "last_db_hash": "same",
        "last_sheet_hash": "same",
        "last_calendar_hash": "same",
    }
    changed = _changed_sources(
        prev,
        db_hash="same",
        sheet_hash="new_sheet",
        calendar_hash="new_calendar",
    )
    assert set(changed) == {"sheet", "calendar"}
