import importlib.util
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKER_PATH = ROOT / "organizer-worker" / "worker.py"

spec = importlib.util.spec_from_file_location("organizer_worker_runtime", WORKER_PATH)
assert spec is not None and spec.loader is not None
worker_runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker_runtime)


def _runtime_tz() -> timezone:
    return timezone(timedelta(minutes=worker_runtime.LOCAL_TZ_OFFSET_MIN))


def test_extract_datetime_day_only_no_legacy_0600_marker() -> None:
    now_local = datetime(2026, 3, 2, 12, 0, tzinfo=_runtime_tz())
    dt = worker_runtime._extract_datetime("встреча 3-го", now_local)
    assert dt is not None
    assert dt.date().isoformat() == "2026-03-03"
    assert (dt.hour, dt.minute) == (
        worker_runtime.DEFAULT_HOUR,
        worker_runtime.DEFAULT_MINUTE,
    )


def test_extract_datetime_relative_period_uses_default_time() -> None:
    now_local = datetime(2026, 3, 2, 12, 0, tzinfo=_runtime_tz())
    dt = worker_runtime._extract_datetime("на следующей неделе", now_local)
    assert dt is not None
    assert (dt.hour, dt.minute) == (
        worker_runtime.DEFAULT_HOUR,
        worker_runtime.DEFAULT_MINUTE,
    )


def test_normalize_health_services_supports_contract_and_legacy_payloads() -> None:
    contract_payload = {
        "services": {
            "asr": "ok",
            "llm": "down",
            "embedding": "ok",
        }
    }
    assert worker_runtime._normalize_health_services(contract_payload) == {
        "asr": "ok",
        "llm": "down",
        "embedding": "ok",
    }

    legacy_payload = {
        "asr": "configured",
        "llm": "up",
    }
    assert worker_runtime._normalize_health_services(legacy_payload) == {
        "asr": "ok",
        "llm": "ok",
        "embedding": "down",
    }


def _prepare_queue_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS inbox_queue (
                id INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                tg_chat_id INTEGER,
                tg_update_id INTEGER,
                tg_message_id INTEGER,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'NEW',
                priority INTEGER NOT NULL DEFAULT 100,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NULL,
                claimed_by TEXT NULL,
                claimed_at TEXT NULL,
                lease_until TEXT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                ingested_at TEXT NULL
            );
            """
        )
        conn.commit()


def test_queue_requeue_failed_marks_stale_rows_dead(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "queue.db"
    _prepare_queue_db(db_path)
    monkeypatch.setattr(worker_runtime, "DB_PATH", str(db_path))
    monkeypatch.setattr(worker_runtime, "B2_REPLAY_MAX_AGE_SEC", 60)
    monkeypatch.setattr(worker_runtime, "B2_REPLAY_SCAN_LIMIT", 50)
    monkeypatch.setattr(worker_runtime, "B2_MAX_ATTEMPTS", 5)

    now = datetime.now(timezone.utc)
    stale_created = (now - timedelta(minutes=20)).isoformat().replace("+00:00", "Z")
    fresh_created = (now - timedelta(seconds=20)).isoformat().replace("+00:00", "Z")

    with worker_runtime._get_conn() as conn:
        conn.execute(
            """
            INSERT INTO inbox_queue (
                source, tg_chat_id, tg_update_id, tg_message_id, kind, payload_json,
                status, attempts, created_at, updated_at, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'FAILED', ?, ?, ?, ?)
            """,
            ("telegram", 1, 10, 10, "text", '{"text":"old"}', 1, stale_created, stale_created, stale_created),
        )
        conn.execute(
            """
            INSERT INTO inbox_queue (
                source, tg_chat_id, tg_update_id, tg_message_id, kind, payload_json,
                status, attempts, created_at, updated_at, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'FAILED', ?, ?, ?, ?)
            """,
            ("telegram", 1, 11, 11, "text", '{"text":"new"}', 1, fresh_created, fresh_created, fresh_created),
        )
        conn.commit()

    moved = worker_runtime._queue_requeue_failed(limit=10)
    assert moved == 1

    with worker_runtime._get_conn() as conn:
        rows = conn.execute("SELECT tg_update_id, status, last_error FROM inbox_queue ORDER BY tg_update_id").fetchall()
        got = {int(row["tg_update_id"]): (str(row["status"]), str(row["last_error"] or "")) for row in rows}

    assert got[10][0] == "DEAD"
    assert got[10][1] == "replay_expired_failed"
    assert got[11][0] == "NEW"


def test_queue_claim_skips_stale_new_rows(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "queue_claim.db"
    _prepare_queue_db(db_path)
    monkeypatch.setattr(worker_runtime, "DB_PATH", str(db_path))
    monkeypatch.setattr(worker_runtime, "B2_REPLAY_MAX_AGE_SEC", 60)
    monkeypatch.setattr(worker_runtime, "B2_REPLAY_SCAN_LIMIT", 50)
    monkeypatch.setattr(worker_runtime, "B2_CLAIM_LEASE_SEC", 120)
    monkeypatch.setattr(worker_runtime, "WORKER_ID", "test-worker")

    now = datetime.now(timezone.utc)
    stale_created = (now - timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
    fresh_created = (now - timedelta(seconds=10)).isoformat().replace("+00:00", "Z")
    with worker_runtime._get_conn() as conn:
        conn.execute(
            """
            INSERT INTO inbox_queue (
                source, tg_chat_id, tg_update_id, tg_message_id, kind, payload_json,
                status, attempts, priority, created_at, updated_at, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'NEW', 0, 100, ?, ?, ?)
            """,
            ("telegram", 1, 21, 21, "text", '{"text":"old"}', stale_created, stale_created, stale_created),
        )
        conn.execute(
            """
            INSERT INTO inbox_queue (
                source, tg_chat_id, tg_update_id, tg_message_id, kind, payload_json,
                status, attempts, priority, created_at, updated_at, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'NEW', 0, 100, ?, ?, ?)
            """,
            ("telegram", 1, 22, 22, "text", '{"text":"new"}', fresh_created, fresh_created, fresh_created),
        )
        conn.commit()

    claimed = worker_runtime._queue_claim()
    assert isinstance(claimed, dict)
    assert int(claimed["tg_update_id"]) == 22

    with worker_runtime._get_conn() as conn:
        stale = conn.execute("SELECT status, last_error FROM inbox_queue WHERE tg_update_id=21").fetchone()
    assert stale is not None
    assert str(stale["status"]) == "DEAD"
    assert str(stale["last_error"]) == "replay_expired_new"


def test_runtime_temporal_window_local_preserves_time_and_duration() -> None:
    start, end, duration = worker_runtime._runtime_temporal_window_local(
        {"start_at": "2026-04-13T14:00:00+03:00", "duration_minutes": 60},
        fallback_minutes=30,
    )
    assert start is not None and end is not None
    assert duration == 60
    assert start.isoformat() == "2026-04-13T14:00:00+03:00"
    assert end.isoformat() == "2026-04-13T15:00:00+03:00"


def test_calendar_datetime_payload_uses_datetime_not_date_fields() -> None:
    start = datetime.fromisoformat("2026-04-13T14:00:00+03:00")
    end = datetime.fromisoformat("2026-04-13T15:00:00+03:00")
    start_payload, end_payload = worker_runtime._calendar_datetime_payload(start, end)

    assert "dateTime" in start_payload and "date" not in start_payload
    assert "dateTime" in end_payload and "date" not in end_payload
    assert start_payload["dateTime"] == "2026-04-13T14:00:00+03:00"
    assert end_payload["dateTime"] == "2026-04-13T15:00:00+03:00"


def test_meeting_update_time_only_keeps_source_date_and_duration(monkeypatch) -> None:
    captured: dict[str, datetime] = {}

    monkeypatch.setattr(
        worker_runtime,
        "_calendar_get_event",
        lambda event_id: {
            "ok": True,
            "event_id": event_id,
            "event_start": "2026-04-13T15:00:00+03:00",
            "event_end": "2026-04-13T16:00:00+03:00",
            "time_zone": "Europe/Moscow",
        },
    )

    def _patch(event_id: str, start: datetime, end: datetime) -> dict:
        captured["start"] = start
        captured["end"] = end
        return {"ok": True}

    monkeypatch.setattr(worker_runtime, "_calendar_patch_event", _patch)

    out = worker_runtime._runtime_commit_meeting_update_calendar(
        flow_id="flow-test-time-only",
        entities={
            "calendar_event_id": "evt-1",
            "start_at": "2026-04-13",
            "start_at_time": "16",
        },
        execution_result={},
    )

    assert bool(out.get("ok")) is True
    assert captured["start"].isoformat() == "2026-04-13T16:00:00+03:00"
    assert captured["end"].isoformat() == "2026-04-13T17:00:00+03:00"


def test_meeting_update_date_only_keeps_source_time(monkeypatch) -> None:
    captured: dict[str, datetime] = {}

    monkeypatch.setattr(
        worker_runtime,
        "_calendar_get_event",
        lambda event_id: {
            "ok": True,
            "event_id": event_id,
            "event_start": "2026-04-13T15:00:00+03:00",
            "event_end": "2026-04-13T16:30:00+03:00",
            "time_zone": "Europe/Moscow",
        },
    )

    def _patch(event_id: str, start: datetime, end: datetime) -> dict:
        captured["start"] = start
        captured["end"] = end
        return {"ok": True}

    monkeypatch.setattr(worker_runtime, "_calendar_patch_event", _patch)

    out = worker_runtime._runtime_commit_meeting_update_calendar(
        flow_id="flow-test-date-only",
        entities={
            "calendar_event_id": "evt-2",
            "start_at_date": "2026-04-14",
        },
        execution_result={},
    )

    assert bool(out.get("ok")) is True
    assert captured["start"].isoformat() == "2026-04-14T15:00:00+03:00"
    assert captured["end"].isoformat() == "2026-04-14T16:30:00+03:00"
