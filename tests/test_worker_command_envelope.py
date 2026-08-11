import json
import sqlite3
import sys
import threading
import urllib.request
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "migrations"
WORKER_ROOT = ROOT / "organizer-worker"

sys.path.append(str(WORKER_ROOT))

import worker  # noqa: E402
from organizer_worker import canon, db  # noqa: E402


def _apply_runtime_migrations(conn: sqlite3.Connection) -> None:
    migs = sorted(p for p in MIGRATIONS_DIR.iterdir() if p.name.endswith(".sql"))
    for path in migs:
        try:
            num = int(path.name.split("_", 1)[0])
        except Exception:
            continue
        if num < 1 or num > 26:
            continue
        conn.executescript(path.read_text(encoding="utf-8"))
    conn.commit()


def _patch_runtime_trace_rows(
    monkeypatch: pytest.MonkeyPatch,
    *,
    user_id: str,
    event_ids: list[str],
) -> None:
    rows: list[tuple[str, dict]] = []
    for idx, event_id in enumerate(event_ids, start=1):
        rows.append(
            (
                f"trace-{user_id}-{idx}",
                {
                    "calendar_event_id": event_id,
                    "debug": {"user_id": user_id},
                    "title": "Встреча",
                },
            )
        )

    monkeypatch.setattr(
        worker,
        "_runtime_trace_list_rows_for_user",
        lambda _user_id, limit=200: rows if str(_user_id or "").strip() == user_id else [],  # noqa: ARG005
    )


@pytest.fixture()
def runtime_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "runtime_envelope.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    worker.DB_PATH = str(db_path)
    db.DB_PATH = str(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
    return db_path


def test_post_command_envelope_returns_clarification_with_choices(runtime_db: Path) -> None:
    with db.connect() as conn:
        db.create_task(conn, title="Отчет для клиента")
        db.create_task(conn, title="Отчет для команды")

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"
        payload = {
            "trace_id": "t-1",
            "command": {
                "intent": "task.set_status",
                "entities": {
                    "task_ref": {"query": "Отчет"},
                    "status": "пауза",
                },
            },
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert body.get("ok") is False
    assert bool(body.get("clarifying_question"))
    choices = body.get("choices")
    assert isinstance(choices, list)
    assert 1 <= len(choices) <= canon.get_disambiguation_top_k()
    assert all(isinstance(ch.get("id"), int) and isinstance(ch.get("label"), str) for ch in choices)


def test_post_command_envelope_trace_id_dedup_returns_cached_response(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict] = []

    def _fake_dispatch(command: dict) -> dict:
        calls.append(command)
        return {"ok": True, "user_message": f"call-{len(calls)}"}

    monkeypatch.setattr(worker, "dispatch_intent", _fake_dispatch)
    monkeypatch.setattr(worker, "RUNTIME_TRACE_DEDUP_TTL_SEC", 3600)

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"
        payload = {
            "trace_id": "trace-dedup-1",
            "command": {"intent": "tasks.list_active", "entities": {}},
        }
        req1 = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req1, timeout=3) as resp:
            body1 = json.loads(resp.read().decode("utf-8"))

        payload2 = {
            "trace_id": "trace-dedup-1",
            "command": {"intent": "tasks.list_active", "entities": {"changed": True}},
        }
        req2 = urllib.request.Request(
            url,
            data=json.dumps(payload2, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req2, timeout=3) as resp:
            body2 = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert len(calls) == 1
    assert body1.get("user_message") == "call-1"
    assert body2.get("ok") is True
    assert body2.get("user_message") == body1.get("user_message")
    trace_snapshot = body2.get("__trace_snapshot") if isinstance(body2.get("__trace_snapshot"), dict) else {}
    assert trace_snapshot.get("trace_id") == "trace-dedup-1"
    assert trace_snapshot.get("intent") == "tasks.list_active"


def test_runtime_command_idempotency_prevents_duplicate_entity_for_same_message(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict] = []

    def _fake_dispatch(command: dict) -> dict:
        calls.append(command)
        return {"ok": True, "user_message": "created", "task_id": 101}

    monkeypatch.setattr(worker, "dispatch_intent", _fake_dispatch)

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"
        payload_1 = {
            "trace_id": "tr-idem-1",
            "source": "telegram-bot",
            "command": {
                "intent": "task.create",
                "entities": {"title": "Купить молоко", "source_msg_id": "tg:123456:12573"},
            },
        }
        req1 = urllib.request.Request(
            url,
            data=json.dumps(payload_1, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req1, timeout=3) as resp:
            body1 = json.loads(resp.read().decode("utf-8"))

        payload_2 = {
            "trace_id": "tr-idem-2",
            "source": "telegram-bot",
            "command": {
                "intent": "task.create",
                "entities": {"title": "Купить молоко", "source_msg_id": "tg:123456:12573"},
            },
        }
        req2 = urllib.request.Request(
            url,
            data=json.dumps(payload_2, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req2, timeout=3) as resp:
            body2 = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert len(calls) == 1
    assert body1.get("task_id") == 101
    assert body2.get("already_executed") is True
    assert body2.get("task_id") == 101
    assert body2.get("entity_type") == "task"
    assert body2.get("entity_id") == "101"


def test_runtime_command_idempotency_survives_worker_restart(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict] = []

    def _fake_dispatch(command: dict) -> dict:
        calls.append(command)
        return {"ok": True, "user_message": "created", "time_block_id": 77, "task_id": 42}

    monkeypatch.setattr(worker, "dispatch_intent", _fake_dispatch)
    monkeypatch.setattr(worker, "_create_event", lambda *args, **kwargs: "evt-restart-1")

    def _send(port: int, trace_id: str) -> dict:
        url = f"http://127.0.0.1:{port}/runtime/command"
        payload = {
            "trace_id": trace_id,
            "source": "telegram-bot",
            "command": {
                "intent": "timeblock.create",
                "entities": {
                    "task_id": 42,
                    "start_at": "2026-03-10T11:00:00+03:00",
                    "duration_minutes": 30,
                    "source_msg_id": "tg:999:321",
                },
            },
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            return json.loads(resp.read().decode("utf-8"))

    server_1 = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th_1 = threading.Thread(target=server_1.serve_forever, daemon=True)
    th_1.start()
    body1 = _send(int(server_1.server_address[1]), "tr-restart-1")
    server_1.shutdown()
    server_1.server_close()

    server_2 = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th_2 = threading.Thread(target=server_2.serve_forever, daemon=True)
    th_2.start()
    try:
        body2 = _send(int(server_2.server_address[1]), "tr-restart-2")
    finally:
        server_2.shutdown()
        server_2.server_close()

    assert len(calls) == 1
    assert body1.get("time_block_id") == 77
    assert body2.get("already_executed") is True
    assert body2.get("time_block_id") == 77
    assert body2.get("entity_type") == "timeblock"


def test_runtime_clarification_flow_reuses_original_idempotency_key(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict] = []

    def _fake_dispatch(command: dict) -> dict:
        calls.append(command)
        if len(calls) == 1:
            return {
                "ok": False,
                "clarifying_question": "На сколько минут поставить блок?",
                "debug": {"missing": "duration_minutes"},
            }
        return {"ok": True, "user_message": "created", "time_block_id": 555, "task_id": 10}

    monkeypatch.setattr(worker, "dispatch_intent", _fake_dispatch)
    monkeypatch.setattr(worker, "_create_event", lambda *args, **kwargs: "evt-clarify-1")

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"
        payload_1 = {
            "trace_id": "tr-clarify-1",
            "source": "telegram-bot",
            "command": {
                "intent": "timeblock.create",
                "entities": {
                    "task_id": 10,
                    "start_at": "2026-03-10T11:00:00+03:00",
                    "source_msg_id": "tg:42:500",
                },
            },
        }
        req1 = urllib.request.Request(
            url,
            data=json.dumps(payload_1, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req1, timeout=3) as resp:
            body1 = json.loads(resp.read().decode("utf-8"))

        payload_2 = {
            "trace_id": "tr-clarify-2",
            "source": "telegram-bot",
            "command": {
                "intent": "timeblock.create",
                "entities": {
                    "task_id": 10,
                    "start_at": "2026-03-10T11:00:00+03:00",
                    "duration_minutes": 30,
                    "source_msg_id": "tg:42:500",
                },
            },
        }
        req2 = urllib.request.Request(
            url,
            data=json.dumps(payload_2, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req2, timeout=3) as resp:
            body2 = json.loads(resp.read().decode("utf-8"))

        payload_3 = {
            "trace_id": "tr-clarify-3",
            "source": "telegram-bot",
            "command": {
                "intent": "timeblock.create",
                "entities": {
                    "task_id": 10,
                    "start_at": "2026-03-10T11:00:00+03:00",
                    "duration_minutes": 30,
                    "source_msg_id": "tg:42:500",
                },
            },
        }
        req3 = urllib.request.Request(
            url,
            data=json.dumps(payload_3, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req3, timeout=3) as resp:
            body3 = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert len(calls) == 2
    assert body1.get("ok") is False
    assert bool(body1.get("clarifying_question"))
    assert body2.get("time_block_id") == 555
    assert body3.get("already_executed") is True
    assert body3.get("time_block_id") == 555


def test_runtime_meeting_update_with_confirmed_target_patches_not_creates(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    create_calls: list[dict] = []
    patch_calls: list[dict] = []

    def _fake_create(item_id, title, start, end, description=None, flow_id=None):  # noqa: ANN001
        create_calls.append(
            {
                "item_id": item_id,
                "title": title,
                "start": start,
                "end": end,
                "description": description,
                "flow_id": flow_id,
            }
        )
        return "evt-9001"

    def _fake_patch(event_id, start, end):  # noqa: ANN001
        patch_calls.append({"event_id": event_id, "start": start, "end": end})
        return "ok"

    def _fake_get(event_id):  # noqa: ANN001
        return {
            "ok": True,
            "event_id": event_id,
            "event_start": "2026-04-13T14:00:00+03:00",
            "event_end": "2026-04-13T15:00:00+03:00",
            "time_zone": "Europe/Moscow",
        }

    monkeypatch.setattr(worker, "_create_event", _fake_create)
    monkeypatch.setattr(worker, "_patch_event", _fake_patch)
    monkeypatch.setattr(worker, "_calendar_get_event", _fake_get)

    caplog.set_level("INFO")
    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"

        create_payload = {
            "trace_id": "meeting-create-1",
            "source": {"user_id": "u-meeting-1", "timezone": "Europe/Moscow"},
            "command": {
                "intent": "meeting.create",
                "entities": {
                    "user_id": "u-meeting-1",
                    "start_at": "2026-04-13T14:00:00+03:00",
                    "duration_minutes": 60,
                    "text": "назначь встречу завтра в 14 на 60 минут",
                },
            },
        }
        req1 = urllib.request.Request(
            url,
            data=json.dumps(create_payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req1, timeout=3) as resp:
            body1 = json.loads(resp.read().decode("utf-8"))

        update_payload = {
            "trace_id": "meeting-update-1",
            "source": {"user_id": "u-meeting-1", "timezone": "Europe/Moscow"},
            "command": {
                "intent": "meeting.update",
                "entities": {
                    "calendar_event_id": "evt-9001",
                    "user_id": "u-meeting-1",
                    "start_at_time": "16",
                    "text": "перенеси на 16",
                },
            },
        }
        req2 = urllib.request.Request(
            url,
            data=json.dumps(update_payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req2, timeout=3) as resp:
            body2 = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert body1.get("ok") is True
    assert body1.get("calendar_event_id") == "evt-9001"
    assert body2.get("ok") is True
    assert body2.get("calendar_event_id") == "evt-9001"
    assert len(create_calls) == 1
    assert len(patch_calls) == 1
    patched = patch_calls[0]
    assert patched["event_id"] == "evt-9001"
    patched_start = patched["start"]
    assert isinstance(patched_start, datetime)
    assert patched_start.strftime("%Y-%m-%d %H:%M") == "2026-04-13 16:00"
    patched_end = patched["end"]
    assert isinstance(patched_end, datetime)
    assert patched_end.strftime("%Y-%m-%d %H:%M") == "2026-04-13 17:00"
    dispatch_record = next((r for r in caplog.records if "runtime_intent_dispatch_start" in r.getMessage() and "intent=meeting.update" in r.getMessage()), None)
    assert dispatch_record is not None
    assert "commit_path=calendar_patch" in dispatch_record.getMessage()
    commit_record = next((r for r in caplog.records if "runtime_commit_path_result" in r.getMessage() and "intent=meeting.update" in r.getMessage()), None)
    assert commit_record is not None
    assert "commit_path=calendar_patch" in commit_record.getMessage()
    patch_record = next((r for r in caplog.records if "meeting_update_patch_result" in r.getMessage()), None)
    assert patch_record is not None


def test_runtime_meeting_update_without_target_confirmation_does_not_patch_latest_meeting(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_calls: list[dict] = []
    patch_calls: list[dict] = []

    def _fake_create(item_id, title, start, end, description=None, flow_id=None):  # noqa: ANN001
        create_calls.append({"item_id": item_id, "start": start, "end": end, "flow_id": flow_id})
        return "evt-9100"

    def _fake_patch(event_id, start, end):  # noqa: ANN001
        patch_calls.append({"event_id": event_id, "start": start, "end": end})
        return "ok"

    monkeypatch.setattr(worker, "_create_event", _fake_create)
    monkeypatch.setattr(worker, "_patch_event", _fake_patch)

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"

        create_payload = {
            "trace_id": "meeting-create-no-target-1",
            "source": {"user_id": "u-meeting-no-target", "timezone": "Europe/Moscow"},
            "command": {
                "intent": "meeting.create",
                "entities": {
                    "user_id": "u-meeting-no-target",
                    "start_at": "2026-04-13T14:00:00+03:00",
                    "duration_minutes": 60,
                },
            },
        }
        req1 = urllib.request.Request(
            url,
            data=json.dumps(create_payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req1, timeout=3) as resp:
            body1 = json.loads(resp.read().decode("utf-8"))

        update_payload = {
            "trace_id": "meeting-update-no-target-1",
            "source": {"user_id": "u-meeting-no-target", "timezone": "Europe/Moscow"},
            "command": {
                "intent": "meeting.update",
                "entities": {
                    "user_id": "u-meeting-no-target",
                    "start_at_time": "16:00",
                    "text": "перенеси встречу на 16",
                },
            },
        }
        req2 = urllib.request.Request(
            url,
            data=json.dumps(update_payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req2, timeout=3) as resp:
            body2 = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert body1.get("ok") is True
    assert body1.get("calendar_event_id") == "evt-9100"
    assert body2.get("ok") is False
    assert "встречу для переноса" in str(body2.get("user_message") or "").lower()
    assert len(create_calls) == 1
    assert len(patch_calls) == 0


def test_runtime_meeting_create_with_date_only_start_at_uses_split_time_fields(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_calls: list[dict] = []

    def _fake_create(item_id, title, start, end, description=None, flow_id=None):  # noqa: ANN001
        create_calls.append(
            {
                "item_id": item_id,
                "title": title,
                "start": start,
                "end": end,
                "description": description,
                "flow_id": flow_id,
            }
        )
        return "evt-call-1700"

    monkeypatch.setattr(worker, "_create_event", _fake_create)

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"
        payload = {
            "trace_id": "meeting-create-call-time-1",
            "source": {"user_id": "u-call-1", "timezone": "Europe/Moscow"},
            "command": {
                "intent": "meeting.create",
                "entities": {
                    "user_id": "u-call-1",
                    "meeting_kind": "созвон",
                    "start_at": "2026-04-13",
                    "start_at_date": "2026-04-13",
                    "start_at_time": "17:00",
                    "duration_minutes": 30,
                    "text": "запланируй созвон с Иваном завтра в 17",
                },
            },
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert body.get("ok") is True
    assert body.get("calendar_event_id") == "evt-call-1700"
    assert body.get("user_message") == "Созвон создан."
    assert len(create_calls) == 1
    created_start = create_calls[0]["start"]
    created_end = create_calls[0]["end"]
    assert isinstance(created_start, datetime)
    assert isinstance(created_end, datetime)
    assert created_start.strftime("%Y-%m-%d %H:%M") == "2026-04-13 17:00"
    assert created_end.strftime("%Y-%m-%d %H:%M") == "2026-04-13 17:30"


def test_runtime_meeting_create_without_event_id_returns_error_not_success(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _fake_create(item_id, title, start, end, description=None, flow_id=None):  # noqa: ANN001, ARG001
        return ""

    monkeypatch.setattr(worker, "_create_event", _fake_create)
    out = worker._runtime_commit_temporal_calendar(
        flow_id="meeting-create-no-event-id-1",
        intent="meeting.create",
        entities={
            "user_id": "u-no-event-id-1",
            "meeting_kind": "встреча",
            "start_at_date": "2026-04-21",
            "start_at_time": "17:00",
            "duration_minutes": 30,
            "text": "запланируй встречу завтра в 17",
        },
        execution_result={"ok": True, "outcome": "success", "user_message": "Встреча создана."},
        source_timezone="Europe/Moscow",
    )

    assert out.get("ok") is False
    assert str(out.get("calendar_event_id") or "").strip() == ""
    assert "создан" not in str(out.get("user_message") or "").lower()
    debug = out.get("debug") if isinstance(out.get("debug"), dict) else {}
    assert str(debug.get("calendar_commit") or "") == "no_event_id"


def test_runtime_meeting_update_success_message_uses_meeting_kind(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _fake_patch(event_id, start, end):  # noqa: ANN001
        return "ok"

    def _fake_get(event_id):  # noqa: ANN001
        return {
            "ok": True,
            "event_id": event_id,
            "event_start": "2026-04-13T14:00:00+03:00",
            "event_end": "2026-04-13T15:00:00+03:00",
            "time_zone": "Europe/Moscow",
        }

    monkeypatch.setattr(worker, "_patch_event", _fake_patch)
    monkeypatch.setattr(worker, "_calendar_get_event", _fake_get)

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"
        payload = {
            "trace_id": "meeting-update-kind-1",
            "source": {"user_id": "u-update-kind-1", "timezone": "Europe/Moscow"},
            "command": {
                "intent": "meeting.update",
                "entities": {
                    "calendar_event_id": "evt-update-kind-1",
                    "user_id": "u-update-kind-1",
                    "meeting_kind": "собрание",
                    "start_at_time": "16:00",
                    "text": "перенеси собрание на 16",
                },
            },
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert body.get("ok") is True
    assert body.get("user_message") == "Собрание перенесено."


def test_runtime_meeting_create_success_text_and_payload_kind_are_consistent_for_all_event_types() -> None:
    cases = [
        ("встреча", "Встреча создана."),
        ("собрание", "Собрание создано."),
        ("созвон", "Созвон создан."),
        ("мероприятие", "Мероприятие создано."),
    ]
    for meeting_kind, expected_success in cases:
        entities = {
            "meeting_kind": meeting_kind,
            "title": f"{meeting_kind.capitalize()} с командой",
            "text": "встреча с командой",
        }
        kind_for_commit = worker._meeting_kind_from_entities("meeting.create", entities)
        title_for_commit = worker._runtime_temporal_title("meeting.create", entities)
        success_text = worker._meeting_success_text(kind_for_commit, "create")

        assert kind_for_commit == meeting_kind
        assert success_text == expected_success
        assert title_for_commit == f"{meeting_kind.capitalize()} с командой"


def test_runtime_meeting_search_matches_time_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_get(event_id: str) -> dict:
        if event_id == "evt-14":
            return {
                "ok": True,
                "event_id": event_id,
                "event_start": "2026-04-13T14:00:00+03:00",
                "event_end": "2026-04-13T15:00:00+03:00",
                "time_zone": "Europe/Moscow",
                "title": "Встреча",
                "description": "",
            }
        return {
            "ok": True,
            "event_id": event_id,
            "event_start": "2026-04-13T18:00:00+03:00",
            "event_end": "2026-04-13T19:00:00+03:00",
            "time_zone": "Europe/Moscow",
            "title": "Встреча",
                "description": "",
            }

    _patch_runtime_trace_rows(monkeypatch, user_id="u-search-1", event_ids=["evt-14", "evt-18"])
    monkeypatch.setattr(worker, "_calendar_get_event", _fake_get)
    out = worker._runtime_meeting_search_for_user("u-search-1", "встречу в 14 часов")

    assert out.get("ok") is True
    candidate = out.get("candidate") if isinstance(out.get("candidate"), dict) else {}
    assert candidate.get("calendar_event_id") == "evt-14"
    assert candidate.get("start_at_time") == "14:00"


def test_runtime_meeting_search_matches_text_hint_in_title_or_description(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_get(event_id: str) -> dict:
        if event_id == "evt-ivan":
            return {
                "ok": True,
                "event_id": event_id,
                "event_start": "2026-04-13T14:00:00+03:00",
                "event_end": "2026-04-13T15:00:00+03:00",
                "time_zone": "Europe/Moscow",
                "title": "Встреча с Иваном",
                "description": "Статус проекта",
            }
        return {
            "ok": True,
            "event_id": event_id,
            "event_start": "2026-04-13T10:00:00+03:00",
            "event_end": "2026-04-13T11:00:00+03:00",
            "time_zone": "Europe/Moscow",
            "title": "Командная встреча",
            "description": "без Ивана",
        }

    _patch_runtime_trace_rows(monkeypatch, user_id="u-search-2", event_ids=["evt-team", "evt-ivan"])
    monkeypatch.setattr(worker, "_calendar_get_event", _fake_get)
    out = worker._runtime_meeting_search_for_user("u-search-2", "с Иваном")

    assert out.get("ok") is True
    candidate = out.get("candidate") if isinstance(out.get("candidate"), dict) else {}
    assert candidate.get("calendar_event_id") == "evt-ivan"


def test_runtime_meeting_search_respects_meeting_kind_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_get(event_id: str) -> dict:
        if event_id == "evt-assembly":
            return {
                "ok": True,
                "event_id": event_id,
                "event_start": "2026-04-13T14:00:00+03:00",
                "event_end": "2026-04-13T15:00:00+03:00",
                "time_zone": "Europe/Moscow",
                "title": "Собрание по проекту",
                "description": "",
            }
        return {
            "ok": True,
            "event_id": event_id,
            "event_start": "2026-04-13T14:00:00+03:00",
            "event_end": "2026-04-13T15:00:00+03:00",
            "time_zone": "Europe/Moscow",
            "title": "Созвон с Иваном",
            "description": "",
        }

    _patch_runtime_trace_rows(monkeypatch, user_id="u-search-kind-1", event_ids=["evt-sync", "evt-assembly"])
    monkeypatch.setattr(worker, "_calendar_get_event", _fake_get)
    out = worker._runtime_meeting_search_for_user("u-search-kind-1", "", "собрание")

    assert out.get("ok") is True
    candidate = out.get("candidate") if isinstance(out.get("candidate"), dict) else {}
    assert candidate.get("calendar_event_id") == "evt-assembly"
    assert candidate.get("meeting_kind") == "собрание"


def test_runtime_meeting_search_multiple_returns_top3_ranked_candidates(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_get(event_id: str) -> dict:
        by_id = {
            "evt-best": {
                "ok": True,
                "event_id": "evt-best",
                "event_start": "2026-04-16T13:00:00+03:00",
                "event_end": "2026-04-16T14:00:00+03:00",
                "time_zone": "Europe/Moscow",
                "title": "Собрание с администраторами",
                "description": "Короткий статус",
            },
            "evt-mid": {
                "ok": True,
                "event_id": "evt-mid",
                "event_start": "2026-04-17T13:00:00+03:00",
                "event_end": "2026-04-17T14:00:00+03:00",
                "time_zone": "Europe/Moscow",
                "title": "Собрание по бюджету",
                "description": "С администраторами и командой",
            },
            "evt-low": {
                "ok": True,
                "event_id": "evt-low",
                "event_start": "2026-04-18T13:00:00+03:00",
                "event_end": "2026-04-18T14:00:00+03:00",
                "time_zone": "Europe/Moscow",
                "title": "Собрание отдела",
                "description": "Общий апдейт",
            },
            "evt-extra": {
                "ok": True,
                "event_id": "evt-extra",
                "event_start": "2026-04-19T13:00:00+03:00",
                "event_end": "2026-04-19T14:00:00+03:00",
                "time_zone": "Europe/Moscow",
                "title": "Собрание с подрядчиками",
                "description": "Без админов",
            },
        }
        return by_id[event_id]

    _patch_runtime_trace_rows(
        monkeypatch,
        user_id="u-search-top3-1",
        event_ids=["evt-best", "evt-mid", "evt-low", "evt-extra"],
    )
    monkeypatch.setattr(worker, "_calendar_get_event", _fake_get)
    out = worker._runtime_meeting_search_for_user("u-search-top3-1", "измени собрание")

    assert out.get("ok") is False
    assert out.get("reason") == "multiple"
    candidates = out.get("candidates") if isinstance(out.get("candidates"), list) else []
    assert len(candidates) == 3
    ids = [str(c.get("calendar_event_id") or "") for c in candidates if isinstance(c, dict)]
    assert "evt-extra" in ids
    assert "evt-low" in ids
    assert "evt-mid" in ids


def test_runtime_meeting_search_ranks_keyword_relevance_for_admins(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_get(event_id: str) -> dict:
        by_id = {
            "evt-best": {
                "ok": True,
                "event_id": "evt-best",
                "event_start": "2026-04-16T13:00:00+03:00",
                "event_end": "2026-04-16T14:00:00+03:00",
                "time_zone": "Europe/Moscow",
                "title": "Встреча с администраторами",
                "description": "Статус",
            },
            "evt-mid": {
                "ok": True,
                "event_id": "evt-mid",
                "event_start": "2026-04-17T13:00:00+03:00",
                "event_end": "2026-04-17T14:00:00+03:00",
                "time_zone": "Europe/Moscow",
                "title": "Встреча команды",
                "description": "Обсуждение с администраторами",
            },
            "evt-other": {
                "ok": True,
                "event_id": "evt-other",
                "event_start": "2026-04-18T13:00:00+03:00",
                "event_end": "2026-04-18T14:00:00+03:00",
                "time_zone": "Europe/Moscow",
                "title": "Встреча отдела",
                "description": "Без этого участника",
            },
        }
        return by_id[event_id]

    _patch_runtime_trace_rows(monkeypatch, user_id="u-search-rank-admin-1", event_ids=["evt-best", "evt-mid", "evt-other"])
    monkeypatch.setattr(worker, "_calendar_get_event", _fake_get)
    out = worker._runtime_meeting_search_for_user("u-search-rank-admin-1", "измени встречу с администраторами")

    assert out.get("ok") is False
    assert out.get("reason") == "multiple"
    candidates = out.get("candidates") if isinstance(out.get("candidates"), list) else []
    assert len(candidates) >= 2
    first_id = str(candidates[0].get("calendar_event_id") or "")
    assert first_id == "evt-best"


def test_runtime_meeting_search_no_matches_returns_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_get(event_id: str) -> dict:
        if event_id == "evt-a":
            return {
                "ok": True,
                "event_id": event_id,
                "event_start": "2026-04-13T11:00:00+03:00",
                "event_end": "2026-04-13T12:00:00+03:00",
                "time_zone": "Europe/Moscow",
                "title": "Встреча команды",
                "description": "Еженедельный статус",
            }
        return {
            "ok": True,
            "event_id": event_id,
            "event_start": "2026-04-13T15:00:00+03:00",
            "event_end": "2026-04-13T16:00:00+03:00",
            "time_zone": "Europe/Moscow",
            "title": "Собрание по релизу",
            "description": "Без участников из запроса",
        }

    _patch_runtime_trace_rows(monkeypatch, user_id="u-search-none-1", event_ids=["evt-a", "evt-b"])
    monkeypatch.setattr(worker, "_calendar_get_event", _fake_get)
    out = worker._runtime_meeting_search_for_user("u-search-none-1", "созвон с Иваном")

    assert out.get("ok") is False
    assert out.get("reason") == "not_found"
    assert out.get("candidates") == []


def test_runtime_meeting_search_filters_deleted_calendar_events_and_keeps_only_real_event(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _fake_get(event_id: str) -> dict:
        if event_id == "evt-real-1":
            return {
                "ok": True,
                "event_id": event_id,
                "event_start": "2026-04-13T14:00:00+03:00",
                "event_end": "2026-04-13T15:00:00+03:00",
                "time_zone": "Europe/Moscow",
                "title": "Встреча с Иваном",
                "description": "",
            }
        return {
            "ok": False,
            "event_id": None,
            "http_status": 404,
            "err": "not_found",
            "event_start": None,
            "event_end": None,
            "time_zone": None,
        }

    _patch_runtime_trace_rows(
        monkeypatch,
        user_id="u-search-clean-1",
        event_ids=["evt-deleted-1", "evt-real-1", "evt-deleted-2"],
    )
    monkeypatch.setattr(worker, "_calendar_get_event", _fake_get)
    out = worker._runtime_meeting_search_for_user("u-search-clean-1", "с Иваном")

    assert out.get("ok") is True
    candidate = out.get("candidate") if isinstance(out.get("candidate"), dict) else {}
    assert candidate.get("calendar_event_id") == "evt-real-1"
    with db.connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS cnt FROM sync_conflicts").fetchone()
    assert int(row["cnt"]) == 2


def test_runtime_meeting_search_collapses_duplicate_live_candidates_by_title_start_and_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_get(event_id: str) -> dict:
        if event_id in {"evt-dup-1", "evt-dup-2"}:
            return {
                "ok": True,
                "event_id": event_id,
                "event_start": "2026-04-13T14:00:00+03:00",
                "event_end": "2026-04-13T15:00:00+03:00",
                "time_zone": "Europe/Moscow",
                "title": "Встреча с Иваном",
                "description": "Статус",
            }
        return {
            "ok": True,
            "event_id": event_id,
            "event_start": "2026-04-13T18:00:00+03:00",
            "event_end": "2026-04-13T19:00:00+03:00",
            "time_zone": "Europe/Moscow",
            "title": "Встреча с Петром",
            "description": "",
        }

    _patch_runtime_trace_rows(monkeypatch, user_id="u-search-dedup-1", event_ids=["evt-dup-1", "evt-dup-2", "evt-other"])
    monkeypatch.setattr(worker, "_calendar_get_event", _fake_get)
    out = worker._runtime_meeting_search_for_user("u-search-dedup-1", "встреча")

    assert out.get("ok") is False
    assert out.get("reason") == "multiple"
    candidates = out.get("candidates") if isinstance(out.get("candidates"), list) else []
    ids = [str(c.get("calendar_event_id") or "") for c in candidates if isinstance(c, dict)]
    assert ids.count("evt-dup-1") + ids.count("evt-dup-2") == 1


def test_runtime_cleanup_stale_events_removes_deleted_trace_entries(runtime_db: Path) -> None:
    worker._runtime_trace_dedup_put(
        "trace-stale-1",
        {"calendar_event_id": "evt-stale-1", "debug": {"user_id": "u-clean-1"}},
    )
    worker._runtime_trace_dedup_put(
        "trace-live-1",
        {"calendar_event_id": "evt-live-1", "debug": {"user_id": "u-clean-1"}},
    )

    def _fake_get(event_id: str) -> dict:
        if event_id == "evt-live-1":
            return {
                "ok": True,
                "event_id": event_id,
                "http_status": None,
                "err": None,
                "event_start": "2026-04-13T14:00:00+03:00",
                "event_end": "2026-04-13T15:00:00+03:00",
                "time_zone": "Europe/Moscow",
                "title": "Встреча",
                "description": "",
            }
        return {
            "ok": False,
            "event_id": None,
            "http_status": 404,
            "err": "not_found",
            "event_start": None,
            "event_end": None,
            "time_zone": None,
        }

    orig_get = worker._calendar_get_event
    worker._calendar_get_event = _fake_get  # type: ignore[assignment]
    try:
        out = worker._runtime_cleanup_stale_calendar_events_for_user("u-clean-1")
        remaining = worker._runtime_trace_list_calendar_events_for_user("u-clean-1")
    finally:
        worker._calendar_get_event = orig_get  # type: ignore[assignment]

    assert out.get("ok") is True
    assert out.get("created_or_reopened") == 1
    conflicts = out.get("conflicts") if isinstance(out.get("conflicts"), list) else []
    assert conflicts
    assert str(conflicts[0].get("calendar_event_id") or "") == "evt-stale-1"
    assert remaining == ["evt-live-1"]


def test_runtime_meeting_create_normalizes_title_and_does_not_use_raw_text_as_description(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_calls: list[dict] = []

    def _fake_create(item_id, title, start, end, description=None, flow_id=None):  # noqa: ANN001
        create_calls.append(
            {
                "item_id": item_id,
                "title": title,
                "start": start,
                "end": end,
                "description": description,
                "flow_id": flow_id,
            }
        )
        return "evt-title-1"

    monkeypatch.setattr(worker, "_create_event", _fake_create)

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"
        create_payload = {
            "trace_id": "meeting-create-title-1",
            "source": {"user_id": "u-title-1", "timezone": "Europe/Moscow"},
            "command": {
                "intent": "meeting.create",
                "entities": {
                    "user_id": "u-title-1",
                    "start_at": "2026-04-13T14:00:00+03:00",
                    "duration_minutes": 60,
                    "text": "назначь созвон с Иваном завтра в 14 на 60 минут",
                },
            },
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(create_payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert body.get("ok") is True
    assert create_calls
    title = str(create_calls[0].get("title") or "").lower()
    assert "созвон" in title
    assert "завтра" not in title
    assert "14" not in title
    assert "60" not in title
    assert create_calls[0].get("description") in (None, "")


def test_runtime_meeting_comment_update_patches_description_only(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    description_calls: list[dict] = []
    datetime_patch_calls: list[dict] = []

    def _fake_patch_description(event_id, description):  # noqa: ANN001
        description_calls.append({"event_id": event_id, "description": description})
        return "ok"

    def _fake_patch(event_id, start, end):  # noqa: ANN001
        datetime_patch_calls.append({"event_id": event_id, "start": start, "end": end})
        return "ok"

    monkeypatch.setattr(worker, "_patch_event_description", _fake_patch_description)
    monkeypatch.setattr(worker, "_patch_event", _fake_patch)

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"
        payload = {
            "trace_id": "meeting-comment-update-1",
            "source": {"user_id": "u-comment-1", "timezone": "Europe/Moscow"},
            "command": {
                "intent": "meeting.comment.update",
                "entities": {
                    "user_id": "u-comment-1",
                    "calendar_event_id": "evt-comment-1",
                    "comment_text": "Новый комментарий",
                },
            },
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert body.get("ok") is True
    assert body.get("calendar_event_id") == "evt-comment-1"
    assert description_calls == [{"event_id": "evt-comment-1", "description": "Новый комментарий"}]
    assert datetime_patch_calls == []


def test_runtime_meeting_create_uses_comment_text_as_calendar_description(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    create_calls: list[dict] = []

    def _fake_create(item_id, title, start, end, description=None, flow_id=None):  # noqa: ANN001
        create_calls.append(
            {
                "item_id": item_id,
                "title": title,
                "start": start,
                "end": end,
                "description": description,
                "flow_id": flow_id,
            }
        )
        return "evt-comment-create-1"

    monkeypatch.setattr(worker, "_create_event", _fake_create)

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"
        payload = {
            "trace_id": "meeting-create-comment-1",
            "source": {"user_id": "u-comment-create-1", "timezone": "Europe/Moscow"},
            "command": {
                "intent": "meeting.create",
                "entities": {
                    "user_id": "u-comment-create-1",
                    "meeting_kind": "встреча",
                    "start_at": "2026-04-13T14:00:00+03:00",
                    "duration_minutes": 30,
                    "comment_text": "обсудить договор",
                },
            },
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert body.get("ok") is True
    assert body.get("calendar_event_id") == "evt-comment-create-1"
    assert create_calls
    assert create_calls[0].get("description") == "обсудить договор"


def test_runtime_meeting_update_patches_description_when_comment_present(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    description_calls: list[dict] = []
    datetime_patch_calls: list[dict] = []

    def _fake_patch_description(event_id, description):  # noqa: ANN001
        description_calls.append({"event_id": event_id, "description": description})
        return "ok"

    def _fake_patch(event_id, start, end):  # noqa: ANN001
        datetime_patch_calls.append({"event_id": event_id, "start": start, "end": end})
        return "ok"

    def _fake_get(event_id):  # noqa: ANN001
        return {
            "ok": True,
            "event_id": event_id,
            "event_start": "2026-04-13T14:00:00+03:00",
            "event_end": "2026-04-13T15:00:00+03:00",
            "time_zone": "Europe/Moscow",
            "title": "Встреча",
            "description": "",
        }

    monkeypatch.setattr(worker, "_patch_event_description", _fake_patch_description)
    monkeypatch.setattr(worker, "_patch_event", _fake_patch)
    monkeypatch.setattr(worker, "_calendar_get_event", _fake_get)

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"
        payload = {
            "trace_id": "meeting-update-comment-1",
            "source": {"user_id": "u-comment-update-1", "timezone": "Europe/Moscow"},
            "command": {
                "intent": "meeting.update",
                "entities": {
                    "user_id": "u-comment-update-1",
                    "calendar_event_id": "evt-comment-update-1",
                    "start_at_time": "16:00",
                    "comment_text": "обсудить договор",
                },
            },
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert body.get("ok") is True
    assert body.get("calendar_event_id") == "evt-comment-update-1"
    assert datetime_patch_calls
    assert description_calls == [{"event_id": "evt-comment-update-1", "description": "обсудить договор"}]


def test_runtime_timeblock_create_persists_comment_in_db(runtime_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with db.connect() as conn:
        task_id = db.create_task(conn, title="Подготовить отчёт")

    monkeypatch.setattr(worker, "_create_event", lambda *args, **kwargs: "evt-timeblock-comment-1")

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"
        payload = {
            "trace_id": "timeblock-comment-1",
            "source": {"user_id": "u-timeblock-comment-1", "timezone": "Europe/Moscow"},
            "command": {
                "intent": "timeblock.create",
                "entities": {
                    "user_id": "u-timeblock-comment-1",
                    "task_id": task_id,
                    "start_at": "2026-04-13T14:00:00+03:00",
                    "duration_minutes": 45,
                    "comment_text": "глубокая работа",
                },
            },
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert body.get("ok") is True
    debug = body.get("debug") if isinstance(body.get("debug"), dict) else {}
    time_block_id = int(debug["time_block_id"])
    with db.connect() as conn:
        row = conn.execute("SELECT comment FROM time_blocks WHERE id = ?", (time_block_id,)).fetchone()
    assert row is not None
    assert str(row["comment"] or "") == "глубокая работа"


def test_runtime_timeblock_update_envelope_updates_existing_block(runtime_db: Path, caplog: pytest.LogCaptureFixture) -> None:
    with db.connect() as conn:
        task_id = db.create_task(conn, title="Подготовить отчёт")
        time_block_id = db.create_time_block(
            conn,
            task_id=task_id,
            start_at="2026-03-10T11:00:00+03:00",
            end_at="2026-03-10T11:30:00+03:00",
            comment="старый комментарий",
        )

    caplog.set_level("INFO")
    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"
        payload = {
            "trace_id": "timeblock-update-1",
            "source": {"user_id": "u-timeblock-update-1", "timezone": "Europe/Moscow"},
            "command": {
                "intent": "timeblock.update",
                "entities": {
                    "user_id": "u-timeblock-update-1",
                    "time_block_id": time_block_id,
                    "start_at": "2026-03-10T12:00:00+03:00",
                    "end_at": "2026-03-10T12:45:00+03:00",
                    "comment_text": "новый комментарий",
                },
            },
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert body.get("ok") is True
    debug = body.get("debug") if isinstance(body.get("debug"), dict) else {}
    assert int(debug.get("time_block_id") or 0) == int(time_block_id)
    trace_row = worker._runtime_trace_dedup_get("timeblock-update-1") or {}
    trace_snapshot = trace_row.get("__trace_snapshot") if isinstance(trace_row.get("__trace_snapshot"), dict) else {}
    assert trace_snapshot.get("intent") == "timeblock.update"
    with db.connect() as conn:
        row = conn.execute("SELECT start_at, end_at, comment FROM time_blocks WHERE id = ?", (time_block_id,)).fetchone()
    assert row is not None
    assert str(row["start_at"]).startswith("2026-03-10T12:00:00")
    assert str(row["end_at"]).startswith("2026-03-10T12:45:00")
    assert str(row["comment"] or "") == "новый комментарий"
    dispatch_record = next((r for r in caplog.records if "runtime_intent_dispatch_start" in r.getMessage() and "intent=timeblock.update" in r.getMessage()), None)
    assert dispatch_record is not None
    assert "commit_path=runtime_state_only" in dispatch_record.getMessage()
    result_record = next((r for r in caplog.records if "runtime_intent_dispatch_result" in r.getMessage() and "intent=timeblock.update" in r.getMessage()), None)
    assert result_record is not None
    assert "commit_path=runtime_state_only" in result_record.getMessage()


def test_runtime_task_comment_update_updates_task_comment(runtime_db: Path) -> None:
    with db.connect() as conn:
        task_id = db.create_task(conn, title="Подготовить отчёт")

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        command_url = f"http://{host}:{port}/runtime/command"
        search_url = f"http://{host}:{port}/runtime/task/search"

        search_req = urllib.request.Request(
            search_url,
            data=json.dumps({"user_id": "u-task-comment", "target_hint": "отч"}, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(search_req, timeout=3) as resp:
            search_body = json.loads(resp.read().decode("utf-8"))

        payload = {
            "trace_id": "task-comment-update-1",
            "source": {"user_id": "u-task-comment"},
            "command": {
                "intent": "task.comment.update",
                "entities": {
                    "task_id": task_id,
                    "comment_text": "Комментарий к задаче",
                },
            },
        }
        req = urllib.request.Request(
            command_url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert search_body.get("ok") is True
    assert body.get("ok") is True
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT comment FROM tasks WHERE id = ?", (task_id,)).fetchone()
    assert row is not None
    assert str(row["comment"] or "") == "Комментарий к задаче"


def test_runtime_cleanup_stale_events_creates_sync_conflict_instead_of_deleting(runtime_db: Path) -> None:
    worker._runtime_trace_dedup_put(
        "trace-stale-conflict-1",
        {
            "calendar_event_id": "evt-stale-conflict-1",
            "title": "Встреча с Алексеем",
            "start_at_date": "2026-05-10",
            "start_at_time": "11:00",
            "duration_minutes": 30,
            "debug": {"user_id": "u-sync-conflict-1"},
        },
    )

    def _fake_get(event_id: str) -> dict:
        return {
            "ok": False,
            "event_id": None,
            "http_status": 404 if event_id == "evt-stale-conflict-1" else 500,
            "err": "not_found",
        }

    orig_get = worker._calendar_get_event
    worker._calendar_get_event = _fake_get  # type: ignore[assignment]
    try:
        out = worker._runtime_cleanup_stale_calendar_events_for_user("u-sync-conflict-1")
        remaining_rows = worker._runtime_trace_list_rows_for_user("u-sync-conflict-1")
        conflicts = out.get("conflicts") if isinstance(out.get("conflicts"), list) else []
        conflict = conflicts[0] if conflicts else {}
    finally:
        worker._calendar_get_event = orig_get  # type: ignore[assignment]

    assert out.get("ok") is True
    assert out.get("created_or_reopened") == 1
    assert remaining_rows
    assert conflict.get("status") == "pending"
    assert conflict.get("calendar_event_id") == "evt-stale-conflict-1"


def test_runtime_sync_conflict_restore_calendar_recreates_event_and_updates_trace(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker._runtime_trace_dedup_put(
        "trace-restore-1",
        {
            "calendar_event_id": "evt-missing-restore-1",
            "__trace_snapshot": {
                "trace_id": "trace-restore-1",
                "intent": "meeting.update",
                "user_id": "u-sync-restore-1",
                "calendar_event_id": "evt-missing-restore-1",
                "title": "Встреча с Алексеем",
                "start_at": "2026-05-10T11:00:00+03:00",
                "start_at_date": "2026-05-10",
                "start_at_time": "11:00",
                "duration_minutes": 30,
                "comment_text": "обсудить договор",
            },
            "debug": {"user_id": "u-sync-restore-1"},
        },
    )
    conflict = worker._runtime_sync_conflict_create_or_reopen(
        user_id="u-sync-restore-1",
        source="runtime_trace_dedup",
        entity_ref="trace-restore-1",
        calendar_event_id="evt-missing-restore-1",
        payload={
            "trace_id": "trace-restore-1",
            "title": "Встреча с Алексеем",
            "start_at": "2026-05-10T11:00:00+03:00",
            "start_at_date": "2026-05-10",
            "start_at_time": "11:00",
            "duration_minutes": 30,
            "comment_text": "обсудить договор",
            "intent": "meeting.update",
            "user_id": "u-sync-restore-1",
        },
    )
    monkeypatch.setattr(worker, "_create_event", lambda *args, **kwargs: "evt-restored-1")

    out = worker._runtime_sync_conflict_apply_action(str(conflict.get("id") or ""), "restore_calendar")
    updated = worker._runtime_trace_dedup_get("trace-restore-1") or {}
    resolved = worker._runtime_sync_conflict_get(str(conflict.get("id") or ""))

    assert out.get("ok") is True
    assert out.get("calendar_event_id") == "evt-restored-1"
    assert updated.get("calendar_event_id") == "evt-restored-1"
    assert isinstance(resolved, dict)
    assert resolved.get("status") == "resolved"


def test_runtime_check_calendar_drift_does_not_spam_existing_pending_conflict(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker._runtime_trace_dedup_put(
        "trace-drift-dup-1",
        {
            "calendar_event_id": "evt-drift-dup-1",
            "__trace_snapshot": {
                "trace_id": "trace-drift-dup-1",
                "intent": "meeting.update",
                "user_id": "u-drift-dup-1",
                "calendar_event_id": "evt-drift-dup-1",
                "source_msg_id": "tg:1001:2002",
                "title": "Встреча с Алексеем",
                "start_at": "2026-05-10T11:00:00+03:00",
                "start_at_date": "2026-05-10",
                "start_at_time": "11:00",
                "duration_minutes": 30,
                "comment_text": "",
            },
            "debug": {"user_id": "u-drift-dup-1"},
        },
    )
    monkeypatch.setattr(worker, "_calendar_get_event", lambda event_id: {"ok": False, "http_status": 404, "err": "not_found"})
    send_calls: list[tuple[int, str]] = []
    monkeypatch.setattr(worker, "_tg_send_message_with_keyboard", lambda chat_id, text, reply_markup, **kwargs: send_calls.append((chat_id, text)) or True)

    first = worker._runtime_check_calendar_drift(
        user_id="u-drift-dup-1",
        from_value="2026-05-01",
        to_value="2026-05-30",
        notify=True,
    )
    second = worker._runtime_check_calendar_drift(
        user_id="u-drift-dup-1",
        from_value="2026-05-01",
        to_value="2026-05-30",
        notify=True,
    )

    assert first.get("conflicts_created_or_found") == 1
    assert second.get("conflicts_created_or_found") == 1
    assert len(send_calls) == 1


def test_runtime_check_calendar_drift_manual_endpoint_creates_conflict(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker._runtime_trace_dedup_put(
        "trace-drift-manual-1",
        {
            "calendar_event_id": "evt-drift-manual-1",
            "__trace_snapshot": {
                "trace_id": "trace-drift-manual-1",
                "intent": "meeting.update",
                "user_id": "u-drift-manual-1",
                "calendar_event_id": "evt-drift-manual-1",
                "source_msg_id": "tg:3001:4002",
                "title": "Встреча manual",
                "start_at": "2026-05-10T11:00:00+03:00",
                "start_at_date": "2026-05-10",
                "start_at_time": "11:00",
                "duration_minutes": 30,
            },
            "debug": {"user_id": "u-drift-manual-1"},
        },
    )
    monkeypatch.setattr(worker, "_calendar_get_event", lambda event_id: {"ok": False, "http_status": 404, "err": "not_found"})
    monkeypatch.setattr(worker, "_tg_send_message_with_keyboard", lambda *args, **kwargs: True)

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/meeting/check_calendar_drift"
        req = urllib.request.Request(
            url,
            data=json.dumps({"user_id": "u-drift-manual-1", "from": "2026-05-01", "to": "2026-05-30"}, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert body.get("ok") is True
    assert body.get("conflicts_created_or_found") == 1


def test_runtime_command_worker_safety_net_canonicalizes_relative_due_date_before_dispatch(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001
            base = cls(2026, 5, 17, 9, 0, 0)
            return base.replace(tzinfo=tz) if tz is not None else base

    monkeypatch.setattr(worker._runtime_handlers_module, "datetime", _FixedDateTime)
    calls: list[dict[str, Any]] = []

    def _fake_dispatch(command: dict) -> dict:
        calls.append(command)
        return {"ok": True, "user_message": "created", "task_id": 101}

    monkeypatch.setattr(worker, "dispatch_intent", _fake_dispatch)

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"
        payload = {
            "trace_id": "task-date-normalize-1",
            "command": {
                "intent": "task.create",
                "entities": {"title": "Купить молоко", "due_date": "завтра"},
            },
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert body.get("ok") is True
    assert len(calls) == 1
    entities = calls[0]["entities"]
    assert entities["due_date"] == "2026-05-18"
    assert entities["planned_at"] == "2026-05-18"


def test_runtime_command_worker_safety_net_returns_clarification_for_unresolved_relative_date(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001
            base = cls(2026, 5, 17, 9, 0, 0)
            return base.replace(tzinfo=tz) if tz is not None else base

    monkeypatch.setattr(worker._runtime_handlers_module, "datetime", _FixedDateTime)
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(worker, "dispatch_intent", lambda command: calls.append(command) or {"ok": True})

    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/runtime/command"
        payload = {
            "trace_id": "task-date-normalize-2",
            "command": {
                "intent": "task.create",
                "entities": {"title": "Купить молоко", "due_date": "на неделе"},
            },
        }
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()

    assert body.get("ok") is False
    assert body.get("missing_field") == "task_create_due_date"
    assert calls == []


def test_runtime_task_duplicate_precheck_logs_candidate_count_and_duplicate_found(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _ = runtime_db
    with db.connect() as conn:
        db.create_task(conn, title="Купить молоко", planned_at="2026-05-18 15:00")

    with caplog.at_level("INFO"):
        result = worker._runtime_task_duplicate_check_for_user(
            "u-log-1",
            title="создай задачу купить молоко",
            planned_at="2026-05-18 00:00",
            parent_task_id="",
        )

    assert result["ok"] is True
    assert result["duplicate_found"] is True
    assert result["planned_at"] == "2026-05-18 00:00"
    assert result["duplicate_scope"] == "dated"
    records = [r for r in caplog.records if r.getMessage() == "task_duplicate_precheck_result"]
    assert records
    rec = records[-1]
    assert getattr(rec, "candidate_count") == 1
    assert getattr(rec, "duplicate_found") is True
    assert getattr(rec, "planned_day") == "2026-05-18"
    assert getattr(rec, "planned_at") == "2026-05-18 00:00"
    assert getattr(rec, "duplicate_scope") == "dated"


def test_runtime_task_duplicate_precheck_logs_undated_scope_for_inbox_duplicate(
    runtime_db: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _ = runtime_db
    with db.connect() as conn:
        db.create_task(conn, title="Позвонить врачу", planned_at=None)

    with caplog.at_level("INFO"):
        result = worker._runtime_task_duplicate_check_for_user(
            "u-log-undated-1",
            title="позвонить врачу",
            planned_at=None,
            parent_task_id="",
        )

    assert result["ok"] is True
    assert result["duplicate_found"] is True
    assert result["planned_at"] is None
    assert result["planned_day"] is None
    assert result["undated_scope"] is True
    assert result["duplicate_scope"] == "inbox"
    records = [r for r in caplog.records if r.getMessage() == "task_duplicate_precheck_result"]
    assert records
    rec = records[-1]
    assert getattr(rec, "candidate_count") == 1
    assert getattr(rec, "duplicate_found") is True
    assert getattr(rec, "planned_at") is None
    assert getattr(rec, "planned_day") is None
    assert getattr(rec, "undated_scope") is True
    assert getattr(rec, "duplicate_scope") == "inbox"
