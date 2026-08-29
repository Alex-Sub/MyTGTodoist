import json
import sqlite3
import sys
import threading
import urllib.request
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
WORKER_ROOT = ROOT / "organizer-worker"
WORKER_SRC = WORKER_ROOT / "src"
sys.path.insert(0, str(WORKER_ROOT))
sys.path.insert(0, str(WORKER_SRC))

import p2_tasks_runtime as p2  # noqa: E402
import worker  # noqa: E402


def _apply_domain_migrations(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        for migration in sorted((ROOT / "migrations").glob("*.sql")):
            try:
                number = int(migration.name.split("_", 1)[0])
            except ValueError:
                continue
            if 10 <= number <= 26:
                conn.executescript(migration.read_text(encoding="utf-8"))
        conn.commit()


@pytest.fixture()
def runtime_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "runtime.db"
    monkeypatch.setenv("P2_DB_PATH", str(db_path))
    worker.DB_PATH = str(db_path)
    p2.DB_PATH = str(db_path)
    _apply_domain_migrations(db_path)
    return db_path


def _payload(*, key: str = "telegram:mytg:123", title: str = "Buy milk") -> dict:
    return {
        "trace_id": "trace-123",
        "source": {
            "channel": "telegram",
            "service": "telegram-gateway",
            "user_id": "user-1",
        },
        "idempotency_key": key,
        "command": {
            "intent": "task.create",
            "entities": {
                "title": title,
                "text": title,
                "source_msg_id": key,
            },
        },
    }


def test_valid_command_and_duplicate_are_one_mutation(runtime_db: Path) -> None:
    first_status, first = worker._runtime_response(_payload())
    second_status, second = worker._runtime_response(_payload())

    assert first_status == 200
    assert first["ok"] is True
    assert first["outcome"] == "succeeded"
    assert first["result_identity"]["entity_type"] == "task"
    assert second_status == 200
    assert second["outcome"] == "duplicate"
    assert second["result_identity"] == first["result_identity"]
    with sqlite3.connect(str(runtime_db)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM runtime_command_dedup").fetchone()[0] == 1


def test_conflicting_duplicate_is_rejected(runtime_db: Path) -> None:
    assert worker._runtime_response(_payload(title="Buy milk"))[0] == 200
    status, response = worker._runtime_response(_payload(title="Delete all tasks"))

    assert status == 409
    assert response == {
        "ok": False,
        "outcome": "rejected",
        "error": "idempotency_conflict",
        "idempotency_key": "telegram:mytg:123",
        "trace_id": "trace-123",
    }
    with sqlite3.connect(str(runtime_db)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 1


@pytest.mark.parametrize(
    "payload, message",
    [({}, "trace_id is required"), ({"trace_id": "t", "command": {}}, "command.intent is required")],
)
def test_invalid_schema_is_safe(payload: dict, message: str, runtime_db: Path) -> None:
    status, response = worker._runtime_response(payload)
    assert status == 400
    assert response["ok"] is False
    assert message in response["error"]


def test_restart_replay_uses_durable_dedup(runtime_db: Path) -> None:
    status, first = worker._runtime_response(_payload(key="telegram:mytg:456"))
    assert status == 200

    # A fresh request path reads the SQLite authority rather than process memory.
    status, replay = worker._runtime_response(
        {**_payload(key="telegram:mytg:456"), "trace_id": "trace-after-restart"}
    )
    assert status == 200
    assert replay["outcome"] == "duplicate"
    assert replay["result_identity"] == first["result_identity"]
    with sqlite3.connect(str(runtime_db)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 1


def test_http_server_health_and_command_endpoint(runtime_db: Path) -> None:
    server = worker.HTTPServer(("127.0.0.1", 0), worker._CommandHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        with urllib.request.urlopen(f"{base_url}/health", timeout=3) as response:
            assert json.load(response) == {"ok": True}
        request = urllib.request.Request(
            f"{base_url}/runtime/command",
            data=json.dumps(_payload(key="telegram:mytg:789")).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=3) as response:
            body = json.load(response)
        assert body["ok"] is True
        assert body["outcome"] == "succeeded"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_runtime_boundary_has_no_telegram_or_asr_ownership() -> None:
    source = "\n".join(
        (WORKER_ROOT / name).read_text(encoding="utf-8")
        for name in ("runtime_server.py", "runtime_handlers.py", "shared_runtime.py")
    )
    assert "getUpdates" not in source
    assert "ASR_SERVICE" not in source
