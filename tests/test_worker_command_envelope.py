import json
import sqlite3
import sys
import threading
import urllib.request
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


@pytest.fixture()
def runtime_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "runtime_envelope.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
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
