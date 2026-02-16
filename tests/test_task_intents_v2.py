import sqlite3
from pathlib import Path

import pytest

from organizer_worker import canon
from organizer_worker import db, handlers


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "migrations"


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
    db_path = tmp_path / "runtime_task_intents_v2.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    db.DB_PATH = str(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
    return db_path


def test_set_status_waiting_vs_paused(runtime_db: Path) -> None:
    t1 = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Задача A"}})
    t2 = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Задача B"}})
    assert t1["ok"] is True and t2["ok"] is True

    id1 = int(t1["debug"]["task_id"])
    id2 = int(t2["debug"]["task_id"])

    r1 = handlers.dispatch_intent({"intent": "task.set_status", "entities": {"task_id": id1, "status": "приостанови"}})
    r2 = handlers.dispatch_intent({"intent": "task.set_status", "entities": {"task_id": id2, "status": "ждем ответа"}})
    assert r1["ok"] is True and r2["ok"] is True
    assert r1["debug"]["status"] == "PAUSED"
    assert r2["debug"]["status"] == "WAITING"

    with sqlite3.connect(str(runtime_db)) as conn:
        row1 = conn.execute("SELECT status, state FROM tasks WHERE id = ?", (id1,)).fetchone()
        row2 = conn.execute("SELECT status, state FROM tasks WHERE id = ?", (id2,)).fetchone()
    assert row1 == ("PAUSED", "PAUSED")
    assert row2 == ("WAITING", "WAITING")


def test_task_reschedule_requires_when(runtime_db: Path) -> None:
    t = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Сдвинуть задачу"}})
    assert t["ok"] is True
    task_id = int(t["debug"]["task_id"])

    res = handlers.dispatch_intent({"intent": "task.reschedule", "entities": {"task_id": task_id}})
    assert res["ok"] is False
    assert res.get("clarifying_question") == "На когда перенести?"
    missing = res.get("debug", {}).get("missing")
    assert isinstance(missing, list)
    assert "entities.planned_at|entities.when" in missing


def test_task_ref_disambiguation_returns_candidates(runtime_db: Path) -> None:
    r1 = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Отчет для клиента"}})
    r2 = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Отчет для команды"}})
    assert r1["ok"] is True and r2["ok"] is True

    res = handlers.dispatch_intent(
        {
            "intent": "task.set_status",
            "entities": {"task_ref": "Отчет", "status": "пауза"},
        }
    )
    assert res["ok"] is False
    assert bool(res.get("clarifying_question"))
    choices = res.get("choices")
    assert isinstance(choices, list)
    assert 1 <= len(choices) <= canon.get_disambiguation_top_k()
    assert all(isinstance(ch.get("id"), int) and isinstance(ch.get("label"), str) for ch in choices)
    candidates = res.get("debug", {}).get("candidates_top")
    assert isinstance(candidates, list)
    assert len(candidates) >= 2


def test_paused_present_in_canon_status() -> None:
    status = canon.get_canon().get("common", {}).get("status", {}).get("canonical", {})
    assert "PAUSED" in status
    assert status["PAUSED"]["ru"] == "Приостановлено"


def test_timeblock_create_requires_duration_or_end(runtime_db: Path) -> None:
    t = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "ТБ задача"}})
    assert t["ok"] is True
    task_id = int(t["debug"]["task_id"])

    res = handlers.dispatch_intent(
        {
            "intent": "timeblock.create",
            "entities": {"task_id": task_id, "start_at": "2026-02-18T10:00:00Z"},
        }
    )
    assert res["ok"] is False
    assert res.get("clarifying_question") == "На сколько минут поставить блок?"
