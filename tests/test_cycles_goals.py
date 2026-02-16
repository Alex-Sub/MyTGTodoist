import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

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
    db_path = tmp_path / "runtime_cycles.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    db.DB_PATH = str(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
    return db_path


def _parse_iso(value: str) -> datetime:
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def test_cycle_goal_task_link_state(runtime_db: Path) -> None:
    res_cycle = handlers.dispatch_intent({"intent": "cycle.create", "entities": {"title": "Февраль", "date_from": "2026-02-01", "date_to": "2026-02-29"}})
    assert res_cycle["ok"] is True
    cycle_id = int(res_cycle["debug"]["cycle_id"])

    res_goal = handlers.dispatch_intent({"intent": "goal.create", "entities": {"cycle_id": cycle_id, "title": "Сделать рутину"}})
    assert res_goal["ok"] is True
    goal_id = int(res_goal["debug"]["goal_id"])

    res_task = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Тестовая задача"}})
    assert res_task["ok"] is True
    task_id = int(res_task["debug"]["task_id"])

    res_link = handlers.dispatch_intent({"intent": "task.update", "entities": {"task_id": task_id, "parent_type": "goal", "parent_id": goal_id}})
    assert res_link["ok"] is True

    res_state = handlers.dispatch_intent({"intent": "state.get"})
    assert res_state["ok"] is True
    st = res_state["debug"]
    assert int(st["cycles_total"]) == 1
    assert int(st["goals_total"]) == 1
    assert int(st["tasks_total"]) == 1

    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute("SELECT parent_type, parent_id FROM tasks WHERE id = ?", (task_id,)).fetchone()
    assert row is not None
    assert row[0] == "goal"
    assert int(row[1]) == goal_id

