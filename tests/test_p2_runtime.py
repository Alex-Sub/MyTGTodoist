import os
import sqlite3
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
WORKER_SRC = ROOT / "organizer-worker" / "src"
WORKER_ROOT = ROOT / "organizer-worker"
MIGRATIONS_DIR = ROOT / "migrations"

sys.path.append(str(WORKER_SRC))
sys.path.append(str(WORKER_ROOT))

import p2_tasks_runtime as p2  # noqa: E402
import worker  # noqa: E402


def _apply_runtime_migrations(conn: sqlite3.Connection) -> None:
    # Apply only runtime P2 migrations (010..019) to avoid legacy deps.
    migs = sorted(p for p in MIGRATIONS_DIR.iterdir() if p.name.endswith(".sql"))
    for path in migs:
        if not path.name.startswith("0"):
            continue
        try:
            num = int(path.name.split("_", 1)[0])
        except Exception:
            continue
        if num < 10 or num > 26:
            continue
        sql = path.read_text(encoding="utf-8")
        conn.executescript(sql)
    conn.commit()


@pytest.fixture()
def runtime_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "runtime.db"
    monkeypatch.setenv("P2_DB_PATH", str(db_path))
    p2.DB_PATH = str(db_path)
    worker.DB_PATH = str(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
    return db_path


def test_convert_direction_to_project_idempotent(runtime_db: Path) -> None:
    direction = p2.create_direction("Health", source_msg_id="s1")
    # existing non-archived project should be returned
    project = p2.create_project("Project A", direction_id=direction.id, source_msg_id="p1")
    res = p2.convert_direction_to_project(direction.id, source_msg_id="c1")
    assert res.id == project.id
    # archived should not block new project creation
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.execute("UPDATE projects SET status='ARCHIVED' WHERE id = ?", (project.id,))
        conn.commit()
    res2 = p2.convert_direction_to_project(direction.id, source_msg_id="c2")
    assert res2.id != project.id


def test_start_cycle_idempotent(runtime_db: Path) -> None:
    c1 = p2.start_cycle("MONTHLY", period_key="2026-02", source_msg_id="sc1")
    c2 = p2.start_cycle("MONTHLY", period_key="2026-02", source_msg_id="sc2")
    assert c2.id == c1.id


def test_create_task_parent_validation(runtime_db: Path) -> None:
    with pytest.raises(ValueError):
        worker.cmd_create_task("Task", parent_type="project")
    with pytest.raises(ValueError):
        worker.cmd_create_task("Task", parent_type="cycle", parent_id=999)
    # happy path
    direction = p2.create_direction("Dir", source_msg_id="sd1")
    project = p2.create_project("Proj", direction_id=direction.id, source_msg_id="sp1")
    task = worker.cmd_create_task("Task", parent_type="project", parent_id=project.id)
    assert task.get("parent_type") == "project"
    assert int(task.get("parent_id")) == project.id


def test_cycle_goal_create_and_continue(runtime_db: Path) -> None:
    cycle = p2.start_cycle("MONTHLY", period_key="2026-02", source_msg_id="c1")
    goal = p2.add_cycle_goal(cycle.id, "Goal 1")
    assert goal.cycle_id == cycle.id
    next_cycle = p2.start_cycle("MONTHLY", period_key="2026-03", source_msg_id="c2")
    continued = p2.continue_cycle_goal(goal.id, next_cycle.id)
    assert continued.cycle_id == next_cycle.id
    assert continued.continued_from_goal_id == goal.id


def test_cycle_close_without_tasks(runtime_db: Path) -> None:
    cycle = p2.start_cycle("QUARTERLY", period_key="2026-Q1", source_msg_id="c3")
    goal = p2.add_cycle_goal(cycle.id, "Goal X")
    closed = p2.close_cycle(cycle.id, status="DONE", summary=None)
    assert closed.status == "DONE"
    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute(
            "SELECT status FROM cycle_goals WHERE id = ?",
            (int(goal.id),),
        ).fetchone()
    assert row is not None and str(row[0]) == "ACTIVE"


def test_cycle_goal_status_updates(runtime_db: Path) -> None:
    cycle = p2.start_cycle("MONTHLY", period_key="2026-04", source_msg_id="c4")
    goal = p2.add_cycle_goal(cycle.id, "Goal Y")
    updated = p2.update_cycle_goal_status(goal.id, "ACHIEVED")
    assert updated.status == "ACHIEVED"
    updated2 = p2.update_cycle_goal_status(goal.id, "DROPPED")
    assert updated2.status == "DROPPED"
