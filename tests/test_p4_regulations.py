import os
import sqlite3
import importlib
import sys
from pathlib import Path

import pytest
from datetime import date


ROOT = Path(__file__).resolve().parents[1]
WORKER_SRC = ROOT / "organizer-worker" / "src"
WORKER_ROOT = ROOT / "organizer-worker"
MIGRATIONS_DIR = ROOT / "migrations"
API_PATH = ROOT / "organizer-api"

sys.path.append(str(WORKER_SRC))
sys.path.append(str(WORKER_ROOT))
sys.path.append(str(API_PATH))

import p2_tasks_runtime as p2  # noqa: E402
import worker  # noqa: E402


def _apply_runtime_migrations(conn: sqlite3.Connection) -> None:
    migs = sorted(p for p in MIGRATIONS_DIR.iterdir() if p.name.endswith(".sql"))
    for path in migs:
        try:
            num = int(path.name.split("_", 1)[0])
        except Exception:
            continue
        if num < 10 or num > 26:
            continue
        conn.executescript(path.read_text(encoding="utf-8"))
    conn.commit()


def _load_api(db_path: Path):
    os.environ["DB_PATH"] = str(db_path)
    app_mod = importlib.import_module("app")
    app_mod.DB_PATH = str(db_path)
    return importlib.reload(app_mod)


@pytest.fixture()
def runtime_db(tmp_path: Path):
    db_path = tmp_path / "runtime_p4.db"
    os.environ["DB_PATH"] = str(db_path)
    p2.DB_PATH = str(db_path)
    worker.DB_PATH = str(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
    return db_path


def test_regulation_runs_idempotent(runtime_db: Path) -> None:
    reg = p2.create_regulation("Monthly Check", day_of_month=5, source_msg_id="r1")
    runs1 = p2.ensure_regulation_runs("2026-02")
    assert len(runs1) == 1
    run_id = runs1[0].id
    with sqlite3.connect(str(runtime_db)) as conn:
        run_cnt = conn.execute("SELECT COUNT(*) FROM regulation_runs").fetchone()[0]
        task_cnt = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE parent_type = 'regulation_run' AND parent_id = ?",
            (int(run_id),),
        ).fetchone()[0]
    assert run_cnt == 1
    assert task_cnt == 1

    runs2 = p2.ensure_regulation_runs("2026-02")
    assert len(runs2) == 1
    with sqlite3.connect(str(runtime_db)) as conn:
        run_cnt2 = conn.execute("SELECT COUNT(*) FROM regulation_runs").fetchone()[0]
        task_cnt2 = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE parent_type = 'regulation_run' AND parent_id = ?",
            (int(run_id),),
        ).fetchone()[0]
    assert run_cnt2 == 1
    assert task_cnt2 == 1

    _ = reg  # keep reference for clarity


def test_mark_regulation_done_open(runtime_db: Path) -> None:
    p2.create_regulation("Monthly Check", day_of_month=10, source_msg_id="r2")
    runs = p2.ensure_regulation_runs("2026-03")
    run = runs[0]
    updated = p2.mark_regulation_done(run.id, done_at="2026-03-10T10:00:00+00:00")
    assert updated.status == "DONE"
    assert updated.done_at is not None


def test_mark_regulation_done_missed(runtime_db: Path) -> None:
    p2.create_regulation("Monthly Check", day_of_month=12, source_msg_id="r3")
    runs = p2.ensure_regulation_runs("2026-04")
    run = runs[0]
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.execute("UPDATE regulation_runs SET status = 'MISSED' WHERE id = ?", (int(run.id),))
        conn.commit()
    updated = p2.mark_regulation_done(run.id, done_at="2026-04-20T10:00:00+00:00")
    assert updated.status == "MISSED"
    assert updated.done_at is not None


def test_monthly_tick_idempotent_and_disabled(runtime_db: Path) -> None:
    reg = p2.create_regulation("Monthly A", day_of_month=2, source_msg_id="r4")
    created1 = p2.monthly_regulation_tick(date(2026, 2, 1))
    assert len(created1) == 1
    created2 = p2.monthly_regulation_tick(date(2026, 2, 1))
    assert created2 == []
    p2.disable_regulation(reg.id)
    created3 = p2.monthly_regulation_tick(date(2026, 3, 1))
    assert created3 == []


def test_complete_skip_idempotent(runtime_db: Path) -> None:
    p2.create_regulation("Monthly B", day_of_month=3, source_msg_id="r5")
    runs = p2.ensure_regulation_runs("2026-02")
    run_id = runs[0].id
    done = p2.complete_regulation_run(run_id, done_at="2026-02-03T10:00:00+00:00")
    assert done.status == "DONE"
    done2 = p2.complete_regulation_run(run_id, done_at="2026-02-04T10:00:00+00:00")
    assert done2.status == "DONE"
    skipped = p2.skip_regulation_run(run_id)
    assert skipped.status == "DONE"


def test_api_regulations_and_runs_shape(tmp_path: Path) -> None:
    db_path = tmp_path / "p4_api.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute(
            """
            INSERT INTO regulations (id, title, note, status, day_of_month, due_time_local, source_msg_id, created_at, updated_at)
            VALUES (1, 'Reg A', NULL, 'ACTIVE', 5, '10:00', 's1', 't', 't')
            """
        )
        conn.execute(
            """
            INSERT INTO regulation_runs (id, regulation_id, period_key, status, due_date, due_time_local, done_at, created_at, updated_at)
            VALUES (10, 1, '2026-02', 'OPEN', '2026-02-05', '10:00', NULL, 't', 't')
            """
        )
        conn.commit()

    app_mod = _load_api(db_path)
    regs = app_mod.list_regulations()
    assert len(regs) == 1
    assert set(regs[0].keys()) == {
        "id",
        "title",
        "note",
        "status",
        "day_of_month",
        "due_time_local",
        "source_msg_id",
        "created_at",
        "updated_at",
    }
    runs = app_mod.list_regulation_runs(period_key="2026-02")
    assert len(runs) == 1
    assert set(runs[0].keys()) == {
        "id",
        "regulation_id",
        "period_key",
        "status",
        "due_date",
        "due_time_local",
        "done_at",
        "created_at",
        "updated_at",
    }


def test_api_regulation_runs_for_reg(tmp_path: Path) -> None:
    db_path = tmp_path / "p4_api_reg_runs.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute(
            """
            INSERT INTO regulations (id, title, note, status, day_of_month, due_time_local, source_msg_id, created_at, updated_at)
            VALUES (1, 'Reg A', NULL, 'ACTIVE', 5, '10:00', 's1', 't', 't')
            """
        )
        conn.execute(
            """
            INSERT INTO regulation_runs (id, regulation_id, period_key, status, due_date, due_time_local, done_at, created_at, updated_at)
            VALUES (10, 1, '2026-02', 'OPEN', '2026-02-05', '10:00', NULL, 't', 't')
            """
        )
        conn.commit()

    app_mod = _load_api(db_path)
    rows = app_mod.list_regulation_runs_for_reg(1, period="2026-02")
    assert len(rows) == 1
    assert rows[0]["regulation_id"] == 1
