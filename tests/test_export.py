import csv
import importlib
import io
import os
import sqlite3
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "migrations"
API_PATH = ROOT / "organizer-api"

sys.path.append(str(API_PATH))


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


def _read_zip_csv(response) -> dict[str, list[list[str]]]:
    data: dict[str, list[list[str]]] = {}
    with zipfile.ZipFile(io.BytesIO(response.body), "r") as zf:
        for name in zf.namelist():
            text = zf.read(name).decode("utf-8")
            rows = list(csv.reader(io.StringIO(text)))
            data[name] = rows
    return data


def test_export_schema_snapshot(tmp_path: Path) -> None:
    db_path = tmp_path / "export_schema.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute(
            """
            INSERT INTO tasks (id, title, status, state, planned_at, calendar_event_id, source_msg_id,
                               parent_type, parent_id, created_at, updated_at, completed_at)
            VALUES (1, 'Task A', 'NEW', 'NEW', NULL, NULL, 'm1', NULL, NULL, '2026-02-01T10:00:00+00:00',
                    '2026-02-01T10:00:00+00:00', NULL)
            """
        )
        conn.execute(
            """
            INSERT INTO subtasks (id, task_id, title, status, source_msg_id, created_at, updated_at, completed_at)
            VALUES (10, 1, 'Sub A', 'NEW', 's1', '2026-02-01T11:00:00+00:00', '2026-02-01T11:00:00+00:00', NULL)
            """
        )
        conn.execute(
            """
            INSERT INTO regulations (id, title, note, status, day_of_month, due_time_local, source_msg_id,
                                     created_at, updated_at)
            VALUES (2, 'Reg A', NULL, 'ACTIVE', 5, '10:00', 'r1', '2026-02-01T12:00:00+00:00',
                    '2026-02-01T12:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO regulation_runs (id, regulation_id, period_key, status, due_date, due_time_local, done_at,
                                         created_at, updated_at)
            VALUES (20, 2, '2026-02', 'OPEN', '2026-02-05', '10:00', NULL, '2026-02-01T12:30:00+00:00',
                    '2026-02-01T12:30:00+00:00')
            """
        )
        conn.commit()

    app_mod = _load_api(db_path)
    payload = app_mod.export_json()
    assert set(payload.keys()) == {"metadata", "tasks", "regulations", "regulation_runs"}
    assert set(payload["metadata"].keys()) == {"exported_at", "timezone"}
    assert len(payload["tasks"]) == 1
    assert set(payload["tasks"][0].keys()) == {
        "id",
        "title",
        "status",
        "state",
        "planned_at",
        "calendar_event_id",
        "source_msg_id",
        "parent_type",
        "parent_id",
        "created_at",
        "updated_at",
        "completed_at",
        "subtasks",
    }
    assert set(payload["tasks"][0]["subtasks"][0].keys()) == {
        "id",
        "task_id",
        "title",
        "status",
        "source_msg_id",
        "created_at",
        "updated_at",
        "completed_at",
    }
    assert set(payload["regulations"][0].keys()) == {
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
    assert set(payload["regulation_runs"][0].keys()) == {
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

    csv_resp = app_mod.export_csv()
    csv_data = _read_zip_csv(csv_resp)
    assert set(csv_data.keys()) == {
        "tasks.csv",
        "subtasks.csv",
        "regulations.csv",
        "regulation_runs.csv",
    }
    assert csv_data["tasks.csv"][0] == app_mod.TASK_COLUMNS
    assert csv_data["subtasks.csv"][0] == app_mod.SUBTASK_COLUMNS
    assert csv_data["regulations.csv"][0] == app_mod.REGULATION_COLUMNS
    assert csv_data["regulation_runs.csv"][0] == app_mod.REGULATION_RUN_COLUMNS


def test_export_empty_db(tmp_path: Path) -> None:
    db_path = tmp_path / "export_empty.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)

    app_mod = _load_api(db_path)
    payload = app_mod.export_json()
    assert payload["tasks"] == []
    assert payload["regulations"] == []
    assert payload["regulation_runs"] == []

    csv_resp = app_mod.export_csv()
    csv_data = _read_zip_csv(csv_resp)
    assert len(csv_data["tasks.csv"]) == 1
    assert len(csv_data["subtasks.csv"]) == 1
    assert len(csv_data["regulations.csv"]) == 1
    assert len(csv_data["regulation_runs.csv"]) == 1


def test_export_date_filter(tmp_path: Path) -> None:
    db_path = tmp_path / "export_filter.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute(
            """
            INSERT INTO tasks (id, title, status, state, planned_at, calendar_event_id, source_msg_id,
                               parent_type, parent_id, created_at, updated_at, completed_at)
            VALUES (1, 'Task Old', 'NEW', 'NEW', NULL, NULL, NULL, NULL, NULL, '2026-02-01T00:00:00+00:00',
                    '2026-02-01T00:00:00+00:00', NULL)
            """
        )
        conn.execute(
            """
            INSERT INTO tasks (id, title, status, state, planned_at, calendar_event_id, source_msg_id,
                               parent_type, parent_id, created_at, updated_at, completed_at)
            VALUES (2, 'Task New', 'NEW', 'NEW', NULL, NULL, NULL, NULL, NULL, '2026-02-10T00:00:00+00:00',
                    '2026-02-10T00:00:00+00:00', NULL)
            """
        )
        conn.execute(
            """
            INSERT INTO regulations (id, title, note, status, day_of_month, due_time_local, source_msg_id,
                                     created_at, updated_at)
            VALUES (3, 'Reg Old', NULL, 'ACTIVE', 1, NULL, NULL, '2026-02-01T00:00:00+00:00',
                    '2026-02-01T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO regulations (id, title, note, status, day_of_month, due_time_local, source_msg_id,
                                     created_at, updated_at)
            VALUES (4, 'Reg New', NULL, 'ACTIVE', 10, NULL, NULL, '2026-02-10T00:00:00+00:00',
                    '2026-02-10T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO regulation_runs (id, regulation_id, period_key, status, due_date, due_time_local, done_at,
                                         created_at, updated_at)
            VALUES (30, 3, '2026-02', 'OPEN', '2026-02-05', NULL, NULL, '2026-02-01T00:00:00+00:00',
                    '2026-02-01T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO regulation_runs (id, regulation_id, period_key, status, due_date, due_time_local, done_at,
                                         created_at, updated_at)
            VALUES (31, 4, '2026-02', 'OPEN', '2026-02-15', NULL, NULL, '2026-02-10T00:00:00+00:00',
                    '2026-02-10T00:00:00+00:00')
            """
        )
        conn.commit()

    app_mod = _load_api(db_path)
    payload = app_mod.export_json(from_="2026-02-01", to="2026-02-05")
    assert [t["id"] for t in payload["tasks"]] == [1]
    assert [r["id"] for r in payload["regulations"]] == [3]
    assert [r["id"] for r in payload["regulation_runs"]] == [30]

    csv_resp = app_mod.export_csv(from_="2026-02-10", to="2026-02-10")
    csv_data = _read_zip_csv(csv_resp)
    assert csv_data["tasks.csv"][1][0] == "2"
    assert csv_data["regulations.csv"][1][0] == "4"
    assert csv_data["regulation_runs.csv"][1][0] == "31"
