import importlib
import logging
import os
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "migrations"
WORKER_ROOT = ROOT / "organizer-worker"
API_ROOT = ROOT / "organizer-api"

sys.path.append(str(WORKER_ROOT))
sys.path.append(str(API_ROOT))

from organizer_worker import db, handlers  # noqa: E402


def _apply_runtime_migrations(conn: sqlite3.Connection) -> None:
    migs = sorted(p for p in MIGRATIONS_DIR.iterdir() if p.name.endswith(".sql"))
    for path in migs:
        try:
            _ = int(path.name.split("_", 1)[0])
        except Exception:
            continue
        conn.executescript(path.read_text(encoding="utf-8"))
    conn.commit()


def _load_api(db_path: Path):
    os.environ["DB_PATH"] = str(db_path)
    app_mod = importlib.import_module("app")
    app_mod.DB_PATH = str(db_path)
    return importlib.reload(app_mod)


@pytest.fixture()
def runtime_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "runtime_task_hierarchy.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    db.DB_PATH = str(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
    return db_path


def test_create_root_task_without_parent_still_works(runtime_db: Path) -> None:
    res = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Root task"}})
    assert res["ok"] is True
    task_id = int(res["debug"]["task_id"])
    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute("SELECT parent_task_id FROM tasks WHERE id = ?", (task_id,)).fetchone()
    assert row is not None
    assert row[0] is None


def test_create_subtask_under_existing_root(runtime_db: Path) -> None:
    root = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Root task"}})
    root_id = int(root["debug"]["task_id"])

    res = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {"title": "Child task", "parent_task_id": root_id, "parent_task_explicit": True},
        }
    )
    assert res["ok"] is True
    child_id = int(res["debug"]["task_id"])

    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute("SELECT parent_task_id FROM tasks WHERE id = ?", (child_id,)).fetchone()
    assert row is not None
    assert int(row[0]) == root_id


def test_create_subtask_under_subtask(runtime_db: Path) -> None:
    root = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Root task"}})
    root_id = int(root["debug"]["task_id"])
    child = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {"title": "Child task", "parent_task_id": root_id, "parent_task_explicit": True},
        }
    )
    child_id = int(child["debug"]["task_id"])

    res = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {"title": "Grandchild task", "parent_task_id": child_id, "parent_task_explicit": True},
        }
    )
    assert res["ok"] is True
    grandchild_id = int(res["debug"]["task_id"])

    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute("SELECT parent_task_id FROM tasks WHERE id = ?", (grandchild_id,)).fetchone()
    assert row is not None
    assert int(row[0]) == child_id


def test_non_explicit_parent_task_id_is_cleared_for_task_create(runtime_db: Path) -> None:
    root = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Root task"}})
    root_id = int(root["debug"]["task_id"])

    res = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {"title": "Should remain root", "parent_task_id": root_id, "parent_task_explicit": False},
            "source": {"channel": "telegram"},
        }
    )
    assert res["ok"] is True
    task_id = int(res["debug"]["task_id"])

    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute("SELECT parent_task_id FROM tasks WHERE id = ?", (task_id,)).fetchone()
    assert row is not None
    assert row[0] is None


def test_explicit_parent_task_id_is_kept_for_task_create(runtime_db: Path) -> None:
    root = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Root task"}})
    root_id = int(root["debug"]["task_id"])

    res = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {"title": "Explicit child", "parent_task_id": root_id, "parent_task_explicit": True},
            "source": {"channel": "sheets"},
        }
    )
    assert res["ok"] is True
    task_id = int(res["debug"]["task_id"])

    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute("SELECT parent_task_id FROM tasks WHERE id = ?", (task_id,)).fetchone()
    assert row is not None
    assert int(row[0]) == root_id


def test_reject_missing_parent_task(runtime_db: Path) -> None:
    res = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Child task", "parent_task_id": 99999}}
    )
    assert res["ok"] is False
    assert "parent task not found" in str(res.get("debug", {}).get("error") or "")


def test_reject_self_parent(runtime_db: Path) -> None:
    root = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Self task"}})
    task_id = int(root["debug"]["task_id"])

    res = handlers.dispatch_intent(
        {"intent": "task.parent.update", "entities": {"task_id": task_id, "parent_task_id": task_id}}
    )
    assert res["ok"] is False
    assert "own parent" in str(res.get("debug", {}).get("error") or "")


def test_multi_level_nesting_is_allowed(runtime_db: Path) -> None:
    root = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Root"}})
    child = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {"title": "Child", "parent_task_id": int(root["debug"]["task_id"]), "parent_task_explicit": True},
        }
    )
    assert child["ok"] is True

    res = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {"title": "Grandchild", "parent_task_id": int(child["debug"]["task_id"]), "parent_task_explicit": True},
        }
    )
    assert res["ok"] is True


def test_reject_cycle(runtime_db: Path) -> None:
    a = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "A"}})
    b = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "B"}})
    a_id = int(a["debug"]["task_id"])
    b_id = int(b["debug"]["task_id"])

    first_move = handlers.dispatch_intent(
        {"intent": "task.parent.update", "entities": {"task_id": a_id, "parent_task_id": b_id}}
    )
    assert first_move["ok"] is True

    cycle = handlers.dispatch_intent(
        {"intent": "task.parent.update", "entities": {"task_id": b_id, "parent_task_id": a_id}}
    )
    assert cycle["ok"] is False
    assert "cycle" in str(cycle.get("debug", {}).get("error") or "")


def test_read_api_exposes_hierarchy_fields(runtime_db: Path) -> None:
    root = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Parent"}})
    root_id = int(root["debug"]["task_id"])
    child = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Child", "parent_task_id": root_id, "parent_task_explicit": True}}
    )
    child_id = int(child["debug"]["task_id"])
    grandchild = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Grandchild", "parent_task_id": child_id, "parent_task_explicit": True}}
    )
    grandchild_id = int(grandchild["debug"]["task_id"])

    app_mod = _load_api(runtime_db)
    task = app_mod.get_task(child_id)
    grandchild_task = app_mod.get_task(grandchild_id)
    rows = app_mod.list_tasks()

    assert task["parent_task_id"] == root_id
    assert task["parent_title"] == "Parent"
    assert task["level"] == 2
    assert task["depth"] == 2
    assert grandchild_task["parent_task_id"] == child_id
    assert grandchild_task["parent_title"] == "Child"
    assert grandchild_task["level"] == 3
    assert grandchild_task["depth"] == 3

    root_row = next(row for row in rows if int(row["id"]) == root_id)
    child_row = next(row for row in rows if int(row["id"]) == child_id)
    grandchild_row = next(row for row in rows if int(row["id"]) == grandchild_id)
    assert root_row["parent_task_id"] is None
    assert root_row["level"] == 1
    assert child_row["parent_task_id"] == root_id
    assert child_row["parent_title"] == "Parent"
    assert grandchild_row["parent_task_id"] == child_id
    assert grandchild_row["parent_title"] == "Child"
    assert grandchild_row["level"] == 3


def test_task_create_duplicate_same_day_returns_confirmation(runtime_db: Path) -> None:
    handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Купить молоко", "planned_at": "2026-05-18T09:00:00Z"}}
    )

    res = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "создай задачу купить молоко", "planned_at": "2026-05-18T18:00:00Z"}}
    )

    assert res["ok"] is False
    assert res["missing_field"] == "task_create_duplicate_confirm"
    assert res["needs_clarification"] is True
    assert "Похоже, такая задача уже есть" in str(res.get("clarifying_question") or "")


def test_task_create_duplicate_override_allows_second_task(runtime_db: Path) -> None:
    first = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Купить молоко", "planned_at": "2026-05-18T09:00:00Z"}}
    )
    second = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {
                "title": "Купить молоко",
                "planned_at": "2026-05-18T12:00:00Z",
                "duplicate_check_override": True,
            },
        }
    )

    assert first["ok"] is True
    assert second["ok"] is True
    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute("SELECT COUNT(1) FROM tasks WHERE title = ?", ("Купить молоко",)).fetchone()
    assert row is not None
    assert int(row[0]) == 2


def test_task_create_same_title_different_day_is_not_duplicate(runtime_db: Path) -> None:
    handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Купить молоко", "planned_at": "2026-05-18T09:00:00Z"}}
    )
    res = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Купить молоко", "planned_at": "2026-05-19T09:00:00Z"}}
    )
    assert res["ok"] is True


def test_task_create_done_or_cancelled_existing_does_not_block_duplicate(runtime_db: Path) -> None:
    created = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Купить молоко", "planned_at": "2026-05-18T09:00:00Z"}}
    )
    task_id = int(created["debug"]["task_id"])
    handlers.dispatch_intent({"intent": "task.set_status", "entities": {"task_id": task_id, "status": "DONE"}})
    res = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Купить молоко", "planned_at": "2026-05-18T12:00:00Z"}}
    )
    assert res["ok"] is True


def test_task_create_same_title_same_day_different_parent_scope_is_not_duplicate(runtime_db: Path) -> None:
    root_a = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Parent A"}})
    root_b = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Parent B"}})
    parent_a = int(root_a["debug"]["task_id"])
    parent_b = int(root_b["debug"]["task_id"])
    handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {
                "title": "Купить молоко",
                "planned_at": "2026-05-18T09:00:00Z",
                "parent_task_id": parent_a,
                "parent_task_explicit": True,
            },
        }
    )
    res = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {
                "title": "Купить молоко",
                "planned_at": "2026-05-18T10:00:00Z",
                "parent_task_id": parent_b,
                "parent_task_explicit": True,
            },
        }
    )
    assert res["ok"] is True


def test_task_create_same_title_same_day_same_parent_scope_warns(runtime_db: Path) -> None:
    root = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Parent"}})
    parent_id = int(root["debug"]["task_id"])
    handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {
                "title": "Купить молоко",
                "planned_at": "2026-05-18T09:00:00Z",
                "parent_task_id": parent_id,
                "parent_task_explicit": True,
            },
        }
    )
    res = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {
                "title": "Купить молоко",
                "planned_at": "2026-05-18T11:00:00Z",
                "parent_task_id": parent_id,
                "parent_task_explicit": True,
            },
        }
    )
    assert res["ok"] is False
    assert res["missing_field"] == "task_create_duplicate_confirm"


def test_task_create_due_date_is_persisted_into_planned_at(runtime_db: Path) -> None:
    res = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Купить молоко", "due_date": "2026-05-18"}}
    )

    assert res["ok"] is True
    task_id = int(res["debug"]["task_id"])
    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute("SELECT planned_at FROM tasks WHERE id = ?", (task_id,)).fetchone()
    assert row is not None
    assert str(row[0] or "") == "2026-05-18"


def test_task_create_duplicate_check_uses_persisted_planned_day_from_due_date_fallback(runtime_db: Path) -> None:
    first = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Купить молоко", "due_date": "2026-05-18"}}
    )
    second = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Купить молоко", "planned_at": "2026-05-18T18:00:00Z"}}
    )

    assert first["ok"] is True
    assert second["ok"] is False
    assert second["missing_field"] == "task_create_duplicate_confirm"


def test_task_create_duplicate_same_calendar_day_matches_existing_midnight_value(runtime_db: Path) -> None:
    first = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Купить молоко", "planned_at": "2026-05-18 00:00"}}
    )
    second = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Купить молоко", "planned_at": "2026-05-18"}}
    )

    assert first["ok"] is True
    assert second["ok"] is False
    assert second["missing_field"] == "task_create_duplicate_confirm"


def test_task_create_duplicate_same_calendar_day_matches_existing_afternoon_value(runtime_db: Path) -> None:
    first = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Купить молоко", "planned_at": "2026-05-18 15:00"}}
    )
    second = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Купить молоко", "planned_at": "2026-05-18 00:00"}}
    )

    assert first["ok"] is True
    assert second["ok"] is False
    assert second["missing_field"] == "task_create_duplicate_confirm"


def test_task_create_duplicate_root_scope_treats_null_and_empty_parent_as_same(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.execute(
            """
            INSERT INTO tasks (
                title, status, state, planned_at, source_msg_id, parent_type, parent_id, parent_task_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Купить молоко",
                "NEW",
                "NEW",
                "2026-05-18 00:00",
                "msg-empty-parent",
                None,
                None,
                "",
                "2026-05-17T09:00:00Z",
                "2026-05-17T09:00:00Z",
            ),
        )
        conn.commit()

    second = handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Купить молоко", "planned_at": "2026-05-18"}}
    )

    assert second["ok"] is False
    assert second["missing_field"] == "task_create_duplicate_confirm"


def test_task_create_persists_comment_and_logs(runtime_db: Path, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO):
        res = handlers.dispatch_intent(
            {
                "intent": "task.create",
                "entities": {
                    "title": "Купить молоко",
                    "planned_at": "2026-05-18",
                    "comment_text": "взять 2 литра",
                },
            }
        )

    assert res["ok"] is True
    task_id = int(res["debug"]["task_id"])
    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute("SELECT comment FROM tasks WHERE id = ?", (task_id,)).fetchone()
    assert row is not None
    assert str(row[0] or "") == "взять 2 литра"
    assert any("task_create_comment_persisted" in record.getMessage() for record in caplog.records)


def test_task_create_duplicate_guard_ignores_comment_field(runtime_db: Path) -> None:
    first = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {
                "title": "Купить молоко",
                "planned_at": "2026-05-18",
                "comment_text": "взять 2 литра",
            },
        }
    )
    second = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {
                "title": "Купить молоко",
                "planned_at": "2026-05-18",
                "comment_text": "не забыть чек",
            },
        }
    )

    assert first["ok"] is True
    assert second["ok"] is False
    assert second["missing_field"] == "task_create_duplicate_confirm"


def test_task_create_duplicate_undated_root_inbox_returns_confirmation(runtime_db: Path) -> None:
    handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Позвонить врачу"}})

    res = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "позвонить врачу"}})

    assert res["ok"] is False
    assert res["missing_field"] == "task_create_duplicate_confirm"
    assert "Такая задача уже есть в InBox" in str(res.get("clarifying_question") or "")


def test_task_create_duplicate_undated_done_existing_does_not_block(runtime_db: Path) -> None:
    created = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Позвонить врачу"}})
    task_id = int(created["debug"]["task_id"])
    handlers.dispatch_intent({"intent": "task.set_status", "entities": {"task_id": task_id, "status": "DONE"}})

    res = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "позвонить врачу"}})

    assert res["ok"] is True


def test_task_create_undated_does_not_match_existing_dated_same_title(runtime_db: Path) -> None:
    handlers.dispatch_intent(
        {"intent": "task.create", "entities": {"title": "Позвонить врачу", "planned_at": "2026-05-23"}}
    )

    res = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "позвонить врачу"}})

    assert res["ok"] is True


def test_task_create_undated_root_does_not_match_same_title_under_other_parent(runtime_db: Path) -> None:
    parent = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Проект"}})
    parent_id = int(parent["debug"]["task_id"])
    handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {
                "title": "Позвонить врачу",
                "parent_task_id": parent_id,
                "parent_task_explicit": True,
            },
        }
    )

    res = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "позвонить врачу"}})

    assert res["ok"] is True


def test_task_create_duplicate_undated_same_parent_scope_returns_confirmation(runtime_db: Path) -> None:
    parent = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Проект"}})
    parent_id = int(parent["debug"]["task_id"])
    handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {
                "title": "Позвонить врачу",
                "parent_task_id": parent_id,
                "parent_task_explicit": True,
            },
        }
    )

    res = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {
                "title": "позвонить врачу",
                "parent_task_id": parent_id,
                "parent_task_explicit": True,
            },
        }
    )

    assert res["ok"] is False
    assert res["missing_field"] == "task_create_duplicate_confirm"
    assert "Такая задача уже есть без срока" in str(res.get("clarifying_question") or "")


def test_task_create_duplicate_undated_override_allows_second_task(runtime_db: Path) -> None:
    first = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Позвонить врачу"}})
    second = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "entities": {"title": "Позвонить врачу", "duplicate_check_override": True},
        }
    )

    assert first["ok"] is True
    assert second["ok"] is True
