import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "migrations"
sys.path.append(str(ROOT))

from src.google import runtime_sheets_reverse_sync as reverse_sync
from src.google.runtime_sheets_reverse_sync import apply_review_changes, build_reverse_sync_review, format_review_text
from src.google.runtime_sheets_sync import build_runtime_sheet_payloads


def _apply_runtime_migrations(conn: sqlite3.Connection) -> None:
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
        conn.executescript(path.read_text(encoding="utf-8"))
    conn.commit()


@pytest.fixture()
def runtime_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "runtime.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute("ALTER TABLE tasks ADD COLUMN comment TEXT NULL")
        conn.execute("ALTER TABLE tasks ADD COLUMN parent_task_id INTEGER NULL")
        conn.execute("ALTER TABLE time_blocks ADD COLUMN comment TEXT NULL")
        conn.execute("ALTER TABLE time_blocks ADD COLUMN user_id TEXT NULL")
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, status, state, planned_at, calendar_event_id,
                source_msg_id, parent_type, parent_id, created_at, updated_at, completed_at, comment
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                101,
                "Buy milk",
                "NEW",
                "NEW",
                "2026-05-12T12:00:00Z",
                "",
                "msg-1",
                "",
                None,
                "2026-05-11T10:00:00Z",
                "2026-05-11T10:00:00Z",
                None,
                "from runtime",
            ),
        )
        conn.execute(
            """
            INSERT INTO time_blocks (id, task_id, user_id, start_at, end_at, created_at, comment)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                201,
                101,
                "u-1",
                "2026-05-12T15:00:00Z",
                "2026-05-12T15:30:00Z",
                "2026-05-11T10:05:00Z",
                "comment-1",
            ),
        )
        conn.commit()
    return db_path


class FakeSheetsClient:
    def __init__(self, tabs: dict[str, list[list[str]]]) -> None:
        self.tabs = tabs
        self.deleted_rows: list[tuple[str, int]] = []
        self.fail_delete_for: set[tuple[str, int]] = set()

    def read_range(self, spreadsheet_id: str, a1_range: str):
        _ = spreadsheet_id
        sheet_name = a1_range.split("!", 1)[0].strip("'")
        return [list(row) for row in self.tabs.get(sheet_name, [])]

    def delete_row(self, spreadsheet_id: str, *, sheet_name: str, row_number: int) -> None:
        _ = spreadsheet_id
        key = (sheet_name, int(row_number))
        if key in self.fail_delete_for:
            raise RuntimeError(f"delete failed for {sheet_name}:{row_number}")
        rows = self.tabs.get(sheet_name, [])
        if int(row_number) <= 1 or int(row_number) > len(rows):
            raise RuntimeError("invalid row number")
        self.deleted_rows.append(key)
        del rows[int(row_number) - 1]


def _write_review_file(tmp_path: Path, payload: dict) -> Path:
    path = tmp_path / "review.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _build_tabs(db_path: Path) -> dict[str, list[list[str]]]:
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    tabs: dict[str, list[list[str]]] = {}
    for sheet_name, (header, rows) in payloads.items():
        tabs[sheet_name] = [list(map(str, header))] + [[str(value) for value in row] for row in rows]
    return tabs


def test_reverse_sync_unchanged_rows_are_ignored(runtime_db: Path) -> None:
    client = FakeSheetsClient(_build_tabs(runtime_db))
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"] == {
        "ignored": 0,
        "proposed": 0,
        "proposed_create": 0,
        "confirm_required": 0,
        "conflict": 0,
        "invalid": 0,
        "total_changes": 0,
    }
    assert result["changes"] == []


def test_reverse_sync_blank_new_task_row_is_ignored(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"].append(["", "", "", "", "", "", "", "", "", "", "", "", ""])
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["total_changes"] == 0
    assert result["changes"] == []


def test_reverse_sync_new_task_row_with_title_becomes_proposed_create(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"].append(["", "", "", "", "", "Sheet created task", "", "", "", "", "", "", ""])
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["proposed"] == 1
    assert result["summary"]["proposed_create"] == 1
    assert result["changes"][0]["status"] == "proposed_create"
    assert result["changes"][0]["field"] == "task"
    assert result["changes"][0]["entity_id"] is None
    assert result["changes"][0]["sheet_value"]["title"] == "Sheet created task"
    assert result["changes"][0]["sheet_value"]["parent_task_explicit"] is False
    assert result["changes"][0]["source_sheet_row"] == 3
    assert result["changes"][0]["source_row_hash"]
    assert result["changes"][0]["source_row_values"]["Задача"] == "Sheet created task"


def test_reverse_sync_new_task_row_with_comment_becomes_proposed_create(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"].append(["", "", "", "", "", "Sheet created task", "", "", "", "", "", "note from sheet", ""])
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["changes"][0]["status"] == "proposed_create"
    assert result["changes"][0]["sheet_value"]["comment"] == "note from sheet"


def test_reverse_sync_duplicate_new_task_row_becomes_confirm_required(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"].append(["", "", "", "", "", "Buy milk", "", "", "12.05.2026 20:00", "", "", "", ""])
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["confirm_required"] == 1
    assert result["summary"]["proposed_create"] == 0
    assert result["changes"][0]["status"] == "confirm_required"
    assert result["changes"][0]["field"] == "task"
    assert "такая задача уже есть" in str(result["changes"][0]["question"]).lower()


def test_reverse_sync_editable_task_field_becomes_proposed(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"][1][5] = "Buy milk and bread"
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["proposed"] == 1
    assert result["changes"][0]["field"] == "Задача"
    assert result["changes"][0]["status"] == "proposed"


def test_reverse_sync_non_editable_field_change_is_invalid(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"][1][9] = "12.05.2026 12:00"
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["invalid"] == 1
    assert result["changes"][0]["field"] == "Создано"
    assert result["changes"][0]["status"] == "invalid"


def test_reverse_sync_stale_db_version_becomes_conflict(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"][1][5] = "Buy milk from sheet"
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.execute(
            "UPDATE tasks SET title = ?, updated_at = ? WHERE id = ?",
            ("Buy milk from db", "2026-05-11T11:00:00Z", 101),
        )
        conn.commit()
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["conflict"] == 1
    assert result["changes"][0]["status"] == "conflict"


def test_reverse_sync_invalid_task_date_is_invalid(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"][1][8] = "tomorrow"
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["invalid"] == 1
    assert result["changes"][0]["field"] == "План"
    assert result["changes"][0]["status"] == "invalid"


def test_reverse_sync_new_task_row_with_invalid_date_is_invalid(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"].append(["", "", "", "", "", "Sheet created task", "", "", "tomorrow", "", "", "", ""])
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["invalid"] == 1
    assert result["changes"][0]["status"] == "invalid"
    assert "План" in result["changes"][0]["question"]


def test_reverse_sync_unknown_id_is_invalid(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"].append(["999", "999", "1", "", "", "Unknown task", "NEW", "NEW", "", "11.05.2026 10:00", "11.05.2026 10:00", "", "0"])
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["invalid"] == 1
    assert result["changes"][0]["field"] == "ID"
    assert result["changes"][0]["status"] == "invalid"


def test_reverse_sync_new_task_row_with_valid_parent_id_becomes_proposed_create(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"].append(["", "", "", "101", "", "Sheet created subtask", "", "", "", "", "", "child note", ""])
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["proposed_create"] == 1
    assert result["changes"][0]["status"] == "proposed_create"
    assert result["changes"][0]["sheet_value"]["parent_task_id"] == 101
    assert result["changes"][0]["sheet_value"]["parent_task_explicit"] is True
    assert result["changes"][0]["sheet_value"]["comment"] == "child note"


def test_reverse_sync_new_task_row_with_subtask_parent_id_becomes_proposed_create(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, status, state, planned_at, calendar_event_id,
                source_msg_id, parent_type, parent_id, created_at, updated_at, completed_at, comment, parent_task_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                102,
                "Existing child",
                "NEW",
                "NEW",
                None,
                "",
                "",
                "",
                None,
                "2026-05-11T11:00:00Z",
                "2026-05-11T11:00:00Z",
                None,
                "",
                101,
            ),
        )
        conn.commit()
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"].append(["", "", "", "102", "", "Sheet created grandchild", "", "", "", "", "", "", ""])
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["proposed_create"] == 1
    assert result["changes"][0]["status"] == "proposed_create"
    assert result["changes"][0]["sheet_value"]["parent_task_id"] == 102
    assert result["changes"][0]["sheet_value"]["parent_task_explicit"] is True


def test_reverse_sync_new_task_row_with_invalid_parent_id_is_invalid(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"].append(["", "", "", "999", "", "Sheet created subtask", "", "", "", "", "", "", ""])
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["invalid"] == 1
    assert result["changes"][0]["status"] == "invalid"
    assert "Родитель ID" in result["changes"][0]["question"]


def test_reverse_sync_existing_parent_id_change_is_confirm_required(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"][1][3] = "102"
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["confirm_required"] == 1
    assert result["changes"][0]["field"] == "Родитель ID"
    assert result["changes"][0]["status"] == "confirm_required"


def test_reverse_sync_edited_tree_number_is_ignored(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"][1][0] = "999.9"
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["total_changes"] == 0
    assert result["changes"] == []


def test_reverse_sync_parent_display_name_edit_is_invalid(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, status, state, planned_at, calendar_event_id,
                source_msg_id, parent_type, parent_id, created_at, updated_at, completed_at, comment, parent_task_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                102,
                "Child task",
                "NEW",
                "NEW",
                None,
                "",
                "",
                "",
                None,
                "2026-05-11T11:00:00Z",
                "2026-05-11T11:00:00Z",
                None,
                "",
                101,
            ),
        )
        conn.commit()
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"][2][4] = "Edited parent title"
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["invalid"] == 1
    assert result["changes"][0]["field"] == "Родитель"
    assert result["changes"][0]["status"] == "invalid"


def test_reverse_sync_new_calendar_row_without_id_remains_blocked(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Календарь"].append(["Блок времени", "Manual block", "18.05.2026", "Пн", "18.05.2026 15:00", "18.05.2026 15:30", "15:00 - 15:30", "30", "", "", "", "", "sheet"])
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["invalid"] == 1
    assert result["changes"][0]["field"] == "ID"


def test_reverse_sync_calendar_meeting_row_is_invalid_for_now(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Календарь"].append(["Встреча", "Meeting", "18.05.2026", "Пн", "18.05.2026 15:00", "18.05.2026 16:00", "15:00 - 16:00", "60", "", "", "meeting_event:777", "777", "runtime_known_meeting"])
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["invalid"] == 1
    assert result["changes"][0]["status"] == "invalid"
    assert result["changes"][0]["entity_type"] == "meeting"


def test_reverse_sync_calendar_timeblock_date_change_requires_confirmation(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Календарь"][1][4] = "12.05.2026 16:00"
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    assert result["summary"]["confirm_required"] == 1
    assert result["changes"][0]["field"] == "Начало"
    assert result["changes"][0]["status"] == "confirm_required"


def test_reverse_sync_unified_external_calendar_row_is_not_applyable(runtime_db: Path) -> None:
    review = {
        "db_path": str(runtime_db),
        "spreadsheet_id": "sheet-1",
        "changes": [
            {
                "change_id": "chg-90001",
                "sheet": "Календарь",
                "entity_type": "timeblock",
                "entity_id": "external_event:evt-1",
                "field": "Начало",
                "db_value": "14.05.2026 12:00",
                "sheet_value": "14.05.2026 13:00",
                "status": "proposed",
                "question": "",
            }
        ],
    }
    result = apply_review_changes(str(_write_review_file(runtime_db.parent, review)), change_ids=["chg-90001"], worker_url="http://worker/runtime/command")
    assert result["results"][0]["apply_status"] == "skipped"
    assert "только изменения задач" in result["results"][0]["error"].lower()


def test_reverse_sync_review_text_contains_russian_labels(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"].append(["", "", "", "", "", "Sheet created task", "", "", "", "", "", "", ""])
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    text = format_review_text(result)
    assert "Лист:" in text
    assert "Тип:" in text
    assert "ID:" in text
    assert "Поле:" in text
    assert "В базе:" in text
    assert "В таблице:" in text
    assert "Статус:" in text
    assert "Вопрос:" in text
    assert "Новая задача" in text


def test_reverse_sync_json_contains_required_fields(runtime_db: Path) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"][1][5] = "Buy milk and bread"
    client = FakeSheetsClient(tabs)
    result = build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    change = result["changes"][0]
    assert set(change.keys()) == {
        "change_id",
        "sheet",
        "entity_type",
        "entity_id",
        "field",
        "db_value",
        "sheet_value",
        "status",
        "question",
        "source_sheet_row",
        "source_row_hash",
        "source_row_values",
    }


def test_reverse_sync_exit_code_zero_for_no_changes_with_patched_client(
    runtime_db: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = FakeSheetsClient(_build_tabs(runtime_db))
    monkeypatch.setattr(reverse_sync, "build_reverse_sync_review", lambda **kwargs: build_reverse_sync_review(client=client, **kwargs))
    exit_code = reverse_sync.main(["--review", "--db-path", str(runtime_db), "--spreadsheet-id", "sheet-1"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Изменений не найдено." in captured.out
    assert "Предложено: 0" in captured.out
    assert "\"summary\"" in captured.out


def test_reverse_sync_exit_code_one_for_proposed_changes(
    runtime_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"][1][5] = "Buy milk and bread"
    client = FakeSheetsClient(tabs)
    monkeypatch.setattr(reverse_sync, "build_reverse_sync_review", lambda **kwargs: build_reverse_sync_review(client=client, **kwargs))
    assert reverse_sync.main(["--review", "--db-path", str(runtime_db), "--spreadsheet-id", "sheet-1"]) == 1


def test_reverse_sync_exit_code_two_for_invalid_or_conflict(
    runtime_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"][1][8] = "tomorrow"
    client = FakeSheetsClient(tabs)
    monkeypatch.setattr(reverse_sync, "build_reverse_sync_review", lambda **kwargs: build_reverse_sync_review(client=client, **kwargs))
    assert reverse_sync.main(["--review", "--db-path", str(runtime_db), "--spreadsheet-id", "sheet-1"]) == 2


def test_reverse_sync_exit_code_three_for_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(reverse_sync, "build_reverse_sync_review", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    assert reverse_sync.main(["--review", "--spreadsheet-id", "sheet-1"]) == 3


def test_reverse_sync_output_writes_review_json(
    runtime_db: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    tabs = _build_tabs(runtime_db)
    tabs["Задачи"][1][5] = "Buy milk and bread"
    client = FakeSheetsClient(tabs)
    out_path = tmp_path / "review.json"
    monkeypatch.setattr(reverse_sync, "build_reverse_sync_review", lambda **kwargs: build_reverse_sync_review(client=client, **kwargs))
    exit_code = reverse_sync.main(["--review", "--db-path", str(runtime_db), "--spreadsheet-id", "sheet-1", "--output", str(out_path)])
    assert exit_code == 1
    payload = out_path.read_text(encoding="utf-8")
    assert "\"changes\"" in payload
    assert "\"change_id\"" in payload


def test_reverse_sync_does_not_mutate_db(runtime_db: Path) -> None:
    traced: list[str] = []
    conn = sqlite3.connect(str(runtime_db))
    conn.row_factory = sqlite3.Row
    conn.set_trace_callback(traced.append)
    client = FakeSheetsClient(_build_tabs(runtime_db))
    original_connect = reverse_sync._connect
    reverse_sync._connect = lambda db_path=None: conn
    try:
        build_reverse_sync_review(db_path=str(runtime_db), spreadsheet_id="sheet-1", client=client)
    finally:
        reverse_sync._connect = original_connect
        conn.close()
    mutating = [sql for sql in traced if sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "REPLACE", "ALTER", "CREATE", "DROP"))]
    assert mutating == []


def test_reverse_sync_review_does_not_call_worker(
    runtime_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeSheetsClient(_build_tabs(runtime_db))

    def _fail(*args, **kwargs):
        raise AssertionError("worker/runtime command path must not be called during review")

    monkeypatch.setattr(reverse_sync, "build_reverse_sync_review", lambda **kwargs: build_reverse_sync_review(client=client, **kwargs))
    monkeypatch.setattr(reverse_sync, "_connect", reverse_sync._connect)
    monkeypatch.setattr("requests.post", _fail, raising=False)
    assert reverse_sync.main(["--review", "--db-path", str(runtime_db), "--spreadsheet-id", "sheet-1"]) == 0


def test_apply_selected_proposed_task_title_change_builds_correct_worker_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_path = _write_review_file(
        tmp_path,
        {
            "db_path": "/tmp/runtime.db",
            "spreadsheet_id": "sheet-1",
            "changes": [
                {
                    "change_id": "chg-00001",
                    "sheet": "Задачи",
                    "entity_type": "task",
                    "entity_id": "101",
                    "field": "Задача",
                    "db_value": "Old title",
                    "sheet_value": "New title",
                    "status": "proposed",
                    "question": "",
                }
            ],
        },
    )
    captured: list[dict] = []
    monkeypatch.setattr(reverse_sync, "_post_worker_command", lambda url, payload: captured.append(payload) or {"ok": True, "user_message": "ok"})
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: {"ok": True, "mode": "apply"})
    result = apply_review_changes(str(review_path), change_ids=["chg-00001"])
    assert result["summary"]["applied"] == 1
    assert captured[0]["command"]["intent"] == "task.update"
    assert captured[0]["command"]["entities"] == {"task_id": 101, "title": "New title"}


def test_apply_selected_proposed_task_comment_change_builds_correct_worker_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_path = _write_review_file(
        tmp_path,
        {
            "db_path": "/tmp/runtime.db",
            "spreadsheet_id": "sheet-1",
            "changes": [
                {
                    "change_id": "chg-00002",
                    "sheet": "Задачи",
                    "entity_type": "task",
                    "entity_id": "101",
                    "field": "Комментарий",
                    "db_value": "old",
                    "sheet_value": "new comment",
                    "status": "proposed",
                    "question": "",
                }
            ],
        },
    )
    captured: list[dict] = []
    monkeypatch.setattr(reverse_sync, "_post_worker_command", lambda url, payload: captured.append(payload) or {"ok": True, "user_message": "ok"})
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: {"ok": True})
    result = apply_review_changes(str(review_path), change_ids=["chg-00002"])
    assert result["summary"]["applied"] == 1
    assert captured[0]["command"]["intent"] == "task.comment.update"
    assert captured[0]["command"]["entities"] == {"task_id": 101, "comment_text": "new comment"}


def test_apply_all_proposed_applies_only_proposed_task_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_path = _write_review_file(
        tmp_path,
        {
            "db_path": "/tmp/runtime.db",
            "spreadsheet_id": "sheet-1",
            "changes": [
                {"change_id": "chg-1", "sheet": "Задачи", "entity_type": "task", "entity_id": "101", "field": "Задача", "db_value": "a", "sheet_value": "b", "status": "proposed", "question": ""},
                {"change_id": "chg-2", "sheet": "Календарь", "entity_type": "timeblock", "entity_id": "201", "field": "Начало", "db_value": "a", "sheet_value": "b", "status": "confirm_required", "question": ""},
                {"change_id": "chg-3", "sheet": "Задачи", "entity_type": "task", "entity_id": "101", "field": "План", "db_value": "a", "sheet_value": "b", "status": "proposed", "question": ""},
            ],
        },
    )
    captured: list[dict] = []
    monkeypatch.setattr(reverse_sync, "_post_worker_command", lambda url, payload: captured.append(payload) or {"ok": True, "user_message": "ok"})
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: {"ok": True})
    result = apply_review_changes(str(review_path), all_proposed=True)
    assert result["summary"]["selected"] == 2
    assert result["summary"]["applied"] == 1
    assert result["summary"]["skipped"] == 1
    assert len(captured) == 1
    assert captured[0]["command"]["intent"] == "task.update"


def test_apply_all_proposed_excludes_proposed_create(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_path = _write_review_file(
        tmp_path,
        {
            "db_path": "/tmp/runtime.db",
            "spreadsheet_id": "sheet-1",
            "changes": [
                {"change_id": "chg-create", "sheet": "Задачи", "entity_type": "task", "entity_id": None, "field": "task", "db_value": "", "sheet_value": {"title": "New task"}, "status": "proposed_create", "question": ""},
                {"change_id": "chg-update", "sheet": "Задачи", "entity_type": "task", "entity_id": "101", "field": "Задача", "db_value": "a", "sheet_value": "b", "status": "proposed", "question": ""},
            ],
        },
    )
    captured: list[dict] = []
    monkeypatch.setattr(reverse_sync, "_post_worker_command", lambda url, payload: captured.append(payload) or {"ok": True, "user_message": "ok"})
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: {"ok": True})
    result = apply_review_changes(str(review_path), all_proposed=True)
    assert result["summary"]["selected"] == 1
    assert len(captured) == 1
    assert captured[0]["command"]["intent"] == "task.update"


def test_apply_conflict_is_rejected(tmp_path: Path) -> None:
    review_path = _write_review_file(
        tmp_path,
        {"changes": [{"change_id": "chg-1", "sheet": "Задачи", "entity_type": "task", "entity_id": "101", "field": "Задача", "db_value": "a", "sheet_value": "b", "status": "conflict", "question": ""}]},
    )
    result = apply_review_changes(str(review_path), change_ids=["chg-1"])
    assert result["results"][0]["apply_status"] == "skipped"


def test_apply_invalid_is_rejected(tmp_path: Path) -> None:
    review_path = _write_review_file(
        tmp_path,
        {"changes": [{"change_id": "chg-1", "sheet": "Задачи", "entity_type": "task", "entity_id": "101", "field": "ID", "db_value": "101", "sheet_value": "102", "status": "invalid", "question": ""}]},
    )
    result = apply_review_changes(str(review_path), change_ids=["chg-1"])
    assert result["results"][0]["apply_status"] == "skipped"


def test_apply_confirm_required_is_rejected(tmp_path: Path) -> None:
    review_path = _write_review_file(
        tmp_path,
        {"changes": [{"change_id": "chg-1", "sheet": "Календарь", "entity_type": "timeblock", "entity_id": "201", "field": "Начало", "db_value": "a", "sheet_value": "b", "status": "confirm_required", "question": ""}]},
    )
    result = apply_review_changes(str(review_path), change_ids=["chg-1"])
    assert result["results"][0]["apply_status"] == "skipped"


def test_apply_calendar_timeblock_change_is_rejected_for_now(tmp_path: Path) -> None:
    review_path = _write_review_file(
        tmp_path,
        {"changes": [{"change_id": "chg-1", "sheet": "Календарь", "entity_type": "timeblock", "entity_id": "201", "field": "Название", "db_value": "a", "sheet_value": "b", "status": "proposed", "question": ""}]},
    )
    result = apply_review_changes(str(review_path), change_ids=["chg-1"])
    assert result["results"][0]["apply_status"] == "skipped"
    assert "только изменения задач" in result["results"][0]["error"]


def test_apply_unsupported_field_is_rejected(tmp_path: Path) -> None:
    review_path = _write_review_file(
        tmp_path,
        {"changes": [{"change_id": "chg-1", "sheet": "Задачи", "entity_type": "task", "entity_id": "101", "field": "План", "db_value": "a", "sheet_value": "b", "status": "proposed", "question": ""}]},
    )
    result = apply_review_changes(str(review_path), change_ids=["chg-1"])
    assert result["results"][0]["apply_status"] == "skipped"
    assert "не поддержано" in result["results"][0]["error"]


def test_apply_proposed_create_passes_comment_into_task_create_and_status_followup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_path = _write_review_file(
        tmp_path,
        {
            "db_path": "/tmp/runtime.db",
            "spreadsheet_id": "sheet-1",
            "changes": [
                {
                    "change_id": "chg-create",
                    "sheet": "Задачи",
                    "entity_type": "task",
                    "entity_id": None,
                    "field": "task",
                    "db_value": "",
                    "sheet_value": {
                        "title": "Created from sheet",
                        "comment": "comment from sheet",
                        "status": "DONE",
                        "planned_at": "2026-05-18T12:00:00Z",
                        "parent_task_explicit": False,
                    },
                    "status": "proposed_create",
                    "question": "",
                }
            ],
        },
    )
    captured: list[dict[str, object]] = []

    def _fake_post(url: str, payload: dict[str, object]) -> dict[str, object]:
        captured.append(payload)
        intent = payload["command"]["intent"]
        if intent == "task.create":
            return {"ok": True, "user_message": "created", "debug": {"task_id": 555}}
        return {"ok": True, "user_message": "ok"}

    monkeypatch.setattr(reverse_sync, "_post_worker_command", _fake_post)
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: {"ok": True})
    result = apply_review_changes(str(review_path), change_ids=["chg-create"])
    assert result["summary"]["created"] == 1
    assert result["summary"]["applied"] == 1
    assert result["results"][0]["created_task_id"] == "555"
    assert result["results"][0]["cleanup_status"] == "skipped"
    assert [payload["command"]["intent"] for payload in captured] == [
        "task.create",
        "task.set_status",
    ]
    assert captured[0]["command"]["entities"] == {
        "title": "Created from sheet",
        "planned_at": "2026-05-18T12:00:00Z",
        "comment_text": "comment from sheet",
        "parent_task_explicit": False,
    }
    assert captured[1]["command"]["entities"] == {"task_id": 555, "status": "DONE"}


def test_apply_proposed_create_with_parent_id_calls_task_create_with_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_path = _write_review_file(
        tmp_path,
        {
            "db_path": "/tmp/runtime.db",
            "spreadsheet_id": "sheet-1",
            "changes": [
                {
                    "change_id": "chg-create-child",
                    "sheet": "Задачи",
                    "entity_type": "task",
                    "entity_id": None,
                    "field": "task",
                    "db_value": "",
                    "sheet_value": {
                        "title": "Created child from sheet",
                        "parent_task_id": 101,
                        "parent_task_explicit": True,
                    },
                    "status": "proposed_create",
                    "question": "",
                }
            ],
        },
    )
    captured: list[dict[str, object]] = []

    def _fake_post(url: str, payload: dict[str, object]) -> dict[str, object]:
        captured.append(payload)
        return {"ok": True, "user_message": "created", "debug": {"task_id": 556}}

    monkeypatch.setattr(reverse_sync, "_post_worker_command", _fake_post)
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: {"ok": True})
    result = apply_review_changes(str(review_path), change_ids=["chg-create-child"])
    assert result["summary"]["created"] == 1
    assert result["results"][0]["created_task_id"] == "556"
    assert captured[0]["command"]["intent"] == "task.create"
    assert captured[0]["command"]["entities"] == {
        "title": "Created child from sheet",
        "parent_task_id": 101,
        "parent_task_explicit": True,
    }


def test_apply_proposed_create_cleanup_removes_original_blank_id_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_payload = {
        "db_path": "/tmp/runtime.db",
        "spreadsheet_id": "sheet-1",
        "changes": [
            {
                "change_id": "chg-cleanup",
                "sheet": "Задачи",
                "entity_type": "task",
                "entity_id": None,
                "field": "task",
                "db_value": "",
                "sheet_value": {"title": "Created from sheet", "comment": "note", "parent_task_id": 101},
                "status": "proposed_create",
                "question": "",
                "source_sheet_row": 2,
                "source_row_hash": "",
                "source_row_values": {
                    "ID": "",
                    "Уровень": "",
                    "Родитель ID": "101",
                    "Родитель": "",
                    "Задача": "Created from sheet",
                    "Статус": "",
                    "Состояние": "",
                    "План": "",
                    "Создано": "",
                    "Обновлено": "",
                    "Комментарий": "note",
                    "Кол-во блоков времени": "",
                },
            }
        ],
    }
    review_payload["changes"][0]["source_row_hash"] = reverse_sync._row_hash(review_payload["changes"][0]["source_row_values"])
    review_path = _write_review_file(tmp_path, review_payload)
    client = FakeSheetsClient(
        {
            "Задачи": [
                ["ID", "Уровень", "Родитель ID", "Родитель", "Задача", "Статус", "Состояние", "План", "Создано", "Обновлено", "Комментарий", "Кол-во блоков времени"],
                ["", "", "101", "", "Created from sheet", "", "", "", "", "", "note", ""],
            ]
        }
    )

    monkeypatch.setattr(reverse_sync, "_post_worker_command", lambda url, payload: {"ok": True, "user_message": "created", "debug": {"task_id": 777}})
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: {"ok": True})
    result = apply_review_changes(str(review_path), change_ids=["chg-cleanup"], client=client)
    assert result["summary"]["created"] == 1
    assert result["results"][0]["cleanup_status"] == "cleaned"
    assert result["results"][0]["cleanup_error"] == ""
    assert client.deleted_rows == [("Задачи", 2)]
    assert len(client.tabs["Задачи"]) == 1


def test_apply_proposed_create_cleanup_skipped_if_row_now_has_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_path = _write_review_file(
        tmp_path,
        {
            "db_path": "/tmp/runtime.db",
            "spreadsheet_id": "sheet-1",
            "changes": [
                {
                    "change_id": "chg-skip-id",
                    "sheet": "Задачи",
                    "entity_type": "task",
                    "entity_id": None,
                    "field": "task",
                    "db_value": "",
                    "sheet_value": {"title": "Created from sheet"},
                    "status": "proposed_create",
                    "question": "",
                    "source_sheet_row": 2,
                    "source_row_hash": "dummy",
                    "source_row_values": {"ID": "", "Задача": "Created from sheet"},
                }
            ],
        },
    )
    client = FakeSheetsClient({"Задачи": [["ID", "Задача"], ["888", "Created from sheet"]]})
    monkeypatch.setattr(reverse_sync, "_post_worker_command", lambda url, payload: {"ok": True, "user_message": "created", "debug": {"task_id": 778}})
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: {"ok": True})
    result = apply_review_changes(str(review_path), change_ids=["chg-skip-id"], client=client)
    assert result["results"][0]["cleanup_status"] == "skipped"
    assert "already has runtime ID" in result["results"][0]["cleanup_error"]


def test_apply_proposed_create_cleanup_skipped_if_row_hash_changed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_path = _write_review_file(
        tmp_path,
        {
            "db_path": "/tmp/runtime.db",
            "spreadsheet_id": "sheet-1",
            "changes": [
                {
                    "change_id": "chg-skip-hash",
                    "sheet": "Задачи",
                    "entity_type": "task",
                    "entity_id": None,
                    "field": "task",
                    "db_value": "",
                    "sheet_value": {"title": "Created from sheet"},
                    "status": "proposed_create",
                    "question": "",
                    "source_sheet_row": 2,
                    "source_row_hash": "expected-old-hash",
                    "source_row_values": {"ID": "", "Задача": "Created from sheet"},
                }
            ],
        },
    )
    client = FakeSheetsClient({"Задачи": [["ID", "Задача"], ["", "Created from sheet v2"]]})
    monkeypatch.setattr(reverse_sync, "_post_worker_command", lambda url, payload: {"ok": True, "user_message": "created", "debug": {"task_id": 779}})
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: {"ok": True})
    result = apply_review_changes(str(review_path), change_ids=["chg-skip-hash"], client=client)
    assert result["results"][0]["cleanup_status"] == "skipped"
    assert "content changed" in result["results"][0]["cleanup_error"]


def test_apply_proposed_create_cleanup_skipped_if_row_number_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_path = _write_review_file(
        tmp_path,
        {
            "db_path": "/tmp/runtime.db",
            "spreadsheet_id": "sheet-1",
            "changes": [
                {
                    "change_id": "chg-missing-row",
                    "sheet": "Задачи",
                    "entity_type": "task",
                    "entity_id": None,
                    "field": "task",
                    "db_value": "",
                    "sheet_value": {"title": "Created from sheet"},
                    "status": "proposed_create",
                    "question": "",
                    "source_row_hash": "hash",
                    "source_row_values": {"ID": "", "Задача": "Created from sheet"},
                }
            ],
        },
    )
    client = FakeSheetsClient({"Задачи": [["ID", "Задача"], ["", "Created from sheet"]]})
    monkeypatch.setattr(reverse_sync, "_post_worker_command", lambda url, payload: {"ok": True, "user_message": "created", "debug": {"task_id": 780}})
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: {"ok": True})
    result = apply_review_changes(str(review_path), change_ids=["chg-missing-row"], client=client)
    assert result["results"][0]["cleanup_status"] == "skipped"
    assert "cleanup metadata" in result["results"][0]["cleanup_error"]


def test_apply_proposed_create_cleanup_failure_does_not_roll_back_create(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_payload = {
        "db_path": "/tmp/runtime.db",
        "spreadsheet_id": "sheet-1",
        "changes": [
            {
                "change_id": "chg-cleanup-fail",
                "sheet": "Задачи",
                "entity_type": "task",
                "entity_id": None,
                "field": "task",
                "db_value": "",
                "sheet_value": {"title": "Created from sheet"},
                "status": "proposed_create",
                "question": "",
                "source_sheet_row": 2,
                "source_row_hash": "",
                "source_row_values": {"ID": "", "Задача": "Created from sheet"},
            }
        ],
    }
    review_payload["changes"][0]["source_row_hash"] = reverse_sync._row_hash(review_payload["changes"][0]["source_row_values"])
    review_path = _write_review_file(tmp_path, review_payload)
    client = FakeSheetsClient({"Задачи": [["ID", "Задача"], ["", "Created from sheet"]]})
    client.fail_delete_for.add(("Задачи", 2))
    monkeypatch.setattr(reverse_sync, "_post_worker_command", lambda url, payload: {"ok": True, "user_message": "created", "debug": {"task_id": 781}})
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: {"ok": True})
    result = apply_review_changes(str(review_path), change_ids=["chg-cleanup-fail"], client=client)
    assert result["summary"]["created"] == 1
    assert result["summary"]["error"] == 0
    assert result["results"][0]["apply_status"] == "created"
    assert result["results"][0]["cleanup_status"] == "failed"
    assert "delete failed" in result["results"][0]["cleanup_error"]


def test_apply_does_not_write_sqlite_directly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_path = _write_review_file(
        tmp_path,
        {
            "db_path": "/tmp/runtime.db",
            "spreadsheet_id": "sheet-1",
            "changes": [{"change_id": "chg-1", "sheet": "Задачи", "entity_type": "task", "entity_id": "101", "field": "Задача", "db_value": "a", "sheet_value": "b", "status": "proposed", "question": ""}],
        },
    )
    monkeypatch.setattr(reverse_sync, "_connect", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("apply must not connect to sqlite directly")))
    monkeypatch.setattr(reverse_sync, "_post_worker_command", lambda url, payload: {"ok": True, "user_message": "ok"})
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: {"ok": True})
    result = apply_review_changes(str(review_path), change_ids=["chg-1"])
    assert result["summary"]["applied"] == 1


def test_apply_create_does_not_write_sqlite_directly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_path = _write_review_file(
        tmp_path,
        {
            "db_path": "/tmp/runtime.db",
            "spreadsheet_id": "sheet-1",
            "changes": [
                {
                    "change_id": "chg-create",
                    "sheet": "Задачи",
                    "entity_type": "task",
                    "entity_id": None,
                    "field": "task",
                    "db_value": "",
                    "sheet_value": {"title": "Created from sheet"},
                    "status": "proposed_create",
                    "question": "",
                }
            ],
        },
    )
    monkeypatch.setattr(reverse_sync, "_connect", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("apply must not connect to sqlite directly")))
    monkeypatch.setattr(reverse_sync, "_post_worker_command", lambda url, payload: {"ok": True, "user_message": "ok", "task_id": 808})
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: {"ok": True})
    result = apply_review_changes(str(review_path), change_ids=["chg-create"])
    assert result["summary"]["created"] == 1


def test_apply_triggers_export_after_successful_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_path = _write_review_file(
        tmp_path,
        {
            "db_path": "/tmp/runtime.db",
            "spreadsheet_id": "sheet-1",
            "changes": [{"change_id": "chg-1", "sheet": "Задачи", "entity_type": "task", "entity_id": "101", "field": "Задача", "db_value": "a", "sheet_value": "b", "status": "proposed", "question": ""}],
        },
    )
    export_calls: list[dict] = []
    monkeypatch.setattr(reverse_sync, "_post_worker_command", lambda url, payload: {"ok": True, "user_message": "ok"})
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: export_calls.append(kwargs) or {"ok": True, "mode": "apply"})
    result = apply_review_changes(str(review_path), change_ids=["chg-1"])
    assert result["summary"]["applied"] == 1
    assert len(export_calls) == 1
    assert export_calls[0]["spreadsheet_id"] == "sheet-1"


def test_apply_create_triggers_export_after_successful_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_path = _write_review_file(
        tmp_path,
        {
            "db_path": "/tmp/runtime.db",
            "spreadsheet_id": "sheet-1",
            "changes": [
                {
                    "change_id": "chg-create",
                    "sheet": "Задачи",
                    "entity_type": "task",
                    "entity_id": None,
                    "field": "task",
                    "db_value": "",
                    "sheet_value": {"title": "Created from sheet"},
                    "status": "proposed_create",
                    "question": "",
                }
            ],
        },
    )
    export_calls: list[dict] = []
    monkeypatch.setattr(reverse_sync, "_post_worker_command", lambda url, payload: {"ok": True, "user_message": "created", "task_id": 777})
    monkeypatch.setattr(reverse_sync, "run_runtime_sheets_sync_once", lambda **kwargs: export_calls.append(kwargs) or {"ok": True, "mode": "apply"})
    result = apply_review_changes(str(review_path), change_ids=["chg-create"])
    assert result["summary"]["created"] == 1
    assert len(export_calls) == 1
    assert export_calls[0]["spreadsheet_id"] == "sheet-1"


def test_apply_worker_failure_returns_error_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_path = _write_review_file(
        tmp_path,
        {"changes": [{"change_id": "chg-1", "sheet": "Задачи", "entity_type": "task", "entity_id": "101", "field": "Задача", "db_value": "a", "sheet_value": "b", "status": "proposed", "question": ""}]},
    )
    monkeypatch.setattr(reverse_sync, "_post_worker_command", lambda url, payload: {"ok": False, "user_message": "worker failed"})
    result = apply_review_changes(str(review_path), change_ids=["chg-1"])
    assert result["results"][0]["apply_status"] == "error"
    assert result["results"][0]["worker_result"]["ok"] is False
