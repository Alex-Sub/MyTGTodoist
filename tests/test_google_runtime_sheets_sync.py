import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "migrations"
sys.path.append(str(ROOT))

from src.google.runtime_sheets_sync import build_runtime_sheet_payloads, cleanup_deprecated_tabs, run_runtime_sheets_sync_once


_SHEETS_EPOCH = datetime(1899, 12, 30, tzinfo=timezone.utc)


def _serial_to_datetime(value: float) -> datetime:
    return _SHEETS_EPOCH + timedelta(days=float(value))


def _render_cell(sheet_name: str, header: list[object], index: int, value: object) -> str:
    if value is None:
        return ""
    column = str(header[index]) if index < len(header) else ""
    if isinstance(value, (int, float)):
        dt = _serial_to_datetime(float(value))
        if sheet_name == "Задачи" and column in {"План", "Создано", "Обновлено"}:
            return dt.strftime("%d.%m.%Y %H:%M")
        if sheet_name == "InBox" and column == "Создано":
            return dt.strftime("%d.%m.%Y %H:%M")
        if sheet_name == "Календарь" and column == "Дата":
            return dt.strftime("%d.%m.%Y")
        if sheet_name == "Календарь" and column in {"Начало", "Конец"}:
            return dt.strftime("%d.%m.%Y %H:%M")
        if sheet_name == "Логи" and column == "Время":
            return dt.strftime("%d.%m.%Y %H:%M")
    return str(value)


class FakeCalendarClient:
    def __init__(self, events: list[dict] | None = None) -> None:
        self.events = list(events or [])

    def list_events(
        self,
        calendar_id: str,
        sync_token: str | None = None,
        time_min: str | None = None,
        time_max: str | None = None,
        page_token: str | None = None,
    ) -> dict:
        _ = calendar_id, sync_token, page_token
        start = None if not time_min else time_min
        end = None if not time_max else time_max
        filtered: list[dict] = []
        for event in self.events:
            start_payload = (event.get("start") or {}) if isinstance(event.get("start"), dict) else {}
            end_payload = (event.get("end") or {}) if isinstance(event.get("end"), dict) else {}
            event_start = str(start_payload.get("dateTime") or start_payload.get("date") or "")
            event_end = str(end_payload.get("dateTime") or end_payload.get("date") or event_start)
            if start and event_end < start:
                continue
            if end and event_start > end:
                continue
            filtered.append(dict(event))
        return {"items": filtered}


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
            INSERT INTO tasks (
                id, title, status, state, planned_at, calendar_event_id,
                source_msg_id, parent_type, parent_id, created_at, updated_at, completed_at, comment
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                102,
                "Past done task",
                "DONE",
                "DONE",
                "2026-05-11T09:00:00Z",
                "evt-102",
                "",
                "project",
                7,
                "2026-05-10T10:00:00Z",
                "2026-05-12T09:00:00Z",
                "2026-05-12T09:05:00Z",
                "",
            ),
        )
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, status, state, planned_at, calendar_event_id,
                source_msg_id, parent_type, parent_id, created_at, updated_at, completed_at, comment
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                103,
                "Unplanned active task",
                "NEW",
                "NEW",
                None,
                "",
                "",
                "",
                None,
                "2026-05-11T08:00:00Z",
                "2026-05-11T12:00:00Z",
                None,
                "",
            ),
        )
        conn.execute(
            "UPDATE tasks SET parent_task_id = ? WHERE id = ?",
            (101, 103),
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
        conn.execute(
            """
            INSERT INTO time_blocks (id, task_id, user_id, start_at, end_at, created_at, comment)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                202,
                101,
                "u-1",
                "2026-05-12T16:00:00Z",
                "2026-05-12T16:30:00Z",
                "2026-05-11T10:06:00Z",
                "",
            ),
        )
        conn.commit()
    return db_path


class FakeSheetsClient:
    def __init__(self) -> None:
        self.tabs: dict[str, list[list[str]]] = {}
        self.write_table_calls = 0
        self.write_range_calls = 0
        self.format_calls: list[dict[str, object]] = []
        self.raw_tables: dict[str, list[list[object]]] = {}
        self.raw_ranges: list[dict[str, object]] = []
        self.fail_format_for: set[str] = set()
        self.deleted_sheet_ids: list[int] = []
        self.sheet_ids: dict[str, int] = {}

    def ensure_tabs(self, spreadsheet_id: str, sheet_names: list[str]) -> list[str]:
        _ = spreadsheet_id
        created = []
        for name in sheet_names:
            if name not in self.tabs:
                self.tabs[name] = []
                if name not in self.sheet_ids:
                    self.sheet_ids[name] = len(self.sheet_ids) + 1
                created.append(name)
        return created

    def read_range(self, spreadsheet_id: str, a1_range: str):
        _ = spreadsheet_id
        sheet_name = a1_range.split("!", 1)[0].strip("'")
        return [list(row) for row in self.tabs.get(sheet_name, [])]

    def write_range(self, spreadsheet_id: str, a1_range: str, rows):
        _ = spreadsheet_id
        self.write_range_calls += 1
        self.raw_ranges.append({"range": a1_range, "rows": [list(row) for row in rows]})
        sheet_name, cell = a1_range.split("!", 1)
        sheet_name = sheet_name.strip("'")
        row_index = int("".join(ch for ch in cell if ch.isdigit())) - 1
        sheet = self.tabs.setdefault(sheet_name, [])
        header = sheet[0] if sheet else []
        while len(sheet) <= row_index:
            sheet.append([])
        for offset, row in enumerate(rows):
            while len(sheet) <= row_index + offset:
                sheet.append([])
            sheet[row_index + offset] = [_render_cell(sheet_name, header, idx, value) for idx, value in enumerate(row)]

    def write_table(self, spreadsheet_id: str, sheet_name: str, header, rows, *, value_input_option: str = "RAW"):
        _ = spreadsheet_id, value_input_option
        self.write_table_calls += 1
        self.raw_tables[sheet_name] = [list(header)] + [list(row) for row in rows]
        header_list = list(map(str, header))
        self.tabs[sheet_name] = [header_list] + [[_render_cell(sheet_name, header_list, idx, value) for idx, value in enumerate(row)] for row in rows]

    def append_rows(self, spreadsheet_id: str, a1_range: str, rows):
        _ = spreadsheet_id, a1_range
        sheet_name = a1_range.split("!", 1)[0].strip("'")
        sheet = self.tabs.setdefault(sheet_name, [])
        if sheet_name not in self.sheet_ids:
            self.sheet_ids[sheet_name] = len(self.sheet_ids) + 1
        header = sheet[0] if sheet else []
        for row in rows:
            sheet.append([_render_cell(sheet_name, header, idx, value) for idx, value in enumerate(row)])

    def apply_basic_tab_formatting(
        self,
        spreadsheet_id: str,
        *,
        sheet_name: str,
        column_count: int,
        freeze_rows: int = 1,
        enable_filter: bool = True,
        auto_resize: bool = True,
        number_formats: list[dict[str, object]] | None = None,
    ) -> None:
        _ = spreadsheet_id
        self.format_calls.append(
            {
                "sheet_name": sheet_name,
                "column_count": int(column_count),
                "freeze_rows": int(freeze_rows),
                "enable_filter": bool(enable_filter),
                "auto_resize": bool(auto_resize),
                "number_formats": list(number_formats or []),
            }
        )
        if sheet_name in self.fail_format_for:
            raise RuntimeError(f"formatting failed for {sheet_name}")

    def list_tabs(self, spreadsheet_id: str) -> list[dict[str, object]]:
        _ = spreadsheet_id
        out: list[dict[str, object]] = []
        for title in self.tabs.keys():
            if title not in self.sheet_ids:
                self.sheet_ids[title] = len(self.sheet_ids) + 1
            out.append({"sheetId": self.sheet_ids[title], "title": title})
        return out

    def delete_sheet(self, spreadsheet_id: str, *, sheet_id: int) -> None:
        _ = spreadsheet_id
        self.deleted_sheet_ids.append(int(sheet_id))
        for title, existing_id in list(self.sheet_ids.items()):
            if int(existing_id) == int(sheet_id):
                self.sheet_ids.pop(title, None)
                self.tabs.pop(title, None)
                break


def test_runtime_sheets_sync_is_idempotent_by_entity_id(runtime_db: Path) -> None:
    client = FakeSheetsClient()

    first = run_runtime_sheets_sync_once(
        db_path=str(runtime_db),
        spreadsheet_id="sheet-1",
        dry_run=False,
        client=client,
        calendar_client=FakeCalendarClient([]),
    )
    assert first["totals"] == {"created": 5, "updated": 0, "skipped": 0, "stale": 0}

    second = run_runtime_sheets_sync_once(
        db_path=str(runtime_db),
        spreadsheet_id="sheet-1",
        dry_run=False,
        client=client,
        calendar_client=FakeCalendarClient([]),
    )
    assert second["totals"] == {"created": 0, "updated": 0, "skipped": 5, "stale": 0}
    assert len(client.tabs["Задачи"]) == 4
    assert len(client.tabs["InBox"]) == 1
    assert len(client.tabs["Календарь"]) == 3


def test_runtime_sheets_sync_updates_existing_rows_without_duplicates(runtime_db: Path) -> None:
    client = FakeSheetsClient()
    run_runtime_sheets_sync_once(
        db_path=str(runtime_db),
        spreadsheet_id="sheet-1",
        dry_run=False,
        client=client,
        calendar_client=FakeCalendarClient([]),
    )

    with sqlite3.connect(str(runtime_db)) as conn:
        conn.execute("UPDATE tasks SET title = ?, updated_at = ? WHERE id = ?", ("Buy milk and bread", "2026-05-11T11:00:00Z", 101))
        conn.execute("UPDATE time_blocks SET comment = ? WHERE id = ?", ("comment-2", 201))
        conn.commit()

    res = run_runtime_sheets_sync_once(
        db_path=str(runtime_db),
        spreadsheet_id="sheet-1",
        dry_run=False,
        client=client,
        calendar_client=FakeCalendarClient([]),
    )
    assert res["totals"] == {"created": 0, "updated": 4, "skipped": 1, "stale": 0}
    assert len(client.tabs["Задачи"]) == 4
    assert len(client.tabs["InBox"]) == 1
    assert len(client.tabs["Календарь"]) == 3
    task_row_101 = next(row for row in client.tabs["Задачи"][1:] if row[1] == "101")
    assert task_row_101[5] == "Buy milk and bread"
    assert client.tabs["Календарь"][1][1] == "Buy milk and bread"
    assert client.tabs["Календарь"][2][1] == "Buy milk and bread"
    assert client.tabs["Календарь"][1][9] == "comment-2"


def test_runtime_sheets_sync_dry_run_reports_counts_without_writing(runtime_db: Path) -> None:
    client = FakeSheetsClient()

    res = run_runtime_sheets_sync_once(
        db_path=str(runtime_db),
        spreadsheet_id="sheet-1",
        dry_run=True,
        client=client,
        calendar_client=FakeCalendarClient([]),
    )
    assert res["totals"] == {"created": 5, "updated": 0, "skipped": 0, "stale": 0}
    assert client.tabs["Задачи"] == []
    assert client.tabs["InBox"] == []
    assert client.tabs["Календарь"] == []
    assert client.tabs["Логи"] == []


def test_runtime_sheets_payload_single_user_filters_to_related_entities(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn, user_id="u-1")
    tasks_header, task_rows = payloads["Задачи"]
    inbox_header, inbox_rows = payloads["InBox"]
    calendar_header, calendar_rows = payloads["Календарь"]
    assert tasks_header[0] == "№"
    assert inbox_header[0] == "№"
    assert calendar_header[0] == "Тип"
    assert [row[1] for row in task_rows] == ["101"]
    assert inbox_rows == []
    assert [row[10] for row in calendar_rows] == ["timeblock:201", "timeblock:202"]


def test_runtime_sheets_headers_are_stable_and_russian(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    tasks_header, _ = payloads["Задачи"]
    inbox_header, _ = payloads["InBox"]
    calendar_header, _ = payloads["Календарь"]
    assert set(payloads.keys()) == {"Задачи", "InBox", "Календарь"}
    assert "RuntimeTasks" not in payloads
    assert "RuntimeTimeBlocks" not in payloads
    assert tasks_header == [
        "№",
        "ID",
        "Уровень",
        "Родитель ID",
        "Родитель",
        "Задача",
        "Статус",
        "Состояние",
        "План",
        "Создано",
        "Обновлено",
        "Комментарий",
        "Кол-во блоков времени",
    ]
    assert inbox_header == [
        "№",
        "ID",
        "Уровень",
        "Родитель ID",
        "Родитель",
        "Задача",
        "Создано",
        "Источник",
        "Комментарий",
    ]
    assert calendar_header == [
        "Тип",
        "Название",
        "Дата",
        "День недели",
        "Начало",
        "Конец",
        "Время",
        "Длительность, мин",
        "Связанная задача",
        "Комментарий",
        "ID",
        "Calendar Event ID",
        "Источник",
    ]


def test_runtime_sheets_calendar_technical_columns_remain_at_the_end(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    calendar_header, calendar_rows = payloads["Календарь"]
    assert calendar_header[-3:] == ["ID", "Calendar Event ID", "Источник"]
    assert calendar_rows[0][-3:] == ["timeblock:201", "", "runtime_timeblock"]


def test_runtime_sheets_missing_optional_fields_do_not_crash_export(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_min.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute("ALTER TABLE tasks ADD COLUMN parent_task_id INTEGER NULL")
        conn.execute(
            """
            INSERT INTO tasks (id, title, status, calendar_event_id, created_at, updated_at, state, planned_at, source_msg_id, parent_type, parent_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "Minimal task",
                "NEW",
                "",
                "2026-05-11T10:00:00Z",
                "2026-05-11T10:00:00Z",
                "NEW",
                None,
                None,
                None,
                None,
            ),
        )
        conn.execute(
            """
            INSERT INTO time_blocks (id, task_id, start_at, end_at, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                2,
                1,
                "2026-05-12T15:00:00Z",
                "2026-05-12T15:30:00Z",
                "2026-05-11T10:05:00Z",
            ),
        )
        conn.commit()
        payloads = build_runtime_sheet_payloads(conn)
    _, task_rows = payloads["Задачи"]
    _, inbox_rows = payloads["InBox"]
    _, calendar_rows = payloads["Календарь"]
    assert task_rows == [[
        "1", "1", "1", "", "", "Minimal task", "NEW", "NEW", "", "11.05.2026 10:00", "11.05.2026 10:00", "", "1"
    ]]
    assert inbox_rows == []
    assert calendar_rows == [[
        "Блок времени", "Minimal task", "12.05.2026", "Вт", "12.05.2026 15:00", "12.05.2026 15:30", "15:00 - 15:30", "30", "Minimal task", "", "timeblock:2", "", "runtime_timeblock"
    ]]


def test_runtime_sheets_sorting_and_datetime_format_are_deterministic(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    _, task_rows = payloads["Задачи"]
    _, inbox_rows = payloads["InBox"]
    _, calendar_rows = payloads["Календарь"]
    assert [row[1] for row in task_rows] == ["102", "101", "103"]
    assert [row[0] for row in task_rows] == ["102", "101", "101.1"]
    assert [row[0] for row in inbox_rows] == []
    assert [row[10] for row in calendar_rows] == ["timeblock:201", "timeblock:202"]
    assert task_rows[1][8] == "12.05.2026 12:00"
    assert task_rows[1][9] == "11.05.2026 10:00"
    assert calendar_rows[0][2] == "12.05.2026"
    assert calendar_rows[0][3] == "Вт"
    assert calendar_rows[0][4] == "12.05.2026 15:00"
    assert calendar_rows[0][5] == "12.05.2026 15:30"
    assert calendar_rows[0][6] == "15:00 - 15:30"


def test_runtime_sheets_task_export_includes_comment_column_value(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, status, state, planned_at, calendar_event_id,
                source_msg_id, parent_type, parent_id, created_at, updated_at, completed_at, comment, parent_task_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                777,
                "Created with comment",
                "NEW",
                "NEW",
                "2026-05-18",
                "",
                "msg-777",
                "",
                None,
                "2026-05-11T10:00:00Z",
                "2026-05-11T10:00:00Z",
                None,
                "comment from create path",
                None,
            ),
        )
        conn.commit()
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    _, task_rows = payloads["Задачи"]
    row = next(row for row in task_rows if row[1] == "777")
    assert row[11] == "comment from create path"


def test_runtime_sheets_header_change_rewrites_tab_in_single_write(runtime_db: Path) -> None:
    client = FakeSheetsClient()
    client.tabs["Задачи"] = [
        ["ID", "Задача", "Статус"],
        ["101", "Old title", "NEW"],
    ]
    client.tabs["InBox"] = [
        ["ID", "Задача", "Создано"],
        ["101", "Old title", "11.05.2026 10:00"],
    ]
    client.tabs["Календарь"] = [
        ["Тип", "Название", "ID"],
        ["Блок времени", "Old title", "timeblock:201"],
    ]

    res = run_runtime_sheets_sync_once(
        db_path=str(runtime_db),
        spreadsheet_id="sheet-1",
        dry_run=False,
        client=client,
    )

    assert res["totals"] == {"created": 0, "updated": 5, "skipped": 0, "stale": 2}
    assert client.write_table_calls >= 4
    assert client.write_range_calls == 0
    assert client.tabs["Задачи"][0] == [
        "№",
        "ID",
        "Уровень",
        "Родитель ID",
        "Родитель",
        "Задача",
        "Статус",
        "Состояние",
        "План",
        "Создано",
        "Обновлено",
        "Комментарий",
        "Кол-во блоков времени",
    ]


def test_unified_calendar_includes_google_event_and_runtime_block_and_dedupes_linked_event(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("UPDATE tasks SET calendar_event_id = ? WHERE id = ?", ("evt-linked-101", 101))
        conn.execute(
            """
            CREATE TABLE items (
                id TEXT PRIMARY KEY,
                title TEXT,
                description TEXT,
                type TEXT,
                status TEXT,
                scheduled_at TEXT,
                duration_min INTEGER,
                event_id TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO items (id, title, description, type, status, scheduled_at, duration_min, event_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "meet-1",
                "Runtime-known meeting",
                "meeting-note",
                "meeting",
                "active",
                "2026-05-13T12:00:00Z",
                60,
                "evt-meeting-1",
            ),
        )
        conn.commit()
        payloads = build_runtime_sheet_payloads(
            conn,
            include_external_calendar=True,
            calendar_client=FakeCalendarClient(
                [
                    {
                        "id": "evt-linked-101",
                        "summary": "Block duplicate event",
                        "description": "must be deduped",
                        "start": {"dateTime": "2026-05-12T15:00:00Z"},
                        "end": {"dateTime": "2026-05-12T15:30:00Z"},
                    },
                    {
                        "id": "evt-meeting-1",
                        "summary": "Runtime-known meeting live",
                        "description": "meeting-live-note",
                        "start": {"dateTime": "2026-05-13T12:00:00Z"},
                        "end": {"dateTime": "2026-05-13T13:00:00Z"},
                    },
                    {
                        "id": "evt-external-1",
                        "summary": "External visible event",
                        "description": "external-note",
                        "start": {"dateTime": "2026-05-14T09:00:00Z"},
                        "end": {"dateTime": "2026-05-14T10:00:00Z"},
                    },
                ]
            ),
        )
    _, calendar_rows = payloads["Календарь"]
    assert [row[10] for row in calendar_rows] == [
        "timeblock:201",
        "timeblock:202",
        "meeting_event:evt-meeting-1",
        "external_event:evt-external-1",
    ]
    assert [row[0] for row in calendar_rows] == [
        "Блок времени",
        "Блок времени",
        "Встреча",
        "Событие календаря",
    ]
    assert calendar_rows[0][11] == "evt-linked-101"
    assert calendar_rows[2][1] == "Runtime-known meeting live"
    assert calendar_rows[2][12] == "runtime_known_meeting"
    assert calendar_rows[3][1] == "External visible event"
    assert calendar_rows[3][12] == "google_calendar"


def test_unified_calendar_bounded_window_excludes_old_and_far_future_events(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(
            conn,
            include_external_calendar=True,
            calendar_client=FakeCalendarClient(
                [
                    {
                        "id": "evt-old",
                        "summary": "Old event",
                        "start": {"dateTime": "2025-01-01T09:00:00Z"},
                        "end": {"dateTime": "2025-01-01T10:00:00Z"},
                    },
                    {
                        "id": "evt-future",
                        "summary": "Far future event",
                        "start": {"dateTime": "2027-12-01T09:00:00Z"},
                        "end": {"dateTime": "2027-12-01T10:00:00Z"},
                    },
                    {
                        "id": "evt-in-window",
                        "summary": "Window event",
                        "start": {"dateTime": "2026-05-20T09:00:00Z"},
                        "end": {"dateTime": "2026-05-20T10:00:00Z"},
                    },
                ]
            ),
        )
    _, calendar_rows = payloads["Календарь"]
    ids = [row[10] for row in calendar_rows]
    assert "external_event:evt-old" not in ids
    assert "external_event:evt-future" not in ids
    assert "external_event:evt-in-window" in ids


def test_runtime_sheets_sync_with_unified_calendar_updates_sheet(runtime_db: Path) -> None:
    client = FakeSheetsClient()
    result = run_runtime_sheets_sync_once(
        db_path=str(runtime_db),
        spreadsheet_id="sheet-1",
        dry_run=False,
        client=client,
        calendar_client=FakeCalendarClient(
            [
                {
                    "id": "evt-external-2",
                    "summary": "External sync event",
                    "start": {"dateTime": "2026-05-14T12:00:00Z"},
                    "end": {"dateTime": "2026-05-14T13:00:00Z"},
                }
            ]
        ),
    )
    assert result["tabs"]["Календарь"]["rows"] == 3
    assert any(row[10] == "external_event:evt-external-2" for row in client.tabs["Календарь"][1:])


def test_runtime_sheets_logs_sheet_keeps_only_last_seven_days(runtime_db: Path) -> None:
    client = FakeSheetsClient()
    client.tabs["Логи"] = [
        ["Время", "Событие", "Лист", "Строк", "Создано", "Обновлено", "Пропущено", "Ошибка"],
        ["01.01.2000 00:00", "runtime_sheets_sync", "Задачи", "1", "1", "0", "0", ""],
    ]

    run_runtime_sheets_sync_once(
        db_path=str(runtime_db),
        spreadsheet_id="sheet-1",
        dry_run=False,
        client=client,
    )

    logs_rows = client.tabs["Логи"]
    assert logs_rows[0] == ["Время", "Событие", "Лист", "Строк", "Создано", "Обновлено", "Пропущено", "Ошибка"]
    assert len(logs_rows) == 4
    assert all(row[2] in {"Задачи", "InBox", "Календарь"} for row in logs_rows[1:])


def test_runtime_sheets_applies_formatting_to_canonical_tabs_only(runtime_db: Path) -> None:
    client = FakeSheetsClient()
    client.tabs["RuntimeTasks"] = [["legacy"]]
    client.tabs["RuntimeTimeBlocks"] = [["legacy"]]
    run_runtime_sheets_sync_once(
        db_path=str(runtime_db),
        spreadsheet_id="sheet-1",
        dry_run=False,
        client=client,
        calendar_client=FakeCalendarClient([]),
    )
    formatted = {str(item["sheet_name"]) for item in client.format_calls}
    assert formatted == {"Задачи", "InBox", "Календарь", "Логи"}
    assert "RuntimeTasks" not in formatted
    assert "RuntimeTimeBlocks" not in formatted


def test_runtime_sheets_writes_real_datetime_values_for_sortable_columns(runtime_db: Path) -> None:
    client = FakeSheetsClient()
    run_runtime_sheets_sync_once(
        db_path=str(runtime_db),
        spreadsheet_id="sheet-1",
        dry_run=False,
        client=client,
        calendar_client=FakeCalendarClient([]),
    )
    tasks_rows = client.raw_tables["Задачи"]
    inbox_rows = client.raw_tables["InBox"]
    calendar_rows = client.raw_tables["Календарь"]
    logs_rows = client.raw_tables["Логи"]

    assert isinstance(tasks_rows[1][8], float)  # План
    assert isinstance(tasks_rows[1][9], float)  # Создано
    assert isinstance(tasks_rows[1][10], float)  # Обновлено
    assert inbox_rows == [["№", "ID", "Уровень", "Родитель ID", "Родитель", "Задача", "Создано", "Источник", "Комментарий"]]
    assert isinstance(calendar_rows[1][2], float)  # Дата
    assert isinstance(calendar_rows[1][4], float)  # Начало
    assert isinstance(calendar_rows[1][5], float)  # Конец
    assert isinstance(logs_rows[1][0], float)  # Время
    assert calendar_rows[1][6] == "15:00 - 15:30"  # display-only helper stays string


def test_runtime_sheets_generates_number_formats_for_canonical_date_columns(runtime_db: Path) -> None:
    client = FakeSheetsClient()
    run_runtime_sheets_sync_once(
        db_path=str(runtime_db),
        spreadsheet_id="sheet-1",
        dry_run=False,
        client=client,
        calendar_client=FakeCalendarClient([]),
    )
    by_sheet = {str(item["sheet_name"]): item for item in client.format_calls}
    tasks_formats = by_sheet["Задачи"]["number_formats"]
    inbox_formats = by_sheet["InBox"]["number_formats"]
    calendar_formats = by_sheet["Календарь"]["number_formats"]
    logs_formats = by_sheet["Логи"]["number_formats"]

    assert tasks_formats == [
        {"startIndex": 8, "endIndex": 9, "type": "DATE_TIME", "pattern": "dd.mm.yyyy hh:mm"},
        {"startIndex": 9, "endIndex": 10, "type": "DATE_TIME", "pattern": "dd.mm.yyyy hh:mm"},
        {"startIndex": 10, "endIndex": 11, "type": "DATE_TIME", "pattern": "dd.mm.yyyy hh:mm"},
    ]
    assert inbox_formats == [
        {"startIndex": 6, "endIndex": 7, "type": "DATE_TIME", "pattern": "dd.mm.yyyy hh:mm"},
    ]
    assert calendar_formats == [
        {"startIndex": 2, "endIndex": 3, "type": "DATE", "pattern": "dd.mm.yyyy"},
        {"startIndex": 4, "endIndex": 5, "type": "DATE_TIME", "pattern": "dd.mm.yyyy hh:mm"},
        {"startIndex": 5, "endIndex": 6, "type": "DATE_TIME", "pattern": "dd.mm.yyyy hh:mm"},
    ]
    assert logs_formats == [
        {"startIndex": 0, "endIndex": 1, "type": "DATE_TIME", "pattern": "dd.mm.yyyy hh:mm"},
    ]


def test_runtime_sheets_formatting_failure_is_non_fatal(runtime_db: Path) -> None:
    client = FakeSheetsClient()
    client.fail_format_for.add("Календарь")
    result = run_runtime_sheets_sync_once(
        db_path=str(runtime_db),
        spreadsheet_id="sheet-1",
        dry_run=False,
        client=client,
        calendar_client=FakeCalendarClient([]),
    )
    assert result["ok"] is True
    assert result["tabs"]["Календарь"]["rows"] == 2
    assert client.tabs["Календарь"][0][0] == "Тип"


def test_runtime_sheets_planned_task_is_not_in_inbox(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    _, task_rows = payloads["Задачи"]
    _, inbox_rows = payloads["InBox"]
    assert "101" in [row[1] for row in task_rows]
    assert "101" not in [row[1] for row in inbox_rows]


def test_runtime_sheets_undated_root_triage_task_is_only_in_inbox(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, status, state, planned_at, calendar_event_id,
                source_msg_id, parent_type, parent_id, created_at, updated_at, completed_at, comment, parent_task_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                148,
                "Inbox-only triage task",
                "NEW",
                "NEW",
                None,
                "",
                "msg-148",
                "",
                None,
                "2026-05-11T14:00:00Z",
                "2026-05-11T14:00:00Z",
                None,
                "",
                None,
            ),
        )
        conn.commit()
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    _, task_rows = payloads["Задачи"]
    _, inbox_rows = payloads["InBox"]
    assert "148" not in [row[1] for row in task_rows]
    assert "148" in [row[1] for row in inbox_rows]


def test_runtime_sheets_unplanned_open_task_is_in_tasks_and_inbox(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    _, task_rows = payloads["Задачи"]
    _, inbox_rows = payloads["InBox"]
    assert "103" in [row[1] for row in task_rows]
    assert "103" not in [row[1] for row in inbox_rows]


def test_runtime_sheets_task_ids_are_mutually_exclusive_between_tasks_and_inbox(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, status, state, planned_at, calendar_event_id,
                source_msg_id, parent_type, parent_id, created_at, updated_at, completed_at, comment, parent_task_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                48,
                "Task 48 triage case",
                "NEW",
                "NEW",
                None,
                "",
                "msg-48",
                "",
                None,
                "2026-05-11T15:00:00Z",
                "2026-05-11T15:00:00Z",
                None,
                "",
                None,
            ),
        )
        conn.commit()
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    _, task_rows = payloads["Задачи"]
    _, inbox_rows = payloads["InBox"]
    task_ids = {str(row[1]) for row in task_rows}
    inbox_ids = {str(row[1]) for row in inbox_rows}
    assert task_ids.isdisjoint(inbox_ids)
    assert "48" not in task_ids
    assert "48" in inbox_ids


def test_runtime_sheets_completed_task_is_not_in_inbox(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    _, inbox_rows = payloads["InBox"]
    assert "102" not in [row[1] for row in inbox_rows]


def test_runtime_sheets_prunes_stale_rows_by_rewriting_tab(runtime_db: Path) -> None:
    client = FakeSheetsClient()
    client.tabs["InBox"] = [
        ["№", "ID", "Уровень", "Родитель ID", "Родитель", "Задача", "Создано", "Источник", "Комментарий"],
        ["101", "101", "1", "", "", "Buy milk", "11.05.2026 10:00", "telegram", "from runtime"],
        ["101.1", "103", "2", "101", "Buy milk", "Unplanned active task", "11.05.2026 08:00", "runtime", ""],
    ]
    client.tabs["Задачи"] = []
    client.tabs["Календарь"] = []

    res = run_runtime_sheets_sync_once(
        db_path=str(runtime_db),
        spreadsheet_id="sheet-1",
        dry_run=False,
        client=client,
    )

    assert res["tabs"]["InBox"]["stale"] == 2
    assert client.tabs["InBox"][1:] == []


def test_runtime_sheets_rewrites_tab_when_existing_ids_are_duplicated(runtime_db: Path) -> None:
    client = FakeSheetsClient()
    client.tabs["InBox"] = [
        ["№", "ID", "Уровень", "Родитель ID", "Родитель", "Задача", "Создано", "Источник", "Комментарий"],
        ["101.1", "103", "2", "101", "Buy milk", "Unplanned active task", "11.05.2026 08:00", "runtime", ""],
        ["101.1", "103", "2", "101", "Buy milk", "Unplanned active task", "11.05.2026 08:00", "runtime", ""],
    ]
    client.tabs["Задачи"] = []
    client.tabs["Календарь"] = []

    run_runtime_sheets_sync_once(
        db_path=str(runtime_db),
        spreadsheet_id="sheet-1",
        dry_run=False,
        client=client,
    )
    assert client.tabs["InBox"][1:] == []


def test_runtime_sheets_exports_hierarchy_fields_for_root_and_subtask(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    _, task_rows = payloads["Задачи"]
    by_id = {row[1]: row for row in task_rows}
    assert by_id["101"][0:5] == ["101", "101", "1", "", ""]
    assert by_id["103"][0:5] == ["101.1", "103", "2", "101", "Buy milk"]


def test_runtime_sheets_exports_multi_level_hierarchy(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, status, state, planned_at, calendar_event_id,
                source_msg_id, parent_type, parent_id, created_at, updated_at, completed_at, comment, parent_task_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                104,
                "Grandchild task",
                "NEW",
                "NEW",
                None,
                "",
                "",
                "",
                None,
                "2026-05-11T13:00:00Z",
                "2026-05-11T13:00:00Z",
                None,
                "",
                103,
            ),
        )
        conn.commit()
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    _, task_rows = payloads["Задачи"]
    by_id = {row[1]: row for row in task_rows}
    assert by_id["104"][0:5] == ["101.1.1", "104", "3", "103", "Unplanned active task"]


def test_runtime_sheets_parent_appears_before_subtask(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    _, task_rows = payloads["Задачи"]
    order = [row[1] for row in task_rows]
    assert order.index("101") < order.index("103")


def test_runtime_sheets_parent_chain_order_is_recursive(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, status, state, planned_at, calendar_event_id,
                source_msg_id, parent_type, parent_id, created_at, updated_at, completed_at, comment, parent_task_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                104,
                "Grandchild task",
                "NEW",
                "NEW",
                None,
                "",
                "",
                "",
                None,
                "2026-05-11T13:00:00Z",
                "2026-05-11T13:00:00Z",
                None,
                "",
                103,
            ),
        )
        conn.commit()
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    _, task_rows = payloads["Задачи"]
    order = [row[1] for row in task_rows]
    assert order.index("101") < order.index("103") < order.index("104")


def test_runtime_sheets_generated_tree_numbers_are_human_readable(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, status, state, planned_at, calendar_event_id,
                source_msg_id, parent_type, parent_id, created_at, updated_at, completed_at, comment, parent_task_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                104,
                "Grandchild task",
                "NEW",
                "NEW",
                None,
                "",
                "",
                "",
                None,
                "2026-05-11T13:00:00Z",
                "2026-05-11T13:00:00Z",
                None,
                "",
                103,
            ),
        )
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, status, state, planned_at, calendar_event_id,
                source_msg_id, parent_type, parent_id, created_at, updated_at, completed_at, comment, parent_task_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                105,
                "Second child task",
                "NEW",
                "NEW",
                None,
                "",
                "",
                "",
                None,
                "2026-05-11T14:00:00Z",
                "2026-05-11T14:00:00Z",
                None,
                "",
                101,
            ),
        )
        conn.commit()
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    _, task_rows = payloads["Задачи"]
    by_id = {row[1]: row for row in task_rows}
    assert by_id["101"][0] == "101"
    assert by_id["103"][0] == "101.1"
    assert by_id["104"][0] == "101.1.1"
    assert by_id["105"][0] == "101.2"
    assert "101.103.104" not in [row[0] for row in task_rows]


def test_runtime_sheets_inbox_header_includes_hierarchy_fields(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        conn.row_factory = sqlite3.Row
        payloads = build_runtime_sheet_payloads(conn)
    inbox_header, _ = payloads["InBox"]
    assert inbox_header[:5] == ["№", "ID", "Уровень", "Родитель ID", "Родитель"]


def test_cleanup_deprecated_tabs_deletes_non_canonical_and_keeps_canonical() -> None:
    client = FakeSheetsClient()
    for name in [
        "Задачи",
        "InBox",
        "Календарь",
        "Логи",
        "RuntimeTasks",
        "RuntimeTimeBlocks",
        "Inbox",
        "Inbox_legacy_deprecated",
    ]:
        client.tabs[name] = [["header"]]
        client.sheet_ids[name] = len(client.sheet_ids) + 1
    result = cleanup_deprecated_tabs(
        spreadsheet_id="sheet-1",
        client=client,
        delete_deprecated=True,
    )
    assert result["deleted_sheets"] == [
        "RuntimeTasks",
        "RuntimeTimeBlocks",
        "Inbox",
        "Inbox_legacy_deprecated",
    ]
    assert set(client.tabs.keys()) == {"Задачи", "InBox", "Календарь", "Логи"}


def test_cleanup_deprecated_tabs_refuses_when_canonical_sheet_is_missing() -> None:
    client = FakeSheetsClient()
    for name in ["Задачи", "InBox", "Календарь", "RuntimeTasks"]:
        client.tabs[name] = [["header"]]
        client.sheet_ids[name] = len(client.sheet_ids) + 1
    with pytest.raises(RuntimeError, match="canonical sheets missing"):
        cleanup_deprecated_tabs(
            spreadsheet_id="sheet-1",
            client=client,
            delete_deprecated=True,
        )


def test_cleanup_deprecated_tabs_preview_reports_found_kept_deleted() -> None:
    client = FakeSheetsClient()
    for name in ["Задачи", "InBox", "Календарь", "Логи", "RuntimeTasks"]:
        client.tabs[name] = [["header"]]
        client.sheet_ids[name] = len(client.sheet_ids) + 1
    result = cleanup_deprecated_tabs(
        spreadsheet_id="sheet-1",
        client=client,
        delete_deprecated=False,
    )
    assert result["found_sheets"] == ["Задачи", "InBox", "Календарь", "Логи", "RuntimeTasks"]
    assert result["kept_sheets"] == ["Задачи", "InBox", "Календарь", "Логи"]
    assert result["deleted_sheets"] == ["RuntimeTasks"]
