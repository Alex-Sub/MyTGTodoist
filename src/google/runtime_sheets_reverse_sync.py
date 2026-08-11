from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from common.date_resolver import resolve_date_phrase

import requests

from src.google.runtime_sheets_sync import (
    _connect,
    _db_path,
    _spreadsheet_id,
    _table_has_column,
    build_runtime_sheet_payloads,
    run_runtime_sheets_sync_once,
)

try:
    from loguru import logger
except Exception:  # pragma: no cover
    import logging

    logger = logging.getLogger(__name__)

_TASKS_TAB = "Задачи"
_INBOX_TAB = "InBox"
_CALENDAR_TAB = "Календарь"
_TASK_SHEETS = {_TASKS_TAB, _INBOX_TAB}
_TASK_APPLYABLE_FIELDS = {"Задача", "Комментарий", "Статус"}
_TASK_CREATE_ALLOWED_FIELDS = {"№", "Задача", "Комментарий", "Статус", "План", "Приоритет", "Родитель ID"}
_EDITABLE_FIELDS = {
    _TASKS_TAB: {"Задача", "Статус", "Приоритет", "План", "Комментарий", "Родитель ID"},
    _INBOX_TAB: {"Задача", "Статус", "Приоритет", "План", "Комментарий", "Родитель ID"},
    _CALENDAR_TAB: {"Начало", "Конец", "Длительность, мин", "Название", "Комментарий", "Связанная задача"},
}
_NON_EDITABLE_FIELDS = {
    _TASKS_TAB: {"ID", "Уровень", "Родитель", "Состояние", "Создано", "Обновлено", "Кол-во блоков времени", "Версия", "Изменено в базе", "Calendar Event ID"},
    _INBOX_TAB: {"ID", "Уровень", "Родитель", "Создано", "Источник", "Версия", "Изменено в базе", "Calendar Event ID"},
    _CALENDAR_TAB: {"ID", "Тип", "Дата", "День недели", "Время", "Calendar Event ID", "Источник", "Версия", "Изменено в базе"},
}
_DISPLAY_ONLY_IGNORED_FIELDS = {
    _TASKS_TAB: {"№"},
    _INBOX_TAB: {"№"},
    _CALENDAR_TAB: set(),
}
_SYSTEM_FIELDS = {
    "entity_id",
    "entity_type",
    "db_updated_at",
    "exported_at",
    "row_hash",
    "sync_status",
    "sync_error",
    "last_review_id",
}


@dataclass(frozen=True, slots=True)
class ReviewChange:
    change_id: str
    sheet: str
    entity_type: str
    entity_id: str | None
    field: str
    db_value: Any
    sheet_value: Any
    status: str
    question: str
    source_sheet_row: int | None = None
    source_row_hash: str = ""
    source_row_values: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "change_id": self.change_id,
            "sheet": self.sheet,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "field": self.field,
            "db_value": self.db_value,
            "sheet_value": self.sheet_value,
            "status": self.status,
            "question": self.question,
            "source_sheet_row": self.source_sheet_row,
            "source_row_hash": self.source_row_hash,
            "source_row_values": self.source_row_values,
        }


@dataclass(frozen=True, slots=True)
class ApplyResultItem:
    change_id: str
    entity_type: str
    entity_id: str
    field: str
    status_before: str
    apply_status: str
    worker_result: dict[str, Any] | None
    error: str
    created_task_id: str = ""
    cleanup_status: str = ""
    cleanup_error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "change_id": self.change_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "field": self.field,
            "status_before": self.status_before,
            "apply_status": self.apply_status,
            "worker_result": self.worker_result,
            "error": self.error,
            "created_task_id": self.created_task_id,
            "cleanup_status": self.cleanup_status,
            "cleanup_error": self.cleanup_error,
        }


def _normalize(value: Any) -> str:
    return str(value or "").strip()


def _json_compact(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return _normalize(value)


def _row_hash(row: dict[str, Any]) -> str:
    payload = {
        str(key): _normalize(value)
        for key, value in row.items()
        if str(key) != "_sheet_row"
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _worker_command_url() -> str:
    return (os.getenv("WORKER_COMMAND_URL", "") or "http://organizer-worker:8002/runtime/command").strip()


def _apply_trace_id(change_id: str) -> str:
    suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    return f"sheets-apply-{change_id}-{suffix}"


def _read_sheet_table(client: Any, spreadsheet_id: str, sheet_name: str) -> tuple[list[str], list[dict[str, str]]]:
    values = client.read_range(spreadsheet_id, f"'{sheet_name}'!A1:Z")
    if not values:
        return [], []
    header = [_normalize(cell) for cell in values[0]]
    rows: list[dict[str, str]] = []
    for index, raw_row in enumerate(values[1:], start=2):
        row: dict[str, str] = {"_sheet_row": str(index)}
        for col, name in enumerate(header):
            row[name] = _normalize(raw_row[col] if col < len(raw_row) else "")
        rows.append(row)
    return header, rows


def _expected_snapshot(
    conn: sqlite3.Connection,
    *,
    calendar_client: Any | None = None,
) -> dict[str, dict[str, Any]]:
    payloads = build_runtime_sheet_payloads(
        conn,
        calendar_client=calendar_client,
        include_external_calendar=True,
    )
    snapshot: dict[str, dict[str, Any]] = {}
    for sheet_name, (header, rows) in payloads.items():
        rows_by_id: dict[str, dict[str, str]] = {}
        for row in rows:
            item = {header[i]: _normalize(row[i] if i < len(row) else "") for i in range(len(header))}
            entity_id = _normalize(item.get("ID"))
            if entity_id:
                rows_by_id[entity_id] = item
        snapshot[sheet_name] = {"header": list(header), "rows": rows_by_id}
    snapshot["_meta"] = {
        "task_priority_supported": _table_has_column(conn, "tasks", "priority"),
    }
    return snapshot


def _parse_user_datetime(value: str) -> datetime | None:
    raw = _normalize(value)
    if not raw:
        return None
    parts = raw.rsplit(" ", 1)
    if len(parts) != 2:
        return None
    date_part, time_part = parts
    date_resolution = resolve_date_phrase(date_part, now=datetime.now())
    if not date_resolution.ok or not date_resolution.date:
        return None
    try:
        parsed_time = datetime.strptime(time_part, "%H:%M").time()
    except Exception:
        return None
    try:
        return datetime.fromisoformat(f"{date_resolution.date}T{parsed_time.strftime('%H:%M:%S')}")
    except Exception:
        return None


def _user_datetime_to_runtime_iso(value: str) -> str | None:
    dt = _parse_user_datetime(value)
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_duplicate_task_title(value: Any) -> str:
    text = _normalize(value).lower().replace("ё", "е")
    if not text:
        return ""
    for prefix in ("создай задачу", "поставь задачу", "добавь задачу"):
        if text.startswith(prefix):
            text = text[len(prefix) :].strip()
            break
    return " ".join(text.split())


def _runtime_day_iso(value: str | None) -> str:
    raw = _normalize(value)
    if not raw:
        return ""
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return raw[:10] if len(raw) >= 10 else ""


def _find_duplicate_task_create_candidate(
    conn: sqlite3.Connection,
    *,
    title: str,
    planned_at: str | None,
    parent_task_id: int | None,
) -> dict[str, Any] | None:
    normalized_title = _normalize_duplicate_task_title(title)
    planned_day = _runtime_day_iso(planned_at)
    if not normalized_title or not planned_day:
        return None

    if parent_task_id is None:
        rows = conn.execute(
            """
            SELECT id, title, status, state, planned_at, parent_task_id
            FROM tasks
            WHERE parent_task_id IS NULL
            ORDER BY created_at ASC, id ASC
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, title, status, state, planned_at, parent_task_id
            FROM tasks
            WHERE parent_task_id = ?
            ORDER BY created_at ASC, id ASC
            """,
            (int(parent_task_id),),
        ).fetchall()

    for row in rows:
        item = dict(row)
        status_norm = _normalize(item.get("status")).upper()
        state_norm = _normalize(item.get("state")).upper()
        if status_norm in {"DONE", "CANCELLED", "CANCELED", "ARCHIVED"}:
            continue
        if state_norm in {"DONE", "CANCELLED", "CANCELED", "ARCHIVED"}:
            continue
        if _normalize_duplicate_task_title(item.get("title")) != normalized_title:
            continue
        if _runtime_day_iso(item.get("planned_at")) != planned_day:
            continue
        return item
    return None


def _validate_task_edit(field: str, value: str, *, priority_supported: bool) -> tuple[str, str]:
    if field == "Задача" and not _normalize(value):
        return ("invalid", "Поле `Задача` не может быть пустым.")
    if field == "План" and _normalize(value) and _parse_user_datetime(value) is None:
        return ("invalid", "Поле `План` должно быть в формате dd.mm.yyyy HH:MM.")
    if field == "Приоритет" and _normalize(value) and not priority_supported:
        return ("invalid", "Поле `Приоритет` не поддерживается текущей runtime-схемой.")
    return ("proposed", "")


def _is_effectively_blank_row(row: dict[str, str]) -> bool:
    return not any(_normalize(v) for k, v in row.items() if k not in {"_sheet_row", "№"})


def _build_task_create_payload(
    conn: sqlite3.Connection,
    sheet_name: str,
    sheet_row: dict[str, str],
    *,
    priority_supported: bool,
    valid_task_ids: set[str],
) -> tuple[dict[str, Any] | None, str, str]:
    if sheet_name not in _TASK_SHEETS:
        return None, "invalid", "Создание через Google Sheets поддержано только для строк задач."
    if _is_effectively_blank_row(sheet_row):
        return None, "ignored", ""
    title = _normalize(sheet_row.get("Задача"))
    if not title:
        return None, "invalid", "Новая задача должна содержать поле `Задача`."

    planned_raw = _normalize(sheet_row.get("План"))
    if planned_raw and _parse_user_datetime(planned_raw) is None:
        return None, "invalid", "Поле `План` должно быть в формате dd.mm.yyyy HH:MM."

    priority = _normalize(sheet_row.get("Приоритет"))
    if priority and not priority_supported:
        return None, "invalid", "Поле `Приоритет` не поддерживается текущей runtime-схемой."

    unsupported_fields = [
        field
        for field, value in sheet_row.items()
        if field not in {"_sheet_row", "ID"} | _TASK_CREATE_ALLOWED_FIELDS and _normalize(value)
    ]
    if unsupported_fields:
        return None, "invalid", f"Новая задача содержит неподдерживаемые поля: {', '.join(sorted(unsupported_fields))}."

    payload: dict[str, Any] = {"title": title}
    comment = _normalize(sheet_row.get("Комментарий"))
    status = _normalize(sheet_row.get("Статус"))
    planned_at = _user_datetime_to_runtime_iso(planned_raw) if planned_raw else None
    if comment:
        payload["comment"] = comment
    if status:
        payload["status"] = status
    if planned_at:
        payload["planned_at"] = planned_at
    if priority:
        payload["priority"] = priority
    parent_task_id_raw = _normalize(sheet_row.get("Родитель ID"))
    if parent_task_id_raw:
        if not parent_task_id_raw.isdigit():
            return None, "invalid", "Поле `Родитель ID` должно содержать существующий числовой ID задачи."
        if parent_task_id_raw not in valid_task_ids:
            return None, "invalid", "Поле `Родитель ID` ссылается на несуществующую runtime-задачу."
        payload["parent_task_id"] = int(parent_task_id_raw)
        payload["parent_task_explicit"] = True
    else:
        payload["parent_task_explicit"] = False
    duplicate = _find_duplicate_task_create_candidate(
        conn,
        title=title,
        planned_at=planned_at,
        parent_task_id=(int(parent_task_id_raw) if parent_task_id_raw else None),
    )
    if duplicate is not None:
        duplicate_date = ""
        if planned_at:
            try:
                duplicate_date = datetime.fromisoformat(planned_at.replace("Z", "+00:00")).strftime("%d.%m.%Y")
            except Exception:
                duplicate_date = planned_at[:10]
        question = (
            f"Похоже, такая задача уже есть на {duplicate_date or 'эту дату'}: "
            f"№ {duplicate.get('id')} — {_normalize(duplicate.get('title')) or title}. "
            "Создание дубликата требует отдельного подтверждения."
        )
        return payload, "confirm_required", question
    return payload, "proposed_create", "Новая задача из Google Sheets. Создать в runtime DB?"


def _validate_calendar_edit(field: str, value: str, *, row: dict[str, str]) -> tuple[str, str]:
    row_type = _normalize(row.get("Тип"))
    if row_type == "Встреча":
        return ("invalid", "Reverse sync для встреч пока не поддержан: нет canonical meeting read model.")
    if row_type != "Блок времени":
        return ("invalid", f"Неподдерживаемый тип календарной строки: {row_type or '-'}")
    if field in {"Начало", "Конец"} and (_normalize(value) and _parse_user_datetime(value) is None):
        return ("invalid", f"Поле `{field}` должно быть в формате dd.mm.yyyy HH:MM.")
    if field == "Длительность, мин":
        try:
            if int(_normalize(value) or "0") <= 0:
                return ("invalid", "Поле `Длительность, мин` должно быть положительным числом.")
        except Exception:
            return ("invalid", "Поле `Длительность, мин` должно быть положительным числом.")
    if field == "Связанная задача":
        return ("invalid", "Изменение `Связанная задача` пока не поддержано: безопасного task_id mapping нет.")
    return ("confirm_required", "Изменение календарного блока требует отдельного подтверждения.")


def _is_stale(sheet_row: dict[str, str], db_row: dict[str, str]) -> bool:
    sheet_updated = _normalize(sheet_row.get("Обновлено"))
    db_updated = _normalize(db_row.get("Обновлено"))
    return bool(sheet_updated and db_updated and sheet_updated != db_updated)


def _compare_row(
    *,
    sheet_name: str,
    entity_type: str,
    entity_id: str,
    sheet_row: dict[str, str],
    db_row: dict[str, str],
    priority_supported: bool,
    counter: list[int],
) -> list[ReviewChange]:
    changes: list[ReviewChange] = []
    allowed = _EDITABLE_FIELDS[sheet_name]
    non_editable = _NON_EDITABLE_FIELDS[sheet_name] | _SYSTEM_FIELDS
    ignored_display_only = _DISPLAY_ONLY_IGNORED_FIELDS.get(sheet_name, set())
    stale = _is_stale(sheet_row, db_row)

    for field, sheet_value in sheet_row.items():
        if field == "_sheet_row" or field not in db_row:
            continue
        db_value = _normalize(db_row.get(field))
        sheet_value = _normalize(sheet_value)
        if sheet_value == db_value:
            continue
        if field in ignored_display_only:
            continue
        counter[0] += 1
        change_id = f"chg-{counter[0]:05d}"
        if entity_type == "task" and field == "Родитель ID":
            changes.append(
                ReviewChange(
                    change_id=change_id,
                    sheet=sheet_name,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    field=field,
                    db_value=db_value,
                    sheet_value=sheet_value,
                    status="confirm_required",
                    question="Изменение родительской задачи требует отдельного подтверждения и пока не применяется автоматически.",
                )
            )
            continue
        if field in non_editable:
            changes.append(
                ReviewChange(
                    change_id=change_id,
                    sheet=sheet_name,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    field=field,
                    db_value=db_value,
                    sheet_value=sheet_value,
                    status="invalid",
                    question=f"Поле `{field}` не редактируется через Google Sheets.",
                )
            )
            continue
        if field not in allowed:
            changes.append(
                ReviewChange(
                    change_id=change_id,
                    sheet=sheet_name,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    field=field,
                    db_value=db_value,
                    sheet_value=sheet_value,
                    status="invalid",
                    question=f"Поле `{field}` не поддерживается reverse sync MVP.",
                )
            )
            continue
        if stale:
            changes.append(
                ReviewChange(
                    change_id=change_id,
                    sheet=sheet_name,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    field=field,
                    db_value=db_value,
                    sheet_value=sheet_value,
                    status="conflict",
                    question="Задача изменилась и в базе, и в таблице. Какую версию оставить?",
                )
            )
            continue
        if entity_type == "task":
            status, question = _validate_task_edit(field, sheet_value, priority_supported=priority_supported)
        else:
            status, question = _validate_calendar_edit(field, sheet_value, row=sheet_row)
        changes.append(
            ReviewChange(
                change_id=change_id,
                sheet=sheet_name,
                entity_type=entity_type,
                entity_id=entity_id,
                field=field,
                db_value=db_value,
                sheet_value=sheet_value,
                status=status,
                question=question,
            )
        )
    return changes


def build_reverse_sync_review(
    *,
    db_path: str | None = None,
    spreadsheet_id: str | None = None,
    client: Any | None = None,
    calendar_client: Any | None = None,
) -> dict[str, Any]:
    spreadsheet = (spreadsheet_id or _spreadsheet_id()).strip()
    if not spreadsheet:
        raise RuntimeError("GOOGLE_SHEETS_SPREADSHEET_ID is required for runtime Sheets reverse sync")

    if client is None:
        from src.google.sheets_client import SheetsClient

        client = SheetsClient()

    with _connect(db_path or _db_path()) as conn:
        snapshot = _expected_snapshot(conn, calendar_client=calendar_client)

    review: list[ReviewChange] = []
    counter = [0]
    task_priority_supported = bool(snapshot["_meta"]["task_priority_supported"])
    valid_task_ids = set(snapshot.get(_TASKS_TAB, {}).get("rows", {}).keys())
    for sheet_name in (_TASKS_TAB, _INBOX_TAB, _CALENDAR_TAB):
        entity_type = "task" if sheet_name in _TASK_SHEETS else "timeblock"
        _, sheet_rows = _read_sheet_table(client, spreadsheet, sheet_name)
        db_rows = snapshot.get(sheet_name, {}).get("rows", {})
        seen_ids: set[str] = set()

        for sheet_row in sheet_rows:
            entity_id = _normalize(sheet_row.get("ID"))
            if not entity_id:
                if sheet_name in _TASK_SHEETS:
                    payload, status, question = _build_task_create_payload(
                        conn,
                        sheet_name,
                        sheet_row,
                        priority_supported=task_priority_supported,
                        valid_task_ids=valid_task_ids,
                    )
                    if status == "ignored":
                        continue
                    counter[0] += 1
                    review.append(
                        ReviewChange(
                            change_id=f"chg-{counter[0]:05d}",
                            sheet=sheet_name,
                            entity_type="task",
                            entity_id=None,
                            field="task",
                            db_value="",
                            sheet_value=payload or {},
                            status=status,
                            question=question,
                            source_sheet_row=int(_normalize(sheet_row.get("_sheet_row")) or 0) or None,
                            source_row_hash=_row_hash(sheet_row),
                            source_row_values={
                                key: _normalize(value)
                                for key, value in sheet_row.items()
                                if key != "_sheet_row"
                            },
                        )
                    )
                    continue
                if any(_normalize(v) for k, v in sheet_row.items() if k not in {"ID", "_sheet_row"}):
                    counter[0] += 1
                    review.append(
                        ReviewChange(
                            change_id=f"chg-{counter[0]:05d}",
                            sheet=sheet_name,
                            entity_type=entity_type,
                            entity_id="",
                            field="ID",
                            db_value="",
                            sheet_value="",
                            status="invalid",
                            question="Строка без `ID` не может быть сопоставлена с runtime DB.",
                        )
                    )
                continue
            seen_ids.add(entity_id)
            if sheet_name == _CALENDAR_TAB and _normalize(sheet_row.get("Тип")) == "Встреча":
                counter[0] += 1
                review.append(
                    ReviewChange(
                        change_id=f"chg-{counter[0]:05d}",
                        sheet=sheet_name,
                        entity_type="meeting",
                        entity_id=entity_id,
                        field="Тип",
                        db_value="",
                        sheet_value="Встреча",
                        status="invalid",
                        question="Reverse sync для встреч пока не поддержан: нет canonical meeting read model.",
                    )
                )
                continue
            db_row = db_rows.get(entity_id)
            if db_row is None:
                counter[0] += 1
                review.append(
                    ReviewChange(
                        change_id=f"chg-{counter[0]:05d}",
                        sheet=sheet_name,
                        entity_type=entity_type,
                        entity_id=entity_id,
                        field="ID",
                        db_value="",
                        sheet_value=entity_id,
                        status="invalid",
                        question="Строка с неизвестным ID не может быть применена в MVP reverse sync.",
                    )
                )
                continue
            review.extend(
                _compare_row(
                    sheet_name=sheet_name,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    sheet_row=sheet_row,
                    db_row=db_row,
                    priority_supported=task_priority_supported,
                    counter=counter,
                )
            )

        for entity_id in sorted(set(db_rows) - seen_ids):
            counter[0] += 1
            review.append(
                ReviewChange(
                    change_id=f"chg-{counter[0]:05d}",
                    sheet=sheet_name,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    field="ID",
                    db_value=entity_id,
                    sheet_value="",
                    status="confirm_required",
                    question="Строка удалена или очищена в таблице. Удаление не будет применено автоматически.",
                )
            )

    counts = Counter(change.status for change in review)
    proposed_count = int(counts.get("proposed", 0)) + int(counts.get("proposed_create", 0))
    result = {
        "ok": True,
        "mode": "review",
        "db_path": db_path or _db_path(),
        "spreadsheet_id": spreadsheet,
        "summary": {
            "ignored": int(counts.get("ignored", 0)),
            "proposed": proposed_count,
            "proposed_create": int(counts.get("proposed_create", 0)),
            "confirm_required": int(counts.get("confirm_required", 0)),
            "conflict": int(counts.get("conflict", 0)),
            "invalid": int(counts.get("invalid", 0)),
            "total_changes": len(review),
        },
        "changes": [change.to_dict() for change in review],
    }
    logger.info(
        f"runtime_sheets_reverse_sync review proposed={result['summary']['proposed']} "
        f"confirm_required={result['summary']['confirm_required']} "
        f"conflict={result['summary']['conflict']} invalid={result['summary']['invalid']}"
    )
    return result


def _review_exit_code(result: dict[str, Any]) -> int:
    summary = dict(result.get("summary") or {})
    if int(summary.get("conflict") or 0) > 0 or int(summary.get("invalid") or 0) > 0:
        return 2
    if int(summary.get("proposed") or 0) > 0 or int(summary.get("confirm_required") or 0) > 0:
        return 1
    return 0


def build_reverse_sync_review_summary(
    *,
    db_path: str | None = None,
    spreadsheet_id: str | None = None,
    client: Any | None = None,
    limit: int = 5,
) -> dict[str, Any]:
    result = build_reverse_sync_review(
        db_path=db_path,
        spreadsheet_id=spreadsheet_id,
        client=client,
    )
    safe_limit = max(0, int(limit))
    changes = [item for item in list(result.get("changes") or []) if isinstance(item, dict)]
    return {
        "ok": True,
        "mode": "review_summary",
        "db_path": result.get("db_path"),
        "spreadsheet_id": result.get("spreadsheet_id"),
        "summary": dict(result.get("summary") or {}),
        "changes": changes[:safe_limit],
        "exit_code": _review_exit_code(result),
    }


def _grouped_changes(result: dict[str, Any]) -> dict[str, dict[str, dict[str, list[dict[str, Any]]]]]:
    grouped: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}
    for change in list(result.get("changes") or []):
        sheet = _normalize(change.get("sheet"))
        entity_key = f"{_normalize(change.get('entity_type'))}:{_normalize(change.get('entity_id'))}"
        status = _normalize(change.get("status"))
        grouped.setdefault(sheet, {}).setdefault(entity_key, {}).setdefault(status, []).append(change)
    return grouped


def format_review_text(result: dict[str, Any]) -> str:
    summary = dict(result.get("summary") or {})
    lines = [
        "REVERSE SYNC REVIEW",
        f"Изменений всего: {int(summary.get('total_changes') or 0)}",
        f"Предложено: {int(summary.get('proposed') or 0)}",
        f"Новые задачи: {int(summary.get('proposed_create') or 0)}",
        f"Нужно подтверждение: {int(summary.get('confirm_required') or 0)}",
        f"Конфликт: {int(summary.get('conflict') or 0)}",
        f"Некорректно: {int(summary.get('invalid') or 0)}",
        f"Без изменений: {int(summary.get('ignored') or 0)}",
    ]
    grouped = _grouped_changes(result)
    for sheet_name in sorted(grouped):
        lines.append("")
        lines.append(f"Лист: {sheet_name}")
        for entity_key in sorted(grouped[sheet_name]):
            entity_type, _, entity_id = entity_key.partition(":")
            lines.append(f"Тип: {entity_type or '-'} | ID: {entity_id or '-'}")
            for status in sorted(grouped[sheet_name][entity_key]):
                lines.append(f"Статус: {'Новая задача' if status == 'proposed_create' else status}")
                for change in grouped[sheet_name][entity_key][status]:
                    lines.extend(
                        [
                            f"Поле: {_normalize(change.get('field')) or '-'}",
                            f"В базе: {_json_compact(change.get('db_value')) or '-'}",
                            f"В таблице: {_json_compact(change.get('sheet_value')) or '-'}",
                            f"Вопрос: {_normalize(change.get('question')) or '-'}",
                        ]
                    )
    if int(summary.get("total_changes") or 0) <= 0:
        lines.append("")
        lines.append("Изменений не найдено.")
    return "\n".join(lines)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual comparison-only runtime Sheets reverse sync")
    parser.add_argument("--dry-run", action="store_true", help="read and compare without any apply path")
    parser.add_argument("--review", action="store_true", help="produce full review output")
    parser.add_argument("--apply", default="", help="apply selected task-only changes from review JSON")
    parser.add_argument("--change-id", action="append", default=[], help="change id to apply from review JSON")
    parser.add_argument("--all-proposed", action="store_true", help="apply all proposed task-only changes from review JSON")
    parser.add_argument("--db-path", default="", help="override runtime SQLite path")
    parser.add_argument("--spreadsheet-id", default="", help="override Google Sheets spreadsheet id")
    parser.add_argument("--output", default="", help="optional JSON output file")
    return parser.parse_args(argv)


def _load_review_file(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise RuntimeError("review file must contain a JSON object")
    changes = data.get("changes")
    if not isinstance(changes, list):
        raise RuntimeError("review file must contain `changes` list")
    return data


def _selected_changes(review: dict[str, Any], *, change_ids: list[str], all_proposed: bool) -> list[dict[str, Any]]:
    changes = [item for item in list(review.get("changes") or []) if isinstance(item, dict)]
    if change_ids:
        wanted = {_normalize(item).lower() for item in change_ids if _normalize(item)}
        return [item for item in changes if _normalize(item.get("change_id")).lower() in wanted]
    if all_proposed:
        return [item for item in changes if _normalize(item.get("status")) == "proposed"]
    raise RuntimeError("apply mode requires --change-id or --all-proposed")


def _build_task_apply_command(change: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None, str]:
    entity_type = _normalize(change.get("entity_type"))
    field = _normalize(change.get("field"))
    entity_id = _normalize(change.get("entity_id"))
    status_before = _normalize(change.get("status"))
    sheet_value = _normalize(change.get("sheet_value"))

    if status_before != "proposed":
        return None, None, f"Изменение со статусом `{status_before or '-'}` не применяется автоматически."
    if entity_type != "task":
        return None, None, "Пока можно применять только изменения задач."
    if field not in _TASK_APPLYABLE_FIELDS:
        return None, None, f"Поле `{field or '-'}` пока не поддержано для apply."
    if not entity_id:
        return None, None, "У изменения нет entity ID."

    entities: dict[str, Any] = {"task_id": int(entity_id)}
    intent = ""
    if field == "Задача":
        intent = "task.update"
        entities["title"] = sheet_value
    elif field == "Комментарий":
        intent = "task.comment.update"
        entities["comment_text"] = sheet_value
    elif field == "Статус":
        intent = "task.set_status"
        entities["status"] = sheet_value
    else:
        return None, None, f"Поле `{field}` не поддержано для apply."

    return intent, entities, ""


def _task_id_from_worker_result(worker_result: dict[str, Any]) -> int | None:
    raw = worker_result.get("task_id")
    if raw is None and isinstance(worker_result.get("debug"), dict):
        raw = worker_result["debug"].get("task_id")
    try:
        return int(raw) if raw is not None else None
    except Exception:
        return None


def _apply_task_create_change(change: dict[str, Any], *, worker_url: str) -> ApplyResultItem:
    change_id = _normalize(change.get("change_id"))
    status_before = _normalize(change.get("status"))
    payload = change.get("sheet_value")
    if status_before != "proposed_create":
        return ApplyResultItem(
            change_id=change_id,
            entity_type="task",
            entity_id="",
            field="task",
            status_before=status_before,
            apply_status="skipped",
            worker_result=None,
            error=f"Изменение со статусом `{status_before or '-'}` не применяется автоматически.",
            cleanup_status="skipped",
        )
    if not isinstance(payload, dict):
        return ApplyResultItem(
            change_id=change_id,
            entity_type="task",
            entity_id="",
            field="task",
            status_before=status_before,
            apply_status="skipped",
            worker_result=None,
            error="У новой задачи некорректный payload review-артефакта.",
            cleanup_status="skipped",
        )

    title = _normalize(payload.get("title"))
    if not title:
        return ApplyResultItem(
            change_id=change_id,
            entity_type="task",
            entity_id="",
            field="task",
            status_before=status_before,
            apply_status="skipped",
            worker_result=None,
            error="Новая задача не может быть создана без поля `Задача`.",
            cleanup_status="skipped",
        )

    create_entities: dict[str, Any] = {"title": title}
    planned_at = _normalize(payload.get("planned_at"))
    if planned_at:
        create_entities["planned_at"] = planned_at
    comment = _normalize(payload.get("comment"))
    if comment:
        create_entities["comment_text"] = comment
    parent_task_id = payload.get("parent_task_id")
    if parent_task_id is not None:
        create_entities["parent_task_id"] = int(parent_task_id)
    if "parent_task_explicit" in payload:
        create_entities["parent_task_explicit"] = bool(payload.get("parent_task_explicit"))

    try:
        create_result = _post_worker_command(
            worker_url,
            {
                "trace_id": _apply_trace_id(change_id or "unknown"),
                "idempotency_key": f"sheets-apply:{change_id}:create",
                "source": {"channel": "google_sheets_reverse_sync"},
                "command": {"intent": "task.create", "entities": create_entities},
            },
        )
    except Exception as exc:
        return ApplyResultItem(
            change_id=change_id,
            entity_type="task",
            entity_id="",
            field="task",
            status_before=status_before,
            apply_status="error",
            worker_result=None,
            error=str(exc)[:300],
            cleanup_status="skipped",
        )
    if not bool(create_result.get("ok")):
        return ApplyResultItem(
            change_id=change_id,
            entity_type="task",
            entity_id="",
            field="task",
            status_before=status_before,
            apply_status="error",
            worker_result=create_result,
            error=_normalize(create_result.get("user_message")) or "worker returned ok=false",
            cleanup_status="skipped",
        )

    task_id = _task_id_from_worker_result(create_result)
    if task_id is None:
        return ApplyResultItem(
            change_id=change_id,
            entity_type="task",
            entity_id="",
            field="task",
            status_before=status_before,
            apply_status="error",
            worker_result=create_result,
            error="worker не вернул task_id для созданной задачи.",
            cleanup_status="skipped",
        )

    subresults: list[dict[str, Any]] = [{"intent": "task.create", "result": create_result}]

    status = _normalize(payload.get("status"))
    if status:
        status_result = _post_worker_command(
            worker_url,
            {
                "trace_id": _apply_trace_id(f"{change_id}-status"),
                "idempotency_key": f"sheets-apply:{change_id}:status",
                "source": {"channel": "google_sheets_reverse_sync"},
                "command": {
                    "intent": "task.set_status",
                    "entities": {"task_id": task_id, "status": status},
                },
            },
        )
        if not bool(status_result.get("ok")):
            return ApplyResultItem(
                change_id=change_id,
                entity_type="task",
                entity_id=str(task_id),
                field="task",
                status_before=status_before,
                apply_status="error",
                worker_result={"task_id": task_id, "subresults": subresults + [{"intent": "task.set_status", "result": status_result}]},
                error=_normalize(status_result.get("user_message")) or "worker returned ok=false on task.set_status",
                created_task_id=str(task_id),
                cleanup_status="skipped",
            )
        subresults.append({"intent": "task.set_status", "result": status_result})

    return ApplyResultItem(
        change_id=change_id,
        entity_type="task",
        entity_id=str(task_id),
        field="task",
        status_before=status_before,
        apply_status="created",
        worker_result={"task_id": task_id, "subresults": subresults},
        error="",
        created_task_id=str(task_id),
    )


def _cleanup_consumed_task_create_row(
    change: dict[str, Any],
    *,
    spreadsheet_id: str,
    client: Any,
) -> tuple[str, str]:
    sheet_name = _normalize(change.get("sheet"))
    if sheet_name not in _TASK_SHEETS:
        return ("skipped", "cleanup supported only for task sheets")
    row_number_raw = change.get("source_sheet_row")
    try:
        row_number = int(row_number_raw)
    except Exception:
        return ("skipped", "review artifact does not contain a valid source_sheet_row")
    if row_number <= 1:
        return ("skipped", "refusing to clean header row or invalid row number")

    expected_hash = _normalize(change.get("source_row_hash"))
    if not expected_hash:
        return ("skipped", "review artifact does not contain source_row_hash")

    expected_values = change.get("source_row_values")
    if not isinstance(expected_values, dict):
        return ("skipped", "review artifact does not contain source_row_values")

    values = client.read_range(spreadsheet_id, f"'{sheet_name}'!A1:Z")
    if len(values) < row_number:
        return ("skipped", "source row no longer exists")
    header = [_normalize(cell) for cell in (values[0] if values else [])]
    raw_row = values[row_number - 1] if row_number - 1 < len(values) else []
    current_row = {
        header[col]: _normalize(raw_row[col] if col < len(raw_row) else "")
        for col in range(len(header))
    }

    current_id = _normalize(current_row.get("ID"))
    if current_id:
        return ("skipped", "source row already has runtime ID")
    current_hash = _row_hash(current_row)
    if current_hash != expected_hash:
        return ("skipped", "source row content changed after review")

    for field_name in ("Задача", "Комментарий", "Родитель ID", "Статус", "План", "Приоритет"):
        if _normalize(current_row.get(field_name)) != _normalize(expected_values.get(field_name)):
            return ("skipped", f"source row field changed after review: {field_name}")

    try:
        client.delete_row(spreadsheet_id, sheet_name=sheet_name, row_number=row_number)
    except Exception as exc:
        return ("failed", str(exc)[:300])
    return ("cleaned", "")


def _has_cleanup_metadata(change: dict[str, Any]) -> bool:
    if not _normalize(change.get("source_row_hash")):
        return False
    if not isinstance(change.get("source_row_values"), dict):
        return False
    try:
        return int(change.get("source_sheet_row") or 0) > 1
    except Exception:
        return False


def _post_worker_command(worker_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = requests.post(worker_url, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError("worker response must be a JSON object")
    return data


def _apply_one_change(change: dict[str, Any], *, worker_url: str) -> ApplyResultItem:
    change_id = _normalize(change.get("change_id"))
    entity_type = _normalize(change.get("entity_type"))
    entity_id = _normalize(change.get("entity_id"))
    field = _normalize(change.get("field"))
    status_before = _normalize(change.get("status"))

    if entity_type == "task" and field == "task" and status_before == "proposed_create":
        return _apply_task_create_change(change, worker_url=worker_url)

    intent, entities, precheck_error = _build_task_apply_command(change)
    if intent is None or entities is None:
        return ApplyResultItem(
            change_id=change_id,
            entity_type=entity_type,
            entity_id=entity_id,
            field=field,
            status_before=status_before,
            apply_status="skipped",
            worker_result=None,
            error=precheck_error,
        )

    payload = {
        "trace_id": _apply_trace_id(change_id or "unknown"),
        "idempotency_key": f"sheets-apply:{change_id}",
        "source": {"channel": "google_sheets_reverse_sync"},
        "command": {
            "intent": intent,
            "entities": entities,
        },
    }
    try:
        worker_result = _post_worker_command(worker_url, payload)
    except Exception as exc:
        return ApplyResultItem(
            change_id=change_id,
            entity_type=entity_type,
            entity_id=entity_id,
            field=field,
            status_before=status_before,
            apply_status="error",
            worker_result=None,
            error=str(exc)[:300],
        )

    if not bool(worker_result.get("ok")):
        return ApplyResultItem(
            change_id=change_id,
            entity_type=entity_type,
            entity_id=entity_id,
            field=field,
            status_before=status_before,
            apply_status="error",
            worker_result=worker_result,
            error=_normalize(worker_result.get("user_message")) or "worker returned ok=false",
        )

    return ApplyResultItem(
        change_id=change_id,
        entity_type=entity_type,
        entity_id=entity_id,
        field=field,
        status_before=status_before,
        apply_status="applied",
        worker_result=worker_result,
        error="",
    )


def apply_review_changes(
    review_path: str,
    *,
    change_ids: list[str] | None = None,
    all_proposed: bool = False,
    worker_url: str | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    review = _load_review_file(review_path)
    selected = _selected_changes(review, change_ids=list(change_ids or []), all_proposed=bool(all_proposed))
    if not selected:
        raise RuntimeError("no review changes matched the apply selection")

    results: list[ApplyResultItem] = []
    spreadsheet_id = _normalize(review.get("spreadsheet_id")) or _spreadsheet_id()
    for change in selected:
        result = _apply_one_change(change, worker_url=(worker_url or _worker_command_url()))
        if result.apply_status == "created" and _normalize(change.get("entity_type")) == "task" and _normalize(change.get("field")) == "task":
            if _has_cleanup_metadata(change):
                if client is None:
                    from src.google.sheets_client import SheetsClient

                    client = SheetsClient()
                cleanup_status, cleanup_error = _cleanup_consumed_task_create_row(
                    change,
                    spreadsheet_id=spreadsheet_id,
                    client=client,
                )
            else:
                cleanup_status, cleanup_error = ("skipped", "review artifact does not contain cleanup metadata")
            result = ApplyResultItem(
                change_id=result.change_id,
                entity_type=result.entity_type,
                entity_id=result.entity_id,
                field=result.field,
                status_before=result.status_before,
                apply_status=result.apply_status,
                worker_result=result.worker_result,
                error=result.error,
                created_task_id=result.created_task_id,
                cleanup_status=cleanup_status,
                cleanup_error=cleanup_error,
            )
        results.append(result)
    applied_count = sum(1 for item in results if item.apply_status in {"applied", "created"})
    created_count = sum(1 for item in results if item.apply_status == "created")
    skipped_count = sum(1 for item in results if item.apply_status == "skipped")
    error_count = sum(1 for item in results if item.apply_status == "error")

    export_result: dict[str, Any] | None = None
    if applied_count > 0:
        export_kwargs: dict[str, Any] = {
            "db_path": _normalize(review.get("db_path")) or None,
            "spreadsheet_id": _normalize(review.get("spreadsheet_id")) or None,
            "dry_run": False,
        }
        if client is not None:
            export_kwargs["client"] = client
        export_result = run_runtime_sheets_sync_once(**export_kwargs)

    result = {
        "ok": error_count == 0,
        "mode": "apply",
        "review_path": review_path,
        "summary": {
            "selected": len(selected),
            "applied": applied_count,
            "created": created_count,
            "skipped": skipped_count,
            "error": error_count,
        },
        "results": [item.to_dict() for item in results],
        "export_result": export_result,
    }
    logger.info(
        f"runtime_sheets_reverse_sync apply selected={len(selected)} applied={applied_count} "
        f"skipped={skipped_count} error={error_count}"
    )
    return result


def format_apply_text(result: dict[str, Any]) -> str:
    summary = dict(result.get("summary") or {})
    lines = [
        "REVERSE SYNC APPLY",
        f"Выбрано: {int(summary.get('selected') or 0)}",
        f"Применено: {int(summary.get('applied') or 0)}",
        f"Создано: {int(summary.get('created') or 0)}",
        f"Пропущено: {int(summary.get('skipped') or 0)}",
        f"Ошибки: {int(summary.get('error') or 0)}",
    ]
    for item in list(result.get("results") or []):
        lines.extend(
            [
                "",
                f"ID изменения: {_normalize(item.get('change_id')) or '-'}",
                f"Тип: {_normalize(item.get('entity_type')) or '-'}",
                f"ID сущности: {_normalize(item.get('entity_id')) or '-'}",
                f"Созданная задача ID: {_normalize(item.get('created_task_id')) or '-'}",
                f"Поле: {_normalize(item.get('field')) or '-'}",
                f"Статус до apply: {_normalize(item.get('status_before')) or '-'}",
                f"Результат apply: {_normalize(item.get('apply_status')) or '-'}",
                f"Cleanup строки: {_normalize(item.get('cleanup_status')) or '-'}",
                f"Ошибка cleanup: {_normalize(item.get('cleanup_error')) or '-'}",
                f"Ошибка: {_normalize(item.get('error')) or '-'}",
            ]
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        if args.apply:
            result = apply_review_changes(
                args.apply,
                change_ids=list(args.change_id or []),
                all_proposed=bool(args.all_proposed),
            )
            rendered = json.dumps(result, ensure_ascii=False, indent=2)
            if args.output:
                with open(args.output, "w", encoding="utf-8") as fh:
                    fh.write(rendered)
                    fh.write("\n")
            print(format_apply_text(result))
            print("")
            print("JSON:")
            print(rendered)
            return 0 if int(result.get("summary", {}).get("error") or 0) == 0 else 1

        result = build_reverse_sync_review(
            db_path=args.db_path or None,
            spreadsheet_id=args.spreadsheet_id or None,
        )
        payload = result
        if args.dry_run and not args.review:
            payload = {k: v for k, v in result.items() if k != "changes"}
        rendered = json.dumps(payload, ensure_ascii=False, indent=2)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as fh:
                fh.write(rendered)
                fh.write("\n")
        if args.review:
            print(format_review_text(result))
            print("")
            print("JSON:")
        print(rendered)
        return _review_exit_code(result)
    except Exception as exc:
        logger.error(f"runtime_sheets_reverse_sync failed err={str(exc)[:300]}")
        return 3


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
