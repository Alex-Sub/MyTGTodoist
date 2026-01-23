from __future__ import annotations

import asyncio
import json
import os
import re
from io import BytesIO
from pathlib import Path
from uuid import uuid4
from datetime import date, datetime, time, timedelta, timezone
from typing import cast
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.filters.callback_data import CallbackData
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, FSInputFile
from loguru import logger
from sqlalchemy import func, select

from src.config import settings
from src.core.asr_client import asr_transcribe
from src.core.asr_normalize import normalize_asr_text
from src.core.llm_client import LLMClient
from src.core.llm_normalizer import LLMNormalizer
from src.core.meeting_checks import (
    find_events_for_day,
    find_similar_title,
    has_time_conflict,
)
from src.core.nlu import Intent, ParsedIntent, parse_intent
from src.core.aliases import (
    button_ru,
    action_ru,
    field_ru,
    intent_ru,
    stage_ru,
    task_status_ru,
)
from src.core.work_time import (
    calc_fact_for_day,
    compute_actuals,
    elapsed_minutes,
    get_active_work,
    resolve_item,
    start_work,
    stop_work,
)
from src.db.models import CalendarSyncState, Item, ItemEvent, PendingAction, Project
from src.db.repositories.items_repo import create_item, move_item
from src.db.repositories.pending_repo import (
    create_pending,
    delete_pending,
    get_latest_pending,
    get_pending,
    set_state,
    update_pending,
)
from src.db.session import get_session
from src.exports.excel_export import export_xlsx
from src.google.drive_client import DriveClient
from src.google.sync_in import sync_in_calendar
from src.google.sync_out import sync_out_meeting
from src.google.tasks_client import TasksClient
from src.telegram.parsers import parse_meet_args

bot = Bot(token=settings.telegram_bot_token)
dp = Dispatcher()
llm_client = LLMClient()
llm_normalizer = LLMNormalizer(llm_client)

FALLBACK_TEXT = (
    "Я не понимаю, что нужно сделать.\n"
    "Перефразируй команду или введи её в верном формате.\n\n"
    "Примеры:\n"
    " /meetings [today|week]\n"
    " /inbox\n"
    " /task <текст>\n"
    " /export"
)
SOFT_FALLBACK_TEXT = "Не понял команду, перефразируй."
ASR_EMPTY_TEXT = "Не смог распознать голос. Повтори."
_DUPLICATE_SCORE_THRESHOLD = 0.7

def _short_id(value: str | None) -> str:
    if not value:
        return ""
    return str(value)[:8]


def _llm_enabled() -> bool:
    value = os.getenv("LLM_NORMALIZE_ENABLED", "false")
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _format_dt_local(value: datetime | None) -> str:
    if value is None:
        return ""
    tz = ZoneInfo(settings.timezone)
    if value.tzinfo is None:
        value = value.replace(tzinfo=tz)
    else:
        value = value.astimezone(tz)
    return value.strftime("%Y-%m-%d %H:%M")


def _parse_task_due(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _format_task_due(value: str | None) -> str:
    due = _parse_task_due(value)
    if not due:
        return "без даты"
    tz = ZoneInfo(settings.timezone)
    if due.tzinfo is None:
        due = due.replace(tzinfo=tz)
    else:
        due = due.astimezone(tz)
    return due.strftime("%Y-%m-%d %H:%M")


def _extract_project_from_notes(notes: str | None) -> str | None:
    if not notes:
        return None
    for line in notes.splitlines():
        lowered = line.strip().lower()
        if lowered.startswith("project:") or lowered.startswith("category:"):
            parts = line.split(":", 1)
            if len(parts) == 2 and parts[1].strip():
                return parts[1].strip()
    return None


def _is_inbox_task(task: dict) -> bool:
    if task.get("deleted") or task.get("status") == "completed":
        return False
    due_is_null = not task.get("due")
    status = (task.get("status") or "").lower()
    status_is_inbox = status in {"needsaction", "new", "inbox"}
    project = _extract_project_from_notes(task.get("notes"))
    project_is_null = not project
    return due_is_null or project_is_null or status_is_inbox


def _normalize_task_title(value: str | None) -> str:
    return (value or "").strip() or "(без названия)"


async def _fetch_inbox_tasks(
    page_token: str | None,
    limit: int = 10,
) -> tuple[list[dict], str | None]:
    client = TasksClient()
    collected: list[dict] = []
    next_token = page_token
    while len(collected) < limit:
        response = await asyncio.to_thread(
            client.list_tasks,
            "@default",
            next_token,
            50,
        )
        items = response.get("items", []) or []
        for task in items:
            if _is_inbox_task(task):
                collected.append(task)
                if len(collected) >= limit:
                    break
        next_token = response.get("nextPageToken")
        if not next_token:
            break
    return collected, next_token


def _build_inbox_message(tasks: list[dict]) -> str:
    if not tasks:
        return "Неразобранных задач нет."
    lines = ["Неразобранные задачи:"]
    for idx, task in enumerate(tasks, start=1):
        title = _normalize_task_title(task.get("title"))
        due = _format_task_due(task.get("due"))
        lines.append(f"{idx}) {title} — {due}")
    return "\n".join(lines)


def _set_project_in_notes(notes: str | None, project: str) -> str:
    lines = (notes or "").splitlines()
    updated = []
    replaced = False
    for line in lines:
        lowered = line.strip().lower()
        if lowered.startswith("project:") or lowered.startswith("category:"):
            updated.append(f"project: {project}")
            replaced = True
        else:
            updated.append(line)
    if not replaced:
        updated.append(f"project: {project}")
    return "\n".join([line for line in updated if line.strip()])


def _clear_task_pending(session, task_id: str, chat_id: int, user_id: int) -> None:
    pattern = f"\"task_id\": \"{task_id}\""
    pendings = list(
        session.scalars(
            select(PendingAction).where(
                PendingAction.chat_id == chat_id,
                PendingAction.user_id == user_id,
                PendingAction.meta_json.contains(pattern),
            )
        ).all()
    )
    for pending in pendings:
        session.delete(pending)


async def _send_task_menu(message: types.Message, task_id: str) -> None:
    client = TasksClient()
    try:
        task = await asyncio.to_thread(client.get_task, "@default", task_id)
    except Exception as exc:
        logger.error("Inbox task fetch failed: {}", exc)
        await message.answer("Не удалось получить задачу.")
        return
    title = _normalize_task_title(task.get("title"))
    due = _format_task_due(task.get("due"))
    text = f"{action_ru('PROCESS_TASK')}:\n{title}\n{field_ru('due_date')}: {due}"
    reply_markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=button_ru("BTN_SET_DATE"), callback_data=f"inbox:menu_set_date:{task_id}")],
            [InlineKeyboardButton(text=button_ru("BTN_SET_PROJECT"), callback_data=f"inbox:menu_set_project:{task_id}")],
            [InlineKeyboardButton(text=button_ru("BTN_RENAME"), callback_data=f"inbox:menu_rename:{task_id}")],
            [InlineKeyboardButton(text=button_ru("BTN_DONE"), callback_data=f"inbox:menu_done:{task_id}")],
            [InlineKeyboardButton(text=button_ru("BTN_CANCEL"), callback_data=f"inbox:menu_cancel:{task_id}")],
        ]
    )
    await message.answer(text, reply_markup=reply_markup)


def _parse_rfc3339(value: str | None) -> datetime | None:
    if not value:
        return None
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)


def _resolve_date_value(value: str) -> date | None:
    tz = ZoneInfo(settings.timezone)
    today = datetime.now(tz).date()
    if value == "today":
        return today
    if value == "tomorrow":
        return today + timedelta(days=1)
    if value == "day_after_tomorrow":
        return today + timedelta(days=2)
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def _build_meeting_datetime(date_value: str, time_value: str) -> datetime | None:
    tz = ZoneInfo(settings.timezone)
    day = _resolve_date_value(date_value)
    if day is None:
        return None
    try:
        dt = datetime.strptime(f"{day:%Y-%m-%d} {time_value}", "%Y-%m-%d %H:%M")
    except Exception:
        return None
    return dt.replace(tzinfo=tz)


def _is_today_date(date_value: str | None) -> bool:
    if not date_value:
        return False
    day = _resolve_date_value(date_value)
    if day is None:
        return False
    tz = ZoneInfo(settings.timezone)
    return day == datetime.now(tz).date()


def _format_time_range(start: datetime, end: datetime) -> str:
    return f"{start.strftime('%H:%M')}-{end.strftime('%H:%M')}"


def _conflict_preview(
    candidate_start: datetime,
    candidate_end: datetime,
    event_title: str,
    event_time: str,
) -> str:
    return (
        f"Время занято: {_format_time_range(candidate_start, candidate_end)}. "
        f"Уже есть: {event_title} {event_time}"
    )


def _duplicate_preview(event_title: str, event_time: str) -> str:
    return f"Похоже, у вас уже есть похожая встреча сегодня: {event_title} {event_time}"


def _pending_markup_for_stage(stage: str | None, pending_id: str) -> InlineKeyboardMarkup:
    if stage == "conflict":
        return _conflict_keyboard(pending_id)
    if stage == "duplicate":
        return _duplicate_keyboard(pending_id)
    return _pending_keyboard(pending_id)


def _conflict_keyboard(pending_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=button_ru("BTN_MOVE_NEW"),
                    callback_data=f"pa:conflict_move_new:{pending_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=button_ru("BTN_MOVE_EXISTING"),
                    callback_data=f"pa:conflict_move_existing:{pending_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=button_ru("BTN_ALLOW_OVERLAP"),
                    callback_data=f"pa:conflict_add:{pending_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=button_ru("BTN_CANCEL"),
                    callback_data=f"pa:cancel:{pending_id}",
                ),
            ],
        ]
    )


def _duplicate_keyboard(pending_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=button_ru("BTN_ADD_NEW"),
                    callback_data=f"pa:duplicate_add:{pending_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=button_ru("BTN_EDIT_TITLE"),
                    callback_data=f"pa:duplicate_edit_title:{pending_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=button_ru("BTN_RESCHEDULE"),
                    callback_data=f"pa:duplicate_reschedule:{pending_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=button_ru("BTN_CANCEL"),
                    callback_data=f"pa:cancel:{pending_id}",
                ),
            ],
        ]
    )


class ResolveAction(CallbackData, prefix="resolve"):
    action: str
    item_id: str


class WorkAction(CallbackData, prefix="work"):
    action: str
    item_id: str


class OpenAction(CallbackData, prefix="open"):
    action: str
    item_id: str


def _resolve_conflict_keyboard(item_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=button_ru("BTN_CONFIRM"),
                    callback_data=ResolveAction(action="remote", item_id=item_id).pack(),
                ),
                InlineKeyboardButton(
                    text=button_ru("BTN_CHANGE"),
                    callback_data=ResolveAction(action="local", item_id=item_id).pack(),
                ),
            ]
        ]
    )


def _work_keyboard(item_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=button_ru("BTN_CANCEL"),
                    callback_data=WorkAction(action="stop", item_id=item_id).pack(),
                ),
                InlineKeyboardButton(
                    text=button_ru("BTN_CONFIRM"),
                    callback_data=WorkAction(action="export_today", item_id=item_id).pack(),
                ),
            ]
        ]
    )


def _open_keyboard(item: Item) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(
                text=button_ru("BTN_CONFIRM"),
                callback_data=OpenAction(action="pull", item_id=item.id).pack(),
            ),
            InlineKeyboardButton(
                text=button_ru("BTN_CHANGE"),
                callback_data=OpenAction(action="push", item_id=item.id).pack(),
            ),
        ]
    ]
    if item.sync_state == "conflict":
        buttons.append(
            [
                InlineKeyboardButton(
                    text=button_ru("BTN_CONFIRM"),
                    callback_data=ResolveAction(action="remote", item_id=item.id).pack(),
                ),
                InlineKeyboardButton(
                    text=button_ru("BTN_CHANGE"),
                    callback_data=ResolveAction(action="local", item_id=item.id).pack(),
                ),
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _pending_keyboard(pending_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=button_ru("BTN_CONFIRM"),
                    callback_data=f"pa:confirm:{pending_id}",
                ),
                InlineKeyboardButton(
                    text=button_ru("BTN_CHANGE"),
                    callback_data=f"pa:edit:{pending_id}",
                ),
                InlineKeyboardButton(
                    text=button_ru("BTN_CANCEL"),
                    callback_data=f"pa:cancel:{pending_id}",
                ),
            ]
        ]
    )


def _resolve_date_str(date_value: str) -> str | None:
    tz = ZoneInfo(settings.timezone)
    if date_value in {"today", "tomorrow", "day_after_tomorrow"}:
        base = datetime.now(tz).date()
        if date_value == "tomorrow":
            base = base + timedelta(days=1)
        if date_value == "day_after_tomorrow":
            base = base + timedelta(days=2)
        return base.strftime("%Y-%m-%d")
    return date_value


def _format_preview_datetime(date_value: str | None, time_value: str | None) -> str | None:
    if not date_value or not time_value:
        return None
    date_str = _resolve_date_str(date_value)
    if not date_str:
        return None
    try:
        dt = datetime.strptime(f"{date_str} {time_value}", "%Y-%m-%d %H:%M")
    except ValueError:
        return None
    return dt.strftime("%d.%m %H:%M")


def _format_pending_preview(
    intent: Intent,
    args: dict,
    *,
    source: str,
    confidence: float,
    missing: list[str],
) -> str:
    source_label = "LLM-нормализатор" if source == "llm" else "НЛУ"
    lines = [stage_ru("preview")]
    missing_ru = [field_ru(name) for name in missing]

    if intent in {Intent.CREATE_MEETING, Intent.MOVE_MEETING}:
        title = args.get("title") or args.get("target")
        when = _format_preview_datetime(args.get("date"), args.get("time"))
        duration = args.get("duration")
        if title:
            lines.append(f"{field_ru('title')}: {title}")
        if when:
            lines.append(f"{field_ru('date')}: {when}")
        if duration:
            lines.append(f"{field_ru('duration')}: {duration} мин")
        lines.append(f"{field_ru('source')}: {source_label}")
        lines.append(f"{field_ru('confidence')}: {confidence:.2f}")
        if missing_ru:
            lines.append(f"{field_ru('missing')}: {', '.join(missing_ru)}")
        return "\n".join(lines)

    if intent == Intent.SHOW_INBOX_TASKS:
        lines.append(action_ru("SHOW_INBOX_TASKS"))
        lines.append(f"{field_ru('source')}: {source_label}")
        lines.append(f"{field_ru('confidence')}: {confidence:.2f}")
        return "\n".join(lines)

    if intent == Intent.SET_TASK_DATE:
        title = args.get("task_title") or "(без названия)"
        date_value = args.get("date")
        if title:
            lines.append(f"{field_ru('title')}: {title}")
        if date_value:
            lines.append(f"{field_ru('date')}: {date_value}")
        lines.append(f"{field_ru('source')}: {source_label}")
        lines.append(f"{field_ru('confidence')}: {confidence:.2f}")
        if missing_ru:
            lines.append(f"{field_ru('missing')}: {', '.join(missing_ru)}")
        return "\n".join(lines)

    if intent == Intent.ASSIGN_TASK_PROJECT:
        title = args.get("task_title") or "(без названия)"
        project = args.get("project")
        if title:
            lines.append(f"{field_ru('title')}: {title}")
        if project:
            lines.append(f"{field_ru('project')}: {project}")
        lines.append(f"{field_ru('source')}: {source_label}")
        lines.append(f"{field_ru('confidence')}: {confidence:.2f}")
        if missing_ru:
            lines.append(f"{field_ru('missing')}: {', '.join(missing_ru)}")
        return "\n".join(lines)

    if intent == Intent.RENAME_TASK:
        title = args.get("title")
        old_title = args.get("task_title")
        if old_title:
            lines.append(f"{field_ru('title')}: {old_title}")
        if title:
            lines.append(f"{field_ru('title')}: {title}")
        lines.append(f"{field_ru('source')}: {source_label}")
        lines.append(f"{field_ru('confidence')}: {confidence:.2f}")
        if missing_ru:
            lines.append(f"{field_ru('missing')}: {', '.join(missing_ru)}")
        return "\n".join(lines)

    if intent == Intent.DELETE_TASK:
        title = args.get("task_title") or "(без названия)"
        lines.append(f"{action_ru('DELETE_TASK')}: {title}")
        lines.append(f"{field_ru('source')}: {source_label}")
        lines.append(f"{field_ru('confidence')}: {confidence:.2f}")
        return "\n".join(lines)

    if intent == Intent.COMPLETE_TASK:
        title = args.get("task_title") or "(без названия)"
        lines.append(f"{action_ru('PROCESS_TASK')}: {title}")
        lines.append(f"{field_ru('source')}: {source_label}")
        lines.append(f"{field_ru('confidence')}: {confidence:.2f}")
        return "\n".join(lines)

    if intent == Intent.CREATE_TASK:
        title = args.get("title")
        project = args.get("project")
        if title:
            lines.append(f"{field_ru('title')}: {title}")
        if project:
            lines.append(f"{field_ru('project')}: {project}")
        lines.append(f"{field_ru('source')}: {source_label}")
        lines.append(f"{field_ru('confidence')}: {confidence:.2f}")
        if missing_ru:
            lines.append(f"{field_ru('missing')}: {', '.join(missing_ru)}")
        return "\n".join(lines)

    if intent == Intent.PLAN_TASK:
        minutes = args.get("minutes")
        target = args.get("target")
        date_value = args.get("date")
        if minutes:
            lines.append(f"{field_ru('duration')}: {minutes} мин")
        if target:
            lines.append(f"{field_ru('title')}: {target}")
        if date_value:
            lines.append(f"{field_ru('date')}: {date_value}")
        lines.append(f"{field_ru('source')}: {source_label}")
        lines.append(f"{field_ru('confidence')}: {confidence:.2f}")
        if missing_ru:
            lines.append(f"{field_ru('missing')}: {', '.join(missing_ru)}")
        return "\n".join(lines)

    if intent == Intent.EXPORT:
        mode = settings.google_drive_mode
        lines.append(f"{action_ru('EXPORT')}: Excel → Drive ({mode})")
        lines.append(f"{field_ru('source')}: {source_label}")
        lines.append(f"{field_ru('confidence')}: {confidence:.2f}")
        if missing_ru:
            lines.append(f"{field_ru('missing')}: {', '.join(missing_ru)}")
        return "\n".join(lines)

    return FALLBACK_TEXT


def _validate_required_args(
    intent: Intent,
    args: dict,
    raw_text: str,
    canonical_text: str,
) -> tuple[list[str], dict]:
    missing: list[str] = []
    updated = dict(args)

    if intent == Intent.CREATE_MEETING:
        date_value = updated.get("date")
        time_value = updated.get("time")
        title = updated.get("title")
        if not title:
            fallback_title = canonical_text or raw_text
            updated["title"] = fallback_title.strip() or "Встреча"
        if not updated.get("duration"):
            updated["duration"] = 60
        if not date_value:
            missing.append("date")
        if not time_value:
            missing.append("time")
        return missing, updated

    if intent == Intent.MOVE_MEETING:
        if not updated.get("target"):
            missing.append("target")
        if not updated.get("date"):
            missing.append("date")
        if not updated.get("time"):
            missing.append("time")
        return missing, updated

    if intent == Intent.CREATE_TASK:
        if not updated.get("title"):
            missing.append("title")
        return missing, updated

    if intent == Intent.PLAN_TASK:
        if not updated.get("minutes"):
            missing.append("minutes")
        if not updated.get("target"):
            missing.append("target")
        return missing, updated

    if intent == Intent.SHOW_INBOX_TASKS:
        return missing, updated

    if intent == Intent.SET_TASK_DATE:
        if not updated.get("task_id"):
            missing.append("task_id")
        if not updated.get("date"):
            missing.append("date")
        return missing, updated

    if intent == Intent.ASSIGN_TASK_PROJECT:
        if not updated.get("task_id"):
            missing.append("task_id")
        if not updated.get("project"):
            missing.append("project")
        return missing, updated

    if intent == Intent.RENAME_TASK:
        if not updated.get("task_id"):
            missing.append("task_id")
        if not updated.get("title"):
            missing.append("title")
        return missing, updated

    if intent == Intent.DELETE_TASK:
        if not updated.get("task_id"):
            missing.append("task_id")
        return missing, updated

    if intent == Intent.COMPLETE_TASK:
        if not updated.get("task_id"):
            missing.append("task_id")
        return missing, updated

    return missing, updated


def _check_create_meeting_preconditions(args: dict) -> tuple[str, str, dict] | None:
    date_value = args.get("date")
    time_value = args.get("time")
    title = args.get("title") or ""
    if not date_value or not time_value:
        return None
    if not _is_today_date(date_value):
        return None

    candidate_start = _build_meeting_datetime(date_value, time_value)
    if candidate_start is None:
        return None
    duration = int(args.get("duration") or 60)
    candidate_end = candidate_start + timedelta(minutes=duration)

    events = find_events_for_day(candidate_start.date())
    if not args.get("allow_overlap"):
        has_conflict, conflicts = has_time_conflict(candidate_start, candidate_end, events)
        if has_conflict and conflicts:
            conflict = conflicts[0]
            event_time = _format_time_range(conflict.start, conflict.end)
            logger.info(
                "conflict_detected event_id={} start={} end={}",
                conflict.item_id,
                conflict.start.isoformat(),
                conflict.end.isoformat(),
            )
            meta = {
                "conflicting_event_ids": [c.item_id for c in conflicts],
                "candidate_start": candidate_start.isoformat(),
                "candidate_end": candidate_end.isoformat(),
            }
            preview = _conflict_preview(
                candidate_start,
                candidate_end,
                conflict.title or "(без названия)",
                event_time,
            )
            return "conflict", preview, meta

    if not args.get("allow_duplicate"):
        match, score = find_similar_title(title, events)
        if match and score >= _DUPLICATE_SCORE_THRESHOLD:
            event_time = _format_time_range(match.start, match.end)
            logger.info(
                "duplicate_detected event_id={} score={:.2f}",
                match.item_id,
                score,
            )
            meta = {
                "duplicate_event_id": match.item_id,
                "duplicate_score": score,
            }
            preview = _duplicate_preview(match.title or "(без названия)", event_time)
            return "duplicate", preview, meta

    return None


def _build_pending(
    parsed: ParsedIntent,
    *,
    raw_head: str,
    raw_text: str,
    source: str,
    confidence: float,
    canonical_text: str,
    missing: list[str],
    message: types.Message,
) -> tuple[str, str, str] | None:
    try:
        missing_list, normalized_args = _validate_required_args(
            parsed.intent,
            parsed.args,
            raw_text,
            canonical_text,
        )
        stage = "normal"
        preview = _format_pending_preview(
            parsed.intent,
            normalized_args,
            source=source,
            confidence=confidence,
            missing=missing_list or missing,
        )
        meta: dict | None = None

        if parsed.intent == Intent.CREATE_MEETING and not (missing_list or missing):
            check = _check_create_meeting_preconditions(normalized_args)
            if check:
                stage, preview, meta = check

        with get_session() as session:
            pending = create_pending(
                session,
                chat_id=message.chat.id,
                user_id=message.from_user.id if message.from_user else 0,
                intent=parsed.intent.value,
                action_type=parsed.intent.name,
                args_dict=normalized_args,
                raw_head=raw_head,
                raw_text=raw_text,
                source=source,
                confidence=confidence,
                canonical_text=canonical_text,
                missing=missing_list or missing,
                stage=stage,
                meta=meta,
            )
        logger.info(
            "PENDING action=create id={} intent={} conf={} head={}",
            pending.id,
            parsed.intent,
            confidence,
            raw_head,
        )
        if preview == FALLBACK_TEXT:
            return None
        return pending.id, preview, stage
    except Exception:
        return None


async def cmd_start(message: types.Message) -> None:
    await message.answer(
        "Бот запущен. Команды: /meet, /inbox, /export, /startwork, /stopwork, /work"
    )


async def _create_meeting(
    message: types.Message,
    title: str,
    scheduled_at: datetime,
    duration: int | None,
) -> Item | None:
    with get_session() as session:
        project = session.scalar(select(Project).where(Project.name == "Проекты"))
        if project is None:
            project = session.scalar(select(Project).where(Project.name == "Inbox"))
        if project is None:
            await message.answer("Проект по умолчанию не найден.")
            return None

        item = create_item(
            session,
            title=title,
            project_id=project.id,
            type="meeting",
            status="active",
            scheduled_at=scheduled_at,
            duration_min=duration,
            calendar_id=settings.google_calendar_id_default,
            sync_state="dirty",
        )
        synced = sync_out_meeting(session, item.id)

    logger.info("Meeting created item_id={}", synced.id)
    return synced


def _normalize_meeting_text(value: str) -> str:
    text = value.lower().replace("ё", "е")
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    for stop_word in ("встреча", "встречу", "встречи"):
        text = re.sub(rf"\b{stop_word}\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _find_meeting_by_target(
    session,
    target: str,
    desired_dt: datetime | None = None,
) -> Item | None:
    item = resolve_item(session, target)
    if item is not None and item.type == "meeting":
        return item
    if not target:
        return None

    candidates = list(session.scalars(select(Item).where(Item.type == "meeting")).all())
    norm_target = _normalize_meeting_text(target)
    tokens = [tok for tok in norm_target.split() if len(tok) >= 3]

    matches: list[Item] = []
    for candidate in candidates:
        norm_title = _normalize_meeting_text(candidate.title or "")
        if norm_target and norm_target in norm_title:
            matches.append(candidate)
            continue
        if tokens and all(tok in norm_title for tok in tokens):
            matches.append(candidate)

    top3 = [
        f"{c.title} @ {_format_dt_local(c.scheduled_at)}"
        for c in sorted(candidates, key=lambda it: it.scheduled_at or datetime.max)[:3]
    ]
    logger.debug(
        "meeting target={} norm_target={} candidates={} top3={} matches={}",
        target,
        norm_target,
        len(candidates),
        top3,
        len(matches),
    )

    if not matches:
        return None
    if desired_dt is None:
        return sorted(matches, key=lambda it: it.scheduled_at or datetime.max)[0]

    tz = ZoneInfo(settings.timezone)
    if desired_dt.tzinfo is None:
        desired_dt = desired_dt.replace(tzinfo=tz)
    else:
        desired_dt = desired_dt.astimezone(tz)

    def _distance(item: Item) -> float:
        if item.scheduled_at is None:
            return float("inf")
        scheduled = item.scheduled_at
        if scheduled.tzinfo is None:
            scheduled = scheduled.replace(tzinfo=tz)
        else:
            scheduled = scheduled.astimezone(tz)
        return abs((scheduled - desired_dt).total_seconds())

    return sorted(matches, key=_distance)[0]


async def cmd_meet(message: types.Message) -> None:
    text = message.text or ""
    try:
        title, scheduled_at, duration = parse_meet_args(text)
    except Exception:
        await message.answer("Формат: /meet \"Название\" YYYY-MM-DD HH:MM 60")
        return

    date_str = scheduled_at.strftime("%Y-%m-%d")
    time_str = scheduled_at.strftime("%H:%M")
    parsed = ParsedIntent(
        Intent.CREATE_MEETING,
        1.0,
        {
            "title": title,
            "date": date_str,
            "time": time_str,
            "duration": duration or 60,
        },
        text,
    )
    pending = _build_pending(
        parsed,
        raw_head=text[:50],
        raw_text=text,
        source="command",
        confidence=1.0,
        canonical_text="",
        missing=[],
        message=message,
    )
    if pending is None:
        await message.answer(SOFT_FALLBACK_TEXT)
        return
    pending_id, preview, stage = pending
    await message.answer(preview, reply_markup=_pending_markup_for_stage(stage, pending_id))


async def cmd_sync_calendar(message: types.Message) -> None:
    with get_session() as session:
        stats = sync_in_calendar(session, settings.google_calendar_id_default)
        dirty_count = session.scalar(
            select(func.count()).select_from(Item).where(
                Item.type == "meeting", Item.sync_state == "dirty"
            )
        ) or 0
        conflict_count = session.scalar(
            select(func.count()).select_from(Item).where(
                Item.type == "meeting", Item.sync_state == "conflict"
            )
        ) or 0

    await message.answer(
        "Pull: "
        f"processed={stats['processed']} created={stats['created']} "
        f"updated={stats['updated']} cancelled={stats['cancelled']} "
        f"conflicts={stats['conflicts']} tokenReset={stats['token_reset']} "
        f"dirty={dirty_count} conflict={conflict_count}"
    )


def _parse_export_scope(text: str) -> dict:
    parts = text.split()
    if len(parts) == 1:
        return {"mode": "today"}

    mode = parts[1].lower()
    if mode in {"all", "inbox", "today", "week", "overdue"}:
        return {"mode": mode}

    if mode == "project":
        if len(parts) < 3:
            raise ValueError("Project name required")
        project_name = " ".join(parts[2:]).strip()
        return {"mode": "project", "project_name": project_name}

    raise ValueError("Unknown export scope")


async def _send_export(message: types.Message, scope: dict) -> None:
    with get_session() as session:
        path = export_xlsx(session, scope)

    if settings.google_drive_enabled:
        try:
            client = DriveClient()
            folder_id = client.find_or_create_folder(settings.google_drive_folder_name)
            if settings.google_drive_mode == "latest":
                filename = "todo_latest.xlsx"
                existing_id = client.find_file_in_folder(folder_id, filename)
            else:
                filename = Path(path).name
                existing_id = None
            result = client.upload_file(
                folder_id=folder_id,
                path=path,
                filename=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                existing_file_id=existing_id,
                convert_to_google=True,
            )
            await message.answer(
                "Экспорт загружен в Drive: "
                f"{settings.google_drive_mode} - ссылка: {result.get('webViewLink','')}"
            )
        except Exception as exc:
            logger.error("Drive upload failed: {}", exc)
            await message.answer("Ошибка загрузки в Drive, отправляю файл напрямую.")
    await message.answer_document(FSInputFile(path))


async def cmd_export(message: types.Message) -> None:
    text = message.text or ""
    try:
        scope = _parse_export_scope(text)
    except Exception:
        await message.answer(
            "Формат: /export [today|week|all|inbox|overdue|project <name>]"
        )
        return

    await _send_export(message, scope)


async def cmd_task(message: types.Message) -> None:
    text = message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Формат: /task <название>")
        return

    title = parts[1].strip()
    with get_session() as session:
        project = session.scalar(select(Project).where(Project.name == "Inbox"))
        if project is None:
            await message.answer("Проект Inbox не найден.")
            return
        item = create_item(
            session,
            title=title,
            project_id=project.id,
            type="task",
            status="inbox",
        )
    await message.answer(f"Добавлено в Inbox: {_short_id(item.id)} - {item.title}")


def _parse_id_arg(text: str) -> str | None:
    parts = text.split()
    if len(parts) < 2:
        return None
    return parts[1].strip()


async def _handle_startwork(message: types.Message, token: str) -> None:
    with get_session() as session:
        item = resolve_item(session, token)
        if item is None:
            await message.answer("Задача не найдена")
            return

        if settings.single_active_work:
            active = get_active_work(session)
            if active and active[0].id != item.id:
                active_item, _ = active
                await message.answer(
                    f"Уже идет работа: {_short_id(active_item.id)} - {active_item.title}. "
                    "Сначала остановите."
                )
                return

        ok, status = start_work(session, item)
        if not ok and status == "already_active":
            await message.answer(f"Уже запущено: {_short_id(item.id)} - {item.title}")
            return

        await message.answer(f"Старт: {_short_id(item.id)} - {item.title}")


async def _handle_stopwork(message: types.Message, token: str) -> None:
    with get_session() as session:
        item = resolve_item(session, token)
        if item is None:
            await message.answer("Задача не найдена")
            return

        ok, session_min, total_min = stop_work(session, item)
        if not ok:
            await message.answer(f"Нет активной работы по задаче: {_short_id(item.id)}")
            return

        await message.answer(
            f"Стоп: {_short_id(item.id)} - {item.title}. "
            f"Сессия: {session_min} мин. Факт всего: {total_min} мин."
        )


async def cmd_startwork(message: types.Message) -> None:
    token = _parse_id_arg(message.text or "")
    if not token:
        await message.answer("Формат: /startwork <id>")
        return

    await _handle_startwork(message, token)


async def cmd_stopwork(message: types.Message) -> None:
    token = _parse_id_arg(message.text or "")
    if not token:
        await message.answer("Формат: /stopwork <id>")
        return

    await _handle_stopwork(message, token)


async def cmd_work(message: types.Message) -> None:
    with get_session() as session:
        active = get_active_work(session)
        if not active:
            await message.answer("Активной работы нет")
            return
        item, start_ts = active
        now_local = datetime.now(ZoneInfo(settings.timezone))
        elapsed_min = elapsed_minutes(start_ts, now_local)
        total_map, _ = compute_actuals(session, [item.id], now_local)
        fact_total = total_map.get(item.id, 0)

    planned = item.planned_min
    if planned is None and item.type == "meeting":
        planned = item.duration_min if item.duration_min is not None else 60

    delta = fact_total - planned if planned is not None else None
    started_local = start_ts
    if started_local.tzinfo is None:
        started_local = started_local.replace(tzinfo=ZoneInfo(settings.timezone))
    else:
        started_local = started_local.astimezone(ZoneInfo(settings.timezone))
    started_str = started_local.strftime("%H:%M")

    lines = [
        "Сейчас в работе:",
        f"{_short_id(item.id)} - {item.title}",
        f"Time: {elapsed_min} мин (с {started_str})",
    ]
    if planned is not None:
        lines.append(f"План: {planned} - Факт: {fact_total} - Delta: {delta}")
    await message.answer("\n".join(lines), reply_markup=_work_keyboard(item.id))


async def on_work_action(
    callback: types.CallbackQuery,
    callback_data: WorkAction,
) -> None:
    message = callback.message
    if message is None:
        await callback.answer("Недоступно", show_alert=True)
        return
    if callback_data.action == "export_today":
        with get_session() as session:
            path = export_xlsx(session, {"mode": "today"})
        await message.answer_document(FSInputFile(path))
        await callback.answer()
        return

    if callback_data.action == "stop":
        with get_session() as session:
            item = session.get(Item, callback_data.item_id)
            if item is None:
                await callback.answer("Задача не найдена", show_alert=True)
                return
            ok, session_min, total_min = stop_work(session, item)
            if not ok:
                await message.answer(
                    f"Нет активной работы по задаче: {_short_id(item.id)}"
                )
                await callback.answer()
                return
            await message.answer(
                f"Стоп: {_short_id(item.id)} - {item.title}. "
                f"Сессия: {session_min} мин. Факт всего: {total_min} мин."
            )
            await callback.answer()


async def cmd_plan(message: types.Message) -> None:
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer("Формат: /plan <id> <минуты>")
        return

    token = parts[1].strip()
    minutes_str = parts[2].strip()
    if not minutes_str.isdigit():
        await message.answer("Формат: /plan <id> <минуты>")
        return

    minutes = int(minutes_str)
    if minutes < 1 or minutes > 1440:
        await message.answer("Формат: /plan <id> <минуты>")
        return

    with get_session() as session:
        item = resolve_item(session, token)
        if item is None:
            await message.answer(f"Не найдено: {token}")
            return

        item.planned_min = minutes
        session.commit()

    await message.answer(
        f"План установлен: {_short_id(item.id)} - {item.title} = {minutes} мин"
    )


async def cmd_today_fact(message: types.Message) -> None:
    tz = ZoneInfo(settings.timezone)
    today = datetime.now(tz).date()

    with get_session() as session:
        data = calc_fact_for_day(session, today, tz)

    if data["total_min"] == 0:
        await message.answer("Сегодня факта нет")
        return

    lines = [f"Факт за сегодня ({settings.timezone}):"]

    for row in data["by_project"][:10]:
        lines.append(f"- {row['project'] or 'Без проекта'}: {row['min']} мин")

    lines.append(f"Итого: {data['total_min']} мин")
    lines.append("Топ задач:")
    for row in data["by_item"][:5]:
        lines.append(
            f"- {row['id_short']} - {row['title']} ({row['project'] or 'Без проекта'}) — {row['min']} мин"
        )

    active = data["active"]
    if active:
        started = active["started_at"]
        if started.tzinfo is None:
            started = started.replace(tzinfo=tz)
        else:
            started = started.astimezone(tz)
        started_str = started.strftime("%H:%M")
        lines.append(
            f"Сейчас в работе: {active['id_short']} - {active['title']} "
            f"({active['elapsed_min']} мин, с {started_str})"
        )

    await message.answer("\n".join(lines))


async def cmd_conflicts(message: types.Message) -> None:
    with get_session() as session:
        items = list(
            session.scalars(
                select(Item).where(Item.sync_state == "conflict").limit(10)
            ).all()
        )

    if not items:
        await message.answer("Конфликтов нет")
        return

    for item in items:
        scheduled = _format_dt_local(item.scheduled_at)
        await message.answer(
            f"{_short_id(item.id)} - {item.title} - {scheduled}",
            reply_markup=_resolve_conflict_keyboard(item.id),
        )


async def cmd_resolve(message: types.Message) -> None:
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer("Формат: /resolve <id> remote|local")
        return

    token = parts[1].strip()
    mode = parts[2].strip().lower()
    if mode not in {"remote", "local"}:
        await message.answer("Формат: /resolve <id> remote|local")
        return

    with get_session() as session:
        item = resolve_item(session, token)
        if item is None:
            await message.answer(f"Не найдено: {token}")
            return

        if mode == "local":
            item.sync_state = "dirty"
            session.add(
                ItemEvent(
                    item_id=item.id,
                    event_type="resolve_local",
                    ts=datetime.now(timezone.utc),
                    meta_json=None,
                )
            )
            session.commit()
            await message.answer("Конфликт разрешен: оставлено локально")
            return

        event = session.scalar(
            select(ItemEvent)
            .where(
                ItemEvent.item_id == item.id,
                ItemEvent.event_type == "sync_in_conflict",
            )
            .order_by(ItemEvent.ts.desc())
        )
        if event is None or not event.meta_json:
            await message.answer("Нет данных календаря для разрешения")
            return

        meta = json.loads(event.meta_json)
        remote = meta.get("remote") or {}
        start_raw = remote.get("start")
        end_raw = remote.get("end")
        if not start_raw or not end_raw:
            await message.answer("Нет данных календаря для разрешения")
            return

        start_dt = _parse_rfc3339(start_raw)
        end_dt = _parse_rfc3339(end_raw)
        if not start_dt or not end_dt:
            await message.answer("Нет данных календаря для разрешения")
            return

        tz = ZoneInfo(remote.get("timeZone") or settings.timezone)
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=tz)
        else:
            start_dt = start_dt.astimezone(tz)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=tz)
        else:
            end_dt = end_dt.astimezone(tz)

        item.title = remote.get("summary") or item.title
        item.description = remote.get("description")
        item.scheduled_at = start_dt
        item.duration_min = int((end_dt - start_dt).total_seconds() / 60)
        item.etag = meta.get("etag") or item.etag
        updated_raw = meta.get("updated")
        if updated_raw:
            item.g_updated = _parse_rfc3339(updated_raw)
        item.sync_state = "synced"
        session.add(
            ItemEvent(
                item_id=item.id,
                event_type="resolve_remote",
                ts=datetime.now(timezone.utc),
                meta_json=None,
            )
        )
        session.commit()

    await message.answer("Конфликт разрешен: принят календарь")


async def cmd_push_calendar(message: types.Message) -> None:
    stats = {"processed": 0, "updated": 0, "created": 0, "cancelled": 0, "errors": 0}
    with get_session() as session:
        items = list(
            session.scalars(
                select(Item)
                .where(Item.type == "meeting", Item.sync_state == "dirty")
            ).all()
        )
        logger.info("push_calendar start dirty_count={}", len(items))
        for item in items:
            stats["processed"] += 1
            try:
                before_event = item.event_id
                before_status = item.status
                sync_out_meeting(session, item.id)
                if before_status == "canceled":
                    stats["cancelled"] += 1
                elif before_event:
                    stats["updated"] += 1
                else:
                    stats["created"] += 1
            except Exception:
                stats["errors"] += 1
        conflict_count = session.scalar(
            select(func.count()).select_from(Item).where(
                Item.type == "meeting", Item.sync_state == "conflict"
            )
        ) or 0
        dirty_count = session.scalar(
            select(func.count()).select_from(Item).where(
                Item.type == "meeting", Item.sync_state == "dirty"
            )
        ) or 0

    await message.answer(
        "Push: "
        f"processed={stats['processed']} created={stats['created']} "
        f"updated={stats['updated']} cancelled={stats['cancelled']} errors={stats['errors']} "
        f"dirty={dirty_count} conflict={conflict_count}"
    )
    logger.info("push_calendar done stats={}", stats)


def _as_local(value: datetime) -> datetime:
    tz = ZoneInfo(settings.timezone)
    if value.tzinfo is None:
        return value.replace(tzinfo=tz)
    return value.astimezone(tz)


async def cmd_cal_status(message: types.Message) -> None:
    with get_session() as session:
        state = session.scalar(
            select(CalendarSyncState).where(
                CalendarSyncState.calendar_id == settings.google_calendar_id_default
            )
        )
        meetings = list(session.scalars(select(Item).where(Item.type == "meeting")).all())

    sync_token_present = bool(state and state.sync_token)
    last_status = state.last_sync_status if state else None
    last_error = state.last_sync_error if state else None
    last_sync_at = _format_dt_local(state.updated_at) if state else ""

    dirty = len([m for m in meetings if m.sync_state == "dirty"])
    conflict = len([m for m in meetings if m.sync_state == "conflict"])
    synced = len([m for m in meetings if m.sync_state == "synced"])

    tz = ZoneInfo(settings.timezone)
    start_day = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = start_day + timedelta(days=1)
    active_today = 0
    for item in meetings:
        if item.scheduled_at is None:
            continue
        sched = _as_local(item.scheduled_at)
        if start_day <= sched < end_day:
            active_today += 1

    lines = [
        f"calendar_id={settings.google_calendar_id_default}",
        f"sync_token_present={str(sync_token_present).lower()}",
        f"last_sync_status={last_status or ''}",
        f"last_sync_error={last_error or ''}",
        f"last_sync_at={last_sync_at}",
        f"dirty={dirty} conflict={conflict} synced={synced}",
        f"meetings_today={active_today}",
    ]
    await message.answer("\n".join(lines))


async def cmd_meetings(message: types.Message) -> None:
    parts = (message.text or "").split()
    period = "today"
    if len(parts) >= 2:
        if parts[1] not in {"today", "week"}:
            await message.answer("Формат: /meetings [today|week]")
            return
        period = parts[1]

    await _send_meetings(message, period)


async def _send_meetings(message: types.Message, period: str) -> None:
    tz = ZoneInfo(settings.timezone)
    start_day = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "week":
        end_day = start_day + timedelta(days=7)
    else:
        end_day = start_day + timedelta(days=1)

    with get_session() as session:
        meetings = list(session.scalars(select(Item).where(Item.type == "meeting")).all())

    filtered = []
    for item in meetings:
        if item.scheduled_at is None:
            continue
        sched = _as_local(item.scheduled_at)
        if start_day <= sched < end_day:
            filtered.append((sched, item))

    filtered.sort(key=lambda row: row[0])
    if not filtered:
        await message.answer("Встреч нет")
        return

    lines = []
    for sched, item in filtered[:20]:
        lines.append(
            f"{sched.strftime('%H:%M')} - {item.title} - {_short_id(item.id)} - {task_status_ru(item.status)}"
        )
    await message.answer("\n".join(lines))


async def cmd_inbox(message: types.Message) -> None:
    await _send_inbox_tasks(message, page_token=None)


async def _send_inbox_tasks(message: types.Message, page_token: str | None) -> None:
    tasks, next_token = await _fetch_inbox_tasks(page_token)
    text = _build_inbox_message(tasks)
    keyboard_rows: list[list[InlineKeyboardButton]] = []
    for task in tasks:
        task_id = task.get("id")
        if not task_id:
            continue
        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    text=button_ru("BTN_PROCESS"),
                    callback_data=f"inbox:menu:{task_id}",
                ),
            ]
        )

    if next_token:
        pending_id = _store_inbox_page_token(message, next_token)
        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    text=button_ru("BTN_SHOW_MORE"),
                    callback_data=f"inbox:more:{pending_id}",
                )
            ]
        )

    reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard_rows) if keyboard_rows else None
    await message.answer(text, reply_markup=reply_markup)


def _store_inbox_page_token(message: types.Message, page_token: str) -> str:
    with get_session() as session:
        pending = create_pending(
            session,
            chat_id=message.chat.id,
            user_id=message.from_user.id if message.from_user else 0,
            intent=Intent.SHOW_INBOX_TASKS.value,
            action_type=Intent.SHOW_INBOX_TASKS.name,
            args_dict={},
            raw_head="inbox_more",
            raw_text="",
            source="system",
            confidence=1.0,
            canonical_text="",
            missing=[],
            stage="list",
            meta={"page_token": page_token},
        )
        return pending.id


def _is_meetings_query(text: str) -> bool:
    lowered = text.strip().lower()
    if not lowered:
        return False
    if "расписание" in lowered:
        return True
    if "meetings" in lowered or "meeting" in lowered:
        return True
    if "покажи" in lowered and "встреч" in lowered:
        return True
    if lowered.startswith("встречи"):
        return True
    return False


def _parse_meetings_period(text: str) -> str:
    lowered = text.lower()
    if "недел" in lowered or "week" in lowered:
        return "week"
    return "today"


async def cmd_open(message: types.Message) -> None:
    token = _parse_id_arg(message.text or "")
    if not token:
        await message.answer("Формат: /open <id>")
        return

    with get_session() as session:
        item = resolve_item(session, token)
        if item is None:
            await message.answer(f"Не найдено: {token}")
            return
        project = session.get(Project, item.project_id) if item.project_id else None
        event = session.scalar(
            select(ItemEvent)
            .where(ItemEvent.item_id == item.id, ItemEvent.event_type == "sync_in_conflict")
            .order_by(ItemEvent.ts.desc())
        )

    project_name = project.name if project else ""
    sched = _format_dt_local(item.scheduled_at or item.due_at)
    event_short = _short_id(item.event_id) if item.event_id else ""
    etag_short = _short_id(item.etag) if item.etag else ""
    updated = _format_dt_local(item.g_updated)

    id_label = field_ru("event_id") if item.type == "meeting" else field_ru("task_id")
    date_label = field_ru("date") if item.type == "meeting" else field_ru("due_date")
    lines = [
        f"{id_label}: {_short_id(item.id)}",
        f"{field_ru('title')}: {item.title}",
        f"{field_ru('status')}: {task_status_ru(item.status)}",
        f"{date_label}: {sched}",
        f"{field_ru('duration')}: {item.duration_min or ''}",
        f"{field_ru('project')}: {project_name}",
        f"{field_ru('sync_state')}: {item.sync_state}",
        f"{field_ru('event_id')}: {event_short}",
        f"{field_ru('g_updated')}: {updated}",
        f"{field_ru('etag')}: {etag_short}",
    ]

    if event and event.meta_json:
        meta = json.loads(event.meta_json)
        remote = meta.get("remote") or {}
        if remote:
            lines.append(
                f"remote: {remote.get('summary','')} | {remote.get('start','')} -> {remote.get('end','')}"
            )

    await message.answer("\n".join(lines), reply_markup=_open_keyboard(item))


async def on_open_action(
    callback: types.CallbackQuery,
    callback_data: OpenAction,
) -> None:
    message = callback.message
    if message is None:
        await callback.answer("Недоступно", show_alert=True)
        return
    if callback_data.action == "pull":
        with get_session() as session:
            stats = sync_in_calendar(session, settings.google_calendar_id_default)
        await message.answer(
            "Pull: "
            f"processed={stats['processed']} created={stats['created']} "
            f"updated={stats['updated']} cancelled={stats['cancelled']} "
            f"conflicts={stats['conflicts']} tokenReset={stats['token_reset']}"
        )
        await callback.answer()
        return

    if callback_data.action == "push":
        with get_session() as session:
            item = session.get(Item, callback_data.item_id)
            if item is None:
                await callback.answer("Задача не найдена", show_alert=True)
                return
            if item.sync_state != "dirty":
                await message.answer("Нет изменений для push")
                await callback.answer()
                return
            try:
                sync_out_meeting(session, item.id)
                await message.answer("Push выполнен")
                await callback.answer()
            except Exception:
                await message.answer("Push ошибка")
                await callback.answer()


def _parse_pending_callback(data: str | None) -> tuple[str, str] | None:
    if not data:
        return None
    parts = data.split(":")
    if len(parts) != 3 or parts[0] != "pa":
        return None
    action, pending_id = parts[1], parts[2]
    return action, pending_id


def _parse_inbox_callback(data: str | None) -> tuple[str, str] | None:
    if not data:
        return None
    parts = data.split(":")
    if len(parts) != 3 or parts[0] != "inbox":
        return None
    action, value = parts[1], parts[2]
    return action, value


def _parse_date_input(text: str) -> str | None:
    lowered = text.strip().lower()
    if lowered in {"сегодня", "today"}:
        return "today"
    if lowered in {"завтра", "tomorrow"}:
        return "tomorrow"
    if lowered in {"послезавтра", "после завтра", "day_after_tomorrow"}:
        return "day_after_tomorrow"
    match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", lowered)
    if match:
        return match.group(1)
    return None


def _parse_time_input(text: str) -> str | None:
    lowered = text.strip().lower()
    match = re.search(r"\b([01]?\d|2[0-3])\s*[:.\-]\s*([0-5]\d)\b", lowered)
    if match:
        hour = int(match.group(1))
        minute = int(match.group(2))
        return f"{hour:02d}:{minute:02d}"
    match = re.search(r"\b([01]?\d|2[0-3])\s+([0-5]\d)\b", lowered)
    if match:
        hour = int(match.group(1))
        minute = int(match.group(2))
        return f"{hour:02d}:{minute:02d}"
    match = re.search(r"\b(?:в\s*)?([01]?\d|2[0-3])\s*(?:час|часа|часов)\b", lowered)
    if match:
        hour = int(match.group(1))
        return f"{hour:02d}:00"
    match = re.search(r"\b([01]?\d|2[0-3])\b", lowered)
    if match:
        hour = int(match.group(1))
        return f"{hour:02d}:00"
    return None


async def _handle_pending_field_input(message: types.Message) -> bool:
    if not message.from_user:
        return False
    with get_session() as session:
        pending = get_latest_pending(
            session,
            chat_id=message.chat.id,
            user_id=message.from_user.id,
        )
        if not pending or pending.state != "await_input" or not pending.awaiting_field:
            return False

        try:
            args_dict = json.loads(pending.args_json)
        except json.JSONDecodeError:
            args_dict = {}

        field = pending.awaiting_field
        value_text = message.text or ""
        updated = dict(args_dict)
        parsed_value: str | None = None
        if field == "time" and not value_text.strip():
            await message.answer(
                f"Пришли значение для поля «{field_ru('time')}», например 18:30."
            )
            return True
        if field == "date":
            parsed_value = _parse_date_input(value_text)
        elif field == "time":
            value_text = normalize_asr_text(value_text)
            parsed_value = _parse_time_input(value_text)
        elif field == "title":
            parsed_value = value_text.strip() or None
        elif field == "project":
            parsed_value = value_text.strip() or None
        elif field == "minutes":
            match = re.search(r"\b(\d{1,4})\b", value_text)
            if match:
                parsed_value = match.group(1)
        elif field == "target":
            parsed_value = value_text.strip() or None

        if not parsed_value:
            logger.info("PENDING field_failed id={} field={}", pending.id, field)
            prompt = "Не удалось распознать значение. Напиши ещё раз."
            if field == "date":
                prompt = (
                    f"{field_ru('missing')} поля: {field_ru('date')}. "
                    "Напиши: сегодня / завтра / 2026-01-22"
                )
            if field == "time":
                prompt = (
                    f"{field_ru('missing')} поля: {field_ru('time')}. "
                    "Напиши: 16:00"
                )
            if field == "project":
                prompt = (
                    f"{field_ru('missing')} поля: {field_ru('project')}. "
                    "Напиши название проекта."
                )
            await message.answer(prompt)
            return True

        updated[field] = parsed_value
        missing_list, normalized_args = _validate_required_args(
            Intent(pending.intent),
            updated,
            pending.raw_text,
            pending.canonical_text,
        )
        next_field = missing_list[0] if missing_list else None
        new_state = "await_input" if next_field else "await_confirm"
        stage = pending.stage or "normal"
        meta: dict | None = None

        if Intent(pending.intent) == Intent.CREATE_MEETING and not next_field:
            check = _check_create_meeting_preconditions(normalized_args)
            if check:
                stage, preview_text, meta = check
                update_pending(
                    session,
                    pending.id,
                    args_dict=normalized_args,
                    missing=missing_list,
                    state="await_confirm",
                    awaiting_field=None,
                    stage=stage,
                    meta=meta,
                )
                await message.answer(
                    preview_text,
                    reply_markup=_pending_markup_for_stage(stage, pending.id),
                )
                return True

        final_stage = stage if next_field else "normal"
        final_meta = meta if meta is not None else ({} if final_stage == "normal" else None)
        update_pending(
            session,
            pending.id,
            args_dict=normalized_args,
            missing=missing_list,
            state=new_state,
            awaiting_field=next_field,
            stage=final_stage,
            meta=final_meta,
        )
        logger.info(
            "PENDING field_ok id={} field={} next_field={}",
            pending.id,
            field,
            next_field or "",
        )
        preview = _format_pending_preview(
            Intent(pending.intent),
            normalized_args,
            source=pending.source,
            confidence=pending.confidence,
            missing=missing_list,
        )
        await message.answer(
            preview,
            reply_markup=_pending_markup_for_stage(final_stage, pending.id),
        )
        return True



async def on_pending_callback(callback: types.CallbackQuery) -> None:
    parsed = _parse_pending_callback(callback.data)
    if not parsed:
        return
    action, pending_id = parsed
    message = callback.message
    if message is None or not isinstance(message, types.Message):
        await callback.answer("Недоступно", show_alert=True)
        return
    message = cast(types.Message, message)
    chat_id = message.chat.id
    user_id = callback.from_user.id if callback.from_user else 0

    now = datetime.now(timezone.utc)
    confirm_parsed: ParsedIntent | None = None
    with get_session() as session:
        pending = get_pending(session, pending_id)
        if pending is None:
            await message.answer("Черновик устарел. Повтори команду.")
            await callback.answer()
            return
        if pending.chat_id != chat_id or pending.user_id != user_id:
            await callback.answer("Недоступно", show_alert=True)
            return
        expires_at = pending.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= now:
            delete_pending(session, pending_id)
            await message.answer("Черновик устарел. Повтори команду.")
            await callback.answer()
            return

        try:
            args_dict = json.loads(pending.args_json)
        except json.JSONDecodeError:
            args_dict = {}
        try:
            meta_dict = json.loads(pending.meta_json) if pending.meta_json else {}
        except json.JSONDecodeError:
            meta_dict = {}

        if action == "cancel":
            delete_pending(session, pending_id)
            logger.info(
                "PENDING action=cancel id={} intent={} source={} conf={}",
                pending_id,
                pending.intent,
                pending.source,
                pending.confidence,
            )
            await message.answer("Отменено.")
            await callback.answer()
            return

        if action == "edit":
            awaiting_field = None
            if pending.intent in {"create_meeting", "move_meeting"}:
                awaiting_field = "date"
            elif pending.intent == Intent.SET_TASK_DATE.value:
                awaiting_field = "date"
            elif pending.intent == Intent.ASSIGN_TASK_PROJECT.value:
                awaiting_field = "project"
            elif pending.intent == Intent.RENAME_TASK.value:
                awaiting_field = "title"
            elif pending.intent in {Intent.DELETE_TASK.value, Intent.COMPLETE_TASK.value}:
                await message.answer("Нечего менять для этого действия.")
                await callback.answer()
                return
            update_pending(
                session,
                pending_id,
                state="await_input" if awaiting_field else "await_edit",
                awaiting_field=awaiting_field,
            )
            logger.info(
                "PENDING action=edit id={} intent={} source={} conf={}",
                pending_id,
                pending.intent,
                pending.source,
                pending.confidence,
            )
            if awaiting_field == "date":
                await message.answer("Ок. Укажи дату: сегодня / завтра / 2026-01-22")
            elif awaiting_field == "project":
                await message.answer("Ок. Укажи проект.")
            elif awaiting_field == "title":
                await message.answer("Ок. Укажи новое название.")
            else:
                await message.answer(
                    "Ок. Напиши корректировку одной фразой (или используй команду /...)."
                )
            await callback.answer()
            return

        if action == "conflict_move_new":
            update_pending(
                session,
                pending_id,
                state="await_input",
                awaiting_field="time",
                stage="conflict",
            )
            await message.answer("Укажи новое время для встречи, например 16:30.")
            await callback.answer()
            return

        if action == "conflict_move_existing":
            conflict_ids = meta_dict.get("conflicting_event_ids") or []
            target_id = conflict_ids[0] if conflict_ids else None
            if not target_id:
                await message.answer("Не удалось найти конфликтующую встречу.")
                await callback.answer()
                return
            delete_pending(session, pending_id)
            move_args = {"target": target_id, "date": "today"}
            move_pending = create_pending(
                session,
                chat_id=chat_id,
                user_id=user_id,
                intent=Intent.MOVE_MEETING.value,
                action_type=Intent.MOVE_MEETING.name,
                args_dict=move_args,
                raw_head="conflict_move_existing",
                raw_text="",
                source="system",
                confidence=1.0,
                canonical_text="",
                missing=["time"],
                stage="normal",
            )
            update_pending(
                session,
                move_pending.id,
                state="await_input",
                awaiting_field="time",
            )
            await message.answer("Укажи новое время для существующей встречи, например 16:30.")
            await callback.answer()
            return

        if action == "conflict_add":
            args_dict["allow_overlap"] = True
            confirm_parsed = ParsedIntent(
                Intent.CREATE_MEETING,
                pending.confidence,
                args_dict,
                pending.raw_text,
            )
            delete_pending(session, pending_id)
            await callback.answer()
            await _dispatch_intent(message, confirm_parsed)
            return

        if action == "duplicate_add":
            args_dict["allow_duplicate"] = True
            confirm_parsed = ParsedIntent(
                Intent.CREATE_MEETING,
                pending.confidence,
                args_dict,
                pending.raw_text,
            )
            delete_pending(session, pending_id)
            await callback.answer()
            await _dispatch_intent(message, confirm_parsed)
            return

        if action == "duplicate_edit_title":
            update_pending(
                session,
                pending_id,
                state="await_input",
                awaiting_field="title",
                stage="duplicate",
            )
            await message.answer("Укажи новое название встречи.")
            await callback.answer()
            return

        if action == "duplicate_reschedule":
            update_pending(
                session,
                pending_id,
                state="await_input",
                awaiting_field="time",
                stage="duplicate",
            )
            await message.answer("Укажи новое время для встречи, например 16:30.")
            await callback.answer()
            return

        if action != "confirm":
            await callback.answer("Недоступно", show_alert=True)
            return

        if pending.state != "await_confirm":
            await message.answer("Черновик устарел. Повтори команду.")
            await callback.answer()
            return

        try:
            missing = json.loads(pending.missing_json)
        except json.JSONDecodeError:
            missing = []
        try:
            pending_intent = Intent(pending.intent)
        except ValueError:
            await message.answer(FALLBACK_TEXT)
            await callback.answer()
            delete_pending(session, pending_id)
            return

        missing_list, normalized_args = _validate_required_args(
            pending_intent,
            args_dict,
            pending.raw_text,
            pending.canonical_text,
        )
        if missing_list:
            awaiting_field = missing_list[0]
            update_pending(
                session,
                pending_id,
                args_dict=normalized_args,
                missing=missing_list,
                state="await_input",
                awaiting_field=awaiting_field,
            )
            logger.info(
                "PENDING missing id={} field={}",
                pending_id,
                awaiting_field,
            )
            if awaiting_field == "date":
                await message.answer(
                    f"{field_ru('missing')} поля: {field_ru('date')}. "
                    "Напиши: сегодня / завтра / 2026-01-22"
                )
            elif awaiting_field == "time":
                await message.answer(
                    f"{field_ru('missing')} поля: {field_ru('time')}. Напиши: 16:00"
                )
            elif awaiting_field == "title":
                await message.answer(
                    f"{field_ru('missing')} поля: {field_ru('title')}. Напиши кратко."
                )
            elif awaiting_field == "project":
                await message.answer(
                    f"{field_ru('missing')} поля: {field_ru('project')}. "
                    "Напиши название проекта."
                )
            elif awaiting_field == "minutes":
                await message.answer(
                    f"{field_ru('missing')} поля: {field_ru('minutes')}. Напиши: 40"
                )
            elif awaiting_field == "target":
                await message.answer(
                    f"{field_ru('missing')} поля: {field_ru('target')}. "
                    "Напиши: <id или название>"
                )
            else:
                await message.answer("Нужно уточнить данные. Напиши корректировку.")
            await callback.answer()
            return

        confirm_parsed = ParsedIntent(
            intent=pending_intent,
            confidence=1.0,
            args=normalized_args,
            raw_text=pending.raw_head,
        )
        logger.info(
            "PENDING action=confirm id={} intent={} source={} conf={}",
            pending_id,
            pending.intent,
            pending.source,
            pending.confidence,
        )
        delete_pending(session, pending_id)

    if confirm_parsed is None:
        await callback.answer()
        return
    await _dispatch_intent(message, confirm_parsed)
    await callback.answer()


async def on_inbox_callback(callback: types.CallbackQuery) -> None:
    parsed = _parse_inbox_callback(callback.data)
    if not parsed:
        return
    action, value = parsed
    message = callback.message
    if message is None or not isinstance(message, types.Message):
        await callback.answer("Недоступно", show_alert=True)
        return
    message = cast(types.Message, message)
    chat_id = message.chat.id
    user_id = callback.from_user.id if callback.from_user else 0

    if action == "more":
        with get_session() as session:
            pending = get_pending(session, value)
            if pending is None:
                await callback.answer("Больше нет", show_alert=True)
                return
            try:
                meta = json.loads(pending.meta_json) if pending.meta_json else {}
            except json.JSONDecodeError:
                meta = {}
            page_token = meta.get("page_token")
            delete_pending(session, pending.id)
        if not page_token:
            await message.answer("Больше нет.")
            await callback.answer()
            return
        await _send_inbox_tasks(message, page_token=page_token)
        await callback.answer()
        return

    task_id = value
    if action == "menu":
        await _send_task_menu(message, task_id)
        await callback.answer()
        return

    if action in {"menu_set_date", "menu_set_project", "menu_rename", "menu_done", "menu_cancel"}:
        with get_session() as session:
            _clear_task_pending(session, task_id, chat_id, user_id)

    client = TasksClient()
    try:
        task = await asyncio.to_thread(client.get_task, "@default", task_id)
    except Exception as exc:
        logger.error("Inbox task fetch failed: {}", exc)
        await callback.answer("Не удалось получить задачу", show_alert=True)
        return

    title = _normalize_task_title(task.get("title"))
    args_base = {"task_id": task_id, "task_title": title, "list_id": "@default"}

    with get_session() as session:
        if action == "menu_set_date":
            pending = create_pending(
                session,
                chat_id=chat_id,
                user_id=user_id,
                intent=Intent.SET_TASK_DATE.value,
                action_type=Intent.SET_TASK_DATE.name,
                args_dict=args_base,
                raw_head="inbox_set_date",
                raw_text="",
                source="system",
                confidence=1.0,
                canonical_text="",
                missing=["date"],
                stage="normal",
                meta={"task_id": task_id},
            )
            update_pending(session, pending.id, state="await_input", awaiting_field="date")
            await message.answer(
                f"Укажи значение для поля «{field_ru('due_date')}» "
                "(сегодня / завтра / 2026-01-22)."
            )
            await callback.answer()
            return
        if action == "menu_set_project":
            pending = create_pending(
                session,
                chat_id=chat_id,
                user_id=user_id,
                intent=Intent.ASSIGN_TASK_PROJECT.value,
                action_type=Intent.ASSIGN_TASK_PROJECT.name,
                args_dict=args_base,
                raw_head="inbox_assign_project",
                raw_text="",
                source="system",
                confidence=1.0,
                canonical_text="",
                missing=["project"],
                stage="normal",
                meta={"task_id": task_id},
            )
            update_pending(session, pending.id, state="await_input", awaiting_field="project")
            await message.answer(f"Укажи значение для поля «{field_ru('project')}».")
            await callback.answer()
            return
        if action == "menu_rename":
            pending = create_pending(
                session,
                chat_id=chat_id,
                user_id=user_id,
                intent=Intent.RENAME_TASK.value,
                action_type=Intent.RENAME_TASK.name,
                args_dict=args_base,
                raw_head="inbox_rename",
                raw_text="",
                source="system",
                confidence=1.0,
                canonical_text="",
                missing=["title"],
                stage="normal",
                meta={"task_id": task_id},
            )
            update_pending(session, pending.id, state="await_input", awaiting_field="title")
            await message.answer(f"Укажи значение для поля «{field_ru('title')}».")
            await callback.answer()
            return
        if action == "menu_done":
            pending = create_pending(
                session,
                chat_id=chat_id,
                user_id=user_id,
                intent=Intent.COMPLETE_TASK.value,
                action_type=Intent.COMPLETE_TASK.name,
                args_dict=args_base,
                raw_head="inbox_done",
                raw_text="",
                source="system",
                confidence=1.0,
                canonical_text="",
                missing=[],
                stage="normal",
                meta={"task_id": task_id},
            )
            preview = _format_pending_preview(
                Intent.COMPLETE_TASK,
                args_base,
                source="system",
                confidence=1.0,
                missing=[],
            )
            await message.answer(preview, reply_markup=_pending_keyboard(pending.id))
            await callback.answer()
            return
        if action == "menu_cancel":
            await message.answer("Отменено.")
            await callback.answer()
            return

    await callback.answer("Недоступно", show_alert=True)


async def on_resolve_action(
    callback: types.CallbackQuery,
    callback_data: ResolveAction,
) -> None:
    message = callback.message
    if message is None or not isinstance(message, types.Message):
        await callback.answer("Недоступно", show_alert=True)
        return
    message = cast(types.Message, message)
    with get_session() as session:
        item = session.get(Item, callback_data.item_id)
        if item is None:
            await callback.answer("Задача не найдена", show_alert=True)
            return

        if callback_data.action == "local":
            item.sync_state = "dirty"
            session.add(
                ItemEvent(
                    item_id=item.id,
                    event_type="resolve_local",
                    ts=datetime.now(timezone.utc),
                    meta_json=None,
                )
            )
            session.commit()
            await message.answer("Конфликт разрешен: оставлено локально")
            await callback.answer()
            return

        event = session.scalar(
            select(ItemEvent)
            .where(
                ItemEvent.item_id == item.id,
                ItemEvent.event_type == "sync_in_conflict",
            )
            .order_by(ItemEvent.ts.desc())
        )
        if event is None or not event.meta_json:
            await callback.answer("Нет данных календаря для разрешения", show_alert=True)
            return

        meta = json.loads(event.meta_json)
        remote = meta.get("remote") or {}
        start_raw = remote.get("start")
        end_raw = remote.get("end")
        if not start_raw or not end_raw:
            await callback.answer("Нет данных календаря для разрешения", show_alert=True)
            return

        start_dt = _parse_rfc3339(start_raw)
        end_dt = _parse_rfc3339(end_raw)
        if not start_dt or not end_dt:
            await callback.answer("Нет данных календаря для разрешения", show_alert=True)
            return

        tz = ZoneInfo(remote.get("timeZone") or settings.timezone)
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=tz)
        else:
            start_dt = start_dt.astimezone(tz)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=tz)
        else:
            end_dt = end_dt.astimezone(tz)

        item.title = remote.get("summary") or item.title
        item.description = remote.get("description")
        item.scheduled_at = start_dt
        item.duration_min = int((end_dt - start_dt).total_seconds() / 60)
        item.etag = meta.get("etag") or item.etag
        updated_raw = meta.get("updated")
        if updated_raw:
            item.g_updated = _parse_rfc3339(updated_raw)
        item.sync_state = "synced"
        session.add(
            ItemEvent(
                item_id=item.id,
                event_type="resolve_remote",
                ts=datetime.now(timezone.utc),
                meta_json=None,
            )
        )
        session.commit()

    await message.answer("Конфликт разрешен: принят календарь")
    await callback.answer()


async def _dispatch_intent(message: types.Message, parsed: "ParsedIntent") -> None:
    logger.info("DISPATCH intent={} conf={}", parsed.intent, parsed.confidence)

    if parsed.intent == Intent.CREATE_MEETING:
        title = parsed.args.get("title") or "Встреча"
        date = parsed.args.get("date")
        time_value = parsed.args.get("time")
        duration = parsed.args.get("duration") or 60
        if not date or not time_value:
            await message.answer("Формат: /meet \"Название\" YYYY-MM-DD HH:MM 60")
            return
        if date in {"today", "tomorrow", "day_after_tomorrow"}:
            tz = ZoneInfo(settings.timezone)
            base = datetime.now(tz).date()
            if date == "tomorrow":
                base = base + timedelta(days=1)
            if date == "day_after_tomorrow":
                base = base + timedelta(days=2)
            date_str = base.strftime("%Y-%m-%d")
        else:
            date_str = date
        scheduled_at = datetime.strptime(f"{date_str} {time_value}", "%Y-%m-%d %H:%M")
        scheduled_at = scheduled_at.replace(tzinfo=ZoneInfo(settings.timezone))

        synced = await _create_meeting(message, title, scheduled_at, duration)
        if synced is None:
            return
        await message.answer(
            f"Встреча в календаре: {scheduled_at.strftime('%Y-%m-%d %H:%M')} - "
            f"{synced.title} (id: {_short_id(synced.id)})"
        )
        return

    if parsed.intent == Intent.CREATE_TASK:
        title = parsed.args.get("title")
        if not title:
            await message.answer("Формат: /task <название>")
            return
        with get_session() as session:
            project = session.scalar(select(Project).where(Project.name == "Inbox"))
            if project is None:
                await message.answer("Проект Inbox не найден.")
                return
            item = create_item(
                session,
                title=title,
                project_id=project.id,
                type="task",
                status="inbox",
            )
        await message.answer(f"Добавлено в Inbox: {_short_id(item.id)} - {item.title}")
        return

    if parsed.intent == Intent.MOVE_MEETING:
        target = parsed.args.get("target")
        date = parsed.args.get("date")
        time_value = parsed.args.get("time")
        if not target or not date or not time_value:
            await message.answer("Формат: перенеси <id|название> YYYY-MM-DD HH:MM")
            return
        if date in {"today", "tomorrow", "day_after_tomorrow"}:
            tz = ZoneInfo(settings.timezone)
            base = datetime.now(tz).date()
            if date == "tomorrow":
                base = base + timedelta(days=1)
            if date == "day_after_tomorrow":
                base = base + timedelta(days=2)
            date_str = base.strftime("%Y-%m-%d")
        else:
            date_str = date
        scheduled_at = datetime.strptime(f"{date_str} {time_value}", "%Y-%m-%d %H:%M")
        scheduled_at = scheduled_at.replace(tzinfo=ZoneInfo(settings.timezone))

        with get_session() as session:
            item = _find_meeting_by_target(session, target, scheduled_at)
            if item is None:
                await message.answer(f"Не найдено: {target}")
                return
            move_item(session, item.id, scheduled_at=scheduled_at)
            try:
                sync_out_meeting(session, item.id)
            except Exception as exc:
                logger.error("Move meeting sync failed: {}", exc)
        await message.answer(
            f"Перенесено: {_short_id(item.id)} - {item.title} на "
            f"{scheduled_at.strftime('%Y-%m-%d %H:%M')}"
        )
        return

    if parsed.intent == Intent.PLAN_TASK:
        minutes = parsed.args.get("minutes")
        target = parsed.args.get("target")
        if not minutes or not target:
            await message.answer("Формат: /plan <id> <минуты>")
            return
        with get_session() as session:
            item = resolve_item(session, target)
            if item is None:
                await message.answer(f"Не найдено: {target}")
                return
            item.planned_min = int(minutes)
            session.commit()
        await message.answer(
            f"План установлен: {_short_id(item.id)} - {item.title} = {minutes} мин"
        )
        return

    if parsed.intent == Intent.START_WORK:
        target = parsed.args.get("target")
        if not target:
            await message.answer("Формат: /startwork <id>")
            return
        await _handle_startwork(message, target)
        return

    if parsed.intent == Intent.STOP_WORK:
        target = parsed.args.get("target")
        if not target:
            await message.answer("Формат: /stopwork <id>")
            return
        await _handle_stopwork(message, target)
        return

    if parsed.intent == Intent.EXPORT:
        await _send_export(message, {"mode": "today"})
        return

    if parsed.intent == Intent.SHOW_INBOX_TASKS:
        await _send_inbox_tasks(message, page_token=None)
        return

    if parsed.intent == Intent.SET_TASK_DATE:
        task_id = parsed.args.get("task_id")
        date_value = parsed.args.get("date")
        list_id = parsed.args.get("list_id") or "@default"
        if not task_id or not date_value:
            await message.answer(
                f"{field_ru('missing')} данных для поля «{field_ru('due_date')}»."
            )
            return
        day = _resolve_date_value(date_value)
        if day is None:
            await message.answer("Не удалось распознать дату.")
            return
        tz = ZoneInfo(settings.timezone)
        due_local = datetime.combine(day, time(hour=9, minute=0), tzinfo=tz)
        due_iso = due_local.astimezone(timezone.utc).isoformat()
        client = TasksClient()
        try:
            await asyncio.to_thread(
                client.update_task,
                list_id,
                task_id,
                {"due": due_iso},
            )
            await message.answer("Дата установлена.")
            await _send_task_menu(message, task_id)
        except Exception as exc:
            logger.error("Task set date failed: {}", exc)
            await message.answer("Не удалось установить дату.")
        return

    if parsed.intent == Intent.ASSIGN_TASK_PROJECT:
        task_id = parsed.args.get("task_id")
        project = parsed.args.get("project")
        list_id = parsed.args.get("list_id") or "@default"
        if not task_id or not project:
            await message.answer(
                f"{field_ru('missing')} данных для поля «{field_ru('project')}»."
            )
            return
        client = TasksClient()
        try:
            task = await asyncio.to_thread(client.get_task, list_id, task_id)
            notes = task.get("notes")
            updated_notes = _set_project_in_notes(notes, project)
            await asyncio.to_thread(
                client.update_task,
                list_id,
                task_id,
                {"notes": updated_notes},
            )
            await message.answer("Проект назначен.")
            await _send_task_menu(message, task_id)
        except Exception as exc:
            logger.error("Task assign project failed: {}", exc)
            await message.answer("Не удалось назначить проект.")
        return

    if parsed.intent == Intent.RENAME_TASK:
        task_id = parsed.args.get("task_id")
        title = parsed.args.get("title")
        list_id = parsed.args.get("list_id") or "@default"
        if not task_id or not title:
            await message.answer(
                f"{field_ru('missing')} данных для поля «{field_ru('title')}»."
            )
            return
        client = TasksClient()
        try:
            await asyncio.to_thread(client.update_task, list_id, task_id, {"title": title})
            await message.answer("Название обновлено.")
            await _send_task_menu(message, task_id)
        except Exception as exc:
            logger.error("Task rename failed: {}", exc)
            await message.answer("Не удалось переименовать.")
        return

    if parsed.intent == Intent.DELETE_TASK:
        task_id = parsed.args.get("task_id")
        list_id = parsed.args.get("list_id") or "@default"
        if not task_id:
            await message.answer(
                f"{field_ru('missing')} данных для поля «{field_ru('task_id')}»."
            )
            return
        client = TasksClient()
        try:
            await asyncio.to_thread(client.delete_task, list_id, task_id)
            await message.answer("Задача удалена.")
        except Exception as exc:
            logger.error("Task delete failed: {}", exc)
            await message.answer("Не удалось удалить задачу.")
        return

    if parsed.intent == Intent.COMPLETE_TASK:
        task_id = parsed.args.get("task_id")
        list_id = parsed.args.get("list_id") or "@default"
        if not task_id:
            await message.answer(
                f"{field_ru('missing')} данных для поля «{field_ru('task_id')}»."
            )
            return
        client = TasksClient()
        try:
            await asyncio.to_thread(client.update_task, list_id, task_id, {"status": "completed"})
            await message.answer("Задача завершена.")
            await _send_task_menu(message, task_id)
        except Exception as exc:
            logger.error("Task complete failed: {}", exc)
            await message.answer("Не удалось завершить задачу.")
        return

    await message.answer(FALLBACK_TEXT)


async def handle_user_text(message: types.Message, text: str, source: str) -> None:
    logger.info(
        "HANDLE_USER_TEXT source={} text_len={} preview={!r}",
        source,
        len(text or ""),
        (text or "")[:80],
    )
    work_text = text or ""
    if source == "voice":
        logger.info("VOICE before_norm={!r}", (work_text or "")[:120])
        work_text = normalize_asr_text(work_text)
        logger.info("VOICE after_norm={!r}", (work_text or "")[:120])
        if not work_text.strip():
            await message.answer(ASR_EMPTY_TEXT)
            return

    if await _handle_pending_field_input(message):
        return

    if _is_meetings_query(work_text):
        period = _parse_meetings_period(work_text)
        await _send_meetings(message, period)
        return

    parsed = parse_intent(work_text)
    head = (work_text or "").replace("\n", " ")[:50]
    logger.info("NLU head={} intent={} conf={}", head, parsed.intent, parsed.confidence)
    if parsed.confidence >= 0.6:
        with get_session() as session:
            latest = get_latest_pending(
                session,
                chat_id=message.chat.id,
                user_id=message.from_user.id if message.from_user else 0,
            )
            if latest and latest.state == "await_edit":
                delete_pending(session, latest.id)
        pending = _build_pending(
            parsed,
            raw_head=head,
            raw_text=work_text,
            source="nlu",
            confidence=parsed.confidence,
            canonical_text="",
            missing=[],
            message=message,
        )
        if pending is None:
            logger.info("FALLBACK legacy path triggered source={} reason=preview_none", source)
            await message.answer(SOFT_FALLBACK_TEXT)
            return
        pending_id, preview, stage = pending
        await message.answer(preview, reply_markup=_pending_markup_for_stage(stage, pending_id))
        logger.info("PREVIEW created id={} source=nlu conf={}", pending_id, parsed.confidence)
        return

    if _llm_enabled():
        llm_result = await llm_normalizer.normalize(work_text)
        llm_head = llm_result.canonical_text[:50]
        logger.info(
            "LLM head={} intent={} conf={}",
            llm_head,
            llm_result.intent,
            llm_result.confidence,
        )
        if llm_result.intent != "NONE" and llm_result.confidence >= 0.6:
            if not llm_result.canonical_text:
                logger.info("FALLBACK legacy path triggered source={} reason=empty_llm", source)
                await message.answer(SOFT_FALLBACK_TEXT)
                return
            parsed_llm = parse_intent(llm_result.canonical_text)
            if parsed_llm.intent == Intent.NONE:
                logger.info("FALLBACK legacy path triggered source={} reason=llm_none", source)
                await message.answer(SOFT_FALLBACK_TEXT)
                return
            with get_session() as session:
                latest = get_latest_pending(
                    session,
                    chat_id=message.chat.id,
                    user_id=message.from_user.id if message.from_user else 0,
                )
                if latest and latest.state == "await_edit":
                    delete_pending(session, latest.id)
            pending = _build_pending(
                parsed_llm,
                raw_head=llm_head,
                raw_text=work_text,
                source="llm",
                confidence=llm_result.confidence,
                canonical_text=llm_result.canonical_text,
                missing=llm_result.missing,
                message=message,
            )
            if pending is None:
                logger.info("FALLBACK legacy path triggered source={} reason=preview_none", source)
                await message.answer(SOFT_FALLBACK_TEXT)
                return
            pending_id, preview, stage = pending
            await message.answer(preview, reply_markup=_pending_markup_for_stage(stage, pending_id))
            logger.info(
                "PREVIEW created id={} source=llm conf={}",
                pending_id,
                llm_result.confidence,
            )
            return

    logger.info("FALLBACK legacy path triggered source={} reason=not_recognized", source)
    await message.answer(SOFT_FALLBACK_TEXT)


async def handle_voice(message: types.Message) -> None:
    audio = message.voice or message.audio
    if audio is None:
        return

    file_id = audio.file_id
    logger.info(
        "VOICE: received file_id={} duration={}",
        file_id,
        getattr(audio, "duration", None),
    )
    try:
        file_info = await bot.get_file(file_id)
        file_path = file_info.file_path or ""
        ext = Path(file_path).suffix or ".ogg"
        buffer = BytesIO()
        await bot.download_file(file_path, destination=buffer)
        data = buffer.getvalue()
        logger.info("VOICE: downloaded bytes = {}", len(data))
        raw_text = await asr_transcribe(
            data,
            filename=f"{file_id}{ext}",
            language="ru",
        )
        logger.info(
            "VOICE raw_text_len={} preview={!r}",
            len(raw_text or ""),
            (raw_text or "")[:80],
        )
        text = raw_text or ""
        logger.info("VOICE ASR text_len={} preview={!r}", len(text), text[:80])
        await handle_user_text(message, text, source="voice")
    except Exception as exc:
        logger.warning("VOICE fail err={}", type(exc).__name__)
        await message.answer(SOFT_FALLBACK_TEXT)


async def echo_text(message: types.Message) -> None:
    text = message.text or ""
    if not text.strip():
        return
    if text.strip().startswith("/"):
        return
    await handle_user_text(message, text, source="text")


def setup_handlers(dispatcher: Dispatcher) -> None:
    dispatcher.message.register(cmd_start, Command("start"))
    dispatcher.message.register(cmd_meet, Command("meet"))
    dispatcher.message.register(cmd_sync_calendar, Command("sync_calendar"))
    dispatcher.message.register(cmd_export, Command("export"))
    dispatcher.message.register(cmd_startwork, Command("startwork"))
    dispatcher.message.register(cmd_stopwork, Command("stopwork"))
    dispatcher.message.register(cmd_work, Command("work"))
    dispatcher.message.register(cmd_plan, Command("plan"))
    dispatcher.message.register(cmd_today_fact, Command("today_fact"))
    dispatcher.message.register(cmd_task, Command("task"))
    dispatcher.message.register(cmd_inbox, Command("inbox"))
    dispatcher.message.register(cmd_conflicts, Command("conflicts"))
    dispatcher.message.register(cmd_resolve, Command("resolve"))
    dispatcher.message.register(cmd_push_calendar, Command("push_calendar"))
    dispatcher.message.register(cmd_cal_status, Command("cal_status"))
    dispatcher.message.register(cmd_meetings, Command("meetings"))
    dispatcher.message.register(cmd_open, Command("open"))
    dispatcher.message.register(handle_voice, F.voice)
    dispatcher.message.register(handle_voice, F.audio)
    dispatcher.message.register(echo_text, F.text)
    dispatcher.callback_query.register(on_pending_callback, F.data.startswith("pa:"))
    dispatcher.callback_query.register(on_inbox_callback, F.data.startswith("inbox:"))
    dispatcher.callback_query.register(on_resolve_action, ResolveAction.filter())
    dispatcher.callback_query.register(on_work_action, WorkAction.filter())
    dispatcher.callback_query.register(on_open_action, OpenAction.filter())


setup_handlers(dp)


