from __future__ import annotations

import re
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
from typing import Any


class Intent(str, Enum):
    NONE = "none"
    CREATE_MEETING = "create_meeting"
    MOVE_MEETING = "move_meeting"
    CREATE_TASK = "create_task"
    PLAN_TASK = "plan_task"
    START_WORK = "start_work"
    STOP_WORK = "stop_work"
    EXPORT = "export"
    SHOW_INBOX_TASKS = "show_inbox_tasks"
    SET_TASK_DATE = "set_task_date"
    ASSIGN_TASK_PROJECT = "assign_task_project"
    RENAME_TASK = "rename_task"
    DELETE_TASK = "delete_task"
    COMPLETE_TASK = "complete_task"


@dataclass
class ParsedIntent:
    intent: Intent
    confidence: float
    args: dict[str, Any]
    raw_text: str


_TIME_RE = re.compile(r"\b(\d{1,2}:\d{2})\b")
_TIME_HOURS_MIN_RE = re.compile(
    r"\b(?:в\s*)?([01]?\d|2[0-3])\s*(?:час|часа|часов|ч)\s*"
    r"([0-5]?\d)\s*(?:минут|мин|м)\b",
    flags=re.IGNORECASE,
)
_TIME_HOURS_RE = re.compile(
    r"\b(?:в\s*)?([01]?\d|2[0-3])\s*(?:час|часа|часов|ч)\b",
    flags=re.IGNORECASE,
)
_DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
_DATE_DOT_RE = re.compile(r"\b(\d{1,2})\.(\d{1,2})\b")
_DURATION_WITH_UNIT_RE = re.compile(r"\b(\d{1,4})\s*(минут|мин|час|ч)\b")
MEETING_WORDS = [
    "встреча",
    "митинг",
    "созвон",
    "созвониться",
    "совещание",
]


def _strip_keywords(text: str, keywords: list[str]) -> str:
    lowered = text.lower()
    for kw in keywords:
        lowered = lowered.replace(kw, "")
    return " ".join(lowered.split())


def _parse_time(text: str) -> str | None:
    match = _TIME_RE.search(text)
    if match:
        return match.group(1)
    hm_match = _TIME_HOURS_MIN_RE.search(text)
    if hm_match:
        hour = int(hm_match.group(1))
        minute = int(hm_match.group(2))
        return f"{hour:02d}:{minute:02d}"
    hours_match = _TIME_HOURS_RE.search(text.lower())
    if hours_match:
        hour = int(hours_match.group(1))
        return f"{hour:02d}:00"
    return None


def _extract_date_time(text: str) -> tuple[str | None, str | None]:
    date = None
    m = _DATE_RE.search(text)
    if m:
        date = m.group(1)
    if date is None:
        d = _DATE_DOT_RE.search(text)
        if d:
            day = int(d.group(1))
            month = int(d.group(2))
            year = datetime.now().year
            date = f"{year:04d}-{month:02d}-{day:02d}"
    lowered = text.lower()
    if date is None and "сегодня" in lowered:
        date = "today"
    if date is None and "завтра" in lowered:
        date = "tomorrow"
    if date is None and "послезавтра" in lowered:
        date = "day_after_tomorrow"
    return date, _parse_time(text)


def _extract_duration_minutes(text: str) -> int | None:
    match = _DURATION_WITH_UNIT_RE.search(text.lower())
    if not match:
        return None
    value = int(match.group(1))
    unit = match.group(2)
    if unit in {"час", "ч"}:
        return value * 60
    return value


def _strip_meeting_title(text: str, keywords: list[str]) -> str:
    cleaned = _strip_keywords(text, keywords)
    cleaned = _DATE_RE.sub("", cleaned)
    cleaned = _DATE_DOT_RE.sub("", cleaned)
    cleaned = _TIME_RE.sub("", cleaned)
    cleaned = _TIME_HOURS_RE.sub("", cleaned)
    cleaned = _DURATION_WITH_UNIT_RE.sub("", cleaned)
    cleaned = cleaned.replace("сегодня", "").replace("завтра", "").replace("послезавтра", "")
    cleaned = re.sub(r"\bна\b", " ", cleaned)
    cleaned = re.sub(r"\bв\b", " ", cleaned)
    return " ".join(cleaned.split())


def _extract_move_target(text: str) -> str:
    cleaned = _strip_keywords(text, ["перенеси", "сдвинь"])
    return _clean_target(cleaned)


def _clean_target(text: str) -> str:
    cleaned = text.strip()

    cleaned = re.sub(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", "", cleaned)
    cleaned = _TIME_HOURS_RE.sub("", cleaned)
    cleaned = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", "", cleaned)
    cleaned = re.sub(r"\b([0-3]?\d)\.([01]?\d)(?:\.(\d{2,4}))?\b", "", cleaned)
    cleaned = re.sub(r"\b(сегодня|завтра|послезавтра)\b", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\b(встреча|встречу|встречи)\b", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\bна\s+\d{1,4}\s*(мин|минут|м)\b", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\bна\s+\d{1,3}\s*(ч|час|часа)\b", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\b(в|на|к)\b", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,;:-")
    return cleaned


def parse_intent(text: str) -> ParsedIntent:
    raw = text or ""
    stripped = raw.strip()
    if not stripped:
        return ParsedIntent(Intent.NONE, 0.0, {}, raw)
    if stripped.startswith("/"):
        return ParsedIntent(Intent.NONE, 0.0, {}, raw)

    lowered = stripped.lower()

    if any(w in lowered for w in MEETING_WORDS) or any(
        k in lowered for k in ["создай встречу", "запланируй встречу"]
    ):
        date, time = _extract_date_time(stripped)
        duration = _extract_duration_minutes(stripped)
        title = _strip_meeting_title(
            stripped,
            ["создай встречу", "запланируй встречу", "встреча"],
        )
        if date and time:
            return ParsedIntent(
                Intent.CREATE_MEETING,
                0.9,
                {"title": title.strip(), "date": date, "time": time, "duration": duration},
                raw,
            )
        return ParsedIntent(
            Intent.CREATE_MEETING,
            0.6,
            {"title": title.strip(), "date": date, "time": time, "duration": duration},
            raw,
        )

    if any(k in lowered for k in ["перенеси", "сдвинь"]):
        date, time = _extract_date_time(stripped)
        target = _extract_move_target(stripped)
        if date and time and target:
            return ParsedIntent(
                Intent.MOVE_MEETING,
                0.9,
                {"target": target.strip(), "date": date, "time": time},
                raw,
            )
        return ParsedIntent(
            Intent.MOVE_MEETING,
            0.6,
            {"target": target.strip(), "date": date, "time": time},
            raw,
        )

    if any(
        k in lowered
        for k in [
            "покажи неразобранные",
            "покажи входящие",
            "inbox",
            "задачи без даты",
            "входящие задачи",
            "неразобранные задачи",
        ]
    ):
        return ParsedIntent(Intent.SHOW_INBOX_TASKS, 0.9, {}, raw)

    if any(k in lowered for k in ["добавь задачу", "задача"]):
        title = _strip_keywords(stripped, ["добавь задачу", "задача"]).strip()
        if title:
            return ParsedIntent(Intent.CREATE_TASK, 0.9, {"title": title}, raw)
        return ParsedIntent(Intent.CREATE_TASK, 0.6, {"title": title}, raw)

    if "план" in lowered:
        minutes_match = re.search(r"\bплан(?:\s+на)?\s+(\d{1,4})\b", lowered)
        minutes = int(minutes_match.group(1)) if minutes_match else None
        target = _strip_keywords(stripped, ["план", "на"]).strip()
        if minutes is not None:
            target = re.sub(rf"\b{minutes}\b", "", target).strip()
        if minutes and target:
            return ParsedIntent(Intent.PLAN_TASK, 0.9, {"minutes": minutes, "target": target}, raw)
        return ParsedIntent(Intent.PLAN_TASK, 0.6, {"minutes": minutes, "target": target}, raw)

    if any(k in lowered for k in ["начал", "старт"]):
        target = _strip_keywords(stripped, ["начал", "старт"]).strip()
        confidence = 0.9 if target else 0.6
        return ParsedIntent(Intent.START_WORK, confidence, {"target": target}, raw)

    if any(k in lowered for k in ["закончил", "стоп"]):
        target = _strip_keywords(stripped, ["закончил", "стоп"]).strip()
        confidence = 0.9 if target else 0.6
        return ParsedIntent(Intent.STOP_WORK, confidence, {"target": target}, raw)

    if any(k in lowered for k in ["экспорт", "таблица", "excel"]):
        return ParsedIntent(Intent.EXPORT, 0.9, {}, raw)

    return ParsedIntent(Intent.NONE, 0.0, {}, raw)
