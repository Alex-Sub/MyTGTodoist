from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import re
from dataclasses import dataclass, field
from typing import Any, Dict

import yaml


TEMPORAL_INTENTS = {
    "meeting.update",
    "meeting_update",
    "meeting.create",
    "meeting_create",
    "timeblock.create",
    "timeblock_create",
    "create_timeblock",
    "block.create",
    "create_block",
    "schedule_block",
    "place_block",
    "calendar_block",
    "schedule_meeting",
    "schedule_call",
    "create_meeting",
    "meeting_create",
    "create_event",
    "meeting.move",
    "move_meeting",
    "reschedule_meeting",
}
INBOX_INTENTS = {
    "inbox.create",
    "create_inbox",
    "task.create",
    "create_task",
    "todo.create",
    "todo.create_generic",
}
TEMPORAL_FIELD_KEYS = {
    "start_at",
    "start_time",
    "starts_at",
    "scheduled_at",
    "datetime",
    "duration_minutes",
    "duration_min",
    "duration_mins",
    "duration",
}
TEMPORAL_DURATION_FIELDS = {
    "duration_minutes",
    "duration_min",
    "duration_mins",
    "duration",
}
_DATE_KEYWORDS_RU = {"сегодня", "завтра", "послезавтра"}
_DATE_KEYWORDS_EN = {"today", "tomorrow"}
_ALIAS_CANON_FILE = "intent_aliases_v1.yml"


@dataclass(frozen=True)
class TemporalExecutionDecision:
    is_temporal: bool
    command: Dict[str, Any]
    missing_field: str = ""
    question: str = ""


@dataclass(frozen=True)
class ClarificationContinuationDecision:
    accepted: bool
    normalized_value: Any = None
    field_name: str = "clarification_value"
    reason: str = ""
    should_reask: bool = False
    command: Dict[str, Any] = field(default_factory=dict)


def is_temporal_text(text: str) -> bool:
    s = str(text or "").strip().lower()
    if not s:
        return False
    if re.search(r"\b(сегодня|завтра|послезавтра|утром|днем|дн[её]м|вечером|ночью|через)\b", s):
        return True
    if re.search(r"\b(в|к)\s*([01]?\d|2[0-3])([:.][0-5]\d)?\b", s):
        return True
    if re.search(r"\bна\s+\d+\s*(мин|минут|час|часа|часов)\b", s):
        return True
    return False


def is_block_action_text(text: str) -> bool:
    s = str(text or "").strip().lower()
    if not s:
        return False
    has_block_token = bool(re.search(r"\b(блок|таймблок|timeblock)\b", s))
    if not has_block_token:
        return False
    has_action_verb = bool(re.search(r"\b(постав|запланир|созда|добав|сдела)\w*\b", s))
    return has_action_verb


def _alias_file_path() -> Path:
    return Path(__file__).resolve().parents[4] / "canon" / _ALIAS_CANON_FILE


@lru_cache(maxsize=1)
def _load_intent_aliases() -> Dict[str, Any]:
    path = _alias_file_path()
    if not path.exists():
        return {}
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _compile_alias_pattern(patterns: list[str]) -> str:
    body = "|".join([str(p).strip() for p in patterns if str(p).strip()])
    if not body:
        return ""
    return rf"\b({body})\w*\b"


def _normalize_alias_text(text: str) -> str:
    s = str(text or "").strip().lower().replace("ё", "е")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def match_alias(text: str, intent_name: str) -> bool:
    s = _normalize_alias_text(text)
    if not s:
        return False
    aliases = _load_intent_aliases()
    intents = aliases.get("intents") if isinstance(aliases.get("intents"), dict) else {}
    intent_alias = intents.get(str(intent_name or "").strip())
    if not isinstance(intent_alias, dict):
        return False

    phrases = intent_alias.get("phrases")
    subject_patterns = intent_alias.get("subject_patterns")
    action_patterns = intent_alias.get("action_patterns")
    phrases = [str(p).strip().lower() for p in phrases] if isinstance(phrases, list) else []
    subject_patterns = subject_patterns if isinstance(subject_patterns, list) else []
    action_patterns = action_patterns if isinstance(action_patterns, list) else []

    for phrase in phrases:
        if phrase and phrase in s:
            return True

    subject_regex = _compile_alias_pattern([str(p) for p in subject_patterns])
    action_regex = _compile_alias_pattern([str(p) for p in action_patterns])
    has_temporal_subject = bool(subject_regex and re.search(subject_regex, s))
    has_temporal_action = bool(action_regex and re.search(action_regex, s))

    has_temporal_markers = is_temporal_text(s)
    if has_temporal_markers:
        return has_temporal_subject or has_temporal_action
    return has_temporal_subject and has_temporal_action


def is_obvious_temporal_command_text(text: str) -> bool:
    return (
        match_alias(text, "meeting.update")
        or match_alias(text, "meeting.create")
        or match_alias(text, "timeblock.create")
    )


def _is_explicit_meeting_update_request_text(text: str) -> bool:
    s = _normalize_alias_text(text)
    if not s:
        return False
    if re.search(r"\b(перенес|сдвин|поменя|подвин)\w*\b", s):
        return True
    if re.search(r"\bдавай\s+на\s+\d{1,2}([:.]\d{2})?\b", s):
        return True
    return False


def is_temporal_intent(intent: str) -> bool:
    return str(intent or "").strip().lower() in TEMPORAL_INTENTS


def is_inbox_intent(intent: str) -> bool:
    return str(intent or "").strip().lower() in INBOX_INTENTS


def has_implicit_default_time(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    s = str(value).strip().lower()
    if s in ("06:00", "6:00"):
        return True
    if "t06:00" in s or " 06:00" in s:
        return True
    return False


def has_explicit_user_time(text: str) -> bool:
    s = str(text or "").strip().lower()
    if not s:
        return False
    if re.search(r"\b(в|к)\s*([01]?\d|2[0-3])([:.][0-5]\d)?\b", s):
        return True
    if re.search(r"(^|[t\s])([01]?\d|2[0-3]):[0-5]\d\b", s):
        return True
    return False


def has_explicit_user_date(text: str) -> bool:
    s = str(text or "").strip().lower()
    if not s:
        return False
    if re.search(r"\b(сегодня|завтра|послезавтра|вчера|понедельник|вторник|среда|четверг|пятница|суббота|воскресенье)\b", s):
        return True
    if re.search(r"\d{4}-\d{2}-\d{2}", s):
        return True
    if re.search(r"\b\d{1,2}[./-]\d{1,2}([./-]\d{2,4})?\b", s):
        return True
    if re.search(
        r"\b\d{1,2}\s+(?:январ[ьяе]?|феврал[ьяе]?|март[ае]?|апрел[ьяе]?|ма[йяе]|июн[ьяе]?|июл[ьяе]?|август[ае]?|сентябр[ьяе]?|октябр[ьяе]?|ноябр[ьяе]?|декабр[ьяе]?)(?:\s+\d{4})?\b",
        s.replace("ё", "е"),
    ):
        return True
    return False


def temporal_question(missing_field: str) -> str:
    normalized = str(missing_field or "").strip()
    if normalized == "start_at_date":
        return "На какую дату запланировать встречу?"
    if normalized == "start_at_time":
        return "Во сколько запланировать встречу?"
    if normalized == "duration_minutes":
        return "На сколько минут поставить блок?"
    return "На какую дату запланировать встречу?"


def normalize_clarification_field_name(field_name: str) -> str:
    f = str(field_name or "").strip()
    if not f:
        return "clarification_value"
    if f.startswith("entities."):
        f = f[len("entities.") :]
    if f in ("start_at_date", "start_date", "date"):
        return "start_at_date"
    if f in ("start_at_time", "time", "start_time_text"):
        return "start_at_time"
    if f in ("awaiting_time", "await_time", "time_awaiting"):
        return "start_at_time"
    if f in TEMPORAL_DURATION_FIELDS:
        return "duration_minutes"
    if f in ("start_time", "starts_at", "scheduled_at", "datetime", "start_at_text"):
        return "start_at"
    return f


def _command_value_for_satisfaction(command: Dict[str, Any], *keys: str) -> Any:
    cmd = command if isinstance(command, dict) else {}
    entities = cmd.get("entities")
    entities = entities if isinstance(entities, dict) else {}
    for key in keys:
        if key in cmd and cmd.get(key) is not None:
            return cmd.get(key)
        if key in entities and entities.get(key) is not None:
            return entities.get(key)
    return None


def _parse_start_at_date(raw_value: Any) -> str | None:
    s = str(raw_value or "").strip().lower()
    if not s:
        return None
    if s in _DATE_KEYWORDS_RU or s in _DATE_KEYWORDS_EN:
        return s
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    if re.fullmatch(r"\d{1,2}[./-]\d{1,2}([./-]\d{2,4})?", s):
        return s
    if has_explicit_user_date(s):
        return s
    return None


def _looks_like_full_datetime(raw_value: Any) -> bool:
    s = str(raw_value or "").strip().lower()
    if not s:
        return False
    # Typical machine-normalized datetime forms:
    # 2026-04-10T12:00:00+03:00, 2026-04-10 12:00, 2026-04-10T12:00
    if re.search(r"\d{4}-\d{2}-\d{2}[t\s]\d{1,2}[:.]\d{2}", s):
        return True
    return False


def _looks_like_date_only_value(raw_value: Any) -> bool:
    s = str(raw_value or "").strip().lower()
    if not s:
        return False
    if _looks_like_full_datetime(s):
        return False
    # Date-like token without time component (e.g. 2026-04-09, 09.04, tomorrow).
    if _parse_start_at_date(s) is None:
        return False
    if _parse_start_at_time(s) is not None:
        return False
    return True


def _is_ambiguous_dotted_time_token(raw_value: Any) -> bool:
    s = str(raw_value or "").strip().lower()
    if not s:
        return False
    # Ambiguous artifact: parser may output time as HH.MM (e.g. 12.00),
    # which should not auto-confirm date when user did not provide one.
    return re.fullmatch(r"\d{1,2}\.\d{2}", s) is not None


def _parse_hour_only_time(raw_value: str) -> str | None:
    s = str(raw_value or "").strip()
    if not re.fullmatch(r"\d{1,2}", s):
        return None
    try:
        hour = int(s)
    except Exception:
        return None
    if hour < 0 or hour > 23:
        return None
    return f"{hour:02d}:00"


def _parse_start_at_time(raw_value: Any) -> str | None:
    s = str(raw_value or "").strip().lower()
    if not s:
        return None
    direct = _parse_hour_only_time(s)
    if direct is not None:
        return direct
    m = re.fullmatch(r"(?:в|к)\s*(\d{1,2})(?:[:.](\d{2}))?", s)
    if m:
        hour = int(m.group(1))
        minute = int(m.group(2) or "00")
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return f"{hour:02d}:{minute:02d}"
        return None
    m = re.fullmatch(r"(\d{1,2})[:.](\d{2})", s)
    if m:
        hour = int(m.group(1))
        minute = int(m.group(2))
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return f"{hour:02d}:{minute:02d}"
        return None
    if has_explicit_user_time(s):
        return s
    return None


def is_missing_field_already_satisfied(command: Dict[str, Any], field_name: str) -> bool:
    cmd = command if isinstance(command, dict) else {}
    normalized = normalize_clarification_field_name(field_name)

    if normalized == "start_at_date":
        direct_date = _command_value_for_satisfaction(cmd, "start_at_date", "start_date", "date")
        if _parse_start_at_date(direct_date) is not None:
            return True
        start_value = _command_value_for_satisfaction(cmd, "start_at", "start_time", "starts_at", "scheduled_at", "datetime")
        return _parse_start_at_date(start_value) is not None

    if normalized == "start_at_time":
        direct_time = _command_value_for_satisfaction(cmd, "start_at_time", "time", "start_time_text")
        if _parse_start_at_time(direct_time) is not None:
            return True
        start_value = _command_value_for_satisfaction(cmd, "start_at", "start_time", "starts_at", "scheduled_at", "datetime")
        if has_implicit_default_time(start_value):
            return False
        return _parse_start_at_time(start_value) is not None

    if normalized == "duration_minutes":
        value = _command_value_for_satisfaction(cmd, "duration_minutes", "duration_min", "duration_mins", "duration")
        if value is None:
            return False
        if isinstance(value, bool):
            return False
        if isinstance(value, int):
            return value > 0
        if isinstance(value, float):
            return value > 0 and value.is_integer()
        raw = str(value).strip()
        if not raw or not re.fullmatch(r"[+-]?\d+", raw):
            return False
        try:
            return int(raw) > 0
        except Exception:
            return False

    value = _command_value_for_satisfaction(cmd, normalized)
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    return True


def _set_missing_field(payload: Dict[str, Any], missing_field: str, value: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(payload if isinstance(payload, dict) else {})
    field_name = normalize_clarification_field_name(missing_field)

    def _extract_date_from_start(raw_start: Any) -> str:
        start_text = str(raw_start or "").strip()
        if not start_text:
            return ""
        first_token = start_text.split()[0]
        parsed_date = _parse_start_at_date(first_token)
        return str(parsed_date or "").strip()

    def _extract_time_from_start(raw_start: Any) -> str:
        start_text = str(raw_start or "").strip()
        if not start_text:
            return ""
        tokens = start_text.split()
        for token in tokens[1:]:
            parsed_time = _parse_start_at_time(token)
            if parsed_time is not None:
                return parsed_time
        parsed_time = _parse_start_at_time(start_text)
        return str(parsed_time or "").strip()

    if field_name in ("start_at_date", "start_at_time"):
        current_start = str(out.get("start_at") or "").strip()
        incoming = str(value or "").strip()
        entities = out.get("entities")
        entities = dict(entities) if isinstance(entities, dict) else {}
        date_value = str(out.get("start_at_date") or entities.get("start_at_date") or _extract_date_from_start(current_start) or "").strip()
        time_value = str(out.get("start_at_time") or entities.get("start_at_time") or _extract_time_from_start(current_start) or "").strip()
        if field_name == "start_at_date":
            out["start_at_date"] = incoming
            entities["start_at_date"] = incoming
            date_value = incoming
        else:
            out["start_at_time"] = incoming
            entities["start_at_time"] = incoming
            time_value = incoming
        if date_value and time_value:
            out["start_at"] = f"{date_value} {time_value}".strip()
        elif date_value:
            out["start_at"] = date_value
        elif time_value:
            out["start_at"] = time_value
        else:
            out["start_at"] = incoming
        out["entities"] = entities
        return out
    parts = [p for p in field_name.split(".") if p]
    if not parts:
        out["clarification_value"] = value
        return out

    cur: Dict[str, Any] = out
    for p in parts[:-1]:
        nested = cur.get(p)
        if not isinstance(nested, dict):
            nested = {}
            cur[p] = nested
        cur = nested
    cur[parts[-1]] = value
    return out


def _coerce_generic_value(raw_value: str) -> Any:
    v = str(raw_value or "").strip()
    if re.fullmatch(r"[+-]?\d+", v):
        try:
            return int(v)
        except Exception:
            return v
    if re.fullmatch(r"[+-]?\d+[.,]\d+", v):
        try:
            return float(v.replace(",", "."))
        except Exception:
            return v
    return v


def _parse_duration_minutes(raw_value: str) -> Any:
    s = str(raw_value or "").strip()
    if not re.fullmatch(r"[+-]?\d+", s):
        return None
    try:
        val = int(s)
    except Exception:
        return None
    if val <= 0:
        return None
    return val


def normalize_clarification_value(field_name: str, raw_value: str) -> ClarificationContinuationDecision:
    normalized_field = normalize_clarification_field_name(field_name)
    if normalized_field == "start_at_date":
        normalized_date = _parse_start_at_date(raw_value)
        if normalized_date is None:
            return ClarificationContinuationDecision(
                accepted=False,
                normalized_value=None,
                field_name="start_at_date",
                reason="invalid_start_at_date",
                should_reask=True,
                command={},
            )
        return ClarificationContinuationDecision(
            accepted=True,
            normalized_value=normalized_date,
            field_name="start_at_date",
            reason="ok",
            should_reask=False,
            command={},
        )
    if normalized_field == "start_at_time":
        normalized_time = _parse_start_at_time(raw_value)
        if normalized_time is None:
            return ClarificationContinuationDecision(
                accepted=False,
                normalized_value=None,
                field_name="start_at_time",
                reason="invalid_start_at_time",
                should_reask=True,
                command={},
            )
        return ClarificationContinuationDecision(
            accepted=True,
            normalized_value=normalized_time,
            field_name="start_at_time",
            reason="ok",
            should_reask=False,
            command={},
        )
    if normalized_field == "duration_minutes":
        parsed = _parse_duration_minutes(raw_value)
        if parsed is None:
            return ClarificationContinuationDecision(
                accepted=False,
                normalized_value=None,
                field_name="duration_minutes",
                reason="invalid_duration_minutes",
                should_reask=True,
                command={},
            )
        return ClarificationContinuationDecision(
            accepted=True,
            normalized_value=parsed,
            field_name="duration_minutes",
            reason="ok",
            should_reask=False,
            command={},
        )

    return ClarificationContinuationDecision(
        accepted=True,
        normalized_value=_coerce_generic_value(raw_value),
        field_name=normalized_field,
        reason="ok",
        should_reask=False,
        command={},
    )


def resolve_clarification_continuation(
    field_name: str,
    raw_value: str,
    payload: Dict[str, Any],
) -> ClarificationContinuationDecision:
    out = normalize_clarification_value(field_name, raw_value)
    cmd = dict(payload if isinstance(payload, dict) else {})
    if not out.accepted:
        return ClarificationContinuationDecision(
            accepted=False,
            normalized_value=out.normalized_value,
            field_name=out.field_name,
            reason=out.reason,
            should_reask=True,
            command=cmd,
        )

    updated = _set_missing_field(cmd, out.field_name, out.normalized_value)
    return ClarificationContinuationDecision(
        accepted=True,
        normalized_value=out.normalized_value,
        field_name=out.field_name,
        reason="ok",
        should_reask=False,
        command=updated,
    )


def _intent_name(command: Dict[str, Any]) -> str:
    return str(command.get("intent") or "").strip().lower()


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _is_positive_duration(value: Any) -> bool:
    if _is_blank(value):
        return False
    if isinstance(value, (int, float)):
        return float(value) > 0.0
    try:
        return float(str(value).replace(",", ".")) > 0.0
    except Exception:
        return False


def has_required_temporal_fields(command: Dict[str, Any], source_text: str) -> Dict[str, Any]:
    cmd: Dict[str, Any] = dict(command if isinstance(command, dict) else {})
    entities = cmd.get("entities")
    entities = entities if isinstance(entities, dict) else {}
    text = str(source_text or "").strip()

    if text and "__source_text" not in cmd:
        cmd["__source_text"] = text

    def _cmd_or_entity(*keys: str) -> Any:
        for key in keys:
            if key in cmd and cmd.get(key) is not None:
                return cmd.get(key)
            if key in entities and entities.get(key) is not None:
                return entities.get(key)
        return None

    start_value = (
        _cmd_or_entity("start_at")
        or _cmd_or_entity("start_time")
        or _cmd_or_entity("starts_at")
        or _cmd_or_entity("scheduled_at")
        or _cmd_or_entity("datetime")
    )
    duration_value = (
        _cmd_or_entity("duration_minutes")
        or _cmd_or_entity("duration_min")
        or _cmd_or_entity("duration_mins")
        or _cmd_or_entity("duration")
    )
    direct_date_value = _cmd_or_entity("start_at_date", "start_date", "date")
    direct_time_value = _cmd_or_entity("start_at_time", "time", "start_time_text")
    source_for_detection = str(cmd.get("__source_text") or text or entities.get("text") or "")
    explicit_time = has_explicit_user_time(source_for_detection)
    explicit_date = has_explicit_user_date(source_for_detection)
    explicit_duration = bool(re.search(r"\bна\s+\d+\s*(мин|минут|минута|минуты|час|часа|часов)\b", source_for_detection.lower()))

    start_date_from_direct_field_raw = _parse_start_at_date(direct_date_value) is not None
    start_date_from_start_value = _parse_start_at_date(start_value) is not None
    # Guard against parser-inferred dates: if user text has no explicit date,
    # direct date slots are not treated as user-confirmed.
    start_date_from_direct_field = start_date_from_direct_field_raw and explicit_date
    inferred_datetime_only_date = _looks_like_full_datetime(start_value) and not explicit_date and not start_date_from_direct_field_raw
    inferred_date_only_start_value = _looks_like_date_only_value(start_value) and not explicit_date and not start_date_from_direct_field_raw
    inferred_ambiguous_dotted_time = (
        _is_ambiguous_dotted_time_token(start_value)
        and _parse_start_at_time(start_value) is not None
        and not explicit_date
    )

    start_has_date = (
        start_date_from_direct_field
        or (
            start_date_from_start_value
            and not inferred_datetime_only_date
            and not inferred_date_only_start_value
            and not inferred_ambiguous_dotted_time
        )
        or explicit_date
    )
    start_has_time = (
        _parse_start_at_time(direct_time_value) is not None
        or _parse_start_at_time(start_value) is not None
        or explicit_time
    )
    if has_implicit_default_time(start_value) and not explicit_time and _parse_start_at_time(direct_time_value) is None:
        start_has_time = False
    duration_missing = not _is_positive_duration(duration_value)
    temporal_update_intent = _intent_name(cmd) in {"meeting.update", "meeting_update", "reschedule_meeting"}
    if temporal_update_intent:
        update_changes_marker = _cmd_or_entity("meeting_update_has_changes")
        has_changes_marker = str(update_changes_marker).strip().lower() in {"1", "true", "yes", "да"}
        has_any_update = bool(has_changes_marker or explicit_date or explicit_time or explicit_duration)
        if not has_any_update:
            return {"ok": False, "missing_field": "start_at_time", "command": cmd}
        return {"ok": True, "missing_field": "", "command": cmd}

    if not start_has_date:
        return {"ok": False, "missing_field": "start_at_date", "command": cmd}
    if not start_has_time:
        return {"ok": False, "missing_field": "start_at_time", "command": cmd}
    if duration_missing:
        return {"ok": False, "missing_field": "duration_minutes", "command": cmd}
    return {"ok": True, "missing_field": "", "command": cmd}


def resolve_temporal_execution_decision(command: Dict[str, Any], source_text: str) -> TemporalExecutionDecision:
    cmd: Dict[str, Any] = dict(command if isinstance(command, dict) else {})
    text = str(source_text or "").strip()
    intent = _intent_name(cmd)
    if intent in {"", "unknown"} and not is_temporal_text(text) and match_alias(text, "task.create"):
        cmd["intent"] = "task.create"
        intent = "task.create"

    temporal_by_intent = is_temporal_intent(intent)
    temporal_by_fields = any((k in cmd) for k in TEMPORAL_FIELD_KEYS)
    temporal_by_text = is_temporal_text(text)
    block_action_by_text = is_block_action_text(text)
    obvious_temporal_unknown = intent in {"", "unknown"} and is_obvious_temporal_command_text(text)
    inbox_intent = is_inbox_intent(intent)
    temporal_candidate = (
        temporal_by_intent
        or temporal_by_fields
        or block_action_by_text
        or (temporal_by_text and inbox_intent)
        or obvious_temporal_unknown
    )

    if not temporal_candidate:
        return TemporalExecutionDecision(is_temporal=False, command=cmd)

    if intent in {"", "unknown"}:
        if match_alias(text, "meeting.update"):
            cmd["intent"] = "meeting.update"
        elif match_alias(text, "meeting.create"):
            cmd["intent"] = "meeting.create"
        elif match_alias(text, "timeblock.create"):
            cmd["intent"] = "create_timeblock"
        intent = _intent_name(cmd)

    if (
        match_alias(text, "meeting.update")
        and _is_explicit_meeting_update_request_text(text)
        and intent in {"schedule_meeting", "create_meeting", "meeting.create"}
    ):
        cmd["intent"] = "meeting.update"

    if intent in {"schedule_block", "create_block", "block.create", "place_block"} or (
        block_action_by_text and intent in {"", "unknown"}
    ):
        cmd["intent"] = "create_timeblock"

    if inbox_intent:
        cmd["intent"] = "create_timeblock"

    if intent == "schedule_call":
        cmd["intent"] = "meeting.create"

    required = has_required_temporal_fields(cmd, text)
    cmd = required["command"] if isinstance(required.get("command"), dict) else cmd
    if not bool(required.get("ok")):
        missing_field = str(required.get("missing_field") or "")
        return TemporalExecutionDecision(
            is_temporal=True,
            command=cmd,
            missing_field=missing_field,
            question=temporal_question(missing_field),
        )

    return TemporalExecutionDecision(is_temporal=True, command=cmd)
