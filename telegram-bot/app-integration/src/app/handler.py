from __future__ import annotations

import logging
import os
import re
import json
import urllib.request
from datetime import date
from typing import Any, Dict, Optional

from src.decision.clarification_rules import (
    normalize_clarification_field_name,
    resolve_clarification_continuation,
)
from src.decision.temporal_rules import (
    has_required_temporal_fields,
    has_explicit_user_date,
    has_explicit_user_time,
    resolve_temporal_execution_decision,
    temporal_question,
)
from src.decision.decision_engine import is_missing_field_already_satisfied
from src.core.failures import build_one_clarifying_question, map_failure_to_user_message
from src.app.voice_pipeline import process_voice_pipeline
from src.reliability.idempotency import resolve_idempotency_key
from src.reliability.local_db import LocalMainDb

LOG = logging.getLogger(__name__)

_PAST_DATE_CONFIRM_FIELD = "start_at_date_past_confirm"
_TEMPORAL_CONFIRM_FIELD = "temporal_commit_confirm"
_TEMPORAL_EDIT_FIELD = "temporal_edit_field"
_TEMPORAL_EDIT_ACTIVE_FIELD_KEY = "__temporal_edit_active_field"
_TASK_CREATE_CONFIRM_FIELD = "task_create_confirm"
_TASK_CREATE_EDIT_FIELD = "task_create_edit"
_MEETING_UPDATE_TARGET_CONFIRM_FIELD = "awaiting_target_confirm"
_MEETING_UPDATE_TARGET_IDENTIFICATION_FIELD = "awaiting_target_identification"
_MEETING_UPDATE_TARGET_SELECT_FIELD = "meeting_update_target_select"
_MEETING_UPDATE_TARGET_REFINE_FIELD = "meeting_update_target_refine"
_MEETING_UPDATE_FINAL_CONFIRM_FIELD = "awaiting_final_confirm"
_MEETING_UPDATE_STAGE_KEY = "__meeting_update_stage"
_MEETING_COMMENT_TARGET_CONFIRM_FIELD = "meeting_comment_target_confirm"
_MEETING_COMMENT_TARGET_SELECT_FIELD = "meeting_comment_target_select"
_MEETING_COMMENT_TEXT_FIELD = "meeting_comment_text"
_MEETING_COMMENT_FINAL_CONFIRM_FIELD = "meeting_comment_final_confirm"
_TASK_COMMENT_TARGET_CONFIRM_FIELD = "task_comment_target_confirm"
_TASK_COMMENT_TARGET_SELECT_FIELD = "task_comment_target_select"
_TASK_COMMENT_TEXT_FIELD = "task_comment_text"
_TASK_COMMENT_FINAL_CONFIRM_FIELD = "task_comment_final_confirm"
_DEFAULT_MEETING_DURATION_MINUTES = int(os.getenv("DEFAULT_MEETING_DURATION_MINUTES", "30"))
_MEETING_UPDATE_SHORTLIST_MAX = 3
_GLOBAL_CANCEL_PHRASES = {
    "отмена",
    "стоп",
    "не надо",
    "отбой",
    "все закончили",
    "всё закончили",
}

_RU_MONTHS = {
    "январ": 1,
    "феврал": 2,
    "март": 3,
    "апрел": 4,
    "ма": 5,
    "июн": 6,
    "июл": 7,
    "август": 8,
    "сентябр": 9,
    "октябр": 10,
    "ноябр": 11,
    "декабр": 12,
}

_MEETING_KIND_VARIANTS = {
    "встреча": ("встреча", "встречу", "встрече", "встречи"),
    "собрание": ("собрание", "собрания", "собранию"),
    "созвон": ("созвон", "созвоне", "созвона", "звонок", "колл"),
    "мероприятие": ("мероприятие", "мероприятия", "ивент", "событие"),
}

_TEMPORAL_INTENT_ALIAS_TO_CANON = {
    "schedule_call": "meeting.create",
    "schedule_meeting": "meeting.create",
    "create_meeting": "meeting.create",
    "meeting_create": "meeting.create",
}


def _try_build_date(year: int, month: int, day: int) -> Optional[date]:
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _parse_human_date_to_iso(
    raw_text: Any,
    *,
    today: Optional[date] = None,
    parser_source: str = "handler._parse_human_date_to_iso",
) -> Optional[str]:
    s = str(raw_text or "").strip().lower().replace("ё", "е")
    s = re.sub(r"[,\u00a0]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None
    base = today or date.today()

    if s in {"сегодня", "today"}:
        normalized = base.isoformat()
        LOG.info("temporal_date_parsed", extra={"raw_date_text": s, "normalized_date_result": normalized, "parser_source": parser_source})
        return normalized
    if s in {"завтра", "tomorrow"}:
        normalized = date.fromordinal(base.toordinal() + 1).isoformat()
        LOG.info("temporal_date_parsed", extra={"raw_date_text": s, "normalized_date_result": normalized, "parser_source": parser_source})
        return normalized
    if s == "послезавтра":
        normalized = date.fromordinal(base.toordinal() + 2).isoformat()
        LOG.info("temporal_date_parsed", extra={"raw_date_text": s, "normalized_date_result": normalized, "parser_source": parser_source})
        return normalized

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        year, month, day = [int(x) for x in s.split("-")]
        parsed = _try_build_date(year, month, day)
        normalized = parsed.isoformat() if parsed else None
        LOG.info("temporal_date_parsed", extra={"raw_date_text": s, "normalized_date_result": normalized or "", "parser_source": parser_source})
        return normalized

    m = re.fullmatch(r"(\d{1,2})[./-](\d{1,2})(?:[./-](\d{4}))?", s)
    if m:
        day = int(m.group(1))
        month = int(m.group(2))
        # day+month without year => current year
        year = int(m.group(3)) if m.group(3) else base.year
        parsed = _try_build_date(year, month, day)
        normalized = parsed.isoformat() if parsed else None
        LOG.info("temporal_date_parsed", extra={"raw_date_text": s, "normalized_date_result": normalized or "", "parser_source": parser_source})
        return normalized

    m = re.fullmatch(r"(\d{1,2})\s+([а-яё]+)(?:\s+(\d{4}))?", s)
    if m:
        day = int(m.group(1))
        month_token = str(m.group(2) or "").strip().rstrip(".")
        month = None
        for stem, month_idx in _RU_MONTHS.items():
            if month_token.startswith(stem):
                month = month_idx
                break
        if month is None:
            return None
        # day+month without year => current year
        year = int(m.group(3)) if m.group(3) else base.year
        parsed = _try_build_date(year, month, day)
        normalized = parsed.isoformat() if parsed else None
        LOG.info("temporal_date_parsed", extra={"raw_date_text": s, "normalized_date_result": normalized or "", "parser_source": parser_source})
        return normalized

    # Do not infer a full date from bare day token ("dd"), this avoids
    # accidental year coercion/fallbacks in ambiguous user input.
    if re.fullmatch(r"\d{1,2}", s):
        return None
    return None


def _normalize_clarification_date_to_iso(
    raw_text: str,
    *,
    today: Optional[date] = None,
    prefer_future_for_ambiguous: bool = True,
) -> Optional[str]:
    return _parse_human_date_to_iso(
        raw_text,
        today=today,
        parser_source="_normalize_clarification_date_to_iso",
    )


def _inject_start_at_date_into_command(command: Dict[str, Any], iso_date: str) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    out["start_at_date"] = iso_date
    out["start_at"] = str(iso_date)
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    entities["start_at_date"] = iso_date
    entities["start_at"] = str(iso_date)
    out["entities"] = entities
    return out


def _extract_explicit_date_from_text(raw_text: Any, *, today: Optional[date] = None) -> Optional[str]:
    s = str(raw_text or "").strip().lower().replace("ё", "е")
    s = re.sub(r"[,\u00a0]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None
    base = today or date.today()

    for pattern in (
        r"\b(сегодня|завтра|послезавтра)\b",
        r"\b\d{4}-\d{2}-\d{2}\b",
        r"\b\d{1,2}[./-]\d{1,2}(?:[./-]\d{4})?\b",
        r"\b\d{1,2}\s+[а-яё]+(?:\s+\d{4})?\b",
    ):
        m = re.search(pattern, s)
        if not m:
            continue
        normalized = _parse_human_date_to_iso(
            m.group(0),
            today=base,
            parser_source="_extract_explicit_date_from_text",
        )
        if normalized:
            return normalized
    return None


def _materialize_temporal_entities(
    command: Dict[str, Any],
    *,
    source_text: str,
    request_id: str,
) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}

    explicit_date = _extract_explicit_date_from_text(source_text)
    command_date_raw = str(_command_value(out, "start_at_date", "start_date", "date") or "").strip()
    command_date = _normalize_clarification_date_to_iso(command_date_raw, prefer_future_for_ambiguous=False) or command_date_raw
    entity_date = str(_command_value({"entities": entities}, "start_at_date", "start_date", "date") or "").strip()
    materialized_date = explicit_date or command_date
    entities_before = entity_date
    if materialized_date and (not entity_date or (explicit_date and entity_date != explicit_date)):
        entities["start_at_date"] = materialized_date
        out["entities"] = entities
        out["start_at_date"] = materialized_date
        source_before = str(out.get("__source_text") or source_text or "").strip()
        if source_before and not has_explicit_user_date(source_before):
            out["__source_text"] = f"{source_before} {materialized_date}".strip()
    time_value = _normalize_time_hhmm_local(_command_value(out, "start_at_time", "start_time", "time"))
    if time_value:
        out["start_at_time"] = time_value
        entities["start_at_time"] = time_value
    if materialized_date and time_value:
        out["start_at"] = f"{materialized_date}T{time_value}"
        entities["start_at"] = f"{materialized_date}T{time_value}"
    out["entities"] = entities
    entities_after = str(_command_value({"entities": entities}, "start_at_date", "start_date", "date") or "").strip()
    LOG.info(
        "temporal_entities_materialized",
        extra={
            "request_id": request_id,
            "flow_id": request_id,
            "meeting_kind": str(_command_value(out, "meeting_kind", "event_kind") or ""),
            "source_text_has_date": bool(explicit_date),
            "parsed_date": str(materialized_date or ""),
            "start_at_time": str(time_value or ""),
            "start_at_value": str(_command_value(out, "start_at") or ""),
            "entities.start_at_date_before": entities_before,
            "entities.start_at_date_after": entities_after,
        },
    )
    LOG.info(
        "meeting_create_date_materialization",
        extra={
            "request_id": request_id,
            "flow_id": request_id,
            "meeting_kind": str(_command_value(out, "meeting_kind", "event_kind") or ""),
            "parsed_date": str(materialized_date or ""),
            "entities.start_at_date_before": entities_before,
            "entities.start_at_date_after": entities_after,
        },
    )
    return out


def _materialize_temporal_date_in_entities(
    command: Dict[str, Any],
    *,
    source_text: str,
    request_id: str,
) -> Dict[str, Any]:
    # Backward-compatible wrapper: canonical materialization path is _materialize_temporal_entities.
    return _materialize_temporal_entities(
        command,
        source_text=source_text,
        request_id=request_id,
    )


def _inject_start_at_time_into_command(command: Dict[str, Any], hhmm_time: str) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    time_value = _normalize_time_hhmm_local(hhmm_time)
    if not time_value:
        return out
    out["start_at_time"] = time_value
    out["start_time"] = time_value
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    entities["start_at_time"] = time_value
    entities["start_time"] = time_value
    date_iso = _extract_start_at_date_iso(out)
    if date_iso:
        out["start_at"] = f"{date_iso}T{time_value}"
        entities["start_at"] = f"{date_iso}T{time_value}"
    out["entities"] = entities
    return out


def _normalize_start_date_fields(command: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}

    def _normalize_date_token(raw: Any) -> Optional[str]:
        return _normalize_clarification_date_to_iso(
            str(raw or ""),
            prefer_future_for_ambiguous=False,
        )

    normalized_iso = None
    for key in ("start_at_date", "start_date", "date"):
        if key in out:
            iso = _normalize_date_token(out.get(key))
            if iso:
                out[key] = iso
                normalized_iso = normalized_iso or iso
        if key in entities:
            iso = _normalize_date_token(entities.get(key))
            if iso:
                entities[key] = iso
                normalized_iso = normalized_iso or iso

    def _normalize_start_value(raw: Any) -> tuple[Optional[str], str]:
        s = str(raw or "").strip()
        if not s:
            return None, s
        first_token = s.split()[0]
        first_token = first_token.split("T")[0]
        date_iso = _normalize_date_token(first_token)
        if not date_iso:
            return None, s
        remainder = s[len(s.split()[0]) :].strip()
        normalized_start = f"{date_iso} {remainder}".strip()
        return date_iso, normalized_start

    for container in (out, entities):
        date_iso, normalized_start = _normalize_start_value(container.get("start_at"))
        if date_iso:
            container["start_at"] = normalized_start
            normalized_iso = normalized_iso or date_iso

    if normalized_iso:
        out["start_at_date"] = str(out.get("start_at_date") or normalized_iso)
        entities["start_at_date"] = str(entities.get("start_at_date") or normalized_iso)
    out["entities"] = entities
    return out


def _hydrate_temporal_datetime_fields(command: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}

    datetime_raw = str(_command_value(out, "datetime", "when") or "").strip()
    if not datetime_raw:
        out["entities"] = entities
        return out

    date_raw = str(_command_value(out, "start_at_date", "start_date", "date") or "").strip()
    time_raw = str(_command_value(out, "start_at_time", "start_time", "time") or "").strip()

    if not date_raw:
        parsed_date = _normalize_clarification_date_to_iso(datetime_raw, prefer_future_for_ambiguous=False)
        if parsed_date:
            out["start_at_date"] = parsed_date
            entities["start_at_date"] = parsed_date

    if not time_raw:
        m = re.search(r"\b([01]?\d|2[0-3])(?:[:.](\d{2}))?\b", datetime_raw)
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2) or "0")
            parsed_time = f"{hh:02d}:{mm:02d}"
            out["start_at_time"] = parsed_time
            entities["start_at_time"] = parsed_time

    final_date = str(_command_value(out, "start_at_date") or "").strip()
    final_time = _normalize_time_hhmm_local(_command_value(out, "start_at_time", "start_time", "time"))
    if final_date and final_time and not str(_command_value(out, "start_at") or "").strip():
        out["start_at"] = f"{final_date}T{final_time}"
        entities["start_at"] = f"{final_date}T{final_time}"

    out["entities"] = entities
    return out


def _extract_start_at_date_iso(command: Dict[str, Any]) -> Optional[str]:
    cmd = _normalize_start_date_fields(command if isinstance(command, dict) else {})
    entities = cmd.get("entities")
    entities = entities if isinstance(entities, dict) else {}
    nested = cmd.get("command")
    nested = nested if isinstance(nested, dict) else {}
    nested = _normalize_start_date_fields(nested) if nested else {}
    nested_entities = nested.get("entities")
    nested_entities = nested_entities if isinstance(nested_entities, dict) else {}

    for container in (cmd, entities, nested, nested_entities):
        for key in ("start_at_date", "start_date", "date"):
            raw = container.get(key)
            iso = _normalize_clarification_date_to_iso(
                str(raw or ""),
                prefer_future_for_ambiguous=False,
            )
            if iso:
                return iso

    for start_value in (
        str(cmd.get("start_at") or entities.get("start_at") or "").strip(),
        str(nested.get("start_at") or nested_entities.get("start_at") or "").strip(),
    ):
        if not start_value:
            continue
        first_token = start_value.split()[0]
        first_token = first_token.split("T")[0]
        iso = _normalize_clarification_date_to_iso(
            first_token,
            prefer_future_for_ambiguous=False,
        )
        if iso:
            return iso
    return None


def _is_past_start_date(command: Dict[str, Any]) -> bool:
    iso = _extract_start_at_date_iso(command)
    if not iso:
        return False
    try:
        parsed = date.fromisoformat(iso)
    except ValueError:
        return False
    return parsed < date.today()


def _is_positive_confirmation(text: str) -> bool:
    s = str(text or "").strip().lower()
    return s in {"да", "ага", "угу", "yes", "y", "ok", "ок", "подтверждаю"}


def _is_negative_confirmation(text: str) -> bool:
    s = str(text or "").strip().lower()
    return s in {"нет", "no", "n", "неа"}


def _binary_confirmation_state(text: str) -> str:
    if _is_positive_confirmation(text):
        return "yes"
    if _is_negative_confirmation(text):
        return "no"
    return "unknown"


def _session_scenario_name(active_session: Any) -> str:
    if not isinstance(active_session, dict):
        return "none"
    missing_field = normalize_clarification_field_name(str(active_session.get("missing_field") or ""))
    if missing_field == _TEMPORAL_CONFIRM_FIELD:
        return "temporal_create"
    if missing_field == _PAST_DATE_CONFIRM_FIELD:
        return "temporal_past_date_confirm"
    if missing_field == _TASK_CREATE_CONFIRM_FIELD:
        return "task_create_confirm"
    if missing_field == _MEETING_UPDATE_TARGET_CONFIRM_FIELD:
        return "meeting_update_target_confirm"
    if missing_field == _MEETING_UPDATE_TARGET_SELECT_FIELD:
        return "meeting_update_target_select"
    if missing_field == _MEETING_COMMENT_TARGET_CONFIRM_FIELD:
        return "meeting_comment_target_confirm"
    if missing_field == _MEETING_COMMENT_TARGET_SELECT_FIELD:
        return "meeting_comment_target_select"
    if missing_field == _MEETING_COMMENT_FINAL_CONFIRM_FIELD:
        return "meeting_comment_final_confirm"
    if missing_field == _TASK_COMMENT_TARGET_CONFIRM_FIELD:
        return "task_comment_target_confirm"
    if missing_field == _TASK_COMMENT_TARGET_SELECT_FIELD:
        return "task_comment_target_select"
    if missing_field == _TASK_COMMENT_FINAL_CONFIRM_FIELD:
        return "task_comment_final_confirm"
    return missing_field or "clarification"


def _is_confirm_field(field_name: str) -> bool:
    normalized = normalize_clarification_field_name(str(field_name or ""))
    return normalized in {
        _PAST_DATE_CONFIRM_FIELD,
        _TEMPORAL_CONFIRM_FIELD,
        _TASK_CREATE_CONFIRM_FIELD,
        _MEETING_UPDATE_TARGET_CONFIRM_FIELD,
        _MEETING_COMMENT_TARGET_CONFIRM_FIELD,
        _MEETING_COMMENT_FINAL_CONFIRM_FIELD,
        _TASK_COMMENT_TARGET_CONFIRM_FIELD,
        _TASK_COMMENT_FINAL_CONFIRM_FIELD,
    }


def _is_meeting_update_intent(intent: str) -> bool:
    normalized = str(intent or "").strip().lower()
    return normalized in {"meeting.update", "meeting_update", "reschedule_meeting", "move_meeting"}


def _is_meeting_comment_update_intent(intent: str) -> bool:
    normalized = str(intent or "").strip().lower()
    return normalized in {"meeting.comment.update", "meeting_comment_update"}


def _is_task_comment_update_intent(intent: str) -> bool:
    normalized = str(intent or "").strip().lower()
    return normalized in {"task.comment.update", "task_comment_update"}


def _looks_like_meeting_update_request(text: str) -> bool:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return False
    has_update_verb = any(token in low for token in ("перенес", "сдвин", "поменя", "измени"))
    if not has_update_verb:
        return False
    has_meeting_noun = any(token in low for token in ("встреч", "собран", "созвон", "мероприят", "событ", "ивент"))
    return has_meeting_noun


def _looks_like_meeting_comment_update_request(text: str) -> bool:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return False
    has_comment_verb = bool(re.search(r"\b(добав|измени|поменяй)\w*\b", low))
    has_comment_noun = "комментар" in low
    has_meeting_noun = any(token in low for token in ("встреч", "собран", "созвон", "мероприят", "событ", "ивент"))
    return has_comment_verb and has_comment_noun and has_meeting_noun


def _looks_like_task_comment_update_request(text: str) -> bool:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return False
    has_comment_verb = bool(re.search(r"\b(добав|измени|поменяй)\w*\b", low))
    has_comment_noun = "комментар" in low
    has_task_noun = "задач" in low
    return has_comment_verb and has_comment_noun and has_task_noun


def _meeting_update_source_value(command: Dict[str, Any], *keys: str) -> Any:
    cmd = command if isinstance(command, dict) else {}
    entities = cmd.get("entities")
    entities = entities if isinstance(entities, dict) else {}
    for key in keys:
        if key in cmd and cmd.get(key) is not None and str(cmd.get(key)).strip() != "":
            return cmd.get(key)
        if key in entities and entities.get(key) is not None and str(entities.get(key)).strip() != "":
            return entities.get(key)
    return None


def _meeting_update_target_event_id(command: Dict[str, Any]) -> str:
    raw = _meeting_update_source_value(command, "meeting_update_source_event_id", "calendar_event_id")
    return str(raw or "").strip()


def _meeting_update_has_source(command: Dict[str, Any]) -> bool:
    return bool(_meeting_update_target_event_id(command))


def _meeting_update_parsed_changes(command: Dict[str, Any]) -> Dict[str, Any]:
    source_date = _meeting_update_source_value(command, "meeting_update_source_date")
    source_time = _meeting_update_source_value(command, "meeting_update_source_time")
    source_duration = _meeting_update_source_value(command, "meeting_update_source_duration")
    current_date = _meeting_update_source_value(command, "start_at_date", "date")
    current_time = _meeting_update_source_value(command, "start_at_time", "time", "start_time")
    current_duration = _meeting_update_source_value(command, "duration_minutes", "duration_min")
    changes: Dict[str, Any] = {}
    if str(current_date or "").strip() and str(current_date or "").strip() != str(source_date or "").strip():
        changes["new_date"] = str(current_date).strip()
    if str(current_time or "").strip() and str(current_time or "").strip() != str(source_time or "").strip():
        changes["new_time"] = str(current_time).strip()
    if str(current_duration or "").strip() and str(current_duration or "").strip() != str(source_duration or "").strip():
        changes["new_duration"] = str(current_duration).strip()
    return changes


def _time_range_from_start_and_duration(start_hhmm: str, duration_minutes: Any) -> str:
    start = str(start_hhmm or "").strip()
    mt = re.fullmatch(r"(\d{1,2}):(\d{2})", start)
    if not mt:
        return start
    try:
        minutes = int(str(duration_minutes or "").strip())
    except Exception:
        minutes = 0
    hh = int(mt.group(1))
    mm = int(mt.group(2))
    start_min = hh * 60 + mm
    end_min = start_min + max(0, minutes)
    end_h = (end_min // 60) % 24
    end_m = end_min % 60
    return f"{hh:02d}:{mm:02d}–{end_h:02d}:{end_m:02d}" if minutes > 0 else f"{hh:02d}:{mm:02d}"


def _meeting_update_target_confirmation_question(command: Dict[str, Any]) -> str:
    source_date = _meeting_update_source_value(command, "meeting_update_source_date", "start_at_date", "date")
    source_time = _meeting_update_source_value(command, "meeting_update_source_time", "start_at_time", "time", "start_time")
    source_duration = _meeting_update_source_value(command, "meeting_update_source_duration", "duration_minutes", "duration_min")
    source_date_label = _format_date_ru(source_date)
    source_range_label = _time_range_from_start_and_duration(str(source_time or ""), source_duration) or "-"
    changes = _meeting_update_parsed_changes(command)
    kind = _resolve_meeting_kind(command) or "встреча"
    kind_acc = "встречу" if kind == "встреча" else kind
    lines = [
        f"Вы имеете в виду {kind_acc} {source_date_label} {source_range_label}?",
    ]
    if changes:
        if changes.get("new_date") and not changes.get("new_time") and not changes.get("new_duration"):
            lines.append(f"Перенести её на {_format_date_ru(changes.get('new_date'))}?")
        elif changes.get("new_time") and not changes.get("new_date") and not changes.get("new_duration"):
            lines.append(f"Перенести её на {str(changes.get('new_time') or '').strip()}?")
        else:
            fragments: list[str] = []
            if changes.get("new_date"):
                fragments.append(f"дата: {_format_date_ru(changes.get('new_date'))}")
            if changes.get("new_time"):
                fragments.append(f"время: {str(changes.get('new_time') or '').strip()}")
            if changes.get("new_duration"):
                fragments.append(f"длительность: {_format_duration_human(changes.get('new_duration'))}")
            lines.append("Применить изменения: " + ", ".join(fragments) + "?")
    else:
        lines.append("Изменения пока не распознаны. Подтвердите, что это нужная встреча.")
    return "\n".join(lines)


def _meeting_update_stage_from_missing_field(field_name: str) -> str:
    normalized = normalize_clarification_field_name(str(field_name or ""))
    if normalized in {"meeting_update_target_ref", _MEETING_UPDATE_TARGET_REFINE_FIELD}:
        return _MEETING_UPDATE_TARGET_IDENTIFICATION_FIELD
    if normalized == _MEETING_UPDATE_TARGET_SELECT_FIELD:
        return _MEETING_UPDATE_TARGET_SELECT_FIELD
    if normalized == _MEETING_UPDATE_TARGET_CONFIRM_FIELD:
        return _MEETING_UPDATE_TARGET_CONFIRM_FIELD
    if normalized == _TEMPORAL_CONFIRM_FIELD:
        return _MEETING_UPDATE_FINAL_CONFIRM_FIELD
    return ""


def _meeting_update_target_hint_matches_source(command: Dict[str, Any], hint_text: str) -> bool:
    hint = str(hint_text or "").strip().lower()
    if not hint:
        return False
    source_title = str(_meeting_update_source_value(command, "meeting_update_source_title") or "").strip().lower()
    source_time = str(_meeting_update_source_value(command, "meeting_update_source_time", "start_at_time", "time", "start_time") or "").strip()
    source_date = str(_meeting_update_source_value(command, "meeting_update_source_date", "start_at_date", "date") or "").strip()
    compact = hint.replace(":", " ").replace(".", " ")
    compact = re.sub(r"\s+", " ", compact).strip()
    if source_title and compact and compact in source_title:
        return True
    if source_time:
        hour_part = source_time.split(":", 1)[0].lstrip("0") or "0"
        if re.search(rf"\b{re.escape(source_time)}\b", hint) or re.search(rf"\b{re.escape(hour_part)}\b", compact):
            return True
    if source_date:
        date_label = _format_date_ru(source_date)
        date_wo_year = date_label[:5] if len(date_label) >= 5 else ""
        if date_label and date_label in hint:
            return True
        if date_wo_year and date_wo_year in hint:
            return True
    return False


def _meeting_search_url() -> str:
    base = str(os.getenv("WORKER_COMMAND_URL", "") or "").strip().rstrip("/")
    if not base:
        return ""
    if base.endswith("/runtime/command"):
        return base[: -len("/runtime/command")] + "/runtime/meeting/search"
    return base + "/runtime/meeting/search"


def _task_search_url() -> str:
    base = str(os.getenv("WORKER_COMMAND_URL", "") or "").strip().rstrip("/")
    if not base:
        return ""
    if base.endswith("/runtime/command"):
        return base[: -len("/runtime/command")] + "/runtime/task/search"
    return base + "/runtime/task/search"


def _extract_comment_text_from_source(raw_text: str) -> str:
    text = str(raw_text or "").strip()
    if not text:
        return ""
    normalized = text.strip()
    patterns = [
        r"\b(?:добав(?:ь|ить)|измени(?:\s+на)?|поменяй(?:\s+на)?)\s+комментари(?:й|я)\s*(?::|-)?\s*(.+)$",
        r"\bкомментари(?:й|я)\s*(?::|-)?\s*(.+)$",
    ]
    for pattern in patterns:
        m = re.search(pattern, normalized, flags=re.IGNORECASE)
        if m:
            value = str(m.group(1) or "").strip(" .,:;\"'")
            if value:
                return value
    return ""


def _normalize_task_target_hint(raw_hint: str) -> str:
    raw = str(raw_hint or "").strip().lower().replace("ё", "е")
    if not raw:
        return ""
    cleaned = re.sub(r"[^\w\s#-]", " ", raw)
    cleaned = re.sub(r"\b(в|задаче|задачу|задача|добавь|добавить|измени|поменяй|комментарий|комментария)\b", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _extract_meeting_comment_target_hint(raw_text: str) -> str:
    raw = str(raw_text or "").strip().lower().replace("ё", "е")
    if not raw:
        return ""
    cleaned = re.sub(r"\b(добавь|добавить|измени|поменяй)\b", " ", raw)
    cleaned = re.sub(r"\bкомментар(?:ий|ия)?\b", " ", cleaned)
    cleaned = re.sub(r"[^\w\s:.-]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return _normalize_meeting_target_hint(cleaned)


def _meeting_comment_repeat_search(user_id: str, target_hint: str, meeting_kind: str = "") -> Dict[str, Any]:
    return _meeting_update_repeat_search(user_id, target_hint, meeting_kind)


def _task_comment_repeat_search(user_id: str, target_hint: str) -> Dict[str, Any]:
    url = _task_search_url()
    uid = str(user_id or "").strip()
    hint = str(target_hint or "").strip()
    if not url or not uid:
        return {"ok": False, "reason": "search_unavailable"}
    req = urllib.request.Request(
        url,
        data=json.dumps({"user_id": uid, "target_hint": hint}, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return payload if isinstance(payload, dict) else {"ok": False, "reason": "invalid_search_payload"}
    except Exception:
        return {"ok": False, "reason": "search_failed"}


def _normalize_time_hhmm_local(raw: Any) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    m = re.fullmatch(r"(\d{1,2})(?::(\d{2}))?", value)
    if not m:
        return ""
    hh = int(m.group(1))
    mm = int(m.group(2) or "0")
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return ""
    return f"{hh:02d}:{mm:02d}"


def _normalize_meeting_target_hint(raw_hint: str) -> str:
    raw = str(raw_hint or "").strip().lower().replace("ё", "е")
    if not raw:
        return ""
    # Keep meaningful temporal tokens for worker search.
    m = re.search(r"\b(?:в\s*)?([01]?\d|2[0-3])(?:[:.](\d{2}))?\s*(?:час|часа|часов)?\b", raw)
    time_token = ""
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2) or "0")
        time_token = f"{hh:02d}:{mm:02d}"

    cleaned = re.sub(r"[^\w\s:.-]", " ", raw)
    cleaned = re.sub(r"\b(встречу|встреча|созвон|перенеси|перенести|нужно|нужна|пожалуйста|это|ее|её)\b", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    parts: list[str] = []
    if time_token:
        parts.append(time_token)
    for tok in cleaned.split():
        if tok in {"в", "на", "с", "по", "час", "часа", "часов"}:
            continue
        if re.fullmatch(r"\d{1,2}(:\d{2})?", tok):
            continue
        parts.append(tok)
    return " ".join(parts).strip()


def _extract_meeting_update_participant_hint(raw_text: str) -> str:
    low = str(raw_text or "").strip().lower().replace("ё", "е")
    if not low:
        return ""
    m = re.search(r"\bс\s+([a-zа-я][a-zа-я0-9_-]{1,})\b", low, flags=re.IGNORECASE)
    if not m:
        return ""
    candidate = str(m.group(1) or "").strip().lower()
    if candidate in {
        "кем",
        "кемто",
        "кем-нибудь",
        "кемнибудь",
        "кем-то",
        "утра",
        "вечера",
        "дня",
        "ночи",
    }:
        return ""
    return candidate


def _extract_meeting_update_title_hint(raw_text: str) -> str:
    low = str(raw_text or "").strip().lower().replace("ё", "е")
    if not low:
        return ""
    cleaned = re.sub(r"[^\w\s:-]", " ", low)
    cleaned = re.sub(
        r"\b(измени|измени|перенеси|перенести|сдвинь|сдвин|поменяй|поменять|обнови|нужно|надо|хочу|пожалуйста)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\b(встреча|встречу|собрание|созвон|мероприятие|событие|ивент)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\b(сегодня|завтра|послезавтра|числа|дату|дата|время|на|в|к|с|по)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\b\d{1,2}(?::\d{2})?\b", " ", cleaned)
    cleaned = re.sub(r"\b\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?\b", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _detect_meeting_update_target_anchor(command: Dict[str, Any], source_text: str) -> tuple[bool, str]:
    cmd = command if isinstance(command, dict) else {}
    source_event_id = _meeting_update_target_event_id(cmd)
    if source_event_id:
        return True, "candidate"
    if _meeting_update_candidate_shortlist_from_command(cmd):
        return True, "candidate"

    text = str(
        source_text
        or cmd.get("__source_text")
        or _command_value(cmd, "text")
        or ""
    ).strip()
    direct_date = str(_meeting_update_source_value(cmd, "start_at_date", "date") or "").strip()
    direct_time = _normalize_time_hhmm_local(_meeting_update_source_value(cmd, "start_at_time", "time", "start_time"))
    has_date = bool(direct_date or _extract_start_at_date_iso(cmd) or has_explicit_user_date(text))
    has_time = bool(direct_time or has_explicit_user_time(text))
    if has_date and has_time:
        return True, "date_time"
    if has_date:
        return True, "date"
    if has_time:
        return True, "time"

    participant_hint = _extract_meeting_update_participant_hint(text)
    if participant_hint:
        return True, "participant"

    title_hint = str(cmd.get("__meeting_update_target_hint") or "").strip()
    if not title_hint:
        title_hint = _extract_meeting_update_title_hint(text)
    if title_hint:
        return True, "title"
    return False, ""


def _meeting_update_repeat_search(user_id: str, target_hint: str, meeting_kind: str = "") -> Dict[str, Any]:
    url = _meeting_search_url()
    uid = str(user_id or "").strip()
    hint = str(target_hint or "").strip()
    if not url or not uid:
        return {"ok": False, "reason": "search_unavailable"}
    body = {"user_id": uid, "target_hint": hint}
    normalized_kind = str(meeting_kind or "").strip().lower()
    if normalized_kind in _MEETING_KIND_VARIANTS:
        body["meeting_kind"] = normalized_kind
    req = urllib.request.Request(
        url,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return payload if isinstance(payload, dict) else {"ok": False, "reason": "invalid_search_payload"}
    except Exception:
        return {"ok": False, "reason": "search_failed"}


def _meeting_update_hydrate_from_candidate(command: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}

    source_event_id = str(candidate.get("calendar_event_id") or "").strip()
    source_date = str(candidate.get("start_at_date") or "").strip()
    source_time = _normalize_time_hhmm_local(candidate.get("start_at_time"))
    source_duration = candidate.get("duration_minutes")
    source_title = str(candidate.get("title") or "").strip()
    source_kind = str(candidate.get("meeting_kind") or "").strip().lower() or _extract_meeting_kind_from_text(source_title)

    old_date = str(_meeting_update_source_value(out, "start_at_date", "date") or "").strip()
    old_time = _normalize_time_hhmm_local(_meeting_update_source_value(out, "start_at_time", "time", "start_time"))
    old_duration = _meeting_update_source_value(out, "duration_minutes", "duration_min")

    # Source baseline.
    out["calendar_event_id"] = source_event_id
    out["meeting_update_source_event_id"] = source_event_id
    out["meeting_update_source_date"] = source_date
    out["meeting_update_source_time"] = source_time
    out["meeting_update_source_duration"] = source_duration
    out["meeting_update_source_title"] = source_title
    if source_kind in _MEETING_KIND_VARIANTS:
        out["meeting_kind"] = source_kind
        out["meeting_update_source_kind"] = source_kind
    out["start_at_date"] = source_date
    out["start_at_time"] = source_time
    out["duration_minutes"] = source_duration

    entities["calendar_event_id"] = source_event_id
    entities["meeting_update_source_event_id"] = source_event_id
    entities["meeting_update_source_date"] = source_date
    entities["meeting_update_source_time"] = source_time
    entities["meeting_update_source_duration"] = source_duration
    entities["meeting_update_source_title"] = source_title
    if source_kind in _MEETING_KIND_VARIANTS:
        entities["meeting_kind"] = source_kind
        entities["meeting_update_source_kind"] = source_kind
    entities["start_at_date"] = source_date
    entities["start_at_time"] = source_time
    entities["duration_minutes"] = source_duration

    # Apply already parsed changes back on top of hydrated source.
    new_date = str(_meeting_update_source_value(command, "meeting_update_new_date") or "").strip()
    new_time = _normalize_time_hhmm_local(_meeting_update_source_value(command, "meeting_update_new_time"))
    new_duration = _meeting_update_source_value(command, "meeting_update_new_duration")

    if not new_date and old_date and old_date != source_date:
        new_date = old_date
    if not new_time and old_time and old_time != source_time:
        new_time = old_time
    if (new_duration is None or str(new_duration).strip() == "") and old_duration is not None and str(old_duration).strip() != str(source_duration).strip():
        new_duration = old_duration

    if new_date:
        out["meeting_update_new_date"] = new_date
        out["start_at_date"] = new_date
        entities["meeting_update_new_date"] = new_date
        entities["start_at_date"] = new_date
    if new_time:
        out["meeting_update_new_time"] = new_time
        out["start_at_time"] = new_time
        entities["meeting_update_new_time"] = new_time
        entities["start_at_time"] = new_time
    if new_duration is not None and str(new_duration).strip() != "":
        out["meeting_update_new_duration"] = new_duration
        out["duration_minutes"] = new_duration
        entities["meeting_update_new_duration"] = new_duration
        entities["duration_minutes"] = new_duration

    out["meeting_update_has_changes"] = bool(new_date or new_time or (new_duration is not None and str(new_duration).strip() != ""))
    entities["meeting_update_has_changes"] = out["meeting_update_has_changes"]
    out["entities"] = entities
    return out


def _meeting_update_source_is_hydrated(command: Dict[str, Any]) -> bool:
    event_id = _meeting_update_target_event_id(command)
    date_value = str(_meeting_update_source_value(command, "start_at_date", "date") or "").strip()
    time_value = _normalize_time_hhmm_local(_meeting_update_source_value(command, "start_at_time", "time", "start_time"))
    duration_value = _meeting_update_source_value(command, "duration_minutes", "duration_min")
    has_duration = duration_value is not None and str(duration_value).strip() != ""
    return bool(event_id and date_value and time_value and has_duration)


def _meeting_update_candidate_shortlist_from_command(command: Dict[str, Any]) -> list[Dict[str, Any]]:
    cmd = command if isinstance(command, dict) else {}
    for key in ("__meeting_update_candidates", "meeting_update_candidates"):
        raw = cmd.get(key)
        if isinstance(raw, list):
            out: list[Dict[str, Any]] = []
            for item in raw:
                if isinstance(item, dict):
                    out.append(dict(item))
            return out
    entities = cmd.get("entities")
    entities = entities if isinstance(entities, dict) else {}
    raw = entities.get("__meeting_update_candidates")
    if isinstance(raw, list):
        out: list[Dict[str, Any]] = []
        for item in raw:
            if isinstance(item, dict):
                out.append(dict(item))
        return out
    return []


def _meeting_update_store_candidate_shortlist(command: Dict[str, Any], candidates: list[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    normalized: list[Dict[str, Any]] = []
    for c in candidates:
        if len(normalized) >= _MEETING_UPDATE_SHORTLIST_MAX:
            break
        if not isinstance(c, dict):
            continue
        normalized.append(
            {
                "calendar_event_id": str(c.get("calendar_event_id") or "").strip(),
                "title": str(c.get("title") or "Встреча").strip() or "Встреча",
                "description": str(c.get("description") or "").strip(),
                "meeting_kind": str(c.get("meeting_kind") or "").strip().lower(),
                "start_at_date": str(c.get("start_at_date") or "").strip(),
                "start_at_time": _normalize_time_hhmm_local(c.get("start_at_time")),
                "duration_minutes": c.get("duration_minutes"),
            }
        )
    out["__meeting_update_candidates"] = normalized
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    entities["__meeting_update_candidates"] = normalized
    out["entities"] = entities
    return out


def _normalize_shortlist_title(title: Any, meeting_kind: Any) -> str:
    raw = str(title or "").strip()
    kind = str(meeting_kind or "").strip().lower()
    if not raw:
        return "Встреча"
    s = raw
    s = re.sub(r"^встреч(?:у|е|и)\b", "встреча", s, flags=re.IGNORECASE)
    s = re.sub(r"^собрани(?:е|я|ю)\b", "собрание", s, flags=re.IGNORECASE)
    s = re.sub(r"^(?:созвон(?:а|е)?|звонок|колл)\b", "созвон", s, flags=re.IGNORECASE)
    s = re.sub(r"^мероприяти(?:е|я)\b", "мероприятие", s, flags=re.IGNORECASE)
    s = re.sub(
        r"^(встреча|собрание|созвон|мероприятие)\s+(?:встреч(?:а|у|е|и)|собрани(?:е|я|ю)|созвон(?:а|е)?|звонок|колл|мероприяти(?:е|я))\b",
        r"\1",
        s,
        flags=re.IGNORECASE,
    ).strip()
    if not re.match(r"^(встреча|собрание|созвон|мероприятие)\b", s, flags=re.IGNORECASE):
        if kind in {"встреча", "собрание", "созвон", "мероприятие"}:
            s = f"{kind} {s}".strip()
    return s[:1].upper() + s[1:] if s else "Встреча"


def _meeting_update_render_candidate_shortlist(candidates: list[Dict[str, Any]]) -> str:
    lines = ["Нашёл несколько встреч. Уточните номер:"]
    for idx, c in enumerate(candidates[:_MEETING_UPDATE_SHORTLIST_MAX], start=1):
        title = _normalize_shortlist_title(c.get("title"), c.get("meeting_kind"))
        d = _format_date_ru(c.get("start_at_date"))
        t = _normalize_time_hhmm_local(c.get("start_at_time")) or str(c.get("start_at_time") or "-")
        dur = _format_duration_human(c.get("duration_minutes"))
        lines.append(f"{idx}. {title} — {d}, {t}, {dur}")
    lines.append("")
    lines.append("Напишите номер варианта или уточнение текстом.")
    return "\n".join(lines)


def _meeting_update_select_candidate_from_shortlist(
    candidates: list[Dict[str, Any]],
    user_input: str,
) -> tuple[Optional[Dict[str, Any]], list[Dict[str, Any]], str]:
    raw = str(user_input or "").strip()
    if not raw:
        return None, candidates, "empty_input"

    if re.fullmatch(r"\d+", raw):
        idx = int(raw)
        if 1 <= idx <= len(candidates):
            return dict(candidates[idx - 1]), [dict(candidates[idx - 1])], "index"
        return None, candidates, "index_out_of_range"

    normalized = _normalize_meeting_target_hint(raw)
    query = normalized or raw.lower()
    time_tokens: list[str] = []
    for m in re.finditer(r"\b([01]?\d|2[0-3])(?::(\d{2}))?\b", query):
        hh = int(m.group(1))
        mm = int(m.group(2) or "0")
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            time_tokens.append(f"{hh:02d}:{mm:02d}")
    text_tokens = [tok for tok in re.split(r"\s+", query) if tok and len(tok) >= 2 and not re.fullmatch(r"\d{1,2}(:\d{2})?", tok)]

    filtered: list[Dict[str, Any]] = []
    for c in candidates:
        title = str(c.get("title") or "").lower()
        description = str(c.get("description") or "").lower()
        start_time = _normalize_time_hhmm_local(c.get("start_at_time"))
        matched = False
        if time_tokens and start_time and start_time in time_tokens:
            matched = True
        if not matched and text_tokens:
            if all(tok in title for tok in text_tokens) or all(tok in description for tok in text_tokens):
                matched = True
        if matched:
            filtered.append(dict(c))

    if len(filtered) == 1:
        return dict(filtered[0]), filtered, "filtered_single"
    if len(filtered) > 1:
        return None, filtered, "filtered_multiple"
    return None, candidates, "no_match"


def _meeting_comment_candidate_shortlist_from_command(command: Dict[str, Any]) -> list[Dict[str, Any]]:
    raw = command.get("__meeting_comment_candidates")
    if isinstance(raw, list):
        return [dict(item) for item in raw if isinstance(item, dict)]
    entities = command.get("entities")
    entities = entities if isinstance(entities, dict) else {}
    raw_entities = entities.get("__meeting_comment_candidates")
    if isinstance(raw_entities, list):
        return [dict(item) for item in raw_entities if isinstance(item, dict)]
    return []


def _meeting_update_target_refinement_question() -> str:
    return (
        "Не нашёл подходящее событие.\n"
        "Уточните дату и время или другой ориентир.\n"
        "Например: 'собрание 16 числа в 13:00' или 'созвон с Иваном'."
    )


def _meeting_update_shortlist_rejected(user_input: str) -> bool:
    normalized = str(user_input or "").strip().lower().replace("ё", "е")
    if not normalized:
        return False
    if normalized in {"нет", "неа", "не то", "мимо"}:
        return True
    return ("ни один" in normalized) or ("ниодин" in normalized)


def _meeting_comment_store_candidate_shortlist(command: Dict[str, Any], candidates: list[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    normalized: list[Dict[str, Any]] = []
    for c in candidates:
        if not isinstance(c, dict):
            continue
        normalized.append(
            {
                "calendar_event_id": str(c.get("calendar_event_id") or "").strip(),
                "title": str(c.get("title") or "Встреча").strip() or "Встреча",
                "start_at_date": str(c.get("start_at_date") or "").strip(),
                "start_at_time": _normalize_time_hhmm_local(c.get("start_at_time")),
                "duration_minutes": c.get("duration_minutes"),
                "meeting_kind": str(c.get("meeting_kind") or "").strip().lower(),
            }
        )
    out["__meeting_comment_candidates"] = normalized
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    entities["__meeting_comment_candidates"] = normalized
    out["entities"] = entities
    return out


def _meeting_comment_render_candidate_shortlist(candidates: list[Dict[str, Any]]) -> str:
    lines = ["Нашёл несколько встреч. Уточните номер:"]
    for idx, c in enumerate(candidates[:5], start=1):
        title = _normalize_shortlist_title(c.get("title"), c.get("meeting_kind"))
        d = _format_date_ru(c.get("start_at_date"))
        t = _normalize_time_hhmm_local(c.get("start_at_time")) or str(c.get("start_at_time") or "-")
        lines.append(f"{idx}. {title} — {d}, {t}")
    lines.append("")
    lines.append("Напишите номер варианта или уточнение текстом.")
    return "\n".join(lines)


def _meeting_comment_select_candidate_from_shortlist(
    candidates: list[Dict[str, Any]],
    user_input: str,
) -> tuple[Optional[Dict[str, Any]], list[Dict[str, Any]], str]:
    return _meeting_update_select_candidate_from_shortlist(candidates, user_input)


def _meeting_comment_hydrate_from_candidate(command: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    out["calendar_event_id"] = str(candidate.get("calendar_event_id") or "").strip()
    out["meeting_title"] = str(candidate.get("title") or "").strip()
    out["start_at_date"] = str(candidate.get("start_at_date") or "").strip()
    out["start_at_time"] = _normalize_time_hhmm_local(candidate.get("start_at_time"))
    meeting_kind = str(candidate.get("meeting_kind") or "").strip().lower()
    if meeting_kind in _MEETING_KIND_VARIANTS:
        out["meeting_kind"] = meeting_kind
        entities["meeting_kind"] = meeting_kind
    entities["calendar_event_id"] = out["calendar_event_id"]
    entities["meeting_title"] = out["meeting_title"]
    entities["start_at_date"] = out["start_at_date"]
    entities["start_at_time"] = out["start_at_time"]
    out["entities"] = entities
    return out


def _meeting_comment_final_confirmation_question(command: Dict[str, Any]) -> str:
    kind = _resolve_meeting_kind(command) or "встреча"
    title = str(_command_value(command, "meeting_title", "title") or "").strip()
    date_text = _format_date_ru(_command_value(command, "start_at_date", "date"))
    time_text = _normalize_time_hhmm_local(_command_value(command, "start_at_time", "time")) or "-"
    comment_text = str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip()
    lines = ["Проверьте изменения комментария:"]
    lines.append(f"Тип: {kind}")
    if title:
        lines.append(f"Событие: {title}")
    lines.append(f"Дата: {date_text}")
    lines.append(f"Время: {time_text}")
    lines.append(f"Комментарий: {comment_text or '-'}")
    lines.append("")
    lines.append("Подтвердить изменение комментария?")
    return "\n".join(lines)


def _meeting_comment_target_confirmation_question(command: Dict[str, Any]) -> str:
    kind = _resolve_meeting_kind(command) or "встреча"
    kind_acc = "встречу" if kind == "встреча" else kind
    title = str(_command_value(command, "meeting_title", "title") or "").strip()
    date_text = _format_date_ru(_command_value(command, "start_at_date", "date"))
    time_text = _normalize_time_hhmm_local(_command_value(command, "start_at_time", "time")) or "-"
    if title:
        return f"Изменяем комментарий в {kind_acc} «{title}» ({date_text}, {time_text})?"
    return f"Изменяем комментарий в {kind_acc} {date_text} {time_text}?"


def _task_comment_candidate_shortlist_from_command(command: Dict[str, Any]) -> list[Dict[str, Any]]:
    raw = command.get("__task_comment_candidates")
    if isinstance(raw, list):
        return [dict(item) for item in raw if isinstance(item, dict)]
    entities = command.get("entities")
    entities = entities if isinstance(entities, dict) else {}
    raw_entities = entities.get("__task_comment_candidates")
    if isinstance(raw_entities, list):
        return [dict(item) for item in raw_entities if isinstance(item, dict)]
    return []


def _task_comment_store_candidate_shortlist(command: Dict[str, Any], candidates: list[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    normalized: list[Dict[str, Any]] = []
    for c in candidates:
        if not isinstance(c, dict):
            continue
        normalized.append(
            {
                "task_id": int(c.get("task_id") or 0),
                "title": str(c.get("title") or "").strip(),
                "comment": str(c.get("comment") or "").strip(),
            }
        )
    out["__task_comment_candidates"] = normalized
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    entities["__task_comment_candidates"] = normalized
    out["entities"] = entities
    return out


def _task_comment_render_candidate_shortlist(candidates: list[Dict[str, Any]]) -> str:
    lines = ["Нашёл несколько задач. Уточните номер:"]
    for idx, c in enumerate(candidates[:5], start=1):
        lines.append(f"{idx}. #{int(c.get('task_id') or 0)} {str(c.get('title') or '').strip()}")
    lines.append("")
    lines.append("Напишите номер варианта или уточнение текстом.")
    return "\n".join(lines)


def _task_comment_select_candidate_from_shortlist(
    candidates: list[Dict[str, Any]],
    user_input: str,
) -> tuple[Optional[Dict[str, Any]], list[Dict[str, Any]], str]:
    raw = str(user_input or "").strip()
    if not raw:
        return None, candidates, "empty_input"
    if re.fullmatch(r"\d+", raw):
        idx = int(raw)
        if 1 <= idx <= len(candidates):
            return dict(candidates[idx - 1]), [dict(candidates[idx - 1])], "index"
        return None, candidates, "index_out_of_range"
    query = raw.lower()
    filtered = [dict(c) for c in candidates if query in str(c.get("title") or "").lower() or query in str(c.get("comment") or "").lower()]
    if len(filtered) == 1:
        return dict(filtered[0]), filtered, "filtered_single"
    if len(filtered) > 1:
        return None, filtered, "filtered_multiple"
    return None, candidates, "no_match"


def _task_comment_hydrate_from_candidate(command: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    task_id = int(candidate.get("task_id") or 0)
    out["task_id"] = task_id
    out["task_title"] = str(candidate.get("title") or "").strip()
    entities["task_id"] = task_id
    entities["task_title"] = out["task_title"]
    out["entities"] = entities
    return out


def _task_comment_final_confirmation_question(command: Dict[str, Any]) -> str:
    task_id = str(_command_value(command, "task_id") or "").strip()
    title = str(_command_value(command, "task_title", "title", "task_ref") or "").strip()
    comment_text = str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip()
    lines = ["Проверьте изменения комментария задачи:"]
    if task_id:
        lines.append(f"Задача: #{task_id} {title}".strip())
    else:
        lines.append(f"Задача: {title or '-'}")
    lines.append(f"Комментарий: {comment_text or '-'}")
    lines.append("")
    lines.append("Подтвердить изменение комментария?")
    return "\n".join(lines)


def _task_comment_target_confirmation_question(command: Dict[str, Any]) -> str:
    task_id = str(_command_value(command, "task_id") or "").strip()
    title = str(_command_value(command, "task_title", "title", "task_ref") or "").strip()
    if task_id:
        return f"Изменяем комментарий в задаче #{task_id} {title}?".strip()
    return f"Изменяем комментарий в задаче {title or ''}?".strip()


def _temporal_edit_target_field(text: str) -> str:
    s = str(text or "").strip().lower()
    if not s:
        return ""
    if "дат" in s:
        return "start_at_date"
    if "длит" in s or "мин" in s or "час" in s:
        return "duration_minutes"
    if "врем" in s:
        return "start_at_time"
    return ""


def _normalize_global_control_text(text: str) -> str:
    s = str(text or "").strip().lower().replace("ё", "е")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_global_cancel_phrase(text: str) -> bool:
    normalized = _normalize_global_control_text(text)
    return normalized in _GLOBAL_CANCEL_PHRASES


def _clear_date_fields_for_reask(command: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    for key in ("start_at_date", "start_date", "date"):
        out.pop(key, None)
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    for key in ("start_at_date", "start_date", "date"):
        entities.pop(key, None)
    start_value = str(out.get("start_at") or "").strip() or str(entities.get("start_at") or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", start_value):
        out.pop("start_at", None)
        entities.pop("start_at", None)
    out["entities"] = entities
    return out


def _mark_start_date_confirmed_in_source(command: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    date_iso = _extract_start_at_date_iso(out)
    if not date_iso:
        return out
    existing = str(out.get("__source_text") or "").strip()
    if date_iso in existing:
        return out
    out["__source_text"] = f"{existing} {date_iso}".strip()
    return out


def _context_key(*, app_id: str, tenant_id: str, user_id: str, channel: str, chat_id: str) -> str:
    scoped_chat_id = str(chat_id or user_id or "").strip()
    return (
        f"{app_id or 'default_app'}::{tenant_id or 'default_tenant'}::"
        f"{channel or 'unknown'}::{scoped_chat_id}::{user_id}"
    )


def _extract_missing_field(rec_res: Dict[str, Any], command: Dict[str, Any]) -> str:
    candidate = rec_res.get("missing_field")
    if isinstance(candidate, str) and candidate.strip():
        return normalize_clarification_field_name(candidate.strip())
    hints = rec_res.get("clarification_hints")
    if isinstance(hints, dict):
        for key in ("missing_field", "field", "slot"):
            v = hints.get(key)
            if isinstance(v, str) and v.strip():
                return normalize_clarification_field_name(v.strip())
    cmd_missing = command.get("missing_field") if isinstance(command, dict) else None
    if isinstance(cmd_missing, str) and cmd_missing.strip():
        return normalize_clarification_field_name(cmd_missing.strip())
    return "clarification_value"


def _next_temporal_missing_field(command: Dict[str, Any]) -> str:
    cmd = dict(command if isinstance(command, dict) else {})
    entities = cmd.get("entities")
    entities = entities if isinstance(entities, dict) else {}

    # Preserve existing behavior for commands where temporal fields may still be under entities.
    for key in (
        "start_at",
        "start_time",
        "starts_at",
        "scheduled_at",
        "datetime",
        "duration_minutes",
        "duration_min",
        "duration_mins",
        "duration",
    ):
        if (key not in cmd or cmd.get(key) is None or str(cmd.get(key)).strip() == "") and entities.get(key) is not None:
            cmd[key] = entities.get(key)

    source_text = str(cmd.get("__source_text") or entities.get("text") or "").strip()
    required = has_required_temporal_fields(cmd, source_text)
    if not bool(required.get("ok")):
        return normalize_clarification_field_name(str(required.get("missing_field") or ""))
    return ""


def _resolve_missing_field_for_persistence(
    *,
    rec_res: Dict[str, Any],
    command: Dict[str, Any],
    previous_missing_field: str,
) -> str:
    current = _extract_missing_field(rec_res, command)
    previous = normalize_clarification_field_name(previous_missing_field)
    current = normalize_clarification_field_name(current)
    if previous and current == previous and is_missing_field_already_satisfied(command, previous):
        inferred = _next_temporal_missing_field(command)
        if inferred:
            return inferred
    return current


def _attach_source_identity(
    command: Dict[str, Any],
    *,
    channel: str,
    chat_id: str,
    source_message_id: str,
    source_text: str = "",
) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    if channel and "__source_channel" not in out:
        out["__source_channel"] = str(channel)
    if chat_id and "__source_chat_id" not in out:
        out["__source_chat_id"] = str(chat_id)
    if source_message_id and "__source_message_id" not in out:
        out["__source_message_id"] = str(source_message_id)
    normalized_source_text = str(source_text or "").strip()
    if normalized_source_text and "__source_text" not in out:
        out["__source_text"] = normalized_source_text
    return out


def _extract_entity_ref(rec_res: Dict[str, Any], command: Dict[str, Any]) -> Dict[str, str]:
    rec = rec_res if isinstance(rec_res, dict) else {}
    cmd = command if isinstance(command, dict) else {}
    entity_type = str(rec.get("entity_type") or "").strip()
    entity_id = str(rec.get("entity_id") or "").strip()

    if not entity_id:
        hits = rec.get("hits")
        if isinstance(hits, list) and hits:
            first = hits[0]
            if isinstance(first, dict):
                entity_id = str(first.get("id") or "").strip()
                if not entity_type:
                    entity_type = str(first.get("type") or "").strip()

    if not entity_type:
        intent = str(cmd.get("intent") or "").strip().lower()
        if intent in ("create_timeblock", "timeblock.create", "timeblock_create"):
            entity_type = "timeblock"
        elif intent in ("create_inbox", "inbox.create"):
            entity_type = "task"
        else:
            entity_type = intent

    return {"entity_type": entity_type, "entity_id": entity_id}


def _command_value(command: Dict[str, Any], *keys: str) -> Any:
    cmd = command if isinstance(command, dict) else {}
    entities = cmd.get("entities")
    entities = entities if isinstance(entities, dict) else {}
    for key in keys:
        if key in cmd and cmd.get(key) is not None:
            return cmd.get(key)
        if key in entities and entities.get(key) is not None:
            return entities.get(key)
    return None


def _temporal_draft_value(command: Dict[str, Any], *keys: str) -> Any:
    cmd = command if isinstance(command, dict) else {}
    entities = cmd.get("entities")
    entities = entities if isinstance(entities, dict) else {}
    nested = cmd.get("command")
    nested = nested if isinstance(nested, dict) else {}
    nested_entities = nested.get("entities")
    nested_entities = nested_entities if isinstance(nested_entities, dict) else {}
    for key in keys:
        if key in cmd and cmd.get(key) is not None:
            return cmd.get(key)
        if key in entities and entities.get(key) is not None:
            return entities.get(key)
        if key in nested and nested.get(key) is not None:
            return nested.get(key)
        if key in nested_entities and nested_entities.get(key) is not None:
            return nested_entities.get(key)
    return None


def _has_non_empty_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) > 0
    return True


def _normalized_intent(command: Dict[str, Any]) -> str:
    return str(command.get("intent") or "").strip().lower()


def _is_non_temporal_shortcut_intent(intent: str) -> bool:
    normalized = str(intent or "").strip().lower()
    if not normalized:
        return False
    if normalized in {"task", "timer"}:
        return True
    if normalized.startswith("task."):
        return True
    if normalized.startswith("timer."):
        return True
    if normalized in {"create_task", "task_create"}:
        return True
    return False


def _is_command_complete_for_execution(command: Dict[str, Any]) -> bool:
    cmd = command if isinstance(command, dict) else {}
    intent = _normalized_intent(cmd)
    if not intent:
        return False

    task_title = _command_value(cmd, "title", "task_title", "text")
    task_ref = _command_value(cmd, "task_id", "task_ref")
    timer_value = _command_value(cmd, "seconds", "minutes", "duration_minutes", "duration")

    if intent in {"task", "create_task", "task_create"} or intent.startswith("task."):
        return _has_non_empty_value(task_title) or _has_non_empty_value(task_ref)
    if intent in {"timer"} or intent.startswith("timer."):
        return _has_non_empty_value(timer_value)
    return False


def _is_task_create_intent(intent: str) -> bool:
    normalized = str(intent or "").strip().lower()
    return normalized in {"task", "task.create", "create_task", "task_create"}


def _is_temporal_intent_like(intent: str) -> bool:
    normalized = str(intent or "").strip().lower()
    if not normalized:
        return False
    return any(token in normalized for token in ("timeblock", "block", "meeting", "event", "schedule"))


def _extract_meeting_kind_from_text(raw_text: Any) -> str:
    text = str(raw_text or "").strip().lower().replace("ё", "е")
    if not text:
        return ""
    for kind, variants in _MEETING_KIND_VARIANTS.items():
        for token in variants:
            if re.search(rf"\b{re.escape(token)}\b", text):
                return kind
    return ""


def _resolve_meeting_kind(command: Dict[str, Any]) -> str:
    explicit = str(_command_value(command, "meeting_kind", "event_kind") or "").strip().lower()
    if explicit in _MEETING_KIND_VARIANTS:
        return explicit
    text = str(
        command.get("__source_text")
        or _command_value(command, "text")
        or _command_value(command, "meeting_update_source_title")
        or ""
    ).strip()
    inferred = _extract_meeting_kind_from_text(text)
    return inferred if inferred in _MEETING_KIND_VARIANTS else ""


def _temporal_event_type_label(command: Dict[str, Any]) -> str:
    intent = str(_temporal_draft_value(command, "intent") or command.get("intent") or "").strip().lower()
    meeting_kind = _resolve_meeting_kind(command)
    if meeting_kind:
        return meeting_kind
    if "meeting" in intent or "event" in intent:
        return "встреча"
    if "block" in intent or "timeblock" in intent:
        return "блок времени"
    return "событие"


def _apply_default_duration_for_meeting_create(command: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    intent = _normalized_intent(out)
    source_text = str(
        out.get("__source_text")
        or _command_value(out, "text")
        or ""
    ).strip()
    has_meeting_kind_hint = bool(_extract_meeting_kind_from_text(source_text))
    if intent not in {"meeting.create", "schedule_meeting", "schedule_call", "create_meeting", "meeting_create"}:
        if not (
            intent in {"", "unknown"}
            and has_meeting_kind_hint
            and not _looks_like_meeting_update_request(source_text)
            and not _looks_like_meeting_comment_update_request(source_text)
        ):
            return out
    if _looks_like_meeting_update_request(source_text):
        return out
    has_duration = _has_non_empty_value(_command_value(out, "duration_minutes", "duration_min", "duration_mins", "duration"))
    if has_duration:
        return out
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    out["duration_minutes"] = int(_DEFAULT_MEETING_DURATION_MINUTES)
    entities["duration_minutes"] = int(_DEFAULT_MEETING_DURATION_MINUTES)
    out["entities"] = entities
    return out


def _extract_time_for_summary(command: Dict[str, Any]) -> str:
    direct = _temporal_draft_value(command, "start_at_time", "time", "start_time_text")
    if _has_non_empty_value(direct):
        normalized_direct = _normalize_time_hhmm_local(direct)
        return normalized_direct or str(direct).strip()
    start_value = str(
        _temporal_draft_value(command, "start_at", "start_time", "starts_at", "scheduled_at", "datetime") or ""
    ).strip()
    if start_value:
        m = re.search(r"(?:[t\s])(\d{1,2}[:.]\d{2})", start_value.lower())
        if m:
            normalized = _normalize_time_hhmm_local(m.group(1).replace(".", ":"))
            if normalized:
                return normalized
    source_text = str(
        command.get("__source_text")
        or _temporal_draft_value(command, "text")
        or ""
    ).strip().lower()
    m = re.search(r"\b(?:в|к)\s*([01]?\d|2[0-3])(?:[:.](\d{2}))?\b", source_text)
    if m:
        hour = int(m.group(1))
        minute = int(m.group(2) or "00")
        return f"{hour:02d}:{minute:02d}"
    return "-"


def _extract_meeting_update_date_override_from_text(raw_text: Any) -> str:
    text = str(raw_text or "").strip().lower().replace("ё", "е")
    if not text:
        return ""
    candidate = ""
    patterns = [
        r"\bна\s+(\d{1,2}[./-]\d{1,2}(?:[./-]\d{4})?)\b",
        r"\bна\s+(\d{1,2}\s+[а-я]+(?:\s+\d{4})?)\b",
        r"\bна\s+(сегодня|завтра|послезавтра)\b",
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            candidate = str(m.group(1) or "").strip()
            if candidate:
                break
    if not candidate:
        return ""
    iso = _normalize_clarification_date_to_iso(candidate, prefer_future_for_ambiguous=False)
    return str(iso or "").strip()


def _preserve_meeting_update_parsed_changes(command: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    if not _is_meeting_update_intent(str(out.get("intent") or "")):
        return out
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    if not str(_meeting_update_source_value(out, "meeting_update_new_date") or "").strip():
        parsed_date = str(_extract_start_at_date_iso(out) or "").strip()
        if not parsed_date:
            parsed_date = _extract_meeting_update_date_override_from_text(
                out.get("__source_text")
                or _command_value(out, "text")
                or ""
            )
        if parsed_date:
            out["start_at_date"] = parsed_date
            out["meeting_update_new_date"] = parsed_date
            entities["start_at_date"] = parsed_date
            entities["meeting_update_new_date"] = parsed_date
            out["entities"] = entities
    return out


def _attach_temporal_draft(command: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    date_value = _extract_start_at_date_iso(out) or "-"
    duration_value = _temporal_draft_value(out, "duration_minutes", "duration_min", "duration_mins", "duration")
    duration_text = str(duration_value).strip() if _has_non_empty_value(duration_value) else "-"
    event_type = _temporal_event_type_label(out)
    if event_type in _MEETING_KIND_VARIANTS:
        out["meeting_kind"] = event_type
        entities = out.get("entities")
        entities = dict(entities) if isinstance(entities, dict) else {}
        entities["meeting_kind"] = event_type
        out["entities"] = entities
    out["__temporal_draft"] = {
        "kind": "temporal_commit",
        "event_type": event_type,
        "meeting_kind": event_type if event_type in _MEETING_KIND_VARIANTS else "",
        "date": date_value,
        "time": _extract_time_for_summary(out),
        "duration_minutes": duration_text,
    }
    return out


def _format_date_ru(raw_date: Any) -> str:
    value = str(raw_date or "").strip()
    if not value:
        return "-"
    try:
        parsed = date.fromisoformat(value)
        return parsed.strftime("%d.%m.%Y")
    except Exception:
        return value


def _format_duration_human(raw_duration: Any) -> str:
    value = str(raw_duration or "").strip()
    if not value:
        return "-"
    try:
        minutes = int(value)
    except Exception:
        return value
    if minutes == 60:
        return "1 час"
    return f"{minutes} минут"


def _temporal_confirmation_question(command: Dict[str, Any]) -> str:
    draft = _attach_temporal_draft(command).get("__temporal_draft")
    draft = draft if isinstance(draft, dict) else {}
    lines = ["Проверьте, всё ли верно."]
    lines.append(f"Тип: {draft.get('event_type') or '-'}")
    lines.append(f"Дата: {_format_date_ru(draft.get('date'))}")
    lines.append(f"Время: {draft.get('time') or '-'}")
    lines.append(f"Длительность: {_format_duration_human(draft.get('duration_minutes'))}")
    lines.append("")
    lines.append("Подтвердить планирование?")
    return "\n".join(lines)


def _temporal_summary_signature(command: Dict[str, Any]) -> str:
    draft = _attach_temporal_draft(command).get("__temporal_draft")
    draft = draft if isinstance(draft, dict) else {}
    return "|".join(
        [
            str(_normalized_intent(command)),
            str(draft.get("event_type") or ""),
            str(draft.get("date") or ""),
            str(draft.get("time") or ""),
            str(draft.get("duration_minutes") or ""),
            str(_meeting_update_target_event_id(command) or ""),
        ]
    )


def _task_create_draft_from_command(command: Dict[str, Any]) -> Dict[str, Any]:
    cmd = command if isinstance(command, dict) else {}
    title = _command_value(cmd, "title", "task_title", "text")
    due_date = _command_value(cmd, "due_date", "planned_at", "date", "when")
    priority = _command_value(cmd, "priority")
    notes = _command_value(cmd, "notes", "note", "description")
    draft: Dict[str, Any] = {
        "kind": "task_create",
        "task_text": str(title).strip() if title is not None else "",
    }
    if _has_non_empty_value(due_date):
        draft["due_date"] = str(due_date).strip()
    if _has_non_empty_value(priority):
        draft["priority"] = str(priority).strip()
    if _has_non_empty_value(notes):
        draft["notes"] = str(notes).strip()
    return draft


def _attach_task_create_draft(command: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    out["__task_create_draft"] = _task_create_draft_from_command(out)
    return out


def _task_create_confirmation_question(command: Dict[str, Any]) -> str:
    draft = _task_create_draft_from_command(command)
    lines = ["Проверь задачу перед созданием:"]
    lines.append(f"Текст: {draft.get('task_text') or '-'}")
    if draft.get("due_date"):
        lines.append(f"Срок: {draft.get('due_date')}")
    if draft.get("priority"):
        lines.append(f"Приоритет: {draft.get('priority')}")
    if draft.get("notes"):
        lines.append(f"Заметки: {draft.get('notes')}")
    lines.append("")
    lines.append("Создать задачу?")
    return "\n".join(lines)


def _log_early_clarification_return(
    *,
    request_id: str,
    query_text: str,
    command: Dict[str, Any],
    missing_field: str,
) -> None:
    LOG.info(
        "runtime_early_clarification_return",
        extra={
            "request_id": request_id,
            "query_text": query_text,
            "intent": command.get("intent") if isinstance(command, dict) else None,
            "missing_field": missing_field,
            "outcome": "rec_needs_clarification",
        },
    )


def _generic_clarification_fallback_text() -> str:
    return "Уточни, пожалуйста."


def _clarification_question_for_field(field_name: str) -> str:
    if field_name == _PAST_DATE_CONFIRM_FIELD:
        return "Эта дата уже прошла. Запланировать именно на эту дату?"
    if field_name == _TEMPORAL_CONFIRM_FIELD:
        return "Подтверди планирование: да или нет."
    if field_name == _TEMPORAL_EDIT_FIELD:
        return "Что исправить?"
    if field_name == _TASK_CREATE_CONFIRM_FIELD:
        return "Подтверди создание задачи: да или нет."
    if field_name == _TASK_CREATE_EDIT_FIELD:
        return "Что исправить в задаче? Напиши новый текст."
    if field_name == _MEETING_UPDATE_TARGET_CONFIRM_FIELD:
        return "Это та встреча, которую нужно перенести?"
    if field_name == _MEETING_UPDATE_TARGET_IDENTIFICATION_FIELD:
        return "Уточните, какую встречу нужно перенести."
    if field_name == _MEETING_UPDATE_TARGET_SELECT_FIELD:
        return "Нашёл несколько встреч. Напишите номер или уточнение."
    if field_name == _MEETING_UPDATE_TARGET_REFINE_FIELD:
        return _meeting_update_target_refinement_question()
    if field_name == "meeting_update_target_ref":
        return "Уточните, какую встречу нужно перенести."
    if field_name == "meeting_comment_target_ref":
        return "Уточните, какую встречу нужно изменить."
    if field_name == "task_comment_target_ref":
        return "Уточните, какую задачу нужно изменить."
    if field_name == _MEETING_COMMENT_TARGET_CONFIRM_FIELD:
        return "Это та встреча, где нужно изменить комментарий?"
    if field_name == _MEETING_COMMENT_TARGET_SELECT_FIELD:
        return "Нашёл несколько встреч. Напишите номер или уточнение."
    if field_name == _MEETING_COMMENT_TEXT_FIELD:
        return "Какой комментарий добавить во встречу?"
    if field_name == _MEETING_COMMENT_FINAL_CONFIRM_FIELD:
        return "Подтвердить изменение комментария встречи: да или нет."
    if field_name == _TASK_COMMENT_TARGET_CONFIRM_FIELD:
        return "Это та задача, где нужно изменить комментарий?"
    if field_name == _TASK_COMMENT_TARGET_SELECT_FIELD:
        return "Нашёл несколько задач. Напишите номер или уточнение."
    if field_name == _TASK_COMMENT_TEXT_FIELD:
        return "Какой комментарий добавить к задаче?"
    if field_name == _TASK_COMMENT_FINAL_CONFIRM_FIELD:
        return "Подтвердить изменение комментария задачи: да или нет."
    if field_name in ("duration_minutes", "start_at", "start_at_date", "start_at_time"):
        return temporal_question(field_name)
    return _generic_clarification_fallback_text()


def _temporal_clarification_question_for_command(field_name: str, command: Dict[str, Any]) -> str:
    normalized_field = normalize_clarification_field_name(field_name)
    if normalized_field not in {"start_at_date", "start_at_time", "duration_minutes", "start_at"}:
        return _clarification_question_for_field(field_name)

    intent = _normalized_intent(command)
    meeting_kind = _resolve_meeting_kind(command)
    if meeting_kind in _MEETING_KIND_VARIANTS:
        noun = "встречу" if meeting_kind == "встреча" else meeting_kind
        if normalized_field == "start_at_date":
            return f"На какую дату запланировать {noun}?"
        if normalized_field == "start_at_time":
            return f"Во сколько запланировать {noun}?"
        if normalized_field == "duration_minutes":
            return f"На сколько минут запланировать {noun}?"
        return f"Когда запланировать {noun}?"

    if "block" in intent or "timeblock" in intent:
        if normalized_field == "start_at_date":
            return "На какую дату поставить блок?"
        if normalized_field == "start_at_time":
            return "Во сколько поставить блок?"
        if normalized_field == "duration_minutes":
            return "На сколько минут поставить блок?"
        return "Когда поставить блок?"

    return _clarification_question_for_field(field_name)


def _normalize_temporal_intent_alias(command: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    intent = _normalized_intent(out)
    mapped = _TEMPORAL_INTENT_ALIAS_TO_CANON.get(intent)
    if not mapped:
        return out
    out["intent"] = mapped
    meeting_kind = _resolve_meeting_kind(out)
    if meeting_kind in _MEETING_KIND_VARIANTS:
        out["meeting_kind"] = meeting_kind
        entities = out.get("entities")
        entities = dict(entities) if isinstance(entities, dict) else {}
        entities["meeting_kind"] = meeting_kind
        out["entities"] = entities
    return out


def _return_clarification_with_persist(
    *,
    request_id: str,
    query_text: str,
    command: Dict[str, Any],
    question: str,
    rec_payload: Dict[str, Any],
    missing_field_for_log: str,
    db: Optional[LocalMainDb],
    context_key: str,
    app_id: str,
    tenant_id: str,
    user_id: str,
    intent: str,
    payload: Dict[str, Any],
    source_message_id: Optional[str],
    idempotency_key: Optional[str],
) -> Dict[str, Any]:
    payload_to_store = dict(payload if isinstance(payload, dict) else {})
    command_for_reply = dict(command if isinstance(command, dict) else {})
    effective_intent = str(intent or payload_to_store.get("intent") or command_for_reply.get("intent") or "").strip()
    if _is_meeting_update_intent(effective_intent):
        stage = _meeting_update_stage_from_missing_field(missing_field_for_log)
        if stage:
            payload_to_store[_MEETING_UPDATE_STAGE_KEY] = stage
            command_for_reply[_MEETING_UPDATE_STAGE_KEY] = stage
        parsed_changes = _meeting_update_parsed_changes(payload_to_store)
        draft_state = {
            "date": str(_meeting_update_source_value(payload_to_store, "start_at_date", "date") or ""),
            "time": str(_meeting_update_source_value(payload_to_store, "start_at_time", "time", "start_time") or ""),
            "duration": str(_meeting_update_source_value(payload_to_store, "duration_minutes", "duration_min") or ""),
        }
        LOG.info(
            "meeting_update_state",
            extra={
                "request_id": request_id,
                "flow_id": request_id,
                "active_intent": effective_intent,
                "stage": str(stage or payload_to_store.get(_MEETING_UPDATE_STAGE_KEY) or ""),
                "parsed_changes": parsed_changes,
                "target_search_input": str(payload_to_store.get("__meeting_update_target_hint") or ""),
                "target_found": bool(_meeting_update_has_source(payload_to_store)),
                "draft_state": draft_state,
            },
        )
    _log_early_clarification_return(
        request_id=request_id,
        query_text=query_text,
        command=command_for_reply,
        missing_field=missing_field_for_log,
    )
    if db is not None:
        db.upsert_clarification_session(
            context_key=context_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent=intent,
            payload=payload_to_store,
            missing_field=missing_field_for_log,
            source_message_id=source_message_id,
            idempotency_key=idempotency_key or None,
        )
    if normalize_clarification_field_name(missing_field_for_log) == _TEMPORAL_CONFIRM_FIELD:
        draft = payload_to_store.get("__temporal_draft") if isinstance(payload_to_store, dict) else {}
        draft = draft if isinstance(draft, dict) else {}
        LOG.info(
            "temporal_summary_rendered",
            extra={
                "request_id": request_id,
                "flow_id": request_id,
                "session_id": context_key,
                "user_id": user_id,
                "state": _TEMPORAL_CONFIRM_FIELD,
                "scenario": "temporal_create",
                "has_draft": bool(draft),
                "draft_date": str(draft.get("date") or ""),
                "draft_time": str(draft.get("time") or ""),
                "draft_duration": str(draft.get("duration_minutes") or ""),
            },
        )
    return {
        "outcome": "rec_needs_clarification",
        "clarifying_question": question,
        "user_message": question,
        "rec": rec_payload,
        "command": command_for_reply,
    }


def handle_user_attempt(
    *,
    request_id: str,
    user_id: str,
    channel: str,
    asr_client: Any,
    llm_client: Any,
    rec_client: Any,
    audio_bytes: Optional[bytes] = None,
    audio_mime: Optional[str] = None,
    audio_filename: Optional[str] = None,
    text: Optional[str] = None,
    local_db: Optional[LocalMainDb] = None,
    app_id: str = "",
    tenant_id: str = "",
    source_chat_id: Optional[str] = None,
    source_message_id: Optional[str] = None,
    clarification_ttl_sec: int = 300,
    command_dedup_reservation_ttl_sec: int = 120,
) -> Dict[str, Any]:
    """
    Minimal orchestrator for contract tests.

    Canon rules (Variant C):
    - If ASR fails -> DO NOT call LLM/REC.
    - If LLM fails -> DO NOT call REC, do not return partial results.
    - If REC outcome == rejected -> DO NOT auto-retry.
    - If needs_clarification -> ask exactly one clarifying question (use hints).
    """
    _ = (source_message_id,)  # reserved for trace/logger integration
    chat_id = str(source_chat_id or user_id or "").strip()
    incoming_message_id = str(source_message_id or "").strip()
    db = local_db
    if db is None:
        path = str(os.getenv("LOCAL_DB_PATH", "") or "").strip()
        if path:
            db = LocalMainDb(path)

    ctx_key = _context_key(app_id=app_id, tenant_id=tenant_id, user_id=user_id, channel=channel, chat_id=chat_id)
    active_session = None
    if db is not None:
        active_session = db.get_active_clarification_session(
            context_key=ctx_key,
            ttl_seconds=int(clarification_ttl_sec),
        )
    from_clarification = active_session is not None
    active_scenario = _session_scenario_name(active_session)

    # 1) Resolve user text.
    resolved_text = (text or "").strip()
    if not resolved_text and audio_bytes is not None:
        voice_result = process_voice_pipeline(
            request_id=request_id,
            user_id=user_id,
            is_clarification_reply=from_clarification,
            audio_bytes=audio_bytes,
            asr_transcribe=lambda prepared_audio: (
                asr_client.transcribe(
                    audio_bytes=prepared_audio,
                    mime_type=audio_mime,
                    filename=audio_filename,
                    request_id=request_id,
                )
                or ""
            ),
        )
        if not voice_result.ok:
            return {
                "outcome": voice_result.status,
                "user_message": str(voice_result.user_message or map_failure_to_user_message("asr_unavailable", details={})),
                "debug_reason": voice_result.debug_reason,
            }
        resolved_text = str(voice_result.text or "").strip()

    if not resolved_text:
        # Not specified by tests; keep it user-friendly and non-technical.
        return {
            "outcome": "rejected",
            "user_message": "Скажи, пожалуйста, что нужно сделать.",
        }

    confirm_state_probe = _binary_confirmation_state(resolved_text)
    if confirm_state_probe in {"yes", "no"}:
        active_missing_for_log = (
            normalize_clarification_field_name(str(active_session.get("missing_field") or ""))
            if isinstance(active_session, dict)
            else ""
        )
        active_payload_for_log = (
            active_session.get("payload")
            if isinstance(active_session, dict) and isinstance(active_session.get("payload"), dict)
            else {}
        )
        LOG.info(
            "confirm_received",
            extra={
                "request_id": request_id,
                "flow_id": request_id,
                "incoming_text": resolved_text,
                "user_id": user_id,
                "confirm_state": confirm_state_probe,
                "session_found": from_clarification,
                "active_session_found": from_clarification,
                "session_state": active_missing_for_log,
                "scenario": active_scenario,
                "session_scenario": active_scenario,
                "from_edited_summary": bool(active_payload_for_log.get("__edited_temporal_draft")),
            },
        )
        if not from_clarification:
            LOG.warning(
                "confirm_not_routed",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "user_id": user_id,
                    "reason": "no_active_session",
                    "context_key": ctx_key,
                    "chat_id": chat_id,
                    "session_state": "",
                    "scenario": "none",
                },
            )

    if _is_global_cancel_phrase(resolved_text):
        if from_clarification:
            if db is not None:
                db.delete_clarification_session(ctx_key)
            return {
                "outcome": "cancelled",
                "user_message": "Ок, остановил текущий сценарий.",
                "command": None,
            }
        return {
            "outcome": "cancel_no_active",
            "user_message": "Сейчас нет активного сценария.",
            "command": None,
        }

    active_missing_field = str(active_session.get("missing_field") or "").strip() if isinstance(active_session, dict) else ""
    active_session_idempotency_key = (
        str(active_session.get("idempotency_key") or "").strip() if isinstance(active_session, dict) else ""
    )
    query_text = resolved_text
    if from_clarification:
        payload = active_session.get("payload") if isinstance(active_session, dict) else {}
        payload = payload if isinstance(payload, dict) else {}
        source_text = str(payload.get("__source_text") or "").strip()
        if not source_text:
            entities = payload.get("entities")
            if isinstance(entities, dict):
                source_text = str(entities.get("text") or "").strip()
        missing_field = normalize_clarification_field_name(str(active_session.get("missing_field") or "clarification_value"))
        session_intent = str(active_session.get("intent") or "").strip() if isinstance(active_session, dict) else ""
        if _is_meeting_update_intent(session_intent):
            incoming_before_lock = str(payload.get("intent") or "").strip()
            payload["intent"] = "meeting.update"
            payload[_MEETING_UPDATE_STAGE_KEY] = _meeting_update_stage_from_missing_field(missing_field) or str(
                payload.get(_MEETING_UPDATE_STAGE_KEY) or ""
            )
            if missing_field in {
                "meeting_update_target_ref",
                _MEETING_UPDATE_TARGET_REFINE_FIELD,
                _MEETING_UPDATE_TARGET_SELECT_FIELD,
                _MEETING_UPDATE_TARGET_CONFIRM_FIELD,
            }:
                LOG.info(
                    "meeting_update_continuation_locked",
                    extra={
                        "request_id": request_id,
                        "flow_id": request_id,
                        "active_intent": session_intent,
                        "state": missing_field,
                    },
                )
            LOG.info(
                "meeting_update_intent_locked",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "active_intent": session_intent,
                    "incoming_intent": incoming_before_lock,
                    "locked_intent": "meeting.update",
                },
            )
            LOG.info(
                "meeting_update_state",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "active_intent": session_intent,
                    "stage": str(payload.get(_MEETING_UPDATE_STAGE_KEY) or ""),
                    "parsed_changes": _meeting_update_parsed_changes(payload),
                    "target_search_input": str(payload.get("__meeting_update_target_hint") or ""),
                    "target_found": bool(_meeting_update_has_source(payload)),
                    "draft_state": {
                        "date": str(_meeting_update_source_value(payload, "start_at_date", "date") or ""),
                        "time": str(_meeting_update_source_value(payload, "start_at_time", "time", "start_time") or ""),
                        "duration": str(_meeting_update_source_value(payload, "duration_minutes", "duration_min") or ""),
                    },
                },
            )
        if (
            confirm_state_probe in {"yes", "no"}
            and missing_field
            not in {
                _TEMPORAL_CONFIRM_FIELD,
                _PAST_DATE_CONFIRM_FIELD,
                _TASK_CREATE_CONFIRM_FIELD,
                _MEETING_COMMENT_TARGET_CONFIRM_FIELD,
                _MEETING_COMMENT_FINAL_CONFIRM_FIELD,
                _TASK_COMMENT_TARGET_CONFIRM_FIELD,
                _TASK_COMMENT_FINAL_CONFIRM_FIELD,
            }
        ):
            LOG.warning(
                "confirm_not_routed",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "user_id": user_id,
                    "reason": "state_mismatch",
                    "session_state": missing_field,
                    "scenario": _session_scenario_name(active_session),
                },
            )
        if missing_field == _PAST_DATE_CONFIRM_FIELD:
            command = dict(payload)
            corrected_date_iso = _normalize_clarification_date_to_iso(
                resolved_text,
                prefer_future_for_ambiguous=False,
            )
            confirm_state = _binary_confirmation_state(resolved_text)
            if corrected_date_iso:
                command = _inject_start_at_date_into_command(command, corrected_date_iso)
                command.pop("__past_date_confirmed", None)
                continuation_accepted = True
                continuation_field_name = "start_at_date"
            elif confirm_state == "yes":
                command["__past_date_confirmed"] = True
                command = _mark_start_date_confirmed_in_source(command)
                continuation_accepted = True
                continuation_field_name = _PAST_DATE_CONFIRM_FIELD
            elif confirm_state == "no":
                command = _clear_date_fields_for_reask(command)
                continuation_accepted = False
                continuation_field_name = "start_at_date"
            else:
                continuation_accepted = False
                continuation_field_name = _PAST_DATE_CONFIRM_FIELD
        elif missing_field == _TEMPORAL_CONFIRM_FIELD:
            command = dict(payload)
            duration_override = resolve_clarification_continuation("duration_minutes", resolved_text, command)
            if bool(duration_override.accepted):
                command = duration_override.command if isinstance(duration_override.command, dict) else command
                command.pop("__temporal_confirmed", None)
                command["__edited_temporal_draft"] = True
                continuation_accepted = True
                continuation_field_name = "duration_minutes"
            else:
                confirm_state = _binary_confirmation_state(resolved_text)
                if confirm_state == "yes":
                    temporal_with_draft = _attach_temporal_draft(command)
                    draft_for_log = temporal_with_draft.get("__temporal_draft")
                    draft_for_log = draft_for_log if isinstance(draft_for_log, dict) else {}
                    if str(command.get("intent") or "").strip().lower() in {"meeting.update", "meeting_update", "reschedule_meeting"}:
                        LOG.info(
                            "meeting_update_confirm_received",
                            extra={
                                "request_id": request_id,
                                "flow_id": request_id,
                                "draft_date": str(draft_for_log.get("date") or ""),
                                "draft_time": str(draft_for_log.get("time") or ""),
                                "draft_duration": str(draft_for_log.get("duration_minutes") or ""),
                                "source_event_id": str(command.get("calendar_event_id") or ""),
                            },
                        )
                    LOG.info(
                        "edited_summary_confirm_received",
                        extra={
                            "request_id": request_id,
                            "flow_id": request_id,
                            "draft_before_commit": bool(draft_for_log),
                            "draft_date": str(draft_for_log.get("date") or ""),
                            "draft_time": str(draft_for_log.get("time") or ""),
                            "draft_duration": str(draft_for_log.get("duration_minutes") or ""),
                            "meeting_intent": str(command.get("intent") or ""),
                            "edited_draft": bool(command.get("__edited_temporal_draft")),
                        },
                    )
                    command = temporal_with_draft
                    command["__temporal_confirmed"] = True
                    continuation_accepted = True
                    continuation_field_name = _TEMPORAL_CONFIRM_FIELD
                elif confirm_state == "no":
                    command.pop("__temporal_confirmed", None)
                    continuation_accepted = False
                    continuation_field_name = _TEMPORAL_EDIT_FIELD
                else:
                    continuation_accepted = False
                    continuation_field_name = _TEMPORAL_CONFIRM_FIELD
        elif missing_field == _TEMPORAL_EDIT_FIELD:
            command = dict(payload)
            direct_date_iso = _normalize_clarification_date_to_iso(
                resolved_text,
                prefer_future_for_ambiguous=False,
            )
            active_edit_field = normalize_clarification_field_name(
                str(command.get(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY) or "")
            )
            if active_edit_field not in {"start_at_date", "start_at_time", "duration_minutes"}:
                active_edit_field = ""
            if direct_date_iso:
                command = _inject_start_at_date_into_command(command, direct_date_iso)
                command.pop(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY, None)
                continuation_accepted = True
                continuation_field_name = "start_at_date"
            else:
                continuation_accepted = False
                direct_fields = (active_edit_field,) if active_edit_field else ("start_at_time", "duration_minutes")
                for direct_field in direct_fields:
                    if not direct_field:
                        continue
                    direct = resolve_clarification_continuation(direct_field, resolved_text, command)
                    if bool(direct.accepted):
                        command = direct.command if isinstance(direct.command, dict) else command
                        command.pop(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY, None)
                        continuation_accepted = True
                        continuation_field_name = direct_field
                        break
                if continuation_accepted:
                    pass
                else:
                    target = _temporal_edit_target_field(resolved_text)
                    if target:
                        LOG.info(
                            "temporal_edit_route",
                            extra={
                                "request_id": request_id,
                                "flow_id": request_id,
                                "target_field": target,
                            },
                        )
                        command[_TEMPORAL_EDIT_ACTIVE_FIELD_KEY] = target
                        continuation_accepted = False
                        continuation_field_name = target
                    else:
                        continuation_accepted = False
                        continuation_field_name = active_edit_field or _TEMPORAL_EDIT_FIELD
        elif missing_field == _TASK_CREATE_CONFIRM_FIELD:
            command = dict(payload)
            confirm_state = _binary_confirmation_state(resolved_text)
            if confirm_state == "yes":
                command["__task_create_confirmed"] = True
                continuation_accepted = True
                continuation_field_name = _TASK_CREATE_CONFIRM_FIELD
            elif confirm_state == "no":
                command.pop("__task_create_confirmed", None)
                continuation_accepted = False
                continuation_field_name = _TASK_CREATE_EDIT_FIELD
            else:
                continuation_accepted = False
                continuation_field_name = _TASK_CREATE_CONFIRM_FIELD
        elif missing_field == _TASK_CREATE_EDIT_FIELD:
            command = dict(payload)
            corrected_title = str(resolved_text or "").strip()
            if corrected_title:
                command["title"] = corrected_title
                entities = command.get("entities")
                entities = dict(entities) if isinstance(entities, dict) else {}
                entities["title"] = corrected_title
                command["entities"] = entities
                command.pop("__task_create_confirmed", None)
                continuation_accepted = True
                continuation_field_name = "title"
            else:
                continuation_accepted = False
                continuation_field_name = _TASK_CREATE_EDIT_FIELD
        elif missing_field == _MEETING_UPDATE_TARGET_CONFIRM_FIELD:
            command = dict(payload)
            confirm_state = _binary_confirmation_state(resolved_text)
            if confirm_state == "yes":
                command["__meeting_update_target_confirmed"] = True
                continuation_accepted = True
                continuation_field_name = _MEETING_UPDATE_TARGET_CONFIRM_FIELD
            elif confirm_state == "no":
                command.pop("__meeting_update_target_confirmed", None)
                continuation_accepted = False
                continuation_field_name = "meeting_update_target_ref"
            else:
                continuation_accepted = False
                continuation_field_name = _MEETING_UPDATE_TARGET_CONFIRM_FIELD
        elif missing_field in {"meeting_update_target_ref", _MEETING_UPDATE_TARGET_REFINE_FIELD}:
            command = dict(payload)
            raw_target_hint = str(resolved_text or "").strip()
            normalized_target_hint = _normalize_meeting_target_hint(raw_target_hint)
            search_query = normalized_target_hint or raw_target_hint
            session_meeting_kind = str(_meeting_update_source_value(command, "meeting_kind", "event_kind") or "").strip().lower()
            if session_meeting_kind not in _MEETING_KIND_VARIANTS:
                session_meeting_kind = _extract_meeting_kind_from_text(
                    str(active_session.get("query_text") or command.get("__source_text") or command.get("text") or "")
                )
            if session_meeting_kind in _MEETING_KIND_VARIANTS:
                command["meeting_kind"] = session_meeting_kind
            command["__meeting_update_target_hint"] = search_query
            active_session_intent = str(active_session.get("intent") or "") if isinstance(active_session, dict) else ""
            LOG.info(
                "meeting_update_target_ref_input",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "target_search_input": raw_target_hint,
                    "active_session_intent": active_session_intent,
                    "parsed_changes": _meeting_update_parsed_changes(command),
                },
            )
            LOG.info(
                "meeting_update_target_ref_normalized_query",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "raw_target_hint": raw_target_hint,
                    "normalized_target_hint": normalized_target_hint,
                    "search_query": search_query,
                    "active_session_intent": active_session_intent,
                    "preserved_parsed_change": _meeting_update_parsed_changes(command),
                },
            )
            user_id_for_search = str(user_id or "").strip()
            LOG.info(
                "meeting_update_repeat_search_started",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "user_id": user_id_for_search,
                    "target_search_input": search_query,
                    "meeting_kind": session_meeting_kind,
                },
            )
            try:
                search_result = _meeting_update_repeat_search(
                    user_id_for_search,
                    search_query,
                    session_meeting_kind,
                )
            except TypeError:
                # Backward-compatible for tests/patches that monkeypatch old 2-arg signature.
                search_result = _meeting_update_repeat_search(user_id_for_search, search_query)
            candidates = search_result.get("candidates") if isinstance(search_result.get("candidates"), list) else []
            candidate = search_result.get("candidate") if isinstance(search_result.get("candidate"), dict) else None
            LOG.info(
                "meeting_update_repeat_search_result",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "ok": bool(search_result.get("ok")),
                    "reason": str(search_result.get("reason") or ""),
                    "candidates_count": len(candidates),
                    "target_found": bool(candidate),
                },
            )
            LOG.info(
                "meeting_update_repeat_search_result_count",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "count": len(candidates) if candidates else (1 if candidate else 0),
                    "target_search_input": search_query,
                },
            )
            continuation_accepted = False
            if bool(search_result.get("ok")) and isinstance(candidate, dict):
                command = _meeting_update_hydrate_from_candidate(command, candidate)
                LOG.info(
                    "meeting_update_source_hydrated",
                    extra={
                        "request_id": request_id,
                        "flow_id": request_id,
                        "calendar_event_id": str(command.get("calendar_event_id") or ""),
                        "draft_date": str(command.get("start_at_date") or ""),
                        "draft_time": str(command.get("start_at_time") or ""),
                        "draft_duration": str(command.get("duration_minutes") or ""),
                        "parsed_changes": _meeting_update_parsed_changes(command),
                    },
                )
                continuation_field_name = _MEETING_UPDATE_TARGET_CONFIRM_FIELD
            elif str(search_result.get("reason") or "").strip().lower() == "multiple" and candidates:
                shortlist = [dict(c) for c in candidates if isinstance(c, dict)][:_MEETING_UPDATE_SHORTLIST_MAX]
                command = _meeting_update_store_candidate_shortlist(command, shortlist)
                LOG.info(
                    "meeting_update_multiple_candidates_found",
                    extra={
                        "request_id": request_id,
                        "flow_id": request_id,
                        "count": len(shortlist),
                        "target_search_input": search_query,
                    },
                )
                LOG.info(
                    "meeting_update_candidate_list_rendered",
                    extra={
                        "request_id": request_id,
                        "flow_id": request_id,
                        "count": len(shortlist),
                    },
                )
                continuation_field_name = _MEETING_UPDATE_TARGET_SELECT_FIELD
            else:
                continuation_field_name = _MEETING_UPDATE_TARGET_REFINE_FIELD
        elif missing_field == _MEETING_UPDATE_TARGET_SELECT_FIELD:
            command = dict(payload)
            shortlist = _meeting_update_candidate_shortlist_from_command(command)
            selection_input = str(resolved_text or "").strip()
            LOG.info(
                "meeting_update_candidate_selection_input",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "input": selection_input,
                    "shortlist_count": len(shortlist),
                },
            )
            selected, filtered, reason = _meeting_update_select_candidate_from_shortlist(shortlist, selection_input)
            continuation_accepted = False
            if isinstance(selected, dict):
                command = _meeting_update_hydrate_from_candidate(command, selected)
                LOG.info(
                    "meeting_update_candidate_selection_matched",
                    extra={
                        "request_id": request_id,
                        "flow_id": request_id,
                        "reason": reason,
                        "calendar_event_id": str(selected.get("calendar_event_id") or ""),
                        "title": str(selected.get("title") or ""),
                    },
                )
                LOG.info(
                    "meeting_update_source_hydrated",
                    extra={
                        "request_id": request_id,
                        "flow_id": request_id,
                        "calendar_event_id": str(command.get("calendar_event_id") or ""),
                        "draft_date": str(command.get("start_at_date") or ""),
                        "draft_time": str(command.get("start_at_time") or ""),
                        "draft_duration": str(command.get("duration_minutes") or ""),
                        "parsed_changes": _meeting_update_parsed_changes(command),
                    },
                )
                continuation_field_name = _MEETING_UPDATE_TARGET_CONFIRM_FIELD
            else:
                rejected = _meeting_update_shortlist_rejected(selection_input)
                if rejected:
                    LOG.info(
                        "meeting_update_candidate_selection_no_match",
                        extra={
                            "request_id": request_id,
                            "flow_id": request_id,
                            "reason": "explicit_reject",
                            "remaining_count": len(filtered),
                        },
                    )
                    continuation_field_name = _MEETING_UPDATE_TARGET_REFINE_FIELD
                elif filtered and len(filtered) > 1:
                    command = _meeting_update_store_candidate_shortlist(
                        command,
                        filtered[:_MEETING_UPDATE_SHORTLIST_MAX],
                    )
                    LOG.info(
                        "meeting_update_candidate_selection_no_match",
                        extra={
                            "request_id": request_id,
                            "flow_id": request_id,
                            "reason": "multiple_after_filter",
                            "remaining_count": len(filtered),
                        },
                    )
                    LOG.info(
                        "meeting_update_candidate_list_rendered",
                        extra={
                            "request_id": request_id,
                            "flow_id": request_id,
                            "count": len(filtered),
                        },
                    )
                    continuation_field_name = _MEETING_UPDATE_TARGET_SELECT_FIELD
                else:
                    LOG.info(
                        "meeting_update_candidate_selection_no_match",
                        extra={
                            "request_id": request_id,
                            "flow_id": request_id,
                            "reason": reason,
                            "remaining_count": len(filtered),
                        },
                    )
                    continuation_field_name = _MEETING_UPDATE_TARGET_REFINE_FIELD
        elif missing_field == "meeting_comment_target_ref":
            command = dict(payload)
            raw_target_hint = str(resolved_text or "").strip()
            normalized_target_hint = _extract_meeting_comment_target_hint(raw_target_hint)
            search_query = normalized_target_hint or raw_target_hint
            meeting_kind = _resolve_meeting_kind(command)
            command["__meeting_comment_target_hint"] = search_query
            search_result = _meeting_comment_repeat_search(str(user_id or "").strip(), search_query, meeting_kind)
            candidates = search_result.get("candidates") if isinstance(search_result.get("candidates"), list) else []
            candidate = search_result.get("candidate") if isinstance(search_result.get("candidate"), dict) else None
            continuation_accepted = False
            if bool(search_result.get("ok")) and isinstance(candidate, dict):
                command = _meeting_comment_hydrate_from_candidate(command, candidate)
                continuation_field_name = _MEETING_COMMENT_TARGET_CONFIRM_FIELD
            elif str(search_result.get("reason") or "").strip().lower() == "multiple" and candidates:
                command = _meeting_comment_store_candidate_shortlist(
                    command,
                    [dict(c) for c in candidates if isinstance(c, dict)],
                )
                continuation_field_name = _MEETING_COMMENT_TARGET_SELECT_FIELD
            else:
                continuation_field_name = "meeting_comment_target_ref"
        elif missing_field == _MEETING_COMMENT_TARGET_SELECT_FIELD:
            command = dict(payload)
            shortlist = _meeting_comment_candidate_shortlist_from_command(command)
            selected, filtered, _reason = _meeting_comment_select_candidate_from_shortlist(shortlist, str(resolved_text or "").strip())
            continuation_accepted = False
            if isinstance(selected, dict):
                command = _meeting_comment_hydrate_from_candidate(command, selected)
                continuation_field_name = _MEETING_COMMENT_TARGET_CONFIRM_FIELD
            elif filtered and len(filtered) > 1:
                command = _meeting_comment_store_candidate_shortlist(command, filtered)
                continuation_field_name = _MEETING_COMMENT_TARGET_SELECT_FIELD
            else:
                continuation_field_name = "meeting_comment_target_ref"
        elif missing_field == _MEETING_COMMENT_TARGET_CONFIRM_FIELD:
            command = dict(payload)
            confirm_state = _binary_confirmation_state(resolved_text)
            if confirm_state == "yes":
                command["__meeting_comment_target_confirmed"] = True
                continuation_accepted = True
                comment_text = str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip()
                continuation_field_name = _MEETING_COMMENT_FINAL_CONFIRM_FIELD if comment_text else _MEETING_COMMENT_TEXT_FIELD
            elif confirm_state == "no":
                command.pop("__meeting_comment_target_confirmed", None)
                continuation_accepted = False
                continuation_field_name = "meeting_comment_target_ref"
            else:
                continuation_accepted = False
                continuation_field_name = _MEETING_COMMENT_TARGET_CONFIRM_FIELD
        elif missing_field == _MEETING_COMMENT_TEXT_FIELD:
            command = dict(payload)
            comment_text = str(resolved_text or "").strip()
            if comment_text:
                command["comment_text"] = comment_text
                entities = command.get("entities")
                entities = dict(entities) if isinstance(entities, dict) else {}
                entities["comment_text"] = comment_text
                command["entities"] = entities
                continuation_accepted = True
                continuation_field_name = _MEETING_COMMENT_FINAL_CONFIRM_FIELD
            else:
                continuation_accepted = False
                continuation_field_name = _MEETING_COMMENT_TEXT_FIELD
        elif missing_field == _MEETING_COMMENT_FINAL_CONFIRM_FIELD:
            command = dict(payload)
            confirm_state = _binary_confirmation_state(resolved_text)
            if confirm_state == "yes":
                command["__comment_update_confirmed"] = True
                continuation_accepted = True
                continuation_field_name = _MEETING_COMMENT_FINAL_CONFIRM_FIELD
            elif confirm_state == "no":
                command.pop("__comment_update_confirmed", None)
                continuation_accepted = False
                continuation_field_name = _MEETING_COMMENT_TEXT_FIELD
            else:
                continuation_accepted = False
                continuation_field_name = _MEETING_COMMENT_FINAL_CONFIRM_FIELD
        elif missing_field == "task_comment_target_ref":
            command = dict(payload)
            raw_target_hint = str(resolved_text or "").strip()
            normalized_target_hint = _normalize_task_target_hint(raw_target_hint)
            search_query = normalized_target_hint or raw_target_hint
            command["__task_comment_target_hint"] = search_query
            search_result = _task_comment_repeat_search(str(user_id or "").strip(), search_query)
            candidates = search_result.get("candidates") if isinstance(search_result.get("candidates"), list) else []
            candidate = search_result.get("candidate") if isinstance(search_result.get("candidate"), dict) else None
            continuation_accepted = False
            if bool(search_result.get("ok")) and isinstance(candidate, dict):
                command = _task_comment_hydrate_from_candidate(command, candidate)
                continuation_field_name = _TASK_COMMENT_TARGET_CONFIRM_FIELD
            elif str(search_result.get("reason") or "").strip().lower() == "multiple" and candidates:
                command = _task_comment_store_candidate_shortlist(
                    command,
                    [dict(c) for c in candidates if isinstance(c, dict)],
                )
                continuation_field_name = _TASK_COMMENT_TARGET_SELECT_FIELD
            else:
                continuation_field_name = "task_comment_target_ref"
        elif missing_field == _TASK_COMMENT_TARGET_SELECT_FIELD:
            command = dict(payload)
            shortlist = _task_comment_candidate_shortlist_from_command(command)
            selected, filtered, _reason = _task_comment_select_candidate_from_shortlist(shortlist, str(resolved_text or "").strip())
            continuation_accepted = False
            if isinstance(selected, dict):
                command = _task_comment_hydrate_from_candidate(command, selected)
                continuation_field_name = _TASK_COMMENT_TARGET_CONFIRM_FIELD
            elif filtered and len(filtered) > 1:
                command = _task_comment_store_candidate_shortlist(command, filtered)
                continuation_field_name = _TASK_COMMENT_TARGET_SELECT_FIELD
            else:
                continuation_field_name = "task_comment_target_ref"
        elif missing_field == _TASK_COMMENT_TARGET_CONFIRM_FIELD:
            command = dict(payload)
            confirm_state = _binary_confirmation_state(resolved_text)
            if confirm_state == "yes":
                command["__task_comment_target_confirmed"] = True
                continuation_accepted = True
                comment_text = str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip()
                continuation_field_name = _TASK_COMMENT_FINAL_CONFIRM_FIELD if comment_text else _TASK_COMMENT_TEXT_FIELD
            elif confirm_state == "no":
                command.pop("__task_comment_target_confirmed", None)
                continuation_accepted = False
                continuation_field_name = "task_comment_target_ref"
            else:
                continuation_accepted = False
                continuation_field_name = _TASK_COMMENT_TARGET_CONFIRM_FIELD
        elif missing_field == _TASK_COMMENT_TEXT_FIELD:
            command = dict(payload)
            comment_text = str(resolved_text or "").strip()
            if comment_text:
                command["comment_text"] = comment_text
                entities = command.get("entities")
                entities = dict(entities) if isinstance(entities, dict) else {}
                entities["comment_text"] = comment_text
                command["entities"] = entities
                continuation_accepted = True
                continuation_field_name = _TASK_COMMENT_FINAL_CONFIRM_FIELD
            else:
                continuation_accepted = False
                continuation_field_name = _TASK_COMMENT_TEXT_FIELD
        elif missing_field == _TASK_COMMENT_FINAL_CONFIRM_FIELD:
            command = dict(payload)
            confirm_state = _binary_confirmation_state(resolved_text)
            if confirm_state == "yes":
                command["__comment_update_confirmed"] = True
                continuation_accepted = True
                continuation_field_name = _TASK_COMMENT_FINAL_CONFIRM_FIELD
            elif confirm_state == "no":
                command.pop("__comment_update_confirmed", None)
                continuation_accepted = False
                continuation_field_name = _TASK_COMMENT_TEXT_FIELD
            else:
                continuation_accepted = False
                continuation_field_name = _TASK_COMMENT_FINAL_CONFIRM_FIELD
        else:
            continuation = None
            manual_start_at_date_iso = None
            manual_start_at_time_hhmm = ""
            if missing_field == "start_at_date":
                existing_date_iso = str(_command_value(payload, "start_at_date", "start_date", "date") or "").strip()
                source_for_existing_date = str(
                    payload.get("__source_text")
                    or _command_value(payload, "text")
                    or ""
                ).strip()
                if existing_date_iso and (
                    has_explicit_user_date(source_for_existing_date)
                    or _extract_explicit_date_from_text(source_for_existing_date)
                ):
                    manual_start_at_date_iso = existing_date_iso
                normalized_date_reuse = _normalize_global_control_text(resolved_text)
                if (
                    _is_meeting_update_intent(session_intent)
                    and normalized_date_reuse in {"эта же дата", "та же дата", "ту же дату", "эту же дату"}
                ):
                    manual_start_at_date_iso = str(
                        _meeting_update_source_value(payload, "start_at_date", "meeting_update_source_date", "date") or ""
                    ).strip()
                if not manual_start_at_date_iso:
                    manual_start_at_date_iso = _normalize_clarification_date_to_iso(
                        resolved_text,
                        prefer_future_for_ambiguous=False,
                    )
            if missing_field == "start_at_time":
                manual_start_at_time_hhmm = _normalize_time_hhmm_local(resolved_text)
            if manual_start_at_date_iso:
                command = _inject_start_at_date_into_command(payload, manual_start_at_date_iso)
                continuation_accepted = True
                continuation_field_name = "start_at_date"
            elif manual_start_at_time_hhmm:
                command = _inject_start_at_time_into_command(payload, manual_start_at_time_hhmm)
                continuation_accepted = True
                continuation_field_name = "start_at_time"
            else:
                continuation = resolve_clarification_continuation(missing_field, resolved_text, payload)
                command = continuation.command if isinstance(continuation.command, dict) else dict(payload)
                continuation_accepted = bool(continuation.accepted)
                continuation_field_name = str(continuation.field_name or missing_field or "clarification_value")
        edited_temporal_fields = {"start_at_date", "start_at_time", "duration_minutes"}
        edited_field_applied = (
            continuation_accepted
            and (
                missing_field in edited_temporal_fields
                or (
                    missing_field == _TEMPORAL_EDIT_FIELD
                    and str(continuation_field_name or "") in edited_temporal_fields
                )
            )
        )
        temporal_intent_hint = str(command.get("intent") or active_session.get("intent") or "")
        if edited_field_applied and _is_temporal_intent_like(temporal_intent_hint):
            command["__edited_temporal_draft"] = True
        if continuation_accepted and str(continuation_field_name or "") in edited_temporal_fields:
            command.pop(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY, None)
        if continuation_accepted:
            command.pop("__temporal_summary_signature", None)
        session_intent = str(active_session.get("intent") or "").strip() if isinstance(active_session, dict) else ""
        if _is_meeting_update_intent(session_intent):
            incoming_before_lock = str(command.get("intent") or "").strip()
            command["intent"] = "meeting.update"
            LOG.info(
                "meeting_update_intent_locked",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "active_intent": session_intent,
                    "incoming_intent": incoming_before_lock,
                    "locked_intent": "meeting.update",
                },
            )
        elif _is_meeting_comment_update_intent(session_intent):
            command["intent"] = "meeting.comment.update"
        elif _is_task_comment_update_intent(session_intent):
            command["intent"] = "task.comment.update"
        if _is_confirm_field(missing_field) and session_intent:
            incoming_intent = str(command.get("intent") or "").strip()
            LOG.info(
                "confirm_intent_from_session",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "session_intent": session_intent,
                    "incoming_intent": incoming_intent,
                    "missing_field": missing_field,
                },
            )
            if incoming_intent and incoming_intent != session_intent:
                LOG.warning(
                    "mismatch_guard",
                    extra={
                        "request_id": request_id,
                        "flow_id": request_id,
                        "session_intent": session_intent,
                        "incoming_intent": incoming_intent,
                        "missing_field": missing_field,
                    },
                )
            command["intent"] = session_intent
            LOG.info(
                "confirm_intent_used",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "intent": session_intent,
                    "missing_field": missing_field,
                },
            )
        command = _attach_source_identity(
            command,
            channel=channel,
            chat_id=chat_id,
            source_message_id=incoming_message_id,
            source_text=source_text,
        )
        if not continuation_accepted:
            reask_field = str(continuation_field_name or missing_field or "clarification_value")
            persisted_missing_field = normalize_clarification_field_name(reask_field)
            intent = str(command.get("intent") or active_session.get("intent") or "unknown")
            if _is_meeting_update_intent(intent) and persisted_missing_field == _MEETING_UPDATE_TARGET_CONFIRM_FIELD:
                q = _meeting_update_target_confirmation_question(command)
            elif _is_meeting_update_intent(intent) and persisted_missing_field == _MEETING_UPDATE_TARGET_SELECT_FIELD:
                q = _meeting_update_render_candidate_shortlist(_meeting_update_candidate_shortlist_from_command(command))
            elif _is_meeting_comment_update_intent(intent) and persisted_missing_field == _MEETING_COMMENT_TARGET_CONFIRM_FIELD:
                q = _clarification_question_for_field(_MEETING_COMMENT_TARGET_CONFIRM_FIELD)
            elif _is_meeting_comment_update_intent(intent) and persisted_missing_field == _MEETING_COMMENT_TARGET_SELECT_FIELD:
                q = _meeting_comment_render_candidate_shortlist(_meeting_comment_candidate_shortlist_from_command(command))
            elif _is_meeting_comment_update_intent(intent) and persisted_missing_field == _MEETING_COMMENT_FINAL_CONFIRM_FIELD:
                q = _meeting_comment_final_confirmation_question(command)
            elif _is_task_comment_update_intent(intent) and persisted_missing_field == _TASK_COMMENT_TARGET_SELECT_FIELD:
                q = _task_comment_render_candidate_shortlist(_task_comment_candidate_shortlist_from_command(command))
            elif _is_task_comment_update_intent(intent) and persisted_missing_field == _TASK_COMMENT_FINAL_CONFIRM_FIELD:
                q = _task_comment_final_confirmation_question(command)
            elif _is_task_comment_update_intent(intent) and persisted_missing_field == _TASK_COMMENT_TARGET_SELECT_FIELD:
                q = _task_comment_render_candidate_shortlist(_task_comment_candidate_shortlist_from_command(command))
            else:
                q = _temporal_clarification_question_for_command(reask_field, command)
            key = resolve_idempotency_key(
                command=command,
                channel=channel,
                chat_id=chat_id,
                source_message_id=source_message_id,
                active_session_key=active_session_idempotency_key,
            )
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text,
                command=command,
                question=q,
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": reask_field},
                missing_field_for_log=persisted_missing_field,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent=intent,
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=key,
            )
        if "intent" not in command:
            command["intent"] = str(active_session.get("intent") or "unknown")
        original_text = str(command.get("__source_text") or "").strip()
        query_text = original_text or resolved_text
    else:
        # 2) LLM parse.
        try:
            llm_res = llm_client.parse(text=resolved_text)
        except TimeoutError:
            return {
                "outcome": "llm_timeout",
                "user_message": map_failure_to_user_message("llm_timeout", details={}),
                "command": None,
            }
        except Exception:
            return {
                "outcome": "llm_invalid_output",
                "user_message": map_failure_to_user_message("llm_invalid_output", details={}),
                "command": None,
            }

        if not isinstance(llm_res, dict):
            return {
                "outcome": "llm_invalid_output",
                "user_message": map_failure_to_user_message("llm_invalid_output", details={}),
                "command": None,
            }

        # "command" here is whatever structured output the app expects later.
        command = llm_res
        LOG.info(
            "temporal_parse_result",
            extra={
                "request_id": request_id,
                "flow_id": request_id,
                "intent": str(command.get("intent") or ""),
                "raw_start_at_date": str(_command_value(command, "start_at_date", "start_date", "date") or ""),
                "raw_start_at_time": str(_command_value(command, "start_at_time", "start_time", "time") or ""),
                "raw_start_at": str(_command_value(command, "start_at") or ""),
                "source_text": str(resolved_text or ""),
            },
        )
        command = _attach_source_identity(
            command,
            channel=channel,
            chat_id=chat_id,
            source_message_id=incoming_message_id,
            source_text=resolved_text,
        )
        if _looks_like_meeting_update_request(resolved_text):
            entities = command.get("entities")
            entities = dict(entities) if isinstance(entities, dict) else {}
            if not str(entities.get("text") or "").strip():
                entities["text"] = resolved_text
            inferred_kind = _extract_meeting_kind_from_text(resolved_text)
            if inferred_kind:
                entities["meeting_kind"] = inferred_kind
                command["meeting_kind"] = inferred_kind
            command["entities"] = entities
            command["intent"] = "meeting.update"
        if _looks_like_meeting_comment_update_request(resolved_text):
            entities = command.get("entities")
            entities = dict(entities) if isinstance(entities, dict) else {}
            if not str(entities.get("text") or "").strip():
                entities["text"] = resolved_text
            inferred_kind = _extract_meeting_kind_from_text(resolved_text)
            if inferred_kind:
                entities["meeting_kind"] = inferred_kind
                command["meeting_kind"] = inferred_kind
            comment_text = _extract_comment_text_from_source(resolved_text)
            if comment_text and not str(entities.get("comment_text") or "").strip():
                entities["comment_text"] = comment_text
            target_hint = _extract_meeting_comment_target_hint(resolved_text)
            if target_hint and not str(entities.get("target_hint") or "").strip():
                entities["target_hint"] = target_hint
            command["entities"] = entities
            command["intent"] = "meeting.comment.update"
        if _looks_like_task_comment_update_request(resolved_text):
            entities = command.get("entities")
            entities = dict(entities) if isinstance(entities, dict) else {}
            if not str(entities.get("text") or "").strip():
                entities["text"] = resolved_text
            comment_text = _extract_comment_text_from_source(resolved_text)
            if comment_text and not str(entities.get("comment_text") or "").strip():
                entities["comment_text"] = comment_text
            target_hint = _normalize_task_target_hint(resolved_text)
            if target_hint and not str(entities.get("task_ref") or "").strip():
                entities["task_ref"] = target_hint
            command["entities"] = entities
            command["intent"] = "task.comment.update"

    command = _normalize_temporal_intent_alias(command)
    command = _apply_default_duration_for_meeting_create(command)
    command = _hydrate_temporal_datetime_fields(command)
    command = _normalize_start_date_fields(command)
    LOG.info(
        "temporal_normalized",
        extra={
            "request_id": request_id,
            "flow_id": request_id,
            "intent": str(command.get("intent") or ""),
            "normalized_start_at_date": str(_command_value(command, "start_at_date", "start_date", "date") or ""),
            "normalized_start_at_time": str(_command_value(command, "start_at_time", "start_time", "time") or ""),
            "normalized_start_at": str(_command_value(command, "start_at") or ""),
        },
    )
    source_for_date_materialization = str(
        command.get("__source_text")
        or _command_value(command, "text")
        or (query_text if from_clarification else resolved_text)
        or ""
    ).strip()
    command = _materialize_temporal_date_in_entities(
        command,
        source_text=source_for_date_materialization,
        request_id=request_id,
    )
    has_explicit_date_in_text = bool(
        has_explicit_user_date(source_for_date_materialization)
        or _extract_explicit_date_from_text(source_for_date_materialization)
    )
    entities_after_materialize = command.get("entities")
    entities_after_materialize = entities_after_materialize if isinstance(entities_after_materialize, dict) else {}
    entity_date_after_materialize = str(
        _command_value({"entities": entities_after_materialize}, "start_at_date", "start_date", "date") or ""
    ).strip()
    if has_explicit_date_in_text and not entity_date_after_materialize:
        LOG.warning(
            "temporal_date_materialization_anomaly",
            extra={
                "request_id": request_id,
                "flow_id": request_id,
                "intent": str(command.get("intent") or ""),
                "source_text": source_for_date_materialization,
                "missing_entities_start_at_date": True,
            },
        )
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text if from_clarification else resolved_text,
            command=command,
            question=_temporal_clarification_question_for_command("start_at_date", command),
            rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": "start_at_date"},
            missing_field_for_log="start_at_date",
            db=db,
            context_key=ctx_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent=str(command.get("intent") or "create_timeblock"),
            payload=command,
            source_message_id=source_message_id,
            idempotency_key=resolve_idempotency_key(
                command=command,
                channel=channel,
                chat_id=chat_id,
                source_message_id=source_message_id,
                active_session_key=active_session_idempotency_key,
            ),
        )
    command = _preserve_meeting_update_parsed_changes(command)
    intent_probe = str(command.get("intent") or "").strip()
    if _is_meeting_update_intent(intent_probe) and not _meeting_update_has_source(command):
        source_for_anchor = str(
            command.get("__source_text")
            or _command_value(command, "text")
            or (query_text if from_clarification else resolved_text)
            or ""
        ).strip()
        anchor_detected, anchor_type = _detect_meeting_update_target_anchor(command, source_for_anchor)
        LOG.info(
            "meeting_update_target_anchor_guard",
            extra={
                "request_id": request_id,
                "flow_id": request_id,
                "update_target_anchor_detected": bool(anchor_detected),
                "update_target_anchor_type": str(anchor_type or ""),
                "update_guard_blocked_no_target_anchor": not bool(anchor_detected),
            },
        )
        if not anchor_detected:
            guard_question = "Какое именно событие нужно изменить? Укажите дату/время или другой ориентир."
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question=guard_question,
                rec_payload={
                    "outcome": "needs_clarification",
                    "needs_clarification": True,
                    "missing_field": "meeting_update_target_ref",
                },
                missing_field_for_log="meeting_update_target_ref",
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent="meeting.update",
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=resolve_idempotency_key(
                    command=command,
                    channel=channel,
                    chat_id=chat_id,
                    source_message_id=source_message_id,
                    active_session_key=active_session_idempotency_key,
                ),
            )
    if (
        _is_meeting_update_intent(intent_probe)
        and _meeting_update_has_source(command)
        and not bool(command.get("__meeting_update_target_confirmed"))
    ):
        source_event_id = _meeting_update_target_event_id(command)
        source_date = _meeting_update_source_value(command, "meeting_update_source_date")
        source_time = _meeting_update_source_value(command, "meeting_update_source_time")
        source_duration = _meeting_update_source_value(command, "meeting_update_source_duration")
        q = _meeting_update_target_confirmation_question(command)
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text if from_clarification else resolved_text,
            command=command,
            question=q,
            rec_payload={
                "outcome": "needs_clarification",
                "needs_clarification": True,
                "missing_field": _MEETING_UPDATE_TARGET_CONFIRM_FIELD,
            },
            missing_field_for_log=_MEETING_UPDATE_TARGET_CONFIRM_FIELD,
            db=db,
            context_key=ctx_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent=str(command.get("intent") or "meeting.update"),
            payload=command,
            source_message_id=source_message_id,
            idempotency_key=resolve_idempotency_key(
                command=command,
                channel=channel,
                chat_id=chat_id,
                source_message_id=source_message_id,
                active_session_key=active_session_idempotency_key,
            ),
        )
    safety = resolve_temporal_execution_decision(command, query_text if from_clarification else resolved_text)
    command = safety.command if isinstance(safety.command, dict) else command
    meeting_update_without_target = _is_meeting_update_intent(str(command.get("intent") or "")) and not _meeting_update_source_is_hydrated(command)
    if (
        bool(safety.is_temporal)
        and not bool(command.get("__past_date_confirmed"))
        and not meeting_update_without_target
        and _is_past_start_date(command)
    ):
        q = _clarification_question_for_field(_PAST_DATE_CONFIRM_FIELD)
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text if from_clarification else resolved_text,
            command=command,
            question=q,
            rec_payload={
                "outcome": "needs_clarification",
                "needs_clarification": True,
                "missing_field": _PAST_DATE_CONFIRM_FIELD,
            },
            missing_field_for_log=_PAST_DATE_CONFIRM_FIELD,
            db=db,
            context_key=ctx_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent=str(command.get("intent") or "create_timeblock"),
            payload=command,
            source_message_id=source_message_id,
            idempotency_key=resolve_idempotency_key(
                command=command,
                channel=channel,
                chat_id=chat_id,
                source_message_id=source_message_id,
                active_session_key=active_session_idempotency_key,
            ),
        )
    idempotency_key = resolve_idempotency_key(
        command=command,
        channel=channel,
        chat_id=chat_id,
        source_message_id=source_message_id,
        active_session_key=active_session_idempotency_key,
    )
    if _is_meeting_update_intent(str(command.get("intent") or "")) and not _meeting_update_source_is_hydrated(command):
        search_query_raw = str(command.get("__meeting_update_target_hint") or "").strip()
        if not search_query_raw:
            search_query_raw = str(
                command.get("__source_text")
                or _command_value(command, "text")
                or (query_text if from_clarification else resolved_text)
                or ""
            ).strip()
        search_query = _normalize_meeting_target_hint(search_query_raw) or search_query_raw
        search_meeting_kind = _resolve_meeting_kind(command)
        if search_meeting_kind in _MEETING_KIND_VARIANTS:
            command["meeting_kind"] = search_meeting_kind
        command["__meeting_update_target_hint"] = search_query
        try:
            auto_search = _meeting_update_repeat_search(
                str(user_id or "").strip(),
                search_query,
                search_meeting_kind,
            )
        except TypeError:
            auto_search = _meeting_update_repeat_search(str(user_id or "").strip(), search_query)
        candidates = auto_search.get("candidates") if isinstance(auto_search.get("candidates"), list) else []
        candidate = auto_search.get("candidate") if isinstance(auto_search.get("candidate"), dict) else None
        if bool(auto_search.get("ok")) and isinstance(candidate, dict):
            command = _meeting_update_hydrate_from_candidate(command, candidate)
            q = _meeting_update_target_confirmation_question(command)
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question=q,
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _MEETING_UPDATE_TARGET_CONFIRM_FIELD},
                missing_field_for_log=_MEETING_UPDATE_TARGET_CONFIRM_FIELD,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent="meeting.update",
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )
        if str(auto_search.get("reason") or "").strip().lower() == "multiple" and candidates:
            shortlist = [dict(c) for c in candidates if isinstance(c, dict)][:_MEETING_UPDATE_SHORTLIST_MAX]
            command = _meeting_update_store_candidate_shortlist(command, shortlist)
            q = _meeting_update_render_candidate_shortlist(shortlist)
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question=q,
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _MEETING_UPDATE_TARGET_SELECT_FIELD},
                missing_field_for_log=_MEETING_UPDATE_TARGET_SELECT_FIELD,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent="meeting.update",
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )
        LOG.info(
            "meeting_update_source_missing_guard",
            extra={
                "request_id": request_id,
                "flow_id": request_id,
                "calendar_event_id": str(_meeting_update_target_event_id(command) or ""),
                "draft_date": str(_meeting_update_source_value(command, "start_at_date", "date") or ""),
                "draft_time": str(_meeting_update_source_value(command, "start_at_time", "time", "start_time") or ""),
                "draft_duration": str(_meeting_update_source_value(command, "duration_minutes", "duration_min") or ""),
                "target_search_input": str(command.get("__meeting_update_target_hint") or ""),
            },
        )
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text if from_clarification else resolved_text,
            command=command,
            question=_clarification_question_for_field(_MEETING_UPDATE_TARGET_REFINE_FIELD),
            rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _MEETING_UPDATE_TARGET_REFINE_FIELD},
            missing_field_for_log=_MEETING_UPDATE_TARGET_REFINE_FIELD,
            db=db,
            context_key=ctx_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent="meeting.update",
            payload=command,
            source_message_id=source_message_id,
            idempotency_key=idempotency_key,
        )
    if _is_meeting_comment_update_intent(str(command.get("intent") or "")):
        entities = command.get("entities")
        entities = dict(entities) if isinstance(entities, dict) else {}
        source_text_for_comment = str(
            command.get("__source_text")
            or _command_value(command, "text")
            or (query_text if from_clarification else resolved_text)
            or ""
        ).strip()
        existing_comment = str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip()
        if existing_comment and not str(command.get("comment_text") or "").strip():
            command["comment_text"] = existing_comment
            entities["comment_text"] = existing_comment
            command["entities"] = entities
        inferred_comment = _extract_comment_text_from_source(source_text_for_comment)
        if inferred_comment and not str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip():
            command["comment_text"] = inferred_comment
            entities["comment_text"] = inferred_comment
            command["entities"] = entities

        if not str(_command_value(command, "calendar_event_id") or "").strip():
            search_query = str(_command_value(command, "target_hint") or command.get("__meeting_comment_target_hint") or "").strip()
            if not search_query:
                search_query = _extract_meeting_comment_target_hint(source_text_for_comment) or source_text_for_comment
            command["__meeting_comment_target_hint"] = search_query
            search_kind = _resolve_meeting_kind(command)
            search_result = _meeting_comment_repeat_search(str(user_id or "").strip(), search_query, search_kind)
            candidates = search_result.get("candidates") if isinstance(search_result.get("candidates"), list) else []
            candidate = search_result.get("candidate") if isinstance(search_result.get("candidate"), dict) else None
            if bool(search_result.get("ok")) and isinstance(candidate, dict):
                command = _meeting_comment_hydrate_from_candidate(command, candidate)
                return _return_clarification_with_persist(
                    request_id=request_id,
                    query_text=query_text if from_clarification else resolved_text,
                    command=command,
                    question=_meeting_comment_target_confirmation_question(command),
                    rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _MEETING_COMMENT_TARGET_CONFIRM_FIELD},
                    missing_field_for_log=_MEETING_COMMENT_TARGET_CONFIRM_FIELD,
                    db=db,
                    context_key=ctx_key,
                    app_id=app_id,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    intent="meeting.comment.update",
                    payload=command,
                    source_message_id=source_message_id,
                    idempotency_key=idempotency_key,
                )
            if str(search_result.get("reason") or "").strip().lower() == "multiple" and candidates:
                command = _meeting_comment_store_candidate_shortlist(
                    command,
                    [dict(c) for c in candidates if isinstance(c, dict)],
                )
                return _return_clarification_with_persist(
                    request_id=request_id,
                    query_text=query_text if from_clarification else resolved_text,
                    command=command,
                    question=_meeting_comment_render_candidate_shortlist(_meeting_comment_candidate_shortlist_from_command(command)),
                    rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _MEETING_COMMENT_TARGET_SELECT_FIELD},
                    missing_field_for_log=_MEETING_COMMENT_TARGET_SELECT_FIELD,
                    db=db,
                    context_key=ctx_key,
                    app_id=app_id,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    intent="meeting.comment.update",
                    payload=command,
                    source_message_id=source_message_id,
                    idempotency_key=idempotency_key,
                )
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question="Уточните, какую встречу нужно изменить.",
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": "meeting_comment_target_ref"},
                missing_field_for_log="meeting_comment_target_ref",
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent="meeting.comment.update",
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )
        if not bool(command.get("__meeting_comment_target_confirmed")):
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question=_meeting_comment_target_confirmation_question(command),
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _MEETING_COMMENT_TARGET_CONFIRM_FIELD},
                missing_field_for_log=_MEETING_COMMENT_TARGET_CONFIRM_FIELD,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent="meeting.comment.update",
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )
        if not str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip():
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question="Какой комментарий добавить во встречу?",
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _MEETING_COMMENT_TEXT_FIELD},
                missing_field_for_log=_MEETING_COMMENT_TEXT_FIELD,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent="meeting.comment.update",
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )
        if not bool(command.get("__comment_update_confirmed")):
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question=_meeting_comment_final_confirmation_question(command),
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _MEETING_COMMENT_FINAL_CONFIRM_FIELD},
                missing_field_for_log=_MEETING_COMMENT_FINAL_CONFIRM_FIELD,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent="meeting.comment.update",
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )
    if _is_task_comment_update_intent(str(command.get("intent") or "")):
        source_text_for_comment = str(
            command.get("__source_text")
            or _command_value(command, "text")
            or (query_text if from_clarification else resolved_text)
            or ""
        ).strip()
        entities = command.get("entities")
        entities = dict(entities) if isinstance(entities, dict) else {}
        existing_comment = str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip()
        if existing_comment and not str(command.get("comment_text") or "").strip():
            command["comment_text"] = existing_comment
            entities["comment_text"] = existing_comment
            command["entities"] = entities
        inferred_comment = _extract_comment_text_from_source(source_text_for_comment)
        if inferred_comment and not str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip():
            command["comment_text"] = inferred_comment
            entities["comment_text"] = inferred_comment
            command["entities"] = entities
        if not str(_command_value(command, "task_id") or "").strip():
            target_hint = str(_command_value(command, "task_ref", "target_hint") or command.get("__task_comment_target_hint") or "").strip()
            if not target_hint:
                target_hint = _normalize_task_target_hint(source_text_for_comment) or source_text_for_comment
            command["__task_comment_target_hint"] = target_hint
            search_result = _task_comment_repeat_search(str(user_id or "").strip(), target_hint)
            candidates = search_result.get("candidates") if isinstance(search_result.get("candidates"), list) else []
            candidate = search_result.get("candidate") if isinstance(search_result.get("candidate"), dict) else None
            if bool(search_result.get("ok")) and isinstance(candidate, dict):
                command = _task_comment_hydrate_from_candidate(command, candidate)
                return _return_clarification_with_persist(
                    request_id=request_id,
                    query_text=query_text if from_clarification else resolved_text,
                    command=command,
                    question=_task_comment_target_confirmation_question(command),
                    rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _TASK_COMMENT_TARGET_CONFIRM_FIELD},
                    missing_field_for_log=_TASK_COMMENT_TARGET_CONFIRM_FIELD,
                    db=db,
                    context_key=ctx_key,
                    app_id=app_id,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    intent="task.comment.update",
                    payload=command,
                    source_message_id=source_message_id,
                    idempotency_key=idempotency_key,
                )
            if str(search_result.get("reason") or "").strip().lower() == "multiple" and candidates:
                command = _task_comment_store_candidate_shortlist(
                    command,
                    [dict(c) for c in candidates if isinstance(c, dict)],
                )
                return _return_clarification_with_persist(
                    request_id=request_id,
                    query_text=query_text if from_clarification else resolved_text,
                    command=command,
                    question=_task_comment_render_candidate_shortlist(_task_comment_candidate_shortlist_from_command(command)),
                    rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _TASK_COMMENT_TARGET_SELECT_FIELD},
                    missing_field_for_log=_TASK_COMMENT_TARGET_SELECT_FIELD,
                    db=db,
                    context_key=ctx_key,
                    app_id=app_id,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    intent="task.comment.update",
                    payload=command,
                    source_message_id=source_message_id,
                    idempotency_key=idempotency_key,
                )
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question="Не нашёл задачу. Уточните, какую задачу нужно изменить.",
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": "task_comment_target_ref"},
                missing_field_for_log="task_comment_target_ref",
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent="task.comment.update",
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )
        if not bool(command.get("__task_comment_target_confirmed")):
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question=_task_comment_target_confirmation_question(command),
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _TASK_COMMENT_TARGET_CONFIRM_FIELD},
                missing_field_for_log=_TASK_COMMENT_TARGET_CONFIRM_FIELD,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent="task.comment.update",
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )
        if not str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip():
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question="Какой комментарий добавить к задаче?",
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _TASK_COMMENT_TEXT_FIELD},
                missing_field_for_log=_TASK_COMMENT_TEXT_FIELD,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent="task.comment.update",
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )
        if not bool(command.get("__comment_update_confirmed")):
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question=_task_comment_final_confirmation_question(command),
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _TASK_COMMENT_FINAL_CONFIRM_FIELD},
                missing_field_for_log=_TASK_COMMENT_FINAL_CONFIRM_FIELD,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent="task.comment.update",
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )
    if safety.missing_field:
        missing_field = normalize_clarification_field_name(str(safety.missing_field))
        if missing_field == "start_at_date":
            source_for_date_guard = str(
                command.get("__source_text")
                or _command_value(command, "text")
                or (query_text if from_clarification else resolved_text)
                or ""
            ).strip()
            has_date_in_text = bool(
                has_explicit_user_date(source_for_date_guard)
                or _extract_explicit_date_from_text(source_for_date_guard)
            )
            entity_date = str(_command_value(command, "start_at_date", "start_date", "date") or "").strip()
            draft = command.get("__temporal_draft")
            draft = draft if isinstance(draft, dict) else {}
            draft_date = str(draft.get("date") or "").strip()
            if (has_date_in_text and entity_date) or draft_date:
                inferred_missing = _next_temporal_missing_field(command)
                if inferred_missing and inferred_missing != "start_at_date":
                    missing_field = inferred_missing
                elif not inferred_missing:
                    missing_field = ""
        if not missing_field:
            safety_missing_cleared = resolve_temporal_execution_decision(
                command,
                query_text if from_clarification else resolved_text,
            )
            command = safety_missing_cleared.command if isinstance(safety_missing_cleared.command, dict) else command
            safety = safety_missing_cleared
        if not missing_field:
            pass
        else:
            q = _temporal_clarification_question_for_command(
                missing_field,
                command,
            )
            LOG.info(
                "meeting_create_missing_field_selected",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "meeting_kind": str(_command_value(command, "meeting_kind", "event_kind") or ""),
                    "parsed_date": str(_command_value(command, "start_at_date", "start_date", "date") or ""),
                    "entities.start_at_date_before": "",
                    "entities.start_at_date_after": str(
                        _command_value({"entities": command.get("entities") if isinstance(command.get("entities"), dict) else {}}, "start_at_date", "start_date", "date")
                        or ""
                    ),
                    "missing_field_selected": missing_field,
                },
            )
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question=q,
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": missing_field},
                missing_field_for_log=missing_field,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent=str(command.get("intent") or "create_timeblock"),
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )

    intent = _normalized_intent(command)
    force_execution_route = False
    temporal_ready = bool(safety.is_temporal and not str(safety.missing_field or "").strip())
    if _is_meeting_update_intent(intent) and not _meeting_update_source_is_hydrated(command):
        LOG.info(
            "meeting_update_source_missing_guard",
            extra={
                "request_id": request_id,
                "flow_id": request_id,
                "calendar_event_id": str(_meeting_update_target_event_id(command) or ""),
                "draft_date": str(_meeting_update_source_value(command, "start_at_date", "date") or ""),
                "draft_time": str(_meeting_update_source_value(command, "start_at_time", "time", "start_time") or ""),
                "draft_duration": str(_meeting_update_source_value(command, "duration_minutes", "duration_min") or ""),
                "target_search_input": str(command.get("__meeting_update_target_hint") or ""),
            },
        )
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text if from_clarification else resolved_text,
            command=command,
            question=_clarification_question_for_field(_MEETING_UPDATE_TARGET_REFINE_FIELD),
            rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _MEETING_UPDATE_TARGET_REFINE_FIELD},
            missing_field_for_log=_MEETING_UPDATE_TARGET_REFINE_FIELD,
            db=db,
            context_key=ctx_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent="meeting.update",
            payload=command,
            source_message_id=source_message_id,
            idempotency_key=idempotency_key,
        )
    temporal_confirm_ready = temporal_ready and not bool(command.get("__temporal_confirmed"))
    if temporal_confirm_ready:
        command = _attach_temporal_draft(command)
        draft = command.get("__temporal_draft")
        draft = draft if isinstance(draft, dict) else {}
        LOG.info(
            "temporal_draft_ready",
            extra={
                "request_id": request_id,
                "flow_id": request_id,
                "intent": str(command.get("intent") or ""),
                "draft_date": str(draft.get("date") or ""),
                "draft_time": str(draft.get("time") or ""),
                "draft_duration": str(draft.get("duration_minutes") or ""),
            },
        )
        summary_signature = _temporal_summary_signature(command)
        previous_signature = str(command.get("__temporal_summary_signature") or "").strip()
        if previous_signature and previous_signature == summary_signature and from_clarification:
            LOG.info(
                "summary_render_skipped_duplicate",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "summary_signature": summary_signature,
                },
            )
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text,
                command=command,
                question="Подтвердите действие: Да или Нет.",
                rec_payload={
                    "outcome": "needs_clarification",
                    "needs_clarification": True,
                    "missing_field": _TEMPORAL_CONFIRM_FIELD,
                },
                missing_field_for_log=_TEMPORAL_CONFIRM_FIELD,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent=str(command.get("intent") or "create_timeblock"),
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )
        command["__temporal_summary_signature"] = summary_signature
        if from_clarification and bool(command.get("__edited_temporal_draft")):
            LOG.info(
                "summary_redisplayed",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "session_state": _TEMPORAL_CONFIRM_FIELD,
                    "scenario": "temporal_create",
                    "has_draft": bool(draft),
                    "draft_date": str(draft.get("date") or ""),
                    "draft_time": str(draft.get("time") or ""),
                    "draft_duration": str(draft.get("duration_minutes") or ""),
                },
            )
        q = _temporal_confirmation_question(command)
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text,
            command=command,
            question=q,
            rec_payload={
                "outcome": "needs_clarification",
                "needs_clarification": True,
                "missing_field": _TEMPORAL_CONFIRM_FIELD,
            },
            missing_field_for_log=_TEMPORAL_CONFIRM_FIELD,
            db=db,
            context_key=ctx_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent=str(command.get("intent") or "create_timeblock"),
            payload=command,
            source_message_id=source_message_id,
            idempotency_key=idempotency_key,
        )
    if temporal_ready:
        force_execution_route = True
    elif _is_non_temporal_shortcut_intent(intent):
        if not safety.is_temporal and _is_command_complete_for_execution(command):
            force_execution_route = True
        else:
            missing_field = normalize_clarification_field_name(active_missing_field or "clarification_value")
            inferred_missing_field = _next_temporal_missing_field(command)
            if inferred_missing_field:
                missing_field = inferred_missing_field
            q = _temporal_clarification_question_for_command(missing_field, command)
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text,
                command=command,
                question=q,
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": missing_field},
                missing_field_for_log=missing_field,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent=str(
                    command.get("intent")
                    or (active_session.get("intent") if isinstance(active_session, dict) else "")
                    or "unknown"
                ),
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )

    task_create_ready = (
        _is_task_create_intent(intent)
        and _is_command_complete_for_execution(command)
        and not bool(command.get("__task_create_confirmed"))
    )
    if task_create_ready:
        command = _attach_task_create_draft(command)
        q = _task_create_confirmation_question(command)
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text,
            command=command,
            question=q,
            rec_payload={
                "outcome": "needs_clarification",
                "needs_clarification": True,
                "missing_field": _TASK_CREATE_CONFIRM_FIELD,
            },
            missing_field_for_log=_TASK_CREATE_CONFIRM_FIELD,
            db=db,
            context_key=ctx_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent=str(command.get("intent") or "task.create"),
            payload=command,
            source_message_id=source_message_id,
            idempotency_key=idempotency_key,
        )
    if (
        _is_meeting_comment_update_intent(intent)
        and bool(command.get("__comment_update_confirmed"))
        and bool(command.get("__meeting_comment_target_confirmed"))
        and str(_command_value(command, "calendar_event_id") or "").strip()
        and str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip()
    ):
        force_execution_route = True
    if (
        _is_task_comment_update_intent(intent)
        and bool(command.get("__comment_update_confirmed"))
        and bool(command.get("__task_comment_target_confirmed"))
        and str(_command_value(command, "task_id") or "").strip()
        and str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip()
    ):
        force_execution_route = True

    LOG.info(
        "runtime_route_choice",
        extra={
            "query_text": query_text,
            "from_clarification": from_clarification,
            "llm_called": not from_clarification,
            "command": command,
            "command.intent": command.get("intent") if isinstance(command, dict) else None,
            "command.entities": command.get("entities") if isinstance(command, dict) else None,
            "command.parse_source": command.get("__parse_source") if isinstance(command, dict) else None,
            "command.parse_reason": command.get("__parse_reason") if isinstance(command, dict) else None,
            "route_target": "execution" if force_execution_route else "rec",
        },
    )

    dedup_reserved = False
    dedup_reclaimed = False
    dedup_ttl = max(1, int(command_dedup_reservation_ttl_sec))
    if db is not None and idempotency_key:
        existing = db.get_command_dedup(idempotency_key)
        if existing is not None:
            status = str(existing.get("status") or "completed").strip().lower()
            if status == "completed":
                if from_clarification:
                    db.delete_clarification_session(ctx_key)
                return {
                    "outcome": "duplicate",
                    "duplicate_state": "completed",
                    "user_message": map_failure_to_user_message("success", details={}),
                    "command": command,
                    "duplicate_of": existing,
                    "entity_type": existing.get("entity_type", ""),
                    "entity_id": existing.get("entity_id", ""),
                }
            if status == "reserved" and not db.is_command_dedup_stale(existing, ttl_seconds=dedup_ttl):
                if from_clarification:
                    db.delete_clarification_session(ctx_key)
                return {
                    "outcome": "duplicate",
                    "duplicate_state": "in_flight",
                    "user_message": map_failure_to_user_message("success", details={}),
                    "command": command,
                    "duplicate_of": existing,
                    "entity_type": existing.get("entity_type", ""),
                    "entity_id": existing.get("entity_id", ""),
                }
            if status == "reserved":
                dedup_reserved = db.reclaim_command_dedup(
                    idempotency_key=idempotency_key,
                    intent=str(command.get("intent") or "unknown"),
                )
                dedup_reclaimed = bool(dedup_reserved)
                if not dedup_reserved:
                    current = db.get_command_dedup(idempotency_key) or {}
                    if from_clarification:
                        db.delete_clarification_session(ctx_key)
                    return {
                        "outcome": "duplicate",
                        "duplicate_state": "in_flight",
                        "user_message": map_failure_to_user_message("success", details={}),
                        "command": command,
                        "duplicate_of": current,
                        "entity_type": str(current.get("entity_type") or ""),
                        "entity_id": str(current.get("entity_id") or ""),
                    }
            else:
                if from_clarification:
                    db.delete_clarification_session(ctx_key)
                return {
                    "outcome": "duplicate",
                    "duplicate_state": "completed",
                    "user_message": map_failure_to_user_message("success", details={}),
                    "command": command,
                    "duplicate_of": existing,
                    "entity_type": existing.get("entity_type", ""),
                    "entity_id": existing.get("entity_id", ""),
                }
        if not dedup_reserved:
            dedup_reserved = db.reserve_command_dedup(
                idempotency_key=idempotency_key,
                intent=str(command.get("intent") or "unknown"),
            )
            if not dedup_reserved:
                existing = db.get_command_dedup(idempotency_key) or {}
                status = str(existing.get("status") or "completed").strip().lower()
                if from_clarification:
                    db.delete_clarification_session(ctx_key)
                return {
                    "outcome": "duplicate",
                    "duplicate_state": "in_flight" if status == "reserved" else "completed",
                    "user_message": map_failure_to_user_message("success", details={}),
                    "command": command,
                    "duplicate_of": existing,
                    "entity_type": str(existing.get("entity_type") or ""),
                    "entity_id": str(existing.get("entity_id") or ""),
                }

    if force_execution_route:
        if bool(safety.is_temporal):
            temporal_with_draft = _attach_temporal_draft(command)
            draft = temporal_with_draft.get("__temporal_draft")
            draft = draft if isinstance(draft, dict) else {}
            LOG.info(
                "temporal_execution_start",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "user_id": user_id,
                    "draft_date": str(draft.get("date") or ""),
                    "draft_time": str(draft.get("time") or ""),
                    "draft_duration": str(draft.get("duration_minutes") or ""),
                },
            )
        if from_clarification and db is not None:
            db.delete_clarification_session(ctx_key)
        if dedup_reserved and db is not None and idempotency_key:
            entity = _extract_entity_ref({}, command)
            db.finalize_command_dedup(
                idempotency_key=idempotency_key,
                entity_type=entity.get("entity_type", ""),
                entity_id=entity.get("entity_id", ""),
            )
        return {
            "outcome": "success",
            "user_message": map_failure_to_user_message("success", details={}),
            "rec": {"outcome": "skipped", "reason": "clarification_executable_guard"},
            "command": command,
            "dedup_state": "reclaimed_stale_retried" if dedup_reclaimed else "reserved_new",
        }

    # 3) REC (only if LLM succeeded and routing requires REC).
    LOG.info(
        "runtime_pre_rec",
        extra={
            "query_text": query_text,
            "command": command,
            "intent": command.get("intent") if isinstance(command, dict) else None,
            "entities": command.get("entities") if isinstance(command, dict) else None,
            "from_clarification": from_clarification,
            "llm_called": not from_clarification,
            "parse_source": command.get("__parse_source") if isinstance(command, dict) else None,
            "parse_reason": command.get("__parse_reason") if isinstance(command, dict) else None,
        },
    )
    rec_res = rec_client.query({"text": query_text, "command": command})
    LOG.info(
        "runtime_rec_result",
        extra={
            "rec_outcome": rec_res.get("outcome") if isinstance(rec_res, dict) else None,
            "needs_clarification": rec_res.get("needs_clarification") if isinstance(rec_res, dict) else None,
            "hits_count": len(rec_res.get("hits") or []) if isinstance(rec_res, dict) else 0,
        },
    )
    if not isinstance(rec_res, dict):
        if dedup_reserved and db is not None and idempotency_key:
            db.delete_command_dedup(idempotency_key)
        if from_clarification and db is not None:
            db.delete_clarification_session(ctx_key)
        return {
            "outcome": "rec_rejected",
            "user_message": map_failure_to_user_message("rec_rejected", details={}),
            "command": None,
        }

    rec_outcome = str(rec_res.get("outcome") or "").strip().lower()
    if rec_outcome == "rejected":
        if dedup_reserved and db is not None and idempotency_key:
            db.delete_command_dedup(idempotency_key)
        if from_clarification and db is not None:
            db.delete_clarification_session(ctx_key)
        return {
            "outcome": "rec_rejected",
            "user_message": map_failure_to_user_message("rec_rejected", details=rec_res),
            "rec": rec_res,
            "command": command,
        }

    needs = bool(rec_res.get("needs_clarification")) or rec_outcome == "needs_clarification"
    if needs:
        if dedup_reserved and db is not None and idempotency_key:
            db.delete_command_dedup(idempotency_key)
        hints = rec_res.get("clarification_hints") if isinstance(rec_res.get("clarification_hints"), dict) else {}
        q = build_one_clarifying_question(hints)
        missing_field = _resolve_missing_field_for_persistence(
            rec_res=rec_res,
            command=command,
            previous_missing_field=active_missing_field,
        )
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text,
            command=command,
            question=q,
            rec_payload=rec_res,
            missing_field_for_log=missing_field,
            db=db,
            context_key=ctx_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent=str(command.get("intent") or rec_res.get("intent") or "unknown"),
            payload=command,
            source_message_id=source_message_id,
            idempotency_key=idempotency_key,
        )

    hits = rec_res.get("hits")
    hits_is_empty = isinstance(hits, list) and len(hits) == 0
    if rec_outcome == "empty" or (not rec_outcome and hits_is_empty):
        if dedup_reserved and db is not None and idempotency_key:
            db.delete_command_dedup(idempotency_key)
        if from_clarification and db is not None:
            db.delete_clarification_session(ctx_key)
        return {
            "outcome": "rec_empty",
            "user_message": map_failure_to_user_message("rec_empty", details=rec_res),
            "rec": rec_res,
            "command": command,
        }

    if from_clarification and db is not None:
        db.delete_clarification_session(ctx_key)
    if dedup_reserved and db is not None and idempotency_key:
        entity = _extract_entity_ref(rec_res, command)
        db.finalize_command_dedup(
            idempotency_key=idempotency_key,
            entity_type=entity.get("entity_type", ""),
            entity_id=entity.get("entity_id", ""),
        )

    return {
        "outcome": "success",
        "user_message": map_failure_to_user_message("success", details={}),
        "rec": rec_res,
        "command": command,
        "dedup_state": "reclaimed_stale_retried" if dedup_reclaimed else "reserved_new",
    }
