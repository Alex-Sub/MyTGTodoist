from __future__ import annotations

import logging
import os
import re
import json
import hashlib
import urllib.request
from datetime import date, datetime
from typing import Any, Dict, Optional
from common.date_resolver import extract_date_resolution, normalize_temporal_fields, resolve_date_phrase

from src.decision.clarification_rules import (
    normalize_clarification_field_name,
    resolve_clarification_continuation,
)
from src.decision.temporal_rules import (
    TemporalExecutionDecision,
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
_ROUTE_RULES_VERSION = "stage67-hardguard-v2"


def _safe_idempotency_key(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return digest[-8:]

_PAST_DATE_CONFIRM_FIELD = "start_at_date_past_confirm"
_TEMPORAL_CONFIRM_FIELD = "temporal_commit_confirm"
_TEMPORAL_EDIT_FIELD = "temporal_edit_field"
_TEMPORAL_EDIT_ACTIVE_FIELD_KEY = "__temporal_edit_active_field"
_SYNC_CONFLICT_FIELD = "sync_conflict_resolution"
_TASK_CREATE_CONFIRM_FIELD = "task_create_confirm"
_TASK_CREATE_DUPLICATE_CONFIRM_FIELD = "task_create_duplicate_confirm"
_TASK_CREATE_EDIT_FIELD = "task_create_edit"
_TASK_CREATE_DATE_FIELD = "task_create_due_date"
_TASK_CREATE_COMMENT_FIELD = "task_create_comment"
_AMBIGUOUS_CREATE_FIELD = "ambiguous_create_route"
_MEETING_UPDATE_TARGET_CONFIRM_FIELD = "awaiting_target_confirm"
_MEETING_UPDATE_TARGET_IDENTIFICATION_FIELD = "awaiting_target_identification"
_MEETING_UPDATE_TARGET_SELECT_FIELD = "meeting_update_target_select"
_MEETING_UPDATE_TARGET_REFINE_FIELD = "meeting_update_target_refine"
_MEETING_UPDATE_FINAL_CONFIRM_FIELD = "awaiting_final_confirm"
_MEETING_UPDATE_EDIT_CHOICE_FIELD = "meeting_update_edit_choice"
_MEETING_UPDATE_EDIT_DATE_FIELD = "meeting_update_edit_date"
_MEETING_UPDATE_EDIT_TIME_FIELD = "meeting_update_edit_time"
_MEETING_UPDATE_EDIT_DURATION_FIELD = "meeting_update_edit_duration"
_MEETING_UPDATE_EDIT_COMMENT_FIELD = "meeting_update_edit_comment"
_MEETING_UPDATE_EDIT_DATE_TIME_DATE_FIELD = "meeting_update_edit_date_time_date"
_MEETING_UPDATE_EDIT_DATE_TIME_TIME_FIELD = "meeting_update_edit_date_time_time"
_MEETING_UPDATE_STAGE_KEY = "__meeting_update_stage"
_MEETING_UPDATE_ACTIVE_EDIT_KEY = "__meeting_update_active_edit_field"
_MEETING_UPDATE_PENDING_COMBO_KEY = "__meeting_update_pending_date_time"
_TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD = "timeblock_update_target_confirm"
_TIMEBLOCK_UPDATE_TARGET_SELECT_FIELD = "timeblock_update_target_select"
_TIMEBLOCK_UPDATE_TARGET_REFINE_FIELD = "timeblock_update_target_refine"
_TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD = "timeblock_update_final_confirm"
_TIMEBLOCK_UPDATE_EDIT_CHOICE_FIELD = "timeblock_update_edit_choice"
_TIMEBLOCK_UPDATE_EDIT_DATE_FIELD = "timeblock_update_edit_date"
_TIMEBLOCK_UPDATE_EDIT_TIME_FIELD = "timeblock_update_edit_time"
_TIMEBLOCK_UPDATE_EDIT_DURATION_FIELD = "timeblock_update_edit_duration"
_TIMEBLOCK_UPDATE_EDIT_COMMENT_FIELD = "timeblock_update_edit_comment"
_TIMEBLOCK_UPDATE_EDIT_DATE_TIME_DATE_FIELD = "timeblock_update_edit_date_time_date"
_TIMEBLOCK_UPDATE_EDIT_DATE_TIME_TIME_FIELD = "timeblock_update_edit_date_time_time"
_TIMEBLOCK_UPDATE_ACTIVE_EDIT_KEY = "__timeblock_update_active_edit_field"
_TIMEBLOCK_UPDATE_PENDING_COMBO_KEY = "__timeblock_update_pending_date_time"
_MEETING_COMMENT_TARGET_CONFIRM_FIELD = "meeting_comment_target_confirm"
_MEETING_COMMENT_TARGET_SELECT_FIELD = "meeting_comment_target_select"
_MEETING_COMMENT_TEXT_FIELD = "meeting_comment_text"
_MEETING_COMMENT_FINAL_CONFIRM_FIELD = "meeting_comment_final_confirm"
_TASK_COMMENT_TARGET_CONFIRM_FIELD = "task_comment_target_confirm"
_TASK_COMMENT_TARGET_SELECT_FIELD = "task_comment_target_select"
_TASK_COMMENT_TEXT_FIELD = "task_comment_text"
_TASK_COMMENT_FINAL_CONFIRM_FIELD = "task_comment_final_confirm"
_SYNC_CONFLICT_ACTIONS = {"delete_db", "restore_calendar", "edit", "skip"}
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
_AMBIGUOUS_CREATE_EXACT_PHRASES = {
    "создай",
    "создать",
    "запланируй",
    "запланировать",
    "добавь",
    "добавить",
    "сделай запись",
    "сделать запись",
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
    "timeblock_update": "timeblock.update",
    "update_timeblock": "timeblock.update",
    "move_timeblock": "timeblock.update",
    "block.update": "timeblock.update",
    "timeblock.move": "timeblock.update",
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
    resolution = resolve_date_phrase(
        str(raw_text or ""),
        now=datetime.combine(today or date.today(), datetime.min.time()),
        prefer_future=True,
    )
    LOG.info(
        "temporal_date_parsed",
        extra={
            "raw_date_text": str(raw_text or "").strip(),
            "normalized_date_result": str(resolution.date or ""),
            "parser_source": parser_source,
            "resolver_reason": str(resolution.reason or ""),
            "needs_clarification": bool(resolution.needs_clarification),
        },
    )
    return resolution.date if resolution.ok else None


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
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    entities["start_at_date"] = iso_date
    time_value = _normalize_time_hhmm_local(_command_value(out, "start_at_time", "start_time", "time"))
    if time_value:
        out["start_at"] = f"{iso_date}T{time_value}"
        entities["start_at"] = f"{iso_date}T{time_value}"
    else:
        out["start_at"] = str(iso_date)
        entities["start_at"] = str(iso_date)
    out["__manual_start_at_date_override"] = iso_date
    out["entities"] = entities
    return out


def _extract_explicit_date_from_text(raw_text: Any, *, today: Optional[date] = None) -> Optional[str]:
    resolution = extract_date_resolution(
        raw_text,
        now=datetime.combine(today or date.today(), datetime.min.time()),
        prefer_future=True,
    )
    return resolution.date if resolution.ok else None


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
    manual_date_override = str(out.get("__manual_start_at_date_override") or "").strip()
    command_date_raw = str(_command_value(out, "start_at_date", "start_date", "date") or "").strip()
    if manual_date_override:
        command_date_raw = manual_date_override
    command_date = _normalize_clarification_date_to_iso(command_date_raw, prefer_future_for_ambiguous=False) or command_date_raw
    entity_date = str(_command_value({"entities": entities}, "start_at_date", "start_date", "date") or "").strip()
    # Source text date should win over stale parser year for create paths, but
    # update confirm must keep the already materialized draft date.
    if _is_meeting_update_intent(str(out.get("intent") or "")) and command_date:
        materialized_date = manual_date_override or command_date
    else:
        materialized_date = manual_date_override or explicit_date or command_date
    entities_before = entity_date
    if materialized_date and entity_date != materialized_date:
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
        if "T" in first_token:
            date_token, time_tail = first_token.split("T", 1)
        else:
            date_token, time_tail = first_token, ""
        date_iso = _normalize_date_token(date_token)
        if not date_iso:
            return None, s
        remainder = s[len(s.split()[0]) :].strip()
        if time_tail:
            remainder = f"T{time_tail}{(' ' + remainder) if remainder else ''}".strip()
        normalized_start = f"{date_iso}{remainder}" if remainder.startswith("T") else f"{date_iso} {remainder}".strip()
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
    start_at_raw = str(_command_value(out, "start_at") or "").strip()
    if not datetime_raw and not start_at_raw:
        out["entities"] = entities
        return out

    date_raw = str(_command_value(out, "start_at_date", "start_date", "date") or "").strip()
    time_raw = str(_command_value(out, "start_at_time", "start_time", "time") or "").strip()
    datetime_source = datetime_raw or start_at_raw

    if not date_raw:
        parsed_date = _normalize_clarification_date_to_iso(datetime_source, prefer_future_for_ambiguous=False)
        if parsed_date:
            out["start_at_date"] = parsed_date
            entities["start_at_date"] = parsed_date

    if not time_raw:
        m = re.search(r"[T\s]\b([01]?\d|2[0-3])(?:[:.](\d{2}))?\b", datetime_source)
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


def _task_create_confirmation_action(text: str) -> str:
    normalized = str(text or "").strip().lower().replace("ё", "е")
    if not normalized:
        return "unknown"
    if _is_positive_confirmation(normalized):
        return "create"
    if normalized in {"создать", "создай", "подтвердить", "подтверждаю"}:
        return "create"
    if normalized in {"комментарий", "добавить комментарий", "изменить комментарий"}:
        return "comment"
    if _is_negative_confirmation(normalized):
        return "edit"
    if normalized in {"исправить", "изменить", "редактировать", "уточнить"}:
        return "edit"
    if normalized in {"не создавать", "не надо создавать", "отменить создание"}:
        return "cancel"
    return "unknown"


def _task_create_duplicate_confirmation_action(text: str) -> str:
    normalized = str(text or "").strip().lower().replace("ё", "е")
    if not normalized:
        return "unknown"
    if _is_positive_confirmation(normalized):
        return "yes"
    if normalized in {"да, создать еще одну", "да, создать ещё одну", "создать еще одну", "создать ещё одну"}:
        return "yes"
    if _is_negative_confirmation(normalized):
        return "no"
    if normalized in {"нет, не создавать", "не создавать"}:
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
    if missing_field == _TASK_CREATE_DUPLICATE_CONFIRM_FIELD:
        return "task_create_duplicate_confirm"
    if missing_field == _TASK_CREATE_DATE_FIELD:
        return "task_create_due_date"
    if missing_field == _TASK_CREATE_COMMENT_FIELD:
        return "task_create_comment"
    if missing_field == _AMBIGUOUS_CREATE_FIELD:
        return "ambiguous_create"
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
        _TASK_CREATE_DUPLICATE_CONFIRM_FIELD,
        _MEETING_UPDATE_TARGET_CONFIRM_FIELD,
        _TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD,
        _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD,
        _MEETING_COMMENT_TARGET_CONFIRM_FIELD,
        _MEETING_COMMENT_FINAL_CONFIRM_FIELD,
        _TASK_COMMENT_TARGET_CONFIRM_FIELD,
        _TASK_COMMENT_FINAL_CONFIRM_FIELD,
    }


def _is_meeting_update_intent(intent: str) -> bool:
    normalized = str(intent or "").strip().lower()
    return normalized in {"meeting.update", "meeting_update", "reschedule_meeting", "move_meeting"}


def _is_timeblock_update_intent(intent: str) -> bool:
    normalized = str(intent or "").strip().lower()
    return normalized in {
        "timeblock.update",
        "timeblock_update",
        "update_timeblock",
        "move_timeblock",
        "timeblock.move",
        "block.update",
    }


def _is_meeting_comment_update_intent(intent: str) -> bool:
    normalized = str(intent or "").strip().lower()
    return normalized in {"meeting.comment.update", "meeting_comment_update", "meeting.comment"}


def _is_task_comment_update_intent(intent: str) -> bool:
    normalized = str(intent or "").strip().lower()
    return normalized in {"task.comment.update", "task_comment_update", "task.comment"}


def _looks_like_meeting_update_request(text: str) -> bool:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return False
    has_update_verb = any(token in low for token in ("перенес", "сдвин", "поменя", "измени"))
    if not has_update_verb:
        return False
    has_meeting_noun = any(token in low for token in ("встреч", "собран", "созвон", "мероприят", "событ", "ивент"))
    return has_meeting_noun


def _looks_like_timeblock_update_request(text: str) -> bool:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return False
    has_update_verb = any(token in low for token in ("перенес", "сдвин", "поменя", "измени", "исправ", "обнов"))
    if not has_update_verb:
        return False
    return ("блок" in low) or ("таймблок" in low)


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


def _looks_like_new_executable_command(text: str) -> bool:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return False
    if _looks_like_meeting_update_request(low):
        return True
    if _looks_like_timeblock_update_request(low):
        return True
    if _looks_like_meeting_comment_update_request(low):
        return True
    if _looks_like_task_comment_update_request(low):
        return True
    has_task_create = (
        bool(re.search(r"\b(созда[йт]?|создай|добав[ьт]?|сдела[йт]?)\b", low))
        and "задач" in low
    )
    if has_task_create:
        return True
    has_meeting_create = (
        bool(re.search(r"\b(запланируй|назнач[ьт]?|постав[ьт]?|созда[йт]?|сдела[йт]?)\b", low))
        and any(tok in low for tok in ("встреч", "собран", "созвон", "мероприят", "событ", "ивент"))
    )
    if has_meeting_create:
        return True
    has_block_create = (
        bool(re.search(r"\b(постав[ьт]?|запланируй|созда[йт]?|сдела[йт]?)\b", low))
        and "блок" in low
    )
    if has_block_create:
        return True
    if _looks_like_generic_task_phrase(low):
        return True
    return _looks_like_timeblock_task_allocation_request(low)


def _has_meeting_create_keywords(text: str) -> bool:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return False
    meeting_tokens = (
        "встреч",
        "собран",
        "созвон",
        "звонок",
        "мероприят",
        "событ",
        "календар",
    )
    meeting_phrases = (
        "встреча с",
        "созвон с",
        "поставить в календарь",
    )
    return any(token in low for token in meeting_tokens) or any(phrase in low for phrase in meeting_phrases)


def _has_explicit_timeblock_create_keywords(text: str) -> bool:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return False
    phrases = (
        "выдели время",
        "забронируй время",
        "забронировать время",
        "зарезервируй время",
        "резерв времени",
        "блок времени",
        "фокус время",
    )
    return any(phrase in low for phrase in phrases) or bool(re.search(r"\bслот\b", low))


def _has_explicit_task_create_keywords(text: str) -> bool:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return False
    has_create_verb = bool(re.search(r"\b(созда[йт]?|создай|добав[ьт]?|сдела[йт]?|постав[ьт]?)\b", low))
    has_task_noun = any(token in low for token in ("задач", "действи", "дело"))
    task_action_markers = (
        "надо",
        "нужно",
        "купить",
        "позвонить",
        "отправить",
        "забрать",
        "проверить",
        "подготовить",
        "сделать",
    )
    return (has_create_verb and has_task_noun) or any(re.search(rf"\b{re.escape(token)}\b", low) for token in task_action_markers)


def _is_ambiguous_create_request(text: str) -> bool:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return False
    compact = re.sub(r"\s+", " ", low).strip(" .,!?:;")
    if compact in _AMBIGUOUS_CREATE_EXACT_PHRASES:
        return True
    return False


def _ambiguous_create_choice_from_text(text: str) -> str:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return ""
    if "событие" in low or "календар" in low:
        return "meeting.create"
    if "блок времени" in low or "время" in low:
        return "timeblock.create"
    if "задач" in low:
        return "task.create"
    return ""


def _route_flags_for_text(text: str) -> Dict[str, bool]:
    raw = str(text or "").strip()
    low = raw.lower().replace("ё", "е")
    date_resolution = extract_date_resolution(low, now=datetime.combine(date.today(), datetime.min.time()), prefer_future=True)
    return {
        "has_date": bool(date_resolution.ok and str(date_resolution.date or "").strip()),
        "has_calendar_marker": _has_meeting_create_keywords(raw),
        "has_timeblock_marker": _has_explicit_timeblock_create_keywords(raw) or _looks_like_timeblock_task_allocation_request(raw),
        "has_task_marker": _has_explicit_task_create_keywords(raw) or _looks_like_generic_task_text(raw),
    }


def _log_runtime_route_selected(
    *,
    request_id: str,
    raw_text: str,
    route: str,
    reason: str,
) -> None:
    flags = _route_flags_for_text(raw_text)
    LOG.info(
        "runtime_route_selected",
        extra={
            "request_id": request_id,
            "trace_id": request_id,
            "raw_text": str(raw_text or "").strip(),
            "route": route,
            "reason": reason,
            "has_date": bool(flags["has_date"]),
            "has_calendar_marker": bool(flags["has_calendar_marker"]),
            "has_timeblock_marker": bool(flags["has_timeblock_marker"]),
            "has_task_marker": bool(flags["has_task_marker"]),
        },
    )


def _looks_like_generic_task_text(text: str) -> bool:
    raw = str(text or "").strip()
    low = raw.lower().replace("ё", "е")
    if not low:
        return False
    if _has_explicit_task_create_keywords(low):
        return True
    if _looks_like_meeting_update_request(low) or _looks_like_timeblock_update_request(low):
        return False
    if _looks_like_meeting_comment_update_request(low) or _looks_like_task_comment_update_request(low):
        return False
    if _has_meeting_create_keywords(low) or _looks_like_timeblock_task_allocation_request(low):
        return False
    normalized_title = _normalize_task_create_title(raw)
    if not normalized_title:
        return False
    title_low = normalized_title.lower().replace("ё", "е")
    if title_low in {"задача", "дело", "план", "напоминание"}:
        return False
    first_word_match = re.match(r"^([a-zа-я0-9_-]+)", title_low)
    first_word = str(first_word_match.group(1) or "").strip() if first_word_match else ""
    if not first_word:
        return False
    infinitive_like = first_word.endswith(("ть", "ти", "чь", "ться", "тись", "чься"))
    imperative_like = first_word in {
        "купи",
        "позвони",
        "отправь",
        "возьми",
        "взять",
        "забрать",
        "закажи",
        "оплати",
        "забери",
        "принеси",
        "сделай",
        "напиши",
    }
    return infinitive_like or imperative_like


def _looks_like_generic_task_phrase(text: str) -> bool:
    raw = str(text or "").strip()
    low = raw.lower().replace("ё", "е")
    if not _looks_like_generic_task_text(raw):
        return False
    resolution = extract_date_resolution(low, now=datetime.combine(date.today(), datetime.min.time()), prefer_future=True)
    return bool(resolution.ok and str(resolution.date or "").strip())


def _is_explicit_memory_query(text: str) -> bool:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return False
    patterns = (
        r"\bвспомни\b",
        r"\bчто ты помнишь\b",
        r"\bнайди\s+в\s+памяти\b",
        r"\bиз\s+памяти\b",
        r"\bпоищи\s+в\s+памяти\b",
        r"\bчто\s+я\s+говорил\b",
    )
    return any(re.search(pattern, low) for pattern in patterns)


def _looks_like_timeblock_task_allocation_request(text: str) -> bool:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return False
    if _looks_like_meeting_update_request(low) or _looks_like_timeblock_update_request(low):
        return False
    if _has_meeting_create_keywords(low):
        return False
    if _has_explicit_task_create_keywords(low):
        return False
    if _has_explicit_timeblock_create_keywords(low):
        return True
    has_allocation_verb = bool(re.search(r"\b(выдел|запланир|постав|забронир|зарезервир)\w*\b", low))
    has_taskish_target = bool(
        re.search(r"\b(задач\w*|дел[оа]?|работ[ауые]?)\b", low)
        or "на нее" in low
        or "на неё" in low
        or "на эту задачу" in low
        or "под задачу" in low
    )
    has_slot_marker = any(token in low for token in ("время", "слот", "блок", "таймблок", "timeblock"))
    has_duration_hint = _extract_duration_minutes_from_text(low) is not None
    return has_allocation_verb and has_taskish_target and (has_slot_marker or has_duration_hint)


def _extract_timeblock_task_hint(raw_text: str) -> str:
    low = str(raw_text or "").strip().lower().replace("ё", "е")
    if not low:
        return ""
    if "работ" in low:
        return "Работа над задачей"
    if any(token in low for token in ("задач", "дело", "дела", "нее", "неё")):
        return "Задача"
    return ""


def _extract_task_create_due_date_hint(raw_text: str) -> str:
    source = str(raw_text or "").strip()
    if not source:
        return ""
    return str(_extract_explicit_date_from_text(source) or "").strip()


def _normalize_task_due_date_to_planned_at(
    raw_due_date: Any,
    raw_planned_at: Any,
    *,
    source_text: str,
    today: Optional[date] = None,
) -> Dict[str, str]:
    normalized = normalize_temporal_fields(
        {
            "due_date": raw_due_date,
            "planned_at": raw_planned_at,
            "text": source_text,
        },
        now=datetime.combine(today or date.today(), datetime.min.time()),
    )
    raw_due = str(raw_due_date or "").strip()
    raw_planned = str(raw_planned_at or "").strip()
    resolved_due_date = str(normalized.get("due_date") or "").strip()
    resolved_planned_at = str(normalized.get("planned_at") or "").strip()
    return {
        "raw_due_date": raw_due,
        "raw_planned_at": raw_planned,
        "resolved_due_date": str(resolved_due_date or "").strip(),
        "resolved_planned_at": str(resolved_planned_at or "").strip(),
    }


def _resolve_task_create_planned_at(command: Dict[str, Any], source_text: str) -> tuple[str, str, str, str]:
    out = command if isinstance(command, dict) else {}
    resolution = _normalize_task_due_date_to_planned_at(
        _command_value(out, "due_date", "date", "when"),
        _command_value(out, "planned_at"),
        source_text=source_text,
    )
    return (
        str(resolution.get("resolved_due_date") or "").strip(),
        str(resolution.get("resolved_planned_at") or "").strip(),
        str(resolution.get("raw_due_date") or "").strip(),
        str(resolution.get("raw_planned_at") or "").strip(),
    )


def _looks_like_explicit_subtask_create_request(text: str) -> bool:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return False
    if "подзадач" in low or "дочерн" in low:
        return True
    if re.search(r"\b(?:к|под|внутри)\s+задач[еи]\s*#?\d+\b", low):
        return True
    if "под задачей" in low or "внутри задачи" in low:
        return True
    return False


def _extract_parent_task_id_from_text(raw_text: str) -> str:
    low = str(raw_text or "").strip().lower().replace("ё", "е")
    if not low:
        return ""
    patterns = (
        r"\bк\s+задач[еи]\s*#?(\d+)\b",
        r"\bподзадач\w*\s+к\s+задач[еи]\s*#?(\d+)\b",
        r"\bпод\s+задач[еи]\s*#?(\d+)\b",
        r"\bвнутри\s+задач[еи]\s*#?(\d+)\b",
    )
    for pattern in patterns:
        match = re.search(pattern, low)
        if match:
            return str(match.group(1) or "").strip()
    return ""


def _normalize_task_create_title(source_text: str) -> str:
    raw = str(source_text or "").strip()
    if not raw:
        return ""
    s = raw.replace("ё", "е").replace("Ё", "Е")
    s = re.sub(r"[\"'`]", " ", s)
    s = re.sub(
        r"\b(созда[йт]?|создай|добав[ьт]?|сдела[йт]?|постав[ьт]?|нужно|надо|хочу)\b",
        " ",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(r"\bподзадач(?:а|у|и|е|ей)?\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\bзадач(?:а|у|и|е|ей)?\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\bдействи(?:е|я|ю|ем)?\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\bдел(?:о|а|у|ом|е)?\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(?:к|под|внутри)\s+задач[еи]\s*#?\d+\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\bпод\s+ней\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\bк\s+ней\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\bкомментари(?:й|я)\s*(?::|-)?\s*.+$", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(сегодня|завтра|послезавтра)\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\b\d{1,2}[./-]\d{1,2}(?:[./-]\d{4})?\b", " ", s)
    s = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", " ", s)
    s = re.sub(
        r"\b(?:в|к|на)\s*\d{1,2}(?::\d{2})?(?:\s*(?:час(?:а|ов)?|утра|дня|вечера|ночи))?\b",
        " ",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(r"\s+", " ", s).strip(" ,.-")
    return s[:1].upper() + s[1:] if s else ""


def _task_create_title_is_weak(command: Dict[str, Any], source_text: str) -> bool:
    current = str(_command_value(command, "title", "task_title") or "").strip()
    source = str(source_text or "").strip()
    if not current:
        return True
    current_low = current.lower().replace("ё", "е")
    source_low = source.lower().replace("ё", "е")
    if source and current_low == source_low:
        return True
    if current_low in {"задача", "task", "создай задачу", "создать задачу"}:
        return True
    if re.search(r"\b(созда[йт]?|создай|добав[ьт]?|сдела[йт]?|постав[ьт]?)\b", current_low) and "задач" in current_low:
        return True
    return False


def _normalize_task_create_from_source(command: Dict[str, Any], source_text: str) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    if not _is_task_create_intent(str(out.get("intent") or "")):
        return out
    if _looks_like_timeblock_task_allocation_request(source_text):
        return out

    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    source = str(source_text or "").strip()
    if source and not str(entities.get("text") or "").strip():
        entities["text"] = source

    explicit_subtask = _looks_like_explicit_subtask_create_request(source)
    extracted_parent_task_id = _extract_parent_task_id_from_text(source)
    existing_parent_task_id = str(_command_value(out, "parent_task_id") or "").strip()
    parent_task_explicit = explicit_subtask and bool(extracted_parent_task_id or existing_parent_task_id)

    if parent_task_explicit:
        effective_parent_task_id = extracted_parent_task_id or existing_parent_task_id
        if effective_parent_task_id:
            out["parent_task_id"] = int(effective_parent_task_id)
            entities["parent_task_id"] = int(effective_parent_task_id)
        out["parent_task_explicit"] = True
        entities["parent_task_explicit"] = True
    else:
        out.pop("parent_task_id", None)
        out["parent_task_explicit"] = False
        if "parent_task_id" in entities:
            entities.pop("parent_task_id", None)
        entities["parent_task_explicit"] = False

    if _task_create_title_is_weak(out, source):
        normalized_title = _normalize_task_create_title(source)
        if normalized_title:
            out["title"] = normalized_title
            entities["title"] = normalized_title

    extracted_date, planned_at, raw_due_date, raw_planned_at = _resolve_task_create_planned_at(out, source)
    if extracted_date:
        out["due_date"] = extracted_date
        entities["due_date"] = extracted_date
    if planned_at:
        out["planned_at"] = planned_at
        entities["planned_at"] = planned_at
    if raw_due_date:
        out["__task_create_raw_due_date"] = raw_due_date
    if raw_planned_at:
        out["__task_create_raw_planned_at"] = raw_planned_at
    existing_comment = str(_command_value(out, "comment_text", "comment", "description", "notes", "note") or "").strip()
    if not existing_comment:
        extracted_comment = _extract_comment_text_from_source(source)
        if extracted_comment:
            out["comment_text"] = extracted_comment
            out["comment"] = extracted_comment
            entities["comment_text"] = extracted_comment
            entities["comment"] = extracted_comment

    out["entities"] = entities
    LOG.info(
        "task_parent_resolution",
        extra={
            "source": "telegram",
            "intent": str(out.get("intent") or ""),
            "parent_task_id": str(_command_value(out, "parent_task_id") or ""),
            "parent_task_explicit": bool(_command_value(out, "parent_task_explicit")),
        },
    )
    LOG.info(
        "task_create_date_resolution",
        extra={
            "source": "telegram",
            "intent": str(out.get("intent") or ""),
            "raw_text": source,
            "raw_due_date": raw_due_date,
            "extracted_date": extracted_date,
            "resolved_planned_at": planned_at,
            "planned_at": planned_at,
            "summary_planned_at": planned_at,
            "persisted_planned_at": "",
        },
    )
    return out


def _extract_start_time_hint_from_text(raw_text: str) -> str:
    low = str(raw_text or "").strip().lower().replace("ё", "е")
    if not low:
        return ""
    match = re.search(r"\b(?:в|к)\s*([01]?\d|2[0-3])(?:[:.](\d{2}))?\b", low)
    if not match:
        return ""
    hour = int(match.group(1))
    minute = int(match.group(2) or "0")
    return f"{hour:02d}:{minute:02d}"


def _apply_timeblock_create_semantic_routing(command: Dict[str, Any], source_text: str) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    source = str(source_text or "").strip()
    if not _looks_like_timeblock_task_allocation_request(source):
        return out

    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    out["intent"] = "timeblock.create"
    if not str(entities.get("text") or "").strip():
        entities["text"] = source
    if not str(_command_value(out, "task_id") or "").strip():
        out["task_ref_optional"] = True
        entities["task_ref_optional"] = True

    if not str(_command_value(out, "task_id", "task_ref", "task_title", "title") or "").strip():
        task_hint = _extract_timeblock_task_hint(source)
        if task_hint:
            out["task_ref"] = task_hint
            entities["task_ref"] = task_hint

    if not str(_command_value(out, "duration_minutes", "duration_min", "duration_mins", "duration") or "").strip():
        inferred_duration = _extract_duration_minutes_from_text(source)
        if inferred_duration is not None:
            out["duration_minutes"] = int(inferred_duration)
            entities["duration_minutes"] = int(inferred_duration)

    if not str(_command_value(out, "start_at_time", "start_time", "time") or "").strip():
        inferred_time = _extract_start_time_hint_from_text(source)
        if inferred_time:
            out["start_at_time"] = inferred_time
            entities["start_at_time"] = inferred_time
            start_date = str(_command_value(out, "start_at_date", "start_date", "date") or "").strip()
            if start_date and not str(_command_value(out, "start_at") or "").strip():
                out["start_at"] = f"{start_date}T{inferred_time}"
                entities["start_at"] = out["start_at"]

    out["entities"] = entities
    return out


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
    current_comment = str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip()
    source_comment = _meeting_update_current_comment(command)
    changes: Dict[str, Any] = {}
    if str(current_date or "").strip() and str(current_date or "").strip() != str(source_date or "").strip():
        changes["new_date"] = str(current_date).strip()
    if str(current_time or "").strip() and str(current_time or "").strip() != str(source_time or "").strip():
        changes["new_time"] = str(current_time).strip()
    if str(current_duration or "").strip() and str(current_duration or "").strip() != str(source_duration or "").strip():
        changes["new_duration"] = str(current_duration).strip()
    if current_comment and current_comment != source_comment:
        changes["new_comment"] = current_comment
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
    kind = _resolve_meeting_kind(command) or "встреча"
    kind_acc = "встречу" if kind == "встреча" else kind
    return f"Вы имеете в виду {kind_acc} {source_date_label} {source_range_label}?"


def _meeting_update_current_comment(command: Dict[str, Any]) -> str:
    return str(
        _command_value(
            command,
            "meeting_update_source_comment",
            "meeting_update_source_description",
            "meeting_update_source_notes",
        )
        or ""
    ).strip()


def _meeting_update_next_comment(command: Dict[str, Any]) -> str:
    explicit = str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip()
    return explicit if explicit else _meeting_update_current_comment(command)


def _meeting_update_recompute_has_changes(command: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    new_date = str(_meeting_update_source_value(out, "meeting_update_new_date") or "").strip()
    new_time = _normalize_time_hhmm_local(_meeting_update_source_value(out, "meeting_update_new_time"))
    new_duration = _meeting_update_source_value(out, "meeting_update_new_duration")
    has_duration_change = new_duration is not None and str(new_duration).strip() != ""
    has_comment_change = _meeting_update_next_comment(out) != _meeting_update_current_comment(out)
    has_changes = bool(new_date or new_time or has_duration_change or has_comment_change)
    out["meeting_update_has_changes"] = has_changes
    entities["meeting_update_has_changes"] = has_changes
    out["entities"] = entities
    return out


def _temporal_edit_entity_kind(intent: str) -> str:
    normalized = str(intent or "").strip().lower()
    if _is_timeblock_update_intent(normalized):
        return "timeblock"
    if _is_meeting_update_intent(normalized):
        return "meeting"
    return "temporal"


def _temporal_edit_entity_id(intent: str, command: Dict[str, Any]) -> str:
    normalized = str(intent or "").strip().lower()
    if _is_timeblock_update_intent(normalized):
        return _timeblock_update_target_block_id(command)
    if _is_meeting_update_intent(normalized):
        return _meeting_update_target_event_id(command)
    return ""


def _temporal_edit_summarize_comment(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    compact = re.sub(r"\s+", " ", text)
    if len(compact) <= 32:
        return compact
    return compact[:29] + "..."


def _temporal_edit_value_for_log(field: str, value: Any) -> Any:
    normalized_field = str(field or "").strip().lower()
    if normalized_field == "comment":
        return _temporal_edit_summarize_comment(value)
    return value


def _meeting_update_summary_payload(command: Dict[str, Any]) -> Dict[str, Any]:
    old_values = {
        "date": _format_date_ru(_meeting_update_source_value(command, "meeting_update_source_date", "start_at_date", "date")),
        "time": str(_meeting_update_source_value(command, "meeting_update_source_time", "start_at_time", "time", "start_time") or "").strip() or "-",
        "duration": _format_duration_human(_meeting_update_source_value(command, "meeting_update_source_duration", "duration_minutes", "duration_min")),
        "comment": _temporal_edit_summarize_comment(_meeting_update_current_comment(command) or "-"),
    }
    new_values = {
        "date": _format_date_ru(
            _meeting_update_source_value(command, "start_at_date", "date", "meeting_update_new_date")
            or _meeting_update_source_value(command, "meeting_update_source_date")
        ),
        "time": str(
            _meeting_update_source_value(command, "start_at_time", "time", "start_time", "meeting_update_new_time")
            or _meeting_update_source_value(command, "meeting_update_source_time")
            or ""
        ).strip()
        or "-",
        "duration": _format_duration_human(
            _meeting_update_source_value(command, "duration_minutes", "duration_min", "meeting_update_new_duration")
            or _meeting_update_source_value(command, "meeting_update_source_duration")
        ),
        "comment": _temporal_edit_summarize_comment(_meeting_update_next_comment(command) or "-"),
    }
    changed_fields = [key for key in ("date", "time", "duration", "comment") if old_values.get(key) != new_values.get(key)]
    return {"old_values": old_values, "new_values": new_values, "changed_fields": changed_fields}


def _timeblock_update_summary_payload(command: Dict[str, Any]) -> Dict[str, Any]:
    old_values = {
        "date": _format_date_ru(_timeblock_update_source_value(command, "date", "start_at_date")),
        "time": str(_timeblock_update_source_value(command, "time", "start_at_time", "start_time") or "").strip() or "-",
        "duration": _format_duration_human(_timeblock_update_source_value(command, "duration", "duration_minutes", "duration_min")),
        "comment": _temporal_edit_summarize_comment(_timeblock_update_current_comment(command) or "-"),
    }
    new_values = {
        "date": _format_date_ru(
            _timeblock_update_source_value(command, "timeblock_update_new_date")
            or _timeblock_update_source_value(command, "date", "start_at_date")
        ),
        "time": str(
            _timeblock_update_source_value(command, "timeblock_update_new_time")
            or _timeblock_update_source_value(command, "time", "start_at_time", "start_time")
            or ""
        ).strip()
        or "-",
        "duration": _format_duration_human(
            _timeblock_update_source_value(command, "timeblock_update_new_duration")
            or _timeblock_update_source_value(command, "duration", "duration_minutes", "duration_min")
        ),
        "comment": _temporal_edit_summarize_comment(_timeblock_update_next_comment(command) or "-"),
    }
    changed_fields = [key for key in ("date", "time", "duration", "comment") if old_values.get(key) != new_values.get(key)]
    return {"old_values": old_values, "new_values": new_values, "changed_fields": changed_fields}


def _temporal_edit_log(
    event: str,
    *,
    request_id: str,
    user_id: str,
    intent: str,
    command: Dict[str, Any],
    field: str = "",
    normalized_value: Any = None,
    from_callback: Optional[bool] = None,
    clarification_state: str = "",
    **extra_fields: Any,
) -> None:
    payload: Dict[str, Any] = {
        "request_id": request_id,
        "trace_id": request_id,
        "flow_id": request_id,
        "user_id": str(user_id or "").strip(),
        "intent": str(intent or "").strip(),
        "entity_kind": _temporal_edit_entity_kind(intent),
        "entity_id": _temporal_edit_entity_id(intent, command),
        "calendar_event_id": _meeting_update_target_event_id(command) if _is_meeting_update_intent(intent) else "",
        "time_block_id": _timeblock_update_target_block_id(command) if _is_timeblock_update_intent(intent) else "",
    }
    if field:
        payload["field"] = field
    if normalized_value is not None:
        payload["normalized_value"] = _temporal_edit_value_for_log(field, normalized_value)
    if from_callback is not None:
        payload["from_callback"] = bool(from_callback)
    if clarification_state:
        payload["clarification_state"] = clarification_state
    payload.update(extra_fields)
    LOG.info(event, extra=payload)


def _meeting_update_field_choice_question() -> str:
    return "Что изменить?"


def _meeting_update_active_edit_choice(command: Dict[str, Any]) -> str:
    return str(command.get(_MEETING_UPDATE_ACTIVE_EDIT_KEY) or "").strip().lower()


def _meeting_update_field_choice_from_text(text: str) -> str:
    s = str(text or "").strip().lower().replace("ё", "е")
    if not s:
        return ""
    if "дат" in s and "врем" in s:
        return "date_time"
    if "коммент" in s:
        return "comment"
    if "длит" in s or "мин" in s or "час" in s:
        return "duration"
    if "врем" in s:
        return "time"
    if "дат" in s:
        return "date"
    return ""


def _meeting_update_field_choice_from_metadata(metadata: Optional[Dict[str, Any]]) -> str:
    meta = metadata if isinstance(metadata, dict) else {}
    callback = meta.get("callback_query") if isinstance(meta.get("callback_query"), dict) else {}
    callback_data = str(callback.get("data") or "").strip()
    if not callback_data.startswith("clarify:v1:meeting_update_edit:"):
        return ""
    value = callback_data.rsplit(":", 1)[-1].strip().lower()
    if value in {"date", "time", "duration", "comment", "date_time", "cancel"}:
        return value
    return ""


def _meeting_update_apply_field_choice(command: Dict[str, Any], choice: str) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    selected = str(choice or "").strip().lower()
    out[_MEETING_UPDATE_ACTIVE_EDIT_KEY] = selected
    out.pop(_MEETING_UPDATE_PENDING_COMBO_KEY, None)
    if selected == "date_time":
        out[_MEETING_UPDATE_PENDING_COMBO_KEY] = True
    return out


def _meeting_update_edit_question(command: Dict[str, Any]) -> str:
    active = _meeting_update_active_edit_choice(command)
    if active == "date":
        return "На какую дату перенести/изменить встречу?"
    if active == "time":
        return "На какое время перенести/изменить встречу?"
    if active == "duration":
        return "На сколько минут изменить длительность встречи?"
    if active == "comment":
        current_comment = _meeting_update_current_comment(command)
        return (
            "Введите новый комментарий.\n\n"
            "Текущий комментарий:\n"
            f"{current_comment or '-'}\n\n"
            "Если передумали — нажмите «Оставить без изменений» или «Отмена»."
        )
    if active == "date_time":
        if bool(command.get(_MEETING_UPDATE_PENDING_COMBO_KEY)):
            return "На какую дату перенести/изменить встречу?"
        return "На какое время перенести/изменить встречу?"
    return _meeting_update_field_choice_question()


def _meeting_update_final_confirmation_question(command: Dict[str, Any]) -> str:
    current_title = str(_meeting_update_source_value(command, "meeting_update_source_title", "title") or "Встреча").strip() or "Встреча"
    current_date = _format_date_ru(_meeting_update_source_value(command, "meeting_update_source_date", "start_at_date", "date"))
    current_time = str(_meeting_update_source_value(command, "meeting_update_source_time", "start_at_time", "time", "start_time") or "").strip() or "-"
    current_duration = _format_duration_human(_meeting_update_source_value(command, "meeting_update_source_duration", "duration_minutes", "duration_min"))
    current_comment = _meeting_update_current_comment(command) or "-"
    new_date = _format_date_ru(_meeting_update_source_value(command, "start_at_date", "date", "meeting_update_new_date") or _meeting_update_source_value(command, "meeting_update_source_date"))
    new_time = str(_meeting_update_source_value(command, "start_at_time", "time", "start_time", "meeting_update_new_time") or _meeting_update_source_value(command, "meeting_update_source_time") or "").strip() or "-"
    new_duration = _format_duration_human(_meeting_update_source_value(command, "duration_minutes", "duration_min", "meeting_update_new_duration") or _meeting_update_source_value(command, "meeting_update_source_duration"))
    new_comment = _meeting_update_next_comment(command) or "-"
    lines = [
        "Проверьте, всё ли верно.",
        f"Встреча: {current_title}",
        "Текущие параметры:",
        f"Дата: {current_date}",
        f"Время: {current_time}",
        f"Длительность: {current_duration}",
        f"Комментарий: {current_comment}",
        "",
        "Новые параметры:",
        f"Дата: {new_date}",
        f"Время: {new_time}",
        f"Длительность: {new_duration}",
        f"Комментарий: {new_comment}",
        "",
        "Подтвердить изменение?",
    ]
    return "\n".join(lines)


def _meeting_update_stage_from_missing_field(field_name: str) -> str:
    normalized = normalize_clarification_field_name(str(field_name or ""))
    if normalized in {"meeting_update_target_ref", _MEETING_UPDATE_TARGET_REFINE_FIELD}:
        return _MEETING_UPDATE_TARGET_IDENTIFICATION_FIELD
    if normalized == _MEETING_UPDATE_TARGET_SELECT_FIELD:
        return _MEETING_UPDATE_TARGET_SELECT_FIELD
    if normalized == _MEETING_UPDATE_TARGET_CONFIRM_FIELD:
        return _MEETING_UPDATE_TARGET_CONFIRM_FIELD
    if normalized in {
        _MEETING_UPDATE_EDIT_CHOICE_FIELD,
        _MEETING_UPDATE_EDIT_DATE_FIELD,
        _MEETING_UPDATE_EDIT_TIME_FIELD,
        _MEETING_UPDATE_EDIT_DURATION_FIELD,
        _MEETING_UPDATE_EDIT_COMMENT_FIELD,
        _MEETING_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
        _MEETING_UPDATE_EDIT_DATE_TIME_TIME_FIELD,
    }:
        return normalized
    if normalized == _TEMPORAL_CONFIRM_FIELD:
        return _MEETING_UPDATE_FINAL_CONFIRM_FIELD
    if normalized == _MEETING_UPDATE_FINAL_CONFIRM_FIELD:
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


def _task_duplicate_check_url() -> str:
    base = str(os.getenv("WORKER_COMMAND_URL", "") or "").strip().rstrip("/")
    if not base:
        return ""
    if base.endswith("/runtime/command"):
        return base[: -len("/runtime/command")] + "/runtime/task/check_duplicate_create"
    return base + "/runtime/task/check_duplicate_create"


def _timeblock_search_url() -> str:
    base = str(os.getenv("WORKER_COMMAND_URL", "") or "").strip().rstrip("/")
    if not base:
        return ""
    if base.endswith("/runtime/command"):
        return base[: -len("/runtime/command")] + "/runtime/timeblock/search"
    return base + "/runtime/timeblock/search"


def _sync_conflict_action_url() -> str:
    base = str(os.getenv("WORKER_COMMAND_URL", "") or "").strip().rstrip("/")
    if not base:
        return ""
    if base.endswith("/runtime/command"):
        return base[: -len("/runtime/command")] + "/runtime/sync_conflict/action"
    return base + "/runtime/sync_conflict/action"


def _sync_conflict_action_from_metadata(metadata: Optional[Dict[str, Any]]) -> tuple[str, str]:
    callback = metadata.get("callback_query") if isinstance(metadata, dict) else None
    callback = callback if isinstance(callback, dict) else {}
    raw = str(callback.get("data") or "").strip()
    parts = raw.split(":")
    if len(parts) != 3 or parts[0] != "sync_conflict":
        return "", ""
    action = str(parts[1] or "").strip().lower()
    conflict_id = str(parts[2] or "").strip()
    if action not in _SYNC_CONFLICT_ACTIONS or not conflict_id:
        return "", ""
    return action, conflict_id


def _sync_conflict_prompt(command: Dict[str, Any]) -> str:
    conflict = command.get("__sync_conflict") if isinstance(command.get("__sync_conflict"), dict) else {}
    summary_text = str(conflict.get("summary_text") or "").strip()
    if not summary_text:
        title = str(conflict.get("title") or "Событие").strip() or "Событие"
        start_date = str(conflict.get("start_at_date") or "").strip()
        start_time = str(conflict.get("start_at_time") or "").strip()
        pieces = [title, " ".join([p for p in (start_date, start_time) if p]).strip()]
        summary_text = "\n".join([piece for piece in pieces if piece])
    return (
        "Найдено событие в базе, которого нет в Google Calendar.\n"
        f"{summary_text}\n"
        "Что сделать?"
    ).strip()


def _sync_conflict_apply_action(conflict_id: str, action: str) -> Dict[str, Any]:
    url = _sync_conflict_action_url()
    if not url:
        return {"ok": False, "reason": "sync_conflict_action_unavailable"}
    req = urllib.request.Request(
        url,
        data=json.dumps({"id": str(conflict_id or "").strip(), "action": str(action or "").strip()}, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return payload if isinstance(payload, dict) else {"ok": False, "reason": "invalid_sync_conflict_payload"}
    except Exception:
        return {"ok": False, "reason": "sync_conflict_action_failed"}


def _sync_conflict_command_for_edit(conflict: Dict[str, Any]) -> Dict[str, Any]:
    comment_text = str(conflict.get("comment_text") or "").strip()
    out: Dict[str, Any] = {
        "intent": "meeting.update",
        "calendar_event_id": str(conflict.get("calendar_event_id") or "").strip(),
        "sync_conflict_id": str(conflict.get("id") or "").strip(),
        "title": str(conflict.get("title") or "Встреча").strip() or "Встреча",
        "start_at_date": str(conflict.get("start_at_date") or "").strip(),
        "start_at_time": str(conflict.get("start_at_time") or "").strip(),
        "duration_minutes": conflict.get("duration_minutes"),
        "comment_text": comment_text,
        "comment": comment_text,
        "meeting_kind": str(conflict.get("meeting_kind") or "встреча").strip() or "встреча",
        "__sync_conflict": dict(conflict),
        "__edited_temporal_draft": True,
    }
    entities = {
        "calendar_event_id": out["calendar_event_id"],
        "sync_conflict_id": out["sync_conflict_id"],
        "title": out["title"],
        "start_at_date": out["start_at_date"],
        "start_at_time": out["start_at_time"],
        "duration_minutes": out["duration_minutes"],
        "comment_text": comment_text,
        "comment": comment_text,
        "meeting_kind": out["meeting_kind"],
    }
    if str(out.get("start_at_date") or "").strip() and str(out.get("start_at_time") or "").strip():
        out["start_at"] = f"{out['start_at_date']}T{out['start_at_time']}:00"
        entities["start_at"] = str(out["start_at"])
    out["entities"] = entities
    return out


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


def _task_create_duplicate_precheck(user_id: str, command: Dict[str, Any]) -> Dict[str, Any]:
    url = _task_duplicate_check_url()
    uid = str(user_id or "").strip()
    title = str(_command_value(command, "title", "task_title", "text") or "").strip()
    planned_at = str(_command_value(command, "planned_at", "due_date", "due_at", "date", "when") or "").strip()
    parent_task_explicit = bool(_command_value(command, "parent_task_explicit"))
    parent_task_id_value = _command_value(command, "parent_task_id")
    if not url or not uid or not title:
        return {"ok": False, "reason": "precheck_not_applicable"}
    duplicate_precheck_planned_day = planned_at[:10] if planned_at else None
    LOG.info(
        "task_duplicate_precheck_start",
        extra={
            "user_id": uid,
            "title": title,
            "planned_at": planned_at,
            "planned_day": duplicate_precheck_planned_day,
            "undated_scope": not bool(planned_at),
        },
    )
    parent_task_id = None
    if parent_task_explicit and str(parent_task_id_value or "").strip():
        try:
            parent_task_id = int(parent_task_id_value)
        except Exception:
            return {"ok": False, "reason": "invalid_parent_task_id"}
    req = urllib.request.Request(
        url,
        data=json.dumps(
            {
                "user_id": uid,
                "title": title,
                "planned_at": (planned_at or None),
                "parent_task_id": parent_task_id,
            },
            ensure_ascii=False,
        ).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            out = payload if isinstance(payload, dict) else {"ok": False, "reason": "invalid_duplicate_precheck_payload"}
            duplicate_scope = str(out.get("duplicate_scope") or "").strip()
            if not duplicate_scope:
                duplicate_scope = "dated"
                if bool(out.get("undated_scope")):
                    duplicate_scope = (
                        "subtask_undated"
                        if parent_task_explicit and parent_task_id_value not in (None, "")
                        else "inbox"
                    )
            LOG.info(
                "task_duplicate_precheck_result",
                extra={
                    "normalized_title": str(out.get("normalized_title") or "").strip(),
                    "planned_at": (planned_at or None),
                    "planned_day": out.get("planned_day") if "planned_day" in out else duplicate_precheck_planned_day,
                    "undated_scope": bool(out.get("undated_scope")),
                    "duplicate_scope": duplicate_scope,
                    "parent_task_id": str(parent_task_id if parent_task_explicit and parent_task_id_value not in (None, "") else ""),
                    "candidate_count": int(out.get("candidate_count") or 0),
                    "duplicate_found": bool(out.get("duplicate_found")),
                    "source": "telegram_handler_precheck",
                },
            )
            return out
    except Exception:
        return {"ok": False, "reason": "duplicate_precheck_failed"}


def _normalize_time_hhmm_local(raw: Any) -> str:
    value = str(raw or "").strip().lower().replace("ё", "е")
    if not value:
        return ""

    def _apply_daypart(hour: int, marker: str) -> int:
        mk = str(marker or "").strip()
        if not mk:
            return hour
        if mk == "вечера" and 1 <= hour <= 11:
            return hour + 12
        if mk == "дня" and 1 <= hour <= 11:
            return hour + 12
        if mk == "ночи" and hour == 12:
            return 0
        return hour

    def _to_hhmm(hour: int, minute: int, marker: str = "") -> str:
        hh = _apply_daypart(int(hour), marker)
        mm = int(minute)
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return ""
        return f"{hh:02d}:{mm:02d}"

    m = re.fullmatch(r"(\d{1,2})(?::(\d{2}))?", value)
    if m:
        return _to_hhmm(int(m.group(1)), int(m.group(2) or "0"))

    m = re.search(r"\b(?:в|к)?\s*(\d{1,2})(?::(\d{2}))?\s*(утра|дня|вечера|ночи)?\b", value)
    if m:
        normalized = _to_hhmm(int(m.group(1)), int(m.group(2) or "0"), str(m.group(3) or ""))
        if normalized:
            return normalized

    word_to_hour = {
        "ноль": 0,
        "нуль": 0,
        "один": 1,
        "одна": 1,
        "два": 2,
        "две": 2,
        "три": 3,
        "четыре": 4,
        "пять": 5,
        "шесть": 6,
        "семь": 7,
        "восемь": 8,
        "девять": 9,
        "десять": 10,
        "одиннадцать": 11,
        "двенадцать": 12,
    }
    word_re = "|".join(sorted((re.escape(k) for k in word_to_hour.keys()), key=len, reverse=True))
    m = re.search(rf"\b(?:в|к)?\s*({word_re})\s*(утра|дня|вечера|ночи)?\b", value)
    if m:
        hour = int(word_to_hour.get(str(m.group(1) or ""), -1))
        if hour >= 0:
            normalized = _to_hhmm(hour, 0, str(m.group(2) or ""))
            if normalized:
                return normalized
    return ""


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
    source_title = _normalize_meeting_title_canonical(candidate.get("title"), candidate.get("meeting_kind"))
    source_kind = str(candidate.get("meeting_kind") or "").strip().lower() or _extract_meeting_kind_from_text(source_title)
    source_comment = str(candidate.get("description") or candidate.get("comment") or "").strip()

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
    out["meeting_update_source_comment"] = source_comment
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
    entities["meeting_update_source_comment"] = source_comment
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

    out["entities"] = entities
    return _meeting_update_recompute_has_changes(out)


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
    s = s.replace("→", " ")
    s = re.sub(r"\s*-\>\s*", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Remove trailing date/time command tails: "в 11 29.04", "-> 29.04", etc.
    s = re.sub(
        r"(?:\s*(?:\b(?:в|на|к)\s*\d{1,2}(?::\d{2})?\b|\b\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?\b|\b\d{4}-\d{2}-\d{2}\b))+\s*$",
        "",
        s,
        flags=re.IGNORECASE,
    ).strip(" ,.-")
    s = re.sub(r"^встреч(?:у|е|и)\b", "встреча", s, flags=re.IGNORECASE)
    s = re.sub(r"^собрани(?:е|я|ю)\b", "собрание", s, flags=re.IGNORECASE)
    s = re.sub(r"^(?:созвон(?:а|е)?|звонок|колл)\b", "созвон", s, flags=re.IGNORECASE)
    s = re.sub(r"^мероприяти(?:е|я)\b", "мероприятие", s, flags=re.IGNORECASE)
    s = re.sub(
        r"^(встреча|собрание|созвон|мероприятие)\s+\1\b",
        r"\1",
        s,
        flags=re.IGNORECASE,
    ).strip()
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


def _normalize_meeting_title_canonical(title: Any, meeting_kind: Any) -> str:
    return _normalize_shortlist_title(title, meeting_kind)


def _normalize_meeting_match_text(value: Any) -> str:
    s = str(value or "").strip().lower().replace("ё", "е")
    if not s:
        return ""
    s = s.replace("→", " ")
    s = re.sub(r"\s*-\>\s*", " ", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\bвстреч(?:у|е|и)\b", "встреча", s)
    s = re.sub(r"\bсобрани(?:я|ю)\b", "собрание", s)
    s = re.sub(r"\bсозвон(?:а|е)?\b", "созвон", s)
    s = re.sub(r"\bмероприяти(?:я)\b", "мероприятие", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


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
    query_norm = _normalize_meeting_match_text(query)
    time_tokens: list[str] = []
    for m in re.finditer(r"\b([01]?\d|2[0-3])(?::(\d{2}))?\b", query):
        hh = int(m.group(1))
        mm = int(m.group(2) or "0")
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            time_tokens.append(f"{hh:02d}:{mm:02d}")
    text_tokens = [tok for tok in re.split(r"\s+", query_norm) if tok and len(tok) >= 2 and not re.fullmatch(r"\d{1,2}(:\d{2})?", tok)]

    filtered: list[Dict[str, Any]] = []
    for c in candidates:
        title = _normalize_meeting_match_text(_normalize_meeting_title_canonical(c.get("title"), c.get("meeting_kind")))
        description = _normalize_meeting_match_text(c.get("description"))
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


def _normalize_timeblock_target_hint(raw_hint: str) -> str:
    raw = str(raw_hint or "").strip().lower().replace("ё", "е")
    if not raw:
        return ""
    cleaned = re.sub(r"\b(измени|перенеси|исправь|обнови|блок|времени|таймблок|на|в)\b", " ", raw)
    cleaned = re.sub(r"[^\w\s:.-]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _timeblock_update_repeat_search(user_id: str, target_hint: str = "") -> Dict[str, Any]:
    url = _timeblock_search_url()
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


def _timeblock_update_target_block_id(command: Dict[str, Any]) -> str:
    return str(
        command.get("time_block_id")
        or command.get("timeblock_update_source_block_id")
        or _command_value(command, "time_block_id")
        or ""
    ).strip()


def _timeblock_update_source_value(command: Dict[str, Any], *keys: str) -> Any:
    entities = command.get("entities")
    entities = entities if isinstance(entities, dict) else {}
    for key in keys:
        if key in command and command.get(key) is not None:
            return command.get(key)
        if key in entities and entities.get(key) is not None:
            return entities.get(key)
        prefixed = f"timeblock_update_source_{key}"
        if prefixed in command and command.get(prefixed) is not None:
            return command.get(prefixed)
        if prefixed in entities and entities.get(prefixed) is not None:
            return entities.get(prefixed)
    return None


def _timeblock_update_current_comment(command: Dict[str, Any]) -> str:
    return str(
        _timeblock_update_source_value(command, "comment")
        or command.get("timeblock_update_source_comment")
        or command.get("comment_text")
        or command.get("comment")
        or ""
    ).strip()


def _timeblock_update_next_comment(command: Dict[str, Any]) -> str:
    if "comment_text" in command or "comment" in command:
        return str(command.get("comment_text") or command.get("comment") or "").strip()
    entities = command.get("entities")
    entities = entities if isinstance(entities, dict) else {}
    if "comment_text" in entities or "comment" in entities:
        return str(entities.get("comment_text") or entities.get("comment") or "").strip()
    return _timeblock_update_current_comment(command)


def _timeblock_update_has_source(command: Dict[str, Any]) -> bool:
    return bool(_timeblock_update_target_block_id(command))


def _timeblock_update_hydrate_from_candidate(command: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    source_block_id = str(candidate.get("time_block_id") or "").strip()
    source_date = str(candidate.get("start_at_date") or "").strip()
    source_time = _normalize_time_hhmm_local(candidate.get("start_at_time"))
    source_duration = candidate.get("duration_minutes")
    source_comment = str(candidate.get("comment") or "").strip()
    source_title = str(candidate.get("title") or "Блок времени").strip() or "Блок времени"
    out["time_block_id"] = source_block_id
    out["timeblock_update_source_block_id"] = source_block_id
    out["timeblock_update_source_date"] = source_date
    out["timeblock_update_source_time"] = source_time
    out["timeblock_update_source_duration"] = source_duration
    out["timeblock_update_source_comment"] = source_comment
    out["timeblock_update_source_title"] = source_title
    out["start_at_date"] = source_date
    out["start_at_time"] = source_time
    out["duration_minutes"] = source_duration
    entities["time_block_id"] = source_block_id
    entities["timeblock_update_source_block_id"] = source_block_id
    entities["timeblock_update_source_date"] = source_date
    entities["timeblock_update_source_time"] = source_time
    entities["timeblock_update_source_duration"] = source_duration
    entities["timeblock_update_source_comment"] = source_comment
    entities["timeblock_update_source_title"] = source_title
    entities["start_at_date"] = source_date
    entities["start_at_time"] = source_time
    entities["duration_minutes"] = source_duration
    old_date = str(_timeblock_update_source_value(command, "timeblock_update_new_date") or "").strip()
    old_time = _normalize_time_hhmm_local(_timeblock_update_source_value(command, "timeblock_update_new_time"))
    old_duration = _timeblock_update_source_value(command, "timeblock_update_new_duration")
    if old_date:
        out["timeblock_update_new_date"] = old_date
        out["start_at_date"] = old_date
        entities["timeblock_update_new_date"] = old_date
        entities["start_at_date"] = old_date
    if old_time:
        out["timeblock_update_new_time"] = old_time
        out["start_at_time"] = old_time
        entities["timeblock_update_new_time"] = old_time
        entities["start_at_time"] = old_time
    if old_duration is not None and str(old_duration).strip() != "":
        out["timeblock_update_new_duration"] = old_duration
        out["duration_minutes"] = old_duration
        entities["timeblock_update_new_duration"] = old_duration
        entities["duration_minutes"] = old_duration
    out["entities"] = entities
    return out


def _timeblock_update_source_is_hydrated(command: Dict[str, Any]) -> bool:
    block_id = _timeblock_update_target_block_id(command)
    date_value = str(_timeblock_update_source_value(command, "date", "start_at_date") or "").strip()
    time_value = _normalize_time_hhmm_local(_timeblock_update_source_value(command, "time", "start_at_time", "start_time"))
    duration_value = _timeblock_update_source_value(command, "duration", "duration_minutes", "duration_min")
    has_duration = duration_value is not None and str(duration_value).strip() != ""
    return bool(block_id and date_value and time_value and has_duration)


def _meeting_update_edit_flow_active(command: Dict[str, Any], active_missing_field: str) -> bool:
    if not _is_meeting_update_intent(str(command.get("intent") or "")):
        return False
    if bool(command.get("__meeting_update_final_confirmed")):
        return False
    normalized_missing_field = normalize_clarification_field_name(str(active_missing_field or ""))
    return normalized_missing_field in {
        _MEETING_UPDATE_TARGET_CONFIRM_FIELD,
        _MEETING_UPDATE_TARGET_SELECT_FIELD,
        _MEETING_UPDATE_TARGET_REFINE_FIELD,
        _MEETING_UPDATE_EDIT_CHOICE_FIELD,
        _MEETING_UPDATE_EDIT_DATE_FIELD,
        _MEETING_UPDATE_EDIT_TIME_FIELD,
        _MEETING_UPDATE_EDIT_DURATION_FIELD,
        _MEETING_UPDATE_EDIT_COMMENT_FIELD,
        _MEETING_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
        _MEETING_UPDATE_EDIT_DATE_TIME_TIME_FIELD,
        _MEETING_UPDATE_FINAL_CONFIRM_FIELD,
    }


def _timeblock_update_edit_flow_active(command: Dict[str, Any], active_missing_field: str) -> bool:
    if not _is_timeblock_update_intent(str(command.get("intent") or "")):
        return False
    if bool(command.get("__timeblock_update_final_confirmed")):
        return False
    normalized_missing_field = normalize_clarification_field_name(str(active_missing_field or ""))
    return normalized_missing_field in {
        _TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD,
        _TIMEBLOCK_UPDATE_TARGET_SELECT_FIELD,
        _TIMEBLOCK_UPDATE_TARGET_REFINE_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_CHOICE_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_DATE_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_TIME_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_DURATION_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_COMMENT_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_TIME_FIELD,
        _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD,
    }


def _timeblock_update_candidate_shortlist_from_command(command: Dict[str, Any]) -> list[Dict[str, Any]]:
    raw = command.get("__timeblock_update_candidates")
    if isinstance(raw, list):
        return [dict(item) for item in raw if isinstance(item, dict)]
    entities = command.get("entities")
    entities = entities if isinstance(entities, dict) else {}
    raw_entities = entities.get("__timeblock_update_candidates")
    if isinstance(raw_entities, list):
        return [dict(item) for item in raw_entities if isinstance(item, dict)]
    return []


def _timeblock_update_store_candidate_shortlist(command: Dict[str, Any], candidates: list[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    normalized = [dict(c) for c in candidates[:_MEETING_UPDATE_SHORTLIST_MAX] if isinstance(c, dict)]
    out["__timeblock_update_candidates"] = normalized
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    entities["__timeblock_update_candidates"] = normalized
    out["entities"] = entities
    return out


def _timeblock_update_render_candidate_shortlist(candidates: list[Dict[str, Any]]) -> str:
    lines = ["Нашёл несколько блоков времени. Уточните номер:"]
    for idx, c in enumerate(candidates[:_MEETING_UPDATE_SHORTLIST_MAX], start=1):
        title = str(c.get("title") or "Блок времени").strip() or "Блок времени"
        d = _format_date_ru(c.get("start_at_date"))
        t = _normalize_time_hhmm_local(c.get("start_at_time")) or str(c.get("start_at_time") or "-")
        dur = _format_duration_human(c.get("duration_minutes"))
        comment = str(c.get("comment") or "").strip()
        suffix = f" — {comment}" if comment else ""
        lines.append(f"{idx}. {title} — {d}, {t}, {dur}{suffix}")
    lines.append("")
    lines.append("Напишите номер варианта или уточнение текстом.")
    return "\n".join(lines)


def _timeblock_update_target_confirmation_question(command: Dict[str, Any]) -> str:
    title = str(_timeblock_update_source_value(command, "title") or "Блок времени").strip() or "Блок времени"
    date_value = _format_date_ru(_timeblock_update_source_value(command, "date", "start_at_date"))
    time_value = str(_timeblock_update_source_value(command, "time", "start_at_time", "start_time") or "").strip() or "-"
    duration_value = _format_duration_human(_timeblock_update_source_value(command, "duration", "duration_minutes", "duration_min"))
    return f"Вы имеете в виду блок времени: {title} — {date_value}, {time_value}, {duration_value}?"


def _timeblock_update_target_refinement_question() -> str:
    return (
        "Не нашёл подходящий блок времени.\n"
        "Уточните дату и время или другой ориентир.\n"
        "Например: 'блок 16 числа в 13:00' или 'блок с комментарием фокус'."
    )


def _timeblock_update_field_choice_question() -> str:
    return "Что изменить?"


def _timeblock_update_active_edit_choice(command: Dict[str, Any]) -> str:
    return str(command.get(_TIMEBLOCK_UPDATE_ACTIVE_EDIT_KEY) or "").strip().lower()


def _timeblock_update_field_choice_from_metadata(metadata: Optional[Dict[str, Any]]) -> str:
    meta = metadata if isinstance(metadata, dict) else {}
    callback = meta.get("callback_query") if isinstance(meta.get("callback_query"), dict) else {}
    callback_data = str(callback.get("data") or "").strip()
    if not callback_data.startswith("clarify:v1:timeblock_update_edit:"):
        return ""
    value = callback_data.rsplit(":", 1)[-1].strip().lower()
    if value in {"date", "time", "duration", "comment", "date_time", "cancel"}:
        return value
    return ""


def _timeblock_update_apply_field_choice(command: Dict[str, Any], choice: str) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    selected = str(choice or "").strip().lower()
    out[_TIMEBLOCK_UPDATE_ACTIVE_EDIT_KEY] = selected
    out.pop(_TIMEBLOCK_UPDATE_PENDING_COMBO_KEY, None)
    if selected == "date_time":
        out[_TIMEBLOCK_UPDATE_PENDING_COMBO_KEY] = True
    return out


def _timeblock_update_edit_question(command: Dict[str, Any]) -> str:
    active = _timeblock_update_active_edit_choice(command)
    if active == "date":
        return "На какую дату перенести/изменить блок?"
    if active == "time":
        return "На какое время перенести/изменить блок?"
    if active == "duration":
        return "На сколько минут изменить длительность блока?"
    if active == "comment":
        current_comment = _timeblock_update_current_comment(command)
        return (
            "Введите новый комментарий.\n\n"
            "Текущий комментарий:\n"
            f"{current_comment or '-'}\n\n"
            "Если передумали — нажмите «Оставить без изменений» или «Отмена»."
        )
    if active == "date_time":
        if bool(command.get(_TIMEBLOCK_UPDATE_PENDING_COMBO_KEY)):
            return "На какую дату перенести/изменить блок?"
        return "На какое время перенести/изменить блок?"
    return _timeblock_update_field_choice_question()


def _timeblock_update_final_confirmation_question(command: Dict[str, Any]) -> str:
    current_title = str(_timeblock_update_source_value(command, "title") or "Блок времени").strip() or "Блок времени"
    current_date = _format_date_ru(_timeblock_update_source_value(command, "date", "start_at_date"))
    current_time = str(_timeblock_update_source_value(command, "time", "start_at_time", "start_time") or "").strip() or "-"
    current_duration = _format_duration_human(_timeblock_update_source_value(command, "duration", "duration_minutes", "duration_min"))
    current_comment = _timeblock_update_current_comment(command) or "-"
    new_date = _format_date_ru(_timeblock_update_source_value(command, "timeblock_update_new_date") or _timeblock_update_source_value(command, "date", "start_at_date"))
    new_time = str(_timeblock_update_source_value(command, "timeblock_update_new_time") or _timeblock_update_source_value(command, "time", "start_at_time", "start_time") or "").strip() or "-"
    new_duration = _format_duration_human(_timeblock_update_source_value(command, "timeblock_update_new_duration") or _timeblock_update_source_value(command, "duration", "duration_minutes", "duration_min"))
    new_comment = _timeblock_update_next_comment(command) or "-"
    lines = [
        "Проверьте, всё ли верно.",
        f"Блок времени: {current_title}",
        "Текущие параметры:",
        f"Дата: {current_date}",
        f"Время: {current_time}",
        f"Длительность: {current_duration}",
        f"Комментарий: {current_comment}",
        "",
        "Новые параметры:",
        f"Дата: {new_date}",
        f"Время: {new_time}",
        f"Длительность: {new_duration}",
        f"Комментарий: {new_comment}",
        "",
        "Подтвердить изменение?",
    ]
    return "\n".join(lines)

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
    meeting_kind = str(candidate.get("meeting_kind") or "").strip().lower()
    out["calendar_event_id"] = str(candidate.get("calendar_event_id") or "").strip()
    out["meeting_title"] = _normalize_meeting_title_canonical(candidate.get("title"), meeting_kind)
    out["start_at_date"] = str(candidate.get("start_at_date") or "").strip()
    out["start_at_time"] = _normalize_time_hhmm_local(candidate.get("start_at_time"))
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
    if "коммент" in s:
        return _MEETING_COMMENT_TEXT_FIELD
    if "дат" in s:
        return "start_at_date"
    if "длит" in s or "мин" in s or "час" in s:
        return "duration_minutes"
    if "врем" in s:
        return "start_at_time"
    return ""


def _temporal_edit_target_field_from_metadata(metadata: Optional[Dict[str, Any]]) -> str:
    meta = metadata if isinstance(metadata, dict) else {}
    callback = meta.get("callback_query") if isinstance(meta.get("callback_query"), dict) else {}
    callback_data = str(callback.get("data") or "").strip()
    if not callback_data.startswith("clarify:v1:temporal_edit:"):
        return ""
    value = callback_data.rsplit(":", 1)[-1].strip().lower()
    if value == "date":
        return "start_at_date"
    if value == "time":
        return "start_at_time"
    if value == "duration":
        return "duration_minutes"
    if value == "comment":
        return _MEETING_COMMENT_TEXT_FIELD
    return ""


def _duration_quick_action_from_metadata(metadata: Optional[Dict[str, Any]], *prefixes: str) -> str:
    meta = metadata if isinstance(metadata, dict) else {}
    callback = meta.get("callback_query") if isinstance(meta.get("callback_query"), dict) else {}
    callback_data = str(callback.get("data") or "").strip()
    allowed = {"15", "30", "45", "60", "other", "cancel"}
    for prefix in prefixes:
        normalized_prefix = str(prefix or "").strip()
        if normalized_prefix and callback_data.startswith(normalized_prefix):
            value = callback_data[len(normalized_prefix) :].strip().lower()
            if value in allowed:
                return value
    return ""


def _temporal_comment_callback_action_from_metadata(metadata: Optional[Dict[str, Any]]) -> str:
    meta = metadata if isinstance(metadata, dict) else {}
    callback = meta.get("callback_query") if isinstance(meta.get("callback_query"), dict) else {}
    callback_data = str(callback.get("data") or "").strip()
    if callback_data == "clarify:v1:temporal_comment:keep":
        return "keep"
    if callback_data == "clarify:v1:temporal_comment:clear":
        return "clear"
    return ""


def _temporal_active_edit_question(command: Dict[str, Any]) -> str:
    active_edit_field = normalize_clarification_field_name(
        str(command.get(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY) or "")
    )
    if active_edit_field == "start_at_date":
        return "На какую дату запланировать?"
    if active_edit_field == "start_at_time":
        return "На какое время запланировать?"
    if active_edit_field == "duration_minutes":
        return "На сколько минут запланировать?"
    if active_edit_field == _MEETING_COMMENT_TEXT_FIELD:
        current_comment = str(_command_value(command, "comment_text", "comment", "description", "notes") or "").strip()
        return (
            "Введите новый комментарий.\n\n"
            "Текущий комментарий:\n"
            f"{current_comment or '-'}\n\n"
            "Если передумали — нажмите «Оставить без изменений» или «Отмена»."
        )
    return _clarification_question_for_field(_TEMPORAL_EDIT_FIELD)


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


def _is_task_or_inbox_route_intent(intent: str) -> bool:
    normalized = str(intent or "").strip().lower()
    if not normalized:
        return False
    if normalized in {"create_inbox", "inbox.create", "task", "task.create", "create_task", "task_create"}:
        return True
    return normalized.startswith("task.")


def _guard_temporal_not_routed_to_task_or_inbox(
    command: Dict[str, Any],
    *,
    source_text: str,
    request_id: str,
) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    intent = _normalized_intent(out)
    source = str(source_text or "").strip()
    if not intent or not source:
        return out
    if not _is_task_or_inbox_route_intent(intent):
        return out
    if _looks_like_timeblock_task_allocation_request(source):
        LOG.error(
            "legacy_route_used",
            extra={
                "request_id": request_id,
                "flow_id": request_id,
                "intent": intent,
                "route_target": "task_or_inbox_fallback",
                "source_text": source,
                "normalized_intent": "timeblock.create",
            },
        )
        return _apply_timeblock_create_semantic_routing(out, source)
    if _has_explicit_task_create_keywords(source) or _looks_like_generic_task_phrase(source):
        return out
    if not _has_meeting_create_keywords(source):
        return out

    LOG.error(
        "legacy_route_used",
        extra={
            "request_id": request_id,
            "flow_id": request_id,
            "intent": intent,
            "route_target": "task_or_inbox_fallback",
            "source_text": source,
            "normalized_intent": "meeting.create",
        },
    )
    out["intent"] = "meeting.create"
    return out


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
    locked = bool(command.get("__meeting_kind_locked"))
    explicit = str(_command_value(command, "meeting_kind", "event_kind") or "").strip().lower()
    if explicit in _MEETING_KIND_VARIANTS:
        return explicit
    if locked:
        return ""
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


def _apply_default_duration_for_meeting_create(command: Dict[str, Any], *, allow_default: bool = True) -> Dict[str, Any]:
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
    if not has_duration:
        inferred_duration = _extract_duration_minutes_from_text(source_text)
        if inferred_duration is not None:
            out["duration_minutes"] = int(inferred_duration)
            entities = out.get("entities")
            entities = dict(entities) if isinstance(entities, dict) else {}
            entities["duration_minutes"] = int(inferred_duration)
            out["entities"] = entities
        has_duration = _has_non_empty_value(_command_value(out, "duration_minutes", "duration_min", "duration_mins", "duration"))
    if has_duration:
        return out
    if not allow_default:
        return out
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    out["duration_minutes"] = int(_DEFAULT_MEETING_DURATION_MINUTES)
    entities["duration_minutes"] = int(_DEFAULT_MEETING_DURATION_MINUTES)
    out["entities"] = entities
    return out


def _extract_duration_minutes_from_text(text: str) -> Optional[int]:
    s = str(text or "").strip().lower().replace("ё", "е")
    if not s:
        return None
    s = re.sub(r"\s+", " ", s)
    # "на 45 минут", "30 минут", "1 час", "1.5 часа"
    m = re.search(r"\b(\d{1,4}(?:[.,]\d+)?)\s*(мин|минута|минуты|минут|час|часа|часов)\b", s)
    if m:
        numeric_raw = str(m.group(1) or "").replace(",", ".")
        value = float(numeric_raw)
        unit = str(m.group(2) or "")
        if value <= 0:
            return None
        if unit.startswith("час"):
            return int(round(value * 60))
        return int(round(value))
    if re.search(r"\bполтора\s+час(?:а|ов)?\b", s):
        return 90
    # "на полчаса" / "полчаса" / "пол часа"
    if re.search(r"\b(?:на\s+)?пол\s*часа\b", s):
        return 30
    if re.search(r"\b(выдел|запланир|постав|забронир|зарезервир)\w*(?:\s+\w+){0,3}\s+час\b", s):
        return 60
    return None


def _parse_duration_minutes_reply(raw: Any) -> Optional[int]:
    text = str(raw or "").strip().lower().replace("ё", "е")
    if not text:
        return None
    compact = re.sub(r"\s+", " ", text)
    direct_number = re.fullmatch(r"(\d{1,4})", compact)
    if direct_number:
        value = int(direct_number.group(1))
        return value if value > 0 else None
    minute_match = re.fullmatch(r"(\d{1,4})\s*(мин|минута|минуты|минут)\b", compact)
    if minute_match:
        value = int(minute_match.group(1))
        return value if value > 0 else None
    hour_match = re.fullmatch(r"(\d{1,3})\s*(час|часа|часов)\b", compact)
    if hour_match:
        value = int(hour_match.group(1))
        return value * 60 if value > 0 else None
    return _extract_duration_minutes_from_text(compact)


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
    comment_value = _command_value(out, "comment_text", "comment", "description", "notes", "note")
    comment_text = str(comment_value).strip() if _has_non_empty_value(comment_value) else ""
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
        "comment": comment_text,
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
    lines.append(f"Комментарий: {draft.get('comment') or '-'}")
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
            str(draft.get("comment") or ""),
            str(_meeting_update_target_event_id(command) or ""),
        ]
    )


def _task_create_draft_from_command(command: Dict[str, Any]) -> Dict[str, Any]:
    cmd = command if isinstance(command, dict) else {}
    title = _command_value(cmd, "title", "task_title", "text")
    due_date = _command_value(cmd, "planned_at", "due_date", "date", "when")
    priority = _command_value(cmd, "priority")
    comment_text = _command_value(cmd, "comment_text", "comment", "description", "notes", "note")
    draft: Dict[str, Any] = {
        "kind": "task_create",
        "task_text": str(title).strip() if title is not None else "",
    }
    if _has_non_empty_value(due_date):
        due_text = str(due_date).strip()
        normalized_due = _normalize_clarification_date_to_iso(due_text, prefer_future_for_ambiguous=False)
        if normalized_due:
            draft["due_date"] = str(normalized_due).strip()
    if _has_non_empty_value(priority):
        draft["priority"] = str(priority).strip()
    if _has_non_empty_value(comment_text):
        draft["comment"] = str(comment_text).strip()
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
        lines.append(f"Срок: {_format_date_ru(draft.get('due_date'))}")
    if draft.get("priority"):
        lines.append(f"Приоритет: {draft.get('priority')}")
    lines.append(f"Комментарий: {draft.get('comment') or '-'}")
    lines.append("")
    lines.append("Создать задачу?")
    return "\n".join(lines)


def _task_create_duplicate_question(existing_task: Dict[str, Any]) -> str:
    duplicate_date = str(existing_task.get("planned_at") or "").strip()
    duplicate_parent = str(existing_task.get("parent_task_id") or "").strip()
    if duplicate_date:
        lead_text = f"Похоже, такая задача уже есть на {duplicate_date[:10]}"
    elif duplicate_parent:
        lead_text = "Такая задача уже есть без срока"
    else:
        lead_text = "Такая задача уже есть в InBox"
    question = (
        f"{lead_text}:\n"
        f"№ {str(existing_task.get('id') or '').strip() or '-'} — {str(existing_task.get('title') or '').strip() or '-'}\n\n"
        "Создать ещё одну?"
    )
    return question


def _return_task_create_clarification(
    *,
    request_id: str,
    query_text: str,
    command: Dict[str, Any],
    db: Optional[LocalMainDb],
    context_key: str,
    app_id: str,
    tenant_id: str,
    user_id: str,
    source_message_id: Optional[str],
    idempotency_key: Optional[str],
) -> Dict[str, Any]:
    normalized_command = _attach_task_create_draft(command)
    raw_due_date = str(normalized_command.get("__task_create_raw_due_date") or "").strip()
    raw_planned_at = str(normalized_command.get("__task_create_raw_planned_at") or "").strip()
    current_due_date = str(_command_value(normalized_command, "due_date", "date", "when") or "").strip()
    resolved_planned_at = str(_command_value(normalized_command, "planned_at") or "").strip()
    if (raw_due_date or raw_planned_at or current_due_date) and not resolved_planned_at:
        question = "На какую дату поставить задачу? Пришлите дату сообщением."
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text,
            command=normalized_command,
            question=question,
            rec_payload={
                "outcome": "needs_clarification",
                "needs_clarification": True,
                "missing_field": _TASK_CREATE_DATE_FIELD,
            },
            missing_field_for_log=_TASK_CREATE_DATE_FIELD,
            db=db,
            context_key=context_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent=str(normalized_command.get("intent") or "task.create"),
            payload=normalized_command,
            source_message_id=source_message_id,
            idempotency_key=idempotency_key,
        )
    duplicate_precheck = _task_create_duplicate_precheck(user_id, normalized_command)
    existing_task = duplicate_precheck.get("existing_task") if isinstance(duplicate_precheck.get("existing_task"), dict) else {}
    if bool(duplicate_precheck.get("duplicate_found")) and existing_task:
        normalized_command = dict(normalized_command)
        normalized_command["__task_create_duplicate_existing_task"] = dict(existing_task)
        question = _task_create_duplicate_question(existing_task)
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text,
            command=normalized_command,
            question=question,
            rec_payload={
                "outcome": "needs_clarification",
                "needs_clarification": True,
                "missing_field": _TASK_CREATE_DUPLICATE_CONFIRM_FIELD,
            },
            missing_field_for_log=_TASK_CREATE_DUPLICATE_CONFIRM_FIELD,
            db=db,
            context_key=context_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent=str(normalized_command.get("intent") or "task.create"),
            payload=normalized_command,
            source_message_id=source_message_id,
            idempotency_key=idempotency_key,
        )
    question = _task_create_confirmation_question(normalized_command)
    return _return_clarification_with_persist(
        request_id=request_id,
        query_text=query_text,
        command=normalized_command,
        question=question,
        rec_payload={
            "outcome": "needs_clarification",
            "needs_clarification": True,
            "missing_field": _TASK_CREATE_CONFIRM_FIELD,
        },
        missing_field_for_log=_TASK_CREATE_CONFIRM_FIELD,
        db=db,
        context_key=context_key,
        app_id=app_id,
        tenant_id=tenant_id,
        user_id=user_id,
        intent=str(normalized_command.get("intent") or "task.create"),
        payload=normalized_command,
        source_message_id=source_message_id,
        idempotency_key=idempotency_key,
    )


def _resolve_rec_empty_result(
    *,
    request_id: str,
    query_text: str,
    rec_res: Dict[str, Any],
    command: Dict[str, Any],
    explicit_memory_query: bool,
    db: Optional[LocalMainDb],
    context_key: str,
    app_id: str,
    tenant_id: str,
    user_id: str,
    source_message_id: Optional[str],
    idempotency_key: Optional[str],
) -> Dict[str, Any]:
    route_flags = _route_flags_for_text(query_text)
    generic_task_candidate = _looks_like_generic_task_text(query_text)
    LOG.info(
        "memory_fallback_before_return",
        extra={
            "request_id": request_id,
            "trace_id": request_id,
            "raw_text": str(query_text or "").strip(),
            "explicit_memory_query": bool(explicit_memory_query),
            "generic_task_candidate": bool(generic_task_candidate),
            "has_date": bool(route_flags["has_date"]),
            "route_rules_version": _ROUTE_RULES_VERSION,
        },
    )
    if not explicit_memory_query:
        if generic_task_candidate or (bool(route_flags["has_date"]) and not bool(route_flags["has_calendar_marker"]) and not bool(route_flags["has_timeblock_marker"])):
            command["intent"] = "task.create"
            command = _normalize_task_create_from_source(command, query_text)
            _log_runtime_route_selected(
                request_id=request_id,
                raw_text=query_text,
                route="task.create",
                reason="rec_empty_hard_block_to_task_create",
            )
            LOG.info(
                "generic_task_fallback_selected",
                extra={
                    "request_id": request_id,
                    "trace_id": request_id,
                    "raw_text": str(query_text or "").strip(),
                    "title": str(_command_value(command, "title", "task_title", "text") or "").strip(),
                    "planned_at": str(_command_value(command, "planned_at", "due_date", "date", "when") or "").strip(),
                    "route": "task.create",
                },
            )
            return _return_task_create_clarification(
                request_id=request_id,
                query_text=query_text,
                command=command,
                db=db,
                context_key=context_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )
        return {
            "outcome": "unrecognized",
            "user_message": "Не понял, что сделать с этим сообщением. Создать задачу или событие?",
            "rec": rec_res,
            "command": command,
        }
    return {
        "outcome": "rec_empty",
        "user_message": map_failure_to_user_message("rec_empty", details=rec_res),
        "rec": rec_res,
        "command": command,
    }


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
        return "Проверьте параметры и подтвердите планирование."
    if field_name == _TEMPORAL_EDIT_FIELD:
        return "Что исправить? Дата, Время, Длительность или Комментарий."
    if field_name == _TASK_CREATE_CONFIRM_FIELD:
        return "Подтверди создание задачи: да или нет."
    if field_name == _TASK_CREATE_DUPLICATE_CONFIRM_FIELD:
        return "Подтверди создание дубликата задачи: да или нет."
    if field_name == _TASK_CREATE_EDIT_FIELD:
        return "Какую задачу создать?"
    if field_name == _TASK_CREATE_COMMENT_FIELD:
        return "Введите комментарий к задаче."
    if field_name == _AMBIGUOUS_CREATE_FIELD:
        return "Что создать: событие в календаре, блок времени или задачу?"
    if field_name == "meeting_create_details":
        return "Какое событие в календаре создать?"
    if field_name == "timeblock_create_details":
        return "Какой блок времени создать?"
    if field_name == _MEETING_UPDATE_TARGET_CONFIRM_FIELD:
        return "Это та встреча, которую нужно перенести?"
    if field_name == _TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD:
        return "Это тот блок времени, который нужно изменить?"
    if field_name == _MEETING_UPDATE_EDIT_CHOICE_FIELD:
        return _meeting_update_field_choice_question()
    if field_name == _TIMEBLOCK_UPDATE_EDIT_CHOICE_FIELD:
        return _timeblock_update_field_choice_question()
    if field_name in {
        _MEETING_UPDATE_EDIT_DATE_FIELD,
        _MEETING_UPDATE_EDIT_TIME_FIELD,
        _MEETING_UPDATE_EDIT_DURATION_FIELD,
        _MEETING_UPDATE_EDIT_COMMENT_FIELD,
        _MEETING_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
        _MEETING_UPDATE_EDIT_DATE_TIME_TIME_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_DATE_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_TIME_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_DURATION_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_COMMENT_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_TIME_FIELD,
    }:
        return "Что изменить?"
    if field_name == _MEETING_UPDATE_TARGET_IDENTIFICATION_FIELD:
        return "Уточните, какую встречу нужно перенести."
    if field_name == _MEETING_UPDATE_TARGET_SELECT_FIELD:
        return "Нашёл несколько встреч. Напишите номер или уточнение."
    if field_name == _MEETING_UPDATE_TARGET_REFINE_FIELD:
        return _meeting_update_target_refinement_question()
    if field_name == _TIMEBLOCK_UPDATE_TARGET_SELECT_FIELD:
        return "Нашёл несколько блоков времени. Напишите номер или уточнение."
    if field_name == _TIMEBLOCK_UPDATE_TARGET_REFINE_FIELD:
        return _timeblock_update_target_refinement_question()
    if field_name == _MEETING_UPDATE_FINAL_CONFIRM_FIELD:
        return "Подтвердить изменение встречи: да или нет."
    if field_name == _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD:
        return "Подтвердить изменение блока времени: да или нет."
    if field_name == "meeting_update_target_ref":
        return "Уточните, какую встречу нужно перенести."
    if field_name == "timeblock_update_target_ref":
        return "Уточните, какой блок времени нужно изменить."
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
        if normalized_field == _MEETING_COMMENT_TEXT_FIELD:
            return "Какой комментарий добавить?"
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


def _lock_meeting_kind_for_create(command: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    if _normalized_intent(out) != "meeting.create":
        return out

    explicit = str(_command_value(out, "meeting_kind", "event_kind") or "").strip().lower()
    if explicit in _MEETING_KIND_VARIANTS:
        locked_kind = explicit
    else:
        source_text = str(
            out.get("__source_text")
            or _command_value(out, "text")
            or ""
        ).strip()
        inferred = _extract_meeting_kind_from_text(source_text)
        locked_kind = inferred if inferred in _MEETING_KIND_VARIANTS else "встреча"

    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    entities["meeting_kind"] = locked_kind
    out["entities"] = entities
    out["meeting_kind"] = locked_kind
    out["__meeting_kind_locked"] = True
    return out


def _extract_meeting_participant_phrase(source_text: str) -> str:
    raw = str(source_text or "").strip()
    if not raw:
        return ""
    # Basic heuristic: keep short "с <name>" participant phrase for title.
    m = re.search(
        r"\bс\s+([A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё-]{1,30}(?:\s+[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё-]{1,30})?)\b",
        raw,
        flags=re.IGNORECASE,
    )
    if not m:
        return ""
    name = str(m.group(1) or "").strip(" ,.!?:;")
    if not name:
        return ""
    return f"с {name}"


def _enrich_meeting_create_title(command: Dict[str, Any], source_text: str) -> Dict[str, Any]:
    out = dict(command if isinstance(command, dict) else {})
    if _normalized_intent(out) != "meeting.create":
        return out
    kind = _resolve_meeting_kind(out) or "встреча"
    label = kind[:1].upper() + kind[1:]
    participant_phrase = _extract_meeting_participant_phrase(source_text)
    entities = out.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    current_title = str(_command_value(out, "title", "meeting_title") or "").strip()
    base_title = current_title or str(source_text or "").strip()
    cleaned = base_title.lower().replace("ё", "е")
    cleaned = cleaned.replace("→", " ")
    cleaned = re.sub(r"\s*-\>\s*", " ", cleaned)
    cleaned = re.sub(r"[\"'`]", " ", cleaned)
    cleaned = re.sub(
        r"\b(запланируй|запланирую|создай|поставь|назначь|сделай)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\b(сегодня|завтра|вечером|вечера|утром|утра|днем|дн[eе]м|дня|ночи)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\b(?:в|к)\s*\d{1,2}(?::\d{2})?\s*(?:час(?:а|ов)?|утра|дня|вечера|ночи)?\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\b(встреча|собрание|созвон|мероприятие)\s+\1\b", r"\1", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.-")

    normalized_clean = _normalize_shortlist_title(cleaned, kind)
    normalized_low = normalized_clean.lower().replace("ё", "е")
    label_low = label.lower().replace("ё", "е")
    if not normalized_clean or normalized_low in {"", label_low}:
        enriched_title = label
    else:
        enriched_title = normalized_clean
    if participant_phrase:
        participant_norm = participant_phrase.lower().replace("ё", "е")
        if participant_norm not in enriched_title.lower().replace("ё", "е"):
            enriched_title = f"{label} {participant_phrase}".strip() if enriched_title == label else f"{enriched_title} {participant_phrase}".strip()

    out["title"] = enriched_title
    entities["title"] = enriched_title
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
    suppress_send: bool = False,
) -> Dict[str, Any]:
    payload_to_store = dict(payload if isinstance(payload, dict) else {})
    command_for_reply = dict(command if isinstance(command, dict) else {})
    effective_intent = str(intent or payload_to_store.get("intent") or command_for_reply.get("intent") or "").strip()
    if _is_meeting_update_intent(effective_intent):
        payload_to_store = _meeting_update_recompute_has_changes(payload_to_store)
        command_for_reply = _meeting_update_recompute_has_changes(command_for_reply)
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
    if normalize_clarification_field_name(missing_field_for_log) == _TEMPORAL_CONFIRM_FIELD and not suppress_send:
        draft = payload_to_store.get("__temporal_draft") if isinstance(payload_to_store, dict) else {}
        draft = draft if isinstance(draft, dict) else {}
        draft_id = str(
            payload_to_store.get("__temporal_summary_signature")
            or command_for_reply.get("__temporal_summary_signature")
            or ""
        ).strip()
        LOG.info(
            "temporal_summary_rendered",
            extra={
                "request_id": request_id,
                "trace_id": request_id,
                "flow_id": request_id,
                "session_id": context_key,
                "user_id": user_id,
                "state": _TEMPORAL_CONFIRM_FIELD,
                "scenario": "temporal_create",
                "draft_id": draft_id,
                "summary_sent": True,
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
        "telemetry": {"suppress_send": True} if suppress_send else {},
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
    metadata: Optional[Dict[str, Any]] = None,
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
        LOG.info(
            "voice_transcribed_text",
            extra={
                "request_id": request_id,
                "trace_id": request_id,
                "user_id": user_id,
                "transcript_len": len(resolved_text),
                "transcript_preview": resolved_text[:200],
            },
        )

    if not resolved_text:
        # Not specified by tests; keep it user-friendly and non-technical.
        return {
            "outcome": "rejected",
            "user_message": "Скажи, пожалуйста, что нужно сделать.",
        }
    LOG.info(
        "telegram_text_route_entry",
        extra={
            "request_id": request_id,
            "trace_id": request_id,
            "raw_text": str(resolved_text or "").strip(),
            "adapter_mode": "runtime_core_direct",
            "route_rules_version": _ROUTE_RULES_VERSION,
        },
    )

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
    normalized_active_missing_field = normalize_clarification_field_name(active_missing_field)
    active_session_source_message_id = (
        str(active_session.get("source_message_id") or "").strip() if isinstance(active_session, dict) else ""
    )
    if (
        from_clarification
        and _is_confirm_field(normalized_active_missing_field)
        and confirm_state_probe == "unknown"
        and _looks_like_new_executable_command(resolved_text)
    ):
        active_payload = active_session.get("payload") if isinstance(active_session, dict) else {}
        active_payload = active_payload if isinstance(active_payload, dict) else {}
        active_source_text = str(active_payload.get("__source_text") or "").strip()
        if not active_source_text:
            active_entities = active_payload.get("entities")
            if isinstance(active_entities, dict):
                active_source_text = str(active_entities.get("text") or "").strip()
        active_render_request_id = str(active_payload.get("__temporal_summary_render_request_id") or "").strip()
        active_intent = str(active_session.get("intent") or active_payload.get("intent") or "").strip()
        if (
            normalized_active_missing_field == _TEMPORAL_CONFIRM_FIELD
            and active_render_request_id
            and active_render_request_id == request_id
            and active_source_text
            and active_source_text == resolved_text
        ):
            LOG.info(
                "temporal_confirm_replay_suppressed",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "session_state": normalized_active_missing_field,
                    "source_text": active_source_text,
                },
            )
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=resolved_text,
                command=active_payload,
                question="",
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
                intent=active_intent or "create_timeblock",
                payload=active_payload,
                source_message_id=source_message_id,
                idempotency_key=active_session_idempotency_key or idempotency_key,
                suppress_send=True,
            )
        if db is not None:
            db.delete_clarification_session(ctx_key)
        active_session = None
        from_clarification = False
        active_scenario = "none"
        active_missing_field = ""
        active_session_idempotency_key = ""
        LOG.info(
            "confirm_session_interrupted_by_new_command",
            extra={
                "request_id": request_id,
                "flow_id": request_id,
                "previous_missing_field": normalized_active_missing_field,
                "incoming_text": resolved_text,
            },
        )
    query_text = resolved_text
    temporal_edit_callback_target = _temporal_edit_target_field_from_metadata(metadata)
    temporal_comment_callback_action = _temporal_comment_callback_action_from_metadata(metadata)
    sync_conflict_action, sync_conflict_id = _sync_conflict_action_from_metadata(metadata)
    has_temporal_edit_callback = bool(temporal_edit_callback_target)
    if not from_clarification and sync_conflict_action and sync_conflict_id:
        action_result = _sync_conflict_apply_action(sync_conflict_id, sync_conflict_action)
        if sync_conflict_action == "edit" and bool(action_result.get("ok")) and isinstance(action_result.get("conflict"), dict):
            command = _attach_temporal_draft(_sync_conflict_command_for_edit(dict(action_result.get("conflict"))))
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=resolved_text,
                command=command,
                question=_temporal_confirmation_question(command),
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _TEMPORAL_CONFIRM_FIELD},
                missing_field_for_log=_TEMPORAL_CONFIRM_FIELD,
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
        if bool(action_result.get("ok")):
            return {
                "outcome": "success",
                "user_message": str(action_result.get("user_message") or "Конфликт обработан."),
                "command": None,
            }
        return {
            "outcome": "error",
            "user_message": "Не получилось обработать конфликт синхронизации. Попробуйте ещё раз.",
            "command": None,
        }
    if from_clarification:
        payload = active_session.get("payload") if isinstance(active_session, dict) else {}
        payload = payload if isinstance(payload, dict) else {}
        active_edit_field_from_payload = normalize_clarification_field_name(
            str(payload.get(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY) or "")
        )
        callback_is_stale = bool(
            (has_temporal_edit_callback or temporal_comment_callback_action)
            and active_session_source_message_id
            and incoming_message_id
            and active_session_source_message_id != incoming_message_id
        )
        if callback_is_stale:
            LOG.info(
                "temporal_edit_callback_stale_ignored",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "target_field": temporal_edit_callback_target,
                    "active_message_id": active_session_source_message_id,
                    "incoming_message_id": incoming_message_id,
                },
            )
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=resolved_text,
                command=payload,
                question="",
                rec_payload={
                    "outcome": "needs_clarification",
                    "needs_clarification": True,
                    "missing_field": normalized_active_missing_field or "clarification_value",
                },
                missing_field_for_log=normalized_active_missing_field or "clarification_value",
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent=str(active_session.get("intent") or payload.get("intent") or "unknown"),
                payload=payload,
                source_message_id=active_session_source_message_id or source_message_id,
                idempotency_key=active_session_idempotency_key or idempotency_key,
                suppress_send=True,
            )
        if (
            has_temporal_edit_callback
            and normalized_active_missing_field == _TEMPORAL_EDIT_FIELD
            and active_edit_field_from_payload in {"start_at_date", "start_at_time", "duration_minutes", _MEETING_COMMENT_TEXT_FIELD}
        ):
            if active_edit_field_from_payload == _MEETING_COMMENT_TEXT_FIELD:
                reminder_question = "Сначала введите новый комментарий или нажмите «Оставить без изменений»."
                LOG.info(
                    "temporal_edit_callback_blocked_while_comment_waiting",
                    extra={
                        "request_id": request_id,
                        "flow_id": request_id,
                        "target_field": temporal_edit_callback_target,
                        "active_edit_field": active_edit_field_from_payload,
                    },
                )
                return _return_clarification_with_persist(
                    request_id=request_id,
                    query_text=resolved_text,
                    command=payload,
                    question=reminder_question,
                    rec_payload={
                        "outcome": "needs_clarification",
                        "needs_clarification": True,
                        "missing_field": _TEMPORAL_EDIT_FIELD,
                    },
                    missing_field_for_log=_TEMPORAL_EDIT_FIELD,
                    db=db,
                    context_key=ctx_key,
                    app_id=app_id,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    intent=str(active_session.get("intent") or payload.get("intent") or "unknown"),
                    payload=payload,
                    source_message_id=active_session_source_message_id or source_message_id,
                    idempotency_key=active_session_idempotency_key or idempotency_key,
                )
            LOG.info(
                "temporal_edit_callback_ignored_while_waiting",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "target_field": temporal_edit_callback_target,
                    "active_edit_field": active_edit_field_from_payload,
                },
            )
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=resolved_text,
                command=payload,
                question="",
                rec_payload={
                    "outcome": "needs_clarification",
                    "needs_clarification": True,
                    "missing_field": _TEMPORAL_EDIT_FIELD,
                },
                missing_field_for_log=_TEMPORAL_EDIT_FIELD,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent=str(active_session.get("intent") or payload.get("intent") or "unknown"),
                payload=payload,
                source_message_id=active_session_source_message_id or source_message_id,
                idempotency_key=active_session_idempotency_key or idempotency_key,
                suppress_send=True,
            )
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
        if _is_timeblock_update_intent(session_intent):
            payload["intent"] = "timeblock.update"
        if _is_task_create_intent(session_intent):
            incoming_before_lock = str(payload.get("intent") or "").strip()
            payload["intent"] = session_intent
            LOG.info(
                "task_create_intent_locked",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "active_intent": session_intent,
                    "incoming_intent": incoming_before_lock,
                    "locked_intent": session_intent,
                    "state": missing_field,
                },
            )
        if (
            confirm_state_probe in {"yes", "no"}
            and missing_field
            not in {
                _TEMPORAL_CONFIRM_FIELD,
                _PAST_DATE_CONFIRM_FIELD,
                _TASK_CREATE_CONFIRM_FIELD,
                _TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD,
                _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD,
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
        if missing_field == _SYNC_CONFLICT_FIELD:
            command = dict(payload)
            conflict = command.get("__sync_conflict") if isinstance(command.get("__sync_conflict"), dict) else {}
            if sync_conflict_action and sync_conflict_id:
                if str(conflict.get("id") or "").strip() and str(conflict.get("id") or "").strip() != sync_conflict_id:
                    continuation_accepted = False
                    continuation_field_name = _SYNC_CONFLICT_FIELD
                elif sync_conflict_action == "edit":
                    edit_command = _sync_conflict_command_for_edit(conflict)
                    command = _attach_temporal_draft(edit_command)
                    continuation_accepted = False
                    continuation_field_name = _TEMPORAL_CONFIRM_FIELD
                else:
                    action_result = _sync_conflict_apply_action(sync_conflict_id, sync_conflict_action)
                    if bool(action_result.get("ok")):
                        if db is not None:
                            db.delete_clarification_session(ctx_key)
                        return {
                            "outcome": "success",
                            "user_message": str(action_result.get("user_message") or "Конфликт обработан."),
                            "command": None,
                        }
                    continuation_accepted = False
                    continuation_field_name = _SYNC_CONFLICT_FIELD
            else:
                continuation_accepted = False
                continuation_field_name = _SYNC_CONFLICT_FIELD
        elif missing_field == _PAST_DATE_CONFIRM_FIELD:
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
            if temporal_edit_callback_target:
                LOG.info(
                    "temporal_edit_callback_consumed",
                    extra={
                        "request_id": request_id,
                        "flow_id": request_id,
                        "target_field": temporal_edit_callback_target,
                    },
                )
                command[_TEMPORAL_EDIT_ACTIVE_FIELD_KEY] = temporal_edit_callback_target
                command.pop("__temporal_confirmed", None)
                continuation_accepted = False
                continuation_field_name = _TEMPORAL_EDIT_FIELD
            else:
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
            confirm_state = _binary_confirmation_state(resolved_text)
            duration_quick_action = _duration_quick_action_from_metadata(metadata, "clarify:v1:temporal_duration:")
            active_edit_field = normalize_clarification_field_name(
                str(command.get(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY) or "")
            )
            if active_edit_field not in {"start_at_date", "start_at_time", "duration_minutes", _MEETING_COMMENT_TEXT_FIELD}:
                active_edit_field = ""
            if temporal_edit_callback_target:
                LOG.info(
                    "temporal_edit_callback_consumed",
                    extra={
                        "request_id": request_id,
                        "flow_id": request_id,
                        "target_field": temporal_edit_callback_target,
                    },
                )
                command[_TEMPORAL_EDIT_ACTIVE_FIELD_KEY] = temporal_edit_callback_target
                continuation_accepted = False
                continuation_field_name = _TEMPORAL_EDIT_FIELD
            elif active_edit_field == _MEETING_COMMENT_TEXT_FIELD and temporal_comment_callback_action == "keep":
                command.pop(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY, None)
                continuation_accepted = True
                continuation_field_name = _MEETING_COMMENT_TEXT_FIELD
            elif active_edit_field == _MEETING_COMMENT_TEXT_FIELD and temporal_comment_callback_action == "clear":
                command["comment_text"] = ""
                command["comment"] = ""
                entities = command.get("entities")
                entities = dict(entities) if isinstance(entities, dict) else {}
                entities["comment_text"] = ""
                entities["comment"] = ""
                command["entities"] = entities
                command.pop(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY, None)
                continuation_accepted = True
                continuation_field_name = _MEETING_COMMENT_TEXT_FIELD
            elif active_edit_field == "duration_minutes" and duration_quick_action in {"15", "30", "45", "60"}:
                command["duration_minutes"] = int(duration_quick_action)
                entities = command.get("entities")
                entities = dict(entities) if isinstance(entities, dict) else {}
                entities["duration_minutes"] = int(duration_quick_action)
                command["entities"] = entities
                command.pop(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY, None)
                continuation_accepted = True
                continuation_field_name = "duration_minutes"
            elif active_edit_field == "duration_minutes" and duration_quick_action == "other":
                continuation_accepted = False
                continuation_field_name = _TEMPORAL_EDIT_FIELD
            elif active_edit_field == "duration_minutes" and duration_quick_action == "cancel":
                command.pop(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY, None)
                continuation_accepted = False
                continuation_field_name = _TEMPORAL_CONFIRM_FIELD
            elif confirm_state == "yes":
                command.pop(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY, None)
                continuation_accepted = True
                continuation_field_name = _TEMPORAL_CONFIRM_FIELD
            elif direct_date_iso:
                command = _inject_start_at_date_into_command(command, direct_date_iso)
                command.pop(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY, None)
                continuation_accepted = True
                continuation_field_name = "start_at_date"
            else:
                continuation_accepted = False
                if active_edit_field == _MEETING_COMMENT_TEXT_FIELD:
                    comment_text = str(resolved_text or "").strip()
                    if comment_text:
                        command["comment_text"] = comment_text
                        command["comment"] = comment_text
                        entities = command.get("entities")
                        entities = dict(entities) if isinstance(entities, dict) else {}
                        entities["comment_text"] = comment_text
                        entities["comment"] = comment_text
                        command["entities"] = entities
                        command.pop(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY, None)
                        continuation_accepted = True
                        continuation_field_name = _MEETING_COMMENT_TEXT_FIELD
                direct_fields = (active_edit_field,) if active_edit_field else ("start_at_time", "duration_minutes")
                for direct_field in direct_fields:
                    if continuation_accepted:
                        break
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
                        continuation_field_name = _TEMPORAL_EDIT_FIELD
                    else:
                        continuation_accepted = False
                        continuation_field_name = _TEMPORAL_EDIT_FIELD
        elif missing_field == _TASK_CREATE_CONFIRM_FIELD:
            command = dict(payload)
            confirm_action = _task_create_confirmation_action(resolved_text)
            if confirm_action == "create":
                command["__task_create_confirmed"] = True
                continuation_accepted = True
                continuation_field_name = _TASK_CREATE_CONFIRM_FIELD
            elif confirm_action == "edit":
                command.pop("__task_create_confirmed", None)
                continuation_accepted = False
                continuation_field_name = _TASK_CREATE_EDIT_FIELD
            elif confirm_action == "comment":
                command.pop("__task_create_confirmed", None)
                continuation_accepted = False
                continuation_field_name = _TASK_CREATE_COMMENT_FIELD
            elif confirm_action == "cancel":
                if db is not None:
                    db.delete_clarification_session(ctx_key)
                return {
                    "outcome": "cancelled",
                    "user_message": "Ок, задачу не создаю.",
                    "command": None,
                }
            else:
                continuation_accepted = False
                continuation_field_name = _TASK_CREATE_CONFIRM_FIELD
        elif missing_field == _TASK_CREATE_COMMENT_FIELD:
            command = dict(payload)
            comment_text = str(resolved_text or "").strip()
            if comment_text:
                command["comment_text"] = comment_text
                command["comment"] = comment_text
                entities = command.get("entities")
                entities = dict(entities) if isinstance(entities, dict) else {}
                entities["comment_text"] = comment_text
                entities["comment"] = comment_text
                command["entities"] = entities
                LOG.info(
                    "task_create_comment_updated",
                    extra={
                        "request_id": request_id,
                        "flow_id": request_id,
                        "user_id": str(user_id or ""),
                        "comment_length": len(comment_text),
                    },
                )
                continuation_accepted = False
                continuation_field_name = _TASK_CREATE_CONFIRM_FIELD
            else:
                continuation_accepted = False
                continuation_field_name = _TASK_CREATE_COMMENT_FIELD
        elif missing_field == _TASK_CREATE_DUPLICATE_CONFIRM_FIELD:
            command = dict(payload)
            confirm_action = _task_create_duplicate_confirmation_action(resolved_text)
            if confirm_action == "yes":
                command["__task_create_confirmed"] = True
                command["duplicate_check_override"] = True
                entities = command.get("entities")
                entities = dict(entities) if isinstance(entities, dict) else {}
                entities["duplicate_check_override"] = True
                command["entities"] = entities
                continuation_accepted = True
                continuation_field_name = _TASK_CREATE_DUPLICATE_CONFIRM_FIELD
            elif confirm_action == "no":
                if db is not None:
                    db.delete_clarification_session(ctx_key)
                return {
                    "outcome": "cancelled",
                    "user_message": "Ок, не создаю дубликат задачи.",
                    "command": None,
                }
            else:
                continuation_accepted = False
                continuation_field_name = _TASK_CREATE_DUPLICATE_CONFIRM_FIELD
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
        elif missing_field == _TASK_CREATE_DATE_FIELD:
            command = dict(payload)
            resolved_due_date = _normalize_clarification_date_to_iso(
                resolved_text,
                prefer_future_for_ambiguous=False,
            )
            if resolved_due_date:
                command["due_date"] = resolved_due_date
                command["planned_at"] = resolved_due_date
                entities = command.get("entities")
                entities = dict(entities) if isinstance(entities, dict) else {}
                entities["due_date"] = resolved_due_date
                entities["planned_at"] = resolved_due_date
                command["entities"] = entities
                command["__task_create_raw_due_date"] = str(resolved_text or "").strip()
                continuation_accepted = True
                continuation_field_name = _TASK_CREATE_DATE_FIELD
            else:
                continuation_accepted = False
                continuation_field_name = _TASK_CREATE_DATE_FIELD
        elif missing_field == _AMBIGUOUS_CREATE_FIELD:
            command = dict(payload)
            selected_route = _ambiguous_create_choice_from_text(resolved_text)
            if selected_route == "task.create":
                command["intent"] = "task.create"
                command["__ambiguous_create_selected_intent"] = selected_route
                continuation_accepted = False
                continuation_field_name = _TASK_CREATE_EDIT_FIELD
            elif selected_route == "meeting.create":
                command["intent"] = "meeting.create"
                command["__ambiguous_create_selected_intent"] = selected_route
                continuation_accepted = False
                continuation_field_name = "meeting_create_details"
            elif selected_route == "timeblock.create":
                command["intent"] = "timeblock.create"
                command["__ambiguous_create_selected_intent"] = selected_route
                continuation_accepted = False
                continuation_field_name = "timeblock_create_details"
            else:
                continuation_accepted = False
                continuation_field_name = _AMBIGUOUS_CREATE_FIELD
        elif missing_field == "meeting_create_details":
            command = dict(payload)
            detail_text = str(resolved_text or "").strip()
            if detail_text:
                command["intent"] = "meeting.create"
                command["__source_text"] = detail_text
                entities = command.get("entities")
                entities = dict(entities) if isinstance(entities, dict) else {}
                entities["text"] = detail_text
                command["entities"] = entities
                continuation_accepted = True
                continuation_field_name = "meeting_create_details"
            else:
                continuation_accepted = False
                continuation_field_name = "meeting_create_details"
        elif missing_field == "timeblock_create_details":
            command = dict(payload)
            detail_text = str(resolved_text or "").strip()
            if detail_text:
                command["intent"] = "timeblock.create"
                command["__source_text"] = detail_text
                entities = command.get("entities")
                entities = dict(entities) if isinstance(entities, dict) else {}
                entities["text"] = detail_text
                command["entities"] = entities
                command = _apply_timeblock_create_semantic_routing(command, detail_text)
                continuation_accepted = True
                continuation_field_name = "timeblock_create_details"
            else:
                continuation_accepted = False
                continuation_field_name = "timeblock_create_details"
        elif missing_field == _TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD:
            command = dict(payload)
            confirm_state = _binary_confirmation_state(resolved_text)
            if confirm_state == "yes":
                command["__timeblock_update_target_confirmed"] = True
                _temporal_edit_log(
                    "temporal_edit_target_confirmed",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="timeblock.update",
                    command=command,
                    clarification_state=missing_field,
                )
                continuation_accepted = False
                continuation_field_name = _TIMEBLOCK_UPDATE_EDIT_CHOICE_FIELD
            elif confirm_state == "no":
                command.pop("__timeblock_update_target_confirmed", None)
                continuation_accepted = False
                continuation_field_name = "timeblock_update_target_ref"
            else:
                continuation_accepted = False
                continuation_field_name = _TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD
        elif missing_field == _TIMEBLOCK_UPDATE_EDIT_CHOICE_FIELD:
            command = dict(payload)
            selection = _timeblock_update_field_choice_from_metadata(metadata) or _meeting_update_field_choice_from_text(resolved_text)
            if selection == "cancel":
                _temporal_edit_log(
                    "temporal_edit_cancelled",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="timeblock.update",
                    command=command,
                    clarification_state=missing_field,
                )
                continuation_accepted = False
                continuation_field_name = _TIMEBLOCK_UPDATE_EDIT_CHOICE_FIELD
            elif selection in {"date", "time", "duration", "comment", "date_time"}:
                command = _timeblock_update_apply_field_choice(command, selection)
                _temporal_edit_log(
                    "temporal_edit_field_selected",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="timeblock.update",
                    command=command,
                    field=selection,
                    clarification_state=missing_field,
                    from_callback=bool(isinstance(metadata, dict) and isinstance(metadata.get("callback_query"), dict)),
                )
                continuation_accepted = False
                continuation_field_name = {
                    "date": _TIMEBLOCK_UPDATE_EDIT_DATE_FIELD,
                    "time": _TIMEBLOCK_UPDATE_EDIT_TIME_FIELD,
                    "duration": _TIMEBLOCK_UPDATE_EDIT_DURATION_FIELD,
                    "comment": _TIMEBLOCK_UPDATE_EDIT_COMMENT_FIELD,
                    "date_time": _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
                }[selection]
            else:
                continuation_accepted = False
                continuation_field_name = _TIMEBLOCK_UPDATE_EDIT_CHOICE_FIELD
        elif missing_field in {
            _TIMEBLOCK_UPDATE_EDIT_DATE_FIELD,
            _TIMEBLOCK_UPDATE_EDIT_TIME_FIELD,
            _TIMEBLOCK_UPDATE_EDIT_DURATION_FIELD,
            _TIMEBLOCK_UPDATE_EDIT_COMMENT_FIELD,
            _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
            _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_TIME_FIELD,
        }:
            command = dict(payload)
            callback_selection = _timeblock_update_field_choice_from_metadata(metadata)
            if callback_selection and callback_selection != "cancel":
                _temporal_edit_log(
                    "temporal_edit_duplicate_callback_ignored",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="timeblock.update",
                    command=command,
                    field=callback_selection,
                    clarification_state=missing_field,
                    from_callback=True,
                )
                continuation_accepted = False
                continuation_field_name = missing_field
            elif missing_field in {_TIMEBLOCK_UPDATE_EDIT_DATE_FIELD, _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_DATE_FIELD}:
                parsed_date = _normalize_clarification_date_to_iso(resolved_text, prefer_future_for_ambiguous=False)
                if parsed_date:
                    command = _inject_start_at_date_into_command(command, parsed_date)
                    entities = command.get("entities")
                    entities = dict(entities) if isinstance(entities, dict) else {}
                    entities["timeblock_update_new_date"] = parsed_date
                    command["timeblock_update_new_date"] = parsed_date
                    command["entities"] = entities
                    _temporal_edit_log(
                        "temporal_edit_value_received",
                        request_id=request_id,
                        user_id=str(user_id or ""),
                        intent="timeblock.update",
                        command=command,
                        field="date",
                        normalized_value=parsed_date,
                        clarification_state=missing_field,
                        from_callback=bool(isinstance(metadata, dict) and isinstance(metadata.get("callback_query"), dict)),
                    )
                    if missing_field == _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_DATE_FIELD:
                        command[_TIMEBLOCK_UPDATE_PENDING_COMBO_KEY] = False
                        continuation_accepted = False
                        continuation_field_name = _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_TIME_FIELD
                    else:
                        continuation_accepted = False
                        continuation_field_name = _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD
                else:
                    continuation_accepted = False
                    continuation_field_name = missing_field
            elif missing_field in {_TIMEBLOCK_UPDATE_EDIT_TIME_FIELD, _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_TIME_FIELD}:
                parsed_time = _normalize_time_hhmm_local(resolved_text)
                if parsed_time:
                    command = _inject_start_at_time_into_command(command, parsed_time)
                    entities = command.get("entities")
                    entities = dict(entities) if isinstance(entities, dict) else {}
                    entities["timeblock_update_new_time"] = parsed_time
                    command["timeblock_update_new_time"] = parsed_time
                    command["entities"] = entities
                    _temporal_edit_log(
                        "temporal_edit_value_received",
                        request_id=request_id,
                        user_id=str(user_id or ""),
                        intent="timeblock.update",
                        command=command,
                        field="time",
                        normalized_value=parsed_time,
                        clarification_state=missing_field,
                        from_callback=bool(isinstance(metadata, dict) and isinstance(metadata.get("callback_query"), dict)),
                    )
                    continuation_accepted = False
                    continuation_field_name = _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD
                else:
                    continuation_accepted = False
                    continuation_field_name = missing_field
            elif missing_field == _TIMEBLOCK_UPDATE_EDIT_DURATION_FIELD:
                command = _timeblock_update_apply_field_choice(command, "duration")
                duration_quick_action = _duration_quick_action_from_metadata(metadata, "clarify:v1:timeblock_update_duration:")
                if duration_quick_action in {"15", "30", "45", "60"}:
                    parsed_duration = int(duration_quick_action)
                    command["timeblock_update_new_duration"] = parsed_duration
                    command["duration_minutes"] = parsed_duration
                    entities = command.get("entities")
                    entities = dict(entities) if isinstance(entities, dict) else {}
                    entities["timeblock_update_new_duration"] = parsed_duration
                    entities["duration_minutes"] = parsed_duration
                    command["entities"] = entities
                    _temporal_edit_log(
                        "temporal_edit_value_received",
                        request_id=request_id,
                        user_id=str(user_id or ""),
                        intent="timeblock.update",
                        command=command,
                        field="duration",
                        normalized_value=parsed_duration,
                        clarification_state=missing_field,
                        from_callback=True,
                    )
                    continuation_accepted = False
                    continuation_field_name = _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD
                elif duration_quick_action == "other":
                    continuation_accepted = False
                    continuation_field_name = _TIMEBLOCK_UPDATE_EDIT_DURATION_FIELD
                elif duration_quick_action == "cancel":
                    continuation_accepted = False
                    continuation_field_name = _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD
                else:
                    parsed_duration = _parse_duration_minutes_reply(resolved_text)
                    if parsed_duration is not None:
                        command["timeblock_update_new_duration"] = int(parsed_duration)
                        command["duration_minutes"] = int(parsed_duration)
                        entities = command.get("entities")
                        entities = dict(entities) if isinstance(entities, dict) else {}
                        entities["timeblock_update_new_duration"] = int(parsed_duration)
                        entities["duration_minutes"] = int(parsed_duration)
                        command["entities"] = entities
                        _temporal_edit_log(
                            "temporal_edit_value_received",
                            request_id=request_id,
                            user_id=str(user_id or ""),
                            intent="timeblock.update",
                            command=command,
                            field="duration",
                            normalized_value=int(parsed_duration),
                            clarification_state=missing_field,
                            from_callback=bool(isinstance(metadata, dict) and isinstance(metadata.get("callback_query"), dict)),
                        )
                        continuation_accepted = False
                        continuation_field_name = _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD
                    else:
                        continuation_accepted = False
                        continuation_field_name = _TIMEBLOCK_UPDATE_EDIT_DURATION_FIELD
            elif missing_field == _TIMEBLOCK_UPDATE_EDIT_COMMENT_FIELD:
                if temporal_comment_callback_action == "keep":
                    _temporal_edit_log(
                        "temporal_edit_value_received",
                        request_id=request_id,
                        user_id=str(user_id or ""),
                        intent="timeblock.update",
                        command=command,
                        field="comment",
                        normalized_value=_timeblock_update_current_comment(command),
                        clarification_state=missing_field,
                        from_callback=True,
                    )
                    continuation_accepted = False
                    continuation_field_name = _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD
                elif temporal_comment_callback_action == "clear":
                    command["comment_text"] = ""
                    command["comment"] = ""
                    entities = command.get("entities")
                    entities = dict(entities) if isinstance(entities, dict) else {}
                    entities["comment_text"] = ""
                    entities["comment"] = ""
                    command["entities"] = entities
                    _temporal_edit_log(
                        "temporal_edit_value_received",
                        request_id=request_id,
                        user_id=str(user_id or ""),
                        intent="timeblock.update",
                        command=command,
                        field="comment",
                        normalized_value="",
                        clarification_state=missing_field,
                        from_callback=True,
                    )
                    continuation_accepted = False
                    continuation_field_name = _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD
                else:
                    comment_text = str(resolved_text or "").strip()
                    if comment_text:
                        command["comment_text"] = comment_text
                        command["comment"] = comment_text
                        entities = command.get("entities")
                        entities = dict(entities) if isinstance(entities, dict) else {}
                        entities["comment_text"] = comment_text
                        entities["comment"] = comment_text
                        command["entities"] = entities
                        _temporal_edit_log(
                            "temporal_edit_value_received",
                            request_id=request_id,
                            user_id=str(user_id or ""),
                            intent="timeblock.update",
                            command=command,
                            field="comment",
                            normalized_value=comment_text,
                            clarification_state=missing_field,
                            from_callback=False,
                        )
                        continuation_accepted = False
                        continuation_field_name = _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD
                    else:
                        continuation_accepted = False
                        continuation_field_name = _TIMEBLOCK_UPDATE_EDIT_COMMENT_FIELD
        elif missing_field == _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD:
            command = dict(payload)
            confirm_state = _binary_confirmation_state(resolved_text)
            if confirm_state == "yes":
                command["__timeblock_update_final_confirmed"] = True
                _temporal_edit_log(
                    "temporal_edit_confirmed",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="timeblock.update",
                    command=command,
                    clarification_state=missing_field,
                )
                continuation_accepted = True
                continuation_field_name = _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD
            elif confirm_state == "no":
                command.pop("__timeblock_update_final_confirmed", None)
                _temporal_edit_log(
                    "temporal_edit_cancelled",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="timeblock.update",
                    command=command,
                    clarification_state=missing_field,
                )
                continuation_accepted = False
                continuation_field_name = _TIMEBLOCK_UPDATE_EDIT_CHOICE_FIELD
            else:
                selection = _timeblock_update_field_choice_from_metadata(metadata) or _meeting_update_field_choice_from_text(resolved_text)
                if selection in {"date", "time", "duration", "comment", "date_time"}:
                    command = _timeblock_update_apply_field_choice(command, selection)
                    _temporal_edit_log(
                        "temporal_edit_field_selected",
                        request_id=request_id,
                        user_id=str(user_id or ""),
                        intent="timeblock.update",
                        command=command,
                        field=selection,
                        clarification_state=missing_field,
                        from_callback=bool(isinstance(metadata, dict) and isinstance(metadata.get("callback_query"), dict)),
                    )
                    continuation_accepted = False
                    continuation_field_name = {
                        "date": _TIMEBLOCK_UPDATE_EDIT_DATE_FIELD,
                        "time": _TIMEBLOCK_UPDATE_EDIT_TIME_FIELD,
                        "duration": _TIMEBLOCK_UPDATE_EDIT_DURATION_FIELD,
                        "comment": _TIMEBLOCK_UPDATE_EDIT_COMMENT_FIELD,
                        "date_time": _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
                    }[selection]
                else:
                    continuation_accepted = False
                    continuation_field_name = _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD
        elif missing_field in {"timeblock_update_target_ref", _TIMEBLOCK_UPDATE_TARGET_REFINE_FIELD}:
            command = dict(payload)
            search_query = _normalize_timeblock_target_hint(resolved_text) or str(resolved_text or "").strip()
            command["__timeblock_update_target_hint"] = search_query
            search_result = _timeblock_update_repeat_search(str(user_id or "").strip(), search_query)
            candidates = search_result.get("candidates") if isinstance(search_result.get("candidates"), list) else []
            candidate = search_result.get("candidate") if isinstance(search_result.get("candidate"), dict) else None
            continuation_accepted = False
            if bool(search_result.get("ok")) and isinstance(candidate, dict):
                command = _timeblock_update_hydrate_from_candidate(command, candidate)
                _temporal_edit_log(
                    "temporal_edit_target_selected",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="timeblock.update",
                    command=command,
                    clarification_state=missing_field,
                )
                continuation_field_name = _TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD
            elif str(search_result.get("reason") or "").strip().lower() == "multiple" and candidates:
                shortlist = [dict(c) for c in candidates if isinstance(c, dict)][:_MEETING_UPDATE_SHORTLIST_MAX]
                command = _timeblock_update_store_candidate_shortlist(command, shortlist)
                continuation_field_name = _TIMEBLOCK_UPDATE_TARGET_SELECT_FIELD
            else:
                continuation_field_name = _TIMEBLOCK_UPDATE_TARGET_REFINE_FIELD
        elif missing_field == _TIMEBLOCK_UPDATE_TARGET_SELECT_FIELD:
            command = dict(payload)
            shortlist = _timeblock_update_candidate_shortlist_from_command(command)
            selected, filtered, reason = _meeting_update_select_candidate_from_shortlist(shortlist, str(resolved_text or "").strip())
            continuation_accepted = False
            if isinstance(selected, dict):
                command = _timeblock_update_hydrate_from_candidate(command, selected)
                _temporal_edit_log(
                    "temporal_edit_target_selected",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="timeblock.update",
                    command=command,
                    clarification_state=missing_field,
                )
                continuation_field_name = _TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD
            elif filtered and len(filtered) > 1:
                command = _timeblock_update_store_candidate_shortlist(command, filtered[:_MEETING_UPDATE_SHORTLIST_MAX])
                continuation_field_name = _TIMEBLOCK_UPDATE_TARGET_SELECT_FIELD
            else:
                continuation_field_name = _TIMEBLOCK_UPDATE_TARGET_REFINE_FIELD
        elif missing_field == _MEETING_UPDATE_TARGET_CONFIRM_FIELD:
            command = dict(payload)
            confirm_state = _binary_confirmation_state(resolved_text)
            if confirm_state == "yes":
                command["__meeting_update_target_confirmed"] = True
                _temporal_edit_log(
                    "temporal_edit_target_confirmed",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="meeting.update",
                    command=command,
                    clarification_state=missing_field,
                )
                continuation_accepted = False
                continuation_field_name = _MEETING_UPDATE_EDIT_CHOICE_FIELD
            elif confirm_state == "no":
                command.pop("__meeting_update_target_confirmed", None)
                continuation_accepted = False
                continuation_field_name = "meeting_update_target_ref"
            else:
                continuation_accepted = False
                continuation_field_name = _MEETING_UPDATE_TARGET_CONFIRM_FIELD
        elif missing_field == _MEETING_UPDATE_EDIT_CHOICE_FIELD:
            command = dict(payload)
            selection = _meeting_update_field_choice_from_metadata(metadata) or _meeting_update_field_choice_from_text(resolved_text)
            if selection == "cancel":
                _temporal_edit_log(
                    "temporal_edit_cancelled",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="meeting.update",
                    command=command,
                    clarification_state=missing_field,
                )
                continuation_accepted = False
                continuation_field_name = _MEETING_UPDATE_EDIT_CHOICE_FIELD
            elif selection in {"date", "time", "duration", "comment", "date_time"}:
                command = _meeting_update_apply_field_choice(command, selection)
                _temporal_edit_log(
                    "temporal_edit_field_selected",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="meeting.update",
                    command=command,
                    field=selection,
                    clarification_state=missing_field,
                    from_callback=bool(isinstance(metadata, dict) and isinstance(metadata.get("callback_query"), dict)),
                )
                continuation_accepted = False
                continuation_field_name = {
                    "date": _MEETING_UPDATE_EDIT_DATE_FIELD,
                    "time": _MEETING_UPDATE_EDIT_TIME_FIELD,
                    "duration": _MEETING_UPDATE_EDIT_DURATION_FIELD,
                    "comment": _MEETING_UPDATE_EDIT_COMMENT_FIELD,
                    "date_time": _MEETING_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
                }[selection]
            else:
                continuation_accepted = False
                continuation_field_name = _MEETING_UPDATE_EDIT_CHOICE_FIELD
        elif missing_field in {
            _MEETING_UPDATE_EDIT_DATE_FIELD,
            _MEETING_UPDATE_EDIT_TIME_FIELD,
            _MEETING_UPDATE_EDIT_DURATION_FIELD,
            _MEETING_UPDATE_EDIT_COMMENT_FIELD,
            _MEETING_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
            _MEETING_UPDATE_EDIT_DATE_TIME_TIME_FIELD,
        }:
            command = dict(payload)
            active_choice = _meeting_update_active_edit_choice(command)
            if active_choice not in {"date", "time", "duration", "comment", "date_time"}:
                active_choice = ""
            callback_selection = _meeting_update_field_choice_from_metadata(metadata)
            if callback_selection and callback_selection != "cancel":
                _temporal_edit_log(
                    "temporal_edit_duplicate_callback_ignored",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="meeting.update",
                    command=command,
                    field=callback_selection,
                    clarification_state=missing_field,
                    from_callback=True,
                )
                continuation_accepted = False
                continuation_field_name = missing_field
            elif missing_field == _MEETING_UPDATE_EDIT_DATE_FIELD or missing_field == _MEETING_UPDATE_EDIT_DATE_TIME_DATE_FIELD:
                parsed_date = _normalize_clarification_date_to_iso(resolved_text, prefer_future_for_ambiguous=False)
                if parsed_date:
                    command = _inject_start_at_date_into_command(command, parsed_date)
                    entities = command.get("entities")
                    entities = dict(entities) if isinstance(entities, dict) else {}
                    entities["meeting_update_new_date"] = parsed_date
                    command["meeting_update_new_date"] = parsed_date
                    command["entities"] = entities
                    _temporal_edit_log(
                        "temporal_edit_value_received",
                        request_id=request_id,
                        user_id=str(user_id or ""),
                        intent="meeting.update",
                        command=command,
                        field="date",
                        normalized_value=parsed_date,
                        clarification_state=missing_field,
                        from_callback=bool(isinstance(metadata, dict) and isinstance(metadata.get("callback_query"), dict)),
                    )
                    if missing_field == _MEETING_UPDATE_EDIT_DATE_TIME_DATE_FIELD:
                        command[_MEETING_UPDATE_PENDING_COMBO_KEY] = False
                        continuation_accepted = False
                        continuation_field_name = _MEETING_UPDATE_EDIT_DATE_TIME_TIME_FIELD
                    else:
                        continuation_accepted = False
                        continuation_field_name = _MEETING_UPDATE_FINAL_CONFIRM_FIELD
                else:
                    continuation_accepted = False
                    continuation_field_name = missing_field
            elif missing_field == _MEETING_UPDATE_EDIT_TIME_FIELD or missing_field == _MEETING_UPDATE_EDIT_DATE_TIME_TIME_FIELD:
                parsed_time = _normalize_time_hhmm_local(resolved_text)
                if parsed_time:
                    command = _inject_start_at_time_into_command(command, parsed_time)
                    entities = command.get("entities")
                    entities = dict(entities) if isinstance(entities, dict) else {}
                    entities["meeting_update_new_time"] = parsed_time
                    command["meeting_update_new_time"] = parsed_time
                    command["entities"] = entities
                    _temporal_edit_log(
                        "temporal_edit_value_received",
                        request_id=request_id,
                        user_id=str(user_id or ""),
                        intent="meeting.update",
                        command=command,
                        field="time",
                        normalized_value=parsed_time,
                        clarification_state=missing_field,
                        from_callback=bool(isinstance(metadata, dict) and isinstance(metadata.get("callback_query"), dict)),
                    )
                    continuation_accepted = False
                    continuation_field_name = _MEETING_UPDATE_FINAL_CONFIRM_FIELD
                else:
                    continuation_accepted = False
                    continuation_field_name = missing_field
            elif missing_field == _MEETING_UPDATE_EDIT_DURATION_FIELD:
                command = _meeting_update_apply_field_choice(command, "duration")
                duration_quick_action = _duration_quick_action_from_metadata(metadata, "clarify:v1:meeting_update_duration:")
                if duration_quick_action in {"15", "30", "45", "60"}:
                    parsed_duration = int(duration_quick_action)
                    command["meeting_update_new_duration"] = parsed_duration
                    command["duration_minutes"] = parsed_duration
                    entities = command.get("entities")
                    entities = dict(entities) if isinstance(entities, dict) else {}
                    entities["meeting_update_new_duration"] = parsed_duration
                    entities["duration_minutes"] = parsed_duration
                    command["entities"] = entities
                    _temporal_edit_log(
                        "temporal_edit_value_received",
                        request_id=request_id,
                        user_id=str(user_id or ""),
                        intent="meeting.update",
                        command=command,
                        field="duration",
                        normalized_value=parsed_duration,
                        clarification_state=missing_field,
                        from_callback=True,
                    )
                    continuation_accepted = False
                    continuation_field_name = _MEETING_UPDATE_FINAL_CONFIRM_FIELD
                elif duration_quick_action == "other":
                    continuation_accepted = False
                    continuation_field_name = _MEETING_UPDATE_EDIT_DURATION_FIELD
                elif duration_quick_action == "cancel":
                    continuation_accepted = False
                    continuation_field_name = _MEETING_UPDATE_FINAL_CONFIRM_FIELD
                else:
                    parsed_duration = _parse_duration_minutes_reply(resolved_text)
                    if parsed_duration is not None:
                        command["meeting_update_new_duration"] = int(parsed_duration)
                        command["duration_minutes"] = int(parsed_duration)
                        entities = command.get("entities")
                        entities = dict(entities) if isinstance(entities, dict) else {}
                        entities["meeting_update_new_duration"] = int(parsed_duration)
                        entities["duration_minutes"] = int(parsed_duration)
                        command["entities"] = entities
                        _temporal_edit_log(
                            "temporal_edit_value_received",
                            request_id=request_id,
                            user_id=str(user_id or ""),
                            intent="meeting.update",
                            command=command,
                            field="duration",
                            normalized_value=int(parsed_duration),
                            clarification_state=missing_field,
                            from_callback=bool(isinstance(metadata, dict) and isinstance(metadata.get("callback_query"), dict)),
                        )
                        continuation_accepted = False
                        continuation_field_name = _MEETING_UPDATE_FINAL_CONFIRM_FIELD
                    else:
                        continuation_accepted = False
                        continuation_field_name = _MEETING_UPDATE_EDIT_DURATION_FIELD
            elif missing_field == _MEETING_UPDATE_EDIT_COMMENT_FIELD:
                if temporal_comment_callback_action == "keep":
                    _temporal_edit_log(
                        "temporal_edit_value_received",
                        request_id=request_id,
                        user_id=str(user_id or ""),
                        intent="meeting.update",
                        command=command,
                        field="comment",
                        normalized_value=_meeting_update_current_comment(command),
                        clarification_state=missing_field,
                        from_callback=True,
                    )
                    continuation_accepted = False
                    continuation_field_name = _MEETING_UPDATE_FINAL_CONFIRM_FIELD
                elif temporal_comment_callback_action == "clear":
                    command["comment_text"] = ""
                    command["comment"] = ""
                    entities = command.get("entities")
                    entities = dict(entities) if isinstance(entities, dict) else {}
                    entities["comment_text"] = ""
                    entities["comment"] = ""
                    command["entities"] = entities
                    _temporal_edit_log(
                        "temporal_edit_value_received",
                        request_id=request_id,
                        user_id=str(user_id or ""),
                        intent="meeting.update",
                        command=command,
                        field="comment",
                        normalized_value="",
                        clarification_state=missing_field,
                        from_callback=True,
                    )
                    continuation_accepted = False
                    continuation_field_name = _MEETING_UPDATE_FINAL_CONFIRM_FIELD
                else:
                    comment_text = str(resolved_text or "").strip()
                    if comment_text:
                        command["comment_text"] = comment_text
                        command["comment"] = comment_text
                        entities = command.get("entities")
                        entities = dict(entities) if isinstance(entities, dict) else {}
                        entities["comment_text"] = comment_text
                        entities["comment"] = comment_text
                        command["entities"] = entities
                        _temporal_edit_log(
                            "temporal_edit_value_received",
                            request_id=request_id,
                            user_id=str(user_id or ""),
                            intent="meeting.update",
                            command=command,
                            field="comment",
                            normalized_value=comment_text,
                            clarification_state=missing_field,
                            from_callback=False,
                        )
                        continuation_accepted = False
                        continuation_field_name = _MEETING_UPDATE_FINAL_CONFIRM_FIELD
                    else:
                        continuation_accepted = False
                        continuation_field_name = _MEETING_UPDATE_EDIT_COMMENT_FIELD
            else:
                continuation_accepted = False
                continuation_field_name = _MEETING_UPDATE_EDIT_CHOICE_FIELD
        elif missing_field == _MEETING_UPDATE_FINAL_CONFIRM_FIELD:
            command = dict(payload)
            confirm_state = _binary_confirmation_state(resolved_text)
            if confirm_state == "yes":
                command["__meeting_update_final_confirmed"] = True
                command["__temporal_confirmed"] = True
                _temporal_edit_log(
                    "temporal_edit_confirmed",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="meeting.update",
                    command=command,
                    clarification_state=missing_field,
                )
                continuation_accepted = True
                continuation_field_name = _MEETING_UPDATE_FINAL_CONFIRM_FIELD
            elif confirm_state == "no":
                command.pop("__meeting_update_final_confirmed", None)
                command.pop("__temporal_confirmed", None)
                _temporal_edit_log(
                    "temporal_edit_cancelled",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="meeting.update",
                    command=command,
                    clarification_state=missing_field,
                )
                continuation_accepted = False
                continuation_field_name = _MEETING_UPDATE_EDIT_CHOICE_FIELD
            else:
                selection = _meeting_update_field_choice_from_metadata(metadata) or _meeting_update_field_choice_from_text(resolved_text)
                if selection in {"date", "time", "duration", "comment", "date_time"}:
                    command = _meeting_update_apply_field_choice(command, selection)
                    _temporal_edit_log(
                        "temporal_edit_field_selected",
                        request_id=request_id,
                        user_id=str(user_id or ""),
                        intent="meeting.update",
                        command=command,
                        field=selection,
                        clarification_state=missing_field,
                        from_callback=bool(isinstance(metadata, dict) and isinstance(metadata.get("callback_query"), dict)),
                    )
                    continuation_accepted = False
                    continuation_field_name = {
                        "date": _MEETING_UPDATE_EDIT_DATE_FIELD,
                        "time": _MEETING_UPDATE_EDIT_TIME_FIELD,
                        "duration": _MEETING_UPDATE_EDIT_DURATION_FIELD,
                        "comment": _MEETING_UPDATE_EDIT_COMMENT_FIELD,
                        "date_time": _MEETING_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
                    }[selection]
                else:
                    continuation_accepted = False
                    continuation_field_name = _MEETING_UPDATE_FINAL_CONFIRM_FIELD
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
                _temporal_edit_log(
                    "temporal_edit_target_selected",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="meeting.update",
                    command=command,
                    clarification_state=missing_field,
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
            elif str(search_result.get("reason") or "").strip().lower() == "sync_conflict" and isinstance(search_result.get("conflict"), dict):
                command["__sync_conflict"] = dict(search_result.get("conflict"))
                continuation_field_name = _SYNC_CONFLICT_FIELD
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
                _temporal_edit_log(
                    "temporal_edit_target_selected",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="meeting.update",
                    command=command,
                    clarification_state=missing_field,
                )
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
            duration_quick_action = ""
            if missing_field == "duration_minutes":
                duration_quick_action = _duration_quick_action_from_metadata(metadata, "clarify:v1:duration:")
            if missing_field == "start_at_date":
                parsed_date_from_reply = _normalize_clarification_date_to_iso(
                    resolved_text,
                    prefer_future_for_ambiguous=False,
                )
                if parsed_date_from_reply:
                    manual_start_at_date_iso = parsed_date_from_reply
                if not manual_start_at_date_iso:
                    short_day = str(resolved_text or "").strip()
                    if re.fullmatch(r"\d{1,2}", short_day):
                        # Short numeric clarification day should always resolve
                        # to current month/current year first.
                        base_for_short_day = date.today()
                        parsed_short_day = _try_build_date(
                            int(base_for_short_day.year),
                            int(base_for_short_day.month),
                            int(short_day),
                        )
                        if parsed_short_day is not None:
                            manual_start_at_date_iso = parsed_short_day.isoformat()
                existing_date_iso = str(_command_value(payload, "start_at_date", "start_date", "date") or "").strip()
                source_for_existing_date = str(
                    payload.get("__source_text")
                    or _command_value(payload, "text")
                    or ""
                ).strip()
                normalized_date_reuse = _normalize_global_control_text(resolved_text)
                if (
                    _is_meeting_update_intent(session_intent)
                    and normalized_date_reuse in {"эта же дата", "та же дата", "ту же дату", "эту же дату"}
                ):
                    manual_start_at_date_iso = str(
                        _meeting_update_source_value(payload, "start_at_date", "meeting_update_source_date", "date") or ""
                    ).strip()
                if (
                    not manual_start_at_date_iso
                    and _is_positive_confirmation(resolved_text)
                    and existing_date_iso
                    and (
                        has_explicit_user_date(source_for_existing_date)
                        or _extract_explicit_date_from_text(source_for_existing_date)
                    )
                ):
                    manual_start_at_date_iso = existing_date_iso
            if missing_field == "start_at_time":
                manual_start_at_time_hhmm = _normalize_time_hhmm_local(resolved_text)
            if duration_quick_action in {"15", "30", "45", "60"}:
                command = dict(payload)
                command["duration_minutes"] = int(duration_quick_action)
                entities = command.get("entities")
                entities = dict(entities) if isinstance(entities, dict) else {}
                entities["duration_minutes"] = int(duration_quick_action)
                command["entities"] = entities
                continuation_accepted = True
                continuation_field_name = "duration_minutes"
            elif duration_quick_action == "other":
                command = dict(payload)
                continuation_accepted = False
                continuation_field_name = "duration_minutes"
            elif duration_quick_action == "cancel":
                command = dict(payload)
                continuation_accepted = False
                continuation_field_name = "duration_minutes"
            elif manual_start_at_date_iso:
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
        edited_temporal_fields = {"start_at_date", "start_at_time", "duration_minutes", _MEETING_COMMENT_TEXT_FIELD}
        edited_field_applied = continuation_accepted and str(continuation_field_name or "") in edited_temporal_fields
        temporal_intent_hint = str(command.get("intent") or active_session.get("intent") or "")
        if edited_field_applied and _is_temporal_intent_like(temporal_intent_hint):
            command["__edited_temporal_draft"] = True
        if continuation_accepted and str(continuation_field_name or "") in edited_temporal_fields:
            command.pop(_TEMPORAL_EDIT_ACTIVE_FIELD_KEY, None)
        if edited_field_applied:
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
        elif _is_timeblock_update_intent(session_intent):
            command["intent"] = "timeblock.update"
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
            elif _is_meeting_update_intent(intent) and persisted_missing_field == _MEETING_UPDATE_EDIT_CHOICE_FIELD:
                q = _meeting_update_field_choice_question()
                LOG.info("meeting_edit_field_choice_requested", extra={"request_id": request_id, "flow_id": request_id})
            elif _is_meeting_update_intent(intent) and persisted_missing_field in {
                _MEETING_UPDATE_EDIT_DATE_FIELD,
                _MEETING_UPDATE_EDIT_TIME_FIELD,
                _MEETING_UPDATE_EDIT_DURATION_FIELD,
                _MEETING_UPDATE_EDIT_COMMENT_FIELD,
                _MEETING_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
                _MEETING_UPDATE_EDIT_DATE_TIME_TIME_FIELD,
            }:
                q = _meeting_update_edit_question(command)
                if persisted_missing_field == _MEETING_UPDATE_EDIT_COMMENT_FIELD:
                    LOG.info("meeting_edit_comment_waiting", extra={"request_id": request_id, "flow_id": request_id})
            elif _is_meeting_update_intent(intent) and persisted_missing_field == _MEETING_UPDATE_FINAL_CONFIRM_FIELD:
                q = _meeting_update_final_confirmation_question(command)
                _temporal_edit_log(
                    "temporal_edit_summary_rendered",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="meeting.update",
                    command=command,
                    clarification_state=persisted_missing_field,
                    **_meeting_update_summary_payload(command),
                )
            elif _is_timeblock_update_intent(intent) and persisted_missing_field == _TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD:
                q = _timeblock_update_target_confirmation_question(command)
            elif _is_timeblock_update_intent(intent) and persisted_missing_field == _TIMEBLOCK_UPDATE_TARGET_SELECT_FIELD:
                q = _timeblock_update_render_candidate_shortlist(_timeblock_update_candidate_shortlist_from_command(command))
            elif _is_timeblock_update_intent(intent) and persisted_missing_field == _TIMEBLOCK_UPDATE_EDIT_CHOICE_FIELD:
                q = _timeblock_update_field_choice_question()
            elif _is_timeblock_update_intent(intent) and persisted_missing_field in {
                _TIMEBLOCK_UPDATE_EDIT_DATE_FIELD,
                _TIMEBLOCK_UPDATE_EDIT_TIME_FIELD,
                _TIMEBLOCK_UPDATE_EDIT_DURATION_FIELD,
                _TIMEBLOCK_UPDATE_EDIT_COMMENT_FIELD,
                _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
                _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_TIME_FIELD,
            }:
                q = _timeblock_update_edit_question(command)
            elif _is_timeblock_update_intent(intent) and persisted_missing_field == _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD:
                q = _timeblock_update_final_confirmation_question(command)
                _temporal_edit_log(
                    "temporal_edit_summary_rendered",
                    request_id=request_id,
                    user_id=str(user_id or ""),
                    intent="timeblock.update",
                    command=command,
                    clarification_state=persisted_missing_field,
                    **_timeblock_update_summary_payload(command),
                )
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
            elif persisted_missing_field == _TASK_CREATE_CONFIRM_FIELD:
                q = _task_create_confirmation_question(command)
            elif persisted_missing_field == _TASK_CREATE_DUPLICATE_CONFIRM_FIELD:
                existing_task = command.get("__task_create_duplicate_existing_task")
                q = (
                    _task_create_duplicate_question(existing_task)
                    if isinstance(existing_task, dict)
                    else _clarification_question_for_field(_TASK_CREATE_DUPLICATE_CONFIRM_FIELD)
                )
            elif persisted_missing_field == _SYNC_CONFLICT_FIELD:
                q = _sync_conflict_prompt(command)
            else:
                if persisted_missing_field == _TEMPORAL_EDIT_FIELD:
                    q = _temporal_active_edit_question(command)
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
        command = _normalize_task_create_from_source(command, query_text)
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
        command = _apply_timeblock_create_semantic_routing(command, resolved_text)
        has_meeting_kw = _has_meeting_create_keywords(resolved_text)
        has_timeblock_kw = _has_explicit_timeblock_create_keywords(resolved_text) or _looks_like_timeblock_task_allocation_request(resolved_text)
        has_task_kw = _has_explicit_task_create_keywords(resolved_text)
        has_generic_task_kw = _looks_like_generic_task_phrase(resolved_text)
        has_generic_undated_task_kw = _looks_like_generic_task_text(resolved_text)
        is_explicit_memory_query = _is_explicit_memory_query(resolved_text)
        is_ambiguous_create = _is_ambiguous_create_request(resolved_text)
        if not _looks_like_meeting_update_request(resolved_text) and not _looks_like_timeblock_update_request(resolved_text):
            if has_meeting_kw:
                command["intent"] = "meeting.create"
                _log_runtime_route_selected(
                    request_id=request_id,
                    raw_text=resolved_text,
                    route="meeting.create",
                    reason="explicit_meeting_marker",
                )
            elif has_timeblock_kw:
                command["intent"] = "timeblock.create"
                _log_runtime_route_selected(
                    request_id=request_id,
                    raw_text=resolved_text,
                    route="timeblock.create",
                    reason="explicit_timeblock_marker",
                )
            elif has_task_kw:
                command["intent"] = "task.create"
                _log_runtime_route_selected(
                    request_id=request_id,
                    raw_text=resolved_text,
                    route="task.create",
                    reason="explicit_task_marker",
                )
            elif has_generic_task_kw:
                command["intent"] = "task.create"
                _log_runtime_route_selected(
                    request_id=request_id,
                    raw_text=resolved_text,
                    route="task.create",
                    reason="generic_task_with_date",
                )
            elif has_generic_undated_task_kw:
                command["intent"] = "task.create"
                _log_runtime_route_selected(
                    request_id=request_id,
                    raw_text=resolved_text,
                    route="task.create",
                    reason="generic_task_without_date",
                )
            elif is_ambiguous_create:
                _log_runtime_route_selected(
                    request_id=request_id,
                    raw_text=resolved_text,
                    route="ambiguous_create",
                    reason="create_verb_without_entity_type",
                )
            elif is_explicit_memory_query:
                _log_runtime_route_selected(
                    request_id=request_id,
                    raw_text=resolved_text,
                    route="memory",
                    reason="explicit_memory_query",
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
        if _looks_like_timeblock_update_request(resolved_text):
            entities = command.get("entities")
            entities = dict(entities) if isinstance(entities, dict) else {}
            if not str(entities.get("text") or "").strip():
                entities["text"] = resolved_text
            command["entities"] = entities
            command["intent"] = "timeblock.update"
        command = _normalize_task_create_from_source(command, resolved_text)
        if (
            str(command.get("intent") or "").strip().lower() == "task.create"
            and (has_generic_task_kw or has_generic_undated_task_kw)
        ):
            LOG.info(
                "generic_task_fallback_selected",
                extra={
                    "request_id": request_id,
                    "trace_id": request_id,
                    "raw_text": str(resolved_text or "").strip(),
                    "title": str(_command_value(command, "title", "task_title", "text") or "").strip(),
                    "planned_at": str(_command_value(command, "planned_at", "due_date", "date", "when") or "").strip(),
                    "route": "task.create",
                },
            )
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

    command = _guard_temporal_not_routed_to_task_or_inbox(
        command,
        source_text=resolved_text,
        request_id=request_id,
    )
    command = _normalize_temporal_intent_alias(command)
    command = _lock_meeting_kind_for_create(command)
    command = _enrich_meeting_create_title(command, resolved_text)
    command = _apply_default_duration_for_meeting_create(command, allow_default=not from_clarification)
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
        (query_text if from_clarification else resolved_text)
        or command.get("__source_text")
        or _command_value(command, "text")
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
    sync_conflict = command.get("__sync_conflict") if isinstance(command.get("__sync_conflict"), dict) else {}
    if not sync_conflict:
        entities_for_conflict = command.get("entities")
        entities_for_conflict = entities_for_conflict if isinstance(entities_for_conflict, dict) else {}
        sync_conflict = (
            entities_for_conflict.get("__sync_conflict")
            if isinstance(entities_for_conflict.get("__sync_conflict"), dict)
            else {}
        )
        if sync_conflict:
            command["__sync_conflict"] = dict(sync_conflict)
    if sync_conflict and _is_meeting_update_intent(intent_probe):
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text if from_clarification else resolved_text,
            command=command,
            question=_sync_conflict_prompt(command),
            rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _SYNC_CONFLICT_FIELD},
            missing_field_for_log=_SYNC_CONFLICT_FIELD,
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
    if _is_task_create_intent(str(command.get("intent") or "")) and not _looks_like_timeblock_task_allocation_request(
        query_text if from_clarification else resolved_text
    ):
        safety = TemporalExecutionDecision(is_temporal=False, command=command)
    else:
        safety = resolve_temporal_execution_decision(command, query_text if from_clarification else resolved_text)
    command = safety.command if isinstance(safety.command, dict) else command
    active_confirm_flow_missing_field = normalize_clarification_field_name(str(active_missing_field or ""))
    non_temporal_confirm_flow_active = active_confirm_flow_missing_field in {
        _MEETING_UPDATE_TARGET_CONFIRM_FIELD,
        _MEETING_UPDATE_TARGET_SELECT_FIELD,
        _MEETING_UPDATE_TARGET_REFINE_FIELD,
        _MEETING_UPDATE_EDIT_CHOICE_FIELD,
        _MEETING_UPDATE_EDIT_DATE_FIELD,
        _MEETING_UPDATE_EDIT_TIME_FIELD,
        _MEETING_UPDATE_EDIT_DURATION_FIELD,
        _MEETING_UPDATE_EDIT_COMMENT_FIELD,
        _MEETING_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
        _MEETING_UPDATE_EDIT_DATE_TIME_TIME_FIELD,
        _TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD,
        _TIMEBLOCK_UPDATE_TARGET_SELECT_FIELD,
        _TIMEBLOCK_UPDATE_TARGET_REFINE_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_CHOICE_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_DATE_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_TIME_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_DURATION_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_COMMENT_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_DATE_FIELD,
        _TIMEBLOCK_UPDATE_EDIT_DATE_TIME_TIME_FIELD,
        _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD,
        _MEETING_COMMENT_TARGET_CONFIRM_FIELD,
        _MEETING_COMMENT_TARGET_SELECT_FIELD,
        _MEETING_COMMENT_TEXT_FIELD,
        _MEETING_COMMENT_FINAL_CONFIRM_FIELD,
        _TASK_COMMENT_TARGET_CONFIRM_FIELD,
        _TASK_COMMENT_TARGET_SELECT_FIELD,
        _TASK_COMMENT_TEXT_FIELD,
        _TASK_COMMENT_FINAL_CONFIRM_FIELD,
    }
    comment_update_flow_intent = _is_meeting_comment_update_intent(str(command.get("intent") or "")) or _is_task_comment_update_intent(
        str(command.get("intent") or "")
    )
    meeting_update_without_target = _is_meeting_update_intent(str(command.get("intent") or "")) and not _meeting_update_source_is_hydrated(command)
    timeblock_update_without_target = _is_timeblock_update_intent(str(command.get("intent") or "")) and not _timeblock_update_source_is_hydrated(command)
    if (
        bool(safety.is_temporal)
        and not bool(command.get("__past_date_confirmed"))
        and not meeting_update_without_target
        and not timeblock_update_without_target
        and not non_temporal_confirm_flow_active
        and not comment_update_flow_intent
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
    if _is_timeblock_update_intent(str(command.get("intent") or "")) and not _timeblock_update_source_is_hydrated(command):
        search_query_raw = str(command.get("__timeblock_update_target_hint") or "").strip()
        if not search_query_raw:
            search_query_raw = str(
                command.get("__source_text")
                or _command_value(command, "text")
                or (query_text if from_clarification else resolved_text)
                or ""
            ).strip()
        search_query = _normalize_timeblock_target_hint(search_query_raw) or search_query_raw
        command["__timeblock_update_target_hint"] = search_query
        auto_search = _timeblock_update_repeat_search(str(user_id or "").strip(), search_query)
        candidates = auto_search.get("candidates") if isinstance(auto_search.get("candidates"), list) else []
        candidate = auto_search.get("candidate") if isinstance(auto_search.get("candidate"), dict) else None
        if bool(auto_search.get("ok")) and isinstance(candidate, dict):
            command = _timeblock_update_hydrate_from_candidate(command, candidate)
            _temporal_edit_log(
                "temporal_edit_target_selected",
                request_id=request_id,
                user_id=str(user_id or ""),
                intent="timeblock.update",
                command=command,
                clarification_state=_TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD,
            )
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question=_timeblock_update_target_confirmation_question(command),
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD},
                missing_field_for_log=_TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent="timeblock.update",
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )
        if str(auto_search.get("reason") or "").strip().lower() == "multiple" and candidates:
            shortlist = [dict(c) for c in candidates if isinstance(c, dict)][:_MEETING_UPDATE_SHORTLIST_MAX]
            command = _timeblock_update_store_candidate_shortlist(command, shortlist)
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question=_timeblock_update_render_candidate_shortlist(shortlist),
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _TIMEBLOCK_UPDATE_TARGET_SELECT_FIELD},
                missing_field_for_log=_TIMEBLOCK_UPDATE_TARGET_SELECT_FIELD,
                db=db,
                context_key=ctx_key,
                app_id=app_id,
                tenant_id=tenant_id,
                user_id=user_id,
                intent="timeblock.update",
                payload=command,
                source_message_id=source_message_id,
                idempotency_key=idempotency_key,
            )
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text if from_clarification else resolved_text,
            command=command,
            question=_timeblock_update_target_refinement_question(),
            rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _TIMEBLOCK_UPDATE_TARGET_REFINE_FIELD},
            missing_field_for_log=_TIMEBLOCK_UPDATE_TARGET_REFINE_FIELD,
            db=db,
            context_key=ctx_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent="timeblock.update",
            payload=command,
            source_message_id=source_message_id,
            idempotency_key=idempotency_key,
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
            _temporal_edit_log(
                "temporal_edit_target_selected",
                request_id=request_id,
                user_id=str(user_id or ""),
                intent="meeting.update",
                command=command,
                clarification_state=_MEETING_UPDATE_TARGET_CONFIRM_FIELD,
            )
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
        if str(auto_search.get("reason") or "").strip().lower() == "sync_conflict" and isinstance(auto_search.get("conflict"), dict):
            command["__sync_conflict"] = dict(auto_search.get("conflict"))
            q = _sync_conflict_prompt(command)
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text if from_clarification else resolved_text,
                command=command,
                question=q,
                rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _SYNC_CONFLICT_FIELD},
                missing_field_for_log=_SYNC_CONFLICT_FIELD,
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
    if (
        _is_meeting_update_intent(str(command.get("intent") or ""))
        and _meeting_update_has_source(command)
        and bool(command.get("__meeting_update_target_confirmed"))
        and not bool(command.get("__meeting_update_final_confirmed"))
        and not from_clarification
    ):
        LOG.info(
            "meeting_edit_field_choice_requested",
            extra={"request_id": request_id, "flow_id": request_id, "calendar_event_id": str(_meeting_update_target_event_id(command) or "")},
        )
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text if from_clarification else resolved_text,
            command=command,
            question=_meeting_update_field_choice_question(),
            rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _MEETING_UPDATE_EDIT_CHOICE_FIELD},
            missing_field_for_log=_MEETING_UPDATE_EDIT_CHOICE_FIELD,
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
    if (
        _is_timeblock_update_intent(str(command.get("intent") or ""))
        and _timeblock_update_has_source(command)
        and bool(command.get("__timeblock_update_target_confirmed"))
        and not bool(command.get("__timeblock_update_final_confirmed"))
        and not from_clarification
    ):
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text if from_clarification else resolved_text,
            command=command,
            question=_timeblock_update_field_choice_question(),
            rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _TIMEBLOCK_UPDATE_EDIT_CHOICE_FIELD},
            missing_field_for_log=_TIMEBLOCK_UPDATE_EDIT_CHOICE_FIELD,
            db=db,
            context_key=ctx_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent="timeblock.update",
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
    meeting_update_edit_flow_active = _meeting_update_edit_flow_active(command, active_missing_field)
    timeblock_update_edit_flow_active = _timeblock_update_edit_flow_active(command, active_missing_field)
    if (
        safety.missing_field
        and not comment_update_flow_intent
        and not meeting_update_edit_flow_active
        and not timeblock_update_edit_flow_active
    ):
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
    temporal_ready = bool((not comment_update_flow_intent) and safety.is_temporal and not str(safety.missing_field or "").strip())
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
    if _is_timeblock_update_intent(intent) and not _timeblock_update_source_is_hydrated(command):
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text if from_clarification else resolved_text,
            command=command,
            question=_timeblock_update_target_refinement_question(),
            rec_payload={"outcome": "needs_clarification", "needs_clarification": True, "missing_field": _TIMEBLOCK_UPDATE_TARGET_REFINE_FIELD},
            missing_field_for_log=_TIMEBLOCK_UPDATE_TARGET_REFINE_FIELD,
            db=db,
            context_key=ctx_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent="timeblock.update",
            payload=command,
            source_message_id=source_message_id,
            idempotency_key=idempotency_key,
        )
    if (
        _is_timeblock_update_intent(intent)
        and _timeblock_update_source_is_hydrated(command)
        and bool(command.get("__timeblock_update_target_confirmed"))
        and bool(command.get("__timeblock_update_final_confirmed"))
    ):
        force_execution_route = True
    temporal_confirm_ready = temporal_ready and not bool(command.get("__temporal_confirmed"))
    if temporal_confirm_ready:
        command = _attach_temporal_draft(command)
        draft = command.get("__temporal_draft")
        draft = draft if isinstance(draft, dict) else {}
        active_payload = active_session.get("payload") if isinstance(active_session, dict) else {}
        active_payload = active_payload if isinstance(active_payload, dict) else {}
        LOG.info(
            "temporal_draft_ready",
            extra={
                "request_id": request_id,
                "flow_id": request_id,
                "intent": str(command.get("intent") or ""),
                "draft_date": str(draft.get("date") or ""),
                "draft_time": str(draft.get("time") or ""),
                "draft_duration": str(draft.get("duration_minutes") or ""),
                "draft_comment": str(draft.get("comment") or ""),
            },
        )
        summary_signature = _temporal_summary_signature(command)
        previous_signature = str(
            command.get("__temporal_summary_signature")
            or active_payload.get("__temporal_summary_signature")
            or ""
        ).strip()
        previous_render_request_id = str(
            command.get("__temporal_summary_render_request_id")
            or active_payload.get("__temporal_summary_render_request_id")
            or ""
        ).strip()
        if previous_signature and previous_signature == summary_signature:
            duplicate_request = previous_render_request_id and previous_render_request_id == request_id
            LOG.info(
                "summary_render_skipped_duplicate",
                extra={
                    "request_id": request_id,
                    "flow_id": request_id,
                    "summary_signature": summary_signature,
                    "duplicate_request": bool(duplicate_request),
                    "previous_render_request_id": previous_render_request_id,
                },
            )
            if duplicate_request:
                return _return_clarification_with_persist(
                    request_id=request_id,
                    query_text=query_text,
                    command=command,
                    question="",
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
                    suppress_send=True,
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
        command["__temporal_summary_render_request_id"] = request_id
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
                    "draft_comment": str(draft.get("comment") or ""),
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

    if not force_execution_route and _is_ambiguous_create_request(query_text):
        LOG.info(
            "ambiguous_create_prompt_rendered",
            extra={
                "request_id": request_id,
                "trace_id": request_id,
                "raw_text": str(query_text or "").strip(),
            },
        )
        return _return_clarification_with_persist(
            request_id=request_id,
            query_text=query_text,
            command=command,
            question="Что создать: событие в календаре, блок времени или задачу?",
            rec_payload={
                "outcome": "needs_clarification",
                "needs_clarification": True,
                "missing_field": _AMBIGUOUS_CREATE_FIELD,
            },
            missing_field_for_log=_AMBIGUOUS_CREATE_FIELD,
            db=db,
            context_key=ctx_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            intent=str(command.get("intent") or "unknown"),
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
        raw_due_date = str(command.get("__task_create_raw_due_date") or "").strip()
        raw_planned_at = str(command.get("__task_create_raw_planned_at") or "").strip()
        current_due_date = str(_command_value(command, "due_date", "date", "when") or "").strip()
        resolved_planned_at = str(_command_value(command, "planned_at") or "").strip()
        if (raw_due_date or raw_planned_at or current_due_date) and not resolved_planned_at:
            q = "На какую дату поставить задачу? Пришлите дату сообщением."
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text,
                command=command,
                question=q,
                rec_payload={
                    "outcome": "needs_clarification",
                    "needs_clarification": True,
                    "missing_field": _TASK_CREATE_DATE_FIELD,
                },
                missing_field_for_log=_TASK_CREATE_DATE_FIELD,
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
        duplicate_precheck = _task_create_duplicate_precheck(user_id, command)
        existing_task = duplicate_precheck.get("existing_task") if isinstance(duplicate_precheck.get("existing_task"), dict) else {}
        if bool(duplicate_precheck.get("duplicate_found")) and existing_task:
            command = dict(command)
            command["__task_create_duplicate_existing_task"] = dict(existing_task)
            q = _task_create_duplicate_question(existing_task)
            return _return_clarification_with_persist(
                request_id=request_id,
                query_text=query_text,
                command=command,
                question=q,
                rec_payload={
                    "outcome": "needs_clarification",
                    "needs_clarification": True,
                    "missing_field": _TASK_CREATE_DUPLICATE_CONFIRM_FIELD,
                },
                missing_field_for_log=_TASK_CREATE_DUPLICATE_CONFIRM_FIELD,
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
            "adapter_mode": "runtime_core_direct",
            "trace_id": request_id,
            "idempotency_key": _safe_idempotency_key(str(idempotency_key or "")),
            "intent": command.get("intent") if isinstance(command, dict) else None,
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

    explicit_memory_query = _is_explicit_memory_query(query_text)
    if not force_execution_route and not explicit_memory_query and _looks_like_generic_task_text(query_text):
        command["intent"] = "task.create"
        command = _normalize_task_create_from_source(command, query_text)
        intent = "task.create"
        force_execution_route = True
        _log_runtime_route_selected(
            request_id=request_id,
            raw_text=query_text,
            route="task.create",
            reason="generic_task_safety_guard_before_memory",
        )
        LOG.info(
            "generic_task_fallback_selected",
            extra={
                "request_id": request_id,
                "trace_id": request_id,
                "raw_text": str(query_text or "").strip(),
                "title": str(_command_value(command, "title", "task_title", "text") or "").strip(),
                "planned_at": str(_command_value(command, "planned_at", "due_date", "date", "when") or "").strip(),
                "route": "task.create",
            },
        )
    if not force_execution_route and not explicit_memory_query:
        if from_clarification and db is not None:
            db.delete_clarification_session(ctx_key)
        return {
            "outcome": "unrecognized",
            "user_message": "Не понял, что сделать с этим сообщением.",
            "command": command,
        }

    dedup_reserved = False
    dedup_reclaimed = False
    dedup_ttl = max(1, int(command_dedup_reservation_ttl_sec))
    if db is not None and idempotency_key:
        existing = db.get_command_dedup(idempotency_key)
        if existing is not None:
            status = str(existing.get("status") or "completed").strip().lower()
            LOG.info(
                "dedup_hit",
                extra={
                    "request_id": request_id,
                    "trace_id": request_id,
                    "idempotency_key": _safe_idempotency_key(str(idempotency_key or "")),
                    "intent": str(command.get("intent") or ""),
                    "status": status,
                },
            )
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
        else:
            LOG.info(
                "dedup_miss",
                extra={
                    "request_id": request_id,
                    "trace_id": request_id,
                    "idempotency_key": _safe_idempotency_key(str(idempotency_key or "")),
                    "intent": str(command.get("intent") or ""),
                },
            )
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
                "draft_comment": str(draft.get("comment") or ""),
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
    if explicit_memory_query:
        LOG.info(
            "memory_fallback_selected",
            extra={
                "request_id": request_id,
                "trace_id": request_id,
                "raw_text": str(query_text or "").strip(),
            },
        )
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
        return _resolve_rec_empty_result(
            request_id=request_id,
            query_text=query_text,
            rec_res=rec_res,
            command=command,
            explicit_memory_query=explicit_memory_query,
            db=db,
            context_key=ctx_key,
            app_id=app_id,
            tenant_id=tenant_id,
            user_id=user_id,
            source_message_id=source_message_id,
            idempotency_key=idempotency_key,
        )

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
