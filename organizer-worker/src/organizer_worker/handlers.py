from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from common.date_resolver import normalize_temporal_fields, resolve_date_phrase

from organizer_worker import db
from organizer_worker import canon
from organizer_worker.time_legacy.aliases import TASK_STATUS_NORMALIZATION


HandlerResult = dict[str, Any]
HandlerFn = Callable[[dict[str, Any]], HandlerResult]
LOG = logging.getLogger(__name__)

EXECUTE_CONFIDENCE = 0.75
CLARIFY_CONFIDENCE = 0.40
DEFAULT_MEETING_DURATION_MINUTES = int(os.getenv("DEFAULT_MEETING_DURATION_MINUTES", "30"))
_MEETING_KIND_DEFAULT = "встреча"

INTENT_ALIAS_TO_CANON = {
    "task_create": "task.create",
    "task_complete": "task.complete",
    "task_update": "task.update",
    "task_parent_update": "task.parent.update",
    "task_move_under_parent": "task.parent.update",
    "task_move": "task.move",
    "task_set_status": "task.set_status",
    "task_reschedule": "task.reschedule",
    "timeblock_create": "timeblock.create",
    "timeblock_move": "timeblock.move",
    "timeblock_update": "timeblock.update",
    "timeblock_delete": "timeblock.delete",
    "meeting_create": "meeting.create",
    "schedule_meeting": "meeting.create",
    "create_event": "meeting.create",
    "meeting_update": "meeting.update",
    "reschedule_meeting": "meeting.update",
    "move_meeting": "meeting.update",
    "meeting_comment_update": "meeting.comment.update",
    "task_comment_update": "task.comment.update",
    "subtask_create": "subtask.create",
    "subtask_complete": "subtask.complete",
}


def build_clarification(
    question: str,
    choices: list[dict[str, Any]] | None = None,
    debug: dict[str, Any] | None = None,
) -> HandlerResult:
    out: HandlerResult = {
        "ok": False,
        "user_message": "Нужны уточнения.",
        "clarifying_question": question,
    }
    if choices:
        out["choices"] = choices
    if debug:
        out["debug"] = debug
    return out


def _entities(payload: dict[str, Any]) -> dict[str, Any]:
    # Accept either flat payload or command-parser shaped payload with "entities".
    ent = payload.get("entities")
    if isinstance(ent, dict):
        return ent
    return payload


def _need(field: str, question: str) -> HandlerResult:
    return build_clarification(question=question, debug={"missing": field})


def _ok(msg: str, **debug: Any) -> HandlerResult:
    out: HandlerResult = {"ok": True, "user_message": msg}
    if debug:
        out["debug"] = debug
    return out


def _fail(msg: str, **debug: Any) -> HandlerResult:
    out: HandlerResult = {"ok": False, "user_message": msg}
    if debug:
        out["debug"] = debug
    return out


def _safe_fail(**debug: Any) -> HandlerResult:
    out: HandlerResult = {
        "ok": False,
        "user_message": "Не могу выполнить. Уточните запрос.",
    }
    details = {"reason": "safe_fail"}
    if debug:
        details.update(debug)
    out["debug"] = details
    return out


def _normalize_meeting_kind(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("ё", "е")
    if not raw:
        return ""
    if raw.startswith("собран"):
        return "собрание"
    if raw.startswith("созвон") or raw.startswith("звон"):
        return "созвон"
    if raw.startswith("мероприят") or raw.startswith("ивент") or raw.startswith("событ"):
        return "мероприятие"
    if raw.startswith("встреч"):
        return "встреча"
    return ""


def _meeting_success_text(meeting_kind: Any, action: str) -> str:
    kind = _normalize_meeting_kind(meeting_kind) or _MEETING_KIND_DEFAULT
    action_norm = str(action or "").strip().lower()
    if action_norm == "create":
        by_kind = {
            "встреча": "Встреча создана.",
            "собрание": "Собрание создано.",
            "созвон": "Созвон создан.",
            "мероприятие": "Мероприятие создано.",
        }
    elif action_norm == "reschedule":
        by_kind = {
            "встреча": "Встреча перенесена.",
            "собрание": "Собрание перенесено.",
            "созвон": "Созвон перенесён.",
            "мероприятие": "Мероприятие перенесено.",
        }
    else:
        by_kind = {
            "встреча": "Встреча обновлена.",
            "собрание": "Собрание обновлено.",
            "созвон": "Созвон обновлён.",
            "мероприятие": "Мероприятие обновлено.",
        }
    return by_kind.get(kind, by_kind[_MEETING_KIND_DEFAULT])


def _parse_iso_utc(value: str) -> datetime:
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _with_minutes_iso(start_at: str, minutes: int) -> str:
    dt = _parse_iso_utc(start_at) + timedelta(minutes=minutes)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_status(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    alias = TASK_STATUS_NORMALIZATION.get(raw.lower(), raw.lower())
    return alias.upper()


def _normalized_datetime_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _format_duplicate_day_ru(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        dt = _parse_iso_utc(raw)
        return dt.strftime("%d.%m.%Y")
    except Exception:
        if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
            try:
                return datetime.fromisoformat(f"{raw[:10]}T00:00:00+00:00").strftime("%d.%m.%Y")
            except Exception:
                return raw[:10]
    return raw


def _task_duplicate_question(existing_task: dict[str, Any], fallback_title: str) -> str:
    duplicate_title = str(existing_task.get("title") or "").strip() or fallback_title
    duplicate_id = str(existing_task.get("id") or "").strip() or "-"
    duplicate_date = _format_duplicate_day_ru(existing_task.get("planned_at"))
    duplicate_parent = str(existing_task.get("parent_task_id") or "").strip()
    if duplicate_date:
        lead_text = f"Похоже, такая задача уже есть на {duplicate_date}"
    elif duplicate_parent:
        lead_text = "Такая задача уже есть без срока"
    else:
        lead_text = "Такая задача уже есть в InBox"
    return (
        f"{lead_text}:\n"
        f"№ {duplicate_id} — {duplicate_title}\n\n"
        "Создать ещё одну?"
    )


def _duplicate_scope_label(*, planned_day: Any, undated_scope: bool, parent_scope: Any) -> str:
    if planned_day:
        return "dated"
    if undated_scope:
        return "subtask_undated" if str(parent_scope or "").strip() else "inbox"
    return "dated"


def _is_truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve_task_id_or_question(e: dict[str, Any], conn: Any) -> tuple[int | None, HandlerResult | None]:
    top_k = canon.get_disambiguation_top_k()
    task_id = e.get("task_id")
    if task_id is not None:
        try:
            return int(task_id), None
        except Exception:
            return None, _need("task_id", "Нужен номер задачи.")

    task_ref = e.get("task_ref")
    if task_ref is None:
        return None, _need("task_ref", "Какую задачу изменить?")

    if isinstance(task_ref, dict):
        chosen_id = task_ref.get("chosen_id") or task_ref.get("task_id") or task_ref.get("id")
        if chosen_id is not None:
            try:
                return int(chosen_id), None
            except Exception:
                return None, _need("task_ref.chosen_id", "Нужен корректный номер задачи.")

        candidates = task_ref.get("candidates")
        if isinstance(candidates, list) and candidates:
            top = candidates[:top_k]
            choices = _choices_from_candidates(top)
            return None, build_clarification(
                question="Нашел несколько похожих задач. Какую именно выбрать?",
                choices=choices,
                debug={"missing": "task_ref.chosen_id", "candidates_top": top},
            )

        text_ref = task_ref.get("text") or task_ref.get("query") or ""
        task_ref = str(text_ref).strip()

    if not isinstance(task_ref, str) or not task_ref.strip():
        return None, _need("task_ref", "Какую задачу изменить?")

    if task_ref.strip().isdigit():
        return int(task_ref.strip()), None

    candidates = db.find_task_candidates(conn, task_ref=task_ref.strip(), limit=top_k)
    if not candidates:
        return None, _fail("Не нашел подходящую задачу.", task_ref=task_ref)
    if len(candidates) > 1:
        top = candidates[:top_k]
        choices = _choices_from_candidates(top)
        return None, build_clarification(
            question="Нашел несколько похожих задач. Какую именно выбрать?",
            choices=choices,
            debug={"missing": "task_ref.chosen_id", "candidates_top": top},
        )
    return int(candidates[0]["id"]), None


def _resolve_optional_task_id_for_timeblock(e: dict[str, Any], conn: Any) -> tuple[int | None, HandlerResult | None]:
    task_id = e.get("task_id")
    if task_id is not None:
        try:
            return int(task_id), None
        except Exception:
            return None, _need("task_id", "Нужен номер задачи.")

    task_ref = e.get("task_ref")
    if task_ref is None:
        return None, None

    if not _is_truthy_flag(e.get("task_ref_optional")):
        return _resolve_task_id_or_question(e, conn)

    if isinstance(task_ref, dict):
        chosen_id = task_ref.get("chosen_id") or task_ref.get("task_id") or task_ref.get("id")
        if chosen_id is not None:
            try:
                return int(chosen_id), None
            except Exception:
                return None, _need("task_ref.chosen_id", "Нужен корректный номер задачи.")
        task_ref = task_ref.get("text") or task_ref.get("query") or ""

    if not isinstance(task_ref, str) or not task_ref.strip():
        return None, None

    if task_ref.strip().isdigit():
        return int(task_ref.strip()), None

    top_k = canon.get_disambiguation_top_k()
    candidates = db.find_task_candidates(conn, task_ref=task_ref.strip(), limit=top_k)
    if len(candidates) == 1:
        return int(candidates[0]["id"]), None
    return None, None


def _timeblock_fallback_task_title(e: dict[str, Any], comment_text: str) -> str:
    candidates = [
        comment_text,
        e.get("task_ref"),
        e.get("title"),
        e.get("task_title"),
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "Блок времени"


def _resolve_goal_id_or_question(e: dict[str, Any], conn: Any) -> tuple[int | None, HandlerResult | None]:
    top_k = canon.get_disambiguation_top_k()
    goal_id = e.get("goal_id")
    if goal_id is not None:
        try:
            return int(goal_id), None
        except Exception:
            return None, _need("goal_id", "Нужен номер цели.")

    goal_ref = e.get("goal_ref")
    if goal_ref is None:
        return None, _need("goal_ref", "Какую цель выбрать?")

    if isinstance(goal_ref, dict):
        chosen_id = goal_ref.get("chosen_id") or goal_ref.get("goal_id") or goal_ref.get("id")
        if chosen_id is not None:
            try:
                return int(chosen_id), None
            except Exception:
                return None, _need("goal_ref.chosen_id", "Нужен корректный номер цели.")
        candidates = goal_ref.get("candidates")
        if isinstance(candidates, list) and candidates:
            top = candidates[:top_k]
            return None, build_clarification(
                question=str(goal_ref.get("ask") or "Какую именно цель выбрать?"),
                choices=_choices_from_candidates(top),
                debug={"missing": "goal_ref.chosen_id", "candidates_top": top},
            )
        text_ref = goal_ref.get("query") or goal_ref.get("text") or ""
        goal_ref = str(text_ref).strip()

    if not isinstance(goal_ref, str) or not goal_ref.strip():
        return None, _need("goal_ref", "Какую цель выбрать?")
    if goal_ref.strip().isdigit():
        return int(goal_ref.strip()), None

    candidates = db.find_goal_candidates(conn, goal_ref=goal_ref.strip(), limit=top_k)
    if not candidates:
        return None, _fail("Не нашел подходящую цель.", goal_ref=goal_ref)
    if len(candidates) > 1:
        top = candidates[:top_k]
        return None, build_clarification(
            question="Нашел несколько похожих целей. Какую именно выбрать?",
            choices=_choices_from_candidates(top),
            debug={"missing": "goal_ref.chosen_id", "candidates_top": top},
        )
    return int(candidates[0]["id"]), None


def _choices_from_candidates(candidates: list[Any]) -> list[dict[str, Any]]:
    choices: list[dict[str, Any]] = []
    for c in candidates:
        if not isinstance(c, dict):
            continue
        cid = c.get("id")
        title = str(c.get("title") or "").strip()
        parsed_id: int | None = None
        if cid is not None:
            try:
                parsed_id = int(cid)
            except Exception:
                parsed_id = None
        if parsed_id is None:
            continue
        label = f"#{parsed_id} {title}".strip()
        choices.append({"id": parsed_id, "label": label})
    return sorted(choices, key=lambda x: (int(x["id"]), str(x["label"])))


def _canon_ref_disambiguation(intent: str, entities: dict[str, Any]) -> HandlerResult | None:
    spec = canon.get_intent_spec(intent) or {}
    dis = spec.get("disambiguation", {})
    if not isinstance(dis, dict):
        return None

    ref_path = dis.get("ref")
    if not isinstance(ref_path, str) or not ref_path.strip():
        return None

    ref_key = ref_path.removeprefix("entities.")
    ref_value = entities.get(ref_key)
    if not isinstance(ref_value, dict):
        return None

    chosen_id = ref_value.get("chosen_id")
    candidates = ref_value.get("candidates")
    if chosen_id is not None:
        return None
    if not isinstance(candidates, list) or not candidates:
        return None

    q = ref_value.get("ask")
    if not isinstance(q, str) or not q.strip():
        q = dis.get("question_fallback")
    if not isinstance(q, str) or not q.strip():
        q = canon.get_disambiguation_default_question()

    top_k = canon.get_disambiguation_top_k()
    top = candidates[:top_k]
    choices = _choices_from_candidates(top)
    return build_clarification(
        question=q,
        choices=choices,
        debug={"missing": f"{ref_key}.chosen_id", "candidates_top": top},
    )


def _read_confidence(cmd: dict[str, Any], payload: dict[str, Any]) -> float:
    raw = payload.get("confidence")
    if raw is None:
        raw = cmd.get("confidence")
    try:
        value = float(raw)
    except Exception:
        return 1.0
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def _is_rejected(cmd: dict[str, Any], payload: dict[str, Any]) -> bool:
    rejected = payload.get("rejected")
    if rejected is None:
        rejected = cmd.get("rejected")
    return rejected is True


def _choices_if_any(entities: dict[str, Any]) -> list[dict[str, Any]] | None:
    top_k = canon.get_disambiguation_top_k()
    for value in entities.values():
        if not isinstance(value, dict):
            continue
        if value.get("chosen_id") is not None:
            continue
        candidates = value.get("candidates")
        if not isinstance(candidates, list) or not candidates:
            continue
        choices = _choices_from_candidates(candidates[:top_k])
        if choices:
            return choices
    return None


def _normalize_intent_alias(intent: str) -> str:
    raw = (intent or "").strip()
    return INTENT_ALIAS_TO_CANON.get(raw, raw)


def _intent_allowlist_choices() -> list[dict[str, Any]]:
    choices: list[dict[str, Any]] = []
    for name in sorted(INTENT_HANDLERS.keys()):
        spec = canon.get_intent_spec(name) or {}
        meaning = str(spec.get("meaning") or "").strip()
        label = meaning if meaning else name
        choices.append({"id": name, "label": label})
    return choices


def task_create(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    title = (e.get("title") or "").strip()
    if not title:
        return _need("title", "Как назвать задачу?")
    planned_at = e.get("planned_at")
    if planned_at in (None, ""):
        for fallback_key in ("due_date", "due_at", "date", "when"):
            fallback_value = e.get(fallback_key)
            if fallback_value not in (None, ""):
                planned_at = fallback_value
                break
    source_msg_id = e.get("source_msg_id")
    comment_text = str(
        e.get("comment_text")
        or e.get("comment")
        or e.get("description")
        or e.get("notes")
        or ""
    ).strip()
    parent_task_id = e.get("parent_task_id")
    parent_task_explicit_raw = e.get("parent_task_explicit")
    has_parent_task_explicit = "parent_task_explicit" in e
    parent_type = e.get("parent_type")
    parent_id = e.get("parent_id")
    duplicate_check_override = _is_truthy_flag(e.get("duplicate_check_override"))
    source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
    parent_source = str(source.get("channel") or source.get("adapter") or "manual").strip() or "manual"
    normalized_planned_at = str(planned_at).strip() if planned_at is not None else None
    if parent_task_id is not None and has_parent_task_explicit and not _is_truthy_flag(parent_task_explicit_raw):
        LOG.info(
            "task_parent_resolution",
            extra={
                "source": parent_source,
                "title": title,
                "parent_task_id": str(parent_task_id),
                "parent_task_explicit": False,
                "action": "cleared_non_explicit_parent",
            },
        )
        parent_task_id = None
    else:
        LOG.info(
            "task_parent_resolution",
            extra={
                "source": parent_source,
                "title": title,
                "parent_task_id": str(parent_task_id or ""),
                "parent_task_explicit": bool(_is_truthy_flag(parent_task_explicit_raw)) if has_parent_task_explicit else None,
                "action": "keep_parent" if parent_task_id is not None else "root_task",
            },
        )
    if parent_task_id is not None and (parent_type is not None or parent_id is not None):
        return _fail("Нельзя одновременно указать родительскую задачу и структурного родителя.")

    try:
        with db.connect() as conn:
            duplicate_probe = db.inspect_duplicate_active_task_create_candidate(
                conn,
                title=title,
                planned_at=normalized_planned_at,
                parent_task_id=(int(parent_task_id) if parent_task_id is not None else None),
            )
            LOG.info(
                "task_duplicate_precheck_result",
                extra={
                    "normalized_title": duplicate_probe.get("normalized_title"),
                    "planned_at": normalized_planned_at,
                    "planned_day": duplicate_probe.get("planned_day"),
                    "undated_scope": bool(duplicate_probe.get("undated_scope")),
                    "duplicate_scope": _duplicate_scope_label(
                        planned_day=duplicate_probe.get("planned_day"),
                        undated_scope=bool(duplicate_probe.get("undated_scope")),
                        parent_scope=duplicate_probe.get("parent_scope"),
                    ),
                    "parent_task_id": duplicate_probe.get("parent_scope"),
                    "candidate_count": int(duplicate_probe.get("candidate_count") or 0),
                    "duplicate_found": bool(duplicate_probe.get("duplicate_found")),
                    "source": "worker_commit_guard",
                },
            )
            duplicate = duplicate_probe.get("duplicate") if isinstance(duplicate_probe.get("duplicate"), dict) else None
            if duplicate is not None and not duplicate_check_override:
                question = _task_duplicate_question(duplicate, title)
                return {
                    "ok": False,
                    "outcome": "needs_clarification",
                    "needs_clarification": True,
                    "missing_field": "task_create_duplicate_confirm",
                    "clarifying_question": question,
                    "user_message": question,
                    "debug": {
                        "duplicate_found": True,
                        "requires_confirmation": True,
                        "existing_task": {
                            "id": duplicate.get("id"),
                            "title": duplicate.get("title"),
                            "planned_at": duplicate.get("planned_at"),
                            "parent_task_id": duplicate.get("parent_task_id"),
                        },
                    },
                }
            task_id = db.create_task(
                conn,
                title=title,
                planned_at=normalized_planned_at,
                source_msg_id=source_msg_id,
                parent_type=(str(parent_type) if parent_type is not None else None),
                parent_id=(int(parent_id) if parent_id is not None else None),
                parent_task_id=(int(parent_task_id) if parent_task_id is not None else None),
                comment=(comment_text or None),
            )
        LOG.info(
            "task_create_date_resolution",
            extra={
                "source": parent_source,
                "intent": "task.create",
                "raw_text": str(e.get("text") or ""),
                "raw_due_date": str(e.get("due_date") or e.get("date") or e.get("when") or ""),
                "extracted_date": str(e.get("due_date") or e.get("date") or e.get("when") or ""),
                "resolved_planned_at": normalized_planned_at or "",
                "planned_at": normalized_planned_at or "",
                "summary_planned_at": normalized_planned_at or "",
                "persisted_planned_at": normalized_planned_at or "",
                "task_id": task_id,
            },
        )
        if comment_text:
            LOG.info(
                "task_create_comment_persisted",
                extra={
                    "task_id": task_id,
                    "comment_length": len(comment_text),
                    "source": parent_source,
                },
            )
        return _ok(f"Задача создана: #{task_id}.", task_id=task_id, parent_task_id=parent_task_id)
    except Exception as exc:
        return _fail("Не получилось создать задачу. Попробуйте еще раз.", error=str(exc))


def task_complete(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    try:
        with db.connect() as conn:
            task_id, question = _resolve_task_id_or_question(e, conn)
            if question is not None:
                return question
            assert task_id is not None
            db.complete_task(conn, task_id=task_id)
        return _ok("Готово. Отметил задачу выполненной.", task_id=task_id)
    except Exception as exc:
        return _fail("Не получилось завершить задачу. Проверьте номер.", error=str(exc))


def task_move(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    # Backward compatibility: old task.move routes to canonical task.set_status.
    mapped = dict(e)
    if mapped.get("status") is None and mapped.get("state") is not None:
        mapped["status"] = mapped.get("state")
    result = task_set_status({"entities": mapped})
    if isinstance(result.get("debug"), dict):
        result["debug"]["deprecated_intent"] = "task.move"
    return result


def task_set_status(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    status = _normalize_status(e.get("status"))
    if status is None:
        return _need("status", "Какой статус поставить задаче?")

    try:
        with db.connect() as conn:
            task_id, question = _resolve_task_id_or_question(e, conn)
            if question is not None:
                return question
            assert task_id is not None
            db.update_task(conn, task_id=task_id, status=status, state=status)
        return _ok("Готово. Обновил статус задачи.", task_id=task_id, status=status)
    except Exception as exc:
        return _fail("Не получилось обновить статус задачи. Проверьте данные.", error=str(exc))


def task_reschedule(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    when = (
        _normalized_datetime_value(e.get("when"))
        or _normalized_datetime_value(e.get("planned_at"))
        or _normalized_datetime_value(e.get("start_at"))
    )
    if when is None:
        return _need("when", "На когда перенести задачу?")

    try:
        with db.connect() as conn:
            task_id, question = _resolve_task_id_or_question(e, conn)
            if question is not None:
                return question
            assert task_id is not None
            db.update_task(conn, task_id=task_id, planned_at=str(when))
        return _ok("Готово. Перенес задачу.", task_id=task_id, when=str(when))
    except Exception as exc:
        return _fail("Не получилось перенести задачу. Проверьте данные.", error=str(exc))


def task_move_to(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    target_ref = e.get("target_ref")
    target_type = e.get("target_type") or e.get("parent_type")
    target_id = e.get("target_id") if e.get("target_id") is not None else e.get("parent_id")
    if isinstance(target_ref, dict):
        target_type = target_type or target_ref.get("type") or target_ref.get("parent_type")
        if target_id is None:
            target_id = target_ref.get("id") or target_ref.get("parent_id")
    if target_type is None or target_id is None:
        return _need("target", "Куда перенести задачу?")

    try:
        with db.connect() as conn:
            task_id, question = _resolve_task_id_or_question(e, conn)
            if question is not None:
                return question
            assert task_id is not None
            db.update_task(conn, task_id=task_id, parent_type=str(target_type), parent_id=int(target_id))
        return _ok("Готово. Перенес задачу.", task_id=task_id, parent_type=str(target_type), parent_id=int(target_id))
    except Exception as exc:
        return _fail("Не получилось перенести задачу. Проверьте данные.", error=str(exc))


def task_update(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    title = e.get("title")
    planned_at = e.get("planned_at")
    status = e.get("status")
    state = e.get("state")
    parent_type = e.get("parent_type")
    parent_id = e.get("parent_id")
    has_parent_task_field = "parent_task_id" in e
    parent_task_id = e.get("parent_task_id")

    # If nothing to update, ask.
    if all(v is None for v in (title, planned_at, status, state, parent_type, parent_id)) and not has_parent_task_field:
        return _need("fields", "Что именно обновить в задаче?")

    try:
        with db.connect() as conn:
            task_id, question = _resolve_task_id_or_question(e, conn)
            if question is not None:
                return question
            assert task_id is not None
            db.update_task(
                conn,
                task_id=task_id,
                title=(str(title).strip() if isinstance(title, str) else None),
                planned_at=(str(planned_at) if planned_at is not None else None),
                status=(_normalize_status(status) if status is not None else None),
                state=(_normalize_status(state) if state is not None else None),
                parent_type=(str(parent_type) if parent_type is not None else None),
                parent_id=(int(parent_id) if parent_id is not None else None),
            )
            if has_parent_task_field:
                db.update_task_parent(
                    conn,
                    task_id=task_id,
                    parent_task_id=(int(parent_task_id) if parent_task_id is not None else None),
                )
        return _ok("Готово. Обновил задачу.", task_id=task_id)
    except Exception as exc:
        return _fail("Не получилось обновить задачу. Проверьте данные.", error=str(exc))


def task_parent_update(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    if "parent_task_id" not in e:
        return _need("parent_task_id", "Под какую родительскую задачу перенести?")
    try:
        with db.connect() as conn:
            task_id, question = _resolve_task_id_or_question(e, conn)
            if question is not None:
                return question
            assert task_id is not None
            db.update_task_parent(
                conn,
                task_id=task_id,
                parent_task_id=(int(e.get("parent_task_id")) if e.get("parent_task_id") is not None else None),
            )
        return _ok("Готово. Обновил родительскую задачу.", task_id=task_id, parent_task_id=e.get("parent_task_id"))
    except Exception as exc:
        return _fail("Не получилось обновить родительскую задачу. Проверьте данные.", error=str(exc))


def subtask_create(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    task_id = e.get("task_id")
    title = (e.get("title") or "").strip()
    if task_id is None:
        return _need("task_id", "К какой задаче добавить подзадачу? Пришлите номер задачи.")
    if not title:
        return _need("title", "Как назвать подзадачу?")

    try:
        with db.connect() as conn:
            sub_id = db.create_subtask(conn, task_id=int(task_id), title=title, source_msg_id=e.get("source_msg_id"))
        return _ok(f"Подзадача создана: #{sub_id}.", subtask_id=sub_id, task_id=int(task_id))
    except Exception as exc:
        return _fail("Не получилось создать подзадачу. Попробуйте еще раз.", error=str(exc))


def subtask_complete(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    subtask_id = e.get("subtask_id")
    if subtask_id is None:
        return _need("subtask_id", "Какую подзадачу завершить? Пришлите номер.")

    try:
        with db.connect() as conn:
            db.complete_subtask(conn, subtask_id=int(subtask_id))
        return _ok("Готово. Отметил подзадачу выполненной.", subtask_id=int(subtask_id))
    except Exception as exc:
        return _fail("Не получилось завершить подзадачу. Проверьте номер.", error=str(exc), subtask_id=subtask_id)


def meeting_create(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    start_at = _normalized_datetime_value(e.get("start_at"))
    duration_min = e.get("duration_minutes")
    if duration_min is None:
        duration_min = e.get("duration_min")
    user_id = str(e.get("user_id") or "").strip()

    meeting_kind = _normalize_meeting_kind(e.get("meeting_kind") or e.get("event_kind"))
    if duration_min is None:
        duration_min = int(DEFAULT_MEETING_DURATION_MINUTES)
        e["duration_minutes"] = int(DEFAULT_MEETING_DURATION_MINUTES)
    if not start_at:
        return _need("start_at", "На какое время запланировать встречу?")
    if not user_id:
        return _need("user_id", "Не удалось определить пользователя для создания встречи.")

    try:
        duration_value = int(duration_min)
    except Exception:
        return _need("duration_minutes", "На сколько минут запланировать встречу?")
    if duration_value <= 0:
        return _need("duration_minutes", "На сколько минут запланировать встречу?")

    LOG.info(
        "creating_meeting",
        extra={
            "user_id": user_id,
            "start_at": str(start_at),
            "duration": duration_value,
        },
    )
    return _ok(
        _meeting_success_text(meeting_kind, "create"),
        start_at=str(start_at),
        duration_minutes=duration_value,
        user_id=user_id,
        comment_text=str(
            e.get("comment_text")
            or e.get("comment")
            or e.get("description")
            or e.get("notes")
            or ""
        ).strip(),
    )


def meeting_update(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    meeting_kind = _normalize_meeting_kind(e.get("meeting_kind") or e.get("event_kind"))
    user_id = str(e.get("user_id") or "").strip()
    if not user_id:
        return _need("user_id", "Не удалось определить пользователя для изменения встречи.")

    has_any_change = any(
        [
            bool(str(e.get("start_at") or "").strip()),
            bool(str(e.get("start_at_date") or e.get("date") or "").strip()),
            bool(str(e.get("start_at_time") or e.get("time") or "").strip()),
            e.get("duration_minutes") is not None,
            e.get("duration_min") is not None,
        ]
    )
    if not has_any_change:
        return _need("start_at_time", "Что изменить во встрече: дату, время или длительность?")

    return _ok(
        _meeting_success_text(meeting_kind, "reschedule"),
        user_id=user_id,
        start_at=str(e.get("start_at") or ""),
        duration_minutes=e.get("duration_minutes") if e.get("duration_minutes") is not None else e.get("duration_min"),
        calendar_event_id=str(e.get("calendar_event_id") or ""),
    )


def meeting_comment_update(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    user_id = str(e.get("user_id") or "").strip()
    if not user_id:
        return _need("user_id", "Не удалось определить пользователя для изменения встречи.")
    calendar_event_id = str(e.get("calendar_event_id") or "").strip()
    if not calendar_event_id:
        return _need("calendar_event_id", "Уточните, какую встречу нужно изменить.")
    comment_text = str(
        e.get("comment_text")
        or e.get("comment")
        or e.get("description")
        or e.get("notes")
        or ""
    ).strip()
    if not comment_text:
        return _need("comment_text", "Какой комментарий добавить во встречу?")
    return _ok(
        "Комментарий встречи обновлён.",
        user_id=user_id,
        calendar_event_id=calendar_event_id,
        comment_text=comment_text,
    )


def task_comment_update(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    comment_text = str(
        e.get("comment_text")
        or e.get("comment")
        or e.get("description")
        or e.get("notes")
        or ""
    ).strip()
    if not comment_text:
        return _need("comment_text", "Какой комментарий добавить к задаче?")

    try:
        with db.connect() as conn:
            task_id, question = _resolve_task_id_or_question(e, conn)
            if question is not None:
                return question
            assert task_id is not None
            db.update_task(conn, task_id=task_id, comment=comment_text)
        return _ok("Комментарий задачи обновлён.", task_id=task_id, comment_text=comment_text)
    except Exception as exc:
        return _fail("Не получилось обновить комментарий задачи. Проверьте данные.", error=str(exc))


def timeblock_create(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    start_at = _normalized_datetime_value(e.get("start_at"))
    duration_min = e.get("duration_minutes")
    user_id = str(e.get("user_id") or "").strip()
    comment_text = str(
        e.get("comment_text")
        or e.get("comment")
        or e.get("description")
        or e.get("notes")
        or ""
    ).strip()
    if duration_min is None:
        duration_min = e.get("duration_min")
    # Deterministic question order: duration first, then start_at.
    if duration_min is None:
        return _need("duration_minutes", "На сколько минут поставить блок?")
    if not start_at:
        return _need("start_at", "На какое время поставить блок?")

    try:
        with db.connect() as conn:
            task_id, question = _resolve_optional_task_id_for_timeblock(e, conn)
            if question is not None:
                return question
            created_fallback_task_id: int | None = None
            if task_id is None:
                fallback_title = _timeblock_fallback_task_title(e, comment_text)
                created_fallback_task_id = db.create_task(conn, title=fallback_title)
                task_id = created_fallback_task_id
            LOG.info(
                "creating_timeblock",
                extra={
                    "user_id": user_id,
                    "start_at": str(start_at),
                    "duration": int(duration_min),
                },
            )
            if db.time_blocks_require_user_id(conn) and not user_id:
                LOG.error(
                    "creating_timeblock_missing_user_id",
                    extra={"start_at": str(start_at), "duration": int(duration_min)},
                )
                return _fail(
                    "Не получилось создать блок времени: не найден пользователь.",
                    reason="missing_user_id",
                )
            end_value = _with_minutes_iso(str(start_at), int(duration_min))
            tb_id = db.create_time_block(
                conn,
                task_id=task_id,
                start_at=str(start_at),
                end_at=str(end_value),
                user_id=(user_id or None),
                comment=(comment_text or None),
            )
        debug: dict[str, Any] = {"time_block_id": tb_id, "task_id": task_id}
        if created_fallback_task_id is not None:
            debug["created_fallback_task_id"] = created_fallback_task_id
        return _ok("Блок времени создан.", **debug)
    except Exception as exc:
        return _fail("Не получилось создать блок времени. Проверьте данные.", error=str(exc))


def timeblock_move(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    tb_id = e.get("time_block_id")
    if tb_id is None:
        return _need("time_block_id", "Какой блок времени изменить? Пришлите номер блока.")

    start_at = _normalized_datetime_value(e.get("start_at"))
    end_at = _normalized_datetime_value(e.get("end_at"))
    task_id = e.get("task_id")
    comment_text = str(
        e.get("comment_text")
        or e.get("comment")
        or e.get("description")
        or e.get("notes")
        or ""
    ).strip()
    if start_at is None and end_at is None and task_id is None and not comment_text:
        return _need("fields", "Что изменить в блоке времени?")

    try:
        with db.connect() as conn:
            db.move_time_block(
                conn,
                time_block_id=int(tb_id),
                start_at=(str(start_at) if start_at is not None else None),
                end_at=(str(end_at) if end_at is not None else None),
                task_id=(int(task_id) if task_id is not None else None),
                comment=(comment_text or None),
            )
        return _ok("Готово. Обновил блок времени.", time_block_id=int(tb_id))
    except Exception as exc:
        return _fail("Не получилось обновить блок времени. Проверьте данные.", error=str(exc))


def timeblock_delete(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    tb_id = e.get("time_block_id")
    if tb_id is None:
        return _need("time_block_id", "Какой блок времени удалить? Пришлите номер блока.")

    try:
        with db.connect() as conn:
            db.delete_time_block(conn, time_block_id=int(tb_id))
        return _ok("Блок времени удален.", time_block_id=int(tb_id))
    except Exception as exc:
        return _fail("Не получилось удалить блок времени. Проверьте номер.", error=str(exc))


def reg_run(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    regulation_id = e.get("regulation_id")
    period_key = e.get("period_key")
    status = e.get("status")
    due_date = e.get("due_date")
    due_time_local = e.get("due_time_local")

    if regulation_id is None:
        return _need("regulation_id", "Какое правило выполнить? Пришлите номер правила.")
    if not period_key:
        return _need("period_key", "За какой период? Пришлите ключ периода.")
    if not status:
        return _need("status", "Какой статус поставить? Например DONE или SKIPPED.")
    if not due_date:
        return _need("due_date", "На какую дату? Пришлите дату.")

    status_norm = str(status).strip().upper()
    done_at = None
    if status_norm == "DONE":
        done_at = None  # runtime may fill later; keep optional

    try:
        with db.connect() as conn:
            run_id = db.upsert_regulation_run(
                conn,
                regulation_id=int(regulation_id),
                period_key=str(period_key),
                status=status_norm,
                due_date=str(due_date),
                due_time_local=(str(due_time_local) if due_time_local is not None else None),
                done_at=done_at,
            )
        return _ok("Готово.", regulation_run_id=run_id, regulation_id=int(regulation_id))
    except Exception as exc:
        return _fail("Не получилось выполнить правило. Попробуйте еще раз.", error=str(exc))


def reg_status(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    regulation_id = e.get("regulation_id")
    limit = int(e.get("limit") or 10)

    try:
        with db.connect() as conn:
            rows = db.list_regulation_runs(conn, regulation_id=(int(regulation_id) if regulation_id is not None else None), limit=limit)
        if not rows:
            return _ok("Пока нет запусков правил.", regulation_id=regulation_id)
        return _ok("Статус правил обновлен.", regulation_id=regulation_id, count=len(rows))
    except Exception as exc:
        return _fail("Не получилось получить статус правил.", error=str(exc))


def state_get(payload: dict[str, Any]) -> HandlerResult:
    try:
        with db.connect() as conn:
            st = db.get_state(conn)
        return _ok(
            "Состояние обновлено.",
            tasks_total=st.tasks_total,
            subtasks_total=st.subtasks_total,
            time_blocks_total=st.time_blocks_total,
            regulations_total=st.regulations_total,
            regulation_runs_total=st.regulation_runs_total,
            queue_total=st.queue_total,
            cycles_total=st.cycles_total,
            goals_total=st.goals_total,
            nudges_total=st.nudges_total,
        )
    except Exception as exc:
        return _fail("Не получилось получить состояние.", error=str(exc))


def cycle_create(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    name = (e.get("name") or e.get("title") or "").strip()
    start_date = e.get("start_date") or e.get("date_from")
    end_date = e.get("end_date") or e.get("date_to")
    if not name:
        return _need("name", "Как назвать цикл?")
    if not start_date:
        return _need("start_date", "Какая дата начала цикла?")
    if not end_date:
        return _need("end_date", "Какая дата конца цикла?")

    try:
        with db.connect() as conn:
            cycle_id = db.create_cycle(conn, name=name, start_date=str(start_date), end_date=str(end_date))
        return _ok("Цикл создан.", cycle_id=cycle_id)
    except Exception as exc:
        return _fail("Не получилось создать цикл. Попробуйте еще раз.", error=str(exc))


def cycle_set_active(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    cycle_id = e.get("cycle_id")
    if cycle_id is None:
        return _need("cycle_id", "Какой цикл сделать активным? Пришлите номер.")

    try:
        with db.connect() as conn:
            db.cycles_set_active(conn, cycle_id=int(cycle_id))
        return _ok("Готово. Сделал цикл активным.", cycle_id=int(cycle_id))
    except Exception as exc:
        return _fail("Не получилось сделать цикл активным. Проверьте номер.", error=str(exc), cycle_id=cycle_id)


def cycle_close(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    cycle_id = e.get("cycle_id")
    if cycle_id is None:
        return _need("cycle_id", "Какой цикл закрыть? Пришлите номер.")

    try:
        with db.connect() as conn:
            summary = db.close_cycle(conn, cycle_id=int(cycle_id))
        return _ok("Цикл закрыт.", cycle_id=int(cycle_id), summary=summary)
    except Exception as exc:
        return _fail("Не получилось закрыть цикл. Проверьте номер.", error=str(exc), cycle_id=cycle_id)


def goal_create(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    cycle_id = e.get("cycle_id")
    title = (e.get("title") or "").strip()
    success_criteria = (e.get("success_criteria") or "").strip()
    planned_end_date = e.get("planned_end_date")
    if cycle_id is None:
        # default to active cycle
        with db.connect() as conn:
            active = db.get_active_cycle(conn)
        if active is not None:
            cycle_id = active.get("id")
    if cycle_id is None:
        return _need("cycle_id", "Для какого цикла добавить цель?")
    if not title:
        return _need("title", "Как назвать цель?")
    if not success_criteria:
        return _need("success_criteria", "По какому критерию понять, что цель достигнута?")
    if not planned_end_date:
        return _need("planned_end_date", "До какой даты запланирована цель?")

    try:
        with db.connect() as conn:
            goal_id = db.create_goal(
                conn,
                cycle_id=int(cycle_id),
                title=title,
                success_criteria=success_criteria,
                planned_end_date=str(planned_end_date),
            )
        return _ok("Цель добавлена.", goal_id=goal_id, cycle_id=int(cycle_id))
    except Exception as exc:
        return _fail("Не получилось добавить цель. Проверьте данные.", error=str(exc))


def goal_update(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    fields: dict[str, Any] = {}
    if e.get("title") is not None:
        fields["title"] = str(e.get("title")).strip()
    if e.get("success_criteria") is not None:
        fields["success_criteria"] = str(e.get("success_criteria")).strip()
    if e.get("planned_end_date") is not None:
        fields["planned_end_date"] = str(e.get("planned_end_date")).strip()
    if e.get("status") is not None:
        fields["status"] = str(e.get("status")).strip().upper()
    if not fields:
        return _need("fields", "Что изменить в цели?")

    try:
        with db.connect() as conn:
            goal_id, question = _resolve_goal_id_or_question(e, conn)
            if question is not None:
                return question
            assert goal_id is not None
            db.update_goal(conn, goal_id=goal_id, fields=fields)
        return _ok("Готово. Обновил цель.", goal_id=goal_id)
    except Exception as exc:
        return _fail("Не получилось обновить цель. Проверьте данные.", error=str(exc))

def goal_reschedule(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    new_end_date = e.get("new_end_date")
    if not new_end_date:
        return _need("new_end_date", "На какую дату перенести цель?")

    try:
        with db.connect() as conn:
            goal_id, question = _resolve_goal_id_or_question(e, conn)
            if question is not None:
                return question
            assert goal_id is not None
            event_id = db.reschedule_goal(conn, goal_id=goal_id, new_end_date=str(new_end_date))
        return _ok("Перенес срок цели.", goal_id=goal_id, event_id=event_id)
    except Exception as exc:
        return _fail("Не получилось перенести срок цели.", error=str(exc))


def goal_link_task(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    try:
        with db.connect() as conn:
            goal_id, gq = _resolve_goal_id_or_question(e, conn)
            if gq is not None:
                return gq
            task_id, tq = _resolve_task_id_or_question(e, conn)
            if tq is not None:
                return tq
            assert goal_id is not None and task_id is not None
            db.link_task_to_goal(conn, task_id=task_id, goal_id=goal_id)
        return _ok("Привязал задачу к цели.", task_id=task_id, goal_id=goal_id)
    except Exception as exc:
        return _fail("Не получилось привязать задачу к цели.", error=str(exc))


def goal_close(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    close_as = str(e.get("close_as") or "DONE").strip().upper()
    if close_as not in {"DONE", "DROPPED"}:
        return _need("close_as", "Закрыть как DONE или DROPPED?")

    try:
        with db.connect() as conn:
            goal_id, question = _resolve_goal_id_or_question(e, conn)
            if question is not None:
                return question
            assert goal_id is not None
            db.close_goal(conn, goal_id=goal_id, close_as=close_as)
        return _ok("Цель закрыта.", goal_id=goal_id, close_as=close_as)
    except Exception as exc:
        return _fail("Не получилось закрыть цель. Проверьте номер.", error=str(exc))


def nudge_list(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    user_id = str(e.get("user_id") or "default")
    today = str(e.get("today") or datetime.now(timezone.utc).date().isoformat())

    try:
        with db.connect() as conn:
            rows = db.list_nudges(conn, user_id=user_id, today=today)
        if not rows:
            return _ok("Сейчас ничего не нужно.", nudges=[])
        return _ok("Есть подсказки.", nudges=rows, count=len(rows))
    except Exception as exc:
        return _fail("Не получилось получить подсказки.", error=str(exc))


def nudge_ack(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    user_id = str(e.get("user_id") or "default")
    nudge_id = e.get("nudge_id")
    nudge_type = e.get("nudge_type")
    entity_type = e.get("entity_type")
    entity_id = e.get("entity_id")

    if nudge_id and (nudge_type is None or entity_type is None or entity_id is None):
        parts = str(nudge_id).split(":")
        if len(parts) == 3:
            nudge_type, entity_type, entity_id = parts[0], parts[1], parts[2]
    if nudge_type is None or entity_type is None or entity_id is None:
        return _need("nudge_id", "Какую подсказку отметить?")

    try:
        with db.connect() as conn:
            db.ack_nudge(
                conn,
                user_id=user_id,
                nudge_type=str(nudge_type),
                entity_type=str(entity_type),
                entity_id=int(entity_id),
            )
        return _ok("Хорошо. Учту.", nudge_type=str(nudge_type), entity_type=str(entity_type), entity_id=int(entity_id))
    except Exception as exc:
        return _fail("Не получилось отметить подсказку. Попробуйте еще раз.", error=str(exc))


def digest_daily(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    today = str(e.get("today") or datetime.now(timezone.utc).date().isoformat())
    tomorrow = str(e.get("tomorrow") or (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat())
    user_id = str(e.get("user_id") or "default")
    try:
        with db.connect() as conn:
            digest = db.compute_daily_digest(conn, today=today, tomorrow=tomorrow, user_id=user_id)
        return _ok("Сводка готова.", digest=digest)
    except Exception as exc:
        return _fail("Не получилось собрать сводку.", error=str(exc))


def tasks_list_today(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    today = str(e.get("today") or datetime.now(timezone.utc).date().isoformat())
    limit = int(e.get("limit") or 50)
    try:
        with db.connect() as conn:
            tasks = db.list_tasks_today(conn, today=today, limit=limit)
        return _ok("Список задач на сегодня готов.", tasks=tasks, count=len(tasks), today=today)
    except Exception as exc:
        return _fail("Не получилось получить задачи на сегодня.", error=str(exc))


def tasks_list_tomorrow(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    tomorrow = str(e.get("tomorrow") or (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat())
    limit = int(e.get("limit") or 50)
    try:
        with db.connect() as conn:
            tasks = db.list_tasks_tomorrow(conn, tomorrow=tomorrow, limit=limit)
        return _ok("Список задач на завтра готов.", tasks=tasks, count=len(tasks), tomorrow=tomorrow)
    except Exception as exc:
        return _fail("Не получилось получить задачи на завтра.", error=str(exc))


def tasks_list_active(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    limit = int(e.get("limit") or 100)
    try:
        with db.connect() as conn:
            tasks = db.list_tasks_active(conn, limit=limit)
        return _ok("Список активных задач готов.", tasks=tasks, count=len(tasks))
    except Exception as exc:
        return _fail("Не получилось получить активные задачи.", error=str(exc))


def goals_list_overdue(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    today = str(e.get("today") or datetime.now(timezone.utc).date().isoformat())
    limit = int(e.get("limit") or 20)
    try:
        with db.connect() as conn:
            goals = db.list_goals_overdue(conn, today=today, limit=limit)
        return _ok("Список просроченных целей готов.", goals=goals, count=len(goals), today=today)
    except Exception as exc:
        return _fail("Не получилось получить просроченные цели.", error=str(exc))


def goals_list_due_soon(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    today = str(e.get("today") or datetime.now(timezone.utc).date().isoformat())
    tomorrow = str(e.get("tomorrow") or (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat())
    limit = int(e.get("limit") or 20)
    try:
        with db.connect() as conn:
            goals = db.list_goals_due_soon(conn, today=today, tomorrow=tomorrow, limit=limit)
        return _ok("Список целей со сроком сегодня/завтра готов.", goals=goals, count=len(goals), today=today, tomorrow=tomorrow)
    except Exception as exc:
        return _fail("Не получилось получить цели со сроком сегодня/завтра.", error=str(exc))


def goals_list_at_risk(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    today = str(e.get("today") or datetime.now(timezone.utc).date().isoformat())
    limit = int(e.get("limit") or 20)
    try:
        with db.connect() as conn:
            goals = db.list_goals_at_risk(conn, today=today, limit=limit)
        return _ok("Список целей под риском готов.", goals=goals, count=len(goals), today=today)
    except Exception as exc:
        return _fail("Не получилось получить цели под риском.", error=str(exc))


INTENT_HANDLERS: dict[str, HandlerFn] = {
    "task.create": task_create,
    "task.complete": task_complete,
    "task.move": task_move,
    "task.set_status": task_set_status,
    "task.reschedule": task_reschedule,
    "task.parent.update": task_parent_update,
    "task.move_to": task_move_to,
    "task.update": task_update,
    "subtask.create": subtask_create,
    "subtask.complete": subtask_complete,
    "meeting.create": meeting_create,
    "meeting.update": meeting_update,
    "meeting.comment.update": meeting_comment_update,
    "task.comment.update": task_comment_update,
    "timeblock.create": timeblock_create,
    "timeblock.move": timeblock_move,
    "timeblock.update": timeblock_move,
    "timeblock.delete": timeblock_delete,
    "reg.run": reg_run,
    "reg.status": reg_status,
    "cycle.create": cycle_create,
    "cycle.close": cycle_close,
    "goal.create": goal_create,
    "goal.update": goal_update,
    "goal.close": goal_close,
    "goal.reschedule": goal_reschedule,
    "goal.link_task": goal_link_task,
    "nudge.list": nudge_list,
    "nudge.ack": nudge_ack,
    "digest.daily": digest_daily,
    "tasks.list_today": tasks_list_today,
    "tasks.list_tomorrow": tasks_list_tomorrow,
    "tasks.list_active": tasks_list_active,
    "goals.list_overdue": goals_list_overdue,
    "goals.list_due_soon": goals_list_due_soon,
    "goals.list_at_risk": goals_list_at_risk,
    "state.get": state_get,
}


def dispatch(intent: str, payload: dict[str, Any]) -> HandlerResult:
    fn = INTENT_HANDLERS.get(intent)
    if fn is None:
        return _fail("Я пока не умею выполнять эту команду.", intent=intent)
    return fn(payload)


def dispatch_intent(cmd: dict[str, Any]) -> HandlerResult:
    # Supports either {"intent": "...", "entities": {...}} or {"command": {"intent": "...", ...}} shapes.
    intent = cmd.get("intent")
    payload: dict[str, Any] = cmd
    if not isinstance(intent, str) or not intent.strip():
        command = cmd.get("command")
        if isinstance(command, dict):
            intent = command.get("intent")
            payload = command

    if not isinstance(intent, str) or not intent.strip():
        return build_clarification(
            question="Уточните, какую команду выполнить.",
            choices=_intent_allowlist_choices(),
            debug={"reason": "missing_intent"},
        )
    intent_norm = _normalize_intent_alias(intent)
    if intent_norm not in INTENT_HANDLERS:
        return build_clarification(
            question="Уточните, какую команду выполнить.",
            choices=_intent_allowlist_choices(),
            debug={"reason": "unknown_intent", "intent": intent_norm},
        )
    payload = normalize_temporal_fields(payload, now=datetime.now()) if isinstance(payload, dict) else payload
    entities = _entities(payload)
    if intent_norm == "task.create":
        for field in ("planned_at", "due_date", "date", "when"):
            raw_value = str(
                entities.get(field)
                or payload.get(field)
                or entities.get(f"__raw_{field}")
                or payload.get(f"__raw_{field}")
                or ""
            ).strip()
            if not raw_value:
                continue
            resolution = resolve_date_phrase(raw_value, now=datetime.now())
            if resolution.ok:
                break
            if resolution.needs_clarification:
                return {
                    "ok": False,
                    "outcome": "needs_clarification",
                    "needs_clarification": True,
                    "missing_field": "task_create_due_date",
                    "clarifying_question": "На какую дату поставить задачу?",
                    "user_message": "На какую дату поставить задачу?",
                    "debug": {"date_resolution_reason": resolution.reason or "unresolved_date_phrase", "field": field},
                }
    if intent_norm in {"timeblock.create", "meeting.create"}:
        LOG.info(
            "runtime_temporal_validation_probe",
            extra={
                "incoming_intent": str(intent or ""),
                "incoming_start_at": str(entities.get("start_at") or ""),
                "incoming_start_at_date": str(entities.get("start_at_date") or entities.get("date") or ""),
                "incoming_start_time": str(entities.get("start_at_time") or entities.get("start_time") or ""),
                "incoming_duration_minutes": str(entities.get("duration_minutes") or entities.get("duration_min") or ""),
            },
        )
        # Contract assert (ML CONTRACT v2.2): start_at must be explicitly provided.
        start_at = _normalized_datetime_value(entities.get("start_at"))
        if not start_at:
            LOG.info(
                "runtime_temporal_validation_result",
                extra={
                    "incoming_intent": str(intent or ""),
                    "validation_result": "missing",
                    "missing_fields": "entities.start_at",
                },
            )
            return build_clarification(
                question=(
                    "На какое время запланировать встречу?"
                    if intent_norm == "meeting.create"
                    else "На какое время поставить блок?"
                ),
                debug={
                    "missing": "entities.start_at",
                    "contract": "ML CONTRACT v2.2",
                    "intent": "meeting_create" if intent_norm == "meeting.create" else "timeblock_create",
                },
            )
    if intent_norm == "meeting.create":
        duration_missing = (
            entities.get("duration_minutes") is None
            and entities.get("duration_min") is None
        )
        if duration_missing:
            entities["duration_minutes"] = int(DEFAULT_MEETING_DURATION_MINUTES)
    confidence = _read_confidence(cmd, payload)
    rejected = _is_rejected(cmd, payload)

    if rejected or confidence < CLARIFY_CONFIDENCE:
        return _safe_fail(confidence=confidence, rejected=rejected)

    # Canon v2: if candidates were provided and choice is not made, ask one clarifying question.
    dis = _canon_ref_disambiguation(intent_norm, entities)
    if dis is not None:
        return dis

    if confidence < EXECUTE_CONFIDENCE:
        question = canon.build_one_question(intent_norm, entities) or "Подтвердите, что именно нужно сделать."
        return build_clarification(question=question, choices=_choices_if_any(entities), debug={"confidence": confidence})

    # Canon v2: centralized required-field validation and one-question clarification.
    missing = canon.validate_required(intent_norm, entities)
    if intent_norm == "timeblock.create" and missing:
        missing = [
            item
            for item in missing
            if str(item).strip() not in {"entities.task_id|entities.task_ref", "entities.task_ref|entities.task_id"}
        ]
    if intent_norm in {"timeblock.create", "meeting.create"}:
        LOG.info(
            "runtime_temporal_validation_result",
            extra={
                "incoming_intent": str(intent or ""),
                "validation_result": ("missing" if missing else "ok"),
                "missing_fields": ",".join(str(x) for x in missing),
            },
        )
    if missing:
        question = canon.build_one_question(intent_norm, entities) or "Нужны уточнения."
        return build_clarification(question=question, debug={"missing": missing})

    return dispatch(intent_norm, payload)
