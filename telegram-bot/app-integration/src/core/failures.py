from __future__ import annotations

from typing import Any, Dict, Optional

USER_MSG_SUCCESS = "Готово."
USER_MSG_REC_EMPTY = "В памяти нет данных по этому запросу."
USER_MSG_GENERIC_FALLBACK = "Не получилось выполнить запрос. Попробуем ещё раз."


def map_failure_to_user_message(event_type: str, details: Optional[Dict[str, Any]] = None) -> str:
    """
    Returns short user-facing text without technical terms.
    """
    et = (event_type or "").strip().lower()

    if et in ("asr_unavailable", "asr_timeout", "asr_empty"):
        return "Не получилось разобрать речь. Попробуй сказать ещё раз."

    if et in ("llm_timeout", "llm_invalid_output"):
        return "Сейчас не могу корректно обработать запрос. Попробуем позже."

    if et in ("rec_rejected",):
        return "Слишком много данных для надёжного ответа. Нужно уточнение."

    if et in ("rec_empty",):
        return USER_MSG_REC_EMPTY

    if et in ("success",):
        return USER_MSG_SUCCESS

    # safe fallback
    return USER_MSG_GENERIC_FALLBACK


def build_one_clarifying_question(hints: Optional[Dict[str, Any]]) -> str:
    """
    App MUST ask exactly one question. Use hints when present.
    """
    h = hints if isinstance(hints, dict) else {}
    topics = h.get("top_topics") if isinstance(h.get("top_topics"), list) else []
    objects = h.get("top_objects") if isinstance(h.get("top_objects"), list) else []

    def pick(vals):
        for v in vals:
            if isinstance(v, dict):
                s = str(v.get("value") or "").strip()
                if s:
                    return s
        return ""

    t = pick(topics)
    o = pick(objects)

    if t and o:
        return f"Уточни: про тему «{t}» и объект «{o}»?"
    if o:
        return f"Уточни: про какой объект ты спрашиваешь (например, «{o}»)?"
    if t:
        return f"Уточни: про какую тему ты спрашиваешь (например, «{t}»)?"
    return "Уточни, пожалуйста: что именно ты имеешь в виду?"
