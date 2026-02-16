from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .llm_client import LLMClient


INTENTS = [
    "CREATE_MEETING",
    "MOVE_MEETING",
    "CREATE_TASK",
    "PLAN_TASK",
    "START_WORK",
    "STOP_WORK",
    "EXPORT",
    "SHOW_INBOX_TASKS",
    "COMPLETE_TASK",
    "NONE",
]


@dataclass
class NormalizedCommand:
    intent: str
    confidence: float
    args: Dict[str, Any]
    canonical_text: str
    need_user_confirmation: bool
    missing: List[str]
    notes: str

    @staticmethod
    def none(notes: str = "") -> "NormalizedCommand":
        return NormalizedCommand(
            intent="NONE",
            confidence=0.0,
            args={},
            canonical_text="",
            need_user_confirmation=True,
            missing=[],
            notes=notes,
        )


def _extract_json_object(text: str) -> Optional[dict]:
    if not text:
        return None

    cleaned = re.sub(r"```(?:json)?", "", text, flags=re.IGNORECASE).replace("```", "").strip()

    try:
        obj = json.loads(cleaned)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None

    candidate = cleaned[start : end + 1].strip()
    try:
        obj = json.loads(candidate)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _validate(obj: dict) -> NormalizedCommand:
    intent = str(obj.get("intent", "NONE")).strip().upper()
    if intent not in INTENTS:
        intent = "NONE"

    try:
        confidence = float(obj.get("confidence", 0.0))
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    args = obj.get("args", {})
    if not isinstance(args, dict):
        args = {}

    canonical_text = str(obj.get("canonical_text", "")).strip()
    missing = obj.get("missing", [])
    if not isinstance(missing, list):
        missing = []

    notes = str(obj.get("notes", "")).strip()

    return NormalizedCommand(
        intent=intent,
        confidence=confidence,
        args=args,
        canonical_text=canonical_text,
        need_user_confirmation=True,
        missing=[str(x) for x in missing],
        notes=notes,
    )


def build_normalizer_prompt(user_text: str) -> list[dict[str, str]]:
    system = (
        "Ты — нормализатор команд для личного органайзера.\n"
        "ТЫ НЕ ПРИНИМАЕШЬ РЕШЕНИЙ И НЕ ВЫПОЛНЯЕШЬ ДЕЙСТВИЙ.\n"
        "Твоя задача: преобразовать пользовательскую фразу в СТРОГИЙ JSON-объект.\n"
        "Всегда возвращай ТОЛЬКО JSON без пояснений.\n\n"
        "Доступные intent:\n"
        "- CREATE_MEETING (создать встречу)\n"
        "- MOVE_MEETING (перенести встречу)\n"
        "- CREATE_TASK (создать задачу)\n"
        "- PLAN_TASK (распланировать задачи)\n"
        "- START_WORK (начать работу/таймер)\n"
        "- STOP_WORK (остановить работу/таймер)\n"
        "- EXPORT (экспорт)\n"
        "- SHOW_INBOX_TASKS (входящие/неразобранные задачи)\n"
        "- COMPLETE_TASK (завершить задачу)\n"
        "- NONE (если не уверен)\n\n"
        "Правила:\n"
        "1) НИКОГДА не угадывай отсутствующие данные. Если не хватает — укажи в missing.\n"
        "2) confidence 0..1: 0.9 если всё ясно, 0.6 если intent ясен, но аргументы неполные, иначе 0.0.\n"
        "3) need_user_confirmation всегда true.\n"
        "4) canonical_text — краткая каноническая команда на русском.\n\n"
        "Формат JSON:\n"
        "{"
        "\"intent\":\"...\","
        "\"confidence\":0.0,"
        "\"args\":{...},"
        "\"canonical_text\":\"...\","
        "\"need_user_confirmation\":true,"
        "\"missing\":[...],"
        "\"notes\":\"\""
        "}\n\n"
        "Синонимы:\n"
        "- митинг/созвон/звонок/встреча => встреча\n"
        "- 'в 16 часов' => 16:00\n"
    )

    user = "Нормализуй фразу в JSON.\n" f"Фраза: {user_text}"

    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


class LLMNormalizer:
    def __init__(self, client: LLMClient) -> None:
        self.client = client

    async def normalize(self, user_text: str) -> NormalizedCommand:
        messages = build_normalizer_prompt(user_text)
        content = await self.client.chat_completions(messages)
        if not content:
            return NormalizedCommand.none("llm_unavailable_or_timeout")

        obj = _extract_json_object(content)
        if not obj:
            return NormalizedCommand.none("llm_invalid_json")

        return _validate(obj)
