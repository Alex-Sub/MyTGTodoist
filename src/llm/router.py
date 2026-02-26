from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import httpx
import yaml

try:
    import jsonschema
    from jsonschema import ValidationError

    _JSONSCHEMA_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    jsonschema = None  # type: ignore[assignment]
    ValidationError = Exception  # type: ignore[misc,assignment]
    _JSONSCHEMA_AVAILABLE = False


COMMAND_KINDS = {"voice_command", "text_command"}
ASSISTANT_KIND = "assistant"

COMMAND_SYSTEM_PROMPT = (
    "You are the command_parser. Return ONLY valid JSON matching the command schema. "
    "Top-level keys must be exactly: intent, confidence, text_original, text_normalized, "
    "datetime_context, entities, needs_clarification, clarifying_question, notes. "
    "Inside entities, extract explicitly when possible: root_title (top-level context), "
    "parent_title (optional parent context), task_title (atomic action), due_datetime (ISO-8601), "
    "calendar_add (boolean true/false). "
    "Do NOT include any other top-level keys (no asr_text, no now_iso, etc.). "
    "No extra text."
)
ASSISTANT_SYSTEM_PROMPT = (
    "Ты — assistant_planner. Твоя задача — помогать пользователю управлять задачами и проектами.\n\n"
    "ПРАВИЛА:\n"
    "1) Не выдумывай факты. Используй только переданные данные.\n"
    "2) Давай результат структурированно в JSON.\n"
    "2.1) Не используй Markdown, не используй ```json```; верни только JSON как plain text.\n"
    "2.2) Всегда отвечай только на русском языке.\n"
    "Всегда отвечай только на русском языке.\n"
    "Не используй Markdown.\n"
    "Не используй ```json```.\n"
    "Верни JSON как обычный текст без пояснений.\n"
    "3) Предлагай конкретные действия, но НЕ выполняй их.\n"
    "4) Если данных недостаточно — задай 1–3 уточняющих вопроса.\n"
    "5) Учитывай приоритеты и перегруз.\n\n"
    "ФОРМАТ ОТВЕТА (ТОЛЬКО JSON):\n"
    "{\n"
    "  \"summary\": \"...\",\n"
    "  \"now_focus\": [{ \"action\": \"...\", \"why\": \"...\", \"suggested_command\": null }],\n"
    "  \"today_plan\": [{ \"task_ref\": \"...\", \"next_step\": \"...\", \"risk\": null, \"suggested_command\": null }],\n"
    "  \"risks\": [{ \"risk\": \"...\", \"impact\": \"...\", \"mitigation\": \"...\" }],\n"
    "  \"questions\": [\"...\"]\n"
    "}"
)
STRICT_JSON_PROMPT = (
    "Return ONLY valid JSON matching the schema. Top-level keys must be exactly: "
    "intent, confidence, text_original, text_normalized, datetime_context, entities, "
    "For entities extraction, include when possible: root_title, parent_title, task_title, "
    "due_datetime, calendar_add. "
    "needs_clarification, clarifying_question, notes. Do NOT include any other "
    "top-level keys (no asr_text, no now_iso, etc.). Do not include any extra text."
)
ASSISTANT_STRICT_JSON_PROMPT = (
    "Отвечай ТОЛЬКО валидным JSON. "
    "Не используй Markdown, не используй ```json```. "
    "Не добавляй пояснений или текста вне JSON. "
    "Отвечай ТОЛЬКО на русском языке."
)

SUPPORTED_INTENTS = {"task_create", "timeblock_create"}
INTENT_CHOICE_LABELS = {
    "timeblock_create": "создать блок времени",
    "task_create": "создать задачу",
}


@dataclass(slots=True)
class LLMRequest:
    kind: str
    text: str
    now_iso: Optional[str] = None


@dataclass(slots=True)
class LLMResult:
    agent: str
    provider: str
    model: str
    payload: Optional[dict[str, Any]]
    raw_text: str
    validation_ok: bool
    error: Optional[str]


@dataclass(slots=True)
class InterpretationResult:
    type: str  # "command" | "clarify"
    command: Optional[dict[str, Any]] = None
    question: Optional[str] = None
    choices: Optional[list[dict[str, str]]] = None
    expected_answer: Optional[str] = None
    debug: Optional[dict[str, Any]] = None


class OpenRouterProvider:
    def __init__(self) -> None:
        self.base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").rstrip("/")
        self.api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
        self.timeout = float(os.getenv("OPENROUTER_HTTP_TIMEOUT_S", "60"))
        self._client = httpx.Client(timeout=self.timeout)

    def chat(self, messages: list[dict[str, str]], model: str) -> str:
        url = f"{self.base_url}/chat/completions"
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if not self.api_key:
            raise RuntimeError("OPENROUTER_API_KEY is not set")
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": 0.0,
        }

        response = self._client.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]

    def close(self) -> None:
        self._client.close()


def _schema_path() -> str:
    here = os.path.abspath(os.path.dirname(__file__))
    return os.path.abspath(os.path.join(here, "..", "..", "schemas", "command.schema.json"))

def _examples_path() -> str:
    here = os.path.abspath(os.path.dirname(__file__))
    return os.path.abspath(os.path.join(here, "..", "..", "prompts", "command_examples.json"))


def _canon_path() -> Path:
    here = Path(__file__).resolve()
    return here.parents[2] / "canon" / "intents_v2.yml"


def _load_schema() -> dict[str, Any]:
    path = _schema_path()
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


@lru_cache(maxsize=1)
def _load_intent_registry() -> dict[str, str]:
    with _canon_path().open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    intents = data.get("intents") if isinstance(data, dict) else {}
    if not isinstance(intents, dict):
        return {}
    out: dict[str, str] = {}
    for raw_name, spec in intents.items():
        if not isinstance(raw_name, str):
            continue
        name = raw_name.strip().replace(".", "_")
        if name not in SUPPORTED_INTENTS:
            continue
        meaning = ""
        if isinstance(spec, dict):
            meaning = str(spec.get("meaning") or "").strip()
        out[name] = meaning or INTENT_CHOICE_LABELS.get(name, name)
    return out


@lru_cache(maxsize=1)
def _load_command_examples() -> list[dict[str, str]]:
    path = _examples_path()
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError("command_examples.json must be a list")
    max_count = int(os.getenv("COMMAND_FEWSHOT_MAX", "8"))
    return data[: max(0, max_count)]


def _build_user_text(text: str, now_iso: Optional[str]) -> str:
    if now_iso:
        return f"CURRENT_DATETIME (now_iso): {now_iso}\n\nASR_TEXT:\n{text}"
    return text


def _build_command_system_prompt() -> str:
    registry = _load_intent_registry()
    lines = []
    for name in sorted(registry.keys()):
        lines.append(f"- {name}: {registry[name]}")
    registry_block = "\n".join(lines) if lines else "- task_create\n- timeblock_create"
    return (
        COMMAND_SYSTEM_PROMPT
        + "\nAllowed intents (must use only these):\n"
        + registry_block
        + "\nIf request is ambiguous between task and timeblock, set intent=unknown and needs_clarification=true."
    )


def _validate_payload(payload: dict[str, Any]) -> tuple[bool, Optional[str]]:
    if not _JSONSCHEMA_AVAILABLE:
        return False, "jsonschema is not installed"

    schema = _load_schema()
    try:
        jsonschema.validate(instance=payload, schema=schema)
    except ValidationError as exc:
        return False, str(exc)
    return True, None


def _parse_json(text: str) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed, None
        return None, "Response JSON must be an object"
    except json.JSONDecodeError as exc:
        return None, str(exc)


def _normalize_command_payload(payload: dict[str, Any]) -> dict[str, Any]:
    # Canonical intent unification for runtime-supported names.
    intent_raw = str(payload.get("intent") or "").strip().lower()
    if intent_raw == "create_event":
        payload["intent"] = "timeblock_create"
    if intent_raw == "meeting_create":
        payload["intent"] = "timeblock_create"
    return payload


def _canonical_intent_name(intent: str) -> str:
    raw = (intent or "").strip().lower()
    aliases = {
        "create_task": "task_create",
        "task_create": "task_create",
        "create_event": "timeblock_create",
        "meeting_create": "timeblock_create",
        "timeblock_create": "timeblock_create",
    }
    return aliases.get(raw, raw)


def _is_ambiguous_task_or_timeblock(text: str) -> bool:
    s = (text or "").lower()
    has_meeting = any(k in s for k in ("созвон", "встреч", "собран", "appointment"))
    has_duration = any(k in s for k in ("мин", "час", "minutes", "hours"))
    return has_meeting and not has_duration


def _clarify_task_or_timeblock(question: str, reason: str) -> InterpretationResult:
    return InterpretationResult(
        type="clarify",
        question=question,
        choices=[
            {"id": "timeblock_create", "label": INTENT_CHOICE_LABELS["timeblock_create"]},
            {"id": "task_create", "label": INTENT_CHOICE_LABELS["task_create"]},
        ],
        expected_answer="choice_id",
        debug={"reason": reason},
    )


def _extract_args(intent: str, entities: dict[str, Any]) -> dict[str, Any]:
    if intent == "task_create":
        title = (
            entities.get("title")
            or entities.get("task_title")
            or entities.get("root_title")
            or entities.get("text")
        )
        planned_at = entities.get("planned_at") or entities.get("when") or entities.get("due_iso")
        out = {"title": title}
        if planned_at is not None:
            out["planned_at"] = planned_at
        return out
    if intent == "timeblock_create":
        title = entities.get("title") or entities.get("task_title")
        start_at = entities.get("start_at") or entities.get("start_iso") or entities.get("when")
        duration = entities.get("duration_minutes")
        if duration is None:
            duration = entities.get("duration_min")
        out = {"title": title, "start_at": start_at, "duration_minutes": duration}
        end_at = entities.get("end_at") or entities.get("end_iso")
        if end_at is not None:
            out["end_at"] = end_at
        return out
    return {}


def interpret(text: str, *, now_iso: Optional[str] = None) -> InterpretationResult:
    req = LLMRequest(kind="text_command", text=text, now_iso=now_iso)
    llm_result = route_llm(req)
    if not llm_result.validation_ok or not isinstance(llm_result.payload, dict):
        return _clarify_task_or_timeblock("Уточните, что нужно сделать?", "invalid_llm_payload")
    payload = llm_result.payload
    intent = _canonical_intent_name(str(payload.get("intent") or ""))
    entities = payload.get("entities")
    entities = entities if isinstance(entities, dict) else {}
    if _is_ambiguous_task_or_timeblock(text):
        return _clarify_task_or_timeblock("Что выбрать: создать блок времени или создать задачу?", "ambiguous_intent")
    if intent not in _load_intent_registry():
        return _clarify_task_or_timeblock("Что выбрать: создать блок времени или создать задачу?", "unsupported_intent")
    return InterpretationResult(
        type="command",
        command={"intent": intent, "args": _extract_args(intent, entities)},
        debug={"source_intent": intent},
    )


def route_llm(request: LLMRequest) -> LLMResult:
    if request.kind in COMMAND_KINDS:
        agent = "command_parser"
        system_prompt = _build_command_system_prompt()
        model = os.getenv("OPENROUTER_MODEL_COMMANDS", "openrouter/free")
    elif request.kind == ASSISTANT_KIND:
        agent = "assistant_planner"
        system_prompt = ASSISTANT_SYSTEM_PROMPT
        model = os.getenv("OPENROUTER_MODEL_ASSISTANT", "openrouter/free")
    else:
        return LLMResult(
            agent="unknown",
            provider="",
            model="",
            payload=None,
            raw_text="",
            validation_ok=False,
            error=f"Unsupported kind: {request.kind}",
        )

    provider = OpenRouterProvider()
    user_text = _build_user_text(request.text, request.now_iso)
    messages = [{"role": "system", "content": system_prompt}]
    if agent == "command_parser":
        messages += _load_command_examples()
    messages.append({"role": "user", "content": user_text})

    raw_text = provider.chat(messages, model)
    payload: Optional[dict[str, Any]] = None
    validation_ok = True
    error: Optional[str] = None

    if agent == "command_parser":
        payload, error = _parse_json(raw_text)
        if payload is not None:
            payload = _normalize_command_payload(payload)
            validation_ok, error = _validate_payload(payload)
        else:
            validation_ok = False

        if not validation_ok:
            retry_messages = [
                {"role": "system", "content": STRICT_JSON_PROMPT},
                {"role": "user", "content": user_text},
            ]
            raw_text = provider.chat(retry_messages, model)
            payload, error = _parse_json(raw_text)
            if payload is not None:
                payload = _normalize_command_payload(payload)
                validation_ok, error = _validate_payload(payload)
            else:
                validation_ok = False

        return LLMResult(
            agent=agent,
            provider="openrouter",
            model=model,
            payload=payload,
            raw_text=raw_text,
            validation_ok=validation_ok,
            error=error,
        )

    # assistant_planner branch
    payload, error = _parse_json(raw_text)
    if payload is None:
        retry_messages = [
            {"role": "system", "content": ASSISTANT_STRICT_JSON_PROMPT},
            {"role": "user", "content": user_text},
        ]
        raw_text = provider.chat(retry_messages, model)
        payload, error = _parse_json(raw_text)

    validation_ok = payload is not None
    return LLMResult(
        agent=agent,
        provider="openrouter",
        model=model,
        payload=payload,
        raw_text=raw_text,
        validation_ok=validation_ok,
        error=None if validation_ok else error,
    )


def _cli() -> int:
    parser = argparse.ArgumentParser(description="Minimal LLM router")
    parser.add_argument("--kind", required=True, help="voice_command | text_command | assistant")
    parser.add_argument("--text", required=True, help="Input text")
    parser.add_argument("--now", required=False, help="Current datetime ISO 8601")
    args = parser.parse_args()

    request = LLMRequest(kind=args.kind, text=args.text, now_iso=args.now)
    result = route_llm(request)

    if result.validation_ok:
        if result.payload is not None:
            print(json.dumps(result.payload, ensure_ascii=False, indent=2))
        else:
            print(result.raw_text)
        return 0

    print(result.error or "Validation failed", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(_cli())
