from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import httpx
import yaml
from .types import Choice, CommandEnvelope, InterpretationResult

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
STRICT_JSON_PROMPT = (
    "Return ONLY valid JSON matching the schema. Top-level keys must be exactly: "
    "intent, confidence, text_original, text_normalized, datetime_context, entities, "
    "For entities extraction, include when possible: root_title, parent_title, task_title, "
    "due_datetime, calendar_add. "
    "needs_clarification, clarifying_question, notes. Do NOT include any other "
    "top-level keys (no asr_text, no now_iso, etc.). Do not include any extra text."
)

SUPPORTED_INTENTS = {"task_create", "timeblock_create"}
DEFAULT_ALLOWED_INTENTS = ("task_create", "timeblock_create")
INTENT_CHOICE_LABELS = {
    "timeblock_create": "создать блок времени",
    "task_create": "создать задачу",
}
INTERPRET_FALLBACK_QUESTION = "Не понял команду. Это задача или блок времени?"
INTERPRET_STRICT_JSON_PROMPT = "Верни СТРОГО JSON без комментариев и лишнего текста."
INTERPRET_SYSTEM_PROMPT = """Ты — интерпретатор команд для личного органайзера.
Твоя задача: вернуть СТРОГО JSON без комментариев.

Правила:
- intent может быть только из allowed_intents.
- Если не уверен(а) между 2-3 intent — верни clarify с choices.
- Пустые строки трактуй как null.
- Относительные даты/время (сегодня/завтра/в пятницу/после обеда) РАЗРЕШАЙ в ISO datetime, используя now_iso и timezone.
- Не выдумывай поля. Используй только snake_case.
- Вариант "встреча/созвон/собрание" = timeblock_create.

Жёсткие примеры:
1) "купи молоко" -> {"type":"command","command":{"command":{"intent":"task_create","entities":{"title":"Купить молоко"}}}}
2) "поставь созвон завтра в 10 на 30 минут" -> {"type":"command","command":{"command":{"intent":"timeblock_create","entities":{"start_at":"<iso>","duration_minutes":30}}}}
3) "завтра созвон" -> {"type":"clarify","clarify":{"clarifying_question":"Что выбрать: создать блок времени или создать задачу?","choices":[{"id":"timeblock_create","title":"Создать блок времени"},{"id":"task_create","title":"Создать задачу"}],"draft_envelope":{"command":{"intent":"timeblock_create"}}}}
"""

# Kept for compatibility with existing code/tests; worker may or may not use this mode.
ASSISTANT_SYSTEM_PROMPT = (
    "Ты — assistant_planner. Верни только JSON без markdown. "
    "Не выдумывай факты: используй только переданные данные."
)
ASSISTANT_STRICT_JSON_PROMPT = "Отвечай ТОЛЬКО валидным JSON без Markdown и текста вне JSON."


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
        headers["Authorization"] = f"Bearer {self.api_key}"

        payload: dict[str, Any] = {"model": model, "messages": messages, "temperature": 0.0}
        response = self._client.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]

    def close(self) -> None:
        self._client.close()


def _repo_root() -> Path:
    # organizer-worker/src/organizer_worker/llm/router.py -> repo root
    return Path(__file__).resolve().parents[4]


def _schema_path() -> Path:
    return _repo_root() / "schemas" / "command.schema.json"


def _examples_path() -> Path:
    return _repo_root() / "prompts" / "command_examples.json"


def _canon_path() -> Path:
    return _repo_root() / "canon" / "intents_v2.yml"


def _load_schema() -> dict[str, Any]:
    with _schema_path().open("r", encoding="utf-8") as handle:
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
    with _examples_path().open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError("prompts/command_examples.json must be a list")
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


def _normalize_allowed_intents(allowed_intents: list[str] | None) -> list[str]:
    raw = allowed_intents or list(DEFAULT_ALLOWED_INTENTS)
    out: list[str] = []
    for name in raw:
        normalized = _canonical_intent_name(str(name or ""))
        if normalized in SUPPORTED_INTENTS and normalized not in out:
            out.append(normalized)
    if not out:
        return list(DEFAULT_ALLOWED_INTENTS)
    return out


def build_prompt(
    text: str,
    now_iso: str | None,
    timezone: str | None,
    allowed_intents: list[str] | None,
) -> tuple[str, str]:
    intents = _normalize_allowed_intents(allowed_intents)
    user_prompt = {
        "text": text,
        "now_iso": now_iso,
        "timezone": timezone or "Europe/Moscow",
        "allowed_intents": intents,
        "output_schema": {
            "type": "command|clarify",
            "command": {
                "trace_id": "string",
                "source": {"channel": "string"},
                "command": {
                    "intent": "task_create|timeblock_create",
                    "confidence": 0.0,
                    "entities": {
                        "title": "string|null",
                        "planned_at": "string|null",
                        "start_at": "string|null",
                        "end_at": "string|null",
                        "duration_minutes": "number|null",
                    },
                },
            },
            "clarify": {
                "clarifying_question": "string",
                "choices": [{"id": "string", "title": "string"}],
                "draft_envelope": "command|null",
            },
        },
    }
    return INTERPRET_SYSTEM_PROMPT, json.dumps(user_prompt, ensure_ascii=False)


def _validate_payload(payload: dict[str, Any]) -> tuple[bool, Optional[str]]:
    if not _JSONSCHEMA_AVAILABLE:
        return False, "jsonschema is not installed"
    try:
        jsonschema.validate(instance=payload, schema=_load_schema())
    except ValidationError as exc:
        return False, str(exc)
    return True, None


def _parse_json(text: str) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        return None, str(exc)
    if not isinstance(parsed, dict):
        return None, "Response JSON must be an object"
    return parsed, None


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
    return InterpretationResult.clarify_result(
        clarifying_question=question,
        choices=[
            Choice(
                id="timeblock_create",
                title=INTENT_CHOICE_LABELS["timeblock_create"],
                patch={"command": {"intent": "timeblock_create"}},
            ),
            Choice(
                id="task_create",
                title=INTENT_CHOICE_LABELS["task_create"],
                patch={"command": {"intent": "task_create"}},
            ),
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
        out = {
            "title": title,
            "start_at": start_at,
            "duration_minutes": duration,
        }
        end_at = entities.get("end_at") or entities.get("end_iso")
        if end_at is not None:
            out["end_at"] = end_at
        return out
    return {}


def _nullify_empty(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    if isinstance(value, dict):
        return {k: _nullify_empty(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_nullify_empty(v) for v in value]
    return value


def _clarify_choices(allowed_intents: list[str]) -> list[Choice]:
    out: list[Choice] = []
    for intent in allowed_intents:
        out.append(
            Choice(
                id=intent,
                title=INTENT_CHOICE_LABELS.get(intent, intent),
                patch={"command": {"intent": intent}},
            )
        )
    return out


def _clarify_result(
    *,
    allowed_intents: list[str],
    reason: str,
    question: str = INTERPRET_FALLBACK_QUESTION,
    draft_envelope: CommandEnvelope | None = None,
) -> InterpretationResult:
    return InterpretationResult.clarify_result(
        clarifying_question=question,
        choices=_clarify_choices(allowed_intents),
        draft_envelope=draft_envelope,
        expected_answer="choice_id",
        debug={"reason": reason},
    )


def _build_draft_from_text(text: str, allowed_intents: list[str]) -> CommandEnvelope | None:
    intent = "timeblock_create" if _is_ambiguous_task_or_timeblock(text) else (allowed_intents[0] if allowed_intents else "task_create")
    try:
        return CommandEnvelope.new(intent=intent, entities={"title": text.strip() or None}, source={"channel": "llm_gateway"})
    except Exception:
        return None


def _parse_envelope(raw: Any) -> CommandEnvelope | None:
    if not isinstance(raw, dict):
        return None
    command = raw.get("command")
    if isinstance(command, dict):
        intent = _canonical_intent_name(str(command.get("intent") or ""))
        entities = command.get("entities") if isinstance(command.get("entities"), dict) else {}
        command["intent"] = intent
        command["entities"] = _nullify_empty(entities)
    try:
        return CommandEnvelope.from_dict(raw)
    except Exception:
        return None


def _normalize_interpretation_payload(
    payload: dict[str, Any],
    *,
    text: str,
    allowed_intents: list[str],
) -> InterpretationResult:
    itype = str(payload.get("type") or "").strip().lower()
    if itype == "command":
        envelope = _parse_envelope(payload.get("command"))
        if envelope is None:
            return _clarify_result(allowed_intents=allowed_intents, reason="invalid_command_payload", draft_envelope=_build_draft_from_text(text, allowed_intents))
        intent = _canonical_intent_name(envelope.command.intent)
        if intent not in allowed_intents:
            return _clarify_result(
                allowed_intents=allowed_intents,
                reason="intent_not_allowed",
                draft_envelope=envelope,
            )
        envelope.command.intent = intent
        return InterpretationResult.command_result(envelope=envelope, debug={"source": "llm"})

    if itype == "clarify":
        clarify = payload.get("clarify") if isinstance(payload.get("clarify"), dict) else {}
        question = str(clarify.get("clarifying_question") or payload.get("clarifying_question") or INTERPRET_FALLBACK_QUESTION).strip()
        choices_raw = clarify.get("choices") if isinstance(clarify.get("choices"), list) else payload.get("choices")
        choices: list[Choice] = []
        if isinstance(choices_raw, list):
            for raw in choices_raw:
                if not isinstance(raw, dict):
                    continue
                cid = _canonical_intent_name(str(raw.get("id") or "").strip())
                if cid not in allowed_intents:
                    continue
                title = str(raw.get("title") or INTENT_CHOICE_LABELS.get(cid, cid)).strip()
                choices.append(Choice(id=cid, title=title, patch={"command": {"intent": cid}}))
        if not choices:
            choices = _clarify_choices(allowed_intents)
        draft_raw = clarify.get("draft_envelope") if isinstance(clarify, dict) else None
        draft = _parse_envelope(draft_raw)
        if draft is None:
            draft = _build_draft_from_text(text, allowed_intents)
        return InterpretationResult.clarify_result(
            clarifying_question=question or INTERPRET_FALLBACK_QUESTION,
            choices=choices,
            draft_envelope=draft,
            expected_answer="choice_id",
            debug={"source": "llm"},
        )

    return _clarify_result(allowed_intents=allowed_intents, reason="invalid_type", draft_envelope=_build_draft_from_text(text, allowed_intents))


def _llm_interpret_raw(
    text: str,
    *,
    now_iso: str | None,
    timezone: str | None,
    allowed_intents: list[str],
) -> tuple[dict[str, Any] | None, str | None]:
    model = os.getenv("OPENROUTER_MODEL_COMMANDS", "openrouter/free")
    system_prompt, user_prompt = build_prompt(text, now_iso, timezone, allowed_intents)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    provider = OpenRouterProvider()
    try:
        raw = provider.chat(messages, model)
        payload, err = _parse_json(raw)
        if payload is not None:
            return payload, None
        retry_messages = [
            {"role": "system", "content": INTERPRET_STRICT_JSON_PROMPT},
            {"role": "user", "content": user_prompt},
        ]
        raw_retry = provider.chat(retry_messages, model)
        retry_payload, retry_err = _parse_json(raw_retry)
        return retry_payload, retry_err
    finally:
        provider.close()


def interpret(
    text: str,
    *,
    now_iso: Optional[str] = None,
    timezone: Optional[str] = None,
    allowed_intents: list[str] | None = None,
) -> InterpretationResult:
    intents = _normalize_allowed_intents(allowed_intents)
    payload, parse_err = _llm_interpret_raw(
        text,
        now_iso=now_iso,
        timezone=timezone,
        allowed_intents=intents,
    )
    if not isinstance(payload, dict):
        return _clarify_result(
            allowed_intents=intents,
            reason="json_parse_error",
            question=INTERPRET_FALLBACK_QUESTION,
            draft_envelope=_build_draft_from_text(text, intents),
        )
    result = _normalize_interpretation_payload(payload, text=text, allowed_intents=intents)
    if parse_err:
        dbg = dict(result.debug or {})
        dbg["retry_parse_error"] = parse_err
        result.debug = dbg
    return result


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
