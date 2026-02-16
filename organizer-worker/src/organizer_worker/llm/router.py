from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import httpx

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
    "Do NOT include any other top-level keys (no asr_text, no now_iso, etc.). "
    "No extra text."
)
STRICT_JSON_PROMPT = (
    "Return ONLY valid JSON matching the schema. Top-level keys must be exactly: "
    "intent, confidence, text_original, text_normalized, datetime_context, entities, "
    "needs_clarification, clarifying_question, notes. Do NOT include any other "
    "top-level keys (no asr_text, no now_iso, etc.). Do not include any extra text."
)

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


def _load_schema() -> dict[str, Any]:
    with _schema_path().open("r", encoding="utf-8") as handle:
        return json.load(handle)


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


def route_llm(request: LLMRequest) -> LLMResult:
    if request.kind in COMMAND_KINDS:
        agent = "command_parser"
        system_prompt = COMMAND_SYSTEM_PROMPT
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

