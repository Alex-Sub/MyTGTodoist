from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from functools import lru_cache
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
    "needs_clarification, clarifying_question, notes. Do NOT include any other "
    "top-level keys (no asr_text, no now_iso, etc.). Do not include any extra text."
)
ASSISTANT_STRICT_JSON_PROMPT = (
    "Отвечай ТОЛЬКО валидным JSON. "
    "Не используй Markdown, не используй ```json```. "
    "Не добавляй пояснений или текста вне JSON. "
    "Отвечай ТОЛЬКО на русском языке."
)


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


def _load_schema() -> dict[str, Any]:
    path = _schema_path()
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


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
