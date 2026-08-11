from __future__ import annotations

import json
import logging
import os
import re
import hashlib
import importlib.util
import sys
import types
from datetime import date, datetime, timezone
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional
from zoneinfo import ZoneInfo
from common.date_resolver import extract_date_resolution, normalize_temporal_fields, resolve_date_phrase

import requests

from src.core.failures import USER_MSG_GENERIC_FALLBACK, USER_MSG_REC_EMPTY, USER_MSG_SUCCESS
from src.integrations.telegram.direct_asr_client import GatewayAsrClient
from src.integrations.telegram.schemas import TelegramRuntimeRequest, TelegramRuntimeResult

BridgeHandler = Callable[[TelegramRuntimeRequest], Dict[str, Any]]
_BRIDGE_ERROR_REPLY = "Сервис обработки временно недоступен. Попробуйте еще раз чуть позже."
_MISSING_RUNTIME_RESULT_REPLY = USER_MSG_REC_EMPTY
_SUCCESS_DEFAULT_REPLY = USER_MSG_SUCCESS
_RUNTIME_ERROR_REPLY = USER_MSG_GENERIC_FALLBACK
_VOICE_DOWNLOAD_FAILED_REPLY = (
    "Не удалось обработать аудио. Попробуй отправить голос ещё раз."
)
_FALLBACK_INTENT = "unknown"
_DEFAULT_LOCAL_DB_PATH = "/data/runtime_local.db"
_LOG = logging.getLogger(__name__)
_MEETING_KIND_BY_TOKEN = {
    "встреч": "встреча",
    "собран": "собрание",
    "созвон": "созвон",
    "звонок": "созвон",
    "мероприят": "мероприятие",
    "ивент": "мероприятие",
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
_TASK_CREATE_CONFIRM_FIELD = "task_create_confirm"
_TASK_CREATE_DUPLICATE_CONFIRM_FIELD = "task_create_duplicate_confirm"


def _safe_idempotency_key(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return digest[-8:]

if TYPE_CHECKING:  # pragma: no cover
    from src.core.config import AppConfig
    from src.reliability.local_db import LocalMainDb


def build_runtime_core_direct_handler(
    *,
    cfg: "AppConfig",
    asr_client: Any,
    llm_client: Any,
    rec_client: Any,
    local_db: Optional["LocalMainDb"] = None,
) -> BridgeHandler:
    try:
        from src.app.runtime import handle_user_attempt_runtime
    except ModuleNotFoundError:
        src_root = Path(__file__).resolve().parents[2]
        src_pkg = types.ModuleType("src")
        src_pkg.__path__ = [str(src_root)]  # type: ignore[attr-defined]
        sys.modules["src"] = src_pkg
        runtime_path = src_root / "app" / "runtime.py"
        spec = importlib.util.spec_from_file_location("app_integration_runtime_module", runtime_path)
        if spec is None or spec.loader is None:
            raise
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        handle_user_attempt_runtime = module.handle_user_attempt_runtime

    def _handler(req: TelegramRuntimeRequest) -> Dict[str, Any]:
        metadata = dict(req.metadata or {})
        command_override = _extract_command_from_metadata(metadata)
        runtime_llm_client = llm_client if command_override is None else _StaticLLMClient(command_override)
        return handle_user_attempt_runtime(
            cfg=cfg,
            tenant_id=req.tenant_id,
            request_id=req.request_id,
            user_id=req.user_id,
            channel=req.channel,
            asr_client=asr_client,
            llm_client=runtime_llm_client,
            rec_client=rec_client,
            audio_bytes=req.audio_bytes,
            audio_mime=req.audio_mime,
            audio_filename=req.audio_filename,
            text=req.text,
            metadata=metadata,
            local_db=local_db,
            source_chat_id=req.chat_id,
            source_message_id=req.source_message_id,
        )

    setattr(_handler, "_local_db", local_db)
    return _handler


def build_runtime_core_direct_handler_from_settings(
    *,
    app_id: str,
    app_version: str,
    app_env: str,
    local_db_path: str,
    cloud_buffer_dsn: str,
    app_dict_path: str,
    queue_replay_ttl_seconds: int,
    clarification_ttl_sec: int,
    command_dedup_reservation_ttl_sec: int,
    ml_gateway_url: str,
    timeout_sec: float = 20.0,
    http_post: Optional[Callable[..., Any]] = None,
    direct_voice_enabled: bool = True,
    direct_asr_timeout_sec: Optional[float] = None,
    direct_asr_retries: int = 0,
) -> BridgeHandler:
    from src.core.config import AppConfig
    from src.reliability.local_db import LocalMainDb

    gateway_url = str(ml_gateway_url or "").strip().rstrip("/")
    if not gateway_url:
        raise ValueError("ML_GATEWAY_URL is required for runtime_core_direct")

    resolved_local_db_path = _resolve_local_db_path(local_db_path)
    cfg = AppConfig(
        app_id=str(app_id or "").strip(),
        app_version=str(app_version or "").strip(),
        app_env=str(app_env or "dev").strip() or "dev",
        local_db_path=resolved_local_db_path,
        cloud_buffer_dsn=str(cloud_buffer_dsn or "").strip(),
        global_dict_url="",
        global_dict_path="",
        app_dict_path=str(app_dict_path or "").strip(),
        buffer_cleanup_delivered_days=7,
        buffer_cleanup_keep_last_per_user=200,
        queue_replay_ttl_seconds=int(queue_replay_ttl_seconds),
        clarification_ttl_sec=int(clarification_ttl_sec),
        command_dedup_reservation_ttl_sec=int(command_dedup_reservation_ttl_sec),
    )

    local_db = LocalMainDb(cfg.local_db_path)
    local_db.ensure_schema()
    _LOG.info("telegram_direct_runtime local_db_path=%s", cfg.local_db_path)

    asr_timeout = float(direct_asr_timeout_sec if direct_asr_timeout_sec is not None else timeout_sec)
    asr_client = GatewayAsrClient(
        base_url=gateway_url,
        timeout_sec=asr_timeout,
        retries=int(direct_asr_retries),
        http_post=http_post,
    )
    llm_client = _GatewayChatLlmClient(
        base_url=gateway_url,
        timeout_sec=float(timeout_sec),
        http_post=http_post,
    )
    rec_client = _GatewayRagRecClient(
        base_url=gateway_url,
        timeout_sec=float(timeout_sec),
        http_post=http_post,
    )

    return build_runtime_core_direct_handler(
        cfg=cfg,
        asr_client=asr_client,
        llm_client=llm_client,
        rec_client=rec_client,
        local_db=local_db,
    )


def _resolve_local_db_path(candidate: str) -> str:
    explicit = str(candidate or "").strip()
    if explicit:
        return explicit
    from_env = str(os.getenv("LOCAL_DB_PATH") or "").strip()
    if from_env:
        return from_env
    return _DEFAULT_LOCAL_DB_PATH


def _extract_command_from_metadata(metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for key in ("runtime_command", "command"):
        raw = metadata.get(key)
        if not isinstance(raw, dict):
            continue
        intent = str(raw.get("intent") or "").strip()
        entities = raw.get("entities")
        if not intent or not isinstance(entities, dict):
            continue
        command: Dict[str, Any] = {
            "intent": intent,
            "entities": dict(entities),
        }
        for optional_key in ("confidence", "rejected"):
            if optional_key in raw:
                command[optional_key] = raw.get(optional_key)
        return command
    return None


def _fallback_command_from_text(text: str, *, reason: str = "parse_fallback") -> Dict[str, Any]:
    return {
        "intent": _FALLBACK_INTENT,
        "entities": {"text": str(text or "").strip()},
        "__parse_source": "llm_parse_fallback",
        "__parse_reason": str(reason or "parse_fallback"),
    }


def _extract_json_object(raw: str) -> Optional[Dict[str, Any]]:
    text = str(raw or "").strip()
    if not text:
        return None
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    candidate = text[start : end + 1]
    try:
        parsed = json.loads(candidate)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _extract_json_object_with_reason(raw: str) -> tuple[Optional[Dict[str, Any]], str]:
    text = str(raw or "").strip()
    if not text:
        return None, "empty_response_text"
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None, "no_json_object"
    candidate = text[start : end + 1]
    try:
        parsed = json.loads(candidate)
    except Exception:
        return None, "json_decode_error"
    if not isinstance(parsed, dict):
        return None, "json_not_object"
    return parsed, "ok"


def _extract_chat_response_text(data: Any) -> tuple[str, str]:
    if not isinstance(data, dict):
        return "", "response_not_dict"

    for key in ("response", "content", "text"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            # Keep canonical envelope first to preserve existing behavior.
            return value.strip(), f"envelope:{key}"

    result = data.get("result")
    if isinstance(result, dict):
        for key in ("response", "content", "text"):
            value = result.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip(), f"envelope:result.{key}"

    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0] if isinstance(choices[0], dict) else {}
        message = first.get("message") if isinstance(first.get("message"), dict) else {}
        content = message.get("content")
        if isinstance(content, str) and content.strip():
            return content.strip(), "envelope:choices[0].message.content"
        text = first.get("text")
        if isinstance(text, str) and text.strip():
            return text.strip(), "envelope:choices[0].text"

    return "", "missing_response_field"


def _normalize_command(command_like: Dict[str, Any], *, source_text: str) -> Dict[str, Any]:
    intent = str(command_like.get("intent") or _FALLBACK_INTENT).strip().lower() or _FALLBACK_INTENT
    entities = command_like.get("entities")
    if not isinstance(entities, dict):
        entities = {}
    if "text" not in entities and source_text.strip():
        entities["text"] = source_text.strip()

    normalized: Dict[str, Any] = {"intent": intent, "entities": entities}
    for key in ("confidence", "rejected", "missing_field", "clarifying_question", "needs_clarification"):
        if key in command_like:
            normalized[key] = command_like.get(key)
    for key in ("__parse_source", "__parse_reason"):
        value = command_like.get(key)
        if isinstance(value, str) and value.strip():
            normalized[key] = value.strip()
    return normalized


def _first_non_empty_text(*candidates: Any) -> Optional[str]:
    for candidate in candidates:
        if isinstance(candidate, str):
            text = candidate.strip()
            if text:
                return text
            continue
        if isinstance(candidate, dict):
            nested = _first_non_empty_text(
                candidate.get("message"),
                candidate.get("text"),
                candidate.get("detail"),
                candidate.get("error"),
                candidate.get("question"),
                candidate.get("clarification_question"),
                candidate.get("clarifying_question"),
            )
            if nested:
                return nested
    return None


class _StaticLLMClient:
    def __init__(self, command: Dict[str, Any]) -> None:
        self.command = _normalize_command(command, source_text=str(command.get("text") or ""))

    def parse(self, *, text: str) -> Dict[str, Any]:
        out = dict(self.command)
        entities = out.get("entities")
        if isinstance(entities, dict) and "text" not in entities:
            entities = dict(entities)
            entities["text"] = str(text or "").strip()
            out["entities"] = entities
        return out


class _GatewayChatLlmClient:
    def __init__(
        self,
        *,
        base_url: str,
        timeout_sec: float,
        http_post: Optional[Callable[..., Any]] = None,
    ) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_sec = float(timeout_sec)
        self.http_post = http_post or requests.post

    def parse(self, *, text: str) -> Dict[str, Any]:
        user_text = str(text or "").strip()
        if not user_text:
            return _fallback_command_from_text(user_text, reason="empty_user_text")

        prompt = (
            "Верни только JSON-объект вида "
            '{"intent":"...","entities":{...}} '
            "для пользовательской команды. "
            'Если intent неясен, используй intent="unknown". '
            f"Команда пользователя: {user_text}"
        )
        response = self.http_post(
            f"{self.base_url}/chat",
            json={"prompt": prompt},
            timeout=self.timeout_sec,
        )
        response.raise_for_status()
        data = response.json()
        content, envelope_reason = _extract_chat_response_text(data)
        parsed, parse_reason = _extract_json_object_with_reason(content)
        if parsed is None:
            reason = parse_reason
            if parse_reason == "empty_response_text":
                reason = envelope_reason
            return _fallback_command_from_text(user_text, reason=reason)
        parse_source = "llm_unknown" if str(parsed.get("intent") or "").strip().lower() == _FALLBACK_INTENT else "llm_response"
        parsed["__parse_source"] = parse_source
        parsed["__parse_reason"] = envelope_reason if envelope_reason else "ok"
        return _normalize_command(parsed, source_text=user_text)


class _GatewayRagRecClient:
    def __init__(
        self,
        *,
        base_url: str,
        timeout_sec: float,
        http_post: Optional[Callable[..., Any]] = None,
    ) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_sec = float(timeout_sec)
        self.http_post = http_post or requests.post

    def query(self, body: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(body or {})
        text = str(payload.get("text") or payload.get("query") or "").strip()
        command = payload.get("command") if isinstance(payload.get("command"), dict) else None
        if not text:
            return {"outcome": "rejected", "needs_clarification": False, "hits": []}

        try:
            request_json: Dict[str, Any] = {"query": text}
            if command is not None:
                request_json["command"] = command
            response = self.http_post(
                f"{self.base_url}/rag/query",
                json=request_json,
                timeout=self.timeout_sec,
            )
        except Exception as exc:
            return {"outcome": "rejected", "needs_clarification": False, "hits": [], "error": str(exc)}

        status_code = getattr(response, "status_code", None)
        if isinstance(status_code, int) and status_code >= 400:
            return {"outcome": "rejected", "needs_clarification": False, "hits": [], "status_code": status_code}

        try:
            data = response.json()
        except Exception as exc:
            return {"outcome": "rejected", "needs_clarification": False, "hits": [], "error": str(exc)}

        raw = data.get("raw") if isinstance(data, dict) else None
        result = raw if isinstance(raw, dict) else (data if isinstance(data, dict) else {})
        if not isinstance(result, dict):
            return {"outcome": "rejected", "needs_clarification": False, "hits": []}

        out = dict(result)
        hits = out.get("hits")
        if not isinstance(hits, list):
            hits = []
        out["hits"] = hits

        outcome = str(out.get("outcome") or "").strip().lower()
        status = str(out.get("status") or "").strip().lower()
        answer = str(out.get("answer") or "").strip()
        has_positive_signal = bool(answer) or status == "ok"
        if not outcome:
            outcome = "ok" if (hits or has_positive_signal) else "empty"
        elif outcome == "empty" and has_positive_signal:
            outcome = "ok"
        out["outcome"] = outcome
        out["needs_clarification"] = bool(out.get("needs_clarification")) or outcome == "needs_clarification"
        return out


class RuntimeBridge:
    DIRECT_MODE = "runtime_core_direct"

    def __init__(
        self,
        *,
        mode: Optional[str] = None,
        worker_command_url: Optional[str] = None,
        timeout_sec: float = 20.0,
        http_post: Optional[Callable[..., Any]] = None,
        direct_handler: Optional[BridgeHandler] = None,
        direct_voice_enabled: bool = True,
    ) -> None:
        # Compat execution path retired: bridge is direct-only.
        # Keep worker_command_url: execution still goes through organizer-worker /runtime/command.
        _ = mode
        self.mode = self.DIRECT_MODE
        self.worker_command_url = str(worker_command_url or "").strip().rstrip("/")
        self.timeout_sec = float(timeout_sec)
        self.http_post = http_post or requests.post
        self.direct_handler = direct_handler
        self.direct_voice_enabled = bool(direct_voice_enabled)
        self.direct_local_db = getattr(direct_handler, "_local_db", None) if direct_handler is not None else None

    def _meeting_latest_url(self) -> str:
        url = str(self.worker_command_url or "").strip().rstrip("/")
        if not url:
            return ""
        if url.endswith("/runtime/command"):
            return url[: -len("/runtime/command")] + "/runtime/meeting/latest"
        return url + "/runtime/meeting/latest"

    def _meeting_search_url(self) -> str:
        url = str(self.worker_command_url or "").strip().rstrip("/")
        if not url:
            return ""
        if url.endswith("/runtime/command"):
            return url[: -len("/runtime/command")] + "/runtime/meeting/search"
        return url + "/runtime/meeting/search"

    @staticmethod
    def _extract_meeting_kind(text: str) -> str:
        low = str(text or "").strip().lower().replace("ё", "е")
        if not low:
            return ""
        for token, kind in _MEETING_KIND_BY_TOKEN.items():
            if token in low:
                return kind
        return ""

    @staticmethod
    def _normalize_meeting_title(text: str, meeting_kind: str) -> str:
        raw = str(text or "").strip()
        kind = str(meeting_kind or "").strip().lower()
        if kind not in {"встреча", "собрание", "созвон", "мероприятие"}:
            kind = "встреча"
        if not raw:
            return kind.capitalize()

        s = raw.replace("ё", "е").replace("Ё", "Е")
        s = s.replace("→", " ")
        s = re.sub(r"\s*-\>\s*", " ", s)
        s = re.sub(r"[\"'`]", " ", s)
        s = re.sub(
            r"\b(назначь|запланируй|поставь|сделай|создай|добавь|перенеси|сдвинь|поменяй|измени|нужно|надо|хочу)\b",
            " ",
            s,
            flags=re.IGNORECASE,
        )
        s = re.sub(r"\b(сегодня|завтра|послезавтра)\b", " ", s, flags=re.IGNORECASE)
        s = re.sub(r"\b\d{1,2}[./-]\d{1,2}(?:[./-]\d{4})?\b", " ", s)
        s = re.sub(r"\b(?:в|на|к)\s*\d{1,2}(?::\d{2})?\b", " ", s, flags=re.IGNORECASE)
        s = re.sub(r"\bна\s+\d+\s*(?:мин|минута|минуты|минут|час|часа|часов)\b", " ", s, flags=re.IGNORECASE)
        s = re.sub(r"\s+", " ", s).strip(" ,.-")
        s = re.sub(
            r"(?:\s*(?:\b(?:в|на|к)\s*\d{1,2}(?::\d{2})?\b|\b\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?\b|\b\d{4}-\d{2}-\d{2}\b))+\s*$",
            "",
            s,
            flags=re.IGNORECASE,
        ).strip(" ,.-")
        if not s:
            return kind.capitalize()

        # Normalize leading inflected event nouns and drop duplicated leading kind.
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

        head_match = re.match(r"^(встреча|собрание|созвон|мероприятие)\b", s, flags=re.IGNORECASE)
        if head_match:
            head = str(head_match.group(1) or "").strip().lower()
            if head != kind:
                s = re.sub(
                    r"^(встреча|собрание|созвон|мероприятие)\b",
                    kind,
                    s,
                    count=1,
                    flags=re.IGNORECASE,
                )
            return s[:1].upper() + s[1:]
        return f"{kind.capitalize()} {s}".strip()

    @staticmethod
    def _extract_meeting_target_hint(text: str) -> str:
        s = str(text or "").strip().lower()
        if not s:
            return ""
        cleaned = s
        cleaned = re.sub(r"\b(на|в)\s+\d{1,2}(?::\d{2})?\b", " ", cleaned)
        cleaned = re.sub(r"\bна\s+\d+\s*(?:мин|минут|минута|минуты)\b", " ", cleaned)
        cleaned = re.sub(r"\b\d{1,2}[./-]\d{1,2}(?:[./-]\d{4})?\b", " ", cleaned)
        cleaned = re.sub(r"\b(сегодня|завтра|послезавтра)\b", " ", cleaned)
        cleaned = re.sub(
            r"\b(перенеси|сдвинь|сдвин|поменяй|измени|встречу|встреча|созвон|мероприятие|пожалуйста|давай)\b",
            " ",
            cleaned,
        )
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        cleaned = re.sub(r"^(на|в|с)\s+", "", cleaned).strip()
        return cleaned

    @staticmethod
    def _extract_update_overrides(text: str, *, timezone_name: str) -> Dict[str, Any]:
        s = str(text or "").strip().lower()
        out: Dict[str, Any] = {}
        if not s:
            return out
        m = re.search(r"\bв\s+(\d{1,2})(?:[:.](\d{2}))?\b", s)
        if not m:
            for m_na in re.finditer(r"\bна\s+(\d{1,2})(?:[:.](\d{2}))?\b", s):
                tail = s[m_na.end() :]
                next_word_match = re.match(r"\s+([а-яё]+)", tail)
                next_word = str(next_word_match.group(1) if next_word_match else "").strip().rstrip(".")
                is_month_context = any(next_word.startswith(stem) for stem in _RU_MONTHS.keys())
                if is_month_context:
                    continue
                m = m_na
                break
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2) or "0")
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                out["start_at_time"] = f"{hh:02d}:{mm:02d}"

        m = re.search(r"\bна\s+(\d+)\s*(?:мин|минут|минута|минуты)\b", s)
        if m:
            try:
                val = int(m.group(1))
                if val > 0:
                    out["duration_minutes"] = val
            except Exception:
                pass

        explicit_date = RuntimeBridge._extract_explicit_date_from_text(s)
        if explicit_date:
            out["start_at_date"] = explicit_date
            _LOG.info(
                "temporal_date_parsed",
                extra={
                    "raw_date_text": s,
                    "normalized_date_result": explicit_date,
                    "parser_source": "RuntimeBridge._extract_update_overrides",
                },
            )
        return out

    @staticmethod
    def _build_start_at_from_parts(*, date_value: str, time_value: str, timezone_name: str) -> str:
        date_ymd = RuntimeBridge._normalize_date_ymd(date_value)
        time_hhmm = RuntimeBridge._normalize_time_hhmm(time_value)
        if not (date_ymd and time_hhmm):
            return ""
        try:
            tz = ZoneInfo(str(timezone_name or "UTC"))
        except Exception:
            tz = timezone.utc
        return datetime.fromisoformat(f"{date_ymd}T{time_hhmm}").replace(tzinfo=tz).isoformat()

    @staticmethod
    def _is_meeting_update_text(text: str) -> bool:
        s = str(text or "").strip()
        if not s:
            return False
        low = s.lower()
        has_update_verb = any(token in low for token in ("перенеси", "сдвин", "поменяй", "измени"))
        has_create_verb = any(token in low for token in ("назнач", "заплан", "созда", "постав"))
        if has_update_verb and not has_create_verb:
            return True
        try:
            from src.decision.decision_engine import match_alias
            alias_update = bool(match_alias(s, "meeting.update"))
            alias_create = bool(match_alias(s, "meeting.create"))
            if alias_update and not alias_create:
                return True
            if has_update_verb:
                return True
            return False
        except Exception:
            return has_update_verb

    def _meeting_update_source_error_result(self, req: TelegramRuntimeRequest) -> TelegramRuntimeResult:
        return TelegramRuntimeResult(
            outcome="rejected",
            reply_text="Не нашёл встречу для переноса",
            telemetry={"mode": self.mode, "request_id": req.request_id, "meeting_update_source_found": False},
        )

    def _meeting_update_multiple_candidates_result(self, req: TelegramRuntimeRequest) -> TelegramRuntimeResult:
        return TelegramRuntimeResult(
            outcome="rejected",
            reply_text="Нашёл несколько встреч. Уточните, какую именно встречу нужно перенести.",
            telemetry={"mode": self.mode, "request_id": req.request_id, "meeting_update_source_found": False},
        )

    def _meeting_update_draft_is_empty(self, draft: Dict[str, Any]) -> bool:
        d = draft if isinstance(draft, dict) else {}
        date_v = str(d.get("start_at_date") or "").strip()
        time_v = str(d.get("start_at_time") or "").strip()
        dur_v = str(d.get("duration_minutes") or "").strip()
        return not (date_v and time_v and dur_v)

    def _maybe_enrich_request_for_meeting_update(
        self, req: TelegramRuntimeRequest
    ) -> tuple[TelegramRuntimeRequest, Optional[TelegramRuntimeResult]]:
        text = str(req.text or "").strip()
        if not text or not self.worker_command_url:
            return req, None
        if not self._is_meeting_update_text(text):
            return req, None
        search_url = self._meeting_search_url()
        if not search_url:
            return req, self._meeting_update_source_error_result(req)
        target_hint = self._extract_meeting_target_hint(text)
        meeting_kind = self._extract_meeting_kind(text)
        overrides = self._extract_update_overrides(text, timezone_name=str(req.timezone or "UTC"))
        has_changes = bool(overrides)
        try:
            search_payload = {"user_id": str(req.user_id or "").strip(), "target_hint": target_hint}
            if meeting_kind:
                search_payload["meeting_kind"] = meeting_kind
            source_resp = self.http_post(
                search_url,
                json=search_payload,
                timeout=self.timeout_sec,
            )
            source_payload = source_resp.json() if hasattr(source_resp, "json") else {}
        except Exception:
            _LOG.info(
                "meeting_update_source_found",
                extra={
                    "request_id": req.request_id,
                    "flow_id": req.request_id,
                    "meeting_update_source_found": False,
                    "source_event_id": "",
                },
            )
            return req, self._meeting_update_source_error_result(req)
        source_payload = source_payload if isinstance(source_payload, dict) else {}
        if not bool(source_payload.get("ok")):
            reason = str(source_payload.get("reason") or "").strip().lower()
            _LOG.info(
                "meeting_update_source_found",
                extra={
                    "request_id": req.request_id,
                    "flow_id": req.request_id,
                    "meeting_update_source_found": False,
                    "reason": reason,
                    "source_event_id": str(source_payload.get("calendar_event_id") or ""),
                },
            )
            if reason == "sync_conflict" and isinstance(source_payload.get("conflict"), dict):
                metadata = dict(req.metadata or {})
                metadata["runtime_command"] = {
                    "intent": "meeting.update",
                    "entities": {
                        "user_id": str(req.user_id or "").strip(),
                        "text": text,
                        "__sync_conflict": dict(source_payload.get("conflict")),
                    },
                    "confidence": 0.95,
                    "rejected": False,
                }
                return replace(req, metadata=metadata), None
            if reason == "multiple":
                candidates_raw = source_payload.get("candidates")
                candidates_raw = candidates_raw if isinstance(candidates_raw, list) else []
                candidates: list[Dict[str, Any]] = []
                for item in candidates_raw:
                    if not isinstance(item, dict):
                        continue
                    c: Dict[str, Any] = {
                        "calendar_event_id": str(item.get("calendar_event_id") or "").strip(),
                        "title": str(item.get("title") or "Встреча").strip() or "Встреча",
                        "description": str(item.get("description") or "").strip(),
                        "meeting_kind": str(item.get("meeting_kind") or "").strip().lower(),
                        "start_at_date": str(item.get("start_at_date") or "").strip(),
                        "start_at_time": str(item.get("start_at_time") or "").strip(),
                        "duration_minutes": item.get("duration_minutes"),
                    }
                    candidates.append(c)
                entities_multiple: Dict[str, Any] = {
                    "user_id": str(req.user_id or "").strip(),
                    "text": text,
                    "meeting_kind": meeting_kind,
                    "__meeting_update_target_hint": target_hint,
                    "__meeting_update_candidates": candidates[:5],
                }
                if overrides.get("start_at_date"):
                    entities_multiple["meeting_update_new_date"] = str(overrides.get("start_at_date"))
                    entities_multiple["start_at_date"] = str(overrides.get("start_at_date"))
                if overrides.get("start_at_time"):
                    entities_multiple["meeting_update_new_time"] = str(overrides.get("start_at_time"))
                    entities_multiple["start_at_time"] = str(overrides.get("start_at_time"))
                if overrides.get("duration_minutes") is not None:
                    entities_multiple["meeting_update_new_duration"] = overrides.get("duration_minutes")
                    entities_multiple["duration_minutes"] = overrides.get("duration_minutes")
                entities_multiple["meeting_update_has_changes"] = has_changes
                metadata = dict(req.metadata or {})
                metadata["runtime_command"] = {
                    "intent": "meeting.update",
                    "entities": entities_multiple,
                    "confidence": 0.95,
                    "rejected": False,
                }
                return replace(req, metadata=metadata), None
            return req, self._meeting_update_source_error_result(req)
        source_candidate = source_payload.get("candidate")
        source_candidate = source_candidate if isinstance(source_candidate, dict) else source_payload
        source_event_id = str(source_candidate.get("calendar_event_id") or "").strip()
        source_date = str(source_candidate.get("start_at_date") or "").strip()
        source_time = str(source_candidate.get("start_at_time") or "").strip()
        source_duration = source_candidate.get("duration_minutes")
        source_title = str(source_candidate.get("title") or "").strip()
        source_kind = (
            meeting_kind
            or str(source_candidate.get("meeting_kind") or "").strip().lower()
            or self._extract_meeting_kind(source_title)
        )
        _LOG.info(
            "meeting_update_source_found",
            extra={
                "request_id": req.request_id,
                "flow_id": req.request_id,
                "meeting_update_source_found": True,
                "target_hint": target_hint,
                "source_event_id": source_event_id,
            },
        )
        _LOG.info(
            "meeting_update_source_loaded",
            extra={
                "request_id": req.request_id,
                "flow_id": req.request_id,
                "source_event_id": source_event_id,
                "source_title": source_title,
                "source_date": source_date,
                "source_time": source_time,
                "source_duration": source_duration,
            },
        )
        hydrated: Dict[str, Any] = {
            "calendar_event_id": source_event_id,
            "start_at_date": source_date,
            "start_at_time": source_time,
            "duration_minutes": source_duration,
            "meeting_kind": source_kind,
            "meeting_update_source_event_id": source_event_id,
            "meeting_update_source_date": source_date,
            "meeting_update_source_time": source_time,
            "meeting_update_source_duration": source_duration,
            "meeting_update_source_title": source_title,
            "user_id": str(req.user_id or "").strip(),
            "text": text,
        }
        _LOG.info(
            "draft_after_hydration",
            extra={
                "request_id": req.request_id,
                "flow_id": req.request_id,
                "source_event_id": source_event_id,
                "draft_date": str(hydrated.get("start_at_date") or ""),
                "draft_time": str(hydrated.get("start_at_time") or ""),
                "draft_duration": hydrated.get("duration_minutes"),
            },
        )
        if self._meeting_update_draft_is_empty(hydrated):
            return req, self._meeting_update_source_error_result(req)

        merged: Dict[str, Any] = dict(hydrated)
        if overrides.get("start_at_date"):
            merged["meeting_update_new_date"] = str(overrides.get("start_at_date"))
        if overrides.get("start_at_time"):
            merged["meeting_update_new_time"] = str(overrides.get("start_at_time"))
        if overrides.get("duration_minutes") is not None:
            merged["meeting_update_new_duration"] = overrides.get("duration_minutes")
        merged.update({k: v for k, v in overrides.items() if v is not None and str(v).strip() != ""})
        merged["meeting_update_has_changes"] = has_changes
        merged_start_at = self._build_start_at_from_parts(
            date_value=str(merged.get("start_at_date") or ""),
            time_value=str(merged.get("start_at_time") or ""),
            timezone_name=str(req.timezone or "UTC"),
        )
        if merged_start_at:
            merged["start_at"] = merged_start_at
        _LOG.info(
            "meeting_update_draft_after_merge",
            extra={
                "request_id": req.request_id,
                "flow_id": req.request_id,
                "source_event_id": source_event_id,
                "source_date": source_date,
                "source_time": source_time,
                "source_duration": source_duration,
                "draft_date": str(merged.get("start_at_date") or ""),
                "draft_time": str(merged.get("start_at_time") or ""),
                "draft_duration": merged.get("duration_minutes"),
                "has_changes": has_changes,
            },
        )
        _LOG.info(
            "draft_after_merge",
            extra={
                "request_id": req.request_id,
                "flow_id": req.request_id,
                "source_event_id": source_event_id,
                "draft_date": str(merged.get("start_at_date") or ""),
                "draft_time": str(merged.get("start_at_time") or ""),
                "draft_duration": merged.get("duration_minutes"),
                "has_changes": has_changes,
            },
        )
        if self._meeting_update_draft_is_empty(merged):
            return req, self._meeting_update_source_error_result(req)
        metadata = dict(req.metadata or {})
        metadata["runtime_command"] = {
            "intent": "meeting.update",
            "entities": merged,
            "confidence": 0.95,
            "rejected": False,
        }
        return replace(req, metadata=metadata), None

    @staticmethod
    def _is_execution_candidate(payload: Dict[str, Any]) -> bool:
        if not isinstance(payload, dict):
            return False
        outcome = str(payload.get("outcome") or "").strip().lower()
        if outcome not in {"success", "ok"}:
            return False
        if bool(payload.get("needs_clarification")):
            return False
        rec = payload.get("rec") if isinstance(payload.get("rec"), dict) else {}
        # Execute only for explicit runtime executable-guard success path.
        if str(rec.get("outcome") or "").strip().lower() != "skipped":
            return False
        if str(rec.get("reason") or "").strip().lower() != "clarification_executable_guard":
            return False
        command = payload.get("command")
        if not isinstance(command, dict):
            return False
        intent = str(command.get("intent") or "").strip()
        return bool(intent)

    @staticmethod
    def _entities_from_command(command: Dict[str, Any]) -> Dict[str, Any]:
        entities = command.get("entities")
        out: Dict[str, Any]
        if isinstance(entities, dict):
            out = dict(entities)
        else:
            out = {}
        for key, value in command.items():
            if key in {"intent", "confidence", "rejected", "entities"}:
                continue
            if str(key).startswith("__"):
                continue
            k = str(key)
            if k not in out:
                out[k] = value
        return out

    @staticmethod
    def _command_value(command: Dict[str, Any], *keys: str) -> Any:
        entities = RuntimeBridge._entities_from_command(command)
        for key in keys:
            if key in command and command.get(key) is not None:
                return command.get(key)
            if key in entities and entities.get(key) is not None:
                return entities.get(key)
        return None

    def _task_duplicate_check_url(self) -> str:
        base = str(self.worker_command_url or "").strip().rstrip("/")
        if not base:
            return ""
        if base.endswith("/runtime/command"):
            return base[: -len("/runtime/command")] + "/runtime/task/check_duplicate_create"
        return base + "/runtime/task/check_duplicate_create"

    @staticmethod
    def _parse_human_date_to_iso(raw_text: Any, *, parser_source: str = "RuntimeBridge._parse_human_date_to_iso") -> str:
        resolution = resolve_date_phrase(str(raw_text or ""), now=datetime.combine(date.today(), datetime.min.time()))
        _LOG.info(
            "temporal_date_parsed",
            extra={
                "raw_date_text": str(raw_text or "").strip(),
                "normalized_date_result": str(resolution.date or ""),
                "parser_source": parser_source,
                "resolver_reason": str(resolution.reason or ""),
                "needs_clarification": bool(resolution.needs_clarification),
            },
        )
        return str(resolution.date or "")

    @staticmethod
    def _extract_explicit_date_from_text(raw_text: Any) -> str:
        resolution = extract_date_resolution(raw_text, now=datetime.combine(date.today(), datetime.min.time()))
        return str(resolution.date or "")

    @staticmethod
    def _normalize_time_hhmm(value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        if re.fullmatch(r"\d{1,2}", raw):
            hour = int(raw)
            if 0 <= hour <= 23:
                return f"{hour:02d}:00"
            return ""
        m = re.fullmatch(r"(\d{1,2}):(\d{2})", raw)
        if not m:
            return ""
        hour = int(m.group(1))
        minute = int(m.group(2))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return ""
        return f"{hour:02d}:{minute:02d}"

    @staticmethod
    def _normalize_date_ymd(value: Any) -> str:
        return RuntimeBridge._parse_human_date_to_iso(
            value,
            parser_source="RuntimeBridge._normalize_date_ymd",
        )

    @staticmethod
    def _normalize_temporal_entities(
        entities: Dict[str, Any],
        *,
        timezone_name: str,
        command: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        out = dict(entities if isinstance(entities, dict) else {})
        cmd = command if isinstance(command, dict) else {}
        temporal_draft = cmd.get("__temporal_draft")
        temporal_draft = temporal_draft if isinstance(temporal_draft, dict) else {}
        prefer_draft = bool(cmd.get("__edited_temporal_draft"))
        date_raw = out.get("start_at_date")
        if not str(date_raw or "").strip():
            date_raw = out.get("date")
        if prefer_draft or not str(date_raw or "").strip():
            date_raw = temporal_draft.get("date")
        time_raw = out.get("start_at_time")
        if not str(time_raw or "").strip():
            time_raw = out.get("start_time")
        if not str(time_raw or "").strip():
            time_raw = out.get("time")
        if prefer_draft or not str(time_raw or "").strip():
            time_raw = temporal_draft.get("time")
        if (prefer_draft or out.get("duration_minutes") is None) and temporal_draft.get("duration_minutes") is not None:
            out["duration_minutes"] = temporal_draft.get("duration_minutes")
        source_text = str(
            out.get("text")
            or cmd.get("__source_text")
            or temporal_draft.get("text")
            or ""
        ).strip()
        explicit_date_from_text = RuntimeBridge._extract_explicit_date_from_text(source_text)
        if explicit_date_from_text and not prefer_draft:
            date_raw = explicit_date_from_text

        date_ymd = RuntimeBridge._normalize_date_ymd(date_raw)
        time_hhmm = RuntimeBridge._normalize_time_hhmm(time_raw)
        if date_ymd:
            out["start_at_date"] = date_ymd
        if time_hhmm:
            out["start_at_time"] = time_hhmm
            out["start_time"] = time_hhmm

        start_at_raw = str(out.get("start_at") or "").strip()
        if not start_at_raw:
            start_at_raw = str(temporal_draft.get("start_at") or "").strip()
            if start_at_raw:
                out["start_at"] = start_at_raw
        if not start_at_raw and date_ymd and time_hhmm:
            tz_name = str(timezone_name or "").strip() or "UTC"
            try:
                tz = ZoneInfo(tz_name)
            except Exception:
                tz = timezone.utc
            try:
                dt = datetime.fromisoformat(f"{date_ymd}T{time_hhmm}").replace(tzinfo=tz)
                out["start_at"] = dt.isoformat()
            except Exception:
                out["start_at"] = f"{date_ymd}T{time_hhmm}"
        normalized = normalize_temporal_fields({"entities": out}, now=datetime.combine(date.today(), datetime.min.time()))
        entities_normalized = normalized.get("entities")
        return dict(entities_normalized) if isinstance(entities_normalized, dict) else out

    @staticmethod
    def _is_calendar_commit_ok(payload: Dict[str, Any]) -> bool:
        debug = payload.get("debug") if isinstance(payload.get("debug"), dict) else {}
        commit_state = str(debug.get("calendar_commit") or "").strip().lower()
        event_id = str(payload.get("calendar_event_id") or "").strip()
        return commit_state == "ok" and bool(event_id)

    @staticmethod
    def _is_temporal_intent(intent: str) -> bool:
        normalized = str(intent or "").strip().lower()
        return normalized in {
            "timeblock.create",
            "timeblock_create",
            "create_timeblock",
            "schedule_meeting",
            "schedule_call",
            "schedule_block",
            "meeting.create",
            "meeting.update",
            "meeting_update",
            "reschedule_meeting",
            "timeblock.update",
            "timeblock_update",
            "timeblock.move",
            "move_timeblock",
            "block.create",
        }

    @staticmethod
    def _canonical_worker_intent(intent: str) -> str:
        normalized = str(intent or "").strip().lower()
        if normalized in {
            "meeting.update",
            "meeting_update",
            "reschedule_meeting",
            "move_meeting",
        }:
            return "meeting.update"
        if normalized in {
            "meeting.create",
            "meeting_create",
            "schedule_meeting",
            "schedule_call",
            "create_meeting",
            "create_event",
        }:
            return "meeting.create"
        if normalized in {
            "timeblock.create",
            "timeblock_create",
            "create_timeblock",
            "schedule_block",
            "block.create",
        }:
            return "timeblock.create"
        if normalized in {
            "timeblock.update",
            "timeblock_update",
            "timeblock.move",
            "move_timeblock",
            "update_timeblock",
            "block.update",
        }:
            return "timeblock.update"
        if normalized in {"meeting.comment.update", "meeting_comment_update"}:
            return "meeting.comment.update"
        if normalized in {"task.comment.update", "task_comment_update"}:
            return "task.comment.update"
        return str(intent or "").strip()

    @staticmethod
    def _log_temporal_commit_dispatch(
        req: TelegramRuntimeRequest,
        *,
        raw_intent: str,
        normalized_intent: str,
        entities: Dict[str, Any],
        raw_user_input: str,
    ) -> None:
        entities = dict(entities if isinstance(entities, dict) else {})
        _LOG.info(
            "temporal_commit_dispatch",
            extra={
                "request_id": req.request_id,
                "flow_id": req.request_id,
                "raw_intent": raw_intent,
                "normalized_intent": normalized_intent,
                "payload_keys": sorted(str(k) for k in entities.keys()),
                "raw_user_input": str(raw_user_input or entities.get("text") or req.text or ""),
                "start_at_date": entities.get("start_at_date") or entities.get("date"),
                "start_time": entities.get("start_at_time") or entities.get("start_time") or entities.get("time"),
                "start_at": entities.get("start_at"),
                "duration_minutes": entities.get("duration_minutes") or entities.get("duration_min"),
                "timezone": req.timezone,
            },
        )

    def _build_runtime_command_envelope(self, req: TelegramRuntimeRequest, payload: Dict[str, Any]) -> Dict[str, Any]:
        command = payload.get("command") if isinstance(payload.get("command"), dict) else {}
        intent = self._canonical_worker_intent(str(command.get("intent") or ""))
        entities = self._entities_from_command(command)
        user_id = str(req.user_id or "").strip()
        if user_id and not str(entities.get("user_id") or "").strip():
            entities["user_id"] = user_id
        normalized_command = normalize_temporal_fields(
            {"intent": intent, "entities": entities, "text": entities.get("text") or req.text or ""},
            now=datetime.combine(date.today(), datetime.min.time()),
        )
        entities = dict(normalized_command.get("entities") or entities)
        if self._is_temporal_intent(intent):
            entities = self._normalize_temporal_entities(
                entities,
                timezone_name=str(req.timezone or "UTC"),
                command=command,
            )
        if intent == "task.create":
            raw_due_date = str(command.get("due_date") or entities.get("due_date") or entities.get("date") or entities.get("when") or "").strip()
            raw_planned_at = str(command.get("planned_at") or entities.get("planned_at") or "").strip()
            resolved_due_date = str(entities.get("due_date") or "").strip()
            resolved_planned_at = str(entities.get("planned_at") or "").strip()
            if resolved_planned_at:
                entities["planned_at"] = resolved_planned_at
            else:
                entities["planned_at"] = None
            if resolved_due_date:
                entities["due_date"] = resolved_due_date
            _LOG.info(
                "task_create_date_resolution",
                extra={
                    "request_id": req.request_id,
                    "flow_id": req.request_id,
                    "source": "runtime_bridge",
                    "intent": intent,
                    "raw_text": str(entities.get("text") or req.text or ""),
                    "raw_due_date": raw_due_date,
                    "extracted_date": str(resolved_due_date or ""),
                    "resolved_planned_at": str(resolved_planned_at or ""),
                    "planned_at": str(entities.get("planned_at") or ""),
                    "summary_planned_at": str(entities.get("planned_at") or ""),
                    "persisted_planned_at": "",
                },
            )
        if intent in {"meeting.create", "meeting.update"}:
            raw_intent = str(command.get("intent") or "").strip().lower()
            meeting_kind_locked = bool(command.get("__meeting_kind_locked"))
            source_text = str(
                entities.get("text")
                or command.get("__source_text")
                or req.text
                or ""
            ).strip()
            meeting_kind = str(entities.get("meeting_kind") or entities.get("event_kind") or "").strip().lower()
            if not meeting_kind and intent == "meeting.update":
                meeting_kind = self._extract_meeting_kind(source_text)
            if not meeting_kind and raw_intent == "schedule_call":
                meeting_kind = "созвон"
            if not meeting_kind and raw_intent in {"schedule_meeting", "create_meeting", "meeting_create"}:
                meeting_kind = "встреча"
            if not meeting_kind and not meeting_kind_locked:
                meeting_kind = self._extract_meeting_kind(source_text)
            if meeting_kind:
                entities["meeting_kind"] = meeting_kind
            explicit_title = str(entities.get("title") or "").strip()
            entities["title"] = self._normalize_meeting_title(explicit_title or source_text, meeting_kind)
        source_msg_id = str(entities.get("source_msg_id") or "").strip()
        if not source_msg_id and req.chat_id and req.source_message_id:
            entities["source_msg_id"] = f"tg:{req.chat_id}:{req.source_message_id}"
        return {
            "trace_id": str(req.request_id or "").strip(),
            "flow_id": str(req.request_id or "").strip(),
            "source": {
                "channel": str(req.channel or "").strip(),
                "adapter": "telegram_app_integration",
                "chat_id": str(req.chat_id or "").strip(),
                "message_id": str(req.source_message_id or "").strip(),
                "user_id": str(req.user_id or "").strip(),
                "timezone": str(req.timezone or "").strip(),
                "flow_id": str(req.request_id or "").strip(),
            },
            "idempotency_key": str(req.request_id or "").strip(),
            "command": {
                "intent": intent,
                "user_id": user_id,
                "entities": entities,
            },
        }

    def _execute_runtime_command(
        self,
        req: TelegramRuntimeRequest,
        payload: Dict[str, Any],
        *,
        envelope: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not self.worker_command_url:
            return {
                "outcome": "bridge_error",
                "error": "runtime command backend is not configured",
            }
        dispatch_envelope = envelope if isinstance(envelope, dict) else self._build_runtime_command_envelope(req, payload)
        try:
            response = self.http_post(
                self.worker_command_url,
                json=dispatch_envelope,
                timeout=self.timeout_sec,
            )
        except Exception as exc:
            return {
                "outcome": "bridge_error",
                "error": str(exc),
            }
        status_code = getattr(response, "status_code", None)
        try:
            body = response.json()
        except Exception:
            body = {}
        if isinstance(status_code, int) and status_code >= 400:
            error_text = _first_non_empty_text(body, getattr(response, "text", None)) or f"runtime_http_{status_code}"
            return {
                "outcome": "bridge_error",
                "error": error_text,
                "status_code": status_code,
            }
        return body if isinstance(body, dict) else {"outcome": "direct_invalid_payload"}

    @staticmethod
    def _clarification_context_key(req: TelegramRuntimeRequest) -> str:
        chat_id = str(req.chat_id or req.user_id or "").strip()
        return (
            f"{req.app_id or 'default_app'}::{req.tenant_id or 'default_tenant'}::"
            f"{req.channel or 'unknown'}::{chat_id}::{req.user_id or ''}"
        )

    @staticmethod
    def _task_create_duplicate_question(existing_task: Dict[str, Any]) -> str:
        duplicate_date = str(existing_task.get("planned_at") or "").strip()
        duplicate_parent = str(existing_task.get("parent_task_id") or "").strip()
        if duplicate_date:
            lead_text = f"Похоже, такая задача уже есть на {duplicate_date[:10]}"
        elif duplicate_parent:
            lead_text = "Такая задача уже есть без срока"
        else:
            lead_text = "Такая задача уже есть в InBox"
        return (
            f"{lead_text}:\n"
            f"№ {str(existing_task.get('id') or '').strip() or '-'} — {str(existing_task.get('title') or '').strip() or '-'}\n\n"
            "Создать ещё одну?"
        )

    def _precheck_task_create_duplicate_confirmation(
        self,
        req: TelegramRuntimeRequest,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not self.worker_command_url:
            return payload
        rec = payload.get("rec") if isinstance(payload.get("rec"), dict) else {}
        missing_field = str(rec.get("missing_field") or payload.get("missing_field") or "").strip()
        if missing_field != _TASK_CREATE_CONFIRM_FIELD:
            return payload
        command = payload.get("command") if isinstance(payload.get("command"), dict) else {}
        if str(command.get("intent") or "").strip().lower() != "task.create":
            return payload
        if bool(command.get("__task_create_confirmed")):
            return payload

        title = str(self._command_value(command, "title", "task_title", "text") or "").strip()
        planned_at = self._command_value(command, "planned_at", "due_date", "due_at", "date", "when")
        planned_at_text = str(planned_at or "").strip()
        if not str(req.user_id or "").strip() or not title:
            return payload
        parent_task_explicit = bool(self._command_value(command, "parent_task_explicit"))
        parent_task_id: int | None = None
        if parent_task_explicit:
            raw_parent_task_id = self._command_value(command, "parent_task_id")
            if str(raw_parent_task_id or "").strip():
                try:
                    parent_task_id = int(raw_parent_task_id)
                except Exception:
                    return payload
        try:
            response = self.http_post(
                self._task_duplicate_check_url(),
                json={
                    "user_id": str(req.user_id or "").strip(),
                    "title": title,
                    "planned_at": (planned_at_text or None),
                    "parent_task_id": parent_task_id,
                },
                timeout=self.timeout_sec,
            )
        except Exception:
            return payload
        status_code = getattr(response, "status_code", None)
        try:
            body = response.json()
        except Exception:
            body = {}
        if not isinstance(body, dict) or (isinstance(status_code, int) and status_code >= 400):
            return payload
        existing_task = body.get("existing_task") if isinstance(body.get("existing_task"), dict) else {}
        if not bool(body.get("duplicate_found")) or not existing_task:
            return payload
        wrapped_response = {
            "clarifying_question": self._task_create_duplicate_question(existing_task),
            "user_message": self._task_create_duplicate_question(existing_task),
            "debug": {
                "duplicate_found": True,
                "existing_task": existing_task,
            },
        }
        return self._wrap_task_create_duplicate_confirmation(req, payload, wrapped_response)

    def _wrap_task_create_duplicate_confirmation(
        self,
        req: TelegramRuntimeRequest,
        payload: Dict[str, Any],
        worker_response: Dict[str, Any],
    ) -> Dict[str, Any]:
        question = _first_non_empty_text(
            worker_response.get("clarifying_question"),
            worker_response.get("user_message"),
        ) or "Похоже, такая задача уже есть. Создать ещё одну?"
        command = payload.get("command") if isinstance(payload.get("command"), dict) else {}
        command = dict(command)
        entities = command.get("entities")
        entities = dict(entities) if isinstance(entities, dict) else {}
        entities["duplicate_check_override"] = True
        command["entities"] = entities
        command["duplicate_check_override"] = True
        debug = worker_response.get("debug") if isinstance(worker_response.get("debug"), dict) else {}
        existing_task = debug.get("existing_task") if isinstance(debug.get("existing_task"), dict) else {}
        if existing_task:
            command["__task_create_duplicate_existing_task"] = dict(existing_task)

        local_db = self.direct_local_db
        if local_db is not None:
            local_db.upsert_clarification_session(
                context_key=self._clarification_context_key(req),
                app_id=str(req.app_id or ""),
                tenant_id=str(req.tenant_id or ""),
                user_id=str(req.user_id or ""),
                intent=str(command.get("intent") or "task.create"),
                payload=command,
                missing_field=_TASK_CREATE_DUPLICATE_CONFIRM_FIELD,
                source_message_id=str(req.source_message_id or "") or None,
                idempotency_key=str(req.request_id or "") or None,
            )

        return {
            "outcome": "rec_needs_clarification",
            "needs_clarification": True,
            "clarifying_question": question,
            "user_message": question,
            "rec": {
                "outcome": "needs_clarification",
                "needs_clarification": True,
                "missing_field": _TASK_CREATE_DUPLICATE_CONFIRM_FIELD,
            },
            "command": command,
        }

    def _commit_temporal(self, req: TelegramRuntimeRequest, payload: Dict[str, Any]) -> Dict[str, Any]:
        command = payload.get("command") if isinstance(payload.get("command"), dict) else {}
        raw_intent = str(command.get("intent") or "").strip()
        normalized_intent = self._canonical_worker_intent(raw_intent)
        envelope = self._build_runtime_command_envelope(req, payload)
        env_command = envelope.get("command") if isinstance(envelope.get("command"), dict) else {}
        env_entities = env_command.get("entities") if isinstance(env_command.get("entities"), dict) else {}
        _LOG.info(
            "calendar_commit_started",
            extra={
                "request_id": req.request_id,
                "flow_id": req.request_id,
                "raw_intent": raw_intent,
                "normalized_intent": normalized_intent,
            },
        )
        self._log_temporal_commit_dispatch(
            req,
            raw_intent=raw_intent,
            normalized_intent=normalized_intent,
            entities=env_entities,
            raw_user_input=str(command.get("__source_text") or ""),
        )
        response = self._execute_runtime_command(req, payload, envelope=envelope)
        response = dict(response) if isinstance(response, dict) else {"outcome": "bridge_error", "error": "empty_response"}
        debug = response.get("debug") if isinstance(response.get("debug"), dict) else {}
        calendar_commit = str(debug.get("calendar_commit") or "").strip().lower()
        calendar_event_id = str(response.get("calendar_event_id") or "").strip()
        _LOG.info(
            "calendar_commit_result",
            extra={
                "request_id": req.request_id,
                "flow_id": req.request_id,
                "raw_intent": raw_intent,
                "normalized_intent": normalized_intent,
                "calendar_commit_result": calendar_commit or "unknown",
                "calendar_event_id": calendar_event_id,
                "ok": bool(response.get("ok")),
            },
        )
        if normalized_intent in {"meeting.create", "meeting.update"} and bool(response.get("ok")) and not self._is_calendar_commit_ok(response):
            response["outcome"] = "error"
            response["ok"] = False
            response["user_message"] = (
                "Не получилось обновить встречу в календаре. Попробуйте еще раз."
                if normalized_intent == "meeting.update"
                else "Не получилось создать встречу в календаре. Попробуйте еще раз."
            )
            if not response.get("error"):
                response["error"] = "calendar_commit_failed"
        if normalized_intent in {"meeting.create", "meeting.update"} and not bool(response.get("ok")):
            current_msg = str(response.get("user_message") or "").strip().lower().replace("ё", "е")
            has_success_phrase = any(
                token in current_msg
                for token in (
                    "создан",
                    "создано",
                    "создана",
                    "перенесен",
                    "перенесена",
                    "перенесено",
                    "перенес",
                    "обновлен",
                    "обновлена",
                    "обновлено",
                )
            )
            if has_success_phrase or not current_msg:
                response["user_message"] = (
                    "Не получилось обновить встречу в календаре. Попробуйте еще раз."
                    if normalized_intent == "meeting.update"
                    else "Не получилось создать встречу в календаре. Попробуйте еще раз."
                )
            if not str(response.get("outcome") or "").strip():
                response["outcome"] = "error"
        return response

    def process_request(self, req: TelegramRuntimeRequest) -> TelegramRuntimeResult:
        _LOG.info(
            "telegram_runtime_bridge process_request mode=%s request_id=%s channel=%s has_text=%s has_audio=%s",
            self.mode,
            req.request_id,
            req.channel,
            bool(str(req.text or "").strip()),
            req.audio_bytes is not None,
        )
        result = self._process_direct(req)
        payload = result.raw_runtime_payload if isinstance(result.raw_runtime_payload, dict) else {}
        command = payload.get("command") if isinstance(payload.get("command"), dict) else {}
        rec = payload.get("rec") if isinstance(payload.get("rec"), dict) else {}
        state_family = str(
            rec.get("missing_field")
            or payload.get("missing_field")
            or payload.get("state")
            or ""
        ).strip()
        _LOG.info(
            "runtime_response_built",
            extra={
                "trace_id": str(req.request_id or ""),
                "idempotency_key": _safe_idempotency_key(str(req.request_id or "")),
                "state_family": state_family,
                "intent": str(command.get("intent") or ""),
                "response_kind": str(result.outcome or ""),
            },
        )
        return result

    @staticmethod
    def _is_voice_only_request(req: TelegramRuntimeRequest) -> bool:
        if str(req.text or "").strip():
            return False
        metadata = req.metadata if isinstance(req.metadata, dict) else {}
        voice_meta = metadata.get("voice") if isinstance(metadata.get("voice"), dict) else None
        has_voice_meta = isinstance(voice_meta, dict)
        has_audio_fields = (
            req.audio_bytes is not None
            or bool(str(req.audio_mime or "").strip())
            or bool(str(req.audio_filename or "").strip())
        )
        return bool(has_voice_meta or has_audio_fields)

    @staticmethod
    def _is_voice_download_failed(req: TelegramRuntimeRequest) -> bool:
        metadata = req.metadata if isinstance(req.metadata, dict) else {}
        return bool(metadata.get("audio_download_error"))

    @staticmethod
    def _from_runtime_payload(payload: Dict[str, Any], *, fallback_outcome: str) -> TelegramRuntimeResult:
        outcome = str(payload.get("outcome") or fallback_outcome)
        runtime_outcome = str(payload.get("outcome") or "").strip().lower()
        # Canonical runtime payload fields only (legacy alias tolerance retired).
        clarifying_question = _first_non_empty_text(payload.get("clarifying_question"))
        user_message = _first_non_empty_text(
            payload.get("reply_text"),
            payload.get("user_message"),
        )
        error_message = _first_non_empty_text(
            payload.get("error"),
            payload.get("error_message"),
            payload.get("detail"),
        )

        needs_clarification = (
            bool(payload.get("needs_clarification"))
            or bool(clarifying_question)
            or runtime_outcome in ("needs_clarification", "rec_needs_clarification", "clarification")
        )
        if needs_clarification and not user_message:
            user_message = clarifying_question

        success_without_text = runtime_outcome in ("success", "ok") or str(outcome).strip().lower() in ("success", "ok")
        invalid_payload = runtime_outcome in ("direct_invalid_payload", "compat_invalid_payload")
        has_error_outcome = runtime_outcome in {
            "error",
            "failed",
            "bridge_error",
            "rec_rejected",
            "rec_empty",
            "llm_timeout",
            "llm_invalid_output",
            "download_failed",
            "convert_failed",
            "asr_timeout",
            "asr_failed",
            "asr_unavailable",
            "asr_empty",
            "empty_transcript",
            "low_quality_transcript",
            "unexpected_error",
            "rejected",
        } or runtime_outcome.endswith("_error")
        no_runtime_result = not runtime_outcome and not clarifying_question and not error_message

        if not user_message:
            if success_without_text:
                user_message = _SUCCESS_DEFAULT_REPLY
            elif error_message:
                user_message = error_message
            elif has_error_outcome:
                user_message = _RUNTIME_ERROR_REPLY
            elif invalid_payload or no_runtime_result:
                user_message = _MISSING_RUNTIME_RESULT_REPLY

        duplicate_state = payload.get("duplicate_state")
        telemetry = payload.get("telemetry")
        telemetry = dict(telemetry) if isinstance(telemetry, dict) else {}
        return TelegramRuntimeResult(
            outcome=outcome,
            reply_text=user_message,
            needs_clarification=needs_clarification,
            clarifying_question=clarifying_question,
            duplicate_state=str(duplicate_state) if isinstance(duplicate_state, str) else None,
            telemetry=telemetry,
            raw_runtime_payload=payload,
        )

    def _bridge_error_result(
        self,
        *,
        reason: str,
        req: TelegramRuntimeRequest,
        status_code: Optional[int] = None,
        details: Optional[str] = None,
    ) -> TelegramRuntimeResult:
        telemetry: Dict[str, Any] = {
            "mode": self.mode,
            "bridge_error": reason,
            "request_id": req.request_id,
        }
        if status_code is not None:
            telemetry["status_code"] = int(status_code)
        if details:
            telemetry["details"] = details[:300]
        return TelegramRuntimeResult(
            outcome="bridge_error",
            reply_text=_BRIDGE_ERROR_REPLY,
            telemetry=telemetry,
        )

    def _process_direct(self, req: TelegramRuntimeRequest) -> TelegramRuntimeResult:
        _LOG.info(
            "telegram_runtime_bridge path_entry mode=%s path=direct request_id=%s",
            self.mode,
            req.request_id,
        )
        voice_direct_enabled = bool(getattr(self, "direct_voice_enabled", False))
        _LOG.info(
            "telegram_debug_voice_request",
            extra={
                "text": req.text,
                "text_len": len(req.text or ""),
                "audio_present": req.audio_bytes is not None,
                "audio_len": len(req.audio_bytes) if req.audio_bytes else 0,
                "mime": getattr(req, "audio_mime", None),
            },
        )
        voice_only = self._is_voice_only_request(req)
        if voice_only and self._is_voice_download_failed(req) and req.audio_bytes is None:
            _LOG.warning(
                "telegram_direct_bridge voice_download_failed request_id=%s mode=%s",
                req.request_id,
                self.mode,
            )
            return TelegramRuntimeResult(
                outcome="download_failed",
                reply_text=_VOICE_DOWNLOAD_FAILED_REPLY,
                telemetry={
                    "mode": self.mode,
                    "voice_direct_enabled": voice_direct_enabled,
                    "download_failed": True,
                },
            )
        if self.direct_handler is None:
            return TelegramRuntimeResult(
                outcome="direct_mode_not_ready",
                reply_text="Режим runtime_core_direct пока не подключен в этом окружении.",
                telemetry={"mode": self.mode},
            )
        if voice_only:
            _LOG.info(
                "voice_routed_to_runtime",
                extra={
                    "request_id": req.request_id,
                    "user_id": req.user_id,
                    "trace_id": req.request_id,
                    "audio_present": req.audio_bytes is not None,
                    "audio_len": len(req.audio_bytes) if req.audio_bytes else 0,
                    "voice_direct_enabled": voice_direct_enabled,
                },
            )
        req_for_handler, prebuilt_result = self._maybe_enrich_request_for_meeting_update(req)
        if prebuilt_result is not None:
            return prebuilt_result
        try:
            payload = self.direct_handler(req_for_handler)
        except Exception as exc:
            _LOG.exception("telegram_direct_bridge request_failed: %s", str(exc))
            return self._bridge_error_result(reason="direct_runtime_error", req=req, details=str(exc))
        payload_dict = payload if isinstance(payload, dict) else {"outcome": "direct_invalid_payload"}
        payload_dict = self._precheck_task_create_duplicate_confirmation(req, payload_dict)
        if self._is_execution_candidate(payload_dict):
            command = payload_dict.get("command") if isinstance(payload_dict.get("command"), dict) else {}
            if self._is_temporal_intent(str(command.get("intent") or "")):
                payload_dict = self._commit_temporal(req, payload_dict)
            else:
                payload_dict = self._execute_runtime_command(req, payload_dict)
                worker_debug = payload_dict.get("debug") if isinstance(payload_dict.get("debug"), dict) else {}
                if (
                    str(command.get("intent") or "").strip().lower() == "task.create"
                    and bool(payload_dict.get("needs_clarification"))
                    and str(payload_dict.get("missing_field") or "").strip() == _TASK_CREATE_DUPLICATE_CONFIRM_FIELD
                    and bool(worker_debug.get("duplicate_found"))
                ):
                    payload_dict = self._wrap_task_create_duplicate_confirmation(req, payload, payload_dict)
        result = self._from_runtime_payload(payload_dict, fallback_outcome="direct_processed")
        if voice_only:
            telemetry = dict(result.telemetry or {})
            telemetry["voice_request"] = True
            telemetry["voice_direct_enabled"] = voice_direct_enabled
            _LOG.info(
                "telegram_direct_bridge voice_runtime_outcome request_id=%s outcome=%s",
                req.request_id,
                result.outcome,
            )
            return replace(
                result,
                outcome="direct_processed" if result.outcome == "rec_needs_clarification" else result.outcome,
                telemetry=telemetry,
            )
        return result
