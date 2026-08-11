from __future__ import annotations

import json
import logging
import os
import sys
import time
import hashlib
import urllib.request
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import requests

from src.integrations.telegram.reply_mapper import map_result_to_send_payload
from src.integrations.telegram.runtime_bridge import RuntimeBridge, build_runtime_core_direct_handler_from_settings
from src.integrations.telegram.schemas import TelegramRuntimeRequest
from src.integrations.telegram.transport import TelegramTransport
from src.integrations.telegram.update_mapper import map_update_to_request

_LOG = logging.getLogger(__name__)
_HEALTH_MARKER_PATH = "/tmp/bot.ok"
_DEFAULT_LOCAL_DB_PATH = "/data/runtime_local.db"
_DEFAULT_VERSION_PROOF_PATH = "/tmp/telegram_adapter.version.json"
_BUILD_INFO_PATH = "/app/build-info.json"
_LOG_EXTRA_ALLOWLIST = {
    "adapter_mode",
    "callback_data",
    "callback_query_id",
    "chat_id",
    "draft_id",
    "flow_id",
    "has_reply_markup",
    "idempotency_key",
    "intent",
    "message_id",
    "outgoing_type",
    "response_kind",
    "route_target",
    "session_id",
    "state_family",
    "status",
    "summary_sent",
    "text_prefix",
    "trace_id",
    "type",
    "update_id",
    "user_id",
}
_LOG_RESERVED_FIELDS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}


class _SafeOneLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        event = str(record.getMessage() or "").strip() or "log"
        parts = [
            f"ts={self.formatTime(record, '%Y-%m-%dT%H:%M:%S')}",
            f"level={record.levelname}",
            f"logger={record.name}",
            f"event={event}",
        ]
        for key in sorted(_LOG_EXTRA_ALLOWLIST):
            if hasattr(record, key):
                value = getattr(record, key)
                if value is None or value == "":
                    continue
                safe = str(value).replace("\n", "\\n").replace("\r", "\\r")
                parts.append(f"{key}={json.dumps(safe, ensure_ascii=False)}")
        if record.exc_info:
            parts.append(f"exc={json.dumps(self.formatException(record.exc_info), ensure_ascii=False)}")
        return " ".join(parts)


def _configure_stdout_logging() -> None:
    raw_level = str(os.getenv("LOG_LEVEL", "INFO") or "INFO").strip().upper()
    level = getattr(logging, raw_level, logging.INFO)
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_SafeOneLineFormatter())
    root.addHandler(handler)
    root.setLevel(level)


def _safe_idempotency_key(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return digest[-8:]


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"{name} must be an integer") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        return float(str(raw).strip())
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"{name} must be a number") from exc


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off", ""}:
        return False
    raise ValueError(f"{name} must be a boolean")


def _require_direct_adapter_mode(mode: str) -> str:
    normalized = str(mode or "").strip() or RuntimeBridge.DIRECT_MODE
    if normalized != RuntimeBridge.DIRECT_MODE:
        _LOG.warning("legacy_route_used route=telegram_adapter_mode requested_mode=%s", normalized)
        raise ValueError(
            "Unsupported TELEGRAM_ADAPTER_MODE="
            f"{normalized!r}. Active production contour is direct-only; "
            "use runtime_core_direct."
        )
    return normalized


@dataclass(frozen=True)
class AdapterSettings:
    telegram_bot_token: str
    telegram_adapter_mode: str
    worker_command_url: str
    organizer_api_url: str
    google_sync_review_url: str
    ml_gateway_url: str

    app_id: str
    app_version: str
    app_env: str
    tenant_id: str

    local_db_path: str
    cloud_buffer_dsn: str
    app_dict_path: str
    queue_replay_ttl_seconds: int
    clarification_ttl_sec: int
    command_dedup_reservation_ttl_sec: int

    state_path: str
    health_port: int
    app_timezone: str

    telegram_http_timeout_sec: float
    telegram_http_retries: int
    telegram_poll_timeout_sec: int
    telegram_poll_interval_sec: float
    telegram_max_cycles: int
    direct_voice_enabled: bool = True
    direct_asr_timeout_sec: float = 20.0
    direct_asr_retries: int = 0


@dataclass
class AdapterApp:
    settings: AdapterSettings
    transport: TelegramTransport
    bridge: RuntimeBridge
    offset: Optional[int] = None


def load_settings_from_env() -> AdapterSettings:
    token = _env("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN is required")

    app_id = _env("APP_ID")
    if not app_id:
        raise ValueError("APP_ID is required")
    app_version = _env("APP_VERSION")
    if not app_version:
        raise ValueError("APP_VERSION is required")
    adapter_mode = _require_direct_adapter_mode(
        _env("TELEGRAM_ADAPTER_MODE", RuntimeBridge.DIRECT_MODE) or RuntimeBridge.DIRECT_MODE
    )

    return AdapterSettings(
        telegram_bot_token=token,
        telegram_adapter_mode=adapter_mode,
        worker_command_url=_env("WORKER_COMMAND_URL", ""),
        organizer_api_url=_env("ORGANIZER_API_URL", "http://organizer-api:8000"),
        google_sync_review_url=_env("GOOGLE_SYNC_REVIEW_URL", "http://google-sync:8010/review"),
        ml_gateway_url=_env("ML_GATEWAY_URL"),
        app_id=app_id,
        app_version=app_version,
        app_env=_env("APP_ENV", "dev"),
        tenant_id=_env("TENANT_ID", "default_tenant"),
        local_db_path=_env("LOCAL_DB_PATH", _DEFAULT_LOCAL_DB_PATH),
        cloud_buffer_dsn=_env("CLOUD_BUFFER_DSN", f"sqlite:///{os.path.join('app-integration', 'data', 'cloud.db')}"),
        app_dict_path=_env("APP_DICT_PATH", os.path.join("app-integration", "src", "dictionaries", "app_dict.yaml")),
        queue_replay_ttl_seconds=_env_int("QUEUE_REPLAY_TTL_SECONDS", 600),
        clarification_ttl_sec=_env_int("CLARIFICATION_TTL_SEC", 300),
        command_dedup_reservation_ttl_sec=_env_int("COMMAND_DEDUP_RESERVATION_TTL_SEC", 120),
        state_path=_env("STATE_PATH", os.path.join("app-integration", "data", "telegram_adapter_state.json")),
        health_port=_env_int("HEALTH_PORT", 0),
        app_timezone=_env("APP_TIMEZONE", "UTC"),
        telegram_http_timeout_sec=float(_env("TELEGRAM_HTTP_TIMEOUT_SEC", "10")),
        telegram_http_retries=_env_int("TELEGRAM_HTTP_RETRIES", 1),
        telegram_poll_timeout_sec=_env_int("TELEGRAM_POLL_TIMEOUT_SEC", 20),
        telegram_poll_interval_sec=float(_env("TELEGRAM_POLL_INTERVAL_SEC", "1.0")),
        telegram_max_cycles=_env_int("TELEGRAM_MAX_CYCLES", 0),
        direct_voice_enabled=_env_bool("DIRECT_VOICE_ENABLED", True),
        direct_asr_timeout_sec=_env_float("DIRECT_ASR_TIMEOUT_SEC", _env_float("TELEGRAM_HTTP_TIMEOUT_SEC", 10.0)),
        direct_asr_retries=_env_int("DIRECT_ASR_RETRIES", 0),
    )


def _load_offset(path: str) -> Optional[int]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    offset = payload.get("offset") if isinstance(payload, dict) else None
    if isinstance(offset, int):
        return offset
    return None


def _save_offset(path: str, offset: int) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"offset": int(offset)}, ensure_ascii=False, indent=2), encoding="utf-8")


def _touch_health_marker(path: Optional[str] = None) -> None:
    marker_path = str(path or _HEALTH_MARKER_PATH)
    try:
        p = Path(marker_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(f"ok {int(time.time())}\n", encoding="utf-8")
    except Exception:
        _LOG.debug("telegram_adapter health_marker_write_failed path=%s", marker_path)


def _context_key(*, app_id: str, tenant_id: str, user_id: str, channel: str, chat_id: str) -> str:
    scoped_chat_id = str(chat_id or user_id or "").strip()
    return (
        f"{app_id or 'default_app'}::{tenant_id or 'default_tenant'}::"
        f"{channel or 'unknown'}::{scoped_chat_id}::{user_id}"
    )


def _extract_sent_message_id(response: Dict[str, Any]) -> str:
    result = response.get("result") if isinstance(response.get("result"), dict) else {}
    value = result.get("message_id")
    return str(value or "").strip()


def _sha256_file(path: Path) -> str:
    try:
        data = path.read_bytes()
    except Exception:
        return ""
    return hashlib.sha256(data).hexdigest()


def _load_build_info(path: str = _BUILD_INFO_PATH) -> Dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _short_value(value: Any, *, size: int = 12) -> str:
    text = str(value or "").strip()
    if not text:
        return "unknown"
    return text[:size]


def _build_version_proof_payload(st: AdapterSettings) -> Dict[str, Any]:
    src_root = Path(__file__).resolve().parents[2]
    handler_path = src_root / "app" / "handler.py"
    handler_sha = _sha256_file(handler_path)
    build_info = _load_build_info()
    route_choice_marker_present = False
    try:
        route_choice_marker_present = "runtime_route_choice" in handler_path.read_text(encoding="utf-8")
    except Exception:
        route_choice_marker_present = False
    git_sha = (
        str(build_info.get("git_sha") or "").strip()
        or
        _env("APP_GIT_SHA")
        or _env("GIT_SHA")
        or _env("BUILD_GIT_SHA")
        or _env("COMMIT_SHA")
        or "unknown"
    )
    route_rules_version = str(build_info.get("route_rules_version") or "").strip() or "unavailable"
    return {
        "event": "telegram_adapter_version_proof",
        "service": str(build_info.get("service") or "telegram-bot"),
        "app_id": st.app_id,
        "app_env": st.app_env,
        "app_version": st.app_version,
        "telegram_adapter_mode": st.telegram_adapter_mode,
        "direct_voice_enabled": bool(st.direct_voice_enabled),
        "git_sha": git_sha,
        "build_timestamp_utc": str(build_info.get("build_timestamp_utc") or "unknown"),
        "route_rules_version": route_rules_version,
        "source_marker": str(build_info.get("source_marker") or ""),
        "handler_path": str(handler_path),
        "handler_sha256": str(build_info.get("handler_sha256") or handler_sha or "unavailable"),
        "dockerfile_path": str(build_info.get("dockerfile_path") or ""),
        "build_context": str(build_info.get("build_context") or ""),
        "entrypoint_module": str(build_info.get("entrypoint_module") or "src.integrations.telegram.bot_main"),
        "handler_route_choice_marker_present": bool(route_choice_marker_present),
    }


def _write_version_proof_file(payload: Dict[str, Any], path: Optional[str] = None) -> None:
    target = str(path or _env("VERSION_PROOF_PATH", _DEFAULT_VERSION_PROOF_PATH)).strip() or _DEFAULT_VERSION_PROOF_PATH
    try:
        p = Path(target)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        _LOG.debug("telegram_adapter version_proof_write_failed path=%s", target)


def _is_retryable_poll_http_error(exc: requests.exceptions.HTTPError) -> bool:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    return status_code in {502, 504}


def bootstrap_adapter_app(
    *,
    settings: Optional[AdapterSettings] = None,
    transport_factory: Callable[..., TelegramTransport] = TelegramTransport,
    bridge_factory: Callable[..., RuntimeBridge] = RuntimeBridge,
) -> AdapterApp:
    st = settings or load_settings_from_env()
    _LOG.info("telegram_adapter bootstrap mode=%s", st.telegram_adapter_mode)
    version_proof = _build_version_proof_payload(st)
    _LOG.info(
        "telegram_adapter build_info service=%s git_sha=%s route_rules_version=%s handler_sha256=%s build_timestamp_utc=%s",
        version_proof.get("service"),
        version_proof.get("git_sha"),
        version_proof.get("route_rules_version"),
        version_proof.get("handler_sha256"),
        version_proof.get("build_timestamp_utc"),
    )
    _LOG.info("telegram_adapter version_proof=%s", json.dumps(version_proof, ensure_ascii=False, sort_keys=True))
    _write_version_proof_file(version_proof)
    transport = transport_factory(
        st.telegram_bot_token,
        timeout_sec=st.telegram_http_timeout_sec,
        retries=st.telegram_http_retries,
    )
    bridge_kwargs: Dict[str, Any] = {
        "mode": st.telegram_adapter_mode,
        "worker_command_url": st.worker_command_url,
        "direct_voice_enabled": st.direct_voice_enabled,
    }
    if st.telegram_adapter_mode == RuntimeBridge.DIRECT_MODE:
        bridge_kwargs["direct_handler"] = build_runtime_core_direct_handler_from_settings(
            app_id=st.app_id,
            app_version=st.app_version,
            app_env=st.app_env,
            local_db_path=st.local_db_path,
            cloud_buffer_dsn=st.cloud_buffer_dsn,
            app_dict_path=st.app_dict_path,
            queue_replay_ttl_seconds=st.queue_replay_ttl_seconds,
            clarification_ttl_sec=st.clarification_ttl_sec,
            command_dedup_reservation_ttl_sec=st.command_dedup_reservation_ttl_sec,
            ml_gateway_url=st.ml_gateway_url,
            timeout_sec=st.telegram_http_timeout_sec,
            direct_voice_enabled=st.direct_voice_enabled,
            direct_asr_timeout_sec=st.direct_asr_timeout_sec,
            direct_asr_retries=st.direct_asr_retries,
        )
    bridge = bridge_factory(
        **bridge_kwargs,
    )
    app = AdapterApp(settings=st, transport=transport, bridge=bridge, offset=_load_offset(st.state_path))
    _touch_health_marker()
    return app


def _try_attach_voice_bytes(req: TelegramRuntimeRequest, transport: TelegramTransport) -> TelegramRuntimeRequest:
    voice_meta = req.metadata.get("voice") if isinstance(req.metadata, dict) else None
    file_id = ""
    if isinstance(voice_meta, dict):
        file_id = str(voice_meta.get("file_id") or "").strip()
    if not file_id:
        return req

    _LOG.info(
        "telegram_voice_download_started request_id=%s file_id=%s",
        req.request_id,
        file_id,
    )
    file_info = transport.get_file(file_id=file_id)
    file_path = str(file_info.get("file_path") or "").strip()
    if not file_path:
        return req
    audio_bytes = transport.download_file(file_path=file_path)
    _LOG.info(
        "telegram_voice_download_succeeded request_id=%s file_id=%s bytes=%s",
        req.request_id,
        file_id,
        len(audio_bytes),
    )
    return replace(req, audio_bytes=audio_bytes)


def _system_fetch_json(url: str, *, timeout_sec: float) -> Dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_sec) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return payload if isinstance(payload, dict) else {}


def _system_mark(ok: bool) -> str:
    return "OK" if ok else "DOWN"


def _system_service_state_from_payload(payload: Dict[str, Any]) -> str:
    services = payload.get("services") if isinstance(payload.get("services"), dict) else {}
    for key in ("asr", "calendar_write"):
        value = str(services.get(key) or "").strip().lower()
        if value:
            return "ok" if value == "ok" else "down"
    if bool(payload.get("ok")):
        return "ok"
    status = str(payload.get("status") or "").strip().lower()
    if status:
        return "ok" if status in {"ok", "healthy"} else "down"
    return "down"


def _worker_health_url(command_url: str) -> str:
    base = str(command_url or "").strip().rstrip("/")
    if base.endswith("/runtime/command"):
        base = base[: -len("/runtime/command")]
    return f"{base}/health" if base else ""


def _system_summary_text(settings: AdapterSettings) -> str:
    timeout_sec = max(1.0, float(settings.telegram_http_timeout_sec))
    api_base = str(settings.organizer_api_url or "").strip().rstrip("/")
    worker_health_url = _worker_health_url(settings.worker_command_url)
    ml_base = str(settings.ml_gateway_url or "").strip().rstrip("/")

    bot_ok = True
    worker_ok = False
    api_ok = False
    google_ok = False
    asr_state = "n/a"

    if worker_health_url:
        try:
            worker_ok = bool(_system_fetch_json(worker_health_url, timeout_sec=timeout_sec).get("ok"))
        except Exception:
            worker_ok = False

    if api_base:
        try:
            api_ok = bool(_system_fetch_json(f"{api_base}/health", timeout_sec=timeout_sec).get("ok"))
        except Exception:
            api_ok = False
        try:
            google_payload = _system_fetch_json(f"{api_base}/google/health", timeout_sec=timeout_sec)
            google_state = _system_service_state_from_payload(google_payload)
            google_ok = google_state == "ok"
        except Exception:
            google_ok = False

    if ml_base:
        try:
            ml_payload = _system_fetch_json(f"{ml_base}/health", timeout_sec=timeout_sec)
            services = ml_payload.get("services") if isinstance(ml_payload.get("services"), dict) else {}
            asr_value = str(services.get("asr") or ml_payload.get("asr") or ml_payload.get("status") or "").strip().lower()
            if asr_value:
                asr_state = "ok" if asr_value == "ok" else "down"
            else:
                asr_state = "down"
        except Exception:
            asr_state = "down"

    version_proof = _build_version_proof_payload(settings)
    lines = [
        "Состояние системы",
        f"Bot/runtime: {_system_mark(bot_ok)}",
        f"Adapter mode: {settings.telegram_adapter_mode}",
        "Build:",
        f"route_rules_version: {version_proof.get('route_rules_version') or 'unavailable'}",
        f"git_sha: {_short_value(version_proof.get('git_sha'))}",
        f"handler_sha256: {_short_value(version_proof.get('handler_sha256'))}",
        f"build_timestamp_utc: {version_proof.get('build_timestamp_utc') or 'unavailable'}",
        f"Worker: {_system_mark(worker_ok)}",
        f"API: {_system_mark(api_ok)}",
        f"Google: {_system_mark(google_ok)}",
        f"Voice/ASR: {asr_state.upper() if asr_state in {'ok', 'down'} else 'N/A'}",
    ]
    return "\n".join(lines)


def _sheets_review_summary_text(settings: AdapterSettings, *, limit: int = 5) -> str:
    timeout_sec = max(1.0, float(settings.telegram_http_timeout_sec))
    base_url = str(settings.google_sync_review_url or "").strip()
    if not base_url:
        return "Проверка Google Sheets недоступна: не настроен GOOGLE_SYNC_REVIEW_URL."

    separator = "&" if "?" in base_url else "?"
    payload = _system_fetch_json(f"{base_url}{separator}limit={int(max(1, limit))}", timeout_sec=timeout_sec)
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    changes = payload.get("changes") if isinstance(payload.get("changes"), list) else []
    total_changes = int(summary.get("total_changes") or 0)
    proposed = int(summary.get("proposed") or 0)
    confirm_required = int(summary.get("confirm_required") or 0)
    conflict = int(summary.get("conflict") or 0)
    invalid = int(summary.get("invalid") or 0)

    lines = [
        "Проверка Google Sheets",
        f"Всего изменений: {total_changes}",
        f"Предложено: {proposed}",
        f"Нужно подтверждение: {confirm_required}",
        f"Конфликт: {conflict}",
        f"Некорректно: {invalid}",
    ]
    if total_changes <= 0:
        lines.append("")
        lines.append("Изменений не найдено.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Первые изменения:")
    for index, change in enumerate(changes[: max(1, limit)], start=1):
        if not isinstance(change, dict):
            continue
        change_id = str(change.get("change_id") or "-").strip()
        sheet = str(change.get("sheet") or "-").strip()
        entity_type = str(change.get("entity_type") or "-").strip()
        entity_id = str(change.get("entity_id") or "-").strip()
        field = str(change.get("field") or "-").strip()
        status = str(change.get("status") or "-").strip()
        lines.append(f"{index}. {change_id} | {sheet} | {entity_type}:{entity_id} | {field} | {status}")

    first_change_id = ""
    for item in changes:
        if isinstance(item, dict):
            first_change_id = str(item.get("change_id") or "").strip()
            if first_change_id:
                break
    if first_change_id:
        lines.append("")
        lines.append("Для применения используйте:")
        lines.append(f".\\scripts\\sheets_review_apply.ps1 -Mode apply -ChangeId {first_change_id}")
    else:
        lines.append("")
        lines.append("Для применения используйте operator script с нужным ChangeId.")
    lines.append("Apply из Telegram отключен.")
    return "\n".join(lines)


def _send_message_with_optional_markup(
    transport: TelegramTransport,
    *,
    user_id: str,
    chat_id: str,
    text: str,
    reply_to_message_id: Optional[str],
    reply_markup: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    _LOG.info(
        "telegram_message_sent",
        extra={
            "user_id": str(user_id or ""),
            "chat_id": str(chat_id or ""),
            "has_reply_markup": isinstance(reply_markup, dict),
            "text_prefix": str(text or "")[:80],
        },
    )
    if not isinstance(reply_markup, dict):
        return transport.send_message(
            chat_id=chat_id,
            text=text,
            reply_to_message_id=reply_to_message_id,
        )

    payload: Dict[str, Any] = {"chat_id": chat_id, "text": text, "reply_markup": reply_markup}
    if reply_to_message_id:
        try:
            payload["reply_to_message_id"] = int(reply_to_message_id)
        except Exception:
            payload["reply_to_message_id"] = reply_to_message_id
    return transport._post("sendMessage", payload)


def _answer_callback_query_if_needed(req: TelegramRuntimeRequest, transport: TelegramTransport) -> None:
    metadata = req.metadata if isinstance(req.metadata, dict) else {}
    callback = metadata.get("callback_query") if isinstance(metadata.get("callback_query"), dict) else {}
    callback_query_id = str(callback.get("id") or "").strip()
    if not callback_query_id:
        return
    try:
        transport._post("answerCallbackQuery", {"callback_query_id": callback_query_id})
        _LOG.info(
            "telegram_callback_answered",
            extra={
                "callback_query_id": callback_query_id,
                "message_id": str(req.source_message_id or ""),
                "user_id": str(req.user_id or ""),
            },
        )
    except Exception:
        _LOG.debug("telegram_adapter answer_callback_query_failed request_id=%s", req.request_id)


def _should_send_runtime_result_message(result: Any) -> bool:
    telemetry = result.telemetry if isinstance(getattr(result, "telemetry", None), dict) else {}
    return not bool(telemetry.get("suppress_send"))


def process_single_update(app: AdapterApp, update: Dict[str, Any]) -> Optional[int]:
    req = map_update_to_request(
        update,
        app_id=app.settings.app_id,
        tenant_id=app.settings.tenant_id,
        timezone=app.settings.app_timezone,
        channel="telegram",
    )
    metadata = req.metadata if isinstance(req.metadata, dict) else {}
    callback = metadata.get("callback_query") if isinstance(metadata.get("callback_query"), dict) else {}
    update_type = "callback" if callback else ("voice" if isinstance(metadata.get("voice"), dict) else "text")
    _LOG.info(
        "telegram_update_received",
        extra={
            "update_id": str(metadata.get("update_id") or ""),
            "message_id": str(req.source_message_id or ""),
            "callback_query_id": str(callback.get("id") or ""),
            "user_id": str(req.user_id or ""),
            "type": update_type,
        },
    )
    if callback:
        _LOG.info(
            "telegram_callback_received",
            extra={
                "callback_query_id": str(callback.get("id") or ""),
                "callback_data": str(callback.get("data") or ""),
                "message_id": str(req.source_message_id or ""),
                "user_id": str(req.user_id or ""),
            },
        )
    if str(req.text or "").strip():
        _LOG.info(
            "telegram_text_received",
            extra={
                "user_id": str(req.user_id or ""),
                "chat_id": str(req.chat_id or ""),
                "message_id": str(req.source_message_id or ""),
                "trace_id": str(req.request_id or ""),
                "text_prefix": str(req.text or "")[:80],
                "type": update_type,
            },
        )
    _LOG.info(
        "text_command_routed",
        extra={
            "user_id": str(req.user_id or ""),
            "message_id": str(req.source_message_id or ""),
            "trace_id": str(req.request_id or ""),
            "idempotency_key": _safe_idempotency_key(req.request_id),
            "text_prefix": str(req.text or "")[:80],
        },
    )
    if str(req.text or "").strip() == "/system":
        summary = _system_summary_text(app.settings)
        _send_message_with_optional_markup(
            app.transport,
            user_id=req.user_id,
            chat_id=req.chat_id,
            text=summary,
            reply_to_message_id=req.source_message_id or None,
            reply_markup=None,
        )
        update_id = update.get("update_id")
        if isinstance(update_id, int):
            next_offset = update_id + 1
            app.offset = next_offset
            _save_offset(app.settings.state_path, next_offset)
            return next_offset
        return None
    if str(req.text or "").strip() == "/sheets_review":
        try:
            summary = _sheets_review_summary_text(app.settings)
        except Exception:
            summary = "Не удалось получить проверку Google Sheets. Проверьте сервис google-sync."
        _send_message_with_optional_markup(
            app.transport,
            user_id=req.user_id,
            chat_id=req.chat_id,
            text=summary,
            reply_to_message_id=req.source_message_id or None,
            reply_markup=None,
        )
        update_id = update.get("update_id")
        if isinstance(update_id, int):
            next_offset = update_id + 1
            app.offset = next_offset
            _save_offset(app.settings.state_path, next_offset)
            return next_offset
        return None
    try:
        req = _try_attach_voice_bytes(req, app.transport)
    except Exception as exc:
        _LOG.warning(
            "telegram_voice_download_failed request_id=%s err=%s",
            req.request_id,
            str(exc),
        )
        req_meta = dict(req.metadata or {})
        req_meta["audio_download_error"] = True
        req = replace(req, metadata=req_meta)

    result = app.bridge.process_request(req)
    if _should_send_runtime_result_message(result):
        payload = map_result_to_send_payload(result)
        send_response = _send_message_with_optional_markup(
            app.transport,
            user_id=req.user_id,
            chat_id=req.chat_id,
            text=str(payload.get("text") or ""),
            reply_to_message_id=req.source_message_id or None,
            reply_markup=payload.get("reply_markup") if isinstance(payload, dict) else None,
        )
        if bool(result.needs_clarification):
            prompt_message_id = _extract_sent_message_id(send_response if isinstance(send_response, dict) else {})
            local_db = getattr(app.bridge, "direct_local_db", None)
            if local_db is not None and prompt_message_id:
                context_key = _context_key(
                    app_id=app.settings.app_id,
                    tenant_id=app.settings.tenant_id,
                    user_id=req.user_id,
                    channel=req.channel,
                    chat_id=req.chat_id,
                )
                try:
                    local_db.bind_clarification_prompt_message(
                        context_key=context_key,
                        prompt_message_id=prompt_message_id,
                    )
                except Exception:
                    _LOG.debug(
                        "telegram_adapter clarification_prompt_bind_failed trace_id=%s message_id=%s",
                        req.request_id,
                        prompt_message_id,
                    )
    else:
        _LOG.info("telegram_adapter send_suppressed request_id=%s", req.request_id)
    _answer_callback_query_if_needed(req, app.transport)

    update_id = update.get("update_id")
    if isinstance(update_id, int):
        next_offset = update_id + 1
        app.offset = next_offset
        _save_offset(app.settings.state_path, next_offset)
        return next_offset
    return None


def run_polling(app: AdapterApp) -> int:
    cycles = 0
    while True:
        _touch_health_marker()
        try:
            updates = app.transport.get_updates(
                offset=app.offset,
                timeout_sec=app.settings.telegram_poll_timeout_sec,
                allowed_updates=["message", "edited_message", "callback_query"],
            )
        except requests.exceptions.ReadTimeout:
            # Long-poll read timeouts are expected when no updates arrive in the poll window.
            _LOG.debug("telegram_adapter get_updates_read_timeout; continuing polling loop")
            updates = []
        except requests.exceptions.HTTPError as exc:
            if not _is_retryable_poll_http_error(exc):
                raise
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            _LOG.warning(
                "telegram_adapter get_updates_http_error status=%s; continuing polling loop",
                status_code,
            )
            updates = []
        for update in updates:
            if isinstance(update, dict):
                try:
                    process_single_update(app, update)
                except Exception as exc:
                    _LOG.exception("telegram_adapter update_processing_failed: %s", str(exc))
        _touch_health_marker()

        cycles += 1
        if app.settings.telegram_max_cycles > 0 and cycles >= app.settings.telegram_max_cycles:
            break
        if not updates:
            time.sleep(max(0.0, app.settings.telegram_poll_interval_sec))
    return 0


def main() -> int:
    _configure_stdout_logging()
    app = bootstrap_adapter_app()
    return run_polling(app)


if __name__ == "__main__":
    raise SystemExit(main())
