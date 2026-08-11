from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

from src.app.runtime import handle_user_attempt_runtime
from src.core.config import AppConfig
from src.reliability.local_db import LocalMainDb


class _UnsupportedAsrClient:
    def transcribe(self, *args: Any, **kwargs: Any) -> str:
        raise RuntimeError("audio_not_supported_in_text_cli")


class _GatewayChatLlmClient:
    def __init__(self, *, base_url: str, timeout_sec: float) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_sec = float(timeout_sec)

    @staticmethod
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

    @staticmethod
    def _fallback_command_from_text(text: str) -> Dict[str, Any]:
        return {"intent": "unknown", "entities": {"text": str(text or "").strip()}}

    @staticmethod
    def _normalize_command(command_like: Dict[str, Any], *, source_text: str) -> Dict[str, Any]:
        intent = str(command_like.get("intent") or "unknown").strip().lower() or "unknown"
        entities = command_like.get("entities")
        if not isinstance(entities, dict):
            entities = {}
        if "text" not in entities and source_text.strip():
            entities["text"] = source_text.strip()
        normalized: Dict[str, Any] = {"intent": intent, "entities": entities}
        for key in ("confidence", "rejected", "missing_field", "clarifying_question", "needs_clarification"):
            if key in command_like:
                normalized[key] = command_like.get(key)
        return normalized

    def parse(self, *, text: str) -> Dict[str, Any]:
        user_text = str(text or "").strip()
        if not user_text:
            return self._fallback_command_from_text(user_text)

        prompt = (
            "Верни только JSON-объект вида "
            '{"intent":"...","entities":{...}} '
            "для пользовательской команды. "
            'Если intent неясен, используй intent="unknown". '
            f"Команда пользователя: {user_text}"
        )
        response = requests.post(
            f"{self.base_url}/chat",
            json={"prompt": prompt},
            timeout=self.timeout_sec,
        )
        response.raise_for_status()
        data = response.json()
        content = str(data.get("response") or "").strip() if isinstance(data, dict) else ""
        parsed = self._extract_json_object(content)
        if parsed is None:
            return self._fallback_command_from_text(user_text)
        return self._normalize_command(parsed, source_text=user_text)


class _GatewayRagRecClient:
    def __init__(self, *, base_url: str, timeout_sec: float) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_sec = float(timeout_sec)

    def query(self, body: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(body or {})
        text = str(payload.get("text") or payload.get("query") or "").strip()
        command = payload.get("command") if isinstance(payload.get("command"), dict) else None
        if not text:
            return {"outcome": "rejected", "needs_clarification": False, "hits": []}

        try:
            req_json: Dict[str, Any] = {"query": text}
            if command is not None:
                req_json["command"] = command
            response = requests.post(
                f"{self.base_url}/rag/query",
                json=req_json,
                timeout=self.timeout_sec,
            )
        except Exception as exc:
            return {"outcome": "rejected", "needs_clarification": False, "hits": [], "error": str(exc)}

        if response.status_code >= 400:
            return {"outcome": "rejected", "needs_clarification": False, "hits": [], "status_code": response.status_code}

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


@dataclass(frozen=True)
class _CliArgs:
    text: str
    ml_gateway_url: str
    app_id: str
    app_version: str
    app_env: str
    tenant_id: str
    user_id: str
    channel: str
    local_db_path: str
    timeout_sec: float
    clarification_ttl_sec: int
    command_dedup_reservation_ttl_sec: int
    queue_replay_ttl_seconds: int


def _parse_args(argv: list[str]) -> _CliArgs:
    parser = argparse.ArgumentParser(description="Minimal runtime text CLI (no Telegram transport).")
    parser.add_argument("--text", required=True, help="User text to send via runtime pipeline.")
    parser.add_argument("--ml-gateway-url", default="", help="ML gateway base URL (or env ML_GATEWAY_URL).")
    parser.add_argument("--app-id", default="runtime-cli")
    parser.add_argument("--app-version", default="1")
    parser.add_argument("--app-env", default="dev")
    parser.add_argument("--tenant-id", default="default_tenant")
    parser.add_argument("--user-id", default="cli_user")
    parser.add_argument("--channel", default="cli")
    parser.add_argument("--local-db-path", default="telegram-bot/app-integration/data/runtime_cli_local.db")
    parser.add_argument("--timeout-sec", type=float, default=20.0)
    parser.add_argument("--clarification-ttl-sec", type=int, default=300)
    parser.add_argument("--command-dedup-reservation-ttl-sec", type=int, default=120)
    parser.add_argument("--queue-replay-ttl-seconds", type=int, default=600)
    ns = parser.parse_args(argv)

    gateway = str(ns.ml_gateway_url or "").strip() or str(os.getenv("ML_GATEWAY_URL", "")).strip()
    if not gateway:
        parser.error("ML gateway URL is required: pass --ml-gateway-url or set ML_GATEWAY_URL")

    return _CliArgs(
        text=str(ns.text or "").strip(),
        ml_gateway_url=gateway.rstrip("/"),
        app_id=str(ns.app_id or "").strip(),
        app_version=str(ns.app_version or "").strip(),
        app_env=str(ns.app_env or "dev").strip() or "dev",
        tenant_id=str(ns.tenant_id or "").strip(),
        user_id=str(ns.user_id or "").strip(),
        channel=str(ns.channel or "cli").strip() or "cli",
        local_db_path=str(ns.local_db_path or "").strip(),
        timeout_sec=float(ns.timeout_sec),
        clarification_ttl_sec=int(ns.clarification_ttl_sec),
        command_dedup_reservation_ttl_sec=int(ns.command_dedup_reservation_ttl_sec),
        queue_replay_ttl_seconds=int(ns.queue_replay_ttl_seconds),
    )


def _build_cfg(args: _CliArgs) -> AppConfig:
    return AppConfig(
        app_id=args.app_id,
        app_version=args.app_version,
        app_env=args.app_env,
        local_db_path=args.local_db_path,
        cloud_buffer_dsn="",
        global_dict_url="",
        global_dict_path="",
        app_dict_path="",
        buffer_cleanup_delivered_days=7,
        buffer_cleanup_keep_last_per_user=200,
        queue_replay_ttl_seconds=args.queue_replay_ttl_seconds,
        clarification_ttl_sec=args.clarification_ttl_sec,
        command_dedup_reservation_ttl_sec=args.command_dedup_reservation_ttl_sec,
    )


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    cfg = _build_cfg(args)

    db = LocalMainDb(cfg.local_db_path)
    db.ensure_schema()

    asr_client = _UnsupportedAsrClient()
    llm_client = _GatewayChatLlmClient(base_url=args.ml_gateway_url, timeout_sec=args.timeout_sec)
    rec_client = _GatewayRagRecClient(base_url=args.ml_gateway_url, timeout_sec=args.timeout_sec)

    result = handle_user_attempt_runtime(
        cfg=cfg,
        tenant_id=args.tenant_id,
        request_id=str(uuid.uuid4()),
        user_id=args.user_id,
        channel=args.channel,
        asr_client=asr_client,
        llm_client=llm_client,
        rec_client=rec_client,
        text=args.text,
        local_db=db,
        source_chat_id=args.user_id,
        source_message_id="",
    )

    user_message = str(result.get("user_message") or result.get("clarifying_question") or "").strip()
    if not user_message:
        user_message = "No user_message in runtime result."
    print(user_message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
