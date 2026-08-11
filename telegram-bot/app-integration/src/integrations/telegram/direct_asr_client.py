from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Callable, Optional

import requests

_LOG = logging.getLogger(__name__)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off", ""}:
        return False
    return bool(default)


def _is_retryable_status(status_code: Optional[int]) -> bool:
    if not isinstance(status_code, int):
        return False
    return status_code in {408, 429, 500, 502, 503, 504}


class GatewayAsrClient:
    def __init__(
        self,
        *,
        base_url: str,
        timeout_sec: float,
        retries: int = 0,
        http_post: Optional[Callable[..., Any]] = None,
    ) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_sec = float(timeout_sec)
        self.retries = max(0, int(retries))
        self.http_post = http_post or requests.post

    def transcribe(
        self,
        audio_bytes: bytes,
        mime_type: str | None = None,
        filename: str | None = None,
        request_id: str | None = None,
    ) -> str:
        if not audio_bytes:
            return ""

        mime = str(mime_type or "audio/ogg").strip() or "audio/ogg"
        name = str(filename or "voice.ogg").strip() or "voice.ogg"
        files = {"file": (name, audio_bytes, mime)}
        url = f"{self.base_url}/asr"

        max_attempts = self.retries + 1
        request_started = time.perf_counter()
        _LOG.info(
            "telegram_direct_asr_started request_id=%s url=%s bytes=%s mime=%s filename=%s retries=%s",
            str(request_id or ""),
            url,
            len(audio_bytes),
            mime,
            name,
            self.retries,
        )

        last_error: Optional[Exception] = None
        saw_timeout = False
        for attempt in range(1, max_attempts + 1):
            attempt_started = time.perf_counter()
            try:
                response = self.http_post(
                    url,
                    files=files,
                    timeout=self.timeout_sec,
                )
                status_code = getattr(response, "status_code", None)
                if isinstance(status_code, int) and status_code >= 400:
                    if _is_retryable_status(status_code) and attempt < max_attempts:
                        _LOG.warning(
                            "telegram_direct_asr_retryable_http attempt=%s/%s status=%s",
                            attempt,
                            max_attempts,
                            status_code,
                        )
                        continue
                    raise Exception(f"asr_upstream_http_{status_code}")
                if hasattr(response, "raise_for_status"):
                    try:
                        response.raise_for_status()
                    except requests.exceptions.HTTPError as exc:
                        retryable_status = getattr(getattr(exc, "response", None), "status_code", None)
                        if _is_retryable_status(retryable_status) and attempt < max_attempts:
                            _LOG.warning(
                                "telegram_direct_asr_retryable_http attempt=%s/%s status=%s",
                                attempt,
                                max_attempts,
                                retryable_status,
                            )
                            continue
                        raise
                if not hasattr(response, "json"):
                    raise Exception("asr_upstream_invalid_response")
                payload = response.json()
                if not isinstance(payload, dict):
                    raise Exception("asr_upstream_invalid_payload")
                text = payload.get("text")
                if _env_bool("DIRECT_ASR_LOG_RAW_RESPONSE", False):
                    _LOG.info(
                        "telegram_direct_asr_raw_response request_id=%s attempt=%s/%s payload=%s",
                        str(request_id or ""),
                        attempt,
                        max_attempts,
                        json.dumps(payload, ensure_ascii=False)[:4000],
                    )
                latency_ms = round((time.perf_counter() - request_started) * 1000, 2)
                _LOG.info(
                    "telegram_direct_asr_completed request_id=%s attempt=%s/%s latency_ms=%s text_len=%s",
                    str(request_id or ""),
                    attempt,
                    max_attempts,
                    latency_ms,
                    len(str(text or "").strip()) if isinstance(text, str) else 0,
                )
                return str(text or "").strip() if isinstance(text, str) else ""
            except TimeoutError as exc:
                saw_timeout = True
                last_error = exc
                attempt_ms = round((time.perf_counter() - attempt_started) * 1000, 2)
                if attempt < max_attempts:
                    _LOG.warning(
                        "telegram_direct_asr_retryable_timeout attempt=%s/%s attempt_ms=%s",
                        attempt,
                        max_attempts,
                        attempt_ms,
                    )
                    continue
                break
            except requests.exceptions.Timeout as exc:
                saw_timeout = True
                last_error = exc
                attempt_ms = round((time.perf_counter() - attempt_started) * 1000, 2)
                if attempt < max_attempts:
                    _LOG.warning(
                        "telegram_direct_asr_retryable_timeout attempt=%s/%s attempt_ms=%s",
                        attempt,
                        max_attempts,
                        attempt_ms,
                    )
                    continue
                break
            except requests.exceptions.ConnectionError as exc:
                last_error = exc
                attempt_ms = round((time.perf_counter() - attempt_started) * 1000, 2)
                if attempt < max_attempts:
                    _LOG.warning(
                        "telegram_direct_asr_retryable_connection_error attempt=%s/%s attempt_ms=%s err=%s",
                        attempt,
                        max_attempts,
                        attempt_ms,
                        str(exc),
                    )
                    continue
                break
            except Exception as exc:
                last_error = exc
                break
        assert last_error is not None
        latency_ms = round((time.perf_counter() - request_started) * 1000, 2)
        _LOG.warning(
            "telegram_direct_asr_failed request_id=%s latency_ms=%s retries=%s err=%s",
            str(request_id or ""),
            latency_ms,
            self.retries,
            str(last_error),
        )
        if saw_timeout:
            raise TimeoutError(str(last_error)) from last_error
        raise Exception(str(last_error)) from last_error
