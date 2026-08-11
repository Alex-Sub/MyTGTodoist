from __future__ import annotations

import requests
from typing import Any, Dict, List, Optional


class TelegramTransport:
    def __init__(
        self,
        token: str,
        *,
        timeout_sec: float = 10.0,
        retries: int = 1,
        api_base: str = "https://api.telegram.org",
        session: Optional[requests.Session] = None,
    ) -> None:
        self.token = (token or "").strip()
        if not self.token:
            raise ValueError("TELEGRAM_BOT_TOKEN is required")

        self.timeout_sec = float(timeout_sec)
        self.retries = max(0, int(retries))
        self.api_base = api_base.rstrip("/")
        self.bot_base = f"{self.api_base}/bot{self.token}"
        self.file_base = f"{self.api_base}/file/bot{self.token}"
        self.session = session or requests.Session()

    def _post(self, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.bot_base}/{method}"
        last_error: Optional[Exception] = None
        for _ in range(self.retries + 1):
            try:
                resp = self.session.post(url, json=payload, timeout=self.timeout_sec)
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, dict):
                    raise ValueError(f"telegram response is not an object for {method}")
                return data
            except Exception as exc:  # pragma: no cover - retried path
                last_error = exc
        assert last_error is not None
        raise last_error
    def _get(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.bot_base}/{method}"
        last_error: Optional[Exception] = None
        for _ in range(self.retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout_sec)
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, dict):
                    raise ValueError(f"telegram response is not an object for {method}")
                return data
            except Exception as exc:  # pragma: no cover - retried path
                last_error = exc
        assert last_error is not None
        raise last_error

    def get_updates(
        self,
        *,
        offset: Optional[int] = None,
        timeout_sec: int = 20,
        allowed_updates: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {"timeout": int(timeout_sec)}
        if offset is not None:
            payload["offset"] = int(offset)
        if allowed_updates:
            payload["allowed_updates"] = allowed_updates
        data = self._get("getUpdates", payload)
        result = data.get("result")
        return result if isinstance(result, list) else []

    def send_message(
        self,
        *,
        chat_id: str,
        text: str,
        reply_to_message_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"chat_id": chat_id, "text": text}
        if reply_to_message_id:
            try:
                payload["reply_to_message_id"] = int(reply_to_message_id)
            except Exception:
                payload["reply_to_message_id"] = reply_to_message_id
        return self._post("sendMessage", payload)

    def get_file(self, *, file_id: str) -> Dict[str, Any]:
        data = self._get("getFile", {"file_id": file_id})
        result = data.get("result")
        if not isinstance(result, dict):
            raise ValueError("telegram getFile result is not an object")
        return result

    def download_file(self, *, file_path: str) -> bytes:
        url = f"{self.file_base}/{file_path.lstrip('/')}"
        last_error: Optional[Exception] = None
        for _ in range(self.retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout_sec)
                resp.raise_for_status()
                return resp.content
            except Exception as exc:  # pragma: no cover - retried path
                last_error = exc
        assert last_error is not None
        raise last_error
