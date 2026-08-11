from __future__ import annotations

from typing import Any, Dict

import requests


class RecClient:
    def __init__(self, base_url: str, *, timeout_sec: float = 10.0) -> None:
        self.base_url = (base_url or "").rstrip("/")
        self.timeout_sec = float(timeout_sec)

    @staticmethod
    def _normalize_body(body: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(body, dict):
            raise TypeError("rec query body must be a dict")

        payload = dict(body)
        text = payload.get("text")
        query = payload.get("query")

        if isinstance(text, str) and text.strip():
            normalized_text = text.strip()
        elif isinstance(query, str) and query.strip():
            normalized_text = query.strip()
        else:
            raise ValueError("rec query body must include non-empty 'text' or 'query'")

        payload["text"] = normalized_text
        payload.setdefault("query", normalized_text)

        command = payload.get("command")
        if command is not None and not isinstance(command, dict):
            raise ValueError("rec query body field 'command' must be a dict when provided")

        return payload

    def query(self, body: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._normalize_body(body)
        r = requests.post(f"{self.base_url}/query", json=payload, timeout=self.timeout_sec)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, dict):
            raise ValueError("rec query response must be a JSON object")
        return data
