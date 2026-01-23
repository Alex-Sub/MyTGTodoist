from __future__ import annotations

import os
from typing import Any, Optional

import httpx


class LLMClient:
    def __init__(self) -> None:
        self.base_url = os.getenv("LLM_URL", "").rstrip("/")
        self.api_key = os.getenv("LLM_API_KEY", "")
        self.timeout = float(os.getenv("LLM_TIMEOUT_SECONDS", "20"))
        self.model = os.getenv("LLM_MODEL", "qwen2.5-3b-instruct-q5_0")
        self.enabled = bool(self.base_url)

    async def chat_completions(self, messages: list[dict[str, str]]) -> Optional[str]:
        if not self.enabled:
            return None

        url = f"{self.base_url}/chat/completions"
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-KEY"] = self.api_key

        payload: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.0,
            "max_tokens": 300,
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()
                return data["choices"][0]["message"]["content"]
        except Exception:
            return None
