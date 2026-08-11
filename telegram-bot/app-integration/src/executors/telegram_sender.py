from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

import requests


@dataclass(frozen=True)
class TelegramSender:
    base_url: str
    timeout_sec: float = 10.0

    def send_message(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = requests.post(f"{self.base_url.rstrip('/')}/send", json=payload, timeout=float(self.timeout_sec))
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, dict) else {"raw": data}
