from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

import requests


@dataclass(frozen=True)
class ExternalApiClient:
    base_url: str
    timeout_sec: float = 10.0

    def post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url.rstrip('/')}/{str(path or '').lstrip('/')}"
        r = requests.post(url, json=payload, timeout=float(self.timeout_sec))
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, dict) else {"raw": data}
