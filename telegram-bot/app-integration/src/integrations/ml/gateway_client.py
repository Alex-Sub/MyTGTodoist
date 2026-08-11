from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


@dataclass(frozen=True)
class VoiceCommandResult:
    raw: Dict[str, Any]


class GatewayClient:
    def __init__(self, base_url: str, *, timeout_sec: float = 120.0, api_key: str = "") -> None:
        self.base_url = (base_url or "").rstrip("/")
        self.timeout_sec = float(timeout_sec)
        self.api_key = api_key

    def voice_command(self, wav_path: str, *, profile_id: str = "", timezone: str = "UTC") -> VoiceCommandResult:
        p = pathlib.Path(wav_path)
        if not p.exists():
            raise FileNotFoundError(str(p))
        headers = {"X-Timezone": timezone}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        params = {}
        if profile_id:
            params["profile"] = profile_id
        with p.open("rb") as f:
            files = {"file": (p.name, f, "audio/wav")}
            r = requests.post(f"{self.base_url}/voice-command", params=params, files=files, headers=headers, timeout=self.timeout_sec)
        r.raise_for_status()
        return VoiceCommandResult(raw=r.json())
