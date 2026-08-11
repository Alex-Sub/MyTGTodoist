from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


def _utc_iso(ts: Optional[float] = None) -> str:
    t = time.time() if ts is None else float(ts)
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


@dataclass
class TraceLoggerCfg:
    path: str
    max_bytes: int = 50 * 1024 * 1024  # 50MB
    max_age_days: int = 7


class TraceLogger:
    def __init__(self, cfg: TraceLoggerCfg) -> None:
        self.cfg = cfg
        os.makedirs(os.path.dirname(os.path.abspath(cfg.path)), exist_ok=True)

    def _rotate_if_needed(self) -> None:
        path = self.cfg.path
        try:
            st = os.stat(path)
        except FileNotFoundError:
            return

        too_big = st.st_size > int(self.cfg.max_bytes)
        too_old = False
        try:
            age_sec = time.time() - float(st.st_mtime)
            too_old = age_sec > int(self.cfg.max_age_days) * 86400
        except Exception:
            too_old = False

        if not (too_big or too_old):
            return

        ts = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
        rotated = f"{path}.{ts}.log"
        try:
            os.replace(path, rotated)
        except Exception:
            # If rotation fails, don't crash app.
            return

    def write_trace(self, trace: Dict[str, Any]) -> None:
        self._rotate_if_needed()
        row = dict(trace)
        row.setdefault("ts_utc", _utc_iso())
        line = json.dumps(row, ensure_ascii=False)
        with open(self.cfg.path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
