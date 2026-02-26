from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .types import Choice, CommandEnvelope, PendingClarification


def _state_path() -> Path:
    env = os.getenv("LLM_PENDING_CLARIFY_PATH", "").strip()
    if env:
        return Path(env)
    if Path("/data").exists():
        return Path("/data/llm.pending_clarifications.json")
    return Path("./data/llm.pending_clarifications.json")


def _deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in patch.items():
        cur = out.get(key)
        if isinstance(cur, dict) and isinstance(value, dict):
            out[key] = _deep_merge(cur, value)
        else:
            out[key] = value
    return out


def _normalize_user_id(value: str | int) -> str:
    return str(value).strip()


def resolve_pending_answer(user_text: str, pending: PendingClarification) -> CommandEnvelope | None:
    text = str(user_text or "").strip().lower()
    if not text:
        return None

    selected: Choice | None = None
    if text in {"1", "2"}:
        idx = int(text) - 1
        if 0 <= idx < len(pending.choices):
            selected = pending.choices[idx]
    if selected is None:
        for ch in pending.choices:
            if ch.id.strip().lower() == text:
                selected = ch
                break
    if selected is None:
        for ch in pending.choices:
            title = ch.title.strip().lower()
            if text and text in title:
                selected = ch
                break
    if selected is None:
        return None

    base = pending.draft_envelope.to_dict() if pending.draft_envelope is not None else {
        "trace_id": pending.trace_id,
        "source": {"channel": pending.channel},
        "command": {"intent": selected.id, "confidence": 1.0, "entities": {}},
    }
    merged = _deep_merge(base, selected.patch)
    return CommandEnvelope.from_dict(merged)


@dataclass(slots=True)
class PendingClarificationStore:
    path: Path

    @classmethod
    def default(cls) -> "PendingClarificationStore":
        return cls(path=_state_path())

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            with self.path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save(self, state: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, self.path)

    def _prune(self, state: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, value in state.items():
            if not isinstance(value, dict):
                continue
            try:
                pending = PendingClarification.from_dict(value)
            except Exception:
                continue
            if pending.is_expired():
                continue
            out[key] = pending.to_dict()
        return out

    def put(self, pending: PendingClarification) -> None:
        pending.validate()
        state = self._prune(self._load())
        key = f"{pending.channel}:{_normalize_user_id(pending.user_id)}"
        state[key] = pending.to_dict()
        self._save(state)

    def get(self, *, channel: str, user_id: str | int) -> PendingClarification | None:
        state = self._prune(self._load())
        key = f"{channel}:{_normalize_user_id(user_id)}"
        raw = state.get(key)
        self._save(state)
        if not isinstance(raw, dict):
            return None
        try:
            return PendingClarification.from_dict(raw)
        except Exception:
            return None

    def clear(self, *, channel: str, user_id: str | int) -> None:
        state = self._prune(self._load())
        key = f"{channel}:{_normalize_user_id(user_id)}"
        state.pop(key, None)
        self._save(state)

    def apply_choice(
        self,
        *,
        channel: str,
        user_id: str | int,
        choice_id: str,
    ) -> CommandEnvelope | None:
        pending = self.get(channel=channel, user_id=user_id)
        if pending is None:
            return None
        out = resolve_pending_answer(choice_id, pending)
        if out is None:
            return None
        # consume-once: user answer is clarification, not new command
        self.clear(channel=channel, user_id=user_id)
        return out
