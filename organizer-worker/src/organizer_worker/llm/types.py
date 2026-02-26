from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4


InterpretationType = Literal["command", "clarify"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_iso(value: str) -> str:
    v = str(value).strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class CommandBody:
    intent: str
    confidence: float = 1.0
    entities: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        if not isinstance(self.intent, str) or not self.intent.strip():
            raise ValueError("command.intent is required")
        if not isinstance(self.entities, dict):
            raise ValueError("command.entities must be object")
        try:
            c = float(self.confidence)
        except Exception as exc:  # pragma: no cover - defensive
            raise ValueError("command.confidence must be number") from exc
        if c < 0 or c > 1:
            raise ValueError("command.confidence must be in [0..1]")
        self.confidence = c

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "intent": self.intent.strip(),
            "confidence": self.confidence,
            "entities": self.entities,
        }


@dataclass(slots=True)
class CommandEnvelope:
    trace_id: str
    source: dict[str, Any]
    command: CommandBody

    def validate(self) -> None:
        if not isinstance(self.trace_id, str) or not self.trace_id.strip():
            raise ValueError("trace_id is required")
        if not isinstance(self.source, dict):
            raise ValueError("source must be object")
        if not isinstance(self.command, CommandBody):
            raise ValueError("command must be CommandBody")
        self.command.validate()

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "trace_id": self.trace_id.strip(),
            "source": self.source,
            "command": self.command.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CommandEnvelope":
        if not isinstance(payload, dict):
            raise ValueError("envelope must be object")
        command_raw = payload.get("command")
        if not isinstance(command_raw, dict):
            raise ValueError("command is required")
        env = cls(
            trace_id=str(payload.get("trace_id") or "").strip(),
            source=(payload.get("source") if isinstance(payload.get("source"), dict) else {}),
            command=CommandBody(
                intent=str(command_raw.get("intent") or "").strip(),
                confidence=float(command_raw.get("confidence", 1.0)),
                entities=(command_raw.get("entities") if isinstance(command_raw.get("entities"), dict) else {}),
            ),
        )
        env.validate()
        return env

    @classmethod
    def new(
        cls,
        *,
        intent: str,
        entities: dict[str, Any] | None = None,
        source: dict[str, Any] | None = None,
        trace_id: str | None = None,
        confidence: float = 1.0,
    ) -> "CommandEnvelope":
        return cls(
            trace_id=(trace_id or f"llm:{uuid4().hex}"),
            source=(source or {"channel": "llm_gateway"}),
            command=CommandBody(intent=intent, confidence=confidence, entities=(entities or {})),
        )


@dataclass(slots=True)
class Choice:
    id: str
    title: str
    patch: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        if not isinstance(self.id, str) or not self.id.strip():
            raise ValueError("choice.id is required")
        if not isinstance(self.title, str) or not self.title.strip():
            raise ValueError("choice.title is required")
        if not isinstance(self.patch, dict):
            raise ValueError("choice.patch must be object")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {"id": self.id.strip(), "title": self.title.strip(), "patch": self.patch}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "Choice":
        if not isinstance(payload, dict):
            raise ValueError("choice must be object")
        out = cls(
            id=str(payload.get("id") or "").strip(),
            title=str(payload.get("title") or "").strip(),
            patch=(payload.get("patch") if isinstance(payload.get("patch"), dict) else {}),
        )
        out.validate()
        return out


@dataclass(slots=True)
class InterpretationResult:
    type: InterpretationType
    envelope: CommandEnvelope | None = None
    clarifying_question: str | None = None
    choices: list[Choice] = field(default_factory=list)
    draft_envelope: CommandEnvelope | None = None
    expected_answer: str | None = None
    debug: dict[str, Any] | None = None

    def validate(self) -> None:
        if self.type not in {"command", "clarify"}:
            raise ValueError("type must be command|clarify")
        if self.type == "command":
            if self.envelope is None:
                raise ValueError("command interpretation requires envelope")
            self.envelope.validate()
            return
        if not isinstance(self.clarifying_question, str) or not self.clarifying_question.strip():
            raise ValueError("clarify interpretation requires clarifying_question")
        for ch in self.choices:
            ch.validate()
        if self.draft_envelope is not None:
            self.draft_envelope.validate()

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        if self.type == "command":
            return {
                "type": "command",
                "envelope": self.envelope.to_dict() if self.envelope is not None else None,
                "debug": self.debug,
            }
        return {
            "type": "clarify",
            "clarifying_question": str(self.clarifying_question or "").strip(),
            "choices": [c.to_dict() for c in self.choices],
            "draft_envelope": self.draft_envelope.to_dict() if self.draft_envelope is not None else None,
            "expected_answer": self.expected_answer,
            "debug": self.debug,
        }

    @classmethod
    def command_result(
        cls, envelope: CommandEnvelope, *, debug: dict[str, Any] | None = None
    ) -> "InterpretationResult":
        return cls(type="command", envelope=envelope, debug=debug)

    @classmethod
    def clarify_result(
        cls,
        *,
        clarifying_question: str,
        choices: list[Choice] | None = None,
        draft_envelope: CommandEnvelope | None = None,
        expected_answer: str | None = "choice_id",
        debug: dict[str, Any] | None = None,
    ) -> "InterpretationResult":
        return cls(
            type="clarify",
            clarifying_question=clarifying_question,
            choices=(choices or []),
            draft_envelope=draft_envelope,
            expected_answer=expected_answer,
            debug=debug,
        )


@dataclass(slots=True)
class PendingClarification:
    pending_id: str
    trace_id: str
    channel: str
    user_id: str | int
    created_at: str
    expires_at: str
    clarifying_question: str
    choices: list[Choice] = field(default_factory=list)
    draft_envelope: CommandEnvelope | None = None
    stage: str = "llm_disambiguation"

    def validate(self) -> None:
        if not isinstance(self.pending_id, str) or not self.pending_id.strip():
            raise ValueError("pending_id is required")
        if not isinstance(self.trace_id, str) or not self.trace_id.strip():
            raise ValueError("trace_id is required")
        if not isinstance(self.channel, str) or not self.channel.strip():
            raise ValueError("channel is required")
        if not isinstance(self.clarifying_question, str) or not self.clarifying_question.strip():
            raise ValueError("clarifying_question is required")
        _normalize_iso(self.created_at)
        _normalize_iso(self.expires_at)
        for ch in self.choices:
            ch.validate()
        if self.draft_envelope is not None:
            self.draft_envelope.validate()

    def is_expired(self, now_iso: str | None = None) -> bool:
        now = _normalize_iso(now_iso or _now_iso())
        return _normalize_iso(self.expires_at) <= now

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "pending_id": self.pending_id,
            "trace_id": self.trace_id,
            "channel": self.channel,
            "user_id": self.user_id,
            "created_at": _normalize_iso(self.created_at),
            "expires_at": _normalize_iso(self.expires_at),
            "clarifying_question": self.clarifying_question,
            "choices": [c.to_dict() for c in self.choices],
            "draft_envelope": self.draft_envelope.to_dict() if self.draft_envelope is not None else None,
            "stage": self.stage,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PendingClarification":
        if not isinstance(payload, dict):
            raise ValueError("pending payload must be object")
        choices_raw = payload.get("choices")
        choices = []
        if isinstance(choices_raw, list):
            for raw in choices_raw:
                if isinstance(raw, dict):
                    choices.append(Choice.from_dict(raw))
        draft_raw = payload.get("draft_envelope")
        draft = CommandEnvelope.from_dict(draft_raw) if isinstance(draft_raw, dict) else None
        out = cls(
            pending_id=str(payload.get("pending_id") or "").strip(),
            trace_id=str(payload.get("trace_id") or "").strip(),
            channel=str(payload.get("channel") or "").strip(),
            user_id=payload.get("user_id"),
            created_at=str(payload.get("created_at") or "").strip(),
            expires_at=str(payload.get("expires_at") or "").strip(),
            clarifying_question=str(payload.get("clarifying_question") or "").strip(),
            choices=choices,
            draft_envelope=draft,
            stage=str(payload.get("stage") or "llm_disambiguation"),
        )
        out.validate()
        return out

    @classmethod
    def new(
        cls,
        *,
        trace_id: str,
        channel: str,
        user_id: str | int,
        clarifying_question: str,
        choices: list[Choice],
        draft_envelope: CommandEnvelope | None,
        stage: str = "llm_disambiguation",
        ttl_sec: int = 300,
    ) -> "PendingClarification":
        now = datetime.now(timezone.utc)
        expires = datetime.fromtimestamp(now.timestamp() + int(ttl_sec), tz=timezone.utc)
        return cls(
            pending_id=uuid4().hex,
            trace_id=trace_id,
            channel=channel,
            user_id=user_id,
            created_at=now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            expires_at=expires.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            clarifying_question=clarifying_question,
            choices=choices,
            draft_envelope=draft_envelope,
            stage=stage,
        )
