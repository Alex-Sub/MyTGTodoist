from __future__ import annotations

from pathlib import Path

from organizer_worker.llm.pending import PendingClarificationStore, resolve_pending_answer
from organizer_worker.llm.types import Choice, CommandEnvelope, PendingClarification


def test_pending_clarification_apply_choice_consumes_pending(tmp_path: Path, monkeypatch) -> None:
    state_path = tmp_path / "llm.pending.json"
    monkeypatch.setenv("LLM_PENDING_CLARIFY_PATH", str(state_path))
    store = PendingClarificationStore.default()

    draft = CommandEnvelope.new(
        trace_id="tr-1",
        source={"channel": "telegram_voice"},
        intent="task_create",
        entities={"title": "Созвон завтра"},
        confidence=0.9,
    )
    pending = PendingClarification.new(
        trace_id="tr-1",
        channel="telegram_voice",
        user_id=42,
        clarifying_question="Что выбрать: создать блок времени или создать задачу?",
        choices=[
            Choice(
                id="timeblock_create",
                title="Поставить блок времени",
                patch={"command": {"intent": "timeblock_create"}},
            ),
            Choice(
                id="task_create",
                title="Создать задачу",
                patch={"command": {"intent": "task_create"}},
            ),
        ],
        draft_envelope=draft,
        stage="llm_disambiguation",
        ttl_sec=120,
    )
    store.put(pending)

    applied = store.apply_choice(channel="telegram_voice", user_id=42, choice_id="timeblock_create")
    assert applied is not None
    payload = applied.to_dict()
    assert payload["command"]["intent"] == "timeblock_create"
    # pending is consumed; user reply is treated as clarification continuation
    assert store.get(channel="telegram_voice", user_id=42) is None


def _sample_pending() -> PendingClarification:
    draft = CommandEnvelope.new(
        trace_id="tr-1",
        source={"channel": "telegram_text"},
        intent="task_create",
        entities={"title": "Созвон завтра"},
        confidence=0.9,
    )
    return PendingClarification.new(
        trace_id="tr-1",
        channel="telegram_text",
        user_id=1,
        clarifying_question="Что выбрать?",
        choices=[
            Choice(
                id="task_create",
                title="Создать задачу",
                patch={"command": {"intent": "task_create"}},
            ),
            Choice(
                id="timeblock_create",
                title="Поставить блок времени",
                patch={"command": {"intent": "timeblock_create"}},
            ),
        ],
        draft_envelope=draft,
        ttl_sec=120,
    )


def test_resolve_pending_answer_with_number_1_returns_first_choice() -> None:
    pending = _sample_pending()
    out = resolve_pending_answer("1", pending)
    assert out is not None
    assert out.to_dict()["command"]["intent"] == "task_create"


def test_resolve_pending_answer_with_partial_title_block_returns_timeblock() -> None:
    pending = _sample_pending()
    out = resolve_pending_answer("блок", pending)
    assert out is not None
    assert out.to_dict()["command"]["intent"] == "timeblock_create"


def test_resolve_pending_answer_unknown_returns_none() -> None:
    pending = _sample_pending()
    out = resolve_pending_answer("что?", pending)
    assert out is None
