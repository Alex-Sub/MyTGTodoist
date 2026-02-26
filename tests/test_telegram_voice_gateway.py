import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = ROOT / "telegram-bot" / "bot.py"

spec = importlib.util.spec_from_file_location("telegram_bot_module", BOT_PATH)
assert spec is not None and spec.loader is not None
telegram_bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(telegram_bot)


def test_build_runtime_envelope_from_ml_happy_path() -> None:
    response = {
        "transcript": "создай задачу купить молоко",
        "transcript_norm": "создай задачу купить молоко",
        "command": {
            "intent": "create_task",
            "title": "Купить молоко",
            "details": "2 литра",
            "when": "2026-02-24T10:00:00+01:00",
            "priority": "high",
            "tags": ["home"],
            "assignees": ["alexey"],
        },
    }
    envelope, err = telegram_bot._build_runtime_envelope_from_ml_response(
        chat_id=123,
        update_id=456,
        message_id=789,
        response_json=response,
    )
    assert err is None
    assert envelope is not None
    assert envelope["command"]["intent"] == "task.create"
    entities = envelope["command"]["entities"]
    assert entities["title"] == "Купить молоко"
    assert entities["planned_at"] == "2026-02-24T10:00:00+01:00"
    assert entities["source_msg_id"] == "tg:123:789"


def test_ml_gateway_timeout_returns_asr_timeout(monkeypatch) -> None:
    class FakeRequests:
        def post(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise TimeoutError("ml timeout")

    monkeypatch.setattr(telegram_bot, "_require_requests", lambda: FakeRequests())
    monkeypatch.setattr(telegram_bot, "ML_CORE_URL", "http://ml.local")
    payload, err = telegram_bot._ml_gateway_voice_command(
        b"abc",
        filename="voice.ogg",
        mime_type="audio/ogg",
    )
    assert payload is None
    assert err == "asr_timeout"


def test_build_runtime_envelope_empty_transcript_returns_asr_empty() -> None:
    response = {
        "transcript": "",
        "transcript_norm": "   ",
        "command": {"intent": "create_task", "title": "Любая задача"},
    }
    envelope, err = telegram_bot._build_runtime_envelope_from_ml_response(
        chat_id=1,
        update_id=2,
        message_id=3,
        response_json=response,
    )
    assert envelope is None
    assert err == "asr_empty"


def test_handle_voice_timeout_returns_retry_message(monkeypatch) -> None:
    sent: list[str] = []
    monkeypatch.setattr(telegram_bot, "_send_message", lambda _chat_id, text: sent.append(text))
    monkeypatch.setattr(telegram_bot, "_tg_get_file_path", lambda _file_id: "voice/file.ogg")
    monkeypatch.setattr(telegram_bot, "_tg_download_file", lambda _path: (b"abc", "voice.ogg", "audio/ogg"))
    monkeypatch.setattr(
        telegram_bot,
        "_ml_gateway_voice_command",
        lambda _audio, **_kwargs: (None, "asr_timeout"),
    )
    message = {"chat": {"id": 42}, "voice": {"file_id": "f1"}, "message_id": 7}
    telegram_bot._handle_voice_message(update_id=1001, message=message)
    assert sent == ["Не получилось разобрать речь. Попробуй сказать ещё раз."]


def test_handle_voice_invalid_json_returns_later_message(monkeypatch) -> None:
    sent: list[str] = []
    monkeypatch.setattr(telegram_bot, "_send_message", lambda _chat_id, text: sent.append(text))
    monkeypatch.setattr(telegram_bot, "_tg_get_file_path", lambda _file_id: "voice/file.ogg")
    monkeypatch.setattr(telegram_bot, "_tg_download_file", lambda _path: (b"abc", "voice.ogg", "audio/ogg"))
    monkeypatch.setattr(
        telegram_bot,
        "_ml_gateway_voice_command",
        lambda _audio, **_kwargs: (None, "llm_invalid_output"),
    )
    message = {"chat": {"id": 42}, "voice": {"file_id": "f1"}, "message_id": 7}
    telegram_bot._handle_voice_message(update_id=1002, message=message)
    assert sent == ["Я сейчас не могу корректно обработать запрос. Давай попробуем позже."]


def test_handle_voice_clarify_creates_pending_and_asks_question(monkeypatch) -> None:
    sent: list[tuple[int, str, dict]] = []
    stored: dict[str, object] = {}
    monkeypatch.setattr(telegram_bot, "_tg_get_file_path", lambda _file_id: "voice/file.ogg")
    monkeypatch.setattr(telegram_bot, "_tg_download_file", lambda _path: (b"abc", "voice.ogg", "audio/ogg"))
    monkeypatch.setattr(
        telegram_bot,
        "_ml_gateway_voice_command",
        lambda _audio, **_kwargs: (
            {
                "type": "clarify",
                "clarifying_question": "Что выбрать: создать блок времени или создать задачу?",
                "choices": [
                    {
                        "id": "timeblock_create",
                        "title": "Поставить блок времени",
                        "patch": {"command": {"intent": "timeblock.create"}},
                    },
                    {
                        "id": "task_create",
                        "title": "Создать задачу",
                        "patch": {"command": {"intent": "task.create"}},
                    },
                ],
                "draft_envelope": {
                    "trace_id": "tr-1",
                    "source": {"channel": "telegram_voice"},
                    "command": {"intent": "task.create", "entities": {"title": "Созвон завтра"}},
                },
            },
            None,
        ),
    )
    monkeypatch.setattr(telegram_bot, "_pending_clarify_put", lambda uid, payload: stored.update({"uid": uid, "payload": payload}))
    monkeypatch.setattr(
        telegram_bot,
        "_send_message_with_keyboard",
        lambda chat_id, text, reply_markup: sent.append((chat_id, text, reply_markup)),
    )
    monkeypatch.setattr(telegram_bot, "_worker_post_ex", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("runtime must not be called")))

    message = {"chat": {"id": 42}, "from": {"id": 4242}, "voice": {"file_id": "f1"}, "message_id": 7}
    telegram_bot._handle_voice_message(update_id=1101, message=message)

    assert stored.get("uid") == 4242
    pending = stored.get("payload")
    assert isinstance(pending, dict)
    assert pending.get("clarifying_question") == "Что выбрать: создать блок времени или создать задачу?"
    assert sent
    assert sent[0][0] == 42
    assert sent[0][1] == "Что выбрать: создать блок времени или создать задачу?"


def test_pending_text_choice_applies_patch_and_routes_to_runtime(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(telegram_bot, "PENDING_CLARIFY_PATH", str(tmp_path / "pending.json"))
    telegram_bot._pending_clarify_state = {
        "99": {
            "pending_id": "p-1",
            "trace_id": "tr-99",
            "channel": "telegram_voice",
            "user_id": "99",
            "created_at": "2026-02-26T00:00:00Z",
            "expires_at": "2099-01-01T00:00:00Z",
            "clarifying_question": "Что выбрать?",
            "choices": [
                {"id": "timeblock_create", "title": "Поставить блок времени", "patch": {"command": {"intent": "timeblock.create"}}},
                {"id": "task_create", "title": "Создать задачу", "patch": {"command": {"intent": "task.create"}}},
            ],
            "draft_envelope": {
                "trace_id": "tr-99",
                "source": {"channel": "telegram_voice"},
                "command": {"intent": "task.create", "entities": {"title": "Созвон завтра"}},
            },
            "stage": "llm_disambiguation",
        }
    }
    posted: list[dict] = []
    sent: list[str] = []
    monkeypatch.setattr(telegram_bot, "_queue_depths", lambda: (0, 0))
    monkeypatch.setattr(telegram_bot, "_enqueue_text", lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not enqueue while pending exists")))
    monkeypatch.setattr(telegram_bot, "_p2_handle_text", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("p2 flow must not run while pending exists")))
    monkeypatch.setattr(
        telegram_bot,
        "_worker_post_ex",
        lambda path, payload: (posted.append({"path": path, "payload": payload}) or True, {"ok": True, "user_message": "OK"}, 200, None),
    )
    monkeypatch.setattr(telegram_bot, "_send_message", lambda _chat_id, text: sent.append(text))

    message = {"chat": {"id": 99}, "from": {"id": 99}, "message_id": 5, "text": "блок"}
    telegram_bot._handle_text_message(update_id=2001, message=message, pending_state={})

    assert posted
    assert posted[0]["path"] == "/runtime/command"
    assert posted[0]["payload"]["command"]["intent"] == "timeblock.create"
    assert "source_msg_id" in posted[0]["payload"]["command"]["entities"]
    assert "99" not in telegram_bot._pending_clarify_state
    assert sent == ["OK"]


def test_expired_pending_is_pruned_and_message_goes_to_normal_flow(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(telegram_bot, "PENDING_CLARIFY_PATH", str(tmp_path / "pending.json"))
    telegram_bot._pending_clarify_state = {
        "77": {
            "pending_id": "p-expired",
            "trace_id": "tr-77",
            "channel": "telegram_text",
            "user_id": "77",
            "created_at": "2020-01-01T00:00:00Z",
            "expires_at": "2020-01-01T00:00:01Z",
            "clarifying_question": "Выбор?",
            "choices": [{"id": "task_create", "title": "Создать задачу", "patch": {"command": {"intent": "task.create"}}}],
            "draft_envelope": None,
            "stage": "llm_disambiguation",
        }
    }
    enq_calls: list[dict] = []
    sent: list[str] = []
    monkeypatch.setattr(telegram_bot, "_queue_depths", lambda: (0, 0))
    monkeypatch.setattr(
        telegram_bot,
        "_enqueue_text",
        lambda **kwargs: (enq_calls.append(kwargs) or True, 1),
    )
    monkeypatch.setattr(telegram_bot, "_p2_handle_text", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(telegram_bot, "_send_message", lambda _chat_id, text: sent.append(text))

    message = {"chat": {"id": 77}, "from": {"id": 77}, "message_id": 11, "text": "купи молоко"}
    telegram_bot._handle_text_message(update_id=2002, message=message, pending_state={})

    assert enq_calls
    assert sent == ["Принято. В очереди: 1."]
    assert "77" not in telegram_bot._pending_clarify_state


def test_pending_unknown_text_replies_choose_one(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(telegram_bot, "PENDING_CLARIFY_PATH", str(tmp_path / "pending.json"))
    telegram_bot._pending_clarify_state = {
        "55": {
            "pending_id": "p-55",
            "trace_id": "tr-55",
            "channel": "telegram_text",
            "user_id": "55",
            "created_at": "2026-02-26T00:00:00Z",
            "expires_at": "2099-01-01T00:00:00Z",
            "clarifying_question": "Что выбрать?",
            "choices": [
                {"id": "task_create", "title": "Создать задачу", "patch": {"command": {"intent": "task.create"}}},
                {"id": "timeblock_create", "title": "Поставить блок времени", "patch": {"command": {"intent": "timeblock.create"}}},
            ],
            "draft_envelope": None,
            "stage": "llm_disambiguation",
        }
    }
    sent_kb: list[tuple[int, str, dict]] = []
    monkeypatch.setattr(telegram_bot, "_send_message_with_keyboard", lambda chat_id, text, reply_markup: sent_kb.append((chat_id, text, reply_markup)))
    monkeypatch.setattr(telegram_bot, "_queue_depths", lambda: (_ for _ in ()).throw(AssertionError("must not enqueue")))
    monkeypatch.setattr(telegram_bot, "_worker_post_ex", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("runtime must not be called")))

    message = {"chat": {"id": 55}, "from": {"id": 55}, "message_id": 9, "text": "что?"}
    telegram_bot._handle_text_message(update_id=2003, message=message, pending_state={})

    assert sent_kb
    assert sent_kb[0][1] == "Не понял выбор. Пожалуйста, выберите один из вариантов:"
    assert "55" in telegram_bot._pending_clarify_state


def test_text_list_active_rule_routes_to_runtime_without_inbox_enqueue(monkeypatch) -> None:
    sent: list[str] = []
    runtime_calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        telegram_bot,
        "_enqueue_text",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not enqueue list-intent text into inbox_queue")),
    )
    monkeypatch.setattr(
        telegram_bot,
        "_queue_depths",
        lambda: (_ for _ in ()).throw(AssertionError("must not check queue depths for list-intent text")),
    )
    monkeypatch.setattr(
        telegram_bot,
        "_p2_handle_text",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("p2 flow must not run for list-intent text")),
    )
    monkeypatch.setattr(telegram_bot, "_handle_pending_clarification_text", lambda **_kwargs: False)
    monkeypatch.setattr(
        telegram_bot,
        "_worker_post_ex",
        lambda path, payload: (
            runtime_calls.append((path, payload)) or True,
            {"ok": True, "debug": {"tasks": []}},
            200,
            None,
        ),
    )
    monkeypatch.setattr(telegram_bot, "_send_message", lambda _chat_id, text: sent.append(text))

    message = {"chat": {"id": 77}, "from": {"id": 77}, "message_id": 12, "text": "список активных задач"}
    telegram_bot._handle_text_message(update_id=3001, message=message, pending_state={})

    assert runtime_calls
    assert runtime_calls[0][0] == "/runtime/command"
    assert runtime_calls[0][1]["command"]["intent"] == "tasks.list_active"
    assert runtime_calls[0][1]["command"]["intent"] != "task.create"
    assert runtime_calls[0][1]["command"]["entities"] == {}
    assert sent == ["Список пуст."]


def test_text_list_tomorrow_rule_routes_to_runtime_without_inbox_enqueue(monkeypatch) -> None:
    sent: list[str] = []
    runtime_calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        telegram_bot,
        "_enqueue_text",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not enqueue list-intent text into inbox_queue")),
    )
    monkeypatch.setattr(
        telegram_bot,
        "_queue_depths",
        lambda: (_ for _ in ()).throw(AssertionError("must not check queue depths for list-intent text")),
    )
    monkeypatch.setattr(
        telegram_bot,
        "_p2_handle_text",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("p2 flow must not run for list-intent text")),
    )
    monkeypatch.setattr(telegram_bot, "_handle_pending_clarification_text", lambda **_kwargs: False)
    monkeypatch.setattr(
        telegram_bot,
        "_worker_post_ex",
        lambda path, payload: (
            runtime_calls.append((path, payload)) or True,
            {"ok": True, "debug": {"tasks": []}},
            200,
            None,
        ),
    )
    monkeypatch.setattr(telegram_bot, "_send_message", lambda _chat_id, text: sent.append(text))

    message = {"chat": {"id": 78}, "from": {"id": 78}, "message_id": 13, "text": "выведи задачи на завтра"}
    telegram_bot._handle_text_message(update_id=3002, message=message, pending_state={})

    assert runtime_calls
    assert runtime_calls[0][0] == "/runtime/command"
    assert runtime_calls[0][1]["command"]["intent"] == "tasks.list_tomorrow"
    assert runtime_calls[0][1]["command"]["intent"] != "task.create"
    assert runtime_calls[0][1]["command"]["entities"] == {}
    assert sent == ["Список пуст."]


def test_text_regular_task_still_goes_to_inbox_queue(monkeypatch) -> None:
    enq_calls: list[dict] = []
    sent: list[str] = []
    monkeypatch.setattr(telegram_bot, "_handle_pending_clarification_text", lambda **_kwargs: False)
    monkeypatch.setattr(telegram_bot, "_queue_depths", lambda: (0, 0))
    monkeypatch.setattr(
        telegram_bot,
        "_enqueue_text",
        lambda **kwargs: (enq_calls.append(kwargs) or True, 1),
    )
    monkeypatch.setattr(telegram_bot, "_p2_handle_text", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(telegram_bot, "_send_message", lambda _chat_id, text: sent.append(text))
    monkeypatch.setattr(
        telegram_bot,
        "_worker_runtime_command",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("runtime list command must not run for regular task text")),
    )

    message = {"chat": {"id": 79}, "from": {"id": 79}, "message_id": 14, "text": "купи молоко"}
    telegram_bot._handle_text_message(update_id=3003, message=message, pending_state={})

    assert enq_calls
    assert sent == ["Принято. В очереди: 1."]


def test_format_runtime_reply_renders_tasks_list() -> None:
    payload = {
        "ok": True,
        "user_message": "Список задач на завтра готов.",
        "debug": {
            "tasks": [
                {
                    "id": 10,
                    "title": "Подготовить встречу",
                    "planned_at": "2026-02-27T09:00:00+03:00",
                    "status": "IN_PROGRESS",
                    "parent_id": None,
                    "level": 0,
                },
                {
                    "id": 11,
                    "title": "Сделать отчёт",
                    "planned_at": None,
                    "status": "NEW",
                    "parent_id": 10,
                    "level": 1,
                },
            ]
        },
    }
    text = telegram_bot._format_runtime_reply(payload)
    assert "1. #10 Подготовить встречу" in text
    assert "2. #11 Сделать отчёт" in text


def test_format_runtime_reply_empty_tasks_returns_list_empty() -> None:
    payload = {"ok": True, "user_message": "Список активных задач готов.", "debug": {"tasks": []}}
    text = telegram_bot._format_runtime_reply(payload)
    assert text == "Список пуст."
