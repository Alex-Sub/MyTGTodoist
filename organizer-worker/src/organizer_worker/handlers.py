from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from organizer_worker import db
from organizer_worker import canon
from organizer_worker.time_legacy.aliases import TASK_STATUS_NORMALIZATION


HandlerResult = dict[str, Any]
HandlerFn = Callable[[dict[str, Any]], HandlerResult]

EXECUTE_CONFIDENCE = 0.75
CLARIFY_CONFIDENCE = 0.40


def build_clarification(
    question: str,
    choices: list[dict[str, Any]] | None = None,
    debug: dict[str, Any] | None = None,
) -> HandlerResult:
    out: HandlerResult = {
        "ok": False,
        "user_message": "Нужны уточнения.",
        "clarifying_question": question,
    }
    if choices:
        out["choices"] = choices
    if debug:
        out["debug"] = debug
    return out


def _entities(payload: dict[str, Any]) -> dict[str, Any]:
    # Accept either flat payload or command-parser shaped payload with "entities".
    ent = payload.get("entities")
    if isinstance(ent, dict):
        return ent
    return payload


def _need(field: str, question: str) -> HandlerResult:
    return build_clarification(question=question, debug={"missing": field})


def _ok(msg: str, **debug: Any) -> HandlerResult:
    out: HandlerResult = {"ok": True, "user_message": msg}
    if debug:
        out["debug"] = debug
    return out


def _fail(msg: str, **debug: Any) -> HandlerResult:
    out: HandlerResult = {"ok": False, "user_message": msg}
    if debug:
        out["debug"] = debug
    return out


def _safe_fail(**debug: Any) -> HandlerResult:
    out: HandlerResult = {
        "ok": False,
        "user_message": "Не могу выполнить. Уточните запрос.",
    }
    details = {"reason": "safe_fail"}
    if debug:
        details.update(debug)
    out["debug"] = details
    return out


def _parse_iso_utc(value: str) -> datetime:
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _with_minutes_iso(start_at: str, minutes: int) -> str:
    dt = _parse_iso_utc(start_at) + timedelta(minutes=minutes)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_status(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    alias = TASK_STATUS_NORMALIZATION.get(raw.lower(), raw.lower())
    return alias.upper()


def _normalized_datetime_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _resolve_task_id_or_question(e: dict[str, Any], conn: Any) -> tuple[int | None, HandlerResult | None]:
    top_k = canon.get_disambiguation_top_k()
    task_id = e.get("task_id")
    if task_id is not None:
        try:
            return int(task_id), None
        except Exception:
            return None, _need("task_id", "Нужен номер задачи.")

    task_ref = e.get("task_ref")
    if task_ref is None:
        return None, _need("task_ref", "Какую задачу изменить?")

    if isinstance(task_ref, dict):
        chosen_id = task_ref.get("chosen_id") or task_ref.get("task_id") or task_ref.get("id")
        if chosen_id is not None:
            try:
                return int(chosen_id), None
            except Exception:
                return None, _need("task_ref.chosen_id", "Нужен корректный номер задачи.")

        candidates = task_ref.get("candidates")
        if isinstance(candidates, list) and candidates:
            top = candidates[:top_k]
            choices = _choices_from_candidates(top)
            return None, build_clarification(
                question="Нашел несколько похожих задач. Какую именно выбрать?",
                choices=choices,
                debug={"missing": "task_ref.chosen_id", "candidates_top": top},
            )

        text_ref = task_ref.get("text") or task_ref.get("query") or ""
        task_ref = str(text_ref).strip()

    if not isinstance(task_ref, str) or not task_ref.strip():
        return None, _need("task_ref", "Какую задачу изменить?")

    if task_ref.strip().isdigit():
        return int(task_ref.strip()), None

    candidates = db.find_task_candidates(conn, task_ref=task_ref.strip(), limit=top_k)
    if not candidates:
        return None, _fail("Не нашел подходящую задачу.", task_ref=task_ref)
    if len(candidates) > 1:
        top = candidates[:top_k]
        choices = _choices_from_candidates(top)
        return None, build_clarification(
            question="Нашел несколько похожих задач. Какую именно выбрать?",
            choices=choices,
            debug={"missing": "task_ref.chosen_id", "candidates_top": top},
        )
    return int(candidates[0]["id"]), None


def _resolve_goal_id_or_question(e: dict[str, Any], conn: Any) -> tuple[int | None, HandlerResult | None]:
    top_k = canon.get_disambiguation_top_k()
    goal_id = e.get("goal_id")
    if goal_id is not None:
        try:
            return int(goal_id), None
        except Exception:
            return None, _need("goal_id", "Нужен номер цели.")

    goal_ref = e.get("goal_ref")
    if goal_ref is None:
        return None, _need("goal_ref", "Какую цель выбрать?")

    if isinstance(goal_ref, dict):
        chosen_id = goal_ref.get("chosen_id") or goal_ref.get("goal_id") or goal_ref.get("id")
        if chosen_id is not None:
            try:
                return int(chosen_id), None
            except Exception:
                return None, _need("goal_ref.chosen_id", "Нужен корректный номер цели.")
        candidates = goal_ref.get("candidates")
        if isinstance(candidates, list) and candidates:
            top = candidates[:top_k]
            return None, build_clarification(
                question=str(goal_ref.get("ask") or "Какую именно цель выбрать?"),
                choices=_choices_from_candidates(top),
                debug={"missing": "goal_ref.chosen_id", "candidates_top": top},
            )
        text_ref = goal_ref.get("query") or goal_ref.get("text") or ""
        goal_ref = str(text_ref).strip()

    if not isinstance(goal_ref, str) or not goal_ref.strip():
        return None, _need("goal_ref", "Какую цель выбрать?")
    if goal_ref.strip().isdigit():
        return int(goal_ref.strip()), None

    candidates = db.find_goal_candidates(conn, goal_ref=goal_ref.strip(), limit=top_k)
    if not candidates:
        return None, _fail("Не нашел подходящую цель.", goal_ref=goal_ref)
    if len(candidates) > 1:
        top = candidates[:top_k]
        return None, build_clarification(
            question="Нашел несколько похожих целей. Какую именно выбрать?",
            choices=_choices_from_candidates(top),
            debug={"missing": "goal_ref.chosen_id", "candidates_top": top},
        )
    return int(candidates[0]["id"]), None


def _choices_from_candidates(candidates: list[Any]) -> list[dict[str, Any]]:
    choices: list[dict[str, Any]] = []
    for c in candidates:
        if not isinstance(c, dict):
            continue
        cid = c.get("id")
        title = str(c.get("title") or "").strip()
        parsed_id: int | None = None
        if cid is not None:
            try:
                parsed_id = int(cid)
            except Exception:
                parsed_id = None
        if parsed_id is None:
            continue
        label = f"#{parsed_id} {title}".strip()
        choices.append({"id": parsed_id, "label": label})
    return sorted(choices, key=lambda x: (int(x["id"]), str(x["label"])))


def _canon_ref_disambiguation(intent: str, entities: dict[str, Any]) -> HandlerResult | None:
    spec = canon.get_intent_spec(intent) or {}
    dis = spec.get("disambiguation", {})
    if not isinstance(dis, dict):
        return None

    ref_path = dis.get("ref")
    if not isinstance(ref_path, str) or not ref_path.strip():
        return None

    ref_key = ref_path.removeprefix("entities.")
    ref_value = entities.get(ref_key)
    if not isinstance(ref_value, dict):
        return None

    chosen_id = ref_value.get("chosen_id")
    candidates = ref_value.get("candidates")
    if chosen_id is not None:
        return None
    if not isinstance(candidates, list) or not candidates:
        return None

    q = ref_value.get("ask")
    if not isinstance(q, str) or not q.strip():
        q = dis.get("question_fallback")
    if not isinstance(q, str) or not q.strip():
        q = canon.get_disambiguation_default_question()

    top_k = canon.get_disambiguation_top_k()
    top = candidates[:top_k]
    choices = _choices_from_candidates(top)
    return build_clarification(
        question=q,
        choices=choices,
        debug={"missing": f"{ref_key}.chosen_id", "candidates_top": top},
    )


def _read_confidence(cmd: dict[str, Any], payload: dict[str, Any]) -> float:
    raw = payload.get("confidence")
    if raw is None:
        raw = cmd.get("confidence")
    try:
        value = float(raw)
    except Exception:
        return 1.0
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def _is_rejected(cmd: dict[str, Any], payload: dict[str, Any]) -> bool:
    rejected = payload.get("rejected")
    if rejected is None:
        rejected = cmd.get("rejected")
    return rejected is True


def _choices_if_any(entities: dict[str, Any]) -> list[dict[str, Any]] | None:
    top_k = canon.get_disambiguation_top_k()
    for value in entities.values():
        if not isinstance(value, dict):
            continue
        if value.get("chosen_id") is not None:
            continue
        candidates = value.get("candidates")
        if not isinstance(candidates, list) or not candidates:
            continue
        choices = _choices_from_candidates(candidates[:top_k])
        if choices:
            return choices
    return None


def _normalize_intent_alias(intent: str) -> str:
    raw = (intent or "").strip()
    aliases = {
        "task_create": "task.create",
        "task_complete": "task.complete",
        "task_update": "task.update",
        "task_move": "task.move",
        "task_set_status": "task.set_status",
        "task_reschedule": "task.reschedule",
        "timeblock_create": "timeblock.create",
        "timeblock_move": "timeblock.move",
        "timeblock_delete": "timeblock.delete",
        "subtask_create": "subtask.create",
        "subtask_complete": "subtask.complete",
    }
    return aliases.get(raw, raw)


def task_create(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    title = (e.get("title") or "").strip()
    if not title:
        return _need("title", "Как назвать задачу?")
    planned_at = e.get("planned_at")  # ISO string (optional)
    source_msg_id = e.get("source_msg_id")

    try:
        with db.connect() as conn:
            task_id = db.create_task(conn, title=title, planned_at=planned_at, source_msg_id=source_msg_id)
        return _ok(f"Задача создана: #{task_id}.", task_id=task_id)
    except Exception as exc:
        return _fail("Не получилось создать задачу. Попробуйте еще раз.", error=str(exc))


def task_complete(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    try:
        with db.connect() as conn:
            task_id, question = _resolve_task_id_or_question(e, conn)
            if question is not None:
                return question
            assert task_id is not None
            db.complete_task(conn, task_id=task_id)
        return _ok("Готово. Отметил задачу выполненной.", task_id=task_id)
    except Exception as exc:
        return _fail("Не получилось завершить задачу. Проверьте номер.", error=str(exc))


def task_move(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    # Backward compatibility: old task.move routes to canonical task.set_status.
    mapped = dict(e)
    if mapped.get("status") is None and mapped.get("state") is not None:
        mapped["status"] = mapped.get("state")
    result = task_set_status({"entities": mapped})
    if isinstance(result.get("debug"), dict):
        result["debug"]["deprecated_intent"] = "task.move"
    return result


def task_set_status(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    status = _normalize_status(e.get("status"))
    if status is None:
        return _need("status", "Какой статус поставить задаче?")

    try:
        with db.connect() as conn:
            task_id, question = _resolve_task_id_or_question(e, conn)
            if question is not None:
                return question
            assert task_id is not None
            db.update_task(conn, task_id=task_id, status=status, state=status)
        return _ok("Готово. Обновил статус задачи.", task_id=task_id, status=status)
    except Exception as exc:
        return _fail("Не получилось обновить статус задачи. Проверьте данные.", error=str(exc))


def task_reschedule(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    when = (
        _normalized_datetime_value(e.get("when"))
        or _normalized_datetime_value(e.get("planned_at"))
        or _normalized_datetime_value(e.get("start_at"))
    )
    if when is None:
        return _need("when", "На когда перенести задачу?")

    try:
        with db.connect() as conn:
            task_id, question = _resolve_task_id_or_question(e, conn)
            if question is not None:
                return question
            assert task_id is not None
            db.update_task(conn, task_id=task_id, planned_at=str(when))
        return _ok("Готово. Перенес задачу.", task_id=task_id, when=str(when))
    except Exception as exc:
        return _fail("Не получилось перенести задачу. Проверьте данные.", error=str(exc))


def task_move_to(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    target_ref = e.get("target_ref")
    target_type = e.get("target_type") or e.get("parent_type")
    target_id = e.get("target_id") if e.get("target_id") is not None else e.get("parent_id")
    if isinstance(target_ref, dict):
        target_type = target_type or target_ref.get("type") or target_ref.get("parent_type")
        if target_id is None:
            target_id = target_ref.get("id") or target_ref.get("parent_id")
    if target_type is None or target_id is None:
        return _need("target", "Куда перенести задачу?")

    try:
        with db.connect() as conn:
            task_id, question = _resolve_task_id_or_question(e, conn)
            if question is not None:
                return question
            assert task_id is not None
            db.update_task(conn, task_id=task_id, parent_type=str(target_type), parent_id=int(target_id))
        return _ok("Готово. Перенес задачу.", task_id=task_id, parent_type=str(target_type), parent_id=int(target_id))
    except Exception as exc:
        return _fail("Не получилось перенести задачу. Проверьте данные.", error=str(exc))


def task_update(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    title = e.get("title")
    planned_at = e.get("planned_at")
    status = e.get("status")
    state = e.get("state")
    parent_type = e.get("parent_type")
    parent_id = e.get("parent_id")

    # If nothing to update, ask.
    if all(v is None for v in (title, planned_at, status, state, parent_type, parent_id)):
        return _need("fields", "Что именно обновить в задаче?")

    try:
        with db.connect() as conn:
            task_id, question = _resolve_task_id_or_question(e, conn)
            if question is not None:
                return question
            assert task_id is not None
            db.update_task(
                conn,
                task_id=task_id,
                title=(str(title).strip() if isinstance(title, str) else None),
                planned_at=(str(planned_at) if planned_at is not None else None),
                status=(_normalize_status(status) if status is not None else None),
                state=(_normalize_status(state) if state is not None else None),
                parent_type=(str(parent_type) if parent_type is not None else None),
                parent_id=(int(parent_id) if parent_id is not None else None),
            )
        return _ok("Готово. Обновил задачу.", task_id=task_id)
    except Exception as exc:
        return _fail("Не получилось обновить задачу. Проверьте данные.", error=str(exc))


def subtask_create(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    task_id = e.get("task_id")
    title = (e.get("title") or "").strip()
    if task_id is None:
        return _need("task_id", "К какой задаче добавить подзадачу? Пришлите номер задачи.")
    if not title:
        return _need("title", "Как назвать подзадачу?")

    try:
        with db.connect() as conn:
            sub_id = db.create_subtask(conn, task_id=int(task_id), title=title, source_msg_id=e.get("source_msg_id"))
        return _ok(f"Подзадача создана: #{sub_id}.", subtask_id=sub_id, task_id=int(task_id))
    except Exception as exc:
        return _fail("Не получилось создать подзадачу. Попробуйте еще раз.", error=str(exc))


def subtask_complete(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    subtask_id = e.get("subtask_id")
    if subtask_id is None:
        return _need("subtask_id", "Какую подзадачу завершить? Пришлите номер.")

    try:
        with db.connect() as conn:
            db.complete_subtask(conn, subtask_id=int(subtask_id))
        return _ok("Готово. Отметил подзадачу выполненной.", subtask_id=int(subtask_id))
    except Exception as exc:
        return _fail("Не получилось завершить подзадачу. Проверьте номер.", error=str(exc), subtask_id=subtask_id)


def timeblock_create(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    start_at = _normalized_datetime_value(e.get("start_at"))
    duration_min = e.get("duration_min")
    if not start_at:
        return _need("start_at", "На какое время поставить блок?")
    if duration_min is None:
        return _need("duration_min", "На сколько минут поставить блок?")

    try:
        with db.connect() as conn:
            task_id, question = _resolve_task_id_or_question(e, conn)
            if question is not None:
                return question
            assert task_id is not None
            end_value = _with_minutes_iso(str(start_at), int(duration_min))
            tb_id = db.create_time_block(conn, task_id=task_id, start_at=str(start_at), end_at=str(end_value))
        return _ok("Блок времени создан.", time_block_id=tb_id, task_id=task_id)
    except Exception as exc:
        return _fail("Не получилось создать блок времени. Проверьте данные.", error=str(exc))


def timeblock_move(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    tb_id = e.get("time_block_id")
    if tb_id is None:
        return _need("time_block_id", "Какой блок времени изменить? Пришлите номер блока.")

    start_at = _normalized_datetime_value(e.get("start_at"))
    end_at = _normalized_datetime_value(e.get("end_at"))
    task_id = e.get("task_id")
    if start_at is None and end_at is None and task_id is None:
        return _need("fields", "Что изменить в блоке времени?")

    try:
        with db.connect() as conn:
            db.move_time_block(
                conn,
                time_block_id=int(tb_id),
                start_at=(str(start_at) if start_at is not None else None),
                end_at=(str(end_at) if end_at is not None else None),
                task_id=(int(task_id) if task_id is not None else None),
            )
        return _ok("Готово. Обновил блок времени.", time_block_id=int(tb_id))
    except Exception as exc:
        return _fail("Не получилось обновить блок времени. Проверьте данные.", error=str(exc))


def timeblock_delete(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    tb_id = e.get("time_block_id")
    if tb_id is None:
        return _need("time_block_id", "Какой блок времени удалить? Пришлите номер блока.")

    try:
        with db.connect() as conn:
            db.delete_time_block(conn, time_block_id=int(tb_id))
        return _ok("Блок времени удален.", time_block_id=int(tb_id))
    except Exception as exc:
        return _fail("Не получилось удалить блок времени. Проверьте номер.", error=str(exc))


def reg_run(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    regulation_id = e.get("regulation_id")
    period_key = e.get("period_key")
    status = e.get("status")
    due_date = e.get("due_date")
    due_time_local = e.get("due_time_local")

    if regulation_id is None:
        return _need("regulation_id", "Какое правило выполнить? Пришлите номер правила.")
    if not period_key:
        return _need("period_key", "За какой период? Пришлите ключ периода.")
    if not status:
        return _need("status", "Какой статус поставить? Например DONE или SKIPPED.")
    if not due_date:
        return _need("due_date", "На какую дату? Пришлите дату.")

    status_norm = str(status).strip().upper()
    done_at = None
    if status_norm == "DONE":
        done_at = None  # runtime may fill later; keep optional

    try:
        with db.connect() as conn:
            run_id = db.upsert_regulation_run(
                conn,
                regulation_id=int(regulation_id),
                period_key=str(period_key),
                status=status_norm,
                due_date=str(due_date),
                due_time_local=(str(due_time_local) if due_time_local is not None else None),
                done_at=done_at,
            )
        return _ok("Готово.", regulation_run_id=run_id, regulation_id=int(regulation_id))
    except Exception as exc:
        return _fail("Не получилось выполнить правило. Попробуйте еще раз.", error=str(exc))


def reg_status(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    regulation_id = e.get("regulation_id")
    limit = int(e.get("limit") or 10)

    try:
        with db.connect() as conn:
            rows = db.list_regulation_runs(conn, regulation_id=(int(regulation_id) if regulation_id is not None else None), limit=limit)
        if not rows:
            return _ok("Пока нет запусков правил.", regulation_id=regulation_id)
        return _ok("Статус правил обновлен.", regulation_id=regulation_id, count=len(rows))
    except Exception as exc:
        return _fail("Не получилось получить статус правил.", error=str(exc))


def state_get(payload: dict[str, Any]) -> HandlerResult:
    try:
        with db.connect() as conn:
            st = db.get_state(conn)
        return _ok(
            "Состояние обновлено.",
            tasks_total=st.tasks_total,
            subtasks_total=st.subtasks_total,
            time_blocks_total=st.time_blocks_total,
            regulations_total=st.regulations_total,
            regulation_runs_total=st.regulation_runs_total,
            queue_total=st.queue_total,
            cycles_total=st.cycles_total,
            goals_total=st.goals_total,
            nudges_total=st.nudges_total,
        )
    except Exception as exc:
        return _fail("Не получилось получить состояние.", error=str(exc))


def cycle_create(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    name = (e.get("name") or e.get("title") or "").strip()
    start_date = e.get("start_date") or e.get("date_from")
    end_date = e.get("end_date") or e.get("date_to")
    if not name:
        return _need("name", "Как назвать цикл?")
    if not start_date:
        return _need("start_date", "Какая дата начала цикла?")
    if not end_date:
        return _need("end_date", "Какая дата конца цикла?")

    try:
        with db.connect() as conn:
            cycle_id = db.create_cycle(conn, name=name, start_date=str(start_date), end_date=str(end_date))
        return _ok("Цикл создан.", cycle_id=cycle_id)
    except Exception as exc:
        return _fail("Не получилось создать цикл. Попробуйте еще раз.", error=str(exc))


def cycle_set_active(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    cycle_id = e.get("cycle_id")
    if cycle_id is None:
        return _need("cycle_id", "Какой цикл сделать активным? Пришлите номер.")

    try:
        with db.connect() as conn:
            db.cycles_set_active(conn, cycle_id=int(cycle_id))
        return _ok("Готово. Сделал цикл активным.", cycle_id=int(cycle_id))
    except Exception as exc:
        return _fail("Не получилось сделать цикл активным. Проверьте номер.", error=str(exc), cycle_id=cycle_id)


def cycle_close(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    cycle_id = e.get("cycle_id")
    if cycle_id is None:
        return _need("cycle_id", "Какой цикл закрыть? Пришлите номер.")

    try:
        with db.connect() as conn:
            summary = db.close_cycle(conn, cycle_id=int(cycle_id))
        return _ok("Цикл закрыт.", cycle_id=int(cycle_id), summary=summary)
    except Exception as exc:
        return _fail("Не получилось закрыть цикл. Проверьте номер.", error=str(exc), cycle_id=cycle_id)


def goal_create(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    cycle_id = e.get("cycle_id")
    title = (e.get("title") or "").strip()
    success_criteria = (e.get("success_criteria") or "").strip()
    planned_end_date = e.get("planned_end_date")
    if cycle_id is None:
        # default to active cycle
        with db.connect() as conn:
            active = db.get_active_cycle(conn)
        if active is not None:
            cycle_id = active.get("id")
    if cycle_id is None:
        return _need("cycle_id", "Для какого цикла добавить цель?")
    if not title:
        return _need("title", "Как назвать цель?")
    if not success_criteria:
        return _need("success_criteria", "По какому критерию понять, что цель достигнута?")
    if not planned_end_date:
        return _need("planned_end_date", "До какой даты запланирована цель?")

    try:
        with db.connect() as conn:
            goal_id = db.create_goal(
                conn,
                cycle_id=int(cycle_id),
                title=title,
                success_criteria=success_criteria,
                planned_end_date=str(planned_end_date),
            )
        return _ok("Цель добавлена.", goal_id=goal_id, cycle_id=int(cycle_id))
    except Exception as exc:
        return _fail("Не получилось добавить цель. Проверьте данные.", error=str(exc))


def goal_update(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    fields: dict[str, Any] = {}
    if e.get("title") is not None:
        fields["title"] = str(e.get("title")).strip()
    if e.get("success_criteria") is not None:
        fields["success_criteria"] = str(e.get("success_criteria")).strip()
    if e.get("planned_end_date") is not None:
        fields["planned_end_date"] = str(e.get("planned_end_date")).strip()
    if e.get("status") is not None:
        fields["status"] = str(e.get("status")).strip().upper()
    if not fields:
        return _need("fields", "Что изменить в цели?")

    try:
        with db.connect() as conn:
            goal_id, question = _resolve_goal_id_or_question(e, conn)
            if question is not None:
                return question
            assert goal_id is not None
            db.update_goal(conn, goal_id=goal_id, fields=fields)
        return _ok("Готово. Обновил цель.", goal_id=goal_id)
    except Exception as exc:
        return _fail("Не получилось обновить цель. Проверьте данные.", error=str(exc))

def goal_reschedule(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    new_end_date = e.get("new_end_date")
    if not new_end_date:
        return _need("new_end_date", "На какую дату перенести цель?")

    try:
        with db.connect() as conn:
            goal_id, question = _resolve_goal_id_or_question(e, conn)
            if question is not None:
                return question
            assert goal_id is not None
            event_id = db.reschedule_goal(conn, goal_id=goal_id, new_end_date=str(new_end_date))
        return _ok("Перенес срок цели.", goal_id=goal_id, event_id=event_id)
    except Exception as exc:
        return _fail("Не получилось перенести срок цели.", error=str(exc))


def goal_link_task(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    try:
        with db.connect() as conn:
            goal_id, gq = _resolve_goal_id_or_question(e, conn)
            if gq is not None:
                return gq
            task_id, tq = _resolve_task_id_or_question(e, conn)
            if tq is not None:
                return tq
            assert goal_id is not None and task_id is not None
            db.link_task_to_goal(conn, task_id=task_id, goal_id=goal_id)
        return _ok("Привязал задачу к цели.", task_id=task_id, goal_id=goal_id)
    except Exception as exc:
        return _fail("Не получилось привязать задачу к цели.", error=str(exc))


def goal_close(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    close_as = str(e.get("close_as") or "DONE").strip().upper()
    if close_as not in {"DONE", "DROPPED"}:
        return _need("close_as", "Закрыть как DONE или DROPPED?")

    try:
        with db.connect() as conn:
            goal_id, question = _resolve_goal_id_or_question(e, conn)
            if question is not None:
                return question
            assert goal_id is not None
            db.close_goal(conn, goal_id=goal_id, close_as=close_as)
        return _ok("Цель закрыта.", goal_id=goal_id, close_as=close_as)
    except Exception as exc:
        return _fail("Не получилось закрыть цель. Проверьте номер.", error=str(exc))


def nudge_list(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    user_id = str(e.get("user_id") or "default")
    today = str(e.get("today") or datetime.now(timezone.utc).date().isoformat())

    try:
        with db.connect() as conn:
            rows = db.list_nudges(conn, user_id=user_id, today=today)
        if not rows:
            return _ok("Сейчас ничего не нужно.", nudges=[])
        return _ok("Есть подсказки.", nudges=rows, count=len(rows))
    except Exception as exc:
        return _fail("Не получилось получить подсказки.", error=str(exc))


def nudge_ack(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    user_id = str(e.get("user_id") or "default")
    nudge_id = e.get("nudge_id")
    nudge_type = e.get("nudge_type")
    entity_type = e.get("entity_type")
    entity_id = e.get("entity_id")

    if nudge_id and (nudge_type is None or entity_type is None or entity_id is None):
        parts = str(nudge_id).split(":")
        if len(parts) == 3:
            nudge_type, entity_type, entity_id = parts[0], parts[1], parts[2]
    if nudge_type is None or entity_type is None or entity_id is None:
        return _need("nudge_id", "Какую подсказку отметить?")

    try:
        with db.connect() as conn:
            db.ack_nudge(
                conn,
                user_id=user_id,
                nudge_type=str(nudge_type),
                entity_type=str(entity_type),
                entity_id=int(entity_id),
            )
        return _ok("Хорошо. Учту.", nudge_type=str(nudge_type), entity_type=str(entity_type), entity_id=int(entity_id))
    except Exception as exc:
        return _fail("Не получилось отметить подсказку. Попробуйте еще раз.", error=str(exc))


def digest_daily(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    today = str(e.get("today") or datetime.now(timezone.utc).date().isoformat())
    tomorrow = str(e.get("tomorrow") or (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat())
    user_id = str(e.get("user_id") or "default")
    try:
        with db.connect() as conn:
            digest = db.compute_daily_digest(conn, today=today, tomorrow=tomorrow, user_id=user_id)
        return _ok("Сводка готова.", digest=digest)
    except Exception as exc:
        return _fail("Не получилось собрать сводку.", error=str(exc))


def tasks_list_today(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    today = str(e.get("today") or datetime.now(timezone.utc).date().isoformat())
    limit = int(e.get("limit") or 50)
    try:
        with db.connect() as conn:
            tasks = db.list_tasks_today(conn, today=today, limit=limit)
        return _ok("Список задач на сегодня готов.", tasks=tasks, count=len(tasks), today=today)
    except Exception as exc:
        return _fail("Не получилось получить задачи на сегодня.", error=str(exc))


def tasks_list_tomorrow(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    tomorrow = str(e.get("tomorrow") or (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat())
    limit = int(e.get("limit") or 50)
    try:
        with db.connect() as conn:
            tasks = db.list_tasks_tomorrow(conn, tomorrow=tomorrow, limit=limit)
        return _ok("Список задач на завтра готов.", tasks=tasks, count=len(tasks), tomorrow=tomorrow)
    except Exception as exc:
        return _fail("Не получилось получить задачи на завтра.", error=str(exc))


def tasks_list_active(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    limit = int(e.get("limit") or 100)
    try:
        with db.connect() as conn:
            tasks = db.list_tasks_active(conn, limit=limit)
        return _ok("Список активных задач готов.", tasks=tasks, count=len(tasks))
    except Exception as exc:
        return _fail("Не получилось получить активные задачи.", error=str(exc))


def goals_list_overdue(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    today = str(e.get("today") or datetime.now(timezone.utc).date().isoformat())
    limit = int(e.get("limit") or 20)
    try:
        with db.connect() as conn:
            goals = db.list_goals_overdue(conn, today=today, limit=limit)
        return _ok("Список просроченных целей готов.", goals=goals, count=len(goals), today=today)
    except Exception as exc:
        return _fail("Не получилось получить просроченные цели.", error=str(exc))


def goals_list_due_soon(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    today = str(e.get("today") or datetime.now(timezone.utc).date().isoformat())
    tomorrow = str(e.get("tomorrow") or (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat())
    limit = int(e.get("limit") or 20)
    try:
        with db.connect() as conn:
            goals = db.list_goals_due_soon(conn, today=today, tomorrow=tomorrow, limit=limit)
        return _ok("Список целей со сроком сегодня/завтра готов.", goals=goals, count=len(goals), today=today, tomorrow=tomorrow)
    except Exception as exc:
        return _fail("Не получилось получить цели со сроком сегодня/завтра.", error=str(exc))


def goals_list_at_risk(payload: dict[str, Any]) -> HandlerResult:
    e = _entities(payload)
    today = str(e.get("today") or datetime.now(timezone.utc).date().isoformat())
    limit = int(e.get("limit") or 20)
    try:
        with db.connect() as conn:
            goals = db.list_goals_at_risk(conn, today=today, limit=limit)
        return _ok("Список целей под риском готов.", goals=goals, count=len(goals), today=today)
    except Exception as exc:
        return _fail("Не получилось получить цели под риском.", error=str(exc))


INTENT_HANDLERS: dict[str, HandlerFn] = {
    "task.create": task_create,
    "task.complete": task_complete,
    "task.move": task_move,
    "task.set_status": task_set_status,
    "task.reschedule": task_reschedule,
    "task.move_to": task_move_to,
    "task.update": task_update,
    "subtask.create": subtask_create,
    "subtask.complete": subtask_complete,
    "timeblock.create": timeblock_create,
    "timeblock.move": timeblock_move,
    "timeblock.delete": timeblock_delete,
    "reg.run": reg_run,
    "reg.status": reg_status,
    "cycle.create": cycle_create,
    "cycle.close": cycle_close,
    "goal.create": goal_create,
    "goal.update": goal_update,
    "goal.close": goal_close,
    "goal.reschedule": goal_reschedule,
    "goal.link_task": goal_link_task,
    "nudge.list": nudge_list,
    "nudge.ack": nudge_ack,
    "digest.daily": digest_daily,
    "tasks.list_today": tasks_list_today,
    "tasks.list_tomorrow": tasks_list_tomorrow,
    "tasks.list_active": tasks_list_active,
    "goals.list_overdue": goals_list_overdue,
    "goals.list_due_soon": goals_list_due_soon,
    "goals.list_at_risk": goals_list_at_risk,
    "state.get": state_get,
}


def dispatch(intent: str, payload: dict[str, Any]) -> HandlerResult:
    fn = INTENT_HANDLERS.get(intent)
    if fn is None:
        return _fail("Я пока не умею выполнять эту команду.", intent=intent)
    return fn(payload)


def dispatch_intent(cmd: dict[str, Any]) -> HandlerResult:
    # Supports either {"intent": "...", "entities": {...}} or {"command": {"intent": "...", ...}} shapes.
    intent = cmd.get("intent")
    payload: dict[str, Any] = cmd
    if not isinstance(intent, str) or not intent.strip():
        command = cmd.get("command")
        if isinstance(command, dict):
            intent = command.get("intent")
            payload = command

    if not isinstance(intent, str) or not intent.strip():
        return _fail("Я не понял команду. Сформулируйте иначе.", reason="missing_intent")
    intent_norm = _normalize_intent_alias(intent)
    entities = _entities(payload)
    confidence = _read_confidence(cmd, payload)
    rejected = _is_rejected(cmd, payload)

    if rejected or confidence < CLARIFY_CONFIDENCE:
        return _safe_fail(confidence=confidence, rejected=rejected)

    # Canon v2: if candidates were provided and choice is not made, ask one clarifying question.
    dis = _canon_ref_disambiguation(intent_norm, entities)
    if dis is not None:
        return dis

    if confidence < EXECUTE_CONFIDENCE:
        question = canon.build_one_question(intent_norm, entities) or "Подтвердите, что именно нужно сделать."
        return build_clarification(question=question, choices=_choices_if_any(entities), debug={"confidence": confidence})

    # Canon v2: centralized required-field validation and one-question clarification.
    missing = canon.validate_required(intent_norm, entities)
    if missing:
        question = canon.build_one_question(intent_norm, entities) or "Нужны уточнения."
        return build_clarification(question=question, debug={"missing": missing})

    return dispatch(intent_norm, payload)
