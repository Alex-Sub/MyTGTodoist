import logging
from datetime import datetime
from typing import Any
from common.date_resolver import normalize_temporal_fields, resolve_date_phrase


def _normalize_worker_command_dates(command: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not isinstance(command, dict):
        return command, None
    entities = command.get("entities")
    entities = dict(entities) if isinstance(entities, dict) else {}
    normalized = normalize_temporal_fields(
        {
            **command,
            "entities": entities,
            "text": entities.get("text") or command.get("text") or "",
        },
        now=datetime.now(),
    )
    normalized_entities = normalized.get("entities")
    normalized_entities = dict(normalized_entities) if isinstance(normalized_entities, dict) else entities
    normalized["entities"] = normalized_entities

    intent = str(normalized.get("intent") or "").strip().lower()
    if intent == "task.create":
        for field in ("planned_at", "due_date", "date", "when"):
            raw_value = str(
                normalized_entities.get(field)
                or normalized.get(field)
                or normalized_entities.get(f"__raw_{field}")
                or normalized.get(f"__raw_{field}")
                or ""
            ).strip()
            if not raw_value:
                continue
            resolution = resolve_date_phrase(raw_value, now=datetime.now())
            if resolution.ok:
                continue
            if resolution.needs_clarification:
                return normalized, {
                    "ok": False,
                    "outcome": "needs_clarification",
                    "needs_clarification": True,
                    "missing_field": "task_create_due_date",
                    "clarifying_question": "На какую дату поставить задачу?",
                    "user_message": "На какую дату поставить задачу?",
                    "debug": {"date_resolution_reason": resolution.reason or "unresolved_date_phrase", "field": field},
                }
    return normalized, None


def handle_runtime_command(payload: dict[str, Any], *, deps: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    trace_id = payload.get("trace_id")
    if not isinstance(trace_id, str) or not trace_id.strip():
        raise ValueError("trace_id is required")
    trace_id = trace_id.strip()
    flow_id = deps["runtime_flow_id"](payload, trace_id)
    source_timezone = deps["runtime_source_timezone"](payload)
    command = payload.get("command")
    if not isinstance(command, dict):
        raise ValueError("command is required")
    intent = command.get("intent")
    if not isinstance(intent, str) or not intent.strip():
        raise ValueError("command.intent is required")
    entities = command.get("entities")
    if not isinstance(entities, dict):
        raise ValueError("command.entities must be object")

    logging.info(
        "runtime_command_in trace_id=%s flow_id=%s intent=%s timezone=%s entities=%s",
        trace_id,
        flow_id,
        intent,
        source_timezone,
        entities,
    )

    cached = deps["runtime_trace_dedup_get"](trace_id)
    if isinstance(cached, dict):
        logging.info(
            "runtime_command_dedup_hit trace_id=%s flow_id=%s intent=%s dedup=trace_cache",
            trace_id,
            flow_id,
            intent,
        )
        return 200, cached

    intent_key = deps["normalize_intent_for_idempotency"](intent)
    idempotency_key = deps["runtime_idempotency_key"](
        {
            "trace_id": trace_id,
            "source": payload.get("source"),
            "idempotency_key": payload.get("idempotency_key"),
            "command": {"intent": intent, "entities": entities},
        }
    )
    if idempotency_key:
        dedup_row = deps["command_dedup_get"](idempotency_key)
        if isinstance(dedup_row, dict):
            duplicate = deps["duplicate_execution_response"](
                dedup_row,
                idempotency_key=idempotency_key,
            )
            deps["runtime_trace_dedup_put"](trace_id, duplicate)
            logging.info(
                "runtime_command_idempotent_hit trace_id=%s flow_id=%s key=%s intent=%s",
                trace_id,
                flow_id,
                idempotency_key,
                intent_key or intent,
            )
            return 200, duplicate
        logging.info(
            "runtime_command_idempotent_miss trace_id=%s flow_id=%s key=%s intent=%s dedup=miss",
            trace_id,
            flow_id,
            idempotency_key,
            intent_key or intent,
        )

    dispatch_command = dict(command)
    dispatch_entities = dict(entities)
    dispatch_entities.setdefault("__flow_id", flow_id)
    source = payload.get("source")
    source = source if isinstance(source, dict) else {}
    source_user_id = str(source.get("user_id") or "").strip()
    if source_user_id and not str(dispatch_entities.get("user_id") or "").strip():
        dispatch_entities["user_id"] = source_user_id
    dispatch_command["entities"] = dispatch_entities
    dispatch_command, clarification = _normalize_worker_command_dates(dispatch_command)
    if clarification is not None:
        return 200, clarification
    dispatch_entities = dict(dispatch_command.get("entities") or dispatch_entities)

    commit_path = "runtime_state_only"
    if intent_key in {"timeblock.create", "meeting.create"}:
        commit_path = "calendar_create"
    elif intent_key == "meeting.update":
        commit_path = "calendar_patch"
    elif intent_key == "meeting.comment.update":
        commit_path = "calendar_patch_description"
    logging.info(
        "runtime_intent_dispatch_start trace_id=%s flow_id=%s intent=%s commit_path=%s",
        trace_id,
        flow_id,
        intent_key or intent,
        commit_path,
    )

    res = deps["dispatch_intent"](dispatch_command)
    logging.info(
        "runtime_intent_dispatch_result trace_id=%s flow_id=%s intent=%s ok=%s commit_path=%s",
        trace_id,
        flow_id,
        intent_key or intent,
        bool(isinstance(res, dict) and res.get("ok")),
        commit_path,
    )
    if (
        intent_key in {"timeblock.create", "meeting.create"}
        and isinstance(res, dict)
        and bool(res.get("ok"))
    ):
        res = deps["runtime_commit_temporal_calendar"](
            flow_id=flow_id,
            intent=str(intent),
            entities=dispatch_entities,
            execution_result=res,
            source_timezone=source_timezone,
        )
        logging.info(
            "runtime_commit_path_result trace_id=%s flow_id=%s intent=%s commit_path=%s ok=%s",
            trace_id,
            flow_id,
            intent_key or intent,
            commit_path,
            bool(isinstance(res, dict) and res.get("ok")),
        )
    elif (
        intent_key == "meeting.update"
        and isinstance(res, dict)
        and bool(res.get("ok"))
    ):
        res = deps["runtime_commit_meeting_update_calendar"](
            flow_id=flow_id,
            entities=dispatch_entities,
            execution_result=res,
        )
        logging.info(
            "runtime_commit_path_result trace_id=%s flow_id=%s intent=%s commit_path=%s ok=%s",
            trace_id,
            flow_id,
            intent_key or intent,
            commit_path,
            bool(isinstance(res, dict) and res.get("ok")),
        )
    elif (
        intent_key == "meeting.comment.update"
        and isinstance(res, dict)
        and bool(res.get("ok"))
    ):
        res = deps["runtime_commit_meeting_comment_update_calendar"](
            flow_id=flow_id,
            entities=dispatch_entities,
            execution_result=res,
        )
        logging.info(
            "runtime_commit_path_result trace_id=%s flow_id=%s intent=%s commit_path=%s ok=%s",
            trace_id,
            flow_id,
            intent_key or intent,
            commit_path,
            bool(isinstance(res, dict) and res.get("ok")),
        )

    if (
        idempotency_key
        and isinstance(res, dict)
        and bool(res.get("ok"))
    ):
        entity_type, entity_id = deps["command_entity_ref"](res)
        deps["command_dedup_put"](
            idempotency_key,
            intent=(intent_key or intent),
            entity_type=entity_type,
            entity_id=entity_id,
        )

    if isinstance(res, dict):
        trace_payload = deps["runtime_build_trace_snapshot"](
            trace_id=trace_id,
            intent=(intent_key or intent),
            entities=dispatch_entities,
            response_payload=res,
            user_id=str(dispatch_entities.get("user_id") or source_user_id or ""),
        )
        deps["runtime_trace_dedup_put"](trace_id, trace_payload)

    return 200, res
