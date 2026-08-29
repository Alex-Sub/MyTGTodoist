"""Domain-facing handler for the versioned Organizer runtime command contract."""

from __future__ import annotations

from typing import Any, Callable

from shared_runtime import (
    begin_command,
    finish_command,
    idempotency_key,
    release_command,
    request_hash,
)


class RuntimeRetryableError(RuntimeError):
    """Raised when the caller may safely retry without a domain result."""


def _result_identity(result: dict[str, Any]) -> dict[str, str] | None:
    entity_type = str(result.get("entity_type") or "").strip()
    entity_id = str(result.get("entity_id") or "").strip()
    if entity_type and entity_id:
        return {"entity_type": entity_type, "entity_id": entity_id}
    return None


def _duplicate_response(record: dict[str, Any], *, key: str, trace_id: str) -> dict[str, Any]:
    prior = record.get("response")
    if not isinstance(prior, dict):
        return {
            "ok": False,
            "outcome": "retryable_failure",
            "retryable": True,
            "error": "request_in_progress",
            "idempotency_key": key,
            "trace_id": trace_id,
        }
    response = dict(prior)
    response.update(
        {
            "outcome": "duplicate",
            "duplicate": True,
            "idempotency_key": key,
            "trace_id": trace_id,
        }
    )
    return response


def _success_response(
    *,
    result: dict[str, Any],
    key: str,
    trace_id: str,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "ok": True,
        "outcome": "succeeded",
        "duplicate": False,
        "idempotency_key": key,
        "trace_id": trace_id,
        "result": result.get("data", result),
    }
    identity = _result_identity(result)
    if identity is not None:
        response["result_identity"] = identity
    return response


def handle_runtime_command(
    payload: dict[str, Any],
    *,
    db_path: str,
    dispatch_intent: Callable[[str, dict[str, Any]], dict[str, Any]],
    recover_result: Callable[[str, dict[str, Any]], dict[str, Any] | None],
) -> tuple[int, dict[str, Any]]:
    if not isinstance(payload, dict):
        raise ValueError("request body must be object")
    trace_id = payload.get("trace_id")
    if not isinstance(trace_id, str) or not trace_id.strip():
        raise ValueError("trace_id is required")
    trace_id = trace_id.strip()

    source = payload.get("source")
    if source is not None and not isinstance(source, dict):
        raise ValueError("source must be object")
    command = payload.get("command")
    if not isinstance(command, dict):
        raise ValueError("command is required")
    intent = command.get("intent")
    if not isinstance(intent, str) or not intent.strip():
        raise ValueError("command.intent is required")
    intent = intent.strip()
    entities = command.get("entities")
    if not isinstance(entities, dict):
        raise ValueError("command.entities must be object")

    key = idempotency_key(payload)
    payload_hash = request_hash(payload)
    record = begin_command(db_path, key=key, intent=intent, payload_hash=payload_hash)
    existing_hash = str(record.get("request_hash") or "")
    if existing_hash and existing_hash != payload_hash:
        return 409, {
            "ok": False,
            "outcome": "rejected",
            "error": "idempotency_conflict",
            "idempotency_key": key,
            "trace_id": trace_id,
        }

    if record.get("state") in {"succeeded", "rejected"}:
        return 200, _duplicate_response(record, key=key, trace_id=trace_id)
    if record.get("state") == "processing" and record.get("response") is None and record.get("request_hash"):
        recovered = recover_result(intent, entities)
        if isinstance(recovered, dict):
            response = _success_response(result=recovered, key=key, trace_id=trace_id)
            finish_command(db_path, key=key, response=response)
            return 200, _duplicate_response({"response": response}, key=key, trace_id=trace_id)
        if record.get("state") != "new":
            return 409, {
                "ok": False,
                "outcome": "retryable_failure",
                "retryable": True,
                "error": "request_in_progress",
                "idempotency_key": key,
                "trace_id": trace_id,
            }

    dispatch_entities = dict(entities)
    if isinstance(source, dict):
        user_id = str(source.get("user_id") or "").strip()
        if user_id and not str(dispatch_entities.get("user_id") or "").strip():
            dispatch_entities["user_id"] = user_id

    try:
        result = dispatch_intent(intent, dispatch_entities)
    except ValueError as exc:
        response = {
            "ok": False,
            "outcome": "rejected",
            "error": str(exc)[:200],
            "idempotency_key": key,
            "trace_id": trace_id,
        }
        finish_command(db_path, key=key, response=response)
        return 422, response
    except Exception as exc:  # pragma: no cover - exercised by server safety test
        release_command(db_path, key=key)
        raise RuntimeRetryableError("runtime dispatch failed") from exc

    if not isinstance(result, dict):
        release_command(db_path, key=key)
        raise RuntimeRetryableError("runtime returned invalid result")
    response = _success_response(result=result, key=key, trace_id=trace_id)
    finish_command(db_path, key=key, response=response)
    return 200, response
