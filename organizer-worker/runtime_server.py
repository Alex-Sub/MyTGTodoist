"""HTTP boundary helpers for Organizer's internal runtime command endpoint."""

from __future__ import annotations

import logging
from typing import Any, Callable

from runtime_handlers import RuntimeRetryableError, handle_runtime_command


def runtime_command_response(
    payload: dict[str, Any],
    *,
    db_path: str,
    dispatch_intent: Callable[[str, dict[str, Any]], dict[str, Any]],
    recover_result: Callable[[str, dict[str, Any]], dict[str, Any] | None],
) -> tuple[int, dict[str, Any]]:
    try:
        return handle_runtime_command(
            payload,
            db_path=db_path,
            dispatch_intent=dispatch_intent,
            recover_result=recover_result,
        )
    except RuntimeRetryableError:
        return 503, {
            "ok": False,
            "outcome": "retryable_failure",
            "retryable": True,
            "error": "runtime_unavailable",
        }
    except ValueError as exc:
        return 400, {"ok": False, "outcome": "rejected", "error": str(exc)[:200]}
    except Exception:
        logging.exception("runtime_command_unhandled_error")
        return 500, {"ok": False, "outcome": "terminal_failure", "error": "internal_error"}
