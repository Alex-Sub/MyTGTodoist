import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable


def make_command_handler(deps_provider: Callable[[], dict[str, Any]]) -> type[BaseHTTPRequestHandler]:
    class _CommandHandler(BaseHTTPRequestHandler):
        def _send_json(self, status: int, payload: dict) -> None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _read_json(self) -> dict:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = 0
            raw = self.rfile.read(length) if length > 0 else b"{}"
            data = json.loads(raw.decode("utf-8"))
            if not isinstance(data, dict):
                raise ValueError("invalid json body")
            return data

        def do_GET(self):  # noqa: N802 - stdlib API
            if self.path == "/health":
                self._send_json(200, {"ok": True})
                return
            self._send_json(404, {"error": "not found"})

        def do_POST(self):  # noqa: N802 - stdlib API
            try:
                data = self._read_json()
                deps = deps_provider()
                if self.path == "/runtime/command":
                    status, res = deps["handle_runtime_command"](data, deps=deps["runtime_handler_deps"]())
                    self._send_json(status, res)
                    return
                if self.path == "/runtime/meeting/latest":
                    user_id = str(data.get("user_id") or "").strip()
                    if not user_id:
                        raise ValueError("user_id is required")
                    source = deps["runtime_meeting_source_for_user"](user_id)
                    self._send_json(200, source)
                    return
                if self.path == "/runtime/meeting/search":
                    user_id = str(data.get("user_id") or "").strip()
                    if not user_id:
                        raise ValueError("user_id is required")
                    target_hint = str(data.get("target_hint") or "").strip()
                    meeting_kind = str(data.get("meeting_kind") or "").strip()
                    result = deps["runtime_meeting_search_for_user"](user_id, target_hint, meeting_kind)
                    self._send_json(200, result)
                    return
                if self.path == "/runtime/meeting/cleanup_stale":
                    user_id = str(data.get("user_id") or "").strip()
                    if not user_id:
                        raise ValueError("user_id is required")
                    limit = int(data.get("limit") or 500)
                    result = deps["runtime_cleanup_stale_calendar_events_for_user"](user_id, limit=limit)
                    self._send_json(200, result)
                    return
                if self.path == "/runtime/meeting/check_calendar_drift":
                    user_id = str(data.get("user_id") or "").strip()
                    result = deps["runtime_check_calendar_drift"](
                        user_id=user_id,
                        from_value=data.get("from"),
                        to_value=data.get("to"),
                        notify=True,
                    )
                    self._send_json(200, result)
                    return
                if self.path == "/runtime/sync_conflict/action":
                    conflict_id = str(data.get("id") or "").strip()
                    action = str(data.get("action") or "").strip()
                    if not conflict_id:
                        raise ValueError("id is required")
                    if not action:
                        raise ValueError("action is required")
                    result = deps["runtime_sync_conflict_apply_action"](conflict_id, action)
                    self._send_json(200, result)
                    return
                if self.path == "/runtime/task/search":
                    user_id = str(data.get("user_id") or "").strip()
                    if not user_id:
                        raise ValueError("user_id is required")
                    target_hint = str(data.get("target_hint") or "").strip()
                    result = deps["runtime_task_search_for_user"](user_id, target_hint)
                    self._send_json(200, result)
                    return
                if self.path == "/runtime/task/check_duplicate_create":
                    user_id = str(data.get("user_id") or "").strip()
                    if not user_id:
                        raise ValueError("user_id is required")
                    title = str(data.get("title") or "").strip()
                    planned_at = data.get("planned_at")
                    parent_task_id = data.get("parent_task_id")
                    result = deps["runtime_task_duplicate_check_for_user"](
                        user_id,
                        title=title,
                        planned_at=planned_at,
                        parent_task_id=parent_task_id,
                    )
                    self._send_json(200, result)
                    return
                if self.path == "/runtime/timeblock/search":
                    user_id = str(data.get("user_id") or "").strip()
                    if not user_id:
                        raise ValueError("user_id is required")
                    target_hint = str(data.get("target_hint") or "").strip()
                    result = deps["runtime_timeblock_search_for_user"](user_id, target_hint)
                    self._send_json(200, result)
                    return
                self._send_json(404, {"error": "not found"})
            except ValueError as exc:
                self._send_json(400, {"error": str(exc)[:200]})
            except Exception as exc:
                logging.exception("runtime_server_post_failed err=%s", str(exc)[:200])
                self._send_json(500, {"error": str(exc)[:200]})

        def log_message(self, format, *args):  # noqa: A003 - stdlib API
            return

    return _CommandHandler


def start_command_server(port: int, handler_cls: type[BaseHTTPRequestHandler]) -> None:
    server = HTTPServer(("0.0.0.0", int(port)), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logging.info("worker_cmd_server started port=%s", int(port))
