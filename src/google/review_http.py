from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from src.google.runtime_sheets_reverse_sync import build_reverse_sync_review_summary

try:
    from loguru import logger
except Exception:  # pragma: no cover
    import logging

    logger = logging.getLogger(__name__)


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _review_payload(limit: int) -> tuple[int, dict[str, Any]]:
    try:
        payload = build_reverse_sync_review_summary(limit=limit)
        return 200, payload
    except Exception as exc:
        logger.error("google_sync_review_http review_failed err={}", str(exc)[:300])
        return 500, {"ok": False, "error": str(exc)[:300]}


def _build_handler() -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                body = _json_bytes({"ok": True, "service": "google-sync-review"})
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/review":
                params = parse_qs(parsed.query)
                try:
                    limit = int(str((params.get("limit") or ["5"])[0] or "5"))
                except Exception:
                    limit = 5
                status, payload = _review_payload(limit=max(0, min(limit, 20)))
                body = _json_bytes(payload)
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            body = _json_bytes({"ok": False, "error": "not found"})
            self.send_response(404)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            logger.debug("google_sync_review_http access=" + (format % args))

    return _Handler


def start_review_http_server(*, port: int) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("0.0.0.0", int(port)), _build_handler())
    thread = threading.Thread(target=server.serve_forever, name="google-sync-review-http", daemon=True)
    thread.start()
    logger.info("google_sync_review_http started port={}", int(port))
    return server
