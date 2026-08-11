from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response
import uvicorn

from filesystem_tools import find_files, grep_text, list_dir, project_tree, read_text_file
from safety import FileServiceError, validate_requested_path

SERVER_NAME = "readonly-filesystem-mcp"
SERVER_VERSION = "1.2.0"
PROTOCOL_VERSION = "2024-11-05"
DEFAULT_MCP_PATH = "/mcp"


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    input_schema: dict[str, Any]


TOOL_ANNOTATIONS: dict[str, Any] = {
    "readOnlyHint": True,
    "openWorldHint": False,
    "destructiveHint": False,
}
TOOL_SECURITY_SCHEMES: list[dict[str, Any]] = [{"type": "noauth"}]


def _list_dir_tool(path: str) -> dict[str, Any]:
    data = list_dir(path)
    dirs = [item for item in data["items"] if item["type"] == "dir"]
    files = [item for item in data["items"] if item["type"] == "file"]
    return {
        "path": data["path"],
        "dirs": dirs,
        "files": files,
    }


def _find_files_tool(query: str, root: str | None = None, max_results: int = 100) -> dict[str, Any]:
    data = find_files(query=query, root=root, max_results=max_results)
    return {
        "query": data["query"],
        "results": data["results"],
    }


def _read_text_file_tool(path: str, max_chars: int = 200_000) -> dict[str, Any]:
    data = read_text_file(path=path, max_chars=max_chars)
    return {
        "path": data["path"],
        "content": data["content"],
        "truncated": data["truncated"],
    }


def _project_tree_tool(path: str, depth: int = 3) -> dict[str, Any]:
    tree = project_tree(path=path, depth=depth)
    return {
        "path": tree["path"],
        "depth": depth,
        "tree": tree,
    }


def _grep_text_tool(query: str, root: str | None = None, max_results: int = 100) -> dict[str, Any]:
    data = grep_text(query=query, root=root, max_results=max_results)
    return {
        "query": data["query"],
        "matches": data["results"],
    }


def _read_many_files_tool(paths: list[str], max_chars_per_file: int = 100_000) -> dict[str, Any]:
    if not isinstance(paths, list) or not paths:
        raise FileServiceError("paths must be a non-empty array of strings.")
    if max_chars_per_file <= 0:
        raise FileServiceError("max_chars_per_file must be > 0.")

    items: list[dict[str, Any]] = []
    for raw_path in paths:
        if not isinstance(raw_path, str) or not raw_path:
            items.append({"path": str(raw_path), "error": "Path must be a non-empty string."})
            continue
        try:
            data = read_text_file(path=raw_path, max_chars=max_chars_per_file)
            items.append(
                {
                    "path": data["path"],
                    "content": data["content"],
                    "truncated": data["truncated"],
                }
            )
        except (FileServiceError, OSError) as exc:
            items.append({"path": raw_path, "error": str(exc)})

    return {"items": items}


def _file_info_tool(path: str) -> dict[str, Any]:
    safe_path = validate_requested_path(path)
    exists = safe_path.exists()
    is_file = safe_path.is_file()
    is_dir = safe_path.is_dir()
    suffix = safe_path.suffix.lower() if is_file else ""
    size_bytes: int | None = None
    modified_at: str | None = None

    if exists:
        stat = safe_path.stat()
        size_bytes = stat.st_size if is_file else None
        modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()

    return {
        "path": str(safe_path),
        "exists": exists,
        "is_file": is_file,
        "is_dir": is_dir,
        "suffix": suffix,
        "size_bytes": size_bytes,
        "modified_at": modified_at,
    }


TOOL_SPECS: list[ToolSpec] = [
    ToolSpec(
        name="list_dir",
        description="List one directory. Returns path, dirs, files.",
        input_schema={
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
            "additionalProperties": False,
        },
    ),
    ToolSpec(
        name="find_files",
        description="Case-insensitive search by partial file/dir name.",
        input_schema={
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "root": {"type": ["string", "null"]},
                "max_results": {"type": "integer", "minimum": 1, "default": 100},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
    ),
    ToolSpec(
        name="read_text_file",
        description="Read one allowed text file only.",
        input_schema={
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "max_chars": {"type": "integer", "minimum": 1, "default": 200000},
            },
            "required": ["path"],
            "additionalProperties": False,
        },
    ),
    ToolSpec(
        name="project_tree",
        description="Return project tree up to depth. Skips service dirs.",
        input_schema={
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "depth": {"type": "integer", "minimum": 0, "default": 3},
            },
            "required": ["path"],
            "additionalProperties": False,
        },
    ),
    ToolSpec(
        name="grep_text",
        description="Case-insensitive text search inside allowed text files.",
        input_schema={
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "root": {"type": ["string", "null"]},
                "max_results": {"type": "integer", "minimum": 1, "default": 100},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
    ),
    ToolSpec(
        name="read_many_files",
        description="Read many allowed text files. Returns partial errors per item.",
        input_schema={
            "type": "object",
            "properties": {
                "paths": {"type": "array", "items": {"type": "string"}, "minItems": 1},
                "max_chars_per_file": {"type": "integer", "minimum": 1, "default": 100000},
            },
            "required": ["paths"],
            "additionalProperties": False,
        },
    ),
    ToolSpec(
        name="file_info",
        description="Return filesystem metadata for a path inside allowed roots.",
        input_schema={
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
            "additionalProperties": False,
        },
    ),
]

TOOL_HANDLERS: dict[str, Callable[..., dict[str, Any]]] = {
    "list_dir": _list_dir_tool,
    "find_files": _find_files_tool,
    "read_text_file": _read_text_file_tool,
    "project_tree": _project_tree_tool,
    "grep_text": _grep_text_tool,
    "read_many_files": _read_many_files_tool,
    "file_info": _file_info_tool,
}


def _normalize_mcp_path(raw_path: str) -> str:
    if not raw_path:
        return DEFAULT_MCP_PATH
    path = raw_path.strip()
    if not path.startswith("/"):
        path = "/" + path
    return path.rstrip("/") or "/"


def _log(enabled: bool, message: str) -> None:
    if enabled:
        print(message, file=sys.stderr, flush=True)


def _read_message() -> dict[str, Any] | None:
    content_length: int | None = None

    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None

        if line in (b"\r\n", b"\n"):
            break

        header = line.decode("utf-8").strip()
        if not header:
            continue

        key, _, value = header.partition(":")
        if key.lower() == "content-length":
            content_length = int(value.strip())

    if content_length is None:
        raise ValueError("Missing Content-Length header")

    body = sys.stdin.buffer.read(content_length)
    if not body:
        return None
    return json.loads(body.decode("utf-8"))


def _write_message(payload: dict[str, Any]) -> None:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    header = f"Content-Length: {len(raw)}\r\n\r\n".encode("ascii")
    sys.stdout.buffer.write(header)
    sys.stdout.buffer.write(raw)
    sys.stdout.buffer.flush()


def _success_response(req_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _error_response(req_id: Any, code: int, message: str) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {"code": code, "message": message},
    }


def _tool_result(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "content": [{"type": "text", "text": json.dumps(data, ensure_ascii=False, indent=2)}],
        "structuredContent": data,
    }


def _tool_error(message: str) -> dict[str, Any]:
    return {
        "content": [{"type": "text", "text": message}],
        "isError": True,
    }


def _handle_initialize(req_id: Any, params: dict[str, Any] | None) -> dict[str, Any]:
    _ = params
    return _success_response(
        req_id,
        {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
        },
    )


def _handle_tools_list(req_id: Any) -> dict[str, Any]:
    return _success_response(
        req_id,
        {
            "tools": [
                {
                    "name": spec.name,
                    "description": spec.description,
                    "inputSchema": spec.input_schema,
                    "annotations": TOOL_ANNOTATIONS,
                    "securitySchemes": TOOL_SECURITY_SCHEMES,
                }
                for spec in TOOL_SPECS
            ]
        },
    )


def _handle_tools_call(req_id: Any, params: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(params, dict):
        return _error_response(req_id, -32602, "Invalid params: object expected")

    name = params.get("name")
    arguments = params.get("arguments", {})

    if name not in TOOL_HANDLERS:
        return _error_response(req_id, -32601, f"Unknown tool: {name}")
    if not isinstance(arguments, dict):
        return _error_response(req_id, -32602, "Invalid params: arguments must be object")

    handler = TOOL_HANDLERS[name]
    try:
        result = handler(**arguments)
        return _success_response(req_id, _tool_result(result))
    except FileServiceError as exc:
        return _success_response(req_id, _tool_error(str(exc)))
    except TypeError as exc:
        return _success_response(req_id, _tool_error(f"Invalid tool arguments: {exc}"))
    except Exception as exc:
        return _success_response(req_id, _tool_error(f"Internal error: {exc}"))


def process_mcp_message(message: dict[str, Any]) -> dict[str, Any] | None:
    req_id = message.get("id")
    method = message.get("method")
    params = message.get("params")

    if method == "initialize":
        return _handle_initialize(req_id, params)
    if method == "tools/list":
        return _handle_tools_list(req_id)
    if method == "tools/call":
        return _handle_tools_call(req_id, params)
    if method == "notifications/initialized":
        return None
    if req_id is not None:
        return _error_response(req_id, -32601, f"Method not found: {method}")
    return None


def run_stdio(debug: bool = False) -> None:
    _log(debug, f"{SERVER_NAME} started in stdio mode")

    while True:
        try:
            message = _read_message()
            if message is None:
                _log(debug, "stdin closed, shutting down")
                return

            _log(debug, f"<= {message}")
            response = process_mcp_message(message)
            if response is None:
                continue
            _log(debug, f"=> {response}")
            _write_message(response)
        except Exception as exc:
            _log(debug, f"fatal: {exc}")
            return


def _is_authorized(request: Request, auth_token: str | None) -> bool:
    if not auth_token:
        return True
    header = request.headers.get("authorization", "")
    return header == f"Bearer {auth_token}"


def build_http_app(mcp_path: str, debug: bool = False, auth_token: str | None = None) -> FastAPI:
    app = FastAPI(title=SERVER_NAME, version=SERVER_VERSION)

    def _cors_headers(response: Response) -> Response:
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "POST, GET, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "content-type, authorization, mcp-session-id"
        response.headers["Access-Control-Expose-Headers"] = "Mcp-Session-Id"
        return response

    @app.get("/")
    async def root() -> Response:
        return PlainTextResponse(f"{SERVER_NAME} {SERVER_VERSION} (read-only)")

    @app.options(mcp_path)
    async def mcp_options() -> Response:
        return _cors_headers(Response(status_code=204))

    @app.options(f"{mcp_path}/{{rest:path}}")
    async def mcp_options_nested(rest: str) -> Response:
        _ = rest
        return _cors_headers(Response(status_code=204))

    @app.get(mcp_path)
    async def mcp_get(request: Request) -> Response:
        if not _is_authorized(request, auth_token):
            return _cors_headers(JSONResponse(status_code=401, content={"detail": "Unauthorized"}))
        info = {
            "server": SERVER_NAME,
            "version": SERVER_VERSION,
            "mode": "read-only",
            "protocolVersion": PROTOCOL_VERSION,
            "hint": "Use POST with MCP JSON-RPC to this /mcp endpoint.",
        }
        return _cors_headers(JSONResponse(content=info))

    @app.delete(mcp_path)
    async def mcp_delete(request: Request) -> Response:
        if not _is_authorized(request, auth_token):
            return _cors_headers(JSONResponse(status_code=401, content={"detail": "Unauthorized"}))
        return _cors_headers(Response(status_code=204))

    @app.post(mcp_path)
    async def mcp_post(request: Request) -> Response:
        if not _is_authorized(request, auth_token):
            return _cors_headers(JSONResponse(status_code=401, content={"detail": "Unauthorized"}))

        try:
            message = await request.json()
            if not isinstance(message, dict):
                return _cors_headers(JSONResponse(status_code=400, content={"detail": "JSON-RPC object expected"}))

            _log(debug, f"<= HTTP {message}")
            response = process_mcp_message(message)
            if response is None:
                return _cors_headers(Response(status_code=202))

            _log(debug, f"=> HTTP {response}")
            return _cors_headers(JSONResponse(content=response))
        except json.JSONDecodeError:
            return _cors_headers(JSONResponse(status_code=400, content={"detail": "Invalid JSON"}))

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only MCP filesystem server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default=os.getenv("MCP_TRANSPORT", "stdio"),
        help="MCP transport. Use stdio for local clients, http for remote /mcp deployment.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logs to stderr (for local development).",
    )
    parser.add_argument("--host", default=os.getenv("MCP_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MCP_PORT", "8787")))
    parser.add_argument("--mcp-path", default=os.getenv("MCP_PATH", DEFAULT_MCP_PATH))
    parser.add_argument(
        "--auth-token",
        default=os.getenv("MCP_AUTH_TOKEN"),
        help="Optional bearer token for HTTP mode. If set, require Authorization: Bearer <token>.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    mcp_path = _normalize_mcp_path(args.mcp_path)

    if args.transport == "stdio":
        run_stdio(debug=args.debug)
        return

    app = build_http_app(mcp_path=mcp_path, debug=args.debug, auth_token=args.auth_token)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
