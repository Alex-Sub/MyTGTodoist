from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from filesystem_tools import find_files, grep_text, list_dir, project_tree, read_text_file
from safety import FileServiceError

app = FastAPI(title="Read-Only File Service", version="1.0.0")


class ListDirRequest(BaseModel):
    path: str


class FindFilesRequest(BaseModel):
    query: str
    root: str | None = None
    max_results: int = Field(default=100, ge=1)


class ReadTextFileRequest(BaseModel):
    path: str
    max_chars: int = Field(default=200_000, ge=1)


class ProjectTreeRequest(BaseModel):
    path: str
    depth: int = Field(default=3, ge=0)


class GrepTextRequest(BaseModel):
    query: str
    root: str | None = None
    max_results: int = Field(default=100, ge=1)


def _run_tool(func, *args, **kwargs) -> Any:
    try:
        return func(*args, **kwargs)
    except FileServiceError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Filesystem error: {exc}") from exc


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "mode": "read-only"}


@app.post("/list_dir")
def list_dir_endpoint(request: ListDirRequest) -> dict:
    return _run_tool(list_dir, request.path)


@app.post("/find_files")
def find_files_endpoint(request: FindFilesRequest) -> dict:
    return _run_tool(find_files, request.query, request.root, request.max_results)


@app.post("/read_text_file")
def read_text_file_endpoint(request: ReadTextFileRequest) -> dict:
    return _run_tool(read_text_file, request.path, request.max_chars)


@app.post("/project_tree")
def project_tree_endpoint(request: ProjectTreeRequest) -> dict:
    return _run_tool(project_tree, request.path, request.depth)


@app.post("/grep_text")
def grep_text_endpoint(request: GrepTextRequest) -> dict:
    return _run_tool(grep_text, request.query, request.root, request.max_results)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="127.0.0.1", port=8765, reload=False)
