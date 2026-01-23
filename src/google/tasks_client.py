from __future__ import annotations

from typing import Any, Callable, Optional

import httpx

from src.google.auth import get_access_token

GOOGLE_TASKS_API = "https://www.googleapis.com/tasks/v1"

AccessTokenProvider = Callable[[], str]


class TasksClient:
    def __init__(self, access_token_provider: Optional[AccessTokenProvider] = None) -> None:
        self._access_token_provider = access_token_provider or get_access_token

    def _headers(self) -> dict[str, str]:
        access_token = self._access_token_provider()
        return {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

    def list_tasks(
        self,
        tasklist_id: str = "@default",
        page_token: Optional[str] = None,
        max_results: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "maxResults": max_results,
            "showDeleted": "false",
            "showHidden": "false",
        }
        if page_token:
            params["pageToken"] = page_token

        with httpx.Client(timeout=10.0) as client:
            resp = client.get(
                f"{GOOGLE_TASKS_API}/lists/{tasklist_id}/tasks",
                headers=self._headers(),
                params=params,
            )
            resp.raise_for_status()
            return resp.json()

    def get_task(self, tasklist_id: str, task_id: str) -> dict[str, Any]:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(
                f"{GOOGLE_TASKS_API}/lists/{tasklist_id}/tasks/{task_id}",
                headers=self._headers(),
            )
            resp.raise_for_status()
            return resp.json()

    def update_task(self, tasklist_id: str, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        with httpx.Client(timeout=10.0) as client:
            resp = client.patch(
                f"{GOOGLE_TASKS_API}/lists/{tasklist_id}/tasks/{task_id}",
                headers=self._headers(),
                json=payload,
            )
            resp.raise_for_status()
            return resp.json()

    def delete_task(self, tasklist_id: str, task_id: str) -> None:
        with httpx.Client(timeout=10.0) as client:
            resp = client.delete(
                f"{GOOGLE_TASKS_API}/lists/{tasklist_id}/tasks/{task_id}",
                headers=self._headers(),
            )
            resp.raise_for_status()
