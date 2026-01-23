from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
from loguru import logger

from src.google.auth import get_access_token

GOOGLE_DRIVE_API = "https://www.googleapis.com/drive/v3"
GOOGLE_DRIVE_UPLOAD = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart"


class DriveClient:
    def __init__(self) -> None:
        pass

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {get_access_token()}",
        }

    def find_or_create_folder(self, name: str) -> str:
        query = f"mimeType='application/vnd.google-apps.folder' and name='{name}' and trashed=false"
        params = {"q": query, "fields": "files(id,name)"}
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(
                f"{GOOGLE_DRIVE_API}/files",
                headers=self._headers(),
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()
            files = data.get("files", [])
            if files:
                return files[0]["id"]

            metadata = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
            create_resp = client.post(
                f"{GOOGLE_DRIVE_API}/files",
                headers={**self._headers(), "Content-Type": "application/json"},
                json=metadata,
            )
            create_resp.raise_for_status()
            created = create_resp.json()
            folder_id = created["id"]
            logger.info("Created Drive folder id={}", folder_id)
            return folder_id

    def find_file_in_folder(self, folder_id: str, name: str) -> str | None:
        query = f"name='{name}' and '{folder_id}' in parents and trashed=false"
        params = {"q": query, "fields": "files(id,name)"}
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(
                f"{GOOGLE_DRIVE_API}/files",
                headers=self._headers(),
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()
            files = data.get("files", [])
            if files:
                return files[0]["id"]
        return None

    def upload_file(
        self,
        folder_id: str,
        path: str,
        filename: str,
        mime: str,
        existing_file_id: str | None = None,
        convert_to_google: bool = True,
    ) -> dict[str, Any]:
        file_path = Path(path)
        if existing_file_id:
            metadata: dict[str, Any] = {"name": filename}
        else:
            metadata = {"name": filename, "parents": [folder_id]}
        if convert_to_google:
            metadata["mimeType"] = "application/vnd.google-apps.spreadsheet"
        files = {
            "metadata": ("metadata.json", json.dumps(metadata), "application/json"),
            "file": (filename, file_path.read_bytes(), mime),
        }
        with httpx.Client(timeout=30.0) as client:
            if existing_file_id:
                update_url = (
                    f"https://www.googleapis.com/upload/drive/v3/files/{existing_file_id}"
                    "?uploadType=multipart&fields=id,webViewLink"
                )
                resp = client.patch(
                    update_url,
                    headers=self._headers(),
                    files=files,
                )
            else:
                resp = client.post(
                    f"{GOOGLE_DRIVE_UPLOAD}&fields=id,webViewLink",
                    headers=self._headers(),
                    files=files,
                )
            resp.raise_for_status()
            uploaded = resp.json()
            file_id = uploaded["id"]
            web_view = uploaded.get("webViewLink")
            if not web_view:
                info_resp = client.get(
                    f"{GOOGLE_DRIVE_API}/files/{file_id}",
                    headers=self._headers(),
                    params={"fields": "webViewLink"},
                )
                info_resp.raise_for_status()
                info = info_resp.json()
                web_view = info.get("webViewLink")

        logger.info("Uploaded Drive file id={}", file_id)
        return {"file_id": file_id, "webViewLink": web_view}
