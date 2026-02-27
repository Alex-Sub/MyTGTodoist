from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import httpx

from src.google.auth import get_access_token

GOOGLE_SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"


def _col_to_a1(col_index: int) -> str:
    n = col_index + 1
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def _split_sheet_name(a1_range: str) -> str:
    if "!" in a1_range:
        return a1_range.split("!", 1)[0].strip("'")
    return "Sheet1"


class SheetsClient:
    def _headers(self) -> dict[str, str]:
        token = get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def read_apply_rows(self, spreadsheet_id: str, a1_range: str) -> tuple[list[dict[str, Any]], dict[str, int], str]:
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(
                f"{GOOGLE_SHEETS_API}/{spreadsheet_id}/values/{quote(a1_range, safe='!:')}",
                headers=self._headers(),
            )
            resp.raise_for_status()
            data = resp.json()

        values = data.get("values") or []
        if not values:
            return [], {}, _split_sheet_name(a1_range)
        headers = [str(v).strip() for v in values[0]]
        idx = {name: i for i, name in enumerate(headers)}
        rows: list[dict[str, Any]] = []
        for row_index, row_values in enumerate(values[1:], start=2):
            row: dict[str, Any] = {"_sheet_row": row_index}
            for name, col in idx.items():
                row[name] = row_values[col] if col < len(row_values) else None
            apply_raw = row.get("apply")
            if str(apply_raw or "").strip().upper() == "TRUE":
                rows.append(row)
        return rows, idx, _split_sheet_name(a1_range)

    def read_range(self, spreadsheet_id: str, a1_range: str) -> list[list[Any]]:
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(
                f"{GOOGLE_SHEETS_API}/{spreadsheet_id}/values/{quote(a1_range, safe='!:')}",
                headers=self._headers(),
            )
            resp.raise_for_status()
            data = resp.json()
        return data.get("values") or []

    def write_range(self, spreadsheet_id: str, a1_range: str, rows: list[list[Any]]) -> None:
        with httpx.Client(timeout=15.0) as client:
            resp = client.put(
                f"{GOOGLE_SHEETS_API}/{spreadsheet_id}/values/{quote(a1_range, safe='!:')}",
                headers=self._headers(),
                params={"valueInputOption": "RAW"},
                json={"range": a1_range, "majorDimension": "ROWS", "values": rows},
            )
            resp.raise_for_status()

    def write_status_updates(
        self,
        spreadsheet_id: str,
        *,
        sheet_name: str,
        headers_idx: dict[str, int],
        row_updates: list[dict[str, Any]],
    ) -> int:
        status_col = headers_idx.get("status")
        apply_col = headers_idx.get("apply")
        if status_col is None and apply_col is None:
            return 0
        written = 0
        with httpx.Client(timeout=15.0) as client:
            for upd in row_updates:
                sheet_row = int(upd.get("sheet_row") or 0)
                if sheet_row <= 0:
                    continue
                if status_col is not None:
                    status_cell = f"'{sheet_name}'!{_col_to_a1(status_col)}{sheet_row}"
                    resp = client.put(
                        f"{GOOGLE_SHEETS_API}/{spreadsheet_id}/values/{quote(status_cell, safe='!:')}",
                        headers=self._headers(),
                        params={"valueInputOption": "RAW"},
                        json={"range": status_cell, "majorDimension": "ROWS", "values": [[str(upd.get('status') or "")]]},
                    )
                    resp.raise_for_status()
                    written += 1
                if apply_col is not None and "apply" in upd:
                    apply_cell = f"'{sheet_name}'!{_col_to_a1(apply_col)}{sheet_row}"
                    apply_value = "TRUE" if bool(upd.get("apply")) else "FALSE"
                    resp = client.put(
                        f"{GOOGLE_SHEETS_API}/{spreadsheet_id}/values/{quote(apply_cell, safe='!:')}",
                        headers=self._headers(),
                        params={"valueInputOption": "RAW"},
                        json={"range": apply_cell, "majorDimension": "ROWS", "values": [[apply_value]]},
                    )
                    resp.raise_for_status()
                    written += 1
        return written

    def ensure_tabs(self, spreadsheet_id: str, sheet_names: list[str]) -> list[str]:
        wanted = [str(name or "").strip() for name in sheet_names if str(name or "").strip()]
        if not wanted:
            return []
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(
                f"{GOOGLE_SHEETS_API}/{spreadsheet_id}",
                headers=self._headers(),
                params={"fields": "sheets.properties.title"},
            )
            resp.raise_for_status()
            data = resp.json()
            existing = {
                str(((sheet.get("properties") or {}).get("title") or "")).strip()
                for sheet in (data.get("sheets") or [])
            }

            missing = [name for name in wanted if name not in existing]
            if not missing:
                return []

            requests = [{"addSheet": {"properties": {"title": name}}} for name in missing]
            resp = client.post(
                f"{GOOGLE_SHEETS_API}/{spreadsheet_id}:batchUpdate",
                headers=self._headers(),
                json={"requests": requests},
            )
            resp.raise_for_status()
            return missing

    def clear_sheet(self, spreadsheet_id: str, sheet_name: str) -> None:
        target = quote(f"'{sheet_name}'", safe="!:'")
        with httpx.Client(timeout=15.0) as client:
            resp = client.post(
                f"{GOOGLE_SHEETS_API}/{spreadsheet_id}/values/{target}:clear",
                headers=self._headers(),
                json={},
            )
            resp.raise_for_status()

    def clear_range(self, spreadsheet_id: str, a1_range: str) -> None:
        with httpx.Client(timeout=15.0) as client:
            resp = client.post(
                f"{GOOGLE_SHEETS_API}/{spreadsheet_id}/values/{quote(a1_range, safe='!:')}:clear",
                headers=self._headers(),
                json={},
            )
            resp.raise_for_status()

    def write_table(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        header: list[str],
        rows: list[list[Any]],
    ) -> None:
        values: list[list[Any]] = [list(header)]
        for row in rows:
            values.append(list(row))
        target = quote(f"'{sheet_name}'!A1", safe="!:'")
        with httpx.Client(timeout=15.0) as client:
            resp = client.put(
                f"{GOOGLE_SHEETS_API}/{spreadsheet_id}/values/{target}",
                headers=self._headers(),
                params={"valueInputOption": "RAW"},
                json={"range": f"'{sheet_name}'!A1", "majorDimension": "ROWS", "values": values},
            )
            resp.raise_for_status()

    def ops_log_upsert(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        meta: dict[str, Any],
        header: list[str],
        row: list[Any],
    ) -> None:
        def _parse_dt(raw: Any) -> datetime | None:
            if raw is None:
                return None
            if isinstance(raw, datetime):
                dt = raw
            else:
                text = str(raw).strip()
                if not text:
                    return None
                if text.endswith("Z"):
                    text = text[:-1] + "+00:00"
                try:
                    dt = datetime.fromisoformat(text)
                except Exception:
                    return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

        now_dt = _parse_dt(meta.get("now")) or datetime.now(timezone.utc)
        now_str = now_dt.isoformat()
        active_count = int(meta.get("active_count") or 0)
        open_conflicts = int(meta.get("open_conflicts") or 0)
        sync_failed = int(meta.get("sync_failed") or 0)
        sync_pending = int(meta.get("sync_pending") or 0)
        last_tasks_pull_at = _parse_dt(meta.get("last_tasks_pull_at"))
        last_sheets_pull_at = _parse_dt(meta.get("last_sheets_pull_at"))
        last_calendar_pull_at = _parse_dt(meta.get("last_calendar_pull_at"))

        last_tasks_pull_age_hours = 10**9
        if last_tasks_pull_at is not None:
            last_tasks_pull_age_hours = (now_dt - last_tasks_pull_at).total_seconds() / 3600.0

        status = "OK"
        if open_conflicts > 0 or sync_failed > 0:
            status = "ATTENTION"
        if last_tasks_pull_age_hours > 2:
            status = "ATTENTION"

        meta_row = [
            "Last update",
            now_str,
            "Status",
            status,
            "Active",
            active_count,
            "Conflicts",
            open_conflicts,
            "Failed",
            sync_failed,
            "Pending",
            sync_pending,
        ]
        meta_range = f"'{sheet_name}'!A1:L1"
        with httpx.Client(timeout=15.0) as client:
            resp = client.put(
                f"{GOOGLE_SHEETS_API}/{spreadsheet_id}/values/{quote(meta_range, safe='!:' + chr(39))}",
                headers=self._headers(),
                params={"valueInputOption": "RAW"},
                json={"range": meta_range, "majorDimension": "ROWS", "values": [meta_row]},
            )
            resp.raise_for_status()

        header_range = f"'{sheet_name}'!A3:Z3"
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(
                f"{GOOGLE_SHEETS_API}/{spreadsheet_id}/values/{quote(header_range, safe='!:' + chr(39))}",
                headers=self._headers(),
            )
            resp.raise_for_status()
            hdr_values = (resp.json().get("values") or [])
            has_header = bool(hdr_values and any(str(cell or "").strip() for cell in hdr_values[0]))
            if not has_header:
                write_header_range = f"'{sheet_name}'!A3"
                resp = client.put(
                    f"{GOOGLE_SHEETS_API}/{spreadsheet_id}/values/{quote(write_header_range, safe='!:' + chr(39))}",
                    headers=self._headers(),
                    params={"valueInputOption": "RAW"},
                    json={"range": write_header_range, "majorDimension": "ROWS", "values": [list(header)]},
                )
                resp.raise_for_status()

            if len(row) > 1:
                row[1] = status
            append_range = f"'{sheet_name}'!A4"
            resp = client.post(
                f"{GOOGLE_SHEETS_API}/{spreadsheet_id}/values/{quote(append_range, safe='!:' + chr(39))}:append",
                headers=self._headers(),
                params={"valueInputOption": "RAW", "insertDataOption": "INSERT_ROWS"},
                json={"range": append_range, "majorDimension": "ROWS", "values": [list(row)]},
            )
            resp.raise_for_status()
