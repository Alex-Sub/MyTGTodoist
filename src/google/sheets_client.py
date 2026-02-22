from __future__ import annotations

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
