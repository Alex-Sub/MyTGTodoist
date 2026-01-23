from __future__ import annotations

import asyncio
from datetime import datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from loguru import logger

from src.config import settings
from src.db.session import get_session
from src.exports.excel_export import export_xlsx
from src.google.drive_client import DriveClient
from src.google.sync_in import sync_in_calendar_window

_SYNC_INTERVAL_SEC = 15 * 60
_lock = asyncio.Lock()


def _upload_drive(path: str | Path) -> dict:
    client = DriveClient()
    folder_id = client.find_or_create_folder(settings.google_drive_folder_name)
    if settings.google_drive_mode == "latest":
        filename = "todo_latest.xlsx"
        existing_id = client.find_file_in_folder(folder_id, filename)
    else:
        filename = Path(path).name
        existing_id = None
    return client.upload_file(
        folder_id=folder_id,
        path=str(path),
        filename=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        existing_file_id=existing_id,
        convert_to_google=True,
    )


def _get_sync_window() -> tuple[str, datetime, datetime]:
    tz_name = settings.sync_timezone or settings.timezone
    tz = ZoneInfo(tz_name)
    today = datetime.now(tz).date()
    start = datetime.combine(today, time.min, tzinfo=tz).replace(microsecond=0)
    end = start + timedelta(days=int(settings.sync_window_days))
    end = end.replace(microsecond=0) - timedelta(seconds=1)
    return tz_name, start, end


async def sync_all(reason: str) -> dict:
    if _lock.locked():
        logger.warning("sync_all skipped reason={} status=locked", reason)
        return {"skipped": True}

    async with _lock:
        parts = 0
        ok = 0
        stats: dict[str, object] = {"calendar": None, "sheet": None, "drive": None}
        export_path: str | None = None

        tz_name, window_start, window_end = _get_sync_window()
        logger.info(
            "sync_all start reason={} window_start={} window_end={} tz={}",
            reason,
            window_start.isoformat(),
            window_end.isoformat(),
            tz_name,
        )

        parts += 1
        try:
            with get_session() as session:
                stats["calendar"] = await asyncio.to_thread(
                    sync_in_calendar_window,
                    session,
                    settings.google_calendar_id_default,
                    window_start,
                    window_end,
                )
            ok += 1
        except Exception:
            stats["calendar"] = "error"
            logger.exception("sync_all calendar error")

        parts += 1
        try:
            with get_session() as session:
                export_path = await asyncio.to_thread(
                    export_xlsx,
                    session,
                    {"mode": "week"},
                )
            stats["sheet"] = export_path
            ok += 1
        except Exception:
            stats["sheet"] = "error"
            logger.exception("sync_all sheet error")

        parts += 1
        if not settings.google_drive_enabled:
            stats["drive"] = "disabled"
        elif not export_path:
            stats["drive"] = "skipped_no_export"
        else:
            try:
                stats["drive"] = await asyncio.to_thread(_upload_drive, export_path)
                ok += 1
            except Exception:
                stats["drive"] = "error"
                logger.exception("sync_all drive error")

        calendar_count = 0
        if isinstance(stats.get("calendar"), dict):
            calendar_count = int(stats["calendar"].get("processed", 0))
        logger.info(
            "sync_all end reason={} ok={} parts={} count={} stats={}",
            reason,
            ok,
            parts,
            calendar_count,
            stats,
        )
        return {"ok": ok, "parts": parts, "stats": stats}


async def run_sync_loop(interval_sec: int = _SYNC_INTERVAL_SEC) -> None:
    logger.info("sync_all scheduler started interval={}s", interval_sec)
    try:
        await sync_all("startup")
        while True:
            await asyncio.sleep(interval_sec)
            await sync_all("interval")
    except asyncio.CancelledError:
        logger.info("sync_all scheduler stopped")
        raise
