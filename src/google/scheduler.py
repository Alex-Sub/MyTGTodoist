from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from loguru import logger
from sqlalchemy import func, select

from src.config import settings
from src.db.models import Conflict, Item
from src.db.session import get_session
from src.exports.vitrina_tasks import build_vitrina
from src.google.google_sync import pull_google_tasks_with_conflicts
from src.google.sheets_client import SheetsClient
from src.google.sheet_pull import pull_google_sheet_apply_rows
from src.google.sync_in import sync_in_calendar_window
from src.google.sync_out import sync_out_meeting

_lock = asyncio.Lock()
_tabs_ensured = False
_last_calendar_pull_at: datetime | None = None
_last_tasks_pull_at: datetime | None = None
_last_sheets_pull_at: datetime | None = None
_last_calendar_pull_error: str | None = None
_last_tasks_pull_error: str | None = None
_last_sheets_pull_error: str | None = None
_last_vitrina_error: str | None = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_dt(value: datetime | None) -> str:
    return value.isoformat() if value is not None else ""


async def run_calendar_scheduler() -> None:
    in_interval = max(30, int(settings.sync_in_interval_sec))
    out_interval = max(30, int(settings.sync_out_interval_sec))
    tasks_pull_interval = max(30, int(settings.google_tasks_pull_interval_sec))
    sheets_pull_interval = max(30, int(settings.google_sheets_pull_interval_sec))
    vitrina_interval = max(60, int(settings.google_vitrina_refresh_interval_sec))
    logger.info(
        "calendar sync scheduler started (in={}s, out={}s, tasks_pull={}s, sheets_pull={}s, vitrina={}s)",
        in_interval,
        out_interval,
        tasks_pull_interval,
        sheets_pull_interval,
        vitrina_interval,
    )

    await _ensure_sheets_tabs_once()
    next_in = 0.0
    next_out = 0.0
    next_tasks_pull = 0.0
    next_sheets_pull = 0.0
    next_vitrina = 0.0
    loop = asyncio.get_running_loop()

    while True:
        now = loop.time()
        if settings.sync_in_enabled and now >= next_in:
            next_in = now + in_interval
            await _run_pull()

        if settings.sync_out_enabled and now >= next_out:
            next_out = now + out_interval
            await _run_push()

        if now >= next_tasks_pull:
            next_tasks_pull = now + tasks_pull_interval
            await _run_tasks_pull()

        if settings.google_sheets_spreadsheet_id and now >= next_sheets_pull:
            next_sheets_pull = now + sheets_pull_interval
            await _run_sheets_pull()

        if settings.google_sheets_spreadsheet_id and now >= next_vitrina:
            next_vitrina = now + vitrina_interval
            await _run_vitrina_refresh()

        await asyncio.sleep(1)


async def _run_pull() -> None:
    global _last_calendar_pull_at
    global _last_calendar_pull_error
    try:
        async with _lock:
            tz_name = settings.sync_timezone or settings.timezone
            tz = ZoneInfo(tz_name)
            window_start = datetime.now(tz).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            window_end = window_start + timedelta(
                days=max(1, int(settings.sync_window_days))
            )
            logger.info(
                "auto pull start window_start={} window_end={}",
                window_start.isoformat(),
                window_end.isoformat(),
            )
            with get_session() as session:
                stats = await asyncio.to_thread(
                    sync_in_calendar_window,
                    session,
                    settings.google_calendar_id_default,
                    window_start,
                    window_end,
                )
            if stats.get("token_reset") == 1:
                logger.warning("auto pull token_reset=1")
            logger.info("auto pull done stats={}", stats)
            _last_calendar_pull_at = _utc_now()
            _last_calendar_pull_error = None
    except Exception as exc:
        _last_calendar_pull_error = str(exc)[:300]
        logger.error("auto pull error: {}", exc)


async def _run_push() -> None:
    stats = {"processed": 0, "created": 0, "updated": 0, "cancelled": 0, "errors": 0}
    try:
        async with _lock:
            logger.info("auto push start")
            with get_session() as session:
                items = list(
                    session.scalars(
                        select(Item)
                        .where(Item.type == "meeting", Item.sync_state == "dirty")
                    ).all()
                )
                for item in items:
                    stats["processed"] += 1
                    try:
                        before_event = item.event_id
                        before_status = item.status
                        sync_out_meeting(session, item.id)
                        if before_status == "canceled":
                            stats["cancelled"] += 1
                        elif before_event:
                            stats["updated"] += 1
                        else:
                            stats["created"] += 1
                    except Exception:
                        stats["errors"] += 1
            logger.info("auto push done stats={}", stats)
    except Exception as exc:
        logger.error("auto push error: {}", exc)


async def _run_tasks_pull() -> None:
    global _last_tasks_pull_at
    global _last_tasks_pull_error
    try:
        async with _lock:
            with get_session() as session:
                res = await asyncio.to_thread(pull_google_tasks_with_conflicts, session)
            stats = res.get("stats") if isinstance(res, dict) else None
            clarification = res.get("clarification") if isinstance(res, dict) else None
            logger.info(
                "auto tasks pull done stats={} open_conflicts={}",
                stats,
                bool(clarification),
            )
            _last_tasks_pull_at = _utc_now()
            _last_tasks_pull_error = None
    except Exception as exc:
        _last_tasks_pull_error = str(exc)[:300]
        logger.error("auto tasks pull error: {}", exc)


async def _run_sheets_pull() -> None:
    global _last_sheets_pull_at
    global _last_sheets_pull_error
    try:
        async with _lock:
            spreadsheet_id = (settings.google_sheets_spreadsheet_id or "").strip()
            if not spreadsheet_id:
                return
            client = SheetsClient()
            rows, headers_idx, sheet_name = await asyncio.to_thread(
                client.read_apply_rows,
                spreadsheet_id,
                settings.google_sheets_range,
            )
            if not rows:
                return
            with get_session() as session:
                stats, row_updates = await asyncio.to_thread(
                    pull_google_sheet_apply_rows,
                    session,
                    rows,
                )
            written = await asyncio.to_thread(
                client.write_status_updates,
                spreadsheet_id,
                sheet_name=sheet_name,
                headers_idx=headers_idx,
                row_updates=row_updates,
            )
            logger.info("auto sheets pull done stats={} writes={}", stats, written)
            _last_sheets_pull_at = _utc_now()
            _last_sheets_pull_error = None
    except Exception as exc:
        _last_sheets_pull_error = str(exc)[:300]
        logger.error("auto sheets pull error: {}", exc)


async def _ensure_sheets_tabs_once() -> None:
    global _tabs_ensured
    if _tabs_ensured:
        return
    spreadsheet_id = (settings.google_sheets_spreadsheet_id or "").strip()
    if not spreadsheet_id:
        _tabs_ensured = True
        return
    try:
        client = SheetsClient()
        await asyncio.to_thread(
            client.ensure_tabs,
            spreadsheet_id,
            [settings.google_vitrina_sheet_name, settings.google_ops_log_sheet_name],
        )
        _tabs_ensured = True
    except Exception as exc:
        logger.error("ensure tabs failed: {}", exc)


def _parse_ts(raw: str) -> datetime | None:
    text = str(raw or "").strip()
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


async def _prune_ops_log(client: SheetsClient, spreadsheet_id: str) -> int:
    retention_days = max(1, int(settings.google_ops_retention_days))
    cutoff = _utc_now() - timedelta(days=retention_days)
    sheet_name = settings.google_ops_log_sheet_name
    values = await asyncio.to_thread(client.read_range, spreadsheet_id, f"'{sheet_name}'!A4:Z")
    if not values:
        return 0
    kept: list[list[Any]] = []
    removed = 0
    for row in values:
        ts = _parse_ts(row[0] if row else "")
        if ts is None or ts >= cutoff:
            kept.append(row)
        else:
            removed += 1
    if removed <= 0:
        return 0
    await asyncio.to_thread(client.clear_range, spreadsheet_id, f"'{sheet_name}'!A4:Z")
    if kept:
        await asyncio.to_thread(client.write_range, spreadsheet_id, f"'{sheet_name}'!A4", kept)
    return removed


async def _run_vitrina_refresh() -> None:
    global _last_vitrina_error
    spreadsheet_id = (settings.google_sheets_spreadsheet_id or "").strip()
    if not spreadsheet_id:
        return
    try:
        async with _lock:
            client = SheetsClient()
            await asyncio.to_thread(
                client.ensure_tabs,
                spreadsheet_id,
                [settings.google_vitrina_sheet_name, settings.google_ops_log_sheet_name],
            )
            with get_session() as session:
                header, rows = build_vitrina(session)
                active_count = int(
                    session.scalar(
                        select(func.count()).where(
                            Item.type == "task",
                            Item.status.notin_(("done", "archived")),
                        )
                    )
                    or 0
                )
                open_conflicts = int(
                    session.scalar(select(func.count()).where(Conflict.status == "open")) or 0
                )
                sync_failed = int(
                    session.scalar(
                        select(func.count()).where(
                            Item.type == "task",
                            Item.google_sync_status == "failed",
                        )
                    )
                    or 0
                )
                sync_pending = int(
                    session.scalar(
                        select(func.count()).where(
                            Item.type == "task",
                            Item.google_sync_status == "pending",
                        )
                    )
                    or 0
                )

            await asyncio.to_thread(client.clear_sheet, spreadsheet_id, settings.google_vitrina_sheet_name)
            await asyncio.to_thread(
                client.write_table,
                spreadsheet_id,
                settings.google_vitrina_sheet_name,
                header,
                rows,
            )

            last_error = _last_vitrina_error or _last_sheets_pull_error or _last_tasks_pull_error or _last_calendar_pull_error or ""
            meta = {
                "now": _fmt_dt(_utc_now()),
                "active_count": active_count,
                "open_conflicts": open_conflicts,
                "sync_failed": sync_failed,
                "sync_pending": sync_pending,
                "last_tasks_pull_at": _fmt_dt(_last_tasks_pull_at),
                "last_sheets_pull_at": _fmt_dt(_last_sheets_pull_at),
                "last_calendar_pull_at": _fmt_dt(_last_calendar_pull_at),
            }
            ops_header = [
                "ts",
                "status",
                "vitrina_rows",
                "open_conflicts",
                "failed_sync",
                "calendar_pull_at",
                "tasks_pull_at",
                "sheets_pull_at",
                "last_error",
            ]
            ops_row = [
                _fmt_dt(_utc_now()),
                "",
                len(rows),
                open_conflicts,
                sync_failed,
                _fmt_dt(_last_calendar_pull_at),
                _fmt_dt(_last_tasks_pull_at),
                _fmt_dt(_last_sheets_pull_at),
                str(last_error or ""),
            ]
            await asyncio.to_thread(
                client.ops_log_upsert,
                spreadsheet_id,
                settings.google_ops_log_sheet_name,
                meta,
                ops_header,
                ops_row,
            )
            pruned = await _prune_ops_log(client, spreadsheet_id)
            logger.info("vitrina refresh done rows={} pruned_ops={}", len(rows), pruned)
            _last_vitrina_error = None
    except Exception as exc:
        _last_vitrina_error = str(exc)[:300]
        logger.error("vitrina refresh error: {}", exc)
