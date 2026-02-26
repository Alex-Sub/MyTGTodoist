from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from loguru import logger
from sqlalchemy import func, select

from src.config import settings
from src.db.models import CalendarSyncState, Conflict, Item, SyncOutbox
from src.db.session import get_session
from src.exports.vitrina_tasks import build_vitrina
from src.google.google_sync import (
    pull_google_tasks_with_conflicts,
    sync_task_completed,
    sync_task_created,
    sync_task_updated,
)
from src.google.sheets_client import SheetsClient
from src.google.sheet_pull import pull_google_sheet_apply_rows
from src.google.sync_in import sync_in_calendar_window
from src.google.sync_out import sync_out_meeting

_lock = asyncio.Lock()
_tabs_ensured = False
_SYNC_STATE_KEY = "__global_sync_policy__"
_OUTBOX_BATCH_SIZE = 50
_OUTBOX_BASE_BACKOFF_SEC = 30
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


def _get_sync_state(session) -> CalendarSyncState:
    state = session.scalar(select(CalendarSyncState).where(CalendarSyncState.calendar_id == _SYNC_STATE_KEY))
    if state is not None:
        return state
    state = CalendarSyncState(calendar_id=_SYNC_STATE_KEY)
    session.add(state)
    session.flush()
    return state


def _current_poll_interval() -> int:
    now = _utc_now()
    with get_session() as session:
        state = _get_sync_state(session)
        active_until = state.active_until
        if active_until is not None:
            if active_until.tzinfo is None:
                active_until = active_until.replace(tzinfo=timezone.utc)
            else:
                active_until = active_until.astimezone(timezone.utc)
        if active_until is not None and now < active_until:
            return max(5, int(settings.sync_poll_active_sec))
    return max(30, int(settings.sync_poll_idle_sec))


async def run_calendar_scheduler() -> None:
    vitrina_interval = max(60, int(settings.google_vitrina_refresh_interval_sec))
    logger.info(
        "calendar sync scheduler started (active_window_min={} active_poll={}s idle_poll={}s vitrina={}s)",
        int(settings.sync_active_window_min),
        int(settings.sync_poll_active_sec),
        int(settings.sync_poll_idle_sec),
        vitrina_interval,
    )

    await _ensure_sheets_tabs_once()
    next_vitrina = 0.0
    loop = asyncio.get_running_loop()

    while True:
        now = loop.time()
        # Poll tick always checks pull sources.
        if settings.sync_in_enabled:
            await _run_pull()
        await _run_tasks_pull()
        if settings.google_sheets_spreadsheet_id:
            await _run_sheets_pull()
        if settings.sync_out_enabled:
            await _run_outbox_processor()

        if settings.google_sheets_spreadsheet_id and now >= next_vitrina:
            next_vitrina = now + vitrina_interval
            await _run_vitrina_refresh()
        interval = _current_poll_interval()
        logger.debug("sync_poll_sleep_sec={} mode={}", interval, "active" if interval == int(settings.sync_poll_active_sec) else "idle")
        await asyncio.sleep(interval)


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


def _outbox_backoff_sec(attempts: int) -> int:
    tries = max(1, int(attempts))
    value = int(_OUTBOX_BASE_BACKOFF_SEC * (2 ** (tries - 1)))
    cap = max(int(settings.sync_poll_idle_sec), _OUTBOX_BASE_BACKOFF_SEC)
    return min(value, cap)


def _process_outbox_item(session, row: SyncOutbox) -> None:
    item = session.get(Item, row.entity_id)
    if item is None:
        row.processed_at = _utc_now()
        row.last_error = "item_not_found"
        return
    if item.type == "meeting":
        sync_out_meeting(session, item.id)
        return
    if item.status == "done":
        sync_task_completed(session, item.id)
        return
    if item.google_task_id:
        sync_task_updated(session, item.id)
        return
    sync_task_created(session, item.id)


async def _run_outbox_processor() -> None:
    stats = {"processed": 0, "success": 0, "failed": 0}
    try:
        async with _lock:
            logger.info("outbox push start")
            now = _utc_now()
            with get_session() as session:
                rows = list(
                    session.scalars(
                        select(SyncOutbox).where(
                            SyncOutbox.processed_at.is_(None),
                            ((SyncOutbox.next_retry_at.is_(None)) | (SyncOutbox.next_retry_at <= now)),
                        ).order_by(SyncOutbox.created_at.asc()).limit(_OUTBOX_BATCH_SIZE)
                    ).all()
                )
                for row in rows:
                    stats["processed"] += 1
                    try:
                        _process_outbox_item(session, row)
                        row.processed_at = _utc_now()
                        row.last_error = None
                        row.next_retry_at = None
                        stats["success"] += 1
                    except Exception as exc:
                        row.attempts = int(row.attempts or 0) + 1
                        row.last_error = str(exc)[:500]
                        row.next_retry_at = _utc_now() + timedelta(seconds=_outbox_backoff_sec(int(row.attempts)))
                        row.payload_json = json.dumps(
                            {
                                "entity_type": row.entity_type,
                                "entity_id": row.entity_id,
                                "operation": row.operation,
                            },
                            ensure_ascii=False,
                        )
                        stats["failed"] += 1
            logger.info("outbox push done stats={}", stats)
    except Exception as exc:
        logger.error("outbox push error: {}", exc)


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
