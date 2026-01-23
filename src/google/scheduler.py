from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from loguru import logger
from sqlalchemy import select

from src.config import settings
from src.db.models import Item
from src.db.session import get_session
from src.google.sync_in import sync_in_calendar_window
from src.google.sync_out import sync_out_meeting

_lock = asyncio.Lock()


async def run_calendar_scheduler() -> None:
    in_interval = max(30, int(settings.sync_in_interval_sec))
    out_interval = max(30, int(settings.sync_out_interval_sec))
    logger.info(
        "calendar sync scheduler started (in={}s, out={}s)",
        in_interval,
        out_interval,
    )

    next_in = 0.0
    next_out = 0.0
    loop = asyncio.get_running_loop()

    while True:
        now = loop.time()
        if settings.sync_in_enabled and now >= next_in:
            next_in = now + in_interval
            await _run_pull()

        if settings.sync_out_enabled and now >= next_out:
            next_out = now + out_interval
            await _run_push()

        await asyncio.sleep(1)


async def _run_pull() -> None:
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
    except Exception as exc:
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
