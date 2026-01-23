import asyncio

from fastapi import FastAPI

from src.api.routes_google_oauth import router as google_oauth_router
from src.api.routes_health import router as health_router
from src.api.routes_asr import router as asr_router
from src.config import settings
from src.db.seed import seed_projects
from src.db.session import get_session
from src.google.scheduler import run_calendar_scheduler
from src.sync.sync_all import run_sync_loop
from src.telegram.webhook import router as telegram_router

app = FastAPI(title="todo-telegram-calendar")

app.include_router(health_router)
app.include_router(google_oauth_router)
app.include_router(asr_router)
app.include_router(telegram_router)

_scheduler_task: asyncio.Task | None = None
_sync_task: asyncio.Task | None = None


@app.on_event("startup")
async def on_startup() -> None:
    with get_session() as session:
        seed_projects(session)
    if settings.sync_in_enabled or settings.sync_out_enabled:
        global _scheduler_task
        if _scheduler_task is None or _scheduler_task.done():
            _scheduler_task = asyncio.create_task(run_calendar_scheduler())
    if not settings.dev_polling:
        global _sync_task
        if _sync_task is None or _sync_task.done():
            _sync_task = asyncio.create_task(run_sync_loop())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if _scheduler_task and not _scheduler_task.done():
        _scheduler_task.cancel()
    if _sync_task and not _sync_task.done():
        _sync_task.cancel()
