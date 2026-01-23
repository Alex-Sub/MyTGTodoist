import asyncio
import os
import sys
from pathlib import Path

import uvicorn
from dotenv import find_dotenv, load_dotenv
from loguru import logger

from src.core.asr_config import load_asr_config
from src.db.seed import seed_projects
from src.db.session import get_session
from src.logging_setup import setup_logging


def _is_true(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_env() -> None:
    env_path = find_dotenv(usecwd=True)
    if not env_path:
        candidate = Path(__file__).resolve().parents[1] / ".env"
        if candidate.exists():
            env_path = str(candidate)
    if env_path:
        logger.info("Loaded .env from {}", env_path)
        load_dotenv(env_path, override=True)
    else:
        logger.warning("No .env found")


def _log_asr_config(config) -> None:
    logger.info(
        "ASR configured: url={} timeout={} key_set={}",
        config.url or "<missing>",
        config.timeout_seconds or "<missing>",
        bool(config.api_key),
    )


def _require_asr_config(config) -> None:
    if config.url and config.api_key:
        return
    logger.error(
        "ASR FATAL: missing configuration (ASR_URL={}, ASR_API_KEY={})",
        config.url or "missing",
        "set" if config.api_key else "missing",
    )
    sys.exit(1)


def _run_migrations() -> None:
    from alembic import command
    from alembic.config import Config

    config_path = Path(__file__).resolve().parents[1] / "alembic.ini"
    alembic_cfg = Config(str(config_path))
    command.upgrade(alembic_cfg, "head")


async def _run_dev_polling() -> None:
    from src.telegram.webhook import start_polling
    from src.sync.sync_all import run_sync_loop

    config = uvicorn.Config("src.api.app:app", host="0.0.0.0", port=8000, reload=False)
    server = uvicorn.Server(config)

    server_task = asyncio.create_task(server.serve())
    polling_task = asyncio.create_task(start_polling())
    sync_task = asyncio.create_task(run_sync_loop())

    done, pending = await asyncio.wait(
        {server_task, polling_task},
        return_when=asyncio.FIRST_COMPLETED,
    )

    if polling_task in done and not server.should_exit:
        server.should_exit = True

    if not sync_task.done():
        sync_task.cancel()

    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    await asyncio.gather(sync_task, return_exceptions=True)


def main() -> None:
    _load_env()
    setup_logging()

    from src.config import settings

    asr_config = load_asr_config()
    _log_asr_config(asr_config)
    _require_asr_config(asr_config)
    dev_polling = settings.dev_polling or _is_true(os.getenv("DEV_POLLING"))
    if dev_polling:
        _run_migrations()
        with get_session() as session:
            seed_projects(session)
        try:
            asyncio.run(_run_dev_polling())
        except KeyboardInterrupt:
            return
        return

    uvicorn.run("src.api.app:app", host="0.0.0.0", port=8000, reload=False)


if __name__ == "__main__":
    main()
