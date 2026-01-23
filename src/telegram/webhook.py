from fastapi import APIRouter
from aiogram.types import Update

from src.config import settings
from src.telegram.bot import bot, dp

router = APIRouter()


@router.post(settings.webhook_path)
async def telegram_webhook(update: Update) -> dict:
    await dp.feed_update(bot, update)
    return {"ok": True}


async def start_polling() -> None:
    await dp.start_polling(bot, drop_pending_updates=True)
