from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse, RedirectResponse
from loguru import logger

from src.config import settings
from src.google.auth import (
    build_auth_url,
    exchange_code_for_tokens,
    generate_state,
    get_token_status,
    store_tokens,
    validate_state,
)
from src.google.calendar_client import CalendarClient
from src.db.session import get_session
from src.google.sync_in import sync_in_calendar

router = APIRouter()


@router.get("/google/auth")
async def google_auth() -> RedirectResponse:
    state = generate_state()
    url = build_auth_url(state)
    return RedirectResponse(url=url)


@router.get("/oauth2/callback", response_class=PlainTextResponse)
async def google_callback(code: str, state: str) -> str:
    if not validate_state(state):
        raise HTTPException(status_code=400, detail="Invalid state")

    tokens = exchange_code_for_tokens(code)
    store_tokens(tokens)
    logger.info("Google OAuth tokens stored (provider=google)")
    return "OK, authorized"


@router.get("/google/status")
async def google_status() -> dict:
    return get_token_status()


@router.get("/google/event/{event_id}")
async def google_event(event_id: str) -> dict:
    client = CalendarClient()
    return client.get_event(settings.google_calendar_id_default, event_id)


@router.post("/google/sync_in")
async def google_sync_in() -> dict:
    with get_session() as session:
        return sync_in_calendar(session, settings.google_calendar_id_default)
