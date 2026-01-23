from __future__ import annotations

import secrets
import time
from typing import Optional
from urllib.parse import urlencode

import httpx
from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import settings
from src.db.models import OAuthToken
from src.db.session import get_session

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"

_STATE_TTL_SEC = 600
_state_store: dict[str, float] = {}


def _now_ts() -> int:
    return int(time.time())


def _cleanup_states() -> None:
    now = _now_ts()
    expired = [key for key, ts in _state_store.items() if now - ts > _STATE_TTL_SEC]
    for key in expired:
        _state_store.pop(key, None)


def generate_state() -> str:
    _cleanup_states()
    value = secrets.token_urlsafe(24)
    _state_store[value] = _now_ts()
    return value


def validate_state(state: str) -> bool:
    _cleanup_states()
    ts = _state_store.pop(state, None)
    if ts is None:
        return False
    return _now_ts() - ts <= _STATE_TTL_SEC


def build_auth_url(state: str) -> str:
    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": settings.google_redirect_uri,
        "response_type": "code",
        "scope": " ".join(settings.google_scopes),
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"


def _save_tokens(session: Session, tokens: dict) -> OAuthToken:
    access_token = tokens["access_token"]
    refresh_token = tokens.get("refresh_token")
    token_type = tokens.get("token_type", "Bearer")
    expires_in = int(tokens.get("expires_in", 3600))
    expiry_ts = _now_ts() + expires_in
    scope = tokens.get("scope")

    existing = session.scalar(select(OAuthToken).where(OAuthToken.provider == "google"))
    if existing:
        existing.access_token = access_token
        existing.token_type = token_type
        existing.expiry_ts = expiry_ts
        existing.scope = scope or existing.scope
        if refresh_token:
            existing.refresh_token = refresh_token
        session.commit()
        session.refresh(existing)
        return existing

    token = OAuthToken(
        provider="google",
        access_token=access_token,
        refresh_token=refresh_token,
        token_type=token_type,
        expiry_ts=expiry_ts,
        scope=scope,
    )
    session.add(token)
    session.commit()
    session.refresh(token)
    return token


def exchange_code_for_tokens(code: str) -> dict:
    payload = {
        "client_id": settings.google_client_id,
        "client_secret": settings.google_client_secret,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": settings.google_redirect_uri,
    }

    with httpx.Client(timeout=10.0) as client:
        response = client.post(GOOGLE_TOKEN_URL, data=payload)
        response.raise_for_status()
        return response.json()


def get_access_token() -> str:
    with get_session() as session:
        token = session.scalar(select(OAuthToken).where(OAuthToken.provider == "google"))
        if not token:
            raise RuntimeError("Google OAuth token not found")

        if token.expiry_ts > _now_ts() + 30:
            return token.access_token

        refresh_token = token.refresh_token
        if not refresh_token:
            raise RuntimeError("Google OAuth refresh token not found")

        payload = {
            "client_id": settings.google_client_id,
            "client_secret": settings.google_client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
        with httpx.Client(timeout=10.0) as client:
            response = client.post(GOOGLE_TOKEN_URL, data=payload)
            response.raise_for_status()
            data = response.json()

        logger.info("Refreshed Google access token (expiry in {}s)", data.get("expires_in"))
        _save_tokens(session, data)
        return data["access_token"]


def store_tokens(tokens: dict) -> OAuthToken:
    with get_session() as session:
        return _save_tokens(session, tokens)


def get_token_status() -> dict:
    with get_session() as session:
        token = session.scalar(select(OAuthToken).where(OAuthToken.provider == "google"))
        if not token:
            return {"authorized": False, "expiry_ts": None}
        return {"authorized": token.expiry_ts > _now_ts(), "expiry_ts": token.expiry_ts}
