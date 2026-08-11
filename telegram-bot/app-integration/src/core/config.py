from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception as e:
        raise ValueError(f"{name} must be an integer") from e


@dataclass(frozen=True)
class AppConfig:
    app_id: str
    app_version: str
    app_env: str

    local_db_path: str
    cloud_buffer_dsn: str

    global_dict_url: str
    global_dict_path: str
    app_dict_path: str

    buffer_cleanup_delivered_days: int
    buffer_cleanup_keep_last_per_user: int
    queue_replay_ttl_seconds: int
    clarification_ttl_sec: int
    command_dedup_reservation_ttl_sec: int


def load_config() -> AppConfig:
    app_id = _env("APP_ID")
    app_version = _env("APP_VERSION")
    app_env = _env("APP_ENV", "dev").lower() or "dev"

    if not app_id:
        raise ValueError("APP_ID is required")
    if not app_version:
        raise ValueError("APP_VERSION is required")

    local_db_path = _env("LOCAL_DB_PATH", os.path.join("app-integration", "data", "local.db"))
    cloud_buffer_dsn = _env("CLOUD_BUFFER_DSN", f"sqlite:///{os.path.join('app-integration','data','cloud.db')}")

    global_dict_url = _env("GLOBAL_DICT_URL", "")
    global_dict_path = _env("GLOBAL_DICT_PATH", "")
    app_dict_path = _env("APP_DICT_PATH", os.path.join("app-integration", "src", "dictionaries", "app_dict.yaml"))

    buffer_cleanup_delivered_days = _env_int("BUFFER_CLEANUP_DELIVERED_DAYS", 7)
    buffer_cleanup_keep_last_per_user = _env_int("BUFFER_CLEANUP_KEEP_LAST_PER_USER", 200)
    queue_replay_ttl_seconds = _env_int("QUEUE_REPLAY_TTL_SECONDS", 600)
    clarification_ttl_sec = _env_int("CLARIFICATION_TTL_SEC", 300)
    command_dedup_reservation_ttl_sec = _env_int("COMMAND_DEDUP_RESERVATION_TTL_SEC", 120)

    return AppConfig(
        app_id=app_id,
        app_version=app_version,
        app_env=app_env,
        local_db_path=local_db_path,
        cloud_buffer_dsn=cloud_buffer_dsn,
        global_dict_url=global_dict_url,
        global_dict_path=global_dict_path,
        app_dict_path=app_dict_path,
        buffer_cleanup_delivered_days=buffer_cleanup_delivered_days,
        buffer_cleanup_keep_last_per_user=buffer_cleanup_keep_last_per_user,
        queue_replay_ttl_seconds=queue_replay_ttl_seconds,
        clarification_ttl_sec=clarification_ttl_sec,
        command_dedup_reservation_ttl_sec=command_dedup_reservation_ttl_sec,
    )
