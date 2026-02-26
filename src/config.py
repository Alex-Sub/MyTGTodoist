from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    dev_polling: bool = True
    telegram_bot_token: str = ""
    timezone: str = "Europe/Moscow"
    sqlite_path: str = "data/app.db"
    log_path: str = "logs/app.log"
    base_url: str = "https://example.com"
    webhook_path: str = "/telegram/webhook"
    ml_core_url: str = "http://host.docker.internal:19000"
    asr_service_url_legacy: str = ""
    google_client_id: str = ""
    google_client_secret: str = ""
    google_redirect_uri: str = "http://localhost:8000/oauth2/callback"
    google_scopes: list[str] = [
        "https://www.googleapis.com/auth/calendar",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/tasks",
    ]
    google_calendar_id_default: str = "primary"
    single_active_work: bool = False
    sync_in_enabled: bool = True
    sync_in_interval_sec: int = 60
    sync_out_enabled: bool = True
    sync_out_interval_sec: int = 60
    sync_window_days: int = 7
    sync_timezone: str | None = None
    sync_active_window_min: int = 30
    sync_poll_active_sec: int = 30
    sync_poll_idle_sec: int = 21600
    google_task_sync_retry_enabled: bool = True
    google_task_sync_retry_interval_sec: int = 60
    google_tasks_pull_interval_sec: int = 120
    google_sheets_pull_interval_sec: int = 180
    google_sheets_spreadsheet_id: str = ""
    google_sheets_range: str = "Tasks!A1:Z"
    google_vitrina_sheet_name: str = "VITRINA_TASKS"
    google_ops_log_sheet_name: str = "OPS_LOG"
    google_vitrina_refresh_interval_sec: int = 3600
    google_ops_retention_days: int = 14
    google_drive_enabled: bool = True
    google_drive_folder_name: str = "TGTodoist Exports"
    google_drive_mode: str = "latest"
    backend_url: str = "http://127.0.0.1:8000"


settings = Settings()
