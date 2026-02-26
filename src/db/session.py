from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine, event, select, text
from sqlalchemy.orm import Session, sessionmaker

from src.config import settings
from src.db.models import CalendarSyncState, Item


def build_database_url() -> str:
    db_path = Path(settings.sqlite_path)
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path
    db_path = db_path.resolve()
    return f"sqlite+pysqlite:///{db_path.as_posix()}"


engine = create_engine(build_database_url(), future=True)


@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_connection, _connection_record) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
)

_SYNC_STATE_KEY = "__global_sync_policy__"


def _collect_db_change_snapshot(session: Session) -> tuple[bool, set[str]]:
    has_changes = bool(session.new or session.dirty or session.deleted)
    item_ids: set[str] = set()
    for obj in list(session.new) + list(session.dirty) + list(session.deleted):
        if isinstance(obj, Item):
            oid = str(getattr(obj, "id", "") or "").strip()
            if oid:
                item_ids.add(oid)
    return has_changes, item_ids


def _apply_sync_policy_after_commit(session: Session, *, item_ids: set[str]) -> None:
    now = datetime.now(timezone.utc)
    active_until = now + timedelta(minutes=max(1, int(settings.sync_active_window_min)))

    state = session.scalar(select(CalendarSyncState).where(CalendarSyncState.calendar_id == _SYNC_STATE_KEY))
    if state is None:
        state = CalendarSyncState(calendar_id=_SYNC_STATE_KEY)
        session.add(state)
        session.flush()
    state.active_until = active_until

    if item_ids:
        for item_id in sorted(item_ids):
            upd = session.execute(
                text(
                    """
                    UPDATE sync_outbox
                    SET payload_json = :payload_json,
                        updated_at = :now,
                        last_error = NULL
                    WHERE entity_type = 'item'
                      AND entity_id = :entity_id
                      AND operation = 'upsert'
                      AND processed_at IS NULL
                    """
                ),
                {
                    "entity_id": item_id,
                    "payload_json": json.dumps({"item_id": item_id}, ensure_ascii=False),
                    "now": now,
                },
            )
            if int(getattr(upd, "rowcount", 0) or 0) > 0:
                continue
            session.execute(
                text(
                    """
                    INSERT INTO sync_outbox
                        (id, entity_type, entity_id, operation, payload_json, attempts, next_retry_at, last_error, processed_at, created_at, updated_at)
                    VALUES
                        (:id, 'item', :entity_id, 'upsert', :payload_json, 0, NULL, NULL, NULL, :now, :now)
                    """
                ),
                {
                    "id": f"obx-{item_id}-{int(now.timestamp())}",
                    "entity_id": item_id,
                    "payload_json": json.dumps({"item_id": item_id}, ensure_ascii=False),
                    "now": now,
                },
            )

    session.commit()


@contextmanager
def get_session() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        has_changes, item_ids = _collect_db_change_snapshot(session)
        session.commit()
        if has_changes:
            try:
                _apply_sync_policy_after_commit(session, item_ids=item_ids)
            except Exception as exc:  # pragma: no cover - defensive
                session.rollback()
                logging.getLogger(__name__).warning("sync_policy_post_commit_failed err=%s", type(exc).__name__)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
