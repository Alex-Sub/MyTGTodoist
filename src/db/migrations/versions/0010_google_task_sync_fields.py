"""google task sync fields

Revision ID: 0010_google_task_sync_fields
Revises: 0009_items_tasks_subtasks
Create Date: 2026-02-22 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "0010_google_task_sync_fields"
down_revision = "0009_items_tasks_subtasks"
branch_labels = None
depends_on = None


def _column_names(conn, table: str) -> set[str]:
    rows = conn.execute(sa.text(f"PRAGMA table_info({table})")).fetchall()
    return {row[1] for row in rows}


def upgrade() -> None:
    conn = op.get_bind()
    cols = _column_names(conn, "items")
    if "google_task_id" not in cols:
        op.execute("ALTER TABLE items ADD COLUMN google_task_id TEXT NULL")
    if "google_parent_task_id" not in cols:
        op.execute("ALTER TABLE items ADD COLUMN google_parent_task_id TEXT NULL")
    if "google_sync_status" not in cols:
        op.execute("ALTER TABLE items ADD COLUMN google_sync_status TEXT NOT NULL DEFAULT 'pending'")
    if "google_sync_attempts" not in cols:
        op.execute("ALTER TABLE items ADD COLUMN google_sync_attempts INTEGER NOT NULL DEFAULT 0")
    if "google_sync_error" not in cols:
        op.execute("ALTER TABLE items ADD COLUMN google_sync_error TEXT NULL")
    if "google_synced_at" not in cols:
        op.execute("ALTER TABLE items ADD COLUMN google_synced_at DATETIME NULL")

    op.execute("CREATE INDEX IF NOT EXISTS ix_items_google_task_id ON items(google_task_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_items_google_parent_task_id ON items(google_parent_task_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_items_google_sync_status ON items(google_sync_status)")


def downgrade() -> None:
    # SQLite cannot drop columns safely in-place.
    pass
