"""items tasks/subtasks fields

Revision ID: 0009_items_tasks_subtasks
Revises: 0008_pending_actions_stage_meta
Create Date: 2026-02-06 19:50:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "0009_items_tasks_subtasks"
down_revision = "0008_pending_actions_stage_meta"
branch_labels = None
depends_on = None


def _column_names(conn, table: str) -> set[str]:
    rows = conn.execute(sa.text(f"PRAGMA table_info({table})")).fetchall()
    return {row[1] for row in rows}


def upgrade() -> None:
    conn = op.get_bind()
    cols = _column_names(conn, "items")
    if "parent_id" not in cols:
        op.execute("ALTER TABLE items ADD COLUMN parent_id TEXT NULL")
    if "type" not in cols:
        op.execute("ALTER TABLE items ADD COLUMN type TEXT NOT NULL DEFAULT 'task'")
    op.execute("CREATE INDEX IF NOT EXISTS ix_items_parent_id ON items(parent_id)")


def downgrade() -> None:
    # SQLite cannot drop columns safely in-place.
    pass
