"""bidir sync state and conflicts

Revision ID: 0013_bidir_sync_state_and_conflicts
Revises: 0012_sync_policy_state_and_outbox
Create Date: 2026-02-28 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "0013_bidir_sync_state_and_conflicts"
down_revision = "0012_sync_policy_state_and_outbox"
branch_labels = None
depends_on = None


def _column_names(conn, table: str) -> set[str]:
    rows = conn.execute(sa.text(f"PRAGMA table_info({table})")).fetchall()
    return {row[1] for row in rows}


def upgrade() -> None:
    conn = op.get_bind()
    cols = _column_names(conn, "items")
    if "notes" not in cols:
        op.execute("ALTER TABLE items ADD COLUMN notes TEXT NULL")

    op.create_table(
        "sync_state",
        sa.Column("item_id", sa.String(length=36), primary_key=True),
        sa.Column("calendar_event_id", sa.String(length=255), nullable=True),
        sa.Column("last_db_hash", sa.String(length=64), nullable=True),
        sa.Column("last_sheet_hash", sa.String(length=64), nullable=True),
        sa.Column("last_calendar_hash", sa.String(length=64), nullable=True),
        sa.Column("last_sheet_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_calendar_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_db_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_conflict_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "sync_conflicts",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("item_id", sa.String(length=36), nullable=False),
        sa.Column("db_payload_json", sa.Text(), nullable=True),
        sa.Column("sheet_payload_json", sa.Text(), nullable=True),
        sa.Column("calendar_payload_json", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'open'")),
        sa.Column("resolution", sa.String(length=32), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_sync_conflicts_item_id", "sync_conflicts", ["item_id"])
    op.create_index("ix_sync_conflicts_status", "sync_conflicts", ["status"])


def downgrade() -> None:
    op.drop_index("ix_sync_conflicts_status", table_name="sync_conflicts")
    op.drop_index("ix_sync_conflicts_item_id", table_name="sync_conflicts")
    op.drop_table("sync_conflicts")
    op.drop_table("sync_state")
