"""sync policy state and outbox

Revision ID: 0012_sync_policy_state_and_outbox
Revises: 0011_conflicts_table
Create Date: 2026-02-26 18:30:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "0012_sync_policy_state_and_outbox"
down_revision = "0011_conflicts_table"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("calendar_sync_state", sa.Column("active_until", sa.DateTime(timezone=True), nullable=True))

    op.create_table(
        "sync_outbox",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("entity_type", sa.String(length=32), nullable=False, server_default=sa.text("'item'")),
        sa.Column("entity_id", sa.String(length=36), nullable=False),
        sa.Column("operation", sa.String(length=32), nullable=False, server_default=sa.text("'upsert'")),
        sa.Column("payload_json", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_sync_outbox_entity_id", "sync_outbox", ["entity_id"])
    op.create_index("ix_sync_outbox_processed_at", "sync_outbox", ["processed_at"])
    op.create_index("ix_sync_outbox_next_retry_at", "sync_outbox", ["next_retry_at"])
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_sync_outbox_pending_entity_op
        ON sync_outbox(entity_type, entity_id, operation)
        WHERE processed_at IS NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_sync_outbox_pending_entity_op")
    op.drop_index("ix_sync_outbox_next_retry_at", table_name="sync_outbox")
    op.drop_index("ix_sync_outbox_processed_at", table_name="sync_outbox")
    op.drop_index("ix_sync_outbox_entity_id", table_name="sync_outbox")
    op.drop_table("sync_outbox")
    op.drop_column("calendar_sync_state", "active_until")

