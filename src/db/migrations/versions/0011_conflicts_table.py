"""conflicts table

Revision ID: 0011_conflicts_table
Revises: 0010_google_task_sync_fields
Create Date: 2026-02-22 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "0011_conflicts_table"
down_revision = "0010_google_task_sync_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "conflicts",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("item_id", sa.String(length=36), sa.ForeignKey("items.id"), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("field_name", sa.String(length=64), nullable=False),
        sa.Column("local_value", sa.Text(), nullable=True),
        sa.Column("remote_value", sa.Text(), nullable=True),
        sa.Column("remote_patch_json", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'open'")),
        sa.Column("resolution", sa.String(length=32), nullable=True),
        sa.Column("row_ref", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_conflicts_item_id", "conflicts", ["item_id"])
    op.create_index("ix_conflicts_source", "conflicts", ["source"])
    op.create_index("ix_conflicts_status", "conflicts", ["status"])


def downgrade() -> None:
    op.drop_index("ix_conflicts_status", table_name="conflicts")
    op.drop_index("ix_conflicts_source", table_name="conflicts")
    op.drop_index("ix_conflicts_item_id", table_name="conflicts")
    op.drop_table("conflicts")
