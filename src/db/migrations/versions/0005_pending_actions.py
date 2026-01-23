"""pending actions

Revision ID: 0005_pending_actions
Revises: 0004_calendar_sync_state_fields
Create Date: 2026-01-21 23:10:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "0005_pending_actions"
down_revision = "0004_calendar_sync_state_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pending_actions",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("chat_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("intent", sa.String(length=64), nullable=False),
        sa.Column("args_json", sa.Text(), nullable=False),
        sa.Column("raw_head", sa.String(length=100), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_pending_actions_chat_user",
        "pending_actions",
        ["chat_id", "user_id"],
    )
    op.create_index(
        "ix_pending_actions_expires_at",
        "pending_actions",
        ["expires_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_pending_actions_expires_at", table_name="pending_actions")
    op.drop_index("ix_pending_actions_chat_user", table_name="pending_actions")
    op.drop_table("pending_actions")
