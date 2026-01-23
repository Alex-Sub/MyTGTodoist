"""pending actions stage meta

Revision ID: 0008_pending_actions_stage_meta
Revises: 0007_pending_actions_awaiting_field
Create Date: 2026-01-22 03:30:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "0008_pending_actions_stage_meta"
down_revision = "0007_pending_actions_awaiting_field"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("pending_actions", sa.Column("action_type", sa.String(length=64), nullable=True))
    op.add_column("pending_actions", sa.Column("stage", sa.String(length=32), nullable=True))
    op.add_column("pending_actions", sa.Column("meta_json", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("pending_actions", "meta_json")
    op.drop_column("pending_actions", "stage")
    op.drop_column("pending_actions", "action_type")
