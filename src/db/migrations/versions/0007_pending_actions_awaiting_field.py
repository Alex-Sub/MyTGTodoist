"""pending actions awaiting field

Revision ID: 0007_pending_actions_awaiting_field
Revises: 0006_pending_actions_llm_fields
Create Date: 2026-01-22 00:10:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "0007_pending_actions_awaiting_field"
down_revision = "0006_pending_actions_llm_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("pending_actions", sa.Column("awaiting_field", sa.String(length=32), nullable=True))


def downgrade() -> None:
    op.drop_column("pending_actions", "awaiting_field")
