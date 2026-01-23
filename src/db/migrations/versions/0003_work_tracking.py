"""work tracking fields

Revision ID: 0003_work_tracking
Revises: 0002_oauth_tokens
Create Date: 2026-01-20 14:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "0003_work_tracking"
down_revision = "0002_oauth_tokens"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("items", sa.Column("planned_min", sa.Integer(), nullable=True))
    op.add_column("items", sa.Column("actual_min", sa.Integer(), nullable=True))
    op.add_column(
        "items",
        sa.Column("working", sa.Boolean(), server_default=sa.text("0"), nullable=False),
    )
    op.add_column("items", sa.Column("work_started_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("items", "work_started_at")
    op.drop_column("items", "working")
    op.drop_column("items", "actual_min")
    op.drop_column("items", "planned_min")
