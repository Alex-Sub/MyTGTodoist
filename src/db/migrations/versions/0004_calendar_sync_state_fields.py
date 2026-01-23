"""calendar sync state fields

Revision ID: 0004_calendar_sync_state_fields
Revises: 0003_work_tracking
Create Date: 2026-01-20 15:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "0004_calendar_sync_state_fields"
down_revision = "0003_work_tracking"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("calendar_sync_state", sa.Column("last_sync_status", sa.String(length=32), nullable=True))
    op.add_column("calendar_sync_state", sa.Column("last_sync_error", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("calendar_sync_state", "last_sync_error")
    op.drop_column("calendar_sync_state", "last_sync_status")
