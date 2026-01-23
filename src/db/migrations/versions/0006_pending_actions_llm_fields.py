"""pending actions llm fields

Revision ID: 0006_pending_actions_llm_fields
Revises: 0005_pending_actions
Create Date: 2026-01-21 23:25:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "0006_pending_actions_llm_fields"
down_revision = "0005_pending_actions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("pending_actions", sa.Column("source", sa.String(length=16), nullable=False, server_default="nlu"))
    op.add_column("pending_actions", sa.Column("confidence", sa.Float(), nullable=False, server_default="0"))
    op.add_column("pending_actions", sa.Column("raw_text", sa.Text(), nullable=False, server_default=""))
    op.add_column("pending_actions", sa.Column("canonical_text", sa.Text(), nullable=False, server_default=""))
    op.add_column("pending_actions", sa.Column("missing_json", sa.Text(), nullable=False, server_default="[]"))


def downgrade() -> None:
    op.drop_column("pending_actions", "missing_json")
    op.drop_column("pending_actions", "canonical_text")
    op.drop_column("pending_actions", "raw_text")
    op.drop_column("pending_actions", "confidence")
    op.drop_column("pending_actions", "source")
