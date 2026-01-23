"""oauth_tokens

Revision ID: 0002_oauth_tokens
Revises: 0001_init
Create Date: 2026-01-20 13:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "0002_oauth_tokens"
down_revision = "0001_init"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "oauth_tokens",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("access_token", sa.Text(), nullable=False),
        sa.Column("refresh_token", sa.Text(), nullable=True),
        sa.Column("token_type", sa.String(length=32), nullable=False),
        sa.Column("expiry_ts", sa.Integer(), nullable=False),
        sa.Column("scope", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("provider", name="uq_oauth_tokens_provider"),
    )


def downgrade() -> None:
    op.drop_table("oauth_tokens")
