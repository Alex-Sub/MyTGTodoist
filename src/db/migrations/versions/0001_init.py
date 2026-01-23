"""init

Revision ID: 0001_init
Revises: 
Create Date: 2026-01-20 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "projects",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_system", sa.Boolean(), server_default=sa.text("0"), nullable=False),
        sa.Column("sort_order", sa.Integer(), server_default=sa.text("100"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("name", name="uq_projects_name"),
    )

    op.create_table(
        "items",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("title", sa.String(length=500), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("project_id", sa.String(length=36), sa.ForeignKey("projects.id"), nullable=True),
        sa.Column("parent_id", sa.String(length=36), sa.ForeignKey("items.id"), nullable=True),
        sa.Column("depth", sa.Integer(), nullable=False),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_min", sa.Integer(), nullable=True),
        sa.Column("priority", sa.Integer(), server_default=sa.text("2"), nullable=False),
        sa.Column("value_score", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("effort", sa.String(length=8), nullable=True),
        sa.Column("review_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_touched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("calendar_id", sa.String(length=255), nullable=True),
        sa.Column("event_id", sa.String(length=255), nullable=True),
        sa.Column("ical_uid", sa.String(length=255), nullable=True),
        sa.Column("etag", sa.String(length=255), nullable=True),
        sa.Column("g_updated", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sync_state", sa.String(length=32), server_default=sa.text("'synced'"), nullable=False),
    )

    op.create_table(
        "item_events",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("item_id", sa.String(length=36), sa.ForeignKey("items.id"), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("meta_json", sa.Text(), nullable=True),
    )

    op.create_table(
        "attachments",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("item_id", sa.String(length=36), sa.ForeignKey("items.id"), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("drive_file_id", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "calendar_sync_state",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("calendar_id", sa.String(length=255), nullable=False),
        sa.Column("sync_token", sa.String(length=255), nullable=True),
        sa.Column("channel_id", sa.String(length=255), nullable=True),
        sa.Column("resource_id", sa.String(length=255), nullable=True),
        sa.Column("expiration", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("calendar_id", name="uq_calendar_sync_state_calendar_id"),
    )

    op.create_index("ix_items_status", "items", ["status"])
    op.create_index("ix_items_due_at", "items", ["due_at"])
    op.create_index("ix_items_scheduled_at", "items", ["scheduled_at"])
    op.create_index("ix_items_project_id", "items", ["project_id"])
    op.create_index("ix_items_parent_id", "items", ["parent_id"])
    op.create_index("ix_items_updated_at", "items", ["updated_at"])
    op.create_index("ix_item_events_item_id", "item_events", ["item_id"])
    op.create_index("ix_item_events_ts", "item_events", ["ts"])


def downgrade() -> None:
    op.drop_index("ix_item_events_ts", table_name="item_events")
    op.drop_index("ix_item_events_item_id", table_name="item_events")
    op.drop_index("ix_items_updated_at", table_name="items")
    op.drop_index("ix_items_parent_id", table_name="items")
    op.drop_index("ix_items_project_id", table_name="items")
    op.drop_index("ix_items_scheduled_at", table_name="items")
    op.drop_index("ix_items_due_at", table_name="items")
    op.drop_index("ix_items_status", table_name="items")
    op.drop_table("calendar_sync_state")
    op.drop_table("attachments")
    op.drop_table("item_events")
    op.drop_table("items")
    op.drop_table("projects")
