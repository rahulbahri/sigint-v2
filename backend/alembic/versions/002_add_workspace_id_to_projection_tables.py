"""add_workspace_id_to_projection_tables

Revision ID: 002
Revises: 001
Create Date: 2026-03-31

Ensures workspace_id exists on projection_uploads and projection_monthly_data.
Since 001 already includes these columns, this migration is a safe no-op on
fresh databases. On older SQLite databases it adds the columns if missing.
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import reflection

# revision identifiers, used by Alembic.
revision = '002'
down_revision = '001'
branch_labels = None
depends_on = None


def _column_exists(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    insp = reflection.Inspector.from_engine(bind)
    cols = [c["name"] for c in insp.get_columns(table_name)]
    return column_name in cols


def upgrade():
    for table in ("projection_uploads", "projection_monthly_data"):
        if not _column_exists(table, "workspace_id"):
            op.add_column(
                table,
                sa.Column("workspace_id", sa.Text(), nullable=True, server_default=""),
            )


def downgrade():
    pass  # non-destructive: don't remove columns on downgrade
