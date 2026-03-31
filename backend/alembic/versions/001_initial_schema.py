"""initial_schema

Revision ID: 001
Revises:
Create Date: 2026-03-31

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
            id SERIAL PRIMARY KEY,
            filename TEXT,
            uploaded_at TEXT,
            row_count INTEGER,
            detected_columns TEXT,
            workspace_id TEXT DEFAULT ''
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS monthly_data (
            id SERIAL PRIMARY KEY,
            upload_id INTEGER,
            year INTEGER,
            month INTEGER,
            data_json TEXT,
            workspace_id TEXT DEFAULT ''
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS kpi_targets (
            kpi_key TEXT,
            target_value REAL,
            unit TEXT,
            direction TEXT,
            workspace_id TEXT DEFAULT '',
            PRIMARY KEY (kpi_key, workspace_id)
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS projection_uploads (
            id SERIAL PRIMARY KEY,
            filename TEXT,
            uploaded_at TEXT,
            row_count INTEGER,
            detected_columns TEXT,
            workspace_id TEXT DEFAULT '',
            version_label TEXT DEFAULT 'v1'
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS projection_monthly_data (
            id SERIAL PRIMARY KEY,
            projection_upload_id INTEGER,
            year INTEGER,
            month INTEGER,
            data_json TEXT,
            workspace_id TEXT DEFAULT '',
            version_label TEXT DEFAULT 'v1'
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS kpi_annotations (
            id SERIAL PRIMARY KEY,
            kpi_key TEXT NOT NULL,
            period TEXT NOT NULL,
            note TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(kpi_key, period)
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS kpi_accountability (
            kpi_key TEXT,
            owner TEXT DEFAULT '',
            due_date TEXT DEFAULT '',
            status TEXT DEFAULT 'open',
            last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
            workspace_id TEXT DEFAULT '',
            PRIMARY KEY (kpi_key, workspace_id)
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS company_settings (
            key TEXT,
            value TEXT,
            workspace_id TEXT DEFAULT '',
            PRIMARY KEY (key, workspace_id)
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id SERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            entity_type TEXT,
            entity_id TEXT,
            description TEXT NOT NULL,
            "user" TEXT DEFAULT 'system',
            ip_address TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            workspace_id TEXT DEFAULT ''
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS annotations (
            id SERIAL PRIMARY KEY,
            kpi_key TEXT NOT NULL,
            period TEXT NOT NULL,
            note TEXT NOT NULL,
            author TEXT DEFAULT 'CFO',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            workspace_id TEXT DEFAULT ''
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_annotations_kpi ON annotations(kpi_key, period)
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS recommendation_outcomes (
            id SERIAL PRIMARY KEY,
            kpi_key TEXT NOT NULL,
            action_text TEXT NOT NULL,
            started_at TEXT DEFAULT CURRENT_TIMESTAMP,
            resolved_at TEXT,
            before_value REAL,
            after_value REAL,
            before_status TEXT,
            after_status TEXT,
            outcome_notes TEXT,
            was_effective INTEGER DEFAULT NULL,
            workspace_id TEXT DEFAULT ''
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS decisions (
            id SERIAL PRIMARY KEY,
            workspace_id TEXT DEFAULT '',
            title TEXT NOT NULL,
            the_decision TEXT NOT NULL,
            rationale TEXT DEFAULT '',
            kpi_context TEXT DEFAULT '[]',
            outcome TEXT DEFAULT '',
            decided_by TEXT DEFAULT 'CFO',
            status TEXT DEFAULT 'active',
            decided_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_decisions_workspace ON decisions(workspace_id)
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS magic_tokens (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL,
            token TEXT NOT NULL UNIQUE,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            expires_at TEXT NOT NULL,
            used INTEGER DEFAULT 0
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            org_id TEXT DEFAULT '',
            role TEXT DEFAULT 'admin',
            display_name TEXT DEFAULT '',
            status TEXT DEFAULT 'active',
            invited_by TEXT DEFAULT '',
            org_migrated INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            last_login TEXT
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS organisations (
            id TEXT PRIMARY KEY,
            name TEXT DEFAULT '',
            plan TEXT DEFAULT 'free',
            invite_only INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS org_invites (
            id SERIAL PRIMARY KEY,
            org_id TEXT NOT NULL,
            email TEXT NOT NULL,
            invited_by TEXT NOT NULL,
            token TEXT UNIQUE NOT NULL,
            expires_at TEXT NOT NULL,
            accepted INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS saved_scenarios (
            id SERIAL PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            name TEXT NOT NULL,
            levers_json TEXT NOT NULL,
            notes TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS connector_configs (
            id SERIAL PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            source_name TEXT NOT NULL,
            config_json TEXT DEFAULT '{}',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (workspace_id, source_name)
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS oauth_states (
            id SERIAL PRIMARY KEY,
            state TEXT UNIQUE NOT NULL,
            workspace_id TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            expires_at TEXT NOT NULL,
            used INTEGER DEFAULT 0
        )
    """)

    # Workspace indexes
    for tbl in [
        "uploads", "monthly_data", "kpi_targets", "projection_uploads",
        "projection_monthly_data", "kpi_accountability", "annotations",
        "recommendation_outcomes", "audit_log", "company_settings",
    ]:
        op.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{tbl}_workspace ON {tbl}(workspace_id)"
        )


def downgrade():
    # Drop indexes first, then tables in reverse dependency order
    for tbl in [
        "uploads", "monthly_data", "kpi_targets", "projection_uploads",
        "projection_monthly_data", "kpi_accountability", "annotations",
        "recommendation_outcomes", "audit_log", "company_settings",
    ]:
        op.execute(f"DROP INDEX IF EXISTS idx_{tbl}_workspace")

    op.execute("DROP INDEX IF EXISTS idx_decisions_workspace")
    op.execute("DROP INDEX IF EXISTS idx_annotations_kpi")

    op.execute("DROP TABLE IF EXISTS oauth_states")
    op.execute("DROP TABLE IF EXISTS connector_configs")
    op.execute("DROP TABLE IF EXISTS saved_scenarios")
    op.execute("DROP TABLE IF EXISTS org_invites")
    op.execute("DROP TABLE IF EXISTS organisations")
    op.execute("DROP TABLE IF EXISTS users")
    op.execute("DROP TABLE IF EXISTS magic_tokens")
    op.execute("DROP TABLE IF EXISTS decisions")
    op.execute("DROP TABLE IF EXISTS recommendation_outcomes")
    op.execute("DROP TABLE IF EXISTS annotations")
    op.execute("DROP TABLE IF EXISTS audit_log")
    op.execute("DROP TABLE IF EXISTS company_settings")
    op.execute("DROP TABLE IF EXISTS kpi_accountability")
    op.execute("DROP TABLE IF EXISTS kpi_annotations")
    op.execute("DROP TABLE IF EXISTS projection_monthly_data")
    op.execute("DROP TABLE IF EXISTS projection_uploads")
    op.execute("DROP TABLE IF EXISTS kpi_targets")
    op.execute("DROP TABLE IF EXISTS monthly_data")
    op.execute("DROP TABLE IF EXISTS uploads")
