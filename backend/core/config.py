"""
core/config.py — All environment variables, constants, and email-domain helpers.
"""
import os
import secrets

# ── Environment Configuration ───────────────────────────────────────────────
DATABASE_URL       = os.environ.get("DATABASE_URL", "")
RESEND_API_KEY     = os.environ.get("RESEND_API_KEY", "")
RESEND_FROM        = os.environ.get("RESEND_FROM_EMAIL", "rahul@axiomsync.ai")
APP_URL            = os.environ.get("APP_URL", "https://sigint-v2.onrender.com")
_jwt_secret_env    = os.environ.get("JWT_SECRET", "")
if not _jwt_secret_env:
    _is_production = bool(os.environ.get("DATABASE_URL")) or os.environ.get("RENDER", "")
    if _is_production:
        raise RuntimeError(
            "FATAL: JWT_SECRET environment variable is not set. "
            "This is required in production — all user sessions depend on it. "
            "Generate one with: python3 -c \"import secrets; print(secrets.token_hex(32))\" "
            "and add it to your Render environment variables."
        )
    import warnings
    _jwt_secret_env = secrets.token_hex(32)
    warnings.warn(
        "JWT_SECRET env var is not set. A random secret was generated — "
        "all sessions will be invalidated on every restart. "
        "Set JWT_SECRET in your environment to fix this.",
        stacklevel=2,
    )
JWT_SECRET         = _jwt_secret_env
STRIPE_SECRET      = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_PUB         = os.environ.get("STRIPE_PUBLISHABLE_KEY", "")
STRIPE_WEBHOOK_SEC = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
QB_CLIENT_ID       = os.environ.get("INTUIT_CLIENT_ID", "")
QB_CLIENT_SECRET   = os.environ.get("INTUIT_CLIENT_SECRET", "")
QB_REDIRECT_URI    = os.environ.get("QB_REDIRECT_URI", "")
ALLOWED_EMAILS     = [e.strip().lower() for e in os.environ.get("ALLOWED_EMAILS", "").split(",") if e.strip()]
ALLOWED_DOMAINS    = [d.strip().lower() for d in os.environ.get("ALLOWED_DOMAINS", "").split(",") if d.strip()]
ADMIN_EMAIL        = os.environ.get("ADMIN_EMAIL", "rahul@axiomsync.ai")
_USE_PG            = bool(DATABASE_URL)

# ── Paths ────────────────────────────────────────────────────────────────────
from pathlib import Path
DB_PATH     = Path(__file__).parent.parent / "uploads" / "axiom.db"
UPLOADS_DIR = Path(__file__).parent.parent / "uploads"
UPLOADS_DIR.mkdir(exist_ok=True)

# ── CORS ─────────────────────────────────────────────────────────────────────
_ALLOWED_ORIGINS = [
    "https://app.axiomsync.ai",
    "http://localhost:5173",
    "http://localhost:5174",
    "http://localhost:3000",
]

# ── PostgreSQL per-table upsert conflict targets ──────────────────────────────
_PG_UPSERT: dict = {
    "company_settings":  ("(key, workspace_id)",         {"key", "workspace_id"}),
    "org_invites":       ("(token)",                     {"token", "id"}),
    "oauth_states":      ("(state)",                     {"state", "id"}),
    "connector_configs": ("(workspace_id, source_name)", {"workspace_id", "source_name", "id"}),
    "kpi_targets":       ("(kpi_key, workspace_id)",     {"kpi_key", "workspace_id", "id"}),
}

# ── Work-email validator ──────────────────────────────────────────────────────
_FREE_EMAIL_DOMAINS = {
    "gmail.com","googlemail.com","yahoo.com","yahoo.co.uk","yahoo.co.in",
    "hotmail.com","hotmail.co.uk","outlook.com","live.com","live.co.uk",
    "icloud.com","me.com","mac.com","aol.com","aol.co.uk",
    "protonmail.com","proton.me","tutanota.com","tutanota.de",
    "fastmail.com","fastmail.fm","zoho.com","yandex.com","yandex.ru",
    "mail.com","inbox.com","gmx.com","gmx.de","web.de",
    "msn.com","passport.com","windowslive.com",
}

def _email_domain(email: str) -> str:
    """Extract domain from email address."""
    return email.split("@")[-1].lower() if "@" in email else ""

def _is_free_email(email: str) -> bool:
    return _email_domain(email) in _FREE_EMAIL_DOMAINS

def _org_id_for_email(email: str) -> str:
    """
    Return the org/workspace ID for an email.
    Work emails → domain (e.g. acmecorp.com)
    Free/personal emails → full email (personal workspace)
    """
    domain = _email_domain(email)
    if not domain or domain in _FREE_EMAIL_DOMAINS:
        return email.lower()
    return domain

def _is_work_email(email: str) -> bool:
    """
    Returns True if the email is allowed to sign in.
    Priority:
      1. Exact match in ALLOWED_EMAILS whitelist (always allowed)
      2. Domain match in ALLOWED_DOMAINS whitelist (if set, restricts to those domains)
      3. If neither whitelist is set: allow all non-free-email domains
    """
    email = email.strip().lower()
    if ALLOWED_EMAILS and email in ALLOWED_EMAILS:
        return True
    if ALLOWED_DOMAINS:
        return _email_domain(email) in ALLOWED_DOMAINS
    return not _is_free_email(email)

# ── Conditional imports ───────────────────────────────────────────────────────
if _USE_PG:
    import psycopg2, psycopg2.extras  # noqa: F401

if STRIPE_SECRET:
    try:
        import stripe as _stripe
        _stripe.api_key = STRIPE_SECRET
    except ImportError:
        _stripe = None
else:
    _stripe = None

try:
    import jwt as _jwt
    _JWT_AVAILABLE = True
except ImportError:
    _JWT_AVAILABLE = False
    _jwt = None
