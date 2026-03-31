"""
tests/conftest.py — Shared fixtures for the FastAPI test suite.
"""
import os
import sys
import tempfile

# Insert backend directory at front of path so `import main` resolves correctly
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Create a temp SQLite DB for tests — never touch the real one
_tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DATABASE_URL"] = ""          # force SQLite (not Postgres)
os.environ["JWT_SECRET"] = "test-secret-not-for-production-xx"  # ≥32 bytes

# Patch DB_PATH before importing the app so it hits the temp DB
import core.config as _cfg
from pathlib import Path
_cfg.DB_PATH = Path(_tmp_db.name)

# Also patch core.database so it uses the already-resolved DB_PATH
import core.database as _db_mod
_db_mod.DB_PATH = Path(_tmp_db.name)

# Now it's safe to import the app
from main import app                          # noqa: E402
from core.database import init_db, get_db    # noqa: E402
from httpx import AsyncClient, ASGITransport  # noqa: E402
import jwt as _jwt                            # noqa: E402
import pytest                                 # noqa: E402

# Initialise the temp DB (creates all tables)
init_db()


# ── Helpers ───────────────────────────────────────────────────────────────────

_JWT_TEST_SECRET = "test-secret-not-for-production-xx"


def _make_token(email: str, org_id: str = None, role: str = "admin") -> str:
    from datetime import datetime, timedelta
    payload = {
        "email":  email,
        "org_id": org_id or email.split("@")[1],
        "role":   role,
        "exp":    datetime.utcnow() + timedelta(hours=1),
        "iat":    datetime.utcnow(),
    }
    return _jwt.encode(payload, _JWT_TEST_SECRET, algorithm="HS256")


def _make_expired_token(email: str) -> str:
    from datetime import datetime, timedelta
    payload = {
        "email":  email,
        "org_id": email.split("@")[1],
        "role":   "admin",
        "exp":    datetime.utcnow() - timedelta(hours=1),   # already expired
        "iat":    datetime.utcnow() - timedelta(hours=2),
    }
    return _jwt.encode(payload, _JWT_TEST_SECRET, algorithm="HS256")


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="session")
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


@pytest.fixture(autouse=True)
def reset_rate_limit():
    """
    Clear the in-memory rate-limit store before every test so that tests
    are independent and don't inadvertently trigger 429 due to prior requests.
    """
    from core.state import _rate_limit_store
    _rate_limit_store.clear()
    yield
    # Optionally clear after as well for cleanliness
    _rate_limit_store.clear()


@pytest.fixture
def auth_headers():
    token = _make_token("alice@testcorp.com", "testcorp.com")
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def other_auth_headers():
    """Headers for a different workspace — must not see alice's data."""
    token = _make_token("bob@othercorp.com", "othercorp.com")
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def expired_headers():
    token = _make_expired_token("carol@testcorp.com")
    return {"Authorization": f"Bearer {token}"}
