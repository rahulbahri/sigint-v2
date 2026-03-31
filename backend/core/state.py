"""
core/state.py — All shared mutable global state.
Imported by routers; never imported by core modules (to avoid circular imports).
"""
import threading
from collections import defaultdict

# ── Rate-limiting state ───────────────────────────────────────────────────────
_rate_limit_store: dict = defaultdict(list)
_MAGIC_LINK_RATE: dict  = defaultdict(list)
_MAGIC_TOKEN_CACHE: dict = {}  # in-memory fallback when DB is unavailable

# ── Forecast engine state ─────────────────────────────────────────────────────
_FORECAST_BUILDING: bool = False
_FORECAST_ERROR: str     = ""
_FORECAST_LOCK           = threading.Lock()

# ── ELT availability ──────────────────────────────────────────────────────────
# These are set at import time in routers/connectors.py after the try/except import
_ELT_AVAILABLE: bool    = False
_ELT_IMPORT_ERROR: str  = ""
