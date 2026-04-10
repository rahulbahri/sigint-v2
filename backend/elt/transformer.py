"""
Transformer — maps raw source records into canonical tables.

For each source+entity_type combination there is a _transform_* method.
Auto-detected field mappings are stored in the field_mappings table.
User-confirmed mappings always take precedence over auto-detection.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

_logger = logging.getLogger(__name__)


# ── Per-connector unit conventions ───────────────────────────────────────────
# Stripe and Brex send amounts in cents; all others in standard currency units.
CONNECTOR_UNIT_CONFIG = {
    "stripe":        {"amount_divisor": 100},
    "brex":          {"amount_divisor": 100},
    "quickbooks":    {"amount_divisor": 1},
    "xero":          {"amount_divisor": 1},
    "hubspot":       {"amount_divisor": 1},
    "salesforce":    {"amount_divisor": 1},
    "shopify":       {"amount_divisor": 1},
    "ramp":          {"amount_divisor": 1},
    "netsuite":      {"amount_divisor": 1},
    "sage_intacct":  {"amount_divisor": 1},
    "snowflake":     {"amount_divisor": 1},
    "google_sheets": {"amount_divisor": 1},
    "seed":          {"amount_divisor": 1},
}

# ── Explicit per-connector field mappings ─────────────────────────────────────
# These bypass the auto-guesser entirely. Keyed by (source, entity_type).
# Only the fields listed here are extracted; all other source fields are ignored.
# This prevents mismatches like Stripe's 12 "amount_*" fields all mapping to "amount".

EXPLICIT_FIELD_MAPS: dict[tuple[str, str], dict[str, str]] = {
    ("stripe", "revenue"): {
        "id": "source_id", "amount": "amount", "currency": "currency",
        "created": "period", "customer": "customer_id",
        "payment_intent": "product_id",
    },
    ("stripe", "customers"): {
        "id": "source_id", "name": "name", "email": "email",
        "phone": "phone", "created": "created_at",
    },
    ("stripe", "invoices"): {
        "id": "source_id", "amount_due": "amount", "currency": "currency",
        "customer": "customer_id", "status": "status",
        "created": "period", "due_date": "due_date",
    },
    ("stripe", "subscriptions"): {
        "id": "source_id", "customer": "customer_id",
        "created": "period", "status": "status",
    },
    ("quickbooks", "invoices"): {
        "Id": "source_id", "TotalAmt": "amount", "CurrencyRef.value": "currency",
        "CustomerRef.value": "customer_id", "TxnDate": "period",
        "DueDate": "due_date",
    },
    ("quickbooks", "revenue"): {
        "Id": "source_id", "TotalAmt": "amount", "CurrencyRef.value": "currency",
        "CustomerRef.value": "customer_id", "TxnDate": "period",
    },
    ("quickbooks", "customers"): {
        "Id": "source_id", "DisplayName": "name",
        "PrimaryEmailAddr.Address": "email",
        "PrimaryPhone.FreeFormNumber": "phone",
        "CompanyName": "company", "MetaData.CreateTime": "created_at",
    },
    ("quickbooks", "expenses"): {
        "Id": "source_id", "TotalAmt": "amount", "CurrencyRef.value": "currency",
        "TxnDate": "period", "EntityRef.name": "vendor",
    },
    ("xero", "invoices"): {
        "InvoiceID": "source_id", "Total": "amount",
        "CurrencyCode": "currency", "Contact.ContactID": "customer_id",
        "DateString": "period", "DueDateString": "due_date",
        "Status": "status",
    },
    ("xero", "customers"): {
        "ContactID": "source_id", "Name": "name",
        "EmailAddress": "email", "FirstName": "company",
    },
    ("xero", "revenue"): {
        "PaymentID": "source_id", "Amount": "amount",
        "CurrencyCode": "currency", "Date": "period",
    },
}

# Mappings below this confidence require human confirmation before use.
CONFIDENCE_THRESHOLD = 0.80


# ── Field name heuristics for auto-mapping ────────────────────────────────────

_AMOUNT_HINTS    = {"amount", "total", "net", "gross", "value", "price", "revenue",
                    "netamount", "netamounthc", "txnamount", "unitprice", "subtotal"}
_DATE_HINTS      = {"date", "createdat", "createdate", "txndate", "closedate",
                    "invoicedate", "orderdate", "period", "timestamp", "time"}
_CUSTOMER_HINTS  = {"customer", "customerref", "contact", "client", "account",
                    "customername", "contactname", "companyname", "email"}
_PRODUCT_HINTS   = {"product", "item", "sku", "productname", "itemname",
                    "description", "lineitem"}
_CURRENCY_HINTS  = {"currency", "currencycode", "currencyref", "iso_code"}


def _normalise_key(k: str) -> str:
    return re.sub(r"[^a-z0-9]", "", k.lower())


def _guess_canonical_field(source_field: str, canonical_table: str) -> tuple[str, float]:
    """Return (canonical_field_name, confidence 0-1)."""
    nk = _normalise_key(source_field)
    if canonical_table in ("revenue", "invoices"):
        if any(h in nk for h in _AMOUNT_HINTS):      return "amount", 0.9
        if any(h in nk for h in _DATE_HINTS):         return "period", 0.85
        if any(h in nk for h in _CUSTOMER_HINTS):     return "customer_id", 0.8
        if any(h in nk for h in _CURRENCY_HINTS):     return "currency", 0.88
        if "sub" in nk or "recurring" in nk:          return "subscription_type", 0.7
        if "product" in nk or "item" in nk:           return "product_id", 0.75
    if canonical_table == "customers":
        if "email" in nk:                             return "email", 0.97
        if "name" in nk:                              return "name", 0.88
        if "company" in nk or "account" in nk:        return "company", 0.8
        if "phone" in nk:                             return "phone", 0.85
        if any(h in nk for h in _DATE_HINTS):         return "created_at", 0.75
    if canonical_table == "pipeline":
        if "amount" in nk or "value" in nk:           return "amount", 0.9
        if "stage" in nk:                             return "stage", 0.88
        if "close" in nk:                             return "close_date", 0.85
        if "prob" in nk:                              return "probability", 0.85
        if "name" in nk or "deal" in nk:              return "name", 0.8
    if canonical_table == "employees":
        if "name" in nk:                              return "name", 0.85
        if "salary" in nk or "wage" in nk:            return "salary", 0.88
        if "title" in nk or "role" in nk:             return "title", 0.8
        if "depart" in nk:                            return "department", 0.82
        if "hire" in nk or "start" in nk:             return "hire_date", 0.8
    if canonical_table == "expenses":
        if any(h in nk for h in _AMOUNT_HINTS):       return "amount", 0.88
        if "cat" in nk or "type" in nk:               return "category", 0.78
        if any(h in nk for h in _DATE_HINTS):         return "period", 0.82
        if "vendor" in nk or "supplier" in nk:        return "vendor", 0.8
    return "unmapped", 0.0


# ── Canonical schemas (field → default) ───────────────────────────────────────

_CANONICAL_SCHEMAS = {
    "revenue": {
        "source": None, "source_id": None, "amount": None, "currency": "USD",
        "period": None, "customer_id": None, "subscription_type": None,
        "product_id": None, "recognized_at": None,
    },
    "customers": {
        "source": None, "source_id": None, "name": None, "email": None,
        "company": None, "phone": None, "country": None, "created_at": None,
        "lifecycle_stage": None,
    },
    "pipeline": {
        "source": None, "source_id": None, "name": None, "amount": None,
        "stage": None, "close_date": None, "probability": None,
        "owner": None, "created_at": None,
    },
    "employees": {
        "source": None, "source_id": None, "name": None, "email": None,
        "title": None, "department": None, "salary": None,
        "hire_date": None, "status": "active",
    },
    "expenses": {
        "source": None, "source_id": None, "amount": None, "currency": "USD",
        "category": None, "vendor": None, "period": None, "description": None,
    },
    "invoices": {
        "source": None, "source_id": None, "amount": None, "currency": "USD",
        "customer_id": None, "issue_date": None, "due_date": None,
        "status": None, "period": None,
    },
    "products": {
        "source": None, "source_id": None, "name": None, "sku": None,
        "price": None, "currency": "USD", "category": None, "active": True,
    },
    "marketing": {
        "source": None, "source_id": None, "channel": None, "spend": None,
        "currency": "USD", "period": None, "leads": None, "conversions": None,
    },
    "balance_sheet": {
        "source": None, "source_id": None, "period": None,
        "cash_balance": None, "current_assets": None,
        "current_liabilities": None, "total_assets": None,
        "total_liabilities": None, "currency": "USD",
    },
    "time_tracking": {
        "source": None, "source_id": None, "worker_id": None,
        "period": None, "billable_hours": None, "total_hours": None,
        "time_type": None,
    },
    "product_usage": {
        "source": None, "source_id": None, "user_id": None,
        "period": None, "feature_id": None, "usage_count": None,
        "activated_at": None, "first_value_at": None,
    },
    "surveys": {
        "source": None, "source_id": None, "respondent_id": None,
        "period": None, "nps_score": None, "csat_score": None,
        "survey_type": None,
    },
    "support": {
        "source": None, "source_id": None, "ticket_id": None,
        "period": None, "resolution_hours": None, "effort_score": None,
        "status": None, "customer_id": None,
    },
}


# ── Main transformer ──────────────────────────────────────────────────────────

class Transformer:
    """
    Usage:
        t = Transformer(conn, workspace_id, source_name)
        canonical_rows = t.transform(entity_type, raw_records, confirmed_mappings)
        t.upsert_canonical(entity_type, canonical_rows)
        t.save_mappings(entity_type, raw_records[0] if raw_records else {})
    """

    def __init__(self, conn, workspace_id: str, source_name: str):
        self._conn         = conn
        self._workspace_id = workspace_id
        self._source       = source_name

    # ── Public API ────────────────────────────────────────────────────────────

    def transform(
        self,
        entity_type: str,
        raw_records: list[dict],
        confirmed_mappings: dict[str, str] | None = None,
    ) -> list[dict]:
        """Map raw records into canonical dicts for entity_type."""
        schema = _CANONICAL_SCHEMAS.get(entity_type)
        if schema is None:
            return []

        # Build effective mapping: confirmed > source-specific > auto-detected
        effective = self._build_mapping(entity_type, raw_records, confirmed_mappings)

        canonical = []
        for raw in raw_records:
            row = dict(schema)           # start from defaults
            row["source"]    = self._source
            row["source_id"] = str(raw.get("id", raw.get("Id", "")))
            for src_field, can_field in effective.items():
                if can_field == "unmapped":
                    continue
                val = self._deep_get(raw, src_field)
                if val is not None:
                    row[can_field] = self._coerce(can_field, val)
            canonical.append(row)
        return canonical

    def upsert_canonical(self, entity_type: str, rows: list[dict]) -> int:
        """Insert or update canonical rows. Returns count upserted."""
        if not rows:
            return 0
        table = f"canonical_{entity_type}"
        self._ensure_canonical_table(entity_type)
        self._ensure_change_log_table()
        count = 0
        for row in rows:
            # ── Change detection: log field-level diffs before overwriting ──
            try:
                existing = self._conn.execute(
                    f"SELECT * FROM canonical_{entity_type} WHERE workspace_id=? AND source=? AND source_id=?",
                    [self._workspace_id, row.get("source", self._source), row.get("source_id")]
                ).fetchone()
                if existing:
                    existing_dict = dict(existing) if hasattr(existing, 'keys') else {}
                    for col, new_val in row.items():
                        if col in ("source", "source_id", "workspace_id", "raw_id", "created_at", "updated_at"):
                            continue
                        old_val = existing_dict.get(col)
                        if old_val is not None and new_val is not None and str(old_val) != str(new_val):
                            self._conn.execute(
                                "INSERT INTO canonical_change_log "
                                "(workspace_id, table_name, source, source_id, field_name, old_value, new_value) "
                                "VALUES (?,?,?,?,?,?,?)",
                                [self._workspace_id, f"canonical_{entity_type}",
                                 row.get("source", self._source), row.get("source_id"),
                                 col, str(old_val), str(new_val)]
                            )
            except Exception:
                pass  # Change detection is non-blocking
            cols   = list(row.keys()) + ["workspace_id"]
            # Safety: stringify any dict/list values that SQLite can't bind
            vals   = [
                json.dumps(v) if isinstance(v, (dict, list)) else v
                for v in list(row.values())
            ] + [self._workspace_id]
            ph     = ",".join(["?"] * len(cols))
            update = ",".join(
                f"{c}=excluded.{c}" for c in cols
                if c not in ("source", "source_id", "workspace_id")
            )
            sql = (
                f"INSERT INTO {table} ({','.join(cols)}) VALUES ({ph}) "
                f"ON CONFLICT(workspace_id, source, source_id) DO UPDATE SET {update}"
            )
            self._conn.execute(sql, vals)
            count += 1
        self._conn.commit()
        return count

    def detect_new_fields(self, entity_type: str, sample_record: dict) -> list[str]:
        """Compare incoming fields against sync_field_snapshots.

        Returns list of field names not previously seen for this
        workspace+source+entity.  On the FIRST sync (no prior snapshot),
        returns an empty list — we treat the initial pull as baseline only
        so the user is not flooded with "new field" notifications.

        Side-effect: upserts every incoming field into sync_field_snapshots.
        """
        if not sample_record:
            return []
        self._ensure_snapshot_table()
        incoming_fields = list(sample_record.keys())

        # Fetch known fields for this (workspace, source, entity)
        rows = self._conn.execute(
            "SELECT source_field FROM sync_field_snapshots "
            "WHERE workspace_id=? AND source_name=? AND entity_type=?",
            [self._workspace_id, self._source, entity_type],
        ).fetchall()
        known_fields = {r[0] for r in rows}
        is_first_sync = len(known_fields) == 0

        # Upsert all incoming fields into the snapshot table
        for field in incoming_fields:
            self._conn.execute(
                "INSERT INTO sync_field_snapshots "
                "(workspace_id, source_name, entity_type, source_field) "
                "VALUES (?,?,?,?) "
                "ON CONFLICT(workspace_id, source_name, entity_type, source_field) "
                "DO UPDATE SET last_seen_at=CURRENT_TIMESTAMP",
                [self._workspace_id, self._source, entity_type, field],
            )
        self._conn.commit()

        # First sync = baseline only — no "new" fields
        if is_first_sync:
            return []

        new_fields = [f for f in incoming_fields if f not in known_fields]

        # Detect disappeared fields (material change flagging)
        disappeared = [f for f in known_fields if f not in incoming_fields]
        if disappeared:
            _logger.warning(
                "[Material Change] %s.%s fields disappeared: %s (source=%s, workspace=%s)",
                entity_type, self._source, disappeared, self._source, self._workspace_id,
            )

        return new_fields

    def save_mappings(
        self,
        entity_type: str,
        sample_record: dict,
        confirmed: dict | None = None,
        new_fields: list[str] | None = None,
    ) -> list[dict]:
        """
        Persist auto-detected field mappings (if not already confirmed).
        Returns list of mapping dicts for the UI review step.

        *new_fields*: field names first seen in the current sync — these get
        ``is_new=1`` in the DB so the frontend can highlight them.
        """
        if not sample_record:
            return []
        self._ensure_mappings_table()
        new_set = set(new_fields or [])
        mappings = []
        for src_field in sample_record.keys():
            can_field, confidence = _guess_canonical_field(src_field, entity_type)
            # Check if already confirmed by user
            existing = self._conn.execute(
                "SELECT confirmed_by_user, canonical_field FROM field_mappings "
                "WHERE workspace_id=? AND source_name=? AND source_field=? AND canonical_table=?",
                [self._workspace_id, self._source, src_field, entity_type],
            ).fetchone()
            if existing and existing[0]:
                mappings.append({
                    "source_field":     src_field,
                    "canonical_table":  entity_type,
                    "canonical_field":  existing[1],
                    "confidence":       1.0,
                    "confirmed_by_user": True,
                    "is_new":           False,
                })
                continue
            # Check user-passed confirmed dict
            override = (confirmed or {}).get(src_field)
            if override:
                can_field, confidence = override, 1.0
            is_new_flag = 1 if src_field in new_set else 0
            self._conn.execute(
                "INSERT INTO field_mappings "
                "(workspace_id,source_name,source_field,canonical_table,canonical_field,"
                "confidence,confirmed_by_user,is_new) VALUES (?,?,?,?,?,?,?,?) "
                "ON CONFLICT(workspace_id,source_name,source_field,canonical_table) "
                "DO UPDATE SET canonical_field=excluded.canonical_field, "
                "confidence=excluded.confidence, "
                "confirmed_by_user=excluded.confirmed_by_user, "
                "is_new=excluded.is_new",
                [self._workspace_id, self._source, src_field, entity_type,
                 can_field, confidence, 1 if override else 0, is_new_flag],
            )
            mappings.append({
                "source_field":     src_field,
                "canonical_table":  entity_type,
                "canonical_field":  can_field,
                "confidence":       confidence,
                "confirmed_by_user": bool(override),
                "is_new":           bool(is_new_flag),
            })
        self._conn.commit()
        return mappings

    # ── Private helpers ───────────────────────────────────────────────────────

    def _build_mapping(
        self,
        entity_type: str,
        raw_records: list[dict],
        confirmed: dict | None,
    ) -> dict[str, str]:
        """Build {source_field: canonical_field} from DB + explicit + heuristics.

        Priority order:
          1. User-confirmed mappings (from DB)
          2. Explicit per-connector mappings (EXPLICIT_FIELD_MAPS)
          3. Auto-detected heuristic mappings (keyword matching)
        """
        mapping = {}
        if not raw_records:
            return mapping
        sample = raw_records[0]

        # 1. Load confirmed mappings from DB first (highest priority)
        try:
            rows = self._conn.execute(
                "SELECT source_field, canonical_field FROM field_mappings "
                "WHERE workspace_id=? AND source_name=? AND canonical_table=? "
                "AND confirmed_by_user=1",
                [self._workspace_id, self._source, entity_type],
            ).fetchall()
            mapping = {r[0]: r[1] for r in rows}
        except Exception:
            pass

        # 2. Check explicit per-connector mapping (if defined, use ONLY these)
        explicit_key = (self._source, entity_type)
        explicit = EXPLICIT_FIELD_MAPS.get(explicit_key)
        if explicit:
            for src_field, can_field in explicit.items():
                if src_field not in mapping:  # don't override user-confirmed
                    mapping[src_field] = can_field
            return mapping  # Skip heuristics entirely when explicit map exists

        # 3. Fill gaps with heuristics (only if confidence meets threshold)
        for k in sample.keys():
            if k not in mapping:
                cf, confidence = _guess_canonical_field(k, entity_type)
                if confidence >= CONFIDENCE_THRESHOLD:
                    mapping[k] = cf
                else:
                    mapping[k] = "unmapped"
                    _logger.info("[Field Mapping] Low confidence %.2f for %s.%s -> %s (needs review)",
                                 confidence, entity_type, k, cf)

        # User-supplied overrides win
        if confirmed:
            mapping.update(confirmed)
        return mapping

    def _deep_get(self, record: dict, field: str) -> Any:
        """Get value from nested dict using dot-notation or flat key."""
        if "." in field:
            parts = field.split(".", 1)
            sub   = record.get(parts[0])
            if isinstance(sub, dict):
                return self._deep_get(sub, parts[1])
            return None
        # Try exact, then case-insensitive
        if field in record:
            val = record[field]
            return val.get("value") if isinstance(val, dict) and "value" in val else val
        lower_map = {k.lower(): k for k in record}
        key = lower_map.get(field.lower())
        if key:
            val = record[key]
            return val.get("value") if isinstance(val, dict) and "value" in val else val
        return None

    def _coerce(self, canonical_field: str, value: Any) -> Any:
        """Light coercion — amounts to float, dates to ISO string."""
        if value is None:
            return None
        if canonical_field in ("amount", "salary", "price", "spend"):
            try:
                val = float(str(value).replace(",", "").replace("$", ""))
                # Apply connector-specific unit conversion (e.g., Stripe cents → dollars)
                divisor = CONNECTOR_UNIT_CONFIG.get(self._source, {}).get("amount_divisor", 1)
                return val / divisor
            except (ValueError, TypeError):
                return None
        if canonical_field in ("period", "recognized_at", "created_at",
                               "close_date", "hire_date", "issue_date", "due_date"):
            if isinstance(value, (int, float)):
                try:
                    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
                except Exception:
                    return str(value)
            return str(value)
        if canonical_field == "probability":
            try:
                f = float(value)
                return f / 100 if f > 1 else f
            except (ValueError, TypeError):
                return None
        return value

    def _ensure_canonical_table(self, entity_type: str) -> None:
        from core.config import _USE_PG
        schema = _CANONICAL_SCHEMAS.get(entity_type, {})
        _real_typed = ("amount", "salary", "price", "spend", "probability",
                       "leads", "conversions", "cash_balance", "current_assets",
                       "current_liabilities", "total_assets", "total_liabilities",
                       "billable_hours", "total_hours", "usage_count",
                       "nps_score", "csat_score", "resolution_hours", "effort_score")
        cols_sql = ", ".join(
            f"{col} REAL" if col in _real_typed else f"{col} TEXT"
            for col in schema.keys()
        )
        if _USE_PG:
            # PostgreSQL: SERIAL, NOW()
            self._conn.execute(f"""
                CREATE TABLE IF NOT EXISTS canonical_{entity_type} (
                    id          SERIAL PRIMARY KEY,
                    workspace_id TEXT NOT NULL,
                    {cols_sql},
                    raw_id      TEXT,
                    created_at  TEXT DEFAULT NOW(),
                    updated_at  TEXT DEFAULT NOW()
                )
            """)
        else:
            # SQLite: AUTOINCREMENT, CURRENT_TIMESTAMP
            self._conn.execute(f"""
                CREATE TABLE IF NOT EXISTS canonical_{entity_type} (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    workspace_id TEXT NOT NULL,
                    {cols_sql},
                    raw_id      TEXT,
                    created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(workspace_id, source, source_id)
                )
            """)
        # Ensure the UNIQUE constraint exists — safe on both new and existing tables.
        # CREATE TABLE IF NOT EXISTS skips creation for existing tables, so the
        # UNIQUE constraint inside it never gets applied. This index guarantees it.
        try:
            self._conn.execute(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS uix_canonical_{entity_type}_upsert
                ON canonical_{entity_type} (workspace_id, source, source_id)
            """)
        except Exception:
            pass  # Index may already exist or constraint may be inline
        self._conn.commit()

        # ── Schema migration: detect and add missing columns ──────────────
        _real_cols = ("amount", "salary", "price", "spend", "probability",
                      "leads", "conversions", "cash_balance", "current_assets",
                      "current_liabilities", "total_assets", "total_liabilities",
                      "billable_hours", "total_hours", "usage_count",
                      "nps_score", "csat_score", "resolution_hours", "effort_score")
        try:
            existing_cols = set()
            try:
                # SQLite
                for row in self._conn.execute(
                    f"PRAGMA table_info(canonical_{entity_type})"
                ).fetchall():
                    existing_cols.add(row[1])
            except Exception:
                # PostgreSQL fallback — use f-string (table name is safe, from code constants)
                for row in self._conn.execute(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_name = 'canonical_{entity_type}'"
                ).fetchall():
                    existing_cols.add(row[0])

            migrated = 0
            for col in schema.keys():
                if col not in existing_cols:
                    col_type = "REAL" if col in _real_cols else "TEXT"
                    self._conn.execute(
                        f"ALTER TABLE canonical_{entity_type} ADD COLUMN {col} {col_type}"
                    )
                    migrated += 1
                    _logger.info("[Schema Migration] Added column %s (%s) to canonical_%s",
                                 col, col_type, entity_type)
            if migrated:
                self._conn.commit()
        except Exception as exc:
            _logger.warning("[Schema Migration] Could not migrate canonical_%s: %s",
                            entity_type, exc)

    def _ensure_change_log_table(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS canonical_change_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                source TEXT,
                source_id TEXT NOT NULL,
                field_name TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                changed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                sync_id TEXT
            )
        """)
        self._conn.commit()

    def _ensure_mappings_table(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS field_mappings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_id    TEXT NOT NULL,
                source_name     TEXT NOT NULL,
                source_field    TEXT NOT NULL,
                canonical_table TEXT NOT NULL,
                canonical_field TEXT NOT NULL,
                confidence      REAL DEFAULT 0,
                confirmed_by_user INTEGER DEFAULT 0,
                is_new          INTEGER DEFAULT 0,
                created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(workspace_id, source_name, source_field, canonical_table)
            )
        """)
        # Schema migration: add is_new column to existing tables
        try:
            self._conn.execute("ALTER TABLE field_mappings ADD COLUMN is_new INTEGER DEFAULT 0")
        except Exception:
            pass  # Column already exists
        self._conn.commit()

    def _ensure_snapshot_table(self) -> None:
        from core.config import _USE_PG
        if _USE_PG:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_field_snapshots (
                    id              SERIAL PRIMARY KEY,
                    workspace_id    TEXT NOT NULL,
                    source_name     TEXT NOT NULL,
                    entity_type     TEXT NOT NULL,
                    source_field    TEXT NOT NULL,
                    first_seen_at   TEXT DEFAULT NOW(),
                    last_seen_at    TEXT DEFAULT NOW(),
                    UNIQUE(workspace_id, source_name, entity_type, source_field)
                )
            """)
        else:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_field_snapshots (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    workspace_id    TEXT NOT NULL,
                    source_name     TEXT NOT NULL,
                    entity_type     TEXT NOT NULL,
                    source_field    TEXT NOT NULL,
                    first_seen_at   TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_seen_at    TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(workspace_id, source_name, entity_type, source_field)
                )
            """)
        self._conn.commit()
