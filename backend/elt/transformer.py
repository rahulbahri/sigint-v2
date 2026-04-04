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
        count = 0
        for row in rows:
            cols   = list(row.keys()) + ["workspace_id"]
            vals   = list(row.values()) + [self._workspace_id]
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

    def save_mappings(
        self, entity_type: str, sample_record: dict, confirmed: dict | None = None
    ) -> list[dict]:
        """
        Persist auto-detected field mappings (if not already confirmed).
        Returns list of mapping dicts for the UI review step.
        """
        if not sample_record:
            return []
        self._ensure_mappings_table()
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
                })
                continue
            # Check user-passed confirmed dict
            override = (confirmed or {}).get(src_field)
            if override:
                can_field, confidence = override, 1.0
            self._conn.execute(
                "INSERT INTO field_mappings "
                "(workspace_id,source_name,source_field,canonical_table,canonical_field,"
                "confidence,confirmed_by_user) VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(workspace_id,source_name,source_field,canonical_table) "
                "DO UPDATE SET canonical_field=excluded.canonical_field, "
                "confidence=excluded.confidence, "
                "confirmed_by_user=excluded.confirmed_by_user",
                [self._workspace_id, self._source, src_field, entity_type,
                 can_field, confidence, 1 if override else 0],
            )
            mappings.append({
                "source_field":     src_field,
                "canonical_table":  entity_type,
                "canonical_field":  can_field,
                "confidence":       confidence,
                "confirmed_by_user": bool(override),
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
        """Build {source_field: canonical_field} from DB + heuristics."""
        mapping = {}
        if not raw_records:
            return mapping
        sample = raw_records[0]

        # Load confirmed mappings from DB first
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

        # Fill gaps with heuristics
        for k in sample.keys():
            if k not in mapping:
                cf, _ = _guess_canonical_field(k, entity_type)
                mapping[k] = cf

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
                return float(str(value).replace(",", "").replace("$", ""))
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
        schema = _CANONICAL_SCHEMAS.get(entity_type, {})
        cols_sql = ", ".join(
            f"{col} TEXT" if col not in ("amount", "salary", "price", "spend",
                                          "probability", "leads", "conversions")
            else f"{col} REAL"
            for col in schema.keys()
        )
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
                # PostgreSQL fallback
                for row in self._conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s", [f"canonical_{entity_type}"]
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
                created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(workspace_id, source_name, source_field, canonical_table)
            )
        """)
        self._conn.commit()
