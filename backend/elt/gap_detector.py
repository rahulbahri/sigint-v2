"""
Gap Detector — identifies which canonical fields are missing
and which KPIs they block.

Reads canonical_* tables for a workspace and returns a structured
list of gaps with remediation suggestions.

Uses ``KPI_FIELD_DEPS`` from ``core.integration_spec`` as the
**single source of truth** for KPI→field dependencies (60+ KPIs).
"""
from __future__ import annotations

from dataclasses import dataclass, field

from core.integration_spec import KPI_FIELD_DEPS


# ── KPI dependency map (derived from integration_spec's KPI_FIELD_DEPS) ──────
# Convert "canonical_revenue.amount" string format to (table, field) tuples.

def _build_kpi_dependencies() -> dict[str, list[tuple[str, str]]]:
    """Convert KPI_FIELD_DEPS from integration_spec.py into
    {kpi: [(canonical_table, canonical_field), ...]} format."""
    result: dict[str, list[tuple[str, str]]] = {}
    for kpi, dep_strings in KPI_FIELD_DEPS.items():
        deps = []
        for dep in dep_strings:
            parts = dep.split(".", 1)
            if len(parts) == 2:
                deps.append((parts[0], parts[1]))
        result[kpi] = deps
    return result


KPI_DEPENDENCIES = _build_kpi_dependencies()


def get_kpi_impact_for_field(canonical_table: str, canonical_field: str) -> list[str]:
    """Return list of KPI names that depend on (canonical_table, canonical_field).

    Accepts table name with or without the ``canonical_`` prefix.
    """
    full_table = canonical_table if canonical_table.startswith("canonical_") else f"canonical_{canonical_table}"
    return [
        kpi for kpi, deps in KPI_DEPENDENCIES.items()
        if (full_table, canonical_field) in deps
    ]

# Suggested remediation per (table, field)
_REMEDIATION: dict[tuple[str, str], str] = {
    ("canonical_revenue", "subscription_type"):
        "Connect Stripe — or tag recurring invoices in QuickBooks as 'subscription'",
    ("canonical_revenue", "customer_id"):
        "Ensure customers are linked to invoices in your accounting system",
    ("canonical_expenses", "category"):
        "Categorise expenses in QuickBooks/Xero (mark COGS separately from OpEx)",
    ("canonical_employees", "source_id"):
        "Connect a payroll source: ADP, Gusto, or BambooHR",
    ("canonical_employees", "status"):
        "Connect a payroll source or upload headcount CSV",
    ("canonical_pipeline", "amount"):
        "Connect HubSpot or Salesforce CRM",
    ("canonical_pipeline", "stage"):
        "Connect HubSpot or Salesforce CRM",
    ("canonical_pipeline", "close_date"):
        "Connect HubSpot or Salesforce and ensure close dates are set on deals",
    ("canonical_invoices", "issue_date"):
        "Connect QuickBooks, Xero, or NetSuite to pull invoice data",
    ("canonical_invoices", "status"):
        "Connect accounting software and ensure invoice statuses are up to date",
}

_DEFAULT_REMEDIATION = "Connect or upload data for this source"


@dataclass
class DataGap:
    canonical_table:  str
    canonical_field:  str
    blocking_kpis:    list[str]
    suggested_fix:    str
    severity:         str   # "critical" | "warning"
    record_count:     int   # how many records are missing this field
    total_records:    int


@dataclass
class GapReport:
    workspace_id:   str
    gaps:           list[DataGap] = field(default_factory=list)
    kpi_status:     dict[str, str] = field(default_factory=dict)  # KPI → "ready"|"partial"|"blocked"
    ready_count:    int = 0
    partial_count:  int = 0
    blocked_count:  int = 0

    def to_dict(self) -> dict:
        return {
            "workspace_id":  self.workspace_id,
            "ready_count":   self.ready_count,
            "partial_count": self.partial_count,
            "blocked_count": self.blocked_count,
            "kpi_status":    self.kpi_status,
            "gaps": [
                {
                    "canonical_table":  g.canonical_table,
                    "canonical_field":  g.canonical_field,
                    "blocking_kpis":    g.blocking_kpis,
                    "suggested_fix":    g.suggested_fix,
                    "severity":         g.severity,
                    "missing_records":  g.record_count,
                    "total_records":    g.total_records,
                    "pct_missing": (
                        round(g.record_count / g.total_records * 100, 1)
                        if g.total_records else 100
                    ),
                }
                for g in self.gaps
            ],
        }


class GapDetector:
    """
    Usage:
        gd = GapDetector(conn, workspace_id)
        report = gd.run()
        report.to_dict()  # → JSON-serialisable
    """

    def __init__(self, conn, workspace_id: str):
        self._conn         = conn
        self._workspace_id = workspace_id

    def run(self) -> GapReport:
        report = GapReport(workspace_id=self._workspace_id)

        # Collect all required (table, field) pairs and which KPIs need them
        field_to_kpis: dict[tuple[str, str], list[str]] = {}
        for kpi, deps in KPI_DEPENDENCIES.items():
            for dep in deps:
                field_to_kpis.setdefault(dep, []).append(kpi)

        # Check each (table, field) against actual data
        gap_map: dict[tuple[str, str], DataGap] = {}
        for (table, col_field), kpis in field_to_kpis.items():
            total, missing = self._count_missing(table, col_field)
            if missing == 0 and total == 0:
                # Table doesn't exist or is empty — treat as fully missing
                gap_map[(table, col_field)] = DataGap(
                    canonical_table=table,
                    canonical_field=col_field,
                    blocking_kpis=kpis,
                    suggested_fix=_REMEDIATION.get((table, col_field), _DEFAULT_REMEDIATION),
                    severity="critical",
                    record_count=0,
                    total_records=0,
                )
            elif missing > 0:
                pct = missing / total if total else 1.0
                gap_map[(table, col_field)] = DataGap(
                    canonical_table=table,
                    canonical_field=col_field,
                    blocking_kpis=kpis,
                    suggested_fix=_REMEDIATION.get((table, col_field), _DEFAULT_REMEDIATION),
                    severity="critical" if pct > 0.5 else "warning",
                    record_count=missing,
                    total_records=total,
                )

        report.gaps = list(gap_map.values())

        # Build KPI status
        for kpi, deps in KPI_DEPENDENCIES.items():
            blocked_deps  = [d for d in deps if d in gap_map and gap_map[d].total_records == 0]
            partial_deps  = [d for d in deps if d in gap_map and gap_map[d].total_records > 0]
            if blocked_deps:
                report.kpi_status[kpi] = "blocked"
                report.blocked_count += 1
            elif partial_deps:
                report.kpi_status[kpi] = "partial"
                report.partial_count += 1
            else:
                report.kpi_status[kpi] = "ready"
                report.ready_count += 1

        return report

    def _count_missing(self, table: str, column: str) -> tuple[int, int]:
        """Return (total_rows, rows_where_column_is_null_or_empty).

        Dual-DB compatible: tries SQLite-style first, falls back to
        PostgreSQL information_schema for table/column existence checks.
        """
        try:
            # Check table exists (dual-DB: try SQLite first, then PG)
            try:
                exists = self._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?", [table]
                ).fetchone()
            except Exception:
                # PostgreSQL fallback
                exists = self._conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_name=%s AND table_schema='public'", [table]
                ).fetchone()
            if not exists:
                return 0, 0

            # Check column exists (dual-DB)
            try:
                cols = [row[1] for row in
                        self._conn.execute(f"PRAGMA table_info({table})").fetchall()]
            except Exception:
                # PostgreSQL fallback
                cols = [row[0] for row in self._conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name=%s", [table]
                ).fetchall()]
            if column not in cols:
                return 0, 0

            t_row = self._conn.execute(
                f"SELECT COUNT(*) as cnt FROM {table} WHERE workspace_id=?",
                [self._workspace_id],
            ).fetchone()
            total = t_row["cnt"] if t_row else 0
            m_row = self._conn.execute(
                f"SELECT COUNT(*) as cnt FROM {table} "
                f"WHERE workspace_id=? AND ({column} IS NULL OR {column}='')",
                [self._workspace_id],
            ).fetchone()
            missing = m_row["cnt"] if m_row else 0
            return total, missing
        except Exception:
            return 0, 0
