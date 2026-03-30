"""
Gap Detector — identifies which canonical fields are missing
and which KPIs they block.

Reads canonical_* tables for a workspace and returns a structured
list of gaps with remediation suggestions.
"""
from __future__ import annotations

from dataclasses import dataclass, field


# ── KPI dependency map ────────────────────────────────────────────────────────
# Each KPI lists the (canonical_table, canonical_field) pairs it requires.

KPI_DEPENDENCIES: dict[str, list[tuple[str, str]]] = {
    "MRR": [
        ("canonical_revenue", "amount"),
        ("canonical_revenue", "period"),
        ("canonical_revenue", "subscription_type"),
        ("canonical_customers", "customer_id"),
    ],
    "ARR": [
        ("canonical_revenue", "amount"),
        ("canonical_revenue", "period"),
        ("canonical_revenue", "subscription_type"),
    ],
    "Gross Revenue": [
        ("canonical_revenue", "amount"),
        ("canonical_revenue", "period"),
    ],
    "Net Revenue Retention": [
        ("canonical_revenue", "amount"),
        ("canonical_revenue", "period"),
        ("canonical_revenue", "customer_id"),
        ("canonical_revenue", "subscription_type"),
    ],
    "Customer Churn": [
        ("canonical_customers", "source_id"),
        ("canonical_customers", "created_at"),
        ("canonical_revenue", "customer_id"),
        ("canonical_revenue", "period"),
    ],
    "CAC": [
        ("canonical_expenses", "amount"),
        ("canonical_expenses", "category"),
        ("canonical_customers", "created_at"),
        ("canonical_pipeline", "amount"),
    ],
    "LTV": [
        ("canonical_revenue", "amount"),
        ("canonical_revenue", "customer_id"),
        ("canonical_customers", "created_at"),
    ],
    "Gross Margin": [
        ("canonical_revenue", "amount"),
        ("canonical_expenses", "amount"),
        ("canonical_expenses", "category"),
    ],
    "Burn Rate": [
        ("canonical_expenses", "amount"),
        ("canonical_expenses", "period"),
    ],
    "Headcount": [
        ("canonical_employees", "source_id"),
        ("canonical_employees", "status"),
    ],
    "Revenue per Employee": [
        ("canonical_revenue", "amount"),
        ("canonical_revenue", "period"),
        ("canonical_employees", "source_id"),
    ],
    "Pipeline Coverage": [
        ("canonical_pipeline", "amount"),
        ("canonical_pipeline", "stage"),
        ("canonical_revenue", "amount"),
    ],
    "Win Rate": [
        ("canonical_pipeline", "stage"),
        ("canonical_pipeline", "amount"),
        ("canonical_pipeline", "close_date"),
    ],
    "Sales Cycle Length": [
        ("canonical_pipeline", "created_at"),
        ("canonical_pipeline", "close_date"),
        ("canonical_pipeline", "stage"),
    ],
    "DSO": [
        ("canonical_invoices", "issue_date"),
        ("canonical_invoices", "due_date"),
        ("canonical_invoices", "amount"),
        ("canonical_invoices", "status"),
    ],
}

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
        """Return (total_rows, rows_where_column_is_null_or_empty)."""
        try:
            # Check table exists
            exists = self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", [table]
            ).fetchone()
            if not exists:
                return 0, 0
            # Check column exists
            cols = [
                row[1] for row in
                self._conn.execute(f"PRAGMA table_info({table})").fetchall()
            ]
            if column not in cols:
                return 0, 0
            total = self._conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE workspace_id=?",
                [self._workspace_id],
            ).fetchone()[0]
            missing = self._conn.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE workspace_id=? AND ({column} IS NULL OR {column}='')",
                [self._workspace_id],
            ).fetchone()[0]
            return total, missing
        except Exception:
            return 0, 0
