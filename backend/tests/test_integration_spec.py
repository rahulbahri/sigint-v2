"""Tests for the Integration Specification workbook generator."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from core.integration_spec import (
    SYSTEM_REGISTRY, CANONICAL_SCHEMAS, KPI_FIELD_DEPS,
    ELT_PIPELINE_RULES, generate_integration_spec_workbook,
)
from core.kpi_defs import KPI_DEFS, EXTENDED_ONTOLOGY_METRICS


# ── Data integrity tests ──────────────────────────────────────────────────

def test_system_count():
    """15 source systems defined."""
    assert len(SYSTEM_REGISTRY) == 15


def test_canonical_table_count():
    """12 canonical tables defined."""
    assert len(CANONICAL_SCHEMAS) == 12


def test_all_systems_have_fields():
    """Every system has at least one field definition."""
    for sys in SYSTEM_REGISTRY:
        assert len(sys["fields"]) > 0, f"{sys['name']} has no fields"


def test_field_canonical_table_validity():
    """Every canonical_table value references a valid canonical table."""
    valid_tables = set(CANONICAL_SCHEMAS.keys()) | {""}
    for sys in SYSTEM_REGISTRY:
        for f in sys["fields"]:
            assert f["canonical_table"] in valid_tables, (
                f"{sys['name']}.{f['source_field']}: "
                f"invalid canonical_table '{f['canonical_table']}'"
            )


def test_field_canonical_field_validity():
    """Every canonical_field references a valid field in its canonical table."""
    for sys in SYSTEM_REGISTRY:
        for f in sys["fields"]:
            ct = f["canonical_table"]
            cf = f["canonical_field"]
            if ct and cf:
                valid_fields = {name for name, _, _ in CANONICAL_SCHEMAS[ct]["fields"]}
                assert cf in valid_fields, (
                    f"{sys['name']}.{f['source_field']}: "
                    f"'{cf}' not in {ct}"
                )


def test_kpis_driven_validity():
    """Every KPI key in 'kpis_driven' exists in KPI_DEFS or EXTENDED_ONTOLOGY_METRICS."""
    all_kpi_keys = {d["key"] for d in KPI_DEFS} | {d["key"] for d in EXTENDED_ONTOLOGY_METRICS}
    # Also allow aggregator-computed intermediate KPIs
    all_kpi_keys |= {"mrr", "cash_burn"}
    for sys in SYSTEM_REGISTRY:
        for f in sys["fields"]:
            if f["kpis_driven"]:
                for kpi in f["kpis_driven"].split(", "):
                    kpi = kpi.strip()
                    if kpi:
                        assert kpi in all_kpi_keys, (
                            f"{sys['name']}.{f['source_field']}: "
                            f"unknown KPI '{kpi}'"
                        )


def test_required_fields_have_descriptions():
    """Required fields must have non-empty description, data_type, and canonical mapping info."""
    for sys in SYSTEM_REGISTRY:
        for f in sys["fields"]:
            if f["required"]:
                assert f["description"], f"{sys['name']}.{f['source_field']}: missing description"
                assert f["data_type"], f"{sys['name']}.{f['source_field']}: missing data_type"


def test_elt_pipeline_rules_reference_valid_tables():
    """Every ELT rule references a valid canonical table and field."""
    valid_tables = set(CANONICAL_SCHEMAS.keys())
    for rule in ELT_PIPELINE_RULES:
        assert rule["canonical_table"] in valid_tables, (
            f"ELT rule references unknown table '{rule['canonical_table']}'"
        )
        valid_fields = {name for name, _, _ in CANONICAL_SCHEMAS[rule["canonical_table"]]["fields"]}
        assert rule["canonical_field"] in valid_fields, (
            f"ELT rule references unknown field '{rule['canonical_field']}' "
            f"in {rule['canonical_table']}"
        )


def test_system_keys_unique():
    """System keys are unique."""
    keys = [s["key"] for s in SYSTEM_REGISTRY]
    assert len(keys) == len(set(keys))


def test_connector_status_flags():
    """Known connectors flagged correctly; planned systems flagged as not having connectors."""
    planned = {"peachtree", "workday", "adp"}
    for sys in SYSTEM_REGISTRY:
        if sys["key"] in planned:
            assert not sys["has_connector"], f"{sys['name']} should be planned"
        else:
            assert sys["has_connector"], f"{sys['name']} should have connector"


# ── Workbook generation tests ─────────────────────────────────────────────

def test_workbook_generates():
    """Workbook generates without error."""
    wb = generate_integration_spec_workbook()
    assert wb is not None


def test_workbook_sheet_count():
    """Workbook has 19 sheets: Overview + Canonical + KPI Ref + 15 systems + ELT."""
    wb = generate_integration_spec_workbook()
    assert len(wb.sheetnames) == 19


def test_workbook_has_all_system_tabs():
    """Every system in SYSTEM_REGISTRY has a corresponding sheet."""
    wb = generate_integration_spec_workbook()
    sheet_names = set(wb.sheetnames)
    for sys in SYSTEM_REGISTRY:
        assert sys["name"] in sheet_names, f"Missing sheet for {sys['name']}"


def test_workbook_system_tab_headers():
    """Each system tab has exactly 10 header columns."""
    wb = generate_integration_spec_workbook()
    expected_headers = [
        "Source Object", "Source Field", "Description", "Data Type",
        "Required", "Frequency", "Unit of Ingest",
        "Canonical Table", "Canonical Field", "KPIs Driven",
    ]
    for sys in SYSTEM_REGISTRY:
        ws = wb[sys["name"]]
        headers = [cell.value for cell in ws[1]]
        assert headers == expected_headers, f"{sys['name']} headers mismatch: {headers}"


def test_workbook_overview_tab():
    """Overview tab has correct system and KPI counts."""
    wb = generate_integration_spec_workbook()
    ws = wb["Overview"]
    # Find the total systems cell
    found_systems = False
    found_kpis = False
    for row in ws.iter_rows(max_row=10, max_col=2, values_only=True):
        if row[0] == "Total Source Systems":
            assert row[1] == 15
            found_systems = True
        if row[0] == "Total KPIs Tracked":
            assert row[1] >= 60  # 62 expected
            found_kpis = True
    assert found_systems, "Missing 'Total Source Systems' row"
    assert found_kpis, "Missing 'Total KPIs Tracked' row"


def test_workbook_kpi_reference_tab():
    """KPI Reference tab lists all KPIs."""
    wb = generate_integration_spec_workbook()
    ws = wb["KPI Reference"]
    all_kpi_keys = {d["key"] for d in KPI_DEFS} | {d["key"] for d in EXTENDED_ONTOLOGY_METRICS}
    kpi_keys_in_sheet = set()
    for row in ws.iter_rows(min_row=2, max_col=1, values_only=True):
        if row[0]:
            kpi_keys_in_sheet.add(row[0])
    missing = all_kpi_keys - kpi_keys_in_sheet
    assert not missing, f"KPI Reference tab missing: {missing}"


def test_workbook_canonical_schema_tab():
    """Canonical Schema tab has all 6 tables."""
    wb = generate_integration_spec_workbook()
    ws = wb["Canonical Schema"]
    tables_in_sheet = set()
    for row in ws.iter_rows(min_row=2, max_col=1, values_only=True):
        if row[0]:
            tables_in_sheet.add(row[0])
    for tname in CANONICAL_SCHEMAS:
        assert tname in tables_in_sheet, f"Missing canonical table {tname}"


def test_workbook_elt_pipeline_tab():
    """ELT Pipeline tab has rows for all rules."""
    wb = generate_integration_spec_workbook()
    ws = wb["ELT Pipeline"]
    row_count = ws.max_row - 1  # minus header
    assert row_count == len(ELT_PIPELINE_RULES)
