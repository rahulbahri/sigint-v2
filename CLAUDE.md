# Axiom Intelligence — Engineering Standards

## Mandatory Pre-Code Protocol

BEFORE writing or editing ANY code, complete this checklist. No exceptions.

### 1. Trace the Data Flow
Read the ACTUAL code (not from memory) at every step:
- What function/endpoint PRODUCES the data? What exact field names does it return?
- What function/component CONSUMES the data? What field names does it expect?
- Do the field names match EXACTLY? (e.g., `target` vs `target_value`, `yellow` vs `amber`)

### 2. Dual Database Compatibility
Every SQL statement must work on BOTH:
- **SQLite** (local dev at `backend/uploads/axiom.db`)
- **PostgreSQL** (production on Render via `DATABASE_URL`)

Known gotchas: `AUTOINCREMENT` (SQLite) vs `SERIAL` (PG), `PRAGMA` (SQLite-only), `?` vs `%s` params, `CURRENT_TIMESTAMP` vs `NOW()`, `CREATE TABLE IF NOT EXISTS` doesn't add constraints to existing tables.

### 3. Handle All Status Values
Backend sends: `red`, `yellow`, `green`, `grey`
Frontend must map ALL of them. Common trap: frontend uses `amber` but backend sends `yellow`.

### 4. Verify Units
Every KPI has a unit (`pct`, `usd`, `ratio`, `days`, `months`, `score`, `count`). The unit must flow from `kpi_defs.py` through every API response to `fmtKpiValue()` in the frontend. If unit is empty, the value displays as a raw number.

### 5. Test Before Push
- `python3 -m pytest tests/ -v` from `backend/` — 225+ must pass
- For data flow changes: test with the user's actual workbook locally
- `npx vite build` from `frontend/` — must build clean
- Rebuild `frontend/dist/` before committing frontend changes

### 6. Self-Validation, Self-Review, Self-Healing (Build Quality Gate)

Every code change MUST be built for end-to-end correctness. No partial implementations that require "fixing later."

**Self-Validation:**
- Every new function gets a corresponding test in `backend/tests/`
- Every new API endpoint gets a request/response test
- Every new SQL table/column gets schema validation in tests
- Every data-flow change gets an end-to-end test: input → transform → output → verify
- Run `python3 -m pytest tests/ -v` after every change — all tests must pass, no exceptions
- Frontend: `npx vite build` must succeed after every component change

**Self-Review:**
- After writing code, trace the FULL data flow from entry point to final consumer
- Verify field names match at EVERY boundary (backend→API→frontend, DB→Python→JSON)
- Check all error paths: what happens if the input is null, empty, malformed, or missing?
- Verify dual-DB compatibility: run `_schema_translate()` mentally on every new DDL
- Check that new code integrates with existing validation (integrity.py, gap_detector, data quality scanner)

**Self-Healing:**
- New features that produce data MUST integrate with `DataIntegrityValidator` — add verification rules for new data flows
- When validation detects bad data from a new feature, it should auto-correct or surface actionable guidance
- Mapping changes → trigger integrity check → auto-correct if discrepancy found
- Every new data table gets a corresponding quality check in the data quality scanner
- Build defensive: wrap data transforms in try/except with structured error logging, never let one record kill a batch

**End-to-End Guarantee:**
- Before considering a feature "done," test the FULL flow with a workbook upload:
  CSV/XLSX upload → canonical population → KPI aggregation → integrity check → frontend display
- For connector features: connector extract → transform → canonical → KPI → integrity → display
- No "it should work" — actually verify it works by running the flow

## Architecture Quick Reference

- Backend: FastAPI + SQLite (dev) / PostgreSQL (prod)
- Frontend: React 18 + JSX + Vite + Tailwind
- KPI computation: TWO paths — simple CSV (`kpi_defs.py:701`) produces 14 KPIs, full canonical (`kpi_aggregator.py:300`) produces 61 KPIs
- `_schema_translate()` in `database.py` converts SQLite DDL to PostgreSQL — use it for any new table creation
- `CONNECTOR_UPLOAD_SENTINEL = -999` identifies aggregator rows in `monthly_data`
- CSV-uploaded data (real upload_id) takes priority over aggregator data (sentinel) in merge logic
- `python` command not found — always use `python3`
- Frontend builds with `export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH" && npx vite build`
- `KPI_FIELD_DEPS` in `integration_spec.py` is the SINGLE SOURCE OF TRUTH for KPI→canonical field dependencies (60+ KPIs). Use it, don't duplicate.
- `_kpis_for_field()` in `integration_spec.py` is the reverse-lookup utility — reuse it.
- `DataIntegrityValidator` in `integrity.py` is the 5-stage validation pipeline — integrate new data flows with it.
