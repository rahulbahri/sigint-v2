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

## Architecture Quick Reference

- Backend: FastAPI + SQLite (dev) / PostgreSQL (prod)
- Frontend: React 18 + JSX + Vite + Tailwind
- KPI computation: TWO paths — simple CSV (`kpi_defs.py:701`) produces 14 KPIs, full canonical (`kpi_aggregator.py:300`) produces 61 KPIs
- `_schema_translate()` in `database.py` converts SQLite DDL to PostgreSQL — use it for any new table creation
- `CONNECTOR_UPLOAD_SENTINEL = -999` identifies aggregator rows in `monthly_data`
- CSV-uploaded data (real upload_id) takes priority over aggregator data (sentinel) in merge logic
- `python` command not found — always use `python3`
- Frontend builds with `export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH" && npx vite build`
