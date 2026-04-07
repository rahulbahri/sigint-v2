# Financial Ingestion & Excel Model — Master Roadmap

## Phase 1: Historical Depth + Smart Model Window — COMPLETE

| Step | Description                         | Status | Tests | Decision Notes |
|------|-------------------------------------|--------|-------|----------------|
| 1.1  | Model window setting (backend)      | done   | 7/7   | Stored as company_settings key, validated 6-120mo |
| 1.2  | Plumb window into forecast training | done   | 5/5   | _mrk_monthly_history gets months_back param |
| 1.3  | Fix XLSX upload parsing (bug fix)   | done   | 2/2   | Was pd.read_csv() for .xlsx; now branches on extension |
| 1.4  | Multi-sheet XLSX bulk upload        | done   | 4/4   | Year/quarter/month-named sheets; unknown falls back to first |
| 1.5  | Seasonality detection               | done   | 4/4   | Autocorrelation lag 12, r>0.3, stored in markov_models |
| 1.6  | Frontend model window control       | done   | manual| Slider in CompanySettings, stage-aware defaults |
| 1.7  | Frontend seasonality badge          | done   | manual| SeasonalityBadge on ForecastPage with r-value tooltip |
| 1.8  | Phase 1 documentation               | done   | --    | DocsPage, DevDocs updated |

## Phase 2: Excel Model Export with Live Formulas — COMPLETE

| Step | Description                         | Status | Tests | Decision Notes |
|------|-------------------------------------|--------|-------|----------------|
| 2.1  | Formula translation engine          | done   | 3/3   | CellMap class for cross-sheet cell references |
| 2.2  | Assumptions sheet (editable)        | done   | 2/2   | 15 params, yellow fill, unlocked cells |
| 2.3  | Actuals sheet (locked)              | done   | 2/2   | All KPIs x all months, locked, zebra striped |
| 2.4  | Forecast sheet (formulas + p50)     | done   | -     | Monte Carlo p50 values (stochastic != formulas) |
| 2.5  | P&L sheet (pure formulas)           | done   | 2/2   | GP=Rev-COGS, OI=GP-OpEx, EBITDA=OI*1.15 |
| 2.6  | Scenarios sheet (CAUSAL_MAP)        | done   | 2/2   | 3 scenarios max, output = base + sum(lever*coeff) |
| 2.7  | Confidence bands sheet              | done   | 2/2   | p10/p25/p50/p75/p90 color-coded |
| 2.8  | Dashboard sheet with charts         | done   | 2/2   | Revenue trend LineChart, summary metrics |
| 2.9  | Export endpoint + version tracking  | done   | 5/5   | GET /api/export/financial-model.xlsx + model_exports table |
| 2.10 | Frontend export buttons             | done   | manual| ForecastPage + ScenarioPlanner |
| 2.11 | Phase 2 documentation               | done   | --    | DocsPage, DevDocs, APIReference |

## Phase 4: Scenario-Forecast Bridge — COMPLETE

| Step | Description                         | Status | Tests | Decision Notes |
|------|-------------------------------------|--------|-------|----------------|
| 4.1  | Trained coefficients endpoint       | done   | 4/4   | GET /api/scenarios/trained-coefficients; merges ontology edges with static fallback per-KPI |
| 4.2  | Scenario-to-forecast bridge         | done   | 4/4   | POST /api/scenarios/run-forecast; percentile mapping for overrides |
| 4.3  | Frontend: calibration badge + "Run Through Model" | done | manual | Green/amber badge, p10/p50/p90 range bars |

## Phase 3: Formula-Aware Import + Diff Engine — COMPLETE

| Step | Description                         | Status | Tests | Decision Notes |
|------|-------------------------------------|--------|-------|----------------|
| 0a   | Populate assumption_snapshot        | done   | 1/1   | JSON of latest KPI values stored on each export |
| 3.1  | Financial model import endpoint     | done   | 6/6   | POST /api/import/financial-model; reads Assumptions sheet, diffs vs snapshot |
| 3.2  | Apply imported changes endpoint     | done   | 2/2   | POST /api/import/financial-model/apply; creates saved_scenario |
| 3.3  | Frontend re-import UI               | done   | manual| ReimportModelSection in CSVUpload; diff table, Apply button |
| 3.4  | Reconciliation report               | done   | manual| Inline in ReimportModelSection (changes table + scenario mapping) |
| 3.5  | Phase 3-4 documentation             | done   | --    | DocsPage, DevDocs updated |

## Phase 5: Google Sheets Live Sync — DEFERRED

| Step | Description                         | Status  | Tests | Decision Notes |
|------|-------------------------------------|---------|-------|----------------|
| 5.1  | Sheets API v4 push/pull             | deferred| 0/0   | Needs production OAuth testing |
| 5.2  | Shared Sheet with platform formulas | deferred| 0/0   | Write scope needed |
| 5.3  | Real-time actuals refresh           | deferred| 0/0   | Conflict resolution strategy TBD |

**Deferred reason**: Google Cloud OAuth credentials not production-tested. Connector exists (read-only) but needs write capability + scope escalation + conflict resolution design.

---

## Test Summary

| Checkpoint | Tests Passing | Delta | Notes |
|------------|---------------|-------|-------|
| Baseline   | 162           | --    | 1 known failure: test_upload_no_auth_behavior |
| Phase 1    | 184           | +22   | Model window, XLSX, seasonality |
| Phase 2    | 208           | +24   | Excel model export, version tracking |
| Phase 4    | 216           | +8    | Trained coefficients, scenario bridge |
| Phase 3    | 224           | +8    | Import, diff, apply |
| **TOTAL**  | **224 passed** | **+62** | **0 regressions throughout** |

## New Files Created

| File | Purpose | Tests |
|------|---------|-------|
| `backend/core/excel_model.py` | 7-sheet financial model workbook generator | 19 |
| `backend/tests/test_settings.py` | Model window settings | 7 |
| `backend/tests/test_forecast_window.py` | Windowed history + seasonality | 9 |
| `backend/tests/test_excel_model.py` | Workbook structure and formulas | 19 |
| `backend/tests/test_export_model.py` | Export endpoint + snapshot | 5 |
| `backend/tests/test_scenario_bridge.py` | Trained coefficients + bridge | 8 |
| `backend/tests/test_import_model.py` | Import + diff + apply | 8 |

## Files Modified

**Backend:**
- `routers/settings.py` — STAGE_DEFAULT_WINDOWS, model-window endpoint, validation
- `routers/forecast.py` — months_back, seasonality detection, seasonality_data column
- `routers/upload.py` — XLSX fix, multi-sheet parser
- `routers/analytics.py` — Financial model export + import endpoints, assumption_snapshot
- `routers/scenarios.py` — Trained coefficients, scenario-forecast bridge, _kpi_to_lever
- `core/database.py` — model_exports table
- `tests/test_upload.py` — XLSX and multi-sheet tests

**Frontend:**
- `components/CompanySettings.jsx` — Model window slider
- `components/ForecastPage.jsx` — SeasonalityBadge, export button
- `components/ScenarioPlanner.jsx` — Calibration badge, "Run Through Model", Monte Carlo panel, export button
- `components/CSVUpload.jsx` — ReimportModelSection (diff table, apply button)
- `components/DocsPage.jsx` — Financial model export, re-import, scenario bridge docs
- `components/DevDocs.jsx` — Changelog, decisions, schema updates

## Pending Work (Beyond Phase 5)

- OAuth end-to-end testing for QuickBooks, Xero, Salesforce
- Data quality & field mapping UI
- Board pack: render on screen, narrative, theme selector, email
- KPI audit export: 50 KPIs lack full verification formulas
- Criticality weights questionnaire UI
