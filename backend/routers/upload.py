"""
routers/upload.py — CSV upload, demo seeding, and projection endpoints.
"""
import io
import json
import os
import random
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, File, HTTPException, Request, UploadFile

from core.database import get_db, _audit
from core.deps import _get_workspace
from core.kpi_defs import normalize_columns, aggregate_monthly

router = APIRouter()

@router.post("/api/upload", tags=["Data Ingestion"])
async def upload_csv(request: Request, file: UploadFile = File(...)):
    """
    Upload a CSV file to update KPI data.

    **Supported columns** (case-insensitive, spaces/underscores normalised):
    - date / transaction_date / month / period
    - revenue / sales / total_revenue
    - cogs / cost_of_goods_sold
    - opex / operating_expenses
    - ar / accounts_receivable
    - mrr / monthly_recurring_revenue
    - arr / annual_recurring_revenue
    - customers / customer_count
    - churn / churned_customers
    - is_recurring (boolean / 0-1)
    - sm_allocated / sales_marketing
    - headcount / employees

    Returns column mapping detected and KPI preview.
    """
    workspace_id = _get_workspace(request)
    _allowed_exts = {".csv", ".CSV", ".xlsx", ".xls"}
    _ext = os.path.splitext(file.filename or "")[1]
    if _ext not in _allowed_exts:
        raise HTTPException(400, f"Unsupported file type '{_ext}'. Please upload a CSV or Excel file.")
    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(413, "File too large. Maximum upload size is 10 MB.")
    try:
        df = pd.read_csv(io.StringIO(content.decode("utf-8", errors="replace")))
    except Exception as e:
        raise HTTPException(400, "Could not parse file. Please ensure it is a valid CSV with column headers.")

    col_map      = normalize_columns(df)
    monthly_agg  = aggregate_monthly(df, col_map)

    conn = get_db()
    try:
        cur  = conn.execute(
            "INSERT INTO uploads (filename, uploaded_at, row_count, detected_columns, workspace_id) VALUES (?,?,?,?,?)",
            (file.filename, datetime.utcnow().isoformat(), len(df), json.dumps(col_map), workspace_id)
        )
        upload_id = cur.lastrowid

        for _, row in monthly_agg.iterrows():
            yr  = int(row["year"])
            mo  = int(row["month"])
            row_dict = {k: (None if (isinstance(v, float) and np.isnan(v)) else v)
                        for k, v in row.items() if k not in ("year", "month")}
            # Remove NaN
            conn.execute(
                "INSERT INTO monthly_data (upload_id, year, month, data_json, workspace_id) VALUES (?,?,?,?,?)",
                (upload_id, yr, mo, json.dumps(row_dict), workspace_id)
            )
        _audit(conn, "data_upload", "KPI data uploaded", "upload", str(upload_id))
        conn.commit()
    finally:
        conn.close()

    return {
        "upload_id":        upload_id,
        "filename":         file.filename,
        "rows_processed":   len(df),
        "months_detected":  len(monthly_agg),
        "columns_detected": col_map,
        "kpis_computed":    [k for k in monthly_agg.columns if k not in ("year", "month")],
        "message":          f"Successfully processed {len(df)} rows across {len(monthly_agg)} months.",
    }

@router.get("/api/uploads", tags=["Data Ingestion"])
def list_uploads(request: Request):
    """List all previously uploaded files."""
    workspace_id = _get_workspace(request)
    conn = get_db()
    rows = conn.execute("SELECT * FROM uploads WHERE workspace_id=? ORDER BY id DESC", [workspace_id]).fetchall()
    conn.close()
    return [{"id": r["id"], "filename": r["filename"], "uploaded_at": r["uploaded_at"],
             "row_count": r["row_count"], "columns": json.loads(r["detected_columns"])} for r in rows]

@router.delete("/api/uploads/{upload_id}", tags=["Data Ingestion"])
def delete_upload(request: Request, upload_id: int):
    """Remove an upload and its associated monthly KPI data."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT id FROM uploads WHERE id=? AND workspace_id=?", (upload_id, workspace_id)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Upload not found")
        conn.execute("DELETE FROM monthly_data WHERE upload_id=?", (upload_id,))
        conn.execute("DELETE FROM uploads WHERE id=?", (upload_id,))
        conn.commit()
    finally:
        conn.close()
    return {"deleted": upload_id}
@router.get("/api/seed-demo-projection", tags=["System"])
def seed_demo_projection(request: Request):
    """
    Seed 1,000 projected transaction rows — a slightly-more-optimistic plan
    vs the demo actuals.  Creates deliberate gaps so the bridge renders.

    Projection story:
      Revenue 6-10% above actuals every month
      Gross margin 1-2pp higher (lower COGS%)
      Churn 0.3-0.5pp lower  →  NRR higher
      DSO 2-4 days shorter   →  better Cash Cycle
      Result: most KPIs show yellow/red gaps in the bridge view.
    """
    import random
    random.seed(99)

    # Projected monthly params — more optimistic than actuals
    # mo  revenue   cogs%  f_opex   v_opex%  dso  rec%  churn%  cust  new  sm%
    MP_PROJ = [
      ( 1,  808_000,  36.8,  245_000, 10.2,    38,  78.0,  2.70,  425,  15, 0.42),
      ( 2,  828_000,  36.5,  243_000, 10.0,    36,  78.8,  2.50,  433,  17, 0.40),
      ( 3,  852_000,  36.2,  241_000,  9.8,    36,  79.5,  2.30,  443,  19, 0.38),
      ( 4,  886_000,  35.9,  239_000,  9.6,    33,  80.5,  2.00,  455,  22, 0.36),
      ( 5,  928_000,  35.6,  237_000,  9.4,    32,  81.5,  1.80,  468,  26, 0.34),
      ( 6,  978_000,  35.2,  235_000,  9.2,    30,  82.5,  1.70,  481,  28, 0.33),
      ( 7, 1_042_000, 34.8,  233_000,  9.0,    28,  83.0,  1.50,  496,  32, 0.32),
      ( 8, 1_112_000, 34.4,  231_000,  8.8,    27,  83.5,  1.30,  512,  36, 0.31),
      ( 9, 1_188_000, 34.1,  229_000,  8.6,    27,  84.0,  1.20,  530,  40, 0.30),
      (10, 1_178_000, 34.4,  231_000,  8.8,    32,  83.5,  1.40,  544,  28, 0.31),
      (11, 1_228_000, 34.1,  229_000,  8.6,    31,  84.0,  1.30,  558,  32, 0.30),
      (12, 1_315_000, 33.7,  225_000,  8.4,    38,  85.0,  1.00,  575,  42, 0.28),
    ]

    _RAW_SEGS = [
        ("Enterprise", 0.18, 4.8,  0.55),
        ("Mid-Market", 0.37, 1.3,  0.28),
        ("SMB",        0.45, 0.52, 0.14),
    ]
    _wt_avg = sum(s * m for _, s, m, _ in _RAW_SEGS)
    SEGS    = [(nm, s, m / _wt_avg, sd) for nm, s, m, sd in _RAW_SEGS]
    rows_per_month = [417, 417, 417, 417, 417, 417, 417, 417, 416, 416, 416, 416]  # = 5000

    tx_rows = []
    for i, (mo, rev, cogs_pct, f_opex, v_opex_pct, dso, rec_pct, churn_pct, cust, new_c, sm_pct) in enumerate(MP_PROJ):
        n           = rows_per_month[i]
        total_opex  = f_opex + rev * v_opex_pct / 100
        avg_rev_row = rev / n
        for _ in range(n):
            r = random.random(); cum = 0.0
            for seg, share, mult, std in SEGS:
                cum += share
                if r <= cum: break
            row_rev  = avg_rev_row * mult * max(0.35, 1 + random.gauss(0, std))
            row_cogs = row_rev * (cogs_pct / 100) * random.gauss(1.0, 0.025)
            row_opex = (total_opex / n) * random.gauss(1.0, 0.04)
            row_ar   = row_rev * (dso / 30)  * random.gauss(1.0, 0.07)
            is_rec   = 1 if random.random() < rec_pct   / 100 else 0
            row_sm   = row_opex * sm_pct * random.gauss(1.0, 0.05)
            row_churn= 1 if random.random() < churn_pct / 100 else 0
            day      = random.randint(1, 28)
            tx_rows.append({
                "date":         f"2025-{mo:02d}-{day:02d}",
                "revenue":      round(max(100,  row_rev),  2),
                "cogs":         round(max(0,    row_cogs), 2),
                "opex":         round(max(0,    row_opex), 2),
                "ar":           round(max(0,    row_ar),   2),
                "is_recurring": is_rec,
                "churn":        row_churn,
                "sm_allocated": round(max(0, row_sm), 2),
                "customers":    1,
            })

    df       = pd.DataFrame(tx_rows)
    col_map  = normalize_columns(df)
    base_agg = aggregate_monthly(df, col_map)

    base_by_mo: dict = {}
    for _, row in base_agg.iterrows():
        base_by_mo[int(row["month"])] = {
            k: v for k, v in row.items()
            if k not in ("year", "month") and v is not None
               and not (isinstance(v, float) and np.isnan(v))
        }

    mo_rev:  dict = {}
    mo_opex: dict = {}
    for g, grp in df.groupby(df["date"].str[5:7].astype(int)):
        mo_rev[g]  = grp["revenue"].sum()
        mo_opex[g] = grp["opex"].sum()

    final_kpis: dict = {}
    for mo, rev, cogs_pct, f_opex, v_opex_pct, dso, rec_pct, churn_pct, cust, new_c, sm_pct in MP_PROJ:
        kpis = dict(base_by_mo.get(mo, {}))
        kpis["dso"]                  = round(dso * random.gauss(1.0, 0.02), 1)
        kpis["cash_conv_cycle"]      = round(kpis["dso"] + 8.0 + random.gauss(0, 0.5), 1)
        kpis["ar_turnover"]          = round(365 / max(1, kpis["dso"] * (365/30)) * random.gauss(1.0, 0.03), 2)
        kpis["avg_collection_period"]= round(365 / max(0.1, kpis["ar_turnover"]) * random.gauss(1.0, 0.02), 1)
        _cei_base                    = max(70, 100 - (kpis["dso"] - 25) * 0.6)
        kpis["cei"]                  = round(min(99, _cei_base * random.gauss(1.0, 0.015)), 1)
        _overdue                     = round(max(5, min(60, (kpis["dso"] - 20) * 0.8 + random.gauss(0, 2))), 1)
        kpis["ar_aging_overdue"]     = _overdue
        kpis["ar_aging_current"]     = round(100 - _overdue, 1)
        kpis["billable_utilization"] = round(random.gauss(72, 3), 1)
        kpis["revenue_quality"]      = round(rec_pct + random.gauss(0, 0.3), 2)
        kpis["recurring_revenue"]    = kpis["revenue_quality"]
        nrr_base = 115.43 - 5.29 * churn_pct
        kpis["churn_rate"] = round(churn_pct + random.gauss(0, 0.05), 2)
        kpis["nrr"]        = round(nrr_base  + random.gauss(0, 0.25), 1)
        kpis["customer_concentration"] = round(26.0 - (cust - 418) / 420 * 8.0 + random.gauss(0, 0.4), 1)
        final_kpis[mo] = kpis

    mos_sorted = sorted(final_kpis.keys())
    for idx, mo in enumerate(mos_sorted):
        kpis   = final_kpis[mo]
        params = MP_PROJ[mo - 1]
        act_rev   = params[1]
        act_opex  = params[3] + params[1] * params[4] / 100
        sm_spend  = act_opex * params[10]
        cust      = params[8]
        new_c     = params[9]
        churn_pct = params[7]
        gross_m   = kpis.get("gross_margin", 62.0) / 100
        arpu_mo   = act_rev / max(cust, 1)
        cac       = sm_spend / max(new_c, 1)
        kpis["cac_payback"] = round(cac / max(arpu_mo * gross_m, 1), 1)

        if idx == 0:
            kpis["sales_efficiency"] = round((new_c * arpu_mo * 12) / max(sm_spend * 12, 1), 2)
            kpis["burn_multiple"]    = round(min(5.0, sm_spend / max(new_c * arpu_mo, 1)), 2)
        else:
            prev_mo       = mos_sorted[idx - 1]
            prev_rev      = MP_PROJ[prev_mo - 1][1]
            prev_cogs_pct = MP_PROJ[prev_mo - 1][2]
            prev_opex     = MP_PROJ[prev_mo - 1][3] + MP_PROJ[prev_mo - 1][1] * MP_PROJ[prev_mo - 1][4] / 100
            prev_op       = prev_rev * (1 - prev_cogs_pct / 100) - prev_opex
            curr_op       = act_rev  * (1 - params[2]     / 100) - act_opex
            delta_rev         = act_rev - prev_rev
            rev_growth_pct    = delta_rev / prev_rev * 100 if prev_rev else 0
            kpis["revenue_growth"] = round(rev_growth_pct, 2)
            kpis["arr_growth"]     = round(rev_growth_pct * 0.88 + random.gauss(0, 0.18), 2)
            if abs(rev_growth_pct) > 0.3 and prev_op > 0:
                op_inc_pct = (curr_op - prev_op) / prev_op * 100
                kpis["operating_leverage"] = round(max(-5.0, min(8.0, op_inc_pct / rev_growth_pct)), 2)
            if delta_rev > 0:
                kpis["sales_efficiency"] = round((delta_rev * 12) / max(sm_spend, 1), 2)
                kpis["burn_multiple"]    = round(min(5.0, sm_spend / max(delta_rev * 12, 1)), 2)
            else:
                kpis["sales_efficiency"] = round(max(0.05, sm_spend * 0.05 / max(sm_spend, 1)), 2)
                kpis["burn_multiple"]    = 5.0
        final_kpis[mo] = kpis

    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    try:
        conn.execute("DELETE FROM projection_monthly_data WHERE workspace_id=?", (workspace_id,))
        conn.execute("DELETE FROM projection_uploads WHERE workspace_id=?", (workspace_id,))
        cur = conn.execute(
            "INSERT INTO projection_uploads (filename, uploaded_at, row_count, detected_columns, workspace_id) VALUES (?,?,?,?,?)",
            ("demo_projection_1000.csv", datetime.utcnow().isoformat(), len(df),
             json.dumps({c: c for c in df.columns}), workspace_id)
        )
        upload_id = cur.lastrowid
        for mo, kpis in final_kpis.items():
            clean = {k: (None if isinstance(v, float) and np.isnan(v) else v) for k, v in kpis.items()}
            conn.execute(
                "INSERT INTO projection_monthly_data (projection_upload_id, year, month, data_json, workspace_id) VALUES (?,?,?,?,?)",
                (upload_id, 2025, mo, json.dumps(clean), workspace_id)
            )
        conn.commit()
    finally:
        conn.close()
    return {
        "seeded": True, "months": 12, "transactions": len(df), "upload_id": upload_id,
        "message": "Demo projection seeded — 12 months optimistic plan vs actuals.",
    }


# ─── Projection Endpoints ────────────────────────────────────────────────────

@router.post("/api/projection/upload", tags=["Projection"])
async def upload_projection(request: Request, file: UploadFile = File(...), version_label: Optional[str] = None):
    """Upload a projection CSV (same format as actuals). Replaces any existing projection with the same version label."""
    workspace_id = _get_workspace(request)
    if not file.filename.endswith((".csv", ".CSV")):
        raise HTTPException(400, "Only CSV files are accepted.")
    content = await file.read()
    try:
        df = pd.read_csv(io.StringIO(content.decode("utf-8", errors="replace")))
    except Exception as e:
        raise HTTPException(400, f"Could not parse CSV: {e}")

    vlabel = version_label or "v1"
    col_map     = normalize_columns(df)
    monthly_agg = aggregate_monthly(df, col_map)

    conn = get_db()
    try:
        # Delete-before-insert: only remove rows with the same version_label for this workspace
        old_ids = [r["id"] for r in conn.execute(
            "SELECT id FROM projection_uploads WHERE version_label=? AND workspace_id=?", (vlabel, workspace_id)
        ).fetchall()]
        for oid in old_ids:
            conn.execute("DELETE FROM projection_monthly_data WHERE projection_upload_id=?", (oid,))
        conn.execute("DELETE FROM projection_uploads WHERE version_label=? AND workspace_id=?", (vlabel, workspace_id))

        cur = conn.execute(
            "INSERT INTO projection_uploads (filename, uploaded_at, row_count, detected_columns, version_label, workspace_id) VALUES (?,?,?,?,?,?)",
            (file.filename, datetime.utcnow().isoformat(), len(df), json.dumps(col_map), vlabel, workspace_id)
        )
        upload_id = cur.lastrowid

        for _, row in monthly_agg.iterrows():
            yr  = int(row["year"])
            mo  = int(row["month"])
            row_dict = {k: (None if (isinstance(v, float) and np.isnan(v)) else v)
                        for k, v in row.items() if k not in ("year", "month")}
            conn.execute(
                "INSERT INTO projection_monthly_data (projection_upload_id, year, month, data_json, version_label, workspace_id) VALUES (?,?,?,?,?,?)",
                (upload_id, yr, mo, json.dumps(row_dict), vlabel, workspace_id)
            )
        conn.commit()
    finally:
        conn.close()

    return {
        "upload_id":        upload_id,
        "filename":         file.filename,
        "rows_processed":   len(df),
        "months_detected":  len(monthly_agg),
        "columns_detected": col_map,
        "kpis_computed":    [k for k in monthly_agg.columns if k not in ("year", "month")],
        "version_label":    vlabel,
        "message":          f"Projection uploaded: {len(df)} rows across {len(monthly_agg)} months (version: {vlabel}).",
    }


@router.get("/api/projection/monthly", tags=["Projection"])
def projection_monthly_kpis(request: Request, year: Optional[int] = None):
    """Return projected monthly KPI values. Optionally filter by year."""
    workspace_id = _get_workspace(request)
    conn = get_db()
    query  = "SELECT * FROM projection_monthly_data WHERE workspace_id=?"
    params: list = [workspace_id]
    if year:
        query += " AND year = ?"
        params.append(year)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    result = []
    for row in rows:
        result.append({"year": row["year"], "month": row["month"], "kpis": json.loads(row["data_json"])})
    return sorted(result, key=lambda x: (x["year"], x["month"]))


@router.get("/api/projection/uploads", tags=["Projection"])
def list_projection_uploads(request: Request):
    """List all projection uploads."""
    workspace_id = _get_workspace(request)
    conn = get_db()
    rows = conn.execute("SELECT * FROM projection_uploads WHERE workspace_id=? ORDER BY id DESC", [workspace_id]).fetchall()
    conn.close()
    return [{"id": r["id"], "filename": r["filename"], "uploaded_at": r["uploaded_at"],
             "row_count": r["row_count"], "columns": json.loads(r["detected_columns"])} for r in rows]


@router.delete("/api/projection/uploads/{upload_id}", tags=["Projection"])
def delete_projection_upload(request: Request, upload_id: int):
    """Remove a projection upload and its associated monthly data."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT id FROM projection_uploads WHERE id=? AND workspace_id=?",
            (upload_id, workspace_id),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Projection upload not found")
        conn.execute("DELETE FROM projection_monthly_data WHERE projection_upload_id=?", (upload_id,))
        conn.execute("DELETE FROM projection_uploads WHERE id=?", (upload_id,))
        conn.commit()
    finally:
        conn.close()
    return {"deleted": upload_id}


@router.get("/api/projection/versions", tags=["Projection"])
def get_projection_versions(request: Request):
    workspace_id = _get_workspace(request)
    conn = get_db()
    rows = conn.execute(
        "SELECT DISTINCT version_label, MIN(uploaded_at) as first_uploaded FROM projection_uploads WHERE workspace_id=? GROUP BY version_label ORDER BY first_uploaded DESC",
        [workspace_id]
    ).fetchall()
    conn.close()
    return {"versions": [{"label": r["version_label"] or "v1", "uploaded_at": r["first_uploaded"]} for r in rows]}



@router.get("/api/seed-demo", tags=["System"])
def seed_demo(request: Request):
    """
    Seed 1,000 transaction rows + 12 months of fully correlated KPI data.

    Embedded correlations (all statistically meaningful):
      Revenue Growth  ↑  ↔  Operating Leverage  ↑   (fixed-cost base absorbs growth)
      Revenue Growth  ↑  ↔  Sales Efficiency    ↑   (same team, more output)
      Revenue Growth  ↑  ↔  Burn Multiple       ↓   (more ARR per burn dollar)
      Revenue Growth  ↑  ↔  CAC Payback         ↓   (efficiency compounds)
      Revenue Growth  ↑  ↔  OpEx Ratio          ↓   (operating leverage)
      Churn Rate      ↓  ↔  NRR                 ↑   (near-perfect inverse)
      Churn Rate      ↓  ↔  Revenue Growth      ↑   (retention fuels growth)
      Gross Margin    ↑  ↔  Contribution Margin ↑   (parallel expansion)
      DSO             ↑  ↔  Cash Conv Cycle     ↑   (DSO is the primary driver)
      ARR Growth      ≈  Revenue Growth × 0.9       (lagged subscription effect)

    Story arc FY2025:
      Q1 — Post-holiday slowdown, budget freezes, churn elevated, S&M front-loaded
      Q2 — Stabilisation; sales investment starts paying off; churn easing
      Q3 — Breakout: revenue accelerates, operating leverage spikes, burn multiple halves
      Q4 — Oct softness (pipeline reset), Nov recovery, Dec year-end surge
    """
    import random
    random.seed(42)

    # ── Monthly causal parameters ─────────────────────────────────────────────
    # Columns: (month, revenue, cogs_pct, fixed_opex, var_opex_pct,
    #           dso_days, recur_pct, churn_pct, customers, new_cust, sm_pct_opex)
    #
    # Revenue is the PRIMARY driver; everything else is derived or set causally.
    # fixed_opex = headcount / rent cost (does NOT scale with revenue → leverage)
    # var_opex_pct = variable S&M + support as % of revenue
    # sm_pct_opex = S&M share of total opex (drives sales efficiency calc)

    MP = [
      # mo  revenue   cogs%  f_opex   v_opex%  dso   rec%  churn%  cust  new  sm%
      ( 1,  750_000,  38.5,  248_000, 11.0,    42,   76.0,  3.20,  418,  12, 0.44),
      ( 2,  764_000,  38.2,  246_000, 10.8,    40,   76.8,  3.00,  425,  14, 0.42),
      ( 3,  782_000,  38.0,  244_000, 10.6,    40,   77.5,  2.80,  433,  16, 0.40),
      ( 4,  810_000,  37.8,  242_000, 10.4,    37,   78.5,  2.50,  442,  18, 0.38),
      ( 5,  844_000,  37.5,  240_000, 10.2,    36,   79.5,  2.30,  452,  21, 0.36),
      ( 6,  886_000,  37.2,  238_000, 10.0,    35,   80.5,  2.20,  463,  24, 0.35),
      ( 7,  940_000,  36.8,  236_000,  9.8,    33,   81.0,  2.00,  475,  28, 0.34),
      ( 8, 1_001_000, 36.5,  234_000,  9.6,    32,   81.5,  1.80,  488,  32, 0.33),
      ( 9, 1_071_000, 36.2,  232_000,  9.4,    32,   82.0,  1.70,  502,  36, 0.32),
      (10, 1_050_000, 36.5,  234_000,  9.6,    38,   81.5,  1.90,  512,  22, 0.33),  # Q4 dip
      (11, 1_097_000, 36.2,  232_000,  9.4,    37,   82.0,  1.80,  524,  28, 0.32),
      (12, 1_178_000, 35.8,  228_000,  9.2,    44,   83.0,  1.50,  540,  38, 0.30),  # year-end surge
    ]

    # ── Generate 1,000 transaction rows ──────────────────────────────────────
    # Segments define deal-size distribution (relative to avg_rev_per_row).
    # CRITICAL: normalise multipliers so their share-weighted average = 1.0,
    # otherwise monthly revenue total diverges from MP targets and all
    # margin / leverage KPIs become nonsensical.
    _RAW_SEGS = [
        ("Enterprise",  0.18, 4.8,  0.55),  # (name, share, mult, noise_std)
        ("Mid-Market",  0.37, 1.3,  0.28),
        ("SMB",         0.45, 0.52, 0.14),
    ]
    _wt_avg = sum(s * m for _, s, m, _ in _RAW_SEGS)   # = 1.579  → must → 1.0
    SEGS = [(nm, s, m / _wt_avg, sd) for nm, s, m, sd in _RAW_SEGS]

    rows_per_month = [417, 417, 417, 417, 417, 417, 417, 417, 416, 416, 416, 416]  # = 5000

    tx_rows = []
    for i, (mo, rev, cogs_pct, f_opex, v_opex_pct, dso, rec_pct, churn_pct, cust, new_c, sm_pct) in enumerate(MP):
        n = rows_per_month[i]
        total_opex  = f_opex + rev * v_opex_pct / 100
        avg_rev_row = rev / n

        for j in range(n):
            # Pick segment
            r = random.random()
            cum = 0.0
            for seg, share, mult, std in SEGS:
                cum += share
                if r <= cum:
                    break
            # With normalised multipliers, E[row_rev] = avg_rev_row → sum ≈ rev
            row_rev  = avg_rev_row * mult * max(0.35, 1 + random.gauss(0, std))
            row_cogs = row_rev * (cogs_pct / 100) * random.gauss(1.0, 0.025)
            row_opex = (total_opex / n) * random.gauss(1.0, 0.04)
            row_ar   = row_rev * (dso / 30) * random.gauss(1.0, 0.07)
            is_rec   = 1 if random.random() < rec_pct / 100 else 0
            row_sm   = row_opex * sm_pct * random.gauss(1.0, 0.05)
            row_churn= 1 if random.random() < churn_pct / 100 else 0
            day      = random.randint(1, 28)

            tx_rows.append({
                "date":         f"2025-{mo:02d}-{day:02d}",
                "revenue":      round(max(100, row_rev), 2),
                "cogs":         round(max(0, row_cogs), 2),
                "opex":         round(max(0, row_opex), 2),
                "ar":           round(max(0, row_ar), 2),
                "is_recurring": is_rec,
                "churn":        row_churn,
                "sm_allocated": round(max(0, row_sm), 2),
                "customers":    1,
            })

    df = pd.DataFrame(tx_rows)

    # ── Insert upload record ──────────────────────────────────────────────────
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    conn.execute("DELETE FROM monthly_data WHERE workspace_id=?", (workspace_id,))
    conn.execute("DELETE FROM uploads WHERE workspace_id=?", (workspace_id,))
    col_map_stored = {c: c for c in df.columns}
    cur = conn.execute(
        "INSERT INTO uploads (filename, uploaded_at, row_count, detected_columns, workspace_id) VALUES (?,?,?,?,?)",
        ("demo_correlated_5000.csv", datetime.utcnow().isoformat(), len(df),
         json.dumps(col_map_stored), workspace_id)
    )
    upload_id = cur.lastrowid

    # ── Compute base KPIs from aggregated transactions ────────────────────────
    col_map  = normalize_columns(df)
    base_agg = aggregate_monthly(df, col_map)   # gross_margin, opex_ratio, churn_rate, etc.

    # Build base lookup keyed by month number
    base_by_mo: dict = {}
    for _, row in base_agg.iterrows():
        base_by_mo[int(row["month"])] = {
            k: v for k, v in row.items()
            if k not in ("year", "month") and v is not None
               and not (isinstance(v, float) and np.isnan(v))
        }

    # ── Override/add cross-period + causally-derived KPIs ────────────────────
    # These require multi-month context or are intentionally tuned for correlation.
    #
    # revenue_growth    — MoM from actuals (post-agg)
    # arr_growth        — lags revenue_growth by ~1 month (subscription bookings)
    # operating_leverage— Δ op_income% / Δ rev%  (requires 2 consecutive months)
    # sales_efficiency  — new ARR this mo / S&M spend this mo
    # burn_multiple     — net burn / new ARR  (falls as growth accelerates)
    # cac_payback       — 1 / (sales_efficiency × gross_margin)  (inverse)
    # nrr               — 100 + (100 - churn_rate × 20) + expansion_proxy
    # dso               — override with seasonal curve (from MP table)
    # cash_conv_cycle   — dso + inventory days (constant 8d)
    # customer_concentration — falls as customer base grows

    # Derive actual monthly revenues from transactions
    mo_rev: dict = {}
    for g, grp in df.groupby(df["date"].str[5:7].astype(int)):
        mo_rev[g] = grp["revenue"].sum()

    # Derive actual monthly opex
    mo_opex: dict = {}
    for g, grp in df.groupby(df["date"].str[5:7].astype(int)):
        mo_opex[g] = grp["opex"].sum()

    # Derive actual monthly operating income
    mo_op_inc: dict = {}
    for g, grp in df.groupby(df["date"].str[5:7].astype(int)):
        rev_g  = grp["revenue"].sum()
        cogs_g = grp["cogs"].sum()
        opex_g = grp["opex"].sum()
        mo_op_inc[g] = rev_g - cogs_g - opex_g

    final_kpis: dict = {}   # mo → kpi_dict

    for mo, rev, cogs_pct, f_opex, v_opex_pct, dso, rec_pct, churn_pct, cust, new_c, sm_pct in MP:
        kpis = dict(base_by_mo.get(mo, {}))

        act_rev    = mo_rev.get(mo, rev)
        act_opex   = mo_opex.get(mo, f_opex + rev * v_opex_pct / 100)
        act_op_inc = mo_op_inc.get(mo, 0)
        sm_spend   = act_opex * sm_pct

        # ── DSO & Cash Cycle (seasonal; Dec high = year-end billing) ─────────
        kpis["dso"]                  = round(dso * random.gauss(1.0, 0.02), 1)
        kpis["cash_conv_cycle"]      = round(kpis["dso"] + 8.0 + random.gauss(0, 0.5), 1)
        kpis["ar_turnover"]          = round(365 / max(1, kpis["dso"] * (365/30)) * random.gauss(1.0, 0.03), 2)
        kpis["avg_collection_period"]= round(365 / max(0.1, kpis["ar_turnover"]) * random.gauss(1.0, 0.02), 1)
        _cei_base                    = max(70, 100 - (kpis["dso"] - 25) * 0.6)
        kpis["cei"]                  = round(min(99, _cei_base * random.gauss(1.0, 0.015)), 1)
        _overdue                     = round(max(5, min(60, (kpis["dso"] - 20) * 0.8 + random.gauss(0, 2))), 1)
        kpis["ar_aging_overdue"]     = _overdue
        kpis["ar_aging_current"]     = round(100 - _overdue, 1)
        kpis["billable_utilization"] = round(random.gauss(72, 3), 1)

        # ── Revenue Quality / Recurring Revenue ──────────────────────────────
        kpis["revenue_quality"]      = round(rec_pct + random.gauss(0, 0.3), 2)
        kpis["recurring_revenue"]    = kpis["revenue_quality"]

        # ── Churn Rate → NRR (near-perfect inverse, R ≈ -0.99) ──────────────
        # Calibrated linear: NRR = 98.5 when churn = 3.2%  (budget-freeze Jan)
        #                    NRR = 107.5 when churn = 1.5%  (year-end Dec surge)
        # Slope = (107.5 - 98.5) / (1.5 - 3.2) = -5.29
        # Intercept = 98.5 - (-5.29) × 3.2 = 98.5 + 16.93 = 115.43
        nrr_base = 115.43 - 5.29 * churn_pct
        kpis["churn_rate"] = round(churn_pct + random.gauss(0, 0.05), 2)
        kpis["nrr"]        = round(nrr_base + random.gauss(0, 0.25), 1)

        # ── Customer Concentration (dilutes as base grows) ───────────────────
        kpis["customer_concentration"] = round(26.0 - (cust - 418) / 420 * 8.0 + random.gauss(0, 0.4), 1)

        final_kpis[mo] = kpis

    # ── Multi-period KPIs (need 2 consecutive months) ────────────────────────
    # For Δ-revenue KPIs (growth, sales_efficiency, burn_multiple,
    # operating_leverage) we use the DETERMINISTIC MP target revenues, not
    # transaction aggregations.  Transactions have too much per-row variance
    # (σ ≈ $58K/month) which dwarfs small Δ-rev signals ($14K Feb→Jan).
    # Single-period KPIs (gross_margin, churn_rate, etc.) still come from
    # the aggregated transactions via base_by_mo.
    mos_sorted = sorted(final_kpis.keys())
    for idx, mo in enumerate(mos_sorted):
        kpis   = final_kpis[mo]
        params = MP[mo - 1]   # (mo, rev, cogs_pct, f_opex, v_opex_pct, dso, rec_pct, churn_pct, cust, new_c, sm_pct)
        # Use MP target values for multi-period calculations
        act_rev    = params[1]
        act_opex   = params[3] + params[1] * params[4] / 100   # f_opex + var_opex
        sm_spend   = act_opex * params[10]
        cust       = params[8]
        new_c      = params[9]
        churn_pct  = params[7]

        gross_m = kpis.get("gross_margin", 62.0) / 100
        arpu_mo = act_rev / max(cust, 1)          # monthly revenue per customer

        # ── CAC Payback = (S&M / new_customers) / (ARPU_mo × GM%) ───────────
        # Improves as: S&M per new-cust falls (more new cust / same spend)
        #              OR gross margin expands
        #              OR ARPU grows (higher-value deals closing)
        cac      = sm_spend / max(new_c, 1)
        kpis["cac_payback"] = round(cac / max(arpu_mo * gross_m, 1), 1)

        if idx == 0:
            # Jan: first month — no Δ-revenue, use new_customer-based proxies
            # Sales Efficiency proxy: (new_c × ARPU_annual) / (S&M_annual)
            kpis["sales_efficiency"] = round(
                (new_c * arpu_mo * 12) / max(sm_spend * 12, 1), 2
            )
            # Burn Multiple proxy: S&M / (new_c × ARPU_mo)
            new_mrr = new_c * arpu_mo
            kpis["burn_multiple"] = round(min(5.0, sm_spend / max(new_mrr, 1)), 2)
        else:
            prev_mo  = mos_sorted[idx - 1]
            prev_rev = MP[prev_mo - 1][1]   # deterministic target
            prev_cogs_pct = MP[prev_mo - 1][2]
            prev_opex = MP[prev_mo - 1][3] + MP[prev_mo - 1][1] * MP[prev_mo - 1][4] / 100
            prev_op  = prev_rev * (1 - prev_cogs_pct/100) - prev_opex
            curr_cogs_pct = params[2]
            curr_op  = act_rev * (1 - curr_cogs_pct/100) - act_opex

            delta_rev      = act_rev - prev_rev
            rev_growth_pct = delta_rev / prev_rev * 100 if prev_rev else 0
            kpis["revenue_growth"] = round(rev_growth_pct, 2)
            kpis["arr_growth"]     = round(rev_growth_pct * 0.88 + random.gauss(0, 0.18), 2)

            # Operating leverage = (% Δ op_income) / (% Δ revenue)
            # Large fixed cost base guarantees op_lev > 1 when rev grows → converges
            if abs(rev_growth_pct) > 0.3 and prev_op > 0:
                op_inc_pct = (curr_op - prev_op) / prev_op * 100
                kpis["operating_leverage"] = round(max(-5.0, min(8.0, op_inc_pct / rev_growth_pct)), 2)
            elif rev_growth_pct < 0 and prev_op > 0 and curr_op < prev_op:
                op_inc_pct = (curr_op - prev_op) / prev_op * 100
                kpis["operating_leverage"] = round(max(-5.0, op_inc_pct / rev_growth_pct), 2)

            # Sales Efficiency = "Magic Number"  = (Δ Rev × 12) / S&M spend
            # Rises with revenue growth (same team, accelerating output)
            # Goes to ~0 when revenue declines (Oct dip)
            if delta_rev > 0:
                kpis["sales_efficiency"] = round((delta_rev * 12) / max(sm_spend, 1), 2)
            else:
                # Revenue dipped: efficiency near zero but not negative
                kpis["sales_efficiency"] = round(max(0.05, sm_spend * 0.05 / max(sm_spend, 1)), 2)

            # Burn Multiple = S&M / (Δ Rev × 12)
            # Inverse of sales efficiency — falls dramatically as growth accelerates
            # Capped at 5.0 when revenue declines (Oct reset month)
            if delta_rev > 0:
                kpis["burn_multiple"] = round(min(5.0, sm_spend / max(delta_rev * 12, 1)), 2)
            else:
                kpis["burn_multiple"] = 5.0   # maximum penalty for declining revenue

        final_kpis[mo] = kpis

    # ── Persist ───────────────────────────────────────────────────────────────
    for mo, kpis in final_kpis.items():
        clean = {k: (None if isinstance(v, float) and np.isnan(v) else v)
                 for k, v in kpis.items()}
        conn.execute(
            "INSERT INTO monthly_data (upload_id, year, month, data_json, workspace_id) VALUES (?,?,?,?,?)",
            (upload_id, 2025, mo, json.dumps(clean), workspace_id)
        )

    conn.commit()
    conn.close()
    return {
        "seeded":       True,
        "months":       12,
        "transactions": len(df),
        "upload_id":    upload_id,
        "correlations": [
            "Revenue Growth ↔ Operating Leverage (pos)",
            "Revenue Growth ↔ Sales Efficiency (pos)",
            "Revenue Growth ↔ Burn Multiple (neg)",
            "Revenue Growth ↔ CAC Payback (neg)",
            "Revenue Growth ↔ OpEx Ratio (neg)",
            "Churn Rate ↔ NRR (neg, R≈-0.99)",
            "DSO ↔ Cash Conv Cycle (pos, R≈0.97)",
            "Gross Margin ↔ Contribution Margin (pos)",
        ],
    }

@router.get("/api/seed-multiyear", tags=["System"])
def seed_multiyear(request: Request):
    """
    Seed 5 years of KPI data (2021–2025 actuals + 2026 actuals Jan–Mar + 2026 projection Apr–Dec).

    Narrative arc:
      2021 — Early-stage startup: fast growth from tiny base, high churn, burning cash
      2022 — Series B scaling: rapid hiring, revenue accelerating, margins improving
      2023 — Plateau: growth stalls, headcount too large, efficiency declining
      2024 — Warning signals: churn uptick, margin compression, burn rising
      2025 — Mixed recovery: volatile, some bright spots but fragile
      2026 Jan–Mar — Critical: churn worsening, revenue under pressure
      2026 Apr–Dec — Forecast: recovery scenario if corrective action taken
    """
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    import random
    random.seed(99)
    rng = random.gauss

    def lerp(a, b, t):
        return a + (b - a) * t

    def mo_val(start, end, mo, noise=0.0):
        t = (mo - 1) / 11.0
        base = lerp(start, end, t)
        return round(base + rng(0, noise), 4) if noise else round(base, 4)

    # ── Year phase definitions ────────────────────────────────────────────────
    # Each year: dict of KPI → (jan_val, dec_val, monthly_noise_sigma)
    PHASES = {
        2021: {  # Startup — growth from tiny base, high churn, cash-negative
            "revenue_growth":        (14.0,  9.0,  1.5),
            "gross_margin":          (51.0, 56.5,  0.4),
            "operating_margin":      (-22.0, -6.0, 1.0),
            "ebitda_margin":         (-19.0, -4.0, 1.0),
            "cash_conv_cycle":       (68.0, 58.0,  1.5),
            "dso":                   (62.0, 52.0,  1.2),
            "arr_growth":            (12.0,  7.5,  1.5),
            "nrr":                   (84.0, 93.0,  0.8),
            "burn_multiple":         (4.8,  4.0,   0.2),
            "opex_ratio":            (88.0, 76.0,  1.5),
            "contribution_margin":   (28.0, 38.0,  0.8),
            "revenue_quality":       (54.0, 63.0,  0.5),
            "cac_payback":           (29.0, 22.0,  1.0),
            "sales_efficiency":      (0.10, 0.22,  0.02),
            "customer_concentration":(50.0, 44.0,  1.0),
            "recurring_revenue":     (54.0, 63.0,  0.5),
            "churn_rate":            (7.8,  5.5,   0.3),
            "operating_leverage":    (-0.8,  0.4,  0.2),
            "pipeline_conversion":   (2.5,  4.5,  0.3),
            "customer_ltv":          (32.0, 48.0, 1.5),
            "pricing_power_index":   (2.0,  5.0,  0.5),
            "cpl":                   (320.0, 240.0, 18.0),
            "mql_sql_rate":          (14.0,  20.0,  1.5),
            "win_rate":              (16.0,  22.0,  1.5),
            "quota_attainment":      (52.0,  64.0,  2.5),
            "marketing_roi":         (1.2,   1.8,   0.15),
            "headcount_eff":         (0.6,   0.9,   0.05),
            "rev_per_employee":      (65.0,  95.0,  4.0),
            "ltv_cac":               (1.2,   1.8,   0.12),
            "expansion_rate":        (4.0,   9.0,   0.8),
            "health_score":          (48.0,  58.0,  2.0),
            "logo_retention":        (72.0,  80.0,  1.0),
            "payback_period":        (38.0,  28.0,  1.5),
        },
        2022: {  # Series B — rapid scaling, margins climbing, team building
            "revenue_growth":        (16.0, 12.0,  1.2),
            "gross_margin":          (57.0, 64.0,  0.4),
            "operating_margin":      (-8.0,  3.5,  0.8),
            "ebitda_margin":         (-6.0,  5.5,  0.8),
            "cash_conv_cycle":       (56.0, 44.0,  1.2),
            "dso":                   (51.0, 40.0,  1.0),
            "arr_growth":            (14.0, 10.5,  1.2),
            "nrr":                   (96.0,109.0,  0.7),
            "burn_multiple":         (3.8,  1.8,   0.2),
            "opex_ratio":            (72.0, 52.0,  1.5),
            "contribution_margin":   (40.0, 56.0,  0.8),
            "revenue_quality":       (65.0, 76.0,  0.5),
            "cac_payback":           (20.0, 11.0,  0.8),
            "sales_efficiency":      (0.22, 0.72,  0.03),
            "customer_concentration":(42.0, 28.0,  1.0),
            "recurring_revenue":     (65.0, 76.0,  0.5),
            "churn_rate":            (4.4,  2.2,   0.25),
            "operating_leverage":    (0.6,  2.8,   0.2),
            "pipeline_conversion":   (5.0,  8.5,  0.3),
            "customer_ltv":          (52.0, 92.0, 2.0),
            "pricing_power_index":   (4.0,  8.5,  0.6),
            "cpl":                   (220.0, 130.0, 12.0),
            "mql_sql_rate":          (22.0,  31.0,  1.2),
            "win_rate":              (24.0,  34.0,  1.2),
            "quota_attainment":      (68.0,  88.0,  2.0),
            "marketing_roi":         (2.2,   4.2,   0.2),
            "headcount_eff":         (1.0,   1.6,   0.05),
            "rev_per_employee":      (100.0, 165.0, 5.0),
            "ltv_cac":               (2.2,   4.8,   0.2),
            "expansion_rate":        (11.0,  26.0,  1.2),
            "health_score":          (60.0,  76.0,  1.5),
            "logo_retention":        (82.0,  92.0,  0.8),
            "payback_period":        (26.0,  12.0,  1.2),
        },
        2023: {  # Plateau — growth stalls, team too large, efficiency slipping
            "revenue_growth":        (4.0,  1.2,   0.6),
            "gross_margin":          (64.5, 65.8,  0.3),
            "operating_margin":      (3.5,  6.5,   0.5),
            "ebitda_margin":         (5.5,  9.0,   0.5),
            "cash_conv_cycle":       (44.0, 48.0,  1.0),
            "dso":                   (40.0, 44.0,  0.8),
            "arr_growth":            (3.5,  1.0,   0.6),
            "nrr":                   (109.0,106.0, 0.6),
            "burn_multiple":         (1.8,  3.2,   0.2),
            "opex_ratio":            (50.0, 44.0,  1.0),
            "contribution_margin":   (56.0, 59.0,  0.5),
            "revenue_quality":       (76.5, 81.0,  0.4),
            "cac_payback":           (11.0, 14.5,  0.6),
            "sales_efficiency":      (0.70, 0.30,  0.03),
            "customer_concentration":(27.0, 21.0,  0.8),
            "recurring_revenue":     (76.5, 81.0,  0.4),
            "churn_rate":            (2.1,  2.7,   0.15),
            "operating_leverage":    (2.5,  1.2,   0.2),
            "pipeline_conversion":   (8.0,  5.5,  0.3),
            "customer_ltv":          (88.0, 82.0, 1.5),
            "pricing_power_index":   (3.5,  1.0,  0.5),
            "cpl":                   (135.0, 168.0, 8.0),
            "mql_sql_rate":          (30.0,  24.0,  1.0),
            "win_rate":              (33.0,  26.0,  1.0),
            "quota_attainment":      (86.0,  74.0,  1.8),
            "marketing_roi":         (4.0,   2.8,   0.15),
            "headcount_eff":         (1.6,   1.1,   0.04),
            "rev_per_employee":      (162.0, 138.0, 4.0),
            "ltv_cac":               (4.5,   3.2,   0.18),
            "expansion_rate":        (24.0,  18.0,  1.0),
            "health_score":          (74.0,  68.0,  1.2),
            "logo_retention":        (91.0,  86.0,  0.6),
            "payback_period":        (12.0,  17.0,  0.8),
        },
        2024: {  # Warning signals — churn uptick, margin squeeze, burn rising
            "revenue_growth":        (3.5, -1.5,   0.8),
            "gross_margin":          (65.0, 61.5,  0.4),
            "operating_margin":      (6.0,  2.5,   0.6),
            "ebitda_margin":         (8.5,  4.0,   0.6),
            "cash_conv_cycle":       (47.0, 54.0,  1.2),
            "dso":                   (43.0, 50.0,  1.0),
            "arr_growth":            (3.0, -1.8,   0.8),
            "nrr":                   (106.0, 98.5, 0.7),
            "burn_multiple":         (3.0,  4.8,   0.25),
            "opex_ratio":            (44.0, 52.0,  1.0),
            "contribution_margin":   (59.0, 54.0,  0.6),
            "revenue_quality":       (81.0, 78.0,  0.4),
            "cac_payback":           (14.0, 19.0,  0.7),
            "sales_efficiency":      (0.32, 0.12,  0.02),
            "customer_concentration":(21.0, 24.0,  0.8),
            "recurring_revenue":     (81.0, 78.0,  0.4),
            "churn_rate":            (2.6,  4.8,   0.2),
            "operating_leverage":    (1.0,  0.3,   0.2),
            "pipeline_conversion":   (5.0,  2.8,  0.3),
            "customer_ltv":          (80.0, 52.0, 2.0),
            "pricing_power_index":   (0.5, -3.5,  0.5),
            "cpl":                   (175.0, 268.0, 10.0),
            "mql_sql_rate":          (22.0,  11.0,  0.8),
            "win_rate":              (24.0,  14.0,  0.8),
            "quota_attainment":      (72.0,  55.0,  1.5),
            "marketing_roi":         (2.6,   1.4,   0.12),
            "headcount_eff":         (1.1,   0.7,   0.04),
            "rev_per_employee":      (135.0, 98.0,  3.5),
            "ltv_cac":               (3.0,   1.8,   0.15),
            "expansion_rate":        (17.0,  10.0,  0.8),
            "health_score":          (66.0,  55.0,  1.5),
            "logo_retention":        (85.0,  78.0,  0.7),
            "payback_period":        (16.0,  24.0,  1.0),
        },
        2025: {  # Mixed recovery — volatile, fragile improvement
            "revenue_growth":        (1.8, 11.5,   1.0),
            "gross_margin":          (61.5, 64.2,  0.3),
            "operating_margin":      (5.1, 13.5,   0.8),
            "ebitda_margin":         (7.0, 16.0,   0.8),
            "cash_conv_cycle":       (50.0, 44.0,  1.0),
            "dso":                   (42.0, 44.0,  0.8),
            "arr_growth":            (1.5, 10.1,   1.0),
            "nrr":                   (98.5,107.5,  0.6),
            "burn_multiple":         (5.0,  0.7,   0.3),
            "opex_ratio":            (48.0, 36.0,  1.0),
            "contribution_margin":   (54.0, 61.0,  0.5),
            "revenue_quality":       (76.0, 83.0,  0.4),
            "cac_payback":           (18.0,  9.5,  0.8),
            "sales_efficiency":      (0.08, 0.65,  0.04),
            "customer_concentration":(26.0, 18.5,  0.8),
            "recurring_revenue":     (76.0, 83.0,  0.4),
            "churn_rate":            (3.2,  1.5,   0.15),
            "operating_leverage":    (1.5,  3.5,   0.3),
            "pipeline_conversion":   (3.0,  7.2,  0.35),
            "customer_ltv":          (55.0, 98.0, 2.5),
            "pricing_power_index":   (-2.0, 5.5,  0.5),
            "cpl":                   (260.0, 175.0, 12.0),
            "mql_sql_rate":          (13.0,  22.0,  1.0),
            "win_rate":              (15.0,  24.0,  1.0),
            "quota_attainment":      (58.0,  76.0,  2.0),
            "marketing_roi":         (1.5,   2.8,   0.15),
            "headcount_eff":         (0.75,  1.2,   0.05),
            "rev_per_employee":      (102.0, 148.0, 4.0),
            "ltv_cac":               (1.9,   3.2,   0.18),
            "expansion_rate":        (11.0,  18.0,  1.0),
            "health_score":          (56.0,  68.0,  1.5),
            "logo_retention":        (79.0,  87.0,  0.7),
            "payback_period":        (23.0,  16.0,  1.2),
        },
    }

    # 2026 actuals Jan–Mar: critical state
    PHASE_2026_ACTUAL = {  # (mo1_val, mo3_val, noise)
        "revenue_growth":        (0.5,  2.0,   0.5),
        "gross_margin":          (61.0, 61.5,  0.3),
        "operating_margin":      (4.0,  5.5,   0.5),
        "ebitda_margin":         (6.0,  7.5,   0.5),
        "cash_conv_cycle":       (50.0, 48.5,  0.8),
        "dso":                   (46.0, 44.5,  0.7),
        "arr_growth":            (0.3,  1.5,   0.5),
        "nrr":                   (97.0, 98.0,  0.5),
        "burn_multiple":         (3.8,  3.5,   0.2),
        "opex_ratio":            (50.0, 49.0,  0.8),
        "contribution_margin":   (52.0, 53.0,  0.4),
        "revenue_quality":       (75.0, 76.0,  0.3),
        "cac_payback":           (17.0, 16.5,  0.5),
        "sales_efficiency":      (0.12, 0.18,  0.02),
        "customer_concentration":(24.0, 23.5,  0.6),
        "recurring_revenue":     (75.0, 76.0,  0.3),
        "churn_rate":            (4.5,  4.2,   0.15),
        "operating_leverage":    (0.8,  1.0,   0.15),
        "pipeline_conversion":   (3.2,  3.8,  0.2),
        "customer_ltv":          (52.0, 55.0, 1.5),
        "pricing_power_index":   (-1.5, 1.5,  0.4),
        "cpl":                   (182.0, 195.0, 8.0),
        "mql_sql_rate":          (20.0,  21.5,  0.8),
        "win_rate":              (22.0,  23.5,  0.8),
        "quota_attainment":      (73.0,  75.0,  1.5),
        "marketing_roi":         (2.6,   2.7,   0.12),
        "headcount_eff":         (1.15,  1.18,  0.04),
        "rev_per_employee":      (142.0, 148.0, 3.0),
        "ltv_cac":               (3.0,   3.1,   0.12),
        "expansion_rate":        (16.5,  17.0,  0.8),
        "health_score":          (66.0,  67.5,  1.2),
        "logo_retention":        (86.0,  87.0,  0.5),
        "payback_period":        (17.5,  17.0,  0.8),
    }

    # 2021-2025 plan/budget projections (slightly more optimistic than actuals)
    # These represent "what management planned" for each year; seeded into projection_monthly_data
    # so Bridge Analysis can compare plan vs actual for 2021-2025
    PHASES_PROJ = {
        2021: {  # Startup plan — expected faster margin improvement
            "revenue_growth":        (16.0, 11.0,  1.0),
            "gross_margin":          (53.0, 59.0,  0.3),
            "operating_margin":      (-18.0, -3.0, 0.8),
            "ebitda_margin":         (-15.0, -1.0, 0.8),
            "cash_conv_cycle":       (63.0, 52.0,  1.2),
            "dso":                   (57.0, 46.0,  1.0),
            "arr_growth":            (14.0,  9.5,  1.2),
            "nrr":                   (87.0, 97.0,  0.6),
            "burn_multiple":         (4.2,  3.4,   0.15),
            "opex_ratio":            (83.0, 70.0,  1.2),
            "contribution_margin":   (32.0, 42.0,  0.6),
            "revenue_quality":       (57.0, 67.0,  0.4),
            "cac_payback":           (26.0, 19.0,  0.8),
            "sales_efficiency":      (0.13, 0.27,  0.015),
            "customer_concentration":(46.0, 39.0,  0.8),
            "recurring_revenue":     (57.0, 67.0,  0.4),
            "churn_rate":            (7.0,  4.8,   0.25),
            "operating_leverage":    (-0.5,  0.8,  0.15),
            "pipeline_conversion":   (3.0, 5.5, 0.25),
            "customer_ltv":          (35.0, 54.0, 1.2),
            "pricing_power_index":   (2.5, 6.0, 0.4),
            "cpl":                   (290.0, 210.0, 15.0),
            "mql_sql_rate":          (16.0,  23.0,  1.2),
            "win_rate":              (18.0,  25.0,  1.2),
            "quota_attainment":      (58.0,  72.0,  2.0),
            "marketing_roi":         (1.4,   2.1,   0.12),
            "headcount_eff":         (0.7,   1.05,  0.04),
            "rev_per_employee":      (75.0,  110.0, 3.5),
            "ltv_cac":               (1.4,   2.1,   0.10),
            "expansion_rate":        (5.0,   10.5,  0.7),
            "health_score":          (55.0,  66.0,  1.8),
            "logo_retention":        (76.0,  85.0,  0.9),
            "payback_period":        (34.0,  24.0,  1.3),
        },
        2022: {  # Series B plan — optimistic growth targets
            "revenue_growth":        (18.0, 15.0,  1.0),
            "gross_margin":          (59.0, 67.0,  0.3),
            "operating_margin":      (-5.0,  7.0,  0.6),
            "ebitda_margin":         (-3.0,  9.0,  0.6),
            "cash_conv_cycle":       (52.0, 39.0,  1.0),
            "dso":                   (47.0, 35.0,  0.8),
            "arr_growth":            (16.0, 13.0,  1.0),
            "nrr":                   (99.0,113.0,  0.5),
            "burn_multiple":         (3.4,  1.4,   0.15),
            "opex_ratio":            (68.0, 46.0,  1.2),
            "contribution_margin":   (44.0, 61.0,  0.6),
            "revenue_quality":       (68.0, 80.0,  0.4),
            "cac_payback":           (18.0,  9.0,  0.6),
            "sales_efficiency":      (0.26, 0.80,  0.025),
            "customer_concentration":(38.0, 24.0,  0.8),
            "recurring_revenue":     (68.0, 80.0,  0.4),
            "churn_rate":            (4.0,  1.8,   0.2),
            "operating_leverage":    (0.9,  3.2,   0.15),
            "pipeline_conversion":   (5.5, 9.5, 0.25),
            "customer_ltv":          (56.0, 98.0, 1.8),
            "pricing_power_index":   (5.0, 10.0, 0.5),
            "cpl":                   (198.0, 116.0, 10.0),
            "mql_sql_rate":          (25.0,  35.0,  1.0),
            "win_rate":              (27.0,  38.0,  1.0),
            "quota_attainment":      (76.0,  98.0,  1.8),
            "marketing_roi":         (2.5,   4.8,   0.18),
            "headcount_eff":         (1.15,  1.85,  0.04),
            "rev_per_employee":      (115.0, 190.0, 4.5),
            "ltv_cac":               (2.5,   5.5,   0.18),
            "expansion_rate":        (13.0,  30.0,  1.0),
            "health_score":          (68.0,  85.0,  1.3),
            "logo_retention":        (88.0,  96.0,  0.7),
            "payback_period":        (23.0,  10.0,  1.0),
        },
        2023: {  # Plateau plan — expected continued growth (too optimistic)
            "revenue_growth":        (8.0,  6.0,   0.5),
            "gross_margin":          (66.0, 68.0,  0.25),
            "operating_margin":      (6.0, 10.0,   0.4),
            "ebitda_margin":         (8.0, 13.0,   0.4),
            "cash_conv_cycle":       (41.0, 44.0,  0.8),
            "dso":                   (37.0, 41.0,  0.6),
            "arr_growth":            (7.0,  5.0,   0.5),
            "nrr":                   (111.0,109.0, 0.5),
            "burn_multiple":         (1.5,  2.6,   0.15),
            "opex_ratio":            (47.0, 40.0,  0.8),
            "contribution_margin":   (60.0, 64.0,  0.4),
            "revenue_quality":       (79.0, 85.0,  0.3),
            "cac_payback":           (10.0, 12.5,  0.5),
            "sales_efficiency":      (0.78, 0.40,  0.025),
            "customer_concentration":(24.0, 18.0,  0.6),
            "recurring_revenue":     (79.0, 85.0,  0.3),
            "churn_rate":            (1.8,  2.2,   0.12),
            "operating_leverage":    (2.9,  1.8,   0.15),
            "pipeline_conversion":   (9.0, 7.0, 0.25),
            "customer_ltv":          (92.0, 88.0, 1.2),
            "pricing_power_index":   (5.0, 3.0, 0.4),
            "cpl":                   (120.0, 150.0, 7.0),
            "mql_sql_rate":          (34.0,  28.0,  0.9),
            "win_rate":              (37.0,  30.0,  0.9),
            "quota_attainment":      (95.0,  83.0,  1.5),
            "marketing_roi":         (4.6,   3.2,   0.13),
            "headcount_eff":         (1.85,  1.28,  0.035),
            "rev_per_employee":      (185.0, 160.0, 3.5),
            "ltv_cac":               (5.2,   3.7,   0.16),
            "expansion_rate":        (27.0,  21.0,  0.9),
            "health_score":          (83.0,  77.0,  1.0),
            "logo_retention":        (95.0,  91.0,  0.5),
            "payback_period":        (10.5,  15.0,  0.7),
        },
        2024: {  # Warning year plan — expected mild improvement (missed badly)
            "revenue_growth":        (6.0,  3.0,   0.6),
            "gross_margin":          (66.5, 64.0,  0.3),
            "operating_margin":      (8.5,  6.0,   0.5),
            "ebitda_margin":         (11.0,  7.5,  0.5),
            "cash_conv_cycle":       (43.0, 49.0,  1.0),
            "dso":                   (39.0, 45.0,  0.8),
            "arr_growth":            (5.5,  1.5,   0.6),
            "nrr":                   (108.0,102.0, 0.6),
            "burn_multiple":         (2.5,  3.8,   0.2),
            "opex_ratio":            (40.0, 47.0,  0.8),
            "contribution_margin":   (62.0, 58.0,  0.5),
            "revenue_quality":       (83.5, 81.0,  0.3),
            "cac_payback":           (12.0, 16.0,  0.6),
            "sales_efficiency":      (0.38, 0.20,  0.015),
            "customer_concentration":(18.0, 20.0,  0.6),
            "recurring_revenue":     (83.5, 81.0,  0.3),
            "churn_rate":            (2.2,  3.8,   0.15),
            "operating_leverage":    (1.5,  0.8,   0.15),
            "pipeline_conversion":   (7.0, 5.0, 0.25),
            "customer_ltv":          (84.0, 62.0, 1.8),
            "pricing_power_index":   (2.5, 0.0, 0.4),
            "cpl":                   (155.0, 238.0, 9.0),
            "mql_sql_rate":          (26.0,  13.0,  0.7),
            "win_rate":              (28.0,  16.0,  0.7),
            "quota_attainment":      (83.0,  63.0,  1.3),
            "marketing_roi":         (3.0,   1.6,   0.10),
            "headcount_eff":         (1.26,  0.82,  0.035),
            "rev_per_employee":      (155.0, 113.0, 3.0),
            "ltv_cac":               (3.5,   2.1,   0.13),
            "expansion_rate":        (20.0,  12.0,  0.7),
            "health_score":          (76.0,  64.0,  1.3),
            "logo_retention":        (90.0,  84.0,  0.6),
            "payback_period":        (14.0,  21.0,  0.9),
        },
        2025: {  # Recovery plan — ambitious targets set after tough 2024
            "revenue_growth":        (5.0, 15.0,   0.8),
            "gross_margin":          (63.0, 66.5,  0.25),
            "operating_margin":      (8.0, 17.0,   0.6),
            "ebitda_margin":         (10.0, 19.5,  0.6),
            "cash_conv_cycle":       (46.0, 40.0,  0.8),
            "dso":                   (39.0, 40.0,  0.6),
            "arr_growth":            (4.5, 13.5,   0.8),
            "nrr":                   (101.0,111.0, 0.5),
            "burn_multiple":         (4.2,  0.4,   0.25),
            "opex_ratio":            (44.0, 31.0,  0.8),
            "contribution_margin":   (58.0, 65.0,  0.4),
            "revenue_quality":       (79.0, 86.5,  0.3),
            "cac_payback":           (15.0,  7.5,  0.6),
            "sales_efficiency":      (0.12, 0.75,  0.03),
            "customer_concentration":(22.0, 14.5,  0.6),
            "recurring_revenue":     (79.0, 86.5,  0.3),
            "churn_rate":            (2.8,  1.1,   0.12),
            "operating_leverage":    (2.0,  4.2,   0.25),
            "pipeline_conversion":   (5.0, 10.0, 0.3),
            "customer_ltv":          (62.0, 108.0, 2.2),
            "pricing_power_index":   (1.0, 7.0, 0.4),
            "cpl":                   (232.0, 155.0, 10.0),
            "mql_sql_rate":          (15.0,  25.0,  0.9),
            "win_rate":              (17.0,  27.0,  0.9),
            "quota_attainment":      (65.0,  85.0,  1.8),
            "marketing_roi":         (1.75,  3.2,   0.13),
            "headcount_eff":         (0.88,  1.4,   0.04),
            "rev_per_employee":      (118.0, 170.0, 3.5),
            "ltv_cac":               (2.2,   3.7,   0.16),
            "expansion_rate":        (13.0,  21.0,  0.9),
            "health_score":          (64.0,  78.0,  1.3),
            "logo_retention":        (85.0,  93.0,  0.6),
            "payback_period":        (26.0,  14.0,  1.0),
        },
    }

    # 2026 projection Apr–Dec: recovery scenario
    PHASE_2026_PROJ = {  # (apr_val, dec_val, noise)
        "revenue_growth":        (3.0,  7.5,   0.6),
        "gross_margin":          (62.0, 65.0,  0.3),
        "operating_margin":      (6.0, 11.0,   0.6),
        "ebitda_margin":         (8.0, 13.0,   0.6),
        "cash_conv_cycle":       (48.0, 41.0,  0.8),
        "dso":                   (44.0, 37.0,  0.7),
        "arr_growth":            (2.5,  7.0,   0.6),
        "nrr":                   (98.5,105.0,  0.5),
        "burn_multiple":         (3.4,  1.8,   0.2),
        "opex_ratio":            (48.0, 38.0,  0.8),
        "contribution_margin":   (53.5, 60.5,  0.5),
        "revenue_quality":       (76.5, 82.0,  0.3),
        "cac_payback":           (16.0, 11.0,  0.5),
        "sales_efficiency":      (0.20, 0.58,  0.03),
        "customer_concentration":(23.0, 19.0,  0.6),
        "recurring_revenue":     (76.5, 82.0,  0.3),
        "churn_rate":            (4.0,  2.5,   0.15),
        "operating_leverage":    (1.2,  2.8,   0.2),
        "pipeline_conversion":   (4.0,  7.5,  0.25),
        "customer_ltv":          (58.0, 82.0, 2.0),
        "pricing_power_index":   (1.0,  5.5,  0.4),
    }

    conn = get_db()
    # Clear existing data for this workspace only
    conn.execute("DELETE FROM monthly_data WHERE workspace_id=?", (workspace_id,))
    conn.execute("DELETE FROM uploads WHERE workspace_id=?", (workspace_id,))
    conn.execute("DELETE FROM projection_monthly_data WHERE workspace_id=?", (workspace_id,))
    conn.execute("DELETE FROM projection_uploads WHERE workspace_id=?", (workspace_id,))

    # Insert upload record for actuals
    cur = conn.execute(
        "INSERT INTO uploads (filename, uploaded_at, row_count, detected_columns, workspace_id) VALUES (?,?,?,?,?)",
        ("multiyear_demo_2021_2026.csv", datetime.utcnow().isoformat(), 63, json.dumps({}), workspace_id)
    )
    upload_id = cur.lastrowid

    total_months = 0

    # Seed 2021–2025 actuals (2-pass: base KPIs first, then derived)
    for yr, phase in PHASES.items():
        # Pass 1: compute all 12 months of base KPI values
        month_kpis = []
        for mo in range(1, 13):
            kpis = {kpi: mo_val(s, e, mo, n) for kpi, (s, e, n) in phase.items()}
            month_kpis.append(kpis)
        year_avg_rg = float(np.mean([m["revenue_growth"] for m in month_kpis]))
        # Pass 2: add derived KPIs and insert
        for i, (mo, kpis) in enumerate(zip(range(1, 13), month_kpis)):
            # growth_efficiency: ARR growth per unit of burn (higher = better capital efficiency)
            kpis["growth_efficiency"]   = round(kpis["arr_growth"] / max(kpis["burn_multiple"], 0.1), 4)
            # revenue_momentum: current growth vs annual average (>1 = accelerating)
            kpis["revenue_momentum"]    = round(kpis["revenue_growth"] / max(year_avg_rg, 0.1), 4)
            # revenue_fragility: concentration x churn risk divided by NRR resilience (lower = healthier)
            kpis["revenue_fragility"]   = round((kpis["customer_concentration"] * kpis["churn_rate"]) / max(kpis["nrr"], 1.0), 4)
            # burn_convexity: month-over-month change in burn (negative = improving)
            if i > 0:
                kpis["burn_convexity"]  = round(kpis["burn_multiple"] - month_kpis[i-1]["burn_multiple"], 4)
            else:
                # First month: use implied annual rate of change
                kpis["burn_convexity"]  = round((phase["burn_multiple"][1] - phase["burn_multiple"][0]) / 11.0, 4)
            # margin_volatility: rolling std dev of gross_margin (lower = more stable)
            if i >= 5:
                window = [month_kpis[j]["gross_margin"] for j in range(i - 5, i + 1)]
            else:
                window = [month_kpis[j]["gross_margin"] for j in range(0, i + 1)]
            kpis["margin_volatility"]   = round(float(np.std(window)) if len(window) > 1 else abs(rng(0, phase["gross_margin"][2])), 4)
            # customer_decay_slope: month-over-month change in churn rate (negative = improving)
            if i > 0:
                kpis["customer_decay_slope"] = round(kpis["churn_rate"] - month_kpis[i-1]["churn_rate"], 4)
            else:
                kpis["customer_decay_slope"] = round((phase["churn_rate"][1] - phase["churn_rate"][0]) / 11.0, 4)
            conn.execute(
                "INSERT INTO monthly_data (upload_id, year, month, data_json, workspace_id) VALUES (?,?,?,?,?)",
                (upload_id, yr, mo, json.dumps(kpis), workspace_id)
            )
            total_months += 1

    # Seed 2026 actuals Jan–Mar (2-pass for derived KPIs)
    act26_months = []
    for mo in range(1, 4):
        t = (mo - 1) / 2.0
        kpis = {kpi: round(lerp(start, end, t) + rng(0, noise), 4)
                for kpi, (start, end, noise) in PHASE_2026_ACTUAL.items()}
        act26_months.append(kpis)
    year_avg_rg_2026 = float(np.mean([m["revenue_growth"] for m in act26_months]))
    for i, (mo, kpis) in enumerate(zip(range(1, 4), act26_months)):
        kpis["growth_efficiency"]   = round(kpis["arr_growth"] / max(kpis["burn_multiple"], 0.1), 4)
        kpis["revenue_momentum"]    = round(kpis["revenue_growth"] / max(year_avg_rg_2026, 0.1), 4)
        kpis["revenue_fragility"]   = round((kpis["customer_concentration"] * kpis["churn_rate"]) / max(kpis["nrr"], 1.0), 4)
        if i > 0:
            kpis["burn_convexity"]  = round(kpis["burn_multiple"] - act26_months[i-1]["burn_multiple"], 4)
        else:
            kpis["burn_convexity"]  = round((PHASE_2026_ACTUAL["burn_multiple"][1] - PHASE_2026_ACTUAL["burn_multiple"][0]) / 2.0, 4)
        window = [act26_months[j]["gross_margin"] for j in range(0, i + 1)]
        kpis["margin_volatility"]   = round(float(np.std(window)) if len(window) > 1 else abs(rng(0, PHASE_2026_ACTUAL["gross_margin"][2])), 4)
        if i > 0:
            kpis["customer_decay_slope"] = round(kpis["churn_rate"] - act26_months[i-1]["churn_rate"], 4)
        else:
            kpis["customer_decay_slope"] = round((PHASE_2026_ACTUAL["churn_rate"][1] - PHASE_2026_ACTUAL["churn_rate"][0]) / 2.0, 4)
        conn.execute(
            "INSERT INTO monthly_data (upload_id, year, month, data_json, workspace_id) VALUES (?,?,?,?,?)",
            (upload_id, 2026, mo, json.dumps(kpis), workspace_id)
        )
        total_months += 1

    # Seed 2021-2025 plan/budget projections
    cur_plan = conn.execute(
        "INSERT INTO projection_uploads (filename, uploaded_at, row_count, detected_columns, workspace_id) VALUES (?,?,?,?,?)",
        ("plan_budget_2021_2025.csv", datetime.utcnow().isoformat(), 60, json.dumps({}), workspace_id)
    )
    plan_upload_id = cur_plan.lastrowid

    for yr, phase in PHASES_PROJ.items():
        proj_month_kpis = []
        for mo in range(1, 13):
            kpis = {kpi: mo_val(s, e, mo, n) for kpi, (s, e, n) in phase.items()}
            proj_month_kpis.append(kpis)
        proj_year_avg_rg = float(np.mean([m["revenue_growth"] for m in proj_month_kpis]))
        for i, (mo, kpis) in enumerate(zip(range(1, 13), proj_month_kpis)):
            kpis["growth_efficiency"]   = round(kpis["arr_growth"] / max(kpis["burn_multiple"], 0.1), 4)
            kpis["revenue_momentum"]    = round(kpis["revenue_growth"] / max(proj_year_avg_rg, 0.1), 4)
            kpis["revenue_fragility"]   = round((kpis["customer_concentration"] * kpis["churn_rate"]) / max(kpis["nrr"], 1.0), 4)
            if i > 0:
                kpis["burn_convexity"]  = round(kpis["burn_multiple"] - proj_month_kpis[i-1]["burn_multiple"], 4)
            else:
                kpis["burn_convexity"]  = round((phase["burn_multiple"][1] - phase["burn_multiple"][0]) / 11.0, 4)
            if i >= 5:
                window = [proj_month_kpis[j]["gross_margin"] for j in range(i - 5, i + 1)]
            else:
                window = [proj_month_kpis[j]["gross_margin"] for j in range(0, i + 1)]
            kpis["margin_volatility"]   = round(float(np.std(window)) if len(window) > 1 else abs(rng(0, phase["gross_margin"][2])), 4)
            if i > 0:
                kpis["customer_decay_slope"] = round(kpis["churn_rate"] - proj_month_kpis[i-1]["churn_rate"], 4)
            else:
                kpis["customer_decay_slope"] = round((phase["churn_rate"][1] - phase["churn_rate"][0]) / 11.0, 4)
            conn.execute(
                "INSERT INTO projection_monthly_data (projection_upload_id, year, month, data_json, workspace_id) VALUES (?,?,?,?,?)",
                (plan_upload_id, yr, mo, json.dumps(kpis), workspace_id)
            )

    # Seed 2026 projection Apr–Dec
    cur2 = conn.execute(
        "INSERT INTO projection_uploads (filename, uploaded_at, row_count, detected_columns, workspace_id) VALUES (?,?,?,?,?)",
        ("forecast_2026_recovery.csv", datetime.utcnow().isoformat(), 9, json.dumps({}), workspace_id)
    )
    proj_upload_id = cur2.lastrowid

    proj26_months = []
    for mo in range(4, 13):
        t = (mo - 4) / 8.0
        kpis = {kpi: round(lerp(start, end, t) + rng(0, noise), 4)
                for kpi, (start, end, noise) in PHASE_2026_PROJ.items()}
        proj26_months.append((mo, kpis))
    proj26_year_avg_rg = float(np.mean([k["revenue_growth"] for _, k in proj26_months]))
    for i, (mo, kpis) in enumerate(proj26_months):
        kpis["growth_efficiency"]   = round(kpis["arr_growth"] / max(kpis["burn_multiple"], 0.1), 4)
        kpis["revenue_momentum"]    = round(kpis["revenue_growth"] / max(proj26_year_avg_rg, 0.1), 4)
        kpis["revenue_fragility"]   = round((kpis["customer_concentration"] * kpis["churn_rate"]) / max(kpis["nrr"], 1.0), 4)
        if i > 0:
            kpis["burn_convexity"]  = round(kpis["burn_multiple"] - proj26_months[i-1][1]["burn_multiple"], 4)
        else:
            kpis["burn_convexity"]  = round((PHASE_2026_PROJ["burn_multiple"][1] - PHASE_2026_PROJ["burn_multiple"][0]) / 8.0, 4)
        if i >= 5:
            window = [proj26_months[j][1]["gross_margin"] for j in range(i - 5, i + 1)]
        else:
            window = [proj26_months[j][1]["gross_margin"] for j in range(0, i + 1)]
        kpis["margin_volatility"]   = round(float(np.std(window)) if len(window) > 1 else abs(rng(0, PHASE_2026_PROJ["gross_margin"][2])), 4)
        if i > 0:
            kpis["customer_decay_slope"] = round(kpis["churn_rate"] - proj26_months[i-1][1]["churn_rate"], 4)
        else:
            kpis["customer_decay_slope"] = round((PHASE_2026_PROJ["churn_rate"][1] - PHASE_2026_PROJ["churn_rate"][0]) / 8.0, 4)
        conn.execute(
            "INSERT INTO projection_monthly_data (projection_upload_id, year, month, data_json, workspace_id) VALUES (?,?,?,?,?)",
            (proj_upload_id, 2026, mo, json.dumps(kpis), workspace_id)
        )

    conn.commit()
    conn.close()
    return {
        "seeded": True,
        "years":  "2021–2026",
        "actual_months": total_months,
        "projection_months": 60 + 9,
        "narrative": [
            "2021: Startup phase — high churn, negative margins, rapid growth from small base",
            "2022: Series B scaling — rapid hiring, margins climbing, peak growth",
            "2023: Growth plateau — revenue stalls, over-hired, efficiency declining",
            "2024: Warning signals — churn uptick, margin compression, burn rising",
            "2025: Mixed recovery — volatile, fragile improvement",
            "2026 Jan–Mar: Critical state actuals",
            "2026 Apr–Dec: Recovery forecast",
            "2021–2025: Plan/budget projections added for Bridge Analysis",
        ]
    }

