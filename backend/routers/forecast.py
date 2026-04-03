"""
routers/forecast.py — Markov / Monte-Carlo KPI forecast endpoints.
"""
import json
import threading
from datetime import datetime
from typing import Optional

from core.queue import enqueue as _enqueue

import numpy as np
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel as _BaseModel

from core.database import get_db
from core.deps import _get_workspace
from core.state import _FORECAST_BUILDING, _FORECAST_ERROR, _FORECAST_LOCK

router = APIRouter()

# ── Per-workspace forecast build status (in-memory) ──────────────────────────
# _FORECAST_BUILDING and _FORECAST_ERROR in core/state.py are typed as single
# values for backward compat; we shadow them locally as dicts so we can track
# per-workspace state without modifying core.state.
_BUILDING: dict = {}   # workspace_id -> bool
_ERROR:    dict = {}   # workspace_id -> str
_LOCK      = threading.Lock()


def _init_forecast_tables():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS forecast_build_status (
            workspace_id TEXT PRIMARY KEY,
            status TEXT DEFAULT 'not_trained',
            message TEXT DEFAULT '',
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS markov_models (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kpis TEXT,
            thresholds TEXT,
            self_matrices TEXT,
            cross_matrices TEXT,
            current_states TEXT,
            upstream_kpis TEXT,
            days_back INTEGER DEFAULT 365,
            trained_at TEXT,
            regime_data TEXT DEFAULT NULL,
            workspace_id TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS forecast_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_id INTEGER,
            horizon_days INTEGER,
            overrides TEXT,
            n_samples INTEGER,
            trajectories TEXT,
            causal_paths TEXT,
            created_at TEXT
        );
    """)
    conn.commit()
    for migration in [
        "ALTER TABLE markov_models ADD COLUMN regime_data TEXT DEFAULT NULL",
        "ALTER TABLE markov_models ADD COLUMN workspace_id TEXT DEFAULT ''",
    ]:
        try:
            conn.execute(migration)
            conn.commit()
        except Exception:
            pass

    # Fix for PostgreSQL: ensure markov_models.id has a sequence for auto-increment.
    # The original DDL used INTEGER PRIMARY KEY (no AUTOINCREMENT) which works in SQLite
    # (rowid alias) but fails in PostgreSQL because INTEGER PRIMARY KEY has no default.
    _pg_migrations = [
        "CREATE SEQUENCE IF NOT EXISTS markov_models_id_seq OWNED BY markov_models.id",
        "ALTER TABLE markov_models ALTER COLUMN id SET DEFAULT nextval('markov_models_id_seq')",
        "SELECT setval('markov_models_id_seq', COALESCE((SELECT MAX(id) FROM markov_models), 0) + 1, false)",
    ]
    for pg_mig in _pg_migrations:
        try:
            conn.execute(pg_mig)
            conn.commit()
        except Exception:
            # SQLite doesn't support sequences — expected to fail; also safe if already applied
            try:
                conn.commit()
            except Exception:
                pass

    conn.close()


_init_forecast_tables()


def _mrk_monthly_history(workspace_id: str = ""):
    conn = get_db()
    if workspace_id:
        rows = conn.execute(
            "SELECT year, month, data_json FROM monthly_data "
            "WHERE workspace_id=? ORDER BY year ASC, month ASC",
            [workspace_id],
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT year, month, data_json FROM monthly_data ORDER BY year ASC, month ASC"
        ).fetchall()
    conn.close()
    result = {}
    for r in rows:
        try:
            d = json.loads(r["data_json"])
        except Exception:
            continue
        for k, v in d.items():
            if v is not None:
                result.setdefault(k, []).append(float(v))
    return result


def _mrk_causal_pairs():
    """Return 5-tuples: (source, target, strength, direction, relation)."""
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT source, target, strength, COALESCE(direction, 'positive') AS direction, relation "
            "FROM ontology_edges "
            "WHERE relation IN ('CAUSES','INFLUENCES','CORRELATES_WITH','LEADS','ANTI_CORRELATES') "
            "ORDER BY strength DESC"
        ).fetchall()
        conn.close()
        return [(r["source"], r["target"], float(r["strength"]), r["direction"], r["relation"])
                for r in rows]
    except Exception:
        return []


def _mrk_monthly_history_dated(workspace_id: str = ""):
    """Returns dict {(year, month): {kpi: float}} sorted chronologically."""
    conn = get_db()
    if workspace_id:
        rows = conn.execute(
            "SELECT year, month, data_json FROM monthly_data "
            "WHERE workspace_id=? ORDER BY year ASC, month ASC",
            [workspace_id],
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT year, month, data_json FROM monthly_data ORDER BY year ASC, month ASC"
        ).fetchall()
    conn.close()
    result = {}
    for r in rows:
        try:
            d = json.loads(r["data_json"])
        except Exception:
            continue
        result[(int(r["year"]), int(r["month"]))] = {
            k: float(v) for k, v in d.items() if v is not None
        }
    return result


def _detect_regimes(dated_history, kpis):
    try:
        from scipy.stats import wasserstein_distance as _wdist
        from sklearn.cluster import KMeans
        from sklearn.manifold import MDS
        from sklearn.metrics import silhouette_score as _silhouette
        from sklearn.preprocessing import StandardScaler
    except ImportError:
        return None

    months = sorted(dated_history.keys())
    n_m = len(months)
    if n_m < 12:
        return None

    mat = np.array(
        [[dated_history[ym].get(k, np.nan) for k in kpis] for ym in months],
        dtype=float,
    )
    col_means = np.nanmean(mat, axis=0)
    for j in range(mat.shape[1]):
        mat[np.isnan(mat[:, j]), j] = col_means[j] if not np.isnan(col_means[j]) else 0.0

    valid_mask = ~np.all(mat == 0, axis=1)
    mat = mat[valid_mask]
    valid_months = [months[i] for i, v in enumerate(valid_mask) if v]
    n_m = len(valid_months)
    if n_m < 12:
        return None

    scaler = StandardScaler()
    mat_std = scaler.fit_transform(mat)

    dist_matrix = np.zeros((n_m, n_m))
    for i in range(n_m):
        for j in range(i + 1, n_m):
            d = _wdist(mat_std[i], mat_std[j])
            dist_matrix[i, j] = d
            dist_matrix[j, i] = d

    try:
        mds = MDS(n_components=2, dissimilarity="precomputed",
                  random_state=42, normalized_stress="auto")
        embedding = mds.fit_transform(dist_matrix)
    except TypeError:
        mds = MDS(n_components=2, dissimilarity="precomputed", random_state=42)
        embedding = mds.fit_transform(dist_matrix)

    # ── Select optimal K via silhouette score ──────────────────────────
    # Test K=2..4 (capped by available months) and pick the K with the
    # highest mean silhouette score.  Silhouette measures how well each
    # point fits its own cluster vs. its nearest neighbour cluster;
    # scores range from -1 (wrong cluster) to +1 (dense, well-separated).
    max_k = min(4, n_m - 1)          # need at least K+1 data points
    best_k, best_sil = 2, -1.0
    for k_cand in range(2, max_k + 1):
        km_cand = KMeans(n_clusters=k_cand, random_state=42, n_init=10)
        labels_cand = km_cand.fit_predict(embedding)
        sil = _silhouette(embedding, labels_cand)
        if sil > best_sil:
            best_sil, best_k = sil, k_cand
    n_clusters = best_k
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    raw_labels = kmeans.fit_predict(embedding)

    growth_kpis = [k for k in ("revenue_growth", "arr_growth", "nrr", "growth_efficiency") if k in kpis]
    stress_kpis = [k for k in ("churn_rate", "burn_multiple", "cac_payback", "burn_convexity") if k in kpis]

    regime_scores = {}
    for r in range(n_clusters):
        mask = raw_labels == r
        cluster_mat = mat_std[mask]
        g = sum(float(np.mean(cluster_mat[:, kpis.index(k)])) for k in growth_kpis) if growth_kpis else 0.0
        s = sum(float(np.mean(cluster_mat[:, kpis.index(k)])) for k in stress_kpis) if stress_kpis else 0.0
        regime_scores[r] = g - s

    sorted_r = sorted(regime_scores, key=lambda r: regime_scores[r])
    _REGIME_LABELS = ["Stress", "Recovery", "Stable", "Growth"]
    if n_clusters == 2:
        regime_name_map = {sorted_r[0]: "Stress", sorted_r[1]: "Growth"}
    elif n_clusters == 3:
        regime_name_map = {sorted_r[0]: "Stress", sorted_r[1]: "Recovery", sorted_r[2]: "Growth"}
    else:
        regime_name_map = {sorted_r[i]: _REGIME_LABELS[i] for i in range(n_clusters)}

    regime_labels = {valid_months[i]: int(raw_labels[i]) for i in range(n_m)}

    regime_deltas = {r: {k: [] for k in kpis} for r in range(n_clusters)}
    for i in range(1, n_m):
        prev_ym = valid_months[i - 1]
        curr_ym = valid_months[i]
        r = regime_labels[prev_ym]
        for k in kpis:
            pv = dated_history.get(prev_ym, {}).get(k)
            cv = dated_history.get(curr_ym, {}).get(k)
            if pv is not None and cv is not None:
                regime_deltas[r][k].append(float(cv) - float(pv))

    curr_ym = valid_months[-1]
    curr_label = regime_labels[curr_ym]
    months_in = 1
    for i in range(n_m - 2, -1, -1):
        if regime_labels[valid_months[i]] == curr_label:
            months_in += 1
        else:
            break

    return {
        "regime_labels":  {f"{ym[0]}-{ym[1]:02d}": lbl for ym, lbl in regime_labels.items()},
        "regime_names":   {str(k): v for k, v in regime_name_map.items()},
        "regime_deltas":  {str(r): d for r, d in regime_deltas.items()},
        "current_regime": {"label": curr_label, "name": regime_name_map[curr_label], "months_in": months_in},
        "n_clusters":     n_clusters,
    }


_CAUSAL_RELATION_PRIORITY = {
    'CAUSES': 3, 'INFLUENCES': 2, 'LEADS': 2,
    'CORRELATES_WITH': 1, 'ANTI_CORRELATES': 1,
}


def _build_deduped_causal_map(causal_pairs, kpis):
    best = {}
    for src, tgt, strength, direction, relation in causal_pairs:
        if src in kpis and tgt in kpis:
            key      = (tgt, src)
            priority = _CAUSAL_RELATION_PRIORITY.get(relation, 0)
            if key not in best or priority > best[key][2]:
                best[key] = (strength, direction, priority)
    causal_map = {}
    for (tgt, src), (strength, direction, _) in best.items():
        causal_map.setdefault(tgt, []).append((src, strength, direction))
    return causal_map


def _set_build_status(workspace_id: str, status: str, message: str = ""):
    """Persist build status to DB so any worker can read it."""
    try:
        c = get_db()
        c.execute(
            """INSERT INTO forecast_build_status (workspace_id, status, message, updated_at)
               VALUES (?,?,?,?)
               ON CONFLICT(workspace_id) DO UPDATE
               SET status=excluded.status, message=excluded.message, updated_at=excluded.updated_at""",
            (workspace_id, status, message, datetime.utcnow().isoformat())
        )
        c.commit()
        c.close()
    except Exception as e:
        print(f"[Forecast] _set_build_status failed: {e}")


def _get_build_status(workspace_id: str):
    """Read build status from DB (cross-worker safe).
    Returns (status, message). If status has been 'building' for >10 min,
    automatically resets to 'error' so users can retry."""
    try:
        c = get_db()
        row = c.execute(
            "SELECT status, message, updated_at FROM forecast_build_status WHERE workspace_id=?",
            [workspace_id]
        ).fetchone()
        c.close()
        if row:
            status, message = row["status"], row["message"]
            # Stale check: if building for more than 10 minutes, reset
            if status == "building" and row["updated_at"]:
                try:
                    started = datetime.fromisoformat(row["updated_at"])
                    if (datetime.utcnow() - started).total_seconds() > 600:
                        _set_build_status(workspace_id, "error",
                                          "Training timed out (exceeded 10 minutes). Please retry.")
                        return "error", "Training timed out (exceeded 10 minutes). Please retry."
                except Exception:
                    pass
            return status, message
    except Exception:
        pass
    return "not_trained", ""


def _build_markov_task(workspace_id: str = ""):
    history       = _mrk_monthly_history(workspace_id)
    dated_history = _mrk_monthly_history_dated(workspace_id)
    if len(history) < 2:
        msg = (
            "Not enough monthly data to train the forecast model. "
            "Upload at least 2 months of KPI data first."
        )
        print(f"[Forecast] {msg} (workspace={workspace_id!r})")
        with _LOCK:
            _ERROR[workspace_id] = msg
        _set_build_status(workspace_id, "error", msg)
        return

    current_values = {}
    value_ranges   = {}
    mean_deltas    = {}
    std_deltas     = {}

    for kpi, values in history.items():
        if len(values) < 2:
            continue
        arr    = np.array(values, dtype=float)
        # Filter out NaN/inf values before computing deltas
        arr    = arr[np.isfinite(arr)]
        if len(arr) < 2:
            continue
        deltas = np.diff(arr).tolist()
        # Filter out NaN deltas as well
        deltas = [d for d in deltas if np.isfinite(d)]
        if not deltas:
            continue

        current_values[kpi] = float(arr[-1])
        mean_deltas[kpi]    = float(np.mean(deltas))
        std_deltas[kpi]     = float(np.std(deltas)) if len(deltas) > 1 \
                              else max(abs(float(np.mean(deltas))) * 0.3, 0.01)
        value_ranges[kpi] = {
            "min":     float(np.min(arr)),
            "max":     float(np.max(arr)),
            "p10":     float(np.percentile(arr, 10)),
            "p25":     float(np.percentile(arr, 25)),
            "p50":     float(np.percentile(arr, 50)),
            "p75":     float(np.percentile(arr, 75)),
            "p90":     float(np.percentile(arr, 90)),
            "current": float(arr[-1]),
            "deltas":  deltas,
        }

    kpis          = list(current_values.keys())
    causal_pairs  = _mrk_causal_pairs()
    upstream_kpis = list({src for src, _, _, _, _ in causal_pairs if src in kpis})

    regime_data = None
    try:
        regime_data = _detect_regimes(dated_history, kpis)
    except Exception:
        import traceback; traceback.print_exc()

    now = datetime.utcnow().isoformat()
    conn = get_db()
    try:
        conn.execute("DELETE FROM markov_models WHERE workspace_id=?", [workspace_id])
        conn.execute(
            "INSERT INTO markov_models (kpis, thresholds, self_matrices, cross_matrices, "
            "current_states, upstream_kpis, days_back, trained_at, regime_data, workspace_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (json.dumps(kpis),
             json.dumps(value_ranges),
             json.dumps(mean_deltas),
             json.dumps(std_deltas),
             json.dumps(current_values),
             json.dumps(upstream_kpis),
             365, now,
             json.dumps(regime_data) if regime_data else None,
             workspace_id)
        )
        conn.commit()
        print(f"[Forecast] Model trained: {len(kpis)} KPIs (workspace={workspace_id!r})")
        _set_build_status(workspace_id, "ready", f"Trained {len(kpis)} KPIs")
    except Exception as _db_err:
        print(f"[Forecast][ERROR] DB write failed for workspace={workspace_id!r}: {_db_err}")
        import traceback as _tb2; _tb2.print_exc()
        _set_build_status(workspace_id, "error", str(_db_err))
        raise
    finally:
        conn.close()


def _project_scenario(horizon_days: int, overrides: dict, n_samples: int,
                      workspace_id: str = ""):
    conn = get_db()
    row  = conn.execute(
        "SELECT * FROM markov_models WHERE workspace_id=? ORDER BY id DESC LIMIT 1",
        [workspace_id],
    ).fetchone()
    conn.close()
    if not row:
        return {"status": "no_model", "message": "No trained model found for this workspace. Please train the forecast model first."}

    kpis         = json.loads(row["kpis"])
    value_ranges = json.loads(row["thresholds"])
    mean_deltas  = json.loads(row["self_matrices"])
    std_deltas   = json.loads(row["cross_matrices"])
    cur_values   = json.loads(row["current_states"])

    regime_data    = json.loads(row["regime_data"]) if row["regime_data"] else None
    current_regime = regime_data["current_regime"] if regime_data else None

    _MIN_REGIME_POOL = 6
    regime_delta_pool = {}
    if regime_data:
        curr_label     = str(current_regime["label"])
        regime_deltas  = regime_data.get("regime_deltas", {})
        r_pool         = regime_deltas.get(curr_label, {})
        for kpi in kpis:
            cond_pool = r_pool.get(kpi, [])
            full_pool = value_ranges.get(kpi, {}).get("deltas", [])
            regime_delta_pool[kpi] = cond_pool if len(cond_pool) >= _MIN_REGIME_POOL else full_pool
    else:
        for kpi in kpis:
            regime_delta_pool[kpi] = value_ranges.get(kpi, {}).get("deltas", [])

    causal_pairs = _mrk_causal_pairs()
    causal_map   = _build_deduped_causal_map(causal_pairs, set(kpis))

    horizon_steps = max(1, round(horizon_days / 30))

    _state_pcts   = ["p10", "p25", "p50", "p75", "p90"]
    override_vals = {}
    for kpi, state_idx in overrides.items():
        if kpi in value_ranges:
            override_vals[kpi] = float(value_ranges[kpi][_state_pcts[min(int(state_idx), 4)]])

    # ── Override bias ────────────────────────────────────────────────────
    # Strength = 0.45: steers ~80 % of the way toward the override over a
    # 3-step (90-day) horizon.  The prior 0.30 left a ~70 % gap, meaning
    # user overrides were largely ignored.  0.45 balances responsiveness
    # with historical anchoring — the simulation still reflects regime
    # dynamics rather than collapsing to a single point.
    _OVERRIDE_STRENGTH = 0.45

    override_delta_bias = {}
    for kpi, override_val in override_vals.items():
        vr       = value_ranges.get(kpi, {})
        hist_p50 = vr.get("p50", override_val)
        sigma    = std_deltas.get(kpi, 0.01) or 0.01
        z        = (override_val - hist_p50) / (sigma * 3)
        override_delta_bias[kpi] = z * sigma * _OVERRIDE_STRENGTH

    # ── Bounds for projection clipping ────────────────────────────────
    # Each KPI is clipped to [hard_floor, hard_ceil] after every step.
    # Floors/ceilings are derived from the historical range (p1 / p99)
    # expanded by 50 % to allow reasonable extrapolation while preventing
    # impossible values (e.g. negative revenue, >100 % margins).
    kpi_bounds = {}
    for kpi in kpis:
        vr        = value_ranges.get(kpi, {})
        hist_lo   = vr.get("p10", 0)
        hist_hi   = vr.get("p90", 0)
        hist_span = abs(hist_hi - hist_lo) or abs(hist_hi) * 0.5 or 1.0
        hard_floor = hist_lo - hist_span * 1.5
        hard_ceil  = hist_hi + hist_span * 1.5
        # Percentage KPIs can never exceed 100 % or drop below 0 %
        if kpi.endswith("_pct") or kpi.endswith("_margin") or kpi in (
            "gross_margin", "operating_margin", "ebitda_margin",
            "contribution_margin", "nrr", "churn_rate",
        ):
            hard_floor = max(hard_floor, 0.0)
            hard_ceil  = min(hard_ceil, 200.0)   # NRR can exceed 100 %
        kpi_bounds[kpi] = (hard_floor, hard_ceil)

    # ── Build KDE samplers for historical deltas ─────────────────────
    # Kernel Density Estimation produces smoother, more realistic samples
    # than raw np.random.choice.  Choice re-draws the exact same deltas
    # with uniform probability, collapsing the tails.  KDE fits a smooth
    # density and samples from it, naturally exploring inter-observation
    # regions and tail behaviour proportional to historical density.
    from scipy.stats import gaussian_kde as _kde

    kde_samplers = {}
    for kpi in kpis:
        pool = regime_delta_pool.get(kpi, [])
        if len(pool) >= 6:
            try:
                kde_samplers[kpi] = _kde(pool, bw_method="silverman")
            except Exception:
                pass  # fall back to normal sampling below

    all_traj = {kpi: [] for kpi in kpis}

    for _ in range(n_samples):
        values = {k: override_vals.get(k, cur_values.get(k, 0.0)) for k in kpis}
        path   = {k: [values[k]] for k in kpis}

        for _ in range(horizon_steps):
            self_deltas = {}
            for kpi in kpis:
                sigma = std_deltas.get(kpi, 0.01)
                if kpi in kde_samplers:
                    # KDE sample: draw from the fitted kernel density of
                    # historical month-over-month deltas.
                    d = float(kde_samplers[kpi].resample(1)[0, 0])
                elif regime_delta_pool.get(kpi):
                    # Fallback: bootstrap from raw pool (< 6 points for KDE)
                    d = float(np.random.choice(regime_delta_pool[kpi]))
                    d += float(np.random.normal(0, sigma * 0.15))
                else:
                    d = float(np.random.normal(mean_deltas.get(kpi, 0.0), sigma))
                d += override_delta_bias.get(kpi, 0.0)
                self_deltas[kpi] = d

            new_values = {}
            for kpi in kpis:
                delta     = self_deltas[kpi]
                tgt_sigma = std_deltas.get(kpi, 1.0) or 1.0

                if kpi in causal_map:
                    causal_delta = 0.0
                    total_w      = 0.0
                    for src, strength, direction in causal_map[kpi]:
                        src_sigma = std_deltas.get(src, 1.0) or 1.0
                        src_vr    = value_ranges.get(src, {})
                        dir_sign  = 1 if direction == 'positive' else -1

                        # Delta term: how much the source KPI moved this
                        # step, scaled to the target's units and weighted
                        # by causal strength.
                        scale      = tgt_sigma / src_sigma
                        delta_term = self_deltas[src] * scale * strength * dir_sign

                        # Level term: persistent pull proportional to how
                        # far the source sits from its historical median.
                        # The 0.25 multiplier caps the level contribution
                        # at ¼ of the delta-term magnitude to prevent
                        # mean-reverting sources from dominating.
                        src_p50    = src_vr.get("p50", values[src])
                        level_z    = (values[src] - src_p50) / (src_sigma * 3 or 1)
                        level_term = level_z * tgt_sigma * strength * dir_sign * 0.25

                        causal_delta += (delta_term + level_term)
                        total_w      += strength

                    if total_w > 0:
                        causal_delta /= total_w
                        # Self/causal blend: 60 % self + 40 % causal.
                        # Each KPI's own regime dynamics are the primary
                        # driver (60 %).  Cross-KPI causal influence adds
                        # 40 % to propagate shocks through the DAG without
                        # letting indirect signals overpower direct history.
                        delta = 0.6 * delta + 0.4 * causal_delta

                raw_val = values[kpi] + delta
                # Clip to historical bounds to prevent impossible values
                lo, hi = kpi_bounds.get(kpi, (-1e12, 1e12))
                new_values[kpi] = max(lo, min(hi, raw_val))

            values = new_values
            for kpi in kpis:
                path[kpi].append(values[kpi])

        for kpi in kpis:
            all_traj[kpi].append(path[kpi])

    # ── Compute trajectory percentiles ────────────────────────────────
    # Returns p10/p25/p50/p75/p90 for each step.  p25 & p75 form the
    # interquartile range (IQR) — the band where 50 % of simulations
    # land — giving users a tighter "likely" band inside the wider
    # p10–p90 confidence envelope.
    trajectories = {}
    for kpi in kpis:
        arr  = np.array(all_traj[kpi])
        vr   = value_ranges.get(kpi, {})
        traj = []
        for step in range(arr.shape[1]):
            col = arr[:, step]
            traj.append({
                "step":     step,
                "p10":      float(np.percentile(col, 10)),
                "p25":      float(np.percentile(col, 25)),
                "p50":      float(np.percentile(col, 50)),
                "p75":      float(np.percentile(col, 75)),
                "p90":      float(np.percentile(col, 90)),
                "label":    "Now" if step == 0 else f"M+{step}",
                "hist_p10": float(vr.get("p10", 0)),
                "hist_p50": float(vr.get("p50", 0)),
                "hist_p90": float(vr.get("p90", 0)),
            })
        trajectories[kpi] = traj

    causal_paths_out = {}
    for kpi in kpis:
        if kpi in causal_map:
            causal_paths_out[kpi] = [
                {"from": src, "strength": round(float(s), 3), "direction": drn}
                for src, s, drn in sorted(causal_map[kpi], key=lambda x: -x[1])[:3]
            ]

    now = datetime.utcnow().isoformat()
    conn = get_db()
    run_id = conn.execute(
        "INSERT INTO forecast_runs (model_id, horizon_days, overrides, n_samples, "
        "trajectories, causal_paths, created_at) VALUES (?,?,?,?,?,?,?)",
        (row["id"], horizon_days, json.dumps(overrides), n_samples,
         json.dumps(trajectories), json.dumps(causal_paths_out), now)
    ).lastrowid
    conn.commit()
    conn.close()

    return {
        "status":           "ok",
        "run_id":           run_id,
        "horizon_days":     horizon_days,
        "n_samples":        n_samples,
        "kpis":             kpis,
        "overrides":        overrides,
        "trajectories":     trajectories,
        "causal_paths":     causal_paths_out,
        "model_trained_at": row["trained_at"],
        "value_ranges":     value_ranges,
        "current_regime":   current_regime,
        "regime_data":      {
            "name":      current_regime["name"]      if current_regime else None,
            "months_in": current_regime["months_in"] if current_regime else None,
            "label":     current_regime["label"]     if current_regime else None,
            "n_regimes": regime_data["n_clusters"]   if regime_data else None,
            "available": regime_data is not None,
        },
    }


class _ProjectRequest(_BaseModel):
    horizon_days: int = 90
    n_samples:    int = 400
    overrides:    dict = {}

    def __init__(self, **data):
        super().__init__(**data)
        # Hard caps to prevent OOM / runaway Monte-Carlo loops
        if self.horizon_days > 730:
            self.horizon_days = 730   # max 2 years
        if self.n_samples > 2000:
            self.n_samples = 2000


@router.post("/api/forecast/build")
def forecast_build(request: Request):
    workspace_id = _get_workspace(request)
    with _LOCK:
        if _BUILDING.get(workspace_id):
            return {"status": "building", "message": "Training already in progress"}
        _BUILDING[workspace_id] = True
        _ERROR.pop(workspace_id, None)
    # Write "building" to DB so any worker sees the correct status
    _set_build_status(workspace_id, "building", "Training started")

    def _bg():
        try:
            _build_markov_task(workspace_id)
        except Exception as exc:
            import traceback as _tb
            _tb.print_exc()
            msg = f"Training failed: {exc}"
            with _LOCK:
                _ERROR[workspace_id] = msg
            _set_build_status(workspace_id, "error", msg)
        finally:
            with _LOCK:
                _BUILDING[workspace_id] = False

    job = _enqueue(_bg, job_timeout=300)
    job_id = job.id if job else None
    return {"status": "building", "message": "Markov model training started", "job_id": job_id}


@router.post("/api/forecast/project")
def forecast_project(request: Request, req: _ProjectRequest):
    workspace_id = _get_workspace(request)
    result = _project_scenario(req.horizon_days, req.overrides, req.n_samples, workspace_id)
    return result


@router.get("/api/forecast/model")
def forecast_model(request: Request):
    workspace_id = _get_workspace(request)

    # Check DB-persisted status first (cross-worker safe)
    db_status, db_msg = _get_build_status(workspace_id)

    # In-memory check as a fast override (valid on same worker)
    with _LOCK:
        mem_building = _BUILDING.get(workspace_id, False)
        mem_err      = _ERROR.get(workspace_id)

    if mem_building or db_status == "building":
        return {"status": "building", "message": "Training in progress…"}
    if mem_err:
        return {"status": "error", "message": mem_err}
    if db_status == "error":
        return {"status": "error", "message": db_msg}

    conn = get_db()
    row = conn.execute(
        "SELECT * FROM markov_models WHERE workspace_id=? ORDER BY id DESC LIMIT 1",
        [workspace_id],
    ).fetchone()
    conn.close()
    if not row:
        return {"status": "not_trained"}
    return {
        "status":         "ready",
        "id":             row["id"],
        "kpis":           json.loads(row["kpis"]),
        "upstream_kpis":  json.loads(row["upstream_kpis"]),
        "current_values": json.loads(row["current_states"]),
        "value_ranges":   json.loads(row["thresholds"]),
        "trained_at":     row["trained_at"],
        "days_back":      row["days_back"],
        "regime_data":    json.loads(row["regime_data"]) if row["regime_data"] else None,
    }
