"""
routers/board_pack.py — Intelligent Board Pack Generator.

POST /api/board-pack/generate — returns PPTX file with:
  - Flexible date range (1 month to multi-year)
  - Four pack modes: talk_track, just_kpis, variance_narrative, financial_projections
  - Knowledge-graph-based causal narratives (not templates)
  - Intelligently selected charts (heatmap, radar, waterfall, line, donut, bar)
  - Single Corporate Blue theme

Uses:
  - core/chart_engine.py for chart rendering
  - core/board_narrative.py for signal detection + narrative generation
  - core/narrative_engine.py for per-KPI root cause analysis
  - core/health_score.py for health score computation
  - core/criticality.py for composite criticality ranking
"""
import io
import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from core.database import get_db
from core.deps import _get_workspace

router = APIRouter()


# ── Request Schema ──────────────────────────────────────────────────────────

class BoardPackRequest(BaseModel):
    company_name: Optional[str] = None
    from_year: int
    from_month: int            # 1-12
    to_year: int
    to_month: int              # 1-12
    modes: list[str]           # ["talk_track", "just_kpis", "variance_narrative", "financial_projections"]
    kpi_keys: Optional[list] = None
    company_stage: str = "series_b"


VALID_MODES = {"talk_track", "just_kpis", "variance_narrative", "financial_projections"}


# ── Corporate Blue Theme (single theme) ─────────────────────────────────────

THEME = {
    "bg":        "FFFFFF",
    "accent":    "003087",
    "highlight": "0055A4",
    "text":      "0F172A",
    "subtext":   "64748B",
    "positive":  "059669",
    "warning":   "D97706",
    "critical":  "DC2626",
    "card_bg":   "F1F5F9",
    "title_bg":  "071E45",
}


# ── Endpoint ────────────────────────────────────────────────────────────────

@router.post("/api/board-pack/generate", tags=["Board Pack"])
async def generate_board_pack(request: Request, body: BoardPackRequest):
    """Generate an intelligent PPTX board pack with causal narratives and charts."""
    workspace_id = _get_workspace(request)

    # Validate modes
    modes = set(body.modes) & VALID_MODES
    if not modes:
        modes = {"just_kpis"}

    # Validate date range
    if (body.from_year, body.from_month) > (body.to_year, body.to_month):
        raise HTTPException(400, "from_date must be before to_date")

    try:
        return _build_board_pack(workspace_id, body, modes)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Board pack generation failed: {str(e)}")


def _build_board_pack(workspace_id: str, body: BoardPackRequest, modes: set[str]):
    """Main generation function."""
    from pptx import Presentation
    from pptx.util import Inches, Pt
    from pptx.dml.color import RGBColor
    from pptx.enum.text import PP_ALIGN

    from core.health_score import compute_health_score
    from core.kpi_defs import KPI_DEFS, EXTENDED_ONTOLOGY_METRICS, BENCHMARKS, ALL_CAUSATION_RULES
    from core.kpi_utils import compute_kpi_avg
    from core.chart_engine import (
        render_line, render_multi_line, render_donut, render_bar_h,
        render_heatmap, render_radar, render_waterfall, render_grouped_bar_h,
        PALETTE, STATUS_COLORS,
    )
    from core.board_narrative import (
        detect_signals, generate_executive_summary, generate_causal_narratives,
        generate_domain_narratives, generate_period_comparison,
        generate_outlook, generate_talk_track, build_so_what,
        _get_domain, _gap_pct, _cell_status, _fmt_val,
    )
    from core.narrative_engine import enrich_needs_attention

    include_notes = "talk_track" in modes

    # ── Load data ───────────────────────────────────────────────────────────
    conn = get_db()
    from_period = (body.from_year, body.from_month)
    to_period = (body.to_year, body.to_month)

    health = compute_health_score(
        conn, workspace_id,
        from_period=from_period, to_period=to_period,
    )

    # Monthly data (filtered by period)
    rows = conn.execute(
        "SELECT year, month, data_json FROM monthly_data "
        "WHERE workspace_id=? AND (year > ? OR (year = ? AND month >= ?)) "
        "AND (year < ? OR (year = ? AND month <= ?)) ORDER BY year, month",
        [workspace_id,
         from_period[0], from_period[0], from_period[1],
         to_period[0], to_period[0], to_period[1]],
    ).fetchall()

    targets_rows = conn.execute(
        "SELECT kpi_key, target_value, direction, unit FROM kpi_targets WHERE workspace_id=?",
        [workspace_id],
    ).fetchall()

    settings_rows = conn.execute(
        "SELECT key, value FROM company_settings WHERE workspace_id=?",
        [workspace_id],
    ).fetchall()

    conn.close()

    targets_map = {r["kpi_key"]: {"target": r["target_value"], "direction": r["direction"] or "higher", "unit": r["unit"] or ""}
                   for r in targets_rows}
    settings = {r["key"]: r["value"] for r in settings_rows}
    company_name = body.company_name or settings.get("company_name", "Company")

    # Period label
    from_label = f"{_month_name(body.from_month)} {body.from_year}"
    to_label = f"{_month_name(body.to_month)} {body.to_year}"
    if body.from_year == body.to_year and body.from_month == body.to_month:
        period_label = from_label
    elif body.from_year == body.to_year:
        period_label = f"{_month_name(body.from_month)}-{_month_name(body.to_month)} {body.from_year}"
    else:
        period_label = f"{from_label} to {to_label}"

    # ── Build fingerprint ───────────────────────────────────────────────────
    all_kpi_meta = {kd["key"]: kd for kd in KPI_DEFS}
    for em in EXTENDED_ONTOLOGY_METRICS:
        if em["key"] not in all_kpi_meta:
            all_kpi_meta[em["key"]] = em

    kpi_monthly: dict = {}
    for row in rows:
        mo_key = f"{row['year']}-{row['month']:02d}"
        data = json.loads(row["data_json"]) if isinstance(row["data_json"], str) else (row["data_json"] or {})
        for kpi_key, val in data.items():
            if kpi_key in ("year", "month") or kpi_key.startswith("_"):
                continue
            if isinstance(val, (int, float)):
                kpi_monthly.setdefault(kpi_key, {})[mo_key] = val

    fingerprint = []
    kpi_avgs = {}
    time_series = {}
    directions = {}
    targets_flat = {}

    for key in set(list(kpi_monthly.keys()) + [kd["key"] for kd in KPI_DEFS]):
        vals_dict = kpi_monthly.get(key, {})
        t = targets_map.get(key, {})
        kdef = all_kpi_meta.get(key, {"key": key, "name": key.replace("_", " ").title(),
                                       "unit": "ratio", "direction": "higher"})
        tval = t.get("target")
        dirn = t.get("direction", kdef.get("direction", "higher"))
        unit = t.get("unit", kdef.get("unit", "ratio"))

        monthly_sorted = sorted(vals_dict.items())
        values = [v for _, v in monthly_sorted]
        monthly_list = [{"period": k, "value": v} for k, v in monthly_sorted]
        avg = compute_kpi_avg(values, window=len(values), period_filtered=True)

        if avg is None and not tval:
            continue

        kpi_avgs[key] = avg
        time_series[key] = values
        directions[key] = dirn
        if tval is not None:
            targets_flat[key] = tval

        fingerprint.append({
            "key": key,
            "name": kdef.get("name", key.replace("_", " ").title()),
            "unit": unit,
            "target": tval,
            "direction": dirn,
            "avg": avg,
            "fy_status": _cell_status(avg, tval, dirn),
            "monthly": monthly_list,
        })

    # Sort: KPI_DEFS order first
    kpi_def_order = {kd["key"]: i for i, kd in enumerate(KPI_DEFS)}
    fingerprint.sort(key=lambda x: (kpi_def_order.get(x["key"], 9999), x["key"]))

    # Classify KPIs
    red_kpis = [k for k in fingerprint if k["fy_status"] == "red"]
    yellow_kpis = [k for k in fingerprint if k["fy_status"] == "yellow"]
    green_kpis = [k for k in fingerprint if k["fy_status"] == "green"]
    total = len(red_kpis) + len(yellow_kpis) + len(green_kpis)

    # Sort red by gap magnitude
    def _gap_mag(k):
        g = _gap_pct(k.get("avg"), k.get("target"), k.get("direction", "higher"))
        return abs(g) if g else 0
    red_kpis.sort(key=_gap_mag, reverse=True)

    # ── Run analyses ────────────────────────────────────────────────────────
    signals = detect_signals(fingerprint)
    domain_narratives = generate_domain_narratives(fingerprint)

    # Root cause analysis for red KPIs
    critical_analyses = enrich_needs_attention(
        [k["key"] for k in red_kpis[:5]],
        kpi_avgs, time_series, targets_flat, directions, {},
    )

    # Executive summary
    exec_summary = generate_executive_summary(
        health, fingerprint, signals, period_label, critical_analyses,
    )

    # Causal narratives
    causal_narratives = generate_causal_narratives(
        red_kpis[:5], kpi_avgs, time_series, targets_flat, directions,
    )

    # Outlook
    outlook_bullets = generate_outlook(fingerprint, signals, domain_narratives)

    # ── PPTX helpers ────────────────────────────────────────────────────────
    def _rgb(hex6: str) -> RGBColor:
        h = hex6.lstrip("#")
        return RGBColor(int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))

    def _fill_bg(slide, hex_color: str):
        fill = slide.background.fill
        fill.solid()
        fill.fore_color.rgb = _rgb(hex_color)

    def _add_text(slide, text, left, top, width, height,
                  font_size=14, bold=False, color="0F172A", align=PP_ALIGN.LEFT, italic=False):
        from pptx.util import Inches as _In, Pt as _Pt
        txBox = slide.shapes.add_textbox(_In(left), _In(top), _In(width), _In(height))
        tf = txBox.text_frame
        tf.word_wrap = True
        p = tf.paragraphs[0]
        p.alignment = align
        run = p.add_run()
        run.text = text
        run.font.size = _Pt(font_size)
        run.font.name = "Calibri"
        run.font.bold = bold
        run.font.italic = italic
        run.font.color.rgb = _rgb(color)
        return txBox

    def _add_paragraphs(slide, paragraphs, left, top, width, height):
        """Add multi-paragraph text. paragraphs: [(text, size, bold, color_hex), ...]"""
        from pptx.util import Inches as _In, Pt as _Pt
        txBox = slide.shapes.add_textbox(_In(left), _In(top), _In(width), _In(height))
        tf = txBox.text_frame
        tf.word_wrap = True
        for i, (text, fs, bold, color) in enumerate(paragraphs):
            p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            p.text = text
            p.font.size = _Pt(fs)
            p.font.name = "Calibri"
            p.font.bold = bold
            if color:
                p.font.color.rgb = _rgb(color)
            p.space_after = _Pt(6)
        return txBox

    def _add_divider(slide, top_in: float, width: float = 12.0):
        from pptx.util import Inches as _In, Pt as _Pt
        line = slide.shapes.add_connector(1, _In(0.5), _In(top_in), _In(0.5 + width), _In(top_in))
        line.line.color.rgb = _rgb(THEME["card_bg"])
        line.line.width = _Pt(0.75)

    def _notes(slide, text):
        if include_notes and text:
            slide.notes_slide.notes_text_frame.text = text

    # ── Create Presentation ─────────────────────────────────────────────────
    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)
    blank = prs.slide_layouts[6]

    # ════════════════════════════════════════════════════════════════════════
    # SLIDE 1: Title (all modes)
    # ════════════════════════════════════════════════════════════════════════
    slide = prs.slides.add_slide(blank)
    _fill_bg(slide, THEME["title_bg"])

    # Accent bar
    rect = slide.shapes.add_shape(1, Inches(0), Inches(0), Inches(0.12), Inches(7.5))
    rect.fill.solid()
    rect.fill.fore_color.rgb = _rgb(THEME["highlight"])
    rect.line.fill.background()

    _add_text(slide, "BOARD PACK", 0.6, 1.8, 11, 0.5,
              font_size=12, bold=True, color=THEME["subtext"])
    _add_text(slide, company_name, 0.6, 2.4, 11, 1.2,
              font_size=40, bold=True, color="FFFFFF")
    _add_text(slide, f"Performance Review  ·  {period_label}", 0.6, 3.8, 11, 0.5,
              font_size=18, color="94A3B8")
    _add_text(slide, f"{len(red_kpis)} critical  ·  {len(yellow_kpis)} watch  ·  "
              f"{len(green_kpis)} on target  ·  {total} KPIs tracked",
              0.6, 4.5, 11, 0.6, font_size=20, bold=True, color="FFFFFF")
    _add_text(slide, f"Generated {datetime.utcnow().strftime('%B %d, %Y')}  ·  Axiom Intelligence",
              0.6, 6.5, 11, 0.4, font_size=10, color="94A3B8", italic=True)

    _notes(slide, generate_talk_track("title", {
        "company_name": company_name, "period_label": period_label,
    }))

    # ════════════════════════════════════════════════════════════════════════
    # SLIDE 2: Executive Health Summary (all modes)
    # ════════════════════════════════════════════════════════════════════════
    slide = prs.slides.add_slide(blank)
    _fill_bg(slide, THEME["bg"])

    _add_text(slide, "EXECUTIVE SUMMARY", 0.5, 0.3, 8, 0.35,
              font_size=9, bold=True, color=THEME["subtext"])

    # Left: narrative paragraphs
    paras = []
    for p_text in exec_summary:
        paras.append((p_text, 12, False, THEME["text"]))
        paras.append(("", 6, False, None))
    _add_paragraphs(slide, paras, 0.5, 1.0, 7.5, 5.5)

    # Right: donut chart
    donut = render_donut(
        [f"Critical ({len(red_kpis)})", f"Watch ({len(yellow_kpis)})", f"On Target ({len(green_kpis)})"],
        [len(red_kpis), len(yellow_kpis), len(green_kpis)],
        [PALETTE["critical"], PALETTE["warning"], PALETTE["positive"]],
        center_text=str(total), center_sub="KPIs",
    )
    slide.shapes.add_picture(donut.image, Inches(8.8), Inches(0.8),
                             Inches(4.0), Inches(4.0))

    # Health score badge
    score = health.get("score", 0)
    score_color = THEME["positive"] if score >= 70 else (THEME["warning"] if score >= 50 else THEME["critical"])
    _add_text(slide, f"Health Score: {score}/100", 9.0, 5.0, 3.5, 0.5,
              font_size=16, bold=True, color=score_color, align=PP_ALIGN.CENTER)
    _add_text(slide, health.get("label", ""), 9.0, 5.5, 3.5, 0.4,
              font_size=12, color=THEME["subtext"], align=PP_ALIGN.CENTER)

    _notes(slide, generate_talk_track("health_summary", {
        "score": score, "label": health.get("label", ""),
        "n_red": len(red_kpis), "n_green": len(green_kpis),
    }))

    # ════════════════════════════════════════════════════════════════════════
    # MODE: just_kpis
    # ════════════════════════════════════════════════════════════════════════
    if "just_kpis" in modes:
        # Slide: KPI Status Heatmap
        if fingerprint:
            slide = prs.slides.add_slide(blank)
            _fill_bg(slide, THEME["bg"])
            _add_text(slide, "KPI STATUS OVERVIEW", 0.5, 0.3, 8, 0.35,
                      font_size=9, bold=True, color=THEME["subtext"])
            _add_text(slide, "Status by KPI and Month", 0.5, 0.65, 8, 0.5,
                      font_size=20, bold=True, color=THEME["text"])

            # Build heatmap data (filtered to KPIs with targets)
            hm_kpis = [k for k in fingerprint if k.get("target") and k.get("monthly")][:20]
            if hm_kpis:
                month_labels = sorted(set(
                    m["period"] for k in hm_kpis for m in k.get("monthly", [])
                ))
                kpi_names = [k["name"][:25] for k in hm_kpis]
                status_matrix = []
                value_matrix = []
                for kpi in hm_kpis:
                    by_period = {m["period"]: m["value"] for m in kpi.get("monthly", [])}
                    row_status = []
                    row_values = []
                    for ml in month_labels:
                        val = by_period.get(ml)
                        row_status.append(_cell_status(val, kpi.get("target"), kpi.get("direction", "higher")))
                        row_values.append(val)
                    status_matrix.append(row_status)
                    value_matrix.append(row_values)

                heatmap = render_heatmap(kpi_names, month_labels, status_matrix, value_matrix)
                slide.shapes.add_picture(heatmap.image, Inches(0.3), Inches(1.3),
                                         Inches(min(heatmap.width_inches, 12.5)),
                                         Inches(min(heatmap.height_inches, 5.8)))

            _notes(slide, "This heatmap shows every KPI's status for each month in the selected period. "
                   "Green = on target, yellow = watch, red = critical. "
                   "Look for horizontal red streaks (persistent problems) and vertical clusters (systemic months).")

        # Slide: Domain Health Radar
        troubled_domains = {d: info for d, info in domain_narratives.items()
                           if d != "other" and info["total"] > 0}
        if len(troubled_domains) >= 3:
            slide = prs.slides.add_slide(blank)
            _fill_bg(slide, THEME["bg"])
            _add_text(slide, "DOMAIN HEALTH", 0.5, 0.3, 8, 0.35,
                      font_size=9, bold=True, color=THEME["subtext"])
            _add_text(slide, "Business Area Performance", 0.5, 0.65, 8, 0.5,
                      font_size=20, bold=True, color=THEME["text"])

            dims = []
            actuals = []
            for d, info in troubled_domains.items():
                dims.append(info["label"][:15])
                # Domain score = % on target
                actuals.append(round(info["green_count"] / info["total"] * 100) if info["total"] else 50)

            radar = render_radar(dims, actuals, title="Domain Health (% On Target)")
            slide.shapes.add_picture(radar.image, Inches(0.5), Inches(1.3),
                                     Inches(5.5), Inches(5.5))

            # Domain summary on right
            dom_paras = []
            for d, info in sorted(troubled_domains.items(), key=lambda x: -x[1].get("red_count", 0)):
                status = "critical" if info["red_count"] > 0 else ("watch" if info["yellow_count"] > 0 else "strong")
                color = THEME["critical"] if status == "critical" else (THEME["warning"] if status == "watch" else THEME["positive"])
                dom_paras.append((f"{info['label']}", 12, True, color))
                dom_paras.append((f"{info['green_count']} on target, {info['yellow_count']} watch, {info['red_count']} critical", 10, False, THEME["subtext"]))
                dom_paras.append(("", 4, False, None))
            _add_paragraphs(slide, dom_paras, 6.5, 1.3, 6.3, 5.5)

            _notes(slide, "The radar chart shows what percentage of KPIs are on target in each business domain. "
                   "Domains with low scores indicate systemic issues requiring domain-level intervention.")

        # Slide: Top/Bottom KPIs
        slide = prs.slides.add_slide(blank)
        _fill_bg(slide, THEME["bg"])
        _add_text(slide, "KEY METRICS", 0.5, 0.3, 12, 0.35,
                  font_size=9, bold=True, color=THEME["subtext"])

        # Top red KPIs (left)
        if red_kpis:
            _add_text(slide, "Needs Attention", 0.5, 0.7, 6, 0.4,
                      font_size=16, bold=True, color=THEME["critical"])
            y = 1.3
            for k in red_kpis[:6]:
                gap = _gap_pct(k.get("avg"), k.get("target"), k.get("direction", "higher"))
                gap_str = f"({abs(gap):.0f}% gap)" if gap else ""
                _add_text(slide, f"● {k['name']}", 0.5, y, 5.5, 0.3,
                          font_size=11, bold=True, color=THEME["text"])
                _add_text(slide, f"{_fmt_val(k.get('avg'), k.get('unit', ''))} vs {_fmt_val(k.get('target'), k.get('unit', ''))} {gap_str}",
                          0.8, y + 0.3, 5.2, 0.25, font_size=9, color=THEME["subtext"])
                y += 0.65

        # Top green KPIs (right)
        if green_kpis:
            _add_text(slide, "Performing Well", 7.0, 0.7, 6, 0.4,
                      font_size=16, bold=True, color=THEME["positive"])
            y = 1.3
            for k in green_kpis[:6]:
                _add_text(slide, f"● {k['name']}", 7.0, y, 5.5, 0.3,
                          font_size=11, bold=True, color=THEME["text"])
                _add_text(slide, f"{_fmt_val(k.get('avg'), k.get('unit', ''))} (target: {_fmt_val(k.get('target'), k.get('unit', ''))})",
                          7.3, y + 0.3, 5.2, 0.25, font_size=9, color=THEME["subtext"])
                y += 0.65

        _notes(slide, "Left: KPIs below target threshold ranked by gap severity. "
               "Right: KPIs meeting or exceeding targets. "
               "Focus discussion on the critical items and what actions are in progress.")

    # ════════════════════════════════════════════════════════════════════════
    # MODE: variance_narrative
    # ════════════════════════════════════════════════════════════════════════
    if "variance_narrative" in modes:

        # Slide: Causal Chain Analysis
        if causal_narratives:
            slide = prs.slides.add_slide(blank)
            _fill_bg(slide, THEME["bg"])
            _add_text(slide, "CAUSAL ANALYSIS", 0.5, 0.3, 8, 0.35,
                      font_size=9, bold=True, color=THEME["subtext"])
            _add_text(slide, "Root Cause Intelligence", 0.5, 0.65, 8, 0.5,
                      font_size=20, bold=True, color=THEME["text"])

            # Left: narrative cards for top 3 critical KPIs
            y_pos = 1.4
            paras = []
            for cn in causal_narratives[:3]:
                paras.append((cn["headline"], 12, True, THEME["critical"]))
                paras.append((cn["narrative"], 10, False, THEME["text"]))
                if cn.get("actions"):
                    paras.append((f"Action: {cn['actions'][0]}", 10, True, THEME["highlight"]))
                paras.append(("", 6, False, None))
            _add_paragraphs(slide, paras, 0.5, 1.4, 7.0, 5.5)

            # Right: trend chart of critical KPIs
            if red_kpis:
                month_labels_all = sorted(set(
                    m["period"] for k in red_kpis[:4] for m in k.get("monthly", [])
                ))
                series = {}
                tgts = {}
                for k in red_kpis[:4]:
                    by_p = {m["period"]: m["value"] for m in k.get("monthly", [])}
                    series[k["name"][:20]] = [by_p.get(ml) for ml in month_labels_all]
                    if k.get("target"):
                        tgts[k["name"][:20]] = k["target"]
                chart = render_multi_line(month_labels_all, series, "Critical KPI Trends", tgts)
                slide.shapes.add_picture(chart.image, Inches(7.8), Inches(1.2),
                                         Inches(5.2), Inches(5.2))

            _notes(slide, generate_talk_track("causal_analysis", {"narratives": causal_narratives}))

        # Slide: Signals & Hidden Risks
        if signals:
            slide = prs.slides.add_slide(blank)
            _fill_bg(slide, THEME["bg"])
            _add_text(slide, "HIDDEN SIGNALS", 0.5, 0.3, 8, 0.35,
                      font_size=9, bold=True, color=THEME["subtext"])
            _add_text(slide, "Structural Patterns in the Data", 0.5, 0.65, 8, 0.5,
                      font_size=20, bold=True, color=THEME["text"])

            paras = []
            sev_colors = {"critical": THEME["critical"], "warning": THEME["warning"], "positive": THEME["positive"]}
            for s in signals[:4]:
                paras.append((s["title"], 13, True, sev_colors.get(s["severity"], THEME["text"])))
                paras.append((s["body"], 10, False, THEME["text"]))
                paras.append(("", 6, False, None))
            _add_paragraphs(slide, paras, 0.5, 1.4, 12.0, 5.5)

            _notes(slide, generate_talk_track("signals", {"signals": signals}))

        # Slides: Domain Deep Dives (top 3 troubled domains)
        troubled = [(d, info) for d, info in domain_narratives.items()
                    if info.get("has_issues") and d != "other"]
        troubled.sort(key=lambda x: -(x[1].get("red_count", 0) * 10 + x[1].get("yellow_count", 0)))

        for domain, info in troubled[:3]:
            slide = prs.slides.add_slide(blank)
            _fill_bg(slide, THEME["bg"])
            _add_text(slide, f"DOMAIN: {info['label'].upper()}", 0.5, 0.3, 8, 0.35,
                      font_size=9, bold=True, color=THEME["subtext"])
            _add_text(slide, info["label"], 0.5, 0.65, 8, 0.5,
                      font_size=20, bold=True, color=THEME["text"])

            # Narrative
            dom_paras = [
                (info["story"], 12, False, THEME["text"]),
                ("", 6, False, None),
                (f"{info['green_count']} on target  ·  {info['yellow_count']} watch  ·  {info['red_count']} critical",
                 11, True, THEME["subtext"]),
            ]
            _add_paragraphs(slide, dom_paras, 0.5, 1.4, 6.5, 5.0)

            # Domain KPI trend chart (right)
            domain_kpis = info.get("kpis", [])
            kpis_with_data = [k for k in domain_kpis if k.get("monthly")][:5]
            if kpis_with_data:
                ml = sorted(set(m["period"] for k in kpis_with_data for m in k.get("monthly", [])))
                series = {}
                for k in kpis_with_data:
                    by_p = {m["period"]: m["value"] for m in k.get("monthly", [])}
                    series[k["name"][:18]] = [by_p.get(p) for p in ml]
                chart = render_multi_line(ml, series, f"{info['label']} Trends")
                slide.shapes.add_picture(chart.image, Inches(7.5), Inches(1.2),
                                         Inches(5.5), Inches(5.5))

            _notes(slide, generate_talk_track("domain", {"domain_info": info}))

        # Slide: Period Comparison (waterfall)
        # Only if we have enough months for a prior period comparison
        total_months = (body.to_year - body.from_year) * 12 + (body.to_month - body.from_month) + 1
        if total_months >= 2:
            # Split into first half / second half for comparison
            mid_months = total_months // 2
            # For waterfall: show KPI deltas between first and second half
            kpis_with_change = []
            for kpi in fingerprint:
                vals = [m["value"] for m in kpi.get("monthly", []) if m.get("value") is not None]
                if len(vals) >= 4:
                    first_half = vals[:len(vals) // 2]
                    second_half = vals[len(vals) // 2:]
                    avg1 = sum(first_half) / len(first_half)
                    avg2 = sum(second_half) / len(second_half)
                    if avg1 != 0:
                        delta = (avg2 - avg1) / abs(avg1) * 100
                        kpis_with_change.append((kpi["name"][:18], delta, kpi.get("direction", "higher")))

            if kpis_with_change:
                # Sort by absolute delta, take top 10
                kpis_with_change.sort(key=lambda x: -abs(x[1]))
                top_changes = kpis_with_change[:10]
                labels = [x[0] for x in top_changes]
                deltas = [x[1] for x in top_changes]

                slide = prs.slides.add_slide(blank)
                _fill_bg(slide, THEME["bg"])
                _add_text(slide, "PERIOD COMPARISON", 0.5, 0.3, 8, 0.35,
                          font_size=9, bold=True, color=THEME["subtext"])
                _add_text(slide, "Biggest Movers (% Change)", 0.5, 0.65, 8, 0.5,
                          font_size=20, bold=True, color=THEME["text"])

                wf = render_waterfall(labels, deltas, "Period-over-Period Change (%)")
                slide.shapes.add_picture(wf.image, Inches(0.5), Inches(1.5),
                                         Inches(12.0), Inches(5.0))

                _notes(slide, "This waterfall shows the biggest KPI movers between the first and second half "
                       "of the selected period. Green bars = improvement, red bars = deterioration.")

        # Slide: Corrective Actions
        if causal_narratives:
            slide = prs.slides.add_slide(blank)
            _fill_bg(slide, THEME["bg"])
            _add_text(slide, "CORRECTIVE ACTIONS", 0.5, 0.3, 8, 0.35,
                      font_size=9, bold=True, color=THEME["subtext"])
            _add_text(slide, "Priority Actions — Data-Grounded", 0.5, 0.65, 8, 0.5,
                      font_size=20, bold=True, color=THEME["text"])

            action_paras = []
            for i, cn in enumerate(causal_narratives[:5], 1):
                action_paras.append((f"{i}. {cn['headline']}", 12, True, THEME["critical"]))
                for a in cn.get("actions", [])[:2]:
                    action_paras.append((f"   → {a}", 10, False, THEME["text"]))
                if cn.get("downstream_count", 0) > 0:
                    action_paras.append((
                        f"   Impact: affects {cn['downstream_count']} downstream KPIs",
                        10, True, THEME["highlight"],
                    ))
                action_paras.append(("", 5, False, None))

            _add_paragraphs(slide, action_paras, 0.5, 1.4, 12.0, 5.5)

            _notes(slide, generate_talk_track("actions", {}))

    # ════════════════════════════════════════════════════════════════════════
    # MODE: financial_projections
    # ════════════════════════════════════════════════════════════════════════
    if "financial_projections" in modes:

        # Slide: Benchmark Position
        bench = {}
        valid_stages = {"seed", "series_a", "series_b", "series_c"}
        stage = body.company_stage if body.company_stage in valid_stages else "series_b"
        for kpi_key, stages_data in BENCHMARKS.items():
            if stage in stages_data:
                bench[kpi_key] = stages_data[stage]

        stage_label = {"seed": "Seed", "series_a": "Series A",
                       "series_b": "Series B", "series_c": "Series C+"}.get(stage, stage)

        # Build benchmark comparison
        kpis_for_bench = [k for k in (red_kpis + yellow_kpis + green_kpis)
                          if k["key"] in bench and k.get("avg") is not None][:10]
        if kpis_for_bench:
            slide = prs.slides.add_slide(blank)
            _fill_bg(slide, THEME["bg"])
            _add_text(slide, f"PEER BENCHMARK — {stage_label.upper()}", 0.5, 0.3, 10, 0.35,
                      font_size=9, bold=True, color=THEME["subtext"])
            _add_text(slide, f"Company vs {stage_label} Peer Median", 0.5, 0.65, 10, 0.5,
                      font_size=20, bold=True, color=THEME["text"])

            names = [k["name"][:25] for k in kpis_for_bench]
            company_vals = [k["avg"] for k in kpis_for_bench]
            peer_vals = [bench[k["key"]]["p50"] for k in kpis_for_bench]
            bar_colors = [
                PALETTE["critical"] if k["fy_status"] == "red" else
                PALETTE["warning"] if k["fy_status"] == "yellow" else
                PALETTE["positive"]
                for k in kpis_for_bench
            ]

            chart = render_grouped_bar_h(names, company_vals, peer_vals, bar_colors,
                                         f"Company vs {stage_label} Median",
                                         peer_label=f"{stage_label} Median")
            slide.shapes.add_picture(chart.image, Inches(0.3), Inches(1.3),
                                     Inches(8.0), Inches(min(chart.height_inches, 5.8)))

            # Narrative: below P25 / above P75
            bench_paras = []
            below_p25 = [k for k in kpis_for_bench if k["avg"] < bench[k["key"]].get("p25", float("inf"))]
            above_p75 = [k for k in kpis_for_bench if k["avg"] >= bench[k["key"]].get("p75", float("inf"))]
            if below_p25:
                bench_paras.append((f"{len(below_p25)} KPIs below {stage_label} bottom quartile (P25):", 12, True, THEME["critical"]))
                for bp in below_p25[:4]:
                    b = bench[bp["key"]]
                    bench_paras.append((f"  {bp['name']}: {bp['avg']:.1f} vs P25 {b['p25']}", 10, False, THEME["text"]))
            if above_p75:
                bench_paras.append(("", 4, False, None))
                bench_paras.append((f"{len(above_p75)} KPIs in top quartile (above P75):", 12, True, THEME["positive"]))
                for ap in above_p75[:4]:
                    b = bench[ap["key"]]
                    bench_paras.append((f"  {ap['name']}: {ap['avg']:.1f} vs P75 {b['p75']}", 10, False, THEME["text"]))
            if bench_paras:
                _add_paragraphs(slide, bench_paras, 8.8, 1.3, 4.2, 5.5)

        # Slide: 30-90 Day Outlook
        slide = prs.slides.add_slide(blank)
        _fill_bg(slide, THEME["bg"])
        _add_text(slide, "OUTLOOK", 0.5, 0.3, 8, 0.35,
                  font_size=9, bold=True, color=THEME["subtext"])
        _add_text(slide, "30-90 Day Forward View", 0.5, 0.65, 8, 0.5,
                  font_size=20, bold=True, color=THEME["text"])

        outlook_paras = []
        for bullet in outlook_bullets:
            outlook_paras.append((f"•  {bullet}", 13, False, THEME["text"]))
            outlook_paras.append(("", 6, False, None))
        _add_paragraphs(slide, outlook_paras, 0.5, 1.5, 12.0, 5.0)

        _notes(slide, generate_talk_track("outlook", {}))

    # ════════════════════════════════════════════════════════════════════════
    # SEC-GRADE ANALYTICS SLIDES (all modes except just_kpis)
    # ════════════════════════════════════════════════════════════════════════
    if "just_kpis" not in modes:
        try:
            _add_sec_slides(prs, blank, workspace_id, body, _add_text, _add_paragraphs,
                           _fill_bg, _rgb, _notes, include_notes, period_label,
                           generate_talk_track, THEME)
        except Exception as e:
            # SEC slides are non-blocking — log and continue
            print(f"[Board Pack] SEC slides failed (non-fatal): {e}")

    # ════════════════════════════════════════════════════════════════════════
    # CLOSING SLIDE: Key Takeaways (all modes)
    # ════════════════════════════════════════════════════════════════════════
    slide = prs.slides.add_slide(blank)
    _fill_bg(slide, THEME["title_bg"])

    # Accent bar
    rect = slide.shapes.add_shape(1, Inches(0), Inches(0), Inches(0.12), Inches(7.5))
    rect.fill.solid()
    rect.fill.fore_color.rgb = _rgb(THEME["highlight"])
    rect.line.fill.background()

    closing_paras = [
        ("Key Takeaways", 28, True, "FFFFFF"),
        ("", 8, False, None),
    ]
    if red_kpis:
        closing_paras.append((
            f"•  {len(red_kpis)} metrics need immediate attention — {red_kpis[0]['name']} is the top priority",
            15, False, "FFFFFF",
        ))
    if yellow_kpis:
        closing_paras.append((
            f"•  {len(yellow_kpis)} metrics in watch zone — monitor weekly to prevent escalation",
            15, False, "FFFFFF",
        ))
    if green_kpis:
        closing_paras.append((
            f"•  {len(green_kpis)} metrics on target — maintain current trajectory",
            15, False, "FFFFFF",
        ))

    # Add the most important signal
    if signals:
        s = signals[0]
        closing_paras.append((f"•  Signal: {s['title']}", 15, False, "FFFFFF"))

    # Add top action
    if causal_narratives and causal_narratives[0].get("actions"):
        closing_paras.append((
            f"•  Priority action: {causal_narratives[0]['actions'][0][:120]}",
            15, False, "FFFFFF",
        ))

    closing_paras.append(("", 12, False, None))
    closing_paras.append((
        f"Generated {datetime.utcnow().strftime('%B %d, %Y')}  ·  Axiom Intelligence  ·  {period_label}",
        11, False, "94A3B8",
    ))

    _add_paragraphs(slide, closing_paras, 0.6, 1.5, 12.0, 5.5)

    _notes(slide, generate_talk_track("takeaways", {}))

    # ── Serialise ───────────────────────────────────────────────────────────
    buf = io.BytesIO()
    prs.save(buf)
    buf.seek(0)

    filename = f"{company_name.replace(' ', '_')}_Board_Pack_{body.from_year}{body.from_month:02d}-{body.to_year}{body.to_month:02d}.pptx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _month_name(month_num: int) -> str:
    """Convert month number to abbreviated name."""
    names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
             "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    return names[month_num] if 1 <= month_num <= 12 else str(month_num)


def _add_sec_slides(prs, blank, workspace_id, body, _add_text, _add_paragraphs,
                    _fill_bg, _rgb, _notes, include_notes, period_label,
                    generate_talk_track, THEME):
    """Add SEC-grade analytics slides to the board pack.

    Includes: ARR Bridge, Customer Concentration, Unit Economics, Rule of 40,
    and Control Attestation summary. Each slide is defensive — a failure in
    one does not block others.
    """
    from pptx.util import Inches, Pt
    from core.database import get_db
    import json

    conn = get_db()

    def _fmt_usd(v):
        if v is None:
            return "--"
        av = abs(float(v))
        sign = "-" if float(v) < 0 else ""
        if av >= 1e9:
            return f"{sign}${av/1e9:.1f}B"
        if av >= 1e6:
            return f"{sign}${av/1e6:.1f}M"
        if av >= 1e3:
            return f"{sign}${av/1e3:.1f}K"
        return f"{sign}${av:.0f}"

    def _fmt_pct(v):
        if v is None:
            return "--"
        return f"{float(v):.1f}%"

    # ── SLIDE: ARR Bridge ────────────────────────────────────────────────
    try:
        rev_rows = conn.execute(
            "SELECT customer_id, period, amount FROM canonical_revenue "
            "WHERE workspace_id=? AND amount IS NOT NULL ORDER BY period",
            [workspace_id],
        ).fetchall()

        if rev_rows and len(rev_rows) > 5:
            # Compute latest period bridge
            customer_period: dict = {}
            for r in rev_rows:
                cid = r[0] or "unknown"
                period = str(r[1] or "")[:7]
                amt = float(r[2] or 0)
                customer_period.setdefault(period, {})
                customer_period[period][cid] = customer_period[period].get(cid, 0) + amt

            sorted_periods = sorted(customer_period.keys())
            if len(sorted_periods) >= 2:
                current = customer_period[sorted_periods[-1]]
                prev = customer_period[sorted_periods[-2]]
                new_arr = sum(v for k, v in current.items() if k not in prev) * 12
                churned_arr = sum(v for k, v in prev.items() if k not in current) * 12
                expansion = sum(max(0, current.get(k, 0) - prev.get(k, 0)) for k in current if k in prev and current[k] > prev[k]) * 12
                contraction = sum(max(0, prev.get(k, 0) - current.get(k, 0)) for k in current if k in prev and current[k] < prev[k]) * 12
                ending_arr = sum(current.values()) * 12

                slide = prs.slides.add_slide(blank)
                _fill_bg(slide, THEME["bg"])
                _add_text(slide, "ARR Bridge — Net New ARR Movement", 0.5, 0.3, 12, 0.6,
                         size=22, bold=True, color=THEME["accent"])
                _add_text(slide, period_label, 0.5, 0.85, 4, 0.3, size=11, color=THEME["subtext"])

                bridge_lines = [
                    f"New ARR:          {_fmt_usd(new_arr)}",
                    f"Expansion ARR:    {_fmt_usd(expansion)}",
                    f"Contraction ARR:  ({_fmt_usd(contraction)})",
                    f"Churned ARR:      ({_fmt_usd(churned_arr)})",
                    f"",
                    f"Net New ARR:      {_fmt_usd(new_arr + expansion - contraction - churned_arr)}",
                    f"Ending ARR:       {_fmt_usd(ending_arr)}",
                ]
                paras = [("ARR Bridge — Latest Period", 16, True, THEME["text"])]
                for line in bridge_lines:
                    color = THEME["positive"] if "New" in line or "Expansion" in line else (
                        THEME["critical"] if "Contraction" in line or "Churned" in line else THEME["text"])
                    paras.append((line, 13, False, color))

                _add_paragraphs(slide, paras, 0.5, 1.5, 6, 5)

                if include_notes:
                    _notes(slide, "ARR Bridge shows how recurring revenue moves period-over-period. "
                           "Focus on Net New ARR — this is the growth engine.")
    except Exception as e:
        print(f"[Board Pack] ARR Bridge slide failed: {e}")

    # ── SLIDE: Customer Concentration ─────────────��──────────────────────
    try:
        conc_rows = conn.execute(
            "SELECT customer_id, SUM(amount) as total FROM canonical_revenue "
            "WHERE workspace_id=? AND customer_id IS NOT NULL AND amount IS NOT NULL "
            "GROUP BY customer_id ORDER BY total DESC LIMIT 10",
            [workspace_id],
        ).fetchall()
        total_rev_row = conn.execute(
            "SELECT SUM(amount) FROM canonical_revenue WHERE workspace_id=? AND amount IS NOT NULL",
            [workspace_id],
        ).fetchone()
        total_rev = float(total_rev_row[0] or 0) if total_rev_row else 0

        if conc_rows and total_rev > 0:
            slide = prs.slides.add_slide(blank)
            _fill_bg(slide, THEME["bg"])
            _add_text(slide, "Customer Concentration Risk", 0.5, 0.3, 12, 0.6,
                     size=22, bold=True, color=THEME["accent"])

            top1_pct = float(conc_rows[0][1] or 0) / total_rev * 100 if conc_rows else 0
            top5_pct = sum(float(r[1] or 0) for r in conc_rows[:5]) / total_rev * 100
            top10_pct = sum(float(r[1] or 0) for r in conc_rows[:10]) / total_rev * 100
            sec_breached = top1_pct > 10

            paras = [
                ("Concentration Summary", 16, True, THEME["text"]),
                ("", 6, False, None),
                (f"Top 1 Customer:    {_fmt_pct(top1_pct)} of revenue" +
                 ("  ⚠ SEC THRESHOLD" if sec_breached else ""), 13, False,
                 THEME["critical"] if sec_breached else THEME["text"]),
                (f"Top 5 Customers:   {_fmt_pct(top5_pct)} of revenue", 13, False, THEME["text"]),
                (f"Top 10 Customers:  {_fmt_pct(top10_pct)} of revenue", 13, False, THEME["text"]),
                ("", 8, False, None),
            ]

            if sec_breached:
                paras.append(("SEC Disclosure Required: One or more customers exceed 10% of revenue. "
                             "This must be disclosed in 10-K filings.", 12, False, THEME["critical"]))

            # Top 5 table
            paras.append(("", 8, False, None))
            paras.append(("Top 5 Customers by Revenue", 14, True, THEME["text"]))
            for i, r in enumerate(conc_rows[:5]):
                cid = r[0] or "Unknown"
                pct = float(r[1] or 0) / total_rev * 100
                flag = "  ⚠" if pct > 10 else ""
                paras.append((f"  {i+1}. {cid[:30]}  —  {_fmt_usd(r[1])}  ({_fmt_pct(pct)}){flag}",
                             11, False, THEME["critical"] if pct > 10 else THEME["text"]))

            _add_paragraphs(slide, paras, 0.5, 1.2, 12, 5.5)

            if include_notes:
                _notes(slide, "Customer concentration is a key risk factor. SEC requires disclosure "
                       "of any customer >10% of revenue. High HHI signals dependency risk.")
    except Exception as e:
        print(f"[Board Pack] Concentration slide failed: {e}")

    # ── SLIDE: Unit Economics & Rule of 40 ──────────��────────────────────
    try:
        kpi_row = conn.execute(
            "SELECT data_json FROM monthly_data WHERE workspace_id=? "
            "ORDER BY year DESC, month DESC LIMIT 1",
            [workspace_id],
        ).fetchone()
        kpi_data = json.loads(kpi_row[0]) if kpi_row else {}

        rev_growth = kpi_data.get("revenue_growth")
        ebitda_margin = kpi_data.get("ebitda_margin")
        gross_margin = kpi_data.get("gross_margin")
        churn = kpi_data.get("churn_rate")
        nrr = kpi_data.get("nrr")
        burn_multiple = kpi_data.get("burn_multiple")
        ltv_cac = kpi_data.get("ltv_cac")
        customer_ltv = kpi_data.get("customer_ltv")

        if rev_growth is not None or ebitda_margin is not None:
            slide = prs.slides.add_slide(blank)
            _fill_bg(slide, THEME["bg"])
            _add_text(slide, "Unit Economics & Strategic Metrics", 0.5, 0.3, 12, 0.6,
                     size=22, bold=True, color=THEME["accent"])
            _add_text(slide, period_label, 0.5, 0.85, 4, 0.3, size=11, color=THEME["subtext"])

            rule_40 = (float(rev_growth or 0) + float(ebitda_margin or 0))
            r40_color = THEME["positive"] if rule_40 >= 40 else (THEME["warning"] if rule_40 >= 25 else THEME["critical"])

            paras = [
                ("Rule of 40", 18, True, THEME["text"]),
                ("", 4, False, None),
                (f"Revenue Growth:   {_fmt_pct(rev_growth)}", 13, False, THEME["text"]),
                (f"EBITDA Margin:    {_fmt_pct(ebitda_margin)}", 13, False, THEME["text"]),
                (f"Rule of 40 Score: {rule_40:.1f}  {'✓ Above 40' if rule_40 >= 40 else '✗ Below 40'}", 14, True, r40_color),
                ("", 10, False, None),
                ("Key Unit Economics", 18, True, THEME["text"]),
                ("", 4, False, None),
                (f"Gross Margin:     {_fmt_pct(gross_margin)}", 13, False, THEME["text"]),
                (f"Churn Rate:       {_fmt_pct(churn)}", 13, False, THEME["text"]),
                (f"NRR:              {_fmt_pct(nrr)}", 13, False, THEME["text"]),
                (f"Burn Multiple:    {burn_multiple:.2f}x" if burn_multiple else "Burn Multiple:    --", 13, False, THEME["text"]),
                (f"LTV:CAC:          {ltv_cac:.1f}x" if ltv_cac else "LTV:CAC:          --", 13, False, THEME["text"]),
                (f"Customer LTV:     {_fmt_usd(customer_ltv)}", 13, False, THEME["text"]),
            ]

            # GAAP/Non-GAAP footnote
            paras.append(("", 8, False, None))
            paras.append(("Note: Revenue Growth and Gross/EBITDA Margins are GAAP-aligned. "
                         "ARR, NRR, Burn Multiple, LTV:CAC are Non-GAAP operating metrics.",
                         9, False, THEME["subtext"]))

            _add_paragraphs(slide, paras, 0.5, 1.3, 12, 5.5)

            if include_notes:
                _notes(slide, "Rule of 40 = Revenue Growth + Profit Margin. Above 40 = healthy SaaS business. "
                       "GAAP vs Non-GAAP distinction is critical for SEC filings.")
    except Exception as e:
        print(f"[Board Pack] Unit Economics slide failed: {e}")

    # ── SLIDE: Data Governance / Control Attestation ───────────���─────────
    try:
        integrity_row = conn.execute(
            "SELECT overall_status, stage0_status, stage1_status, stage2_status, "
            "stage3_status, stage4_status, correction_attempted, correction_succeeded "
            "FROM integrity_checks WHERE workspace_id=? ORDER BY started_at DESC LIMIT 1",
            [workspace_id],
        ).fetchone()

        if integrity_row:
            slide = prs.slides.add_slide(blank)
            _fill_bg(slide, THEME["bg"])
            _add_text(slide, "Data Governance & Control Attestation", 0.5, 0.3, 12, 0.6,
                     size=22, bold=True, color=THEME["accent"])

            status = integrity_row[0] or "unknown"
            stages = ["Temporal", "Source Reconciliation", "KPI Logic", "Display Consistency", "Statistical"]
            stage_statuses = [integrity_row[i+1] or "unknown" for i in range(5)]

            paras = [
                ("Integrity Check Summary", 16, True, THEME["text"]),
                ("", 4, False, None),
                (f"Overall Status: {status.upper()}", 14, True,
                 THEME["positive"] if status == "pass" else (THEME["warning"] if status == "warn" else THEME["critical"])),
                ("", 6, False, None),
            ]

            for i, (stage_name, s_status) in enumerate(zip(stages, stage_statuses)):
                icon = "✓" if s_status == "pass" else ("⚠" if s_status == "warn" else "✗")
                color = THEME["positive"] if s_status == "pass" else (THEME["warning"] if s_status == "warn" else THEME["critical"])
                paras.append((f"  Stage {i}: {stage_name}  —  {icon} {s_status.upper()}", 12, False, color))

            if integrity_row[6]:  # correction_attempted
                paras.append(("", 6, False, None))
                success = "succeeded" if integrity_row[7] else "failed"
                paras.append((f"Auto-correction: {success}", 12, False,
                             THEME["positive"] if integrity_row[7] else THEME["critical"]))

            paras.append(("", 10, False, None))
            paras.append(("This attestation summarizes automated data quality controls. "
                         "5 stages covering temporal validation, source reconciliation, "
                         "KPI computation verification, display consistency, and statistical anomaly detection.",
                         9, False, THEME["subtext"]))

            _add_paragraphs(slide, paras, 0.5, 1.3, 12, 5.5)

            if include_notes:
                _notes(slide, "Control attestation provides audit-grade evidence of data integrity. "
                       "5 stages mirror SOX internal control framework.")
    except Exception as e:
        print(f"[Board Pack] Attestation slide failed: {e}")

    conn.close()
