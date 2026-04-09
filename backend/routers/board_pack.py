"""
routers/board_pack.py — Board Pack generator.

POST /api/board-pack/generate  — returns PPTX file download
GET  /api/board-pack/themes    — available themes

Uses python-pptx to generate a professional slide deck with:
  - Title slide
  - Health Score summary
  - KPI Overview (top metrics)
  - Needs Attention slide
  - Variance Analysis
  - Talk tracks in slide notes for each slide
"""
import io
import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from core.database import get_db
from core.deps import _require_workspace
from core.health_score import compute_health_score

router = APIRouter()

# ── Colour themes ─────────────────────────────────────────────────────────────

THEMES = {
    "axiom": {
        "name":       "Axiom Dark",
        "background": "0F172A",
        "accent":     "0055A4",
        "highlight":  "00AEEF",
        "text":       "FFFFFF",
        "subtext":    "94A3B8",
        "positive":   "059669",
        "warning":    "D97706",
        "critical":   "DC2626",
        "card_bg":    "1E293B",
    },
    "corporate": {
        "name":       "Corporate Blue",
        "background": "FFFFFF",
        "accent":     "003087",
        "highlight":  "0055A4",
        "text":       "0F172A",
        "subtext":    "64748B",
        "positive":   "059669",
        "warning":    "D97706",
        "critical":   "DC2626",
        "card_bg":    "F1F5F9",
    },
    "slate": {
        "name":       "Slate Professional",
        "background": "1E293B",
        "accent":     "6366F1",
        "highlight":  "818CF8",
        "text":       "F1F5F9",
        "subtext":    "94A3B8",
        "positive":   "10B981",
        "warning":    "F59E0B",
        "critical":   "EF4444",
        "card_bg":    "334155",
    },
    "minimal": {
        "name":       "Minimal Light",
        "background": "FAFAFA",
        "accent":     "18181B",
        "highlight":  "3F3F46",
        "text":       "09090B",
        "subtext":    "71717A",
        "positive":   "16A34A",
        "warning":    "CA8A04",
        "critical":   "DC2626",
        "card_bg":    "F4F4F5",
    },
}


@router.get("/api/board-pack/themes", tags=["Board Pack"])
def get_themes():
    return [{"id": k, "name": v["name"]} for k, v in THEMES.items()]


class BoardPackRequest(BaseModel):
    theme:        str = "corporate"
    company_name: Optional[str] = None
    period_label: Optional[str] = None
    include_talk_tracks:  bool = True
    include_variance:     bool = True
    include_forward:      bool = False
    kpi_keys:     Optional[list] = None  # None = auto-select top KPIs


@router.post("/api/board-pack/generate", tags=["Board Pack"])
async def generate_board_pack(request: Request, body: BoardPackRequest):
    """
    Generate a PPTX board pack. Returns the file as a download.
    """
    workspace_id = _require_workspace(request)

    try:
        from pptx import Presentation
        from pptx.util import Inches, Pt, Emu
        from pptx.dml.color import RGBColor
        from pptx.enum.text import PP_ALIGN
        from pptx.util import Inches, Pt
    except ImportError:
        raise HTTPException(status_code=500, detail="python-pptx not installed")

    try:
        return _generate_board_pack_inner(workspace_id, body, Presentation, Inches, Pt, Emu, RGBColor, PP_ALIGN)
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[ERROR] Board pack generation failed: {e}\n{tb}")
        raise HTTPException(status_code=500, detail=f"Board pack generation failed: {str(e)}")


def _generate_board_pack_inner(workspace_id, body, Presentation, Inches, Pt, Emu, RGBColor, PP_ALIGN):
    """Inner function — separated so exceptions are caught by the caller."""
    theme = THEMES.get(body.theme, THEMES["corporate"])
    conn = get_db()

    # ── Load data ─────────────────────────────────────────────────────────────
    health = compute_health_score(conn, workspace_id)
    rows   = conn.execute(
        "SELECT year, month, data_json FROM monthly_data WHERE workspace_id=? ORDER BY year, month",
        [workspace_id]
    ).fetchall()
    targets_rows = conn.execute(
        "SELECT kpi_key, target_value, direction, unit FROM kpi_targets WHERE workspace_id=?",
        [workspace_id]
    ).fetchall()
    settings_rows = conn.execute(
        "SELECT key, value FROM company_settings WHERE workspace_id=?", [workspace_id]
    ).fetchall()
    conn.close()

    targets_map = {r["kpi_key"]: {"target": r["target_value"], "direction": r["direction"] or "higher", "unit": r["unit"] or ""} for r in targets_rows}
    settings    = {r["key"]: r["value"] for r in settings_rows}
    company_name = body.company_name or settings.get("company_name", "Company")
    period_label = body.period_label or datetime.utcnow().strftime("%B %Y")

    # Build KPI averages
    kpi_monthly: dict = {}
    for row in rows:
        d = json.loads(row["data_json"])
        for k, v in d.items():
            if v is not None and k not in ("year","month"):
                kpi_monthly.setdefault(k, []).append(v)
    kpi_avgs = {k: round(sum(v) / len(v), 2) for k, v in kpi_monthly.items() if v}

    # Normalise health keys that may be None or missing
    h_needs_attention = health.get("needs_attention") or []
    h_doing_well      = health.get("doing_well") or []

    # Select KPIs to show
    if body.kpi_keys:
        show_keys = body.kpi_keys[:12]
    else:
        # Auto-select: red first, then yellow, then green — max 12
        red    = [k for k in h_needs_attention if k in kpi_avgs]
        green  = [k for k in h_doing_well     if k in kpi_avgs]
        others = [k for k in kpi_avgs if k not in red and k not in green]
        show_keys = (red + green + others)[:12]

    def _rgb(hex6: str) -> RGBColor:
        h = hex6.lstrip("#")
        return RGBColor(int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))

    def _add_textbox(slide, text, left, top, width, height,
                     font_size=14, bold=False, color="FFFFFF", align=PP_ALIGN.LEFT, italic=False):
        txBox = slide.shapes.add_textbox(Inches(left), Inches(top), Inches(width), Inches(height))
        tf = txBox.text_frame
        tf.word_wrap = True
        p = tf.paragraphs[0]
        p.alignment = align
        run = p.add_run()
        run.text = text
        run.font.size = Pt(font_size)
        run.font.bold = bold
        run.font.italic = italic
        run.font.color.rgb = _rgb(color)
        return txBox

    def _fill_slide_bg(slide, prs, hex_color: str):
        fill = slide.background.fill
        fill.solid()
        fill.fore_color.rgb = _rgb(hex_color)

    def _add_divider(slide, top: float, color: str, width: float = 9.0):
        from pptx.util import Pt as _Pt
        line = slide.shapes.add_connector(1, Inches(0.5), Inches(top), Inches(0.5 + width), Inches(top))
        line.line.color.rgb = _rgb(color)
        line.line.width = _Pt(0.75)

    # ── Create presentation ───────────────────────────────────────────────────
    prs = Presentation()
    prs.slide_width  = Inches(10)
    prs.slide_height = Inches(5.625)  # 16:9

    blank_layout = prs.slide_layouts[6]  # blank

    bg  = theme["background"]
    acc = theme["accent"]
    hi  = theme["highlight"]
    txt = theme["text"]
    sub = theme["subtext"]
    pos = theme["positive"]
    wrn = theme["warning"]
    crt = theme["critical"]

    # ─────────────────────────────────────────────────────────────────────────
    # SLIDE 1: Title
    # ─────────────────────────────────────────────────────────────────────────
    slide = prs.slides.add_slide(blank_layout)
    _fill_slide_bg(slide, prs, bg)

    # Accent bar left edge
    rect = slide.shapes.add_shape(1, Inches(0), Inches(0), Inches(0.08), Inches(5.625))
    rect.fill.solid()
    rect.fill.fore_color.rgb = _rgb(acc)
    rect.line.fill.background()

    _add_textbox(slide, "BOARD PACK", 0.3, 1.5, 9, 0.5, font_size=10, bold=True, color=sub)
    _add_textbox(slide, company_name, 0.3, 2.0, 9, 1.0, font_size=36, bold=True, color=txt)
    _add_textbox(slide, f"Performance Review · {period_label}", 0.3, 3.0, 9, 0.5, font_size=14, color=sub)
    _add_textbox(slide, f"Prepared by Axiom Intelligence", 0.3, 3.5, 9, 0.4, font_size=11, color=sub, italic=True)

    if body.include_talk_tracks:
        slide.notes_slide.notes_text_frame.text = (
            f"Opening slide for {company_name} board review — {period_label}.\n\n"
            "Talk Track: Welcome the board to the performance review. "
            "This deck uses Axiom Intelligence data to give an objective view of the company's "
            "health across all key financial and operational metrics. "
            "We'll walk through the Health Score, specific KPI performance, areas needing attention, "
            "and what we're doing well."
        )

    # ─────────────────────────────────────────────────────────────────────────
    # SLIDE 2: Health Score Summary
    # ─────────────────────────────────────────────────────────────────────────
    slide = prs.slides.add_slide(blank_layout)
    _fill_slide_bg(slide, prs, bg)

    score = health.get("score") or 0
    score_color = pos if score >= 70 else (wrn if score >= 50 else crt)
    health_label = health.get("label") or "No Data"
    h_momentum = health.get("momentum") or 0
    h_target   = health.get("target_achievement") or 0
    h_risk     = health.get("risk_flags") or 0

    _add_textbox(slide, "HEALTH SCORE", 0.5, 0.3, 9, 0.35, font_size=9, bold=True, color=sub)
    _add_textbox(slide, f"{score}", 0.5, 0.65, 2.5, 1.4, font_size=72, bold=True, color=score_color)
    _add_textbox(slide, f"/ 100", 2.7, 1.1, 1.5, 0.6, font_size=18, color=sub)
    _add_textbox(slide, health_label, 0.5, 2.0, 3, 0.5, font_size=18, bold=True, color=score_color)

    # Component breakdown right side
    components = [
        ("Momentum",           h_momentum,  "30%"),
        ("Target Achievement", h_target,    "40%"),
        ("Risk Score",         h_risk,      "30%"),
    ]
    y = 0.6
    for name, val, weight in components:
        bar_color = pos if val >= 70 else (wrn if val >= 50 else crt)
        _add_textbox(slide, f"{name} ({weight})", 4.2, y, 3.5, 0.3, font_size=10, color=sub)
        _add_textbox(slide, f"{val:.0f}", 8.0, y, 1.5, 0.3, font_size=11, bold=True, color=bar_color, align=PP_ALIGN.RIGHT)
        # Bar background
        bar_bg = slide.shapes.add_shape(1, Inches(4.2), Inches(y + 0.32), Inches(4.8), Inches(0.12))
        bar_bg.fill.solid(); bar_bg.fill.fore_color.rgb = _rgb("334155"); bar_bg.line.fill.background()
        # Bar fill
        bar_w = max(0.1, (val / 100) * 4.8)
        bar_fill = slide.shapes.add_shape(1, Inches(4.2), Inches(y + 0.32), Inches(bar_w), Inches(0.12))
        bar_fill.fill.solid(); bar_fill.fill.fore_color.rgb = _rgb(bar_color); bar_fill.line.fill.background()
        y += 0.8

    # KPI status counts
    _add_divider(slide, 3.1, sub, 9.0)
    _add_textbox(slide, "KPI STATUS DISTRIBUTION", 0.5, 3.25, 9, 0.3, font_size=8, bold=True, color=sub)

    status_items = [
        (str(health.get("kpis_green", 0)),  "On Target",  pos),
        (str(health.get("kpis_yellow", 0)), "Watch",      wrn),
        (str(health.get("kpis_red", 0)),    "Critical",   crt),
        (str(health.get("kpis_grey", 0)),   "No Target",  sub),
    ]
    x = 0.5
    for val, label, color in status_items:
        _add_textbox(slide, val,   x, 3.6, 2.0, 0.6, font_size=28, bold=True, color=color)
        _add_textbox(slide, label, x, 4.2, 2.0, 0.3, font_size=9, color=sub)
        x += 2.3

    if body.include_talk_tracks:
        momentum_words = {"improving": "trending upward", "stable": "holding steady", "declining": "under pressure"}
        slide.notes_slide.notes_text_frame.text = (
            f"Health Score: {score}/100 — {health_label}\n\n"
            f"Talk Track: The company's overall health score is {score} out of 100, rated {health_label}. "
            f"Momentum is {momentum_words.get(health.get('momentum_trend', ''), 'stable')} at {h_momentum:.0f} points. "
            f"We are hitting targets on {h_target:.0f}% of our KPIs. "
            f"There are {health.get('kpis_red', 0)} KPIs in critical territory that we will address on the next slide. "
            f"The risk score of {h_risk:.0f} reflects our overall exposure."
        )

    # ─────────────────────────────────────────────────────────────────────────
    # SLIDE 3: Needs Attention
    # ─────────────────────────────────────────────────────────────────────────
    if h_needs_attention:
        slide = prs.slides.add_slide(blank_layout)
        _fill_slide_bg(slide, prs, bg)

        _add_textbox(slide, "NEEDS ATTENTION", 0.5, 0.3, 9, 0.35, font_size=9, bold=True, color=sub)
        _add_textbox(slide, "KPIs Below Target Threshold", 0.5, 0.65, 9, 0.5, font_size=20, bold=True, color=txt)

        y = 1.4
        for key in h_needs_attention[:5]:
            avg   = kpi_avgs.get(key)
            tval  = targets_map.get(key, {}).get("target")
            dirn  = targets_map.get(key, {}).get("direction", "higher")
            unit  = targets_map.get(key, {}).get("unit", "")
            label = key.replace("_", " ").title()

            if avg is not None and tval:
                if dirn == "higher":
                    gap_pct = (avg / tval - 1) * 100
                else:
                    gap_pct = (tval / avg - 1) * 100
                gap_str = f"{gap_pct:+.1f}% vs target"
                gap_color = crt if gap_pct < -10 else wrn
            else:
                gap_str, gap_color = "No target set", sub

            avg_str = f"{avg:.1f}{unit}" if avg is not None else "—"
            tgt_str = f"{tval:.1f}{unit}" if tval is not None else "—"

            _add_textbox(slide, "●", 0.5, y, 0.4, 0.35, font_size=10, color=gap_color)
            _add_textbox(slide, label, 0.85, y, 4.0, 0.35, font_size=13, bold=True, color=txt)
            _add_textbox(slide, f"Actual: {avg_str}  ·  Target: {tgt_str}  ·  {gap_str}", 0.85, y + 0.35, 8.0, 0.3, font_size=10, color=sub)
            y += 0.82

        if body.include_talk_tracks:
            kpi_names = ", ".join(k.replace("_"," ").title() for k in h_needs_attention[:4])
            slide.notes_slide.notes_text_frame.text = (
                f"KPIs Needing Attention\n\n"
                f"Talk Track: The following KPIs are below the 90% target threshold and require management focus: {kpi_names}. "
                f"For each, I'll walk through the current value versus target, the gap percentage, and what actions are underway. "
                f"We ask the board to weigh in on prioritisation where resource allocation is required."
            )

    # ─────────────────────────────────────────────────────────────────────────
    # SLIDE 4: Doing Well
    # ─────────────────────────────────────────────────────────────────────────
    if h_doing_well:
        slide = prs.slides.add_slide(blank_layout)
        _fill_slide_bg(slide, prs, bg)

        _add_textbox(slide, "DOING WELL", 0.5, 0.3, 9, 0.35, font_size=9, bold=True, color=sub)
        _add_textbox(slide, "KPIs Outperforming Target", 0.5, 0.65, 9, 0.5, font_size=20, bold=True, color=txt)

        y = 1.4
        for key in h_doing_well[:5]:
            avg  = kpi_avgs.get(key)
            tval = targets_map.get(key, {}).get("target")
            dirn = targets_map.get(key, {}).get("direction", "higher")
            unit = targets_map.get(key, {}).get("unit", "")
            label = key.replace("_", " ").title()

            if avg is not None and tval:
                if dirn == "higher":
                    gap_pct = (avg / tval - 1) * 100
                else:
                    gap_pct = (tval / avg - 1) * 100
                gap_str = f"{gap_pct:+.1f}% vs target"
            else:
                gap_str = "On target"

            avg_str = f"{avg:.1f}{unit}" if avg is not None else "—"
            tgt_str = f"{tval:.1f}{unit}" if tval is not None else "—"

            _add_textbox(slide, "●", 0.5, y, 0.4, 0.35, font_size=10, color=pos)
            _add_textbox(slide, label, 0.85, y, 4.0, 0.35, font_size=13, bold=True, color=txt)
            _add_textbox(slide, f"Actual: {avg_str}  ·  Target: {tgt_str}  ·  {gap_str}", 0.85, y + 0.35, 8.0, 0.3, font_size=10, color=sub)
            y += 0.82

        if body.include_talk_tracks:
            kpi_names = ", ".join(k.replace("_"," ").title() for k in h_doing_well[:4])
            slide.notes_slide.notes_text_frame.text = (
                f"KPIs Doing Well\n\nTalk Track: I want to highlight the areas where we are outperforming: {kpi_names}. "
                "These represent genuine strengths we should protect and, where possible, leverage as competitive advantages. "
                "The team deserves recognition for these results."
            )

    # ─────────────────────────────────────────────────────────────────────────
    # SLIDE 5: KPI Overview Table
    # ─────────────────────────────────────────────────────────────────────────
    slide = prs.slides.add_slide(blank_layout)
    _fill_slide_bg(slide, prs, bg)

    _add_textbox(slide, "KPI OVERVIEW", 0.5, 0.3, 9, 0.35, font_size=9, bold=True, color=sub)
    _add_textbox(slide, "Key Metrics at a Glance", 0.5, 0.65, 9, 0.5, font_size=20, bold=True, color=txt)

    # Table header
    _add_textbox(slide, "METRIC",    0.5, 1.35, 4.0, 0.3, font_size=8, bold=True, color=sub)
    _add_textbox(slide, "ACTUAL",    4.5, 1.35, 1.5, 0.3, font_size=8, bold=True, color=sub, align=PP_ALIGN.CENTER)
    _add_textbox(slide, "TARGET",    6.0, 1.35, 1.5, 0.3, font_size=8, bold=True, color=sub, align=PP_ALIGN.CENTER)
    _add_textbox(slide, "STATUS",    7.5, 1.35, 1.5, 0.3, font_size=8, bold=True, color=sub, align=PP_ALIGN.CENTER)

    _add_divider(slide, 1.65, sub, 9.0)

    y = 1.7
    for key in show_keys[:10]:
        avg   = kpi_avgs.get(key)
        tval  = targets_map.get(key, {}).get("target")
        dirn  = targets_map.get(key, {}).get("direction", "higher")
        unit  = targets_map.get(key, {}).get("unit", "")
        label = key.replace("_", " ").title()

        # Status dot
        if avg is None or tval is None:
            st_color, st_char = sub, "○"
        elif dirn == "higher":
            pct = avg / tval if tval else 0
            st_color = pos if pct >= 0.98 else (wrn if pct >= 0.90 else crt)
            st_char  = "●"
        else:
            pct = tval / avg if avg else 0
            st_color = pos if pct >= 0.98 else (wrn if pct >= 0.90 else crt)
            st_char  = "●"

        avg_str = f"{avg:.1f}{unit}" if avg is not None else "—"
        tgt_str = f"{tval:.1f}{unit}" if tval is not None else "—"

        _add_textbox(slide, label,   0.5, y, 4.0, 0.3, font_size=10, color=txt)
        _add_textbox(slide, avg_str, 4.5, y, 1.5, 0.3, font_size=10, color=txt, align=PP_ALIGN.CENTER)
        _add_textbox(slide, tgt_str, 6.0, y, 1.5, 0.3, font_size=10, color=sub, align=PP_ALIGN.CENTER)
        _add_textbox(slide, st_char, 7.5, y, 1.5, 0.3, font_size=10, color=st_color, align=PP_ALIGN.CENTER)
        y += 0.38

    if body.include_talk_tracks:
        slide.notes_slide.notes_text_frame.text = (
            "KPI Overview Table\n\nTalk Track: This slide provides the complete snapshot of our key performance indicators. "
            "Green dots indicate KPIs meeting or exceeding target. "
            "Amber indicates within 10% of target — close but not there. "
            "Red indicates more than 10% below target. "
            "Let's use this as a reference point throughout the discussion."
        )

    # ─────────────────────────────────────────────────────────────────────────
    # SLIDE 6: Closing / Next Steps
    # ─────────────────────────────────────────────────────────────────────────
    slide = prs.slides.add_slide(blank_layout)
    _fill_slide_bg(slide, prs, bg)

    rect = slide.shapes.add_shape(1, Inches(0), Inches(0), Inches(0.08), Inches(5.625))
    rect.fill.solid(); rect.fill.fore_color.rgb = _rgb(acc); rect.line.fill.background()

    _add_textbox(slide, "NEXT STEPS", 0.3, 1.5, 9, 0.5, font_size=10, bold=True, color=sub)
    _add_textbox(slide, "Actions & Decisions Required", 0.3, 2.0, 9, 0.8, font_size=24, bold=True, color=txt)
    _add_textbox(slide, "Add your key next steps and board asks here.", 0.3, 2.9, 9, 0.5, font_size=13, color=sub, italic=True)
    _add_textbox(slide, f"Generated by Axiom Intelligence · {period_label}", 0.3, 4.8, 9, 0.35, font_size=9, color=sub)

    if body.include_talk_tracks:
        slide.notes_slide.notes_text_frame.text = (
            "Next Steps & Board Asks\n\nTalk Track: Before we close, I want to highlight the specific decisions and endorsements "
            "we are seeking from the board today. Please add your specific asks to this slide before presenting. "
            "Standard items might include: budget reallocation requests, hiring approvals, strategic pivots, "
            "or acknowledgement of risk areas. Thank the board for their time and engagement."
        )

    # ── Serialise to bytes ────────────────────────────────────────────────────
    buf = io.BytesIO()
    prs.save(buf)
    buf.seek(0)

    filename = f"{company_name.replace(' ', '_')}_Board_Pack_{datetime.utcnow().strftime('%Y%m')}.pptx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
