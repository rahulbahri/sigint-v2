"""
core/chart_engine.py — Intelligent chart selection + matplotlib rendering.

Analyses data patterns and automatically selects the most appropriate
visualisation type, then renders it as a PNG BytesIO for embedding in PPTX.

Chart types supported:
  - line          : single KPI trend over time
  - multi_line    : 2-5 KPI trends overlaid
  - donut         : part-to-whole composition (status distribution)
  - bar_h         : horizontal bar ranking / comparison
  - heatmap       : KPI × month status grid
  - radar         : multi-axis domain health comparison
  - waterfall     : period-over-period incremental changes
  - kpi_card      : hero number with sparkline
  - grouped_bar_h : company vs benchmark comparison

All rendering uses the Agg backend (no display). Every figure is closed
after rendering to prevent memory leaks.
"""
from __future__ import annotations

import io
import math
from dataclasses import dataclass
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import numpy as np

# ── Corporate Blue Palette ──────────────────────────────────────────────────

PALETTE = {
    "primary":   "#003087",
    "accent":    "#0055A4",
    "highlight": "#2563EB",
    "positive":  "#059669",
    "warning":   "#D97706",
    "critical":  "#DC2626",
    "text":      "#0F172A",
    "subtext":   "#64748B",
    "muted":     "#94A3B8",
    "bg":        "#FFFFFF",
    "card_bg":   "#F1F5F9",
    "grid":      "#E2E8F0",
    "light_bg":  "#F8FAFC",
}

STATUS_COLORS = {
    "green":  PALETTE["positive"],
    "yellow": PALETTE["warning"],
    "red":    PALETTE["critical"],
    "grey":   PALETTE["muted"],
}

# Cycle for multi-series charts
SERIES_COLORS = [
    "#DC2626", "#D97706", "#2563EB", "#059669", "#7C3AED", "#DB2777",
    "#0891B2", "#65A30D", "#EA580C", "#4F46E5",
]

DPI = 200
FONT_FAMILY = "sans-serif"


@dataclass
class ChartSpec:
    """Rendered chart ready for PPTX embedding."""
    chart_type: str
    title: str
    image: io.BytesIO
    width_inches: float
    height_inches: float


# ── Styling ─────────────────────────────────────────────────────────────────

def _style(fig, ax, title: str = ""):
    """Apply consistent professional styling to any chart."""
    fig.patch.set_facecolor("white")
    if ax is not None:
        ax.set_facecolor("white")
        ax.grid(True, alpha=0.25, color=PALETTE["grid"], linewidth=0.5)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color(PALETTE["grid"])
        ax.spines["bottom"].set_color(PALETTE["grid"])
        ax.tick_params(colors=PALETTE["subtext"], labelsize=8)
    if title:
        fig.suptitle(title, fontsize=13, fontweight="bold",
                     color=PALETTE["text"], y=0.98, x=0.05, ha="left")


def _save(fig, width: float = 6.0, height: float = 3.5) -> io.BytesIO:
    """Save figure to BytesIO PNG, close it, return seeked buffer."""
    buf = io.BytesIO()
    try:
        fig.savefig(buf, format="png", dpi=DPI, bbox_inches="tight",
                    facecolor="white", edgecolor="none")
    finally:
        plt.close(fig)
    buf.seek(0)
    return buf


def _fmt_val(val, unit: str = "") -> str:
    """Format a value with its unit for chart labels."""
    if val is None:
        return "—"
    u = (unit or "").lower()
    if u in ("pct", "%"):
        return f"{val:.1f}%"
    if u in ("usd", "$"):
        if abs(val) >= 1_000_000:
            return f"${val / 1_000_000:.1f}M"
        if abs(val) >= 1_000:
            return f"${val / 1_000:.0f}K"
        return f"${val:,.0f}"
    if u == "days":
        return f"{val:.0f}d"
    if u == "months":
        return f"{val:.1f}mo"
    if u in ("ratio", "x"):
        return f"{val:.2f}x"
    return f"{val:.1f}"


# ── Chart Renderers ─────────────────────────────────────────────────────────

def render_line(
    periods: list[str],
    values: list[float],
    target: Optional[float],
    name: str,
    unit: str = "",
    status_color: str = PALETTE["accent"],
) -> ChartSpec:
    """Single KPI trend line with area fill and target reference."""
    fig, ax = plt.subplots(figsize=(6.5, 3.2))
    _style(fig, ax)

    valid_idx = [i for i, v in enumerate(values) if v is not None]
    if not valid_idx:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center",
                transform=ax.transAxes, fontsize=11, color=PALETTE["muted"])
        return ChartSpec("line", name, _save(fig, 6.5, 3.2), 6.5, 3.2)

    x = list(range(len(periods)))
    ax.plot(x, values, color=status_color, linewidth=2.2, marker="o",
            markersize=4, zorder=3)
    ax.fill_between(x, values, alpha=0.08, color=status_color)

    if target is not None:
        ax.axhline(y=target, color=PALETTE["muted"], linestyle="--",
                   linewidth=1.2, alpha=0.7)
        ax.text(len(periods) - 0.5, target, f"Target: {_fmt_val(target, unit)}",
                fontsize=7, color=PALETTE["subtext"], va="bottom")

    # Annotate last value
    last_v = values[-1] if values[-1] is not None else values[valid_idx[-1]]
    last_i = len(values) - 1 if values[-1] is not None else valid_idx[-1]
    ax.annotate(_fmt_val(last_v, unit), (last_i, last_v),
                textcoords="offset points", xytext=(8, 8),
                fontsize=9, fontweight="bold", color=status_color)

    # X-axis labels
    step = max(1, len(periods) // 10)
    ax.set_xticks(x[::step])
    ax.set_xticklabels([periods[i] for i in range(0, len(periods), step)],
                       fontsize=7, rotation=30)

    ax.set_title(name, fontsize=11, fontweight="bold", color=PALETTE["text"],
                 loc="left", pad=10)
    fig.tight_layout()
    return ChartSpec("line", name, _save(fig, 6.5, 3.2), 6.5, 3.2)


def render_multi_line(
    periods: list[str],
    series_dict: dict[str, list[float]],
    title: str,
    targets: Optional[dict[str, float]] = None,
) -> ChartSpec:
    """Multiple KPI trends overlaid (max 6 series)."""
    fig, ax = plt.subplots(figsize=(7.0, 4.0))
    _style(fig, ax)

    x = list(range(len(periods)))
    targets = targets or {}
    plotted = 0

    for i, (name, vals) in enumerate(list(series_dict.items())[:6]):
        color = SERIES_COLORS[i % len(SERIES_COLORS)]
        ax.plot(x, vals, color=color, linewidth=2, marker="o",
                markersize=3, label=name, zorder=3)
        tgt = targets.get(name)
        if tgt is not None:
            ax.axhline(y=tgt, color=color, linestyle="--", alpha=0.3, linewidth=1)
        plotted += 1

    if plotted == 0:
        ax.text(0.5, 0.5, "No data", ha="center", va="center",
                transform=ax.transAxes, fontsize=11, color=PALETTE["muted"])

    step = max(1, len(periods) // 10)
    ax.set_xticks(x[::step])
    ax.set_xticklabels([periods[i] for i in range(0, len(periods), step)],
                       fontsize=7, rotation=30)
    ax.legend(fontsize=8, loc="upper left", framealpha=0.9, fancybox=True)
    ax.set_title(title, fontsize=11, fontweight="bold", color=PALETTE["text"],
                 loc="left", pad=10)
    fig.tight_layout()
    return ChartSpec("multi_line", title, _save(fig, 7.0, 4.0), 7.0, 4.0)


def render_donut(
    labels: list[str],
    values: list[int],
    colors: list[str],
    center_text: str = "",
    center_sub: str = "",
) -> ChartSpec:
    """Donut chart for status distribution or composition."""
    fig, ax = plt.subplots(figsize=(4.0, 4.0))
    fig.patch.set_facecolor("white")

    # Filter out zeros
    filtered = [(l, v, c) for l, v, c in zip(labels, values, colors) if v > 0]
    if not filtered:
        ax.text(0.5, 0.5, "No data", ha="center", va="center",
                transform=ax.transAxes, fontsize=11, color=PALETTE["muted"])
        return ChartSpec("donut", "", _save(fig, 4.0, 4.0), 4.0, 4.0)

    f_labels, f_values, f_colors = zip(*filtered)
    wedges, texts, autotexts = ax.pie(
        f_values, colors=f_colors, labels=f_labels,
        autopct="%1.0f%%", startangle=90, pctdistance=0.78,
        textprops={"fontsize": 9, "color": PALETTE["text"]},
    )
    for at in autotexts:
        at.set_fontsize(10)
        at.set_fontweight("bold")
        at.set_color("white")

    centre = plt.Circle((0, 0), 0.55, fc="white")
    ax.add_artist(centre)
    if center_text:
        ax.text(0, 0.06, center_text, ha="center", va="center",
                fontsize=24, fontweight="bold", color=PALETTE["text"])
    if center_sub:
        ax.text(0, -0.16, center_sub, ha="center", va="center",
                fontsize=9, color=PALETTE["subtext"])

    fig.tight_layout()
    return ChartSpec("donut", center_text, _save(fig, 4.0, 4.0), 4.0, 4.0)


def render_bar_h(
    names: list[str],
    values: list[float],
    colors: list[str],
    title: str,
    unit: str = "",
) -> ChartSpec:
    """Horizontal bar chart for ranking / comparison."""
    n = min(len(names), 12)
    height = max(n * 0.45 + 1.0, 2.5)
    fig, ax = plt.subplots(figsize=(7.0, height))
    _style(fig, ax)

    y_pos = list(range(n))
    ax.barh(y_pos, values[:n], height=0.55, color=colors[:n], alpha=0.9, zorder=3)

    # Value annotations
    for i in range(n):
        ax.text(values[i] + max(values[:n]) * 0.02, i,
                _fmt_val(values[i], unit), va="center",
                fontsize=8, color=PALETTE["text"], fontweight="bold")

    ax.set_yticks(y_pos)
    ax.set_yticklabels([n[:30] for n in names[:n]], fontsize=8)
    ax.invert_yaxis()
    ax.set_title(title, fontsize=11, fontweight="bold", color=PALETTE["text"],
                 loc="left", pad=10)
    ax.spines["left"].set_visible(False)
    fig.tight_layout()
    return ChartSpec("bar_h", title, _save(fig, 7.0, height), 7.0, height)


def render_grouped_bar_h(
    names: list[str],
    company_vals: list[float],
    peer_vals: list[float],
    bar_colors: list[str],
    title: str,
    peer_label: str = "Peer Median",
    unit: str = "",
) -> ChartSpec:
    """Grouped horizontal bar: company vs peer benchmark."""
    n = min(len(names), 10)
    height = max(n * 0.7 + 1.2, 3.0)
    fig, ax = plt.subplots(figsize=(7.5, height))
    _style(fig, ax)

    y_pos = np.arange(n)
    bar_h = 0.32
    ax.barh(y_pos - bar_h / 2, company_vals[:n], height=bar_h,
            color=bar_colors[:n], label="Company", alpha=0.9, zorder=3)
    ax.barh(y_pos + bar_h / 2, peer_vals[:n], height=bar_h,
            color=PALETTE["muted"], label=peer_label, alpha=0.5, zorder=2)

    ax.set_yticks(y_pos)
    ax.set_yticklabels([n[:28] for n in names[:n]], fontsize=8)
    ax.invert_yaxis()
    ax.legend(fontsize=8, loc="lower right", framealpha=0.9)
    ax.set_title(title, fontsize=11, fontweight="bold", color=PALETTE["text"],
                 loc="left", pad=10)
    fig.tight_layout()
    return ChartSpec("grouped_bar_h", title, _save(fig, 7.5, height), 7.5, height)


def render_heatmap(
    kpi_names: list[str],
    month_labels: list[str],
    status_matrix: list[list[str]],
    value_matrix: Optional[list[list]] = None,
) -> ChartSpec:
    """KPI × month status heatmap. status_matrix[row][col] = 'green'|'yellow'|'red'|'grey'."""
    n_kpis = len(kpi_names)
    n_months = len(month_labels)
    if n_kpis == 0 or n_months == 0:
        fig, ax = plt.subplots(figsize=(6, 2))
        ax.text(0.5, 0.5, "No data", ha="center", va="center",
                transform=ax.transAxes, fontsize=11, color=PALETTE["muted"])
        return ChartSpec("heatmap", "", _save(fig, 6, 2), 6, 2)

    cell_w, cell_h = 0.7, 0.4
    fig_w = max(n_months * cell_w + 2.5, 6)
    fig_h = max(n_kpis * cell_h + 1.5, 3)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    color_map = {
        "green": "#059669", "yellow": "#D97706",
        "red": "#DC2626", "grey": "#CBD5E1",
    }

    for row in range(n_kpis):
        for col in range(n_months):
            status = status_matrix[row][col] if row < len(status_matrix) and col < len(status_matrix[row]) else "grey"
            fc = color_map.get(status, "#CBD5E1")
            rect = mpatches.FancyBboxPatch(
                (col, n_kpis - 1 - row), 0.85, 0.75,
                boxstyle="round,pad=0.05", facecolor=fc, edgecolor="white",
                linewidth=1.5,
            )
            ax.add_patch(rect)

            # Show value inside cell if available
            if value_matrix and row < len(value_matrix) and col < len(value_matrix[row]):
                v = value_matrix[row][col]
                if v is not None:
                    txt_color = "white" if status in ("red", "green") else "#1E293B"
                    ax.text(col + 0.425, n_kpis - 1 - row + 0.375,
                            f"{v:.0f}" if abs(v) >= 10 else f"{v:.1f}",
                            ha="center", va="center", fontsize=6,
                            color=txt_color, fontweight="bold")

    ax.set_xlim(-0.1, n_months)
    ax.set_ylim(-0.1, n_kpis)
    ax.set_xticks([i + 0.425 for i in range(n_months)])
    ax.set_xticklabels(month_labels, fontsize=7, rotation=45, ha="right")
    ax.set_yticks([n_kpis - 1 - i + 0.375 for i in range(n_kpis)])
    ax.set_yticklabels([n[:25] for n in kpi_names], fontsize=7)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_visible(False)
    ax.tick_params(length=0)

    fig.tight_layout()
    return ChartSpec("heatmap", "KPI Status", _save(fig, fig_w, fig_h), fig_w, fig_h)


def render_radar(
    dimensions: list[str],
    actual_values: list[float],
    target_values: Optional[list[float]] = None,
    title: str = "Domain Health",
) -> ChartSpec:
    """Radar / spider chart for multi-dimensional comparison."""
    n = len(dimensions)
    if n < 3:
        fig, ax = plt.subplots(figsize=(4.5, 4.5))
        ax.text(0.5, 0.5, "Need 3+ dimensions", ha="center", va="center",
                transform=ax.transAxes, fontsize=11, color=PALETTE["muted"])
        return ChartSpec("radar", title, _save(fig, 4.5, 4.5), 4.5, 4.5)

    angles = np.linspace(0, 2 * np.pi, n, endpoint=False).tolist()
    angles += angles[:1]
    actual = actual_values + actual_values[:1]

    fig, ax = plt.subplots(figsize=(4.5, 4.5), subplot_kw=dict(polar=True))
    fig.patch.set_facecolor("white")

    ax.plot(angles, actual, "o-", color=PALETTE["accent"], linewidth=2,
            markersize=5, label="Actual", zorder=3)
    ax.fill(angles, actual, alpha=0.15, color=PALETTE["accent"])

    if target_values:
        tgt = target_values + target_values[:1]
        ax.plot(angles, tgt, "--", color=PALETTE["muted"], linewidth=1.5,
                label="Target", zorder=2)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(dimensions, fontsize=8, color=PALETTE["text"])
    ax.set_title(title, fontsize=11, fontweight="bold", color=PALETTE["text"],
                 pad=20)
    ax.legend(fontsize=8, loc="upper right", bbox_to_anchor=(1.2, 1.1))
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    return ChartSpec("radar", title, _save(fig, 4.5, 4.5), 4.5, 4.5)


def render_waterfall(
    labels: list[str],
    deltas: list[float],
    title: str = "Period Comparison",
) -> ChartSpec:
    """Waterfall chart showing incremental changes."""
    n = len(labels)
    if n == 0:
        fig, ax = plt.subplots(figsize=(7, 3.5))
        ax.text(0.5, 0.5, "No comparison data", ha="center", va="center",
                transform=ax.transAxes, fontsize=11, color=PALETTE["muted"])
        return ChartSpec("waterfall", title, _save(fig, 7, 3.5), 7, 3.5)

    fig, ax = plt.subplots(figsize=(7.0, 3.5))
    _style(fig, ax)

    cumulative = 0
    bottoms = []
    heights = []
    colors = []
    for d in deltas:
        bottoms.append(cumulative if d >= 0 else cumulative + d)
        heights.append(abs(d))
        colors.append(PALETTE["positive"] if d >= 0 else PALETTE["critical"])
        cumulative += d

    x = list(range(n))
    ax.bar(x, heights, bottom=bottoms, color=colors, width=0.6, zorder=3, alpha=0.9)

    # Annotations
    running = 0
    for i, d in enumerate(deltas):
        running += d
        sign = "+" if d >= 0 else ""
        ax.text(i, running + max(abs(d) for d in deltas) * 0.05,
                f"{sign}{d:.1f}", ha="center", fontsize=7,
                fontweight="bold", color=colors[i])

    # Connectors
    running = 0
    for i in range(n - 1):
        running += deltas[i]
        ax.plot([i + 0.35, i + 0.65], [running, running],
                color=PALETTE["muted"], linewidth=0.8, linestyle=":")

    ax.set_xticks(x)
    ax.set_xticklabels([l[:20] for l in labels], fontsize=7, rotation=30, ha="right")
    ax.axhline(y=0, color=PALETTE["grid"], linewidth=0.8)
    ax.set_title(title, fontsize=11, fontweight="bold", color=PALETTE["text"],
                 loc="left", pad=10)
    fig.tight_layout()
    return ChartSpec("waterfall", title, _save(fig, 7.0, 3.5), 7.0, 3.5)


# ── Intelligent Chart Selection ─────────────────────────────────────────────

def select_chart_for_context(context: str, **kwargs) -> str:
    """Given a slide context, return the best chart type.

    Contexts:
      - "status_distribution"     → donut
      - "kpi_trend_single"        → line
      - "kpi_trend_multi"         → multi_line
      - "kpi_ranking"             → bar_h
      - "status_grid"             → heatmap
      - "domain_health"           → radar
      - "period_comparison"       → waterfall
      - "benchmark_comparison"    → grouped_bar_h

    Falls back to "bar_h" for unknown contexts.
    """
    mapping = {
        "status_distribution":  "donut",
        "kpi_trend_single":     "line",
        "kpi_trend_multi":      "multi_line",
        "kpi_ranking":          "bar_h",
        "status_grid":          "heatmap",
        "domain_health":        "radar",
        "period_comparison":    "waterfall",
        "benchmark_comparison": "grouped_bar_h",
    }
    return mapping.get(context, "bar_h")
