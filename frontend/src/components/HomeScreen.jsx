import { useState, useEffect, useRef } from 'react'
import axios from 'axios'
import {
  TrendingUp, TrendingDown, Minus, AlertTriangle,
  CheckCircle2, Zap, ArrowRight, RefreshCw,
  Activity, Target, Shield, BarChart2,
  X, ChevronRight, Info, Clock, Eye
} from 'lucide-react'

// ── KPI contextual info dictionary ───────────────────────────────────────────
const KPI_INFO = {
  revenue_growth: {
    what: 'Month-over-month or year-over-year percentage change in total revenue.',
    why:  'The primary indicator of business momentum — investors and boards use this as the headline growth signal.',
    how:  'Computed as (current period revenue − prior period revenue) / prior period revenue × 100.',
    tab:  'trends',
  },
  gross_margin: {
    what: 'Revenue minus cost of goods sold, expressed as a percentage of revenue.',
    why:  'Shows how efficiently you deliver your product. SaaS companies typically target 70–80%.',
    how:  'Computed as (Revenue − COGS) / Revenue × 100. Sourced from your monthly P&L upload.',
    tab:  'variance',
  },
  net_revenue_retention: {
    what: 'Percentage of recurring revenue retained from existing customers, including expansions.',
    why:  'NRR > 100% means your existing base grows on its own — the gold standard for SaaS.',
    how:  'Computed as (Starting MRR − Churn + Expansion) / Starting MRR × 100.',
    tab:  'variance',
  },
  logo_churn_rate: {
    what: 'Percentage of customers who cancelled in a given period.',
    why:  'Customer churn erodes your installed base and signals product-market fit issues.',
    how:  'Computed as customers lost / customers at start of period × 100.',
    tab:  'variance',
  },
  arr: {
    what: 'Annualised Recurring Revenue — the predictable, contracted annual revenue run-rate.',
    why:  'The canonical SaaS valuation metric. ARR growth drives your company multiple.',
    how:  'MRR × 12, or sum of all active annual contract values.',
    tab:  'trends',
  },
  mrr: {
    what: 'Monthly Recurring Revenue — contracted, predictable monthly revenue.',
    why:  'The pulse of the business. Month-on-month MRR growth shows compounding momentum.',
    how:  'Sum of all active subscription monthly fees at end of period.',
    tab:  'trends',
  },
  cac: {
    what: 'Customer Acquisition Cost — total sales & marketing spend divided by new customers won.',
    why:  'High CAC relative to LTV signals inefficient go-to-market and unsustainable unit economics.',
    how:  'Total S&M spend in period / number of new customers acquired in that period.',
    tab:  'variance',
  },
  ltv: {
    what: 'Lifetime Value — projected total revenue from an average customer over their entire relationship.',
    why:  'LTV:CAC ratio (target ≥ 3:1) is a core efficiency metric used by investors.',
    how:  'ARPU / Churn Rate (simplified), or ARPU × Gross Margin / Churn Rate.',
    tab:  'variance',
  },
  ltv_cac_ratio: {
    what: 'Ratio of customer lifetime value to acquisition cost.',
    why:  'Below 3x signals go-to-market inefficiency; above 5x may mean under-investment in growth.',
    how:  'LTV / CAC. Industry benchmark: 3–5× for healthy SaaS.',
    tab:  'variance',
  },
  payback_period: {
    what: 'Months required to recoup the cost of acquiring a customer.',
    why:  'Shorter payback means faster capital recycling. Best-in-class is under 12 months.',
    how:  'CAC / (ARPU × Gross Margin %). Sourced from your CAC and margin inputs.',
    tab:  'variance',
  },
  magic_number: {
    what: 'Sales efficiency metric: net new ARR generated per dollar of S&M spend.',
    why:  'Magic Number ≥ 0.75 indicates efficient growth; < 0.5 signals go-to-market issues.',
    how:  'Net New ARR / Prior Quarter S&M Spend.',
    tab:  'variance',
  },
  burn_multiple: {
    what: 'Net cash burned per dollar of net new ARR added.',
    why:  'Lower is better. > 2× is a warning sign; the best companies operate at < 1×.',
    how:  'Net Burn / Net New ARR. Sourced from your cash flow and ARR data.',
    tab:  'variance',
  },
  runway_months: {
    what: 'Months of operating runway at current burn rate.',
    why:  'Critical risk indicator. < 12 months means fundraising must begin immediately.',
    how:  'Cash Balance / Monthly Net Burn.',
    tab:  'variance',
  },
  cash_burn: {
    what: 'Net cash consumed per month (operating + investing activities).',
    why:  'Directly determines runway. Rising burn with flat ARR signals efficiency problems.',
    how:  'Cash at start of period − Cash at end of period (net of financing).',
    tab:  'trends',
  },
  ebitda_margin: {
    what: 'Earnings before interest, taxes, depreciation and amortisation as a % of revenue.',
    why:  'Proxy for operating profitability. Negative is acceptable early; trend toward 20%+ at scale.',
    how:  'EBITDA / Revenue × 100. From your uploaded P&L data.',
    tab:  'variance',
  },
  operating_margin: {
    what: 'Operating income as a percentage of revenue.',
    why:  'Shows the core business profitability before financing decisions.',
    how:  '(Revenue − OPEX − COGS) / Revenue × 100.',
    tab:  'variance',
  },
  dau_mau_ratio: {
    what: 'Daily Active Users divided by Monthly Active Users — product stickiness.',
    why:  'Measures how often users return. 20%+ is decent; 50%+ is excellent (WhatsApp-level).',
    how:  'DAU / MAU × 100. Sourced from your product analytics upload.',
    tab:  'trends',
  },
  nps: {
    what: 'Net Promoter Score — customer sentiment measured on a −100 to +100 scale.',
    why:  'Strong leading indicator of retention and referral-driven growth.',
    how:  '% Promoters (9–10) − % Detractors (0–6). From customer survey data.',
    tab:  'trends',
  },
  sales_cycle_days: {
    what: 'Average days from first contact to closed-won deal.',
    why:  'Longer cycles compress win rates and slow ARR growth. Benchmark varies by ACV.',
    how:  'Average of (close date − first touch date) across closed-won deals.',
    tab:  'variance',
  },
  win_rate: {
    what: 'Percentage of sales opportunities that result in a closed-won deal.',
    why:  'Direct measure of GTM effectiveness and product-market fit.',
    how:  'Closed-won deals / Total deals entering final stage × 100.',
    tab:  'variance',
  },
}

// ── Circular Health Gauge (compact) ──────────────────────────────────────────
function HealthGauge({ score, color, size = 110 }) {
  const r = (size / 2) - 11
  const circ = 2 * Math.PI * r
  const progress = Math.max(0, Math.min(score, 100)) / 100 * circ
  const strokeColor = color === 'green' ? '#059669' : color === 'amber' ? '#D97706' : color === 'red' ? '#DC2626' : '#94a3b8'

  return (
    <svg width={size} height={size} className="transform -rotate-90 drop-shadow-sm flex-shrink-0">
      <circle cx={size/2} cy={size/2} r={r} fill="none" stroke="#E2E8F0" strokeWidth="9"/>
      <circle
        cx={size/2} cy={size/2} r={r}
        fill="none" stroke={strokeColor} strokeWidth="9" strokeLinecap="round"
        strokeDasharray={`${progress} ${circ}`}
        style={{ transition: 'stroke-dasharray 1.2s ease-in-out' }}
      />
    </svg>
  )
}

// ── Mini sparkline ────────────────────────────────────────────────────────────
function Sparkline({ data, color = '#059669', width = 64, height = 24 }) {
  if (!data || data.length < 2) return <div style={{ width, height }} />
  const nums = data.filter(v => v != null)
  if (nums.length < 2) return <div style={{ width, height }} />
  const min = Math.min(...nums)
  const max = Math.max(...nums)
  const range = max - min || 1
  const pts = nums.map((v, i) => {
    const x = (i / (nums.length - 1)) * width
    const y = height - 2 - ((v - min) / range) * (height - 6)
    return `${x},${y}`
  }).join(' ')
  return (
    <svg width={width} height={height} className="flex-shrink-0">
      <polyline points={pts} fill="none" stroke={color} strokeWidth="1.5"
        strokeLinecap="round" strokeLinejoin="round"/>
    </svg>
  )
}

// ── KPI Slide-out drawer ──────────────────────────────────────────────────────
function KpiSlideOut({ kpi, status, onClose, onNavigate }) {
  const info = KPI_INFO[kpi?.key] || {}
  const label = (kpi?.key || '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
  const avg    = kpi?.avg  != null ? `${kpi.avg}${kpi.unit || ''}` : '—'
  const target = kpi?.target != null ? `${kpi.target}${kpi.unit || ''}` : 'Not set'
  const sparkColor = status === 'green' ? '#059669' : status === 'red' ? '#DC2626' : '#D97706'
  const statusColors = {
    red:   { pill: 'bg-red-100 text-red-700',     label: 'Below Target' },
    amber: { pill: 'bg-amber-100 text-amber-700', label: 'Watch Zone'   },
    green: { pill: 'bg-emerald-100 text-emerald-700', label: 'On Target' },
  }
  const sc = statusColors[status] || { pill: 'bg-slate-100 text-slate-600', label: 'No Target' }

  const gapPct = (kpi?.avg != null && kpi?.target)
    ? (kpi.direction === 'higher'
        ? ((kpi.avg / kpi.target - 1) * 100).toFixed(1)
        : ((kpi.target / kpi.avg - 1) * 100).toFixed(1))
    : null

  const narrative = () => {
    if (!kpi?.avg || !kpi?.target) return `No target has been set for ${label}. Add a target in Settings to track performance.`
    if (status === 'green') return `${label} is performing at ${avg} against a target of ${target} — ${gapPct > 0 ? `${gapPct}% above target` : 'on track'}. This is a positive signal for your business health.`
    if (status === 'red') return `${label} is at ${avg}, which is ${Math.abs(gapPct)}% ${kpi.direction === 'higher' ? 'below' : 'above'} the target of ${target}. This is dragging down your health score and needs attention.`
    return `${label} is at ${avg} against a target of ${target} — within watch range. Monitor closely over the next 1–2 months.`
  }

  return (
    <div className="fixed inset-0 z-50 flex justify-end" onClick={onClose}>
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/20 backdrop-blur-[1px]" />
      {/* Drawer */}
      <div
        className="relative bg-white w-[400px] h-full shadow-2xl flex flex-col overflow-hidden"
        style={{ animation: 'slideInRight 0.22s ease-out' }}
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-100">
          <div className="flex items-center gap-2">
            <span className={`text-[11px] font-semibold px-2 py-0.5 rounded-full ${sc.pill}`}>{sc.label}</span>
            <span className="text-slate-700 text-sm font-bold">{label}</span>
          </div>
          <button onClick={onClose} className="p-1 rounded-md hover:bg-slate-100 text-slate-400 hover:text-slate-600 transition-colors">
            <X size={15} />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-5">
          {/* Value + Sparkline */}
          <div className="flex items-end justify-between">
            <div>
              <div className="text-3xl font-extrabold text-slate-900">{avg}</div>
              <div className="text-slate-400 text-[11px] mt-0.5">6-month avg vs target: <span className="font-semibold text-slate-600">{target}</span></div>
              {gapPct !== null && (
                <div className={`text-xs font-bold mt-1 ${status === 'green' ? 'text-emerald-600' : status === 'red' ? 'text-red-600' : 'text-amber-600'}`}>
                  {gapPct > 0 ? '+' : ''}{gapPct}% vs target
                </div>
              )}
            </div>
            <Sparkline data={kpi?.sparkline} color={sparkColor} width={96} height={40} />
          </div>

          {/* Narrative */}
          <div className="bg-slate-50 rounded-xl p-4">
            <p className="text-slate-700 text-[12px] leading-relaxed">{narrative()}</p>
          </div>

          {/* What / Why / How */}
          {info.what && (
            <div className="space-y-3">
              <div>
                <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1">What is this?</p>
                <p className="text-slate-600 text-[12px] leading-relaxed">{info.what}</p>
              </div>
              <div>
                <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1">Why it matters</p>
                <p className="text-slate-600 text-[12px] leading-relaxed">{info.why}</p>
              </div>
              <div>
                <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1">How it's computed</p>
                <p className="text-slate-600 text-[12px] leading-relaxed font-mono bg-slate-50 px-3 py-2 rounded-lg">{info.how}</p>
              </div>
            </div>
          )}
          {!info.what && (
            <p className="text-slate-400 text-[12px]">No additional context available for this KPI.</p>
          )}
        </div>

        {/* Footer */}
        {info.tab && (
          <div className="px-5 py-4 border-t border-slate-100">
            <button
              onClick={() => { onNavigate?.(info.tab); onClose() }}
              className="w-full flex items-center justify-center gap-2 bg-[#0055A4] hover:bg-[#0046882] text-white text-[12px] font-semibold py-2.5 rounded-xl transition-colors"
            >
              Open Full Analysis <ArrowRight size={13} />
            </button>
          </div>
        )}
      </div>
    </div>
  )
}

// ── Score Breakdown Modal ─────────────────────────────────────────────────────
function ScoreBreakdownModal({ health, onClose }) {
  const score = health?.score ?? 0
  const color = health?.color ?? 'grey'
  const narrative = () => {
    const m = health?.momentum ?? 0
    const t = health?.target_achievement ?? 0
    const r = health?.risk_flags ?? 0
    const parts = []
    if (m >= 70) parts.push('strong momentum')
    else if (m < 50) parts.push('weak momentum')
    if (t >= 70) parts.push('healthy target achievement')
    else if (t < 40) parts.push('low target achievement')
    if (r < 40) parts.push('elevated risk flags')
    return parts.length
      ? `Your score of ${score} reflects ${parts.join(' and ')}. ${t < 50 ? 'Set more KPI targets to unlock the Target Achievement score.' : ''}`
      : `Your overall health score of ${score} reflects the weighted combination of momentum, target achievement, and risk factors below.`
  }
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="absolute inset-0 bg-black/30 backdrop-blur-[2px]" />
      <div
        className="relative bg-white rounded-2xl shadow-2xl w-full max-w-md p-6"
        onClick={e => e.stopPropagation()}
        style={{ animation: 'fadeInScale 0.18s ease-out' }}
      >
        <div className="flex items-center justify-between mb-4">
          <p className="text-slate-800 font-bold text-sm">How is the Health Score calculated?</p>
          <button onClick={onClose} className="p-1 rounded-md hover:bg-slate-100 text-slate-400"><X size={14}/></button>
        </div>
        <p className="text-slate-500 text-[12px] leading-relaxed mb-5">{narrative()}</p>
        <div className="space-y-4">
          {[
            { label: 'Momentum', value: health?.momentum ?? 0, weight: 30, Icon: Activity, desc: 'Measures how many KPIs are improving vs declining over the last 3 months.' },
            { label: 'Target Achievement', value: health?.target_achievement ?? 0, weight: 40, Icon: Target, desc: 'Percentage of KPIs with targets that are currently on track (green status).' },
            { label: 'Risk Score', value: health?.risk_flags ?? 0, weight: 30, Icon: Shield, desc: 'Inverse of risk — penalises for KPIs in critical/red status and negative momentum.' },
          ].map(({ label, value, weight, Icon, desc }) => {
            const c = value >= 70 ? '#059669' : value >= 50 ? '#D97706' : '#DC2626'
            return (
              <div key={label}>
                <div className="flex items-center gap-2 mb-1">
                  <Icon size={13} style={{ color: c }} />
                  <span className="text-slate-700 text-[12px] font-semibold flex-1">{label}</span>
                  <span className="text-slate-400 text-[11px]">{weight}% weight</span>
                  <span className="text-[12px] font-bold" style={{ color: c }}>{value.toFixed(0)}</span>
                </div>
                <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden mb-1.5">
                  <div className="h-full rounded-full transition-all duration-700" style={{ width: `${value}%`, backgroundColor: c }} />
                </div>
                <p className="text-slate-400 text-[11px] leading-snug">{desc}</p>
              </div>
            )
          })}
        </div>
        <div className="mt-5 bg-slate-50 rounded-xl p-3">
          <p className="text-slate-500 text-[11px]">
            <span className="font-semibold text-slate-600">Formula: </span>
            Score = (Momentum × 0.30) + (Target Achievement × 0.40) + (Risk × 0.30)
          </p>
        </div>
      </div>
    </div>
  )
}

// ── Distribution Modal ────────────────────────────────────────────────────────
function DistributionModal({ health, onClose, onNavigate }) {
  const items = [
    { count: health?.kpis_green,  label: 'On Target', color: '#059669', bg: 'bg-emerald-50', text: 'text-emerald-700', desc: 'These KPIs are meeting or exceeding their targets. Keep monitoring for sustained performance.' },
    { count: health?.kpis_yellow, label: 'Watch',     color: '#D97706', bg: 'bg-amber-50',   text: 'text-amber-700',   desc: 'These KPIs are close to target but trending in the wrong direction. Early intervention recommended.' },
    { count: health?.kpis_red,    label: 'Critical',  color: '#DC2626', bg: 'bg-red-50',     text: 'text-red-700',     desc: 'These KPIs are significantly below target. Immediate review and action required.' },
    { count: health?.kpis_grey,   label: 'No Target', color: '#94a3b8', bg: 'bg-slate-50',   text: 'text-slate-600',   desc: 'No target is set. Go to Settings → Targets to configure benchmarks for accurate scoring.' },
  ]
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="absolute inset-0 bg-black/30 backdrop-blur-[2px]" />
      <div
        className="relative bg-white rounded-2xl shadow-2xl w-full max-w-md p-6"
        onClick={e => e.stopPropagation()}
        style={{ animation: 'fadeInScale 0.18s ease-out' }}
      >
        <div className="flex items-center justify-between mb-5">
          <p className="text-slate-800 font-bold text-sm">KPI Distribution</p>
          <button onClick={onClose} className="p-1 rounded-md hover:bg-slate-100 text-slate-400"><X size={14}/></button>
        </div>
        <div className="space-y-3">
          {items.map(({ count, label, color, bg, text, desc }) => (
            <div key={label} className={`flex items-start gap-3 ${bg} rounded-xl p-3`}>
              <div className="text-2xl font-extrabold flex-shrink-0 leading-none mt-0.5" style={{ color }}>{count ?? 0}</div>
              <div>
                <p className={`text-[12px] font-semibold ${text} mb-0.5`}>{label}</p>
                <p className="text-slate-500 text-[11px] leading-snug">{desc}</p>
              </div>
            </div>
          ))}
        </div>
        <button
          onClick={() => { onNavigate?.('variance'); onClose() }}
          className="mt-5 w-full flex items-center justify-center gap-2 border border-slate-200 hover:border-slate-300 text-slate-600 text-[12px] font-semibold py-2.5 rounded-xl transition-colors"
        >
          View Variance Analysis <ArrowRight size={12}/>
        </button>
      </div>
    </div>
  )
}

// ── KPI Card (compact, clickable) ─────────────────────────────────────────────
function KpiCard({ kpi, status, onOpen }) {
  const s = {
    red:   { dot: '#DC2626', bg: 'bg-red-50',    border: 'border-red-200',    text: 'text-red-700',     hover: 'hover:border-red-300'    },
    amber: { bg: 'bg-amber-50',  border: 'border-amber-200',  text: 'text-amber-700',   hover: 'hover:border-amber-300'  },
    green: { bg: 'bg-emerald-50',border: 'border-emerald-200',text: 'text-emerald-700', hover: 'hover:border-emerald-300' },
  }[status] || { bg: 'bg-slate-50', border: 'border-slate-200', text: 'text-slate-500', hover: 'hover:border-slate-300' }

  const label = (kpi.key || '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
  const avg    = kpi.avg  != null ? `${kpi.avg}${kpi.unit || ''}` : '—'
  const target = kpi.target != null ? `${kpi.target}${kpi.unit || ''}` : '—'
  const gapPct = (kpi.avg != null && kpi.target)
    ? (kpi.direction === 'higher'
        ? ((kpi.avg / kpi.target - 1) * 100).toFixed(1)
        : ((kpi.target / kpi.avg - 1) * 100).toFixed(1))
    : null
  const sparkColor = status === 'green' ? '#059669' : status === 'red' ? '#DC2626' : '#D97706'

  return (
    <button
      onClick={() => onOpen?.(kpi, status)}
      className={`w-full text-left card p-3.5 ${s.bg} ${s.border} ${s.hover} hover:shadow-md transition-all group cursor-pointer`}
    >
      <div className="flex items-start justify-between gap-2 mb-1.5">
        <p className="text-slate-800 text-[12px] font-semibold leading-tight flex-1">{label}</p>
        <div className="flex items-center gap-1 flex-shrink-0">
          <Sparkline data={kpi.sparkline} color={sparkColor} width={56} height={22} />
          <ChevronRight size={12} className="text-slate-300 group-hover:text-slate-500 transition-colors" />
        </div>
      </div>
      <div className="flex items-center gap-1.5 flex-wrap">
        <span className="text-slate-900 text-base font-extrabold leading-none">{avg}</span>
        <span className="text-slate-400 text-[10px]">vs {target}</span>
        {gapPct !== null && (
          <span className={`text-[11px] font-bold ${s.text}`}>
            {gapPct > 0 ? '+' : ''}{gapPct}%
          </span>
        )}
      </div>
    </button>
  )
}

// ── Score component bar (compact, clickable) ──────────────────────────────────
function ScoreBar({ label, value, weight, Icon, onClick }) {
  const color = value >= 70 ? '#059669' : value >= 50 ? '#D97706' : '#DC2626'
  return (
    <button className="flex items-center gap-2.5 w-full text-left group" onClick={onClick}>
      <div className="w-5 h-5 rounded-md bg-slate-100 flex items-center justify-center flex-shrink-0 group-hover:bg-slate-200 transition-colors">
        <Icon size={11} className="text-slate-500" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center justify-between mb-1">
          <span className="text-slate-600 text-[11px] font-medium group-hover:text-slate-800 transition-colors">{label}</span>
          <div className="flex items-center gap-1.5">
            <span className="text-slate-400 text-[10px]">{weight}</span>
            <span className="text-[11px] font-bold" style={{ color }}>{value.toFixed(0)}</span>
          </div>
        </div>
        <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden">
          <div className="h-full rounded-full transition-all duration-1000" style={{ width: `${Math.round(value)}%`, backgroundColor: color }} />
        </div>
      </div>
    </button>
  )
}

// ── Health narrative ──────────────────────────────────────────────────────────
function healthNarrative(health) {
  if (!health) return ''
  const {
    score, color, momentum_trend,
    kpis_green = 0, kpis_yellow = 0, kpis_red = 0, kpis_grey = 0,
    target_achievement = 0, momentum = 0
  } = health

  const total = kpis_green + kpis_yellow + kpis_red + kpis_grey
  const tracked = kpis_green + kpis_yellow + kpis_red

  // Opening — score-based verdict
  const opener =
    score >= 80 ? `At ${score}/100 your business is in strong health.` :
    score >= 65 ? `At ${score}/100 your business is performing well overall.` :
    score >= 50 ? `At ${score}/100 there are areas of the business that need attention.` :
    score >= 35 ? `At ${score}/100 several KPIs are significantly off-target.` :
                  `At ${score}/100 urgent action is needed across multiple KPIs.`

  // Targets context
  const targetLine = kpis_grey > 3
    ? `${kpis_grey} of ${total} KPIs have no target — set them in KPI Targets to unlock a more accurate score.`
    : tracked > 0
      ? `Of ${tracked} tracked KPIs, ${kpis_green} are on target${kpis_red > 0 ? `, ${kpis_red} are critical` : ''}${kpis_yellow > 0 ? ` and ${kpis_yellow} need watching` : ''}.`
      : null

  // Momentum line
  const momLine =
    momentum_trend === 'improving' ? 'Momentum is building — more KPIs are improving than declining.' :
    momentum_trend === 'declining' ? 'Momentum is declining — act quickly to prevent further deterioration.' :
    'Momentum is stable with no strong directional trend.'

  // Action prompt
  const actionLine =
    kpis_red > 0 ? `Start with the ${kpis_red} critical KPI${kpis_red > 1 ? 's' : ''} below.` :
    kpis_grey > 3 ? 'Go to Settings → KPI Targets to configure benchmarks.' :
    kpis_yellow > 0 ? `Watch the ${kpis_yellow} amber KPI${kpis_yellow > 1 ? 's' : ''} closely this month.` :
    'All tracked KPIs are on target — maintain the discipline.'

  return [opener, targetLine, momLine, actionLine].filter(Boolean).join(' ')
}

// ── Format data period ────────────────────────────────────────────────────────
function formatPeriod(from, to) {
  if (!from && !to) return null
  if (from === to || !from) return to
  return `${from} – ${to}`
}

function formatUploadedAt(ts) {
  if (!ts) return null
  try {
    const d = new Date(ts)
    return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })
  } catch { return ts }
}

// ── Main component ────────────────────────────────────────────────────────────
export default function HomeScreen({ onNavigate, onAskAnika }) {
  const [data, setData]               = useState(null)
  const [loading, setLoading]         = useState(true)
  const [error, setError]             = useState(false)
  const [slideOut, setSlideOut]       = useState(null)
  const [showScoreModal, setShowScoreModal] = useState(false)
  const [showDistModal, setShowDistModal]   = useState(false)
  const [seeding, setSeeding]         = useState(false)

  const load = () => {
    setLoading(true); setError(false)
    axios.get('/api/home')
      .then(r  => { setData(r.data); setLoading(false) })
      .catch(() => { setError(true); setLoading(false) })
  }

  const loadDemoData = async () => {
    setSeeding(true)
    try {
      await axios.get('/api/seed-multiyear')
    } catch {}
    setSeeding(false)
    load()
  }

  useEffect(() => { load() }, [])

  if (loading) return (
    <div className="flex items-center justify-center h-64">
      <div className="w-7 h-7 rounded-full border-2 border-[#0055A4] border-t-transparent animate-spin"/>
    </div>
  )

  if (error || !data) return (
    <div className="flex flex-col items-center justify-center h-64 gap-3">
      <p className="text-slate-500 text-sm">Unable to load home screen data.</p>
      <button onClick={load} className="text-[12px] text-slate-400 hover:text-slate-600 flex items-center gap-1.5 transition-colors">
        <RefreshCw size={12}/> Retry
      </button>
    </div>
  )

  const { health, needs_attention, doing_well, data_period } = data
  const score    = health?.score ?? 0
  const color    = health?.color ?? 'grey'
  const scoreHex = color === 'green' ? '#059669' : color === 'amber' ? '#D97706' : color === 'red' ? '#DC2626' : '#94a3b8'

  const momentumConfig = {
    improving: { Icon: TrendingUp,   text: 'Improving',  style: 'text-emerald-600' },
    stable:    { Icon: Minus,        text: 'Stable',     style: 'text-slate-500'   },
    declining: { Icon: TrendingDown, text: 'Declining',  style: 'text-red-500'     },
  }
  const mc   = momentumConfig[health?.momentum_trend] || momentumConfig.stable
  const MIcon = mc.Icon

  const period   = formatPeriod(data_period?.from, data_period?.to)
  const uploadAt = formatUploadedAt(data_period?.uploaded_at)
  const narrative = healthNarrative(health)

  return (
    <div className="space-y-5 max-w-5xl">

      {/* ── Top meta bar ───────────────────────────────────────────────── */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-[11px] text-slate-400">
          {period && (
            <>
              <BarChart2 size={12} className="text-slate-300" />
              <span>Data period: <span className="font-semibold text-slate-500">{period}</span></span>
            </>
          )}
          {uploadAt && (
            <>
              <span className="text-slate-200">·</span>
              <Clock size={11} className="text-slate-300" />
              <span>Last uploaded: <span className="font-semibold text-slate-500">{uploadAt}</span></span>
            </>
          )}
          {!period && !uploadAt && (
            <span className="text-slate-400 italic">No data uploaded yet</span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={loadDemoData}
            disabled={seeding}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-[#0055A4] hover:bg-[#003d80] text-white text-[11px] font-semibold rounded-lg transition-colors disabled:opacity-60"
          >
            {seeding
              ? <><div className="w-2.5 h-2.5 rounded-full border-2 border-white/40 border-t-white animate-spin"/>Loading…</>
              : <><Zap size={11}/> Load Demo Data</>
            }
          </button>
          <button onClick={load} className="p-1.5 rounded-lg hover:bg-slate-100 text-slate-400 hover:text-slate-600 transition-colors" title="Refresh">
            <RefreshCw size={13} />
          </button>
        </div>
      </div>

      {/* ── Health Score Card ───────────────────────────────────────────── */}
      <div className="card p-5 shadow-sm hover:shadow-md transition-shadow">
        <div className="flex items-stretch gap-5">

          {/* Gauge column */}
          <div className="flex flex-col items-center justify-center flex-shrink-0 gap-2">
            <div className="relative">
              <HealthGauge score={score} color={color} size={110} />
              <div className="absolute inset-0 flex flex-col items-center justify-center">
                <span className="text-3xl font-extrabold text-slate-900 leading-none">{score}</span>
                <span className="text-slate-400 text-[10px]">/ 100</span>
              </div>
            </div>
            <div className="text-center">
              <p className="text-[12px] font-bold" style={{ color: scoreHex }}>{health?.label}</p>
              <p className="text-slate-400 text-[10px]">Health Score</p>
            </div>
            <div className={`flex items-center gap-1 text-[10px] font-semibold ${mc.style}`}>
              <MIcon size={11}/> {mc.text}
            </div>
          </div>

          {/* Divider */}
          <div className="w-px bg-slate-100 flex-shrink-0" />

          {/* Breakdown + Distribution column */}
          <div className="flex-1 min-w-0 flex flex-col justify-between gap-4">

            {/* Score breakdown */}
            <div>
              <div className="flex items-center gap-1.5 mb-2.5">
                <p className="text-slate-400 text-[10px] font-bold uppercase tracking-widest">Score Breakdown</p>
                <button onClick={() => setShowScoreModal(true)} className="text-slate-300 hover:text-slate-500 transition-colors" title="How is this calculated?">
                  <Info size={11} />
                </button>
                <span className="text-[9px] text-slate-300 ml-auto cursor-pointer hover:text-slate-400" onClick={() => setShowScoreModal(true)}>click to explain ↗</span>
              </div>
              <div className="space-y-2.5">
                <ScoreBar label="Momentum"           value={health?.momentum ?? 0}           weight="30%" Icon={Activity} onClick={() => setShowScoreModal(true)}/>
                <ScoreBar label="Target Achievement" value={health?.target_achievement ?? 0} weight="40%" Icon={Target}   onClick={() => setShowScoreModal(true)}/>
                <ScoreBar label="Risk Score"         value={health?.risk_flags ?? 0}         weight="30%" Icon={Shield}   onClick={() => setShowScoreModal(true)}/>
              </div>
            </div>

            {/* KPI Distribution */}
            <div>
              <div className="flex items-center gap-1.5 mb-2">
                <p className="text-slate-400 text-[10px] font-bold uppercase tracking-widest">KPI Distribution</p>
                <button onClick={() => setShowDistModal(true)} className="text-slate-300 hover:text-slate-500 transition-colors">
                  <Info size={11} />
                </button>
                <span className="text-[9px] text-slate-300 ml-auto cursor-pointer hover:text-slate-400" onClick={() => setShowDistModal(true)}>click to explore ↗</span>
              </div>
              <button onClick={() => setShowDistModal(true)} className="grid grid-cols-4 gap-2 w-full text-left hover:bg-slate-50 rounded-xl p-1.5 -mx-1.5 transition-colors group">
                {[
                  { count: health?.kpis_green,  label: 'On Target', color: '#059669' },
                  { count: health?.kpis_yellow, label: 'Watch',     color: '#D97706' },
                  { count: health?.kpis_red,    label: 'Critical',  color: '#DC2626' },
                  { count: health?.kpis_grey,   label: 'No Target', color: '#94a3b8' },
                ].map(({ count, label, color: c }) => (
                  <div key={label} className="text-center">
                    <div className="text-xl font-extrabold leading-tight" style={{ color: c }}>{count ?? 0}</div>
                    <div className="text-slate-400 text-[10px] font-medium leading-tight">{label}</div>
                  </div>
                ))}
              </button>
            </div>
          </div>

          {/* Right: narrative */}
          {narrative && (
            <>
              <div className="w-px bg-slate-100 flex-shrink-0 hidden lg:block" />
              <div className="hidden lg:flex flex-col justify-center max-w-[200px]">
                <p className="text-slate-500 text-[11px] leading-relaxed">{narrative}</p>
                <button
                  onClick={() => setShowScoreModal(true)}
                  className="mt-3 text-[10px] font-semibold text-[#0055A4] hover:underline self-start"
                >
                  How is this score computed? →
                </button>
              </div>
            </>
          )}
        </div>
      </div>

      {/* ── Needs Attention ────────────────────────────────────────────── */}
      {needs_attention?.length > 0 && (
        <div>
          <div className="flex items-center justify-between mb-2.5">
            <div className="flex items-center gap-2">
              <AlertTriangle size={13} className="text-red-500" />
              <h2 className="text-slate-700 text-[11px] font-bold uppercase tracking-wider">Needs Attention</h2>
              <span className="bg-red-100 text-red-700 text-[10px] font-bold px-1.5 py-0.5 rounded-full">{needs_attention.length}</span>
              <span className="text-[9px] text-slate-300 italic">— click any card to explore</span>
            </div>
            <button onClick={() => onNavigate?.('variance')}
              className="text-[11px] text-slate-400 hover:text-[#0055A4] flex items-center gap-1 transition-colors font-medium">
              Full Analysis <ArrowRight size={10}/>
            </button>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-2.5">
            {needs_attention.slice(0, 6).map(kpi => (
              <KpiCard key={kpi.key} kpi={kpi} status="red"
                onOpen={(k, s) => setSlideOut({ kpi: k, status: s })} />
            ))}
          </div>
        </div>
      )}

      {/* ── Doing Well ─────────────────────────────────────────────────── */}
      {doing_well?.length > 0 && (
        <div>
          <div className="flex items-center justify-between mb-2.5">
            <div className="flex items-center gap-2">
              <CheckCircle2 size={13} className="text-emerald-500" />
              <h2 className="text-slate-700 text-[11px] font-bold uppercase tracking-wider">Doing Well</h2>
              <span className="bg-emerald-100 text-emerald-700 text-[10px] font-bold px-1.5 py-0.5 rounded-full">{doing_well.length}</span>
              <span className="text-[9px] text-slate-300 italic">— click any card to explore</span>
            </div>
            <button onClick={() => onNavigate?.('board')}
              className="text-[11px] text-slate-400 hover:text-[#0055A4] flex items-center gap-1 transition-colors font-medium">
              Executive Brief <ArrowRight size={10}/>
            </button>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-2.5">
            {doing_well.slice(0, 6).map(kpi => (
              <KpiCard key={kpi.key} kpi={kpi} status="green"
                onOpen={(k, s) => setSlideOut({ kpi: k, status: s })} />
            ))}
          </div>
        </div>
      )}

      {/* ── No colored KPIs: either no data or no targets set ────────────── */}
      {(!needs_attention?.length && !doing_well?.length) && (
        health?.kpis_grey > 0
          ? /* Has data but no targets */ (
            <div className="card p-6 border-l-4 border-amber-400 bg-amber-50/30">
              <div className="flex items-start gap-4">
                <Target size={22} className="text-amber-500 flex-shrink-0 mt-0.5" />
                <div className="flex-1">
                  <p className="text-slate-700 text-[13px] font-bold mb-1">
                    {health.kpis_grey} KPI{health.kpis_grey !== 1 ? 's have' : ' has'} no target set
                  </p>
                  <p className="text-slate-500 text-[12px] leading-relaxed mb-3">
                    Your data is loaded but KPI targets are not configured. Without targets, all KPIs show as grey
                    and the Needs Attention / Doing Well sections stay empty. Set targets to unlock red/green status,
                    the full health score, clickable KPI cards, and Slack alerts.
                  </p>
                  <div className="flex items-center gap-2 flex-wrap">
                    <button
                      onClick={() => onNavigate?.('targets')}
                      className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-amber-500 hover:bg-amber-600 text-white text-[11px] font-semibold rounded-lg transition-colors"
                    >
                      Configure KPI Targets <ArrowRight size={11}/>
                    </button>
                    <button
                      onClick={loadDemoData}
                      disabled={seeding}
                      className="inline-flex items-center gap-1.5 px-3 py-1.5 border border-amber-400 text-amber-700 hover:bg-amber-100 text-[11px] font-semibold rounded-lg transition-colors disabled:opacity-60"
                    >
                      {seeding ? 'Loading…' : 'Or: Load Demo Data with targets'}
                    </button>
                  </div>
                </div>
              </div>
            </div>
          )
          : /* No data at all */ (
            <div className="card p-8 flex flex-col items-center gap-4 text-center">
              <BarChart2 size={28} className="text-slate-300" />
              <div>
                <p className="text-slate-600 text-sm font-semibold mb-1">No data yet</p>
                <p className="text-slate-400 text-[12px] max-w-sm">Upload your financial data or load 5 years of demo data (including KPI targets) to explore the full platform.</p>
              </div>
              <div className="flex items-center gap-3">
                <button
                  onClick={loadDemoData}
                  disabled={seeding}
                  className="flex items-center gap-2 px-4 py-2 bg-[#0055A4] hover:bg-[#003d80] text-white text-[12px] font-semibold rounded-lg transition-colors disabled:opacity-60"
                >
                  {seeding
                    ? <><div className="w-3 h-3 rounded-full border-2 border-white/40 border-t-white animate-spin"/>Loading…</>
                    : <>Load Demo Data (5 years)</>
                  }
                </button>
                <button onClick={() => onNavigate?.('upload')}
                  className="px-4 py-2 border border-slate-300 rounded-lg text-slate-600 hover:border-slate-400 text-[12px] font-medium transition-colors">
                  Upload CSV
                </button>
              </div>
            </div>
          )
      )}

      {/* ── Quick navigation ────────────────────────────────────────────── */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2.5">
        {[
          { label: 'Variance Analysis',   tab: 'variance',    desc: 'KPI gap deep-dive',      accent: '#0055A4' },
          { label: 'Performance Heatmap', tab: 'fingerprint', desc: '12-month pattern view',  accent: '#7c3aed' },
          { label: 'Trend Explorer',      tab: 'trends',      desc: 'Historical trend lines', accent: '#0891b2' },
          { label: 'Board Pack',          tab: 'board_pack',  desc: 'Executive presentation', accent: '#d97706' },
        ].map(({ label, tab, desc, accent }) => (
          <button key={tab} onClick={() => onNavigate?.(tab)}
            className="card text-left p-3.5 hover:shadow-md transition-shadow group shadow-sm">
            <div className="w-1.5 h-1.5 rounded-full mb-1.5" style={{ backgroundColor: accent }}/>
            <p className="text-slate-800 text-[12px] font-semibold group-hover:text-[#0055A4] transition-colors leading-tight">{label}</p>
            <p className="text-slate-400 text-[10px] mt-0.5">{desc}</p>
          </button>
        ))}
      </div>

      {/* ── Modals & Drawers ────────────────────────────────────────────── */}
      {slideOut && (
        <KpiSlideOut
          kpi={slideOut.kpi}
          status={slideOut.status}
          onClose={() => setSlideOut(null)}
          onNavigate={onNavigate}
        />
      )}
      {showScoreModal && (
        <ScoreBreakdownModal health={health} onClose={() => setShowScoreModal(false)} />
      )}
      {showDistModal && (
        <DistributionModal health={health} onClose={() => setShowDistModal(false)} onNavigate={onNavigate} />
      )}

      {/* ── CSS animations ─────────────────────────────────────────────── */}
      <style>{`
        @keyframes slideInRight {
          from { transform: translateX(100%); opacity: 0; }
          to   { transform: translateX(0);    opacity: 1; }
        }
        @keyframes fadeInScale {
          from { transform: scale(0.95); opacity: 0; }
          to   { transform: scale(1);    opacity: 1; }
        }
      `}</style>
    </div>
  )
}
