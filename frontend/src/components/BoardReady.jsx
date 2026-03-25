import { useState, useMemo } from 'react'
import {
  RadarChart, PolarGrid, PolarAngleAxis, Radar, ResponsiveContainer,
  LineChart, Line, AreaChart, Area, ReferenceLine, Tooltip,
} from 'recharts'
import {
  ChevronRight, Printer, TrendingUp, TrendingDown, Minus,
  AlertTriangle, CheckCircle2, Activity, Zap, Eye, Shield,
  Target, AlertCircle, BarChart3, Layers, ArrowUpRight,
  ArrowDownRight, Info, X, Maximize2, ExternalLink,
} from 'lucide-react'

// ── Slide-in animation ──────────────────────────────────────────────────────
const PANEL_ANIM = `
@keyframes slideInRight {
  from { transform: translateX(100%); opacity: 0; }
  to   { transform: translateX(0);    opacity: 1; }
}
`

// ── Constants ───────────────────────────────────────────────────────────────
const MONTHS     = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
const MONTH_NUMS = [1,2,3,4,5,6,7,8,9,10,11,12]

// ── Period presets ───────────────────────────────────────────────────────────
const PERIOD_PRESETS = [
  { id: 'fy', label: 'Full Year', months: [1,2,3,4,5,6,7,8,9,10,11,12] },
  { id: 'h1', label: 'H1',        months: [1,2,3,4,5,6] },
  { id: 'h2', label: 'H2',        months: [7,8,9,10,11,12] },
  { id: 'q1', label: 'Q1',        months: [1,2,3] },
  { id: 'q2', label: 'Q2',        months: [4,5,6] },
  { id: 'q3', label: 'Q3',        months: [7,8,9] },
  { id: 'q4', label: 'Q4',        months: [10,11,12] },
  { id: 'r3', label: 'Last 3M',   months: [10,11,12] },
  { id: 'r6', label: 'Last 6M',   months: [7,8,9,10,11,12] },
]

// ── Period-aware fingerprint view ─────────────────────────────────────────────
function applyPeriod(fingerprint, monthNums) {
  if (!monthNums || monthNums.length === 12) return fingerprint
  return fingerprint.map(kpi => {
    const hits = (kpi.monthly || []).filter(m => {
      const mo = parseInt(m.period.split('-')[1], 10)
      return monthNums.includes(mo) && m.value != null
    })
    const periodAvg = hits.length
      ? hits.reduce((s, m) => s + m.value, 0) / hits.length
      : kpi.avg
    const newStatus = cellStatus(periodAvg, kpi.target, kpi.direction)
    return { ...kpi, avg: periodAvg, fy_status: newStatus }
  })
}

// ── Period label ──────────────────────────────────────────────────────────────
function periodLabel(preset) {
  if (!preset || preset.id === 'fy') return 'Full Year'
  return preset.label
}

const SOURCE = {
  dashboard:   { label: 'Command Center',  color: '#0055A4', text: 'text-blue-700',    bg: 'bg-blue-50',    border: 'border-blue-200'    },
  fingerprint: { label: 'Org Fingerprint', color: '#7c3aed', text: 'text-violet-700',  bg: 'bg-violet-50',  border: 'border-violet-200'  },
  trends:      { label: 'Monthly Trends',  color: '#059669', text: 'text-emerald-700', bg: 'bg-emerald-50', border: 'border-emerald-200' },
  projection:  { label: 'Bridge Analysis', color: '#d97706', text: 'text-amber-700',   bg: 'bg-amber-50',   border: 'border-amber-200'   },
}

const DOMAIN_MAP = {
  growth:      ['revenue', 'arr', 'mrr', 'growth', 'cac', 'ltv', 'pipeline', 'deal', 'win_rate', 'new_'],
  retention:   ['nrr', 'churn', 'retention', 'activation', 'nps', 'satisfaction', 'health', 'adoption', 'time_to_value', 'ttv'],
  efficiency:  ['margin', 'burn', 'sales_cycle', 'payback', 'magic_number', 'rule_of_40', 'opex', 'cogs'],
  cashflow:    ['cash', 'runway', 'fcf', 'free_cash', 'operating_cash'],
}
const DOMAIN_META = {
  growth:     { label: 'Growth Engine',         color: '#0055A4', bg: '#eff6ff', Icon: TrendingUp   },
  retention:  { label: 'Retention Health',       color: '#059669', bg: '#f0fdf4', Icon: Shield       },
  efficiency: { label: 'Operating Efficiency',   color: '#7c3aed', bg: '#f5f3ff', Icon: Zap          },
  cashflow:   { label: 'Cash & Runway',          color: '#d97706', bg: '#fffbeb', Icon: BarChart3    },
  other:      { label: 'Other Metrics',          color: '#64748b', bg: '#f8fafc', Icon: Activity     },
}

function getDomain(kpi) {
  const k = ((kpi.key || '') + ' ' + (kpi.name || '')).toLowerCase()
  for (const [domain, keywords] of Object.entries(DOMAIN_MAP)) {
    if (keywords.some(w => k.includes(w))) return domain
  }
  return 'other'
}

// ── Formatters ──────────────────────────────────────────────────────────────
const UNIT_FMT = {
  pct:    v => `${v?.toFixed(1)}%`,
  days:   v => `${v?.toFixed(1)}d`,
  months: v => `${v?.toFixed(1)}mo`,
  ratio:  v => `${v?.toFixed(2)}x`,
  '$':    v => `$${v?.toFixed(1)}`,
}
function fmt(val, unit) {
  if (val == null) return '—'
  return (UNIT_FMT[unit] || (v => v?.toFixed(2)))(val)
}
function gapPct(kpi) {
  if (kpi.avg == null || !kpi.target) return null
  const raw = (kpi.avg / kpi.target - 1) * 100
  return kpi.direction !== 'higher' ? -raw : raw
}

// ── Streak calculators ───────────────────────────────────────────────────────
function cellStatus(val, target, direction) {
  if (val == null || !target) return 'grey'
  const r = direction === 'higher' ? val / target : target / val
  return r >= 0.98 ? 'green' : r >= 0.90 ? 'yellow' : 'red'
}
function redStreak(kpi) {
  const byMonth = {}
  kpi.monthly?.forEach(m => { byMonth[parseInt(m.period.split('-')[1], 10)] = m.value })
  let streak = 0
  for (let mo = 12; mo >= 1; mo--) {
    if (cellStatus(byMonth[mo], kpi.target, kpi.direction) === 'red') streak++
    else break
  }
  return streak
}
function greenStreak(kpi) {
  const byMonth = {}
  kpi.monthly?.forEach(m => { byMonth[parseInt(m.period.split('-')[1], 10)] = m.value })
  let streak = 0
  for (let mo = 12; mo >= 1; mo--) {
    if (cellStatus(byMonth[mo], kpi.target, kpi.direction) === 'green') streak++
    else break
  }
  return streak
}

// ── Sparkline / chart data ───────────────────────────────────────────────────
function sparkData(kpi) {
  return MONTH_NUMS.map((mo, idx) => {
    const m = kpi.monthly?.find(d => parseInt(d.period.split('-')[1], 10) === mo)
    return { month: MONTHS[idx], value: m?.value ?? null }
  })
}

// ── "So What" contextualiser ─────────────────────────────────────────────────
function soWhat(kpi) {
  const key  = (kpi.key || '').toLowerCase()
  const gap  = gapPct(kpi)
  const gStr = gap != null ? Math.abs(gap).toFixed(0) : null

  if (key.includes('nrr') || key.includes('net_revenue_retention')) {
    if (kpi.avg != null && kpi.avg < 100)
      return 'Below 100% means the customer base is contracting without new sales.'
    if (kpi.avg != null && kpi.avg >= 110)
      return 'Above 110% indicates strong expansion — existing customers are funding growth.'
    return 'NRR at 100–110%: stable but growth requires continuous new sales effort.'
  }
  if (key.includes('churn')) {
    const annual = kpi.avg ? (kpi.avg * 12).toFixed(0) : null
    return annual ? `At this rate, ~${annual}% of the customer base churns annually.` : 'Churn rate impacts long-term revenue compounding.'
  }
  if (key.includes('burn_multiple') || key.includes('burn multiple')) {
    return kpi.avg ? `Every $${kpi.avg.toFixed(1)} spent generates $1 of new ARR.` : 'Burn multiple measures capital efficiency of growth.'
  }
  if (key.includes('gross_margin') || key.includes('gross margin')) {
    return kpi.avg ? `Each revenue dollar generates ${kpi.avg.toFixed(0)}¢ of gross profit.` : 'Gross margin determines the ceiling on long-term profitability.'
  }
  if (key.includes('cac') && !key.includes('payback')) {
    return gStr ? `Acquiring each customer costs ${gStr}% ${gap < 0 ? 'more' : 'less'} than target.` : 'CAC drives the efficiency of the growth engine.'
  }
  if (key.includes('runway')) {
    return kpi.avg ? `At current burn, ${kpi.avg.toFixed(0)} months of runway remaining.` : 'Runway determines strategic optionality.'
  }
  if (key.includes('ltv') && !key.includes('cac')) {
    return 'LTV decline compresses the ROI ceiling on acquisition spend.'
  }
  if (gap != null) {
    return gap < 0
      ? `${Math.abs(gap).toFixed(1)}% below target — gap is ${Math.abs(gap) > 15 ? 'structurally significant' : 'manageable with targeted intervention'}.`
      : `${gap.toFixed(1)}% above target — a signal worth protecting.`
  }
  return null
}

// ── Signal action items ──────────────────────────────────────────────────────
function signalActions(signal) {
  const t = signal.title?.toLowerCase() || ''
  if (t.includes('streak') || t.includes('consecutive'))
    return ['Request a root cause analysis with accountable owner', 'Set a 30-day milestone with a specific measurable outcome', 'Determine if this is cyclical or structural before escalating further']
  if (t.includes('masking') || t.includes('retention'))
    return ['Cross-reference NRR and churn trends with revenue data', 'Ask management to model the compounding impact over 3 quarters', 'Ensure retention KPIs are explicitly covered in the next board pack']
  if (t.includes('green on paper') || t.includes('deteriorating'))
    return ['Flag this KPI for monthly monitoring at executive level', 'Request the trailing 3-month trend to be reported alongside averages', 'Set an alert threshold before it crosses into amber']
  if (t.includes('burn') || t.includes('efficiency'))
    return ['Model the break-even growth rate at current burn', 'Ask management to present the path to self-funded growth', 'Stress-test runway against a growth slowdown scenario']
  if (t.includes('clustered') || t.includes('simultaneously'))
    return ['Identify the common upstream cause across all flagged KPIs', 'Assign a single accountable owner for the domain', 'Consider a focused operational review of this business area']
  return ['Review with the responsible executive before next board session', 'Request a data-backed action plan with clear milestones']
}

// ── Thesis sentence ───────────────────────────────────────────────────────────
// buildThesis — removed; replaced with factual status distribution

// ── Hidden signal detector ────────────────────────────────────────────────────
function detectSignals(fingerprint) {
  const signals = []
  const streakers = fingerprint.map(k => ({ ...k, _streak: redStreak(k) })).filter(k => k._streak >= 3).sort((a, b) => b._streak - a._streak)
  if (streakers.length) {
    const k = streakers[0]
    signals.push({ sev: 'critical', icon: AlertCircle, title: `${k.name} has missed target ${k._streak} consecutive months`, body: `A streak of ${k._streak} months indicates a structural failure, not a one-off miss. Sustained red streaks compound — each additional month makes recovery significantly harder. Escalate before this reaches a step-change inflection.`, tab: 'fingerprint' })
  }
  const traps = fingerprint.filter(k => {
    if (k.fy_status !== 'green') return false
    const vals = (k.monthly || []).map(m => m.value).filter(v => v != null)
    if (vals.length < 3) return false
    const last3 = vals.slice(-3)
    return k.direction === 'higher' ? last3[2] < last3[0] : last3[2] > last3[0]
  })
  if (traps.length) {
    const k = traps[0]
    signals.push({ sev: 'warning', icon: Eye, title: `${k.name} is green on paper but the trend is deteriorating`, body: `The current average meets target, but the last 3 months show a consistent adverse trajectory. This is a leading indicator: if the trend continues unchecked, this KPI will breach the warning threshold within 1–2 quarters — the financials won't reflect this yet.`, tab: 'trends' })
  }
  const recovering = fingerprint.filter(k => {
    if (k.fy_status === 'green') return false
    const vals = (k.monthly || []).map(m => m.value).filter(v => v != null)
    if (vals.length < 3) return false
    const last3 = vals.slice(-3)
    return k.direction === 'higher' ? last3[2] > last3[0] * 1.02 : last3[2] < last3[0] * 0.98
  })
  if (recovering.length) {
    const k = recovering[0]
    signals.push({ sev: 'positive', icon: TrendingUp, title: `${k.name} is below target but showing genuine momentum`, body: `Despite missing its target, ${k.name} has improved consistently over the last 3 months. Early recovery signals in KPIs that have historically been leading indicators are worth monitoring — if sustained, this could represent a turning point.`, tab: 'trends' })
  }
  const retentionKpis = fingerprint.filter(k => { const key = (k.key || '').toLowerCase(); return key.includes('nrr') || key.includes('churn') || key.includes('retention') || key.includes('logo') })
  const growthKpis = fingerprint.filter(k => { const key = (k.key || '').toLowerCase(); return key.includes('revenue') || key.includes('arr') || key.includes('mrr') })
  if (retentionKpis.some(k => k.fy_status !== 'green') && growthKpis.some(k => k.fy_status === 'green') && retentionKpis.length > 0)
    signals.push({ sev: 'warning', icon: AlertTriangle, title: 'Growth is masking a retention problem — the P&L hides this', body: 'Top-line revenue looks healthy, but retention metrics are under stress. This divergence is a classic early warning: retention problems typically surface in the revenue line 2–3 quarters later, after churn compounds. Boards reviewing only the income statement will miss this signal entirely.', tab: 'dashboard' })
  const burnKpi = fingerprint.find(k => (k.key || '').toLowerCase().includes('burn'))
  const revKpi  = fingerprint.find(k => { const key = (k.key || '').toLowerCase(); return (key.includes('revenue_growth') || key.includes('arr_growth')) && !key.includes('cac') })
  if (burnKpi && revKpi && burnKpi.fy_status !== 'green' && revKpi.fy_status === 'green')
    signals.push({ sev: 'warning', icon: Zap, title: 'Revenue growth is being bought, not earned — watch the efficiency ratio', body: `Growth looks strong but Burn Multiple signals the current gains are capital-intensive. This is sustainable short-term but will face investor scrutiny at the next fundraise. The question is whether the growth will become self-funding before capital runs short.`, tab: 'projection' })
  const byDomain = {}
  fingerprint.forEach(k => { const d = getDomain(k); byDomain[d] = byDomain[d] || []; byDomain[d].push(k) })
  for (const [domain, kpis] of Object.entries(byDomain)) {
    const yellows = kpis.filter(k => k.fy_status === 'yellow')
    if (yellows.length >= 2 && domain !== 'other') {
      const meta = DOMAIN_META[domain]
      signals.push({ sev: 'warning', icon: Info, title: `${yellows.length} ${meta?.label || domain} metrics simultaneously in the warning zone`, body: `Clustered warnings within a single domain suggest a systemic constraint rather than isolated underperformance. When multiple KPIs in the same area miss together, the root cause is usually structural — a process, team, or market factor that individual KPI owners cannot solve in isolation.`, tab: 'fingerprint' })
      break
    }
  }
  return signals.slice(0, 5)
}

// ── Domain story builder ──────────────────────────────────────────────────────
function buildDomainStory(domain, kpis) {
  if (!kpis.length) return null
  const red    = kpis.filter(k => k.fy_status === 'red')
  const yellow = kpis.filter(k => k.fy_status === 'yellow')
  const green  = kpis.filter(k => k.fy_status === 'green')
  const stories = {
    growth: () => {
      if (green.length === kpis.length) return `Growth metrics are firing on all cylinders — ${kpis.length}/${kpis.length} KPIs on target. Top-line momentum is genuine and broad-based, not concentrated in a single metric. The pipeline and conversion economics support continued expansion.`
      if (red.length >= kpis.length / 2) return `Growth is under significant strain — ${red.length} of ${kpis.length} KPIs are critical. The growth engine is constrained; without intervention, the gap between current trajectory and targets will widen. Diagnose whether this is a pipeline, conversion, or retention issue.`
      return `Growth presents a mixed picture: ${green.length} KPIs on track, but ${red.length + yellow.length} are dragging the aggregate. The growth engine has the right components but isn't firing consistently — focus effort on the highest-leverage bottleneck, not the longest list of issues.`
    },
    retention: () => {
      if (red.length === 0 && yellow.length === 0) return `Retention health is genuinely strong — the customer base is stable and expanding. Strong NRR dynamics mean existing customers are funding a portion of growth, reducing reliance on new sales and compressing the effective CAC.`
      if (red.length > 0) return `Retention is the most consequential risk in this dataset. ${red.length} KPI${red.length > 1 ? 's are' : ' is'} critical, and churn dynamics at this level will compound against revenue within 2–3 quarters. A 1% improvement in monthly churn has a larger NPV impact than most growth initiatives.`
      return `Retention is in the watch zone — no critical failures yet, but ${yellow.length} metric${yellow.length > 1 ? 's are' : ' is'} trending adversely. Proactive intervention at the watch stage costs a fraction of what remediation costs once customers begin churning. The window to act is now.`
    },
    efficiency: () => {
      if (green.length === kpis.length) return `Operational efficiency is a demonstrable strength. The business generates output at or above target relative to its cost structure — this creates operating leverage that becomes increasingly valuable as scale increases.`
      if (red.length > 0) return `Efficiency metrics signal that costs are growing faster than the value they generate. ${red.length} KPI${red.length > 1 ? 's need' : ' needs'} structural intervention — incremental optimisation will not close the gap. Review the cost architecture at the program level, not the line item level.`
      return `Efficiency is in transition. The operating model has the right structure but margins and burn aren't improving at the rate expected at this growth stage. The operating leverage story needs to be built deliberately — it typically doesn't emerge without intentional choices.`
    },
    cashflow: () => {
      if (green.length === kpis.length) return `Cash position is healthy and the trajectory is positive. Strong runway and cash generation give the business the strategic flexibility to pursue growth without short-term capital pressure — a significant strategic advantage.`
      if (red.length > 0) return `Cash dynamics are a board-level concern requiring direct attention. ${red.length} KPI${red.length > 1 ? 's require' : ' requires'} immediate review — runway and free cash generation should be stress-tested against multiple scenarios at the next board session.`
      return `Cash generation is adequate but the trajectory warrants monitoring. Key metrics are in the yellow zone — not critical, but the margin of safety is narrowing. Revisit budget assumptions and ensure contingency plans are current.`
    },
    other: () => `${kpis.length} additional KPIs tracked: ${green.length} on target, ${yellow.length} watch, ${red.length} critical.`,
  }
  const story = (stories[domain] || stories.other)()
  return { story, red, yellow, green }
}

// ── Outlook generator ─────────────────────────────────────────────────────────
function buildOutlook(fingerprint, bridgeData) {
  const bullets = []
  const streakers = fingerprint.filter(k => redStreak(k) >= 3).sort((a,b) => redStreak(b)-redStreak(a))
  if (streakers.length)
    bullets.push(`Monitor ${streakers[0].name} closely — a ${redStreak(streakers[0])}-month red streak is the highest-priority operational risk.`)
  const traps = fingerprint.filter(k => {
    if (k.fy_status !== 'green') return false
    const vals = (k.monthly || []).map(m => m.value).filter(v => v != null)
    if (vals.length < 3) return false
    const last3 = vals.slice(-3)
    return k.direction === 'higher' ? last3[2] < last3[0] : last3[2] > last3[0]
  })
  if (traps.length)
    bullets.push(`${traps[0].name} will likely move from green to amber within 60–90 days if the current declining trajectory is not reversed.`)
  const redKpis = fingerprint.filter(k => k.fy_status === 'red')
  if (redKpis.length >= 3)
    bullets.push(`With ${redKpis.length} critical KPIs, the board should request a corrective action plan with accountable owners and measurable 30-day milestones — not just an update.`)
  if (bridgeData?.summary?.behind > 0)
    bullets.push(`${bridgeData.summary.behind} KPI${bridgeData.summary.behind > 1 ? 's are' : ' is'} behind projection — if not addressed, the annual plan may need to be re-baselined before Q3.`)
  const greenKpis = fingerprint.filter(k => k.fy_status === 'green')
  if (greenKpis.length > 0 && redKpis.length > 0)
    bullets.push(`Protect the ${greenKpis.length} on-target KPIs from resource diversion toward problem areas — over-correction is a common board intervention failure mode.`)
  if (!bullets.length)
    bullets.push('Continue monitoring the current KPI set — no acute risks detected at this time.')
  return bullets.slice(0, 4)
}

function vsTarget(kpi) {
  if (!kpi.target) return 100
  const r = kpi.direction === 'higher' ? kpi.avg / kpi.target : kpi.target / kpi.avg
  return Math.min(Math.round(r * 100), 140)
}

// ── Shared sub-components ─────────────────────────────────────────────────────

function NavPill({ tabId, onNavigate }) {
  const src = SOURCE[tabId]
  if (!src) return null
  return (
    <button
      onClick={e => { e.stopPropagation(); onNavigate(tabId) }}
      className={`inline-flex items-center gap-1 text-[10px] font-bold px-2 py-0.5 rounded-full border ${src.bg} ${src.border} ${src.text} hover:opacity-80 transition-opacity`}>
      {src.label} <ChevronRight size={10}/>
    </button>
  )
}

function ExpandBtn({ onClick }) {
  return (
    <button
      onClick={e => { e.stopPropagation(); onClick() }}
      className="flex-shrink-0 w-6 h-6 flex items-center justify-center rounded-lg bg-slate-50 border border-slate-200 text-slate-400 hover:bg-slate-100 hover:text-slate-600 hover:border-slate-300 transition-all"
      title="Open detail view">
      <Maximize2 size={11}/>
    </button>
  )
}

// ── SIDE PANEL ────────────────────────────────────────────────────────────────

function KpiDetailPanel({ kpi, onNavigate, onClose, periodDisplay }) {
  const st      = kpi.fy_status || 'grey'
  const gap     = gapPct(kpi)
  const rs      = redStreak(kpi)
  const gs      = greenStreak(kpi)
  const sw      = soWhat(kpi)
  const domain  = getDomain(kpi)
  const dmeta   = DOMAIN_META[domain] || DOMAIN_META.other
  const DIcon   = dmeta.Icon
  const data    = sparkData(kpi)
  const stColor = st === 'red' ? '#ef4444' : st === 'yellow' ? '#f59e0b' : st === 'green' ? '#059669' : '#94a3b8'
  const stLabel = st === 'red' ? 'Critical' : st === 'yellow' ? 'Watch' : st === 'green' ? 'On Target' : 'No Data'
  const trendVals = (kpi.monthly || []).map(m => m.value).filter(v => v != null)
  const trendDir  = trendVals.length >= 2
    ? (trendVals.at(-1) > trendVals[0] ? 'up' : trendVals.at(-1) < trendVals[0] ? 'down' : 'flat')
    : 'flat'

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-start justify-between p-5 border-b border-slate-100 sticky top-0 bg-white z-10">
        <div className="flex-1 min-w-0 pr-3">
          <div className="flex items-center gap-2 mb-1 flex-wrap">
            <span className="inline-flex items-center gap-1 text-[10px] font-bold px-2 py-0.5 rounded-full"
              style={{ background: dmeta.bg, color: dmeta.color, border: `1px solid ${dmeta.color}30` }}>
              <DIcon size={9}/> {dmeta.label}
            </span>
            <span className="text-[10px] font-bold px-2 py-0.5 rounded-full border"
              style={{ background: stColor + '15', color: stColor, borderColor: stColor + '40' }}>
              {stLabel}
            </span>
          </div>
          <h2 className="text-[17px] font-black text-slate-800 leading-snug">{kpi.name}</h2>
          {periodDisplay && (
            <div className="text-[10px] font-semibold text-slate-400 mt-0.5">Period: {periodDisplay}</div>
          )}
        </div>
        <button onClick={onClose} className="flex-shrink-0 w-8 h-8 flex items-center justify-center rounded-full bg-slate-100 hover:bg-slate-200 text-slate-500 transition-colors">
          <X size={14}/>
        </button>
      </div>

      <div className="flex-1 overflow-y-auto p-5 space-y-5">

        {/* Value / Target / Gap */}
        <div className="grid grid-cols-3 gap-3">
          {[
            { label: 'Current',  value: fmt(kpi.avg, kpi.unit),    color: stColor },
            { label: 'Target',   value: fmt(kpi.target, kpi.unit), color: '#64748b' },
            { label: 'Gap',      value: gap != null ? `${gap > 0 ? '+' : ''}${gap.toFixed(1)}%` : '—', color: gap == null ? '#94a3b8' : gap >= 0 ? '#059669' : '#ef4444' },
          ].map(c => (
            <div key={c.label} className="rounded-xl border border-slate-100 bg-slate-50 p-3 text-center">
              <div className="text-[18px] font-black leading-none mb-1" style={{ color: c.color }}>{c.value}</div>
              <div className="text-[9px] font-bold text-slate-400 uppercase tracking-wider">{c.label}</div>
            </div>
          ))}
        </div>

        {/* 12-month chart */}
        <div>
          <div className="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">12-Month Trend</div>
          <div className="bg-slate-50 rounded-xl border border-slate-100 p-3">
            <ResponsiveContainer width="100%" height={130}>
              <AreaChart data={data} margin={{ top: 6, right: 6, bottom: 0, left: 0 }}>
                <defs>
                  <linearGradient id={`grad-${kpi.key}`} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%"  stopColor={stColor} stopOpacity={0.25}/>
                    <stop offset="95%" stopColor={stColor} stopOpacity={0}/>
                  </linearGradient>
                </defs>
                {kpi.target && <ReferenceLine y={kpi.target} stroke="#cbd5e1" strokeDasharray="4 3" strokeWidth={1}/>}
                <Tooltip
                  contentStyle={{ background: '#1e293b', border: 'none', borderRadius: 8, fontSize: 11, color: '#f1f5f9', padding: '6px 10px' }}
                  formatter={v => [fmt(v, kpi.unit), kpi.name]}
                  labelStyle={{ color: '#94a3b8', fontSize: 10 }}
                />
                <Area type="monotone" dataKey="value" stroke={stColor} strokeWidth={2}
                  fill={`url(#grad-${kpi.key})`} dot={false} connectNulls/>
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Monthly pills */}
        <div>
          <div className="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">Monthly Status</div>
          <div className="grid grid-cols-6 gap-1.5">
            {MONTHS.map((mo, idx) => {
              const m  = kpi.monthly?.find(d => parseInt(d.period.split('-')[1], 10) === idx + 1)
              const s  = cellStatus(m?.value, kpi.target, kpi.direction)
              const bg = s === 'green' ? '#f0fdf4' : s === 'yellow' ? '#fffbeb' : s === 'red' ? '#fef2f2' : '#f8fafc'
              const tc = s === 'green' ? '#059669' : s === 'yellow' ? '#d97706' : s === 'red' ? '#dc2626' : '#94a3b8'
              const bc = s === 'green' ? '#bbf7d0' : s === 'yellow' ? '#fde68a' : s === 'red' ? '#fecaca' : '#e2e8f0'
              return (
                <div key={mo} className="rounded-lg border text-center py-1.5 px-1" style={{ background: bg, borderColor: bc }}>
                  <div className="text-[8px] font-bold" style={{ color: tc }}>{mo}</div>
                  <div className="text-[9px] font-black" style={{ color: tc }}>
                    {m?.value != null ? fmt(m.value, kpi.unit) : '—'}
                  </div>
                </div>
              )
            })}
          </div>
        </div>

        {/* Streak */}
        {(rs > 0 || gs > 0) && (
          <div className="flex gap-3">
            {rs > 0 && (
              <div className="flex-1 rounded-xl bg-red-50 border border-red-100 p-3 text-center">
                <div className="flex items-center justify-center gap-1 mb-1">
                  {rs >= 3 && <span className="w-1.5 h-1.5 rounded-full bg-red-500 animate-pulse"/>}
                  <span className="text-[20px] font-black text-red-600">{rs}</span>
                </div>
                <div className="text-[9px] font-bold text-red-400 uppercase">Consecutive Misses</div>
              </div>
            )}
            {gs > 0 && (
              <div className="flex-1 rounded-xl bg-emerald-50 border border-emerald-100 p-3 text-center">
                <div className="text-[20px] font-black text-emerald-600 mb-1">{gs}</div>
                <div className="text-[9px] font-bold text-emerald-400 uppercase">Consecutive On-Target</div>
              </div>
            )}
          </div>
        )}

        {/* So what */}
        {sw && (
          <div className="rounded-xl bg-slate-50 border border-slate-200 p-4">
            <div className="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-1.5">Why this matters</div>
            <p className="text-[13px] text-slate-700 leading-relaxed">{sw}</p>
          </div>
        )}

        {/* Nav pills */}
        <div>
          <div className="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">View in full context</div>
          <div className="flex flex-wrap gap-2">
            <NavPill tabId="dashboard" onNavigate={onNavigate}/>
            <NavPill tabId="trends" onNavigate={onNavigate}/>
            <NavPill tabId="fingerprint" onNavigate={onNavigate}/>
          </div>
        </div>
      </div>
    </div>
  )
}

function SignalDetailPanel({ signal, onNavigate, onClose, periodDisplay }) {
  const s       = { critical: { bg: '#fef2f2', bar: '#ef4444', badge: '#fca5a5', label: 'CRITICAL', tc: '#dc2626' }, warning: { bg: '#fffbeb', bar: '#f59e0b', badge: '#fde68a', label: 'WATCH', tc: '#d97706' }, positive: { bg: '#f0fdf4', bar: '#10b981', badge: '#bbf7d0', label: 'SIGNAL', tc: '#059669' } }[signal.sev] || {}
  const Icon    = signal.icon
  const actions = signalActions(signal)

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-start justify-between p-5 border-b border-slate-100 sticky top-0 bg-white z-10">
        <div className="flex-1 min-w-0 pr-3">
          <span className="inline-block text-[9px] font-black px-2 py-0.5 rounded-full border mb-2"
            style={{ background: s.bg, color: s.tc, borderColor: s.badge }}>
            {s.label}
          </span>
          <h2 className="text-[15px] font-black text-slate-800 leading-snug">{signal.title}</h2>
          {periodDisplay && (
            <div className="text-[10px] font-semibold text-slate-400 mt-0.5">Period: {periodDisplay}</div>
          )}
        </div>
        <button onClick={onClose} className="flex-shrink-0 w-8 h-8 flex items-center justify-center rounded-full bg-slate-100 hover:bg-slate-200 text-slate-500 transition-colors">
          <X size={14}/>
        </button>
      </div>

      <div className="flex-1 overflow-y-auto p-5 space-y-5">
        {/* Severity strip + icon */}
        <div className="rounded-xl border p-4 flex items-start gap-3" style={{ background: s.bg, borderColor: s.badge }}>
          <div className="w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0"
            style={{ background: s.bar + '25' }}>
            <Icon size={18} style={{ color: s.bar }}/>
          </div>
          <p className="text-[13px] text-slate-700 leading-relaxed">{signal.body}</p>
        </div>

        {/* Recommended actions */}
        <div>
          <div className="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2.5">Recommended Board Actions</div>
          <div className="space-y-2">
            {actions.map((a, i) => (
              <div key={i} className="flex items-start gap-2.5 rounded-xl bg-slate-50 border border-slate-100 p-3">
                <div className="w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 mt-0.5 bg-slate-200">
                  <span className="text-[9px] font-black text-slate-500">{i + 1}</span>
                </div>
                <p className="text-[12px] text-slate-700 leading-relaxed">{a}</p>
              </div>
            ))}
          </div>
        </div>

        {/* Nav */}
        <div>
          <div className="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">View source data</div>
          <NavPill tabId={signal.tab} onNavigate={onNavigate}/>
        </div>
      </div>
    </div>
  )
}

function DomainDetailPanel({ domain, kpis, onNavigate, onClose, periodDisplay }) {
  const meta   = DOMAIN_META[domain] || DOMAIN_META.other
  const Icon   = meta.Icon
  const result = buildDomainStory(domain, kpis)
  const { story, red, yellow, green } = result || { story: '', red: [], yellow: [], green: [] }
  const healthPct = kpis.length ? Math.round((green.length * 100 + yellow.length * 55) / kpis.length) : 0
  const sorted = [...red, ...yellow, ...green]

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-start justify-between p-5 border-b border-slate-100 sticky top-0 bg-white z-10">
        <div className="flex items-center gap-2.5 flex-1 min-w-0">
          <div className="w-8 h-8 rounded-xl flex items-center justify-center flex-shrink-0"
            style={{ background: meta.bg }}>
            <Icon size={16} style={{ color: meta.color }}/>
          </div>
          <div>
            <div className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Domain Detail</div>
            <h2 className="text-[16px] font-black text-slate-800 leading-tight">{meta.label}</h2>
          {periodDisplay && (
            <div className="text-[10px] font-semibold text-slate-400">Period: {periodDisplay}</div>
          )}
          </div>
        </div>
        <button onClick={onClose} className="flex-shrink-0 w-8 h-8 flex items-center justify-center rounded-full bg-slate-100 hover:bg-slate-200 text-slate-500 transition-colors">
          <X size={14}/>
        </button>
      </div>

      <div className="flex-1 overflow-y-auto p-5 space-y-5">
        {/* Health score */}
        <div className="rounded-xl border border-slate-100 bg-slate-50 p-4">
          <div className="flex items-center justify-between mb-2">
            <span className="text-[11px] font-bold text-slate-500">Domain Health Score</span>
            <span className="text-[20px] font-black" style={{ color: meta.color }}>{healthPct}%</span>
          </div>
          <div className="h-2 bg-slate-200 rounded-full overflow-hidden">
            <div className="h-full rounded-full" style={{ width: `${healthPct}%`, background: meta.color }}/>
          </div>
          <div className="flex gap-3 mt-2">
            {red.length > 0    && <span className="text-[10px] font-bold text-red-500">{red.length} critical</span>}
            {yellow.length > 0 && <span className="text-[10px] font-bold text-amber-500">{yellow.length} watch</span>}
            {green.length > 0  && <span className="text-[10px] font-bold text-emerald-500">{green.length} on target</span>}
          </div>
        </div>

        {/* Narrative */}
        {story && (
          <div className="rounded-xl p-4" style={{ background: meta.bg, border: `1px solid ${meta.color}20` }}>
            <div className="text-[10px] font-bold uppercase tracking-wider mb-1.5" style={{ color: meta.color }}>Domain Signal</div>
            <p className="text-[13px] text-slate-700 leading-relaxed">{story}</p>
          </div>
        )}

        {/* KPI list */}
        <div>
          <div className="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">{kpis.length} KPIs in this domain</div>
          <div className="space-y-2">
            {sorted.map(kpi => {
              const st    = kpi.fy_status || 'grey'
              const gap   = gapPct(kpi)
              const color = st === 'red' ? '#ef4444' : st === 'yellow' ? '#f59e0b' : st === 'green' ? '#059669' : '#94a3b8'
              const bg    = st === 'red' ? '#fef2f2' : st === 'yellow' ? '#fffbeb' : st === 'green' ? '#f0fdf4' : '#f8fafc'
              const bc    = st === 'red' ? '#fecaca' : st === 'yellow' ? '#fde68a' : st === 'green' ? '#bbf7d0' : '#e2e8f0'
              const data  = sparkData(kpi)
              return (
                <div key={kpi.key} className="flex items-center gap-3 rounded-xl border p-3"
                  style={{ background: bg, borderColor: bc }}>
                  <div className="flex-1 min-w-0">
                    <div className="text-[12px] font-bold text-slate-700 truncate">{kpi.name}</div>
                    <div className="flex items-center gap-2 mt-0.5">
                      <span className="font-mono text-[11px] font-bold" style={{ color }}>{fmt(kpi.avg, kpi.unit)}</span>
                      {gap != null && <span className="text-[10px] font-bold" style={{ color }}>{gap > 0 ? '+' : ''}{gap.toFixed(1)}%</span>}
                    </div>
                  </div>
                  <div className="w-20 h-8 flex-shrink-0">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={data} margin={{ top: 2, right: 2, bottom: 2, left: 2 }}>
                        <Line type="monotone" dataKey="value" stroke={color} strokeWidth={1.5} dot={false} connectNulls/>
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              )
            })}
          </div>
        </div>

        {/* Nav */}
        <div>
          <div className="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">View in full context</div>
          <div className="flex flex-wrap gap-2">
            <NavPill tabId="trends" onNavigate={onNavigate}/>
            <NavPill tabId="fingerprint" onNavigate={onNavigate}/>
          </div>
        </div>
      </div>
    </div>
  )
}

// ── Period Selector ───────────────────────────────────────────────────────────
function PeriodSelector({ selected, onChange }) {
  const [showCustom, setShowCustom] = useState(false)
  const [customMonths, setCustomMonths] = useState([])

  function toggleMonth(m) {
    setCustomMonths(prev =>
      prev.includes(m) ? prev.filter(x => x !== m) : [...prev, m].sort((a,b) => a-b)
    )
  }

  function applyCustom() {
    if (!customMonths.length) return
    onChange({ id: 'custom', label: `${customMonths.length}M`, months: customMonths })
    setShowCustom(false)
  }

  return (
    <div className="relative flex items-center gap-1 flex-wrap">
      {PERIOD_PRESETS.map(p => (
        <button
          key={p.id}
          onClick={() => { onChange(p); setShowCustom(false) }}
          className={`px-2.5 py-1 rounded-lg text-[11px] font-bold transition-all border ${
            selected?.id === p.id
              ? 'bg-[#0055A4] text-white border-[#0055A4] shadow-sm'
              : 'bg-white text-slate-500 border-slate-200 hover:border-slate-300 hover:text-slate-700'
          }`}>
          {p.label}
        </button>
      ))}
      {/* Custom picker toggle */}
      <button
        onClick={() => setShowCustom(v => !v)}
        className={`px-2.5 py-1 rounded-lg text-[11px] font-bold transition-all border ${
          selected?.id === 'custom'
            ? 'bg-[#0055A4] text-white border-[#0055A4] shadow-sm'
            : 'bg-white text-slate-500 border-slate-200 hover:border-slate-300 hover:text-slate-700'
        }`}>
        Custom {selected?.id === 'custom' ? `(${selected.label})` : ''}
      </button>

      {/* Custom month picker dropdown */}
      {showCustom && (
        <div className="absolute top-full left-0 mt-1.5 z-30 bg-white border border-slate-200 rounded-2xl shadow-xl p-4 min-w-[280px]">
          <div className="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">Select months</div>
          <div className="grid grid-cols-6 gap-1.5 mb-3">
            {MONTHS.map((mo, idx) => {
              const m   = idx + 1
              const sel = customMonths.includes(m)
              return (
                <button key={m} onClick={() => toggleMonth(m)}
                  className={`text-[10px] font-bold py-1 rounded-lg border transition-all ${
                    sel
                      ? 'bg-[#0055A4] text-white border-[#0055A4]'
                      : 'bg-slate-50 text-slate-500 border-slate-200 hover:border-[#0055A4]/40 hover:text-[#0055A4]'
                  }`}>
                  {mo}
                </button>
              )
            })}
          </div>
          <div className="flex gap-2">
            <button onClick={applyCustom}
              disabled={!customMonths.length}
              className="flex-1 py-1.5 bg-[#0055A4] text-white text-[11px] font-bold rounded-lg disabled:opacity-40 hover:bg-[#004494] transition-colors">
              Apply {customMonths.length > 0 ? `(${customMonths.length}mo)` : ''}
            </button>
            <button onClick={() => setShowCustom(false)}
              className="px-3 py-1.5 bg-slate-100 text-slate-500 text-[11px] font-bold rounded-lg hover:bg-slate-200 transition-colors">
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

function SidePanel({ card, fingerprint, onNavigate, onClose, periodDisplay }) {
  if (!card) return null
  return (
    <>
      <style>{PANEL_ANIM}</style>
      <div className="fixed inset-0 z-40 bg-slate-900/40 backdrop-blur-[2px]" onClick={onClose}/>
      <div
        className="fixed right-0 top-0 h-full bg-white shadow-2xl z-50 overflow-hidden flex flex-col"
        style={{ width: 'min(440px, 95vw)', animation: 'slideInRight 0.22s ease-out' }}>
        {card.type === 'kpi'    && <KpiDetailPanel    kpi={card.kpi}       onNavigate={onNavigate} onClose={onClose} periodDisplay={periodDisplay}/>}
        {card.type === 'signal' && <SignalDetailPanel  signal={card.signal} onNavigate={onNavigate} onClose={onClose} periodDisplay={periodDisplay}/>}
        {card.type === 'domain' && <DomainDetailPanel  domain={card.domain} kpis={card.kpis} onNavigate={onNavigate} onClose={onClose} periodDisplay={periodDisplay}/>}
      </div>
    </>
  )
}

// ── Severity styles ───────────────────────────────────────────────────────────
const SEV = {
  critical: { bg: 'bg-red-50',     border: 'border-red-200',   icon: 'text-red-500',   bar: '#ef4444', badge: 'bg-red-100 text-red-700 border-red-200',    label: 'HIGH PRIORITY', hover: 'hover:shadow-[0_8px_30px_rgba(239,68,68,0.18)]' },
  warning:  { bg: 'bg-amber-50',   border: 'border-amber-200', icon: 'text-amber-500', bar: '#f59e0b', badge: 'bg-amber-100 text-amber-700 border-amber-200', label: 'WATCH SIGNAL', hover: 'hover:shadow-[0_8px_30px_rgba(245,158,11,0.18)]' },
  positive: { bg: 'bg-emerald-50', border: 'border-emerald-200', icon: 'text-emerald-500', bar: '#10b981', badge: 'bg-emerald-100 text-emerald-700 border-emerald-200', label: 'OPPORTUNITY', hover: 'hover:shadow-[0_8px_30px_rgba(16,185,129,0.18)]' },
}

// ── Inline clickable chip for narrative text ──────────────────────────────────
// Color is intentionally inherited — the dotted underline is the sole click signal
function TextChip({ children, onClick }) {
  return (
    <button
      onClick={e => { e.stopPropagation(); onClick?.() }}
      className="underline decoration-dotted underline-offset-2 cursor-pointer hover:opacity-60 transition-opacity inline"
      style={{ color: 'inherit', background: 'none', border: 'none', padding: 0, font: 'inherit', fontSize: 'inherit', lineHeight: 'inherit', letterSpacing: 'inherit' }}>
      {children}
    </button>
  )
}

// ── Card components ───────────────────────────────────────────────────────────

function HiddenSignalCard({ signal, onNavigate, onExpand }) {
  const s    = SEV[signal.sev] || SEV.warning
  const Icon = signal.icon
  return (
    <div
      className={`rounded-2xl border ${s.border} ${s.bg} overflow-hidden flex flex-col cursor-pointer group transition-all duration-200 ${s.hover} hover:-translate-y-0.5`}
      onClick={() => onExpand({ type: 'signal', signal })}>
      <div style={{ height: 3, background: s.bar }}/>
      <div className="p-4 flex-1 flex flex-col gap-2.5">
        <div className="flex items-start gap-2.5">
          <Icon size={16} className={`${s.icon} flex-shrink-0 mt-0.5`}/>
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 flex-wrap mb-1">
              <span className={`text-[9px] font-black px-1.5 py-0.5 rounded border ${s.badge}`}>{s.label}</span>
            </div>
            <p className="text-[13px] font-bold text-slate-800 leading-snug">{signal.title}</p>
          </div>
          <ExpandBtn onClick={() => onExpand({ type: 'signal', signal })}/>
        </div>
        <p className="text-[11px] text-slate-500 leading-relaxed flex-1 line-clamp-3">{signal.body}</p>
        <div className="flex items-center justify-between pt-1">
          <NavPill tabId={signal.tab} onNavigate={onNavigate}/>
          <span className="text-[10px] text-slate-300 group-hover:text-slate-400 transition-colors">Tap to expand →</span>
        </div>
      </div>
    </div>
  )
}

function KpiStatusRow({ kpi, rank, onNavigate, onExpand }) {
  const st     = kpi.fy_status || 'grey'
  const gap    = gapPct(kpi)
  const streak = redStreak(kpi)
  const sw     = soWhat(kpi)
  const vals   = (kpi.monthly || []).map(m => m.value).filter(v => v != null)
  const trendDir = vals.length >= 2
    ? (vals.at(-1) > vals[0] ? 'up' : vals.at(-1) < vals[0] ? 'down' : 'flat')
    : 'flat'
  const isGoodTrend = trendDir === 'up'
    ? kpi.direction === 'higher'
    : trendDir === 'down'
    ? kpi.direction !== 'higher'
    : null
  const data = sparkData(kpi)
  const stColor = st === 'red' ? '#ef4444' : st === 'yellow' ? '#f59e0b' : '#10b981'

  return (
    <div
      className="rounded-xl border border-slate-100 bg-white hover:shadow-[0_6px_24px_rgba(0,0,0,0.10)] hover:border-slate-200 hover:-translate-y-0.5 transition-all duration-200 p-3.5 flex flex-col gap-2 cursor-pointer group"
      onClick={() => onExpand({ type: 'kpi', kpi })}>
      <div className="flex items-start gap-2.5">
        <span className="text-slate-300 font-mono text-[11px] w-4 text-center flex-shrink-0 mt-0.5">{rank}</span>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-1.5 flex-wrap">
            <span className="text-[13px] font-bold text-slate-800 leading-snug">{kpi.name}</span>
            {streak >= 2 && (
              <span className="flex items-center gap-0.5 text-[9px] font-bold text-red-500 bg-red-50 border border-red-200 px-1.5 py-0.5 rounded-full flex-shrink-0">
                {streak >= 3 && <span className="w-1.5 h-1.5 rounded-full bg-red-500 animate-pulse flex-shrink-0"/>}
                {streak}mo
              </span>
            )}
          </div>
          <div className="flex items-center gap-3 mt-0.5 flex-wrap">
            <span className="text-[12px] font-mono font-bold text-slate-700">{fmt(kpi.avg, kpi.unit)}</span>
            {kpi.target && <span className="text-[11px] text-slate-400">tgt {fmt(kpi.target, kpi.unit)}</span>}
            {gap != null && (
              <span className={`text-[11px] font-bold ${gap >= 0 ? 'text-emerald-600' : 'text-red-500'}`}>
                {gap > 0 ? '+' : ''}{gap.toFixed(1)}%
              </span>
            )}
            {trendDir === 'up'   && <TrendingUp   size={11} className={isGoodTrend ? 'text-emerald-500' : 'text-red-400'}/>}
            {trendDir === 'down' && <TrendingDown  size={11} className={isGoodTrend ? 'text-emerald-500' : 'text-red-400'}/>}
            {trendDir === 'flat' && <Minus         size={11} className="text-slate-300"/>}
          </div>
        </div>
        {/* Sparkline */}
        <div className="flex-shrink-0 w-16 h-8">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 2, right: 2, bottom: 2, left: 2 }}>
              <Line type="monotone" dataKey="value" stroke={stColor} strokeWidth={1.5} dot={false} connectNulls/>
            </LineChart>
          </ResponsiveContainer>
        </div>
        <ExpandBtn onClick={() => onExpand({ type: 'kpi', kpi })}/>
      </div>
      {sw && (
        <p className="text-[11px] text-slate-400 leading-snug border-t border-slate-50 pt-1.5 italic group-hover:text-slate-500 transition-colors">
          {sw}
        </p>
      )}
    </div>
  )
}

function DomainStoryCard({ domain, kpis, onNavigate, onExpand }) {
  const meta   = DOMAIN_META[domain] || DOMAIN_META.other
  const result = buildDomainStory(domain, kpis)
  if (!result) return null
  const { story, red, yellow, green } = result
  const Icon = meta.Icon
  const total = kpis.length
  const healthPct = total ? Math.round((green.length * 100 + yellow.length * 55) / total) : 0
  const spotlightKpis = [...red, ...yellow, ...green].slice(0, 2)
  const shadowColor = meta.color + '28'

  return (
    <div
      className="rounded-2xl border border-slate-200 overflow-hidden bg-white flex flex-col cursor-pointer group transition-all duration-200 hover:-translate-y-0.5"
      style={{ '--hover-shadow': `0 8px 32px ${shadowColor}` }}
      onMouseEnter={e => e.currentTarget.style.boxShadow = `0 8px 32px ${shadowColor}`}
      onMouseLeave={e => e.currentTarget.style.boxShadow = ''}
      onClick={() => onExpand({ type: 'domain', domain, kpis })}>
      <div style={{ height: 4, background: meta.color }}/>
      <div className="p-5 flex flex-col gap-3 flex-1">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-lg flex items-center justify-center flex-shrink-0" style={{ background: meta.bg }}>
              <Icon size={14} style={{ color: meta.color }}/>
            </div>
            <span className="text-[12px] font-black text-slate-700 uppercase tracking-wide">{meta.label}</span>
          </div>
          <div className="flex items-center gap-1.5 flex-shrink-0 ml-2">
            {red.length > 0    && <span className="whitespace-nowrap text-[9px] font-bold bg-red-50 border border-red-200 text-red-600 px-2 py-0.5 rounded-md">{red.length} critical</span>}
            {yellow.length > 0 && <span className="whitespace-nowrap text-[9px] font-bold bg-amber-50 border border-amber-200 text-amber-600 px-2 py-0.5 rounded-md">{yellow.length} watch</span>}
            {red.length === 0 && yellow.length === 0 && <span className="whitespace-nowrap text-[9px] font-bold bg-emerald-50 border border-emerald-200 text-emerald-600 px-2 py-0.5 rounded-md">on target</span>}
            <ExpandBtn onClick={() => onExpand({ type: 'domain', domain, kpis })}/>
          </div>
        </div>

        {/* Health bar */}
        <div>
          <div className="flex items-center justify-between mb-1">
            <span className="text-[10px] text-slate-400 font-medium">Domain health</span>
            <span className="text-[10px] font-bold" style={{ color: meta.color }}>{healthPct}%</span>
          </div>
          <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden">
            <div className="h-full rounded-full transition-all" style={{ width: `${healthPct}%`, background: meta.color }}/>
          </div>
        </div>

        {/* Narrative */}
        <p className="text-[12px] text-slate-600 leading-relaxed flex-1">{story}</p>

        {/* Sparklines */}
        {spotlightKpis.length > 0 && (
          <div className="flex gap-3 pt-1 border-t border-slate-50">
            {spotlightKpis.map(kpi => {
              const data   = sparkData(kpi)
              const st     = kpi.fy_status || 'grey'
              const lcolor = st === 'red' ? '#ef4444' : st === 'yellow' ? '#f59e0b' : '#10b981'
              return (
                <div key={kpi.key} className="flex-1 min-w-0">
                  <div className="text-[9px] font-semibold text-slate-500 truncate mb-1">{kpi.name}</div>
                  <ResponsiveContainer width="100%" height={32}>
                    <LineChart data={data} margin={{ top: 2, right: 2, bottom: 2, left: 2 }}>
                      <Line type="monotone" dataKey="value" stroke={lcolor} strokeWidth={1.5} dot={false} connectNulls/>
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              )
            })}
          </div>
        )}

        {/* Footer */}
        <div className="flex items-center justify-between pt-1">
          <NavPill tabId="trends" onNavigate={onNavigate}/>
          <span className="text-[10px] text-slate-300 group-hover:text-slate-400 transition-colors">Tap to expand →</span>
        </div>
      </div>
    </div>
  )
}

// ── Above-fold compact signal row ─────────────────────────────────────────────
function AboveFoldSignalRow({ signal, onNavigate, onExpand }) {
  const s    = SEV[signal.sev] || SEV.warning
  const Icon = signal.icon
  return (
    <div
      className={`rounded-xl border ${s.border} ${s.bg} overflow-hidden flex cursor-pointer group transition-all duration-150 hover:-translate-y-0.5 hover:shadow-md`}
      onClick={() => onExpand({ type: 'signal', signal })}>
      <div style={{ width: 4, background: s.bar, flexShrink: 0 }}/>
      <div className="flex items-center gap-3 px-4 py-3 flex-1 min-w-0">
        <Icon size={15} className={`${s.icon} flex-shrink-0`}/>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-0.5">
            <span className={`text-[9px] font-black px-1.5 py-0.5 rounded border ${s.badge}`}>{s.label}</span>
          </div>
          <p className="text-[13px] font-bold text-slate-800 leading-snug truncate">{signal.title}</p>
          <p className="text-[11px] text-slate-500 leading-snug mt-0.5 line-clamp-1">{signal.body}</p>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          <NavPill tabId={signal.tab} onNavigate={onNavigate}/>
          <span className="text-[10px] text-slate-300 group-hover:text-slate-400 transition-colors">→</span>
        </div>
      </div>
    </div>
  )
}

// ── Stage label helper ─────────────────────────────────────────────────────────
function stageLabel(s) {
  return ({ seed: 'Seed', series_a: 'Series A', series_b: 'Series B', series_c: 'Series C+' }[s] || s)
}

// ── MAIN COMPONENT ────────────────────────────────────────────────────────────
export default function BoardReady({ fingerprint, bridgeData, onNavigate, periodLabel: globalPeriodLabel, benchmarks, companyStage }) {
  const [sideCard, setSideCard] = useState(null)
  const dateStr = new Date().toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' })

  if (!fingerprint?.length) {
    return (
      <div className="flex flex-col items-center justify-center h-72 gap-3">
        <Layers size={36} className="text-slate-300"/>
        <p className="text-slate-400 text-sm text-center max-w-xs">
          No data yet. Load demo data or upload a CSV to generate your board intelligence brief.
        </p>
      </div>
    )
  }

  // fingerprint is already year/month-filtered by the parent (filteredFingerprint)
  const fp = fingerprint

  // ── Derived ────────────────────────────────────────────────────────────
  const greenKpis  = fp.filter(k => k.fy_status === 'green')
  const yellowKpis = fp.filter(k => k.fy_status === 'yellow')
  const redKpis    = fp.filter(k => k.fy_status === 'red')
  const total      = fp.length
  // ── Period-over-period status change tracking ─────────────────────────────
  // Compute previous-period status for each KPI to detect zone transitions
  const periodTransitions = useMemo(() => {
    const allPeriods = [...new Set(fp.flatMap(k => (k.monthly || []).map(m => m.period)))].sort()
    if (allPeriods.length < 2) return { recovered: [], worsened: [], prevRed: 0, prevYellow: 0, prevGreen: 0, hasPrev: false }
    const lastP = allPeriods[allPeriods.length - 1]
    const prevP = allPeriods[allPeriods.length - 2]
    const recovered = [] // moved toward green
    const worsened  = [] // moved toward red
    let prevRed = 0, prevYellow = 0, prevGreen = 0
    fp.forEach(kpi => {
      const lastVal = kpi.monthly?.find(m => m.period === lastP)?.value
      const prevVal = kpi.monthly?.find(m => m.period === prevP)?.value
      const lastSt = cellStatus(lastVal, kpi.target, kpi.direction)
      const prevSt = cellStatus(prevVal, kpi.target, kpi.direction)
      if (prevSt === 'red') prevRed++
      else if (prevSt === 'yellow') prevYellow++
      else if (prevSt === 'green') prevGreen++
      const rank = { red: 0, yellow: 1, green: 2, grey: -1 }
      if (rank[lastSt] > rank[prevSt] && rank[prevSt] >= 0) recovered.push({ kpi, from: prevSt, to: lastSt })
      if (rank[lastSt] < rank[prevSt] && rank[lastSt] >= 0) worsened.push({ kpi, from: prevSt, to: lastSt })
    })
    return { recovered, worsened, prevRed, prevYellow, prevGreen, hasPrev: true }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fingerprint])

  // eslint-disable-next-line react-hooks/exhaustive-deps
  const signals = useMemo(() => detectSignals(fp), [fingerprint])
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const outlook = useMemo(() => buildOutlook(fp, bridgeData), [fingerprint, bridgeData])

  const domainGroups = useMemo(() => {
    const groups = {}
    fp.forEach(k => {
      const d = getDomain(k)
      groups[d] = groups[d] || []
      groups[d].push(k)
    })
    return groups
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fingerprint])

  const storyDomains = ['growth', 'retention', 'efficiency', 'cashflow'].filter(d => (domainGroups[d]?.length || 0) >= 1)

  // ── Overall direction: compare declining streaks vs recovering KPIs ──────
  const declineCount    = fp.filter(k => redStreak(k) >= 2).length
  const recoveringCount = fp.filter(k => {
    if (k.fy_status === 'green') return false
    const vals = (k.monthly || []).map(m => m.value).filter(v => v != null)
    if (vals.length < 3) return false
    const last3 = vals.slice(-3)
    return k.direction === 'higher' ? last3[2] > last3[0] * 1.02 : last3[2] < last3[0] * 0.98
  }).length
  const overallTrend      = declineCount > recoveringCount ? 'declining' : recoveringCount > declineCount ? 'recovering' : 'steady'

  // ── Domain health chips ───────────────────────────────────────────────────
  const domainChips = ['growth', 'retention', 'efficiency', 'cashflow'].map(d => {
    const kpis = domainGroups[d] || []
    if (!kpis.length) return null
    const reds = kpis.filter(k => k.fy_status === 'red').length
    const yels = kpis.filter(k => k.fy_status === 'yellow').length
    const col  = reds > 0 ? '#ef4444' : yels > 0 ? '#f59e0b' : '#10b981'
    return { d, meta: DOMAIN_META[d], col, kpis, reds, yels }
  }).filter(Boolean)

  const atRisk = [...redKpis, ...yellowKpis].sort((a, b) => {
    if ((a.fy_status === 'red') !== (b.fy_status === 'red')) return a.fy_status === 'red' ? -1 : 1
    return Math.abs(gapPct(b) || 0) - Math.abs(gapPct(a) || 0)
  })

  const radarData = fp.filter(k => k.avg != null && k.target != null).slice(0, 10).map(k => ({
    kpi:    k.name.length > 15 ? k.name.slice(0, 13) + '…' : k.name,
    actual: Math.min(vsTarget(k), 135),
    target: 100,
  }))

  const bridgeRisks = bridgeData?.kpis
    ? Object.values(bridgeData.kpis).filter(k => k.avg_gap_pct != null && k.overall_status !== 'green').sort((a, b) => a.avg_gap_pct - b.avg_gap_pct).slice(0, 4)
    : []

  const streakAlerts = fp.filter(k => k.monthly?.length && redStreak(k) >= 2).map(k => ({ ...k, streak: redStreak(k) })).sort((a, b) => b.streak - a.streak).slice(0, 4)
  const strongSorted = [...greenKpis].sort((a, b) => (gapPct(b) || 0) - (gapPct(a) || 0))

  // Period label shown in UI — mirrors the global year/month selector
  const periodDisplay = globalPeriodLabel || 'All Data'

  return (
    <div className="space-y-5 max-w-screen-xl">

      {/* ── HEADER: Status distribution + period delta ────────────────────── */}
      <div className="bg-white border border-slate-200 rounded-2xl px-6 py-4 shadow-sm">

        <div className="flex items-center gap-5 flex-wrap">

          {/* Status distribution bar + counts */}
          <div className="flex-1 min-w-[280px]">
            <div className="flex items-center justify-between mb-2">
              <div className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">KPI Status Distribution</div>
              <div className="text-[11px] text-slate-400">
                Period: <span className="font-semibold text-slate-700">{periodDisplay}</span>
              </div>
            </div>

            {/* Proportional status bar */}
            <div className="flex h-3 rounded-full overflow-hidden mb-2.5">
              {redKpis.length > 0 && (
                <div className="bg-red-500 transition-all duration-500" style={{ width: `${(redKpis.length / total) * 100}%` }}/>
              )}
              {yellowKpis.length > 0 && (
                <div className="bg-amber-400 transition-all duration-500" style={{ width: `${(yellowKpis.length / total) * 100}%` }}/>
              )}
              {greenKpis.length > 0 && (
                <div className="bg-emerald-500 transition-all duration-500" style={{ width: `${(greenKpis.length / total) * 100}%` }}/>
              )}
            </div>

            {/* Count pills */}
            <div className="flex items-center gap-2 flex-wrap">
              {[
                { count: redKpis.length,    label: 'Critical',  bg: 'bg-red-50',     border: 'border-red-200',     dot: 'bg-red-500',     text: 'text-red-700',     sub: 'text-red-500',    pulse: true  },
                { count: yellowKpis.length, label: 'Watch',     bg: 'bg-amber-50',   border: 'border-amber-200',   dot: 'bg-amber-400',   text: 'text-amber-700',   sub: 'text-amber-500',  pulse: false },
                { count: greenKpis.length,  label: 'On Target', bg: 'bg-emerald-50', border: 'border-emerald-200', dot: 'bg-emerald-500', text: 'text-emerald-700', sub: 'text-emerald-500', pulse: false },
              ].map(({ count, label, bg, border, dot, text, sub, pulse }) => (
                <button key={label}
                  onClick={() => document.getElementById('kpi-status-section')?.scrollIntoView({ behavior: 'smooth', block: 'start' })}
                  className={`flex items-center gap-1.5 px-3 py-2 rounded-xl ${bg} ${border} border hover:opacity-80 transition-opacity cursor-pointer`}>
                  <span className={`w-2 h-2 rounded-full ${dot} ${pulse ? 'animate-pulse' : ''}`}/>
                  <span className={`text-[14px] font-black ${text}`}>{count}</span>
                  <span className={`text-[11px] font-semibold ${sub}`}>{label}</span>
                </button>
              ))}
            </div>
          </div>

          <div className="w-px h-16 bg-slate-200 flex-shrink-0 hidden sm:block"/>

          {/* Period-over-period delta */}
          <div className="flex-shrink-0 min-w-[180px]">
            {periodTransitions.hasPrev ? (
              <div>
                <div className="text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1.5">Since Last Period</div>
                <div className="space-y-1">
                  {periodTransitions.recovered.length > 0 && (
                    <div className="flex items-center gap-1.5">
                      <span className="text-[13px]">↑</span>
                      <span className="text-[12px] text-emerald-700 font-semibold">
                        {periodTransitions.recovered.length} recovered
                      </span>
                      <span className="text-[10px] text-slate-400">
                        ({periodTransitions.recovered.map(r => r.kpi.name.split(' ')[0]).join(', ')})
                      </span>
                    </div>
                  )}
                  {periodTransitions.worsened.length > 0 && (
                    <div className="flex items-center gap-1.5">
                      <span className="text-[13px]">↓</span>
                      <span className="text-[12px] text-red-600 font-semibold">
                        {periodTransitions.worsened.length} worsened
                      </span>
                      <span className="text-[10px] text-slate-400">
                        ({periodTransitions.worsened.map(w => w.kpi.name.split(' ')[0]).join(', ')})
                      </span>
                    </div>
                  )}
                  {periodTransitions.recovered.length === 0 && periodTransitions.worsened.length === 0 && (
                    <div className="flex items-center gap-1.5">
                      <span className="text-[13px]">→</span>
                      <span className="text-[12px] text-slate-500 font-semibold">No zone changes</span>
                    </div>
                  )}
                  <div className="text-[10px] text-slate-400 mt-0.5">
                    Was: {periodTransitions.prevRed} critical · {periodTransitions.prevYellow} watch · {periodTransitions.prevGreen} on target
                  </div>
                </div>
              </div>
            ) : (
              <div>
                <div className="text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1">Period Trend</div>
                <div className="text-[11px] text-slate-400">Need 2+ periods for comparison</div>
              </div>
            )}
          </div>
        </div>

        <div className="flex justify-end mt-3">
          <button onClick={() => {
              const a = document.createElement('a')
              a.href = `/api/export/board-deck.pptx?stage=${companyStage || 'series_b'}`
              a.download = 'board-deck.pptx'
              a.click()
            }}
            className="flex items-center gap-1.5 px-3 py-2 bg-[#0055A4] hover:bg-[#003d80] border border-[#0055A4] rounded-xl text-[11px] text-white font-semibold transition-all">
            <ExternalLink size={12}/> Board Deck
          </button>
          <button onClick={e => { e.stopPropagation(); window.print() }}
            className="flex items-center gap-1.5 px-3 py-2 bg-slate-100 hover:bg-slate-200 border border-slate-200 rounded-xl text-[11px] text-slate-600 font-semibold transition-all">
            <Printer size={12}/> Print
          </button>
        </div>
      </div>

      {/* ── NARRATIVE HERO ──────────────────────────────────────────────────── */}
      {(() => {
        // ── Data assembly ──────────────────────────────────────────────────
        const domainScores = ['growth', 'retention', 'efficiency', 'cashflow', 'risk', 'profitability'].map(d => {
          const kpis = domainGroups[d] || []
          if (!kpis.length) return null
          const reds   = kpis.filter(k => k.fy_status === 'red').length
          const yels   = kpis.filter(k => k.fy_status === 'yellow').length
          const greens = kpis.filter(k => k.fy_status === 'green').length
          return { d, meta: DOMAIN_META[d], reds, yels, greens, total: kpis.length, kpis }
        }).filter(Boolean)

        const worstDomain  = [...domainScores].sort((a, b) => (b.reds / b.total) - (a.reds / a.total))[0]
        const bestDomain   = [...domainScores].filter(x => x.reds === 0 && x.greens > 0).sort((a, b) => b.greens - a.greens)[0]

        // Top 2 red KPIs by gap magnitude
        const top2Red = [...redKpis]
          .sort((a, b) => Math.abs(gapPct(b) || 0) - Math.abs(gapPct(a) || 0))
          .slice(0, 2)

        // Top 2 yellow KPIs closest to going red
        const top2Yellow = [...yellowKpis]
          .sort((a, b) => Math.abs(gapPct(b) || 0) - Math.abs(gapPct(a) || 0))
          .slice(0, 2)

        // All multi-month streakers, sorted
        const allStreakers = fp
          .map(k => ({ k, s: redStreak(k) }))
          .filter(x => x.s >= 2)
          .sort((a, b) => b.s - a.s)
        const worstStreaker = allStreakers[0]

        // Retention / churn risk KPI
        const retentionKpi = fp.find(k => {
          const key = (k.key || '').toLowerCase()
          return (key.includes('nrr') || key.includes('churn')) && k.fy_status !== 'green'
        })

        // Strongest green KPI (highest % above target)
        const strongestGreen = greenKpis.length
          ? [...greenKpis].sort((a, b) => (gapPct(b) || 0) - (gapPct(a) || 0))[0]
          : null

        // KPIs in worst domain that are specifically red
        const worstDomainRedKpis = worstDomain
          ? worstDomain.kpis.filter(k => k.fy_status === 'red').slice(0, 2)
          : []

        // Trajectory descriptor
        const trendWord = overallTrend === 'declining' ? 'deteriorating' : overallTrend === 'recovering' ? 'improving' : 'stable'

        // ── Chip helpers (all inherit text color — underline is the only click signal) ──
        const kpiChip = (kpi, label) => kpi ? (
          <TextChip key={kpi.key} onClick={() => setSideCard({ type: 'kpi', kpi })}>
            {label || kpi.name}
          </TextChip>
        ) : null
        const domainChip = (ds, label) => ds ? (
          <TextChip key={ds.d} onClick={() => setSideCard({ type: 'domain', domain: ds.d, kpis: ds.kpis })}>
            {label || ds.meta?.label}
          </TextChip>
        ) : null

        // ── Per-KPI causation helper — returns cause sentence + downstream cascade + fix ──
        // Pulls from kpi.causation if available; falls back to generic language
        const kpiDetail = (kpi) => {
          if (!kpi) return null
          const absGap   = Math.abs(Math.round(gapPct(kpi) || 0))
          const cause    = kpi.causation?.root_causes?.[0]
          const fix      = kpi.causation?.corrective_actions?.[0]
          // Downstream KPIs that are also in distress
          const downstream = (kpi.causation?.downstream_impact || [])
            .map(key => fp.find(k => k.key === key))
            .filter(k => k && k.fy_status !== 'green')
          // Benchmark peer context
          const bm = benchmarks?.[kpi.key]
          const isLower = kpi.direction === 'lower'
          const pctFromMedian = (bm && kpi.avg != null && bm.p50 != null && bm.p50 !== 0)
            ? ((kpi.avg - bm.p50) / Math.abs(bm.p50)) * 100
            : null
          const isAboveMedian = pctFromMedian != null
            ? (isLower ? pctFromMedian < 0 : pctFromMedian > 0)
            : null
          return (
            <>
              {kpiChip(kpi)} at {fmt(kpi.avg, kpi.unit)} vs target {fmt(kpi.target, kpi.unit)} ({absGap}% off)
              {cause ? ` — ${cause.charAt(0).toLowerCase() + cause.slice(1)}` : ''}
              {downstream.length > 0 && (
                <>, already dragging{' '}
                  {downstream.slice(0, 2).map((dk, i) => (
                    <span key={dk.key}>{i > 0 ? ' and ' : ' '}{kpiChip(dk)}</span>
                  ))}
                </>
              )}
              {fix ? <>. To address: {fix.charAt(0).toLowerCase() + fix.slice(1)}</> : ''}.
              {bm && pctFromMedian != null && (
                <> Peer context: {stageLabel(companyStage)} SaaS median is {fmt(bm.p50, kpi.unit)} — this business is {Math.abs(pctFromMedian).toFixed(0)}% {isAboveMedian ? 'above' : 'below'} the industry midpoint.</>
              )}
            </>
          )
        }

        // ── Narrative paragraphs ───────────────────────────────────────────
        const paragraphs = []

        // — Opening: trajectory + count summary
        const openingTone = redKpis.length === 0
          ? `performing strongly — ${greenKpis.length} of ${total} KPIs on or above target`
          : redKpis.length <= total * 0.25
          ? `under mixed pressure — ${redKpis.length} KPI${redKpis.length !== 1 ? 's' : ''} critical, ${yellowKpis.length} in the watch zone, ${greenKpis.length} on target`
          : `in significant distress — ${redKpis.length} of ${total} KPIs critically off-target, a broad-based failure pattern`

        paragraphs.push(
          <p key="opening" className="text-[13px] text-white/90 leading-relaxed">
            In {periodDisplay} the business is {openingTone}.{' '}
            The overall trajectory is {trendWord}
            {declineCount > 0 ? `, with ${declineCount} KPI${declineCount !== 1 ? 's' : ''} in sustained multi-period decline` : ''}
            {recoveringCount > 0 ? ` and ${recoveringCount} showing recovery momentum` : ''}.
            {periodTransitions.hasPrev && periodTransitions.recovered.length > 0 && (
              <>{' '}Since last period, {periodTransitions.recovered.length} KPI{periodTransitions.recovered.length !== 1 ? 's' : ''} recovered
                ({periodTransitions.recovered.map(r => r.kpi.name).join(', ')}).</>
            )}
            {periodTransitions.hasPrev && periodTransitions.worsened.length > 0 && (
              <>{' '}{periodTransitions.worsened.length} KPI{periodTransitions.worsened.length !== 1 ? 's' : ''} worsened
                ({periodTransitions.worsened.map(w => w.kpi.name).join(', ')}).</>
            )}
          </p>
        )

        // — Month-over-Month Delta
        const momDelta = (() => {
          // Get all periods from the first KPI's monthly data, sorted
          const allPeriods = [...new Set(fp.flatMap(k => (k.monthly || []).map(m => m.period)))].sort()
          if (allPeriods.length < 2) return null
          const lastPeriod = allPeriods[allPeriods.length - 1]
          const prevPeriod = allPeriods[allPeriods.length - 2]

          let improved = 0, deteriorated = 0, crossedToRed = [], crossedToGreen = []
          let biggestImprover = null, biggestDecliner = null
          let bestDelta = -Infinity, worstDelta = Infinity

          fp.forEach(kpi => {
            const last = kpi.monthly?.find(m => m.period === lastPeriod)?.value
            const prev = kpi.monthly?.find(m => m.period === prevPeriod)?.value
            if (last == null || prev == null || prev === 0) return

            const delta = ((last - prev) / Math.abs(prev)) * 100
            const isImprovement = kpi.direction === 'higher' ? delta > 0 : delta < 0

            if (isImprovement) improved++
            else if (Math.abs(delta) > 1) deteriorated++

            // Status changes
            const lastStatus = cellStatus(last, kpi.target, kpi.direction)
            const prevStatus = cellStatus(prev, kpi.target, kpi.direction)
            if (lastStatus === 'red' && prevStatus !== 'red') crossedToRed.push(kpi)
            if (lastStatus === 'green' && prevStatus !== 'green') crossedToGreen.push(kpi)

            const signedDelta = isImprovement ? Math.abs(delta) : -Math.abs(delta)
            if (signedDelta > bestDelta) { bestDelta = signedDelta; biggestImprover = { kpi, delta } }
            if (signedDelta < worstDelta) { worstDelta = signedDelta; biggestDecliner = { kpi, delta } }
          })

          return { improved, deteriorated, crossedToRed, crossedToGreen, biggestImprover, biggestDecliner, lastPeriod, prevPeriod }
        })()

        if (momDelta && (momDelta.improved + momDelta.deteriorated > 0)) {
          const MONTHS_SHORT = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
          const periodName = (p) => {
            const [y,m] = p.split('-')
            return `${MONTHS_SHORT[parseInt(m,10)-1]} ${y}`
          }
          paragraphs.push(
            <p key="mom-delta" className="text-[13px] text-white/90 leading-relaxed">
              Compared to {periodName(momDelta.prevPeriod)}: {momDelta.improved} KPI{momDelta.improved !== 1 ? 's' : ''} improved, {momDelta.deteriorated} deteriorated.
              {momDelta.crossedToRed.length > 0 && (
                <>{' '}{momDelta.crossedToRed.map((k,i) => <span key={k.key}>{i > 0 ? ', ' : ''}{kpiChip(k)}</span>)} crossed into the red zone this period.</>
              )}
              {momDelta.crossedToGreen.length > 0 && (
                <>{' '}{momDelta.crossedToGreen.map((k,i) => <span key={k.key}>{i > 0 ? ', ' : ''}{kpiChip(k)}</span>)} recovered to on-target.</>
              )}
              {momDelta.biggestImprover && (
                <>{' '}Biggest positive move: {kpiChip(momDelta.biggestImprover.kpi)}, up {Math.abs(momDelta.biggestImprover.delta).toFixed(1)}%.</>
              )}
              {momDelta.biggestDecliner && momDelta.biggestDecliner.kpi.key !== momDelta.biggestImprover?.kpi?.key && (
                <>{' '}Sharpest decline: {kpiChip(momDelta.biggestDecliner.kpi)}, down {Math.abs(momDelta.biggestDecliner.delta).toFixed(1)}%.</>
              )}
            </p>
          )
        }

        // — Peer context sentence for top red KPI (if benchmark data available)
        if (top2Red.length > 0 && benchmarks) {
          const topKpi = top2Red[0]
          const bm = benchmarks[topKpi.key]
          if (bm && topKpi.avg != null && bm.p50 != null && bm.p50 !== 0) {
            const isLower = topKpi.direction === 'lower'
            const pctFromMedian = ((topKpi.avg - bm.p50) / Math.abs(bm.p50)) * 100
            const isAboveMedian = isLower ? pctFromMedian < 0 : pctFromMedian > 0
            paragraphs.push(
              <p key="peer-context" className="text-[13px] text-white/60 leading-relaxed italic">
                For context: {topKpi.name} median for {stageLabel(companyStage)} SaaS is {fmt(bm.p50, topKpi.unit)} — this business is{' '}
                {Math.abs(pctFromMedian).toFixed(0)}% {isAboveMedian ? 'above' : 'below'} the peer median.
              </p>
            )
          }
        }

        // — Critical KPI deep-dives (top 2 red)
        if (top2Red.length > 0) {
          paragraphs.push(
            <p key="red1" className="text-[13px] text-white/90 leading-relaxed">
              The deepest failure this period:{' '}{kpiDetail(top2Red[0])}
              {top2Red[1] && (
                <>{' '}A second critical gap:{' '}{kpiDetail(top2Red[1])}</>
              )}
            </p>
          )
        }

        // — Domain stress paragraph
        if (worstDomain && worstDomain.reds >= 1) {
          const domainKpiList = worstDomainRedKpis
          paragraphs.push(
            <p key="domain" className="text-[13px] text-white/90 leading-relaxed">
              {domainChip(worstDomain)} is the most pressured domain with {worstDomain.reds} of {worstDomain.total} metrics in the red
              {domainKpiList.length > 0 && (
                <>, concentrated in {domainKpiList.map((k, i) => (
                  <span key={k.key}>{i > 0 ? ' and ' : ' '}{kpiChip(k)}</span>
                ))}</>
              )}.{' '}
              {worstDomain.reds >= 2
                ? `Left unresolved, domain-level failures tend to create compounding cross-functional drag — revenue, cost and customer outcomes become mutually reinforcing.`
                : `Monitor whether this spreads — a single-metric domain failure can escalate quickly if the root cause is structural.`
              }
            </p>
          )
        }

        // — Streak warning: structural vs one-off
        if (worstStreaker && worstStreaker.s >= 2) {
          const stCause = worstStreaker.k.causation?.root_causes?.[0]
          const stFix   = worstStreaker.k.causation?.corrective_actions?.[0]
          paragraphs.push(
            <p key="streak" className="text-[13px] text-white/90 leading-relaxed">
              {kpiChip(worstStreaker.k)} has been below target for {worstStreaker.s} consecutive periods — at this point it is a structural issue, not a variance.{' '}
              {stCause ? `The most likely driver: ${stCause.charAt(0).toLowerCase() + stCause.slice(1)}. ` : ''}
              {allStreakers.length > 1 && `${allStreakers.length - 1} other KPI${allStreakers.length > 2 ? 's are' : ' is'} also in multi-period decline. `}
              {stFix ? `Recommended immediate action: ${stFix.charAt(0).toLowerCase() + stFix.slice(1)}.` : ''}
            </p>
          )
        }

        // — Retention / churn forward risk
        if (retentionKpi) {
          const retCause = retentionKpi.causation?.root_causes?.[0]
          const retFix   = retentionKpi.causation?.corrective_actions?.[0]
          paragraphs.push(
            <p key="retention" className="text-[13px] text-white/90 leading-relaxed">
              {kpiChip(retentionKpi)} at {fmt(retentionKpi.avg, retentionKpi.unit)} is an early-warning signal the P&L has not yet absorbed.{' '}
              Retention and churn failures typically materialise in revenue 2–3 quarters later — by the time they appear in the income statement, the damage is already set.{' '}
              {retCause ? `Root cause: ${retCause.charAt(0).toLowerCase() + retCause.slice(1)}. ` : ''}
              {retFix ? `Priority fix: ${retFix.charAt(0).toLowerCase() + retFix.slice(1)}.` : ''}
            </p>
          )
        }

        // — Watch zone: approaching critical
        if (top2Yellow.length > 0) {
          paragraphs.push(
            <p key="watchzone" className="text-[13px] text-white/90 leading-relaxed">
              Watch zone:{' '}
              {top2Yellow.map((k, i) => (
                <span key={k.key}>
                  {i > 0 ? ' and ' : ''}{kpiChip(k)} at {fmt(k.avg, k.unit)} vs target {fmt(k.target, k.unit)}
                </span>
              ))}{' '}
              {top2Yellow.length > 1 ? 'are' : 'is'} in the amber band and approaching the critical threshold.{' '}
              Without corrective action in this period, {top2Yellow.length > 1 ? 'these will' : 'this will'} increase the critical count next period.
            </p>
          )
        }

        // — Bright spot + protect signal
        if (bestDomain) {
          const bsKpi = bestDomain.kpis.filter(k => k.fy_status === 'green')
            .sort((a, b) => (gapPct(b) || 0) - (gapPct(a) || 0))[0]
          paragraphs.push(
            <p key="brightspot" className="text-[13px] text-white/90 leading-relaxed">
              Bright spot: {domainChip(bestDomain)} is the one clear area of strength — {bestDomain.greens} of {bestDomain.total} metrics on or above target
              {bsKpi ? <>, with {kpiChip(bsKpi)} leading at {fmt(bsKpi.avg, bsKpi.unit)} vs target {fmt(bsKpi.target, bsKpi.unit)}</> : ''}.{' '}
              Protect this domain from resource reallocation toward problem areas — it is absorbing stability that would otherwise amplify the pressure elsewhere.
            </p>
          )
        } else if (greenKpis.length > 0 && strongestGreen) {
          paragraphs.push(
            <p key="brightspot" className="text-[13px] text-white/90 leading-relaxed">
              {kpiChip(strongestGreen)} remains on target at {fmt(strongestGreen.avg, strongestGreen.unit)} — the most important individual bright spot this period. Protect it.
            </p>
          )
        } else if (greenKpis.length === 0) {
          paragraphs.push(
            <p key="brightspot" className="text-[13px] text-white/90 leading-relaxed">
              No KPIs are currently on target. This is the most urgent signal in the data — immediate cross-functional intervention is required before the position deteriorates further.
            </p>
          )
        }

        return (
          <div className="rounded-2xl overflow-hidden border border-white/10"
            style={{ background: 'linear-gradient(135deg, #071e45 0%, #0a2d6e 45%, #0d3d8e 100%)' }}>
            {/* Label bar */}
            <div className="flex items-center gap-2 px-6 pt-5 pb-0">
              <Zap size={10} className="text-teal-400"/>
              <span className="text-[10px] font-bold text-teal-400 uppercase tracking-widest">Executive Signal</span>
              <span className="text-[10px] text-white/30 mx-1">·</span>
              <span className="text-[10px] font-semibold text-white/50">{periodDisplay}</span>
              <span className="text-[10px] text-white/20 ml-2">{dateStr}</span>
            </div>

            {/* Single-column narrative */}
            <div className="px-6 pt-4 pb-5 space-y-3">
              {paragraphs}
              <p className="text-[10px] text-white/25 pt-1">
                Click any underlined term to open detailed analysis
              </p>
            </div>
          </div>
        )
      })()}

      {/* ── SIGNALS ──────────────────────────────────────────────────────────── */}
      {signals.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-3">
            <Zap size={13} className="text-slate-500"/>
            <span className="text-[11px] font-black text-slate-600 uppercase tracking-widest">Signals Not Visible in the Financials</span>
            <div className="flex-1 h-px bg-slate-100"/>
            <span className="text-[10px] text-slate-400">{periodDisplay}</span>
          </div>
          <div className={`grid gap-4 ${signals.length === 1 ? 'grid-cols-1' : 'grid-cols-1 sm:grid-cols-2'}`}>
            {signals.map((sig, i) => (
              <HiddenSignalCard key={i} signal={sig} onNavigate={onNavigate} onExpand={setSideCard}/>
            ))}
          </div>
        </div>
      )}

      {/* ── KPI STATUS — Critical | Watch | Strong ───────────────────────── */}
      <div id="kpi-status-section">
        <div className="flex items-center gap-2 mb-3">
          <AlertCircle size={13} className="text-slate-500"/>
          <span className="text-[11px] font-black text-slate-600 uppercase tracking-widest">KPI Status</span>
          <div className="flex-1 h-px bg-slate-100"/>
          <span className="text-[10px] text-slate-400">{periodDisplay}</span>
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">

          {/* Critical */}
          <div className="rounded-2xl border border-red-200 overflow-hidden bg-white shadow-sm">
            <div style={{ height: 4, background: '#ef4444' }}/>
            <div className="p-4">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <AlertCircle size={14} className="text-red-500"/>
                  <span className="text-[11px] font-black text-red-700 uppercase tracking-wider">Critical</span>
                  <span className="text-[11px] font-bold text-red-500 bg-red-50 border border-red-200 rounded-full px-1.5">{redKpis.length}</span>
                </div>
                <NavPill tabId="dashboard" onNavigate={onNavigate}/>
              </div>
            {redKpis.length === 0 ? (
              <div className="py-6 text-center">
                <CheckCircle2 size={22} className="text-emerald-400 mx-auto mb-1.5"/>
                <p className="text-[12px] text-slate-400">No critical KPIs</p>
              </div>
            ) : (
              <div className="space-y-2">
                {atRisk.filter(k => k.fy_status === 'red').slice(0, 6).map((kpi, i) => (
                  <KpiStatusRow key={kpi.key} kpi={kpi} rank={i+1} onNavigate={onNavigate}
                    onExpand={setSideCard}/>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Watch */}
        <div className="rounded-2xl border border-amber-200 overflow-hidden bg-white shadow-sm hover:shadow-[0_8px_32px_rgba(245,158,11,0.12)] transition-shadow duration-200">
          <div style={{ height: 4, background: '#f59e0b' }}/>
          <div className="p-4">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2">
                <Eye size={14} className="text-amber-500"/>
                <span className="text-[11px] font-black text-amber-700 uppercase tracking-wider">Watch</span>
                <span className="text-[11px] font-bold text-amber-500 bg-amber-50 border border-amber-200 rounded-full px-1.5">{yellowKpis.length}</span>
              </div>
              <NavPill tabId="dashboard" onNavigate={onNavigate}/>
            </div>
            {yellowKpis.length === 0 ? (
              <div className="py-6 text-center">
                <CheckCircle2 size={22} className="text-emerald-400 mx-auto mb-1.5"/>
                <p className="text-[12px] text-slate-400">Nothing in the watch zone</p>
              </div>
            ) : (
              <div className="space-y-2">
                {atRisk.filter(k => k.fy_status === 'yellow').slice(0, 6).map((kpi, i) => (
                  <KpiStatusRow key={kpi.key} kpi={kpi} rank={i+1} onNavigate={onNavigate}
                    onExpand={setSideCard}/>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Strong */}
        <div className="rounded-2xl border border-emerald-200 overflow-hidden bg-white shadow-sm hover:shadow-[0_8px_32px_rgba(5,150,105,0.12)] transition-shadow duration-200">
          <div style={{ height: 4, background: '#059669' }}/>
          <div className="p-4">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2">
                <CheckCircle2 size={14} className="text-emerald-500"/>
                <span className="text-[11px] font-black text-emerald-700 uppercase tracking-wider">Strong</span>
                <span className="text-[11px] font-bold text-emerald-600 bg-emerald-50 border border-emerald-200 rounded-full px-1.5">{greenKpis.length}</span>
              </div>
              <NavPill tabId="fingerprint" onNavigate={onNavigate}/>
            </div>
            {greenKpis.length === 0 ? (
              <div className="py-6 text-center">
                <p className="text-[12px] text-slate-400">No on-target KPIs yet</p>
              </div>
            ) : (
              <div className="space-y-2">
                {strongSorted.slice(0, 8).map(kpi => {
                  const gap  = gapPct(kpi)
                  const gStr = greenStreak(kpi)
                  return (
                    <div key={kpi.key}
                      className="flex items-center justify-between py-2 px-3 rounded-xl bg-emerald-50 border border-emerald-100 hover:bg-emerald-100/60 hover:border-emerald-200 hover:shadow-sm cursor-pointer transition-all duration-150 group"
                      onClick={() => setSideCard({ type: 'kpi', kpi })}>
                      <div className="flex items-center gap-2 min-w-0">
                        <span className="text-[12px] font-semibold text-slate-700 truncate">{kpi.name}</span>
                        {gStr >= 3 && (
                          <span className="text-[9px] font-bold text-emerald-600 bg-emerald-100 border border-emerald-200 px-1.5 py-0.5 rounded-full flex-shrink-0">
                            {gStr}mo ✓
                          </span>
                        )}
                      </div>
                      <div className="flex items-center gap-2 flex-shrink-0 ml-2">
                        <span className="font-mono text-[11px] font-bold text-slate-600">{fmt(kpi.avg, kpi.unit)}</span>
                        {gap != null && <span className="text-[11px] font-bold text-emerald-600">+{gap.toFixed(1)}%</span>}
                        <Maximize2 size={10} className="text-slate-300 group-hover:text-slate-400 transition-colors flex-shrink-0"/>
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        </div>
        </div>{/* end 3-col grid */}
      </div>{/* end KPI Status section */}

      {/* ── DOMAIN STORIES ───────────────────────────────────────────────── */}
      {storyDomains.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-3">
            <BarChart3 size={13} className="text-slate-500"/>
            <span className="text-[11px] font-black text-slate-600 uppercase tracking-widest">The Story by Domain</span>
            <div className="flex-1 h-px bg-slate-100"/>
            <span className="text-[10px] text-slate-400">{periodDisplay}</span>
          </div>
          <div className={`grid gap-4 ${storyDomains.length >= 4 ? 'grid-cols-1 sm:grid-cols-2 lg:grid-cols-4' : `grid-cols-1 sm:grid-cols-${storyDomains.length}`}`}>
            {storyDomains.map(domain => (
              <DomainStoryCard key={domain} domain={domain} kpis={domainGroups[domain] || []}
                onNavigate={onNavigate} onExpand={setSideCard}/>
            ))}
          </div>
        </div>
      )}

      {/* ── PERFORMANCE SNAPSHOT ─────────────────────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">

        {/* Radar */}
        <div className="rounded-2xl border border-slate-200 bg-white overflow-hidden lg:col-span-1 cursor-pointer hover:shadow-[0_8px_32px_rgba(0,85,164,0.12)] transition-all duration-200 hover:-translate-y-0.5"
          onClick={() => onNavigate('fingerprint')}>
          <div style={{ height: 4, background: SOURCE.fingerprint.color }}/>
          <div className="p-5">
            <div className="flex items-center justify-between mb-1">
              <span className="text-[12px] font-black text-slate-700">Performance Radar</span>
              <NavPill tabId="fingerprint" onNavigate={onNavigate}/>
            </div>
            <p className="text-[10px] text-slate-400 mb-2">All KPIs normalised to % of target (100 = on target)</p>
            <ResponsiveContainer width="100%" height={200}>
              <RadarChart data={radarData} margin={{ top: 8, right: 20, bottom: 8, left: 20 }}>
                <PolarGrid stroke="#f1f5f9"/>
                <PolarAngleAxis dataKey="kpi" tick={{ fill: '#94a3b8', fontSize: 8 }}/>
                <Radar name="Target" dataKey="target" stroke="#e2e8f0" strokeWidth={1} strokeDasharray="4 3" fill="none"/>
                <Radar name="Actual" dataKey="actual" stroke="#0055A4" fill="#0055A4" fillOpacity={0.18} strokeWidth={2}
                  dot={{ fill: '#0055A4', r: 2, strokeWidth: 0 }}/>
              </RadarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Consecutive Misses */}
        <div className="rounded-2xl border border-slate-200 bg-white overflow-hidden cursor-pointer hover:shadow-[0_8px_32px_rgba(239,68,68,0.12)] transition-all duration-200 hover:-translate-y-0.5"
          onClick={() => onNavigate('fingerprint')}>
          <div style={{ height: 4, background: '#ef4444' }}/>
          <div className="p-5">
            <div className="flex items-center justify-between mb-1">
              <span className="text-[12px] font-black text-slate-700">Consecutive Misses</span>
              <NavPill tabId="fingerprint" onNavigate={onNavigate}/>
            </div>
            <p className="text-[10px] text-slate-400 mb-4">KPIs with 2+ consecutive red months — the longer the streak, the more structural the problem</p>
            {streakAlerts.length === 0 ? (
              <div className="py-8 text-center">
                <CheckCircle2 size={24} className="text-emerald-400 mx-auto mb-2"/>
                <p className="text-[12px] text-slate-400">No consecutive misses detected</p>
              </div>
            ) : (
              <div className="space-y-3">
                {streakAlerts.map(k => {
                  const width = Math.min((k.streak / 12) * 100, 100)
                  return (
                    <div key={k.key}
                      className="cursor-pointer hover:bg-red-50/50 rounded-lg px-1 py-0.5 -mx-1 transition-colors"
                      onClick={e => { e.stopPropagation(); setSideCard({ type: 'kpi', kpi: k }) }}>
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-[12px] font-semibold text-slate-700">{k.name}</span>
                        <span className="flex items-center gap-1 text-[11px] text-red-500 font-bold">
                          {k.streak >= 4 && <span className="w-1.5 h-1.5 rounded-full bg-red-500 animate-pulse"/>}
                          {k.streak} months
                        </span>
                      </div>
                      <div className="h-1.5 bg-red-50 rounded-full overflow-hidden border border-red-100">
                        <div className="h-full bg-red-400 rounded-full transition-all" style={{ width: `${width}%` }}/>
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        </div>

        {/* Bridge / Forecast */}
        <div className="rounded-2xl border overflow-hidden cursor-pointer hover:shadow-[0_8px_32px_rgba(217,119,6,0.12)] transition-all duration-200 hover:-translate-y-0.5 bg-white"
          style={{ borderColor: bridgeData?.has_overlap ? '#fde68a' : '#e2e8f0' }}
          onClick={() => onNavigate('projection')}>
          <div style={{ height: 4, background: SOURCE.projection.color }}/>
          <div className="p-5">
            <div className="flex items-center justify-between mb-1">
              <span className="text-[12px] font-black text-slate-700">Forecast vs Actuals</span>
              <NavPill tabId="projection" onNavigate={onNavigate}/>
            </div>
            {bridgeData?.has_projection && bridgeData?.has_overlap ? (
              <>
                <p className="text-[10px] text-slate-400 mb-4">How actuals are tracking against the projection plan</p>
                <div className="grid grid-cols-2 gap-2 mb-4">
                  {[
                    { label: 'On Track',    value: bridgeData.summary?.on_track, color: '#059669', bg: '#f0fdf4' },
                    { label: 'Behind Plan', value: bridgeData.summary?.behind,   color: '#dc2626', bg: '#fef2f2' },
                    { label: 'Ahead',       value: bridgeData.summary?.ahead,    color: '#0055A4', bg: '#eff6ff' },
                    { label: 'Months',      value: bridgeData.summary?.total_months_compared, color: '#64748b', bg: '#f8fafc' },
                  ].map(t => (
                    <div key={t.label} className="rounded-xl p-2.5 text-center border border-slate-100" style={{ background: t.bg }}>
                      <div className="text-xl font-black" style={{ color: t.color }}>{t.value ?? '—'}</div>
                      <div className="text-[9px] font-bold text-slate-500 uppercase tracking-wide mt-0.5">{t.label}</div>
                    </div>
                  ))}
                </div>
                {bridgeRisks.length > 0 && (
                  <div className="space-y-1.5">
                    <p className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">Top Gaps</p>
                    {bridgeRisks.slice(0,3).map(k => (
                      <div key={k.name} className="flex items-center justify-between text-[11px] py-1.5 px-2.5 rounded-lg bg-amber-50 border border-amber-100">
                        <span className="font-medium text-slate-700 truncate max-w-[120px]">{k.name}</span>
                        <span className={`font-bold flex-shrink-0 ml-2 ${k.avg_gap_pct < -3 ? 'text-red-500' : 'text-amber-600'}`}>
                          {k.avg_gap_pct != null ? `${k.avg_gap_pct > 0 ? '+' : ''}${k.avg_gap_pct.toFixed(1)}%` : '—'}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </>
            ) : (
              <div className="py-8 text-center">
                <Target size={24} className="text-amber-300 mx-auto mb-2"/>
                <p className="text-[12px] font-semibold text-amber-600 mb-1">No projection data</p>
                <p className="text-[11px] text-slate-400">Upload a projection CSV to see<br/>forecast vs actuals comparison</p>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* ── OUTLOOK ──────────────────────────────────────────────────────── */}
      <div className="rounded-2xl overflow-hidden border border-slate-200"
        style={{ background: 'linear-gradient(135deg, #0f172a 0%, #1e293b 100%)' }}>
        <div className="p-6">
          <div className="flex items-center gap-2 mb-4">
            <Target size={14} className="text-white/50"/>
            <span className="text-[11px] font-black text-white/50 uppercase tracking-widest">30–90 Day Outlook</span>
            <div className="flex-1 h-px bg-white/8"/>
            <span className="text-[10px] text-white/30">Derived from current signal patterns</span>
          </div>
          <div className="space-y-3">
            {outlook.map((bullet, i) => (
              <div key={i} className="flex items-start gap-3">
                <div className="w-5 h-5 rounded-full flex-shrink-0 flex items-center justify-center mt-0.5"
                  style={{ background: i === 0 && redKpis.length > 0 ? '#ef444425' : '#ffffff10', border: `1px solid ${i === 0 && redKpis.length > 0 ? '#ef444450' : '#ffffff15'}` }}>
                  <span className="text-[9px] font-black" style={{ color: i === 0 && redKpis.length > 0 ? '#fca5a5' : 'rgba(255,255,255,0.4)' }}>{i + 1}</span>
                </div>
                <p className="text-[12px] text-white/70 leading-relaxed">{bullet}</p>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* ── FUNDRAISING READINESS ─────────────────────────────────────────── */}
      {benchmarks && Object.keys(benchmarks).length > 0 && (() => {
        // Score each KPI vs peer benchmarks
        let aboveP75 = 0, aboveP50 = 0, belowP25 = 0, total = 0
        const scrutinyKpis = []
        const strengthKpis = []

        fp.forEach(kpi => {
          const bm = benchmarks[kpi.key]
          if (!bm || kpi.avg == null) return
          total++
          const isLower = kpi.direction === 'lower'
          const val = kpi.avg

          // For "lower is better" KPIs, being below p25 means being BETTER
          const isAboveP75 = isLower ? val < bm.p25 : val > bm.p75
          const isAboveP50 = isLower ? val < bm.p50 : val > bm.p50
          const isBelowP25 = isLower ? val > bm.p75 : val < bm.p25

          if (isAboveP75) { aboveP75++; strengthKpis.push(kpi) }
          else if (isAboveP50) aboveP50++
          if (isBelowP25) { belowP25++; scrutinyKpis.push(kpi) }
        })

        if (total === 0) return null

        const topQuartilePct = Math.round((aboveP75 / total) * 100)
        const bottomQuartilePct = Math.round((belowP25 / total) * 100)

        // Overall positioning
        const position = aboveP75 > belowP25 * 2 ? 'top quartile'
          : aboveP75 >= belowP25 ? 'median range'
          : 'bottom quartile'

        const posColor = position === 'top quartile' ? '#10b981' : position === 'median range' ? '#f59e0b' : '#ef4444'
        const nextRound = { seed: 'Series A', series_a: 'Series B', series_b: 'Series C', series_c: 'Growth/IPO' }[companyStage] || 'next round'

        return (
          <div className="rounded-2xl overflow-hidden border border-slate-200 bg-white shadow-sm">
            <div style={{ height: 4, background: posColor }}/>
            <div className="p-6">
              <div className="flex items-center gap-2 mb-4">
                <ArrowUpRight size={14} className="text-slate-500"/>
                <span className="text-[11px] font-black text-slate-600 uppercase tracking-widest">Fundraising Readiness</span>
                <div className="flex-1 h-px bg-slate-100"/>
                <span className="text-[10px] text-slate-400">vs {stageLabel(companyStage)} peers</span>
              </div>

              <div className="flex items-start gap-6 flex-wrap">
                {/* Position badge */}
                <div className="text-center px-4 py-3 rounded-xl border" style={{ borderColor: posColor + '40', background: posColor + '08' }}>
                  <div className="text-[28px] font-black leading-none" style={{ color: posColor }}>
                    {position === 'top quartile' ? 'T1' : position === 'median range' ? 'M' : 'B1'}
                  </div>
                  <div className="text-[10px] font-bold text-slate-500 uppercase mt-1 tracking-wide">
                    {position}
                  </div>
                </div>

                {/* Narrative */}
                <div className="flex-1 min-w-[260px]">
                  <p className="text-[13px] text-slate-700 leading-relaxed mb-3">
                    Based on current metrics, this business would be positioned in the{' '}
                    <span className="font-bold" style={{ color: posColor }}>{position}</span>{' '}
                    for a {nextRound} raise.{' '}
                    {topQuartilePct > 0 && <>{topQuartilePct}% of benchmarked KPIs exceed the 75th percentile. </>}
                    {bottomQuartilePct > 0 && <>{bottomQuartilePct}% fall below the 25th percentile. </>}
                  </p>

                  {scrutinyKpis.length > 0 && (
                    <div className="mb-2">
                      <span className="text-[10px] font-bold text-red-500 uppercase tracking-wider">Likely investor scrutiny:</span>
                      <div className="flex flex-wrap gap-1.5 mt-1">
                        {scrutinyKpis.slice(0, 4).map(k => (
                          <span key={k.key} className="text-[11px] font-medium text-red-700 bg-red-50 border border-red-200 px-2 py-0.5 rounded-full">
                            {k.name}
                          </span>
                        ))}
                      </div>
                    </div>
                  )}

                  {strengthKpis.length > 0 && (
                    <div>
                      <span className="text-[10px] font-bold text-emerald-600 uppercase tracking-wider">Likely investor strengths:</span>
                      <div className="flex flex-wrap gap-1.5 mt-1">
                        {strengthKpis.slice(0, 4).map(k => (
                          <span key={k.key} className="text-[11px] font-medium text-emerald-700 bg-emerald-50 border border-emerald-200 px-2 py-0.5 rounded-full">
                            {k.name}
                          </span>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        )
      })()}

      {/* ── SIDE PANEL ───────────────────────────────────────────────────── */}
      <SidePanel
        card={sideCard}
        fingerprint={fp}
        periodDisplay={periodDisplay}
        onNavigate={tabId => { setSideCard(null); onNavigate(tabId) }}
        onClose={() => setSideCard(null)}
      />

    </div>
  )
}
