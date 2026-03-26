import { useState, useEffect, useMemo, useCallback, Component } from 'react'
import axios from 'axios'
import {
  AlertCircle, AlertTriangle, CheckCircle2, User, Calendar,
  ChevronDown, ChevronUp, Download, TrendingDown, TrendingUp, X,
  Loader2, Clock, FileQuestion, ArrowRight
} from 'lucide-react'

// ── Core KPI set (12 metrics CFOs care about most) ──────────────────────────
const CORE_KPIS = new Set([
  'revenue_growth', 'arr_growth', 'gross_margin', 'burn_multiple',
  'nrr', 'churn_rate', 'cac_payback', 'sales_efficiency',
  'operating_margin', 'recurring_revenue', 'cash_conv_cycle', 'customer_concentration'
])

// ── Formatting helpers ───────────────────────────────────────────────────────
const UNIT_FMT = {
  pct: v => `${v?.toFixed(1)}%`,
  days: v => `${v?.toFixed(1)}d`,
  months: v => `${v?.toFixed(1)}mo`,
  ratio: v => `${v?.toFixed(2)}x`,
  '$': v => `$${v?.toFixed(1)}`,
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

// ── Status badge colors ──────────────────────────────────────────────────────
const STATUS_STYLES = {
  red:    { dot: 'bg-red-500',    text: 'text-red-600',    bg: 'bg-red-50',    border: 'border-red-200'    },
  yellow: { dot: 'bg-amber-400',  text: 'text-amber-600',  bg: 'bg-amber-50',  border: 'border-amber-200'  },
  green:  { dot: 'bg-emerald-500', text: 'text-emerald-600', bg: 'bg-emerald-50', border: 'border-emerald-200' },
}

// ── Trend arrow helper ───────────────────────────────────────────────────────
function TrendArrow({ values, direction }) {
  if (!values || values.length < 2) return null
  const last = values[values.length - 1]
  const prev = values[values.length - 2]
  if (last == null || prev == null) return null
  const up = last > prev
  const goodUp = direction !== 'lower'
  const isGood = (up && goodUp) || (!up && !goodUp)
  const Icon = up ? TrendingUp : TrendingDown
  return <Icon size={12} className={isGood ? 'text-emerald-500' : 'text-red-500'} />
}

// ── Main component ───────────────────────────────────────────────────────────
export default function VarianceCommand({ fingerprint, bridgeData, benchmarks, companyStage, periodLabel, onKpiClick }) {
  const [accountability, setAccountability] = useState({})
  const [showAll, setShowAll] = useState(false)
  const [expandedKpi, setExpandedKpi] = useState(null)
  const [saving, setSaving] = useState(null)
  const [showResolved, setShowResolved] = useState(false)
  const [smartActionsCache, setSmartActionsCache] = useState({})   // { [kpiKey]: { data, loading, error } }

  // ── Fetch accountability data ────────────────────────────────────────────
  useEffect(() => {
    axios.get('/api/accountability')
      .then(res => setAccountability(res.data || {}))
      .catch(() => {})
  }, [])

  // ── Fetch smart actions when a KPI is expanded ─────────────────────────
  const fetchSmartActions = useCallback((kpiKey) => {
    if (smartActionsCache[kpiKey]?.data || smartActionsCache[kpiKey]?.loading) return
    setSmartActionsCache(prev => ({ ...prev, [kpiKey]: { data: null, loading: true, error: false } }))
    axios.get(`/api/smart-actions/${kpiKey}?stage=${companyStage || 'series_b'}`)
      .then(res => {
        setSmartActionsCache(prev => ({ ...prev, [kpiKey]: { data: res.data, loading: false, error: false } }))
      })
      .catch(() => {
        setSmartActionsCache(prev => ({ ...prev, [kpiKey]: { data: null, loading: false, error: true } }))
      })
  }, [companyStage, smartActionsCache])

  // ── Save accountability field (supports notes) ─────────────────────────
  const saveAccountability = (kpiKey, field, value) => {
    const current = accountability[kpiKey] || { owner: '', due_date: '', status: 'open', notes: '', status_history: [] }
    const now = new Date().toISOString()
    let updated = { ...current, [field]: value, last_updated: now }

    // Track status changes in history
    if (field === 'status' && value !== current.status) {
      const history = [...(current.status_history || [])]
      history.push({ status: value, timestamp: now })
      updated.status_history = history
    }
    // Track owner assignment
    if (field === 'owner' && value && value !== current.owner) {
      const history = [...(updated.status_history || [])]
      history.push({ status: `assigned to ${value}`, timestamp: now })
      updated.status_history = history
    }

    setAccountability(prev => ({ ...prev, [kpiKey]: updated }))
    setSaving(kpiKey)
    axios.put(`/api/accountability/${kpiKey}`, updated)
      .catch(() => {})
      .finally(() => setSaving(null))
  }

  // ── Derived data ─────────────────────────────────────────────────────────
  const fp = fingerprint || []

  const atRiskKpis = useMemo(() =>
    fp
      .filter(k => k.fy_status === 'red' || k.fy_status === 'yellow')
      .filter(k => showAll || CORE_KPIS.has(k.key))
      .sort((a, b) => {
        if (a.fy_status !== b.fy_status) return a.fy_status === 'red' ? -1 : 1
        return Math.abs(gapPct(b) || 0) - Math.abs(gapPct(a) || 0)
      }),
    [fp, showAll]
  )

  const onTargetKpis = useMemo(() =>
    fp.filter(k => k.fy_status === 'green'),
    [fp]
  )

  const resolvedKpis = useMemo(() =>
    atRiskKpis.filter(k => accountability[k.key]?.status === 'resolved'),
    [atRiskKpis, accountability]
  )

  const activeKpis = useMemo(() =>
    atRiskKpis.filter(k => accountability[k.key]?.status !== 'resolved'),
    [atRiskKpis, accountability]
  )

  const redCount = fp.filter(k => k.fy_status === 'red').length
  const yellowCount = fp.filter(k => k.fy_status === 'yellow').length
  const greenCount = fp.filter(k => k.fy_status === 'green').length

  // ── Benchmark context helper ─────────────────────────────────────────────
  const getBenchmarkContext = (kpi) => {
    const bm = benchmarks?.[kpi.key]
    if (!bm) return null
    const peerMedian = bm.median ?? bm.p50 ?? bm.benchmark
    if (peerMedian == null) return null
    const diff = kpi.avg != null ? ((kpi.avg / peerMedian - 1) * 100) : null
    return { peerMedian, diff }
  }

  // ── Recent trend: last 3 months ──────────────────────────────────────────
  const getRecentTrend = (kpi) => {
    const monthly = kpi.monthly || []
    const sorted = [...monthly]
      .filter(m => m.value != null)
      .sort((a, b) => (a.period || '').localeCompare(b.period || ''))
    return sorted.slice(-3)
  }

  // ── Render ───────────────────────────────────────────────────────────────
  return (
    <div className="space-y-5">
      {/* ── Top Banner ──────────────────────────────────────────────────── */}
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-[15px] font-bold text-slate-800 tracking-tight">
            Variance Command Center
          </h2>
          <p className="text-[11px] text-slate-500 mt-0.5">
            Period: {periodLabel || 'Full Year'}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {/* Toggle Core 12 / All */}
          <button
            onClick={() => setShowAll(v => !v)}
            className="px-3 py-1.5 rounded-lg border border-slate-200 text-[11px] font-medium text-slate-600 hover:bg-slate-50 transition-all"
          >
            {showAll ? 'Show Core 12' : `Show All ${fp.length}`}
          </button>
          {/* Download Weekly Briefing */}
          <button
            onClick={() => {
              const a = document.createElement('a')
              a.href = `/api/export/weekly-briefing.html?stage=${companyStage || 'series_b'}`
              a.download = 'weekly-briefing.html'
              a.click()
            }}
            className="flex items-center gap-1.5 px-3 py-2 bg-[#0055A4] hover:bg-[#003d80] rounded-xl text-[11px] text-white font-semibold transition-all"
          >
            <Download size={12} /> Weekly Briefing
          </button>
        </div>
      </div>

      {/* ── Summary Strip ───────────────────────────────────────────────── */}
      <div className="grid grid-cols-3 gap-3">
        <SummaryCard count={redCount} label="require action" color="red" />
        <SummaryCard count={yellowCount} label="to monitor" color="yellow" />
        <SummaryCard count={greenCount} label="on target" color="green" />
      </div>

      {/* ── At-Risk KPI Table ───────────────────────────────────────────── */}
      {activeKpis.length > 0 && (
        <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden">
          {/* Table Header */}
          <div className="grid grid-cols-[24px_1.2fr_0.7fr_0.7fr_0.7fr_1.2fr_1fr_0.8fr_0.7fr_28px] gap-2 px-4 py-2.5 border-b border-slate-100 bg-slate-50/50">
            <div />
            <div className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">KPI</div>
            <div className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Current</div>
            <div className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Target</div>
            <div className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Gap</div>
            <div className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Root Cause</div>
            <div className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Owner</div>
            <div className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Due Date</div>
            <div className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Status</div>
            <div />
          </div>

          {/* Table Rows */}
          {activeKpis.map((kpi, idx) => (
            <KpiRow
              key={kpi.key}
              kpi={kpi}
              idx={idx}
              accountability={accountability[kpi.key]}
              expanded={expandedKpi === kpi.key}
              saving={saving === kpi.key}
              benchmarkCtx={getBenchmarkContext(kpi)}
              recentTrend={getRecentTrend(kpi)}
              smartActions={smartActionsCache[kpi.key]}
              fingerprint={fp}
              onToggle={() => {
                const willExpand = expandedKpi !== kpi.key
                setExpandedKpi(willExpand ? kpi.key : null)
                if (willExpand) fetchSmartActions(kpi.key)
              }}
              onSave={(field, value) => saveAccountability(kpi.key, field, value)}
              onKpiClick={onKpiClick}
            />
          ))}
        </div>
      )}

      {activeKpis.length === 0 && (
        <div className="bg-white rounded-2xl border border-slate-200 p-8 text-center">
          <CheckCircle2 size={24} className="text-emerald-500 mx-auto mb-2" />
          <p className="text-[13px] font-medium text-slate-700">All KPIs are on target</p>
          <p className="text-[11px] text-slate-400 mt-1">No variances require attention for this period.</p>
        </div>
      )}

      {/* ── Resolved Section ────────────────────────────────────────────── */}
      {resolvedKpis.length > 0 && (
        <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden">
          <button
            onClick={() => setShowResolved(v => !v)}
            className="w-full flex items-center justify-between px-4 py-3 hover:bg-slate-50/50 transition-colors"
          >
            <div className="flex items-center gap-2">
              <CheckCircle2 size={14} className="text-emerald-500" />
              <span className="text-[12px] font-semibold text-slate-600">
                Resolved ({resolvedKpis.length})
              </span>
            </div>
            {showResolved ? <ChevronUp size={14} className="text-slate-400" /> : <ChevronDown size={14} className="text-slate-400" />}
          </button>
          {showResolved && (
            <div className="border-t border-slate-100 opacity-60">
              {resolvedKpis.map((kpi, idx) => (
                <div
                  key={kpi.key}
                  className={`grid grid-cols-[24px_1.2fr_0.7fr_0.7fr_0.7fr_1.2fr_1fr_0.8fr_0.7fr_28px] gap-2 px-4 py-2.5 items-center ${
                    idx % 2 === 0 ? 'bg-white' : 'bg-slate-50/30'
                  }`}
                >
                  <div className="flex items-center justify-center">
                    <span className={`w-2.5 h-2.5 rounded-full ${STATUS_STYLES[kpi.fy_status]?.dot || 'bg-slate-300'}`} />
                  </div>
                  <span className="text-[12px] text-slate-500 line-through">{kpi.name}</span>
                  <span className="text-[11px] text-slate-400">{fmt(kpi.avg, kpi.unit)}</span>
                  <span className="text-[11px] text-slate-400">{fmt(kpi.target, kpi.unit)}</span>
                  <GapCell kpi={kpi} />
                  <span className="text-[11px] text-slate-400 truncate">
                    {kpi.causation?.root_causes?.[0] || '—'}
                  </span>
                  <span className="text-[11px] text-slate-400">
                    {accountability[kpi.key]?.owner || '—'}
                  </span>
                  <span className="text-[11px] text-slate-400">
                    {accountability[kpi.key]?.due_date || '—'}
                  </span>
                  <span className="text-[10px] text-emerald-600 font-medium">resolved</span>
                  <div />
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* ── On-Target Summary ───────────────────────────────────────────── */}
      {onTargetKpis.length > 0 && (
        <div className="bg-emerald-50/40 rounded-2xl border border-emerald-100 px-4 py-3">
          <div className="flex items-center gap-2 mb-2">
            <CheckCircle2 size={13} className="text-emerald-500" />
            <span className="text-[11px] font-semibold text-emerald-700">On Target ({onTargetKpis.length})</span>
          </div>
          <div className="flex flex-wrap gap-1.5">
            {onTargetKpis.map(kpi => (
              <button
                key={kpi.key}
                onClick={() => onKpiClick?.(kpi.key)}
                className="px-2 py-1 rounded-md bg-white/70 border border-emerald-200 text-[10px] text-emerald-700 hover:bg-emerald-50 transition-colors cursor-pointer"
              >
                {kpi.name}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ── Summary Card ─────────────────────────────────────────────────────────────
function SummaryCard({ count, label, color }) {
  const styles = {
    red:    { bg: 'bg-red-50',     border: 'border-red-200',     text: 'text-red-700',     num: 'text-red-600',     Icon: AlertCircle    },
    yellow: { bg: 'bg-amber-50',   border: 'border-amber-200',   text: 'text-amber-700',   num: 'text-amber-600',   Icon: AlertTriangle  },
    green:  { bg: 'bg-emerald-50', border: 'border-emerald-200', text: 'text-emerald-700', num: 'text-emerald-600', Icon: CheckCircle2   },
  }
  const s = styles[color]
  return (
    <div className={`${s.bg} ${s.border} border rounded-xl px-4 py-3 flex items-center gap-3`}>
      <s.Icon size={18} className={s.num} />
      <div>
        <div className={`text-[20px] font-bold ${s.num} leading-none`}>{count}</div>
        <div className={`text-[10px] ${s.text} mt-0.5 font-medium`}>{label}</div>
      </div>
    </div>
  )
}

// ── Gap Cell ─────────────────────────────────────────────────────────────────
function GapCell({ kpi }) {
  const gap = gapPct(kpi)
  if (gap == null) return <span className="text-[11px] text-slate-400">—</span>
  const isNeg = gap < 0
  const color = kpi.fy_status === 'red' ? 'text-red-600' : 'text-amber-600'
  return (
    <span className={`text-[11px] font-semibold ${color} flex items-center gap-0.5`}>
      {isNeg ? <TrendingDown size={11} /> : <TrendingUp size={11} />}
      {isNeg ? '' : '+'}{gap.toFixed(1)}%
    </span>
  )
}

// ── KPI Row ──────────────────────────────────────────────────────────────────
function KpiRow({ kpi, idx, accountability: acct, expanded, saving, benchmarkCtx, recentTrend, smartActions, fingerprint, onToggle, onSave, onKpiClick }) {
  const acc = acct || { owner: '', due_date: '', status: 'open', notes: '', status_history: [] }
  const style = STATUS_STYLES[kpi.fy_status] || STATUS_STYLES.yellow

  return (
    <>
      {/* Main row */}
      <div
        className={`grid grid-cols-[24px_1.2fr_0.7fr_0.7fr_0.7fr_1.2fr_1fr_0.8fr_0.7fr_28px] gap-2 px-4 py-2.5 items-center transition-colors ${
          idx % 2 === 0 ? 'bg-white' : 'bg-slate-50/30'
        } ${expanded ? 'border-b border-slate-100' : ''}`}
      >
        {/* Status dot */}
        <div className="flex items-center justify-center">
          <span className={`w-2.5 h-2.5 rounded-full ${style.dot}`} />
        </div>

        {/* KPI Name */}
        <button
          onClick={() => onKpiClick?.(kpi.key)}
          className="text-[12px] font-semibold text-slate-800 hover:text-blue-600 text-left truncate transition-colors"
          title={kpi.name}
        >
          {kpi.name}
        </button>

        {/* Current */}
        <span className="text-[11px] text-slate-700 font-medium tabular-nums">
          {fmt(kpi.avg, kpi.unit)}
        </span>

        {/* Target */}
        <span className="text-[11px] text-slate-500 tabular-nums">
          {fmt(kpi.target, kpi.unit)}
        </span>

        {/* Gap */}
        <GapCell kpi={kpi} />

        {/* Root Cause */}
        <span className="text-[11px] text-slate-500 truncate" title={kpi.causation?.root_causes?.[0]}>
          {kpi.causation?.root_causes?.[0] || '---'}
        </span>

        {/* Owner input */}
        <div className="relative">
          <input
            type="text"
            placeholder="Assign..."
            defaultValue={acc.owner}
            onBlur={e => onSave('owner', e.target.value)}
            className="w-full text-[11px] text-slate-700 bg-transparent border-b border-slate-200 focus:border-blue-400 focus:outline-none focus:ring-0 py-0.5 px-0 placeholder:text-slate-300 transition-colors"
          />
          {saving && <span className="absolute right-0 top-0.5 text-[9px] text-blue-400">saving...</span>}
        </div>

        {/* Due Date input */}
        <input
          type="date"
          defaultValue={acc.due_date}
          onBlur={e => onSave('due_date', e.target.value)}
          className="text-[11px] text-slate-600 bg-transparent border-b border-slate-200 focus:border-blue-400 focus:outline-none focus:ring-0 py-0.5 px-0 transition-colors"
        />

        {/* Status select */}
        <select
          value={acc.status}
          onChange={e => onSave('status', e.target.value)}
          className={`text-[10px] font-medium rounded-md px-1.5 py-0.5 border-0 focus:ring-1 focus:ring-blue-300 cursor-pointer transition-colors ${
            acc.status === 'open'
              ? 'bg-red-50 text-red-600'
              : acc.status === 'in-progress'
              ? 'bg-amber-50 text-amber-600'
              : 'bg-emerald-50 text-emerald-600'
          }`}
        >
          <option value="open">open</option>
          <option value="in-progress">in-progress</option>
          <option value="resolved">resolved</option>
        </select>

        {/* Expand toggle */}
        <button onClick={onToggle} className="flex items-center justify-center text-slate-400 hover:text-slate-600 transition-colors">
          {expanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
        </button>
      </div>

      {/* Expanded detail panel */}
      {expanded && (
        <SafeExpandedDetail
          kpi={kpi}
          benchmarkCtx={benchmarkCtx}
          recentTrend={recentTrend}
          smartActions={smartActions}
          fingerprint={fingerprint}
          accountability={acc}
          onSave={onSave}
          idx={idx}
        />
      )}
    </>
  )
}

// ── Mini Spark Trend (3 dots) ─────────────────────────────────────────────────
function SparkTrend({ recentTrend, kpi }) {
  if (!recentTrend || recentTrend.length === 0) return null
  return (
    <div className="flex items-center gap-1.5">
      {recentTrend.map((m, i) => (
        <span key={i} className="text-[10px] font-medium text-slate-600 tabular-nums">
          {fmt(m.value, kpi.unit)}
        </span>
      ))}
      <TrendArrow values={recentTrend.map(m => m.value)} direction={kpi.direction} />
    </div>
  )
}

// ── Status Timeline ──────────────────────────────────────────────────────────
function StatusTimeline({ history }) {
  if (!history || history.length === 0) return null
  const fmtDate = (ts) => {
    if (!ts) return ''
    const d = new Date(ts)
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
  }
  return (
    <div className="flex items-center gap-1 flex-wrap">
      {history.map((entry, i) => (
        <span key={i} className="flex items-center gap-1">
          {i > 0 && <ArrowRight size={9} className="text-slate-300" />}
          <span className="text-[10px] text-slate-500">
            <span className="capitalize font-medium">{entry.status}</span>
            {' '}
            <span className="text-slate-400">{fmtDate(entry.timestamp)}</span>
          </span>
        </span>
      ))}
    </div>
  )
}

// ── Priority Badge ───────────────────────────────────────────────────────────
function PriorityBadge({ priority }) {
  // priority may be a number (from API: 1,2,3) or string ("high","medium","low")
  let p = 'medium'
  if (typeof priority === 'number') {
    p = priority <= 1 ? 'high' : priority <= 2 ? 'medium' : 'low'
  } else if (typeof priority === 'string') {
    p = priority.toLowerCase()
  }
  const styles = {
    high:   'bg-red-100 text-red-700 border-red-200',
    medium: 'bg-amber-100 text-amber-700 border-amber-200',
    low:    'bg-slate-100 text-slate-600 border-slate-200',
  }
  return (
    <span className={`text-[9px] font-bold uppercase px-1.5 py-0.5 rounded border ${styles[p] || styles.medium}`}>
      {p}
    </span>
  )
}

// ── Safe wrapper to prevent crashes from blanking the whole page ──────────────
class ErrorCatcher extends Component {
  constructor(props) { super(props); this.state = { hasError: false, error: null } }
  static getDerivedStateFromError(error) { return { hasError: true, error } }
  render() {
    if (this.state.hasError) {
      return (
        <div className="px-6 py-4 border-b border-slate-100 bg-red-50/40">
          <p className="text-[12px] text-red-600 font-medium">Failed to load analysis panel</p>
          <p className="text-[10px] text-red-400 mt-1">{this.state.error?.message || 'Unknown error'}</p>
        </div>
      )
    }
    return this.props.children
  }
}

function SafeExpandedDetail(props) {
  return <ErrorCatcher><ExpandedDetail {...props} /></ErrorCatcher>
}

// ── Expanded Detail Panel ────────────────────────────────────────────────────
function ExpandedDetail({ kpi, benchmarkCtx, recentTrend, smartActions, fingerprint, accountability, onSave, idx }) {
  // Fall back to static causation data if smart actions failed or haven't loaded
  const sa = smartActions?.data
  const loading = smartActions?.loading
  const failed = smartActions?.error

  // Static fallback data from fingerprint causation
  const staticCauses = kpi.causation?.root_causes || []
  const staticDownstream = kpi.causation?.downstream_impact || kpi.causation?.impacted_kpis || []
  const staticCorrective = kpi.causation?.corrective_actions || kpi.causation?.recommended_actions || []

  // Smart actions data (or fallback)
  const upstreamCauses = sa?.upstream_causes || []
  const downstreamImpact = sa?.downstream_impact || []
  const actions = sa?.actions || []
  const dataGaps = sa?.data_gaps || []
  const quantifiedProblem = sa?.quantified_problem || null

  // Build upstream causes from fingerprint if API didn't return them
  const resolvedUpstream = upstreamCauses.length > 0 ? upstreamCauses : staticCauses.map(c => ({
    explanation: typeof c === 'string' ? c : (c?.explanation || c?.description || String(c || '')),
    kpi_key: null,
    status: null,
  }))
  const resolvedDownstream = downstreamImpact.length > 0 ? downstreamImpact : (staticDownstream || []).map(d =>
    typeof d === 'string' ? d : (d?.kpi_key || d?.key || d?.name || String(d || ''))
  )
  const resolvedActions = actions.length > 0 ? actions : staticCorrective.map((a, i) => ({
    action: typeof a === 'string' ? a : (a?.action || a?.description || String(a || '')),
    priority: 'medium',
    number: i + 1,
  }))

  // Lookup helper for fingerprint KPIs by key
  const fpLookup = useMemo(() => {
    const map = {}
    ;(fingerprint || []).forEach(k => { map[k.key] = k })
    return map
  }, [fingerprint])

  // Border color for upstream KPIs based on status
  const borderForStatus = (status) => {
    if (status === 'red') return 'border-l-red-500'
    if (status === 'yellow') return 'border-l-amber-400'
    return 'border-l-emerald-500'
  }

  const acc = accountability || {}

  // Loading state
  if (loading) {
    return (
      <div className="px-6 py-8 border-b border-slate-100 bg-slate-50/40">
        <div className="flex items-center justify-center gap-2 text-slate-400">
          <Loader2 size={16} className="animate-spin" />
          <span className="text-[12px]">Loading smart analysis...</span>
        </div>
      </div>
    )
  }

  return (
    <div className="px-4 py-4 border-b border-slate-100 bg-slate-50/20">
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">

        {/* ── Quantified Problem Banner ──────────────────────────────────── */}
        <div className="px-5 py-3 bg-gradient-to-r from-slate-50 to-white border-b border-slate-100">
          <div className="flex items-start justify-between gap-4">
            <div className="flex-1">
              {quantifiedProblem ? (
                <p className="text-[12px] text-slate-700 font-medium leading-relaxed">{quantifiedProblem}</p>
              ) : (
                <p className="text-[12px] text-slate-700 font-medium leading-relaxed">
                  {kpi.name} is at {fmt(kpi.avg, kpi.unit)}
                  {kpi.target != null && <>, {Math.abs(gapPct(kpi) || 0).toFixed(0)}% {(gapPct(kpi) || 0) < 0 ? 'below' : 'above'} target {fmt(kpi.target, kpi.unit)}</>}
                  {benchmarkCtx && <>. Peer median: {fmt(benchmarkCtx.peerMedian, kpi.unit)} — {Math.abs(benchmarkCtx.diff || 0).toFixed(0)}% {(benchmarkCtx.diff || 0) < 0 ? 'below' : 'above'} midpoint.</>}
                </p>
              )}
            </div>
            {/* Spark trend */}
            <div className="shrink-0">
              <SparkTrend recentTrend={recentTrend} kpi={kpi} />
            </div>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-0 divide-x divide-slate-100">
          {/* ── Left Column: Causes + Downstream ─────────────────────────── */}
          <div className="p-4 space-y-4">

            {/* Upstream Causes */}
            <div>
              <h4 className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-2">Upstream Causes</h4>
              {resolvedUpstream.length > 0 ? (
                <div className="space-y-2">
                  {resolvedUpstream.map((cause, i) => {
                    const causeKey = cause.kpi_key || cause.key
                    const causeKpi = causeKey ? fpLookup[causeKey] : null
                    const causeStatus = causeKpi?.fy_status || cause.status
                    return (
                      <div
                        key={i}
                        className={`rounded-lg border border-slate-100 bg-slate-50/50 p-2.5 border-l-[3px] ${borderForStatus(causeStatus)}`}
                      >
                        {causeKpi ? (
                          <>
                            <div className="flex items-center justify-between mb-1">
                              <span className="text-[11px] font-semibold text-slate-700">{causeKpi.name}</span>
                              <span className={`text-[10px] font-medium ${causeStatus === 'red' ? 'text-red-600' : causeStatus === 'yellow' ? 'text-amber-600' : 'text-emerald-600'}`}>
                                {fmt(causeKpi.avg, causeKpi.unit)} / {fmt(causeKpi.target, causeKpi.unit)}
                              </span>
                            </div>
                            {gapPct(causeKpi) != null && (
                              <div className="text-[10px] text-red-500 font-medium mb-1">
                                Gap: {gapPct(causeKpi).toFixed(1)}%
                              </div>
                            )}
                          </>
                        ) : cause.name ? (
                          <div className="flex items-center justify-between mb-1">
                            <span className="text-[11px] font-semibold text-slate-700">{cause.name}</span>
                            {cause.value != null && (
                              <span className="text-[10px] text-slate-500">{cause.value}{cause.target != null && ` / ${cause.target}`}</span>
                            )}
                          </div>
                        ) : null}
                        <p className="text-[10px] text-slate-500 leading-relaxed">{typeof cause === 'string' ? cause : (cause?.explanation || cause?.description || '')}</p>
                      </div>
                    )
                  })}
                </div>
              ) : (
                <p className="text-[11px] text-slate-400 italic">No upstream causes identified</p>
              )}
            </div>

            {/* Downstream Impact */}
            {resolvedDownstream.length > 0 && (
              <div>
                <h4 className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-2">Downstream Impact</h4>
                <div className="space-y-1.5">
                  {resolvedDownstream.map((d, i) => {
                    const dKey = typeof d === 'string' ? d : (d.kpi_key || d.key || d.kpi || d.name)
                    const dKpi = dKey ? fpLookup[dKey] : null
                    const dName = dKpi?.name || (typeof d === 'string' ? d : (d?.name || d?.kpi || d?.kpi_key || String(d || '')))
                    return (
                      <div key={i} className="flex items-center justify-between px-2.5 py-1.5 rounded-md bg-red-50/60 border border-red-100">
                        <span className="text-[11px] text-red-700 font-medium">{dName}</span>
                        {dKpi && (
                          <span className="text-[10px] text-red-500 tabular-nums">
                            {fmt(dKpi.avg, dKpi.unit)}
                          </span>
                        )}
                      </div>
                    )
                  })}
                </div>
              </div>
            )}

            {/* Data Gaps */}
            {dataGaps.length > 0 && (
              <div>
                <h4 className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-2">Data Needed</h4>
                <div className="space-y-1">
                  {dataGaps.map((gap, i) => (
                    <div key={i} className="flex items-start gap-1.5 text-[10px] text-amber-700">
                      <FileQuestion size={11} className="mt-0.5 shrink-0 text-amber-400" />
                      <span>{typeof gap === 'string' ? gap : (gap?.metric || gap?.description || String(gap || ''))}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* ── Right Column: Actions + Accountability ───────────────────── */}
          <div className="p-4 space-y-4">

            {/* Specific Actions */}
            <div>
              <h4 className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-2">Actions</h4>
              {resolvedActions.length > 0 ? (
                <div className="space-y-2">
                  {resolvedActions.map((a, i) => {
                    const actionText = typeof a === 'string' ? a : (a.action || a.description || String(a))
                    const num = a.number || i + 1
                    const priority = a.priority || 'medium'
                    const impact = a.expected_impact || a.impact
                    const owner = a.suggested_owner || a.owner
                    const timeframe = a.timeframe || a.timeline
                    return (
                      <div key={i} className="rounded-lg border border-slate-200 bg-white p-3 hover:shadow-sm transition-shadow">
                        <div className="flex items-start gap-2">
                          <div className="flex items-center justify-center w-5 h-5 rounded-full bg-blue-50 text-blue-600 text-[10px] font-bold shrink-0 mt-0.5">
                            {num}
                          </div>
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 mb-1">
                              <span className="text-[11px] text-slate-700 font-medium leading-snug">{actionText}</span>
                              <PriorityBadge priority={priority} />
                            </div>
                            <div className="flex flex-wrap gap-x-3 gap-y-0.5">
                              {impact && (
                                <span className="text-[9px] text-emerald-600 font-medium">
                                  Impact: {impact}
                                </span>
                              )}
                              {owner && (
                                <span className="text-[9px] text-slate-400 flex items-center gap-0.5">
                                  <User size={8} /> {owner}
                                </span>
                              )}
                              {timeframe && (
                                <span className="text-[9px] text-slate-400 flex items-center gap-0.5">
                                  <Clock size={8} /> {timeframe}
                                </span>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>
                    )
                  })}
                </div>
              ) : (
                <p className="text-[11px] text-slate-400 italic">No actions listed</p>
              )}
            </div>

            {/* ── Benchmark Context (compact) ─────────────────────────────── */}
            {benchmarkCtx && (
              <div className="bg-slate-50 rounded-lg p-2.5 border border-slate-100">
                <div className="flex items-center justify-between text-[11px]">
                  <span className="text-slate-500">Peer median</span>
                  <span className="font-medium text-slate-700">{fmt(benchmarkCtx.peerMedian, kpi.unit)}</span>
                </div>
                <div className="flex items-center justify-between text-[11px] mt-1">
                  <span className="text-slate-500">You</span>
                  <span className="font-medium text-slate-700">{fmt(kpi.avg, kpi.unit)}</span>
                </div>
                {benchmarkCtx.diff != null && (
                  <div className={`text-[10px] font-semibold mt-1.5 ${benchmarkCtx.diff >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>
                    {benchmarkCtx.diff >= 0 ? '+' : ''}{benchmarkCtx.diff.toFixed(1)}% vs. peers
                  </div>
                )}
              </div>
            )}

            {/* ── Accountability: Notes + Timeline ────────────────────────── */}
            <div className="border-t border-slate-100 pt-3 space-y-2">
              <h4 className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Accountability</h4>

              {/* Status timeline */}
              {acc.status_history && acc.status_history.length > 0 && (
                <StatusTimeline history={acc.status_history} />
              )}

              {/* Last updated */}
              {acc.last_updated && (
                <div className="flex items-center gap-1 text-[9px] text-slate-400">
                  <Clock size={9} />
                  Last updated: {new Date(acc.last_updated).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}
                </div>
              )}

              {/* Notes textarea */}
              <textarea
                placeholder="Add notes..."
                defaultValue={acc.notes || ''}
                onBlur={e => onSave('notes', e.target.value)}
                rows={2}
                className="w-full text-[11px] text-slate-600 bg-slate-50 border border-slate-200 rounded-lg px-2.5 py-1.5 focus:border-blue-400 focus:outline-none focus:ring-1 focus:ring-blue-200 placeholder:text-slate-300 resize-none transition-colors"
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
