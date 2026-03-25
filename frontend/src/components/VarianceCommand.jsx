import { useState, useEffect, useMemo } from 'react'
import axios from 'axios'
import {
  AlertCircle, AlertTriangle, CheckCircle2, User, Calendar,
  ChevronDown, ChevronUp, Download, TrendingDown, TrendingUp, X
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

  // ── Fetch accountability data ────────────────────────────────────────────
  useEffect(() => {
    axios.get('/api/accountability')
      .then(res => setAccountability(res.data || {}))
      .catch(() => {})
  }, [])

  // ── Save accountability field ────────────────────────────────────────────
  const saveAccountability = (kpiKey, field, value) => {
    const current = accountability[kpiKey] || { owner: '', due_date: '', status: 'open' }
    const updated = { ...current, [field]: value }
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
              onToggle={() => setExpandedKpi(expandedKpi === kpi.key ? null : kpi.key)}
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
function KpiRow({ kpi, idx, accountability: acct, expanded, saving, benchmarkCtx, recentTrend, onToggle, onSave, onKpiClick }) {
  const acc = acct || { owner: '', due_date: '', status: 'open' }
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
          {kpi.causation?.root_causes?.[0] || '—'}
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
        <ExpandedDetail
          kpi={kpi}
          benchmarkCtx={benchmarkCtx}
          recentTrend={recentTrend}
          idx={idx}
        />
      )}
    </>
  )
}

// ── Expanded Detail Panel ────────────────────────────────────────────────────
function ExpandedDetail({ kpi, benchmarkCtx, recentTrend, idx }) {
  const causes = kpi.causation?.root_causes || []
  const downstream = kpi.causation?.downstream_impact || kpi.causation?.impacted_kpis || []
  const corrective = kpi.causation?.corrective_actions || kpi.causation?.recommended_actions || []
  const annotations = kpi.annotations || kpi.causation?.annotations || []

  return (
    <div className={`px-6 py-4 ${idx % 2 === 0 ? 'bg-white' : 'bg-slate-50/30'} border-b border-slate-100`}>
      <div className="grid grid-cols-3 gap-6">
        {/* Left: Causation */}
        <div className="space-y-3">
          <h4 className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Root Causes</h4>
          {causes.length > 0 ? (
            <ul className="space-y-1.5">
              {causes.map((c, i) => (
                <li key={i} className="flex items-start gap-1.5 text-[11px] text-slate-600">
                  <AlertCircle size={11} className="text-red-400 mt-0.5 shrink-0" />
                  <span>{c}</span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-[11px] text-slate-400 italic">No root causes identified</p>
          )}

          {downstream.length > 0 && (
            <div className="mt-3">
              <h4 className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-1.5">Downstream Impact</h4>
              <div className="flex flex-wrap gap-1">
                {downstream.map((d, i) => (
                  <span key={i} className="px-2 py-0.5 rounded bg-red-50 text-red-600 text-[10px] font-medium border border-red-100">
                    {typeof d === 'string' ? d : d.kpi || d.name || d}
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Center: Corrective Actions */}
        <div className="space-y-3">
          <h4 className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Corrective Actions</h4>
          {corrective.length > 0 ? (
            <ul className="space-y-1.5">
              {corrective.map((a, i) => (
                <li key={i} className="flex items-start gap-1.5 text-[11px] text-slate-600">
                  <CheckCircle2 size={11} className="text-blue-400 mt-0.5 shrink-0" />
                  <span>{typeof a === 'string' ? a : a.action || a.description || a}</span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-[11px] text-slate-400 italic">No corrective actions listed</p>
          )}

          {annotations.length > 0 && (
            <div className="mt-3">
              <h4 className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-1.5">Annotations</h4>
              {annotations.map((note, i) => (
                <p key={i} className="text-[10px] text-slate-500 italic mb-1">{typeof note === 'string' ? note : note.text || note}</p>
              ))}
            </div>
          )}
        </div>

        {/* Right: Benchmark + Trend */}
        <div className="space-y-3">
          {benchmarkCtx && (
            <div>
              <h4 className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-1.5">Benchmark Context</h4>
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
            </div>
          )}

          <div>
            <h4 className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-1.5">Recent Trend</h4>
            {recentTrend.length > 0 ? (
              <div className="flex items-end gap-2">
                {recentTrend.map((m, i) => (
                  <div key={i} className="text-center">
                    <div className="text-[11px] font-medium text-slate-700 tabular-nums">{fmt(m.value, kpi.unit)}</div>
                    <div className="text-[9px] text-slate-400 mt-0.5">
                      {m.period?.split('-').slice(1).join('/') || `M${i + 1}`}
                    </div>
                  </div>
                ))}
                <TrendArrow
                  values={recentTrend.map(m => m.value)}
                  direction={kpi.direction}
                />
              </div>
            ) : (
              <p className="text-[11px] text-slate-400 italic">No trend data available</p>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
