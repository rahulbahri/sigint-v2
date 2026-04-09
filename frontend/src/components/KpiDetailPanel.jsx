import { useEffect } from 'react'
import AnnotationPanel from './AnnotationPanel.jsx'
import {
  X, TrendingUp, TrendingDown, Minus,
  Target, FlaskConical, Calendar, Star, AlertTriangle,
  ArrowRight, Lightbulb, AlertOctagon, CheckCircle2, BookMarked
} from 'lucide-react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ReferenceLine, ResponsiveContainer, Dot
} from 'recharts'

/* ── formatting helpers ─────────────────────────────── */
const UNIT_FMT = {
  pct:    v => `${v?.toFixed(1)}%`,
  days:   v => `${v?.toFixed(1)}d`,
  months: v => `${v?.toFixed(1)}mo`,
  ratio:  v => `${v?.toFixed(2)}x`,
}
function fmt(val, unit) {
  if (val == null) return '—'
  return (UNIT_FMT[unit] || (v => v?.toFixed(2)))(val)
}

const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

function cellStatus(val, target, direction) {
  if (val == null || !target) return 'grey'
  const pct = val / target
  if (direction === 'higher') return pct >= 0.98 ? 'green' : pct >= 0.90 ? 'yellow' : 'red'
  return pct <= 1.02 ? 'green' : pct <= 1.10 ? 'yellow' : 'red'
}

const STATUS_BADGE = {
  green:  'bg-emerald-100 text-emerald-700 border-emerald-200',
  yellow: 'bg-amber-100 text-amber-700 border-amber-200',
  red:    'bg-red-100 text-red-700 border-red-200',
  grey:   'bg-slate-100 text-slate-500 border-slate-200',
}
const STATUS_DOT = {
  green: 'bg-emerald-400', yellow: 'bg-amber-400', red: 'bg-red-400', grey: 'bg-slate-300'
}
const STATUS_LABEL = { green: 'On Target', yellow: 'Needs Attention', red: 'Critical', grey: 'No Target' }

/* ── custom dot: highlight best/worst ──────────────── */
function SmartDot({ cx, cy, value, isBest, isWorst }) {
  if (value == null) return null
  if (isBest || isWorst) {
    return (
      <Dot cx={cx} cy={cy} r={5}
        fill={isBest ? '#059669' : '#dc2626'}
        stroke="#fff" strokeWidth={1.5}/>
    )
  }
  return <Dot cx={cx} cy={cy} r={3} fill="#0055A4" stroke="#fff" strokeWidth={1}/>
}

/* ── stage label helper ─────────────────────────────── */
function stageLabel(s) {
  return ({ seed: 'Seed', series_a: 'Series A', series_b: 'Series B', series_c: 'Series C+' }[s] || s)
}

/* ── main panel ─────────────────────────────────────── */
export default function KpiDetailPanel({ kpi, onClose, periodLabel, benchmarks, companyStage }) {
  const isOpen = !!kpi

  /* Escape key to close */
  useEffect(() => {
    const handler = e => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [onClose])

  /* Build row data from actual filtered monthly periods (not hardcoded 12) */
  const sortedMonthly = [...(kpi?.monthly ?? [])].sort((a, b) => a.period.localeCompare(b.period))
  const multiYearPanel = new Set(sortedMonthly.map(m => m.period.split('-')[0])).size > 1

  const rows = sortedMonthly.map(m => {
    const [yr, mo] = m.period.split('-').map(Number)
    const val = m.value ?? null
    const pctOfTarget = (kpi?.target && val != null) ? (val / kpi.target) * 100 : null
    const st  = cellStatus(val, kpi?.target, kpi?.direction)
    const monthName = MONTHS[mo - 1]
    return { month: multiYearPanel ? `${monthName} ${yr}` : monthName, period: m.period, mo, yr, val, pctOfTarget, st }
  })

  const withVals = rows.filter(r => r.val != null)
  const bestRow  = withVals.length ? (kpi?.direction === 'higher'
    ? withVals.reduce((a, b) => a.val > b.val ? a : b)
    : withVals.reduce((a, b) => a.val < b.val ? a : b)) : null
  const worstRow = withVals.length ? (kpi?.direction === 'higher'
    ? withVals.reduce((a, b) => a.val < b.val ? a : b)
    : withVals.reduce((a, b) => a.val > b.val ? a : b)) : null

  /* Chart data */
  const chartData = rows.map(r => ({
    month: r.month,
    value: r.val,
    isBest:  bestRow  && r.period === bestRow.period,
    isWorst: worstRow && r.period === worstRow.period,
  }))

  const delta = (kpi?.avg != null && kpi?.target != null)
    ? kpi.avg - kpi.target : null
  const deltaGood = delta != null && (
    kpi?.direction === 'higher' ? delta >= 0 : delta <= 0
  )

  const pctOfTarget = (kpi?.avg != null && kpi?.target)
    ? ((kpi.avg / kpi.target) * 100).toFixed(1) : null

  return (
    <>
      {/* Overlay */}
      <div
        className={`fixed inset-0 bg-black/30 z-40 transition-opacity duration-300 ${
          isOpen ? 'opacity-100 pointer-events-auto' : 'opacity-0 pointer-events-none'
        }`}
        onClick={onClose}
      />

      {/* Panel */}
      <div className={`fixed top-0 right-0 h-screen w-[460px] bg-white shadow-2xl z-50
                       flex flex-col overflow-hidden
                       transition-transform duration-300 ease-in-out
                       ${isOpen ? 'translate-x-0' : 'translate-x-full'}`}>

        {!kpi ? null : <>

          {/* ── Header ─────────────────────────────────── */}
          <div className="flex-shrink-0 px-5 py-4 border-b border-slate-100 flex items-start justify-between gap-3 bg-white">
            <div className="min-w-0">
              <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">KPI Deep Dive</p>
              <h2 className="text-base font-bold text-slate-800 leading-snug">{kpi.name}</h2>
            </div>
            <div className="flex items-center gap-2 flex-shrink-0 pt-1">
              <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold border ${STATUS_BADGE[kpi.fy_status || 'grey']}`}>
                <span className={`w-1.5 h-1.5 rounded-full ${STATUS_DOT[kpi.fy_status || 'grey']}`}/>
                {STATUS_LABEL[kpi.fy_status || 'grey']}
              </span>
              <button onClick={onClose}
                className="w-7 h-7 rounded-lg flex items-center justify-center text-slate-400 hover:text-slate-700 hover:bg-slate-100 transition-colors">
                <X size={15}/>
              </button>
            </div>
          </div>

          {/* ── Scrollable body ─────────────────────────── */}
          <div className="flex-1 overflow-y-auto">

            {/* ── Hero metrics ──────────────────────────── */}
            <div className="px-5 py-4 bg-slate-50/60 border-b border-slate-100">
              <div className="grid grid-cols-3 gap-3">
                {/* FY Avg */}
                <div className="bg-white rounded-xl p-3 border border-slate-100 shadow-sm">
                  <p className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold mb-1">{periodLabel || 'Period'} Avg</p>
                  <p className="text-2xl font-bold text-slate-900 leading-none">{fmt(kpi.avg, kpi.unit)}</p>
                  {pctOfTarget && (
                    <p className={`text-xs mt-1 font-medium ${deltaGood ? 'text-emerald-600' : 'text-red-500'}`}>
                      {pctOfTarget}% of target
                    </p>
                  )}
                </div>

                {/* Target */}
                <div className="bg-white rounded-xl p-3 border border-slate-100 shadow-sm">
                  <p className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold mb-1">Target</p>
                  <p className="text-2xl font-bold text-slate-900 leading-none">{fmt(kpi.target, kpi.unit)}</p>
                  <p className="text-xs mt-1 text-slate-400">{kpi.direction === 'higher' ? '↑ Higher is better' : '↓ Lower is better'}</p>
                </div>

                {/* Delta + Trend */}
                <div className="bg-white rounded-xl p-3 border border-slate-100 shadow-sm">
                  <p className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold mb-1">vs Target</p>
                  {delta != null ? (
                    <p className={`text-2xl font-bold leading-none ${deltaGood ? 'text-emerald-600' : 'text-red-500'}`}>
                      {delta > 0 ? '+' : ''}{kpi.unit === 'pct' ? `${delta.toFixed(1)}pp` : fmt(delta, kpi.unit)}
                    </p>
                  ) : <p className="text-2xl font-bold text-slate-300">—</p>}
                  <div className="flex items-center gap-1 mt-1">
                    {kpi.trend === 'up'   && <TrendingUp  size={11} className={kpi.direction !== 'lower' ? 'text-emerald-500' : 'text-red-500'}/>}
                    {kpi.trend === 'down' && <TrendingDown size={11} className={kpi.direction === 'lower' ? 'text-emerald-500' : 'text-red-500'}/>}
                    {kpi.trend === 'flat' && <Minus size={11} className="text-slate-400"/>}
                    <span className="text-xs text-slate-400 capitalize">{kpi.trend || 'flat'} trend</span>
                  </div>
                </div>
              </div>
            </div>

            {/* ── Trend chart ───────────────────────────── */}
            <div className="px-5 py-4 border-b border-slate-100">
              <p className="text-xs font-semibold text-slate-600 mb-3 flex items-center gap-1.5">
                <Calendar size={12} className="text-slate-400"/>
                {periodLabel ? `${periodLabel} Trend` : 'Period Trend'}
                <span className="text-[10px] font-normal text-slate-400">({rows.length} months)</span>
              </p>
              <ResponsiveContainer width="100%" height={180}>
                <LineChart data={chartData} margin={{ top: 8, right: 8, bottom: 0, left: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9"/>
                  <XAxis dataKey="month" tick={{ fill: '#94a3b8', fontSize: 10 }}/>
                  <YAxis tick={{ fill: '#94a3b8', fontSize: 10 }} width={38}
                    tickFormatter={v => fmt(v, kpi.unit)}/>
                  <Tooltip
                    contentStyle={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, fontSize: 11, color: '#0f172a' }}
                    formatter={v => [fmt(v, kpi.unit), kpi.name]}/>
                  {kpi.target != null && (
                    <ReferenceLine y={kpi.target} stroke="#0055A4" strokeDasharray="4 4" strokeOpacity={0.5}
                      label={{ value: 'Target', fill: '#0055A4', fontSize: 9, position: 'insideTopRight' }}/>
                  )}
                  <Line type="monotone" dataKey="value" stroke="#0055A4" strokeWidth={2} connectNulls
                    dot={({ key, ...props }) => {
                      const row = chartData[props.index]
                      return <SmartDot key={key} {...props} isBest={row?.isBest} isWorst={row?.isWorst}/>
                    }}/>
                </LineChart>
              </ResponsiveContainer>
              {/* Legend */}
              <div className="flex items-center gap-4 mt-2 justify-center">
                <span className="flex items-center gap-1 text-[10px] text-slate-500">
                  <span className="w-2 h-2 rounded-full bg-emerald-500 inline-block"/>Best month
                </span>
                <span className="flex items-center gap-1 text-[10px] text-slate-500">
                  <span className="w-2 h-2 rounded-full bg-red-500 inline-block"/>Worst month
                </span>
                <span className="flex items-center gap-1 text-[10px] text-slate-500">
                  <span className="inline-block w-4 border-t-2 border-dashed border-[#0055A4]/50"/>Target
                </span>
              </div>
            </div>

            {/* ── Formula & Calculation ─────────────────── */}
            {kpi.formula && (
              <div className="px-5 py-4 border-b border-slate-100">
                <p className="text-xs font-semibold text-slate-600 mb-3 flex items-center gap-1.5">
                  <FlaskConical size={12} className="text-slate-400"/>
                  Formula &amp; Calculation
                </p>
                <div className="bg-slate-800 rounded-xl px-4 py-3">
                  <p className="text-emerald-400 text-xs font-mono leading-relaxed">{kpi.formula}</p>
                </div>
                <div className="mt-2.5 grid grid-cols-2 gap-2 text-[11px]">
                  <div className="bg-slate-50 rounded-lg px-3 py-2 border border-slate-100">
                    <span className="text-slate-400 font-medium">Unit: </span>
                    <span className="text-slate-700 font-semibold">{
                      kpi.unit === 'pct' ? 'Percentage (%)' :
                      kpi.unit === 'days' ? 'Days' :
                      kpi.unit === 'months' ? 'Months' :
                      kpi.unit === 'ratio' ? 'Ratio (×)' : kpi.unit
                    }</span>
                  </div>
                  <div className="bg-slate-50 rounded-lg px-3 py-2 border border-slate-100">
                    <span className="text-slate-400 font-medium">Optimise: </span>
                    <span className={`font-semibold ${kpi.direction === 'higher' ? 'text-emerald-600' : 'text-blue-600'}`}>
                      {kpi.direction === 'higher' ? 'Maximise ↑' : 'Minimise ↓'}
                    </span>
                  </div>
                </div>
              </div>
            )}

            {/* ── Status thresholds ─────────────────────── */}
            {kpi.target != null && (
              <div className="px-5 py-4 border-b border-slate-100">
                <p className="text-xs font-semibold text-slate-600 mb-3 flex items-center gap-1.5">
                  <Target size={12} className="text-slate-400"/>
                  Performance Thresholds
                </p>
                <div className="space-y-2">
                  {(kpi.direction === 'higher' ? [
                    { label: 'On Target',        range: `≥ ${fmt(kpi.target * 0.98, kpi.unit)}`,  cls: 'bg-emerald-50 border-emerald-200 text-emerald-700', dot: 'bg-emerald-400' },
                    { label: 'Needs Attention',  range: `${fmt(kpi.target * 0.90, kpi.unit)} – ${fmt(kpi.target * 0.98, kpi.unit)}`, cls: 'bg-amber-50 border-amber-200 text-amber-700', dot: 'bg-amber-400' },
                    { label: 'Critical',         range: `< ${fmt(kpi.target * 0.90, kpi.unit)}`,   cls: 'bg-red-50 border-red-200 text-red-700', dot: 'bg-red-400' },
                  ] : [
                    { label: 'On Target',        range: `≤ ${fmt(kpi.target * 1.02, kpi.unit)}`,  cls: 'bg-emerald-50 border-emerald-200 text-emerald-700', dot: 'bg-emerald-400' },
                    { label: 'Needs Attention',  range: `${fmt(kpi.target * 1.02, kpi.unit)} – ${fmt(kpi.target * 1.10, kpi.unit)}`, cls: 'bg-amber-50 border-amber-200 text-amber-700', dot: 'bg-amber-400' },
                    { label: 'Critical',         range: `> ${fmt(kpi.target * 1.10, kpi.unit)}`,   cls: 'bg-red-50 border-red-200 text-red-700', dot: 'bg-red-400' },
                  ]).map(thresh => (
                    <div key={thresh.label} className={`flex items-center justify-between px-3 py-2 rounded-lg border ${thresh.cls}`}>
                      <span className="flex items-center gap-1.5 text-xs font-semibold">
                        <span className={`w-2 h-2 rounded-full ${thresh.dot}`}/>
                        {thresh.label}
                      </span>
                      <span className="text-xs font-mono font-medium">{thresh.range}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* ── Best / Worst callout ──────────────────── */}
            {bestRow && worstRow && (
              <div className="px-5 py-4 border-b border-slate-100">
                <p className="text-xs font-semibold text-slate-600 mb-3 flex items-center gap-1.5">
                  <Star size={12} className="text-slate-400"/>
                  Highlights
                </p>
                <div className="grid grid-cols-2 gap-3">
                  <div className="bg-emerald-50 border border-emerald-100 rounded-xl p-3 text-center">
                    <p className="text-[10px] text-emerald-600 font-semibold uppercase tracking-wider mb-1">Best Month</p>
                    <p className="text-xl font-bold text-emerald-700">{fmt(bestRow.val, kpi.unit)}</p>
                    <p className="text-xs text-emerald-500 mt-0.5">{bestRow.month}</p>
                  </div>
                  <div className="bg-red-50 border border-red-100 rounded-xl p-3 text-center">
                    <p className="text-[10px] text-red-500 font-semibold uppercase tracking-wider mb-1">Worst Month</p>
                    <p className="text-xl font-bold text-red-600">{fmt(worstRow.val, kpi.unit)}</p>
                    <p className="text-xs text-red-400 mt-0.5">{worstRow.month}</p>
                  </div>
                </div>
                {bestRow.val != null && worstRow.val != null && (
                  <p className="text-[11px] text-slate-400 text-center mt-2">
                    Range spread: <span className="text-slate-600 font-semibold">{fmt(Math.abs(bestRow.val - worstRow.val), kpi.unit)}</span>
                  </p>
                )}
              </div>
            )}

            {/* ── 12-Month source data table ────────────── */}
            <div className="px-5 py-4">
              <p className="text-xs font-semibold text-slate-600 mb-3 flex items-center gap-1.5">
                <AlertTriangle size={12} className="text-slate-400"/>
                Monthly Source Data — {periodLabel || 'Selected Period'}
              </p>
              <div className="rounded-xl border border-slate-100 overflow-hidden">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="bg-slate-50 border-b border-slate-100">
                      <th className="text-left text-slate-500 font-semibold py-2 px-3">Month</th>
                      <th className="text-right text-slate-500 font-semibold py-2 px-3">Value</th>
                      <th className="text-right text-slate-500 font-semibold py-2 px-3">% of Target</th>
                      <th className="text-center text-slate-500 font-semibold py-2 px-3">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((r, i) => {
                      const isBestRow  = bestRow  && r.mo === bestRow.mo
                      const isWorstRow = worstRow && r.mo === worstRow.mo
                      const statusColors = {
                        green:  'text-emerald-700 bg-emerald-50',
                        yellow: 'text-amber-700 bg-amber-50',
                        red:    'text-red-600 bg-red-50',
                        grey:   'text-slate-400 bg-slate-50',
                      }
                      return (
                        <tr key={r.month}
                          className={`border-b border-slate-50 last:border-0 ${
                            isBestRow ? 'bg-emerald-50/40' : isWorstRow ? 'bg-red-50/30' : i % 2 ? 'bg-slate-50/30' : ''
                          }`}>
                          <td className="py-2 px-3 text-slate-700 font-medium flex items-center gap-1.5">
                            {r.month}
                            {isBestRow  && <span className="text-[9px] text-emerald-600 font-bold bg-emerald-100 px-1 rounded">BEST</span>}
                            {isWorstRow && <span className="text-[9px] text-red-500 font-bold bg-red-100 px-1 rounded">LOW</span>}
                          </td>
                          <td className="py-2 px-3 text-right font-mono font-medium text-slate-700">
                            {r.val != null ? fmt(r.val, kpi.unit) : <span className="text-slate-300">—</span>}
                          </td>
                          <td className="py-2 px-3 text-right font-mono text-slate-500">
                            {r.pctOfTarget != null ? `${r.pctOfTarget.toFixed(1)}%` : '—'}
                          </td>
                          <td className="py-2 px-3 text-center">
                            {r.val != null ? (
                              <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-bold ${statusColors[r.st]}`}>
                                {r.st.toUpperCase()}
                              </span>
                            ) : <span className="text-slate-300">—</span>}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                  {/* Summary row */}
                  {kpi.avg != null && (
                    <tfoot>
                      <tr className="bg-slate-100 border-t border-slate-200">
                        <td className="py-2 px-3 text-slate-600 font-bold">{periodLabel || 'Period'} Avg</td>
                        <td className="py-2 px-3 text-right font-mono font-bold text-slate-800">{fmt(kpi.avg, kpi.unit)}</td>
                        <td className="py-2 px-3 text-right font-mono text-slate-500">
                          {pctOfTarget ? `${pctOfTarget}%` : '—'}
                        </td>
                        <td className="py-2 px-3 text-center">
                          <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-bold ${STATUS_BADGE[kpi.fy_status || 'grey']}`}>
                            {(kpi.fy_status || 'grey').toUpperCase()}
                          </span>
                        </td>
                      </tr>
                    </tfoot>
                  )}
                </table>
              </div>
            </div>

            {/* ── Causation Analysis ────────────────────── */}
            {kpi.causation && (
              kpi.causation.root_causes?.length > 0 ||
              kpi.causation.downstream_impact?.length > 0 ||
              kpi.causation.corrective_actions?.length > 0
            ) && (() => {
              // Build period-anchored context string from actual filtered values
              const avgVal   = kpi.avg != null ? fmt(kpi.avg, kpi.unit) : null
              const tgtVal   = kpi.target != null ? fmt(kpi.target, kpi.unit) : null
              const gapPctV  = (kpi.avg != null && kpi.target != null && kpi.target !== 0)
                ? ((kpi.direction === 'lower'
                    ? (kpi.target - kpi.avg) / Math.abs(kpi.target)
                    : (kpi.avg - kpi.target) / Math.abs(kpi.target)) * 100).toFixed(1)
                : null
              const gapSign  = gapPctV != null ? (parseFloat(gapPctV) >= 0 ? '+' : '') : ''
              const trendTxt = kpi.trend === 'up' ? 'trending up' : kpi.trend === 'down' ? 'trending down' : 'flat'
              const stColor  = kpi.fy_status === 'red' ? '#dc2626' : kpi.fy_status === 'yellow' ? '#d97706' : '#059669'
              const periodCtx = [
                avgVal && tgtVal ? `Avg ${avgVal} vs target ${tgtVal}` : null,
                gapPctV != null ? `gap ${gapSign}${gapPctV}%` : null,
                `${trendTxt} over period`,
              ].filter(Boolean).join(' · ')
              const isGreen  = kpi.fy_status === 'green'
              const isYellow = kpi.fy_status === 'yellow'
              const ctxHeader = isGreen
                ? { label: 'Performance Context', Icon: CheckCircle2 }
                : isYellow
                ? { label: 'Performance Pressure', Icon: AlertTriangle }
                : { label: 'Upstream Drivers',     Icon: AlertOctagon }
              const rcTitle = isGreen
                ? 'Risk Factors Currently Managed Well'
                : isYellow
                ? 'Areas Requiring Attention'
                : 'Domain Analysis: Likely Drivers'
              const rcIntro = isGreen
                ? 'These factors are under control this period — sustaining strong performance'
                : isYellow
                ? 'Factors placing pressure on this metric in the selected period'
                : 'Based on financial domain expertise'
              const rcBullet = isGreen
                ? 'bg-emerald-100 text-emerald-600'
                : isYellow
                ? 'bg-amber-100 text-amber-600'
                : 'bg-red-100 text-red-600'
              const rcLabelColor = isGreen ? '#059669' : isYellow ? '#d97706' : '#ef4444'
              const actionTitle = isGreen
                ? 'How to Sustain This Performance'
                : 'Recommended Action Steps'
              return (
              <div className="px-5 py-4 border-t border-slate-100 space-y-5">
                <div>
                  <p className="text-xs font-semibold text-slate-600 flex items-center gap-1.5 mb-1.5">
                    <ctxHeader.Icon size={12} className="text-slate-400"/>
                    {ctxHeader.label}
                  </p>
                  {periodCtx && (
                    <div className="inline-flex items-center gap-1.5 text-[10.5px] font-semibold px-2.5 py-1 rounded-full border"
                      style={{ background: stColor + '12', color: stColor, borderColor: stColor + '35' }}>
                      <span className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ background: stColor }}/>
                      {periodLabel ? `${periodLabel}: ` : ''}{periodCtx}
                    </div>
                  )}
                  {isGreen && (
                    <p className="text-[10.5px] text-emerald-700 mt-2 font-medium">
                      This KPI is performing at or above target for the selected period. Analysis below reflects factors currently being managed effectively.
                    </p>
                  )}
                </div>

                {/* Domain Analysis — period-aware */}
                {kpi.causation?.root_causes?.length > 0 && (
                  <div>
                    <p className="text-[10px] uppercase tracking-wider font-semibold mb-1.5"
                      style={{ color: rcLabelColor }}>
                      {rcTitle}
                    </p>
                    <p className="text-[10px] text-slate-400 mb-2 italic">
                      {rcIntro}
                    </p>
                    <ol className="space-y-2">
                      {kpi.causation.root_causes.map((rc, i) => (
                        <li key={i} className="flex items-start gap-2.5">
                          <span className={`flex-shrink-0 w-5 h-5 rounded-full text-[9px] font-bold flex items-center justify-center mt-0.5 ${rcBullet}`}>
                            {isGreen ? '✓' : i + 1}
                          </span>
                          <span className="text-xs text-slate-600 leading-snug">{rc}</span>
                        </li>
                      ))}
                    </ol>
                  </div>
                )}

                {/* Downstream */}
                {kpi.causation?.downstream_impact?.length > 0 && (
                  <div>
                    <p className="text-[10px] uppercase tracking-wider font-semibold text-amber-600 mb-2.5">
                      {isGreen ? 'Metrics Benefiting from This Performance' : 'What This Affects Downstream'}
                    </p>
                    <div className="flex flex-wrap items-center gap-1.5">
                      {kpi.causation.downstream_impact.map((d, i) => (
                        <span key={d} className="flex items-center gap-1">
                          <span className="text-[11px] bg-amber-50 text-amber-700 border border-amber-200
                                           px-2.5 py-1 rounded-full font-medium">
                            {d.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                          </span>
                          {i < kpi.causation.downstream_impact.length - 1 && (
                            <ArrowRight size={11} className="text-slate-300 flex-shrink-0"/>
                          )}
                        </span>
                      ))}
                    </div>
                    <p className="text-[10px] text-slate-400 mt-1.5">
                      {isGreen
                        ? 'Strong performance here is creating positive downstream effects.'
                        : 'A sustained deviation here will propagate to the metrics above.'}
                    </p>
                  </div>
                )}

                {/* Action steps — period-aware label */}
                {kpi.causation?.corrective_actions?.length > 0 && (
                  <div className="bg-emerald-50/60 border border-emerald-100 rounded-xl p-4">
                    <p className="text-[10px] uppercase tracking-wider font-semibold text-emerald-700 mb-2.5 flex items-center gap-1.5">
                      <CheckCircle2 size={11} className="text-emerald-500"/>
                      {actionTitle}
                    </p>
                    <ul className="space-y-2">
                      {kpi.causation.corrective_actions.map((ca, i) => (
                        <li key={i} className="flex items-start gap-2.5">
                          <span className="flex-shrink-0 text-emerald-500 mt-0.5">
                            <CheckCircle2 size={13}/>
                          </span>
                          <span className="text-xs text-slate-600 leading-snug">{ca}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
              )
            })()}

            {/* ── vs Industry Peers ─────────────────────────── */}
            {benchmarks && kpi && benchmarks[kpi.key] && (() => {
              const bm = benchmarks[kpi.key]
              const avg = kpi.avg
              const isLower = kpi.direction === 'lower'

              // Compute position on the p25–p75 scale for the bar
              // We render a range bar showing p25, p50, p75 and a dot for company value
              const allVals = [bm.p25, bm.p50, bm.p75, avg].filter(v => v != null)
              const barMin = Math.min(...allVals) * (isLower ? 1.1 : 0.9)
              const barMax = Math.max(...allVals) * (isLower ? 0.9 : 1.1)
              const range = barMax - barMin || 1

              function pct(v) {
                return Math.max(0, Math.min(100, ((v - barMin) / range) * 100))
              }

              const companyPct = avg != null ? pct(avg) : null
              const pctFromMedian = (avg != null && bm.p50 != null && bm.p50 !== 0)
                ? ((avg - bm.p50) / Math.abs(bm.p50)) * 100
                : null
              const isAboveMedian = pctFromMedian != null
                ? (isLower ? pctFromMedian < 0 : pctFromMedian > 0)
                : null

              return (
                <div className="px-6 py-4 border-t border-slate-100">
                  <p className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-3">
                    vs Industry Peers ({stageLabel(companyStage)})
                  </p>

                  {/* Visual range bar */}
                  <div className="relative h-5 mb-3">
                    {/* Background track */}
                    <div className="absolute top-1/2 -translate-y-1/2 left-0 right-0 h-1.5 bg-slate-100 rounded-full"/>

                    {/* p25–p75 highlighted range */}
                    <div
                      className="absolute top-1/2 -translate-y-1/2 h-1.5 rounded-full bg-blue-100 border border-blue-200"
                      style={{
                        left: `${pct(bm.p25)}%`,
                        width: `${pct(bm.p75) - pct(bm.p25)}%`,
                      }}
                    />

                    {/* p25 marker */}
                    <div className="absolute top-0 h-full flex flex-col items-center" style={{ left: `${pct(bm.p25)}%` }}>
                      <div className="w-0.5 h-full bg-blue-300"/>
                    </div>

                    {/* p50 marker */}
                    <div className="absolute top-0 h-full flex flex-col items-center" style={{ left: `${pct(bm.p50)}%` }}>
                      <div className="w-0.5 h-full bg-blue-500"/>
                    </div>

                    {/* p75 marker */}
                    <div className="absolute top-0 h-full flex flex-col items-center" style={{ left: `${pct(bm.p75)}%` }}>
                      <div className="w-0.5 h-full bg-blue-300"/>
                    </div>

                    {/* Company dot */}
                    {companyPct != null && (
                      <div
                        className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2 w-3.5 h-3.5 rounded-full border-2 border-white shadow-md z-10"
                        style={{
                          left: `${companyPct}%`,
                          background: isAboveMedian ? '#059669' : '#dc2626',
                        }}
                      />
                    )}
                  </div>

                  {/* Labels */}
                  <div className="flex justify-between text-[9px] text-slate-400 mb-3">
                    <span>P25: {fmt(bm.p25, kpi.unit)}</span>
                    <span className="font-semibold text-blue-500">Median: {fmt(bm.p50, kpi.unit)}</span>
                    <span>P75: {fmt(bm.p75, kpi.unit)}</span>
                  </div>

                  {/* Verdict */}
                  {avg != null && pctFromMedian != null && (
                    <p className={`text-[11px] font-semibold ${isAboveMedian ? 'text-emerald-600' : 'text-red-500'}`}>
                      Your avg {fmt(avg, kpi.unit)} is {Math.abs(pctFromMedian).toFixed(1)}%{' '}
                      {isAboveMedian ? 'above' : 'below'} the {stageLabel(companyStage)} peer median.
                    </p>
                  )}
                  {avg == null && (
                    <p className="text-[11px] text-slate-400">No data available for comparison.</p>
                  )}

                  <p className="text-[9px] text-slate-300 mt-1.5">
                    Source: OpenView, Bessemer, SaaS Capital benchmarks
                  </p>
                </div>
              )
            })()}

            {/* ── Linked Decisions ───────────────────── */}
            {kpi?.linked_decisions?.length > 0 && (
              <div className="mt-6 pt-5 border-t border-slate-100 px-5">
                <p className="text-xs font-semibold text-slate-600 mb-3 flex items-center gap-1.5">
                  <BookMarked size={12} className="text-[#0055A4]"/>
                  Linked Decisions ({kpi.linked_decisions.length})
                </p>
                <div className="space-y-2">
                  {kpi.linked_decisions.map(dec => {
                    const statusStyle = {
                      active:   'bg-blue-50 text-blue-700 border-blue-200',
                      resolved: 'bg-emerald-50 text-emerald-700 border-emerald-200',
                      reversed: 'bg-amber-50 text-amber-700 border-amber-200',
                    }[dec.status] || 'bg-slate-50 text-slate-600 border-slate-200'
                    const delta = dec.baseline_value != null && dec.resolved_value != null
                      ? dec.resolved_value - dec.baseline_value : null
                    return (
                      <div key={dec.id} className="bg-slate-50 rounded-xl p-3 border border-slate-100">
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-[12px] font-semibold text-slate-700 truncate mr-2">{dec.title}</span>
                          <span className={`text-[9px] font-bold px-2 py-0.5 rounded-full border shrink-0 ${statusStyle}`}>
                            {dec.status}
                          </span>
                        </div>
                        <div className="flex items-center gap-3 text-[11px]">
                          {dec.baseline_value != null && (
                            <span className="text-slate-500">
                              Baseline: {fmt(dec.baseline_value, kpi.unit)}
                            </span>
                          )}
                          {delta != null && (
                            <span className={`font-bold ${delta >= 0 ? 'text-emerald-600' : 'text-red-500'}`}>
                              {delta > 0 ? '+' : ''}{fmt(delta, kpi.unit)}
                            </span>
                          )}
                        </div>
                        <p className="text-[10px] text-slate-400 mt-1">
                          {dec.decided_by} &middot; {dec.decided_at?.slice(0,10)}
                          {dec.outcome && <span className="ml-2 text-emerald-600">&mdash; {dec.outcome.slice(0, 60)}{dec.outcome.length > 60 ? '...' : ''}</span>}
                        </p>
                      </div>
                    )
                  })}
                </div>
              </div>
            )}

            {/* Annotations */}
            <div className="mt-6 pt-5 border-t border-slate-100 px-5 pb-4">
              <AnnotationPanel
                kpiKey={kpi?.key}
                periods={(kpi?.monthly || []).map(m => m.period)}
              />
            </div>

            {/* Bottom padding */}
            <div className="h-6"/>
          </div>
        </>}
      </div>
    </>
  )
}
