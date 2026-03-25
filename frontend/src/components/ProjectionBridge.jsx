import { useState, useEffect } from 'react'
import {
  BarChart, Bar, ComposedChart, Line, XAxis, YAxis,
  Tooltip, ResponsiveContainer, Cell, ReferenceLine
} from 'recharts'
import {
  AlertTriangle, TrendingDown, TrendingUp,
  Minus, ChevronDown, ChevronUp, X, GitBranch,
  ArrowRight
} from 'lucide-react'

// ─── Color constants ────────────────────────────────────────────────────────
const C_PROJECTED = '#3b82f6'
const C_AHEAD     = '#10b981'
const C_BEHIND    = '#ef4444'
const C_YELLOW    = '#f59e0b'

function statusColor(status) {
  if (status === 'green')  return C_AHEAD
  if (status === 'yellow') return C_YELLOW
  return C_BEHIND
}

function gapColor(gapPct) {
  if (gapPct >= 3)  return C_AHEAD
  if (gapPct >= -3) return '#94a3b8'
  if (gapPct >= -8) return C_YELLOW
  return C_BEHIND
}

function fmtVal(val, unit) {
  if (val === null || val === undefined || typeof val !== 'number') return '—'
  if (unit === 'pct')    return `${val.toFixed(2)}%`
  if (unit === 'ratio')  return val.toFixed(2)
  if (unit === 'days')   return `${val.toFixed(1)}d`
  if (unit === 'months') return `${val.toFixed(1)}mo`
  if (unit === 'usd')    return `$${val.toFixed(1)}K`
  return val.toFixed(2)
}

// ─── SummaryBanner ──────────────────────────────────────────────────────────
function SummaryBanner({ summary }) {
  const tiles = [
    { label: 'On Track',         value: summary.on_track,               color: 'emerald' },
    { label: 'Behind Plan',      value: summary.behind,                 color: 'red'     },
    { label: 'Ahead of Plan',    value: summary.ahead,                  color: 'blue'    },
    { label: 'Months Compared',  value: summary.total_months_compared,  color: 'slate'   },
  ]
  const colorMap = {
    emerald: { bg: 'bg-emerald-50',  border: 'border-emerald-200', text: 'text-emerald-700', sub: 'text-emerald-500' },
    red:     { bg: 'bg-red-50',      border: 'border-red-200',     text: 'text-red-700',     sub: 'text-red-400'     },
    blue:    { bg: 'bg-blue-50',     border: 'border-blue-200',    text: 'text-blue-700',    sub: 'text-blue-500'    },
    slate:   { bg: 'bg-slate-50',    border: 'border-slate-200',   text: 'text-slate-700',   sub: 'text-slate-500'   },
  }
  return (
    <div className="grid grid-cols-4 gap-4 mb-6">
      {tiles.map(({ label, value, color }) => {
        const c = colorMap[color]
        return (
          <div key={label} className={`card p-4 ${c.bg} border ${c.border}`}>
            <div className={`text-3xl font-bold ${c.text}`}>{value}</div>
            <div className={`text-[11px] mt-0.5 font-medium ${c.sub} uppercase tracking-wide`}>{label}</div>
          </div>
        )
      })}
    </div>
  )
}

// ─── WaterfallSummary ────────────────────────────────────────────────────────
function WaterfallSummary({ kpis }) {
  const data = Object.entries(kpis)
    .filter(([, v]) => v.avg_gap_pct != null && Math.abs(v.avg_gap_pct) > 0.5)
    .map(([, v]) => ({
      name:    v.name.length > 14 ? v.name.slice(0, 12) + '…' : v.name,
      gap:     parseFloat(v.avg_gap_pct?.toFixed(1)) || 0,
      status:  v.overall_status,
    }))
    .sort((a, b) => a.gap - b.gap)

  if (!data.length) return null

  return (
    <div className="card p-5 mb-6">
      <div className="mb-3">
        <h3 className="text-sm font-semibold text-slate-800">Gap to Target — All KPIs</h3>
        <p className="text-[11px] text-slate-500 mt-0.5">Bars below zero = behind plan · Sorted by severity</p>
      </div>
      <ResponsiveContainer width="100%" height={160}>
        <BarChart data={data} margin={{ top: 5, right: 10, bottom: 36, left: 30 }}>
          <XAxis dataKey="name" tick={{ fill: '#64748b', fontSize: 9 }} angle={-30} textAnchor="end" interval={0}/>
          <YAxis tick={{ fill: '#64748b', fontSize: 9 }} tickFormatter={v => `${v}%`}/>
          <Tooltip
            contentStyle={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, fontSize: 11 }}
            formatter={(v) => [`${v > 0 ? '+' : ''}${v}%`, 'Gap vs Plan']}
          />
          <ReferenceLine y={0} stroke="#94a3b8" strokeWidth={1.5}/>
          <Bar dataKey="gap" name="Gap %" radius={[3,3,0,0]} barSize={16}>
            {data.map((entry, i) => (
              <Cell key={i} fill={gapColor(entry.gap)}/>
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ─── OverviewChart ──────────────────────────────────────────────────────────
function OverviewChart({ kpis }) {
  const kpiList = Object.entries(kpis)
  const [selected, setSelected] = useState(
    kpiList.slice(0, 4).map(([k]) => k)
  )

  function toggle(key) {
    setSelected(prev =>
      prev.includes(key)
        ? prev.filter(k => k !== key)
        : prev.length < 6 ? [...prev, key] : prev
    )
  }

  const chartData = kpiList
    .filter(([k]) => selected.includes(k))
    .map(([k, v]) => ({
      name:       v.name.length > 18 ? v.name.slice(0, 16) + '…' : v.name,
      projected:  v.avg_projected,
      actual:     v.avg_actual,
      status:     v.overall_status,
      unit:       v.unit,
    }))

  return (
    <div className="card p-5 mb-6">
      <div className="flex items-start justify-between mb-4">
        <div>
          <h3 className="text-sm font-semibold text-slate-800">KPI Overview — Projected vs Actual</h3>
          <p className="text-[11px] text-slate-500 mt-0.5">Averages across compared months · Select up to 6 KPIs</p>
        </div>
        <div className="flex items-center gap-3 text-[10px] text-slate-500">
          <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm inline-block" style={{background: C_PROJECTED}}/> Projected</span>
          <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm inline-block bg-emerald-500"/> On Track</span>
          <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm inline-block bg-red-500"/> Behind</span>
        </div>
      </div>

      <div className="flex flex-wrap gap-1.5 mb-4">
        {kpiList.map(([k, v]) => (
          <button key={k}
            onClick={() => toggle(k)}
            className={`text-[10px] px-2.5 py-1 rounded-full border transition-all ${
              selected.includes(k)
                ? 'border-blue-400 bg-blue-50 text-blue-700'
                : 'border-slate-200 bg-white text-slate-500 hover:border-slate-300 hover:text-slate-700'
            }`}>
            {v.name}
          </button>
        ))}
      </div>

      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={chartData} margin={{ top: 5, right: 10, bottom: 40, left: 0 }}>
          <XAxis dataKey="name" tick={{ fill: '#64748b', fontSize: 10 }} angle={-30} textAnchor="end" interval={0}/>
          <YAxis tick={{ fill: '#64748b', fontSize: 10 }}/>
          <Tooltip
            contentStyle={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, fontSize: 11 }}
            labelStyle={{ color: '#475569' }}
          />
          <Bar dataKey="projected" name="Projected" fill={C_PROJECTED} radius={[3,3,0,0]} barSize={14} opacity={0.5}/>
          <Bar dataKey="actual"    name="Actual"    radius={[3,3,0,0]} barSize={14}>
            {chartData.map((entry, i) => (
              <Cell key={i} fill={entry.status === 'green' ? C_AHEAD : entry.status === 'yellow' ? C_YELLOW : C_BEHIND}/>
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ─── WaterfallMini ──────────────────────────────────────────────────────────
function WaterfallMini({ months }) {
  const entries = Object.entries(months).sort(([a],[b]) => a.localeCompare(b))
  if (!entries.length) return null

  const data = entries.map(([period, v]) => ({
    period:    period.slice(5),
    projected: v.projected,
    actual:    v.actual,
    gapPct:    v.gap_pct,
  }))

  return (
    <ResponsiveContainer width="100%" height={80}>
      <ComposedChart data={data} margin={{ top: 4, right: 4, bottom: 4, left: 0 }}>
        <XAxis dataKey="period" tick={{ fill: '#64748b', fontSize: 9 }} axisLine={false} tickLine={false}/>
        <Tooltip
          contentStyle={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 6, fontSize: 10 }}
          formatter={(val, name) => [typeof val === 'number' ? val.toFixed(2) : val, name]}
        />
        <Bar dataKey="projected" name="Projected" fill={`${C_PROJECTED}30`} barSize={8} radius={[2,2,0,0]}/>
        <Bar dataKey="actual"    name="Actual"    barSize={8} radius={[2,2,0,0]}>
          {data.map((entry, i) => (
            <Cell key={i} fill={gapColor(entry.gapPct) + 'cc'}/>
          ))}
        </Bar>
        <Line type="monotone" dataKey="projected" stroke={C_PROJECTED} strokeWidth={1.5}
              dot={false} strokeDasharray="3 3" name="Proj line"/>
      </ComposedChart>
    </ResponsiveContainer>
  )
}

// ─── BridgeDetailPanel ───────────────────────────────────────────────────────
function BridgeDetailPanel({ kpiData, onClose, onAskAnika }) {
  useEffect(() => {
    const handler = e => { if (e.key === 'Escape') onClose() }
    document.addEventListener('keydown', handler)
    return () => document.removeEventListener('keydown', handler)
  }, [onClose])

  if (!kpiData) return null

  const entries = Object.entries(kpiData.months).sort(([a],[b]) => a.localeCompare(b))
  const chartData = entries.map(([period, v]) => ({
    period:    period.slice(5),
    projected: v.projected,
    actual:    v.actual,
    gap:       v.gap,
    gapPct:    v.gap_pct,
  }))

  const statusBadgeCls = kpiData.overall_status === 'green'
    ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
    : kpiData.overall_status === 'yellow'
      ? 'bg-amber-50 text-amber-700 border-amber-200'
      : 'bg-red-50 text-red-700 border-red-200'

  const tileColors = {
    blue:    { bg: 'bg-blue-50',    border: 'border-blue-100',    text: 'text-blue-800',    sub: 'text-blue-500'    },
    emerald: { bg: 'bg-emerald-50', border: 'border-emerald-100', text: 'text-emerald-800', sub: 'text-emerald-500' },
    red:     { bg: 'bg-red-50',     border: 'border-red-100',     text: 'text-red-800',     sub: 'text-red-500'     },
    amber:   { bg: 'bg-amber-50',   border: 'border-amber-100',   text: 'text-amber-800',   sub: 'text-amber-500'   },
    slate:   { bg: 'bg-slate-50',   border: 'border-slate-100',   text: 'text-slate-700',   sub: 'text-slate-500'   },
  }

  const actualColor = kpiData.overall_status === 'green' ? 'emerald' : kpiData.overall_status === 'yellow' ? 'amber' : 'red'
  const gapPctColor = kpiData.avg_gap_pct >= 0 ? 'emerald' : 'red'
  const gapSign     = kpiData.avg_gap_pct > 0 ? '+' : ''

  const heroTiles = [
    { label: 'Avg Projected', value: fmtVal(kpiData.avg_projected, kpiData.unit), color: 'blue'       },
    { label: 'Avg Actual',    value: fmtVal(kpiData.avg_actual,    kpiData.unit), color: actualColor  },
    { label: 'Avg Gap',       value: `${gapSign}${kpiData.avg_gap_pct?.toFixed(1)}%`, color: gapPctColor },
    { label: 'Months',        value: entries.length,                               color: 'slate'      },
  ]

  function getCellClass(gapPct) {
    if (gapPct >= 3)  return 'hm-green'
    if (gapPct >= -3) return ''
    if (gapPct >= -8) return 'hm-yellow'
    return 'hm-red'
  }

  return (
    <>
      {/* Backdrop */}
      <div className="fixed inset-0 bg-black/30 z-40 transition-opacity" onClick={onClose}/>

      {/* Slide panel */}
      <div className="fixed inset-y-0 right-0 w-[520px] bg-white z-50 flex flex-col
                      shadow-2xl border-l border-slate-200
                      transform transition-transform duration-300 ease-in-out translate-x-0">

        {/* Header */}
        <div className="flex-shrink-0 flex items-start justify-between px-6 py-4
                        border-b border-slate-100">
          <div className="min-w-0 mr-3">
            <h2 className="text-base font-bold text-slate-800 leading-tight">{kpiData.name}</h2>
            <div className="flex items-center gap-2 mt-1.5">
              <span className={`text-[10px] px-2 py-0.5 rounded-full border font-semibold uppercase tracking-wide ${statusBadgeCls}`}>
                {kpiData.overall_status}
              </span>
              <span className="text-[10px] text-slate-400">Projection vs Actual — Monthly Detail</span>
            </div>
          </div>
          <button onClick={onClose}
            className="flex-shrink-0 text-slate-400 hover:text-slate-700 transition-colors p-1.5
                       rounded-lg hover:bg-slate-100 mt-0.5">
            <X size={15}/>
          </button>
        </div>

        {/* Scrollable body */}
        <div className="flex-1 overflow-y-auto px-6 py-4 space-y-5">

          {/* Hero tiles */}
          <div className="grid grid-cols-4 gap-2">
            {heroTiles.map(({ label, value, color }) => {
              const c = tileColors[color]
              return (
                <div key={label} className={`rounded-xl p-3 ${c.bg} border ${c.border}`}>
                  <div className={`text-lg font-bold leading-none ${c.text}`}>{value}</div>
                  <div className={`text-[9px] uppercase tracking-wide mt-1 font-medium ${c.sub}`}>{label}</div>
                </div>
              )
            })}
          </div>

          {/* Full waterfall chart */}
          <div>
            <p className="text-[10px] uppercase tracking-wider font-semibold text-slate-400 mb-2">
              Month-by-Month Chart
            </p>
            <ResponsiveContainer width="100%" height={260}>
              <ComposedChart data={chartData} margin={{ top: 8, right: 8, bottom: 5, left: 0 }}>
                <XAxis dataKey="period" tick={{ fill: '#64748b', fontSize: 10 }}/>
                <YAxis tick={{ fill: '#64748b', fontSize: 10 }} width={40}/>
                <Tooltip
                  contentStyle={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, fontSize: 11 }}
                  formatter={(val, name) => [typeof val === 'number' ? val.toFixed(2) : val, name]}
                />
                <ReferenceLine y={0} stroke="#e2e8f0"/>
                <Bar dataKey="projected" name="Projected" fill={`${C_PROJECTED}25`} barSize={16} radius={[2,2,0,0]}/>
                <Bar dataKey="actual"    name="Actual"    barSize={16} radius={[2,2,0,0]}>
                  {chartData.map((entry, i) => (
                    <Cell key={i} fill={gapColor(entry.gapPct) + 'dd'}/>
                  ))}
                </Bar>
                <Line type="monotone" dataKey="projected" stroke={C_PROJECTED} strokeWidth={2}
                      dot={false} strokeDasharray="4 2" name="Proj (line)"/>
              </ComposedChart>
            </ResponsiveContainer>
            {/* Legend */}
            <div className="flex items-center gap-4 mt-1 text-[9px] text-slate-400">
              <span className="flex items-center gap-1.5">
                <span className="w-3 h-0.5 inline-block rounded" style={{background: C_PROJECTED}}/>
                Projected
              </span>
              <span className="flex items-center gap-1.5">
                <span className="w-2.5 h-2.5 rounded-sm inline-block" style={{background: C_AHEAD + 'dd'}}/>
                Actual (ahead)
              </span>
              <span className="flex items-center gap-1.5">
                <span className="w-2.5 h-2.5 rounded-sm inline-block" style={{background: C_BEHIND + 'dd'}}/>
                Actual (behind)
              </span>
            </div>
          </div>

          {/* Month-by-month table */}
          <div>
            <p className="text-[10px] uppercase tracking-wider font-semibold text-slate-400 mb-2">
              Monthly Breakdown
            </p>
            <div className="rounded-xl border border-slate-100 overflow-hidden">
              <div className="overflow-y-auto" style={{ maxHeight: 240 }}>
                <table className="w-full text-xs">
                  <thead className="bg-slate-50 sticky top-0">
                    <tr>
                      {['Period','Projected','Actual','Gap','Gap %','Status'].map(h => (
                        <th key={h} className="px-3 py-2 text-left font-semibold text-slate-500 text-[10px] uppercase tracking-wide border-b border-slate-100">
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {entries.map(([period, v], i) => (
                      <tr key={period} className={i % 2 === 0 ? 'bg-white' : 'bg-slate-50/50'}>
                        <td className="px-3 py-2 font-medium text-slate-700">{period}</td>
                        <td className="px-3 py-2 text-slate-600">{fmtVal(v.projected, kpiData.unit)}</td>
                        <td className="px-3 py-2 text-slate-700 font-medium">{fmtVal(v.actual, kpiData.unit)}</td>
                        <td className={`px-3 py-2 font-medium ${v.gap >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>
                          {v.gap > 0 ? '+' : ''}{fmtVal(v.gap, kpiData.unit)}
                        </td>
                        <td className={`px-3 py-2 font-semibold rounded-sm ${getCellClass(v.gap_pct)}`}>
                          {v.gap_pct > 0 ? '+' : ''}{v.gap_pct?.toFixed(1)}%
                        </td>
                        <td className="px-3 py-2">
                          <span className={`inline-block w-2 h-2 rounded-full`}
                            style={{ background: gapColor(v.gap_pct) }}/>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* Root Cause Analysis */}
          {kpiData.causation && (
            <div className="space-y-4">
              <p className="text-[10px] uppercase tracking-wider font-semibold text-slate-400">
                Root Cause Analysis
              </p>

              {kpiData.causation.root_causes?.length > 0 && (
                <div>
                  <p className="text-[11px] font-semibold text-slate-600 mb-2">Root Causes</p>
                  <ol className="space-y-1.5">
                    {kpiData.causation.root_causes.map((rc, i) => (
                      <li key={i} className="flex items-start gap-2 text-[11px] text-slate-600">
                        <span className="flex-shrink-0 w-4 h-4 rounded-full bg-red-100 text-red-600
                                         text-[9px] font-bold flex items-center justify-center mt-0.5">
                          {i + 1}
                        </span>
                        {rc}
                      </li>
                    ))}
                  </ol>
                </div>
              )}

              {kpiData.causation.downstream_impact?.length > 0 && (
                <div>
                  <p className="text-[11px] font-semibold text-slate-600 mb-2">Downstream Impact</p>
                  <div className="flex flex-wrap items-center gap-1.5">
                    {kpiData.causation.downstream_impact.map((d, i) => (
                      <span key={d} className="flex items-center gap-1">
                        <span className="text-[10px] bg-amber-50 text-amber-700 border border-amber-200
                                         px-2 py-0.5 rounded-full font-medium">
                          {d.replace(/_/g, ' ')}
                        </span>
                        {i < kpiData.causation.downstream_impact.length - 1 && (
                          <ArrowRight size={10} className="text-slate-300"/>
                        )}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {kpiData.causation.corrective_actions?.length > 0 && (
                <div>
                  <p className="text-[11px] font-semibold text-slate-600 mb-2">Corrective Actions</p>
                  <ul className="space-y-1.5">
                    {kpiData.causation.corrective_actions.map((ca, i) => (
                      <li key={i} className="flex items-start gap-2 text-[11px] text-slate-600">
                        <span className="flex-shrink-0 text-emerald-500 mt-0.5 font-bold">✓</span>
                        {ca}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}

        </div>

        {/* Footer — Ask Anika CTA */}
        <div className="flex-shrink-0 px-6 py-4 border-t border-slate-100 bg-slate-50/50">
          <button
            onClick={() => { onAskAnika(kpiData.name); onClose() }}
            className="w-full py-2.5 rounded-xl bg-[#0055A4] hover:bg-[#003d80] text-white
                       text-sm font-semibold transition-colors flex items-center justify-center gap-2">
            Ask Anika about this KPI →
          </button>
        </div>
      </div>
    </>
  )
}

// ─── KpiBridgeCard ───────────────────────────────────────────────────────────
function KpiBridgeCard({ kpiData, onAskAnika, onExpand, revenueBase }) {
  const [open, setOpen] = useState(false)

  const sc = statusColor(kpiData.overall_status)

  const dirIcon = kpiData.avg_gap_pct >= 3
    ? <TrendingUp  size={12} style={{color: C_AHEAD}}/>
    : kpiData.avg_gap_pct <= -3
      ? <TrendingDown size={12} style={{color: C_BEHIND}}/>
      : <Minus size={12} className="text-slate-400"/>

  const badgeClass = kpiData.overall_status === 'green'
    ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
    : kpiData.overall_status === 'yellow'
      ? 'bg-amber-50 text-amber-700 border-amber-200'
      : 'bg-red-50 text-red-700 border-red-200'

  const gapTextColor = kpiData.avg_gap_pct >= 0 ? 'text-emerald-600' : 'text-red-600'
  const gapSign = kpiData.avg_gap_pct > 0 ? '+' : ''

  // Dollar impact — only shown for revenue-adjacent KPIs with a negative gap
  const dollarImpact = (revenueBase && kpiData.avg_gap_pct < -0.5 && kpiData.unit === 'pct')
    ? Math.abs(kpiData.avg_gap_pct / 100) * revenueBase * 12
    : null

  return (
    <div
      className="card p-4 flex flex-col gap-3 cursor-pointer hover:shadow-md transition-all"
      style={{ borderLeftColor: sc, borderLeftWidth: 3 }}
      onClick={onExpand}
    >
      {/* Header */}
      <div className="flex items-start justify-between gap-2">
        <div className="flex-1 min-w-0">
          <h4 className="text-xs font-semibold text-slate-800 leading-tight truncate">{kpiData.name}</h4>
          <div className="flex items-center gap-2 mt-1">
            <span className={`text-[9px] px-1.5 py-0.5 rounded-full border font-medium uppercase tracking-wide ${badgeClass}`}>
              {kpiData.overall_status}
            </span>
            <span className={`flex items-center gap-0.5 text-[10px] font-semibold ${gapTextColor}`}>
              {dirIcon} {gapSign}{kpiData.avg_gap_pct?.toFixed(1)}%
            </span>
          </div>
          {dollarImpact != null && (
            <div className="mt-1 text-[9px] font-semibold text-red-500">
              ≈ ${dollarImpact >= 1e6
                ? `${(dollarImpact / 1e6).toFixed(1)}M`
                : `${(dollarImpact / 1e3).toFixed(0)}K`} annualised impact
            </div>
          )}
        </div>
        <div className="text-right flex-shrink-0">
          <div className="text-[10px] text-slate-400">Actual</div>
          <div className="text-xs font-semibold text-slate-800">{fmtVal(kpiData.avg_actual, kpiData.unit)}</div>
          <div className="text-[10px] text-slate-400 mt-0.5">vs {fmtVal(kpiData.avg_projected, kpiData.unit)}</div>
        </div>
      </div>

      {/* Mini waterfall */}
      <WaterfallMini months={kpiData.months}/>

      {/* Root cause toggle + Ask Anika */}
      <div className="flex items-center gap-2" onClick={e => e.stopPropagation()}>
        <button
          onClick={() => setOpen(v => !v)}
          className="flex-1 flex items-center justify-center gap-1.5 py-1.5 rounded-lg
                     text-[10px] text-slate-500 hover:text-slate-800 border border-slate-200
                     hover:border-slate-300 transition-all bg-slate-50 hover:bg-slate-100">
          {open ? <ChevronUp size={10}/> : <ChevronDown size={10}/>}
          Root Cause Analysis
        </button>
        {kpiData.overall_status !== 'green' && (
          <button
            onClick={() => onAskAnika(kpiData.name)}
            className="py-1.5 px-2.5 rounded-lg text-[10px] font-medium text-blue-700
                       border border-blue-200 bg-blue-50 hover:bg-blue-100 transition-all whitespace-nowrap">
            Ask Anika
          </button>
        )}
      </div>

      {/* Collapsible root cause */}
      {open && kpiData.causation && (
        <div className="border-t border-slate-100 pt-3 space-y-3" onClick={e => e.stopPropagation()}>
          {kpiData.causation.root_causes?.length > 0 && (
            <div>
              <p className="text-[9px] uppercase tracking-wider font-semibold text-slate-400 mb-1.5">Root Causes</p>
              <ul className="space-y-1">
                {kpiData.causation.root_causes.map((rc, i) => (
                  <li key={i} className="flex items-start gap-1.5 text-[10px] text-slate-600">
                    <span className="text-red-500 mt-0.5 flex-shrink-0">•</span>
                    {rc}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {kpiData.causation.downstream_impact?.length > 0 && (
            <div>
              <p className="text-[9px] uppercase tracking-wider font-semibold text-slate-400 mb-1.5">Downstream Impact</p>
              <div className="flex flex-wrap gap-1">
                {kpiData.causation.downstream_impact.map(d => (
                  <span key={d} className="text-[9px] bg-amber-50 text-amber-700 border border-amber-200 px-1.5 py-0.5 rounded-full">
                    {d}
                  </span>
                ))}
              </div>
            </div>
          )}

          {kpiData.causation.corrective_actions?.length > 0 && (
            <div>
              <p className="text-[9px] uppercase tracking-wider font-semibold text-slate-400 mb-1.5">Corrective Actions</p>
              <ul className="space-y-1">
                {kpiData.causation.corrective_actions.map((ca, i) => (
                  <li key={i} className="flex items-start gap-1.5 text-[10px] text-slate-600">
                    <span className="text-emerald-600 mt-0.5 flex-shrink-0">✓</span>
                    {ca}
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ─── Main ProjectionBridge ───────────────────────────────────────────────────
export default function ProjectionBridge({ bridgeData, projectionMonthly, onUploaded, onAskAnika, onNavigateToUpload }) {
  const [detailKpi, setDetailKpi] = useState(null)

  const hasProjection = bridgeData?.has_projection
  const hasOverlap    = bridgeData?.has_overlap

  const sortedKpis = hasOverlap
    ? Object.entries(bridgeData.kpis).sort(([, a], [, b]) => {
        const order = { red: 0, yellow: 1, green: 2 }
        return (order[a.overall_status] ?? 3) - (order[b.overall_status] ?? 3)
      })
    : []

  // Derive avg monthly revenue from actuals for dollar impact calc
  const revenueBase = hasOverlap
    ? (() => {
        const rev = bridgeData.kpis?.['revenue_growth'] ?? bridgeData.kpis?.['gross_margin']
        if (rev?.avg_actual != null) {
          // avg_actual for revenue_growth is a %, use fallback $1M/month
          return 1_000_000  // $1M/month = $12M annualised — conservative default
        }
        return 1_000_000
      })()
    : 1_000_000

  return (
    <div className="space-y-0">

      {/* Compact projection status bar */}
      <div className="flex items-center justify-between card px-4 py-3 mb-5 border border-blue-100 bg-blue-50/40">
        <div className="flex items-center gap-2">
          <GitBranch size={13} className="text-blue-500"/>
          {hasProjection
            ? <span className="text-xs text-slate-700 font-medium">
                Projection loaded · <span className="text-slate-500 font-normal">active</span>
              </span>
            : <span className="text-xs text-slate-500">No projection loaded</span>
          }
        </div>
        <button
          onClick={() => onNavigateToUpload?.()}
          className="text-xs text-[#0055A4] hover:underline cursor-pointer font-medium">
          {hasProjection ? 'Change ↗' : 'Upload in Data Upload →'}
        </button>
      </div>

      {/* No projection */}
      {!hasProjection && (
        <div className="card p-10 text-center">
          <GitBranch size={36} className="text-slate-300 mx-auto mb-4"/>
          <h3 className="text-slate-600 font-semibold text-base mb-2">No Projection Loaded</h3>
          <p className="text-slate-400 text-sm max-w-md mx-auto">
            Upload a 12-month projection CSV in the <strong className="text-slate-500">Data Upload</strong> tab
            (same format as actuals) to unlock the bridge analysis — gap waterfall charts, root cause
            diagnostics, downstream impact chains, and corrective action playbooks for every KPI.
          </p>
          <button
            onClick={() => onNavigateToUpload?.()}
            className="mt-5 px-5 py-2 rounded-lg bg-[#0055A4] hover:bg-[#003d80] text-white
                       text-sm font-medium transition-colors inline-flex items-center gap-1.5">
            Go to Data Upload →
          </button>
        </div>
      )}

      {/* No overlap */}
      {hasProjection && !hasOverlap && (
        <div className="card p-8 text-center">
          <AlertTriangle size={28} className="text-amber-500 mx-auto mb-3"/>
          <h3 className="text-slate-700 font-semibold text-sm mb-2">No Overlapping Periods</h3>
          <p className="text-slate-400 text-xs max-w-sm mx-auto">
            The projection and actuals don't share any year-month combinations.
            Upload actuals data that overlaps with the projection period.
          </p>
        </div>
      )}

      {/* Full bridge UI */}
      {hasProjection && hasOverlap && (
        <>
          <SummaryBanner summary={bridgeData.summary}/>
          <WaterfallSummary kpis={bridgeData.kpis}/>
          <OverviewChart kpis={bridgeData.kpis}/>

          <div className="mb-4">
            <h3 className="text-sm font-semibold text-slate-800 mb-1">KPI Bridge Cards</h3>
            <p className="text-[11px] text-slate-500">
              Click any card for detailed analysis · Sorted by severity: Red → Yellow → Green
            </p>
          </div>

          <div className="grid grid-cols-3 gap-4">
            {sortedKpis.map(([key, kpiData]) => (
              <KpiBridgeCard
                key={key}
                kpiData={kpiData}
                onAskAnika={onAskAnika}
                onExpand={() => setDetailKpi(key)}
                revenueBase={revenueBase}
              />
            ))}
          </div>
        </>
      )}

      {/* Bridge Detail Panel (slide-out) */}
      {detailKpi && bridgeData?.kpis?.[detailKpi] && (
        <BridgeDetailPanel
          kpiData={bridgeData.kpis[detailKpi]}
          onClose={() => setDetailKpi(null)}
          onAskAnika={onAskAnika}
        />
      )}
    </div>
  )
}
