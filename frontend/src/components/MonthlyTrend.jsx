import { useState, useRef } from 'react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ReferenceLine, ReferenceArea, ResponsiveContainer, Legend
} from 'recharts'
import { ChevronRight, Pin, X, BookMarked } from 'lucide-react'

const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
const COLORS  = ['#0055A4','#059669','#d97706','#dc2626','#7c3aed','#ea580c','#0891b2','#db2777']

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

function badgeCls(s) {
  return { green:'badge-green', yellow:'badge-yellow', red:'badge-red', grey:'badge-grey' }[s] || 'badge-grey'
}

// ── Custom annotation label rendered inside the SVG ──────────────────────
function AnnotationLabel({ viewBox, month }) {
  if (!viewBox) return null
  const { x, y } = viewBox
  return (
    <g>
      <circle cx={x} cy={y + 8} r={6} fill="#f59e0b" opacity={0.9}/>
      <text x={x} y={y + 12} textAnchor="middle" fontSize="8" fill="#fff" fontWeight="700">!</text>
    </g>
  )
}

// ── Decision marker label rendered inside the SVG ───────────────────────
function DecisionLabel({ viewBox, count }) {
  if (!viewBox) return null
  const { x, y } = viewBox
  return (
    <g>
      <rect x={x - 7} y={y + 2} width={14} height={14} rx={3} fill="#0055A4" opacity={0.85}/>
      <text x={x} y={y + 13} textAnchor="middle" fontSize="8" fill="#fff" fontWeight="700">
        {count > 1 ? count : 'D'}
      </text>
    </g>
  )
}

export default function MonthlyTrend({ fingerprint, onKpiClick, periodLabel, decisionMarkers = {} }) {
  const [selected, setSelected]     = useState(fingerprint?.slice(0, 4).map(k => k.key) || [])
  const [normMode, setNormMode]     = useState(false)
  const [annotations, setAnnotations] = useState(() => {
    try { return JSON.parse(localStorage.getItem('si_month_annotations') || '{}') }
    catch { return {} }
  })
  const [editingMonth, setEditingMonth] = useState(null)
  const [editText, setEditText]         = useState('')
  const inputRef = useRef(null)
  const [dropdownOpen, setDropdownOpen] = useState(false)

  function handleChartClick(data) {
    if (!data?.activeLabel) return
    setEditingMonth(data.activeLabel)
    setEditText(annotations[data.activeLabel] || '')
    setTimeout(() => inputRef.current?.focus(), 50)
  }

  function saveAnnotation() {
    if (!editingMonth) return
    const next = { ...annotations }
    if (editText.trim()) {
      next[editingMonth] = editText.trim()
    } else {
      delete next[editingMonth]
    }
    setAnnotations(next)
    localStorage.setItem('si_month_annotations', JSON.stringify(next))
    setEditingMonth(null)
    setEditText('')
  }

  function deleteAnnotation(month) {
    const next = { ...annotations }
    delete next[month]
    setAnnotations(next)
    localStorage.setItem('si_month_annotations', JSON.stringify(next))
  }

  const toggle = key => setSelected(s => s.includes(key) ? s.filter(x => x !== key) : [...s, key])

  if (!fingerprint?.length) return null

  // Derive all unique sorted periods actually present in the filtered data
  const allPeriods = [...new Set(
    fingerprint.flatMap(kpi => kpi.monthly?.map(m => m.period) ?? [])
  )].sort()

  // If data spans multiple years, label as "Jan 2023"; single year: "Jan"
  const uniqueYears = [...new Set(allPeriods.map(p => parseInt(p.split('-')[0])))].length
  const multiYear   = uniqueYears > 1

  function periodTick(period) {
    const [yr, mo] = period.split('-').map(Number)
    return multiYear ? `${MONTHS[mo - 1]} ${yr}` : MONTHS[mo - 1]
  }

  // Raw chart data — one entry per actual period (not hardcoded 12)
  const rawChartData = allPeriods.map(period => {
    const entry = { month: periodTick(period) }
    fingerprint.forEach(kpi => {
      const m = kpi.monthly?.find(d => d.period === period)
      entry[kpi.key] = m?.value ?? null
    })
    return entry
  })

  // Normalised chart data — each value becomes (value / target) * 100
  const normChartData = allPeriods.map(period => {
    const entry = { month: periodTick(period) }
    fingerprint.forEach(kpi => {
      const m = kpi.monthly?.find(d => d.period === period)
      entry[kpi.key] = (m?.value != null && kpi.target)
        ? (m.value / kpi.target) * 100
        : null
    })
    return entry
  })

  // For multi-year, thin the X-axis ticks so it stays readable
  const xTickInterval = allPeriods.length > 24
    ? Math.ceil(allPeriods.length / 10)
    : allPeriods.length > 12 ? 2 : 0

  const chartData = normMode ? normChartData : rawChartData
  const active = fingerprint.filter(k => selected.includes(k.key))

  return (
    <div className="space-y-5 max-w-7xl">
      {/* KPI selector — compact multi-select dropdown */}
      <div className="card p-4">
        <div className="flex items-center justify-between mb-2">
          <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider">KPIs to overlay</p>
          <div className="flex items-center gap-2">
            <span className="text-xs text-slate-400">{selected.length} selected</span>
            <button
              onClick={() => setSelected(selected.length === fingerprint.length ? [] : fingerprint.map(k => k.key))}
              className="text-xs text-[#0055A4] hover:underline font-medium">
              {selected.length === fingerprint.length ? 'Deselect all' : 'Select all'}
            </button>
          </div>
        </div>
        <div className="relative">
          <button
            onClick={() => setDropdownOpen(o => !o)}
            className="w-full flex items-center justify-between px-3 py-2 rounded-lg border border-slate-200 bg-white hover:border-slate-300 transition-colors text-sm text-slate-600">
            <span className="flex flex-wrap gap-1.5 flex-1 min-w-0">
              {selected.length === 0 && <span className="text-slate-400 text-xs">No KPIs selected — click to choose</span>}
              {selected.slice(0, 4).map(key => {
                const kpi = fingerprint.find(k => k.key === key)
                const i   = fingerprint.findIndex(k => k.key === key)
                if (!kpi) return null
                return (
                  <span key={key}
                    className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-medium text-white"
                    style={{ background: COLORS[i % COLORS.length] }}>
                    {kpi.name}
                    <button
                      onClick={e => { e.stopPropagation(); toggle(key) }}
                      className="opacity-70 hover:opacity-100 leading-none font-bold">×</button>
                  </span>
                )
              })}
              {selected.length > 4 && (
                <span className="inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-medium bg-slate-100 text-slate-600">
                  +{selected.length - 4} more
                </span>
              )}
            </span>
            <svg className={`ml-2 flex-shrink-0 w-4 h-4 text-slate-400 transition-transform ${dropdownOpen ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7"/></svg>
          </button>
          {dropdownOpen && (
            <div className="absolute z-20 mt-1 w-full bg-white border border-slate-200 rounded-xl shadow-lg max-h-64 overflow-y-auto">
              {fingerprint.map((kpi, i) => (
                <label key={kpi.key}
                  className="flex items-center gap-3 px-3 py-2 hover:bg-slate-50 cursor-pointer transition-colors">
                  <input
                    type="checkbox"
                    checked={selected.includes(kpi.key)}
                    onChange={() => toggle(kpi.key)}
                    className="rounded"
                    style={{ accentColor: COLORS[i % COLORS.length] }}
                  />
                  <span className="flex items-center gap-2 flex-1 min-w-0">
                    <span className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ background: COLORS[i % COLORS.length] }}/>
                    <span className="text-sm text-slate-700 truncate">{kpi.name}</span>
                  </span>
                </label>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Combined chart */}
      {active.length > 0 && (
        <div className="card p-6">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="text-sm font-semibold text-slate-700">
                Overlaid KPI Trends — {periodLabel || 'Selected Period'}
                <span className="ml-2 text-[10px] font-normal text-slate-400">({allPeriods.length} months)</span>
              </h3>
              <p className="text-xs text-slate-400 mt-0.5">
                {normMode ? 'All KPIs normalised to % of target · 100 = on target · ' : ''}
                <span className="text-amber-500 font-medium">Click any month to annotate an event</span>
              </p>
            </div>
            <div className="flex items-center gap-1 bg-slate-100 rounded-lg p-1">
              <button
                onClick={() => setNormMode(false)}
                className={`px-3 py-1 rounded-md text-xs font-medium transition-all ${
                  !normMode ? 'bg-white text-slate-700 shadow-sm' : 'text-slate-500 hover:text-slate-700'
                }`}>
                Raw Values
              </button>
              <button
                onClick={() => setNormMode(true)}
                className={`px-3 py-1 rounded-md text-xs font-medium transition-all ${
                  normMode ? 'bg-white text-slate-700 shadow-sm' : 'text-slate-500 hover:text-slate-700'
                }`}>
                % of Target
              </button>
            </div>
          </div>
          <ResponsiveContainer width="100%" height={360}>
            <LineChart data={chartData} margin={{ top: 5, right: 20, bottom: 5, left: 10 }}
              onClick={handleChartClick} style={{ cursor: 'crosshair' }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9"/>
              <XAxis dataKey="month" tick={{ fill: '#64748b', fontSize: multiYear ? 9 : 11 }} interval={xTickInterval}/>
              <YAxis
                tick={{ fill: '#64748b', fontSize: 11 }}
                domain={normMode ? [50, 135] : undefined}
                tickFormatter={normMode ? v => `${v}%` : undefined}
              />
              <Tooltip
                contentStyle={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, fontSize: 11, color: '#0f172a', boxShadow: '0 4px 12px rgba(0,0,0,0.08)' }}
                formatter={(v, name) => {
                  const kpi = fingerprint.find(k => k.key === name)
                  if (normMode) return [`${v?.toFixed(1)}% of target`, kpi?.name]
                  return [fmt(v, kpi?.unit), kpi?.name]
                }}/>
              <Legend formatter={name => fingerprint.find(k => k.key === name)?.name || name}
                wrapperStyle={{ fontSize: 11, color: '#64748b' }}/>
              {normMode && (
                <>
                  <ReferenceLine y={100} stroke="#64748b" strokeWidth={1.5} strokeDasharray="5 3"
                    label={{ value: 'Target', position: 'right', fontSize: 10, fill: '#64748b' }}/>
                  <ReferenceArea y1={95} y2={105} fill="#f1f5f9" fillOpacity={0.6}/>
                </>
              )}
              {active.map((kpi) => (
                <Line key={kpi.key} type="monotone" dataKey={kpi.key}
                  stroke={COLORS[fingerprint.indexOf(kpi) % COLORS.length]}
                  strokeWidth={2} dot={{ r: 3, strokeWidth: 0 }} connectNulls/>
              ))}
              {!normMode && active.map((kpi) => kpi.target != null && (
                <ReferenceLine key={`t-${kpi.key}`} y={kpi.target}
                  stroke={COLORS[fingerprint.indexOf(kpi) % COLORS.length]}
                  strokeDasharray="4 4" strokeOpacity={0.8} strokeWidth={1.5}/>
              ))}
              {/* Annotation reference lines — amber vertical markers */}
              {Object.keys(annotations).map(month => (
                <ReferenceLine key={`ann-${month}`} x={month}
                  stroke="#f59e0b" strokeWidth={1.5} strokeDasharray="3 3" opacity={0.7}
                  label={<AnnotationLabel month={month}/>}/>
              ))}
              {/* Decision markers — blue vertical lines */}
              {Object.entries(decisionMarkers).map(([period, decs]) => {
                const tick = periodTick(period)
                if (!chartData.find(d => d.month === tick)) return null
                return (
                  <ReferenceLine key={`dec-${period}`} x={tick}
                    stroke="#0055A4" strokeWidth={1.5} strokeDasharray="5 3" opacity={0.6}
                    label={<DecisionLabel count={decs.length}/>}/>
                )
              })}
            </LineChart>
          </ResponsiveContainer>

          {/* Inline annotation editor */}
          {editingMonth && (
            <div className="mt-3 flex items-center gap-2 p-3 bg-amber-50 rounded-xl border border-amber-200 animate-fade-in">
              <Pin size={13} className="text-amber-500 flex-shrink-0"/>
              <span className="text-xs font-bold text-amber-800 flex-shrink-0 w-8">{editingMonth}</span>
              <input
                ref={inputRef}
                value={editText}
                onChange={e => setEditText(e.target.value)}
                onKeyDown={e => {
                  e.stopPropagation()
                  if (e.key === 'Enter') { e.preventDefault(); saveAnnotation() }
                  if (e.key === 'Escape') { e.preventDefault(); setEditingMonth(null); setEditText('') }
                }}
                placeholder="Add event note for this month… (Enter to save, Esc to cancel)"
                className="flex-1 text-xs border border-amber-300 rounded-lg px-2.5 py-1.5 bg-white focus:outline-none focus:ring-1 focus:ring-amber-400 placeholder-amber-300"/>
              <button onClick={saveAnnotation}
                className="text-xs px-3 py-1.5 bg-amber-500 text-white rounded-lg font-semibold hover:bg-amber-600 transition-colors flex-shrink-0">
                Save
              </button>
              <button onClick={() => { setEditingMonth(null); setEditText('') }}
                className="text-amber-400 hover:text-amber-700 flex-shrink-0">
                <X size={14}/>
              </button>
            </div>
          )}

          {/* Annotation tags strip */}
          {Object.keys(annotations).length > 0 && (
            <div className="mt-3 flex flex-wrap gap-2">
              {Object.entries(annotations).map(([month, text]) => (
                <span key={month}
                  className="inline-flex items-center gap-1.5 text-[11px] bg-amber-50 border border-amber-200 text-amber-800 px-2.5 py-1 rounded-full cursor-pointer hover:border-amber-400 transition-colors"
                  onClick={() => { setEditingMonth(month); setEditText(text); setTimeout(() => inputRef.current?.focus(), 50) }}>
                  <Pin size={9} className="text-amber-500"/>
                  <strong>{month}:</strong> {text}
                  <button
                    onClick={e => { e.stopPropagation(); deleteAnnotation(month) }}
                    className="ml-0.5 text-amber-400 hover:text-amber-700 font-bold leading-none">×</button>
                </span>
              ))}
            </div>
          )}

          {/* Decision markers legend */}
          {Object.keys(decisionMarkers).length > 0 && (
            <div className="mt-2 flex flex-wrap gap-2">
              {Object.entries(decisionMarkers)
                .filter(([period]) => chartData.find(d => d.month === periodTick(period)))
                .map(([period, decs]) => (
                <span key={`dec-tag-${period}`}
                  className="inline-flex items-center gap-1.5 text-[11px] bg-blue-50 border border-blue-200 text-blue-700 px-2.5 py-1 rounded-full">
                  <BookMarked size={9} className="text-[#0055A4]"/>
                  <strong>{periodTick(period)}:</strong> {decs.map(d => d.title).join(', ')}
                </span>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Individual sparkline grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
        {fingerprint.filter(kpi => kpi.monthly?.length).map((kpi, i) => {
          const kpiPeriods = [...(kpi.monthly ?? [])].sort((a, b) => a.period.localeCompare(b.period))
          const data = kpiPeriods.map(m => {
            const [yr, mo] = m.period.split('-').map(Number)
            return { month: multiYear ? `${MONTHS[mo - 1]} ${yr}` : MONTHS[mo - 1], value: m.value ?? null }
          })
          const clr = COLORS[i % COLORS.length]
          const targetBand = kpi.target ? { y1: kpi.target * 0.95, y2: kpi.target * 1.05 } : null
          return (
            <div key={kpi.key}
              className="card p-4 cursor-pointer hover:shadow-md hover:scale-[1.01] hover:border-[#0055A4]/30 transition-all group"
              onClick={() => onKpiClick?.(kpi.key)}>
              <div className="flex justify-between items-start mb-3">
                <div className="min-w-0">
                  <p className="text-sm font-semibold text-slate-700 flex items-center gap-1">
                    {kpi.name}
                    <ChevronRight size={12} className="text-slate-300 group-hover:text-[#0055A4] transition-colors flex-shrink-0"/>
                  </p>
                  <p className="text-xs text-slate-500 mt-0.5">
                    Avg: <span className="text-slate-600 font-medium">{fmt(kpi.avg, kpi.unit)}</span>
                    {kpi.target && <> · Target: <span className="text-slate-500">{fmt(kpi.target, kpi.unit)}</span></>}
                  </p>
                </div>
                <span className={`text-[10px] px-2 py-0.5 rounded-full font-bold flex-shrink-0 ${badgeCls(kpi.fy_status)}`}>
                  {kpi.fy_status?.toUpperCase()}
                </span>
              </div>
              <ResponsiveContainer width="100%" height={70}>
                <LineChart data={data}>
                  <CartesianGrid strokeDasharray="2 2" stroke="#f1f5f9"/>
                  {targetBand && (
                    <ReferenceArea y1={targetBand.y1} y2={targetBand.y2} fill="#f59e0b" fillOpacity={0.1}/>
                  )}
                  <Line type="monotone" dataKey="value" stroke={clr} strokeWidth={2} dot={false} connectNulls/>
                  {kpi.target && <ReferenceLine y={kpi.target} stroke="#f59e0b" strokeDasharray="3 3" strokeWidth={1.5}/>}
                </LineChart>
              </ResponsiveContainer>
            </div>
          )
        })}
      </div>
    </div>
  )
}
