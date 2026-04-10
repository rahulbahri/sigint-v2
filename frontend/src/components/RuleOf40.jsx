import { useState, useEffect, useMemo, useCallback } from 'react'
import axios from 'axios'
import {
  ScatterChart, Scatter, XAxis, YAxis, Tooltip, ResponsiveContainer,
  ReferenceLine, ZAxis, Line, LineChart, CartesianGrid,
} from 'recharts'
import { Target, RefreshCw, TrendingUp, TrendingDown, AlertTriangle } from 'lucide-react'

const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
const GREEN = '#059669'
const RED   = '#DC2626'

// ── Custom dot that is green above the Rule-of-40 line, red below ──────────
function Ro40Dot({ cx, cy, payload }) {
  if (!payload) return null
  const score = (payload.x ?? 0) + (payload.y ?? 0)
  return (
    <g>
      <circle cx={cx} cy={cy} r={5} fill={score >= 40 ? GREEN : RED} stroke="#1a1f2e" strokeWidth={2} />
    </g>
  )
}

// ── Custom tooltip ─────────────────────────────────────────────────────────
function Ro40Tooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload
  if (!d) return null
  const score = (d.x + d.y).toFixed(1)
  return (
    <div className="bg-white border border-slate-200 rounded-lg px-3 py-2 shadow-xl text-xs">
      <p className="text-slate-800 font-semibold mb-1">{d.label}</p>
      <p className="text-slate-500">Revenue Growth: <span className="text-slate-800">{d.x.toFixed(1)}%</span></p>
      <p className="text-slate-500">EBITDA Margin: <span className="text-slate-800">{d.y.toFixed(1)}%</span></p>
      <p className={`mt-1 font-semibold ${Number(score) >= 40 ? 'text-emerald-400' : 'text-red-400'}`}>
        Score: {score}
      </p>
    </div>
  )
}

// ── Custom reference line for x + y = 40 diagonal ─────────────────────────
function DiagonalReferenceLine({ xAxisMap, yAxisMap }) {
  const xAxis = xAxisMap && Object.values(xAxisMap)[0]
  const yAxis = yAxisMap && Object.values(yAxisMap)[0]
  if (!xAxis || !yAxis) return null

  const xMin = xAxis.domain?.[0] ?? -20
  const xMax = xAxis.domain?.[1] ?? 80
  // Line: y = 40 - x
  const y1 = 40 - xMin
  const y2 = 40 - xMax

  const x1px = xAxis.scale?.(xMin) ?? 0
  const x2px = xAxis.scale?.(xMax) ?? 0
  const y1px = yAxis.scale?.(y1)   ?? 0
  const y2px = yAxis.scale?.(y2)   ?? 0

  return (
    <line
      x1={x1px} y1={y1px}
      x2={x2px} y2={y2px}
      stroke="#D97706"
      strokeWidth={1.5}
      strokeDasharray="6 4"
    />
  )
}

// ── Main component ─────────────────────────────────────────────────────────
export default function RuleOf40({ periodDates }) {
  const [raw, setRaw]         = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState('')

  const load = useCallback(async () => {
    setLoading(true); setError('')
    try {
      const params = new URLSearchParams()
      if (periodDates?.fromYear) {
        params.set('year', periodDates.fromYear)  // /api/monthly uses ?year= filter
      }
      const { data } = await axios.get(`/api/monthly?${params}`)
      setRaw(Array.isArray(data) ? data : [])
    } catch (e) {
      setError(e.response?.data?.detail || 'Failed to load monthly data')
    } finally {
      setLoading(false)
    }
  }, [periodDates?.fromYear, periodDates?.fromMonth, periodDates?.toYear, periodDates?.toMonth])

  useEffect(() => { load() }, [load])

  // Build scatter points from monthly KPI data
  const points = useMemo(() => {
    return raw
      .filter(m => m.kpis?.revenue_growth != null && m.kpis?.ebitda_margin != null)
      .map(m => {
        const mon = MONTHS[(m.month || 1) - 1] || '?'
        const yr  = String(m.year || '').slice(-2)
        return {
          x:     m.kpis.revenue_growth,
          y:     m.kpis.ebitda_margin,
          label: `${mon} ${yr}`,
          year:  m.year,
          month: m.month,
        }
      })
      .sort((a, b) => (a.year - b.year) || (a.month - b.month))
  }, [raw])

  const latest = points.length > 0 ? points[points.length - 1] : null
  const prev   = points.length > 1 ? points[points.length - 2] : null
  const latestScore = latest ? (latest.x + latest.y) : null
  const prevScore   = prev   ? (prev.x + prev.y)     : null
  const trajectory  = latestScore != null && prevScore != null
    ? (latestScore > prevScore ? 'improving' : latestScore < prevScore ? 'declining' : 'stable')
    : null

  // ── Loading state ────────────────────────────────────────────────────────
  if (loading) return (
    <div className="flex items-center justify-center h-64">
      <RefreshCw size={20} className="animate-spin text-[#0055A4]" />
    </div>
  )

  // ── Error state ──────────────────────────────────────────────────────────
  if (error) return (
    <div className="p-6 max-w-4xl mx-auto">
      <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-4 text-red-400 text-sm">{error}</div>
    </div>
  )

  // ── Empty state ──────────────────────────────────────────────────────────
  if (points.length === 0) return (
    <div className="p-6 max-w-4xl mx-auto space-y-5">
      <h2 className="text-slate-800 text-xl font-semibold flex items-center gap-2">
        <Target size={20} className="text-[#0055A4]" />
        Rule of 40
      </h2>
      <div className="bg-white border border-slate-200 rounded-2xl p-12 text-center">
        <AlertTriangle size={32} className="text-slate-500 mx-auto mb-3" />
        <p className="text-slate-500 text-sm font-semibold">Insufficient data for Rule of 40 analysis</p>
        <p className="text-slate-500 text-xs mt-1">
          Both revenue growth and EBITDA margin are required in monthly KPI data.
        </p>
      </div>
    </div>
  )

  // Axis domain helpers
  const allX = points.map(p => p.x)
  const allY = points.map(p => p.y)
  const xMin = Math.floor(Math.min(...allX, 0) / 10) * 10 - 10
  const xMax = Math.ceil(Math.max(...allX, 50) / 10) * 10 + 10
  const yMin = Math.floor(Math.min(...allY, 0) / 10) * 10 - 10
  const yMax = Math.ceil(Math.max(...allY, 50) / 10) * 10 + 10

  // Build line data for chronological connection
  const lineData = points.map(p => ({ revGrowth: p.x, ebitdaMargin: p.y }))

  return (
    <div className="p-6 max-w-4xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-slate-800 text-xl font-semibold flex items-center gap-2">
            <Target size={20} className="text-[#0055A4]" />
            Rule of 40
          </h2>
          <p className="text-slate-500 text-sm mt-1">
            Revenue Growth % + EBITDA Margin % &ge; 40 indicates a healthy SaaS business
          </p>
        </div>
        <button
          onClick={load}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-slate-500 hover:text-slate-800 bg-slate-50 hover:bg-slate-100 rounded-lg transition-colors"
        >
          <RefreshCw size={12} /> Refresh
        </button>
      </div>

      {/* Scatter chart */}
      <div className="bg-white border border-slate-200 rounded-2xl p-5">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h3 className="text-slate-800 text-sm font-semibold">Monthly Scatter</h3>
            <p className="text-slate-500 text-xs mt-0.5">
              Dashed line = Rule of 40 threshold. Green dots pass, red dots fail.
            </p>
          </div>
          <div className="flex items-center gap-3 text-[10px]">
            <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-full bg-emerald-500 inline-block" /> Above 40</span>
            <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-full bg-red-500 inline-block" /> Below 40</span>
            <span className="flex items-center gap-1"><span className="w-3 h-0 border-t border-dashed border-yellow-500 inline-block" /> Threshold</span>
          </div>
        </div>

        <ResponsiveContainer width="100%" height={340}>
          <ScatterChart margin={{ top: 10, right: 20, bottom: 20, left: 10 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis
              type="number"
              dataKey="x"
              name="Revenue Growth"
              domain={[xMin, xMax]}
              tick={{ fill: '#9ca3af', fontSize: 10 }}
              axisLine={{ stroke: '#4b5563' }}
              tickLine={false}
              label={{ value: 'Revenue Growth %', position: 'bottom', offset: 0, fill: '#6b7280', fontSize: 11 }}
            />
            <YAxis
              type="number"
              dataKey="y"
              name="EBITDA Margin"
              domain={[yMin, yMax]}
              tick={{ fill: '#9ca3af', fontSize: 10 }}
              axisLine={{ stroke: '#4b5563' }}
              tickLine={false}
              label={{ value: 'EBITDA Margin %', angle: -90, position: 'insideLeft', offset: 0, fill: '#6b7280', fontSize: 11 }}
            />
            <ZAxis range={[60, 60]} />
            <Tooltip content={<Ro40Tooltip />} cursor={false} />

            {/* Diagonal reference line x+y=40 rendered as a Recharts ReferenceLine segment */}
            <ReferenceLine
              segment={[
                { x: xMin, y: 40 - xMin },
                { x: xMax, y: 40 - xMax },
              ]}
              stroke="#D97706"
              strokeWidth={1.5}
              strokeDasharray="6 4"
              label={{ value: 'x+y=40', position: 'insideTopRight', fill: '#D97706', fontSize: 10 }}
            />

            {/* Chronological connecting line */}
            {points.length > 1 && (
              <Scatter data={points} line={{ stroke: '#6b7280', strokeWidth: 1 }} lineType="joint" shape={<Ro40Dot />} />
            )}
            {points.length === 1 && (
              <Scatter data={points} shape={<Ro40Dot />} />
            )}
          </ScatterChart>
        </ResponsiveContainer>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-4">
        <div className="bg-white border border-slate-200 rounded-xl p-5 text-center">
          <div className={`text-3xl font-bold ${
            latestScore != null && latestScore >= 40 ? 'text-emerald-400' : 'text-red-400'
          }`}>
            {latestScore != null ? latestScore.toFixed(1) : '\u2014'}
          </div>
          <div className="text-slate-500 text-xs mt-1.5 uppercase tracking-wider font-medium">
            Latest Score
          </div>
          {latest && (
            <div className="text-slate-500 text-[10px] mt-0.5">{latest.label}</div>
          )}
        </div>

        <div className="bg-white border border-slate-200 rounded-xl p-5 text-center">
          <div className="text-3xl font-bold text-slate-800">40</div>
          <div className="text-slate-500 text-xs mt-1.5 uppercase tracking-wider font-medium">
            Benchmark
          </div>
          <div className="text-slate-500 text-[10px] mt-0.5">Growth % + Margin %</div>
        </div>

        <div className="bg-white border border-slate-200 rounded-xl p-5 text-center">
          <div className="flex items-center justify-center gap-1.5">
            {trajectory === 'improving' && <TrendingUp size={20} className="text-emerald-400" />}
            {trajectory === 'declining' && <TrendingDown size={20} className="text-red-400" />}
            <span className={`text-xl font-bold ${
              trajectory === 'improving' ? 'text-emerald-400'
                : trajectory === 'declining' ? 'text-red-400'
                : 'text-slate-500'
            }`}>
              {trajectory ? trajectory.charAt(0).toUpperCase() + trajectory.slice(1) : '\u2014'}
            </span>
          </div>
          <div className="text-slate-500 text-xs mt-1.5 uppercase tracking-wider font-medium">
            Trajectory
          </div>
          {latestScore != null && prevScore != null && (
            <div className="text-slate-500 text-[10px] mt-0.5">
              {(latestScore - prevScore) >= 0 ? '+' : ''}{(latestScore - prevScore).toFixed(1)} vs prior month
            </div>
          )}
        </div>
      </div>

      {/* Health banner */}
      {latestScore != null && (
        <div className={`flex items-center gap-2 px-4 py-3 rounded-xl text-sm font-medium ${
          latestScore >= 40
            ? 'bg-emerald-500/10 border border-emerald-500/20 text-emerald-400'
            : 'bg-red-500/10 border border-red-500/20 text-red-400'
        }`}>
          {latestScore >= 40 ? <TrendingUp size={16} /> : <TrendingDown size={16} />}
          {latestScore >= 40
            ? `Passing Rule of 40 with a score of ${latestScore.toFixed(1)} (${(latestScore - 40).toFixed(1)} points above threshold)`
            : `Below Rule of 40 threshold by ${(40 - latestScore).toFixed(1)} points \u2014 focus on growth or profitability improvement`}
        </div>
      )}
    </div>
  )
}
