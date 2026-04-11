import { useState, useEffect } from 'react'
import axios from 'axios'
import {
  ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend, ReferenceLine
} from 'recharts'
import {
  TrendingUp, TrendingDown, DollarSign, RefreshCw,
  AlertTriangle, BarChart3
} from 'lucide-react'
import { fmtKpiValueCompact } from './kpiFormat'

// ─── Color constants ────────────────────────────────────────────────────────
const C_NEW        = '#059669'   // green — new ARR
const C_EXPANSION  = '#0d9488'   // teal  — expansion
const C_CONTRACTION = '#D97706'  // orange — contraction (negative)
const C_CHURNED    = '#DC2626'   // red   — churned (negative)
const C_ENDING_ARR = '#0055A4'   // accent blue — ending ARR line
const C_GRID       = '#e2e8f0'

// ─── Custom Tooltip ─────────────────────────────────────────────────────────
function BridgeTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div className="bg-white border border-slate-200 rounded-lg px-4 py-3 shadow-xl text-xs">
      <p className="text-slate-800 font-semibold mb-2">{label}</p>
      {payload.map((entry, i) => (
        <div key={i} className="flex items-center justify-between gap-4 py-0.5">
          <span className="flex items-center gap-1.5">
            <span className="w-2 h-2 rounded-full shrink-0" style={{ background: entry.color }} />
            <span className="text-slate-500">{entry.name}</span>
          </span>
          <span className="text-slate-800 font-medium">{fmtKpiValueCompact(entry.value, 'usd')}</span>
        </div>
      ))}
    </div>
  )
}

// ─── Summary Card ───────────────────────────────────────────────────────────
function SummaryCard({ label, value, color, icon: Icon }) {
  return (
    <div className="bg-white border border-slate-200 rounded-xl px-4 py-3 text-center">
      <div className="flex items-center justify-center gap-1.5 mb-1">
        {Icon && <Icon size={12} className={color} />}
        <p className="text-[10px] text-slate-500 uppercase tracking-wider">{label}</p>
      </div>
      <p className={`text-xl font-bold ${color}`}>{fmtKpiValueCompact(value, 'usd')}</p>
    </div>
  )
}

// ─── Main Component ─────────────────────────────────────────────────────────
export default function ArrBridge({ periodDates }) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState('')

  useEffect(() => {
    setLoading(true)
    setError('')
    const params = new URLSearchParams()
    if (periodDates?.fromYear) {
      params.set('from_year', periodDates.fromYear)
      params.set('from_month', periodDates.fromMonth)
      params.set('to_year', periodDates.toYear)
      params.set('to_month', periodDates.toMonth)
    }
    axios.get(`/api/analytics/arr-bridge?${params}`)
      .then(r => setData(r.data))
      .catch(e => setError(e.response?.data?.detail || 'Failed to load ARR bridge data'))
      .finally(() => setLoading(false))
  }, [periodDates?.fromYear, periodDates?.fromMonth, periodDates?.toYear, periodDates?.toMonth])

  // ── Loading state ──
  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw size={20} className="animate-spin text-[#0055A4]" />
      </div>
    )
  }

  // ── Error state ──
  if (error) {
    return (
      <div className="p-6 max-w-4xl mx-auto">
        <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-4 flex items-center gap-2 text-red-400 text-sm">
          <AlertTriangle size={14} className="shrink-0" />
          {error}
        </div>
      </div>
    )
  }

  // ── Empty state ──
  if (!data?.periods?.length) {
    return (
      <div className="p-6 max-w-4xl mx-auto space-y-5">
        <h2 className="text-slate-800 text-xl font-semibold flex items-center gap-2">
          <BarChart3 size={18} className="text-[#0055A4]" />
          ARR Bridge
        </h2>
        <div className="bg-white border border-slate-200 rounded-2xl p-10 text-center">
          <DollarSign size={28} className="text-slate-500 mx-auto mb-2" />
          <p className="text-slate-800 text-sm font-semibold">No revenue data available</p>
          <p className="text-slate-500 text-xs mt-1">
            Upload subscription data with customer IDs to enable ARR bridge analysis.
          </p>
        </div>
      </div>
    )
  }

  const { periods, summary } = data

  // Build chart data — contraction and churned shown as negative values
  const chartData = periods.map(p => ({
    period:      p.period,
    new_arr:     p.new_arr ?? 0,
    expansion:   p.expansion_arr ?? 0,
    contraction: -(Math.abs(p.contraction_arr ?? 0)),
    churned:     -(Math.abs(p.churned_arr ?? 0)),
    ending_arr:  p.ending_arr ?? 0,
  }))

  const netChange = (summary?.net_change ?? summary?.net_new_arr) ?? 0
  const isPositiveNet = netChange >= 0

  return (
    <div className="p-6 max-w-5xl mx-auto space-y-5">
      {/* Header */}
      <div>
        <h2 className="text-slate-800 text-xl font-semibold flex items-center gap-2">
          <BarChart3 size={18} className="text-[#0055A4]" />
          ARR Bridge
        </h2>
        <p className="text-slate-500 text-sm mt-1">
          Waterfall view of ARR movements across periods — new, expansion, contraction, and churn.
        </p>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        <SummaryCard
          label="Total New ARR"
          value={summary?.total_new_arr}
          color="text-green-400"
          icon={TrendingUp}
        />
        <SummaryCard
          label="Total Expansion"
          value={summary?.total_expansion}
          color="text-teal-400"
          icon={TrendingUp}
        />
        <SummaryCard
          label="Total Contraction"
          value={summary?.total_contraction}
          color="text-orange-400"
          icon={TrendingDown}
        />
        <SummaryCard
          label="Total Churned"
          value={summary?.total_churned}
          color="text-red-400"
          icon={TrendingDown}
        />
        <SummaryCard
          label="Net Change"
          value={netChange}
          color={isPositiveNet ? 'text-green-400' : 'text-red-400'}
          icon={isPositiveNet ? TrendingUp : TrendingDown}
        />
      </div>

      {/* Waterfall chart */}
      <div className="bg-white border border-slate-200 rounded-2xl p-5">
        <h3 className="text-[11px] font-bold text-slate-500 uppercase tracking-wider mb-4">
          ARR Movement by Period
        </h3>
        <div className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={chartData} margin={{ top: 10, right: 20, bottom: 20, left: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={C_GRID} />
              <XAxis
                dataKey="period"
                tick={{ fill: '#9ca3af', fontSize: 10 }}
                axisLine={{ stroke: '#e2e8f0' }}
                tickLine={false}
              />
              <YAxis
                tick={{ fill: '#9ca3af', fontSize: 10 }}
                axisLine={{ stroke: '#e2e8f0' }}
                tickLine={false}
                tickFormatter={v => fmtKpiValueCompact(v, 'usd')}
              />
              <Tooltip content={<BridgeTooltip />} />
              <Legend
                wrapperStyle={{ fontSize: 11, color: '#9ca3af' }}
                iconType="circle"
                iconSize={8}
              />
              <ReferenceLine y={0} stroke="#94a3b8" strokeWidth={1} />

              {/* Positive bars: stacked New + Expansion */}
              <Bar
                dataKey="new_arr"
                name="New ARR"
                fill={C_NEW}
                stackId="positive"
                radius={[0, 0, 0, 0]}
                barSize={28}
              />
              <Bar
                dataKey="expansion"
                name="Expansion"
                fill={C_EXPANSION}
                stackId="positive"
                radius={[3, 3, 0, 0]}
                barSize={28}
              />

              {/* Negative bars: stacked Contraction + Churned */}
              <Bar
                dataKey="contraction"
                name="Contraction"
                fill={C_CONTRACTION}
                stackId="negative"
                radius={[0, 0, 0, 0]}
                barSize={28}
              />
              <Bar
                dataKey="churned"
                name="Churned"
                fill={C_CHURNED}
                stackId="negative"
                radius={[0, 0, 3, 3]}
                barSize={28}
              />

              {/* Ending ARR line overlay */}
              <Line
                dataKey="ending_arr"
                name="Ending ARR"
                type="monotone"
                stroke={C_ENDING_ARR}
                strokeWidth={2.5}
                dot={{ r: 4, fill: C_ENDING_ARR, stroke: '#1a1f2e', strokeWidth: 2 }}
                activeDot={{ r: 6 }}
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Period detail table */}
      <div className="bg-white border border-slate-200 rounded-2xl overflow-hidden">
        <div className="px-5 py-3 border-b border-slate-100">
          <h3 className="text-[11px] font-bold text-slate-500 uppercase tracking-wider">
            Period Detail
          </h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-slate-100">
                {['Period', 'Beginning ARR', 'New', 'Expansion', 'Contraction', 'Churned', 'Net New', 'Ending ARR'].map(h => (
                  <th key={h} className="px-4 py-2.5 text-left text-[10px] font-semibold text-slate-500 uppercase tracking-wider">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {periods.map((p, i) => (
                <tr key={i} className="border-b border-slate-50 hover:bg-slate-50 transition-colors">
                  <td className="px-4 py-2.5 text-slate-800 font-medium">{p.period}</td>
                  <td className="px-4 py-2.5 text-slate-700">{fmtKpiValueCompact(p.beginning_arr, 'usd')}</td>
                  <td className="px-4 py-2.5 text-green-400 font-medium">{fmtKpiValueCompact(p.new_arr, 'usd')}</td>
                  <td className="px-4 py-2.5 text-teal-400 font-medium">{fmtKpiValueCompact(p.expansion_arr, 'usd')}</td>
                  <td className="px-4 py-2.5 text-orange-400 font-medium">{fmtKpiValueCompact(p.contraction_arr, 'usd')}</td>
                  <td className="px-4 py-2.5 text-red-400 font-medium">{fmtKpiValueCompact(p.churned_arr, 'usd')}</td>
                  <td className={`px-4 py-2.5 font-medium ${(p.net_new_arr ?? 0) >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {fmtKpiValueCompact(p.net_new_arr, 'usd')}
                  </td>
                  <td className="px-4 py-2.5 text-slate-800 font-semibold">{fmtKpiValueCompact(p.ending_arr, 'usd')}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
