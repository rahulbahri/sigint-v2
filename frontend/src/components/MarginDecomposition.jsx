import { useState, useEffect } from 'react'
import axios from 'axios'
import {
  AreaChart, Area, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend, ComposedChart,
} from 'recharts'
import { Layers } from 'lucide-react'

// Color palette for COGS components — enough for typical breakdowns
const COMPONENT_COLORS = [
  '#DC2626', // red — hosting / infrastructure
  '#D97706', // yellow — personnel / labor
  '#7c3aed', // violet — third-party services
  '#0891b2', // cyan — support costs
  '#db2777', // pink — other
  '#ea580c', // orange — additional
  '#059669', // green — additional
  '#6366f1', // indigo — additional
]

function fmtUsd(v) {
  if (v == null) return '-'
  const abs = Math.abs(v)
  const sign = v < 0 ? '-' : ''
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`
  if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(1)}K`
  return `${sign}$${abs.toFixed(0)}`
}

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div className="bg-white border border-slate-200 rounded-lg px-3 py-2 text-xs shadow-lg">
      <p className="text-slate-700 font-semibold mb-1">{label}</p>
      {payload.map((p, i) => (
        <div key={i} className="flex items-center justify-between gap-4">
          <span className="flex items-center gap-1.5">
            <span className="w-2 h-2 rounded-full inline-block" style={{ backgroundColor: p.color }} />
            <span className="text-slate-500">{p.name}</span>
          </span>
          <span className="text-slate-700 font-medium">
            {p.name === 'Gross Margin %' ? `${p.value?.toFixed(1)}%` : fmtUsd(p.value)}
          </span>
        </div>
      ))}
    </div>
  )
}

export default function MarginDecomposition() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    axios.get('/api/analytics/margin-decomposition')
      .then(r => setData(r.data))
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-40 text-slate-500 text-sm">
        Loading margin data...
      </div>
    )
  }

  if (!data?.periods?.length) {
    return (
      <div className="space-y-5">
        <h1 className="text-lg font-bold text-slate-800 flex items-center gap-2">
          <Layers size={18} className="text-[#0055A4]" />
          Margin Decomposition
        </h1>
        <div className="bg-white rounded-2xl border border-slate-200 p-10 text-center">
          <Layers size={28} className="text-slate-500 mx-auto mb-2" />
          <p className="text-slate-700 text-sm font-semibold">No margin data available</p>
          <p className="text-slate-500 text-xs mt-1">
            Upload P&L data with COGS line items to enable margin decomposition.
          </p>
        </div>
      </div>
    )
  }

  const { periods } = data

  // Collect all unique COGS component categories across all periods
  const allCategories = []
  const seen = new Set()
  periods.forEach(p => {
    p.cogs_components?.forEach(c => {
      if (!seen.has(c.category)) {
        seen.add(c.category)
        allCategories.push(c.category)
      }
    })
  })

  // Build chart data — one row per period, with each component as a key
  const chartData = periods.map(p => {
    const row = {
      period: p.period,
      revenue: p.revenue,
      total_cogs: p.total_cogs,
      gross_margin_pct: p.gross_margin_pct,
    }
    // Initialize all categories to 0
    allCategories.forEach(cat => { row[cat] = 0 })
    // Fill in actual values
    p.cogs_components?.forEach(c => {
      row[c.category] = c.amount
    })
    return row
  })

  return (
    <div className="space-y-5">
      {/* Header */}
      <div>
        <h1 className="text-lg font-bold text-slate-800 flex items-center gap-2">
          <Layers size={18} className="text-[#0055A4]" />
          Margin Decomposition
        </h1>
        <p className="text-[12px] text-slate-500 mt-0.5">
          Cost of goods sold breakdown with gross margin overlay.
        </p>
      </div>

      {/* Stacked Area Chart with Gross Margin Line */}
      <div className="bg-white rounded-2xl border border-slate-200 p-5">
        <h2 className="text-[11px] font-bold text-slate-500 uppercase tracking-wider mb-3">
          COGS Components & Gross Margin
        </h2>
        <div className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
              <XAxis
                dataKey="period"
                tick={{ fontSize: 10, fill: '#9ca3af' }}
                axisLine={{ stroke: 'rgba(255,255,255,0.1)' }}
                tickLine={false}
              />
              <YAxis
                yAxisId="left"
                tick={{ fontSize: 10, fill: '#9ca3af' }}
                tickFormatter={v => fmtUsd(v)}
                axisLine={{ stroke: 'rgba(255,255,255,0.1)' }}
                tickLine={false}
              />
              <YAxis
                yAxisId="right"
                orientation="right"
                tick={{ fontSize: 10, fill: '#9ca3af' }}
                tickFormatter={v => `${v}%`}
                axisLine={{ stroke: 'rgba(255,255,255,0.1)' }}
                tickLine={false}
                domain={[0, 100]}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend
                wrapperStyle={{ fontSize: 11, color: '#9ca3af' }}
                iconType="circle"
                iconSize={8}
              />
              {/* Stacked areas for each COGS component */}
              {allCategories.map((cat, i) => (
                <Area
                  key={cat}
                  yAxisId="left"
                  type="monotone"
                  dataKey={cat}
                  stackId="cogs"
                  fill={COMPONENT_COLORS[i % COMPONENT_COLORS.length]}
                  stroke={COMPONENT_COLORS[i % COMPONENT_COLORS.length]}
                  fillOpacity={0.7}
                  strokeWidth={1}
                  name={cat}
                />
              ))}
              {/* Gross margin % overlay line on right axis */}
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="gross_margin_pct"
                stroke="#059669"
                strokeWidth={2.5}
                dot={{ r: 3, fill: '#059669', stroke: '#1a1f2e', strokeWidth: 2 }}
                name="Gross Margin %"
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Latest Period Breakdown Table */}
      {periods.length > 0 && (() => {
        const latest = periods[periods.length - 1]
        return (
          <div className="bg-white rounded-2xl border border-slate-200 p-5">
            <h2 className="text-[11px] font-bold text-slate-500 uppercase tracking-wider mb-3">
              Latest Period: {latest.period}
            </h2>
            <div className="space-y-1.5">
              {/* Revenue row */}
              <div className="flex items-center justify-between px-3 py-2 rounded-lg bg-slate-50">
                <span className="text-slate-700 text-xs font-semibold">Revenue</span>
                <span className="text-slate-800 text-xs font-bold">{fmtUsd(latest.revenue)}</span>
              </div>
              {/* COGS components */}
              {latest.cogs_components?.map((c, i) => (
                <div key={c.category} className="flex items-center justify-between px-3 py-2 rounded-lg bg-slate-50">
                  <span className="flex items-center gap-2 text-slate-500 text-xs">
                    <span
                      className="w-2.5 h-2.5 rounded-full inline-block"
                      style={{ backgroundColor: COMPONENT_COLORS[i % COMPONENT_COLORS.length] }}
                    />
                    {c.category}
                  </span>
                  <div className="flex items-center gap-3">
                    <span className="text-slate-500 text-[10px]">{c.pct_of_revenue?.toFixed(1)}%</span>
                    <span className="text-slate-700 text-xs font-medium">{fmtUsd(c.amount)}</span>
                  </div>
                </div>
              ))}
              {/* Total COGS */}
              <div className="flex items-center justify-between px-3 py-2 rounded-lg bg-slate-50 border-t border-slate-200">
                <span className="text-slate-700 text-xs font-semibold">Total COGS</span>
                <span className="text-red-400 text-xs font-bold">{fmtUsd(latest.total_cogs)}</span>
              </div>
              {/* Gross Margin */}
              <div className="flex items-center justify-between px-3 py-2 rounded-lg bg-emerald-500/10">
                <span className="text-emerald-400 text-xs font-semibold">Gross Margin</span>
                <span className="text-emerald-400 text-xs font-bold">
                  {latest.gross_margin_pct?.toFixed(1)}%
                </span>
              </div>
            </div>
          </div>
        )
      })()}
    </div>
  )
}
