import { useState, useEffect, useMemo } from 'react'
import axios from 'axios'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  LineChart, Line, Cell, ReferenceLine,
} from 'recharts'
import { Wallet, TrendingDown, Clock } from 'lucide-react'
import { fmtKpiValueCompact } from './kpiFormat'

// Category colors for the waterfall
const WATERFALL_COLORS = {
  opening:  '#003087',  // dark blue — starting point
  inflow:   '#059669',  // green — revenue
  expense:  '#DC2626',  // red — expenses
  closing:  '#0055A4',  // blue — ending point
}

function WaterfallTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload
  if (!d) return null
  return (
    <div className="bg-white border border-slate-200 rounded-lg px-3 py-2 text-xs shadow-lg">
      <p className="text-slate-700 font-semibold mb-1">{d.label}</p>
      <p className="text-slate-700">{fmtKpiValueCompact(Math.abs(d.displayValue), 'usd')}</p>
    </div>
  )
}

function TrendTooltip({ active, payload, label }) {
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
            {fmtKpiValueCompact(p.value, 'usd')}
          </span>
        </div>
      ))}
    </div>
  )
}

export default function CashWaterfall() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    axios.get('/api/analytics/cash-waterfall')
      .then(r => setData(r.data))
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }, [])

  // Build waterfall chart data from the latest period
  const { waterfallData, latest, trendData } = useMemo(() => {
    if (!data?.periods?.length) return { waterfallData: [], latest: null, trendData: [] }

    const periods = data.periods
    const latestPeriod = periods[periods.length - 1]

    // Build waterfall bars for the latest period
    // Each bar needs: a hidden base (invisible) + visible portion
    const bars = []
    let running = 0

    // Opening cash
    bars.push({
      label: 'Opening Cash',
      displayValue: latestPeriod.opening_cash,
      base: 0,
      value: latestPeriod.opening_cash,
      type: 'opening',
    })
    running = latestPeriod.opening_cash

    // Revenue inflow (positive)
    bars.push({
      label: 'Revenue',
      displayValue: latestPeriod.revenue_inflow,
      base: running,
      value: latestPeriod.revenue_inflow,
      type: 'inflow',
    })
    running += latestPeriod.revenue_inflow

    // Expense categories (negative)
    latestPeriod.expense_categories?.forEach(exp => {
      const amt = Math.abs(exp.amount)
      running -= amt
      bars.push({
        label: exp.category,
        displayValue: -amt,
        base: running,
        value: amt,
        type: 'expense',
      })
    })

    // Closing cash
    bars.push({
      label: 'Closing Cash',
      displayValue: latestPeriod.closing_cash,
      base: 0,
      value: latestPeriod.closing_cash,
      type: 'closing',
    })

    // Trend data for line chart
    const trend = periods.map(p => ({
      period: p.period,
      closing_cash: p.closing_cash,
      net_burn: p.net_burn,
    }))

    return { waterfallData: bars, latest: latestPeriod, trendData: trend }
  }, [data])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-40 text-slate-500 text-sm">
        Loading cash flow data...
      </div>
    )
  }

  if (!data?.periods?.length) {
    return (
      <div className="space-y-5">
        <h1 className="text-lg font-bold text-slate-800 flex items-center gap-2">
          <Wallet size={18} className="text-[#0055A4]" />
          Cash Waterfall
        </h1>
        <div className="bg-white rounded-2xl border border-slate-200 p-10 text-center">
          <Wallet size={28} className="text-slate-500 mx-auto mb-2" />
          <p className="text-slate-700 text-sm font-semibold">No cash flow data available</p>
          <p className="text-slate-500 text-xs mt-1">
            Upload financial data with cash flow details to enable waterfall analysis.
          </p>
        </div>
      </div>
    )
  }

  const runwayColor = latest.runway_months >= 18
    ? 'text-emerald-400'
    : latest.runway_months >= 12
      ? 'text-[#D97706]'
      : 'text-[#DC2626]'

  return (
    <div className="space-y-5">
      {/* Header */}
      <div>
        <h1 className="text-lg font-bold text-slate-800 flex items-center gap-2">
          <Wallet size={18} className="text-[#0055A4]" />
          Cash Waterfall
        </h1>
        <p className="text-[12px] text-slate-500 mt-0.5">
          Cash flow waterfall for {latest.period} with historical trend.
        </p>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-3 gap-3">
        <div className="bg-white rounded-xl border border-slate-200 px-4 py-3 text-center">
          <p className="text-[10px] text-slate-500 uppercase tracking-wider">Current Cash</p>
          <p className="text-xl font-bold text-slate-800">
            {fmtKpiValueCompact(latest.closing_cash, 'usd')}
          </p>
        </div>
        <div className="bg-white rounded-xl border border-slate-200 px-4 py-3 text-center">
          <div className="flex items-center justify-center gap-1 mb-0.5">
            <TrendingDown size={12} className="text-[#DC2626]" />
            <p className="text-[10px] text-slate-500 uppercase tracking-wider">Monthly Burn</p>
          </div>
          <p className="text-xl font-bold text-[#DC2626]">
            {fmtKpiValueCompact(Math.abs(latest.net_burn), 'usd')}
          </p>
        </div>
        <div className="bg-white rounded-xl border border-slate-200 px-4 py-3 text-center">
          <div className="flex items-center justify-center gap-1 mb-0.5">
            <Clock size={12} className={runwayColor} />
            <p className="text-[10px] text-slate-500 uppercase tracking-wider">Runway</p>
          </div>
          <p className={`text-xl font-bold ${runwayColor}`}>
            {latest.runway_months != null ? `${latest.runway_months.toFixed(1)} mo` : '\u2014'}
          </p>
        </div>
      </div>

      {/* Waterfall Chart */}
      <div className="bg-white rounded-2xl border border-slate-200 p-5">
        <h2 className="text-[11px] font-bold text-slate-500 uppercase tracking-wider mb-3">
          Cash Flow Waterfall &mdash; {latest.period}
        </h2>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={waterfallData} barCategoryGap="15%">
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
              <XAxis
                dataKey="label"
                tick={{ fontSize: 10, fill: '#9ca3af' }}
                axisLine={{ stroke: 'rgba(255,255,255,0.1)' }}
                tickLine={false}
                interval={0}
                angle={-20}
                textAnchor="end"
                height={50}
              />
              <YAxis
                tick={{ fontSize: 10, fill: '#9ca3af' }}
                tickFormatter={v => fmtKpiValueCompact(v, 'usd')}
                axisLine={{ stroke: 'rgba(255,255,255,0.1)' }}
                tickLine={false}
              />
              <Tooltip content={<WaterfallTooltip />} cursor={false} />
              <ReferenceLine y={0} stroke="rgba(255,255,255,0.15)" />
              {/* Invisible base bar */}
              <Bar dataKey="base" stackId="waterfall" fill="transparent" isAnimationActive={false} />
              {/* Visible value bar */}
              <Bar dataKey="value" stackId="waterfall" radius={[3, 3, 0, 0]}>
                {waterfallData.map((entry, i) => (
                  <Cell
                    key={i}
                    fill={WATERFALL_COLORS[entry.type] || WATERFALL_COLORS.expense}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Cash Trend Over Time */}
      {trendData.length > 1 && (
        <div className="bg-white rounded-2xl border border-slate-200 p-5">
          <h2 className="text-[11px] font-bold text-slate-500 uppercase tracking-wider mb-3">
            Closing Cash Trend
          </h2>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={trendData}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
                <XAxis
                  dataKey="period"
                  tick={{ fontSize: 10, fill: '#9ca3af' }}
                  axisLine={{ stroke: 'rgba(255,255,255,0.1)' }}
                  tickLine={false}
                />
                <YAxis
                  tick={{ fontSize: 10, fill: '#9ca3af' }}
                  tickFormatter={v => fmtKpiValueCompact(v, 'usd')}
                  axisLine={{ stroke: 'rgba(255,255,255,0.1)' }}
                  tickLine={false}
                />
                <Tooltip content={<TrendTooltip />} />
                <Line
                  type="monotone"
                  dataKey="closing_cash"
                  stroke="#0055A4"
                  strokeWidth={2.5}
                  dot={{ r: 3, fill: '#0055A4', stroke: '#1a1f2e', strokeWidth: 2 }}
                  name="Closing Cash"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}
    </div>
  )
}
