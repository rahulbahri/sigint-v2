import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, ReferenceLine,
} from 'recharts'
import { DollarSign, RefreshCw, TrendingUp, AlertTriangle } from 'lucide-react'
import { fmtKpiValueCompact } from './kpiFormat'

// ── Waterfall helpers ──────────────────────────────────────────────────────
function buildWaterfallData(metrics) {
  if (!metrics?.length) return []

  const byLabel = {}
  metrics.forEach(m => { byLabel[m.label.toLowerCase()] = m })

  const arpu        = byLabel['arpu']?.value        ?? 0
  const cogs        = byLabel['cogs']?.value        ?? 0
  const grossProfit = arpu - cogs
  const cac         = byLabel['cac']?.value         ?? 0
  const netContrib  = grossProfit - cac

  let running = 0
  const steps = [
    { name: 'ARPU',            value: arpu,         isPositive: true  },
    { name: 'COGS',            value: -cogs,        isPositive: false },
    { name: 'Gross Profit',    value: grossProfit,  isPositive: true  },
    { name: 'CAC',             value: -cac,         isPositive: false },
    { name: 'Net Contribution', value: netContrib,  isPositive: netContrib >= 0 },
  ]

  return steps.map(s => {
    const base = running
    running += s.value
    return {
      name:       s.name,
      value:      s.value,
      base:       Math.min(base, base + s.value),
      top:        Math.max(base, base + s.value),
      isPositive: s.isPositive,
    }
  })
}

function WaterfallTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload
  if (!d) return null
  return (
    <div className="bg-white border border-slate-200 rounded-lg px-3 py-2 shadow-xl text-xs">
      <p className="text-slate-800 font-semibold mb-0.5">{d.name}</p>
      <p className={d.isPositive ? 'text-emerald-400' : 'text-red-400'}>
        {d.value >= 0 ? '+' : ''}{fmtKpiValueCompact(d.value, 'usd')}
      </p>
    </div>
  )
}

// ── Summary card ───────────────────────────────────────────────────────────
function SummaryCard({ label, value, unit, color }) {
  return (
    <div className="bg-white border border-slate-200 rounded-xl p-5 text-center">
      <div className={`text-3xl font-bold ${color}`}>
        {fmtKpiValueCompact(value, unit)}
      </div>
      <div className="text-slate-500 text-xs mt-1.5 uppercase tracking-wider font-medium">
        {label}
      </div>
    </div>
  )
}

// ── Main component ─────────────────────────────────────────────────────────
export default function UnitEconomics({ periodDates }) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState('')

  const load = useCallback(async () => {
    setLoading(true); setError('')
    try {
      const params = new URLSearchParams()
      if (periodDates?.fromYear) {
        params.set('from_year', periodDates.fromYear)
        params.set('from_month', periodDates.fromMonth)
        params.set('to_year', periodDates.toYear)
        params.set('to_month', periodDates.toMonth)
      }
      const { data: d } = await axios.get(`/api/analytics/unit-economics?${params}`)
      setData(d)
    } catch (e) {
      setError(e.response?.data?.detail || 'Failed to load unit economics')
    } finally {
      setLoading(false)
    }
  }, [periodDates?.fromYear, periodDates?.fromMonth, periodDates?.toYear, periodDates?.toMonth])

  useEffect(() => { load() }, [load])

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
  if (!data?.metrics?.length) return (
    <div className="p-6 max-w-4xl mx-auto space-y-5">
      <h2 className="text-slate-800 text-xl font-semibold flex items-center gap-2">
        <DollarSign size={20} className="text-[#0055A4]" />
        Unit Economics
      </h2>
      <div className="bg-white border border-slate-200 rounded-2xl p-12 text-center">
        <AlertTriangle size={32} className="text-slate-500 mx-auto mb-3" />
        <p className="text-slate-500 text-sm font-semibold">No unit economics data available</p>
        <p className="text-slate-500 text-xs mt-1">Upload customer-level revenue and cost data to enable unit economics analysis.</p>
      </div>
    </div>
  )

  const waterfallData = buildWaterfallData(data.metrics)

  return (
    <div className="p-6 max-w-4xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-slate-800 text-xl font-semibold flex items-center gap-2">
            <DollarSign size={20} className="text-[#0055A4]" />
            Unit Economics
          </h2>
          <p className="text-slate-500 text-sm mt-1">
            {data.customer_count != null && `${data.customer_count.toLocaleString()} customers`}
            {data.months_of_data != null && ` \u00b7 ${data.months_of_data} months of data`}
          </p>
        </div>
        <button
          onClick={load}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-slate-500 hover:text-slate-800 bg-slate-50 hover:bg-slate-100 rounded-lg transition-colors"
        >
          <RefreshCw size={12} /> Refresh
        </button>
      </div>

      {/* Waterfall chart */}
      <div className="bg-white border border-slate-200 rounded-2xl p-5">
        <h3 className="text-slate-800 text-sm font-semibold mb-1">Revenue Waterfall</h3>
        <p className="text-slate-500 text-xs mb-4">ARPU through to net contribution per customer</p>
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={waterfallData} margin={{ top: 10, right: 20, bottom: 5, left: 20 }}>
            <XAxis
              dataKey="name"
              tick={{ fill: '#9ca3af', fontSize: 11 }}
              axisLine={{ stroke: '#374151' }}
              tickLine={false}
            />
            <YAxis
              tick={{ fill: '#9ca3af', fontSize: 10 }}
              tickFormatter={v => fmtKpiValueCompact(v, 'usd')}
              axisLine={false}
              tickLine={false}
            />
            <Tooltip content={<WaterfallTooltip />} cursor={false} />
            <ReferenceLine y={0} stroke="#4b5563" strokeWidth={1} />
            {/* Invisible base bar for waterfall offset */}
            <Bar dataKey="base" stackId="stack" fill="transparent" />
            {/* Visible bar for the step value */}
            <Bar dataKey={(d) => d.top - d.base} stackId="stack" radius={[4, 4, 0, 0]} barSize={48}>
              {waterfallData.map((entry, i) => (
                <Cell key={i} fill={entry.isPositive ? '#059669' : '#DC2626'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Metrics detail row */}
      {data.metrics.length > 0 && (
        <div className="bg-white border border-slate-200 rounded-2xl p-5">
          <h3 className="text-slate-800 text-sm font-semibold mb-3">All Metrics</h3>
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
            {data.metrics.map((m, i) => (
              <div key={i} className="bg-slate-50 rounded-lg px-3 py-2.5">
                <div className="text-slate-500 text-[10px] uppercase tracking-wider mb-0.5">{m.label}</div>
                <div className="text-slate-800 text-base font-bold">
                  {fmtKpiValueCompact(m.value, m.unit)}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* LTV / CAC / Ratio summary cards */}
      <div className="grid grid-cols-3 gap-4">
        <SummaryCard
          label="Customer LTV"
          value={data.ltv}
          unit="usd"
          color="text-emerald-400"
        />
        <SummaryCard
          label="CAC"
          value={data.cac}
          unit="usd"
          color="text-red-400"
        />
        <SummaryCard
          label="LTV : CAC"
          value={data.ltv_cac_ratio}
          unit="ratio"
          color={
            data.ltv_cac_ratio >= 3 ? 'text-emerald-400'
              : data.ltv_cac_ratio >= 1 ? 'text-yellow-400'
              : 'text-red-400'
          }
        />
      </div>

      {/* Health indicator */}
      {data.ltv_cac_ratio != null && (
        <div className={`flex items-center gap-2 px-4 py-3 rounded-xl text-sm font-medium ${
          data.ltv_cac_ratio >= 3
            ? 'bg-emerald-500/10 border border-emerald-500/20 text-emerald-400'
            : data.ltv_cac_ratio >= 1
            ? 'bg-yellow-500/10 border border-yellow-500/20 text-yellow-400'
            : 'bg-red-500/10 border border-red-500/20 text-red-400'
        }`}>
          <TrendingUp size={16} />
          {data.ltv_cac_ratio >= 3
            ? 'Healthy unit economics \u2014 LTV:CAC above 3x benchmark'
            : data.ltv_cac_ratio >= 1
            ? 'Unit economics need improvement \u2014 LTV:CAC below 3x'
            : 'Unhealthy unit economics \u2014 CAC exceeds lifetime value'}
        </div>
      )}
    </div>
  )
}
