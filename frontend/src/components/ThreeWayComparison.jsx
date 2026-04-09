import { useState, useEffect } from 'react'
import axios from 'axios'
import { GitBranch, TrendingUp, TrendingDown, Target, AlertTriangle, CheckCircle2 } from 'lucide-react'

function fmtVal(v, unit) {
  if (v == null) return '-'
  if (unit === 'pct') return `${v.toFixed(1)}%`
  if (unit === 'usd' || unit === '$') return `$${v.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
  if (unit === 'ratio') return `${v.toFixed(2)}x`
  return v.toFixed(2)
}

function GapBadge({ pct }) {
  if (pct == null) return <span className="text-[10px] text-slate-300">-</span>
  const color = pct >= 0 ? 'text-emerald-600' : pct >= -3 ? 'text-amber-600' : 'text-red-600'
  return <span className={`text-[10px] font-bold ${color}`}>{pct > 0 ? '+' : ''}{pct.toFixed(1)}%</span>
}

export default function ThreeWayComparison() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    axios.get('/api/bridge')
      .then(r => setData(r.data))
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <div className="flex items-center justify-center h-40 text-slate-400">Loading...</div>
  if (!data?.has_projection) {
    return (
      <div className="max-w-6xl mx-auto px-6 py-6 space-y-5">
        <h1 className="text-xl font-bold text-slate-900 flex items-center gap-2">
          <GitBranch size={20} className="text-[#0055A4]" /> Three-Way Comparison
        </h1>
        <div className="bg-white rounded-2xl border shadow-sm p-10 text-center">
          <Target size={28} className="text-slate-200 mx-auto mb-2" />
          <p className="text-slate-500 text-sm font-semibold">No projections uploaded</p>
          <p className="text-slate-400 text-xs mt-1">Upload a projection file first to compare Actuals vs Projections vs Targets.</p>
        </div>
      </div>
    )
  }

  const kpis = Object.entries(data.kpis || {}).sort((a, b) => (a[1].avg_gap_pct || 0) - (b[1].avg_gap_pct || 0))

  return (
    <div className="max-w-7xl space-y-5">
      <div>
        <h1 className="text-xl font-bold text-slate-900 flex items-center gap-2">
          <GitBranch size={20} className="text-[#0055A4]" /> Three-Way Comparison
        </h1>
        <p className="text-[12px] text-slate-500 mt-0.5">
          Actuals vs Projections vs Targets — side by side.
        </p>
        <span className="inline-block mt-1 text-[9px] font-bold text-amber-600 bg-amber-50 border border-amber-200 px-2 py-0.5 rounded-full">
          ROADMAPPED — Under Testing
        </span>
      </div>

      {/* Summary */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <div className="bg-white rounded-xl border shadow-sm px-4 py-3 text-center">
          <p className="text-[10px] text-slate-400 uppercase">On Track</p>
          <p className="text-xl font-bold text-emerald-600">{data.summary?.on_track || 0}</p>
        </div>
        <div className="bg-white rounded-xl border shadow-sm px-4 py-3 text-center">
          <p className="text-[10px] text-slate-400 uppercase">Behind Plan</p>
          <p className="text-xl font-bold text-red-600">{data.summary?.behind || 0}</p>
        </div>
        <div className="bg-white rounded-xl border shadow-sm px-4 py-3 text-center">
          <p className="text-[10px] text-slate-400 uppercase">Ahead</p>
          <p className="text-xl font-bold text-blue-600">{data.summary?.ahead || 0}</p>
        </div>
        <div className="bg-white rounded-xl border shadow-sm px-4 py-3 text-center">
          <p className="text-[10px] text-slate-400 uppercase">Months Compared</p>
          <p className="text-xl font-bold text-slate-700">{data.summary?.total_months_compared || 0}</p>
        </div>
      </div>

      {/* KPI Table */}
      <div className="bg-white rounded-2xl border shadow-sm overflow-hidden">
        <div className="px-4 py-3 bg-slate-50 border-b border-slate-100">
          <div className="grid grid-cols-7 gap-2 text-[9px] font-bold text-slate-500 uppercase tracking-wider">
            <span className="col-span-2">KPI</span>
            <span className="text-right">Actual</span>
            <span className="text-right">Projected</span>
            <span className="text-right">Target</span>
            <span className="text-right">vs Plan</span>
            <span className="text-right">vs Target</span>
          </div>
        </div>
        <div className="divide-y divide-slate-50">
          {kpis.map(([key, kpi]) => (
            <div key={key} className="grid grid-cols-7 gap-2 px-4 py-2.5 hover:bg-slate-50/60 items-center">
              <div className="col-span-2">
                <span className="text-[11px] font-semibold text-slate-700">{kpi.name}</span>
              </div>
              <span className="text-[11px] text-slate-800 font-bold text-right">{fmtVal(kpi.avg_actual, kpi.unit)}</span>
              <span className="text-[11px] text-blue-600 text-right">{fmtVal(kpi.avg_projected, kpi.unit)}</span>
              <span className="text-[11px] text-slate-500 text-right">{fmtVal(kpi.target_value, kpi.unit)}</span>
              <div className="text-right"><GapBadge pct={kpi.avg_gap_pct} /></div>
              <div className="text-right"><GapBadge pct={kpi.actual_vs_target_pct} /></div>
            </div>
          ))}
        </div>
      </div>

      {kpis.length === 0 && (
        <div className="text-center py-6 text-slate-400 text-sm">
          No overlapping periods between actuals and projections.
        </div>
      )}
    </div>
  )
}
