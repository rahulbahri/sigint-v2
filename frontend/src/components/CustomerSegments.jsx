import { useState, useEffect } from 'react'
import axios from 'axios'
import { PieChart, Users, TrendingDown, AlertTriangle, DollarSign } from 'lucide-react'

function fmtUsd(v) {
  if (v == null) return '-'
  return `$${v.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
}

const TIER_COLORS = {
  Enterprise:  { bg: 'bg-blue-50',    border: 'border-blue-200',    text: 'text-blue-700',    bar: 'bg-blue-500' },
  'Mid-Market':{ bg: 'bg-violet-50',  border: 'border-violet-200',  text: 'text-violet-700',  bar: 'bg-violet-500' },
  SMB:         { bg: 'bg-emerald-50', border: 'border-emerald-200', text: 'text-emerald-700', bar: 'bg-emerald-500' },
}

function TierCard({ seg }) {
  const style = TIER_COLORS[seg.tier] || TIER_COLORS.SMB
  return (
    <div className={`rounded-xl border p-4 ${style.bg} ${style.border}`}>
      <div className="flex items-center justify-between mb-2">
        <h3 className={`text-[13px] font-bold ${style.text}`}>{seg.tier}</h3>
        <span className="text-[10px] font-semibold text-slate-500">{seg.revenue_share_pct}% of revenue</span>
      </div>
      <div className="grid grid-cols-2 gap-3 mb-2">
        <div>
          <p className="text-[9px] text-slate-400 uppercase tracking-wider">Customers</p>
          <p className="text-lg font-bold text-slate-800">{seg.customers}</p>
        </div>
        <div>
          <p className="text-[9px] text-slate-400 uppercase tracking-wider">ARPU</p>
          <p className="text-lg font-bold text-slate-800">{fmtUsd(seg.arpu)}</p>
        </div>
        <div>
          <p className="text-[9px] text-slate-400 uppercase tracking-wider">Revenue</p>
          <p className="text-[13px] font-bold text-slate-700">{fmtUsd(seg.revenue)}</p>
        </div>
        <div>
          <p className="text-[9px] text-slate-400 uppercase tracking-wider">Churn</p>
          <p className={`text-[13px] font-bold ${seg.churn_rate > 5 ? 'text-red-600' : seg.churn_rate > 0 ? 'text-amber-600' : 'text-emerald-600'}`}>
            {seg.churn_rate.toFixed(1)}%
            {seg.churned > 0 && <span className="text-[10px] font-normal text-slate-400 ml-1">({seg.churned} lost)</span>}
          </p>
        </div>
      </div>
      {/* Revenue share bar */}
      <div className="h-1.5 rounded-full bg-white/60 overflow-hidden">
        <div className={`h-full ${style.bar} rounded-full`} style={{ width: `${Math.min(seg.revenue_share_pct, 100)}%` }} />
      </div>
    </div>
  )
}

export default function CustomerSegments() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    axios.get('/api/segments')
      .then(r => setData(r.data))
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <div className="flex items-center justify-center h-40 text-slate-400 text-sm">Loading segments...</div>
  if (!data?.segments?.by_tier?.length) {
    return (
      <div className="max-w-7xl space-y-5">
        <h1 className="text-lg font-bold text-slate-800 flex items-center gap-2">
          <PieChart size={18} className="text-[#0055A4]" />
          Customer Segments
        </h1>
        <div className="bg-white rounded-2xl border border-slate-100 shadow-sm p-10 text-center">
          <Users size={28} className="text-slate-200 mx-auto mb-2" />
          <p className="text-slate-500 text-sm font-semibold">No customer data available</p>
          <p className="text-slate-400 text-xs mt-1">Upload data with customer IDs to enable segmentation.</p>
        </div>
      </div>
    )
  }

  const { by_tier, by_stage } = data.segments
  const { summary } = data

  return (
    <div className="max-w-7xl space-y-5">
      {/* Header */}
      <div>
        <h1 className="text-lg font-bold text-slate-800 flex items-center gap-2">
          <PieChart size={18} className="text-[#0055A4]" />
          Customer Segments
        </h1>
        <p className="text-[12px] text-slate-500 mt-0.5">
          Revenue and health by customer tier and lifecycle stage. Period: {summary.period}
        </p>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <div className="bg-white rounded-xl border border-slate-100 shadow-sm px-4 py-3 text-center">
          <p className="text-[10px] text-slate-400 uppercase tracking-wider">Total Customers</p>
          <p className="text-xl font-bold text-slate-800">{summary.total_customers}</p>
        </div>
        <div className="bg-white rounded-xl border border-slate-100 shadow-sm px-4 py-3 text-center">
          <p className="text-[10px] text-slate-400 uppercase tracking-wider">Total Revenue</p>
          <p className="text-xl font-bold text-slate-800">{fmtUsd(summary.total_revenue)}</p>
        </div>
        <div className="bg-white rounded-xl border border-slate-100 shadow-sm px-4 py-3 text-center">
          <p className="text-[10px] text-slate-400 uppercase tracking-wider">Top Customer</p>
          <p className={`text-xl font-bold ${summary.top1_concentration_pct > 25 ? 'text-red-600' : 'text-slate-800'}`}>
            {summary.top1_concentration_pct}%
          </p>
        </div>
        <div className="bg-white rounded-xl border border-slate-100 shadow-sm px-4 py-3 text-center">
          <p className="text-[10px] text-slate-400 uppercase tracking-wider">Top 5 Concentration</p>
          <p className={`text-xl font-bold ${summary.top5_concentration_pct > 60 ? 'text-amber-600' : 'text-slate-800'}`}>
            {summary.top5_concentration_pct}%
          </p>
        </div>
      </div>

      {/* Revenue tier breakdown */}
      <div>
        <h2 className="text-[11px] font-bold text-slate-600 uppercase tracking-wider mb-3">By Revenue Tier</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          {by_tier.map(seg => <TierCard key={seg.tier} seg={seg} />)}
        </div>
      </div>

      {/* Lifecycle stage breakdown */}
      {by_stage.length > 0 && (
        <div>
          <h2 className="text-[11px] font-bold text-slate-600 uppercase tracking-wider mb-3">By Lifecycle Stage</h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
            {by_stage.map(seg => (
              <div key={seg.stage} className="bg-white rounded-xl border border-slate-100 shadow-sm px-4 py-3">
                <p className="text-[11px] font-semibold text-slate-700 truncate">{seg.stage}</p>
                <p className="text-lg font-bold text-slate-800 mt-1">{seg.customers}</p>
                <p className="text-[10px] text-slate-400">{fmtUsd(seg.revenue)} · {seg.revenue_share_pct}%</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Concentration risk warning */}
      {summary.top1_concentration_pct > 20 && (
        <div className="bg-amber-50 border border-amber-200 rounded-xl px-4 py-3 flex items-start gap-2">
          <AlertTriangle size={14} className="text-amber-500 mt-0.5 shrink-0" />
          <div>
            <p className="text-[11px] font-bold text-amber-700">Revenue Concentration Risk</p>
            <p className="text-[10px] text-amber-600 leading-relaxed">
              Your largest customer represents {summary.top1_concentration_pct}% of revenue.
              {summary.top1_concentration_pct > 30 && ' This exceeds the 30% threshold for investor concern.'}
              {' '}Diversify your customer base to reduce single-client dependency.
            </p>
          </div>
        </div>
      )}
    </div>
  )
}
