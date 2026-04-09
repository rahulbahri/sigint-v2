import { useState, useEffect } from 'react'
import axios from 'axios'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import { DollarSign, Clock, AlertTriangle } from 'lucide-react'

function fmtUsd(v) {
  if (v == null) return '-'
  return `$${v.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
}

export default function DeferredRevenuePage() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    axios.get('/api/deferred-revenue')
      .then(r => setData(r.data))
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <div className="flex items-center justify-center h-40 text-slate-400 text-sm">Loading...</div>
  if (!data?.schedule?.length) {
    return (
      <div className="max-w-7xl space-y-5">
        <h1 className="text-lg font-bold text-slate-800 flex items-center gap-2">
          <Clock size={18} className="text-[#0055A4]" /> Revenue Recognition
        </h1>
        <div className="bg-white rounded-2xl border border-slate-100 shadow-sm p-10 text-center">
          <DollarSign size={28} className="text-slate-200 mx-auto mb-2" />
          <p className="text-slate-500 text-sm font-semibold">No revenue data available</p>
          <p className="text-slate-400 text-xs mt-1">Upload data with subscription_type to enable ASC 606 tracking.</p>
        </div>
      </div>
    )
  }

  const { schedule, summary, by_type } = data

  // Chart data — last 12 periods
  const chartData = schedule.slice(-12).map(s => ({
    period: s.period.slice(2), // "24-01" format
    Recognized: Math.round(s.recognized),
    Deferred: Math.round(s.deferred_balance),
    Bookings: Math.round(s.bookings),
  }))

  return (
    <div className="max-w-7xl space-y-5">
      <div>
        <h1 className="text-lg font-bold text-slate-800 flex items-center gap-2">
          <Clock size={18} className="text-[#0055A4]" /> Revenue Recognition (ASC 606)
        </h1>
        <p className="text-[12px] text-slate-500 mt-0.5">
          Deferred revenue tracking by subscription type. Annual contracts recognized ratably over 12 months.
        </p>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <div className="bg-white rounded-xl border border-slate-100 shadow-sm px-4 py-3 text-center">
          <p className="text-[10px] text-slate-400 uppercase tracking-wider">Total Booked</p>
          <p className="text-xl font-bold text-slate-800">{fmtUsd(summary.total_booked)}</p>
        </div>
        <div className="bg-white rounded-xl border border-slate-100 shadow-sm px-4 py-3 text-center">
          <p className="text-[10px] text-slate-400 uppercase tracking-wider">Total Recognized</p>
          <p className="text-xl font-bold text-emerald-600">{fmtUsd(summary.total_recognized)}</p>
        </div>
        <div className="bg-white rounded-xl border border-slate-100 shadow-sm px-4 py-3 text-center">
          <p className="text-[10px] text-slate-400 uppercase tracking-wider">Current Deferred</p>
          <p className={`text-xl font-bold ${summary.current_deferred > 0 ? 'text-amber-600' : 'text-slate-800'}`}>
            {fmtUsd(summary.current_deferred)}
          </p>
        </div>
        <div className="bg-white rounded-xl border border-slate-100 shadow-sm px-4 py-3 text-center">
          <p className="text-[10px] text-slate-400 uppercase tracking-wider">Deferred %</p>
          <p className="text-xl font-bold text-slate-800">{summary.deferred_pct}%</p>
        </div>
      </div>

      {/* Chart */}
      <div className="bg-white rounded-2xl border border-slate-100 shadow-sm p-5">
        <h2 className="text-[11px] font-bold text-slate-600 uppercase tracking-wider mb-3">Recognition Schedule</h2>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartData} barCategoryGap="20%">
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis dataKey="period" tick={{ fontSize: 10 }} />
              <YAxis tick={{ fontSize: 10 }} tickFormatter={v => `$${(v / 1000).toFixed(0)}k`} />
              <Tooltip formatter={v => `$${v.toLocaleString()}`} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Bar dataKey="Bookings" fill="#93c5fd" radius={[2, 2, 0, 0]} />
              <Bar dataKey="Recognized" fill="#059669" radius={[2, 2, 0, 0]} />
              <Bar dataKey="Deferred" fill="#D97706" radius={[2, 2, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* By subscription type */}
      <div className="bg-white rounded-2xl border border-slate-100 shadow-sm p-5">
        <h2 className="text-[11px] font-bold text-slate-600 uppercase tracking-wider mb-3">By Subscription Type</h2>
        <div className="space-y-2">
          {by_type.map(t => (
            <div key={t.type} className="flex items-center justify-between px-3 py-2.5 bg-slate-50 rounded-lg">
              <div className="flex items-center gap-3">
                <span className="text-[12px] font-semibold text-slate-700 capitalize w-24">{t.type}</span>
                <span className="text-[10px] text-slate-400">{t.count} transactions</span>
                <span className="text-[10px] text-slate-400">{t.term_months}mo term</span>
              </div>
              <div className="flex items-center gap-3">
                <span className="text-[12px] font-bold text-slate-800">{fmtUsd(t.amount)}</span>
                <span className="text-[10px] text-slate-400">{t.pct_of_total}%</span>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* ASC 606 compliance note */}
      <div className="bg-blue-50 border border-blue-200 rounded-xl px-4 py-3 flex items-start gap-2">
        <AlertTriangle size={14} className="text-blue-500 mt-0.5 shrink-0" />
        <div>
          <p className="text-[11px] font-bold text-blue-700">ASC 606 Note</p>
          <p className="text-[10px] text-blue-600 leading-relaxed">
            This schedule assumes ratable recognition for multi-month contracts.
            Revenue is recognized evenly over the contract term starting from the booking period.
            Consult your auditor for complex arrangements (milestone-based, usage-based, or bundled contracts).
          </p>
        </div>
      </div>
    </div>
  )
}
