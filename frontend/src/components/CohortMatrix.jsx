import { useState, useEffect } from 'react'
import axios from 'axios'
import { Users, ToggleLeft, ToggleRight } from 'lucide-react'

const RETENTION_COLORS = {
  high:   'bg-emerald-700/80 text-white',       // >100%
  good:   'bg-emerald-600/50 text-emerald-100',  // 80-100%
  medium: 'bg-yellow-600/40 text-yellow-100',    // 60-80%
  low:    'bg-red-600/50 text-red-200',           // <60%
}

function retentionClass(pct) {
  if (pct == null) return 'bg-transparent text-gray-600'
  if (pct > 100) return RETENTION_COLORS.high
  if (pct >= 80) return RETENTION_COLORS.good
  if (pct >= 60) return RETENTION_COLORS.medium
  return RETENTION_COLORS.low
}

function fmtPct(v) {
  if (v == null) return ''
  return `${v.toFixed(1)}%`
}

function fmtSize(v, metric) {
  if (v == null) return '-'
  if (metric === 'revenue') {
    if (v >= 1e6) return `$${(v / 1e6).toFixed(1)}M`
    if (v >= 1e3) return `$${(v / 1e3).toFixed(1)}K`
    return `$${v.toFixed(0)}`
  }
  return v.toLocaleString()
}

export default function CohortMatrix() {
  const [metric, setMetric] = useState('revenue')
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    axios.get(`/api/analytics/cohort-retention?metric=${metric}`)
      .then(r => setData(r.data))
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }, [metric])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-40 text-gray-400 text-sm">
        Loading cohort data...
      </div>
    )
  }

  if (!data?.cohorts?.length) {
    return (
      <div className="space-y-5">
        <h1 className="text-lg font-bold text-white flex items-center gap-2">
          <Users size={18} className="text-[#0055A4]" />
          Cohort Retention
        </h1>
        <div className="bg-[#1a1f2e] rounded-2xl border border-white/8 p-10 text-center">
          <Users size={28} className="text-gray-600 mx-auto mb-2" />
          <p className="text-gray-300 text-sm font-semibold">No cohort data available</p>
          <p className="text-gray-500 text-xs mt-1">
            Upload transaction data with customer IDs and dates to enable cohort analysis.
          </p>
        </div>
      </div>
    )
  }

  const { cohorts } = data

  // Determine max month offset across all cohorts
  const maxOffset = cohorts.reduce((mx, c) => {
    const cMax = c.months?.length ? Math.max(...c.months.map(m => m.month_offset)) : 0
    return Math.max(mx, cMax)
  }, 0)
  const offsets = Array.from({ length: maxOffset + 1 }, (_, i) => i)

  return (
    <div className="space-y-5">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-lg font-bold text-white flex items-center gap-2">
            <Users size={18} className="text-[#0055A4]" />
            Cohort Retention
          </h1>
          <p className="text-[12px] text-gray-400 mt-0.5">
            {metric === 'revenue' ? 'Revenue' : 'Customer count'} retention by acquisition cohort.
          </p>
        </div>

        {/* Toggle */}
        <button
          onClick={() => setMetric(m => m === 'revenue' ? 'count' : 'revenue')}
          className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-white/5 border border-white/8
                     text-gray-300 text-xs font-medium hover:bg-white/10 transition-colors"
        >
          {metric === 'revenue' ? (
            <ToggleRight size={16} className="text-[#0055A4]" />
          ) : (
            <ToggleLeft size={16} className="text-gray-500" />
          )}
          {metric === 'revenue' ? 'Revenue' : 'Customer Count'}
        </button>
      </div>

      {/* Cohort Triangle Table */}
      <div className="bg-[#1a1f2e] rounded-2xl border border-white/8 p-5 overflow-x-auto">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr>
              <th className="text-left text-gray-400 font-semibold px-2 py-2 whitespace-nowrap">
                Cohort
              </th>
              <th className="text-right text-gray-400 font-semibold px-2 py-2 whitespace-nowrap">
                Size
              </th>
              {offsets.map(o => (
                <th key={o} className="text-center text-gray-400 font-semibold px-2 py-2 whitespace-nowrap">
                  M{o}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {cohorts.map(cohort => {
              // Build a lookup for quick access
              const monthMap = {}
              cohort.months?.forEach(m => { monthMap[m.month_offset] = m })

              return (
                <tr key={cohort.acquisition_period} className="border-t border-white/5">
                  <td className="text-gray-300 font-medium px-2 py-2 whitespace-nowrap">
                    {cohort.acquisition_period}
                  </td>
                  <td className="text-right text-gray-400 px-2 py-2 whitespace-nowrap">
                    {fmtSize(cohort.size, metric)}
                  </td>
                  {offsets.map(o => {
                    const m = monthMap[o]
                    if (!m) {
                      return (
                        <td key={o} className="px-1 py-1">
                          <div className="w-full h-8" />
                        </td>
                      )
                    }
                    return (
                      <td key={o} className="px-1 py-1">
                        <div
                          className={`rounded px-2 py-1.5 text-center font-medium text-[11px] ${retentionClass(m.retention_pct)}`}
                          title={`${cohort.acquisition_period} M${o}: ${fmtPct(m.retention_pct)} (${fmtSize(m.value, metric)})`}
                        >
                          {fmtPct(m.retention_pct)}
                        </div>
                      </td>
                    )
                  })}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {/* Legend */}
      <div className="flex items-center gap-4 text-[10px] text-gray-400 px-1">
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded bg-emerald-700/80 inline-block" /> &gt;100%
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded bg-emerald-600/50 inline-block" /> 80-100%
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded bg-yellow-600/40 inline-block" /> 60-80%
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded bg-red-600/50 inline-block" /> &lt;60%
        </span>
      </div>
    </div>
  )
}
