import { useState, useEffect } from 'react'
import axios from 'axios'
import {
  AlertTriangle, CheckCircle2, Shield, RefreshCw,
  Users, TrendingUp, TrendingDown, Minus, BarChart3
} from 'lucide-react'
import { fmtKpiValueCompact } from './kpiFormat'

// ─── Color helpers ──────────────────────────────────────────────────────────
function hhiColor(hhi) {
  if (hhi == null) return 'text-gray-400'
  if (hhi < 1500) return 'text-green-400'
  if (hhi < 2500) return 'text-yellow-400'
  return 'text-red-400'
}

function hhiLabel(hhi) {
  if (hhi == null) return 'N/A'
  if (hhi < 1500) return 'Low'
  if (hhi < 2500) return 'Moderate'
  return 'High'
}

function pctBarColor(pct) {
  if (pct >= 20) return 'bg-red-500'
  if (pct >= 10) return 'bg-orange-500'
  if (pct >= 5)  return 'bg-yellow-500'
  return 'bg-green-500'
}

// ─── Summary Card ───────────────────────────────────────────────────────────
function SummaryCard({ label, value, suffix, color, subLabel }) {
  return (
    <div className="bg-[#1a1f2e] border border-white/8 rounded-xl px-4 py-3 text-center">
      <p className="text-[10px] text-gray-400 uppercase tracking-wider mb-1">{label}</p>
      <p className={`text-xl font-bold ${color}`}>
        {value ?? '\u2014'}{suffix || ''}
      </p>
      {subLabel && <p className="text-[10px] text-gray-500 mt-0.5">{subLabel}</p>}
    </div>
  )
}

// ─── SEC Threshold Badge ────────────────────────────────────────────────────
function SecBadge({ breached }) {
  if (breached) {
    return (
      <div className="bg-[#1a1f2e] border border-red-500/30 rounded-xl px-4 py-3 text-center">
        <p className="text-[10px] text-gray-400 uppercase tracking-wider mb-1">SEC Threshold</p>
        <div className="flex items-center justify-center gap-1.5">
          <AlertTriangle size={14} className="text-red-400" />
          <p className="text-xl font-bold text-red-400">Breached</p>
        </div>
        <p className="text-[10px] text-red-400/70 mt-0.5">Disclosure required</p>
      </div>
    )
  }
  return (
    <div className="bg-[#1a1f2e] border border-green-500/20 rounded-xl px-4 py-3 text-center">
      <p className="text-[10px] text-gray-400 uppercase tracking-wider mb-1">SEC Threshold</p>
      <div className="flex items-center justify-center gap-1.5">
        <CheckCircle2 size={14} className="text-green-400" />
        <p className="text-xl font-bold text-green-400">Clear</p>
      </div>
      <p className="text-[10px] text-green-400/70 mt-0.5">Within safe limits</p>
    </div>
  )
}

// ─── Main Component ─────────────────────────────────────────────────────────
export default function ConcentrationTable() {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState('')

  useEffect(() => {
    setLoading(true)
    setError('')
    axios.get('/api/analytics/customer-concentration?top_n=20')
      .then(r => setData(r.data))
      .catch(e => setError(e.response?.data?.detail || 'Failed to load concentration data'))
      .finally(() => setLoading(false))
  }, [])

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
  if (!data?.customers?.length) {
    return (
      <div className="p-6 max-w-4xl mx-auto space-y-5">
        <h2 className="text-white text-xl font-semibold flex items-center gap-2">
          <Shield size={18} className="text-[#0055A4]" />
          Customer Concentration
        </h2>
        <div className="bg-[#1a1f2e] border border-white/8 rounded-2xl p-10 text-center">
          <Users size={28} className="text-gray-600 mx-auto mb-2" />
          <p className="text-white text-sm font-semibold">No customer data available</p>
          <p className="text-gray-500 text-xs mt-1">
            Upload data with customer IDs and revenue to enable concentration analysis.
          </p>
        </div>
      </div>
    )
  }

  const {
    period, total_revenue, customers,
    hhi_index, top_1_pct, top_5_pct, top_10_pct, top_20_pct,
    sec_threshold_breached
  } = data

  return (
    <div className="p-6 max-w-5xl mx-auto space-y-5">
      {/* Header */}
      <div>
        <h2 className="text-white text-xl font-semibold flex items-center gap-2">
          <Shield size={18} className="text-[#0055A4]" />
          Customer Concentration
        </h2>
        <p className="text-gray-400 text-sm mt-1">
          Revenue concentration risk analysis.
          {period && <span className="text-gray-500"> Period: {period}</span>}
          {total_revenue != null && (
            <span className="text-gray-500"> &middot; Total Revenue: {fmtKpiValueCompact(total_revenue, 'usd')}</span>
          )}
        </p>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        <SummaryCard
          label="HHI Index"
          value={hhi_index != null ? Math.round(hhi_index) : null}
          color={hhiColor(hhi_index)}
          subLabel={hhiLabel(hhi_index)}
        />
        <SummaryCard
          label="Top 1%"
          value={top_1_pct != null ? top_1_pct.toFixed(1) : null}
          suffix="%"
          color={top_1_pct > 30 ? 'text-red-400' : top_1_pct > 15 ? 'text-yellow-400' : 'text-green-400'}
        />
        <SummaryCard
          label="Top 5%"
          value={top_5_pct != null ? top_5_pct.toFixed(1) : null}
          suffix="%"
          color={top_5_pct > 60 ? 'text-red-400' : top_5_pct > 40 ? 'text-yellow-400' : 'text-green-400'}
        />
        <SummaryCard
          label="Top 10%"
          value={top_10_pct != null ? top_10_pct.toFixed(1) : null}
          suffix="%"
          color={top_10_pct > 80 ? 'text-red-400' : top_10_pct > 60 ? 'text-yellow-400' : 'text-green-400'}
        />
        <SecBadge breached={sec_threshold_breached} />
      </div>

      {/* Concentration risk warning */}
      {sec_threshold_breached && (
        <div className="bg-red-500/10 border border-red-500/20 rounded-xl px-4 py-3 flex items-start gap-2">
          <AlertTriangle size={14} className="text-red-400 mt-0.5 shrink-0" />
          <div>
            <p className="text-sm font-semibold text-red-400">SEC Disclosure Threshold Breached</p>
            <p className="text-xs text-red-400/70 leading-relaxed mt-0.5">
              One or more customers exceed 10% of total revenue. SEC regulations may require
              disclosure of material customer concentration in financial filings.
            </p>
          </div>
        </div>
      )}

      {/* Customer table */}
      <div className="bg-[#1a1f2e] border border-white/8 rounded-2xl overflow-hidden">
        <div className="px-5 py-3 border-b border-white/5">
          <h3 className="text-[11px] font-bold text-gray-400 uppercase tracking-wider">
            Top {customers.length} Customers by Revenue
          </h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-white/5">
                <th className="px-4 py-2.5 text-left text-[10px] font-semibold text-gray-500 uppercase tracking-wider w-12">
                  Rank
                </th>
                <th className="px-4 py-2.5 text-left text-[10px] font-semibold text-gray-500 uppercase tracking-wider">
                  Customer
                </th>
                <th className="px-4 py-2.5 text-right text-[10px] font-semibold text-gray-500 uppercase tracking-wider">
                  Revenue
                </th>
                <th className="px-4 py-2.5 text-left text-[10px] font-semibold text-gray-500 uppercase tracking-wider w-52">
                  % of Total
                </th>
                <th className="px-4 py-2.5 text-center text-[10px] font-semibold text-gray-500 uppercase tracking-wider w-20">
                  Trend
                </th>
              </tr>
            </thead>
            <tbody>
              {customers.map((c, i) => {
                const isHighConcentration = (c.pct_of_total ?? 0) > 10
                return (
                  <tr
                    key={c.customer_id ?? i}
                    className={`border-b border-white/3 transition-colors ${
                      isHighConcentration
                        ? 'bg-red-500/8 hover:bg-red-500/12'
                        : 'hover:bg-white/3'
                    }`}
                  >
                    {/* Rank */}
                    <td className="px-4 py-2.5">
                      <span className={`inline-flex items-center justify-center w-6 h-6 rounded-full text-[10px] font-bold ${
                        c.rank <= 3
                          ? 'bg-[#0055A4]/20 text-[#0055A4]'
                          : 'bg-white/5 text-gray-500'
                      }`}>
                        {c.rank}
                      </span>
                    </td>

                    {/* Customer name */}
                    <td className="px-4 py-2.5">
                      <div className="flex items-center gap-2">
                        <span className="text-white font-medium truncate max-w-[200px]">
                          {c.customer_name || c.customer_id}
                        </span>
                        {isHighConcentration && (
                          <span className="flex items-center gap-0.5 text-[9px] font-semibold text-red-400 bg-red-400/10 px-1.5 py-0.5 rounded-full shrink-0">
                            <AlertTriangle size={9} /> &gt;10%
                          </span>
                        )}
                      </div>
                    </td>

                    {/* Revenue */}
                    <td className="px-4 py-2.5 text-right text-gray-300 font-medium">
                      {fmtKpiValueCompact(c.revenue, 'usd')}
                    </td>

                    {/* Percent of total with bar */}
                    <td className="px-4 py-2.5">
                      <div className="flex items-center gap-2">
                        <div className="flex-1 h-1.5 bg-white/5 rounded-full overflow-hidden">
                          <div
                            className={`h-full rounded-full ${pctBarColor(c.pct_of_total ?? 0)}`}
                            style={{ width: `${Math.min((c.pct_of_total ?? 0) * 2, 100)}%` }}
                          />
                        </div>
                        <span className={`text-xs font-medium shrink-0 w-12 text-right ${
                          isHighConcentration ? 'text-red-400' : 'text-gray-400'
                        }`}>
                          {(c.pct_of_total ?? 0).toFixed(1)}%
                        </span>
                      </div>
                    </td>

                    {/* Trend indicator */}
                    <td className="px-4 py-2.5 text-center">
                      {c.trend === 'up' && <TrendingUp size={14} className="text-green-400 mx-auto" />}
                      {c.trend === 'down' && <TrendingDown size={14} className="text-red-400 mx-auto" />}
                      {(!c.trend || c.trend === 'flat') && <Minus size={14} className="text-gray-500 mx-auto" />}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* HHI explanation footer */}
      <div className="bg-[#1a1f2e] border border-white/8 rounded-xl px-5 py-3">
        <div className="flex items-start gap-2">
          <BarChart3 size={13} className="text-gray-500 mt-0.5 shrink-0" />
          <div className="text-[10px] text-gray-500 leading-relaxed">
            <span className="font-semibold text-gray-400">HHI (Herfindahl-Hirschman Index)</span>
            {' '}measures market concentration. Below 1,500 = low concentration,
            1,500-2,500 = moderate, above 2,500 = high.
            Rows highlighted in red indicate customers exceeding the 10% SEC disclosure threshold.
          </div>
        </div>
      </div>
    </div>
  )
}
