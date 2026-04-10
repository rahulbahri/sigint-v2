import { useState, useEffect, useMemo, useCallback } from 'react'
import axios from 'axios'
import {
  Users, RefreshCw, AlertTriangle, ChevronDown, ChevronUp,
  CheckCircle2, AlertCircle, XCircle, HelpCircle,
} from 'lucide-react'
import { fmtKpiValue } from './kpiFormat'

// ── Status helpers ─────────────────────────────────────────────────────────
const STATUS_CFG = {
  green:  { color: '#059669', label: 'On Target', Icon: CheckCircle2, textClass: 'text-emerald-400' },
  yellow: { color: '#D97706', label: 'Watch',     Icon: AlertCircle,  textClass: 'text-yellow-400'  },
  red:    { color: '#DC2626', label: 'Critical',  Icon: XCircle,      textClass: 'text-red-400'     },
  grey:   { color: '#6b7280', label: 'No Data',   Icon: HelpCircle,   textClass: 'text-slate-500'    },
}

function statusDot(status) {
  const cfg = STATUS_CFG[status] || STATUS_CFG.grey
  return <span className="inline-block w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: cfg.color }} />
}

// ── Donut ring (SVG) ───────────────────────────────────────────────────────
function StatusRing({ green = 0, yellow = 0, red = 0, grey = 0, size = 72 }) {
  const total = green + yellow + red + grey
  if (total === 0) {
    return (
      <svg width={size} height={size} viewBox="0 0 36 36">
        <circle cx="18" cy="18" r="15.915" fill="none" stroke="#374151" strokeWidth="3" />
      </svg>
    )
  }

  const segments = [
    { count: green,  color: '#059669' },
    { count: yellow, color: '#D97706' },
    { count: red,    color: '#DC2626' },
    { count: grey,   color: '#6b7280' },
  ].filter(s => s.count > 0)

  let offset = 25 // start at 12 o'clock
  const arcs = segments.map(s => {
    const pct  = (s.count / total) * 100
    const arc  = { pct, color: s.color, offset }
    offset += pct
    return arc
  })

  return (
    <svg width={size} height={size} viewBox="0 0 36 36" className="shrink-0">
      {arcs.map((a, i) => (
        <circle
          key={i}
          cx="18" cy="18" r="15.915"
          fill="none"
          stroke={a.color}
          strokeWidth="3"
          strokeDasharray={`${a.pct} ${100 - a.pct}`}
          strokeDashoffset={`${-a.offset}`}
          strokeLinecap="round"
        />
      ))}
    </svg>
  )
}

// ── Owner card ─────────────────────────────────────────────────────────────
function OwnerCard({ owner }) {
  const [expanded, setExpanded] = useState(false)
  const total = owner.total_kpis || 0

  return (
    <div className="bg-white border border-slate-200 rounded-xl p-5 flex flex-col">
      {/* Top row: donut + name + resolution */}
      <div className="flex items-start gap-4 mb-3">
        <StatusRing
          green={owner.green}
          yellow={owner.yellow}
          red={owner.red}
          grey={owner.grey}
          size={64}
        />
        <div className="flex-1 min-w-0">
          <h3 className="text-slate-800 text-sm font-bold truncate">{owner.name}</h3>
          <p className="text-slate-500 text-xs mt-0.5">{total} KPI{total !== 1 ? 's' : ''} assigned</p>

          {/* Status breakdown */}
          <div className="flex flex-wrap gap-x-3 gap-y-0.5 mt-2 text-[10px]">
            {owner.green > 0 && (
              <span className="text-emerald-400 font-semibold">{owner.green} green</span>
            )}
            {owner.yellow > 0 && (
              <span className="text-yellow-400 font-semibold">{owner.yellow} yellow</span>
            )}
            {owner.red > 0 && (
              <span className="text-red-400 font-semibold">{owner.red} red</span>
            )}
            {owner.grey > 0 && (
              <span className="text-slate-500 font-semibold">{owner.grey} no data</span>
            )}
          </div>
        </div>
      </div>

      {/* Resolution rate */}
      <div className="mb-3">
        <div className="flex items-center justify-between text-xs mb-1">
          <span className="text-slate-500">Resolution Rate</span>
          <span className={`font-bold ${
            owner.resolution_rate_pct >= 80 ? 'text-emerald-400'
              : owner.resolution_rate_pct >= 50 ? 'text-yellow-400'
              : 'text-red-400'
          }`}>
            {owner.resolution_rate_pct != null ? `${owner.resolution_rate_pct}%` : '\u2014'}
          </span>
        </div>
        <div className="h-1.5 rounded-full bg-slate-50 overflow-hidden">
          <div
            className={`h-full rounded-full transition-all ${
              owner.resolution_rate_pct >= 80 ? 'bg-emerald-500'
                : owner.resolution_rate_pct >= 50 ? 'bg-yellow-500'
                : 'bg-red-500'
            }`}
            style={{ width: `${Math.min(owner.resolution_rate_pct ?? 0, 100)}%` }}
          />
        </div>
      </div>

      {/* Expandable KPI list */}
      {owner.kpis?.length > 0 && (
        <>
          <button
            onClick={() => setExpanded(e => !e)}
            className="flex items-center gap-1.5 text-xs text-slate-500 hover:text-slate-700 transition-colors mt-auto"
          >
            {expanded ? <ChevronUp size={13} /> : <ChevronDown size={13} />}
            {expanded ? 'Hide KPIs' : `Show ${owner.kpis.length} KPIs`}
          </button>

          {expanded && (
            <div className="mt-3 space-y-1.5 border-t border-slate-100 pt-3">
              {owner.kpis.map((kpi, i) => (
                <div key={i} className="flex items-center gap-2 text-xs">
                  {statusDot(kpi.status)}
                  <span className="text-slate-700 flex-1 min-w-0 truncate">
                    {kpi.name || kpi.key?.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}
                  </span>
                  <span className="text-slate-500 shrink-0">
                    {kpi.value != null ? fmtKpiValue(kpi.value, kpi.unit) : '\u2014'}
                  </span>
                  {kpi.target != null && (
                    <span className="text-slate-500 shrink-0 text-[10px]">
                      / {fmtKpiValue(kpi.target, kpi.unit)}
                    </span>
                  )}
                  {kpi.due_date && (
                    <span className="text-slate-500 shrink-0 text-[10px]">
                      {kpi.due_date}
                    </span>
                  )}
                  {kpi.resolution_status && (
                    <span className={`shrink-0 text-[10px] px-1.5 py-0.5 rounded ${
                      kpi.resolution_status === 'resolved'
                        ? 'bg-emerald-500/10 text-emerald-400'
                        : kpi.resolution_status === 'in_progress'
                        ? 'bg-yellow-500/10 text-yellow-400'
                        : 'bg-slate-50 text-slate-500'
                    }`}>
                      {kpi.resolution_status.replace(/_/g, ' ')}
                    </span>
                  )}
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  )
}

// ── Main component ─────────────────────────────────────────────────────────
export default function AccountabilityDashboard() {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState('')

  const load = useCallback(async () => {
    setLoading(true); setError('')
    try {
      const { data: d } = await axios.get('/api/analytics/accountability-rollup')
      setData(d)
    } catch (e) {
      setError(e.response?.data?.detail || 'Failed to load accountability data')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  // Sort owners by red count descending (most critical first)
  const sortedOwners = useMemo(() => {
    if (!data?.owners?.length) return []
    return [...data.owners].sort((a, b) => (b.red || 0) - (a.red || 0))
  }, [data])

  // Aggregate totals for summary
  const totals = useMemo(() => {
    if (!sortedOwners.length) return null
    return sortedOwners.reduce((acc, o) => ({
      owners:  acc.owners + 1,
      kpis:    acc.kpis + (o.total_kpis || 0),
      green:   acc.green + (o.green || 0),
      yellow:  acc.yellow + (o.yellow || 0),
      red:     acc.red + (o.red || 0),
      grey:    acc.grey + (o.grey || 0),
    }), { owners: 0, kpis: 0, green: 0, yellow: 0, red: 0, grey: 0 })
  }, [sortedOwners])

  // ── Loading state ────────────────────────────────────────────────────────
  if (loading) return (
    <div className="flex items-center justify-center h-64">
      <RefreshCw size={20} className="animate-spin text-[#0055A4]" />
    </div>
  )

  // ── Error state ──────────────────────────────────────────────────────────
  if (error) return (
    <div className="p-6 max-w-5xl mx-auto">
      <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-4 text-red-400 text-sm">{error}</div>
    </div>
  )

  // ── Empty state ──────────────────────────────────────────────────────────
  if (sortedOwners.length === 0) return (
    <div className="p-6 max-w-5xl mx-auto space-y-5">
      <h2 className="text-slate-800 text-xl font-semibold flex items-center gap-2">
        <Users size={20} className="text-[#0055A4]" />
        Accountability Dashboard
      </h2>
      <div className="bg-white border border-slate-200 rounded-2xl p-12 text-center">
        <AlertTriangle size={32} className="text-slate-500 mx-auto mb-3" />
        <p className="text-slate-500 text-sm font-semibold">No accountability data available</p>
        <p className="text-slate-500 text-xs mt-1">Assign KPI owners in the Targets Editor to enable accountability tracking.</p>
      </div>
    </div>
  )

  return (
    <div className="p-6 max-w-5xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-slate-800 text-xl font-semibold flex items-center gap-2">
            <Users size={20} className="text-[#0055A4]" />
            Accountability Dashboard
          </h2>
          <p className="text-slate-500 text-sm mt-1">
            KPI ownership and resolution tracking, sorted by criticality
          </p>
        </div>
        <button
          onClick={load}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-slate-500 hover:text-slate-800 bg-slate-50 hover:bg-slate-100 rounded-lg transition-colors"
        >
          <RefreshCw size={12} /> Refresh
        </button>
      </div>

      {/* Summary bar */}
      {totals && (
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-6 gap-3">
          {[
            { label: 'Owners',     value: totals.owners, color: 'text-blue-400'    },
            { label: 'Total KPIs', value: totals.kpis,   color: 'text-slate-800'       },
            { label: 'Green',      value: totals.green,  color: 'text-emerald-400' },
            { label: 'Yellow',     value: totals.yellow, color: 'text-yellow-400'  },
            { label: 'Red',        value: totals.red,    color: 'text-red-400'     },
            { label: 'No Data',    value: totals.grey,   color: 'text-slate-500'    },
          ].map(({ label, value, color }) => (
            <div key={label} className="bg-white border border-slate-200 rounded-xl p-3 text-center">
              <div className={`text-xl font-bold ${color}`}>{value}</div>
              <div className="text-slate-500 text-[10px] mt-0.5 uppercase tracking-wider">{label}</div>
            </div>
          ))}
        </div>
      )}

      {/* Owner cards grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {sortedOwners.map((owner, i) => (
          <OwnerCard key={owner.name || i} owner={owner} />
        ))}
      </div>
    </div>
  )
}
