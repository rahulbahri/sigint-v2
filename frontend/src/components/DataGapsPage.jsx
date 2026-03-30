import { useState, useEffect } from 'react'
import axios from 'axios'
import { AlertTriangle, CheckCircle2, XCircle, RefreshCw, ChevronDown, ChevronUp } from 'lucide-react'

const SEVERITY_CONFIG = {
  critical: { color: 'text-red-400',    bg: 'bg-red-400/10',    border: 'border-red-400/20',    icon: XCircle,        label: 'Blocked' },
  warning:  { color: 'text-yellow-400', bg: 'bg-yellow-400/10', border: 'border-yellow-400/20', icon: AlertTriangle,  label: 'Partial' },
}

const KPI_STATUS_CONFIG = {
  ready:   { color: 'text-green-400',  bg: 'bg-green-400/10',  icon: CheckCircle2,   label: 'Ready' },
  partial: { color: 'text-yellow-400', bg: 'bg-yellow-400/10', icon: AlertTriangle,  label: 'Partial' },
  blocked: { color: 'text-red-400',    bg: 'bg-red-400/10',    icon: XCircle,        label: 'Blocked' },
}

function KpiStatusGrid({ kpiStatus }) {
  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
      {Object.entries(kpiStatus).map(([kpi, status]) => {
        const cfg = KPI_STATUS_CONFIG[status] || KPI_STATUS_CONFIG.blocked
        const Icon = cfg.icon
        return (
          <div key={kpi}
            className={`flex items-center gap-2 px-3 py-2 rounded-lg border ${cfg.bg} border-white/8`}>
            <Icon size={13} className={cfg.color}/>
            <span className="text-white text-xs truncate">{kpi}</span>
          </div>
        )
      })}
    </div>
  )
}

function GapCard({ gap }) {
  const [open, setOpen] = useState(false)
  const cfg  = SEVERITY_CONFIG[gap.severity] || SEVERITY_CONFIG.warning
  const Icon = cfg.icon
  const pct  = gap.pct_missing ?? 100

  return (
    <div className={`rounded-xl border ${cfg.border} ${cfg.bg} overflow-hidden`}>
      <button
        className="w-full flex items-start gap-4 px-5 py-4 text-left"
        onClick={() => setOpen(o => !o)}
      >
        <Icon size={18} className={`${cfg.color} mt-0.5 shrink-0`}/>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-white font-medium text-sm">
              {gap.canonical_table.replace('canonical_', '')} → {gap.canonical_field}
            </span>
            <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${cfg.bg} ${cfg.color} border ${cfg.border}`}>
              {cfg.label}
            </span>
          </div>
          <p className="text-gray-400 text-xs mt-1">
            Blocking: {gap.blocking_kpis.join(' · ')}
          </p>
          {gap.total_records > 0 && (
            <div className="flex items-center gap-2 mt-2">
              <div className="flex-1 h-1 bg-white/10 rounded-full overflow-hidden max-w-[120px]">
                <div
                  className={`h-full rounded-full ${gap.severity === 'critical' ? 'bg-red-400' : 'bg-yellow-400'}`}
                  style={{ width: `${pct}%` }}
                />
              </div>
              <span className="text-gray-500 text-xs">{pct}% missing</span>
            </div>
          )}
        </div>
        {open ? <ChevronUp size={14} className="text-gray-500 shrink-0 mt-1"/> : <ChevronDown size={14} className="text-gray-500 shrink-0 mt-1"/>}
      </button>

      {open && (
        <div className="px-5 pb-4 pt-0 border-t border-white/5">
          <p className="text-gray-300 text-sm mt-3 mb-3">
            <span className="text-gray-500">How to fix: </span>
            {gap.suggested_fix}
          </p>
          {gap.total_records > 0 && (
            <p className="text-gray-500 text-xs">
              {gap.missing_records.toLocaleString()} of {gap.total_records.toLocaleString()} records are missing this field.
            </p>
          )}
          {gap.total_records === 0 && (
            <p className="text-gray-500 text-xs">No data found for this entity — connect the relevant source to proceed.</p>
          )}
        </div>
      )}
    </div>
  )
}

export default function DataGapsPage() {
  const [report, setReport]   = useState(null)
  const [loading, setLoading] = useState(true)
  const [tab, setTab]         = useState('gaps')  // 'gaps' | 'kpis'

  async function load() {
    setLoading(true)
    try {
      const { data } = await axios.get('/api/data-gaps')
      setReport(data)
    } catch {
      // silent
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  if (loading) return (
    <div className="flex items-center justify-center h-64">
      <RefreshCw size={20} className="animate-spin text-[#00AEEF]"/>
    </div>
  )

  if (!report) return (
    <div className="p-6 text-center text-gray-500">Could not load data gap report.</div>
  )

  const criticalGaps = report.gaps.filter(g => g.severity === 'critical')
  const warningGaps  = report.gaps.filter(g => g.severity === 'warning')

  return (
    <div className="p-6 max-w-4xl mx-auto">
      {/* Header */}
      <div className="flex items-start justify-between mb-6">
        <div>
          <h2 className="text-white text-xl font-semibold">Data Gaps</h2>
          <p className="text-gray-400 text-sm mt-1">
            What's missing and which KPIs it blocks.
          </p>
        </div>
        <button
          onClick={load}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-gray-400
            bg-white/5 hover:bg-white/10 rounded-lg transition-colors"
        >
          <RefreshCw size={12}/> Refresh
        </button>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-3 mb-6">
        {[
          { label: 'KPIs Ready',   value: report.ready_count,   color: 'text-green-400',  bg: 'bg-green-400/10' },
          { label: 'KPIs Partial', value: report.partial_count, color: 'text-yellow-400', bg: 'bg-yellow-400/10' },
          { label: 'KPIs Blocked', value: report.blocked_count, color: 'text-red-400',    bg: 'bg-red-400/10' },
        ].map(s => (
          <div key={s.label} className={`${s.bg} border border-white/8 rounded-xl p-4 text-center`}>
            <p className={`text-2xl font-bold ${s.color}`}>{s.value}</p>
            <p className="text-gray-400 text-xs mt-1">{s.label}</p>
          </div>
        ))}
      </div>

      {/* Tabs */}
      <div className="flex gap-1 mb-5 bg-white/5 rounded-lg p-1 w-fit">
        {[['gaps', 'Data Gaps'], ['kpis', 'KPI Status']].map(([id, label]) => (
          <button
            key={id}
            onClick={() => setTab(id)}
            className={`px-4 py-1.5 text-sm rounded-md transition-all ${
              tab === id ? 'bg-[#00AEEF] text-white font-medium' : 'text-gray-400 hover:text-white'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === 'kpis' && (
        <KpiStatusGrid kpiStatus={report.kpi_status || {}}/>
      )}

      {tab === 'gaps' && (
        <div className="space-y-3">
          {report.gaps.length === 0 && (
            <div className="text-center py-16 text-gray-500">
              <CheckCircle2 size={40} className="text-green-400 mx-auto mb-3"/>
              <p className="text-white font-medium">No data gaps detected</p>
              <p className="text-sm mt-1">All KPI dependencies are satisfied.</p>
            </div>
          )}

          {criticalGaps.length > 0 && (
            <>
              <p className="text-gray-500 text-xs font-semibold uppercase tracking-wider">
                Critical — fully blocked ({criticalGaps.length})
              </p>
              {criticalGaps.map((g, i) => <GapCard key={i} gap={g}/>)}
            </>
          )}

          {warningGaps.length > 0 && (
            <>
              <p className="text-gray-500 text-xs font-semibold uppercase tracking-wider mt-4">
                Partial — some data missing ({warningGaps.length})
              </p>
              {warningGaps.map((g, i) => <GapCard key={i} gap={g}/>)}
            </>
          )}
        </div>
      )}
    </div>
  )
}
