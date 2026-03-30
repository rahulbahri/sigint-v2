import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import { AlertTriangle, CheckCircle2, RefreshCw, XCircle, Info } from 'lucide-react'

function SeverityBadge({ severity }) {
  if (severity === 'critical')
    return <span className="flex items-center gap-1 text-xs font-semibold text-red-400"><XCircle size={12}/> Critical</span>
  return <span className="flex items-center gap-1 text-xs font-semibold text-yellow-400"><AlertTriangle size={12}/> Warning</span>
}

function IssueCard({ issue }) {
  const pct = issue.total > 0 ? Math.round(issue.count / issue.total * 100) : 100
  return (
    <div className={`bg-[#1a1f2e] border rounded-xl p-4 ${issue.severity === 'critical' ? 'border-red-500/30' : 'border-yellow-500/20'}`}>
      <div className="flex items-start justify-between gap-3 mb-2">
        <p className="text-white text-sm font-medium">{issue.description}</p>
        <SeverityBadge severity={issue.severity}/>
      </div>
      <div className="flex items-center gap-2 mb-3">
        <div className="flex-1 h-1.5 bg-white/5 rounded-full overflow-hidden">
          <div
            className={`h-full rounded-full ${issue.severity === 'critical' ? 'bg-red-500' : 'bg-yellow-500'}`}
            style={{ width: `${pct}%` }}
          />
        </div>
        <span className="text-xs text-gray-500 shrink-0">{pct}% affected</span>
      </div>
      <div className="flex items-start gap-2 text-xs text-gray-400 bg-white/3 rounded-lg px-3 py-2">
        <Info size={11} className="text-[#00AEEF] mt-0.5 shrink-0"/>
        <span>{issue.fix}</span>
      </div>
      <p className="text-gray-600 text-xs mt-2">{issue.table.replace('canonical_', '')} table</p>
    </div>
  )
}

export default function DataQualityPage() {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState('')

  const load = useCallback(async () => {
    setLoading(true); setError('')
    try {
      const { data: d } = await axios.get('/api/data-quality')
      setData(d)
    } catch (e) {
      setError(e.response?.data?.detail || 'Failed to load data quality report')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  if (loading) return (
    <div className="flex items-center justify-center h-64">
      <RefreshCw size={20} className="animate-spin text-[#00AEEF]"/>
    </div>
  )

  if (error) return (
    <div className="p-6 max-w-3xl mx-auto">
      <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-4 text-red-400 text-sm">{error}</div>
    </div>
  )

  const critical = data?.issues?.filter(i => i.severity === 'critical') || []
  const warnings = data?.issues?.filter(i => i.severity === 'warning')  || []
  const s = data?.summary || {}

  return (
    <div className="p-6 max-w-3xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-white text-xl font-semibold">Data Quality</h2>
          <p className="text-gray-400 text-sm mt-1">Issues detected in your canonical data that may affect KPI accuracy.</p>
        </div>
        <button onClick={load} className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-gray-400 hover:text-white bg-white/5 hover:bg-white/10 rounded-lg transition-colors">
          <RefreshCw size={12}/> Refresh
        </button>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-3 mb-8">
        {[
          { label: 'Tables Scanned', value: s.tables_scanned ?? 0, color: 'text-blue-400' },
          { label: 'Critical Issues', value: s.critical ?? 0,      color: 'text-red-400'  },
          { label: 'Warnings',        value: s.warning  ?? 0,      color: 'text-yellow-400' },
        ].map(({ label, value, color }) => (
          <div key={label} className="bg-[#1a1f2e] border border-white/8 rounded-xl p-4 text-center">
            <div className={`text-2xl font-bold ${color}`}>{value}</div>
            <div className="text-gray-500 text-xs mt-1">{label}</div>
          </div>
        ))}
      </div>

      {s.total_issues === 0 ? (
        <div className="flex flex-col items-center justify-center py-16 text-center">
          <CheckCircle2 size={40} className="text-green-400 mb-4"/>
          <p className="text-white font-semibold text-lg">All clean</p>
          <p className="text-gray-500 text-sm mt-1">
            {s.tables_scanned > 0
              ? `No data quality issues found across ${s.tables_scanned} canonical tables.`
              : 'No canonical data yet. Connect a data source to begin syncing.'}
          </p>
        </div>
      ) : (
        <div className="space-y-6">
          {critical.length > 0 && (
            <div>
              <h3 className="text-red-400 text-xs font-semibold uppercase tracking-wider mb-3">
                Critical — Affecting KPI accuracy ({critical.length})
              </h3>
              <div className="space-y-3">
                {critical.map((issue, i) => <IssueCard key={i} issue={issue}/>)}
              </div>
            </div>
          )}
          {warnings.length > 0 && (
            <div>
              <h3 className="text-yellow-400 text-xs font-semibold uppercase tracking-wider mb-3">
                Warnings — Review recommended ({warnings.length})
              </h3>
              <div className="space-y-3">
                {warnings.map((issue, i) => <IssueCard key={i} issue={issue}/>)}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
