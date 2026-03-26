import { useState, useEffect } from 'react'
import axios from 'axios'
import { Clock, Upload, Target, User, RefreshCw } from 'lucide-react'

const EVENT_ICONS = {
  data_upload: Upload,
  target_changed: Target,
  accountability_update: User,
  default: Clock,
}

const EVENT_COLORS = {
  data_upload: 'text-blue-500 bg-blue-50',
  target_changed: 'text-amber-500 bg-amber-50',
  accountability_update: 'text-violet-500 bg-violet-50',
  default: 'text-slate-400 bg-slate-100',
}

export default function AuditLog() {
  const [events, setEvents] = useState([])
  const [loading, setLoading] = useState(true)
  const [filter, setFilter] = useState('all')

  const load = () => {
    setLoading(true)
    const url = filter === 'all' ? '/api/audit-log?limit=200' : `/api/audit-log?limit=200&event_type=${filter}`
    axios.get(url)
      .then(r => setEvents(r.data))
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, [filter])

  const fmtDate = (ts) => {
    if (!ts) return ''
    return new Date(ts + 'Z').toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-[15px] font-bold text-slate-800">Audit Trail</h2>
          <p className="text-[12px] text-slate-500 mt-0.5">Every change, upload, and accountability update — timestamped and traceable.</p>
        </div>
        <button onClick={load} className="flex items-center gap-1.5 px-3 py-1.5 text-[11px] text-slate-500 border border-slate-200 rounded-lg hover:bg-slate-50">
          <RefreshCw size={12}/> Refresh
        </button>
      </div>

      <div className="flex gap-2">
        {['all','data_upload','target_changed','accountability_update'].map(f => (
          <button key={f} onClick={() => setFilter(f)}
            className={`px-3 py-1 text-[10px] font-medium rounded-full border transition-colors ${
              filter === f ? 'bg-[#0055A4] text-white border-[#0055A4]' : 'text-slate-500 border-slate-200 hover:border-slate-300'
            }`}>
            {f === 'all' ? 'All' : f.replace(/_/g,' ').replace(/\b\w/g, c => c.toUpperCase())}
          </button>
        ))}
      </div>

      <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden">
        {loading && <div className="px-4 py-8 text-center text-[12px] text-slate-400">Loading...</div>}
        {!loading && events.length === 0 && (
          <div className="px-4 py-8 text-center text-[12px] text-slate-400">No audit events yet. Actions will appear here as you use the platform.</div>
        )}
        {!loading && events.map((e, i) => {
          const Icon = EVENT_ICONS[e.event_type] || EVENT_ICONS.default
          const colorClass = EVENT_COLORS[e.event_type] || EVENT_COLORS.default
          return (
            <div key={e.id} className={`flex items-start gap-3 px-4 py-3 ${i % 2 === 0 ? 'bg-white' : 'bg-slate-50/30'} border-b border-slate-50 last:border-0`}>
              <div className={`shrink-0 w-7 h-7 rounded-lg flex items-center justify-center mt-0.5 ${colorClass}`}>
                <Icon size={13}/>
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-[12px] text-slate-700">{e.description}</p>
                <p className="text-[10px] text-slate-400 mt-0.5">{e.user} · {fmtDate(e.created_at)}</p>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
