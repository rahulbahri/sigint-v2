import { useState, useEffect } from 'react'
import { Users, Database, Upload, Activity, Trash2, RefreshCw, ShieldCheck, Wifi, WifiOff } from 'lucide-react'

export default function AdminPanel() {
  const [stats, setStats]                   = useState(null)
  const [workspaces, setWorkspaces]         = useState([])
  const [connectorHealth, setConnectorHealth] = useState([])
  const [loading, setLoading]               = useState(true)
  const [deleting, setDeleting]             = useState(null)
  const [error, setError]                   = useState('')

  function load() {
    setLoading(true)
    Promise.all([
      fetch('/api/admin/stats').then(r => r.json()),
      fetch('/api/admin/workspaces').then(r => r.json()),
      fetch('/api/admin/connector-health').then(r => r.json()).catch(() => ({ workspaces: [] })),
    ]).then(([s, w, h]) => {
      setStats(s)
      setWorkspaces(w.workspaces || [])
      setConnectorHealth(h.workspaces || [])
      setLoading(false)
    }).catch(() => { setError('Failed to load admin data'); setLoading(false) })
  }

  useEffect(() => { load() }, [])

  function deleteWorkspace(email) {
    if (!confirm(`Delete ALL data for ${email}? This cannot be undone.`)) return
    setDeleting(email)
    fetch(`/api/admin/workspace/${encodeURIComponent(email)}`, { method: 'DELETE' })
      .then(r => r.json())
      .then(() => { setDeleting(null); load() })
      .catch(() => { setDeleting(null); setError('Failed to delete workspace') })
  }

  function fmt(dt) {
    if (!dt || dt === 'None' || dt === '') return '—'
    try { return new Date(dt).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) }
    catch { return dt }
  }

  if (loading) return (
    <div className="flex items-center justify-center h-64">
      <div className="w-6 h-6 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin"/>
    </div>
  )

  return (
    <div className="p-6 space-y-6 max-w-5xl mx-auto">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <ShieldCheck size={20} className="text-indigo-400"/>
          <div>
            <h2 className="text-[15px] font-semibold text-slate-900">Admin Panel</h2>
            <p className="text-[12px] text-slate-500">Platform overview and workspace management</p>
          </div>
        </div>
        <button onClick={load} className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-[12px] text-slate-500 hover:text-slate-700 border border-slate-200 hover:border-slate-300 transition-all">
          <RefreshCw size={11}/> Refresh
        </button>
      </div>

      {error && <div className="bg-red-50 border border-red-200 rounded-xl p-3 text-[12px] text-red-600">{error}</div>}

      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {[
            { label: 'Total Users',        value: stats.total_users,                    icon: Users,    color: 'indigo'  },
            { label: 'Active Workspaces',  value: stats.total_workspaces,               icon: Database, color: 'violet'  },
            { label: 'Total Uploads',      value: stats.total_uploads,                  icon: Upload,   color: 'blue'    },
            { label: 'Data Points',        value: stats.total_data_points?.toLocaleString(), icon: Activity, color: 'emerald' },
          ].map(({ label, value, icon: Icon, color }) => (
            <div key={label} className="bg-white rounded-2xl border border-slate-200 p-4">
              <div className={`w-8 h-8 rounded-lg bg-${color}-50 flex items-center justify-center mb-3`}>
                <Icon size={14} className={`text-${color}-500`}/>
              </div>
              <div className="text-[22px] font-bold text-slate-900">{value ?? '—'}</div>
              <div className="text-[11px] text-slate-500 mt-0.5">{label}</div>
            </div>
          ))}
        </div>
      )}

      {stats?.recent_logins?.length > 0 && (
        <div className="bg-white rounded-2xl border border-slate-200 p-4">
          <h3 className="text-[12px] font-semibold text-slate-700 mb-3">Recent Sign-ins</h3>
          <div className="space-y-2">
            {stats.recent_logins.map(l => (
              <div key={l.email} className="flex items-center justify-between text-[12px]">
                <span className="text-slate-600">{l.email}</span>
                <span className="text-slate-400">{fmt(l.last_login)}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden">
        <div className="px-4 py-3 border-b border-slate-100">
          <h3 className="text-[12px] font-semibold text-slate-700">Workspaces ({workspaces.length})</h3>
        </div>
        {workspaces.length === 0 ? (
          <div className="p-8 text-center text-[12px] text-slate-400">No workspaces yet</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-slate-100 bg-slate-50/50">
                  {['Email','Uploads','Data Points','Last Upload','Last Login',''].map(h => (
                    <th key={h} className="text-left px-4 py-2.5 text-[11px] font-semibold text-slate-500">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {workspaces.map((ws, i) => (
                  <tr key={ws.email} className={`border-b border-slate-50 ${i % 2 === 0 ? '' : 'bg-slate-50/30'}`}>
                    <td className="px-4 py-2.5">
                      <div className="flex items-center gap-2">
                        <div className="w-6 h-6 rounded-full bg-indigo-100 flex items-center justify-center text-[10px] font-semibold text-indigo-600">
                          {ws.email[0].toUpperCase()}
                        </div>
                        <span className="text-[12px] text-slate-700">{ws.email}</span>
                        {ws.role === 'admin' && <span className="text-[10px] bg-indigo-100 text-indigo-600 px-1.5 py-0.5 rounded">admin</span>}
                      </div>
                    </td>
                    <td className="px-4 py-2.5 text-[12px] text-slate-600">{ws.uploads}</td>
                    <td className="px-4 py-2.5 text-[12px] text-slate-600">{ws.data_points?.toLocaleString()}</td>
                    <td className="px-4 py-2.5 text-[12px] text-slate-500">{fmt(ws.last_upload)}</td>
                    <td className="px-4 py-2.5 text-[12px] text-slate-500">{fmt(ws.last_login)}</td>
                    <td className="px-4 py-2.5">
                      {ws.role !== 'admin' && (
                        <button onClick={() => deleteWorkspace(ws.email)} disabled={deleting === ws.email}
                          className="p-1.5 rounded-lg text-slate-400 hover:text-red-500 hover:bg-red-50 transition-colors disabled:opacity-40"
                          title="Delete workspace">
                          {deleting === ws.email
                            ? <div className="w-3 h-3 border border-red-400 border-t-transparent rounded-full animate-spin"/>
                            : <Trash2 size={12}/>}
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
      {/* Connector Health */}
      {connectorHealth.length > 0 && (
        <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-100">
            <h3 className="text-[12px] font-semibold text-slate-700">Connector Health</h3>
          </div>
          <div className="divide-y divide-slate-50">
            {connectorHealth.map(ws => (
              <div key={ws.workspace_id} className="px-4 py-3">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-[12px] font-medium text-slate-700">{ws.workspace_id}</span>
                  <span className="text-[11px] text-slate-400">{ws.healthy}/{ws.total} healthy</span>
                </div>
                <div className="flex flex-wrap gap-2">
                  {ws.connectors.map(c => (
                    <span key={c.source}
                      title={c.last_error || c.last_sync_at || ''}
                      className={`flex items-center gap-1 text-[11px] px-2 py-0.5 rounded-full border
                        ${c.status === 'ok'
                          ? 'bg-green-50 text-green-600 border-green-200'
                          : c.status === 'error'
                          ? 'bg-red-50 text-red-600 border-red-200'
                          : 'bg-slate-50 text-slate-500 border-slate-200'}`}
                    >
                      {c.status === 'ok' ? <Wifi size={9}/> : <WifiOff size={9}/>}
                      {c.source}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
