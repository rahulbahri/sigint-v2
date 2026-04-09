import { useState, useEffect } from 'react'
import axios from 'axios'
import {
  Shield, CheckCircle2, AlertTriangle, XCircle, Database,
  Brain, Network, Zap, ChevronDown, ArrowRight, Wrench,
  RefreshCw, Clock, Target, HelpCircle,
} from 'lucide-react'

const STATUS_CONFIG = {
  pass:    { Icon: CheckCircle2, color: 'text-emerald-500', bg: 'bg-emerald-50', border: 'border-emerald-200' },
  healthy: { Icon: CheckCircle2, color: 'text-emerald-500', bg: 'bg-emerald-50', border: 'border-emerald-200' },
  warn:    { Icon: AlertTriangle, color: 'text-amber-500', bg: 'bg-amber-50', border: 'border-amber-200' },
  degraded:{ Icon: AlertTriangle, color: 'text-amber-500', bg: 'bg-amber-50', border: 'border-amber-200' },
  fail:    { Icon: XCircle, color: 'text-red-500', bg: 'bg-red-50', border: 'border-red-200' },
  critical:{ Icon: XCircle, color: 'text-red-500', bg: 'bg-red-50', border: 'border-red-200' },
  skip:    { Icon: Clock, color: 'text-slate-400', bg: 'bg-slate-50', border: 'border-slate-200' },
  unknown: { Icon: HelpCircle, color: 'text-slate-400', bg: 'bg-slate-50', border: 'border-slate-200' },
}

const SUBSYSTEM_ICONS = {
  data_pipeline: Database,
  computation:   Target,
  intelligence:  Brain,
  agents:        Zap,
  connectors:    Network,
}

const SUBSYSTEM_LABELS = {
  data_pipeline: 'Data Pipeline',
  computation:   'Computation Engine',
  intelligence:  'Intelligence Layer',
  agents:        'Autonomous Agents',
  connectors:    'Data Connectors',
}

function StatusBadge({ status }) {
  const cfg = STATUS_CONFIG[status] || STATUS_CONFIG.unknown
  return (
    <span className={`inline-flex items-center gap-1 text-[10px] font-bold px-2 py-0.5 rounded-full border ${cfg.bg} ${cfg.border} ${cfg.color}`}>
      <cfg.Icon size={10} /> {status}
    </span>
  )
}

function SubsystemCard({ name, data, expanded, onToggle }) {
  const Icon = SUBSYSTEM_ICONS[name] || Shield
  const label = SUBSYSTEM_LABELS[name] || name
  const cfg = STATUS_CONFIG[data.status] || STATUS_CONFIG.unknown

  return (
    <div className={`rounded-xl border shadow-sm overflow-hidden ${cfg.border}`}>
      <button onClick={onToggle}
        className={`w-full flex items-center justify-between px-4 py-3 ${cfg.bg} hover:brightness-95 transition-all`}>
        <div className="flex items-center gap-2.5">
          <Icon size={15} className={cfg.color} />
          <span className="text-[12px] font-bold text-slate-700">{label}</span>
          <StatusBadge status={data.status} />
        </div>
        <div className="flex items-center gap-2">
          <span className="text-lg font-bold text-slate-800">{data.score}</span>
          <ChevronDown size={12} className={`text-slate-400 transition-transform ${expanded ? 'rotate-180' : ''}`} />
        </div>
      </button>
      {expanded && (
        <div className="px-4 py-3 bg-white space-y-1.5">
          {(data.checks || []).map((check, i) => {
            const cc = STATUS_CONFIG[check.status] || STATUS_CONFIG.unknown
            return (
              <div key={i} className="flex items-center gap-2 py-1">
                <cc.Icon size={11} className={cc.color} />
                <span className="text-[11px] font-semibold text-slate-600 w-36">{check.name}</span>
                <span className="text-[10px] text-slate-500 flex-1">{check.detail}</span>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

export default function PlatformHealth() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [expanded, setExpanded] = useState({})

  useEffect(() => { load() }, []) // eslint-disable-line

  async function load() {
    setLoading(true)
    try {
      const r = await axios.get('/api/platform-health')
      setData(r.data)
    } catch { }
    finally { setLoading(false) }
  }

  if (loading) return <div className="flex items-center justify-center h-40 text-slate-400">Loading platform health...</div>
  if (!data) return <div className="text-center py-10 text-slate-400">Could not load health data</div>

  const scoreColor = data.platform_score >= 70 ? 'text-emerald-600' : data.platform_score >= 40 ? 'text-amber-600' : 'text-red-600'
  const scoreBg = data.platform_score >= 70 ? 'bg-emerald-50 border-emerald-200' : data.platform_score >= 40 ? 'bg-amber-50 border-amber-200' : 'bg-red-50 border-red-200'

  return (
    <div className="max-w-6xl mx-auto px-6 py-6 space-y-5">
      {/* Header + Score */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-xl font-bold text-slate-900 flex items-center gap-2">
            <Shield size={20} className="text-[#0055A4]" />
            Platform Health Command Center
          </h1>
          <p className="text-[12px] text-slate-500 mt-0.5">
            End-to-end health of the data pipeline, computation engine, and intelligence layer.
          </p>
        </div>
        <button onClick={load} className="flex items-center gap-1.5 text-[11px] font-semibold px-3 py-1.5 bg-white border border-slate-200 rounded-lg hover:bg-slate-50">
          <RefreshCw size={11} className={loading ? 'animate-spin' : ''} /> Refresh
        </button>
      </div>

      {/* Platform Score Banner */}
      <div className={`rounded-2xl border p-5 ${scoreBg} flex items-center justify-between`}>
        <div>
          <p className="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-1">Platform Health Score</p>
          <div className="flex items-end gap-2">
            <span className={`text-4xl font-black ${scoreColor}`}>{data.platform_score}</span>
            <span className="text-lg text-slate-400 mb-1">/100</span>
          </div>
          <p className="text-[12px] text-slate-600 mt-1 capitalize">{data.platform_status}</p>
        </div>
        <div className="text-right">
          <p className="text-[10px] text-slate-400">
            {Object.values(data.subsystems || {}).filter(s => s.status === 'healthy').length}/{Object.keys(data.subsystems || {}).length} subsystems healthy
          </p>
          {data.self_healing?.corrections_succeeded > 0 && (
            <p className="text-[10px] text-emerald-600 font-medium mt-0.5">
              <Wrench size={9} className="inline mr-0.5" />
              {data.self_healing.corrections_succeeded} auto-corrections applied
            </p>
          )}
        </div>
      </div>

      {/* Subsystem Cards */}
      <div className="space-y-2">
        {Object.entries(data.subsystems || {}).map(([name, sub]) => (
          <SubsystemCard key={name} name={name} data={sub}
            expanded={expanded[name]}
            onToggle={() => setExpanded(prev => ({ ...prev, [name]: !prev[name] }))} />
        ))}
      </div>

      {/* User Action Items */}
      {data.user_actions?.length > 0 && (
        <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
          <h2 className="text-[11px] font-bold text-slate-600 uppercase tracking-wider mb-3 flex items-center gap-1.5">
            <Target size={12} className="text-[#0055A4]" /> Action Items
          </h2>
          <div className="space-y-2">
            {data.user_actions.map((item, i) => {
              const priorityColor = item.priority === 'high' ? 'text-red-600 bg-red-50 border-red-200'
                : item.priority === 'medium' ? 'text-amber-600 bg-amber-50 border-amber-200'
                : 'text-blue-600 bg-blue-50 border-blue-200'
              return (
                <div key={i} className={`flex items-center gap-3 px-3 py-2.5 rounded-lg border ${priorityColor.split(' ').slice(1).join(' ')}`}>
                  <span className={`text-[9px] font-bold uppercase px-1.5 py-0.5 rounded ${priorityColor}`}>{item.priority}</span>
                  <div className="flex-1 min-w-0">
                    <span className="text-[11px] font-semibold text-slate-700">{item.action}</span>
                    <p className="text-[10px] text-slate-500">{item.detail}</p>
                  </div>
                  {item.link && (
                    <span className="text-[10px] text-[#0055A4] font-semibold flex items-center gap-0.5 shrink-0">
                      Go <ArrowRight size={9} />
                    </span>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Self-Healing Log */}
      {data.self_healing?.recent?.length > 0 && (
        <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
          <h2 className="text-[11px] font-bold text-slate-600 uppercase tracking-wider mb-3 flex items-center gap-1.5">
            <Wrench size={12} className="text-emerald-500" /> Self-Healing Log
          </h2>
          <div className="space-y-1.5">
            {data.self_healing.recent.map((entry, i) => (
              <div key={i} className="flex items-center gap-3 text-[10px] py-1.5 border-b border-slate-50 last:border-0">
                {entry.success
                  ? <CheckCircle2 size={11} className="text-emerald-500 shrink-0" />
                  : <XCircle size={11} className="text-red-500 shrink-0" />}
                <span className="text-slate-600 font-medium">{entry.what}</span>
                <span className="text-slate-400">({entry.trigger})</span>
                <span className="text-slate-300 ml-auto">{entry.when?.slice(0, 16)}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Triage Guide */}
      <div className="bg-slate-50 rounded-2xl border border-slate-200 p-5">
        <h2 className="text-[11px] font-bold text-slate-600 uppercase tracking-wider mb-3 flex items-center gap-1.5">
          <HelpCircle size={12} className="text-slate-400" /> Troubleshooting Guide
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {Object.entries(data.triage_guide || {}).map(([key, value]) => (
            <div key={key} className="bg-white rounded-lg px-3 py-2.5 border border-slate-100">
              <p className="text-[10px] font-bold text-slate-600 mb-1">
                {key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}
              </p>
              <p className="text-[10px] text-slate-500 leading-relaxed">{value}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
