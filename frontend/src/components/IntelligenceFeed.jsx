import { useState, useEffect } from 'react'
import axios from 'axios'
import {
  Brain, AlertTriangle, TrendingUp, Target, Link2,
  BarChart3, CheckCircle2, XCircle, Clock, ChevronDown,
  Zap, Eye, EyeOff,
} from 'lucide-react'

const ICON_MAP = {
  anomaly:              { Icon: AlertTriangle, color: 'text-red-500' },
  systemic_anomaly:     { Icon: Zap,           color: 'text-red-600' },
  causal_discovery:     { Icon: Link2,         color: 'text-blue-500' },
  regime_transition:    { Icon: TrendingUp,    color: 'text-amber-500' },
  retrain_suggestion:   { Icon: Clock,         color: 'text-amber-500' },
  target_suggestion:    { Icon: Target,        color: 'text-blue-500' },
  structural_miss:      { Icon: XCircle,       color: 'text-red-500' },
  decision_pattern:     { Icon: CheckCircle2,  color: 'text-emerald-500' },
  decision_maker_pattern: { Icon: BarChart3,   color: 'text-slate-500' },
  industry_update:      { Icon: BarChart3,     color: 'text-blue-400' },
}

const SEVERITY_STYLE = {
  critical: 'border-red-200 bg-red-50/50',
  warning:  'border-amber-200 bg-amber-50/50',
  info:     'border-slate-200 bg-white',
  positive: 'border-emerald-200 bg-emerald-50/50',
}

function timeAgo(iso) {
  if (!iso) return ''
  const diff = (Date.now() - new Date(iso).getTime()) / 1000
  if (diff < 60) return 'just now'
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}

export default function IntelligenceFeed({ authToken, compact = false }) {
  const [insights, setInsights] = useState([])
  const [loading, setLoading]   = useState(true)
  const [showAll, setShowAll]   = useState(false)
  const headers = authToken ? { Authorization: `Bearer ${authToken}` } : {}

  useEffect(() => {
    axios.get('/api/agent-insights?limit=20&status=all', { headers })
      .then(r => setInsights(r.data?.insights || []))
      .catch(() => {})
      .finally(() => setLoading(false))
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const dismiss = async (id) => {
    await axios.put(`/api/agent-insights/${id}/status`, { status: 'dismissed' }, { headers }).catch(() => {})
    setInsights(prev => prev.filter(i => i.id !== id))
  }

  const active = insights.filter(i => i.status !== 'dismissed')
  const display = showAll ? active : active.slice(0, compact ? 3 : 6)

  if (loading) return null
  if (!active.length) return null

  return (
    <div className="card p-4 shadow-sm">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <Brain size={13} className="text-[#0055A4]" />
          <h2 className="text-slate-700 text-[11px] font-bold uppercase tracking-wider">Intelligence Feed</h2>
          <span className="bg-blue-100 text-blue-700 text-[9px] font-bold px-1.5 py-0.5 rounded-full">{active.length}</span>
        </div>
        {active.length > (compact ? 3 : 6) && (
          <button
            onClick={() => setShowAll(!showAll)}
            className="text-[10px] text-[#0055A4] font-semibold hover:underline flex items-center gap-1"
          >
            {showAll ? 'Show less' : `View all ${active.length}`}
            <ChevronDown size={10} className={showAll ? 'rotate-180' : ''} />
          </button>
        )}
      </div>

      <div className="space-y-2">
        {display.map(ins => {
          const cfg = ICON_MAP[ins.insight_type] || { Icon: Brain, color: 'text-slate-400' }
          const style = SEVERITY_STYLE[ins.severity] || SEVERITY_STYLE.info
          return (
            <div key={ins.id} className={`flex items-start gap-2.5 p-2.5 rounded-xl border ${style} group`}>
              <cfg.Icon size={13} className={`${cfg.color} mt-0.5 flex-shrink-0`} />
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-0.5">
                  <span className="text-[11px] font-semibold text-slate-700 truncate">{ins.title}</span>
                  <span className="text-[9px] text-slate-400 flex-shrink-0">{timeAgo(ins.created_at)}</span>
                </div>
                {ins.description && (
                  <p className="text-[10px] text-slate-500 leading-relaxed line-clamp-2">{ins.description}</p>
                )}
                {ins.confidence != null && (
                  <span className="text-[9px] text-slate-400 mt-0.5 inline-block">
                    {(ins.confidence * 100).toFixed(0)}% confidence
                  </span>
                )}
              </div>
              <button
                onClick={() => dismiss(ins.id)}
                className="text-slate-300 hover:text-slate-500 opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0"
                title="Dismiss"
              >
                <EyeOff size={11} />
              </button>
            </div>
          )
        })}
      </div>
    </div>
  )
}
