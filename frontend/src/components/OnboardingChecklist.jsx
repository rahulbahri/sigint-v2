import { useState, useEffect } from 'react'
import axios from 'axios'
import { CheckCircle2, Circle, ChevronDown, ChevronUp, X, Zap, TrendingUp } from 'lucide-react'

export default function OnboardingChecklist({ fingerprint, onNavigate, authToken }) {
  const [dismissed, setDismissed] = useState(
    () => localStorage.getItem('axiom_checklist_dismissed') === 'true'
  )
  const [collapsed, setCollapsed] = useState(false)
  const [hasOwner, setHasOwner]   = useState(false)
  const [coverage, setCoverage]   = useState(null)

  const headers = authToken ? { Authorization: `Bearer ${authToken}` } : {}

  useEffect(() => {
    axios.get('/api/accountability', { headers })
      .then(r => {
        const entries = r.data?.accountability || r.data || {}
        setHasOwner(Object.values(entries).some(v => v.owner && v.owner.trim()))
      })
      .catch(() => {})

    axios.get('/api/kpi-coverage', { headers })
      .then(r => setCoverage(r.data))
      .catch(() => {})
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  if (dismissed) return null

  const hasData    = fingerprint?.some(k => k.avg != null)
  const hasStage   = !!localStorage.getItem('axiom_stage')
  const hasDeck    = !!localStorage.getItem('axiom_deck_exported')
  const hasTargets = !!localStorage.getItem('axiom_targets_set')
  const hasSource  = (coverage?.source_count ?? 0) > 0 || (coverage?.has_csv_data ?? false)

  const steps = [
    { id: 'stage',   done: hasStage,   label: 'Set your company stage',          action: () => {} },
    { id: 'data',    done: hasData,    label: 'Load your KPI data',               action: () => onNavigate('upload') },
    { id: 'source',  done: hasSource,  label: 'Connect a live data source',       action: () => onNavigate('sources') },
    { id: 'targets', done: hasTargets, label: 'Review KPI targets',              action: () => onNavigate('targets') },
    { id: 'owner',   done: hasOwner,   label: 'Assign an owner to a critical KPI', action: () => onNavigate('variance') },
    { id: 'deck',    done: hasDeck,    label: 'Export your first board deck',     action: () => onNavigate('board') },
  ]

  const doneCount = steps.filter(s => s.done).length
  const allDone   = doneCount === steps.length
  if (allDone) return null

  const pct           = Math.round((doneCount / steps.length) * 100)
  const coveragePct   = coverage?.coverage_pct ?? 0
  const coveredKpis   = coverage?.covered_kpis ?? 0
  const totalKpis     = coverage?.total_kpis ?? 57
  const sourceCount   = coverage?.source_count ?? 0

  return (
    <div className="mx-4 mt-3 bg-white border border-blue-100 rounded-2xl shadow-sm overflow-hidden">

      {/* Header row */}
      <div
        className="flex items-center justify-between px-4 py-2.5 cursor-pointer hover:bg-slate-50/50"
        onClick={() => setCollapsed(v => !v)}
      >
        <div className="flex items-center gap-3">
          <div className="text-[12px] font-semibold text-slate-700">
            Setup — {doneCount}/{steps.length} complete
          </div>
          <div className="w-20 h-1.5 bg-slate-100 rounded-full overflow-hidden">
            <div
              className="h-full bg-[#0055A4] rounded-full transition-all"
              style={{ width: `${pct}%` }}
            />
          </div>
        </div>
        <div className="flex items-center gap-2">
          {collapsed
            ? <ChevronDown size={14} className="text-slate-400" />
            : <ChevronUp size={14} className="text-slate-400" />
          }
          <button
            onClick={e => {
              e.stopPropagation()
              localStorage.setItem('axiom_checklist_dismissed', 'true')
              setDismissed(true)
            }}
            className="text-slate-300 hover:text-slate-500"
          >
            <X size={14} />
          </button>
        </div>
      </div>

      {!collapsed && (
        <div className="border-t border-slate-50">

          {/* KPI Coverage Score */}
          {coverage != null && (
            <div className="px-4 py-3 bg-slate-50/70 flex items-center justify-between gap-4">
              <div className="flex items-center gap-2.5">
                <div className="flex items-center gap-1.5">
                  <Zap size={11} className="text-[#0055A4]" />
                  <span className="text-[11px] font-bold text-slate-600">KPI Coverage</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <div className="w-24 h-1.5 bg-slate-200 rounded-full overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all ${
                        coveragePct >= 60 ? 'bg-emerald-500' :
                        coveragePct >= 30 ? 'bg-amber-400' : 'bg-slate-400'
                      }`}
                      style={{ width: `${coveragePct}%` }}
                    />
                  </div>
                  <span className={`text-[11px] font-bold ${
                    coveragePct >= 60 ? 'text-emerald-600' :
                    coveragePct >= 30 ? 'text-amber-600' : 'text-slate-500'
                  }`}>
                    {coveragePct}%
                  </span>
                </div>
              </div>
              <div className="flex items-center gap-3">
                <span className="text-[10px] text-slate-400">
                  {coveredKpis}/{totalKpis} KPIs
                </span>
                {sourceCount === 0 && (
                  <button
                    onClick={() => onNavigate('sources')}
                    className="flex items-center gap-1 text-[10px] font-bold text-[#0055A4] hover:text-blue-700"
                  >
                    <TrendingUp size={9} /> Connect sources to unlock more
                  </button>
                )}
                {sourceCount > 0 && (
                  <span className="text-[10px] text-emerald-600 font-medium">
                    {sourceCount} live connector{sourceCount > 1 ? 's' : ''}
                  </span>
                )}
              </div>
            </div>
          )}

          {/* Steps */}
          <div className="px-4 pb-3 pt-2 space-y-1.5">
            {steps.map(step => (
              <div key={step.id} className="flex items-center gap-2.5">
                {step.done
                  ? <CheckCircle2 size={14} className="text-emerald-500 shrink-0" />
                  : <Circle size={14} className="text-slate-300 shrink-0" />
                }
                <button
                  onClick={step.action}
                  className={`text-[11px] ${
                    step.done
                      ? 'line-through text-slate-400'
                      : 'text-slate-700 hover:text-blue-600'
                  } text-left transition-colors`}
                >
                  {step.label}
                </button>
                {!step.done && (
                  <span className="text-[9px] text-blue-500 font-bold">→</span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
