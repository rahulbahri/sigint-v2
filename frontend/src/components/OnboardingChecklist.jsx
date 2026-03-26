import { useState, useEffect } from 'react'
import axios from 'axios'
import { CheckCircle2, Circle, ChevronDown, ChevronUp, X } from 'lucide-react'

export default function OnboardingChecklist({ fingerprint, onNavigate }) {
  const [dismissed, setDismissed] = useState(() => localStorage.getItem('axiom_checklist_dismissed') === 'true')
  const [collapsed, setCollapsed] = useState(false)
  const [hasOwner, setHasOwner] = useState(false)

  useEffect(() => {
    axios.get('/api/accountability')
      .then(r => {
        const entries = r.data?.accountability || r.data || {}
        setHasOwner(Object.values(entries).some(v => v.owner && v.owner.trim()))
      })
      .catch(() => {})
  }, [])

  if (dismissed) return null

  const hasData = fingerprint?.some(k => k.avg != null)
  const hasStage = !!localStorage.getItem('axiom_stage')
  const hasDeck = !!localStorage.getItem('axiom_deck_exported')
  const hasTargets = !!localStorage.getItem('axiom_targets_set')

  const steps = [
    { id: 'stage',   done: hasStage,   label: 'Set your company stage', action: () => {} },
    { id: 'data',    done: hasData,    label: 'Load your KPI data',      action: () => onNavigate('upload') },
    { id: 'targets', done: hasTargets, label: 'Review KPI targets',       action: () => onNavigate('targets') },
    { id: 'owner',   done: hasOwner,   label: 'Assign an owner to a critical KPI', action: () => onNavigate('variance') },
    { id: 'deck',    done: hasDeck,    label: 'Export your first board deck', action: () => onNavigate('board') },
  ]

  const doneCount = steps.filter(s => s.done).length
  const allDone = doneCount === steps.length

  if (allDone) {
    return null // hide when complete
  }

  const pct = Math.round((doneCount / steps.length) * 100)

  return (
    <div className="mx-4 mt-3 bg-white border border-blue-100 rounded-2xl shadow-sm overflow-hidden">
      <div
        className="flex items-center justify-between px-4 py-2.5 cursor-pointer hover:bg-slate-50/50"
        onClick={() => setCollapsed(v => !v)}
      >
        <div className="flex items-center gap-3">
          <div className="text-[12px] font-semibold text-slate-700">Getting started — {doneCount}/{steps.length} complete</div>
          <div className="w-24 h-1.5 bg-slate-100 rounded-full overflow-hidden">
            <div className="h-full bg-[#0055A4] rounded-full transition-all" style={{ width: `${pct}%` }}/>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {collapsed ? <ChevronDown size={14} className="text-slate-400"/> : <ChevronUp size={14} className="text-slate-400"/>}
          <button
            onClick={e => { e.stopPropagation(); localStorage.setItem('axiom_checklist_dismissed','true'); setDismissed(true) }}
            className="text-slate-300 hover:text-slate-500"
          >
            <X size={14}/>
          </button>
        </div>
      </div>

      {!collapsed && (
        <div className="px-4 pb-3 space-y-1.5 border-t border-slate-50">
          {steps.map(step => (
            <div key={step.id} className="flex items-center gap-2.5">
              {step.done
                ? <CheckCircle2 size={14} className="text-emerald-500 shrink-0"/>
                : <Circle size={14} className="text-slate-300 shrink-0"/>
              }
              <button
                onClick={step.action}
                className={`text-[11px] ${step.done ? 'line-through text-slate-400' : 'text-slate-700 hover:text-blue-600'} text-left transition-colors`}
              >
                {step.label}
              </button>
              {!step.done && (
                <span className="text-[9px] text-blue-500 font-medium">→</span>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
