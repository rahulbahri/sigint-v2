import { useState } from 'react'

// ─── Onboarding Modal ────────────────────────────────────────────────────────
// Shown on first visit (when localStorage('axiom_onboarded') is absent).
// 3-step flow: Stage → Priorities → Get Started.
// On completion, persists choices and dismisses itself.

const STAGES = [
  { id: 'seed',     label: 'Seed',     sub: 'Pre-revenue or early traction'       },
  { id: 'series_a', label: 'Series A', sub: '$1M–5M ARR, finding product-market fit' },
  { id: 'series_b', label: 'Series B', sub: '$5M–20M ARR, scaling go-to-market'   },
  { id: 'series_c', label: 'Series C+', sub: '$20M+ ARR, optimising for efficiency' },
]

const PRIORITIES = [
  { id: 'revenue',   icon: '📈', label: 'Revenue Growth',       sub: 'ARR, MRR, new bookings' },
  { id: 'burn',      icon: '🔥', label: 'Burn Efficiency',       sub: 'Runway, burn multiple, CAC' },
  { id: 'retention', icon: '🔄', label: 'Customer Retention',    sub: 'Churn, NRR, expansion' },
  { id: 'sales',     icon: '💼', label: 'Sales Productivity',    sub: 'Win rate, cycle length, quota' },
  { id: 'unit',      icon: '📐', label: 'Unit Economics',        sub: 'LTV:CAC, gross margin' },
  { id: 'ops',       icon: '⚙️',  label: 'Operational Metrics',  sub: 'Headcount efficiency, NPS' },
]

export default function OnboardingModal({ onComplete, initialStage = 'series_b' }) {
  const [step, setStep]           = useState(1)
  const [stage, setStage]         = useState(initialStage)
  const [priorities, setPriorities] = useState([])
  const [mode, setMode]           = useState(null)   // 'load' | 'demo'

  function togglePriority(id) {
    setPriorities(prev =>
      prev.includes(id) ? prev.filter(p => p !== id) : [...prev, id]
    )
  }

  function finish(selectedMode) {
    const m = selectedMode || mode || 'demo'
    localStorage.setItem('axiom_onboarded',   'true')
    localStorage.setItem('axiom_stage',        stage)
    localStorage.setItem('axiom_priorities',   JSON.stringify(priorities))
    onComplete({ stage, priorities, mode: m })
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4">
      <div className="w-full max-w-lg bg-[#0f172a] border border-white/15 rounded-2xl shadow-2xl overflow-hidden">

        {/* ── Progress bar ── */}
        <div className="flex h-1">
          {[1, 2, 3].map(s => (
            <div
              key={s}
              className={`flex-1 transition-colors duration-300 ${s <= step ? 'bg-blue-500' : 'bg-white/10'}`}
            />
          ))}
        </div>

        <div className="p-8">

          {/* ── Step 1: Stage ── */}
          {step === 1 && (
            <div className="space-y-6">
              <div>
                <p className="text-[11px] font-semibold text-blue-400 uppercase tracking-widest mb-2">Step 1 of 3</p>
                <h2 className="text-xl font-semibold text-white mb-1">What stage is your company at?</h2>
                <p className="text-[13px] text-white/50">
                  This calibrates industry benchmarks so you're always compared to peers at the same stage — not against unicorns.
                </p>
              </div>

              <div className="space-y-2">
                {STAGES.map(s => (
                  <button
                    key={s.id}
                    onClick={() => setStage(s.id)}
                    className={`w-full flex items-center gap-4 px-4 py-3 rounded-xl border transition-all text-left
                      ${stage === s.id
                        ? 'border-blue-500 bg-blue-950/40 text-white'
                        : 'border-white/10 bg-white/5 text-white/60 hover:border-white/25 hover:text-white'
                      }`}
                  >
                    <div className={`w-4 h-4 rounded-full border-2 flex-shrink-0 transition-colors ${stage === s.id ? 'border-blue-500 bg-blue-500' : 'border-white/30'}`} />
                    <div>
                      <div className="text-[14px] font-medium">{s.label}</div>
                      <div className="text-[12px] opacity-60">{s.sub}</div>
                    </div>
                  </button>
                ))}
              </div>

              <button
                onClick={() => setStep(2)}
                className="w-full py-3 rounded-xl bg-blue-600 hover:bg-blue-500 text-white text-[14px] font-semibold transition-colors"
              >
                Next →
              </button>
            </div>
          )}

          {/* ── Step 2: Priorities ── */}
          {step === 2 && (
            <div className="space-y-6">
              <div>
                <p className="text-[11px] font-semibold text-blue-400 uppercase tracking-widest mb-2">Step 2 of 3</p>
                <h2 className="text-xl font-semibold text-white mb-1">What matters most right now?</h2>
                <p className="text-[13px] text-white/50">
                  Pick up to three focus areas. The Executive Brief will lead with these — everything else stays visible but secondary.
                </p>
              </div>

              <div className="grid grid-cols-2 gap-2">
                {PRIORITIES.map(p => {
                  const on = priorities.includes(p.id)
                  return (
                    <button
                      key={p.id}
                      onClick={() => togglePriority(p.id)}
                      disabled={!on && priorities.length >= 3}
                      className={`flex items-start gap-3 p-3 rounded-xl border text-left transition-all
                        ${on
                          ? 'border-blue-500 bg-blue-950/40 text-white'
                          : 'border-white/10 bg-white/5 text-white/50 hover:border-white/20 hover:text-white/80'
                        } disabled:opacity-30 disabled:cursor-not-allowed`}
                    >
                      <span className="text-xl leading-none mt-0.5">{p.icon}</span>
                      <div>
                        <div className="text-[13px] font-medium leading-tight">{p.label}</div>
                        <div className="text-[11px] opacity-60 mt-0.5">{p.sub}</div>
                      </div>
                    </button>
                  )
                })}
              </div>

              {priorities.length > 0 && (
                <p className="text-[12px] text-blue-300/70">
                  {priorities.length}/3 selected
                  {priorities.length === 3 && ' — max reached'}
                </p>
              )}

              <div className="flex gap-3">
                <button
                  onClick={() => setStep(1)}
                  className="flex-1 py-3 rounded-xl bg-white/10 hover:bg-white/15 text-white/70 text-[14px] font-medium transition-colors"
                >
                  ← Back
                </button>
                <button
                  onClick={() => setStep(3)}
                  className="flex-[2] py-3 rounded-xl bg-blue-600 hover:bg-blue-500 text-white text-[14px] font-semibold transition-colors"
                >
                  Next →
                </button>
              </div>
            </div>
          )}

          {/* ── Step 3: Load data or demo ── */}
          {step === 3 && (
            <div className="space-y-6">
              <div>
                <p className="text-[11px] font-semibold text-blue-400 uppercase tracking-widest mb-2">Step 3 of 3</p>
                <h2 className="text-xl font-semibold text-white mb-1">Ready to go.</h2>
                <p className="text-[13px] text-white/50">
                  You can load your own KPI data now, or explore a fully-loaded demo dataset to see how the platform works before committing your numbers.
                </p>
              </div>

              {/* Summary of choices */}
              <div className="rounded-xl bg-white/5 border border-white/10 px-4 py-3 space-y-1 text-[12px]">
                <div className="flex justify-between">
                  <span className="text-white/50">Stage</span>
                  <span className="text-white font-medium">{STAGES.find(s => s.id === stage)?.label}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-white/50">Focus areas</span>
                  <span className="text-white font-medium">
                    {priorities.length
                      ? priorities.map(id => PRIORITIES.find(p => p.id === id)?.label).join(', ')
                      : 'All KPIs'}
                  </span>
                </div>
              </div>

              <div className="space-y-2">
                <button
                  onClick={() => finish('load')}
                  className="w-full py-3 rounded-xl bg-teal-600 hover:bg-teal-500 text-white text-[14px] font-semibold transition-colors"
                >
                  Upload my data →
                </button>
                <button
                  onClick={() => finish('demo')}
                  className="w-full py-3 rounded-xl bg-white/10 hover:bg-white/15 text-white/80 text-[14px] font-medium transition-colors"
                >
                  Explore with demo data first
                </button>
              </div>

              <button
                onClick={() => setStep(2)}
                className="w-full text-center text-[12px] text-white/30 hover:text-white/60 transition-colors"
              >
                ← Back
              </button>
            </div>
          )}

        </div>
      </div>
    </div>
  )
}
