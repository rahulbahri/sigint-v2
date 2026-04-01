import { useState, useMemo, useEffect, useCallback } from 'react'
import {
  Sliders, TrendingUp, TrendingDown, Minus,
  RefreshCw, BookMarked, ChevronDown, ChevronUp, Info,
} from 'lucide-react'
import axios from 'axios'

// ── KPI causal impact matrix ─────────────────────────────────────────────────
// For each output KPI, define which levers affect it and by how much (sensitivity coefficient)
// sign: +1 means moving lever UP moves this KPI UP, -1 means the opposite
const CAUSAL_MAP = {
  gross_margin:        { revenue_growth: 0.05,  gross_margin_adj: 1.0,  cost_reduction: 0.8  },
  operating_margin:    { revenue_growth: 0.04,  gross_margin_adj: 0.9,  headcount_delta: -0.3, cost_reduction: 0.7 },
  ebitda_margin:       { revenue_growth: 0.04,  gross_margin_adj: 0.85, headcount_delta: -0.3 },
  nrr:                 { churn_adj: -0.8,       expansion_adj: 0.8                              },
  arr_growth:          { revenue_growth: 0.9,   churn_adj: -0.4                                 },
  churn_rate:          { churn_adj: 1.0                                                          },
  burn_multiple:       { revenue_growth: -0.5,  headcount_delta: 0.4,   cost_reduction: -0.6   },
  cac_payback:         { cac_adj: 0.7,          revenue_growth: -0.2,   gross_margin_adj: -0.3 },
  ltv_cac:             { churn_adj: -0.5,       gross_margin_adj: 0.4,  cac_adj: -0.5          },
  dso:                 { revenue_growth: 0.1                                                     },
  opex_ratio:          { revenue_growth: -0.4,  headcount_delta: 0.5,   cost_reduction: -0.7   },
  headcount_eff:       { revenue_growth: 0.6,   headcount_delta: -0.5                           },
  rev_per_employee:    { revenue_growth: 0.5,   headcount_delta: -0.6                           },
}

// Levers the user can adjust
const LEVERS = [
  {
    id: 'revenue_growth',
    label: 'Revenue Growth Rate',
    unit: 'pp',
    description: 'Adjust the monthly revenue growth rate',
    min: -15, max: 15, step: 0.5, default: 0,
    color: '#0055A4',
  },
  {
    id: 'gross_margin_adj',
    label: 'Gross Margin Improvement',
    unit: 'pp',
    description: 'Improve or degrade gross margin (e.g. pricing, COGS optimisation)',
    min: -10, max: 10, step: 0.5, default: 0,
    color: '#059669',
  },
  {
    id: 'churn_adj',
    label: 'Churn Rate Change',
    unit: 'pp',
    description: 'Change in monthly churn (negative = improvement)',
    min: -5, max: 5, step: 0.25, default: 0,
    color: '#dc2626',
  },
  {
    id: 'cac_adj',
    label: 'CAC Change',
    unit: 'pp',
    description: 'Improve or worsen cost to acquire a customer (negative = cheaper)',
    min: -30, max: 30, step: 1, default: 0,
    color: '#d97706',
  },
  {
    id: 'headcount_delta',
    label: 'Headcount Growth',
    unit: 'pp',
    description: 'Change in headcount relative to revenue growth',
    min: -20, max: 20, step: 1, default: 0,
    color: '#7c3aed',
  },
  {
    id: 'cost_reduction',
    label: 'Opex Reduction',
    unit: 'pp',
    description: 'Direct operating expense reduction or increase',
    min: -10, max: 10, step: 0.5, default: 0,
    color: '#0891b2',
  },
  {
    id: 'expansion_adj',
    label: 'Expansion Revenue',
    unit: 'pp',
    description: 'Change in expansion / upsell rate',
    min: -10, max: 10, step: 0.5, default: 0,
    color: '#16a34a',
  },
]

// KPIs shown in the impact table
const OUTPUT_KPIS = [
  { key: 'gross_margin',     name: 'Gross Margin',        unit: 'pct', direction: 'higher' },
  { key: 'operating_margin', name: 'Operating Margin',    unit: 'pct', direction: 'higher' },
  { key: 'ebitda_margin',    name: 'EBITDA Margin',       unit: 'pct', direction: 'higher' },
  { key: 'nrr',              name: 'Net Revenue Retention', unit: 'pct', direction: 'higher' },
  { key: 'arr_growth',       name: 'ARR Growth',          unit: 'pct', direction: 'higher' },
  { key: 'churn_rate',       name: 'Churn Rate',          unit: 'pct', direction: 'lower'  },
  { key: 'burn_multiple',    name: 'Burn Multiple',       unit: 'ratio', direction: 'lower' },
  { key: 'cac_payback',      name: 'CAC Payback',         unit: 'months', direction: 'lower' },
  { key: 'ltv_cac',          name: 'LTV:CAC',             unit: 'ratio', direction: 'higher' },
  { key: 'opex_ratio',       name: 'Opex Ratio',          unit: 'pct', direction: 'lower'  },
  { key: 'headcount_eff',    name: 'Headcount Efficiency', unit: 'ratio', direction: 'higher' },
]

const UNIT_FMT = {
  pct:    v => `${v?.toFixed(1)}%`,
  ratio:  v => `${v?.toFixed(2)}x`,
  months: v => `${v?.toFixed(1)}mo`,
}

function fmt(v, unit) {
  if (v == null || isNaN(v)) return '—'
  return (UNIT_FMT[unit] || (x => x?.toFixed(2)))(v)
}

function deltaColor(delta, direction) {
  if (Math.abs(delta) < 0.01) return 'text-slate-400'
  const positive = direction === 'higher' ? delta > 0 : delta < 0
  return positive ? 'text-emerald-600' : 'text-red-500'
}

function deltaIcon(delta, direction) {
  if (Math.abs(delta) < 0.01) return <Minus size={11} className="text-slate-300" />
  const positive = direction === 'higher' ? delta > 0 : delta < 0
  return positive
    ? <TrendingUp size={11} className="text-emerald-500" />
    : <TrendingDown size={11} className="text-red-400" />
}

// ── Project impact from levers ────────────────────────────────────────────────
function projectImpact(baseValues, leverValues) {
  const result = {}
  for (const kpi of OUTPUT_KPIS) {
    const base    = baseValues[kpi.key]
    if (base == null) { result[kpi.key] = null; continue }
    const impacts = CAUSAL_MAP[kpi.key] || {}
    let totalDelta = 0
    for (const [lever, coef] of Object.entries(impacts)) {
      totalDelta += (leverValues[lever] || 0) * coef
    }
    result[kpi.key] = base + totalDelta
  }
  return result
}

// ── Component ─────────────────────────────────────────────────────────────────
export default function ScenarioPlanner({ fingerprint, authToken, onNavigateToDecisions }) {
  const initialLevers = Object.fromEntries(LEVERS.map(l => [l.id, l.default]))
  const [levers, setLevers]             = useState(initialLevers)
  const [scenarioName, setScenarioName] = useState('What-If Scenario')
  const [saving, setSaving]             = useState(false)
  const [saved, setSaved]               = useState(false)
  const [saveError, setSaveError]       = useState(null)
  const [showInfo, setShowInfo]         = useState(false)
  const [savedScenarios, setSavedScenarios]   = useState([])
  const [loadingScenarios, setLoadingScenarios] = useState(false)
  const [showSavedList, setShowSavedList]     = useState(false)

  const headers = authToken ? { Authorization: `Bearer ${authToken}` } : {}

  const loadSavedScenarios = useCallback(async () => {
    setLoadingScenarios(true)
    try {
      const r = await axios.get('/api/scenarios', { headers })
      setSavedScenarios(r.data.scenarios || [])
    } catch { /* ignore */ }
    setLoadingScenarios(false)
  }, [])

  useEffect(() => { loadSavedScenarios() }, [])

  // Base case values from fingerprint
  const baseValues = useMemo(() => {
    const map = {}
    for (const kpi of OUTPUT_KPIS) {
      const fp = fingerprint?.find(f => f.key === kpi.key)
      map[kpi.key] = fp?.avg ?? null
    }
    return map
  }, [fingerprint])

  // Projected values
  const projected = useMemo(() => projectImpact(baseValues, levers), [baseValues, levers])

  // Any lever is non-zero
  const hasChanges = LEVERS.some(l => levers[l.id] !== 0)

  // Reset all levers
  function reset() {
    setLevers(initialLevers)
    setSaved(false)
  }

  // Save scenario to API
  async function saveScenario() {
    if (!hasChanges) return
    setSaving(true)
    setSaveError(null)
    try {
      await axios.post('/api/scenarios', {
        name:        scenarioName,
        levers_json: JSON.stringify(levers),
        notes:       '',
      }, { headers })
      setSaved(true)
      setTimeout(() => setSaved(false), 3000)
      loadSavedScenarios()
    } catch (err) {
      setSaveError(err?.response?.data?.detail || 'Save failed — please try again.')
    } finally {
      setSaving(false)
    }
  }

  function loadScenario(scenario) {
    try {
      const parsed = JSON.parse(scenario.levers_json)
      setLevers({ ...initialLevers, ...parsed })
      setScenarioName(scenario.name)
      setShowSavedList(false)
    } catch { /* ignore */ }
  }

  return (
    <div className="max-w-5xl mx-auto px-6 py-6 space-y-6">

      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-xl font-bold text-slate-900 flex items-center gap-2">
            <Sliders size={20} className="text-[#0055A4]" />
            Scenario Planner
          </h1>
          <p className="text-[13px] text-slate-500 mt-0.5 max-w-xl">
            Adjust business levers and see the projected impact on your KPIs.
            Projections use causal relationships between assumptions and outcomes.
          </p>
        </div>
        <button
          onClick={() => setShowInfo(v => !v)}
          className="text-slate-400 hover:text-slate-600 mt-1"
        >
          <Info size={16} />
        </button>
      </div>

      {showInfo && (
        <div className="bg-blue-50 border border-blue-100 rounded-xl px-5 py-4 text-[12px] text-blue-800 leading-relaxed">
          <strong>How projections work:</strong> Each lever has a calibrated sensitivity coefficient
          for each output KPI, derived from the causal chain model. For example, a 1pp improvement in
          gross margin has a 0.9x impact on operating margin and 0.85x on EBITDA margin.
          These are direction-correct approximations, not accounting-precise forecasts.
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-5">

        {/* ── Left: Levers ── */}
        <div className="lg:col-span-2 space-y-3">
          <div className="flex items-center justify-between mb-1">
            <p className="text-[11px] font-bold text-slate-500 uppercase tracking-widest">Assumptions</p>
            {hasChanges && (
              <button
                onClick={reset}
                className="flex items-center gap-1 text-[10px] text-slate-400 hover:text-slate-600"
              >
                <RefreshCw size={9} /> Reset
              </button>
            )}
          </div>

          {LEVERS.map(lever => {
            const val = levers[lever.id]
            const isNonZero = val !== 0
            return (
              <div
                key={lever.id}
                className={`bg-white rounded-xl border ${isNonZero ? 'border-blue-200 shadow-sm' : 'border-slate-100'} px-4 py-3`}
              >
                <div className="flex items-center justify-between mb-2">
                  <div>
                    <p className="text-[12px] font-semibold text-slate-700">{lever.label}</p>
                    <p className="text-[10px] text-slate-400">{lever.description}</p>
                  </div>
                  <span
                    className={`text-sm font-bold tabular-nums ${
                      val > 0 ? 'text-emerald-600' : val < 0 ? 'text-red-500' : 'text-slate-400'
                    }`}
                  >
                    {val > 0 ? '+' : ''}{val}{lever.unit}
                  </span>
                </div>
                <input
                  type="range"
                  min={lever.min}
                  max={lever.max}
                  step={lever.step}
                  value={val}
                  onChange={e => setLevers(v => ({ ...v, [lever.id]: parseFloat(e.target.value) }))}
                  className="w-full h-1.5 rounded-full appearance-none cursor-pointer"
                  style={{
                    accentColor: lever.color,
                    background: `linear-gradient(to right, ${lever.color} ${((val - lever.min) / (lever.max - lever.min)) * 100}%, #e2e8f0 0%)`,
                  }}
                />
                <div className="flex justify-between text-[9px] text-slate-400 mt-0.5">
                  <span>{lever.min}{lever.unit}</span>
                  <span>0</span>
                  <span>+{lever.max}{lever.unit}</span>
                </div>
              </div>
            )
          })}

          {/* Save Scenario */}
          <div className="bg-white rounded-xl border border-slate-100 px-4 py-3 space-y-2.5">
            <p className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">
              Save Scenario
            </p>
            {savedScenarios.length > 0 && (
              <div className="relative">
                <button
                  type="button"
                  onClick={() => setShowSavedList(v => !v)}
                  className="w-full text-left flex items-center justify-between px-3 py-2 rounded-lg border border-slate-200 text-xs text-slate-500 hover:border-[#0055A4] hover:text-[#0055A4] transition-colors"
                >
                  <span>Load saved scenario…</span>
                  <ChevronDown size={11}/>
                </button>
                {showSavedList && (
                  <div className="absolute z-20 left-0 right-0 top-full mt-1 bg-white border border-slate-200 rounded-xl shadow-lg overflow-hidden max-h-48 overflow-y-auto">
                    {savedScenarios.map(s => (
                      <button
                        key={s.id}
                        onClick={() => loadScenario(s)}
                        className="w-full text-left px-3 py-2.5 text-xs text-slate-700 hover:bg-slate-50 flex items-center justify-between gap-2 border-b border-slate-50 last:border-0"
                      >
                        <span className="font-medium truncate">{s.name}</span>
                        <span className="text-slate-400 flex-shrink-0">{new Date(s.created_at).toLocaleDateString()}</span>
                      </button>
                    ))}
                  </div>
                )}
              </div>
            )}
            <input
              className="w-full border border-slate-200 rounded-lg px-3 py-2 text-sm text-slate-800 focus:outline-none focus:border-[#0055A4]"
              value={scenarioName}
              onChange={e => setScenarioName(e.target.value)}
              placeholder="Scenario name"
            />
            <button
              onClick={saveScenario}
              disabled={!hasChanges || saving}
              className="w-full flex items-center justify-center gap-1.5 text-[11px] font-bold bg-[#0055A4] text-white rounded-lg py-2 hover:bg-blue-700 disabled:opacity-40 transition-colors"
            >
              <BookMarked size={11} />
              {saved ? 'Scenario Saved!' : saving ? 'Saving…' : 'Save Scenario'}
            </button>
            {saveError && (
              <p className="text-[10px] text-red-500 flex items-center gap-1 mt-1">
                ⚠ {saveError}
              </p>
            )}
          </div>
        </div>

        {/* ── Right: Impact table ── */}
        <div className="lg:col-span-3">
          <p className="text-[11px] font-bold text-slate-500 uppercase tracking-widest mb-3">
            Projected Impact
          </p>
          <div className="bg-white rounded-2xl border border-slate-100 shadow-sm overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-100 bg-slate-50/70">
                  <th className="text-left px-4 py-3 text-[10px] font-bold text-slate-500 uppercase tracking-widest">KPI</th>
                  <th className="text-right px-4 py-3 text-[10px] font-bold text-slate-500 uppercase tracking-widest">Base</th>
                  <th className="text-right px-4 py-3 text-[10px] font-bold text-slate-500 uppercase tracking-widest">Scenario</th>
                  <th className="text-right px-4 py-3 text-[10px] font-bold text-slate-500 uppercase tracking-widest">Delta</th>
                  <th className="px-3 py-3"></th>
                </tr>
              </thead>
              <tbody>
                {OUTPUT_KPIS.map((kpi, i) => {
                  const base   = baseValues[kpi.key]
                  const proj   = projected[kpi.key]
                  const delta  = proj != null && base != null ? proj - base : null
                  const impactful = delta != null && Math.abs(delta) > 0.05

                  return (
                    <tr
                      key={kpi.key}
                      className={`border-b border-slate-50 last:border-0 ${
                        impactful ? 'bg-blue-50/30' : ''
                      }`}
                    >
                      <td className="px-4 py-3">
                        <span className="text-[12px] font-medium text-slate-700">{kpi.name}</span>
                      </td>
                      <td className="px-4 py-3 text-right">
                        <span className="text-[12px] text-slate-500 tabular-nums font-mono">
                          {fmt(base, kpi.unit)}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right">
                        <span className={`text-[12px] tabular-nums font-mono font-semibold ${
                          impactful ? deltaColor(delta, kpi.direction) : 'text-slate-500'
                        }`}>
                          {fmt(proj, kpi.unit)}
                        </span>
                      </td>
                      <td className={`px-4 py-3 text-right text-[11px] tabular-nums font-mono ${
                        impactful ? deltaColor(delta, kpi.direction) : 'text-slate-300'
                      }`}>
                        {delta != null
                          ? `${delta > 0 ? '+' : ''}${delta.toFixed(1)}`
                          : '—'
                        }
                      </td>
                      <td className="px-3 py-3 text-center">
                        {delta != null && deltaIcon(delta, kpi.direction)}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>

          {/* Summary banner */}
          {hasChanges && (
            <div className="mt-3 bg-slate-50 border border-slate-200 rounded-xl px-4 py-3">
              <p className="text-[11px] font-bold text-slate-500 uppercase tracking-widest mb-2">
                Scenario Summary
              </p>
              <div className="flex flex-wrap gap-2">
                {OUTPUT_KPIS.map(kpi => {
                  const base  = baseValues[kpi.key]
                  const proj  = projected[kpi.key]
                  const delta = proj != null && base != null ? proj - base : null
                  if (delta == null || Math.abs(delta) < 0.1) return null
                  const positive = kpi.direction === 'higher' ? delta > 0 : delta < 0
                  return (
                    <span
                      key={kpi.key}
                      className={`text-[10px] font-bold border rounded-full px-2.5 py-1 ${
                        positive
                          ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
                          : 'bg-red-50 text-red-700 border-red-200'
                      }`}
                    >
                      {kpi.name}: {delta > 0 ? '+' : ''}{delta.toFixed(1)}
                    </span>
                  )
                }).filter(Boolean)}
              </div>
            </div>
          )}

          {/* Push to Decision Log */}
          {hasChanges && onNavigateToDecisions && (
            <button
              onClick={() => {
                // Build a pre-filled decision from the current scenario levers + projected impact
                const leverSummary = LEVERS
                  .filter(l => levers[l.id] !== 0)
                  .map(l => `${l.label}: ${levers[l.id] > 0 ? '+' : ''}${levers[l.id]}${l.unit}`)
                  .join(', ')
                const impactSummary = OUTPUT_KPIS
                  .map(kpi => {
                    const base  = baseValues[kpi.key]
                    const proj  = projected[kpi.key]
                    const delta = proj != null && base != null ? proj - base : null
                    return delta != null && Math.abs(delta) >= 0.1
                      ? `${kpi.name}: ${delta > 0 ? '+' : ''}${delta.toFixed(1)}`
                      : null
                  })
                  .filter(Boolean)
                  .join(', ')
                onNavigateToDecisions({
                  title:        scenarioName,
                  the_decision: `Based on scenario "${scenarioName}": ${leverSummary}`,
                  rationale:    impactSummary
                    ? `Projected impact: ${impactSummary}.`
                    : 'Scenario analysis — see lever adjustments above.',
                  decided_by:   'CFO',
                  kpi_context:  OUTPUT_KPIS
                    .filter(kpi => {
                      const base = baseValues[kpi.key]
                      const proj = projected[kpi.key]
                      return base != null && proj != null && Math.abs(proj - base) >= 0.1
                    })
                    .map(kpi => kpi.key),
                })
              }}
              className="mt-3 w-full flex items-center justify-center gap-2 px-4 py-2.5
                         border border-[#0055A4] text-[#0055A4] rounded-xl text-[12px] font-bold
                         hover:bg-[#0055A4] hover:text-white transition-all"
            >
              <BookMarked size={13}/>
              Push to Decision Log
            </button>
          )}

          <p className="text-[10px] text-slate-400 mt-3 leading-relaxed">
            Projections are directional estimates based on calibrated causal sensitivity coefficients.
            Use &ldquo;Push to Decision Log&rdquo; to record the reasoning behind this scenario.
          </p>
        </div>
      </div>
    </div>
  )
}
