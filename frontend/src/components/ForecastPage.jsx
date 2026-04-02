import { useState, useEffect, useCallback, useMemo } from 'react'
import axios from 'axios'
import {
  ComposedChart, Area, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, ReferenceLine
} from 'recharts'
import {
  TrendingUp, TrendingDown, Minus, Zap, Play, RefreshCw,
  ChevronRight, Info, AlertCircle, X, Plus, RotateCcw
} from 'lucide-react'

// ── Constants ─────────────────────────────────────────────────────────────────

const ACCENT = '#00AEEF'
const MAX_SCENARIOS = 5
const STATE_LABELS        = ['Very Low', 'Below Avg', 'Average', 'Above Avg', 'Very High']
const STATE_LABELS_LOWER  = ['Best', 'Good', 'Average', 'Poor', 'Worst']
const STATE_COLORS        = ['#ef4444', '#f97316', '#94a3b8', '#22c55e', '#3b82f6']
const STATE_PCTS    = ['p10', 'p25', 'p50', 'p75', 'p90']

// KPIs where lower = better (for narrative direction)
const LOWER_BETTER = new Set([
  'churn_rate', 'dso', 'cash_conv_cycle', 'cac_payback',
  'burn_multiple', 'opex_ratio', 'customer_concentration',
  'revenue_fragility', 'burn_convexity', 'margin_volatility', 'customer_decay_slope',
])

// ── Formatters ────────────────────────────────────────────────────────────────

const PCT_KPIS = new Set([
  'gross_margin', 'operating_margin', 'ebitda_margin', 'opex_ratio',
  'contribution_margin', 'revenue_quality', 'recurring_revenue',
  'customer_concentration', 'churn_rate', 'revenue_growth', 'arr_growth', 'nrr',
  'margin_volatility', 'pipeline_conversion', 'customer_decay_slope', 'pricing_power_index',
])
const DAY_KPIS   = new Set(['dso', 'cash_conv_cycle'])
const MONTH_KPIS = new Set(['cac_payback'])
const USD_KPIS   = new Set(['customer_ltv'])

function fmtVal(key, val) {
  if (val == null || isNaN(val)) return '—'
  if (PCT_KPIS.has(key))   return val.toFixed(2) + '%'
  if (DAY_KPIS.has(key))   return val.toFixed(2) + 'd'
  if (MONTH_KPIS.has(key)) return val.toFixed(2) + 'mo'
  if (USD_KPIS.has(key))   return '$' + val.toFixed(1) + 'K'
  return val.toFixed(2) + 'x'
}

function formatKpiKey(key) {
  if (!key) return ''
  return key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
}

// ── Narrative generator ────────────────────────────────────────────────────────

function buildNarrative(kpi, traj, causalPaths, horizonDays, scenarios, valueRanges) {
  if (!traj?.length || !kpi) return null

  const first  = traj[0]
  const last   = traj[traj.length - 1]
  const months = Math.round(horizonDays / 30)
  const vr     = valueRanges?.[kpi] ?? {}

  const current   = first.p50
  const projected = last.p50
  const change    = projected - current
  const pctChange = current !== 0 ? (change / Math.abs(current)) * 100 : 0

  const higherBetter = !LOWER_BETTER.has(kpi)
  const businessGood = higherBetter ? change > 0 : change < 0
  const meaningful   = Math.abs(pctChange) > 1.5

  // Direction phrase
  let dirPhrase, dirIcon
  if (!meaningful) {
    dirPhrase = 'remain broadly stable'
    dirIcon   = 'flat'
  } else if (businessGood) {
    dirPhrase = `improve by ${Math.abs(pctChange).toFixed(1)}%`
    dirIcon   = 'up'
  } else {
    dirPhrase = `deteriorate by ${Math.abs(pctChange).toFixed(1)}%`
    dirIcon   = 'down'
  }

  // Uncertainty
  const band    = last.p90 - last.p10
  const bandPct = Math.abs(projected) !== 0 ? (band / Math.abs(projected)) * 100 : 0
  let uncertainty, uncertaintyNote
  if (bandPct < 8) {
    uncertainty     = 'narrow'
    uncertaintyNote = 'Historical patterns are consistent — this projection carries high confidence.'
  } else if (bandPct < 20) {
    uncertainty     = 'moderate'
    uncertaintyNote = 'Some variability in historical patterns — treat the median as the base case.'
  } else {
    uncertainty     = 'wide'
    uncertaintyNote = 'High month-to-month variability in history — the range of outcomes is broad. Do not treat the median as certain.'
  }

  // Key sentence
  const histMedian = vr.p50 != null ? ` (historical median: ${fmtVal(kpi, vr.p50)})` : ''
  let text = `**${formatKpiKey(kpi)}** is currently at **${fmtVal(kpi, current)}**${histMedian}. `
  text += `Over the next ${months} month${months !== 1 ? 's' : ''}, the simulation projects this to **${dirPhrase}**, `
  text += `with a median outcome of **${fmtVal(kpi, projected)}** `
  text += `(scenario range: ${fmtVal(kpi, last.p10)} – ${fmtVal(kpi, last.p90)}). `

  // Uncertainty note
  text += `The confidence band is **${uncertainty}** (±${bandPct.toFixed(0)}% of median). ${uncertaintyNote} `

  // Causal drivers
  if (causalPaths?.length) {
    const names = causalPaths.slice(0, 2).map(
      cp => `${formatKpiKey(cp.from)} (${(cp.strength * 100).toFixed(0)}% weight)`
    )
    text += `The primary causal driver${names.length > 1 ? 's are' : ' is'} ${names.join(' and ')}. `
    text += `Changes in ${names.length > 1 ? 'these KPIs' : 'this KPI'} will propagate into ${formatKpiKey(kpi)} in subsequent months. `
  }

  // Scenario override note
  const activeScenario = scenarios.find(s => s.kpi === kpi)
  if (activeScenario && vr[STATE_PCTS[activeScenario.state]] != null) {
    const overrideVal  = vr[STATE_PCTS[activeScenario.state]]
    const stateName    = STATE_LABELS[activeScenario.state]
    const vsActual     = overrideVal > current ? 'above' : 'below'
    text += `**Scenario note:** This KPI's starting value is pinned to **${stateName}** (${fmtVal(kpi, overrideVal)}), `
    text += `which is ${vsActual} its current level of ${fmtVal(kpi, current)}. `
    text += `This directly shapes the simulated trajectory shown. `
  }

  // Drivers from upstream scenarios that feed into this KPI
  const upstreamActive = scenarios.filter(
    s => s.kpi !== kpi && causalPaths?.some(cp => cp.from === s.kpi)
  )
  if (upstreamActive.length) {
    const names = upstreamActive.map(s => `${formatKpiKey(s.kpi)} set to ${STATE_LABELS[s.state]}`).join(', ')
    text += `The scenario also sets **${names}** — these upstream shifts propagate through the causal network and contribute to the projected range. `
  }

  // Action signal
  if (!meaningful) {
    text += '**Signal:** No significant directional trend detected. Monitor for any emerging change in inputs.'
  } else if (businessGood) {
    text += '**Signal:** Trajectory is constructive. Sustain the conditions driving this trend — do not assume it continues without active reinforcement.'
  } else {
    text += `**Signal: Attention required.** Left unchecked, this deterioration will compound — ${formatKpiKey(kpi)} does not self-correct to average. Identify and act on the root cause.`
  }

  return { text, dirIcon, businessGood, meaningful, projected, band, pctChange }
}

// ── Regime Badge ──────────────────────────────────────────────────────────────

const REGIME_STYLES = {
  Growth:   { bg: 'bg-emerald-50', border: 'border-emerald-200', text: 'text-emerald-700', dot: 'bg-emerald-400' },
  Recovery: { bg: 'bg-sky-50',     border: 'border-sky-200',     text: 'text-sky-700',     dot: 'bg-sky-400'     },
  Stress:   { bg: 'bg-red-50',     border: 'border-red-200',     text: 'text-red-700',     dot: 'bg-red-400'     },
}

function RegimeBadge({ regime }) {
  if (!regime?.name) return null
  const s = REGIME_STYLES[regime.name] ?? REGIME_STYLES.Recovery
  return (
    <div className={`mt-1 flex items-center gap-2 px-2.5 py-1.5 rounded-lg border text-[11px] font-medium ${s.bg} ${s.border} ${s.text}`}>
      <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${s.dot}`} />
      <span>{regime.name} Regime</span>
      <span className="ml-auto text-[10px] font-normal opacity-70">
        Month {regime.months_in} of {regime.months_in}
      </span>
    </div>
  )
}

// ── Narrative rendering ────────────────────────────────────────────────────────

function NarrativePanel({ narrative }) {
  if (!narrative) return null
  const { text, dirIcon, businessGood, meaningful } = narrative

  const parts = text.split(/(\*\*[^*]+\*\*)/)

  return (
    <div className="card p-5 border-l-4" style={{
      borderLeftColor: !meaningful ? '#94a3b8' : businessGood ? '#22c55e' : '#ef4444'
    }}>
      <div className="flex items-center gap-2 mb-3">
        {!meaningful
          ? <Minus size={15} className="text-slate-400" />
          : businessGood
            ? <TrendingUp size={15} className="text-emerald-500" />
            : <TrendingDown size={15} className="text-red-500" />
        }
        <h3 className="text-sm font-semibold text-slate-700">Simulation Narrative</h3>
      </div>
      <p className="text-[13px] text-slate-600 leading-relaxed">
        {parts.map((part, i) =>
          part.startsWith('**') && part.endsWith('**')
            ? <strong key={i} className="text-slate-800">{part.slice(2, -2)}</strong>
            : <span key={i}>{part}</span>
        )}
      </p>
    </div>
  )
}

// ── Tooltip ───────────────────────────────────────────────────────────────────

function ChartTooltip({ active, payload, label, kpi }) {
  if (!active || !payload?.length) return null
  const p50 = payload.find(p => p.dataKey === 'p50')?.value
  const p10 = payload.find(p => p.dataKey === 'p10')?.value
  const p90 = payload.find(p => p.dataKey === 'p90')?.value
  return (
    <div className="card p-3 text-xs shadow-lg border border-slate-200 min-w-[160px]">
      <p className="font-semibold text-slate-700 mb-2">{label}</p>
      {p50 != null && <div className="flex justify-between gap-4"><span className="text-slate-500">Median</span><span className="font-bold" style={{ color: ACCENT }}>{fmtVal(kpi, p50)}</span></div>}
      {p90 != null && <div className="flex justify-between gap-4"><span className="text-slate-400">Optimistic</span><span className="text-emerald-600">{fmtVal(kpi, p90)}</span></div>}
      {p10 != null && <div className="flex justify-between gap-4"><span className="text-slate-400">Pessimistic</span><span className="text-red-400">{fmtVal(kpi, p10)}</span></div>}
    </div>
  )
}

// ── Scenario slider card ───────────────────────────────────────────────────────

function ScenarioSlider({ kpi, state, valueRanges, onChange, onRemove }) {
  const vr           = valueRanges?.[kpi]
  const mapped       = vr ? vr[STATE_PCTS[state]] : null
  const lowerBetter  = LOWER_BETTER.has(kpi)
  // colorState inverts for lower-better KPIs so colour reflects business quality:
  //   low churn (state 0) → colorState 4 (blue = good performance)
  //   high churn (state 4) → colorState 0 (red = bad performance)
  // labelState always matches the raw percentile level so the caption is literal:
  //   state 0 = "Very Low" (low churn), state 4 = "Very High" (high churn)
  const colorState = lowerBetter ? 4 - state : state

  return (
    <div className="rounded-lg border border-slate-100 bg-slate-50 p-3">
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-semibold text-slate-700">{formatKpiKey(kpi)}</span>
        <div className="flex items-center gap-2">
          <span
            className="text-[10px] font-bold px-1.5 py-0.5 rounded"
            style={{ backgroundColor: STATE_COLORS[colorState] + '22', color: STATE_COLORS[colorState] }}
          >
            {lowerBetter ? STATE_LABELS_LOWER[state] : STATE_LABELS[state]}
          </span>
          <button onClick={() => onRemove(kpi)} className="text-slate-300 hover:text-red-400 transition-colors">
            <X size={12} />
          </button>
        </div>
      </div>
      <input
        type="range" min={0} max={4} step={1}
        value={state}
        onChange={e => onChange(kpi, Number(e.target.value))}
        className="w-full accent-[#00AEEF]"
      />
      <div className="flex justify-between text-[9px] text-slate-300 mt-1">
        {lowerBetter
          ? <><span>Best</span><span>Avg</span><span>Worst</span></>
          : <><span>Very Low</span><span>Avg</span><span>Very High</span></>
        }
      </div>
      {vr && (
        <div className="flex items-center justify-between mt-1.5 text-[10px] text-slate-400">
          <span>Current: <strong className="text-slate-600">{fmtVal(kpi, vr.current)}</strong></span>
          {mapped != null && <span>Set to: <strong style={{ color: STATE_COLORS[colorState] }}>{fmtVal(kpi, mapped)}</strong></span>}
        </div>
      )}
    </div>
  )
}

// ── Main component ─────────────────────────────────────────────────────────────

export default function ForecastPage() {
  const [model, setModel]             = useState(null)
  const [modelLoading, setModelLoading] = useState(true)
  const [building, setBuilding]       = useState(false)
  const [running, setRunning]         = useState(false)
  const [result, setResult]           = useState(null)
  const [error, setError]             = useState(null)

  const [horizonDays, setHorizonDays] = useState(90)
  const [nSamples, setNSamples]       = useState(400)
  const [selectedKpi, setSelectedKpi] = useState(null)

  // Scenario inputs: [{kpi, state}], max 5
  const [scenarios, setScenarios]     = useState([])
  const [addingKpi, setAddingKpi]     = useState(false)

  const loadModel = useCallback(async () => {
    setModelLoading(true)
    try {
      const res = await axios.get('/api/forecast/model')
      setModel(res.data)
      if (res.data?.kpis?.length && !selectedKpi) {
        setSelectedKpi(res.data.kpis[0])
      }
    } catch (e) {
      console.error(e)
    }
    setModelLoading(false)
  }, [selectedKpi])

  useEffect(() => { loadModel() }, [])

  async function handleBuild() {
    setBuilding(true)
    setError(null)
    setResult(null)
    try {
      await axios.post('/api/forecast/build')
      // Poll up to 120 s (60 × 2 s). The server now returns status "building"
      // while training is in progress, "ready" on success, "error" on failure.
      for (let i = 0; i < 60; i++) {
        await new Promise(r => setTimeout(r, 2000))
        const res = await axios.get('/api/forecast/model')
        const s = res.data?.status
        if (s === 'ready') {
          setModel(res.data)
          if (res.data.kpis?.length && !selectedKpi) setSelectedKpi(res.data.kpis[0])
          setBuilding(false)
          return
        }
        if (s === 'error') {
          setError(res.data?.message || 'Training failed — check that KPI data is loaded.')
          setBuilding(false)
          return
        }
        // Never abort early on 'not_trained' — multi-worker deployments can
        // serve the poll from a different process that hasn't seen the build start.
        // Keep polling until we get 'ready', 'error', or the full 120-second timeout.
      }
      setError('Training is taking longer than expected. Click "Refresh Status" below to check if it completed.')
    } catch {
      setError('Failed to start training. Make sure data is loaded and try again.')
    }
    setBuilding(false)
  }

  async function handleRun() {
    if (!model || model.status !== 'ready') return
    setRunning(true)
    setError(null)
    try {
      const overrides = {}
      scenarios.forEach(({ kpi, state }) => { overrides[kpi] = state })
      const res = await axios.post('/api/forecast/project', {
        horizon_days: horizonDays,
        n_samples:    nSamples,
        overrides,
      })
      setResult(res.data)
      if (!selectedKpi && res.data.kpis?.length) setSelectedKpi(res.data.kpis[0])
    } catch (e) {
      setError('Projection failed. ' + (e.response?.data?.detail ?? ''))
    }
    setRunning(false)
  }

  function addScenario(kpi) {
    if (!kpi || scenarios.length >= MAX_SCENARIOS) return
    if (scenarios.find(s => s.kpi === kpi)) return
    const vr = model?.value_ranges?.[kpi]
    // Init slider at position closest to current value
    let initState = 2
    if (vr) {
      const cur = vr.current
      const thresholds = [vr.p10, vr.p25, vr.p50, vr.p75, vr.p90]
      for (let i = 0; i < thresholds.length; i++) {
        if (cur <= thresholds[i]) { initState = i; break }
        initState = 4
      }
    }
    setScenarios(prev => [...prev, { kpi, state: initState }])
    setAddingKpi(false)
  }

  function updateScenario(kpi, state) {
    setScenarios(prev => prev.map(s => s.kpi === kpi ? { ...s, state } : s))
  }

  function removeScenario(kpi) {
    setScenarios(prev => prev.filter(s => s.kpi !== kpi))
  }

  const kpis         = model?.kpis ?? []
  const valueRanges  = result?.value_ranges ?? model?.value_ranges ?? {}
  const trajectories = result?.trajectories ?? {}
  const causalPaths  = result?.causal_paths?.[selectedKpi] ?? []
  const chartData    = selectedKpi ? (trajectories[selectedKpi] ?? []) : []

  const availableToAdd = kpis.filter(k => !scenarios.find(s => s.kpi === k))

  const narrative = useMemo(() =>
    result
      ? buildNarrative(selectedKpi, trajectories[selectedKpi], causalPaths, horizonDays, scenarios, valueRanges)
      : null,
    [result, selectedKpi, causalPaths, horizonDays, scenarios, valueRanges]
  )

  // Y-axis domain with padding
  const yDomain = useMemo(() => {
    if (!chartData.length) return ['auto', 'auto']
    const vals = chartData.flatMap(d => [d.p10, d.p50, d.p90, d.hist_p10, d.hist_p90].filter(v => v != null))
    const mn = Math.min(...vals), mx = Math.max(...vals)
    const pad = (mx - mn) * 0.15 || Math.abs(mn) * 0.1 || 1
    return [mn - pad, mx + pad]
  }, [chartData])

  const histMedian = chartData[0]?.hist_p50

  return (
    <div className="flex flex-col gap-4 h-full min-h-0">

      {/* ── Plain-English intro card ─────────────────────── */}
      <div className="flex-shrink-0 rounded-xl border border-blue-100 bg-blue-50/60 px-5 py-4">
        <div className="flex items-start gap-3">
          <span className="text-blue-400 text-lg flex-shrink-0">📈</span>
          <div>
            <p className="text-sm font-semibold text-blue-900 mb-1">What this shows</p>
            <p className="text-xs text-blue-700 leading-relaxed">
              Select any KPI, set a time horizon, and run the projection. The chart shows three outcomes:
              the <strong>most likely path</strong> (solid line), an <strong>optimistic scenario</strong> (upper band),
              and a <strong>pessimistic scenario</strong> (lower band) — based on how this KPI has behaved historically.
              Use <strong>Scenario Inputs</strong> to pin a KPI at a specific value and see how it changes the outlook
              across connected metrics.
            </p>
          </div>
        </div>
      </div>

      <div className="flex gap-5 flex-1 min-h-0">

      {/* ── Left: Controls ─────────────────────────────────── */}
      <div className="w-72 flex-shrink-0 flex flex-col gap-3 overflow-y-auto pb-2">

        {/* Model Status */}
        <div className="card p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-semibold text-slate-700 text-sm flex items-center gap-2">
              <Zap size={14} style={{ color: ACCENT }} /> Forecast Engine
            </h3>
            <button onClick={loadModel} className="text-slate-400 hover:text-slate-600 transition-colors" title="Refresh">
              <RefreshCw size={13} />
            </button>
          </div>

          {modelLoading ? (
            <p className="text-xs text-slate-400">Checking…</p>
          ) : model?.status === 'ready' ? (
            <div className="space-y-1">
              <div className="flex items-center gap-2">
                <span className="w-2 h-2 rounded-full bg-emerald-400" />
                <span className="text-xs text-emerald-600 font-medium">Analysis engine ready</span>
              </div>
              <p className="text-[11px] text-slate-400">
                {model.kpis?.length} KPIs · {new Date(model.trained_at).toLocaleDateString()}
              </p>
              {model.regime_data?.current_regime && (
                <RegimeBadge regime={model.regime_data.current_regime} />
              )}
            </div>
          ) : (
            <div className="flex items-start gap-2 text-xs text-amber-600">
              <AlertCircle size={13} className="mt-0.5 flex-shrink-0" />
              <span>No model yet — click Build to train from KPI history.</span>
            </div>
          )}

          <button
            onClick={handleBuild}
            disabled={building}
            className="mt-3 w-full flex items-center justify-center gap-2 py-2 rounded-lg
                       text-xs font-medium border border-slate-200 bg-slate-50
                       hover:bg-slate-100 text-slate-600 disabled:opacity-50 transition-all"
          >
            {building
              ? <><div className="w-3 h-3 rounded-full border-2 border-slate-400 border-t-transparent animate-spin" />Training…</>
              : <><RefreshCw size={12} />Build / Retrain</>
            }
          </button>
        </div>

        {/* Projection Settings */}
        <div className="card p-4">
          <h3 className="font-semibold text-slate-700 text-sm mb-3 flex items-center gap-2">
            <TrendingUp size={14} style={{ color: ACCENT }} /> Projection Settings
          </h3>
          <div className="space-y-3">
            <div>
              <label className="text-[11px] font-medium text-slate-500 uppercase tracking-wide block mb-1">
                Horizon: {horizonDays} days ({Math.round(horizonDays / 30)}mo)
              </label>
              <input type="range" min={30} max={365} step={30} value={horizonDays}
                onChange={e => setHorizonDays(Number(e.target.value))}
                className="w-full accent-[#00AEEF]" />
              <div className="flex justify-between text-[9px] text-slate-300 mt-0.5">
                <span>1mo</span><span>6mo</span><span>12mo</span>
              </div>
            </div>
            <div>
              <label className="text-[11px] font-medium text-slate-500 uppercase tracking-wide block mb-1">
                Simulations: {nSamples}
              </label>
              <input type="range" min={100} max={1000} step={100} value={nSamples}
                onChange={e => setNSamples(Number(e.target.value))}
                className="w-full accent-[#00AEEF]" />
              <div className="flex justify-between text-[9px] text-slate-300 mt-0.5">
                <span>100</span><span>500</span><span>1000</span>
              </div>
            </div>
          </div>
        </div>

        {/* Scenario Inputs */}
        {model?.status === 'ready' && (
          <div className="card p-4">
            <div className="flex items-center justify-between mb-1">
              <div className="flex items-center gap-1.5">
                <h3 className="font-semibold text-slate-700 text-sm">Scenario Inputs</h3>
                <span title="Pin any KPI to a scenario value to model what-if conditions. Up to 5 simultaneous inputs."
                  className="text-slate-400 hover:text-slate-600 cursor-help">
                  <Info size={12} />
                </span>
              </div>
              {scenarios.length > 0 && (
                <button
                  onClick={() => setScenarios([])}
                  className="flex items-center gap-1 text-[10px] text-slate-400 hover:text-red-400 transition-colors"
                >
                  <RotateCcw size={10} /> Reset all
                </button>
              )}
            </div>
            <p className="text-[11px] text-slate-400 mb-3">
              Pin any KPI to a scenario level — changes propagate through the causal network. Up to {MAX_SCENARIOS} at once.
            </p>

            <div className="space-y-2 mb-3">
              {scenarios.map(({ kpi, state }) => (
                <ScenarioSlider
                  key={kpi}
                  kpi={kpi}
                  state={state}
                  valueRanges={valueRanges}
                  onChange={updateScenario}
                  onRemove={removeScenario}
                />
              ))}
            </div>

            {scenarios.length < MAX_SCENARIOS && (
              addingKpi ? (
                <div>
                  <select
                    autoFocus
                    defaultValue=""
                    onChange={e => addScenario(e.target.value)}
                    onBlur={() => setAddingKpi(false)}
                    className="w-full text-xs border border-slate-200 rounded-lg px-2 py-1.5 bg-white text-slate-600 focus:outline-none focus:border-[#00AEEF]"
                  >
                    <option value="" disabled>Select a KPI…</option>
                    {availableToAdd.map(k => (
                      <option key={k} value={k}>{formatKpiKey(k)}</option>
                    ))}
                  </select>
                </div>
              ) : (
                <button
                  onClick={() => setAddingKpi(true)}
                  disabled={availableToAdd.length === 0}
                  className="w-full flex items-center justify-center gap-1.5 py-2 rounded-lg border border-dashed border-slate-200 text-xs text-slate-400 hover:text-[#00AEEF] hover:border-[#00AEEF] transition-all disabled:opacity-30"
                >
                  <Plus size={12} /> Add scenario input
                </button>
              )
            )}
          </div>
        )}

        {/* Run Button */}
        <button
          onClick={handleRun}
          disabled={running || !model || model.status !== 'ready'}
          className="flex items-center justify-center gap-2 py-3 rounded-xl text-sm font-semibold
                     text-white transition-all disabled:opacity-40"
          style={{ backgroundColor: ACCENT }}
        >
          {running
            ? <><div className="w-4 h-4 rounded-full border-2 border-white/40 border-t-white animate-spin" />Running…</>
            : <><Play size={14} fill="white" />Run Projection</>
          }
        </button>

        {error && (
          <div className="flex items-start gap-2 text-xs text-red-600 bg-red-50 border border-red-200 rounded-lg p-3">
            <AlertCircle size={13} className="mt-0.5 flex-shrink-0" />{error}
          </div>
        )}
      </div>

      {/* ── Right: Chart + Narrative ────────────────────────── */}
      <div className="flex-1 flex flex-col gap-4 min-w-0 overflow-y-auto pb-2">

        {/* KPI Selector */}
        {kpis.length > 0 && (
          <div className="card p-4">
            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-3">Select Signal to View</h3>
            <div className="flex flex-wrap gap-2">
              {kpis.map(kpi => {
                const isActive   = kpi === selectedKpi
                const hasScenario = scenarios.some(s => s.kpi === kpi)
                const vr = valueRanges?.[kpi]
                return (
                  <button
                    key={kpi}
                    onClick={() => setSelectedKpi(kpi)}
                    className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium
                                transition-all border ${
                      isActive
                        ? 'text-white border-transparent'
                        : 'bg-white text-slate-600 border-slate-200 hover:border-slate-300'
                    }`}
                    style={isActive ? { backgroundColor: ACCENT, borderColor: ACCENT } : {}}
                  >
                    {vr && result && (() => {
                      const traj = trajectories[kpi] ?? []
                      const last = traj.at(-1)
                      const dir  = last ? last.p50 - traj[0]?.p50 : 0
                      const good = LOWER_BETTER.has(kpi) ? dir < 0 : dir > 0
                      return <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${
                        Math.abs(dir / (Math.abs(traj[0]?.p50) || 1)) > 0.015
                          ? good ? 'bg-emerald-400' : 'bg-red-400'
                          : 'bg-slate-300'
                      }`} />
                    })()}
                    {!result && <span className="w-1.5 h-1.5 rounded-full flex-shrink-0 bg-slate-200" />}
                    {formatKpiKey(kpi)}
                    {hasScenario && (
                      <span className="text-[8px] px-1 py-0.5 rounded uppercase tracking-wide"
                        style={{ backgroundColor: isActive ? 'rgba(255,255,255,0.25)' : ACCENT + '22', color: isActive ? 'white' : ACCENT }}>
                        pinned
                      </span>
                    )}
                  </button>
                )
              })}
            </div>
          </div>
        )}

        {/* Trajectory Chart */}
        <div className="card p-5 flex-shrink-0">
          {!result ? (
            <div className="flex flex-col items-center justify-center h-64 gap-3 text-center">
              <div className="w-14 h-14 rounded-2xl flex items-center justify-center" style={{ backgroundColor: ACCENT + '15' }}>
                <TrendingUp size={26} style={{ color: ACCENT }} />
              </div>
              <div>
                <p className="font-semibold text-slate-600 mb-1">Signal Trajectory Projection</p>
                <p className="text-sm text-slate-400 max-w-sm">
                  {model?.status !== 'ready'
                    ? 'Build the engine first, then run a projection.'
                    : 'Add scenario inputs if needed, then click Run Projection.'}
                </p>
              </div>
            </div>
          ) : (
            <>
              <div className="flex items-start justify-between mb-4">
                <div>
                  <h3 className="font-semibold text-slate-700">
                    {formatKpiKey(selectedKpi ?? '')} — {Math.round(horizonDays / 30)}-Month Projection
                  </h3>
                  <p className="text-xs text-slate-400 mt-0.5">
                    {nSamples} simulated paths · showing likely range (p10–p90) and most probable outcome (median)
                  </p>
                  {result?.regime_data?.available && (
                    <RegimeBadge regime={result.regime_data} />
                  )}
                </div>
                <div className="flex items-center gap-4 text-[11px] text-slate-400 flex-shrink-0">
                  <span className="flex items-center gap-1.5">
                    <span className="w-8 h-0.5 rounded inline-block" style={{ backgroundColor: ACCENT + '55' }} />
                    p10–p90 band
                  </span>
                  <span className="flex items-center gap-1.5">
                    <span className="w-8 h-0.5 rounded inline-block" style={{ backgroundColor: ACCENT }} />
                    median
                  </span>
                  {histMedian != null && (
                    <span className="flex items-center gap-1.5">
                      <span className="w-8 border-t border-dashed border-slate-300 inline-block" />
                      hist. median
                    </span>
                  )}
                </div>
              </div>

              <ResponsiveContainer width="100%" height={260}>
                <ComposedChart data={chartData} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                  <XAxis dataKey="label" tick={{ fontSize: 11, fill: '#94a3b8' }} axisLine={false} tickLine={false} />
                  <YAxis
                    domain={yDomain}
                    tickFormatter={v => fmtVal(selectedKpi, v)}
                    tick={{ fontSize: 10, fill: '#94a3b8' }}
                    axisLine={false} tickLine={false} width={64}
                  />
                  <Tooltip content={<ChartTooltip kpi={selectedKpi} />} />

                  {/* Historical median reference */}
                  {histMedian != null && (
                    <ReferenceLine
                      y={histMedian}
                      stroke="#cbd5e1"
                      strokeDasharray="5 4"
                      label={{ value: 'hist. median', position: 'insideTopRight', fontSize: 9, fill: '#94a3b8' }}
                    />
                  )}

                  {/* Confidence band — p90 filled, p10 overdrawn white */}
                  <Area type="monotone" dataKey="p90" stroke="none"
                    fill={ACCENT} fillOpacity={0.12} legendType="none" />
                  <Area type="monotone" dataKey="p10" stroke="none"
                    fill="#ffffff" fillOpacity={1} legendType="none" />

                  {/* Median */}
                  <Line type="monotone" dataKey="p50" stroke={ACCENT} strokeWidth={2.5}
                    dot={{ r: 3, fill: ACCENT, strokeWidth: 0 }} activeDot={{ r: 5 }} name="Median" />
                </ComposedChart>
              </ResponsiveContainer>
            </>
          )}
        </div>

        {/* Narrative */}
        {narrative && <NarrativePanel narrative={narrative} />}

        {/* Causal Drivers */}
        {causalPaths.length > 0 && (
          <div className="card p-4">
            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-3">
              Causal Drivers → {formatKpiKey(selectedKpi ?? '')}
            </h3>
            <div className="flex flex-wrap gap-3">
              {causalPaths.map((cp, i) => {
                const srcScenario = scenarios.find(s => s.kpi === cp.from)
                return (
                  <div key={i} className="flex items-center gap-2 px-3 py-2 rounded-lg border border-slate-100 bg-slate-50 text-sm">
                    <span className="font-medium text-slate-600">{formatKpiKey(cp.from)}</span>
                    {srcScenario && (
                      <span className="text-[9px] px-1 py-0.5 rounded uppercase"
                        style={{ backgroundColor: ACCENT + '18', color: ACCENT }}>
                        {STATE_LABELS[srcScenario.state]}
                      </span>
                    )}
                    <ChevronRight size={12} className="text-slate-300" />
                    <span className="text-slate-400 text-xs">{formatKpiKey(selectedKpi ?? '')}</span>
                    <span className="ml-1 text-[10px] font-bold px-1.5 py-0.5 rounded"
                      style={{ backgroundColor: ACCENT + '18', color: ACCENT }}>
                      {(cp.strength * 100).toFixed(0)}%
                    </span>
                  </div>
                )
              })}
            </div>
          </div>
        )}

        {/* KPI summary cards */}
        {result?.status === 'ok' && (
          <div className="grid grid-cols-4 gap-3">
            {kpis.slice(0, 4).map(kpi => {
              const traj  = trajectories[kpi] ?? []
              const last  = traj.at(-1)
              const first = traj[0]
              if (!last || !first) return null
              const delta      = last.p50 - first.p50
              const pct        = first.p50 !== 0 ? (delta / Math.abs(first.p50)) * 100 : 0
              const businessUp = LOWER_BETTER.has(kpi) ? delta < 0 : delta > 0
              const meaningful = Math.abs(pct) > 1.5
              return (
                <button key={kpi} onClick={() => setSelectedKpi(kpi)}
                  className={`card p-3 text-left transition-all hover:shadow-md ${kpi === selectedKpi ? 'ring-2 ring-[#00AEEF]' : ''}`}>
                  <p className="text-[10px] text-slate-400 mb-1 truncate">{formatKpiKey(kpi)}</p>
                  <p className="text-sm font-bold text-slate-700">{fmtVal(kpi, last.p50)}</p>
                  {meaningful && (
                    <p className={`text-[10px] font-medium mt-0.5 ${businessUp ? 'text-emerald-500' : 'text-red-400'}`}>
                      {businessUp ? '↑' : '↓'} {Math.abs(pct).toFixed(1)}%
                    </p>
                  )}
                  {!meaningful && <p className="text-[10px] text-slate-300 mt-0.5">→ stable</p>}
                </button>
              )
            })}
          </div>
        )}
      </div>
      </div>  {/* closes flex gap-5 flex-1 */}
    </div>
  )
}
