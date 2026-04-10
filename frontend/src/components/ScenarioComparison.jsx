import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import { GitCompare, RefreshCw, Check, AlertTriangle, ChevronDown, ChevronUp } from 'lucide-react'

// ── Helpers ────────────────────────────────────────────────────────────────
function fmtLeverValue(val) {
  if (val == null) return '\u2014'
  const n = Number(val)
  if (!Number.isFinite(n)) return String(val)
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toFixed(1)}%`
}

function leverColor(val) {
  if (val == null) return 'text-slate-500'
  const n = Number(val)
  if (n > 0) return 'text-emerald-400'
  if (n < 0) return 'text-red-400'
  return 'text-slate-500'
}

// ── Scenario selection checkbox ────────────────────────────────────────────
function ScenarioCheckbox({ scenario, checked, disabled, onChange }) {
  return (
    <label
      className={`flex items-center gap-2.5 px-3 py-2.5 rounded-lg border transition-colors cursor-pointer ${
        checked
          ? 'bg-[#0055A4]/15 border-[#0055A4]/40 text-slate-800'
          : disabled
          ? 'bg-slate-50/50 border-slate-100 text-slate-500 cursor-not-allowed'
          : 'bg-slate-50 border-slate-200 text-slate-500 hover:border-slate-300'
      }`}
    >
      <input
        type="checkbox"
        checked={checked}
        disabled={disabled}
        onChange={onChange}
        className="sr-only"
      />
      <span className={`w-4 h-4 rounded border flex items-center justify-center shrink-0 ${
        checked
          ? 'bg-[#0055A4] border-[#0055A4]'
          : 'border-slate-200 bg-transparent'
      }`}>
        {checked && <Check size={10} className="text-white" />}
      </span>
      <div className="min-w-0">
        <p className="text-sm font-medium truncate">{scenario.name}</p>
        {scenario.notes && (
          <p className="text-[10px] text-slate-500 truncate mt-0.5">{scenario.notes}</p>
        )}
      </div>
    </label>
  )
}

// ── Main component ─────────────────────────────────────────────────────────
export default function ScenarioComparison() {
  const [scenarios, setScenarios]   = useState([])
  const [selected, setSelected]     = useState([])
  const [comparison, setComparison] = useState(null)
  const [loading, setLoading]       = useState(true)
  const [comparing, setComparing]   = useState(false)
  const [error, setError]           = useState('')
  const [expanded, setExpanded]     = useState(true)

  // Fetch scenario list
  const loadScenarios = useCallback(async () => {
    setLoading(true); setError('')
    try {
      const { data } = await axios.get('/api/scenarios')
      const list = Array.isArray(data) ? data : (data?.scenarios || [])
      setScenarios(list)
    } catch (e) {
      setError(e.response?.data?.detail || 'Failed to load scenarios')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { loadScenarios() }, [loadScenarios])

  // Fetch comparison when selection changes
  const loadComparison = useCallback(async (ids) => {
    if (ids.length < 2) { setComparison(null); return }
    setComparing(true)
    try {
      const { data } = await axios.get(`/api/scenarios/compare?ids=${ids.join(',')}`)
      setComparison(data)
    } catch (e) {
      setComparison(null)
    } finally {
      setComparing(false)
    }
  }, [])

  function toggleScenario(id) {
    setSelected(prev => {
      let next
      if (prev.includes(id)) {
        next = prev.filter(x => x !== id)
      } else if (prev.length < 4) {
        next = [...prev, id]
      } else {
        return prev
      }
      loadComparison(next)
      return next
    })
  }

  // ── Loading state ────────────────────────────────────────────────────────
  if (loading) return (
    <div className="flex items-center justify-center h-64">
      <RefreshCw size={20} className="animate-spin text-[#0055A4]" />
    </div>
  )

  // ── Error state ──────────────────────────────────────────────────────────
  if (error) return (
    <div className="p-6 max-w-5xl mx-auto">
      <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-4 text-red-400 text-sm">{error}</div>
    </div>
  )

  // ── Empty state ──────────────────────────────────────────────────────────
  if (scenarios.length === 0) return (
    <div className="p-6 max-w-5xl mx-auto space-y-5">
      <h2 className="text-slate-800 text-xl font-semibold flex items-center gap-2">
        <GitCompare size={20} className="text-[#0055A4]" />
        Scenario Comparison
      </h2>
      <div className="bg-white border border-slate-200 rounded-2xl p-12 text-center">
        <AlertTriangle size={32} className="text-slate-500 mx-auto mb-3" />
        <p className="text-slate-500 text-sm font-semibold">No saved scenarios</p>
        <p className="text-slate-500 text-xs mt-1">Create scenarios in the Scenario Planner to compare them here.</p>
      </div>
    </div>
  )

  const compScenarios  = comparison?.scenarios || []
  const compRows       = comparison?.comparison || []

  return (
    <div className="p-6 max-w-5xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-slate-800 text-xl font-semibold flex items-center gap-2">
            <GitCompare size={20} className="text-[#0055A4]" />
            Scenario Comparison
          </h2>
          <p className="text-slate-500 text-sm mt-1">
            Select 2\u20134 scenarios to compare lever adjustments side by side
          </p>
        </div>
        <button
          onClick={loadScenarios}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-slate-500 hover:text-slate-800 bg-slate-50 hover:bg-slate-100 rounded-lg transition-colors"
        >
          <RefreshCw size={12} /> Refresh
        </button>
      </div>

      {/* Scenario selector */}
      <div className="bg-white border border-slate-200 rounded-2xl p-5">
        <button
          onClick={() => setExpanded(e => !e)}
          className="flex items-center justify-between w-full text-left"
        >
          <div>
            <h3 className="text-slate-800 text-sm font-semibold">Select Scenarios</h3>
            <p className="text-slate-500 text-xs mt-0.5">
              {selected.length} of {scenarios.length} selected (min 2, max 4)
            </p>
          </div>
          {expanded ? <ChevronUp size={16} className="text-slate-500" /> : <ChevronDown size={16} className="text-slate-500" />}
        </button>

        {expanded && (
          <div className="grid grid-cols-2 sm:grid-cols-3 gap-2 mt-4">
            {scenarios.map(s => (
              <ScenarioCheckbox
                key={s.id}
                scenario={s}
                checked={selected.includes(s.id)}
                disabled={!selected.includes(s.id) && selected.length >= 4}
                onChange={() => toggleScenario(s.id)}
              />
            ))}
          </div>
        )}
      </div>

      {/* Comparison table */}
      {selected.length < 2 && (
        <div className="bg-slate-50 border border-slate-100 rounded-xl px-4 py-8 text-center">
          <p className="text-slate-500 text-sm">Select at least 2 scenarios to see the comparison table.</p>
        </div>
      )}

      {comparing && (
        <div className="flex items-center justify-center h-32">
          <RefreshCw size={18} className="animate-spin text-[#0055A4]" />
          <span className="text-slate-500 text-sm ml-2">Loading comparison...</span>
        </div>
      )}

      {!comparing && compRows.length > 0 && (
        <div className="bg-white border border-slate-200 rounded-2xl overflow-hidden">
          <div className="p-4 border-b border-slate-100">
            <h3 className="text-slate-800 text-sm font-semibold">Lever Comparison</h3>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-100">
                  <th className="text-left px-4 py-3 text-slate-500 font-medium text-xs uppercase tracking-wider">
                    Lever
                  </th>
                  {compScenarios.map(s => (
                    <th key={s.id} className="text-center px-4 py-3 text-slate-500 font-medium text-xs uppercase tracking-wider">
                      {s.name}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {compRows.map((row, i) => {
                  // Extract scenario values dynamically
                  const scenarioKeys = compScenarios.map(s => `scenario_${s.id}`)
                  return (
                    <tr key={i} className={`border-b border-slate-50 ${i % 2 === 0 ? 'bg-slate-50/50' : ''}`}>
                      <td className="px-4 py-3 text-slate-700 font-medium">
                        {(row.lever || '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}
                      </td>
                      {scenarioKeys.map((key, j) => {
                        const val = row[key]
                        return (
                          <td key={j} className={`px-4 py-3 text-center font-semibold ${leverColor(val)}`}>
                            {fmtLeverValue(val)}
                          </td>
                        )
                      })}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>

          {/* Scenario notes */}
          {compScenarios.some(s => s.notes) && (
            <div className="p-4 border-t border-slate-100 space-y-2">
              <p className="text-slate-500 text-xs font-medium uppercase tracking-wider">Notes</p>
              {compScenarios.filter(s => s.notes).map(s => (
                <div key={s.id} className="flex items-start gap-2 text-xs">
                  <span className="text-[#0055A4] font-semibold shrink-0">{s.name}:</span>
                  <span className="text-slate-500">{s.notes}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
