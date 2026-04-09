import { useState, useEffect } from 'react'
import axios from 'axios'
import {
  Target, Plus, X, ChevronDown, ChevronUp, Trash2,
  CheckCircle2, AlertTriangle, Link2, Save,
} from 'lucide-react'

const STATUS_STYLES = {
  on_track:  { bg: 'bg-emerald-50', border: 'border-emerald-200', text: 'text-emerald-700', label: 'On Track' },
  at_risk:   { bg: 'bg-amber-50',   border: 'border-amber-200',   text: 'text-amber-700',   label: 'At Risk' },
  behind:    { bg: 'bg-red-50',     border: 'border-red-200',     text: 'text-red-700',     label: 'Behind' },
  active:    { bg: 'bg-blue-50',    border: 'border-blue-200',    text: 'text-blue-700',    label: 'Active' },
  completed: { bg: 'bg-emerald-50', border: 'border-emerald-200', text: 'text-emerald-700', label: 'Completed' },
}

function ProgressBar({ pct, size = 'md' }) {
  const h = size === 'sm' ? 'h-1.5' : 'h-2'
  const color = pct >= 70 ? 'bg-emerald-500' : pct >= 40 ? 'bg-amber-400' : 'bg-red-500'
  return (
    <div className={`${h} rounded-full bg-slate-100 overflow-hidden`}>
      <div className={`${h} ${color} rounded-full transition-all`} style={{ width: `${Math.min(pct, 100)}%` }} />
    </div>
  )
}

export default function OKRPage({ fingerprint }) {
  const [objectives, setObjectives] = useState([])
  const [loading, setLoading] = useState(true)
  const [showForm, setShowForm] = useState(false)
  const [expanded, setExpanded] = useState(null)
  const [form, setForm] = useState({ title: '', description: '', owner: '', quarter: '', confidence: 50 })
  const [krForm, setKrForm] = useState({ title: '', kpi_key: '', target_value: '', unit: '' })
  const [addingKrFor, setAddingKrFor] = useState(null)

  const availableKpis = (fingerprint || []).map(k => k.key).filter(Boolean)

  useEffect(() => { load() }, []) // eslint-disable-line

  async function load() {
    try {
      const r = await axios.get('/api/okrs')
      setObjectives(r.data?.objectives || [])
    } catch { } finally { setLoading(false) }
  }

  async function createObjective(e) {
    e.preventDefault()
    if (!form.title.trim()) return
    await axios.post('/api/okrs/objectives', form).catch(() => {})
    setForm({ title: '', description: '', owner: '', quarter: '', confidence: 50 })
    setShowForm(false)
    load()
  }

  async function deleteObjective(id) {
    if (!window.confirm('Delete this objective and all key results?')) return
    await axios.delete(`/api/okrs/objectives/${id}`).catch(() => {})
    load()
  }

  async function addKeyResult(objId) {
    if (!krForm.title.trim()) return
    await axios.post('/api/okrs/key-results', { ...krForm, objective_id: objId, target_value: parseFloat(krForm.target_value) || null }).catch(() => {})
    setKrForm({ title: '', kpi_key: '', target_value: '', unit: '' })
    setAddingKrFor(null)
    load()
  }

  async function deleteKr(id) {
    await axios.delete(`/api/okrs/key-results/${id}`).catch(() => {})
    load()
  }

  async function updateConfidence(objId, confidence) {
    await axios.put(`/api/okrs/objectives/${objId}`, { confidence }).catch(() => {})
    load()
  }

  if (loading) return <div className="flex items-center justify-center h-40 text-slate-400 text-sm">Loading OKRs...</div>

  return (
    <div className="max-w-6xl mx-auto px-6 py-6 space-y-5">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-xl font-bold text-slate-900 flex items-center gap-2">
            <Target size={20} className="text-[#0055A4]" />
            Objectives & Key Results
          </h1>
          <p className="text-[13px] text-slate-500 mt-0.5">
            Set strategic objectives, link key results to KPIs, and track progress automatically.
          </p>
        </div>
        <button onClick={() => setShowForm(v => !v)}
          className="flex items-center gap-1.5 bg-[#0055A4] text-white text-sm font-semibold px-4 py-2 rounded-xl hover:bg-blue-700 transition-colors">
          <Plus size={14} /> New Objective
        </button>
      </div>

      {/* New objective form */}
      {showForm && (
        <form onSubmit={createObjective} className="bg-white rounded-2xl border border-blue-100 shadow-sm p-5 space-y-3">
          <div className="grid grid-cols-2 gap-3">
            <div className="col-span-2">
              <label className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Objective *</label>
              <input className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" placeholder="e.g., Achieve product-market fit in Q2"
                value={form.title} onChange={e => setForm(v => ({ ...v, title: e.target.value }))} />
            </div>
            <div>
              <label className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Owner</label>
              <input className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" placeholder="CEO"
                value={form.owner} onChange={e => setForm(v => ({ ...v, owner: e.target.value }))} />
            </div>
            <div>
              <label className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Quarter</label>
              <input className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm" placeholder="Q2 2026"
                value={form.quarter} onChange={e => setForm(v => ({ ...v, quarter: e.target.value }))} />
            </div>
          </div>
          <div className="flex justify-end gap-2">
            <button type="button" onClick={() => setShowForm(false)} className="text-sm text-slate-500 px-4 py-2">Cancel</button>
            <button type="submit" className="bg-[#0055A4] text-white text-sm font-semibold px-5 py-2 rounded-xl hover:bg-blue-700">Save</button>
          </div>
        </form>
      )}

      {/* Objectives list */}
      {objectives.length === 0 ? (
        <div className="bg-white rounded-2xl border border-slate-100 shadow-sm p-14 text-center">
          <Target size={36} className="text-slate-200 mx-auto mb-3" />
          <p className="text-slate-600 font-semibold text-sm">No objectives set</p>
          <p className="text-slate-400 text-xs mt-1">Create objectives and link key results to your KPIs for automatic progress tracking.</p>
        </div>
      ) : (
        <div className="space-y-4">
          {objectives.map(obj => {
            const isExp = expanded === obj.id
            const krs = obj.key_results || []
            const progress = obj.progress_pct || 0
            return (
              <div key={obj.id} className="bg-white rounded-2xl border border-slate-100 shadow-sm overflow-hidden">
                {/* Objective header */}
                <div className="px-5 py-4 cursor-pointer hover:bg-slate-50/60" onClick={() => setExpanded(isExp ? null : obj.id)}>
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-3">
                      <span className="text-[14px] font-bold text-slate-800">{obj.title}</span>
                      {obj.quarter && <span className="text-[10px] font-medium text-slate-400 bg-slate-100 px-2 py-0.5 rounded-full">{obj.quarter}</span>}
                    </div>
                    <div className="flex items-center gap-3">
                      <span className="text-[12px] font-bold text-slate-600">{progress.toFixed(0)}%</span>
                      {isExp ? <ChevronUp size={14} className="text-slate-400" /> : <ChevronDown size={14} className="text-slate-400" />}
                    </div>
                  </div>
                  <ProgressBar pct={progress} />
                  <div className="flex items-center gap-3 mt-2 text-[10px] text-slate-400">
                    {obj.owner && <span>{obj.owner}</span>}
                    <span>{krs.length} key result{krs.length !== 1 ? 's' : ''}</span>
                    <span>Confidence: {obj.confidence}%</span>
                  </div>
                </div>

                {/* Expanded: Key Results */}
                {isExp && (
                  <div className="px-5 pb-5 border-t border-slate-50 space-y-3 pt-3">
                    {krs.map(kr => (
                      <div key={kr.id} className="flex items-center gap-3 bg-slate-50 rounded-xl px-4 py-3">
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 mb-1">
                            <span className="text-[11px] font-semibold text-slate-700">{kr.title}</span>
                            {kr.kpi_key && (
                              <span className="text-[9px] font-mono text-blue-500 bg-blue-50 px-1.5 py-0.5 rounded border border-blue-100">
                                <Link2 size={8} className="inline mr-0.5" />{kr.kpi_key}
                              </span>
                            )}
                          </div>
                          <div className="flex items-center gap-3 text-[10px] text-slate-500">
                            {kr.current_value != null && <span>Current: {kr.current_value}</span>}
                            {kr.target_value != null && <span>Target: {kr.target_value}</span>}
                            <span>{(kr.progress_pct || 0).toFixed(0)}%</span>
                          </div>
                          <ProgressBar pct={kr.progress_pct || 0} size="sm" />
                        </div>
                        <button onClick={() => deleteKr(kr.id)} className="text-slate-300 hover:text-red-400 shrink-0">
                          <Trash2 size={12} />
                        </button>
                      </div>
                    ))}

                    {/* Add KR form */}
                    {addingKrFor === obj.id ? (
                      <div className="bg-blue-50/60 rounded-xl p-3 border border-blue-100 space-y-2">
                        <div className="grid grid-cols-2 gap-2">
                          <input className="col-span-2 border border-slate-200 rounded-lg px-3 py-1.5 text-sm" placeholder="Key result title"
                            value={krForm.title} onChange={e => setKrForm(v => ({ ...v, title: e.target.value }))} />
                          <select className="border border-slate-200 rounded-lg px-3 py-1.5 text-sm"
                            value={krForm.kpi_key} onChange={e => setKrForm(v => ({ ...v, kpi_key: e.target.value }))}>
                            <option value="">Link to KPI (optional)</option>
                            {availableKpis.map(k => <option key={k} value={k}>{k}</option>)}
                          </select>
                          <input className="border border-slate-200 rounded-lg px-3 py-1.5 text-sm" placeholder="Target value"
                            type="number" value={krForm.target_value} onChange={e => setKrForm(v => ({ ...v, target_value: e.target.value }))} />
                        </div>
                        <div className="flex justify-end gap-2">
                          <button onClick={() => setAddingKrFor(null)} className="text-xs text-slate-500 px-3 py-1">Cancel</button>
                          <button onClick={() => addKeyResult(obj.id)} className="text-xs bg-[#0055A4] text-white font-semibold px-3 py-1 rounded-lg">Add</button>
                        </div>
                      </div>
                    ) : (
                      <button onClick={() => setAddingKrFor(obj.id)}
                        className="flex items-center gap-1.5 text-[11px] text-[#0055A4] font-semibold hover:underline">
                        <Plus size={11} /> Add Key Result
                      </button>
                    )}

                    {/* Confidence slider + delete */}
                    <div className="flex items-center justify-between pt-2 border-t border-slate-100">
                      <div className="flex items-center gap-2">
                        <span className="text-[10px] text-slate-400">Confidence:</span>
                        <input type="range" min="0" max="100" value={obj.confidence}
                          onChange={e => updateConfidence(obj.id, parseInt(e.target.value))}
                          className="w-24 h-1 accent-[#0055A4]" />
                        <span className="text-[10px] font-bold text-slate-600">{obj.confidence}%</span>
                      </div>
                      <button onClick={() => deleteObjective(obj.id)} className="text-[10px] text-slate-300 hover:text-red-400 flex items-center gap-1">
                        <Trash2 size={10} /> Delete
                      </button>
                    </div>
                  </div>
                )}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
