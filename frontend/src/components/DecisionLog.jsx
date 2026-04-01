import { useState, useEffect } from 'react'
import axios from 'axios'
import {
  BookMarked, Plus, X, ChevronDown, ChevronUp, Trash2,
  CheckCircle2, TrendingUp, User, Calendar, FileText, Lightbulb,
  RotateCcw,
} from 'lucide-react'

const STATUS_STYLES = {
  active:   { badge: 'bg-blue-50 text-blue-700 border border-blue-200',     label: 'Active'    },
  resolved: { badge: 'bg-emerald-50 text-emerald-700 border border-emerald-200', label: 'Resolved'  },
  reversed: { badge: 'bg-amber-50 text-amber-700 border border-amber-200',  label: 'Reversed'  },
}

function fmt(dtStr) {
  if (!dtStr) return ''
  return dtStr.slice(0, 10)
}

export default function DecisionLog({ authToken, fingerprint, prefillDecision, onPrefillConsumed }) {
  const [decisions, setDecisions] = useState([])
  const [loading, setLoading]     = useState(true)
  const [showForm, setShowForm]   = useState(false)
  const [expanded, setExpanded]   = useState(null)
  const [form, setForm]           = useState({
    title: '', the_decision: '', rationale: '', decided_by: 'CFO', kpi_context: [],
  })
  const [saving, setSaving]       = useState(false)
  const [formError, setFormError] = useState('')
  const [outcomeInput, setOutcomeInput] = useState({})

  // Pre-fill from Scenario Planner "Push to Decision Log"
  useEffect(() => {
    if (prefillDecision) {
      setForm({
        title:        prefillDecision.title        || '',
        the_decision: prefillDecision.the_decision || '',
        rationale:    prefillDecision.rationale    || '',
        decided_by:   prefillDecision.decided_by   || 'CFO',
        kpi_context:  prefillDecision.kpi_context  || [],
      })
      setShowForm(true)
      onPrefillConsumed?.()
    }
  }, [prefillDecision]) // eslint-disable-line react-hooks/exhaustive-deps

  const headers = authToken ? { Authorization: `Bearer ${authToken}` } : {}

  useEffect(() => { load() }, []) // eslint-disable-line react-hooks/exhaustive-deps

  async function load() {
    try {
      const r = await axios.get('/api/decisions', { headers })
      setDecisions(r.data.decisions || [])
    } catch (e) {
      console.error('Decision log fetch error', e)
    } finally {
      setLoading(false)
    }
  }

  async function handleSubmit(e) {
    e.preventDefault()
    setFormError('')
    if (!form.title.trim() || !form.the_decision.trim()) {
      setFormError('Decision title and description are required.')
      return
    }
    setSaving(true)
    try {
      await axios.post('/api/decisions', form, { headers })
      setForm({ title: '', the_decision: '', rationale: '', decided_by: 'CFO', kpi_context: [] })
      setShowForm(false)
      load()
    } catch {
      setFormError('Failed to save. Please try again.')
    } finally {
      setSaving(false)
    }
  }

  async function handleDelete(id) {
    if (!window.confirm('Remove this decision log entry?')) return
    await axios.delete(`/api/decisions/${id}`, { headers })
    load()
  }

  async function handleStatusUpdate(id, status) {
    const outcome = (outcomeInput[id] || '').trim()
    await axios.put(`/api/decisions/${id}`, { status, outcome }, { headers })
    setOutcomeInput(v => ({ ...v, [id]: '' }))
    load()
  }

  // KPI keys available to link
  const availableKpis = (fingerprint || []).map(k => k.key).filter(Boolean)

  if (loading) {
    return (
      <div className="flex items-center justify-center h-40 text-slate-400 text-sm">
        Loading decision log...
      </div>
    )
  }

  return (
    <div className="max-w-4xl mx-auto px-6 py-6 space-y-5">

      {/* ── Header ── */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-xl font-bold text-slate-900 flex items-center gap-2">
            <BookMarked size={20} className="text-[#0055A4]" />
            Decision Log
          </h1>
          <p className="text-[13px] text-slate-500 mt-0.5 max-w-lg">
            Preserve the reasoning behind key financial decisions. Track what was decided, why,
            and what happened after.
          </p>
        </div>
        <button
          onClick={() => setShowForm(v => !v)}
          className="flex items-center gap-1.5 bg-[#0055A4] text-white text-sm font-semibold px-4 py-2 rounded-xl hover:bg-blue-700 transition-colors shrink-0 mt-1"
        >
          <Plus size={14} />
          Log Decision
        </button>
      </div>

      {/* ── Stats bar ── */}
      {decisions.length > 0 && (
        <div className="grid grid-cols-3 gap-3">
          {[
            { label: 'Total Decisions', value: decisions.length, color: 'text-slate-800' },
            { label: 'Active', value: decisions.filter(d => d.status === 'active').length, color: 'text-blue-700' },
            { label: 'Resolved', value: decisions.filter(d => d.status === 'resolved').length, color: 'text-emerald-700' },
          ].map(s => (
            <div key={s.label} className="bg-white rounded-xl border border-slate-100 px-4 py-3 text-center shadow-sm">
              <p className={`text-xl font-bold ${s.color}`}>{s.value}</p>
              <p className="text-[11px] text-slate-500 mt-0.5">{s.label}</p>
            </div>
          ))}
        </div>
      )}

      {/* ── New decision form ── */}
      {showForm && (
        <form
          onSubmit={handleSubmit}
          className="bg-white rounded-2xl border border-blue-100 shadow-sm p-5 space-y-4"
        >
          <div className="flex items-center justify-between">
            <p className="text-sm font-bold text-slate-800">New Decision Entry</p>
            <button type="button" onClick={() => setShowForm(false)} className="text-slate-300 hover:text-slate-500">
              <X size={14} />
            </button>
          </div>

          <div>
            <label className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">
              Decision Title *
            </label>
            <input
              className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm text-slate-800 focus:outline-none focus:border-[#0055A4] focus:ring-1 focus:ring-[#0055A4]/20"
              placeholder="e.g., Shift to annual billing model, Hire 3 AEs in Q3"
              value={form.title}
              onChange={e => setForm(v => ({ ...v, title: e.target.value }))}
            />
          </div>

          <div>
            <label className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">
              The Decision *
            </label>
            <textarea
              className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm text-slate-800 focus:outline-none focus:border-[#0055A4] focus:ring-1 focus:ring-[#0055A4]/20 resize-none"
              rows={3}
              placeholder="State exactly what was decided. Be specific enough that someone reading this in 12 months understands without context."
              value={form.the_decision}
              onChange={e => setForm(v => ({ ...v, the_decision: e.target.value }))}
            />
          </div>

          <div>
            <label className="text-[10px] font-bold text-slate-500 uppercase tracking-widest flex items-center gap-1">
              <Lightbulb size={9} /> Rationale &amp; Context
            </label>
            <textarea
              className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm text-slate-800 focus:outline-none focus:border-[#0055A4] focus:ring-1 focus:ring-[#0055A4]/20 resize-none"
              rows={4}
              placeholder="Why was this decided? What signals or KPIs prompted it? What alternatives were considered and why were they rejected?"
              value={form.rationale}
              onChange={e => setForm(v => ({ ...v, rationale: e.target.value }))}
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">
                Decision Maker
              </label>
              <input
                className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm text-slate-800 focus:outline-none focus:border-[#0055A4]"
                placeholder="CFO"
                value={form.decided_by}
                onChange={e => setForm(v => ({ ...v, decided_by: e.target.value }))}
              />
            </div>
            <div>
              <label className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">
                Linked KPIs
              </label>
              <select
                className="mt-1 w-full border border-slate-200 rounded-lg px-3 py-2 text-sm text-slate-800 focus:outline-none focus:border-[#0055A4]"
                multiple
                size={3}
                value={form.kpi_context}
                onChange={e =>
                  setForm(v => ({ ...v, kpi_context: [...e.target.selectedOptions].map(o => o.value) }))
                }
              >
                {availableKpis.map(k => (
                  <option key={k} value={k}>{k}</option>
                ))}
              </select>
              <p className="text-[10px] text-slate-400 mt-0.5">Ctrl/Cmd + click for multiple</p>
            </div>
          </div>

          {formError && <p className="text-xs text-red-600 font-medium">{formError}</p>}

          <div className="flex justify-end gap-2 pt-1">
            <button
              type="button"
              onClick={() => setShowForm(false)}
              className="text-sm text-slate-500 hover:text-slate-700 px-4 py-2"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={saving}
              className="bg-[#0055A4] text-white text-sm font-semibold px-5 py-2 rounded-xl hover:bg-blue-700 disabled:opacity-50 transition-colors"
            >
              {saving ? 'Saving...' : 'Save Decision'}
            </button>
          </div>
        </form>
      )}

      {/* ── Decision list ── */}
      {decisions.length === 0 ? (
        <div className="bg-white rounded-2xl border border-slate-100 shadow-sm p-14 text-center">
          <BookMarked size={36} className="text-slate-200 mx-auto mb-3" />
          <p className="text-slate-600 font-semibold text-sm">No decisions logged yet</p>
          <p className="text-slate-400 text-xs mt-1 max-w-xs mx-auto leading-relaxed">
            Start capturing the reasoning behind your key decisions. Future you will thank present you.
          </p>
          <button
            onClick={() => setShowForm(true)}
            className="mt-5 inline-flex items-center gap-1.5 text-sm font-semibold text-[#0055A4] hover:text-blue-700"
          >
            <Plus size={13} /> Log your first decision
          </button>
        </div>
      ) : (
        <div className="space-y-3">
          {decisions.map(d => {
            const isExpanded = expanded === d.id
            const style = STATUS_STYLES[d.status] || STATUS_STYLES.active
            return (
              <div key={d.id} className="bg-white rounded-2xl border border-slate-100 shadow-sm overflow-hidden">

                {/* Row header */}
                <div
                  className="flex items-center justify-between px-5 py-3.5 cursor-pointer hover:bg-slate-50/60 transition-colors"
                  onClick={() => setExpanded(isExpanded ? null : d.id)}
                >
                  <div className="flex items-center gap-3 min-w-0">
                    <span className={`text-[10px] font-bold rounded-full px-2.5 py-0.5 shrink-0 ${style.badge}`}>
                      {style.label}
                    </span>
                    <span className="font-semibold text-slate-800 text-sm truncate">{d.title}</span>
                  </div>
                  <div className="flex items-center gap-4 shrink-0 ml-4">
                    <span className="hidden sm:flex items-center gap-1 text-[11px] text-slate-400">
                      <User size={10} /> {d.decided_by}
                    </span>
                    <span className="flex items-center gap-1 text-[11px] text-slate-400">
                      <Calendar size={10} /> {fmt(d.decided_at)}
                    </span>
                    {isExpanded
                      ? <ChevronUp size={14} className="text-slate-400" />
                      : <ChevronDown size={14} className="text-slate-400" />
                    }
                  </div>
                </div>

                {/* Expanded body */}
                {isExpanded && (
                  <div className="px-5 pb-5 pt-4 border-t border-slate-50 space-y-4">

                    <div>
                      <p className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-1.5 flex items-center gap-1">
                        <FileText size={9} /> Decision
                      </p>
                      <p className="text-sm text-slate-700 leading-relaxed">{d.the_decision}</p>
                    </div>

                    {d.rationale && (
                      <div>
                        <p className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-1.5 flex items-center gap-1">
                          <Lightbulb size={9} /> Rationale
                        </p>
                        <p className="text-sm text-slate-600 leading-relaxed">{d.rationale}</p>
                      </div>
                    )}

                    {d.kpi_context?.length > 0 && (
                      <div>
                        <p className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-2">
                          Linked KPIs
                        </p>
                        <div className="flex flex-wrap gap-1.5">
                          {d.kpi_context.map(k => (
                            <span
                              key={k}
                              className="bg-blue-50 text-blue-700 text-[10px] font-mono font-bold border border-blue-100 rounded px-2 py-0.5"
                            >
                              {k}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}

                    {d.outcome && (
                      <div className="bg-emerald-50 border border-emerald-100 rounded-xl px-4 py-3">
                        <p className="text-[10px] font-bold text-emerald-600 uppercase tracking-widest mb-1">
                          Recorded Outcome
                        </p>
                        <p className="text-sm text-emerald-800 leading-relaxed">{d.outcome}</p>
                      </div>
                    )}

                    {/* Outcome entry & status actions */}
                    {d.status === 'active' && (
                      <div className="border border-slate-100 rounded-xl p-4 space-y-3">
                        <p className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">
                          Record Outcome (optional)
                        </p>
                        <textarea
                          className="w-full border border-slate-200 rounded-lg px-3 py-2 text-sm text-slate-800 focus:outline-none focus:border-[#0055A4] resize-none"
                          rows={2}
                          placeholder="What happened? Did this decision achieve its goal?"
                          value={outcomeInput[d.id] || ''}
                          onChange={e => setOutcomeInput(v => ({ ...v, [d.id]: e.target.value }))}
                        />
                        <div className="flex gap-2">
                          <button
                            onClick={() => handleStatusUpdate(d.id, 'resolved')}
                            className="flex items-center gap-1.5 text-[11px] font-bold text-emerald-700 bg-emerald-50 border border-emerald-200 rounded-lg px-3 py-1.5 hover:bg-emerald-100 transition-colors"
                          >
                            <CheckCircle2 size={11} /> Mark Resolved
                          </button>
                          <button
                            onClick={() => handleStatusUpdate(d.id, 'reversed')}
                            className="flex items-center gap-1.5 text-[11px] font-bold text-amber-700 bg-amber-50 border border-amber-200 rounded-lg px-3 py-1.5 hover:bg-amber-100 transition-colors"
                          >
                            <RotateCcw size={11} /> Mark Reversed
                          </button>
                        </div>
                      </div>
                    )}

                    {d.status === 'resolved' && (
                      <div className="flex gap-2">
                        <button
                          onClick={() => handleStatusUpdate(d.id, 'active')}
                          className="flex items-center gap-1.5 text-[11px] font-bold text-blue-600 bg-blue-50 border border-blue-200 rounded-lg px-3 py-1.5 hover:bg-blue-100 transition-colors"
                        >
                          <TrendingUp size={11} /> Reopen
                        </button>
                      </div>
                    )}

                    <div className="flex justify-end pt-1 border-t border-slate-50">
                      <button
                        onClick={() => handleDelete(d.id)}
                        className="flex items-center gap-1 text-[11px] text-slate-300 hover:text-red-400 transition-colors"
                      >
                        <Trash2 size={11} /> Delete entry
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
