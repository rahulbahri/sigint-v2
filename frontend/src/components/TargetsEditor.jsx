import { useState, useEffect } from 'react'
import axios from 'axios'

const DOMAINS = {
  financial: { label: 'Financial Performance', color: 'blue' },
  growth: { label: 'Growth & Sales', color: 'emerald' },
  retention: { label: 'Retention & Customer', color: 'violet' },
  efficiency: { label: 'Efficiency & Operations', color: 'amber' },
  other: { label: 'Other Metrics', color: 'slate' },
}

const UNIT_LABELS = { pct: '%', ratio: 'x', days: 'days', months: 'mo', usd: '$k', score: 'pts' }

// Guidance for each unit type — helps users understand what basis to enter
const UNIT_GUIDANCE = {
  pct: 'Monthly rate',
  ratio: 'Target ratio',
  days: 'Target days',
  months: 'Target months',
  usd: 'Monthly level',
  score: 'Target score',
  count: 'Monthly avg',
}

export default function TargetsEditor() {
  const [kpis, setKpis] = useState([])
  const [edited, setEdited] = useState({})
  const [saving, setSaving] = useState(null)
  const [saved, setSaved] = useState({})

  useEffect(() => {
    axios.get('/api/fingerprint').then(r => setKpis(r.data))
  }, [])

  const handleChange = (key, val) => {
    setEdited(prev => ({ ...prev, [key]: val }))
  }

  const handleSave = async (kpi) => {
    const val = parseFloat(edited[kpi.key])
    if (isNaN(val)) return
    setSaving(kpi.key)
    await axios.put(`/api/targets/${kpi.key}`, { target: val, unit: kpi.unit, direction: kpi.direction })
      .catch(() => {})
    setSaving(null)
    setSaved(prev => ({ ...prev, [kpi.key]: true }))
    setTimeout(() => setSaved(prev => ({ ...prev, [kpi.key]: false })), 2000)
  }

  // Group KPIs by domain
  const grouped = {}
  kpis.forEach(k => {
    const domain = k.domain || 'other'
    if (!grouped[domain]) grouped[domain] = []
    grouped[domain].push(k)
  })

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-[15px] font-bold text-slate-800">KPI Targets</h2>
        <p className="text-[12px] text-slate-500 mt-0.5">Set the desired monthly level for each metric. Targets are compared to the rolling average of monthly data — they stay the same regardless of which period you select.</p>
      </div>

      {Object.entries(DOMAINS).map(([domainKey, domainMeta]) => {
        const kpisInDomain = grouped[domainKey] || []
        if (kpisInDomain.length === 0) return null
        return (
          <div key={domainKey} className="bg-white rounded-2xl border border-slate-200 overflow-hidden">
            <div className="px-4 py-3 border-b border-slate-100 bg-slate-50/50">
              <h3 className="text-[12px] font-semibold text-slate-700">{domainMeta.label}</h3>
            </div>
            <div className="divide-y divide-slate-50">
              {kpisInDomain.map(kpi => {
                const currentVal = edited[kpi.key] ?? (kpi.target ?? '')
                const unit = UNIT_LABELS[kpi.unit] || ''
                const isSaving = saving === kpi.key
                const isSaved = saved[kpi.key]
                return (
                  <div key={kpi.key} className="grid grid-cols-[1fr_auto_auto] gap-3 px-4 py-2.5 items-center hover:bg-slate-50/30">
                    <div>
                      <div className="text-[12px] font-medium text-slate-700">{kpi.name}</div>
                      <div className="text-[10px] text-slate-400">
                        {kpi.direction === 'higher' ? '↑ Higher is better' : '↓ Lower is better'} · Current avg: {kpi.avg != null ? kpi.avg : '—'}{unit} · {UNIT_GUIDANCE[kpi.unit] || 'Target level'}
                      </div>
                    </div>
                    <div className="flex items-center gap-1.5">
                      <input
                        type="number"
                        value={currentVal}
                        onChange={e => handleChange(kpi.key, e.target.value)}
                        onKeyDown={e => { if (e.key === 'Enter') handleSave(kpi) }}
                        className="w-24 text-[12px] text-slate-700 text-right border border-slate-200 rounded-lg px-2.5 py-1 focus:border-blue-400 focus:outline-none focus:ring-1 focus:ring-blue-100"
                      />
                      <span className="text-[11px] text-slate-400 w-6">{unit}</span>
                    </div>
                    <button
                      onClick={() => handleSave(kpi)}
                      disabled={isSaving || edited[kpi.key] == null}
                      className={`px-2.5 py-1 text-[10px] font-semibold rounded-lg transition-all min-w-[52px] ${
                        isSaved ? 'bg-emerald-50 text-emerald-600 border border-emerald-200' :
                        'bg-[#0055A4] text-white hover:bg-[#003d80] disabled:opacity-30'
                      }`}
                    >
                      {isSaving ? '...' : isSaved ? '✓ Saved' : 'Save'}
                    </button>
                  </div>
                )
              })}
            </div>
          </div>
        )
      })}
    </div>
  )
}
