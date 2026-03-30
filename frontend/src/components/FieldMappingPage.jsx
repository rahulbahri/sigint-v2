import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import { CheckCircle2, AlertCircle, RefreshCw, ChevronDown } from 'lucide-react'

const CANONICAL_FIELDS = {
  revenue:   ['amount','currency','period','customer_id','subscription_type','product_id','recognized_at','source_id'],
  customers: ['name','email','company','phone','country','created_at','lifecycle_stage','source_id'],
  pipeline:  ['name','amount','stage','close_date','probability','owner','created_at','source_id'],
  employees: ['name','email','title','department','salary','hire_date','status','source_id'],
  expenses:  ['amount','currency','category','vendor','period','description','source_id'],
  invoices:  ['amount','currency','customer_id','issue_date','due_date','status','period','source_id'],
  products:  ['name','sku','price','currency','category','active','source_id'],
  marketing: ['channel','spend','currency','period','leads','conversions','source_id'],
  ignore:    ['ignore / skip this field'],
}

const ALL_CANONICAL_OPTIONS = [
  ...new Set(Object.values(CANONICAL_FIELDS).flat())
].sort()

function ConfidencePill({ confidence, confirmed }) {
  if (confirmed) return <span className="text-xs text-green-400 font-medium flex items-center gap-1"><CheckCircle2 size={12}/> Confirmed</span>
  const pct = Math.round((confidence || 0) * 100)
  const color = pct >= 85 ? 'text-green-400' : pct >= 50 ? 'text-yellow-400' : 'text-red-400'
  return <span className={`text-xs ${color} font-medium`}>{pct}% sure</span>
}

function MappingRow({ mapping, onConfirm }) {
  const [selected, setSelected] = useState(mapping.canonical_field)
  const [saving, setSaving]     = useState(false)
  const needsReview = !mapping.confirmed_by_user && mapping.confidence < 0.85

  async function handleConfirm() {
    setSaving(true)
    try {
      await axios.put(`/api/connectors/mappings/${mapping.id}`, { canonical_field: selected })
      onConfirm(mapping.id, selected)
    } finally {
      setSaving(false)
    }
  }

  return (
    <tr className={`border-b border-white/5 ${needsReview ? 'bg-yellow-400/3' : ''}`}>
      <td className="px-4 py-3">
        <span className="text-gray-300 text-sm font-mono">{mapping.source_field}</span>
      </td>
      <td className="px-4 py-3">
        <span className="text-gray-500 text-xs">{mapping.source_name}</span>
      </td>
      <td className="px-4 py-3">
        <span className="text-gray-500 text-xs">{mapping.canonical_table}</span>
      </td>
      <td className="px-4 py-3">
        <div className="relative">
          <select
            value={selected}
            onChange={e => setSelected(e.target.value)}
            disabled={mapping.confirmed_by_user}
            className="appearance-none bg-[#0d1117] border border-white/10 text-white text-xs
              rounded-lg pl-3 pr-7 py-1.5 w-full outline-none focus:border-[#00AEEF]/50
              disabled:opacity-60 disabled:cursor-default"
          >
            {ALL_CANONICAL_OPTIONS.map(opt => (
              <option key={opt} value={opt}>{opt}</option>
            ))}
          </select>
          {!mapping.confirmed_by_user && (
            <ChevronDown size={12} className="absolute right-2 top-2.5 text-gray-500 pointer-events-none"/>
          )}
        </div>
      </td>
      <td className="px-4 py-3">
        <ConfidencePill confidence={mapping.confidence} confirmed={mapping.confirmed_by_user}/>
      </td>
      <td className="px-4 py-3">
        {!mapping.confirmed_by_user && (
          <button
            onClick={handleConfirm}
            disabled={saving}
            className="px-3 py-1 text-xs bg-[#00AEEF] text-white rounded-lg
              hover:bg-[#0099d4] disabled:opacity-50 transition-colors"
          >
            {saving ? '…' : 'Confirm'}
          </button>
        )}
      </td>
    </tr>
  )
}

export default function FieldMappingPage() {
  const [mappings, setMappings]   = useState([])
  const [loading, setLoading]     = useState(true)
  const [filterSource, setFilter] = useState('')
  const [showAll, setShowAll]     = useState(false)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const params = filterSource ? `?source=${filterSource}` : ''
      const { data } = await axios.get(`/api/connectors/mappings${params}`)
      setMappings(data.mappings || [])
    } finally {
      setLoading(false)
    }
  }, [filterSource])

  useEffect(() => { load() }, [load])

  function handleConfirmed(id, newField) {
    setMappings(m => m.map(row =>
      row.id === id ? { ...row, canonical_field: newField, confirmed_by_user: 1, confidence: 1.0 } : row
    ))
  }

  const sources       = [...new Set(mappings.map(m => m.source_name))]
  const needsReview   = mappings.filter(m => !m.confirmed_by_user && m.confidence < 0.85)
  const displayed     = showAll ? mappings : (needsReview.length ? needsReview : mappings)

  if (loading) return (
    <div className="flex items-center justify-center h-64">
      <RefreshCw size={20} className="animate-spin text-[#00AEEF]"/>
    </div>
  )

  return (
    <div className="p-6 max-w-5xl mx-auto">
      <div className="flex items-start justify-between mb-6">
        <div>
          <h2 className="text-white text-xl font-semibold">Field Mappings</h2>
          <p className="text-gray-400 text-sm mt-1">
            Review how source fields map to canonical data fields. Confirm uncertain ones.
          </p>
        </div>
        <button onClick={load} className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-gray-400 bg-white/5 hover:bg-white/10 rounded-lg transition-colors">
          <RefreshCw size={12}/> Refresh
        </button>
      </div>

      {/* Summary */}
      <div className="flex items-center gap-4 mb-5 flex-wrap">
        <div className="flex items-center gap-2 text-sm">
          <span className="text-white font-medium">{mappings.length}</span>
          <span className="text-gray-500">total mappings</span>
        </div>
        {needsReview.length > 0 && (
          <div className="flex items-center gap-2 text-sm">
            <AlertCircle size={14} className="text-yellow-400"/>
            <span className="text-yellow-400 font-medium">{needsReview.length}</span>
            <span className="text-gray-500">need your review</span>
          </div>
        )}
        {needsReview.length === 0 && mappings.length > 0 && (
          <div className="flex items-center gap-2 text-sm">
            <CheckCircle2 size={14} className="text-green-400"/>
            <span className="text-green-400 font-medium">All mappings confirmed</span>
          </div>
        )}
      </div>

      {/* Filters */}
      <div className="flex items-center gap-3 mb-4 flex-wrap">
        <select
          value={filterSource}
          onChange={e => setFilter(e.target.value)}
          className="bg-[#1a1f2e] border border-white/10 text-gray-300 text-xs rounded-lg px-3 py-1.5 outline-none"
        >
          <option value="">All sources</option>
          {sources.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
        {needsReview.length > 0 && (
          <button
            onClick={() => setShowAll(a => !a)}
            className="text-xs text-[#00AEEF] hover:underline"
          >
            {showAll ? 'Show only needs-review' : 'Show all mappings'}
          </button>
        )}
      </div>

      {mappings.length === 0 ? (
        <div className="text-center py-16 text-gray-500">
          <p className="text-white font-medium mb-1">No field mappings yet</p>
          <p className="text-sm">Connect a data source and run a sync to populate mappings.</p>
        </div>
      ) : (
        <div className="bg-[#1a1f2e] border border-white/8 rounded-xl overflow-hidden">
          <table className="w-full">
            <thead>
              <tr className="border-b border-white/10">
                {['Source Field','Source','Entity','Maps To','Confidence',''].map(h => (
                  <th key={h} className="px-4 py-3 text-left text-gray-500 text-xs font-semibold uppercase tracking-wider">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {displayed.map(m => (
                <MappingRow key={m.id} mapping={m} onConfirm={handleConfirmed}/>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
