import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import { CheckCircle2, AlertCircle, RefreshCw, ChevronDown, ChevronRight, Sparkles, Zap } from 'lucide-react'

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

function KpiImpactPills({ kpis }) {
  if (!kpis || kpis.length === 0) return <span className="text-xs text-gray-600">-</span>
  return (
    <div className="flex flex-wrap gap-0.5">
      {kpis.slice(0, 3).map(k => (
        <span key={k} className="text-[9px] bg-[#00AEEF]/10 text-[#00AEEF] px-1.5 py-0.5 rounded font-medium">
          {k.replace(/_/g, ' ')}
        </span>
      ))}
      {kpis.length > 3 && (
        <span className="text-[9px] text-gray-500">+{kpis.length - 3}</span>
      )}
    </div>
  )
}

function MappingRow({ mapping, onConfirm }) {
  const [selected, setSelected] = useState(mapping.canonical_field)
  const [saving, setSaving]     = useState(false)
  const needsReview = !mapping.confirmed_by_user && mapping.confidence < 0.85
  const isNew = mapping.is_new

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
    <tr className={`border-b border-white/5 ${isNew ? 'bg-cyan-400/5' : needsReview ? 'bg-yellow-400/3' : ''}`}>
      <td className="px-4 py-3">
        <div className="flex items-center gap-1.5">
          <span className="text-gray-300 text-sm font-mono">{mapping.source_field}</span>
          {isNew && (
            <span className="text-[9px] font-bold bg-cyan-400/20 text-cyan-400 px-1.5 py-0.5 rounded-full">NEW</span>
          )}
        </div>
      </td>
      <td className="px-4 py-3">
        <span className="text-gray-500 text-xs">{mapping.source_name || mapping.source}</span>
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
        <KpiImpactPills kpis={mapping.kpi_impact} />
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
            {saving ? '...' : 'Confirm'}
          </button>
        )}
      </td>
    </tr>
  )
}

function StagingSection({ sourceName, entities, onConfirm }) {
  const [open, setOpen] = useState(true)
  const totalNew = Object.values(entities).reduce((s, e) => s + (e.new_field_count || 0), 0)
  const totalUnmapped = Object.values(entities).reduce((s, e) => s + (e.unmapped_count || 0), 0)

  return (
    <div className="bg-[#1a1f2e] border border-white/8 rounded-xl overflow-hidden mb-4">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center gap-3 px-4 py-3 hover:bg-white/3 transition-colors"
      >
        {open ? <ChevronDown size={14} className="text-gray-400"/> : <ChevronRight size={14} className="text-gray-400"/>}
        <span className="text-white font-semibold text-sm capitalize">{sourceName}</span>
        <div className="flex items-center gap-2 ml-auto">
          {totalNew > 0 && (
            <span className="text-[10px] font-bold bg-cyan-400/20 text-cyan-400 px-2 py-0.5 rounded-full">
              {totalNew} new
            </span>
          )}
          {totalUnmapped > 0 && (
            <span className="text-[10px] font-bold bg-red-400/20 text-red-400 px-2 py-0.5 rounded-full">
              {totalUnmapped} unmapped
            </span>
          )}
        </div>
      </button>
      {open && Object.entries(entities).map(([entityType, entity]) => (
        <div key={entityType} className="border-t border-white/5">
          <div className="px-4 py-2 bg-white/2">
            <span className="text-gray-400 text-xs font-semibold uppercase tracking-wider">{entityType}</span>
          </div>
          <table className="w-full">
            <thead>
              <tr className="border-b border-white/10">
                {['Source Field','Source','Entity','Maps To','KPI Impact','Confidence',''].map(h => (
                  <th key={h} className="px-4 py-2 text-left text-gray-500 text-[10px] font-semibold uppercase tracking-wider">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {(entity.fields || [])
                .sort((a, b) => (b.is_new ? 1 : 0) - (a.is_new ? 1 : 0))
                .map(f => (
                  <MappingRow
                    key={f.id || f.source_field}
                    mapping={{ ...f, source_name: sourceName }}
                    onConfirm={onConfirm}
                  />
                ))
              }
            </tbody>
          </table>
        </div>
      ))}
    </div>
  )
}

export default function FieldMappingPage() {
  const [view, setView]           = useState('staging')  // 'staging' or 'flat'
  const [mappings, setMappings]   = useState([])
  const [staging, setStaging]     = useState(null)
  const [loading, setLoading]     = useState(true)
  const [filterSource, setFilter] = useState('')
  const [showAll, setShowAll]     = useState(false)
  const [bulkConfirming, setBulk] = useState(false)
  const [recomputing, setRecomp]  = useState(false)

  const loadFlat = useCallback(async () => {
    setLoading(true)
    try {
      const params = filterSource ? `?source=${filterSource}` : ''
      const { data } = await axios.get(`/api/connectors/mappings${params}`)
      setMappings(data.mappings || [])
    } finally {
      setLoading(false)
    }
  }, [filterSource])

  const loadStaging = useCallback(async () => {
    setLoading(true)
    try {
      const params = filterSource ? `?source=${filterSource}` : ''
      const { data } = await axios.get(`/api/connectors/mappings/staging${params}`)
      setStaging(data)
    } finally {
      setLoading(false)
    }
  }, [filterSource])

  useEffect(() => {
    if (view === 'staging') loadStaging()
    else loadFlat()
  }, [view, loadStaging, loadFlat])

  function handleConfirmed(id, newField) {
    // Update both flat and staging views
    setMappings(m => m.map(row =>
      row.id === id ? { ...row, canonical_field: newField, confirmed_by_user: 1, confidence: 1.0, is_new: 0 } : row
    ))
    // Reload staging view to get updated counts
    if (view === 'staging') loadStaging()
  }

  async function handleBulkConfirm() {
    if (!staging) return
    setBulk(true)
    try {
      // Gather all unconfirmed fields with confidence >= 0.80
      const toConfirm = []
      for (const [, entities] of Object.entries(staging.sources || {})) {
        for (const [, entity] of Object.entries(entities)) {
          for (const f of entity.fields || []) {
            if (!f.confirmed_by_user && f.confidence >= 0.80 && f.canonical_field !== 'unmapped') {
              toConfirm.push({ id: f.id, canonical_field: f.canonical_field })
            }
          }
        }
      }
      if (toConfirm.length === 0) return
      await axios.post('/api/connectors/mappings/bulk-confirm', { mappings: toConfirm })
      loadStaging()
    } finally {
      setBulk(false)
    }
  }

  async function handleRecomputeKpis() {
    setRecomp(true)
    try {
      await axios.post('/api/connectors/mappings/bulk-confirm', { mappings: [] })
    } finally {
      setRecomp(false)
    }
  }

  async function handleMarkReviewed() {
    try {
      await axios.put('/api/connectors/mappings/mark-reviewed' + (filterSource ? `?source=${filterSource}` : ''))
      loadStaging()
    } catch { /* silent */ }
  }

  // Flat view data
  const sources       = [...new Set(mappings.map(m => m.source_name))]
  const needsReview   = mappings.filter(m => !m.confirmed_by_user && m.confidence < 0.85)
  const displayed     = showAll ? mappings : (needsReview.length ? needsReview : mappings)

  // Staging view data
  const stagingSources = staging ? Object.keys(staging.sources || {}) : []
  const qualityScore = staging?.mapping_quality?.score ?? 100
  const qualityLabel = staging?.mapping_quality?.label ?? 'high'

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
        <div className="flex items-center gap-2">
          {/* View toggle */}
          <div className="flex bg-white/5 rounded-lg p-0.5">
            <button
              onClick={() => setView('staging')}
              className={`px-3 py-1 text-xs rounded-md transition-colors ${
                view === 'staging' ? 'bg-[#00AEEF] text-white' : 'text-gray-400 hover:text-white'
              }`}
            >
              Staging
            </button>
            <button
              onClick={() => setView('flat')}
              className={`px-3 py-1 text-xs rounded-md transition-colors ${
                view === 'flat' ? 'bg-[#00AEEF] text-white' : 'text-gray-400 hover:text-white'
              }`}
            >
              All Mappings
            </button>
          </div>
          <button
            onClick={view === 'staging' ? loadStaging : loadFlat}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-gray-400 bg-white/5 hover:bg-white/10 rounded-lg transition-colors"
          >
            <RefreshCw size={12}/> Refresh
          </button>
        </div>
      </div>

      {/* Summary bar */}
      <div className="flex items-center gap-4 mb-5 flex-wrap">
        {view === 'staging' && staging && (
          <>
            <div className="flex items-center gap-2 text-sm">
              <span className={`font-bold ${qualityLabel === 'high' ? 'text-green-400' : qualityLabel === 'moderate' ? 'text-yellow-400' : 'text-red-400'}`}>
                {qualityScore}%
              </span>
              <span className="text-gray-500">mapping quality</span>
            </div>
            {staging.total_new > 0 && (
              <div className="flex items-center gap-2 text-sm">
                <Sparkles size={14} className="text-cyan-400"/>
                <span className="text-cyan-400 font-medium">{staging.total_new}</span>
                <span className="text-gray-500">new field(s)</span>
              </div>
            )}
            {staging.total_unmapped > 0 && (
              <div className="flex items-center gap-2 text-sm">
                <AlertCircle size={14} className="text-yellow-400"/>
                <span className="text-yellow-400 font-medium">{staging.total_unmapped}</span>
                <span className="text-gray-500">unmapped</span>
              </div>
            )}
            {staging.critical_unmapped > 0 && (
              <div className="flex items-center gap-2 text-sm">
                <AlertCircle size={14} className="text-red-400"/>
                <span className="text-red-400 font-medium">{staging.critical_unmapped}</span>
                <span className="text-gray-500">critical</span>
              </div>
            )}
            {staging.total_new === 0 && staging.total_unmapped === 0 && (
              <div className="flex items-center gap-2 text-sm">
                <CheckCircle2 size={14} className="text-green-400"/>
                <span className="text-green-400 font-medium">All mappings confirmed</span>
              </div>
            )}
          </>
        )}
        {view === 'flat' && (
          <>
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
          </>
        )}
      </div>

      {/* Action buttons */}
      <div className="flex items-center gap-3 mb-4 flex-wrap">
        <select
          value={filterSource}
          onChange={e => setFilter(e.target.value)}
          className="bg-[#1a1f2e] border border-white/10 text-gray-300 text-xs rounded-lg px-3 py-1.5 outline-none"
        >
          <option value="">All sources</option>
          {(view === 'staging' ? stagingSources : sources).map(s => <option key={s} value={s}>{s}</option>)}
        </select>

        {view === 'staging' && staging && staging.total_unmapped > 0 && (
          <button
            onClick={handleBulkConfirm}
            disabled={bulkConfirming}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-[#00AEEF] text-white rounded-lg
              hover:bg-[#0099d4] disabled:opacity-50 transition-colors"
          >
            <Zap size={12}/>
            {bulkConfirming ? 'Confirming...' : 'Confirm All Auto-Detected'}
          </button>
        )}

        {view === 'staging' && staging && staging.total_new > 0 && (
          <button
            onClick={handleMarkReviewed}
            className="text-xs text-gray-400 hover:text-white transition-colors"
          >
            Mark all as reviewed
          </button>
        )}

        {view === 'flat' && needsReview.length > 0 && (
          <button
            onClick={() => setShowAll(a => !a)}
            className="text-xs text-[#00AEEF] hover:underline"
          >
            {showAll ? 'Show only needs-review' : 'Show all mappings'}
          </button>
        )}
      </div>

      {/* Staging View */}
      {view === 'staging' && staging && (
        <>
          {Object.keys(staging.sources || {}).length === 0 ? (
            <div className="text-center py-16 text-gray-500">
              <p className="text-white font-medium mb-1">No field mappings yet</p>
              <p className="text-sm">Connect a data source and run a sync to populate mappings.</p>
            </div>
          ) : (
            Object.entries(staging.sources).map(([srcName, entities]) => (
              <StagingSection
                key={srcName}
                sourceName={srcName}
                entities={entities}
                onConfirm={handleConfirmed}
              />
            ))
          )}
        </>
      )}

      {/* Flat View */}
      {view === 'flat' && (
        <>
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
                    {['Source Field','Source','Entity','Maps To','KPI Impact','Confidence',''].map(h => (
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
        </>
      )}
    </div>
  )
}
