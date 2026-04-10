import { useState, useEffect } from 'react'
import { Zap, AlertCircle, ShieldCheck, GitBranch, Shield, FileSpreadsheet, CheckCircle2, Clock } from 'lucide-react'
import axios from 'axios'
import DataSourcesPage    from './DataSourcesPage.jsx'
import DataGapsPage       from './DataGapsPage.jsx'
import DataQualityPage    from './DataQualityPage.jsx'
import FieldMappingPage   from './FieldMappingPage.jsx'
import DataIntegrityPage  from './DataIntegrityPage.jsx'
import NotificationBanner from './NotificationBanner.jsx'

function ControlAttestationTab() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  useEffect(() => {
    axios.get('/api/integrity-check/attestation').then(r => setData(r.data)).catch(() => {}).finally(() => setLoading(false))
  }, [])
  if (loading) return <div className="flex justify-center py-12"><div className="animate-spin w-6 h-6 border-2 border-[#0055A4] border-t-transparent rounded-full"/></div>
  if (!data || data.controls_tested === 0) return <div className="text-center py-12 text-slate-400"><p className="font-medium text-slate-600 mb-1">No integrity checks run yet</p><p className="text-sm">Run an integrity check from the Integrity tab first.</p></div>
  const assessColor = data.overall_assessment.includes('Effective') && !data.overall_assessment.includes('weakness') ? 'text-green-600 bg-green-50 border-green-200' : data.overall_assessment.includes('exception') ? 'text-yellow-600 bg-yellow-50 border-yellow-200' : 'text-red-600 bg-red-50 border-red-200'
  return (
    <div className="space-y-4">
      <div className={`border rounded-xl px-5 py-4 ${assessColor}`}>
        <p className="font-semibold text-lg">{data.overall_assessment}</p>
        <p className="text-sm mt-1 opacity-80">{data.controls_tested} controls tested: {data.passed} passed, {data.warned} warned, {data.failed} failed</p>
      </div>
      <div className="grid grid-cols-4 gap-3">
        {[{l:'Tested',v:data.controls_tested,c:'text-slate-600'},{l:'Passed',v:data.passed,c:'text-green-600'},{l:'Warnings',v:data.warned,c:'text-yellow-600'},{l:'Failed',v:data.failed,c:'text-red-600'}].map(x=>(
          <div key={x.l} className="bg-white border border-slate-200 rounded-xl px-4 py-3 text-center">
            <p className={`text-2xl font-bold ${x.c}`}>{x.v}</p><p className="text-xs text-slate-400 mt-0.5">{x.l}</p>
          </div>
        ))}
      </div>
      {data.material_exceptions.length > 0 && (
        <div className="bg-red-50 border border-red-200 rounded-xl px-5 py-3">
          <p className="font-semibold text-red-700 text-sm mb-2">Material Exceptions</p>
          {data.material_exceptions.map((e,i) => <p key={i} className="text-xs text-red-600 mb-1">Stage {e.stage}: {e.check} — {e.detail}</p>)}
        </div>
      )}
      {data.observations.length > 0 && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-xl px-5 py-3">
          <p className="font-semibold text-yellow-700 text-sm mb-2">Observations</p>
          {data.observations.map((e,i) => <p key={i} className="text-xs text-yellow-600 mb-1">Stage {e.stage}: {e.check} — {e.detail}</p>)}
        </div>
      )}
    </div>
  )
}

function RestatementHistoryTab() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  useEffect(() => {
    axios.get('/api/analytics/restatement-history').then(r => setData(r.data)).catch(() => {}).finally(() => setLoading(false))
  }, [])
  if (loading) return <div className="flex justify-center py-12"><div className="animate-spin w-6 h-6 border-2 border-[#0055A4] border-t-transparent rounded-full"/></div>
  const items = data?.restatements || []
  if (items.length === 0) return <div className="text-center py-12 text-slate-400"><p className="font-medium text-slate-600 mb-1">No restatements</p><p className="text-sm">Data corrections will appear here when they occur.</p></div>
  return (
    <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
      <table className="w-full">
        <thead><tr className="border-b border-slate-100 bg-slate-50">{['Table','Field','Old Value','New Value','Change %','Date'].map(h=><th key={h} className="px-4 py-2 text-left text-slate-500 text-xs font-semibold">{h}</th>)}</tr></thead>
        <tbody>{items.map((r,i) => (
          <tr key={i} className="border-b border-slate-50 hover:bg-slate-50">
            <td className="px-4 py-2 text-xs text-slate-600">{r.table}</td>
            <td className="px-4 py-2 text-xs text-slate-600">{r.field}</td>
            <td className="px-4 py-2 text-xs text-slate-500 font-mono">{r.old_value}</td>
            <td className="px-4 py-2 text-xs text-slate-700 font-mono font-medium">{r.new_value}</td>
            <td className="px-4 py-2 text-xs"><span className={r.pct_change > 0 ? 'text-green-600' : 'text-red-600'}>{r.pct_change > 0 ? '+' : ''}{r.pct_change}%</span></td>
            <td className="px-4 py-2 text-xs text-slate-400">{r.changed_at ? new Date(r.changed_at).toLocaleDateString() : '-'}</td>
          </tr>
        ))}</tbody>
      </table>
    </div>
  )
}

const TABS = [
  { id: 'sources',     label: 'Data Sources',        Icon: Zap,           Component: DataSourcesPage  },
  { id: 'gaps',        label: 'Data Gaps',            Icon: AlertCircle,   Component: DataGapsPage     },
  { id: 'quality',     label: 'Data Quality',         Icon: ShieldCheck,   Component: DataQualityPage  },
  { id: 'mappings',    label: 'Field Mappings',       Icon: GitBranch,     Component: FieldMappingPage },
  { id: 'integrity',   label: 'Integrity',            Icon: Shield,        Component: DataIntegrityPage },
  { id: 'attestation', label: 'Controls',             Icon: CheckCircle2,  Component: ControlAttestationTab },
  { id: 'restatement', label: 'Restatements',         Icon: Clock,         Component: RestatementHistoryTab },
]

export default function DataHealthPage() {
  const [activeTab, setActiveTab] = useState('sources')
  const [exporting, setExporting] = useState(false)
  const active = TABS.find(t => t.id === activeTab)
  const ActiveComponent = active?.Component

  async function downloadIntegrationSpec() {
    setExporting(true)
    try {
      const r = await axios.get('/api/export/integration-spec.xlsx', { responseType: 'blob' })
      const url = window.URL.createObjectURL(new Blob([r.data]))
      const a = document.createElement('a')
      a.href = url
      a.download = r.headers['content-disposition']?.match(/filename=(.+)/)?.[1] || 'axiom-integration-spec.xlsx'
      document.body.appendChild(a)
      a.click()
      a.remove()
      window.URL.revokeObjectURL(url)
    } catch (err) {
      console.error('Integration spec export failed', err)
    } finally {
      setExporting(false)
    }
  }

  return (
    <div className="max-w-7xl space-y-5">
      {/* Notification banner (unmapped fields, mapping required) */}
      <NotificationBanner onNavigate={(tab) => setActiveTab(tab === 'field-mapping' ? 'mappings' : tab)} />

      {/* Sub-navigation + download */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex gap-1 bg-white border border-slate-200 rounded-xl p-1 w-fit shadow-sm">
          {TABS.map(({ id, label, Icon }) => (
            <button
              key={id}
              onClick={() => setActiveTab(id)}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                activeTab === id
                  ? 'bg-[#0055A4] text-white shadow-sm'
                  : 'text-slate-500 hover:text-slate-800 hover:bg-slate-50'
              }`}
            >
              <Icon size={14} />
              {label}
            </button>
          ))}
        </div>
        <button
          onClick={downloadIntegrationSpec}
          disabled={exporting}
          className="flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-medium text-white
                     bg-[#0055A4] hover:bg-[#003d80] rounded-lg transition-all
                     disabled:opacity-50 disabled:cursor-not-allowed shadow-sm"
        >
          <FileSpreadsheet size={12} />
          {exporting ? 'Generating...' : 'Integration Spec (.xlsx)'}
        </button>
      </div>

      {/* Active sub-page */}
      {ActiveComponent && <ActiveComponent />}
    </div>
  )
}
