import { useState } from 'react'
import { Zap, AlertCircle, ShieldCheck, GitBranch, Shield, FileSpreadsheet } from 'lucide-react'
import axios from 'axios'
import DataSourcesPage    from './DataSourcesPage.jsx'
import DataGapsPage       from './DataGapsPage.jsx'
import DataQualityPage    from './DataQualityPage.jsx'
import FieldMappingPage   from './FieldMappingPage.jsx'
import DataIntegrityPage  from './DataIntegrityPage.jsx'

const TABS = [
  { id: 'sources',   label: 'Data Sources',   Icon: Zap,          Component: DataSourcesPage  },
  { id: 'gaps',      label: 'Data Gaps',      Icon: AlertCircle,  Component: DataGapsPage     },
  { id: 'quality',   label: 'Data Quality',   Icon: ShieldCheck,  Component: DataQualityPage  },
  { id: 'mappings',  label: 'Field Mappings', Icon: GitBranch,    Component: FieldMappingPage },
  { id: 'integrity', label: 'Integrity',      Icon: Shield,       Component: DataIntegrityPage },
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
