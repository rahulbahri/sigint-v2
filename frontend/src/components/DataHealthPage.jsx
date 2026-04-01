import { useState } from 'react'
import { Zap, AlertCircle, ShieldCheck, GitBranch } from 'lucide-react'
import DataSourcesPage  from './DataSourcesPage.jsx'
import DataGapsPage     from './DataGapsPage.jsx'
import DataQualityPage  from './DataQualityPage.jsx'
import FieldMappingPage from './FieldMappingPage.jsx'

const TABS = [
  { id: 'sources',  label: 'Data Sources',   Icon: Zap,          Component: DataSourcesPage  },
  { id: 'gaps',     label: 'Data Gaps',      Icon: AlertCircle,  Component: DataGapsPage     },
  { id: 'quality',  label: 'Data Quality',   Icon: ShieldCheck,  Component: DataQualityPage  },
  { id: 'mappings', label: 'Field Mappings', Icon: GitBranch,    Component: FieldMappingPage },
]

export default function DataHealthPage() {
  const [activeTab, setActiveTab] = useState('sources')
  const active = TABS.find(t => t.id === activeTab)
  const ActiveComponent = active?.Component

  return (
    <div>
      {/* Sub-navigation */}
      <div className="flex gap-1 mb-6 bg-white border border-slate-200 rounded-xl p-1 w-fit shadow-sm">
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

      {/* Active sub-page */}
      {ActiveComponent && <ActiveComponent />}
    </div>
  )
}
