import { useState, useEffect } from 'react'
import axios from 'axios'
import {
  Users, ChevronRight, AlertTriangle, CheckCircle2, Eye,
  Plus, Settings2, Trash2,
} from 'lucide-react'

function fmtVal(v, unit) {
  if (v == null) return '-'
  if (unit === 'pct') return `${v.toFixed(1)}%`
  if (unit === 'usd' || unit === '$') return `$${v.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
  return v.toFixed(2)
}

function DeptCard({ dept, onClick }) {
  const total = (dept.red || 0) + (dept.yellow || 0) + (dept.green || 0)
  const barW = total > 0 ? 100 : 0
  const greenPct = total > 0 ? (dept.green / total) * 100 : 0
  const yellowPct = total > 0 ? (dept.yellow / total) * 100 : 0
  const redPct = total > 0 ? (dept.red / total) * 100 : 0

  return (
    <button
      onClick={onClick}
      className="w-full text-left bg-white rounded-2xl border border-slate-100 shadow-sm p-4 hover:shadow-md hover:border-slate-200 transition-all group"
    >
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: dept.color || '#0055A4' }} />
          <h3 className="text-[13px] font-bold text-slate-800">{dept.name}</h3>
        </div>
        <ChevronRight size={13} className="text-slate-300 group-hover:text-slate-500" />
      </div>

      <p className="text-[10px] text-slate-400 mb-2">{dept.owner || 'No owner'} · {dept.kpi_count} KPIs</p>

      {/* Score */}
      {dept.score != null && (
        <div className="flex items-center gap-2 mb-2">
          <span className={`text-lg font-bold ${dept.score >= 70 ? 'text-emerald-600' : dept.score >= 40 ? 'text-amber-600' : 'text-red-600'}`}>
            {dept.score}%
          </span>
          <span className="text-[10px] text-slate-400">on target</span>
        </div>
      )}

      {/* Status bar */}
      {total > 0 && (
        <div className="flex h-1.5 rounded-full overflow-hidden bg-slate-100 mb-2">
          {greenPct > 0 && <div className="bg-emerald-500" style={{ width: `${greenPct}%` }} />}
          {yellowPct > 0 && <div className="bg-amber-400" style={{ width: `${yellowPct}%` }} />}
          {redPct > 0 && <div className="bg-red-500" style={{ width: `${redPct}%` }} />}
        </div>
      )}

      <div className="flex gap-3 text-[10px]">
        <span className="text-emerald-600 font-semibold">{dept.green} on target</span>
        <span className="text-amber-500 font-semibold">{dept.yellow} watch</span>
        <span className="text-red-500 font-semibold">{dept.red} critical</span>
      </div>

      {/* Top issues */}
      {dept.top_issues?.length > 0 && (
        <div className="mt-2 pt-2 border-t border-slate-50 space-y-1">
          {dept.top_issues.map(issue => (
            <div key={issue.key} className="flex items-center gap-1.5 text-[10px]">
              <AlertTriangle size={9} className="text-red-400" />
              <span className="text-slate-600 truncate">{issue.name?.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}</span>
            </div>
          ))}
        </div>
      )}
    </button>
  )
}

export default function DepartmentDashboard({ onKpiClick }) {
  const [departments, setDepartments] = useState([])
  const [loading, setLoading] = useState(true)
  const [selectedDept, setSelectedDept] = useState(null)

  useEffect(() => {
    axios.get('/api/departments/dashboard')
      .then(r => setDepartments(r.data?.departments || []))
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-40 text-slate-400 text-sm">
        Loading departments...
      </div>
    )
  }

  return (
    <div className="space-y-5 max-w-7xl">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-bold text-slate-800 flex items-center gap-2">
            <Users size={18} className="text-[#0055A4]" />
            Department Performance
          </h1>
          <p className="text-[12px] text-slate-500 mt-0.5">
            KPI health by team. Each department maps to one or more business domains.
          </p>
        </div>
      </div>

      {/* Department grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {departments.map(dept => (
          <DeptCard
            key={dept.id}
            dept={dept}
            onClick={() => setSelectedDept(selectedDept?.id === dept.id ? null : dept)}
          />
        ))}
      </div>

      {/* Selected department detail */}
      {selectedDept && (
        <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-5">
          <div className="flex items-center gap-2 mb-3">
            <div className="w-3 h-3 rounded-full" style={{ backgroundColor: selectedDept.color }} />
            <h2 className="text-[14px] font-bold text-slate-800">{selectedDept.name}</h2>
            <span className="text-[11px] text-slate-400">· {selectedDept.owner} · Domains: {selectedDept.domains?.join(', ')}</span>
          </div>

          {selectedDept.top_issues?.length > 0 ? (
            <div className="space-y-2">
              <p className="text-[10px] font-bold text-red-500 uppercase tracking-wider">Critical KPIs Requiring Action</p>
              {selectedDept.top_issues.map(issue => (
                <button
                  key={issue.key}
                  onClick={() => onKpiClick?.({ key: issue.key })}
                  className="flex items-center justify-between w-full text-left px-3 py-2 bg-red-50 rounded-lg border border-red-100 hover:border-red-200 transition-colors group"
                >
                  <div className="flex items-center gap-2">
                    <AlertTriangle size={11} className="text-red-500" />
                    <span className="text-[12px] font-semibold text-slate-700">
                      {issue.name?.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}
                    </span>
                  </div>
                  <div className="flex items-center gap-2">
                    {issue.gap_pct != null && (
                      <span className="text-[10px] font-bold text-red-500">
                        {issue.gap_pct > 0 ? '+' : ''}{typeof issue.gap_pct === 'number' ? issue.gap_pct.toFixed(0) : issue.gap_pct}%
                      </span>
                    )}
                    <ChevronRight size={11} className="text-slate-300 group-hover:text-slate-500" />
                  </div>
                </button>
              ))}
            </div>
          ) : (
            <div className="text-center py-6">
              <CheckCircle2 size={24} className="text-emerald-400 mx-auto mb-2" />
              <p className="text-[12px] text-emerald-600 font-semibold">All KPIs on target</p>
              <p className="text-[10px] text-slate-400">No critical issues in this department</p>
            </div>
          )}
        </div>
      )}

      {departments.length === 0 && (
        <div className="bg-white rounded-2xl border border-slate-100 shadow-sm p-10 text-center">
          <Users size={28} className="text-slate-200 mx-auto mb-2" />
          <p className="text-slate-500 text-sm font-semibold">No departments configured</p>
          <p className="text-slate-400 text-xs mt-1">Departments will be auto-created when data is loaded.</p>
        </div>
      )}
    </div>
  )
}
