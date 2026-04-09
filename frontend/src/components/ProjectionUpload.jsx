import { useState } from 'react'
import axios from 'axios'
import { Upload, CheckCircle2, AlertTriangle, FileSpreadsheet } from 'lucide-react'

export default function ProjectionUpload() {
  const [uploading, setUploading] = useState(false)
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [versionLabel, setVersionLabel] = useState('v1')

  async function handleUpload(e) {
    const file = e.target.files?.[0]
    if (!file) return
    setUploading(true)
    setError(null)
    setResult(null)
    try {
      const fd = new FormData()
      fd.append('file', file)
      fd.append('version_label', versionLabel)
      const r = await axios.post('/api/projection/upload', fd)
      setResult(r.data)
    } catch (err) {
      setError(err?.response?.data?.detail || 'Upload failed')
    } finally {
      setUploading(false)
    }
  }

  return (
    <div className="max-w-4xl mx-auto px-6 py-6 space-y-5">
      <div>
        <h1 className="text-xl font-bold text-slate-900 flex items-center gap-2">
          <FileSpreadsheet size={20} className="text-[#0055A4]" />
          Projection Upload
        </h1>
        <p className="text-[12px] text-slate-500 mt-0.5">
          Upload your budget/plan as CSV or XLSX. The platform will parse KPIs by month
          and make them available for Plan vs Actual comparison, scenario planning, and Excel export.
        </p>
        <span className="inline-block mt-1 text-[9px] font-bold text-amber-600 bg-amber-50 border border-amber-200 px-2 py-0.5 rounded-full">
          ROADMAPPED — Under Testing
        </span>
      </div>

      <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6 space-y-4">
        <div>
          <label className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Version Label</label>
          <input
            className="mt-1 w-48 border border-slate-200 rounded-lg px-3 py-2 text-sm"
            value={versionLabel}
            onChange={e => setVersionLabel(e.target.value)}
            placeholder="v1"
          />
          <p className="text-[10px] text-slate-400 mt-0.5">Use different labels to compare multiple projection versions.</p>
        </div>

        <div className="border-2 border-dashed border-slate-200 rounded-xl p-8 text-center">
          <Upload size={24} className="text-slate-300 mx-auto mb-2" />
          <label className="cursor-pointer">
            <span className="text-sm font-semibold text-[#0055A4] hover:underline">
              {uploading ? 'Uploading...' : 'Choose file (.csv or .xlsx)'}
            </span>
            <input type="file" accept=".csv,.xlsx,.xls" onChange={handleUpload} className="hidden" disabled={uploading} />
          </label>
          <p className="text-[10px] text-slate-400 mt-1">Same format as actuals — date column + KPI columns, or P&L sheet with months across top.</p>
        </div>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-xl px-4 py-3 flex items-start gap-2">
          <AlertTriangle size={14} className="text-red-500 mt-0.5" />
          <p className="text-[11px] text-red-700">{error}</p>
        </div>
      )}

      {result && (
        <div className="bg-emerald-50 border border-emerald-200 rounded-xl px-4 py-3">
          <div className="flex items-center gap-2 mb-2">
            <CheckCircle2 size={14} className="text-emerald-500" />
            <span className="text-[12px] font-bold text-emerald-700">{result.message}</span>
          </div>
          <div className="grid grid-cols-3 gap-3 text-[11px]">
            <div>
              <p className="text-slate-400">Months detected</p>
              <p className="font-bold text-slate-700">{result.months_detected}</p>
            </div>
            <div>
              <p className="text-slate-400">KPIs computed</p>
              <p className="font-bold text-slate-700">{result.kpis_computed?.length || 0}</p>
            </div>
            <div>
              <p className="text-slate-400">Version</p>
              <p className="font-bold text-slate-700">{result.version_label}</p>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
