import { useState, useRef, useEffect } from 'react'
import axios from 'axios'
import { Upload, CheckCircle, AlertCircle, FileText, Trash2, GitBranch, Activity, AlertTriangle, Download, Database, Zap } from 'lucide-react'

const ACTUALS_TEMPLATE = `date,revenue,cogs,opex,ar,customers,churn,is_recurring,sm_allocated,arr
2025-01-15,25000,9500,7200,28000,1,0,1,3200,18000
2025-01-22,18000,7000,5400,20000,1,0,0,2400,0
2025-02-10,32000,12000,9100,35000,1,0,1,4000,24000
`

const PROJECTION_TEMPLATE = `date,revenue,cogs,opex,ar,customers,churn,is_recurring,sm_allocated,arr
2025-01-15,28000,10000,7500,30000,1,0,1,3400,20000
2025-01-22,20000,7500,5600,22000,1,0,0,2600,0
2025-02-10,36000,13000,9500,38000,1,0,1,4200,26000
`

function downloadTemplate(content, filename) {
  const blob = new Blob([content], { type: 'text/csv' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url; a.download = filename; a.click()
  URL.revokeObjectURL(url)
}

export default function CSVUpload({ onUploaded }) {

  // ── KPI Export / Import state ─────────────────────────────────────────────
  const [exporting,    setExporting]    = useState(false)
  const [importing,    setImporting]    = useState(false)
  const [importResult, setImportResult] = useState(null)
  const [importError,  setImportError]  = useState(null)
  const importRef = useRef()

  async function handleExport() {
    setExporting(true)
    try {
      const r = await axios.get('/api/export/data.xlsx', { responseType: 'blob' })
      const url = URL.createObjectURL(new Blob([r.data]))
      const a = document.createElement('a')
      a.href = url; a.download = 'axiom_kpi_data.xlsx'; a.click()
      URL.revokeObjectURL(url)
    } catch { /* silently ignore */ }
    setExporting(false)
  }

  async function handleImportFile(file) {
    if (!/\.(xlsx|csv)$/i.test(file?.name || '')) { setImportError('Please select the .xlsx or .csv file you exported.'); return }
    setImporting(true); setImportResult(null); setImportError(null)
    const fd = new FormData(); fd.append('file', file)
    try {
      const r = await axios.post('/api/import/data', fd)
      setImportResult(r.data); onUploaded?.()
    } catch(e) { setImportError(e.response?.data?.detail || 'Import failed') }
    setImporting(false)
  }

  // ── Actuals state ────────────────────────────────────────────────────────
  const [dragging,    setDragging]    = useState(false)
  const [result,      setResult]      = useState(null)
  const [error,       setError]       = useState(null)
  const [loading,     setLoading]     = useState(false)
  const [seeding,     setSeeding]     = useState(false)
  const [uploads,     setUploads]     = useState([])
  const fileRef = useRef()

  async function fetchUploads() {
    try { const r = await axios.get('/api/uploads'); setUploads(r.data) } catch {}
  }
  async function handleFile(file) {
    if (!/\.(csv|xlsx|xls)$/i.test(file?.name || '')) { setError('Please upload a .csv or .xlsx file'); return }
    setLoading(true); setResult(null); setError(null)
    const fd = new FormData(); fd.append('file', file)
    try {
      const r = await axios.post('/api/upload', fd)
      setResult(r.data); onUploaded?.(); fetchUploads()
    } catch(e) { setError(e.response?.data?.detail || 'Upload failed') }
    setLoading(false)
  }
  async function deleteUpload(id) {
    await axios.delete(`/api/uploads/${id}`); fetchUploads(); onUploaded?.()
  }
  async function seedDemoActuals() {
    setSeeding(true); setResult(null); setError(null)
    try {
      const r = await axios.get('/api/seed-demo-actuals')
      setResult({ message: r.data.message, rows_processed: r.data.transactions,
                  months_detected: r.data.months, upload_id: r.data.upload_id, kpis_computed: [] })
      onUploaded?.(); fetchUploads()
    } catch(e) { setError(e.response?.data?.detail || 'Seed failed') }
    setSeeding(false)
  }

  // ── Projection state ─────────────────────────────────────────────────────
  const [projDragging, setProjDragging] = useState(false)
  const [projResult,   setProjResult]   = useState(null)
  const [projError,    setProjError]    = useState(null)
  const [projLoading,  setProjLoading]  = useState(false)
  const [projSeeding,  setProjSeeding]  = useState(false)
  const [projUploads,  setProjUploads]  = useState([])
  const projFileRef = useRef()

  async function fetchProjUploads() {
    try { const r = await axios.get('/api/projection/uploads'); setProjUploads(r.data) } catch {}
  }
  async function handleProjFile(file) {
    if (!/\.(csv|xlsx|xls)$/i.test(file?.name || '')) { setProjError('Please upload a .csv or .xlsx file'); return }
    setProjLoading(true); setProjResult(null); setProjError(null)
    const fd = new FormData(); fd.append('file', file)
    try {
      const r = await axios.post('/api/projection/upload', fd)
      setProjResult(r.data); onUploaded?.(); fetchProjUploads()
    } catch(e) { setProjError(e.response?.data?.detail || 'Upload failed') }
    setProjLoading(false)
  }
  async function deleteProjUpload(id) {
    await axios.delete(`/api/projection/uploads/${id}`); fetchProjUploads(); onUploaded?.()
  }
  async function seedDemoProjection() {
    setProjSeeding(true); setProjError(null)
    try { await axios.get('/api/seed-demo-projection'); fetchProjUploads(); onUploaded?.() }
    catch { setProjError('Seed failed') }
    setProjSeeding(false)
  }

  // ── Full demo data (5 years + targets) ────────────────────────────────────
  const [fullSeeding, setFullSeeding] = useState(false)
  async function seedFullDemo() {
    setFullSeeding(true)
    try { await axios.get('/api/seed-multiyear'); onUploaded?.(); fetchUploads(); fetchProjUploads() }
    catch { /* silently ignore */ }
    setFullSeeding(false)
  }

  // Fetch both on mount
  useEffect(() => { fetchUploads(); fetchProjUploads() }, [])

  return (
    <div className="space-y-10 max-w-3xl">

      {/* ══ Quick Start: Load Demo Data ════════════════════════════════ */}
      <div className="card p-5 border border-blue-100 bg-gradient-to-r from-blue-50/60 to-slate-50/40">
        <div className="flex items-start gap-4">
          <div className="w-10 h-10 rounded-xl bg-[#0055A4] flex items-center justify-center flex-shrink-0">
            <Zap size={18} className="text-white" />
          </div>
          <div className="flex-1">
            <h2 className="text-slate-800 font-semibold text-base mb-1">Quick Start with Demo Data</h2>
            <p className="text-slate-500 text-xs leading-relaxed mb-3">
              Load 5 years of realistic B2B SaaS data with pre-configured KPI targets. Explore every feature of the platform instantly.
            </p>
            <button
              onClick={seedFullDemo}
              disabled={fullSeeding}
              className="flex items-center gap-2 px-4 py-2 bg-[#0055A4] hover:bg-[#003d80] text-white text-sm font-semibold rounded-xl transition-all disabled:opacity-60 shadow-sm hover:shadow-md"
            >
              {fullSeeding
                ? <><div className="w-3 h-3 rounded-full border-2 border-white/40 border-t-white animate-spin"/>Loading demo data...</>
                : <><Zap size={14}/> Load Demo Data (5 years)</>
              }
            </button>
          </div>
        </div>
      </div>

      {/* ══ Section 0: Export / Import KPI Model Data ════════════════════ */}
      <div className="card p-6 border border-emerald-100 bg-emerald-50/30 space-y-4">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <span className="w-1 h-5 bg-emerald-500 rounded-full"/>
            <h2 className="text-slate-800 font-semibold text-base">Export &amp; Edit KPI Model Data</h2>
          </div>
          <p className="text-slate-500 text-xs pl-3">
            Download all computed monthly KPI values as an Excel file, edit them to create test scenarios, then re-upload to see how the signals and forecast respond.
          </p>
        </div>

        <div className="flex flex-wrap gap-3">
          <button
            onClick={handleExport}
            disabled={exporting}
            className="flex items-center gap-2 px-4 py-2 rounded-xl text-sm font-medium
                       bg-emerald-600 hover:bg-emerald-700 text-white transition-all disabled:opacity-50">
            <Download size={14}/>
            {exporting ? 'Downloading…' : 'Download KPI Data (.xlsx)'}
          </button>

          <button
            onClick={() => { setImportResult(null); setImportError(null); importRef.current?.click() }}
            disabled={importing}
            className="flex items-center gap-2 px-4 py-2 rounded-xl text-sm font-medium
                       border border-emerald-300 text-emerald-700 bg-white hover:bg-emerald-50 transition-all disabled:opacity-50">
            <Upload size={14}/>
            {importing ? 'Importing…' : 'Upload Edited File'}
          </button>
          <input ref={importRef} type="file" accept=".xlsx,.csv" className="hidden"
            onChange={e => handleImportFile(e.target.files[0])}/>
        </div>

        {importResult && (
          <div className="flex items-center gap-2 px-4 py-3 bg-emerald-100 rounded-xl border border-emerald-200">
            <CheckCircle size={15} className="text-emerald-600 shrink-0"/>
            <span className="text-emerald-700 text-sm font-medium">
              Imported {importResult.rows_imported} month{importResult.rows_imported !== 1 ? 's' : ''} of KPI data successfully.
            </span>
          </div>
        )}
        {importError && (
          <div className="flex items-center gap-2 px-4 py-3 bg-red-50 rounded-xl border border-red-200">
            <AlertCircle size={15} className="text-red-500 shrink-0"/>
            <span className="text-red-700 text-sm">{importError}</span>
          </div>
        )}

        <div className="text-[11px] text-slate-400 bg-white border border-slate-100 rounded-lg px-3 py-2 space-y-0.5">
          <p><span className="font-semibold text-slate-600">Tip:</span> The Excel file has one row per month. Edit the numeric values — do not rename the column headers or change the Year/Month columns.</p>
          <p>After re-uploading, refresh the dashboard to see the updated signals.</p>
        </div>
      </div>

      {/* ══ Section 1: Actuals Data ═══════════════════════════════════════ */}
      <div className="space-y-5">
        <div>
          <div className="flex items-center justify-between mb-1">
            <div className="flex items-center gap-2">
              <span className="w-1 h-5 bg-[#0055A4] rounded-full"/>
              <h2 className="text-slate-800 font-semibold text-base">Actuals Data</h2>
            </div>
            <button
              onClick={() => downloadTemplate(ACTUALS_TEMPLATE, 'actuals_template.csv')}
              className="flex items-center gap-1.5 text-xs text-[#0055A4] border border-[#0055A4]/25
                         bg-[#0055A4]/5 hover:bg-[#0055A4]/15 px-3 py-1.5 rounded-lg transition-all">
              <Download size={12}/> Download Template
            </button>
          </div>
          <p className="text-slate-500 text-xs pl-3">
            Upload a CSV of raw transactions to compute all 18 KPIs. Column names are auto-detected.
          </p>
        </div>

        {/* Drop zone */}
        <div
          onDragOver={e => { e.preventDefault(); setDragging(true) }}
          onDragLeave={() => setDragging(false)}
          onDrop={e => { e.preventDefault(); setDragging(false); handleFile(e.dataTransfer.files[0]) }}
          onClick={() => fileRef.current?.click()}
          className={`border-2 border-dashed rounded-2xl p-12 text-center cursor-pointer transition-all ${
            dragging ? 'border-[#0055A4] bg-blue-50' : 'border-slate-200 hover:border-slate-300 hover:bg-slate-50'
          }`}
        >
          <input ref={fileRef} type="file" accept=".csv,.xlsx,.xls" className="hidden"
            onChange={e => handleFile(e.target.files[0])}/>
          {loading
            ? <div className="flex flex-col items-center gap-3">
                <div className="w-10 h-10 border-4 border-[#0055A4] border-t-transparent rounded-full animate-spin"/>
                <p className="text-slate-500 text-sm">Processing…</p>
              </div>
            : <div className="flex flex-col items-center gap-3">
                <Upload size={36} className="text-slate-400"/>
                <p className="text-slate-700 font-medium">Drop CSV or Excel file here, or click to browse</p>
                <p className="text-slate-400 text-xs">
                  Accepts: date, revenue, cogs, opex, ar, mrr, arr, customers, churn, is_recurring, sm_allocated, headcount
                </p>
              </div>
          }
        </div>

        {/* Demo seed */}
        <div className="flex items-center gap-3">
          <div className="flex-1 h-px bg-slate-200"/>
          <span className="text-[10px] text-slate-400 uppercase tracking-wide">or load demo data</span>
          <div className="flex-1 h-px bg-slate-200"/>
        </div>
        <button
          onClick={seedDemoActuals}
          disabled={seeding}
          className="w-full flex items-center justify-center gap-2 py-2.5 rounded-xl font-medium
                     text-sm bg-[#0055A4]/8 border border-[#0055A4]/25 text-[#0055A4]
                     hover:bg-[#0055A4]/15 transition-all disabled:opacity-50">
          <Database size={14}/>
          {seeding ? 'Seeding…' : 'Load Demo Actuals — 36 months of B2B SaaS growth data (2022–2024)'}
        </button>

        {result && (
          <div className="card p-5 border-l-4 border-l-emerald-500 bg-emerald-50/60">
            <div className="flex items-center gap-2 mb-3">
              <CheckCircle size={16} className="text-emerald-600"/>
              <span className="text-emerald-700 font-medium text-sm">{result.message}</span>
            </div>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              {[['Rows', result.rows_processed], ['Months', result.months_detected],
                ['KPIs', result.kpis_computed?.length ?? '—'], ['Upload', `#${result.upload_id}`]].map(([l, v]) => (
                <div key={l} className="bg-white rounded-lg p-3 text-center border border-emerald-100">
                  <div className="text-slate-800 font-bold">{v}</div>
                  <div className="text-slate-500 text-xs">{l}</div>
                </div>
              ))}
            </div>
            {result.kpis_computed?.length > 0 && (
              <div className="mt-3">
                <p className="text-slate-500 text-xs mb-1">KPIs computed:</p>
                <div className="flex flex-wrap gap-1.5">
                  {result.kpis_computed.map(k => (
                    <span key={k} className="px-2 py-0.5 bg-emerald-100 text-emerald-700 rounded text-xs font-medium">{k}</span>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {error && (
          <div className="card p-4 border-l-4 border-l-red-500 bg-red-50/60 flex items-center gap-2">
            <AlertCircle size={16} className="text-red-500 shrink-0"/>
            <p className="text-red-700 text-sm">{error}</p>
          </div>
        )}

        {/* Expected format */}
        <div className="card p-5">
          <h3 className="text-slate-700 text-sm font-semibold mb-3">Expected CSV Format</h3>
          <div className="overflow-x-auto">
            <table className="text-xs text-left w-full">
              <thead>
                <tr className="border-b border-slate-100">
                  {['Column','Aliases','Required','Used For'].map(h => (
                    <th key={h} className="py-2 pr-6 text-slate-500 font-semibold">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-50">
                {[
                  ['date',         'transaction_date, month, period',      '✓', 'Month grouping'],
                  ['revenue',      'sales, total_revenue, net_revenue',    '✓', 'Most KPIs'],
                  ['cogs',         'cost_of_goods_sold, cost',             '',  'Gross/Operating Margin'],
                  ['opex',         'operating_expenses, sg_and_a',         '',  'Margins, OpEx Ratio'],
                  ['ar',           'accounts_receivable, receivables',     '',  'DSO, Cash Cycle'],
                  ['customers',    'customer_count, clients',              '',  'Churn, CAC Payback'],
                  ['churn',        'churned_customers, lost_customers',    '',  'Churn Rate, NRR'],
                  ['is_recurring', 'recurring (0/1)',                      '',  'Revenue Quality'],
                  ['sm_allocated', 'sales_marketing, s_m',                 '',  'Sales Efficiency'],
                  ['arr',          'annual_recurring_revenue',             '',  'ARR Growth'],
                ].map(([col, alias, req, use]) => (
                  <tr key={col} className="hover:bg-slate-50/60">
                    <td className="py-2 pr-6 text-[#0055A4] font-mono font-medium">{col}</td>
                    <td className="py-2 pr-6 text-slate-500">{alias}</td>
                    <td className="py-2 pr-6 text-amber-600 font-bold">{req}</td>
                    <td className="py-2 pr-6 text-slate-600">{use}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Actuals history */}
        {uploads.length > 0 && (
          <div className="card p-5">
            <h3 className="text-slate-700 text-sm font-semibold mb-3">Actuals Upload History</h3>
            <div className="space-y-1">
              {uploads.map(u => (
                <div key={u.id} className="flex items-center justify-between py-2.5 border-b border-slate-100 last:border-0">
                  <div className="flex items-center gap-2">
                    <FileText size={14} className="text-slate-400"/>
                    <div>
                      <p className="text-slate-700 text-sm font-medium">{u.filename}</p>
                      <p className="text-slate-400 text-xs">{u.row_count} rows · {new Date(u.uploaded_at).toLocaleString()}</p>
                    </div>
                  </div>
                  <button onClick={() => deleteUpload(u.id)}
                    className="p-1.5 rounded hover:bg-red-50 text-slate-400 hover:text-red-500 transition-colors">
                    <Trash2 size={13}/>
                  </button>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* ── Divider ─────────────────────────────────────────────────────── */}
      <div className="flex items-center gap-3">
        <div className="flex-1 h-px bg-slate-200"/>
        <span className="text-[10px] text-slate-400 uppercase tracking-widest font-medium flex items-center gap-1.5">
          <GitBranch size={11}/> Projection Data
        </span>
        <div className="flex-1 h-px bg-slate-200"/>
      </div>

      {/* ══ Section 2: Projection Data ════════════════════════════════════ */}
      <div className="space-y-5">
        <div>
          <div className="flex items-center justify-between mb-1">
            <div className="flex items-center gap-2">
              <span className="w-1 h-5 bg-blue-400 rounded-full"/>
              <h2 className="text-slate-800 font-semibold text-base">Projection Data</h2>
              <span className="text-[10px] text-amber-700 bg-amber-50 border border-amber-200 px-2 py-0.5 rounded-full">
                replaces existing
              </span>
            </div>
            <button
              onClick={() => downloadTemplate(PROJECTION_TEMPLATE, 'projection_template.csv')}
              className="flex items-center gap-1.5 text-xs text-blue-600 border border-blue-200
                         bg-blue-50 hover:bg-blue-100 px-3 py-1.5 rounded-lg transition-all">
              <Download size={12}/> Download Template
            </button>
          </div>
          <p className="text-slate-500 text-xs pl-3">
            Upload a 12-month projection CSV (same format as actuals) to unlock Bridge Analysis — gap waterfall charts, root cause diagnostics, and corrective action playbooks.
          </p>
        </div>

        {/* Projection drop zone */}
        <div
          onDragOver={e => { e.preventDefault(); setProjDragging(true) }}
          onDragLeave={() => setProjDragging(false)}
          onDrop={e => { e.preventDefault(); setProjDragging(false); handleProjFile(e.dataTransfer.files[0]) }}
          onClick={() => projFileRef.current?.click()}
          className={`border-2 border-dashed rounded-2xl p-10 text-center cursor-pointer transition-all ${
            projDragging ? 'border-blue-400 bg-blue-50' : 'border-slate-200 hover:border-blue-300 hover:bg-blue-50/40'
          }`}
        >
          <input ref={projFileRef} type="file" accept=".csv,.xlsx,.xls" className="hidden"
            onChange={e => handleProjFile(e.target.files[0])}/>
          {projLoading
            ? <div className="flex flex-col items-center gap-3">
                <div className="w-10 h-10 border-4 border-blue-500 border-t-transparent rounded-full animate-spin"/>
                <p className="text-slate-500 text-sm">Processing projection…</p>
              </div>
            : <div className="flex flex-col items-center gap-3">
                <GitBranch size={32} className="text-slate-400"/>
                <p className="text-slate-700 font-medium">Drop projection CSV here or click to browse</p>
                <p className="text-slate-400 text-xs">Same format as actuals — 12-month plan transactions</p>
              </div>
          }
        </div>

        {/* Demo seed */}
        <div className="flex items-center gap-3">
          <div className="flex-1 h-px bg-slate-200"/>
          <span className="text-[10px] text-slate-400 uppercase tracking-wide">or</span>
          <div className="flex-1 h-px bg-slate-200"/>
        </div>
        <button
          onClick={seedDemoProjection}
          disabled={projSeeding}
          className="w-full flex items-center justify-center gap-2 py-2.5 rounded-xl font-medium
                     text-sm bg-[#0055A4]/8 border border-[#0055A4]/25 text-[#0055A4]
                     hover:bg-[#0055A4]/15 transition-all disabled:opacity-50">
          <Activity size={14}/>
          {projSeeding ? 'Seeding…' : 'Load Demo Projection (12 months of optimistic plan data)'}
        </button>

        {projResult && (
          <div className="card p-5 border-l-4 border-l-blue-500 bg-blue-50/60">
            <div className="flex items-center gap-2 mb-2">
              <CheckCircle size={16} className="text-blue-600"/>
              <span className="text-blue-700 font-medium text-sm">
                Projection loaded — {projResult.rows_processed} rows · {projResult.months_detected} months
              </span>
            </div>
            <p className="text-blue-600 text-xs">Navigate to Bridge Analysis to view the projection vs actual comparison.</p>
          </div>
        )}

        {projError && (
          <div className="card p-4 border-l-4 border-l-red-500 bg-red-50/60 flex items-center gap-2">
            <AlertTriangle size={16} className="text-red-500 shrink-0"/>
            <p className="text-red-700 text-sm">{projError}</p>
          </div>
        )}

        {/* Projection history */}
        {projUploads.length > 0 && (
          <div className="card p-5">
            <h3 className="text-slate-700 text-sm font-semibold mb-1">Active Projection</h3>
            <p className="text-slate-400 text-xs mb-3">Only one projection is active at a time. Uploading a new file replaces the current one.</p>
            <div className="space-y-1">
              {projUploads.map(u => (
                <div key={u.id} className="flex items-center justify-between py-2.5 border-b border-slate-100 last:border-0">
                  <div className="flex items-center gap-2">
                    <GitBranch size={14} className="text-blue-400"/>
                    <div>
                      <p className="text-slate-700 text-sm font-medium">{u.filename}</p>
                      <p className="text-slate-400 text-xs">{u.row_count.toLocaleString()} rows · {new Date(u.uploaded_at).toLocaleString()}</p>
                    </div>
                  </div>
                  <button onClick={() => deleteProjUpload(u.id)}
                    className="p-1.5 rounded hover:bg-red-50 text-slate-400 hover:text-red-500 transition-colors">
                    <Trash2 size={13}/>
                  </button>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
