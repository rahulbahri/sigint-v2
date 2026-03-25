import { useState } from 'react'
import { Copy, CheckCheck, ExternalLink } from 'lucide-react'

const ENDPOINTS = [
  {
    method: 'GET', path: '/api/health',
    desc: 'Health check',
    response: `{ "status": "ok", "timestamp": "2025-01-15T..." }`,
  },
  {
    method: 'GET', path: '/api/kpi-definitions',
    desc: 'List all 18 Priority-1 KPI definitions with formulas, targets, units.',
    response: `[{ "key": "gross_margin", "name": "Gross Margin %", "unit": "pct", "direction": "higher", "target": 62.0, "formula": "..." }]`,
  },
  {
    method: 'GET', path: '/api/kpi-definitions/{kpi_key}',
    desc: 'Single KPI definition by key (e.g. gross_margin, churn_rate).',
    response: `{ "key": "gross_margin", "name": "Gross Margin %", ... }`,
  },
  {
    method: 'GET', path: '/api/monthly?year=2025',
    desc: 'Computed monthly KPI values. Optional ?year= filter.',
    response: `[{ "year": 2025, "month": 1, "kpis": { "gross_margin": 62.4, ... } }]`,
  },
  {
    method: 'GET', path: '/api/fingerprint?year=2025',
    desc: 'Full organisational fingerprint: 12-month profile per KPI, trend direction, green/yellow/red status.',
    response: `[{ "key": "gross_margin", "avg": 62.6, "trend": "up", "fy_status": "green", "monthly": [...] }]`,
  },
  {
    method: 'GET', path: '/api/summary',
    desc: 'Dashboard summary: upload count, KPIs tracked, status breakdown.',
    response: `{ "uploads": 2, "kpis_tracked": 16, "status_breakdown": { "green": 10, "yellow": 4, "red": 2 } }`,
  },
  {
    method: 'POST', path: '/api/upload',
    desc: 'Upload a CSV file (multipart/form-data, field: file). Returns detected columns and computed KPIs.',
    body: `curl -X POST /api/upload -F "file=@transactions.csv"`,
    response: `{ "upload_id": 3, "rows_processed": 500, "months_detected": 12, "kpis_computed": ["gross_margin", ...] }`,
  },
  {
    method: 'GET', path: '/api/uploads',
    desc: 'List all previously uploaded files.',
    response: `[{ "id": 1, "filename": "data.csv", "row_count": 500, "uploaded_at": "..." }]`,
  },
  {
    method: 'DELETE', path: '/api/uploads/{id}',
    desc: 'Delete an upload and its associated monthly KPI data.',
    response: `{ "deleted": 1 }`,
  },
  {
    method: 'PUT', path: '/api/targets/{kpi_key}?target_value=65',
    desc: 'Update the target value for a KPI (used for performance thresholds).',
    body: `curl -X PUT "/api/targets/gross_margin?target_value=65"`,
    response: `{ "kpi_key": "gross_margin", "target_value": 65.0 }`,
  },
  {
    method: 'GET', path: '/api/seed-demo',
    desc: 'Load 12 months of synthetic FY2025 demo data (matches Excel model).',
    response: `{ "seeded": true, "months": 12, "upload_id": 1 }`,
  },
]

const METHOD_COLORS = {
  GET:    'bg-blue-50 text-blue-700 border-blue-200',
  POST:   'bg-emerald-50 text-emerald-700 border-emerald-200',
  PUT:    'bg-amber-50 text-amber-700 border-amber-200',
  DELETE: 'bg-red-50 text-red-700 border-red-200',
}

function CopyBtn({ text }) {
  const [copied, setCopied] = useState(false)
  const copy = () => { navigator.clipboard.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 1500) }
  return (
    <button onClick={copy} className="p-1 rounded hover:bg-slate-100 text-slate-400 hover:text-slate-600 transition-colors">
      {copied ? <CheckCheck size={13} className="text-emerald-500"/> : <Copy size={13}/>}
    </button>
  )
}

export default function APIReference({ kpiDefs }) {
  const [open, setOpen] = useState(null)
  const base = window.location.origin

  return (
    <div className="space-y-6 max-w-4xl">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-slate-800 font-semibold text-base mb-1 flex items-center gap-2">
            <span className="w-1 h-5 bg-[#0055A4] rounded-full"/>
            API Reference
          </h2>
          <p className="text-slate-500 text-xs">Base URL: <code className="text-[#0055A4] font-mono">{base}</code></p>
        </div>
        <a href="/api/docs" target="_blank"
           className="flex items-center gap-1.5 px-3 py-2 text-xs rounded-lg border border-[#0055A4]/30 text-[#0055A4] hover:bg-blue-50 transition-colors font-medium">
          <ExternalLink size={12}/> Swagger UI
        </a>
      </div>

      {/* KPI Keys reference */}
      <div className="card p-5">
        <h3 className="text-slate-700 text-sm font-semibold mb-3">Available KPI Keys</h3>
        <div className="flex flex-wrap gap-2">
          {kpiDefs.map(k => (
            <div key={k.key} className="flex items-center gap-1.5 px-2.5 py-1 bg-slate-50 rounded-lg border border-slate-200">
              <code className="text-[#0055A4] text-xs font-mono font-medium">{k.key}</code>
              <span className="text-slate-300 text-xs">·</span>
              <span className="text-slate-500 text-xs">{k.unit}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Endpoints */}
      <div className="space-y-2">
        {ENDPOINTS.map((ep, i) => (
          <div key={i} className="card overflow-hidden">
            <button
              onClick={() => setOpen(open === i ? null : i)}
              className="w-full flex items-center gap-3 p-4 text-left hover:bg-slate-50/70 transition-colors"
            >
              <span className={`px-2 py-0.5 rounded text-xs font-bold border ${METHOD_COLORS[ep.method]}`}>
                {ep.method}
              </span>
              <code className="text-slate-700 text-sm font-mono">{ep.path}</code>
              <span className="text-slate-400 text-xs ml-2 flex-1 truncate">{ep.desc}</span>
              <span className="text-slate-400 text-xs flex-shrink-0">{open === i ? '▲' : '▼'}</span>
            </button>

            {open === i && (
              <div className="px-4 pb-4 space-y-3 border-t border-slate-100">
                <p className="text-slate-600 text-sm pt-3">{ep.desc}</p>

                {ep.body && (
                  <div>
                    <p className="text-slate-400 text-xs mb-1 font-semibold uppercase tracking-wider">Example Request</p>
                    <div className="relative bg-slate-800 rounded-lg p-3">
                      <pre className="text-emerald-400 text-xs overflow-x-auto">{ep.body}</pre>
                      <div className="absolute top-2 right-2">
                        <button onClick={() => navigator.clipboard.writeText(ep.body)}
                          className="p-1 rounded hover:bg-white/10 text-slate-400 hover:text-slate-200 transition-colors">
                          <Copy size={13}/>
                        </button>
                      </div>
                    </div>
                  </div>
                )}

                <div>
                  <p className="text-slate-400 text-xs mb-1 font-semibold uppercase tracking-wider">Response</p>
                  <div className="relative bg-slate-800 rounded-lg p-3">
                    <pre className="text-blue-300 text-xs overflow-x-auto">{ep.response}</pre>
                    <div className="absolute top-2 right-2">
                      <button onClick={() => navigator.clipboard.writeText(ep.response)}
                        className="p-1 rounded hover:bg-white/10 text-slate-400 hover:text-slate-200 transition-colors">
                        <Copy size={13}/>
                      </button>
                    </div>
                  </div>
                </div>

                <div className="flex gap-2">
                  <a href={`${base}${ep.path.split('?')[0].replace('{kpi_key}','gross_margin').replace('{id}','1')}`}
                     target="_blank" rel="noreferrer"
                     className="text-xs text-[#0055A4] hover:underline flex items-center gap-1 font-medium">
                    Try it <ExternalLink size={10}/>
                  </a>
                </div>
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Integration snippet */}
      <div className="card p-5">
        <h3 className="text-slate-700 text-sm font-semibold mb-3">Integration Snippet</h3>
        <div className="relative bg-slate-800 rounded-lg p-4">
          <pre className="text-slate-200 text-xs overflow-x-auto">{`// Fetch KPI fingerprint
const res = await fetch('${base}/api/fingerprint?year=2025')
const fingerprint = await res.json()

// Upload new CSV
const fd = new FormData()
fd.append('file', csvFile)
await fetch('${base}/api/upload', { method: 'POST', body: fd })

// Update a target
await fetch('${base}/api/targets/gross_margin?target_value=65', { method: 'PUT' })`}
          </pre>
          <div className="absolute top-2 right-2">
            <CopyBtn text={`const res = await fetch('${base}/api/fingerprint?year=2025')\nconst fingerprint = await res.json()`}/>
          </div>
        </div>
      </div>
    </div>
  )
}
