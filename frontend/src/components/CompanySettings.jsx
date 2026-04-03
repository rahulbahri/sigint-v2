import { useState, useEffect, useRef } from 'react'
import axios from 'axios'
import { Upload, Building2, Check, AlertCircle, Sliders, RotateCcw } from 'lucide-react'

const STAGES = [
  { id: 'seed',     label: 'Seed',     desc: 'Pre-revenue or early traction' },
  { id: 'series_a', label: 'Series A', desc: 'Product-market fit, scaling' },
  { id: 'series_b', label: 'Series B', desc: 'Scaling GTM and operations' },
  { id: 'series_c', label: 'Series C+', desc: 'Growth, efficiency, path to profitability' },
]

export default function CompanySettings({ onSave }) {
  const [companyName, setCompanyName]   = useState('')
  const [industry, setIndustry]         = useState('')
  const [stage, setStage]               = useState(() => localStorage.getItem('axiom_stage') || 'series_b')
  const [logo, setLogo]                 = useState(null)       // preview URL
  const [saving, setSaving]             = useState(false)
  const [status, setStatus]             = useState(null)       // 'ok' | 'error'
  const [logoUploading, setLogoUploading] = useState(false)
  const [logoError, setLogoError]       = useState(null)
  const fileRef = useRef(null)

  // Composite criticality weights
  const DEFAULT_CW = { gap: 25, trend: 25, impact: 30, domain: 20 }
  const [critWeights, setCritWeights] = useState({ ...DEFAULT_CW })
  const [cwSaved, setCwSaved]         = useState(false)

  // Load current settings on mount
  useEffect(() => {
    axios.get('/api/company-settings')
      .then(r => {
        setCompanyName(r.data.company_name || '')
        setIndustry(r.data.industry || '')
        if (r.data.logo) setLogo(r.data.logo)
        if (r.data.company_stage) {
          setStage(r.data.company_stage)
          localStorage.setItem('axiom_stage', r.data.company_stage)
        }
        if (r.data.criticality_weights) {
          try {
            const cw = JSON.parse(r.data.criticality_weights)
            setCritWeights({ gap: cw.gap ?? 25, trend: cw.trend ?? 25, impact: cw.impact ?? 30, domain: cw.domain ?? 20 })
          } catch {}
        }
      })
      .catch(() => {})
  }, [])

  async function handleSave() {
    setSaving(true)
    setStatus(null)
    try {
      await axios.put('/api/company-settings', {
        company_name: companyName,
        industry,
        company_stage: stage,
        criticality_weights: JSON.stringify(critWeights),
      })
      localStorage.setItem('axiom_stage', stage)
      setStatus('ok')
      if (onSave) onSave({ company_name: companyName, industry, company_stage: stage })
    } catch {
      setStatus('error')
    } finally {
      setSaving(false)
    }
  }

  async function handleLogoUpload(e) {
    const file = e.target.files?.[0]
    if (!file) return
    // 5 MB client-side guard — gives a friendlier message before hitting the server
    if (file.size > 5 * 1024 * 1024) {
      setLogoError('File is too large. Please use an image under 5 MB.')
      return
    }
    setLogoError(null)
    setLogoUploading(true)
    const formData = new FormData()
    formData.append('file', file)
    try {
      // ⚠️  Do NOT set Content-Type manually — axios must let the browser set it
      //     so the multipart boundary is included in the header automatically.
      const r = await axios.post('/api/company-settings/logo', formData)
      setLogo(r.data.logo)
      if (onSave) onSave({ logo: r.data.logo })
    } catch (err) {
      const detail = err?.response?.data?.detail || 'Upload failed — please try again.'
      setLogoError(detail)
    } finally {
      setLogoUploading(false)
    }
  }

  return (
    <div className="max-w-xl mx-auto space-y-6">
      <div>
        <h2 className="text-lg font-bold text-slate-800 mb-1">Company Settings</h2>
        <p className="text-sm text-slate-500">Customise how your organisation appears in the platform.</p>
      </div>

      {/* Logo upload */}
      <div className="bg-white rounded-xl border border-slate-200 p-5 space-y-4">
        <h3 className="text-sm font-semibold text-slate-700 flex items-center gap-2">
          <Building2 size={15} className="text-[#0055A4]"/> Company Logo
        </h3>
        <div className="flex items-center gap-4">
          <div className="w-16 h-16 rounded-xl border-2 border-dashed border-slate-200
                          flex items-center justify-center bg-slate-50 overflow-hidden flex-shrink-0">
            {logo
              ? <img src={logo} alt="Company logo" className="w-full h-full object-contain"/>
              : <span className="text-slate-300 text-2xl font-bold">AX</span>
            }
          </div>
          <div className="flex-1 space-y-2">
            <p className="text-xs text-slate-500">PNG, JPG or SVG — displayed in the sidebar. Max 1 MB recommended.</p>
            <button
              onClick={() => { setLogoError(null); fileRef.current?.click() }}
              disabled={logoUploading}
              className="flex items-center gap-2 px-3 py-1.5 rounded-lg border border-slate-200
                         text-xs font-medium text-slate-600 hover:border-[#0055A4] hover:text-[#0055A4]
                         transition-all disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <Upload size={12}/>
              {logoUploading ? 'Uploading…' : 'Upload Logo'}
            </button>
            {logoError && (
              <p className="text-xs text-red-500 flex items-center gap-1 mt-1">
                <AlertCircle size={11}/> {logoError}
              </p>
            )}
            <input
              ref={fileRef}
              type="file"
              accept="image/png,image/jpeg,image/svg+xml,image/webp"
              className="hidden"
              onChange={handleLogoUpload}
            />
          </div>
        </div>
      </div>

      {/* Text fields */}
      <div className="bg-white rounded-xl border border-slate-200 p-5 space-y-4">
        <h3 className="text-sm font-semibold text-slate-700">Organisation Details</h3>

        <div className="space-y-1">
          <label className="block text-xs font-semibold text-slate-600 uppercase tracking-wide">
            Company Name
          </label>
          <input
            type="text"
            value={companyName}
            onChange={e => setCompanyName(e.target.value)}
            placeholder="e.g. Acme Corp"
            className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm
                       text-slate-800 placeholder:text-slate-300
                       focus:outline-none focus:ring-2 focus:ring-[#0055A4]/30 focus:border-[#0055A4]
                       transition-all"
          />
        </div>

        <div className="space-y-1">
          <label className="block text-xs font-semibold text-slate-600 uppercase tracking-wide">
            Industry / Sector
          </label>
          <input
            type="text"
            value={industry}
            onChange={e => setIndustry(e.target.value)}
            placeholder="e.g. SaaS, FinTech, Healthcare"
            className="w-full px-3 py-2 rounded-lg border border-slate-200 text-sm
                       text-slate-800 placeholder:text-slate-300
                       focus:outline-none focus:ring-2 focus:ring-[#0055A4]/30 focus:border-[#0055A4]
                       transition-all"
          />
        </div>
      </div>

      {/* Funding Stage */}
      <div className="bg-white rounded-xl border border-slate-200 p-5 space-y-4">
        <div>
          <h3 className="text-sm font-semibold text-slate-700 mb-1">Funding Stage</h3>
          <p className="text-xs text-slate-500">Used to calibrate KPI benchmarks to your stage. Affects variance analysis and board pack targets.</p>
        </div>
        <div className="grid grid-cols-2 gap-2">
          {STAGES.map(({ id, label, desc }) => (
            <button
              key={id}
              onClick={() => setStage(id)}
              className={`text-left p-3 rounded-xl border-2 transition-all ${
                stage === id
                  ? 'border-[#0055A4] bg-[#0055A4]/5'
                  : 'border-slate-200 hover:border-slate-300 bg-white'
              }`}
            >
              <p className={`text-sm font-bold leading-tight ${stage === id ? 'text-[#0055A4]' : 'text-slate-700'}`}>
                {label}
              </p>
              <p className="text-[11px] text-slate-400 mt-0.5 leading-snug">{desc}</p>
            </button>
          ))}
        </div>
      </div>

      {/* Composite Criticality Weights */}
      <div className="bg-white rounded-xl border border-slate-200 p-5 space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-sm font-semibold text-slate-700 flex items-center gap-2">
              <Sliders size={15} className="text-[#0055A4]"/> Criticality Ranking Weights
            </h3>
            <p className="text-xs text-slate-500 mt-1">
              Controls how KPIs are ranked on the Home screen. Weights auto-normalise to 100%.
            </p>
          </div>
          <button
            onClick={() => setCritWeights({ ...DEFAULT_CW })}
            className="flex items-center gap-1 text-[10px] font-medium text-slate-400 hover:text-slate-600 transition-colors"
            title="Reset to defaults"
          >
            <RotateCcw size={11}/> Reset
          </button>
        </div>
        <div className="space-y-3">
          {[
            { key: 'gap',    label: 'Gap Severity',    desc: 'How far from target',         color: '#DC2626' },
            { key: 'trend',  label: 'Trend Momentum',  desc: 'Rate of deterioration',       color: '#D97706' },
            { key: 'impact', label: 'Business Impact',  desc: 'Downstream causal effect',    color: '#7c3aed' },
            { key: 'domain', label: 'Domain Urgency',  desc: 'Business area survival tier', color: '#0891b2' },
          ].map(({ key, label, desc, color }) => {
            const total = Object.values(critWeights).reduce((s, v) => s + v, 0) || 1
            const pct = Math.round(critWeights[key] / total * 100)
            return (
              <div key={key}>
                <div className="flex items-center justify-between mb-1">
                  <div>
                    <span className="text-xs font-semibold text-slate-700">{label}</span>
                    <span className="text-[10px] text-slate-400 ml-2">{desc}</span>
                  </div>
                  <span className="text-xs font-bold tabular-nums" style={{ color }}>{pct}%</span>
                </div>
                <input
                  type="range"
                  min={0} max={100} step={5}
                  value={critWeights[key]}
                  onChange={e => setCritWeights(prev => ({ ...prev, [key]: Number(e.target.value) }))}
                  className="w-full h-1.5 rounded-full appearance-none cursor-pointer"
                  style={{
                    background: `linear-gradient(to right, ${color} 0%, ${color} ${critWeights[key]}%, #e2e8f0 ${critWeights[key]}%, #e2e8f0 100%)`,
                  }}
                />
              </div>
            )
          })}
        </div>
        <div className="flex items-center gap-2 pt-2 border-t border-slate-100">
          <div className="flex-1 flex items-center gap-1.5">
            {[
              { key: 'gap', color: '#DC2626' }, { key: 'trend', color: '#D97706' },
              { key: 'impact', color: '#7c3aed' }, { key: 'domain', color: '#0891b2' },
            ].map(({ key, color }) => {
              const total = Object.values(critWeights).reduce((s, v) => s + v, 0) || 1
              const pct = critWeights[key] / total * 100
              return (
                <div key={key} className="h-2 rounded-full" style={{ width: `${pct}%`, backgroundColor: color, minWidth: 4 }} />
              )
            })}
          </div>
          <span className="text-[10px] text-slate-400 flex-shrink-0">
            = 100%
          </span>
        </div>
      </div>

      {/* Save button + status */}
      <div className="flex items-center gap-3">
        <button
          onClick={handleSave}
          disabled={saving}
          className="px-5 py-2 rounded-lg bg-[#0055A4] hover:bg-[#003d80] disabled:opacity-60
                     text-white text-sm font-semibold transition-all"
        >
          {saving ? 'Saving…' : 'Save Changes'}
        </button>

        {status === 'ok' && (
          <span className="flex items-center gap-1.5 text-emerald-600 text-sm font-medium">
            <Check size={14}/> Saved successfully
          </span>
        )}
        {status === 'error' && (
          <span className="flex items-center gap-1.5 text-red-500 text-sm font-medium">
            <AlertCircle size={14}/> Save failed — please try again
          </span>
        )}
      </div>
    </div>
  )
}
