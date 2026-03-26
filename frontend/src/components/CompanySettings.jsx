import { useState, useEffect, useRef } from 'react'
import axios from 'axios'
import { Upload, Building2, Check, AlertCircle } from 'lucide-react'

export default function CompanySettings({ onSave }) {
  const [companyName, setCompanyName] = useState('')
  const [industry, setIndustry]       = useState('')
  const [logo, setLogo]               = useState(null)       // preview URL
  const [saving, setSaving]           = useState(false)
  const [status, setStatus]           = useState(null)       // 'ok' | 'error'
  const fileRef = useRef(null)

  // Load current settings on mount
  useEffect(() => {
    axios.get('/api/company-settings')
      .then(r => {
        setCompanyName(r.data.company_name || '')
        setIndustry(r.data.industry || '')
        if (r.data.logo) setLogo(r.data.logo)
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
      })
      setStatus('ok')
      if (onSave) onSave({ company_name: companyName, industry })
    } catch {
      setStatus('error')
    } finally {
      setSaving(false)
    }
  }

  async function handleLogoUpload(e) {
    const file = e.target.files?.[0]
    if (!file) return
    const formData = new FormData()
    formData.append('file', file)
    try {
      const r = await axios.post('/api/company-settings/logo', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
      })
      setLogo(r.data.logo)
      if (onSave) onSave({ logo: r.data.logo })
    } catch {
      setStatus('error')
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
              onClick={() => fileRef.current?.click()}
              className="flex items-center gap-2 px-3 py-1.5 rounded-lg border border-slate-200
                         text-xs font-medium text-slate-600 hover:border-[#0055A4] hover:text-[#0055A4] transition-all"
            >
              <Upload size={12}/> Upload Logo
            </button>
            <input
              ref={fileRef}
              type="file"
              accept="image/*"
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
