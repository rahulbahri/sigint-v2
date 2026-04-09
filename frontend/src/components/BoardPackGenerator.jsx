import { useState } from 'react'
import axios from 'axios'
import {
  Download, Palette, FileText, CheckSquare, Square,
  Loader2, Sparkles, Info
} from 'lucide-react'

const THEMES = [
  { id: 'corporate', name: 'Corporate Blue',     desc: 'Classic white with navy accents — boardroom standard' },
  { id: 'axiom',     name: 'Axiom Dark',         desc: 'Dark professional, high contrast, bold'              },
  { id: 'slate',     name: 'Slate Professional', desc: 'Dark blue-grey, modern and understated'              },
  { id: 'minimal',   name: 'Minimal Light',      desc: 'Clean monochrome, maximum readability'               },
]

function Toggle({ checked, onChange, label, description }) {
  return (
    <button
      onClick={() => onChange(!checked)}
      className="flex items-start gap-3 w-full text-left p-3 rounded-lg hover:bg-slate-50 transition-colors"
    >
      <div className="flex-shrink-0 mt-0.5">
        {checked
          ? <CheckSquare size={16} className="text-[#0055A4]" />
          : <Square      size={16} className="text-slate-300" />
        }
      </div>
      <div>
        <p className="text-slate-700 text-sm font-medium">{label}</p>
        {description && <p className="text-slate-400 text-xs mt-0.5">{description}</p>}
      </div>
    </button>
  )
}

export default function BoardPackGenerator({ companySettings }) {
  const [theme,             setTheme]             = useState('corporate')
  const [includeTalkTracks, setIncludeTalkTracks] = useState(true)
  const [includeVariance,   setIncludeVariance]   = useState(true)
  const [includeForward,    setIncludeForward]    = useState(false)
  const [periodLabel,       setPeriodLabel]       = useState(() => {
    const n = new Date()
    return n.toLocaleString('default', { month: 'long', year: 'numeric' })
  })
  const [generating, setGenerating] = useState(false)
  const [error,      setError]      = useState(null)
  const [success,    setSuccess]    = useState(false)

  async function handleGenerate() {
    setGenerating(true)
    setError(null)
    setSuccess(false)
    try {
      const resp = await axios.post(
        '/api/board-pack/generate',
        { theme, period_label: periodLabel, include_talk_tracks: includeTalkTracks,
          include_variance: includeVariance, include_forward: includeForward },
        { responseType: 'blob' }
      )
      const cd       = resp.headers['content-disposition'] || ''
      const match    = cd.match(/filename="(.+)"/)
      const filename = match ? match[1] : 'board_pack.pptx'
      const url      = window.URL.createObjectURL(new Blob([resp.data]))
      const a        = document.createElement('a')
      a.href = url; a.download = filename
      document.body.appendChild(a); a.click(); a.remove()
      window.URL.revokeObjectURL(url)
      setSuccess(true)
      setTimeout(() => setSuccess(false), 5000)
    } catch (err) {
      // responseType: 'blob' means error body is a Blob, not JSON — read it
      let detail = null
      try {
        if (err?.response?.data instanceof Blob) {
          const text = await err.response.data.text()
          const parsed = JSON.parse(text)
          detail = parsed.detail
        } else {
          detail = err?.response?.data?.detail
        }
      } catch {}
      setError(detail || 'Generation failed — ensure data is loaded and try again.')
    } finally {
      setGenerating(false)
    }
  }

  const company = companySettings?.company_name || 'Company'

  return (
    <div className="max-w-3xl space-y-5">

      {/* Header */}
      <div className="card p-6">
        <div className="flex items-start gap-4">
          <div className="w-10 h-10 rounded-xl bg-[#0055A4]/10 border border-[#0055A4]/20
                          flex items-center justify-center flex-shrink-0">
            <FileText size={18} className="text-[#0055A4]"/>
          </div>
          <div>
            <h2 className="text-slate-800 text-base font-bold mb-1">Board Pack Generator</h2>
            <p className="text-slate-500 text-sm leading-relaxed">
              Generate a professional PPTX presentation with live KPI data, health score analysis,
              and presenter talk tracks for each slide. Download and apply your own branding in PowerPoint.
            </p>
          </div>
        </div>
      </div>

      {/* Period */}
      <div className="card p-6">
        <h3 className="text-slate-500 text-[10px] font-bold uppercase tracking-widest mb-4">Period</h3>
        <label className="block text-slate-600 text-xs font-medium mb-2">
          Period Label — appears on the cover slide
        </label>
        <input
          value={periodLabel}
          onChange={e => setPeriodLabel(e.target.value)}
          className="w-full bg-white border border-slate-200 rounded-lg px-3 py-2.5
                     text-slate-800 text-sm focus:outline-none focus:border-[#0055A4]
                     focus:ring-1 focus:ring-[#0055A4]/20 transition-colors"
          placeholder="e.g. Q3 2025, January 2026, FY2025 Annual"
        />
      </div>

      {/* Theme */}
      <div className="card p-6">
        <div className="flex items-center gap-2 mb-4">
          <Palette size={14} className="text-slate-400"/>
          <h3 className="text-slate-500 text-[10px] font-bold uppercase tracking-widest">Theme</h3>
        </div>
        <div className="grid grid-cols-2 gap-3">
          {THEMES.map(t => (
            <button
              key={t.id}
              onClick={() => setTheme(t.id)}
              className={`text-left p-4 rounded-xl border transition-all ${
                theme === t.id
                  ? 'bg-[#0055A4]/5 border-[#0055A4]/40'
                  : 'bg-white border-slate-200 hover:border-slate-300 hover:bg-slate-50'
              }`}
            >
              <div className="flex items-center justify-between mb-1">
                <p className={`text-sm font-semibold ${theme === t.id ? 'text-[#0055A4]' : 'text-slate-700'}`}>
                  {t.name}
                </p>
                {theme === t.id && <span className="w-2 h-2 rounded-full bg-[#0055A4]"/>}
              </div>
              <p className="text-slate-400 text-xs">{t.desc}</p>
            </button>
          ))}
        </div>
      </div>

      {/* Options */}
      <div className="card p-6">
        <h3 className="text-slate-500 text-[10px] font-bold uppercase tracking-widest mb-3">Options</h3>
        <div className="divide-y divide-slate-50">
          <Toggle checked={includeTalkTracks} onChange={setIncludeTalkTracks}
            label="Include Talk Tracks"
            description="Add presenter notes to each slide with suggested talking points for board delivery"/>
          <Toggle checked={includeVariance} onChange={setIncludeVariance}
            label="Include Variance Slide"
            description="Add a KPI variance and gap analysis slide"/>
          <Toggle checked={includeForward} onChange={setIncludeForward}
            label="Include Forward Signals"
            description="Add 90-day outlook slide (requires 18+ months of data to unlock)"/>
        </div>
      </div>

      {/* Generate */}
      <div className="space-y-3">
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-red-700 text-sm">
            {error}
          </div>
        )}
        {success && (
          <div className="bg-emerald-50 border border-emerald-200 rounded-lg px-4 py-3 text-emerald-700 text-sm flex items-center gap-2">
            <Sparkles size={14}/> Board pack downloaded — open in PowerPoint to customise branding and fonts.
          </div>
        )}
        <button
          onClick={handleGenerate}
          disabled={generating}
          className={`flex items-center justify-center gap-2.5 w-full py-3.5 rounded-xl
                      text-sm font-bold transition-all shadow-sm ${
            generating
              ? 'bg-slate-100 text-slate-400 cursor-not-allowed'
              : 'bg-[#0055A4] hover:bg-[#003d80] text-white hover:shadow-md'
          }`}
        >
          {generating
            ? <><Loader2 size={15} className="animate-spin"/> Generating {company} Board Pack…</>
            : <><Download size={15}/> Generate &amp; Download Board Pack (.pptx)</>
          }
        </button>
        <p className="text-slate-400 text-xs text-center flex items-center justify-center gap-1">
          <Info size={11}/>
          Download opens in PowerPoint · Apply custom logos, fonts and colours after download
        </p>
      </div>

    </div>
  )
}
