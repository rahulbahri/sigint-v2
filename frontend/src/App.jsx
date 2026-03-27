import { useState, useEffect, useMemo } from 'react'
import axios from 'axios'
import {
  LayoutDashboard, Fingerprint, TrendingUp,
  Upload, Code2, RefreshCw, ChevronRight,
  Activity, GitBranch, Network, Layers, BarChart2, BookOpen, Bell, Settings2, Target,
  Shield, Menu, X, Zap
} from 'lucide-react'
import Scorecard from './components/Scorecard.jsx'
import Fingerprint2 from './components/Fingerprint.jsx'
import MonthlyTrend from './components/MonthlyTrend.jsx'
import CSVUpload from './components/CSVUpload.jsx'
import APIReference from './components/APIReference.jsx'
import SummaryBar from './components/SummaryBar.jsx'
import KpiDetailPanel from './components/KpiDetailPanel.jsx'
import AiQueryPanel from './components/AiQueryPanel.jsx'
import ProjectionBridge from './components/ProjectionBridge.jsx'
import MonthRangeFilter from './components/MonthRangeFilter.jsx'
import OntologyPage from './components/OntologyPage.jsx'
import BoardReady from './components/BoardReady.jsx'
import ForecastPage from './components/ForecastPage.jsx'
import DevDocs from './components/DevDocs.jsx'
import SlackAlerts from './components/SlackAlerts.jsx'
import CompanySettings from './components/CompanySettings.jsx'
import OnboardingModal from './components/OnboardingModal.jsx'
import VarianceCommand from './components/VarianceCommand.jsx'
import TargetsEditor from './components/TargetsEditor.jsx'
import AuditLog from './components/AuditLog.jsx'
import OnboardingChecklist from './components/OnboardingChecklist.jsx'
import PricingPage from './components/PricingPage'
import LoginPage from './components/LoginPage'

// ── V2: Nav structured into labelled zones with business-friendly names ──────
const NAV_GROUPS = [
  {
    label: 'Intelligence',
    tabs: [
      { id: 'board',    label: 'Executive Brief',       Icon: Layers    },
      { id: 'variance', label: 'Variance Command',      Icon: Activity  },
    ],
  },
  {
    label: 'Analysis',
    tabs: [
      { id: 'fingerprint', label: 'Performance Fingerprint', Icon: Fingerprint     },
      { id: 'trends',      label: 'Trend Explorer',          Icon: TrendingUp      },
      { id: 'forecast',    label: 'Forward Signals',         Icon: BarChart2        },
      { id: 'projection',  label: 'Plan vs Actual',          Icon: GitBranch       },
    ],
  },
  {
    label: 'Settings',
    tabs: [
      { id: 'upload',  label: 'Data Upload',      Icon: Upload    },
      { id: 'alerts',  label: 'Slack Alerts',     Icon: Bell      },
      { id: 'targets', label: 'KPI Targets',      Icon: Target    },
      { id: 'audit',   label: 'Audit Trail',      Icon: Shield    },
      { id: 'company', label: 'Company Settings', Icon: Settings2 },
    ],
  },
]

// Advanced tabs — shown in collapsible section
const ADVANCED_TABS_BASE = [
  { id: 'dashboard', label: 'Command Center',  Icon: LayoutDashboard },
  { id: 'ontology',  label: 'KPI Causal Map',  Icon: Network         },
  { id: 'api',       label: 'API Reference',   Icon: Code2           },
  { id: 'devdocs',   label: 'Dev Docs',        Icon: BookOpen        },
]

// Flat list kept for places that need to iterate all tabs (ADVANCED_TABS is derived at runtime based on devMode)
const TABS_BASE = [...NAV_GROUPS.flatMap(g => g.tabs), ...ADVANCED_TABS_BASE]

const PAGE_TITLES = {
  board:       'Executive Brief',
  variance:    'Variance Command Center',
  dashboard:   'Command Center',
  fingerprint: 'Performance Fingerprint',
  trends:      'Trend Explorer',
  projection:  'Plan vs Actual',
  ontology:    'KPI Causal Map',
  forecast:    'Forward Signals — 90-Day Outlook',
  upload:      'Data Upload',
  alerts:      'Slack Alerts',
  targets:     'KPI Targets',
  api:         'API Reference',
  devdocs:     'Developer Documentation',
  company:     'Company Settings',
  audit:       'Audit Trail',
}

const FILTER_TABS = new Set(['variance', 'dashboard', 'fingerprint', 'trends', 'projection'])

// Recompute a KPI's status from its filtered average
function kpiStatus(avg, target, direction) {
  if (avg == null || !target) return 'grey'
  const r = direction === 'higher' ? avg / target : target / avg
  return r >= 0.98 ? 'green' : r >= 0.90 ? 'yellow' : 'red'
}

export default function App() {
  const [tab, setTab]                             = useState('variance')
  const [sidebarOpen, setSidebarOpen]             = useState(false)
  const [summary, setSummary]                     = useState(null)
  const [kpiDefs, setKpiDefs]                     = useState([])
  const [monthly, setMonthly]                     = useState([])
  const [fingerprint, setFingerprint]             = useState([])
  const [loading, setLoading]                     = useState(true)
  const [selectedKpi, setSelectedKpi]             = useState(null)
  const [projectionMonthly, setProjectionMonthly] = useState([])
  const [bridgeData, setBridgeData]               = useState(null)
  const [prefillQuestion, setPrefillQuestion]     = useState(null)
  const [boardView, setBoardView]                 = useState(false)
  const [selectedYears, setSelectedYears]         = useState([])   // empty = all years
  const [selectedMonths, setSelectedMonths]       = useState([])   // empty = all months
  const [availableYears, setAvailableYears]       = useState([])
  const [companyStage, setCompanyStage]           = useState(() => localStorage.getItem('axiom_stage') || 'series_b')
  const [benchmarks, setBenchmarks]               = useState({})
  const [showOnboarding, setShowOnboarding]       = useState(() => !localStorage.getItem('axiom_onboarded'))
  const [advancedOpen, setAdvancedOpen]           = useState(false)
  const [devMode]                                 = useState(() => localStorage.getItem('axiom_dev_mode') === 'true')
  const [companySettings, setCompanySettings]     = useState({})
  const [authToken, setAuthToken]                 = useState('')
  const [authChecked, setAuthChecked]             = useState(false)
  const [showPricing, setShowPricing]             = useState(false)

  // ── Validate stored token with backend on every load ─────────────────────
  useEffect(() => {
    const stored = localStorage.getItem('axiom_auth_token') || ''
    if (!stored) { setAuthChecked(true); return }
    fetch('/api/auth/me', {
      headers: { 'Authorization': `Bearer ${stored}` }
    })
      .then(r => {
        if (r.ok) { setAuthToken(stored) }
        else { localStorage.removeItem('axiom_auth_token') }
      })
      .catch(() => { localStorage.removeItem('axiom_auth_token') })
      .finally(() => setAuthChecked(true))
  }, [])

  // Dev mode: filter DevDocs from advanced tabs unless dev mode is on
  const ADVANCED_TABS = devMode
    ? ADVANCED_TABS_BASE
    : ADVANCED_TABS_BASE.filter(t => t.id !== 'devdocs')

  // ── Derived filter sets ───────────────────────────────────────────────────
  const yearSet  = useMemo(() => new Set(selectedYears),  [selectedYears])
  const monthSet = useMemo(() => new Set(selectedMonths), [selectedMonths])

  function inFilter(yr, mo) {
    const yearOk  = yearSet.size  === 0 || yearSet.has(yr)
    const monthOk = monthSet.size === 0 || monthSet.has(mo)
    return yearOk && monthOk
  }

  // ── Human-readable period label for all cards/charts ─────────────────────
  const PLABELS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
  const periodLabel = useMemo(() => {
    const yrs = selectedYears.length ? [...selectedYears].sort((a, b) => a - b) : []
    const mos = selectedMonths.length ? [...selectedMonths].sort((a, b) => a - b) : []
    // No filters active — show full available range
    if (!yrs.length && !mos.length) {
      if (availableYears.length >= 2) {
        const sy = [...availableYears].sort((a, b) => a - b)
        return `${sy[0]}–${sy[sy.length - 1]}`
      }
      return availableYears.length === 1 ? `${availableYears[0]}` : 'All Data'
    }
    const yearStr = yrs.length === 1 ? `${yrs[0]}`
      : yrs.length > 1 ? `${yrs[0]}–${yrs[yrs.length - 1]}` : ''
    if (!mos.length) return yearStr || 'All Years'
    const moSet   = new Set(mos)
    const suffix  = yearStr ? ` ${yearStr}` : ''
    if (mos.length === 3  && moSet.has(1) && moSet.has(2)  && moSet.has(3))  return `Q1${suffix}`
    if (mos.length === 3  && moSet.has(4) && moSet.has(5)  && moSet.has(6))  return `Q2${suffix}`
    if (mos.length === 3  && moSet.has(7) && moSet.has(8)  && moSet.has(9))  return `Q3${suffix}`
    if (mos.length === 3  && moSet.has(10) && moSet.has(11) && moSet.has(12)) return `Q4${suffix}`
    if (mos.length === 6  && moSet.has(1) && moSet.has(6))  return `H1${suffix}`
    if (mos.length === 6  && moSet.has(7) && moSet.has(12)) return `H2${suffix}`
    if (mos.length === 12) return yearStr || 'Full Year'
    if (mos.length === 1)  return `${PLABELS[mos[0] - 1]}${suffix}`
    return `${PLABELS[mos[0] - 1]}–${PLABELS[mos[mos.length - 1] - 1]}${suffix}`
  }, [selectedYears, selectedMonths, availableYears]) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Derived / filtered data ──────────────────────────────────────────────

  const filteredFingerprint = useMemo(() => {
    if (!fingerprint?.length) return fingerprint
    return fingerprint.map(kpi => {
      const months = (kpi.monthly ?? []).filter(m => {
        const [yr, mo] = m.period.split('-').map(Number)
        return inFilter(yr, mo)
      })
      const vals = months.map(m => m.value).filter(v => v != null)
      const avg  = vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null
      const trend = vals.length >= 2
        ? (vals.at(-1) > vals[0] ? 'up' : vals.at(-1) < vals[0] ? 'down' : 'flat')
        : (kpi.trend ?? 'flat')
      return { ...kpi, monthly: months, avg, fy_status: kpiStatus(avg, kpi.target, kpi.direction), trend }
    })
  }, [fingerprint, yearSet, monthSet])

  // Year-only filtered fingerprint for Org Fingerprint tab — preserves all 12 months
  // so the Compare Periods feature can compare any sub-period within the selected year
  const yearFilteredFingerprint = useMemo(() => {
    if (!fingerprint?.length) return fingerprint
    return fingerprint.map(kpi => {
      const months = (kpi.monthly ?? []).filter(m => {
        const [yr] = m.period.split('-').map(Number)
        return yearSet.size === 0 || yearSet.has(yr)
      })
      const vals = months.map(m => m.value).filter(v => v != null)
      const avg  = vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null
      const trend = vals.length >= 2
        ? (vals.at(-1) > vals[0] ? 'up' : vals.at(-1) < vals[0] ? 'down' : 'flat')
        : (kpi.trend ?? 'flat')
      return { ...kpi, monthly: months, avg, fy_status: kpiStatus(avg, kpi.target, kpi.direction), trend }
    })
  }, [fingerprint, yearSet])

  const filteredMonthly = useMemo(() =>
    monthly.filter(m => inFilter(m.year, m.month)),
  [monthly, yearSet, monthSet])

  const filteredProjectionMonthly = useMemo(() =>
    projectionMonthly.filter(m => inFilter(m.year, m.month)),
  [projectionMonthly, yearSet, monthSet])

  const filteredBridgeData = useMemo(() => {
    if (!bridgeData?.has_projection || !bridgeData?.has_overlap) return bridgeData
    let on_track = 0, behind = 0, ahead = 0
    const kpis = {}
    Object.entries(bridgeData.kpis).forEach(([key, kpi]) => {
      const months = Object.fromEntries(
        Object.entries(kpi.months).filter(([p]) => {
          const [yr, mo] = p.split('-').map(Number)
          return inFilter(yr, mo)
        })
      )
      const mv = Object.values(months)
      if (!mv.length) return   // no overlap for this filter — skip from counts and table
      const avgActual    = mv.reduce((s, m) => s + m.actual,    0) / mv.length
      const avgProjected = mv.reduce((s, m) => s + m.projected, 0) / mv.length
      const avgGap       = avgActual - avgProjected
      const avgGapPct    = avgProjected
        ? (kpi.direction === 'higher'
            ? (avgActual - avgProjected) / Math.abs(avgProjected) * 100
            : (avgProjected - avgActual) / Math.abs(avgProjected) * 100)
        : 0
      const status = avgGapPct >= -3 ? 'green' : avgGapPct >= -8 ? 'yellow' : 'red'
      if (avgGapPct > 3) ahead++
      else if (status === 'green') on_track++
      else behind++
      kpis[key] = { ...kpi, months, avg_actual: avgActual, avg_projected: avgProjected,
                    avg_gap: avgGap, avg_gap_pct: avgGapPct, overall_status: status }
    })
    const totalMo = new Set(Object.values(kpis).flatMap(k => Object.keys(k.months))).size
    return { ...bridgeData, kpis, summary: { on_track, behind, ahead, total_months_compared: totalMo } }
  }, [bridgeData, yearSet, monthSet])

  // Summary with status counts recomputed from the filtered fingerprint
  const filteredSummary = useMemo(() => {
    if (!summary) return summary
    const sb = { green: 0, yellow: 0, red: 0, grey: 0 }
    filteredFingerprint?.forEach(k => sb[k.fy_status || 'grey']++)
    return { ...summary, status_breakdown: sb }
  }, [summary, filteredFingerprint])

  // ── Data loading ─────────────────────────────────────────────────────────

  async function loadAll() {
    setLoading(true)
    // Always fetch all data — year/month filtering happens entirely on the frontend
    try {
      const [s, k, m, f, b, pm, ay] = await Promise.all([
        axios.get('/api/summary'),
        axios.get('/api/kpi-definitions'),
        axios.get('/api/monthly'),
        axios.get('/api/fingerprint'),
        axios.get('/api/bridge'),
        axios.get('/api/projection/monthly'),
        axios.get('/api/available-years').catch(() => ({ data: [] })),
      ])
      setSummary(s.data); setKpiDefs(k.data)
      setMonthly(m.data); setFingerprint(f.data)
      setBridgeData(b.data); setProjectionMonthly(pm.data)
      if (Array.isArray(ay.data) && ay.data.length) setAvailableYears(ay.data)
      // Auto-run ontology discovery if no nodes exist yet
      try {
        const ont = await axios.get('/api/ontology/stats')
        if (!ont.data?.total_nodes) {
          await axios.post('/api/ontology/discover')
        }
      } catch {}
    } catch (e) { console.error(e) }
    setLoading(false)
  }

  async function seedDemo() {
    await axios.get('/api/seed-multiyear')
    loadAll()
  }

  function openKpi(kpiKey) {
    const kpi = filteredFingerprint.find(k => k.key === kpiKey)
    const def = kpiDefs.find(k => k.key === kpiKey)
    setSelectedKpi(kpi ? { ...kpi, formula: def?.formula ?? null } : null)
  }

  const closeKpi = () => setSelectedKpi(null)

  // Handle auth token from magic link URL
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const token = params.get('auth_token')
    if (token) {
      fetch('/api/auth/verify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token })
      })
        .then(r => r.json())
        .then(d => {
          if (d.token) {
            localStorage.setItem('axiom_auth_token', d.token)
            setAuthToken(d.token)
            // Clean URL
            window.history.replaceState({}, '', window.location.pathname)
          }
        })
        .catch(() => {})
    }
  }, [])

  useEffect(() => {
    loadAll()
    axios.get('/api/company-settings').then(r => setCompanySettings(r.data)).catch(() => {})
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    axios.get(`/api/benchmarks?stage=${companyStage}`)
      .then(r => setBenchmarks(r.data.benchmarks || {}))
      .catch(() => {})
  }, [companyStage])

  const noData    = !loading && summary?.months_of_data === 0
  const sb        = filteredSummary?.status_breakdown || {}
  const critical  = sb.red    || 0
  const attention = sb.yellow || 0
  const onTarget  = sb.green  || 0
  const total     = critical + attention + onTarget

  // ── Auth gate ───────────────────────────────────────────────────────────────
  if (!authChecked) {
    return (
      <div className="min-h-screen bg-slate-950 flex items-center justify-center">
        <div className="w-8 h-8 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
      </div>
    )
  }
  if (!authToken) {
    return <LoginPage onAuthSuccess={(tok) => {
      localStorage.setItem('axiom_auth_token', tok)
      setAuthToken(tok)
    }} />
  }

  return (
    <div className="flex h-screen overflow-hidden">

      {/* ── Mobile Overlay Backdrop ───────────────────────── */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-black/30 z-40 md:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* ── Left Sidebar ──────────────────────────────────── */}
      <aside className={`sidebar w-56 flex-shrink-0 flex flex-col h-full overflow-hidden fixed inset-y-0 left-0 z-50 transform transition-transform ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'} md:relative md:translate-x-0`}>

        {/* Logo */}
        <div className="px-5 py-5 border-b border-white/10">
          <div className="flex items-center gap-3">
            <button className="md:hidden absolute top-3 right-3 text-slate-400 hover:text-white" onClick={() => setSidebarOpen(false)}><X size={16}/></button>
            <div className="w-8 h-8 rounded-lg bg-[#00AEEF]/20 border border-[#00AEEF]/40
                            flex items-center justify-center pulse-accent flex-shrink-0 overflow-hidden">
              {companySettings.logo
                ? <img src={companySettings.logo} alt="logo" className="w-full h-full object-cover rounded-lg"/>
                : <span className="text-[#00AEEF] font-bold text-xs">AX</span>
              }
            </div>
            <div className="min-w-0">
              <p className="text-white font-bold text-sm leading-none">
                {companySettings.company_name || 'Axiom'}
              </p>
              <p className="text-[#00AEEF] text-[10px] mt-0.5 tracking-widest uppercase truncate">
                Intelligence
              </p>
            </div>
          </div>
        </div>

        {/* Status Distribution */}
        {!loading && filteredSummary && (
          <div className="px-4 py-3 border-b border-white/10">
            <p className="text-slate-400 text-[10px] uppercase tracking-wider mb-2 font-medium">
              Status Distribution
            </p>
            <div className="flex items-center gap-2 flex-wrap text-[11px] font-semibold">
              <span className="flex items-center gap-1">
                <span className="inline-block w-2 h-2 rounded-full bg-red-500" />
                <span className="text-red-400">{critical} critical</span>
              </span>
              <span className="text-slate-600">·</span>
              <span className="flex items-center gap-1">
                <span className="inline-block w-2 h-2 rounded-full bg-amber-500" />
                <span className="text-amber-400">{attention} watch</span>
              </span>
              <span className="text-slate-600">·</span>
              <span className="flex items-center gap-1">
                <span className="inline-block w-2 h-2 rounded-full bg-emerald-500" />
                <span className="text-emerald-400">{onTarget} on target</span>
              </span>
            </div>
          </div>
        )}

        {/* ── Company Stage Selector ─────────────────────── */}
        <div className="px-3 py-2 border-b border-white/10">
          <p className="text-slate-500 text-[9px] uppercase tracking-widest font-semibold mb-1.5 px-1">
            Stage
          </p>
          <div className="flex gap-1 flex-wrap">
            {[
              { id: 'seed',     label: 'Seed'  },
              { id: 'series_a', label: 'Ser A' },
              { id: 'series_b', label: 'Ser B' },
              { id: 'series_c', label: 'Ser C+' },
            ].map(({ id, label }) => (
              <button
                key={id}
                onClick={() => {
                  setCompanyStage(id)
                  localStorage.setItem('axiom_stage', id)
                }}
                className={`flex-1 text-center text-[10px] font-bold py-1 rounded-lg border transition-all ${
                  companyStage === id
                    ? 'bg-[#0055A4] border-[#00AEEF]/50 text-white'
                    : 'bg-white/5 border-white/10 text-slate-400 hover:text-white hover:border-white/25'
                }`}>
                {label}
              </button>
            ))}
          </div>
        </div>

        {/* Navigation + AI Panel */}
        <div className="flex-1 flex flex-col min-h-0">

          {/* ── Onboarding Checklist ──────────────────────────── */}
          <OnboardingChecklist fingerprint={filteredFingerprint} onNavigate={setTab} />

          {/* ── Anika co-pilot CTA — prominent, always visible ── */}
          <div className="px-3 pt-3 pb-1">
            <button
              onClick={() => {
                // Expand AI panel — find and click its toggle
                document.querySelector('[data-anika-toggle]')?.click()
              }}
              className="w-full flex items-center gap-2 px-3 py-2 rounded-xl
                         bg-gradient-to-r from-teal-500/20 to-blue-500/20
                         border border-teal-400/30 hover:border-teal-400/60
                         text-teal-300 hover:text-teal-200 transition-all group"
            >
              <span className="text-base">✦</span>
              <span className="flex-1 text-left text-[11px] font-semibold tracking-wide">Ask Anika</span>
              <span className="text-[9px] text-teal-400/70 font-medium bg-teal-500/10 px-1.5 py-0.5 rounded-full">
                AI CFO Analyst
              </span>
            </button>
          </div>

          <nav className="flex-1 min-h-0 py-2 overflow-y-auto">
            {NAV_GROUPS.map(group => (
              <div key={group.label} className="mb-1">
                <p className="text-slate-500 text-[9px] uppercase tracking-widest font-semibold px-5 py-1.5">
                  {group.label}
                </p>
                {group.tabs.map(({ id, label, Icon }) => (
                  <button
                    key={id}
                    onClick={() => setTab(id)}
                    className={`sidebar-link w-full text-left ${tab === id ? 'active' : ''}`}
                  >
                    <Icon size={15} className="flex-shrink-0" />
                    <span className="flex-1">{label}</span>
                    {tab === id && <ChevronRight size={12} className="text-[#00AEEF]" />}
                  </button>
                ))}
              </div>
            ))}

            {/* Advanced section */}
            <div className="mt-2">
              <button
                onClick={() => setAdvancedOpen(!advancedOpen)}
                className="flex items-center gap-2 w-full px-3 py-1.5 text-[10px] font-bold text-slate-400 uppercase tracking-widest hover:text-slate-500 transition-colors"
              >
                <ChevronRight size={10} className={`transition-transform ${advancedOpen ? 'rotate-90' : ''}`}/>
                Advanced
              </button>
              {advancedOpen && ADVANCED_TABS.map(({ id, label, Icon }) => (
                <button key={id} onClick={() => setTab(id)}
                  className={`flex items-center gap-2.5 w-full px-4 py-2 rounded-xl text-[12px] font-medium transition-all duration-150 ${
                    tab === id ? 'bg-[#0055A4] text-white shadow-md' : 'text-slate-500 hover:bg-slate-100 hover:text-slate-700'
                  }`}>
                  <Icon size={14}/>
                  {label}
                </button>
              ))}
            </div>
          </nav>

          {/* AI Query Panel — collapsed by default, opened via Anika CTA */}
          <AiQueryPanel
            bridgeData={filteredBridgeData}
            prefillQuestion={prefillQuestion}
            onPrefillConsumed={() => setPrefillQuestion(null)}
            selectedYears={selectedYears}
          />
        </div>

        {/* Footer */}
        <div className="px-4 py-4 border-t border-white/10 space-y-2">
          <button
            onClick={() => setShowPricing(true)}
            className="w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-left text-[12px] font-medium text-indigo-300 hover:bg-indigo-500/15 transition-colors"
          >
            <Zap size={13} className="text-indigo-400" />
            View Pricing
          </button>
          <button onClick={loadAll}
            className="w-full flex items-center justify-center gap-2 py-2 rounded-lg
                       text-xs text-slate-400 hover:text-white border border-white/10
                       hover:border-white/25 transition-all">
            <RefreshCw size={11}/> Refresh
          </button>
          <a href="/api/docs" target="_blank"
             className="w-full flex items-center justify-center gap-2 py-2 rounded-lg
                        text-xs text-slate-500 hover:text-slate-300 transition-all">
            <Code2 size={11}/> API Docs ↗
          </a>
        </div>
      </aside>

      {/* ── Main Content ──────────────────────────────────── */}
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden w-full">

        {/* Topbar */}
        <header className="topbar flex-shrink-0 px-6 py-3.5 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <button
              className="md:hidden mr-1 text-slate-500 hover:text-slate-700"
              onClick={() => setSidebarOpen(v => !v)}
            >
              <Menu size={20}/>
            </button>
            <h1 className="page-title">{PAGE_TITLES[tab]}</h1>
            {summary && (
              <span className="text-xs text-slate-400 hidden md:block">
                {summary.months_of_data} months · {summary.kpis_tracked} KPIs
              </span>
            )}
          </div>
          <div className="flex items-center gap-2 text-xs text-slate-400">
            <span className="hidden lg:block">
              {selectedYears.length === 0 ? 'All Years' : selectedYears.length === 1 ? `FY ${selectedYears[0]}` : selectedYears.join(', ')}
              {' · '}
              {selectedMonths.length === 0 ? 'All Months' : `${selectedMonths.length} month${selectedMonths.length > 1 ? 's' : ''}`}
            </span>
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse hidden lg:block"/>
            <span className="hidden lg:block text-emerald-600 font-medium">Live</span>
          </div>
        </header>

        {/* Filter strip — shown on data tabs only */}
        {!loading && !noData && FILTER_TABS.has(tab) && (
          <MonthRangeFilter
            selectedYears={selectedYears}
            onYearsChange={setSelectedYears}
            selectedMonths={selectedMonths}
            onMonthsChange={setSelectedMonths}
            availableYears={availableYears}
          />
        )}

        {/* ── Anika proactive nudge bar — contextual, one insight per view ── */}
        {!loading && !noData && filteredFingerprint.length > 0 && (() => {
          const redCount = filteredFingerprint.filter(k => k.fy_status === 'red').length
          const worstKpi = [...filteredFingerprint]
            .filter(k => k.fy_status === 'red' && k.avg != null && k.target)
            .sort((a, b) => {
              const gA = a.direction === 'higher' ? a.avg / a.target : a.target / a.avg
              const gB = b.direction === 'higher' ? b.avg / b.target : b.target / b.avg
              return gA - gB
            })[0]
          const nudgeMap = {
            board:       worstKpi ? `✦  ${worstKpi.name} is the deepest gap this period — ask Anika for a full breakdown` : '✦  Ask Anika anything about this period',
            dashboard:   `✦  ${redCount} KPIs are critical — ask Anika which to fix first`,
            fingerprint: '✦  Ask Anika to explain any pattern you see in the heatmap',
            trends:      '✦  Ask Anika to identify the most important trend shift in this data',
            projection:  '✦  Ask Anika why specific KPIs are behind plan',
            ontology:    '✦  Ask Anika how your most influential KPI is affecting the rest of the business',
            forecast:    '✦  Ask Anika to explain this forecast in plain English',
            upload:      null,
            api:         null,
          }
          const nudge = nudgeMap[tab]
          if (!nudge) return null
          return (
            <div className="flex-shrink-0 mx-6 mb-0 mt-2">
              <button
                onClick={() => document.querySelector('[data-anika-toggle]')?.click()}
                className="w-full flex items-center gap-2.5 px-4 py-2 rounded-xl
                           bg-gradient-to-r from-teal-950/60 to-blue-950/60
                           border border-teal-700/30 hover:border-teal-500/50
                           text-teal-300/80 hover:text-teal-200 transition-all text-left"
              >
                <span className="text-[11px] flex-1">{nudge}</span>
                <span className="text-[10px] text-teal-500/60 font-medium flex-shrink-0">Ask →</span>
              </button>
            </div>
          )
        })()}

        {/* Scrollable content */}
        <main className="flex-1 overflow-y-auto px-6 py-5">

          {loading && (
            <div className="flex items-center justify-center h-64">
              <div className="animate-spin w-8 h-8 rounded-full border-4 border-[#0055A4] border-t-transparent"/>
            </div>
          )}

          {!loading && noData && (
            <div className="flex flex-col items-center justify-center h-64 gap-4">
              <p className="text-slate-500 text-base">No data yet — upload a CSV to get started.</p>
              <div className="flex gap-3">
                <button onClick={() => setTab('upload')}
                  className="px-5 py-2 rounded-lg bg-[#0055A4] hover:bg-[#003d80] text-white text-sm font-medium transition-colors">
                  Upload CSV
                </button>
              </div>
            </div>
          )}

          {!loading && !noData && (
            <>
              {tab === 'variance' && (
                <VarianceCommand
                  fingerprint={filteredFingerprint}
                  bridgeData={filteredBridgeData}
                  benchmarks={benchmarks}
                  companyStage={companyStage}
                  periodLabel={periodLabel}
                  onKpiClick={openKpi}
                />
              )}
              {tab === 'board' && (
                <BoardReady
                  fingerprint={filteredFingerprint}
                  bridgeData={filteredBridgeData}
                  onNavigate={setTab}
                  periodLabel={periodLabel}
                  benchmarks={benchmarks}
                  companyStage={companyStage}
                  companySettings={companySettings}
                />
              )}
              {tab === 'dashboard'   && (
                <>
                  <SummaryBar summary={filteredSummary} onRefresh={loadAll} onSeed={seedDemo}/>
                  {/* Board View toggle */}
                  <div className="flex justify-end mb-3">
                    <div className="flex items-center gap-1 bg-slate-100 rounded-lg p-1">
                      <button
                        onClick={() => setBoardView(false)}
                        className={`px-3 py-1 rounded-md text-xs font-medium transition-all ${
                          !boardView ? 'bg-white text-slate-700 shadow-sm' : 'text-slate-500 hover:text-slate-700'
                        }`}>
                        Full View
                      </button>
                      <button
                        onClick={() => setBoardView(true)}
                        className={`px-3 py-1 rounded-md text-xs font-medium transition-all ${
                          boardView ? 'bg-white text-[#0055A4] shadow-sm' : 'text-slate-500 hover:text-slate-700'
                        }`}>
                        Board View
                      </button>
                    </div>
                  </div>
                  <Scorecard fingerprint={filteredFingerprint} kpiDefs={kpiDefs} onKpiClick={openKpi} boardView={boardView} periodLabel={periodLabel} benchmarks={benchmarks} companyStage={companyStage}/>
                </>
              )}
              {tab === 'fingerprint' && <Fingerprint2 fingerprint={yearFilteredFingerprint} onKpiClick={openKpi}/>}
              {tab === 'trends'      && <MonthlyTrend fingerprint={filteredFingerprint} monthly={filteredMonthly} onKpiClick={openKpi} periodLabel={periodLabel}/>}
              {tab === 'projection'  && (
                <ProjectionBridge
                  bridgeData={filteredBridgeData}
                  projectionMonthly={filteredProjectionMonthly}
                  onUploaded={loadAll}
                  onAskAnika={(kpiName) => setPrefillQuestion(`Why is ${kpiName} below projection?`)}
                  onNavigateToUpload={() => setTab('upload')}
                />
              )}
              {tab === 'ontology'    && <OntologyPage />}
              {tab === 'forecast'    && <ForecastPage />}
              {tab === 'upload'      && <CSVUpload onUploaded={loadAll}/>}
              {tab === 'alerts'      && <SlackAlerts filteredFingerprint={filteredFingerprint}/>}
              {tab === 'targets'     && <TargetsEditor />}
              {tab === 'audit'      && <AuditLog />}
              {tab === 'company'     && <CompanySettings onSave={(updated) => setCompanySettings(prev => ({ ...prev, ...updated }))}/>}
              {tab === 'api'         && <APIReference kpiDefs={kpiDefs}/>}
              {tab === 'devdocs'     && <DevDocs />}
            </>
          )}

          {!loading && noData && tab === 'ontology'   && <OntologyPage />}
          {!loading && noData && tab === 'forecast'   && <ForecastPage />}
          {!loading && noData && tab === 'upload'     && <CSVUpload onUploaded={loadAll}/>}
          {!loading && noData && tab === 'alerts'     && <SlackAlerts filteredFingerprint={[]}/>}
          {!loading && noData && tab === 'targets'    && <TargetsEditor />}
          {!loading && noData && tab === 'audit'     && <AuditLog />}
          {!loading && noData && tab === 'company'    && <CompanySettings onSave={(updated) => setCompanySettings(prev => ({ ...prev, ...updated }))}/>}
          {!loading && noData && tab === 'api'        && <APIReference kpiDefs={kpiDefs}/>}
          {!loading && noData && tab === 'devdocs'    && <DevDocs />}
          {!loading && noData && tab === 'projection' && (
            <ProjectionBridge
              bridgeData={filteredBridgeData}
              projectionMonthly={filteredProjectionMonthly}
              onUploaded={loadAll}
              onAskAnika={(kpiName) => setPrefillQuestion(`Why is ${kpiName} below projection?`)}
              onNavigateToUpload={() => setTab('upload')}
            />
          )}
        </main>
      </div>

      {/* ── KPI Detail Panel (global, fixed overlay) ──────── */}
      <KpiDetailPanel kpi={selectedKpi} onClose={closeKpi} periodLabel={periodLabel} benchmarks={benchmarks} companyStage={companyStage}/>

      {/* ── Onboarding Modal (first-run only) ─────────────── */}
      {showOnboarding && (
        <OnboardingModal
          initialStage={companyStage}
          onComplete={({ stage, mode }) => {
            setCompanyStage(stage)
            localStorage.setItem('axiom_stage', stage)
            setShowOnboarding(false)
            if (mode === 'load') setTab('upload')
          }}
        />
      )}

      {/* ── Pricing Modal ──────────────────────────────────── */}
      {showPricing && <PricingPage onClose={() => setShowPricing(false)} />}
    </div>
  )
}
