import { useState, useEffect, useMemo } from 'react'
import axios from 'axios'
import {
  LayoutDashboard, Fingerprint, TrendingUp,
  Upload, Code2, RefreshCw, ChevronRight,
  Activity, GitBranch, Network, Layers, BarChart2
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

const TABS = [
  { id: 'board',       label: 'Executive Signal',  Icon: Layers          },
  { id: 'dashboard',   label: 'Command Center',    Icon: LayoutDashboard },
  { id: 'fingerprint', label: 'Org Fingerprint',   Icon: Fingerprint     },
  { id: 'trends',      label: 'Monthly Trends',    Icon: TrendingUp      },
  { id: 'projection',  label: 'Bridge Analysis',   Icon: GitBranch       },
  { id: 'ontology',    label: 'Data Ontology',     Icon: Network         },
  { id: 'forecast',    label: 'Signals Forecast',  Icon: BarChart2       },
  { id: 'upload',      label: 'Data Upload',       Icon: Upload          },
  { id: 'api',         label: 'API Reference',     Icon: Code2           },
]

const PAGE_TITLES = {
  board:       'Executive Signal',
  dashboard:   'Actionable Intelligence Command Center',
  fingerprint: 'Organisational Fingerprint',
  trends:      'Monthly KPI Trends',
  projection:  'Projection vs Actual — Bridge Analysis',
  ontology:    'Data Ontology — KPI Knowledge Graph',
  forecast:    'Signals Forecast — Markov Scenario Projection',
  upload:      'Data Upload',
  api:         'API Reference',
}

const FILTER_TABS = new Set(['dashboard', 'fingerprint', 'trends', 'projection'])

// Recompute a KPI's status from its filtered average
function kpiStatus(avg, target, direction) {
  if (avg == null || !target) return 'grey'
  const r = direction === 'higher' ? avg / target : target / avg
  return r >= 0.98 ? 'green' : r >= 0.90 ? 'yellow' : 'red'
}

export default function App() {
  const [tab, setTab]                             = useState('dashboard')
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

  useEffect(() => { loadAll() }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const noData    = !loading && summary?.months_of_data === 0
  const sb        = filteredSummary?.status_breakdown || {}
  const critical  = sb.red    || 0
  const attention = sb.yellow || 0
  const onTarget  = sb.green  || 0
  const total     = critical + attention + onTarget
  const bhi       = total > 0 ? Math.round((onTarget * 100 + attention * 60) / total) : null
  const bhiColor  = bhi == null ? '#94a3b8' : bhi >= 80 ? '#059669' : bhi >= 60 ? '#d97706' : '#dc2626'
  const bhiTrack  = bhi == null ? '#e2e8f0' : bhi >= 80 ? '#dcfce7' : bhi >= 60 ? '#fef3c7' : '#fee2e2'

  return (
    <div className="flex h-screen overflow-hidden">

      {/* ── Left Sidebar ──────────────────────────────────── */}
      <aside className="sidebar w-56 flex-shrink-0 flex flex-col h-full overflow-hidden">

        {/* Logo */}
        <div className="px-5 py-5 border-b border-white/10">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-[#00AEEF]/20 border border-[#00AEEF]/40
                            flex items-center justify-center pulse-accent flex-shrink-0">
              <span className="text-[#00AEEF] font-bold text-xs">SI</span>
            </div>
            <div className="min-w-0">
              <p className="text-white font-bold text-sm leading-none">Signals</p>
              <p className="text-[#00AEEF] text-[10px] mt-0.5 tracking-widest uppercase truncate">
                Intelligence
              </p>
            </div>
          </div>
        </div>

        {/* Business Health Index — replaces count pills */}
        {!loading && filteredSummary && (
          <div className="px-4 py-3 border-b border-white/10">
            <p className="text-slate-400 text-[10px] uppercase tracking-wider mb-2 font-medium">
              Business Health Index
            </p>
            {/* BHI score with arc indicator */}
            <div className="flex items-center gap-3 mb-2">
              <div className="relative flex-shrink-0">
                <svg width="48" height="48" viewBox="0 0 48 48">
                  <circle cx="24" cy="24" r="20" fill="none" stroke={bhiTrack} strokeWidth="4"/>
                  <circle cx="24" cy="24" r="20" fill="none" stroke={bhiColor} strokeWidth="4"
                    strokeDasharray={`${((bhi ?? 0) / 100) * 125.7} 125.7`}
                    strokeLinecap="round"
                    transform="rotate(-90 24 24)"/>
                </svg>
                <span className="absolute inset-0 flex items-center justify-center text-sm font-bold"
                  style={{ color: bhiColor }}>
                  {bhi ?? '—'}
                </span>
              </div>
              <div className="min-w-0">
                <p className="text-white text-xs font-semibold leading-none">
                  {bhi == null ? 'No data' : bhi >= 80 ? 'Healthy' : bhi >= 60 ? 'Caution' : 'At Risk'}
                </p>
                <p className="text-slate-400 text-[10px] mt-0.5">out of 100</p>
              </div>
            </div>
            {/* Supporting detail pills */}
            <div className="flex gap-1.5">
              <span className="flex-1 text-center text-[9px] font-bold px-1 py-1 rounded bg-red-500/15 text-red-400">
                {critical} red
              </span>
              <span className="flex-1 text-center text-[9px] font-bold px-1 py-1 rounded bg-amber-500/15 text-amber-400">
                {attention} watch
              </span>
              <span className="flex-1 text-center text-[9px] font-bold px-1 py-1 rounded bg-emerald-500/15 text-emerald-400">
                {onTarget} ok
              </span>
            </div>
          </div>
        )}

        {/* Navigation + AI Panel */}
        <div className="flex-1 flex flex-col min-h-0">
          <nav className="flex-1 min-h-0 py-4 space-y-0.5 overflow-y-auto">
            <p className="text-slate-500 text-[10px] uppercase tracking-wider font-medium px-6 mb-2">
              Navigation
            </p>
            {TABS.map(({ id, label, Icon }) => (
              <button
                key={id}
                onClick={() => setTab(id)}
                className={`sidebar-link w-full text-left ${tab === id ? 'active' : ''} ${
                  id === 'board' && tab !== 'board'
                    ? 'border border-[#00AEEF]/30 bg-[#00AEEF]/10 !text-[#00AEEF] mb-1'
                    : ''
                }`}
              >
                <Icon size={15} className="flex-shrink-0" />
                <span className="flex-1">{label}</span>
                {id === 'board' && tab !== 'board' && (
                  <span className="text-[8px] font-bold px-1.5 py-0.5 rounded bg-[#00AEEF]/20 text-[#00AEEF] uppercase tracking-wider">
                    New
                  </span>
                )}
                {tab === id && <ChevronRight size={12} className="text-[#00AEEF]" />}
              </button>
            ))}
          </nav>

          {/* AI Query Panel */}
          <AiQueryPanel
            bridgeData={filteredBridgeData}
            prefillQuestion={prefillQuestion}
            onPrefillConsumed={() => setPrefillQuestion(null)}
            selectedYears={selectedYears}
          />
        </div>

        {/* Footer */}
        <div className="px-4 py-4 border-t border-white/10 space-y-2">
          <button onClick={loadAll}
            className="w-full flex items-center justify-center gap-2 py-2 rounded-lg
                       text-xs text-slate-400 hover:text-white border border-white/10
                       hover:border-white/25 transition-all">
            <RefreshCw size={11}/> Refresh
          </button>
          <button onClick={seedDemo}
            className="w-full flex items-center justify-center gap-2 py-2 rounded-lg
                       text-xs bg-[#0055A4]/50 border border-[#00AEEF]/30 text-[#00AEEF]
                       hover:bg-[#0055A4] transition-all">
            <Activity size={11}/> Load Demo
          </button>
          <a href="/api/docs" target="_blank"
             className="w-full flex items-center justify-center gap-2 py-2 rounded-lg
                        text-xs text-slate-500 hover:text-slate-300 transition-all">
            <Code2 size={11}/> API Docs ↗
          </a>
        </div>
      </aside>

      {/* ── Main Content ──────────────────────────────────── */}
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">

        {/* Topbar */}
        <header className="topbar flex-shrink-0 px-6 py-3.5 flex items-center justify-between">
          <div className="flex items-center gap-3">
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

        {/* Scrollable content */}
        <main className="flex-1 overflow-y-auto px-6 py-5">

          {loading && (
            <div className="flex items-center justify-center h-64">
              <div className="animate-spin w-8 h-8 rounded-full border-4 border-[#0055A4] border-t-transparent"/>
            </div>
          )}

          {!loading && noData && (
            <div className="flex flex-col items-center justify-center h-64 gap-4">
              <p className="text-slate-500 text-base">No data yet — load demo data or upload a CSV.</p>
              <div className="flex gap-3">
                <button onClick={seedDemo}
                  className="px-5 py-2 rounded-lg bg-[#0055A4] hover:bg-[#003d80] text-white text-sm font-medium transition-colors">
                  Load Demo Data (5 Years)
                </button>
                <button onClick={() => setTab('upload')}
                  className="px-5 py-2 rounded-lg border border-slate-300 hover:border-slate-400 text-slate-600 text-sm font-medium transition-colors">
                  Upload CSV
                </button>
              </div>
            </div>
          )}

          {!loading && !noData && (
            <>
              {tab === 'board' && (
                <BoardReady
                  fingerprint={filteredFingerprint}
                  bridgeData={filteredBridgeData}
                  onNavigate={setTab}
                  periodLabel={periodLabel}
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
                  <Scorecard fingerprint={filteredFingerprint} kpiDefs={kpiDefs} onKpiClick={openKpi} boardView={boardView} periodLabel={periodLabel}/>
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
              {tab === 'api'         && <APIReference kpiDefs={kpiDefs}/>}
            </>
          )}

          {!loading && noData && tab === 'ontology'   && <OntologyPage />}
          {!loading && noData && tab === 'forecast'   && <ForecastPage />}
          {!loading && noData && tab === 'upload'     && <CSVUpload onUploaded={loadAll}/>}
          {!loading && noData && tab === 'api'        && <APIReference kpiDefs={kpiDefs}/>}
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
      <KpiDetailPanel kpi={selectedKpi} onClose={closeKpi} periodLabel={periodLabel}/>
    </div>
  )
}
