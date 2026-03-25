import { useState } from 'react'

const SECTIONS = [
  { id: 'overview',      label: 'Platform Overview' },
  { id: 'stack',         label: 'Tech Stack' },
  { id: 'running',       label: 'Running Locally' },
  { id: 'frontend-arch', label: 'Frontend Architecture' },
  { id: 'backend-arch',  label: 'Backend Architecture' },
  { id: 'data-model',    label: 'Data Model (SQLite)' },
  { id: 'kpi-defs',      label: 'KPI Definitions' },
  { id: 'changelog',     label: 'V2 Changelog' },
  { id: 'decisions',     label: 'Build Decisions Log' },
  { id: 'roadmap',       label: 'Roadmap / Known Issues' },
]

function Code({ children }) {
  return (
    <pre className="bg-[#0d1117] text-[#e6edf3] text-[12px] font-mono rounded-xl p-4 overflow-x-auto leading-relaxed border border-[#30363d] my-3">
      {children}
    </pre>
  )
}

function InlineCode({ children }) {
  return (
    <code className="bg-slate-100 text-[#0055A4] font-mono text-[11px] px-1.5 py-0.5 rounded border border-slate-200">
      {children}
    </code>
  )
}

function SectionHeading({ id, children }) {
  return (
    <h2 id={id} className="text-xl font-bold text-slate-900 mt-10 mb-4 pb-2 border-b border-slate-200 scroll-mt-6">
      {children}
    </h2>
  )
}

function SubHeading({ children }) {
  return (
    <h3 className="text-base font-bold text-slate-700 mt-5 mb-2">{children}</h3>
  )
}

function Pill({ color, children }) {
  const colors = {
    blue:   'bg-blue-50 text-blue-700 border-blue-200',
    green:  'bg-emerald-50 text-emerald-700 border-emerald-200',
    amber:  'bg-amber-50 text-amber-700 border-amber-200',
    purple: 'bg-violet-50 text-violet-700 border-violet-200',
    slate:  'bg-slate-100 text-slate-600 border-slate-200',
  }
  return (
    <span className={`inline-block text-[11px] font-semibold px-2.5 py-0.5 rounded-full border ${colors[color] || colors.slate}`}>
      {children}
    </span>
  )
}

const KPI_TABLE = [
  { key: 'revenue_growth',       name: 'Revenue Growth Rate',          unit: 'pct',    dir: 'higher', formula: '(Rev_Month − Rev_PrevMonth) / Rev_PrevMonth × 100' },
  { key: 'gross_margin',         name: 'Gross Margin %',               unit: 'pct',    dir: 'higher', formula: '(Revenue − COGS) / Revenue × 100' },
  { key: 'operating_margin',     name: 'Operating Margin %',           unit: 'pct',    dir: 'higher', formula: '(Revenue − COGS − OpEx) / Revenue × 100' },
  { key: 'ebitda_margin',        name: 'EBITDA Margin %',              unit: 'pct',    dir: 'higher', formula: 'EBITDA / Revenue × 100' },
  { key: 'cash_conv_cycle',      name: 'Cash Conversion Cycle',        unit: 'days',   dir: 'lower',  formula: 'DSO + DIO − DPO' },
  { key: 'dso',                  name: 'Days Sales Outstanding',       unit: 'days',   dir: 'lower',  formula: '(AR / Revenue) × 30' },
  { key: 'arr_growth',           name: 'ARR Growth Rate',              unit: 'pct',    dir: 'higher', formula: '(ARR_Month − ARR_PrevMonth) / ARR_PrevMonth × 100' },
  { key: 'nrr',                  name: 'Net Revenue Retention',        unit: 'pct',    dir: 'higher', formula: '(MRR_Start + Expansion − Churn − Contraction) / MRR_Start × 100' },
  { key: 'burn_multiple',        name: 'Burn Multiple',                unit: 'ratio',  dir: 'lower',  formula: 'Net Burn / Net New ARR' },
  { key: 'opex_ratio',           name: 'Operating Expense Ratio',      unit: 'pct',    dir: 'lower',  formula: 'OpEx / Revenue × 100' },
  { key: 'contribution_margin',  name: 'Contribution Margin %',        unit: 'pct',    dir: 'higher', formula: '(Revenue − COGS − Variable_Costs) / Revenue × 100' },
  { key: 'revenue_quality',      name: 'Revenue Quality Ratio',        unit: 'pct',    dir: 'higher', formula: 'Recurring_Revenue / Total_Revenue × 100' },
  { key: 'cac_payback',          name: 'CAC Payback Period',           unit: 'months', dir: 'lower',  formula: 'CAC / (ARPU × Gross_Margin_pct)' },
  { key: 'sales_efficiency',     name: 'Sales Efficiency Ratio',       unit: 'ratio',  dir: 'higher', formula: 'New_ARR / Sales_Marketing_Spend' },
  { key: 'customer_concentration','name': 'Customer Concentration',    unit: 'pct',    dir: 'lower',  formula: 'Top_Customer_Revenue / Total_Revenue × 100' },
  { key: 'recurring_revenue',    name: 'Recurring Revenue Ratio',      unit: 'pct',    dir: 'higher', formula: 'Recurring_Revenue / Total_Revenue × 100' },
  { key: 'churn_rate',           name: 'Monthly Churn Rate',           unit: 'pct',    dir: 'lower',  formula: 'Lost_Customers / Total_Customers × 100' },
  { key: 'operating_leverage',   name: 'Operating Leverage Index',     unit: 'ratio',  dir: 'higher', formula: '% Change in Operating Income / % Change in Revenue' },
  { key: 'growth_efficiency',    name: 'Growth Efficiency Index',      unit: 'ratio',  dir: 'higher', formula: 'ARR_Growth_Rate / Burn_Multiple' },
  { key: 'revenue_momentum',     name: 'Revenue Momentum Index',       unit: 'ratio',  dir: 'higher', formula: 'Current_Rev_Growth / Annual_Avg_Rev_Growth' },
  { key: 'revenue_fragility',    name: 'Strategic Revenue Fragility',  unit: 'ratio',  dir: 'lower',  formula: '(Customer_Concentration × Churn_Rate) / NRR' },
  { key: 'burn_convexity',       name: 'Burn Convexity',               unit: 'ratio',  dir: 'lower',  formula: 'Δ_Burn_Multiple Month-over-Month' },
  { key: 'margin_volatility',    name: 'Margin Volatility Index',      unit: 'pct',    dir: 'lower',  formula: '6M_Rolling_Std_Dev_of_Gross_Margin' },
  { key: 'pipeline_conversion',  name: 'Pipeline Conversion Rate',     unit: 'pct',    dir: 'higher', formula: 'MQL_to_Win_End_to_End_Conversion_%' },
  { key: 'customer_decay_slope', name: 'Customer Decay Curve Slope',   unit: 'pct',    dir: 'lower',  formula: 'Δ_Churn_Rate Month-over-Month' },
  { key: 'customer_ltv',         name: 'Customer Lifetime Value',      unit: 'usd',    dir: 'higher', formula: '(ARPU × Gross_Margin%) / Monthly_Churn_Rate' },
  { key: 'pricing_power_index',  name: 'Pricing Power Index',          unit: 'pct',    dir: 'higher', formula: 'ΔARPU% − Δ_Customer_Volume%' },
]

const CHANGELOG = [
  { date: '2026-03-24', item: 'DevDocs tab added — this page' },
  { date: '2026-03-24', item: 'Benchmarking added — industry peer comparison by company stage (Seed / Series A / Series B / Series C+) for 18 KPIs; stage selector in sidebar saved to localStorage' },
  { date: '2026-03-24', item: 'Heatmap year-aware columns: dynamic YYYY-MM format, year-band header row, fixes year collision bug in multi-year data' },
  { date: '2026-03-24', item: 'Executive Brief: cause → consequence → fix narrative structure, single-column layout, uniform text colour; TextChip components use dotted underline only as the click signal' },
  { date: '2026-03-24', item: 'Excel export enriched with 4-row header block: row 1 machine-readable keys (import anchor), rows 2–4 human metadata (name / unit / used-for)' },
  { date: '2026-03-24', item: 'KPI domain analysis now period-aware: green-period status shows "What\'s Contributing" label instead of "Likely Drivers"' },
  { date: '2026-03-24', item: 'Forward Signals plain-English explainer card added above the 90-day forecast chart' },
  { date: '2026-03-24', item: 'KPI Causal Map plain-English explainer added above the ontology graph' },
  { date: '2026-03-24', item: 'Ask Anika promoted to persistent co-pilot CTA in sidebar above the navigation' },
  { date: '2026-03-24', item: 'Grouped navigation: Intelligence / Analysis / Knowledge / Settings zones; Executive Brief set as the default tab' },
  { date: '2026-03-24', item: 'Axiom brand identity: AX logo, "Intelligence · V2" sub-label, #00AEEF accent colour' },
]

const DECISIONS = [
  { decision: 'Single SQLite file for simplicity', rationale: 'No multi-tenant requirement in V1/V2. Swap for Postgres when multi-tenant. DB path: backend/uploads/axiom.db' },
  { decision: 'Fingerprint data pre-filtered in App.jsx', rationale: 'All child components receive already-filtered data via filteredFingerprint. Avoids repeated filter logic inside each chart component.' },
  { decision: 'Causation data embedded in fingerprint response', rationale: 'Not a separate endpoint — fewer round trips on the critical path. CAUSATION_RULES dict in main.py is merged into each KPI at query time.' },
  { decision: 'Excel export uses 4-row header block', rationale: 'Row 1 is machine-readable keys (import anchor). Rows 2–4 are human metadata. Auto-detect logic in import checks for this structure to distinguish V2 from legacy V1 CSV.' },
  { decision: 'Markov forecast uses Wasserstein distance for regime detection', rationale: 'Technically sophisticated but the label is hidden from users in V2. Business-friendly label "Forward Signals" shown instead.' },
  { decision: 'TextChip components use dotted underline only', rationale: 'No colour change on hover — the dotted underline is the sole click signal. Keeps uniform text colour throughout the Executive Brief narrative.' },
  { decision: 'BHI score computed on the frontend from filteredFingerprint', rationale: 'Avoids a round-trip. The BHI formula is: (green×100 + yellow×60) / total KPIs. Status thresholds: ≥98% = green, ≥90% = yellow, <90% = red.' },
  { decision: 'Benchmarks stored as a Python dict (not DB table)', rationale: 'Static reference data — no user-modifiable content. Easier to update inline. Source: OpenView, Bessemer, SaaS Capital reports.' },
]

export default function DevDocs() {
  const [activeSection, setActiveSection] = useState('overview')

  function scrollTo(id) {
    setActiveSection(id)
    document.getElementById(id)?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }

  return (
    <div className="flex h-full" style={{ minHeight: 0 }}>

      {/* ── Left sidebar ─────────────────────────────────────────── */}
      <aside className="w-52 flex-shrink-0 h-full overflow-y-auto sticky top-0"
        style={{ background: '#0f172a', borderRight: '1px solid rgba(255,255,255,0.08)' }}>
        <div className="px-4 pt-5 pb-3">
          <p className="text-[10px] font-bold text-[#00AEEF] uppercase tracking-widest mb-1">
            Developer Docs
          </p>
          <p className="text-[11px] text-white/40">
            Axiom Intelligence V2
          </p>
        </div>
        <nav className="px-2 pb-6">
          {SECTIONS.map(s => (
            <button
              key={s.id}
              onClick={() => scrollTo(s.id)}
              className={`w-full text-left px-3 py-2 rounded-lg text-[12px] mb-0.5 transition-all ${
                activeSection === s.id
                  ? 'bg-[#0055A4]/50 text-white font-semibold border border-[#00AEEF]/30'
                  : 'text-white/50 hover:text-white/80 hover:bg-white/5'
              }`}>
              {s.label}
            </button>
          ))}
        </nav>
      </aside>

      {/* ── Content area ─────────────────────────────────────────── */}
      <main className="flex-1 overflow-y-auto px-10 py-6 bg-white"
        onScroll={e => {
          const container = e.currentTarget
          for (const s of SECTIONS) {
            const el = document.getElementById(s.id)
            if (!el) continue
            const top = el.getBoundingClientRect().top - container.getBoundingClientRect().top
            if (top <= 80) setActiveSection(s.id)
          }
        }}>

        <div className="max-w-3xl">

          {/* ── 1. Platform Overview ─────────────────────────────── */}
          <SectionHeading id="overview">1. Platform Overview</SectionHeading>

          <p className="text-sm text-slate-600 leading-relaxed mb-3">
            Axiom Intelligence is a SaaS KPI intelligence platform designed for <strong>CFOs and finance operators</strong> at
            growth-stage software companies. It ingests monthly financial data (via Excel/CSV upload or direct entry),
            computes 27 derived KPIs, and surfaces board-ready narratives, trend analysis, and 90-day Markov-model
            projections — without requiring users to write queries or configure dashboards.
          </p>

          <SubHeading>V1 vs V2 Differences</SubHeading>
          <div className="overflow-x-auto rounded-xl border border-slate-200 mb-4">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-slate-50 border-b border-slate-200">
                  <th className="text-left text-slate-500 text-xs font-semibold py-2.5 px-4 w-1/3">Area</th>
                  <th className="text-left text-slate-500 text-xs font-semibold py-2.5 px-4">V1</th>
                  <th className="text-left text-[#0055A4] text-xs font-semibold py-2.5 px-4">V2</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {[
                  ['Navigation', 'Flat tab list', 'Grouped zones: Intelligence / Analysis / Knowledge / Settings'],
                  ['Brand', 'Generic header', 'Axiom brand: AX logo, #00AEEF accent, "Intelligence · V2" label'],
                  ['AI copilot', 'Hidden in settings', 'Ask Anika CTA promoted to persistent sidebar element above nav'],
                  ['Forecast tab', 'Forward Signals (chart only)', 'Plain-English explainer card above the 90-day chart'],
                  ['KPI Causal Map', 'Graph only', 'Plain-English explainer card above the ontology graph'],
                  ['Executive Brief', 'Multi-column card grid', 'Single-column cause → consequence → fix narrative'],
                  ['Excel export', '1-row header (keys only)', '4-row header block: key / name / unit / used-for'],
                  ['Domain analysis', 'Static label "Likely Drivers"', 'Period-aware: green period shows "What\'s Contributing"'],
                  ['Benchmarking', 'Not present', 'Industry peer comparison by stage, visual bar in KPI panel'],
                  ['Default tab', 'Dashboard / Scorecard', 'Executive Brief'],
                ].map(([area, v1, v2]) => (
                  <tr key={area} className="hover:bg-slate-50/50">
                    <td className="py-2.5 px-4 font-medium text-slate-700 text-xs">{area}</td>
                    <td className="py-2.5 px-4 text-slate-400 text-xs">{v1}</td>
                    <td className="py-2.5 px-4 text-[#0055A4] text-xs font-medium">{v2}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* ── 2. Tech Stack ────────────────────────────────────── */}
          <SectionHeading id="stack">2. Tech Stack</SectionHeading>

          <div className="grid grid-cols-2 gap-4 mb-4">
            {[
              { label: 'Backend', color: 'blue', items: [
                'FastAPI (Python 3.11)',
                'SQLite via backend/uploads/axiom.db',
                'uvicorn (ASGI server)',
                'openpyxl — Excel import/export',
                'numpy / scipy — Markov forecast',
                'pandas — data processing',
              ]},
              { label: 'Frontend', color: 'green', items: [
                'React 18 + Vite',
                'Tailwind CSS (utility-first)',
                'Recharts (charts)',
                'Lucide React (icons)',
                'axios (HTTP client)',
              ]},
              { label: 'Deployment', color: 'amber', items: [
                'Render (render.yaml at repo root)',
                'Backend: Render web service (Python)',
                'Frontend: built to frontend/dist/',
                'Static files served from FastAPI',
                'Single-service deployment',
              ]},
              { label: 'Dev Tooling', color: 'purple', items: [
                'Vite dev server (port 5176)',
                'uvicorn --reload (port 8003)',
                'V1 node_modules/vite binary for V2 builds (path-spaces workaround)',
                'Git remote: rahulbahri/sigint-v2',
              ]},
            ].map(block => (
              <div key={block.label} className="rounded-xl border border-slate-200 p-4">
                <div className="flex items-center gap-2 mb-2">
                  <Pill color={block.color}>{block.label}</Pill>
                </div>
                <ul className="space-y-1">
                  {block.items.map(item => (
                    <li key={item} className="text-xs text-slate-600 flex items-start gap-2">
                      <span className="text-slate-300 flex-shrink-0 mt-0.5">·</span>
                      {item}
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </div>

          {/* ── 3. Running Locally ───────────────────────────────── */}
          <SectionHeading id="running">3. Running Locally</SectionHeading>

          <SubHeading>Backend</SubHeading>
          <Code>{`cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8003`}</Code>
          <p className="text-xs text-slate-500 mb-3">
            DB auto-creates at <InlineCode>backend/uploads/axiom.db</InlineCode> on first run.
            If the DB is empty the seed routine inserts 5 years of demo data.
            FastAPI docs available at <InlineCode>http://localhost:8003/api/docs</InlineCode>.
          </p>

          <SubHeading>Frontend (dev)</SubHeading>
          <Code>{`cd frontend
npm install
npm run dev    # http://localhost:5176`}</Code>
          <p className="text-xs text-slate-500 mb-3">
            Vite proxies <InlineCode>/api/*</InlineCode> to <InlineCode>localhost:8003</InlineCode>.
            See <InlineCode>frontend/vite.config.js</InlineCode> for the proxy config.
          </p>

          <SubHeading>Frontend (production build)</SubHeading>
          <Code>{`# Workaround: V2 node_modules has a path-with-spaces issue.
# Use V1's vite binary, pointed at V2's frontend source:

cd "/Users/rahulbahri/Signal-platform V2/signals-intelligence-v2/frontend"
"/Users/rahulbahri/Signal-platform V1/signals-intelligence-main/frontend/node_modules/.bin/vite" build`}</Code>
          <p className="text-xs text-slate-500 mb-3">
            Output goes to <InlineCode>frontend/dist/</InlineCode>. FastAPI serves this as static files in production.
          </p>

          {/* ── 4. Frontend Architecture ─────────────────────────── */}
          <SectionHeading id="frontend-arch">4. Frontend Architecture</SectionHeading>

          <p className="text-xs text-slate-600 leading-relaxed mb-3">
            The entire app is a single-page application. All server state is owned by <InlineCode>App.jsx</InlineCode> and
            passed down as props. There is no global state library — React <InlineCode>useState</InlineCode> and
            <InlineCode>useMemo</InlineCode> are used throughout.
          </p>

          <SubHeading>App.jsx — root state</SubHeading>
          <div className="bg-slate-50 rounded-xl border border-slate-200 p-4 mb-3">
            <div className="space-y-1.5">
              {[
                ['tab', 'Active navigation tab ID. Default: "board"'],
                ['fingerprint', 'Raw fingerprint from /api/fingerprint — 27 KPIs with monthly data, targets, causation'],
                ['filteredFingerprint', 'Derived: fingerprint re-computed with year/month filter applied. Passed to all child tabs.'],
                ['yearFilteredFingerprint', 'Derived: year-only filter (preserves all 12 months) — used by Fingerprint2 tab for compare-periods feature'],
                ['selectedYears / selectedMonths', 'Filter state from MonthRangeFilter. Empty array = show all.'],
                ['companyStage', 'Benchmarking stage. Default "series_b". Saved to localStorage as axiom_stage.'],
                ['benchmarks', 'Fetched from /api/benchmarks?stage=. Re-fetched when stage changes.'],
                ['summary', 'From /api/summary — KPI count, status breakdown, months of data'],
                ['bridgeData', 'From /api/bridge — plan vs actual comparison data'],
              ].map(([key, desc]) => (
                <div key={key} className="flex gap-3 text-xs">
                  <InlineCode>{key}</InlineCode>
                  <span className="text-slate-500 flex-1">{desc}</span>
                </div>
              ))}
            </div>
          </div>

          <SubHeading>NAV_GROUPS — V2 navigation structure</SubHeading>
          <Code>{`const NAV_GROUPS = [
  { label: 'Intelligence', tabs: [
    { id: 'board',    label: 'Executive Brief',   Icon: Layers    },
    { id: 'forecast', label: 'Forward Signals',   Icon: BarChart2 },
  ]},
  { label: 'Analysis', tabs: [
    { id: 'dashboard',   label: 'Command Center',          Icon: LayoutDashboard },
    { id: 'fingerprint', label: 'Performance Fingerprint', Icon: Fingerprint     },
    { id: 'trends',      label: 'Trend Explorer',          Icon: TrendingUp      },
    { id: 'projection',  label: 'Plan vs Actual',          Icon: GitBranch       },
  ]},
  { label: 'Knowledge', tabs: [
    { id: 'ontology', label: 'KPI Causal Map', Icon: Network },
  ]},
  { label: 'Settings', tabs: [
    { id: 'upload',  label: 'Data Upload',   Icon: Upload   },
    { id: 'api',     label: 'API Reference', Icon: Code2    },
    { id: 'devdocs', label: 'Dev Docs',      Icon: BookOpen },
  ]},
]`}</Code>

          <SubHeading>Component tree</SubHeading>
          <Code>{`App
├── aside (sidebar)
│   ├── Logo / BHI ring
│   ├── Stage selector (benchmarking pill buttons)
│   ├── Ask Anika CTA button
│   ├── nav (NAV_GROUPS rendered as sidebar-link buttons)
│   ├── AiQueryPanel (collapsed by default)
│   └── footer (Refresh / Load Demo / API Docs)
├── main content
│   ├── header (page title + period info)
│   ├── MonthRangeFilter (visible on data tabs)
│   ├── Anika nudge bar (contextual)
│   └── tab content (one of):
│       ├── BoardReady      ← tab: 'board'
│       ├── Scorecard       ← tab: 'dashboard'
│       ├── Fingerprint2    ← tab: 'fingerprint'
│       ├── MonthlyTrend    ← tab: 'trends'
│       ├── ProjectionBridge← tab: 'projection'
│       ├── OntologyPage    ← tab: 'ontology'
│       ├── ForecastPage    ← tab: 'forecast'
│       ├── CSVUpload       ← tab: 'upload'
│       ├── APIReference    ← tab: 'api'
│       └── DevDocs         ← tab: 'devdocs'
└── KpiDetailPanel (global fixed overlay, shown when selectedKpi != null)`}</Code>

          {/* ── 5. Backend Architecture ──────────────────────────── */}
          <SectionHeading id="backend-arch">5. Backend Architecture</SectionHeading>

          <p className="text-xs text-slate-600 leading-relaxed mb-3">
            The entire backend is a single file: <InlineCode>backend/main.py</InlineCode> (~3800 lines).
            FastAPI is used for routing. SQLite is accessed via the standard <InlineCode>sqlite3</InlineCode> module
            (no ORM). DB tables are created at startup if they do not exist. If the DB is empty, demo data is seeded automatically.
          </p>

          <SubHeading>Key Endpoints</SubHeading>
          <div className="overflow-x-auto rounded-xl border border-slate-200 mb-4">
            <table className="w-full text-xs">
              <thead>
                <tr className="bg-slate-50 border-b border-slate-200">
                  <th className="text-left text-slate-500 font-semibold py-2.5 px-4">Method + Path</th>
                  <th className="text-left text-slate-500 font-semibold py-2.5 px-4">Description</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {[
                  ['GET /api/summary',                    'KPI count, status breakdown, months of data'],
                  ['GET /api/fingerprint?year=&month=',   '27-KPI fingerprint with monthly data, causation rules, targets'],
                  ['GET /api/projection-bridge',          'Plan vs actual comparison — gap %, status per KPI'],
                  ['GET /api/ontology/graph',             'KPI knowledge graph (nodes + edges)'],
                  ['GET /api/ontology/recommendations',   'Signal-based KPI improvement recommendations'],
                  ['POST /api/ontology/discover',         'Run ontology discovery and populate graph tables'],
                  ['GET /api/forecast/model',             'Markov model status (trained_at, KPI list)'],
                  ['POST /api/forecast/build',            'Build / retrain the Markov forecast model'],
                  ['POST /api/forecast/project',          'Run a projection (horizon_days, overrides, n_samples)'],
                  ['POST /api/ai/query',                  'Anika AI query — returns plain-English analysis'],
                  ['GET /api/export/data.xlsx',           'Excel export with 4-row header (key / name / unit / used-for)'],
                  ['POST /api/import/data',               'Excel import — auto-detects V1 legacy vs V2 enriched format'],
                  ['GET /api/benchmarks?stage=',          'Industry benchmark percentiles for the given company stage (seed / series_a / series_b / series_c)'],
                ].map(([ep, desc]) => (
                  <tr key={ep} className="hover:bg-slate-50/50">
                    <td className="py-2.5 px-4 font-mono text-[#0055A4]">{ep}</td>
                    <td className="py-2.5 px-4 text-slate-500">{desc}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <SubHeading>Key data structures in main.py</SubHeading>
          <div className="space-y-2 mb-4">
            {[
              { name: 'KPI_DEFS', desc: 'List of 27 KPI dicts: key, name, unit, direction, formula. Source of truth for what KPIs exist.' },
              { name: 'CAUSATION_RULES', desc: 'Dict keyed by KPI key → { root_causes, downstream_impact, corrective_actions }. Merged into fingerprint response.' },
              { name: 'BENCHMARKS', desc: 'Dict keyed by KPI key → { seed, series_a, series_b, series_c } → { p25, p50, p75 }. Sourced from OpenView / Bessemer / SaaS Capital reports.' },
              { name: 'DEFAULT_TARGETS', desc: 'Dict of KPI key → target value used as fallback when no user-uploaded targets exist.' },
            ].map(item => (
              <div key={item.name} className="bg-slate-50 rounded-lg border border-slate-200 px-4 py-3 flex gap-3">
                <InlineCode>{item.name}</InlineCode>
                <span className="text-xs text-slate-500 flex-1">{item.desc}</span>
              </div>
            ))}
          </div>

          {/* ── 6. Data Model ────────────────────────────────────── */}
          <SectionHeading id="data-model">6. Data Model (SQLite)</SectionHeading>
          <p className="text-xs text-slate-500 mb-3">
            DB path: <InlineCode>backend/uploads/axiom.db</InlineCode>
          </p>

          <div className="space-y-3 mb-4">
            {[
              { table: 'uploads', cols: 'id, filename, uploaded_at, status', desc: 'One row per uploaded file. status: "processed" | "error".' },
              { table: 'monthly_data', cols: 'id, upload_id, year, month, data_json', desc: 'data_json is a JSON blob of {kpi_key: value}. One row per month per upload.' },
              { table: 'kpi_targets', cols: 'id, kpi_id (= key string), target_value, unit, direction', desc: 'User-configurable targets. Read at query time and merged into fingerprint.' },
              { table: 'projection_uploads', cols: 'id, filename, uploaded_at, status', desc: 'Same structure as uploads but for plan/projection files.' },
              { table: 'projection_monthly_data', cols: 'id, upload_id, year, month, data_json', desc: 'Plan data. Joined with monthly_data in the bridge endpoint to compute gaps.' },
              { table: 'markov_models', cols: 'id, kpis, thresholds, self_matrices, cross_matrices, trained_at, regime_data', desc: 'Serialised Markov chain parameters. One row = one trained model.' },
              { table: 'forecast_runs', cols: 'id, model_id, horizon_days, overrides, n_samples, result_json, created_at', desc: 'Results of each /api/forecast/project call.' },
              { table: 'ontology_nodes', cols: 'id, kpi_key, name, domain, influence_score', desc: 'Knowledge graph nodes — one per KPI.' },
              { table: 'ontology_edges', cols: 'id, source_key, target_key, correlation, edge_type', desc: 'Knowledge graph edges — causal/correlation relationships.' },
              { table: 'ontology_recommendations', cols: 'id, rec_type, kpi_key, title, description, priority, dismissed', desc: 'Surface-level signal recommendations derived from the graph.' },
            ].map(row => (
              <div key={row.table} className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
                <div className="flex items-start gap-3">
                  <InlineCode>{row.table}</InlineCode>
                  <div>
                    <p className="text-[11px] font-mono text-slate-500 mb-1">{row.cols}</p>
                    <p className="text-xs text-slate-400">{row.desc}</p>
                  </div>
                </div>
              </div>
            ))}
          </div>

          {/* ── 7. KPI Definitions ───────────────────────────────── */}
          <SectionHeading id="kpi-defs">7. KPI Definitions</SectionHeading>
          <p className="text-xs text-slate-500 mb-3">
            27 KPIs total. First 18 appear in the main dashboard. Last 9 are enriched/derived KPIs added in V2.
          </p>
          <div className="overflow-x-auto rounded-xl border border-slate-200 mb-4">
            <table className="w-full text-xs">
              <thead>
                <tr className="bg-slate-50 border-b border-slate-200">
                  {['Key', 'Name', 'Unit', 'Dir', 'Formula'].map(h => (
                    <th key={h} className="text-left text-slate-500 font-semibold py-2.5 px-3 whitespace-nowrap">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {KPI_TABLE.map((kpi, i) => (
                  <tr key={kpi.key} className={i % 2 === 0 ? '' : 'bg-slate-50/40'}>
                    <td className="py-2 px-3 font-mono text-[#0055A4] whitespace-nowrap">{kpi.key}</td>
                    <td className="py-2 px-3 text-slate-700 font-medium whitespace-nowrap">{kpi.name}</td>
                    <td className="py-2 px-3 text-slate-500">{kpi.unit}</td>
                    <td className="py-2 px-3">
                      <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded ${kpi.dir === 'higher' ? 'bg-emerald-50 text-emerald-600' : 'bg-blue-50 text-blue-600'}`}>
                        {kpi.dir === 'higher' ? '↑ max' : '↓ min'}
                      </span>
                    </td>
                    <td className="py-2 px-3 font-mono text-slate-400 text-[10px]">{kpi.formula}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* ── 8. V2 Changelog ──────────────────────────────────── */}
          <SectionHeading id="changelog">8. V2 Changelog</SectionHeading>
          <div className="space-y-2 mb-4">
            {CHANGELOG.map((entry, i) => (
              <div key={i} className="flex gap-4 py-2.5 border-b border-slate-100 last:border-0">
                <span className="font-mono text-[11px] text-slate-400 flex-shrink-0 w-24">{entry.date}</span>
                <span className="text-xs text-slate-600">{entry.item}</span>
              </div>
            ))}
          </div>

          {/* ── 9. Build Decisions Log ───────────────────────────── */}
          <SectionHeading id="decisions">9. Build Decisions Log</SectionHeading>
          <div className="space-y-3 mb-4">
            {DECISIONS.map((d, i) => (
              <div key={i} className="rounded-xl border border-slate-200 bg-white p-4">
                <p className="text-[12px] font-semibold text-slate-800 mb-1">{d.decision}</p>
                <p className="text-xs text-slate-500 leading-relaxed">{d.rationale}</p>
              </div>
            ))}
          </div>

          {/* ── 10. Roadmap / Known Issues ───────────────────────── */}
          <SectionHeading id="roadmap">10. Roadmap / Known Issues</SectionHeading>

          <SubHeading>Roadmap</SubHeading>
          <ul className="space-y-1.5 mb-5">
            {[
              'Slack / email alerts — trigger when KPI crosses red threshold',
              'Integration scaffolding — Xero / QuickBooks OAuth UI (backend hooks ready)',
              'Onboarding flow — guided setup for first-time data upload',
              'Audit trail — log of data uploads, deletions, and config changes',
              'KPI annotations — attach board notes to specific months',
              'Mobile responsive layout — sidebar collapses to bottom nav on small screens',
              'Multi-tenant / per-org DB isolation (swap SQLite for Postgres)',
            ].map(item => (
              <li key={item} className="flex items-start gap-2 text-xs text-slate-600">
                <span className="text-[#0055A4] font-bold flex-shrink-0 mt-0.5">→</span>
                {item}
              </li>
            ))}
          </ul>

          <SubHeading>Known Issues</SubHeading>
          <div className="space-y-2 mb-6">
            {[
              {
                issue: 'node_modules path-with-spaces bug (V2 Vite build)',
                detail: 'The V2 repo path contains a space ("Signal-platform V2/") which breaks the local vite binary. Workaround: use the V1 vite binary explicitly. See "Running Locally > Frontend (production build)" above.',
              },
              {
                issue: 'Markov model requires minimum 12 months of data',
                detail: 'The /api/forecast/build endpoint will return an error if fewer than 12 monthly data points are available. The Forward Signals tab shows an appropriate empty state in this case.',
              },
              {
                issue: 'Excel import V1 legacy format detection is heuristic',
                detail: 'The import endpoint checks for the 4-row V2 header block. If a V1 file happens to have data in row 1 that looks like KPI keys, it may be mis-detected. Manual review recommended for legacy imports.',
              },
            ].map((item, i) => (
              <div key={i} className="rounded-xl border border-amber-200 bg-amber-50 p-4">
                <p className="text-[12px] font-semibold text-amber-800 mb-1">{item.issue}</p>
                <p className="text-xs text-amber-700 leading-relaxed">{item.detail}</p>
              </div>
            ))}
          </div>

        </div>
      </main>
    </div>
  )
}
