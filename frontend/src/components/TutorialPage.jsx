import { useState, useRef, useEffect } from 'react'
import {
  Gauge, Layers, Presentation, Activity, BookMarked,
  Fingerprint, TrendingUp, BarChart2, GitBranch, Sliders,
  Database, Upload, Bell, Target, Shield, Settings2, Users,
  Network, LayoutDashboard,
  ChevronRight, ChevronDown, Search, ArrowRight,
  Zap, CheckCircle2, Info, Star, Lightbulb, BookOpen,
  AlertTriangle, Rocket, Lock, Eye, Download, Play
} from 'lucide-react'

// ── Section definitions ──────────────────────────────────────────────────────
const SECTIONS = [
  {
    id: 'overview',
    label: 'Platform Overview',
    Icon: Rocket,
    accent: '#0055A4',
    group: 'Getting Started',
  },
  {
    id: 'getting_started',
    label: 'Quick Start Guide',
    Icon: Play,
    accent: '#0055A4',
    group: 'Getting Started',
  },

  // Intelligence
  { id: 'home',       label: 'Home',               Icon: Gauge,         accent: '#0055A4', group: 'Intelligence', tab: 'home'       },
  { id: 'board',      label: 'Executive Brief',     Icon: Layers,        accent: '#059669', group: 'Intelligence', tab: 'board'      },
  { id: 'board', label: 'Board Pack',          Icon: Presentation,  accent: '#D97706', group: 'Intelligence', tab: 'board' },
  { id: 'variance',   label: 'Variance Command',    Icon: Activity,      accent: '#DC2626', group: 'Intelligence', tab: 'variance'   },
  { id: 'decisions',  label: 'Decision Log',        Icon: BookMarked,    accent: '#7c3aed', group: 'Intelligence', tab: 'decisions'  },

  // Analysis
  { id: 'fingerprint', label: 'Performance Fingerprint', Icon: Fingerprint, accent: '#7c3aed', group: 'Analysis', tab: 'fingerprint' },
  { id: 'trends',      label: 'Trend Explorer',          Icon: TrendingUp,  accent: '#0891b2', group: 'Analysis', tab: 'trends'      },
  { id: 'forecast',    label: 'Forward Signals',         Icon: BarChart2,   accent: '#059669', group: 'Analysis', tab: 'forecast'    },
  { id: 'projection',  label: 'Plan vs Actual',          Icon: GitBranch,   accent: '#D97706', group: 'Analysis', tab: 'projection'  },
  { id: 'scenario',    label: 'Scenario Planner',        Icon: Sliders,     accent: '#DC2626', group: 'Analysis', tab: 'scenario'    },

  // Data
  { id: 'data_health', label: 'Data Health',   Icon: Database, accent: '#0891b2', group: 'Data', tab: 'data_health' },
  { id: 'upload',      label: 'Manual Upload', Icon: Upload,   accent: '#0891b2', group: 'Data', tab: 'upload'      },

  // Settings
  { id: 'targets', label: 'KPI Targets',      Icon: Target,    accent: '#D97706', group: 'Settings', tab: 'targets' },
  { id: 'alerts',  label: 'Slack Alerts',     Icon: Bell,      accent: '#059669', group: 'Settings', tab: 'alerts'  },
  { id: 'audit',   label: 'Audit Trail',      Icon: Shield,    accent: '#94a3b8', group: 'Settings', tab: 'audit'   },
  { id: 'company', label: 'Company Settings', Icon: Settings2, accent: '#94a3b8', group: 'Settings', tab: 'company' },
  { id: 'team',    label: 'Team & Access',    Icon: Users,     accent: '#94a3b8', group: 'Settings', tab: 'team'    },

  // Labs
  { id: 'ontology',  label: 'KPI Causal Map',   Icon: Network,         accent: '#7c3aed', group: 'Labs', tab: 'ontology'  },
  { id: 'dashboard', label: 'Command Center',   Icon: LayoutDashboard, accent: '#0055A4', group: 'Labs', tab: 'dashboard' },
]

// ── Re-usable sub-components ─────────────────────────────────────────────────

function Pill({ color, children }) {
  const map = {
    blue:   'bg-blue-100 text-blue-700',
    green:  'bg-emerald-100 text-emerald-700',
    amber:  'bg-amber-100 text-amber-700',
    red:    'bg-red-100 text-red-700',
    purple: 'bg-purple-100 text-purple-700',
    slate:  'bg-slate-100 text-slate-600',
    cyan:   'bg-cyan-100 text-cyan-700',
  }
  return (
    <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full ${map[color] || map.slate}`}>
      {children}
    </span>
  )
}

function StepList({ steps }) {
  return (
    <ol className="space-y-3 mt-3">
      {steps.map((step, i) => (
        <li key={i} className="flex gap-3">
          <span className="flex-shrink-0 w-5 h-5 rounded-full bg-[#0055A4] text-white text-[10px] font-bold flex items-center justify-center mt-0.5">{i + 1}</span>
          <div className="flex-1">
            {typeof step === 'string'
              ? <p className="text-slate-600 text-[12px] leading-relaxed">{step}</p>
              : <>
                  <p className="text-slate-700 text-[12px] font-semibold leading-tight mb-0.5">{step.title}</p>
                  <p className="text-slate-500 text-[11px] leading-relaxed">{step.desc}</p>
                </>
            }
          </div>
        </li>
      ))}
    </ol>
  )
}

function FeatureGrid({ features }) {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 gap-2.5 mt-3">
      {features.map((f, i) => (
        <div key={i} className="flex gap-2.5 bg-slate-50 rounded-xl p-3">
          <f.Icon size={14} className="flex-shrink-0 mt-0.5" style={{ color: f.color || '#0055A4' }} />
          <div>
            <p className="text-slate-700 text-[11px] font-semibold">{f.title}</p>
            <p className="text-slate-500 text-[11px] mt-0.5 leading-relaxed">{f.desc}</p>
          </div>
        </div>
      ))}
    </div>
  )
}

function TipBox({ children, type = 'tip' }) {
  const config = {
    tip:     { bg: 'bg-blue-50',   border: 'border-blue-200',   Icon: Lightbulb,    color: '#0055A4', label: 'Pro Tip'  },
    warning: { bg: 'bg-amber-50',  border: 'border-amber-200',  Icon: AlertTriangle, color: '#D97706', label: 'Note'     },
    success: { bg: 'bg-emerald-50',border: 'border-emerald-200',Icon: CheckCircle2, color: '#059669', label: 'Best Practice' },
    info:    { bg: 'bg-purple-50', border: 'border-purple-200', Icon: Info,         color: '#7c3aed', label: 'Did you know?' },
  }[type]
  return (
    <div className={`flex gap-3 ${config.bg} border ${config.border} rounded-xl p-3.5 mt-4`}>
      <config.Icon size={14} className="flex-shrink-0 mt-0.5" style={{ color: config.color }} />
      <div>
        <p className="text-[10px] font-bold uppercase tracking-wider mb-1" style={{ color: config.color }}>{config.label}</p>
        <p className="text-slate-600 text-[11px] leading-relaxed">{children}</p>
      </div>
    </div>
  )
}

function SectionHeader({ Icon, accent, label, tag, children }) {
  return (
    <div className="mb-6">
      <div className="flex items-center gap-2.5 mb-3">
        <div className="w-8 h-8 rounded-xl flex items-center justify-center flex-shrink-0" style={{ backgroundColor: accent + '18' }}>
          <Icon size={16} style={{ color: accent }} />
        </div>
        <div>
          <div className="flex items-center gap-2">
            <h2 className="text-slate-900 text-base font-bold">{label}</h2>
            {tag && <Pill color={tag.color}>{tag.label}</Pill>}
          </div>
        </div>
      </div>
      <p className="text-slate-500 text-[12px] leading-relaxed">{children}</p>
    </div>
  )
}

function SubSection({ title, children }) {
  return (
    <div className="mt-5">
      <h3 className="text-slate-700 text-[11px] font-bold uppercase tracking-widest mb-2">{title}</h3>
      {children}
    </div>
  )
}

function GoToTabButton({ label, onClick }) {
  if (!onClick) return null
  return (
    <button
      onClick={onClick}
      className="mt-5 inline-flex items-center gap-2 bg-[#0055A4] hover:bg-[#004494] text-white text-[11px] font-semibold px-4 py-2 rounded-xl transition-colors"
    >
      Open {label} <ArrowRight size={12} />
    </button>
  )
}

// ── Section content ──────────────────────────────────────────────────────────

function SectionOverview() {
  return (
    <div>
      <SectionHeader Icon={Rocket} accent="#0055A4" label="What is Axiom Intelligence?">
        Axiom Intelligence is a real-time B2B SaaS intelligence platform that transforms your raw financial and operational data into actionable insight. It monitors every KPI that matters to investors, operators, and boards — and surfaces the signals that drive better decisions faster.
      </SectionHeader>

      <div className="bg-gradient-to-br from-[#0055A4]/5 to-purple-50 rounded-2xl p-5 mb-6 border border-[#0055A4]/10">
        <p className="text-slate-700 text-[12px] font-semibold mb-3">The platform is built around three core pillars:</p>
        <div className="space-y-3">
          {[
            { num: '01', label: 'Monitor', color: '#0055A4', desc: 'Track 30+ SaaS KPIs across revenue, retention, efficiency, and risk in one unified dashboard. Automatic red/amber/green status based on your targets.' },
            { num: '02', label: 'Analyse', color: '#7c3aed', desc: 'Identify patterns, trends, and causal relationships in your data. Forecast 90 days ahead. Compare scenarios. Understand what drives what.' },
            { num: '03', label: 'Act',     color: '#059669', desc: 'Generate board-ready packs in one click. Log decisions with rationale. Set Slack alerts for threshold breaches. Keep your entire team aligned.' },
          ].map(({ num, label, color, desc }) => (
            <div key={num} className="flex gap-3">
              <span className="flex-shrink-0 text-[22px] font-black opacity-20 leading-none" style={{ color }}>{num}</span>
              <div>
                <p className="text-slate-800 text-[12px] font-bold" style={{ color }}>{label}</p>
                <p className="text-slate-500 text-[11px] leading-relaxed mt-0.5">{desc}</p>
              </div>
            </div>
          ))}
        </div>
      </div>

      <SubSection title="Who is this for?">
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-2.5">
          {[
            { role: 'Founders & CEOs', desc: 'Single-pane view of company health, board narrative prep, and strategic decision support.', Icon: Star, color: '#D97706' },
            { role: 'CFOs & Finance Teams', desc: 'Deep KPI analysis, variance tracking, forecast accuracy, and automated executive reporting.', Icon: BarChart2, color: '#0055A4' },
            { role: 'Ops & Revenue Leaders', desc: 'Track GTM efficiency, retention signals, and pipeline health — with scenario modelling.', Icon: Zap, color: '#7c3aed' },
          ].map(({ role, desc, Icon: I, color }) => (
            <div key={role} className="bg-white border border-slate-200 rounded-xl p-3.5 shadow-sm">
              <I size={14} style={{ color }} className="mb-2" />
              <p className="text-slate-800 text-[11px] font-bold mb-1">{role}</p>
              <p className="text-slate-500 text-[11px] leading-relaxed">{desc}</p>
            </div>
          ))}
        </div>
      </SubSection>

      <SubSection title="Platform architecture">
        <div className="bg-slate-50 rounded-xl p-4 space-y-2.5">
          {[
            { layer: 'Data Layer',        desc: 'CSV upload or API integration → monthly KPI time-series stored per workspace' },
            { layer: 'Intelligence Layer',desc: 'Health scoring, status computation, forecast models, causal graph engine' },
            { layer: 'Presentation Layer',desc: 'Interactive dashboards, board pack generation, Slack alerts, decision logging' },
            { layer: 'Collaboration Layer',desc: 'Multi-user workspace, audit trail, role-based access, team settings' },
          ].map(({ layer, desc }) => (
            <div key={layer} className="flex gap-3 items-start">
              <div className="w-2 h-2 rounded-full bg-[#0055A4] flex-shrink-0 mt-1.5" />
              <div>
                <span className="text-slate-700 text-[11px] font-semibold">{layer}: </span>
                <span className="text-slate-500 text-[11px]">{desc}</span>
              </div>
            </div>
          ))}
        </div>
      </SubSection>

      <SubSection title="KPIs covered">
        <p className="text-slate-500 text-[11px] mb-2">Axiom monitors 30+ KPIs across six domains:</p>
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
          {[
            { domain: 'Revenue & Growth', examples: 'ARR, MRR, Revenue Growth, Win Rate', color: '#059669' },
            { domain: 'Retention',        examples: 'NRR, Logo Churn, DAU/MAU, NPS',       color: '#0055A4' },
            { domain: 'Efficiency',       examples: 'Magic Number, CAC, Payback Period',    color: '#7c3aed' },
            { domain: 'Margins',          examples: 'Gross Margin, EBITDA, Op Margin',      color: '#D97706' },
            { domain: 'Cash & Risk',      examples: 'Runway, Cash Burn, Burn Multiple',     color: '#DC2626' },
            { domain: 'Unit Economics',   examples: 'LTV, LTV:CAC, Sales Cycle Days',       color: '#0891b2' },
          ].map(({ domain, examples, color }) => (
            <div key={domain} className="bg-slate-50 rounded-xl p-2.5">
              <p className="text-[10px] font-bold mb-0.5" style={{ color }}>{domain}</p>
              <p className="text-slate-400 text-[10px] leading-relaxed">{examples}</p>
            </div>
          ))}
        </div>
      </SubSection>

      <TipBox type="info">
        Axiom is workspace-isolated — your data is completely private to your organisation. All KPI computations happen server-side; the frontend only displays results.
      </TipBox>
    </div>
  )
}

function SectionGettingStarted({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Play} accent="#0055A4" label="Quick Start Guide" tag={{ label: 'Start Here', color: 'blue' }}>
        Get from zero to a fully populated, health-scored dashboard in under 10 minutes. Follow these four steps in order.
      </SectionHeader>

      <div className="space-y-4">
        {[
          {
            step: 1, title: 'Upload your data', tab: 'upload', color: '#0055A4',
            desc: 'Go to Data → Manual Upload and paste or upload a CSV with your monthly KPI data. Each row is one month; each column is one KPI. Use the "Load Demo Data" button to instantly seed 36 months of sample data and explore the platform.',
            tip: 'Your CSV needs at minimum: year, month, and at least one KPI column. The platform auto-detects column names against its 30+ KPI library.',
          },
          {
            step: 2, title: 'Configure your targets', tab: 'targets', color: '#7c3aed',
            desc: 'Go to Settings → KPI Targets. Set a target value for each KPI you track. This is what drives the red/amber/green status system — without targets, all KPIs show grey "No Target".',
            tip: 'Start with your most important 5–8 KPIs. You can always add more targets later. Industry benchmarks are shown next to each input.',
          },
          {
            step: 3, title: 'Review your Health Score', tab: 'home', color: '#059669',
            desc: 'Return to Home. You should now see a Health Score (0–100), KPI distribution (on target / watch / critical), and spotlight cards for your most important KPIs. Click any card to see full computation details.',
            tip: 'A score of 65+ is healthy for most-stage companies. Below 50 indicates multiple KPIs need urgent attention.',
          },
          {
            step: 4, title: 'Explore your intelligence', tab: 'variance', color: '#D97706',
            desc: 'With data and targets in place, all other tabs unlock. Start with Variance Command for a prioritised action list, then try Performance Fingerprint for pattern analysis, and Forward Signals for 90-day forecasts.',
            tip: 'Bookmark the platform and check it weekly. The real value compounds when you track trends month-over-month.',
          },
        ].map(({ step, title, tab, color, desc, tip }) => (
          <div key={step} className="border border-slate-200 rounded-2xl p-4 shadow-sm">
            <div className="flex items-center gap-3 mb-2">
              <div className="w-7 h-7 rounded-full flex items-center justify-center text-white text-[11px] font-black flex-shrink-0" style={{ backgroundColor: color }}>
                {step}
              </div>
              <p className="text-slate-800 text-[13px] font-bold">{title}</p>
            </div>
            <p className="text-slate-500 text-[12px] leading-relaxed ml-10">{desc}</p>
            <div className="flex gap-2 items-start ml-10 mt-2.5 bg-slate-50 rounded-xl p-2.5">
              <Lightbulb size={12} className="flex-shrink-0 mt-0.5" style={{ color }} />
              <p className="text-slate-500 text-[11px] leading-relaxed">{tip}</p>
            </div>
          </div>
        ))}
      </div>

      <TipBox type="success">
        The platform fully unlocks once you have at least 3 months of data and targets set for 5+ KPIs. The Health Score, Forecast, and Fingerprint tabs all become much more meaningful with 12+ months of history.
      </TipBox>
    </div>
  )
}

function SectionHome({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Gauge} accent="#0055A4" label="Home" tag={{ label: 'Start Here Daily', color: 'blue' }}>
        The Home screen is your command centre — a single-pane view of company health, designed to answer the question "how is my business doing right now?" in under 30 seconds.
      </SectionHeader>

      <SubSection title="Health Score (0–100)">
        <p className="text-slate-500 text-[12px] leading-relaxed">The circular gauge in the top-left is your Company Health Score. It's a weighted composite of three signals:</p>
        <div className="mt-3 space-y-2">
          {[
            { label: 'Momentum (30%)',           desc: 'How many of your KPIs are trending upward vs downward over the last 3 months.',                         color: '#0055A4' },
            { label: 'Target Achievement (40%)', desc: 'What percentage of KPIs with targets are currently in green (on-target) status.',                       color: '#7c3aed' },
            { label: 'Risk Score (30%)',          desc: 'Inversely weighted by the number of KPIs in critical/red status and negative momentum.',               color: '#DC2626' },
          ].map(({ label, desc, color }) => (
            <div key={label} className="flex gap-3 bg-slate-50 rounded-xl p-3">
              <div className="w-2 h-2 rounded-full flex-shrink-0 mt-1.5" style={{ backgroundColor: color }} />
              <div>
                <p className="text-slate-700 text-[11px] font-semibold">{label}</p>
                <p className="text-slate-500 text-[11px] mt-0.5">{desc}</p>
              </div>
            </div>
          ))}
        </div>
        <p className="text-slate-400 text-[11px] mt-2">Click the score breakdown bars to open a modal with the full formula explanation.</p>
      </SubSection>

      <SubSection title="KPI Distribution">
        <p className="text-slate-500 text-[12px] leading-relaxed">Below the score breakdown, four numbers show the distribution of your KPIs:</p>
        <div className="grid grid-cols-2 gap-2 mt-2">
          {[
            { label: 'On Target (green)',  desc: 'Meeting or exceeding target',          color: '#059669' },
            { label: 'Watch (amber)',      desc: 'Within 10% of target, trending down',  color: '#D97706' },
            { label: 'Critical (red)',     desc: 'Significantly below/above target',     color: '#DC2626' },
            { label: 'No Target (grey)',   desc: 'No benchmark configured yet',          color: '#94a3b8' },
          ].map(({ label, desc, color }) => (
            <div key={label} className="flex gap-2 bg-slate-50 rounded-lg p-2.5">
              <div className="w-2 h-2 rounded-full flex-shrink-0 mt-1" style={{ backgroundColor: color }} />
              <div>
                <p className="text-[11px] font-semibold" style={{ color }}>{label}</p>
                <p className="text-slate-400 text-[10px]">{desc}</p>
              </div>
            </div>
          ))}
        </div>
        <p className="text-slate-400 text-[11px] mt-2">Click the distribution grid to open a modal explaining each status in detail.</p>
      </SubSection>

      <SubSection title="Needs Attention &amp; Doing Well">
        <p className="text-slate-500 text-[12px] leading-relaxed">The platform automatically identifies the KPIs most in need of attention (red) and those performing well (green). Each card shows:</p>
        <ul className="mt-2 space-y-1.5">
          {['Current 6-month average value', 'Target value and gap percentage', 'Mini sparkline of recent trend', 'Click to open full context drawer'].map(item => (
            <li key={item} className="flex gap-2 text-slate-500 text-[11px]"><ChevronRight size={11} className="flex-shrink-0 mt-0.5 text-slate-300"/>{item}</li>
          ))}
        </ul>
      </SubSection>

      <SubSection title="KPI Slide-Out Drawer">
        <p className="text-slate-500 text-[12px] leading-relaxed">Click any KPI card to open a 400px right-side drawer. This shows:</p>
        <StepList steps={[
          'Current value, target, gap percentage and sparkline',
          'Auto-generated narrative explaining the KPI\'s status in plain English',
          '"What is this?" — definition of the metric',
          '"Why it matters" — business significance and industry context',
          '"How it\'s computed" — the exact formula used',
          '"Open Full Analysis" button that navigates to the relevant analysis tab',
        ]} />
      </SubSection>

      <SubSection title="Data Period &amp; Freshness">
        <p className="text-slate-500 text-[12px] leading-relaxed">The top bar shows your data period (e.g. "2022-01 – 2024-12") and the date of last upload. This tells you how current your intelligence is. Click the refresh icon to reload data without navigating away.</p>
      </SubSection>

      <TipBox type="tip">
        Check the Home screen every Monday morning. The narrative explanation and health score give you a 30-second brief before any meeting. If the score dropped week-on-week, click through to Variance Command.
      </TipBox>

      <GoToTabButton label="Home" onClick={() => onNavigate?.('home')} />
    </div>
  )
}

function SectionBoard({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Layers} accent="#059669" label="Executive Brief">
        A board-ready summary view of all your KPIs, formatted for executive presentation. Shows colour-coded status, momentum indicators, and priority actions — all in one screen.
      </SectionHeader>

      <SubSection title="What you see">
        <FeatureGrid features={[
          { Icon: CheckCircle2, color: '#059669', title: 'KPI Status Grid',      desc: 'All KPIs with green/amber/red dots, current value, target, and month-on-month change.' },
          { Icon: AlertTriangle,color: '#DC2626', title: 'Priority Actions',    desc: 'Top 3 red/amber KPIs with suggested actions surfaced automatically.' },
          { Icon: Star,         color: '#D97706', title: 'Board View',          desc: 'Toggle to show only the 5 VC-critical KPIs: Revenue Growth, ARR Growth, Gross Margin, Burn Multiple, NRR.' },
          { Icon: TrendingUp,   color: '#0055A4', title: 'Trend Indicators',    desc: 'Up/down/flat arrows showing MoM direction for each KPI.' },
        ]} />
      </SubSection>

      <SubSection title="How to use it">
        <StepList steps={[
          { title: 'Default view', desc: 'All KPIs organised by status — Critical first, then Needs Attention, then On Target, then No Target.' },
          { title: 'Board View toggle', desc: 'Switch to "Board View" for a focused 5-KPI view that mirrors what investors want to see on a board update.' },
          { title: 'Priority Actions', desc: 'Review the 3 priority actions in the right panel. These auto-populate from your worst-performing KPIs.' },
          { title: 'Drill down', desc: 'Click any KPI row to open the KPI detail panel with full monthly history, benchmark comparisons, and analysis.' },
        ]} />
      </SubSection>

      <TipBox type="tip">
        Use Board View before investor calls. The 5-headline KPIs are exactly what most VCs focus on in Series A/B updates.
      </TipBox>

      <GoToTabButton label="Executive Brief" onClick={() => onNavigate?.('board')} />
    </div>
  )
}

function SectionBoardPack({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Presentation} accent="#D97706" label="Board Pack Generator">
        One-click generation of a professionally formatted PowerPoint board pack. Includes your KPI data, health analysis, variance analysis, and forward outlook — all ready to send to your board.
      </SectionHeader>

      <SubSection title="What it generates">
        <ul className="space-y-1.5 mt-1">
          {[
            'Cover slide with company name and period label',
            'Company Health Score summary slide',
            'Full KPI status grid (colour-coded)',
            'Variance analysis slide (top gaps vs target)',
            'Forward Signals slide (90-day outlook)',
            'Talk tracks: bullet-point narratives for each KPI section',
          ].map(item => (
            <li key={item} className="flex gap-2 text-slate-500 text-[11px]">
              <CheckCircle2 size={11} className="flex-shrink-0 mt-0.5 text-emerald-400"/>
              {item}
            </li>
          ))}
        </ul>
      </SubSection>

      <SubSection title="How to generate a pack">
        <StepList steps={[
          { title: 'Set the period label', desc: 'Type your reporting period (e.g. "Q3 2025" or "September 2025"). This appears on the cover.' },
          { title: 'Choose a theme', desc: 'Four options: Corporate Blue (default), Axiom Dark, Slate Professional, Minimal Light.' },
          { title: 'Select content options', desc: 'Toggle on/off: Talk Tracks, Variance Slide, Forward Signals slide.' },
          { title: 'Click Generate', desc: 'The PPTX is built server-side and downloads automatically. Usually takes 3–8 seconds.' },
        ]} />
      </SubSection>

      <TipBox type="warning">
        Board Pack generation requires data to be uploaded and KPI targets to be configured. With no data or targets, the generated pack will have limited content.
      </TipBox>

      <TipBox type="tip">
        Generate a board pack the day before your board meeting. Set the period label to match your board cadence (monthly or quarterly), and use the "Slate Professional" theme for a clean, neutral look.
      </TipBox>

      <GoToTabButton label="Board Pack" onClick={() => onNavigate?.('board')} />
    </div>
  )
}

function SectionVariance({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Activity} accent="#DC2626" label="Variance Command" tag={{ label: 'Most Powerful', color: 'red' }}>
        The operational heartbeat of the platform. Variance Command shows every KPI gap in priority order, assigns ownership, tracks accountability, and generates smart action recommendations — all in one place.
      </SectionHeader>

      <SubSection title="Reading the KPI table">
        <p className="text-slate-500 text-[12px] leading-relaxed">Each row shows one KPI with:</p>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mt-2">
          {[
            { col: 'Status badge',    desc: 'Red / Amber / Green indicating performance vs target' },
            { col: 'Actual value',    desc: 'Your 6-month average for this KPI' },
            { col: 'Target value',    desc: 'The benchmark you set in KPI Targets' },
            { col: 'Gap %',           desc: 'How far above or below target you are' },
            { col: 'Trend arrow',     desc: 'Direction of change over last 3 months' },
            { col: 'Owner',           desc: 'Who is accountable for this KPI' },
          ].map(({ col, desc }) => (
            <div key={col} className="flex gap-2 bg-slate-50 rounded-lg p-2">
              <ChevronRight size={10} className="flex-shrink-0 mt-1 text-slate-300"/>
              <div>
                <span className="text-slate-700 text-[11px] font-semibold">{col}: </span>
                <span className="text-slate-400 text-[11px]">{desc}</span>
              </div>
            </div>
          ))}
        </div>
      </SubSection>

      <SubSection title="Expanding a KPI row">
        <p className="text-slate-500 text-[12px] mb-2 leading-relaxed">Click any KPI row to expand it. You'll see:</p>
        <StepList steps={[
          { title: 'Smart Actions', desc: 'Stage-aware tactical recommendations for improving this KPI. These are tailored to your company stage (Series A, B, etc.).' },
          { title: 'Accountability', desc: 'Assign an owner by name, set a review due date, and set status: Open → Resolved → Reversed.' },
          { title: 'Outcome notes', desc: 'Once resolved, record what action was taken and what the result was. This builds your institutional knowledge base.' },
        ]} />
      </SubSection>

      <SubSection title="Weekly Briefing (HTML report)">
        <p className="text-slate-500 text-[12px] leading-relaxed">Click the "Weekly Briefing" button in the top-right to generate a self-contained HTML report that opens in a new browser tab. It includes:</p>
        <ul className="mt-2 space-y-1 text-[11px] text-slate-500">
          {['Full KPI status table with colour coding', 'Executive summary paragraph', 'Top risks and highlights', 'Formatted for printing or email attachment'].map(i => (
            <li key={i} className="flex gap-2"><ChevronRight size={10} className="flex-shrink-0 mt-0.5 text-slate-300"/>{i}</li>
          ))}
        </ul>
      </SubSection>

      <SubSection title="Show All KPIs">
        <p className="text-slate-500 text-[12px] leading-relaxed">By default, only your configured KPIs are shown. Click "Show All" to expand to all 33 KPIs — including those not in your current dataset. KPIs without data will explain what data is needed to unlock them.</p>
      </SubSection>

      <TipBox type="tip">
        Run a weekly 15-minute team meeting using Variance Command as your agenda. Walk through each red KPI, confirm/update the owner, and review any resolved items from the previous week.
      </TipBox>

      <TipBox type="success">
        The accountability system is designed to create an audit trail of who owned what and when. Over time this becomes invaluable for board updates and retrospectives.
      </TipBox>

      <GoToTabButton label="Variance Command" onClick={() => onNavigate?.('variance')} />
    </div>
  )
}

function SectionDecisions({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={BookMarked} accent="#7c3aed" label="Decision Log">
        A structured journal of the key decisions made in your business — what was decided, why, who decided it, which KPIs it was meant to move, and what actually happened.
      </SectionHeader>

      <SubSection title="Why use it">
        <p className="text-slate-500 text-[12px] leading-relaxed">Most companies make good decisions — but lose the institutional memory of why. The Decision Log solves this by creating a searchable, permanent record that you can:</p>
        <ul className="mt-2 space-y-1.5">
          {[
            'Reference in board meetings to show decision-making rigour',
            'Use in retrospectives to learn from what worked and what didn\'t',
            'Share with new hires to explain strategic context',
            'Link to specific KPIs to see cause and effect over time',
          ].map(item => (
            <li key={item} className="flex gap-2 text-slate-500 text-[11px]"><CheckCircle2 size={11} className="flex-shrink-0 mt-0.5 text-purple-400"/>{item}</li>
          ))}
        </ul>
      </SubSection>

      <SubSection title="Creating a decision">
        <StepList steps={[
          { title: 'Click "New Decision"', desc: 'Opens the entry form in-line.' },
          { title: 'Title', desc: 'Short, descriptive name (e.g. "Shift to product-led growth", "Reduce burn by 20%").' },
          { title: 'Decision statement', desc: 'Exactly what was decided in one or two sentences. Be specific.' },
          { title: 'Rationale', desc: 'Why was this the right call? What data or signal prompted it?' },
          { title: 'Decision maker', desc: 'Who made the call — CEO, CFO, Board, etc.' },
          { title: 'KPI context', desc: 'Select which KPIs this decision is intended to impact.' },
          { title: 'Save', desc: 'The entry appears in the log with an "Active" status badge.' },
        ]} />
      </SubSection>

      <SubSection title="Tracking outcomes">
        <p className="text-slate-500 text-[12px] leading-relaxed">Once a decision plays out, expand the row to record the outcome and change status to:</p>
        <div className="space-y-1.5 mt-2">
          {[
            { status: 'Active',   color: '#0055A4', desc: 'Decision is in flight — being executed' },
            { status: 'Resolved', color: '#059669', desc: 'Completed with a measurable outcome recorded' },
            { status: 'Reversed', color: '#DC2626', desc: 'The decision was undone — record why' },
          ].map(({ status, color, desc }) => (
            <div key={status} className="flex gap-3 bg-slate-50 rounded-xl p-2.5">
              <span className="text-[10px] font-bold px-2 py-0.5 rounded-full text-white flex-shrink-0" style={{ backgroundColor: color }}>{status}</span>
              <p className="text-slate-500 text-[11px] leading-snug">{desc}</p>
            </div>
          ))}
        </div>
      </SubSection>

      <SubSection title="Integration with Scenario Planner">
        <p className="text-slate-500 text-[12px] leading-relaxed">When you build a scenario in the Scenario Planner tab, you can click "Push to Decision Log" to automatically pre-fill a Decision Log entry with the scenario assumptions. This creates a tight link between your modelling and your commitments.</p>
      </SubSection>

      <TipBox type="info">
        The Decision Log feeds your board narrative. Before each board meeting, scan for decisions taken since your last update and summarise outcomes — it shows investors you operate with data-driven discipline.
      </TipBox>

      <GoToTabButton label="Decision Log" onClick={() => onNavigate?.('decisions')} />
    </div>
  )
}

function SectionFingerprint({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Fingerprint} accent="#7c3aed" label="Performance Fingerprint" tag={{ label: 'Pattern Analysis', color: 'purple' }}>
        A visual heatmap of your entire KPI history — every month, every metric, in one grid. Designed to reveal patterns, streaks, and structural issues that are invisible in any single-number view.
      </SectionHeader>

      <SubSection title="The heatmap">
        <p className="text-slate-500 text-[12px] leading-relaxed">Each cell represents one KPI in one month. The colour encodes performance:</p>
        <div className="grid grid-cols-4 gap-2 mt-2">
          {[
            { bg: 'bg-emerald-100', border: 'border-emerald-300', label: 'Green',  desc: 'On target' },
            { bg: 'bg-amber-100',   border: 'border-amber-300',   label: 'Amber',  desc: 'Watch zone' },
            { bg: 'bg-red-100',     border: 'border-red-300',     label: 'Red',    desc: 'Below target' },
            { bg: 'bg-slate-100',   border: 'border-slate-300',   label: 'Grey',   desc: 'No target / no data' },
          ].map(({ bg, border, label, desc }) => (
            <div key={label} className={`rounded-lg p-2 border text-center ${bg} ${border}`}>
              <p className="text-[11px] font-bold text-slate-700">{label}</p>
              <p className="text-[10px] text-slate-500">{desc}</p>
            </div>
          ))}
        </div>
        <p className="text-slate-400 text-[11px] mt-2">Reading columns left-to-right shows the trend over time. Reading rows top-to-bottom shows which KPIs have been persistently problematic.</p>
      </SubSection>

      <SubSection title="What to look for">
        <FeatureGrid features={[
          { Icon: AlertTriangle, color: '#DC2626', title: 'Red Streaks', desc: '3+ consecutive red months in a row — a structural problem, not a blip. These are flagged with a streak counter.' },
          { Icon: TrendingUp,    color: '#059669', title: 'Green Runs',  desc: 'KPIs consistently green for 6+ months — these are your business strengths worth doubling down on.' },
          { Icon: Activity,      color: '#D97706', title: 'Volatility',  desc: 'KPIs that alternate red/green rapidly signal data quality issues or external seasonality effects.' },
          { Icon: Eye,           color: '#7c3aed', title: 'Cohort Patterns', desc: 'If multiple KPIs went red in the same month, investigate what systemic event occurred that month.' },
        ]} />
      </SubSection>

      <SubSection title="Period selection">
        <p className="text-slate-500 text-[12px] leading-relaxed">Use the period selector to filter to Q1–Q4, H1/H2, Last 3M/6M, or Full Year. This is useful for:</p>
        <ul className="mt-1.5 space-y-1">
          {['Quarterly business reviews', 'Year-end analysis', 'Comparing same period year-on-year'].map(i => (
            <li key={i} className="flex gap-2 text-slate-500 text-[11px]"><ChevronRight size={10} className="flex-shrink-0 mt-0.5 text-slate-300"/>{i}</li>
          ))}
        </ul>
      </SubSection>

      <TipBox type="tip">
        The Fingerprint is most valuable in retrospective review sessions. Run it for the last 12 months and use it as a structured conversation starter: "Why was ARR Growth red for three months in Q2?"
      </TipBox>

      <GoToTabButton label="Performance Fingerprint" onClick={() => onNavigate?.('fingerprint')} />
    </div>
  )
}

function SectionTrends({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={TrendingUp} accent="#0891b2" label="Trend Explorer">
        Multi-KPI overlay line charts with event annotation. Compare the trajectory of multiple KPIs on one canvas, mark the moments that caused changes, and switch between raw values and % of target.
      </SectionHeader>

      <SubSection title="Multi-KPI overlay">
        <p className="text-slate-500 text-[12px] leading-relaxed">The Trend Explorer defaults to showing your top 4 KPIs on one chart. You can:</p>
        <StepList steps={[
          'Click the KPI selector dropdown to add or remove metrics from the chart',
          'Toggle "Normalised" to show all KPIs as % of target (useful when comparing different units)',
          'Toggle "Raw Values" to see absolute numbers',
          'Hover over any data point to see exact values for all selected KPIs',
        ]} />
      </SubSection>

      <SubSection title="Annotations">
        <p className="text-slate-500 text-[12px] leading-relaxed">Annotations let you mark key business events on the timeline. Click anywhere on the chart to add a note for that month. Examples of useful annotations:</p>
        <div className="grid grid-cols-2 gap-2 mt-2">
          {[
            '"Series A closed — headcount +8"',
            '"New pricing model launched"',
            '"Lost top 3 enterprise clients"',
            '"Entered new market segment"',
          ].map(ex => (
            <div key={ex} className="bg-purple-50 rounded-lg px-3 py-2 text-slate-600 text-[11px] italic border border-purple-100">{ex}</div>
          ))}
        </div>
        <p className="text-slate-400 text-[11px] mt-2">Annotations are stored and persist across sessions. They help future team members understand the context behind inflection points in your data.</p>
      </SubSection>

      <SubSection title="Target reference lines">
        <p className="text-slate-500 text-[12px] leading-relaxed">When viewing raw values, a dotted target line appears for each KPI. The shaded area between the line and target shows the gap at any point in time.</p>
      </SubSection>

      <TipBox type="success">
        Annotate as you go — don't wait for retrospectives. Every time you make a significant change to the business (new hire, product launch, pricing change), add it. In 12 months this becomes an invaluable causal record.
      </TipBox>

      <GoToTabButton label="Trend Explorer" onClick={() => onNavigate?.('trends')} />
    </div>
  )
}

function SectionForecast({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={BarChart2} accent="#059669" label="Forward Signals (90-Day Forecast)" tag={{ label: 'AI-Powered', color: 'green' }}>
        Probabilistic 90-day forecasts for all your KPIs using Markov-chain statistical models. Rather than a single prediction, Forward Signals shows a range of outcomes — from pessimistic to optimistic — so you can stress-test your assumptions.
      </SectionHeader>

      <SubSection title="Training the model">
        <p className="text-slate-500 text-[12px] leading-relaxed">Before forecasts are available, you need to train the model on your historical data:</p>
        <StepList steps={[
          { title: 'Ensure 3+ months of data is uploaded', desc: 'The model requires at least 3 months of KPI history to detect patterns. 12+ months gives significantly better results.' },
          { title: 'Click "Train Model"', desc: 'This starts the model-building process in the background. Training typically takes 10–30 seconds.' },
          { title: 'Wait for confirmation', desc: 'A success badge appears when training is complete. You can now generate forecasts.' },
        ]} />
      </SubSection>

      <SubSection title="Reading the forecast">
        <p className="text-slate-500 text-[12px] leading-relaxed">Each KPI forecast shows probability bands:</p>
        <div className="space-y-1.5 mt-2">
          {[
            { band: 'P90 (top of range)',    desc: 'Optimistic scenario — only 10% chance of exceeding this',   color: '#059669' },
            { band: 'P75',                  desc: 'Positive case',                                              color: '#6ee7b7' },
            { band: 'P50 (median)',          desc: 'Most likely outcome based on historical patterns',          color: '#0055A4' },
            { band: 'P25',                  desc: 'Cautious case',                                             color: '#fbbf24' },
            { band: 'P10 (bottom of range)', desc: 'Pessimistic scenario — 10% chance of going below this',    color: '#DC2626' },
          ].map(({ band, desc, color }) => (
            <div key={band} className="flex gap-2.5 items-start">
              <div className="w-2.5 h-2.5 rounded-sm flex-shrink-0 mt-0.5" style={{ backgroundColor: color }} />
              <div>
                <span className="text-slate-700 text-[11px] font-semibold">{band}: </span>
                <span className="text-slate-400 text-[11px]">{desc}</span>
              </div>
            </div>
          ))}
        </div>
      </SubSection>

      <SubSection title="Scenario comparison">
        <p className="text-slate-500 text-[12px] leading-relaxed">Create up to 5 named scenarios with different assumptions. Compare them side-by-side to understand the range of outcomes for different strategic choices.</p>
      </SubSection>

      <SubSection title="Causal paths">
        <p className="text-slate-500 text-[12px] leading-relaxed">The causal path analysis shows which upstream KPIs are driving each forecast. For example: "NRR is forecast to decline because logo churn has been rising for 3 months."</p>
      </SubSection>

      <TipBox type="warning">
        Forecasts are statistical extrapolations of historical patterns. They don't account for unknown future events (market changes, fundraises, pivots). Use them as a baseline, not a guarantee.
      </TipBox>

      <TipBox type="tip">
        Use the P10–P50 range as your "conservative scenario" for board presentations — it shows investors you're thinking probabilistically rather than just presenting a best-case line.
      </TipBox>

      <GoToTabButton label="Forward Signals" onClick={() => onNavigate?.('forecast')} />
    </div>
  )
}

function SectionProjection({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={GitBranch} accent="#D97706" label="Plan vs Actual">
        If you've uploaded both an actuals file and a projection/plan file, Plan vs Actual overlays them to show exactly where your business is tracking ahead of or behind plan — KPI by KPI, month by month.
      </SectionHeader>

      <SubSection title="What it shows">
        <FeatureGrid features={[
          { Icon: CheckCircle2, color: '#059669', title: 'On Track',    desc: 'KPIs where actual performance is meeting or beating the plan.' },
          { Icon: AlertTriangle,color: '#DC2626', title: 'Behind Plan', desc: 'KPIs with a negative gap between actuals and projected targets.' },
          { Icon: TrendingUp,   color: '#0055A4', title: 'Ahead of Plan',desc: 'KPIs outperforming initial projections.' },
          { Icon: BarChart2,    color: '#7c3aed', title: 'Gap Chart',   desc: 'Waterfall-style chart showing the magnitude of over/under-performance.' },
        ]} />
      </SubSection>

      <SubSection title="How to set it up">
        <StepList steps={[
          { title: 'Upload your actuals', desc: 'In Data → Manual Upload, upload your historical actuals CSV (same format as usual).' },
          { title: 'Upload your plan', desc: 'Upload a separate CSV with the same format but containing your originally projected values for each KPI.' },
          { title: 'Plan vs Actual auto-populates', desc: 'The system automatically detects the overlapping months and computes the deltas.' },
        ]} />
      </SubSection>

      <SubSection title="How it differs from targets">
        <p className="text-slate-500 text-[12px] leading-relaxed">KPI Targets (in Settings) are fixed annual benchmarks — e.g. "Gross Margin should always be above 70%". Plan vs Actual uses a <em>time-series plan</em> — month-by-month projections you set at the start of the year. This enables accountability against a specific financial model, not just a static number.</p>
      </SubSection>

      <TipBox type="info">
        Plan vs Actual is most useful from Month 3 of a new financial year — once you have enough actuals to compare against the plan. Before that, use Forward Signals to validate whether your plan is achievable.
      </TipBox>

      <GoToTabButton label="Plan vs Actual" onClick={() => onNavigate?.('projection')} />
    </div>
  )
}

function SectionScenario({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Sliders} accent="#DC2626" label="Scenario Planner">
        A lever-based business modeller. Adjust seven key levers (revenue growth, churn, CAC, headcount, etc.) and see the downstream impact on all your KPIs in real time. Then save the scenario and push it to the Decision Log.
      </SectionHeader>

      <SubSection title="The seven levers">
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mt-1">
          {[
            { lever: 'Revenue Growth Delta',  desc: 'Change in MoM revenue growth rate (e.g. +2% means targeting 2% faster growth)' },
            { lever: 'Margin Improvement',    desc: 'Change in gross margin percentage' },
            { lever: 'Churn Reduction',       desc: 'Reduction in logo churn rate (negative = increase)' },
            { lever: 'CAC Change',            desc: 'Change in customer acquisition cost' },
            { lever: 'Headcount Change',      desc: 'Net headcount addition/reduction' },
            { lever: 'OpEx Change',           desc: 'Change in total operating expenses' },
            { lever: 'Expansion Revenue',     desc: 'Additional expansion MRR from existing customers' },
          ].map(({ lever, desc }) => (
            <div key={lever} className="bg-slate-50 rounded-xl p-2.5">
              <p className="text-slate-700 text-[11px] font-semibold">{lever}</p>
              <p className="text-slate-400 text-[10px] mt-0.5">{desc}</p>
            </div>
          ))}
        </div>
      </SubSection>

      <SubSection title="How to build a scenario">
        <StepList steps={[
          'Drag each slider to your assumed change (or type a value)',
          'Watch the impact table update in real time — each KPI shows base value, scenario value, and delta',
          'When satisfied, click "Save Scenario" and give it a name (e.g. "Aggressive growth Q3", "Burn reduction plan")',
          'Optionally, click "Push to Decision Log" to create a formal decision entry from this scenario',
          'Load saved scenarios from the dropdown to compare strategies',
        ]} />
      </SubSection>

      <SubSection title="How the projections work">
        <p className="text-slate-500 text-[12px] leading-relaxed">Each lever has a sensitivity matrix that maps its change to downstream KPI impacts. For example, a 10% reduction in churn will improve NRR by ~8%, extend runway by ~2 months, and reduce CAC payback period. These coefficients are calibrated to industry-standard SaaS benchmarks.</p>
      </SubSection>

      <TipBox type="tip">
        Build a "Base Case", "Upside", and "Downside" scenario at the start of each quarter. Use these in your quarterly board update to show the range of outcomes and which scenario you're currently tracking to.
      </TipBox>

      <GoToTabButton label="Scenario Planner" onClick={() => onNavigate?.('scenario')} />
    </div>
  )
}

function SectionDataHealth({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Database} accent="#0891b2" label="Data Health">
        Your data pipeline control centre — shows data source connections, identifies gaps, validates quality, and manages field mappings. If your KPIs are showing unexpected values, start your investigation here.
      </SectionHeader>

      <SubSection title="Four sub-sections">
        <div className="space-y-3 mt-1">
          {[
            { title: 'Data Sources',  desc: 'Status of all configured data connections and import pipelines. Shows last sync time, connection status, and any errors.',          color: '#0055A4' },
            { title: 'Data Gaps',     desc: 'Identifies months where KPI data is missing or incomplete. Helps you understand which analysis tabs will be limited.',            color: '#D97706' },
            { title: 'Data Quality',  desc: 'Validation rules run against your uploaded data. Flags outliers, implausible values, and format inconsistencies.',               color: '#DC2626' },
            { title: 'Field Mappings',desc: 'Configure how your source columns map to Axiom\'s KPI keys. Essential if your CSV uses non-standard column names.',              color: '#7c3aed' },
          ].map(({ title, desc, color }) => (
            <div key={title} className="flex gap-3 border border-slate-200 rounded-xl p-3">
              <div className="w-2 h-2 rounded-full flex-shrink-0 mt-1.5" style={{ backgroundColor: color }} />
              <div>
                <p className="text-slate-700 text-[12px] font-semibold">{title}</p>
                <p className="text-slate-400 text-[11px] mt-0.5 leading-relaxed">{desc}</p>
              </div>
            </div>
          ))}
        </div>
      </SubSection>

      <TipBox type="warning">
        If your Health Score seems too low or KPIs show unexpected zeros, visit Data Gaps first — missing months can create false averages that skew your status calculations.
      </TipBox>

      <GoToTabButton label="Data Health" onClick={() => onNavigate?.('data_health')} />
    </div>
  )
}

function SectionUpload({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Upload} accent="#0891b2" label="Manual Upload">
        The primary data ingestion interface. Upload monthly KPI data via CSV, or use the "Load Demo Data" button to instantly populate 36 months of realistic sample data.
      </SectionHeader>

      <SubSection title="CSV format requirements">
        <p className="text-slate-500 text-[12px] leading-relaxed">Your CSV must follow this structure:</p>
        <div className="bg-slate-900 rounded-xl p-3 mt-2 overflow-x-auto">
          <pre className="text-emerald-400 text-[10px] font-mono leading-relaxed">{`year,month,revenue_growth,gross_margin,arr,mrr,...
2024,1,18.5,72.3,1250000,104166,...
2024,2,19.2,73.1,1280000,106666,...
2024,3,17.8,71.9,1310000,109166,...`}</pre>
        </div>
        <p className="text-slate-400 text-[11px] mt-2">The <code className="bg-slate-100 px-1 py-0.5 rounded text-[10px]">year</code> and <code className="bg-slate-100 px-1 py-0.5 rounded text-[10px]">month</code> columns are required. All KPI columns are optional — include whichever you track.</p>
      </SubSection>

      <SubSection title="Supported KPI column names">
        <p className="text-slate-500 text-[12px] leading-relaxed">Axiom auto-maps 30+ standard column names. Common ones:</p>
        <div className="grid grid-cols-3 gap-1.5 mt-2">
          {['revenue_growth', 'gross_margin', 'arr', 'mrr', 'nrr', 'logo_churn_rate', 'cac', 'ltv', 'burn_multiple', 'runway_months', 'magic_number', 'nps'].map(k => (
            <code key={k} className="bg-slate-100 text-slate-600 text-[10px] px-2 py-1 rounded-lg font-mono">{k}</code>
          ))}
        </div>
      </SubSection>

      <SubSection title="Upload steps">
        <StepList steps={[
          'Go to Data → Manual Upload',
          'Drag and drop your CSV file onto the upload area, or click to browse',
          'Review the preview table to confirm columns mapped correctly',
          'Click "Upload" — data is processed and immediately available across all tabs',
          'Or click "Load Demo Data" to seed 36 months of sample B2B SaaS data instantly',
        ]} />
      </SubSection>

      <TipBox type="warning">
        Each upload replaces the existing data for the months included in the file. It does not delete months not present in the file. Re-uploading the same months with corrected data will overwrite just those months.
      </TipBox>

      <TipBox type="tip">
        Export data from your accounting software (Xero, QuickBooks, NetSuite) as a monthly P&L summary, then manually add operational KPIs (churn, NRR, NPS) in a spreadsheet before uploading as one combined CSV.
      </TipBox>

      <GoToTabButton label="Manual Upload" onClick={() => onNavigate?.('upload')} />
    </div>
  )
}

function SectionTargets({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Target} accent="#D97706" label="KPI Targets" tag={{ label: 'Configure First', color: 'amber' }}>
        Set the target value for each KPI. Targets are the foundation of the entire status system — every red/amber/green signal, health score component, and alert is driven by how your actuals compare to these benchmarks.
      </SectionHeader>

      <SubSection title="Why targets matter">
        <p className="text-slate-500 text-[12px] leading-relaxed">Without targets, all KPIs show as grey "No Target" — and the Health Score Target Achievement component scores zero. Targets are what transform raw data into intelligence.</p>
      </SubSection>

      <SubSection title="Setting targets">
        <StepList steps={[
          'Go to Settings → KPI Targets',
          'KPIs are grouped by domain: Financial, Growth, Retention, Efficiency, Other',
          'Click the target value input next to any KPI',
          'Type your target value (numbers only, without units)',
          'Press Enter or click Save — the new target takes effect immediately across all tabs',
        ]} />
      </SubSection>

      <SubSection title="Direction setting">
        <p className="text-slate-500 text-[12px] leading-relaxed">Each KPI has a direction indicator showing whether higher or lower is better:</p>
        <div className="grid grid-cols-2 gap-2 mt-2">
          <div className="bg-emerald-50 rounded-xl p-3 border border-emerald-200">
            <p className="text-emerald-700 text-[11px] font-bold mb-1">Higher is better</p>
            <p className="text-slate-500 text-[10px]">Revenue Growth, NRR, Gross Margin, Win Rate</p>
          </div>
          <div className="bg-red-50 rounded-xl p-3 border border-red-200">
            <p className="text-red-700 text-[11px] font-bold mb-1">Lower is better</p>
            <p className="text-slate-500 text-[10px]">Churn Rate, CAC, Burn Multiple, Payback Period</p>
          </div>
        </div>
      </SubSection>

      <SubSection title="Suggested benchmarks">
        <p className="text-slate-500 text-[12px] mb-2 leading-relaxed">Industry benchmarks for Series B SaaS companies (adjust for your stage):</p>
        <div className="grid grid-cols-2 gap-1.5">
          {[
            { kpi: 'Revenue Growth',  benchmark: '> 30% YoY' },
            { kpi: 'Gross Margin',    benchmark: '> 70%' },
            { kpi: 'NRR',             benchmark: '> 110%' },
            { kpi: 'Logo Churn',      benchmark: '< 5%' },
            { kpi: 'Burn Multiple',   benchmark: '< 1.5×' },
            { kpi: 'Magic Number',    benchmark: '> 0.75' },
            { kpi: 'CAC Payback',     benchmark: '< 18 months' },
            { kpi: 'Runway',          benchmark: '> 18 months' },
          ].map(({ kpi, benchmark }) => (
            <div key={kpi} className="flex justify-between bg-slate-50 rounded-lg px-3 py-2">
              <span className="text-slate-600 text-[11px]">{kpi}</span>
              <span className="text-slate-400 text-[10px] font-mono">{benchmark}</span>
            </div>
          ))}
        </div>
      </SubSection>

      <TipBox type="success">
        Set targets for your top 8–10 KPIs first, then expand. The more targets configured, the more accurate and meaningful the Health Score becomes.
      </TipBox>

      <GoToTabButton label="KPI Targets" onClick={() => onNavigate?.('targets')} />
    </div>
  )
}

function SectionAlerts({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Bell} accent="#059669" label="Slack Alerts">
        Automated KPI threshold alerts delivered directly to your Slack workspace. Configure once, then receive notifications whenever KPIs enter critical/red status — without having to log in.
      </SectionHeader>

      <SubSection title="Setup steps">
        <StepList steps={[
          { title: 'Create a Slack Incoming Webhook', desc: 'In your Slack workspace, go to Apps → Incoming Webhooks and create one for your desired channel (e.g. #kpi-alerts or #ops).' },
          { title: 'Copy the webhook URL', desc: 'It looks like: https://hooks.slack.com/services/T.../B.../xxx' },
          { title: 'Paste into Axiom', desc: 'Go to Settings → Slack Alerts and paste the URL into the Webhook URL field.' },
          { title: 'Set your company name', desc: 'This appears in the header of every alert message.' },
          { title: 'Test the connection', desc: 'Click "Send Test Message" to verify the webhook is working.' },
          { title: 'Configure triggers', desc: 'Toggle on/off which events trigger an alert.' },
        ]} />
      </SubSection>

      <SubSection title="Alert triggers">
        <div className="space-y-2 mt-1">
          {[
            { trigger: 'KPI enters red status',       desc: 'Fires when any KPI drops below its target threshold into critical range' },
            { trigger: 'Health score drops > 5 pts',  desc: 'Fires when the weekly health score falls by more than 5 points' },
            { trigger: 'New data uploaded',           desc: 'Confirms when new KPI data is successfully processed' },
            { trigger: 'Red streak alert',            desc: 'Fires when any KPI is red for 3+ consecutive months' },
          ].map(({ trigger, desc }) => (
            <div key={trigger} className="flex gap-3 bg-slate-50 rounded-xl p-3">
              <Bell size={12} className="flex-shrink-0 mt-0.5 text-emerald-500" />
              <div>
                <p className="text-slate-700 text-[11px] font-semibold">{trigger}</p>
                <p className="text-slate-400 text-[10px] mt-0.5">{desc}</p>
              </div>
            </div>
          ))}
        </div>
      </SubSection>

      <SubSection title="Manual fire">
        <p className="text-slate-500 text-[12px] leading-relaxed">The "Fire Alert Now" button sends an immediate alert with all current red KPIs listed. Use this before team or board meetings to share a status snapshot with your Slack channel.</p>
      </SubSection>

      <TipBox type="tip">
        Set alerts to your finance or ops Slack channel, not a general channel. Alert fatigue is real — start with just "KPI enters red status" and add more triggers once the team is comfortable with the workflow.
      </TipBox>

      <GoToTabButton label="Slack Alerts" onClick={() => onNavigate?.('alerts')} />
    </div>
  )
}

function SectionAudit({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Shield} accent="#94a3b8" label="Audit Trail">
        A complete, immutable log of all changes made to your workspace — data uploads, target updates, decision log entries, user access changes, and more.
      </SectionHeader>

      <SubSection title="What is logged">
        <ul className="space-y-1.5 mt-1">
          {[
            'Data uploads (who uploaded, when, how many rows)',
            'KPI target changes (old value → new value, changed by whom)',
            'Decision log entries (created, updated, resolved)',
            'Scenario saves',
            'User access changes',
            'Slack alert fires',
          ].map(item => (
            <li key={item} className="flex gap-2 text-slate-500 text-[11px]"><CheckCircle2 size={11} className="flex-shrink-0 mt-0.5 text-slate-300"/>{item}</li>
          ))}
        </ul>
      </SubSection>

      <SubSection title="Why it matters">
        <p className="text-slate-500 text-[12px] leading-relaxed">For investor-backed companies, the Audit Trail provides a compliance record. For operations, it helps diagnose when and why a KPI changed unexpectedly — "did someone update the gross margin target?" is immediately answerable here.</p>
      </SubSection>

      <TipBox type="info">
        The Audit Trail is append-only — entries cannot be deleted. This is intentional for data integrity. If a target was set incorrectly, update it and the correction will appear as a new audit entry.
      </TipBox>

      <GoToTabButton label="Audit Trail" onClick={() => onNavigate?.('audit')} />
    </div>
  )
}

function SectionCompany({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Settings2} accent="#94a3b8" label="Company Settings">
        Configure your workspace identity — company name, stage, fiscal year, and other global settings that affect how the platform presents your data.
      </SectionHeader>

      <SubSection title="Key settings">
        <div className="space-y-2 mt-1">
          {[
            { setting: 'Company Name',    desc: 'Appears in board packs, Slack alerts, and exported reports' },
            { setting: 'Company Stage',   desc: 'Seed / Series A / Series B / Growth / Public — affects smart action recommendations in Variance Command' },
            { setting: 'Fiscal Year Start',desc: 'The month your financial year begins. Affects period labels and year-end reporting.' },
            { setting: 'Primary Currency', desc: 'Used for formatting financial KPIs (ARR, MRR, Cash Burn, etc.)' },
          ].map(({ setting, desc }) => (
            <div key={setting} className="flex gap-3 bg-slate-50 rounded-xl p-3">
              <div className="w-2 h-2 rounded-full bg-slate-300 flex-shrink-0 mt-1.5" />
              <div>
                <p className="text-slate-700 text-[11px] font-semibold">{setting}</p>
                <p className="text-slate-400 text-[11px] mt-0.5">{desc}</p>
              </div>
            </div>
          ))}
        </div>
      </SubSection>

      <TipBox type="warning">
        Set your Company Stage correctly — it determines which smart actions appear in Variance Command. A Series A company has very different recommended actions from a Series B.
      </TipBox>

      <GoToTabButton label="Company Settings" onClick={() => onNavigate?.('company')} />
    </div>
  )
}

function SectionTeam({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Users} accent="#94a3b8" label="Team &amp; Access">
        Manage who has access to your workspace and what permissions they have. All users within the same email domain are automatically grouped into the same workspace.
      </SectionHeader>

      <SubSection title="Access model">
        <p className="text-slate-500 text-[12px] leading-relaxed">Axiom uses domain-based workspaces. Everyone at @yourcompany.com who signs up will see the same data. Role-based permissions control what each person can edit vs. view-only.</p>
      </SubSection>

      <SubSection title="Roles">
        <div className="space-y-2 mt-1">
          {[
            { role: 'Admin',   desc: 'Full access: upload data, change targets, invite/remove users, access all settings' },
            { role: 'Editor',  desc: 'Can upload data, create decisions, set accountability owners. Cannot change team settings.' },
            { role: 'Viewer',  desc: 'Read-only access to all intelligence tabs. Cannot upload or change any settings.' },
          ].map(({ role, desc }) => (
            <div key={role} className="flex gap-3 bg-slate-50 rounded-xl p-3">
              <Lock size={12} className="flex-shrink-0 mt-0.5 text-slate-400" />
              <div>
                <p className="text-slate-700 text-[11px] font-semibold">{role}</p>
                <p className="text-slate-400 text-[11px] mt-0.5">{desc}</p>
              </div>
            </div>
          ))}
        </div>
      </SubSection>

      <TipBox type="tip">
        Invite your CFO as Admin, your department heads as Editors, and board members as Viewers. This gives the board read access to your live dashboard without the risk of accidental data changes.
      </TipBox>

      <GoToTabButton label="Team &amp; Access" onClick={() => onNavigate?.('team')} />
    </div>
  )
}

function SectionOntology({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={Network} accent="#7c3aed" label="KPI Causal Map" tag={{ label: 'Labs', color: 'purple' }}>
        An interactive force-directed graph that visualises the causal and correlative relationships between your KPIs. Understand which metrics drive which other metrics — and where to intervene for maximum impact.
      </SectionHeader>

      <SubSection title="The graph">
        <p className="text-slate-500 text-[12px] leading-relaxed">Each KPI appears as a node. Edges (lines between nodes) show the relationship type:</p>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mt-2">
          {[
            { type: 'Causes',        color: '#DC2626', desc: 'Direct causal relationship (CAC → Payback Period)' },
            { type: 'Leads',         color: '#D97706', desc: 'Leading indicator — one predicts the other with a lag' },
            { type: 'Influences',    color: '#0055A4', desc: 'Partial causal effect — one factor among many' },
            { type: 'Correlates',    color: '#059669', desc: 'Move together, causality unclear' },
            { type: 'Anti-Correlates',color: '#7c3aed',desc: 'Move in opposite directions' },
          ].map(({ type, color, desc }) => (
            <div key={type} className="flex gap-2 bg-slate-50 rounded-lg p-2.5">
              <div className="w-2.5 h-2.5 rounded-full flex-shrink-0 mt-0.5" style={{ backgroundColor: color }} />
              <div>
                <p className="text-[11px] font-semibold" style={{ color }}>{type}</p>
                <p className="text-slate-400 text-[10px]">{desc}</p>
              </div>
            </div>
          ))}
        </div>
      </SubSection>

      <SubSection title="Interacting with the graph">
        <StepList steps={[
          'Drag the background to pan around the graph',
          'Scroll to zoom in and out',
          'Hover a node to see all its direct relationships highlighted',
          'Click a node to open the detail panel showing causal paths and impact coefficients',
          'Use the filter controls to show only specific relationship types',
        ]} />
      </SubSection>

      <SubSection title="Leading indicators">
        <p className="text-slate-500 text-[12px] leading-relaxed">The "Detect Leading Indicators" feature analyses your historical data to find KPIs that consistently predict others with a time lag. For example: "NPS score changes predict NRR changes 2–3 months later." These leading indicators are the most valuable early warning signals in your data.</p>
      </SubSection>

      <TipBox type="info">
        The Causal Map is a Labs feature — it's experimental and the relationships are based on industry research plus your own data correlations. Treat it as a hypothesis tool, not definitive causality.
      </TipBox>

      <GoToTabButton label="KPI Causal Map" onClick={() => onNavigate?.('ontology')} />
    </div>
  )
}

function SectionDashboard({ onNavigate }) {
  return (
    <div>
      <SectionHeader Icon={LayoutDashboard} accent="#0055A4" label="Command Center" tag={{ label: 'Labs', color: 'purple' }}>
        The full KPI grid — every single KPI in one dense table view with current values, targets, MoM change, trend sparklines, and status badges. For power users who want maximum information density.
      </SectionHeader>

      <SubSection title="How it differs from Executive Brief">
        <p className="text-slate-500 text-[12px] leading-relaxed">Executive Brief is curated and designed for presenting. Command Center is raw and dense — it shows all 30+ KPIs in a scrollable grid, sorted by status priority. It's designed for the CFO or ops lead who wants to scan the entire business in one view without any filtering or curation.</p>
      </SubSection>

      <SubSection title="Features">
        <FeatureGrid features={[
          { Icon: Eye,         color: '#0055A4', title: 'All KPIs visible',  desc: 'Every configured KPI in one scrollable grid — no tabs, no pagination.' },
          { Icon: BarChart2,   color: '#7c3aed', title: 'Sparkbars',         desc: 'Mini bar charts showing the last 6 months of performance inline.' },
          { Icon: AlertTriangle,color: '#DC2626',title: 'Priority sorting',  desc: 'Critical KPIs surface to the top automatically.' },
          { Icon: Download,    color: '#059669', title: 'Board View',        desc: 'Toggle to a 5-KPI investor-focused view.' },
        ]} />
      </SubSection>

      <TipBox type="info">
        Command Center is in Labs because the design is still evolving. The core data is the same as Executive Brief — it's purely a different presentation mode.
      </TipBox>

      <GoToTabButton label="Command Center" onClick={() => onNavigate?.('dashboard')} />
    </div>
  )
}

// ── Section renderer ──────────────────────────────────────────────────────────
const SECTION_RENDERERS = {
  overview:    SectionOverview,
  getting_started: SectionGettingStarted,
  home:        SectionHome,
  board:       SectionBoard,
  board:  SectionBoardPack,
  variance:    SectionVariance,
  decisions:   SectionDecisions,
  fingerprint: SectionFingerprint,
  trends:      SectionTrends,
  forecast:    SectionForecast,
  projection:  SectionProjection,
  scenario:    SectionScenario,
  data_health: SectionDataHealth,
  upload:      SectionUpload,
  targets:     SectionTargets,
  alerts:      SectionAlerts,
  audit:       SectionAudit,
  company:     SectionCompany,
  team:        SectionTeam,
  ontology:    SectionOntology,
  dashboard:   SectionDashboard,
}

// ── Main Tutorial Page ────────────────────────────────────────────────────────
export default function TutorialPage({ onNavigate }) {
  const [active, setActive]     = useState('overview')
  const [search, setSearch]     = useState('')
  const [collapsed, setCollapsed] = useState({})
  const contentRef = useRef(null)

  const groups = ['Getting Started', 'Intelligence', 'Analysis', 'Data', 'Settings', 'Labs']

  const filtered = search
    ? SECTIONS.filter(s => s.label.toLowerCase().includes(search.toLowerCase()))
    : SECTIONS

  const grouped = groups.reduce((acc, g) => {
    const items = filtered.filter(s => s.group === g)
    if (items.length) acc[g] = items
    return acc
  }, {})

  const handleSelect = (id) => {
    setActive(id)
    contentRef.current?.scrollTo({ top: 0, behavior: 'smooth' })
  }

  const activeSection = SECTIONS.find(s => s.id === active)
  const SectionComp = SECTION_RENDERERS[active]

  const allIds = SECTIONS.map(s => s.id)
  const currentIdx = allIds.indexOf(active)
  const prevSection = currentIdx > 0 ? SECTIONS[currentIdx - 1] : null
  const nextSection = currentIdx < allIds.length - 1 ? SECTIONS[currentIdx + 1] : null

  return (
    <div className="flex h-[calc(100vh-120px)] max-w-6xl overflow-hidden rounded-2xl border border-slate-200 shadow-sm bg-white">

      {/* ── Sidebar ──────────────────────────────────────────────────── */}
      <div className="w-56 flex-shrink-0 border-r border-slate-100 flex flex-col bg-slate-50/60">
        {/* Search */}
        <div className="p-3 border-b border-slate-100">
          <div className="flex items-center gap-2 bg-white border border-slate-200 rounded-xl px-3 py-2">
            <Search size={12} className="text-slate-400 flex-shrink-0" />
            <input
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Search sections…"
              className="flex-1 text-[11px] bg-transparent outline-none text-slate-700 placeholder-slate-400"
            />
          </div>
        </div>

        {/* Nav */}
        <div className="flex-1 overflow-y-auto py-2">
          {Object.entries(grouped).map(([group, items]) => (
            <div key={group} className="mb-1">
              <button
                onClick={() => setCollapsed(c => ({ ...c, [group]: !c[group] }))}
                className="w-full flex items-center justify-between px-4 py-1.5 text-[9px] font-black uppercase tracking-widest text-slate-400 hover:text-slate-600 transition-colors"
              >
                {group}
                <ChevronDown size={9} className={`transition-transform ${collapsed[group] ? '-rotate-90' : ''}`} />
              </button>
              {!collapsed[group] && items.map(s => (
                <button
                  key={s.id}
                  onClick={() => handleSelect(s.id)}
                  className={`w-full flex items-center gap-2.5 px-4 py-2 text-left transition-colors ${
                    active === s.id
                      ? 'bg-white border-r-2 text-slate-800 shadow-sm'
                      : 'text-slate-500 hover:bg-white/70 hover:text-slate-700'
                  }`}
                  style={{ borderRightColor: active === s.id ? s.accent : 'transparent' }}
                >
                  <s.Icon size={12} style={{ color: active === s.id ? s.accent : undefined }} className={active !== s.id ? 'text-slate-400' : ''} />
                  <span className="text-[11px] font-medium leading-tight">{s.label}</span>
                </button>
              ))}
            </div>
          ))}
        </div>

        {/* Footer */}
        <div className="p-3 border-t border-slate-100">
          <p className="text-slate-400 text-[9px] text-center">{SECTIONS.length} sections · Full Manual</p>
        </div>
      </div>

      {/* ── Content ──────────────────────────────────────────────────── */}
      <div ref={contentRef} className="flex-1 overflow-y-auto">
        {/* Header bar */}
        <div className="sticky top-0 z-10 bg-white/95 backdrop-blur-sm border-b border-slate-100 px-8 py-3 flex items-center justify-between">
          <div className="flex items-center gap-2">
            {activeSection && (
              <>
                <div className="w-5 h-5 rounded-lg flex items-center justify-center" style={{ backgroundColor: activeSection.accent + '18' }}>
                  <activeSection.Icon size={11} style={{ color: activeSection.accent }} />
                </div>
                <span className="text-slate-500 text-[11px]">{activeSection.group}</span>
                <ChevronRight size={10} className="text-slate-300" />
                <span className="text-slate-800 text-[11px] font-semibold">{activeSection.label}</span>
              </>
            )}
          </div>
          <div className="flex items-center gap-1.5">
            <span className="text-slate-300 text-[10px]">{currentIdx + 1} / {SECTIONS.length}</span>
          </div>
        </div>

        {/* Section content */}
        <div className="px-8 py-6 max-w-2xl">
          {SectionComp && <SectionComp onNavigate={onNavigate} />}

          {/* Prev / Next navigation */}
          <div className="flex items-center justify-between mt-8 pt-6 border-t border-slate-100">
            {prevSection ? (
              <button onClick={() => handleSelect(prevSection.id)} className="flex items-center gap-2 text-slate-400 hover:text-slate-700 transition-colors">
                <ChevronRight size={13} className="rotate-180" />
                <div className="text-left">
                  <p className="text-[9px] uppercase tracking-wider text-slate-300">Previous</p>
                  <p className="text-[11px] font-semibold">{prevSection.label}</p>
                </div>
              </button>
            ) : <div />}
            {nextSection ? (
              <button onClick={() => handleSelect(nextSection.id)} className="flex items-center gap-2 text-slate-400 hover:text-slate-700 transition-colors">
                <div className="text-right">
                  <p className="text-[9px] uppercase tracking-wider text-slate-300">Next</p>
                  <p className="text-[11px] font-semibold">{nextSection.label}</p>
                </div>
                <ChevronRight size={13} />
              </button>
            ) : <div />}
          </div>
        </div>
      </div>
    </div>
  )
}
