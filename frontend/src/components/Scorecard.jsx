import { useState } from 'react'
import {
  TrendingUp, TrendingDown, Minus, AlertTriangle, Eye,
  CheckCircle2, ChevronRight, ChevronDown, ChevronUp, Zap
} from 'lucide-react'

const UNIT_FMT = {
  pct:    v => `${v?.toFixed(1)}%`,
  days:   v => `${v?.toFixed(1)}d`,
  months: v => `${v?.toFixed(1)}mo`,
  ratio:  v => `${v?.toFixed(2)}x`,
}
function fmt(val, unit) {
  if (val == null) return '—'
  return (UNIT_FMT[unit] || (v => v?.toFixed(2)))(val)
}

// ─── Quick action lookup ────────────────────────────────────────────────────
const QUICK_ACTIONS = {
  revenue_growth:         'Review pipeline velocity and accelerate close rate',
  arr_growth:             'Accelerate new ARR via pipeline conversion and expansion plays',
  gross_margin:           'Analyse COGS by product line; renegotiate key vendor contracts',
  operating_margin:       'Identify opex reduction opportunities across G&A and R&D',
  ebitda_margin:          'Review non-cash charges and opex efficiency initiatives',
  nrr:                    'Launch targeted expansion campaigns for key customer cohorts',
  churn_rate:             'Prioritise at-risk accounts and deploy win-back programmes',
  dso:                    'Tighten collection cycles; review payment terms with top accounts',
  cash_conv_cycle:        'Improve AR collections and review payables timing',
  burn_multiple:          'Review headcount efficiency and defer non-critical spend',
  opex_ratio:             'Identify top cost lines above budget; prioritise reduction',
  cac_payback:            'Optimise marketing channel mix for lower-CAC acquisition',
  sales_efficiency:       'Increase AE productivity; focus on high-yield deal segments',
  contribution_margin:    'Review variable cost structure and pricing by segment',
  revenue_quality:        'Shift revenue mix toward higher-margin recurring contracts',
  customer_concentration: 'Diversify customer base; accelerate SMB or new-segment growth',
  recurring_revenue:      'Convert non-recurring deals; focus on subscription upgrades',
  operating_leverage:     'Ensure revenue grows faster than fixed costs; review hiring plan',
}

// ─── TrendIcon ──────────────────────────────────────────────────────────────
function TrendIcon({ trend, direction }) {
  const goodUp   = direction !== 'lower'
  const isGoodTrend = (trend === 'up' && goodUp) || (trend === 'down' && !goodUp)
  if (trend === 'up')   return <TrendingUp  size={13} className={isGoodTrend ? 'text-emerald-500' : 'text-red-500'}/>
  if (trend === 'down') return <TrendingDown size={13} className={isGoodTrend ? 'text-emerald-500' : 'text-red-500'}/>
  return <Minus size={13} className="text-slate-400"/>
}

function vsTarget(val, target, direction) {
  if (!target || !val) return null
  const pct = ((val / target) * 100).toFixed(0)
  const good = direction === 'higher' ? val >= target : val <= target
  return { pct, good }
}

function SparkBar({ data, target, direction }) {
  const values = data.map(d => d.value).filter(v => v != null)
  if (!values.length) return null
  const min = Math.min(...values); const max = Math.max(...values); const range = max - min || 1
  return (
    <div className="flex items-end gap-0.5 h-5 mt-1">
      {values.map((v, i) => {
        const h = Math.max(3, ((v - min) / range) * 100)
        const ok = target == null ? true : direction === 'higher' ? v >= target * 0.92 : v <= target * 1.08
        return (
          <div key={i} style={{ height: `${h}%` }}
            className={`flex-1 rounded-sm ${ok ? 'bg-emerald-400/70' : 'bg-red-400/60'}`}/>
        )
      })}
    </div>
  )
}

// ─── KPICard ────────────────────────────────────────────────────────────────
function KPICard({ kpi, onKpiClick, periodLabel }) {
  const borderCls = {
    green:  'kpi-card-green',
    yellow: 'kpi-card-yellow',
    red:    'kpi-card-red',
    grey:   'kpi-card-grey',
  }[kpi.fy_status] || 'kpi-card-grey'

  const vs = vsTarget(kpi.avg, kpi.target, kpi.direction)

  const best  = kpi.monthly?.length ? (kpi.direction === 'higher'
    ? Math.max(...kpi.monthly.map(m => m.value)) : Math.min(...kpi.monthly.map(m => m.value))) : null
  const worst = kpi.monthly?.length ? (kpi.direction === 'higher'
    ? Math.min(...kpi.monthly.map(m => m.value)) : Math.max(...kpi.monthly.map(m => m.value))) : null

  // ── MoM change chip ──────────────────────────────────────────────────────
  const sortedMonths = [...(kpi.monthly ?? [])].sort((a, b) => a.period.localeCompare(b.period))
  const lastVal  = sortedMonths.at(-1)?.value
  const prevVal  = sortedMonths.at(-2)?.value
  const momDelta = (lastVal != null && prevVal != null) ? lastVal - prevVal : null
  const momSign  = momDelta > 0 ? '+' : ''
  const momColor = kpi.direction === 'lower'
    ? (momDelta <= 0 ? 'text-emerald-600 bg-emerald-50' : 'text-red-500 bg-red-50')
    : (momDelta >= 0 ? 'text-emerald-600 bg-emerald-50' : 'text-red-500 bg-red-50')

  // ── Target progress bar ──────────────────────────────────────────────────
  const progressPct = (kpi.target && kpi.avg != null)
    ? Math.min(100, kpi.direction === 'higher'
        ? (kpi.avg / kpi.target) * 100
        : (kpi.target / kpi.avg) * 100)
    : null
  const progressColor = kpi.fy_status === 'green'
    ? 'bg-emerald-400'
    : kpi.fy_status === 'yellow'
      ? 'bg-amber-400'
      : 'bg-red-400'

  return (
    <div className={`card ${borderCls} p-4 flex flex-col gap-2 cursor-pointer
                     hover:shadow-md hover:scale-[1.01] hover:border-[#0055A4]/30 transition-all group`}
         onClick={() => onKpiClick?.(kpi.key)}>
      {/* Name + trend + MoM chip + drill icon */}
      <div className="flex items-start justify-between gap-2">
        <p className="text-xs font-semibold text-slate-600 leading-snug">{kpi.name}</p>
        <div className="flex items-center gap-1 flex-shrink-0">
          <TrendIcon trend={kpi.trend} direction={kpi.direction}/>
          {momDelta != null && (
            <span className={`text-[9px] px-1 py-0.5 rounded font-medium ${momColor}`}>
              {momSign}{momDelta.toFixed(1)}{kpi.unit === 'pct' ? 'pp' : ''} MoM
            </span>
          )}
          <ChevronRight size={11} className="text-slate-300 group-hover:text-[#0055A4] transition-colors"/>
        </div>
      </div>

      {/* Value */}
      <div className="flex items-baseline gap-1.5">
        <span className="text-2xl font-bold text-slate-900 leading-none">{fmt(kpi.avg, kpi.unit)}</span>
        <span className="text-xs text-slate-400">{periodLabel || 'avg'} avg</span>
      </div>

      {/* Target row */}
      <div className="flex items-center justify-between text-xs">
        <span className="text-slate-400">Target: <span className="text-slate-600 font-medium">{fmt(kpi.target, kpi.unit)}</span></span>
        {vs && (
          <span className={`font-semibold ${vs.good ? 'text-emerald-600' : 'text-red-600'}`}>
            {vs.good ? '+' : ''}{(kpi.avg - kpi.target).toFixed(1)}{kpi.unit === 'pct' ? 'pp' : ''}
          </span>
        )}
      </div>

      {/* Progress bar */}
      {progressPct != null && (
        <div className="w-full">
          <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden">
            <div
              className={`h-full rounded-full transition-all ${progressColor}`}
              style={{ width: `${progressPct}%` }}
            />
          </div>
          <div className="flex justify-between text-[9px] text-slate-400 mt-0.5">
            <span>0</span>
            <span className={
              kpi.fy_status === 'green' ? 'text-emerald-600' :
              kpi.fy_status === 'yellow' ? 'text-amber-600' : 'text-red-500'
            }>
              {Math.round(progressPct)}% of target
            </span>
            <span>100%</span>
          </div>
        </div>
      )}

      {/* Sparkbar */}
      {kpi.monthly?.length > 1 && (
        <SparkBar data={kpi.monthly} target={kpi.target} direction={kpi.direction}/>
      )}

      {/* Best / Worst */}
      <div className="flex justify-between text-[10px] text-slate-400 pt-0.5">
        <span>Best <span className="text-slate-600">{fmt(best, kpi.unit)}</span></span>
        <span>Worst <span className="text-slate-600">{fmt(worst, kpi.unit)}</span></span>
      </div>
    </div>
  )
}

// ─── Priority Actions Panel ─────────────────────────────────────────────────
function PriorityActionsPanel({ red, yellow, onKpiClick }) {
  const topKpis = [...red, ...yellow].slice(0, 3)
  if (!topKpis.length) return null

  return (
    <div className="card p-4 mb-6 border-l-4 border-l-orange-400 bg-orange-50/30">
      <div className="flex items-center gap-2 mb-3">
        <Zap size={14} className="text-orange-500"/>
        <span className="text-sm font-bold text-slate-800">Priority Actions</span>
        <span className="text-[10px] px-2 py-0.5 rounded-full bg-orange-100 text-orange-700 font-semibold">
          {topKpis.length} item{topKpis.length !== 1 ? 's' : ''}
        </span>
      </div>
      <div className="space-y-0">
        {topKpis.map((kpi, i) => {
          const action = QUICK_ACTIONS[kpi.key] ?? 'Review this KPI and identify improvement opportunities'
          const delta = kpi.target && kpi.avg != null
            ? `${fmt(kpi.avg, kpi.unit)} actual vs ${fmt(kpi.target, kpi.unit)} target`
            : null
          const badgeCls = kpi.fy_status === 'red'
            ? 'bg-red-100 text-red-700'
            : 'bg-amber-100 text-amber-700'

          return (
            <div key={kpi.key}
              className={`flex items-center justify-between gap-3 py-2.5 ${
                i < topKpis.length - 1 ? 'border-b border-orange-100' : ''
              }`}>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-0.5">
                  <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-semibold uppercase tracking-wide ${badgeCls}`}>
                    {kpi.fy_status}
                  </span>
                  <span className="text-xs font-semibold text-slate-700 truncate">{kpi.name}</span>
                </div>
                {delta && (
                  <p className="text-[10px] text-slate-400 mb-0.5">{delta}</p>
                )}
                <p className="text-[10px] text-slate-500 leading-snug truncate">{action}</p>
              </div>
              <button
                onClick={() => onKpiClick?.(kpi.key)}
                className="flex-shrink-0 text-[10px] text-[#0055A4] border border-[#0055A4]/30
                           bg-[#0055A4]/5 hover:bg-[#0055A4]/15 px-2.5 py-1 rounded-lg
                           font-medium transition-colors whitespace-nowrap">
                Review →
              </button>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ─── Section config ──────────────────────────────────────────────────────────
const SECTION_CONFIG = {
  red: {
    label:       'Critical — Immediate Action Required',
    sublabel:    'These KPIs are significantly off target and need urgent attention.',
    Icon:        AlertTriangle,
    headerColor: 'border-red-500',
    iconColor:   'text-red-500',
    titleColor:  'text-red-700',
    badgeCls:    'bg-red-100 text-red-700',
    bg:          'bg-red-50/60',
    defaultOpen: true,
  },
  yellow: {
    label:       'Needs Attention — Monitor Closely',
    sublabel:    'These KPIs are trending below target and require proactive management.',
    Icon:        Eye,
    headerColor: 'border-amber-400',
    iconColor:   'text-amber-500',
    titleColor:  'text-amber-700',
    badgeCls:    'bg-amber-100 text-amber-700',
    bg:          'bg-amber-50/50',
    defaultOpen: true,
  },
  green: {
    label:       'On Target — Performing Well',
    sublabel:    'These KPIs are at or above target. Continue monitoring for sustainability.',
    Icon:        CheckCircle2,
    headerColor: 'border-emerald-500',
    iconColor:   'text-emerald-500',
    titleColor:  'text-emerald-700',
    badgeCls:    'bg-emerald-100 text-emerald-700',
    bg:          'bg-emerald-50/30',
    defaultOpen: false,
  },
  grey: {
    label:       'No Target Set',
    sublabel:    '',
    Icon:        Minus,
    headerColor: 'border-slate-300',
    iconColor:   'text-slate-400',
    titleColor:  'text-slate-500',
    badgeCls:    'bg-slate-100 text-slate-500',
    bg:          '',
    defaultOpen: false,
  },
}

// ─── Section ─────────────────────────────────────────────────────────────────
function Section({ status, kpis, onKpiClick, periodLabel }) {
  const cfg = SECTION_CONFIG[status]
  const [open, setOpen] = useState(cfg.defaultOpen)

  if (!kpis.length) return null

  const { label, sublabel, Icon, headerColor, iconColor, titleColor, badgeCls, bg } = cfg

  return (
    <div className={`rounded-2xl border border-slate-200 overflow-hidden mb-6 ${bg}`}>
      {/* Section header — clickable to collapse */}
      <div
        className={`flex items-center gap-3 px-5 py-4 border-b-2 ${headerColor} bg-white/70 cursor-pointer select-none`}
        onClick={() => setOpen(v => !v)}
      >
        <Icon size={18} className={iconColor}/>
        <div className="flex-1 min-w-0">
          <h2 className={`text-sm font-bold ${titleColor}`}>{label}</h2>
          {open
            ? sublabel && <p className="text-xs text-slate-500 mt-0.5">{sublabel}</p>
            : <p className="text-xs text-slate-400 mt-0.5">Click to expand · {kpis.length} KPI{kpis.length !== 1 ? 's' : ''}</p>
          }
        </div>
        <span className={`px-2.5 py-1 rounded-full text-xs font-bold ${badgeCls}`}>
          {kpis.length} KPI{kpis.length !== 1 ? 's' : ''}
        </span>
        {open
          ? <ChevronUp size={15} className="text-slate-400 flex-shrink-0"/>
          : <ChevronDown size={15} className="text-slate-400 flex-shrink-0"/>
        }
      </div>

      {/* KPI grid — only when expanded */}
      {open && (
        <div className="p-4 grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-3">
          {kpis.map(kpi => <KPICard key={kpi.key} kpi={kpi} onKpiClick={onKpiClick} periodLabel={periodLabel}/>)}
        </div>
      )}
    </div>
  )
}

// ─── Board View VC Lens ───────────────────────────────────────────────────────
const BOARD_KPIS = ['revenue_growth', 'arr_growth', 'gross_margin', 'burn_multiple', 'nrr']

function BoardKPICard({ kpi, onKpiClick, periodLabel }) {
  if (!kpi) return null
  const statusColor = {
    green:  { border: 'border-emerald-400', bg: 'bg-emerald-50/30', text: 'text-emerald-700', badge: 'badge-green' },
    yellow: { border: 'border-amber-400',   bg: 'bg-amber-50/30',   text: 'text-amber-700',   badge: 'badge-yellow' },
    red:    { border: 'border-red-400',     bg: 'bg-red-50/30',     text: 'text-red-700',     badge: 'badge-red' },
    grey:   { border: 'border-slate-200',   bg: '',                 text: 'text-slate-500',   badge: 'badge-grey' },
  }[kpi.fy_status] || { border: 'border-slate-200', bg: '', text: 'text-slate-500', badge: 'badge-grey' }

  const vs = vsTarget(kpi.avg, kpi.target, kpi.direction)
  const sortedMonths = [...(kpi.monthly ?? [])].sort((a, b) => a.period.localeCompare(b.period))
  const lastVal = sortedMonths.at(-1)?.value
  const prevVal = sortedMonths.at(-2)?.value
  const momDelta = (lastVal != null && prevVal != null) ? lastVal - prevVal : null
  const isGoodMom = kpi.direction === 'lower' ? momDelta <= 0 : momDelta >= 0

  return (
    <div
      className={`flex-1 min-w-[180px] card border-2 ${statusColor.border} ${statusColor.bg} p-5 cursor-pointer
                  hover:shadow-lg hover:scale-[1.02] transition-all group`}
      onClick={() => onKpiClick?.(kpi.key)}>
      <div className="flex items-start justify-between mb-2">
        <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide">{kpi.name}</p>
        <span className={`text-[10px] px-2 py-0.5 rounded-full font-bold flex-shrink-0 ${statusColor.badge}`}>
          {kpi.fy_status?.toUpperCase()}
        </span>
      </div>
      <div className="text-3xl font-bold text-slate-900 mb-1">{fmt(kpi.avg, kpi.unit)}</div>
      <div className="text-[10px] text-slate-400 mb-1">{periodLabel || 'Period'} avg</div>
      <div className="flex items-center justify-between text-xs mb-3">
        <span className="text-slate-400">Target: <span className="text-slate-600 font-medium">{fmt(kpi.target, kpi.unit)}</span></span>
        {vs && (
          <span className={`font-bold ${vs.good ? 'text-emerald-600' : 'text-red-500'}`}>
            {vs.good ? '▲' : '▼'} {Math.abs(kpi.avg - kpi.target).toFixed(1)}{kpi.unit === 'pct' ? 'pp' : ''}
          </span>
        )}
      </div>
      {momDelta != null && (
        <div className={`text-[11px] font-medium mb-2 ${isGoodMom ? 'text-emerald-600' : 'text-red-500'}`}>
          {momDelta > 0 ? '+' : ''}{momDelta.toFixed(1)}{kpi.unit === 'pct' ? 'pp' : ''} MoM
        </div>
      )}
      {kpi.monthly?.length > 1 && (
        <SparkBar data={kpi.monthly} target={kpi.target} direction={kpi.direction}/>
      )}
    </div>
  )
}

// ─── Scorecard ───────────────────────────────────────────────────────────────
export default function Scorecard({ fingerprint, onKpiClick, boardView, periodLabel }) {
  if (!fingerprint?.length) return null

  if (boardView) {
    const boardKpis = BOARD_KPIS.map(key => fingerprint.find(k => k.key === key)).filter(Boolean)
    const groups = { red: [], yellow: [], green: [], grey: [] }
    boardKpis.forEach(kpi => groups[kpi.fy_status || 'grey'].push(kpi))
    return (
      <div>
        <div className="mb-4 px-1 flex items-center justify-between">
          <div>
            <p className="text-xs text-slate-500 font-medium uppercase tracking-wider">
              VC / Board Lens — 5 Headline KPIs
            </p>
            <p className="text-xs text-slate-400 mt-0.5">Revenue Growth · ARR Growth · Gross Margin · Burn Multiple · NRR</p>
          </div>
          {periodLabel && (
            <span className="text-[11px] px-2.5 py-1 rounded-full bg-[#0055A4]/10 text-[#0055A4] font-semibold border border-[#0055A4]/20">
              📅 {periodLabel}
            </span>
          )}
        </div>
        <div className="flex flex-wrap gap-4 mb-6">
          {boardKpis.map(kpi => (
            <BoardKPICard key={kpi.key} kpi={kpi} onKpiClick={onKpiClick} periodLabel={periodLabel}/>
          ))}
        </div>
        <PriorityActionsPanel
          red={groups.red}
          yellow={groups.yellow}
          onKpiClick={onKpiClick}
        />
      </div>
    )
  }

  const groups = { red: [], yellow: [], green: [], grey: [] }
  fingerprint.forEach(kpi => groups[kpi.fy_status || 'grey'].push(kpi))

  return (
    <div>
      {periodLabel && (
        <div className="flex items-center gap-2 mb-4">
          <span className="text-[11px] px-2.5 py-1 rounded-full bg-[#0055A4]/10 text-[#0055A4] font-semibold border border-[#0055A4]/20">
            📅 Analysing: {periodLabel}
          </span>
          <span className="text-[11px] text-slate-400">
            Averages and status across {fingerprint[0]?.monthly?.length ? `${new Set(fingerprint.flatMap(k => k.monthly?.map(m => m.period) ?? [])).size} months` : 'selected period'}
          </span>
        </div>
      )}
      <PriorityActionsPanel
        red={groups.red}
        yellow={groups.yellow}
        onKpiClick={onKpiClick}
      />
      <Section status="red"    kpis={groups.red}    onKpiClick={onKpiClick} periodLabel={periodLabel}/>
      <Section status="yellow" kpis={groups.yellow} onKpiClick={onKpiClick} periodLabel={periodLabel}/>
      <Section status="green"  kpis={groups.green}  onKpiClick={onKpiClick} periodLabel={periodLabel}/>
      <Section status="grey"   kpis={groups.grey}   onKpiClick={onKpiClick} periodLabel={periodLabel}/>
    </div>
  )
}
