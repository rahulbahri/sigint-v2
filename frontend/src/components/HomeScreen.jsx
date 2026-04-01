import { useState, useEffect } from 'react'
import axios from 'axios'
import {
  TrendingUp, TrendingDown, Minus, AlertTriangle,
  CheckCircle2, Zap, ArrowRight, RefreshCw,
  Activity, Target, Shield, BarChart2
} from 'lucide-react'

// ── Circular Health Gauge ─────────────────────────────────────────────────────
function HealthGauge({ score, color, size = 148 }) {
  const r = (size / 2) - 14
  const circ = 2 * Math.PI * r
  const progress = Math.max(0, Math.min(score, 100)) / 100 * circ
  const strokeColor = color === 'green' ? '#059669' : color === 'amber' ? '#D97706' : '#DC2626'

  return (
    <svg width={size} height={size} className="transform -rotate-90 drop-shadow-sm">
      <circle cx={size/2} cy={size/2} r={r} fill="none" stroke="#DDE5F4" strokeWidth="11"/>
      <circle
        cx={size/2} cy={size/2} r={r}
        fill="none" stroke={strokeColor} strokeWidth="11" strokeLinecap="round"
        strokeDasharray={`${progress} ${circ}`}
        style={{ transition: 'stroke-dasharray 1.2s ease-in-out' }}
      />
    </svg>
  )
}

// ── Mini sparkline ─────────────────────────────────────────────────────────────
function Sparkline({ data, color = '#059669', width = 72, height = 28 }) {
  if (!data || data.length < 2) return <div style={{ width, height }} />
  const nums = data.filter(v => v != null)
  if (nums.length < 2) return <div style={{ width, height }} />
  const min = Math.min(...nums)
  const max = Math.max(...nums)
  const range = max - min || 1
  const pts = nums.map((v, i) => {
    const x = (i / (nums.length - 1)) * width
    const y = height - 2 - ((v - min) / range) * (height - 6)
    return `${x},${y}`
  }).join(' ')
  return (
    <svg width={width} height={height} className="flex-shrink-0">
      <polyline points={pts} fill="none" stroke={color} strokeWidth="1.75"
        strokeLinecap="round" strokeLinejoin="round"/>
    </svg>
  )
}

// ── KPI Spotlight Card ─────────────────────────────────────────────────────────
function KpiCard({ kpi, status, onAskAnika }) {
  const s = {
    red:   { dot: '#DC2626', bg: 'bg-red-50',    border: 'border-red-200',    text: 'text-red-700'    },
    amber: { dot: '#D97706', bg: 'bg-amber-50',  border: 'border-amber-200',  text: 'text-amber-700'  },
    green: { dot: '#059669', bg: 'bg-emerald-50',border: 'border-emerald-200',text: 'text-emerald-700' },
  }[status] || { dot: '#94a3b8', bg: 'bg-slate-50', border: 'border-slate-200', text: 'text-slate-600' }

  const label = (kpi.key || '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
  const avg    = kpi.avg  != null ? `${kpi.avg}${kpi.unit || ''}` : '—'
  const target = kpi.target != null ? `${kpi.target}${kpi.unit || ''}` : '—'
  const gapPct = (kpi.avg != null && kpi.target)
    ? (kpi.direction === 'higher'
        ? ((kpi.avg / kpi.target - 1) * 100).toFixed(1)
        : ((kpi.target / kpi.avg - 1) * 100).toFixed(1))
    : null
  const sparkColor = status === 'green' ? '#059669' : status === 'red' ? '#DC2626' : '#D97706'

  return (
    <div className={`card p-4 ${s.bg} ${s.border} kpi-card-${status === 'amber' ? 'yellow' : status}`}>
      <div className="flex items-start justify-between gap-2 mb-2">
        <p className="text-slate-800 text-sm font-semibold leading-tight">{label}</p>
        <Sparkline data={kpi.sparkline} color={sparkColor} />
      </div>
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-slate-900 text-lg font-extrabold">{avg}</span>
        <span className="text-slate-400 text-xs">vs {target}</span>
        {gapPct !== null && (
          <span className={`text-xs font-bold ${s.text}`}>
            {gapPct > 0 ? '+' : ''}{gapPct}%
          </span>
        )}
        {onAskAnika && (
          <button
            onClick={() => onAskAnika(`Why is ${label} ${status === 'red' ? 'below target' : 'performing well'}?`)}
            className="ml-auto p-1 rounded-md text-slate-300 hover:text-teal-500 hover:bg-teal-50 transition-colors"
            title="Ask Anika"
          >
            <Zap size={12} />
          </button>
        )}
      </div>
    </div>
  )
}

// ── Score component bar ────────────────────────────────────────────────────────
function ScoreBar({ label, value, weight, Icon }) {
  const color = value >= 70 ? '#059669' : value >= 50 ? '#D97706' : '#DC2626'
  const bgPct  = Math.round(value)
  return (
    <div className="flex items-center gap-3">
      <div className="w-6 h-6 rounded-lg bg-slate-100 flex items-center justify-center flex-shrink-0">
        <Icon size={12} className="text-slate-500" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center justify-between mb-1.5">
          <span className="text-slate-600 text-xs font-medium">{label}</span>
          <div className="flex items-center gap-1.5">
            <span className="text-slate-400 text-[10px]">{weight}</span>
            <span className="text-slate-800 text-xs font-bold" style={{ color }}>{value.toFixed(0)}</span>
          </div>
        </div>
        <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-1000"
            style={{ width: `${bgPct}%`, backgroundColor: color }}
          />
        </div>
      </div>
    </div>
  )
}

// ── Main component ─────────────────────────────────────────────────────────────
export default function HomeScreen({ onNavigate, onAskAnika }) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(false)

  useEffect(() => {
    let alive = true
    setLoading(true)
    axios.get('/api/home')
      .then(r  => { if (alive) { setData(r.data); setLoading(false) } })
      .catch(() => { if (alive) { setError(true);  setLoading(false) } })
    return () => { alive = false }
  }, [])

  if (loading) return (
    <div className="flex items-center justify-center h-64">
      <div className="w-8 h-8 rounded-full border-2 border-[#0055A4] border-t-transparent animate-spin"/>
    </div>
  )

  if (error || !data) return (
    <div className="flex flex-col items-center justify-center h-64 gap-3">
      <p className="text-slate-500">Unable to load home screen data.</p>
      <button onClick={() => { setError(false); setLoading(true); axios.get('/api/home').then(r => { setData(r.data); setLoading(false) }).catch(() => { setError(true); setLoading(false) }) }}
        className="text-sm text-slate-400 hover:text-slate-600 flex items-center gap-1.5">
        <RefreshCw size={13}/> Retry
      </button>
    </div>
  )

  const { health, needs_attention, doing_well } = data
  const score = health?.score ?? 0
  const color = health?.color ?? 'grey'
  const scoreHex = color === 'green' ? '#059669' : color === 'amber' ? '#D97706' : '#DC2626'

  const momentumConfig = {
    improving: { Icon: TrendingUp,   text: 'Improving',  style: 'text-emerald-600' },
    stable:    { Icon: Minus,        text: 'Stable',     style: 'text-slate-500'   },
    declining: { Icon: TrendingDown, text: 'Declining',  style: 'text-red-500'     },
  }
  const mc = momentumConfig[health?.momentum_trend] || momentumConfig.stable
  const MIcon = mc.Icon

  return (
    <div className="space-y-6 max-w-5xl">

      {/* ── Health Score + Components ──────────────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">

        {/* Gauge card */}
        <div className="lg:col-span-2 card p-6 flex flex-col items-center justify-center gap-3">
          <div className="relative">
            <HealthGauge score={score} color={color} size={148} />
            <div className="absolute inset-0 flex flex-col items-center justify-center">
              <span className="text-4xl font-extrabold text-slate-900 leading-none">{score}</span>
              <span className="text-slate-400 text-xs mt-0.5">/ 100</span>
            </div>
          </div>
          <div className="text-center">
            <p className="text-base font-bold" style={{ color: scoreHex }}>{health?.label}</p>
            <p className="text-slate-400 text-xs mt-0.5">Company Health Score</p>
          </div>
          <div className={`flex items-center gap-1.5 text-xs font-semibold ${mc.style}`}>
            <MIcon size={13}/> {mc.text} Momentum
          </div>
        </div>

        {/* Breakdown card */}
        <div className="lg:col-span-3 card p-6 space-y-5">
          <div>
            <p className="text-slate-400 text-[10px] font-bold uppercase tracking-widest mb-4">Score Breakdown</p>
            <div className="space-y-3.5">
              <ScoreBar label="Momentum"           value={health?.momentum ?? 0}           weight="30%" Icon={Activity}/>
              <ScoreBar label="Target Achievement" value={health?.target_achievement ?? 0} weight="40%" Icon={Target}  />
              <ScoreBar label="Risk Score"         value={health?.risk_flags ?? 0}         weight="30%" Icon={Shield}  />
            </div>
          </div>

          <div className="pt-4 border-t border-slate-100">
            <p className="text-slate-400 text-[10px] font-bold uppercase tracking-widest mb-3">KPI Distribution</p>
            <div className="grid grid-cols-4 gap-2">
              {[
                { count: health?.kpis_green,  label: 'On Target', color: '#059669' },
                { count: health?.kpis_yellow, label: 'Watch',     color: '#D97706' },
                { count: health?.kpis_red,    label: 'Critical',  color: '#DC2626' },
                { count: health?.kpis_grey,   label: 'No Target', color: '#94a3b8' },
              ].map(({ count, label, color: c }) => (
                <div key={label} className="text-center">
                  <div className="text-2xl font-extrabold" style={{ color: c }}>{count ?? 0}</div>
                  <div className="text-slate-400 text-[10px] mt-0.5 font-medium">{label}</div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* ── Needs Attention ───────────────────────────────────────────── */}
      {needs_attention?.length > 0 && (
        <div>
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <AlertTriangle size={14} className="text-red-500" />
              <h2 className="text-slate-700 text-sm font-bold uppercase tracking-wider">Needs Attention</h2>
              <span className="badge-red text-[10px] font-bold px-2 py-0.5 rounded-full">{needs_attention.length}</span>
            </div>
            <button onClick={() => onNavigate?.('variance')}
              className="text-xs text-slate-400 hover:text-[#0055A4] flex items-center gap-1 transition-colors font-medium">
              Full Analysis <ArrowRight size={11}/>
            </button>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
            {needs_attention.slice(0, 6).map(kpi => (
              <KpiCard key={kpi.key} kpi={kpi} status="red" onAskAnika={onAskAnika} />
            ))}
          </div>
        </div>
      )}

      {/* ── Doing Well ────────────────────────────────────────────────── */}
      {doing_well?.length > 0 && (
        <div>
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <CheckCircle2 size={14} className="text-emerald-500" />
              <h2 className="text-slate-700 text-sm font-bold uppercase tracking-wider">Doing Well</h2>
              <span className="badge-green text-[10px] font-bold px-2 py-0.5 rounded-full">{doing_well.length}</span>
            </div>
            <button onClick={() => onNavigate?.('board')}
              className="text-xs text-slate-400 hover:text-[#0055A4] flex items-center gap-1 transition-colors font-medium">
              Executive Brief <ArrowRight size={11}/>
            </button>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
            {doing_well.slice(0, 6).map(kpi => (
              <KpiCard key={kpi.key} kpi={kpi} status="green" onAskAnika={onAskAnika} />
            ))}
          </div>
        </div>
      )}

      {/* ── No data state ─────────────────────────────────────────────── */}
      {(!needs_attention?.length && !doing_well?.length) && (
        <div className="card p-10 flex flex-col items-center gap-4 text-center">
          <BarChart2 size={32} className="text-slate-300" />
          <div>
            <p className="text-slate-600 text-base font-semibold mb-1">No data yet</p>
            <p className="text-slate-400 text-sm">Upload your financial data or load demo data to see your health score.</p>
          </div>
          <div className="flex gap-3">
            <button onClick={() => onNavigate?.('upload')}
              className="px-4 py-2 border border-slate-300 rounded-lg text-slate-600 hover:border-slate-400 text-sm font-medium transition-colors">
              Upload Data
            </button>
          </div>
        </div>
      )}

      {/* ── Quick navigation ──────────────────────────────────────────── */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        {[
          { label: 'Variance Analysis',   tab: 'variance',    desc: 'KPI gap deep-dive',        accent: '#0055A4' },
          { label: 'Performance Heatmap', tab: 'fingerprint', desc: '12-month pattern view',    accent: '#7c3aed' },
          { label: 'Trend Explorer',      tab: 'trends',      desc: 'Historical trend lines',   accent: '#0891b2' },
          { label: 'Board Pack',          tab: 'board_pack',  desc: 'Download presentation',    accent: '#d97706' },
        ].map(({ label, tab, desc, accent }) => (
          <button key={tab} onClick={() => onNavigate?.(tab)}
            className="card text-left p-4 hover:shadow-md transition-shadow group">
            <div className="w-1.5 h-1.5 rounded-full mb-2" style={{ backgroundColor: accent }}/>
            <p className="text-slate-800 text-sm font-semibold group-hover:text-[#0055A4] transition-colors">{label}</p>
            <p className="text-slate-400 text-xs mt-0.5">{desc}</p>
          </button>
        ))}
      </div>

    </div>
  )
}
