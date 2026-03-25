import { Upload, TrendingUp, TrendingDown, BarChart2, Activity } from 'lucide-react'

export default function SummaryBar({ summary }) {
  if (!summary) return null
  const { kpis_tracked, kpis_available, months_of_data, status_breakdown } = summary
  const sb = status_breakdown || {}

  // ── KPI Health score ──────────────────────────────────────────────────────
  const total      = (sb.green || 0) + (sb.yellow || 0) + (sb.red || 0)
  const healthPct  = total ? Math.round((sb.green || 0) / total * 100) : 0
  const healthColor  = healthPct >= 75 ? 'text-emerald-700' : healthPct >= 50 ? 'text-amber-700'  : 'text-red-700'
  const healthBg     = healthPct >= 75 ? 'bg-emerald-50'    : healthPct >= 50 ? 'bg-amber-50'     : 'bg-red-50'
  const healthBorder = healthPct >= 75 ? 'border-emerald-200': healthPct >= 50 ? 'border-amber-200': 'border-red-200'

  const tiles = [
    { label: 'Months of Data',  value: months_of_data,                    color: 'text-[#0055A4]',  bg: 'bg-blue-50',    border: 'border-blue-200',     Icon: BarChart2    },
    { label: 'KPIs Tracked',    value: kpis_tracked,                        color: 'text-slate-700', bg: 'bg-slate-50',   border: 'border-slate-200',    Icon: TrendingUp   },
    { label: 'On Target',       value: sb.green  || 0,                    color: 'text-emerald-700', bg: 'bg-emerald-50', border: 'border-emerald-200',  Icon: TrendingUp   },
    { label: 'Needs Attention', value: sb.yellow || 0,                    color: 'text-amber-700',   bg: 'bg-amber-50',   border: 'border-amber-200',    Icon: TrendingDown },
    { label: 'Critical',        value: sb.red    || 0,                    color: 'text-red-700',     bg: 'bg-red-50',     border: 'border-red-200',      Icon: TrendingDown },
    { label: 'KPI Health',      value: `${healthPct}%`,                   color: healthColor,        bg: healthBg,        border: healthBorder,          Icon: Activity     },
  ]

  return (
    <div className="flex flex-wrap gap-3 mb-5">
      {tiles.map(({ label, value, color, bg, border, Icon }) => (
        <div key={label} className={`summary-tile flex items-center gap-3 min-w-[130px] ${bg} border ${border}`}>
          <div className={`w-8 h-8 rounded-lg ${bg} flex items-center justify-center flex-shrink-0`}>
            <Icon size={14} className={color}/>
          </div>
          <div>
            <div className={`text-xl font-bold leading-none ${color}`}>{value}</div>
            <div className="text-xs text-slate-400 mt-0.5">{label}</div>
          </div>
        </div>
      ))}
    </div>
  )
}
