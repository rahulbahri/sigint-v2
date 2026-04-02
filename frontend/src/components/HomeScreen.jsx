import { useState, useEffect, useRef, useCallback } from 'react'
import axios from 'axios'
import {
  TrendingUp, TrendingDown, Minus, AlertTriangle,
  CheckCircle2, Zap, ArrowRight, RefreshCw,
  Activity, Target, Shield, BarChart2,
  X, ChevronRight, Info, Clock, Eye,
  ChevronDown, Sliders, RotateCcw, Save,
  Loader2, GitBranch, AlertCircle, Bookmark,
  ExternalLink, Calendar
} from 'lucide-react'

// ── KPI contextual info dictionary ───────────────────────────────────────────
const KPI_INFO = {
  revenue_growth: {
    what: 'Month-over-month or year-over-year percentage change in total revenue.',
    why:  'The primary indicator of business momentum — investors and boards use this as the headline growth signal.',
    how:  'Computed as (current period revenue − prior period revenue) / prior period revenue × 100.',
    tab:  'trends',
  },
  gross_margin: {
    what: 'Revenue minus cost of goods sold, expressed as a percentage of revenue.',
    why:  'Shows how efficiently you deliver your product. SaaS companies typically target 70–80%.',
    how:  'Computed as (Revenue − COGS) / Revenue × 100. Sourced from your monthly P&L upload.',
    tab:  'variance',
  },
  net_revenue_retention: {
    what: 'Percentage of recurring revenue retained from existing customers, including expansions.',
    why:  'NRR > 100% means your existing base grows on its own — the gold standard for SaaS.',
    how:  'Computed as (Starting MRR − Churn + Expansion) / Starting MRR × 100.',
    tab:  'variance',
  },
  logo_churn_rate: {
    what: 'Percentage of customers who cancelled in a given period.',
    why:  'Customer churn erodes your installed base and signals product-market fit issues.',
    how:  'Computed as customers lost / customers at start of period × 100.',
    tab:  'variance',
  },
  arr: {
    what: 'Annualised Recurring Revenue — the predictable, contracted annual revenue run-rate.',
    why:  'The canonical SaaS valuation metric. ARR growth drives your company multiple.',
    how:  'MRR × 12, or sum of all active annual contract values.',
    tab:  'trends',
  },
  mrr: {
    what: 'Monthly Recurring Revenue — contracted, predictable monthly revenue.',
    why:  'The pulse of the business. Month-on-month MRR growth shows compounding momentum.',
    how:  'Sum of all active subscription monthly fees at end of period.',
    tab:  'trends',
  },
  cac: {
    what: 'Customer Acquisition Cost — total sales & marketing spend divided by new customers won.',
    why:  'High CAC relative to LTV signals inefficient go-to-market and unsustainable unit economics.',
    how:  'Total S&M spend in period / number of new customers acquired in that period.',
    tab:  'variance',
  },
  ltv: {
    what: 'Lifetime Value — projected total revenue from an average customer over their entire relationship.',
    why:  'LTV:CAC ratio (target ≥ 3:1) is a core efficiency metric used by investors.',
    how:  'ARPU / Churn Rate (simplified), or ARPU × Gross Margin / Churn Rate.',
    tab:  'variance',
  },
  ltv_cac_ratio: {
    what: 'Ratio of customer lifetime value to acquisition cost.',
    why:  'Below 3x signals go-to-market inefficiency; above 5x may mean under-investment in growth.',
    how:  'LTV / CAC. Industry benchmark: 3–5× for healthy SaaS.',
    tab:  'variance',
  },
  payback_period: {
    what: 'Months required to recoup the cost of acquiring a customer.',
    why:  'Shorter payback means faster capital recycling. Best-in-class is under 12 months.',
    how:  'CAC / (ARPU × Gross Margin %). Sourced from your CAC and margin inputs.',
    tab:  'variance',
  },
  magic_number: {
    what: 'Sales efficiency metric: net new ARR generated per dollar of S&M spend.',
    why:  'Magic Number ≥ 0.75 indicates efficient growth; < 0.5 signals go-to-market issues.',
    how:  'Net New ARR / Prior Quarter S&M Spend.',
    tab:  'variance',
  },
  burn_multiple: {
    what: 'Net cash burned per dollar of net new ARR added.',
    why:  'Lower is better. > 2× is a warning sign; the best companies operate at < 1×.',
    how:  'Net Burn / Net New ARR. Sourced from your cash flow and ARR data.',
    tab:  'variance',
  },
  runway_months: {
    what: 'Months of operating runway at current burn rate.',
    why:  'Critical risk indicator. < 12 months means fundraising must begin immediately.',
    how:  'Cash Balance / Monthly Net Burn.',
    tab:  'variance',
  },
  cash_burn: {
    what: 'Net cash consumed per month (operating + investing activities).',
    why:  'Directly determines runway. Rising burn with flat ARR signals efficiency problems.',
    how:  'Cash at start of period − Cash at end of period (net of financing).',
    tab:  'trends',
  },
  ebitda_margin: {
    what: 'Earnings before interest, taxes, depreciation and amortisation as a % of revenue.',
    why:  'Proxy for operating profitability. Negative is acceptable early; trend toward 20%+ at scale.',
    how:  'EBITDA / Revenue × 100. From your uploaded P&L data.',
    tab:  'variance',
  },
  operating_margin: {
    what: 'Operating income as a percentage of revenue.',
    why:  'Shows the core business profitability before financing decisions.',
    how:  '(Revenue − OPEX − COGS) / Revenue × 100.',
    tab:  'variance',
  },
  dau_mau_ratio: {
    what: 'Daily Active Users divided by Monthly Active Users — product stickiness.',
    why:  'Measures how often users return. 20%+ is decent; 50%+ is excellent (WhatsApp-level).',
    how:  'DAU / MAU × 100. Sourced from your product analytics upload.',
    tab:  'trends',
  },
  nps: {
    what: 'Net Promoter Score — customer sentiment measured on a −100 to +100 scale.',
    why:  'Strong leading indicator of retention and referral-driven growth.',
    how:  '% Promoters (9–10) − % Detractors (0–6). From customer survey data.',
    tab:  'trends',
  },
  sales_cycle_days: {
    what: 'Average days from first contact to closed-won deal.',
    why:  'Longer cycles compress win rates and slow ARR growth. Benchmark varies by ACV.',
    how:  'Average of (close date − first touch date) across closed-won deals.',
    tab:  'variance',
  },
  win_rate: {
    what: 'Percentage of sales opportunities that result in a closed-won deal.',
    why:  'Direct measure of GTM effectiveness and product-market fit.',
    how:  'Closed-won deals / Total deals entering final stage × 100.',
    tab:  'variance',
  },
}

// ── Helper: format KPI key to label ──────────────────────────────────────────
function formatKpiLabel(key) {
  return (key || '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
}

// ── Circular Health Gauge (compact) ──────────────────────────────────────────
function HealthGauge({ score, color, size = 110 }) {
  const r = (size / 2) - 11
  const circ = 2 * Math.PI * r
  const progress = Math.max(0, Math.min(score, 100)) / 100 * circ
  const strokeColor = color === 'green' ? '#059669' : color === 'amber' ? '#D97706' : color === 'red' ? '#DC2626' : '#94a3b8'

  return (
    <svg width={size} height={size} className="transform -rotate-90 drop-shadow-sm flex-shrink-0">
      <circle cx={size/2} cy={size/2} r={r} fill="none" stroke="#E2E8F0" strokeWidth="9"/>
      <circle
        cx={size/2} cy={size/2} r={r}
        fill="none" stroke={strokeColor} strokeWidth="9" strokeLinecap="round"
        strokeDasharray={`${progress} ${circ}`}
        style={{ transition: 'stroke-dasharray 1.2s ease-in-out' }}
      />
    </svg>
  )
}

// ── Mini sparkline ────────────────────────────────────────────────────────────
function Sparkline({ data, color = '#059669', width = 64, height = 24 }) {
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
      <polyline points={pts} fill="none" stroke={color} strokeWidth="1.5"
        strokeLinecap="round" strokeLinejoin="round"/>
    </svg>
  )
}

// ── KPI Slide-out drawer (enriched) ──────────────────────────────────────────
function KpiSlideOut({ kpi, status, onClose, onNavigate }) {
  const info = KPI_INFO[kpi?.key] || {}
  const label = formatKpiLabel(kpi?.key)
  const avg    = kpi?.avg  != null ? `${kpi.avg}${kpi.unit || ''}` : '—'
  const target = kpi?.target != null ? `${kpi.target}${kpi.unit || ''}` : 'Not set'
  const sparkColor = status === 'green' ? '#059669' : status === 'red' ? '#DC2626' : '#D97706'
  const statusColors = {
    red:   { pill: 'bg-red-100 text-red-700',     label: 'Below Target' },
    amber: { pill: 'bg-amber-100 text-amber-700', label: 'Watch Zone'   },
    green: { pill: 'bg-emerald-100 text-emerald-700', label: 'On Target' },
  }
  const sc = statusColors[status] || { pill: 'bg-slate-100 text-slate-600', label: 'No Target' }

  const gapPct = (kpi?.avg != null && kpi?.target)
    ? (kpi.direction === 'higher'
        ? ((kpi.avg / kpi.target - 1) * 100).toFixed(1)
        : ((kpi.target / kpi.avg - 1) * 100).toFixed(1))
    : null

  const narrative = () => {
    if (!kpi?.avg || !kpi?.target) return `No target has been set for ${label}. Add a target in Settings to track performance.`
    if (status === 'green') return `${label} is performing at ${avg} against a target of ${target} — ${gapPct > 0 ? `${gapPct}% above target` : 'on track'}. This is a positive signal for your business health.`
    if (status === 'red') return `${label} is at ${avg}, which is ${Math.abs(gapPct)}% ${kpi.direction === 'higher' ? 'below' : 'above'} the target of ${target}. This is dragging down your health score and needs attention.`
    return `${label} is at ${avg} against a target of ${target} — within watch range. Monitor closely over the next 1–2 months.`
  }

  // Fetch enriched detail from API
  const [detail, setDetail] = useState(null)
  const [detailLoading, setDetailLoading] = useState(false)

  useEffect(() => {
    if (!kpi?.key) return
    setDetailLoading(true)
    axios.get(`/api/kpi-detail/${kpi.key}`)
      .then(r => setDetail(r.data))
      .catch(() => setDetail(null))
      .finally(() => setDetailLoading(false))
  }, [kpi?.key])

  return (
    <div className="fixed inset-0 z-50 flex justify-end" onClick={onClose}>
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/20 backdrop-blur-[1px]" />
      {/* Drawer */}
      <div
        className="relative bg-white w-[420px] h-full shadow-2xl flex flex-col overflow-hidden"
        style={{ animation: 'slideInRight 0.22s ease-out' }}
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-100">
          <div className="flex items-center gap-2">
            <span className={`text-[11px] font-semibold px-2 py-0.5 rounded-full ${sc.pill}`}>{sc.label}</span>
            <span className="text-slate-700 text-sm font-bold">{label}</span>
          </div>
          <button onClick={onClose} className="p-1 rounded-md hover:bg-slate-100 text-slate-400 hover:text-slate-600 transition-colors">
            <X size={15} />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-5">
          {/* Value + Sparkline */}
          <div className="flex items-end justify-between">
            <div>
              <div className="text-3xl font-extrabold text-slate-900">{avg}</div>
              <div className="text-slate-400 text-[11px] mt-0.5">6-month avg vs target: <span className="font-semibold text-slate-600">{target}</span></div>
              {gapPct !== null && (
                <div className={`text-xs font-bold mt-1 ${status === 'green' ? 'text-emerald-600' : status === 'red' ? 'text-red-600' : 'text-amber-600'}`}>
                  {gapPct > 0 ? '+' : ''}{gapPct}% vs target
                </div>
              )}
            </div>
            <Sparkline data={kpi?.sparkline} color={sparkColor} width={96} height={40} />
          </div>

          {/* Narrative */}
          <div className="bg-slate-50 rounded-xl p-4">
            <p className="text-slate-700 text-[12px] leading-relaxed">{narrative()}</p>
          </div>

          {/* What / Why / How */}
          {info.what && (
            <div className="space-y-3">
              <div>
                <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1">What is this?</p>
                <p className="text-slate-600 text-[12px] leading-relaxed">{info.what}</p>
              </div>
              <div>
                <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1">Why it matters</p>
                <p className="text-slate-600 text-[12px] leading-relaxed">{info.why}</p>
              </div>
              <div>
                <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1">How it's computed</p>
                <p className="text-slate-600 text-[12px] leading-relaxed font-mono bg-slate-50 px-3 py-2 rounded-lg">{info.how}</p>
              </div>
            </div>
          )}
          {!info.what && (
            <p className="text-slate-400 text-[12px]">No additional context available for this KPI.</p>
          )}

          {/* ── Enriched detail sections (fetched from API) ───────────── */}
          {detailLoading && (
            <div className="flex items-center justify-center py-4">
              <Loader2 size={16} className="animate-spin text-slate-400" />
              <span className="text-slate-400 text-[11px] ml-2">Loading detail...</span>
            </div>
          )}

          {detail && !detailLoading && (
            <div className="space-y-4 border-t border-slate-100 pt-4">

              {/* Computation / Formula */}
              {detail.formula && (
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1">Computation</p>
                  <div className="bg-slate-900 text-emerald-400 text-[11px] font-mono px-3 py-2.5 rounded-lg leading-relaxed whitespace-pre-wrap">
                    {detail.formula}
                  </div>
                </div>
              )}

              {/* Your Data — last 6 months */}
              {detail.recent_data && detail.recent_data.length > 0 && (
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1.5">Your Data</p>
                  <div className="bg-slate-50 rounded-lg overflow-hidden">
                    <table className="w-full text-[11px]">
                      <thead>
                        <tr className="border-b border-slate-200">
                          <th className="text-left text-slate-500 font-semibold px-3 py-1.5">Period</th>
                          <th className="text-right text-slate-500 font-semibold px-3 py-1.5">Value</th>
                        </tr>
                      </thead>
                      <tbody>
                        {detail.recent_data.slice(0, 6).map((row, i) => (
                          <tr key={i} className={i % 2 === 0 ? '' : 'bg-white'}>
                            <td className="px-3 py-1.5 text-slate-600">{row.period || row.date || '—'}</td>
                            <td className="px-3 py-1.5 text-slate-800 font-semibold text-right">
                              {row.value != null ? `${row.value}${kpi.unit || ''}` : '—'}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* Root Causes */}
              {detail.root_causes && detail.root_causes.length > 0 && (
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1">Root Causes</p>
                  <ul className="space-y-1">
                    {detail.root_causes.map((cause, i) => (
                      <li key={i} className="flex items-start gap-2 text-[12px] text-slate-600 leading-relaxed">
                        <AlertCircle size={11} className="text-red-400 mt-0.5 flex-shrink-0" />
                        {cause}
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {/* Downstream Impact */}
              {detail.downstream_kpis && detail.downstream_kpis.length > 0 && (
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1.5">Downstream Impact</p>
                  <div className="flex flex-wrap gap-1.5">
                    {detail.downstream_kpis.map((dk, i) => {
                      const dkColor = dk.status === 'green' ? 'bg-emerald-100 text-emerald-700'
                        : dk.status === 'red' ? 'bg-red-100 text-red-700'
                        : dk.status === 'amber' ? 'bg-amber-100 text-amber-700'
                        : 'bg-slate-100 text-slate-600'
                      return (
                        <span key={i} className={`text-[10px] font-semibold px-2 py-0.5 rounded-full ${dkColor}`}>
                          {formatKpiLabel(dk.key || dk)}
                        </span>
                      )
                    })}
                  </div>
                </div>
              )}

              {/* Recommended Actions */}
              {detail.recommended_actions && detail.recommended_actions.length > 0 && (
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1">Recommended Actions</p>
                  <ul className="space-y-1">
                    {detail.recommended_actions.map((action, i) => (
                      <li key={i} className="flex items-start gap-2 text-[12px] text-slate-600 leading-relaxed">
                        <CheckCircle2 size={11} className="text-emerald-500 mt-0.5 flex-shrink-0" />
                        {action}
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {/* Industry Benchmarks */}
              {detail.benchmarks && (
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1.5">Industry Benchmarks</p>
                  <div className="grid grid-cols-3 gap-2">
                    {[
                      { label: 'P25', value: detail.benchmarks.p25 },
                      { label: 'Median', value: detail.benchmarks.p50 },
                      { label: 'P75', value: detail.benchmarks.p75 },
                    ].map(({ label: bl, value: bv }) => (
                      <div key={bl} className="bg-slate-50 rounded-lg p-2 text-center">
                        <div className="text-[10px] text-slate-400 font-medium">{bl}</div>
                        <div className="text-[13px] font-bold text-slate-700">
                          {bv != null ? `${bv}${kpi.unit || ''}` : '—'}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        {info.tab && (
          <div className="px-5 py-4 border-t border-slate-100">
            <button
              onClick={() => { onNavigate?.(info.tab); onClose() }}
              className="w-full flex items-center justify-center gap-2 bg-[#0055A4] hover:bg-[#004688] text-white text-[12px] font-semibold py-2.5 rounded-xl transition-colors"
            >
              Open Full Analysis <ArrowRight size={13} />
            </button>
          </div>
        )}
      </div>
    </div>
  )
}

// ── Score Breakdown Modal (with adjustable weight sliders) ───────────────────
function ScoreBreakdownModal({ health, onClose }) {
  const score = health?.score ?? 0
  const color = health?.color ?? 'grey'
  const mom = health?.momentum ?? 0
  const tgt = health?.target_achievement ?? 0
  const rsk = health?.risk_flags ?? 0

  // Editable weights
  const [weights, setWeights] = useState({ momentum: 30, target: 40, risk: 30 })
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)

  // Compute preview score
  const previewScore = (
    mom * (weights.momentum / 100) +
    tgt * (weights.target / 100) +
    rsk * (weights.risk / 100)
  ).toFixed(1)

  // Redistribute weights when one slider changes
  const handleWeightChange = (key, newVal) => {
    const val = Math.max(0, Math.min(100, Number(newVal)))
    const otherKeys = Object.keys(weights).filter(k => k !== key)
    const remaining = 100 - val
    const otherTotal = otherKeys.reduce((s, k) => s + weights[k], 0)

    const newWeights = { ...weights, [key]: val }
    if (otherTotal === 0) {
      // Split evenly
      otherKeys.forEach(k => { newWeights[k] = Math.round(remaining / otherKeys.length) })
    } else {
      let allocated = 0
      otherKeys.forEach((k, i) => {
        if (i === otherKeys.length - 1) {
          newWeights[k] = remaining - allocated
        } else {
          const proportion = weights[k] / otherTotal
          const share = Math.round(remaining * proportion)
          newWeights[k] = share
          allocated += share
        }
      })
    }
    // Clamp negatives
    otherKeys.forEach(k => { if (newWeights[k] < 0) newWeights[k] = 0 })
    setWeights(newWeights)
    setSaved(false)
  }

  const resetDefaults = () => {
    setWeights({ momentum: 30, target: 40, risk: 30 })
    setSaved(false)
  }

  const saveWeights = async () => {
    setSaving(true)
    try {
      await axios.put('/api/company-settings', {
        weight_momentum: weights.momentum,
        weight_target: weights.target,
        weight_risk: weights.risk,
      })
      setSaved(true)
    } catch {}
    setSaving(false)
  }

  const narrative = () => {
    const parts = []
    if (mom >= 70) parts.push('strong momentum')
    else if (mom < 50) parts.push('weak momentum')
    if (tgt >= 70) parts.push('healthy target achievement')
    else if (tgt < 40) parts.push('low target achievement')
    if (rsk < 40) parts.push('elevated risk flags')
    return parts.length
      ? `Your score of ${score} reflects ${parts.join(' and ')}. ${tgt < 50 ? 'Set more KPI targets to unlock the Target Achievement score.' : ''}`
      : `Your overall health score of ${score} reflects the weighted combination of momentum, target achievement, and risk factors below.`
  }

  const components = [
    { key: 'momentum', label: 'Momentum', value: mom, Icon: Activity, desc: 'Measures how many KPIs are improving vs declining over the last 3 months.' },
    { key: 'target', label: 'Target Achievement', value: tgt, Icon: Target, desc: 'Percentage of KPIs with targets that are currently on track (green status).' },
    { key: 'risk', label: 'Risk Score', value: rsk, Icon: Shield, desc: 'Inverse of risk — penalises for KPIs in critical/red status and negative momentum.' },
  ]

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="absolute inset-0 bg-black/30 backdrop-blur-[2px]" />
      <div
        className="relative bg-white rounded-2xl shadow-2xl w-full max-w-lg p-6"
        onClick={e => e.stopPropagation()}
        style={{ animation: 'fadeInScale 0.18s ease-out' }}
      >
        <div className="flex items-center justify-between mb-4">
          <p className="text-slate-800 font-bold text-sm">How is the Health Score calculated?</p>
          <button onClick={onClose} className="p-1 rounded-md hover:bg-slate-100 text-slate-400"><X size={14}/></button>
        </div>
        <p className="text-slate-500 text-[12px] leading-relaxed mb-5">{narrative()}</p>

        <div className="space-y-4">
          {components.map(({ key, label, value, Icon, desc }) => {
            const c = value >= 70 ? '#059669' : value >= 50 ? '#D97706' : '#DC2626'
            return (
              <div key={key}>
                <div className="flex items-center gap-2 mb-1">
                  <Icon size={13} style={{ color: c }} />
                  <span className="text-slate-700 text-[12px] font-semibold flex-1">{label}</span>
                  <span className="text-[12px] font-bold" style={{ color: c }}>{value.toFixed(0)}</span>
                </div>
                <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden mb-1.5">
                  <div className="h-full rounded-full transition-all duration-700" style={{ width: `${value}%`, backgroundColor: c }} />
                </div>
                <p className="text-slate-400 text-[11px] leading-snug mb-2">{desc}</p>
                {/* Weight slider */}
                <div className="flex items-center gap-3 bg-slate-50 rounded-lg px-3 py-2">
                  <Sliders size={11} className="text-slate-400 flex-shrink-0" />
                  <span className="text-slate-500 text-[10px] font-medium w-12 flex-shrink-0">Weight:</span>
                  <input
                    type="range"
                    min={0}
                    max={100}
                    value={weights[key]}
                    onChange={e => handleWeightChange(key, e.target.value)}
                    className="flex-1 h-1.5 accent-[#0055A4] cursor-pointer"
                  />
                  <span className="text-[12px] font-bold text-[#0055A4] w-10 text-right">{weights[key]}%</span>
                </div>
              </div>
            )
          })}
        </div>

        {/* Preview score */}
        <div className="mt-5 bg-slate-50 rounded-xl p-3">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-slate-500 text-[11px]">
                <span className="font-semibold text-slate-600">Formula: </span>
                Score = (Momentum x {weights.momentum/100}) + (Target x {weights.target/100}) + (Risk x {weights.risk/100})
              </p>
              <p className="text-slate-400 text-[10px] mt-0.5">
                Weights must sum to 100%. Currently: {weights.momentum + weights.target + weights.risk}%
              </p>
            </div>
            <div className="text-right">
              <div className="text-[10px] text-slate-400 uppercase font-bold">Preview</div>
              <div className="text-xl font-extrabold text-slate-900">{previewScore}</div>
            </div>
          </div>
        </div>

        {/* Action buttons */}
        <div className="flex items-center gap-2 mt-4">
          <button
            onClick={resetDefaults}
            className="flex items-center gap-1.5 px-3 py-2 border border-slate-200 hover:border-slate-300 text-slate-600 text-[11px] font-semibold rounded-xl transition-colors"
          >
            <RotateCcw size={11} /> Reset to Default
          </button>
          <button
            onClick={saveWeights}
            disabled={saving}
            className="flex items-center gap-1.5 px-4 py-2 bg-[#0055A4] hover:bg-[#004688] text-white text-[11px] font-semibold rounded-xl transition-colors disabled:opacity-60 ml-auto"
          >
            {saving ? <Loader2 size={11} className="animate-spin" /> : <Save size={11} />}
            {saved ? 'Saved!' : 'Save Weights'}
          </button>
        </div>
      </div>
    </div>
  )
}

// ── Distribution Modal (with expandable KPI detail) ──────────────────────────
function DistributionModal({ health, onClose, onNavigate, onOpenKpi }) {
  const [expanded, setExpanded] = useState(null)

  const categories = [
    {
      key: 'green',
      count: health?.kpis_green,
      label: 'On Target',
      color: '#059669',
      bg: 'bg-emerald-50',
      text: 'text-emerald-700',
      border: 'border-emerald-200',
      desc: 'These KPIs are meeting or exceeding their targets. Keep monitoring for sustained performance.',
      kpis: health?.green_kpis_detail || [],
      fallbackList: [],
    },
    {
      key: 'yellow',
      count: health?.kpis_yellow,
      label: 'Watch',
      color: '#D97706',
      bg: 'bg-amber-50',
      text: 'text-amber-700',
      border: 'border-amber-200',
      desc: 'These KPIs are close to target but trending in the wrong direction. Early intervention recommended.',
      kpis: health?.yellow_kpis_detail || [],
      fallbackList: [],
    },
    {
      key: 'red',
      count: health?.kpis_red,
      label: 'Critical',
      color: '#DC2626',
      bg: 'bg-red-50',
      text: 'text-red-700',
      border: 'border-red-200',
      desc: 'These KPIs are significantly below target. Immediate review and action required.',
      kpis: health?.red_kpis_detail || [],
      fallbackList: [],
    },
    {
      key: 'grey',
      count: health?.kpis_grey,
      label: 'No Target',
      color: '#94a3b8',
      bg: 'bg-slate-50',
      text: 'text-slate-600',
      border: 'border-slate-200',
      desc: 'No target is set. Go to Settings -> Targets to configure benchmarks for accurate scoring.',
      kpis: (health?.grey_kpis_list || []).map(k => (typeof k === 'string' ? { key: k } : k)),
      fallbackList: [],
    },
  ]

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="absolute inset-0 bg-black/30 backdrop-blur-[2px]" />
      <div
        className="relative bg-white rounded-2xl shadow-2xl w-full max-w-lg p-6 max-h-[80vh] overflow-y-auto"
        onClick={e => e.stopPropagation()}
        style={{ animation: 'fadeInScale 0.18s ease-out' }}
      >
        <div className="flex items-center justify-between mb-5">
          <p className="text-slate-800 font-bold text-sm">KPI Distribution</p>
          <button onClick={onClose} className="p-1 rounded-md hover:bg-slate-100 text-slate-400"><X size={14}/></button>
        </div>
        <div className="space-y-3">
          {categories.map(({ key, count, label, color, bg, text, border, desc, kpis }) => (
            <div key={key}>
              <button
                onClick={() => setExpanded(expanded === key ? null : key)}
                className={`flex items-start gap-3 ${bg} rounded-xl p-3 w-full text-left transition-all border border-transparent hover:${border}`}
              >
                <div className="text-2xl font-extrabold flex-shrink-0 leading-none mt-0.5" style={{ color }}>{count ?? 0}</div>
                <div className="flex-1">
                  <div className="flex items-center gap-2">
                    <p className={`text-[12px] font-semibold ${text} mb-0.5`}>{label}</p>
                    {kpis.length > 0 && (
                      <ChevronDown
                        size={12}
                        className={`transition-transform ${expanded === key ? 'rotate-180' : ''} ${text}`}
                      />
                    )}
                  </div>
                  <p className="text-slate-500 text-[11px] leading-snug">{desc}</p>
                </div>
              </button>

              {/* Expanded KPI list */}
              {expanded === key && kpis.length > 0 && (
                <div className="mt-1 ml-9 space-y-1 mb-2">
                  {kpis.map((kpiItem, i) => {
                    const kpiKey = kpiItem.key || kpiItem
                    const kpiLabel = formatKpiLabel(kpiKey)
                    const perf = kpiItem.performance != null ? `${kpiItem.performance}%` : null
                    const kpiNarrative = kpiItem.narrative || null
                    const kpiInfo = KPI_INFO[kpiKey]
                    return (
                      <button
                        key={i}
                        onClick={() => {
                          onOpenKpi?.({ key: kpiKey, avg: kpiItem.avg, target: kpiItem.target, unit: kpiItem.unit, sparkline: kpiItem.sparkline, direction: kpiItem.direction }, key === 'grey' ? 'grey' : key === 'yellow' ? 'amber' : key)
                        }}
                        className="flex items-center gap-2 w-full text-left px-3 py-2 rounded-lg hover:bg-white/80 transition-colors group"
                      >
                        <div className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="text-[11px] font-semibold text-slate-700 truncate">{kpiLabel}</span>
                            {perf && (
                              <span className={`text-[10px] font-bold ${text}`}>{perf}</span>
                            )}
                          </div>
                          {kpiNarrative && (
                            <p className="text-slate-400 text-[10px] truncate leading-snug">{kpiNarrative}</p>
                          )}
                        </div>
                        <ChevronRight size={11} className="text-slate-300 group-hover:text-slate-500 flex-shrink-0" />
                      </button>
                    )
                  })}
                </div>
              )}
            </div>
          ))}
        </div>
        <button
          onClick={() => { onNavigate?.('variance'); onClose() }}
          className="mt-5 w-full flex items-center justify-center gap-2 border border-slate-200 hover:border-slate-300 text-slate-600 text-[12px] font-semibold py-2.5 rounded-xl transition-colors"
        >
          View Variance Analysis <ArrowRight size={12}/>
        </button>
      </div>
    </div>
  )
}

// ── KPI Card (compact, clickable) ─────────────────────────────────────────────
function KpiCard({ kpi, status, onOpen }) {
  const s = {
    red:   { dot: '#DC2626', bg: 'bg-red-50',    border: 'border-red-200',    text: 'text-red-700',     hover: 'hover:border-red-300'    },
    amber: { bg: 'bg-amber-50',  border: 'border-amber-200',  text: 'text-amber-700',   hover: 'hover:border-amber-300'  },
    green: { bg: 'bg-emerald-50',border: 'border-emerald-200',text: 'text-emerald-700', hover: 'hover:border-emerald-300' },
  }[status] || { bg: 'bg-slate-50', border: 'border-slate-200', text: 'text-slate-500', hover: 'hover:border-slate-300' }

  const label = formatKpiLabel(kpi.key)
  const avg    = kpi.avg  != null ? `${kpi.avg}${kpi.unit || ''}` : '—'
  const target = kpi.target != null ? `${kpi.target}${kpi.unit || ''}` : '—'
  const gapPct = (kpi.avg != null && kpi.target)
    ? (kpi.direction === 'higher'
        ? ((kpi.avg / kpi.target - 1) * 100).toFixed(1)
        : ((kpi.target / kpi.avg - 1) * 100).toFixed(1))
    : null
  const sparkColor = status === 'green' ? '#059669' : status === 'red' ? '#DC2626' : '#D97706'

  return (
    <button
      onClick={() => onOpen?.(kpi, status)}
      className={`w-full text-left card p-3.5 ${s.bg} ${s.border} ${s.hover} hover:shadow-md transition-all group cursor-pointer`}
    >
      <div className="flex items-start justify-between gap-2 mb-1.5">
        <p className="text-slate-800 text-[12px] font-semibold leading-tight flex-1">{label}</p>
        <div className="flex items-center gap-1 flex-shrink-0">
          <Sparkline data={kpi.sparkline} color={sparkColor} width={56} height={22} />
          <ChevronRight size={12} className="text-slate-300 group-hover:text-slate-500 transition-colors" />
        </div>
      </div>
      <div className="flex items-center gap-1.5 flex-wrap">
        <span className="text-slate-900 text-base font-extrabold leading-none">{avg}</span>
        <span className="text-slate-400 text-[10px]">vs {target}</span>
        {gapPct !== null && (
          <span className={`text-[11px] font-bold ${s.text}`}>
            {gapPct > 0 ? '+' : ''}{gapPct}%
          </span>
        )}
      </div>
    </button>
  )
}

// ── Score component bar (compact, clickable) ──────────────────────────────────
function ScoreBar({ label, value, weight, Icon, onClick }) {
  const color = value >= 70 ? '#059669' : value >= 50 ? '#D97706' : '#DC2626'
  return (
    <button className="flex items-center gap-2.5 w-full text-left group" onClick={onClick}>
      <div className="w-5 h-5 rounded-md bg-slate-100 flex items-center justify-center flex-shrink-0 group-hover:bg-slate-200 transition-colors">
        <Icon size={11} className="text-slate-500" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center justify-between mb-1">
          <span className="text-slate-600 text-[11px] font-medium group-hover:text-slate-800 transition-colors">{label}</span>
          <div className="flex items-center gap-1.5">
            <span className="text-slate-400 text-[10px]">{weight}</span>
            <span className="text-[11px] font-bold" style={{ color }}>{value.toFixed(0)}</span>
          </div>
        </div>
        <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden">
          <div className="h-full rounded-full transition-all duration-1000" style={{ width: `${Math.round(value)}%`, backgroundColor: color }} />
        </div>
      </div>
    </button>
  )
}

// ── Health narrative (enriched) ──────────────────────────────────────────────
function healthNarrative(health) {
  if (!health) return ''

  // If the API provides a detailed narrative, use it as a base
  if (health.narrative_detail) {
    return health.narrative_detail
  }

  const {
    score, color, momentum_trend,
    kpis_green = 0, kpis_yellow = 0, kpis_red = 0, kpis_grey = 0,
    target_achievement = 0, momentum = 0, risk_flags = 0,
    red_kpis_detail = [], yellow_kpis_detail = []
  } = health

  const total = kpis_green + kpis_yellow + kpis_red + kpis_grey
  const tracked = kpis_green + kpis_yellow + kpis_red

  // Zone label
  const zone =
    score >= 80 ? 'Excellent' :
    score >= 65 ? 'Good' :
    score >= 50 ? 'Watch' :
    score >= 35 ? 'Warning' :
                  'Critical'

  // Opening — what the score means
  const opener = `Your health score of ${score}/100 places your business in the ${zone} zone.`

  // Component analysis — which is the weakest
  const components = [
    { name: 'Momentum', value: momentum },
    { name: 'Target Achievement', value: target_achievement },
    { name: 'Risk', value: risk_flags },
  ].sort((a, b) => a.value - b.value)
  const weakest = components[0]
  const componentLine = weakest.value < 50
    ? `${weakest.name} is the biggest drag on your score at ${weakest.value.toFixed(0)}/100, while ${components[2].name} is your strongest component at ${components[2].value.toFixed(0)}/100.`
    : `All three score components (Momentum, Target Achievement, Risk) are reasonably balanced.`

  // Worst KPIs
  const worstKpis = red_kpis_detail.slice(0, 3)
  const worstLine = worstKpis.length > 0
    ? `The KPIs requiring most urgency are ${worstKpis.map(k => formatKpiLabel(k.key || k)).join(', ')}.`
    : kpis_yellow > 0
      ? `${kpis_yellow} KPI${kpis_yellow > 1 ? 's are' : ' is'} in the watch zone — monitor closely.`
      : null

  // Primary action
  const actionLine =
    kpis_red >= 3 ? `Focus this week on triaging the ${kpis_red} critical KPIs — start with the one with the largest gap to target.` :
    kpis_red > 0 ? `Start by investigating the ${kpis_red} critical KPI${kpis_red > 1 ? 's' : ''} flagged below.` :
    kpis_grey > 3 ? 'Set KPI targets in Settings to unlock an accurate health score.' :
    kpis_yellow > 0 ? `Watch the ${kpis_yellow} amber KPI${kpis_yellow > 1 ? 's' : ''} closely this month.` :
    'All tracked KPIs are on target — maintain the discipline.'

  return [opener, componentLine, worstLine, actionLine].filter(Boolean).join(' ')
}

// ── Format data period ────────────────────────────────────────────────────────
function formatPeriod(from, to) {
  if (!from && !to) return null
  if (from === to || !from) return to
  return `${from} – ${to}`
}

function formatUploadedAt(ts) {
  if (!ts) return null
  try {
    const d = new Date(ts)
    return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })
  } catch { return ts }
}

// ── Data Period Presets ──────────────────────────────────────────────────────
const PERIOD_PRESETS = [
  { label: 'Last 3 months', months: 3 },
  { label: 'Last 6 months', months: 6 },
  { label: 'Last 12 months', months: 12 },
  { label: 'YTD', months: -1 },   // special case
  { label: 'All Data', months: 0 },
]

function computePeriodParams(preset) {
  if (preset.months === 0) return {} // All data
  const now = new Date()
  const toYear = now.getFullYear()
  const toMonth = now.getMonth() + 1

  if (preset.months === -1) {
    // YTD
    return { from_year: toYear, from_month: 1, to_year: toYear, to_month: toMonth }
  }

  const fromDate = new Date(now.getFullYear(), now.getMonth() - preset.months, 1)
  return {
    from_year: fromDate.getFullYear(),
    from_month: fromDate.getMonth() + 1,
    to_year: toYear,
    to_month: toMonth,
  }
}

// ── Period Selector ─────────────────────────────────────────────────────────
function PeriodSelector({ selected, onSelect }) {
  const [open, setOpen] = useState(false)
  const ref = useRef(null)

  useEffect(() => {
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-1.5 px-2.5 py-1.5 border border-slate-200 hover:border-slate-300 rounded-lg text-[11px] text-slate-600 font-medium transition-colors bg-white"
      >
        <Calendar size={11} className="text-slate-400" />
        {selected}
        <ChevronDown size={10} className={`text-slate-400 transition-transform ${open ? 'rotate-180' : ''}`} />
      </button>
      {open && (
        <div className="absolute right-0 top-full mt-1 bg-white border border-slate-200 rounded-xl shadow-lg py-1 z-30 min-w-[140px]"
             style={{ animation: 'fadeInScale 0.12s ease-out' }}>
          {PERIOD_PRESETS.map(p => (
            <button
              key={p.label}
              onClick={() => { onSelect(p); setOpen(false) }}
              className={`w-full text-left px-3 py-1.5 text-[11px] transition-colors ${
                selected === p.label
                  ? 'bg-[#0055A4]/10 text-[#0055A4] font-semibold'
                  : 'text-slate-600 hover:bg-slate-50'
              }`}
            >
              {p.label}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Main component ────────────────────────────────────────────────────────────
export default function HomeScreen({ onNavigate, onAskAnika }) {
  const [data, setData]               = useState(null)
  const [loading, setLoading]         = useState(true)
  const [error, setError]             = useState(false)
  const [slideOut, setSlideOut]       = useState(null)
  const [showScoreModal, setShowScoreModal] = useState(false)
  const [showDistModal, setShowDistModal]   = useState(false)
  const [seeding, setSeeding]         = useState(false)
  const [showAllAttention, setShowAllAttention] = useState(false)
  const [showAllDoingWell, setShowAllDoingWell] = useState(false)
  const [selectedPeriod, setSelectedPeriod] = useState('All Data')

  const load = useCallback((periodParams = {}) => {
    setLoading(true); setError(false)
    const params = new URLSearchParams()
    if (periodParams.from_year) params.set('from_year', periodParams.from_year)
    if (periodParams.from_month) params.set('from_month', periodParams.from_month)
    if (periodParams.to_year) params.set('to_year', periodParams.to_year)
    if (periodParams.to_month) params.set('to_month', periodParams.to_month)
    const qs = params.toString()
    axios.get(`/api/home${qs ? `?${qs}` : ''}`)
      .then(r  => { setData(r.data); setLoading(false) })
      .catch(() => { setError(true); setLoading(false) })
  }, [])

  const loadDemoData = async () => {
    setSeeding(true)
    try {
      await axios.get('/api/seed-multiyear')
    } catch {}
    setSeeding(false)
    load()
  }

  const handlePeriodChange = (preset) => {
    setSelectedPeriod(preset.label)
    const params = computePeriodParams(preset)
    load(params)
  }

  useEffect(() => { load() }, [load])

  if (loading) return (
    <div className="flex items-center justify-center h-64">
      <div className="w-7 h-7 rounded-full border-2 border-[#0055A4] border-t-transparent animate-spin"/>
    </div>
  )

  if (error || !data) return (
    <div className="flex flex-col items-center justify-center h-64 gap-3">
      <p className="text-slate-500 text-sm">Unable to load home screen data.</p>
      <button onClick={() => load()} className="text-[12px] text-slate-400 hover:text-slate-600 flex items-center gap-1.5 transition-colors">
        <RefreshCw size={12}/> Retry
      </button>
    </div>
  )

  const { health, needs_attention, doing_well, data_period } = data
  const score    = health?.score ?? 0
  const color    = health?.color ?? 'grey'
  const scoreHex = color === 'green' ? '#059669' : color === 'amber' ? '#D97706' : color === 'red' ? '#DC2626' : '#94a3b8'

  const momentumConfig = {
    improving: { Icon: TrendingUp,   text: 'Improving',  style: 'text-emerald-600' },
    stable:    { Icon: Minus,        text: 'Stable',     style: 'text-slate-500'   },
    declining: { Icon: TrendingDown, text: 'Declining',  style: 'text-red-500'     },
  }
  const mc   = momentumConfig[health?.momentum_trend] || momentumConfig.stable
  const MIcon = mc.Icon

  const period   = formatPeriod(data_period?.from, data_period?.to)
  const uploadAt = formatUploadedAt(data_period?.uploaded_at)
  const narrative = healthNarrative(health)

  // Determine visible KPIs for show all / collapse
  const attentionVisible = showAllAttention ? needs_attention : needs_attention?.slice(0, 6)
  const doingWellVisible = showAllDoingWell ? doing_well : doing_well?.slice(0, 6)

  return (
    <div className="space-y-5 max-w-5xl">

      {/* ── Top meta bar ───────────────────────────────────────────────── */}
      <div className="flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-2 text-[11px] text-slate-400">
          {period && (
            <>
              <BarChart2 size={12} className="text-slate-300" />
              <span>Data period: <span className="font-semibold text-slate-500">{period}</span></span>
            </>
          )}
          {uploadAt && (
            <>
              <span className="text-slate-200">·</span>
              <Clock size={11} className="text-slate-300" />
              <span>Last uploaded: <span className="font-semibold text-slate-500">{uploadAt}</span></span>
            </>
          )}
          {!period && !uploadAt && (
            <span className="text-slate-400 italic">No data uploaded yet</span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <PeriodSelector selected={selectedPeriod} onSelect={handlePeriodChange} />
          <button
            onClick={loadDemoData}
            disabled={seeding}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-[#0055A4] hover:bg-[#003d80] text-white text-[11px] font-semibold rounded-lg transition-colors disabled:opacity-60"
          >
            {seeding
              ? <><div className="w-2.5 h-2.5 rounded-full border-2 border-white/40 border-t-white animate-spin"/>Loading...</>
              : <><Zap size={11}/> Load Demo Data</>
            }
          </button>
          <button onClick={() => load()} className="p-1.5 rounded-lg hover:bg-slate-100 text-slate-400 hover:text-slate-600 transition-colors" title="Refresh">
            <RefreshCw size={13} />
          </button>
        </div>
      </div>

      {/* ── Health Score Card ───────────────────────────────────────────── */}
      <div className="card p-5 shadow-sm hover:shadow-md transition-shadow">
        <div className="flex items-stretch gap-5">

          {/* Gauge column */}
          <div className="flex flex-col items-center justify-center flex-shrink-0 gap-2">
            <div className="relative">
              <HealthGauge score={score} color={color} size={110} />
              <div className="absolute inset-0 flex flex-col items-center justify-center">
                <span className="text-3xl font-extrabold text-slate-900 leading-none">{score}</span>
                <span className="text-slate-400 text-[10px]">/ 100</span>
              </div>
            </div>
            <div className="text-center">
              <p className="text-[12px] font-bold" style={{ color: scoreHex }}>{health?.label}</p>
              <p className="text-slate-400 text-[10px]">Health Score</p>
            </div>
            <div className={`flex items-center gap-1 text-[10px] font-semibold ${mc.style}`}>
              <MIcon size={11}/> {mc.text}
            </div>
          </div>

          {/* Divider */}
          <div className="w-px bg-slate-100 flex-shrink-0" />

          {/* Breakdown + Distribution column */}
          <div className="flex-1 min-w-0 flex flex-col justify-between gap-4">

            {/* Score breakdown */}
            <div>
              <div className="flex items-center gap-1.5 mb-2.5">
                <p className="text-slate-400 text-[10px] font-bold uppercase tracking-widest">Score Breakdown</p>
                <button onClick={() => setShowScoreModal(true)} className="text-slate-300 hover:text-slate-500 transition-colors" title="How is this calculated?">
                  <Info size={11} />
                </button>
                <span className="text-[9px] text-slate-300 ml-auto cursor-pointer hover:text-slate-400" onClick={() => setShowScoreModal(true)}>click to explain / adjust weights</span>
              </div>
              <div className="space-y-2.5">
                <ScoreBar label="Momentum"           value={health?.momentum ?? 0}           weight="30%" Icon={Activity} onClick={() => setShowScoreModal(true)}/>
                <ScoreBar label="Target Achievement" value={health?.target_achievement ?? 0} weight="40%" Icon={Target}   onClick={() => setShowScoreModal(true)}/>
                <ScoreBar label="Risk Score"         value={health?.risk_flags ?? 0}         weight="30%" Icon={Shield}   onClick={() => setShowScoreModal(true)}/>
              </div>
            </div>

            {/* KPI Distribution */}
            <div>
              <div className="flex items-center gap-1.5 mb-2">
                <p className="text-slate-400 text-[10px] font-bold uppercase tracking-widest">KPI Distribution</p>
                <button onClick={() => setShowDistModal(true)} className="text-slate-300 hover:text-slate-500 transition-colors">
                  <Info size={11} />
                </button>
                <span className="text-[9px] text-slate-300 ml-auto cursor-pointer hover:text-slate-400" onClick={() => setShowDistModal(true)}>click to explore</span>
              </div>
              <button onClick={() => setShowDistModal(true)} className="grid grid-cols-4 gap-2 w-full text-left hover:bg-slate-50 rounded-xl p-1.5 -mx-1.5 transition-colors group">
                {[
                  { count: health?.kpis_green,  label: 'On Target', color: '#059669' },
                  { count: health?.kpis_yellow, label: 'Watch',     color: '#D97706' },
                  { count: health?.kpis_red,    label: 'Critical',  color: '#DC2626' },
                  { count: health?.kpis_grey,   label: 'No Target', color: '#94a3b8' },
                ].map(({ count, label, color: c }) => (
                  <div key={label} className="text-center">
                    <div className="text-xl font-extrabold leading-tight" style={{ color: c }}>{count ?? 0}</div>
                    <div className="text-slate-400 text-[10px] font-medium leading-tight">{label}</div>
                  </div>
                ))}
              </button>
            </div>
          </div>

          {/* Right: narrative */}
          {narrative && (
            <>
              <div className="w-px bg-slate-100 flex-shrink-0 hidden lg:block" />
              <div className="hidden lg:flex flex-col justify-center max-w-[220px]">
                <p className="text-slate-500 text-[11px] leading-relaxed">{narrative}</p>
                <button
                  onClick={() => setShowScoreModal(true)}
                  className="mt-3 text-[10px] font-semibold text-[#0055A4] hover:underline self-start"
                >
                  How is this score computed?
                </button>
              </div>
            </>
          )}
        </div>
      </div>

      {/* ── Needs Attention ────────────────────────────────────────────── */}
      {needs_attention?.length > 0 && (
        <div>
          <div className="flex items-center justify-between mb-2.5">
            <div className="flex items-center gap-2">
              <AlertTriangle size={13} className="text-red-500" />
              <h2 className="text-slate-700 text-[11px] font-bold uppercase tracking-wider">Needs Attention</h2>
              <span className="bg-red-100 text-red-700 text-[10px] font-bold px-1.5 py-0.5 rounded-full">{needs_attention.length}</span>
              <span className="text-[9px] text-slate-300 italic">— click any card to explore</span>
            </div>
            <button onClick={() => onNavigate?.('variance')}
              className="text-[11px] text-slate-400 hover:text-[#0055A4] flex items-center gap-1 transition-colors font-medium">
              Full Analysis <ArrowRight size={10}/>
            </button>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-2.5">
            {attentionVisible.map(kpi => (
              <KpiCard key={kpi.key} kpi={kpi} status="red"
                onOpen={(k, s) => setSlideOut({ kpi: k, status: s })} />
            ))}
          </div>
          {needs_attention.length > 6 && (
            <button
              onClick={() => setShowAllAttention(!showAllAttention)}
              className="mt-2 text-[11px] text-slate-400 hover:text-[#0055A4] font-medium transition-colors flex items-center gap-1"
            >
              {showAllAttention
                ? <>Show less</>
                : <>Show all {needs_attention.length} <ArrowRight size={10}/></>
              }
            </button>
          )}
        </div>
      )}

      {/* ── Doing Well ─────────────────────────────────────────────────── */}
      {doing_well?.length > 0 && (
        <div>
          <div className="flex items-center justify-between mb-2.5">
            <div className="flex items-center gap-2">
              <CheckCircle2 size={13} className="text-emerald-500" />
              <h2 className="text-slate-700 text-[11px] font-bold uppercase tracking-wider">Doing Well</h2>
              <span className="bg-emerald-100 text-emerald-700 text-[10px] font-bold px-1.5 py-0.5 rounded-full">{doing_well.length}</span>
              <span className="text-[9px] text-slate-300 italic">— click any card to explore</span>
            </div>
            <button onClick={() => onNavigate?.('board')}
              className="text-[11px] text-slate-400 hover:text-[#0055A4] flex items-center gap-1 transition-colors font-medium">
              Executive Brief <ArrowRight size={10}/>
            </button>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-2.5">
            {doingWellVisible.map(kpi => (
              <KpiCard key={kpi.key} kpi={kpi} status="green"
                onOpen={(k, s) => setSlideOut({ kpi: k, status: s })} />
            ))}
          </div>
          {doing_well.length > 6 && (
            <button
              onClick={() => setShowAllDoingWell(!showAllDoingWell)}
              className="mt-2 text-[11px] text-slate-400 hover:text-[#0055A4] font-medium transition-colors flex items-center gap-1"
            >
              {showAllDoingWell
                ? <>Show less</>
                : <>Show all {doing_well.length} <ArrowRight size={10}/></>
              }
            </button>
          )}
        </div>
      )}

      {/* ── No colored KPIs: either no data or no targets set ────────────── */}
      {(!needs_attention?.length && !doing_well?.length) && (
        health?.kpis_grey > 0
          ? /* Has data but no targets */ (
            <div className="card p-6 border-l-4 border-amber-400 bg-amber-50/30">
              <div className="flex items-start gap-4">
                <Target size={22} className="text-amber-500 flex-shrink-0 mt-0.5" />
                <div className="flex-1">
                  <p className="text-slate-700 text-[13px] font-bold mb-1">
                    {health.kpis_grey} KPI{health.kpis_grey !== 1 ? 's have' : ' has'} no target set
                  </p>
                  <p className="text-slate-500 text-[12px] leading-relaxed mb-3">
                    Your data is loaded but KPI targets are not configured. Without targets, all KPIs show as grey
                    and the Needs Attention / Doing Well sections stay empty. Set targets to unlock red/green status,
                    the full health score, clickable KPI cards, and Slack alerts.
                  </p>
                  <div className="flex items-center gap-2 flex-wrap">
                    <button
                      onClick={() => onNavigate?.('targets')}
                      className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-amber-500 hover:bg-amber-600 text-white text-[11px] font-semibold rounded-lg transition-colors"
                    >
                      Configure KPI Targets <ArrowRight size={11}/>
                    </button>
                    <button
                      onClick={loadDemoData}
                      disabled={seeding}
                      className="inline-flex items-center gap-1.5 px-3 py-1.5 border border-amber-400 text-amber-700 hover:bg-amber-100 text-[11px] font-semibold rounded-lg transition-colors disabled:opacity-60"
                    >
                      {seeding ? 'Loading...' : 'Or: Load Demo Data with targets'}
                    </button>
                  </div>
                </div>
              </div>
            </div>
          )
          : /* No data at all */ (
            <div className="card p-8 flex flex-col items-center gap-4 text-center">
              <BarChart2 size={28} className="text-slate-300" />
              <div>
                <p className="text-slate-600 text-sm font-semibold mb-1">No data yet</p>
                <p className="text-slate-400 text-[12px] max-w-sm">Upload your financial data or load 5 years of demo data (including KPI targets) to explore the full platform.</p>
              </div>
              <div className="flex items-center gap-3">
                <button
                  onClick={loadDemoData}
                  disabled={seeding}
                  className="flex items-center gap-2 px-4 py-2 bg-[#0055A4] hover:bg-[#003d80] text-white text-[12px] font-semibold rounded-lg transition-colors disabled:opacity-60"
                >
                  {seeding
                    ? <><div className="w-3 h-3 rounded-full border-2 border-white/40 border-t-white animate-spin"/>Loading...</>
                    : <>Load Demo Data (5 years)</>
                  }
                </button>
                <button onClick={() => onNavigate?.('upload')}
                  className="px-4 py-2 border border-slate-300 rounded-lg text-slate-600 hover:border-slate-400 text-[12px] font-medium transition-colors">
                  Upload CSV
                </button>
              </div>
            </div>
          )
      )}

      {/* ── Quick navigation ────────────────────────────────────────────── */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2.5">
        {[
          { label: 'Variance Analysis',   tab: 'variance',    desc: 'KPI gap deep-dive',      accent: '#0055A4' },
          { label: 'Performance Heatmap', tab: 'fingerprint', desc: '12-month pattern view',  accent: '#7c3aed' },
          { label: 'Trend Explorer',      tab: 'trends',      desc: 'Historical trend lines', accent: '#0891b2' },
          { label: 'Board Pack',          tab: 'board_pack',  desc: 'Executive presentation', accent: '#d97706' },
        ].map(({ label, tab, desc, accent }) => (
          <button key={tab} onClick={() => onNavigate?.(tab)}
            className="card text-left p-3.5 hover:shadow-md transition-shadow group shadow-sm">
            <div className="w-1.5 h-1.5 rounded-full mb-1.5" style={{ backgroundColor: accent }}/>
            <p className="text-slate-800 text-[12px] font-semibold group-hover:text-[#0055A4] transition-colors leading-tight">{label}</p>
            <p className="text-slate-400 text-[10px] mt-0.5">{desc}</p>
          </button>
        ))}
      </div>

      {/* ── Modals & Drawers ────────────────────────────────────────────── */}
      {slideOut && (
        <KpiSlideOut
          kpi={slideOut.kpi}
          status={slideOut.status}
          onClose={() => setSlideOut(null)}
          onNavigate={onNavigate}
        />
      )}
      {showScoreModal && (
        <ScoreBreakdownModal health={health} onClose={() => setShowScoreModal(false)} />
      )}
      {showDistModal && (
        <DistributionModal
          health={health}
          onClose={() => setShowDistModal(false)}
          onNavigate={onNavigate}
          onOpenKpi={(kpi, status) => { setShowDistModal(false); setSlideOut({ kpi, status }) }}
        />
      )}

      {/* ── CSS animations ─────────────────────────────────────────────── */}
      <style>{`
        @keyframes slideInRight {
          from { transform: translateX(100%); opacity: 0; }
          to   { transform: translateX(0);    opacity: 1; }
        }
        @keyframes fadeInScale {
          from { transform: scale(0.95); opacity: 0; }
          to   { transform: scale(1);    opacity: 1; }
        }
      `}</style>
    </div>
  )
}
