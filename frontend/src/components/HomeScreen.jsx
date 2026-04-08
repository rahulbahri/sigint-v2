import { useState, useEffect, useRef, useCallback } from 'react'
import axios from 'axios'
import {
  TrendingUp, TrendingDown, Minus, AlertTriangle,
  CheckCircle2, ArrowRight, RefreshCw,
  Activity, Target, Shield, BarChart2,
  X, ChevronRight, Info, Clock, Eye,
  ChevronDown, Sliders, RotateCcw, Save,
  Loader2, AlertCircle, Calendar,
  Flame, Zap, FileText, MessageSquare
} from 'lucide-react'
import { fmtKpiValue, fmtKpiRange } from './kpiFormat'

// ── KPI contextual info dictionary ───────────────────────────────────────────
const KPI_INFO = {
  // ── Revenue & Growth ──────────────────────────────────────────────────────
  revenue_growth: {
    what: 'Month-over-month percentage change in total revenue.',
    why:  'The primary indicator of business momentum — investors and boards use this as the headline growth signal.',
    how:  'Computed as (Revenue_Month − Revenue_PrevMonth) / Revenue_PrevMonth × 100.',
    tab:  'trends',
  },
  arr_growth: {
    what: 'Month-over-month percentage change in Annualised Recurring Revenue.',
    why:  'ARR growth rate is the headline SaaS metric — it determines valuation multiples and fundraising trajectory.',
    how:  'Computed as (ARR_Month − ARR_PrevMonth) / ARR_PrevMonth × 100.',
    tab:  'trends',
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
  // ── Margins & Profitability ───────────────────────────────────────────────
  gross_margin: {
    what: 'Revenue minus cost of goods sold, expressed as a percentage of revenue.',
    why:  'Shows how efficiently you deliver your product. SaaS companies typically target 70-80%.',
    how:  'Computed as (Revenue − COGS) / Revenue × 100. Sourced from your monthly P&L upload.',
    tab:  'variance',
  },
  operating_margin: {
    what: 'Operating income as a percentage of revenue.',
    why:  'Shows the core business profitability before financing decisions.',
    how:  '(Revenue − OPEX − COGS) / Revenue × 100.',
    tab:  'variance',
  },
  ebitda_margin: {
    what: 'Earnings before interest, taxes, depreciation and amortisation as a % of revenue.',
    why:  'Proxy for operating profitability. Negative is acceptable early; trend toward 20%+ at scale.',
    how:  'EBITDA / Revenue × 100. From your uploaded P&L data.',
    tab:  'variance',
  },
  opex_ratio: {
    what: 'Total operating expenses as a percentage of revenue.',
    why:  'Tracks cost discipline. Decreasing opex ratio signals operating leverage kicking in.',
    how:  'OpEx / Revenue × 100.',
    tab:  'variance',
  },
  contribution_margin: {
    what: 'Revenue minus all variable costs as a percentage of revenue.',
    why:  'Shows per-unit profitability before fixed costs. Critical for unit economics analysis.',
    how:  '(Revenue − COGS − Variable Costs) / Revenue × 100.',
    tab:  'variance',
  },
  // ── Retention & Churn ─────────────────────────────────────────────────────
  nrr: {
    what: 'Percentage of recurring revenue retained from existing customers, including expansions.',
    why:  'NRR > 100% means your existing base grows on its own — the gold standard for SaaS.',
    how:  'Computed as (Starting MRR − Churn + Expansion) / Starting MRR × 100.',
    tab:  'variance',
  },
  churn_rate: {
    what: 'Percentage of customers who cancelled in a given period.',
    why:  'Customer churn erodes your installed base and signals product-market fit issues.',
    how:  'Computed as customers lost / customers at start of period × 100.',
    tab:  'variance',
  },
  logo_retention: {
    what: 'Percentage of customers retained from the prior period.',
    why:  'Logo retention tracks customer stickiness independent of revenue — losing many small accounts may not show in NRR.',
    how:  '(1 − Customers Lost / Customers at Start) × 100.',
    tab:  'variance',
  },
  contraction_rate: {
    what: 'Percentage of existing customer revenue lost to downgrades or reduced usage.',
    why:  'Contraction erodes NRR silently — customers stay but pay less.',
    how:  'Contraction Revenue / Prior Period Revenue from retained customers × 100.',
    tab:  'variance',
  },
  expansion_rate: {
    what: 'Percentage of existing customer revenue gained from upsells and cross-sells.',
    why:  'Expansion is the cheapest growth — no CAC required. Drives NRR above 100%.',
    how:  'Expansion Revenue / Prior Period Revenue from retained customers × 100.',
    tab:  'variance',
  },
  // ── Unit Economics ────────────────────────────────────────────────────────
  cac: {
    what: 'Customer Acquisition Cost — total sales & marketing spend divided by new customers won.',
    why:  'High CAC relative to LTV signals inefficient go-to-market and unsustainable unit economics.',
    how:  'Total S&M spend in period / number of new customers acquired in that period.',
    tab:  'variance',
  },
  customer_ltv: {
    what: 'Lifetime Value — projected total revenue from an average customer over their entire relationship.',
    why:  'LTV:CAC ratio (target >= 3:1) is a core efficiency metric used by investors.',
    how:  '(ARPU × Gross Margin) / Monthly Churn Rate.',
    tab:  'variance',
  },
  ltv_cac: {
    what: 'Ratio of customer lifetime value to acquisition cost.',
    why:  'Below 3x signals go-to-market inefficiency; above 5x may mean under-investment in growth.',
    how:  'LTV / CAC. Industry benchmark: 3-5x for healthy SaaS.',
    tab:  'variance',
  },
  cac_payback: {
    what: 'Months required to recoup the cost of acquiring a customer.',
    why:  'Shorter payback means faster capital recycling. Best-in-class is under 12 months.',
    how:  'CAC / (ARPU × Gross Margin %). Sourced from your CAC and margin inputs.',
    tab:  'variance',
  },
  payback_period: {
    what: 'Months required to recoup the cost of acquiring a customer.',
    why:  'Shorter payback means faster capital recycling. Best-in-class is under 12 months.',
    how:  'CAC / (ARPU × Gross Margin %). Sourced from your CAC and margin inputs.',
    tab:  'variance',
  },
  // ── Sales & GTM ───────────────────────────────────────────────────────────
  sales_efficiency: {
    what: 'Sales efficiency metric: net new ARR generated per dollar of S&M spend.',
    why:  'Sales Efficiency >= 0.75 indicates efficient growth; < 0.5 signals go-to-market issues.',
    how:  'Net New ARR / S&M Spend (annualised).',
    tab:  'variance',
  },
  pipeline_conversion: {
    what: 'Percentage of pipeline value that converts to closed-won revenue.',
    why:  'Low conversion signals deal qualification or competitive issues. Directly drives ARR growth.',
    how:  'Won Deal Value / Total Pipeline Value × 100.',
    tab:  'variance',
  },
  win_rate: {
    what: 'Percentage of sales opportunities that result in a closed-won deal.',
    why:  'Direct measure of GTM effectiveness and product-market fit.',
    how:  'Closed-won deals / Total deals entering final stage × 100.',
    tab:  'variance',
  },
  ramp_time: {
    what: 'Average days from first contact to closed-won deal.',
    why:  'Longer cycles compress win rates and slow ARR growth. Benchmark varies by ACV.',
    how:  'Average of (close date − first touch date) across closed-won deals.',
    tab:  'variance',
  },
  // ── Burn & Cash ───────────────────────────────────────────────────────────
  burn_multiple: {
    what: 'Net cash burned per dollar of net new ARR added.',
    why:  'Lower is better. > 2x is a warning sign; the best companies operate at < 1x.',
    how:  'Net Burn / Net New ARR. Sourced from your cash flow and ARR data.',
    tab:  'variance',
  },
  cash_runway: {
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
  // ── Derived & Composite ───────────────────────────────────────────────────
  growth_efficiency: {
    what: 'ARR Growth Rate divided by Burn Multiple — how efficiently growth is purchased.',
    why:  'Combines growth quality with capital efficiency. Higher means more growth per dollar burned.',
    how:  'ARR Growth Rate / |Burn Multiple|. Extreme values indicate near-zero burn multiple.',
    tab:  'variance',
  },
  pricing_power_index: {
    what: 'Difference between ARPU growth rate and customer volume growth rate.',
    why:  'Positive means price increases stick; negative means growth is discount-driven.',
    how:  '(ARPU % Change) − (Customer Volume % Change).',
    tab:  'variance',
  },
  customer_concentration: {
    what: 'Revenue share of the largest customer or HHI concentration index.',
    why:  'High concentration means revenue risk — losing one customer has outsized impact.',
    how:  'Top Customer Revenue / Total Revenue × 100 (or HHI index if customer data available).',
    tab:  'variance',
  },
  time_to_value: {
    what: 'Average days from signup to first meaningful product engagement.',
    why:  'Faster time-to-value reduces early churn and increases activation rates.',
    how:  'Average of (first value event date − signup date) across new users.',
    tab:  'variance',
  },
  // ── Receivables & Cash Cycle ──────────────────────────────────────────────
  dso: {
    what: 'Average days to collect payment after a sale.',
    why:  'Higher DSO ties up cash and increases working capital needs.',
    how:  '(Accounts Receivable / Revenue) × 30.',
    tab:  'variance',
  },
  // ── Product & Engagement ──────────────────────────────────────────────────
  dau_mau_ratio: {
    what: 'Daily Active Users divided by Monthly Active Users — product stickiness.',
    why:  'Measures how often users return. 20%+ is decent; 50%+ is excellent.',
    how:  'DAU / MAU × 100. Sourced from your product analytics upload.',
    tab:  'trends',
  },
  product_nps: {
    what: 'Net Promoter Score — customer sentiment measured on a -100 to +100 scale.',
    why:  'Strong leading indicator of retention and referral-driven growth.',
    how:  '% Promoters (9-10) − % Detractors (0-6). From customer survey data.',
    tab:  'trends',
  },
  recurring_revenue: {
    what: 'Percentage of total revenue that is recurring (subscription-based).',
    why:  'Higher recurring revenue means more predictable cash flows and higher valuation multiples.',
    how:  'Recurring Revenue / Total Revenue × 100.',
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

// ── KPI Slide-out drawer (enriched with navigation history + causal chain) ──
function KpiSlideOut({ kpi: initialKpi, status: initialStatus, onClose, onNavigate }) {
  // Navigation history for drilling into downstream KPIs
  const [history, setHistory] = useState([])
  const [currentKpi, setCurrentKpi] = useState(initialKpi)
  const [currentStatus, setCurrentStatus] = useState(initialStatus)

  // ── Hooks first (React rules: all hooks before any computed values) ─────
  // Fetch enriched detail from API
  const [detail, setDetail] = useState(null)
  const [detailLoading, setDetailLoading] = useState(false)

  useEffect(() => {
    if (!currentKpi?.key) return
    setDetail(null) // Clear stale detail when navigating to a new KPI
    setDetailLoading(true)
    axios.get(`/api/kpi-detail/${currentKpi.key}`)
      .then(r => {
        setDetail(r.data)
        // Override status from API's direction-aware computation (never trust the prop)
        if (r.data?.status) setCurrentStatus(r.data.status)
      })
      .catch(() => setDetail(null))
      .finally(() => setDetailLoading(false))
  }, [currentKpi?.key])

  // ── Computed display values ─────────────────────────────────────────────
  const info = KPI_INFO[currentKpi?.key] || {}
  const label = formatKpiLabel(currentKpi?.key)

  // Merge data: prefer detail API response, fall back to card-passed data.
  // When slideout opens from compact cards or navigation, currentKpi may only have {key}.
  // The detail API fetches full data — use it when available.
  // API field names: target (not target_value), no avg field (compute from time_series)
  const _rawAvg = (() => {
    // 1. Try card-passed avg
    if (currentKpi?.avg != null) return currentKpi.avg
    // 2. Compute from detail API time_series (last 6 months)
    if (detail?.time_series?.length) {
      const v = detail.time_series.slice(-6).map(p => p.value)
      return v.reduce((a, b) => a + b, 0) / v.length
    }
    return null
  })()
  const _rawTarget = detail?.target ?? currentKpi?.target ?? null
  const _direction = detail?.direction ?? currentKpi?.direction
  const _unit = detail?.unit ?? currentKpi?.unit ?? ''

  const avg    = fmtKpiValue(_rawAvg, _unit)
  const target = fmtKpiValue(_rawTarget, _unit)
  const sparkColor = currentStatus === 'green' ? '#059669' : currentStatus === 'red' ? '#DC2626' : '#D97706'
  const statusColors = {
    red:    { pill: 'bg-red-100 text-red-700',     label: 'Below Target' },
    amber:  { pill: 'bg-amber-100 text-amber-700', label: 'Watch Zone'   },
    yellow: { pill: 'bg-amber-100 text-amber-700', label: 'Watch Zone'   },
    green:  { pill: 'bg-emerald-100 text-emerald-700', label: 'On Target' },
    grey:   { pill: 'bg-slate-100 text-slate-600', label: 'No Target'   },
  }
  const sc = statusColors[currentStatus] || { pill: 'bg-slate-100 text-slate-600', label: 'No Target' }

  // Direction guidance — use merged direction
  const directionLabel = _direction === 'higher'
    ? { arrow: '\u2191', text: 'Higher is better', color: 'text-emerald-600' }
    : _direction === 'lower'
    ? { arrow: '\u2193', text: 'Lower is better', color: 'text-blue-600' }
    : null

  const isLowerBetter = _direction === 'lower'

  // Gap calculation: uses merged avg/target so it works even when card data was incomplete
  const gapPct = (() => {
    if (_rawAvg == null || _rawTarget == null || _rawTarget === 0) return null
    const a = _rawAvg, t = _rawTarget
    if (isLowerBetter) {
      return ((t - a) / Math.abs(t) * 100).toFixed(1)
    } else {
      return ((a / t - 1) * 100).toFixed(1)
    }
  })()

  // Determine if the KPI is actually performing well or poorly based on direction
  const isPerformingWell = (() => {
    if (_rawAvg == null || _rawTarget == null) return null
    if (isLowerBetter) return _rawAvg <= _rawTarget
    return _rawAvg >= _rawTarget
  })()

  const narrative = () => {
    if (_rawAvg == null || _rawTarget == null) return `No target has been set for ${label}. Add a target in Settings to track performance.`

    if (isPerformingWell) {
      const absDiff = Math.abs(gapPct)
      return `${label} is at ${avg} against a target of ${target}. The current value is ${absDiff}% better than the target — this KPI is on track.`
    } else {
      const absDiff = Math.abs(gapPct)
      if (isLowerBetter) {
        return `${label} is at ${avg}, which is ${absDiff}% above the target of ${target}. Since lower values are better for this metric, this KPI needs to come down.`
      } else {
        return `${label} is at ${avg}, which is ${absDiff}% below the target of ${target}. This KPI needs attention to improve.`
      }
    }
  }

  // Navigate to a downstream KPI
  const navigateToKpi = (dkKey, dkStatus) => {
    setHistory(prev => [...prev, { kpi: currentKpi, status: currentStatus }])
    setCurrentKpi({ key: dkKey })
    setCurrentStatus(dkStatus || 'grey')
  }

  // Go back in navigation history
  const goBack = () => {
    if (history.length === 0) return
    const prev = history[history.length - 1]
    setHistory(h => h.slice(0, -1))
    setCurrentKpi(prev.kpi)
    setCurrentStatus(prev.status)
  }

  // Close and clear history
  const handleClose = () => {
    setHistory([])
    onClose()
  }

  // Compute typical range from benchmarks if available
  const typicalRange = (() => {
    if (!detail?.benchmarks) return null
    const stages = Object.values(detail.benchmarks)
    if (stages.length === 0) return null
    const firstStage = stages[0]
    if (firstStage?.p25 != null && firstStage?.p75 != null) {
      return `${firstStage.p25}${currentKpi?.unit || ''} - ${firstStage.p75}${currentKpi?.unit || ''}`
    }
    return null
  })()

  return (
    <div className="fixed inset-0 z-50 flex justify-end" onClick={handleClose}>
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/20 backdrop-blur-[1px]" />
      {/* Drawer */}
      <div
        className="relative bg-white w-[420px] h-full shadow-2xl flex flex-col overflow-hidden"
        style={{ animation: 'slideInRight 0.22s ease-out' }}
        onClick={e => e.stopPropagation()}
      >
        {/* Back button (when navigating downstream) */}
        {history.length > 0 && (
          <button
            onClick={goBack}
            className="flex items-center gap-1.5 px-5 py-2 bg-slate-50 text-[11px] font-semibold text-[#0055A4] hover:bg-slate-100 transition-colors border-b border-slate-100"
          >
            <ArrowRight size={11} className="rotate-180" />
            Back to {formatKpiLabel(history[history.length - 1]?.kpi?.key)}
          </button>
        )}

        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-100">
          <div className="flex items-center gap-2 flex-wrap">
            <span className={`text-[11px] font-semibold px-2 py-0.5 rounded-full ${sc.pill}`}>{sc.label}</span>
            <span className="text-slate-700 text-sm font-bold">{label}</span>
          </div>
          <button onClick={handleClose} className="p-1 rounded-md hover:bg-slate-100 text-slate-400 hover:text-slate-600 transition-colors">
            <X size={15} />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-6">
          {/* Direction guidance + Typical range + Benchmark placeholder */}
          <div className="flex items-center gap-3 flex-wrap">
            {directionLabel && (
              <span className={`text-[11px] font-semibold ${directionLabel.color}`}>
                {directionLabel.arrow} {directionLabel.text}
              </span>
            )}
            {typicalRange && (
              <span className="text-[11px] text-slate-400">
                Typical range: <span className="font-semibold text-slate-500">{typicalRange}</span>
              </span>
            )}
            {!typicalRange && (
              <span className="text-[11px] text-slate-300">Industry Benchmark: —</span>
            )}
          </div>

          {/* Value + Sparkline */}
          <div className="flex items-end justify-between">
            <div>
              <div className="text-3xl font-extrabold text-slate-900">{avg}</div>
              <div className="text-slate-400 text-[11px] mt-0.5">6-month avg vs target: <span className="font-semibold text-slate-600">{target}</span></div>
              {gapPct !== null && (
                <div className={`text-xs font-bold mt-1 ${isPerformingWell ? 'text-emerald-600' : 'text-red-600'}`}>
                  {isPerformingWell ? `+${Math.abs(gapPct)}% vs target (${isLowerBetter ? 'below' : 'above'} — good)` : `-${Math.abs(gapPct)}% vs target (${isLowerBetter ? 'above' : 'below'} — needs work)`}
                </div>
              )}
            </div>
            <Sparkline data={currentKpi?.sparkline || detail?.time_series?.slice(-6).map(p => p.value)} color={sparkColor} width={96} height={40} />
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
          {!info.what && !detail && !detailLoading && (
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
              {detail.time_series && detail.time_series.length > 0 && (
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-0.5">Your Data</p>
                  <p className="text-[9px] text-slate-400 mb-1.5">Monthly actuals from your uploaded data (last 6 months shown)</p>
                  <div className="bg-slate-50 rounded-lg overflow-hidden">
                    <table className="w-full text-[11px]">
                      <thead>
                        <tr className="border-b border-slate-200">
                          <th className="text-left text-slate-500 font-semibold px-3 py-1.5">Period</th>
                          <th className="text-right text-slate-500 font-semibold px-3 py-1.5">Value</th>
                        </tr>
                      </thead>
                      <tbody>
                        {detail.time_series.slice(-6).map((row, i) => (
                          <tr key={i} className={i % 2 === 0 ? '' : 'bg-white'}>
                            <td className="px-3 py-1.5 text-slate-600">{row.period || row.date || '—'}</td>
                            <td className="px-3 py-1.5 text-slate-800 font-semibold text-right">
                              {fmtKpiValue(row.value, currentKpi?.unit)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* Causal Consistency (top-down + bottom-up validation) */}
              {currentKpi?.causal_validation?.flags?.length > 0 && (
                <div className="space-y-1.5">
                  <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1">Causal Validation</p>
                  {currentKpi.causal_validation.flags.map((flag, i) => {
                    const upstream = currentKpi.causal_validation.upstream_check?.verdict
                    const isWarning = upstream === 'orphan_issue' || upstream === 'lagging_indicator'
                    return (
                      <div key={i} className={`flex items-start gap-2 text-[11px] px-3 py-2 rounded-lg ${
                        isWarning ? 'bg-amber-50 text-amber-700 border border-amber-200' : 'bg-blue-50 text-blue-700 border border-blue-200'
                      }`}>
                        <Info size={11} className="mt-0.5 flex-shrink-0" />
                        <span>{flag}</span>
                      </div>
                    )
                  })}
                  {currentKpi.causal_validation.granger_confidence && (
                    <div className="flex items-center gap-2 text-[10px] text-slate-400">
                      <span className={`px-1.5 py-0.5 rounded text-[9px] font-medium ${
                        currentKpi.causal_validation.granger_confidence.confidence_label === 'high' ? 'bg-emerald-100 text-emerald-700' :
                        currentKpi.causal_validation.granger_confidence.confidence_label === 'moderate' ? 'bg-blue-100 text-blue-700' :
                        currentKpi.causal_validation.granger_confidence.confidence_label === 'low' ? 'bg-slate-100 text-slate-500' :
                        'bg-slate-50 text-slate-400'
                      }`}>
                        {currentKpi.causal_validation.granger_confidence.confidence_label === 'high' ? 'Statistically confirmed' :
                         currentKpi.causal_validation.granger_confidence.confidence_label === 'moderate' ? 'Partially confirmed' :
                         currentKpi.causal_validation.granger_confidence.confidence_label === 'low' ? 'Domain expertise only' :
                         'Unverified'}
                      </span>
                      <span>
                        {currentKpi.causal_validation.granger_confidence.confirmed_edges} confirmed,
                        {' '}{currentKpi.causal_validation.granger_confidence.expert_prior_edges} assumed causal edges
                      </span>
                    </div>
                  )}
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

              {/* Cause & Effect Chain */}
              {detail.causal_chain && (detail.causal_chain.children?.length > 0 || detail.downstream_impact?.length > 0) && (
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-2">Cause & Effect Chain</p>
                  <div className="bg-slate-50 rounded-xl p-3">
                    {/* Root node: current KPI */}
                    <div className="flex items-center gap-2 mb-1">
                      <div className={`text-[10px] font-bold px-2 py-0.5 rounded-full ${sc.pill}`}>
                        {label}
                      </div>
                      {detail.data_grounded && (
                        <span className="text-[8px] font-medium px-1.5 py-0.5 rounded-full bg-emerald-50 text-emerald-600 border border-emerald-200">
                          Data confirmed
                        </span>
                      )}
                      {!detail.data_grounded && (
                        <span className="text-[8px] font-medium px-1.5 py-0.5 rounded-full bg-slate-50 text-slate-400 border border-slate-200">
                          Expert hypothesis
                        </span>
                      )}
                      {detail.root_causes && detail.root_causes.length > 0 && (
                        <span className="text-[9px] text-slate-400">{detail.root_causes.slice(0, 2).map(r => r.replace('[Template] ', '')).join(', ')}</span>
                      )}
                    </div>
                    {/* Downstream tree */}
                    <div className="ml-2 border-l-2 border-slate-300 pl-3 space-y-1.5 mt-1">
                      {(detail.causal_chain.children || detail.downstream_impact || []).map((node, i) => {
                        const nodeKey = typeof node === 'string' ? node : (node.node || node.key || node)
                        const nodeLabel = formatKpiLabel(nodeKey)
                        const nodeStatus = typeof node === 'object' ? node.status : null
                        const nodeColor = nodeStatus === 'green' ? 'bg-emerald-100 text-emerald-700'
                          : nodeStatus === 'red' ? 'bg-red-100 text-red-700'
                          : (nodeStatus === 'amber' || nodeStatus === 'yellow') ? 'bg-amber-100 text-amber-700'
                          : 'bg-slate-100 text-slate-600'
                        const nodeCauses = typeof node === 'object' && node.root_causes ? node.root_causes : []
                        const nodeChildren = typeof node === 'object' && node.children ? node.children : []
                        return (
                          <div key={i}>
                            <div className="flex items-center gap-2">
                              <div className="w-1.5 h-1.5 rounded-full bg-slate-400 flex-shrink-0" />
                              <button
                                onClick={() => navigateToKpi(nodeKey, nodeStatus)}
                                className={`text-[10px] font-semibold px-2 py-0.5 rounded-full cursor-pointer hover:opacity-80 transition-opacity ${nodeColor}`}
                              >
                                {nodeLabel}
                              </button>
                              {typeof node === 'object' && node.confidence && (
                                <span className={`text-[8px] font-medium px-1 py-0.5 rounded-full ${
                                  node.confidence === 'granger_confirmed' ? 'bg-emerald-50 text-emerald-600' :
                                  node.confidence === 'directionally_supported' ? 'bg-blue-50 text-blue-600' :
                                  'bg-slate-50 text-slate-400'
                                }`}>
                                  {node.confidence === 'granger_confirmed' ? 'Confirmed' :
                                   node.confidence === 'directionally_supported' ? 'Directional' : 'Hypothesis'}
                                </span>
                              )}
                              {nodeCauses.length > 0 && (
                                <span className="text-[9px] text-slate-400">{nodeCauses.slice(0, 2).map(r => r.replace('[Template] ', '')).join(', ')}</span>
                              )}
                            </div>
                            {/* Second-hop children */}
                            {nodeChildren.length > 0 && (
                              <div className="ml-4 border-l border-slate-200 pl-2.5 mt-1 space-y-1">
                                {nodeChildren.map((child, ci) => {
                                  const childKey = typeof child === 'string' ? child : (child.node || child.key || child)
                                  const childLabel = formatKpiLabel(childKey)
                                  const childStatus = typeof child === 'object' ? child.status : null
                                  const childColor = childStatus === 'green' ? 'bg-emerald-100 text-emerald-700'
                                    : childStatus === 'red' ? 'bg-red-100 text-red-700'
                                    : (childStatus === 'amber' || childStatus === 'yellow') ? 'bg-amber-100 text-amber-700'
                                    : 'bg-slate-100 text-slate-600'
                                  return (
                                    <div key={ci} className="flex items-center gap-2">
                                      <div className="w-1 h-1 rounded-full bg-slate-300 flex-shrink-0" />
                                      <button
                                        onClick={() => navigateToKpi(childKey, childStatus)}
                                        className={`text-[9px] font-semibold px-1.5 py-0.5 rounded-full cursor-pointer hover:opacity-80 transition-opacity ${childColor}`}
                                      >
                                        {childLabel}
                                      </button>
                                    </div>
                                  )
                                })}
                              </div>
                            )}
                          </div>
                        )
                      })}
                    </div>
                  </div>
                </div>
              )}

              {/* Downstream Impact (clickable pills) */}
              {detail.downstream_impact && detail.downstream_impact.length > 0 && (
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1.5">Downstream Impact</p>
                  <div className="flex flex-wrap gap-1.5">
                    {detail.downstream_impact.map((dk, i) => {
                      const dkKey = typeof dk === 'string' ? dk : dk.key || dk
                      const dkStatus = typeof dk === 'object' ? dk.status : null
                      const dkColor = dkStatus === 'green' ? 'bg-emerald-100 text-emerald-700 hover:bg-emerald-200'
                        : dkStatus === 'red' ? 'bg-red-100 text-red-700 hover:bg-red-200'
                        : (dkStatus === 'amber' || dkStatus === 'yellow') ? 'bg-amber-100 text-amber-700 hover:bg-amber-200'
                        : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                      return (
                        <button
                          key={i}
                          onClick={() => navigateToKpi(dkKey, dkStatus)}
                          className={`text-[10px] font-semibold px-2 py-0.5 rounded-full cursor-pointer transition-colors ${dkColor}`}
                        >
                          {formatKpiLabel(dkKey)}
                        </button>
                      )
                    })}
                  </div>
                </div>
              )}

              {/* Recommended Actions */}
              {detail.corrective_actions && detail.corrective_actions.length > 0 && (
                <div>
                  <div className="flex items-center gap-2 mb-1">
                    <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400">Recommended Actions</p>
                    {(detail.actions_source === 'data_grounded' || detail.actions_source === 'data_driven_context') && (
                      <span className="text-[8px] font-medium px-1.5 py-0.5 rounded-full bg-emerald-50 text-emerald-600 border border-emerald-200">
                        Data-grounded
                      </span>
                    )}
                  </div>
                  <ul className="space-y-1">
                    {detail.corrective_actions.map((action, i) => (
                      <li key={i} className="flex items-start gap-2 text-[12px] text-slate-600 leading-relaxed">
                        <CheckCircle2 size={11} className="text-emerald-500 mt-0.5 flex-shrink-0" />
                        {action}
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {/* Industry Benchmarks (simplified — single most relevant stage) */}
              {detail.benchmarks && Object.keys(detail.benchmarks).length > 0 && (() => {
                const stages = detail.benchmarks
                // Pick series_a as default, or fall back to the first available stage
                const bestStage = stages.series_a || stages[Object.keys(stages)[0]]
                if (!bestStage) return null
                return (
                  <div>
                    <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1.5">Industry Benchmarks</p>
                    <div className="grid grid-cols-3 gap-2">
                      {[
                        { label: 'P25', value: bestStage?.p25 },
                        { label: 'Median', value: bestStage?.p50 },
                        { label: 'P75', value: bestStage?.p75 },
                      ].map(({ label: bl, value: bv }) => (
                        <div key={bl} className="bg-slate-50 rounded-lg p-2 text-center">
                          <div className="text-[10px] text-slate-400 font-medium">{bl}</div>
                          <div className="text-[13px] font-bold text-slate-700">
                            {bv != null ? `${bv}${bestStage?.label ? ` ${bestStage.label}` : (currentKpi?.unit || '')}` : '—'}
                          </div>
                        </div>
                      ))}
                    </div>
                    <p className="text-[9px] text-slate-400 mt-1.5">Based on SaaS industry data</p>
                  </div>
                )
              })()}

              {/* Benchmark positioning (stage-specific) */}
              {detail.benchmark_position && (
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1.5">Peer Positioning</p>
                  <div className="bg-slate-50 rounded-xl p-3">
                    <div className="flex items-center gap-2 mb-1">
                      <span className={`text-[11px] font-bold ${
                        detail.benchmark_position.quartile === 4 ? 'text-emerald-600' :
                        detail.benchmark_position.quartile === 1 ? 'text-red-600' :
                        'text-amber-600'
                      }`}>
                        {detail.benchmark_position.quartile_label}
                      </span>
                      {detail.benchmark_position.stage_label && (
                        <span className="text-[10px] text-slate-400">for {detail.benchmark_position.stage_label} companies</span>
                      )}
                    </div>
                    {detail.benchmark_position.percentile != null && (
                      <div className="w-full bg-slate-200 rounded-full h-1.5 mt-2">
                        <div className="h-1.5 rounded-full transition-all"
                          style={{
                            width: `${Math.min(100, Math.max(0, detail.benchmark_position.percentile))}%`,
                            backgroundColor: detail.benchmark_position.quartile >= 3 ? '#059669' : detail.benchmark_position.quartile === 2 ? '#D97706' : '#DC2626',
                          }}
                        />
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* Correlated KPIs */}
              {detail.correlations && detail.correlations.length > 0 && (
                <div>
                  <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1.5">Correlated KPIs</p>
                  <p className="text-[9px] text-slate-400 mb-2">Data-driven correlations from your actual monthly trends</p>
                  <div className="space-y-1.5">
                    {detail.correlations.slice(0, 5).map((corr, i) => (
                      <button key={i}
                        onClick={() => navigateToKpi(corr.key || corr.kpi_key)}
                        className="flex items-center gap-2 w-full text-left px-3 py-2 bg-slate-50 rounded-lg hover:bg-slate-100 transition-colors"
                      >
                        <span className={`w-2 h-2 rounded-full flex-shrink-0 ${
                          corr.correlation > 0 ? 'bg-emerald-400' : 'bg-red-400'
                        }`}/>
                        <span className="text-[11px] text-slate-700 font-medium flex-1 truncate">
                          {formatKpiLabel(corr.key || corr.kpi_key)}
                        </span>
                        <span className={`text-[10px] font-bold ${
                          corr.correlation > 0 ? 'text-emerald-600' : 'text-red-600'
                        }`}>
                          {corr.correlation > 0 ? '+' : ''}{(corr.correlation * 100).toFixed(0)}%
                        </span>
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Footer with cross-page CTAs */}
        <div className="px-5 py-4 border-t border-slate-100 space-y-2">
          {info.tab && (
            <button
              onClick={() => { onNavigate?.(info.tab); handleClose() }}
              className="w-full flex items-center justify-center gap-2 bg-[#0055A4] hover:bg-[#004688] text-white text-[12px] font-semibold py-2.5 rounded-xl transition-colors"
            >
              Open Full Analysis <ArrowRight size={13} />
            </button>
          )}
          <div className="flex items-center gap-2">
            <button onClick={() => { onNavigate?.('forecast'); handleClose() }}
              className="flex-1 flex items-center justify-center gap-1.5 py-2 text-[11px] font-semibold text-slate-600 hover:text-[#0055A4] bg-slate-50 hover:bg-slate-100 rounded-lg transition-colors">
              <BarChart2 size={11}/> Forecast
            </button>
            <button onClick={() => { onNavigate?.('variance'); handleClose() }}
              className="flex-1 flex items-center justify-center gap-1.5 py-2 text-[11px] font-semibold text-slate-600 hover:text-[#0055A4] bg-slate-50 hover:bg-slate-100 rounded-lg transition-colors">
              <Activity size={11}/> Variance
            </button>
            <button onClick={() => { onNavigate?.('decisions'); handleClose() }}
              className="flex-1 flex items-center justify-center gap-1.5 py-2 text-[11px] font-semibold text-slate-600 hover:text-[#0055A4] bg-slate-50 hover:bg-slate-100 rounded-lg transition-colors">
              <FileText size={11}/> Decision
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}

// ── Score Breakdown Modal (with expandable component detail) ──────────────────
function ScoreBreakdownModal({ health, onClose, onWeightsApply }) {
  const score = health?.score ?? 0
  const color = health?.color ?? 'grey'
  const mom = health?.momentum ?? 0
  const tgt = health?.target_achievement ?? 0
  const rsk = health?.risk_flags ?? 0
  const cd = health?.component_detail || {}

  // Editable weights
  const [weights, setWeights] = useState({ momentum: 30, target: 40, risk: 30 })
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)
  const [expandedComponent, setExpandedComponent] = useState(null)

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
      onWeightsApply?.(weights)
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

  const toggleExpand = (key) => setExpandedComponent(prev => prev === key ? null : key)

  const components = [
    {
      key: 'momentum', label: 'Momentum', value: mom, Icon: Activity, wKey: 'momentum',
      desc: 'Compares the average of each KPI over the last 3 months vs the 3 months before that. KPIs improving by >0.5% count as "improving"; declining by >0.5% count as "declining".',
      rationale: 'Why this matters: Momentum captures the direction of change, not just the current level. A business hitting all targets but trending downward has different urgency than one recovering from a dip. This signal detects deterioration before it shows up in targets.',
      formula: 'Score = (Improving KPIs / (Improving + Declining)) x 100. Stable KPIs are excluded. 50 = neutral.',
    },
    {
      key: 'target', label: 'Target Achievement', value: tgt, Icon: Target, wKey: 'target',
      desc: 'Percentage of KPIs with targets that are currently on track (within 2% of target, direction-aware). Higher-is-better and lower-is-better KPIs are scored correctly.',
      rationale: 'Why this matters: Target achievement is the most direct measure of whether the business is executing against its plan. It carries the highest default weight (40%) because a company hitting its targets is fundamentally healthy regardless of other signals.',
      formula: 'Score = (KPIs on target / Total KPIs with targets) x 100. "On target" = within 2% tolerance.',
    },
    {
      key: 'risk', label: 'Risk Score', value: rsk, Icon: Shield, wKey: 'risk',
      desc: 'Starts at 100 and deducts points for each KPI in critical/red status (>10% miss from target). More red KPIs = lower risk score.',
      rationale: 'Why this matters: Risk score penalises concentration of failures. Even if most KPIs are green, having several critical misses signals structural problems. This prevents a high target achievement score from masking serious underperformance in key areas.',
      formula: 'Score = (1 - Red KPIs / Total Scored KPIs) x 100. No targets = 70 (moderate default).',
    },
  ]

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="absolute inset-0 bg-black/30 backdrop-blur-[2px]" />
      <div
        className="relative bg-white rounded-2xl shadow-2xl w-full max-w-lg max-h-[90vh] flex flex-col"
        onClick={e => e.stopPropagation()}
        style={{ animation: 'fadeInScale 0.18s ease-out' }}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 pt-6 pb-3">
          <p className="text-slate-800 font-bold text-sm">How is the Health Score calculated?</p>
          <button onClick={onClose} className="p-1 rounded-md hover:bg-slate-100 text-slate-400"><X size={14}/></button>
        </div>

        {/* Scrollable content */}
        <div className="flex-1 overflow-y-auto px-6 pb-6">
          <p className="text-slate-500 text-[12px] leading-relaxed mb-5">{narrative()}</p>

          <div className="space-y-3">
            {components.map(({ key, label, value, Icon, desc, rationale, formula, wKey }) => {
              const safeValue = value ?? 0
              const c = safeValue >= 70 ? '#059669' : safeValue >= 50 ? '#D97706' : '#DC2626'
              const isExpanded = expandedComponent === key
              const detail = key === 'momentum' ? cd.momentum : key === 'target' ? cd.target_achievement : cd.risk
              return (
                <div key={key} className="rounded-xl border border-slate-200 overflow-hidden">
                  {/* Clickable header */}
                  <button
                    onClick={() => toggleExpand(key)}
                    className="w-full text-left px-4 py-3 hover:bg-slate-50 transition-colors"
                  >
                    <div className="flex items-center gap-2">
                      <Icon size={14} style={{ color: c }} />
                      <span className="text-slate-700 text-[12px] font-bold flex-1">{label}</span>
                      <span className="text-[13px] font-extrabold tabular-nums" style={{ color: c }}>{safeValue.toFixed(0)}</span>
                      <span className="text-[10px] text-slate-400 font-medium">/ 100</span>
                      <ChevronDown size={12} className={`text-slate-400 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                    </div>
                    <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden mt-2">
                      <div className="h-full rounded-full transition-all duration-700" style={{ width: `${safeValue}%`, backgroundColor: c }} />
                    </div>
                  </button>

                  {/* Expanded detail */}
                  {isExpanded && (
                    <div className="px-4 pb-4 space-y-3 border-t border-slate-100 bg-slate-50/50">
                      {/* What it measures */}
                      <div className="pt-3">
                        <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1">What it measures</p>
                        <p className="text-slate-600 text-[11px] leading-relaxed">{desc}</p>
                      </div>

                      {/* Formula */}
                      <div>
                        <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1">Formula</p>
                        <p className="text-slate-700 text-[11px] leading-relaxed font-mono bg-white px-3 py-2 rounded-lg border border-slate-100">{formula}</p>
                      </div>

                      {/* Rationale */}
                      <div>
                        <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1">Rationale</p>
                        <p className="text-slate-600 text-[11px] leading-relaxed">{rationale}</p>
                      </div>

                      {/* Per-KPI breakdown */}
                      {key === 'momentum' && detail?.kpis?.length > 0 && (
                        <div>
                          <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1.5">
                            KPI Breakdown ({detail.total_improving} improving, {detail.total_declining} declining, {detail.total_stable} stable)
                          </p>
                          <div className="space-y-1 max-h-48 overflow-y-auto">
                            {detail.kpis.map(k => (
                              <div key={k.key} className="flex items-center gap-2 px-2 py-1.5 bg-white rounded-lg text-[11px]">
                                <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${
                                  k.status === 'improving' ? 'bg-emerald-500' : k.status === 'declining' ? 'bg-red-500' : 'bg-slate-300'
                                }`} />
                                <span className="text-slate-700 font-medium flex-1 truncate">{k.name}</span>
                                <span className={`font-bold tabular-nums ${
                                  k.status === 'improving' ? 'text-emerald-600' : k.status === 'declining' ? 'text-red-600' : 'text-slate-400'
                                }`}>
                                  {k.delta_pct > 0 ? '+' : ''}{k.delta_pct}%
                                </span>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {key === 'target' && detail?.kpis?.length > 0 && (
                        <div>
                          <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1.5">
                            KPI Breakdown ({detail.total_on_target} on target, {detail.total_off_target} off target)
                          </p>
                          <div className="space-y-1 max-h-48 overflow-y-auto">
                            {detail.kpis.map(k => (
                              <div key={k.key} className="flex items-center gap-2 px-2 py-1.5 bg-white rounded-lg text-[11px]">
                                {k.on_target
                                  ? <CheckCircle2 size={11} className="text-emerald-500 flex-shrink-0" />
                                  : <AlertCircle size={11} className="text-red-500 flex-shrink-0" />
                                }
                                <span className="text-slate-700 font-medium flex-1 truncate">{k.name}</span>
                                <span className="text-slate-500 tabular-nums">{k.avg}</span>
                                <span className="text-slate-300">vs</span>
                                <span className="text-slate-500 tabular-nums">{k.target}</span>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {key === 'risk' && (
                        <div>
                          <p className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-1.5">
                            {detail?.total_red || 0} red KPI{(detail?.total_red || 0) !== 1 ? 's' : ''} out of {detail?.total_scored || 0} scored
                          </p>
                          {detail?.kpis?.length > 0 ? (
                            <div className="space-y-1 max-h-48 overflow-y-auto">
                              {detail.kpis.map(k => (
                                <div key={k.key} className="flex items-center gap-2 px-2 py-1.5 bg-white rounded-lg text-[11px]">
                                  <AlertTriangle size={11} className="text-red-500 flex-shrink-0" />
                                  <span className="text-slate-700 font-medium flex-1 truncate">{k.name}</span>
                                  <span className="text-red-600 font-bold tabular-nums">{k.avg}</span>
                                  <span className="text-slate-300">vs</span>
                                  <span className="text-slate-500 tabular-nums">{k.target}</span>
                                </div>
                              ))}
                            </div>
                          ) : (
                            <p className="text-emerald-600 text-[11px] font-medium">No KPIs in critical status. Risk score is at maximum.</p>
                          )}
                        </div>
                      )}

                      {/* Weight slider */}
                      <div className="flex items-center gap-3 bg-white rounded-lg px-3 py-2 border border-slate-100">
                        <Sliders size={11} className="text-slate-400 flex-shrink-0" />
                        <span className="text-slate-500 text-[10px] font-medium w-12 flex-shrink-0">Weight:</span>
                        <input
                          type="range"
                          min={0}
                          max={100}
                          value={weights[wKey]}
                          onChange={e => handleWeightChange(wKey, e.target.value)}
                          className="flex-1 h-1.5 accent-[#0055A4] cursor-pointer"
                        />
                        <span className="text-[12px] font-bold text-[#0055A4] w-10 text-right">{weights[wKey]}%</span>
                      </div>
                    </div>
                  )}
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

// ── Composite Score Breakdown (mini bar chart for the 4 signals) ─────────────
function CompositeBreakdown({ kpi, compact = false }) {
  const signals = [
    { key: 'gap',    label: 'Gap',    value: kpi.gap_score,    color: '#DC2626' },
    { key: 'trend',  label: 'Trend',  value: kpi.trend_score,  color: '#D97706' },
    { key: 'impact', label: 'Impact', value: kpi.impact_score, color: '#7c3aed' },
    { key: 'domain', label: 'Domain', value: kpi.domain_score, color: '#0055A4' },
  ].filter(s => s.value != null)

  if (signals.length === 0) return null

  if (compact) {
    return (
      <div className="flex items-center gap-0.5">
        {signals.map(s => (
          <div key={s.key} className="relative group">
            <div
              className="h-1.5 rounded-full"
              style={{ width: `${Math.max(s.value / 100 * 24, 2)}px`, backgroundColor: s.color, opacity: 0.7 }}
            />
            <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1 hidden group-hover:block z-20">
              <div className="bg-slate-900 text-white text-[9px] px-1.5 py-0.5 rounded whitespace-nowrap">
                {s.label}: {s.value}
              </div>
            </div>
          </div>
        ))}
      </div>
    )
  }

  return (
    <div className="flex items-center gap-2">
      {signals.map(s => (
        <div key={s.key} className="flex items-center gap-1">
          <div className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: s.color }} />
          <span className="text-[9px] text-slate-400">{s.label}</span>
          <span className="text-[9px] font-bold" style={{ color: s.color }}>{s.value}</span>
        </div>
      ))}
    </div>
  )
}

// ── Domain Badge ─────────────────────────────────────────────────────────────
const DOMAIN_COLORS = {
  cashflow:      { bg: 'bg-blue-100',    text: 'text-blue-700' },
  risk:          { bg: 'bg-red-100',     text: 'text-red-700' },
  growth:        { bg: 'bg-emerald-100', text: 'text-emerald-700' },
  revenue:       { bg: 'bg-violet-100',  text: 'text-violet-700' },
  retention:     { bg: 'bg-amber-100',   text: 'text-amber-700' },
  profitability: { bg: 'bg-cyan-100',    text: 'text-cyan-700' },
  efficiency:    { bg: 'bg-slate-100',   text: 'text-slate-600' },
  other:         { bg: 'bg-slate-100',   text: 'text-slate-500' },
}

function DomainBadge({ domain, label, status }) {
  // When status is provided (critical/needs-attention contexts), use status color
  // so the badge doesn't misleadingly show green for a critical KPI's domain.
  const STATUS_COLORS = {
    red:    { bg: 'bg-red-100',    text: 'text-red-700' },
    yellow: { bg: 'bg-amber-100',  text: 'text-amber-700' },
    green:  { bg: 'bg-emerald-100', text: 'text-emerald-700' },
  }
  const dc = (status && STATUS_COLORS[status])
    ? STATUS_COLORS[status]
    : (DOMAIN_COLORS[domain] || DOMAIN_COLORS.other)
  return (
    <span className={`text-[9px] font-semibold px-1.5 py-0.5 rounded-full ${dc.bg} ${dc.text}`}>
      {label || domain}
    </span>
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
  const avg    = fmtKpiValue(kpi.avg, kpi.unit)
  const target = fmtKpiValue(kpi.target, kpi.unit)
  // Direction-aware gap: positive = performing well, negative = underperforming
  const isLower = kpi.direction === 'lower'
  const gapPct = (kpi.avg != null && kpi.target)
    ? (isLower
        ? ((kpi.target - kpi.avg) / Math.abs(kpi.target) * 100).toFixed(1)
        : ((kpi.avg / kpi.target - 1) * 100).toFixed(1))
    : null
  const isWell = isLower ? kpi.avg <= kpi.target : kpi.avg >= kpi.target
  const sparkColor = status === 'green' ? '#059669' : status === 'red' ? '#DC2626' : '#D97706'

  return (
    <button
      onClick={() => onOpen?.(kpi, status)}
      className={`w-full text-left card p-3.5 ${s.bg} ${s.border} ${s.hover} hover:shadow-md transition-all group cursor-pointer`}
    >
      <div className="flex items-start justify-between gap-2 mb-1.5">
        <div className="flex items-center gap-1.5 flex-1 min-w-0 flex-wrap">
          <p className="text-slate-800 text-[12px] font-semibold leading-tight">{label}</p>
          {kpi.domain && <DomainBadge domain={kpi.domain} label={kpi.domain_label} />}
        </div>
        <div className="flex items-center gap-1 flex-shrink-0">
          <Sparkline data={kpi.sparkline} color={sparkColor} width={56} height={22} />
          <ChevronRight size={12} className="text-slate-300 group-hover:text-slate-500 transition-colors" />
        </div>
      </div>
      <div className="flex items-center gap-1.5 flex-wrap">
        <span className="text-slate-900 text-base font-extrabold leading-none">{avg}</span>
        <span className="text-slate-400 text-[10px]">vs {target}</span>
        {gapPct !== null && (
          <span className={`text-[11px] font-bold ${isWell ? 'text-emerald-600' : s.text}`}>
            {isWell ? `+${Math.abs(gapPct)}% ${isLower ? 'below' : 'above'}` : `-${Math.abs(gapPct)}% ${isLower ? 'above' : 'below'}`}
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
            <span className="text-[11px] font-bold" style={{ color }}>{(value ?? 0).toFixed(0)}</span>
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

  // If the API provides a detailed narrative, build a string from it
  if (health.narrative_detail && typeof health.narrative_detail === 'object') {
    const nd = health.narrative_detail
    const parts = []
    if (nd.score_meaning) parts.push(nd.score_meaning)
    if (nd.top_drags && nd.top_drags.length > 0) {
      parts.push(`The KPIs requiring most urgency are ${nd.top_drags.map(d => d.name || formatKpiLabel(d.key || d)).join(', ')}.`)
    }
    if (nd.primary_action) parts.push(nd.primary_action)
    if (parts.length > 0) return parts.join(' ')
  }

  const {
    score, color, momentum_trend,
    kpis_green: _kg = 0, kpis_yellow: _ky = 0, kpis_red: _kr = 0, kpis_grey: _kgr = 0,
    target_achievement: _ta = 0, momentum: _mom = 0, risk_flags: _rf = 0,
    red_kpis_detail: _rkd = [], yellow_kpis_detail: _ykd = []
  } = health
  // Null-safe: API may return null for these fields, which bypasses destructuring defaults
  const kpis_green = _kg ?? 0, kpis_yellow = _ky ?? 0, kpis_red = _kr ?? 0, kpis_grey = _kgr ?? 0
  const target_achievement = _ta ?? 0, momentum = _mom ?? 0, risk_flags = _rf ?? 0
  const red_kpis_detail = _rkd ?? [], yellow_kpis_detail = _ykd ?? []

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
    ? `The KPIs requiring most urgency are ${worstKpis.map(k => k.name || formatKpiLabel(k.key || k)).join(', ')}.`
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

// ── Months / Years for period picker ────────────────────────────────────────
const MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
function buildYearOptions() {
  const cur = new Date().getFullYear()
  const yrs = []
  for (let y = cur - 6; y <= cur; y++) yrs.push(y)
  return yrs
}

const QUICK_PRESETS = [
  { label: 'L3M', months: 3 },
  { label: 'L6M', months: 6 },
  { label: 'L12M', months: 12 },
  { label: 'YTD', months: -1 },
  { label: 'All', months: 0 },
]

// ── Period Selector (From-To date picker with quick presets) ───────────────
function PeriodSelector({ selected, periodDates, onSelect }) {
  const now = new Date()
  const [fromMonth, setFromMonth] = useState(periodDates?.fromMonth ?? 1)
  const [fromYear, setFromYear]   = useState(periodDates?.fromYear ?? now.getFullYear() - 1)
  const [toMonth, setToMonth]     = useState(periodDates?.toMonth ?? (now.getMonth() + 1))
  const [toYear, setToYear]       = useState(periodDates?.toYear ?? now.getFullYear())
  const [open, setOpen]           = useState(false)
  const [activePreset, setActivePreset] = useState(selected || 'All')
  const [validationMsg, setValidationMsg] = useState(null)

  // Sync from parent props when they change
  useEffect(() => {
    if (periodDates) {
      setFromMonth(periodDates.fromMonth); setFromYear(periodDates.fromYear)
      setToMonth(periodDates.toMonth); setToYear(periodDates.toYear)
    }
  }, [periodDates?.fromMonth, periodDates?.fromYear, periodDates?.toMonth, periodDates?.toYear])

  useEffect(() => {
    if (selected) setActivePreset(selected)
  }, [selected])
  const ref = useRef(null)
  const yearOptions = buildYearOptions()

  useEffect(() => {
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const applyFromTo = (fm, fy, tm, ty, presetLabel) => {
    setFromMonth(fm); setFromYear(fy); setToMonth(tm); setToYear(ty)
    setActivePreset(presetLabel || null)
    if (fy === 0 && ty === 0) {
      // "All" preset
      onSelect({ label: 'All Data', months: 0 })
    } else {
      onSelect({
        label: presetLabel || `${MONTH_NAMES[fm-1]} ${fy} - ${MONTH_NAMES[tm-1]} ${ty}`,
        months: null,
        _params: { from_year: fy, from_month: fm, to_year: ty, to_month: tm },
      })
    }
  }

  const handlePreset = (p) => {
    if (p.months === 0) {
      setActivePreset(p.label)
      onSelect({ label: 'All Data', months: 0 })
      return
    }
    const params = computePeriodParams(p)
    setFromMonth(params.from_month); setFromYear(params.from_year)
    setToMonth(params.to_month); setToYear(params.to_year)
    setActivePreset(p.label)
    onSelect(p)
  }

  const handleManualChange = (fm, fy, tm, ty) => {
    const fromVal = fy * 12 + fm
    const toVal = ty * 12 + tm
    if (fromVal > toVal) {
      setValidationMsg('"To" date cannot be before "From" date. Selection was adjusted.')
      setTimeout(() => setValidationMsg(null), 4000)
      const curFromVal = fromYear * 12 + fromMonth
      if (fm !== fromMonth || fy !== fromYear) {
        tm = fm; ty = fy
      } else {
        fm = tm; fy = ty
      }
    } else {
      setValidationMsg(null)
    }
    applyFromTo(fm, fy, tm, ty, null)
  }

  const displayLabel = activePreset || `${MONTH_NAMES[fromMonth-1]} ${fromYear} - ${MONTH_NAMES[toMonth-1]} ${toYear}`

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-1.5 px-2.5 py-1.5 border border-slate-200 hover:border-slate-300 rounded-lg text-[11px] text-slate-600 font-medium transition-colors bg-white"
      >
        <Calendar size={11} className="text-slate-400" />
        {displayLabel}
        <ChevronDown size={10} className={`text-slate-400 transition-transform ${open ? 'rotate-180' : ''}`} />
      </button>
      {open && (
        <div className="absolute right-0 top-full mt-1 bg-white border border-slate-200 rounded-xl shadow-lg p-3 z-30 min-w-[280px]"
             style={{ animation: 'fadeInScale 0.12s ease-out' }}>
          {/* Quick presets */}
          <div className="flex items-center gap-1 mb-3">
            {QUICK_PRESETS.map(p => (
              <button
                key={p.label}
                onClick={() => { handlePreset(p); setOpen(false) }}
                className={`px-2 py-1 rounded-full text-[10px] font-semibold transition-colors ${
                  activePreset === p.label
                    ? 'bg-[#0055A4] text-white'
                    : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
                }`}
              >
                {p.label}
              </button>
            ))}
          </div>
          {/* Validation message */}
          {validationMsg && (
            <div className="bg-amber-50 border border-amber-200 rounded-lg px-2.5 py-1.5 mb-2 flex items-center gap-1.5">
              <AlertTriangle size={10} className="text-amber-500 flex-shrink-0" />
              <span className="text-[10px] text-amber-700 font-medium">{validationMsg}</span>
            </div>
          )}
          {/* From selects */}
          <div className="space-y-2">
            <div>
              <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-1">From</p>
              <div className="flex items-center gap-1.5">
                <select
                  value={fromMonth}
                  onChange={e => { const fm = Number(e.target.value); setFromMonth(fm); handleManualChange(fm, fromYear, toMonth, toYear) }}
                  className="flex-1 text-[11px] border border-slate-200 rounded-lg px-2 py-1.5 text-slate-600 bg-white focus:outline-none focus:border-[#0055A4]"
                >
                  {MONTH_NAMES.map((m, i) => <option key={m} value={i+1}>{m}</option>)}
                </select>
                <select
                  value={fromYear}
                  onChange={e => { const fy = Number(e.target.value); setFromYear(fy); handleManualChange(fromMonth, fy, toMonth, toYear) }}
                  className="w-20 text-[11px] border border-slate-200 rounded-lg px-2 py-1.5 text-slate-600 bg-white focus:outline-none focus:border-[#0055A4]"
                >
                  {yearOptions.map(y => <option key={y} value={y}>{y}</option>)}
                </select>
              </div>
            </div>
            <div>
              <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-1">To</p>
              <div className="flex items-center gap-1.5">
                <select
                  value={toMonth}
                  onChange={e => { const tm = Number(e.target.value); setToMonth(tm); handleManualChange(fromMonth, fromYear, tm, toYear) }}
                  className="flex-1 text-[11px] border border-slate-200 rounded-lg px-2 py-1.5 text-slate-600 bg-white focus:outline-none focus:border-[#0055A4]"
                >
                  {MONTH_NAMES.map((m, i) => <option key={m} value={i+1}>{m}</option>)}
                </select>
                <select
                  value={toYear}
                  onChange={e => { const ty = Number(e.target.value); setToYear(ty); handleManualChange(fromMonth, fromYear, toMonth, ty) }}
                  className="w-20 text-[11px] border border-slate-200 rounded-lg px-2 py-1.5 text-slate-600 bg-white focus:outline-none focus:border-[#0055A4]"
                >
                  {yearOptions.map(y => <option key={y} value={y}>{y}</option>)}
                </select>
              </div>
            </div>
          </div>
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
  const [showOtherCritical, setShowOtherCritical] = useState(false)
  const [showAllDoingWell, setShowAllDoingWell] = useState(false)
  const [selectedPeriod, setSelectedPeriod] = useState('All Data')
  const [periodDates, setPeriodDates] = useState({ fromMonth: 1, fromYear: new Date().getFullYear() - 1, toMonth: new Date().getMonth() + 1, toYear: new Date().getFullYear() })
  const [activeWeights, setActiveWeights] = useState(null)
  const [critWeights, setCritWeights]     = useState(null)
  const [settingsLoaded, setSettingsLoaded] = useState(false)

  // Load persisted period selection + criticality weights from company_settings on mount
  useEffect(() => {
    axios.get('/api/company-settings')
      .then(r => {
        const s = r.data || {}
        if (s.home_period_preset) {
          setSelectedPeriod(s.home_period_preset)
        }
        if (s.home_period_dates) {
          try {
            const d = JSON.parse(s.home_period_dates)
            if (d.fromMonth && d.fromYear && d.toMonth && d.toYear) {
              setPeriodDates(d)
            }
          } catch {}
        }
        if (s.criticality_weights) {
          try {
            const cw = JSON.parse(s.criticality_weights)
            setCritWeights(cw)
          } catch {}
        }
        setSettingsLoaded(true)
      })
      .catch(() => setSettingsLoaded(true))
  }, [])

  const load = useCallback((periodParams = {}, weightOverrides = null) => {
    setLoading(true); setError(false)
    const params = new URLSearchParams()
    if (periodParams.from_year) params.set('from_year', periodParams.from_year)
    if (periodParams.from_month) params.set('from_month', periodParams.from_month)
    if (periodParams.to_year) params.set('to_year', periodParams.to_year)
    if (periodParams.to_month) params.set('to_month', periodParams.to_month)
    const w = weightOverrides || null
    if (w) {
      params.set('w_momentum', w.momentum)
      params.set('w_target', w.target)
      params.set('w_risk', w.risk)
    }
    // Pass criticality weights if configured
    if (critWeights) {
      const total = (critWeights.gap || 0) + (critWeights.trend || 0) + (critWeights.impact || 0) + (critWeights.domain || 0)
      if (total > 0) {
        params.set('cw_gap',    (critWeights.gap    / total).toFixed(3))
        params.set('cw_trend',  (critWeights.trend  / total).toFixed(3))
        params.set('cw_impact', (critWeights.impact / total).toFixed(3))
        params.set('cw_domain', (critWeights.domain / total).toFixed(3))
      }
    }
    const qs = params.toString()
    axios.get(`/api/home${qs ? `?${qs}` : ''}`)
      .then(r  => { setData(r.data); setLoading(false) })
      .catch(() => { setError(true); setLoading(false) })
  }, [critWeights])

  const handleWeightsApply = useCallback((weights) => {
    setActiveWeights(weights)
    load({}, weights)
  }, [load])

  const loadDemoData = async () => {
    setSeeding(true)
    try {
      await axios.get('/api/reseed-canonical')
      // Background seed takes 2-5 min; poll for data
      let attempts = 0
      const poll = setInterval(async () => {
        attempts++
        try {
          const r = await axios.get('/api/home')
          if (r.data?.health?.kpis_green > 0 || attempts > 30) {
            clearInterval(poll)
            setSeeding(false)
            load()
          }
        } catch { if (attempts > 30) { clearInterval(poll); setSeeding(false); load() } }
      }, 10000)
    } catch { setSeeding(false); load() }
  }

  const handlePeriodChange = (preset) => {
    setSelectedPeriod(preset.label)
    const params = preset._params ? preset._params : computePeriodParams(preset)
    const newDates = params.from_year
      ? { fromMonth: params.from_month, fromYear: params.from_year, toMonth: params.to_month, toYear: params.to_year }
      : periodDates
    if (params.from_year) setPeriodDates(newDates)
    load(params)
    // Persist selection to DB for all team members
    axios.put('/api/company-settings', {
      home_period_preset: preset.label,
      home_period_dates: JSON.stringify(newDates),
    }).catch(() => {})
  }

  // Initial load: wait for settings, then load with persisted period
  useEffect(() => {
    if (!settingsLoaded) return
    if (selectedPeriod === 'All Data') {
      load()
    } else {
      // Rebuild params from persisted dates
      const params = {
        from_year: periodDates.fromYear,
        from_month: periodDates.fromMonth,
        to_year: periodDates.toYear,
        to_month: periodDates.toMonth,
      }
      load(params)
    }
  }, [settingsLoaded]) // eslint-disable-line react-hooks/exhaustive-deps

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
  const topCritical = needs_attention?.slice(0, 3) || []
  const otherCritical = needs_attention?.slice(3) || []
  const watchKpis = (data.watch_zone || health?.yellow_kpis_detail || []).slice(0, 4)
  const greyKpis = (health?.grey_kpis_list || []).map(k => typeof k === 'string' ? { key: k } : k)
  const doingWellVisible = showAllDoingWell ? doing_well : doing_well?.slice(0, 6)

  return (
    <div className="space-y-5 max-w-7xl">

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
          <PeriodSelector selected={selectedPeriod} periodDates={periodDates} onSelect={handlePeriodChange} />
          <button onClick={() => load()} className="p-1.5 rounded-lg hover:bg-slate-100 text-slate-400 hover:text-slate-600 transition-colors" title="Refresh">
            <RefreshCw size={13} />
          </button>
        </div>
      </div>

      {/* ── Health Score + Most Critical (side-by-side) ────────────────── */}
      <div className={`grid grid-cols-1 ${topCritical.length > 0 ? 'lg:grid-cols-[1fr_1fr]' : ''} gap-4 items-start`}>

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

      {/* ── Top 3 Most Critical (Compact Right Panel) ────────────────── */}
      {topCritical.length > 0 && (
        <div className="card p-4 shadow-sm hover:shadow-md transition-shadow border-red-100 bg-red-50/30">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <AlertTriangle size={13} className="text-red-500" />
              <h2 className="text-slate-700 text-[11px] font-bold uppercase tracking-wider">Most Critical</h2>
              <span className="bg-red-100 text-red-700 text-[10px] font-bold px-1.5 py-0.5 rounded-full">{needs_attention?.length || 0}</span>
            </div>
            <button onClick={() => onNavigate?.('variance')}
              className="text-[11px] text-slate-400 hover:text-[#0055A4] flex items-center gap-1 transition-colors font-medium">
              Full Analysis <ArrowRight size={10}/>
            </button>
          </div>

          <div className="space-y-2">
            {topCritical.map(kpi => {
              const kLabel = formatKpiLabel(kpi.key)
              const kAvg = fmtKpiValue(kpi.avg, kpi.unit)
              const kTarget = fmtKpiValue(kpi.target, kpi.unit)
              const kGapFromApi = kpi.gap_pct
              const kComposite = kpi.composite
              const kRank = kpi.rank
              return (
                <button
                  key={kpi.key}
                  onClick={() => setSlideOut({ kpi, status: kpi.status || 'red' })}
                  className="w-full text-left bg-white rounded-xl p-3 border border-red-200 hover:border-red-300 hover:shadow-md transition-all group cursor-pointer"
                >
                  <div className="flex items-center gap-2.5">
                    {kRank && (
                      <span className="bg-red-600 text-white text-[9px] font-bold w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0">
                        {kRank}
                      </span>
                    )}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-0.5 flex-wrap">
                        <span className="text-slate-800 text-[12px] font-bold truncate">{kLabel}</span>
                        {kpi.domain && <DomainBadge domain={kpi.domain} label={kpi.domain_label} status="red" />}
                      </div>
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-slate-900 text-sm font-extrabold">{kAvg}</span>
                        <span className="text-slate-400 text-[10px]">vs {kTarget}</span>
                        {kGapFromApi != null && (
                          <span className="text-red-600 text-[10px] font-bold">{kGapFromApi}% off</span>
                        )}
                        {kpi.is_structural && (
                          <span className="text-[9px] font-semibold text-orange-600 bg-orange-100 px-1.5 py-0.5 rounded-full flex items-center gap-0.5">
                            <Flame size={8}/>{kpi.miss_streak}mo streak
                          </span>
                        )}
                        {!kpi.is_structural && kpi.miss_streak > 1 && (
                          <span className="text-[9px] font-medium text-amber-600 bg-amber-50 px-1.5 py-0.5 rounded-full">
                            {kpi.miss_streak}mo miss
                          </span>
                        )}
                        {kpi.benchmark?.quartile_label && (
                          <span className={`text-[9px] font-medium px-1.5 py-0.5 rounded-full ${
                            kpi.benchmark.quartile === 1 ? 'text-red-600 bg-red-50' :
                            kpi.benchmark.quartile === 4 ? 'text-emerald-600 bg-emerald-50' :
                            'text-slate-500 bg-slate-100'
                          }`}>
                            {kpi.benchmark.quartile_label}
                          </span>
                        )}
                        {kComposite != null && (
                          <span className="text-[9px] font-bold text-red-500 bg-red-100 px-1.5 py-0.5 rounded-full ml-auto">
                            {kComposite}
                          </span>
                        )}
                      </div>
                    </div>
                    <div className="flex flex-col items-end gap-0.5 flex-shrink-0">
                      <Sparkline data={kpi.sparkline} color="#DC2626" width={60} height={24} />
                      <ChevronRight size={11} className="text-slate-300 group-hover:text-slate-500" />
                    </div>
                  </div>
                  {/* Cross-page CTAs */}
                  <div className="flex items-center gap-1.5 mt-2 pt-2 border-t border-red-100">
                    <span className="text-[9px] text-red-300 font-medium flex-shrink-0">Quick:</span>
                    <button onClick={(e) => { e.stopPropagation(); onNavigate?.('forecast') }}
                      className="text-[9px] font-semibold text-slate-500 hover:text-[#0055A4] bg-white border border-slate-200 hover:border-[#0055A4]/30 px-2 py-0.5 rounded-md transition-colors">
                      Forecast →
                    </button>
                    <button onClick={(e) => { e.stopPropagation(); onNavigate?.('variance') }}
                      className="text-[9px] font-semibold text-slate-500 hover:text-[#0055A4] bg-white border border-slate-200 hover:border-[#0055A4]/30 px-2 py-0.5 rounded-md transition-colors">
                      Variance →
                    </button>
                    <button onClick={(e) => { e.stopPropagation(); onNavigate?.('decisions') }}
                      className="text-[9px] font-semibold text-slate-500 hover:text-[#0055A4] bg-white border border-slate-200 hover:border-[#0055A4]/30 px-2 py-0.5 rounded-md transition-colors">
                      Log Decision →
                    </button>
                  </div>
                </button>
              )
            })}
          </div>

          {/* Compact methodology hint */}
          {data?.composite_methodology && (
            <div className="mt-3 pt-2.5 border-t border-red-100">
              <div className="flex items-center gap-2 flex-wrap">
                <span className="text-[9px] text-slate-400 font-medium">Ranked by:</span>
                {data.composite_methodology.signals.map(s => (
                  <span key={s.key} className="text-[9px] text-slate-500">
                    {s.label} {s.weight}%
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      </div>{/* end side-by-side grid */}

      {/* ── Decision Check-ins (30-day reminders) ──────────────────── */}
      {data?.decision_check_ins?.length > 0 && (
        <div className="space-y-2">
          {data.decision_check_ins.map((ci, i) => (
            <button key={i} onClick={() => onNavigate?.('decisions')}
              className="w-full text-left card p-3.5 bg-blue-50/60 border-blue-200 hover:border-blue-300 hover:shadow-md transition-all group">
              <div className="flex items-start gap-3">
                <MessageSquare size={14} className="text-blue-500 mt-0.5 flex-shrink-0" />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-0.5">
                    <span className="text-slate-700 text-[12px] font-bold">{ci.title}</span>
                    <span className="text-[9px] font-medium text-blue-600 bg-blue-100 px-1.5 py-0.5 rounded-full">{ci.days_since}d ago</span>
                  </div>
                  <p className="text-slate-500 text-[11px] leading-snug">{ci.reason}</p>
                </div>
                <span className="text-[10px] text-blue-500 font-semibold flex items-center gap-1 flex-shrink-0 group-hover:text-blue-700">
                  Review <ArrowRight size={10}/>
                </span>
              </div>
            </button>
          ))}
        </div>
      )}

      {/* ── Domain Narratives + Period Comparison ─────────────────────── */}
      {(data?.domain_narratives?.length > 0 || data?.period_comparison) && (
        <div className="grid grid-cols-1 lg:grid-cols-[1fr_1fr] gap-4">

          {/* Domain narratives */}
          {data?.domain_narratives?.length > 0 && (
            <div className="card p-4 shadow-sm">
              <div className="flex items-center gap-2 mb-3">
                <FileText size={13} className="text-slate-400" />
                <h2 className="text-slate-700 text-[11px] font-bold uppercase tracking-wider">Domain Intelligence</h2>
              </div>
              <div className="space-y-2">
                {data.domain_narratives.map((dn, i) => (
                  <div key={i} className="flex items-start gap-2.5">
                    <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 mt-1.5 ${
                      dn.severity === 'critical' ? 'bg-red-500' :
                      dn.severity === 'warning'  ? 'bg-amber-500' :
                      'bg-emerald-500'
                    }`} />
                    <div className="min-w-0">
                      <span className="text-slate-600 text-[11px] font-semibold">{dn.domain_label || dn.domain}: </span>
                      <span className="text-slate-500 text-[11px] leading-relaxed">{dn.narrative}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Period comparison */}
          {data?.period_comparison && (data.period_comparison.improved?.length > 0 || data.period_comparison.deteriorated?.length > 0) && (
            <div className="card p-4 shadow-sm">
              <div className="flex items-center gap-2 mb-3">
                <Zap size={13} className="text-slate-400" />
                <h2 className="text-slate-700 text-[11px] font-bold uppercase tracking-wider">vs Prior Period</h2>
              </div>
              <div className="space-y-3">
                {data.period_comparison.improved?.length > 0 && (
                  <div>
                    <div className="flex items-center gap-1.5 mb-1.5">
                      <TrendingUp size={11} className="text-emerald-500" />
                      <span className="text-[10px] font-bold text-emerald-600 uppercase tracking-wider">Improved ({data.period_comparison.improved.length})</span>
                    </div>
                    <div className="flex flex-wrap gap-1.5">
                      {data.period_comparison.improved.map(kpi => {
                        const pct = kpi.delta_pct ?? (kpi.prev ? Math.max(-999, Math.min(999, (kpi.delta / Math.abs(kpi.prev)) * 100)) : kpi.delta)
                        const currVal = kpi.curr != null ? fmtKpiValue(kpi.curr, kpi.unit) : null
                        return (
                          <button key={kpi.key} onClick={() => setSlideOut({ kpi: { key: kpi.key, avg: kpi.curr, unit: kpi.unit, direction: kpi.direction }, status: 'green' })}
                            className="text-[10px] font-medium text-emerald-700 bg-emerald-50 px-2 py-1 rounded-lg hover:bg-emerald-100 transition-colors cursor-pointer">
                            {formatKpiLabel(kpi.key)} {currVal && <span className="font-bold">{currVal}</span>} <span className="opacity-70">({pct > 0 ? '+' : ''}{pct?.toFixed?.(1) ?? pct}%)</span>
                          </button>
                        )
                      })}
                    </div>
                  </div>
                )}
                {data.period_comparison.deteriorated?.length > 0 && (
                  <div>
                    <div className="flex items-center gap-1.5 mb-1.5">
                      <TrendingDown size={11} className="text-red-500" />
                      <span className="text-[10px] font-bold text-red-600 uppercase tracking-wider">Deteriorated ({data.period_comparison.deteriorated.length})</span>
                    </div>
                    <div className="flex flex-wrap gap-1.5">
                      {data.period_comparison.deteriorated.map(kpi => {
                        const pct = kpi.delta_pct ?? (kpi.prev ? Math.max(-999, Math.min(999, (kpi.delta / Math.abs(kpi.prev)) * 100)) : kpi.delta)
                        const currVal = kpi.curr != null ? fmtKpiValue(kpi.curr, kpi.unit) : null
                        return (
                          <button key={kpi.key} onClick={() => setSlideOut({ kpi: { key: kpi.key, avg: kpi.curr, unit: kpi.unit, direction: kpi.direction }, status: kpi.status || 'red' })}
                            className="text-[10px] font-medium text-red-700 bg-red-50 px-2 py-1 rounded-lg hover:bg-red-100 transition-colors cursor-pointer">
                            {formatKpiLabel(kpi.key)} {currVal && <span className="font-bold">{currVal}</span>} <span className="opacity-70">({pct > 0 ? '+' : ''}{pct?.toFixed?.(1) ?? pct}%)</span>
                          </button>
                        )
                      })}
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── Other Critical (by Business Domain) ─────────────────────── */}
      {otherCritical.length > 0 && (
        <div>
          <button
            onClick={() => setShowOtherCritical(!showOtherCritical)}
            className="flex items-center gap-2 mb-2 group"
          >
            <h2 className="text-slate-700 text-[11px] font-bold uppercase tracking-wider">Other Critical</h2>
            <span className="bg-red-100 text-red-700 text-[10px] font-bold px-1.5 py-0.5 rounded-full">{otherCritical.length}</span>
            <ChevronDown size={12} className={`text-slate-400 transition-transform ${showOtherCritical ? 'rotate-180' : ''}`} />
          </button>
          {showOtherCritical && (() => {
            // Group other critical KPIs by domain
            const byDomain = {}
            otherCritical.forEach(kpi => {
              const d = kpi.domain || 'other'
              if (!byDomain[d]) byDomain[d] = { label: kpi.domain_label || d, kpis: [] }
              byDomain[d].kpis.push(kpi)
            })
            const domainKeys = Object.keys(byDomain).sort((a, b) => {
              const order = ['cashflow','risk','growth','revenue','retention','profitability','efficiency','other']
              return order.indexOf(a) - order.indexOf(b)
            })
            return (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                {domainKeys.map(dk => (
                  <div key={dk} className="bg-slate-50 rounded-xl p-2">
                    <div className="flex items-center gap-1.5 px-2 mb-1">
                      <DomainBadge domain={dk} label={byDomain[dk].label} status="red" />
                      <span className="text-[10px] text-slate-400">{byDomain[dk].kpis.length} metric{byDomain[dk].kpis.length > 1 ? 's' : ''}</span>
                    </div>
                    {byDomain[dk].kpis.map(kpi => {
                      const kLabel = formatKpiLabel(kpi.key)
                      const kAvg = fmtKpiValue(kpi.avg, kpi.unit)
                      const kTarget = fmtKpiValue(kpi.target, kpi.unit)
                      return (
                        <button
                          key={kpi.key}
                          onClick={() => setSlideOut({ kpi, status: kpi.status || 'red' })}
                          className="flex items-center gap-3 w-full text-left px-3 py-2 rounded-lg hover:bg-white transition-colors group"
                        >
                          <span className="text-[9px] font-bold text-red-500 bg-red-100 w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0">
                            {kpi.rank || '·'}
                          </span>
                          <span className="text-[11px] font-semibold text-slate-700 flex-1 truncate">{kLabel}</span>
                          {kpi.composite != null && (
                            <span className="text-[10px] font-bold text-red-500">{kpi.composite}</span>
                          )}
                          <span className="text-[11px] text-slate-800 font-bold">{kAvg}</span>
                          <span className="text-[10px] text-slate-400">vs {kTarget}</span>
                          <CompositeBreakdown kpi={kpi} compact />
                          <ChevronRight size={11} className="text-slate-300 group-hover:text-slate-500 flex-shrink-0" />
                        </button>
                      )
                    })}
                  </div>
                ))}
              </div>
            )
          })()}
        </div>
      )}

      {/* ── Watch Zone ──────────────────────────────────────────────── */}
      {watchKpis.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-2.5">
            <Eye size={13} className="text-amber-500" />
            <h2 className="text-slate-700 text-[11px] font-bold uppercase tracking-wider">Watch Zone</h2>
            <span className="bg-amber-100 text-amber-700 text-[10px] font-bold px-1.5 py-0.5 rounded-full">{health?.kpis_yellow || watchKpis.length}</span>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-2.5">
            {watchKpis.map(kpi => {
              const wKey = kpi.key || kpi
              // Enriched watch_zone data has avg/target/sparkline; legacy yellow_kpis_detail only has key+pct
              const hasFullData = kpi.avg != null
              if (hasFullData) {
                return (
                  <KpiCard key={wKey} kpi={{ key: wKey, avg: kpi.avg, target: kpi.target, unit: kpi.unit, sparkline: kpi.sparkline, direction: kpi.direction, domain: kpi.domain, domain_label: kpi.domain_label }} status="amber"
                    onOpen={(k, s) => setSlideOut({ kpi: k, status: s })} />
                )
              }
              // Fallback: compact card when only key+pct available (shouldn't happen with enriched data)
              const wLabel = formatKpiLabel(wKey)
              const wInfo = KPI_INFO[wKey]
              const wPct = kpi.pct != null ? `${kpi.pct}%` : null
              return (
                <button
                  key={wKey}
                  onClick={() => setSlideOut({ kpi: { key: wKey, pct: kpi.pct }, status: 'amber' })}
                  className="w-full text-left card p-3.5 bg-amber-50 border-amber-200 hover:border-amber-300 hover:shadow-md transition-all group cursor-pointer"
                >
                  <div className="flex items-start justify-between gap-2 mb-1.5">
                    <p className="text-slate-800 text-[12px] font-semibold leading-tight flex-1">{wLabel}</p>
                    <ChevronRight size={12} className="text-slate-300 group-hover:text-slate-500 transition-colors flex-shrink-0" />
                  </div>
                  <div className="flex items-center gap-1.5 flex-wrap mb-1">
                    {wPct && (
                      <span className="text-amber-700 text-sm font-extrabold">{wPct}</span>
                    )}
                    <span className="text-amber-600 text-[10px] font-semibold">Watch Zone</span>
                  </div>
                  {wInfo?.why && (
                    <p className="text-slate-400 text-[10px] leading-snug line-clamp-2">{wInfo.why}</p>
                  )}
                </button>
              )
            })}
          </div>
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

      {/* ── No Target KPIs ────────────────────────────────────────────── */}
      {greyKpis.length > 0 && (needs_attention?.length > 0 || doing_well?.length > 0) && (
        <div>
          <div className="flex items-center gap-2 mb-2.5">
            <Target size={13} className="text-slate-400" />
            <h2 className="text-slate-700 text-[11px] font-bold uppercase tracking-wider">No Target</h2>
            <span className="bg-slate-100 text-slate-600 text-[10px] font-bold px-1.5 py-0.5 rounded-full">{greyKpis.length}</span>
          </div>
          <div className="bg-slate-50 rounded-xl p-3 space-y-1.5">
            <p className="text-[10px] text-slate-400 leading-snug px-2 pb-1">These KPIs cannot be scored without a target. Set targets in Settings to include them in your health score.</p>
            {greyKpis.map(kpi => {
              const gKey = kpi.key || kpi
              const gLabel = formatKpiLabel(gKey)
              const gInfo = KPI_INFO[gKey]
              const requirement = gInfo?.how || 'Target configuration required'
              return (
                <div key={gKey} className="flex items-start gap-2.5 px-2 py-1.5 bg-white/60 rounded-lg">
                  <div className="w-1.5 h-1.5 rounded-full bg-slate-300 flex-shrink-0 mt-1.5" />
                  <div className="flex-1 min-w-0">
                    <span className="text-[11px] font-semibold text-slate-600">{gLabel}</span>
                    <p className="text-[10px] text-slate-400 leading-snug">Requires: {requirement}</p>
                    <p className="text-[9px] text-slate-300 leading-snug mt-0.5">This KPI cannot be computed without the specific data inputs described above.</p>
                  </div>
                </div>
              )
            })}
            <button
              onClick={() => onNavigate?.('targets')}
              className="mt-1.5 text-[10px] font-semibold text-[#0055A4] hover:underline"
            >
              Configure targets to unlock scoring
            </button>
          </div>
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
        <ScoreBreakdownModal health={health} onClose={() => setShowScoreModal(false)} onWeightsApply={handleWeightsApply} />
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
