/**
 * Shared KPI value formatting utility.
 * Ensures consistent display of numbers + units across all components.
 *
 * Usage: import { fmtKpiValue } from './kpiFormat'
 *        fmtKpiValue(3.5, 'pct')   -> '3.5%'
 *        fmtKpiValue(5000, 'usd')  -> '$5,000'
 *        fmtKpiValue(45, 'days')   -> '45 days'
 *        fmtKpiValue(3.2, 'ratio') -> '3.2x'
 *        fmtKpiValue(null)         -> '--'
 */

const _round = (v, d = 1) => {
  const n = Number(v)
  return Number.isFinite(n) ? n.toFixed(d) : v
}

const UNIT_FMT = {
  pct:    v => `${_round(v)}%`,
  '%':    v => `${_round(v)}%`,
  usd:    v => `$${Number(Number(v).toFixed(0)).toLocaleString()}`,
  '$':    v => `$${Number(Number(v).toFixed(0)).toLocaleString()}`,
  days:   v => `${_round(v)} days`,
  months: v => `${_round(v)} mo`,
  ratio:  v => `${_round(v, 2)}x`,
  x:      v => `${_round(v, 2)}x`,
  score:  v => `${_round(v)}`,
  count:  v => Number(Number(v).toFixed(0)).toLocaleString(),
}

/**
 * Format a KPI value with its unit, ensuring no scrunched text.
 * @param {number|string|null} val - The raw KPI value
 * @param {string} [unit] - The unit type (pct, usd, days, months, ratio, score, count)
 * @returns {string} Formatted display string
 */
export function fmtKpiValue(val, unit) {
  if (val == null || val === '' || (typeof val === 'number' && !Number.isFinite(val))) {
    return '\u2014'
  }
  const formatter = UNIT_FMT[(unit || '').toLowerCase()]
  if (formatter) return formatter(val)
  // Unknown unit: just return the number rounded
  const n = Number(val)
  return Number.isFinite(n) ? _round(n) : String(val)
}

/**
 * Format a KPI value range (e.g., for benchmark p25-p75).
 */
export function fmtKpiRange(low, high, unit) {
  return `${fmtKpiValue(low, unit)} \u2013 ${fmtKpiValue(high, unit)}`
}

/**
 * Investor-grade compact formatting for board presentations.
 * Abbreviates large USD values (K, M, B), rounds percentages,
 * and uses presentation-standard notation.
 *
 * Usage: fmtKpiValueCompact(4217893, 'usd')  -> '$4.2M'
 *        fmtKpiValueCompact(4217, 'usd')     -> '$4.2K'
 *        fmtKpiValueCompact(3.456, 'pct')    -> '3.5%'
 */
const _compact_usd = v => {
  const n = Math.abs(Number(v))
  const sign = Number(v) < 0 ? '-' : ''
  if (n >= 1e9) return `${sign}$${(n / 1e9).toFixed(1)}B`
  if (n >= 1e6) return `${sign}$${(n / 1e6).toFixed(1)}M`
  if (n >= 1e3) return `${sign}$${(n / 1e3).toFixed(1)}K`
  return `${sign}$${n.toFixed(0)}`
}

const UNIT_FMT_COMPACT = {
  pct:    v => `${_round(v)}%`,
  '%':    v => `${_round(v)}%`,
  usd:    _compact_usd,
  '$':    _compact_usd,
  days:   v => `${Math.round(Number(v))} days`,
  months: v => `${_round(v)} mo`,
  ratio:  v => `${_round(v, 1)}x`,
  x:      v => `${_round(v, 1)}x`,
  score:  v => `${Math.round(Number(v))}`,
  count:  v => {
    const n = Math.abs(Number(v))
    const sign = Number(v) < 0 ? '-' : ''
    if (n >= 1e6) return `${sign}${(n / 1e6).toFixed(1)}M`
    if (n >= 1e3) return `${sign}${(n / 1e3).toFixed(1)}K`
    return `${sign}${n.toFixed(0)}`
  },
}

export function fmtKpiValueCompact(val, unit) {
  if (val == null || val === '' || (typeof val === 'number' && !Number.isFinite(val))) {
    return '\u2014'
  }
  const formatter = UNIT_FMT_COMPACT[(unit || '').toLowerCase()]
  if (formatter) return formatter(val)
  const n = Number(val)
  return Number.isFinite(n) ? _round(n) : String(val)
}

/**
 * GAAP classification badge text for a KPI.
 * Returns 'GAAP', 'Non-GAAP', or 'Operating' based on KPI metadata.
 */
export const GAAP_STATUS = {
  // GAAP-aligned metrics (ASC 606 revenue, GAAP margins, GAAP ratios)
  revenue_growth: 'gaap', gross_margin: 'gaap', operating_margin: 'gaap',
  ebitda_margin: 'gaap', contribution_margin: 'gaap', gross_profit: 'gaap',
  opex_ratio: 'gaap', operating_leverage: 'gaap', margin_volatility: 'gaap',
  // Non-GAAP operating metrics (SaaS-specific, not derived from GAAP statements)
  arr: 'non_gaap', mrr: 'non_gaap', arr_growth: 'non_gaap', nrr: 'non_gaap',
  burn_multiple: 'non_gaap', rule_of_40: 'non_gaap', magic_number: 'non_gaap',
  recurring_revenue: 'non_gaap', revenue_quality: 'non_gaap', arpu: 'non_gaap',
  growth_efficiency: 'non_gaap', revenue_momentum: 'non_gaap', burn_convexity: 'non_gaap',
  gross_dollar_ret: 'non_gaap', expansion_rate: 'non_gaap', contraction_rate: 'non_gaap',
  pricing_power_index: 'non_gaap', revenue_fragility: 'non_gaap', cash_burn: 'non_gaap',
  payback_period: 'non_gaap',
  // Operating / management metrics (KPIs used for ops, not financial reporting)
  churn_rate: 'operating', cac: 'operating', cac_payback: 'operating',
  customer_ltv: 'operating', ltv_cac: 'operating', sales_efficiency: 'operating',
  customer_concentration: 'operating', pipeline_conversion: 'operating',
  win_rate: 'operating', dso: 'operating', cash_conv_cycle: 'operating',
  cash_runway: 'operating', headcount_eff: 'operating', rev_per_employee: 'operating',
  billable_utilization: 'operating', product_nps: 'operating', csat: 'operating',
  customer_decay_slope: 'operating', logo_retention: 'operating',
  avg_collection_period: 'operating', ar_turnover: 'operating', cei: 'operating',
  ar_aging_current: 'operating', ar_aging_overdue: 'operating',
  cpl: 'operating', mql_sql_rate: 'operating', pipeline_velocity: 'operating',
  quota_attainment: 'operating', marketing_roi: 'operating',
  avg_deal_size: 'operating', feature_adoption: 'operating', activation_rate: 'operating',
  time_to_value: 'operating', health_score: 'operating', ramp_time: 'operating',
  support_volume: 'operating', automation_rate: 'operating',
  current_ratio: 'operating', working_capital: 'operating',
  organic_traffic: 'operating', brand_awareness: 'operating',
}

export function getGaapLabel(kpiKey) {
  const status = GAAP_STATUS[kpiKey]
  if (status === 'gaap') return 'GAAP'
  if (status === 'non_gaap') return 'Non-GAAP'
  return 'Operating'
}

export function getGaapColor(kpiKey) {
  const status = GAAP_STATUS[kpiKey]
  if (status === 'gaap') return 'text-blue-400 bg-blue-400/10'
  if (status === 'non_gaap') return 'text-gray-400 bg-gray-400/10'
  return 'text-slate-500 bg-slate-400/10'
}
