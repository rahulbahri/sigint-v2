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
