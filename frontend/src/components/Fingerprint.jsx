import { useState } from 'react'
import {
  RadarChart, PolarGrid, PolarAngleAxis, Radar,
  ResponsiveContainer, Tooltip, Legend
} from 'recharts'
import { ChevronRight, FileDown, GitCompare, Download, X } from 'lucide-react'

const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
const MONTH_NUMS = [1,2,3,4,5,6,7,8,9,10,11,12]

const UNIT_FMT = {
  pct:    v => `${v?.toFixed(1)}%`,
  days:   v => `${v?.toFixed(1)}d`,
  months: v => `${v?.toFixed(1)}mo`,
  ratio:  v => `${v?.toFixed(2)}x`,
  usd:    v => `$${v?.toFixed(1)}K`,
}
function fmt(val, unit) {
  if (val == null) return '—'
  return (UNIT_FMT[unit] || (v => v?.toFixed(2)))(val)
}

function vsTarget(val, target, direction) {
  if (!target) return 100
  const pct = (val / target) * 100
  if (direction === 'higher') return Math.min(pct, 135)
  return Math.max(200 - pct, 30)
}

function cellBg(s) {
  return { green: 'hm-green', yellow: 'hm-yellow', red: 'hm-red', grey: 'hm-grey' }[s] || 'hm-grey'
}

function badgeCls(s) {
  return {
    green:  'badge-green',
    yellow: 'badge-yellow',
    red:    'badge-red',
    grey:   'badge-grey',
  }[s] || 'badge-grey'
}

function cellStatus(val, target, direction) {
  if (val == null || !target) return 'grey'
  const pct = val / target
  if (direction === 'higher') return pct >= 0.98 ? 'green' : pct >= 0.90 ? 'yellow' : 'red'
  return pct <= 1.02 ? 'green' : pct <= 1.10 ? 'yellow' : 'red'
}

// Year-aware red streak — walks sorted period list backwards
function redStreak(kpi, sortedPeriods) {
  const byP = {}
  kpi.monthly?.forEach(m => { byP[m.period] = m.value })
  const periods = sortedPeriods?.length
    ? sortedPeriods
    : (kpi.monthly?.map(m => m.period).sort() || [])
  let streak = 0
  for (let i = periods.length - 1; i >= 0; i--) {
    if (cellStatus(byP[periods[i]], kpi.target, kpi.direction) === 'red') streak++
    else break
  }
  return streak
}

// Average for any arbitrary set of month numbers (1–12)
function periodAvg(kpi, monthNums) {
  if (!kpi.monthly?.length || !monthNums?.length) return kpi.avg
  const hits = kpi.monthly.filter(m => {
    const mo = parseInt(m.period.split('-')[1], 10)
    return monthNums.includes(mo) && m.value != null
  })
  if (!hits.length) return kpi.avg
  return hits.reduce((s, m) => s + m.value, 0) / hits.length
}

// Human-readable label for selected months
function periodLabel(monthNums) {
  if (!monthNums?.length) return 'None'
  if (monthNums.length === 12) return 'Full Year'
  const sorted = [...monthNums].sort((a, b) => a - b)
  // Detect common presets
  const key = sorted.join(',')
  if (key === '1,2,3,4,5,6') return 'H1'
  if (key === '7,8,9,10,11,12') return 'H2'
  if (key === '1,2,3') return 'Q1'
  if (key === '4,5,6') return 'Q2'
  if (key === '7,8,9') return 'Q3'
  if (key === '10,11,12') return 'Q4'
  if (sorted.length <= 3) return sorted.map(n => MONTHS[n - 1]).join(', ')
  return `${sorted.length} months`
}

// ── Board pack ─────────────────────────────────────────────────────────────
function generateBoardPack(fingerprint) {
  const now    = new Date()
  const dateStr = now.toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' })
  const green  = fingerprint.filter(k => k.fy_status === 'green').length
  const yellow = fingerprint.filter(k => k.fy_status === 'yellow').length
  const red    = fingerprint.filter(k => k.fy_status === 'red').length
  const bhi    = Math.round((green * 100 + yellow * 60) / Math.max(green + yellow + red, 1))

  const rows = fingerprint.map(k => {
    const byMonth = {}
    k.monthly?.forEach(m => { byMonth[parseInt(m.period.split('-')[1], 10)] = m.value })
    const lastMo  = [12,11,10,9].find(n => byMonth[n] != null)
    const prevMo  = lastMo ? [lastMo - 1, lastMo - 2].find(n => byMonth[n] != null) : null
    const lastVal = lastMo ? byMonth[lastMo] : null
    const prevVal = prevMo ? byMonth[prevMo] : null
    const trend   = lastVal != null && prevVal != null
      ? (lastVal > prevVal ? '▲' : lastVal < prevVal ? '▼' : '→') : '—'
    const trendGood = trend === '▲'
      ? k.direction === 'higher'
      : trend === '▼' ? k.direction !== 'higher' : null
    const trendColor = trendGood === true ? '#16a34a' : trendGood === false ? '#dc2626' : '#94a3b8'
    const stColor = { green: '#16a34a', yellow: '#d97706', red: '#dc2626' }[k.fy_status] || '#94a3b8'
    return `<tr>
      <td>${k.name}</td>
      <td style="text-align:right">${fmt(k.target, k.unit)}</td>
      <td style="text-align:right">${fmt(k.avg, k.unit)}</td>
      <td style="text-align:right">${fmt(lastVal, k.unit)}</td>
      <td style="color:${stColor};font-weight:700;text-align:center">${k.fy_status?.toUpperCase() || '—'}</td>
      <td style="color:${trendColor};font-weight:700;text-align:center">${trend}</td>
    </tr>`
  }).join('')

  const bhiColor = bhi >= 70 ? '#16a34a' : bhi >= 50 ? '#d97706' : '#dc2626'

  const html = `<!DOCTYPE html>
<html><head><title>Board Performance Pack — FY 2025</title>
<meta charset="utf-8"/>
<style>
  * { box-sizing: border-box; }
  body { font-family: -apple-system, Helvetica Neue, Arial, sans-serif; color: #1e293b; padding: 48px; max-width: 860px; margin: 0 auto; font-size: 13px; }
  h1  { font-size: 20px; font-weight: 800; color: #0055A4; margin: 0 0 4px; }
  .sub { color: #64748b; font-size: 12px; margin-bottom: 28px; }
  .summary { display: flex; gap: 0; margin-bottom: 28px; border: 1px solid #e2e8f0; border-radius: 10px; overflow: hidden; }
  .s-item { flex: 1; padding: 16px 20px; text-align: center; border-right: 1px solid #e2e8f0; }
  .s-item:last-child { border-right: none; }
  .s-num  { font-size: 30px; font-weight: 900; line-height: 1; }
  .s-lbl  { font-size: 10px; color: #64748b; text-transform: uppercase; letter-spacing: 0.06em; margin-top: 4px; }
  table { width: 100%; border-collapse: collapse; margin-top: 4px; }
  thead th { background: #f1f5f9; padding: 8px 12px; color: #64748b; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; border-bottom: 1px solid #e2e8f0; }
  thead th:first-child { text-align: left; }
  tbody td { padding: 8px 12px; border-bottom: 1px solid #f8fafc; }
  tbody tr:last-child td { border-bottom: none; }
  .footer { margin-top: 28px; color: #94a3b8; font-size: 10px; border-top: 1px solid #e2e8f0; padding-top: 14px; }
  @media print { body { padding: 24px; } .no-print { display: none; } }
</style>
</head><body>
<h1>FY 2025 — Board Performance Pack</h1>
<div class="sub">Signals Intelligence · Prepared ${dateStr}</div>
<div class="summary">
  <div class="s-item"><div class="s-num" style="color:${bhiColor}">${bhi}</div><div class="s-lbl">Business Health Index</div></div>
  <div class="s-item"><div class="s-num" style="color:#16a34a">${green}</div><div class="s-lbl">On Target</div></div>
  <div class="s-item"><div class="s-num" style="color:#d97706">${yellow}</div><div class="s-lbl">Needs Attention</div></div>
  <div class="s-item"><div class="s-num" style="color:#dc2626">${red}</div><div class="s-lbl">Critical</div></div>
  <div class="s-item"><div class="s-num" style="color:#475569">${fingerprint.length}</div><div class="s-lbl">Total KPIs</div></div>
</div>
<table>
  <thead><tr>
    <th>KPI</th>
    <th style="text-align:right">Target</th>
    <th style="text-align:right">FY Average</th>
    <th style="text-align:right">Latest</th>
    <th style="text-align:center">Status</th>
    <th style="text-align:center">Trend</th>
  </tr></thead>
  <tbody>${rows}</tbody>
</table>
<div class="footer">Confidential — For board distribution only &nbsp;·&nbsp; Signals Intelligence Platform &nbsp;·&nbsp; ${dateStr}</div>
<div class="no-print" style="margin-top:24px;text-align:center">
  <button onclick="window.print()" style="padding:8px 24px;background:#0055A4;color:#fff;border:none;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer">
    Print / Save as PDF
  </button>
</div>
</body></html>`

  const w = window.open('', '_blank')
  w.document.write(html)
  w.document.close()
  setTimeout(() => w.focus(), 300)
}

// ── Presentation (multi-slide HTML deck, print to PDF) ─────────────────────
function generatePresentation(fingerprint, periodA, periodB) {
  const now     = new Date()
  const dateStr = now.toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' })
  const green   = fingerprint.filter(k => k.fy_status === 'green').length
  const yellow  = fingerprint.filter(k => k.fy_status === 'yellow').length
  const red     = fingerprint.filter(k => k.fy_status === 'red').length
  const bhi     = Math.round((green * 100 + yellow * 60) / Math.max(green + yellow + red, 1))
  const bhiColor = bhi >= 70 ? '#16a34a' : bhi >= 50 ? '#d97706' : '#dc2626'

  const pALabel = periodLabel(periodA)
  const pBLabel = periodLabel(periodB)
  const compareNote = (periodA?.length && periodB?.length)
    ? `Period comparison: <strong>${pALabel}</strong> vs <strong>${pBLabel}</strong>`
    : ''

  // Slide 1 — Title + BHI
  const slide1 = `
    <div class="slide">
      <div class="slide-header"><span class="logo">Signals Intelligence</span><span class="date">${dateStr}</span></div>
      <div class="slide-body center">
        <div class="bhi-ring" style="border-color:${bhiColor}">
          <div class="bhi-num" style="color:${bhiColor}">${bhi}</div>
          <div class="bhi-lbl">Business Health Index</div>
        </div>
        <h1 class="slide-title">FY 2025 Performance Review</h1>
        <p class="slide-sub">KPI Fingerprint &amp; Trend Analysis${compareNote ? ' · ' + compareNote : ''}</p>
        <div class="pill-row">
          <span class="pill green">${green} On Target</span>
          <span class="pill yellow">${yellow} Watch</span>
          <span class="pill red">${red} Critical</span>
          <span class="pill grey">${fingerprint.length} Total KPIs</span>
        </div>
      </div>
      <div class="slide-footer">Confidential — Not for distribution</div>
    </div>`

  // Slide 2 — Full KPI scorecard table
  const tableRows = fingerprint.map(k => {
    const stColor = { green: '#16a34a', yellow: '#d97706', red: '#dc2626' }[k.fy_status] || '#94a3b8'
    const aVal = (periodA?.length) ? periodAvg(k, periodA) : null
    const bVal = (periodB?.length) ? periodAvg(k, periodB) : null
    const delta = (aVal != null && bVal != null) ? aVal - bVal : null
    const deltaFmt = delta != null
      ? `<span style="color:${delta > 0 && k.direction === 'higher' ? '#16a34a' : delta < 0 && k.direction !== 'higher' ? '#16a34a' : delta === 0 ? '#94a3b8' : '#dc2626'}">${delta > 0 ? '▲' : delta < 0 ? '▼' : '→'} ${fmt(Math.abs(delta), k.unit)}</span>`
      : '—'
    return `<tr>
      <td>${k.name}</td>
      <td style="text-align:right">${fmt(k.target, k.unit)}</td>
      <td style="text-align:right">${fmt(k.avg, k.unit)}</td>
      ${aVal != null ? `<td style="text-align:right">${fmt(aVal, k.unit)}</td>` : ''}
      ${bVal != null ? `<td style="text-align:right">${fmt(bVal, k.unit)}</td>` : ''}
      ${delta != null ? `<td style="text-align:center">${deltaFmt}</td>` : ''}
      <td style="color:${stColor};font-weight:700;text-align:center">${k.fy_status?.toUpperCase() || '—'}</td>
    </tr>`
  }).join('')

  const extraCols = (periodA?.length && periodB?.length)
    ? `<th style="text-align:right">${pALabel}</th><th style="text-align:right">${pBLabel}</th><th style="text-align:center">Δ</th>`
    : ''

  const slide2 = `
    <div class="slide">
      <div class="slide-header"><span class="logo">Signals Intelligence</span><span class="date">${dateStr}</span></div>
      <div class="slide-body">
        <h2 class="section-title">Full KPI Scorecard</h2>
        <table>
          <thead><tr>
            <th>KPI</th><th style="text-align:right">Target</th><th style="text-align:right">FY Avg</th>
            ${extraCols}
            <th style="text-align:center">Status</th>
          </tr></thead>
          <tbody>${tableRows}</tbody>
        </table>
      </div>
      <div class="slide-footer">Slide 2 of 4 — Confidential</div>
    </div>`

  // Slide 3 — Critical & Watch items
  const atRisk = fingerprint.filter(k => k.fy_status === 'red' || k.fy_status === 'yellow')
  const riskCards = atRisk.map(k => {
    const stColor = k.fy_status === 'red' ? '#dc2626' : '#d97706'
    const stBg    = k.fy_status === 'red' ? '#fef2f2' : '#fffbeb'
    const stBorder = k.fy_status === 'red' ? '#fca5a5' : '#fcd34d'
    const gap = k.target ? ((k.avg / k.target - 1) * 100).toFixed(1) : null
    return `
      <div class="kpi-card" style="border-color:${stBorder};background:${stBg}">
        <div class="kpi-card-header">
          <span class="kpi-name">${k.name}</span>
          <span class="kpi-badge" style="color:${stColor}">${k.fy_status?.toUpperCase()}</span>
        </div>
        <div class="kpi-stats">
          <span>Avg: <strong>${fmt(k.avg, k.unit)}</strong></span>
          <span>Target: <strong>${fmt(k.target, k.unit)}</strong></span>
          ${gap != null ? `<span style="color:${stColor}">Gap: ${gap > 0 ? '+' : ''}${gap}%</span>` : ''}
        </div>
      </div>`
  }).join('')

  const slide3 = `
    <div class="slide">
      <div class="slide-header"><span class="logo">Signals Intelligence</span><span class="date">${dateStr}</span></div>
      <div class="slide-body">
        <h2 class="section-title">Critical &amp; Watch — Items Requiring Action</h2>
        ${atRisk.length ? `<div class="card-grid">${riskCards}</div>` : '<p style="color:#64748b;margin-top:24px">No critical or watch KPIs — all metrics on target.</p>'}
      </div>
      <div class="slide-footer">Slide 3 of 4 — Confidential</div>
    </div>`

  // Slide 4 — Strong performers
  const strong = fingerprint.filter(k => k.fy_status === 'green')
  const strongCards = strong.map(k => {
    const gap = k.target ? ((k.avg / k.target - 1) * 100).toFixed(1) : null
    const gapSign = gap != null ? (k.direction === 'higher' ? (gap >= 0 ? '+' : '') + gap + '%' : (gap <= 0 ? '' : '+') + gap + '%') : null
    return `
      <div class="kpi-card" style="border-color:#86efac;background:#f0fdf4">
        <div class="kpi-card-header">
          <span class="kpi-name">${k.name}</span>
          <span class="kpi-badge" style="color:#16a34a">ON TARGET</span>
        </div>
        <div class="kpi-stats">
          <span>Avg: <strong>${fmt(k.avg, k.unit)}</strong></span>
          <span>Target: <strong>${fmt(k.target, k.unit)}</strong></span>
          ${gapSign != null ? `<span style="color:#16a34a">${gapSign} vs target</span>` : ''}
        </div>
      </div>`
  }).join('')

  const slide4 = `
    <div class="slide">
      <div class="slide-header"><span class="logo">Signals Intelligence</span><span class="date">${dateStr}</span></div>
      <div class="slide-body">
        <h2 class="section-title">Strong Performers — On or Above Target</h2>
        ${strong.length ? `<div class="card-grid">${strongCards}</div>` : '<p style="color:#64748b;margin-top:24px">No green KPIs currently.</p>'}
      </div>
      <div class="slide-footer">Slide 4 of 4 — Confidential &nbsp;·&nbsp; Signals Intelligence Platform</div>
    </div>`

  const html = `<!DOCTYPE html>
<html><head><title>FY 2025 — KPI Performance Presentation</title>
<meta charset="utf-8"/>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, Helvetica Neue, Arial, sans-serif; background: #f1f5f9; }
  .slide {
    width: 297mm; min-height: 210mm; background: #fff;
    display: flex; flex-direction: column; margin: 0 auto 12px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.10); page-break-after: always;
  }
  .slide-header {
    display: flex; justify-content: space-between; align-items: center;
    padding: 14px 32px; background: #0055A4; color: #fff;
  }
  .logo { font-size: 13px; font-weight: 700; letter-spacing: 0.04em; }
  .date { font-size: 11px; opacity: 0.8; }
  .slide-body { flex: 1; padding: 28px 32px; overflow: hidden; }
  .slide-footer {
    padding: 10px 32px; background: #f8fafc; border-top: 1px solid #e2e8f0;
    font-size: 10px; color: #94a3b8; text-align: right;
  }
  .center { display: flex; flex-direction: column; align-items: center; justify-content: center; text-align: center; }
  .bhi-ring {
    width: 120px; height: 120px; border-radius: 50%; border: 6px solid #16a34a;
    display: flex; flex-direction: column; align-items: center; justify-content: center;
    margin-bottom: 20px;
  }
  .bhi-num { font-size: 38px; font-weight: 900; line-height: 1; }
  .bhi-lbl { font-size: 10px; color: #64748b; text-transform: uppercase; letter-spacing: 0.06em; margin-top: 2px; }
  .slide-title { font-size: 26px; font-weight: 800; color: #0f172a; margin-bottom: 6px; }
  .slide-sub { font-size: 13px; color: #64748b; margin-bottom: 20px; }
  .pill-row { display: flex; gap: 8px; flex-wrap: wrap; justify-content: center; }
  .pill { padding: 4px 14px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .pill.green  { background: #dcfce7; color: #16a34a; }
  .pill.yellow { background: #fef9c3; color: #a16207; }
  .pill.red    { background: #fee2e2; color: #dc2626; }
  .pill.grey   { background: #f1f5f9; color: #475569; }
  .section-title { font-size: 16px; font-weight: 700; color: #0055A4; margin-bottom: 16px; border-bottom: 2px solid #e2e8f0; padding-bottom: 8px; }
  table { width: 100%; border-collapse: collapse; font-size: 11px; }
  thead th { background: #f1f5f9; padding: 7px 10px; color: #64748b; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; border-bottom: 1px solid #e2e8f0; text-align: left; }
  tbody td { padding: 7px 10px; border-bottom: 1px solid #f8fafc; color: #1e293b; }
  .card-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-top: 4px; }
  .kpi-card { border: 1px solid #e2e8f0; border-radius: 8px; padding: 12px 14px; }
  .kpi-card-header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 8px; }
  .kpi-name { font-size: 12px; font-weight: 600; color: #1e293b; }
  .kpi-badge { font-size: 10px; font-weight: 700; flex-shrink: 0; margin-left: 8px; }
  .kpi-stats { display: flex; gap: 12px; font-size: 11px; color: #64748b; flex-wrap: wrap; }
  @media print {
    body { background: white; }
    .no-print { display: none; }
    .slide { box-shadow: none; margin: 0; page-break-after: always; }
  }
</style>
</head>
<body>
${slide1}
${slide2}
${slide3}
${slide4}
<div class="no-print" style="text-align:center;padding:24px">
  <button onclick="window.print()" style="padding:10px 28px;background:#0055A4;color:#fff;border:none;border-radius:8px;font-size:14px;font-weight:700;cursor:pointer">
    🖨 Print / Save as PDF
  </button>
</div>
</body></html>`

  const w = window.open('', '_blank')
  w.document.write(html)
  w.document.close()
  setTimeout(() => w.focus(), 300)
}

// ── CSV download ────────────────────────────────────────────────────────────
function downloadCSV(fingerprint) {
  const headers = ['KPI', 'Key', 'Unit', 'Direction', 'Target', ...MONTHS, 'FY Average', 'Status']
  const rows = fingerprint.map(k => {
    const byMonth = {}
    k.monthly?.forEach(m => { byMonth[parseInt(m.period.split('-')[1], 10)] = m.value })
    const monthVals = MONTH_NUMS.map(mo => byMonth[mo] != null ? byMonth[mo] : '')
    return [
      `"${k.name}"`,
      k.key,
      k.unit || '',
      k.direction || '',
      k.target != null ? k.target : '',
      ...monthVals,
      k.avg != null ? k.avg : '',
      k.fy_status || '',
    ]
  })
  const csv = [headers, ...rows].map(r => r.join(',')).join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url  = URL.createObjectURL(blob)
  const a    = document.createElement('a')
  a.href     = url
  a.download = `kpi-fingerprint-fy2025-${new Date().toISOString().slice(0,10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

// ── Period Picker Panel ─────────────────────────────────────────────────────
const PRESETS = [
  { label: 'H1',  a: [1,2,3,4,5,6],       b: [7,8,9,10,11,12] },
  { label: 'H2',  a: [7,8,9,10,11,12],     b: [1,2,3,4,5,6]    },
  { label: 'Q1v3',a: [1,2,3],             b: [7,8,9]           },
  { label: 'Q2v4',a: [4,5,6],             b: [10,11,12]        },
  { label: 'Last 3 vs Prior 3', a: [10,11,12], b: [7,8,9]      },
]

function PeriodPicker({ periodA, periodB, onChange, onClose }) {
  function toggleMonth(period, mo) {
    const current = period === 'A' ? periodA : periodB
    const next = current.includes(mo) ? current.filter(x => x !== mo) : [...current, mo]
    onChange(period, next)
  }

  function applyPreset(preset) {
    onChange('A', preset.a)
    onChange('B', preset.b)
  }

  return (
    <div className="mt-3 p-4 bg-slate-50 border border-slate-200 rounded-xl space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-xs font-semibold text-slate-600 uppercase tracking-wider">Custom Period Comparison</p>
        <button onClick={onClose} className="text-slate-400 hover:text-slate-600">
          <X size={14}/>
        </button>
      </div>

      {/* Presets */}
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-[11px] text-slate-500 font-medium">Presets:</span>
        {PRESETS.map(p => (
          <button
            key={p.label}
            onClick={() => applyPreset(p)}
            className="px-2.5 py-1 text-[11px] font-medium bg-white border border-slate-200 rounded-full hover:border-[#0055A4] hover:text-[#0055A4] transition-colors">
            {p.label}
          </button>
        ))}
      </div>

      {/* Period A — blue */}
      <div>
        <p className="text-[11px] font-semibold text-blue-600 mb-1.5">
          Period A (blue) — {periodLabel(periodA)}
          {periodA.length > 0 && (
            <button onClick={() => onChange('A', [])} className="ml-2 text-[10px] text-slate-400 hover:text-slate-600">clear</button>
          )}
        </p>
        <div className="flex flex-wrap gap-1.5">
          {MONTH_NUMS.map(mo => (
            <button
              key={mo}
              onClick={() => toggleMonth('A', mo)}
              className={`w-9 h-7 text-[11px] font-medium rounded transition-all ${
                periodA.includes(mo)
                  ? 'bg-blue-600 text-white border border-blue-600'
                  : 'bg-white text-slate-500 border border-slate-200 hover:border-blue-300'
              }`}>
              {MONTHS[mo - 1]}
            </button>
          ))}
        </div>
      </div>

      {/* Period B — amber */}
      <div>
        <p className="text-[11px] font-semibold text-amber-600 mb-1.5">
          Period B (amber) — {periodLabel(periodB)}
          {periodB.length > 0 && (
            <button onClick={() => onChange('B', [])} className="ml-2 text-[10px] text-slate-400 hover:text-slate-600">clear</button>
          )}
        </p>
        <div className="flex flex-wrap gap-1.5">
          {MONTH_NUMS.map(mo => (
            <button
              key={mo}
              onClick={() => toggleMonth('B', mo)}
              className={`w-9 h-7 text-[11px] font-medium rounded transition-all ${
                periodB.includes(mo)
                  ? 'bg-amber-500 text-white border border-amber-500'
                  : 'bg-white text-slate-500 border border-slate-200 hover:border-amber-300'
              }`}>
              {MONTHS[mo - 1]}
            </button>
          ))}
        </div>
      </div>
    </div>
  )
}

// ── Main component ─────────────────────────────────────────────────────────
export default function Fingerprint({ fingerprint, onKpiClick }) {
  const [showCompare,  setShowCompare]  = useState(false)
  const [showPicker,   setShowPicker]   = useState(false)
  const [periodA,      setPeriodA]      = useState([7,8,9,10,11,12])   // H2 default
  const [periodB,      setPeriodB]      = useState([1,2,3,4,5,6])      // H1 default
  const [showDelta,    setShowDelta]    = useState(false)

  function handlePeriodChange(which, months) {
    if (which === 'A') setPeriodA(months)
    else setPeriodB(months)
  }

  if (!fingerprint?.length) return null

  const hasA = showCompare && periodA.length > 0
  const hasB = showCompare && periodB.length > 0

  // Radar data
  const radarData = fingerprint
    .filter(k => k.avg != null && k.target != null)
    .slice(0, 12)
    .map(k => {
      const aAvg = periodAvg(k, periodA)
      const bAvg = periodAvg(k, periodB)
      return {
        kpi:    k.name.length > 20 ? k.name.slice(0, 18) + '…' : k.name,
        actual: Math.min(vsTarget(hasA ? aAvg : k.avg, k.target, k.direction), 135),
        target: 100,
        prior:  hasB ? Math.min(vsTarget(bAvg, k.target, k.direction), 135) : undefined,
      }
    })

  const heat = fingerprint.filter(k => k.monthly?.length)

  // ── Dynamic year-month column list ─────────────────────────────────────
  // Collect every "YYYY-MM" period present in the data, sorted chronologically
  const allPeriods = [...new Set(
    heat.flatMap(k => k.monthly.map(m => m.period))
  )].sort()
  const allYears   = [...new Set(allPeriods.map(p => p.slice(0, 4)))].sort()
  const isMultiYear = allYears.length > 1

  // Label a period column: "Jan" (single year) or "Jan'21" (multi-year)
  function colLabel(period) {
    const [yr, mo] = period.split('-')
    const mName = MONTHS[parseInt(mo, 10) - 1]
    return isMultiYear ? `${mName}'${yr.slice(2)}` : mName
  }

  // Number of period columns that belong to each year (for the year-band header)
  const yearSpans = allYears.map(yr => ({
    yr,
    count: allPeriods.filter(p => p.startsWith(yr)).length,
  }))

  return (
    <div className="space-y-6">

      {/* ── Radar ──────────────────────────────────────────────────────── */}
      <div className="card p-6">
        <div className="flex items-center justify-between mb-1">
          <div>
            <h3 className="text-sm font-semibold text-slate-700">Performance Radar — % of Target</h3>
            <p className="text-xs text-slate-400 mt-0.5">
              100 = on target · Outward = outperforming · Inward = gap
              {showCompare && hasA && hasB && (
                <span className="ml-2 text-amber-600 font-medium">
                  · <span className="text-blue-600">{periodLabel(periodA)}</span> vs <span className="text-amber-600">{periodLabel(periodB)}</span>
                </span>
              )}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => { setShowCompare(c => !c); setShowPicker(false) }}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border transition-all ${
                showCompare
                  ? 'bg-amber-50 text-amber-700 border-amber-300'
                  : 'bg-slate-50 text-slate-500 border-slate-200 hover:border-slate-300'
              }`}>
              <GitCompare size={12}/>
              {showCompare ? `${periodLabel(periodA)} vs ${periodLabel(periodB)}` : 'Compare Periods'}
            </button>
            {showCompare && (
              <button
                onClick={() => setShowPicker(p => !p)}
                className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-all ${
                  showPicker
                    ? 'bg-blue-50 text-blue-700 border-blue-300'
                    : 'bg-slate-50 text-slate-500 border-slate-200 hover:border-slate-300'
                }`}>
                {showPicker ? 'Close Picker' : 'Select Months…'}
              </button>
            )}
          </div>
        </div>

        {/* Period picker panel */}
        {showCompare && showPicker && (
          <PeriodPicker
            periodA={periodA}
            periodB={periodB}
            onChange={handlePeriodChange}
            onClose={() => setShowPicker(false)}
          />
        )}

        <ResponsiveContainer width="100%" height={380}>
          <RadarChart data={radarData} margin={{ top: 10, right: 30, bottom: 10, left: 30 }}>
            <PolarGrid stroke="#e2e8f0"/>
            <PolarAngleAxis dataKey="kpi" tick={{ fill: '#64748b', fontSize: 10 }}/>
            {/* Target ring — darker stroke, no fill */}
            <Radar name="Target (100%)" dataKey="target"
              stroke="#94a3b8" strokeWidth={1.5} strokeDasharray="5 3"
              fill="none"/>
            {/* Period B — amber, rendered first so A sits on top */}
            {hasB && (
              <Radar name={`${periodLabel(periodB)} (Period B)`} dataKey="prior"
                stroke="#f59e0b" fill="#f59e0b" fillOpacity={0.08}
                strokeWidth={1.5} strokeDasharray="6 4"/>
            )}
            {/* Period A / actual */}
            <Radar name={showCompare && hasA ? `${periodLabel(periodA)} (Period A)` : 'Actual'} dataKey="actual"
              stroke="#0055A4" fill="#0055A4" fillOpacity={0.18}
              strokeWidth={2} dot={{ fill: '#0055A4', r: 3 }}/>
            <Tooltip
              contentStyle={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, fontSize: 11, color: '#0f172a' }}
              formatter={(v, n) => [`${v?.toFixed(1)}% of target`, n]}/>
            <Legend wrapperStyle={{ fontSize: 11, color: '#64748b' }}/>
          </RadarChart>
        </ResponsiveContainer>
      </div>

      {/* ── Heatmap Narrative Card ─────────────────────────────────────── */}
      {(() => {
        const red    = fingerprint.filter(k => k.fy_status === 'red')
        const yellow = fingerprint.filter(k => k.fy_status === 'yellow')
        const green  = fingerprint.filter(k => k.fy_status === 'green')
        const total  = fingerprint.length

        // Domain breakdown
        const domainMap = {}
        fingerprint.forEach(k => {
          const d = k.domain || 'Other'
          if (!domainMap[d]) domainMap[d] = { red: 0, yellow: 0, green: 0 }
          if (k.fy_status === 'red')    domainMap[d].red++
          else if (k.fy_status === 'yellow') domainMap[d].yellow++
          else if (k.fy_status === 'green')  domainMap[d].green++
        })
        const domains = Object.entries(domainMap)
          .map(([name, counts]) => ({ name, ...counts, score: counts.red * 3 + counts.yellow }))
          .sort((a, b) => b.score - a.score)
        const worstDomain  = domains[0]
        const strongDomain = [...domains].sort((a, b) => b.green - a.green)[0]

        // Longest red streaker (year-aware)
        const streakers = fingerprint
          .map(k => ({ k, streak: redStreak(k, allPeriods) }))
          .filter(x => x.streak >= 3)
          .sort((a, b) => b.streak - a.streak)
        const topStreaker = streakers[0]

        // Best green KPI (highest vs target %)
        const bestGreen = green.length
          ? green.map(k => ({ k, pct: k.avg != null && k.target ? k.avg / k.target : 0 }))
              .sort((a, b) => (k => k.direction === 'higher' ? b.pct - a.pct : a.pct - b.pct)(b.k))
              .sort((a, b) => b.pct - a.pct)[0]?.k
          : null

        const statusBg = red.length > total * 0.4
          ? { bg: '#fef2f2', border: '#fca5a5', dot: '#dc2626', label: 'CRITICAL ATTENTION REQUIRED' }
          : red.length > total * 0.2
          ? { bg: '#fffbeb', border: '#fcd34d', dot: '#d97706', label: 'NEEDS ATTENTION' }
          : { bg: '#f0fdf4', border: '#86efac', dot: '#059669', label: 'BROADLY ON TRACK' }

        const sentences = []
        sentences.push(
          `${total} KPI${total !== 1 ? 's' : ''} tracked this period — ` +
          (red.length    ? `${red.length} critical` + (yellow.length || green.length ? ', ' : '') : '') +
          (yellow.length ? `${yellow.length} need${yellow.length === 1 ? 's' : ''} attention` + (green.length ? ', ' : '') : '') +
          (green.length  ? `${green.length} on or above target` : '') + '.'
        )
        if (worstDomain && worstDomain.red + worstDomain.yellow > 0) {
          sentences.push(
            `${worstDomain.name.replace(/_/g,' ')} is the most pressured domain with ${worstDomain.red} critical and ${worstDomain.yellow} watch KPI${worstDomain.yellow !== 1 ? 's' : ''}.`
          )
        }
        if (topStreaker) {
          sentences.push(
            `${topStreaker.k.name} has been below target for ${topStreaker.streak} consecutive months — immediate review recommended.`
          )
        }
        if (strongDomain && strongDomain.green >= 2) {
          sentences.push(
            `${strongDomain.name.replace(/_/g,' ')} is the strongest domain, with ${strongDomain.green} KPI${strongDomain.green !== 1 ? 's' : ''} performing on or above target.`
          )
        }
        if (red.length === 0 && yellow.length === 0) {
          sentences.push('All KPIs are on or above target — focus on sustaining performance and monitoring leading indicators.')
        }

        return (
          <div className="rounded-xl border p-5" style={{ background: statusBg.bg, borderColor: statusBg.border }}>
            <div className="flex items-start gap-3">
              <span className="w-2.5 h-2.5 rounded-full flex-shrink-0 mt-1" style={{ background: statusBg.dot }}/>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-2">
                  <p className="text-[10px] font-bold uppercase tracking-widest" style={{ color: statusBg.dot }}>
                    {statusBg.label}
                  </p>
                  <div className="flex items-center gap-1.5">
                    {red.length > 0 && (
                      <span className="inline-flex items-center gap-1 text-[10px] font-semibold bg-red-100 text-red-700 border border-red-200 px-2 py-0.5 rounded-full">
                        {red.length} Critical
                      </span>
                    )}
                    {yellow.length > 0 && (
                      <span className="inline-flex items-center gap-1 text-[10px] font-semibold bg-amber-100 text-amber-700 border border-amber-200 px-2 py-0.5 rounded-full">
                        {yellow.length} Watch
                      </span>
                    )}
                    {green.length > 0 && (
                      <span className="inline-flex items-center gap-1 text-[10px] font-semibold bg-emerald-100 text-emerald-700 border border-emerald-200 px-2 py-0.5 rounded-full">
                        {green.length} On Target
                      </span>
                    )}
                  </div>
                </div>
                <p className="text-xs text-slate-700 leading-relaxed">
                  {sentences.join(' ')}
                </p>
              </div>
            </div>
          </div>
        )
      })()}

      {/* ── Heat map ───────────────────────────────────────────────────── */}
      <div className="card p-6 overflow-x-auto">
        <div className="flex items-center justify-between mb-1">
          <div>
            <h3 className="text-sm font-semibold text-slate-700">
              KPI Heat Map
              {allPeriods.length > 0 && (
                <span className="ml-2 text-slate-400 font-normal text-xs">
                  — {isMultiYear
                    ? `${allYears[0]}–${allYears[allYears.length - 1]} · ${allPeriods.length} months`
                    : `${allYears[0]} · ${allPeriods.length} months`}
                </span>
              )}
            </h3>
            <p className="text-xs text-slate-400 mt-0.5">
              Cell colour = performance vs target · Click any row for deep-dive
              {showDelta && <span className="ml-2 text-blue-600 font-medium">· Showing Δ vs prior period</span>}
            </p>
          </div>
          <div className="flex items-center gap-2 flex-shrink-0">
            <button
              onClick={() => setShowDelta(d => !d)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border transition-all ${
                showDelta
                  ? 'bg-blue-50 text-blue-700 border-blue-300'
                  : 'bg-slate-50 text-slate-500 border-slate-200 hover:border-slate-300'
              }`}>
              <GitCompare size={12}/>
              Δ Prior Month
            </button>
            <button
              onClick={() => downloadCSV(fingerprint)}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold bg-slate-700 text-white border border-slate-700 hover:bg-slate-800 transition-all">
              <Download size={12}/>
              Download Data
            </button>
            <button
              onClick={() => generatePresentation(fingerprint, showCompare ? periodA : [], showCompare ? periodB : [])}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold bg-emerald-600 text-white border border-emerald-600 hover:bg-emerald-700 transition-all">
              <FileDown size={12}/>
              Presentation
            </button>
            <button
              onClick={() => generateBoardPack(fingerprint)}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold bg-[#0055A4] text-white border border-[#0055A4] hover:bg-[#0044a0] transition-all">
              <FileDown size={12}/>
              Board Pack
            </button>
          </div>
        </div>

        <table className="w-full text-xs border-collapse mt-4">
          <thead>
            {/* Year-band row — only shown when multiple years are in scope */}
            {isMultiYear && (
              <tr>
                <th colSpan={2}/>
                {yearSpans.map(({ yr, count }, yi) => (
                  <th
                    key={yr}
                    colSpan={count}
                    className={`text-center text-[10px] font-bold text-blue-700 bg-blue-50 py-1 border-b-2 border-blue-200 ${
                      yi > 0 ? 'border-l-2 border-blue-300' : ''
                    }`}>
                    {yr}
                  </th>
                ))}
                <th colSpan={3}/>
              </tr>
            )}

            {/* Month / period header row */}
            <tr className="bg-slate-50">
              <th className="text-left text-slate-500 font-semibold py-2.5 pr-4 pl-2 whitespace-nowrap rounded-tl-lg">KPI</th>
              <th className="text-right text-slate-500 font-semibold py-2.5 px-3">Target</th>
              {allPeriods.map((period, idx) => {
                const isNewYear = period.endsWith('-01') && idx > 0
                return (
                  <th
                    key={period}
                    className={`text-center text-slate-500 font-semibold py-2.5 px-1 min-w-[52px] ${
                      isNewYear ? 'border-l-2 border-blue-200' : ''
                    }`}>
                    {colLabel(period)}
                  </th>
                )
              })}
              <th className="text-center text-slate-500 font-semibold py-2.5 px-2 bg-slate-100 whitespace-nowrap">
                {isMultiYear ? 'Avg' : 'FY Avg'}
              </th>
              <th className="text-center text-slate-500 font-semibold py-2.5 px-2">Status</th>
              <th className="py-2.5 px-2"/>
            </tr>
          </thead>
          <tbody>
            {heat.map((kpi, ri) => {
              // Full "YYYY-MM" keyed lookup — no year collisions
              const byPeriod = {}
              kpi.monthly.forEach(m => { byPeriod[m.period] = m.value })
              const streak = redStreak(kpi, allPeriods)
              return (
                <tr key={kpi.key}
                  onClick={() => onKpiClick?.(kpi.key)}
                  className={`border-t border-slate-100 cursor-pointer hover:bg-blue-50/40 transition-colors group ${ri % 2 === 0 ? '' : 'bg-slate-50/40'}`}>
                  <td className="py-2 pr-2 pl-2 text-slate-700 font-medium whitespace-nowrap">
                    <span className="flex items-center gap-1.5">
                      {kpi.name}
                      {streak >= 2 && (
                        <span className="flex items-center gap-0.5 text-[9px] font-bold text-red-500 bg-red-50 border border-red-200 px-1.5 py-0.5 rounded-full flex-shrink-0">
                          {streak >= 3 && <span className="w-1.5 h-1.5 rounded-full bg-red-500 animate-pulse flex-shrink-0"/>}
                          {streak}mo
                        </span>
                      )}
                      <ChevronRight size={11} className="text-slate-300 group-hover:text-[#0055A4] transition-colors flex-shrink-0"/>
                    </span>
                  </td>
                  <td className="py-2 px-3 text-right text-slate-500 font-mono">{fmt(kpi.target, kpi.unit)}</td>

                  {allPeriods.map((period, idx) => {
                    const val      = byPeriod[period]
                    const prevVal  = idx > 0 ? byPeriod[allPeriods[idx - 1]] : null
                    const st       = cellStatus(val, kpi.target, kpi.direction)
                    const delta    = (showDelta && val != null && prevVal != null) ? val - prevVal : null
                    const deltaGood = delta != null
                      ? (kpi.direction === 'higher' ? delta > 0 : delta < 0)
                      : null
                    const isNewYear = period.endsWith('-01') && idx > 0
                    return (
                      <td key={period} className={`py-1.5 px-0.5 ${isNewYear ? 'border-l-2 border-blue-100' : ''}`}>
                        <div className={`rounded px-1 py-1 text-center font-mono text-[11px] font-medium ${cellBg(st)}`}>
                          {val != null ? fmt(val, kpi.unit) : <span className="text-slate-300">—</span>}
                          {delta != null && (
                            <div className={`text-[8px] font-bold leading-tight mt-0.5 ${
                              deltaGood ? 'text-emerald-700' : 'text-red-500'
                            }`}>
                              {delta > 0 ? '▲' : '▼'}{Math.abs(delta).toFixed(1)}
                            </div>
                          )}
                        </div>
                      </td>
                    )
                  })}

                  <td className="py-1.5 px-1">
                    <div className={`rounded px-1 py-1.5 text-center font-mono text-[11px] font-bold ${cellBg(kpi.fy_status)} bg-opacity-80`}>
                      {fmt(kpi.avg, kpi.unit)}
                    </div>
                  </td>
                  <td className="py-1.5 px-2 text-center">
                    <span className={`inline-block px-2 py-0.5 rounded-full text-[10px] font-bold ${badgeCls(kpi.fy_status)}`}>
                      {kpi.fy_status?.toUpperCase()}
                    </span>
                  </td>
                  <td className="py-1.5 px-2 text-center">
                    <ChevronRight size={13} className="text-slate-200 group-hover:text-[#0055A4] transition-colors"/>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
