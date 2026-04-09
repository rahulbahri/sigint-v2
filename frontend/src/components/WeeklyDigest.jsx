import { useState, useEffect, useMemo } from 'react'
import axios from 'axios'
import {
  Zap, TrendingUp, TrendingDown, AlertTriangle, CheckCircle2,
  Target, ChevronRight, ArrowRight, Copy, Check,
} from 'lucide-react'

function fmtVal(v, unit) {
  if (v == null) return '-'
  if (unit === 'pct') return `${v.toFixed(1)}%`
  if (unit === 'usd' || unit === '$') return `$${v.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
  if (unit === 'ratio') return `${v.toFixed(2)}x`
  if (unit === 'days') return `${v.toFixed(0)}d`
  if (unit === 'months') return `${v.toFixed(1)}mo`
  return v.toFixed(2)
}

function formatLabel(key) {
  return key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
}

export default function WeeklyDigest({ data, onNavigate, onKpiClick }) {
  const [copied, setCopied] = useState(false)

  const digest = useMemo(() => {
    if (!data) return null

    const needs = (data.needs_attention || []).slice(0, 5)
    const doing_well = data.doing_well || []
    const period_comp = data.period_comparison || {}
    const check_ins = data.decision_check_ins || []
    const health = data.health || {}

    // Top risks (already sorted by composite criticality)
    const topRisks = needs.map(k => ({
      key: k.key || k,
      name: formatLabel(k.key || k),
      avg: k.avg,
      target: k.target,
      unit: k.unit || '',
      gap_pct: k.gap_pct || (k.avg != null && k.target ? Math.round((k.avg / k.target - 1) * 100) : null),
      composite: k.composite,
      domain: k.domain_label || k.domain || '',
    }))

    // Top wins (improved KPIs from period comparison)
    const wins = (period_comp.improved || []).slice(0, 5).map(k => ({
      key: k.key,
      name: formatLabel(k.key),
      curr: k.curr,
      delta_pct: k.delta_pct ?? (k.prev ? ((k.delta / Math.abs(k.prev)) * 100) : null),
      unit: k.unit || '',
    }))

    // Strategic recommendation
    let recommendation = null
    if (topRisks.length > 0) {
      const worst = topRisks[0]
      recommendation = `Focus this week: ${worst.name} is ${Math.abs(worst.gap_pct || 0).toFixed(0)}% off target${worst.domain ? ` in ${worst.domain}` : ''}. Review root causes and assign corrective actions.`
    } else if (health.score >= 80) {
      recommendation = 'All systems healthy. Focus on maintaining momentum and exploring expansion opportunities.'
    } else {
      recommendation = 'Review KPI targets and ensure data connections are current.'
    }

    // Decision check-ins due
    const dueDecs = check_ins.slice(0, 2)

    return { topRisks, wins, recommendation, dueDecs, health }
  }, [data])

  if (!digest) return null

  const copyDigest = () => {
    const lines = [
      `Weekly Digest — Health Score: ${digest.health?.score || '?'}/100`,
      '',
      'TOP RISKS:',
      ...digest.topRisks.map((r, i) => `${i + 1}. ${r.name}: ${fmtVal(r.avg, r.unit)} vs target ${fmtVal(r.target, r.unit)} (${r.gap_pct > 0 ? '+' : ''}${r.gap_pct?.toFixed(0) || '?'}%)`),
      '',
      'WINS:',
      ...digest.wins.map((w, i) => `${i + 1}. ${w.name}: ${fmtVal(w.curr, w.unit)} (${w.delta_pct > 0 ? '+' : ''}${w.delta_pct?.toFixed(1) || '?'}%)`),
      '',
      `RECOMMENDATION: ${digest.recommendation}`,
    ]
    navigator.clipboard.writeText(lines.join('\n')).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }

  return (
    <div className="card p-4 shadow-sm border-l-4 border-l-[#0055A4]">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <Zap size={14} className="text-[#0055A4]" />
          <h2 className="text-slate-700 text-[12px] font-bold uppercase tracking-wider">Weekly Digest</h2>
        </div>
        <button
          onClick={copyDigest}
          className="flex items-center gap-1 text-[10px] text-slate-400 hover:text-[#0055A4] transition-colors"
          title="Copy digest to clipboard (paste into email or Slack)"
        >
          {copied ? <Check size={10} className="text-emerald-500" /> : <Copy size={10} />}
          {copied ? 'Copied' : 'Copy for email'}
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Left: Top Risks */}
        <div>
          <p className="text-[10px] font-bold text-red-500 uppercase tracking-wider mb-2 flex items-center gap-1">
            <AlertTriangle size={10} /> Focus Areas ({digest.topRisks.length})
          </p>
          <div className="space-y-1.5">
            {digest.topRisks.map((risk, i) => (
              <button
                key={risk.key}
                onClick={() => onKpiClick?.({ key: risk.key, avg: risk.avg, unit: risk.unit })}
                className="flex items-center gap-2 w-full text-left px-2.5 py-2 bg-red-50/60 rounded-lg border border-red-100 hover:border-red-200 transition-colors group"
              >
                <span className="text-[10px] font-bold text-red-400 w-4">{i + 1}</span>
                <div className="flex-1 min-w-0">
                  <span className="text-[11px] font-semibold text-slate-700 truncate block">{risk.name}</span>
                  <span className="text-[10px] text-slate-400">{fmtVal(risk.avg, risk.unit)} vs {fmtVal(risk.target, risk.unit)}</span>
                </div>
                {risk.gap_pct != null && (
                  <span className="text-[10px] font-bold text-red-500">{risk.gap_pct > 0 ? '+' : ''}{risk.gap_pct.toFixed(0)}%</span>
                )}
                <ChevronRight size={10} className="text-slate-300 group-hover:text-slate-500 shrink-0" />
              </button>
            ))}
            {digest.topRisks.length === 0 && (
              <p className="text-[11px] text-emerald-600 font-medium py-2">No critical KPIs this week</p>
            )}
          </div>
        </div>

        {/* Right: Wins */}
        <div>
          <p className="text-[10px] font-bold text-emerald-500 uppercase tracking-wider mb-2 flex items-center gap-1">
            <TrendingUp size={10} /> Wins This Period ({digest.wins.length})
          </p>
          <div className="space-y-1.5">
            {digest.wins.map((win, i) => (
              <div
                key={win.key}
                className="flex items-center gap-2 px-2.5 py-2 bg-emerald-50/60 rounded-lg border border-emerald-100"
              >
                <span className="text-[10px] font-bold text-emerald-400 w-4">{i + 1}</span>
                <div className="flex-1 min-w-0">
                  <span className="text-[11px] font-semibold text-slate-700 truncate block">{win.name}</span>
                  <span className="text-[10px] text-slate-400">{fmtVal(win.curr, win.unit)}</span>
                </div>
                {win.delta_pct != null && (
                  <span className="text-[10px] font-bold text-emerald-600">+{win.delta_pct.toFixed(1)}%</span>
                )}
              </div>
            ))}
            {digest.wins.length === 0 && (
              <p className="text-[11px] text-slate-400 py-2">No improved KPIs this period</p>
            )}
          </div>
        </div>
      </div>

      {/* Recommendation */}
      <div className="mt-3 bg-blue-50/60 border border-blue-100 rounded-lg px-3 py-2.5">
        <p className="text-[10px] font-bold text-[#0055A4] uppercase tracking-wider mb-1 flex items-center gap-1">
          <Target size={10} /> This Week's Priority
        </p>
        <p className="text-[11px] text-slate-600 leading-relaxed">{digest.recommendation}</p>
      </div>

      {/* Decision check-ins */}
      {digest.dueDecs.length > 0 && (
        <div className="mt-2 flex flex-wrap gap-2">
          {digest.dueDecs.map((dec, i) => (
            <button
              key={i}
              onClick={() => onNavigate?.('decisions')}
              className="text-[10px] text-blue-600 bg-blue-50 border border-blue-200 rounded-lg px-2.5 py-1 hover:bg-blue-100 transition-colors"
            >
              Decision check-in: {dec.title} ({dec.days_since}d ago) <ArrowRight size={9} className="inline ml-1" />
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
