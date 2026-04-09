import { useState, useEffect } from 'react'
import axios from 'axios'
import { Shield, CheckCircle2, AlertTriangle, XCircle, RefreshCw, Wrench } from 'lucide-react'

const STATUS_STYLE = {
  pass:      { bg: 'bg-emerald-50', border: 'border-emerald-200', text: 'text-emerald-700', Icon: CheckCircle2, label: 'Pass' },
  warn:      { bg: 'bg-amber-50',   border: 'border-amber-200',   text: 'text-amber-700',   Icon: AlertTriangle, label: 'Warning' },
  fail:      { bg: 'bg-red-50',     border: 'border-red-200',     text: 'text-red-700',     Icon: XCircle, label: 'Fail' },
  corrected: { bg: 'bg-blue-50',    border: 'border-blue-200',    text: 'text-blue-700',    Icon: Wrench, label: 'Corrected' },
  pending:   { bg: 'bg-slate-50',   border: 'border-slate-200',   text: 'text-slate-500',   Icon: RefreshCw, label: 'Pending' },
}

function StatusBadge({ status }) {
  const s = STATUS_STYLE[status] || STATUS_STYLE.pending
  return (
    <span className={`inline-flex items-center gap-1 text-[10px] font-bold px-2 py-0.5 rounded-full border ${s.bg} ${s.border} ${s.text}`}>
      <s.Icon size={10} /> {s.label}
    </span>
  )
}

function StageCard({ title, stage }) {
  if (!stage) return null
  const s = STATUS_STYLE[stage.status] || STATUS_STYLE.pending
  return (
    <div className={`rounded-xl border p-4 ${s.bg} ${s.border}`}>
      <div className="flex items-center justify-between mb-2">
        <h3 className="text-[12px] font-bold text-slate-700">{title}</h3>
        <StatusBadge status={stage.status} />
      </div>
      <p className="text-[11px] text-slate-500 mb-2">{stage.summary}</p>
      {stage.discrepancies?.length > 0 && (
        <div className="space-y-1 mt-2">
          {stage.discrepancies.slice(0, 5).map((d, i) => (
            <div key={i} className="text-[10px] bg-white rounded-lg px-3 py-1.5 border border-red-100 text-red-700 font-mono">
              {d.period} {d.kpi}: stored={d.stored} recomputed={d.recomputed} diff={d.diff_pct}%
            </div>
          ))}
          {stage.discrepancies.length > 5 && (
            <p className="text-[10px] text-slate-400">... and {stage.discrepancies.length - 5} more</p>
          )}
        </div>
      )}
      {stage.checks?.length > 0 && (
        <div className="space-y-1 mt-2">
          {stage.checks.filter(c => !c.passed).map((c, i) => (
            <div key={i} className="text-[10px] bg-white rounded-lg px-3 py-1.5 border border-red-100 text-red-700">
              {c.table || c.check}: expected={c.expected ?? c.canonical_total} actual={c.actual ?? c.monthly_data_total}
            </div>
          ))}
        </div>
      )}
      {stage.inconsistencies?.length > 0 && (
        <div className="space-y-1 mt-2">
          {stage.inconsistencies.map((inc, i) => (
            <div key={i} className="text-[10px] bg-white rounded-lg px-3 py-1.5 border border-amber-100 text-amber-700">
              {inc.kpi}: {inc.issue}
            </div>
          ))}
        </div>
      )}
      {stage.issues?.length > 0 && (
        <div className="space-y-1 mt-2">
          {stage.issues.slice(0, 8).map((iss, i) => (
            <div key={i} className={`text-[10px] bg-white rounded-lg px-3 py-1.5 border ${iss.severity === 'fail' ? 'border-red-100 text-red-700' : 'border-amber-100 text-amber-700'}`}>
              {iss.check}: {iss.msg || iss.period || ''} {iss.kpi ? `(${iss.kpi})` : ''} {iss.change_pct ? `${iss.change_pct}% swing` : ''}
            </div>
          ))}
          {stage.issues.length > 8 && <p className="text-[10px] text-slate-400">... and {stage.issues.length - 8} more</p>}
        </div>
      )}
      {stage.anomalies?.length > 0 && (
        <div className="space-y-1 mt-2">
          {stage.anomalies.slice(0, 5).map((a, i) => (
            <div key={i} className={`text-[10px] bg-white rounded-lg px-3 py-1.5 border ${a.severity === 'anomaly' ? 'border-red-100 text-red-700' : 'border-amber-100 text-amber-700'}`}>
              {a.kpi} {a.period}: z={a.z_score} (value={a.value}, mean={a.rolling_mean})
            </div>
          ))}
          {stage.anomalies.length > 5 && <p className="text-[10px] text-slate-400">... and {stage.anomalies.length - 5} more</p>}
        </div>
      )}
      {stage.null_issues?.length > 0 && (
        <div className="space-y-1 mt-2">
          <p className="text-[9px] font-bold text-amber-600 uppercase">Null Propagation</p>
          {stage.null_issues.slice(0, 3).map((n, i) => (
            <div key={i} className="text-[10px] bg-white rounded-lg px-3 py-1.5 border border-amber-100 text-amber-700">
              {n.period}: {n.issue}
            </div>
          ))}
        </div>
      )}
      {stage.business_issues?.length > 0 && (
        <div className="space-y-1 mt-2">
          <p className="text-[9px] font-bold text-amber-600 uppercase">Business Logic</p>
          {stage.business_issues.slice(0, 3).map((b, i) => (
            <div key={i} className="text-[10px] bg-white rounded-lg px-3 py-1.5 border border-amber-100 text-amber-700">
              {b.period}: {b.msg}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default function DataIntegrityPage() {
  const [history, setHistory] = useState([])
  const [current, setCurrent] = useState(null)
  const [loading, setLoading] = useState(false)
  const [correcting, setCorrecting] = useState(false)

  useEffect(() => {
    axios.get('/api/integrity-check/history').then(r => setHistory(r.data?.checks || [])).catch(() => {})
  }, [])

  async function runCheck() {
    setLoading(true)
    try {
      const r = await axios.get('/api/integrity-check')
      setCurrent(r.data)
      // Refresh history
      const h = await axios.get('/api/integrity-check/history')
      setHistory(h.data?.checks || [])
    } catch (e) {
      console.error('Integrity check failed', e)
    } finally {
      setLoading(false)
    }
  }

  async function runCorrection() {
    setCorrecting(true)
    try {
      const r = await axios.post('/api/integrity-check/correct')
      setCurrent(r.data)
      const h = await axios.get('/api/integrity-check/history')
      setHistory(h.data?.checks || [])
    } catch (e) {
      console.error('Integrity correction failed', e)
    } finally {
      setCorrecting(false)
    }
  }

  return (
    <div className="space-y-5 max-w-5xl">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-sm font-bold text-slate-800 flex items-center gap-2">
            <Shield size={15} className="text-[#0055A4]" />
            Data Integrity
          </h2>
          <p className="text-[11px] text-slate-500 mt-0.5">
            End-to-end validation: source data through computation to display.
          </p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={runCheck}
            disabled={loading}
            className="flex items-center gap-1.5 text-[11px] font-semibold px-3 py-1.5 bg-white border border-slate-200 rounded-lg hover:bg-slate-50 disabled:opacity-50"
          >
            <RefreshCw size={11} className={loading ? 'animate-spin' : ''} />
            {loading ? 'Checking...' : 'Run Check'}
          </button>
          <button
            onClick={runCorrection}
            disabled={correcting}
            className="flex items-center gap-1.5 text-[11px] font-semibold px-3 py-1.5 bg-[#0055A4] text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
          >
            <Wrench size={11} />
            {correcting ? 'Correcting...' : 'Run & Correct'}
          </button>
        </div>
      </div>

      {/* Current result */}
      {current && (
        <div className="space-y-3">
          <div className="flex items-center gap-3">
            <StatusBadge status={current.overall_status} />
            <span className="text-[11px] text-slate-500">
              {current.run_id} &middot; {current.completed_at?.slice(0, 19)}
            </span>
            {current.correction_attempted && (
              <span className="text-[10px] font-bold text-blue-600 bg-blue-50 px-2 py-0.5 rounded-full border border-blue-200">
                Auto-corrected
              </span>
            )}
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
            <StageCard title="Stage 0: Temporal" stage={current.stage0} />
            <StageCard title="Stage 1: Source to Canonical" stage={current.stage1} />
            <StageCard title="Stage 2: Computation Verify" stage={current.stage2} />
            <StageCard title="Stage 3: Display Consistency" stage={current.stage3} />
            <StageCard title="Stage 4: Statistical Anomaly" stage={current.stage4} />
          </div>
        </div>
      )}

      {/* History */}
      {history.length > 0 && (
        <div>
          <h3 className="text-[11px] font-bold text-slate-600 uppercase tracking-wider mb-2">Recent Checks</h3>
          <div className="space-y-1.5">
            {history.map(h => (
              <div key={h.run_id} className="flex items-center gap-3 bg-white rounded-lg border border-slate-100 px-3 py-2 text-[11px]">
                <StatusBadge status={h.overall_status} />
                <span className="text-slate-600 font-mono">{h.run_id}</span>
                <span className="text-slate-400">{h.trigger}</span>
                <div className="flex-1" />
                <span className="text-slate-400">
                  S1:<span className={h.stage1_status === 'pass' ? 'text-emerald-600' : 'text-red-500'}>{h.stage1_status}</span>
                  {' '}S2:<span className={h.stage2_status === 'pass' ? 'text-emerald-600' : 'text-red-500'}>{h.stage2_status}</span>
                  {' '}S3:<span className={h.stage3_status === 'pass' ? 'text-emerald-600' : 'text-red-500'}>{h.stage3_status}</span>
                </span>
                <span className="text-slate-300">{h.started_at?.slice(0, 16)}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {!current && history.length === 0 && (
        <div className="bg-white rounded-2xl border border-slate-100 p-10 text-center">
          <Shield size={28} className="text-slate-200 mx-auto mb-2" />
          <p className="text-slate-500 text-sm font-semibold">No integrity checks run yet</p>
          <p className="text-slate-400 text-xs mt-1">Click "Run Check" to validate your data pipeline.</p>
        </div>
      )}
    </div>
  )
}
