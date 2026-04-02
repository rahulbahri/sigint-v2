import { useState, useEffect } from 'react'
import axios from 'axios'

const API = import.meta.env.VITE_API_URL ?? 'http://localhost:8003'

// ─── Slack Alerts Settings Panel ────────────────────────────────────────────
// Lets the user paste a Slack Incoming Webhook URL, test it, and configure
// which KPI threshold events should fire a notification.

const DEFAULT_TRIGGERS = [
  { key: 'any_red',          label: 'Any KPI drops into the red zone',          enabled: true  },
  { key: 'new_red',          label: 'A KPI crosses into red for the first time', enabled: true  },
  { key: 'streak_3',         label: 'KPI has been red for 3+ consecutive months', enabled: false },
  { key: 'multi_domain_red', label: 'Two or more KPI domains are simultaneously red', enabled: false },
]

export default function SlackAlerts({ filteredFingerprint = [] }) {
  const [webhookUrl, setWebhookUrl]     = useState(() => localStorage.getItem('axiom_slack_webhook') || '')
  const [companyName, setCompanyName]   = useState(() => localStorage.getItem('axiom_company_name') || '')
  const [triggers, setTriggers]         = useState(DEFAULT_TRIGGERS)
  const [testState, setTestState]       = useState('idle')   // idle | sending | ok | error
  const [testError, setTestError]       = useState('')
  const [alertState, setAlertState]     = useState('idle')
  const [alertError, setAlertError]     = useState('')
  const [lastFired, setLastFired]       = useState(() => localStorage.getItem('axiom_slack_last_fired') || null)
  const [showHowTo, setShowHowTo]       = useState(false)

  // Persist webhook URL & company name to localStorage whenever they change
  useEffect(() => { localStorage.setItem('axiom_slack_webhook', webhookUrl)  }, [webhookUrl])
  useEffect(() => { localStorage.setItem('axiom_company_name', companyName)  }, [companyName])

  // Derive red KPIs from fingerprint for manual fire / preview
  const redKpis = filteredFingerprint
    .filter(k => k.status === 'red')
    .map(k => ({
      key:     k.key,
      name:    k.name,
      value:   k.avg != null ? +k.avg.toFixed(2) : null,
      target:  k.target,
      pct_off: k.target && k.avg != null
        ? Math.abs(((k.direction === 'higher' ? k.avg / k.target : k.target / k.avg) - 1) * 100)
        : null,
    }))

  function toggleTrigger(key) {
    setTriggers(prev => prev.map(t => t.key === key ? { ...t, enabled: !t.enabled } : t))
  }

  async function sendTest() {
    if (!webhookUrl.trim()) return
    setTestState('sending')
    setTestError('')
    try {
      await axios.post(`${API}/api/slack/test`, { webhook_url: webhookUrl.trim() })
      setTestState('ok')
      setTimeout(() => setTestState('idle'), 3000)
    } catch (err) {
      setTestError(err?.response?.data?.detail || err.message || 'Unknown error')
      setTestState('error')
    }
  }

  async function fireNow() {
    if (!webhookUrl.trim() || !redKpis.length) return
    setAlertState('sending')
    setAlertError('')
    try {
      await axios.post(`${API}/api/slack/notify`, {
        webhook_url:  webhookUrl.trim(),
        red_kpis:     redKpis,
        company_name: companyName.trim() || 'Your Company',
      })
      const now = new Date().toLocaleString()
      setLastFired(now)
      localStorage.setItem('axiom_slack_last_fired', now)
      setAlertState('ok')
      setTimeout(() => setAlertState('idle'), 3000)
    } catch (err) {
      setAlertError(err?.response?.data?.detail || err.message || 'Unknown error')
      setAlertState('error')
    }
  }

  const isConfigured = webhookUrl.trim().startsWith('https://hooks.slack.com/')

  return (
    <div className="p-6 max-w-2xl mx-auto space-y-8">
      {/* ── Page header ── */}
      <div>
        <h2 className="text-xl font-semibold text-slate-800 mb-1">Slack Alerts</h2>
        <p className="text-slate-500 text-[13px] leading-relaxed">
          Connect a Slack channel so Axiom Intelligence fires an alert whenever a KPI
          crosses its critical threshold. Alerts include the KPI name, current value,
          target, and how far off it is — no need to log into the platform to know something
          needs attention.
        </p>
      </div>

      {/* ── How-to accordion ── */}
      <div className="rounded-xl border border-slate-200 bg-slate-50 overflow-hidden">
        <button
          className="w-full flex items-center justify-between px-4 py-3 text-[13px] text-slate-600 hover:text-slate-800 transition-colors"
          onClick={() => setShowHowTo(v => !v)}
        >
          <span className="font-medium">How to get a Slack webhook URL</span>
          <span className="text-lg">{showHowTo ? '▲' : '▼'}</span>
        </button>
        {showHowTo && (
          <div className="px-4 pb-4 text-[13px] text-slate-500 space-y-2 border-t border-slate-200 pt-3">
            <p>1. Go to <strong className="text-slate-700">api.slack.com/apps</strong> and click <em>Create New App → From scratch</em>.</p>
            <p>2. Choose a name (e.g. "Axiom Alerts") and pick your workspace.</p>
            <p>3. In the left sidebar click <em>Incoming Webhooks</em> and toggle it <em>On</em>.</p>
            <p>4. Scroll down and click <em>Add New Webhook to Workspace</em>, select the channel you want alerts in, and click <em>Allow</em>.</p>
            <p>5. Copy the webhook URL that appears — it starts with <code className="text-blue-600">https://hooks.slack.com/services/…</code></p>
            <p>6. Paste it below and click <em>Send Test</em> to verify.</p>
          </div>
        )}
      </div>

      {/* ── Webhook URL input ── */}
      <div className="space-y-3">
        <label className="block text-[13px] font-medium text-slate-700">Webhook URL</label>
        <input
          type="url"
          placeholder="https://hooks.slack.com/services/T.../B.../..."
          value={webhookUrl}
          onChange={e => setWebhookUrl(e.target.value)}
          className="w-full px-3 py-2 rounded-lg bg-white border border-slate-200 text-[13px] text-slate-800 placeholder-slate-400 focus:outline-none focus:border-blue-400 focus:ring-1 focus:ring-blue-100 transition-colors"
        />
        <div className="flex items-center gap-3">
          <button
            onClick={sendTest}
            disabled={!webhookUrl.trim() || testState === 'sending'}
            className="px-4 py-2 rounded-lg text-[13px] font-medium bg-[#0055A4] text-white hover:bg-[#003d80] disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            {testState === 'sending' ? 'Sending…' : testState === 'ok' ? '✓ Test sent!' : testState === 'error' ? '✕ Failed' : 'Send Test Message'}
          </button>
          {isConfigured && (
            <span className="text-[12px] text-emerald-600 font-medium">● Connected</span>
          )}
        </div>
        {testState === 'error' && testError && (
          <p className="text-[12px] text-red-500">{testError}</p>
        )}
      </div>

      {/* ── Company name (for message header) ── */}
      <div className="space-y-2">
        <label className="block text-[13px] font-medium text-slate-700">Company name <span className="text-slate-400 font-normal">(shown in alert header)</span></label>
        <input
          type="text"
          placeholder="e.g. Acme Corp"
          value={companyName}
          onChange={e => setCompanyName(e.target.value)}
          className="w-full px-3 py-2 rounded-lg bg-white border border-slate-200 text-[13px] text-slate-800 placeholder-slate-400 focus:outline-none focus:border-blue-400 focus:ring-1 focus:ring-blue-100 transition-colors"
        />
      </div>

      {/* ── Trigger configuration ── */}
      <div className="space-y-3">
        <label className="block text-[13px] font-medium text-slate-700">Alert triggers</label>
        <div className="rounded-xl border border-slate-200 bg-white divide-y divide-slate-100">
          {triggers.map(t => (
            <label key={t.key} className="flex items-center gap-3 px-4 py-3 cursor-pointer hover:bg-slate-50 transition-colors">
              <input
                type="checkbox"
                checked={t.enabled}
                onChange={() => toggleTrigger(t.key)}
                className="w-4 h-4 rounded accent-blue-600"
              />
              <span className="text-[13px] text-slate-700">{t.label}</span>
            </label>
          ))}
        </div>
        <p className="text-[12px] text-slate-400">
          Alert checks run automatically each time new data is loaded into the platform.
          You can also fire manually below.
        </p>
      </div>

      {/* ── Current red KPIs preview + manual fire ── */}
      <div className="rounded-xl border border-slate-200 bg-white p-4 space-y-3">
        <div className="flex items-center justify-between">
          <h3 className="text-[13px] font-semibold text-slate-700">Current red KPIs</h3>
          {lastFired && (
            <span className="text-[11px] text-slate-400">Last alert: {lastFired}</span>
          )}
        </div>

        {redKpis.length === 0 ? (
          <p className="text-[13px] text-slate-400 italic">
            No KPIs are currently in the red zone — nothing to alert on.
          </p>
        ) : (
          <div className="space-y-2">
            {redKpis.map(k => (
              <div key={k.key} className="flex items-center justify-between bg-red-50 border border-red-200 rounded-lg px-3 py-2">
                <span className="text-[13px] text-red-800 font-medium">{k.name}</span>
                <div className="flex items-center gap-4 text-[12px]">
                  <span className="text-red-600">Current: {k.value ?? '–'}</span>
                  <span className="text-slate-500">Target: {k.target ?? '–'}</span>
                  {k.pct_off != null && (
                    <span className="text-red-600 font-semibold">{k.pct_off.toFixed(0)}% off</span>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}

        <button
          onClick={fireNow}
          disabled={!isConfigured || redKpis.length === 0 || alertState === 'sending'}
          className="mt-1 px-4 py-2 rounded-lg text-[13px] font-medium bg-orange-500 text-white hover:bg-orange-600 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
        >
          {alertState === 'sending' ? 'Sending…'
            : alertState === 'ok'     ? '✓ Alert sent!'
            : alertState === 'error'  ? '✕ Failed'
            : `Fire Alert Now (${redKpis.length} KPI${redKpis.length !== 1 ? 's' : ''})`}
        </button>
        {alertState === 'error' && alertError && (
          <p className="text-[12px] text-red-500">{alertError}</p>
        )}
      </div>

      {/* ── Roadmap note ── */}
      <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3">
        <p className="text-[12px] text-amber-700 leading-relaxed">
          <strong className="text-amber-800">Coming soon:</strong> Automatic alerts fire as soon
          as new data is imported — no manual action needed. Digest mode (daily / weekly summary)
          and Microsoft Teams support are also on the roadmap.
        </p>
      </div>
    </div>
  )
}
