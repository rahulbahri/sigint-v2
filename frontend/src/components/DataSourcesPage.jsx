import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import {
  CheckCircle2, XCircle, AlertCircle, RefreshCw, Plus, Trash2,
  Zap, Clock, ChevronRight, Key, Globe
} from 'lucide-react'

const SOURCE_META = {
  stripe:      { icon: '💳', desc: 'Revenue, subscriptions, invoices' },
  quickbooks:  { icon: '📒', desc: 'Accounting, expenses, customers' },
  hubspot:     { icon: '🟠', desc: 'CRM, pipeline, contacts' },
  xero:        { icon: '🟦', desc: 'Accounting, invoices, expenses' },
  shopify:     { icon: '🛍️', desc: 'Orders, products, customers' },
  salesforce:  { icon: '☁️', desc: 'CRM, opportunities, accounts' },
}

function StatusBadge({ status }) {
  if (status === 'ok' || status === 'connected')
    return <span className="flex items-center gap-1 text-green-400 text-xs font-medium"><CheckCircle2 size={13}/> Connected</span>
  if (status === 'error')
    return <span className="flex items-center gap-1 text-red-400 text-xs font-medium"><XCircle size={13}/> Error</span>
  if (status === 'syncing')
    return <span className="flex items-center gap-1 text-yellow-400 text-xs font-medium"><RefreshCw size={13} className="animate-spin"/> Syncing…</span>
  return <span className="flex items-center gap-1 text-gray-500 text-xs font-medium"><AlertCircle size={13}/> Not connected</span>
}

function StripeConnectModal({ onClose, onConnected }) {
  const [apiKey, setApiKey] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  async function handleConnect() {
    if (!apiKey.trim()) { setError('Enter your Stripe secret key'); return }
    setLoading(true); setError('')
    try {
      await axios.post('/api/connectors/stripe/connect', { api_key: apiKey.trim() })
      onConnected()
      onClose()
    } catch (e) {
      setError(e.response?.data?.detail || 'Connection failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
      <div className="bg-[#1a1f2e] border border-white/10 rounded-xl p-6 w-full max-w-md shadow-2xl">
        <h3 className="text-white font-semibold text-lg mb-1">Connect Stripe</h3>
        <p className="text-gray-400 text-sm mb-4">
          Find your secret key at <a href="https://dashboard.stripe.com/apikeys" target="_blank" rel="noreferrer" className="text-[#00AEEF] hover:underline">dashboard.stripe.com/apikeys</a>
        </p>
        <div className="flex items-center gap-2 bg-[#0d1117] border border-white/10 rounded-lg px-3 py-2 mb-3">
          <Key size={14} className="text-gray-500 shrink-0"/>
          <input
            type="password"
            placeholder="sk_live_..."
            value={apiKey}
            onChange={e => setApiKey(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && handleConnect()}
            className="bg-transparent text-white text-sm flex-1 outline-none placeholder-gray-600"
          />
        </div>
        {error && <p className="text-red-400 text-xs mb-3">{error}</p>}
        <div className="flex gap-2 justify-end">
          <button onClick={onClose} className="px-4 py-2 text-sm text-gray-400 hover:text-white transition-colors">Cancel</button>
          <button
            onClick={handleConnect}
            disabled={loading}
            className="px-4 py-2 text-sm bg-[#00AEEF] text-white rounded-lg hover:bg-[#0099d4] disabled:opacity-50 transition-colors"
          >
            {loading ? 'Connecting…' : 'Connect'}
          </button>
        </div>
      </div>
    </div>
  )
}

function ApiTokenModal({ source, label, placeholder, helpText, helpUrl, onClose, onConnected }) {
  const [apiKey, setApiKey] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  async function handleConnect() {
    if (!apiKey.trim()) { setError(`Enter your ${label} token`); return }
    setLoading(true); setError('')
    try {
      await axios.post(`/api/connectors/${source}/connect`, { api_key: apiKey.trim() })
      onConnected()
      onClose()
    } catch (e) {
      setError(e.response?.data?.detail || 'Connection failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
      <div className="bg-[#1a1f2e] border border-white/10 rounded-xl p-6 w-full max-w-md shadow-2xl">
        <h3 className="text-white font-semibold text-lg mb-1">Connect {label}</h3>
        <p className="text-gray-400 text-sm mb-4">
          {helpText}{' '}
          {helpUrl && <a href={helpUrl} target="_blank" rel="noreferrer" className="text-[#00AEEF] hover:underline">Get your token →</a>}
        </p>
        <div className="flex items-center gap-2 bg-[#0d1117] border border-white/10 rounded-lg px-3 py-2 mb-3">
          <Key size={14} className="text-gray-500 shrink-0"/>
          <input
            type="password"
            placeholder={placeholder}
            value={apiKey}
            onChange={e => setApiKey(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && handleConnect()}
            className="bg-transparent text-white text-sm flex-1 outline-none placeholder-gray-600"
          />
        </div>
        {error && <p className="text-red-400 text-xs mb-3">{error}</p>}
        <div className="flex gap-2 justify-end">
          <button onClick={onClose} className="px-4 py-2 text-sm text-gray-400 hover:text-white transition-colors">Cancel</button>
          <button onClick={handleConnect} disabled={loading}
            className="px-4 py-2 text-sm bg-[#00AEEF] text-white rounded-lg hover:bg-[#0099d4] disabled:opacity-50 transition-colors">
            {loading ? 'Connecting…' : 'Connect'}
          </button>
        </div>
      </div>
    </div>
  )
}

function ShopifyConnectModal({ onClose }) {
  const [shop, setShop] = useState('')

  function handleConnect() {
    if (!shop.trim()) return
    const domain = shop.includes('.') ? shop : `${shop}.myshopify.com`
    window.location.href = `/api/connectors/shopify/auth-url?shop=${encodeURIComponent(domain)}`
  }

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
      <div className="bg-[#1a1f2e] border border-white/10 rounded-xl p-6 w-full max-w-md shadow-2xl">
        <h3 className="text-white font-semibold text-lg mb-1">Connect Shopify</h3>
        <p className="text-gray-400 text-sm mb-4">Enter your Shopify store domain</p>
        <div className="flex items-center gap-2 bg-[#0d1117] border border-white/10 rounded-lg px-3 py-2 mb-3">
          <Globe size={14} className="text-gray-500 shrink-0"/>
          <input
            type="text"
            placeholder="your-store.myshopify.com"
            value={shop}
            onChange={e => setShop(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && handleConnect()}
            className="bg-transparent text-white text-sm flex-1 outline-none placeholder-gray-600"
          />
        </div>
        <div className="flex gap-2 justify-end">
          <button onClick={onClose} className="px-4 py-2 text-sm text-gray-400 hover:text-white transition-colors">Cancel</button>
          <button onClick={handleConnect} className="px-4 py-2 text-sm bg-[#00AEEF] text-white rounded-lg hover:bg-[#0099d4] transition-colors">
            Authorise in Shopify →
          </button>
        </div>
      </div>
    </div>
  )
}

export default function DataSourcesPage() {
  const [connectors, setConnectors] = useState([])
  const [loading, setLoading]       = useState(true)
  const [syncing, setSyncing]       = useState({})
  const [modal, setModal]           = useState(null)   // 'stripe' | 'shopify' | null
  const [toast, setToast]           = useState('')

  const load = useCallback(async () => {
    try {
      const { data } = await axios.get('/api/connectors')
      setConnectors(data.connectors || [])
    } catch {
      // silent
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  // Handle redirect back from OAuth
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const connected = params.get('connector_connected')
    const error     = params.get('connector_error')
    if (connected) {
      showToast(`${connected.charAt(0).toUpperCase() + connected.slice(1)} connected successfully!`)
      window.history.replaceState({}, '', window.location.pathname)
      load()
    }
    if (error) {
      showToast(`Connection failed: ${error}`, true)
      window.history.replaceState({}, '', window.location.pathname)
    }
  }, [load])

  function showToast(msg, isError = false) {
    setToast({ msg, isError })
    setTimeout(() => setToast(''), 4000)
  }

  async function handleConnect(source) {
    if (source === 'stripe')  { setModal('stripe');  return }
    if (source === 'hubspot') { setModal('hubspot'); return }
    if (source === 'shopify') { setModal('shopify'); return }
    // OAuth sources
    try {
      const { data } = await axios.get(`/api/connectors/${source}/auth-url`)
      window.location.href = data.auth_url
    } catch (e) {
      showToast(e.response?.data?.detail || `Could not start ${source} OAuth`, true)
    }
  }

  async function handleSync(source) {
    setSyncing(s => ({ ...s, [source]: true }))
    try {
      await axios.post(`/api/connectors/${source}/sync`)
      showToast(`Sync started for ${source}`)
      setTimeout(load, 3000)
    } catch (e) {
      showToast(e.response?.data?.detail || 'Sync failed', true)
    } finally {
      setSyncing(s => ({ ...s, [source]: false }))
    }
  }

  async function handleDisconnect(source) {
    if (!window.confirm(`Disconnect ${source}? Synced data will remain but future syncs will stop.`)) return
    try {
      await axios.delete(`/api/connectors/${source}`)
      showToast(`${source} disconnected`)
      load()
    } catch {
      showToast('Disconnect failed', true)
    }
  }

  if (loading) return (
    <div className="flex items-center justify-center h-64">
      <RefreshCw size={20} className="animate-spin text-[#00AEEF]"/>
    </div>
  )

  const connected   = connectors.filter(c => c.connected)
  const unconnected = connectors.filter(c => !c.connected)

  return (
    <div className="p-6 max-w-4xl mx-auto">
      {/* Toast */}
      {toast && (
        <div className={`fixed top-4 right-4 z-50 px-4 py-3 rounded-lg text-sm text-white shadow-lg transition-all
          ${toast.isError ? 'bg-red-600' : 'bg-green-600'}`}>
          {toast.msg}
        </div>
      )}

      <div className="mb-6">
        <h2 className="text-white text-xl font-semibold">Data Sources</h2>
        <p className="text-gray-400 text-sm mt-1">
          Connect your business tools to automatically sync data and compute KPIs.
        </p>
      </div>

      {/* Connected sources */}
      {connected.length > 0 && (
        <div className="mb-8">
          <h3 className="text-gray-400 text-xs font-semibold uppercase tracking-wider mb-3">
            Connected ({connected.length})
          </h3>
          <div className="space-y-2">
            {connected.map(c => {
              const meta = SOURCE_META[c.source_name] || {}
              return (
                <div key={c.source_name}
                  className="flex items-center gap-4 bg-[#1a1f2e] border border-white/8 rounded-xl px-5 py-4">
                  <span className="text-2xl w-8 text-center">{meta.icon || '🔗'}</span>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-3">
                      <span className="text-white font-medium">{c.label}</span>
                      <StatusBadge status={c.status}/>
                      {c.has_unmapped_critical && (
                        <span className="text-[9px] font-bold bg-yellow-400/20 text-yellow-400 px-2 py-0.5 rounded-full">
                          Unmapped fields
                        </span>
                      )}
                    </div>
                    <p className="text-gray-500 text-xs mt-0.5">{meta.desc}</p>
                    {c.last_sync_at && (
                      <p className="text-gray-600 text-xs flex items-center gap-1 mt-0.5">
                        <Clock size={10}/> Last sync: {new Date(c.last_sync_at).toLocaleString()}
                      </p>
                    )}
                    {c.last_error && (
                      <p className="text-red-400 text-xs mt-0.5 truncate">⚠ {c.last_error}</p>
                    )}
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => handleSync(c.source_name)}
                      disabled={syncing[c.source_name]}
                      className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-gray-300
                        bg-white/5 hover:bg-white/10 rounded-lg transition-colors disabled:opacity-50"
                    >
                      <RefreshCw size={12} className={syncing[c.source_name] ? 'animate-spin' : ''}/>
                      Resync
                    </button>
                    <button
                      onClick={() => handleDisconnect(c.source_name)}
                      className="p-1.5 text-gray-500 hover:text-red-400 rounded-lg hover:bg-red-400/10 transition-colors"
                      title="Disconnect"
                    >
                      <Trash2 size={14}/>
                    </button>
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Available to connect */}
      {unconnected.length > 0 && (
        <div>
          <h3 className="text-gray-400 text-xs font-semibold uppercase tracking-wider mb-3">
            Available to Connect
          </h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {unconnected.map(c => {
              const meta = SOURCE_META[c.source_name] || {}
              return (
                <button
                  key={c.source_name}
                  onClick={() => handleConnect(c.source_name)}
                  className="flex items-center gap-4 bg-[#1a1f2e] border border-white/8
                    rounded-xl px-5 py-4 hover:border-[#00AEEF]/40 hover:bg-[#1e2438]
                    transition-all text-left group"
                >
                  <span className="text-2xl w-8 text-center">{meta.icon || '🔗'}</span>
                  <div className="flex-1 min-w-0">
                    <p className="text-white font-medium">{c.label}</p>
                    <p className="text-gray-500 text-xs mt-0.5">{meta.desc}</p>
                  </div>
                  <ChevronRight size={16} className="text-gray-600 group-hover:text-[#00AEEF] transition-colors shrink-0"/>
                </button>
              )
            })}
          </div>
        </div>
      )}

      {/* Modals */}
      {modal === 'stripe' && (
        <ApiTokenModal
          source="stripe" label="Stripe" placeholder="sk_live_..."
          helpText="Find your secret key at"
          helpUrl="https://dashboard.stripe.com/apikeys"
          onClose={() => setModal(null)} onConnected={load}
        />
      )}
      {modal === 'hubspot' && (
        <ApiTokenModal
          source="hubspot" label="HubSpot" placeholder="pat-na2-..."
          helpText="In HubSpot: Settings → Integrations → Private Apps → Create private app."
          helpUrl="https://app.hubspot.com/private-apps"
          onClose={() => setModal(null)} onConnected={load}
        />
      )}
      {modal === 'shopify' && <ShopifyConnectModal onClose={() => setModal(null)}/>}
    </div>
  )
}
