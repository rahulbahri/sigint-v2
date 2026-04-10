import { useState, useEffect } from 'react'
import axios from 'axios'
import { Clock } from 'lucide-react'

/**
 * DataFreshnessBar — persistent "Data as of" indicator shown globally.
 * Shows the most recent data period and last upload time with a freshness
 * indicator (green <7 days, yellow 7-30 days, red >30 days).
 */
export default function DataFreshnessBar() {
  const [info, setInfo] = useState(null)

  useEffect(() => {
    axios.get('/api/home').then(r => {
      const d = r.data || {}
      setInfo({
        period: d.data_period?.to || null,
        uploadAt: d.data_period?.last_upload_at || null,
      })
    }).catch(() => {})
  }, [])

  if (!info || !info.period) return null

  // Compute freshness
  const daysSinceUpload = info.uploadAt
    ? Math.floor((Date.now() - new Date(info.uploadAt).getTime()) / 86400000)
    : 999
  const freshnessColor = daysSinceUpload <= 7 ? 'bg-green-400' : daysSinceUpload <= 30 ? 'bg-yellow-400' : 'bg-red-400'
  const freshnessLabel = daysSinceUpload <= 7 ? 'Current' : daysSinceUpload <= 30 ? 'Aging' : 'Stale'

  // Format the period as a readable date
  const periodDate = info.period // e.g., "2025-03" → "March 2025"
  const [yr, mo] = (periodDate || '').split('-')
  const monthNames = ['', 'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December']
  const formatted = yr && mo ? `${monthNames[parseInt(mo, 10)] || mo} ${yr}` : periodDate

  return (
    <div className="flex items-center gap-2 text-[11px] text-slate-400">
      <Clock size={11} className="text-slate-500" />
      <span className="font-semibold text-slate-300">Data as of {formatted}</span>
      <span className={`w-1.5 h-1.5 rounded-full ${freshnessColor}`} title={freshnessLabel} />
      <span className="text-slate-500">{freshnessLabel}</span>
    </div>
  )
}
