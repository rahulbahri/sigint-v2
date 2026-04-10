import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import { AlertTriangle, AlertCircle, X, ArrowRight } from 'lucide-react'

/**
 * NotificationBanner — displays workspace notifications (unmapped fields, mapping required, etc.)
 *
 * Props:
 *   onNavigate(tab)  — callback to navigate to a specific tab (e.g., 'field-mapping')
 *   source           — optional source filter for notifications
 */
export default function NotificationBanner({ onNavigate, source }) {
  const [notifications, setNotifications] = useState([])

  const load = useCallback(async () => {
    try {
      const { data } = await axios.get('/api/notifications?unread_only=true')
      const items = data.notifications || []
      setNotifications(source ? items.filter(n => {
        const d = n.data || {}
        return !source || d.source === source
      }) : items)
    } catch {
      // Silent — notification fetch is non-critical
    }
  }, [source])

  useEffect(() => {
    load()
    const interval = setInterval(load, 60_000)
    return () => clearInterval(interval)
  }, [load])

  async function dismiss(id) {
    try {
      await axios.put(`/api/notifications/${id}/dismiss`)
      setNotifications(n => n.filter(x => x.id !== id))
    } catch {
      // Silent
    }
  }

  if (notifications.length === 0) return null

  return (
    <div className="space-y-2 mb-4">
      {notifications.map(n => {
        const isCritical = n.severity === 'critical'
        const Icon = isCritical ? AlertCircle : AlertTriangle
        const bgClass = isCritical
          ? 'bg-red-400/10 border-red-400/20'
          : 'bg-yellow-400/10 border-yellow-400/20'
        const iconClass = isCritical ? 'text-red-400' : 'text-yellow-400'
        const data = n.data || {}

        return (
          <div key={n.id} className={`border rounded-lg px-4 py-3 flex items-start gap-3 ${bgClass}`}>
            <Icon size={18} className={`${iconClass} mt-0.5 flex-shrink-0`} />
            <div className="flex-1 min-w-0">
              <p className="text-slate-800 text-sm font-medium">{n.title}</p>
              <p className="text-slate-500 text-xs mt-0.5">{n.message}</p>
              {data.blocked_kpis && data.blocked_kpis.length > 0 && (
                <div className="flex flex-wrap gap-1 mt-1.5">
                  {data.blocked_kpis.slice(0, 5).map(kpi => (
                    <span key={kpi} className="text-[10px] bg-slate-50 text-slate-700 px-1.5 py-0.5 rounded">
                      {kpi}
                    </span>
                  ))}
                  {data.blocked_kpis.length > 5 && (
                    <span className="text-[10px] text-slate-500">+{data.blocked_kpis.length - 5} more</span>
                  )}
                </div>
              )}
            </div>
            <div className="flex items-center gap-2 flex-shrink-0">
              {onNavigate && (
                <button
                  onClick={() => onNavigate('field-mapping')}
                  className="flex items-center gap-1 px-2.5 py-1 text-xs bg-[#0055A4]/10 text-[#0055A4]
                    rounded-lg hover:bg-[#0055A4]/20 transition-colors"
                >
                  Review Mappings <ArrowRight size={12} />
                </button>
              )}
              <button
                onClick={() => dismiss(n.id)}
                className="p-1 text-slate-500 hover:text-slate-700 transition-colors"
                title="Dismiss"
              >
                <X size={14} />
              </button>
            </div>
          </div>
        )
      })}
    </div>
  )
}
