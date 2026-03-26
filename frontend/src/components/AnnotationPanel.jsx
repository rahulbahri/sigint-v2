import { useState, useEffect } from 'react'
import axios from 'axios'

export default function AnnotationPanel({ kpiKey, periods = [] }) {
  const [annotations, setAnnotations] = useState([])
  const [newNote, setNewNote] = useState('')
  const [newPeriod, setNewPeriod] = useState('general')
  const [posting, setPosting] = useState(false)

  useEffect(() => {
    if (!kpiKey) return
    axios.get(`/api/annotations/${kpiKey}`)
      .then(r => setAnnotations(r.data))
      .catch(() => {})
  }, [kpiKey])

  const postNote = async () => {
    if (!newNote.trim()) return
    setPosting(true)
    try {
      const r = await axios.post(`/api/annotations/${kpiKey}`, { note: newNote, period: newPeriod })
      setAnnotations(prev => [r.data, ...prev])
      setNewNote('')
    } finally {
      setPosting(false)
    }
  }

  const deleteNote = async (id) => {
    await axios.delete(`/api/annotations/${id}`)
    setAnnotations(prev => prev.filter(a => a.id !== id))
  }

  const timeAgo = (ts) => {
    const diff = Date.now() - new Date(ts + 'Z').getTime()
    const mins = Math.floor(diff / 60000)
    if (mins < 60) return `${mins}m ago`
    const hrs = Math.floor(mins / 60)
    if (hrs < 24) return `${hrs}h ago`
    return `${Math.floor(hrs / 24)}d ago`
  }

  return (
    <div className="space-y-3">
      <h4 className="text-[11px] font-semibold text-slate-500 uppercase tracking-wider">Notes & Context</h4>

      {annotations.length === 0 && (
        <p className="text-[11px] text-slate-400 italic">No notes yet. Add context about what caused changes in this KPI.</p>
      )}

      <div className="space-y-2 max-h-48 overflow-y-auto">
        {annotations.map(a => (
          <div key={a.id} className="group flex gap-2.5 items-start">
            <div className="shrink-0 w-6 h-6 rounded-full bg-[#0055A4] flex items-center justify-center text-white text-[9px] font-bold">
              {(a.author || 'U').slice(0, 2).toUpperCase()}
            </div>
            <div className="flex-1 bg-slate-50 rounded-lg px-2.5 py-1.5 border border-slate-100">
              <div className="flex items-center justify-between mb-0.5">
                <span className="text-[10px] font-medium text-slate-600">{a.author}</span>
                <div className="flex items-center gap-1.5">
                  {a.period !== 'general' && (
                    <span className="text-[9px] text-blue-500 font-medium">{a.period}</span>
                  )}
                  <span className="text-[9px] text-slate-400">{timeAgo(a.created_at)}</span>
                  <button
                    onClick={() => deleteNote(a.id)}
                    className="opacity-0 group-hover:opacity-100 text-slate-300 hover:text-red-400 transition-all text-[10px]"
                  >×</button>
                </div>
              </div>
              <p className="text-[11px] text-slate-700 leading-relaxed">{a.note}</p>
            </div>
          </div>
        ))}
      </div>

      <div className="border-t border-slate-100 pt-3 space-y-2">
        <textarea
          value={newNote}
          onChange={e => setNewNote(e.target.value)}
          placeholder="Add context: what caused this change? What action was taken?"
          rows={2}
          className="w-full text-[11px] text-slate-700 bg-white border border-slate-200 rounded-lg px-2.5 py-1.5 focus:border-blue-400 focus:outline-none focus:ring-1 focus:ring-blue-100 placeholder:text-slate-300 resize-none"
        />
        <div className="flex items-center gap-2">
          <select
            value={newPeriod}
            onChange={e => setNewPeriod(e.target.value)}
            className="text-[10px] text-slate-600 border border-slate-200 rounded-md px-2 py-1 focus:outline-none focus:border-blue-400 bg-white flex-1"
          >
            <option value="general">General note</option>
            {[...periods].reverse().slice(0, 24).map(p => (
              <option key={p} value={p}>{p}</option>
            ))}
          </select>
          <button
            onClick={postNote}
            disabled={posting || !newNote.trim()}
            className="px-3 py-1 bg-[#0055A4] text-white text-[10px] font-semibold rounded-lg hover:bg-[#003d80] disabled:opacity-40 transition-colors shrink-0"
          >
            {posting ? 'Posting...' : 'Add Note'}
          </button>
        </div>
      </div>
    </div>
  )
}
