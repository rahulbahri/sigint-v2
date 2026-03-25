import { useState, useRef, useEffect } from 'react'
import axios from 'axios'
import { Sparkles, Send, ChevronDown, ChevronUp, Bot, User } from 'lucide-react'

const BASE_SUGGESTIONS = [
  "Which KPIs need immediate action?",
  "Summarise our FY 2025 performance",
  "What's driving the margin improvement?",
  "Where should management focus this quarter?",
]

const BRIDGE_SUGGESTIONS = [
  "Why is gross margin below projection and what should we do?",
  "Which KPIs are furthest behind projection?",
  "What's causing the gap between projected and actual?",
]

function TypingDots() {
  return (
    <div className="flex items-center gap-1 px-3 py-2">
      <span className="w-1.5 h-1.5 rounded-full bg-slate-400 animate-bounce" style={{ animationDelay: '0ms' }}/>
      <span className="w-1.5 h-1.5 rounded-full bg-slate-400 animate-bounce" style={{ animationDelay: '150ms' }}/>
      <span className="w-1.5 h-1.5 rounded-full bg-slate-400 animate-bounce" style={{ animationDelay: '300ms' }}/>
    </div>
  )
}

export default function AiQueryPanel({ bridgeData, prefillQuestion, onPrefillConsumed, selectedYears }) {
  const [expanded, setExpanded]   = useState(false)
  const [messages, setMessages]   = useState([])
  const [input, setInput]         = useState('')
  const [loading, setLoading]     = useState(false)
  const messagesEndRef             = useRef(null)
  const inputRef                   = useRef(null)

  const hasProjection = bridgeData?.has_projection && bridgeData?.has_overlap
  const suggestions   = hasProjection
    ? [...BRIDGE_SUGGESTIONS, ...BASE_SUGGESTIONS]
    : BASE_SUGGESTIONS

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    if (expanded && messagesEndRef.current) {
      messagesEndRef.current.scrollIntoView({ behavior: 'smooth' })
    }
  }, [messages, loading, expanded])

  // Pre-fill from "Ask Anika" button in ProjectionBridge
  useEffect(() => {
    if (!prefillQuestion) return
    setExpanded(true)
    setInput(prefillQuestion)
    onPrefillConsumed?.()
    // Focus input after expansion renders
    setTimeout(() => inputRef.current?.focus(), 50)
  }, [prefillQuestion])

  async function send(question) {
    const q = (question ?? input).trim()
    if (!q || loading) return

    setInput('')
    setExpanded(true)
    setMessages(prev => [...prev, { role: 'user', text: q }])
    setLoading(true)

    try {
      const { data } = await axios.post('/api/query', { question: q, years: selectedYears?.length ? selectedYears : null })
      setMessages(prev => [...prev, { role: 'ai', text: data.answer, kpis: data.kpis_referenced }])
    } catch (err) {
      setMessages(prev => [...prev, { role: 'ai', text: 'Sorry, the query service is unavailable right now.', kpis: [] }])
    } finally {
      setLoading(false)
    }
  }

  function handleKey(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      send()
    }
  }

  const hasMessages = messages.length > 0

  return (
    <div className="border-t border-white/10 flex flex-col">

      {/* Header */}
      <button
        onClick={() => setExpanded(v => !v)}
        className="flex items-center justify-between px-4 py-3 hover:bg-white/5 transition-colors w-full text-left"
      >
        <div className="flex items-center gap-2">
          <Sparkles size={13} className="text-[#00AEEF]" />
          <span className="text-xs font-semibold text-slate-300">Ask Anika</span>
          {hasMessages && (
            <span className="text-[9px] bg-[#00AEEF]/20 text-[#00AEEF] px-1.5 py-0.5 rounded-full font-medium">
              {messages.filter(m => m.role === 'ai').length}
            </span>
          )}
          {hasProjection && (
            <span className="text-[9px] bg-blue-500/20 text-blue-400 px-1.5 py-0.5 rounded-full font-medium border border-blue-500/30">
              + Projection
            </span>
          )}
        </div>
        {expanded
          ? <ChevronDown size={12} className="text-slate-500"/>
          : <ChevronUp   size={12} className="text-slate-500"/>
        }
      </button>

      {/* Expanded body */}
      {expanded && (
        <div className="flex flex-col">

          {/* Message history or suggestions */}
          <div className="max-h-44 overflow-y-auto px-3 pb-1 space-y-2">
            {!hasMessages && (
              <div className="pt-1 pb-2">
                <p className="text-[10px] text-slate-500 mb-2 uppercase tracking-wider font-medium">Suggestions</p>
                <div className="flex flex-col gap-1.5">
                  {suggestions.map(s => (
                    <button key={s}
                      onClick={() => send(s)}
                      className="text-left text-[11px] text-slate-400 hover:text-[#00AEEF] py-1 px-2
                                 rounded-md hover:bg-white/5 transition-colors leading-snug border
                                 border-white/5 hover:border-[#00AEEF]/20">
                      {s}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {messages.map((msg, i) => (
              <div key={i} className={`flex gap-1.5 ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                {msg.role === 'ai' && (
                  <div className="w-4 h-4 rounded-full bg-[#0055A4]/30 flex items-center justify-center flex-shrink-0 mt-0.5">
                    <Bot size={9} className="text-[#00AEEF]"/>
                  </div>
                )}
                <div className={`rounded-lg px-2.5 py-1.5 text-[11px] leading-relaxed max-w-[82%] ${
                  msg.role === 'user'
                    ? 'bg-[#0055A4]/40 text-slate-200 rounded-tr-sm'
                    : 'bg-white/8 text-slate-300 rounded-tl-sm border border-white/10'
                }`}>
                  {msg.text}
                  {msg.kpis?.length > 0 && (
                    <div className="mt-1.5 flex flex-wrap gap-1">
                      {msg.kpis.slice(0, 3).map(k => (
                        <span key={k} className="text-[9px] bg-[#00AEEF]/15 text-[#00AEEF] px-1.5 py-0.5 rounded-full">
                          {k}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
                {msg.role === 'user' && (
                  <div className="w-4 h-4 rounded-full bg-slate-600/50 flex items-center justify-center flex-shrink-0 mt-0.5">
                    <User size={9} className="text-slate-400"/>
                  </div>
                )}
              </div>
            ))}

            {loading && (
              <div className="flex gap-1.5 justify-start">
                <div className="w-4 h-4 rounded-full bg-[#0055A4]/30 flex items-center justify-center flex-shrink-0 mt-0.5">
                  <Bot size={9} className="text-[#00AEEF]"/>
                </div>
                <div className="bg-white/8 border border-white/10 rounded-lg rounded-tl-sm">
                  <TypingDots/>
                </div>
              </div>
            )}
            <div ref={messagesEndRef}/>
          </div>

          {/* Input */}
          <div className="flex items-end gap-1.5 px-3 pb-3 pt-2">
            <textarea
              ref={inputRef}
              rows={1}
              value={input}
              onChange={e => setInput(e.target.value)}
              onKeyDown={handleKey}
              placeholder="Ask Anika about your KPIs…"
              className="flex-1 bg-white/8 border border-white/15 rounded-lg px-2.5 py-1.5
                         text-[11px] text-slate-200 placeholder-slate-600 resize-none
                         focus:outline-none focus:border-[#00AEEF]/40 focus:bg-white/10
                         transition-colors leading-relaxed"
              style={{ minHeight: 32, maxHeight: 72 }}
            />
            <button
              onClick={() => send()}
              disabled={!input.trim() || loading}
              className="flex-shrink-0 w-7 h-7 rounded-lg bg-[#0055A4]/60 hover:bg-[#0055A4]
                         disabled:opacity-30 disabled:cursor-not-allowed
                         flex items-center justify-center transition-colors">
              <Send size={11} className="text-white"/>
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
