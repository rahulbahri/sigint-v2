import React, { useState } from 'react'
import { Zap, Mail, ArrowRight, CheckCircle2, Loader2 } from 'lucide-react'

export default function LoginPage({ onAuthSuccess }) {
  const [email, setEmail] = useState('')
  const [step, setStep] = useState('email') // 'email' | 'sent' | 'error'
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const handleRequest = async (e) => {
    e.preventDefault()
    if (!email.trim()) return
    setLoading(true)
    setError('')
    try {
      const resp = await fetch('/api/auth/request-link', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: email.trim().toLowerCase() })
      })
      if (resp.ok) {
        setStep('sent')
      } else {
        const data = await resp.json().catch(() => ({}))
        setError(data.detail || 'This email is not authorized.')
      }
    } catch {
      setError('Network error — please try again.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-slate-900 to-indigo-950 flex items-center justify-center px-4">
      <div className="w-full max-w-md">
        {/* Logo */}
        <div className="text-center mb-10">
          <div className="inline-flex items-center gap-2.5 mb-3">
            <div className="w-10 h-10 rounded-xl bg-indigo-600 flex items-center justify-center">
              <Zap size={20} className="text-white" />
            </div>
            <span className="text-2xl font-bold text-white tracking-tight">Axiom</span>
          </div>
          <p className="text-slate-400 text-sm">Intelligence platform for CFOs</p>
        </div>

        {/* Card */}
        <div className="bg-white/5 backdrop-blur border border-white/10 rounded-2xl p-8 shadow-2xl">
          {step === 'email' && (
            <>
              <h2 className="text-xl font-semibold text-white mb-1">Sign in</h2>
              <p className="text-slate-400 text-sm mb-6">
                Enter your email to receive a secure sign-in link.
              </p>
              <form onSubmit={handleRequest} className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-slate-300 mb-1.5">
                    Email address
                  </label>
                  <div className="relative">
                    <Mail size={15} className="absolute left-3.5 top-1/2 -translate-y-1/2 text-slate-400" />
                    <input
                      type="email"
                      value={email}
                      onChange={e => setEmail(e.target.value)}
                      placeholder="you@company.com"
                      required
                      className="w-full pl-10 pr-4 py-2.5 bg-white/10 border border-white/15 rounded-lg
                                 text-white placeholder-slate-500 text-sm
                                 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent
                                 transition-all"
                    />
                  </div>
                </div>
                {error && (
                  <p className="text-red-400 text-sm bg-red-500/10 border border-red-500/20 rounded-lg px-3 py-2">
                    {error}
                  </p>
                )}
                <button
                  type="submit"
                  disabled={loading || !email.trim()}
                  className="w-full flex items-center justify-center gap-2 py-2.5 px-4
                             bg-indigo-600 hover:bg-indigo-500 disabled:bg-indigo-800 disabled:opacity-50
                             text-white font-semibold rounded-lg text-sm transition-all"
                >
                  {loading ? (
                    <><Loader2 size={15} className="animate-spin" /> Sending…</>
                  ) : (
                    <>Send sign-in link <ArrowRight size={15} /></>
                  )}
                </button>
              </form>
            </>
          )}

          {step === 'sent' && (
            <div className="text-center py-4">
              <div className="w-14 h-14 rounded-full bg-emerald-500/15 flex items-center justify-center mx-auto mb-4">
                <CheckCircle2 size={28} className="text-emerald-400" />
              </div>
              <h2 className="text-xl font-semibold text-white mb-2">Check your email</h2>
              <p className="text-slate-400 text-sm leading-relaxed">
                We sent a sign-in link to <span className="text-white font-medium">{email}</span>.
                <br />It expires in 15 minutes.
              </p>
              <button
                onClick={() => setStep('email')}
                className="mt-6 text-sm text-indigo-400 hover:text-indigo-300 transition-colors"
              >
                ← Use a different email
              </button>
            </div>
          )}
        </div>

        <p className="text-center text-slate-600 text-xs mt-6">
          Axiom Intelligence · axiomsync.ai
        </p>
      </div>
    </div>
  )
}
