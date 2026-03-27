import React, { useState, useEffect } from 'react'
import { Check, Zap, ArrowRight, Loader2, X } from 'lucide-react'

const PLANS = [
  {
    id: 'starter',
    name: 'Starter',
    price: 299,
    period: 'month',
    description: 'For founders and solo CFOs who need signal clarity fast.',
    priceIdEnvKey: 'starter',
    features: [
      'All 39 KPI metrics tracked',
      'MoM variance analysis',
      'Board-ready deck export',
      '3-hop causal chain analysis',
      'Email KPI alerts',
      '1 user seat',
    ],
    cta: 'Start with Starter',
    highlight: false,
  },
  {
    id: 'growth',
    name: 'Growth',
    price: 799,
    period: 'month',
    description: 'For Series A+ companies preparing for the next raise.',
    priceIdEnvKey: 'growth',
    features: [
      'Everything in Starter',
      'QuickBooks auto-sync',
      'Investor-ready benchmarks',
      'Custom KPI targets',
      'Annotation + audit trail',
      'Up to 5 user seats',
      'Priority email support',
    ],
    cta: 'Start with Growth',
    highlight: true,
  },
  {
    id: 'enterprise',
    name: 'Enterprise',
    price: null,
    period: null,
    description: 'For portfolio companies and multi-entity CFOs.',
    priceIdEnvKey: 'enterprise',
    features: [
      'Everything in Growth',
      'Multi-entity dashboards',
      'Custom integrations',
      'Dedicated onboarding',
      'SLA + uptime guarantee',
      'Unlimited seats',
    ],
    cta: 'Contact us',
    highlight: false,
  },
]

export default function PricingPage({ onClose }) {
  const [loading, setLoading] = useState(null)
  const [stripeReady, setStripeReady] = useState(false)
  const [priceIds, setPriceIds] = useState({})

  useEffect(() => {
    fetch('/api/stripe/config')
      .then(r => r.json())
      .then(d => {
        setStripeReady(d.configured)
        if (d.price_ids) setPriceIds(d.price_ids)
      })
      .catch(() => {})
  }, [])

  const handlePlanClick = async (plan) => {
    if (plan.id === 'enterprise') {
      window.location.href = 'mailto:rahul@axiomsync.ai?subject=Axiom Enterprise Inquiry'
      return
    }
    const priceId = priceIds[plan.id]
    if (!priceId || !stripeReady) {
      alert('Payment processing is being set up. Please contact rahul@axiomsync.ai to subscribe.')
      return
    }
    setLoading(plan.id)
    try {
      const resp = await fetch('/api/stripe/create-checkout-session', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          price_id: priceId,
          success_url: `${window.location.origin}?payment=success`,
          cancel_url: `${window.location.origin}?payment=cancelled`,
        })
      })
      const data = await resp.json()
      if (data.url) {
        window.location.href = data.url
      }
    } catch {
      alert('Unable to start checkout. Please try again.')
    } finally {
      setLoading(null)
    }
  }

  return (
    <div className="fixed inset-0 bg-slate-950/90 backdrop-blur-sm z-50 flex items-center justify-center p-4 overflow-y-auto">
      <div className="w-full max-w-5xl">
        {/* Header */}
        <div className="text-center mb-10 relative">
          {onClose && (
            <button
              onClick={onClose}
              className="absolute right-0 top-0 text-slate-400 hover:text-white transition-colors"
            >
              <X size={20} />
            </button>
          )}
          <div className="inline-flex items-center gap-1.5 bg-indigo-500/15 border border-indigo-500/25 rounded-full px-3 py-1 text-indigo-300 text-xs font-medium mb-4">
            <Zap size={11} />
            Simple, transparent pricing
          </div>
          <h1 className="text-3xl font-bold text-white mb-3">
            Turn your numbers into decisions
          </h1>
          <p className="text-slate-400 text-base max-w-lg mx-auto">
            One platform for CFOs who need signal clarity, not spreadsheet chaos.
            Cancel anytime.
          </p>
        </div>

        {/* Plans */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
          {PLANS.map(plan => (
            <div
              key={plan.id}
              className={`relative rounded-2xl p-6 border transition-all ${
                plan.highlight
                  ? 'bg-indigo-600 border-indigo-500 shadow-2xl shadow-indigo-900/50 scale-[1.02]'
                  : 'bg-white/5 border-white/10'
              }`}
            >
              {plan.highlight && (
                <div className="absolute -top-3 left-1/2 -translate-x-1/2">
                  <span className="bg-amber-400 text-amber-900 text-xs font-bold px-3 py-1 rounded-full">
                    Most Popular
                  </span>
                </div>
              )}

              <div className="mb-5">
                <h3 className={`text-lg font-bold mb-1 ${plan.highlight ? 'text-white' : 'text-white'}`}>
                  {plan.name}
                </h3>
                <p className={`text-sm leading-relaxed ${plan.highlight ? 'text-indigo-200' : 'text-slate-400'}`}>
                  {plan.description}
                </p>
              </div>

              <div className="mb-6">
                {plan.price ? (
                  <div className="flex items-end gap-1">
                    <span className={`text-4xl font-bold ${plan.highlight ? 'text-white' : 'text-white'}`}>
                      ${plan.price}
                    </span>
                    <span className={`text-sm mb-1.5 ${plan.highlight ? 'text-indigo-200' : 'text-slate-400'}`}>
                      /{plan.period}
                    </span>
                  </div>
                ) : (
                  <div className="text-3xl font-bold text-white">Custom</div>
                )}
              </div>

              <ul className="space-y-2.5 mb-6">
                {plan.features.map(f => (
                  <li key={f} className="flex items-start gap-2.5">
                    <Check
                      size={14}
                      className={`mt-0.5 shrink-0 ${plan.highlight ? 'text-indigo-200' : 'text-emerald-400'}`}
                    />
                    <span className={`text-sm ${plan.highlight ? 'text-indigo-100' : 'text-slate-300'}`}>
                      {f}
                    </span>
                  </li>
                ))}
              </ul>

              <button
                onClick={() => handlePlanClick(plan)}
                disabled={loading === plan.id}
                className={`w-full flex items-center justify-center gap-2 py-2.5 rounded-lg font-semibold text-sm transition-all ${
                  plan.highlight
                    ? 'bg-white text-indigo-700 hover:bg-indigo-50'
                    : 'bg-white/10 text-white hover:bg-white/15 border border-white/15'
                } disabled:opacity-50`}
              >
                {loading === plan.id ? (
                  <><Loader2 size={14} className="animate-spin" /> Processing…</>
                ) : (
                  <>{plan.cta} <ArrowRight size={14} /></>
                )}
              </button>
            </div>
          ))}
        </div>

        <p className="text-center text-slate-600 text-xs mt-8">
          All plans include a 14-day free trial · No credit card required to start ·{' '}
          <a href="mailto:rahul@axiomsync.ai" className="text-slate-500 hover:text-slate-400">
            Questions? Email us
          </a>
        </p>
      </div>
    </div>
  )
}
