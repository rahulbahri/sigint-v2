export default function Header() {
  return (
    <header className="border-b border-white/8 bg-gradient-to-r from-[#003366] via-[#0055A4] to-[#1E3A5F]">
      <div className="max-w-screen-2xl mx-auto px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <div className="w-9 h-9 rounded-lg bg-[#00AEEF]/20 border border-[#00AEEF]/40 flex items-center justify-center pulse-accent">
            <span className="text-[#00AEEF] font-bold text-sm">SI</span>
          </div>
          <div>
            <h1 className="text-white font-semibold text-lg leading-none">Signals Intelligence</h1>
            <p className="text-[#00AEEF] text-xs mt-0.5 font-medium tracking-wider uppercase">Actionable Intelligence Command Center</p>
          </div>
        </div>
        <div className="flex items-center gap-6 text-xs text-slate-400">
          <span className="hidden sm:block">FY 2025 · Priority-1 KPIs · 18 Metrics</span>
          <a href="/api/docs" target="_blank"
             className="px-3 py-1.5 rounded border border-[#00AEEF]/40 text-[#00AEEF] hover:bg-[#00AEEF]/10 transition-colors font-medium">
            API Docs ↗
          </a>
        </div>
      </div>
    </header>
  )
}
