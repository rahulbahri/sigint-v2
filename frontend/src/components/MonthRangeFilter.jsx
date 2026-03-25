const MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

const PERIOD_PRESETS = [
  { label: 'Q1', months: [1,2,3]       },
  { label: 'Q2', months: [4,5,6]       },
  { label: 'Q3', months: [7,8,9]       },
  { label: 'Q4', months: [10,11,12]    },
  { label: 'H1', months: [1,2,3,4,5,6] },
  { label: 'H2', months: [7,8,9,10,11,12] },
]

export default function MonthRangeFilter({
  selectedYears = [], onYearsChange,
  selectedMonths = [], onMonthsChange,
  availableYears = [],
}) {
  const years    = Array.isArray(availableYears) && availableYears.length
    ? availableYears
    : [2021, 2022, 2023, 2024, 2025, 2026]
  const yearSet  = new Set(selectedYears)
  const monthSet = new Set(selectedMonths)

  function toggleYear(y) {
    onYearsChange(
      yearSet.has(y)
        ? selectedYears.filter(yr => yr !== y)
        : [...selectedYears, y].sort((a, b) => a - b)
    )
  }

  function toggleMonth(m) {
    onMonthsChange(
      monthSet.has(m)
        ? selectedMonths.filter(mo => mo !== m)
        : [...selectedMonths, m].sort((a, b) => a - b)
    )
  }

  // Detect if current month selection exactly matches a preset
  const activePreset = PERIOD_PRESETS.find(
    p => p.months.length === selectedMonths.length && p.months.every(m => monthSet.has(m))
  )

  const btnCls = (active) =>
    `px-2 py-0.5 text-xs rounded-md font-medium transition-colors whitespace-nowrap ${
      active
        ? 'bg-[#0055A4] text-white'
        : 'text-slate-500 hover:text-slate-700 hover:bg-slate-100 border border-transparent hover:border-slate-200'
    }`

  return (
    <div className="flex-shrink-0 px-6 py-2 border-b border-slate-100 bg-slate-50/80 space-y-1.5">

      {/* ── Year row ── */}
      <div className="flex items-center gap-1.5 flex-wrap">
        <span className="text-[10px] text-slate-400 uppercase tracking-wider font-medium w-10 flex-shrink-0">Year</span>
        <button key="all-years" onClick={() => onYearsChange([])} className={btnCls(selectedYears.length === 0)}>All</button>
        <div className="w-px h-3 bg-slate-200 mx-0.5"/>
        {years.map(y => (
          <button key={y} onClick={() => toggleYear(y)} className={btnCls(yearSet.has(y))}>{y}</button>
        ))}
      </div>

      {/* ── Month row ── */}
      <div className="flex items-center gap-1.5 flex-wrap">
        <span className="text-[10px] text-slate-400 uppercase tracking-wider font-medium w-10 flex-shrink-0">Month</span>
        <button key="all-months" onClick={() => onMonthsChange([])} className={btnCls(selectedMonths.length === 0)}>All</button>
        <div className="w-px h-3 bg-slate-200 mx-0.5"/>
        {PERIOD_PRESETS.map(p => (
          <button key={p.label} onClick={() => onMonthsChange(p.months)} className={btnCls(activePreset?.label === p.label)}>{p.label}</button>
        ))}
        <div className="w-px h-3 bg-slate-200 mx-0.5"/>
        {MONTH_NAMES.map((name, i) => (
          <button key={name} onClick={() => toggleMonth(i + 1)} className={btnCls(monthSet.has(i + 1))}>{name}</button>
        ))}
      </div>

    </div>
  )
}
