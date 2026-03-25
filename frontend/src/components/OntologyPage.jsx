import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import axios from 'axios'
import { Network, RefreshCw, Sparkles, X, ChevronRight, Zap, BarChart2 } from 'lucide-react'

// ── Domain colours ────────────────────────────────────────────────────────
const DOMAIN_COLOR = {
  growth:        '#00AEEF',
  retention:     '#10b981',
  profitability: '#f59e0b',
  efficiency:    '#8b5cf6',
  cashflow:      '#ef4444',
  revenue:       '#06b6d4',
  risk:          '#f97316',
  other:         '#94a3b8',
}

const RELATION_COLOR = {
  CAUSES:          '#f59e0b',   // amber  — expert-encoded dependency
  LEADS:           '#10b981',   // green  — Granger-confirmed leading indicator
  INFLUENCES:      '#8b5cf6',   // purple
  CORRELATES_WITH: '#00AEEF',   // blue
  ANTI_CORRELATES: '#ef4444',   // red
}

// Human-readable edge labels
const RELATION_LABEL = {
  CAUSES:          'Precedes / Drives',
  LEADS:           'Leading Indicator',
  INFLUENCES:      'Influences',
  CORRELATES_WITH: 'Moves With',
  ANTI_CORRELATES: 'Moves Against',
}
const fmtRelation = r => RELATION_LABEL[r] || r.split('_').map(w => w[0] + w.slice(1).toLowerCase()).join(' ')
// Title-case a domain: growth → Growth
const fmtDomain   = d => d ? d[0].toUpperCase() + d.slice(1) : d

// ── Zoom + pan hook (shared by ForceGraph & ClusterGraph) ─────────────────
function useZoomPan(svgRef, W, H) {
  const [vp, setVp] = useState({ scale: 1, tx: 0, ty: 0 })
  const vpRef    = useRef(vp)
  const dragging = useRef(false)
  const dragStart = useRef({ x: 0, y: 0, tx: 0, ty: 0 })

  useEffect(() => { vpRef.current = vp }, [vp])

  // Wheel zoom — must be non-passive to preventDefault
  useEffect(() => {
    const el = svgRef.current
    if (!el) return
    const handler = (e) => {
      e.preventDefault()
      const factor = e.deltaY > 0 ? 0.88 : 1.14
      const { scale, tx, ty } = vpRef.current
      const rect = el.getBoundingClientRect()
      const mx = (e.clientX - rect.left) / rect.width  * W
      const my = (e.clientY - rect.top)  / rect.height * H
      const newScale = Math.max(0.15, Math.min(10, scale * factor))
      setVp({
        scale: newScale,
        tx: mx - (mx - tx) * (newScale / scale),
        ty: my - (my - ty) * (newScale / scale),
      })
    }
    el.addEventListener('wheel', handler, { passive: false })
    return () => el.removeEventListener('wheel', handler)
  }, [svgRef, W, H])

  const handlers = {
    onMouseDown(e) {
      if (e.target.closest('[data-node]')) return   // don't pan when clicking a node
      dragging.current = true
      dragStart.current = { x: e.clientX, y: e.clientY, tx: vpRef.current.tx, ty: vpRef.current.ty }
      e.currentTarget.style.cursor = 'grabbing'
      e.preventDefault()
    },
    onMouseMove(e) {
      if (!dragging.current) return
      const rect = svgRef.current.getBoundingClientRect()
      setVp(v => ({
        ...v,
        tx: dragStart.current.tx + (e.clientX - dragStart.current.x) * W / rect.width,
        ty: dragStart.current.ty + (e.clientY - dragStart.current.y) * H / rect.height,
      }))
    },
    onMouseUp(e)    { dragging.current = false; e.currentTarget.style.cursor = 'grab' },
    onMouseLeave(e) { dragging.current = false; e.currentTarget.style.cursor = 'grab' },
  }

  function zoomBy(factor) {
    setVp(({ scale, tx, ty }) => {
      const newScale = Math.max(0.15, Math.min(10, scale * factor))
      const cx = W / 2, cy = H / 2
      return { scale: newScale, tx: cx - (cx - tx) * (newScale / scale), ty: cy - (cy - ty) * (newScale / scale) }
    })
  }

  return { vp, handlers, zoomBy, reset: () => setVp({ scale: 1, tx: 0, ty: 0 }) }
}

// ── Zoom controls overlay ─────────────────────────────────────────────────
function ZoomControls({ scale, zoomBy, reset }) {
  const btn = (label, onClick) => (
    <button key={label} onClick={onClick}
      title={label === '+' ? 'Zoom in' : label === '−' ? 'Zoom out' : 'Reset view'}
      style={{ width: 28, height: 28, background: '#1e293b', border: '1px solid #475569',
        borderRadius: 6, color: '#e2e8f0', fontSize: label === 'Reset' ? 9 : 18,
        cursor: 'pointer', display: 'flex', alignItems: 'center',
        justifyContent: 'center', fontWeight: 700, lineHeight: 1 }}>
      {label}
    </button>
  )
  return (
    <div style={{ position: 'absolute', bottom: 14, right: 14,
      display: 'flex', flexDirection: 'column', gap: 4, zIndex: 10 }}>
      {btn('+', () => zoomBy(1.3))}
      {btn('−', () => zoomBy(1 / 1.3))}
      {btn('Reset', reset)}
      <div style={{ textAlign: 'center', color: '#e2e8f0', fontSize: 9, fontWeight: 600, marginTop: 2 }}>
        {Math.round(scale * 100)}%
      </div>
    </div>
  )
}

// ── Force-directed layout ─────────────────────────────────────────────────
function useForceLayout(nodes, edges, width, height) {
  const posRef   = useRef({})
  const velRef   = useRef({})
  const frameRef = useRef(null)
  const [tick, setTick] = useState(0)

  useEffect(() => {
    if (!nodes.length) return

    // Always reset when canvas size changes so nodes re-spread on the new canvas
    posRef.current = {}
    velRef.current = {}

    const domains = [...new Set(nodes.map(n => n.domain))]
    nodes.forEach((n, i) => {
      const di = domains.indexOf(n.domain)
      const angle = (di / domains.length) * Math.PI * 2 + (i * 0.3)
      const r = Math.min(width, height) * 0.25
      posRef.current[n.key] = {
        x: width  / 2 + r * Math.cos(angle) + (Math.random() - 0.5) * 100,
        y: height / 2 + r * Math.sin(angle) + (Math.random() - 0.5) * 100,
      }
      velRef.current[n.key] = { x: 0, y: 0 }
    })

    const adj = {}
    edges.forEach(e => {
      adj[e.source] = adj[e.source] || new Set()
      adj[e.target] = adj[e.target] || new Set()
      adj[e.source].add(e.target)
      adj[e.target].add(e.source)
    })

    let t = 0
    const REPULSION = 14000   // was 4500 — much stronger push between nodes
    const SPRING    = 0.03
    const DAMPING   = 0.70    // was 0.75
    const REST      = 260     // was 100 — longer spring rest length

    function step() {
      const pos = posRef.current
      const vel = velRef.current
      const keys = nodes.map(n => n.key).filter(k => pos[k])

      for (let i = 0; i < keys.length; i++) {
        for (let j = i + 1; j < keys.length; j++) {
          const a = keys[i], b = keys[j]
          const dx = pos[a].x - pos[b].x
          const dy = pos[a].y - pos[b].y
          const dist = Math.max(Math.sqrt(dx * dx + dy * dy), 1)
          const force = REPULSION / (dist * dist)
          const fx = (dx / dist) * force
          const fy = (dy / dist) * force
          vel[a].x += fx; vel[a].y += fy
          vel[b].x -= fx; vel[b].y -= fy
        }
      }

      edges.forEach(e => {
        const pa = pos[e.source], pb = pos[e.target]
        if (!pa || !pb) return
        const dx = pb.x - pa.x
        const dy = pb.y - pa.y
        const dist = Math.max(Math.sqrt(dx * dx + dy * dy), 1)
        const force = SPRING * (dist - REST) * (e.strength || 0.5)
        const fx = (dx / dist) * force
        const fy = (dy / dist) * force
        vel[e.source].x += fx; vel[e.source].y += fy
        vel[e.target].x -= fx; vel[e.target].y -= fy
      })

      // Weaker centre gravity so nodes spread more freely
      keys.forEach(k => {
        vel[k].x += (width  / 2 - pos[k].x) * 0.0006
        vel[k].y += (height / 2 - pos[k].y) * 0.0006
      })

      keys.forEach(k => {
        vel[k].x *= DAMPING; vel[k].y *= DAMPING
        pos[k].x += vel[k].x; pos[k].y += vel[k].y
        pos[k].x = Math.max(40, Math.min(width - 40, pos[k].x))
        pos[k].y = Math.max(40, Math.min(height - 40, pos[k].y))
      })

      t++
      if (t % 6 === 0) setTick(t)
      if (t < 600) frameRef.current = requestAnimationFrame(step)
    }

    frameRef.current = requestAnimationFrame(step)
    return () => cancelAnimationFrame(frameRef.current)
  }, [nodes.length, edges.length, width, height])

  return posRef.current
}

// ── Force graph SVG ───────────────────────────────────────────────────────
function ForceGraph({ nodes, edges, selected, onSelect, linkFilter, domainFilter }) {
  const W = 1200, H = 875
  const pos = useForceLayout(nodes, edges, W, H)

  const svgRef = useRef(null)
  const { vp, handlers, zoomBy, reset } = useZoomPan(svgRef, W, H)
  const { scale, tx, ty } = vp

  const neighborSet = useMemo(() => {
    if (!selected) return null
    const s = new Set([selected])
    edges.forEach(e => {
      if (e.source === selected) s.add(e.target)
      if (e.target === selected) s.add(e.source)
    })
    return s
  }, [selected, edges])

  // Fast domain lookup for edge filtering
  const domainByKey = useMemo(() => {
    const m = {}; nodes.forEach(n => { m[n.key] = n.domain }); return m
  }, [nodes])

  if (!nodes.length) return null

  return (
    <div style={{ position: 'relative' }}>
      <svg ref={svgRef} width="100%" height="875" viewBox={`0 0 ${W} ${H}`}
        style={{ background: '#0f172a', borderRadius: 8, display: 'block', cursor: 'grab' }}
        {...handlers}>
        {/* Arrow marker defs stay outside the transform group */}
        <defs>
          {Object.entries(RELATION_COLOR).map(([rel, col]) => (
            <marker key={rel} id={`arr-${rel}`} markerWidth="6" markerHeight="6"
              refX="5" refY="3" orient="auto">
              <path d="M0,0 L0,6 L6,3 z" fill={col} opacity="0.7"/>
            </marker>
          ))}
        </defs>

        {/* All content inside the zoomable/pannable group */}
        <g transform={`translate(${tx},${ty}) scale(${scale})`}>
          {edges.map((e, i) => {
            const pa = pos[e.source], pb = pos[e.target]
            if (!pa || !pb) return null
            const neighbourDim  = neighborSet && !neighborSet.has(e.source) && !neighborSet.has(e.target)
            const linkDim       = linkFilter?.size > 0 && !linkFilter.has(e.relation)
            const srcDom        = domainByKey[e.source], tgtDom = domainByKey[e.target]
            const domainDim     = domainFilter?.size > 0 && !domainFilter.has(srcDom) && !domainFilter.has(tgtDom)
            const dimmed        = neighbourDim || linkDim || domainDim
            const col = RELATION_COLOR[e.relation] || '#64748b'
            return (
              <line key={i}
                x1={pa.x} y1={pa.y} x2={pb.x} y2={pb.y}
                stroke={col}
                strokeWidth={Math.max(0.5, (e.strength || 0.4) * 2)}
                opacity={dimmed ? 0.04 : 0.70}
                markerEnd={`url(#arr-${e.relation})`}
              />
            )
          })}

          {nodes.map(n => {
            const p = pos[n.key]
            if (!p) return null
            const neighbourDim = neighborSet && !neighborSet.has(n.key)
            const domainDim    = domainFilter?.size > 0 && !domainFilter.has(n.domain)
            const dimmed       = neighbourDim || domainDim
            const r = 6 + (n.centrality || 0) * 18
            const col = DOMAIN_COLOR[n.domain] || '#94a3b8'
            const isSelected = selected === n.key
            return (
              <g key={n.key} data-node="true"
                onClick={() => onSelect(isSelected ? null : n.key)}
                style={{ cursor: 'pointer' }} opacity={dimmed ? 0.12 : 1}>
                {/* glow halo */}
                <circle cx={p.x} cy={p.y} r={r + 5} fill={col} opacity={0.18}/>
                {isSelected && (
                  <circle cx={p.x} cy={p.y} r={r + 10} fill="none" stroke={col} strokeWidth={2} opacity={0.7}/>
                )}
                <circle cx={p.x} cy={p.y} r={r} fill={col} opacity={1}/>
                <text x={p.x} y={p.y + r + 12} textAnchor="middle"
                  fontSize="11" fill="#f1f5f9" style={{ pointerEvents: 'none' }}>
                  {n.name}
                </text>
              </g>
            )
          })}
        </g>
      </svg>
      <ZoomControls scale={scale} zoomBy={zoomBy} reset={reset}/>
      <div style={{ position: 'absolute', bottom: 14, left: 14, background: '#0f172acc',
        borderRadius: 5, padding: '3px 8px', color: '#e2e8f0', fontSize: 10, pointerEvents: 'none' }}>
        Scroll to zoom · Drag to pan
      </div>
    </div>
  )
}

// ── Cluster View ─────────────────────────────────────────────────────────
function ClusterGraph({ nodes, edges, selected, onSelect, linkFilter, domainFilter }) {
  const W = 1600, H = 1200

  const svgRef = useRef(null)
  const { vp, handlers, zoomBy, reset } = useZoomPan(svgRef, W, H)
  const { scale, tx, ty } = vp

  // Group nodes by domain
  const byDomain = useMemo(() => {
    const map = {}
    nodes.forEach(n => {
      const d = n.domain || 'other'
      if (!map[d]) map[d] = []
      map[d].push(n)
    })
    return map
  }, [nodes])

  // Lay out domain cluster centres — wider grid on larger canvas
  const clusterPos = useMemo(() => {
    const domains = Object.keys(byDomain)
    const COLS = 3
    const xStep = W / COLS
    const yStart = 160
    const yStep = 320
    const pos = {}
    domains.forEach((d, i) => {
      pos[d] = {
        x: (i % COLS + 0.5) * xStep,
        y: yStart + Math.floor(i / COLS) * yStep,
      }
    })
    return pos
  }, [byDomain])

  // Compute per-node positions — circle around cluster centre with more radius
  const nodePos = useMemo(() => {
    const pos = {}
    Object.entries(byDomain).forEach(([d, dnodes]) => {
      const centre = clusterPos[d]
      if (!centre) return
      const R = Math.min(110, 44 + dnodes.length * 10)
      dnodes.forEach((n, i) => {
        const angle = (i / Math.max(dnodes.length, 1)) * Math.PI * 2 - Math.PI / 2
        pos[n.key] = {
          x: centre.x + R * Math.cos(angle),
          y: centre.y + R * Math.sin(angle),
        }
      })
    })
    return pos
  }, [byDomain, clusterPos])

  const neighborSet = useMemo(() => {
    if (!selected) return null
    const s = new Set([selected])
    edges.forEach(e => {
      if (e.source === selected) s.add(e.target)
      if (e.target === selected) s.add(e.source)
    })
    return s
  }, [selected, edges])

  // Fast domain lookup for edge filter
  const domainByKey = useMemo(() => {
    const m = {}; nodes.forEach(n => { m[n.key] = n.domain }); return m
  }, [nodes])

  return (
    <div style={{ position: 'relative' }}>
      <svg ref={svgRef} width="100%" height="875" viewBox={`0 0 ${W} ${H}`}
        style={{ background: '#0f172a', borderRadius: 8, display: 'block', cursor: 'grab' }}
        {...handlers}>
        <defs>
          {Object.entries(RELATION_COLOR).map(([rel, col]) => (
            <marker key={rel} id={`cl-arr-${rel}`} markerWidth="6" markerHeight="6"
              refX="5" refY="3" orient="auto">
              <path d="M0,0 L0,6 L6,3 z" fill={col} opacity="0.7"/>
            </marker>
          ))}
        </defs>

        <g transform={`translate(${tx},${ty}) scale(${scale})`}>
          {/* Domain bubble backgrounds — dim when domain filter active */}
          {Object.entries(clusterPos).map(([d, pos]) => {
            const dnodes = byDomain[d] || []
            const R = Math.min(130, 60 + dnodes.length * 11)
            const col = DOMAIN_COLOR[d] || '#94a3b8'
            const domainDimmed = domainFilter?.size > 0 && !domainFilter.has(d)
            return (
              <g key={d} opacity={domainDimmed ? 0.18 : 1}>
                <circle cx={pos.x} cy={pos.y} r={R + 28}
                  fill={col} fillOpacity={0.05}
                  stroke={col} strokeOpacity={0.20} strokeWidth={1.5} strokeDasharray="6 5"/>
                <text x={pos.x} y={pos.y - R - 14}
                  textAnchor="middle" fontSize="14" fontWeight="700"
                  fill={col} opacity={0.9}
                  style={{ letterSpacing: '0.07em', textTransform: 'uppercase', pointerEvents: 'none' }}>
                  {fmtDomain(d)}
                </text>
              </g>
            )
          })}

          {/* Edges */}
          {edges.map((e, i) => {
            const pa = nodePos[e.source], pb = nodePos[e.target]
            if (!pa || !pb) return null
            const neighbourDim = neighborSet && !neighborSet.has(e.source) && !neighborSet.has(e.target)
            const linkDim      = linkFilter?.size > 0 && !linkFilter.has(e.relation)
            const srcDom       = domainByKey[e.source], tgtDom = domainByKey[e.target]
            const domainDim    = domainFilter?.size > 0 && !domainFilter.has(srcDom) && !domainFilter.has(tgtDom)
            const dimmed       = neighbourDim || linkDim || domainDim
            const col = RELATION_COLOR[e.relation] || '#64748b'
            return (
              <line key={i} x1={pa.x} y1={pa.y} x2={pb.x} y2={pb.y}
                stroke={col} strokeWidth={1.5}
                opacity={dimmed ? 0.04 : 0.65}
                markerEnd={`url(#cl-arr-${e.relation})`}/>
            )
          })}

          {/* Nodes */}
          {nodes.map(n => {
            const p = nodePos[n.key]
            if (!p) return null
            const neighbourDim = neighborSet && !neighborSet.has(n.key)
            const domainDim    = domainFilter?.size > 0 && !domainFilter.has(n.domain)
            const dimmed       = neighbourDim || domainDim
            const r = 5 + (n.centrality || 0) * 16
            const col = DOMAIN_COLOR[n.domain] || '#94a3b8'
            const isSel = selected === n.key
            return (
              <g key={n.key} data-node="true"
                onClick={() => onSelect(isSel ? null : n.key)}
                style={{ cursor: 'pointer' }} opacity={dimmed ? 0.12 : 1}>
                {/* glow halo */}
                <circle cx={p.x} cy={p.y} r={r + 5} fill={col} opacity={0.18}/>
                {isSel && (
                  <circle cx={p.x} cy={p.y} r={r + 9} fill="none" stroke={col} strokeWidth={2} opacity={0.7}/>
                )}
                <circle cx={p.x} cy={p.y} r={r} fill={col} opacity={1}/>
                <text x={p.x} y={p.y + r + 12} textAnchor="middle"
                  fontSize="11" fill="#f1f5f9" style={{ pointerEvents: 'none' }}>
                  {n.name}
                </text>
              </g>
            )
          })}
        </g>
      </svg>
      <ZoomControls scale={scale} zoomBy={zoomBy} reset={reset}/>
      <div style={{ position: 'absolute', bottom: 14, left: 14, background: '#0f172acc',
        borderRadius: 5, padding: '3px 8px', color: '#e2e8f0', fontSize: 10, pointerEvents: 'none' }}>
        Scroll to zoom · Drag to pan
      </div>
    </div>
  )
}

// ── Node inspector ────────────────────────────────────────────────────────
function NodeInspector({ nodeKey, nodes, edges, onClose }) {
  const node = nodes.find(n => n.key === nodeKey)
  if (!node) return null
  const outgoing = edges.filter(e => e.source === nodeKey)
  const incoming = edges.filter(e => e.target === nodeKey)
  const col      = DOMAIN_COLOR[node.domain] || '#94a3b8'
  const centrality = node.centrality || 0
  const totalConn  = outgoing.length + incoming.length

  // Plain-English influence level
  const influence = centrality >= 0.60
    ? { label: 'High-leverage',    desc: 'Changes here cascade widely across multiple KPIs', color: '#ef4444' }
    : centrality >= 0.35
    ? { label: 'Moderate influence', desc: 'Key driver within its domain cluster',              color: '#f59e0b' }
    : { label: 'Focused metric',   desc: 'Targeted, more contained impact',                   color: '#10b981' }

  return (
    <div style={{ background: '#1e293b', borderRadius: 8, border: '1px solid #334155',
      padding: 16, width: 260, flexShrink: 0 }}>

      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 10 }}>
        <div>
          <div style={{ fontWeight: 700, color: col, fontSize: 14, lineHeight: 1.2 }}>{node.name}</div>
          <div style={{ color: '#e2e8f0', fontSize: 11, marginTop: 2 }}>{fmtDomain(node.domain)} domain</div>
        </div>
        <button onClick={onClose} style={{ color: '#e2e8f0', background: 'none', border: 'none', cursor: 'pointer', padding: 2, marginLeft: 8, flexShrink: 0 }}>
          <X size={14}/>
        </button>
      </div>

      {/* Influence banner */}
      <div style={{ background: influence.color + '18', border: `1px solid ${influence.color}44`,
        borderRadius: 6, padding: '7px 10px', marginBottom: 12 }}>
        <div style={{ color: influence.color, fontSize: 11, fontWeight: 700 }}>{influence.label}</div>
        <div style={{ color: '#e2e8f0', fontSize: 10, marginTop: 2, lineHeight: 1.4 }}>{influence.desc}</div>
      </div>

      {/* Key stats — 3 meaningful metrics */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 6, marginBottom: 14 }}>
        {[
          { label: 'Connections',   val: totalConn,                              tip: 'Total direct links' },
          { label: 'Drives',        val: outgoing.length,                        tip: 'KPIs this affects' },
          { label: 'Driven by',     val: incoming.length,                        tip: 'KPIs that affect this' },
        ].map(({ label, val, tip }) => (
          <div key={label} style={{ background: '#0f172a', borderRadius: 6, padding: '7px 8px', textAlign: 'center' }}
            title={tip}>
            <div style={{ color: '#f1f5f9', fontWeight: 700, fontSize: 18, lineHeight: 1 }}>{val}</div>
            <div style={{ color: '#e2e8f0', fontSize: 9, marginTop: 3, textTransform: 'uppercase', letterSpacing: '0.05em' }}>{label}</div>
          </div>
        ))}
      </div>

      {/* Outgoing — "This KPI drives…" */}
      {outgoing.length > 0 && (
        <div style={{ marginBottom: 10 }}>
          <div style={{ color: '#e2e8f0', fontSize: 10, fontWeight: 700, marginBottom: 5,
            textTransform: 'uppercase', letterSpacing: '0.06em' }}>
            ▶ This KPI drives
          </div>
          {outgoing.slice(0, 6).map((e, i) => {
            const tn = nodes.find(n => n.key === e.target)
            const rc = RELATION_COLOR[e.relation] || '#94a3b8'
            return (
              <div key={i} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '4px 0', borderBottom: '1px solid #0f172a' }}>
                <span style={{ color: '#e2e8f0', fontSize: 11 }}>{tn?.name || e.target}</span>
                <span style={{ fontSize: 10, padding: '1px 7px', borderRadius: 999,
                  background: rc + '28', color: rc, whiteSpace: 'nowrap', marginLeft: 6 }}>
                  {fmtRelation(e.relation)}
                </span>
              </div>
            )
          })}
        </div>
      )}

      {/* Incoming — "Influenced by…" */}
      {incoming.length > 0 && (
        <div>
          <div style={{ color: '#e2e8f0', fontSize: 10, fontWeight: 700, marginBottom: 5,
            textTransform: 'uppercase', letterSpacing: '0.06em' }}>
            ◀ Influenced by
          </div>
          {incoming.slice(0, 6).map((e, i) => {
            const sn = nodes.find(n => n.key === e.source)
            const rc = RELATION_COLOR[e.relation] || '#94a3b8'
            return (
              <div key={i} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '4px 0', borderBottom: '1px solid #0f172a' }}>
                <span style={{ color: '#e2e8f0', fontSize: 11 }}>{sn?.name || e.source}</span>
                <span style={{ fontSize: 10, padding: '1px 7px', borderRadius: 999,
                  background: rc + '28', color: rc, whiteSpace: 'nowrap', marginLeft: 6 }}>
                  {fmtRelation(e.relation)}
                </span>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════════════
// FOCUS MODE — BFS influence engine + radial graph + narrative
// ══════════════════════════════════════════════════════════════════════════

function computeInfluenceScores(focalKey, nodes, edges) {
  const scores  = {}                         // key → { score, minDepth }
  const visited = {}                         // key → bestScore seen so far
  const queue   = [{ key: focalKey, depth: 0, acc: 1.0 }]
  visited[focalKey] = 1.0

  while (queue.length) {
    const { key, depth, acc } = queue.shift()
    if (depth >= 3) continue

    edges.forEach(e => {
      const neighbor = e.source === key ? e.target : e.target === key ? e.source : null
      if (!neighbor || neighbor === focalKey) return

      const edgeStr  = e.strength || 0.5
      const newAcc   = acc * edgeStr * (1 / (depth + 1))
      const newDepth = depth + 1

      if (newAcc > (visited[neighbor] || 0)) {
        visited[neighbor]  = newAcc
        scores[neighbor]   = { score: newAcc, minDepth: newDepth }
        queue.push({ key: neighbor, depth: newDepth, acc: newAcc })
      }
    })
  }

  // Normalise scores 0 → 1
  const max = Math.max(...Object.values(scores).map(s => s.score), 0.001)
  Object.keys(scores).forEach(k => { scores[k] = { ...scores[k], score: scores[k].score / max } })
  return scores
}

function buildFocusNarrative(focalKey, nodes, edges, influenceScores) {
  const nm = {}; nodes.forEach(n => (nm[n.key] = n))
  const focal = nm[focalKey]
  if (!focal) return ''

  const direct   = Object.entries(influenceScores).filter(([, i]) => i.minDepth === 1).sort(([, a], [, b]) => b.score - a.score)
  const indirect = Object.entries(influenceScores).filter(([, i]) => i.minDepth >= 2) .sort(([, a], [, b]) => b.score - a.score).slice(0, 2)
  const total    = Object.keys(influenceScores).length

  let txt = `${focal.name} connects to ${total} KPI${total !== 1 ? 's' : ''} across up to 3 hops. `
  if (direct.length) {
    const names = direct.slice(0, 3).map(([k, info]) => {
      const rel = edges.find(e => (e.source === focalKey && e.target === k) || (e.target === focalKey && e.source === k))
      return `${nm[k]?.name} (${Math.round(info.score * 100)}% — ${fmtRelation(rel?.relation || 'INFLUENCES')})`
    })
    txt += `Direct connections: ${names.join(', ')}. `
  }
  if (indirect.length) {
    const names = indirect.map(([k, info]) => `${nm[k]?.name} (${Math.round(info.score * 100)}% at ${info.minDepth} hops)`)
    txt += `Indirect influence flows from ${names.join(' and ')} through the network with diminishing weight.`
  }
  return txt
}

// Split a label into up to 2 lines at word boundaries (no truncation)
function wrapLabel(name, maxW) {
  if (name.length <= maxW) return [name]
  const words = name.split(' ')
  let l1 = ''
  for (let i = 0; i < words.length; i++) {
    const candidate = l1 ? l1 + ' ' + words[i] : words[i]
    if (candidate.length > maxW && l1) return [l1, words.slice(i).join(' ')]
    l1 = candidate
  }
  return [l1]
}

// ── Radial Focus Graph ────────────────────────────────────────────────────
function FocusGraph({ nodes, edges, focalKey, influenceScores, linkFilter, domainFilter }) {
  const svgRef = useRef(null)
  const W = 900, H = 880
  const { vp, handlers, zoomBy, reset } = useZoomPan(svgRef, W, H)
  const { scale, tx, ty } = vp

  const nodeMap = useMemo(() => { const m = {}; nodes.forEach(n => (m[n.key] = n)); return m }, [nodes])

  const layout = useMemo(() => {
    if (!focalKey) return {}
    const pos = { [focalKey]: { x: W / 2, y: H / 2 } }
    const byDepth = { 1: [], 2: [], 3: [] }
    Object.entries(influenceScores).forEach(([k, info]) => {
      if (byDepth[info.minDepth]) byDepth[info.minDepth].push({ key: k, score: info.score })
    })
    // Radii sized so outward labels stay within viewBox
    const RADII = { 1: 165, 2: 285, 3: 390 }
    Object.entries(byDepth).forEach(([d, items]) => {
      items.sort((a, b) => b.score - a.score)
      const r = RADII[d]
      items.forEach(({ key }, i) => {
        const angle = (i / items.length) * Math.PI * 2 - Math.PI / 2
        pos[key] = { x: W / 2 + r * Math.cos(angle), y: H / 2 + r * Math.sin(angle) }
      })
    })
    return pos
  }, [focalKey, influenceScores])

  if (!focalKey || !nodeMap[focalKey]) return (
    <div style={{ height: 880, background: '#0f172a', borderRadius: 8, display: 'flex',
      alignItems: 'center', justifyContent: 'center', flexDirection: 'column', gap: 10 }}>
      <Network size={40} color="#334155"/>
      <p style={{ color: '#475569', fontSize: 14 }}>Select a KPI above to see its influence map</p>
    </div>
  )

  const visibleEdges = edges.filter(e => layout[e.source] && layout[e.target])

  return (
    <div style={{ position: 'relative' }}>
      <svg ref={svgRef} width="100%" height="770" viewBox={`0 0 ${W} ${H}`}
        style={{ background: '#0f172a', borderRadius: 8, display: 'block', cursor: 'grab' }}
        {...handlers}>
        <defs>
          {Object.entries(RELATION_COLOR).map(([rel, col]) => (
            <marker key={rel} id={`fa-${rel}`} markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
              <path d="M0,0 L0,6 L6,3 z" fill={col} opacity="0.85"/>
            </marker>
          ))}
        </defs>
        <g transform={`translate(${tx},${ty}) scale(${scale})`}>
          {/* Concentric ring guides — one label per ring */}
          {[165, 285, 390].map((ringR, i) => (
            <g key={ringR}>
              <circle cx={W/2} cy={H/2} r={ringR} fill="none" stroke="#233044" strokeWidth="1" strokeDasharray="5 7"/>
              <text x={W/2} y={H/2 - ringR - 7} textAnchor="middle" fontSize="9" fill="#64748b" fontWeight="600"
                style={{ pointerEvents: 'none' }}>{i + 1} hop{i > 0 ? 's' : ''}</text>
            </g>
          ))}

          {/* Edges */}
          {visibleEdges.map((e, i) => {
            const pa = layout[e.source], pb = layout[e.target]
            if (!pa || !pb) return null
            const isFocalEdge = e.source === focalKey || e.target === focalKey
            const neighbor    = e.source === focalKey ? e.target : e.source
            const info        = influenceScores[neighbor]
            const col         = RELATION_COLOR[e.relation] || '#64748b'
            const sw          = isFocalEdge ? Math.max(0.8, (info?.score || 0.1) * 4) : 0.4
            const linkDimmed  = linkFilter?.size > 0 && !linkFilter.has(e.relation)
            const baseOpacity = isFocalEdge ? 0.75 : 0.18
            // Expert-prior CAUSES edges render dashed; data-confirmed or LEADS render solid
            const isExpertPrior = e.relation === 'CAUSES' && e.confidence_tier !== 'data_confirmed'
            const dash = isExpertPrior ? '5 4' : null
            // Mid-point for Granger lag label on focal confirmed edges
            const showLagLabel = isFocalEdge && e.granger_lag && e.confidence_tier === 'data_confirmed'
            const mx = (pa.x + pb.x) / 2, my = (pa.y + pb.y) / 2
            return (
              <g key={i}>
                <line x1={pa.x} y1={pa.y} x2={pb.x} y2={pb.y}
                  stroke={col} strokeWidth={sw}
                  strokeDasharray={dash || undefined}
                  opacity={linkDimmed ? 0.04 : baseOpacity}
                  markerEnd={`url(#fa-${e.relation})`}/>
                {showLagLabel && (
                  <text x={mx} y={my - 5} textAnchor="middle" fontSize="7"
                    fill={col} opacity="0.9" style={{ pointerEvents: 'none' }}>
                    ~{e.granger_lag}mo
                  </text>
                )}
              </g>
            )
          })}

          {/* Nodes */}
          {Object.entries(layout).map(([key, pos]) => {
            const node = nodeMap[key]; if (!node) return null
            const isFocal     = key === focalKey
            const info        = influenceScores[key]
            const col         = DOMAIN_COLOR[node.domain] || '#94a3b8'
            // Half-size nodes
            const r            = isFocal ? 11 : Math.max(3, 3 + (info?.score || 0) * 5)
            const domainDimmed = !isFocal && domainFilter?.size > 0 && !domainFilter.has(node.domain)
            const depth        = info?.minDepth ?? 0
            // Label placed radially outward from centre — prevents overlap between adjacent nodes
            const angle    = Math.atan2(pos.y - H / 2, pos.x - W / 2)
            const lGap     = r + 9
            const lx       = isFocal ? pos.x : pos.x + Math.cos(angle) * lGap
            const ly       = isFocal ? pos.y + r + 14 : pos.y + Math.sin(angle) * lGap
            const anchor   = isFocal ? 'middle' : Math.cos(angle) > 0.25 ? 'start' : Math.cos(angle) < -0.25 ? 'end' : 'middle'
            const baseline = isFocal ? 'auto' : 'central'
            const labelSize = isFocal ? 11 : depth === 1 ? 7 : 6
            const lineMaxW  = isFocal ? 20 : depth === 1 ? 13 : 11
            const lines     = wrapLabel(node.name, lineMaxW)
            const lh        = labelSize + 2   // line height
            return (
              <g key={key} opacity={domainDimmed ? 0.12 : 1}>
                {/* Soft glow */}
                <circle cx={pos.x} cy={pos.y} r={r + 4} fill={col} opacity={0.15}/>
                {/* Focal: dashed orbit ring */}
                {isFocal && <circle cx={pos.x} cy={pos.y} r={r + 11} fill="none"
                  stroke={col} strokeWidth="1.5" opacity={0.4} strokeDasharray="5 4"/>}
                <circle cx={pos.x} cy={pos.y} r={r} fill={col} opacity={1}/>
                {/* Label — word-wrapped, radially outward from centre */}
                <text x={lx} textAnchor={anchor}
                  fontSize={labelSize} fontWeight={isFocal ? '700' : '400'}
                  fill={isFocal ? '#f8fafc' : '#cbd5e1'}
                  style={{ pointerEvents: 'none' }}>
                  {lines.map((line, i) => (
                    <tspan key={i} x={lx} y={ly + (i - (lines.length - 1) / 2) * lh}>{line}</tspan>
                  ))}
                </text>
                {/* Focal FOCUS badge */}
                {isFocal && (
                  <text x={pos.x} y={pos.y + r + 14 + lines.length * lh} textAnchor="middle"
                    fontSize="7" fontWeight="800" fill={col} letterSpacing="1"
                    style={{ pointerEvents: 'none' }}>FOCUS</text>
                )}
              </g>
            )
          })}
        </g>
      </svg>
      <ZoomControls scale={scale} zoomBy={zoomBy} reset={reset}/>
      {/* Reading guide overlay — bottom left */}
      <div style={{ position: 'absolute', bottom: 14, left: 14, background: '#0f172acc',
        borderRadius: 6, padding: '7px 11px', pointerEvents: 'none',
        display: 'flex', flexDirection: 'column', gap: 5 }}>
        <div style={{ display: 'flex', gap: 14, alignItems: 'center' }}>
          <span style={{ color: '#00AEEF', fontSize: 9, fontWeight: 700 }}>━ 1 hop</span>
          <span style={{ color: '#8b5cf6', fontSize: 9, fontWeight: 700 }}>━ 2 hops</span>
          <span style={{ color: '#475569', fontSize: 9, fontWeight: 700 }}>━ 3 hops</span>
          <span style={{ color: '#64748b', fontSize: 9 }}>·  node size = influence score  ·  scroll to zoom</span>
        </div>
        <div style={{ display: 'flex', gap: 14, alignItems: 'center' }}>
          <span style={{ color: '#f59e0b', fontSize: 9, fontWeight: 700 }}>━ domain knowledge</span>
          <span style={{ color: '#f59e0b', fontSize: 9, fontWeight: 400, letterSpacing: 1 }}>╌ expert prior (unconfirmed)</span>
          <span style={{ color: '#10b981', fontSize: 9, fontWeight: 700 }}>━ data confirmed</span>
          <span style={{ color: '#64748b', fontSize: 9 }}>·  ~Nmo = Granger lead time</span>
        </div>
      </div>
    </div>
  )
}

// ── Influence sidebar panel (Focus mode) ──────────────────────────────────
function InfluencePanel({ focalKey, nodes, influenceScores, narrative }) {
  const nm = {}; nodes.forEach(n => (nm[n.key] = n))
  const ranked = Object.entries(influenceScores).sort(([, a], [, b]) => b.score - a.score).slice(0, 12)
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, width: 260, flexShrink: 0 }}>
      <div style={{ background: '#1e293b', borderRadius: 8, border: '1px solid #334155', padding: 14 }}>
        <div style={{ color: '#e2e8f0', fontSize: 12, fontWeight: 700, marginBottom: 8 }}>Focus Analysis</div>
        <p style={{ color: '#cbd5e1', fontSize: 11, lineHeight: 1.65 }}>{narrative}</p>
      </div>
      <div style={{ background: '#1e293b', borderRadius: 8, border: '1px solid #334155', padding: 14 }}>
        <div style={{ color: '#e2e8f0', fontSize: 12, fontWeight: 700, marginBottom: 2 }}>Connection Strength Index</div>
        <div style={{ color: '#64748b', fontSize: 10, marginBottom: 10, lineHeight: 1.5 }}>
          Bar length = relative strength of path to focus node.<br/>
          100 = strongest connection found. Not a share — scores don't sum to 100.
        </div>
        {ranked.map(([key, info], i) => {
          const n        = nm[key]; if (!n) return null
          const col      = DOMAIN_COLOR[n.domain] || '#94a3b8'
          const idx      = Math.round(info.score * 100)
          const hopColor = info.minDepth === 1 ? '#00AEEF' : info.minDepth === 2 ? '#8b5cf6' : '#475569'
          const hopLabel = info.minDepth === 1 ? '1 hop' : `${info.minDepth} hops`
          return (
            <div key={key} style={{ display: 'flex', alignItems: 'center', gap: 8,
              padding: '6px 0', borderBottom: '1px solid #0f172a' }}>
              <span style={{ color: '#334155', fontSize: 10, width: 16, textAlign: 'right', fontWeight: 700 }}>{i+1}</span>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ color: '#f1f5f9', fontSize: 11, fontWeight: 600 }}>{n.name}</div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 5, marginTop: 3 }}>
                  <div style={{ height: 3, borderRadius: 2, background: col, width: `${idx}%`, maxWidth: 80 }}/>
                  <span style={{ fontSize: 8, fontWeight: 700, padding: '1px 5px', borderRadius: 3,
                    background: hopColor + '22', color: hopColor, border: `1px solid ${hopColor}44` }}>
                    {hopLabel}
                  </span>
                </div>
              </div>
              <span style={{ color: '#e2e8f0', fontWeight: 700, fontSize: 12, flexShrink: 0,
                minWidth: 28, textAlign: 'right' }}>{idx}</span>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════════════
// SENSITIVITY MODE — per-domain weight sliders + weighted graph + narrative
// ══════════════════════════════════════════════════════════════════════════

const DEFAULT_SENSITIVITY = { growth:1,retention:1,profitability:1,efficiency:1,cashflow:1,revenue:1,risk:1 }

const SENSITIVITY_PRESETS = [
  { name:'Cash Preservation', s:{ growth:0.5,retention:1.2,profitability:1.5,efficiency:2.0,cashflow:3.0,revenue:0.7,risk:2.0 } },
  { name:'Growth Focus',      s:{ growth:3.0,retention:1.5,profitability:0.5,efficiency:0.7,cashflow:0.5,revenue:2.5,risk:0.5 } },
  { name:'Retention Priority',s:{ growth:0.8,retention:3.0,profitability:1.0,efficiency:1.0,cashflow:1.0,revenue:1.5,risk:1.2 } },
  { name:'Risk Alert',        s:{ growth:0.6,retention:1.5,profitability:1.2,efficiency:1.0,cashflow:2.0,revenue:0.8,risk:3.0 } },
]

function applyEdgeWeights(edges, nodes, sensitivity) {
  const nm = {}; nodes.forEach(n => (nm[n.key] = n))
  return edges.map(e => {
    const sw = sensitivity[nm[e.source]?.domain] ?? 1
    const tw = sensitivity[nm[e.target]?.domain] ?? 1
    return { ...e, strength: (e.strength || 0.5) * Math.sqrt(sw * tw) }
  })
}

function applyNodeWeights(nodes, weightedEdges, sensitivity) {
  const wDeg = {}
  weightedEdges.forEach(e => {
    wDeg[e.source] = (wDeg[e.source] || 0) + (e.strength || 0.5)
    wDeg[e.target] = (wDeg[e.target] || 0) + (e.strength || 0.5)
  })
  const max = Math.max(...Object.values(wDeg), 0.001)
  return nodes.map(n => ({ ...n, centrality: ((wDeg[n.key] || 0) / max) * (sensitivity[n.domain] ?? 1) }))
}

function buildSensitivityNarrative(nodes, weightedEdges, sensitivity) {
  const entries = Object.entries(sensitivity)
  const maxD    = entries.reduce((a, b) => b[1] > a[1] ? b : a)
  const minD    = entries.reduce((a, b) => b[1] < a[1] ? b : a)
  const atBase  = entries.every(([, v]) => Math.abs(v - 1.0) < 0.05)

  if (atBase) return 'All domains at baseline (1.0×). Adjust sliders to see how different priorities reshape the network — higher sensitivity amplifies edge weights and node prominence in that domain.'

  const wDeg = {}
  weightedEdges.forEach(e => {
    wDeg[e.source] = (wDeg[e.source] || 0) + (e.strength || 0.5)
    wDeg[e.target] = (wDeg[e.target] || 0) + (e.strength || 0.5)
  })
  const nm = {}; nodes.forEach(n => (nm[n.key] = n))
  const topKey  = Object.entries(wDeg).sort(([, a], [, b]) => b - a)[0]?.[0]
  const topNode = nm[topKey]

  let txt = ''
  if (maxD[1] > 1.2) txt += `At ${maxD[1].toFixed(1)}× sensitivity, ${fmtDomain(maxD[0])} domain connections carry amplified weight — relationships touching these KPIs become primary signal pathways. `
  if (minD[1] < 0.8) txt += `${fmtDomain(minD[0])} KPIs are de-emphasised (${minD[1].toFixed(1)}×), reducing their pull on the network topology. `
  if (topNode)        txt += `Under these weights, ${topNode.name} (${fmtDomain(topNode.domain)}) emerges as the highest-leverage node.`
  return txt
}

// ── Sensitivity sidebar panel ─────────────────────────────────────────────
function SensitivityPanel({ sensitivity, onChange, onReset, scenarios, onSave, onLoad, narrative }) {
  const [nameInput, setNameInput] = useState('')
  const allPresets = [...SENSITIVITY_PRESETS, ...scenarios]
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, width: 260, flexShrink: 0 }}>

      {/* Sliders */}
      <div style={{ background: '#1e293b', borderRadius: 8, border: '1px solid #334155', padding: 14 }}>
        <div style={{ color: '#e2e8f0', fontSize: 12, fontWeight: 700, marginBottom: 12 }}>Domain Sensitivity</div>
        {Object.keys(DEFAULT_SENSITIVITY).map(domain => {
          const val = sensitivity[domain] ?? 1.0
          const col = DOMAIN_COLOR[domain] || '#94a3b8'
          const delta = val - 1.0
          return (
            <div key={domain} style={{ marginBottom: 14 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                <span style={{ color: col, fontSize: 11, fontWeight: 600 }}>{fmtDomain(domain)}</span>
                <span style={{ fontSize: 11, fontWeight: 700,
                  color: delta > 0.05 ? '#10b981' : delta < -0.05 ? '#ef4444' : '#e2e8f0' }}>
                  {val.toFixed(1)}×
                </span>
              </div>
              <input type="range" min="0.1" max="3.0" step="0.1" value={val}
                onChange={e => onChange({ ...sensitivity, [domain]: parseFloat(e.target.value) })}
                style={{ width: '100%', accentColor: col, cursor: 'pointer' }}/>
              <div style={{ display:'flex', justifyContent:'space-between', marginTop: 2 }}>
                <span style={{ color:'#334155', fontSize: 9 }}>0.1× (mute)</span>
                <span style={{ color:'#334155', fontSize: 9 }}>3.0× (amplify)</span>
              </div>
            </div>
          )
        })}
        <button onClick={onReset}
          style={{ width:'100%', padding:'6px', borderRadius:6, background:'#334155',
            color:'#e2e8f0', border:'none', cursor:'pointer', fontSize:11, fontWeight:600, marginTop:4 }}>
          ↺ Reset to Baseline
        </button>
      </div>

      {/* Narrative */}
      <div style={{ background: '#1e293b', borderRadius: 8, border: '1px solid #334155', padding: 14 }}>
        <div style={{ color: '#e2e8f0', fontSize: 11, fontWeight: 700, marginBottom: 8 }}>Sensitivity Insight</div>
        <p style={{ color: '#cbd5e1', fontSize: 11, lineHeight: 1.65 }}>{narrative}</p>
      </div>

      {/* Scenarios */}
      <div style={{ background: '#1e293b', borderRadius: 8, border: '1px solid #334155', padding: 14 }}>
        <div style={{ color: '#e2e8f0', fontSize: 11, fontWeight: 700, marginBottom: 8 }}>Scenarios</div>
        <div style={{ display:'flex', gap:4, marginBottom: 10 }}>
          <input value={nameInput} onChange={e => setNameInput(e.target.value)}
            placeholder="Save current as…"
            style={{ flex:1, padding:'4px 8px', borderRadius:6, border:'1px solid #334155',
              background:'#0f172a', color:'#e2e8f0', fontSize:11 }}/>
          <button onClick={() => { if (nameInput.trim()) { onSave(nameInput.trim(), sensitivity); setNameInput('') } }}
            style={{ padding:'4px 10px', borderRadius:6, background:'#0055A4',
              color:'#fff', border:'none', cursor:'pointer', fontSize:11, fontWeight:600 }}>
            Save
          </button>
        </div>
        {allPresets.map((sc, i) => (
          <div key={i} onClick={() => onLoad(sc.s)}
            style={{ padding:'7px 8px', borderRadius:6, cursor:'pointer',
              borderBottom:'1px solid #0f172a', display:'flex', alignItems:'center', justifyContent:'space-between',
              transition:'background 0.1s' }}
            onMouseEnter={e => e.currentTarget.style.background = '#0f172a'}
            onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
            <span style={{ color:'#e2e8f0', fontSize:11 }}>{sc.name}</span>
            <span style={{ color:'#334155', fontSize:10 }}>Load →</span>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Recommendation filter definitions ─────────────────────────────────────
const REC_FILTERS = [
  { id: 'all',                     label: 'All types' },
  { id: 'hidden_leading_indicator', label: 'Hidden leading indicators' },
  { id: 'predictive_chain',        label: 'Predictive chains' },
  { id: 'untested_link',           label: 'Potential upstream drivers' },
  { id: 'bridge_node',             label: 'Bridge nodes' },
  { id: 'cluster',                 label: 'Metric clusters' },
]

const REC_TYPE_COLOR = {
  hidden_leading_indicator: '#10b981',  // green — data-confirmed
  untested_link:            '#00AEEF',  // blue  — to investigate
  bridge_node:              '#8b5cf6',  // purple
  cluster:                  '#f59e0b',  // amber
  predictive_chain:         '#f59e0b',
}

// Human-readable type badges
const REC_TYPE_LABEL = {
  hidden_leading_indicator: 'Data Confirmed',
  untested_link:            'Potential Driver',
  bridge_node:              'Bridge Node',
  cluster:                  'Metric Cluster',
  predictive_chain:         'Predictive Chain',
}

// ── Recommendation card ────────────────────────────────────────────────────
function RecCard({ rec, nodes, onDismiss }) {
  const [expanded, setExpanded] = useState(false)
  const typeColor = REC_TYPE_COLOR[rec.rec_type] || '#94a3b8'

  const pathNames = (rec.path || []).map(k => {
    const n = nodes.find(nd => nd.key === k)
    return { key: k, name: n?.name || k.replace(/_/g, ' '), node: n }
  })

  const lastNode  = pathNames[pathNames.length - 1]?.node
  const actions   = lastNode?.corrective_actions || []
  const firstNode = pathNames[0]?.node
  const impacts   = (firstNode?.downstream_impact || [])
    .map(k => nodes.find(n => n.key === k)).filter(Boolean)

  return (
    <div style={{ background: '#1e293b', borderRadius: 10,
      border: '1px solid #3b5070', padding: '16px 18px', position: 'relative',
      display: 'flex', flexDirection: 'column', gap: 12 }}>

      {/* Dismiss */}
      <button onClick={() => onDismiss(rec.id)}
        style={{ position: 'absolute', top: 12, right: 12, background: 'none',
          border: 'none', color: '#e2e8f0', cursor: 'pointer', padding: 2,
          display: 'flex', alignItems: 'center' }}>
        <X size={14}/>
      </button>

      {/* Icon + Title */}
      <div style={{ display: 'flex', gap: 12, alignItems: 'flex-start', paddingRight: 24 }}>
        <div style={{ width: 36, height: 36, borderRadius: 8,
          background: typeColor + '22', display: 'flex', alignItems: 'center',
          justifyContent: 'center', flexShrink: 0 }}>
          <BarChart2 size={18} style={{ color: typeColor }}/>
        </div>
        <div style={{ flex: 1 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
            <span style={{ fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4,
              background: typeColor + '22', color: typeColor, border: `1px solid ${typeColor}44` }}>
              {REC_TYPE_LABEL[rec.rec_type] || rec.rec_type}
            </span>
            {rec.rec_type === 'hidden_leading_indicator' && (
              <span style={{ fontSize: 10, color: '#10b981', fontWeight: 700 }}>★ Confirmed in your data</span>
            )}
          </div>
          <div style={{ color: '#f1f5f9', fontSize: 14, fontWeight: 600, lineHeight: 1.35 }}>
            {rec.title}
          </div>
        </div>
      </div>

      {/* Path chain pills */}
      {pathNames.length > 0 && (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5, alignItems: 'center' }}>
          {pathNames.map((item, i) => (
            <span key={item.key} style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
              <span style={{ fontSize: 11, padding: '3px 9px', borderRadius: 4,
                background: '#0f172a', color: '#e2e8f0', border: '1px solid #475569',
                whiteSpace: 'nowrap' }}>
                {item.name.toLowerCase()}
              </span>
              {i < pathNames.length - 1 && (
                <span style={{ color: '#e2e8f0', fontSize: 13, lineHeight: 1 }}>→</span>
              )}
            </span>
          ))}
        </div>
      )}

      {/* Confidence / Novelty / Impact */}
      <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap' }}>
        <span style={{ fontSize: 12, color: '#e2e8f0' }}>
          Confidence{' '}
          <span style={{ color: '#00AEEF', fontWeight: 700 }}>
            {Math.round((rec.confidence || 0) * 100)}%
          </span>
        </span>
        <span style={{ fontSize: 12, color: '#e2e8f0' }}>
          Novelty{' '}
          <span style={{ color: '#f59e0b', fontWeight: 700 }}>
            {Math.round((rec.novelty || 0) * 100)}%
          </span>
        </span>
        <span style={{ fontSize: 12, color: '#e2e8f0' }}>
          Impact{' '}
          <span style={{ color: '#10b981', fontWeight: 700 }}>
            {Math.round((rec.impact || 0) * 100)}%
          </span>
        </span>
      </div>

      {/* Expandable toggle */}
      <button onClick={() => setExpanded(e => !e)}
        style={{ display: 'inline-flex', alignItems: 'center', gap: 4,
          background: 'none', border: 'none', color: '#e2e8f0',
          cursor: 'pointer', fontSize: 12, padding: 0, alignSelf: 'flex-start' }}>
        <ChevronRight size={13} style={{
          transform: expanded ? 'rotate(90deg)' : 'none',
          transition: 'transform 0.15s' }}/>
        {expanded ? 'Hide hypothesis & steps' : 'Show hypothesis & steps'}
      </button>

      {/* Expanded content */}
      {expanded && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 14,
          paddingTop: 12, borderTop: '1px solid #0f172a' }}>

          {rec.hypothesis && (
            <div>
              <div style={{ color: '#e2e8f0', fontSize: 10, fontWeight: 700,
                letterSpacing: '0.08em', textTransform: 'uppercase', marginBottom: 6 }}>
                Hypothesis
              </div>
              <div style={{ background: '#0f172a', borderRadius: 8, padding: '10px 14px',
                borderLeft: `3px solid ${typeColor}` }}>
                <div style={{ display: 'flex', gap: 8, alignItems: 'flex-start' }}>
                  <Sparkles size={13} style={{ color: typeColor, flexShrink: 0, marginTop: 1 }}/>
                  <span style={{ color: '#e2e8f0', fontSize: 12, lineHeight: 1.5 }}>
                    {rec.hypothesis}
                  </span>
                </div>
              </div>
            </div>
          )}

          {actions.length > 0 && (
            <div>
              <div style={{ color: '#e2e8f0', fontSize: 10, fontWeight: 700,
                letterSpacing: '0.08em', textTransform: 'uppercase', marginBottom: 6 }}>
                Steps
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                {actions.slice(0, 3).map((a, i) => (
                  <div key={i} style={{ display: 'flex', gap: 10, alignItems: 'flex-start' }}>
                    <div style={{ width: 18, height: 18, borderRadius: '50%',
                      background: '#0055A4', color: '#fff', fontSize: 10, fontWeight: 700,
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                      flexShrink: 0 }}>{i + 1}</div>
                    <span style={{ color: '#e2e8f0', fontSize: 12, lineHeight: 1.4 }}>{a}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {impacts.length > 0 && (
            <div>
              <div style={{ color: '#e2e8f0', fontSize: 10, fontWeight: 700,
                letterSpacing: '0.08em', textTransform: 'uppercase', marginBottom: 6 }}>
                Downstream Impact
              </div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {impacts.map(n => (
                  <span key={n.key} style={{ fontSize: 11, padding: '3px 10px', borderRadius: 999,
                    background: (DOMAIN_COLOR[n.domain] || '#94a3b8') + '22',
                    color: DOMAIN_COLOR[n.domain] || '#94a3b8',
                    border: `1px solid ${(DOMAIN_COLOR[n.domain] || '#94a3b8')}44` }}>
                    {n.name}
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ── Main page ─────────────────────────────────────────────────────────────
export default function OntologyPage() {
  const [graph, setGraph]       = useState({ nodes: [], edges: [] })
  const [allNodes, setAllNodes] = useState([])   // unfiltered — for focus dropdown & BFS
  const [allEdges, setAllEdges] = useState([])   // unfiltered — for focus BFS
  const [stats, setStats]       = useState(null)
  const [recs, setRecs]         = useState([])
  const [tab, setTab]           = useState('graph')
  const [domain, setDomain]     = useState('all')
  const [selected, setSelected] = useState(null)
  const [discovering, setDisc]  = useState(false)
  const [loading, setLoading]   = useState(true)
  const [recFilter, setRecFilter] = useState('all')
  const [clusterView, setClusterView] = useState(false)

  // ── New: graph analysis modes ──────────────────────────────────────────
  const [graphMode,   setGraphMode]   = useState('standard') // 'standard' | 'focus' | 'sensitivity'
  const [focusNode,   setFocusNode]   = useState('')
  const [sensitivity, setSensitivity] = useState({ ...DEFAULT_SENSITIVITY })
  const [scenarios,   setScenarios]   = useState([])

  // ── Legend bar filters ─────────────────────────────────────────────────
  const [activeLinkFilters,   setActiveLinkFilters]   = useState(new Set())
  const [activeDomainFilters, setActiveDomainFilters] = useState(new Set())
  const toggleLinkFilter   = r => setActiveLinkFilters(p  => { const n = new Set(p); n.has(r) ? n.delete(r) : n.add(r); return n })
  const toggleDomainFilter = d => setActiveDomainFilters(p => { const n = new Set(p); n.has(d) ? n.delete(d) : n.add(d); return n })
  const clearLegendFilters = () => { setActiveLinkFilters(new Set()); setActiveDomainFilters(new Set()) }
  const legendFiltersActive = activeLinkFilters.size > 0 || activeDomainFilters.size > 0

  // Derived: Focus Mode — always use full (unfiltered) graph for BFS
  const focusNodes = allNodes.length ? allNodes : graph.nodes
  const focusEdges = allEdges.length ? allEdges : graph.edges
  const influenceScores = useMemo(
    () => focusNode && graphMode === 'focus' ? computeInfluenceScores(focusNode, focusNodes, focusEdges) : {},
    [focusNode, focusNodes, focusEdges, graphMode]
  )
  const focusNarrative = useMemo(
    () => focusNode && graphMode === 'focus' ? buildFocusNarrative(focusNode, focusNodes, focusEdges, influenceScores) : '',
    [focusNode, focusNodes, focusEdges, influenceScores, graphMode]
  )

  // Derived: Sensitivity Mode
  const weightedEdges = useMemo(
    () => graphMode === 'sensitivity' ? applyEdgeWeights(graph.edges, graph.nodes, sensitivity) : graph.edges,
    [graph, sensitivity, graphMode]
  )
  const weightedNodes = useMemo(
    () => graphMode === 'sensitivity' ? applyNodeWeights(graph.nodes, weightedEdges, sensitivity) : graph.nodes,
    [graph, weightedEdges, graphMode]
  )
  const sensitivityNarrative = useMemo(
    () => buildSensitivityNarrative(graph.nodes, weightedEdges, sensitivity),
    [graph.nodes, weightedEdges, sensitivity]
  )

  const loadData = useCallback(async (dom = domain) => {
    setLoading(true)
    try {
      const [gRes, sRes, rRes, allRes] = await Promise.all([
        axios.get(`/api/ontology/graph${dom !== 'all' ? `?domain=${dom}` : ''}`),
        axios.get('/api/ontology/stats'),
        axios.get('/api/ontology/recommendations'),
        dom !== 'all' ? axios.get('/api/ontology/graph') : Promise.resolve(null),
      ])
      setGraph(gRes.data)
      if (allRes) { setAllNodes(allRes.data.nodes || []); setAllEdges(allRes.data.edges || []) }
      else { setAllNodes(gRes.data.nodes || []); setAllEdges(gRes.data.edges || []) }
      setStats(sRes.data)
      setRecs(rRes.data)
    } catch (e) { console.error(e) }
    setLoading(false)
  }, [domain])

  useEffect(() => { loadData(domain) }, [domain])

  async function discover() {
    setDisc(true)
    await axios.post('/api/ontology/discover')
    setTimeout(() => { loadData(domain); setDisc(false) }, 8000)
  }

  async function dismiss(id) {
    await axios.post(`/api/ontology/recommendations/${id}/dismiss`)
    setRecs(r => r.filter(rec => rec.id !== id))
  }

  const filteredRecs = recFilter === 'all'
    ? recs
    : recs.filter(r => r.rec_type === recFilter)

  const noData = !loading && graph.nodes.length === 0
  const DOMAINS = ['all', ...Object.keys(DOMAIN_COLOR).filter(d => d !== 'other')]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, height: '100%' }}>

      {/* Plain-English explainer */}
      <div className="rounded-xl border border-slate-200 bg-slate-50 px-5 py-3 flex items-start gap-3 flex-shrink-0">
        <span className="text-slate-400 text-lg flex-shrink-0">🔗</span>
        <p className="text-xs text-slate-600 leading-relaxed">
          <strong className="text-slate-800">How your KPIs are connected.</strong>{' '}
          Every KPI in your business influences others — burn drives CAC payback, churn drives NRR, NRR drives revenue.
          The map below shows those causal chains. The <strong>Top Leverage Points</strong> are the KPIs that, if improved,
          create the most positive knock-on effects across your entire business. Click any node to explore its connections.
        </p>
      </div>

      {/* Stat cards */}
      {stats && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
          {[
            { label: 'Knowledge Nodes', value: stats.total_nodes,             sub: 'KPI metrics' },
            { label: 'Relationships',   value: stats.total_edges,             sub: 'typed edges' },
            { label: 'Recommendations', value: stats.active_recommendations,  sub: 'signal ideas' },
            { label: 'Domains',         value: Object.keys(stats.domain_distribution || {}).length,
              sub: Object.entries(stats.domain_distribution || {}).map(([d,c]) => `${fmtDomain(d)}:${c}`).join(' · ') },
          ].map(({ label, value, sub }) => (
            <div key={label} className="card" style={{ padding: 16 }}>
              <div style={{ color: '#64748b', fontSize: 11, fontWeight: 600, marginBottom: 4 }}>{label}</div>
              <div style={{ color: '#0055A4', fontSize: 28, fontWeight: 700 }}>{value}</div>
              <div style={{ color: '#94a3b8', fontSize: 11 }}>{sub}</div>
            </div>
          ))}
        </div>
      )}

      {/* Top 3 Leverage Points */}
      {graph.nodes.length > 0 && (() => {
        const top3 = [...graph.nodes]
          .filter(n => n.degree_centrality != null)
          .sort((a, b) => (b.degree_centrality ?? 0) - (a.degree_centrality ?? 0))
          .slice(0, 3)
        if (!top3.length) return null
        const ACTIONS = {
          revenue_growth: 'Accelerate pipeline conversion and reduce churn to compound revenue momentum',
          arr_growth: 'Focus expansion ARR via upsell programmes in top-tier accounts',
          gross_margin: 'Renegotiate COGS with key vendors; optimise product tier mix',
          burn_multiple: 'Review headcount efficiency; defer non-critical spend',
          nrr: 'Deploy retention and expansion playbooks for at-risk cohorts',
          churn_rate: 'Prioritise at-risk accounts; improve onboarding and QBR cadence',
          operating_margin: 'Identify opex above budget across G&A and R&D',
          sales_efficiency: 'Increase AE productivity; focus on highest-yield segments',
          cac_payback: 'Optimise marketing channel mix for lower-CAC acquisition',
          dso: 'Tighten collection cycles; review payment terms with top accounts',
        }
        return (
          <div className="card p-4 border-l-4 border-l-[#0055A4]">
            <div className="flex items-center gap-2 mb-3">
              <Zap size={14} className="text-[#0055A4]"/>
              <span className="text-sm font-bold text-slate-800">Top 3 Leverage Points</span>
              <span className="text-[10px] px-2 py-0.5 rounded-full bg-blue-50 text-blue-700 border border-blue-100 font-medium">
                Highest-impact nodes this period
              </span>
            </div>
            <div className="grid grid-cols-3 gap-3">
              {top3.map((node, i) => (
                <div key={node.key} className="bg-slate-50 rounded-xl p-3 border border-slate-100">
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">#{i+1} Leverage</span>
                    <span className="text-[10px] font-mono text-[#0055A4]">
                      {((node.degree_centrality ?? 0) * 100).toFixed(0)}% centrality
                    </span>
                  </div>
                  <p className="text-xs font-semibold text-slate-800 mb-1">{node.name}</p>
                  <p className="text-[10px] text-slate-500 leading-snug">
                    {ACTIONS[node.key] ?? 'Review this KPI and identify downstream improvement levers'}
                  </p>
                </div>
              ))}
            </div>
          </div>
        )
      })()}

      {/* Tabs + Run Discovery */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <div style={{ display: 'flex', gap: 4 }}>
          {[['graph', 'Causal Map'], ['recs', `Actionable Insights${recs.length ? ` (${recs.length})` : ''}`]].map(([id, label]) => (
            <button key={id} onClick={() => setTab(id)}
              style={{ padding: '6px 16px', borderRadius: 6, fontSize: 13, display: 'inline-flex',
                border: 'none', cursor: 'pointer', fontWeight: 600,
                background: tab === id ? '#0055A4' : '#2d3f55',
                color: tab === id ? '#fff' : '#e2e8f0',
                boxShadow: tab === id ? '0 0 0 1px #0066cc' : 'none' }}>
              {label}
            </button>
          ))}
        </div>
        <button onClick={discover} disabled={discovering}
          style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '8px 16px',
            borderRadius: 8, background: '#0055A4', color: '#fff', border: 'none',
            cursor: discovering ? 'not-allowed' : 'pointer', fontSize: 13, fontWeight: 600,
            opacity: discovering ? 0.7 : 1 }}>
          <RefreshCw size={14} style={{ animation: discovering ? 'spin 1s linear infinite' : 'none' }}/>
          {discovering ? 'Running…' : 'Run Discovery'}
        </button>
      </div>

      {/* Knowledge Graph tab */}
      {tab === 'graph' && (
        <>
          {/* ── Controls row ─────────────────────────────────────────────── */}
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>

            {/* Left: domain pills (standard) | focus KPI selector | sensitivity label */}
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
              {graphMode === 'standard' && DOMAINS.map(d => (
                <button key={d} onClick={() => setDomain(d)}
                  style={{ padding: '4px 12px', borderRadius: 999, fontSize: 12, border: 'none',
                    cursor: 'pointer', fontWeight: 500,
                    background: domain === d ? (DOMAIN_COLOR[d] || '#0055A4') : '#334155',
                    color: domain === d ? '#fff' : '#e2e8f0' }}>
                  {d === 'all' ? 'All Domains' : fmtDomain(d)}
                </button>
              ))}

              {graphMode === 'focus' && (
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ color: '#94a3b8', fontSize: 12 }}>Focus KPI:</span>
                  <select value={focusNode} onChange={e => setFocusNode(e.target.value)}
                    style={{ padding: '5px 10px', borderRadius: 8, background: '#1e293b',
                      color: '#f1f5f9', border: '1px solid #334155', fontSize: 12, cursor: 'pointer' }}>
                    <option value="">— select a KPI —</option>
                    {[...(allNodes.length ? allNodes : graph.nodes)].sort((a, b) => a.name.localeCompare(b.name)).map(n => (
                      <option key={n.key} value={n.key}>{n.name}</option>
                    ))}
                  </select>
                  {focusNode && (
                    <span style={{ fontSize: 11, color: '#94a3b8' }}>
                      {Object.keys(influenceScores).length} KPIs connected · up to 3 hops
                    </span>
                  )}
                </div>
              )}

              {graphMode === 'sensitivity' && (
                <span style={{ color: '#94a3b8', fontSize: 12 }}>
                  Adjust domain sliders → to re-weight the network in real time
                </span>
              )}
            </div>

            {/* Right: mode switcher + layout toggle (standard only) */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
              {/* Graph mode switcher */}
              <div style={{ display:'flex', alignItems:'center', background:'#1e293b',
                borderRadius:8, padding:3, gap:2, border:'1px solid #334155' }}>
                {[['standard','Standard'],['focus','Focus Mode'],['sensitivity','Sensitivity']].map(([id, label]) => (
                  <button key={id} onClick={() => setGraphMode(id)}
                    style={{ padding:'4px 14px', borderRadius:6, fontSize:11, border:'none',
                      cursor:'pointer', fontWeight:600,
                      background: graphMode === id ? '#0055A4' : 'transparent',
                      color: graphMode === id ? '#fff' : '#94a3b8' }}>
                    {label}
                  </button>
                ))}
              </div>
              {/* Layout toggle — only in standard mode */}
              {graphMode === 'standard' && (
                <div style={{ display:'flex', alignItems:'center', background:'#1e293b',
                  borderRadius:8, padding:3, gap:2, border:'1px solid #334155' }}>
                  <button onClick={() => setClusterView(false)}
                    style={{ padding:'4px 12px', borderRadius:6, fontSize:11, border:'none',
                      cursor:'pointer', fontWeight:600,
                      background: !clusterView ? '#0055A4' : 'transparent',
                      color: !clusterView ? '#fff' : '#94a3b8' }}>
                    Force
                  </button>
                  <button onClick={() => setClusterView(true)}
                    style={{ padding:'4px 12px', borderRadius:6, fontSize:11, border:'none',
                      cursor:'pointer', fontWeight:600,
                      background: clusterView ? '#0055A4' : 'transparent',
                      color: clusterView ? '#fff' : '#94a3b8' }}>
                    Cluster
                  </button>
                </div>
              )}
            </div>
          </div>

          {noData ? (
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column',
              alignItems: 'center', justifyContent: 'center', gap: 12,
              background: '#f8fafc', borderRadius: 8, padding: 40 }}>
              <Network size={48} color="#94a3b8"/>
              <p style={{ color: '#e2e8f0', fontSize: 15 }}>No ontology data yet</p>
              <button onClick={discover} style={{ padding: '8px 20px', background: '#0055A4',
                color: '#fff', border: 'none', borderRadius: 8, cursor: 'pointer', fontWeight: 600 }}>
                Run Discovery
              </button>
            </div>
          ) : (
            <div style={{ display: 'flex', gap: 12, alignItems: 'flex-start' }}>
              <div style={{ flex: 1, minWidth: 0 }}>

                {/* ── Map Key / Filter Bar ── */}
                <div style={{ marginBottom: 8, background: '#0f172a', borderRadius: 8,
                  border: `1px solid ${legendFiltersActive ? '#334155' : '#1e293b'}`,
                  padding: '7px 14px',
                  display: 'flex', flexWrap: 'wrap', alignItems: 'center',
                  columnGap: 6, rowGap: 5 }}>

                  {/* LINKS label */}
                  <span style={{ color: '#94a3b8', fontSize: 10, fontWeight: 700,
                    textTransform: 'uppercase', letterSpacing: '0.07em', flexShrink: 0,
                    marginRight: 4 }}>
                    Links
                  </span>

                  {/* Relationship type filter buttons */}
                  {Object.entries(RELATION_COLOR).map(([rel, col]) => {
                    const active = activeLinkFilters.has(rel)
                    const faded  = legendFiltersActive && !active
                    return (
                      <div key={rel} onClick={() => toggleLinkFilter(rel)}
                        style={{ display: 'flex', alignItems: 'center', gap: 5, flexShrink: 0,
                          cursor: 'pointer', padding: '3px 9px', borderRadius: 20,
                          background: active ? col + '28' : 'rgba(255,255,255,0.06)',
                          border: `1px solid ${active ? col + 'aa' : '#334155'}`,
                          opacity: faded ? 0.38 : 1,
                          transition: 'all 0.15s',
                          userSelect: 'none' }}>
                        <svg width="20" height="8" style={{ flexShrink: 0 }}>
                          <line x1="0" y1="4" x2="14" y2="4" stroke={col} strokeWidth="2"
                            strokeDasharray={rel === 'ANTI_CORRELATES' ? '3 2' : 'none'}/>
                          <polygon points="14,1 20,4 14,7" fill={col} opacity="0.9"/>
                        </svg>
                        <span style={{ color: active ? '#f1f5f9' : '#cbd5e1', fontSize: 11,
                          fontWeight: active ? 600 : 400 }}>{fmtRelation(rel)}</span>
                      </div>
                    )
                  })}

                  {/* Divider */}
                  <span style={{ width: 1, height: 14, background: '#334155', flexShrink: 0, margin: '0 6px' }}/>

                  {/* DOMAINS label */}
                  <span style={{ color: '#94a3b8', fontSize: 10, fontWeight: 700,
                    textTransform: 'uppercase', letterSpacing: '0.07em', flexShrink: 0,
                    marginRight: 4 }}>
                    Domains
                  </span>

                  {/* Domain filter buttons */}
                  {Object.entries(DOMAIN_COLOR).filter(([d]) => d !== 'other').map(([d, c]) => {
                    const active = activeDomainFilters.has(d)
                    const faded  = legendFiltersActive && !active
                    return (
                      <div key={d} onClick={() => toggleDomainFilter(d)}
                        style={{ display: 'flex', alignItems: 'center', gap: 5, flexShrink: 0,
                          cursor: 'pointer', padding: '3px 9px', borderRadius: 20,
                          background: active ? c + '28' : 'rgba(255,255,255,0.06)',
                          border: `1px solid ${active ? c + 'aa' : '#334155'}`,
                          opacity: faded ? 0.38 : 1,
                          transition: 'all 0.15s',
                          userSelect: 'none' }}>
                        <span style={{ width: 8, height: 8, borderRadius: '50%', background: c,
                          display: 'inline-block', flexShrink: 0,
                          boxShadow: active ? `0 0 5px ${c}88` : 'none' }}/>
                        <span style={{ color: active ? '#f1f5f9' : '#cbd5e1', fontSize: 11,
                          fontWeight: active ? 600 : 400 }}>{fmtDomain(d)}</span>
                      </div>
                    )
                  })}

                  {/* Divider */}
                  <span style={{ width: 1, height: 14, background: '#334155', flexShrink: 0, margin: '0 6px' }}/>

                  {/* All / reset button — shown when filters active, otherwise reading hint */}
                  {legendFiltersActive ? (
                    <button onClick={clearLegendFilters}
                      style={{ background: '#334155', border: '1px solid #475569', borderRadius: 20,
                        color: '#f1f5f9', fontSize: 11, padding: '3px 12px', cursor: 'pointer',
                        fontWeight: 600, flexShrink: 0, display: 'flex', alignItems: 'center', gap: 5 }}>
                      <span style={{ fontSize: 10 }}>✕</span> Show All
                    </button>
                  ) : (
                    <span style={{ color: '#64748b', fontSize: 11, flexShrink: 0 }}>
                      ⬤ size = influence &nbsp;·&nbsp; click node to inspect &nbsp;·&nbsp; scroll to zoom
                    </span>
                  )}
                </div>

                {/* ── Canvas: mode-aware ─────────────────────────────── */}
                {graphMode === 'focus' ? (
                  <FocusGraph
                    nodes={focusNodes}
                    edges={focusEdges}
                    focalKey={focusNode}
                    influenceScores={influenceScores}
                    linkFilter={activeLinkFilters}
                    domainFilter={activeDomainFilters}
                  />
                ) : graphMode === 'sensitivity' ? (
                  <ForceGraph
                    nodes={weightedNodes}
                    edges={weightedEdges}
                    selected={selected}
                    onSelect={setSelected}
                    linkFilter={activeLinkFilters}
                    domainFilter={activeDomainFilters}
                  />
                ) : clusterView ? (
                  <ClusterGraph
                    nodes={graph.nodes}
                    edges={graph.edges}
                    selected={selected}
                    onSelect={setSelected}
                    linkFilter={activeLinkFilters}
                    domainFilter={activeDomainFilters}
                  />
                ) : (
                  <ForceGraph
                    nodes={graph.nodes}
                    edges={graph.edges}
                    selected={selected}
                    onSelect={setSelected}
                    linkFilter={activeLinkFilters}
                    domainFilter={activeDomainFilters}
                  />
                )}
              </div>

              {/* ── Right sidebar: mode-aware ────────────────────────── */}
              <div style={{ display: 'flex', flexDirection: 'column', gap: 12, width: 260, flexShrink: 0 }}>

                {graphMode === 'focus' && focusNode && (
                  <InfluencePanel
                    focalKey={focusNode}
                    nodes={focusNodes}
                    influenceScores={influenceScores}
                    narrative={focusNarrative}
                  />
                )}

                {graphMode === 'sensitivity' && (
                  <SensitivityPanel
                    sensitivity={sensitivity}
                    onChange={setSensitivity}
                    onReset={() => setSensitivity({ ...DEFAULT_SENSITIVITY })}
                    scenarios={scenarios}
                    onSave={(name, s) => setScenarios(prev => [...prev, { name, s }])}
                    onLoad={s => setSensitivity({ ...s })}
                    narrative={sensitivityNarrative}
                  />
                )}

                {graphMode === 'standard' && selected ? (
                  <NodeInspector
                    nodeKey={selected}
                    nodes={graph.nodes}
                    edges={graph.edges}
                    onClose={() => setSelected(null)}
                  />
                ) : graphMode === 'standard' && stats?.top_nodes_by_pagerank?.length > 0 && (
                  <div style={{ background: '#1e293b', borderRadius: 8, border: '1px solid #334155', padding: 14 }}>
                    <div style={{ marginBottom: 10 }}>
                      <div style={{ color: '#e2e8f0', fontSize: 12, fontWeight: 700 }}>
                        Most Influential KPIs
                      </div>
                      <div style={{ color: '#e2e8f0', fontSize: 10, marginTop: 2 }}>
                        KPIs other metrics depend on most — click to inspect
                      </div>
                    </div>
                    {stats.top_nodes_by_pagerank.map((n, i) => (
                      <div key={n.key} onClick={() => setSelected(n.key)}
                        style={{ display: 'flex', alignItems: 'center', gap: 8,
                          padding: '7px 6px', borderRadius: 6, cursor: 'pointer',
                          borderBottom: '1px solid #0f172a',
                          transition: 'background 0.1s' }}
                        onMouseEnter={e => e.currentTarget.style.background = '#0f172a'}
                        onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
                        <span style={{ color: '#334155', fontSize: 11, width: 16, textAlign: 'center',
                          fontWeight: 700 }}>{i + 1}</span>
                        <div style={{ flex: 1, minWidth: 0 }}>
                          <div style={{ color: '#f1f5f9', fontSize: 12, fontWeight: 600 }}>{n.name}</div>
                          <div style={{ color: DOMAIN_COLOR[n.domain] || '#94a3b8', fontSize: 10 }}>{fmtDomain(n.domain)}</div>
                        </div>
                        <div style={{ textAlign: 'right', flexShrink: 0 }}>
                          <div style={{ color: '#00AEEF', fontWeight: 700, fontSize: 13 }}>
                            {Math.round((n.pagerank || 0) * 100)}%
                          </div>
                          <div style={{ color: '#e2e8f0', fontSize: 9 }}>influence</div>
                        </div>
                      </div>
                    ))}
                  </div>
                )}

                {graphMode === 'standard' && stats?.edge_type_distribution && (
                  <div style={{ background: '#1e293b', borderRadius: 8, border: '1px solid #334155', padding: 14 }}>
                    <div style={{ marginBottom: 10 }}>
                      <div style={{ color: '#e2e8f0', fontSize: 12, fontWeight: 700 }}>Relationship Breakdown</div>
                      <div style={{ color: '#e2e8f0', fontSize: 10, marginTop: 2 }}>How KPIs are connected</div>
                    </div>
                    {Object.entries(stats.edge_type_distribution).map(([rel, cnt]) => {
                      const rc = RELATION_COLOR[rel] || '#94a3b8'
                      const total = Object.values(stats.edge_type_distribution).reduce((s, v) => s + v, 0)
                      const pct = total ? Math.round(cnt / total * 100) : 0
                      return (
                        <div key={rel} style={{ marginBottom: 8 }}>
                          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 3 }}>
                            <span style={{ fontSize: 12, color: rc, fontWeight: 600 }}>{fmtRelation(rel)}</span>
                            <span style={{ color: '#e2e8f0', fontSize: 11 }}>{cnt} links</span>
                          </div>
                          <div style={{ height: 3, background: '#0f172a', borderRadius: 2, overflow: 'hidden' }}>
                            <div style={{ height: '100%', width: `${pct}%`, background: rc, borderRadius: 2, opacity: 0.7 }}/>
                          </div>
                        </div>
                      )
                    })}
                  </div>
                )}
              </div>
            </div>
          )}
        </>
      )}

      {/* Recommendations tab */}
      {tab === 'recs' && (
        <div>
          {/* Filter pills */}
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 16 }}>
            {REC_FILTERS.map(f => {
              const active = recFilter === f.id
              const cnt = f.id === 'all'
                ? recs.length
                : recs.filter(r => r.rec_type === f.id).length
              return (
                <button key={f.id} onClick={() => setRecFilter(f.id)}
                  style={{ padding: '5px 14px', borderRadius: 999, fontSize: 12,
                    border: 'none', cursor: 'pointer', fontWeight: 500,
                    background: active ? '#0055A4' : '#334155',
                    color:      active ? '#fff'    : '#e2e8f0' }}>
                  {f.label}{cnt > 0 && f.id !== 'all' ? ` (${cnt})` : ''}
                </button>
              )
            })}
          </div>

          {filteredRecs.length === 0 ? (
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center',
              padding: 40, background: '#1e293b', borderRadius: 8, gap: 12 }}>
              <Sparkles size={40} color="#94a3b8"/>
              <p style={{ color: '#e2e8f0' }}>
                {recs.length === 0
                  ? 'No recommendations yet — run Discovery first.'
                  : 'No recommendations of this type.'}
              </p>
            </div>
          ) : (
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
              {filteredRecs.map(rec => (
                <RecCard key={rec.id} rec={rec}
                  nodes={graph.nodes}
                  onDismiss={dismiss}/>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
