import{a as e,n as t,t as n}from"./jsx-runtime-D57Vegw5.js";import{t as r}from"./createLucideIcon-D2PiFcvX.js";import{t as i}from"./download-BtdPflsz.js";import{Y as a,r as o,z as s}from"./index-BVDOoy9r.js";import{B as c,L as l,R as u}from"./generateCategoricalChart-E4EYuRfn.js";import{i as d,n as f,r as p,t as m}from"./RadarChart-CpUVMvz_.js";var h=r(`FileDown`,[[`path`,{d:`M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z`,key:`1rqfz7`}],[`path`,{d:`M14 2v4a2 2 0 0 0 2 2h4`,key:`tnqrlb`}],[`path`,{d:`M12 18v-6`,key:`17g6i2`}],[`path`,{d:`m9 15 3 3 3-3`,key:`1npd3o`}]]),g=r(`GitCompare`,[[`circle`,{cx:`18`,cy:`18`,r:`3`,key:`1xkwt0`}],[`circle`,{cx:`6`,cy:`6`,r:`3`,key:`1lh9wr`}],[`path`,{d:`M13 6h3a2 2 0 0 1 2 2v7`,key:`1yeb86`}],[`path`,{d:`M11 18H8a2 2 0 0 1-2-2V9`,key:`19pyzm`}]]),_=e(t(),1),v=n(),y=[`Jan`,`Feb`,`Mar`,`Apr`,`May`,`Jun`,`Jul`,`Aug`,`Sep`,`Oct`,`Nov`,`Dec`],b=[1,2,3,4,5,6,7,8,9,10,11,12],x={pct:e=>`${e?.toFixed(1)}%`,days:e=>`${e?.toFixed(1)}d`,months:e=>`${e?.toFixed(1)}mo`,ratio:e=>`${e?.toFixed(2)}x`,usd:e=>`$${e?.toFixed(1)}K`};function S(e,t){return e==null?`—`:(x[t]||(e=>e?.toFixed(2)))(e)}function C(e,t,n){if(!t)return 100;let r=e/t*100;return n===`higher`?Math.min(r,135):Math.max(200-r,30)}function w(e){return{green:`hm-green`,yellow:`hm-yellow`,red:`hm-red`,grey:`hm-grey`}[e]||`hm-grey`}function ee(e){return{green:`badge-green`,yellow:`badge-yellow`,red:`badge-red`,grey:`badge-grey`}[e]||`badge-grey`}function T(e,t,n){if(e==null||!t)return`grey`;let r=e/t;return n===`higher`?r>=.98?`green`:r>=.9?`yellow`:`red`:r<=1.02?`green`:r<=1.1?`yellow`:`red`}function E(e,t){let n={};e.monthly?.forEach(e=>{n[e.period]=e.value});let r=t?.length?t:e.monthly?.map(e=>e.period).sort()||[],i=0;for(let t=r.length-1;t>=0&&T(n[r[t]],e.target,e.direction)===`red`;t--)i++;return i}function D(e,t){if(!e.monthly?.length||!t?.length)return e.avg;let n=e.monthly.filter(e=>{let n=parseInt(e.period.split(`-`)[1],10);return t.includes(n)&&e.value!=null});return n.length?n.reduce((e,t)=>e+t.value,0)/n.length:e.avg}function O(e){if(!e?.length)return`None`;if(e.length===12)return`Full Year`;let t=[...e].sort((e,t)=>e-t),n=t.join(`,`);return n===`1,2,3,4,5,6`?`H1`:n===`7,8,9,10,11,12`?`H2`:n===`1,2,3`?`Q1`:n===`4,5,6`?`Q2`:n===`7,8,9`?`Q3`:n===`10,11,12`?`Q4`:t.length<=3?t.map(e=>y[e-1]).join(`, `):`${t.length} months`}function k(e){let t=new Date().toLocaleDateString(`en-GB`,{day:`numeric`,month:`long`,year:`numeric`}),n=e.filter(e=>e.fy_status===`green`).length,r=e.filter(e=>e.fy_status===`yellow`).length,i=e.filter(e=>e.fy_status===`red`).length,a=e.map(e=>{let t={};e.monthly?.forEach(e=>{t[parseInt(e.period.split(`-`)[1],10)]=e.value});let n=[12,11,10,9].find(e=>t[e]!=null),r=n?[n-1,n-2].find(e=>t[e]!=null):null,i=n?t[n]:null,a=r?t[r]:null,o=i!=null&&a!=null?i>a?`▲`:i<a?`▼`:`→`:`—`,s=o===`▲`?e.direction===`higher`:o===`▼`?e.direction!==`higher`:null,c=s===!0?`#16a34a`:s===!1?`#dc2626`:`#94a3b8`,l={green:`#16a34a`,yellow:`#d97706`,red:`#dc2626`}[e.fy_status]||`#94a3b8`;return`<tr>
      <td>${e.name}</td>
      <td style="text-align:right">${S(e.target,e.unit)}</td>
      <td style="text-align:right">${S(e.avg,e.unit)}</td>
      <td style="text-align:right">${S(i,e.unit)}</td>
      <td style="color:${l};font-weight:700;text-align:center">${e.fy_status?.toUpperCase()||`—`}</td>
      <td style="color:${c};font-weight:700;text-align:center">${o}</td>
    </tr>`}).join(``),o=`<!DOCTYPE html>
<html><head><title>Board Performance Pack — FY 2025</title>
<meta charset="utf-8"/>
<style>
  * { box-sizing: border-box; }
  body { font-family: -apple-system, Helvetica Neue, Arial, sans-serif; color: #1e293b; padding: 48px; max-width: 860px; margin: 0 auto; font-size: 13px; }
  h1  { font-size: 20px; font-weight: 800; color: #0055A4; margin: 0 0 4px; }
  .sub { color: #64748b; font-size: 12px; margin-bottom: 28px; }
  .summary { display: flex; gap: 0; margin-bottom: 28px; border: 1px solid #e2e8f0; border-radius: 10px; overflow: hidden; }
  .s-item { flex: 1; padding: 16px 20px; text-align: center; border-right: 1px solid #e2e8f0; }
  .s-item:last-child { border-right: none; }
  .s-num  { font-size: 30px; font-weight: 900; line-height: 1; }
  .s-lbl  { font-size: 10px; color: #64748b; text-transform: uppercase; letter-spacing: 0.06em; margin-top: 4px; }
  table { width: 100%; border-collapse: collapse; margin-top: 4px; }
  thead th { background: #f1f5f9; padding: 8px 12px; color: #64748b; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; border-bottom: 1px solid #e2e8f0; }
  thead th:first-child { text-align: left; }
  tbody td { padding: 8px 12px; border-bottom: 1px solid #f8fafc; }
  tbody tr:last-child td { border-bottom: none; }
  .footer { margin-top: 28px; color: #94a3b8; font-size: 10px; border-top: 1px solid #e2e8f0; padding-top: 14px; }
  @media print { body { padding: 24px; } .no-print { display: none; } }
</style>
</head><body>
<h1>FY 2025 — Board Performance Pack</h1>
<div class="sub">Signals Intelligence · Prepared ${t}</div>
<div class="summary">
  <div class="s-item"><div class="s-num" style="color:#16a34a">${n}</div><div class="s-lbl">On Target</div></div>
  <div class="s-item"><div class="s-num" style="color:#d97706">${r}</div><div class="s-lbl">Needs Attention</div></div>
  <div class="s-item"><div class="s-num" style="color:#dc2626">${i}</div><div class="s-lbl">Critical</div></div>
  <div class="s-item"><div class="s-num" style="color:#475569">${e.length}</div><div class="s-lbl">Total KPIs</div></div>
</div>
<table>
  <thead><tr>
    <th>KPI</th>
    <th style="text-align:right">Target</th>
    <th style="text-align:right">FY Average</th>
    <th style="text-align:right">Latest</th>
    <th style="text-align:center">Status</th>
    <th style="text-align:center">Trend</th>
  </tr></thead>
  <tbody>${a}</tbody>
</table>
<div class="footer">Confidential — For board distribution only &nbsp;·&nbsp; Signals Intelligence Platform &nbsp;·&nbsp; ${t}</div>
<div class="no-print" style="margin-top:24px;text-align:center">
  <button onclick="window.print()" style="padding:8px 24px;background:#0055A4;color:#fff;border:none;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer">
    Print / Save as PDF
  </button>
</div>
</body></html>`,s=window.open(``,`_blank`);s.document.write(o),s.document.close(),setTimeout(()=>s.focus(),300)}function A(e,t,n){let r=new Date().toLocaleDateString(`en-GB`,{day:`numeric`,month:`long`,year:`numeric`}),i=e.filter(e=>e.fy_status===`green`).length,a=e.filter(e=>e.fy_status===`yellow`).length,o=e.filter(e=>e.fy_status===`red`).length,s=O(t),c=O(n),l=t?.length&&n?.length?`Period comparison: <strong>${s}</strong> vs <strong>${c}</strong>`:``,u=`
    <div class="slide">
      <div class="slide-header"><span class="logo">Signals Intelligence</span><span class="date">${r}</span></div>
      <div class="slide-body center">
        <h1 class="slide-title">FY 2025 Performance Review</h1>
        <p class="slide-sub">KPI Fingerprint &amp; Trend Analysis${l?` · `+l:``}</p>
        <div class="pill-row">
          <span class="pill green">${i} On Target</span>
          <span class="pill yellow">${a} Watch</span>
          <span class="pill red">${o} Critical</span>
          <span class="pill grey">${e.length} Total KPIs</span>
        </div>
      </div>
      <div class="slide-footer">Confidential — Not for distribution</div>
    </div>`,d=e.map(e=>{let r={green:`#16a34a`,yellow:`#d97706`,red:`#dc2626`}[e.fy_status]||`#94a3b8`,i=t?.length?D(e,t):null,a=n?.length?D(e,n):null,o=i!=null&&a!=null?i-a:null,s=o==null?`—`:`<span style="color:${o>0&&e.direction===`higher`||o<0&&e.direction!==`higher`?`#16a34a`:o===0?`#94a3b8`:`#dc2626`}">${o>0?`▲`:o<0?`▼`:`→`} ${S(Math.abs(o),e.unit)}</span>`;return`<tr>
      <td>${e.name}</td>
      <td style="text-align:right">${S(e.target,e.unit)}</td>
      <td style="text-align:right">${S(e.avg,e.unit)}</td>
      ${i==null?``:`<td style="text-align:right">${S(i,e.unit)}</td>`}
      ${a==null?``:`<td style="text-align:right">${S(a,e.unit)}</td>`}
      ${o==null?``:`<td style="text-align:center">${s}</td>`}
      <td style="color:${r};font-weight:700;text-align:center">${e.fy_status?.toUpperCase()||`—`}</td>
    </tr>`}).join(``),f=`
    <div class="slide">
      <div class="slide-header"><span class="logo">Signals Intelligence</span><span class="date">${r}</span></div>
      <div class="slide-body">
        <h2 class="section-title">Full KPI Scorecard</h2>
        <table>
          <thead><tr>
            <th>KPI</th><th style="text-align:right">Target</th><th style="text-align:right">FY Avg</th>
            ${t?.length&&n?.length?`<th style="text-align:right">${s}</th><th style="text-align:right">${c}</th><th style="text-align:center">Δ</th>`:``}
            <th style="text-align:center">Status</th>
          </tr></thead>
          <tbody>${d}</tbody>
        </table>
      </div>
      <div class="slide-footer">Slide 2 of 4 — Confidential</div>
    </div>`,p=e.filter(e=>e.fy_status===`red`||e.fy_status===`yellow`),m=p.map(e=>{let t=e.fy_status===`red`?`#dc2626`:`#d97706`,n=e.fy_status===`red`?`#fef2f2`:`#fffbeb`,r=e.fy_status===`red`?`#fca5a5`:`#fcd34d`,i=e.target?((e.avg/e.target-1)*100).toFixed(1):null;return`
      <div class="kpi-card" style="border-color:${r};background:${n}">
        <div class="kpi-card-header">
          <span class="kpi-name">${e.name}</span>
          <span class="kpi-badge" style="color:${t}">${e.fy_status?.toUpperCase()}</span>
        </div>
        <div class="kpi-stats">
          <span>Avg: <strong>${S(e.avg,e.unit)}</strong></span>
          <span>Target: <strong>${S(e.target,e.unit)}</strong></span>
          ${i==null?``:`<span style="color:${t}">Gap: ${i>0?`+`:``}${i}%</span>`}
        </div>
      </div>`}).join(``),h=`
    <div class="slide">
      <div class="slide-header"><span class="logo">Signals Intelligence</span><span class="date">${r}</span></div>
      <div class="slide-body">
        <h2 class="section-title">Critical &amp; Watch — Items Requiring Action</h2>
        ${p.length?`<div class="card-grid">${m}</div>`:`<p style="color:#64748b;margin-top:24px">No critical or watch KPIs — all metrics on target.</p>`}
      </div>
      <div class="slide-footer">Slide 3 of 4 — Confidential</div>
    </div>`,g=e.filter(e=>e.fy_status===`green`),_=g.map(e=>{let t=e.target?((e.avg/e.target-1)*100).toFixed(1):null,n=t==null?null:e.direction===`higher`?(t>=0?`+`:``)+t+`%`:(t<=0?``:`+`)+t+`%`;return`
      <div class="kpi-card" style="border-color:#86efac;background:#f0fdf4">
        <div class="kpi-card-header">
          <span class="kpi-name">${e.name}</span>
          <span class="kpi-badge" style="color:#16a34a">ON TARGET</span>
        </div>
        <div class="kpi-stats">
          <span>Avg: <strong>${S(e.avg,e.unit)}</strong></span>
          <span>Target: <strong>${S(e.target,e.unit)}</strong></span>
          ${n==null?``:`<span style="color:#16a34a">${n} vs target</span>`}
        </div>
      </div>`}).join(``),v=`<!DOCTYPE html>
<html><head><title>FY 2025 — KPI Performance Presentation</title>
<meta charset="utf-8"/>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, Helvetica Neue, Arial, sans-serif; background: #f1f5f9; }
  .slide {
    width: 297mm; min-height: 210mm; background: #fff;
    display: flex; flex-direction: column; margin: 0 auto 12px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.10); page-break-after: always;
  }
  .slide-header {
    display: flex; justify-content: space-between; align-items: center;
    padding: 14px 32px; background: #0055A4; color: #fff;
  }
  .logo { font-size: 13px; font-weight: 700; letter-spacing: 0.04em; }
  .date { font-size: 11px; opacity: 0.8; }
  .slide-body { flex: 1; padding: 28px 32px; overflow: hidden; }
  .slide-footer {
    padding: 10px 32px; background: #f8fafc; border-top: 1px solid #e2e8f0;
    font-size: 10px; color: #94a3b8; text-align: right;
  }
  .center { display: flex; flex-direction: column; align-items: center; justify-content: center; text-align: center; }
  .slide-title { font-size: 26px; font-weight: 800; color: #0f172a; margin-bottom: 6px; }
  .slide-sub { font-size: 13px; color: #64748b; margin-bottom: 20px; }
  .pill-row { display: flex; gap: 8px; flex-wrap: wrap; justify-content: center; }
  .pill { padding: 4px 14px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .pill.green  { background: #dcfce7; color: #16a34a; }
  .pill.yellow { background: #fef9c3; color: #a16207; }
  .pill.red    { background: #fee2e2; color: #dc2626; }
  .pill.grey   { background: #f1f5f9; color: #475569; }
  .section-title { font-size: 16px; font-weight: 700; color: #0055A4; margin-bottom: 16px; border-bottom: 2px solid #e2e8f0; padding-bottom: 8px; }
  table { width: 100%; border-collapse: collapse; font-size: 11px; }
  thead th { background: #f1f5f9; padding: 7px 10px; color: #64748b; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; border-bottom: 1px solid #e2e8f0; text-align: left; }
  tbody td { padding: 7px 10px; border-bottom: 1px solid #f8fafc; color: #1e293b; }
  .card-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-top: 4px; }
  .kpi-card { border: 1px solid #e2e8f0; border-radius: 8px; padding: 12px 14px; }
  .kpi-card-header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 8px; }
  .kpi-name { font-size: 12px; font-weight: 600; color: #1e293b; }
  .kpi-badge { font-size: 10px; font-weight: 700; flex-shrink: 0; margin-left: 8px; }
  .kpi-stats { display: flex; gap: 12px; font-size: 11px; color: #64748b; flex-wrap: wrap; }
  @media print {
    body { background: white; }
    .no-print { display: none; }
    .slide { box-shadow: none; margin: 0; page-break-after: always; }
  }
</style>
</head>
<body>
${u}
${f}
${h}
${`
    <div class="slide">
      <div class="slide-header"><span class="logo">Signals Intelligence</span><span class="date">${r}</span></div>
      <div class="slide-body">
        <h2 class="section-title">Strong Performers — On or Above Target</h2>
        ${g.length?`<div class="card-grid">${_}</div>`:`<p style="color:#64748b;margin-top:24px">No green KPIs currently.</p>`}
      </div>
      <div class="slide-footer">Slide 4 of 4 — Confidential &nbsp;·&nbsp; Signals Intelligence Platform</div>
    </div>`}
<div class="no-print" style="text-align:center;padding:24px">
  <button onclick="window.print()" style="padding:10px 28px;background:#0055A4;color:#fff;border:none;border-radius:8px;font-size:14px;font-weight:700;cursor:pointer">
    🖨 Print / Save as PDF
  </button>
</div>
</body></html>`,y=window.open(``,`_blank`);y.document.write(v),y.document.close(),setTimeout(()=>y.focus(),300)}function j(e){let t=[[`KPI`,`Key`,`Unit`,`Direction`,`Target`,...y,`FY Average`,`Status`],...e.map(e=>{let t={};e.monthly?.forEach(e=>{t[parseInt(e.period.split(`-`)[1],10)]=e.value});let n=b.map(e=>t[e]==null?``:t[e]);return[`"${e.name}"`,e.key,e.unit||``,e.direction||``,e.target==null?``:e.target,...n,e.avg==null?``:e.avg,e.fy_status||``]})].map(e=>e.join(`,`)).join(`
`),n=new Blob([t],{type:`text/csv;charset=utf-8;`}),r=URL.createObjectURL(n),i=document.createElement(`a`);i.href=r,i.download=`kpi-fingerprint-fy2025-${new Date().toISOString().slice(0,10)}.csv`,i.click(),URL.revokeObjectURL(r)}var M=[{label:`H1`,a:[1,2,3,4,5,6],b:[7,8,9,10,11,12]},{label:`H2`,a:[7,8,9,10,11,12],b:[1,2,3,4,5,6]},{label:`Q1v3`,a:[1,2,3],b:[7,8,9]},{label:`Q2v4`,a:[4,5,6],b:[10,11,12]},{label:`Last 3 vs Prior 3`,a:[10,11,12],b:[7,8,9]}];function N({periodA:e,periodB:t,onChange:n,onClose:r}){function i(r,i){let a=r===`A`?e:t;n(r,a.includes(i)?a.filter(e=>e!==i):[...a,i])}function a(e){n(`A`,e.a),n(`B`,e.b)}return(0,v.jsxs)(`div`,{className:`mt-3 p-4 bg-slate-50 border border-slate-200 rounded-xl space-y-4`,children:[(0,v.jsxs)(`div`,{className:`flex items-center justify-between`,children:[(0,v.jsx)(`p`,{className:`text-xs font-semibold text-slate-600 uppercase tracking-wider`,children:`Custom Period Comparison`}),(0,v.jsx)(`button`,{onClick:r,className:`text-slate-400 hover:text-slate-600`,children:(0,v.jsx)(o,{size:14})})]}),(0,v.jsxs)(`div`,{className:`flex items-center gap-2 flex-wrap`,children:[(0,v.jsx)(`span`,{className:`text-[11px] text-slate-500 font-medium`,children:`Presets:`}),M.map(e=>(0,v.jsx)(`button`,{onClick:()=>a(e),className:`px-2.5 py-1 text-[11px] font-medium bg-white border border-slate-200 rounded-full hover:border-[#0055A4] hover:text-[#0055A4] transition-colors`,children:e.label},e.label))]}),(0,v.jsxs)(`div`,{children:[(0,v.jsxs)(`p`,{className:`text-[11px] font-semibold text-blue-600 mb-1.5`,children:[`Period A (blue) — `,O(e),e.length>0&&(0,v.jsx)(`button`,{onClick:()=>n(`A`,[]),className:`ml-2 text-[10px] text-slate-400 hover:text-slate-600`,children:`clear`})]}),(0,v.jsx)(`div`,{className:`flex flex-wrap gap-1.5`,children:b.map(t=>(0,v.jsx)(`button`,{onClick:()=>i(`A`,t),className:`w-9 h-7 text-[11px] font-medium rounded transition-all ${e.includes(t)?`bg-blue-600 text-white border border-blue-600`:`bg-white text-slate-500 border border-slate-200 hover:border-blue-300`}`,children:y[t-1]},t))})]}),(0,v.jsxs)(`div`,{children:[(0,v.jsxs)(`p`,{className:`text-[11px] font-semibold text-amber-600 mb-1.5`,children:[`Period B (amber) — `,O(t),t.length>0&&(0,v.jsx)(`button`,{onClick:()=>n(`B`,[]),className:`ml-2 text-[10px] text-slate-400 hover:text-slate-600`,children:`clear`})]}),(0,v.jsx)(`div`,{className:`flex flex-wrap gap-1.5`,children:b.map(e=>(0,v.jsx)(`button`,{onClick:()=>i(`B`,e),className:`w-9 h-7 text-[11px] font-medium rounded transition-all ${t.includes(e)?`bg-amber-500 text-white border border-amber-500`:`bg-white text-slate-500 border border-slate-200 hover:border-amber-300`}`,children:y[e-1]},e))})]})]})}function P({fingerprint:e,onKpiClick:t}){let[n,r]=(0,_.useState)(!1),[b,x]=(0,_.useState)(!1),[M,P]=(0,_.useState)([7,8,9,10,11,12]),[F,I]=(0,_.useState)([1,2,3,4,5,6]),[L,R]=(0,_.useState)(!1),[z,B]=(0,_.useState)({}),[V,H]=(0,_.useState)(null),[U,W]=(0,_.useState)(``);(0,_.useEffect)(()=>{a.get(`/api/annotations`).then(e=>{let t={};(e.data.annotations||[]).forEach(e=>{t[`${e.kpi_key}::${e.period}`]=e}),B(t)}).catch(()=>{})},[]);function G(e,t){e===`A`?P(t):I(t)}if(!e?.length)return null;let K=n&&M.length>0,q=n&&F.length>0,J=e.filter(e=>e.avg!=null&&e.target!=null).slice(0,12).map(e=>{let t=D(e,M),n=D(e,F);return{kpi:e.name.length>20?e.name.slice(0,18)+`…`:e.name,actual:Math.min(C(K?t:e.avg,e.target,e.direction),135),target:100,prior:q?Math.min(C(n,e.target,e.direction),135):void 0}}),Y=e.filter(e=>e.monthly?.length),X=[...new Set(Y.flatMap(e=>e.monthly.map(e=>e.period)))].sort(),Z=[...new Set(X.map(e=>e.slice(0,4)))].sort(),Q=Z.length>1;function $(e){let[t,n]=e.split(`-`),r=y[parseInt(n,10)-1];return Q?`${r}'${t.slice(2)}`:r}let te=Z.map(e=>({yr:e,count:X.filter(t=>t.startsWith(e)).length}));return(0,v.jsxs)(`div`,{className:`space-y-6 max-w-7xl`,children:[(0,v.jsxs)(`div`,{className:`card p-6`,children:[(0,v.jsxs)(`div`,{className:`flex items-center justify-between mb-1`,children:[(0,v.jsxs)(`div`,{children:[(0,v.jsx)(`h3`,{className:`text-sm font-semibold text-slate-700`,children:`Performance Radar — % of Target`}),(0,v.jsxs)(`p`,{className:`text-xs text-slate-400 mt-0.5`,children:[`100 = on target · Outward = outperforming · Inward = gap`,n&&K&&q&&(0,v.jsxs)(`span`,{className:`ml-2 text-amber-600 font-medium`,children:[`· `,(0,v.jsx)(`span`,{className:`text-blue-600`,children:O(M)}),` vs `,(0,v.jsx)(`span`,{className:`text-amber-600`,children:O(F)})]})]})]}),(0,v.jsxs)(`div`,{className:`flex items-center gap-2`,children:[(0,v.jsxs)(`button`,{onClick:()=>{r(e=>!e),x(!1)},className:`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border transition-all ${n?`bg-amber-50 text-amber-700 border-amber-300`:`bg-slate-50 text-slate-500 border-slate-200 hover:border-slate-300`}`,children:[(0,v.jsx)(g,{size:12}),n?`${O(M)} vs ${O(F)}`:`Compare Periods`]}),n&&(0,v.jsx)(`button`,{onClick:()=>x(e=>!e),className:`px-3 py-1.5 rounded-lg text-xs font-medium border transition-all ${b?`bg-blue-50 text-blue-700 border-blue-300`:`bg-slate-50 text-slate-500 border-slate-200 hover:border-slate-300`}`,children:b?`Close Picker`:`Select Months…`})]})]}),n&&b&&(0,v.jsx)(N,{periodA:M,periodB:F,onChange:G,onClose:()=>x(!1)}),(0,v.jsx)(l,{width:`100%`,height:380,children:(0,v.jsxs)(m,{data:J,margin:{top:10,right:30,bottom:10,left:30},children:[(0,v.jsx)(d,{stroke:`#e2e8f0`}),(0,v.jsx)(p,{dataKey:`kpi`,tick:{fill:`#64748b`,fontSize:10}}),(0,v.jsx)(f,{name:`Target (100%)`,dataKey:`target`,stroke:`#94a3b8`,strokeWidth:1.5,strokeDasharray:`5 3`,fill:`none`}),q&&(0,v.jsx)(f,{name:`${O(F)} (Period B)`,dataKey:`prior`,stroke:`#f59e0b`,fill:`#f59e0b`,fillOpacity:.08,strokeWidth:1.5,strokeDasharray:`6 4`}),(0,v.jsx)(f,{name:n&&K?`${O(M)} (Period A)`:`Actual`,dataKey:`actual`,stroke:`#0055A4`,fill:`#0055A4`,fillOpacity:.18,strokeWidth:2,dot:{fill:`#0055A4`,r:3}}),(0,v.jsx)(u,{contentStyle:{background:`#fff`,border:`1px solid #e2e8f0`,borderRadius:8,fontSize:11,color:`#0f172a`},formatter:(e,t)=>[`${e?.toFixed(1)}% of target`,t]}),(0,v.jsx)(c,{wrapperStyle:{fontSize:11,color:`#64748b`}})]})})]}),(()=>{let t=e.filter(e=>e.fy_status===`red`),n=e.filter(e=>e.fy_status===`yellow`),r=e.filter(e=>e.fy_status===`green`),i=e.length,a={};e.forEach(e=>{let t=e.domain||`Other`;a[t]||(a[t]={red:0,yellow:0,green:0}),e.fy_status===`red`?a[t].red++:e.fy_status===`yellow`?a[t].yellow++:e.fy_status===`green`&&a[t].green++});let o=Object.entries(a).map(([e,t])=>({name:e,...t,score:t.red*3+t.yellow})).sort((e,t)=>t.score-e.score),s=o[0],c=[...o].sort((e,t)=>t.green-e.green)[0],l=e.map(e=>({k:e,streak:E(e,X)})).filter(e=>e.streak>=3).sort((e,t)=>t.streak-e.streak)[0];r.length&&r.map(e=>({k:e,pct:e.avg!=null&&e.target?e.avg/e.target:0})).sort((e,t)=>(n=>n.direction===`higher`?t.pct-e.pct:e.pct-t.pct)(t.k)).sort((e,t)=>t.pct-e.pct)[0]?.k;let u=t.length>i*.4?{bg:`#fef2f2`,border:`#fca5a5`,dot:`#dc2626`,label:`CRITICAL ATTENTION REQUIRED`}:t.length>i*.2?{bg:`#fffbeb`,border:`#fcd34d`,dot:`#d97706`,label:`NEEDS ATTENTION`}:{bg:`#f0fdf4`,border:`#86efac`,dot:`#059669`,label:`BROADLY ON TRACK`},d=[];return d.push(`${i} KPI${i===1?``:`s`} tracked this period — `+(t.length?`${t.length} critical`+(n.length||r.length?`, `:``):``)+(n.length?`${n.length} need${n.length===1?`s`:``} attention`+(r.length?`, `:``):``)+(r.length?`${r.length} on or above target`:``)+`.`),s&&s.red+s.yellow>0&&d.push(`${s.name.replace(/_/g,` `)} is the most pressured domain with ${s.red} critical and ${s.yellow} watch KPI${s.yellow===1?``:`s`}.`),l&&d.push(`${l.k.name} has been below target for ${l.streak} consecutive months — immediate review recommended.`),c&&c.green>=2&&d.push(`${c.name.replace(/_/g,` `)} is the strongest domain, with ${c.green} KPI${c.green===1?``:`s`} performing on or above target.`),t.length===0&&n.length===0&&d.push(`All KPIs are on or above target — focus on sustaining performance and monitoring leading indicators.`),(0,v.jsx)(`div`,{className:`rounded-xl border p-5`,style:{background:u.bg,borderColor:u.border},children:(0,v.jsxs)(`div`,{className:`flex items-start gap-3`,children:[(0,v.jsx)(`span`,{className:`w-2.5 h-2.5 rounded-full flex-shrink-0 mt-1`,style:{background:u.dot}}),(0,v.jsxs)(`div`,{className:`flex-1 min-w-0`,children:[(0,v.jsxs)(`div`,{className:`flex items-center gap-2 mb-2`,children:[(0,v.jsx)(`p`,{className:`text-[10px] font-bold uppercase tracking-widest`,style:{color:u.dot},children:u.label}),(0,v.jsxs)(`div`,{className:`flex items-center gap-1.5`,children:[t.length>0&&(0,v.jsxs)(`span`,{className:`inline-flex items-center gap-1 text-[10px] font-semibold bg-red-100 text-red-700 border border-red-200 px-2 py-0.5 rounded-full`,children:[t.length,` Critical`]}),n.length>0&&(0,v.jsxs)(`span`,{className:`inline-flex items-center gap-1 text-[10px] font-semibold bg-amber-100 text-amber-700 border border-amber-200 px-2 py-0.5 rounded-full`,children:[n.length,` Watch`]}),r.length>0&&(0,v.jsxs)(`span`,{className:`inline-flex items-center gap-1 text-[10px] font-semibold bg-emerald-100 text-emerald-700 border border-emerald-200 px-2 py-0.5 rounded-full`,children:[r.length,` On Target`]})]})]}),(0,v.jsx)(`p`,{className:`text-xs text-slate-700 leading-relaxed`,children:d.join(` `)})]})]})})})(),(0,v.jsxs)(`div`,{className:`card p-6 overflow-x-auto`,children:[(0,v.jsxs)(`div`,{className:`flex items-center justify-between mb-1`,children:[(0,v.jsxs)(`div`,{children:[(0,v.jsxs)(`h3`,{className:`text-sm font-semibold text-slate-700`,children:[`KPI Heat Map`,X.length>0&&(0,v.jsxs)(`span`,{className:`ml-2 text-slate-400 font-normal text-xs`,children:[`— `,Q?`${Z[0]}–${Z[Z.length-1]} · ${X.length} months`:`${Z[0]} · ${X.length} months`]})]}),(0,v.jsxs)(`p`,{className:`text-xs text-slate-400 mt-0.5`,children:[`Cell colour = performance vs target · Click any row for deep-dive`,L&&(0,v.jsx)(`span`,{className:`ml-2 text-blue-600 font-medium`,children:`· Showing Δ vs prior period`})]})]}),(0,v.jsxs)(`div`,{className:`flex items-center gap-2 flex-shrink-0`,children:[(0,v.jsxs)(`button`,{onClick:()=>R(e=>!e),className:`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border transition-all ${L?`bg-blue-50 text-blue-700 border-blue-300`:`bg-slate-50 text-slate-500 border-slate-200 hover:border-slate-300`}`,children:[(0,v.jsx)(g,{size:12}),`Δ Prior Month`]}),(0,v.jsxs)(`button`,{onClick:()=>j(e),className:`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold bg-slate-700 text-white border border-slate-700 hover:bg-slate-800 transition-all`,children:[(0,v.jsx)(i,{size:12}),`Download Data`]}),(0,v.jsxs)(`button`,{onClick:()=>A(e,n?M:[],n?F:[]),className:`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold bg-emerald-600 text-white border border-emerald-600 hover:bg-emerald-700 transition-all`,children:[(0,v.jsx)(h,{size:12}),`Presentation`]}),(0,v.jsxs)(`button`,{onClick:()=>k(e),className:`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold bg-[#0055A4] text-white border border-[#0055A4] hover:bg-[#0044a0] transition-all`,children:[(0,v.jsx)(h,{size:12}),`Board Pack`]})]})]}),(0,v.jsxs)(`table`,{className:`w-full text-xs border-collapse mt-4`,children:[(0,v.jsxs)(`thead`,{children:[Q&&(0,v.jsxs)(`tr`,{children:[(0,v.jsx)(`th`,{colSpan:2}),te.map(({yr:e,count:t},n)=>(0,v.jsx)(`th`,{colSpan:t,className:`text-center text-[10px] font-bold text-blue-700 bg-blue-50 py-1 border-b-2 border-blue-200 ${n>0?`border-l-2 border-blue-300`:``}`,children:e},e)),(0,v.jsx)(`th`,{colSpan:3})]}),(0,v.jsxs)(`tr`,{className:`bg-slate-50`,children:[(0,v.jsx)(`th`,{className:`text-left text-slate-500 font-semibold py-2.5 pr-4 pl-2 whitespace-nowrap rounded-tl-lg`,children:`KPI`}),(0,v.jsx)(`th`,{className:`text-right text-slate-500 font-semibold py-2.5 px-3`,children:`Target`}),X.map((e,t)=>(0,v.jsx)(`th`,{className:`text-center text-slate-500 font-semibold py-2.5 px-1 min-w-[52px] ${e.endsWith(`-01`)&&t>0?`border-l-2 border-blue-200`:``}`,children:$(e)},e)),(0,v.jsx)(`th`,{className:`text-center text-slate-500 font-semibold py-2.5 px-2 bg-slate-100 whitespace-nowrap`,children:Q?`Avg`:`FY Avg`}),(0,v.jsx)(`th`,{className:`text-center text-slate-500 font-semibold py-2.5 px-2`,children:`Status`}),(0,v.jsx)(`th`,{className:`py-2.5 px-2`})]})]}),(0,v.jsx)(`tbody`,{children:Y.map((e,n)=>{let r={};e.monthly.forEach(e=>{r[e.period]=e.value});let i=E(e,X);return(0,v.jsxs)(`tr`,{onClick:()=>t?.(e.key),className:`border-t border-slate-100 cursor-pointer hover:bg-blue-50/40 transition-colors group ${n%2==0?``:`bg-slate-50/40`}`,children:[(0,v.jsx)(`td`,{className:`py-2 pr-2 pl-2 text-slate-700 font-medium whitespace-nowrap`,children:(0,v.jsxs)(`span`,{className:`flex items-center gap-1.5`,children:[e.name,i>=2&&(0,v.jsxs)(`span`,{className:`flex items-center gap-0.5 text-[9px] font-bold text-red-500 bg-red-50 border border-red-200 px-1.5 py-0.5 rounded-full flex-shrink-0`,children:[i>=3&&(0,v.jsx)(`span`,{className:`w-1.5 h-1.5 rounded-full bg-red-500 animate-pulse flex-shrink-0`}),i,`mo`]}),(0,v.jsx)(s,{size:11,className:`text-slate-300 group-hover:text-[#0055A4] transition-colors flex-shrink-0`})]})}),(0,v.jsx)(`td`,{className:`py-2 px-3 text-right text-slate-500 font-mono`,children:S(e.target,e.unit)}),X.map((t,n)=>{let i=r[t],a=n>0?r[X[n-1]]:null,o=T(i,e.target,e.direction),s=L&&i!=null&&a!=null?i-a:null,c=s==null?null:e.direction===`higher`?s>0:s<0;return(0,v.jsx)(`td`,{className:`py-1.5 px-0.5 ${t.endsWith(`-01`)&&n>0?`border-l-2 border-blue-100`:``}`,children:(0,v.jsxs)(`div`,{className:`relative rounded px-1 py-1 text-center font-mono text-[11px] font-medium ${w(o)} group/cell`,onContextMenu:n=>{n.preventDefault(),n.stopPropagation();let r=z[`${e.key}::${t}`];H({kpiKey:e.key,kpiName:e.name,period:t,periodLabel:$(t)}),W(r?.note||``)},children:[z[`${e.key}::${t}`]&&(0,v.jsx)(`div`,{className:`absolute top-0.5 right-0.5 w-1.5 h-1.5 rounded-full bg-blue-400`,title:z[`${e.key}::${t}`].note}),(0,v.jsx)(`span`,{className:`absolute bottom-0 right-0.5 text-[8px] text-slate-400 opacity-0 group-hover/cell:opacity-60 transition-opacity pointer-events-none`,children:`✎`}),i==null?(0,v.jsx)(`span`,{className:`text-slate-300`,children:`—`}):S(i,e.unit),s!=null&&(0,v.jsxs)(`div`,{className:`text-[8px] font-bold leading-tight mt-0.5 ${c?`text-emerald-700`:`text-red-500`}`,children:[s>0?`▲`:`▼`,Math.abs(s).toFixed(1)]})]})},t)}),(0,v.jsx)(`td`,{className:`py-1.5 px-1`,children:(0,v.jsx)(`div`,{className:`rounded px-1 py-1.5 text-center font-mono text-[11px] font-bold ${w(e.fy_status)} bg-opacity-80`,children:S(e.avg,e.unit)})}),(0,v.jsx)(`td`,{className:`py-1.5 px-2 text-center`,children:(0,v.jsx)(`span`,{className:`inline-block px-2 py-0.5 rounded-full text-[10px] font-bold ${ee(e.fy_status)}`,children:e.fy_status?.toUpperCase()})}),(0,v.jsx)(`td`,{className:`py-1.5 px-2 text-center`,children:(0,v.jsx)(s,{size:13,className:`text-slate-200 group-hover:text-[#0055A4] transition-colors`})})]},e.key)})})]})]}),V&&(0,v.jsx)(`div`,{className:`fixed inset-0 z-50 flex items-center justify-center bg-black/20`,onClick:()=>H(null),children:(0,v.jsxs)(`div`,{className:`bg-white rounded-xl shadow-2xl border border-slate-200 p-4 w-80`,onClick:e=>e.stopPropagation(),children:[(0,v.jsxs)(`div`,{className:`flex items-center justify-between mb-2`,children:[(0,v.jsxs)(`span`,{className:`text-[12px] font-bold text-slate-700`,children:[`Note: `,V.kpiName,` · `,V.periodLabel]}),(0,v.jsx)(`button`,{onClick:()=>H(null),className:`text-slate-400 hover:text-slate-600`,children:(0,v.jsx)(o,{size:14})})]}),(0,v.jsx)(`textarea`,{className:`w-full border border-slate-200 rounded-lg p-2 text-[12px] text-slate-700 resize-none focus:outline-none focus:ring-2 focus:ring-blue-300`,rows:3,value:U,onChange:e=>W(e.target.value),placeholder:`e.g. Lost Acme Corp deal, Hired 3 SDRs...`,autoFocus:!0}),(0,v.jsxs)(`div`,{className:`flex justify-between mt-2`,children:[z[`${V.kpiKey}::${V.period}`]&&(0,v.jsx)(`button`,{onClick:()=>{let e=z[`${V.kpiKey}::${V.period}`];a.delete(`/api/annotations/${e.id}`).then(()=>{B(e=>{let t={...e};return delete t[`${V.kpiKey}::${V.period}`],t}),H(null)})},className:`text-[11px] text-red-500 hover:text-red-700 font-medium`,children:`Delete`}),(0,v.jsxs)(`div`,{className:`flex gap-2 ml-auto`,children:[(0,v.jsx)(`button`,{onClick:()=>H(null),className:`text-[11px] text-slate-500 hover:text-slate-700 font-medium px-2 py-1`,children:`Cancel`}),(0,v.jsx)(`button`,{onClick:()=>{U.trim()&&a.put(`/api/annotations`,{kpi_key:V.kpiKey,period:V.period,note:U.trim()}).then(e=>{let t=e.data.annotation;B(e=>({...e,[`${t.kpi_key}::${t.period}`]:t})),H(null)})},className:`text-[11px] bg-[#0055A4] text-white font-medium px-3 py-1 rounded-lg hover:bg-[#003d80]`,children:`Save`})]})]})]})})]})}export{P as default};