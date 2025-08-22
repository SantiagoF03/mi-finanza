import { useMemo, useState, useEffect, useRef } from 'react'
import MovimientoCard from './components/MovimientoCard.jsx'
import AddMovimientoForm from './components/AddMovimientoForm.jsx'
import InvestmentsPage from './components/InvestmentsPage.jsx'
import initialMovs from './data/movimientos.json'

const formatMoney = (n) => Number(n).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
const toNumber = (x) => Number(String(x).replace(',', '.'))
const toDateKey = (s) => { if(!s) return ''; if(s.includes('-')) return s; const [d,m,y]=s.split('/'); return `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}` }
const valorConSigno = (m) => (m.tipo === 'Gasto' ? -Math.abs(toNumber(m.monto)) : Math.abs(toNumber(m.monto)))
const buildDailySeries = (movs) => {
  const porDia = {}; for (const m of movs) { const k=toDateKey(m.fecha); porDia[k]=(porDia[k]||0)+valorConSigno(m) }
  const orden = Object.entries(porDia).sort(([a],[b])=>a.localeCompare(b)); let acc=0
  return orden.map(([fecha,diario])=>{ acc+=diario; return {fecha,diario,acumulado:acc} })
}
const Sparkline = ({ values, width=300, height=56, strokeWidth=2 }) => {
  if(!values?.length) return null
  const min=Math.min(...values), max=Math.max(...values); const range=max-min||1
  const stepX = values.length>1? width/(values.length-1) : width
  const pts = values.map((v,i)=>`${i*stepX},${height-((v-min)/range)*height}`).join(' ')
  return <svg width={width} height={height} className="w-full"><polyline fill="none" stroke="currentColor" strokeWidth={strokeWidth} points={pts} /></svg>
}
const STORAGE_KEY='mi-finanzas/movs-v1'

export default function App(){
  const [page,setPage]=useState('dashboard')

  const [movs,setMovs]=useState(initialMovs)
  const didHydrate = useRef(false)
  useEffect(()=>{ try{const raw=localStorage.getItem(STORAGE_KEY); if(raw){const p=JSON.parse(raw); if(Array.isArray(p)) setMovs(p)} }catch{}; didHydrate.current=true },[])
  useEffect(()=>{ if(!didHydrate.current) return; try{ localStorage.setItem(STORAGE_KEY, JSON.stringify(movs)) }catch{} },[movs])

  const [desde,setDesde]=useState(''); const [hasta,setHasta]=useState(''); const [medio,setMedio]=useState('Todos')
  const mediosDisponibles = useMemo(()=>['Todos', ...Array.from(new Set(movs.map(m=>m.medio).filter(Boolean)))],[movs])
  const filteredMovimientos = useMemo(()=> movs.filter(m=>{ const k=toDateKey(m.fecha); if(desde&&k<desde) return false; if(hasta&&k>hasta) return false; if(medio!=='Todos'&&m.medio!==medio) return false; return true }), [movs,desde,hasta,medio])

  const gastos=useMemo(()=>filteredMovimientos.filter(m=>m.tipo==='Gasto'),[filteredMovimientos])
  const ingresos=useMemo(()=>filteredMovimientos.filter(m=>m.tipo==='Transferencia in'||m.tipo==='Ingreso'),[filteredMovimientos])
  const totalGastos=useMemo(()=>gastos.reduce((a,m)=>a+Math.abs(toNumber(m.monto)),0),[gastos])
  const totalIngresos=useMemo(()=>ingresos.reduce((a,m)=>a+toNumber(m.monto),0),[ingresos])
  const balanceNeto=totalIngresos-totalGastos
  const topGastos=useMemo(()=>gastos.slice().sort((a,b)=>Math.abs(toNumber(b.monto))-Math.abs(toNumber(a.monto))).slice(0,3),[gastos])
  const serieDiaria=useMemo(()=>buildDailySeries(filteredMovimientos),[filteredMovimientos])
  const valoresAcum=useMemo(()=>serieDiaria.map(p=>p.acumulado),[serieDiaria])

  const handleAdd=(m)=> setMovs(prev=>[{id:Date.now(),...m}, ...prev])

  const exportCSV=()=>{ const headers=['fecha','hora','descripcion','monto','tipo','medio']; const lines=[headers.join(',')]; for(const m of movs){ const row=headers.map(h=>{const v=(m[h]??'').toString().replace(/\r|\n/g,' ').replace(/"/g,'""'); return /[",;]/.test(v)?`"${v}"`:v}).join(','); lines.push(row)} const blob=new Blob([lines.join('\n')],{type:'text/csv;charset=utf-8;'}); const url=URL.createObjectURL(blob); const a=document.createElement('a'); a.href=url; a.download='movimientos.csv'; a.click(); URL.revokeObjectURL(url) }

  const importCSV=(file)=>{ const reader=new FileReader(); reader.onload=()=>{ try{ const text=reader.result; const [headerLine,...rows]=text.trim().split(/\r?\n/); const delim=headerLine.includes(';')?';':','; const headers=headerLine.split(delim).map(h=>h.trim().toLowerCase()); const idx=(n)=>headers.indexOf(n); const out=[]; for(const line of rows){ if(!line.trim()) continue; const cols=line.split(delim); const obj={fecha:cols[idx('fecha')]||'',hora:cols[idx('hora')]||'',descripcion:cols[idx('descripcion')]||'',monto:(cols[idx('monto')]||'').replace('.','').replace(',','.'),tipo:cols[idx('tipo')]||'Gasto',medio:cols[idx('medio')]||'Otro'}; if(!obj.fecha||!obj.descripcion||!obj.monto) continue; out.push(obj)} if(out.length) setMovs(prev=>[...out,...prev]) }catch(e){ alert('No se pudo importar el CSV. Verificá el formato.') } }; reader.readAsText(file,'utf-8') }

  const importFromGmail=()=>{ alert('Demo: aquí llamaríamos al backend que usa el conector Gmail (Visa/MP/Ualá/Bancos).') }

  return (
    <div className="min-h-screen bg-gray-900 text-gray-100">
      <header className="border-b border-gray-800">
        <div className="max-w-4xl mx-auto px-6 py-4 flex items-center justify-between">
          <nav className="text-sm">
            <span className={`cursor-pointer ${page==='dashboard'?'text-white':'text-gray-400 hover:text-gray-200'}`} onClick={()=>setPage('dashboard')}>Inicio</span>
            <span className="mx-2 text-gray-600">/</span>
            <span className={`cursor-pointer ${page==='inversiones'?'text-white':'text-gray-400 hover:text-gray-200'}`} onClick={()=>setPage('inversiones')}>Inversiones</span>
          </nav>
          <div className="text-xs text-gray-500">Mi Finanzas</div>
        </div>
      </header>

      <main className="max-w-4xl mx-auto px-6 py-6">
        {page==='inversiones' ? (
          <InvestmentsPage />
        ) : (
          <>
            <section className="mb-6 flex flex-wrap gap-3 items-center">
              <button onClick={exportCSV} className="px-4 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-700">Exportar CSV</button>
              <label className="px-4 py-2 rounded-lg bg-indigo-600 hover:bg-indigo-700 cursor-pointer">
                Importar CSV
                <input type="file" accept=".csv" onChange={(e)=>{const f=e.target.files?.[0]; if(f) importCSV(f); e.target.value=''}} className="hidden" />
              </label>
              <button onClick={importFromGmail} className="px-4 py-2 rounded-lg bg-sky-600 hover:bg-sky-700">Importar desde Gmail</button>
            </section>

            <section className="mb-6"><AddMovimientoForm onAdd={handleAdd} /></section>

            <section className="mb-6 p-4 bg-gray-800 border border-gray-700 rounded-xl shadow grid md:grid-cols-4 gap-3 text-left">
              <div>
                <label className="block text-sm text-gray-400 mb-1">Desde</label>
                <input type="date" value={desde} onChange={e=>setDesde(e.target.value)} className="w-full bg-gray-900 border border-gray-700 rounded-lg px-3 py-2" />
              </div>
              <div>
                <label className="block text-sm text-gray-400 mb-1">Hasta</label>
                <input type="date" value={hasta} onChange={e=>setHasta(e.target.value)} className="w-full bg-gray-900 border border-gray-700 rounded-lg px-3 py-2" />
              </div>
              <div className="md:col-span-2">
                <label className="block text-sm text-gray-400 mb-1">Medio de pago</label>
                <select value={medio} onChange={e=>setMedio(e.target.value)} className="w-full bg-gray-900 border border-gray-700 rounded-lg px-3 py-2">
                  {mediosDisponibles.map(op=><option key={op} value={op}>{op}</option>)}
                </select>
              </div>
            </section>

            <h1 className="text-4xl font-extrabold mb-4 text-left">Resumen financiero</h1>

            <section className="grid md:grid-cols-3 gap-4 mb-6">
              <div className="p-4 bg-gray-800 border border-gray-700 rounded-xl shadow text-left">
                <p className="text-sm text-gray-400">Total gastado</p>
                <p className="text-xl font-bold text-red-400">${formatMoney(totalGastos)}</p>
              </div>
              <div className="p-4 bg-gray-800 border border-gray-700 rounded-xl shadow text-left">
                <p className="text-sm text-gray-400">Total ingresado</p>
                <p className="text-xl font-bold text-green-400">${formatMoney(totalIngresos)}</p>
              </div>
              <div className="p-4 bg-gray-800 border border-gray-700 rounded-xl shadow text-left">
                <p className="text-sm text-gray-400">Balance neto</p>
                <p className={`text-xl font-bold ${balanceNeto>=0?'text-green-400':'text-red-400'}`}>${formatMoney(balanceNeto)}</p>
              </div>
            </section>

            <section className="mb-6 p-4 bg-gray-800 border border-gray-700 rounded-xl shadow text-left">
              <h2 className="text-lg font-semibold mb-2">Evolución diaria (saldo acumulado)</h2>
              <div className="text-sm text-gray-400 mb-2">
                {serieDiaria.length>0 && <>Desde {serieDiaria[0].fecha} hasta {serieDiaria[serieDiaria.length-1].fecha}</>}
              </div>
              <div className="text-blue-400 mb-3"><Sparkline values={valoresAcum} /></div>
              <ul className="grid md:grid-cols-2 gap-2 text-sm">
                {serieDiaria.map(p=>(
                  <li key={p.fecha} className="flex justify-between">
                    <span>{p.fecha}</span>
                    <span className="font-medium">${formatMoney(p.acumulado)}</span>
                  </li>
                ))}
              </ul>
            </section>

            <section className="mb-6">
              <h2 className="text-lg font-semibold mb-2 text-left">Top 3 gastos</h2>
              <ul className="space-y-2">
                {topGastos.map((g,i)=>(
                  <li key={g.id ?? `top-${i}`} className="flex items-center justify-between bg-gray-800 border border-gray-700 rounded-lg shadow p-3">
                    <span className="truncate">{g.descripcion ?? g.comercio}</span>
                    <span className="font-bold text-red-400">${formatMoney(Math.abs(toNumber(g.monto)))}</span>
                  </li>
                ))}
              </ul>
            </section>

            <section>
              <h2 className="text-lg font-semibold mb-2 text-left">Movimientos</h2>
              {filteredMovimientos.map((m,i)=>{
                const montoNum = m.tipo==='Gasto'? Math.abs(toNumber(m.monto)) : toNumber(m.monto)
                const comercio = m.descripcion ?? m.comercio
                const fecha = `${m.fecha}${m.hora?` ${m.hora}`:''}`
                return <MovimientoCard key={m.id ?? i} fecha={fecha} comercio={comercio} monto={formatMoney(montoNum)} tipo={m.tipo} />
              })}
            </section>
          </>
        )}
      </main>
    </div>
  )
}
