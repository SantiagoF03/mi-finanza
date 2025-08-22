import React, { useState } from 'react'
const mediosPredef = ['Tarjeta','Banco','Efectivo','MP','SUBE','Otro']
const tipos = ['Gasto','Ingreso','Transferencia in']
export default function AddMovimientoForm({ onAdd }) {
  const [fecha, setFecha] = useState('')
  const [hora, setHora] = useState('')
  const [descripcion, setDescripcion] = useState('')
  const [monto, setMonto] = useState('')
  const [tipo, setTipo] = useState('Gasto')
  const [medio, setMedio] = useState('Tarjeta')
  const [error, setError] = useState('')
  const toNumber = (x) => Number(String(x).replace(',', '.'))
  const handleSubmit = (e) => {
    e.preventDefault()
    setError('')
    if (!fecha) return setError('La fecha es obligatoria')
    if (!descripcion.trim()) return setError('La descripción es obligatoria')
    const num = toNumber(monto)
    if (!Number.isFinite(num) || num <= 0) return setError('El monto debe ser un número positivo')
    onAdd({ fecha, hora: hora || null, descripcion: descripcion.trim(), monto: num.toString(), tipo, medio })
    setDescripcion(''); setMonto(''); setHora('')
  }
  return (
    <form onSubmit={handleSubmit} className="bg-gray-800 border border-gray-700 rounded-xl shadow p-4 grid md:grid-cols-6 gap-3 text-left">
      <div className="md:col-span-2">
        <label className="block text-sm text-gray-400 mb-1">Fecha</label>
        <input type="date" value={fecha} onChange={e=>setFecha(e.target.value)} className="w-full bg-gray-900 border border-gray-700 rounded-lg px-3 py-2" />
      </div>
      <div>
        <label className="block text-sm text-gray-400 mb-1">Hora</label>
        <input type="time" value={hora} onChange={e=>setHora(e.target.value)} className="w-full bg-gray-900 border border-gray-700 rounded-lg px-3 py-2" />
      </div>
      <div className="md:col-span-2">
        <label className="block text-sm text-gray-400 mb-1">Descripción</label>
        <input type="text" value={descripcion} onChange={e=>setDescripcion(e.target.value)} placeholder="Ej: Supermercado" className="w-full bg-gray-900 border border-gray-700 rounded-lg px-3 py-2" />
      </div>
      <div>
        <label className="block text-sm text-gray-400 mb-1">Monto</label>
        <input type="text" inputMode="decimal" value={monto} onChange={e=>setMonto(e.target.value)} placeholder="1200,50" className="w-full bg-gray-900 border border-gray-700 rounded-lg px-3 py-2" />
      </div>
      <div>
        <label className="block text-sm text-gray-400 mb-1">Tipo</label>
        <select value={tipo} onChange={e=>setTipo(e.target.value)} className="w-full bg-gray-900 border border-gray-700 rounded-lg px-3 py-2">
          {tipos.map(t => <option key={t} value={t}>{t}</option>)}
        </select>
      </div>
      <div>
        <label className="block text-sm text-gray-400 mb-1">Medio</label>
        <select value={medio} onChange={e=>setMedio(e.target.value)} className="w-full bg-gray-900 border border-gray-700 rounded-lg px-3 py-2">
          {mediosPredef.map(m => <option key={m} value={m}>{m}</option>)}
        </select>
      </div>
      <div className="md:col-span-6 flex items-center gap-3">
        <button type="submit" className="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-700">Agregar</button>
        {error && <span className="text-red-400 text-sm">{error}</span>}
      </div>
    </form>
  )
}
