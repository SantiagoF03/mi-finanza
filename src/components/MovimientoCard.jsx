import React from 'react'
export default function MovimientoCard({ fecha, comercio, monto, tipo }) {
  const esGasto = tipo === 'Gasto'
  return (
    <div className="border rounded-lg p-4 mb-3 bg-gray-800 border-gray-700 shadow flex justify-between items-center">
      <div>
        <p className="font-medium truncate max-w-xs" title={comercio}>{comercio}</p>
        <p className="text-sm text-gray-400">{fecha}</p>
      </div>
      <div className={`font-bold ${esGasto ? 'text-red-400' : 'text-green-400'}`}>${monto}</div>
    </div>
  )
}
