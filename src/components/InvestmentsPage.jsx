import React from 'react'
import recs from '../data/investmentRecommendations.json'
export default function InvestmentsPage() {
  const today = new Date().toISOString().slice(0,10)
  const todayBlock = recs.find(r => r.date === today) || recs[0]
  return (
    <div className="bg-gray-800 border border-gray-700 rounded-xl shadow p-5 text-left">
      <h2 className="text-xl font-semibold mb-1">Análisis diario de mercado</h2>
      <p className="text-sm text-gray-400 mb-4">Última actualización: {todayBlock?.date}</p>
      {todayBlock?.recommendations?.length ? (
        <ul className="space-y-3">
          {todayBlock.recommendations.map((r, idx) => (
            <li key={idx} className="bg-gray-900 border border-gray-700 rounded-lg p-3">
              <p className="font-medium">{r.title}</p>
              <p className="text-sm text-gray-400 mt-1">{r.detail}</p>
            </li>
          ))}
        </ul>
      ) : (
        <p className="text-gray-400">Sin recomendaciones para hoy todavía.</p>
      )}
      <div className="text-xs text-gray-500 mt-4">
        Nota: demo estática. En producción, un job diario del backend guarda el resultado de
        fuentes confiables y esta página sólo lo muestra.
      </div>
    </div>
  )
}
