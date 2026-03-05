import { useEffect, useState } from 'react'

const API = 'http://localhost:8000/api'

export default function App() {
  const [summary, setSummary] = useState(null)
  const [analysis, setAnalysis] = useState(null)
  const [news, setNews] = useState([])
  const [current, setCurrent] = useState(null)
  const [history, setHistory] = useState([])
  const [error, setError] = useState('')

  const load = async (trigger = false) => {
    setError('')
    try {
      if (trigger) await fetch(`${API}/analysis/run`, { method: 'POST' })
      const [sRes, aRes, nRes, cRes, hRes] = await Promise.all([
        fetch(`${API}/portfolio/summary`),
        fetch(`${API}/portfolio/analysis`),
        fetch(`${API}/news/recent`),
        fetch(`${API}/recommendations/current`),
        fetch(`${API}/history`),
      ])

      if (!sRes.ok || !aRes.ok || !nRes.ok || !hRes.ok) throw new Error('backend_unavailable')

      setSummary(await sRes.json())
      setAnalysis(await aRes.json())
      setNews(await nRes.json())
      setHistory(await hRes.json())
      setCurrent(cRes.ok ? await cRes.json() : null)
    } catch {
      setError('Backend no disponible. Levantá FastAPI para usar la app (sin mock frontend local).')
      setSummary(null)
      setAnalysis(null)
      setNews([])
      setCurrent(null)
      setHistory([])
    }
  }

  useEffect(() => {
    load(false)
  }, [])

  const decide = async (decision) => {
    if (!current?.id) return
    await fetch(`${API}/recommendations/${current.id}/decision`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ decision, note: '' }),
    })
    load(false)
  }

  return (
    <main className="container">
      <h1>Mi Finanza MVP</h1>
      <button onClick={() => load(true)}>Disparar análisis manual</button>
      {error && <p style={{ color: '#b42318' }}>{error}</p>}

      {summary && (
        <section>
          <h2>Dashboard principal</h2>
          <p>Valor total: {summary.total_value.toLocaleString()} {summary.currency}</p>
          <p>Cash: {summary.cash.toLocaleString()} {summary.currency}</p>
          <p>Última actualización: {new Date(summary.created_at).toLocaleString()}</p>
          <h3>Composición por activo</h3>
          <ul>{summary.positions.map((p) => <li key={p.symbol}>{p.symbol}: {p.market_value}</li>)}</ul>
        </section>
      )}

      {analysis && (
        <section>
          <h2>Análisis de cartera</h2>
          <p>Concentración: {analysis.concentration_score}</p>
          <p>Score de riesgo: {analysis.risk_score}</p>
          <p>Composición por moneda: {JSON.stringify(analysis.weights_by_currency)}</p>
          <p>Alertas: {analysis.alerts?.join(' | ') || 'Sin alertas'}</p>
        </section>
      )}

      {current && (
        <section>
          <h2>Recomendación actual</h2>
          <p>Estado: <strong>{current.status}</strong></p>
          {current.status === 'blocked' && <p style={{ color: '#b42318' }}>Bloqueada por reglas: {current.blocked_reason}</p>}
          <p>Acción: <strong>{current.action}</strong></p>
          <p>Porcentaje sugerido: {(current.suggested_pct * 100).toFixed(2)}%</p>
          <p>Confianza: {(current.confidence * 100).toFixed(0)}%</p>
          <p>Motivo: {current.rationale}</p>
          <p>Riesgos: {current.risks}</p>
          <p>Resumen ejecutivo: {current.executive_summary}</p>
          <p>Reglas aplicadas: {(current.rules_applied || []).join(' | ') || 'Sin bloqueos'}</p>
          <h3>Activos afectados</h3>
          <ul>{current.actions.map((a, idx) => <li key={idx}>{a.symbol}: {(a.target_change_pct * 100).toFixed(2)}% ({a.reason})</li>)}</ul>
          {(current.status === 'pending' || current.status === 'blocked') && (
            <div className="actions">
              <button onClick={() => decide('approved')}>Aprobar</button>
              <button onClick={() => decide('rejected')}>Rechazar</button>
              <button onClick={() => alert(JSON.stringify(current, null, 2))}>Ver detalle</button>
            </div>
          )}
        </section>
      )}

      <section>
        <h2>Noticias relevantes</h2>
        <ul>
          {news.map((item) => (
            <li key={item.id}>
              <strong>{item.title}</strong> — {item.event_type} — impacto {item.impact} — activos {item.related_assets.join(', ')}
            </li>
          ))}
        </ul>
      </section>

      <section>
        <h2>Historial de recomendaciones</h2>
        <ul>
          {history.map((item) => (
            <li key={item.id}>{new Date(item.date).toLocaleString()} | {item.action} | estado: {item.status} | decisión: {item.decision} | {item.summary}</li>
          ))}
        </ul>
      </section>
    </main>
  )
}
