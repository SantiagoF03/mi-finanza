import { useEffect, useState } from 'react'

const API = 'http://localhost:8000/api'

function formatRemaining(totalSeconds) {
  const seconds = Math.max(0, Number(totalSeconds || 0))
  const mins = Math.floor(seconds / 60)
  const secs = seconds % 60
  return `${mins} min ${secs} s`
}

export default function App() {
  const [summary, setSummary] = useState(null)
  const [analysis, setAnalysis] = useState(null)
  const [news, setNews] = useState([])
  const [current, setCurrent] = useState(null)
  const [history, setHistory] = useState([])
  const [events, setEvents] = useState([])
  const [alerts, setAlerts] = useState([])
  const [error, setError] = useState('')
  const [currentInfo, setCurrentInfo] = useState('')
  const [cooldownMessage, setCooldownMessage] = useState('')
  const [cooldownRemaining, setCooldownRemaining] = useState(0)

  useEffect(() => {
    if (cooldownRemaining <= 0) return undefined
    const timer = setInterval(() => {
      setCooldownRemaining((prev) => {
        const next = prev - 1
        if (next <= 0) {
          setCooldownMessage('')
          return 0
        }
        setCooldownMessage(`Todavía no podés generar una nueva recomendación. Esperá ${formatRemaining(next)}.`)
        return next
      })
    }, 1000)
    return () => clearInterval(timer)
  }, [cooldownRemaining])

  const load = async () => {
    setError('')
    setCurrentInfo('')
    try {
      const [sRes, aRes, nRes, cRes, hRes, evRes, alRes] = await Promise.all([
        fetch(`${API}/portfolio/summary`),
        fetch(`${API}/portfolio/analysis`),
        fetch(`${API}/news/recent`),
        fetch(`${API}/recommendations/current`),
        fetch(`${API}/history`),
        fetch(`${API}/events/recent`),
        fetch(`${API}/alerts/current`),
      ])

      if (!sRes.ok || !aRes.ok || !nRes.ok || !hRes.ok) throw new Error('backend_unavailable')

      setSummary(await sRes.json())
      setAnalysis(await aRes.json())
      setNews(await nRes.json())
      setHistory(await hRes.json())
      if (evRes.ok) setEvents(await evRes.json())
      if (alRes.ok) setAlerts(await alRes.json())

      if (cRes.status === 404) {
        setCurrent(null)
        setCurrentInfo('No hay recomendación abierta actualmente.')
      } else if (cRes.ok) {
        setCurrent(await cRes.json())
      } else {
        throw new Error('backend_unavailable')
      }
    } catch {
      setError('Backend no disponible. Levantá FastAPI para usar la app (sin mock frontend local).')
      setSummary(null)
      setAnalysis(null)
      setNews([])
      setCurrent(null)
      setHistory([])
      setEvents([])
      setAlerts([])
      setCurrentInfo('')
    }
  }

  useEffect(() => {
    load()
  }, [])

  const triggerAnalysis = async () => {
    setError('')
    try {
      const resp = await fetch(`${API}/analysis/run`, { method: 'POST' })
      if (!resp.ok) throw new Error('trigger_failed')
      const payload = await resp.json()

      if (payload?.status === 'cooldown' && payload?.skipped) {
        const remainingSeconds = Number(payload.cooldown_remaining_seconds || 0)
        setCooldownRemaining(remainingSeconds)
        setCooldownMessage(
          `Todavía no podés generar una nueva recomendación. Esperá ${formatRemaining(remainingSeconds)}.`,
        )
        await load()
        return
      }

      setCooldownRemaining(0)
      setCooldownMessage('')
      await load()
    } catch {
      setError('No se pudo ejecutar el análisis manual en este momento.')
    }
  }

  const triggerIngestion = async () => {
    setError('')
    try {
      const resp = await fetch(`${API}/events/run-ingestion`, { method: 'POST' })
      if (!resp.ok) throw new Error('ingestion_failed')
      await load()
    } catch {
      setError('No se pudo ejecutar la ingesta de noticias.')
    }
  }

  const acknowledgeAlert = async (alertId) => {
    await fetch(`${API}/alerts/${alertId}/acknowledge`, { method: 'POST' })
    load()
  }

  const decide = async (decision) => {
    if (!current?.id) return
    await fetch(`${API}/recommendations/${current.id}/decision`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ decision, note: '' }),
    })
    load()
  }

  return (
    <main className="container">
      <h1>Mi Finanza MVP</h1>
      <button onClick={triggerAnalysis} disabled={cooldownRemaining > 0}>
        Disparar análisis manual
      </button>
      {' '}
      <button onClick={triggerIngestion}>
        Ingestar noticias
      </button>
      {cooldownMessage && <p style={{ color: '#7a2e0a', fontWeight: 600 }}>{cooldownMessage}</p>}
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
          {analysis.profile_label && <p><strong>Perfil aplicado:</strong> {analysis.profile_label}</p>}
          <p>Concentración: {(analysis.concentration_score * 100).toFixed(1)}%</p>
          <p>Score de riesgo: {(analysis.risk_score * 100).toFixed(1)}%</p>
          {analysis.equity_weight != null && <p>Equity total: {(analysis.equity_weight * 100).toFixed(1)}%</p>}
          <h3>Exposición por moneda (económica)</h3>
          <ul>
            {Object.entries(analysis.weights_by_currency || {}).map(([ccy, w]) => (
              <li key={ccy}>{ccy}: {(w * 100).toFixed(1)}%</li>
            ))}
          </ul>
          {analysis.weights_by_bucket && (
            <>
              <h3>Distribución por bucket</h3>
              <ul>
                {Object.entries(analysis.weights_by_bucket).map(([bucket, w]) => (
                  <li key={bucket}>{bucket}: {(w * 100).toFixed(1)}%</li>
                ))}
              </ul>
            </>
          )}
          <p>Alertas: {analysis.alerts?.join(' | ') || 'Sin alertas'}</p>
        </section>
      )}

      {currentInfo && <p>{currentInfo}</p>}

      {current && (
        <section>
          <h2>Recomendación actual</h2>
          {current.unchanged && (
            <p style={{ background: '#e8f5e9', padding: '8px 12px', borderRadius: 6, fontWeight: 600 }}>
              No hubo cambios materiales desde el último análisis. Se mantiene la recomendación anterior.
            </p>
          )}
          <p>Estado: <strong>{current.status}</strong></p>
          {current.status === 'blocked' && <p style={{ color: '#b42318' }}>Bloqueada por reglas: {current.blocked_reason}</p>}
          <p>Acción: <strong>{current.action}</strong></p>
          <p>Porcentaje sugerido: {(current.suggested_pct * 100).toFixed(2)}%</p>
          <p>Confianza: {(current.confidence * 100).toFixed(0)}%</p>
          {current.profile_label && <p>Perfil: <strong>{current.profile_label}</strong></p>}
          <p>Motivo: {current.recommendation_explanation_llm || current.rationale}</p>
          {(current.rationale_reasons || []).length > 0 && (
            <div style={{ background: '#f8f9fa', padding: '8px 12px', borderRadius: 6, margin: '8px 0' }}>
              <strong>Detalle del análisis:</strong>
              <ul style={{ margin: '4px 0' }}>
                {current.rationale_reasons.map((r, i) => (
                  <li key={i} style={{ fontSize: '0.9em' }}>
                    <span style={{ background: '#e3f2fd', padding: '1px 5px', borderRadius: 3, fontSize: '0.8em', marginRight: 4 }}>{r.type}</span>
                    {r.detail}
                  </li>
                ))}
              </ul>
            </div>
          )}
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


      {current && (current.external_opportunities || []).length > 0 && (
        <section>
          <h2>Oportunidades detectadas (activos para vigilar)</h2>
          <p>Estas oportunidades son externas a tu cartera actual. No generan órdenes ni approve/reject.</p>
          <ul>
            {current.external_opportunities.map((op, idx) => (
              <li key={`${op.symbol}-${idx}`} style={{ marginBottom: 10, paddingBottom: 8, borderBottom: '1px solid #eee' }}>
                <strong>{op.symbol}</strong>
                {' '}
                {(op.source_types || []).map((s) => (
                  <span key={s} style={{ background: '#e8eaf6', padding: '1px 5px', borderRadius: 3, marginRight: 4, fontSize: '0.8em' }}>{s}</span>
                ))}
                {op.tracking_status && op.tracking_status !== 'untracked' && (
                  <span style={{ background: '#e3f2fd', padding: '2px 6px', borderRadius: 4, marginLeft: 2, fontSize: '0.85em' }}>
                    {op.tracking_status === 'watchlist' ? 'en watchlist' : op.tracking_status === 'in_universe' ? 'en universo' : op.tracking_status}
                  </span>
                )}
                {op.tracking_status === 'untracked' && (
                  <span style={{ background: '#fff3e0', padding: '2px 6px', borderRadius: 4, marginLeft: 2, fontSize: '0.85em' }}>
                    no rastreado
                  </span>
                )}
                {op.asset_type_status === 'unsupported' && (
                  <span style={{ background: '#ffab91', padding: '2px 6px', borderRadius: 4, marginLeft: 2, fontSize: '0.85em' }}>tipo no soportado: {op.asset_type}</span>
                )}
                {op.asset_type_status === 'unknown' && (
                  <span style={{ background: '#ffe0b2', padding: '2px 6px', borderRadius: 4, marginLeft: 2, fontSize: '0.85em' }}>tipo desconocido</span>
                )}
                {op.asset_type_status === 'known_valid' && (
                  <span style={{ background: '#c8e6c9', padding: '2px 6px', borderRadius: 4, marginLeft: 2, fontSize: '0.85em' }}>tipo: {op.asset_type}</span>
                )}
                {op.investable && (
                  <span style={{ background: '#a5d6a7', padding: '2px 6px', borderRadius: 4, marginLeft: 2, fontSize: '0.85em', fontWeight: 600 }}>invertible</span>
                )}
                {!op.investable && op.actionable_external && (
                  <span style={{ background: '#c8e6c9', padding: '2px 6px', borderRadius: 4, marginLeft: 2, fontSize: '0.85em' }}>seguimiento</span>
                )}
                {!op.actionable_external && (
                  <span style={{ background: '#ffcdd2', padding: '2px 6px', borderRadius: 4, marginLeft: 2, fontSize: '0.85em' }}>solo observado</span>
                )}
                {op.in_main_allowed && (
                  <span style={{ background: '#bbdefb', padding: '2px 6px', borderRadius: 4, marginLeft: 2, fontSize: '0.85em' }}>en whitelist</span>
                )}
                <br />
                <span style={{ fontSize: '0.9em', color: '#555' }}>
                  prioridad: {op.priority_score ?? '-'} | impacto: {op.impact} | tipo: {op.event_type} | confianza: {(Number(op.confidence || 0) * 100).toFixed(0)}%
                </span>
                <br />
                <span style={{ fontSize: '0.9em', color: '#666' }}>{op.actionable_reason}</span>
                <br />
                <span style={{ fontSize: '0.9em' }}>motivo: {op.reason}</span>
              </li>
            ))}
          </ul>
        </section>
      )}

      {current?.allowed_assets && (
        <section>
          <h2>Activos permitidos</h2>
          <p><strong>Holdings reales:</strong> {(current.allowed_assets.holdings || []).join(', ') || 'Ninguno'}</p>
          <p><strong>Whitelist manual:</strong> {(current.allowed_assets.whitelist || []).join(', ') || 'Ninguna'}</p>
          {(current.allowed_assets.watchlist || []).length > 0 && (
            <p><strong>Watchlist externa:</strong> {current.allowed_assets.watchlist.join(', ')}</p>
          )}
          {(current.allowed_assets.universe || []).length > 0 && (
            <p><strong>Universo de mercado:</strong> {current.allowed_assets.universe.join(', ')}</p>
          )}
          <p><strong>Total permitidos para acciones:</strong> {(current.allowed_assets.main_allowed || []).join(', ')}</p>
        </section>
      )}

      <section>
        <h2>Noticias relevantes</h2>
        {current?.news_summary && (
          <div style={{ background: '#f3f4f6', padding: '8px 12px', borderRadius: 6, marginBottom: 12, whiteSpace: 'pre-wrap' }}>
            <strong>Resumen de noticias (LLM):</strong>
            <p>{current.news_summary}</p>
          </div>
        )}
        <ul>
          {news.map((item) => (
            <li key={item.id}>
              <strong>{item.title}</strong> — {item.event_type} — impacto {item.impact} — activos {item.related_assets.join(', ')}
            </li>
          ))}
        </ul>
      </section>

      {alerts.length > 0 && (
        <section>
          <h2>Alertas activas</h2>
          <ul>
            {alerts.map((a) => (
              <li key={a.id} style={{ marginBottom: 8, paddingBottom: 6, borderBottom: '1px solid #eee' }}>
                <span style={{
                  background: a.severity === 'critical' ? '#d32f2f' : a.severity === 'high' ? '#f57c00' : '#fbc02d',
                  color: a.severity === 'critical' ? '#fff' : '#333',
                  padding: '2px 6px', borderRadius: 4, fontSize: '0.8em', marginRight: 6
                }}>{a.severity}</span>
                <strong>{a.message}</strong>
                {a.affected_symbols?.length > 0 && <span> — {a.affected_symbols.join(', ')}</span>}
                {a.triggered_recalc && <span style={{ background: '#e8f5e9', padding: '2px 6px', borderRadius: 4, marginLeft: 6, fontSize: '0.8em' }}>recalculado</span>}
                <span style={{ fontSize: '0.85em', color: '#888', marginLeft: 8 }}>{a.trigger_type}</span>
                {' '}
                <button onClick={() => acknowledgeAlert(a.id)} style={{ fontSize: '0.8em', padding: '1px 6px' }}>OK</button>
              </li>
            ))}
          </ul>
        </section>
      )}

      {events.length > 0 && (
        <section>
          <h2>Eventos de mercado recientes</h2>
          <ul>
            {events.map((e) => (
              <li key={e.id} style={{ marginBottom: 4 }}>
                <span style={{
                  background: e.severity === 'critical' ? '#ffcdd2' : e.severity === 'high' ? '#ffe0b2' : e.severity === 'medium' ? '#fff9c4' : '#e8f5e9',
                  padding: '1px 5px', borderRadius: 3, fontSize: '0.8em', marginRight: 4
                }}>{e.severity}</span>
                {e.message}
                {e.affected_symbols?.length > 0 && <span style={{ fontSize: '0.85em', color: '#666' }}> [{e.affected_symbols.join(', ')}]</span>}
                {e.triggered_recalc && <span style={{ fontSize: '0.8em', color: '#388e3c', marginLeft: 4 }}>(recalc)</span>}
                <span style={{ fontSize: '0.8em', color: '#999', marginLeft: 6 }}>{e.created_at}</span>
              </li>
            ))}
          </ul>
        </section>
      )}

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
