import { useEffect, useState } from 'react'

const API = 'http://localhost:8000/api'

function formatRemaining(totalSeconds) {
  const seconds = Math.max(0, Number(totalSeconds || 0))
  const mins = Math.floor(seconds / 60)
  const secs = seconds % 60
  return `${mins} min ${secs} s`
}

function Badge({ type, children }) {
  return <span className={`badge badge-${type}`}>{children}</span>
}

export default function App() {
  const [summary, setSummary] = useState(null)
  const [analysis, setAnalysis] = useState(null)
  const [news, setNews] = useState([])
  const [current, setCurrent] = useState(null)
  const [history, setHistory] = useState([])
  const [events, setEvents] = useState([])
  const [alerts, setAlerts] = useState([])
  const [executions, setExecutions] = useState([])
  const [error, setError] = useState('')
  const [currentInfo, setCurrentInfo] = useState('')
  const [cooldownMessage, setCooldownMessage] = useState('')
  const [cooldownRemaining, setCooldownRemaining] = useState(0)
  const [tab, setTab] = useState('dashboard')
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (cooldownRemaining <= 0) return undefined
    const timer = setInterval(() => {
      setCooldownRemaining((prev) => {
        const next = prev - 1
        if (next <= 0) {
          setCooldownMessage('')
          return 0
        }
        setCooldownMessage(`Esperá ${formatRemaining(next)} para nuevo análisis.`)
        return next
      })
    }, 1000)
    return () => clearInterval(timer)
  }, [cooldownRemaining])

  const load = async () => {
    setError('')
    setCurrentInfo('')
    try {
      const [sRes, aRes, nRes, cRes, hRes, evRes, alRes, exRes] = await Promise.all([
        fetch(`${API}/portfolio/summary`),
        fetch(`${API}/portfolio/analysis`),
        fetch(`${API}/news/recent`),
        fetch(`${API}/recommendations/current`),
        fetch(`${API}/history`),
        fetch(`${API}/events/recent`),
        fetch(`${API}/alerts/current`),
        fetch(`${API}/executions/recent`),
      ])

      if (!sRes.ok || !aRes.ok || !nRes.ok || !hRes.ok) throw new Error('backend_unavailable')

      setSummary(await sRes.json())
      setAnalysis(await aRes.json())
      setNews(await nRes.json())
      setHistory(await hRes.json())
      if (evRes.ok) setEvents(await evRes.json())
      if (alRes.ok) setAlerts(await alRes.json())
      if (exRes.ok) setExecutions(await exRes.json())

      if (cRes.status === 404) {
        setCurrent(null)
        setCurrentInfo('No hay recomendación abierta actualmente.')
      } else if (cRes.ok) {
        setCurrent(await cRes.json())
      } else {
        throw new Error('backend_unavailable')
      }
    } catch {
      setError('Backend no disponible.')
      setSummary(null)
      setAnalysis(null)
      setNews([])
      setCurrent(null)
      setHistory([])
      setEvents([])
      setAlerts([])
      setExecutions([])
      setCurrentInfo('')
    }
  }

  useEffect(() => { load() }, [])

  const triggerAnalysis = async () => {
    setError('')
    setLoading(true)
    try {
      const resp = await fetch(`${API}/analysis/run`, { method: 'POST' })
      if (!resp.ok) throw new Error('trigger_failed')
      const payload = await resp.json()

      if (payload?.status === 'cooldown' && payload?.skipped) {
        const remainingSeconds = Number(payload.cooldown_remaining_seconds || 0)
        setCooldownRemaining(remainingSeconds)
        setCooldownMessage(`Esperá ${formatRemaining(remainingSeconds)} para nuevo análisis.`)
        await load()
        return
      }

      setCooldownRemaining(0)
      setCooldownMessage('')
      await load()
    } catch {
      setError('No se pudo ejecutar el análisis.')
    } finally {
      setLoading(false)
    }
  }

  const triggerIngestion = async () => {
    setError('')
    try {
      const resp = await fetch(`${API}/events/run-ingestion`, { method: 'POST' })
      if (!resp.ok) throw new Error('ingestion_failed')
      await load()
    } catch {
      setError('No se pudo ejecutar la ingesta.')
    }
  }

  const acknowledgeAlert = async (alertId) => {
    await fetch(`${API}/alerts/${alertId}/acknowledge`, { method: 'POST' })
    load()
  }

  const approveRecommendation = async () => {
    if (!current?.id) return
    setLoading(true)
    try {
      const resp = await fetch(`${API}/recommendations/${current.id}/approve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ note: '' }),
      })
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({}))
        setError(err.detail || 'Error al aprobar')
      }
      await load()
    } catch {
      setError('Error al aprobar la recomendación.')
    } finally {
      setLoading(false)
    }
  }

  const rejectRecommendation = async () => {
    if (!current?.id) return
    try {
      await fetch(`${API}/recommendations/${current.id}/reject`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ note: '' }),
      })
      await load()
    } catch {
      setError('Error al rechazar.')
    }
  }

  const tabs = [
    { id: 'dashboard', label: 'Inicio' },
    { id: 'recommendation', label: 'Rec.' },
    { id: 'alerts', label: `Alertas${alerts.length ? ` (${alerts.length})` : ''}` },
    { id: 'executions', label: 'Ejecuciones' },
    { id: 'history', label: 'Historial' },
  ]

  return (
    <main className="container">
      <div className="app-header">
        <h1>Mi Finanza</h1>
        <div className="header-actions">
          <button onClick={triggerAnalysis} disabled={cooldownRemaining > 0 || loading} className="btn-primary">
            {loading ? 'Analizando...' : 'Analizar'}
          </button>
          <button onClick={triggerIngestion}>Ingestar</button>
          <button onClick={load} className="btn-sm">Refrescar</button>
        </div>
      </div>

      {cooldownMessage && <p className="info-cooldown">{cooldownMessage}</p>}
      {error && <p className="error-msg">{error}</p>}

      <div className="tab-bar">
        {tabs.map((t) => (
          <button key={t.id} className={`tab-btn ${tab === t.id ? 'active' : ''}`} onClick={() => setTab(t.id)}>
            {t.label}
          </button>
        ))}
      </div>

      {/* DASHBOARD TAB */}
      {tab === 'dashboard' && (
        <>
          {summary && (
            <section>
              <h2>Portfolio</h2>
              <p><strong>{summary.total_value.toLocaleString()}</strong> {summary.currency}</p>
              <p>Cash: {summary.cash.toLocaleString()} {summary.currency}</p>
              <p style={{ fontSize: '0.85em', color: '#888' }}>Actualizado: {new Date(summary.created_at).toLocaleString()}</p>
              <h3>Posiciones</h3>
              <ul>
                {summary.positions.map((p) => (
                  <li key={p.symbol}>
                    <strong>{p.symbol}</strong> — {p.market_value.toLocaleString()} {p.currency}
                    <span style={{ fontSize: '0.85em', color: '#888' }}> ({p.asset_type})</span>
                  </li>
                ))}
              </ul>
            </section>
          )}

          {analysis && (
            <section>
              <h2>Análisis</h2>
              {analysis.profile_label && <p>Perfil: <strong>{analysis.profile_label}</strong></p>}
              <p>Concentración: {(analysis.concentration_score * 100).toFixed(1)}% | Riesgo: {(analysis.risk_score * 100).toFixed(1)}%</p>
              {analysis.equity_weight != null && <p>Equity: {(analysis.equity_weight * 100).toFixed(1)}%</p>}
              <h3>Moneda</h3>
              <ul>
                {Object.entries(analysis.weights_by_currency || {}).map(([ccy, w]) => (
                  <li key={ccy}>{ccy}: {(w * 100).toFixed(1)}%</li>
                ))}
              </ul>
              {analysis.weights_by_bucket && (
                <>
                  <h3>Buckets</h3>
                  <ul>
                    {Object.entries(analysis.weights_by_bucket).map(([b, w]) => (
                      <li key={b}>{b}: {(w * 100).toFixed(1)}%</li>
                    ))}
                  </ul>
                </>
              )}
              <p>Alertas: {analysis.alerts?.join(' | ') || 'Sin alertas'}</p>
            </section>
          )}

          {news.length > 0 && (
            <section>
              <h2>Noticias</h2>
              {current?.news_summary && (
                <div className="detail-panel" style={{ whiteSpace: 'pre-wrap', marginBottom: 8 }}>
                  <strong>Resumen LLM:</strong>
                  <p style={{ margin: '4px 0' }}>{current.news_summary}</p>
                </div>
              )}
              <ul>
                {news.map((item) => (
                  <li key={item.id}>
                    <strong>{item.title}</strong> — {item.event_type} — {item.impact}
                  </li>
                ))}
              </ul>
            </section>
          )}
        </>
      )}

      {/* RECOMMENDATION TAB */}
      {tab === 'recommendation' && (
        <>
          {currentInfo && !current && <p>{currentInfo}</p>}

          {current && (
            <section>
              <h2>Recomendación actual</h2>
              {current.unchanged && (
                <div className="info-box info-unchanged">
                  Sin cambios materiales. Se mantiene la recomendación anterior.
                </div>
              )}

              <p>Estado: <Badge type={current.status}>{current.status}</Badge></p>
              {current.status === 'blocked' && <div className="info-box info-blocked">{current.blocked_reason}</div>}

              <p>Acción: <strong>{current.action}</strong> | {(current.suggested_pct * 100).toFixed(2)}% | Confianza: {(current.confidence * 100).toFixed(0)}%</p>
              {current.profile_label && <p>Perfil: <strong>{current.profile_label}</strong></p>}
              <p>{current.recommendation_explanation_llm || current.rationale}</p>

              {(current.rationale_reasons || []).length > 0 && (
                <div className="detail-panel">
                  <strong>Detalle:</strong>
                  <ul>
                    {current.rationale_reasons.map((r, i) => (
                      <li key={i} style={{ fontSize: '0.9em' }}>
                        <span className="reason-tag">{r.type}</span>
                        {r.detail}
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              <p style={{ fontSize: '0.9em' }}>Riesgos: {current.risks}</p>
              <p style={{ fontSize: '0.9em' }}>{current.executive_summary}</p>

              <h3>Activos afectados</h3>
              <ul>
                {current.actions.map((a, idx) => (
                  <li key={idx}>{a.symbol}: {(a.target_change_pct * 100).toFixed(2)}% ({a.reason})</li>
                ))}
              </ul>

              {(current.status === 'pending' || current.status === 'blocked') && (
                <div className="actions">
                  <button onClick={approveRecommendation} className="btn-success" disabled={loading}>
                    {loading ? 'Procesando...' : 'Aprobar y Ejecutar'}
                  </button>
                  <button onClick={rejectRecommendation} className="btn-danger" disabled={loading}>
                    Rechazar
                  </button>
                </div>
              )}

              {current.status === 'approved' && (
                <div className="info-box" style={{ background: '#e8f5e9' }}>
                  Aprobada. Órdenes enviadas al broker.
                </div>
              )}
            </section>
          )}

          {current && (current.external_opportunities || []).length > 0 && (
            <section>
              <h2>Oportunidades externas</h2>
              <p style={{ fontSize: '0.85em', color: '#888' }}>Solo informativo. No genera órdenes.</p>
              {current.external_opportunities.map((op, idx) => (
                <div key={`${op.symbol}-${idx}`} className="opportunity-item">
                  <strong>{op.symbol}</strong>
                  <div className="opp-badges">
                    {(op.source_types || []).map((s) => <span key={s} className="opp-badge" style={{ background: '#e8eaf6' }}>{s}</span>)}
                    {op.investable && <span className="opp-badge" style={{ background: '#a5d6a7', fontWeight: 600 }}>invertible</span>}
                    {op.asset_type_status === 'unsupported' && <span className="opp-badge" style={{ background: '#ffab91' }}>no soportado</span>}
                  </div>
                  <div style={{ fontSize: '0.85em', color: '#666' }}>
                    prioridad: {op.priority_score ?? '-'} | impacto: {op.impact} | confianza: {(Number(op.confidence || 0) * 100).toFixed(0)}%
                  </div>
                  <div style={{ fontSize: '0.85em' }}>{op.reason}</div>
                </div>
              ))}
            </section>
          )}

          {current?.allowed_assets && (
            <section>
              <h2>Activos permitidos</h2>
              <p><strong>Holdings:</strong> {(current.allowed_assets.holdings || []).join(', ') || 'Ninguno'}</p>
              <p><strong>Whitelist:</strong> {(current.allowed_assets.whitelist || []).join(', ') || 'Ninguna'}</p>
              {(current.allowed_assets.watchlist || []).length > 0 && <p><strong>Watchlist:</strong> {current.allowed_assets.watchlist.join(', ')}</p>}
              {(current.allowed_assets.universe || []).length > 0 && <p><strong>Universo:</strong> {current.allowed_assets.universe.join(', ')}</p>}
            </section>
          )}
        </>
      )}

      {/* ALERTS TAB */}
      {tab === 'alerts' && (
        <>
          {alerts.length > 0 && (
            <section>
              <h2>Alertas activas</h2>
              {alerts.map((a) => (
                <div key={a.id} className="alert-item">
                  <Badge type={a.severity}>{a.severity}</Badge>
                  <div className="alert-content">
                    <strong>{a.message}</strong>
                    {a.affected_symbols?.length > 0 && <span> — {a.affected_symbols.join(', ')}</span>}
                    {a.triggered_recalc && <Badge type="low">recalculado</Badge>}
                    <div style={{ fontSize: '0.8em', color: '#888' }}>{a.trigger_type} | {a.created_at}</div>
                  </div>
                  <button onClick={() => acknowledgeAlert(a.id)} className="btn-sm">OK</button>
                </div>
              ))}
            </section>
          )}
          {alerts.length === 0 && <section><p>Sin alertas activas.</p></section>}

          {events.length > 0 && (
            <section>
              <h2>Eventos recientes</h2>
              {events.map((e) => (
                <div key={e.id} style={{ padding: '4px 0', borderBottom: '1px solid var(--border)', fontSize: '0.9em' }}>
                  <Badge type={e.severity}>{e.severity}</Badge>{' '}
                  {e.message}
                  {e.affected_symbols?.length > 0 && <span style={{ color: '#666' }}> [{e.affected_symbols.join(', ')}]</span>}
                  {e.triggered_recalc && <span style={{ color: '#388e3c', marginLeft: 4 }}>(recalc)</span>}
                  <span style={{ fontSize: '0.8em', color: '#999', marginLeft: 6 }}>{e.created_at}</span>
                </div>
              ))}
            </section>
          )}
        </>
      )}

      {/* EXECUTIONS TAB */}
      {tab === 'executions' && (
        <section>
          <h2>Ejecuciones recientes</h2>
          {executions.length === 0 && <p>Sin ejecuciones aún.</p>}
          {executions.map((ex) => (
            <div key={ex.id} className="execution-row">
              <Badge type={ex.status === 'executed' ? 'executed' : ex.status === 'failed' || ex.status === 'rejected_by_broker' ? 'failed' : 'pending'}>
                {ex.status}
              </Badge>
              <strong>{ex.symbol}</strong>
              <span>{ex.side}</span>
              <span style={{ fontSize: '0.85em' }}>{(ex.target_change_pct * 100).toFixed(2)}%</span>
              {ex.broker_order_id && <span style={{ fontSize: '0.8em', color: '#888' }}>#{ex.broker_order_id}</span>}
              {ex.error_message && <span style={{ fontSize: '0.8em', color: 'var(--danger)' }}>{ex.error_message}</span>}
              <span style={{ fontSize: '0.8em', color: '#999' }}>{ex.created_at}</span>
            </div>
          ))}
        </section>
      )}

      {/* HISTORY TAB */}
      {tab === 'history' && (
        <section>
          <h2>Historial</h2>
          {history.map((item) => (
            <div key={item.id} className="history-item">
              <Badge type={item.status}>{item.status}</Badge>{' '}
              <strong>{item.action}</strong> — {item.decision}
              <div style={{ fontSize: '0.85em', color: '#888' }}>
                {new Date(item.date).toLocaleString()} | {item.summary}
              </div>
            </div>
          ))}
        </section>
      )}
    </main>
  )
}
