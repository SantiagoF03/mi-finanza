import { useEffect, useState } from 'react'

import { API, authHeaders } from './api'

function urlBase64ToUint8Array(base64String) {
  const padding = '='.repeat((4 - (base64String.length % 4)) % 4)
  const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/')
  const rawData = atob(base64)
  return new Uint8Array([...rawData].map((c) => c.charCodeAt(0)))
}

function NotificationPanel({ api }) {
  const supported =
    typeof Notification !== 'undefined' &&
    'serviceWorker' in navigator &&
    'PushManager' in window

  const [permission, setPermission] = useState(() =>
    supported ? Notification.permission : 'unsupported'
  )
  const [swState, setSwState] = useState('checking')
  const [subscribed, setSubscribed] = useState(null)
  const [loading, setLoading] = useState(false)
  const [msg, setMsg] = useState('')

  useEffect(() => {
    if (!supported) { setSwState('unavailable'); setSubscribed(false); return }
    navigator.serviceWorker.ready
      .then((reg) => { setSwState('ready'); return reg.pushManager.getSubscription() })
      .then((sub) => setSubscribed(!!sub))
      .catch(() => { setSwState('unavailable'); setSubscribed(false) })
  }, [])

  const activate = async () => {
    setLoading(true); setMsg('')
    try {
      const perm = await Notification.requestPermission()
      setPermission(perm)
      if (perm !== 'granted') {
        setMsg('Permiso denegado. En Chrome Android: Configuración → Sitios → Notificaciones.')
        return
      }
      const vapidRes = await fetch(`${api}/push/vapid-public-key`)
      const { vapid_public_key } = await vapidRes.json()
      if (!vapid_public_key) {
        setMsg('VAPID key no configurada en el servidor. Revisá VAPID_PUBLIC_KEY en el .env del backend.')
        return
      }
      const reg = await navigator.serviceWorker.ready
      setSwState('ready')
      const sub = await reg.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(vapid_public_key),
      })
      const subJson = sub.toJSON()
      const postRes = await fetch(`${api}/push/subscribe`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ endpoint: subJson.endpoint, keys: subJson.keys }),
      })
      if (!postRes.ok) throw new Error('Error registrando suscripción en el servidor.')
      // Auto-promote channel to web_push so the persisted setting matches the real delivery path.
      try {
        await fetch(`${api}/notifications/settings`, {
          method: 'PUT',
          headers: authHeaders({ 'Content-Type': 'application/json' }),
          body: JSON.stringify({ notification_channel: 'web_push' }),
        })
      } catch { /* non-fatal: subscription still works */ }
      setSubscribed(true)
      setMsg('Suscripción activa. Canal configurado como web push. Ya podés recibir notificaciones.')
    } catch (err) {
      setMsg(`Error: ${err.message}`)
    } finally {
      setLoading(false)
    }
  }

  const sendTest = async () => {
    setLoading(true); setMsg('')
    try {
      const res = await fetch(`${api}/push/test`, { method: 'POST', headers: authHeaders() })
      const data = await res.json()
      if (res.ok) {
        const sent = data.sent ?? 0
        const failed = data.failed ?? 0
        setMsg(
          sent > 0 ? 'Push enviado. Revisá tu celular.' :
          failed > 0 ? `Falló el envío (${failed} errores). Verificá claves VAPID.` :
          'Sin suscripciones activas. Activá notificaciones primero.'
        )
      } else {
        setMsg('Error al enviar push de prueba.')
      }
    } catch { setMsg('No se pudo conectar con el servidor.') }
    finally { setLoading(false) }
  }

  const permLabel = permission === 'granted' ? '✓ Otorgado' : permission === 'denied' ? '✗ Denegado' : permission === 'default' ? 'Sin definir' : 'No soportado'
  const swLabel = swState === 'ready' ? '✓ Activo' : swState === 'checking' ? '...' : '✗ No disponible'
  const subLabel = subscribed === null ? '...' : subscribed ? '✓ Activa' : 'Sin suscripción'
  const canActivate = supported && permission !== 'denied' && !subscribed && subscribed !== null

  return (
    <section>
      <h2>Notificaciones Push</h2>
      {!supported && (
        <div className="info-box info-blocked">Navegador no soporta push. Usá Chrome en Android.</div>
      )}
      {supported && (
        <>
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 12 }}>
            <span style={{ fontSize: '0.85em' }}>Permiso: <strong>{permLabel}</strong></span>
            <span style={{ fontSize: '0.85em' }}>SW: <strong>{swLabel}</strong></span>
            <span style={{ fontSize: '0.85em' }}>Suscripción: <strong>{subLabel}</strong></span>
          </div>
          {permission === 'denied' && (
            <div className="info-box info-blocked" style={{ marginBottom: 8 }}>
              Bloqueado. En Chrome Android: Configuración → Configuración del sitio → Notificaciones → habilitá este sitio.
            </div>
          )}
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {canActivate && (
              <button onClick={activate} disabled={loading} className="btn-primary">
                {loading ? 'Activando...' : 'Activar notificaciones'}
              </button>
            )}
            {subscribed && (
              <button onClick={sendTest} disabled={loading}>
                {loading ? 'Enviando...' : 'Probar notificación'}
              </button>
            )}
          </div>
          {msg && (
            <p style={{ marginTop: 8, fontSize: '0.9em', color: msg.startsWith('Error') || msg.startsWith('Bloq') || msg.startsWith('Falló') ? 'var(--danger)' : 'var(--success)' }}>
              {msg}
            </p>
          )}
        </>
      )}
    </section>
  )
}

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
  const [executionPreview, setExecutionPreview] = useState(null)
  const [previewError, setPreviewError] = useState('')
  const [showConfirm, setShowConfirm] = useState(false)
  const [confirmText, setConfirmText] = useState('')
  // Execution credential: React state only. Never localStorage/sessionStorage,
  // never VITE_*, never logged. Cleared on close and after submit.
  const [executionKeyInput, setExecutionKeyInput] = useState('')
  const [execReadiness, setExecReadiness] = useState(null)
  // Reconciliation (Ejecuciones tab)
  const [reconQueue, setReconQueue] = useState([])
  const [reconTarget, setReconTarget] = useState(null)
  const [reconAction, setReconAction] = useState('confirm_not_sent')
  const [reconPhrase, setReconPhrase] = useState('')
  const [reconNote, setReconNote] = useState('')
  const [reconBrokerId, setReconBrokerId] = useState('')
  const [reconQty, setReconQty] = useState('')
  const [reconPrice, setReconPrice] = useState('')
  // Same rules as executionKeyInput: React state only, cleared on close/submit.
  const [reconKeyInput, setReconKeyInput] = useState('')
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

  const loadExecutionPreview = async (recommendationId) => {
    if (!recommendationId) {
      setExecutionPreview(null)
      setPreviewError('')
      return
    }

    try {
      const res = await fetch(`${API}/recommendations/${recommendationId}/execution-preview`, {
        headers: authHeaders(),
      })

      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        setExecutionPreview(null)
        setPreviewError(err.detail || 'No se pudo cargar la vista previa de ejecución.')
        return
      }

      const data = await res.json()
      setExecutionPreview(data)
      setPreviewError('')
    } catch {
      setExecutionPreview(null)
      setPreviewError('No se pudo conectar para cargar la vista previa de ejecución.')
    }
  }

  const load = async () => {
    setError('')
    setCurrentInfo('')
    try {
      const [sRes, aRes, nRes, cRes, hRes, evRes, alRes, exRes, rqRes, erRes] = await Promise.all([
        fetch(`${API}/portfolio/summary`),
        fetch(`${API}/portfolio/analysis`),
        fetch(`${API}/news/recent`),
        fetch(`${API}/recommendations/current`),
        fetch(`${API}/history`),
        fetch(`${API}/events/recent`),
        fetch(`${API}/alerts/current`),
        fetch(`${API}/executions/recent`, { headers: authHeaders() }),
        fetch(`${API}/executions/reconciliation-queue`, { headers: authHeaders() }),
        fetch(`${API}/broker/execution-readiness`, { headers: authHeaders() }),
      ])

      if (!sRes.ok || !aRes.ok || !nRes.ok || !hRes.ok) throw new Error('backend_unavailable')

      setSummary(await sRes.json())
      setAnalysis(await aRes.json())
      setNews(await nRes.json())
      setHistory(await hRes.json())
      if (evRes.ok) setEvents(await evRes.json())
      if (alRes.ok) setAlerts(await alRes.json())
      if (exRes.ok) setExecutions(await exRes.json())
      if (rqRes.ok) setReconQueue(await rqRes.json())
      if (erRes.ok) setExecReadiness(await erRes.json())

      if (cRes.status === 404) {
        setCurrent(null)
        setCurrentInfo('No hay recomendación abierta actualmente.')
        setExecutionPreview(null)
        setPreviewError('')
      } else if (cRes.ok) {
        const currentPayload = await cRes.json()
        setCurrent(currentPayload)

        if (currentPayload?.id && (currentPayload.status === 'pending' || currentPayload.status === 'blocked')) {
          await loadExecutionPreview(currentPayload.id)
        } else {
          setExecutionPreview(null)
          setPreviewError('')
        }
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
      setExecutionPreview(null)
      setPreviewError('')
      setReconQueue([])
    }
  }

  useEffect(() => { load() }, [])

  const RECON_ACTIONS = {
    confirm_not_sent: { label: 'Confirmar NO enviada', phrase: (id) => `CONCILIAR EJECUCION ${id} COMO NO ENVIADA` },
    confirm_sent: { label: 'Confirmar enviada', phrase: (id) => `CONCILIAR EJECUCION ${id} COMO ENVIADA` },
    confirm_rejected: { label: 'Confirmar rechazada', phrase: (id) => `CONCILIAR EJECUCION ${id} COMO RECHAZADA` },
    confirm_executed: { label: 'Confirmar ejecutada', phrase: (id) => `CONCILIAR EJECUCION ${id} COMO EJECUTADA` },
  }

  const closeRecon = () => {
    setReconTarget(null)
    setReconAction('confirm_not_sent')
    setReconPhrase('')
    setReconNote('')
    setReconBrokerId('')
    setReconQty('')
    setReconPrice('')
    setReconKeyInput('')
  }

  const submitReconciliation = async () => {
    if (!reconTarget?.id) return
    const required = RECON_ACTIONS[reconAction]?.phrase(reconTarget.id)
    // Frontend guards — the backend remains the final authority.
    if (reconPhrase.trim() !== required) {
      setError(`Frase de conciliación incorrecta. Escribí exactamente: ${required}`)
      return
    }
    if (!reconNote.trim()) {
      setError('La nota de conciliación es obligatoria (motivo y evidencia).')
      return
    }
    if (!reconKeyInput) {
      setError('Ingresá la credencial de ejecución.')
      return
    }
    if (reconAction === 'confirm_sent' && !reconBrokerId.trim()) {
      setError('confirm_sent requiere el broker order id.')
      return
    }
    if (reconAction === 'confirm_executed' && (!reconBrokerId.trim() || !(Number(reconQty) > 0) || !(Number(reconPrice) > 0))) {
      setError('confirm_executed requiere broker order id, cantidad y precio positivos.')
      return
    }

    setLoading(true)
    try {
      const resp = await fetch(`${API}/executions/${reconTarget.id}/reconcile`, {
        method: 'POST',
        headers: authHeaders({
          'Content-Type': 'application/json',
          'X-Execution-Key': reconKeyInput,
        }),
        body: JSON.stringify({
          action: reconAction,
          confirmation_text: reconPhrase.trim(),
          note: reconNote.trim(),
          broker_order_id: reconBrokerId.trim() || null,
          executed_quantity: reconQty ? Number(reconQty) : null,
          executed_price: reconPrice ? Number(reconPrice) : null,
        }),
      })
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({}))
        setError(err.detail || 'Error al conciliar la ejecución.')
      } else {
        setError('')
      }
      await load()
    } catch {
      setError('Error al conciliar la ejecución.')
    } finally {
      setLoading(false)
      closeRecon()
    }
  }

  const triggerAnalysis = async () => {
    setError('')
    setLoading(true)
    try {
      const resp = await fetch(`${API}/analysis/run`, { method: 'POST', headers: authHeaders() })
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

  const closeConfirm = () => {
    setShowConfirm(false)
    setConfirmText('')
    setExecutionKeyInput('')
  }

  const openConfirm = () => {
    if (!executionPreview?.can_submit_approval) {
      setError('La aprobación no está disponible según el preview actual.')
      return
    }
    setError('')
    setShowConfirm(true)
  }

  const approveRecommendation = async () => {
    if (!current?.id) return

    // Frontend guards — the backend remains the final authority.
    if (!executionPreview) {
      setError('No hay vista previa de ejecución cargada.')
      return
    }
    if (executionPreview.can_submit_approval !== true) {
      setError('La aprobación no está disponible según el preview actual.')
      return
    }
    if (!executionPreview.preview_hash) {
      setError('El preview no está firmado. No se puede aprobar.')
      return
    }
    if (executionPreview.expires_at && new Date(executionPreview.expires_at) <= new Date()) {
      setError('El preview venció. Refrescá para generar uno nuevo.')
      return
    }
    const orders = executionPreview.orders_preview || []
    if (!orders.length || orders.some((o) => !o.valid)) {
      setError('No hay órdenes válidas para ejecutar.')
      return
    }
    const requiredPhrase = `EJECUTAR RECOMENDACION ${current.id}`
    if (confirmText.trim() !== requiredPhrase) {
      setError(`Frase de confirmación incorrecta. Escribí exactamente: ${requiredPhrase}`)
      return
    }
    if (!executionKeyInput) {
      setError('Ingresá la credencial de ejecución.')
      return
    }

    setLoading(true)
    try {
      const resp = await fetch(`${API}/recommendations/${current.id}/approve`, {
        method: 'POST',
        headers: authHeaders({
          'Content-Type': 'application/json',
          'X-Execution-Key': executionKeyInput,
        }),
        body: JSON.stringify({
          note: '',
          preview_hash: executionPreview.preview_hash,
          preview_generated_at: executionPreview.generated_at,
          confirmation_text: confirmText.trim(),
        }),
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
      closeConfirm()
    }
  }

  const rejectRecommendation = async () => {
    if (!current?.id) return
    try {
      await fetch(`${API}/recommendations/${current.id}/reject`, {
        method: 'POST',
        headers: authHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify({ note: '' }),
      })
      await load()
    } catch {
      setError('Error al rechazar.')
    }
  }

  const executionEnabled = executionPreview?.order_execution_enabled === true
  const canApproveReal = executionPreview?.can_submit_approval === true

  const BLOCKING_LABELS = {
    execution_locked: 'ejecución bloqueada por safety lock',
    execution_admin_key_not_configured: 'credencial de ejecución no configurada',
    preview_signing_not_configured: 'firma de preview no configurada',
    execution_limits_not_configured: 'límites de ejecución no configurados',
    recommendation_not_pending: 'la recomendación no está pendiente',
    recommendation_stale: 'la recomendación excede la antigüedad máxima',
    recommendation_superseded: 'la recomendación fue reemplazada',
    snapshot_missing: 'no hay snapshot de portfolio',
    invalid_order: 'hay órdenes inválidas',
    order_limit_exceeded: 'límite por orden excedido',
    total_limit_exceeded: 'límite total excedido',
    portfolio_pct_limit_exceeded: 'límite porcentual del portfolio excedido',
    currency_mismatch: 'monedas incompatibles, no se convierte automáticamente',
    already_executed: 'la recomendación ya fue ejecutada',
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

              {previewError && <div className="info-box info-blocked">{previewError}</div>}

              {executionPreview && (
                <div className="detail-panel">
                  <h3>Vista previa de ejecución</h3>
                  <p><strong>Broker:</strong> {executionPreview.broker_mode}</p>
                  <p>
                    <strong>Safety lock:</strong>{' '}
                    {executionPreview.order_execution_enabled ? 'Ejecución habilitada' : 'Ejecución bloqueada'}
                  </p>
                  <p><strong>Dry run:</strong> {String(executionPreview.dry_run)}</p>
                  <p><strong>Ejecutaría:</strong> {String(executionPreview.would_execute)}</p>
                  {executionPreview.snapshot_id != null && (
                    <p style={{ fontSize: '0.85em', color: '#888' }}>
                      Snapshot #{executionPreview.snapshot_id}
                      {executionPreview.generated_at && ` · Generado: ${new Date(executionPreview.generated_at).toLocaleString()}`}
                      {executionPreview.expires_at && ` · Vence: ${new Date(executionPreview.expires_at).toLocaleString()}`}
                    </p>
                  )}
                  <p><strong>Órdenes preview:</strong> {(executionPreview.orders_preview || []).length}</p>

                  {(executionPreview.orders_preview || []).map((o, idx) => (
                    <div key={`${o.symbol}-${idx}`} className="opportunity-item">
                      <strong>{o.symbol}</strong> — {o.side}
                      <div style={{ fontSize: '0.85em' }}>
                        Cantidad planificada: {o.quantity_planned} · Cambio objetivo: {(Number(o.target_change_pct || 0) * 100).toFixed(2)}%
                      </div>
                      <div style={{ fontSize: '0.85em' }}>
                        Portfolio usado: {Number(o.portfolio_value_used || 0).toLocaleString()} · Posición usada: {Number(o.position_value_used || 0).toLocaleString()}
                      </div>
                      <div style={{ fontSize: '0.85em' }}>
                        Precio ref snapshot: {Number(o.snapshot_price_ref || 0).toLocaleString()} · Monto estimado: {Number(o.estimated_notional || 0).toLocaleString()} · % portfolio: {(Number(o.portfolio_pct || 0) * 100).toFixed(2)}% · Válida: {o.valid ? 'sí' : 'no'}
                      </div>
                      {o.blocked_reason && (
                        <div className="info-box info-blocked">{o.blocked_reason}</div>
                      )}
                    </div>
                  ))}

                  {executionPreview.limits && (
                    <p style={{ fontSize: '0.85em', color: '#888' }}>
                      Límites: por orden {Number(executionPreview.limits.max_order_value || 0).toLocaleString()} · total {Number(executionPreview.limits.max_total_value || 0).toLocaleString()} · % portfolio {(Number(executionPreview.limits.max_portfolio_pct || 0) * 100).toFixed(2)}%
                      {' '}({executionPreview.limits.configured ? 'configurados' : 'no configurados'}) · Total estimado: {Number(executionPreview.limits.total_estimated_notional || 0).toLocaleString()}
                    </p>
                  )}
                  {executionPreview.execution_venue && (
                    <p style={{ fontSize: '0.85em', color: '#888' }}>
                      Política de orden: mercado {executionPreview.execution_venue.market || '—'} · plazo {executionPreview.execution_venue.settlement || '—'} · tipo {executionPreview.execution_venue.order_type || '—'} · validez {executionPreview.execution_venue.validity_minutes} min
                      {' '}· cotización: {executionPreview.execution_venue.quote_policy} (máx {executionPreview.execution_venue.max_quote_age_seconds}s) · desviación máx {(Number(executionPreview.execution_venue.max_price_deviation_pct || 0) * 100).toFixed(2)}%
                    </p>
                  )}
                </div>
              )}

              {executionPreview && (executionPreview.blocking_reasons || []).length > 0 && (
                <div className="info-box info-blocked">
                  La aprobación no está disponible:
                  <ul style={{ margin: '4px 0 0 16px' }}>
                    {executionPreview.blocking_reasons.map((code) => (
                      <li key={code}>{BLOCKING_LABELS[code] || code}</li>
                    ))}
                  </ul>
                </div>
              )}

              {executionPreview && !executionPreview.order_execution_enabled && (
                <div className="info-box info-blocked">
                  Ejecución real bloqueada por safety lock. La app puede analizar y previsualizar, pero no enviar órdenes.
                </div>
              )}

              {(current.status === 'pending' || current.status === 'blocked') && (
                <div className="actions">
                  <button
                    onClick={openConfirm}
                    className="btn-success"
                    disabled={loading || !canApproveReal}
                    title={!canApproveReal ? 'Aprobación no disponible según el preview' : 'Abre la confirmación de ejecución'}
                  >
                    {loading ? 'Procesando...' : canApproveReal ? 'Aprobar y Ejecutar' : 'Ejecución bloqueada'}
                  </button>
                  <button onClick={rejectRecommendation} className="btn-danger" disabled={loading}>
                    Rechazar
                  </button>
                </div>
              )}

              {showConfirm && executionPreview && canApproveReal && (
                <div className="detail-panel" style={{ border: '2px solid var(--danger, #c62828)', marginTop: 8 }}>
                  <h3>Confirmar ejecución real</h3>
                  <p style={{ fontSize: '0.9em' }}>
                    Recomendación #{current.id} · Broker: {executionPreview.broker_mode} · Snapshot #{executionPreview.snapshot_id}
                  </p>
                  <p style={{ fontSize: '0.85em', color: '#888' }}>
                    Generado: {executionPreview.generated_at && new Date(executionPreview.generated_at).toLocaleString()}
                    {' '}· Vence: {executionPreview.expires_at && new Date(executionPreview.expires_at).toLocaleString()}
                    {' '}· Hash: {(executionPreview.preview_hash || '').slice(0, 12)}…
                  </p>
                  <ul style={{ fontSize: '0.9em' }}>
                    {(executionPreview.orders_preview || []).map((o, idx) => (
                      <li key={`confirm-${o.symbol}-${idx}`}>
                        <strong>{o.symbol}</strong> {o.side} · cantidad {o.quantity_planned} · precio ref {Number(o.snapshot_price_ref || 0).toLocaleString()} · monto {Number(o.estimated_notional || 0).toLocaleString()} · {(Number(o.portfolio_pct || 0) * 100).toFixed(2)}% del portfolio
                      </li>
                    ))}
                  </ul>
                  {executionPreview.limits && (
                    <p style={{ fontSize: '0.85em', color: '#888' }}>
                      Límites aplicados: por orden {Number(executionPreview.limits.max_order_value || 0).toLocaleString()} · total {Number(executionPreview.limits.max_total_value || 0).toLocaleString()} · % {(Number(executionPreview.limits.max_portfolio_pct || 0) * 100).toFixed(2)}%
                    </p>
                  )}
                  <p style={{ fontSize: '0.9em' }}>
                    Para confirmar, escribí exactamente: <code>EJECUTAR RECOMENDACION {current.id}</code>
                  </p>
                  <input
                    type="text"
                    value={confirmText}
                    onChange={(e) => setConfirmText(e.target.value)}
                    placeholder={`EJECUTAR RECOMENDACION ${current.id}`}
                    style={{ display: 'block', width: '100%', marginBottom: 8, padding: 6 }}
                  />
                  <input
                    type="password"
                    value={executionKeyInput}
                    onChange={(e) => setExecutionKeyInput(e.target.value)}
                    placeholder="Credencial de ejecución"
                    autoComplete="off"
                    style={{ display: 'block', width: '100%', marginBottom: 8, padding: 6 }}
                  />
                  <div className="actions">
                    <button onClick={approveRecommendation} className="btn-danger" disabled={loading}>
                      {loading ? 'Ejecutando...' : 'Confirmar ejecución'}
                    </button>
                    <button onClick={closeConfirm} disabled={loading}>Cancelar</button>
                  </div>
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

          {/* --- Review Queue: human-first signal view (from decision_summary.review_queue) --- */}
          {(() => {
            const rq = current?.decision_summary?.review_queue
            const pc = current?.decision_summary?.pipeline_counts
            if (!rq) return null

            const suppressionLabel = (reason) => {
              const map = {
                weak_signal_not_tracked: 'Señal débil, no rastreado',
                weak_signal_low_score: 'Score bajo, señal insuficiente',
              }
              return map[reason] || reason
            }

            return (
              <>
                {/* Actionable now: top priority — goes first */}
                {rq.actionable_now && rq.actionable_now.count > 0 && (
                  <section>
                    <h2>Accionables ahora <span style={{ fontSize: '0.7em', color: '#888' }}>({rq.actionable_now.count})</span></h2>
                    <p style={{ fontSize: '0.85em', color: '#888' }}>Solo informativo. No genera órdenes automáticas.</p>
                    {(rq.actionable_now.items || []).map((item, idx) => (
                      <div key={`act-${item.symbol}-${idx}`} className="opportunity-item">
                        <strong>{item.symbol}</strong>
                        {item.effective_score != null && <span style={{ fontSize: '0.8em', color: '#888', marginLeft: 6 }}>score {(item.effective_score * 100).toFixed(0)}%</span>}
                        <div className="opp-badges">
                          {item.investable && <span className="opp-badge" style={{ background: '#a5d6a7', fontWeight: 600 }}>invertible</span>}
                          {item.signal_quality && <span className="opp-badge" style={{ background: item.signal_quality === 'strong' ? '#a5d6a7' : '#fff9c4' }}>{item.signal_quality === 'strong' ? 'señal fuerte' : 'señal débil'}</span>}
                        </div>
                        {item.reason && <div style={{ fontSize: '0.85em' }}>{item.reason}</div>}
                      </div>
                    ))}
                  </section>
                )}

                {/* Watchlist: main signal monitoring layer */}
                {rq.watchlist_now && rq.watchlist_now.count > 0 && (
                  <section>
                    <h2>Watchlist <span style={{ fontSize: '0.7em', color: '#888' }}>({rq.watchlist_now.count} señales)</span></h2>
                    {rq.watchlist_now.relevant_not_investable_count > 0 && (
                      <p style={{ fontSize: '0.85em', color: '#888' }}>
                        {rq.watchlist_now.investable_signal_count ?? 0} operables · {rq.watchlist_now.relevant_not_investable_count} no invertibles aún
                      </p>
                    )}
                    {(rq.watchlist_now.items || []).map((item, idx) => (
                      <div key={`wl-${item.symbol}-${idx}`} className="opportunity-item">
                        <strong>{item.symbol}</strong>
                        {item.effective_score != null && <span style={{ fontSize: '0.8em', color: '#888', marginLeft: 6 }}>score {(item.effective_score * 100).toFixed(0)}%</span>}
                        <div className="opp-badges">
                          {item.signal_quality && <span className="opp-badge" style={{ background: item.signal_quality === 'strong' ? '#a5d6a7' : '#fff9c4' }}>{item.signal_quality === 'strong' ? 'señal fuerte' : 'señal débil'}</span>}
                          {item.operational_status === 'relevant_not_investable' && <span className="opp-badge" style={{ background: '#ffab91' }}>no invertible</span>}
                        </div>
                        {item.reason && <div style={{ fontSize: '0.85em' }}>{item.reason}</div>}
                      </div>
                    ))}
                  </section>
                )}

                {/* Pipeline counters — context badges after the actionable sections */}
                {pc && (
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, padding: '8px 0' }}>
                    <Badge type="pending">{pc.actionable_count ?? 0} accionables</Badge>
                    <Badge type="low">{pc.observed_count ?? 0} observados</Badge>
                    <Badge type="blocked">{pc.suppressed_count ?? 0} suprimidos</Badge>
                    {(pc.promoted_from_observed_count ?? 0) > 0 && <Badge type="executed">{pc.promoted_from_observed_count} promovidos</Badge>}
                  </div>
                )}

                {/* Suppressed: discarded with reason */}
                {rq.suppressed_review && rq.suppressed_review.count > 0 && (
                  <section>
                    <h2>Suprimidos <span style={{ fontSize: '0.7em', color: '#888' }}>({rq.suppressed_review.count})</span></h2>
                    {(rq.suppressed_review.items || []).map((item, idx) => (
                      <div key={`sup-${item.symbol}-${idx}`} style={{ padding: '4px 0', borderBottom: '1px solid var(--border)', fontSize: '0.9em' }}>
                        <strong>{item.symbol}</strong>
                        <span style={{ color: '#888', marginLeft: 6 }}>— {suppressionLabel(item.suppression_reason)}</span>
                      </div>
                    ))}
                  </section>
                )}

                {/* Catalog compact: relegated, collapsed by default */}
                {rq.catalog_compact && rq.catalog_compact.count > 0 && (
                  <section>
                    <details>
                      <summary style={{ cursor: 'pointer', fontSize: '0.9em', color: '#888' }}>
                        Catálogo ({rq.catalog_compact.count} instrumentos)
                      </summary>
                      {(rq.catalog_compact.top_by_priority || []).map((item, idx) => (
                        <div key={`cat-${item.symbol}-${idx}`} style={{ padding: '2px 0', fontSize: '0.85em' }}>
                          {item.symbol}
                        </div>
                      ))}
                      {rq.catalog_compact.count > 3 && (
                        <p style={{ fontSize: '0.8em', color: '#999' }}>y {rq.catalog_compact.count - 3} más...</p>
                      )}
                    </details>
                  </section>
                )}
              </>
            )
          })()}

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
          <NotificationPanel api={API} />

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
      {tab === 'executions' && execReadiness && (
        <section>
          <h2>Estado de ejecución</h2>
          <div style={{ fontSize: '0.9em' }}>
            <p>
              Broker: <strong>{execReadiness.broker_mode}</strong> ({execReadiness.environment}) · Host: {execReadiness.api_host || '—'}
            </p>
            <p style={{ fontSize: '0.9em' }}>
              Lock real: <strong>{execReadiness.order_execution_enabled ? 'ABIERTO' : 'cerrado'}</strong>
              {' '}· Lock sandbox: <strong>{execReadiness.sandbox_execution_enabled ? 'ABIERTO' : 'cerrado'}</strong>
              {' '}· Credenciales: {execReadiness.credentials_configured ? '✓' : '✗'}
              {' '}· Execution key: {execReadiness.execution_admin_key_configured ? '✓' : '✗'}
              {' '}· Firma preview: {execReadiness.preview_secret_configured ? '✓' : '✗'}
              {' '}· Límites: {execReadiness.limits_configured ? '✓' : '✗'}
              {' '}· Política de orden: {execReadiness.order_policy_configured ? `✓ ${execReadiness.market}/${execReadiness.settlement}` : '✗'}
            </p>
            <p>
              Listo para ejecución real: <strong>{execReadiness.ready_for_real_execution ? 'SÍ' : 'no'}</strong>
              {' '}· Listo para sandbox: <strong>{execReadiness.ready_for_sandbox_execution ? 'SÍ' : 'no'}</strong>
            </p>
            {(execReadiness.blocking_reasons || []).length > 0 && (
              <details style={{ fontSize: '0.85em', color: '#888' }}>
                <summary style={{ cursor: 'pointer' }}>Motivos de bloqueo ({execReadiness.blocking_reasons.length})</summary>
                <ul style={{ margin: '4px 0 0 16px' }}>
                  {execReadiness.blocking_reasons.map((code) => <li key={code}>{code}</li>)}
                </ul>
              </details>
            )}
          </div>
        </section>
      )}

      {tab === 'executions' && reconQueue.length > 0 && (
        <section>
          <h2>Conciliación pendiente <span style={{ fontSize: '0.7em', color: '#888' }}>({reconQueue.length})</span></h2>
          <p style={{ fontSize: '0.85em', color: '#888' }}>
            Órdenes con resultado incierto. Conciliar registra una decisión manual: nunca reenvía ni crea órdenes.
          </p>
          {reconQueue.map((item) => (
            <div key={`recon-${item.id}`} className="opportunity-item">
              <Badge type={item.status}>{item.status}</Badge>{' '}
              <strong>{item.symbol}</strong> {item.side} · ejecución #{item.id} · rec #{item.recommendation_id} ({item.recommendation_status})
              <div style={{ fontSize: '0.85em' }}>
                Cantidad planificada: {item.quantity_planned} · Enviada: {item.quantity_sent ?? '—'} · Broker order: {item.broker_order_id || '—'}
              </div>
              <div style={{ fontSize: '0.85em', color: '#888' }}>
                Snapshot #{item.request_audit?.snapshot_id ?? '—'} · Hash: {(item.request_audit?.preview_hash || '').slice(0, 12) || '—'}…
                {' '}· Monto est.: {Number(item.request_audit?.estimated_notional || 0).toLocaleString()} · {item.created_at && new Date(item.created_at).toLocaleString()}
              </div>
              {item.error_message && <div style={{ fontSize: '0.85em', color: 'var(--danger)' }}>{item.error_message}</div>}
              {(item.reconciliation_audit || []).length > 0 && (
                <details style={{ fontSize: '0.8em', color: '#888' }}>
                  <summary style={{ cursor: 'pointer' }}>Auditoría previa ({item.reconciliation_audit.length})</summary>
                  {item.reconciliation_audit.map((a, i) => (
                    <div key={i}>{a.timestamp}: {a.action} ({a.previous_status} → {a.new_status}) — {a.note}</div>
                  ))}
                </details>
              )}
              {(item.status === 'submitting' || item.status === 'manual_reconciliation_required') && (
                <button className="btn-sm" onClick={() => { setReconTarget(item); setReconPhrase(''); }}>
                  Conciliar manualmente
                </button>
              )}
            </div>
          ))}

          {reconTarget && (
            <div className="detail-panel" style={{ border: '2px solid var(--danger, #c62828)', marginTop: 8 }}>
              <h3>Conciliar ejecución #{reconTarget.id}</h3>
              <p style={{ fontSize: '0.9em' }}>
                {reconTarget.symbol} {reconTarget.side} · cantidad {reconTarget.quantity_planned} · estado actual: {reconTarget.status}
              </p>
              <select value={reconAction} onChange={(e) => { setReconAction(e.target.value); setReconPhrase(''); }}
                      style={{ display: 'block', width: '100%', marginBottom: 8, padding: 6 }}>
                {Object.entries(RECON_ACTIONS).map(([value, cfg]) => (
                  <option key={value} value={value}>{cfg.label}</option>
                ))}
              </select>
              <p style={{ fontSize: '0.9em' }}>
                Escribí exactamente: <code>{RECON_ACTIONS[reconAction].phrase(reconTarget.id)}</code>
              </p>
              <input type="text" value={reconPhrase} onChange={(e) => setReconPhrase(e.target.value)}
                     placeholder={RECON_ACTIONS[reconAction].phrase(reconTarget.id)}
                     style={{ display: 'block', width: '100%', marginBottom: 8, padding: 6 }} />
              <input type="text" value={reconNote} onChange={(e) => setReconNote(e.target.value)}
                     placeholder="Nota obligatoria: motivo y evidencia"
                     style={{ display: 'block', width: '100%', marginBottom: 8, padding: 6 }} />
              {(reconAction === 'confirm_sent' || reconAction === 'confirm_executed') && (
                <input type="text" value={reconBrokerId} onChange={(e) => setReconBrokerId(e.target.value)}
                       placeholder="Broker order id (IOL)"
                       style={{ display: 'block', width: '100%', marginBottom: 8, padding: 6 }} />
              )}
              {reconAction === 'confirm_executed' && (
                <>
                  <input type="number" value={reconQty} onChange={(e) => setReconQty(e.target.value)}
                         placeholder="Cantidad ejecutada" min="0"
                         style={{ display: 'block', width: '100%', marginBottom: 8, padding: 6 }} />
                  <input type="number" value={reconPrice} onChange={(e) => setReconPrice(e.target.value)}
                         placeholder="Precio ejecutado" min="0"
                         style={{ display: 'block', width: '100%', marginBottom: 8, padding: 6 }} />
                </>
              )}
              <input type="password" value={reconKeyInput} onChange={(e) => setReconKeyInput(e.target.value)}
                     placeholder="Credencial de ejecución" autoComplete="off"
                     style={{ display: 'block', width: '100%', marginBottom: 8, padding: 6 }} />
              <div className="actions">
                <button onClick={submitReconciliation} className="btn-danger" disabled={loading}>
                  {loading ? 'Conciliando...' : 'Confirmar conciliación'}
                </button>
                <button onClick={closeRecon} disabled={loading}>Cancelar</button>
              </div>
            </div>
          )}
        </section>
      )}

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
