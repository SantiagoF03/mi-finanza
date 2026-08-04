# Modo semiautomático V1

El scheduler **ingiere, analiza y propone**. Nunca aprueba, nunca envía y
nunca crea una `OrderExecution`. La única ruta ejecutable sigue siendo
`POST /api/recommendations/{id}/approve`, con todos sus candados intactos.

## Secuencia operativa

```
ingesta y snapshot
→ análisis
→ como máximo UNA recomendación abierta
→ notificación de revisión humana (una sola vez)
→ espera
→ aprobación o rechazo humano
→ ejecución solo por la ruta reforzada
→ conciliación manual si el resultado es incierto
→ recién entonces puede crearse otra recomendación
```

## Puerta central de creación

Toda creación de `Recommendation` pasa por
`app.services.analysis_gate.check_recommendation_creation_allowed`, evaluada
**dentro del lease** e inmediatamente antes de persistir. Bloquea mientras
exista:

- una `Recommendation` en estado **no terminal** (incluye un piloto sin
  resolver — un piloto es simplemente una recomendación no terminal marcada
  con `execution_pilot=true`);
- una `OrderExecution` en estado **no terminal**.

### Estados declarados una sola vez

| | Terminal (no bloquea) | Bloqueante |
|---|---|---|
| **Recommendation** | `rejected`, `approved`, `executed`, `execution_failed`, `superseded`, `expired`, `cancelled` | todo lo demás (`pending`, `blocked`, `execution_pending`, `manual_reconciliation_required`, `execution_partial`, …) |
| **OrderExecution** | `executed`, `failed`, `rejected_by_broker`, `validation_failed`, `not_sent_confirmed`, `preflight_cancelled`, `cancelled` | `execution_requested`, `execution_ready`, `submitting`, `execution_sent`, `manual_reconciliation_required` |

`execution_sent` bloquea a propósito: hasta que se confirme terminalmente (o
se concilie) no se puede probar el resultado, así que no se toma una decisión
nueva.

## Sin supersession automática

`_supersede_open_recommendations` quedó como **tripwire**: lanza
`RuntimeError`. Ningún flujo ordinario (scheduler, ingesta, full cycle,
postmarket, análisis manual, LLM, fallback) puede marcar `superseded` una
recomendación abierta. La única supersession legítima es la explícita y
auditada del endpoint administrativo del piloto.

## Códigos de skip (constantes estables)

| Código | Significado |
|---|---|
| `open_recommendation_requires_decision` | Hay una recomendación esperando decisión humana |
| `execution_requires_resolution` | Hay una ejecución no terminal sin resolver |
| `analysis_lease_unavailable` | Otro proceso tiene el lease |
| `analysis_lease_error` | Error al adquirir el lease → fail closed |
| `recommendation_creation_conflict` | La puerta no pudo verificar el estado → fail closed |

Un skip devuelve **HTTP 200** con `skipped=true` (no es un error), e incluye
`code`, `source`, `blocking_recommendation_id`, `blocking_execution_id`,
`deferred_events_count` y `snapshot_id`.

Un ciclo bloqueado **no refresca el snapshot**: hacerlo invalidaría el
`preview_hash` firmado de la recomendación abierta.

## Lease persistente (`analysis_leases`)

Fila única con adquisición **atómica** mediante un solo UPDATE condicional:

```sql
UPDATE analysis_leases
   SET owner_id=?, acquired_at=?, expires_at=?
 WHERE name='analysis_cycle'
   AND (expires_at IS NULL OR expires_at <= :now)
```

La transacción que commitea primero empuja `expires_at` al futuro, así que el
`WHERE` del perdedor deja de matchear y su `rowcount` es 0. **No hay ventana
read-then-write** (el patrón inseguro «SELECT y después INSERT»), que es lo
que lo hace correcto en SQLite, donde el lock de escritura serializa los
UPDATE. Usa el mismo engine que `SessionLocal` (se consolidó el engine
duplicado que había en `session.py`). `owner_id` es un token anónimo; la
observabilidad lo muestra truncado, nunca hostname ni secretos.

Vencido, cualquier proceso puede readquirirlo. Ante error: fail closed.

## Zona horaria

`SCHEDULER_TIMEZONE` (IANA, validada al arranque). **Default `UTC`, que
preserva exactamente los instantes actuales** — las horas de apertura/cierre
ya se interpretaban en UTC, y ahora APScheduler recibe la zona de forma
explícita en lugar de heredar la del host.

Para pasar a hora local: configurar `SCHEDULER_TIMEZONE=America/Argentina/Buenos_Aires`
**y ajustar deliberadamente** `SCHEDULER_MARKET_OPEN_HOUR` / `CLOSE_HOUR`
(p. ej. 11/20 UTC → 8/17 ART), revisando `interpreted_open_time` en
`/api/scheduler/status` antes y después. Cambiar solo la zona **sí** mueve la
hora real de ejecución.

## Eventos diferidos

Un ciclo con skip **no** marca los eventos como consumidos: siguen pendientes
y se procesan cuando se libera el bloqueo, sin duplicar recomendaciones.
`deferred_events_count` queda visible en el skip y en el status.

Conceptos separados: evento *ingerido* ≠ *analizado* ≠ *incorporado a una
recomendación*.

## Recuperación de `execution_requested`

Una caída puede dejar un intento en `execution_requested` **antes** de
cualquier llamada al broker. Ahora:

- aparece en la cola de conciliación;
- solo admite `confirm_not_sent` → `not_sent_confirmed`
  (`reason_code: prepared_but_not_submitted`);
- `confirm_sent` / `confirm_executed` / `confirm_rejected` → 409;
- nunca se consulta al broker ni se reenvía.

Igual que `execution_ready`. No hay endpoints de resume/retry/resubmit.

## Notificaciones

Una recomendación nueva genera **una sola** notificación de revisión humana
(deduplicada con un flag persistido), con `recommendation_id` y deep link, sin
credenciales, sin `preview_hash` y sin decir que se ejecutó algo. Un ciclo con
skip **no notifica**, así que un bloqueo persistente no genera spam.

## Variables nuevas

```
SCHEDULER_TIMEZONE=UTC            # IANA; default preserva el comportamiento actual
ANALYSIS_LEASE_TTL_SECONDS=600    # > 0
```

## Despliegue (no ejecutado acá)

1. Mergear con `SCHEDULER_ENABLED=false` (estado actual).
2. Desplegar. Al arrancar, `_patch_schema` crea `analysis_leases`.
3. Revisar `GET /api/scheduler/status`: `enabled_config=false`,
   `running=false`, `jobs=[]`, y `interpreted_open_time` / `close_time`.
4. Confirmar que la recomendación abierta sigue `pending` y que
   `/api/executions/reconciliation-queue` está vacía.
5. Recién entonces, y por separado, decidir `SCHEDULER_TIMEZONE` y las horas.
6. Recién entonces `SCHEDULER_ENABLED=true`, y volver a mirar el status.

`ORDER_EXECUTION_ENABLED`, `SANDBOX_EXECUTION_ENABLED` y
`EXECUTION_PILOT_CREATION_ENABLED` siguen en `false` y no se tocan en este
cambio.
