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

## Orden correcto: lease → gate → análisis → verificación final

El lease se adquiere **primero**; el gate se evalúa **dentro** del lease
(evaluarlo antes abriría un TOCTOU donde dos procesos leen «permitido»).
Inmediatamente **antes del INSERT** se ejecuta `verify_can_persist_recommendation`,
que:

1. **renueva el lease atómicamente** (`renew_analysis_lease`) — el UPDATE
   condicional exige `name` + `owner_id` + lease vigente, así que un
   `rowcount != 1` significa que se perdió (venció o lo tomó otro proceso);
2. **vuelve a correr el gate** — durante un análisis largo pudo aparecer una
   recomendación o ejecución bloqueante.

Si cualquiera de las dos falla, **no se persiste** y se devuelve un skip
estable. Un análisis que supera el TTL no puede escribir sin renovar: no se
asume que «normalmente tarda menos».

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

## Piloto bajo la misma exclusión

`create_execution_pilot_recommendation` adquiere **el mismo lease** y corre
**el mismo gate**. Si existe cualquier recomendación abierta devuelve **409**
y exige resolverla explícitamente. **No supersede nada**: la versión anterior
estampaba `superseded_at` + metadata pero dejaba la recomendación previa en
`pending`, lo que podía dejar dos recomendaciones abiertas a la vez. Ese
helper fue eliminado.

## Consumo transaccional de eventos

`consume_recalc_events(db, events, cycle_result)` es el **único** punto de
consumo, usado por `scheduled_ingestion` **y** `scheduled_full_cycle` (antes
el full cycle no consumía nada, así que sus eventos quedaban pendientes y
podían generar una recomendación duplicada). Marca `triggered_recalc=True` y
`recalc_recommendation_id` **solo** cuando el ciclo devolvió
`recommendation_id`; en skip, cooldown, lease no disponible o error no
consume nada, y ambos campos se commitean juntos.

## Notificaciones

**Un solo push principal por recomendación**: la notificación de revisión
humana es el aviso principal; en el ciclo que crea la recomendación,
`dispatch_recommendation_alerts` corre con `suppress_push=True` (conserva su
auditoría, no manda push). Así la misma recomendación nunca produce dos
avisos.

La entrega se registra **solo con evidencia** (`sent > 0`). Estados
persistidos:

| Campo | Valores |
|---|---|
| `review_notification_status` | `pending` / `delivered` / `failed` / `disabled` |
| `review_notification_attempts` | contador (máx. 3) |
| `review_notification_last_attempt_at` | ISO |
| `review_notification_delivered_at` | ISO, solo en `delivered` |

`review_notification_sent=True` se escribe **únicamente** en entrega real.
Notificaciones deshabilitadas, excepciones y `sent=0` dejan un estado
reintentable con cooldown de 300 s y máximo 3 intentos — el reintento aplica
**solo al transporte**; el análisis y la ejecución nunca se reintentan. Un
ciclo con skip no notifica.

## Observabilidad

`last_status` distingue `created` / `skipped` / `error` / `no_cycle_needed`
(antes decía `ok` incluso en skips) y `last_source` conserva el source real
(`scheduler_event` / `scheduler` / `manual`) en vez de un literal del job.
Los campos `last_skip_code`, `blocking_recommendation_id`,
`blocking_execution_id` y `deferred_events_count` se limpian en cuanto dejan
de aplicar, así que el status nunca muestra un bloqueo obsoleto. El cooldown
tiene código estable `analysis_cooldown` y el payload completo.

## Timezone portable

`python:3.11-slim` **no** trae la base IANA del sistema, así que `zoneinfo`
no podría resolver `America/Argentina/Buenos_Aires`. Se agregó **`tzdata`**
a `requirements.txt`. El default sigue siendo `UTC` y los horarios
productivos no cambian.

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
