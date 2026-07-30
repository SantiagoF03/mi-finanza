# IOL Execution Readiness — guía operativa

Estado seguro por defecto (producción actual — NO cambiar sin decisión explícita):

```
BROKER_MODE=real
ORDER_EXECUTION_ENABLED=false
SANDBOX_EXECUTION_ENABLED=false
```

Con esa configuración la app **lee** la cartera real de IOL, analiza, genera
previews firmados, pero **no puede enviar ninguna orden** (ni real ni
sandbox). Todo lo que sigue está bloqueado por diseño fail-closed: cualquier
variable vacía o en 0 bloquea, nunca significa "sin límite".

Contrato HTTP de órdenes (Execution Contract V1):

- `POST /api/v2/operar/Comprar` | `POST /api/v2/operar/Vender`
- `Content-Type: application/x-www-form-urlencoded` (nunca JSON)
- Campos: `mercado`, `simbolo`, `cantidad` (entera exacta), `precio`
  (Decimal estable), `plazo`, `validez` (hora de Buenos Aires, mismo día
  operativo), `tipoOrden=precioLimite` (las órdenes de mercado están
  prohibidas en esta versión)
- Cotización ejecutable: `GET /api/v2/{mercado}/Titulos/{simbolo}/Cotizacion`
  (verificado contra la API real; el viejo `/api/v2/Cotizaciones/detalle/...`
  responde HTTP 400). Mejor bid para venta / mejor ask para compra —
  `ultimoPrecio` nunca se usa para ejecutar; sin punta no hay orden
- Semántica de envío: *at-most-once automatic submission + manual
  reconciliation on uncertain outcome* (timeouts, 5xx o 2xx sin
  `numeroOperacion` terminan en conciliación manual, jamás en reintento)
- **Clasificación de respuestas 2xx** (verificado contra la API real): IOL
  puede responder **HTTP 202 con un cuerpo de errores de validación**
  (`[{"title": "PrecioLimite", "description": "Los decimales indicados no
  son compatibles con la alteración mínima permitida..."}]`). Eso es un
  **rechazo definitivo** — la orden nunca se creó — y se clasifica como
  `rejected`, no como envío incierto. Solo un 2xx con `numeroOperacion` no
  vacío cuenta como enviado; un 2xx desconocido sigue siendo incierto.

## Etapas permitidas (en orden, sin saltear)

```
1. mock local              → flujo completo sin red
2. sandbox IOL             → misma app, host sandbox, credenciales sandbox
3. revisión humana         → leer request_audit.iol_request de cada orden
4. conciliación sandbox    → resolver cualquier resultado incierto simulado
5. recién luego            → evaluación de UNA orden real mínima (decisión manual)
```

Esta guía NO indica habilitar ejecución real. Ese paso es una decisión
manual futura, posterior a completar el checklist.

## Variables requeridas (sin valores reales)

Ambiente (elegir uno; sandbox y real nunca comparten credenciales):

```
BROKER_MODE=mock | sandbox | real
IOL_REAL_API_BASE=https://api.invertironline.com   # único host permitido en real
IOL_REAL_USERNAME= / IOL_REAL_PASSWORD=            # (o los legacy IOL_USERNAME/IOL_PASSWORD)
IOL_SANDBOX_API_BASE=                              # obtener de IOL; vacío = sandbox bloqueado
IOL_SANDBOX_USERNAME= / IOL_SANDBOX_PASSWORD=
SANDBOX_EXECUTION_ENABLED=false                    # lock propio del sandbox
```

> `IOL_USE_SANDBOX` está deprecado: si vale true se mapea a
> `BROKER_MODE=sandbox` (nunca queda sin efecto). Preferir el modo explícito.
> La URL del sandbox oficial de IOL NO está documentada en este repo — no se
> adivina: obtenerla de IOL y configurarla; vacía, todo falla cerrado.

Autorización de ejecución (todo fail-closed):

```
EXECUTION_ADMIN_KEY=            # header X-Execution-Key
EXECUTION_PREVIEW_SECRET=       # firma HMAC del preview
EXECUTION_PREVIEW_TTL_SECONDS=300
EXECUTION_MAX_RECOMMENDATION_AGE_MINUTES=60
EXECUTION_MAX_ORDER_VALUE=0     # 0 = bloqueado
EXECUTION_MAX_TOTAL_VALUE=0
EXECUTION_MAX_PORTFOLIO_PCT=0
```

Política de orden IOL (vacío/0 = bloqueado; sin defaults ocultos):

```
IOL_ORDER_MARKET=               # ej: bCBA (único mercado conocido por el proyecto)
IOL_ORDER_SETTLEMENT=           # t0 | t1 | t2
IOL_ORDER_TYPE=precioLimite     # único tipo soportado
IOL_ORDER_VALIDITY_MINUTES=10
EXECUTION_MAX_QUOTE_AGE_SECONDS=15
EXECUTION_MAX_PRICE_DEVIATION_PCT=0   # 0 = bloqueado
```

Alcance de ejecución (allowlist por instrumento, fail closed):

```
EXECUTION_SELL_ONLY=true
EXECUTION_INSTRUMENT_POLICIES={}      # JSON; vacío = nada operable
EXECUTION_REQUIRE_LIVE_POSITION_CHECK=true
```

`EXECUTION_INSTRUMENT_POLICIES` es un **JSON** que funciona como
**allowlist**: un símbolo que aparece en una recomendación NO es operable
hasta tener su política explícita. Estructura por símbolo (todos los campos
obligatorios):

```
{"<SIMBOLO>": {
  "asset_type": "...", "instrument_type": "...", "currency": "...",
  "market": "<igual a IOL_ORDER_MARKET>",
  "settlement": "<igual a IOL_ORDER_SETTLEMENT>",
  "quantity_step": <>0, "price_tick": <>0,
  "max_quantity": <>0, "max_notional": <>0
}}
```

`price_tick` es la **alteración mínima** del instrumento. El precio límite
debe ser múltiplo exacto de ese valor: si no lo es, la orden se bloquea con
`price_tick_mismatch` **antes de enviarse** y el precio nunca se redondea
para encajar (eso cambiaría silenciosamente el límite revisado). Es
obligatorio: sin tick conocido no se puede garantizar un precio aceptable.

`market`/`settlement` deben coincidir **exactamente** con la política global
(`IOL_ORDER_MARKET` / `IOL_ORDER_SETTLEMENT`); si difieren se bloquea con
`instrument_market_mismatch` / `instrument_settlement_mismatch` — nunca se
elige uno en silencio. La identidad de la política debe coincidir además con
la posición del snapshot y con la posición **real** verificada antes de
enviar. Campos desconocidos invalidan la política (un typo no puede cambiar
la semántica).

Al inicio **solo se habilitarán ventas** de símbolos expresamente
autorizados: con `EXECUTION_SELL_ONLY=true` toda compra queda bloqueada con
`buy_execution_disabled` (no se convierte ni se ignora). Este repo no trae
ninguna política activa ni símbolo autorizado por defecto.

Guard de posición real: antes de cotizar, antes de marcar `submitting` y
antes de cualquier POST, se relee la cartera (read-only, contrato público
del broker) y se verifica que la posición exista, mantenga la identidad
firmada y alcance la cantidad. Solo puede **bloquear**: nunca reduce ni
aumenta la cantidad firmada. Sus fallos son definitivos previos al broker
(`failed`, `quantity_sent=null`, sin POST), nunca conciliación incierta.

## Invariantes V1 (no configurables para sandbox/real)

- **Live position check obligatorio**: `EXECUTION_REQUIRE_LIVE_POSITION_CHECK`
  en `false` bloquea preview, readiness y approve (423,
  `live_position_verification_required`) antes de cualquier escritura.
- **Fase estrictamente sell-only**: `EXECUTION_SELL_ONLY` en `false` bloquea
  igual (`sell_only_mode_required`) — todavía no existe guard de saldo/cash
  para compras, así que las compras quedan fuera de alcance por completo.
  Aun con sell-only activo, toda orden `buy` se bloquea con
  `buy_execution_disabled`.
- **Preflight total antes de enviar**: la cartera live se lee **una sola vez
  por lote**; luego se validan todas las posiciones, todas las cotizaciones,
  todos los notionals y límites, y se construyen todos los requests. Recién
  cuando **todas** las órdenes quedan en `execution_ready` empieza la fase
  de envío.
- **Ningún POST si falla una orden del lote**: la orden causante queda
  `failed` con su código específico y las demás `preflight_cancelled`; la
  recomendación queda `execution_failed`, con `quantity_sent=null` en todas.
- **Límites revalidados con la cotización fresca** (`Decimal`, nunca float):
  por orden (`fresh_order_limit_exceeded`), por símbolo
  (`fresh_symbol_notional_limit_exceeded`), por porcentaje de cartera live
  (`fresh_portfolio_pct_limit_exceeded`) y por total del lote
  (`fresh_total_limit_exceeded`). Los límites del snapshot siguen aplicando
  antes, en el preview.
- **Validación numérica estricta**: `NaN`, `Infinity`, negativos, cero donde
  no corresponde y valores no numéricos se rechazan explícitamente
  (`invalid_live_position_quantity`, `invalid_execution_quantity`,
  `invalid_execution_price`, `invalid_portfolio_value`,
  `invalid_execution_notional`).
- **Los estados inciertos no se reintentan**: un timeout o un 2xx sin
  `numeroOperacion` deja la orden en `manual_reconciliation_required`; un
  proceso caído con órdenes en `execution_ready` o `submitting` requiere
  revisión manual — no hay reintento automático en ninguna parte del sistema.
- **Timestamps de cotización**: se rechazan los que no tengan zona horaria,
  los mal formados y los que estén más adelante en el futuro que
  `EXECUTION_QUOTE_CLOCK_SKEW_SECONDS` (0-10, default 2) →
  `quote_timestamp_invalid`. Nunca se asume UTC en silencio.
- **Configuración numérica validada al arranque**: ventanas
  (`EXECUTION_PREVIEW_TTL_SECONDS`, `EXECUTION_MAX_RECOMMENDATION_AGE_MINUTES`,
  `IOL_ORDER_VALIDITY_MINUTES`, `EXECUTION_MAX_QUOTE_AGE_SECONDS`) deben ser
  finitas y > 0; los límites fail-closed admiten 0 (= no configurado) pero
  nunca negativos ni `NaN`/`Infinity`. Una configuración inválida hace fallar
  el arranque en vez de producir comparaciones ambiguas durante una orden.

## Piloto administrativo (solo creación, nunca envío)

`POST /api/execution-pilot/recommendations` crea **una** recomendación de
venta de exactamente 1 BYMA. **No envía órdenes**: no instancia broker, no
pide cotización, no llama `place_order` ni `submit_order_request`.

Requiere `X-API-Key` + `X-Execution-Key` + la frase exacta
`CREAR PILOTO BYMA 1`, y un **doble candado**:

```
EXECUTION_PILOT_CREATION_ENABLED=true
ORDER_EXECUTION_ENABLED=false        # el piloto solo se PREPARA
```

Activar el lock del piloto **no habilita enviar órdenes**. Payload
estrictamente literal (`symbol=BYMA`, `side=sell`, `quantity=1`); cualquier
otro valor se rechaza, sin defaults implícitos. Antes de escribir valida
snapshot, identidad (ACCIONES/ACCIONES/ARS), tenencia ≥ 1, allowlist,
`max_quantity`, `quantity_step` y que no haya otro piloto pendiente.

La cantidad exacta se expresa con `RecommendationAction.quantity_override`
(entero positivo, columna nullable). Solo se honra si
`metadata_json.execution_pilot=true`; en cualquier otra recomendación un
override no nulo invalida la orden (fail-closed). Las recomendaciones
automáticas dejan siempre `quantity_override=NULL` y siguen derivando la
cantidad de `target_change_pct`. El override viaja en `orders_preview`,
`estimated_notional`, el **payload canónico firmado** y `request_audit`:
cambiarlo invalida el `preview_hash`.

El piloto **supersede** las recomendaciones abiertas anteriores dejando
trazabilidad (`superseded_by_execution_pilot`, `superseded_at`); no se borra
historial ni se convierte una recomendación existente en BYMA.

Después, el flujo es el de siempre y sin atajos:
`GET /api/recommendations/{id}/execution-preview` y, solo si se decide
ejecutar, `POST /api/recommendations/{id}/approve` — que sigue bloqueado por
`ORDER_EXECUTION_ENABLED=false`.

## `execution_ready`: estado durable PRE-POST

`execution_ready` significa exactamente: **el preflight quedó persistido y
todavía no se inició ningún POST de orden** (el flujo siempre commitea
`submitting` antes de enviar).

Si el proceso cae con órdenes en ese estado:

- aparecen en la **cola de conciliación**, junto con el resto de las órdenes
  de la misma recomendación (contexto completo del lote);
- la recomendación queda `manual_reconciliation_required` y **nunca** vuelve
  a `pending`, `blocked` ni `approved`;
- la **única** resolución permitida es `confirm_not_sent` →
  `not_sent_confirmed`, con la frase exacta
  `CONCILIAR EJECUCION {id} COMO NO ENVIADA` y `reason_code:
  prepared_but_not_submitted` en la auditoría;
- `confirm_sent`, `confirm_executed` y `confirm_rejected` se rechazan con 409.

**No puede reanudarse ni reenviarse**: su request preparado puede tener
cotización vencida, validez vencida, posición cambiada o límites cambiados.
No existen endpoints de resume/retry/resubmit, ni scheduler de reintentos.
Para volver a operar hay que generar un preview nuevo desde cero.

Verificación rápida: `GET /api/broker/execution-readiness` (con X-API-Key)
muestra ambiente configurado vs efectivo, locks, sell-only, chequeo de
posición, símbolos autorizados, qué falta configurar y los blocking
reasons — sin exponer ningún secreto.

> Ambiente efectivo: `IOL_USE_SANDBOX=true` (deprecado) hace que
> `BROKER_MODE=real` opere como **sandbox**. Todas las decisiones (locks,
> factory, firma del preview) usan el ambiente **efectivo**, no el
> configurado. Las URLs de real y sandbox deben ser HTTPS, sin credenciales
> embebidas, sin query ni fragment, y puerto 443.

## Checklist previo a considerar una orden real

- [ ] CI verde (backend + frontend)
- [ ] Build verde local
- [ ] Smoke sandbox exitoso (`RUN_IOL_SANDBOX_TESTS=true`, solo con URL y
      credenciales sandbox oficiales de IOL)
- [ ] Flujo sandbox completo: preview → approve reforzado → `execution_sent`
- [ ] `request_audit.iol_request` revisado a mano: form-urlencoded verificado
- [ ] `mercado` verificado contra la política configurada
- [ ] `plazo` verificado
- [ ] `validez` verificada (hora ART, mismo día operativo)
- [ ] `cantidad` verificada (entera, igual al preview firmado)
- [ ] `tipoOrden=precioLimite` verificado (nunca precioMercado)
- [ ] `preview_hash` verificado contra el preview revisado
- [ ] Límites (`EXECUTION_MAX_*`) verificados con valores reales prudentes
- [ ] `EXECUTION_INSTRUMENT_POLICIES` con SOLO los símbolos a operar, con
      límites por símbolo prudentes y `market`/`settlement` coincidentes
- [ ] `EXECUTION_SELL_ONLY=true` (las compras siguen fuera de alcance)
- [ ] `EXECUTION_REQUIRE_LIVE_POSITION_CHECK=true`
- [ ] `live_position_check` revisado en el audit de la orden sandbox
- [ ] `EXECUTION_ADMIN_KEY` configurada (y solo en manos del operador)
- [ ] Cola de conciliación vacía (`GET /api/executions/reconciliation-queue`)
- [ ] `GET /api/executions/recent` revisado (sin órdenes colgadas)
- [ ] La primera orden real evaluada es MÍNIMA y aprobada manualmente con
      frase exacta + execution key

Cualquier resultado incierto (timeout, 5xx, 2xx sin `numeroOperacion`)
queda en `manual_reconciliation_required`: se resuelve con la cola de
conciliación y evidencia del panel de IOL — nunca reintentando.
