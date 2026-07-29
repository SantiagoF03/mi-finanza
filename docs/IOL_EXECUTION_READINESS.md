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
- Cotización ejecutable: mejor bid para venta / mejor ask para compra —
  `ultimoPrecio` nunca se usa para ejecutar; sin punta no hay orden
- Semántica de envío: *at-most-once automatic submission + manual
  reconciliation on uncertain outcome* (timeouts, 5xx o 2xx sin
  `numeroOperacion` terminan en conciliación manual, jamás en reintento)

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
  "quantity_step": <>0, "max_quantity": <>0, "max_notional": <>0
}}
```

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
