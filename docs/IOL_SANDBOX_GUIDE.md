# Guía de pruebas en sandbox de IOL

> **Nada de lo que sigue se ejecutó en este trabajo.** No hay credenciales de
> sandbox ni autorización expresa para usarlas. Esta guía describe cómo
> hacerlo cuando ambas cosas existan.

## Requisitos previos

El sandbox usa **exclusivamente** variables propias. Nunca reutiliza
credenciales reales, y la app rechaza estructuralmente un sandbox que apunte
al host real (`sandbox_environment_invalid`).

```
BROKER_MODE=sandbox
IOL_SANDBOX_API_BASE=            # obtener de IOL. Vacío = todo bloqueado.
IOL_SANDBOX_USERNAME=            # credenciales SOLO de sandbox
IOL_SANDBOX_PASSWORD=
SANDBOX_EXECUTION_ENABLED=true   # candado propio del sandbox
```

> La URL oficial del sandbox **no está documentada en este repositorio y no
> se adivina**. `api-sandbox.invertironline.com` no resuelve desde el entorno
> de build. Obtenerla de IOL.

Además, para que algo sea operable hacen falta las capacidades y el catálogo:

```
SECURITIES_BUY_ENABLED=true      # sólo para el ensayo en sandbox
SECURITIES_SELL_ENABLED=true
EXECUTION_SELL_ONLY=false        # deprecado; mientras esté en true bloquea compras
EXECUTION_CLASS_POLICIES={...}   # ver README §Políticas por clase
EXECUTION_ADMIN_KEY=...
EXECUTION_PREVIEW_SECRET=...
SCHEDULER_MARKET_OPEN_TIME=10:30
SCHEDULER_MARKET_CLOSE_TIME=17:00
SCHEDULER_TIMEZONE=America/Argentina/Buenos_Aires
```

Verificación antes de empezar:

```
GET /api/broker/execution-readiness      → ready_for_sandbox_execution: true
GET /api/broker/instrument-capabilities  → buy_ready / sell_ready por símbolo
```

## Secuencia de pruebas

Cada paso se revisa a mano antes de pasar al siguiente. Ninguno se saltea.

### 1. Fondeo simulado

Confirmar que la cuenta sandbox tiene saldo en la moneda correcta.

```
GET /api/broker/ping                     → autenticación sandbox OK
GET /api/broker/execution-readiness      → real_configured=false, sandbox_configured=true
```

Verificar que `get_live_cash("ARS")` devuelve `available=true` con un importe
razonable. **Si devuelve `available=false`, toda compra queda bloqueada con
`live_cash_unavailable`** — eso es correcto, no un bug.

### 2. Refrescar el catálogo de ejecución

```
POST /api/broker/instrument-catalog/refresh
     X-API-Key + X-Execution-Key
```

Es read-only contra IOL: lee la cartera y escribe identidad. Revisar que cada
instrumento tenga `market`, `settlement`, `currency`, `price_tick` y
`quantity_step`. Los que no, aparecen como `instrument_catalog_incomplete` en
`/api/broker/instrument-capabilities` — completarlos con
`EXECUTION_INSTRUMENT_TICKS` antes de seguir.

### 3. Compra simulada de una acción argentina

1. `GET /api/recommendations/{id}/execution-preview`
2. Revisar **a mano**: clase, moneda, mercado, plazo, cantidad, precio
   límite, monto, buffer de costos, límites, vencimiento del preview.
3. `POST /api/recommendations/{id}/approve` con `X-Execution-Key`,
   `preview_hash`, `preview_generated_at` y la frase exacta.
4. Revisar `request_audit.iol_request`: `mercado`, `simbolo`, `cantidad`,
   `precio`, `plazo`, `validez`, `tipoOrden=precioLimite`.
5. Revisar `request_audit.live_cash_check`: saldo vivo, buffer, reserva,
   compras pendientes, total necesario.

### 4. Venta simulada de la misma acción

Igual que §3, verificando además `request_audit.live_position_check`:
cantidad disponible ≥ cantidad requerida, identidad coincidente.

### 5. Compra simulada de un CEDEAR

Igual que §3. Verificar específicamente:

- `instrument_type = CEDEAR` (no el subyacente extranjero);
- moneda `ARS`;
- que el `quantity_step` usado sea el del catálogo y **no** el ratio de
  conversión del CEDEAR.

### 6. Venta simulada del CEDEAR

Igual que §4.

### 7. Cancelación

La API de IOL **no tiene contrato de cancelación verificado** en este
repositorio. Lo que se prueba acá es el camino manual:

1. `GET /api/executions/reconciliation-queue`
2. `POST /api/executions/{id}/refresh-broker-status` (read-only; **no** cambia
   el estado automáticamente)
3. Cancelar en el panel de IOL
4. `POST /api/executions/{id}/reconcile` con `confirm_rejected` o
   `confirm_not_sent` según lo que muestre IOL, frase exacta y nota

### 8. Concertación

Con una orden que sí se ejecutó en sandbox:

1. `POST /api/executions/{id}/refresh-broker-status` → revisar `estadoActual`
2. `POST /api/executions/{id}/reconcile` con `confirm_executed`,
   `broker_order_id`, `executed_quantity`, `executed_price`

### 9. Conciliación de un resultado incierto

Forzar (o esperar) un timeout. Verificar que:

- la orden queda en `manual_reconciliation_required`;
- **no** hubo reintento automático;
- un segundo `approve` responde `409 already_executed`;
- la resolución es exclusivamente manual, con evidencia del panel de IOL.

## Ejecutar el smoke test armado

```bash
cd backend
RUN_IOL_SANDBOX_TESTS=true \
IOL_SANDBOX_SMOKE_SYMBOL=... \
IOL_SANDBOX_SMOKE_QUANTITY=1 \
pytest tests/test_iol_sandbox_smoke.py -v
```

Sin `RUN_IOL_SANDBOX_TESTS=true` se saltea y **no toca la red**. Es el único
test del repositorio autorizado a hacer una llamada externa, y falla cerrado
si algo huele a producción.

## Después del sandbox

Volver **todo** a su estado seguro antes de considerar cualquier orden real:

```
SECURITIES_BUY_ENABLED=false
SECURITIES_SELL_ENABLED=false
SANDBOX_EXECUTION_ENABLED=false
ORDER_EXECUTION_ENABLED=false
```

Una orden real es una decisión manual posterior, con su propio checklist
(ver `docs/IOL_EXECUTION_READINESS.md`).
