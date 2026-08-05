# Runbook: activar ejecución de títulos (ACCIONES y CEDEARS)

> **Este runbook no se ejecuta solo.** Cada paso es una decisión humana. El
> único paso que envía una orden real es el 10, y está rodeado a propósito por
> dos pasos de candado: el 9 lo abre y el 11 lo cierra.

**Estado inicial obligatorio** — así está producción hoy y así vuelve al
terminar:

```
ORDER_EXECUTION_ENABLED=false
SECURITIES_BUY_ENABLED=false
SECURITIES_SELL_ENABLED=false
EXECUTION_PILOT_CREATION_ENABLED=false
EXECUTION_SELL_ONLY=true
EXECUTION_REQUIRE_LIVE_POSITION_CHECK=true
EXECUTION_REQUIRE_LIVE_CASH_CHECK=true
```

Nada de lo que sigue incluye credenciales reales. Donde dice `<...>`, va un
valor que **no** se escribe en el repositorio.

---

## 1. Resolución read-only del instrumento

```
POST /api/broker/instruments/resolve
     {"symbols": ["BYMA"]}
```

Lee identidad desde endpoints oficiales de títulos y cotizaciones. **No envía
órdenes.** Sólo resuelve símbolos que ya están en una fuente acotada
(recomendación abierta, watchlist, universo habilitado, posición): un símbolo
nunca es operable sólo porque alguien lo escribió.

El resultado es `candidate`, nunca `verified`. Eso es correcto: haber leído un
campo lo hace visible, no confiable.

## 2. Revisión del catálogo

```
GET /api/broker/instrument-capabilities
GET /api/broker/pilot-readiness?symbols=BYMA
```

`pilot-readiness` responde la única pregunta que importa acá: **¿podría** esta
app mandar una orden de este lado, sin chocar con un bloqueo? Mirá:

- `buy.blocking_reasons` y `sell.blocking_reasons` — son listas separadas
  porque son capacidades separadas;
- `manual_configuration_required` — lo que falta que decida un humano;
- `suggested_pilot_max_quantity` / `_notional` — **topes técnicos**, no
  objetivos de inversión.

Los bloqueos típicos en este punto:

| Código | Qué falta |
|---|---|
| `instrument_not_verified` | tick y/o step sin verificación administrativa |
| `class_policy_not_configured` | falta la política de la clase (paso 3) |
| `quote_unavailable` | no hay punta de ese lado del libro |
| `quote_wrong_side` | el proveedor contestó con la otra punta |

Para tick y step:

```
POST /api/broker/instruments/{symbol}/verify-fields
```

**Verificalos contra la documentación real.** Completar un tick "que parece
razonable" es afirmarlo; una vez en la config, un número inventado es
indistinguible de uno verificado.

## 3. Plantilla de políticas

```
GET /api/broker/pilot-policy-template?symbols=BYMA,SPY
```

Devuelve un JSON **para revisar**. No escribe nada — ni base, ni entorno, ni
Railway.

Reglas que ya vienen aplicadas:

- `buy_enabled` y `sell_enabled` en **false**;
- `default_price_tick` y `default_quantity_step` en **null** (un tick por
  clase es una afirmación sobre todos sus instrumentos);
- ACCIONES y CEDEARS en bloques **separados**, aunque los números coincidan;
- moneda, mercado y plazo copiados de lo observado, nunca inventados.

**Todo `null` que completes a mano es una afirmación tuya.** Si no lo
verificaste, dejalo en `null`: el instrumento queda bloqueado, que es el
resultado correcto.

## 4. Configuración en Railway

Cargá a mano, revisadas una por una:

```
EXECUTION_CLASS_POLICIES=<JSON del paso 3, revisado>
EXECUTION_INSTRUMENT_OVERRIDES=<JSON del paso 3, revisado>
EXECUTION_DENYLIST=<símbolos que nunca se operan>
EXECUTION_ADMIN_KEY=<secreto nuevo, no reutilizado>
EXECUTION_PREVIEW_SECRET=<secreto nuevo, no reutilizado>
```

Los dos secretos son **nuevos**: reutilizar uno existente hace que cualquier
sistema que ya lo tenga pueda firmar previews.

Seguí sin tocar `ORDER_EXECUTION_ENABLED`.

## 5. Redeploy con el candado cerrado

Desplegá con `ORDER_EXECUTION_ENABLED=false`. El objetivo del deploy es que la
configuración esté cargada y verificable, **no** que el sistema pueda operar.

Verificá que el estado persistente no cambió:

- Recommendation 12 → `approved`
- Recommendation 13 → `pending`
- OrderExecution 1 → `executed`, `numeroOperacion=183382167`

Las migraciones son aditivas (`ADD COLUMN`, `CREATE TABLE IF NOT EXISTS`) y no
hay ningún `UPDATE` ni `DELETE` en el arranque. Si algo de eso cambió,
**pará acá**.

## 6. Readiness

```
GET /api/broker/execution-readiness
```

Leé, en este orden:

1. `blocking_reasons` — debería contener sólo `execution_locked`;
2. `technically_ready_but_locked` — `true` significa "todo lo técnico está
   hecho y el candado sigue cerrado", que es el estado deseado antes del
   piloto;
3. `next_safe_action` — determinístico, derivado de los bloqueos;
4. `acciones` / `cedears` — por clase. **Una clase no está lista porque un
   símbolo lo esté**: `covered_symbols` y `*_ready` son datos distintos.

`ready_for_real_execution=true` con el candado cerrado es imposible por
construcción. Si lo ves, es un bug.

## 7. Creación del piloto

```
POST /api/execution-pilot/securities
Headers: X-API-Key, X-Execution-Key
{
  "symbol": "BYMA",
  "side": "sell",
  "quantity": 1,
  "confirmation_text": "CREAR PILOTO SELL BYMA 1",
  "note": "piloto controlado de venta"
}
```

Requiere `EXECUTION_PILOT_CREATION_ENABLED=true` y **`ORDER_EXECUTION_ENABLED`
sigue en false**: el piloto se *prepara* mientras el envío está bloqueado.

La frase nombra símbolo, lado y cantidad, así que **la frase de un piloto no
autoriza otro**. Cuatro pilotos independientes:

| Piloto | side | Frase |
|---|---|---|
| Acción argentina venta | `sell` | `CREAR PILOTO SELL BYMA 1` |
| Acción argentina compra | `buy` | `CREAR PILOTO BUY BYMA 1` |
| CEDEAR venta | `sell` | `CREAR PILOTO SELL SPY 1` |
| CEDEAR compra | `buy` | `CREAR PILOTO BUY SPY 1` |

Crea una Recommendation **nueva** en `pending`, marcada
`metadata_json.execution_pilot=true`. Nunca reutiliza ni reemplaza una
recomendación existente: si hay una decisión pendiente, la creación se
**bloquea** en vez de superponerse.

> El camino legacy de BYMA (`POST /api/execution-pilot/recommendations`,
> frase `CREAR PILOTO BYMA 1`) sigue existiendo sin cambios. Ya ejecutó una
> venta real de punta a punta; migrarlo no es gratis y no se migró.

## 8. Preview

```
GET /api/recommendations/{id}/execution-preview
```

Leé el preview completo antes de decidir. Contiene precio límite, cantidad,
notional, antigüedad de la cotización, límites aplicados y `blocking_reasons`.
Está firmado (HMAC) y tiene TTL.

**Este paso no envía nada.** Si el preview no dice exactamente lo que esperás,
volvé al paso 2.

## 9. Apertura temporal del candado

```
ORDER_EXECUTION_ENABLED=true
SECURITIES_SELL_ENABLED=true     # o SECURITIES_BUY_ENABLED, según el piloto
EXECUTION_SELL_ONLY=false        # sólo al migrar fuera del camino legacy
```

Redeploy. **A partir de acá el sistema puede enviar órdenes reales.**

Dejalo abierto el menor tiempo posible. Tené el paso 11 listo antes de
ejecutar el 10.

## 10. Aprobación

```
POST /api/recommendations/{id}/approve
Headers: X-API-Key, X-Execution-Key
{
  "preview_hash": "<del paso 8>",
  "preview_generated_at": "<del paso 8>",
  "confirmation_text": "<frase exacta que indica el preview>",
  "note": "piloto controlado"
}
```

Esto **envía una orden real**. Exactamente una: el punto de no retorno reclama
la ejecución atómicamente y ninguna rama repite el envío.

Un timeout deja la ejecución en `submission_unknown` y **no se reintenta**.
Reenviar una orden que no podemos probar que falló es cómo se compra dos veces.

## 11. Cierre inmediato del candado

```
ORDER_EXECUTION_ENABLED=false
```

Redeploy. **Sin esperar la confirmación de la orden.** El candado protege
contra el próximo envío, no contra el que ya salió; dejarlo abierto "hasta
confirmar" es dejarlo abierto sin necesidad.

## 12. Conciliación

```
GET  /api/executions/recent
GET  /api/executions/reconciliation-queue
POST /api/executions/{id}/reconcile
```

Comparado contra el panel de IOL:

- si la orden se ejecutó → registrá `numeroOperacion` y el precio real;
- si quedó `submission_unknown` → **mirá IOL primero**, después conciliá. No
  hay botón de reenviar y no debe haberlo;
- una cancelación **no libera** presupuesto diario automáticamente: liberar es
  un acto administrativo con motivo registrado.

Verificá al cierre que los flags volvieron al estado inicial de este
documento.
