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
GET /api/broker/pilot-readiness?symbols=GGAL
```

> **No uses BYMA como candidato del piloto nuevo.** BYMA tiene una política
> legacy por símbolo que atajea el camino por clase, así que un piloto verde
> en BYMA demuestra que el puente legacy funciona, no que ACCIONES funciona.
> El candidato sale de la readiness técnica, no de una lista fija.

`pilot-readiness` separa **dos preguntas distintas**:

| Campo | Qué responde |
|---|---|
| `technically_ready` / `technical_blocking_reasons` | ¿el instrumento está bien descripto, cotizado y dimensionado? |
| `activation_ready` / `activation_blocking_reasons` | ¿tenemos permiso para enviar? |

Con todos los candados cerrados —que es el estado correcto en este paso— lo
esperado es:

```json
{"technically_ready": true, "activation_ready": false,
 "activation_blocking_reasons": ["execution_locked", "sell_execution_disabled"]}
```

Si las dos preguntas fueran una sola, este estado diría "no listo" y no
podrías distinguir un tick faltante de un candado cerrado.

También mirá `manual_configuration_required` (lo que falta que decida un
humano) y `suggested_pilot_max_quantity` / `minimum_valid_quantity` — **topes
técnicos**, no objetivos de inversión.

Bloqueos técnicos típicos en este punto:

| Código | Qué falta |
|---|---|
| `instrument_identity_not_verified` | identidad o mecánica sin procedencia confiable |
| `instrument_buy_not_verified` / `instrument_sell_not_verified` | esa capacidad, específicamente |
| `class_policy_not_configured` | falta la política de la clase (paso 3) |
| `quote_unavailable` | no hay punta de ese lado del libro |
| `quote_wrong_side` | el proveedor contestó con la otra punta |
| `live_check_not_performed` | pediste `live=false`: no se miró saldo ni tenencia |

Para tick y step:

```
POST /api/broker/instruments/{symbol}/verify-fields
```

**Verificalos contra la documentación real.** Completar un tick "que parece
razonable" es afirmarlo; una vez en la config, un número inventado es
indistinguible de uno verificado.

### Comprobación viva de la cantidad exacta

```
GET /api/broker/pilot-readiness?live=true&symbols=GGAL&side=buy&quantity=1
Headers: X-Execution-Key
```

`live=true` es **otra cosa** que el modo por defecto: consulta el libro del
lado pedido y comprueba **saldo vivo** (compra) o **tenencia viva** (venta)
para esa cantidad exacta. Cuesta una llamada real por símbolo, así que exige
la credencial, símbolos explícitos (máximo 10), `side` y `quantity`. Nunca
recorre el catálogo entero.

Devuelve `exact_notional` = `quantity × ask` (compra) o `× bid` (venta), más
`live_cash_check` o `live_position_check`. Compra y venta se valúan cada una
con **su** punta; nunca al revés.

`live=false` **no afirma** que haya saldo o tenencia disponible, porque no
miró. Eso se reporta como `live_check_not_performed`, no como "todo bien".

## 3. Plantilla de políticas

```
GET /api/broker/pilot-policy-template?symbols=BYMA,SPY
```

Devuelve un JSON **para revisar**. No escribe nada — ni base, ni entorno, ni
Railway.

**La plantilla ahora carga sin errores.** La versión anterior emitía
`default_price_tick: null` contra un esquema que exigía un número positivo, y
`price_tick`/`quantity_step` dentro de un override que no admite esos campos —
así que pegarla en producción daba `class_policy_invalid` y te enterabas
después. Hay un test que la genera, la carga con `load_class_policies` y
`load_instrument_overrides`, y exige cero errores.

Reglas que ya vienen aplicadas:

- `buy_enabled` y `sell_enabled` en **false**;
- `default_price_tick` y `default_quantity_step` en **null** — ahora un valor
  válido y explícito: la clase no afirma nada sobre tick ni step. No es una
  puerta trasera: ese fallback lleva procedencia `class_policy_default`, que
  **no verifica**, así que el instrumento sigue bloqueado. El catálogo
  verificado sigue siendo la autoridad;
- **tick y step NO van en overrides** — van en la sección aparte
  `INSTRUMENT_FIELD_VERIFICATION_PAYLOADS`, que **no es una variable de
  Railway**: es el payload para `verify-fields`;
- `max_quantity` de la clase nunca queda por debajo del lote mínimo de sus
  miembros (un límite de 1 sobre un instrumento que opera de a 100 rechaza
  todas sus órdenes);
- si el lote mínimo vale más que el límite del piloto, se reporta
  `pilot_limit_below_minimum_lot` y **no se amplía el límite solo**;
- los overrides sólo **endurecen**;
- ACCIONES y CEDEARS en bloques **separados**, aunque los números coincidan;
- moneda, mercado y plazo copiados de lo observado, con la grafía canónica que
  el validador espera (`bCBA`, no `bcba`).

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
3. `next_safe_actions` — **una por capacidad**:

   ```json
   {"acciones": "ready_for_controlled_acciones_pilot",
    "cedears": "configure_class_policy_cedears",
    "fci_subscription": "configure_fci_limits",
    "fci_redemption": "configure_fci_limits"}
   ```

   Las capacidades no dependen entre sí, así que sus instrucciones tampoco:
   que falten los límites de FCI **no** bloquea el camino de ACCIONES. Antes
   una sola cadena cubría todo y mandaba a configurar FCI a alguien que estaba
   trabajando en acciones;
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
sigue en false**: el piloto se *prepara* mientras el envío está bloqueado. Eso
es deliberado — exigir el candado abierto para preparar haría el sistema
imposible de preparar.

Lo que sí exige es `technically_ready=true` **para ese lado y esa cantidad
exacta**, comprobado en vivo. No se crea un piloto que ya sabemos que no tiene
saldo (compra) o no tiene tenencia (venta).

El orden interno importa: credencial → flag de creación → payload → catálogo →
chequeos estáticos → **recién entonces** un broker. Un payload inválido o una
credencial equivocada no instancian broker ni tocan la red.

### Antes del primer piloto: Recommendation 13

El gate central impide dos recomendaciones abiertas a la vez, y **Recommendation
13 sigue `pending`**. Mientras esté abierta, la creación de un piloto devuelve
409 en vez de superponerse.

**Esa decisión es tuya y hay que tomarla explícitamente** — aprobarla o
rechazarla desde la UI. Este trabajo no la resuelve, y no debería: una
recomendación pendiente es una decisión humana esperando, no un obstáculo
técnico que el código deba apartar.

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
