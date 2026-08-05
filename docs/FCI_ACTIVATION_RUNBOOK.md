# Runbook: activar suscripción y rescate de FCI

> **Un FCI no es un título.** Se opera por `Monto`, no por cantidad a precio
> límite; no tiene libro, ni punta, ni tick, ni lote; y el valor de cuotaparte
> que decide el resultado **no existe** cuando se envía la operación. Por eso
> este runbook no se parece al de títulos, y por eso "enviada" nunca significa
> "ejecutada".

**Estado inicial obligatorio:**

```
ORDER_EXECUTION_ENABLED=false
FCI_SUBSCRIPTION_ENABLED=false
FCI_REDEMPTION_ENABLED=false
FCI_MAX_OPERATION_AMOUNT=0     # 0 = NO CONFIGURADO = bloqueado
FCI_MAX_DAILY_AMOUNT=0
```

Contrato oficial, verificado, idéntico en ambos endpoints:

```
POST /api/v2/operar/suscripcion/fci
POST /api/v2/operar/rescate/fci
Content-Type: application/x-www-form-urlencoded

Simbolo=ADBAICA&Monto=145.54&soloValidar=true
```

**El rescate también se expresa por `Monto`.** No hay campo oficial para
cuotapartes y no se envía ninguno. Corolario aceptado: no existe forma de
pedir "rescatá todo".

---

## 1. Catálogo read-only

```
POST /api/funds/catalog/refresh
Headers: X-API-Key, X-Execution-Key
```

Lee `GET /api/v2/Titulos/FCI`. **No suscribe ni rescata nada.**

Cada fondo queda en `candidate`, no en `verified`. Los nombres de campo del
catálogo (`horarioCorte`, `montoMinimo`, `plazoLiquidacion`) son **observados**,
no documentados: leerlos hace visible un valor, no lo hace confiable para
temporizar o dimensionar plata real. Se guardan con procedencia
`iol_fci_catalog_observed`, que deliberadamente **no** verifica.

Revisá el estado de un fondo:

```
GET /api/funds/catalog/{symbol}
```

Devuelve `subscription_blockers` y `redemption_blockers` — listas separadas,
porque son capacidades separadas.

## 2. Verificación administrativa

```
POST /api/funds/catalog/{symbol}/verify
Headers: X-API-Key, X-Execution-Key
{
  "cutoff_local_time": "15:00",
  "minimum_amount": 1000.0,
  "settlement_delay_days": 1,
  "currency": "ARS",
  "subscription_supported": true,
  "redemption_supported": false,
  "note": "Verificado contra el prospecto oficial del fondo, sección 4.2.",
  "source": "fund_prospectus"
}
```

**Todos los parámetros son obligatorios y todos son afirmaciones tuyas.** Nada
se hereda del valor observado: el sentido del endpoint es que alguien leyó la
documentación, y reutilizar en silencio una lectura no verificada anularía el
ejercicio.

Qué hace y qué **no** hace:

| Hace | No hace |
|---|---|
| Cambia `verification_status` a `verified` | Llamar a IOL |
| Escribe una fila de auditoría con `data_hash` | Encender ningún flag |
| Marca procedencia `admin_verified_override` | Crear una FundOperation |
| Habilita **sólo** las capacidades que declaraste | Ejecutar nada |

El cutoff se valida como `HH:MM` 24h, el mínimo debe ser `> 0`, el plazo `>= 0`
y la moneda debe coincidir con la del catálogo (si no coincide, estás
describiendo otro fondo → `fund_currency_mismatch`).

Para revertir:

```
POST /api/funds/catalog/{symbol}/demote   # vuelve a candidate
POST /api/funds/catalog/{symbol}/reject   # lo rechaza; un refresh no lo apela
```

Ambos requieren `note` y apagan las dos capacidades: ni un rechazo ni una
degradación dejan un fondo medio operable.

**Cambio de identidad.** Si un refresh automático encuentra que cambió el
símbolo, la moneda o la administradora, el fondo se **congela** en
`identity_changed` y la verificación **no** se hereda: una aprobación dada
para el fondo A no autoriza plata al fondo B. Mientras la identidad no cambia,
un refresh **preserva** la verificación administrativa aunque el catálogo
traiga otro `horarioCorte` observado.

## 3. Límites

```
FCI_MAX_OPERATION_AMOUNT=<monto máximo por operación>
FCI_MAX_DAILY_AMOUNT=<monto máximo por día y por moneda>
FCI_MIN_CASH_RESERVE=<efectivo que nunca se toca>
FCI_FEE_BUFFER_PCT=<colchón de comisiones, p.ej. 0.01>
FCI_VALIDATION_TTL_SECONDS=120
```

`0` significa **no configurado** = bloqueado (`fci_limits_not_configured`),
nunca "sin límite".

El ledger diario usa claves **explícitas y separadas**: `FCI_SUBSCRIBE` y
`FCI_REDEEM`, por fecha y por moneda. Un día de rescates no habilita una
suscripción, y ninguno de los dos comparte cupo con los títulos.

## 4. Preparación y `soloValidar`

```
POST /api/funds/operations
     {"fund_symbol": "ADBAICA", "operation": "subscribe", "amount": 145.54}
```

Sólo acepta fondos `verified`. Un `candidate` se rechaza con
`fund_not_verified`: estar en el catálogo no es estar en condiciones de operar.

```
POST /api/funds/operations/{id}/validate
Headers: X-Execution-Key
```

Envía `soloValidar=true` — **un solo POST**, sin reintento. Requiere el candado
global abierto **y** el flag de la capacidad: validar un rescate que no podemos
enviar es una llamada autenticada sobre una operación que no puede ocurrir.

Una validación **nunca** es una ejecución: no fija `numeroOperacion`, no mueve
la operación a un estado enviado, no consume presupuesto y no crea decisión.

Queda vinculada a **exactamente lo que validaste**: `validated_at` (TTL 120 s)
y `validated_payload_hash`. Cambiar el monto la invalida
(`fci_validation_payload_changed`); dejarla envejecer también
(`fci_validation_expired`).

## 5. Preview

```
GET /api/funds/operations/{id}/preview
```

Firmado (HMAC), con TTL, `would_execute=false`. Contiene símbolo,
administradora, moneda, operación, monto, mínimo, **cutoff del fondo**, plazo,
frescura de la validación y `blocking_reasons`.

`limit_price`, `best_bid`, `best_ask` y `quantity_step` son `None` **por
construcción**: un fondo no tiene ninguno.

## 6. Aprobación

Abrí el candado sólo ahora, y sólo la capacidad que vas a usar:

```
ORDER_EXECUTION_ENABLED=true
FCI_SUBSCRIPTION_ENABLED=true    # o FCI_REDEMPTION_ENABLED
```

Son **independientes**: poder poner plata en un fondo no dice nada sobre poder
sacarla.

## 7. El POST único

```
POST /api/funds/operations/{id}/submit
Headers: X-API-Key, X-Execution-Key
{
  "preview_hash": "<del paso 5>",
  "preview_generated_at": "<del paso 5>",
  "confirmation_text": "EJECUTAR OPERACION FCI {id}",
  "note": "aprobado por el usuario"
}
```

Orden interna, que importa:

1. validaciones puras (instrumento, estado, monto, cutoff, mínimo, flags,
   validación fresca, preview firmado, credencial, frase);
2. consultas **vivas** read-only (saldo para suscribir, tenencia para
   rescatar, pendientes por tipo);
3. **claim atómico + reserva del cupo diario, en la misma transacción**;
4. **un** POST.

El orden es el punto: la reserva ocurre **después** de las consultas vivas, así
que una operación rechazada por falta de saldo o de tenencia **no consume cupo
del día**. Y el claim y la reserva viven o mueren juntos: un claim sin reserva
dejaría la operación en `submitting` para siempre.

Resultados posibles:

| Resultado | `http_requests_sent` | Estado | Cupo |
|---|---|---|---|
| Enviada | 1 | `pending_confirmation` | consumido |
| Rechazada por IOL | 1 | `rejected` | consumido (liberar es administrativo) |
| Ambiguo (timeout, 5xx, 2xx sin `numeroOperacion`) | 1 | `submission_unknown` | **se conserva** |
| Falla local antes del POST | **0** | vuelve a `validated` | **se libera** |

La última fila es la única liberación automática, y es segura precisamente
porque no hay ninguna request en IOL contra la cual conciliar.

## 8. `pending_confirmation`

**No existe el estado `executed`.** El fondo confirma después del cutoff, a un
valor de cuotaparte que no existía al enviar. Hasta entonces la operación
reserva capacidad: `pending_fund_subscriptions` descuenta del efectivo
disponible, `pending_fund_redemptions` de la tenencia rescatable de **ese**
fondo. Una suscripción pendiente no achica una tenencia; un rescate pendiente
no achica el efectivo.

Cerrá el candado ya:

```
ORDER_EXECUTION_ENABLED=false
```

## 9. Conciliación

```
GET  /api/funds/operations/{id}
POST /api/funds/operations/{id}/reconcile
     {"outcome": "confirmed", "note": "...", "broker_operation_id": "..."}
```

`outcome` ∈ `confirmed` | `rejected` | `cancelled` | `reconciliation_required`.

Esta es la **única** salida de `submission_unknown`. No hay reenvío y no debe
haberlo: una operación de FCI no es idempotente y una suscripción duplicada es
plata real. Un reintento humano sobre una operación ya enviada devuelve 409.

Antes de conciliar, **mirá el panel de IOL**. Conciliar es declarar qué pasó,
no adivinarlo.

---

## Antes de habilitar nada en producción

1. Ensayar en **sandbox** (`RUN_IOL_SANDBOX_TESTS`), con `soloValidar=true`
   primero y credenciales exclusivamente de sandbox.
2. Confirmar contra una respuesta real cómo llega `numeroOperacion` y qué
   devuelve una validación rechazada.
3. Verificar administrativamente el fondo (paso 2).
4. Configurar los límites (paso 3).
5. Recién entonces evaluar encender los flags.

El contrato de request proviene de la documentación oficial de "Mi Cuenta"
provista por el responsable del repositorio. **No fue ejercitado contra la API
real desde el entorno de desarrollo**, que tiene `invertironline.com` bloqueado
en el gateway.
