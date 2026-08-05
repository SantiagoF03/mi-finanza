# FCI: contrato oficial y estado de implementación

> **Historial de correcciones.**
> - **PR #138** afirmaba que *no existe contrato oficial de FCI*. Falso: la
>   documentación oficial publica suscripción, rescate y catálogo.
> - **PR #139** corrigió eso pero dejó `FCI_REQUEST_CONTRACT_VERIFIED = False`
>   y `FCI_REQUEST_FIELDS = {}`, con lo cual la familia FCI quedaba
>   estructuralmente completa pero **imposible de enviar**.
> - **Este PR** completa el contrato con los nombres de campo exactos de la
>   documentación oficial de "Mi Cuenta". FCI sigue **apagado en producción**:
>   ahora por decisión de flags, no por contrato faltante.

## Endpoints oficiales

```
GET    /api/v2/Titulos/FCI                    catálogo de fondos
GET    /api/v2/Titulos/FCI/{simbolo}          detalle de un fondo
POST   /api/v2/operar/suscripcion/fci         suscripción
POST   /api/v2/operar/rescate/fci             rescate
DELETE /api/v2/operaciones/{numeroOperacion}  cancelación (títulos)
```

## El contrato del request — verificado

Ambos endpoints de operación toman **exactamente tres campos**, codificados
como `application/x-www-form-urlencoded`:

| Campo | Ejemplo | Notas |
|---|---|---|
| `Simbolo` | `ADBAICA` | símbolo del fondo, en mayúsculas |
| `Monto` | `145.54` | **monto**, no cuotapartes |
| `soloValidar` | `true` \| `false` | valida sin crear la operación |

```python
# app/services/fci.py
FCI_REQUEST_CONTRACT_VERIFIED = True
FCI_CONTENT_TYPE = "application/x-www-form-urlencoded"
FCI_REQUEST_FIELDS = {
    "subscribe": {"fund_symbol": "Simbolo", "amount": "Monto"},
    "redeem":    {"fund_symbol": "Simbolo", "amount": "Monto"},
}
```

### El rescate también se expresa por `Monto`

Es la parte del contrato que más fácil se implementa mal. **No existe campo
oficial para cuotapartes en el request.** `FundOperation.quotaparts` puede
existir para mostrar, pero `build_fund_request()` **nunca** lo envía: agregar
un campo no documentado a un rescate real es exactamente cómo una orden se
reinterpreta en silencio.

Corolario: **no hay forma de decir "rescatá todas mis cuotapartes"**. Sin un
`Monto` positivo no hay request que construir → `invalid_fund_amount`.

### `Monto` se serializa con Decimal, nunca con `str(float)`

```python
serialize_monto(Decimal("145.54"))  -> "145.54"
serialize_monto(100000)             -> "100000"     # no "1E+5"
```

Un `str(float)` produce `145.54000000000002` o `1e+05` según el valor. Es
plata y además entra en el hash firmado del preview: la misma operación tiene
que producir el mismo string siempre.

## Validación obligatoria y fresca (`soloValidar`)

`soloValidar=true` es el mecanismo oficial para pre-chequear **sin crear** la
operación. En este repo es **obligatorio antes de enviar**, y además tiene que
ser **fresco y del mismo payload**:

- `FundOperation.validated_at` — TTL de `FCI_VALIDATION_TTL_SECONDS` (120 s).
- `FundOperation.validated_payload_hash` — hash del payload exacto validado.

Si el monto, el símbolo o la operación cambian después de validar, el hash
deja de coincidir y hay que revalidar (`fci_validation_payload_changed`). Si
pasa el TTL, `fci_validation_expired`. Una validación de ayer no autoriza un
envío de hoy: entre medio cambian el valor de cuotaparte, el saldo y el cutoff.

Una validación **nunca** es una ejecución: no fija `broker_operation_id`, no
mueve la operación a un estado enviado, no consume presupuesto at-most-once y
no crea `FundOperationDecision`.

## El candado global también tapa a FCI

`fci_execution_locked(settings)` gatea **validación, preview y envío**. Con
`ORDER_EXECUTION_ENABLED=false`, `soloValidar=true` tampoco sale: es una
llamada autenticada al broker hecha en preparación para ejecutar, y con la
ejecución apagada no se hace. Respuesta `423`, código `execution_locked`.

## Familia separada — nunca `OrderExecution`

Un FCI **no es un título**:

- se suscribe/rescata por **monto**, no por cantidad a precio límite;
- no tiene libro de órdenes, ni punta, ni tick, ni lote;
- el valor de cuotaparte que decide el resultado **no existe** al momento del
  envío;
- la confirmación es **asincrónica**, posterior al cutoff del fondo.

Por eso hay modelos propios:

| Modelo | Rol |
|---|---|
| `FundInstrument` | fondo, con su **cutoff propio**, mínimo, plazo, moneda |
| `FundOperation` | una suscripción o rescate, con su máquina de estados |
| `FundOperationDecision` | la decisión humana que la autorizó |

Estados: `prepared` → `validation_requested` → `validated` →
`approval_requested` → `submitting` → `submitted` → `pending_confirmation` →
`confirmed` | `rejected` | `cancelled` | `submission_unknown` |
`reconciliation_required`.

**No existe el estado `executed`.** "Enviada" nunca significa "ejecutada": el
fondo confirma después, a un valor que al enviar no existía.

Un instrumento cuya familia es `fund` es rechazado por el evaluador de alcance
de **títulos** con `fund_requires_fci_contract` — no porque no se soporte,
sino porque debe ir por su propio contrato.

## Endpoints de la app

| Método y ruta | Qué hace |
|---|---|
| `POST /api/funds/operations` | prepara la operación (no llama al broker) |
| `POST /api/funds/operations/{id}/validate` | `soloValidar=true`, un solo POST |
| `GET  /api/funds/operations/{id}/preview` | preview firmado, read-only |
| `POST /api/funds/operations/{id}/submit` | `soloValidar=false`, un solo POST |
| `GET  /api/funds/operations/{id}` | estado, sin secretos |
| `POST /api/funds/operations/{id}/reconcile` | conciliación humana |
| `POST /api/funds/catalog/refresh` | refresca el catálogo de fondos |

## Preflight en vivo

Antes de enviar, `fund_live_preflight()` exige datos **vivos**, no cacheados:

| Operación | Requiere | Falla con |
|---|---|---|
| Suscripción | saldo disponible vivo ≥ monto + buffer + reserva | `insufficient_live_cash`, `live_cash_unavailable` |
| Rescate | tenencia viva del fondo ≥ monto | `live_fund_position_insufficient`, `live_fund_position_unavailable`, `live_fund_position_missing` |

Más límites propios de FCI, **fail-closed**: `FCI_MAX_OPERATION_AMOUNT`,
`FCI_MAX_DAILY_AMOUNT` (contra el pendiente del día en la misma moneda),
`FCI_MIN_CASH_RESERVE`, `FCI_FEE_BUFFER_PCT`. En 0 = **no configurado** =
bloqueado (`fci_limits_not_configured`), nunca "sin límite".

## Preview

Contiene: símbolo, administradora, moneda, operación (`subscribe`/`redeem`),
monto, mínimo, **cutoff del fondo**, plazo, resultado y frescura de la
validación, TTL, HMAC, blocking reasons, `would_execute=false`.

**No contiene** `limit_price`, `best_bid`, `best_ask` ni `quantity_step` — son
`None` por construcción, porque un fondo no tiene ninguno.

## Cutoff por fondo

El cutoff vive en `FundInstrument.cutoff_local_time`, leído del catálogo
oficial. **No hay 15:00 hardcodeado**: aplicar el cutoff de un fondo a otro
desfasa la operación un día de liquidación entero. Un fondo sin cutoff
conocido queda `candidate` y no puede operar (`fund_cutoff_unknown`).

## Catálogo: presencia ≠ verificación

Los nombres de campo del **catálogo** (`horarioCorte`, `montoMinimo`,
`plazoLiquidacion`, …) son **observados**, no documentados. Leerlos hace
visible un valor; no lo hace confiable para temporizar o dimensionar plata
real. Por eso un valor leído de un campo observado se guarda con procedencia
`iol_fci_catalog_observed`, que **no** está en `FUND_VERIFYING_PROVENANCES` y
por lo tanto no alcanza para dejar el fondo `verified`.

Ningún campo se inventa: si no vino, el valor es `None` y el fondo queda
`candidate`.

## Requisitos para ejecutar

Todos, simultáneamente:

1. `ORDER_EXECUTION_ENABLED=true` (candado global);
2. `FCI_SUBSCRIPTION_ENABLED` o `FCI_REDEMPTION_ENABLED` — independientes
   entre sí, default `false`;
3. `FCI_REQUEST_CONTRACT_VERIFIED = True` — **hoy `True`**;
4. credencial administrativa `X-Execution-Key`;
5. validación `soloValidar=true` fresca y del mismo payload;
6. preview firmado y vigente;
7. frase exacta `EJECUTAR OPERACION FCI {id}`;
8. aprobación humana;
9. fondo `verified`;
10. preflight vivo: saldo (suscripción) o tenencia (rescate);
11. límites FCI configurados y respetados;
12. cutoff abierto y día hábil;
13. un solo envío, sin reintento automático.

`ready_for_real_fci_subscription` / `ready_for_real_fci_redemption` requieren
**flag Y contrato verificado**. Un flag solo nunca alcanza.

Ante timeout ambiguo: `submission_unknown`. **No se repite el POST** — una
operación de FCI no es idempotente y una suscripción duplicada es plata real.
Un reintento humano sobre una operación en `submission_unknown` es rechazado;
la salida es conciliar (`/reconcile`), no reenviar.

## Qué falta para habilitarlo

1. Ensayar en **sandbox** (`RUN_IOL_SANDBOX_TESTS`), nunca en producción,
   con `soloValidar=true` primero.
2. Confirmar contra una respuesta real cómo viene `numeroOperacion` y qué
   devuelve una validación rechazada.
3. Configurar los límites FCI (hoy en 0 = bloqueado).
4. Recién entonces evaluar encender los flags.

## Sandbox

Las pruebas externas van detrás de `RUN_IOL_SANDBOX_TESTS`, con credenciales
**exclusivamente de sandbox**. Nunca se reutilizan credenciales reales, nunca
corre en CI normal y nunca enciende flags productivos.
