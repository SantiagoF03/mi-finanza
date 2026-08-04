# FCI: contrato oficial y estado de implementación

> **Corrección respecto del PR #138.** Ese PR afirmaba que *no existe contrato
> oficial de FCI*. **Esa afirmación era incorrecta y queda eliminada.** La
> documentación oficial de IOL publica endpoints de suscripción, rescate y
> catálogo de FCI. Lo que sigue describe qué está implementado, qué falta
> verificar y por qué FCI sigue apagado en producción.

## Endpoints oficiales

```
GET  /api/v2/Titulos/FCI                  catálogo de fondos
GET  /api/v2/Titulos/FCI/{simbolo}        detalle de un fondo
POST /api/v2/operar/suscripcion/fci       suscripción
POST /api/v2/operar/rescate/fci           rescate
```

La documentación describe además el mecanismo **`soloValidar`**: el mismo
endpoint, invocado para validar **sin crear** la operación.

## Estado de verificación

| Elemento | Estado |
|---|---|
| Rutas de los endpoints | ✅ documentadas |
| Mecanismo `soloValidar` | ✅ documentado |
| **Nombres exactos de los campos del request** | ❌ **no verificados en este repo** |

> **Limitación del entorno de build.** La política de red de este entorno
> **bloquea `invertironline.com` en el gateway** (`403` al CONNECT para
> `developers.invertironline.com`, `api.invertironline.com` y
> `www.invertironline.com`). No fue posible abrir la documentación oficial
> desde acá para transcribir los nombres de campo.

Consecuencia, implementada y testeada:

```python
FCI_REQUEST_CONTRACT_VERIFIED = False   # app/services/fci.py
FCI_REQUEST_FIELDS = {}
```

`build_fund_request()` **falla cerrado** con `fci_request_contract_unverified`
mientras esa constante sea `False`. **No se inventan nombres de campo**: un
mapeo adivinado produce una suscripción real malformada, que es peor que no
enviar nada.

Todo lo que **no** depende de esos nombres está implementado y cubierto por
tests: catálogo, cutoff por fondo, validación, preview firmado, aprobación
humana, máquina de estados, at-most-once y conciliación.

## Familia separada — nunca `OrderExecution`

Un FCI **no es un título**:

- se suscribe/rescata por **monto** (o cuotapartes), no por cantidad a precio
  límite;
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

## Preview

Contiene: símbolo, administradora, moneda, operación (`subscribe`/`redeem`),
monto o cuotapartes, mínimo, **cutoff del fondo**, plazo, resultado de
validación, TTL, HMAC, blocking reasons, `would_execute=false`.

**No contiene** `limit_price`, `best_bid`, `best_ask` ni `quantity_step` — son
`None` por construcción, porque un fondo no tiene ninguno.

## Cutoff por fondo

El cutoff vive en `FundInstrument.cutoff_local_time`, leído del catálogo
oficial. **No hay 15:00 hardcodeado**: aplicar el cutoff de un fondo a otro
desfasa la operación un día de liquidación entero. Un fondo sin cutoff
conocido queda `candidate` y no puede operar (`fund_cutoff_unknown`).

## Requisitos para ejecutar

Todos, simultáneamente:

1. `FCI_SUBSCRIPTION_ENABLED` o `FCI_REDEMPTION_ENABLED` (default `false`);
2. `FCI_REQUEST_CONTRACT_VERIFIED = True` — **hoy `False`**;
3. credencial administrativa `X-Execution-Key`;
4. preview firmado y vigente;
5. frase exacta `EJECUTAR OPERACION FCI {id}`;
6. aprobación humana;
7. fondo `verified`;
8. saldo vivo (suscripción) o tenencia (rescate);
9. cutoff abierto y día hábil;
10. un solo envío, sin reintento automático.

`ready_for_real_fci_subscription` / `ready_for_real_fci_redemption` requieren
**flag Y contrato verificado**. Un flag solo nunca alcanza.

Ante timeout ambiguo: `submission_unknown`. **No se repite el POST** — una
operación de FCI no es idempotente y una suscripción duplicada es plata real.

## Qué falta para habilitarlo

1. Abrir la documentación oficial y transcribir los nombres de campo exactos
   de `/api/v2/operar/suscripcion/fci` y `/api/v2/operar/rescate/fci`
   (incluido cómo se expresa `soloValidar` y qué devuelve la respuesta).
2. Completar `FCI_REQUEST_FIELDS` y poner
   `FCI_REQUEST_CONTRACT_VERIFIED = True`.
3. Implementar `submit_fund_request` en el cliente de broker con ese contrato.
4. Ensayar en **sandbox** (`RUN_IOL_SANDBOX_TESTS`), nunca en producción.
5. Recién entonces evaluar encender los flags.

## Sandbox

Las pruebas externas van detrás de `RUN_IOL_SANDBOX_TESTS`, con credenciales
**exclusivamente de sandbox**. Nunca se reutilizan credenciales reales, nunca
corre en CI normal y nunca enciende flags productivos.
