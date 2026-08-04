# FCI: determinación de capacidad y bloqueo documentado

## Resultado

```
fci_execution_supported = false
bloqueo estable          = fci_not_supported_by_iol_api
```

Fuente de verdad en el código: `backend/app/services/fci.py`
(`FCI_EXECUTION_SUPPORTED = False`). Endpoint de consulta:
`GET /api/broker/fci-capability`.

## Por qué

La consigna es explícita: **no inferir endpoints** y **no reutilizar
endpoints privados del sitio web**. Para habilitar suscripción y rescate
haría falta un contrato oficial verificado, y este repositorio **no lo
tiene**:

- no hay documentación oficial de IOL comprobable desde este entorno — la
  política de red bloquea `invertironline.com` (`403` al CONNECT del proxy) y
  `api-sandbox.invertironline.com` no resuelve por DNS;
- no hay request ni response verificados para suscripción o rescate;
- no hay identificador de operación (equivalente al `numeroOperacion` de
  títulos) comprobado;
- no hay cutoff, plazo de liquidación ni monto mínimo por fondo verificados;
- no hay sandbox verificado donde ensayarlo.

Que un FCI aparezca en el análisis y en la cartera **no demuestra nada sobre
la capacidad de operarlo**: `/api/v2/portafolio/{pais}` informa tenencias,
no habilita operaciones. Confundir esas dos cosas es exactamente el error
que este documento existe para evitar.

## Qué SÍ hace la aplicación con FCI

| Capacidad | Estado |
|---|---|
| Análisis de FCI en la cartera | ✅ |
| Visualización de tenencias FCI | ✅ |
| Recomendación sobre FCI | ✅ |
| Preview **informativo** | ✅ |
| Indicación de operar manualmente en IOL | ✅ |
| Enviar suscripción | ❌ `fci_not_supported_by_iol_api` |
| Enviar rescate | ❌ `fci_not_supported_by_iol_api` |
| Cancelar una operación de FCI | ❌ |

El preview informativo (`build_fci_informational_preview`) está construido
para **no poder mentir**:

- vocabulario propio: `subscribe` / `redeem`, nunca `buy` / `sell`;
- `limit_price`, `best_bid`, `best_ask` y `quantity_step` son `None` **por
  construcción** — un fondo no tiene precio límite ni punta, y mostrar uno
  sería inventar información;
- lleva los campos que sí corresponden a un fondo: `fund_cutoff_local_time`,
  `settlement_delay_days`, `fund_minimum_amount`, moneda;
- `executable=False`, `would_execute=False`, `immediate=False`,
  `manual_operation_required=True`;
- `blocking_reasons = ["fci_not_supported_by_iol_api"]`.

**El cutoff es por fondo.** No se hardcodea 15:00 para todos: viaja en la
entrada de catálogo del fondo (`fund_cutoff_local_time`) y el preview lo
muestra tal cual. Dos fondos con cutoffs distintos se muestran distintos.

## Garantías estructurales (no sólo un flag)

`FCI_SUBSCRIPTION_ENABLED` y `FCI_REDEMPTION_ENABLED` existen y se reportan
en readiness, pero **encenderlos no habilita nada**:

- `ready_for_real_fci_subscription` y `ready_for_real_fci_redemption` son
  `False` incondicionalmente mientras el contrato no esté verificado;
- `fci_execution_blocked()` no tiene ninguna rama que pueda responder
  "permitido";
- un instrumento cuya `execution_family` es `fund` es rechazado por el
  evaluador de alcance **antes** de cualquier consideración de flags, con
  `fci_not_supported_by_iol_api`;
- por lo tanto un FCI **no puede** generar un `OrderExecution` ni viajar por
  el contrato de títulos.

Cubierto por `tests/test_cedears_fci_and_batches.py` y
`tests/test_migration_and_security.py`.

## Qué haría falta para habilitarlo

No alcanza con cambiar la constante. El trabajo mínimo honesto es:

1. **Verificar el contrato oficial** de suscripción y rescate contra la
   documentación de IOL: endpoint exacto, método, request, response,
   identificador de operación, entorno (real/sandbox), limitaciones.
2. **Registrar la evidencia** en este documento, con el mismo nivel de
   detalle que `docs/IOL_CAPABILITY_MATRIX.md` §1.
3. **Implementar modelos propios** — `FundInstrument`, `FundOperation`,
   `FundOperationPreview`, `FundOperationDecision` — con su **máquina de
   estados asíncrona** propia: `prepared`, `approval_requested`, `submitted`,
   `pending_confirmation`, `confirmed`, `rejected`, `cancelled`,
   `reconciliation_required`, `submission_unknown`.
   **Nunca** reutilizar `OrderExecution`.
4. **Validaciones específicas de fondo**: fondo exacto, moneda, monto mínimo,
   saldo vivo (suscripción), cuotapartes rescatables (rescate), cutoff **del
   fondo**, día hábil, plazo de liquidación, operación previa duplicada,
   monto máximo, porcentaje máximo de cartera, confirmación humana, hash
   firmado, at-most-once.
5. **No afirmar "ejecutada" al enviar**: una suscripción se confirma más
   tarde, a un valor de cuotaparte que al momento del envío no existe.
6. **No reintentar** cuando el resultado del primer envío sea desconocido.
7. Recién entonces, poner `FCI_EXECUTION_SUPPORTED = True` y que los flags
   pasen a tener efecto.

## Cancelación de FCI

No se asume que una operación pendiente pueda cancelarse. Si IOL ofreciera
cancelación, se implementaría según su contrato oficial; hasta entonces
`cancellation_supported = false` y no existe ningún camino de cancelación
para fondos.
