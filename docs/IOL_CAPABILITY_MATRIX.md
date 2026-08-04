# Matriz de capacidades de la API de IOL (Fase 0)

Este documento registra **qué puede hacer verificablemente la aplicación
contra la API de IOL**, con la fuente de cada afirmación. Es la base de todo
lo que el motor de ejecución permite: **una capacidad que no está acá, no se
habilita**.

## Método y niveles de evidencia

| Nivel | Significado |
|---|---|
| **VERIFICADO-REAL** | Ejercitado manualmente contra `https://api.invertironline.com` y fijado por tests con `httpx.MockTransport`. |
| **VERIFICADO-PRODUCCIÓN** | Confirmado por una operación real registrada en esta app. |
| **NO VERIFICADO** | Sin contrato oficial comprobado por este repositorio. **Bloqueado.** |

| **DOCUMENTADO** | Ruta y método publicados en la documentación oficial de IOL, provistos por el responsable del repositorio. Implementado, pero apagado hasta ejercitarlo en sandbox. |

> **Limitación explícita del entorno de trabajo.** La política de red del
> entorno donde se desarrollaron estos cambios **bloquea `invertironline.com`
> en el gateway**: `developers.invertironline.com`, `api.invertironline.com` y
> `www.invertironline.com` responden `403` al CONNECT del proxy, y
> `api-sandbox.invertironline.com` no resuelve por DNS. **No fue posible
> revalidar los contratos leyendo la documentación oficial desde acá.**
>
> En consecuencia:
>
> - las **rutas y métodos** marcados DOCUMENTADO provienen de la lista
>   provista explícitamente por el responsable del repositorio, que sí tiene
>   acceso a la documentación;
> - los **nombres de campo** que no estaban ya verificados en el repo **no se
>   inventan**. Donde faltan (FCI), el armado de la request falla cerrado;
> - todo lo marcado VERIFICADO proviene de evidencia de primera mano ya
>   presente en el repositorio (tests de contrato contra la API real y la
>   operación productiva 183382167).

---

## 1. Acción argentina en pesos (ACCIONES / ARS / bCBA)

| Capacidad | Estado | Endpoint | Método | Evidencia |
|---|---|---|---|---|
| Cotización | **VERIFICADO-REAL** | `/api/v2/{mercado}/Titulos/{simbolo}/Cotizacion` | GET | `tests/test_iol_real_contract_fixes.py`: BYMA devolvió `ultimoPrecio 298`, `precioCompra 298`, `precioVenta 299`. El viejo `/api/v2/Cotizaciones/detalle/{mercado}/{simbolo}` responde **HTTP 400**. |
| Venta | **VERIFICADO-PRODUCCIÓN** | `/api/v2/operar/Vender` | POST | Operación IOL **183382167**: venta BYMA, cantidad 1, precio ejecutado ARS 297. `OrderExecution 1 = executed`. |
| Compra | **VERIFICADO-REAL** (contrato) | `/api/v2/operar/Comprar` | POST | Mismo contrato form-urlencoded que Vender (misma familia de endpoints, mismos campos). **No ejercitado con una compra real** → habilitado sólo detrás de `SECURITIES_BUY_ENABLED` + preflight completo de saldo vivo. |
| Consulta de operación | **VERIFICADO-REAL** | `/api/v2/operaciones/{numeroOperacion}` | GET | Devuelve `estadoActual` (p. ej. `iniciada`, `cancelada`). Usado por `refresh_broker_status`, read-only. |
| Consulta de posición | **VERIFICADO-REAL** | `/api/v2/portafolio/{pais}` | GET | Estructura `activos[].titulo{simbolo,tipo,moneda}` + `cantidad`, `valorizado`, `ppc`. |
| Consulta de saldo | **VERIFICADO-REAL** | `/api/v2/estadocuenta` | GET | Estructura `cuentas[]` con `moneda`, `disponible`, `saldos[].disponibleOperar`, `comprometido`. |
| **Cancelación** | **NO VERIFICADO** | — | — | No hay contrato de cancelación comprobado. Ver §5. |

### Contrato de orden (verificado)

```
POST /api/v2/operar/Vender      |  POST /api/v2/operar/Comprar
Content-Type: application/x-www-form-urlencoded     (nunca JSON)
Campos: mercado, simbolo, cantidad (entera exacta), precio (Decimal estable),
        plazo, validez (hora Buenos Aires, mismo día operativo),
        tipoOrden=precioLimite
```

**Clasificación de respuestas (verificada contra la API real):**

| Respuesta | Clasificación | Motivo |
|---|---|---|
| 2xx con `numeroOperacion` no vacío | `sent` | Único caso probado de aceptación. |
| **HTTP 202 con cuerpo de errores de validación** | `rejected` (definitivo) | Verificado: `[{"title":"PrecioLimite","description":"Los decimales indicados no son compatibles con la alteración mínima permitida..."}]`. La orden nunca se creó. |
| 4xx | `rejected` (definitivo) | La request fue evaluada y no aceptada. |
| Timeout / error de red / 5xx / 2xx sin `numeroOperacion` | `submission_uncertain` | **Conciliación manual, jamás reintento.** |

**Identificador idempotente:** IOL **no ofrece** claves de idempotencia. Por
eso la semántica del sistema es *at-most-once automatic submission + manual
reconciliation*: nunca se garantiza exactly-once, se garantiza que **no se
reenvía automáticamente**.

---

## 2. CEDEAR en pesos (CEDEAR / ARS / bCBA)

Mismas capacidades, mismos endpoints y misma clasificación de respuestas que
§1: en la API de IOL un CEDEAR es un título más, negociado en el mismo
mercado y con el mismo contrato `operar/Comprar|Vender`.

Diferencias que el motor **sí** modela por separado:

- clase de ejecución propia (`CEDEARS`), con sus propios límites;
- `instrument_type = CEDEAR` en la identidad firmada;
- **el ratio de conversión del CEDEAR no es un `quantity_step`.** El ratio
  describe cuántos CEDEARs equivalen a una acción del subyacente; el lote
  negociable viene del catálogo (`quantity_step`), nunca del ratio;
- **el CEDEAR local en ARS no es el activo subyacente extranjero.** La
  identidad (símbolo + moneda + mercado) se compara contra la posición real:
  una tenencia en USD bajo el mismo símbolo bloquea con
  `instrument_currency_mismatch`.

---

## 3. Especies negociadas en dólares

**Fuera de alcance. NO habilitadas.**

`_map_currency` normaliza a `ARS`/`USD`, y `/api/v2/estadocuenta` expone
cuentas por moneda, así que técnicamente la moneda es *legible*. Pero:

- no hay evidencia verificada de una orden en USD enviada por esta app;
- las variantes en dólares (MEP/CCL/cable) tienen especies, plazos y
  liquidaciones propias que **no fueron auditadas**;
- validar el saldo correcto exige mapear la cuenta USD exacta contra la
  especie exacta, y ese mapeo no está verificado.

Consecuencia en el código: las políticas por clase declaran
`currencies: ["ARS"]`. Una especie en USD queda bloqueada con
`instrument_currency_mismatch`. Habilitarla es una decisión posterior, con
evidencia propia.

---

## 4. FCI — Fondos Comunes de Inversión

**CORRECCIÓN respecto del PR #138.** Ese PR declaraba `fci_execution_supported
= false` afirmando que *no existe contrato oficial*. **Esa afirmación era
incorrecta.** La documentación oficial publica:

| Capacidad | Endpoint | Método | Estado |
|---|---|---|---|
| Catálogo de fondos | `/api/v2/Titulos/FCI` | GET | ✅ documentado, implementado |
| Detalle de un fondo | `/api/v2/Titulos/FCI/{simbolo}` | GET | ✅ documentado |
| Suscripción | `/api/v2/operar/suscripcion/fci` | POST | ✅ documentado |
| Rescate | `/api/v2/operar/rescate/fci` | POST | ✅ documentado |
| Validación previa | mismo endpoint con `soloValidar` | POST | ✅ documentado |

Implementado como **familia separada** (`FundInstrument`, `FundOperation`,
`FundOperationDecision`), nunca sobre `OrderExecution`.

**Lo que sigue sin verificar son los nombres exactos de los campos del
request.** El entorno de build bloquea `invertironline.com`, así que no fue
posible transcribirlos, y **no se inventan**:
`FCI_REQUEST_CONTRACT_VERIFIED = False` hace fallar cerrado el armado de la
request con `fci_request_contract_unverified`.

FCI permanece **apagado en producción**. Detalle completo en
**`docs/IOL_FCI_CAPABILITY.md`**.

---

## 5. Cancelación de órdenes

**CORRECCIÓN respecto del PR #138.** Ese PR declaraba que no había contrato de
cancelación. La documentación oficial publica:

```
DELETE /api/v2/operaciones/{numeroOperacion}
```

Implementado en `app/services/cancellation.py`, con estas garantías:

- **flag propio** `ORDER_CANCELLATION_ENABLED` (default `false`) — poder
  enviar una orden no dice nada sobre poder cancelarla;
- preview firmado (HMAC + TTL) que lee el estado **fresco** de la operación en
  IOL antes de decidir;
- `X-Execution-Key` + frase exacta `CANCELAR EJECUCION {id}`;
- **exactamente un DELETE**, reclamado atómicamente: una segunda solicitud
  concurrente pierde y recibe 409;
- **nunca automática**, nunca desde el scheduler, nunca desde el LLM;
- timeout o 5xx → `cancellation_unknown` y **jamás se reintenta**: reenviar un
  DELETE que no podemos probar que falló arriesga cancelar una orden
  **distinta y posterior**;
- consulta posterior read-only para conciliar.

`confirm_cancelled` sigue existiendo con su significado original — registrar
que **un humano canceló en el panel de IOL** — y **no** envía el DELETE. Son
dos cosas distintas y el código no las confunde.

## 6. Sandbox

**NO VERIFICADO.** La URL oficial del sandbox de IOL no está documentada en
este repositorio y `api-sandbox.invertironline.com` no resuelve desde el
entorno de build. **No se adivina.**

El soporte de sandbox está implementado y es completamente fail-closed:
`IOL_SANDBOX_API_BASE` vacío ⇒ `sandbox_environment_not_configured`. El
sandbox nunca puede apuntar al host real, y nunca comparte credenciales con
producción. Ver `docs/IOL_SANDBOX_GUIDE.md`.

---

## Resumen ejecutable

| Familia | Clase | Compra | Venta | Cotización | Cancelación |
|---|---|---|---|---|---|
| `securities` | `ACCIONES` | ✅ tras `SECURITIES_BUY_ENABLED` | ✅ tras `SECURITIES_SELL_ENABLED` | ✅ | ✅ tras `ORDER_CANCELLATION_ENABLED` |
| `securities` | `CEDEARS` | ✅ tras `SECURITIES_BUY_ENABLED` | ✅ tras `SECURITIES_SELL_ENABLED` | ✅ | ✅ tras `ORDER_CANCELLATION_ENABLED` |
| `fund` | `FCI` | ⏸ contrato oficial, campos sin verificar | ⏸ ídem | n/a | ⏸ |
| — | BONO / ON / TitulosPublicos / ETF | ❌ sin clase de ejecución | ❌ | — | ❌ |

Todas las capacidades arrancan **apagadas**. El candado global
`ORDER_EXECUTION_ENABLED=false` sigue por encima de todas.
