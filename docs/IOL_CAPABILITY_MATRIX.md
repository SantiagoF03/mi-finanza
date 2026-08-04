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

> **Limitación explícita del entorno de trabajo.** La política de red del
> entorno donde se desarrolló este cambio **bloquea el dominio
> `invertironline.com`** (el proxy responde `403` al CONNECT, y
> `api-sandbox.invertironline.com` no resuelve por DNS). No fue posible leer
> la documentación oficial desde acá. Por lo tanto **no se agregó ninguna
> capacidad nueva por inferencia**: todo lo marcado VERIFICADO proviene de
> evidencia de primera mano ya presente en el repositorio (tests de contrato
> y la operación productiva 183382167), y todo lo demás queda NO VERIFICADO y
> bloqueado. Esto es exactamente lo que pide la consigna: *no inferir
> endpoints*.

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

**`fci_execution_supported = false`** · bloqueo estable:
**`fci_not_supported_by_iol_api`**

| Capacidad requerida | Estado |
|---|---|
| Catálogo / identificación del fondo | NO VERIFICADO |
| Suscripción | NO VERIFICADO |
| Rescate | NO VERIFICADO |
| Consulta de operación | NO VERIFICADO |
| Cancelación | NO VERIFICADO |
| Cutoff | NO VERIFICADO |
| Plazo de liquidación | NO VERIFICADO |
| Moneda | NO VERIFICADO |
| Monto mínimo | NO VERIFICADO |
| Sandbox | NO VERIFICADO |

Ver el detalle y las consecuencias en **`docs/IOL_FCI_CAPABILITY.md`**.

Que un FCI aparezca en `/api/v2/portafolio/{pais}` demuestra que IOL **lo
informa como tenencia**; no demuestra en absoluto que exista un endpoint
público para suscribirlo o rescatarlo. Son afirmaciones distintas y el motor
no las confunde.

---

## 5. Cancelación de órdenes

**NO VERIFICADO** para cualquier instrumento. No se inventó un endpoint de
cancelación.

Lo que sí existe y está implementado:

- `cancellation_supported = false` en todas las entradas del catálogo
  alimentadas por descubrimiento read-only;
- **cancelación manual** como decisión humana registrada: la cola de
  conciliación (`GET /api/executions/reconciliation-queue`) y
  `POST /api/executions/{id}/reconcile` permiten resolver explícitamente una
  orden (`confirm_not_sent`, `confirm_sent`, `confirm_rejected`,
  `confirm_executed`) con frase exacta + credencial de ejecución;
- **consulta read-only** del estado en el broker
  (`POST /api/executions/{id}/refresh-broker-status`), que **nunca** cambia
  el estado automáticamente.

Es decir: la app **no cancela en IOL**. Registra que un humano canceló (o
verificó) en IOL. Eso es honesto y auditable; simular una cancelación que la
API no expone no lo sería.

---

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
| `securities` | `ACCIONES` | ✅ tras `SECURITIES_BUY_ENABLED` | ✅ tras `SECURITIES_SELL_ENABLED` | ✅ | ❌ manual |
| `securities` | `CEDEARS` | ✅ tras `SECURITIES_BUY_ENABLED` | ✅ tras `SECURITIES_SELL_ENABLED` | ✅ | ❌ manual |
| `fund` | `FCI` | ❌ `fci_not_supported_by_iol_api` | ❌ `fci_not_supported_by_iol_api` | n/a | ❌ |
| — | BONO / ON / TitulosPublicos / ETF | ❌ sin clase de ejecución | ❌ | — | ❌ |

Todas las capacidades arrancan **apagadas**. El candado global
`ORDER_EXECUTION_ENABLED=false` sigue por encima de todas.
