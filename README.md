# Mi Finanza MVP (mock-first + IOL read-only)

MVP de inversión semiautomática con reglas hard y estados de recomendación.
Ahora soporta broker real IOL en **modo solo lectura** para snapshot de portafolio.

## Stack
- Backend: Python 3.12, FastAPI, SQLAlchemy, APScheduler
- Frontend: React + Vite
- DB: SQLite (diseñado para migrar a Postgres)

## Estados de recomendación
- `pending`: recomendación activa.
- `blocked`: degradada por reglas hard.
- `approved`: cerrada por aprobación.
- `rejected`: cerrada por rechazo.
- `superseded`: reemplazada por una nueva.

## Recomendación actual
La recomendación actual es la más reciente abierta (`pending` o `blocked`).
Si se crea una nueva, abiertas previas pasan a `superseded`.

## Broker mode
- `BROKER_MODE=mock`: usa `MockBrokerClient`.
- `BROKER_MODE=real`: usa `IolBrokerClient` read-only.

### IOL read-only (nuevo)
- Auth:
  - `POST {IOL_API_BASE}/token` con `grant_type=password`.
  - Refresh con `grant_type=refresh_token`.
- Portfolio:
  - `GET {IOL_API_BASE}/api/v2/portafolio/{IOL_PORTFOLIO_COUNTRY}`.
- Seguridad:
  - no se loguea token ni password.
  - password no se persiste.
- Fallback:
  - si falla auth/portfolio en modo real, el ciclo usa mock fallback para no romper pipeline.

## Endpoint de validación broker
- `GET /api/broker/ping`
  - valida conectividad/autenticación del broker sin ejecutar ciclo completo.

## Idempotencia / scheduler
- `TRIGGER_COOLDOWN_SECONDS` evita ejecuciones duplicadas por triggers seguidos.
- Scheduler con `coalesce`, `max_instances=1`, `replace_existing=True`.

## Levantar local
```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp ../.env.example .env
uvicorn app.main:app --reload --port 8000
```

```bash
npm --prefix frontend install
npm --prefix frontend run dev
```

## Probar IOL real en 5 minutos
1. Editá `backend/.env`:
   - `BROKER_MODE=real`
   - `IOL_API_BASE=https://api.invertironline.com`
   - `IOL_USERNAME=...`
   - `IOL_PASSWORD=...`
   - `IOL_PORTFOLIO_COUNTRY=argentina`
2. Levantá backend.
3. Probá `GET /api/broker/ping`.
4. Ejecutá `POST /api/analysis/run`.
5. Consultá `GET /api/portfolio/summary` y verificá que posiciones/cash vienen de IOL.

## Ejecución de órdenes (aprobación del usuario)

El flujo de ejecución es **semi-automático**: la app sugiere, el usuario decide.

### Flujo completo

```
Motor rule-based → Recomendación (pending/blocked)
  → Usuario aprueba en UI → POST /api/recommendations/{id}/approve
    → Se crean OrderExecution rows (execution_requested)
    → Broker.place_order() → execution_sent
    → Resultado: executed / partially_executed / rejected_by_broker / failed
  → Usuario rechaza → POST /api/recommendations/{id}/reject
    → Sin órdenes. Recomendación queda en "rejected".
```

### Invariantes de seguridad
- **El scheduler NUNCA ejecuta órdenes** — solo ingesta y análisis
- **El LLM NUNCA ejecuta órdenes** — solo explica
- **Solo `POST /api/recommendations/{id}/approve`** dispara ejecución real
- Recomendación debe estar en `pending` o `blocked` para aprobar/rechazar

### Estados de ejecución

| Estado | Significado |
|---|---|
| `execution_requested` | Orden creada, pendiente de envío |
| `execution_sent` | Enviada al broker |
| `executed` | Ejecutada exitosamente |
| `partially_executed` | Ejecución parcial |
| `rejected_by_broker` | Rechazada por el broker |
| `failed` | Error técnico |

### Broker modes para ejecución
- `BROKER_MODE=mock`: MockBrokerClient simula órdenes exitosas
- `BROKER_MODE=real`: IolBrokerClient envía órdenes reales via `POST /api/v2/operar`

## Cobertura de instrumentos: catálogo + políticas por clase

La app dejó de estar limitada a un piloto de un solo símbolo. La cobertura de
**acciones argentinas y CEDEARs** se resuelve con dos piezas, sin escribir una
política a mano por cada símbolo:

### 1. Catálogo de ejecución (`execution_instruments`)

Es la **única autoridad sobre identidad operable**. Deliberadamente separado
de `instrument_catalog` (que es el universo de *análisis*): un universo de
análisis nunca debe poder autorizar una orden por sí solo.

Se alimenta con datos **read-only** del broker (la cartera real informa
`asset_type`, `instrument_type` y `moneda` de cada símbolo) vía
`POST /api/broker/instrument-catalog/refresh`. **Nunca** se alimenta con texto
generado por un LLM: una cadena generada no puede declarar moneda, mercado ni
tick.

### Estados de verificación

Que un campo esté **presente** no es lo mismo que esté **verificado**. Cada
entrada lleva un `verification_status` y una **procedencia por campo**:

| Estado | Puede operar |
|---|---|
| `candidate` | ❌ resuelto read-only, todavía sin verificar |
| `verified` | ✅ cada campo operativo tiene procedencia confiable |
| `identity_changed` | ❌ congelado hasta decisión humana |
| `rejected` | ❌ un humano lo rechazó |
| `stale` | ❌ la verificación venció |

Procedencias que **cuentan** como verificación: `iol_portfolio`,
`iol_title_detail`, `iol_quote`, `iol_fci_catalog`, `admin_verified_override`.
**`class_policy_default` NO cuenta**: una política aporta un límite, pero
nunca miró el instrumento y no puede afirmar que verificó su tick, su moneda
ni sus capacidades.

`buy_supported` / `sell_supported` **no se infieren por familia ni uno del
otro**: tener algo en cartera no prueba que se pueda comprar, vender o
cotizar, y poder venderlo no prueba que se pueda comprarlo. Cada lado exige su
propia evidencia y del lado correcto del libro: `sell_supported` requiere una
punta compradora real (`source == "bid"`, precio positivo) y `buy_supported`
una punta vendedora (`source == "ask"`). Un instrumento con sólo una de las
dos queda operable de un solo lado.

### Cambio de identidad

Identidad es **qué instrumento es**: símbolo, mercado, tipo, moneda, país. El
**plazo de liquidación no es identidad** — es un parámetro de la orden, que el
mismo instrumento puede tener en t0 o t1 según cómo se opere. Tratarlo como
identidad congelaba instrumentos correctos cada vez que el broker devolvía el
plazo del otro lado.

Si cambia algún campo de identidad, la entrada **no se sobrescribe**: se
guarda la identidad anterior y la propuesta, pasa a `identity_changed`, se
bloquean compra y venta, y `verified_at` **no** se toca. Un refresh automático
posterior **no** puede resolverlo — hace falta
`POST /api/broker/instruments/{symbol}/identity-decision`. Aceptar devuelve la
entrada a `candidate`, no a `verified`: lo verificado bajo la identidad
anterior ya no aplica.

El hash de identidad está **versionado** (`IDENTITY_HASH_VERSION`). Cambiar la
definición de identidad cambia todos los hashes; sin versión, ese cambio de
esquema se leía como "el broker cambió el instrumento" y congelaba la cartera
entera de golpe. Una entrada guardada con una versión anterior se recalcula,
no se marca como cambiada.

### Instrumentos no tenidos

`POST /api/broker/instruments/resolve` resuelve read-only un símbolo que
todavía no está en cartera, usando sólo endpoints oficiales de títulos y
cotizaciones. Sólo se resuelven símbolos que ya están en una fuente acotada
(recomendación abierta, watchlist, universo habilitado, posición): **un
símbolo nunca es operable sólo porque alguien lo escribió**.

Otros bloqueos: `instrument_catalog_missing`, `instrument_catalog_incomplete`,
`instrument_catalog_stale`, `instrument_not_verified`, `instrument_inactive`,
`instrument_class_unsupported`. No hay wildcard.

### 2. Políticas por clase (`EXECUTION_CLASS_POLICIES`)

Una política cubre **todos** los instrumentos de su clase — agregar un CEDEAR
no requiere escribir nada nuevo.

| Clase | Familia | Estado |
|---|---|---|
| `ACCIONES` | `securities` | operable |
| `CEDEARS` | `securities` | operable |
| `FCI` | `fund` | contrato propio, apagado (ver abajo) |

Cada política define: `buy_enabled`, `sell_enabled`, `currencies`, `markets`,
`settlements`, `max_order_notional`, `max_daily_notional`, `max_quantity`,
`max_portfolio_pct`, `min_cash_reserve`, `fee_buffer_pct`,
`max_quote_age_seconds`, `max_price_deviation_pct`, `order_type`,
`validity_minutes`, `catalog_max_age_seconds`, `default_quantity_step`,
`default_price_tick`. **Todos obligatorios**: un límite ausente nunca se lee
como "sin límite".

Encima de la clase:

- **Overrides por símbolo** (`EXECUTION_INSTRUMENT_OVERRIDES`): sólo pueden
  **endurecer** (bajar un límite, apagar un lado, subir la reserva). Aflojar
  requiere el escape administrativo explícito
  `EXECUTION_ALLOW_OVERRIDE_LIMIT_INCREASE=true`; sin él se responde
  `override_increases_limit` y se conserva el valor de la clase.
- **Denylist** (`EXECUTION_DENYLIST`): gana siempre, sobre cualquier override.
- **Allowlist legacy** (`EXECUTION_INSTRUMENT_POLICIES`): sigue funcionando y
  manda para los símbolos que cubre.

Detalle completo y ejemplo de JSON: `backend/.env.execution.example`.

## Capacidades separadas (todas apagadas por defecto)

```
ORDER_EXECUTION_ENABLED=false     # candado global, por encima de todo
SECURITIES_BUY_ENABLED=false
SECURITIES_SELL_ENABLED=false
FCI_SUBSCRIPTION_ENABLED=false
FCI_REDEMPTION_ENABLED=false
```

`EXECUTION_SELL_ONLY` está **deprecado pero no eliminado**: mientras esté en
`true` sigue bloqueando toda compra y sigue autorizando el camino legacy de
venta, así que la configuración productiva actual no cambia de comportamiento.

Migración por etapas, cada una con su propia política:

| Etapa | Flags | Qué habilita |
|---|---|---|
| **Legacy** | `EXECUTION_SELL_ONLY=true`, ambos `SECURITIES_*=false` | Sólo la venta legacy de BYMA, tal como ya se ejecutó |
| **ACCIONES** | `EXECUTION_SELL_ONLY=false`, `SECURITIES_BUY_ENABLED=true`, `SECURITIES_SELL_ENABLED=true` | Con una policy `ACCIONES` conservadora |
| **CEDEARS** | Mismos flags globales | Con una policy `CEDEARS` **independiente** |

`GET /api/broker/execution-readiness` reporta cada tramo por separado —
`legacy_byma.legacy_sell_path_ready`, `acciones.buy_ready`,
`acciones.sell_ready`, `cedears.buy_ready`, `cedears.sell_ready`— y **nunca
declara lista una clase entera porque un símbolo lo esté**: `covered_symbols` y
`*_ready` son datos distintos, reportados aparte por eso mismo.

También devuelve `next_safe_action` (determinístico, derivado de los bloqueos:
`resolve_instruments` → `verify_instrument_fields` → `configure_class_policies`
→ `configure_fci_limits` → `verify_fund` → `run_sandbox_validation` →
`ready_for_controlled_pilot`) y distingue `technically_ready_but_locked` de
`ready_for_real_execution`: con el candado cerrado, lo segundo es `false` por
construcción.

Ni el scheduler ni ningún endpoint público pueden cambiar estos flags.

### Pilotos controlados

`POST /api/execution-pilot/securities` crea **una** recomendación `pending`
marcada `metadata_json.execution_pilot=true`. Cuatro pilotos independientes
(ACCIONES buy/sell, CEDEARS buy/sell) comparten el endpoint; cuál se crea lo
decide únicamente el símbolo y el lado explícitos. Nada se implica: sin símbolo
por defecto, sin lado por defecto, sin cantidad por defecto, y con una frase de
confirmación que nombra los tres —`CREAR PILOTO SELL BYMA 1`— **así que la
frase de un piloto no autoriza otro**.

Exige `EXECUTION_PILOT_CREATION_ENABLED=true`, el símbolo `technically_ready`
para **ese lado**, cantidad múltiplo del step y dentro del tope técnico. Crear
no es aprobar y no es enviar: si hay una decisión pendiente, la creación se
**bloquea** en vez de superponerse — Recommendation 13 nunca se toca.

El camino legacy (`POST /api/execution-pilot/recommendations`, frase
`CREAR PILOTO BYMA 1`) sigue existiendo sin cambios: ya ejecutó una venta real
de punta a punta y migrarlo no es gratis.

Runbook completo: `docs/SECURITIES_ACTIVATION_RUNBOOK.md`.

## Compra de títulos: preflight de saldo vivo

Antes de enviar una compra se valida, **inmediatamente antes del envío**:

1. **Recomendación** — pending/blocked, no vencida, sin ejecución ni
   aprobación previa, preview firmado y vigente.
2. **Instrumento** — catálogo válido y fresco, `buy_supported`, clase
   habilitada, mercado/plazo/moneda correctos, tick y step conocidos.
3. **Mercado** — día hábil y horario válido (`market_closed`,
   `market_schedule_unknown`). Fail closed ante horario desconocido.
4. **Cotización** — **best ask** (nunca `ultimoPrecio`, nunca el bid),
   timestamp con zona horaria, antigüedad dentro del máximo de la clase,
   precio múltiplo exacto del `price_tick` (nunca se redondea para encajar),
   desviación máxima respecto de la referencia firmada.
5. **Saldo** — `get_live_cash(moneda)` contra `/api/v2/estadocuenta`,
   **por moneda** (sumar pesos y dólares daría un número sin sentido).
   Se descuentan buffer de costos, reserva mínima y compras pendientes.
   `snapshot.cash` sólo puede **achicar** una orden, jamás autorizarla.
6. **Cantidad** — múltiplo del `quantity_step`, mínimo, máximos por símbolo,
   por clase, por orden, **por día** y como porcentaje de cartera.
7. **Confirmación** — preview firmado + frase exacta + `X-Execution-Key`.

Códigos de bloqueo: `buy_execution_disabled`, `live_cash_unavailable`,
`insufficient_live_cash`, `currency_cash_mismatch`, `fee_buffer_exceeded`,
`quote_stale`, `quote_unavailable`, `price_tick_mismatch`,
`quantity_step_mismatch`, `order_limit_exceeded`, `daily_limit_exceeded`,
`portfolio_pct_limit_exceeded`.

El límite diario se consume **sólo en el punto de no retorno** (cuando la
orden se commitea como `submitting`), así que un preflight bloqueado nunca
gasta presupuesto del día.

### Límite diario resistente a concurrencia

`ExecutionDailyNotional` tiene unicidad real por
`(trade_date, execution_class, currency)` y la reserva es **atómica**:

```sql
UPDATE ... SET submitted = submitted + :amt
WHERE clave = ... AND submitted + :amt <= :limit
```

La aritmética y la comparación ocurren **dentro de la base**. Dos aprobaciones
simultáneas que en un `SELECT` verían presupuesto disponible no pueden ganar
ambas este `UPDATE`. Sin la unicidad, cada proceso insertaría su propia fila y
se creería dueño de todo el cupo diario — exactamente el bug que la restricción
existe para impedir.

ARS y USD **no comparten ledger**, ni las clases entre sí, ni los días. Las
compras pendientes se agrupan por **moneda y fecha operativa**: una compra en
dólares ya no recorta el cupo en pesos, y una operación sin conciliar de hace
un mes ya no reserva plata para siempre (se reporta aparte como
`stale_pending_buys`).

Una cancelación **no libera** presupuesto automáticamente: al enterarnos casi
nunca podemos probar que la orden no consumió nada en el broker. Liberar es un
acto administrativo explícito con motivo registrado.

### Leer `estadocuenta`: disponible ≠ disponible hoy

El saldo vivo sale de los buckets de liquidación **inmediata** de
`/api/v2/estadocuenta`, nunca del total de la cuenta: plata que liquida en t+1
no paga una compra de hoy.

Un bucket cuya etiqueta de plazo viene **vacía** no es inmediato. Antes, una
etiqueta vacía pasaba el filtro y su saldo se contaba como disponible hoy —
un saldo a plazo mal etiquetado se leía como efectivo. Hoy la etiqueta tiene
que estar presente **y** ser una de las inmediatas conocidas.

Cuando una cuenta no expone ningún bucket inmediato legible, el resultado es
`available = None` con `unreadable = True` y `no_immediate_bucket = True` —
**no** `0` y **no** el total. Cero significaría "no tenés plata" y el total
significaría "tenés toda"; las dos son afirmaciones que el payload no
respalda. `None` bloquea con `live_cash_unavailable`, que es la respuesta
correcta a "no sé".

`GET /api/broker/account-status-diagnostic` muestra la normalización completa,
bucket por bucket, para poder auditarla sin adivinar.

## FCI: familia separada con contrato oficial

Los endpoints de FCI **son oficiales** y están implementados como una familia
aparte — nunca sobre `OrderExecution`:

```
GET  /api/v2/Titulos/FCI              catálogo de fondos
GET  /api/v2/Titulos/FCI/{simbolo}    detalle
POST /api/v2/operar/suscripcion/fci   suscripción
POST /api/v2/operar/rescate/fci       rescate
```

El contrato del request está **completo y verificado** — tres campos
form-urlencoded, iguales en ambos endpoints:

```
Content-Type: application/x-www-form-urlencoded
Simbolo=ADBAICA&Monto=145.54&soloValidar=true
```

El **rescate también se expresa por `Monto`**. No existe campo oficial para
cuotapartes, así que no se envía ninguno: agregar un campo inventado a un
rescate real es cómo una orden se reinterpreta en silencio. Corolario: no hay
forma de pedir "rescatá todo".

`Monto` se serializa con `Decimal`, nunca con `str(float)` — un float binario
imprime `145.54000000000002` o `1e+05` según el valor, y ese string entra en
el hash firmado del preview.

Modelos propios: `FundInstrument`, `FundOperation`, `FundOperationDecision`,
con máquina de estados asincrónica (`prepared` → … → `pending_confirmation` →
`confirmed`). **No existe el estado `executed`**: "enviada" nunca significa
"ejecutada", porque el fondo confirma después, a un valor de cuotaparte que al
enviar no existía.

Un FCI no tiene precio límite, punta, tick ni lote — el preview los expone
como `None` por construcción. El **cutoff es por fondo**
(`FundInstrument.cutoff_local_time`), leído del catálogo: no hay 15:00
hardcodeado.

### `soloValidar` obligatorio, fresco y del mismo payload

Validar es obligatorio antes de enviar, con TTL (`FCI_VALIDATION_TTL_SECONDS`,
120 s) **y** hash del payload validado. Cambiar el monto invalida la
validación (`fci_validation_payload_changed`); dejarla envejecer también
(`fci_validation_expired`). Una validación nunca crea decisión, ni fija
`broker_operation_id`, ni consume presupuesto.

El **candado global tapa a FCI**: con `ORDER_EXECUTION_ENABLED=false` ni
siquiera sale el `soloValidar=true`, porque es una llamada autenticada al
broker hecha en preparación para ejecutar. Devuelve `423 execution_locked`.

**FCI sigue apagado en producción**, ahora por decisión de flags:
`FCI_SUBSCRIPTION_ENABLED` y `FCI_REDEMPTION_ENABLED` son **independientes**
—poder poner plata en un fondo no dice nada sobre poder sacarla— y los límites
`FCI_MAX_OPERATION_AMOUNT` / `FCI_MAX_DAILY_AMOUNT` están en 0, que significa
*no configurado* = bloqueado. Ver `docs/IOL_FCI_CAPABILITY.md`.

| Method | Path | Description |
|---|---|---|
| POST | `/api/funds/operations` | Prepara la operación (sólo fondos `verified`) |
| POST | `/api/funds/operations/{id}/validate` | `soloValidar=true`, un solo POST |
| GET | `/api/funds/operations/{id}/preview` | Preview firmado, read-only |
| POST | `/api/funds/operations/{id}/submit` | `soloValidar=false`, un solo POST |
| GET | `/api/funds/operations/{id}` | Estado, sin secretos |
| POST | `/api/funds/operations/{id}/reconcile` | Conciliación humana |
| POST | `/api/funds/catalog/refresh` | Refresca el catálogo de fondos |
| GET | `/api/funds/catalog/{symbol}` | Estado del fondo + auditoría de verificación |
| POST | `/api/funds/catalog/{symbol}/verify` | Verificación administrativa |
| POST | `/api/funds/catalog/{symbol}/reject` | Rechaza el fondo |
| POST | `/api/funds/catalog/{symbol}/demote` | Retira la verificación |

### Un fondo catalogado no es un fondo operable

`create_fund_operation` exige `verification_status=verified`, no sólo que el
símbolo exista. Un `candidate` se rechaza con `fund_not_verified`: leer un
fondo lo hace visible, no confiable para temporizar y dimensionar plata real.

`POST /api/funds/catalog/{symbol}/verify` promueve un fondo **sobre la
afirmación explícita de un humano**: cutoff, mínimo, plazo, moneda y cada
capacidad por separado, todos obligatorios, ninguno heredado del valor
observado. Escribe una fila de auditoría (`FundInstrumentVerification`) con el
hash de lo verificado. **No llama a IOL, no enciende ningún flag y no crea
ninguna operación.**

Un refresh automático **preserva** esa verificación mientras símbolo, moneda y
administradora no cambien. Si cambian, el fondo se **congela** en
`identity_changed`: una aprobación dada para el fondo A no autoriza plata al
fondo B.

### El orden del envío: la reserva va después de las consultas vivas

1. validaciones puras (estado, monto, cutoff, mínimo, flags, validación fresca,
   preview firmado, credencial, frase);
2. consultas **vivas** read-only (saldo, tenencia, pendientes por tipo);
3. **claim atómico + reserva del cupo diario, en la misma transacción**;
4. **un** POST.

Antes, la reserva ocurría en el paso 2: una operación rechazada por falta de
saldo igual quemaba cupo del día, y la siguiente —fondeada— podía chocar con un
límite que la primera nunca tuvo derecho a consumir. Y el claim y la reserva
viven o mueren juntos: un claim sin reserva dejaría la operación en
`submitting` para siempre, ni enviable ni reintentable.

### Pendientes por tipo, no por total

| Función | Afecta | Agrupa por |
|---|---|---|
| `pending_fund_subscriptions` | efectivo disponible | moneda |
| `pending_fund_redemptions` | tenencia rescatable | símbolo **y** moneda |

Una suscripción pendiente es plata saliendo: no achica ninguna tenencia. Un
rescate pendiente es plata entrando: no achica el efectivo. Y rescatar del
fondo A no dice nada sobre cuánto queda del B.

Reservan capacidad: `submitting`, `submitted`, `pending_confirmation`,
`submission_unknown`, `reconciliation_required`. No reservan: `prepared`,
`validated`, `rejected`, `cancelled` y `confirmed` — este último porque el
saldo real ya lo refleja, y contarlo otra vez restaría la misma plata dos
veces.

### El ciclo de vida de la request: "no se envió" ≠ "quizás se envió"

El cliente estampa un `lifecycle` a medida que avanza: `before_send`,
`request_started`, `response_received`, `response_parsed`. `request_started` se
marca justo antes de que empiece el POST, que es el único límite demostrable:
antes de ahí no salió nada, después no podemos probar que no salió.

| Falla | `http_requests_sent` | Estado | Cupo |
|---|---|---|---|
| Antes del POST (auth, request inválida, bug local) | **0** | vuelve a `validated` | **se libera** |
| Durante o después del POST (timeout, 5xx, 2xx sin `numeroOperacion`) | 1 | `submission_unknown` | **se conserva** |

Un `except Exception` que inventa `http_requests_sent=1` deja una operación
esperando conciliación humana para siempre por un bug que nunca tocó la red.

Runbook completo: `docs/FCI_ACTIVATION_RUNBOOK.md`.

## Cancelación real de órdenes

```
DELETE /api/v2/operaciones/{numeroOperacion}
```

Con flag propio `ORDER_CANCELLATION_ENABLED=false`: poder enviar una orden no
dice nada sobre poder cancelarla. Flujo: preview firmado que lee el estado
**fresco** en IOL → `X-Execution-Key` + frase `CANCELAR EJECUCION {id}` →
**exactamente un DELETE**, reclamado atómicamente.

Nunca automática, nunca desde el scheduler ni el LLM. Un timeout o 5xx queda
en `cancellation_unknown` y **jamás se reintenta**: reenviar un DELETE que no
podemos probar que falló arriesga cancelar una orden **distinta y posterior**.

`confirm_cancelled` conserva su significado original — registrar que un humano
canceló en el panel de IOL — y **no** envía el DELETE.

## Calendario y horarios (minutos, no sólo horas)

```
SCHEDULER_MARKET_OPEN_TIME=10:30
SCHEDULER_MARKET_CLOSE_TIME=17:00
SCHEDULER_TIMEZONE=America/Argentina/Buenos_Aires
MARKET_HOLIDAYS=
```

`SCHEDULER_MARKET_OPEN_HOUR` / `SCHEDULER_MARKET_CLOSE_HOUR` quedan
**deprecadas** (sólo horas enteras, no pueden representar 10:30). Se siguen
honrando mientras las variables HH:MM estén ausentes.

El cron genera **slots exactos**, enumerados: 10:30, 11:00, 11:30 … 16:30.
Un `minute=*/30, hour=10-16` también dispara a las **10:00**, media hora antes
de la apertura, y ese run se reportaba como "market hours" — ese bug está
corregido y tiene test de regresión. Premarket (09:30, 10:15), último chequeo
16:55 y ciclo de cierre 17:05 son jobs independientes. El job de rueda además
**re-verifica la sesión al arrancar** (defensa en profundidad ante misfires o
DST). Un horario irresoluble **no registra ningún job** en vez de adivinar.

El calendario de títulos (BYMA) y el cutoff de cada FCI son cosas separadas:
el cutoff vive en la entrada de catálogo del fondo, no en el calendario.

### El scheduler sigue sin poder ejecutar

Puede analizar y recomendar. **No puede** aprobar, llamar a execution, enviar
una orden, suscribir, rescatar, cancelar ni habilitar flags. Garantizado por
tests AST sobre `app/scheduler/jobs.py`, `app/services/orchestrator.py`,
`app/services/analysis_gate.py`, `app/notifications/dispatcher.py`,
`app/llm/explainer.py`, `app/news/ingestion.py` y `app/market/calendar.py`.

## PWA / Mobile

La app es instalable como PWA (Progressive Web App):
- Manifest con íconos, theme_color, display standalone
- Service worker con cache offline (vite-plugin-pwa)
- UI responsive con tabs para navegación móvil
- Botones de aprobar/rechazar grandes y touch-friendly
- Web push subscription infrastructure (VAPID keys)

## Endpoints de ejecución y notificaciones

| Method | Path | Description |
|---|---|---|
| POST | `/api/recommendations/{id}/approve` | Aprobar y ejecutar órdenes |
| POST | `/api/recommendations/{id}/reject` | Rechazar sin ejecutar |
| GET | `/api/broker/execution-readiness` | Readiness por capacidad, por clase, límites y `next_safe_action` |
| GET | `/api/broker/pilot-readiness` | Aptitud **técnica** por símbolo y lado (puede leer cotización) |
| GET | `/api/broker/pilot-policy-template` | Borrador JSON de políticas, para revisar. No escribe nada |
| POST | `/api/execution-pilot/securities` | Crea UN piloto controlado (`pending`, sin aprobar) |
| GET | `/api/broker/instrument-capabilities` | Matriz read-only: buy/sell ready y motivos de bloqueo |
| POST | `/api/broker/instrument-catalog/refresh` | Refresca identidad desde datos read-only del broker |
| GET | `/api/broker/fci-capability` | Estado de la capacidad FCI (bloqueada y por qué) |
| GET | `/api/executions/recent` | Ejecuciones recientes |
| GET | `/api/executions/reconciliation-queue` | Órdenes que requieren conciliación manual |
| POST | `/api/executions/{id}/reconcile` | Resolución manual (incluye `confirm_cancelled`) |
| GET | `/api/executions/{id}/cancellation-preview` | Preview firmado de cancelación (no envía DELETE) |
| POST | `/api/executions/{id}/cancel` | Envía **un** DELETE a IOL |
| POST | `/api/broker/instruments/resolve` | Resolución read-only de instrumentos no tenidos |
| POST | `/api/broker/instruments/{symbol}/identity-decision` | Aceptar/rechazar un cambio de identidad |
| POST | `/api/broker/instruments/{symbol}/verify-fields` | Verificación administrativa de tick/step |
| GET | `/api/broker/account-status-diagnostic` | Diagnóstico normalizado de `estadocuenta` |
| POST | `/api/executions/{id}/refresh-broker-status` | Consulta read-only de estado al broker |
| GET | `/api/executions/{id}` | Detalle de una ejecución |
| GET | `/api/notifications/settings` | Config de notificaciones |
| PUT | `/api/notifications/settings` | Actualizar config |
| POST | `/api/push/subscribe` | Registrar push subscription |
| GET | `/api/push/vapid-public-key` | Obtener VAPID public key |


## Corrección de recomendación (símbolos reales)
- El motor ya no sugiere símbolos hardcodeados.
- Los símbolos recomendados salen únicamente de la cartera actual (`snapshot.positions`) y métricas derivadas del snapshot (`weights_by_asset`, `rebalance_deviation`).
- Si por cualquier motivo una acción apunta a un símbolo fuera del snapshot, la recomendación hace fallback a `mantener` con explicación.


## Mejoras MVP recientes
- **Cash real desde IOL**: el snapshot toma `cash` de `GET /api/v2/estadocuenta` (prioriza `disponible`, con fallbacks `saldoDisponible`, `cuentas.disponible`, `cuenta.disponible`, `cash`).
- **Noticias sin duplicados**: se evita insertar duplicados por `title + summary` (mock o provider real).
- **UI/API más limpia**: `GET /api/news/recent` devuelve solo las últimas 10 noticias.


## UX cooldown del trigger manual
- Si se dispara análisis durante cooldown, `POST /api/analysis/run` devuelve:
  - `status: "cooldown"`
  - `skipped: true`
  - `message`
  - `cooldown_remaining_seconds`
  - `cooldown_remaining_minutes`
- La UI muestra: “Todavía no podés generar una nueva recomendación. Esperá X min Y s.”
- El botón de trigger queda deshabilitado temporalmente con countdown local.
- Si `GET /api/recommendations/current` responde 404, la UI lo interpreta como estado válido: “No hay recomendación abierta actualmente”.


## Noticias reales (sin LLM)
- El pipeline usa provider configurable por `.env` con interfaz `get_recent_news()`.
- `NEWS_PROVIDER=mock` usa noticias simuladas (fallback seguro).
- `NEWS_PROVIDER=rss` usa feeds RSS reales (`NEWS_RSS_URLS`) y clasificación rule-based mínima:
  - `impact`: positivo / negativo / neutro
  - `event_type`: earnings / guidance / inflación / tasas / regulatorio / geopolítico / sectorial / ia / otro
  - `related_assets`: símbolos detectados en titular/resumen sobre la cartera
  - `confidence`: score simple rule-based
- Si el provider real no devuelve noticias, el sistema hace fallback a mock para no romper el ciclo.
- Persistencia anti-duplicados: no inserta dos veces la misma noticia (`title + summary`).
- `GET /api/news/recent` muestra solo las últimas 10 noticias.


## Universo operable y whitelist dinámica

Implementado en `backend/app/recommendations/universe.py`.

### Jerarquía de activos permitidos

| Capa | Fuente | Efecto |
|---|---|---|
| **Holdings reales** | `snapshot.positions` | Auto-permitidos para acciones principales. No necesitan estar en whitelist. |
| **Whitelist manual** | `WHITELIST_ASSETS` (.env) | Override manual. También permitidos para acciones principales. |
| **Watchlist externa** | `WATCHLIST_ASSETS` (.env) | Activos externos rastreados para oportunidades. No entran en acciones principales. |
| **Universo de mercado** | `MARKET_UNIVERSE_ASSETS` (.env) | Set amplio de activos operables conocidos. Base para futuras oportunidades. |

### Comportamiento clave

- **Un holding nuevo en IOL se permite automáticamente** sin tocar `.env`
- **WHITELIST_ASSETS sigue funcionando** como override (backward compatible)
- **Oportunidades externas** muestran su `tracking_status`: `watchlist`, `in_universe`, o `untracked`
- **Nunca** se promueve una oportunidad externa a acción principal sin que sea holding o esté en whitelist

### Tipos de activo soportados
`CEDEAR`, `ACCIONES`, `TitulosPublicos`, `FondoComundeInversion`, `ETF`, `BONO`, `ON`

### Configuración (.env)
```
WHITELIST_ASSETS=AAPL,MSFT,SPY,QQQ,AL30,BND,CASH
WATCHLIST_ASSETS=TSLA,NVDA,GOOGL
MARKET_UNIVERSE_ASSETS=MELI,GLOB,BBAR,GGAL,YPFD
```

### Campos en API
`GET /api/recommendations/current` incluye:
- `allowed_assets.holdings`: activos reales del snapshot
- `allowed_assets.whitelist`: whitelist manual
- `allowed_assets.watchlist`: watchlist configurada
- `allowed_assets.universe`: universo de mercado
- `allowed_assets.main_allowed`: unión de holdings + whitelist
- Cada `external_opportunity` tiene `tracking_status`

## Candidate sourcing para oportunidades externas

Implementado en `backend/app/market/candidates.py`.

Las oportunidades externas ahora se generan desde tres fuentes, no solo noticias:

| Fuente | Descripción |
|---|---|
| **news** | Noticias sobre activos no tenidos (como antes) |
| **watchlist** | Símbolos en `WATCHLIST_ASSETS` aparecen como candidatos aunque no haya noticias |
| **universe** | Símbolos en `MARKET_UNIVERSE_ASSETS` aparecen como candidatos observados |

Cada oportunidad externa incluye:
- `source_types`: lista de fuentes (`["news", "watchlist"]`, etc.) — refleja TODAS las fuentes combinadas
- `tracking_status`: clasificación (`watchlist`, `in_universe`, `untracked`)
- `asset_type` / `asset_type_status`: tipo resuelto y su estado (`known_valid`, `unknown`, `unsupported`)
- `in_main_allowed`: bool — si el símbolo está en whitelist/main_allowed (podría estar en acciones principales)
- `actionable_external`: bool — habilitado para seguimiento (en watchlist/universe + tipo no unsupported)
- `investable`: bool — listo para inversión manual (en main_allowed + tipo known_valid)
- `actionable_reason`: explicación semántica sin contradicciones
- `priority_score`: score dinámico — sube al combinar fuentes, al tener tipo válido, al ser investable

### Semántica de tres niveles

| Nivel | Flag | Significado |
|---|---|---|
| **Observado** | aparece en lista | Solo se ve, sin acción sugerida |
| **Seguimiento** | `actionable_external=true` | En watchlist/universe, habilitado para tracking activo |
| **Invertible** | `investable=true` | En whitelist + tipo válido, listo para inversión manual |

**Ejemplo real**: AAPL en `MARKET_UNIVERSE_ASSETS` + `WHITELIST_ASSETS` → `actionable_external=true`, `investable=true`, `asset_type=CEDEAR`, `asset_type_status=known_valid`.

## Resolución de tipos de activo

Implementado en `backend/app/market/assets.py`.

El sistema resuelve `asset_type` para cualquier símbolo usando múltiples fuentes en orden de prioridad:

1. **Posiciones (holdings)** — lookup directo, más confiable
2. **Mapa estático `KNOWN_ASSET_TYPES`** — ~100 símbolos conocidos del mercado argentino (CEDEARs, bonos, acciones, ONs, ETFs, FCIs)
3. **Heurística por sufijo** — patrones simples como terminación en "O" → ON
4. **Fallback** → `DESCONOCIDO` / `unknown`

### Campo `asset_type_status`

Cada candidato externo ahora incluye `asset_type_status` con tres valores posibles:

| Status | Significado | Efecto en actionable |
|---|---|---|
| `known_valid` | Tipo conocido y soportado (ej: CEDEAR, BONO) | No bloquea |
| `unknown` | No se pudo determinar el tipo | No bloquea (pendiente de resolver) |
| `unsupported` | Tipo conocido pero no soportado (ej: CRYPTOCURRENCY) | Bloquea actionable |

**Importante**: `DESCONOCIDO` ahora se muestra como `unknown`, **no** como `unsupported`. Un símbolo desconocido en watchlist sigue siendo actionable.

## Normalización de tipos de activo IOL

Implementado en `backend/app/broker/clients.py` → `_normalize_asset_type()`.

IOL V2 devuelve `titulo.tipo` en formato lowercase con underscores (`"acciones"`, `"cedears"`, `"fondos_comunes_de_inversion"`). El sistema normaliza automáticamente al formato canónico:

| IOL devuelve | Normalizado a |
|---|---|
| `acciones`, `accion` | `ACCIONES` |
| `cedears`, `cedear` | `CEDEAR` |
| `bonos`, `bono` | `BONO` |
| `letras`, `titulos_publicos` | `TitulosPublicos` |
| `obligaciones_negociables` | `ON` |
| `fondos_comunes_de_inversion`, `fci` | `FondoComundeInversion` |
| `etf`, `etfs` | `ETF` |

Sin esta normalización, posiciones como BYMA (`acciones`) o CRTAFAA (`fondos_comunes_de_inversion`) caían al bucket "otros", dejando buckets como `equity_local` y `fci` vacíos y distorsionando todo el rebalanceo.

## Composición por moneda (exposición económica)

Implementado en `backend/app/portfolio/analyzer.py` → `_infer_economic_currency()`.

`weights_by_currency` refleja **exposición económica**, no solo la moneda de trading:

| Tipo de activo | Moneda económica | Motivo |
|---|---|---|
| `CEDEAR` | USD | Representan acciones/ETFs de EE.UU. |
| `ETF` | USD | ETFs internacionales (SPY, QQQ, etc.) |
| `BONO` (GD*, AE*) | USD | Bonos globales dollar-linked |
| `BONO` (AL*, otros) | Trading currency | Bonos peso-linked |
| `ACCIONES` | ARS | Acciones locales argentinas |
| `FondoComundeInversion` | Trading currency | Depende del FCI |
| `DESCONOCIDO` | Trading currency | Fallback conservador |

**Antes**: SPY (CEDEAR/ETF traded en ARS) mostraba 100% ARS. **Ahora**: muestra como USD.

## Distribución por bucket (`weights_by_bucket`)

El análisis ahora incluye `weights_by_bucket` para transparencia:

```json
"weights_by_bucket": {
  "equity_exterior": 0.55,
  "equity_local": 0.09,
  "renta_fija": 0.12,
  "fci": 0.22,
  "cash": 0.02
}
```

Esto permite verificar que los buckets están correctamente poblados y que el rebalanceo tiene sentido.

## Perfil de inversor objetivo

Implementado en `backend/app/portfolio/profiles.py` y `backend/app/core/config.py`.

El perfil del inversor es una **configuración explícita** del sistema — no una suposición difusa ni una inferencia del LLM.

### Configuración

```
INVESTOR_PROFILE_TARGET=moderate_aggressive   # perfil objetivo del usuario
```

Se puede actualizar en runtime via API:
- `GET /api/profile/settings` — leer config actual
- `PUT /api/profile/settings` — cambiar perfil y overrides

### Perfiles disponibles

| Perfil | cash | renta_fija | equity_ext | equity_local | fci | otros | max_single | max_equity |
|---|---|---|---|---|---|---|---|---|
| conservative | 25% | 40% | 15% | 10% | 5% | 5% | 30% | 35% |
| moderate | 15% | 25% | 30% | 15% | 10% | 5% | 35% | 55% |
| **moderate_aggressive** | **10%** | **15%** | **40%** | **20%** | **10%** | **5%** | **40%** | **70%** |
| aggressive | 5% | 10% | 45% | 25% | 10% | 5% | 45% | 80% |

### Efecto en el sistema

- El análisis de cartera usa los thresholds del perfil para alertas y rebalanceo
- El rationale menciona explícitamente el perfil aplicado (ej: "perfil moderado-agresivo")
- Si el perfil es `moderate_aggressive`, la lógica NO castiga equity/growth como si fuera conservador
- Los alerts de concentración y equity band se ajustan al perfil

### Mapeo asset_type -> bucket
- `BONO`, `ON`, `TitulosPublicos` -> renta_fija
- `CEDEAR`, `ETF` -> equity_exterior
- `ACCIONES` -> equity_local
- `FondoComundeInversion` -> fci
- Desconocido -> otros

Si un bucket no tiene holdings, su peso se redistribuye a CASH para que los target weights siempre sumen 1.0.

## Motor principal usa noticias triageadas

El motor de recomendación (`generate_recommendation`) y el LLM se alimentan **exclusivamente** del pipeline de ingestion + triage. No usan noticias crudas del provider.

### Flujo completo

```
News Provider (RSS/Mock)
  → Ingestion Pipeline (dedup, classify, score, triage)
  → news_normalized (persist)
  → get_engine_eligible_news() → observe + send_to_llm + trigger_recalc → Motor principal
  → get_llm_eligible_news()    → send_to_llm + trigger_recalc          → LLM explicativo
```

### Observabilidad en metadata

Cada recomendación incluye en `metadata_json`:
- `news_used_engine`: cantidad de noticias triageadas usadas por el motor principal
- `news_used_llm`: cantidad de noticias usadas por el LLM
- `ingestion.triage_counts`: desglose por nivel de triage
- `ingestion.holdings_source`: `snapshot` o `whitelist`
- `profile_applied`: perfil de inversor usado
- `rationale_reasons`: lista estructurada de motivos (ver GAP 3)

## Rationale enriquecido

Las recomendaciones incluyen `rationale_reasons` con motivos estructurados:

| Tipo | Cuándo aparece |
|---|---|
| `target_profile_reason` | Desvío vs target del perfil objetivo |
| `concentration_reason` | Sobreconcentración en un activo |
| `overlap_reason` | Overlap entre ETFs (SPY/QQQ/ACWI/VOO/VTI/IVV) |
| `risk_reduction_reason` | Equity band excedida o sugerencia de pasar a liquidez |
| `return_expectation_reason` | Catalizador positivo o sugerencia de aumentar posición |

### Cómo interpretar una reducción de posición

- Si dice `concentration_reason` → el activo excede el peso máximo del perfil
- Si dice `overlap_reason` → hay redundancia entre ETFs de renta variable USA
- Si dice `risk_reduction_reason` → equity total excede la banda del perfil
- Si dice `target_profile_reason` → desvío técnico vs target
- **Destino**: en esta versión, la sugerencia es pasar a liquidez. No hay reasignación multi-activo automática

## Market Event Pipeline

### Ingestion Flow

```
News Provider (RSS/Mock)
  → Fetch & Deduplicate (by canonical URL + normalized title fallback)
  → Persist raw (news_raw table)
  → Classify (event_type, impact, related_assets)
  → Recency Filter (explicit time windows)
  → Pre-Score (cheap, rule-based, uses real holdings from snapshot)
  → Topic hash + multi-source repetition detection
  → Assign Triage Level
  → Persist normalized (news_normalized table)
  → Create MarketEvent (if observe+)
  → Trigger alert/recalc if warranted
```

### Recency Filter (Part B)

Explicit time windows by event type — NOT delegated to LLM:

| Event Type | Max Age |
|---|---|
| earnings, guidance, tasas, geopolítico | 24h |
| inflación, regulatorio, sectorial, ia | 48h |
| otro | 24h |

News older than 2x the window → `store_only` (persisted but no further processing).

### Pre-Scoring (Part B)

Cheap rule-based score (0.0–1.0) using these signals:
- Mentions holdings: +0.25
- Mentions watchlist/universe: +0.10
- Recency (linear decay over window): up to +0.20
- Top-tier source (Reuters, Bloomberg, Investing.com, etc.): +0.10
- Hard news type (earnings, guidance, tasas, etc.): +0.10
- Non-neutral impact: +0.10
- Confidence: up to +0.15

### Triage Levels

| Level | Condition | Effect |
|---|---|---|
| `store_only` | Old or irrelevant | Persisted, no further processing |
| `observe` | Moderate score, recent | Shown in events feed, no LLM |
| `send_to_llm` | Good score, recent | Eligible for LLM explanation next cycle |
| `trigger_recalc` | High score + holding mention + fresh | Triggers full cycle recalculation |

### LLM Cost Control

The scheduler NEVER calls the LLM unconditionally:
1. Ingestion runs are lightweight (fetch + classify + score)
2. Full cycle (with potential LLM) only runs when `trigger_recalc` events exist
3. `store_only` and `observe` events never reach the LLM
4. Post-market gets one scheduled full cycle; all other runs are ingestion-only unless triggered

### Alerting

Events with severity >= medium appear as active alerts in the frontend.

Severity mapping:
- `trigger_recalc` + negativo → **critical**
- `trigger_recalc` + other → **high**
- `send_to_llm` → **medium**
- `observe` → **low**

Alerts can be acknowledged via `POST /api/alerts/{id}/acknowledge`.

### Scheduler (Part D)

Market-hours aware, configurable via settings:

| Phase | Default Schedule | Action |
|---|---|---|
| Pre-market | 60min and 15min before open | Ingestion only |
| Market open | Every 30 min | Ingestion; full cycle only if `trigger_recalc` events |
| Post-market | Close +5min | Full cycle |
| Post-market | Close +1h | Light ingestion |
| Off-hours / Weekend | Nothing | Nothing |

Settings: `SCHEDULER_MARKET_OPEN_HOUR` (default 11 UTC = 8 ART), `SCHEDULER_MARKET_CLOSE_HOUR` (default 20 UTC = 17 ART), `SCHEDULER_OPEN_INTERVAL_MINUTES` (default 30).

### Notifications (Part E — Telegram)

Configure in `.env`:
```
NOTIFICATION_ENABLED=true
NOTIFICATION_CHANNEL=telegram
TELEGRAM_BOT_TOKEN=your_token
TELEGRAM_CHAT_ID=your_chat_id
NOTIFICATION_MIN_SEVERITY=medium
NOTIFICATION_COOLDOWN_SECONDS=300
```

### New API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/events/recent` | Recent market events (last 30) |
| GET | `/api/alerts/current` | Active alerts (unacknowledged, medium+ severity) |
| POST | `/api/events/run-ingestion` | Manual ingestion trigger |
| POST | `/api/alerts/{id}/acknowledge` | Acknowledge an alert |
| GET | `/api/profile/settings` | Leer perfil de inversor y thresholds actuales |
| PUT | `/api/profile/settings` | Actualizar perfil objetivo y overrides |

### New DB Tables

- `ingestion_runs` — tracks each ingestion execution
- `news_raw` — raw fetched news with dedup hash
- `news_normalized` — classified news with pre-score and triage level
- `market_events` — events that passed triage, with severity and trigger info

## Calibración de `suggested_pct`

`suggested_pct` se deriva del peor desvío material detectado:

```
raw_pct = abs(worst_deviation) * 0.5
suggested_pct = min(MAX_MOVEMENT_PER_CYCLE, max(0.02, raw_pct))
```

- **Escala gradual**: sugiere corregir ~50% del peor desvío por ciclo, no el desvío completo
- **Mínimo 2%**: evita sugerencias triviales
- **Capped a `MAX_MOVEMENT_PER_CYCLE`** (default 10%): previene movimientos excesivos
- **Confianza dinámica**: escala de 55% a 70% según severidad del desvío (20% dev = máxima severidad)

Ejemplo: desvío de 12% → sugiere 6%. Desvío de 28% → sugiere 10% (capped). Desvío de 8% → sugiere 4%.

## Recomendación principal vs oportunidades externas
- **Recomendación principal de cartera**: usa holdings reales (`snapshot.positions`), análisis de cartera y señales de mercado que afecten la cartera; sus `actions` solo pueden apuntar a activos en cartera o whitelist.
- **Oportunidades externas de mercado**: candidatos generados desde noticias + watchlist + universe, con campos enriched (`source_types`, `actionable_external`, `priority_score`, etc.).
- Las oportunidades externas **no** se mezclan con `actions` y **no** disparan approve/reject.


## Detección de “sin cambios materiales”

Implementado en `backend/app/recommendations/unchanged.py`.

El ciclo compara la nueva recomendación contra la última relevante (cualquier estado) usando estos criterios MVP:

| Criterio | Detalle |
|---|---|
| `action` | Si cambió la acción (mantener, reducir riesgo, etc.) |
| símbolos en `actions` | Si los activos afectados cambiaron |
| `suggested_pct` | Diferencia > `RECOMMENDATION_UNCHANGED_PCT_THRESHOLD` (default 0.01) |
| `blocked_reason` | Si la razón de bloqueo cambió |
| `risk_score` | Diferencia > `RECOMMENDATION_UNCHANGED_RISK_THRESHOLD` (default 0.03) |
| `concentration_score` | Diferencia > umbral de riesgo |
| `alerts` | Si las alertas de análisis cambiaron |
| noticias | Si la cantidad de noticias cambió en >= 2 |
| oportunidades externas | Si los símbolos de oportunidades cambiaron |

Si **ningún** criterio cambia materialmente → `unchanged=true`.

Campos persistidos en `metadata_json`:
- `unchanged`: bool
- `unchanged_reason`: string explicativo

Campos expuestos en `GET /api/recommendations/current`:
- `unchanged`: bool
- `unchanged_reason`: string

En frontend: si `unchanged=true`, se muestra un banner verde: *”No hubo cambios materiales desde el último análisis.”*

Variables de configuración:
- `RECOMMENDATION_UNCHANGED_PCT_THRESHOLD` (default: 0.01)
- `RECOMMENDATION_UNCHANGED_RISK_THRESHOLD` (default: 0.03)

## Capa LLM (solo explicación)

Módulo: `backend/app/llm/explainer.py`.

El LLM se usa **solo** para generar texto explicativo:
- `news_summary`: resumen legible de noticias recientes
- `recommendation_explanation_llm`: explicación en lenguaje simple de la recomendación

El LLM **NO** decide ni modifica:
- símbolos, porcentajes, reglas hard, estados (`pending/blocked/approved/rejected/superseded`)
- La recomendación estructurada siempre sale del motor rule-based

Campos persistidos en `metadata_json` y expuestos en API:
- `news_summary`: string | null
- `recommendation_explanation_llm`: string | null

En frontend:
- Si `recommendation_explanation_llm` existe, se usa como motivo principal (en vez de `rationale`)
- Si `news_summary` existe, se muestra en la sección de noticias
- Si no existen, se usan `rationale`/`executive_summary` normales

### Configuración LLM (.env)
```
LLM_ENABLED=false          # true para activar
LLM_PROVIDER=openai        # solo openai soportado
LLM_API_KEY=               # API key del proveedor
LLM_MODEL=gpt-4o-mini      # modelo a usar
LLM_TIMEOUT_SECONDS=15     # timeout de la llamada
```

Para activar: setear `LLM_ENABLED=true` y `LLM_API_KEY=sk-...` en `.env`.
Para desactivar: `LLM_ENABLED=false` (default).

### Resiliencia

- Si el LLM está deshabilitado o falla por timeout/error, el ciclo **no** se rompe.
- La recomendación estructurada sigue saliendo por reglas (rule-based).
- Los campos `news_summary` / `recommendation_explanation_llm` quedan en `null`.
