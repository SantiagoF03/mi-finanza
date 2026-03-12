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
| GET | `/api/executions/recent` | Ejecuciones recientes |
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
