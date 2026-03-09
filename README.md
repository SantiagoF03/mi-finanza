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

## Restricción
No hay endpoints de compra/venta implementados. Solo lectura.


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


## Recomendación principal vs oportunidades externas
- **Recomendación principal de cartera**: usa holdings reales (`snapshot.positions`), análisis de cartera y señales de mercado que afecten la cartera; sus `actions` solo pueden apuntar a activos en cartera.
- **Oportunidades externas de mercado**: noticias sobre activos no tenidos se guardan como `external_opportunities` (watchlist), con `symbol`, `reason`, `confidence`, `event_type`, `impact`.
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
