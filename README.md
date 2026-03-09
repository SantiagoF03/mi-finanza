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
- El ciclo compara la nueva recomendación contra la última relevante usando criterios MVP explícitos:
  - `action`
  - símbolos principales en `actions`
  - diferencia de `suggested_pct` (umbral `RECOMMENDATION_UNCHANGED_PCT_THRESHOLD`)
  - `blocked_reason`
  - señales de análisis (`risk_score`, `concentration_score`, `alerts`)
  - fingerprint de noticias
- Si no hay cambios materiales, se guarda en metadata:
  - `unchanged=true`
  - `unchanged_reason`
- La UI muestra el mensaje: “No hubo cambios materiales desde el último análisis.”

## Capa LLM (solo explicación)
- Módulo: `backend/app/llm/explainer.py`.
- Usa LLM **solo** para:
  - `news_summary`
  - `recommendation_explanation_llm`
- El LLM **no** decide:
  - símbolos
  - porcentajes
  - reglas hard
  - estados (`pending/blocked/approved/rejected/superseded`)
- Si está deshabilitado o falla, el ciclo sigue con fallback rule-based y no se rompe.
- Variables nuevas:
- `LLM_ENABLED`, `LLM_PROVIDER`, `LLM_API_KEY`, `LLM_MODEL`, `LLM_TIMEOUT_SECONDS`.

### Nota de resiliencia (MVP)

- Si el proveedor LLM está deshabilitado o falla por timeout/error, el flujo **no** corta el ciclo.
- En ese caso, la recomendación estructurada sigue saliendo por reglas (rule-based) y los campos
  `news_summary` / `recommendation_explanation_llm` pueden venir en `null`.
