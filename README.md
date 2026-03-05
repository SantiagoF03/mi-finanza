# Mi Finanza MVP (mock-first)

MVP de inversión semiautomática con foco en consistencia: analiza cartera, procesa noticias mock y genera recomendaciones con aprobación manual obligatoria.

## Stack
- Backend: Python 3.12, FastAPI, SQLAlchemy, APScheduler
- Frontend: React + Vite
- DB: SQLite (diseñado para migrar a Postgres)

## Flujo de estados de recomendación
- `pending`: recomendación activa y operable manualmente.
- `blocked`: recomendación degradada por reglas hard (no se muestra como recomendación normal).
- `approved`: cerrada por aprobación del usuario.
- `rejected`: cerrada por rechazo del usuario.
- `superseded`: reemplazada por una recomendación más nueva.

### Definición de “recomendación actual”
La recomendación actual es la más reciente en estado `pending` o `blocked`.
- Si existe una abierta, y se crea una nueva, la vieja pasa a `superseded`.
- Al aprobar/rechazar, la recomendación se cierra (`approved`/`rejected`).
- Si no hay abiertas, `/recommendations/current` devuelve 404.

## Hardening aplicado
- Idempotencia de trigger: cooldown configurable (`TRIGGER_COOLDOWN_SECONDS`) para evitar duplicados por clicks o reinicios.
- Scheduler con `coalesce`, `max_instances=1` y `replace_existing=True`.
- Reglas hard se aplican antes de persistir y quedan visibles en respuesta (`status`, `blocked_reason`, `rules_applied`).
- Frontend sin mocks locales: backend es única fuente de verdad; si backend cae, se muestra error explícito.

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

## Qué sigue siendo mock
- `MockBrokerClient`
- Pipeline de noticias mock
- Sin ejecución real de órdenes

## Qué quedó listo para integrar IOL real
- Interfaz `BrokerClient`
- `IolBrokerClient` placeholder
- Persistencia y estado de recomendaciones endurecido

## Limitaciones actuales
- Sin autenticación de usuarios
- Sin migraciones Alembic
- Sin proveedor real de noticias/LLM productivo
