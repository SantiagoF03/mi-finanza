# AGENTS.md

## Objetivo del proyecto
Este repositorio implementa un MVP de inversión semiautomática para IOL en modo read-only.
La app:
- lee cartera real o mock,
- analiza portfolio,
- genera recomendaciones,
- requiere aprobación o rechazo manual,
- no ejecuta operaciones reales.

## Reglas permanentes
- NO implementar compra/venta real ni envío de órdenes, salvo que el prompt lo pida de forma explícita.
- Mantener IOL en modo solo lectura por defecto.
- No hardcodear símbolos como AAPL/MSFT/SPY en recomendaciones.
- Los símbolos recomendados deben salir siempre del snapshot real o del analysis derivado de ese snapshot.
- Si `/api/recommendations/current` devuelve 404, eso significa “no hay recomendación abierta”, no “backend caído”.
- Mantener diffs chicos y enfocados: un tema por cambio.
- No romper estados existentes: `pending`, `blocked`, `approved`, `rejected`, `superseded`.
- No romper reglas hard ni whitelist.
- Si falta una integración externa, usar fallback claro y documentado.

## Backend
- Priorizar cambios en:
  - `backend/app/services/orchestrator.py`
  - `backend/app/recommendations/engine.py`
  - `backend/app/broker/clients.py`
  - `backend/app/api/routes.py`
- Mantener compatibilidad con SQLite del MVP.
- Si se agregan columnas o cambia esquema, documentar si hace falta resetear DB local.
- Evitar respuestas ambiguas en endpoints: devolver JSON explícito para skipped/cooldown/fallback.

## Frontend
- Tratar estados vacíos como estados válidos de UI.
- No mostrar “backend no disponible” cuando hay 404 esperables.
- Mostrar mensajes claros de cooldown, estados vacíos y bloqueos por reglas.

## Testing
Después de cambios backend:
- correr `pytest -q`
- si falla un test desactualizado, corregirlo según el comportamiento real implementado
- no dejar tests rotos

## Documentación
- Actualizar README cuando cambie el contrato de endpoints, variables `.env`, o comportamiento visible en UI.
- Mantener la explicación simple y orientada al uso local del MVP.

## Forma de entrega
Respondé siempre con:
- Cambios aplicados
- Archivos tocados
- Cómo probarlo en 5 minutos
- Riesgos/pendientes
