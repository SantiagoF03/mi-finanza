# Arquitectura MVP endurecida

## Pipeline
1. Snapshot broker (mock/IOL adapter)
2. Persistencia snapshot + posiciones
3. Ingesta de noticias/eventos
4. Análisis de cartera
5. Generación de recomendación
6. Enforce reglas hard (antes de persistir)
7. Persistencia de recomendación + acciones + estado
8. Decisión manual (approved/rejected)

## Recomendación actual
- Query principal: última recomendación en `pending` o `blocked`.
- Reemplazo: nueva recomendación supersede abiertas previas.

## Idempotencia
- Cooldown temporal en trigger para evitar duplicados por doble ejecución.
- Scheduler configurado con una sola instancia y coalesce.

## Separación rule-based vs LLM
- Core de decisión y seguridad: deterministic/rule-based.
- LLM (futuro): solo resumen/redacción, jamás romper reglas hard.
