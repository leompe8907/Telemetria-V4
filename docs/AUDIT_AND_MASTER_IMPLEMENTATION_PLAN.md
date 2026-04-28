# Auditoría y Plan Maestro de Implementación — Telemetría OTT (Django)

## Resumen ejecutivo (técnico–ejecutivo)
El repositorio está **bien encaminado** hacia una plataforma de telemetría moderna: ya existe un *data plane* coherente (raw → merged → gold), una base de **operación auditable** (`TelemetryJobRun`), un plan de **Dashboard API** con performance/caching, y un **pipeline ML baseline** (dataset builder + entrenamiento + versionado de artefactos).

Para cerrar la visión de “telemetría avanzada de alta disponibilidad + ML + Agentes de IA”, el trabajo crítico se concentra en:

- **Escalabilidad operativa**: mover jobs (sync/merge/agregados/ML/insights) a **Celery + Redis** con políticas de concurrencia/locks, y medir SLOs.
- **Tiempo real**: decidir explícitamente entre **near-real-time** (cache+invalidación) vs **push** (Channels/SSE) y configurar ASGI en consecuencia.
- **IA auditable**: incorporar Agentes como “operadores” asistidos (NOC/Analista/ML-Ops) **fuera del request** (workers), con guardrails, auditoría y sin PII en prompts.

Este documento consolida la **auditoría/diagnóstico** y el **plan maestro por sprints** para ejecutar con Redis, Celery y PostgreSQL.

---

## Alcance y objetivos del sistema
- **Ingesta**: eventos OTT desde PanAccess (y/o batch) con idempotencia, dedupe y reintentos.
- **Persistencia**: PostgreSQL como sistema de registro; índices diseñados para analytics y filtros temporales.
- **Analytics & Dashboard**: endpoints DRF orientados a KPIs, rankings y series temporales con caching.
- **ML**: entrenamiento, versionado, batch scoring y detección de anomalías.
- **Agentes de IA**: diagnóstico operativo y asistencia analítica (explicaciones/reportes), bajo políticas estrictas.
- **Alta disponibilidad**: jobs resilientes, observabilidad completa, despliegue reproducible y control de degradación.

---

## Estado actual (evidencia del repositorio)

### Datos: modelos (raw/merged/gold) y performance
- **Raw**: `TelemetryRecordEntryDelancer` (tabla principal), con múltiples índices compuestos por `actionId`, `timestamp`, `deviceId`, `dataDate`, `subscriberCode`, `dataName`.
- **Merged**: tablas especializadas (OTT/DVB/VOD/Catchup) para lecturas rápidas y consultas por fecha/hora.
- **Gold (agregados)**: `TelemetryChannelDailyAgg` y `TelemetryUserDailyAgg` para acelerar el dashboard y servir de base para features ML.

**Lectura arquitectónica**: es un diseño correcto para escalar: *raw* conserva el detalle, *merged* estabiliza la semántica de eventos, *gold* reduce costo de queries.

### Operación y auditoría
- Modelo `TelemetryJobRun` para registrar runs (sync/merge/integrity/ML build dataset/train).
- Logging con rotación y filtros “unicode safe” configurado en `backend/settings.py`.

### Cache/Redis
- `django-redis` disponible y `CACHES` se activa por `REDIS_URL` (fallback a `LocMemCache`).

### ML (baseline ya implementado)
- `ml_build_dataset`: construye CSV tabular desde agregados por usuario y target futuro (watch-time).
- `ml_train`: entrena baseline robusto tabular (`HistGradientBoostingRegressor` + `TransformedTargetRegressor(log1p)`), guarda `model.joblib` + `metrics.json` + `feature_names.json`.

**Conclusión**: hay un “ML v0” real, *deployable* como batch/servicio con trabajo adicional en split temporal y serving.

### Tiempo real / WebSockets
El proyecto incluye dependencias (`channels`, `daphne`, `twisted`, `autobahn`), pero actualmente se observa configuración basada en WSGI y sin activación explícita de ASGI/Channels. Esto sugiere que “tiempo real” aún no está cerrado como decisión de arquitectura.

---

## Auditoría y diagnóstico (protocolo)
Antes de introducir cambios, esta es la inspección recomendada para asegurar escalabilidad/HA:

### 1) Contrato de datos y consistencia temporal
- Validar definición de:
  - `timestamp` (DateTime) vs `dataDate` (Date) vs `timeDate` (hora o entero).
  - Zona horaria (UTC/local) y normalización.
- Confirmar idempotencia:
  - `recordId` único y estrategia de *upserts/ignore_conflicts*.
  - reintentos sin duplicados y sin inconsistencias en merges.
- Revisar “late arrivals”:
  - política de backfill (ventana) y detección de desfase.

### 2) Performance de queries
- Identificar endpoints “calientes” del dashboard y su plan de ejecución:
  - índices usados y cardinalidades.
  - `iterator()`/paginación para evitar cargas masivas.
- Definir “hot path” con cache:
  - TTL por endpoint, y (si hace falta) invalidación por versión post-run.

### 3) Resiliencia operacional (jobs)
- Revisar:
  - *locks* (evitar runs solapados),
  - límites por batch,
  - reintentos con backoff y circuit breakers (PanAccess).
- SLOs mínimos:
  - duración esperada de `sync`, `merge`, `aggregates`, `ml_build_dataset`, `ml_train`.
  - tasa de error tolerable y degradación controlada.

### 4) Seguridad (API)
- Revisión de:
  - autenticación (API keys RO/RW), mensajes de error, y “least privilege”.
  - rate limiting para endpoints operativos/IA.
  - tratamiento de PII (`subscriberCode`, `deviceId`, `smartcardId`).

### 5) IA (riesgos y gobernanza)
- Verificación de:
  - leakage temporal y splits (deben ser temporales, no shuffle).
  - drift (data & concept drift) y señales mínimas.
  - auditoría de prompts/respuestas (si se integra LLM).

### 6) Tiempo real (decisión de arquitectura)
Definir explícitamente:
- **Opción A — near-real-time**: cache Redis + refresh por jobs + endpoints rápidos (recomendado para MVP HA).
- **Opción B — push**: Channels (WebSockets) o SSE para notificaciones/series; requiere ASGI, scaling y backpressure.

---

## Viabilidad de IA (ML + Agentes) y cómo integrarla

### ML (casos de uso priorizados)
1) **Detección de anomalías (operación)** — prioridad alta
   - señales: caídas de audiencia/watch-time por canal, spikes de errores, cambios abruptos por franja horaria.
   - técnicas: umbrales adaptativos + detector estadístico, y/o Isolation Forest sobre agregados.

2) **Regresión de watch-time futuro (MVP ML)** — ya encaminado
   - features: agregados por usuario (views, watch_seconds, días activos, diversidad).
   - target: watch-time futuro (7/14 días).
   - modelo: baseline tabular (sklearn) + split temporal.

3) **Próximo canal / recomendación simple (fase posterior)**
   - baseline Markov (transiciones) + ranking por popularidad y horario.

### Agentes de IA (rol y guardrails)
Recomendación: Agentes como “capa de decisión asistida” y no como actor directo.

- **Agente NOC (Ops)**:
  - entrada: `TelemetryJobRun`, salud, lag, métricas de anomalías.
  - salida: diagnóstico, severidad, recomendación y playbook.
- **Agente Analista**:
  - entrada: agregados (gold) y series.
  - salida: explicación en lenguaje natural, insights y “qué cambió” (sin PII).
- **Agente ML-Ops**:
  - entrada: drift y métricas.
  - salida: recomendación de reentrenar, y validación post-train.

**Integración en Django**:
- Nunca correr LLM/agentes dentro del request crítico; usar **Celery workers**.
- Persistir resultados en una entidad “AI Insights” (tabla) o cache con TTL.
- Auditar cada ejecución (inputs agregados, decisiones, coste/latencia, prompts redacted).

Framework sugerido:
- **Semantic Kernel** o **LangChain**, priorizando una capa propia de “tools”/políticas para minimizar lock-in.

---

## Arquitectura objetivo (HA + escalable)

### Componentes
- **Django + DRF**: API de lectura (dashboard) y control (operativo) con API keys RO/RW.
- **PostgreSQL**: sistema de registro + tablas gold/materializaciones.
- **Redis**:
  - cache de dashboard,
  - rate limiting,
  - locks de jobs,
  - broker/backing para Celery.
- **Celery**:
  - `telemetry_sync`, `merge_ott`, `build_aggregates`,
  - `ml_build_dataset`, `ml_train`, `ml_predict`,
  - `ai_ops_insights`, `ai_analytics_summary`.
- **(Opcional) Channels/SSE**: sólo si “push” aporta valor claro en el dashboard.

### Principios de diseño
- **Separación de responsabilidades** (servicios vs API vs tasks).
- **Idempotencia por defecto** en jobs.
- **Observabilidad first-class** (métricas y runs auditables).
- **PII minimization** en analytics y en IA.

---

## Plan maestro por etapas (Sprint-based)

### Sprint 0 — Estabilización y baseline HA (1 semana)
- **Datos**
  - Validar contrato temporal (UTC/local) y reglas de normalización.
  - Revisar índices vs queries reales del dashboard.
- **Operación**
  - Estándar de SLOs: duración/throughput/errores por job.
  - Locks anti-solapamiento y política de reintentos.
- **Seguridad**
  - API keys RO/RW en todos los endpoints sensibles.
  - Rate limiting para endpoints operativos.
- **Calidad**
  - tests mínimos (idempotencia, auth, parse fechas, jobs mock).

### Sprint 1 — Orquestación moderna (Celery + Redis) (1 semana)
- Migrar jobs críticos a Celery:
  - sync/merge/agregados como tareas con parámetros y trazabilidad.
- Implementar scheduling con Celery Beat.
- Definir colas/concursos (ej: `telemetry_high`, `telemetry_low`, `ml`, `ai`).

### Sprint 2 — Dashboard v1 “rápido” (1 semana)
- Endpoints MVP (overview/top/temporal/user).
- Cache Redis por TTL + versionado simple opcional al finalizar un run.
- Budget de latencia:
  - cache caliente: < 300–800ms,
  - cache frío: queries sin cargas masivas.

### Sprint 3 — ML v1 productizable (2 semanas)
- Split temporal real (train en pasado → validación → test futuro).
- Registro simple de modelos (metadata, rango temporal, features, métricas, artefactos).
- Batch scoring (diario) y persistencia de predicciones.
- Anomalías v1 sobre series agregadas (alertas).

### Sprint 4 — Agentes v1 (1 semana)
- Agente NOC (ops) que produce insights y playbooks a partir de:
  - runs, lag, anomalías, errores recurrentes.
- Agente Analista para narrativas del dashboard (solo agregados).
- Guardrails:
  - allowlist de tools, límites de costo, auditoría, no PII.

### Sprint 5 — DevOps/Operación HA (1 semana)
- CI/CD (tests, lint, migraciones).
- Monitoreo de salud:
  - métricas + alarmas por lag/errores.
- Estrategia de despliegue:
  - multi-worker (Django), workers Celery escalables, Redis gestionado, backups Postgres.

---

## Decisiones pendientes (bloqueantes)
1) **Tiempo real**:
   - near-real-time (cache+refresh) vs push (Channels/SSE).
2) **Estrategia de serving ML**:
   - offline batch + endpoint RO, o predicción on-demand (con cache).
3) **Nivel de autonomía del Agente**:
   - “recomendación” vs “ejecución automática” (recomendado iniciar en recomendación + aprobación).

---

## Riesgos principales y mitigaciones
- **Leakage temporal en ML**: forzar split temporal y point-in-time features.
- **PII en IA**: prompts solo con agregados; masking y auditoría.
- **Jobs solapados**: locks Redis + idempotencia + backfills acotados.
- **Crecimiento de volumen**: tablas gold/materializadas + particionado futuro si aplica + cache.

---

## Entregables (artefactos) esperados
- “Ops pack”: métricas, runs, alertas y playbooks.
- “ML pack”: dataset builder, train, registry, batch scoring, drift.
- “Agent pack”: insights auditables + endpoints RO para consumir insights.
- “Dashboard pack”: endpoints rápidos + cache + (opcional) streaming.

