# Runbook — Celery + Redis (Telemetría OTT Backend)

## Objetivo
Habilitar ejecución **asíncrona y programada** (HA-friendly) para:
- `telemetry_run` (sync + merge OTT)
- `build_aggregates` (materialización diaria)

Sin romper el modo actual por CLI/Task Scheduler: Celery es **opcional**.

---

## Prerrequisitos
- Redis accesible (local o remoto)
- Variables de entorno configuradas en `.env`

Variables mínimas:
- `REDIS_URL=redis://localhost:6379/0`

Opcional (si quieres separar broker/backend):
- `CELERY_BROKER_URL=redis://localhost:6379/0`
- `CELERY_RESULT_BACKEND=redis://localhost:6379/0`

---

## Activar schedules (Celery Beat)
Por defecto Beat está deshabilitado. Para habilitarlo:

- `CELERY_ENABLE_BEAT=1`

Schedules configurables:
- `CELERY_SCHEDULE_TELEMETRY_RUN_SECONDS=600` (cada 10 min)
- `CELERY_SCHEDULE_BUILD_AGG_SECONDS=86400` (cada 24h)

Parámetros del job `telemetry_run` (opcional):
- `TELEMETRY_RUN_LIMIT=1000`
- `TELEMETRY_RUN_BATCH_SIZE=1000`
- `TELEMETRY_RUN_PROCESS_TIMESTAMPS=1`
- `TELEMETRY_RUN_MERGE_BATCH_SIZE=500`
- `TELEMETRY_RUN_BACKFILL_LAST_N=0`

Parámetros del job de agregados (opcional):
- `TELEMETRY_AGG_DAYS=7`

---

## Ejecutar local (Windows / PowerShell)
Desde la carpeta `backend/` (donde está `manage.py`):

### 1) Levantar Redis
Si usas Redis en Docker:

```powershell
docker run --name telemetria-redis -p 6379:6379 redis:7
```

### 2) Worker Celery

```powershell
celery -A backend worker -l info -P solo
```

Notas:
- En Windows suele ser más estable usar `-P solo`.
- Si `REDIS_URL` no está configurado, Celery no podrá conectarse al broker.

### 3) Beat (scheduler)

```powershell
celery -A backend beat -l info
```

---

## Ejecutar manualmente tareas (sin Beat)
Puedes disparar tareas desde Django shell:

```powershell
python manage.py shell
```

```python
from delancert.tasks import telemetry_run_task, telemetry_build_aggregates_task

telemetry_run_task.delay(limit=1000, batch_size=1000, merge_batch_size=500, backfill_last_n=0)
telemetry_build_aggregates_task.delay(days=7)
```

---

## Observabilidad y auditoría
- Cada tarea registra un `TelemetryJobRun`.
- Para diagnóstico:
  - `GET /delancert/ops/alerts/`
  - `GET /delancert/ops/summary/`
  - `GET /delancert/jobs/runs/?limit=20`

---

## Checklist de producción (mínimo)
- Redis gestionado (persistencia/HA según necesidad).
- `CELERY_TASK_TIME_LIMIT` y `CELERY_TASK_SOFT_TIME_LIMIT` ajustados.
- Locks activos (via cache/Redis) para evitar solapamiento.
- Alertas por lag y fallos consecutivos.

