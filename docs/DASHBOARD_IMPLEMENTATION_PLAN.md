# Plan de implementación – Dashboard de Telemetría OTT

## Objetivo
Exponer un **Dashboard API** (Django REST Framework) para consultar métricas OTT de forma rápida, estable y escalable, usando datos locales ya normalizados:

- Tabla raw/silver: `TelemetryRecordEntryDelancer`
- Tabla materializada para OTT: `MergedTelemetricOTTDelancer` (actionId 7/8 fusionado)

El dashboard debe soportar:
- KPIs por rango de fechas
- Rankings (top canales)
- Series temporales (daily/hourly)
- Perfil de usuario/subscriber (y rango)
- Segmentación general (optimizada)

## Principios de diseño
- **Separación de responsabilidades**:
  - `delancert/analytics/*` → lógica de métricas (ORM/SQL)
  - `delancert/api/*` o `delancert/server/dashboard.py` → endpoints + validación + response formatting
- **Performance**:
  - queries con `aggregate/annotate` usando índices existentes (`dataDate`, `timeDate`, `dataName`, `subscriberCode`, `deviceId`, `recordId`)
  - cache de resultados para endpoints frecuentes
  - evitar cargas masivas a memoria (especialmente en análisis “general users”)
- **Seguridad**:
  - endpoints de analytics NO deben exponer PII crudo sin necesidad
  - rate limiting (futuro) para endpoints costosos
- **Idempotencia operacional**:
  - el pipeline `telemetry_sync` + `merge_ott` produce datos consistentes para lectura

## Scope v1 (MVP del Dashboard)

### Endpoints propuestos
Base path: `/delancert/dashboard/`

1) **Overview / KPIs**
- `GET /delancert/dashboard/overview?start=YYYY-MM-DD&end=YYYY-MM-DD`
  - total_views, unique_users, unique_devices, unique_channels
  - total_watch_hours, avg_duration, min/max duration
  - avg_views_per_day

2) **Top canales**
- `GET /delancert/dashboard/channels/top?start=...&end=...&limit=10`
  - channel, total_views, percentage (opcional)

3) **Audiencia por canal**
- `GET /delancert/dashboard/channels/audience?start=...&end=...`
  - unique_devices, unique_users, total_views, watch_hours

4) **Actividad temporal**
- `GET /delancert/dashboard/temporal?start=...&end=...&period=daily|weekly|monthly`
  - serie de views/horas por período

5) **Usuario (perfil)**
- `GET /delancert/dashboard/users/{subscriberCode}?start=...&end=...`
  - resumen, top canales, patrones horarios, dispositivos

6) **Usuario (rango detallado)**
- `GET /delancert/dashboard/users/{subscriberCode}/range?start=...&end=...`
  - daily activity, anomalías simples, comparación contra promedio global del rango

7) **Usuarios general (optimizado)**
- `GET /delancert/dashboard/users/general?start=...&end=...`
  - métricas agregadas (promedios)
  - segmentación aproximada (sin cargar todos los usuarios)
  - top usuarios (por horas/vistas/canales) usando query limitada

## Cache (v1)
Implementar utilidades tipo `cached_result()` usando `django.core.cache`.

### Reglas recomendadas
- **TTL 60–300s**: top channels, audience, overview
- **TTL 300–900s**: temporal breakdown / series
- **TTL 60–300s**: user profile (depende tráfico)

### Invalidación
En v1:
- invalidación por TTL (suficiente)
- opcional: versionado simple (incrementar versión de cache al terminar `telemetry_sync --merge-ott`)

Evitar `KEYS` en Redis en prod (costoso). Si se requiere invalidación, preferir:
- version prefix (`telemetria:v2:...`)
- o lista explícita de claves.

## Migración desde analytics legacy
Se migrarán funciones del proyecto anterior, ajustando:
- imports → `delancert.models.MergedTelemetricOTTDelancer`
- cache decorator → util actual del proyecto
- SQL específico MySQL/SQLite → simplificar a PostgreSQL/ORM

### Qué migrar primero (alto valor)
- `period_summary` / overview
- top channels + audience + peak hours
- user profile + user range

### Qué reescribir (performance)
- “general users analysis” para evitar `list(user_stats)` de todos los usuarios

## Plan de implementación (pasos)
1) **Crear módulo de cache** (`delancert/utils/cache_utils.py`) con `cached_result()`.
2) **Crear paquete analytics** (`delancert/analytics/`) y migrar:
   - `overview.py`, `channels.py`, `temporal.py`, `users.py`
3) **Crear endpoints DRF** (views) y mapear URLs:
   - `delancert/dashboard/...`
4) **Pruebas manuales**:
   - llamadas en Postman contra períodos cortos y largos
5) **Optimización iterativa**:
   - medir tiempos, agregar índices si falta alguno crítico
   - cache TTL tuning

## Criterios de “done”
- Endpoints responden en < 300–800ms con cache caliente (local).
- Con cache frío, queries razonables (sin cargar datasets completos a memoria).
- Respuestas JSON consistentes (tipos serializables).
- Manejo de errores claro (400 por parámetros inválidos, 500 por errores inesperados).

