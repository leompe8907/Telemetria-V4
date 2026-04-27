# Telemetría OTT Backend – Documento del Proyecto

## Objetivo
Construir una plataforma **modular y escalable** para:

- **Ingestar** telemetría de una aplicación OTT (eventos de reproducción, acciones, métricas).
- **Persistir** eventos normalizados en PostgreSQL.
- **Exponer** un API (Django REST Framework) para:
  - dashboards (KPIs, tendencias, consumo por canal/dispositivo/suscriptor),
  - sincronización/consulta de datos desde un servidor externo (PanAccess),
  - predicciones (ML) y reportes.
- **Entrenar y servir modelos ML** (Scikit-Learn y/o TensorFlow) para analítica predictiva.
- **Automatizar** pipelines (Celery + Redis) para re-entrenamiento y generación de reportes.
- Integrar **LLM** (OpenAI/Anthropic u otro) para:
  - traducción dinámica de UI/labels (cuando aplique),
  - explicaciones en lenguaje natural sobre gráficos y anomalías (sin exponer PII).

## Contexto de datos (CSV de referencia)
Se trabajó con un dataset de telemetría exportado a CSV con:

- Separador `;`
- Columna `data` con JSON embebido (ej. `streamId`, `streamName`, `duration`)
- Muchas columnas enriquecidas (IP/whois/keys) que pueden venir vacías según el export.

Implicación: el diseño prioriza un **modelo de eventos normalizado** + un **feature store ligero** para ML.

## Estado actual del repositorio
El backend ya tiene:

- **Django + DRF** configurado.
- **PostgreSQL** por variables de entorno.
- **CORS** habilitado (middleware).
- Cliente hacia **PanAccess** con reintentos y manejo de sesión:
  - `delancert/server/panaccess_client.py`
  - `delancert/server/panaccess_singleton.py`
  - `delancert/server/auth.py`
- Endpoint:
  - `GET/POST /api/telemetry/sync/` (según implementación de `TelemetrySyncView`)

Además, se ajustó configuración para:

- Parsear `DEBUG` como boolean real.
- Variables de CORS/CSRF por `.env`.
- Plantilla `.env.example`.

## Alcance funcional (qué se quiere implementar)

### 1) Telemetría (Data Layer)
- **Ingesta** desde:
  - servidor externo (PanAccess u otro API),
  - cargas batch (CSV/archivos),
  - eventualmente eventos en tiempo real (opcional).
- **Normalización**:
  - parseo de JSON en `data`,
  - tipado consistente,
  - deduplicación por `recordId`,
  - timestamps coherentes (UTC).
- **Persistencia**:
  - tabla de eventos (`TelemetryEvent`) con índices por `created`, `device_id`, `subscriber_code`, `stream_id`.

### 2) Reporting / Dashboard
- KPIs: consumo total, sesiones, duración media/mediana, top canales, distribución por hora/día, early exits.
- Agregados materializados (para performance) + cache en Redis para endpoints “calientes”.

### 3) Machine Learning (Predictive Analytics)
Objetivos iniciales recomendados (dependen de calidad de telemetría disponible):

- **Regresión de duración**: predecir `duration` esperada dado contexto (stream, hora, historial reciente).
- **Clasificación de abandono temprano**: `duration < umbral` (p. ej. 60–120s).
- **Detección de anomalías**: outliers de duración/patrones de eventos.
- (Fase posterior) **Next-content**: sugerencia del siguiente canal/stream por historial.

### 4) Automatización (Orquestación)
- **Objetivo inmediato**: automatizar la sincronización desde PanAccess y el merge OTT (actionId 7/8)
  para que el dashboard/consultas trabajen contra tablas locales.

#### Opción A (rápida en Windows): Task Scheduler + comando Django
Se agregó un comando de management para poder programarlo sin depender de Celery:

```bash
python manage.py telemetry_sync --limit 1000 --batch-size 1000 --merge-ott --merge-batch-size 500
```

- **Qué hace**:
  - descarga registros nuevos desde PanAccess,
  - guarda en `TelemetryRecordEntryDelancer`,
  - ejecuta el merge OTT y guarda en `MergedTelemetricOTTDelancer`.

Esto se puede ejecutar cada X minutos usando **Windows Task Scheduler**.

#### Opción B (escalable): Celery + Redis (backlog)
Cuando se requiera mayor throughput / reintentos robustos / jobs pesados:

- Celery Beat:
  - sync de telemetría (cada 5–15 min),
  - merge OTT (cada 5–15 min, o inmediatamente después del sync),
  - refresh de agregados/reportes.
- Worker Celery:
  - ETL pesado, ML training, batch scoring.

### 5) LLM (Traducción + Explicaciones)
- **Traducción**: solo para strings dinámicos / explicaciones; UI base debe usar i18n tradicional.
- **Explicación de gráficos**:
  - enviar al LLM solo agregados (nunca PII raw),
  - caching + auditoría de prompts/respuestas,
  - rate limiting + validación de inputs (OWASP).

## Arquitectura propuesta (modular)

### Capas
- **API (DRF)**: serializers/views/permissions.
- **Servicios (domain/services)**:
  - `telemetry.services`: ingesta, normalización, reporting.
  - `ml.services`: features, entrenamiento, inferencia, registry.
- **Persistencia (models)**: tablas Django para eventos, reportes y registry de modelos.
- **Async jobs (Celery)**: tareas programadas y on-demand.

### Estructura sugerida de carpetas
Recomendación (sin romper el estado actual; puede evolucionar):

```
backend/
  backend/                  # settings/urls/asgi/wsgi
  delancert/                # integración actual (PanAccess + endpoint sync)
  apps/
    telemetry/
      api/
      services/
      models.py
    ml/
      api/
      services/
      pipelines/
      models.py             # registry metadata
      tasks.py              # Celery tasks
  common/
    security/
    observability/
```

## Seguridad y calidad (Clean Code + SOLID + OWASP)
- **Configuración por entorno**: nunca hardcodear secretos; usar `.env` y/o secret managers.
- **Validación de inputs** en endpoints de sync/reportes/LLM.
- **Autenticación/autorización**: proteger endpoints críticos (sync, training, explain).
- **Rate limiting**: especialmente para endpoints que disparen jobs o LLM.
- **Logging seguro**: redactar tokens/sessionIds/credenciales; logs rotativos.
- **PII**: tratar `subscriberCode`, `deviceId`, `smartcardId` como sensibles.

## Roadmap recomendado (por fases)

### Fase 0 – Base estable (1–2 días)
- Modelos de telemetría y migraciones.
- Endpoint de ingesta/sync robusto (idempotente, validado, con paginado).
- CORS/CSRF correcto para el frontend.
- Tests básicos: parseo JSON `data`, dedupe, persistencia.

### Fase 1 – Reporting (3–7 días)
- Agregados por stream/hora/día.
- Endpoints para dashboard.
- Cache Redis para agregados.

### Fase 2 – ML v1 (1–2 semanas)
- Feature engineering (rolling windows).
- Entrenamiento automatizado (Celery).
- Registro de modelos (MLflow o registry simple).
- Endpoint de predicción + métricas de drift.

### Fase 3 – LLM (1 semana)
- Servicio de explicaciones + caching.
- Auditoría + políticas anti prompt-injection.
- Traducción dinámica (solo donde aplique).

## Guía rápida de ejecución (local)

### Requisitos
- Python 3.12+
- PostgreSQL (local o Docker)
- (Recomendado) Redis (para cache y/o Celery)

### Variables de entorno
- Crear `.env` basado en `.env.example` y completar valores.

Variables clave:
- `SECRET_KEY`, `DEBUG`, `ALLOWED_HOSTS`, `SALT`
- DB: `DB_*` (preferible) o `ENGINE/NAME/USER/PASSWORD/HOST/PORT`
- CORS/CSRF si hay frontend separado:
  - `CORS_ALLOWED_ORIGINS`, `CSRF_TRUSTED_ORIGINS`, `CORS_ALLOW_CREDENTIALS`

### Comandos
```bash
pip install -r requirements.txt
python manage.py migrate
python manage.py runserver

# sync manual (y merge OTT)
python manage.py telemetry_sync --merge-ott
```

## Próximos pasos inmediatos
- Revisar `TelemetrySyncView` (validación, auth, idempotencia, paginado).
- Definir modelo `TelemetryEvent` (silver) y agregados (gold).
- Añadir Celery + Redis si se habilitan pipelines automáticos.

## Operación (Health / Integridad)
- **Health (solo lectura)**: `GET /delancert/health/`
  - muestra `max_record_id` de tabla raw y tabla merged OTT, conteos últimas 24h y lag estimado.
- **Integrity check (CLI)**:

```bash
python manage.py telemetry_integrity_check --hours 24
```


