# ML en Telemetría OTT — Análisis del CSV y plan de acción

## 1) Resumen ejecutivo
El dataset adjunto (`telemetryrecords_2026-04-21_13-59_UTC.csv`) contiene **eventos OTT** donde:
- `actionId=7` representa **inicio** de stream.
- `actionId=8` representa **fin/stop** con `duration`.

Con esto se pueden construir **sesiones de consumo** por usuario/dispositivo/canal y, a partir de ahí, habilitar 3 líneas de ML útiles:
1) **Predicción de watch-time futuro** por usuario (regresión).
2) **Predicción del próximo canal** (clasificación / ranking).
3) **Riesgo de abandono (churn)** (clasificación) usando definiciones operativas basadas en inactividad.

El plan recomendado es empezar con (1) o (2) porque requieren menos supuestos de negocio que churn y se validan rápido.

---

## 2) Estructura y calidad del CSV (EDA “rápida”)

### 2.1 Formato
- **Delimitador**: `;`
- **Filas**: ~**655,229**
- **Columnas**: **26**
- Campo `data`: string con **JSON embebido** (ej: `streamId`, `streamName`, `duration`, `appId`, `appName`).

### 2.2 Distribución de `actionId`
Los eventos están dominados por OTT:
- `7`: ~**324,188**
- `8`: ~**324,187**
- Otros (19/20/9/10/16/17) aparecen mucho menos.

### 2.3 Rango temporal y cardinalidades
- `timestamp` mínimo: **2024-12-28**
- `timestamp` máximo: **2026-12-31** *(ojo: hay timestamps “futuros” respecto a hoy; esto debe normalizarse en pipeline)*
- `subscriberCode` distintos: **60**
- `deviceId` distintos: **1,725**
- `dataId` (streamId) distintos: **261**
- `dataName` distintos: **255**

### 2.4 Campos con nulos altos / poco valor predictivo (en este extract)
Varios campos están al **100% null** en el CSV:
`actionKey`, `anonymized`, `date`, `whoisCountry`, `whoisIsp`, `ipId`, `dataTsId`, `dataSeviceId`, `dataNetId`, `reasonKey`, `profileId`, `ip`.

Recomendación: no usarlos como features por ahora; mantenerlos solo si en PanAccess real se poblaran en otro ambiente.

### 2.5 `dataDuration` / `duration` (target natural)
El JSON de `data` trae `duration` en muchos eventos (típicamente en `actionId=8`).
Estadísticas rápidas (muestra grande):
- `p50` ~ 2,100s (35 min)
- `p95` ~ 62,721s
- `max` muy alto (posibles outliers / sesiones “largas” o errores)

Esto lo hace un **target excelente** para modelos de watch-time y para features de “consumo”.

---

## 3) Qué variables sirven como features (parámetros/condiciones)

### 3.1 Identificadores / agrupación
- **Usuario**: `subscriberCode`
- **Dispositivo**: `deviceId` (puede ser proxy de household / multi-device)
- **Canal/stream**: `dataId` (streamId) y `dataName` (streamName)
- **Tiempo**: `timestamp`, `dataDate`, `timeDate` (en backend ya se derivan)

### 3.2 Features temporales (muy predictivas)
Derivadas de `timestamp`/`dataDate`/`timeDate`:
- día de semana, fin de semana, hora del día (peak hours)
- recencia: minutos/días desde la última sesión
- estacionalidad semanal

### 3.3 Features de comportamiento (por usuario y ventanas móviles)
Sobre sesiones OTT (actionId 7/8 “mergeadas”):
- total watch-time últimos 1/7/14/30 días
- # sesiones (views) por ventana
- duración media/mediana, p90 de duración
- diversidad: # canales únicos vistos en ventana
- top canal y “share” del top canal
- regularidad (días activos en ventana)
- hora típica de consumo (centroide / histograma)

### 3.4 Features por canal
Para modelos de recomendación/próximo canal:
- popularidad global del canal por día/hora
- popularidad por segmento (si existe segmentación posterior)
- transición canalA→canalB (matriz de transición por usuario)

### 3.5 Cuidado con leakage
Si el target es “watch-time futuro” o “próximo canal”, NO usar eventos posteriores al punto de predicción.
Siempre split temporal.

---

## 3.6 Métricas/analítica base que deben existir (antes y durante ML)
Estas métricas son clave porque:
- sirven para **monitoreo del producto** (picos/valles, rotación),
- sirven para **features** y **labels** (ML),
- y ayudan a detectar drift y anomalías.

### A) Canales más vistos (y watch-time)
**Qué calcular** (por día y por franja horaria opcional):
- `views` por canal (conteo de sesiones)
- `watch_time` por canal (sum(dataDuration))
- `unique_users` por canal

**Cómo se usa en ML**
- Feature global: popularidad del canal por día/hora.
- Feature por usuario: % de consumo del top canal, diversidad.
- Baseline recomendación: ranking por popularidad + ajuste por historial del usuario.

### B) Frecuencia de consumo: picos y valles
**Qué calcular**
- distribución por `timeDate` (0–23) y día de semana
- serie temporal de `views` y `watch_time` (diario/semanal)
- detección de picos/valles (z-score simple o percentiles)

**Cómo se usa en ML**
- Features temporales fuertes (hora/día) para “próximo canal” y watch-time.
- Señal para anomalías operativas (audiencia cae/sube anormal).

### C) Rotación de usuarios (retención/actividad)
Definir “usuario activo” por ventana:
- activo si tuvo ≥1 sesión OTT en los últimos N días (ej: 7).

**Qué calcular**
- usuarios activos diarios/semanales (DAU/WAU)
- cohortes de retención (D1/D7/D30)
- recencia y frecuencia por usuario

**Cómo se usa en ML**
- Label base para churn operativo (inactividad futura).
- Features: recency/frequency, días activos, tendencia (sube/baja consumo).

### D) Rotación de canales (catálogo dinámico)
**Qué calcular**
- canales “entrando” al top-N vs “saliendo” del top-N por semana
- share de consumo del top-10 vs long tail
- tasa de novedad: canales nuevos vistos por usuario en ventana

**Cómo se usa en ML**
- Feature de exploración vs rutina del usuario.
- Control de recomendación: evitar sesgo a canales con alta rotación estacional.

## 4) Casos de uso ML priorizados (MVP → avanzado)

### Caso A — Watch-time futuro por usuario (MVP recomendado)
- **Input**: features agregadas de los últimos N días.
- **Target**: minutos/segundos vistos por `subscriberCode` en los próximos 7 días.
- **Tipo**: regresión.
- **Modelos baseline**:
  - Ridge/ElasticNet
  - HistGradientBoostingRegressor
  - (opcional) XGBoost/LightGBM si agregas dependencia
- **Métricas**: MAE, RMSE, MAPE (con cuidado), y calibración por segmentos.

### Caso B — Próximo canal probable (recomendación simple)
- **Input**: últimas K sesiones + features agregadas + canal actual.
- **Target**: próximo `dataName` (top-1 o top-k).
- **Tipo**: clasificación multiclase o ranking heurístico (Markov + features).
- **Modelos**:
  - baseline Markov (transiciones)
  - Logistic Regression / LinearSVC con hashing
  - Gradient boosting sobre features tabulares
- **Métricas**: top-k accuracy, MAP@K.

### Caso C — “Churn” operativo (definición por inactividad)
- **Definición** (ejemplo): churn=1 si no hay sesiones OTT en los siguientes 14/30 días.
- **Tipo**: clasificación binaria.
- **Métricas**: ROC-AUC + PR-AUC (imbalance), recall@precision.

---

## 5) Plan de implementación en el proyecto (pasos concretos)

### 5.1 Fase 0 — Decisiones y contrato de datos (1 día)
- Definir **primer objetivo**: A, B o C.
- Definir el “**punto de predicción**” (cutoff time) y horizonte (7/14/30 días).
- Congelar features iniciales (lista mínima).

### 5.2 Fase 1 — Dataset builder desde BD local (2–3 días)
Usar `MergedTelemetricOTTDelancer` como fuente “silver”.
- Crear módulo `ml/` (o app Django `apps/ml/`).
- Implementar comando:
  - `python manage.py ml_build_dataset --start YYYY-MM-DD --end YYYY-MM-DD`
  - genera dataset tabular por usuario (y opcional por usuario+canal).
- Guardar el dataset como:
  - CSV/Parquet en disco (dev), o
  - tabla `ml_training_snapshot` (prod) si prefieres todo en DB.

### 5.3 Fase 2 — Entrenamiento baseline + versionado (2–4 días)
- Comando:
  - `python manage.py ml_train --task watch_time_7d`
- Pipeline:
  - split temporal (train/val/test)
  - imputación/escala (si aplica)
  - modelo baseline
  - métricas + artefactos (joblib/pickle) con “model_id”
- Registrar metadata:
  - fecha, rango de entrenamiento, features, métricas, hash del código.

### 5.4 Fase 3 — Serving en backend (1–2 días)
Según necesidad:
- **Offline**: guardar predicciones diarias en tabla `ml_predictions_user_daily`.
- **API**: endpoint `POST /delancert/ml/predict/watch-time/` (RO o RW según política).

### 5.5 Fase 4 — Automatización (1 día)
Programar:
- `ml_train` semanal o cuando haya drift
- `ml_predict` diario
en Linux: cron/systemd timers (como ya hicimos con telemetry).

---

## 6) Integración con lo ya implementado
Tu backend ya tiene:
- ingesta incremental + merge OTT
- auditoría `TelemetryJobRun`
- ops/alerts + ops/summary
- agregados “gold” por día

ML debe “engancharse” a esto así:
- dataset builder usa `MergedTelemetricOTTDelancer` y/o `TelemetryUserDailyAgg`
- training/prediction generan sus propios `TelemetryJobRun` (nuevo `JobType` recomendado: `ML_TRAIN`, `ML_PREDICT`)
- health/ops puede incorporar señales ML (último modelo, métricas, drift simple)

---

## 7) Recomendación concreta (para empezar ya)
Empezar por **Watch-time 7 días (Caso A)**:
- dataset por `subscriberCode` usando features de `TelemetryUserDailyAgg` (rápido)
- target: sum(watch_seconds) próximos 7 días (desde merged OTT)
- serving: offline + endpoint opcional

