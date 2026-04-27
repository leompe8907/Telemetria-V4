# Deuda técnica (pendientes) – Telemetría OTT Backend

## 1) Mensaje genérico de DRF en 403
**Síntoma**: cuando falta la API key o es inválida, algunas respuestas devuelven:

```json
{"detail":"Authentication credentials were not provided."}
```

**Causa**: el esquema anterior usaba API key a nivel de `permission_classes` (no autenticación DRF),
por lo que Django REST Framework podía responder con un mensaje genérico.

**Estado**: RESUELTO ✅

**Propuesta**:
- Implementado `TelemetryApiKeyAuthentication` (DRF). Ahora responde **401** con mensajes claros:
  - `Missing API key.`
  - `Invalid API key.`
  - y mantiene scopes RO/RW.

## 2) Smoke tests locales dependen del entorno del servidor
**Síntoma**: una prueba HTTP local puede fallar con 403 si:
- el servidor (daphne) está corriendo con variables de entorno distintas a las del `.env`,
- o no se reinició el proceso tras cambios de `.env`.

**Impacto**: bajo, pero puede generar falsos negativos.

**Estado**: RESUELTO ✅

**Qué se hizo**:
- Se agregaron **tests automáticos** en `delancert/tests.py` (sin HTTP real) para validar:
  - Auth API key devuelve **401** con mensajes claros (`Missing API key.`, `Invalid API key.`)
  - Permisos RO/RW (RO no puede escribir → 403)
  - Rate limit (2da llamada bloquea)
  - Parseo de fechas ISO (YYYY-MM-DD)
  - `telemetry/run` crea `TelemetryJobRun` usando **mocks** (sin PanAccess)
- Recomendación operativa mantenida: si cambias `.env`, **reinicia daphne** para que tome nuevas variables.

## 3) Conflictos de dependencias en el entorno
Durante `pip install -r requirements.txt` pueden aparecer warnings de conflictos si el venv contiene paquetes extra
(ej. `django-q`, `django-timezone-field`) no usados por este proyecto.

**Impacto**: medio en mantenimiento (ambiente “sucio”), bajo en runtime si `manage.py check` es OK.

**Estado**: RESUELTO ✅

**Qué se hizo**:
- Se ejecutó `pip check` y se resolvieron conflictos eliminando paquetes no usados:
  - `django-q`, `django-timezone-field`, `django-celery-beat`
- `pip check` quedó en **OK** (“No broken requirements found”).
- Se regeneró `requirements.txt` en formato legible/estable.

## 4) Próximo trabajo recomendado
**Estado**: RESUELTO ✅ (tests mínimos)

**Qué se hizo**:
- Implementados y ejecutados tests mínimos (ver punto 2).
- Auditoría de jobs ya estaba implementada; auditoría adicional para `sync/merge` individuales queda como mejora opcional.

## 5) PanAccess: “sin permisos” cuando caduca la sesión
**Síntoma**: después de un tiempo sin actividad, PanAccess puede responder con texto tipo “You do not have the permission…”
aunque la causa real sea un `sessionId` caducado.

**Estado**: RESUELTO ✅

**Qué se hizo**:
- Se añadió una heurística en `delancert/server/telemetry_fetcher.py`:
  - Si **todas** las funciones candidatas fallan con `no_access_to_function`, se fuerza `reset_session()` y se reintenta **1 vez**.
  - Evita loops infinitos y recupera el flujo cuando el mensaje de PanAccess es ambiguo.
- Se agregó test con mocks en `delancert/tests.py` para validar que:
  - se llama `reset_session()` **una sola vez**
  - luego la llamada puede continuar si PanAccess vuelve a responder OK.

