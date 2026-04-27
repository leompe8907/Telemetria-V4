from __future__ import annotations

import os
from typing import Optional, Iterable

from django.conf import settings
from rest_framework.permissions import BasePermission


def _get_env(name: str) -> Optional[str]:
    value = (os.getenv(name) or "").strip()
    return value or None


def _iter_expected_keys(names: Iterable[str]) -> list[str]:
    keys: list[str] = []
    for n in names:
        v = _get_env(n)
        if v:
            keys.append(v)
    return keys


class _BaseTelemetryApiKeyPermission(BasePermission):
    expected_env_vars: tuple[str, ...] = ("TELEMETRIA_API_KEY",)

    """
    Permiso simple por API Key para endpoints operativos.

    - Header recomendado: `X-Telemetria-Key: <key>`
    - Alternativa: `Authorization: Api-Key <key>`

    Si la API key NO está configurada:
    - en DEBUG: permite acceso (conveniente en dev local)
    - en producción: deniega (evita exponer endpoints por accidente)
    """

    message = "Missing or invalid API key."

    def has_permission(self, request, view) -> bool:
        expected_keys = _iter_expected_keys(self.expected_env_vars)
        if not expected_keys:
            return bool(getattr(settings, "DEBUG", False))

        provided = (request.headers.get("X-Telemetria-Key") or "").strip()
        if provided and provided in expected_keys:
            return True

        auth = request.headers.get("Authorization", "")
        if auth.lower().startswith("api-key "):
            token = auth.split(" ", 1)[1].strip()
            return token in expected_keys

        return False


class HasTelemetryWriteApiKey(_BaseTelemetryApiKeyPermission):
    """
    Permite operaciones de escritura (sync/merge/run).

    - Preferido: TELEMETRIA_API_KEY_RW
    - Fallback: TELEMETRIA_API_KEY (compat)
    """

    expected_env_vars = ("TELEMETRIA_API_KEY_RW", "TELEMETRIA_API_KEY")


class HasTelemetryReadApiKey(_BaseTelemetryApiKeyPermission):
    """
    Permite operaciones de lectura (health/dashboard).

    - Preferido: TELEMETRIA_API_KEY_RO
    - También permite RW
    - Fallback: TELEMETRIA_API_KEY (compat)
    """

    expected_env_vars = ("TELEMETRIA_API_KEY_RO", "TELEMETRIA_API_KEY_RW", "TELEMETRIA_API_KEY")

