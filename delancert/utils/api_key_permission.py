from __future__ import annotations

import os
from typing import Optional

from django.conf import settings
from rest_framework.permissions import BasePermission


def _get_api_key() -> Optional[str]:
    return (os.getenv("TELEMETRIA_API_KEY") or "").strip() or None


class HasTelemetryApiKey(BasePermission):
    """
    Permiso simple por API Key para endpoints operativos.

    - Header recomendado: `X-Telemetria-Key: <key>`
    - Alternativa: `Authorization: Api-Key <key>`

    Si `TELEMETRIA_API_KEY` NO está configurada:
    - en DEBUG: permite acceso (conveniente en dev local)
    - en producción: deniega (evita exponer endpoints por accidente)
    """

    message = "Missing or invalid API key."

    def has_permission(self, request, view) -> bool:
        expected = _get_api_key()
        if expected is None:
            return bool(getattr(settings, "DEBUG", False))

        provided = request.headers.get("X-Telemetria-Key")
        if provided and provided.strip() == expected:
            return True

        auth = request.headers.get("Authorization", "")
        if auth.lower().startswith("api-key "):
            token = auth.split(" ", 1)[1].strip()
            return token == expected

        return False

