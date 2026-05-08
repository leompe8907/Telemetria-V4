from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from django.conf import settings
from django.contrib.auth.models import AnonymousUser
from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed


@dataclass(frozen=True)
class TelemetriaAuth:
    scope: str  # "ro" | "rw"
    key_name: str  # which env var matched


def _env(name: str) -> Optional[str]:
    v = (os.getenv(name) or "").strip()
    return v or None


def _extract_key(request) -> Optional[str]:
    k = (request.headers.get("X-Telemetria-Key") or "").strip()
    if k:
        return k
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("api-key "):
        return auth.split(" ", 1)[1].strip() or None
    return None


class TelemetryApiKeyAuthentication(BaseAuthentication):
    """
    Autenticación por API key.

    Devuelve (AnonymousUser, TelemetriaAuth) con scope ro/rw.
    Si faltan/son inválidas las keys, lanza AuthenticationFailed con mensaje claro (401).
    """

    def authenticate_header(self, request) -> str:
        # Importante: si esto devuelve None, DRF puede transformar el 401 en 403.
        return "Api-Key"

    def authenticate(self, request):
        # Si no hay keys configuradas: en DEBUG no exigir auth; en prod fallar.
        key_rw = _env("TELEMETRIA_API_KEY_RW")
        key_ro = _env("TELEMETRIA_API_KEY_RO")
        key_any = _env("TELEMETRIA_API_KEY")

        any_keys = [k for k in (key_rw, key_ro, key_any) if k]
        if not any_keys:
            if bool(getattr(settings, "DEBUG", False)):
                return (AnonymousUser(), TelemetriaAuth(scope="rw", key_name="DEBUG_NO_KEY"))
            raise AuthenticationFailed("Missing API key configuration on server.")

        provided = _extract_key(request)
        if not provided:
            raise AuthenticationFailed("Missing API key.")

        # Scope resolution: RO allows RW too (read endpoints can be accessed with RW key).
        if key_rw and provided == key_rw:
            return (AnonymousUser(), TelemetriaAuth(scope="rw", key_name="TELEMETRIA_API_KEY_RW"))
        if key_ro and provided == key_ro:
            return (AnonymousUser(), TelemetriaAuth(scope="ro", key_name="TELEMETRIA_API_KEY_RO"))
        if key_any and provided == key_any:
            return (AnonymousUser(), TelemetriaAuth(scope="rw", key_name="TELEMETRIA_API_KEY"))

        raise AuthenticationFailed("Invalid API key.")

