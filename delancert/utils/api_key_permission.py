from __future__ import annotations

from rest_framework.permissions import BasePermission

from delancert.utils.api_key_authentication import TelemetriaAuth


class HasTelemetryWriteApiKey(BasePermission):
    message = "Write API key required."

    def has_permission(self, request, view) -> bool:
        auth = getattr(request, "auth", None)
        if not isinstance(auth, TelemetriaAuth):
            return False
        return auth.scope == "rw"


class HasTelemetryReadApiKey(BasePermission):
    message = "Read API key required."

    def has_permission(self, request, view) -> bool:
        auth = getattr(request, "auth", None)
        if not isinstance(auth, TelemetriaAuth):
            return False
        # RW también puede leer
        return auth.scope in ("ro", "rw")

