from __future__ import annotations

from django.core.management.base import BaseCommand
from rest_framework.test import APIRequestFactory

from delancert.server.ops import TelemetryOpsAlertsView


class Command(BaseCommand):
    help = "Ejecuta chequeo operativo (ops/alerts) desde CLI y devuelve exit code según severidad."

    def handle(self, *args, **options):
        # Reutilizamos la misma lógica del endpoint, pero sin auth (es CLI local).
        # Esto permite agendarlo y que falle (exit code != 0) si hay alertas críticas.
        factory = APIRequestFactory()
        req = factory.get("/delancert/ops/alerts/")
        view = TelemetryOpsAlertsView.as_view(authentication_classes=[], permission_classes=[])
        resp = view(req)
        payload = resp.data if hasattr(resp, "data") else {}
        alerts = payload.get("alerts") or []
        critical = [a for a in alerts if (a or {}).get("severity") == "critical"]

        self.stdout.write(str(payload))
        raise SystemExit(2 if critical else (1 if alerts else 0))

