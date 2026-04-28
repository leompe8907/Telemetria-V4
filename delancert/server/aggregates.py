from __future__ import annotations

from django.conf import settings
from django.core.management import call_command
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from delancert.utils.api_key_authentication import TelemetryApiKeyAuthentication
from delancert.utils.api_key_permission import HasTelemetryWriteApiKey
from delancert.utils.rate_limit import acquire_rate_limit


class TelemetryBuildAggregatesView(APIView):
    """
    Endpoint operativo para materializar agregados diarios (gold).

    - Sync (default): ejecuta `python manage.py telemetry_build_aggregates --days N`.
    - Async: si `async=true` y Celery está habilitado, encola `telemetria.build_aggregates`.
    """

    permission_classes = [HasTelemetryWriteApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def post(self, request):
        payload = request.data or {}
        async_requested = payload.get("async", False)
        if isinstance(async_requested, str):
            async_requested = async_requested.strip().lower() in ("1", "true", "yes")

        days = int(payload.get("days", 7))

        if async_requested:
            rl = acquire_rate_limit("telemetry_build_aggregates_enqueue", ttl_seconds=10)
            if not rl.allowed:
                return Response(
                    {"success": False, "error": "Rate limited", "retry_after_seconds": rl.retry_after_seconds},
                    status=status.HTTP_429_TOO_MANY_REQUESTS,
                    headers={"Retry-After": str(rl.retry_after_seconds)},
                )

            if not bool(getattr(settings, "CELERY_BROKER_URL", None)):
                return Response(
                    {"success": False, "error": "Celery disabled", "message": "CELERY_BROKER_URL/REDIS_URL no configurado."},
                    status=status.HTTP_503_SERVICE_UNAVAILABLE,
                )

            from delancert.tasks import telemetry_build_aggregates_task

            async_result = telemetry_build_aggregates_task.delay(days=days)
            return Response(
                {"success": True, "accepted": True, "task_name": "telemetria.build_aggregates", "task_id": async_result.id},
                status=status.HTTP_202_ACCEPTED,
            )

        rl = acquire_rate_limit("telemetry_build_aggregates", ttl_seconds=60)
        if not rl.allowed:
            return Response(
                {"success": False, "error": "Rate limited", "retry_after_seconds": rl.retry_after_seconds},
                status=status.HTTP_429_TOO_MANY_REQUESTS,
                headers={"Retry-After": str(rl.retry_after_seconds)},
            )

        call_command("telemetry_build_aggregates", days=days)
        return Response({"success": True, "message": "Aggregates built", "days": days}, status=status.HTTP_200_OK)

