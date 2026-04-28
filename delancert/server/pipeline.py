from __future__ import annotations

from django.conf import settings
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from delancert.utils.api_key_authentication import TelemetryApiKeyAuthentication
from delancert.utils.api_key_permission import HasTelemetryWriteApiKey
from delancert.utils.rate_limit import acquire_rate_limit


class PipelineRunView(APIView):
    """
    Endpoint RW para ejecutar el pipeline completo.

    Por defecto encola en Celery (async). Si Celery no está habilitado, devuelve 503.
    """

    permission_classes = [HasTelemetryWriteApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def post(self, request):
        rl = acquire_rate_limit("pipeline_run_enqueue", ttl_seconds=10)
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

        from delancert.tasks import pipeline_run_task

        p = request.data or {}
        async_result = pipeline_run_task.delay(
            limit=int(p.get("limit", 1000)),
            batch_size=int(p.get("batch_size", 1000)),
            process_timestamps=bool(p.get("process_timestamps", True)),
            merge_batch_size=int(p.get("merge_batch_size", 500)),
            backfill_last_n=int(p.get("backfill_last_n", 0)),
            aggregates_days=int(p.get("aggregates_days", 7)),
            predict_lookback_days=int(p.get("predict_lookback_days", 7)),
            predict_horizon_days=int(p.get("predict_horizon_days", 7)),
        )
        return Response(
            {"success": True, "accepted": True, "task_name": "telemetria.pipeline_run", "task_id": async_result.id},
            status=status.HTTP_202_ACCEPTED,
        )

