from __future__ import annotations

from django.conf import settings
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from delancert.utils.api_key_authentication import TelemetryApiKeyAuthentication
from delancert.utils.api_key_permission import HasTelemetryReadApiKey, HasTelemetryWriteApiKey
from delancert.utils.rate_limit import acquire_rate_limit


def _celery_enabled() -> bool:
    return bool(getattr(settings, "CELERY_BROKER_URL", None))


class TelemetryRunEnqueueView(APIView):
    """
    Encola `telemetry_run` en Celery (sync + merge OTT).

    Respuesta:
    - 202 con {task_id, task_name, accepted:true}
    - 503 si Celery no está configurado (sin broker)
    """

    permission_classes = [HasTelemetryWriteApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def post(self, request):
        rl = acquire_rate_limit("telemetry_run_enqueue", ttl_seconds=10)
        if not rl.allowed:
            return Response(
                {"success": False, "error": "Rate limited", "retry_after_seconds": rl.retry_after_seconds},
                status=status.HTTP_429_TOO_MANY_REQUESTS,
                headers={"Retry-After": str(rl.retry_after_seconds)},
            )

        if not _celery_enabled():
            return Response(
                {"success": False, "error": "Celery disabled", "message": "CELERY_BROKER_URL/REDIS_URL no configurado."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        from delancert.tasks import telemetry_run_task

        payload = request.data or {}
        limit = int(payload.get("limit", 1000))
        batch_size = int(payload.get("batch_size", 1000))
        merge_batch_size = int(payload.get("merge_batch_size", 500))
        backfill_last_n = int(payload.get("backfill_last_n", 0))
        process_timestamps = payload.get("process_timestamps", True)
        if isinstance(process_timestamps, str):
            process_timestamps = process_timestamps.lower() in ("true", "1", "yes")

        async_result = telemetry_run_task.delay(
            limit=limit,
            batch_size=batch_size,
            process_timestamps=bool(process_timestamps),
            merge_batch_size=merge_batch_size,
            backfill_last_n=backfill_last_n,
        )

        return Response(
            {"success": True, "accepted": True, "task_name": "telemetria.telemetry_run", "task_id": async_result.id},
            status=status.HTTP_202_ACCEPTED,
        )


class TelemetryBuildAggregatesEnqueueView(APIView):
    """
    Encola `build_aggregates` en Celery (materialización gold).
    """

    permission_classes = [HasTelemetryWriteApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def post(self, request):
        rl = acquire_rate_limit("telemetry_build_aggregates_enqueue", ttl_seconds=10)
        if not rl.allowed:
            return Response(
                {"success": False, "error": "Rate limited", "retry_after_seconds": rl.retry_after_seconds},
                status=status.HTTP_429_TOO_MANY_REQUESTS,
                headers={"Retry-After": str(rl.retry_after_seconds)},
            )

        if not _celery_enabled():
            return Response(
                {"success": False, "error": "Celery disabled", "message": "CELERY_BROKER_URL/REDIS_URL no configurado."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        from delancert.tasks import telemetry_build_aggregates_task

        payload = request.data or {}
        days = int(payload.get("days", 7))
        async_result = telemetry_build_aggregates_task.delay(days=days)

        return Response(
            {"success": True, "accepted": True, "task_name": "telemetria.build_aggregates", "task_id": async_result.id},
            status=status.HTTP_202_ACCEPTED,
        )


class MlBuildDatasetEnqueueView(APIView):
    """
    Encola `ml_build_dataset` en Celery.
    """

    permission_classes = [HasTelemetryWriteApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def post(self, request):
        rl = acquire_rate_limit("ml_build_dataset_enqueue", ttl_seconds=10)
        if not rl.allowed:
            return Response(
                {"success": False, "error": "Rate limited", "retry_after_seconds": rl.retry_after_seconds},
                status=status.HTTP_429_TOO_MANY_REQUESTS,
                headers={"Retry-After": str(rl.retry_after_seconds)},
            )

        if not _celery_enabled():
            return Response(
                {"success": False, "error": "Celery disabled", "message": "CELERY_BROKER_URL/REDIS_URL no configurado."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        from delancert.tasks import ml_build_dataset_task

        payload = request.data or {}
        async_result = ml_build_dataset_task.delay(
            as_of=payload.get("as_of"),
            lookback_days=int(payload.get("lookback_days", 7)),
            horizon_days=int(payload.get("horizon_days", 7)),
            output=str(payload.get("output", "artifacts/ml/datasets/watch_time_7d.csv")),
            min_history_days=int(payload.get("min_history_days", 1)),
        )
        return Response(
            {"success": True, "accepted": True, "task_name": "telemetria.ml_build_dataset", "task_id": async_result.id},
            status=status.HTTP_202_ACCEPTED,
        )


class MlTrainEnqueueView(APIView):
    """
    Encola `ml_train` en Celery.
    """

    permission_classes = [HasTelemetryWriteApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def post(self, request):
        rl = acquire_rate_limit("ml_train_enqueue", ttl_seconds=10)
        if not rl.allowed:
            return Response(
                {"success": False, "error": "Rate limited", "retry_after_seconds": rl.retry_after_seconds},
                status=status.HTTP_429_TOO_MANY_REQUESTS,
                headers={"Retry-After": str(rl.retry_after_seconds)},
            )

        if not _celery_enabled():
            return Response(
                {"success": False, "error": "Celery disabled", "message": "CELERY_BROKER_URL/REDIS_URL no configurado."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        from delancert.tasks import ml_train_task

        payload = request.data or {}
        async_result = ml_train_task.delay(
            dataset=str(payload.get("dataset", "artifacts/ml/datasets/watch_time_7d.csv")),
            out_dir=payload.get("out_dir"),
        )
        return Response(
            {"success": True, "accepted": True, "task_name": "telemetria.ml_train", "task_id": async_result.id},
            status=status.HTTP_202_ACCEPTED,
        )


class MlPredictEnqueueView(APIView):
    """
    Encola `ml_predict` (batch scoring) en Celery.
    """

    permission_classes = [HasTelemetryWriteApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def post(self, request):
        rl = acquire_rate_limit("ml_predict_enqueue", ttl_seconds=10)
        if not rl.allowed:
            return Response(
                {"success": False, "error": "Rate limited", "retry_after_seconds": rl.retry_after_seconds},
                status=status.HTTP_429_TOO_MANY_REQUESTS,
                headers={"Retry-After": str(rl.retry_after_seconds)},
            )

        if not _celery_enabled():
            return Response(
                {"success": False, "error": "Celery disabled", "message": "CELERY_BROKER_URL/REDIS_URL no configurado."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        from delancert.tasks import ml_predict_task

        payload = request.data or {}
        async_result = ml_predict_task.delay(
            as_of=payload.get("as_of"),
            lookback_days=int(payload.get("lookback_days", 7)),
            horizon_days=int(payload.get("horizon_days", 7)),
            model_dir=payload.get("model_dir"),
        )
        return Response(
            {"success": True, "accepted": True, "task_name": "telemetria.ml_predict", "task_id": async_result.id},
            status=status.HTTP_202_ACCEPTED,
        )


class CeleryTaskStatusView(APIView):
    """
    Estado de una tarea Celery por task_id.

    Nota: requiere backend de resultados configurado para ver `result`.
    """

    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request, task_id: str):
        if not _celery_enabled():
            return Response(
                {"success": False, "error": "Celery disabled", "message": "CELERY_BROKER_URL/REDIS_URL no configurado."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        from celery.result import AsyncResult  # type: ignore[import-not-found]

        r = AsyncResult(task_id)
        payload = {
            "success": True,
            "task_id": task_id,
            "state": r.state,
            "ready": bool(r.ready()),
            "successful": bool(r.successful()) if r.ready() else None,
        }
        # Evitar reventar JSON con exceptions no serializables
        if r.ready():
            try:
                payload["result"] = r.result
            except Exception:
                payload["result"] = None

        return Response(payload, status=status.HTTP_200_OK)

