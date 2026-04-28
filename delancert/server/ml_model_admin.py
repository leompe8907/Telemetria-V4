from __future__ import annotations

import os
from pathlib import Path

from django.db import transaction
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from delancert.models import TelemetryModelArtifact
from delancert.utils.api_key_authentication import TelemetryApiKeyAuthentication
from delancert.utils.api_key_permission import HasTelemetryWriteApiKey
from delancert.utils.rate_limit import acquire_rate_limit


class ActivateModelView(APIView):
    """
    Activa un modelo del registry y desactiva el resto para el mismo task.

    POST /delancert/ml/models/activate/
    Body:
      - task: "watch_time_7d"
      - model_dir: "<path>" (requerido)
    """

    permission_classes = [HasTelemetryWriteApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def post(self, request):
        rl = acquire_rate_limit("ml_model_activate", ttl_seconds=5)
        if not rl.allowed:
            return Response(
                {"success": False, "error": "Rate limited", "retry_after_seconds": rl.retry_after_seconds},
                status=status.HTTP_429_TOO_MANY_REQUESTS,
                headers={"Retry-After": str(rl.retry_after_seconds)},
            )

        task = (request.data.get("task") or "watch_time_7d").strip()
        model_dir = (request.data.get("model_dir") or "").strip()
        if not model_dir:
            return Response({"success": False, "error": "model_dir requerido."}, status=status.HTTP_400_BAD_REQUEST)

        m = TelemetryModelArtifact.objects.filter(task=task, model_dir=model_dir).first()
        if not m:
            return Response({"success": False, "error": "Modelo no encontrado."}, status=status.HTTP_404_NOT_FOUND)

        validate_files = str(request.data.get("validate_files", "")).strip().lower() in ("1", "true", "yes")
        validate_files = validate_files or ((os.getenv("TELEMETRIA_VALIDATE_MODEL_FILES", "0") or "0").strip().lower() in ("1", "true", "yes"))
        if validate_files:
            p = Path(model_dir)
            missing = []
            if not (p / "model.joblib").exists():
                missing.append("model.joblib")
            if not (p / "feature_names.json").exists():
                missing.append("feature_names.json")
            if missing:
                return Response(
                    {"success": False, "error": "Model files missing", "missing": missing, "model_dir": model_dir},
                    status=status.HTTP_409_CONFLICT,
                )

        with transaction.atomic():
            TelemetryModelArtifact.objects.filter(task=task, active=True).exclude(id=m.id).update(active=False)
            TelemetryModelArtifact.objects.filter(id=m.id).update(active=True)

        return Response({"success": True, "task": task, "model_dir": model_dir, "active": True}, status=status.HTTP_200_OK)


class RollbackModelView(APIView):
    """
    Rollback: activa el modelo anterior (por created_at) y desactiva el actual.

    POST /delancert/ml/models/rollback/
    Body:
      - task: "watch_time_7d"
    """

    permission_classes = [HasTelemetryWriteApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def post(self, request):
        rl = acquire_rate_limit("ml_model_rollback", ttl_seconds=5)
        if not rl.allowed:
            return Response(
                {"success": False, "error": "Rate limited", "retry_after_seconds": rl.retry_after_seconds},
                status=status.HTTP_429_TOO_MANY_REQUESTS,
                headers={"Retry-After": str(rl.retry_after_seconds)},
            )

        task = (request.data.get("task") or "watch_time_7d").strip()
        # Dos últimos modelos para el task (tie-breaker por id)
        latest_two = list(TelemetryModelArtifact.objects.filter(task=task).order_by("-created_at", "-id")[:2])
        if len(latest_two) < 2:
            return Response({"success": False, "error": "No hay modelo previo para rollback."}, status=status.HTTP_409_CONFLICT)

        current = latest_two[0]
        previous = latest_two[1]

        with transaction.atomic():
            TelemetryModelArtifact.objects.filter(task=task, active=True).update(active=False)
            TelemetryModelArtifact.objects.filter(id=previous.id).update(active=True)

        return Response(
            {
                "success": True,
                "task": task,
                "rolled_back_to": previous.model_dir,
                "previous_active": True,
                "current_deactivated": current.model_dir,
            },
            status=status.HTTP_200_OK,
        )

