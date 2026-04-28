from __future__ import annotations

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from delancert.models import TelemetryModelArtifact
from delancert.utils.api_key_authentication import TelemetryApiKeyAuthentication
from delancert.utils.api_key_permission import HasTelemetryReadApiKey


class LatestModelView(APIView):
    """
    Consulta el último modelo activo (registry).

    GET /delancert/ml/models/latest/?task=watch_time_7d
    """

    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        task = (request.query_params.get("task") or "watch_time_7d").strip()
        r = TelemetryModelArtifact.objects.filter(task=task, active=True).order_by("-created_at").first()
        if not r:
            return Response({"success": False, "error": "No active model found."}, status=status.HTTP_404_NOT_FOUND)
        return Response(
            {
                "success": True,
                "task": r.task,
                "model_dir": r.model_dir,
                "feature_names": r.feature_names,
                "metrics": r.metrics,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            },
            status=status.HTTP_200_OK,
        )


class ModelListView(APIView):
    """
    Lista modelos del registry (histórico).

    GET /delancert/ml/models/?task=watch_time_7d&limit=50
    """

    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        task = (request.query_params.get("task") or "watch_time_7d").strip()
        limit = int(request.query_params.get("limit", 50))
        limit = max(1, min(limit, 200))

        qs = TelemetryModelArtifact.objects.filter(task=task).order_by("-created_at", "-id")[:limit]
        data = [
            {
                "task": r.task,
                "model_dir": r.model_dir,
                "active": bool(r.active),
                "feature_names": r.feature_names,
                "metrics": r.metrics,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in qs
        ]
        return Response({"success": True, "task": task, "models": data}, status=status.HTTP_200_OK)

