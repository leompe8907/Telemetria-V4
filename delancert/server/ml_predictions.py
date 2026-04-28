from __future__ import annotations

from datetime import date

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from delancert.models import TelemetryUserDailyPrediction
from delancert.utils.api_key_authentication import TelemetryApiKeyAuthentication
from delancert.utils.api_key_permission import HasTelemetryReadApiKey


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


class UserPredictionsView(APIView):
    """
    Consulta predicciones por usuario.

    GET /delancert/ml/predictions/users/<subscriber_code>/?start=YYYY-MM-DD&end=YYYY-MM-DD&horizon_days=7
    """

    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request, subscriber_code: str):
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")
        horizon_days = int(request.query_params.get("horizon_days", 7))

        if not start_s or not end_s:
            return Response(
                {"success": False, "error": "Missing start/end (YYYY-MM-DD)."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        start = _parse_date(start_s)
        end = _parse_date(end_s)
        if end < start:
            return Response({"success": False, "error": "end must be >= start."}, status=status.HTTP_400_BAD_REQUEST)

        qs = (
            TelemetryUserDailyPrediction.objects.filter(
                subscriber_code=subscriber_code,
                horizon_days=horizon_days,
                day__gte=start,
                day__lte=end,
            )
            .order_by("day")
            .values("day", "y_pred_watch_seconds", "model_dir", "created_at")
        )
        data = [
            {
                "day": r["day"].isoformat(),
                "y_pred_watch_seconds": float(r["y_pred_watch_seconds"]),
                "model_dir": r["model_dir"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in qs
        ]
        return Response(
            {"success": True, "subscriber_code": subscriber_code, "horizon_days": horizon_days, "predictions": data},
            status=status.HTTP_200_OK,
        )


class DailyPredictionsSummaryView(APIView):
    """
    Resumen simple de predicciones de un día.

    GET /delancert/ml/predictions/daily/?day=YYYY-MM-DD&horizon_days=7
    """

    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        day_s = request.query_params.get("day")
        horizon_days = int(request.query_params.get("horizon_days", 7))
        if not day_s:
            return Response({"success": False, "error": "Missing day (YYYY-MM-DD)."}, status=status.HTTP_400_BAD_REQUEST)

        d = _parse_date(day_s)
        qs = TelemetryUserDailyPrediction.objects.filter(day=d, horizon_days=horizon_days)
        return Response(
            {
                "success": True,
                "day": d.isoformat(),
                "horizon_days": horizon_days,
                "count": qs.count(),
            },
            status=status.HTTP_200_OK,
        )

