from __future__ import annotations

from datetime import timedelta

from django.utils import timezone
from django.db.models import Max, Count
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny

from delancert.models import TelemetryRecordEntryDelancer, MergedTelemetricOTTDelancer


class TelemetryHealthView(APIView):
    """
    Endpoint operativo para validar estado del pipeline local.

    NO llama a PanAccess. Solo consulta la BD local.
    """

    permission_classes = [AllowAny]

    def get(self, request):
        now = timezone.now()
        since = now - timedelta(hours=24)

        raw_max = TelemetryRecordEntryDelancer.objects.aggregate(max_record_id=Max("recordId"))["max_record_id"] or 0
        merged_max = MergedTelemetricOTTDelancer.objects.aggregate(max_record_id=Max("recordId"))["max_record_id"] or 0

        raw_24h = TelemetryRecordEntryDelancer.objects.filter(timestamp__gte=since).count()
        merged_24h = MergedTelemetricOTTDelancer.objects.filter(timestamp__gte=since).count()

        lag = max(0, int(raw_max) - int(merged_max))

        payload = {
            "time": now.isoformat(),
            "raw": {"max_record_id": raw_max, "count_last_24h": raw_24h},
            "merged_ott": {"max_record_id": merged_max, "count_last_24h": merged_24h},
            "lag": {"raw_minus_merged_record_id": lag},
            "notes": [
                "Si lag es alto, ejecutar merge OTT (o aumentar backfill_last_n).",
                "Este endpoint es solo lectura y refleja estado de la BD local.",
            ],
        }
        return Response(payload, status=status.HTTP_200_OK)

