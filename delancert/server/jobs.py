from __future__ import annotations

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from delancert.models import TelemetryJobRun
from delancert.utils.api_key_permission import HasTelemetryReadApiKey
from delancert.utils.api_key_authentication import TelemetryApiKeyAuthentication


class TelemetryJobRunsView(APIView):
    """
    Lista los últimos runs operativos (auditoría).
    """

    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        limit = int(request.query_params.get("limit", 20))
        limit = max(1, min(limit, 200))

        qs = TelemetryJobRun.objects.all().order_by("-started_at")[:limit]
        data = [
            {
                "id": r.id,
                "job_type": r.job_type,
                "status": r.status,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "duration_ms": r.duration_ms,
                "downloaded": r.downloaded,
                "saved": r.saved,
                "skipped": r.skipped,
                "errors": r.errors,
                "highest_record_id_before": r.highest_record_id_before,
                "highest_record_id_after": r.highest_record_id_after,
                "merged_saved": r.merged_saved,
                "merged_deleted_existing": r.merged_deleted_existing,
                "merge_backfill_last_n": r.merge_backfill_last_n,
                "error_message": r.error_message,
            }
            for r in qs
        ]
        return Response({"runs": data}, status=status.HTTP_200_OK)

