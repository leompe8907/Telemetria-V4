from __future__ import annotations

import os
from datetime import timedelta

from django.db.models import Max
from django.utils import timezone
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from delancert.models import TelemetryJobRun, TelemetryRecordEntryDelancer, MergedTelemetricOTTDelancer
from delancert.utils.api_key_authentication import TelemetryApiKeyAuthentication
from delancert.utils.api_key_permission import HasTelemetryReadApiKey


def _int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        return int(raw) if raw else int(default)
    except Exception:
        return int(default)


class TelemetryOpsAlertsView(APIView):
    """
    Alertas operativas simples (solo lectura) basadas en la BD local.
    """

    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        now = timezone.now()

        # Umbrales (env-configurables)
        lag_warn = _int_env("TELEMETRIA_ALERT_LAG_WARN", 200)
        lag_crit = _int_env("TELEMETRIA_ALERT_LAG_CRIT", 1000)
        no_new_minutes_warn = _int_env("TELEMETRIA_ALERT_NO_NEW_MIN_WARN", 30)
        no_new_minutes_crit = _int_env("TELEMETRIA_ALERT_NO_NEW_MIN_CRIT", 60)
        consecutive_fail_crit = _int_env("TELEMETRIA_ALERT_CONSEC_FAIL_CRIT", 3)

        raw_max = TelemetryRecordEntryDelancer.objects.aggregate(max_record_id=Max("recordId"))["max_record_id"] or 0
        merged_max = MergedTelemetricOTTDelancer.objects.aggregate(max_record_id=Max("recordId"))["max_record_id"] or 0
        lag = max(0, int(raw_max) - int(merged_max))

        last_raw_ts = (
            TelemetryRecordEntryDelancer.objects.order_by("-timestamp").values_list("timestamp", flat=True).first()
        )
        mins_since_new = None
        clock_skew = False
        if last_raw_ts:
            delta_minutes = int((now - last_raw_ts).total_seconds() // 60)
            # Si el último timestamp está "en el futuro" (por tz o data source), no disparar alertas falsas.
            if delta_minutes < 0:
                clock_skew = True
                mins_since_new = 0
            else:
                mins_since_new = delta_minutes

        # Consecutive failures (a partir de job runs más recientes)
        recent_runs = list(
            TelemetryJobRun.objects.filter(job_type=TelemetryJobRun.JobType.RUN).order_by("-started_at")[:20]
        )
        consec_fail = 0
        for r in recent_runs:
            if r.status == TelemetryJobRun.JobStatus.ERROR:
                consec_fail += 1
            else:
                break

        alerts = []

        # Lag alerts
        if lag >= lag_crit:
            alerts.append(
                {
                    "code": "LAG_CRIT",
                    "severity": "critical",
                    "message": f"Lag crítico raw vs merged: {lag} (raw_max={raw_max}, merged_max={merged_max}).",
                }
            )
        elif lag >= lag_warn:
            alerts.append(
                {
                    "code": "LAG_WARN",
                    "severity": "warning",
                    "message": f"Lag alto raw vs merged: {lag} (raw_max={raw_max}, merged_max={merged_max}).",
                }
            )

        # No-new-data alerts
        if mins_since_new is not None:
            if mins_since_new >= no_new_minutes_crit:
                alerts.append(
                    {
                        "code": "NO_NEW_DATA_CRIT",
                        "severity": "critical",
                        "message": f"Sin nuevos registros raw hace {mins_since_new} min (último timestamp: {last_raw_ts}).",
                    }
                )
            elif mins_since_new >= no_new_minutes_warn:
                alerts.append(
                    {
                        "code": "NO_NEW_DATA_WARN",
                        "severity": "warning",
                        "message": f"Sin nuevos registros raw hace {mins_since_new} min (último timestamp: {last_raw_ts}).",
                    }
                )

        # Consecutive failures
        if consec_fail >= consecutive_fail_crit:
            alerts.append(
                {
                    "code": "CONSEC_FAIL_CRIT",
                    "severity": "critical",
                    "message": f"{consec_fail} ejecuciones telemetry_run seguidas en ERROR.",
                }
            )

        payload = {
            "time": now.isoformat(),
            "thresholds": {
                "lag_warn": lag_warn,
                "lag_crit": lag_crit,
                "no_new_minutes_warn": no_new_minutes_warn,
                "no_new_minutes_crit": no_new_minutes_crit,
                "consecutive_fail_crit": consecutive_fail_crit,
            },
            "signals": {
                "raw_max_record_id": raw_max,
                "merged_max_record_id": merged_max,
                "lag_raw_minus_merged_record_id": lag,
                "last_raw_timestamp": last_raw_ts.isoformat() if last_raw_ts else None,
                "minutes_since_new_raw": mins_since_new,
                "clock_skew_detected": clock_skew,
                "consecutive_run_failures": consec_fail,
            },
            "alerts": alerts,
            "ok": len(alerts) == 0,
            "notes": [
                "Este endpoint NO llama a PanAccess; solo evalúa señales locales (BD + auditoría).",
                "Si alerts no está vacío, revisar logs y /delancert/jobs/runs/.",
                "Si clock_skew_detected=true, revisar zona horaria/parseo de timestamp (puede haber timestamps futuros).",
            ],
        }
        return Response(payload, status=status.HTTP_200_OK)

