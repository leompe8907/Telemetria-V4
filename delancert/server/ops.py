from __future__ import annotations

import os
from datetime import timedelta

from django.db.models import Max
from django.utils import timezone
from django.db.models import Avg, Sum
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from delancert.models import (
    TelemetryJobRun,
    TelemetryRecordEntryDelancer,
    MergedTelemetricOTTDelancer,
    TelemetryUserDailyAgg,
    TelemetryUserDailyPrediction,
)
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
        ml_drift_warn_pct = _int_env("TELEMETRIA_ML_DRIFT_WARN_PCT", 30)
        ml_drift_crit_pct = _int_env("TELEMETRIA_ML_DRIFT_CRIT_PCT", 60)
        ml_pred_coverage_warn_pct = _int_env("TELEMETRIA_ML_PRED_COVERAGE_WARN_PCT", 80)
        ml_pred_coverage_crit_pct = _int_env("TELEMETRIA_ML_PRED_COVERAGE_CRIT_PCT", 50)

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

        # =============================================================================
        # Alertas ML (drift + cobertura)
        # =============================================================================
        as_of_day = timezone.localdate()
        pred_count = TelemetryUserDailyPrediction.objects.filter(day=as_of_day, horizon_days=7).count()
        active_users_today = (
            TelemetryUserDailyAgg.objects.filter(day=as_of_day).values("subscriber_code").distinct().count()
        )
        coverage_pct = None
        if active_users_today > 0:
            coverage_pct = int(round(pred_count / active_users_today * 100.0))

        if coverage_pct is not None:
            if coverage_pct <= ml_pred_coverage_crit_pct:
                alerts.append(
                    {
                        "code": "ML_PRED_COVERAGE_CRIT",
                        "severity": "critical",
                        "message": f"Cobertura de predicciones baja: {coverage_pct}% (pred={pred_count}, active={active_users_today}).",
                    }
                )
            elif coverage_pct <= ml_pred_coverage_warn_pct:
                alerts.append(
                    {
                        "code": "ML_PRED_COVERAGE_WARN",
                        "severity": "warning",
                        "message": f"Cobertura de predicciones baja: {coverage_pct}% (pred={pred_count}, active={active_users_today}).",
                    }
                )

        def _avg_features(start_day, end_day):
            r = (
                TelemetryUserDailyAgg.objects.filter(day__gte=start_day, day__lte=end_day)
                .aggregate(
                    avg_views=Avg("views"),
                    avg_unique_channels=Avg("unique_channels"),
                    avg_watch_seconds=Avg("total_duration_seconds"),
                )
            )
            return {
                "avg_views": float(r["avg_views"] or 0.0),
                "avg_unique_channels": float(r["avg_unique_channels"] or 0.0),
                "avg_watch_seconds": float(r["avg_watch_seconds"] or 0.0),
            }

        last7_start = as_of_day - timedelta(days=6)
        prev7_start = as_of_day - timedelta(days=13)
        prev7_end = as_of_day - timedelta(days=7)
        cur = _avg_features(last7_start, as_of_day)
        prev = _avg_features(prev7_start, prev7_end)

        def _pct_change(cur_v: float, prev_v: float) -> float | None:
            if prev_v == 0:
                return None if cur_v == 0 else 999.0
            return (cur_v - prev_v) / abs(prev_v) * 100.0

        for k in ("avg_views", "avg_unique_channels", "avg_watch_seconds"):
            pct = _pct_change(cur[k], prev[k])
            if pct is None:
                continue
            apct = abs(float(pct))
            if apct >= float(ml_drift_crit_pct):
                alerts.append(
                    {
                        "code": "ML_DRIFT_CRIT",
                        "severity": "critical",
                        "message": f"Drift crítico en {k}: {round(pct, 2)}% (7d vs prev7d).",
                    }
                )
            elif apct >= float(ml_drift_warn_pct):
                alerts.append(
                    {
                        "code": "ML_DRIFT_WARN",
                        "severity": "warning",
                        "message": f"Drift alto en {k}: {round(pct, 2)}% (7d vs prev7d).",
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
                "ml_drift_warn_pct": ml_drift_warn_pct,
                "ml_drift_crit_pct": ml_drift_crit_pct,
                "ml_pred_coverage_warn_pct": ml_pred_coverage_warn_pct,
                "ml_pred_coverage_crit_pct": ml_pred_coverage_crit_pct,
            },
            "signals": {
                "raw_max_record_id": raw_max,
                "merged_max_record_id": merged_max,
                "lag_raw_minus_merged_record_id": lag,
                "last_raw_timestamp": last_raw_ts.isoformat() if last_raw_ts else None,
                "minutes_since_new_raw": mins_since_new,
                "clock_skew_detected": clock_skew,
                "consecutive_run_failures": consec_fail,
                "ml_predictions_today": pred_count,
                "ml_active_users_today": active_users_today,
                "ml_pred_coverage_pct": coverage_pct,
            },
            "alerts": alerts,
            "ok": len(alerts) == 0,
            "notes": [
                "Este endpoint NO llama a PanAccess; solo evalúa señales locales (BD + auditoría).",
                "Si alerts no está vacío, revisar logs y /delancert/jobs/runs/.",
                "Si clock_skew_detected=true, revisar zona horaria/parseo de timestamp (puede haber timestamps futuros).",
                "Alertas ML son heurísticas: validar con contexto (campañas, estacionalidad, fallas de scoring).",
            ],
        }
        return Response(payload, status=status.HTTP_200_OK)


class TelemetryOpsSummaryView(APIView):
    """
    Resumen operativo en una sola llamada:
    - señales health (raw/merged/lag)
    - alertas (ops/alerts)
    - últimos job runs (RUN / SYNC / MERGE_OTT / INTEGRITY_CHECK)
    """

    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        now = timezone.now()

        raw_max = TelemetryRecordEntryDelancer.objects.aggregate(max_record_id=Max("recordId"))["max_record_id"] or 0
        merged_max = MergedTelemetricOTTDelancer.objects.aggregate(max_record_id=Max("recordId"))["max_record_id"] or 0
        lag = max(0, int(raw_max) - int(merged_max))

        # Reusar lógica de alertas (sin duplicar reglas)
        alerts_payload = TelemetryOpsAlertsView().get(request).data

        last_runs = {}
        for jt in (
            TelemetryJobRun.JobType.RUN,
            TelemetryJobRun.JobType.SYNC,
            TelemetryJobRun.JobType.MERGE_OTT,
            TelemetryJobRun.JobType.INTEGRITY_CHECK,
            TelemetryJobRun.JobType.ML_TRAIN,
            TelemetryJobRun.JobType.ML_PREDICT,
        ):
            r = TelemetryJobRun.objects.filter(job_type=jt).order_by("-started_at").first()
            if not r:
                last_runs[jt] = None
            else:
                last_runs[jt] = {
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

        # =============================================================================
        # Señales ML (mínimas) + drift simple en features agregadas
        # =============================================================================
        drift_warn_pct = _int_env("TELEMETRIA_ML_DRIFT_WARN_PCT", 30)  # 30%
        drift_crit_pct = _int_env("TELEMETRIA_ML_DRIFT_CRIT_PCT", 60)  # 60%

        as_of_day = timezone.localdate()
        # Cobertura de predicciones del día
        pred_count = TelemetryUserDailyPrediction.objects.filter(day=as_of_day, horizon_days=7).count()
        active_users_today = TelemetryUserDailyAgg.objects.filter(day=as_of_day).values("subscriber_code").distinct().count()

        def _avg_features(start_day, end_day):
            r = (
                TelemetryUserDailyAgg.objects.filter(day__gte=start_day, day__lte=end_day)
                .aggregate(
                    avg_views=Avg("views"),
                    avg_unique_channels=Avg("unique_channels"),
                    avg_watch_seconds=Avg("total_duration_seconds"),
                )
            )
            return {
                "avg_views": float(r["avg_views"] or 0.0),
                "avg_unique_channels": float(r["avg_unique_channels"] or 0.0),
                "avg_watch_seconds": float(r["avg_watch_seconds"] or 0.0),
            }

        # Comparación simple: últimos 7 días vs 7 días previos
        last7_start = as_of_day - timedelta(days=6)
        prev7_start = as_of_day - timedelta(days=13)
        prev7_end = as_of_day - timedelta(days=7)

        cur = _avg_features(last7_start, as_of_day)
        prev = _avg_features(prev7_start, prev7_end)

        def _pct_change(cur_v: float, prev_v: float) -> float | None:
            if prev_v == 0:
                return None if cur_v == 0 else 999.0
            return (cur_v - prev_v) / abs(prev_v) * 100.0

        drift = {}
        drift_alerts = []
        for k in ("avg_views", "avg_unique_channels", "avg_watch_seconds"):
            pct = _pct_change(cur[k], prev[k])
            drift[k] = {"current": cur[k], "previous": prev[k], "pct_change": pct}
            if pct is None:
                continue
            apct = abs(float(pct))
            if apct >= float(drift_crit_pct):
                drift_alerts.append({"code": "ML_DRIFT_CRIT", "severity": "critical", "metric": k, "pct_change": pct})
            elif apct >= float(drift_warn_pct):
                drift_alerts.append({"code": "ML_DRIFT_WARN", "severity": "warning", "metric": k, "pct_change": pct})

        payload = {
            "time": now.isoformat(),
            "health": {
                "raw_max_record_id": raw_max,
                "merged_ott_max_record_id": merged_max,
                "lag_raw_minus_merged_record_id": lag,
            },
            "alerts": alerts_payload,
            "last_runs": last_runs,
            "ml": {
                "as_of_day": as_of_day.isoformat(),
                "predictions_today": {"count": pred_count, "active_users_today": active_users_today},
                "feature_drift_7d_vs_prev7d": {
                    "window_current": {"start": last7_start.isoformat(), "end": as_of_day.isoformat()},
                    "window_previous": {"start": prev7_start.isoformat(), "end": prev7_end.isoformat()},
                    "metrics": drift,
                    "alerts": drift_alerts,
                    "thresholds": {"warn_pct": drift_warn_pct, "crit_pct": drift_crit_pct},
                },
            },
        }
        return Response(payload, status=status.HTTP_200_OK)

