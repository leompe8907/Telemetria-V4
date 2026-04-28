from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from delancert.utils.api_key_authentication import TelemetryApiKeyAuthentication
from delancert.utils.api_key_permission import HasTelemetryReadApiKey
from delancert.server.ops import TelemetryOpsAlertsView, TelemetryOpsSummaryView


@dataclass(frozen=True)
class Recommendation:
    code: str
    severity: str  # info|warning|critical
    title: str
    rationale: str
    actions: list[dict[str, Any]]


def _rec(code: str, severity: str, title: str, rationale: str, actions: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "title": title,
        "rationale": rationale,
        "actions": actions,
    }


class NocRecommendationsView(APIView):
    """
    “Agente NOC v0” determinístico:
    - Lee señales/alertas locales (sin llamar PanAccess).
    - Devuelve recomendaciones accionables (playbook) para operación.
    """

    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        alerts_payload = TelemetryOpsAlertsView().get(request).data
        summary_payload = TelemetryOpsSummaryView().get(request).data

        alerts = alerts_payload.get("alerts") or []
        recs: list[dict[str, Any]] = []

        # Helpers de acciones
        def _enqueue_telemetry_run(backfill_last_n: int = 0) -> dict[str, Any]:
            return {
                "type": "api",
                "method": "POST",
                "endpoint": "/delancert/telemetry/run/",
                "body": {"async": True, "backfill_last_n": backfill_last_n},
                "notes": "Requiere API key RW. Si Celery no está habilitado, usar sync o Task Scheduler.",
            }

        def _enqueue_build_aggregates(days: int = 7) -> dict[str, Any]:
            return {
                "type": "api",
                "method": "POST",
                "endpoint": "/delancert/telemetry/build-aggregates/",
                "body": {"async": True, "days": days},
                "notes": "Requiere API key RW.",
            }

        def _enqueue_ml_predict() -> dict[str, Any]:
            return {
                "type": "api",
                "method": "POST",
                "endpoint": "/delancert/tasks/ml/predict/",
                "body": {"as_of": None, "lookback_days": 7, "horizon_days": 7},
                "notes": "Batch scoring. Requiere API key RW.",
            }

        def _enqueue_ml_train() -> dict[str, Any]:
            return {
                "type": "api",
                "method": "POST",
                "endpoint": "/delancert/tasks/ml/train/",
                "body": {"dataset": "artifacts/ml/datasets/watch_time_7d.csv", "out_dir": None},
                "notes": "Entrena modelo. Requiere API key RW.",
            }

        # Mapear alertas -> recomendaciones
        codes = {a.get("code") for a in alerts if isinstance(a, dict)}

        if "LAG_CRIT" in codes or "LAG_WARN" in codes:
            recs.append(
                _rec(
                    code="PLAYBOOK_LAG",
                    severity="critical" if "LAG_CRIT" in codes else "warning",
                    title="Reducir lag raw→merged (OTT)",
                    rationale="Hay diferencia significativa entre recordId max en raw vs merged; el dashboard/ML puede estar leyendo datos atrasados.",
                    actions=[
                        _enqueue_telemetry_run(backfill_last_n=200 if "LAG_CRIT" in codes else 50),
                        _enqueue_build_aggregates(days=7),
                        {
                            "type": "api",
                            "method": "GET",
                            "endpoint": "/delancert/jobs/runs/?limit=20",
                            "notes": "Verificar runs recientes y errores asociados.",
                        },
                    ],
                )
            )

        if "NO_NEW_DATA_CRIT" in codes or "NO_NEW_DATA_WARN" in codes:
            recs.append(
                _rec(
                    code="PLAYBOOK_NO_NEW_DATA",
                    severity="critical" if "NO_NEW_DATA_CRIT" in codes else "warning",
                    title="Diagnóstico de ingesta (sin nuevos datos)",
                    rationale="No se observan nuevos registros raw en la ventana esperada. Puede ser upstream (PanAccess) o scheduler detenido.",
                    actions=[
                        {
                            "type": "api",
                            "method": "GET",
                            "endpoint": "/delancert/health/",
                            "notes": "Confirmar estado local (no llama PanAccess).",
                        },
                        _enqueue_telemetry_run(backfill_last_n=0),
                        {
                            "type": "ops",
                            "notes": "Verificar que el scheduler esté corriendo (Task Scheduler o Celery Beat/Worker) y que variables .env estén cargadas en el proceso.",
                        },
                    ],
                )
            )

        if "CONSEC_FAIL_CRIT" in codes:
            recs.append(
                _rec(
                    code="PLAYBOOK_CONSEC_FAIL",
                    severity="critical",
                    title="Recuperación ante fallos consecutivos",
                    rationale="Múltiples runs en ERROR indican falla persistente (upstream, DB, credenciales, o regresión).",
                    actions=[
                        {
                            "type": "api",
                            "method": "GET",
                            "endpoint": "/delancert/jobs/runs/?limit=20",
                            "notes": "Revisar error_message y tiempos.",
                        },
                        {
                            "type": "ops",
                            "notes": "Revisar logs del proceso (daphne/worker) y conectividad a Postgres/Redis.",
                        },
                    ],
                )
            )

        if "ML_PRED_COVERAGE_CRIT" in codes or "ML_PRED_COVERAGE_WARN" in codes:
            recs.append(
                _rec(
                    code="PLAYBOOK_ML_COVERAGE",
                    severity="critical" if "ML_PRED_COVERAGE_CRIT" in codes else "warning",
                    title="Recuperar cobertura de predicciones (batch scoring)",
                    rationale="Hay usuarios activos sin predicción del día; el scoring no corrió o falló.",
                    actions=[
                        _enqueue_ml_predict(),
                        {
                            "type": "api",
                            "method": "GET",
                            "endpoint": "/delancert/jobs/runs/?limit=20",
                            "notes": "Confirmar último ML_PREDICT y errores.",
                        },
                    ],
                )
            )

        if "ML_DRIFT_CRIT" in codes or "ML_DRIFT_WARN" in codes:
            recs.append(
                _rec(
                    code="PLAYBOOK_ML_DRIFT",
                    severity="critical" if "ML_DRIFT_CRIT" in codes else "warning",
                    title="Mitigar drift (features agregadas)",
                    rationale="Cambios fuertes en distribución pueden degradar el modelo. Puede ser estacionalidad/campaña o cambio de pipeline.",
                    actions=[
                        {
                            "type": "api",
                            "method": "GET",
                            "endpoint": "/delancert/ops/summary/",
                            "notes": "Revisar métricas drift y ventanas comparadas.",
                        },
                        _enqueue_ml_train(),
                        _enqueue_ml_predict(),
                        {
                            "type": "ops",
                            "notes": "Validar si hubo cambios de producto/catálogo/horarios o incidencias upstream.",
                        },
                    ],
                )
            )

        payload = {
            "success": True,
            "time": summary_payload.get("time"),
            "alerts": alerts_payload,
            "summary": summary_payload,
            "recommendations": recs,
        }
        return Response(payload, status=status.HTTP_200_OK)

