from __future__ import annotations

import json
from typing import Any

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from delancert.server.ops import TelemetryOpsAlertsView, TelemetryOpsSummaryView
from delancert.utils.api_key_authentication import TelemetryApiKeyAuthentication
from delancert.utils.api_key_permission import HasTelemetryReadApiKey


def _bullet(lines: list[str]) -> str:
    return "\n".join([f"- {l}" for l in lines if l])


def _deterministic_narrative(*, alerts: list[dict[str, Any]], summary: dict[str, Any]) -> dict[str, Any]:
    codes = {a.get("code") for a in alerts if isinstance(a, dict)}

    highlights: list[str] = []
    risks: list[str] = []
    next_actions: list[str] = []

    if not alerts:
        highlights.append("Operación OK: no hay alertas activas.")
    if "LAG_CRIT" in codes or "LAG_WARN" in codes:
        risks.append("Lag raw→merged alto: el dashboard/ML puede leer datos atrasados.")
        next_actions.append("Ejecutar `telemetry/run` (async) con backfill acotado y luego reconstruir agregados.")
    if "NO_NEW_DATA_CRIT" in codes or "NO_NEW_DATA_WARN" in codes:
        risks.append("No se observan nuevos registros raw: posible interrupción upstream o scheduler detenido.")
        next_actions.append("Verificar scheduler (Task Scheduler / Celery) y ejecutar `telemetry/run` para validar conectividad.")
    if "CONSEC_FAIL_CRIT" in codes:
        risks.append("Fallos consecutivos en jobs: falla persistente (upstream/DB/credenciales/regresión).")
        next_actions.append("Revisar `jobs/runs` + logs del proceso; corregir causa raíz antes de reintentar en loop.")
    if "ML_PRED_COVERAGE_CRIT" in codes or "ML_PRED_COVERAGE_WARN" in codes:
        risks.append("Cobertura de predicciones baja: scoring no corrió o falló.")
        next_actions.append("Encolar `ml_predict` (batch scoring) y confirmar último `ML_PREDICT` en `jobs/runs`.")
    if "ML_DRIFT_CRIT" in codes or "ML_DRIFT_WARN" in codes:
        risks.append("Drift en features agregadas: posible degradación del modelo o cambio de comportamiento/estacionalidad.")
        next_actions.append("Re-entrenar (`ml_train`) y re-scorear (`ml_predict`) tras validar contexto de negocio.")

    # Resumen ejecutivo de salud
    health = summary.get("health") or {}
    ml = summary.get("ml") or {}

    executive = {
        "health": {
            "lag_raw_minus_merged_record_id": health.get("lag_raw_minus_merged_record_id"),
            "raw_max_record_id": health.get("raw_max_record_id"),
            "merged_ott_max_record_id": health.get("merged_ott_max_record_id"),
        },
        "ml": {
            "predictions_today": (ml.get("predictions_today") or {}),
            "drift_alerts": ((ml.get("feature_drift_7d_vs_prev7d") or {}).get("alerts") or []),
        },
    }

    return {
        "mode": "deterministic",
        "executive": executive,
        "highlights_md": _bullet(highlights),
        "risks_md": _bullet(risks),
        "next_actions_md": _bullet(next_actions),
    }


class OpsAnalystReportView(APIView):
    """
    “Agente Analista v0”:
    - Consume `ops/alerts` + `ops/summary` (agregados, sin PII).
    - Produce reporte ejecutivo determinístico.
    - Si `use_llm=true` y LLM está configurado por env, agrega narrativa LLM (opcional).
    """

    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        use_llm = request.query_params.get("use_llm", "0")
        use_llm = str(use_llm).strip().lower() in ("1", "true", "yes")

        alerts_payload = TelemetryOpsAlertsView().get(request).data
        summary_payload = TelemetryOpsSummaryView().get(request).data

        alerts = alerts_payload.get("alerts") or []
        report = _deterministic_narrative(alerts=alerts, summary=summary_payload)

        llm_text = None
        llm_error = None
        if use_llm:
            try:
                from delancert.utils.llm_client import generate_text

                system = (
                    "Eres un analista SRE/ML-Ops. Responde en español, tono ejecutivo y técnico. "
                    "No inventes datos. No solicites PII. Usa únicamente el JSON provisto (agregados)."
                )
                user = (
                    "Genera un reporte corto con secciones: Resumen, Riesgos, Acciones recomendadas.\n\n"
                    f"OPS_ALERTS_JSON:\n{json.dumps(alerts_payload, ensure_ascii=False)}\n\n"
                    f"OPS_SUMMARY_JSON:\n{json.dumps(summary_payload, ensure_ascii=False)}\n"
                )
                llm_text = generate_text(system=system, user=user, max_tokens=350)
            except Exception as e:
                llm_error = str(e)

        return Response(
            {
                "success": True,
                "time": summary_payload.get("time"),
                "alerts": alerts_payload,
                "summary": summary_payload,
                "report": report,
                "llm": {"enabled": bool(use_llm), "text": llm_text, "error": llm_error},
            },
            status=status.HTTP_200_OK,
        )

