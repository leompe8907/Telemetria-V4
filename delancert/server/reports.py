from __future__ import annotations

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from delancert.models import TelemetryAgentReport
from delancert.utils.api_key_authentication import TelemetryApiKeyAuthentication
from delancert.utils.api_key_permission import HasTelemetryReadApiKey, HasTelemetryWriteApiKey
from delancert.utils.rate_limit import acquire_rate_limit
from delancert.server.noc import NocRecommendationsView
from delancert.server.analyst import OpsAnalystReportView


def _severity_from_alerts(alerts_payload: dict) -> str:
    alerts = alerts_payload.get("alerts") or []
    sev = "info"
    for a in alerts:
        s = (a or {}).get("severity")
        if s == "critical":
            return "critical"
        if s == "warning":
            sev = "warning"
    return sev


class AgentReportsListView(APIView):
    """
    Lista reportes persistidos.
    GET /delancert/ops/reports/?type=noc|analyst&limit=50
    """

    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request):
        limit = int(request.query_params.get("limit", 50))
        limit = max(1, min(limit, 200))
        rtype = (request.query_params.get("type") or "").strip().lower()

        qs = TelemetryAgentReport.objects.all().order_by("-created_at")
        if rtype in ("noc", "analyst"):
            qs = qs.filter(report_type=rtype)
        qs = qs[:limit]

        data = [
            {
                "id": r.id,
                "report_type": r.report_type,
                "severity": r.severity,
                "title": r.title,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "llm_enabled": bool(r.llm_enabled),
                "llm_model": r.llm_model,
            }
            for r in qs
        ]
        return Response({"success": True, "reports": data}, status=status.HTTP_200_OK)


class AgentReportDetailView(APIView):
    """
    Detalle de un reporte persistido.
    GET /delancert/ops/reports/<id>/
    """

    permission_classes = [HasTelemetryReadApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def get(self, request, report_id: int):
        r = TelemetryAgentReport.objects.filter(id=int(report_id)).first()
        if not r:
            return Response({"success": False, "error": "Not found."}, status=status.HTTP_404_NOT_FOUND)
        return Response(
            {
                "success": True,
                "id": r.id,
                "report_type": r.report_type,
                "severity": r.severity,
                "title": r.title,
                "report_md": r.report_md,
                "ops_alerts": r.ops_alerts,
                "ops_summary": r.ops_summary,
                "llm": {"enabled": bool(r.llm_enabled), "model": r.llm_model, "error": r.llm_error},
                "created_at": r.created_at.isoformat() if r.created_at else None,
            },
            status=status.HTTP_200_OK,
        )


class NocRunAndPersistView(APIView):
    """
    Genera el output del NOC y lo persiste en BD.
    POST /delancert/ops/noc/run/
    """

    permission_classes = [HasTelemetryWriteApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def post(self, request):
        rl = acquire_rate_limit("noc_run_persist", ttl_seconds=10)
        if not rl.allowed:
            return Response(
                {"success": False, "error": "Rate limited", "retry_after_seconds": rl.retry_after_seconds},
                status=status.HTTP_429_TOO_MANY_REQUESTS,
                headers={"Retry-After": str(rl.retry_after_seconds)},
            )

        payload = NocRecommendationsView().get(request).data
        alerts_payload = payload.get("alerts") or {}
        summary_payload = payload.get("summary") or {}

        sev = _severity_from_alerts(alerts_payload)
        recs = payload.get("recommendations") or []
        lines = []
        for rec in recs:
            title = (rec or {}).get("title") or (rec or {}).get("code") or "recommendation"
            lines.append(f"### {title}")
            rationale = (rec or {}).get("rationale") or ""
            if rationale:
                lines.append(rationale)
            actions = (rec or {}).get("actions") or []
            if actions:
                lines.append("**Acciones sugeridas:**")
                for a in actions:
                    lines.append(f"- {a}")
            lines.append("")
        report_md = "## NOC Recommendations\n\n" + "\n".join(lines).strip()

        r = TelemetryAgentReport.objects.create(
            report_type=TelemetryAgentReport.ReportType.NOC,
            severity=sev,
            title="NOC recommendations",
            report_md=report_md,
            ops_alerts=alerts_payload,
            ops_summary=summary_payload,
            llm_enabled=False,
            llm_model=None,
            llm_error=None,
        )
        return Response({"success": True, "id": r.id}, status=status.HTTP_201_CREATED)


class AnalystRunAndPersistView(APIView):
    """
    Genera el reporte del Analista y lo persiste en BD.
    POST /delancert/ops/analyst/run/
    Body opcional:
      - use_llm: bool
    """

    permission_classes = [HasTelemetryWriteApiKey]
    authentication_classes = [TelemetryApiKeyAuthentication]

    def post(self, request):
        rl = acquire_rate_limit("analyst_run_persist", ttl_seconds=10)
        if not rl.allowed:
            return Response(
                {"success": False, "error": "Rate limited", "retry_after_seconds": rl.retry_after_seconds},
                status=status.HTTP_429_TOO_MANY_REQUESTS,
                headers={"Retry-After": str(rl.retry_after_seconds)},
            )

        use_llm = request.data.get("use_llm", False)
        if isinstance(use_llm, str):
            use_llm = use_llm.strip().lower() in ("1", "true", "yes")

        # Reusar el mismo core del view, replicando su lógica sin mutar request.
        from delancert.server.ops import TelemetryOpsAlertsView, TelemetryOpsSummaryView
        from delancert.server.analyst import _deterministic_narrative

        alerts_payload = TelemetryOpsAlertsView().get(request).data
        summary_payload = TelemetryOpsSummaryView().get(request).data

        alerts = alerts_payload.get("alerts") or []
        report = _deterministic_narrative(alerts=alerts, summary=summary_payload)

        llm_text = None
        llm_error = None
        llm_model = None
        if use_llm:
            try:
                from delancert.utils.llm_client import generate_text, get_llm_config
                import json as _json

                cfg = get_llm_config()
                llm_model = cfg.model if cfg else None

                system = (
                    "Eres un analista SRE/ML-Ops. Responde en español, tono ejecutivo y técnico. "
                    "No inventes datos. No solicites PII. Usa únicamente el JSON provisto (agregados)."
                )
                user = (
                    "Genera un reporte corto con secciones: Resumen, Riesgos, Acciones recomendadas.\n\n"
                    f"OPS_ALERTS_JSON:\n{_json.dumps(alerts_payload, ensure_ascii=False)}\n\n"
                    f"OPS_SUMMARY_JSON:\n{_json.dumps(summary_payload, ensure_ascii=False)}\n"
                )
                llm_text = generate_text(system=system, user=user, max_tokens=350)
            except Exception as e:
                llm_error = str(e)

        payload = {
            "alerts": alerts_payload,
            "summary": summary_payload,
            "report": report,
            "llm": {"enabled": bool(use_llm), "text": llm_text, "error": llm_error, "model": llm_model},
        }
        alerts_payload = payload.get("alerts") or {}
        summary_payload = payload.get("summary") or {}
        report = payload.get("report") or {}
        llm = payload.get("llm") or {}

        sev = _severity_from_alerts(alerts_payload)
        report_md = "\n\n".join(
            [
                "## Resumen",
                report.get("highlights_md") or "",
                "## Riesgos",
                report.get("risks_md") or "",
                "## Próximas acciones",
                report.get("next_actions_md") or "",
                "## LLM (opcional)",
                (llm.get("text") or "") if llm.get("enabled") else "",
            ]
        ).strip()

        r = TelemetryAgentReport.objects.create(
            report_type=TelemetryAgentReport.ReportType.ANALYST,
            severity=sev,
            title="Ops analyst report",
            report_md=report_md,
            ops_alerts=alerts_payload,
            ops_summary=summary_payload,
            llm_enabled=bool(llm.get("enabled")),
            llm_model=llm.get("model"),
            llm_error=llm.get("error"),
        )
        return Response({"success": True, "id": r.id}, status=status.HTTP_201_CREATED)

