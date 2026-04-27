from __future__ import annotations

from typing import Any, Dict, List, Optional

from django.db.models import Count, Sum, Avg, Min, Max

from delancert.analytics.common import DateRange
from delancert.models import MergedTelemetricOTTDelancer
from delancert.utils.cache_utils import cached_result


@cached_result(timeout=600, key_prefix="dashboard_users_general")
def users_general(range_: Optional[DateRange]) -> Dict[str, Any]:
    """
    Versión optimizada del análisis general de usuarios.

    Objetivo: NO cargar todos los usuarios a memoria.
    Estrategia:
    - Agregados globales con aggregate()
    - Top-N usuarios con queries limitadas
    - Segmentación aproximada basada en buckets de horas (configurable en frontend)
    """
    qs = MergedTelemetricOTTDelancer.objects.filter(subscriberCode__isnull=False, dataDuration__isnull=False)
    if range_ is not None:
        qs = qs.filter(dataDate__gte=range_.start, dataDate__lte=range_.end)

    global_stats = qs.aggregate(
        total_views=Count("id"),
        total_seconds=Sum("dataDuration"),
        unique_users=Count("subscriberCode", distinct=True),
        unique_devices=Count("deviceId", distinct=True),
        unique_channels=Count("dataName", distinct=True),
        min_date=Min("dataDate"),
        max_date=Max("dataDate"),
    )

    unique_users = int(global_stats["unique_users"] or 0)
    total_views = int(global_stats["total_views"] or 0)
    total_hours = float(global_stats["total_seconds"] or 0) / 3600.0

    avg_views_per_user = (total_views / unique_users) if unique_users > 0 else 0
    avg_hours_per_user = (total_hours / unique_users) if unique_users > 0 else 0

    # Top usuarios por horas
    top_by_hours_rows = (
        qs.values("subscriberCode")
        .annotate(total_seconds=Sum("dataDuration"), total_views=Count("id"))
        .order_by("-total_seconds")[:10]
    )
    top_by_hours = [
        {
            "subscriber_code": r["subscriberCode"],
            "total_hours": round(float(r["total_seconds"] or 0) / 3600.0, 2),
            "total_views": r["total_views"],
        }
        for r in top_by_hours_rows
    ]

    # Top usuarios por views
    top_by_views_rows = (
        qs.values("subscriberCode")
        .annotate(total_seconds=Sum("dataDuration"), total_views=Count("id"))
        .order_by("-total_views")[:10]
    )
    top_by_views = [
        {
            "subscriber_code": r["subscriberCode"],
            "total_views": r["total_views"],
            "total_hours": round(float(r["total_seconds"] or 0) / 3600.0, 2),
        }
        for r in top_by_views_rows
    ]

    # Segmentación aproximada (buckets de horas por usuario) - calculado en DB por usuario, pero no se trae todo:
    # En v1 devolvemos solo valores globales + top lists y dejamos la segmentación full para v2 (materialized).

    return {
        "range": {
            "start": (range_.start.isoformat() if range_ else (global_stats["min_date"].isoformat() if global_stats["min_date"] else None)),
            "end": (range_.end.isoformat() if range_ else (global_stats["max_date"].isoformat() if global_stats["max_date"] else None)),
        },
        "aggregate_stats": {
            "unique_users": unique_users,
            "unique_devices": int(global_stats["unique_devices"] or 0),
            "unique_channels": int(global_stats["unique_channels"] or 0),
            "total_views_all_users": total_views,
            "total_hours_all_users": round(total_hours, 2),
            "avg_views_per_user": round(avg_views_per_user, 2),
            "avg_hours_per_user": round(avg_hours_per_user, 2),
        },
        "top_users": {"by_hours": top_by_hours, "by_views": top_by_views},
        "segmentation": {
            "note": "v1: segmentación completa pendiente. Se recomienda materializar agregados por usuario/día para segmentación eficiente.",
        },
    }

