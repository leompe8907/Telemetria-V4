from __future__ import annotations

from typing import Any, Dict, List, Optional

from django.db.models import Count, Sum
from django.db.models.functions import TruncDate, TruncWeek, TruncMonth

from delancert.analytics.common import DateRange
from delancert.models import MergedTelemetricOTTDelancer, TelemetryChannelDailyAgg
from delancert.utils.cache_utils import cached_result


@cached_result(timeout=600, key_prefix="dashboard_temporal")
def temporal(range_: Optional[DateRange], period: str = "daily") -> List[Dict[str, Any]]:
    # Fast path: daily desde agregados (solo si hay rango).
    if period == "daily" and range_ is not None:
        agg_qs = TelemetryChannelDailyAgg.objects.filter(day__gte=range_.start, day__lte=range_.end)
        rows = (
            agg_qs.values("day")
            .annotate(views=Sum("views"), watch_seconds=Sum("total_duration_seconds"))
            .order_by("day")
        )
        return [
            {
                "period": r["day"].isoformat(),
                "views": int(r["views"] or 0),
                "watch_hours": round(float(r["watch_seconds"] or 0) / 3600.0, 2),
            }
            for r in rows
        ]

    qs = MergedTelemetricOTTDelancer.objects.filter(dataDate__isnull=False, dataDuration__isnull=False)
    if range_ is not None:
        qs = qs.filter(dataDate__gte=range_.start, dataDate__lte=range_.end)

    if period == "daily":
        bucket = TruncDate("dataDate")
    elif period == "weekly":
        bucket = TruncWeek("dataDate")
    elif period == "monthly":
        bucket = TruncMonth("dataDate")
    else:
        raise ValueError("period debe ser daily|weekly|monthly")

    rows = (
        qs.annotate(bucket=bucket)
        .values("bucket")
        .annotate(views=Count("id"), watch_seconds=Sum("dataDuration"))
        .order_by("bucket")
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "period": r["bucket"].date().isoformat() if hasattr(r["bucket"], "date") else str(r["bucket"]),
                "views": r["views"],
                "watch_hours": round(float(r["watch_seconds"] or 0) / 3600.0, 2),
            }
        )
    return out

