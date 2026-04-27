from __future__ import annotations

from typing import Any, Dict, List, Optional

from django.db.models import Count, Sum

from delancert.analytics.common import DateRange
from delancert.models import MergedTelemetricOTTDelancer, TelemetryChannelDailyAgg
from delancert.utils.cache_utils import cached_result


@cached_result(timeout=180, key_prefix="dashboard_top_channels")
def top_channels(range_: Optional[DateRange], limit: int = 10) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 100))

    # Fast path: usar agregados diarios cuando hay rango por fechas.
    if range_ is not None:
        agg_qs = TelemetryChannelDailyAgg.objects.filter(day__gte=range_.start, day__lte=range_.end)
        total_views = int(agg_qs.aggregate(v=Sum("views"))["v"] or 0)
        rows = (
            agg_qs.values("channel")
            .annotate(total_views=Sum("views"))
            .order_by("-total_views")[:limit]
        )
        out: List[Dict[str, Any]] = []
        for r in rows:
            tv = int(r["total_views"] or 0)
            pct = (tv / total_views * 100.0) if total_views > 0 else 0.0
            out.append({"channel": r["channel"], "total_views": tv, "percentage": round(pct, 2)})
        return out

    # Fallback: tabla grande (sin rango)
    qs = MergedTelemetricOTTDelancer.objects.filter(dataName__isnull=False)
    total_views = qs.count()
    rows = qs.values("dataName").annotate(total_views=Count("id")).order_by("-total_views")[:limit]

    out: List[Dict[str, Any]] = []
    for r in rows:
        tv = int(r["total_views"] or 0)
        pct = (tv / total_views * 100.0) if total_views > 0 else 0.0
        out.append({"channel": r["dataName"], "total_views": tv, "percentage": round(pct, 2)})
    return out


@cached_result(timeout=300, key_prefix="dashboard_channel_audience")
def channel_audience(range_: Optional[DateRange]) -> List[Dict[str, Any]]:
    qs = MergedTelemetricOTTDelancer.objects.filter(dataName__isnull=False, dataDuration__isnull=False)
    if range_ is not None:
        qs = qs.filter(dataDate__gte=range_.start, dataDate__lte=range_.end)

    rows = (
        qs.values("dataName")
        .annotate(
            unique_devices=Count("deviceId", distinct=True),
            unique_users=Count("subscriberCode", distinct=True),
            total_views=Count("id"),
            total_watch_seconds=Sum("dataDuration"),
        )
        .order_by("-total_views")
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "channel": r["dataName"],
                "unique_devices": r["unique_devices"],
                "unique_users": r["unique_users"],
                "total_views": r["total_views"],
                "watch_hours": round(float(r["total_watch_seconds"] or 0) / 3600.0, 2),
            }
        )
    return out


@cached_result(timeout=300, key_prefix="dashboard_peak_hours_by_channel")
def peak_hours_by_channel(range_: Optional[DateRange], channel: Optional[str] = None) -> List[Dict[str, Any]]:
    qs = MergedTelemetricOTTDelancer.objects.filter(dataName__isnull=False, timeDate__isnull=False)
    if channel:
        qs = qs.filter(dataName=channel)
    if range_ is not None:
        qs = qs.filter(dataDate__gte=range_.start, dataDate__lte=range_.end)

    rows = qs.values("dataName", "timeDate").annotate(views=Count("id")).order_by("dataName", "-views")
    return list(rows)

