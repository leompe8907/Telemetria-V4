from __future__ import annotations

from datetime import date
from typing import Any, Optional, Dict

from django.db.models import Count, Sum, Avg, Max, Min, Q

from delancert.analytics.common import DateRange
from delancert.models import MergedTelemetricOTTDelancer
from delancert.utils.cache_utils import cached_result


@cached_result(timeout=300, key_prefix="dashboard_overview")
def overview(range_: Optional[DateRange]) -> Dict[str, Any]:
    qs = MergedTelemetricOTTDelancer.objects.filter(dataDuration__isnull=False)
    if range_ is not None:
        qs = qs.filter(dataDate__gte=range_.start, dataDate__lte=range_.end)

    stats = qs.aggregate(
        total_views=Count("id"),
        unique_users=Count("subscriberCode", distinct=True),
        unique_devices=Count("deviceId", distinct=True),
        unique_channels=Count("dataName", distinct=True, filter=Q(dataName__isnull=False)),
        total_watch_seconds=Sum("dataDuration"),
        avg_duration_seconds=Avg("dataDuration"),
        max_duration_seconds=Max("dataDuration"),
        min_duration_seconds=Min("dataDuration"),
        min_date=Min("dataDate"),
        max_date=Max("dataDate"),
    )

    total_watch_hours = float(stats["total_watch_seconds"] or 0) / 3600.0
    avg_duration_hours = float(stats["avg_duration_seconds"] or 0) / 3600.0
    max_duration_hours = float(stats["max_duration_seconds"] or 0) / 3600.0
    min_duration_hours = float(stats["min_duration_seconds"] or 0) / 3600.0

    if range_ is not None:
        days = (range_.end - range_.start).days + 1
        start = range_.start
        end = range_.end
    else:
        start = stats["min_date"]
        end = stats["max_date"]
        if isinstance(start, date) and isinstance(end, date):
            days = (end - start).days + 1
        else:
            days = 0

    total_views = int(stats["total_views"] or 0)
    avg_views_per_day = round(total_views / days, 2) if days > 0 else 0

    return {
        "range": {
            "start": start.isoformat() if start else None,
            "end": end.isoformat() if end else None,
            "days": days,
        },
        "kpis": {
            "total_views": total_views,
            "unique_users": int(stats["unique_users"] or 0),
            "unique_devices": int(stats["unique_devices"] or 0),
            "unique_channels": int(stats["unique_channels"] or 0),
            "total_watch_hours": round(total_watch_hours, 2),
            "avg_duration_hours": round(avg_duration_hours, 2),
            "max_duration_hours": round(max_duration_hours, 2),
            "min_duration_hours": round(min_duration_hours, 2),
            "avg_views_per_day": avg_views_per_day,
        },
    }

