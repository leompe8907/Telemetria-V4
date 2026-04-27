from __future__ import annotations

from typing import Any, Dict, List, Optional

from django.db.models import Count, Sum, Avg, Min, Max, Case, When, Value, CharField, FloatField, ExpressionWrapper

from delancert.analytics.common import DateRange
from delancert.models import MergedTelemetricOTTDelancer
from delancert.utils.cache_utils import cached_result


def _base_user_qs(subscriber_code: str, range_: Optional[DateRange]):
    qs = MergedTelemetricOTTDelancer.objects.filter(subscriberCode=subscriber_code, dataDuration__isnull=False)
    if range_ is not None:
        qs = qs.filter(dataDate__gte=range_.start, dataDate__lte=range_.end)
    return qs


@cached_result(timeout=300, key_prefix="dashboard_user_profile")
def user_profile(subscriber_code: str, range_: Optional[DateRange]) -> Dict[str, Any]:
    qs = _base_user_qs(subscriber_code, range_)
    if not qs.exists():
        return {"subscriber_code": subscriber_code, "total_records": 0, "message": "No se encontraron registros"}

    profile = qs.aggregate(
        total_views=Count("id"),
        total_seconds=Sum("dataDuration"),
        unique_channels=Count("dataName", distinct=True),
        unique_devices=Count("deviceId", distinct=True),
        active_days=Count("dataDate", distinct=True),
        avg_duration_seconds=Avg("dataDuration"),
        first_activity=Min("timestamp"),
        last_activity=Max("timestamp"),
    )

    total_hours = float(profile["total_seconds"] or 0) / 3600.0

    top_channels_rows = (
        qs.filter(dataName__isnull=False)
        .values("dataName")
        .annotate(views=Count("id"), total_seconds=Sum("dataDuration"), unique_days=Count("dataDate", distinct=True))
        .order_by("-views")[:10]
    )

    top_channels: List[Dict[str, Any]] = [
        {
            "channel": r["dataName"],
            "views": r["views"],
            "total_hours": round(float(r["total_seconds"] or 0) / 3600.0, 2),
            "active_days": r["unique_days"],
        }
        for r in top_channels_rows
    ]

    time_slot_rows = (
        qs.filter(timeDate__isnull=False)
        .annotate(
            time_slot=Case(
                When(timeDate__gte=0, timeDate__lte=5, then=Value("madrugada")),
                When(timeDate__gte=6, timeDate__lte=11, then=Value("mañana")),
                When(timeDate__gte=12, timeDate__lte=17, then=Value("tarde")),
                default=Value("noche"),
                output_field=CharField(),
            )
        )
        .values("time_slot")
        .annotate(total_seconds=Sum("dataDuration"), total_views=Count("id"))
        .order_by("-total_seconds")
    )

    time_slots: Dict[str, Any] = {k: {"total_hours": 0, "total_views": 0} for k in ["madrugada", "mañana", "tarde", "noche"]}
    for r in time_slot_rows:
        time_slots[r["time_slot"]] = {
            "total_hours": round(float(r["total_seconds"] or 0) / 3600.0, 2),
            "total_views": r["total_views"],
        }

    devices_rows = (
        qs.filter(deviceId__isnull=False)
        .values("deviceId")
        .annotate(views=Count("id"), total_seconds=Sum("dataDuration"))
        .order_by("-views")
    )
    devices = [
        {"device_id": r["deviceId"], "views": r["views"], "total_hours": round(float(r["total_seconds"] or 0) / 3600.0, 2)}
        for r in devices_rows
    ]

    hourly_rows = (
        qs.filter(timeDate__isnull=False)
        .values("timeDate")
        .annotate(views=Count("id"), total_seconds=Sum("dataDuration"))
        .order_by("timeDate")
    )
    hourly = [
        {"hour": r["timeDate"], "views": r["views"], "total_hours": round(float(r["total_seconds"] or 0) / 3600.0, 2)}
        for r in hourly_rows
    ]

    return {
        "subscriber_code": subscriber_code,
        "profile": {
            "total_views": int(profile["total_views"] or 0),
            "total_hours": round(total_hours, 2),
            "unique_channels": int(profile["unique_channels"] or 0),
            "unique_devices": int(profile["unique_devices"] or 0),
            "active_days": int(profile["active_days"] or 0),
            "avg_duration_hours": round(float(profile["avg_duration_seconds"] or 0) / 3600.0, 2),
            "first_activity": profile["first_activity"].isoformat() if profile["first_activity"] else None,
            "last_activity": profile["last_activity"].isoformat() if profile["last_activity"] else None,
        },
        "consumption_behavior": {
            "top_channels": top_channels,
            "preferred_time_slots": time_slots,
            "devices_used": devices,
        },
        "temporal_patterns": {"hourly_activity": hourly},
    }


@cached_result(timeout=300, key_prefix="dashboard_user_range")
def user_range(subscriber_code: str, range_: DateRange) -> Dict[str, Any]:
    qs = _base_user_qs(subscriber_code, range_)
    total_records = qs.count()
    if total_records == 0:
        return {
            "subscriber_code": subscriber_code,
            "period": {"start": range_.start.isoformat(), "end": range_.end.isoformat()},
            "total_records": 0,
            "message": "No se encontraron registros para este usuario en el período seleccionado",
        }

    period_summary = qs.aggregate(
        total_views=Count("id"),
        total_seconds=Sum("dataDuration"),
        unique_channels=Count("dataName", distinct=True),
        unique_devices=Count("deviceId", distinct=True),
        active_days=Count("dataDate", distinct=True),
        avg_duration_seconds=Avg("dataDuration"),
    )

    daily_rows = (
        qs.values("dataDate")
        .annotate(views=Count("id"), total_seconds=Sum("dataDuration"), unique_channels=Count("dataName", distinct=True))
        .order_by("dataDate")
    )
    daily = [
        {
            "date": str(r["dataDate"]),
            "views": r["views"],
            "total_hours": round(float(r["total_seconds"] or 0) / 3600.0, 2),
            "unique_channels": r["unique_channels"],
        }
        for r in daily_rows
    ]

    # comparación con promedio general (en el rango), evitando expresiones inválidas:
    general = MergedTelemetricOTTDelancer.objects.filter(
        dataDate__gte=range_.start, dataDate__lte=range_.end, dataDuration__isnull=False
    ).aggregate(
        total_views=Count("id"),
        total_seconds=Sum("dataDuration"),
        unique_users=Count("subscriberCode", distinct=True),
    )
    unique_users = float(general["unique_users"] or 0)
    avg_views = (float(general["total_views"] or 0) / unique_users) if unique_users > 0 else 0
    avg_hours = ((float(general["total_seconds"] or 0) / 3600.0) / unique_users) if unique_users > 0 else 0

    user_views = int(period_summary["total_views"] or 0)
    user_hours = float(period_summary["total_seconds"] or 0) / 3600.0

    return {
        "subscriber_code": subscriber_code,
        "period": {"start": range_.start.isoformat(), "end": range_.end.isoformat(), "days": (range_.end - range_.start).days + 1},
        "period_summary": {
            "total_views": user_views,
            "total_hours": round(user_hours, 2),
            "unique_channels": int(period_summary["unique_channels"] or 0),
            "unique_devices": int(period_summary["unique_devices"] or 0),
            "active_days": int(period_summary["active_days"] or 0),
            "avg_duration_hours": round(float(period_summary["avg_duration_seconds"] or 0) / 3600.0, 2),
        },
        "temporal_evolution": {"daily_activity": daily},
        "comparison_with_average": {
            "user_views": user_views,
            "avg_views": round(avg_views, 2),
            "user_vs_avg_views_pct": round((user_views / avg_views * 100) if avg_views > 0 else 0, 2),
            "user_hours": round(user_hours, 2),
            "avg_hours": round(avg_hours, 2),
            "user_vs_avg_hours_pct": round((user_hours / avg_hours * 100) if avg_hours > 0 else 0, 2),
        },
    }

