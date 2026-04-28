from __future__ import annotations

import os
from dataclasses import dataclass

from celery import shared_task
from django.core.cache import cache


@dataclass(frozen=True)
class TaskLock:
    acquired: bool
    key: str


def _acquire_task_lock(name: str, ttl_seconds: int) -> TaskLock:
    """
    Lock distribuido best-effort usando cache (Redis recomendado).
    - Evita solapamiento de tareas pesadas (sync/merge/aggregates/ML).
    - Si el cache falla, no bloquea el job (degradación controlada).
    """
    ttl = max(1, int(ttl_seconds))
    key = f"telemetria:tasklock:{name}"
    try:
        ok = bool(cache.add(key, "1", timeout=ttl))
        return TaskLock(acquired=ok, key=key)
    except Exception:
        return TaskLock(acquired=True, key=key)


def _release_task_lock(lock: TaskLock) -> None:
    try:
        cache.delete(lock.key)
    except Exception:
        pass


@shared_task(bind=True, name="telemetria.telemetry_run")
def telemetry_run_task(
    self,
    *,
    limit: int = 1000,
    batch_size: int = 1000,
    process_timestamps: bool = True,
    merge_batch_size: int = 500,
    backfill_last_n: int = 0,
    lock_ttl_seconds: int = 900,
):
    from delancert.server.telemetry_fetcher import (
        fetch_telemetry_records_smart,
        save_telemetry_records,
        get_highest_record_id,
        is_database_empty,
    )
    from delancert.server.merge7_8 import merge_ott_records
    from delancert.models import TelemetryJobRun
    from django.utils import timezone

    lock = _acquire_task_lock("telemetry_run", ttl_seconds=lock_ttl_seconds)
    if not lock.acquired:
        return {"skipped": True, "reason": "locked"}

    job = TelemetryJobRun.objects.create(
        job_type=TelemetryJobRun.JobType.RUN,
        status=TelemetryJobRun.JobStatus.SUCCESS,
        started_at=timezone.now(),
        merge_backfill_last_n=int(backfill_last_n or 0),
    )
    try:
        highest_id_before = get_highest_record_id()

        records = fetch_telemetry_records_smart(limit=int(limit), process_timestamps=bool(process_timestamps))
        downloaded = len(records)
        saved = skipped = errors = 0
        if records:
            save_result = save_telemetry_records(records, batch_size=int(batch_size))
            saved = int(save_result.get("saved_records") or 0)
            skipped = int(save_result.get("skipped_records") or 0)
            errors = int(save_result.get("errors") or 0)

        highest_id_after = get_highest_record_id()

        merge_result = merge_ott_records(batch_size=int(merge_batch_size), backfill_last_n=int(backfill_last_n))

        finished_at = timezone.now()
        job.finished_at = finished_at
        job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
        job.downloaded = downloaded
        job.saved = saved
        job.skipped = skipped
        job.errors = errors
        job.highest_record_id_before = highest_id_before
        job.highest_record_id_after = highest_id_after
        job.merged_saved = int((merge_result or {}).get("saved_records") or 0)
        job.merged_deleted_existing = int((merge_result or {}).get("deleted_existing") or 0)
        job.merge_backfill_last_n = int(backfill_last_n or 0)
        job.status = TelemetryJobRun.JobStatus.SUCCESS
        job.save(
            update_fields=[
                "finished_at",
                "duration_ms",
                "downloaded",
                "saved",
                "skipped",
                "errors",
                "highest_record_id_before",
                "highest_record_id_after",
                "merged_saved",
                "merged_deleted_existing",
                "merge_backfill_last_n",
                "status",
            ]
        )

        return {
            "downloaded": downloaded,
            "saved": saved,
            "skipped": skipped,
            "errors": errors,
            "merge_ott": merge_result,
            "highest_record_id_before": highest_id_before,
            "highest_record_id_after": highest_id_after,
            "was_empty_before": bool(is_database_empty()),  # señal informativa
        }
    except Exception as e:
        from django.utils import timezone

        finished_at = timezone.now()
        job.finished_at = finished_at
        job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
        job.status = TelemetryJobRun.JobStatus.ERROR
        job.error_message = str(e)[:2000]
        job.save(update_fields=["finished_at", "duration_ms", "status", "error_message"])
        raise
    finally:
        _release_task_lock(lock)


@shared_task(bind=True, name="telemetria.build_aggregates")
def telemetry_build_aggregates_task(self, *, days: int = 7, lock_ttl_seconds: int = 1200):
    from datetime import date, timedelta

    from django.db import transaction
    from django.db.models import Count, Sum
    from django.utils import timezone

    from delancert.models import (
        MergedTelemetricOTTDelancer,
        TelemetryChannelDailyAgg,
        TelemetryUserDailyAgg,
        TelemetryJobRun,
    )

    lock = _acquire_task_lock("telemetry_build_aggregates", ttl_seconds=lock_ttl_seconds)
    if not lock.acquired:
        return {"skipped": True, "reason": "locked"}

    days = max(1, int(days))
    today = timezone.localdate()
    start_day: date = today - timedelta(days=days - 1)

    job = TelemetryJobRun.objects.create(
        job_type=TelemetryJobRun.JobType.RUN,
        status=TelemetryJobRun.JobStatus.SUCCESS,
        started_at=timezone.now(),
    )
    try:
        qs = MergedTelemetricOTTDelancer.objects.filter(dataDate__gte=start_day, dataDate__isnull=False)

        channel_rows = (
            qs.exclude(dataName__isnull=True)
            .exclude(dataName="")
            .values("dataDate", "dataName")
            .annotate(
                views=Count("id"),
                unique_users=Count("subscriberCode", distinct=True),
                total_duration_seconds=Sum("dataDuration"),
            )
        )

        user_rows = (
            qs.exclude(subscriberCode__isnull=True)
            .exclude(subscriberCode="")
            .values("dataDate", "subscriberCode")
            .annotate(
                views=Count("id"),
                unique_channels=Count("dataName", distinct=True),
                total_duration_seconds=Sum("dataDuration"),
            )
        )

        with transaction.atomic():
            TelemetryChannelDailyAgg.objects.filter(day__gte=start_day).delete()
            TelemetryUserDailyAgg.objects.filter(day__gte=start_day).delete()

            TelemetryChannelDailyAgg.objects.bulk_create(
                [
                    TelemetryChannelDailyAgg(
                        day=r["dataDate"],
                        channel=r["dataName"],
                        views=int(r["views"] or 0),
                        unique_users=int(r["unique_users"] or 0),
                        total_duration_seconds=int(r["total_duration_seconds"] or 0),
                    )
                    for r in channel_rows
                ],
                batch_size=1000,
            )

            TelemetryUserDailyAgg.objects.bulk_create(
                [
                    TelemetryUserDailyAgg(
                        day=r["dataDate"],
                        subscriber_code=r["subscriberCode"],
                        views=int(r["views"] or 0),
                        unique_channels=int(r["unique_channels"] or 0),
                        total_duration_seconds=int(r["total_duration_seconds"] or 0),
                    )
                    for r in user_rows
                ],
                batch_size=1000,
            )

        finished_at = timezone.now()
        job.finished_at = finished_at
        job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
        job.status = TelemetryJobRun.JobStatus.SUCCESS
        job.save(update_fields=["finished_at", "duration_ms", "status"])

        return {
            "start_day": start_day.isoformat(),
            "days": days,
            "channels": TelemetryChannelDailyAgg.objects.filter(day__gte=start_day).count(),
            "users": TelemetryUserDailyAgg.objects.filter(day__gte=start_day).count(),
        }
    except Exception as e:
        from django.utils import timezone

        finished_at = timezone.now()
        job.finished_at = finished_at
        job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
        job.status = TelemetryJobRun.JobStatus.ERROR
        job.error_message = str(e)[:2000]
        job.save(update_fields=["finished_at", "duration_ms", "status", "error_message"])
        raise
    finally:
        _release_task_lock(lock)

