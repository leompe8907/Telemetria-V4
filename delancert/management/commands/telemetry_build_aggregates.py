from __future__ import annotations

from datetime import date, timedelta

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Count, Sum
from django.utils import timezone

from delancert.models import (
    MergedTelemetricOTTDelancer,
    TelemetryChannelDailyAgg,
    TelemetryUserDailyAgg,
    TelemetryJobRun,
)


class Command(BaseCommand):
    help = "Materializa agregados diarios (canal/usuario) desde MergedTelemetricOTTDelancer."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=7, help="Recalcular ventana de últimos N días.")

    def handle(self, *args, **options):
        days = max(1, int(options["days"]))
        today = timezone.localdate()
        start_day: date = today - timedelta(days=days - 1)

        job = TelemetryJobRun.objects.create(
            job_type=TelemetryJobRun.JobType.RUN,
            status=TelemetryJobRun.JobStatus.SUCCESS,
            started_at=timezone.now(),
        )
        # Nota: reutilizamos JobType.RUN por ahora (agregados se recomputan a demanda).

        try:
            qs = MergedTelemetricOTTDelancer.objects.filter(dataDate__gte=start_day, dataDate__isnull=False)

            # Channel daily
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

            # User daily
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

            self.stdout.write(
                self.style.SUCCESS(
                    f"OK telemetry_build_aggregates days={days} start_day={start_day} "
                    f"channels={TelemetryChannelDailyAgg.objects.filter(day__gte=start_day).count()} "
                    f"users={TelemetryUserDailyAgg.objects.filter(day__gte=start_day).count()}"
                )
            )
        except Exception as e:
            finished_at = timezone.now()
            job.finished_at = finished_at
            job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
            job.status = TelemetryJobRun.JobStatus.ERROR
            job.error_message = str(e)[:2000]
            job.save(update_fields=["finished_at", "duration_ms", "status", "error_message"])
            raise

