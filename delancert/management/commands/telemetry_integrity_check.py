from __future__ import annotations

from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db.models import Count, Max
from django.utils import timezone

from delancert.models import TelemetryRecordEntryDelancer, MergedTelemetricOTTDelancer
from delancert.models import TelemetryJobRun


class Command(BaseCommand):
    help = "Chequeos de integridad operativa (duplicados, lag merge, nulos críticos)."

    def add_arguments(self, parser):
        parser.add_argument("--hours", type=int, default=24, help="Ventana para conteos recientes.")
        parser.add_argument("--max-duplicate-samples", type=int, default=5)
        parser.add_argument("--fail-on-duplicates", action="store_true", default=False)

    def handle(self, *args, **options):
        job = TelemetryJobRun.objects.create(
            job_type=TelemetryJobRun.JobType.INTEGRITY_CHECK,
            status=TelemetryJobRun.JobStatus.SUCCESS,
            started_at=timezone.now(),
        )
        hours = int(options["hours"])
        since = timezone.now() - timedelta(hours=hours)

        try:
            raw_max = (
                TelemetryRecordEntryDelancer.objects.aggregate(max_record_id=Max("recordId"))["max_record_id"] or 0
            )
            merged_max = (
                MergedTelemetricOTTDelancer.objects.aggregate(max_record_id=Max("recordId"))["max_record_id"] or 0
            )
            lag = max(0, int(raw_max) - int(merged_max))

            raw_recent = TelemetryRecordEntryDelancer.objects.filter(timestamp__gte=since).count()
            merged_recent = MergedTelemetricOTTDelancer.objects.filter(timestamp__gte=since).count()

            self.stdout.write(f"raw_max_record_id={raw_max}")
            self.stdout.write(f"merged_ott_max_record_id={merged_max}")
            self.stdout.write(f"lag_raw_minus_merged_record_id={lag}")
            self.stdout.write(f"raw_count_last_{hours}h={raw_recent}")
            self.stdout.write(f"merged_ott_count_last_{hours}h={merged_recent}")

            # Duplicados (en teoría no deben existir por unique=True, pero si hubo imports manuales/migraciones raras)
            dup_qs = (
                TelemetryRecordEntryDelancer.objects.values("recordId")
                .annotate(c=Count("recordId"))
                .filter(recordId__isnull=False, c__gt=1)
                .order_by("-c")
            )
            dup_count = dup_qs.count()
            self.stdout.write(f"raw_duplicate_recordIds={dup_count}")
            if dup_count:
                samples = list(dup_qs[: int(options["max_duplicate_samples"])])
                self.stdout.write(f"raw_duplicate_samples={samples}")

            merged_dup_qs = (
                MergedTelemetricOTTDelancer.objects.values("recordId")
                .annotate(c=Count("recordId"))
                .filter(recordId__isnull=False, c__gt=1)
                .order_by("-c")
            )
            merged_dup_count = merged_dup_qs.count()
            self.stdout.write(f"merged_ott_duplicate_recordIds={merged_dup_count}")
            if merged_dup_count:
                samples = list(merged_dup_qs[: int(options["max_duplicate_samples"])])
                self.stdout.write(f"merged_ott_duplicate_samples={samples}")

            # Nulos críticos en tabla merged (para analytics)
            null_dataDate = MergedTelemetricOTTDelancer.objects.filter(dataDate__isnull=True).count()
            null_timeDate = MergedTelemetricOTTDelancer.objects.filter(timeDate__isnull=True).count()
            self.stdout.write(f"merged_ott_null_dataDate={null_dataDate}")
            self.stdout.write(f"merged_ott_null_timeDate={null_timeDate}")

            if options["fail_on_duplicates"] and (dup_count or merged_dup_count):
                raise SystemExit(2)

            finished_at = timezone.now()
            job.finished_at = finished_at
            job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
            job.highest_record_id_before = int(raw_max)
            job.highest_record_id_after = int(merged_max)
            job.errors = int(dup_count) + int(merged_dup_count)
            job.status = TelemetryJobRun.JobStatus.SUCCESS
            job.save(update_fields=["finished_at", "duration_ms", "highest_record_id_before", "highest_record_id_after", "errors", "status"])
        except Exception as e:
            finished_at = timezone.now()
            job.finished_at = finished_at
            job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
            job.status = TelemetryJobRun.JobStatus.ERROR
            job.error_message = str(e)[:2000]
            job.save(update_fields=["finished_at", "duration_ms", "status", "error_message"])
            raise

