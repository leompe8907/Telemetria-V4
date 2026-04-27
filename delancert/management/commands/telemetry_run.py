from __future__ import annotations

from django.core.management.base import BaseCommand
from django.utils import timezone

from delancert.models import TelemetryJobRun
from delancert.server.telemetry_fetcher import (
    fetch_telemetry_records_smart,
    save_telemetry_records,
    get_highest_record_id,
    is_database_empty,
)
from delancert.server.merge7_8 import merge_ott_records


class Command(BaseCommand):
    help = "Ejecuta sync incremental + merge OTT (7/8) y registra auditoría (TelemetryJobRun)."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=1000)
        parser.add_argument("--batch-size", type=int, default=1000)
        parser.add_argument("--no-process-timestamps", action="store_true", default=False)
        parser.add_argument("--merge-batch-size", type=int, default=500)
        parser.add_argument("--backfill-last-n", type=int, default=0)

    def handle(self, *args, **options):
        limit = int(options["limit"])
        batch_size = int(options["batch_size"])
        process_timestamps = not bool(options["no_process_timestamps"])
        merge_batch_size = int(options["merge_batch_size"])
        backfill_last_n = int(options["backfill_last_n"])

        job = TelemetryJobRun.objects.create(
            job_type=TelemetryJobRun.JobType.RUN,
            status=TelemetryJobRun.JobStatus.SUCCESS,
            started_at=timezone.now(),
            merge_backfill_last_n=backfill_last_n,
        )

        try:
            was_empty_before = is_database_empty()
            highest_id_before = get_highest_record_id()

            records = fetch_telemetry_records_smart(limit=limit, process_timestamps=process_timestamps)
            downloaded = len(records)
            saved = 0
            skipped = 0
            errors = 0

            if records:
                save_result = save_telemetry_records(records, batch_size=batch_size)
                saved = int(save_result.get("saved_records") or 0)
                skipped = int(save_result.get("skipped_records") or 0)
                errors = int(save_result.get("errors") or 0)

            highest_id_after = get_highest_record_id()

            merge_result = merge_ott_records(batch_size=merge_batch_size, backfill_last_n=backfill_last_n)

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
            job.merge_backfill_last_n = backfill_last_n
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

            self.stdout.write(
                self.style.SUCCESS(
                    f"OK telemetry_run | downloaded={downloaded} saved={saved} skipped={skipped} errors={errors} "
                    f"| was_empty_before={was_empty_before} highest_before={highest_id_before} highest_after={highest_id_after} "
                    f"| merge_saved={job.merged_saved} backfill_last_n={backfill_last_n}"
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

