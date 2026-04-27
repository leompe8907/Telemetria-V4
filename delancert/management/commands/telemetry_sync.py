from django.core.management.base import BaseCommand
from django.utils import timezone

from delancert.server.telemetry_fetcher import (
    fetch_telemetry_records_smart,
    save_telemetry_records,
)
from delancert.server.merge7_8 import merge_ott_records
from delancert.models import TelemetryJobRun


class Command(BaseCommand):
    help = "Sincroniza telemetría desde PanAccess y opcionalmente ejecuta merge OTT (7/8)."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=1000)
        parser.add_argument("--batch-size", type=int, default=1000)
        parser.add_argument("--no-process-timestamps", action="store_true", default=False)
        parser.add_argument("--merge-ott", action="store_true", default=False)
        parser.add_argument("--merge-batch-size", type=int, default=500)
        parser.add_argument("--merge-backfill-last-n", type=int, default=0)

    def handle(self, *args, **options):
        limit = int(options["limit"])
        batch_size = int(options["batch_size"])
        process_timestamps = not bool(options["no_process_timestamps"])

        sync_job = TelemetryJobRun.objects.create(
            job_type=TelemetryJobRun.JobType.SYNC,
            status=TelemetryJobRun.JobStatus.SUCCESS,
            started_at=timezone.now(),
        )

        try:
            records = fetch_telemetry_records_smart(limit=limit, process_timestamps=process_timestamps)
            self.stdout.write(self.style.SUCCESS(f"Descargados {len(records)} registros nuevos"))

            downloaded = len(records)
            saved = 0
            skipped = 0
            errors = 0

            if records:
                result = save_telemetry_records(records, batch_size=batch_size)
                saved = int(result.get("saved_records") or 0)
                skipped = int(result.get("skipped_records") or 0)
                errors = int(result.get("errors") or 0)
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Guardados: {saved} | omitidos: {skipped} | errores: {errors}"
                    )
                )

            finished_at = timezone.now()
            sync_job.finished_at = finished_at
            sync_job.duration_ms = int((finished_at - sync_job.started_at).total_seconds() * 1000)
            sync_job.downloaded = downloaded
            sync_job.saved = saved
            sync_job.skipped = skipped
            sync_job.errors = errors
            sync_job.status = TelemetryJobRun.JobStatus.SUCCESS
            sync_job.save(
                update_fields=["finished_at", "duration_ms", "downloaded", "saved", "skipped", "errors", "status"]
            )
        except Exception as e:
            finished_at = timezone.now()
            sync_job.finished_at = finished_at
            sync_job.duration_ms = int((finished_at - sync_job.started_at).total_seconds() * 1000)
            sync_job.status = TelemetryJobRun.JobStatus.ERROR
            sync_job.error_message = str(e)[:2000]
            sync_job.save(update_fields=["finished_at", "duration_ms", "status", "error_message"])
            raise

        if options["merge_ott"]:
            merge_job = TelemetryJobRun.objects.create(
                job_type=TelemetryJobRun.JobType.MERGE_OTT,
                status=TelemetryJobRun.JobStatus.SUCCESS,
                started_at=timezone.now(),
                merge_backfill_last_n=int(options["merge_backfill_last_n"] or 0),
            )
            try:
                merge_result = merge_ott_records(
                    batch_size=int(options["merge_batch_size"]),
                    backfill_last_n=int(options["merge_backfill_last_n"]),
                )
                self.stdout.write(self.style.SUCCESS(f"Merge OTT: {merge_result}"))
                finished_at = timezone.now()
                merge_job.finished_at = finished_at
                merge_job.duration_ms = int((finished_at - merge_job.started_at).total_seconds() * 1000)
                merge_job.merged_saved = int((merge_result or {}).get("saved_records") or 0)
                merge_job.merged_deleted_existing = int((merge_result or {}).get("deleted_existing") or 0)
                merge_job.status = TelemetryJobRun.JobStatus.SUCCESS
                merge_job.save(
                    update_fields=[
                        "finished_at",
                        "duration_ms",
                        "merged_saved",
                        "merged_deleted_existing",
                        "merge_backfill_last_n",
                        "status",
                    ]
                )
            except Exception as e:
                finished_at = timezone.now()
                merge_job.finished_at = finished_at
                merge_job.duration_ms = int((finished_at - merge_job.started_at).total_seconds() * 1000)
                merge_job.status = TelemetryJobRun.JobStatus.ERROR
                merge_job.error_message = str(e)[:2000]
                merge_job.save(update_fields=["finished_at", "duration_ms", "status", "error_message"])
                raise

