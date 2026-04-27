from django.core.management.base import BaseCommand

from delancert.server.telemetry_fetcher import (
    fetch_telemetry_records_smart,
    save_telemetry_records,
)
from delancert.server.merge7_8 import merge_ott_records


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
        limit = options["limit"]
        batch_size = options["batch_size"]
        process_timestamps = not options["no_process_timestamps"]

        records = fetch_telemetry_records_smart(limit=limit, process_timestamps=process_timestamps)
        self.stdout.write(self.style.SUCCESS(f"Descargados {len(records)} registros nuevos"))

        if records:
            result = save_telemetry_records(records, batch_size=batch_size)
            self.stdout.write(self.style.SUCCESS(f"Guardados: {result.get('saved_records')} | omitidos: {result.get('skipped_records')} | errores: {result.get('errors')}"))

        if options["merge_ott"]:
            merge_result = merge_ott_records(
                batch_size=options["merge_batch_size"],
                backfill_last_n=options["merge_backfill_last_n"],
            )
            self.stdout.write(self.style.SUCCESS(f"Merge OTT: {merge_result}"))

