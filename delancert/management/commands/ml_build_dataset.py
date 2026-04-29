from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

from django.core.management.base import BaseCommand
from django.db.models import Sum
from django.utils import timezone

from delancert.models import MergedTelemetricOTTDelancer, TelemetryUserDailyAgg, TelemetryJobRun


@dataclass(frozen=True)
class Window:
    start: date
    end: date  # inclusive


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _window(end_day: date, days: int) -> Window:
    days = max(1, int(days))
    start = end_day - timedelta(days=days - 1)
    return Window(start=start, end=end_day)


def _future_window(as_of: date, horizon_days: int) -> Window:
    horizon_days = max(1, int(horizon_days))
    start = as_of + timedelta(days=1)
    end = as_of + timedelta(days=horizon_days)
    return Window(start=start, end=end)


class Command(BaseCommand):
    help = "Construye dataset tabular (features) y target watch-time futuro desde la BD."

    def add_arguments(self, parser):
        parser.add_argument("--as-of", type=str, default=None, help="Fecha cutoff (YYYY-MM-DD).")
        parser.add_argument(
            "--as-of-start",
            type=str,
            default=None,
            help="Inicio de rango de as_of (YYYY-MM-DD). Si se define junto a --as-of-end, genera un dataset multi-fecha.",
        )
        parser.add_argument(
            "--as-of-end",
            type=str,
            default=None,
            help="Fin de rango de as_of (YYYY-MM-DD). Si se define junto a --as-of-start, genera un dataset multi-fecha.",
        )
        parser.add_argument("--lookback-days", type=int, default=7, help="Ventana de features hacia atrás.")
        parser.add_argument("--horizon-days", type=int, default=7, help="Horizonte del target hacia adelante.")
        parser.add_argument(
            "--output",
            type=str,
            default="artifacts/ml/datasets/watch_time_7d.csv",
            help="Ruta CSV de salida (relativa al backend).",
        )
        parser.add_argument(
            "--min-history-days",
            type=int,
            default=1,
            help="Filtra usuarios con al menos N días con data en lookback.",
        )

    def handle(self, *args, **options):
        as_of_str = options["as_of"]
        as_of_start = options["as_of_start"]
        as_of_end = options["as_of_end"]
        lookback_days = int(options["lookback_days"])
        horizon_days = int(options["horizon_days"])
        out_path = Path(str(options["output"]))
        min_history_days = max(1, int(options["min_history_days"]))

        job = TelemetryJobRun.objects.create(
            job_type=TelemetryJobRun.JobType.ML_BUILD_DATASET,
            status=TelemetryJobRun.JobStatus.SUCCESS,
            started_at=timezone.now(),
        )

        out_path.parent.mkdir(parents=True, exist_ok=True)

        # Resolver as_of (single) o rango (multi)
        if as_of_start and as_of_end:
            start = _parse_date(str(as_of_start))
            end = _parse_date(str(as_of_end))
            if end < start:
                raise SystemExit("--as-of-end debe ser >= --as-of-start")
            as_of_days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
        else:
            as_of = _parse_date(as_of_str) if as_of_str else timezone.localdate()
            as_of_days = [as_of]

        # Escribir CSV (sin pandas para que sea estable en server)
        import csv

        with out_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "as_of",
                    "subscriber_code",
                    "feature_start",
                    "feature_end",
                    "target_start",
                    "target_end",
                    "x_views",
                    "x_unique_channels_sum",
                    "x_watch_seconds",
                    "x_active_days",
                    "y_watch_seconds_next_horizon",
                ],
            )
            w.writeheader()
            n = 0
            from django.db.models import Count

            for as_of_day in as_of_days:
                feature_win = _window(as_of_day, lookback_days)
                target_win = _future_window(as_of_day, horizon_days)

                # Features desde agregados diarios por usuario (rápido)
                f_qs = TelemetryUserDailyAgg.objects.filter(day__gte=feature_win.start, day__lte=feature_win.end)
                f_rows = (
                    f_qs.values("subscriber_code")
                    .annotate(
                        feat_views=Sum("views"),
                        feat_unique_channels=Sum("unique_channels"),
                        feat_watch_seconds=Sum("total_duration_seconds"),
                        feat_active_days=Count("day"),
                    )
                    .filter(feat_active_days__gte=min_history_days)
                )

                # Target exacto desde merged OTT (sum(dataDuration) en ventana futura)
                t_rows = (
                    MergedTelemetricOTTDelancer.objects.filter(
                        dataDate__gte=target_win.start,
                        dataDate__lte=target_win.end,
                        subscriberCode__isnull=False,
                    )
                    .values("subscriberCode")
                    .annotate(y_watch_seconds=Sum("dataDuration"))
                )
                y_by_user = {r["subscriberCode"]: int(r["y_watch_seconds"] or 0) for r in t_rows}

                for r in f_rows.iterator():
                    u = r["subscriber_code"]
                    row = {
                        "as_of": as_of_day.isoformat(),
                        "subscriber_code": u,
                        "feature_start": feature_win.start.isoformat(),
                        "feature_end": feature_win.end.isoformat(),
                        "target_start": target_win.start.isoformat(),
                        "target_end": target_win.end.isoformat(),
                        "x_views": int(r["feat_views"] or 0),
                        "x_unique_channels_sum": int(r["feat_unique_channels"] or 0),
                        "x_watch_seconds": int(r["feat_watch_seconds"] or 0),
                        "x_active_days": int(r["feat_active_days"] or 0),
                        "y_watch_seconds_next_horizon": int(y_by_user.get(u, 0)),
                    }
                    w.writerow(row)
                    n += 1

        finished_at = timezone.now()
        job.finished_at = finished_at
        job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
        job.saved = n
        job.status = TelemetryJobRun.JobStatus.SUCCESS
        job.save(update_fields=["finished_at", "duration_ms", "saved", "status"])

        self.stdout.write(
            self.style.SUCCESS(
                f"OK ml_build_dataset as_of={as_of_days[-1]} lookback_days={lookback_days} horizon_days={horizon_days} "
                f"rows={n} output={out_path.as_posix()}"
            )
        )

