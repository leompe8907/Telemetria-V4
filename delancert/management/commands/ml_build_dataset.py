from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

from django.core.management.base import BaseCommand
from django.db.models import Sum
from django.utils import timezone

from delancert.models import MergedTelemetricOTTDelancer, TelemetryUserDailyAgg


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
        lookback_days = int(options["lookback_days"])
        horizon_days = int(options["horizon_days"])
        out_path = Path(str(options["output"]))
        min_history_days = max(1, int(options["min_history_days"]))

        as_of = _parse_date(as_of_str) if as_of_str else timezone.localdate()

        feature_win = _window(as_of, lookback_days)
        target_win = _future_window(as_of, horizon_days)

        out_path.parent.mkdir(parents=True, exist_ok=True)

        # Features desde agregados diarios por usuario (rápido)
        f_qs = TelemetryUserDailyAgg.objects.filter(day__gte=feature_win.start, day__lte=feature_win.end)

        # Usuarios con historia mínima en ventana
        # (sin Count para mantener dependencias mínimas; hacemos 1 query por user en el dict)
        # Alternativa: annotate(days=Count('day')).
        from django.db.models import Count

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
        # Nota: dataDuration está en segundos.
        # Hacemos un group-by por subscriberCode.
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
            for r in f_rows.iterator():
                u = r["subscriber_code"]
                row = {
                    "as_of": as_of.isoformat(),
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

        self.stdout.write(
            self.style.SUCCESS(
                f"OK ml_build_dataset as_of={as_of} lookback_days={lookback_days} horizon_days={horizon_days} "
                f"rows={n} output={out_path.as_posix()}"
            )
        )

