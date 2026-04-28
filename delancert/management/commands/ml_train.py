from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone as dt_timezone
from pathlib import Path
from typing import Dict, List, Tuple

from django.core.management.base import BaseCommand
from django.utils import timezone

from delancert.models import TelemetryJobRun


@dataclass(frozen=True)
class TrainResult:
    model_dir: Path
    n_rows: int
    n_train: int
    n_test: int
    mae: float
    rmse: float


def _read_dataset_csv(path: Path) -> Tuple[List[str], List[List[float]], List[float]]:
    """
    Lee el CSV producido por ml_build_dataset.
    Retorna (feature_names, X, y).
    """
    import csv

    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        rows = list(r)

    feature_names = ["x_views", "x_unique_channels_sum", "x_watch_seconds", "x_active_days"]
    X: List[List[float]] = []
    y: List[float] = []
    for row in rows:
        X.append([float(row.get(c) or 0) for c in feature_names])
        y.append(float(row.get("y_watch_seconds_next_horizon") or 0))
    return feature_names, X, y


def _train_test_split(X: List[List[float]], y: List[float], test_size: float = 0.2, seed: int = 42):
    # Split determinista sin depender de pandas
    import random

    n = len(X)
    idx = list(range(n))
    rng = random.Random(seed)
    rng.shuffle(idx)
    n_test = max(1, int(n * test_size)) if n > 1 else 0
    test_idx = set(idx[:n_test])
    X_train, y_train, X_test, y_test = [], [], [], []
    for i in range(n):
        if i in test_idx:
            X_test.append(X[i])
            y_test.append(y[i])
        else:
            X_train.append(X[i])
            y_train.append(y[i])
    return X_train, X_test, y_train, y_test


def _fit_and_eval(feature_names: List[str], X: List[List[float]], y: List[float], out_dir: Path) -> TrainResult:
    import math

    from joblib import dump
    from sklearn.compose import TransformedTargetRegressor
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    import numpy as np

    X_train, X_test, y_train, y_test = _train_test_split(X, y, test_size=0.2, seed=42)

    # Modelo robusto para tabular (baseline)
    base = HistGradientBoostingRegressor(
        learning_rate=0.1,
        max_depth=6,
        max_iter=200,
        random_state=42,
    )

    # Transformación de target para heavy-tail (duraciones).
    # Usamos funciones de numpy directamente para que el modelo sea serializable (joblib).
    model = TransformedTargetRegressor(regressor=base, func=np.log1p, inverse_func=np.expm1)
    model.fit(X_train, y_train)

    preds = model.predict(X_test) if X_test else []
    mae = float(mean_absolute_error(y_test, preds)) if X_test else 0.0
    rmse = float(math.sqrt(mean_squared_error(y_test, preds))) if X_test else 0.0

    out_dir.mkdir(parents=True, exist_ok=True)
    dump(model, out_dir / "model.joblib")

    (out_dir / "feature_names.json").write_text(json.dumps(feature_names, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "metrics.json").write_text(
        json.dumps(
            {"mae": mae, "rmse": rmse, "n_rows": len(X), "n_train": len(X_train), "n_test": len(X_test)},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    return TrainResult(
        model_dir=out_dir,
        n_rows=len(X),
        n_train=len(X_train),
        n_test=len(X_test),
        mae=mae,
        rmse=rmse,
    )


class Command(BaseCommand):
    help = "Entrena un modelo baseline de watch-time futuro (7d) desde el dataset CSV."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dataset",
            type=str,
            default="artifacts/ml/datasets/watch_time_7d.csv",
            help="Ruta al dataset CSV generado por ml_build_dataset.",
        )
        parser.add_argument(
            "--out-dir",
            type=str,
            default=None,
            help="Directorio destino del modelo. Si no se especifica, se versiona por timestamp.",
        )

    def handle(self, *args, **options):
        dataset_path = Path(str(options["dataset"]))
        if not dataset_path.exists():
            raise SystemExit(f"Dataset no existe: {dataset_path}")

        ts = datetime.now(dt_timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_dir = Path(str(options["out_dir"])) if options["out_dir"] else Path(f"artifacts/ml/models/watch_time_7d/{ts}")

        job = TelemetryJobRun.objects.create(
            job_type=TelemetryJobRun.JobType.ML_TRAIN,
            status=TelemetryJobRun.JobStatus.SUCCESS,
            started_at=timezone.now(),
        )
        try:
            feature_names, X, y = _read_dataset_csv(dataset_path)
            result = _fit_and_eval(feature_names, X, y, out_dir)

            finished_at = timezone.now()
            job.finished_at = finished_at
            job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
            job.status = TelemetryJobRun.JobStatus.SUCCESS
            job.save(update_fields=["finished_at", "duration_ms", "status"])

            self.stdout.write(
                self.style.SUCCESS(
                    f"OK ml_train rows={result.n_rows} train={result.n_train} test={result.n_test} "
                    f"mae={round(result.mae, 4)} rmse={round(result.rmse, 4)} out={result.model_dir.as_posix()}"
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

