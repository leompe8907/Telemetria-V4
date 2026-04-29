from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
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


def _read_dataset_csv(path: Path) -> Tuple[List[str], List[List[float]], List[float], List[date]]:
    """
    Lee el CSV producido por ml_build_dataset.
    Retorna (feature_names, X, y, as_of_days).
    """
    import csv

    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        rows = list(r)

    feature_names = ["x_views", "x_unique_channels_sum", "x_watch_seconds", "x_active_days"]
    X: List[List[float]] = []
    y: List[float] = []
    as_of_days: List[date] = []
    for row in rows:
        X.append([float(row.get(c) or 0) for c in feature_names])
        y.append(float(row.get("y_watch_seconds_next_horizon") or 0))
        as_of_days.append(date.fromisoformat(str(row.get("as_of") or "")))
    return feature_names, X, y, as_of_days


def _train_test_split_random(X: List[List[float]], y: List[float], test_size: float = 0.2, seed: int = 42):
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


def _train_test_split_temporal(
    X: List[List[float]],
    y: List[float],
    as_of_days: List[date],
    test_size: float = 0.2,
):
    """
    Split temporal real:
    - Ordena por as_of ascendente.
    - Usa el tail (últimas fechas) como test.
    Si el dataset no tiene variedad temporal, cae a random.
    """
    n = len(X)
    if n <= 1:
        return X, [], y, []

    distinct_days = sorted(set(as_of_days))
    if len(distinct_days) <= 1:
        return _train_test_split_random(X, y, test_size=test_size, seed=42)

    # Determinar cuántos samples al test (mínimo 1)
    n_test = max(1, int(round(n * float(test_size))))

    # Orden estable por (as_of, idx) para reproducibilidad.
    order = sorted(range(n), key=lambda i: (as_of_days[i], i))
    test_idx = set(order[-n_test:])

    X_train, y_train, X_test, y_test = [], [], [], []
    for i in range(n):
        if i in test_idx:
            X_test.append(X[i])
            y_test.append(y[i])
        else:
            X_train.append(X[i])
            y_train.append(y[i])
    return X_train, X_test, y_train, y_test


def _feature_drift_summary(feature_names: List[str], X_train: List[List[float]], X_test: List[List[float]]) -> dict:
    """
    Drift mínimo train vs test: delta% de medias por feature.
    Útil como señal de sanity-check del split temporal.
    """
    if not X_train or not X_test:
        return {"available": False}

    def _mean(col: int, Xv: List[List[float]]) -> float:
        return float(sum(float(r[col]) for r in Xv) / max(1, len(Xv)))

    drift = {}
    for j, name in enumerate(feature_names):
        m_tr = _mean(j, X_train)
        m_te = _mean(j, X_test)
        if m_tr == 0.0:
            pct = None if m_te == 0.0 else 999.0
        else:
            pct = (m_te - m_tr) / abs(m_tr) * 100.0
        drift[name] = {"train_mean": m_tr, "test_mean": m_te, "pct_change": pct}
    return {"available": True, "train_vs_test_mean_pct": drift}


def _fit_and_eval(
    feature_names: List[str],
    X: List[List[float]],
    y: List[float],
    as_of_days: List[date],
    out_dir: Path,
    *,
    split: str,
    test_size: float,
) -> TrainResult:
    import math

    from joblib import dump
    from sklearn.compose import TransformedTargetRegressor
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    import numpy as np

    if split == "random":
        X_train, X_test, y_train, y_test = _train_test_split_random(X, y, test_size=test_size, seed=42)
    else:
        X_train, X_test, y_train, y_test = _train_test_split_temporal(X, y, as_of_days, test_size=test_size)

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
    drift = _feature_drift_summary(feature_names, X_train, X_test)
    (out_dir / "metrics.json").write_text(
        json.dumps(
            {
                "mae": mae,
                "rmse": rmse,
                "n_rows": len(X),
                "n_train": len(X_train),
                "n_test": len(X_test),
                "split": split,
                "test_size": float(test_size),
                "feature_drift_train_vs_test": drift,
            },
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
        parser.add_argument(
            "--split",
            type=str,
            default="temporal",
            choices=["temporal", "random"],
            help="Estrategia de split. 'temporal' usa as_of (recomendado).",
        )
        parser.add_argument(
            "--test-size",
            type=float,
            default=0.2,
            help="Proporción de test (0..0.9). En temporal usa tail del dataset ordenado por as_of.",
        )

    def handle(self, *args, **options):
        dataset_path = Path(str(options["dataset"]))
        if not dataset_path.exists():
            raise SystemExit(f"Dataset no existe: {dataset_path}")

        ts = datetime.now(dt_timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_dir = Path(str(options["out_dir"])) if options["out_dir"] else Path(f"artifacts/ml/models/watch_time_7d/{ts}")
        split = str(options["split"] or "temporal").strip().lower()
        test_size = float(options["test_size"] or 0.2)
        if test_size <= 0 or test_size >= 0.9:
            raise SystemExit("--test-size debe estar en (0, 0.9)")

        job = TelemetryJobRun.objects.create(
            job_type=TelemetryJobRun.JobType.ML_TRAIN,
            status=TelemetryJobRun.JobStatus.SUCCESS,
            started_at=timezone.now(),
        )
        try:
            feature_names, X, y, as_of_days = _read_dataset_csv(dataset_path)
            result = _fit_and_eval(feature_names, X, y, as_of_days, out_dir, split=split, test_size=test_size)

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

