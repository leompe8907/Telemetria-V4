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


@shared_task(bind=True, name="telemetria.ml_build_dataset")
def ml_build_dataset_task(
    self,
    *,
    as_of: str | None = None,
    lookback_days: int = 7,
    horizon_days: int = 7,
    output: str = "artifacts/ml/datasets/watch_time_7d.csv",
    min_history_days: int = 1,
    lock_ttl_seconds: int = 1800,
):
    from datetime import date, timedelta
    from dataclasses import dataclass
    from pathlib import Path

    from django.db.models import Count, Sum
    from django.utils import timezone

    from delancert.models import MergedTelemetricOTTDelancer, TelemetryJobRun, TelemetryUserDailyAgg

    lock = _acquire_task_lock("ml_build_dataset", ttl_seconds=lock_ttl_seconds)
    if not lock.acquired:
        return {"skipped": True, "reason": "locked"}

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

    job = TelemetryJobRun.objects.create(
        job_type=TelemetryJobRun.JobType.ML_BUILD_DATASET,
        status=TelemetryJobRun.JobStatus.SUCCESS,
        started_at=timezone.now(),
    )
    try:
        as_of_day = _parse_date(as_of) if as_of else timezone.localdate()
        feature_win = _window(as_of_day, int(lookback_days))
        target_win = _future_window(as_of_day, int(horizon_days))

        out_path = Path(str(output))
        out_path.parent.mkdir(parents=True, exist_ok=True)

        f_qs = TelemetryUserDailyAgg.objects.filter(day__gte=feature_win.start, day__lte=feature_win.end)
        min_history_days = max(1, int(min_history_days))
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
                w.writerow(
                    {
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
                )
                n += 1

        finished_at = timezone.now()
        job.finished_at = finished_at
        job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
        job.saved = int(n)
        job.status = TelemetryJobRun.JobStatus.SUCCESS
        job.save(update_fields=["finished_at", "duration_ms", "saved", "status"])

        return {"rows": n, "output": out_path.as_posix(), "as_of": as_of_day.isoformat()}
    except Exception as e:
        finished_at = timezone.now()
        job.finished_at = finished_at
        job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
        job.status = TelemetryJobRun.JobStatus.ERROR
        job.error_message = str(e)[:2000]
        job.save(update_fields=["finished_at", "duration_ms", "status", "error_message"])
        raise
    finally:
        _release_task_lock(lock)


@shared_task(bind=True, name="telemetria.ml_train")
def ml_train_task(
    self,
    *,
    dataset: str = "artifacts/ml/datasets/watch_time_7d.csv",
    out_dir: str | None = None,
    lock_ttl_seconds: int = 3600,
):
    import json
    import math
    from dataclasses import dataclass
    from datetime import datetime, timezone as dt_timezone
    from pathlib import Path
    from typing import List, Tuple

    from django.utils import timezone
    from delancert.models import TelemetryJobRun

    lock = _acquire_task_lock("ml_train", ttl_seconds=lock_ttl_seconds)
    if not lock.acquired:
        return {"skipped": True, "reason": "locked"}

    @dataclass(frozen=True)
    class TrainResult:
        model_dir: Path
        n_rows: int
        n_train: int
        n_test: int
        mae: float
        rmse: float
        feature_names: list[str]

    def _read_dataset_csv(path: Path) -> Tuple[List[str], List[List[float]], List[float]]:
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
        from joblib import dump
        import numpy as np
        from sklearn.compose import TransformedTargetRegressor
        from sklearn.ensemble import HistGradientBoostingRegressor
        from sklearn.metrics import mean_absolute_error, mean_squared_error

        X_train, X_test, y_train, y_test = _train_test_split(X, y, test_size=0.2, seed=42)
        base = HistGradientBoostingRegressor(learning_rate=0.1, max_depth=6, max_iter=200, random_state=42)
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
            feature_names=feature_names,
        )

    dataset_path = Path(str(dataset))
    if not dataset_path.exists():
        raise SystemExit(f"Dataset no existe: {dataset_path}")

    ts = datetime.now(dt_timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = Path(str(out_dir)) if out_dir else Path(f"artifacts/ml/models/watch_time_7d/{ts}")

    job = TelemetryJobRun.objects.create(
        job_type=TelemetryJobRun.JobType.ML_TRAIN,
        status=TelemetryJobRun.JobStatus.SUCCESS,
        started_at=timezone.now(),
    )
    try:
        feature_names, X, y = _read_dataset_csv(dataset_path)
        result = _fit_and_eval(feature_names, X, y, out_path)

        # Registrar modelo entrenado (registry mínimo)
        from delancert.models import TelemetryModelArtifact

        TelemetryModelArtifact.objects.create(
            task="watch_time_7d",
            model_dir=result.model_dir.as_posix(),
            feature_names=result.feature_names,
            metrics={"mae": result.mae, "rmse": result.rmse, "n_rows": result.n_rows, "n_train": result.n_train, "n_test": result.n_test},
            active=True,
        )

        finished_at = timezone.now()
        job.finished_at = finished_at
        job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
        job.status = TelemetryJobRun.JobStatus.SUCCESS
        job.save(update_fields=["finished_at", "duration_ms", "status"])

        return {
            "rows": result.n_rows,
            "train": result.n_train,
            "test": result.n_test,
            "mae": result.mae,
            "rmse": result.rmse,
            "out_dir": result.model_dir.as_posix(),
        }
    except Exception as e:
        finished_at = timezone.now()
        job.finished_at = finished_at
        job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
        job.status = TelemetryJobRun.JobStatus.ERROR
        job.error_message = str(e)[:2000]
        job.save(update_fields=["finished_at", "duration_ms", "status", "error_message"])
        raise
    finally:
        _release_task_lock(lock)


@shared_task(bind=True, name="telemetria.ml_predict")
def ml_predict_task(
    self,
    *,
    as_of: str | None = None,
    lookback_days: int = 7,
    horizon_days: int = 7,
    model_dir: str | None = None,
    lock_ttl_seconds: int = 3600,
):
    """
    Batch scoring: genera predicciones por usuario para un `as_of` dado.
    Fuente de features: `TelemetryUserDailyAgg` (sumas en ventana lookback).
    Persistencia: `TelemetryUserDailyPrediction` (upsert simple por delete+insert).
    """

    import json
    from datetime import date, timedelta
    from pathlib import Path

    from joblib import load
    from django.db import transaction
    from django.db.models import Count, Sum
    from django.utils import timezone

    from delancert.models import TelemetryJobRun, TelemetryUserDailyAgg, TelemetryUserDailyPrediction, TelemetryModelArtifact

    lock = _acquire_task_lock("ml_predict", ttl_seconds=lock_ttl_seconds)
    if not lock.acquired:
        return {"skipped": True, "reason": "locked"}

    def _parse_date(value: str) -> date:
        return date.fromisoformat(value)

    def _window(end_day: date, days: int) -> tuple[date, date]:
        days = max(1, int(days))
        start = end_day - timedelta(days=days - 1)
        return start, end_day

    job = TelemetryJobRun.objects.create(
        job_type=TelemetryJobRun.JobType.ML_PREDICT,
        status=TelemetryJobRun.JobStatus.SUCCESS,
        started_at=timezone.now(),
    )
    try:
        as_of_day = _parse_date(as_of) if as_of else timezone.localdate()
        lookback_days = max(1, int(lookback_days))
        horizon_days = max(1, int(horizon_days))
        start_day, end_day = _window(as_of_day, lookback_days)

        # Resolver modelo (si no se indica) — preferir registry DB
        resolved_model_dir: Path
        if model_dir:
            resolved_model_dir = Path(str(model_dir))
        else:
            latest = (
                TelemetryModelArtifact.objects.filter(task="watch_time_7d", active=True)
                .order_by("-created_at")
                .first()
            )
            if latest:
                resolved_model_dir = Path(latest.model_dir)
            else:
                root = Path("artifacts/ml/models/watch_time_7d")
                if not root.exists():
                    raise SystemExit("No se encontró un modelo. Entrena uno o especifica model_dir.")
                candidates = [p for p in root.iterdir() if p.is_dir()]
                if not candidates:
                    raise SystemExit("No se encontró un modelo. Entrena uno o especifica model_dir.")
                resolved_model_dir = sorted(candidates, key=lambda p: p.name)[-1]

        model_path = resolved_model_dir / "model.joblib"
        feature_names_path = resolved_model_dir / "feature_names.json"
        if not model_path.exists():
            raise SystemExit(f"Modelo no existe: {model_path}")
        if not feature_names_path.exists():
            raise SystemExit(f"feature_names.json no existe: {feature_names_path}")

        feature_names = json.loads(feature_names_path.read_text(encoding="utf-8"))
        model = load(model_path)

        # Features agregadas en ventana lookback
        rows = (
            TelemetryUserDailyAgg.objects.filter(day__gte=start_day, day__lte=end_day)
            .values("subscriber_code")
            .annotate(
                x_views=Sum("views"),
                x_unique_channels_sum=Sum("unique_channels"),
                x_watch_seconds=Sum("total_duration_seconds"),
                x_active_days=Count("day"),
            )
        )

        X = []
        users: list[str] = []
        for r in rows.iterator():
            users.append(r["subscriber_code"])
            # order según feature_names
            feats = {
                "x_views": float(r.get("x_views") or 0),
                "x_unique_channels_sum": float(r.get("x_unique_channels_sum") or 0),
                "x_watch_seconds": float(r.get("x_watch_seconds") or 0),
                "x_active_days": float(r.get("x_active_days") or 0),
            }
            X.append([float(feats.get(fn) or 0) for fn in feature_names])

        preds = model.predict(X) if X else []

        with transaction.atomic():
            TelemetryUserDailyPrediction.objects.filter(day=as_of_day, horizon_days=horizon_days).delete()
            TelemetryUserDailyPrediction.objects.bulk_create(
                [
                    TelemetryUserDailyPrediction(
                        day=as_of_day,
                        subscriber_code=users[i],
                        horizon_days=horizon_days,
                        y_pred_watch_seconds=float(preds[i]),
                        model_dir=resolved_model_dir.as_posix(),
                    )
                    for i in range(len(users))
                ],
                batch_size=1000,
            )

        finished_at = timezone.now()
        job.finished_at = finished_at
        job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
        job.saved = int(len(users))
        job.status = TelemetryJobRun.JobStatus.SUCCESS
        job.save(update_fields=["finished_at", "duration_ms", "saved", "status"])

        return {"as_of": as_of_day.isoformat(), "rows": len(users), "model_dir": resolved_model_dir.as_posix()}
    except Exception as e:
        finished_at = timezone.now()
        job.finished_at = finished_at
        job.duration_ms = int((finished_at - job.started_at).total_seconds() * 1000)
        job.status = TelemetryJobRun.JobStatus.ERROR
        job.error_message = str(e)[:2000]
        job.save(update_fields=["finished_at", "duration_ms", "status", "error_message"])
        raise
    finally:
        _release_task_lock(lock)

