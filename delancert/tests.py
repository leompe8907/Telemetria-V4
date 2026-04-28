from __future__ import annotations

import os
from unittest.mock import patch

from django.test import override_settings
from django.urls import reverse
from django.core.management import call_command
from django.core.cache import cache
from rest_framework.test import APITestCase

from delancert.analytics.common import parse_date_range
from delancert.analytics.common import DateRange
from delancert.models import TelemetryJobRun
from delancert.models import TelemetryChannelDailyAgg, TelemetryUserDailyAgg, MergedTelemetricOTTDelancer
from delancert.models import TelemetryUserDailyPrediction
from delancert.utils.rate_limit import acquire_rate_limit
from delancert.exceptions import PanAccessAPIError


@override_settings(
    # Tests no deben depender de Postgres/PanAccess.
    DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
    ALLOWED_HOSTS=["testserver", "127.0.0.1", "localhost"],
)
class TelemetriaAuthAndEndpointsTests(APITestCase):
    def setUp(self):
        super().setUp()
        try:
            cache.clear()
        except Exception:
            pass
        os.environ["TELEMETRIA_API_KEY_RW"] = "rw-key"
        os.environ["TELEMETRIA_API_KEY_RO"] = "ro-key"
        os.environ.pop("TELEMETRIA_API_KEY", None)

    def tearDown(self):
        os.environ.pop("TELEMETRIA_API_KEY_RW", None)
        os.environ.pop("TELEMETRIA_API_KEY_RO", None)
        os.environ.pop("TELEMETRIA_API_KEY", None)
        super().tearDown()

    def test_health_requires_api_key_returns_401(self):
        url = reverse("telemetry-health")
        r = self.client.get(url)
        self.assertEqual(r.status_code, 401)
        self.assertIn("Missing API key", r.json().get("detail", ""))

    def test_health_with_invalid_key_returns_401(self):
        url = reverse("telemetry-health")
        self.client.credentials(HTTP_X_TELEMETRIA_KEY="bad")
        r = self.client.get(url)
        self.assertEqual(r.status_code, 401)
        self.assertIn("Invalid API key", r.json().get("detail", ""))

    def test_read_endpoint_accepts_ro_key(self):
        url = reverse("telemetry-health")
        self.client.credentials(HTTP_X_TELEMETRIA_KEY="ro-key")
        r = self.client.get(url)
        # si falla por BD vacía u otro motivo, igual debe pasar auth/permiso (no 401/403)
        self.assertNotIn(r.status_code, (401, 403))

    def test_write_endpoint_rejects_ro_key_with_403(self):
        url = reverse("telemetry-run")
        self.client.credentials(HTTP_X_TELEMETRIA_KEY="ro-key")
        r = self.client.post(url, data={"limit": 1}, format="json")
        self.assertEqual(r.status_code, 403)

    @patch("delancert.tasks.telemetry_run_task")
    def test_enqueue_telemetry_run_requires_celery_enabled(self, mock_task):
        # Sin broker configurado, debe devolver 503 y NO encolar
        with override_settings(CELERY_BROKER_URL=None):
            url = reverse("tasks-telemetry-run")
            self.client.credentials(HTTP_X_TELEMETRIA_KEY="rw-key")
            r = self.client.post(url, data={"limit": 1}, format="json")
            self.assertEqual(r.status_code, 503)
            self.assertFalse(mock_task.delay.called)

    @patch("delancert.tasks.telemetry_run_task")
    def test_enqueue_telemetry_run_accepts_and_returns_task_id(self, mock_task):
        mock_task.delay.return_value.id = "task-123"
        with override_settings(CELERY_BROKER_URL="redis://localhost:6379/0"):
            url = reverse("tasks-telemetry-run")
            self.client.credentials(HTTP_X_TELEMETRIA_KEY="rw-key")
            r = self.client.post(url, data={"limit": 1}, format="json")
            self.assertEqual(r.status_code, 202)
            self.assertTrue(r.json().get("accepted"))
            self.assertEqual(r.json().get("task_id"), "task-123")

    @patch("delancert.tasks.ml_build_dataset_task")
    def test_enqueue_ml_build_dataset_accepts(self, mock_task):
        mock_task.delay.return_value.id = "task-ml-1"
        with override_settings(CELERY_BROKER_URL="redis://localhost:6379/0"):
            url = reverse("tasks-ml-build-dataset")
            self.client.credentials(HTTP_X_TELEMETRIA_KEY="rw-key")
            r = self.client.post(url, data={"lookback_days": 7, "horizon_days": 7}, format="json")
            self.assertEqual(r.status_code, 202)
            self.assertEqual(r.json().get("task_id"), "task-ml-1")

    @patch("delancert.tasks.ml_train_task")
    def test_enqueue_ml_train_accepts(self, mock_task):
        mock_task.delay.return_value.id = "task-ml-2"
        with override_settings(CELERY_BROKER_URL="redis://localhost:6379/0"):
            url = reverse("tasks-ml-train")
            self.client.credentials(HTTP_X_TELEMETRIA_KEY="rw-key")
            r = self.client.post(url, data={"dataset": "artifacts/ml/datasets/watch_time_7d.csv"}, format="json")
            self.assertEqual(r.status_code, 202)
            self.assertEqual(r.json().get("task_id"), "task-ml-2")

    @patch("delancert.tasks.ml_predict_task")
    def test_enqueue_ml_predict_accepts(self, mock_task):
        mock_task.delay.return_value.id = "task-ml-3"
        with override_settings(CELERY_BROKER_URL="redis://localhost:6379/0"):
            url = reverse("tasks-ml-predict")
            self.client.credentials(HTTP_X_TELEMETRIA_KEY="rw-key")
            r = self.client.post(url, data={"as_of": "2026-01-01"}, format="json")
            self.assertEqual(r.status_code, 202)
            self.assertEqual(r.json().get("task_id"), "task-ml-3")

    def test_ops_alerts_requires_api_key(self):
        url = reverse("telemetry-ops-alerts")
        r = self.client.get(url)
        self.assertEqual(r.status_code, 401)

    def test_ops_summary_requires_api_key(self):
        url = reverse("telemetry-ops-summary")
        r = self.client.get(url)
        self.assertEqual(r.status_code, 401)

    @patch("delancert.server.action.get_highest_record_id")
    @patch("delancert.server.action.is_database_empty")
    @patch("delancert.server.action.save_telemetry_records")
    @patch("delancert.server.action.fetch_telemetry_records_smart")
    def test_sync_endpoint_creates_sync_job_run(
        self,
        mock_fetch_smart,
        mock_save_records,
        mock_is_database_empty,
        mock_get_highest_record_id,
    ):
        mock_is_database_empty.return_value = False
        mock_get_highest_record_id.side_effect = [10, 12]
        mock_fetch_smart.return_value = [{"recordId": 11}]
        mock_save_records.return_value = {"saved_records": 1, "skipped_records": 0, "errors": 0}

        self.client.credentials(HTTP_X_TELEMETRIA_KEY="rw-key")
        url = reverse("telemetry-sync")
        r = self.client.post(url, data={"limit": 1}, format="json")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json().get("success"))
        self.assertEqual(
            TelemetryJobRun.objects.filter(job_type=TelemetryJobRun.JobType.SYNC).count(),
            1,
        )

    @patch("delancert.server.action.merge_ott_records")
    def test_merge_endpoint_creates_merge_job_run(self, mock_merge):
        mock_merge.return_value = {"saved_records": 2, "deleted_existing": 0}

        self.client.credentials(HTTP_X_TELEMETRIA_KEY="rw-key")
        url = reverse("telemetry-merge-ott")
        r = self.client.post(url, data={"batch_size": 10, "backfill_last_n": 0}, format="json")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json().get("success"))
        self.assertEqual(
            TelemetryJobRun.objects.filter(job_type=TelemetryJobRun.JobType.MERGE_OTT).count(),
            1,
        )

    @patch("delancert.server.action.merge_ott_records")
    @patch("delancert.server.action.save_telemetry_records")
    @patch("delancert.server.action.fetch_telemetry_records_smart")
    @patch("delancert.server.action.is_database_empty")
    @patch("delancert.server.action.get_highest_record_id")
    def test_telemetry_run_creates_job_run_with_mocks(
        self,
        mock_get_highest_record_id,
        mock_is_database_empty,
        mock_fetch_smart,
        mock_save_records,
        mock_merge,
    ):
        # Arrange
        mock_is_database_empty.return_value = False
        mock_get_highest_record_id.side_effect = [100, 120]  # before / after
        mock_fetch_smart.return_value = [{"recordId": 101}]
        mock_save_records.return_value = {"saved_records": 1, "skipped_records": 0, "errors": 0}
        mock_merge.return_value = {
            "total_processed": 1,
            "merged_records": 1,
            "saved_records": 1,
            "skipped_records": 0,
            "errors": 0,
            "start_record_id": 100,
            "max_record_id": 100,
            "backfill_last_n": 0,
            "deleted_existing": 0,
        }

        self.client.credentials(HTTP_X_TELEMETRIA_KEY="rw-key")
        url = reverse("telemetry-run")

        # Act
        r = self.client.post(url, data={"limit": 1, "backfill_last_n": 0}, format="json")

        # Assert
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json().get("success"))
        self.assertEqual(TelemetryJobRun.objects.count(), 1)
        job = TelemetryJobRun.objects.first()
        self.assertIsNotNone(job.started_at)
        self.assertIsNotNone(job.finished_at)
        self.assertEqual(job.status, TelemetryJobRun.JobStatus.SUCCESS)

    @patch("delancert.tasks.telemetry_run_task")
    def test_telemetry_run_async_returns_202_and_task_id(self, mock_task):
        mock_task.delay.return_value.id = "task-telemetry-1"
        with override_settings(CELERY_BROKER_URL="redis://localhost:6379/0"):
            self.client.credentials(HTTP_X_TELEMETRIA_KEY="rw-key")
            url = reverse("telemetry-run")
            r = self.client.post(url, data={"async": True, "limit": 1}, format="json")
            self.assertEqual(r.status_code, 202)
            self.assertTrue(r.json().get("accepted"))
            self.assertEqual(r.json().get("task_id"), "task-telemetry-1")
            # El job run se crea dentro de la task, no en el endpoint async.
            self.assertEqual(TelemetryJobRun.objects.count(), 0)

    def test_telemetry_run_async_returns_503_when_celery_disabled(self):
        with override_settings(CELERY_BROKER_URL=None):
            self.client.credentials(HTTP_X_TELEMETRIA_KEY="rw-key")
            url = reverse("telemetry-run")
            r = self.client.post(url, data={"async": True, "limit": 1}, format="json")
            self.assertEqual(r.status_code, 503)

    @patch("delancert.tasks.telemetry_build_aggregates_task")
    def test_build_aggregates_async_returns_202_and_task_id(self, mock_task):
        mock_task.delay.return_value.id = "task-agg-1"
        with override_settings(CELERY_BROKER_URL="redis://localhost:6379/0"):
            self.client.credentials(HTTP_X_TELEMETRIA_KEY="rw-key")
            url = reverse("telemetry-build-aggregates")
            r = self.client.post(url, data={"async": True, "days": 7}, format="json")
            self.assertEqual(r.status_code, 202)
            self.assertTrue(r.json().get("accepted"))
            self.assertEqual(r.json().get("task_id"), "task-agg-1")

    def test_build_aggregates_async_returns_503_when_celery_disabled(self):
        with override_settings(CELERY_BROKER_URL=None):
            self.client.credentials(HTTP_X_TELEMETRIA_KEY="rw-key")
            url = reverse("telemetry-build-aggregates")
            r = self.client.post(url, data={"async": True, "days": 7}, format="json")
            self.assertEqual(r.status_code, 503)


class TelemetriaUtilsTests(APITestCase):
    @override_settings(CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}})
    def test_rate_limit_blocks_second_call(self):
        r1 = acquire_rate_limit("x", ttl_seconds=3)
        r2 = acquire_rate_limit("x", ttl_seconds=3)
        self.assertTrue(r1.allowed)
        self.assertFalse(r2.allowed)
        self.assertGreaterEqual(r2.retry_after_seconds, 1)

    def test_parse_date_range_requires_iso_format(self):
        with self.assertRaises(ValueError):
            parse_date_range("2025/12/01", "2026/01/30")

    def test_parse_date_range_requires_both(self):
        with self.assertRaises(ValueError):
            parse_date_range("2025-12-01", None)

    @patch("delancert.server.telemetry_fetcher.get_panaccess")
    def test_panaccess_permission_error_triggers_one_session_refresh(self, mock_get_panaccess):
        """
        Si PanAccess responde 'no_access_to_function' para todas las funciones candidatas,
        hacemos un refresh de sesión (1 vez) y reintentamos.
        """
        mock_pan = mock_get_panaccess.return_value
        calls = {"n": 0}

        def _call(*args, **kwargs):
            calls["n"] += 1
            # Primer pass: 3 candidatos -> todos permission error
            # Segundo pass: 1er candidato ya funciona
            if calls["n"] <= 3:
                raise PanAccessAPIError("no permission", error_code="no_access_to_function")
            return {"success": True, "answer": {"telemetryRecordEntries": []}}

        mock_pan.call.side_effect = _call
        mock_pan.reset_session.return_value = None

        from delancert.server.telemetry_fetcher import get_telemetry_records

        r = get_telemetry_records(offset=0, limit=10)
        self.assertTrue(r.get("success"))
        mock_pan.reset_session.assert_called_once()

    @patch("delancert.server.ops.TelemetryJobRun")
    def test_ops_alerts_ok_payload_shape(self, mock_jobrun):
        # Solo valida que el endpoint devuelve un payload con claves esperadas (sin depender de BD real).
        from delancert.server.ops import TelemetryOpsAlertsView
        from rest_framework.test import APIRequestFactory

        mock_jobrun.objects.filter.return_value.order_by.return_value.__getitem__.return_value = []

        f = APIRequestFactory()
        req = f.get("/delancert/ops/alerts/")
        view = TelemetryOpsAlertsView.as_view(authentication_classes=[], permission_classes=[])
        resp = view(req)
        self.assertEqual(resp.status_code, 200)
        self.assertIn("alerts", resp.data)
        self.assertIn("signals", resp.data)

    def test_telemetry_ops_check_exit_codes(self):
        # OK -> exit 0
        with self.assertRaises(SystemExit) as e:
            call_command("telemetry_ops_check")
        self.assertIn(int(e.exception.code), (0, 1, 2))

    def test_telemetry_integrity_check_creates_job_run(self):
        before = TelemetryJobRun.objects.filter(job_type=TelemetryJobRun.JobType.INTEGRITY_CHECK).count()
        call_command("telemetry_integrity_check", hours=1)
        after = TelemetryJobRun.objects.filter(job_type=TelemetryJobRun.JobType.INTEGRITY_CHECK).count()
        self.assertEqual(after, before + 1)

    def test_build_aggregates_creates_rows(self):
        # Crear datos mínimos en merged_ott
        from django.utils import timezone as dj_tz

        d = dj_tz.localdate()
        MergedTelemetricOTTDelancer.objects.create(
            recordId=1,
            actionId=8,
            subscriberCode="u1",
            dataName="ch1",
            dataDuration=60,
            dataDate=d,
            timestamp=dj_tz.now(),
        )
        MergedTelemetricOTTDelancer.objects.create(
            recordId=2,
            actionId=8,
            subscriberCode="u1",
            dataName="ch1",
            dataDuration=120,
            dataDate=d,
            timestamp=dj_tz.now(),
        )
        MergedTelemetricOTTDelancer.objects.create(
            recordId=3,
            actionId=8,
            subscriberCode="u2",
            dataName="ch2",
            dataDuration=30,
            dataDate=d,
            timestamp=dj_tz.now(),
        )

        call_command("telemetry_build_aggregates", days=1)

        self.assertEqual(TelemetryChannelDailyAgg.objects.count(), 2)
        self.assertEqual(TelemetryUserDailyAgg.objects.count(), 2)

        ch1 = TelemetryChannelDailyAgg.objects.get(day=d, channel="ch1")
        self.assertEqual(ch1.views, 2)
        self.assertEqual(ch1.unique_users, 1)
        self.assertEqual(ch1.total_duration_seconds, 180)

    def test_ml_predictions_read_endpoint(self):
        from django.utils import timezone as dj_tz

        d = dj_tz.localdate()
        TelemetryUserDailyPrediction.objects.create(
            day=d,
            subscriber_code="u1",
            horizon_days=7,
            y_pred_watch_seconds=123.0,
            model_dir="artifacts/ml/models/watch_time_7d/x",
        )

        url = reverse("ml-user-predictions", kwargs={"subscriber_code": "u1"})
        # sin key => 401
        r0 = self.client.get(url, data={"start": d.isoformat(), "end": d.isoformat()})
        self.assertEqual(r0.status_code, 401)

        # con RO key => 200
        os.environ["TELEMETRIA_API_KEY_RO"] = "ro-key"
        self.client.credentials(HTTP_X_TELEMETRIA_KEY="ro-key")
        r = self.client.get(url, data={"start": d.isoformat(), "end": d.isoformat(), "horizon_days": 7})
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json().get("success"))
        self.assertEqual(len(r.json().get("predictions") or []), 1)

    def test_top_channels_uses_daily_aggs(self):
        from django.utils import timezone as dj_tz
        from delancert.analytics.channels import top_channels

        d = dj_tz.localdate()
        TelemetryChannelDailyAgg.objects.create(day=d, channel="ch1", views=10, unique_users=5, total_duration_seconds=600)
        TelemetryChannelDailyAgg.objects.create(day=d, channel="ch2", views=5, unique_users=3, total_duration_seconds=300)

        out = top_channels(DateRange(start=d, end=d), limit=10)
        self.assertEqual(out[0]["channel"], "ch1")
        self.assertEqual(out[0]["total_views"], 10)

    def test_temporal_daily_uses_daily_aggs(self):
        from django.utils import timezone as dj_tz
        from delancert.analytics.temporal import temporal

        d = dj_tz.localdate()
        TelemetryChannelDailyAgg.objects.create(day=d, channel="ch1", views=2, unique_users=1, total_duration_seconds=3600)
        out = temporal(DateRange(start=d, end=d), period="daily")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["views"], 2)
        self.assertEqual(out[0]["watch_hours"], 1.0)

    def test_overview_uses_daily_aggs_for_totals(self):
        from django.utils import timezone as dj_tz
        from delancert.analytics.overview import overview
        from delancert.models import TelemetryChannelDailyAgg

        d = dj_tz.localdate()
        TelemetryChannelDailyAgg.objects.create(day=d, channel="ch1", views=10, unique_users=2, total_duration_seconds=7200)

        out = overview(DateRange(start=d, end=d))
        self.assertEqual(out["kpis"]["total_views"], 10)
        self.assertEqual(out["kpis"]["total_watch_hours"], 2.0)

    def test_ml_build_dataset_watch_time_7d(self):
        from django.utils import timezone as dj_tz
        from django.core.management import call_command
        from pathlib import Path
        from tempfile import TemporaryDirectory
        import csv

        d0 = dj_tz.localdate()
        # features lookback (d0-6 .. d0)
        TelemetryUserDailyAgg.objects.create(day=d0, subscriber_code="u1", views=3, unique_channels=2, total_duration_seconds=300)
        TelemetryUserDailyAgg.objects.create(day=d0, subscriber_code="u2", views=1, unique_channels=1, total_duration_seconds=60)

        # target horizon (d0+1 .. d0+7)
        from datetime import timedelta

        MergedTelemetricOTTDelancer.objects.create(
            recordId=1001,
            actionId=8,
            subscriberCode="u1",
            dataName="ch1",
            dataDuration=120,
            dataDate=d0 + timedelta(days=1),
            timestamp=dj_tz.now(),
        )
        MergedTelemetricOTTDelancer.objects.create(
            recordId=1002,
            actionId=8,
            subscriberCode="u1",
            dataName="ch2",
            dataDuration=180,
            dataDate=d0 + timedelta(days=2),
            timestamp=dj_tz.now(),
        )

        with TemporaryDirectory() as td:
            out = Path(td) / "test_watch_time.csv"
            call_command(
                "ml_build_dataset",
                **{
                    "as_of": d0.isoformat(),
                    "lookback_days": 7,
                    "horizon_days": 7,
                    "output": str(out),
                },
            )
            self.assertTrue(out.exists())
            rows = list(csv.DictReader(out.open("r", encoding="utf-8")))
        # u1 y u2 están en features
        by_u = {r["subscriber_code"]: r for r in rows}
        self.assertIn("u1", by_u)
        self.assertIn("u2", by_u)
        self.assertEqual(int(by_u["u1"]["y_watch_seconds_next_horizon"]), 300)
        self.assertEqual(int(by_u["u2"]["y_watch_seconds_next_horizon"]), 0)

    def test_ml_train_creates_model_artifacts(self):
        from pathlib import Path
        import csv
        from django.core.management import call_command
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as td:
            root = Path(td)
            ds = root / "test_train.csv"
            with ds.open("w", newline="", encoding="utf-8") as f:
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
                w.writerow(
                    {
                        "as_of": "2026-01-01",
                        "subscriber_code": "u1",
                        "feature_start": "2025-12-26",
                        "feature_end": "2026-01-01",
                        "target_start": "2026-01-02",
                        "target_end": "2026-01-08",
                        "x_views": 10,
                        "x_unique_channels_sum": 3,
                        "x_watch_seconds": 1000,
                        "x_active_days": 3,
                        "y_watch_seconds_next_horizon": 1200,
                    }
                )
                w.writerow(
                    {
                        "as_of": "2026-01-01",
                        "subscriber_code": "u2",
                        "feature_start": "2025-12-26",
                        "feature_end": "2026-01-01",
                        "target_start": "2026-01-02",
                        "target_end": "2026-01-08",
                        "x_views": 2,
                        "x_unique_channels_sum": 1,
                        "x_watch_seconds": 200,
                        "x_active_days": 1,
                        "y_watch_seconds_next_horizon": 50,
                    }
                )

            out_dir = root / "test_model"
            call_command("ml_train", dataset=str(ds), out_dir=str(out_dir))
            self.assertTrue((out_dir / "model.joblib").exists())
            self.assertTrue((out_dir / "metrics.json").exists())
            self.assertTrue((out_dir / "feature_names.json").exists())
