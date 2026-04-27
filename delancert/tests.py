from __future__ import annotations

import os
from unittest.mock import patch

from django.test import override_settings
from django.urls import reverse
from django.core.management import call_command
from rest_framework.test import APITestCase

from delancert.analytics.common import parse_date_range
from delancert.models import TelemetryJobRun
from delancert.models import TelemetryChannelDailyAgg, TelemetryUserDailyAgg, MergedTelemetricOTTDelancer
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
