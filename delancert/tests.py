from __future__ import annotations

import os
from unittest.mock import patch

from django.test import override_settings
from django.urls import reverse
from rest_framework.test import APITestCase

from delancert.analytics.common import parse_date_range
from delancert.models import TelemetryJobRun
from delancert.utils.rate_limit import acquire_rate_limit


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
