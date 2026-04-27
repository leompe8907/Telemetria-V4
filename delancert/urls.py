from django.urls import path
from delancert.server.action import TelemetrySyncView, MergeOTTView, TelemetryRunView
from delancert.server.health import TelemetryHealthView
from delancert.server.jobs import TelemetryJobRunsView
from delancert.server.ops import TelemetryOpsAlertsView, TelemetryOpsSummaryView
from delancert.server.dashboard import (
    DashboardOverviewView,
    DashboardTopChannelsView,
    DashboardChannelAudienceView,
    DashboardPeakHoursByChannelView,
    DashboardTemporalView,
    DashboardUserProfileView,
    DashboardUserRangeView,
    DashboardUsersGeneralView,
)

urlpatterns = [
    path('telemetry/sync/', TelemetrySyncView.as_view(), name='telemetry-sync'),
    path('telemetry/merge/ott/', MergeOTTView.as_view(), name='telemetry-merge-ott'),
    path('telemetry/run/', TelemetryRunView.as_view(), name='telemetry-run'),
    path("health/", TelemetryHealthView.as_view(), name="telemetry-health"),
    path("jobs/runs/", TelemetryJobRunsView.as_view(), name="telemetry-job-runs"),
    path("ops/alerts/", TelemetryOpsAlertsView.as_view(), name="telemetry-ops-alerts"),
    path("ops/summary/", TelemetryOpsSummaryView.as_view(), name="telemetry-ops-summary"),
    # Dashboard API
    path("dashboard/overview/", DashboardOverviewView.as_view(), name="dashboard-overview"),
    path("dashboard/channels/top/", DashboardTopChannelsView.as_view(), name="dashboard-top-channels"),
    path("dashboard/channels/audience/", DashboardChannelAudienceView.as_view(), name="dashboard-channel-audience"),
    path("dashboard/channels/peak-hours/", DashboardPeakHoursByChannelView.as_view(), name="dashboard-peak-hours"),
    path("dashboard/temporal/", DashboardTemporalView.as_view(), name="dashboard-temporal"),
    path("dashboard/users/<str:subscriber_code>/", DashboardUserProfileView.as_view(), name="dashboard-user-profile"),
    path("dashboard/users/<str:subscriber_code>/range/", DashboardUserRangeView.as_view(), name="dashboard-user-range"),
    path("dashboard/users/general/", DashboardUsersGeneralView.as_view(), name="dashboard-users-general"),
]