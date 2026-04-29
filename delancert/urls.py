from django.urls import path
from delancert.server.action import TelemetrySyncView, MergeOTTView, TelemetryRunView
from delancert.server.aggregates import TelemetryBuildAggregatesView
from delancert.server.health import TelemetryHealthView
from delancert.server.jobs import TelemetryJobRunsView
from delancert.server.ops import TelemetryOpsAlertsView, TelemetryOpsSummaryView
from delancert.server.noc import NocRecommendationsView
from delancert.server.analyst import OpsAnalystReportView
from delancert.server.pipeline import PipelineRunView
from delancert.server.tasks_api import (
    TelemetryRunEnqueueView,
    TelemetryBuildAggregatesEnqueueView,
    MlBuildDatasetEnqueueView,
    MlTrainEnqueueView,
    MlPredictEnqueueView,
    CeleryTaskStatusView,
)
from delancert.server.ml_predictions import UserPredictionsView, DailyPredictionsSummaryView
from delancert.server.ml_models import LatestModelView, ModelListView
from delancert.server.ml_model_admin import ActivateModelView, RollbackModelView
from delancert.server.reports import (
    AgentReportsListView,
    AgentReportDetailView,
    NocRunAndPersistView,
    AnalystRunAndPersistView,
)
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
    path("telemetry/build-aggregates/", TelemetryBuildAggregatesView.as_view(), name="telemetry-build-aggregates"),
    # Async jobs (Celery)
    path("tasks/telemetry/run/", TelemetryRunEnqueueView.as_view(), name="tasks-telemetry-run"),
    path("tasks/telemetry/build-aggregates/", TelemetryBuildAggregatesEnqueueView.as_view(), name="tasks-build-aggregates"),
    path("tasks/ml/build-dataset/", MlBuildDatasetEnqueueView.as_view(), name="tasks-ml-build-dataset"),
    path("tasks/ml/train/", MlTrainEnqueueView.as_view(), name="tasks-ml-train"),
    path("tasks/ml/predict/", MlPredictEnqueueView.as_view(), name="tasks-ml-predict"),
    path("tasks/status/<str:task_id>/", CeleryTaskStatusView.as_view(), name="tasks-status"),
    path("health/", TelemetryHealthView.as_view(), name="telemetry-health"),
    path("jobs/runs/", TelemetryJobRunsView.as_view(), name="telemetry-job-runs"),
    path("ops/alerts/", TelemetryOpsAlertsView.as_view(), name="telemetry-ops-alerts"),
    path("ops/summary/", TelemetryOpsSummaryView.as_view(), name="telemetry-ops-summary"),
    path("ops/noc/recommendations/", NocRecommendationsView.as_view(), name="telemetry-ops-noc-recommendations"),
    path("ops/analyst/report/", OpsAnalystReportView.as_view(), name="telemetry-ops-analyst-report"),
    path("ops/pipeline/run/", PipelineRunView.as_view(), name="telemetry-ops-pipeline-run"),
    # Agent reports (persisted)
    path("ops/reports/", AgentReportsListView.as_view(), name="telemetry-ops-reports"),
    path("ops/reports/<int:report_id>/", AgentReportDetailView.as_view(), name="telemetry-ops-report-detail"),
    path("ops/noc/run/", NocRunAndPersistView.as_view(), name="telemetry-ops-noc-run"),
    path("ops/analyst/run/", AnalystRunAndPersistView.as_view(), name="telemetry-ops-analyst-run"),
    # ML predictions (read-only)
    path("ml/predictions/users/<str:subscriber_code>/", UserPredictionsView.as_view(), name="ml-user-predictions"),
    path("ml/predictions/daily/", DailyPredictionsSummaryView.as_view(), name="ml-daily-predictions"),
    path("ml/models/latest/", LatestModelView.as_view(), name="ml-latest-model"),
    path("ml/models/", ModelListView.as_view(), name="ml-models-list"),
    path("ml/models/activate/", ActivateModelView.as_view(), name="ml-model-activate"),
    path("ml/models/rollback/", RollbackModelView.as_view(), name="ml-model-rollback"),
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