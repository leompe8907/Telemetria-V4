from django.urls import path 
from delancertv.Panaccess.action.action import (
    TelemetrySyncView,
    MergeOTTView,
    TelemetryRunView
)

from delancertv.Panaccess.action.dashboard import (
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
    #Funciones de sincronización y merge OTT
    path('telemetry/fetch/', TelemetrySyncView.as_view(), name='telemetry_sync'),
    path('telemetry/mergeott/', MergeOTTView.as_view(), name='merge_ott'),

    #Funcion de ejecucion de sync y merge
    path('telemetry/run/', TelemetryRunView.as_view(), name='telemetry_run'),

    #Funciones de dashboard
    path('dashboard/overview/', DashboardOverviewView.as_view(), name='dashboard_overview'),
    path('dashboard/top_channels/', DashboardTopChannelsView.as_view(), name='dashboard_top_channels'),
    path('dashboard/channel_audience/', DashboardChannelAudienceView.as_view(), name='dashboard_channel_audience'),
    path('dashboard/peak_hours_by_channel/', DashboardPeakHoursByChannelView.as_view(), name='dashboard_peak_hours_by_channel'),
    path('dashboard/temporal/', DashboardTemporalView.as_view(), name='dashboard_temporal'),
    path("dashboard/user_profile/<str:subscriber_code>/", DashboardUserProfileView.as_view(), name="dashboard_user_profile"),
    path("dashboard/user_range/<str:subscriber_code>/", DashboardUserRangeView.as_view(), name="dashboard_user_range"),
    path('dashboard/users_general/', DashboardUsersGeneralView.as_view(), name='dashboard_users_general'),
]