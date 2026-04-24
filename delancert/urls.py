from django.urls import path
from delancert.server.action import TelemetrySyncView

urlpatterns = [
    path('telemetry/sync/', TelemetrySyncView.as_view(), name='telemetry-sync'),
]