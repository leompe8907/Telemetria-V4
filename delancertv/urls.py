from django.urls import path 
from delancertv.Panaccess.action.action import TelemetrySyncView, MergeOTTView, TelemetryRunView

urlpatterns = [
    #Funciones de sincronización y merge OTT
    path('telemetry/fetch/', TelemetrySyncView.as_view(), name='telemetry_sync'),
    path('telemetry/mergeott/', MergeOTTView.as_view(), name='merge_ott'),
    #Funcion de ejecucion de sync y merge
    path('telemetry/run/', TelemetryRunView.as_view(), name='telemetry_run'),
]