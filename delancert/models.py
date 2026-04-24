from django.db import models

class TelemetryBase(models.Model):
    """Modelo base abstracto con todos los campos comunes"""
    actionId = models.IntegerField(null=True, blank=True)
    actionKey = models.CharField(max_length=20, null=True, blank=True)
    anonymized = models.BooleanField(null=True, blank=True)
    data = models.CharField(max_length=200, null=True, blank=True)
    dataDuration = models.IntegerField(null=True, blank=True)
    dataId = models.IntegerField(null=True, blank=True)
    dataName = models.CharField(max_length=200, blank=True, null=True)
    dataNetId = models.IntegerField(null=True, blank=True)
    dataPrice = models.IntegerField(null=True, blank=True)
    dataSeviceId = models.IntegerField(null=True, blank=True)
    dataTsId = models.IntegerField(null=True, blank=True)
    date = models.IntegerField(null=True, blank=True)
    deviceId = models.IntegerField(null=True, blank=True)
    ip = models.GenericIPAddressField(null=True, blank=True)
    ipId = models.IntegerField(null=True, blank=True)
    manual = models.BooleanField(null=True, blank=True)
    profileId = models.IntegerField(null=True, blank=True)
    reaonId = models.IntegerField(null=True, blank=True)
    reasonKey = models.CharField(max_length=20, null=True, blank=True)
    recordId = models.IntegerField(null=True, blank=True, unique=True)
    smartcardId = models.CharField(max_length=50, null=True)
    subscriberCode = models.CharField(max_length=50, null=True)
    timestamp = models.DateTimeField(null=True, blank=True)
    dataDate = models.DateField(null=True, blank=True)
    timeDate = models.IntegerField(null=True, blank=True)
    whoisCountry = models.CharField(max_length=20, null=True, blank=True)
    whoisIsp = models.CharField(max_length=20, null=True, blank=True)
    
    class Meta:
        abstract = True  # Esto hace que no se cree una tabla para este modelo
    
    def __str__(self):
        # Método seguro que no falla si data es None
        return f"Record {self.recordId or 'N/A'} - Action {self.actionId or 'N/A'}"


class TelemetryRecordEntryDelancer(TelemetryBase):
    """Tabla principal - almacena TODOS los registros"""
    
    class Meta:
        db_table = 'telemetry_record_entry'
        verbose_name = 'Telemetry Record Entry'
        verbose_name_plural = 'Telemetry Record Entries'
        ordering = ['-timestamp']  # Cambié de '-created' a '-timestamp'
        indexes = [
            models.Index(fields=['actionId', 'timestamp']),
            models.Index(fields=['recordId']),  # Ya es unique, pero el índice ayuda
            models.Index(fields=['timestamp']),
            models.Index(fields=['deviceId', 'timestamp']),
            models.Index(fields=['dataDate', 'timeDate']),  # Para filtros por fecha/hora
        ]


class MergedTelemetricOTTDelancer(TelemetryBase):
    """Tabla especializada para OTT Streams (actionId 7, 8)"""
    
    class Meta:
        db_table = 'merged_telemetric_ott'
        verbose_name = 'Merged Telemetric OTT'
        verbose_name_plural = 'Merged Telemetric OTT'
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['actionId', 'timestamp']),
            models.Index(fields=['dataDate', 'timeDate']),  # Para tus consultas por franja horaria
            models.Index(fields=['dataName']),  # Para agrupaciones por canal
            models.Index(fields=['deviceId', 'dataDate']),  # Para análisis por dispositivo
            models.Index(fields=['recordId']),
            # Índices compuestos optimizados para consultas frecuentes
            models.Index(fields=['subscriberCode', 'dataDate']),  # Para análisis de usuarios por fecha
            models.Index(fields=['dataDate', 'dataName']),  # Para análisis por canal y fecha
            models.Index(fields=['dataDate', 'subscriberCode', 'dataName']),  # Consultas complejas
            models.Index(fields=['timestamp', 'dataDate']),  # Para filtros temporales
            models.Index(fields=['dataDate', 'dataDuration']),  # Para análisis de duración por fecha
        ]


class MergedTelemetricDVBDelancer(TelemetryBase):
    """Tabla especializada para DVB Services (actionId 5, 6)"""
    
    class Meta:
        db_table = 'merged_telemetric_dvb'
        verbose_name = 'Merged Telemetric DVB'
        verbose_name_plural = 'Merged Telemetric DVB'
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['actionId', 'timestamp']),
            models.Index(fields=['dataDate', 'timeDate']),
            models.Index(fields=['dataName']),
            models.Index(fields=['deviceId', 'dataDate']),
            models.Index(fields=['recordId']),
        ]


class MergedTelemetricStopCatchupDelancer(TelemetryBase):
    """Tabla especializada para Catchup detenido (actionId 17)"""
    
    class Meta:
        db_table = 'merged_telemetric_stop_catchup'
        verbose_name = 'Merged Telemetric Stop Catchup'
        verbose_name_plural = 'Merged Telemetric Stop Catchup'
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['actionId', 'timestamp']),
            models.Index(fields=['dataDate']),
            models.Index(fields=['dataName']),
            models.Index(fields=['deviceId', 'dataDate']),
            models.Index(fields=['recordId']),
        ]


class MergedTelemetricEndCatchupDelancer(TelemetryBase):
    """Tabla especializada para Catchup terminado (actionId 18)"""
    
    class Meta:
        db_table = 'merged_telemetric_end_catchup'
        verbose_name = 'Merged Telemetric End Catchup'
        verbose_name_plural = 'Merged Telemetric End Catchup'
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['actionId', 'timestamp']),
            models.Index(fields=['dataDate']),
            models.Index(fields=['dataName']),
            models.Index(fields=['deviceId', 'dataDate']),
            models.Index(fields=['recordId']),
        ]


class MergedTelemetricStopVODDelancer(TelemetryBase):
    """Tabla especializada para VOD detenido (actionId 14)"""
    
    class Meta:
        db_table = 'merged_telemetric_stop_vod'
        verbose_name = 'Merged Telemetric Stop VOD'
        verbose_name_plural = 'Merged Telemetric Stop VOD'
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['actionId', 'timestamp']),
            models.Index(fields=['dataDate']),
            models.Index(fields=['dataName']),
            models.Index(fields=['deviceId', 'dataDate']),
            models.Index(fields=['recordId']),
        ]


class MergedTelemetricEndVODDelancer(TelemetryBase):
    """Tabla especializada para VOD terminado (actionId 15)"""
    
    class Meta:
        db_table = 'merged_telemetric_end_vod'
        verbose_name = 'Merged Telemetric End VOD'
        verbose_name_plural = 'Merged Telemetric End VOD'
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['actionId', 'timestamp']),
            models.Index(fields=['dataDate']),
            models.Index(fields=['dataName']),
            models.Index(fields=['deviceId', 'dataDate']),
            models.Index(fields=['recordId']),
        ]