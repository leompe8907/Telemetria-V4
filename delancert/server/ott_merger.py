"""
Módulo para fusionar registros de telemetría OTT (actionId 7 y 8).

Este módulo fusiona el dataName de actionId=7 a actionId=8 cuando comparten
el mismo dataId, y guarda los registros en la tabla MergedTelemetricOTTDelancer.
"""
import logging
from typing import List, Optional
from django.db import transaction
from django.db.models import Max

from delancert.models import TelemetryRecordEntryDelancer, MergedTelemetricOTTDelancer

logger = logging.getLogger(__name__)


def merge_ott_records(max_record_id: Optional[int] = None, batch_size: int = 500) -> dict:
    """
    Fusiona registros OTT (actionId 7 y 8) y los guarda en MergedTelemetricOTTDelancer.
    
    Lógica:
    1. Obtiene el mapeo dataId -> dataName de actionId=7
    2. Obtiene registros actionId=8 nuevos (recordId > max_record_id)
    3. Fusiona el dataName de actionId=7 a actionId=8 cuando comparten dataId
    4. Guarda los registros fusionados en MergedTelemetricOTT
    
    Args:
        max_record_id: RecordId máximo ya procesado (None = procesar todos)
        batch_size: Tamaño del lote para bulk_create
    
    Returns:
        Diccionario con estadísticas:
        {
            "total_processed": int,
            "merged_records": int,
            "saved_records": int,
            "skipped_records": int,
            "errors": int
        }
    """
    # Si no se proporciona max_record_id, obtener el máximo de la tabla destino
    if max_record_id is None:
        max_record_result = MergedTelemetricOTTDelancer.objects.aggregate(max_record=Max('recordId'))
        max_record_id = max_record_result['max_record'] or 0
        logger.info(f"Obtenido max_record_id de BD: {max_record_id}")
    
    logger.info(f"Iniciando merge OTT desde recordId {max_record_id}")
    
    # 1. Obtener mapeo dataId -> dataName de actionId=7 (solo los que tienen dataName válido)
    logger.info("Obteniendo mapeo dataId->dataName de actionId=7...")
    action7_mapping = TelemetryRecordEntryDelancer.objects.filter(
        actionId=7,
        dataId__isnull=False,
        dataName__isnull=False
    ).exclude(dataName='').values('dataId', 'dataName').distinct()
    
    dataid_to_dataname = {item['dataId']: item['dataName'] for item in action7_mapping}
    logger.info(f"Mapeo obtenido: {len(dataid_to_dataname)} dataId únicos con dataName")
    
    # 2. Obtener registros actionId=8 nuevos que necesitan merge
    logger.info(f"Obteniendo registros actionId=8 con recordId > {max_record_id}...")
    action8_records = TelemetryRecordEntryDelancer.objects.filter(
        actionId=8,
        recordId__gt=max_record_id,
        dataId__isnull=False
    ).order_by('recordId')
    
    total_processed = action8_records.count()
    logger.info(f"Registros a procesar: {total_processed}")
    
    if total_processed == 0:
        return {
            "total_processed": 0,
            "merged_records": 0,
            "saved_records": 0,
            "skipped_records": 0,
            "errors": 0
        }
    
            # 3. Crear objetos MergedTelemetricOTTDelancer con dataName fusionado
    merged_objects = []
    merged_count = 0
    skipped_count = 0
    error_count = 0
    
    for record in action8_records.iterator(chunk_size=1000):
        try:
            # Fusionar dataName si existe en actionId=7
            merged_dataName = dataid_to_dataname.get(record.dataId)
            
            if merged_dataName:
                merged_count += 1
            else:
                # Si no hay dataName para fusionar, usar el original (puede ser None)
                merged_dataName = record.dataName
                skipped_count += 1
            
            # Crear objeto MergedTelemetricOTTDelancer con todos los campos
            merged_obj = MergedTelemetricOTTDelancer(
                actionId=record.actionId,
                actionKey=record.actionKey,
                anonymized=record.anonymized,
                data=record.data,
                dataDuration=record.dataDuration,
                dataId=record.dataId,
                dataName=merged_dataName,  # ← Fusionado de actionId=7
                dataNetId=record.dataNetId,
                dataPrice=record.dataPrice,
                dataSeviceId=record.dataSeviceId,
                dataTsId=record.dataTsId,
                date=record.date,
                deviceId=record.deviceId,
                ip=record.ip,
                ipId=record.ipId,
                manual=record.manual,
                profileId=record.profileId,
                reaonId=record.reaonId,
                reasonKey=record.reasonKey,
                recordId=record.recordId,
                smartcardId=record.smartcardId,
                subscriberCode=record.subscriberCode,
                timestamp=record.timestamp,
                dataDate=record.dataDate,
                timeDate=record.timeDate,
                whoisCountry=record.whoisCountry,
                whoisIsp=record.whoisIsp,
            )
            
            merged_objects.append(merged_obj)
            
            # Guardar en lotes
            if len(merged_objects) >= batch_size:
                saved = _bulk_save_merged(merged_objects, batch_size)
                merged_objects = []
                
        except Exception as e:
            error_count += 1
            logger.error(f"Error procesando recordId {record.recordId}: {str(e)}")
            continue
    
    # Guardar registros restantes
    saved_count = 0
    if merged_objects:
        saved_count = _bulk_save_merged(merged_objects, batch_size)
    
    result = {
        "total_processed": total_processed,
        "merged_records": merged_count,
        "saved_records": saved_count,
        "skipped_records": skipped_count,
        "errors": error_count
    }
    
    logger.info(
        f"Merge OTT completado: {total_processed} procesados, "
        f"{merged_count} fusionados, {saved_count} guardados, "
        f"{skipped_count} sin dataName, {error_count} errores"
    )
    
    return result


def _bulk_save_merged(merged_objects: List[MergedTelemetricOTTDelancer], batch_size: int) -> int:
    """
    Guarda objetos MergedTelemetricOTTDelancer usando bulk_create.
    
    Args:
        merged_objects: Lista de objetos MergedTelemetricOTTDelancer
        batch_size: Tamaño del lote
    
    Returns:
        Cantidad de registros guardados
    """
    try:
        with transaction.atomic():
            MergedTelemetricOTTDelancer.objects.bulk_create(
                merged_objects,
                ignore_conflicts=True,
                batch_size=batch_size
            )
        saved_count = len(merged_objects)
        logger.debug(f"Guardado lote: {saved_count} registros")
        return saved_count
    except Exception as e:
        logger.error(f"Error guardando lote: {str(e)}")
        return 0

