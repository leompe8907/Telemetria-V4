"""
Módulo para fusionar registros de telemetría OTT (actionId 7 y 8).

Objetivo:
- Para registros OTT con actionId=8, completar/normalizar `dataName` usando el
  `dataName` de actionId=7 cuando comparten el mismo `dataId`.
- Guardar el resultado en `MergedTelemetricOTTDelancer` (tabla materializada para consultas).
"""

import logging
from typing import List, Optional

from django.db import transaction
from django.db.models import Max, OuterRef, Subquery

from delancert.models import TelemetryRecordEntryDelancer, MergedTelemetricOTTDelancer

logger = logging.getLogger(__name__)


def merge_ott_records(
    max_record_id: Optional[int] = None,
    batch_size: int = 500,
    backfill_last_n: int = 0,
) -> dict:
    """
    Fusiona registros OTT (actionId 7 y 8) y los guarda en MergedTelemetricOTTDelancer.

    Estrategia (eficiente):
    - Procesa solo actionId=8 con recordId > max_record_id.
    - Para cada fila, obtiene el `dataName` más reciente de actionId=7 con el mismo dataId
      usando un Subquery (evita cargar un mapping gigante en memoria).

    Args:
        max_record_id: recordId máximo ya procesado (None => se toma el máximo de la tabla destino)
        batch_size: tamaño de lote para bulk_create
        backfill_last_n: si > 0, re-procesa también los últimos N recordIds (útil si actionId=7 llega tarde)

    Returns:
        Métricas del merge.
    """
    if max_record_id is None:
        max_record_result = MergedTelemetricOTTDelancer.objects.aggregate(max_record=Max("recordId"))
        max_record_id = max_record_result["max_record"] or 0
        logger.info(f"Obtenido max_record_id de BD: {max_record_id}")

    if backfill_last_n < 0:
        backfill_last_n = 0

    # Para backfill: re-procesar una ventana reciente y "reemplazar" esos registros en la tabla destino
    start_record_id = max(0, max_record_id - backfill_last_n)
    logger.info(
        "Iniciando merge OTT "
        f"(max_record_id={max_record_id}, backfill_last_n={backfill_last_n}, start_record_id={start_record_id})"
    )

    # Salida rápida: si no hay actionId=8 nuevos (ni ventana a backfill), no hacer nada.
    if backfill_last_n == 0:
        has_new = TelemetryRecordEntryDelancer.objects.filter(actionId=8, recordId__gt=max_record_id).exists()
        if not has_new:
            return {
                "total_processed": 0,
                "merged_records": 0,
                "saved_records": 0,
                "skipped_records": 0,
                "errors": 0,
                "start_record_id": start_record_id,
                "max_record_id": max_record_id,
                "backfill_last_n": backfill_last_n,
                "deleted_existing": 0,
            }

    # Subquery: dataName más reciente (por recordId) para actionId=7 y mismo dataId
    latest_action7_name = (
        TelemetryRecordEntryDelancer.objects.filter(actionId=7, dataId=OuterRef("dataId"))
        .exclude(dataName__isnull=True)
        .exclude(dataName="")
        .order_by("-recordId")
        .values("dataName")[:1]
    )

    action8_records = (
        TelemetryRecordEntryDelancer.objects.filter(actionId=8, recordId__gt=start_record_id, dataId__isnull=False)
        .annotate(action7_data_name=Subquery(latest_action7_name))
        .order_by("recordId")
    )

    total_processed = action8_records.count()
    logger.info(f"Registros a procesar: {total_processed}")

    if total_processed == 0:
        return {
            "total_processed": 0,
            "merged_records": 0,
            "saved_records": 0,
            "skipped_records": 0,
            "errors": 0,
            "start_record_id": start_record_id,
            "max_record_id": max_record_id,
            "backfill_last_n": backfill_last_n,
            "deleted_existing": 0,
        }

    merged_objects: List[MergedTelemetricOTTDelancer] = []
    merged_count = 0
    skipped_count = 0
    error_count = 0
    saved_total = 0
    deleted_existing = 0

    # Si estamos en modo backfill, eliminar previamente el rango para reinsertar con dataName corregido.
    # Esto funciona porque `recordId` es unique en la tabla destino.
    if start_record_id < max_record_id:
        deleted_existing = MergedTelemetricOTTDelancer.objects.filter(recordId__gt=start_record_id).delete()[0]
        logger.info(f"Backfill activo: eliminados {deleted_existing} registros existentes (recordId > {start_record_id})")

    for record in action8_records.iterator(chunk_size=1000):
        try:
            merged_data_name = record.action7_data_name
            if merged_data_name:
                merged_count += 1
            else:
                merged_data_name = record.dataName
                skipped_count += 1

            merged_objects.append(
                MergedTelemetricOTTDelancer(
                    actionId=record.actionId,
                    actionKey=record.actionKey,
                    anonymized=record.anonymized,
                    data=record.data,
                    dataDuration=record.dataDuration,
                    dataId=record.dataId,
                    dataName=merged_data_name,
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
            )

            if len(merged_objects) >= batch_size:
                saved_total += _bulk_save_merged(merged_objects, batch_size=batch_size)
                merged_objects = []
        except Exception as e:
            error_count += 1
            logger.error(f"Error procesando recordId {getattr(record, 'recordId', 'N/A')}: {str(e)}")

    if merged_objects:
        saved_total += _bulk_save_merged(merged_objects, batch_size=batch_size)

    result = {
        "total_processed": total_processed,
        "merged_records": merged_count,
        "saved_records": saved_total,
        "skipped_records": skipped_count,
        "errors": error_count,
        "start_record_id": start_record_id,
        "max_record_id": max_record_id,
        "backfill_last_n": backfill_last_n,
        "deleted_existing": deleted_existing,
    }

    logger.info(
        f"Merge OTT completado: {total_processed} procesados, "
        f"{merged_count} fusionados, {saved_total} guardados, "
        f"{skipped_count} sin dataName (actionId=7), {error_count} errores"
    )
    return result


def _bulk_save_merged(merged_objects: List[MergedTelemetricOTTDelancer], batch_size: int) -> int:
    try:
        with transaction.atomic():
            MergedTelemetricOTTDelancer.objects.bulk_create(
                merged_objects,
                ignore_conflicts=True,
                batch_size=batch_size,
            )
        return len(merged_objects)
    except Exception as e:
        logger.error(f"Error guardando lote: {str(e)}")
        return 0

