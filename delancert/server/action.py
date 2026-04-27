from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from delancert.utils.api_key_permission import HasTelemetryApiKey
import logging
import math
from decimal import Decimal
from delancert.server.telemetry_fetcher import (
    fetch_telemetry_records_smart,
    save_telemetry_records,
    is_database_empty,
    get_highest_record_id,
    get_telemetry_records,
    extract_timestamp_details
)
from delancert.server.ott_merger import merge_ott_records
from delancert.exceptions import PanAccessException, PanAccessAPIError
from datetime import datetime, date

logger = logging.getLogger(__name__)
from delancert.utils.rate_limit import acquire_rate_limit

def _serialize_for_json(obj):
    """
    Serializa objetos para JSON, convirtiendo datetime, date, Decimal y otros tipos no serializables.
    """
    # Tipos primitivos que ya son serializables
    if obj is None or isinstance(obj, (str, int, bool)):
        return obj
    
    # Floats - manejar NaN, inf, -inf
    if isinstance(obj, float):
        if math.isnan(obj):
            return None  # Convertir NaN a None
        elif math.isinf(obj):
            return None if obj > 0 else None  # Convertir inf a None
        return obj
    
    # Fechas y tiempos
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    
    # Decimal (común en Django)
    if isinstance(obj, Decimal):
        val = float(obj)
        # Verificar si el Decimal convertido es NaN o inf
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    
    # Diccionarios
    if isinstance(obj, dict):
        return {key: _serialize_for_json(value) for key, value in obj.items()}
    
    # Listas y tuplas
    if isinstance(obj, (list, tuple)):
        return [_serialize_for_json(item) for item in obj]
    
    # Sets
    if isinstance(obj, set):
        return [_serialize_for_json(item) for item in obj]
    
    # Objetos con __dict__ (modelos, etc.)
    if hasattr(obj, '__dict__'):
        try:
            return _serialize_for_json(obj.__dict__)
        except (TypeError, AttributeError):
            pass
    
    # Si es un objeto QuerySet o similar, convertirlo a lista
    if hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes)):
        try:
            return [_serialize_for_json(item) for item in obj]
        except (TypeError, AttributeError):
            pass
    
    # Manejar tipos de NumPy/Pandas (opcional)
    try:
        import numpy as np  # type: ignore[import-not-found]
        if isinstance(obj, (np.integer, np.floating)):
            val = float(obj)
            if math.isnan(val) or math.isinf(val):
                return None
            return val
        if isinstance(obj, np.ndarray):
            return [_serialize_for_json(item) for item in obj.tolist()]
    except (ImportError, AttributeError, TypeError):
        pass
    
    # Si todo falla, convertir a string
    try:
        return str(obj)
    except Exception:
        return None


class TelemetrySyncView(APIView):
    """
    Endpoint único para sincronizar registros de telemetría desde PanAccess.
    
    Este endpoint descarga y guarda automáticamente los registros:
    - Si la BD está vacía: descarga un lote de registros (o max_records si se especifica)
    - Si la BD tiene registros: descarga solo los NUEVOS desde el último recordId
    """
    permission_classes = [HasTelemetryApiKey]
    
    def post(self, request):
        """
        POST: Sincroniza registros de telemetría desde PanAccess.
        
        Parámetros opcionales:
        - limit: Cantidad de registros por página (default: 100, max: 1000)
        - process_timestamps: Si procesar timestamps para extraer fecha/hora (default: true)
        - batch_size: Tamaño del lote para guardar en BD (default: 100)
        """
        try:
            rl = acquire_rate_limit("telemetry_sync", ttl_seconds=30)
            if not rl.allowed:
                return Response(
                    {"success": False, "error": "Rate limited", "retry_after_seconds": rl.retry_after_seconds},
                    status=status.HTTP_429_TOO_MANY_REQUESTS,
                    headers={"Retry-After": str(rl.retry_after_seconds)},
                )

            # Obtener parámetros
            limit = int(request.data.get('limit', 1000))
            process_timestamps = request.data.get('process_timestamps', True)
            batch_size = int(request.data.get('batch_size', 1000))
            
            if isinstance(process_timestamps, str):
                process_timestamps = process_timestamps.lower() in ('true', '1', 'yes')
            
            # Estado inicial de la BD
            was_empty_before = is_database_empty()
            highest_id_before = get_highest_record_id()
            
            logger.info(f"Sincronización iniciada - limit={limit}, process_timestamps={process_timestamps}, batch_size={batch_size}")
            
            # Descargar y guardar en lotes (más seguro ante interrupciones)
            total_downloaded = 0
            total_saved = 0
            total_skipped = 0
            total_errors = 0
            
            # Verificar si la BD está vacía
            if is_database_empty():
                logger.info("BD vacía - descargando y guardando TODOS los registros en lotes")
                # Descargar y guardar página por página
                offset = 0
                page_count = 0
                
                while True:
                    try:
                        page_count += 1
                        # Descargar un lote                        
                        response = get_telemetry_records(
                            offset=offset,
                            limit=limit,
                            order_by="recordId",
                            order_dir="DESC"
                        )
                        
                        answer = response.get("answer", {})
                        records = answer.get("telemetryRecordEntries", [])
                        
                        if not records:
                            logger.info(f"No hay más registros en offset {offset}")
                            break
                        
                        # Procesar timestamps si se solicita
                        if process_timestamps:
                            records = extract_timestamp_details(records)
                        
                        total_downloaded += len(records)
                        
                        # Guardar inmediatamente este lote
                        logger.info(f"Guardando lote {page_count}: {len(records)} registros (offset={offset})")
                        save_result = save_telemetry_records(records, batch_size=batch_size)
                        total_saved += save_result['saved_records']
                        total_skipped += save_result['skipped_records']
                        total_errors += save_result['errors']
                        
                        logger.info(f"Lote {page_count} guardado: {save_result['saved_records']} guardados, {save_result['skipped_records']} omitidos")
                        
                        # Si obtuvimos menos registros que el límite, es la última página
                        if len(records) < limit:
                            break
                        
                        # Preparar siguiente página
                        offset += limit
                        
                    except Exception as e:
                        logger.error(f"Error en lote {page_count} (offset={offset}): {str(e)}", exc_info=True)
                        # Continuar con el siguiente lote en lugar de fallar completamente
                        offset += limit
                        continue
            else:
                # BD tiene registros - descargar solo los nuevos
                logger.info("BD tiene registros - descargando solo los nuevos desde el último recordId")
                records = fetch_telemetry_records_smart(
                    limit=limit,
                    process_timestamps=process_timestamps
                )
                
                total_downloaded = len(records)
                
                if records:
                    logger.info(f"Guardando {len(records)} registros nuevos en BD")
                    save_result = save_telemetry_records(records, batch_size=batch_size)
                    total_saved = save_result['saved_records']
                    total_skipped = save_result['skipped_records']
                    total_errors = save_result['errors']
            
            save_result = {
                "total_records": total_downloaded,
                "saved_records": total_saved,
                "skipped_records": total_skipped,
                "errors": total_errors
            }
            
            # Estado final de la BD
            is_empty_after = is_database_empty()
            highest_id_after = get_highest_record_id()
            
            # Preparar respuesta
            response_data = {
                "success": True,
                "message": "Sincronización completada exitosamente",
                "download": {
                    "total_records_downloaded": total_downloaded
                },
                "save": save_result,
                "database_status": {
                    "was_empty_before": was_empty_before,
                    "highest_record_id_before": highest_id_before,
                    "is_empty_after": is_empty_after,
                    "highest_record_id_after": highest_id_after
                }
            }
            
            logger.info(
                f"Sincronización completada: {total_downloaded} descargados, "
                f"{total_saved} guardados, {total_skipped} omitidos, {total_errors} errores"
            )
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except PanAccessException as e:
            logger.error(f"Error de PanAccess: {str(e)}")
            return Response(
                {
                    "success": False,
                    "error": "Error de PanAccess",
                    "message": str(e)
                },
                status=status.HTTP_502_BAD_GATEWAY
            )
        except PanAccessAPIError as e:
            # Mapear permisos insuficientes a 502 (error upstream de configuración)
            if getattr(e, "error_code", None) == "no_access_to_function":
                return Response(
                    {
                        "success": False,
                        "error": "Permisos insuficientes en PanAccess",
                        "message": str(e),
                    },
                    status=status.HTTP_502_BAD_GATEWAY,
                )
            return Response(
                {
                    "success": False,
                    "error": "Error de API PanAccess",
                    "message": str(e),
                },
                status=status.HTTP_502_BAD_GATEWAY,
            )
        except Exception as e:
            logger.error(f"Error inesperado: {str(e)}", exc_info=True)
            return Response(
                {
                    "success": False,
                    "error": "Error inesperado",
                    "message": str(e)
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class MergeOTTView(APIView):
    """
    Endpoint para fusionar registros OTT (actionId 7 y 8) en MergedTelemetricOTTDelancer.
    
    Fusiona el dataName de actionId=7 a actionId=8 cuando comparten el mismo dataId.
    Solo procesa registros nuevos desde el último recordId guardado.
    """
    permission_classes = [HasTelemetryApiKey]
    
    def post(self, request):
        """
        POST: Ejecuta el merge de registros OTT.
        
        Parámetros opcionales:
        - max_record_id: RecordId máximo ya procesado (None = usar el máximo de la BD)
        - batch_size: Tamaño del lote para guardar (default: 500)
        """
        try:
            rl = acquire_rate_limit("merge_ott", ttl_seconds=30)
            if not rl.allowed:
                return Response(
                    {"success": False, "error": "Rate limited", "retry_after_seconds": rl.retry_after_seconds},
                    status=status.HTTP_429_TOO_MANY_REQUESTS,
                    headers={"Retry-After": str(rl.retry_after_seconds)},
                )

            # Obtener parámetros
            max_record_id = request.data.get('max_record_id')
            if max_record_id is not None:
                max_record_id = int(max_record_id)
            
            batch_size = int(request.data.get('batch_size', 500))
            backfill_last_n = int(request.data.get("backfill_last_n", 0))
            
            logger.info(
                f"Merge OTT iniciado - max_record_id={max_record_id}, batch_size={batch_size}, "
                f"backfill_last_n={backfill_last_n}"
            )
            
            # Ejecutar merge
            result = merge_ott_records(
                max_record_id=max_record_id,
                batch_size=batch_size,
                backfill_last_n=backfill_last_n,
            )
            
            # Preparar respuesta
            response_data = {
                "success": True,
                "message": "Merge OTT completado exitosamente",
                "result": result
            }
            
            logger.info(f"Merge OTT completado: {result}")
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Error en merge OTT: {str(e)}", exc_info=True)
            return Response(
                {
                    "success": False,
                    "error": "Error en merge OTT",
                    "message": str(e)
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def get(self, request):
        """
        GET: Obtiene información sobre el estado del merge OTT.
        """
        def _get_data(request):
            from delancert.models import MergedTelemetricOTTDelancer
            from django.db.models import Max, Count
            
            # Obtener estadísticas
            max_record = MergedTelemetricOTTDelancer.objects.aggregate(Max('recordId'))['recordId__max']
            total_records = MergedTelemetricOTTDelancer.objects.count()
            
            # Distribución por actionId
            action_dist = MergedTelemetricOTTDelancer.objects.values('actionId').annotate(
                count=Count('actionId')
            ).order_by('actionId')
            
            return {
                "message": "Estado del merge OTT",
                "merged_table_status": {
                    "total_records": total_records,
                    "max_record_id": max_record
                },
                "actionId_distribution": list(action_dist),
                "endpoint_info": {
                    "post": "/delancer/telemetry/merge/ott/ - Ejecuta el merge",
                    "parameters": {
                        "max_record_id": "Opcional - RecordId máximo ya procesado",
                        "batch_size": "Opcional - Tamaño del lote (default: 500)"
                    }
                }
            }
        
        try:
            data = _get_data(request)
            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"Error en GET merge OTT: {str(e)}", exc_info=True)
            return Response(
                {"error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class TelemetryRunView(APIView):
    """
    Endpoint operativo: ejecuta sync + merge OTT en una sola llamada.
    """

    permission_classes = [HasTelemetryApiKey]

    def post(self, request):
        try:
            rl = acquire_rate_limit("telemetry_run", ttl_seconds=60)
            if not rl.allowed:
                return Response(
                    {"success": False, "error": "Rate limited", "retry_after_seconds": rl.retry_after_seconds},
                    status=status.HTTP_429_TOO_MANY_REQUESTS,
                    headers={"Retry-After": str(rl.retry_after_seconds)},
                )

            limit = int(request.data.get("limit", 1000))
            process_timestamps = request.data.get("process_timestamps", True)
            batch_size = int(request.data.get("batch_size", 1000))
            merge_batch_size = int(request.data.get("merge_batch_size", 500))
            backfill_last_n = int(request.data.get("backfill_last_n", 0))

            if isinstance(process_timestamps, str):
                process_timestamps = process_timestamps.lower() in ("true", "1", "yes")

            # 1) Sync (reusar la misma lógica que TelemetrySyncView, pero sin duplicar todo)
            # Nota: por simplicidad, hacemos incremental si la BD no está vacía; si está vacía, paginamos.
            total_downloaded = 0
            total_saved = 0
            total_skipped = 0
            total_errors = 0

            was_empty_before = is_database_empty()
            highest_id_before = get_highest_record_id()

            if is_database_empty():
                offset = 0
                while True:
                    response = get_telemetry_records(offset=offset, limit=limit, order_by="recordId", order_dir="DESC")
                    answer = response.get("answer", {})
                    records = answer.get("telemetryRecordEntries", [])
                    if not records:
                        break
                    if process_timestamps:
                        records = extract_timestamp_details(records)
                    total_downloaded += len(records)
                    save_result = save_telemetry_records(records, batch_size=batch_size)
                    total_saved += save_result["saved_records"]
                    total_skipped += save_result["skipped_records"]
                    total_errors += save_result["errors"]
                    if len(records) < limit:
                        break
                    offset += limit
            else:
                records = fetch_telemetry_records_smart(limit=limit, process_timestamps=process_timestamps)
                total_downloaded = len(records)
                if records:
                    save_result = save_telemetry_records(records, batch_size=batch_size)
                    total_saved = save_result["saved_records"]
                    total_skipped = save_result["skipped_records"]
                    total_errors = save_result["errors"]

            highest_id_after = get_highest_record_id()

            # 2) Merge OTT (incremental + backfill opcional)
            merge_result = merge_ott_records(batch_size=merge_batch_size, backfill_last_n=backfill_last_n)

            return Response(
                {
                    "success": True,
                    "sync": {
                        "downloaded": total_downloaded,
                        "saved": total_saved,
                        "skipped": total_skipped,
                        "errors": total_errors,
                        "database_status": {
                            "was_empty_before": was_empty_before,
                            "highest_record_id_before": highest_id_before,
                            "highest_record_id_after": highest_id_after,
                        },
                    },
                    "merge_ott": merge_result,
                },
                status=status.HTTP_200_OK,
            )
        except ValueError as e:
            return Response({"success": False, "error": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"Error en telemetry run: {str(e)}", exc_info=True)
            return Response(
                {"success": False, "error": "Error en telemetry run", "message": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
