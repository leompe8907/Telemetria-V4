"""
Cliente para interactuar con la API de PanAccess.

Este módulo proporciona una clase cliente para realizar llamadas a la API
de PanAccess, manejando automáticamente la autenticación y el sessionId.
"""
import logging
import time
import requests
from urllib.parse import urlencode
from typing import Dict, Any, Optional

from appConfig import PanaccessConfigDelancer
from delancert.server.auth import login, logged_in
from delancert.exceptions import (
    PanAccessException,
    PanAccessConnectionError,
    PanAccessTimeoutError,
    PanAccessAPIError,
    PanAccessSessionError
)

logger = logging.getLogger(__name__)


class PanAccessClient:
    """
    Cliente para interactuar con la API de PanAccess.
    
    Maneja la autenticación y el sessionId automáticamente.
    Proporciona métodos para realizar llamadas a las funciones de la API.
    """
    
    # Configuración de reintentos para errores de conexión/timeout
    MAX_RETRY_ATTEMPTS = 3
    INITIAL_RETRY_DELAY = 2  # segundos
    MAX_RETRY_DELAY = 30  # segundos
    
    def __init__(self, base_url: str = None):
        """
        Inicializa el cliente de PanAccess.
        
        Args:
            base_url: URL base de PanAccess (por defecto usa la de la configuración)
        """
        PanaccessConfigDelancer.configure()
        self.base_url = base_url or PanaccessConfigDelancer.DRM
        self.session_id: Optional[str] = None
    
    def authenticate(self) -> str:
        """
        Realiza la autenticación con PanAccess y guarda el sessionId.
        
        Returns:
            sessionId obtenido de PanAccess
        
        Raises:
            PanAccessException: Si hay algún error en la autenticación
        """
        self.session_id = login()
        return self.session_id
    
    def _ensure_valid_session(self):
        """
        Asegura que haya una sesión válida.
        
        Si no hay sessionId, realiza un nuevo login automáticamente.
        NO valida la sesión antes de cada llamada para evitar logins innecesarios.
        La validación periódica del singleton se encarga de mantener la sesión activa.
        """
        # Si no hay sessionId, autenticar
        if not self.session_id:
            self.authenticate()
            return
        
        # NO validar la sesión aquí - confiar en la validación periódica del singleton
        # Esto evita logins innecesarios cuando la sesión es válida pero hay errores
        # temporales o de permisos en la validación
        # La validación periódica en background se encargará de refrescar si es necesario
    
    def call(self, func_name: str, parameters: Dict[str, Any] = None, timeout: int = 60) -> Dict[str, Any]:
        """
        Llama a una función remota del API PanAccess con reintentos automáticos.
        
        Si no hay sessionId o si está caducado, intenta autenticarse/refrescar
        automáticamente antes de realizar la llamada (excepto para la función 'login').
        
        Implementa reintentos con backoff exponencial para errores de conexión y timeout.
        
        Args:
            func_name: Nombre de la función a llamar (ej: 'cvGetSubscriber')
            parameters: Diccionario con los parámetros de la función
            timeout: Timeout en segundos para la conexión (default: 60)
        
        Returns:
            Diccionario con la respuesta de la API
        
        Raises:
            PanAccessException: Si hay algún error en la llamada después de todos los reintentos
        """
        if parameters is None:
            parameters = {}
        
        # Asegurar sesión válida antes de hacer la llamada (excepto para login)
        if func_name != 'login' and func_name != 'cvLoggedIn':
            self._ensure_valid_session()
        
        # Preparar parámetros para logging (ocultar sessionId por seguridad)
        log_parameters = parameters.copy()
        if 'sessionId' in log_parameters:
            session_id_value = log_parameters['sessionId']
            if session_id_value:
                log_parameters['sessionId'] = f"{session_id_value[:20]}..." if len(str(session_id_value)) > 20 else "[REDACTED]"
        
        # Agregar sessionId a los parámetros si existe y no es login
        if self.session_id and func_name != 'login' and func_name != 'cvLoggedIn':
            parameters['sessionId'] = self.session_id
        
        # Construir URL
        url = f"{self.base_url}?f={func_name}&requestMode=function"
        
        # Preparar headers y datos
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        param_string = urlencode(parameters)
        
        # Log de la petición
        logger.debug(f"Llamando '{func_name}' - Parámetros: {log_parameters}")
        
        # Reintentos con backoff exponencial para errores de conexión/timeout
        attempt = 0
        delay = self.INITIAL_RETRY_DELAY
        last_exception = None
        
        while attempt < self.MAX_RETRY_ATTEMPTS:
            try:
                response = requests.post(
                    url,
                    data=param_string,
                    headers=headers,
                    timeout=timeout
                )
                
                response.raise_for_status()
                
                # Parsear respuesta JSON
                try:
                    json_response = response.json()
                except ValueError as e:
                    logger.error(f"Error al parsear JSON '{func_name}': {str(e)}")
                    raise PanAccessAPIError(
                        f"Respuesta inválida del servidor PanAccess: {response.text}",
                        status_code=response.status_code
                    )
                
                # Verificar si hay error en la respuesta
                success = json_response.get("success")
                
                if not success:
                    error_message = json_response.get("errorMessage", "Error desconocido")
                    logger.error(f"Llamada '{func_name}' falló: {error_message}")
                    
                    # Si el error es de sesión o permisos, limpiar sessionId
                    error_lower = error_message.lower()
                    is_session_error = (
                        "session" in error_lower or 
                        "logged" in error_lower or 
                        "permission" in error_lower or
                        "do not have" in error_lower
                    )
                    
                    if is_session_error:
                        logger.warning(f"Error de sesión/permisos '{func_name}', limpiando sessionId")
                        self.session_id = None
                        raise PanAccessSessionError(
                            f"Error de sesión: {error_message}"
                        )
                    
                    raise PanAccessAPIError(
                        f"Error en la respuesta de PanAccess: {error_message}",
                        status_code=response.status_code
                    )
                
                logger.debug(f"Llamada '{func_name}' exitosa")
                return json_response
                
            except requests.exceptions.Timeout as e:
                attempt += 1
                last_exception = e
                logger.warning(f"Timeout '{func_name}' (intento {attempt}/{self.MAX_RETRY_ATTEMPTS})")
                
                # Si es el último intento, lanzar excepción
                if attempt >= self.MAX_RETRY_ATTEMPTS:
                    logger.error(f"Timeout después de {self.MAX_RETRY_ATTEMPTS} intentos")
                    raise PanAccessTimeoutError(
                        f"Timeout al llamar a {func_name}. "
                        f"El servidor no respondió en {timeout} segundos después de {self.MAX_RETRY_ATTEMPTS} intentos."
                    )
                
                # Calcular delay con backoff exponencial
                delay = min(delay * 2, self.MAX_RETRY_DELAY)
                logger.debug(f"Reintentando en {delay} segundos...")
                time.sleep(delay)
                
            except requests.exceptions.ConnectionError as e:
                attempt += 1
                last_exception = e
                logger.warning(f"Error de conexión '{func_name}' (intento {attempt}/{self.MAX_RETRY_ATTEMPTS})")
                
                # Si es el último intento, lanzar excepción
                if attempt >= self.MAX_RETRY_ATTEMPTS:
                    logger.error(f"Error de conexión después de {self.MAX_RETRY_ATTEMPTS} intentos")
                    raise PanAccessConnectionError(
                        f"Error de conexión con PanAccess después de {self.MAX_RETRY_ATTEMPTS} intentos: {str(e)}"
                    )
                
                # Calcular delay con backoff exponencial
                delay = min(delay * 2, self.MAX_RETRY_DELAY)
                logger.info(f"🔄 [call] Reintentando en {delay} segundos...")
                time.sleep(delay)
                
            except requests.exceptions.HTTPError as e:
                status_code = response.status_code if 'response' in locals() else None
                logger.error(f"❌ [call] Error HTTP al llamar a '{func_name}': {str(e)} (Status: {status_code})")
                if 'response' in locals():
                    logger.error(f"❌ [call] Respuesta completa: {response.text}")
                raise PanAccessAPIError(
                    f"Error HTTP al llamar a {func_name}: {str(e)}",
                    status_code=status_code
                )
            except (PanAccessSessionError, PanAccessAPIError):
                # No reintentar para errores de sesión o API - lanzar inmediatamente
                raise
            except Exception as e:
                # Para otros errores, no reintentar (pueden ser errores de lógica)
                logger.error(f"💥 [call] Error inesperado al llamar a '{func_name}': {str(e)}", exc_info=True)
                raise PanAccessAPIError(
                    f"Error inesperado al llamar a {func_name}: {str(e)}"
                )
        
        # No debería llegar aquí, pero por seguridad
        if last_exception:
            if isinstance(last_exception, requests.exceptions.Timeout):
                raise PanAccessTimeoutError(
                    f"Timeout al llamar a {func_name} después de {self.MAX_RETRY_ATTEMPTS} intentos"
                )
            else:
                raise PanAccessConnectionError(
                    f"Error de conexión con PanAccess después de {self.MAX_RETRY_ATTEMPTS} intentos"
                )
    
    def logout(self) -> bool:
        """
        Cierra la sesión actual en PanAccess.
        
        Returns:
            True si el logout fue exitoso, False en caso contrario
        
        Raises:
            PanAccessException: Si hay algún error al cerrar sesión
        """
        if not self.session_id:
            return True  # Ya no hay sesión activa
        
        try:
            result = self.call("cvLogout", {})
            self.session_id = None
            return result.get("success", False)
        except PanAccessException:
            # Limpiar sessionId incluso si hay error
            self.session_id = None
            raise
    
    def is_authenticated(self) -> bool:
        """
        Verifica si hay una sesión activa.
        
        Returns:
            True si hay sessionId, False en caso contrario
        """
        return self.session_id is not None
    
    def check_session(self) -> bool:
        """
        Verifica si la sesión actual sigue siendo válida.
        
        Returns:
            True si la sesión es válida, False si está caducada
        
        Raises:
            PanAccessException: Si hay algún error al verificar la sesión
        """
        if not self.session_id:
            return False
        
        try:
            return logged_in(self.session_id)
        except PanAccessException:
            # Si hay error al verificar, asumimos que la sesión no es válida
            self.session_id = None
            return False

