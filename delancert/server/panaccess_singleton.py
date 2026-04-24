"""
Cliente singleton thread-safe para PanAccess.

Este módulo proporciona una instancia única y compartida del cliente PanAccess
que se inicializa al arrancar Django y se mantiene durante toda la vida del servidor.
"""
import threading
import time
import logging
from typing import Optional

from delancert.server.panaccess_client import PanAccessClient
from delancert.server.auth import login, logged_in
from delancert.exceptions import (
    PanAccessException,
    PanAccessAuthenticationError,
    PanAccessConnectionError,
    PanAccessTimeoutError,
    PanAccessAPIError,
    PanAccessSessionError
)

logger = logging.getLogger(__name__)


class PanAccessSingleton:
    """
    Singleton thread-safe para el cliente PanAccess.
    
    Garantiza que solo haya una instancia del cliente compartida entre
    todos los threads/workers, con manejo seguro de concurrencia.
    """
    
    _instance = None
    _lock = threading.Lock()  # Lock para inicialización
    _session_lock = threading.RLock()  # Reentrant lock para sesión
    
    # Configuración de reintentos
    MAX_RETRY_ATTEMPTS = 5
    INITIAL_RETRY_DELAY = 1  # segundos
    MAX_RETRY_DELAY = 60  # segundos
    ALERT_AFTER_ATTEMPTS = 3  # Enviar alerta después de X intentos
    
    # Configuración de validación periódica
    VALIDATION_INTERVAL = 14400  # Validar cada 4 horas (14400 segundos)
    
    def __new__(cls):
        """
        Implementa el patrón Singleton con thread-safety.
        """
        if cls._instance is None:
            with cls._lock:
                # Double-check locking pattern
                if cls._instance is None:
                    cls._instance = super(PanAccessSingleton, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """
        Inicializa el singleton (solo se ejecuta una vez).
        """
        if self._initialized:
            return
        
        self.client = PanAccessClient()
        self._initialized = True
        self._retry_count = 0
        self._last_alert_sent = False
        self._validation_thread = None
        self._stop_validation = threading.Event()
        
        logger.debug("PanAccessSingleton inicializado")
    
    def _authenticate_with_retry(self) -> str:
        """
        Intenta autenticarse con reintentos y backoff exponencial.
        
        Returns:
            sessionId obtenido
        
        Raises:
            PanAccessException: Si falla después de todos los reintentos
        """
        attempt = 0
        delay = self.INITIAL_RETRY_DELAY
        
        while attempt < self.MAX_RETRY_ATTEMPTS:
            try:
                logger.debug(f"Intento de login #{attempt + 1}/{self.MAX_RETRY_ATTEMPTS}")
                session_id = login()
                
                # Login exitoso, resetear contador
                self._retry_count = 0
                self._last_alert_sent = False
                logger.info("Login exitoso")
                return session_id
                
            except (PanAccessAuthenticationError, PanAccessConnectionError, PanAccessTimeoutError) as e:
                attempt += 1
                self._retry_count = attempt
                
                # Enviar alerta después de X intentos
                if attempt >= self.ALERT_AFTER_ATTEMPTS and not self._last_alert_sent:
                    self._send_alert(attempt, str(e))
                    self._last_alert_sent = True
                
                # Si es el último intento, lanzar excepción
                if attempt >= self.MAX_RETRY_ATTEMPTS:
                    logger.error(f"Login falló después de {self.MAX_RETRY_ATTEMPTS} intentos")
                    raise PanAccessException(
                        f"Error de autenticación después de {self.MAX_RETRY_ATTEMPTS} intentos: {str(e)}"
                    )
                
                # Calcular delay con backoff exponencial
                delay = min(delay * 2, self.MAX_RETRY_DELAY)
                logger.warning(f"Login falló (intento {attempt}/{self.MAX_RETRY_ATTEMPTS}), reintentando en {delay}s")
                
                time.sleep(delay)
            
            except PanAccessException as e:
                # Re-lanzar excepciones de PanAccess
                raise
            except Exception as e:
                # Error inesperado
                attempt += 1
                if attempt >= self.MAX_RETRY_ATTEMPTS:
                    logger.error(f"❌ Error inesperado después de {attempt} intentos: {str(e)}")
                    raise PanAccessException(f"Error inesperado en login: {str(e)}")
                
                delay = min(delay * 2, self.MAX_RETRY_DELAY)
                logger.warning(
                    f"⚠️ Error inesperado (intento {attempt}/{self.MAX_RETRY_ATTEMPTS}). "
                    f"Reintentando en {delay} segundos..."
                )
                time.sleep(delay)
        
        # No debería llegar aquí, pero por seguridad
        raise PanAccessException("Error crítico: no se pudo autenticar después de múltiples intentos")
    
    def _send_alert(self, attempt: int, error_message: str):
        """
        Envía una alerta cuando se superan los intentos de alerta.
        
        Por ahora solo loguea, pero puedes extender esto para enviar emails,
        notificaciones, etc.
        
        Args:
            attempt: Número de intento actual
            error_message: Mensaje de error
        """
        alert_message = (
            f"🚨 ALERTA: PanAccess login ha fallado {attempt} veces. "
            f"Último error: {error_message}. "
            f"El sistema seguirá intentando hasta {self.MAX_RETRY_ATTEMPTS} intentos."
        )
        logger.error(alert_message)
        
        # TODO: Aquí puedes agregar:
        # - Envío de email
        # - Notificación a Slack/Discord
        # - Métricas a sistema de monitoreo
        # - etc.
    
    def ensure_session(self):
        """
        Asegura que haya una sesión válida (thread-safe).
        
        Si no hay sessionId, lo obtiene automáticamente.
        Solo valida la sesión si hay un error específico de sesión inválida en una llamada.
        Solo un thread puede ejecutar el refresh a la vez.
        """
        with self._session_lock:
            # Verificar si hay sessionId
            if not self.client.session_id:
                logger.debug("No hay sesión, autenticando...")
                self.client.session_id = self._authenticate_with_retry()
                return
            
            # NO validar la sesión aquí automáticamente
            # Solo validar si hay un error específico de sesión inválida en una llamada real
            # Esto evita logins innecesarios cuando la sesión es válida pero hay errores
            # de permisos o temporales en la validación
            logger.debug("Sesión existente, confiando en validación periódica")
    
    def call(self, func_name: str, parameters: dict = None, timeout: int = 60) -> dict:
        """
        Llama a una función de la API PanAccess (thread-safe).
        
        Usa el sessionId que está siendo mantenido por la validación periódica.
        Si por alguna razón no hay sesión, intenta obtenerla (fallback de seguridad).
        
        Nota: La validación periódica mantiene la sesión activa automáticamente,
        por lo que normalmente no será necesario validar aquí.
        
        Args:
            func_name: Nombre de la función a llamar
            parameters: Parámetros de la función
            timeout: Timeout en segundos
        
        Returns:
            Respuesta de la API
        
        Raises:
            PanAccessException: Si hay algún error
        """
        # Fallback de seguridad: si no hay sesión, intentar obtenerla
        # (normalmente la validación periódica ya la mantiene activa)
        if func_name != 'login' and func_name != 'cvLoggedIn':
            if not self.client.session_id:
                logger.warning("No hay sesión activa, obteniendo una...")
                self.ensure_session()
        
        # Usar el cliente para hacer la llamada
        # El cliente ya tiene el sessionId y lo agregará automáticamente
        try:
            return self.client.call(func_name, parameters, timeout)
        except PanAccessSessionError:
            # Si hay error de sesión, refrescar y reintentar una vez
            logger.warning(f"Error de sesión detectado en '{func_name}', refrescando sesión...")
            with self._session_lock:
                self.client.session_id = None
                self.ensure_session()
            # Reintentar la llamada con la nueva sesión
            logger.info(f"Reintentando llamada '{func_name}' con nueva sesión...")
            return self.client.call(func_name, parameters, timeout)
        except PanAccessAPIError as e:
            # Si es un error de permisos, no reintentar con nuevo login.
            if getattr(e, "error_code", None) == "no_access_to_function":
                raise
            raise
    
    def get_client(self) -> PanAccessClient:
        """
        Obtiene la instancia del cliente (para uso avanzado).
        
        Returns:
            Instancia del PanAccessClient
        """
        return self.client
    
    def reset_session(self):
        """
        Fuerza el reset de la sesión (útil para testing o recuperación).
        """
        with self._session_lock:
            self.client.session_id = None
            logger.debug("Sesión reseteada manualmente")
    
    def _periodic_validation(self):
        """
        Thread en background que valida periódicamente si la sesión está activa.
        
        Solo valida si hay una sesión existente y solo la refresca si realmente está caducada.
        Este thread se ejecuta cada VALIDATION_INTERVAL segundos.
        """
        logger.debug(f"Thread de validación periódica iniciado (intervalo: {self.VALIDATION_INTERVAL}s)")
        
        while not self._stop_validation.is_set():
            try:
                # Esperar el intervalo (o hasta que se detenga)
                if self._stop_validation.wait(timeout=self.VALIDATION_INTERVAL):
                    # Si el evento está activado, salir del loop
                    break
                
                # Solo validar si hay una sesión existente
                with self._session_lock:
                    if not self.client.session_id:
                        logger.debug("No hay sesión para validar, saltando validación periódica")
                        continue
                    
                    # Validar la sesión solo si existe
                    try:
                        logger.debug("Validando sesión periódicamente...")
                        is_valid = logged_in(self.client.session_id)
                        if not is_valid:
                            logger.info("Sesión caducada, refrescando...")
                            self.client.session_id = self._authenticate_with_retry()
                        else:
                            logger.debug("Sesión válida en validación periódica")
                    except (PanAccessConnectionError, PanAccessTimeoutError) as e:
                        # Error de conexión/timeout - no refrescar, solo loguear
                        logger.warning(f"Error de conexión en validación periódica: {str(e)}")
                    except PanAccessAPIError as e:
                        # Error de API - verificar si es por permisos o sesión inválida
                        error_code = getattr(e, 'error_code', None)
                        if error_code == 'no_access_to_function':
                            # Error de permisos, no de sesión inválida - mantener sesión
                            logger.debug("Error de permisos en validación periódica, manteniendo sesión")
                        else:
                            # Otro error de API - podría ser sesión inválida, refrescar
                            logger.warning(f"Error de API en validación periódica: {str(e)}")
                            try:
                                self.client.session_id = self._authenticate_with_retry()
                            except Exception:
                                logger.error("❌ Error al refrescar sesión en validación periódica")
                
                logger.debug("Validación periódica completada")
                
            except Exception as e:
                logger.error(f"❌ Error en validación periódica: {str(e)}")
                # Continuar el loop aunque haya error
                # El siguiente ciclo intentará nuevamente
        
        logger.debug("Thread de validación periódica detenido")
    
    def start_periodic_validation(self):
        """
        Inicia el thread de validación periódica en background.
        
        Este thread valida la sesión cada VALIDATION_INTERVAL segundos
        y la refresca automáticamente si está caducada.
        """
        if self._validation_thread is not None and self._validation_thread.is_alive():
            logger.warning("Thread de validación ya está corriendo")
            return
        
        # Detener cualquier thread anterior
        self.stop_periodic_validation()
        
        # Crear y empezar nuevo thread
        self._stop_validation.clear()
        self._validation_thread = threading.Thread(
            target=self._periodic_validation,
            name="PanAccessValidationThread",
            daemon=True  # Thread daemon se detiene cuando el proceso principal termina
        )
        self._validation_thread.start()
        logger.debug("Thread de validación periódica iniciado")
    
    def stop_periodic_validation(self):
        """
        Detiene el thread de validación periódica.
        """
        if self._validation_thread is not None and self._validation_thread.is_alive():
            logger.debug("Deteniendo thread de validación periódica...")
            self._stop_validation.set()
            self._validation_thread.join(timeout=5)  # Esperar máximo 5 segundos
            if self._validation_thread.is_alive():
                logger.warning("Thread de validación no se detuvo en 5 segundos")
            else:
                logger.debug("Thread de validación detenido correctamente")
            self._validation_thread = None


# Instancia global del singleton
_panaccess_singleton: Optional[PanAccessSingleton] = None


def get_panaccess() -> PanAccessSingleton:
    """
    Obtiene la instancia singleton de PanAccess.
    
    Returns:
        Instancia de PanAccessSingleton
    """
    global _panaccess_singleton
    if _panaccess_singleton is None:
        _panaccess_singleton = PanAccessSingleton()
    return _panaccess_singleton


def initialize_panaccess():
    """
    Inicializa el singleton, realiza el primer login y inicia la validación periódica.
    
    Esta función debe llamarse al arrancar Django (en AppConfig.ready()).
    
    Flujo:
    1. Obtiene el singleton
    2. Hace login inicial
    3. Inicia thread de validación periódica en background
    """
    singleton = get_panaccess()
    try:
        # 1. Login inicial
        singleton.ensure_session()
        logger.info("PanAccess inicializado y autenticado")
        
        # 2. Iniciar validación periódica en background
        singleton.start_periodic_validation()
        logger.debug("Validación periódica iniciada")
        
    except PanAccessException as e:
        logger.error(f"❌ Error al inicializar PanAccess: {str(e)}")
        # No lanzamos excepción para que Django pueda arrancar
        # El sistema intentará autenticarse en el primer request
        logger.warning("El sistema intentará autenticarse en el primer request")
        
        # Intentar iniciar validación periódica de todas formas
        # (puede que el login falle pero la validación periódica lo intente después)
        try:
            singleton.start_periodic_validation()
        except Exception as ve:
            logger.error(f"❌ Error al iniciar validación periódica: {str(ve)}")

