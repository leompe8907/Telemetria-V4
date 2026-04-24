import os
from dotenv import load_dotenv

load_dotenv()

def _csv(name: str):
    raw = os.getenv(name)
    return raw.split(',') if raw else []

def _bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default

def _first_env(*names: str):
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return v
    return None


class ServidorConfig:
    SECRET_KEY = os.getenv('SECRET_KEY')
    DEBUG = _bool('DEBUG', default=False)
    ALLOWED_HOSTS = _csv('ALLOWED_HOSTS')
    SALT = os.getenv('SALT')
    @classmethod
    def configure(cls):
        missing = []
        if not cls.SECRET_KEY:
            missing.append('SECRET_KEY')
        # DEBUG puede ser False explícitamente; no se considera "missing"
        if not cls.ALLOWED_HOSTS:
            missing.append('ALLOWED_HOSTS')
        if not cls.SALT:
            missing.append('SALT')
        if missing:
            raise ValueError(f'Missing environment variables: {", ".join(missing)}')
        return cls
    

class DatabaseConfig:
    # Soportar nombres actuales y nombres más explícitos (preferibles en prod).
    ENGINE = _first_env('DB_ENGINE', 'ENGINE')
    NAME = _first_env('DB_NAME', 'NAME')
    USER = _first_env('DB_USER', 'USER')
    PASSWORD = _first_env('DB_PASSWORD', 'PASSWORD')
    HOST = _first_env('DB_HOST', 'HOST')
    PORT = _first_env('DB_PORT', 'PORT')

    @classmethod
    def configure(cls):
        missing = []
        if not cls.ENGINE:
            missing.append('ENGINE')
        if not cls.NAME:
            missing.append('NAME')
        if not cls.USER:
            missing.append('USER')
        if not cls.PASSWORD:
            missing.append('PASSWORD')
        if not cls.HOST:
            missing.append('HOST')
        if not cls.PORT:
            missing.append('PORT')
        if missing:
            raise ValueError(f'Missing environment variables: {", ".join(missing)}')
        return cls
    

class PanaccessConfigDelancer:
    DRM = os.getenv('drm')
    USERNAME = os.getenv('username')
    PASSWORD = os.getenv('password')
    API_TOKEN = os.getenv('api_token')

    @classmethod
    def configure(cls):
        missing = []
        if not cls.DRM:
            missing.append('drm')
        if not cls.USERNAME:
            missing.append('username')
        if not cls.PASSWORD:
            missing.append('password')
        if not cls.API_TOKEN:
            missing.append('api_token')
        if missing:
            raise ValueError(f'Missing environment variables: {", ".join(missing)}')
        return cls

class CORSConfig:
    ALLOW_ALL_ORIGINS = _bool('CORS_ALLOW_ALL_ORIGINS', default=False)
    ALLOWED_ORIGINS = _csv('CORS_ALLOWED_ORIGINS')
    ALLOW_CREDENTIALS = _bool('CORS_ALLOW_CREDENTIALS', default=False)
    TRUSTED_ORIGINS = _csv('CSRF_TRUSTED_ORIGINS')

    @classmethod
    def configure(cls):
        missing = []
        if not cls.ALLOW_ALL_ORIGINS:
            missing.append('CORS_ALLOW_ALL_ORIGINS')
        if not cls.ALLOWED_ORIGINS:
            missing.append('CORS_ALLOWED_ORIGINS')
        if not cls.ALLOW_CREDENTIALS:
            missing.append('CORS_ALLOW_CREDENTIALS')
        if not cls.TRUSTED_ORIGINS:
            missing.append('CSRF_TRUSTED_ORIGINS')
        if missing:
            raise ValueError(f'Missing environment variables: {", ".join(missing)}')
        return cls