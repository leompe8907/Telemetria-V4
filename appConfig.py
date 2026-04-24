import os
from dotenv import load_dotenv

load_dotenv()

def _csv(name: str):
    raw = os.getenv(name)
    return raw.split(',') if raw else []


class ServidorConfig:
    SECRET_KEY = os.getenv('SECRET_KEY')
    DEBUG = os.getenv('DEBUG')
    ALLOWED_HOSTS = _csv('ALLOWED_HOSTS')
    SALT = os.getenv('SALT')
    @classmethod
    def configure(cls):
        missing = []
        if not cls.SECRET_KEY:
            missing.append('SECRET_KEY')
        if not cls.DEBUG:
            missing.append('DEBUG')
        if not cls.ALLOWED_HOSTS:
            missing.append('ALLOWED_HOSTS')
        if not cls.SALT:
            missing.append('SALT')
        if missing:
            raise ValueError(f'Missing environment variables: {", ".join(missing)}')
        return cls
    

class DatabaseConfig:
    ENGINE = os.getenv('ENGINE')
    NAME = os.getenv('NAME')
    USER = os.getenv('USER')
    PASSWORD = os.getenv('PASSWORD')
    HOST = os.getenv('HOST')
    PORT = os.getenv('PORT')

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