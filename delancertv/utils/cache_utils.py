"""
Utilidades de cache para analytics del dashboard.

- cached_result(): cachea el resultado de funciones puras (dict/list/str/números).
- Genera keys hash a partir de args/kwargs para evitar claves largas.
- Incluye mitigación simple de "stampede" con un lock en cache (best-effort).
"""

from __future__ import annotations

from functools import wraps
import hashlib
import json
import logging
import time
from typing import Any, Callable, TypeVar, ParamSpec, cast

from django.core.cache import cache
from django.conf import settings

logger = logging.getLogger(__name__)

T = TypeVar("T")
P = ParamSpec("P")


def cache_key_from_params(prefix: str, *args: Any, **kwargs: Any) -> str:
    parts: list[str] = [prefix]

    for arg in args:
        if arg is None or isinstance(arg, (str, int, float, bool)):
            parts.append(str(arg))
        else:
            try:
                parts.append(json.dumps(arg, sort_keys=True, default=str))
            except (TypeError, ValueError):
                parts.append(str(hash(str(arg))))

    for k, v in sorted(kwargs.items()):
        if v is None or isinstance(v, (str, int, float, bool)):
            parts.append(f"{k}:{v}")
        else:
            try:
                parts.append(f"{k}:{json.dumps(v, sort_keys=True, default=str)}")
            except (TypeError, ValueError):
                parts.append(f"{k}:{str(hash(str(v)))}")

    raw = "|".join(parts)
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()
    return f"telemetria:{prefix}:{digest}"


def cached_result(
    timeout: int | None = None,
    key_prefix: str | None = None,
    lock_timeout: int = 30,
    lock_wait_ms: int = 150,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """
    Cachea el resultado de una función.

    Anti-stampede (best-effort):
    - usa cache.add(lock_key) como lock temporal
    - si no puede lockear, espera brevemente y reintenta obtener el cache
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        prefix = key_prefix or func.__name__

        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            cache_key = cache_key_from_params(prefix, *args, **kwargs)
            cached = cache.get(cache_key)
            if cached is not None:
                return cast(T, cached)

            ttl = int(timeout or getattr(settings, "CACHE_TIMEOUT_ANALYTICS", 300))
            lock_key = f"{cache_key}:lock"

            have_lock = False
            try:
                have_lock = bool(cache.add(lock_key, "1", timeout=lock_timeout))
            except Exception:
                have_lock = False

            if not have_lock:
                # Espera corta y re-check (reduce stampede bajo carga)
                time.sleep(max(0, lock_wait_ms) / 1000.0)
                cached2 = cache.get(cache_key)
                if cached2 is not None:
                    return cast(T, cached2)

            result = func(*args, **kwargs)

            try:
                cache.set(cache_key, result, timeout=ttl)
            except Exception as e:
                logger.warning(f"No se pudo guardar cache ({cache_key}): {e}")
            finally:
                if have_lock:
                    try:
                        cache.delete(lock_key)
                    except Exception:
                        pass

            return result

        return wrapper

    return decorator

