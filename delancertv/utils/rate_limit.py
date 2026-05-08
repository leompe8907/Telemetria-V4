from __future__ import annotations

from dataclasses import dataclass

from django.core.cache import cache


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    retry_after_seconds: int


def acquire_rate_limit(name: str, ttl_seconds: int) -> RateLimitResult:
    """
    Rate limiting simple (global) por nombre usando cache.
    - Devuelve allowed=True si adquiere el lock
    - Si no, allowed=False y retry_after_seconds=ttl_seconds (aproximado)
    """
    ttl = max(1, int(ttl_seconds))
    key = f"telemetria:ratelimit:{name}"
    allowed = False
    try:
        allowed = bool(cache.add(key, "1", timeout=ttl))
    except Exception:
        # si el cache falla, no bloquear operación
        allowed = True
    return RateLimitResult(allowed=allowed, retry_after_seconds=ttl if not allowed else 0)

