from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from redis.asyncio import Redis

from ...config import settings
from ...utils import normalize_mpn

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CachedEntry:
    payload: Any
    stored_at: datetime  # timezone-aware UTC


class MouserCache(Protocol):
    async def get(self, key: str) -> CachedEntry | None: ...
    async def set(self, key: str, payload: Any, ttl_seconds: float) -> None: ...


class RedisMouserCache:
    """Redis-backed MouserCache.

    Each entry is stored as a JSON envelope:
    {"stored_at": <ISO-8601 UTC>, "payload": <extracted parts list>}
    Expiry is delegated to Redis via SET ... EX, so TTL survives restarts
    (given persistence is enabled in redis.conf).
    """

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def get(self, key: str) -> CachedEntry | None:
        try:
            raw = await self._redis.get(key)
        except Exception as exc:
            # Cache is best-effort: degrade to a miss, never fail the tool.
            logger.warning("Mouser cache GET failed (key=%s): %s", key, exc)
            return None
        if raw is None:
            return None
        try:
            envelope = json.loads(raw)  # redis returns bytes; json.loads handles them
            return CachedEntry(
                payload=envelope["payload"],
                stored_at=datetime.fromisoformat(envelope["stored_at"]),
            )
        except (TypeError, ValueError, KeyError) as exc:
            logger.warning("Corrupt mouser cache entry (key=%s): %s", key, exc)
            return None

    async def set(self, key: str, payload: Any, ttl_seconds: float) -> None:
        envelope = json.dumps({
            "stored_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        })
        try:
            await self._redis.set(key, envelope, ex=max(1, int(ttl_seconds)))
        except Exception as exc:
            logger.warning("Mouser cache SET failed (key=%s): %s", key, exc)


class NullMouserCache:
    """No-op cache used until the Redis implementation lands."""
    async def get(self, key: str) -> CachedEntry | None:
        return None
    
    async def set(self, key: str, payload: Any, ttl_seconds: float) -> None:
        return None


def build_cache_key(mpn: str) -> str:
    """Build a normalized cache key for a part number."""
    normalized = normalize_mpn(mpn)
    return f"{settings.REDIS_KEY_PREFIX}:mouser:part:{normalized}"


def entry_age_hours(entry: CachedEntry, now: datetime | None = None) -> float:
    """Calculate the age of a cache entry in hours."""
    if now is None:
        now = datetime.now(timezone.utc)
    age_seconds = (now - entry.stored_at).total_seconds()
    return round(age_seconds / 3600, 3)


def cache_metadata(
    *,
    caching_enabled: bool,
    ttl_hours: float,
    entry: CachedEntry | None,
) -> dict[str, Any]:
    """Build the cache metadata dictionary."""
    return {
        "caching_enabled": caching_enabled,
        "cached": entry is not None,
        "cache_age_hours": entry_age_hours(entry) if entry is not None else None,
        "cache_ttl_hours": ttl_hours,
    }


# Replace with the Redis-backed implementation later; this is the only
# place the tool's cache backend is chosen.
default_cache: MouserCache = NullMouserCache()
