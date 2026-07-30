from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from ...config import settings


@dataclass(frozen=True)
class CachedEntry:
    payload: Any
    stored_at: datetime  # timezone-aware UTC


class MouserCache(Protocol):
    async def get(self, key: str) -> CachedEntry | None: ...
    async def set(self, key: str, payload: Any, ttl_seconds: float) -> None: ...


class NullMouserCache:
    """No-op cache used until the Redis implementation lands."""
    async def get(self, key: str) -> CachedEntry | None:
        return None
    
    async def set(self, key: str, payload: Any, ttl_seconds: float) -> None:
        return None


def build_cache_key(mpn: str) -> str:
    """Build a normalized cache key for a part number."""
    normalized = re.sub(r"\s+", "", mpn).upper()
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
