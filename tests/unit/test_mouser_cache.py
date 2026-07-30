"""Tests for alphaplan_mcp.apis.mouser.cache."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from alphaplan_mcp.apis.mouser.cache import (
    CachedEntry,
    NullMouserCache,
    RedisMouserCache,
    build_cache_key,
    cache_metadata,
    entry_age_hours,
)


@pytest.mark.asyncio
async def test_build_cache_key_normalization():
    """Cache key normalizes whitespace and case."""
    key1 = build_cache_key(" abc 123 ")
    key2 = build_cache_key("ABC123")
    
    assert key1 == key2
    assert key1.startswith("alphaplan-mcp:mouser:part:")


@pytest.mark.asyncio
async def test_null_mouser_cache_get():
    """NullMouserCache.get always returns None."""
    cache = NullMouserCache()
    result = await cache.get("any-key")
    assert result is None


@pytest.mark.asyncio
async def test_null_mouser_cache_set():
    """NullMouserCache.set is a no-op."""
    cache = NullMouserCache()
    # Should not raise
    await cache.set("any-key", {"data": "value"}, 3600)


def test_entry_age_hours():
    """entry_age_hours calculates age correctly."""
    from datetime import timedelta
    
    now = datetime.now(timezone.utc)
    entry = CachedEntry(payload={"data": "value"}, stored_at=now - timedelta(hours=1, minutes=30))
    
    age = entry_age_hours(entry, now=now)
    assert age == 1.5


def test_cache_metadata_miss():
    """cache_metadata with no entry."""
    
    metadata = cache_metadata(
        caching_enabled=True,
        ttl_hours=24.0,
        entry=None,
    )
    
    assert metadata["caching_enabled"] is True
    assert metadata["cached"] is False
    assert metadata["cache_age_hours"] is None
    assert metadata["cache_ttl_hours"] == 24.0


def test_cache_metadata_hit():
    """cache_metadata with entry."""
    from datetime import timedelta
    
    now = datetime.now(timezone.utc)
    entry = CachedEntry(payload={"data": "value"}, stored_at=now - timedelta(hours=2))
    
    metadata = cache_metadata(
        caching_enabled=False,
        ttl_hours=48.0,
        entry=entry,
    )
    
    assert metadata["caching_enabled"] is False
    assert metadata["cached"] is True
    assert metadata["cache_age_hours"] == 2.0
    assert metadata["cache_ttl_hours"] == 48.0
@pytest.mark.asyncio
async def test_redis_cache_roundtrip(fake_redis):
    cache = RedisMouserCache(fake_redis)
    await cache.set("k", [{"a": 1}], 3600)
    entry = await cache.get("k")
    assert entry is not None
    assert entry.payload == [{"a": 1}]
    assert entry.stored_at.tzinfo is not None  # tz-aware UTC

@pytest.mark.asyncio
async def test_redis_cache_miss_returns_none(fake_redis):
    assert await RedisMouserCache(fake_redis).get("missing") is None

@pytest.mark.asyncio
async def test_redis_cache_applies_ttl(fake_redis):
    cache = RedisMouserCache(fake_redis)
    await cache.set("k", [], 3600)
    ttl = await fake_redis.ttl("k")
    assert 0 < ttl <= 3600

@pytest.mark.asyncio
async def test_redis_cache_corrupt_entry_is_miss(fake_redis):
    await fake_redis.set("k", b"not-json")
    assert await RedisMouserCache(fake_redis).get("k") is None

@pytest.mark.asyncio
async def test_redis_cache_backend_failure_degrades_gracefully():
    redis = AsyncMock()
    redis.get.side_effect = ConnectionError("down")
    redis.set.side_effect = ConnectionError("down")
    cache = RedisMouserCache(redis)
    assert await cache.get("k") is None
    await cache.set("k", [], 60)  # must not raise
