"""Tests for alphaplan_mcp.apis.mouser.cache."""
from __future__ import annotations

import pytest

from alphaplan_mcp.apis.mouser.cache import (
    NullMouserCache,
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
    
    from alphaplan_mcp.apis.mouser.cache import CachedEntry
    
    now = pytest.importorskip("datetime").datetime.now(pytest.importorskip("datetime").timezone.utc)
    entry = CachedEntry(payload={"data": "value"}, stored_at=now - timedelta(hours=1, minutes=30))
    
    age = entry_age_hours(entry, now=now)
    assert age == 1.5


def test_cache_metadata_miss():
    """cache_metadata with no entry."""
    from alphaplan_mcp.apis.mouser.cache import cache_metadata
    
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
    
    from alphaplan_mcp.apis.mouser.cache import CachedEntry, cache_metadata
    
    now = pytest.importorskip("datetime").datetime.now(pytest.importorskip("datetime").timezone.utc)
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
