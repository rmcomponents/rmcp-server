"""Tests for alphaplan_mcp.tools.mouser.search_mouser_parts."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from alphaplan_mcp.tools.mouser import search_mouser_parts

FIXTURE = json.loads(
    (Path(__file__).parents[2] / "src" / "alphaplan_mcp" / "apis" / "mouser" / "example-response.json").read_text()
)


@pytest.mark.asyncio
async def test_search_mouser_parts_happy_path():
    """Happy path: calls API, extracts parts, returns metadata."""
    from alphaplan_mcp.apis.mouser.cache import CachedEntry
    
    # Mock the API and cache
    with patch("alphaplan_mcp.tools.mouser.search_by_part_number", new_callable=AsyncMock) as mock_api, \
         patch("alphaplan_mcp.tools.mouser.default_cache") as mock_cache, \
         patch("alphaplan_mcp.tools.mouser.extract_purchasing_parts") as mock_extract:
        
        mock_api.return_value = FIXTURE
        mock_cache.get = AsyncMock(return_value=None)
        mock_cache.set = AsyncMock()
        mock_extract.return_value = FIXTURE.get("SearchResults", {}).get("Parts", [])
        
        result = await search_mouser_parts("ABC123")
        
        # Verify API was called
        mock_api.assert_called_once_with("ABC123")
        
        # Verify cache was not used
        mock_cache.get.assert_called_once()
        mock_cache.set.assert_called_once()
        
        # Verify extraction
        mock_extract.assert_called_once_with(FIXTURE)
        
        # Verify result structure
        assert result["query"]["mpn"] == "ABC123"
        assert result["part_count"] == len(FIXTURE.get("SearchResults", {}).get("Parts", []))
        assert result["parts"] == mock_extract.return_value
        
        metadata = result["metadata"]
        assert metadata["caching_enabled"] is True
        assert metadata["cached"] is False
        assert metadata["cache_age_hours"] is None
        assert metadata["cache_ttl_hours"] == 24.0
        assert metadata["source"] == "mouser_api"
        datetime.fromisoformat(metadata["fetched_at"])


@pytest.mark.asyncio
async def test_search_mouser_parts_custom_ttl():
    """Custom TTL is reflected in metadata."""
    with patch("alphaplan_mcp.tools.mouser.search_by_part_number", new_callable=AsyncMock) as mock_api, \
         patch("alphaplan_mcp.tools.mouser.default_cache") as mock_cache, \
         patch("alphaplan_mcp.tools.mouser.extract_purchasing_parts") as mock_extract:
        
        mock_api.return_value = FIXTURE
        mock_cache.get = AsyncMock(return_value=None)
        mock_cache.set = AsyncMock()
        mock_extract.return_value = []
        
        result = await search_mouser_parts("ABC123", cache_ttl_override_hours=4.0)
        
        assert result["metadata"]["cache_ttl_hours"] == 4.0


@pytest.mark.asyncio
async def test_search_mouser_parts_caching_disabled():
    """caching=False skips cache operations."""
    with patch("alphaplan_mcp.tools.mouser.search_by_part_number", new_callable=AsyncMock) as mock_api, \
         patch("alphaplan_mcp.tools.mouser.default_cache") as mock_cache, \
         patch("alphaplan_mcp.tools.mouser.extract_purchasing_parts") as mock_extract:
        
        mock_api.return_value = FIXTURE
        mock_cache.get = AsyncMock(return_value=None)
        mock_cache.set = AsyncMock()
        mock_extract.return_value = []
        
        result = await search_mouser_parts("ABC123", caching=False)
        
        # Cache should not be called
        mock_cache.get.assert_not_called()
        mock_cache.set.assert_not_called()
        
        # API should still be called
        mock_api.assert_called_once()
        
        metadata = result["metadata"]
        assert metadata["caching_enabled"] is False
        assert metadata["cached"] is False


@pytest.mark.asyncio
async def test_search_mouser_parts_cache_hit():
    """Cache hit skips API call and uses cached data."""
    from alphaplan_mcp.apis.mouser.cache import CachedEntry
    
    cached_parts = [{"manufacturer": "Test", "manufacturer_part_number": "ABC123"}]
    now = datetime.now(timezone.utc)
    entry = CachedEntry(payload=cached_parts, stored_at=now - timedelta(hours=2))
    
    with patch("alphaplan_mcp.tools.mouser.search_by_part_number") as mock_api, \
         patch("alphaplan_mcp.tools.mouser.default_cache") as mock_cache, \
         patch("alphaplan_mcp.tools.mouser.extract_purchasing_parts") as mock_extract:
        
        mock_cache.get = AsyncMock(return_value=entry)
        mock_cache.set = AsyncMock()
        mock_extract.return_value = cached_parts
        
        result = await search_mouser_parts("ABC123")
        
        # API should NOT be called
        mock_api.assert_not_called()
        
        # Cache was used
        mock_cache.get.assert_called_once()
        
        # Extraction should use cached data
        mock_extract.assert_not_called()
        
        metadata = result["metadata"]
        assert metadata["cached"] is True
        assert metadata["cache_age_hours"] == 2.0
        assert metadata["source"] == "cache"


@pytest.mark.asyncio
async def test_search_mouser_parts_invalid_mpn():
    """Empty MPN returns ToolError dict."""
    result = await search_mouser_parts("")
    
    assert "error" in result
    assert result["error"] == "INVALID_MPN"


@pytest.mark.asyncio
async def test_search_mouser_parts_invalid_cache_ttl():
    """Invalid TTL returns ToolError dict."""
    result = await search_mouser_parts("ABC123", cache_ttl_override_hours=0)
    
    assert "error" in result
    assert result["error"] == "INVALID_CACHE_TTL"
