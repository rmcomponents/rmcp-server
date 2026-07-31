"""Tests for alphaplan_mcp.tools.mouser.search_mouser_parts."""
from __future__ import annotations

import asyncio
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
        
        result = await search_mouser_parts(["ABC123"])
        
        # Verify API was called
        mock_api.assert_called_once_with("ABC123")
        
        # Verify cache was not used
        mock_cache.get.assert_called_once()
        mock_cache.set.assert_called_once()
        
        # Verify extraction
        mock_extract.assert_called_once_with(FIXTURE)
        
        # Verify result structure
        assert "ABC123" in result["results"]
        res = result["results"]["ABC123"]
        assert res["part_count"] == len(FIXTURE.get("SearchResults", {}).get("Parts", []))
        assert res["parts"] == mock_extract.return_value
        
        metadata = res["metadata"]
        assert metadata["caching_enabled"] is True
        assert metadata["cached"] is False
        assert metadata["cache_age_hours"] is None
        assert metadata["cache_ttl_hours"] == 168.0
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
        
        result = await search_mouser_parts(["ABC123"], cache_ttl_override_hours=4.0)
        
        res = result["results"]["ABC123"]
        assert res["metadata"]["cache_ttl_hours"] == 4.0


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
        
        result = await search_mouser_parts(["ABC123"], caching=False)
        
        # Cache should not be called
        mock_cache.get.assert_not_called()
        mock_cache.set.assert_not_called()
        
        # API should still be called
        mock_api.assert_called_once()
        
        res = result["results"]["ABC123"]
        metadata = res["metadata"]
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
        
        result = await search_mouser_parts(["ABC123"])
        
        # API should NOT be called
        mock_api.assert_not_called()
        
        # Cache was used
        mock_cache.get.assert_called_once()
        
        # Extraction should use cached data
        mock_extract.assert_not_called()
        
        res = result["results"]["ABC123"]
        metadata = res["metadata"]
        assert metadata["cached"] is True
        assert metadata["cache_age_hours"] == 2.0
        assert metadata["source"] == "cache"


@pytest.mark.asyncio
async def test_search_mouser_parts_invalid_mpns_empty_list():
    """Empty MPNs list returns ToolError dict."""
    result = await search_mouser_parts([])
    
    assert "error" in result
    assert result["error"] == "INVALID_MPNS"


@pytest.mark.asyncio
async def test_search_mouser_parts_invalid_mpn():
    """Empty MPN returns ToolError dict."""
    result = await search_mouser_parts([""])
    
    assert "" in result["results"]
    assert result["results"][""]["error"] == "INVALID_MPN"


@pytest.mark.asyncio
async def test_search_mouser_parts_invalid_cache_ttl():
    """Invalid TTL returns ToolError dict."""
    result = await search_mouser_parts(["ABC123"], cache_ttl_override_hours=0)
    
    assert "error" in result
    assert result["error"] == "INVALID_CACHE_TTL"


@pytest.mark.asyncio
async def test_search_mouser_parts_too_many_mpns():
    result = await search_mouser_parts(["A", "B", "C", "D", "E", "F"])
    assert result["error"] == "TOO_MANY_MPNS"


@pytest.mark.asyncio
async def test_search_mouser_parts_mixed_cache_hits():
    """3 cached, 2 uncached -> only 2 API calls."""
    from alphaplan_mcp.apis.mouser.cache import CachedEntry, build_cache_key

    now = datetime.now(timezone.utc)
    entries = {
        build_cache_key(m): CachedEntry(payload=[{"manufacturer_part_number": m}], stored_at=now)
        for m in ["A", "B", "C"]
    }

    async def fake_get(key):
        return entries.get(key)

    with patch("alphaplan_mcp.tools.mouser.search_by_part_number", new_callable=AsyncMock) as mock_api, \
         patch("alphaplan_mcp.tools.mouser.default_cache") as mock_cache, \
         patch("alphaplan_mcp.tools.mouser.extract_purchasing_parts") as mock_extract:

        mock_api.return_value = FIXTURE
        mock_cache.get = AsyncMock(side_effect=fake_get)
        mock_cache.set = AsyncMock()
        mock_extract.return_value = []

        result = await search_mouser_parts(["A", "B", "C", "D", "E"])

        assert mock_api.call_count == 2
        assert {c.args[0] for c in mock_api.call_args_list} == {"D", "E"}
        assert mock_cache.set.await_count == 2

        results = result["results"]
        assert set(results) == {"A", "B", "C", "D", "E"}
        assert all(results[m]["metadata"]["cached"] for m in ["A", "B", "C"])
        assert all(not results[m]["metadata"]["cached"] for m in ["D", "E"])
        assert all(results[m]["metadata"]["source"] == "cache" for m in ["A", "B", "C"])
        assert all(results[m]["metadata"]["source"] == "mouser_api" for m in ["D", "E"])


@pytest.mark.asyncio
async def test_search_mouser_parts_partial_failure():
    """One failing MPN does not fail the others."""
    from alphaplan_mcp.errors import tool_error

    async def fake_api(mpn):
        if mpn == "BAD":
            raise tool_error("MOUSER_TIMEOUT", "timed out")
        return FIXTURE

    with patch("alphaplan_mcp.tools.mouser.search_by_part_number", new_callable=AsyncMock) as mock_api, \
         patch("alphaplan_mcp.tools.mouser.default_cache") as mock_cache, \
         patch("alphaplan_mcp.tools.mouser.extract_purchasing_parts") as mock_extract:

        mock_api.side_effect = fake_api
        mock_cache.get = AsyncMock(return_value=None)
        mock_cache.set = AsyncMock()
        mock_extract.return_value = []

        result = await search_mouser_parts(["BAD", "GOOD"])

        assert result["results"]["BAD"]["error"] == "MOUSER_TIMEOUT"
        assert result["results"]["GOOD"]["part_count"] == 0


@pytest.mark.asyncio
async def test_search_mouser_parts_runs_in_parallel():
    """Sequential execution would deadlock the barrier and time out."""
    entered = 0
    both_entered = asyncio.Event()

    async def fake_api(mpn):
        nonlocal entered
        entered += 1
        if entered == 2:
            both_entered.set()
        await asyncio.wait_for(both_entered.wait(), timeout=1)
        return FIXTURE

    with patch("alphaplan_mcp.tools.mouser.search_by_part_number", new_callable=AsyncMock) as mock_api, \
         patch("alphaplan_mcp.tools.mouser.default_cache") as mock_cache, \
         patch("alphaplan_mcp.tools.mouser.extract_purchasing_parts") as mock_extract:

        mock_api.side_effect = fake_api
        mock_cache.get = AsyncMock(return_value=None)
        mock_cache.set = AsyncMock()
        mock_extract.return_value = []

        result = await search_mouser_parts(["A", "B"])
        assert set(result["results"]) == {"A", "B"}
