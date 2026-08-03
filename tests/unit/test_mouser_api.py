"""Tests for alphaplan_mcp.apis.mouser.api."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from alphaplan_mcp.apis.mouser.api import search_by_part_number
from alphaplan_mcp.errors import ToolError

@pytest.fixture(autouse=True)
def mock_mouser_api_key(monkeypatch):
    monkeypatch.setattr("alphaplan_mcp.apis.mouser.api.settings.MOUSER_API_KEY", "test")

FIXTURE = json.loads(
    (Path(__file__).parents[2] / "src" / "alphaplan_mcp" / "apis" / "mouser" / "example-response.json").read_text()
)


def build_mock_handler(response=None, exc=None, status_code=200):
    """Build an httpx mock handler."""
    async def handler(request: httpx.Request):
        if exc:
            raise exc
        if response is not None:
            return httpx.Response(status_code=status_code, json=response)
        return httpx.Response(status_code=status_code)
    return handler


@pytest.mark.asyncio
async def test_search_by_part_number_success():
    """Success case: returns raw dict unchanged."""
    handler = build_mock_handler(response=FIXTURE)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    
    result = await search_by_part_number("ABC123", client=client)
    
    assert result == FIXTURE


@pytest.mark.asyncio
async def test_search_by_part_number_empty_mpn():
    """Empty or whitespace MPN raises INVALID_MPN."""
    with pytest.raises(ToolError) as exc_info:
        await search_by_part_number("")
    assert exc_info.value.code == "INVALID_MPN"
    
    with pytest.raises(ToolError) as exc_info:
        await search_by_part_number("   ")
    assert exc_info.value.code == "INVALID_MPN"


@pytest.mark.asyncio
async def test_search_by_part_number_no_api_key(monkeypatch):
    """Empty API key raises MOUSER_NOT_CONFIGURED."""
    monkeypatch.setattr("alphaplan_mcp.apis.mouser.api.settings.MOUSER_API_KEY", "")
    
    handler = build_mock_handler(response=FIXTURE)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    
    with pytest.raises(ToolError) as exc_info:
        await search_by_part_number("ABC123", client=client)
    assert exc_info.value.code == "MOUSER_NOT_CONFIGURED"


@pytest.mark.asyncio
async def test_search_by_part_number_auth_error():
    """HTTP 401/403 raises MOUSER_AUTH_ERROR."""
    handler = build_mock_handler(status_code=401)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    
    with pytest.raises(ToolError) as exc_info:
        await search_by_part_number("ABC123", client=client)
    assert exc_info.value.code == "MOUSER_AUTH_ERROR"


@pytest.mark.asyncio
async def test_search_by_part_number_rate_limit():
    """HTTP 429 raises MOUSER_RATE_LIMIT."""
    handler = build_mock_handler(status_code=429)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    
    with pytest.raises(ToolError) as exc_info:
        await search_by_part_number("ABC123", client=client)
    assert exc_info.value.code == "MOUSER_RATE_LIMIT"


@pytest.mark.asyncio
async def test_search_by_part_number_rate_limit_retry():
    """429 with retry_on_rate_limit=True retries once after 60s."""
    from alphaplan_mcp.config import settings

    calls = 0

    async def handler(request: httpx.Request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(status_code=429)
        return httpx.Response(status_code=200, json=FIXTURE)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    # Patch sleep to avoid real 60s wait
    with patch("alphaplan_mcp.apis.mouser.api.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        result = await search_by_part_number("ABC123", client=client, retry_on_rate_limit=True)

    assert calls == 2
    mock_sleep.assert_awaited_once_with(settings.MOUSER_RATE_LIMIT_RETRY_SECONDS)
    assert result == FIXTURE


@pytest.mark.asyncio
async def test_search_by_part_number_rate_limit_no_retry():
    """429 with retry_on_rate_limit=False raises immediately."""
    calls = 0

    async def handler(request: httpx.Request):
        nonlocal calls
        calls += 1
        return httpx.Response(status_code=429)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with pytest.raises(ToolError) as exc_info:
        await search_by_part_number("ABC123", client=client, retry_on_rate_limit=False)

    assert exc_info.value.code == "MOUSER_RATE_LIMIT"
    assert calls == 1


@pytest.mark.asyncio
async def test_search_by_part_number_api_error():
    """Other HTTP errors raise MOUSER_API_ERROR."""
    handler = build_mock_handler(status_code=500)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    
    with pytest.raises(ToolError) as exc_info:
        await search_by_part_number("ABC123", client=client)
    assert exc_info.value.code == "MOUSER_API_ERROR"
    assert exc_info.value.details["status_code"] == 500


@pytest.mark.asyncio
async def test_search_by_part_number_timeout():
    """Timeout raises MOUSER_TIMEOUT."""
    exc = httpx.TimeoutException("timeout", request=httpx.Request("POST", "http://test"))
    handler = build_mock_handler(exc=exc)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    
    with pytest.raises(ToolError) as exc_info:
        await search_by_part_number("ABC123", client=client)
    assert exc_info.value.code == "MOUSER_TIMEOUT"


@pytest.mark.asyncio
async def test_search_by_part_number_http_error():
    """Transport HTTP errors raise MOUSER_HTTP_ERROR."""
    exc = httpx.ConnectError("connection failed", request=httpx.Request("POST", "http://test"))
    handler = build_mock_handler(exc=exc)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    
    with pytest.raises(ToolError) as exc_info:
        await search_by_part_number("ABC123", client=client)
    assert exc_info.value.code == "MOUSER_HTTP_ERROR"


@pytest.mark.asyncio
async def test_search_by_part_number_api_errors_in_response():
    """Response with non-empty Errors list raises MOUSER_API_ERROR."""
    response_with_errors = {
        **FIXTURE,
        "Errors": [{"message": "Part not found"}],
    }
    handler = build_mock_handler(response=response_with_errors)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    
    with pytest.raises(ToolError) as exc_info:
        await search_by_part_number("ABC123", client=client)
    assert exc_info.value.code == "MOUSER_API_ERROR"
    assert exc_info.value.details["errors"] == [{"message": "Part not found"}]


@pytest.mark.asyncio
async def test_search_by_part_number_invalid_json():
    """Invalid JSON body raises MOUSER_BAD_RESPONSE."""
    async def handler(request: httpx.Request):
        return httpx.Response(status_code=200, text="not json")
    
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    
    with pytest.raises(ToolError) as exc_info:
        await search_by_part_number("ABC123", client=client)
    assert exc_info.value.code == "MOUSER_BAD_RESPONSE"


@pytest.mark.asyncio
async def test_search_by_part_number_non_dict_response():
    """Non-dict JSON response raises MOUSER_BAD_RESPONSE."""
    handler = build_mock_handler(response=["not", "a", "dict"])
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    
    with pytest.raises(ToolError) as exc_info:
        await search_by_part_number("ABC123", client=client)
    assert exc_info.value.code == "MOUSER_BAD_RESPONSE"
