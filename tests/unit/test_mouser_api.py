"""Tests for alphaplan_mcp.apis.mouser.api."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

from alphaplan_mcp.apis.mouser.api import search_by_part_number
from alphaplan_mcp.errors import ToolError

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
    monkeypatch.setenv("MOUSER_API_KEY", "")
    
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
