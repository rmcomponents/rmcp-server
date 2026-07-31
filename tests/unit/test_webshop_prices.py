import pytest
import pyarrow as pa
from unittest.mock import patch
from alphaplan_mcp.tools.webshop import get_webshop_prices
from alphaplan_mcp.errors import ToolError


@pytest.mark.asyncio
async def test_get_webshop_prices_success():
    batch = pa.RecordBatch.from_pylist([
        {
            "mpn": "ABC123",
            "quantity_breakpoint": 1,
            "unit_price": 0.25,
            "updated_at": "2024-01-01 12:00:00",
        },
        {
            "mpn": "ABC123",
            "quantity_breakpoint": 100,
            "unit_price": 0.15,
            "updated_at": "2024-01-01 12:00:00",
        },
    ])

    async def fake_stream(*args, **kwargs):
        yield batch

    with patch("alphaplan_mcp.tools.webshop.fetch_webshop_prices_stream", side_effect=fake_stream):
        result = await get_webshop_prices(["ABC123"])
        assert "mpn,quantity_breakpoint,unit_price,updated_at" in result
        assert "ABC123,1,0.25" in result
        assert "ABC123,100,0.15" in result


@pytest.mark.asyncio
async def test_get_webshop_prices_empty():
    async def fake_stream(*args, **kwargs):
        if False:
            yield

    with patch("alphaplan_mcp.tools.webshop.fetch_webshop_prices_stream", side_effect=fake_stream):
        result = await get_webshop_prices(["NOPE"])
        assert result == "mpn,quantity_breakpoint,unit_price,updated_at\n"


@pytest.mark.asyncio
async def test_get_webshop_prices_missing_mpn():
    with pytest.raises(ToolError):
        await get_webshop_prices([])


@pytest.mark.asyncio
async def test_get_webshop_prices_whitespace_mpn():
    with pytest.raises(ToolError):
        await get_webshop_prices(["   "])


@pytest.mark.asyncio
async def test_get_webshop_prices_multiple_mpns():
    batch = pa.RecordBatch.from_pylist([
        {"mpn": "ABC123", "quantity_breakpoint": 1, "unit_price": 0.25, "updated_at": "2024-01-01 12:00:00"},
        {"mpn": "DEF456", "quantity_breakpoint": 1, "unit_price": 0.50, "updated_at": "2024-01-01 12:00:00"},
    ])

    captured = {}

    async def fake_stream(**kwargs):
        captured["mpns"] = kwargs.get("mpns")
        yield batch

    with patch("alphaplan_mcp.tools.webshop.fetch_webshop_prices_stream", side_effect=fake_stream):
        result = await get_webshop_prices(["ABC123", "DEF456"])
        assert captured["mpns"] == ["ABC123", "DEF456"]
        assert "ABC123,1,0.25" in result
        assert "DEF456,1,0.5" in result or "DEF456,1,0.50" in result


@pytest.mark.asyncio
async def test_get_webshop_prices_too_many_mpns():
    with pytest.raises(ToolError):
        await get_webshop_prices([f"MPN{i}" for i in range(11)])


@pytest.mark.asyncio
async def test_get_webshop_prices_blank_mpn():
    with pytest.raises(ToolError):
        await get_webshop_prices(["ABC123", "   "])
