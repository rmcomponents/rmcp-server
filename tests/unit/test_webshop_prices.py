import base64
import hashlib
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
        csv_data = result["csv"]

        assert result["sha256_base64"] == base64.b64encode(
            hashlib.sha256(csv_data.encode("utf-8")).digest()
        ).decode("ascii")
        assert csv_data.endswith("\n")

        lines = csv_data.strip().split("\n")
        assert lines[0] == "MPN,breakpoint_min,breakpoint_max,unit_price_min,unit_price_max,updated_at,error"
        assert len(lines) == 2
        row = lines[1].split(",")
        assert row[0] == "ABC123"
        assert row[1] == "1"
        assert row[2] == "100"
        assert row[3] == "0.15"
        assert row[4] == "0.25"
        # updated_at is current time, so just check it's non-empty
        assert row[5] != ""
        assert row[6] == ""


@pytest.mark.asyncio
async def test_get_webshop_prices_empty():
    async def fake_stream(*args, **kwargs):
        if False:
            yield

    with patch("alphaplan_mcp.tools.webshop.fetch_webshop_prices_stream", side_effect=fake_stream):
        result = await get_webshop_prices(["NOPE"])
        assert result["csv"] == "MPN,breakpoint_min,breakpoint_max,unit_price_min,unit_price_max,updated_at,error\n"
        assert result["sha256_base64"] == base64.b64encode(
            hashlib.sha256(result["csv"].encode("utf-8")).digest()
        ).decode("ascii")


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
        csv_data = result["csv"]

        assert result["sha256_base64"] == base64.b64encode(
            hashlib.sha256(csv_data.encode("utf-8")).digest()
        ).decode("ascii")
        assert csv_data.endswith("\n")

        lines = csv_data.strip().split("\n")
        assert len(lines) == 3  # header + 2 rows
        assert captured["mpns"] == ["ABC123", "DEF456"]
        # Check both rows exist
        assert any(line.startswith("ABC123,") for line in lines[1:])
        assert any(line.startswith("DEF456,") for line in lines[1:])


@pytest.mark.asyncio
async def test_get_webshop_prices_too_many_mpns():
    with pytest.raises(ToolError):
        await get_webshop_prices([f"MPN{i}" for i in range(26)])


@pytest.mark.asyncio
async def test_get_webshop_prices_blank_mpn():
    with pytest.raises(ToolError):
        await get_webshop_prices(["ABC123", "   "])


@pytest.mark.asyncio
async def test_get_webshop_prices_json_output():
    batch = pa.RecordBatch.from_pylist([
        {"mpn": "ABC123", "quantity_breakpoint": 1, "unit_price": 0.25, "updated_at": "2024-01-01 12:00:00"},
    ])

    async def fake_stream(*args, **kwargs):
        yield batch

    with patch("alphaplan_mcp.tools.webshop.fetch_webshop_prices_stream", side_effect=fake_stream):
        result = await get_webshop_prices(["ABC123"], output_format="json")
        assert "results" in result
        assert len(result["results"]) == 1
        row = result["results"][0]
        assert row["MPN"] == "ABC123"
        assert row["breakpoint_min"] == 1
        assert row["breakpoint_max"] == 1
        assert row["unit_price_min"] == 0.25
        assert row["unit_price_max"] == 0.25
        assert row["updated_at"] != ""
        assert row["error"] is None
