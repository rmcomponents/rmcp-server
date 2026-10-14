import base64
import hashlib
import pytest
import pyarrow as pa
from unittest.mock import patch
from alphaplan_mcp.tools.webshop import get_webshop_preise
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
        result = await get_webshop_preise(["ABC123"])
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
        assert row[5] == "2024-01-01"  # date part of the price rows' updated_at
        assert row[6] == ""


@pytest.mark.asyncio
async def test_get_webshop_prices_empty():
    async def fake_stream(*args, **kwargs):
        if False:
            yield

    with patch("alphaplan_mcp.tools.webshop.fetch_webshop_prices_stream", side_effect=fake_stream):
        result = await get_webshop_preise(["NOPE"])
        assert result["csv"] == (
            "MPN,breakpoint_min,breakpoint_max,unit_price_min,unit_price_max,updated_at,error\n"
            "NOPE,,,,,,no webshop prices found\n"
        )
        assert result["sha256_base64"] == base64.b64encode(
            hashlib.sha256(result["csv"].encode("utf-8")).digest()
        ).decode("ascii")


@pytest.mark.asyncio
async def test_get_webshop_prices_missing_mpn():
    with pytest.raises(ToolError):
        await get_webshop_preise([])


@pytest.mark.asyncio
async def test_get_webshop_prices_whitespace_mpn():
    with pytest.raises(ToolError):
        await get_webshop_preise(["   "])


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
        result = await get_webshop_preise(["ABC123", "DEF456"])
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
        await get_webshop_preise([f"MPN{i}" for i in range(26)])


@pytest.mark.asyncio
async def test_get_webshop_prices_blank_mpn():
    with pytest.raises(ToolError):
        await get_webshop_preise(["ABC123", "   "])


@pytest.mark.asyncio
async def test_get_webshop_prices_json_output():
    batch = pa.RecordBatch.from_pylist([
        {"mpn": "ABC123", "quantity_breakpoint": 1, "unit_price": 0.25, "updated_at": "2024-01-01 12:00:00"},
    ])

    async def fake_stream(*args, **kwargs):
        yield batch

    with patch("alphaplan_mcp.tools.webshop.fetch_webshop_prices_stream", side_effect=fake_stream):
        result = await get_webshop_preise(["ABC123"], output_format="json")
        assert "results" in result
        assert len(result["results"]) == 1
        row = result["results"][0]
        assert row["MPN"] == "ABC123"
        assert row["breakpoint_min"] == 1
        assert row["breakpoint_max"] == 1
        assert row["unit_price_min"] == 0.25
        assert row["unit_price_max"] == 0.25
        assert row["updated_at"] == "2024-01-01"  # date part of the price row's updated_at
        assert row["error"] is None


@pytest.mark.asyncio
async def test_get_webshop_prices_updated_at_is_latest_price_row_date():
    from datetime import datetime
    batch = pa.RecordBatch.from_pylist([
        {"mpn": "ABC123", "quantity_breakpoint": 1, "unit_price": 0.25,
         "updated_at": datetime(2026, 3, 1, 9, 0)},
        {"mpn": "ABC123", "quantity_breakpoint": 100, "unit_price": 0.15,
         "updated_at": datetime(2026, 8, 4, 13, 31, 10, 342676)},
        {"mpn": "DEF456", "quantity_breakpoint": 1, "unit_price": 0.50, "updated_at": None},
    ])
    # a column holds one type, so the unparseable string comes in its own batch
    string_batch = pa.RecordBatch.from_pylist([
        {"mpn": "GHI789", "quantity_breakpoint": 1, "unit_price": 0.70, "updated_at": "not a date"},
    ])

    async def fake_stream(*args, **kwargs):
        yield batch
        yield string_batch

    with patch("alphaplan_mcp.tools.webshop.fetch_webshop_prices_stream", side_effect=fake_stream):
        result = await get_webshop_preise(["ABC123", "DEF456", "GHI789"], output_format="json")
        by_mpn = {r["MPN"]: r for r in result["results"]}
        assert by_mpn["ABC123"]["updated_at"] == "2026-08-04"
        assert by_mpn["DEF456"]["updated_at"] is None
        assert by_mpn["GHI789"]["updated_at"] is None

        csv_result = await get_webshop_preise(["DEF456"])
        assert csv_result["csv"].splitlines()[1].split(",")[5] == ""


@pytest.mark.asyncio
async def test_get_webshop_prices_unknown_mpn_keeps_input_order():
    batch = pa.RecordBatch.from_pylist([
        {"mpn": "KNOWN", "quantity_breakpoint": 1, "unit_price": 0.25, "updated_at": "2024-01-01 12:00:00"},
    ])

    async def fake_stream(*args, **kwargs):
        yield batch

    with patch("alphaplan_mcp.tools.webshop.fetch_webshop_prices_stream", side_effect=fake_stream):
        result = await get_webshop_preise(["UNKNOWN", "KNOWN"], output_format="json")
    rows = result["results"]
    assert [r["MPN"] for r in rows] == ["UNKNOWN", "KNOWN"]
    assert rows[0]["error"] == "no webshop prices found" and rows[0]["unit_price_min"] is None
    assert rows[1]["error"] is None
