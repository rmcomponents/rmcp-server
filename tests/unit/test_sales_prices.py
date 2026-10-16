"""Unit tests for get_verkaufspreise (AlphaPlan VerkaufsPreise, sentinel price lists)."""
import base64
import hashlib
import json
from datetime import datetime
from unittest.mock import patch

import pytest

from alphaplan_mcp.config import settings
from alphaplan_mcp.server import mcp
from alphaplan_mcp.tools.prices import get_verkaufspreise


def _row(mpn, anzahl_bes, netto, vpe=3000, currency="EUR", updated=datetime(2026, 10, 9, 16, 0)):
    return {"mpn": mpn, "vpe": vpe, "anzahl_bes": anzahl_bes, "netto_pro_1000": netto,
            "currency": currency, "updated_at": updated}


def _patched(rows):
    calls = []

    def fake(adress_nummer, mpns):
        calls.append((adress_nummer, mpns))
        return rows
    return patch("alphaplan_mcp.tools.prices.fetch_sales_prices", side_effect=fake), calls


@pytest.mark.asyncio
async def test_webshop_sentinel_resolves_from_settings_and_csv():
    rows = [_row("ABC", 3000.0, 44.4), _row("ABC", 30000.0, 34.3, updated=datetime(2026, 10, 10))]
    p, calls = _patched(rows)
    with p:
        result = await get_verkaufspreise(["ABC", " XYZ ", "ABC"], "SENTINEL_WEBSHOP")
    assert calls == [(settings.VERKAUFSPREISE_WEBSHOP_ADRESS_NUMMER, ["ABC", "XYZ"])]
    csv_data = result["csv"]
    assert result["sha256_base64"] == base64.b64encode(
        hashlib.sha256(csv_data.encode("utf-8")).digest()).decode("ascii")
    lines = csv_data.strip().split("\n")
    assert lines[0] == "MPN,breakpoint_min,breakpoint_max,unit_price_min,unit_price_max,currency,updated_at,error"
    assert lines[1] == "ABC,3000,30000,0.0343,0.0444,EUR,2026-10-10,"
    assert lines[2] == "XYZ,,,,,,,no sales prices found"
    assert len(lines) == 3


@pytest.mark.asyncio
async def test_luminovo_sentinel_uses_vpe_breakpoint_and_drops_invalid_vpe():
    rows = [_row("ABC", 0.0, 50.0, vpe=4000), _row("BAD", 1.0, 50.0, vpe=None)]
    p, calls = _patched(rows)
    with p:
        result = await get_verkaufspreise(["ABC", "BAD"], "SENTINEL_LUMINOVO", output_format="json")
    assert calls[0][0] == settings.VERKAUFSPREISE_LUMINOVO_ADRESS_NUMMER
    assert result["adress_nummer"] == "SENTINEL_LUMINOVO"
    abc, bad = result["results"]
    assert abc["tiers"] == [{"breakpoint": 4000, "unit_price": 0.05, "currency": "EUR"}]
    assert bad["error"] == "no sales prices found"


@pytest.mark.asyncio
async def test_customer_address_zero_breakpoint_is_first_piece():
    rows = [_row("ABC", 0.0, 20.0, vpe=5000), _row("ABC", 10000.0, 18.0, currency="USD")]
    p, calls = _patched(rows)
    with p:
        result = await get_verkaufspreise(["ABC"], "D010187", output_format="json")
    assert calls[0][0] == "D010187"
    row = result["results"][0]
    assert [t["breakpoint"] for t in row["tiers"]] == [1, 10000]
    assert row["currency"] == "EUR;USD"


@pytest.mark.asyncio
async def test_overlapping_rows_keep_first_per_breakpoint():
    """The fetcher orders newest GueltigVon first per breakpoint; 0 and 1 are the same tier."""
    rows = [_row("ABC", 1.0, 850.0, currency="JPY"), _row("ABC", 0.0, 1042.0, currency="JPY")]
    p, _ = _patched(rows)
    with p:
        result = await get_verkaufspreise(["ABC"], "D000577", output_format="json")
    assert result["results"][0]["tiers"] == [{"breakpoint": 1, "unit_price": 0.85, "currency": "JPY"}]


@pytest.mark.asyncio
async def test_unknown_address():
    p, _ = _patched(None)
    with p:
        result = await get_verkaufspreise(["ABC"], "NOPE")
    assert result["error"] == "UNKNOWN_ADDRESS"


@pytest.mark.asyncio
@pytest.mark.parametrize("mpns,address,fmt,code", [
    ([], "D1", "csv", "INVALID_MPNS"),
    (["A"] * 26, "D1", "csv", "TOO_MANY_MPNS"),
    (["A", " "], "D1", "csv", "INVALID_MPNS"),
    (["A"], " ", "csv", "INVALID_ARGUMENT"),
    (["A"], "D1", "xml", "INVALID_FORMAT"),
])
async def test_invalid_input(mpns, address, fmt, code):
    result = await get_verkaufspreise(mpns, address, output_format=fmt)
    assert result["error"] == code


@pytest.mark.asyncio
async def test_database_error_is_wrapped():
    def boom(*_):
        raise RuntimeError("Login failed for user 'secret'")
    with patch("alphaplan_mcp.tools.prices.fetch_sales_prices", side_effect=boom):
        result = await get_verkaufspreise(["ABC"], "D1")
    assert result["error"] == "DATABASE_UNAVAILABLE"
    assert "secret" not in json.dumps(result)


@pytest.mark.asyncio
async def test_description_does_not_leak_sentinel_targets():
    tools = await mcp.get_tools() if hasattr(mcp, "get_tools") else {t.name: t for t in await mcp.list_tools()}
    tool = tools["get_verkaufspreise"]
    text = json.dumps({"d": tool.description, "p": tool.parameters})
    assert "SENTINEL_WEBSHOP" in text and "SENTINEL_LUMINOVO" in text
    for value in (settings.VERKAUFSPREISE_WEBSHOP_ADRESS_NUMMER,
                  settings.VERKAUFSPREISE_LUMINOVO_ADRESS_NUMMER):
        assert value not in text
