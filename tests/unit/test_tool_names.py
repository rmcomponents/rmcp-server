"""Pins the public tool surface: German domain names, English verbs and technical parameters."""
import pytest

from alphaplan_mcp.server import mcp

EXPECTED = {
    "list_beleg_positionen": ["artikel_nummer", "beleg_typ", "date_from", "date_to", "include_archived",
                              "adress_nummer", "open_only", "include_text_lines"],
    "list_adressen": ["such_name"],
    "list_artikel": ["positive_filter_term", "negative_filter_term", "match_code", "sample_n",
                     "include_archived"],
    "list_artikel_statistik": ["artikel_nummer", "jahr", "einkauf_verkauf", "limit"],
    "get_webshop_preise": ["artikel_nummern", "output_format"],
    "search_mouser_artikel": ["artikel_nummern", "caching", "cache_ttl_override_hours", "output_format",
                              "retry_on_rate_limit"],
    "list_documentation": [],
    "read_documentation": ["uri"],
    "search_documentation": ["query"],
}


async def _tools():
    if hasattr(mcp, "get_tools"):
        return await mcp.get_tools()
    return {t.name: t for t in await mcp.list_tools()}


@pytest.mark.asyncio
async def test_domain_tool_names_and_parameters():
    tools = await _tools()
    for name, params in EXPECTED.items():
        assert name in tools, name
        assert list((tools[name].parameters or {}).get("properties", {})) == params, name


@pytest.mark.asyncio
async def test_old_tool_names_are_gone():
    tools = await _tools()
    for old in ("list_line_items", "list_customers", "list_parts", "list_part_stats",
                "get_webshop_prices", "search_mouser_parts", "list_documents", "read_document",
                "search_documents"):
        assert old not in tools, old
