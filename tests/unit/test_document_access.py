"""Tests for the document compatibility tools and their shared backend."""

import pytest

from alphaplan_mcp.alphaplan_resources import ALPHAPLAN_RESOURCES
from alphaplan_mcp.errors import ToolError
from alphaplan_mcp.tools import document_access


@pytest.mark.asyncio
async def test_list_document_metadata_deckt_resources(mcp_server):
    eintraege = await document_access.list_document_metadata(mcp_server)
    uris = {eintrag["uri"] for eintrag in eintraege}
    erwartet = {eintrag[1] for eintrag in ALPHAPLAN_RESOURCES}
    assert uris == erwartet
    metadata = {eintrag["uri"]: eintrag for eintrag in eintraege}
    for _, uri, name, description in ALPHAPLAN_RESOURCES:
        eintrag = metadata[uri]
        assert eintrag["name"] == name
        assert eintrag["description"] == description
        assert eintrag["mime_type"] == "text/markdown"


@pytest.mark.asyncio
async def test_load_document_liest_alle_resources(mcp_server):
    for _, uri, _, _ in ALPHAPLAN_RESOURCES:
        inhalt = await document_access.load_document(mcp_server, uri)
        assert isinstance(inhalt, str)
        assert inhalt.strip(), f"leerer Inhalt: {uri}"


@pytest.mark.asyncio
async def test_load_document_unbekannte_uri_raises(mcp_server):
    with pytest.raises(ToolError) as excinfo:
        await document_access.load_document(mcp_server, "alphaplan://gibt-es-nicht")
    assert excinfo.value.code == "RESOURCE_NOT_FOUND"


@pytest.mark.asyncio
async def test_search_document_content_findet_treffer(mcp_server):
    treffer = await document_access.search_document_content(mcp_server, "SQL")
    assert treffer, "erwartete Treffer für 'SQL'"
    assert treffer[0]["uri"] == "alphaplan://sql-muster"
    assert treffer[0]["title"]
    assert "SQL" in treffer[0]["excerpt"]
    assert treffer[0]["relevance"]


@pytest.mark.asyncio
async def test_search_document_content_leere_query(mcp_server):
    assert await document_access.search_document_content(mcp_server, "   ") == []


@pytest.mark.asyncio
async def test_search_document_content_limit(mcp_server):
    treffer = await document_access.search_document_content(mcp_server, "AlphaPlan", limit=2)
    assert len(treffer) <= 2