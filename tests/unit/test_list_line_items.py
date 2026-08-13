import pytest
import pyarrow as pa
from unittest.mock import patch, MagicMock
from alphaplan_mcp.tools.documents import list_line_items, _price_summary
from alphaplan_mcp.errors import ToolError

@pytest.mark.asyncio
async def test_list_line_items_missing_params():
    with pytest.raises(ToolError):
        await list_line_items(artikel_nummer="", document_type="AR", date_from="2025-01-01", date_to="2025-12-31")
    with pytest.raises(ToolError):
        await list_line_items(artikel_nummer="A123", document_type="AR", date_from=None, date_to="2025-12-31")
    with pytest.raises(ToolError):
        await list_line_items(artikel_nummer="A123", document_type="AR", date_from="2025-01-01", date_to=None)
    with pytest.raises(ToolError):
        await list_line_items(artikel_nummer="A123", document_type="", date_from="2025-01-01", date_to="2025-12-31")

@pytest.mark.asyncio
async def test_list_line_items_invalid_document_type():
    with pytest.raises(ToolError) as exc_info:
        await list_line_items(artikel_nummer="A123", document_type="XX", date_from="2025-01-01", date_to="2025-12-31")
    assert exc_info.value.code == "invalid_parameters"
    assert "available_document_types" in exc_info.value.details
    assert "EB" in exc_info.value.details["available_document_types"]

def _batch(rows):
    return pa.RecordBatch.from_pylist(rows)

_ROW_BASE = {
    "ArtikelNummer": "A123", "BelegNummer": "ER1", "KundenNummer": 4711,
    "KundeOderLieferant": "Supplier A",
    "KundeKlassifizierung": "S", "KundeBranche": "Industrial", "Menge": 100,
    "MengeOffen": 0, "NettoPreisStueckEUR": 0.05, "BelegDatum": "2025-01-14 15:06:44.000",
    "LieferDatum": "2025-02-01 00:00:00.000", "LieferDatumBestaetigt": None,
    "OriginalDatum": "2025-01-20 00:00:00.000", "BelegKursFaktor": 0.006,
    "BelegWaehrung": "JPY", "IstArchiviert": 0,
}

@pytest.mark.asyncio
async def test_list_line_items_store_result_with_summary():
    row1 = dict(_ROW_BASE, BelegNummer="ER1", NettoPreisStueckEUR=0.03, IstArchiviert=1)
    row2 = dict(_ROW_BASE, BelegNummer="ER2", NettoPreisStueckEUR=0.05, IstArchiviert=0)
    rows = [row1, row2]

    async def mock_stream(*args, **kwargs):
        yield _batch(rows)

    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = MagicMock(store_result_stream=None)

    async def consume(session_id, provenance, batches, **kwargs):
        captured["provenance"] = provenance
        async for _ in batches:
            pass
        return {"result_id": "rs_123"}
    captured = {}
    mock_store.store_result_stream = consume
    mock_ctx.fastmcp.result_store = mock_store

    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        result = await list_line_items(
            artikel_nummer="A123",
            document_type="er",
            date_from="2025-01-01",
            date_to="2025-12-31",
            ctx=mock_ctx,
        )
    assert isinstance(result, dict)
    assert result["result_id"] == "rs_123"
    assert captured["provenance"].op == "list_line_items"
    assert captured["provenance"].parameters["beleg_typ"] == 10
    ps = result["price_summary"]
    assert ps["min"] == 0.03
    assert ps["max"] == 0.05
    assert ps["average"] == 0.04
    assert ps["median"] == 0.04
    assert ps["count"] == 2
    assert result["archived_summary"] == {"archived": 1, "non_archived": 1}

@pytest.mark.asyncio
async def test_list_line_items_resolves_eb_to_9():
    async def mock_stream(*args, **kwargs):
        captured["beleg_typ"] = kwargs.get("beleg_typ")
        return
        yield  # pragma: no cover

    captured = {}
    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = MagicMock(store_result_stream=None)

    async def consume(session_id, provenance, batches, **kwargs):
        async for _ in batches:
            pass
        return {"result_id": "rs_eb"}
    mock_store.store_result_stream = consume
    mock_ctx.fastmcp.result_store = mock_store

    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        result = await list_line_items(
            artikel_nummer="A123",
            document_type="EB",
            date_from="2025-01-01",
            date_to="2025-12-31",
            ctx=mock_ctx,
        )
    assert result["result_id"] == "rs_eb"
    assert captured["beleg_typ"] == 9


def _run(captured):
    async def mock_stream(*args, **kwargs):
        captured["include_archived"] = kwargs.get("include_archived")
        return
        yield  # pragma: no cover

    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = MagicMock(store_result_stream=None)

    async def consume(session_id, provenance, batches, **kwargs):
        async for _ in batches:
            pass
        return {"result_id": "rs_arch"}
    mock_store.store_result_stream = consume
    mock_ctx.fastmcp.result_store = mock_store
    return mock_ctx, mock_stream


@pytest.mark.parametrize("doc_type,expected", [
    ("AB", True),
    ("ab", True),
    ("AN", False),
    ("AR", False),
    ("EB", False),
    ("ER", False),
    ("AL", False),
    ("AA", False),
])
@pytest.mark.asyncio
async def test_list_line_items_include_archived_default(doc_type, expected):
    captured = {}
    mock_ctx, mock_stream = _run(captured)
    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        await list_line_items(
            artikel_nummer="A123",
            document_type=doc_type,
            date_from="2025-01-01",
            date_to="2025-12-31",
            ctx=mock_ctx,
        )
    assert captured["include_archived"] is expected


@pytest.mark.asyncio
async def test_list_line_items_include_archived_explicit_override():
    captured = {}
    mock_ctx, mock_stream = _run(captured)
    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        await list_line_items(
            artikel_nummer="A123",
            document_type="AR",
            date_from="2025-01-01",
            date_to="2025-12-31",
            include_archived=True,
            ctx=mock_ctx,
        )
    assert captured["include_archived"] is True

    captured2 = {}
    mock_ctx2, mock_stream2 = _run(captured2)
    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream2):
        await list_line_items(
            artikel_nummer="A123",
            document_type="AB",
            date_from="2025-01-01",
            date_to="2025-12-31",
            include_archived=False,
            ctx=mock_ctx2,
        )
    assert captured2["include_archived"] is False

@pytest.mark.asyncio
async def test_price_summary_median_even_and_odd():
    assert _price_summary([3, 1, 2])["median"] == 2
    assert _price_summary([4, 1, 3, 2])["median"] == 2.5
    assert _price_summary([10]) == {"min": 10, "max": 10, "average": 10, "median": 10, "count": 1}

@pytest.mark.asyncio
async def test_list_line_items_empty_skips_nulls():
    batch = _batch([
        dict(_ROW_BASE, BelegNummer="ER1", NettoPreisStueckEUR=None, LieferDatum=None,
             LieferDatumBestaetigt=None, OriginalDatum=None, IstArchiviert=0),
    ])

    async def mock_stream(*args, **kwargs):
        yield batch

    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = MagicMock(store_result_stream=None)

    async def consume(session_id, provenance, batches, **kwargs):
        async for _ in batches:
            pass
        return {"result_id": "rs_empty"}
    mock_store.store_result_stream = consume
    mock_ctx.fastmcp.result_store = mock_store

    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        result = await list_line_items(
            artikel_nummer="A123",
            document_type="AR",
            date_from="2025-01-01",
            date_to="2025-12-31",
            ctx=mock_ctx,
        )
    assert result["price_summary"] == {"min": None, "max": None, "average": None, "median": None, "count": 0}
    assert result["archived_summary"] == {"archived": 0, "non_archived": 1}