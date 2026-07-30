import pytest
import pyarrow as pa
from unittest.mock import patch, AsyncMock, MagicMock
from alphaplan_mcp.tools.parts import list_part_stats
from alphaplan_mcp.errors import ToolError

@pytest.mark.asyncio
async def test_list_part_stats_missing_year():
    with pytest.raises(ToolError):
        await list_part_stats(year=None)

@pytest.mark.asyncio
async def test_list_part_stats_with_limit():
    mock_batch = pa.RecordBatch.from_pylist([{
        "ArtikelNummer": "A123",
        "EinkaufVerkauf": "V",
        "Jahr": 2025,
        "KW": 1,
        "Netto": 100.0,
        "Rohertrag": 20.0,
        "Menge": 5
    }])
    
    async def mock_stream(*args, **kwargs):
        yield mock_batch

    with patch('alphaplan_mcp.tools.parts.fetch_part_stats_stream', side_effect=mock_stream):
        result = await list_part_stats(year=2025, limit=10)
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["ArtikelNummer"] == "A123"

@pytest.mark.asyncio
async def test_list_part_stats_store_result():
    mock_batch = pa.RecordBatch.from_pylist([{
        "ArtikelNummer": "A123",
        "EinkaufVerkauf": "V",
        "Jahr": 2025,
        "KW": 1,
        "Netto": 100.0,
        "Rohertrag": 20.0,
        "Menge": 5
    }])
    
    async def mock_stream(*args, **kwargs):
        yield mock_batch

    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = AsyncMock()
    mock_store.store_result_stream.return_value = {"result_id": "rs_123"}
    mock_ctx.fastmcp.result_store = mock_store

    with patch('alphaplan_mcp.tools.parts.fetch_part_stats_stream', side_effect=mock_stream):
        result = await list_part_stats(year=2025, ctx=mock_ctx)
        assert isinstance(result, dict)
        assert result["result_id"] == "rs_123"
        mock_store.store_result_stream.assert_called_once()
