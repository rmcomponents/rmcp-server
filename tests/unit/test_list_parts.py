"""Unit tests for list_artikel default-exclusion warning (Rec 4)."""

import pytest
import pyarrow as pa
from unittest.mock import patch, AsyncMock, MagicMock

from alphaplan_mcp.tools.parts import list_artikel


@pytest.mark.asyncio
async def test_list_parts_default_exclusion_adds_warning():
    mock_batch = pa.RecordBatch.from_pylist([{"ArtikelNummer": "A123"}])

    async def mock_stream(*args, **kwargs):
        yield mock_batch

    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = AsyncMock()
    mock_store.store_result_stream.return_value = {
        "result_id": "rs_1",
        "row_count": 1,
        "owner_session_id": "test_session",
        "provenance": {},
        "row_count_total": 1,
        "column_count": 1,
        "stored_size_bytes": 100,
        "created_at": None,
        "expires_at": None,
        "columns": [],
    }
    mock_ctx.fastmcp.result_store = mock_store

    with patch("alphaplan_mcp.tools.parts.fetch_parts_stream", side_effect=mock_stream):
        result = await list_artikel(ctx=mock_ctx)

    # default negative_filter_term ('TCC%') applied -> prominent warning present
    assert isinstance(result, dict)
    assert result["result_id"] == "rs_1"
    assert "warning" in result
    assert "TCC" in result["warning"]
    # the filter is still propagated to the DB stream with the default value
    _, kwargs = mock_store.store_result_stream.call_args
    provenance_params = kwargs["provenance"].parameters
    assert provenance_params["negative_filter_term"] == "TCC%"


@pytest.mark.asyncio
async def test_list_parts_no_default_warning_when_explicit_none():
    mock_batch = pa.RecordBatch.from_pylist([{"ArtikelNummer": "A234"}])

    async def mock_stream(*args, **kwargs):
        yield mock_batch

    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = AsyncMock()
    mock_store.store_result_stream.return_value = {
        "result_id": "rs_2",
    }
    mock_ctx.fastmcp.result_store = mock_store

    with patch("alphaplan_mcp.tools.parts.fetch_parts_stream", side_effect=mock_stream):
        result = await list_artikel(negative_filter_term=None, ctx=mock_ctx)

    assert isinstance(result, dict)
    assert result["result_id"] == "rs_2"
    assert "warning" not in result


@pytest.mark.asyncio
async def test_list_parts_include_archived_propagated():
    mock_batch = pa.RecordBatch.from_pylist([{"ArtikelNummer": "A234"}])

    def mock_factory(*args, **kwargs):
        captured["include_archived"] = kwargs.get("include_archived")
        async def gen():
            yield mock_batch
        return gen()

    captured = {}
    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = AsyncMock()
    mock_store.store_result_stream.return_value = {"result_id": "rs_3"}
    mock_ctx.fastmcp.result_store = mock_store

    with patch("alphaplan_mcp.tools.parts.fetch_parts_stream", side_effect=mock_factory):
        await list_artikel(negative_filter_term=None, include_archived=True, ctx=mock_ctx)

    assert captured["include_archived"] is True
    _, kwargs = mock_store.store_result_stream.call_args
    assert kwargs["provenance"].parameters["include_archived"] is True


@pytest.mark.asyncio
async def test_list_parts_include_archived_default_false():
    mock_batch = pa.RecordBatch.from_pylist([{"ArtikelNummer": "A234"}])

    def mock_factory(*args, **kwargs):
        captured["include_archived"] = kwargs.get("include_archived")
        async def gen():
            yield mock_batch
        return gen()

    captured = {}
    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = AsyncMock()
    mock_store.store_result_stream.return_value = {"result_id": "rs_4"}
    mock_ctx.fastmcp.result_store = mock_store

    with patch("alphaplan_mcp.tools.parts.fetch_parts_stream", side_effect=mock_factory):
        await list_artikel(negative_filter_term=None, ctx=mock_ctx)

    assert captured["include_archived"] is False
    _, kwargs = mock_store.store_result_stream.call_args
    assert kwargs["provenance"].parameters["include_archived"] is False