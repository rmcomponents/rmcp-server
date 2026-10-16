"""Unit tests for list_artikel parameter defaults and propagation."""

import pytest
import pyarrow as pa
from unittest.mock import patch, AsyncMock, MagicMock

from alphaplan_mcp.tools.parts import list_artikel


@pytest.mark.asyncio
async def test_list_parts_default_has_no_exclusion_or_warning():
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

    # default negative_filter_term is None -> no TCC exclusion, no warning
    assert isinstance(result, dict)
    assert result["result_id"] == "rs_1"
    assert "warning" not in result
    _, kwargs = mock_store.store_result_stream.call_args
    provenance_params = kwargs["provenance"].parameters
    assert provenance_params["negative_filter_term"] is None


@pytest.mark.asyncio
async def test_list_parts_explicit_negative_filter_propagated():
    mock_batch = pa.RecordBatch.from_pylist([{"ArtikelNummer": "A234"}])

    def mock_factory(*args, **kwargs):
        captured["negative_filter_term"] = kwargs.get("negative_filter_term")
        async def gen():
            yield mock_batch
        return gen()

    captured = {}
    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = AsyncMock()
    mock_store.store_result_stream.return_value = {
        "result_id": "rs_2",
    }
    mock_ctx.fastmcp.result_store = mock_store

    with patch("alphaplan_mcp.tools.parts.fetch_parts_stream", side_effect=mock_factory):
        result = await list_artikel(negative_filter_term="TCC%", ctx=mock_ctx)

    assert result["result_id"] == "rs_2"
    assert "warning" not in result
    assert captured["negative_filter_term"] == "TCC%"
    _, kwargs = mock_store.store_result_stream.call_args
    assert kwargs["provenance"].parameters["negative_filter_term"] == "TCC%"


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

class _FakeCursor:
    def __init__(self, description, rows):
        self.description = description
        self._rows = list(rows)
        self.executed = None

    def execute(self, sql, params):
        self.executed = (sql, params)

    def fetchmany(self, n):
        out, self._rows = self._rows[:n], self._rows[n:]
        return out

    def close(self):
        pass


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        pass


async def _run_parts_fetch(monkeypatch, **kwargs):
    from alphaplan_mcp import database

    rows = kwargs.pop("rows", [("A1", None, 5.0)])
    description = [("ArtikelNummer", str), ("NachfolgeArtikel", str), ("Bestand", float)]
    cursor = _FakeCursor(description, rows)
    monkeypatch.setattr(database, "build_connection", lambda: _FakeConn(cursor))
    batches = [b async for b in database.fetch_parts_stream(**kwargs)]
    return cursor, batches


@pytest.mark.asyncio
async def test_fetch_parts_default_has_no_negative_filter(monkeypatch):
    cursor, _ = await _run_parts_fetch(monkeypatch)
    sql, params = cursor.executed
    assert "NOT LIKE" not in sql
    assert params == []


@pytest.mark.asyncio
async def test_fetch_parts_explicit_negative_filter(monkeypatch):
    cursor, _ = await _run_parts_fetch(monkeypatch, negative_filter_term="TCC%")
    sql, params = cursor.executed
    assert "A.ArtikelNummer NOT LIKE ?" in sql
    assert params == ["TCC%"]


@pytest.mark.asyncio
async def test_fetch_parts_bestand_only_rm_lager(monkeypatch):
    cursor, batches = await _run_parts_fetch(monkeypatch, sample_n=3)
    sql, params = cursor.executed
    assert "COALESCE(LB.Bestand, 0) AS Bestand" in sql
    assert "SUM(LG.Bestand)" in sql
    assert "LG.LagerOrteID = 2" in sql
    assert params == [3]
    meta = batches[0].schema.field("Bestand").metadata
    assert b"RM-Lager" in meta[b"description"]
    assert b"not historic" in meta[b"description"]


@pytest.mark.asyncio
async def test_fetch_parts_all_null_text_column_stays_string(monkeypatch):
    """34 of 34 NachfolgeArtikel null must not type the column as null (breaks like/regex)."""
    _, batches = await _run_parts_fetch(monkeypatch, rows=[("A1", None, 1.0), ("A2", None, 0.0)])
    assert batches[0].schema.field("NachfolgeArtikel").type == pa.string()
    assert batches[0].schema.field("Bestand").type == pa.float64()


@pytest.mark.asyncio
async def test_fetch_parts_empty_result_keeps_schema(monkeypatch):
    _, batches = await _run_parts_fetch(monkeypatch, rows=[])
    assert len(batches) == 1 and batches[0].num_rows == 0
    assert batches[0].schema.names == ["ArtikelNummer", "NachfolgeArtikel", "Bestand"]


@pytest.mark.asyncio
async def test_fetch_part_stats_all_null_text_column_stays_string(monkeypatch):
    from alphaplan_mcp import database

    description = [("ArtikelNummer", str), ("EinkaufVerkauf", str), ("Netto", float)]
    cursor = _FakeCursor(description, [("A1", None, 1.0)])
    monkeypatch.setattr(database, "build_connection", lambda: _FakeConn(cursor))
    batches = [b async for b in database.fetch_part_stats_stream(year=2025)]
    assert batches[0].schema.field("EinkaufVerkauf").type == pa.string()
    assert b"description" in batches[0].schema.field("Netto").metadata
