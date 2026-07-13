"""Generic result‑set tools."""

import logging
from typing import List, Optional

from fastmcp import Context

from ..models import FilterGroup, ResultSetSummary, SortSpec
from ..query_engine import QueryEngine
from ..result_store import ResultSetStore

logger = logging.getLogger(__name__)

async def get_result_info(
    result_id: str,
    ctx: Context,
):
    """Return metadata and summary for a stored result set."""
    store: ResultSetStore = ctx.request_context.app.state.result_store
    engine = QueryEngine(store, ctx.session_id)
    return await engine.get_result_info(result_id)


async def list_result_sets(
    ctx: Context,
):
    """List all result sets owned by the current session."""
    store: ResultSetStore = ctx.request_context.app.state.result_store
    engine = QueryEngine(store, ctx.session_id)
    return await engine.list_result_sets()


async def explain_columns(
    result_id: str,
    columns: Optional[List[str]] = None,
    include_examples: bool = False,
    ctx: Context = None,
):
    """Return per‑column statistics, optionally with examples."""
    store: ResultSetStore = ctx.request_context.app.state.result_store
    engine = QueryEngine(store, ctx.session_id)
    return await engine.explain_columns(result_id, columns, include_examples)


async def list_rows(
    result_id: str,
    columns: Optional[List[str]] = None,
    limit: int = 50,
    offset: int = 0,
    sort: Optional[List[SortSpec]] = None,
    ctx: Context = None,
):
    """Return a subset of rows from a stored result set."""
    store: ResultSetStore = ctx.request_context.app.state.result_store
    engine = QueryEngine(store, ctx.session_id)
    return await engine.list_rows(result_id, columns, limit, offset, sort)


async def count_rows(
    result_id: str,
    filters: Optional[FilterGroup] = None,
    ctx: Context = None,
):
    """Return the number of rows matching optional filters."""
    store: ResultSetStore = ctx.request_context.app.state.result_store
    engine = QueryEngine(store, ctx.session_id)
    return await engine.count_rows(result_id, filters)
