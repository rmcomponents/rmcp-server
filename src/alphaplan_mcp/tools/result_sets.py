# Implements generic MCP tools for exploring and aggregating stored result sets,
# such as list_rows, query_rows, and aggregate.
# Enforces session ownership and safety limits.
"""Generic result‑set tools."""

import logging
from typing import Any, Dict, List, Optional, Union

from fastmcp import Context

from ..errors import ToolError, tool_error
from ..models import FilterGroup, SortSpec, AggregationSpec, ResultSetSummary
from ..query_engine import QueryEngine
from ..result_store import ResultSetStore

logger = logging.getLogger(__name__)


async def get_result_info(result_id: str, ctx: Context):
    """Return metadata and summary for a stored result set."""
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.get_result_info(result_id)
    except ToolError as e:
        return e.to_dict()
    except Exception as e:
        logger.exception("get_result_info failed")
        return tool_error("INTERNAL_ERROR", "An unexpected error occurred.").to_dict()




async def explain_columns(
    result_id: str,
    columns: Optional[List[str]] = None,
    include_examples: bool = False,
    ctx: Context = None,
):
    """Return per‑column statistics, optionally with examples."""
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.explain_columns(result_id, columns, include_examples)
    except ToolError as e:
        return e.to_dict()
    except Exception as e:
        logger.exception("explain_columns failed")
        return tool_error("INTERNAL_ERROR", "An unexpected error occurred.").to_dict()


async def list_rows(
    result_id: str,
    columns: Optional[List[str]] = None,
    limit: int = 50,
    offset: int = 0,
    sort: Optional[List[SortSpec]] = None,
    store_result: bool = False,
    ctx: Context = None,
) -> Union[Dict[str, Any], ResultSetSummary]:
    """Return a subset of rows from a stored result set."""
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.list_rows(result_id, columns, limit, offset, sort, store_result)
    except ToolError as e:
        return e.to_dict()
    except Exception as e:
        logger.exception("list_rows failed")
        return tool_error("INTERNAL_ERROR", "An unexpected error occurred.").to_dict()


async def count_rows(
    result_id: str,
    filters: Optional[FilterGroup] = None,
    ctx: Context = None,
):
    """Return the number of rows matching optional filters."""
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.count_rows(result_id, filters)
    except ToolError as e:
        return e.to_dict()
    except Exception as e:
        logger.exception("count_rows failed")
        return tool_error("INTERNAL_ERROR", "An unexpected error occurred.").to_dict()


async def query_rows(
    result_id: str,
    filters: Optional[FilterGroup] = None,
    columns: Optional[List[str]] = None,
    sort: Optional[List[SortSpec]] = None,
    limit: int = 50,
    offset: int = 0,
    store_result: bool = False,
    ctx: Context = None,
) -> Union[Dict[str, Any], ResultSetSummary]:
    """Return rows matching structured filters, with optional sorting and projection."""
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.query_rows(result_id, filters, columns, sort, limit, offset, store_result)
    except ToolError as e:
        return e.to_dict()
    except Exception as e:
        logger.exception("query_rows failed")
        return tool_error("INTERNAL_ERROR", "An unexpected error occurred.").to_dict()


async def distinct_values(
    result_id: str,
    column: str,
    filters: Optional[FilterGroup] = None,
    limit: int = 50,
    offset: int = 0,
    include_counts: bool = True,
    order: str = "count_desc",
    include_null: bool = False,
    ctx: Context = None,
):
    """Return distinct values for a column, with optional counts and ordering."""
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.distinct_values(
            result_id, column, filters, limit, offset, include_counts, order, include_null
        )
    except ToolError as e:
        return e.to_dict()
    except Exception as e:
        logger.exception("distinct_values failed")
        return tool_error("INTERNAL_ERROR", "An unexpected error occurred.").to_dict()


async def aggregate(
    result_id: str,
    aggregations: List[AggregationSpec],
    filters: Optional[FilterGroup] = None,
    ctx: Context = None,
):
    """Compute aggregations over the result set (or a filtered subset)."""
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.aggregate(result_id, aggregations, filters)
    except ToolError as e:
        return e.to_dict()
    except Exception as e:
        logger.exception("aggregate failed")
        return tool_error("INTERNAL_ERROR", "An unexpected error occurred.").to_dict()


async def group_by(
    result_id: str,
    group_columns: List[str],
    aggregations: List[AggregationSpec],
    filters: Optional[FilterGroup] = None,
    sort: Optional[List[SortSpec]] = None,
    limit: int = 50,
    offset: int = 0,
    ctx: Context = None,
):
    """Group rows and compute aggregations per group."""
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.group_by(
            result_id, group_columns, aggregations, filters, sort, limit, offset
        )
    except ToolError as e:
        return e.to_dict()
    except Exception as e:
        logger.exception("group_by failed")
        return tool_error("INTERNAL_ERROR", "An unexpected error occurred.").to_dict()


async def histogram(
    result_id: str,
    column: str,
    bins: int = 10,
    filters: Optional[FilterGroup] = None,
    ctx: Context = None,
):
    """
    Compute a histogram for a numeric, date, or string column.
    
    For numeric or date columns, returns binned counts.
    For string columns, returns the top N most frequent values (where N is `bins`),
    along with an `other_count` representing the remaining values.
    """
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.histogram(result_id, column, bins, filters)
    except ToolError as e:
        return e.to_dict()
    except Exception as e:
        logger.exception("histogram failed")
        return tool_error("INTERNAL_ERROR", "An unexpected error occurred.").to_dict()


async def delete_result_set(
    result_id: str,
    ctx: Context = None,
):
    """Delete a stored result set."""
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.delete_result_set(result_id)
    except ToolError as e:
        return e.to_dict()
    except Exception as e:
        logger.exception("delete_result_set failed")
        return tool_error("INTERNAL_ERROR", "An unexpected error occurred.").to_dict()
