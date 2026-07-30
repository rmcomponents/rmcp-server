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
    format: str = "json",
    encoding: str = "plain",
    round_to: Optional[int] = None,
    ctx: Context = None,
) -> Union[Dict[str, Any], ResultSetSummary]:
    """Return a subset of rows from a stored result set.

    Read-only. `sort`, `columns` and `round_to` are applied per call and never modify the stored
    set, so the same result_id can be read repeatedly in different orders and projections.

    Parameters:
    - `columns`: Optional. Project to these columns. Sorting is applied BEFORE projection, so you
      may sort by a column you do not select.
    - `sort`: Optional. Applied at read time. A unique tie-break is appended automatically, so
      ordering is deterministic and `limit`/`offset` paging cannot duplicate or drop rows across
      tied values. String comparison is ordinal (uppercase sorts before lowercase).
    - `limit`, `offset`: Paging. The response carries `total_rows`, `returned`, `has_more` and
      `next_offset`.
    - `format`: "json" (default) returns `data` as an array of objects. "csv" returns `data` as a
      string with a header line and RFC 4180 quoting.
    - `encoding`: "plain" (default) or "gzip+base64". With gzip, `data` is base64 of a gzip stream,
      and `uncompressed_bytes` and `sha256` describe the decoded payload.
      NOTE: gzip is silently downgraded to plain when compression would not shrink the payload.
      Read the returned `encoding` field; do not assume what you requested.
    - `round_to`: Optional. Rounds numeric columns to this many decimal places. Values that are
      integral are emitted without a decimal point.
    - `store_result`: Optional. If true, stores the read subset as a new result set and returns a `ResultSetSummary` instead of the rows.

    The payload is always in `data`, whose type depends on `format`.

    Efficiency: `format="csv"` with `round_to` is the cheapest option and much smaller than the
    default JSON. gzip adds little on top and only pays for large row counts. For a large export,
    aggregate first (`group_by`) and read the derived result_id.

    Errors: INVALID_FORMAT, INVALID_ENCODING, UNKNOWN_COLUMN (returns `available_columns` in
    `details`).
    """
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.list_rows(
            result_id, columns, limit, offset, sort, store_result, 
            format, encoding, round_to
        )
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
    """Return rows matching structured filters, with optional sorting and projection.

    Parameters:
    - `store_result`: Optional. If true, stores the result subset as a new result set and returns a `ResultSetSummary` instead of the rows.

    Chaining: any tool taking a `result_id` accepts a derived one, including `group_by` output.
    """
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
    store_result: bool = False,
    result_label: Optional[str] = None,
    ctx: Context = None,
):
    """Return distinct values for a column, with optional counts and ordering.

    Parameters:
    - `store_result`: Optional. If true, stores the result subset as a new result set and returns a `ResultSetSummary` instead of the rows.

    Chaining: any tool taking a `result_id` accepts a derived one, including `group_by` output.
    """
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.distinct_values(
            result_id, column, filters, limit, offset, include_counts, order, include_null,
            store_result, result_label
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
    store_result: bool = False,
    result_label: Optional[str] = None,
    ctx: Context = None,
):
    """Compute aggregations over the result set (or a filtered subset).

    Parameters:
    - `store_result`: Optional. If true, stores the result subset as a new result set and returns a `ResultSetSummary` instead of the rows.

    Chaining: any tool taking a `result_id` accepts a derived one, including `group_by` output.
    """
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.aggregate(result_id, aggregations, filters, store_result, result_label)
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
    store_result: bool = False,
    result_label: Optional[str] = None,
    ctx: Context = None,
):
    """Group rows and compute aggregations per group.

    Parameters:
    - `store_result`: Optional. If true, stores the result subset as a new result set and returns a `ResultSetSummary` instead of the rows.
      `group_by` with `store_result=true` stores the complete grouped set, while `limit` only caps the rows returned in that response.

    Chaining: any tool taking a `result_id` accepts a derived one, including `group_by` output.
    """
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.group_by(
            result_id, group_columns, aggregations, filters, sort, limit, offset,
            store_result, result_label
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
    store_result: bool = False,
    result_label: Optional[str] = None,
    ctx: Context = None,
):
    """
    Compute a histogram for a numeric, date, or string column.
    
    For numeric or date columns, returns binned counts.
    For string columns, returns the top N most frequent values (where N is `bins`),
    along with an `other_count` representing the remaining values.

    Parameters:
    - `store_result`: Optional. If true, stores the result subset as a new result set and returns a `ResultSetSummary` instead of the rows.

    Chaining: any tool taking a `result_id` accepts a derived one, including `group_by` output.
    """
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.histogram(result_id, column, bins, filters, store_result, result_label)
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
