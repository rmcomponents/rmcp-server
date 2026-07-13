"""Generic result‑set tools."""

import logging
from typing import List, Optional

from fastmcp import Context

from ..errors import ToolError, tool_error
from ..models import FilterGroup, SortSpec
from ..query_engine import QueryEngine
from ..result_store import ResultSetStore

logger = logging.getLogger(__name__)


async def get_result_info(result_id: str, ctx: Context):
    """Return metadata and summary for a stored result set."""
    try:
        store: ResultSetStore = ctx.request_context.app.state.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.get_result_info(result_id)
    except ToolError as e:
        return e.to_dict()
    except Exception as e:
        logger.exception("get_result_info failed")
        return tool_error("INTERNAL_ERROR", "An unexpected error occurred.").to_dict()


async def list_result_sets(ctx: Context):
    """List all result sets owned by the current session."""
    try:
        store: ResultSetStore = ctx.request_context.app.state.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.list_result_sets()
    except ToolError as e:
        return e.to_dict()
    except Exception as e:
        logger.exception("list_result_sets failed")
        return tool_error("INTERNAL_ERROR", "An unexpected error occurred.").to_dict()


async def explain_columns(
    result_id: str,
    columns: Optional[List[str]] = None,
    include_examples: bool = False,
    ctx: Context = None,
):
    """Return per‑column statistics, optionally with examples."""
    try:
        store: ResultSetStore = ctx.request_context.app.state.result_store
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
    ctx: Context = None,
):
    """Return a subset of rows from a stored result set."""
    try:
        store: ResultSetStore = ctx.request_context.app.state.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.list_rows(result_id, columns, limit, offset, sort)
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
        store: ResultSetStore = ctx.request_context.app.state.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.count_rows(result_id, filters)
    except ToolError as e:
        return e.to_dict()
    except Exception as e:
        logger.exception("count_rows failed")
        return tool_error("INTERNAL_ERROR", "An unexpected error occurred.").to_dict()
