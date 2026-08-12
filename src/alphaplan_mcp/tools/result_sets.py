# Implements generic MCP tools for exploring and aggregating stored result sets,
# such as list_rows, query_rows, and aggregate.
# Enforces session ownership and safety limits.
"""Generic result‑set tools."""

import logging
from typing import Any, Dict, List, Optional, Union

from fastmcp import Context

from ..errors import ToolError, tool_error
from ..models import FilterGroup, SortSpec, AggregationSpec, JoinColumnPair, ResultSetSummary
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

    The final `sha256-base64` value is Base64(SHA-256(exact UTF-8 bytes of the
    preceding CSV)), excluding the checksum line. Preserve LF line endings and
    the final newline exactly. Verify before using the data.

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


async def join(
    left_result_id: str,
    right_result_id: str,
    on: List[Union[str, JoinColumnPair]],
    how: str = "left",
    columns_left: Optional[List[str]] = None,
    columns_right: Optional[List[str]] = None,
    suffix: str = "_r",
    normalize_keys: bool = True,
    validate: Optional[str] = "many_to_one",
    indicator: Union[str, bool] = "_join_match",
    result_label: Optional[str] = None,
    ctx: Context = None,
) -> Union[Dict[str, Any], ResultSetSummary]:
    """Join two stored result sets on shared keys and store the combined result.

    Combines two result sets into one chainable result set. This is the only operation
    that spans more than one `result_id`; it exists to enable questions such as
    "revenue by product group" when the measures and the dimensions live in different
    result sets (e.g. `list_part_stats` left-joined with `list_parts`).

    Parameters:
    - `left_result_id`, `right_result_id`: The two sets. Both must belong to the caller's
      session (`OWNER_SESSION_MISMATCH` otherwise).
    - `on`: Join keys. Each entry is either a shared column name (`"ArtikelNummer"`) or an
      explicit pair (`{"left": "...", "right": "..."}`) when the sides differ. Mixed forms are
      allowed; multi-column keys are combined with AND. All listed columns must exist on their
      side.
    - `how`: "left" (default), "inner", "right" or "full". `left` is the default deliberately:
      it keeps every left row (right columns null where unmatched) so silent fan-out or dropped
      rows cannot hide coverage problems. An `inner` join requires asking for `inner` explicitly.
    - `columns_left` / `columns_right`: Projection per side; `null` means all columns. Join key
      columns are always retained regardless of projection. Projecting `columns_right` narrowly is
      strongly recommended — e.g. the full article master carries `Beschreibung` which reaches
      3,093 characters per row.
    - `suffix`: Suffix appended to the right-side column when its name collides with a left-side
      name (default "_r"). An unresolved collision raises `COLUMN_NAME_COLLISION`.
    - `normalize_keys`: `true` (default) normalizes string keys before matching via a single
      fixed pipeline (Unicode NFKC, dash folding (U+2011 etc. -> '-'), collapse all whitespace,
      uppercase, strip non-alphanumeric except '-', '_' and '.'); `false` matches keys on raw
      equality. The shadow key is used only for matching; emitted key columns carry the original
      values.
    - `validate`: Guards the classic silent fan-out bug. Values: "many_to_one" (default; right-side
      keys must be unique), "one_to_one" (both sides unique), "one_to_many" (left side unique),
      "many_to_many" (no constraint), or null (no check). A violation raises
      `CARDINALITY_VIOLATION` with up to 20 offending keys and produces no result set.
    - `indicator`: Adds a generated match-indicator column recording for every row whether the key
      was found on both sides ("both"), only on the left ("left_only") or only on the right
      ("right_only"). Default name "_join_match"; pass `false` to omit the column. For `how="inner"`
      the column is constant "both" (still emitted, for schema stability). Filtering on
      `_join_match = "left_only"` then lets `aggregate` / `group_by` / `query_rows` quantify exactly
      what did not match.
    - `result_label`: Optional free-text label stored verbatim in metadata.

    Return value: A `JoinResultSummary` — `ResultSetSummary` plus a `join` report containing only
    structural metrics (never business measures): `matched_rows`, `unmatched_left_rows`,
    `unmatched_right_rows`, distinct key counts (`left_keys_total/matched/unmatched`,
    `right_keys_total/matched`), an `unmatched_key_sample` (capped at 20), and `normalization` with
    `enabled`, `left_keys_altered`, `right_keys_altered`, `matches_gained_by_normalization` and up
    to 20 `collisions`. If `unmatched_left_rows` is non-zero, quantify the impact with:
      aggregate(filters=[{column: "_join_match", operator: "eq", value: "left_only"}], ...)
    and break it down per group with `group_by(..., filters=[...same...])`.

    Chaining: the returned `result_id` is a first-class result set usable by `list_rows`,
    `query_rows`, `group_by`, `aggregate`, `distinct_values`, `histogram`, `count_rows`,
    `explain_columns`, `delete_result_set`, and as an input to a further `join`.

    Errors: RESULT_NOT_FOUND, OWNER_SESSION_MISMATCH, UNKNOWN_COLUMN (with `available_columns` and
    `side`), JOIN_KEY_TYPE_MISMATCH, CARDINALITY_VIOLATION, COLUMN_NAME_COLLISION,
    RESULT_SET_TOO_LARGE (with `estimated_row_count`), INVALID_ARGUMENT.
    """
    try:
        store: ResultSetStore = ctx.fastmcp.result_store
        engine = QueryEngine(store, ctx.session_id)
        return await engine.join(
            left_result_id, right_result_id, on, how, columns_left, columns_right,
            suffix, normalize_keys, validate, indicator, result_label
        )
    except ToolError as e:
        return e.to_dict()
    except Exception as e:
        logger.exception("join failed")
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
