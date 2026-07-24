# Implements the shared QueryEngine for safe filtering, grouping,
# sorting, and aggregation on materialized result sets using Polars,
# without constructing arbitrary SQL.
"""Polars‑based query engine for stored result sets."""

import logging
import math
import re
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union

import polars as pl
import pyarrow as pa

from .config import Settings, settings
from .errors import tool_error
from .models import (
    AggregationSpec,
    ColumnInfo,
    FilterClause,
    FilterGroup,
    ResultSetMetadata,
    ResultSetSummary,
    SortSpec,
)
from .result_store import ResultSetStore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _like_to_regex(pattern: str, case_sensitive: bool = False) -> str:
    """Convert a SQL LIKE pattern (% _ ) to a regex, escaping everything else."""
    # Escape regex metacharacters except those introduced by LIKE.
    escaped = re.escape(pattern)
    # Replace escaped wildcards with their regex equivalents
    escaped = escaped.replace(r"\%", "%").replace(r"\_", "_")
    # Now replace the LIKE wildcards with regex equivalents
    # This is safe because everything else is escaped.
    escaped = escaped.replace("%", ".*").replace("_", ".")
    if case_sensitive:
        return f"^{escaped}$"
    return f"(?i)^{escaped}$"


def _py_value_to_polars(value: Any) -> Any:
    """Convert a Python value to something Polars can handle in filters."""
    # Polars expects native Python types; maybe convert date/datetime to same.
    return value


def _build_filter_expr(filter_group: FilterGroup, schema: Dict[str, pl.DataType]) -> pl.Expr:
    """Translate a FilterGroup into a Polars expression."""
    if not filter_group.clauses:
        return pl.lit(True)

    # NEW: enforce max clauses
    if len(filter_group.clauses) > settings.TOOL_MAX_FILTER_CLAUSES:
        raise tool_error(
            "INVALID_ARGUMENT",
            f"Too many filter clauses (max {settings.TOOL_MAX_FILTER_CLAUSES})",
        )

    # NEW: validate combine value
    if filter_group.combine not in ("all", "any"):
        raise tool_error("INVALID_ARGUMENT", "FilterGroup.combine must be 'all' or 'any'")

    expressions = []
    for clause in filter_group.clauses:
        expr = _build_single_filter(clause, schema)
        expressions.append(expr)

    if filter_group.combine == "any":
        combined = expressions[0]
        for e in expressions[1:]:
            combined = combined | e
    else:  # "all"
        combined = expressions[0]
        for e in expressions[1:]:
            combined = combined & e
    return combined


def _build_single_filter(clause: FilterClause, schema: Dict[str, pl.DataType]) -> pl.Expr:
    col = clause.column
    if col not in schema:
        raise tool_error("UNKNOWN_COLUMN", f"Column '{col}' does not exist.",
                         {"available_columns": list(schema.keys())})
    dtype = schema[col]
    op = clause.operator.lower()
    value = clause.value
    case_sensitive = clause.case_sensitive

    col_expr = pl.col(col)

    # Null checks
    if op == "is_null":
        return col_expr.is_null()
    if op == "is_not_null":
        return col_expr.is_not_null()

    # Operators requiring value
    if value is None:
        raise tool_error("INVALID_ARGUMENT", f"Operator '{op}' requires a value.")

    if op == "eq":
        if _needs_like(value):
            # treat as LIKE with case-insensitive default unless case_sensitive
            return _string_like(col_expr, value, case_sensitive, negate=False)
        else:
            return col_expr == _py_value_to_polars(value)
    elif op == "ne":
        if _needs_like(value):
            return _string_like(col_expr, value, case_sensitive, negate=True)
        else:
            return col_expr != _py_value_to_polars(value)
    elif op == "like":
        return _string_like(col_expr, value, case_sensitive, negate=False)
    elif op == "not_like":
        return _string_like(col_expr, value, case_sensitive, negate=True)
    elif op == "contains":
        # Use str.contains
        if case_sensitive:
            return col_expr.str.contains(value, literal=True)
        else:
            return col_expr.str.to_lowercase().str.contains(value.lower(), literal=True)
    elif op == "starts_with":
        if case_sensitive:
            return col_expr.str.starts_with(value)
        else:
            return col_expr.str.to_lowercase().str.starts_with(value.lower())
    elif op == "ends_with":
        if case_sensitive:
            return col_expr.str.ends_with(value)
        else:
            return col_expr.str.to_lowercase().str.ends_with(value.lower())
    elif op in ("gt", "gte", "lt", "lte"):
        # numeric/date comparisons
        polars_op = {"gt": "gt", "gte": "ge", "lt": "lt", "lte": "le"}[op]
        return getattr(col_expr, polars_op)(_py_value_to_polars(value))
    elif op == "in":
        if not isinstance(value, list):
            raise tool_error("INVALID_ARGUMENT", "Operator 'in' requires a list of values.")
        # NEW: separate limit for in/not_in values
        MAX_IN_VALUES = 100
        if len(value) > MAX_IN_VALUES:
            raise tool_error("INVALID_ARGUMENT",
                             f"Too many values in 'in' (max {MAX_IN_VALUES})")
        return col_expr.is_in(value)
    elif op == "not_in":
        if not isinstance(value, list):
            raise tool_error("INVALID_ARGUMENT", "Operator 'not_in' requires a list of values.")
        MAX_IN_VALUES = 100
        if len(value) > MAX_IN_VALUES:
            raise tool_error("INVALID_ARGUMENT",
                             f"Too many values in 'not_in' (max {MAX_IN_VALUES})")
        return ~col_expr.is_in(value)
    elif op == "between":
        if not isinstance(value, list) or len(value) != 2:
            raise tool_error("INVALID_ARGUMENT", "Operator 'between' requires a list of two values.")
        low, high = value
        return (col_expr >= low) & (col_expr <= high)
    else:
        raise tool_error("INVALID_ARGUMENT", f"Unsupported operator '{op}'.")


def _needs_like(value: str) -> bool:
    """Check if value contains unescaped % or _."""
    if not isinstance(value, str):
        return False
    i = 0
    while i < len(value):
        if value[i] == "\\":
            i += 2  # skip escaped char
            continue
        if value[i] in ("%", "_"):
            return True
        i += 1
    return False


def _string_like(col_expr, pattern: str, case_sensitive: bool, negate: bool = False) -> pl.Expr:
    """Convert a SQL LIKE pattern (with escapes) to a Polars regex expression."""
    # Build regex
    # Escape everything except unescaped % and _
    # We'll use a regex pattern.
    escaped = re.escape(pattern)
    # re.escape escapes % and _ because they are not regex metacharacters, so they are just literal % and _
    # So after escaping, % and _ are still % and _ (since they are not special). So we can replace them.
    # But re.escape will escape backslashes, so we need to handle the escape character.
    # Actually re.escape makes everything literal, so a backslash becomes \\.
    # To preserve our escape semantics, we need to process the original pattern manually.
    # Let's rebuild the regex manually to be safe.
    regex_parts = []
    i = 0
    while i < len(pattern):
        c = pattern[i]
        if c == "\\":
            if i + 1 < len(pattern):
                next_char = pattern[i + 1]
                # escape the next character literally
                regex_parts.append(re.escape(next_char))
                i += 2
                continue
            else:
                regex_parts.append(re.escape(c))
        elif c == "%":
            regex_parts.append(".*")
        elif c == "_":
            regex_parts.append(".")
        else:
            regex_parts.append(re.escape(c))
        i += 1
    full_regex = "^" + "".join(regex_parts) + "$"
    if not case_sensitive:
        full_regex = "(?i)" + full_regex

    # Polars' str.contains with regex
    if negate:
        return ~col_expr.str.contains(full_regex, literal=False)
    return col_expr.str.contains(full_regex, literal=False)


def _apply_sort(df: pl.DataFrame, sort_specs: List[SortSpec]) -> pl.DataFrame:
    """Apply sorting according to SortSpec list."""
    if not sort_specs:
        return df
    by = []
    descending = []
    nulls_last = []
    for spec in sort_specs:
        if spec.column not in df.columns:
            raise tool_error("UNKNOWN_COLUMN", f"Column '{spec.column}' does not exist.",
                             {"available_columns": list(df.columns)})
        by.append(spec.column)
        descending.append(spec.direction.lower() == "desc")
        nulls_last.append(spec.nulls.lower() == "last")
    # Polars sort: nulls_last param
    return df.sort(by, descending=descending, nulls_last=nulls_last)


def _normalize_value(value: Any) -> Any:
    """Convert Polars values to JSON-friendly types."""
    if value is None:
        return None
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None  # or string?
        return value
    return value


def _df_to_rows(df: pl.DataFrame) -> List[Dict[str, Any]]:
    """Convert a Polars DataFrame to a list of dicts with normalized values."""
    rows = []
    for row in df.iter_rows(named=True):
        normalized = {k: _normalize_value(v) for k, v in row.items()}
        rows.append(normalized)
    return rows


# ---------------------------------------------------------------------------
# helpers for aggregation and pagination
# ---------------------------------------------------------------------------

def _build_agg_exprs(aggregations: List[AggregationSpec]) -> List[pl.Expr]:
    """Translate AggregationSpecs into Polars aggregation expressions.
    
    Note: For chunked processing, avg is computed as sum/count. Polars handles 
    this correctly when aggregating a full DataFrame, but if we ever stream 
    chunks, we must aggregate sum and count separately to maintain numerical correctness.
    """
    agg_exprs = []
    for agg in aggregations:
        op = agg.operation.lower()
        alias = agg.alias
        col = agg.column
        if op == "count":
            if col:
                raise tool_error("INVALID_AGGREGATION", "count does not accept a column.")
            agg_exprs.append(pl.len().alias(alias))
        elif op == "count_non_null":
            if not col:
                raise tool_error("INVALID_AGGREGATION", "count_non_null requires a column.")
            agg_exprs.append(pl.col(col).count().alias(alias))
        elif op == "count_distinct":
            if not col:
                raise tool_error("INVALID_AGGREGATION", "count_distinct requires a column.")
            agg_exprs.append(pl.col(col).n_unique().alias(alias))
        elif op == "sum":
            if not col:
                raise tool_error("INVALID_AGGREGATION", "sum requires a column.")
            agg_exprs.append(pl.col(col).sum().alias(alias))
        elif op == "avg":
            if not col:
                raise tool_error("INVALID_AGGREGATION", "avg requires a column.")
            agg_exprs.append(pl.col(col).mean().alias(alias))
        elif op == "min":
            if not col:
                raise tool_error("INVALID_AGGREGATION", "min requires a column.")
            agg_exprs.append(pl.col(col).min().alias(alias))
        elif op == "max":
            if not col:
                raise tool_error("INVALID_AGGREGATION", "max requires a column.")
            agg_exprs.append(pl.col(col).max().alias(alias))
        else:
            raise tool_error("INVALID_AGGREGATION", f"Unsupported operation '{op}'.")
    
    if not agg_exprs:
        raise tool_error("INVALID_ARGUMENT", "At least one aggregation is required.")
        
    return agg_exprs


def _format_paginated_result(
    result_id: str, 
    df: pl.DataFrame, 
    offset: int, 
    limit: int, 
    total_rows: int
) -> Dict[str, Any]:
    """Apply offset/limit slicing and format the standard pagination response."""
    df_slice = df.slice(offset, limit)
    rows = _df_to_rows(df_slice)
    has_more = offset + limit < total_rows
    next_offset = offset + limit if has_more else None
    return {
        "result_id": result_id,
        "offset": offset,
        "limit": limit,
        "returned": len(rows),
        "total_rows": total_rows,
        "has_more": has_more,
        "next_offset": next_offset,
        "rows": rows,
    }


# ---------------------------------------------------------------------------
# QueryEngine
# ---------------------------------------------------------------------------

class QueryEngine:
    def __init__(
        self,
        result_store: ResultSetStore,
        session_id: str,
        settings_obj: Settings = settings,
    ):
        self.store = result_store
        self.session_id = session_id
        self.settings = settings_obj

    async def _ensure_owned(self, result_id: str) -> ResultSetMetadata:
        meta = await self.store.get_metadata(result_id)
        if meta is None:
            raise tool_error("RESULT_NOT_FOUND", f"Result set '{result_id}' not found.")
        # update access to extend TTL
        await self.store.update_access(result_id)
        return meta

    async def _load_full_df(self, result_id: str, columns: Optional[List[str]] = None) -> pl.DataFrame:
        meta = await self._ensure_owned(result_id)
        # Safety: refuse to load huge result sets entirely into memory to prevent OOM.
        if meta.uncompressed_size_bytes > self.settings.RESULTSET_MAX_BYTES:
            raise tool_error(
                "OPERATION_TOO_LARGE",
                "Result set is too large to load entirely into memory for this operation."
            )
        table = await self.store.load_table(result_id)
        if table is None:
            raise tool_error("RESULT_NOT_FOUND", f"Result set '{result_id}' not found.")
        df = pl.from_arrow(table)
        if columns:
            for c in columns:
                if c not in df.columns:
                    raise tool_error("UNKNOWN_COLUMN", f"Column '{c}' does not exist.",
                                     {"available_columns": list(df.columns)})
            df = df.select(columns)
        return df

    async def _load_range_df(
        self, result_id: str, start: int, end: int, columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        table = await self.store.load_range(result_id, start, end)
        if table is None:
            raise tool_error("RESULT_NOT_FOUND", f"Result set '{result_id}' not found.")
        df = pl.from_arrow(table)
        if columns:
            for c in columns:
                if c not in df.columns:
                    raise tool_error("UNKNOWN_COLUMN", f"Column '{c}' does not exist.",
                                     {"available_columns": list(df.columns)})
            df = df.select(columns)
        return df

    # ---------- Helper methods ----------

    async def _store_result_from_df(
        self,
        df: pl.DataFrame,
        source: str,
        source_parameters: dict,
    ) -> ResultSetSummary:
        """Convert a Polars DataFrame to a PyArrow Table and store it as a new result set."""
        table = df.to_arrow()
        return await self.store.store_result(
            session_id=self.session_id,
            source=source,
            source_parameters=source_parameters,
            table=table,
        )

    # ---------- Tool helpers that will be called by MCP tools ----------

    async def get_result_info(self, result_id: str) -> Dict[str, Any]:
        meta = await self._ensure_owned(result_id)
        # Return only safe fields (no owner_session_id)
        return {
            "result_id": meta.result_id,
            "source": meta.source,
            "row_count": meta.row_count,
            "column_count": meta.column_count,
            "stored_size_bytes": meta.stored_size_bytes,
            "created_at": meta.created_at.isoformat(),
            "expires_at": meta.absolute_expires_at.isoformat(),
            "columns": [c.model_dump() for c in meta.columns],
        }


    async def explain_columns(
        self,
        result_id: str,
        columns: Optional[List[str]] = None,
        include_examples: bool = False,
    ) -> Dict[str, Any]:
        meta = await self._ensure_owned(result_id)
        all_columns = {c.name: c for c in meta.columns}
        if columns is None:
            columns = list(all_columns.keys())
        else:
            # validate
            for c in columns:
                if c not in all_columns:
                    raise tool_error("UNKNOWN_COLUMN", f"Column '{c}' does not exist.",
                                     {"available_columns": list(all_columns.keys())})

        result = []
        need_distinct_examples = include_examples
        if need_distinct_examples:
            # Load full table to compute distinct counts and examples
            # But we might need only a sample; for distinct we need full scan, but we can limit.
            # We'll load the full table and compute for requested columns.
            df = await self._load_full_df(result_id, columns=columns)
            # For each requested column, compute info
            for col_name in columns:
                col_info = all_columns[col_name]
                col_entry = {
                    "name": col_info.name,
                    "type": col_info.type,
                    "description": col_info.description,
                    "null_count": col_info.null_count,
                    "non_null_count": col_info.non_null_count,
                    "min": col_info.min,
                    "max": col_info.max,
                    "sum": col_info.sum,
                    "avg": col_info.avg,
                    "min_length": col_info.min_length,
                    "max_length": col_info.max_length,
                }
                # compute distinct count if possible
                try:
                    distinct_count = df[col_name].n_unique()
                    col_entry["distinct_count"] = distinct_count
                except Exception:
                    col_entry["distinct_count"] = None
                # examples (up to 3 distinct non-empty values)
                if include_examples:
                    try:
                        col_series = df[col_name]
                        non_null = col_series.drop_nulls()
                        # Filter out empty strings for string/categorical columns
                        if non_null.dtype in (pl.Utf8, pl.Categorical):
                            non_empty = non_null.filter(non_null != "")
                        else:
                            non_empty = non_null
                        
                        if non_empty.len() > 0:
                            examples = non_empty.unique(maintain_order=True).head(3).to_list()
                        else:
                            examples = []
                            
                        col_entry["examples"] = [_normalize_value(v) for v in examples]
                    except Exception:
                        col_entry["examples"] = []
                result.append(col_entry)
        else:
            # Use stored stats only
            for col_name in columns:
                col_info = all_columns[col_name]
                result.append({
                    "name": col_info.name,
                    "type": col_info.type,
                    "description": col_info.description,
                    "null_count": col_info.null_count,
                    "non_null_count": col_info.non_null_count,
                    "min": col_info.min,
                    "max": col_info.max,
                    "sum": col_info.sum,
                    "avg": col_info.avg,
                    "min_length": col_info.min_length,
                    "max_length": col_info.max_length,
                })

        return {"columns": result}

    async def list_rows(
        self,
        result_id: str,
        columns: Optional[List[str]] = None,
        limit: int = 50,
        offset: int = 0,
        sort: Optional[List[SortSpec]] = None,
        store_result: bool = False,
    ) -> Union[Dict[str, Any], ResultSetSummary]:
        limit = min(limit, self.settings.TOOL_MAX_ROWS)
        if offset < 0:
            offset = 0
        if limit < 0:
            limit = 0
        meta = await self._ensure_owned(result_id)

        if store_result:
            df = await self._load_full_df(result_id, columns=columns)
            if sort:
                df = _apply_sort(df, sort)
            return await self._store_result_from_df(
                df,
                source="list_rows",
                source_parameters={
                    "source_result_id": result_id,
                    "columns": columns,
                    "sort": [s.model_dump() for s in sort] if sort else None,
                },
            )

        if sort:
            # Load full table, sort, then slice
            df = await self._load_full_df(result_id, columns=columns)
            df = _apply_sort(df, sort)
            total_rows = df.height
            return _format_paginated_result(result_id, df, offset, limit, total_rows)
        else:
            # Use range loading to load only the needed rows
            if limit == 0 or offset >= meta.row_count:
                return {
                    "result_id": result_id,
                    "offset": offset,
                    "limit": limit,
                    "returned": 0,
                    "total_rows": meta.row_count,
                    "has_more": False,
                    "next_offset": None,
                    "rows": [],
                }
            start = offset
            end = min(offset + limit, meta.row_count)
            df = await self._load_range_df(result_id, start, end, columns=columns)
            rows = _df_to_rows(df)
            has_more = end < meta.row_count
            next_offset = end if has_more else None
            return {
                "result_id": result_id,
                "offset": offset,
                "limit": limit,
                "returned": len(rows),
                "total_rows": meta.row_count,
                "has_more": has_more,
                "next_offset": next_offset,
                "rows": rows,
            }

    async def count_rows(
        self,
        result_id: str,
        filters: Optional[FilterGroup] = None,
    ) -> Dict[str, Any]:
        meta = await self._ensure_owned(result_id)
        if filters and filters.clauses:
            df = await self._load_full_df(result_id)
            schema = {name: df[name].dtype for name in df.columns}
            mask = _build_filter_expr(filters, schema)
            filtered = df.filter(mask)
            count = filtered.height
        else:
            count = meta.row_count
        return {"result_id": result_id, "count": count}

    # ---------- New tools (Phase 6) ----------

    async def query_rows(
        self,
        result_id: str,
        filters: Optional[FilterGroup] = None,
        columns: Optional[List[str]] = None,
        sort: Optional[List[SortSpec]] = None,
        limit: int = 50,
        offset: int = 0,
        store_result: bool = False,
    ) -> Union[Dict[str, Any], ResultSetSummary]:
        limit = min(limit, self.settings.TOOL_MAX_ROWS)
        if offset < 0:
            offset = 0
        if limit < 0:
            limit = 0
        meta = await self._ensure_owned(result_id)

        # Determine needed columns for both filtering and selecting
        needed_cols = set(columns) if columns else set()
        if filters and filters.clauses:
            for clause in filters.clauses:
                needed_cols.add(clause.column)

        df = await self._load_full_df(result_id, columns=list(needed_cols) if needed_cols else None)
        schema = {name: df[name].dtype for name in df.columns}
        if filters and filters.clauses:
            mask = _build_filter_expr(filters, schema)
            df = df.filter(mask)
        if sort:
            df = _apply_sort(df, sort)
        # Select only the requested columns before returning
        if columns:
            df = df.select(columns)
            
        if store_result:
            return await self._store_result_from_df(
                df,
                source="query_rows",
                source_parameters={
                    "source_result_id": result_id,
                    "filters": filters.model_dump() if filters else None,
                    "columns": columns,
                    "sort": [s.model_dump() for s in sort] if sort else None,
                },
            )

        total_rows = df.height
        return _format_paginated_result(result_id, df, offset, limit, total_rows)

    async def distinct_values(
        self,
        result_id: str,
        column: str,
        filters: Optional[FilterGroup] = None,
        limit: int = 50,
        offset: int = 0,
        include_counts: bool = True,
        order: str = "count_desc",
        include_null: bool = False,
    ) -> Dict[str, Any]:
        limit = min(limit, self.settings.TOOL_MAX_DISTINCT_VALUES)
        meta = await self._ensure_owned(result_id)
        df = await self._load_full_df(result_id, columns=[column])
        schema = {name: df[name].dtype for name in df.columns}
        if filters and filters.clauses:
            mask = _build_filter_expr(filters, schema)
            df = df.filter(mask)
        # Compute value counts
        if include_null:
            # Polars group_by includes nulls by default
            value_counts = df.group_by(column).agg(pl.len().alias("count"))
        else:
            value_counts = df.drop_nulls().group_by(column).agg(pl.len().alias("count"))
        # Order
        if order == "count_desc":
            value_counts = value_counts.sort("count", descending=True)
        elif order == "count_asc":
            value_counts = value_counts.sort("count", descending=False)
        elif order == "value_asc":
            value_counts = value_counts.sort(column, descending=False)
        elif order == "value_desc":
            value_counts = value_counts.sort(column, descending=True)
        else:
            raise tool_error("INVALID_ARGUMENT", f"Unsupported order '{order}'.")
        total_distinct = value_counts.height
        value_counts_slice = value_counts.slice(offset, limit)
        values = []
        for row in value_counts_slice.iter_rows(named=True):
            entry = {"value": _normalize_value(row[column])}
            if include_counts:
                entry["count"] = row["count"]
            values.append(entry)
        has_more = offset + limit < total_distinct
        next_offset = offset + limit if has_more else None
        return {
            "result_id": result_id,
            "column": column,
            "returned": len(values),
            "total_distinct": total_distinct,
            "has_more": has_more,
            "next_offset": next_offset,
            "values": values,
        }

    async def aggregate(
        self,
        result_id: str,
        aggregations: List[AggregationSpec],
        filters: Optional[FilterGroup] = None,
    ) -> Dict[str, Any]:
        meta = await self._ensure_owned(result_id)
        # Determine needed columns
        needed_cols = set()
        for agg in aggregations:
            if agg.column:
                needed_cols.add(agg.column)
        df = await self._load_full_df(result_id, columns=list(needed_cols) if needed_cols else None)
        schema = {name: df[name].dtype for name in df.columns}
        if filters and filters.clauses:
            mask = _build_filter_expr(filters, schema)
            df = df.filter(mask)
        # Build aggregation expressions
        agg_exprs = _build_agg_exprs(aggregations)
        result_df = df.select(agg_exprs)
        row = result_df.to_dicts()[0]
        normalized = {k: _normalize_value(v) for k, v in row.items()}
        return {"result_id": result_id, "aggregations": normalized}

    async def group_by(
        self,
        result_id: str,
        group_columns: List[str],
        aggregations: List[AggregationSpec],
        filters: Optional[FilterGroup] = None,
        sort: Optional[List[SortSpec]] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        limit = min(limit, self.settings.TOOL_MAX_ROWS)
        if len(group_columns) > self.settings.TOOL_MAX_GROUP_COLUMNS:
            raise tool_error("INVALID_ARGUMENT", f"Too many group columns (max {self.settings.TOOL_MAX_GROUP_COLUMNS}).")
        meta = await self._ensure_owned(result_id)
        # Determine needed columns
        needed_cols = set(group_columns)
        for agg in aggregations:
            if agg.column:
                needed_cols.add(agg.column)
        df = await self._load_full_df(result_id, columns=list(needed_cols))
        schema = {name: df[name].dtype for name in df.columns}
        if filters and filters.clauses:
            mask = _build_filter_expr(filters, schema)
            df = df.filter(mask)
        # Build aggregation expressions
        agg_exprs = _build_agg_exprs(aggregations)
        # Group by
        grouped = df.group_by(group_columns).agg(agg_exprs)
        # Enforce max groups to prevent memory exhaustion from high-cardinality groupings.
        if grouped.height > self.settings.RESULTSET_MAX_GROUPS:
            raise tool_error(
                "TOO_MANY_GROUPS",
                f"Group cardinality {grouped.height} exceeds limit {self.settings.RESULTSET_MAX_GROUPS}."
            )
        total_groups = grouped.height
        if sort:
            grouped = _apply_sort(grouped, sort)
        grouped_slice = grouped.slice(offset, limit)
        rows = _df_to_rows(grouped_slice)
        has_more = offset + limit < total_groups
        next_offset = offset + limit if has_more else None
        return {
            "result_id": result_id,
            "group_columns": group_columns,
            "offset": offset,
            "limit": limit,
            "returned": len(rows),
            "total_groups": total_groups,
            "has_more": has_more,
            "next_offset": next_offset,
            "rows": rows,
        }

    async def histogram(
        self,
        result_id: str,
        column: str,
        bins: int = 10,
        filters: Optional[FilterGroup] = None,
    ) -> Dict[str, Any]:
        bins = min(bins, 50)
        meta = await self._ensure_owned(result_id)
        df = await self._load_full_df(result_id, columns=[column])
        schema = {name: df[name].dtype for name in df.columns}
        if filters and filters.clauses:
            mask = _build_filter_expr(filters, schema)
            df = df.filter(mask)
        
        non_null = df.drop_nulls()
        null_count = df.height - non_null.height
        
        if non_null.height == 0:
            return {
                "result_id": result_id,
                "column": column,
                "bins": 0,
                "null_count": null_count,
                "histogram": [],
            }

        col_type = non_null[column].dtype
        
        if col_type in (pl.Utf8, pl.Categorical):
            # String histogram: top N values by count
            max_bins = min(bins, self.settings.TOOL_MAX_DISTINCT_VALUES)
            value_counts = non_null.group_by(column).agg(pl.len().alias("count"))
            value_counts = value_counts.sort("count", descending=True)
            
            total_distinct = value_counts.height
            top_values = value_counts.slice(0, max_bins)
            
            histogram = []
            for row in top_values.iter_rows(named=True):
                histogram.append({
                    "value": _normalize_value(row[column]),
                    "count": row["count"],
                })
                
            other_count = 0
            if total_distinct > max_bins:
                other_count = non_null.height - top_values["count"].sum()
                
            return {
                "result_id": result_id,
                "column": column,
                "bins": len(histogram),
                "null_count": null_count,
                "histogram": histogram,
                "other_count": other_count,
                "total_distinct": total_distinct,
            }
        else:
            # Numeric histogram
            col_min = non_null[column].min()
            col_max = non_null[column].max()
            if col_min is None or col_max is None:
                return {
                    "result_id": result_id,
                    "column": column,
                    "bins": 0,
                    "null_count": null_count,
                    "histogram": [],
                }
            try:
                hist_df = non_null[column].hist(bin_count=bins)
            except pl.exceptions.InvalidOperationError:
                raise tool_error(
                    "INVALID_ARGUMENT",
                    f"Column '{column}' is not numeric and cannot be used for histograms."
                )
            
            histogram = []
            for row in hist_df.iter_rows(named=True):
                histogram.append({
                    "upper_bound": _normalize_value(row["breakpoint"]),
                    "count": row["count"],
                })
            return {
                "result_id": result_id,
                "column": column,
                "bins": len(histogram),
                "null_count": null_count,
                "histogram": histogram,
            }

    async def delete_result_set(self, result_id: str) -> Dict[str, Any]:
        meta = await self._ensure_owned(result_id)
        deleted = await self.store.delete_result(self.session_id, result_id)
        return {"result_id": result_id, "deleted": deleted}
