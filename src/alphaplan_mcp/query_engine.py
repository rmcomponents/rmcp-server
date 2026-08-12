# Implements the shared QueryEngine for safe filtering, grouping,
# sorting, and aggregation on materialized result sets using Polars,
# without constructing arbitrary SQL.
"""Polars‑based query engine for stored result sets."""

import base64
import gzip
import hashlib
import json
import logging
import math
import re
from collections import Counter, OrderedDict
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
    ColumnOrigin,
    FilterClause,
    FilterGroup,
    JoinColumnPair,
    JoinDataQualityReport,
    JoinDryRunReport,
    JoinNormalizationReport,
    JoinReport,
    JoinResultSummary,
    ResultSetMetadata,
    ResultSetSummary,
    ResultProvenance,
    SortSpec,
)
from .result_store import ResultSetStore
from .utils import NORMALIZE_STEPS, normalize_mpn, normalize_mpn_with_steps

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


def _cast_value(value: Any, dtype: pl.DataType) -> Any:
    """Attempt to cast a Python value to match a Polars column dtype."""
    if value is None:
        return None
    if isinstance(value, list):
        return [_cast_value(v, dtype) for v in value]
    
    if dtype in (pl.Utf8, pl.Categorical):
        if not isinstance(value, str):
            return str(value)
    elif dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                raise tool_error("TYPE_MISMATCH", f"Cannot convert '{value}' to integer for column comparison")
    elif dtype in (pl.Float32, pl.Float64):
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                raise tool_error("TYPE_MISMATCH", f"Cannot convert '{value}' to float for column comparison")
    elif dtype == pl.Boolean:
        if isinstance(value, str):
            if value.lower() in ("true", "1"):
                return True
            elif value.lower() in ("false", "0"):
                return False
            raise tool_error("TYPE_MISMATCH", f"Cannot convert '{value}' to boolean for column comparison")
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
    value = _cast_value(clause.value, dtype)
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
    elif op in ("regex", "not_regex"):
        if not _is_string_dtype(dtype):
            raise tool_error("INVALID_ARGUMENT", f"Operator '{op}' requires a string column.")
        if not isinstance(value, str):
            raise tool_error("INVALID_ARGUMENT", f"Operator '{op}' requires a string pattern.")
        flags = 0 if case_sensitive else re.IGNORECASE
        try:
            re.compile(value, flags)
        except re.error as exc:
            raise tool_error("INVALID_ARGUMENT", f"Invalid regular expression: {exc}", {"pattern": value})
        full = value if case_sensitive else f"(?i){value}"
        match = col_expr.str.contains(full, literal=False)
        return match if op == "regex" else ~match
    elif op in ("matches_charset", "not_matches_charset"):
        if not _is_string_dtype(dtype):
            raise tool_error("INVALID_ARGUMENT", f"Operator '{op}' requires a string column.")
        if not isinstance(value, str) or not value:
            raise tool_error("INVALID_ARGUMENT", f"Operator '{op}' requires a non-empty set of allowed characters.")
        # `value` is the set of allowed characters (each regex-escaped, so ranges are literal).
        # Build ^[\Q...\E]*$ equivalent; case-insensitivity via the (?i) inline flag.
        cls = _charset_class(value)
        full = f"^(?i:[{cls}])*$" if not case_sensitive else f"^[{cls}]*$"
        match = col_expr.str.contains(full, literal=False)
        return match if op == "matches_charset" else ~match
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


def _charset_class(allowed: str) -> str:
    """Turn a set of allowed characters into a regex character-class body.

    Each character is regex-escaped so that ranged/meta characters (``-``, ``]``, ``\\``,
    ``^``) are treated literally rather than forming a class range or escape.
    """
    return "".join(re.escape(c) for c in allowed)


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


def _sha256_b64(text: str) -> str:
    """Return Base64(SHA-256(UTF-8 bytes of text))."""
    return base64.b64encode(hashlib.sha256(text.encode("utf-8")).digest()).decode("ascii")


# ---------------------------------------------------------------------------
# helpers for aggregation and pagination
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# aggregation operation vocabulary
# ---------------------------------------------------------------------------

# Operations that count rows / non-null / per-value predicates. Their result type is int64.
COUNT_OPS = {"count", "count_non_null", "count_distinct", "nunique", "count_null", "count_zero", "count_negative", "count_positive"}

# Operations whose result type is float64 (regardless of source column type).
FLOAT_OPS = {
    "avg", "mean", "median", "percentile", "stddev", "variance", "iqr", "mad",
    "trimmed_mean", "skewness", "kurtosis", "coefficient_of_variation",
    "weighted_avg", "ratio",
}

# The full, discoverable operation vocabulary (used for INVALID_AGGREGATION details).
KNOWN_OPERATIONS = sorted(COUNT_OPS | FLOAT_OPS | {"sum", "min", "max", "mode"})

_QUANTILE_METHODS = {"linear", "lower", "higher", "nearest", "midpoint"}


def _build_agg_expr(agg: AggregationSpec) -> pl.Expr:
    """Translate a single AggregationSpec into a Polars aggregation expression."""
    op = agg.operation.lower()
    alias = agg.alias

    if op == "count":
        if agg.column:
            raise tool_error("INVALID_AGGREGATION", "count does not accept a column.",
                             {"available_operations": KNOWN_OPERATIONS})
        return pl.len().alias(alias)

    if not agg.column:
        raise tool_error("INVALID_AGGREGATION",
                         f"{op} requires a column.",
                         {"available_operations": KNOWN_OPERATIONS})

    col = pl.col(agg.column)

    if op == "count_non_null":
        expr = col.count()
    elif op in ("count_distinct", "nunique"):
        expr = col.n_unique()
    elif op == "sum":
        expr = col.sum()
    elif op == "avg" or op == "mean":
        expr = col.mean()
    elif op == "min":
        expr = col.min()
    elif op == "max":
        expr = col.max()
    elif op == "median":
        expr = col.median()
    elif op == "percentile":
        if agg.q is None:
            raise tool_error("INVALID_AGGREGATION",
                             "percentile requires a 'q' parameter between 0 and 1.",
                             {"available_operations": KNOWN_OPERATIONS})
        if not (0.0 <= agg.q <= 1.0):
            raise tool_error("INVALID_AGGREGATION",
                             f"percentile 'q' must be between 0 and 1, got {agg.q}.",
                             {"available_operations": KNOWN_OPERATIONS})
        if agg.method not in _QUANTILE_METHODS:
            raise tool_error("INVALID_AGGREGATION",
                             f"percentile method '{agg.method}' invalid; must be one of {sorted(_QUANTILE_METHODS)}.",
                             {"available_operations": KNOWN_OPERATIONS})
        expr = col.quantile(agg.q, interpolation=agg.method)
    elif op == "stddev":
        expr = col.std(ddof=agg.ddof)
    elif op == "variance":
        expr = col.var(ddof=agg.ddof)
    elif op == "mode":
        expr = col.mode().first()
    elif op == "iqr":
        expr = col.quantile(0.75, interpolation="linear") - col.quantile(0.25, interpolation="linear")
    elif op == "mad":
        # median absolute deviation: median(|x - median(x)|). All aggregation exprs skip nulls.
        expr = (col - col.median()).abs().median()
    elif op == "trimmed_mean":
        if agg.trim is None:
            raise tool_error("INVALID_AGGREGATION",
                             "trimmed_mean requires a 'trim' fraction between 0 and 0.5.",
                             {"available_operations": KNOWN_OPERATIONS})
        if not (0.0 <= agg.trim < 0.5):
            raise tool_error("INVALID_AGGREGATION",
                             f"trimmed_mean 'trim' must be between 0 (inclusive) and 0.5 (exclusive), got {agg.trim}.",
                             {"available_operations": KNOWN_OPERATIONS})
        c = col.drop_nulls()
        lo = c.quantile(agg.trim, interpolation="linear")
        hi = c.quantile(1.0 - agg.trim, interpolation="linear")
        expr = c.filter((c >= lo) & (c <= hi)).mean()
    elif op == "skewness":
        expr = col.skew()
    elif op == "kurtosis":
        expr = col.kurtosis()
    elif op == "coefficient_of_variation":
        expr = col.std(ddof=1) / col.mean()
    elif op == "count_null":
        expr = col.is_null().sum()
    elif op == "count_zero":
        expr = (col == 0).fill_null(False).sum()
    elif op == "count_negative":
        expr = (col < 0).fill_null(False).sum()
    elif op == "count_positive":
        expr = (col > 0).fill_null(False).sum()
    elif op == "weighted_avg":
        if not agg.weight_column:
            raise tool_error("INVALID_AGGREGATION",
                             "weighted_avg requires a 'weight_column'.",
                             {"available_operations": KNOWN_OPERATIONS})
        w = pl.col(agg.weight_column)
        expr = (col.fill_null(0) * w.fill_null(0)).sum() / w.fill_null(0).sum()
    elif op == "ratio":
        if not agg.denominator_column:
            raise tool_error("INVALID_AGGREGATION",
                             "ratio requires a 'denominator_column'.",
                             {"available_operations": KNOWN_OPERATIONS})
        expr = col.sum() / pl.col(agg.denominator_column).sum()
    else:
        raise tool_error("INVALID_AGGREGATION", f"Unsupported operation '{op}'.",
                         {"available_operations": KNOWN_OPERATIONS})

    return expr.alias(alias)


def _build_agg_exprs(aggregations: List[AggregationSpec]) -> List[pl.Expr]:
    """Translate AggregationSpecs into Polars aggregation expressions.

    Note: For chunked processing, avg is computed as sum/count. Polars handles
    this correctly when aggregating a full DataFrame, but if we ever stream
    chunks, we must aggregate sum and count separately to maintain numerical correctness.
    """
    if not aggregations:
        raise tool_error("INVALID_ARGUMENT", "At least one aggregation is required.")

    agg_exprs = []
    for agg in aggregations:
        agg_exprs.append(_build_agg_expr(agg))

    if not agg_exprs:
        raise tool_error("INVALID_ARGUMENT", "At least one aggregation is required.")

    return agg_exprs


def _agg_result_type(agg: AggregationSpec, parent_meta: Optional[ResultSetMetadata]) -> str:
    """Infer the stored column type for an aggregation's result (for store_result)."""
    op = agg.operation.lower()
    if op in COUNT_OPS:
        return "int64"
    if op in FLOAT_OPS:
        return "float64"
    # sum / min / max / mode inherit the source column's numeric (or string) type.
    p_col = None
    if parent_meta is not None and agg.column:
        p_col = next((c for c in parent_meta.columns if c.name == agg.column), None)
    agg_type = p_col.type if p_col else "float64"
    if agg_type == "large_string":
        agg_type = "string"
    return agg_type


_ALIAS_REGEX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_aliases(aggregations: List[AggregationSpec], group_columns: Optional[List[str]] = None) -> None:
    seen = set(group_columns) if group_columns else set()
    for agg in aggregations:
        alias = agg.alias
        if not _ALIAS_REGEX.match(alias):
            raise tool_error(
                "INVALID_ALIAS",
                f"Invalid alias '{alias}'. Must match ^[A-Za-z_][A-Za-z0-9_]*$ and not contain whitespace, dots, or quotes."
            )
        if alias in seen:
            raise tool_error(
                "ALIAS_COLLISION",
                f"Alias '{alias}' collides with a group column or another alias."
            )
        seen.add(alias)


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
# join helpers
# ---------------------------------------------------------------------------

def _is_string_dtype(dtype) -> bool:
    """Whether a Polars dtype is a string-like type."""
    str_type = getattr(pl, "String", None) or getattr(pl, "Utf8", None)
    return dtype == str_type or dtype == pl.Categorical


def _apply_suffix(name: str, suffix: str) -> str:
    return f"{name}{suffix}"


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
        provenance: ResultProvenance,
        columns: Optional[List[ColumnInfo]] = None,
        source: Optional[Dict[str, Any]] = None,
        grain: Optional[str] = None,
        source_row_count: Optional[int] = None,
        result_label: Optional[str] = None,
    ) -> ResultSetSummary:
        """Convert a Polars DataFrame to a PyArrow Table and store it as a new result set."""
        table = df.to_arrow()
        return await self.store.store_result(
            session_id=self.session_id,
            provenance=provenance,
            table=table,
            columns=columns,
            source=source,
            grain=grain,
            source_row_count=source_row_count,
            result_label=result_label,
        )

    # ---------- Tool helpers that will be called by MCP tools ----------

    def _get_description(self, col_info: ColumnInfo, meta: ResultSetMetadata) -> Optional[str]:
        if col_info.description:
            return col_info.description
        op = meta.provenance.op
        if col_info.origin == ColumnOrigin.group_key:
            return f"{op} group key: {col_info.source_column}"
        elif col_info.origin == ColumnOrigin.aggregate:
            if col_info.operation == "count" and col_info.source_column is None:
                if op == "aggregate":
                    return f"{op} aggregation: total row count"
                return f"{op} aggregation: count of rows"
            return f"{op} aggregation: {col_info.operation} of {col_info.source_column}"
        elif col_info.origin == ColumnOrigin.derived:
            return f"{op} derived: {col_info.name}"
        elif col_info.origin == ColumnOrigin.passthrough:
            if col_info.source_column:
                return f"{op} passthrough: {col_info.source_column}"
            return None
        return None

    async def get_result_info(self, result_id: str) -> Dict[str, Any]:
        meta = await self._ensure_owned(result_id)
        # Return only safe fields (no owner_session_id)
        return {
            "result_id": meta.result_id,
            "provenance": meta.provenance.model_dump(),
            "source": meta.source,
            "grain": meta.grain,
            "source_row_count": meta.source_row_count,
            "result_label": meta.result_label,
            "row_count": meta.row_count,
            "column_count": meta.column_count,
            "stored_size_bytes": meta.stored_size_bytes,
            "created_at": meta.created_at.isoformat(),
            "expires_at": meta.absolute_expires_at.isoformat(),
            "columns": [
                {**c.model_dump(), "description": self._get_description(c, meta)}
                for c in meta.columns
            ],
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
        if include_examples:
            df = await self._load_full_df(result_id, columns=columns)

        for col_name in columns:
            col_info = all_columns[col_name]
            col_entry = {
                "name": col_info.name,
                "type": col_info.type,
                "description": self._get_description(col_info, meta),
                "null_count": col_info.null_count,
                "non_null_count": col_info.non_null_count,
                "min": col_info.min,
                "max": col_info.max,
                "sum": col_info.sum,
                "avg": col_info.avg,
                "min_length": col_info.min_length,
                "max_length": col_info.max_length,
            }

            if include_examples:
                # compute distinct count if possible
                try:
                    distinct_count = df[col_name].n_unique()
                    col_entry["distinct_count"] = distinct_count
                except Exception:
                    col_entry["distinct_count"] = None

                # examples (up to 3 distinct non-empty values)
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

        return {
            "result_id": result_id,
            "grain": meta.grain,
            "source_row_count": meta.source_row_count,
            "columns": result,
        }

    async def describe_columns(
        self,
        result_id: str,
        columns: Optional[List[str]] = None,
        percentiles: Optional[List[float]] = None,
        filters: Optional[FilterGroup] = None,
        group_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Return a distribution summary bundle per column.

        The "what am I even looking at" counterpart to `explain_columns` (which covers the
        schema side). For each requested column it computes `count`, `count_null`, `mean`,
        `stddev`, `min`, the requested percentiles (linear interpolation), `max`, `skewness`
        and `count_negative` (how many rows are below zero — useful for spotting credits /
        corrections). Non-numeric columns report `count`, `count_null` and `distinct_count`
        instead. Optional `filters` and `group_columns` let the bundle be computed per subset.
        """
        meta = await self._ensure_owned(result_id)
        all_columns = {c.name: c for c in meta.columns}
        if columns is None:
            columns = list(all_columns.keys())
        else:
            for c in columns:
                if c not in all_columns:
                    raise tool_error("UNKNOWN_COLUMN", f"Column '{c}' does not exist.",
                                     {"available_columns": list(all_columns.keys())})

        if percentiles is None:
            percentiles = [0.05, 0.25, 0.5, 0.75, 0.95]
        for q in percentiles:
            if not (0.0 <= q <= 1.0):
                raise tool_error("INVALID_ARGUMENT",
                                 f"Percentile value must be between 0 and 1, got {q}.")

        if group_columns and len(group_columns) > self.settings.TOOL_MAX_GROUP_COLUMNS:
            raise tool_error("INVALID_ARGUMENT",
                             f"Too many group columns (max {self.settings.TOOL_MAX_GROUP_COLUMNS}).")

        needed_cols = set(columns)
        if group_columns:
            needed_cols.update(group_columns)
            for gc in group_columns:
                if gc not in all_columns:
                    raise tool_error("UNKNOWN_COLUMN", f"Column '{gc}' does not exist.",
                                     {"available_columns": list(all_columns.keys())})
        if filters and filters.clauses:
            for clause in filters.clauses:
                needed_cols.add(clause.column)

        df = await self._load_full_df(result_id, columns=list(needed_cols))
        schema = {name: df[name].dtype for name in df.columns}
        if filters and filters.clauses:
            mask = _build_filter_expr(filters, schema)
            df = df.filter(mask)

        def _type_of(col_name: str) -> str:
            t = all_columns[col_name].type
            return "string" if t == "large_string" else t

        target_cols = [c for c in columns]
        if group_columns:
            rows = []
            for keys, sub in df.group_by(group_columns):
                key_row = {}
                for gc, k in zip(group_columns, keys):
                    key_row[gc] = _normalize_value(k)
                key_row["columns"] = {
                    c: self._describe_series(c, sub[c], percentiles) for c in target_cols
                }
                rows.append(key_row)
            return {
                "result_id": result_id,
                "grain": f"one row per ({', '.join(group_columns)})",
                "percentiles": percentiles,
                "column_types": {c: _type_of(c) for c in target_cols},
                "groups": rows,
            }

        return {
            "result_id": result_id,
            "grain": "single summary row",
            "percentiles": percentiles,
            "columns": [
                self._describe_series(c, df[c], percentiles) for c in target_cols
            ],
        }

    def _describe_series(self, name: str, series: pl.Series, percentiles: List[float]) -> Dict[str, Any]:
        """Compute the distribution bundle for a single column's values."""
        null_count = int(series.null_count())
        count = int(series.len()) - null_count
        dtype = series.dtype
        if dtype in (pl.Utf8, pl.Categorical):
            return {
                "name": name,
                "kind": "string",
                "count": count,
                "count_null": null_count,
                "distinct_count": int(series.n_unique()),
            }
        if dtype == pl.Boolean:
            return {
                "name": name,
                "kind": "bool",
                "count": count,
                "count_null": null_count,
                "true_count": int(series.fill_null(False).sum()),
            }
        nn = series.drop_nulls()
        is_numeric = dtype.is_numeric()
        stats = {
            "name": name,
            "kind": "numeric" if is_numeric else "temporal",
            "count": count,
            "count_null": null_count,
        }
        stats["mean"] = _normalize_value(nn.mean())
        stats["stddev"] = _normalize_value(nn.std())
        stats["min"] = _normalize_value(nn.min())
        stats["max"] = _normalize_value(nn.max())
        if is_numeric:
            stats["skewness"] = _normalize_value(nn.skew())
            stats["count_negative"] = int((nn < 0).sum())
            stats["percentiles"] = {
                str(q): _normalize_value(nn.quantile(q, interpolation="linear")) for q in percentiles
            }
        return stats

    async def list_rows(
        self,
        result_id: str,
        columns: Optional[List[str]] = None,
        limit: int = 50,
        offset: int = 0,
        sort: Optional[List[SortSpec]] = None,
        store_result: bool = False,
        format: str = "json",
        encoding: str = "plain",
        round_to: Optional[int] = None,
        stable_sort: bool = True,
    ) -> Union[Dict[str, Any], ResultSetSummary]:
        limit = min(limit, self.settings.TOOL_MAX_ROWS)
        if offset < 0:
            offset = 0
        if limit < 0:
            limit = 0
        meta = await self._ensure_owned(result_id)

        # Validate format and encoding
        if format not in {"json", "csv"}:
            raise tool_error("INVALID_FORMAT", f"Unsupported format '{format}'. Must be 'json' or 'csv'.")
        if encoding not in {"plain", "gzip+base64"}:
            raise tool_error("INVALID_ENCODING", f"Unsupported encoding '{encoding}'. Must be 'plain' or 'gzip+base64'.")

        # Determine stable sort tie-breakers
        if stable_sort and sort:
            # Add all other columns as tie-breakers in alphabetical order
            existing_sort_cols = {s.column for s in sort}
            additional_sorts = []
            for c in sorted(meta.columns, key=lambda x: x.name):
                if c.name not in existing_sort_cols:
                    additional_sorts.append(SortSpec(column=c.name, direction="asc"))
            effective_sort = sort + additional_sorts
        else:
            effective_sort = sort

        if store_result:
            # Load full table if sorting, to ensure sort columns are present before projection
            df = await self._load_full_df(result_id) if effective_sort else await self._load_full_df(result_id, columns=columns)
            if effective_sort:
                df = _apply_sort(df, effective_sort)
            if columns:
                df = df.select(columns)
            # Do not slice by limit/offset when storing the result
            parent_meta = await self.store.get_metadata(result_id)
            cols_info = []
            for c in df.columns:
                p_col = next((pc for pc in parent_meta.columns if pc.name == c), None)
                p_type = p_col.type if p_col else "string"
                if p_type == "large_string":
                    p_type = "string"
                cols_info.append(ColumnInfo(
                    name=c, type=p_type, null_count=0, non_null_count=0,
                    description=p_col.description if p_col else None,
                    origin=ColumnOrigin.passthrough, source_column=c
                ))
            return await self._store_result_from_df(
                df,
                provenance=ResultProvenance(
                    op="list_rows",
                    parent=result_id,
                    parameters={
                        "columns": columns,
                        "sort": [s.model_dump() for s in sort] if sort else None,
                    },
                ),
                columns=cols_info,
                source={"op": "list_rows", "parent": result_id, "columns": columns, "sort": [s.model_dump() for s in sort] if sort else None},
                grain="one row per source row",
                source_row_count=parent_meta.row_count,
            )

        if effective_sort:
            # Load full table, sort, then slice
            df = await self._load_full_df(result_id) # Load all columns
            df = _apply_sort(df, effective_sort)
            if columns:
                df = df.select(columns)
            total_rows = df.height
            df_slice = df.slice(offset, limit)
        else:
            # Use range loading to load only the needed rows
            total_rows = meta.row_count
            if limit == 0 or offset >= total_rows:
                df_slice = pl.DataFrame()
            else:
                start = offset
                end = min(offset + limit, total_rows)
                df_slice = await self._load_range_df(result_id, start, end, columns=columns)

        # Apply rounding if requested
        if round_to is not None:
            for col in df_slice.columns:
                if df_slice[col].dtype in (pl.Float32, pl.Float64):
                    if round_to == 0:
                        df_slice = df_slice.with_columns((pl.col(col) + 1e-12).round(round_to).cast(pl.Int64))
                    else:
                        rounded_expr = (pl.col(col) + 1e-12).round(round_to)
                        df_slice = df_slice.with_columns(rounded_expr.alias(col))
                        # If all non-null values are integral, cast the whole column to Int64
                        if (df_slice[col].drop_nulls() % 1 == 0).all():
                            df_slice = df_slice.with_columns(df_slice[col].cast(pl.Int64))

        # Build base response
        has_more = offset + limit < total_rows
        next_offset = offset + limit if has_more else None
        base_response = {
            "result_id": result_id,
            "offset": offset,
            "limit": limit,
            "total_rows": total_rows,
            "has_more": has_more,
            "next_offset": next_offset,
        }

        # Format response data
        if format == "json" and encoding == "plain":
            rows = _df_to_rows(df_slice)
            base_response["format"] = "json"
            base_response["encoding"] = "plain"
            base_response["data"] = rows
            base_response["returned"] = len(rows)
            return base_response

        # Handle CSV or gzip+base64
        if format == "csv":
            plain_payload_str = df_slice.write_csv()
        else:  # json
            rows = _df_to_rows(df_slice)
            plain_payload_str = json.dumps(rows, default=str)

        csv_sha256_base64 = _sha256_b64(plain_payload_str) if format == "csv" else None

        if encoding == "gzip+base64":
            payload_bytes = plain_payload_str.encode("utf-8")
            compressed = gzip.compress(payload_bytes)
            import hashlib
            sha256 = hashlib.sha256(payload_bytes).hexdigest()
            uncompressed_bytes = len(payload_bytes)
            
            # Fallback to plain if compressed is larger
            if len(compressed) < len(payload_bytes):
                gzip_payload_str = base64.b64encode(compressed).decode("utf-8")
                base_response["format"] = format
                base_response["encoding"] = "gzip+base64"
                base_response["data"] = gzip_payload_str
                base_response["returned"] = df_slice.height
                base_response["uncompressed_bytes"] = uncompressed_bytes
                if format == "csv":
                    base_response["sha256_base64"] = csv_sha256_base64
                else:
                    base_response["sha256"] = sha256
                return base_response
            else:
                base_response["format"] = format
                base_response["encoding"] = "plain"
                base_response["returned"] = df_slice.height
                base_response["uncompressed_bytes"] = uncompressed_bytes
                base_response["sha256"] = sha256
                if format == "csv":
                    base_response["data"] = plain_payload_str
                    base_response["sha256_base64"] = csv_sha256_base64
                else:
                    rows = _df_to_rows(df_slice)
                    base_response["data"] = rows
                    base_response["sha256"] = sha256
                return base_response

        # Plain response (originally plain)
        base_response["format"] = format
        base_response["encoding"] = encoding
        base_response["returned"] = df_slice.height
        if format == "csv":
            base_response["data"] = plain_payload_str
            base_response["sha256_base64"] = csv_sha256_base64
        else:
            rows = _df_to_rows(df_slice)
            base_response["data"] = rows
        return base_response

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

        # If columns is None, we need to load all columns for projection
        load_cols = list(needed_cols) if columns else None
        df = await self._load_full_df(result_id, columns=load_cols)
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
            # Do not slice by limit/offset when storing the result
            parent_meta = await self.store.get_metadata(result_id)
            cols_info = []
            for c in df.columns:
                p_col = next((pc for pc in parent_meta.columns if pc.name == c), None)
                p_type = p_col.type if p_col else "string"
                if p_type == "large_string":
                    p_type = "string"
                cols_info.append(ColumnInfo(
                    name=c, type=p_type, null_count=0, non_null_count=0,
                    description=p_col.description if p_col else None,
                    origin=ColumnOrigin.passthrough, source_column=c
                ))
            return await self._store_result_from_df(
                df,
                provenance=ResultProvenance(
                    op="query_rows",
                    parent=result_id,
                    parameters={
                        "filters": filters.model_dump() if filters else None,
                        "columns": columns,
                        "sort": [s.model_dump() for s in sort] if sort else None,
                    },
                ),
                columns=cols_info,
                source={"op": "query_rows", "parent": result_id, "filters": filters.model_dump() if filters else None, "columns": columns, "sort": [s.model_dump() for s in sort] if sort else None},
                grain="one row per source row",
                source_row_count=parent_meta.row_count,
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
        store_result: bool = False,
        result_label: Optional[str] = None,
    ) -> Union[Dict[str, Any], ResultSetSummary]:
        limit = min(limit, self.settings.TOOL_MAX_DISTINCT_VALUES)
        meta = await self._ensure_owned(result_id)
        parent_meta = await self.store.get_metadata(result_id)
        needed_cols = {column}
        if filters and filters.clauses:
            for clause in filters.clauses:
                needed_cols.add(clause.column)
        df = await self._load_full_df(result_id, columns=list(needed_cols))
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

        if store_result:
            p_col = next((c for c in parent_meta.columns if c.name == column), None)
            p_type = p_col.type if p_col else "string"
            if p_type == "large_string":
                p_type = "string"
            cols_info = [
                ColumnInfo(name=column, type=p_type, null_count=0, non_null_count=0, description=p_col.description if p_col else f"distinct_values passthrough: {column}", origin=ColumnOrigin.passthrough, source_column=column),
                ColumnInfo(name="count", type="int64", null_count=0, non_null_count=0, description="distinct_values derived: count", origin=ColumnOrigin.derived)
            ]
            return await self._store_result_from_df(
                value_counts,
                provenance=ResultProvenance(
                    op="distinct_values",
                    parent=result_id,
                    parameters={
                        "column": column,
                        "filters": filters.model_dump() if filters else None,
                    },
                ),
                columns=cols_info,
                source={"op": "distinct_values", "parent": result_id, "column": column, "filters": filters.model_dump() if filters else None},
                grain=f"one row per distinct {column}",
                source_row_count=parent_meta.row_count,
                result_label=result_label,
            )

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
        store_result: bool = False,
        result_label: Optional[str] = None,
    ) -> Union[Dict[str, Any], ResultSetSummary]:
        meta = await self._ensure_owned(result_id)
        parent_meta = await self.store.get_metadata(result_id)
        _validate_aliases(aggregations)
        # Determine needed columns
        needed_cols = set()
        for agg in aggregations:
            if agg.column:
                needed_cols.add(agg.column)
            if agg.weight_column:
                needed_cols.add(agg.weight_column)
            if agg.denominator_column:
                needed_cols.add(agg.denominator_column)
        if filters and filters.clauses:
            for clause in filters.clauses:
                needed_cols.add(clause.column)
        df = await self._load_full_df(result_id, columns=list(needed_cols) if needed_cols else None)
        schema = {name: df[name].dtype for name in df.columns}
        if filters and filters.clauses:
            mask = _build_filter_expr(filters, schema)
            df = df.filter(mask)
        # Build aggregation expressions
        agg_exprs = _build_agg_exprs(aggregations)
        result_df = df.select(agg_exprs)

        if store_result:
            cols_info = []
            for agg in aggregations:
                agg_type = _agg_result_type(agg, parent_meta)
                desc = f"aggregate aggregation: {agg.operation} of {agg.column}"
                if agg.operation == "count" and agg.column is None:
                    desc = "aggregate aggregation: total row count"
                cols_info.append(ColumnInfo(
                    name=agg.alias, type=agg_type, null_count=0, non_null_count=0,
                    description=desc,
                    origin=ColumnOrigin.aggregate, operation=agg.operation, source_column=agg.column
                ))
            return await self._store_result_from_df(
                result_df,
                provenance=ResultProvenance(
                    op="aggregate",
                    parent=result_id,
                    parameters={
                        "aggregations": [a.model_dump() for a in aggregations],
                        "filters": filters.model_dump() if filters else None,
                    },
                ),
                columns=cols_info,
                source={"op": "aggregate", "parent": result_id, "aggregations": [a.model_dump() for a in aggregations], "filters": filters.model_dump() if filters else None},
                grain="single row",
                source_row_count=parent_meta.row_count,
                result_label=result_label,
            )

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
        store_result: bool = False,
        result_label: Optional[str] = None,
    ) -> Union[Dict[str, Any], ResultSetSummary]:
        limit = min(limit, self.settings.TOOL_MAX_ROWS)
        if len(group_columns) > self.settings.TOOL_MAX_GROUP_COLUMNS:
            raise tool_error("INVALID_ARGUMENT", f"Too many group columns (max {self.settings.TOOL_MAX_GROUP_COLUMNS}).")
        meta = await self._ensure_owned(result_id)
        parent_meta = await self.store.get_metadata(result_id)
        _validate_aliases(aggregations, group_columns)
        # Determine needed columns
        needed_cols = set(group_columns)
        for agg in aggregations:
            if agg.column:
                needed_cols.add(agg.column)
            if agg.weight_column:
                needed_cols.add(agg.weight_column)
            if agg.denominator_column:
                needed_cols.add(agg.denominator_column)
        if filters and filters.clauses:
            for clause in filters.clauses:
                needed_cols.add(clause.column)
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

        if store_result:
            # Do not slice by limit/offset when storing the result
            cols_info = []
            for gc in group_columns:
                p_col = next((c for c in parent_meta.columns if c.name == gc), None)
                p_type = p_col.type if p_col else "string"
                if p_type == "large_string":
                    p_type = "string"
                cols_info.append(ColumnInfo(
                    name=gc, type=p_type, null_count=0, non_null_count=0,
                    description=p_col.description if p_col else f"group_by group key: {gc}",
                    origin=ColumnOrigin.group_key, source_column=gc
                ))
            for agg in aggregations:
                agg_type = _agg_result_type(agg, parent_meta)
                desc = f"group_by aggregation: {agg.operation} of {agg.column}"
                if agg.operation == "count" and agg.column is None:
                    desc = "group_by aggregation: count of rows"
                cols_info.append(ColumnInfo(
                    name=agg.alias, type=agg_type, null_count=0, non_null_count=0,
                    description=desc,
                    origin=ColumnOrigin.aggregate, operation=agg.operation, source_column=agg.column
                ))
            return await self._store_result_from_df(
                grouped,
                provenance=ResultProvenance(
                    op="group_by",
                    parent=result_id,
                    parameters={
                        "group_columns": group_columns,
                        "aggregations": [a.model_dump() for a in aggregations],
                        "filters": filters.model_dump() if filters else None,
                        "sort": [s.model_dump() for s in sort] if sort else None,
                    },
                ),
                columns=cols_info,
                source={"op": "group_by", "parent": result_id, "group_columns": group_columns, "aggregations": [a.model_dump() for a in aggregations], "filters": filters.model_dump() if filters else None},
                grain=f"one row per ({', '.join(group_columns)})",
                source_row_count=parent_meta.row_count,
                result_label=result_label,
            )

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
        store_result: bool = False,
        result_label: Optional[str] = None,
    ) -> Union[Dict[str, Any], ResultSetSummary]:
        bins = min(bins, 50)
        meta = await self._ensure_owned(result_id)
        parent_meta = await self.store.get_metadata(result_id)
        needed_cols = {column}
        if filters and filters.clauses:
            for clause in filters.clauses:
                needed_cols.add(clause.column)
        df = await self._load_full_df(result_id, columns=list(needed_cols))
        schema = {name: df[name].dtype for name in df.columns}
        if filters and filters.clauses:
            mask = _build_filter_expr(filters, schema)
            df = df.filter(mask)
        
        non_null = df.drop_nulls()
        null_count = df.height - non_null.height
        
        if non_null.height == 0:
            if store_result:
                empty_df = pl.DataFrame({column: [], "count": []})
                p_col = next((c for c in parent_meta.columns if c.name == column), None)
                p_type = p_col.type if p_col else "string"
                if p_type == "large_string":
                    p_type = "string"
                cols_info = [
                    ColumnInfo(name=column, type=p_type, null_count=0, non_null_count=0, description=p_col.description if p_col else f"histogram passthrough: {column}", origin=ColumnOrigin.passthrough, source_column=column),
                    ColumnInfo(name="count", type="int64", null_count=0, non_null_count=0, description="histogram derived: count", origin=ColumnOrigin.derived)
                ]
                return await self._store_result_from_df(
                    empty_df,
                    provenance=ResultProvenance(
                        op="histogram",
                        parent=result_id,
                        parameters={
                            "column": column,
                            "bins": bins,
                            "filters": filters.model_dump() if filters else None,
                        },
                    ),
                    columns=cols_info,
                    source={"op": "histogram", "parent": result_id, "column": column, "bins": bins, "filters": filters.model_dump() if filters else None},
                    grain=f"one row per histogram bin for {column}",
                    source_row_count=parent_meta.row_count,
                    result_label=result_label,
                )
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
            
            if total_distinct > max_bins:
                top_values = value_counts.slice(0, max_bins)
                other_count = value_counts.slice(max_bins)["count"].sum()
                other_row = pl.DataFrame({
                    column: [None],
                    "count": [other_count],
                    "is_other": [True]
                })
                top_values = top_values.with_columns(pl.lit(False).alias("is_other"))
                final_df = pl.concat([top_values, other_row], how="vertical_relaxed")
            else:
                final_df = value_counts.with_columns(pl.lit(False).alias("is_other"))
                
            if store_result:
                p_col = next((c for c in parent_meta.columns if c.name == column), None)
                p_type = p_col.type if p_col else "string"
                if p_type == "large_string":
                    p_type = "string"
                cols_info = [
                    ColumnInfo(name=column, type=p_type, null_count=0, non_null_count=0, description=p_col.description if p_col else f"histogram passthrough: {column}", origin=ColumnOrigin.passthrough, source_column=column),
                    ColumnInfo(name="count", type="int64", null_count=0, non_null_count=0, description="histogram derived: count", origin=ColumnOrigin.derived),
                    ColumnInfo(name="is_other", type="bool", null_count=0, non_null_count=0, description="histogram derived: is_other", origin=ColumnOrigin.derived)
                ]
                return await self._store_result_from_df(
                    final_df,
                    provenance=ResultProvenance(
                        op="histogram",
                        parent=result_id,
                        parameters={
                            "column": column,
                            "bins": bins,
                            "filters": filters.model_dump() if filters else None,
                        },
                    ),
                    columns=cols_info,
                    source={"op": "histogram", "parent": result_id, "column": column, "bins": bins, "filters": filters.model_dump() if filters else None},
                    grain=f"one row per histogram bin for {column}",
                    source_row_count=parent_meta.row_count,
                    result_label=result_label,
                )

            histogram = []
            for row in final_df.iter_rows(named=True):
                histogram.append({
                    "value": _normalize_value(row[column]),
                    "count": row["count"],
                    "is_other": row["is_other"]
                })
                
            return {
                "result_id": result_id,
                "column": column,
                "bins": len(histogram),
                "null_count": null_count,
                "histogram": histogram,
                "total_distinct": total_distinct,
            }
        else:
            # Numeric histogram
            col_min = non_null[column].min()
            col_max = non_null[column].max()
            if col_min is None or col_max is None:
                if store_result:
                    empty_df = pl.DataFrame({column: [], "count": []})
                    p_col = next((c for c in parent_meta.columns if c.name == column), None)
                    p_type = p_col.type if p_col else "float64"
                    if p_type == "large_string":
                        p_type = "string"
                    cols_info = [
                        ColumnInfo(name="bin_index", type="int64", null_count=0, non_null_count=0, description="histogram derived: bin_index", origin=ColumnOrigin.derived),
                        ColumnInfo(name="bin_lower", type=p_type, null_count=0, non_null_count=0, description="histogram derived: bin_lower", origin=ColumnOrigin.derived),
                        ColumnInfo(name="bin_upper", type=p_type, null_count=0, non_null_count=0, description="histogram derived: bin_upper", origin=ColumnOrigin.derived),
                        ColumnInfo(name="bin_label", type="string", null_count=0, non_null_count=0, description="histogram derived: bin_label", origin=ColumnOrigin.derived),
                        ColumnInfo(name="count", type="int64", null_count=0, non_null_count=0, description="histogram derived: count", origin=ColumnOrigin.derived)
                    ]
                    return await self._store_result_from_df(
                        empty_df,
                        provenance=ResultProvenance(
                            op="histogram",
                            parent=result_id,
                            parameters={
                                "column": column,
                                "bins": bins,
                                "filters": filters.model_dump() if filters else None,
                            },
                        ),
                        columns=cols_info,
                        source={"op": "histogram", "parent": result_id, "column": column, "bins": bins, "filters": filters.model_dump() if filters else None},
                        grain=f"one row per histogram bin for {column}",
                        source_row_count=parent_meta.row_count,
                        result_label=result_label,
                    )
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
            
            # Rename and add required columns
            hist_df = hist_df.rename({"breakpoint": "bin_upper"})
            hist_df = hist_df.with_columns([
                pl.col("bin_upper").shift(1).fill_null(float("-inf")).alias("bin_lower"),
                pl.arange(0, hist_df.height).alias("bin_index"),
            ])
            hist_df = hist_df.with_columns([
                pl.format("[{}, {}]", pl.col("bin_lower"), pl.col("bin_upper")).alias("bin_label")
            ])
            # Reorder columns
            hist_df = hist_df.select(["bin_index", "bin_lower", "bin_upper", "bin_label", "count"])
            
            if store_result:
                p_col = next((c for c in parent_meta.columns if c.name == column), None)
                p_type = p_col.type if p_col else "float64"
                if p_type == "large_string":
                    p_type = "string"
                cols_info = [
                    ColumnInfo(name="bin_index", type="int64", null_count=0, non_null_count=0, description="histogram derived: bin_index", origin=ColumnOrigin.derived),
                    ColumnInfo(name="bin_lower", type=p_type, null_count=0, non_null_count=0, description="histogram derived: bin_lower", origin=ColumnOrigin.derived),
                    ColumnInfo(name="bin_upper", type=p_type, null_count=0, non_null_count=0, description="histogram derived: bin_upper", origin=ColumnOrigin.derived),
                    ColumnInfo(name="bin_label", type="string", null_count=0, non_null_count=0, description="histogram derived: bin_label", origin=ColumnOrigin.derived),
                    ColumnInfo(name="count", type="int64", null_count=0, non_null_count=0, description="histogram derived: count", origin=ColumnOrigin.derived)
                ]
                return await self._store_result_from_df(
                    hist_df,
                    provenance=ResultProvenance(
                        op="histogram",
                        parent=result_id,
                        parameters={
                            "column": column,
                            "bins": bins,
                            "filters": filters.model_dump() if filters else None,
                        },
                    ),
                    columns=cols_info,
                    source={"op": "histogram", "parent": result_id, "column": column, "bins": bins, "filters": filters.model_dump() if filters else None},
                    grain=f"one row per histogram bin for {column}",
                    source_row_count=parent_meta.row_count,
                    result_label=result_label,
                )

            histogram = []
            for row in hist_df.iter_rows(named=True):
                histogram.append({
                    "bin_index": row["bin_index"],
                    "bin_lower": _normalize_value(row["bin_lower"]),
                    "bin_upper": _normalize_value(row["bin_upper"]),
                    "bin_label": row["bin_label"],
                    "count": row["count"],
                })
            return {
                "result_id": result_id,
                "column": column,
                "bins": len(histogram),
                "null_count": null_count,
                "histogram": histogram,
            }

    async def join(
        self,
        left_result_id: str,
        right_result_id: str,
        on: List[Union[str, JoinColumnPair]],
        how: str = "left",
        columns_left: Optional[List[str]] = None,
        columns_right: Optional[List[str]] = None,
        suffix: str = "_r",
        normalize_keys: bool = True,
        validate: Optional[str] = "many_to_one",
        right_duplicates: str = "error",
        indicator: Union[str, bool] = "_join_match",
        result_label: Optional[str] = None,
        dry_run: bool = False,
    ) -> Union[JoinResultSummary, JoinDryRunReport]:
        """Join two stored result sets on normalized keys and store the result.

        `dry_run=true` returns the structural report (`JoinDryRunReport`) without materializing a
        result set, while still enforcing `validate` and the output-size guard.

        See `tools/result_sets.join` for the full user-facing contract.
        """
        # --- argument validation -------------------------------------------------
        if not on:
            raise tool_error("INVALID_ARGUMENT", "'on' must list at least one join key.")
        if how not in ("left", "inner", "right", "full"):
            raise tool_error("INVALID_ARGUMENT", f"Invalid 'how' value '{how}'. Must be one of 'left', 'inner', 'right', 'full'.", {"offending_value": how})
        if validate not in (None, "many_to_one", "one_to_one", "one_to_many", "many_to_many"):
            raise tool_error("INVALID_ARGUMENT", f"Invalid 'validate' value '{validate}'. Must be one of 'many_to_one', 'one_to_one', 'one_to_many', 'many_to_many' or null.", {"offending_value": validate})
        if right_duplicates not in ("error", "first", "report_only"):
            raise tool_error("INVALID_ARGUMENT", f"Invalid 'right_duplicates' value '{right_duplicates}'. Must be one of 'error', 'first', 'report_only'.", {"offending_value": right_duplicates})

        pairs: List[Tuple[str, str]] = []
        for key in on:
            if isinstance(key, JoinColumnPair):
                pairs.append((key.left, key.right))
            elif isinstance(key, str):
                pairs.append((key, key))
            elif isinstance(key, dict):
                pairs.append((key.get("left"), key.get("right")))
            else:
                raise tool_error("INVALID_ARGUMENT", f"Invalid join key entry '{key!r}'. Must be a column name, a {{'left','right'}} pair, or a JoinColumnPair.", {"offending_value": key})
        for left_col, right_col in pairs:
            if not isinstance(left_col, str) or not isinstance(right_col, str):
                raise tool_error("INVALID_ARGUMENT", "Join key columns must be column names.")

        # duplicate key column detection (same left column used twice is an error)
        left_key_names = [p[0] for p in pairs]
        right_key_names = [p[1] for p in pairs]
        if len(left_key_names) != len(set(left_key_names)) or len(right_key_names) != len(set(right_key_names)):
            raise tool_error("INVALID_ARGUMENT", "Join key columns must be distinct per side.")

        if indicator is False:
            indicator_name = None
        elif isinstance(indicator, str):
            indicator_name = indicator if indicator else None
        else:  # True -> default name
            indicator_name = "_join_match"

        # --- existence -------------------------------------------------------------
        left_meta = await self._ensure_owned(left_result_id)
        right_meta = await self._ensure_owned(right_result_id)

        # --- load data -------------------------------------------------------------
        left_df = await self._load_full_df(left_result_id)
        right_df = await self._load_full_df(right_result_id)
        left_schema_names = list(left_df.columns)
        right_schema_names = list(right_df.columns)

        # --- key column resolution & type checks -----------------------------------
        for left_col, right_col in pairs:
            if left_col not in left_schema_names:
                raise tool_error("UNKNOWN_COLUMN", f"Column '{left_col}' does not exist on the left side.", {"available_columns": left_schema_names, "side": "left"})
            if right_col not in right_schema_names:
                raise tool_error("UNKNOWN_COLUMN", f"Column '{right_col}' does not exist on the right side.", {"available_columns": right_schema_names, "side": "right"})

        key_is_string: List[bool] = []
        for (left_col, right_col) in pairs:
            left_dt = left_df[left_col].dtype
            right_dt = right_df[right_col].dtype
            if _is_string_dtype(left_dt) and _is_string_dtype(right_dt):
                key_is_string.append(True)
            elif _is_string_dtype(left_dt) != _is_string_dtype(right_dt):
                raise tool_error(
                    "JOIN_KEY_TYPE_MISMATCH",
                    f"Key columns '{left_col}' (left) and '{right_col}' (right) have incompatible types.",
                    {"left_column": left_col, "left_type": str(left_dt), "right_column": right_col, "right_type": str(right_dt)},
                )
            elif left_dt != right_dt:
                raise tool_error(
                    "JOIN_KEY_TYPE_MISMATCH",
                    f"Key columns '{left_col}' (left) and '{right_col}' (right) must share the same type for non-string keys.",
                    {"left_column": left_col, "left_type": str(left_dt), "right_column": right_col, "right_type": str(right_dt)},
                )
            else:
                key_is_string.append(False)

        # --- projection ------------------------------------------------------------
        if columns_left is not None:
            for c in columns_left:
                if c not in left_schema_names:
                    raise tool_error("UNKNOWN_COLUMN", f"Column '{c}' does not exist on the left side.", {"available_columns": left_schema_names, "side": "left"})
        if columns_right is not None:
            for c in columns_right:
                if c not in right_schema_names:
                    raise tool_error("UNKNOWN_COLUMN", f"Column '{c}' does not exist on the right side.", {"available_columns": right_schema_names, "side": "right"})

        left_sel = set(columns_left) if columns_left is not None else None
        right_sel = set(columns_right) if columns_right is not None else None
        left_out = list(dict.fromkeys(c for c in left_schema_names if c in left_key_names or (left_sel is None or c in left_sel)))
        right_out = list(dict.fromkeys(c for c in right_schema_names if c in right_key_names or (right_sel is None or c in right_sel)))

        # right-side final naming (collision -> suffix; unresolvable collision -> error)
        used_names = set(left_out)
        right_final: List[Tuple[str, str, bool, Optional[int]]] = []  # (orig, final, is_key, key_index)
        for c in right_out:
            if c in right_key_names:
                right_final.append((c, f"__rk_{right_key_names.index(c)}", True, right_key_names.index(c)))
            else:
                final = c
                if final in used_names:
                    final = _apply_suffix(c, suffix)
                if final in used_names:
                    raise tool_error("COLUMN_NAME_COLLISION", f"Column name collision for '{final}'. Rename with a different 'suffix'.", {"conflicting_names": [final]})
                used_names.add(final)
                right_final.append((c, final, False, None))

        if indicator_name is not None and indicator_name in used_names:
            raise tool_error("COLUMN_NAME_COLLISION", f"Indicator column name '{indicator_name}' collides with an existing column.", {"conflicting_names": [indicator_name]})

        # --- normalized shadow keys -----------------------------------------------
        left_shadow_names = [f"__ln_{i}" for i in range(len(pairs))]
        right_shadow_names = [f"__rn_{i}" for i in range(len(pairs))]
        left_raw_cols = [left_df[c].to_list() for c in left_key_names]
        right_raw_cols = [right_df[c].to_list() for c in right_key_names]
        if normalize_keys:
            left_norm_cols = [[normalize_mpn(v) for v in col] for col in left_raw_cols]
            right_norm_cols = [[normalize_mpn(v) for v in col] for col in right_raw_cols]
        else:
            left_norm_cols = left_raw_cols
            right_norm_cols = right_raw_cols

        any_string_key = any(key_is_string)
        enabled_steps = list(NORMALIZE_STEPS) if (any_string_key and normalize_keys) else []

        # --- per-row key tuples -> counts / sets -----------------------------------
        def _key_tuples(cols):
            return [(t if all(v is not None for v in t) else None) for t in zip(*cols)]

        left_keys = _key_tuples(left_norm_cols)
        right_keys = _key_tuples(right_norm_cols)
        left_raw_tuples = _key_tuples(left_raw_cols)
        right_raw_tuples = _key_tuples(right_raw_cols)

        left_counts = Counter(k for k in left_keys if k is not None)
        right_counts = Counter(k for k in right_keys if k is not None)
        left_set = set(left_counts)
        right_set = set(right_counts)

        def _norm_tuple(raw_tuple):
            if not normalize_keys:
                return raw_tuple
            return tuple(normalize_mpn(v) for v in raw_tuple)

        # distinct raw keys per side (non-null), in first-seen order
        left_raw_order: "list" = []
        left_raw_seen = set()
        for rt in left_raw_tuples:
            if rt is not None and rt not in left_raw_seen:
                left_raw_seen.add(rt)
                left_raw_order.append(rt)
        right_raw_order, right_raw_seen = [], set()
        for rt in right_raw_tuples:
            if rt is not None and rt not in right_raw_seen:
                right_raw_seen.add(rt)
                right_raw_order.append(rt)

        # --- right_duplicates="first": deterministic dedup of the right side -------------
        # Keep the first right row per normalized key (lowest ordinal). This cannot fan out
        # (each left row matches at most one right row), preserving row-count and sums. The
        # removed-row count is reported and recorded in provenance so consumers can see it.
        right_deduplicated_rows = 0
        if right_duplicates == "first":
            seen: set = set()
            mask = []
            for k in right_keys:
                if k is None or k not in seen:
                    if k is not None:
                        seen.add(k)
                    mask.append(True)
                else:
                    mask.append(False)
            right_deduplicated_rows = mask.count(False)
            if right_deduplicated_rows:
                right_keys = [x for x, m in zip(right_keys, mask) if m]
                right_raw_tuples = [x for x, m in zip(right_raw_tuples, mask) if m]
                right_norm_cols = [[v for v, m in zip(col, mask) if m] for col in right_norm_cols]
                right_raw_cols = [[v for v, m in zip(col, mask) if m] for col in right_raw_cols]
                right_df = right_df.filter(pl.Series(mask))
                # recompute count structures and distinct raw order for the filtered side
                right_counts = Counter(k for k in right_keys if k is not None)
                right_set = set(right_counts)
                right_raw_order, right_raw_seen = [], set()
                for rt in right_raw_tuples:
                    if rt is not None and rt not in right_raw_seen:
                        right_raw_seen.add(rt)
                        right_raw_order.append(rt)

        def _render_key(value) -> str:
            if isinstance(value, tuple):
                return " | ".join(str(v) for v in value)
            return str(value)

        # altered-key counts (distinct raw keys changed by normalization)
        def _count_altered(raw_order):
            if not any_string_key:
                return 0
            return sum(1 for rt in raw_order if _norm_tuple(rt) != rt)

        left_keys_altered = _count_altered(left_raw_order)
        right_keys_altered = _count_altered(right_raw_order)

        # collisions: distinct raw keys collapsing to one normalized key
        def _entry_steps(raw_tuples):
            """Union of normalization steps (in NORMALIZE_STEPS order) that altered any raw key."""
            applied = []
            seen = set()
            for rt in raw_tuples:
                if rt in seen:
                    continue
                seen.add(rt)
                for v in rt:
                    _, ss = normalize_mpn_with_steps(v)
                    applied.extend(ss)
            return [s for s in NORMALIZE_STEPS if s in applied]

        def _collision_entry(nt, raw_tuples, side, count=None):
            rendered_raw = [_render_key(r[0] if len(r) == 1 else r) for r in raw_tuples]
            distinct_raw = list(dict.fromkeys(rendered_raw))
            entry = {
                "normalized": _render_key(nt[0] if len(nt) == 1 else nt),
                "count": count,
                "raw": distinct_raw,
                "side": side,
                "caused_by_normalization": len(distinct_raw) > 1,
                "steps_applied": _entry_steps(raw_tuples),
            }
            return entry

        def _collisions(raw_order, counts, side):
            out = []
            groups = {}
            for rt in raw_order:
                nt = _norm_tuple(rt)
                if nt is None:
                    continue
                groups.setdefault(nt, []).append(rt)
            for nt, raws in groups.items():
                if len(raws) > 1:
                    out.append(_collision_entry(nt, raws, side, count=counts.get(nt)))
                if len(out) >= 20:
                    break
            return out

        left_collisions = _collisions(left_raw_order, left_counts, "left") if any_string_key else []
        right_collisions = _collisions(right_raw_order, right_counts, "right") if any_string_key else []

        # --- cardinality validation (on normalized keys, restricted to participating) --
        # Rec 1: a duplicate only endangers a result if its key is present on the OTHER side
        # (that is what fan-out means). Duplicates whose key never matches are reported in
        # `data_quality`, never blocking. `right_duplicates="report_only"` additionally skips
        # the right-side uniqueness enforcement and records a flag in result metadata.
        def _group_raws_by_norm(raw_order):
            groups = {}
            for rt in raw_order:
                nt = _norm_tuple(rt)
                if nt is None:
                    continue
                groups.setdefault(nt, []).append(rt)
            return groups

        def _dup_entries(raw_order, counts, side, predicate):
            groups = _group_raws_by_norm(raw_order)
            out = []
            for k, c in counts.items():
                if c > 1 and predicate(k):
                    out.append(_collision_entry(k, groups.get(k, []), side, count=c))
            return out

        right_enforced = validate in ("many_to_one", "one_to_one") and right_duplicates == "error"
        left_enforced = validate in ("one_to_one", "one_to_many")

        if right_enforced:
            d = _dup_entries(right_raw_order, right_counts, "right", lambda k: k in left_set)
            if d:
                raise tool_error("CARDINALITY_VIOLATION", f"Right-side join keys are not unique (required: {validate}).", {"keys": d, "side": "right"})
        if left_enforced:
            d = _dup_entries(left_raw_order, left_counts, "left", lambda k: k in right_set)
            if d:
                raise tool_error("CARDINALITY_VIOLATION", f"Left-side join keys are not unique (required: {validate}).", {"keys": d, "side": "left"})

        # --- data-quality report (always surfaced, never blocking) ---------------------
        # Non-participating collisions: duplicate normalized keys absent from the other side,
        # so they can never distort a match. Reported so the finding is not lost, e.g. the eight
        # comma-corrupted article numbers that do not appear in the 2025 sales statistics.
        # (4.3) Filter out legal same-side duplicates that are merely a consequence of the data
        # grain (e.g. one left row per article per week under many_to_one): they are noise unless
        # normalization itself collapsed distinct strings. Keep (a) normalization-caused
        # collisions and (b) duplicates on a side the chosen `validate` mode declares unique.
        right_side_constrained = validate in ("many_to_one", "one_to_one")
        left_side_constrained = validate in ("one_to_one", "one_to_many")

        def _dq_is_signal(entry):
            if entry["steps_applied"]:
                return True
            if entry["side"] == "right" and right_side_constrained:
                return True
            if entry["side"] == "left" and left_side_constrained:
                return True
            return False

        dq_sample = _dup_entries(right_raw_order, right_counts, "right", lambda k: k not in left_set)
        dq_sample += _dup_entries(left_raw_order, left_counts, "left", lambda k: k not in right_set)
        dq_sample = [e for e in dq_sample if _dq_is_signal(e)]
        data_quality = JoinDataQualityReport(
            non_participating_collisions=len(dq_sample),
            sample=dq_sample[:20],
        )

        # --- report metrics ----------------------------------------------------------
        matched_left_rows = sum(cnt for k, cnt in left_counts.items() if k in right_set)
        matched_right_rows = sum(cnt for k, cnt in right_counts.items() if k in left_set)
        unmatched_left_rows = len(left_keys) - matched_left_rows
        unmatched_right_rows = len(right_keys) - matched_right_rows
        matched_keys = left_set & right_set
        left_keys_matched = len(matched_keys)
        right_keys_matched = len(matched_keys)
        left_keys_unmatched = len(left_set) - left_keys_matched

        left_unmatched = sorted(left_set - right_set, key=lambda t: min(i for i, k in enumerate(left_keys) if k == t))
        right_only = sorted(right_set - left_set, key=lambda t: min(i for i, k in enumerate(right_keys) if k == t))
        # raw renderings for the sample: first raw key seen for each normalized key
        left_raw_by_norm = {}
        for rt in left_raw_order:
            left_raw_by_norm.setdefault(_norm_tuple(rt), rt)
        right_raw_by_norm = {}
        for rt in right_raw_order:
            right_raw_by_norm.setdefault(_norm_tuple(rt), rt)

        unmatched_sample: List[Dict[str, str]] = []
        def _append_unmatched(k, side):
            raw_by_norm = left_raw_by_norm if side == "left" else right_raw_by_norm
            raw = raw_by_norm.get(k)
            if raw is None:
                return False
            v = _render_key(raw[0] if len(raw) == 1 else raw)
            if any(e["key"] == v and e["side"] == side for e in unmatched_sample):
                return False
            unmatched_sample.append({"key": v, "side": side})
            return len(unmatched_sample) >= 20
        for k in left_unmatched:
            if _append_unmatched(k, "left"):
                break
        for k in right_only:
            if _append_unmatched(k, "right"):
                break

        # matches gained purely by normalization (left rows matched under norm, not under raw)
        matches_gained_by_normalization = 0
        if any_string_key and any(enabled_steps):
            right_raw_set = set(right_raw_tuples) if right_raw_tuples else set()
            # exclude None entries
            right_raw_set.discard(None)
            for k, rt in zip(left_keys, left_raw_tuples):
                if k is None or rt is None:
                    continue
                if k in right_set and rt not in right_raw_set:
                    matches_gained_by_normalization += 1

        # --- exact output-size estimate (before materialization) --------------------
        estimated_rows = 0
        for k, cnt in left_counts.items():
            rc = right_counts.get(k, 0)
            if rc and rc > 0:
                estimated_rows += rc * cnt
            elif how in ("left", "full"):
                estimated_rows += cnt
        # left rows with a NULL join key are emitted by left/full joins (they never match).
        if how in ("left", "full"):
            estimated_rows += left_keys.count(None)
        if how in ("right", "full"):
            estimated_rows += unmatched_right_rows
        if estimated_rows > self.settings.RESULTSET_MAX_ROWS:
            raise tool_error(
                "RESULT_SET_TOO_LARGE",
                f"Join output would exceed {self.settings.RESULTSET_MAX_ROWS} rows.",
                {"estimated_row_count": estimated_rows},
            )

        # matched output rows (both side rows that would pair up)
        matched_output_rows = 0
        for k, cnt in left_counts.items():
            rc = right_counts.get(k, 0)
            if rc and rc > 0:
                matched_output_rows += rc * cnt

        report = JoinReport(
            how=how,
            indicator_column=indicator_name,
            matched_rows=matched_output_rows,
            unmatched_left_rows=unmatched_left_rows,
            unmatched_right_rows=unmatched_right_rows,
            left_keys_total=len(left_set),
            left_keys_matched=left_keys_matched,
            left_keys_unmatched=left_keys_unmatched,
            right_keys_total=len(right_set),
            right_keys_matched=right_keys_matched,
            deduplicated_rows=right_deduplicated_rows,
            unmatched_key_sample=unmatched_sample,
            normalization=JoinNormalizationReport(
                enabled=enabled_steps,
                left_keys_altered=left_keys_altered,
                right_keys_altered=right_keys_altered,
                matches_gained_by_normalization=matches_gained_by_normalization,
                collisions=left_collisions + right_collisions,
            ),
            data_quality=data_quality,
        )

        # dry run: return the report without materializing anything. `validate` and the
        # size guard above are still enforced, so a caller can inspect collisions and
        # cardinality without having to disable the safety net to learn why it fired.
        if dry_run:
            return JoinDryRunReport(report=report, estimated_row_count=estimated_rows)

        # --- polars join ------------------------------------------------------------
        ljoin = left_df.with_columns(
            [pl.Series(name, col) for name, col in zip(left_shadow_names, left_norm_cols)]
            + [pl.lit(1).alias("__sideL")]
        ).select(left_out + left_shadow_names + ["__sideL"])

        right_select_exprs = []
        for orig, final, is_key, _ki in right_final:
            if is_key:
                right_select_exprs.append(pl.col(orig).alias(final))
            else:
                right_select_exprs.append(pl.col(orig).alias(final))
        right_select_exprs += [pl.Series(name, col) for name, col in zip(right_shadow_names, right_norm_cols)]
        right_select_exprs.append(pl.lit(1).alias("__sideR"))
        rjoin = right_df.select(right_select_exprs)

        joined = ljoin.join(rjoin, left_on=left_shadow_names, right_on=right_shadow_names, how=how)

        def _indicator_expr():
            is_left = pl.col("__sideL").is_not_null()
            is_right = pl.col("__sideR").is_not_null()
            return (
                pl.when(is_left & is_right).then(pl.lit("both"))
                .when(is_left).then(pl.lit("left_only"))
                .otherwise(pl.lit("right_only"))
            )

        # keys are already named by their left-side column name; right-side columns
        # were renamed (suffix) in `rjoin`, so reference the final names below.
        if how in ("right", "full"):
            out_cols: List[pl.Expr] = [
                pl.coalesce([pl.col(left_col), pl.col(f"__rk_{i}")]).alias(left_col)
                for i, (left_col, right_col) in enumerate(pairs)
            ]
            out_cols += [pl.col(c) for c in left_out if c not in left_key_names]
            out_cols += [pl.col(final) for orig, final, is_key, _ki in right_final if not is_key]
            if indicator_name is not None:
                out_cols.append(_indicator_expr().alias(indicator_name))
            result_df = joined.select(out_cols)
        else:
            out_cols = [pl.col(c) for c in left_out]
            out_cols += [pl.col(final) for orig, final, is_key, _ki in right_final if not is_key]
            if indicator_name is not None:
                out_cols.append(_indicator_expr().alias(indicator_name))
            result_df = joined.select(out_cols)

        # --- column info ------------------------------------------------------------
        left_info_map = {c.name: c for c in left_meta.columns}
        right_info_map = {c.name: c for c in right_meta.columns}
        cols_info: List[ColumnInfo] = []

        def _clean_type(dtype_str: str) -> str:
            return "string" if dtype_str == "large_string" else dtype_str

        for c in left_out:
            pc = left_info_map.get(c)
            cols_info.append(ColumnInfo(
                name=c,
                type=_clean_type(pc.type if pc else "string"),
                null_count=0, non_null_count=0,
                description=pc.description if pc else None,
                origin=ColumnOrigin.join_left, source_column=c,
            ))
        for orig, final, is_key, _ki in right_final:
            if is_key:
                continue
            pc = right_info_map.get(orig)
            cols_info.append(ColumnInfo(
                name=final,
                type=_clean_type(pc.type if pc else "string"),
                null_count=0, non_null_count=0,
                description=pc.description if pc else None,
                origin=ColumnOrigin.join_right, source_column=orig,
            ))
        if indicator_name is not None:
            cols_info.append(ColumnInfo(
                name=indicator_name,
                type="string",
                null_count=0, non_null_count=0,
                description="Join match indicator: 'both', 'left_only', or 'right_only'",
                origin=ColumnOrigin.join_generated,
            ))

        # matched output rows (both side rows that paired up)
        matched_output_rows = 0
        for k, cnt in left_counts.items():
            rc = right_counts.get(k, 0)
            if rc and rc > 0:
                matched_output_rows += rc * cnt

        # --- grain ------------------------------------------------------------------
        if validate in ("one_to_many", "many_to_many") or how == "full":
            grain = None
        elif how == "right":
            grain = right_meta.grain
        else:
            grain = left_meta.grain

        if how in ("right", "full"):
            source_row_count = right_meta.row_count if how == "right" else None
        else:
            source_row_count = left_meta.row_count

        on_params = [o.model_dump() if isinstance(o, JoinColumnPair) else o for o in on]
        provenance = ResultProvenance(
            op="join",
            parents=[left_result_id, right_result_id],
            parameters={
                "on": on_params,
                "how": how,
                "validate": validate,
                "right_duplicates": right_duplicates,
                "deduplicated_rows": right_deduplicated_rows,
                "suffix": suffix,
                "normalize_keys": normalize_keys,
                "columns_left": columns_left,
                "columns_right": columns_right,
                "indicator": indicator if indicator is not False else False,
            },
        )
        if right_duplicates == "first" and right_deduplicated_rows:
            data_quality_note = f"{right_deduplicated_rows} right rows deduplicated via 'first' (lowest ordinal per normalized key kept)."
        elif right_duplicates == "report_only":
            data_quality_note = "Right-side duplicates were kept as-is (report_only); proceed accordingly."
        elif right_duplicates == "first":
            data_quality_note = "No right-side duplicates found (right_duplicates='first'); nothing deduplicated."
        else:
            data_quality_note = None
        source = {
            "op": "join",
            "parents": [left_result_id, right_result_id],
            "on": on_params,
            "how": how,
            "validate": validate,
            "right_duplicates": right_duplicates,
            "deduplicated_rows": right_deduplicated_rows,
            "data_quality_note": data_quality_note,
        }

        summary = await self._store_result_from_df(
            result_df,
            provenance=provenance,
            columns=cols_info,
            source=source,
            grain=grain,
            source_row_count=source_row_count,
            result_label=result_label,
        )
        return JoinResultSummary(**summary.model_dump(), join=report)

    async def delete_result_set(self, result_id: str) -> Dict[str, Any]:
        meta = await self._ensure_owned(result_id)
        deleted = await self.store.delete_result(self.session_id, result_id)
        return {"result_id": result_id, "deleted": deleted}
