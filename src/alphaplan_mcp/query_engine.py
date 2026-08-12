# Implements the shared QueryEngine for safe filtering, grouping,
# sorting, and aggregation on materialized result sets using Polars,
# without constructing arbitrary SQL.
"""Polars-based query engine for stored result sets.

Structural rule for this module: **no function or method exceeds ~60 lines**.
Shared mechanics live in small, single-purpose helpers:

- filter/sort/aggregation *expression* building (module level, pure),
- loading + filtering (`_load_filtered_df`), pagination clamping/encoding,
- `store_result` plumbing (`_store_derived`, `_inherited_column_info`,
  `_agg_column_infos`) so each tool assembles provenance exactly once,
- the join pipeline (`_JoinKeys`, `_JoinPlan`, report builders) — `join()`
  itself is only orchestration.

Public tool methods on `QueryEngine` are thin orchestrations over these.
"""

import base64
import gzip
import hashlib
import json
import logging
import math
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import polars as pl

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
# small generic helpers
# ---------------------------------------------------------------------------


def _safe(fn: Callable[[], Any], default: Any = None) -> Any:
    """Run fn(), returning `default` on any exception (best-effort stats)."""
    try:
        return fn()
    except Exception:
        return default


def _schema_of(df: pl.DataFrame) -> Dict[str, pl.DataType]:
    """Column name -> Polars dtype map for a materialized frame."""
    return {name: df[name].dtype for name in df.columns}


def _clean_meta_type(dtype_str: str) -> str:
    """Normalize stored type names (PyArrow's ``large_string`` surfaces as ``string``)."""
    return "string" if dtype_str == "large_string" else dtype_str


def _is_string_dtype(dtype) -> bool:
    """Whether a Polars dtype is a string-like type."""
    str_type = getattr(pl, "String", None) or getattr(pl, "Utf8", None)
    return dtype == str_type or dtype == pl.Categorical


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
            return None
        return value
    return value


def _df_to_rows(df: pl.DataFrame) -> List[Dict[str, Any]]:
    """Convert a Polars DataFrame to a list of dicts with normalized values."""
    return [{k: _normalize_value(v) for k, v in row.items()} for row in df.iter_rows(named=True)]


def _sha256_b64(text: str) -> str:
    """Return Base64(SHA-256(UTF-8 bytes of text))."""
    return base64.b64encode(hashlib.sha256(text.encode("utf-8")).digest()).decode("ascii")


# ---------------------------------------------------------------------------
# filter expression building
# ---------------------------------------------------------------------------

_PATTERN_OPS = {"like", "not_like", "regex", "not_regex", "matches_charset", "not_matches_charset"}
_SUBSTRING_OPS = {"contains", "starts_with", "ends_with"}
_COMPARE_OPS = {"gt": "gt", "gte": "ge", "lt": "lt", "lte": "le"}
_MAX_IN_VALUES = 100


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

    if len(filter_group.clauses) > settings.TOOL_MAX_FILTER_CLAUSES:
        raise tool_error(
            "INVALID_ARGUMENT",
            f"Too many filter clauses (max {settings.TOOL_MAX_FILTER_CLAUSES})",
        )
    if filter_group.combine not in ("all", "any"):
        raise tool_error("INVALID_ARGUMENT", "FilterGroup.combine must be 'all' or 'any'")

    expressions = [_build_single_filter(clause, schema) for clause in filter_group.clauses]
    combined = expressions[0]
    for e in expressions[1:]:
        combined = (combined | e) if filter_group.combine == "any" else (combined & e)
    return combined


def _build_single_filter(clause: FilterClause, schema: Dict[str, pl.DataType]) -> pl.Expr:
    col = clause.column
    if col not in schema:
        raise tool_error("UNKNOWN_COLUMN", f"Column '{col}' does not exist.",
                         {"available_columns": list(schema.keys())})
    dtype = schema[col]
    op = clause.operator.lower()
    col_expr = pl.col(col)

    if op == "is_null":
        return col_expr.is_null()
    if op == "is_not_null":
        return col_expr.is_not_null()

    value = _cast_value(clause.value, dtype)
    if value is None:
        raise tool_error("INVALID_ARGUMENT", f"Operator '{op}' requires a value.")

    if op in ("eq", "ne"):
        return _equality_expr(col_expr, value, clause.case_sensitive, negate=(op == "ne"))
    if op in _PATTERN_OPS:
        return _pattern_expr(col_expr, op, value, clause.case_sensitive, dtype)
    if op in _SUBSTRING_OPS:
        return _substring_expr(col_expr, op, value, clause.case_sensitive)
    if op in _COMPARE_OPS:
        return getattr(col_expr, _COMPARE_OPS[op])(value)
    if op in ("in", "not_in"):
        return _in_list_expr(col_expr, op, value)
    if op == "between":
        if not isinstance(value, list) or len(value) != 2:
            raise tool_error("INVALID_ARGUMENT", "Operator 'between' requires a list of two values.")
        low, high = value
        return (col_expr >= low) & (col_expr <= high)
    raise tool_error("INVALID_ARGUMENT", f"Unsupported operator '{op}'.")


def _equality_expr(col_expr: pl.Expr, value: Any, case_sensitive: bool, negate: bool) -> pl.Expr:
    """eq/ne; a string value with unescaped wildcards degrades to a LIKE match."""
    if _needs_like(value):
        return _string_like(col_expr, value, case_sensitive, negate=negate)
    return (col_expr != value) if negate else (col_expr == value)


def _pattern_expr(col_expr: pl.Expr, op: str, value: Any, case_sensitive: bool, dtype) -> pl.Expr:
    """like / regex / matches_charset (and their negations)."""
    negate = op.startswith("not_")
    if op in ("like", "not_like"):
        return _string_like(col_expr, value, case_sensitive, negate=negate)
    if not _is_string_dtype(dtype):
        raise tool_error("INVALID_ARGUMENT", f"Operator '{op}' requires a string column.")
    if op in ("regex", "not_regex"):
        if not isinstance(value, str):
            raise tool_error("INVALID_ARGUMENT", f"Operator '{op}' requires a string pattern.")
        pattern = _validated_regex(value, case_sensitive)
    else:  # matches_charset / not_matches_charset
        if not isinstance(value, str) or not value:
            raise tool_error("INVALID_ARGUMENT", f"Operator '{op}' requires a non-empty set of allowed characters.")
        cls = _charset_class(value)
        pattern = f"^[{cls}]*$" if case_sensitive else f"^(?i:[{cls}])*$"
    match = col_expr.str.contains(pattern, literal=False)
    return ~match if negate else match


def _substring_expr(col_expr: pl.Expr, op: str, value: Any, case_sensitive: bool) -> pl.Expr:
    """contains / starts_with / ends_with; case-insensitive by lowercasing both sides."""
    if not case_sensitive:
        col_expr = col_expr.str.to_lowercase()
        value = value.lower()
    if op == "contains":
        return col_expr.str.contains(value, literal=True)
    if op == "starts_with":
        return col_expr.str.starts_with(value)
    return col_expr.str.ends_with(value)


def _in_list_expr(col_expr: pl.Expr, op: str, value: Any) -> pl.Expr:
    if not isinstance(value, list):
        raise tool_error("INVALID_ARGUMENT", f"Operator '{op}' requires a list of values.")
    if len(value) > _MAX_IN_VALUES:
        raise tool_error("INVALID_ARGUMENT", f"Too many values in '{op}' (max {_MAX_IN_VALUES})")
    expr = col_expr.is_in(value)
    return ~expr if op == "not_in" else expr


def _validated_regex(pattern: str, case_sensitive: bool) -> str:
    """Compile-check a user regex; returns it with an inline (?i) flag when needed."""
    flags = 0 if case_sensitive else re.IGNORECASE
    try:
        re.compile(pattern, flags)
    except re.error as exc:
        raise tool_error("INVALID_ARGUMENT", f"Invalid regular expression: {exc}", {"pattern": pattern})
    return pattern if case_sensitive else f"(?i){pattern}"


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
    # Rebuild the regex manually (rather than re.escape + replace) so that our
    # backslash-escape semantics survive: \<char> is literal, % -> .*, _ -> .
    regex_parts = []
    i = 0
    while i < len(pattern):
        c = pattern[i]
        if c == "\\":
            if i + 1 < len(pattern):
                regex_parts.append(re.escape(pattern[i + 1]))
                i += 2
                continue
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

    if negate:
        return ~col_expr.str.contains(full_regex, literal=False)
    return col_expr.str.contains(full_regex, literal=False)


def _charset_class(allowed: str) -> str:
    """Turn a set of allowed characters into a regex character-class body.

    Each character is regex-escaped so that ranged/meta characters (``-``, ``]``, ``\\``,
    ``^``) are treated literally rather than forming a class range or escape.
    """
    return "".join(re.escape(c) for c in allowed)


def _filter_columns(filters: FilterGroup) -> set:
    """Column names referenced by a filter group's clauses."""
    return {clause.column for clause in filters.clauses}


# ---------------------------------------------------------------------------
# sorting & pagination
# ---------------------------------------------------------------------------


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
    return df.sort(by, descending=descending, nulls_last=nulls_last)


def _stable_tiebreak(sort: List[SortSpec], meta: ResultSetMetadata) -> List[SortSpec]:
    """Append all remaining columns (alphabetical) as tie-breakers for a stable order."""
    existing = {s.column for s in sort}
    extra = [
        SortSpec(column=c.name, direction="asc")
        for c in sorted(meta.columns, key=lambda x: x.name)
        if c.name not in existing
    ]
    return sort + extra


def _apply_rounding(df: pl.DataFrame, round_to: Optional[int]) -> pl.DataFrame:
    """Round float columns; columns that come out fully integral are cast to Int64."""
    if round_to is None:
        return df
    for col in df.columns:
        if df[col].dtype not in (pl.Float32, pl.Float64):
            continue
        if round_to == 0:
            df = df.with_columns((pl.col(col) + 1e-12).round(round_to).cast(pl.Int64))
            continue
        df = df.with_columns((pl.col(col) + 1e-12).round(round_to).alias(col))
        if (df[col].drop_nulls() % 1 == 0).all():
            df = df.with_columns(df[col].cast(pl.Int64))
    return df


def _format_paginated_result(
    result_id: str,
    df: pl.DataFrame,
    offset: int,
    limit: int,
    total_rows: int,
) -> Dict[str, Any]:
    """Apply offset/limit slicing and format the standard pagination response."""
    rows = _df_to_rows(df.slice(offset, limit))
    has_more = offset + limit < total_rows
    return {
        "result_id": result_id,
        "offset": offset,
        "limit": limit,
        "returned": len(rows),
        "total_rows": total_rows,
        "has_more": has_more,
        "next_offset": offset + limit if has_more else None,
        "rows": rows,
    }


def _encode_row_page(
    result_id: str,
    df_slice: pl.DataFrame,
    offset: int,
    limit: int,
    total_rows: int,
    format: str,
    encoding: str,
) -> Dict[str, Any]:
    """Format a page of rows for `list_rows` in the requested format/encoding.

    gzip is silently downgraded to plain when compression would not shrink the
    payload; the returned `encoding` field always reflects what was emitted.
    """
    has_more = offset + limit < total_rows
    resp: Dict[str, Any] = {
        "result_id": result_id,
        "offset": offset,
        "limit": limit,
        "total_rows": total_rows,
        "has_more": has_more,
        "next_offset": offset + limit if has_more else None,
        "returned": df_slice.height,
    }
    rows = _df_to_rows(df_slice) if format == "json" else None
    text = df_slice.write_csv() if format == "csv" else json.dumps(rows, default=str)

    if encoding == "plain":
        resp.update(format=format, encoding="plain")
        if format == "csv":
            resp["data"] = text
            resp["sha256_base64"] = _sha256_b64(text)
        else:
            resp["data"] = rows
        return resp

    # gzip+base64 requested
    payload = text.encode("utf-8")
    compressed = gzip.compress(payload)
    resp["uncompressed_bytes"] = len(payload)
    if len(compressed) < len(payload):
        resp.update(format=format, encoding="gzip+base64",
                    data=base64.b64encode(compressed).decode("utf-8"))
        if format == "csv":
            resp["sha256_base64"] = _sha256_b64(text)
        else:
            resp["sha256"] = hashlib.sha256(payload).hexdigest()
        return resp

    # downgrade: compressed would be larger than plain
    resp.update(format=format, encoding="plain", sha256=hashlib.sha256(payload).hexdigest())
    if format == "csv":
        resp["data"] = text
        resp["sha256_base64"] = _sha256_b64(text)
    else:
        resp["data"] = rows
    return resp


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

# Operations expressible as a single Polars call on the column: (col, spec) -> expr.
_SIMPLE_AGG_EXPRS: Dict[str, Callable[[pl.Expr, AggregationSpec], pl.Expr]] = {
    "count_non_null": lambda c, a: c.count(),
    "count_distinct": lambda c, a: c.n_unique(),
    "nunique": lambda c, a: c.n_unique(),
    "sum": lambda c, a: c.sum(),
    "avg": lambda c, a: c.mean(),
    "mean": lambda c, a: c.mean(),
    "min": lambda c, a: c.min(),
    "max": lambda c, a: c.max(),
    "median": lambda c, a: c.median(),
    "stddev": lambda c, a: c.std(ddof=a.ddof),
    "variance": lambda c, a: c.var(ddof=a.ddof),
    # polars mode() returns the set of most-frequent values in unspecified order;
    # .min() makes ties deterministic (lowest value wins) across calls/rows.
    "mode": lambda c, a: c.mode().min(),
    "iqr": lambda c, a: c.quantile(0.75, interpolation="linear") - c.quantile(0.25, interpolation="linear"),
    # median absolute deviation: median(|x - median(x)|). All aggregation exprs skip nulls.
    "mad": lambda c, a: (c - c.median()).abs().median(),
    "skewness": lambda c, a: c.skew(),
    "kurtosis": lambda c, a: c.kurtosis(),
    "coefficient_of_variation": lambda c, a: c.std(ddof=1) / c.mean(),
    "count_null": lambda c, a: c.is_null().sum(),
    "count_zero": lambda c, a: (c == 0).fill_null(False).sum(),
    "count_negative": lambda c, a: (c < 0).fill_null(False).sum(),
    "count_positive": lambda c, a: (c > 0).fill_null(False).sum(),
}


def _invalid_agg(message: str):
    """INVALID_AGGREGATION error carrying the discoverable operation vocabulary."""
    return tool_error("INVALID_AGGREGATION", message, {"available_operations": KNOWN_OPERATIONS})


def _build_agg_expr(agg: AggregationSpec) -> pl.Expr:
    """Translate a single AggregationSpec into a Polars aggregation expression."""
    op = agg.operation.lower()
    if op == "count":
        if agg.column:
            raise _invalid_agg("count does not accept a column.")
        return pl.len().alias(agg.alias)
    if not agg.column:
        raise _invalid_agg(f"{op} requires a column.")
    col = pl.col(agg.column)
    simple = _SIMPLE_AGG_EXPRS.get(op)
    if simple is not None:
        return simple(col, agg).alias(agg.alias)
    return _complex_agg_expr(op, col, agg).alias(agg.alias)


def _complex_agg_expr(op: str, col: pl.Expr, agg: AggregationSpec) -> pl.Expr:
    """Aggregations that take extra parameters or multi-step expressions."""
    if op == "percentile":
        if agg.q is None:
            raise _invalid_agg("percentile requires a 'q' parameter between 0 and 1.")
        if not (0.0 <= agg.q <= 1.0):
            raise _invalid_agg(f"percentile 'q' must be between 0 and 1, got {agg.q}.")
        if agg.method not in _QUANTILE_METHODS:
            raise _invalid_agg(f"percentile method '{agg.method}' invalid; must be one of {sorted(_QUANTILE_METHODS)}.")
        return col.quantile(agg.q, interpolation=agg.method)
    if op == "trimmed_mean":
        if agg.trim is None:
            raise _invalid_agg("trimmed_mean requires a 'trim' fraction between 0 and 0.5.")
        if not (0.0 <= agg.trim < 0.5):
            raise _invalid_agg(f"trimmed_mean 'trim' must be between 0 (inclusive) and 0.5 (exclusive), got {agg.trim}.")
        c = col.drop_nulls()
        lo = c.quantile(agg.trim, interpolation="linear")
        hi = c.quantile(1.0 - agg.trim, interpolation="linear")
        return c.filter((c >= lo) & (c <= hi)).mean()
    if op == "weighted_avg":
        if not agg.weight_column:
            raise _invalid_agg("weighted_avg requires a 'weight_column'.")
        w = pl.col(agg.weight_column)
        return (col.fill_null(0) * w.fill_null(0)).sum() / w.fill_null(0).sum()
    if op == "ratio":
        if not agg.denominator_column:
            raise _invalid_agg("ratio requires a 'denominator_column'.")
        return col.sum() / pl.col(agg.denominator_column).sum()
    raise _invalid_agg(f"Unsupported operation '{op}'.")


def _build_agg_exprs(aggregations: List[AggregationSpec]) -> List[pl.Expr]:
    """Translate AggregationSpecs into Polars aggregation expressions.

    Note: For chunked processing, avg is computed as sum/count. Polars handles
    this correctly when aggregating a full DataFrame, but if we ever stream
    chunks, we must aggregate sum and count separately to maintain numerical correctness.
    """
    if not aggregations:
        raise tool_error("INVALID_ARGUMENT", "At least one aggregation is required.")
    return [_build_agg_expr(agg) for agg in aggregations]


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
    return _clean_meta_type(p_col.type) if p_col else "float64"


def _agg_needed_columns(aggregations: List[AggregationSpec]) -> set:
    """Columns an aggregation list reads (value, weight and denominator columns)."""
    needed = set()
    for agg in aggregations:
        for col in (agg.column, agg.weight_column, agg.denominator_column):
            if col:
                needed.add(col)
    return needed


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


# ---------------------------------------------------------------------------
# join helpers: argument parsing, key data, reporting, frame execution
# ---------------------------------------------------------------------------

_MAX_REPORT_ENTRIES = 20


def _apply_suffix(name: str, suffix: str) -> str:
    return f"{name}{suffix}"


def _parse_join_pairs(on: List[Union[str, JoinColumnPair]]) -> List[Tuple[str, str]]:
    """Normalize `on` entries to (left, right) column pairs and validate them."""
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
    left_names = [p[0] for p in pairs]
    right_names = [p[1] for p in pairs]
    if len(left_names) != len(set(left_names)) or len(right_names) != len(set(right_names)):
        raise tool_error("INVALID_ARGUMENT", "Join key columns must be distinct per side.")
    return pairs


def _resolve_indicator(indicator: Union[str, bool]) -> Optional[str]:
    """Map the `indicator` argument to a column name (None = no indicator column)."""
    if indicator is False:
        return None
    if isinstance(indicator, str):
        return indicator or None
    return "_join_match"  # True -> default name


def _key_tuples(cols: List[List[Any]]) -> List[Optional[tuple]]:
    """Per-row key tuples; None when any component is null (null keys never match)."""
    return [(t if all(v is not None for v in t) else None) for t in zip(*cols)]


def _distinct_raw_order(raw_tuples: List[Optional[tuple]]) -> List[tuple]:
    """Distinct non-null raw keys in first-seen order."""
    order, seen = [], set()
    for rt in raw_tuples:
        if rt is not None and rt not in seen:
            seen.add(rt)
            order.append(rt)
    return order


def _norm_tuple(raw_tuple: tuple, normalize: bool) -> tuple:
    return tuple(normalize_mpn(v) for v in raw_tuple) if normalize else raw_tuple


def _render_key(value) -> str:
    if isinstance(value, tuple):
        return " | ".join(str(v) for v in value)
    return str(value)


@dataclass
class _JoinKeys:
    """One side's join-key columns plus the key statistics derived from them."""

    raw_cols: List[List[Any]]
    norm_cols: List[List[Any]]
    keys: List[Optional[tuple]]        # per-row normalized key tuples
    raw_tuples: List[Optional[tuple]]  # per-row raw key tuples
    counts: Counter                    # normalized key -> row count (null keys excluded)
    raw_order: List[tuple]             # distinct raw keys, first-seen order

    @classmethod
    def _from_cols(cls, raw_cols: List[List[Any]], norm_cols: List[List[Any]]) -> "_JoinKeys":
        keys = _key_tuples(norm_cols)
        raw_tuples = _key_tuples(raw_cols)
        return cls(
            raw_cols=raw_cols,
            norm_cols=norm_cols,
            keys=keys,
            raw_tuples=raw_tuples,
            counts=Counter(k for k in keys if k is not None),
            raw_order=_distinct_raw_order(raw_tuples),
        )

    @classmethod
    def build(cls, df: pl.DataFrame, key_names: List[str], normalize: bool) -> "_JoinKeys":
        raw_cols = [df[c].to_list() for c in key_names]
        norm_cols = [[normalize_mpn(v) for v in col] for col in raw_cols] if normalize else raw_cols
        return cls._from_cols(raw_cols, norm_cols)

    def filtered(self, mask: List[bool]) -> "_JoinKeys":
        """The same key data restricted to the rows kept by `mask`."""
        raw = [[v for v, m in zip(col, mask) if m] for col in self.raw_cols]
        norm = [[v for v, m in zip(col, mask) if m] for col in self.norm_cols]
        return _JoinKeys._from_cols(raw, norm)

    @property
    def key_set(self) -> set:
        return set(self.counts)


@dataclass
class _JoinRequest:
    """The arguments of a join call (everything except `dry_run`)."""

    left_id: str
    right_id: str
    on: List[Union[str, JoinColumnPair]]
    how: str
    columns_left: Optional[List[str]]
    columns_right: Optional[List[str]]
    suffix: str
    normalize_keys: bool
    validate: Optional[str]
    right_duplicates: str
    indicator: Union[str, bool]
    result_label: Optional[str]

    def provenance(self, dedup_count: int) -> ResultProvenance:
        return ResultProvenance(
            op="join",
            parents=[self.left_id, self.right_id],
            parameters={
                "on": self._on_params(),
                "how": self.how,
                "validate": self.validate,
                "right_duplicates": self.right_duplicates,
                "deduplicated_rows": dedup_count,
                "suffix": self.suffix,
                "normalize_keys": self.normalize_keys,
                "columns_left": self.columns_left,
                "columns_right": self.columns_right,
                "indicator": self.indicator if self.indicator is not False else False,
            },
        )

    def source(self, dedup_count: int) -> Dict[str, Any]:
        return {
            "op": "join",
            "parents": [self.left_id, self.right_id],
            "on": self._on_params(),
            "how": self.how,
            "validate": self.validate,
            "right_duplicates": self.right_duplicates,
            "deduplicated_rows": dedup_count,
            "data_quality_note": self._data_quality_note(dedup_count),
        }

    def _on_params(self) -> list:
        return [o.model_dump() if isinstance(o, JoinColumnPair) else o for o in self.on]

    def _data_quality_note(self, dedup_count: int) -> Optional[str]:
        if self.right_duplicates == "first" and dedup_count:
            return f"{dedup_count} right rows deduplicated via 'first' (lowest ordinal per normalized key kept)."
        if self.right_duplicates == "report_only":
            return "Right-side duplicates were kept as-is (report_only); proceed accordingly."
        if self.right_duplicates == "first":
            return "No right-side duplicates found (right_duplicates='first'); nothing deduplicated."
        return None


@dataclass
class _JoinPlan:
    """Everything needed to execute the Polars join and name its output columns."""

    pairs: List[Tuple[str, str]]
    how: str
    left_out: List[str]  # left-side output columns (original names)
    right_final: List[Tuple[str, str, bool, Optional[int]]]  # (orig, final, is_key, key_index)
    indicator_name: Optional[str]

    @property
    def left_shadow(self) -> List[str]:
        return [f"__ln_{i}" for i in range(len(self.pairs))]

    @property
    def right_shadow(self) -> List[str]:
        return [f"__rn_{i}" for i in range(len(self.pairs))]


def _entry_steps(raw_tuples: List[tuple]) -> List[str]:
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


def _collision_entry(norm_key: tuple, raw_tuples: List[tuple], side: str, count=None) -> Dict[str, Any]:
    rendered = [_render_key(r[0] if len(r) == 1 else r) for r in raw_tuples]
    distinct_raw = list(dict.fromkeys(rendered))
    return {
        "normalized": _render_key(norm_key[0] if len(norm_key) == 1 else norm_key),
        "count": count,
        "raw": distinct_raw,
        "side": side,
        "caused_by_normalization": len(distinct_raw) > 1,
        "steps_applied": _entry_steps(raw_tuples),
    }


def _group_raws_by_norm(raw_order: List[tuple], normalize: bool) -> Dict[tuple, List[tuple]]:
    groups: Dict[tuple, List[tuple]] = {}
    for rt in raw_order:
        nt = _norm_tuple(rt, normalize)
        if nt is not None:
            groups.setdefault(nt, []).append(rt)
    return groups


def _dup_entries(raw_order, counts, side, predicate, normalize) -> List[Dict[str, Any]]:
    """Collision entries for duplicate normalized keys satisfying `predicate`."""
    groups = _group_raws_by_norm(raw_order, normalize)
    return [
        _collision_entry(k, groups.get(k, []), side, count=c)
        for k, c in counts.items() if c > 1 and predicate(k)
    ]


def _side_collisions(raw_order, counts, side, normalize) -> List[Dict[str, Any]]:
    """Distinct raw keys collapsing onto one normalized key (capped at 20)."""
    out = []
    for nt, raws in _group_raws_by_norm(raw_order, normalize).items():
        if len(raws) > 1:
            out.append(_collision_entry(nt, raws, side, count=counts.get(nt)))
        if len(out) >= _MAX_REPORT_ENTRIES:
            break
    return out


def _count_altered(raw_order, any_string_key: bool, normalize: bool) -> int:
    """How many distinct raw keys are changed by normalization."""
    if not any_string_key:
        return 0
    return sum(1 for rt in raw_order if _norm_tuple(rt, normalize) != rt)


def _cardinality_guard(validate, right_duplicates, left: _JoinKeys, right: _JoinKeys, normalize) -> None:
    """Enforce `validate` on normalized keys, restricted to *participating* keys.

    A duplicate only endangers a result if its key is present on the OTHER side
    (that is what fan-out means). Duplicates whose key never matches are reported
    in `data_quality`, never blocking. `right_duplicates="report_only"` additionally
    skips the right-side uniqueness enforcement and records a flag in result metadata.
    """
    right_enforced = validate in ("many_to_one", "one_to_one") and right_duplicates == "error"
    left_enforced = validate in ("one_to_one", "one_to_many")
    if right_enforced:
        d = _dup_entries(right.raw_order, right.counts, "right", lambda k: k in left.key_set, normalize)
        if d:
            raise tool_error("CARDINALITY_VIOLATION", f"Right-side join keys are not unique (required: {validate}).", {"keys": d, "side": "right"})
    if left_enforced:
        d = _dup_entries(left.raw_order, left.counts, "left", lambda k: k in right.key_set, normalize)
        if d:
            raise tool_error("CARDINALITY_VIOLATION", f"Left-side join keys are not unique (required: {validate}).", {"keys": d, "side": "left"})


def _data_quality_report(validate, left: _JoinKeys, right: _JoinKeys, normalize) -> JoinDataQualityReport:
    """Non-participating collisions: always surfaced, never blocking.

    These are duplicate normalized keys absent from the other side, so they can never
    distort a match. Legal same-side duplicates implied by the data grain (e.g. one
    left row per article per week under many_to_one) are filtered out as noise unless
    normalization itself collapsed distinct strings or `validate` declares that side
    unique.
    """
    right_constrained = validate in ("many_to_one", "one_to_one")
    left_constrained = validate in ("one_to_one", "one_to_many")

    def is_signal(entry) -> bool:
        if entry["steps_applied"]:
            return True
        if entry["side"] == "right" and right_constrained:
            return True
        return entry["side"] == "left" and left_constrained

    sample = _dup_entries(right.raw_order, right.counts, "right", lambda k: k not in left.key_set, normalize)
    sample += _dup_entries(left.raw_order, left.counts, "left", lambda k: k not in right.key_set, normalize)
    sample = [e for e in sample if is_signal(e)]
    return JoinDataQualityReport(
        non_participating_collisions=len(sample),
        sample=sample[:_MAX_REPORT_ENTRIES],
    )


def _unmatched_key_sample(left: _JoinKeys, right: _JoinKeys, normalize) -> List[Dict[str, str]]:
    """Up to 20 unmatched keys (raw rendering), labelled by side, left side first."""
    left_unmatched = sorted(left.key_set - right.key_set, key=lambda t: min(i for i, k in enumerate(left.keys) if k == t))
    right_only = sorted(right.key_set - left.key_set, key=lambda t: min(i for i, k in enumerate(right.keys) if k == t))
    # raw renderings for the sample: first raw key seen for each normalized key
    raw_by_norm = {"left": {}, "right": {}}
    for rt in left.raw_order:
        raw_by_norm["left"].setdefault(_norm_tuple(rt, normalize), rt)
    for rt in right.raw_order:
        raw_by_norm["right"].setdefault(_norm_tuple(rt, normalize), rt)

    sample: List[Dict[str, str]] = []

    def append(k, side) -> bool:
        raw = raw_by_norm[side].get(k)
        if raw is None:
            return False
        v = _render_key(raw[0] if len(raw) == 1 else raw)
        if any(e["key"] == v and e["side"] == side for e in sample):
            return False
        sample.append({"key": v, "side": side})
        return len(sample) >= _MAX_REPORT_ENTRIES

    for k in left_unmatched:
        if append(k, "left"):
            break
    for k in right_only:
        if append(k, "right"):
            break
    return sample


def _matches_gained(left: _JoinKeys, right: _JoinKeys, any_string_key: bool, enabled_steps) -> int:
    """Left rows that match only under normalization (raw key absent on the right)."""
    if not (any_string_key and any(enabled_steps)):
        return 0
    right_raw_set = set(right.raw_tuples)
    right_raw_set.discard(None)
    gained = 0
    for k, rt in zip(left.keys, left.raw_tuples):
        if k is None or rt is None:
            continue
        if k in right.key_set and rt not in right_raw_set:
            gained += 1
    return gained


def _estimate_join_rows(how: str, left: _JoinKeys, right: _JoinKeys, unmatched_right_rows: int) -> int:
    """Exact output row count, computed from key multiplicities before materializing."""
    estimated = 0
    for k, cnt in left.counts.items():
        rc = right.counts.get(k, 0)
        if rc and rc > 0:
            estimated += rc * cnt
        elif how in ("left", "full"):
            estimated += cnt
    # left rows with a NULL join key are emitted by left/full joins (they never match).
    if how in ("left", "full"):
        estimated += left.keys.count(None)
    if how in ("right", "full"):
        estimated += unmatched_right_rows
    return estimated


def _build_join_report(
    req: _JoinRequest,
    left: _JoinKeys,
    right: _JoinKeys,
    any_string_key: bool,
    indicator_name: Optional[str],
    dedup_count: int,
) -> Tuple[JoinReport, int]:
    """Structural join report (never business measures) + exact output row estimate."""
    _cardinality_guard(req.validate, req.right_duplicates, left, right, req.normalize_keys)
    data_quality = _data_quality_report(req.validate, left, right, req.normalize_keys)

    matched_left_rows = sum(cnt for k, cnt in left.counts.items() if k in right.key_set)
    matched_right_rows = sum(cnt for k, cnt in right.counts.items() if k in left.key_set)
    matched_keys = left.key_set & right.key_set
    unmatched_right_rows = len(right.keys) - matched_right_rows
    estimated_rows = _estimate_join_rows(req.how, left, right, unmatched_right_rows)

    enabled_steps = list(NORMALIZE_STEPS) if (any_string_key and req.normalize_keys) else []
    collisions = []
    if any_string_key:
        collisions = _side_collisions(left.raw_order, left.counts, "left", req.normalize_keys)
        collisions += _side_collisions(right.raw_order, right.counts, "right", req.normalize_keys)

    report = JoinReport(
        how=req.how,
        indicator_column=indicator_name,
        matched_rows=sum(right.counts.get(k, 0) * cnt for k, cnt in left.counts.items() if right.counts.get(k, 0)),
        unmatched_left_rows=len(left.keys) - matched_left_rows,
        unmatched_right_rows=unmatched_right_rows,
        left_keys_total=len(left.key_set),
        left_keys_matched=len(matched_keys),
        left_keys_unmatched=len(left.key_set) - len(matched_keys),
        right_keys_total=len(right.key_set),
        right_keys_matched=len(matched_keys),
        deduplicated_rows=dedup_count,
        unmatched_key_sample=_unmatched_key_sample(left, right, req.normalize_keys),
        normalization=JoinNormalizationReport(
            enabled=enabled_steps,
            left_keys_altered=_count_altered(left.raw_order, any_string_key, req.normalize_keys),
            right_keys_altered=_count_altered(right.raw_order, any_string_key, req.normalize_keys),
            matches_gained_by_normalization=_matches_gained(left, right, any_string_key, enabled_steps),
            collisions=collisions,
        ),
        data_quality=data_quality,
    )
    return report, estimated_rows


def _join_frames(plan: _JoinPlan, left_df, right_df, left_keys: _JoinKeys, right_keys: _JoinKeys):
    """Build the two join input frames: output columns + normalized shadow keys + side markers."""
    ljoin = left_df.with_columns(
        [pl.Series(name, col) for name, col in zip(plan.left_shadow, left_keys.norm_cols)]
        + [pl.lit(1).alias("__sideL")]
    ).select(plan.left_out + plan.left_shadow + ["__sideL"])

    right_exprs = [pl.col(orig).alias(final) for orig, final, _is_key, _ki in plan.right_final]
    right_exprs += [pl.Series(name, col) for name, col in zip(plan.right_shadow, right_keys.norm_cols)]
    right_exprs.append(pl.lit(1).alias("__sideR"))
    rjoin = right_df.select(right_exprs)
    return ljoin, rjoin


def _indicator_expr() -> pl.Expr:
    is_left = pl.col("__sideL").is_not_null()
    is_right = pl.col("__sideR").is_not_null()
    return (
        pl.when(is_left & is_right).then(pl.lit("both"))
        .when(is_left).then(pl.lit("left_only"))
        .otherwise(pl.lit("right_only"))
    )


def _join_output_exprs(plan: _JoinPlan) -> List[pl.Expr]:
    """Final select list; for right/full joins key columns coalesce left and right values."""
    left_key_names = [p[0] for p in plan.pairs]
    if plan.how in ("right", "full"):
        out = [
            pl.coalesce([pl.col(left_col), pl.col(f"__rk_{i}")]).alias(left_col)
            for i, (left_col, _rc) in enumerate(plan.pairs)
        ]
        out += [pl.col(c) for c in plan.left_out if c not in left_key_names]
    else:
        out = [pl.col(c) for c in plan.left_out]
    out += [pl.col(final) for _orig, final, is_key, _ki in plan.right_final if not is_key]
    if plan.indicator_name is not None:
        out.append(_indicator_expr().alias(plan.indicator_name))
    return out


def _join_side_info(parent_col: Optional[ColumnInfo], name: str, origin: ColumnOrigin, source: Optional[str] = None) -> ColumnInfo:
    return ColumnInfo(
        name=name,
        type=_clean_meta_type(parent_col.type if parent_col else "string"),
        null_count=0, non_null_count=0,
        description=parent_col.description if parent_col else None,
        origin=origin, source_column=source if source is not None else name,
    )


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

    # ---------- loading ----------

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

    async def _load_filtered_df(
        self,
        result_id: str,
        columns: Optional[List[str]] = None,
        filters: Optional[FilterGroup] = None,
    ) -> pl.DataFrame:
        """Load `columns` (None = all), auto-loading filter columns, then apply `filters`."""
        load_cols = list(columns) if columns else None
        if filters and filters.clauses and load_cols is not None:
            load_cols = sorted(set(load_cols) | _filter_columns(filters))
        df = await self._load_full_df(result_id, columns=load_cols)
        if filters and filters.clauses:
            df = df.filter(_build_filter_expr(filters, _schema_of(df)))
        return df

    def _clamp_page(self, limit: int, offset: int) -> Tuple[int, int]:
        limit = min(limit, self.settings.TOOL_MAX_ROWS)
        return max(limit, 0), max(offset, 0)

    # ---------- store_result plumbing ----------

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

    async def _store_derived(
        self,
        df: pl.DataFrame,
        *,
        op: str,
        parent_id: str,
        parameters: Dict[str, Any],
        columns_info: List[ColumnInfo],
        grain: str,
        parent_meta: ResultSetMetadata,
        result_label: Optional[str] = None,
        source: Optional[Dict[str, Any]] = None,
    ) -> ResultSetSummary:
        """Persist a derived table with uniform provenance/source assembly."""
        if source is None:
            source = {"op": op, "parent": parent_id, **parameters}
        return await self._store_result_from_df(
            df,
            provenance=ResultProvenance(op=op, parent=parent_id, parameters=parameters),
            columns=columns_info,
            source=source,
            grain=grain,
            source_row_count=parent_meta.row_count,
            result_label=result_label,
        )

    @staticmethod
    def _inherited_column_info(
        parent_meta: ResultSetMetadata,
        name: str,
        origin: ColumnOrigin,
        fallback_desc: Optional[str] = None,
        default_type: str = "string",
    ) -> ColumnInfo:
        """ColumnInfo for a carried-over column, inheriting type+description from the parent."""
        p_col = next((c for c in parent_meta.columns if c.name == name), None)
        return ColumnInfo(
            name=name,
            type=_clean_meta_type(p_col.type if p_col else default_type),
            null_count=0, non_null_count=0,
            description=(p_col.description if p_col else fallback_desc),
            origin=origin, source_column=name,
        )

    @staticmethod
    def _agg_column_infos(
        aggregations: List[AggregationSpec],
        parent_meta: ResultSetMetadata,
        op_name: str,
    ) -> List[ColumnInfo]:
        """ColumnInfo for one aggregation output column each, typed from the operation."""
        infos = []
        for agg in aggregations:
            if agg.operation == "count" and agg.column is None:
                desc = (f"{op_name} aggregation: total row count" if op_name == "aggregate"
                        else f"{op_name} aggregation: count of rows")
            else:
                desc = f"{op_name} aggregation: {agg.operation} of {agg.column}"
            infos.append(ColumnInfo(
                name=agg.alias, type=_agg_result_type(agg, parent_meta),
                null_count=0, non_null_count=0, description=desc,
                origin=ColumnOrigin.aggregate, operation=agg.operation, source_column=agg.column,
            ))
        return infos

    @staticmethod
    def _derived_info(name: str, type_: str, op_name: str) -> ColumnInfo:
        return ColumnInfo(
            name=name, type=type_, null_count=0, non_null_count=0,
            description=f"{op_name} derived: {name}", origin=ColumnOrigin.derived,
        )

    # ---------- metadata tools ----------

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
            for c in columns:
                if c not in all_columns:
                    raise tool_error("UNKNOWN_COLUMN", f"Column '{c}' does not exist.",
                                     {"available_columns": list(all_columns.keys())})

        df = await self._load_full_df(result_id, columns=columns) if include_examples else None
        result = [self._explain_column(all_columns[c], meta, df) for c in columns]
        return {
            "result_id": result_id,
            "grain": meta.grain,
            "source_row_count": meta.source_row_count,
            "columns": result,
        }

    def _explain_column(self, col_info: ColumnInfo, meta: ResultSetMetadata, df: Optional[pl.DataFrame]) -> Dict[str, Any]:
        entry = {
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
        if df is not None:
            entry["distinct_count"] = _safe(lambda: df[col_info.name].n_unique())
            entry["examples"] = _safe(lambda: _column_examples(df[col_info.name]), default=[])
        return entry

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
        columns = self._check_columns(all_columns, columns)
        percentiles = self._check_percentiles(percentiles)
        if group_columns:
            if len(group_columns) > self.settings.TOOL_MAX_GROUP_COLUMNS:
                raise tool_error("INVALID_ARGUMENT",
                                 f"Too many group columns (max {self.settings.TOOL_MAX_GROUP_COLUMNS}).")
            self._check_columns(all_columns, group_columns)

        needed = list(set(columns) | set(group_columns or []))
        df = await self._load_filtered_df(result_id, needed, filters)

        if group_columns:
            return self._describe_by_group(result_id, df, columns, group_columns, percentiles, all_columns)
        return {
            "result_id": result_id,
            "grain": "single summary row",
            "percentiles": percentiles,
            "columns": [self._describe_series(c, df[c], percentiles) for c in columns],
        }

    @staticmethod
    def _check_columns(all_columns: Dict[str, ColumnInfo], columns: Optional[List[str]]) -> List[str]:
        """Default to all columns; validate existence otherwise."""
        if columns is None:
            return list(all_columns.keys())
        for c in columns:
            if c not in all_columns:
                raise tool_error("UNKNOWN_COLUMN", f"Column '{c}' does not exist.",
                                 {"available_columns": list(all_columns.keys())})
        return columns

    @staticmethod
    def _check_percentiles(percentiles: Optional[List[float]]) -> List[float]:
        if percentiles is None:
            return [0.05, 0.25, 0.5, 0.75, 0.95]
        for q in percentiles:
            if not (0.0 <= q <= 1.0):
                raise tool_error("INVALID_ARGUMENT",
                                 f"Percentile value must be between 0 and 1, got {q}.")
        return percentiles

    def _describe_by_group(self, result_id, df, columns, group_columns, percentiles, all_columns) -> Dict[str, Any]:
        rows = []
        for keys, sub in df.group_by(group_columns):
            key_row = {gc: _normalize_value(k) for gc, k in zip(group_columns, keys)}
            key_row["columns"] = {c: self._describe_series(c, sub[c], percentiles) for c in columns}
            rows.append(key_row)
        return {
            "result_id": result_id,
            "grain": f"one row per ({', '.join(group_columns)})",
            "percentiles": percentiles,
            "column_types": {c: _clean_meta_type(all_columns[c].type) for c in columns},
            "groups": rows,
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
        stats["min"] = _normalize_value(nn.min())
        stats["max"] = _normalize_value(nn.max())
        if is_numeric:
            # mean/stddev are only valid on numeric columns; temporal (date/datetime)
            # columns support min/max but not these aggregates.
            stats["mean"] = _normalize_value(nn.mean())
            stats["stddev"] = _normalize_value(nn.std())
            stats["skewness"] = _normalize_value(nn.skew())
            stats["count_negative"] = int((nn < 0).sum())
            stats["percentiles"] = {
                str(q): _normalize_value(nn.quantile(q, interpolation="linear")) for q in percentiles
            }
        return stats

    # ---------- row access tools ----------

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
        limit, offset = self._clamp_page(limit, offset)
        meta = await self._ensure_owned(result_id)
        self._check_format(format, encoding)
        effective_sort = _stable_tiebreak(sort, meta) if (stable_sort and sort) else sort

        if store_result:
            return await self._store_list_rows(result_id, columns, sort, effective_sort, meta)

        df_slice, total_rows = await self._read_page_slice(result_id, columns, effective_sort, limit, offset, meta)
        df_slice = _apply_rounding(df_slice, round_to)
        return _encode_row_page(result_id, df_slice, offset, limit, total_rows, format, encoding)

    @staticmethod
    def _check_format(format: str, encoding: str) -> None:
        if format not in {"json", "csv"}:
            raise tool_error("INVALID_FORMAT", f"Unsupported format '{format}'. Must be 'json' or 'csv'.")
        if encoding not in {"plain", "gzip+base64"}:
            raise tool_error("INVALID_ENCODING", f"Unsupported encoding '{encoding}'. Must be 'plain' or 'gzip+base64'.")

    async def _read_page_slice(self, result_id, columns, effective_sort, limit, offset, meta) -> Tuple[pl.DataFrame, int]:
        """Read one page; sorts require the full frame, unsorted reads only the needed range."""
        if effective_sort:
            df = await self._load_full_df(result_id)
            df = _apply_sort(df, effective_sort)
            if columns:
                df = df.select(columns)
            return df.slice(offset, limit), df.height
        total_rows = meta.row_count
        if limit == 0 or offset >= total_rows:
            return pl.DataFrame(), total_rows
        end = min(offset + limit, total_rows)
        return await self._load_range_df(result_id, offset, end, columns=columns), total_rows

    async def _store_list_rows(self, result_id, columns, sort, effective_sort, meta) -> ResultSetSummary:
        # Load full table if sorting, to ensure sort columns are present before projection.
        df = await self._load_full_df(result_id) if effective_sort else await self._load_full_df(result_id, columns=columns)
        if effective_sort:
            df = _apply_sort(df, effective_sort)
        if columns:
            df = df.select(columns)
        cols_info = [self._inherited_column_info(meta, c, ColumnOrigin.passthrough) for c in df.columns]
        params = {"columns": columns, "sort": [s.model_dump() for s in sort] if sort else None}
        return await self._store_derived(
            df, op="list_rows", parent_id=result_id, parameters=params,
            columns_info=cols_info, grain="one row per source row", parent_meta=meta,
        )

    async def count_rows(
        self,
        result_id: str,
        filters: Optional[FilterGroup] = None,
    ) -> Dict[str, Any]:
        meta = await self._ensure_owned(result_id)
        if not (filters and filters.clauses):
            return {"result_id": result_id, "count": meta.row_count}
        df = await self._load_filtered_df(result_id, filters=filters)
        return {"result_id": result_id, "count": df.height}

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
        limit, offset = self._clamp_page(limit, offset)
        meta = await self._ensure_owned(result_id)
        df = await self._load_filtered_df(result_id, columns, filters)
        if sort:
            df = _apply_sort(df, sort)
        if columns:
            df = df.select(columns)

        if store_result:
            cols_info = [self._inherited_column_info(meta, c, ColumnOrigin.passthrough) for c in df.columns]
            params = {
                "filters": filters.model_dump() if filters else None,
                "columns": columns,
                "sort": [s.model_dump() for s in sort] if sort else None,
            }
            return await self._store_derived(
                df, op="query_rows", parent_id=result_id, parameters=params,
                columns_info=cols_info, grain="one row per source row", parent_meta=meta,
            )
        return _format_paginated_result(result_id, df, offset, limit, df.height)

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
        df = await self._load_filtered_df(result_id, [column], filters)
        value_counts = _value_counts_frame(df, column, include_null, order)

        if store_result:
            cols_info = [
                self._inherited_column_info(meta, column, ColumnOrigin.passthrough, f"distinct_values passthrough: {column}"),
                self._derived_info("count", "int64", "distinct_values"),
            ]
            params = {"column": column, "filters": filters.model_dump() if filters else None}
            return await self._store_derived(
                value_counts, op="distinct_values", parent_id=result_id, parameters=params,
                columns_info=cols_info, grain=f"one row per distinct {column}",
                parent_meta=meta, result_label=result_label,
            )

        total_distinct = value_counts.height
        values = []
        for row in value_counts.slice(offset, limit).iter_rows(named=True):
            entry = {"value": _normalize_value(row[column])}
            if include_counts:
                entry["count"] = row["count"]
            values.append(entry)
        has_more = offset + limit < total_distinct
        return {
            "result_id": result_id,
            "column": column,
            "returned": len(values),
            "total_distinct": total_distinct,
            "has_more": has_more,
            "next_offset": offset + limit if has_more else None,
            "values": values,
        }

    # ---------- aggregation tools ----------

    async def aggregate(
        self,
        result_id: str,
        aggregations: List[AggregationSpec],
        filters: Optional[FilterGroup] = None,
        store_result: bool = False,
        result_label: Optional[str] = None,
    ) -> Union[Dict[str, Any], ResultSetSummary]:
        meta = await self._ensure_owned(result_id)
        _validate_aliases(aggregations)
        needed = _agg_needed_columns(aggregations)
        df = await self._load_filtered_df(result_id, list(needed) if needed else None, filters)
        result_df = df.select(_build_agg_exprs(aggregations))

        if store_result:
            params = {
                "aggregations": [a.model_dump() for a in aggregations],
                "filters": filters.model_dump() if filters else None,
            }
            return await self._store_derived(
                result_df, op="aggregate", parent_id=result_id, parameters=params,
                columns_info=self._agg_column_infos(aggregations, meta, "aggregate"),
                grain="single row", parent_meta=meta, result_label=result_label,
            )

        row = result_df.to_dicts()[0]
        return {"result_id": result_id, "aggregations": {k: _normalize_value(v) for k, v in row.items()}}

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
        _validate_aliases(aggregations, group_columns)
        needed = set(group_columns) | _agg_needed_columns(aggregations)
        df = await self._load_filtered_df(result_id, list(needed), filters)

        grouped = df.group_by(group_columns).agg(_build_agg_exprs(aggregations))
        # Enforce max groups to prevent memory exhaustion from high-cardinality groupings.
        if grouped.height > self.settings.RESULTSET_MAX_GROUPS:
            raise tool_error(
                "TOO_MANY_GROUPS",
                f"Group cardinality {grouped.height} exceeds limit {self.settings.RESULTSET_MAX_GROUPS}."
            )
        if sort:
            grouped = _apply_sort(grouped, sort)

        if store_result:
            return await self._store_grouped(result_id, grouped, group_columns, aggregations, filters, sort, meta, result_label)

        rows = _df_to_rows(grouped.slice(offset, limit))
        has_more = offset + limit < grouped.height
        return {
            "result_id": result_id,
            "group_columns": group_columns,
            "offset": offset,
            "limit": limit,
            "returned": len(rows),
            "total_groups": grouped.height,
            "has_more": has_more,
            "next_offset": offset + limit if has_more else None,
            "rows": rows,
        }

    async def _store_grouped(self, result_id, grouped, group_columns, aggregations, filters, sort, meta, result_label) -> ResultSetSummary:
        cols_info = [
            self._inherited_column_info(meta, gc, ColumnOrigin.group_key, f"group_by group key: {gc}")
            for gc in group_columns
        ] + self._agg_column_infos(aggregations, meta, "group_by")
        params = {
            "group_columns": group_columns,
            "aggregations": [a.model_dump() for a in aggregations],
            "filters": filters.model_dump() if filters else None,
            "sort": [s.model_dump() for s in sort] if sort else None,
        }
        # `source` intentionally mirrors the spec example (no `sort` field).
        source = {
            "op": "group_by", "parent": result_id, "group_columns": group_columns,
            "aggregations": params["aggregations"], "filters": params["filters"],
        }
        return await self._store_derived(
            grouped, op="group_by", parent_id=result_id, parameters=params,
            columns_info=cols_info, grain=f"one row per ({', '.join(group_columns)})",
            parent_meta=meta, result_label=result_label, source=source,
        )

    # ---------- histogram ----------

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
        df = await self._load_filtered_df(result_id, [column], filters)
        non_null = df.drop_nulls()
        null_count = df.height - non_null.height

        hist_df, cols_info, rows, extra = self._build_histogram(non_null, column, bins, meta)
        if store_result:
            params = {"column": column, "bins": bins, "filters": filters.model_dump() if filters else None}
            return await self._store_derived(
                hist_df, op="histogram", parent_id=result_id, parameters=params,
                columns_info=cols_info, grain=f"one row per histogram bin for {column}",
                parent_meta=meta, result_label=result_label,
            )
        return {
            "result_id": result_id,
            "column": column,
            "bins": len(rows),
            "null_count": null_count,
            "histogram": rows,
            **extra,
        }

    def _build_histogram(self, non_null: pl.DataFrame, column: str, bins: int, meta) -> Tuple[pl.DataFrame, List[ColumnInfo], list, dict]:
        """Return (stored table, columns_info, inline rows, extra response fields)."""
        if non_null.height == 0:
            empty = pl.DataFrame({column: [], "count": []})
            infos = self._histogram_value_infos(meta, column)
            return empty, infos, [], {}
        if non_null[column].dtype in (pl.Utf8, pl.Categorical):
            return self._string_histogram(non_null, column, bins, meta)
        return self._numeric_histogram(non_null, column, bins, meta)

    def _histogram_value_infos(self, meta, column: str) -> List[ColumnInfo]:
        return [
            self._inherited_column_info(meta, column, ColumnOrigin.passthrough, f"histogram passthrough: {column}"),
            self._derived_info("count", "int64", "histogram"),
        ]

    def _histogram_binned_infos(self, meta, column: str) -> List[ColumnInfo]:
        p_col = next((c for c in meta.columns if c.name == column), None)
        p_type = _clean_meta_type(p_col.type if p_col else "float64")
        return [
            self._derived_info("bin_index", "int64", "histogram"),
            self._derived_info("bin_lower", p_type, "histogram"),
            self._derived_info("bin_upper", p_type, "histogram"),
            self._derived_info("bin_label", "string", "histogram"),
            self._derived_info("count", "int64", "histogram"),
        ]

    def _string_histogram(self, non_null: pl.DataFrame, column: str, bins: int, meta):
        """Top-N values by count plus an explicit trailing `is_other` remainder row."""
        max_bins = min(bins, self.settings.TOOL_MAX_DISTINCT_VALUES)
        value_counts = non_null.group_by(column).agg(pl.len().alias("count")).sort("count", descending=True)
        total_distinct = value_counts.height
        if total_distinct > max_bins:
            top = value_counts.slice(0, max_bins).with_columns(pl.lit(False).alias("is_other"))
            other = pl.DataFrame({
                column: [None],
                "count": [value_counts.slice(max_bins)["count"].sum()],
                "is_other": [True],
            })
            final_df = pl.concat([top, other], how="vertical_relaxed")
        else:
            final_df = value_counts.with_columns(pl.lit(False).alias("is_other"))

        cols_info = self._histogram_value_infos(meta, column) + [self._derived_info("is_other", "bool", "histogram")]
        rows = [
            {"value": _normalize_value(r[column]), "count": r["count"], "is_other": r["is_other"]}
            for r in final_df.iter_rows(named=True)
        ]
        return final_df, cols_info, rows, {"total_distinct": total_distinct}

    def _numeric_histogram(self, non_null: pl.DataFrame, column: str, bins: int, meta):
        col_min = non_null[column].min()
        col_max = non_null[column].max()
        if col_min is None or col_max is None:
            empty = pl.DataFrame({column: [], "count": []})
            return empty, self._histogram_binned_infos(meta, column), [], {}
        try:
            hist_df = non_null[column].hist(bin_count=bins)
        except pl.exceptions.InvalidOperationError:
            raise tool_error(
                "INVALID_ARGUMENT",
                f"Column '{column}' is not numeric and cannot be used for histograms."
            )
        hist_df = _format_bins(hist_df)
        rows = [
            {
                "bin_index": r["bin_index"],
                "bin_lower": _normalize_value(r["bin_lower"]),
                "bin_upper": _normalize_value(r["bin_upper"]),
                "bin_label": r["bin_label"],
                "count": r["count"],
            }
            for r in hist_df.iter_rows(named=True)
        ]
        return hist_df, self._histogram_binned_infos(meta, column), rows, {}

    # ---------- join ----------

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
        req = _JoinRequest(left_result_id, right_result_id, on, how, columns_left, columns_right,
                           suffix, normalize_keys, validate, right_duplicates, indicator, result_label)
        self._check_join_enums(req)
        pairs = _parse_join_pairs(req.on)
        indicator_name = _resolve_indicator(req.indicator)
        left_meta, right_meta, left_df, right_df, any_str_key, plan = await self._prepare_join(req, pairs, indicator_name)

        left_keys = _JoinKeys.build(left_df, [p[0] for p in pairs], req.normalize_keys)
        right_keys = _JoinKeys.build(right_df, [p[1] for p in pairs], req.normalize_keys)
        right_df, right_keys, dedup_count = self._dedup_right(right_df, right_keys, req.right_duplicates)

        report, estimated_rows = _build_join_report(req, left_keys, right_keys, any_str_key, indicator_name, dedup_count)
        if estimated_rows > self.settings.RESULTSET_MAX_ROWS:
            raise tool_error(
                "RESULT_SET_TOO_LARGE",
                f"Join output would exceed {self.settings.RESULTSET_MAX_ROWS} rows.",
                {"estimated_row_count": estimated_rows},
            )
        # dry run: the report without materializing anything; `validate` and the size
        # guard above are still enforced, so collisions can be inspected safely.
        if dry_run:
            return JoinDryRunReport(report=report, estimated_row_count=estimated_rows)

        result_df = self._execute_join(plan, left_df, right_df, left_keys, right_keys)
        summary = await self._store_result_from_df(
            result_df,
            provenance=req.provenance(dedup_count),
            columns=self._join_column_info(left_meta, right_meta, plan),
            source=req.source(dedup_count),
            grain=self._join_grain(req, left_meta, right_meta),
            source_row_count=self._join_source_row_count(req.how, left_meta, right_meta),
            result_label=req.result_label,
        )
        return JoinResultSummary(**summary.model_dump(), join=report)

    @staticmethod
    def _check_join_enums(req: _JoinRequest) -> None:
        if not req.on:
            raise tool_error("INVALID_ARGUMENT", "'on' must list at least one join key.")
        if req.how not in ("left", "inner", "right", "full"):
            raise tool_error("INVALID_ARGUMENT", f"Invalid 'how' value '{req.how}'. Must be one of 'left', 'inner', 'right', 'full'.", {"offending_value": req.how})
        if req.validate not in (None, "many_to_one", "one_to_one", "one_to_many", "many_to_many"):
            raise tool_error("INVALID_ARGUMENT", f"Invalid 'validate' value '{req.validate}'. Must be one of 'many_to_one', 'one_to_one', 'one_to_many', 'many_to_many' or null.", {"offending_value": req.validate})
        if req.right_duplicates not in ("error", "first", "report_only"):
            raise tool_error("INVALID_ARGUMENT", f"Invalid 'right_duplicates' value '{req.right_duplicates}'. Must be one of 'error', 'first', 'report_only'.", {"offending_value": req.right_duplicates})

    async def _prepare_join(self, req: _JoinRequest, pairs, indicator_name):
        """Load both sides, check key types, and resolve the output-column plan."""
        left_meta = await self._ensure_owned(req.left_id)
        right_meta = await self._ensure_owned(req.right_id)
        left_df = await self._load_full_df(req.left_id)
        right_df = await self._load_full_df(req.right_id)
        key_is_string = self._check_key_types(left_df, right_df, pairs)
        left_out, right_final = self._plan_output_columns(left_df, right_df, pairs, req, indicator_name)
        plan = _JoinPlan(pairs=pairs, how=req.how, left_out=left_out, right_final=right_final, indicator_name=indicator_name)
        return left_meta, right_meta, left_df, right_df, any(key_is_string), plan

    @staticmethod
    def _check_key_types(left_df, right_df, pairs) -> List[bool]:
        """Key columns must exist and be type-compatible; returns per-pair string-key flags."""
        for left_col, right_col in pairs:
            if left_col not in left_df.columns:
                raise tool_error("UNKNOWN_COLUMN", f"Column '{left_col}' does not exist on the left side.", {"available_columns": list(left_df.columns), "side": "left"})
            if right_col not in right_df.columns:
                raise tool_error("UNKNOWN_COLUMN", f"Column '{right_col}' does not exist on the right side.", {"available_columns": list(right_df.columns), "side": "right"})
        key_is_string: List[bool] = []
        for left_col, right_col in pairs:
            left_dt = left_df[left_col].dtype
            right_dt = right_df[right_col].dtype
            details = {"left_column": left_col, "left_type": str(left_dt), "right_column": right_col, "right_type": str(right_dt)}
            if _is_string_dtype(left_dt) and _is_string_dtype(right_dt):
                key_is_string.append(True)
            elif _is_string_dtype(left_dt) != _is_string_dtype(right_dt):
                raise tool_error("JOIN_KEY_TYPE_MISMATCH", f"Key columns '{left_col}' (left) and '{right_col}' (right) have incompatible types.", details)
            elif left_dt != right_dt:
                raise tool_error("JOIN_KEY_TYPE_MISMATCH", f"Key columns '{left_col}' (left) and '{right_col}' (right) must share the same type for non-string keys.", details)
            else:
                key_is_string.append(False)
        return key_is_string

    @staticmethod
    def _plan_output_columns(left_df, right_df, pairs, req: _JoinRequest, indicator_name):
        """Resolve projections and final right-side names (suffix on collision, else error)."""
        left_names = list(left_df.columns)
        right_names = list(right_df.columns)
        for c in (req.columns_left or []):
            if c not in left_names:
                raise tool_error("UNKNOWN_COLUMN", f"Column '{c}' does not exist on the left side.", {"available_columns": left_names, "side": "left"})
        for c in (req.columns_right or []):
            if c not in right_names:
                raise tool_error("UNKNOWN_COLUMN", f"Column '{c}' does not exist on the right side.", {"available_columns": right_names, "side": "right"})

        left_key_names = [p[0] for p in pairs]
        right_key_names = [p[1] for p in pairs]
        left_sel = set(req.columns_left) if req.columns_left is not None else None
        right_sel = set(req.columns_right) if req.columns_right is not None else None
        left_out = list(dict.fromkeys(c for c in left_names if c in left_key_names or (left_sel is None or c in left_sel)))
        right_out = list(dict.fromkeys(c for c in right_names if c in right_key_names or (right_sel is None or c in right_sel)))

        used = set(left_out)
        right_final: List[Tuple[str, str, bool, Optional[int]]] = []
        for c in right_out:
            if c in right_key_names:
                ki = right_key_names.index(c)
                right_final.append((c, f"__rk_{ki}", True, ki))
                continue
            final = c if c not in used else _apply_suffix(c, req.suffix)
            if final in used:
                raise tool_error("COLUMN_NAME_COLLISION", f"Column name collision for '{final}'. Rename with a different 'suffix'.", {"conflicting_names": [final]})
            used.add(final)
            right_final.append((c, final, False, None))

        if indicator_name is not None and indicator_name in used:
            raise tool_error("COLUMN_NAME_COLLISION", f"Indicator column name '{indicator_name}' collides with an existing column.", {"conflicting_names": [indicator_name]})
        return left_out, right_final

    @staticmethod
    def _dedup_right(right_df, right_keys: _JoinKeys, right_duplicates: str):
        """right_duplicates="first": keep the lowest-ordinal right row per normalized key.

        This cannot fan out (each left row matches at most one right row), preserving
        row-count and sums. The removed-row count is reported and recorded in provenance.
        """
        if right_duplicates != "first":
            return right_df, right_keys, 0
        seen: set = set()
        mask = []
        for k in right_keys.keys:
            keep = k is None or k not in seen
            if k is not None:
                seen.add(k)
            mask.append(keep)
        removed = mask.count(False)
        if not removed:
            return right_df, right_keys, 0
        return right_df.filter(pl.Series(mask)), right_keys.filtered(mask), removed

    @staticmethod
    def _execute_join(plan: _JoinPlan, left_df, right_df, left_keys: _JoinKeys, right_keys: _JoinKeys) -> pl.DataFrame:
        ljoin, rjoin = _join_frames(plan, left_df, right_df, left_keys, right_keys)
        joined = ljoin.join(rjoin, left_on=plan.left_shadow, right_on=plan.right_shadow, how=plan.how)
        return joined.select(_join_output_exprs(plan))

    @staticmethod
    def _join_column_info(left_meta, right_meta, plan: _JoinPlan) -> List[ColumnInfo]:
        left_map = {c.name: c for c in left_meta.columns}
        right_map = {c.name: c for c in right_meta.columns}
        infos = [_join_side_info(left_map.get(c), c, ColumnOrigin.join_left) for c in plan.left_out]
        infos += [
            _join_side_info(right_map.get(orig), final, ColumnOrigin.join_right, source=orig)
            for orig, final, is_key, _ki in plan.right_final if not is_key
        ]
        if plan.indicator_name is not None:
            infos.append(ColumnInfo(
                name=plan.indicator_name,
                type="string",
                null_count=0, non_null_count=0,
                description="Join match indicator: 'both', 'left_only', or 'right_only'",
                origin=ColumnOrigin.join_generated,
            ))
        return infos

    @staticmethod
    def _join_grain(req: _JoinRequest, left_meta, right_meta) -> Optional[str]:
        if req.validate in ("one_to_many", "many_to_many") or req.how == "full":
            return None
        if req.how == "right":
            return right_meta.grain
        return left_meta.grain

    @staticmethod
    def _join_source_row_count(how, left_meta, right_meta) -> Optional[int]:
        if how == "right":
            return right_meta.row_count
        if how == "full":
            return None
        return left_meta.row_count

    # ---------- deletion ----------

    async def delete_result_set(self, result_id: str) -> Dict[str, Any]:
        meta = await self._ensure_owned(result_id)
        deleted = await self.store.delete_result(self.session_id, result_id)
        return {"result_id": result_id, "deleted": deleted}


# ---------------------------------------------------------------------------
# remaining module-level helpers that need no engine state
# ---------------------------------------------------------------------------


def _column_examples(series: pl.Series) -> List[Any]:
    """Up to 3 distinct non-null, non-empty example values."""
    non_null = series.drop_nulls()
    if non_null.dtype in (pl.Utf8, pl.Categorical):
        non_null = non_null.filter(non_null != "")
    if non_null.len() == 0:
        return []
    return [_normalize_value(v) for v in non_null.unique(maintain_order=True).head(3).to_list()]


def _value_counts_frame(df: pl.DataFrame, column: str, include_null: bool, order: str) -> pl.DataFrame:
    """Group-by-count frame for `distinct_values`, ordered per the `order` enum."""
    frame = df if include_null else df.drop_nulls()
    value_counts = frame.group_by(column).agg(pl.len().alias("count"))
    if order == "count_desc":
        return value_counts.sort("count", descending=True)
    if order == "count_asc":
        return value_counts.sort("count", descending=False)
    if order == "value_asc":
        return value_counts.sort(column, descending=False)
    if order == "value_desc":
        return value_counts.sort(column, descending=True)
    raise tool_error("INVALID_ARGUMENT", f"Unsupported order '{order}'.")


def _format_bins(hist_df: pl.DataFrame) -> pl.DataFrame:
    """Reshape Polars' hist() output into bin_index/lower/upper/label/count columns."""
    hist_df = hist_df.rename({"breakpoint": "bin_upper"})
    hist_df = hist_df.with_columns([
        pl.col("bin_upper").shift(1).fill_null(float("-inf")).alias("bin_lower"),
        pl.arange(0, hist_df.height).alias("bin_index"),
    ])
    hist_df = hist_df.with_columns(
        pl.format("[{}, {}]", pl.col("bin_lower"), pl.col("bin_upper")).alias("bin_label")
    )
    return hist_df.select(["bin_index", "bin_lower", "bin_upper", "bin_label", "count"])
