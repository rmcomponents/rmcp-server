# Defines reusable Pydantic models for generic tool arguments,
# including filtering, sorting, and aggregation specifications.
"""Pydantic models for metadata, filters, sorting, and aggregations."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, model_validator

class ColumnOrigin(str, Enum):
    group_key = "group_key"
    aggregate = "aggregate"
    passthrough = "passthrough"
    derived = "derived"
    join_left = "join_left"
    join_right = "join_right"
    join_generated = "join_generated"

class ResultProvenance(BaseModel):
    op: str
    parent: Optional[str] = None
    parents: Optional[List[str]] = None
    parameters: Optional[Dict[str, Any]] = None

class ColumnInfo(BaseModel):
    name: str
    type: str
    null_count: int
    non_null_count: int
    min: Optional[Any] = None
    max: Optional[Any] = None
    sum: Optional[float] = None
    avg: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    distinct_count: Optional[int] = None
    examples: Optional[List[Any]] = None
    description: Optional[str] = None
    origin: ColumnOrigin = ColumnOrigin.passthrough
    operation: Optional[str] = None
    source_column: Optional[str] = None

class ResultSetMetadata(BaseModel):
    result_id: str
    owner_session_id: str
    provenance: ResultProvenance
    source: Optional[Dict[str, Any]] = None
    grain: Optional[str] = None
    source_row_count: Optional[int] = None
    result_label: Optional[str] = None
    created_at: datetime
    last_accessed_at: datetime
    absolute_expires_at: datetime
    #: Actual current expiry, reflecting the sliding idle TTL (updated on each access).
    #: Backfilled to `absolute_expires_at` by the validator when reading older stored sets.
    expires_at: Optional[datetime] = None
    row_count: int
    column_count: int
    uncompressed_size_bytes: int
    stored_size_bytes: int
    chunk_count: int
    columns: List[ColumnInfo]

    @model_validator(mode="after")
    def _backfill_expires_at(self):
        if self.expires_at is None and self.absolute_expires_at is not None:
            self.expires_at = self.absolute_expires_at
        return self

class ResultSetSummary(BaseModel):
    result_id: str
    owner_session_id: str
    provenance: ResultProvenance
    source: Optional[Dict[str, Any]] = None
    grain: Optional[str] = None
    source_row_count: Optional[int] = None
    result_label: Optional[str] = None
    row_count: int
    column_count: int
    stored_size_bytes: int
    created_at: datetime
    expires_at: datetime
    columns: List[ColumnInfo]

class FilterClause(BaseModel):
    """A single filter condition.

    Supported `operator` values: `eq`, `ne`, `like`, `not_like`, `regex`, `not_regex`,
    `matches_charset`, `not_matches_charset`, `contains`, `starts_with`, `ends_with`, `in`,
    `not_in`, `between`, `gt`, `gte`, `lt`, `lte`, `is_null`, `is_not_null`.
    """

    column: str
    operator: str
    value: Optional[Any] = None
    case_sensitive: bool = False

class FilterGroup(BaseModel):
    combine: str = "all"          # "all" or "any"
    clauses: List[FilterClause] = []

class SortSpec(BaseModel):
    column: str
    direction: str = "asc"
    nulls: str = "last"

class AggregationSpec(BaseModel):
    """A single aggregation.

    Supported `operation` values:
    - `count` (no column; counts rows), `count_non_null`, `count_distinct` (alias `nunique`)
    - `sum`, `avg` (alias `mean`), `min`, `max`
    - Central tendency: `median`, `percentile` (needs `q`, optional `method`),
      `mode`, `trimmed_mean` (needs `trim`), `weighted_avg` (needs `weight_column`)
    - Spread/shape: `stddev` (optional `ddof`), `variance` (optional `ddof`), `iqr`, `mad`,
      `skewness`, `kurtosis`, `coefficient_of_variation`
    - Counts: `count_null`, `count_zero`, `count_negative`, `count_positive`
    - Ratio: `ratio` (needs `denominator_column`; computes `sum(column)/sum(denominator_column)`)

    All statistical operations exclude nulls before computing (pair with `count_null` if you
    need to know how many were dropped). Percentiles default to `linear` interpolation.
    """

    column: Optional[str] = None  # not used for "count"
    operation: str
    alias: str
    q: Optional[float] = None  # percentile: quantile in [0, 1]
    method: str = "linear"  # percentile: linear | lower | higher | nearest | midpoint
    ddof: int = 1  # stddev / variance: degrees of freedom (1 = sample, 0 = population)
    trim: Optional[float] = None  # trimmed_mean: fraction to drop from each end, [0, 0.5)
    weight_column: Optional[str] = None  # weighted_avg: weight column
    denominator_column: Optional[str] = None  # ratio: denominator column


class JoinColumnPair(BaseModel):
    """A join key pairing a left column with a differently-named right column."""

    left: str
    right: str


class JoinNormalizationReport(BaseModel):
    """Structural effects of key normalization (no business measures)."""

    enabled: List[str]
    left_keys_altered: int = 0
    right_keys_altered: int = 0
    matches_gained_by_normalization: int = 0
    collisions: List[Dict[str, Any]] = []


class JoinDataQualityReport(BaseModel):
    """Non-blocking data-quality findings from a join.

    Records collisions that do *not* participate in the join (the offending normalized key is
    absent from the matching side), so they can never distort a match. These are surfaced, never
    blocking: they neither raise `CARDINALITY_VIOLATION` nor pause the join. Consumers should
    treat them as a standing data-quality signal worth cleaning up at the source.
    """

    non_participating_collisions: int = 0
    sample: List[Dict[str, Any]] = []


class JoinReport(BaseModel):
    """Structural report of a join. Never contains business measures (see AGENTS.md §9)."""

    how: str
    indicator_column: Optional[str] = None
    matched_rows: int = 0
    unmatched_left_rows: int = 0
    unmatched_right_rows: int = 0
    left_keys_total: int = 0
    left_keys_matched: int = 0
    left_keys_unmatched: int = 0
    right_keys_total: int = 0
    right_keys_matched: int = 0
    #: number of right-side ROWS removed by `right_duplicates="first"` deduplication (0 = none)
    deduplicated_rows: int = 0
    unmatched_key_sample: List[Dict[str, str]] = []
    normalization: JoinNormalizationReport
    data_quality: JoinDataQualityReport = JoinDataQualityReport()


class JoinResultSummary(ResultSetSummary):
    """ResultSetSummary extended with the join's structural report."""

    join: Optional[JoinReport] = None


class JoinDryRunReport(BaseModel):
    """Structural report from a dry-run join (no result set is created).

    Returned by `join` when `dry_run=true`. Contains the same structural `report` as a real join
    plus an `estimated_row_count` of what the stored output would have been, but no `result_id`:
    nothing is materialized and `validate` is still enforced.
    """

    dry_run: bool = True
    estimated_row_count: int = 0
    report: JoinReport
