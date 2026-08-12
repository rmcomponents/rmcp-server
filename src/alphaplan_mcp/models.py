# Defines reusable Pydantic models for generic tool arguments,
# including filtering, sorting, and aggregation specifications.
"""Pydantic models for metadata, filters, sorting, and aggregations."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

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
    row_count: int
    column_count: int
    uncompressed_size_bytes: int
    stored_size_bytes: int
    chunk_count: int
    columns: List[ColumnInfo]

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
    column: Optional[str] = None  # not used for "count"
    operation: str
    alias: str


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
    unmatched_key_sample: List[str] = []
    normalization: JoinNormalizationReport


class JoinResultSummary(ResultSetSummary):
    """ResultSetSummary extended with the join's structural report."""

    join: Optional[JoinReport] = None
