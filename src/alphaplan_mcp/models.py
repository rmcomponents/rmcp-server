# Defines reusable Pydantic models for generic tool arguments,
# including filtering, sorting, and aggregation specifications.
"""Pydantic models for metadata, filters, sorting, and aggregations."""

from datetime import datetime
from typing import Any, List, Optional
from pydantic import BaseModel, Field

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

class ResultSetMetadata(BaseModel):
    result_id: str
    source: str
    created_at: datetime
    last_accessed_at: datetime
    absolute_expires_at: datetime
    row_count: int
    column_count: int
    uncompressed_size_bytes: int
    stored_size_bytes: int
    chunk_count: int
    columns: List[ColumnInfo]
    source_parameters: Optional[dict] = None

class ResultSetSummary(BaseModel):
    result_id: str
    source: str
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
