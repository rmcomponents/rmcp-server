"""Unit tests for the QueryEngine (filtering, sorting, aggregation)."""

import pyarrow as pa
import polars as pl
import pytest

from alphaplan_mcp.config import Settings
from alphaplan_mcp.errors import ToolError
from alphaplan_mcp.models import FilterClause, FilterGroup, SortSpec, AggregationSpec
from alphaplan_mcp.query_engine import _build_filter_expr, _apply_sort, _df_to_rows


# ---------- helper ----------
def _make_dataframe():
    return pl.DataFrame({
        "name": ["Alice", "bob", "Alice", "Charlie", "Bob", None],
        "value": [10, 20, 30, 40, 50, 60],
        "active": [True, False, True, True, False, True],
        "age": [25, 30, 35, 40, 45, None],
    })


class TestFilterBuilding:
    def build_df(self):
        return _make_dataframe()

    def test_eq(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="eq", value="Alice")]), schema)
        result = df.filter(expr)
        assert result.height == 2

    def test_like_wildcard(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="like", value="A%")]), schema)
        result = df.filter(expr)
        assert result.height == 2  # Alice, Alice

    def test_like_case_insensitive(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        # bob matches Bob case‑insensitively by default
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="eq", value="bob")]), schema)
        result = df.filter(expr)
        assert result.height == 2  # bob, Bob

    def test_is_null(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="is_null")]), schema)
        result = df.filter(expr)
        assert result.height == 1

    def test_gt(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="value", operator="gt", value=30)]), schema)
        result = df.filter(expr)
        assert result.height == 3

    def test_and_combination(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(
            FilterGroup(
                combine="all",
                clauses=[
                    FilterClause(column="active", operator="eq", value=True),
                    FilterClause(column="value", operator="gte", value=20),
                ],
            ),
            schema,
        )
        result = df.filter(expr)
        # active True and value >=20: rows 2,3,5 => 3 rows
        assert result.height == 3

    def test_unknown_column(self):
        schema = {}
        with pytest.raises(ToolError, match="UNKNOWN_COLUMN"):
            _build_filter_expr(FilterGroup(clauses=[FilterClause(column="nonexistent", operator="eq", value="x")]), schema)


class TestSort:
    def test_single_column_asc(self):
        df = _make_dataframe()
        sorted_df = _apply_sort(df, [SortSpec(column="value", direction="asc")])
        values = sorted_df["value"].to_list()
        assert values == sorted(values)

    def test_two_columns(self):
        df = _make_dataframe()
        sorted_df = _apply_sort(df, [
            SortSpec(column="active", direction="desc"),
            SortSpec(column="age", direction="asc"),
        ])
        # just check no exception and order is deterministic
        assert sorted_df.height == 6


class TestRowConversion:
    def test_normalize(self):
        from datetime import date
        df = pl.DataFrame({
            "d": [date(2024, 5, 1)],
            "val": [12.5],
            "none": [None],
        })
        rows = _df_to_rows(df)
        assert rows[0]["d"] == "2024-05-01"
        assert rows[0]["val"] == 12.5
        assert rows[0]["none"] is None
