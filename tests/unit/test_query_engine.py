"""Unit tests for the QueryEngine (filtering, sorting, aggregation)."""

import pyarrow as pa
import polars as pl
import pytest
import pytest_asyncio

from alphaplan_mcp.config import Settings
from alphaplan_mcp.errors import ToolError
from alphaplan_mcp.models import FilterClause, FilterGroup, SortSpec, AggregationSpec
from alphaplan_mcp.query_engine import _build_filter_expr, _apply_sort, _df_to_rows, QueryEngine
from alphaplan_mcp.result_store import ResultSetStore


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
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="like", value="bob")]), schema)
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
        with pytest.raises(ToolError, match="does not exist"):
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


@pytest_asyncio.fixture
async def setup_engine(fake_redis):
    store = ResultSetStore(fake_redis)
    session_id = "test_session"
    
    table = pa.table({
        "name": ["Alice", "bob", "Alice", "Charlie", "Bob", None],
        "value": [10, 20, 30, 40, 50, 60],
        "active": [True, False, True, True, False, True],
        "age": [25, 30, 35, 40, 45, None],
    })
    
    summary = await store.store_result(session_id, "test_source", None, table)
    engine = QueryEngine(store, session_id)
    return engine, summary.result_id


class TestQueryEngine:
    @pytest.mark.asyncio
    async def test_list_rows(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.list_rows(result_id, limit=2)
        assert res["returned"] == 2
        assert res["total_rows"] == 6
        assert res["has_more"] is True
        assert res["next_offset"] == 2

    @pytest.mark.asyncio
    async def test_list_rows_sort(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.list_rows(result_id, sort=[SortSpec(column="value", direction="desc")], limit=2)
        assert res["rows"][0]["value"] == 60
        assert res["rows"][1]["value"] == 50

    @pytest.mark.asyncio
    async def test_count_rows(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.count_rows(result_id)
        assert res["count"] == 6

    @pytest.mark.asyncio
    async def test_count_rows_filtered(self, setup_engine):
        engine, result_id = setup_engine
        filters = FilterGroup(clauses=[FilterClause(column="value", operator="gt", value=30)])
        res = await engine.count_rows(result_id, filters=filters)
        assert res["count"] == 3

    @pytest.mark.asyncio
    async def test_query_rows(self, setup_engine):
        engine, result_id = setup_engine
        filters = FilterGroup(clauses=[FilterClause(column="active", operator="eq", value=True)])
        res = await engine.query_rows(result_id, filters=filters, columns=["name", "value"])
        assert res["total_rows"] == 4
        assert "age" not in res["rows"][0]

    @pytest.mark.asyncio
    async def test_distinct_values(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.distinct_values(result_id, column="name", include_null=True)
        # Alice, bob, Charlie, Bob, None -> 5 distinct
        assert res["total_distinct"] == 5

    @pytest.mark.asyncio
    async def test_aggregate(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="sum", column="value", alias="total_value")]
        res = await engine.aggregate(result_id, aggregations=aggs)
        assert res["aggregations"]["total_value"] == 210

    @pytest.mark.asyncio
    async def test_group_by(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="count", alias="count")]
        res = await engine.group_by(result_id, group_columns=["active"], aggregations=aggs)
        assert res["total_groups"] == 2

    @pytest.mark.asyncio
    async def test_histogram(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.histogram(result_id, column="value", bins=2)
        assert res["bins"] == 2
        assert res["null_count"] == 0
