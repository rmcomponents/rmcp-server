"""Unit tests for the QueryEngine (filtering, sorting, aggregation)."""

import pyarrow as pa
import polars as pl
import pytest
import pytest_asyncio

from alphaplan_mcp.config import Settings
from alphaplan_mcp.errors import ToolError
from alphaplan_mcp.models import FilterClause, FilterGroup, SortSpec, AggregationSpec, ResultSetSummary, ResultProvenance, ColumnOrigin
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

    def test_filter_string_column_with_numeric_value(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        # 'name' is Utf8, filter with integer 42
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="eq", value=42)]), schema)
        result = df.filter(expr)
        assert result.height == 0

    def test_filter_numeric_column_with_string_value(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        # 'value' is Int64, filter with string "20"
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="value", operator="eq", value="20")]), schema)
        result = df.filter(expr)
        assert result.height == 1
        assert result["value"][0] == 20


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


class TestListRowsFormats:
    @pytest.mark.asyncio
    async def test_invalid_format(self, setup_engine):
        engine, result_id = setup_engine
        with pytest.raises(ToolError, match="INVALID_FORMAT"):
            await engine.list_rows(result_id, format="parquet")

    @pytest.mark.asyncio
    async def test_invalid_encoding(self, setup_engine):
        engine, result_id = setup_engine
        with pytest.raises(ToolError, match="INVALID_ENCODING"):
            await engine.list_rows(result_id, encoding="zstd")

    @pytest.mark.asyncio
    async def test_paging_fields_json_plain(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.list_rows(result_id, limit=2)
        assert res["offset"] == 0
        assert res["limit"] == 2
        assert res["has_more"] is True
        assert res["next_offset"] == 2
        assert isinstance(res["data"], list)

    @pytest.mark.asyncio
    async def test_paging_fields_csv_plain(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.list_rows(result_id, limit=2, format="csv")
        assert res["offset"] == 0
        assert res["limit"] == 2
        assert res["has_more"] is True
        assert res["next_offset"] == 2
        assert isinstance(res["data"], str)

    @pytest.mark.asyncio
    async def test_paging_fields_json_gzip(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.list_rows(result_id, limit=2, encoding="gzip+base64")
        assert res["offset"] == 0
        assert res["limit"] == 2
        assert res["has_more"] is True
        assert res["next_offset"] == 2
        assert "data" in res
        assert "uncompressed_bytes" in res
        assert "sha256" in res

    @pytest.mark.asyncio
    async def test_round_to_0_casts_to_int(self, setup_engine_float):
        engine, result_id = setup_engine_float
        res = await engine.list_rows(result_id, round_to=0)
        assert isinstance(res["data"][0]["value"], int)
        assert res["data"][0]["value"] == 11

    @pytest.mark.asyncio
    async def test_stable_sort_group_by(self, fake_redis):
        store = ResultSetStore(fake_redis)
        session_id = "test_session"
        table = pa.table({
            "group_col": ["A", "A", "B", "B"],
            "value": [1, 1, 2, 2],
            "name": ["Z", "A", "Y", "B"],
        })
        provenance = ResultProvenance(op="group_by", parameters={"group_columns": ["group_col"]})
        summary = await store.store_result(session_id, provenance, table)
        engine = QueryEngine(store, session_id)
        res = await engine.list_rows(
            summary.result_id,
            sort=[SortSpec(column="value", direction="asc")],
            stable_sort=True
        )
        # Within same value, should be ordered by group_col then name
        names = [r["name"] for r in res["data"]]
        assert names == ["A", "Z", "B", "Y"]

    @pytest.mark.asyncio
    async def test_fallback_to_plain(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.list_rows(result_id, limit=1, encoding="gzip+base64")
        assert res["encoding"] == "plain"
        assert isinstance(res["data"], list)

    @pytest.mark.asyncio
    async def test_csv_escaping(self, fake_redis):
        store = ResultSetStore(fake_redis)
        session_id = "test_session"
        table = pa.table({
            "text": ['comma, here', 'quote " here', 'newline\n here'],
        })
        provenance = ResultProvenance(op="test_source")
        summary = await store.store_result(session_id, provenance, table)
        engine = QueryEngine(store, session_id)
        res = await engine.list_rows(summary.result_id, format="csv")
        assert '"comma, here"' in res["data"]
        assert '"quote "" here"' in res["data"]
        assert '"newline\n here"' in res["data"]

    @pytest.mark.asyncio
    async def test_csv_escaping_quotes_and_newlines(self, fake_redis):
        store = ResultSetStore(fake_redis)
        session_id = "test_session"
        table = pa.table({
            "text": ['quote " here', 'newline\n here'],
        })
        provenance = ResultProvenance(op="test_source")
        summary = await store.store_result(session_id, provenance, table)
        engine = QueryEngine(store, session_id)
        res = await engine.list_rows(summary.result_id, format="csv")
        assert '"quote "" here"' in res["data"]
        assert '"newline\n here"' in res["data"]

    @pytest.mark.asyncio
    async def test_round_to_2_integral_doubles(self, setup_engine_float_integral):
        engine, result_id = setup_engine_float_integral
        res = await engine.list_rows(result_id, round_to=2)
        assert isinstance(res["data"][0]["value"], int)
        assert res["data"][0]["value"] == 10


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
    
    provenance = ResultProvenance(op="test_source")
    summary = await store.store_result(session_id, provenance, table)
    engine = QueryEngine(store, session_id)
    return engine, summary.result_id


@pytest_asyncio.fixture
async def setup_engine_float(fake_redis):
    store = ResultSetStore(fake_redis)
    session_id = "test_session"
    table = pa.table({
        "name": ["Alice", "bob", "Alice", "Charlie", "Bob", None],
        "value": [10.5, 20.5, 30.5, 40.5, 50.5, 60.5],
        "active": [True, False, True, True, False, True],
        "age": [25, 30, 35, 40, 45, None],
    })
    provenance = ResultProvenance(op="test_source")
    summary = await store.store_result(session_id, provenance, table)
    engine = QueryEngine(store, session_id)
    return engine, summary.result_id


@pytest_asyncio.fixture
async def setup_engine_float_integral(fake_redis):
    store = ResultSetStore(fake_redis)
    session_id = "test_session"
    table = pa.table({
        "name": ["Alice", "bob", "Alice", "Charlie", "Bob", None],
        "value": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "active": [True, False, True, True, False, True],
        "age": [25, 30, 35, 40, 45, None],
    })
    provenance = ResultProvenance(op="test_source")
    summary = await store.store_result(session_id, provenance, table)
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

    @pytest.mark.asyncio
    async def test_list_rows_store_result(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.list_rows(result_id, store_result=True)
        assert isinstance(res, ResultSetSummary)
        assert res.row_count == 6
        assert res.provenance.op == "list_rows"
        # Verify the new result set
        new_meta = await engine.store.get_metadata(res.result_id)
        assert new_meta is not None
        assert new_meta.row_count == 6

    @pytest.mark.asyncio
    async def test_list_rows_store_result_empty(self, setup_engine):
        engine, result_id = setup_engine
        # Create an empty result set first
        empty_table = pa.table({})
        empty_summary = await engine.store.store_result("test_session", ResultProvenance(op="test"), empty_table)
        res = await engine.list_rows(empty_summary.result_id, store_result=True)
        assert isinstance(res, ResultSetSummary)
        assert res.row_count == 0

    @pytest.mark.asyncio
    async def test_list_rows_store_result_column_subset(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.list_rows(result_id, columns=["name", "value"], store_result=True)
        assert isinstance(res, ResultSetSummary)
        assert res.row_count == 6
        assert res.column_count == 2
        new_meta = await engine.store.get_metadata(res.result_id)
        assert new_meta.column_count == 2

    @pytest.mark.asyncio
    async def test_list_rows_store_result_ignores_limit(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.list_rows(result_id, limit=2, store_result=True)
        assert isinstance(res, ResultSetSummary)
        assert res.row_count == 6  # Full set, not 2

    @pytest.mark.asyncio
    async def test_list_rows_columns_and_sort(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.list_rows(
            result_id, 
            columns=["name", "value"], 
            sort=[SortSpec(column="value", direction="desc")],
            stable_sort=True
        )
        assert res["returned"] == 6
        assert res["data"][0]["value"] == 60
        assert "age" not in res["data"][0]

    @pytest.mark.asyncio
    async def test_query_rows_store_result(self, setup_engine):
        engine, result_id = setup_engine
        filters = FilterGroup(clauses=[FilterClause(column="active", operator="eq", value=True)])
        res = await engine.query_rows(result_id, filters=filters, store_result=True)
        assert isinstance(res, ResultSetSummary)
        assert res.row_count == 4
        assert res.provenance.op == "query_rows"

    @pytest.mark.asyncio
    async def test_query_rows_store_result_empty(self, setup_engine):
        engine, result_id = setup_engine
        filters = FilterGroup(clauses=[FilterClause(column="value", operator="gt", value=100)])
        res = await engine.query_rows(result_id, filters=filters, store_result=True)
        assert isinstance(res, ResultSetSummary)
        assert res.row_count == 0

    @pytest.mark.asyncio
    async def test_query_rows_store_result_column_subset(self, setup_engine):
        engine, result_id = setup_engine
        filters = FilterGroup(clauses=[FilterClause(column="active", operator="eq", value=True)])
        res = await engine.query_rows(result_id, filters=filters, columns=["name"], store_result=True)
        assert isinstance(res, ResultSetSummary)
        assert res.row_count == 4
        assert res.column_count == 1

    @pytest.mark.asyncio
    async def test_query_rows_store_result_ignores_limit(self, setup_engine):
        engine, result_id = setup_engine
        filters = FilterGroup(clauses=[FilterClause(column="active", operator="eq", value=True)])
        res = await engine.query_rows(result_id, filters=filters, limit=2, store_result=True)
        assert isinstance(res, ResultSetSummary)
        assert res.row_count == 4  # Full filtered set, not 2

    # ---------- Provenance Persistence Tests ----------
    @pytest.mark.asyncio
    async def test_group_by_provenance(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="count", alias="count")]
        res = await engine.group_by(result_id, group_columns=["active"], aggregations=aggs, store_result=True)
        assert isinstance(res, ResultSetSummary)
        meta = await engine.store.get_metadata(res.result_id)
        assert meta.provenance.op == "group_by"
        assert meta.provenance.parent == result_id
        assert meta.source is not None
        assert meta.source["op"] == "group_by"
        assert meta.grain == "one row per (active)"
        assert meta.source_row_count == 6

        # Check column provenance
        active_col = next(c for c in meta.columns if c.name == "active")
        assert active_col.origin == ColumnOrigin.group_key
        assert active_col.source_column == "active"

        count_col = next(c for c in meta.columns if c.name == "count")
        assert count_col.origin == ColumnOrigin.aggregate
        assert count_col.operation == "count"

    @pytest.mark.asyncio
    async def test_aggregate_provenance(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="sum", column="value", alias="total_value")]
        res = await engine.aggregate(result_id, aggregations=aggs, store_result=True)
        assert isinstance(res, ResultSetSummary)
        meta = await engine.store.get_metadata(res.result_id)
        assert meta.provenance.op == "aggregate"
        assert meta.provenance.parent == result_id
        assert meta.source is not None
        assert meta.source["op"] == "aggregate"
        assert meta.grain == "single row"

        total_col = next(c for c in meta.columns if c.name == "total_value")
        assert total_col.origin == ColumnOrigin.aggregate
        assert total_col.operation == "sum"
        assert total_col.source_column == "value"

    @pytest.mark.asyncio
    async def test_distinct_values_provenance(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.distinct_values(result_id, column="name", store_result=True)
        assert isinstance(res, ResultSetSummary)
        meta = await engine.store.get_metadata(res.result_id)
        assert meta.provenance.op == "distinct_values"
        assert meta.provenance.parent == result_id
        assert meta.source is not None
        assert meta.source["op"] == "distinct_values"
        assert meta.grain == "one row per distinct name"

        name_col = next(c for c in meta.columns if c.name == "name")
        assert name_col.origin == ColumnOrigin.passthrough
        assert name_col.source_column == "name"

        count_col = next(c for c in meta.columns if c.name == "count")
        assert count_col.origin == ColumnOrigin.derived

    @pytest.mark.asyncio
    async def test_histogram_provenance(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.histogram(result_id, column="value", bins=2, store_result=True)
        assert isinstance(res, ResultSetSummary)
        meta = await engine.store.get_metadata(res.result_id)
        assert meta.provenance.op == "histogram"
        assert meta.provenance.parent == result_id
        assert meta.source is not None
        assert meta.source["op"] == "histogram"
        assert meta.grain == "one row per histogram bin for value"

        bin_lower_col = next(c for c in meta.columns if c.name == "bin_lower")
        assert bin_lower_col.origin == ColumnOrigin.derived

        count_col = next(c for c in meta.columns if c.name == "count")
        assert count_col.origin == ColumnOrigin.derived

    # ---------- Alias Validation Tests ----------
    @pytest.mark.asyncio
    async def test_group_by_invalid_alias(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="sum", column="value", alias="invalid-alias")]
        with pytest.raises(ToolError, match="Invalid alias"):
            await engine.group_by(result_id, group_columns=["active"], aggregations=aggs)

    @pytest.mark.asyncio
    async def test_group_by_alias_collision(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="sum", column="value", alias="active")]
        with pytest.raises(ToolError, match="collides"):
            await engine.group_by(result_id, group_columns=["active"], aggregations=aggs)

    @pytest.mark.asyncio
    async def test_group_by_store_result_ignores_limit(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="count", alias="count")]
        res = await engine.group_by(result_id, group_columns=["active"], aggregations=aggs, limit=1, store_result=True)
        assert isinstance(res, ResultSetSummary)
        assert res.row_count == 2  # Full grouped set, not 1

    @pytest.mark.asyncio
    async def test_aggregate_invalid_alias(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="sum", column="value", alias="123invalid")]
        with pytest.raises(ToolError, match="Invalid alias"):
            await engine.aggregate(result_id, aggregations=aggs)

    # ---------- Column Naming Rules Tests ----------
    @pytest.mark.asyncio
    async def test_distinct_values_column_names(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.distinct_values(result_id, column="name", store_result=True)
        meta = await engine.store.get_metadata(res.result_id)
        column_names = [c.name for c in meta.columns]
        assert "name" in column_names
        assert "count" in column_names

    @pytest.mark.asyncio
    async def test_histogram_numeric_column_names(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.histogram(result_id, column="value", bins=2, store_result=True)
        meta = await engine.store.get_metadata(res.result_id)
        column_names = [c.name for c in meta.columns]
        assert "bin_index" in column_names
        assert "bin_lower" in column_names
        assert "bin_upper" in column_names
        assert "bin_label" in column_names
        assert "count" in column_names

    # ---------- Type and Description Inheritance Tests ----------
    @pytest.mark.asyncio
    async def test_passthrough_type_inheritance(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.query_rows(result_id, store_result=True)
        meta = await engine.store.get_metadata(res.result_id)
        for col in meta.columns:
            if col.origin == ColumnOrigin.passthrough:
                parent_meta = await engine.store.get_metadata(result_id)
                parent_col = next((c for c in parent_meta.columns if c.name == col.source_column), None)
                if parent_col:
                    assert col.type == parent_col.type

    @pytest.mark.asyncio
    async def test_aggregate_type_inference(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="avg", column="value", alias="avg_value")]
        res = await engine.aggregate(result_id, aggregations=aggs, store_result=True)
        meta = await engine.store.get_metadata(res.result_id)
        avg_col = next(c for c in meta.columns if c.name == "avg_value")
        assert avg_col.type == "float64"

    @pytest.mark.asyncio
    async def test_description_generation(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="count", alias="count")]
        res = await engine.group_by(result_id, group_columns=["active"], aggregations=aggs, store_result=True)
        meta = await engine.store.get_metadata(res.result_id)
        count_col = next(c for c in meta.columns if c.name == "count")
        assert count_col.description is not None
        assert "count" in count_col.description.lower()

    # ---------- get_result_info and explain_columns Tests ----------
    @pytest.mark.asyncio
    async def test_get_result_info_exposes_provenance(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="count", alias="count")]
        res = await engine.group_by(result_id, group_columns=["active"], aggregations=aggs, store_result=True)
        info = await engine.get_result_info(res.result_id)
        assert info["provenance"]["op"] == "group_by"
        assert info["source"] is not None
        assert info["grain"] == "one row per (active)"
        assert info["source_row_count"] == 6

    @pytest.mark.asyncio
    async def test_explain_columns_exposes_provenance(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="count", alias="count")]
        res = await engine.group_by(result_id, group_columns=["active"], aggregations=aggs, store_result=True)
        info = await engine.explain_columns(res.result_id)
        assert info["grain"] == "one row per (active)"
        assert info["source_row_count"] == 6
        count_col = next(c for c in info["columns"] if c["name"] == "count")
        assert count_col["description"] is not None

    # ---------- Chaining Tests ----------
    @pytest.mark.asyncio
    async def test_chaining_query_rows_on_group_by(self, setup_engine):
        engine, result_id = setup_engine
        # First, create a group_by result
        aggs = [AggregationSpec(operation="count", alias="count")]
        group_res = await engine.group_by(result_id, group_columns=["active"], aggregations=aggs, store_result=True)
        # Then, filter on the group_by result
        filters = FilterGroup(clauses=[FilterClause(column="count", operator="gt", value=1)])
        res = await engine.query_rows(group_res.result_id, filters=filters, store_result=True)
        assert isinstance(res, ResultSetSummary)
        meta = await engine.store.get_metadata(res.result_id)
        assert meta.provenance.op == "query_rows"
        assert meta.provenance.parent == group_res.result_id

    @pytest.mark.asyncio
    async def test_chaining_group_by_on_distinct_values(self, setup_engine):
        engine, result_id = setup_engine
        # First, create a distinct_values result
        distinct_res = await engine.distinct_values(result_id, column="name", store_result=True)
        # Then, group_by on the distinct_values result
        aggs = [AggregationSpec(operation="count", alias="count")]
        res = await engine.group_by(distinct_res.result_id, group_columns=["name"], aggregations=aggs, store_result=True)
        assert isinstance(res, ResultSetSummary)
        meta = await engine.store.get_metadata(res.result_id)
        assert meta.provenance.op == "group_by"
        assert meta.provenance.parent == distinct_res.result_id

    # ---------- result_label Tests ----------
    @pytest.mark.asyncio
    async def test_group_by_with_result_label(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="count", alias="count")]
        res = await engine.group_by(
            result_id, group_columns=["active"], aggregations=aggs, 
            store_result=True, result_label="Active counts"
        )
        assert isinstance(res, ResultSetSummary)
        meta = await engine.store.get_metadata(res.result_id)
        assert meta.result_label == "Active counts"

    @pytest.mark.asyncio
    async def test_aggregate_with_result_label(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="sum", column="value", alias="total_value")]
        res = await engine.aggregate(
            result_id, aggregations=aggs, 
            store_result=True, result_label="Total value"
        )
        assert isinstance(res, ResultSetSummary)
        meta = await engine.store.get_metadata(res.result_id)
        assert meta.result_label == "Total value"

    @pytest.mark.asyncio
    async def test_distinct_values_with_result_label(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.distinct_values(
            result_id, column="name", store_result=True, result_label="Unique names"
        )
        assert isinstance(res, ResultSetSummary)
        meta = await engine.store.get_metadata(res.result_id)
        assert meta.result_label == "Unique names"

    @pytest.mark.asyncio
    async def test_histogram_with_result_label(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.histogram(
            result_id, column="value", bins=2, store_result=True, result_label="Value distribution"
        )
        assert isinstance(res, ResultSetSummary)
        meta = await engine.store.get_metadata(res.result_id)
        assert meta.result_label == "Value distribution"

    @pytest.mark.asyncio
    async def test_get_result_info_exposes_result_label(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="count", alias="count")]
        res = await engine.group_by(
            result_id, group_columns=["active"], aggregations=aggs, 
            store_result=True, result_label="Active counts"
        )
        info = await engine.get_result_info(res.result_id)
        assert info["result_label"] == "Active counts"

    @pytest.mark.asyncio
    async def test_aggregate_with_filter_on_unprojected_column(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="avg", column="value", alias="avg_value")]
        filters = FilterGroup(clauses=[FilterClause(column="active", operator="eq", value=True)])
        res = await engine.aggregate(result_id, aggregations=aggs, filters=filters)
        # active=True values: 10, 30, 40, 60 -> avg = 35.0
        assert res["aggregations"]["avg_value"] == 35.0

    @pytest.mark.asyncio
    async def test_group_by_with_filter_on_unprojected_column(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [AggregationSpec(operation="count", alias="count")]
        filters = FilterGroup(clauses=[FilterClause(column="active", operator="eq", value=True)])
        res = await engine.group_by(result_id, group_columns=["name"], aggregations=aggs, filters=filters)
        # active=True: Alice(2), Charlie(1), None(1) -> 3 groups
        assert res["total_groups"] == 3

    @pytest.mark.asyncio
    async def test_distinct_values_with_filter_on_unprojected_column(self, setup_engine):
        engine, result_id = setup_engine
        filters = FilterGroup(clauses=[FilterClause(column="active", operator="eq", value=True)])
        res = await engine.distinct_values(result_id, column="name", filters=filters, include_null=True)
        # active=True: Alice, Charlie, None -> 3 distinct
        assert res["total_distinct"] == 3

    @pytest.mark.asyncio
    async def test_histogram_with_filter_on_unprojected_column(self, setup_engine):
        engine, result_id = setup_engine
        filters = FilterGroup(clauses=[FilterClause(column="active", operator="eq", value=True)])
        res = await engine.histogram(result_id, column="value", bins=2, filters=filters)
        # active=True: 10, 30, 40, 60 -> 4 rows
        assert res["null_count"] == 0
        assert sum(h["count"] for h in res["histogram"]) == 4
