"""Unit tests for the QueryEngine (filtering, sorting, aggregation)."""

import base64
import hashlib
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

    def test_regex(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="regex", value="^bo")]), schema)
        result = df.filter(expr)
        assert result["name"].to_list() == ["bob", "Bob"]

    def test_not_regex(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="not_regex", value="^b")]), schema)
        result = df.filter(expr)
        names = result["name"].to_list()
        assert "bob" not in names and "Bob" not in names
        assert "Alice" in names and "Charlie" in names

    def test_regex_invalid_pattern(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        with pytest.raises(ToolError, match="Invalid regular expression"):
            _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="regex", value="[")]), schema)

    def test_matches_charset(self):
        df = pl.DataFrame({"k": ["SMAJ60CA-H-HS", "SMAJ6,0CA-H-HS", "udz27b-fl-h-hs"]})
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(
            FilterGroup(clauses=[FilterClause(
                column="k", operator="matches_charset",
                value="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.")]),
            schema,
        )
        result = df.filter(expr)
        assert result["k"].to_list() == ["SMAJ60CA-H-HS", "udz27b-fl-h-hs"]  # comma row excluded

    def test_not_matches_charset_sweeps_dirty_keys(self):
        df = pl.DataFrame({"k": ["SMAJ60CA-H-HS", "SMAJ6,0CA-H-HS", "OK-123"]})
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(
            FilterGroup(clauses=[FilterClause(
                column="k", operator="not_matches_charset",
                value="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")]),
            schema,
        )
        result = df.filter(expr)
        assert result["k"].to_list() == ["SMAJ6,0CA-H-HS"]

    def test_regex_requires_string_column(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        with pytest.raises(ToolError, match="requires a string column"):
            _build_filter_expr(FilterGroup(clauses=[FilterClause(column="value", operator="regex", value="1")]), schema)

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

    def test_ne(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="ne", value="Alice")]), schema)
        result = df.filter(expr)
        assert "Alice" not in result["name"].to_list()

    def test_ne_with_wildcard_hits_like(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="ne", value="A%")]), schema)
        result = df.filter(expr)
        assert "Alice" not in result["name"].to_list()

    def test_not_like(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="not_like", value="A%")]), schema)
        result = df.filter(expr)
        assert "Alice" not in result["name"].to_list()

    def test_is_not_null(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="is_not_null")]), schema)
        assert df.filter(expr).height == 5

    def test_lt_lte(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        lt = df.filter(_build_filter_expr(FilterGroup(clauses=[FilterClause(column="value", operator="lt", value=30)]), schema))
        assert sorted(lt["value"].to_list()) == [10, 20]
        lte = df.filter(_build_filter_expr(FilterGroup(clauses=[FilterClause(column="value", operator="lte", value=30)]), schema))
        assert sorted(lte["value"].to_list()) == [10, 20, 30]

    def test_contains_starts_with_ends_with(self):
        df = pl.DataFrame({"k": ["apple", "banana", "cherry", "orange"]})
        schema = {c: df[c].dtype for c in df.columns}
        contains = df.filter(_build_filter_expr(FilterGroup(clauses=[FilterClause(column="k", operator="contains", value="an")]), schema))
        assert contains["k"].to_list() == ["banana", "orange"]
        starts = df.filter(_build_filter_expr(FilterGroup(clauses=[FilterClause(column="k", operator="starts_with", value="c")]), schema))
        assert starts["k"].to_list() == ["cherry"]
        ends = df.filter(_build_filter_expr(FilterGroup(clauses=[FilterClause(column="k", operator="ends_with", value="e")]), schema))
        assert sorted(ends["k"].to_list()) == ["apple", "orange"]

    def test_contains_case_sensitive(self):
        df = pl.DataFrame({"k": ["Alpha", "alpha", "ALPHA"]})
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="k", operator="contains", value="Al", case_sensitive=True)]), schema)
        assert df.filter(expr)["k"].to_list() == ["Alpha"]

    def test_contains_case_insensitive_default(self):
        df = pl.DataFrame({"k": ["Alpha", "alpha", "ALPHA"]})
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="k", operator="contains", value="alp")]), schema)
        assert len(df.filter(expr)) == 3

    def test_in_not_in(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        inn = df.filter(_build_filter_expr(FilterGroup(clauses=[FilterClause(column="value", operator="in", value=[10, 60])]), schema))
        assert sorted(inn["value"].to_list()) == [10, 60]
        nin = df.filter(_build_filter_expr(FilterGroup(clauses=[FilterClause(column="value", operator="not_in", value=[10, 60])]), schema))
        assert 10 not in nin["value"].to_list() and 60 not in nin["value"].to_list()

    def test_in_too_many_values(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        with pytest.raises(ToolError, match="Too many values in 'in'"):
            _build_filter_expr(FilterGroup(clauses=[FilterClause(column="value", operator="in", value=list(range(101)))]), schema)

    def test_in_requires_list(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        with pytest.raises(ToolError, match="requires a list"):
            _build_filter_expr(FilterGroup(clauses=[FilterClause(column="value", operator="in", value=10)]), schema)

    def test_between(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="value", operator="between", value=[20, 40])]), schema)
        assert sorted(df.filter(expr)["value"].to_list()) == [20, 30, 40]

    def test_between_wrong_length(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        with pytest.raises(ToolError, match="list of two values"):
            _build_filter_expr(FilterGroup(clauses=[FilterClause(column="value", operator="between", value=[1])]), schema)

    def test_any_combination(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(
            combine="any",
            clauses=[
                FilterClause(column="name", operator="eq", value="Alice"),
                FilterClause(column="value", operator="gt", value=55),
            ],
        ), schema)
        assert df.filter(expr).height == 3  # rows 0, 2, 5

    def test_empty_clauses_is_truthy(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[]), schema)
        assert df.filter(expr).height == df.height

    def test_boolean_value_cast_from_string(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="active", operator="eq", value="true")]), schema)
        assert df.filter(expr).height == 4

    def test_type_mismatch_integer(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        with pytest.raises(ToolError, match="TYPE_MISMATCH"):
            _build_filter_expr(FilterGroup(clauses=[FilterClause(column="value", operator="eq", value="abc")]), schema)

    def test_type_mismatch_float(self):
        df = pl.DataFrame({"f": [1.5, 2.5]})
        schema = {c: df[c].dtype for c in df.columns}
        with pytest.raises(ToolError, match="TYPE_MISMATCH"):
            _build_filter_expr(FilterGroup(clauses=[FilterClause(column="f", operator="eq", value="abc")]), schema)

    def test_like_escaped_wildcard_literal(self):
        df = pl.DataFrame({"k": ["a%b", "axb"]})
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="k", operator="like", value=r"a\%b")]), schema)
        assert df.filter(expr)["k"].to_list() == ["a%b"]

    def test_like_case_sensitive(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        expr = _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="like", value="bob", case_sensitive=True)]), schema)
        assert df.filter(expr)["name"].to_list() == ["bob"]

    def test_unsupported_operator(self):
        df = self.build_df()
        schema = {c: df[c].dtype for c in df.columns}
        with pytest.raises(ToolError, match="Unsupported operator"):
            _build_filter_expr(FilterGroup(clauses=[FilterClause(column="name", operator="frobnicate", value="x")]), schema)


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

    def test_nulls_last(self):
        df = _make_dataframe()
        sorted_df = _apply_sort(df, [SortSpec(column="age", direction="asc", nulls="last")])
        ages = sorted_df["age"].to_list()
        assert ages[-1] is None
        assert ages[-2] == 45

    def test_nulls_first(self):
        df = _make_dataframe()
        sorted_df = _apply_sort(df, [SortSpec(column="age", direction="asc", nulls="first")])
        assert sorted_df["age"][0] is None

    def test_mixed_directions(self):
        df = _make_dataframe()
        sorted_df = _apply_sort(df, [
            SortSpec(column="active", direction="desc"),
            SortSpec(column="value", direction="asc", nulls="last"),
        ])
        actives = sorted_df["active"].to_list()
        assert actives[0] is True
        assert actives[-1] is False

    def test_string_column_nulls_last(self):
        df = _make_dataframe()
        sorted_df = _apply_sort(df, [SortSpec(column="name", direction="asc", nulls="last")])
        names = sorted_df["name"].to_list()
        assert len(names) == 6
        assert names[-1] is None

    def test_unknown_column_raises(self):
        df = _make_dataframe()
        with pytest.raises(ToolError, match="does not exist"):
            _apply_sort(df, [SortSpec(column="nope", direction="asc")])


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

    def test_decimal_to_string(self):
        from decimal import Decimal
        df = pl.DataFrame({"d": [Decimal("1.50")]}, schema={"d": pl.Decimal(10, 2)})
        rows = _df_to_rows(df)
        assert rows[0]["d"] == "1.50"

    def test_nan_and_inf_to_none(self):
        df = pl.DataFrame({"x": [float("nan"), float("inf")]})
        rows = _df_to_rows(df)
        assert rows[0]["x"] is None
        assert rows[1]["x"] is None

    def test_datetime_isoformat(self):
        from datetime import datetime
        df = pl.DataFrame({"dt": [datetime(2024, 5, 1, 12, 30)]})
        rows = _df_to_rows(df)
        assert rows[0]["dt"] == "2024-05-01T12:30:00"


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
        assert res["sha256_base64"] == base64.b64encode(
            hashlib.sha256(res["data"].encode("utf-8")).digest()
        ).decode("ascii")
        assert res["data"].endswith("\n")

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
        assert res["data"][0]["value"] == 60
        assert res["data"][1]["value"] == 50

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
    async def test_aggregate_nunique_alias(self, setup_engine):
        engine, result_id = setup_engine
        aggs = [
            AggregationSpec(operation="nunique", column="name", alias="n"),
            AggregationSpec(operation="count_distinct", column="name", alias="cd"),
        ]
        res = await engine.aggregate(result_id, aggregations=aggs)
        assert res["aggregations"]["n"] == 5
        assert res["aggregations"]["cd"] == 5

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


# ---------------------------------------------------------------------------
# Extended aggregation vocabulary (value-level correctness + error paths)
# ---------------------------------------------------------------------------
class TestAggregateMethods:
    @pytest.mark.asyncio
    async def test_median_min_max(self, setup_engine):
        engine, rid = setup_engine
        aggs = [
            AggregationSpec(operation="median", column="value", alias="med"),
            AggregationSpec(operation="min", column="value", alias="lo"),
            AggregationSpec(operation="max", column="value", alias="hi"),
        ]
        res = await engine.aggregate(rid, aggregations=aggs)
        assert res["aggregations"]["med"] == 35.0
        assert res["aggregations"]["lo"] == 10
        assert res["aggregations"]["hi"] == 60

    @pytest.mark.asyncio
    async def test_percentile_linear(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.aggregate(rid, aggregations=[
            AggregationSpec(operation="percentile", column="value", alias="p50", q=0.5),
            AggregationSpec(operation="percentile", column="value", alias="p0", q=0.0),
            AggregationSpec(operation="percentile", column="value", alias="p100", q=1.0),
        ])
        assert res["aggregations"]["p50"] == 35.0
        assert res["aggregations"]["p0"] == 10.0
        assert res["aggregations"]["p100"] == 60.0

    @pytest.mark.asyncio
    async def test_percentile_invalid_q(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError, match="must be between 0 and 1"):
            await engine.aggregate(rid, aggregations=[AggregationSpec(operation="percentile", column="value", q=1.5, alias="p")])

    @pytest.mark.asyncio
    async def test_percentile_invalid_method(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError, match="method"):
            await engine.aggregate(rid, aggregations=[AggregationSpec(operation="percentile", column="value", q=0.5, method="bogus", alias="p")])

    @pytest.mark.asyncio
    async def test_mode_is_deterministic_lowest(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.aggregate(rid, aggregations=[AggregationSpec(operation="mode", column="value", alias="m")])
        # all values are equally frequent -> mode is a tie; lowest value must win deterministically.
        assert res["aggregations"]["m"] == 10

    @pytest.mark.asyncio
    async def test_stddev_variance_ddof(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.aggregate(rid, aggregations=[
            AggregationSpec(operation="stddev", column="value", alias="sd1", ddof=1),
            AggregationSpec(operation="stddev", column="value", alias="sd0", ddof=0),
            AggregationSpec(operation="variance", column="value", alias="var1", ddof=1),
            AggregationSpec(operation="variance", column="value", alias="var0", ddof=0),
        ])
        assert res["aggregations"]["sd1"] == pytest.approx(18.708286933869708)
        assert res["aggregations"]["sd0"] == pytest.approx(17.07825127659933)
        assert res["aggregations"]["var1"] == pytest.approx(350.0)
        assert res["aggregations"]["var0"] == pytest.approx(291.6666666666667)

    @pytest.mark.asyncio
    async def test_iqr_mad(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.aggregate(rid, aggregations=[
            AggregationSpec(operation="iqr", column="value", alias="iqr"),
            AggregationSpec(operation="mad", column="value", alias="mad"),
        ])
        assert res["aggregations"]["iqr"] == pytest.approx(25.0)
        assert res["aggregations"]["mad"] == pytest.approx(15.0)

    @pytest.mark.asyncio
    async def test_trimmed_mean_valid(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.aggregate(rid, aggregations=[
            AggregationSpec(operation="trimmed_mean", column="value", trim=0.0, alias="tm"),
        ])
        assert res["aggregations"]["tm"] == pytest.approx(35.0)

    @pytest.mark.asyncio
    async def test_trimmed_mean_invalid_trim(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError, match="trim"):
            await engine.aggregate(rid, aggregations=[AggregationSpec(operation="trimmed_mean", column="value", trim=0.6, alias="tm")])

    @pytest.mark.asyncio
    async def test_skewness_kurtosis_cv(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.aggregate(rid, aggregations=[
            AggregationSpec(operation="skewness", column="value", alias="sk"),
            AggregationSpec(operation="kurtosis", column="value", alias="ku"),
            AggregationSpec(operation="coefficient_of_variation", column="value", alias="cv"),
        ])
        assert res["aggregations"]["sk"] == pytest.approx(0.0, abs=1e-9)
        assert res["aggregations"]["ku"] == pytest.approx(-1.268571428571429)
        assert res["aggregations"]["cv"] == pytest.approx(0.5345224838248488)

    @pytest.mark.asyncio
    async def test_count_ops(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.aggregate(rid, aggregations=[
            AggregationSpec(operation="count_non_null", column="value", alias="nn"),
            AggregationSpec(operation="count_null", column="value", alias="nl"),
            AggregationSpec(operation="count_zero", column="value", alias="z"),
            AggregationSpec(operation="count_negative", column="value", alias="neg"),
            AggregationSpec(operation="count_positive", column="value", alias="pos"),
            AggregationSpec(operation="count_distinct", column="value", alias="d"),
        ])
        a = res["aggregations"]
        assert a["nn"] == 6 and a["nl"] == 0
        assert a["z"] == 0 and a["neg"] == 0 and a["pos"] == 6
        assert a["d"] == 6

    @pytest.mark.asyncio
    async def test_weighted_avg(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.aggregate(rid, aggregations=[
            AggregationSpec(operation="weighted_avg", column="value", weight_column="age", alias="wa"),
        ])
        assert res["aggregations"]["wa"] == pytest.approx(32.857142857142854)

    @pytest.mark.asyncio
    async def test_weighted_avg_missing_weight(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError, match="weight_column"):
            await engine.aggregate(rid, aggregations=[AggregationSpec(operation="weighted_avg", column="value", alias="wa")])

    @pytest.mark.asyncio
    async def test_ratio(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.aggregate(rid, aggregations=[
            AggregationSpec(operation="ratio", column="value", denominator_column="age", alias="r"),
        ])
        assert res["aggregations"]["r"] == pytest.approx(1.2)

    @pytest.mark.asyncio
    async def test_ratio_missing_denominator(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError, match="denominator_column"):
            await engine.aggregate(rid, aggregations=[AggregationSpec(operation="ratio", column="value", alias="r")])

    @pytest.mark.asyncio
    async def test_count_with_column_raises(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError, match="count does not accept a column"):
            await engine.aggregate(rid, aggregations=[AggregationSpec(operation="count", column="value", alias="c")])

    @pytest.mark.asyncio
    async def test_operation_without_column_raises(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError, match="requires a column"):
            await engine.aggregate(rid, aggregations=[AggregationSpec(operation="sum", alias="s")])

    @pytest.mark.asyncio
    async def test_unknown_operation(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError, match="Unsupported operation"):
            await engine.aggregate(rid, aggregations=[AggregationSpec(operation="geomean", column="value", alias="g")])

    @pytest.mark.asyncio
    async def test_empty_aggregations_raises(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError, match="At least one aggregation"):
            await engine.aggregate(rid, aggregations=[])

    @pytest.mark.asyncio
    async def test_avg_drops_nulls(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.aggregate(rid, aggregations=[AggregationSpec(operation="avg", column="age", alias="a")])
        assert res["aggregations"]["a"] == 35.0

    @pytest.mark.asyncio
    async def test_empty_after_filter_sum_is_zero(self, setup_engine):
        # NB: polars sum() over a filtered-to-empty Int64 column yields 0 (not null),
        # unlike a freshly-constructed empty column. Pin this quirk so the refactor
        # cannot silently flip it to null.
        engine, rid = setup_engine
        filters = FilterGroup(clauses=[FilterClause(column="value", operator="gt", value=100)])
        res = await engine.aggregate(rid, aggregations=[AggregationSpec(operation="sum", column="value", alias="s")], filters=filters)
        assert res["aggregations"]["s"] == 0


# ---------------------------------------------------------------------------
# Extended group_by behaviour
# ---------------------------------------------------------------------------
class TestGroupByMethods:
    @pytest.mark.asyncio
    async def test_multiple_group_columns(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.group_by(rid, group_columns=["active", "name"], aggregations=[AggregationSpec(operation="count", alias="count")])
        # combos: (T,Alice),(F,bob),(T,Charlie),(F,Bob),(T,None)
        assert res["total_groups"] == 5

    @pytest.mark.asyncio
    async def test_null_group_key_included(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.group_by(rid, group_columns=["name"], aggregations=[AggregationSpec(operation="count", alias="count")])
        names = [r["name"] for r in res["rows"]]
        assert None in names
        assert res["total_groups"] == 5

    @pytest.mark.asyncio
    async def test_sum_avg_per_group(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.group_by(rid, group_columns=["active"], aggregations=[
            AggregationSpec(operation="sum", column="value", alias="sum"),
            AggregationSpec(operation="avg", column="value", alias="avg"),
        ])
        by_key = {r["active"]: r for r in res["rows"]}
        assert by_key[False]["sum"] == 70 and by_key[False]["avg"] == 35.0
        assert by_key[True]["sum"] == 140 and by_key[True]["avg"] == 35.0

    @pytest.mark.asyncio
    async def test_sort_grouped_output(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.group_by(
            rid,
            group_columns=["active"],
            aggregations=[AggregationSpec(operation="sum", column="value", alias="total")],
            sort=[SortSpec(column="total", direction="desc")],
        )
        assert res["rows"][0]["total"] == 140  # active True first

    @pytest.mark.asyncio
    async def test_group_by_pagination(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.group_by(rid, group_columns=["name"], aggregations=[AggregationSpec(operation="count", alias="count")], limit=2)
        assert res["total_groups"] == 5
        assert res["returned"] == 2
        assert res["has_more"] is True
        assert res["next_offset"] == 2

    @pytest.mark.asyncio
    async def test_alias_collision_between_aggregations(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError, match="collides"):
            await engine.group_by(
                rid,
                group_columns=["active"],
                aggregations=[AggregationSpec(operation="count", alias="x"), AggregationSpec(operation="sum", column="value", alias="x")],
            )

    @pytest.mark.asyncio
    async def test_too_many_groups(self, fake_redis):
        store = ResultSetStore(fake_redis)
        engine = QueryEngine(store, "s", settings_obj=Settings(RESULTSET_MAX_GROUPS=4))
        table = pl.DataFrame({"name": list("abcdefghij")}).to_arrow()
        summary = await store.store_result("s", ResultProvenance(op="t"), table)
        with pytest.raises(ToolError, match="TOO_MANY_GROUPS"):
            await engine.group_by(summary.result_id, group_columns=["name"], aggregations=[AggregationSpec(operation="count", alias="count")])


# ---------------------------------------------------------------------------
# describe_columns (previously completely untested)
# ---------------------------------------------------------------------------
class TestDescribeColumns:
    @pytest.mark.asyncio
    async def test_numeric_bundle(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.describe_columns(rid, columns=["value"])
        c = res["columns"][0]
        assert c["kind"] == "numeric"
        assert c["count"] == 6 and c["count_null"] == 0
        assert c["mean"] == 35.0
        assert c["stddev"] == pytest.approx(18.708286933869708)
        assert c["min"] == 10 and c["max"] == 60
        assert c["skewness"] == pytest.approx(0.0, abs=1e-9)
        assert c["count_negative"] == 0
        assert c["percentiles"]["0.25"] == 22.5
        assert c["percentiles"]["0.5"] == 35.0
        assert c["percentiles"]["0.75"] == 47.5

    @pytest.mark.asyncio
    async def test_string_bundle(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.describe_columns(rid, columns=["name"])
        c = res["columns"][0]
        assert c["kind"] == "string"
        assert c["count"] == 5 and c["count_null"] == 1
        assert c["distinct_count"] == 5

    @pytest.mark.asyncio
    async def test_bool_bundle(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.describe_columns(rid, columns=["active"])
        c = res["columns"][0]
        assert c["kind"] == "bool"
        assert c["count"] == 6 and c["count_null"] == 0
        assert c["true_count"] == 4

    @pytest.mark.asyncio
    async def test_group_columns_variant(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.describe_columns(rid, columns=["value"], group_columns=["active"])
        assert res["grain"] == "one row per (active)"
        assert len(res["groups"]) == 2
        for group in res["groups"]:
            assert group["columns"]["value"]["mean"] == 35.0

    @pytest.mark.asyncio
    async def test_invalid_percentile(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError, match="Percentile value must be between 0 and 1"):
            await engine.describe_columns(rid, columns=["value"], percentiles=[1.5])

    @pytest.mark.asyncio
    async def test_too_many_group_columns(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError, match="Too many group columns"):
            await engine.describe_columns(rid, columns=["value"], group_columns=["name", "value", "active", "age"])


# ---------------------------------------------------------------------------
# histogram branches (string / empty / single-value / non-numeric / cap)
# ---------------------------------------------------------------------------
class TestHistogramBranches:
    @pytest.mark.asyncio
    async def test_string_top_n_with_other(self, fake_redis):
        store = ResultSetStore(fake_redis)
        table = pl.DataFrame({"k": ["a", "b", "c", "d", "e", "f", "f", "f", "f"]}).to_arrow()
        summary = await store.store_result("s", ResultProvenance(op="t"), table)
        engine = QueryEngine(store, "s")
        res = await engine.histogram(summary.result_id, column="k", bins=2)
        # top-2 by count: f (4) + one of the five singletons; the other four go to "other"
        others = [h for h in res["histogram"] if h["is_other"]]
        assert len(res["histogram"]) == 3
        assert len(others) == 1
        assert others[0]["count"] == 4  # a, b, c, d (whichever singleton misses the top-2)

    @pytest.mark.asyncio
    async def test_all_null_column_empty(self, fake_redis):
        store = ResultSetStore(fake_redis)
        table = pl.DataFrame({"x": [None, None]}).to_arrow()
        summary = await store.store_result("s", ResultProvenance(op="t"), table)
        engine = QueryEngine(store, "s")
        res = await engine.histogram(summary.result_id, column="x")
        assert res["bins"] == 0
        assert res["null_count"] == 2
        assert res["histogram"] == []

    @pytest.mark.asyncio
    async def test_single_value_column(self, fake_redis):
        store = ResultSetStore(fake_redis)
        table = pl.DataFrame({"v": [5.0, 5.0, 5.0]}).to_arrow()
        summary = await store.store_result("s", ResultProvenance(op="t"), table)
        engine = QueryEngine(store, "s")
        res = await engine.histogram(summary.result_id, column="v", bins=2)
        assert res["bins"] == 2
        assert sum(h["count"] for h in res["histogram"]) == 3

    @pytest.mark.asyncio
    async def test_non_numeric_raises_invalid_argument(self, fake_redis):
        from datetime import datetime
        store = ResultSetStore(fake_redis)
        table = pl.DataFrame({"d": [datetime(2024, 1, 1), datetime(2024, 6, 1)]}).to_arrow()
        summary = await store.store_result("s", ResultProvenance(op="t"), table)
        engine = QueryEngine(store, "s")
        with pytest.raises(ToolError, match="INVALID_ARGUMENT"):
            await engine.histogram(summary.result_id, column="d")

    @pytest.mark.asyncio
    async def test_bins_capped_at_50(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.histogram(rid, column="value", bins=100)
        assert res["bins"] == 50
        assert len(res["histogram"]) == 50


# ---------------------------------------------------------------------------
# cross-cutting correctness: round-trips, deletion, metadata exploration
# ---------------------------------------------------------------------------
class TestCrossCutting:
    @pytest.mark.asyncio
    async def test_delete_result_set_then_not_found(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.delete_result_set(rid)
        assert res["deleted"] is True
        with pytest.raises(ToolError, match="not found"):
            await engine.list_rows(rid)

    @pytest.mark.asyncio
    async def test_explain_columns_include_examples(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.explain_columns(rid, columns=["active", "name"], include_examples=True)
        by_col = {c["name"]: c for c in res["columns"]}
        assert by_col["active"]["distinct_count"] == 2
        assert by_col["name"]["examples"] == ["Alice", "bob", "Charlie"]

    @pytest.mark.asyncio
    async def test_explain_columns_unknown_column(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError, match="does not exist"):
            await engine.explain_columns(rid, columns=["nope"])

    @pytest.mark.asyncio
    async def test_query_rows_sort_store_result_projection(self, setup_engine):
        engine, rid = setup_engine
        filters = FilterGroup(clauses=[FilterClause(column="active", operator="eq", value=True)])
        res = await engine.query_rows(
            rid,
            filters=filters,
            columns=["name", "value"],
            sort=[SortSpec(column="value", direction="desc")],
            store_result=True,
        )
        assert isinstance(res, ResultSetSummary)
        assert res.row_count == 4
        assert res.column_count == 2
        meta = await engine.store.get_metadata(res.result_id)
        assert meta.provenance.parent == rid and meta.provenance.op == "query_rows"

    @pytest.mark.asyncio
    async def test_round_trip_store_then_read_identical(self, setup_engine):
        engine, rid = setup_engine
        filters = FilterGroup(clauses=[FilterClause(column="active", operator="eq", value=True)])
        stored = await engine.query_rows(rid, filters=filters, columns=["name", "value"], store_result=True)
        listed = await engine.list_rows(stored.result_id, columns=["name", "value"])
        assert listed["total_rows"] == 4
        names = [r["name"] for r in listed["data"]]
        assert set(n for n in names if n is not None) == {"Alice", "Charlie"}

    @pytest.mark.asyncio
    async def test_distinct_values_orders_and_counts(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.distinct_values(rid, column="name", order="value_asc", include_counts=False)
        values = [v["value"] for v in res["values"]]
        assert values == ["Alice", "Bob", "Charlie", "bob"]  # 4 distinct non-null, asc
        assert "count" not in res["values"][0]

    @pytest.mark.asyncio
    async def test_all_null_column_across_tools(self, fake_redis):
        store = ResultSetStore(fake_redis)
        table = pl.DataFrame({"x": [None, None, None]}).to_arrow()
        summary = await store.store_result("s", ResultProvenance(op="t"), table)
        engine = QueryEngine(store, "s")
        rid = summary.result_id

        agg = await engine.aggregate(rid, aggregations=[AggregationSpec(operation="count_null", column="x", alias="nl")])
        assert agg["aggregations"]["nl"] == 3

        dv = await engine.distinct_values(rid, column="x", include_null=True)
        assert dv["total_distinct"] == 1

        gb = await engine.group_by(rid, group_columns=["x"], aggregations=[AggregationSpec(operation="count", alias="count")])
        assert gb["total_groups"] == 1
        assert gb["rows"][0]["count"] == 3


# ---------------------------------------------------------------------------
# Multi-chunk result sets (span the store's 5000-row chunk boundary)
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture
async def setup_multi_chunk(fake_redis):
    store = ResultSetStore(fake_redis)
    table = pl.DataFrame({
        "g": [i % 3 for i in range(12000)],
        "v": list(range(12000)),
    }).to_arrow()
    summary = await store.store_result("s", ResultProvenance(op="t"), table)
    meta = await store.get_metadata(summary.result_id)
    return QueryEngine(store, "s"), summary.result_id, meta.chunk_count


class TestMultiChunkResultSets:
    @pytest.mark.asyncio
    async def test_data_spans_multiple_chunks(self, setup_multi_chunk):
        engine, rid, chunk_count = setup_multi_chunk
        assert chunk_count > 1  # guard: test is only meaningful if >1 chunk

    @pytest.mark.asyncio
    async def test_count_rows(self, setup_multi_chunk):
        engine, rid, _ = setup_multi_chunk
        res = await engine.count_rows(rid)
        assert res["count"] == 12000

    @pytest.mark.asyncio
    async def test_aggregate_spans_chunks(self, setup_multi_chunk):
        engine, rid, _ = setup_multi_chunk
        res = await engine.aggregate(rid, aggregations=[AggregationSpec(operation="sum", column="v", alias="s")])
        assert res["aggregations"]["s"] == sum(range(12000))

    @pytest.mark.asyncio
    async def test_group_by_spans_chunks(self, setup_multi_chunk):
        engine, rid, _ = setup_multi_chunk
        res = await engine.group_by(rid, group_columns=["g"], aggregations=[AggregationSpec(operation="count", alias="c")])
        by_key = {r["g"]: r["c"] for r in res["rows"]}
        assert by_key == {0: 4000, 1: 4000, 2: 4000}

    @pytest.mark.asyncio
    async def test_distinct_spans_chunks(self, setup_multi_chunk):
        engine, rid, _ = setup_multi_chunk
        res = await engine.distinct_values(rid, column="g")
        assert res["total_distinct"] == 3

    @pytest.mark.asyncio
    async def test_cross_chunk_pagination_is_consistent(self, setup_multi_chunk):
        engine, rid, _ = setup_multi_chunk
        total, off, step = 0, 0, 300
        while True:
            page = await engine.list_rows(rid, limit=step, offset=off)
            total += sum(r["v"] for r in page["data"])
            if page["has_more"] and page["data"]:
                off = page["next_offset"]
            else:
                break
        assert total == sum(range(12000))


# ---------------------------------------------------------------------------
# Empty result set through every tool
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture
async def setup_empty_engine(fake_redis):
    store = ResultSetStore(fake_redis)
    table = pl.DataFrame({"name": [], "value": []}).to_arrow()
    summary = await store.store_result("s", ResultProvenance(op="t"), table)
    return QueryEngine(store, "s"), summary.result_id


class TestEmptyResultSet:
    @pytest.mark.asyncio
    async def test_count_rows(self, setup_empty_engine):
        engine, rid = setup_empty_engine
        assert (await engine.count_rows(rid))["count"] == 0

    @pytest.mark.asyncio
    async def test_aggregate_counts(self, setup_empty_engine):
        engine, rid = setup_empty_engine
        res = await engine.aggregate(rid, aggregations=[
            AggregationSpec(operation="count", alias="c"),
            AggregationSpec(operation="count_non_null", column="value", alias="nn"),
        ])
        assert res["aggregations"]["c"] == 0
        assert res["aggregations"]["nn"] == 0

    @pytest.mark.asyncio
    async def test_aggregate_sum_is_null(self, setup_empty_engine):
        # NB: a freshly-constructed (never filtered) empty column sums to null,
        # unlike the filtered-to-empty path which yields 0 (see TestAggregateMethods).
        engine, rid = setup_empty_engine
        res = await engine.aggregate(rid, aggregations=[AggregationSpec(operation="sum", column="value", alias="s")])
        assert res["aggregations"]["s"] is None

    @pytest.mark.asyncio
    async def test_distinct_values_zero(self, setup_empty_engine):
        engine, rid = setup_empty_engine
        res = await engine.distinct_values(rid, column="name")
        assert res["total_distinct"] == 0 and res["values"] == []

    @pytest.mark.asyncio
    async def test_group_by_zero_groups(self, setup_empty_engine):
        engine, rid = setup_empty_engine
        res = await engine.group_by(rid, group_columns=["name"], aggregations=[AggregationSpec(operation="count", alias="c")])
        assert res["total_groups"] == 0 and res["rows"] == []

    @pytest.mark.asyncio
    async def test_histogram_empty(self, setup_empty_engine):
        engine, rid = setup_empty_engine
        res = await engine.histogram(rid, column="value")
        assert res["bins"] == 0 and res["histogram"] == [] and res["null_count"] == 0


# ---------------------------------------------------------------------------
# query_rows: filter on a column that is projected out of the stored result
# ---------------------------------------------------------------------------
class TestProjectedOutFilterStoreResult:
    @pytest.mark.asyncio
    async def test_filter_column_not_in_stored_projection(self, setup_engine):
        engine, rid = setup_engine
        filters = FilterGroup(clauses=[FilterClause(column="active", operator="eq", value=True)])
        res = await engine.query_rows(rid, filters=filters, columns=["name", "value"], store_result=True)
        assert isinstance(res, ResultSetSummary)
        assert res.row_count == 4
        assert res.column_count == 2
        meta = await engine.store.get_metadata(res.result_id)
        names = [c.name for c in meta.columns]
        assert names == ["name", "value"]
        assert "active" not in names

    @pytest.mark.asyncio
    async def test_filter_only_column(self, setup_engine):
        # filter on a column not in projection at all, stored set has just the project column
        engine, rid = setup_engine
        filters = FilterGroup(clauses=[FilterClause(column="value", operator="gt", value=30)])
        res = await engine.query_rows(rid, filters=filters, columns=["name"], store_result=True)
        assert res.row_count == 3
        assert res.column_count == 1


# ---------------------------------------------------------------------------
# get_result_info must never leak the owner session id
# ---------------------------------------------------------------------------
class TestGetResultInfoSafeFields:
    @pytest.mark.asyncio
    async def test_no_owner_session_id_leak(self, setup_engine):
        engine, rid = setup_engine
        info = await engine.get_result_info(rid)
        serialized = str(info)
        assert "owner_session_id" not in serialized
        assert {"result_id", "provenance", "source", "grain", "source_row_count",
                "result_label", "row_count", "column_count", "stored_size_bytes",
                "created_at", "expires_at", "columns"} <= set(info.keys())

    @pytest.mark.asyncio
    async def test_no_owner_in_columns(self, setup_engine):
        engine, rid = setup_engine
        info = await engine.get_result_info(rid)
        for c in info["columns"]:
            assert "owner_session_id" not in str(c)


# ---------------------------------------------------------------------------
# describe_columns temporal branch (crashed before the fix at query_engine.py:_describe_series)
# ---------------------------------------------------------------------------
class TestDescribeTemporal:
    @pytest.mark.asyncio
    async def test_datetime_bundle(self, fake_redis):
        from datetime import datetime
        store = ResultSetStore(fake_redis)
        table = pl.DataFrame({"d": [datetime(2024, 1, 1), datetime(2024, 6, 15)]}).to_arrow()
        summary = await store.store_result("s", ResultProvenance(op="t"), table)
        engine = QueryEngine(store, "s")
        res = await engine.describe_columns(summary.result_id, columns=["d"])
        c = res["columns"][0]
        assert c["kind"] == "temporal"
        assert c["count"] == 2 and c["count_null"] == 0
        assert c["min"] == "2024-01-01T00:00:00"
        assert c["max"] == "2024-06-15T00:00:00"
        # temporal columns must not expose numeric-only stats
        assert "mean" not in c and "stddev" not in c and "percentiles" not in c


# ---------------------------------------------------------------------------
# histogram store_result=True for the string top-N and empty branches
# ---------------------------------------------------------------------------
class TestHistogramStoreResultBranches:
    @pytest.mark.asyncio
    async def test_string_branch(self, fake_redis):
        store = ResultSetStore(fake_redis)
        table = pl.DataFrame({"k": ["a", "b", "c", "d", "e", "f", "f", "f", "f"]}).to_arrow()
        summary = await store.store_result("s", ResultProvenance(op="t"), table)
        engine = QueryEngine(store, "s")
        res = await engine.histogram(summary.result_id, column="k", bins=2, store_result=True)
        assert isinstance(res, ResultSetSummary)
        assert res.row_count == 3  # top-2 values + "other"
        meta = await engine.store.get_metadata(res.result_id)
        names = [c.name for c in meta.columns]
        assert names == ["k", "count", "is_other"]

    @pytest.mark.asyncio
    async def test_empty_branch(self, setup_empty_engine):
        engine, rid = setup_empty_engine
        res = await engine.histogram(rid, column="value", store_result=True)
        assert isinstance(res, ResultSetSummary)
        assert res.row_count == 0
        assert res.column_count == 2


# ---------------------------------------------------------------------------
# distinct_values ordering variants (count_asc / count_desc)
# ---------------------------------------------------------------------------
class TestDistinctOrderingVariants:
    @pytest.mark.asyncio
    async def test_count_desc(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.distinct_values(rid, column="name", order="count_desc")
        # Alice appears twice; everyone else once
        assert res["values"][0]["count"] == 2

    @pytest.mark.asyncio
    async def test_count_asc(self, setup_engine):
        engine, rid = setup_engine
        res = await engine.distinct_values(rid, column="name", order="count_asc")
        assert res["values"][-1]["count"] == 2

    @pytest.mark.asyncio
    async def test_invalid_order(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError, match="Unsupported order"):
            await engine.distinct_values(rid, column="name", order="bogus")


# ---------------------------------------------------------------------------
# CSV/gzip content round-trip: decoded gzip equals the plain CSV of the same slice
# ---------------------------------------------------------------------------
class TestCsvGzipContentRoundTrip:
    @pytest.mark.asyncio
    async def test_gzip_decodes_to_plain_csv(self, fake_redis):
        import gzip
        store = ResultSetStore(fake_redis)
        table = pl.DataFrame({"t": ["long repeated text " * 20 for _ in range(200)]}).to_arrow()
        summary = await store.store_result("s", ResultProvenance(op="t"), table)
        engine = QueryEngine(store, "s")

        gz = await engine.list_rows(summary.result_id, format="csv", encoding="gzip+base64", limit=200)
        assert gz["encoding"] == "gzip+base64"
        decoded = gzip.decompress(base64.b64decode(gz["data"])).decode("utf-8")
        assert decoded.endswith("\n")
        assert gz["sha256_base64"] == base64.b64encode(hashlib.sha256(decoded.encode("utf-8")).digest()).decode("ascii")

        plain = await engine.list_rows(summary.result_id, format="csv", limit=200)
        assert decoded == plain["data"]
        assert gz["uncompressed_bytes"] == len(decoded.encode("utf-8"))


# ---------------------------------------------------------------------------
# Categorical columns through filter + sort
# ---------------------------------------------------------------------------
class TestCategoricalColumns:
    @pytest.mark.asyncio
    async def test_filter_and_sort(self, fake_redis):
        store = ResultSetStore(fake_redis)
        df = pl.DataFrame().with_columns(
            pl.Series("k", ["b", "a", "c", "a"], dtype=pl.Categorical),
            pl.Series("v", [3, 1, 2, 4]),
        )
        summary = await store.store_result("s", ResultProvenance(op="t"), df.to_arrow())
        engine = QueryEngine(store, "s")

        counted = await engine.count_rows(summary.result_id, filters=FilterGroup(clauses=[FilterClause(column="k", operator="eq", value="a")]))
        assert counted["count"] == 2

        rows = await engine.list_rows(summary.result_id, sort=[SortSpec(column="k", direction="asc")])
        assert [r["k"] for r in rows["data"]] == ["a", "a", "b", "c"]


# ---------------------------------------------------------------------------
# Regression: limit=0 pagination must be terminal (Bug 1)
# ---------------------------------------------------------------------------
class TestPaginationLimitZero:
    @pytest.mark.asyncio
    async def test_list_rows_limit_zero_is_terminal(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.list_rows(result_id, limit=0)
        assert res["returned"] == 0
        assert res["data"] == []
        assert res["has_more"] is False
        assert res["next_offset"] is None

    @pytest.mark.asyncio
    async def test_query_rows_limit_zero_is_terminal(self, setup_engine):
        engine, result_id = setup_engine
        res = await engine.query_rows(result_id, limit=0)
        assert res["returned"] == 0
        assert res["rows"] == []
        assert res["has_more"] is False
        assert res["next_offset"] is None


# ---------------------------------------------------------------------------
# Regression: query_rows supports result_label + round_to; list_rows supports result_label (Bug 3)
# ---------------------------------------------------------------------------
class TestQueryRowsOptionalParams:
    @pytest.mark.asyncio
    async def test_query_rows_store_supports_result_label(self, setup_engine):
        engine, result_id = setup_engine
        summary = await engine.query_rows(
            result_id,
            filters=FilterGroup(clauses=[FilterClause(column="value", operator="gt", value=10)]),
            store_result=True,
            result_label="my-query-rows",
        )
        assert isinstance(summary, ResultSetSummary)
        assert summary.result_label == "my-query-rows"
        info = await engine.get_result_info(summary.result_id)
        assert info["result_label"] == "my-query-rows"

    @pytest.mark.asyncio
    async def test_query_rows_supports_round_to(self, setup_engine_float):
        engine, result_id = setup_engine_float
        res = await engine.query_rows(result_id, columns=["value"], round_to=0)
        assert all(isinstance(r["value"], int) for r in res["rows"])

    @pytest.mark.asyncio
    async def test_list_rows_store_supports_result_label(self, setup_engine):
        engine, result_id = setup_engine
        summary = await engine.list_rows(result_id, limit=2, store_result=True, result_label="my-list-rows")
        assert isinstance(summary, ResultSetSummary)
        assert summary.result_label == "my-list-rows"
