"""Unit tests for the extended aggregation operation vocabulary and describe_columns."""

import pyarrow as pa
import pytest
import pytest_asyncio

from alphaplan_mcp.config import Settings
from alphaplan_mcp.errors import ToolError
from alphaplan_mcp.models import AggregationSpec, ResultProvenance
from alphaplan_mcp.query_engine import QueryEngine
from alphaplan_mcp.result_store import ResultSetStore


@pytest_asyncio.fixture
async def setup_engine(fake_redis):
    store = ResultSetStore(fake_redis)
    session_id = "test_session"
    table = pa.table({
        "value": [10, 20, 30, 40, 50, 60],
        "skew": [1, 2, 3, 4, 100, 200],
        "credits": [5, -3, 0, 7, -2, 1],
        "withnull": [1.0, None, 3.0, None, 5.0, None],
        "weight": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        "grp": ["a", "a", "a", "b", "b", "b"],
    })
    provenance = ResultProvenance(op="test_source")
    summary = await store.store_result(session_id, provenance, table)
    engine = QueryEngine(store, session_id)
    return engine, summary.result_id


def _agg(op, col=None, alias="a", **kw):
    return AggregationSpec(operation=op, column=col, alias=alias, **kw)


class TestDistributionOps:
    @pytest.mark.asyncio
    async def test_median(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[_agg("median", "value")])
        assert r["aggregations"]["a"] == 35.0

    @pytest.mark.asyncio
    async def test_median_less_than_mean_on_skewed(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[
            _agg("median", "skew", "median"),
            _agg("avg", "skew", "mean"),
        ])
        assert r["aggregations"]["median"] == 3.5
        assert r["aggregations"]["mean"] > r["aggregations"]["median"]

    @pytest.mark.asyncio
    async def test_percentile_linear(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[_agg("percentile", "value", q=0.95)])
        assert r["aggregations"]["a"] == 57.5

    @pytest.mark.asyncio
    async def test_percentile_methods(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[
            _agg("percentile", "value", "lower", method="lower", q=0.3),
            _agg("percentile", "value", "higher", method="higher", q=0.3),
            _agg("percentile", "value", "nearest", method="nearest", q=0.3),
            _agg("percentile", "value", "midpoint", method="midpoint", q=0.3),
            _agg("percentile", "value", "linear", method="linear", q=0.3),
        ])
        # q=0.3, n=6 -> idx=1.5 between value[1]=20 and value[2]=30
        assert r["aggregations"]["linear"] == 25.0
        assert {r["aggregations"]["lower"], r["aggregations"]["higher"]} == {20.0, 30.0}
        assert r["aggregations"]["midpoint"] == 25.0

    @pytest.mark.asyncio
    async def test_stddev_ddof(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[
            _agg("stddev", "value", "s1"),
            _agg("variance", "value", "v0", ddof=0),
            _agg("variance", "value", "v1", ddof=1),
        ])
        # sample std of [10..60] = sqrt(350)
        assert abs(r["aggregations"]["s1"] - 350 ** 0.5) < 1e-9
        assert abs(r["aggregations"]["v0"] - 291.6666667) < 1e-6
        assert abs(r["aggregations"]["v1"] - 350.0) < 1e-6

    @pytest.mark.asyncio
    async def test_mode(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[_agg("mode", "grp")])
        assert r["aggregations"]["a"] in ("a", "b")

    @pytest.mark.asyncio
    async def test_iqr_and_mad(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[
            _agg("iqr", "value", "iqr"),
            _agg("mad", "value", "mad"),
        ])
        assert r["aggregations"]["iqr"] == 25.0
        assert r["aggregations"]["mad"] == 15.0

    @pytest.mark.asyncio
    async def test_trimmed_mean(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[
            _agg("trimmed_mean", "skew", "tm", trim=0.2),
            _agg("avg", "skew", "mean"),
        ])
        assert r["aggregations"]["tm"] == 27.25
        assert r["aggregations"]["mean"] == 51.666666666666664

    @pytest.mark.asyncio
    async def test_coefficient_of_variation(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[_agg("coefficient_of_variation", "value", "cv")])
        assert abs(r["aggregations"]["cv"] - (350 ** 0.5) / 35.0) < 1e-9

    @pytest.mark.asyncio
    async def test_skewness_and_kurtosis(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[
            _agg("skewness", "skew", "sk"),
            _agg("kurtosis", "skew", "ku"),
        ])
        # right-skewed data -> positive skew
        assert r["aggregations"]["sk"] > 0


class TestConntOps:
    @pytest.mark.asyncio
    async def test_count_negative_zero_positive(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[
            _agg("count_negative", "credits", "neg"),
            _agg("count_zero", "credits", "zero"),
            _agg("count_positive", "credits", "pos"),
            _agg("count_null", "credits", "null"),
        ])
        assert r["aggregations"] == {"neg": 2, "zero": 1, "pos": 3, "null": 0}

    @pytest.mark.asyncio
    async def test_count_null_non_null(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[
            _agg("count_null", "withnull", "null"),
            _agg("count_non_null", "withnull", "non"),
        ])
        assert r["aggregations"] == {"null": 3, "non": 3}

    @pytest.mark.asyncio
    async def test_count_negative_null_excludes_nulls(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[_agg("count_negative", "withnull", "neg")])
        assert r["aggregations"]["neg"] == 0


class TestMultiColumnOps:
    @pytest.mark.asyncio
    async def test_weighted_avg(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[
            _agg("weighted_avg", "value", "wa", weight_column="weight")
        ])
        assert abs(r["aggregations"]["wa"] - 910.0 / 21.0) < 1e-9

    @pytest.mark.asyncio
    async def test_ratio(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[
            _agg("ratio", "value", "rt", denominator_column="credits")
        ])
        # sum(value)=210, sum(credits)=8
        assert abs(r["aggregations"]["rt"] - 26.25) < 1e-9

    @pytest.mark.asyncio
    async def test_ratio_requires_denominator(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError) as exc:
            await engine.aggregate(rid, aggregations=[_agg("ratio", "value")])
        assert exc.value.code == "INVALID_AGGREGATION"

    @pytest.mark.asyncio
    async def test_weighted_avg_under_group_by(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.group_by(rid, group_columns=["grp"], aggregations=[
            _agg("weighted_avg", "value", "wa", weight_column="weight")
        ])
        by_grp = {row["grp"]: row for row in r["rows"]}
        # group a: values [10,20,30], weights [1,2,3] -> (10+40+90)/6 = 140/6
        assert abs(by_grp["a"]["wa"] - 140.0 / 6.0) < 1e-9


class TestGroupByAndStore:
    @pytest.mark.asyncio
    async def test_median_under_group_by(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.group_by(rid, group_columns=["grp"], aggregations=[
            _agg("median", "value", "med")
        ])
        by_grp = {row["grp"]: row for row in r["rows"]}
        assert by_grp["a"]["med"] == 20.0
        assert by_grp["b"]["med"] == 50.0

    @pytest.mark.asyncio
    async def test_store_result_float_ops_typed(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.aggregate(rid, aggregations=[
            _agg("median", "value", "med"),
            _agg("count_negative", "credits", "neg"),
            _agg("mode", "grp", "m"),
        ], store_result=True)
        info = await engine.get_result_info(r.result_id)
        types = {c["name"]: c["type"] for c in info["columns"]}
        assert types["med"] == "float64"
        assert types["neg"] == "int64"
        assert types["m"] == "string"


class TestDiscoverabilityAndValidation:
    @pytest.mark.asyncio
    async def test_unknown_operation_lists_available(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError) as exc:
            await engine.aggregate(rid, aggregations=[_agg("bogus", "value")])
        assert exc.value.code == "INVALID_AGGREGATION"
        assert "median" in exc.value.details["available_operations"]
        assert isinstance(exc.value.details["available_operations"], list)

    @pytest.mark.asyncio
    async def test_percentile_requires_q(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError) as exc:
            await engine.aggregate(rid, aggregations=[_agg("percentile", "value")])
        assert exc.value.code == "INVALID_AGGREGATION"

    @pytest.mark.asyncio
    async def test_percentile_q_out_of_range(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError) as exc:
            await engine.aggregate(rid, aggregations=[_agg("percentile", "value", q=1.5)])
        assert exc.value.code == "INVALID_AGGREGATION"

    @pytest.mark.asyncio
    async def test_percentile_bad_method(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError) as exc:
            await engine.aggregate(rid, aggregations=[_agg("percentile", "value", q=0.5, method="bogus")])
        assert exc.value.code == "INVALID_AGGREGATION"

    @pytest.mark.asyncio
    async def test_trimmed_mean_requires_trim(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError) as exc:
            await engine.aggregate(rid, aggregations=[_agg("trimmed_mean", "value")])
        assert exc.value.code == "INVALID_AGGREGATION"

    @pytest.mark.asyncio
    async def test_trim_out_of_range(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError) as exc:
            await engine.aggregate(rid, aggregations=[_agg("trimmed_mean", "value", trim=0.6)])
        assert exc.value.code == "INVALID_AGGREGATION"


class TestDescribeColumns:
    @pytest.mark.asyncio
    async def test_describe_columns_whole_set(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.describe_columns(rid, columns=["value"])
        assert len(r["columns"]) == 1
        col = r["columns"][0]
        assert col["name"] == "value"
        assert col["count"] == 6
        assert col["count_null"] == 0
        assert col["mean"] == 35.0
        assert col["percentiles"]["0.5"] == 35.0
        assert col["count_negative"] == 0

    @pytest.mark.asyncio
    async def test_describe_columns_with_nulls_and_negatives(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.describe_columns(rid, columns=["credits", "withnull"])
        columns = {c["name"]: c for c in r["columns"]}
        assert columns["credits"]["count_negative"] == 2
        assert columns["withnull"]["count"] == 3
        assert columns["withnull"]["count_null"] == 3

    @pytest.mark.asyncio
    async def test_describe_columns_grouped(self, setup_engine):
        engine, rid = setup_engine
        r = await engine.describe_columns(rid, columns=["value"], group_columns=["grp"])
        assert len(r["groups"]) == 2
        for g in r["groups"]:
            assert set(g.keys()) == {"grp", "columns"}

    @pytest.mark.asyncio
    async def test_describe_columns_unknown_column(self, setup_engine):
        engine, rid = setup_engine
        with pytest.raises(ToolError) as exc:
            await engine.describe_columns(rid, columns=["nope"])
        assert exc.value.code == "UNKNOWN_COLUMN"
        assert "available_columns" in exc.value.details
