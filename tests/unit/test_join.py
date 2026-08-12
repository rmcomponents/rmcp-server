"""Unit tests for the join operation (QueryEngine.join / tools.result_sets.join)."""

import uuid
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from alphaplan_mcp.config import Settings
from alphaplan_mcp.errors import ToolError
from alphaplan_mcp.models import (
    AggregationSpec,
    FilterClause,
    FilterGroup,
    JoinColumnPair,
    ResultProvenance,
)
from alphaplan_mcp.query_engine import QueryEngine
from alphaplan_mcp.result_store import ResultSetStore


settings_for_tests = Settings(
    RESULTSET_MAX_ROWS=1000,
    RESULTSET_MAX_BYTES=50_000_000,
    RESULTSET_CHUNK_ROWS=100,
    RESULTSET_IDLE_TTL_SECONDS=3600,
    RESULTSET_MAX_TTL_SECONDS=86400,
    REDIS_KEY_PREFIX="test-join:" + uuid.uuid4().hex[:6],
)


async def _build_store(fake_redis, settings=settings_for_tests):
    return ResultSetStore(
        fake_redis,
        prefix=settings.REDIS_KEY_PREFIX,
        chunk_rows=settings.RESULTSET_CHUNK_ROWS,
        idle_ttl=settings.RESULTSET_IDLE_TTL_SECONDS,
        max_ttl=settings.RESULTSET_MAX_TTL_SECONDS,
        max_rows=settings.RESULTSET_MAX_ROWS,
        max_bytes=settings.RESULTSET_MAX_BYTES,
    )


async def _store(store, session, table, op="test", **params):
    return await store.store_result(
        session_id=session,
        provenance=ResultProvenance(op=op, parameters=params or None),
        table=table,
    )


def _engine(store, session, settings=settings_for_tests):
    return QueryEngine(store, session, settings_obj=settings)


def _stats_table():
    return pa.table({
        "ArtikelNummer": ["A1", "A2", "A3"],
        "Netto": [10, 20, 30],
        "Menge": [1, 2, 3],
    })


def _parts_table():
    return pa.table({
        "ArtikelNummer": ["A1", "A2"],
        "WarenGruppe": ["WG1", "WG2"],
        "HauptLieferant": ["S1", "S2"],
        "Beschreibung": ["desc1", "desc2"],
    })


@pytest.mark.asyncio
async def test_left_join_projects_right_columns_and_indicator(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", _stats_table(), op="stats")
    right = await _store(store, "s1", _parts_table(), op="parts")

    summary = await _engine(store, "s1").join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        how="left",
        columns_right=["WarenGruppe", "HauptLieferant"],
    )

    assert summary.join is not None
    assert summary.join.how == "left"
    assert summary.join.indicator_column == "_join_match"
    assert summary.join.matched_rows == 2
    assert summary.join.unmatched_left_rows == 1
    assert summary.join.unmatched_right_rows == 0
    assert summary.join.left_keys_total == 3
    assert summary.join.left_keys_matched == 2
    assert summary.join.left_keys_unmatched == 1
    assert "A3" in summary.join.unmatched_key_sample
    assert summary.row_count == 3

    out = await store.load_table(summary.result_id)
    col_names = out.column_names
    assert "ArtikelNummer" in col_names
    assert "WarenGruppe" in col_names
    assert "HauptLieferant" in col_names
    assert "_join_match" in col_names
    # right columns not projected
    assert "Beschreibung" not in col_names

    rows = {r["ArtikelNummer"]: r for r in out.to_pylist()}
    assert rows["A1"]["WarenGruppe"] == "WG1"
    assert rows["A1"]["_join_match"] == "both"
    assert rows["A3"]["WarenGruppe"] is None
    assert rows["A3"]["_join_match"] == "left_only"


@pytest.mark.asyncio
async def test_sum_preserved_no_fanout(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", _stats_table(), op="stats")
    right = await _store(store, "s1", _parts_table(), op="parts")

    summary = await _engine(store, "s1").join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        how="left",
        validate="many_to_one",
    )
    agg = await _engine(store, "s1").aggregate(
        summary.result_id,
        aggregations=[AggregationSpec(column="Netto", operation="sum", alias="netto")],
    )
    assert agg["aggregations"]["netto"] == 60


@pytest.mark.asyncio
async def test_duplicate_right_key_raises_cardinality_violation(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", _stats_table(), op="stats")
    right = await _store(
        store, "s1",
        pa.table({"ArtikelNummer": ["A1", "A1"], "WarenGruppe": ["X", "Y"]}),
        op="dup",
    )
    before = await store.list_results("s1")

    with pytest.raises(ToolError) as ei:
        await _engine(store, "s1").join(
            left_result_id=left.result_id,
            right_result_id=right.result_id,
            on=["ArtikelNummer"],
            validate="many_to_one",
        )
    assert ei.value.code == "CARDINALITY_VIOLATION"
    assert ei.value.details["side"] == "right"
    assert any(k["key"] == "A1" and k["count"] == 2 for k in ei.value.details["keys"])
    # no new result set was created
    after = await store.list_results("s1")
    assert sorted(r.result_id for r in before) == sorted(r.result_id for r in after)


@pytest.mark.asyncio
async def test_trailing_space_matches_with_normalization(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", pa.table({"ArtikelNummer": ["FM2150-M-HS "]}), op="stats")
    right = await _store(store, "s1", pa.table({"ArtikelNummer": ["FM2150-M-HS"], "WG": ["X"]}), op="parts")

    engine = _engine(store, "s1")
    summary_on = await engine.join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        normalize_keys=True,
    )
    assert summary_on.join.matched_rows == 1
    assert summary_on.join.unmatched_left_rows == 0
    assert summary_on.join.normalization.left_keys_altered == 1
    assert summary_on.join.normalization.matches_gained_by_normalization == 1

    summary_off = await engine.join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        normalize_keys=False,
    )
    assert summary_off.join.matched_rows == 0
    assert summary_off.join.unmatched_left_rows == 1
    assert summary_off.join.normalization.matches_gained_by_normalization == 0


@pytest.mark.asyncio
async def test_nb_hyphen_matches_with_normalization(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", pa.table({"ArtikelNummer": ["BZX84C4V7\u2011Q1\u2011H\u2011HS"]}), op="stats")
    right = await _store(store, "s1", pa.table({"ArtikelNummer": ["BZX84C4V7-Q1-H-HS"], "WG": ["X"]}), op="parts")

    summary = await _engine(store, "s1").join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        normalize_keys=True,
    )
    assert summary.join.matched_rows == 1
    assert summary.join.normalization.matches_gained_by_normalization == 1


@pytest.mark.asyncio
async def test_normalization_is_single_fixed_pipeline(fake_redis):
    from alphaplan_mcp.utils import normalize_mpn

    # The single fixed pipeline applies NFKC, dash-fold, whitespace-collapse,
    # uppercase and char-strip to every string key.
    assert normalize_mpn("fm-2150x") == "FM-2150X"                # case
    assert normalize_mpn("FM\u20112150\u00a0X") == "FM-2150X"     # non-breaking hyphen + NBSP
    assert normalize_mpn("\uff2d\u2011\uff12") == "M-2"           # fullwidth NFKC + dash-fold
    assert normalize_mpn(" f m\t2 150 ") == "FM2150"              # interior/edge whitespace collapse
    assert normalize_mpn("fm/&2-150x") == "FM2-150X"              # '/' and '&' stripped
    assert normalize_mpn(None) is None
    assert normalize_mpn("") == ""
    assert normalize_mpn(42) == 42

    store = await _build_store(fake_redis)
    left = await _store(store, "s1", pa.table({"ArtikelNummer": ["fm-2150x"]}), op="stats")
    right = await _store(store, "s1", pa.table({"ArtikelNummer": ["FM\u20112150\u00a0X"], "WG": ["X"]}), op="parts")

    summary = await _engine(store, "s1").join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        normalize_keys=True,
    )
    assert summary.join.matched_rows == 1
    assert summary.join.normalization.matches_gained_by_normalization == 1
    assert set(summary.join.normalization.enabled) == {
        "trim_whitespace", "unicode_nfkc", "fold_dashes", "uppercase", "strip",
    }


@pytest.mark.asyncio
async def test_normalize_keys_false_disables_pipeline(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", pa.table({"ArtikelNummer": ["fm-2150"]}), op="stats")
    right = await _store(store, "s1", pa.table({"ArtikelNummer": ["FM2150"], "WG": ["X"]}), op="parts")

    summary = await _engine(store, "s1").join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        normalize_keys=False,
    )
    # raw equality -> no match, nothing reported as enabled
    assert summary.join.matched_rows == 0
    assert summary.join.normalization.enabled == []


@pytest.mark.asyncio
async def test_report_is_structural_only(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", _stats_table(), op="stats")
    right = await _store(store, "s1", _parts_table(), op="parts")

    summary = await _engine(store, "s1").join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        how="left",
        validate="many_to_one",
    )
    structural = {
        "how", "indicator_column", "matched_rows", "unmatched_left_rows",
        "unmatched_right_rows", "left_keys_total", "left_keys_matched",
        "left_keys_unmatched", "right_keys_total", "right_keys_matched",
        "unmatched_key_sample", "normalization",
    }
    report_dump = summary.join.model_dump()
    assert set(report_dump.keys()) == structural
    norm_dump = report_dump["normalization"]
    assert set(norm_dump.keys()) == {
        "enabled", "left_keys_altered", "right_keys_altered",
        "matches_gained_by_normalization", "collisions",
    }
    # no business measure appears anywhere in the report
    assert "Netto" not in str(report_dump)
    assert "HauptLieferant" not in str(report_dump)


@pytest.mark.asyncio
async def test_indicator_disabled_and_inner_constant(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", _stats_table(), op="stats")
    right = await _store(store, "s1", _parts_table(), op="parts")
    engine = _engine(store, "s1")

    no_ind = await engine.join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        how="left",
        indicator=False,
    )
    assert no_ind.join.indicator_column is None
    out = await store.load_table(no_ind.result_id)
    assert "_join_match" not in out.column_names

    inner = await engine.join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        how="inner",
    )
    out = await store.load_table(inner.result_id)
    assert "_join_match" in out.column_names
    assert set(out["_join_match"].to_pylist()) == {"both"}


@pytest.mark.asyncio
async def test_right_and_full_joins(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", pa.table({"K": ["A1", "A2"], "v": [10, 20]}), op="l")
    right = await _store(store, "s1", pa.table({"K": ["A2", "A3"], "w": [200, 300]}), op="r")
    engine = _engine(store, "s1")

    rs = await engine.join(left.result_id, right.result_id, on=["K"], how="right")
    out = {r["K"]: r for r in (await store.load_table(rs.result_id)).to_pylist()}
    assert set(out.keys()) == {"A2", "A3"}
    assert out["A2"]["_join_match"] == "both"
    assert out["A3"]["_join_match"] == "right_only"
    assert out["A3"]["v"] is None
    assert out["A3"]["K"] == "A3"  # key coalesced from the right side

    fs = await engine.join(left.result_id, right.result_id, on=["K"], how="full")
    out = {r["K"]: r for r in (await store.load_table(fs.result_id)).to_pylist()}
    assert set(out.keys()) == {"A1", "A2", "A3"}
    assert out["A1"]["_join_match"] == "left_only"
    assert out["A2"]["_join_match"] == "both"
    assert out["A3"]["_join_match"] == "right_only"
    assert fs.join.matched_rows == 1
    assert fs.join.unmatched_left_rows == 1
    assert fs.join.unmatched_right_rows == 1


@pytest.mark.asyncio
async def test_count_left_only_matches_report(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", _stats_table(), op="stats")
    right = await _store(store, "s1", _parts_table(), op="parts")
    engine = _engine(store, "s1")

    summary = await engine.join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        how="left",
    )
    result_id = summary.result_id
    filters = FilterGroup(clauses=[FilterClause(column="_join_match", operator="eq", value="left_only")])
    count = await engine.count_rows(result_id, filters=filters)
    assert count["count"] == summary.join.unmatched_left_rows == 1


@pytest.mark.asyncio
async def test_aggregate_and_group_by_on_unmatched(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", _stats_table(), op="stats")
    right = await _store(store, "s1", _parts_table(), op="parts")
    engine = _engine(store, "s1")

    summary = await engine.join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        how="left",
        columns_right=["HauptLieferant", "WarenGruppe"],
    )
    result_id = summary.result_id
    filters = FilterGroup(clauses=[FilterClause(column="_join_match", operator="eq", value="left_only")])

    agg = await engine.aggregate(
        result_id,
        aggregations=[
            AggregationSpec(column="Netto", operation="sum", alias="netto_unmatched"),
            AggregationSpec(operation="count", alias="rows_unmatched"),
        ],
        filters=filters,
    )
    assert agg["aggregations"]["rows_unmatched"] == 1
    assert agg["aggregations"]["netto_unmatched"] == 30  # the unmatched left row

    grouped = await engine.group_by(
        result_id,
        group_columns=["HauptLieferant"],
        aggregations=[AggregationSpec(column="Netto", operation="sum", alias="netto_unmatched")],
        filters=filters,
    )
    assert grouped["total_groups"] == 1
    assert grouped["rows"][0]["netto_unmatched"] == 30
    assert grouped["rows"][0]["HauptLieferant"] is None


@pytest.mark.asyncio
async def test_unmatched_left_row_has_null_right_cols(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", _stats_table(), op="stats")
    right = await _store(store, "s1", _parts_table(), op="parts")
    engine = _engine(store, "s1")

    summary = await engine.join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        how="left",
    )
    rows = {r["ArtikelNummer"]: r for r in (await store.load_table(summary.result_id)).to_pylist()}
    assert rows["A3"]["HauptLieferant"] is None
    assert rows["A3"]["Beschreibung"] is None
    assert rows["A3"]["_join_match"] == "left_only"
    assert "A3" in summary.join.unmatched_key_sample


@pytest.mark.asyncio
async def test_collision_suffix_and_indicator_collision(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", pa.table({"K": ["A1"], "Netto": [10]}), op="l")
    right = await _store(store, "s1", pa.table({"K": ["A1"], "Netto": [999], "Alt": ["x"]}), op="r")
    engine = _engine(store, "s1")

    summary = await engine.join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["K"],
        how="left",
    )
    out = await store.load_table(summary.result_id)
    assert "Netto" in out.column_names
    assert "Netto_r" in out.column_names
    assert out["Netto_r"].to_pylist() == [999]

    with pytest.raises(ToolError) as ei:
        await engine.join(
            left_result_id=left.result_id,
            right_result_id=right.result_id,
            on=["K"],
            how="left",
            indicator="Netto",
        )
    assert ei.value.code == "COLUMN_NAME_COLLISION"


@pytest.mark.asyncio
async def test_collision_unresolvable_with_empty_suffix(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", pa.table({"K": ["A1"], "Netto": [10]}), op="l")
    right = await _store(store, "s1", pa.table({"K": ["A1"], "Netto": [999]}), op="r")
    with pytest.raises(ToolError) as ei:
        await _engine(store, "s1").join(
            left_result_id=left.result_id,
            right_result_id=right.result_id,
            on=["K"],
            how="left",
            suffix="",
        )
    assert ei.value.code == "COLUMN_NAME_COLLISION"


@pytest.mark.asyncio
async def test_output_size_estimate_before_materialization(fake_redis):
    small = Settings(
        RESULTSET_MAX_ROWS=5,
        RESULTSET_MAX_BYTES=50_000_000,
        RESULTSET_CHUNK_ROWS=100,
        RESULTSET_IDLE_TTL_SECONDS=3600,
        RESULTSET_MAX_TTL_SECONDS=86400,
        REDIS_KEY_PREFIX="test-join-small:" + uuid.uuid4().hex[:6],
    )
    # Inputs are stored with the normal (high) store limit; only the engine's
    # RESULTSET_MAX_ROWS is lowered so the pre-materialization estimate trips.
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", pa.table({"K": ["A1"]}), op="l")
    right = await _store(store, "s1", pa.table({"K": ["A1", "A1", "A1", "A1", "A1", "A1"]}), op="r")

    with pytest.raises(ToolError) as ei:
        await _engine(store, "s1", settings=small).join(
            left_result_id=left.result_id,
            right_result_id=right.result_id,
            on=["K"],
            how="left",
            validate="many_to_many",
        )
    assert ei.value.code == "RESULT_SET_TOO_LARGE"
    assert ei.value.details["estimated_row_count"] == 6


@pytest.mark.asyncio
async def test_owner_session_mismatch(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "sessA", _stats_table(), op="stats")
    right = await _store(store, "sessB", _parts_table(), op="parts")
    with pytest.raises(ToolError) as ei:
        await _engine(store, "sessA").join(
            left_result_id=left.result_id,
            right_result_id=right.result_id,
            on=["ArtikelNummer"],
        )
    assert ei.value.code == "OWNER_SESSION_MISMATCH"
    assert ei.value.details["left_session_id"] == "sessA"
    assert ei.value.details["right_session_id"] == "sessB"


@pytest.mark.asyncio
async def test_join_key_pair_different_names(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", pa.table({"KundenNr": ["K1"]}), op="orders")
    right = await _store(store, "s1", pa.table({"KundenNummer": ["K1"], "Land": ["DE"]}), op="cust")
    summary = await _engine(store, "s1").join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=[JoinColumnPair(left="KundenNr", right="KundenNummer")],
        how="left",
        columns_right=["Land"],
    )
    assert summary.join.matched_rows == 1
    out = await store.load_table(summary.result_id)
    assert out.column_names[0] == "KundenNr"
    assert "KundenNummer" not in out.column_names


@pytest.mark.asyncio
async def test_multi_column_key(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", pa.table({"A": ["x"], "B": ["y"], "Netto": [5]}), op="l")
    right = await _store(store, "s1", pa.table({"A": ["x"], "B": ["y"], "WG": ["z"]}), op="r")
    summary = await _engine(store, "s1").join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["A", "B"],
        how="left",
        columns_right=["WG"],
    )
    assert summary.join.matched_rows == 1
    out = await store.load_table(summary.result_id)
    assert out["WG"].to_pylist() == ["z"]


@pytest.mark.asyncio
async def test_chaining_into_all_read_tools_and_nested_join(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", _stats_table(), op="stats")
    right = await _store(store, "s1", _parts_table(), op="parts")
    engine = _engine(store, "s1")

    summary = await engine.join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        how="left",
        columns_right=["WarenGruppe", "HauptLieferant"],
    )
    rid = summary.result_id

    info = await engine.get_result_info(rid)
    assert info["row_count"] == 3
    columns = await engine.explain_columns(rid)
    assert len(columns["columns"]) == 6  # 3 left + 2 right + indicator
    rows = await engine.list_rows(rid, limit=2)
    assert rows["total_rows"] == 3
    dv = await engine.distinct_values(rid, column="_join_match")
    assert {v["value"] for v in dv["values"]} == {"both", "left_only"}
    hist = await engine.histogram(rid, column="_join_match")
    assert hist is not None
    agg = await engine.aggregate(rid, aggregations=[AggregationSpec(column="Netto", operation="sum", alias="n")])
    assert agg["aggregations"]["n"] == 60

    # nested join: join the join output onto a third set
    third = await _store(store, "s1", pa.table({"ArtikelNummer": ["A1", "A2"], "Status": ["x", "y"]}), op="third")
    nested = await engine.join(
        left_result_id=rid,
        right_result_id=third.result_id,
        on=["ArtikelNummer"],
        how="left",
        columns_right=["Status"],
        indicator="_join_match2",
    )
    assert nested.join.matched_rows == 2
    assert nested.join.indicator_column == "_join_match2"


@pytest.mark.asyncio
async def test_join_provenance_has_parents(fake_redis):
    store = await _build_store(fake_redis)
    left = await _store(store, "s1", _stats_table(), op="stats")
    right = await _store(store, "s1", _parts_table(), op="parts")
    summary = await _engine(store, "s1").join(
        left_result_id=left.result_id,
        right_result_id=right.result_id,
        on=["ArtikelNummer"],
        how="left",
    )
    meta = await store.get_metadata(summary.result_id)
    assert meta.provenance.op == "join"
    assert meta.provenance.parents == [left.result_id, right.result_id]
    assert meta.provenance.parameters["how"] == "left"