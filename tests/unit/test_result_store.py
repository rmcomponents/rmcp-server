"""Unit tests for ResultSetStore using a fake Redis."""

import uuid
from datetime import datetime, timezone, timedelta

import pyarrow as pa
import pytest

from alphaplan_mcp.config import Settings
from alphaplan_mcp.result_store import ResultSetStore
from alphaplan_mcp.errors import ToolError
from alphaplan_mcp.models import ResultProvenance


settings_for_tests = Settings(
    RESULTSET_MAX_ROWS=1000,
    RESULTSET_MAX_BYTES=10_000_000,
    RESULTSET_CHUNK_ROWS=100,
    RESULTSET_IDLE_TTL_SECONDS=3600,
    RESULTSET_MAX_TTL_SECONDS=86400,
    REDIS_KEY_PREFIX="test:" + uuid.uuid4().hex[:6],
)


def _make_table(n=10):
    return pa.table({
        "id": list(range(n)),
        "name": [f"name_{i}" for i in range(n)],
        "value": [i * 1.5 for i in range(n)],
    })


@pytest.mark.asyncio
async def test_store_and_load(fake_redis):
    store = ResultSetStore(
        fake_redis,
        prefix=settings_for_tests.REDIS_KEY_PREFIX,
        chunk_rows=settings_for_tests.RESULTSET_CHUNK_ROWS,
        idle_ttl=settings_for_tests.RESULTSET_IDLE_TTL_SECONDS,
        max_ttl=settings_for_tests.RESULTSET_MAX_TTL_SECONDS,
        max_rows=settings_for_tests.RESULTSET_MAX_ROWS,
        max_bytes=settings_for_tests.RESULTSET_MAX_BYTES,
    )

    table = _make_table(50)
    summary = await store.store_result(
        session_id="sess1",
        provenance=ResultProvenance(op="test", parameters={"filter": "all"}),
        table=table,
    )
    assert summary.result_id.startswith("rs_")
    assert summary.row_count == 50
    assert summary.column_count == 3

    # Load back
    loaded = await store.load_table(summary.result_id)
    assert loaded is not None
    assert loaded.num_rows == 50
    assert loaded.column("id").to_pylist() == list(range(50))


@pytest.mark.asyncio
async def test_store_result_exceeds_max_rows(fake_redis):
    store = ResultSetStore(
        fake_redis,
        prefix=settings_for_tests.REDIS_KEY_PREFIX,
        chunk_rows=settings_for_tests.RESULTSET_CHUNK_ROWS,
        idle_ttl=settings_for_tests.RESULTSET_IDLE_TTL_SECONDS,
        max_ttl=settings_for_tests.RESULTSET_MAX_TTL_SECONDS,
        max_rows=settings_for_tests.RESULTSET_MAX_ROWS,
        max_bytes=settings_for_tests.RESULTSET_MAX_BYTES,
    )

    big_table = _make_table(1500)  # > 1000
    with pytest.raises(ToolError) as exc:
        await store.store_result("sess1", ResultProvenance(op="test"), big_table)
    assert "TOO_LARGE" in exc.value.code


@pytest.mark.asyncio
async def test_ownership_enforced(fake_redis):
    store = ResultSetStore(
        fake_redis,
        prefix=settings_for_tests.REDIS_KEY_PREFIX,
        chunk_rows=settings_for_tests.RESULTSET_CHUNK_ROWS,
        idle_ttl=settings_for_tests.RESULTSET_IDLE_TTL_SECONDS,
        max_ttl=settings_for_tests.RESULTSET_MAX_TTL_SECONDS,
        max_rows=settings_for_tests.RESULTSET_MAX_ROWS,
        max_bytes=settings_for_tests.RESULTSET_MAX_BYTES,
    )

    table = _make_table(10)
    summary = await store.store_result("sess1", ResultProvenance(op="test"), table)

    # Different session tries to access
    meta = await store.get_metadata(summary.result_id)
    assert meta is not None
    assert meta.owner_session_id == "sess1"

    # delete by wrong session (ownership is enforced at the tool level, not store level)
    deleted = await store.delete_result("sess2", summary.result_id)
    assert deleted is True

    # delete by correct session (already deleted)
    deleted = await store.delete_result("sess1", summary.result_id)
    assert deleted is False

    # after deletion
    meta = await store.get_metadata(summary.result_id)
    assert meta is None


@pytest.mark.asyncio
async def test_list_results(fake_redis):
    store = ResultSetStore(
        fake_redis,
        prefix=settings_for_tests.REDIS_KEY_PREFIX,
        chunk_rows=settings_for_tests.RESULTSET_CHUNK_ROWS,
        idle_ttl=settings_for_tests.RESULTSET_IDLE_TTL_SECONDS,
        max_ttl=settings_for_tests.RESULTSET_MAX_TTL_SECONDS,
        max_rows=settings_for_tests.RESULTSET_MAX_ROWS,
        max_bytes=settings_for_tests.RESULTSET_MAX_BYTES,
    )

    await store.store_result("sess1", ResultProvenance(op="src1"), _make_table(5))
    await store.store_result("sess1", ResultProvenance(op="src2"), _make_table(10))
    # session2 storage
    await store.store_result("sess2", ResultProvenance(op="other"), _make_table(3))

    list_s1 = await store.list_results("sess1")
    assert len(list_s1) == 2
    assert all(m.owner_session_id == "sess1" for m in list_s1)

    list_s2 = await store.list_results("sess2")
    assert len(list_s2) == 1
    assert list_s2[0].provenance.op == "other"


@pytest.mark.asyncio
async def test_load_range(fake_redis):
    store = ResultSetStore(
        fake_redis,
        prefix=settings_for_tests.REDIS_KEY_PREFIX,
        chunk_rows=10,  # small chunks for testing
        idle_ttl=settings_for_tests.RESULTSET_IDLE_TTL_SECONDS,
        max_ttl=settings_for_tests.RESULTSET_MAX_TTL_SECONDS,
        max_rows=settings_for_tests.RESULTSET_MAX_ROWS,
        max_bytes=settings_for_tests.RESULTSET_MAX_BYTES,
    )

    table = _make_table(35)  # will be split into 4 chunks (10,10,10,5)
    summary = await store.store_result("sess1", ResultProvenance(op="test"), table)

    # Load rows 5-25 (1-indexed: start=5, end=25)
    partial = await store.load_range(summary.result_id, 5, 25)
    assert partial is not None
    assert partial.num_rows == 20
    assert partial.column("id").to_pylist() == list(range(5, 25))

    # Entire range
    full = await store.load_range(summary.result_id, 0, 35)
    assert full is not None
    assert full.num_rows == 35


# ---------------------------------------------------------------------------
# Lifetime policy: a set lives idle_ttl after its last use - creation counts as a use - and
# never beyond max_ttl after creation. expires_at always reports that real expiry, and
# reading a set never shortens its life.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_expires_at_is_idle_window_from_creation_and_access(fake_redis):
    store = ResultSetStore(
        fake_redis,
        prefix=settings_for_tests.REDIS_KEY_PREFIX,
        idle_ttl=10,
        max_ttl=3600,
    )
    summary = await store.store_result(
        session_id="sess1",
        provenance=ResultProvenance(op="t"),
        table=_make_table(3),
    )
    now = datetime.now(timezone.utc)

    # At creation: the idle window, reported and enforced (summary == stored metadata == key TTL).
    assert 0 < (summary.expires_at - now).total_seconds() <= 10 + 2
    meta0 = await store.get_metadata(summary.result_id)
    assert meta0.expires_at == summary.expires_at
    assert (meta0.absolute_expires_at - now).total_seconds() >= 3550  # cap stays 1 h
    assert 0 < await fake_redis.ttl(store._meta_key(summary.result_id)) <= 10
    assert 0 < await fake_redis.ttl(store._chunk_key(summary.result_id, 0)) <= 10

    # After an access: re-armed to the idle window again (never shortened below it).
    updated = await store.update_access(summary.result_id)
    distance = (updated.expires_at - datetime.now(timezone.utc)).total_seconds()
    assert 0 < distance <= 10 + 2


@pytest.mark.asyncio
async def test_stream_store_uses_idle_window_from_creation(fake_redis):
    store = ResultSetStore(fake_redis, prefix=settings_for_tests.REDIS_KEY_PREFIX,
                           idle_ttl=10, max_ttl=3600)

    async def batches():
        yield pa.RecordBatch.from_pylist([{"a": 1}])

    summary = await store.store_result_stream(
        session_id="sess1", provenance=ResultProvenance(op="t"), batches=batches())
    assert 0 < (summary.expires_at - datetime.now(timezone.utc)).total_seconds() <= 10 + 2
    assert 0 < await fake_redis.ttl(store._chunk_key(summary.result_id, 0)) <= 10


@pytest.mark.asyncio
async def test_idle_window_never_exceeds_absolute_cap(fake_redis):
    store = ResultSetStore(fake_redis, prefix=settings_for_tests.REDIS_KEY_PREFIX,
                           idle_ttl=7200, max_ttl=60)
    summary = await store.store_result(
        session_id="sess1", provenance=ResultProvenance(op="t"), table=_make_table(3))
    assert (summary.expires_at - datetime.now(timezone.utc)).total_seconds() <= 60 + 2


# Older/backfilled metadata without `expires_at` still deserializes and reports the cap.
@pytest.mark.asyncio
async def test_update_access_returns_refreshed_metadata(fake_redis):
    store = ResultSetStore(fake_redis, prefix=settings_for_tests.REDIS_KEY_PREFIX)
    summary = await store.store_result(
        session_id="sess1", provenance=ResultProvenance(op="t"), table=_make_table(3)
    )
    updated = await store.update_access(summary.result_id)
    assert updated is not None
    assert updated.last_accessed_at >= updated.created_at


@pytest.mark.asyncio
async def test_store_stream_with_date_columns(fake_redis):
    """date32 columns (list_beleg_positionen dates) store, carry min/max stats and load back."""
    import datetime as dt
    store = ResultSetStore(
        fake_redis,
        prefix=settings_for_tests.REDIS_KEY_PREFIX,
        chunk_rows=settings_for_tests.RESULTSET_CHUNK_ROWS,
        idle_ttl=settings_for_tests.RESULTSET_IDLE_TTL_SECONDS,
        max_ttl=settings_for_tests.RESULTSET_MAX_TTL_SECONDS,
        max_rows=settings_for_tests.RESULTSET_MAX_ROWS,
        max_bytes=settings_for_tests.RESULTSET_MAX_BYTES,
    )
    schema = pa.schema([("BelegDatum", pa.date32()), ("BestaetigtZum", pa.date32())])

    async def batches():
        yield pa.RecordBatch.from_pylist(
            [{"BelegDatum": dt.date(2025, 12, 1), "BestaetigtZum": None}], schema=schema)
        yield pa.RecordBatch.from_pylist(
            [{"BelegDatum": dt.date(2025, 12, 31), "BestaetigtZum": dt.date(2026, 1, 5)}], schema=schema)

    summary = await store.store_result_stream(
        session_id="sess1",
        provenance=ResultProvenance(op="test", parameters={}),
        batches=batches(),
    )
    info = {c.name: c for c in summary.columns}
    assert str(info["BelegDatum"].min) == "2025-12-01"
    assert str(info["BelegDatum"].max) == "2025-12-31"
    loaded = await store.load_table(summary.result_id)
    assert loaded.column("BestaetigtZum").to_pylist() == [None, dt.date(2026, 1, 5)]


@pytest.mark.asyncio
async def test_empty_stream_with_schema_is_queryable(fake_redis):
    """A result with 0 rows keeps its columns; filters and query_rows return 0 rows, not RESULT_NOT_FOUND."""
    from alphaplan_mcp.query_engine import QueryEngine
    from alphaplan_mcp.models import FilterClause, FilterGroup
    store = ResultSetStore(fake_redis, prefix=settings_for_tests.REDIS_KEY_PREFIX)
    schema = pa.schema([("BelegNummer", pa.string()), ("Menge", pa.float64())])

    async def batches():
        yield pa.RecordBatch.from_pylist([], schema=schema)

    summary = await store.store_result_stream(
        session_id="sess1", provenance=ResultProvenance(op="test", parameters={}), batches=batches())
    assert summary.row_count == 0 and summary.column_count == 2

    engine = QueryEngine(store, "sess1")
    result = await engine.query_rows(summary.result_id)
    assert result["total_rows"] == 0 and result["rows"] == []
    flt = FilterGroup(clauses=[FilterClause(column="Menge", operator="gt", value=0)])
    assert (await engine.count_rows(summary.result_id, filters=flt))["count"] == 0


@pytest.mark.asyncio
async def test_empty_stream_without_batches_is_queryable(fake_redis):
    """Even a stream without any batch stores a readable (column-less) empty set."""
    from alphaplan_mcp.query_engine import QueryEngine
    store = ResultSetStore(fake_redis, prefix=settings_for_tests.REDIS_KEY_PREFIX)

    async def batches():
        return
        yield  # pragma: no cover

    summary = await store.store_result_stream(
        session_id="sess1", provenance=ResultProvenance(op="test", parameters={}), batches=batches())
    result = await QueryEngine(store, "sess1").query_rows(summary.result_id)
    assert result["total_rows"] == 0
