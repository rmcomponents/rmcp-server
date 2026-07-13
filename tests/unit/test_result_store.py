"""Unit tests for ResultSetStore using a fake Redis."""

import uuid
from datetime import datetime, timezone, timedelta

import pyarrow as pa
import pytest

from alphaplan_mcp.config import Settings
from alphaplan_mcp.result_store import ResultSetStore
from alphaplan_mcp.errors import ToolError


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
        source="test",
        source_parameters={"filter": "all"},
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
        await store.store_result("sess1", "test", None, big_table)
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
    summary = await store.store_result("sess1", "test", None, table)

    # Different session tries to access
    meta = await store.get_metadata(summary.result_id)
    assert meta is not None
    assert meta.owner_session_id == "sess1"

    # delete by wrong session
    deleted = await store.delete_result("sess2", summary.result_id)
    assert deleted is False

    # delete by correct session
    deleted = await store.delete_result("sess1", summary.result_id)
    assert deleted is True

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

    await store.store_result("sess1", "src1", None, _make_table(5))
    await store.store_result("sess1", "src2", None, _make_table(10))
    # session2 storage
    await store.store_result("sess2", "other", None, _make_table(3))

    list_s1 = await store.list_results("sess1")
    assert len(list_s1) == 2
    assert all(m.owner_session_id == "sess1" for m in list_s1)

    list_s2 = await store.list_results("sess2")
    assert len(list_s2) == 1
    assert list_s2[0].source == "other"


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
    summary = await store.store_result("sess1", "test", None, table)

    # Load rows 5-25 (1-indexed: start=5, end=25)
    partial = await store.load_range(summary.result_id, 5, 25)
    assert partial is not None
    assert partial.num_rows == 20
    assert partial.column("id").to_pylist() == list(range(5, 25))

    # Entire range
    full = await store.load_range(summary.result_id, 0, 35)
    assert full is not None
    assert full.num_rows == 35
