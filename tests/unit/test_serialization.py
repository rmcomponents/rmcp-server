"""Unit tests for serialization helpers (serialization.py)."""

import json
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from alphaplan_mcp.models import ColumnInfo, ResultSetMetadata
from alphaplan_mcp.serialization import (
    serialize_metadata,
    deserialize_metadata,
    serialize_table,
    deserialize_table,
)


class TestMetadataSerialization:
    def test_round_trip(self):
        now = datetime.now(timezone.utc)
        meta = ResultSetMetadata(
            result_id="rs_abc123",
            owner_session_id="session1",
            source="customers",
            created_at=now,
            last_accessed_at=now,
            absolute_expires_at=now,
            row_count=100,
            column_count=3,
            uncompressed_size_bytes=5000,
            stored_size_bytes=2500,
            chunk_count=2,
            columns=[
                ColumnInfo(name="id", type="int64", null_count=0, non_null_count=100),
                ColumnInfo(name="name", type="string", null_count=5, non_null_count=95),
            ],
            source_parameters={"customer_name": "Acme"},
        )

        data = serialize_metadata(meta)
        # Should be valid JSON bytes
        parsed = json.loads(data.decode("utf-8"))
        assert parsed["result_id"] == "rs_abc123"
        assert parsed["owner_session_id"] == "session1"

        recovered = deserialize_metadata(data)
        assert recovered.result_id == meta.result_id
        assert recovered.owner_session_id == meta.owner_session_id
        assert recovered.row_count == meta.row_count
        assert len(recovered.columns) == 2
        assert recovered.columns[0].name == "id"
        assert recovered.source_parameters == {"customer_name": "Acme"}


class TestArrowSerialization:
    def test_round_trip_small_table(self):
        table = pa.table({
            "col_int": [1, 2, 3],
            "col_str": ["a", "b", None],
            "col_float": [1.1, 2.2, 3.3],
        })
        data = serialize_table(table)
        recovered = deserialize_table(data)
        assert recovered.num_rows == 3
        assert recovered.num_columns == 3
        assert recovered.column("col_int").to_pylist() == [1, 2, 3]
        assert recovered.column("col_str").to_pylist() == ["a", "b", None]

    def test_round_trip_empty(self):
        table = pa.table({"a": pa.array([], type=pa.int64())})
        data = serialize_table(table)
        recovered = deserialize_table(data)
        assert recovered.num_rows == 0

    def test_preserves_dates(self):
        from datetime import date
        table = pa.table({"d": [date(2024, 1, 1), date(2024, 12, 31)]})
        data = serialize_table(table)
        recovered = deserialize_table(data)
        assert recovered.column("d").to_pylist() == [date(2024, 1, 1), date(2024, 12, 31)]

    def test_preserves_decimals(self):
        import decimal
        arr = pa.array([decimal.Decimal("10.50"), decimal.Decimal("0.01")])
        table = pa.table({"price": arr})
        data = serialize_table(table)
        recovered = deserialize_table(data)
        assert recovered.column("price").to_pylist() == [decimal.Decimal("10.50"), decimal.Decimal("0.01")]
