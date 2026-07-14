# Handles serialization and deserialization of PyArrow tables
# and ResultSetMetadata to/from bytes for Redis storage,
# preserving database types.
"""PyArrow serialization helpers."""

import json
from datetime import datetime
from typing import Any, Dict, Optional

import pyarrow as pa

from .models import ResultSetMetadata, ColumnInfo

def _default_encoder(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def serialize_metadata(meta: ResultSetMetadata) -> bytes:
    """Convert metadata model to JSON bytes."""
    return json.dumps(meta.model_dump(), default=_default_encoder).encode("utf-8")

def deserialize_metadata(data: bytes) -> ResultSetMetadata:
    """Reconstruct metadata model from JSON bytes."""
    return ResultSetMetadata.model_validate(json.loads(data.decode("utf-8")))

def serialize_table(table: pa.Table) -> bytes:
    """Compress a PyArrow table to LZ4-compressed Arrow IPC bytes."""
    import pyarrow.ipc as ipc
    # Use RecordBatchStreamWriter with LZ4 compression
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema, options=ipc.IpcWriteOptions(compression="lz4")) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()

def deserialize_table(data: bytes) -> pa.Table:
    """Decompress LZ4-compressed Arrow IPC bytes back to a PyArrow table."""
    import pyarrow.ipc as ipc
    reader = ipc.open_stream(pa.py_buffer(data))
    return reader.read_all()
