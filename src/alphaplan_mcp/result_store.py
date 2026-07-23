# Implements the ResultSetStore for chunked, transactional Redis storage.
# Enforces ownership, TTLs, and complete-or-fail safety limits.
"""Redis-backed result‑set storage with ownership, TTL, and chunking."""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Dict, List, Optional, Tuple

import pyarrow as pa
from redis.asyncio import Redis

from .config import settings
from .models import ColumnInfo, ResultSetMetadata, ResultSetSummary
from .serialization import deserialize_metadata, deserialize_table, serialize_metadata, serialize_table
from .errors import tool_error

logger = logging.getLogger(__name__)

class ResultSetStore:
    def __init__(
        self,
        redis: Redis,
        prefix: str = settings.REDIS_KEY_PREFIX,
        chunk_rows: int = settings.RESULTSET_CHUNK_ROWS,
        idle_ttl: int = settings.RESULTSET_IDLE_TTL_SECONDS,
        max_ttl: int = settings.RESULTSET_MAX_TTL_SECONDS,
        max_rows: int = settings.RESULTSET_MAX_ROWS,
        max_bytes: int = settings.RESULTSET_MAX_BYTES,
    ):
        self.redis = redis
        self.prefix = prefix
        self.chunk_rows = chunk_rows
        self.idle_ttl = idle_ttl
        self.max_ttl = max_ttl
        self.max_rows = max_rows
        self.max_bytes = max_bytes

    # ---------- key generators ----------
    def _result_key(self, result_id: str) -> str:
        return f"{self.prefix}:result:{result_id}"

    def _meta_key(self, result_id: str) -> str:
        return f"{self._result_key(result_id)}:meta"

    def _chunk_key(self, result_id: str, chunk_index: int) -> str:
        return f"{self._result_key(result_id)}:chunk:{chunk_index:06d}"

    def _session_key(self, session_id: str) -> str:
        return f"{self.prefix}:session:{session_id}:results"

    # ---------- core ingestion ----------
    async def store_result(
        self,
        session_id: str,
        source: str,
        source_parameters: Optional[dict],
        table: pa.Table,
    ) -> ResultSetSummary:
        """Stream a PyArrow table into Redis, enforcing row/byte limits."""
        if table.num_rows > self.max_rows:
            raise tool_error(
                "RESULT_SET_TOO_LARGE",
                f"Row count {table.num_rows} exceeds limit {self.max_rows}",
            )

        result_id = f"rs_{uuid.uuid4()}"
        now = datetime.now(timezone.utc)
        absolute_expires_at = now + timedelta(seconds=self.max_ttl)

        # split into chunks
        total_rows = table.num_rows
        chunks = []
        for start in range(0, total_rows, self.chunk_rows):
            end = min(start + self.chunk_rows, total_rows)
            chunks.append(table.slice(start, end - start))

        # compute column stats and metadata
        columns_info = self._compute_column_stats(table)
        meta = ResultSetMetadata(
            result_id=result_id,
            owner_session_id=session_id,
            source=source,
            created_at=now,
            last_accessed_at=now,
            absolute_expires_at=absolute_expires_at,
            row_count=total_rows,
            column_count=len(table.schema),
            uncompressed_size_bytes=table.nbytes,
            stored_size_bytes=0,      # will fill after serialization
            chunk_count=len(chunks),
            columns=columns_info,
            source_parameters=source_parameters,
        )

        if meta.uncompressed_size_bytes > self.max_bytes:
            raise tool_error(
                "RESULT_SET_TOO_LARGE",
                f"Uncompressed size {meta.uncompressed_size_bytes} exceeds limit {self.max_bytes}",
            )

        # write chunks and metadata in one pipeline
        pipe = self.redis.pipeline()
        total_stored = 0
        for idx, chunk in enumerate(chunks):
            chunk_key = self._chunk_key(result_id, idx)
            serialized = serialize_table(chunk)
            total_stored += len(serialized)
            pipe.set(chunk_key, serialized, ex=self.max_ttl)

        meta.stored_size_bytes = total_stored
        meta_key = self._meta_key(result_id)
        pipe.set(meta_key, serialize_metadata(meta), ex=self.max_ttl)

        # session index
        session_key = self._session_key(session_id)
        pipe.sadd(session_key, result_id)
        pipe.expire(session_key, self.max_ttl)

        await pipe.execute()

        logger.info(
            "Stored result set %s: %d rows, %d chunks, %d bytes",
            result_id, total_rows, len(chunks), total_stored,
        )

        return ResultSetSummary(
            result_id=result_id,
            source=source,
            row_count=total_rows,
            column_count=len(table.schema),
            stored_size_bytes=total_stored,
            created_at=now,
            expires_at=absolute_expires_at,
            columns=[ColumnInfo(name=c.name, type=c.type, null_count=c.null_count, non_null_count=c.non_null_count)
                     for c in columns_info],
        )

    async def store_result_stream(
        self,
        session_id: str,
        source: str,
        source_parameters: Optional[dict],
        batches: AsyncGenerator[pa.RecordBatch, None],
    ) -> ResultSetSummary:
        """Ingest a stream of Arrow record batches, writing chunks incrementally."""
        result_id = f"rs_{uuid.uuid4()}"
        now = datetime.now(timezone.utc)
        absolute_expires_at = now + timedelta(seconds=self.max_ttl)

        # Temporary TTL for chunks during ingestion (5 minutes)
        staging_ttl = 300

        chunk_index = 0
        total_rows = 0
        total_stored = 0
        total_uncompressed = 0          # NEW accumulator
        schema = None
        stats_accum = _IncrementalStats()

        pipe = self.redis.pipeline()
        async for batch in batches:
            if schema is None:
                schema = batch.schema
            total_rows += batch.num_rows
            if total_rows > self.max_rows:
                # Clean up already written chunks using a fresh pipeline
                chunk_keys = [self._chunk_key(result_id, idx) for idx in range(chunk_index)]
                if chunk_keys:
                    await self.redis.delete(*chunk_keys)
                raise tool_error(
                    "RESULT_SET_TOO_LARGE",
                    f"Row count exceeded {self.max_rows}",
                )

            # NEW: track uncompressed size
            batch_bytes = batch.nbytes
            total_uncompressed += batch_bytes
            if total_uncompressed > self.max_bytes:
                chunk_keys = [self._chunk_key(result_id, idx) for idx in range(chunk_index)]
                if chunk_keys:
                    await self.redis.delete(*chunk_keys)
                raise tool_error(
                    "RESULT_SET_TOO_LARGE",
                    f"Uncompressed size {total_uncompressed} exceeds limit {self.max_bytes}",
                )

            stats_accum.update(batch)

            chunk_key = self._chunk_key(result_id, chunk_index)
            serialized = serialize_table(pa.Table.from_batches([batch]))
            total_stored += len(serialized)
            pipe.set(chunk_key, serialized, ex=staging_ttl)
            chunk_index += 1

            # Flush pipeline periodically to avoid huge pipelines
            if chunk_index % 10 == 0:
                await pipe.execute()
                pipe = self.redis.pipeline()

        await pipe.execute()

        # Treat an empty stream as a valid, empty result set
        if schema is None:
            schema = pa.schema([])
            columns_info: List[ColumnInfo] = []
            total_rows = 0
            total_uncompressed = 0
            total_stored = 0
            chunk_count = 0
        else:
            columns_info = stats_accum.finalize(schema)
            chunk_count = chunk_index

        meta = ResultSetMetadata(
            result_id=result_id,
            owner_session_id=session_id,
            source=source,
            created_at=now,
            last_accessed_at=now,
            absolute_expires_at=absolute_expires_at,
            row_count=total_rows,
            column_count=len(schema),
            uncompressed_size_bytes=total_uncompressed,
            stored_size_bytes=total_stored,
            chunk_count=chunk_count,
            columns=columns_info,
            source_parameters=source_parameters,
        )

        # Promote chunk TTLs (if any) and write metadata + session index
        pipe = self.redis.pipeline()
        for idx in range(chunk_count):
            pipe.expire(self._chunk_key(result_id, idx), self.max_ttl)
        pipe.set(self._meta_key(result_id), serialize_metadata(meta), ex=self.max_ttl)
        pipe.sadd(self._session_key(session_id), result_id)
        pipe.expire(self._session_key(session_id), self.max_ttl)
        await pipe.execute()

        logger.info(
            "Stream-stored result set %s: %d rows, %d chunks, %d bytes",
            result_id, total_rows, chunk_count, total_stored,
        )

        # Build summary with only name, type, null_count (as per agent.md)
        summary_columns = [
            ColumnInfo(name=c.name, type=c.type, null_count=c.null_count, non_null_count=c.non_null_count)
            for c in columns_info
        ]
        return ResultSetSummary(
            result_id=result_id,
            source=source,
            row_count=total_rows,
            column_count=len(schema),
            stored_size_bytes=total_stored,
            created_at=now,
            expires_at=absolute_expires_at,
            columns=summary_columns,
        )

    # ---------- retrieval ----------
    async def get_metadata(self, result_id: str) -> Optional[ResultSetMetadata]:
        data = await self.redis.get(self._meta_key(result_id))
        if data is None:
            return None
        return deserialize_metadata(data)

    async def update_access(self, result_id: str) -> None:
        meta = await self.get_metadata(result_id)
        if not meta:
            return
        now = datetime.now(timezone.utc)
        remaining = (meta.absolute_expires_at - now).total_seconds()
        if remaining <= 0:
            return
        effective_ttl = min(self.idle_ttl, int(remaining))
        if effective_ttl <= 0:
            return
        pipe = self.redis.pipeline()
        pipe.expire(self._meta_key(result_id), effective_ttl)
        for idx in range(meta.chunk_count):
            pipe.expire(self._chunk_key(result_id, idx), effective_ttl)
        await pipe.execute()

    async def load_table(self, result_id: str) -> Optional[pa.Table]:
        meta = await self.get_metadata(result_id)
        if not meta:
            return None
        tables = []
        for idx in range(meta.chunk_count):
            data = await self.redis.get(self._chunk_key(result_id, idx))
            if data is None:
                logger.error("Missing chunk %d for result %s", idx, result_id)
                return None
            tables.append(deserialize_table(data))
        return pa.concat_tables(tables) if tables else None

    async def load_range(self, result_id: str, start: int, end: int) -> Optional[pa.Table]:
        meta = await self.get_metadata(result_id)
        if not meta or start >= meta.row_count or end <= 0:
            return None
        start = max(0, start)
        end = min(meta.row_count, end)
        first_chunk = start // self.chunk_rows
        last_chunk = (end - 1) // self.chunk_rows
        tables = []
        for idx in range(first_chunk, last_chunk + 1):
            data = await self.redis.get(self._chunk_key(result_id, idx))
            if data is None:
                logger.error("Missing chunk %d", idx)
                return None
            chunk_table = deserialize_table(data)
            chunk_start = idx * self.chunk_rows
            offset = max(0, start - chunk_start)
            length = min(chunk_table.num_rows, end - chunk_start) - offset
            if length <= 0:
                continue
            tables.append(chunk_table.slice(offset, length))
        return pa.concat_tables(tables) if tables else pa.table({})

    # ---------- management ----------
    async def delete_result(self, session_id: str, result_id: str) -> bool:
        meta = await self.get_metadata(result_id)
        if not meta or meta.owner_session_id != session_id:
            return False
        pipe = self.redis.pipeline()
        pipe.delete(self._meta_key(result_id))
        for idx in range(meta.chunk_count):
            pipe.delete(self._chunk_key(result_id, idx))
        pipe.srem(self._session_key(session_id), result_id)
        await pipe.execute()
        return True

    async def list_results(self, session_id: str) -> List[ResultSetMetadata]:
        session_key = self._session_key(session_id)
        result_ids = await self.redis.smembers(session_key)
        metas: List[ResultSetMetadata] = []
        for rid in result_ids:
            rid_str = rid.decode() if isinstance(rid, bytes) else rid
            meta = await self.get_metadata(rid_str)
            if meta and meta.owner_session_id == session_id:
                metas.append(meta)
            else:
                # stale entry – remove from index
                await self.redis.srem(session_key, rid_str)
        return metas

    # ---------- cleanup ----------
    async def _cleanup_orphans(self) -> int:
        """Scan Redis for result‑related keys and remove orphan chunks/session entries."""
        cursor = 0
        cleaned = 0
        pattern = f"{self.prefix}:result:*"
        while True:
            cursor, keys = await self.redis.scan(cursor=cursor, match=pattern, count=100)
            for key in keys:
                key_str = key.decode() if isinstance(key, bytes) else key
                parts = key_str.split(":")
                if len(parts) < 4:
                    continue
                result_id = parts[2]
                # If metadata does not exist, delete this key (could be orphan chunk)
                meta_key = self._meta_key(result_id)
                if not await self.redis.exists(meta_key):
                    await self.redis.delete(key)
                    cleaned += 1
            if cursor == 0:
                break
        return cleaned

    async def run_cleanup(self):
        """Entry point for periodic background cleanup."""
        try:
            orphan_count = await self._cleanup_orphans()
            if orphan_count:
                logger.info("Cleanup removed %d orphan keys", orphan_count)
        except Exception as e:
            logger.warning("Cleanup encountered an error: %s", e)

    # ---------- internal helpers ----------
    def _compute_column_stats(self, table: pa.Table) -> List[ColumnInfo]:
        import pyarrow.compute as pc
        infos = []
        for field in table.schema:
            col = table.column(field.name)
            null_count = col.null_count
            non_null_count = table.num_rows - null_count
            min_val = max_val = sum_val = avg_val = min_len = max_len = None

            if pa.types.is_integer(col.type) or pa.types.is_floating(col.type):
                min_val = pc.min(col).as_py()
                max_val = pc.max(col).as_py()
                sum_val = pc.sum(col).as_py()
                avg_val = pc.mean(col).as_py()
            elif pa.types.is_date(col.type) or pa.types.is_timestamp(col.type):
                min_val = pc.min(col).as_py()
                max_val = pc.max(col).as_py()
            elif pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
                lengths = pc.utf8_length(col)
                min_len = pc.min(lengths).as_py()
                max_len = pc.max(lengths).as_py()

            description = field.metadata.get(b"description", b"").decode("utf-8") if field.metadata else ""
            infos.append(ColumnInfo(
                name=field.name,
                type=str(col.type),
                null_count=null_count,
                non_null_count=non_null_count,
                min=min_val,
                max=max_val,
                sum=sum_val,
                avg=avg_val,
                min_length=min_len,
                max_length=max_len,
                description=description,
            ))
        return infos


class _IncrementalStats:
    """Accumulate column statistics across multiple record batches."""

    def __init__(self):
        self._stats = {}  # column name -> dict of accumulators

    def update(self, batch: pa.RecordBatch):
        import pyarrow.compute as pc
        for field in batch.schema:
            col = batch.column(field.name)
            name = field.name
            if name not in self._stats:
                self._stats[name] = {
                    "null_count": 0,
                    "min": None,
                    "max": None,
                    "sum": 0.0,
                    "count": 0,
                    "min_length": None,
                    "max_length": None,
                }
            s = self._stats[name]
            s["null_count"] += col.null_count

            non_null = len(col) - col.null_count
            if non_null == 0:
                continue

            if pa.types.is_integer(col.type) or pa.types.is_floating(col.type):
                col_min = pc.min(col).as_py()
                col_max = pc.max(col).as_py()
                col_sum = pc.sum(col).as_py()
                if s["min"] is None or col_min < s["min"]:
                    s["min"] = col_min
                if s["max"] is None or col_max > s["max"]:
                    s["max"] = col_max
                s["sum"] += col_sum
                s["count"] += non_null
            elif pa.types.is_date(col.type) or pa.types.is_timestamp(col.type):
                col_min = pc.min(col).as_py()
                col_max = pc.max(col).as_py()
                if s["min"] is None or col_min < s["min"]:
                    s["min"] = col_min
                if s["max"] is None or col_max > s["max"]:
                    s["max"] = col_max
            elif pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
                lengths = pc.utf8_length(col)
                col_min_len = pc.min(lengths).as_py()
                col_max_len = pc.max(lengths).as_py()
                if s["min_length"] is None or col_min_len < s["min_length"]:
                    s["min_length"] = col_min_len
                if s["max_length"] is None or col_max_len > s["max_length"]:
                    s["max_length"] = col_max_len

    def finalize(self, schema: pa.Schema) -> List[ColumnInfo]:
        infos = []
        for field in schema:
            s = self._stats.get(field.name, {})
            avg = None
            if s.get("count", 0) > 0 and s.get("sum") is not None:
                avg = s["sum"] / s["count"]
            description = field.metadata.get(b"description", b"").decode("utf-8") if field.metadata else ""
            infos.append(ColumnInfo(
                name=field.name,
                type=str(field.type),
                null_count=s.get("null_count", 0),
                non_null_count=(s.get("count", 0) or 0) + (s.get("null_count", 0)),  # approximated
                min=s.get("min"),
                max=s.get("max"),
                sum=s.get("sum"),
                avg=avg,
                min_length=s.get("min_length"),
                max_length=s.get("max_length"),
                description=description,
            ))
        return infos
