# Implementation plan: Redis-backed generic result-set MCP server

Implement the supplied rudimentary FastMCP server as a production-oriented MCP service that:

1. Runs together with Redis through Docker Compose.
2. Uses Redis for FastMCP session-state persistence.
3. Fetches complete database result sets without returning all rows to the model.
4. Stores result sets under opaque string identifiers.
5. Exposes generic tools for exploring and aggregating any stored tabular result set.
6. Prevents result-set access across MCP sessions.
7. Automatically removes old result sets.
8. Keeps all Docker-related files under `docker/` and all runtime Python source under `src/`.

Do not add arbitrary SQL execution. Generic querying must operate only on already-materialized result sets.

---

## 1. Technical direction

Use the following stack:

* Python 3.12 or another explicitly pinned supported Python version.
* FastMCP `>=3,<4`.
* `pyodbc`.
* `py-key-value-aio[redis]` for FastMCP’s `session_state_store`.
* `redis` asyncio client for application-owned result-set storage.
* PyArrow for typed, portable result-set serialization.
* Polars for safe filtering, grouping, sorting, and aggregation without constructing arbitrary SQL.
* Pydantic and `pydantic-settings` for configuration and tool argument models.
* Pytest and `pytest-asyncio` for tests.

Do not store complete row sets through `ctx.set_state()`. FastMCP session state should contain only small session metadata such as known result IDs. Store row data separately in Redis using an application-owned `ResultSetStore`.

Make all MCP tools asynchronous. Since `pyodbc` is blocking, run database work through `asyncio.to_thread()`. Create and close each ODBC connection inside the worker thread rather than sharing connections between threads.

---

## 2. Repository layout

Create approximately this structure:

```text
.
├── docker/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── redis.conf
├── src/
│   └── alphaplan_mcp/
│       ├── __init__.py
│       ├── __main__.py
│       ├── server.py
│       ├── config.py
│       ├── database.py
│       ├── errors.py
│       ├── models.py
│       ├── serialization.py
│       ├── result_store.py
│       ├── query_engine.py
│       └── tools/
│           ├── __init__.py
│           ├── customers.py
│           └── result_sets.py
├── tests/
│   ├── unit/
│   └── integration/
├── pyproject.toml
├── .env.example
├── .gitignore
└── README.md
```

Keep MCP registration in `server.py`, database-specific loading in `tools/customers.py`, and all generic tabular operations in `tools/result_sets.py`.

The query engine and result store must not depend on the customer table. Future database tools should be able to call the same result-set ingestion API.

---

## 3. Configuration

Create a typed `Settings` class. Support at least:

```text
MCP_HOST=0.0.0.0
MCP_PORT=9000

REDIS_URL=redis://redis:6379/0
REDIS_KEY_PREFIX=alphaplan-mcp

RESULTSET_CHUNK_ROWS=5000
RESULTSET_IDLE_TTL_SECONDS=3600
RESULTSET_MAX_TTL_SECONDS=86400
RESULTSET_MAX_ROWS=250000
RESULTSET_MAX_BYTES=268435456
RESULTSET_MAX_GROUPS=10000

TOOL_MAX_ROWS=200
TOOL_MAX_DISTINCT_VALUES=200
TOOL_MAX_FILTER_CLAUSES=20
TOOL_MAX_GROUP_COLUMNS=3
TOOL_MAX_AGGREGATIONS=10

SQLSERVER_HOST=
SQLSERVER_DATABASE=ApiPreise
SQLSERVER_USER=
SQLSERVER_PASSWORD=
SQLSERVER_DRIVER=ODBC Driver 18 for SQL Server
SQLSERVER_TRUST_CERTIFICATE=yes
SQLSERVER_QUERY_TIMEOUT_SECONDS=120
```

Treat row and byte limits as safety limits, not pagination. A source query must either store its complete result or fail explicitly. Never silently truncate and call the result complete.

Reject invalid configuration at startup.

Do not call `load_dotenv()` inside every database connection. Load settings once at application startup.

---

## 4. Docker implementation

### Dockerfile

Create a slim Linux Python image that:

1. Installs Microsoft ODBC Driver 18 for SQL Server using Microsoft’s supported package repository.
2. Installs required `unixODBC` build/runtime packages.
3. Installs the Python project from `pyproject.toml`.
4. Runs as a non-root user.
5. Copies only required files.
6. Exposes port `9000`.
7. Starts the server using:

```text
python -m alphaplan_mcp
```

Pin important image and package versions where practical. Do not use an unpinned `latest` base image.

### Docker Compose

Create two services:

#### `mcp`

* Build from `docker/Dockerfile`.
* Build context must be the repository root.
* Load runtime variables from `.env`.
* Publish `9000:9000`.
* Depend on Redis becoming healthy.
* Restart unless stopped.
* Use an application health endpoint.
* Do not run multiple Uvicorn/FastMCP workers in this initial implementation.

#### `redis`

* Use an explicitly pinned official Redis image.
* Enable append-only persistence.
* Mount a named volume.
* Use `redis.conf`.
* Add a `redis-cli ping` health check.
* Do not expose Redis publicly unless needed for local debugging.
* Place both services on a private Compose network.

Create a named `redis-data` volume.

Add a FastMCP custom HTTP route such as `/health` that checks:

* The Python process is alive.
* Redis responds to `PING`.

Do not make health checks query SQL Server, because database maintenance should not continuously restart the MCP service.

---

## 5. FastMCP construction and Redis clients

Construct two Redis abstractions from the same `REDIS_URL`:

### FastMCP state backend

Create a `RedisStore` and pass it as:

```python
FastMCP(
    "AlphaPlanMCP",
    session_state_store=fastmcp_redis_store,
)
```

Use this only for small FastMCP-managed session values.

### Application result-set client

Create one shared `redis.asyncio.Redis` client for binary result storage.

Initialize it during the server lifespan and close it during shutdown. Expose the client and `ResultSetStore` through lifespan context or dependency injection.

Use separate key prefixes for:

```text
alphaplan-mcp:fastmcp:...
alphaplan-mcp:result:...
alphaplan-mcp:session:...
```

Where possible, wrap or configure FastMCP’s store with its own namespace so application keys cannot collide with FastMCP internals.

---

## 6. Result-set representation

Define an opaque result identifier such as:

```text
rs_<UUIDv4>
```

Do not derive it from table names, customer names, query arguments, or session IDs.

Every result set must have metadata containing at least:

```json
{
  "result_id": "rs_...",
  "owner_session_id": "...",
  "source": "customers",
  "created_at": "...",
  "last_accessed_at": "...",
  "absolute_expires_at": "...",
  "row_count": 123,
  "column_count": 12,
  "uncompressed_size_bytes": 12345,
  "stored_size_bytes": 6789,
  "chunk_count": 1,
  "schema": [],
  "summary_statistics": {},
  "source_parameters": {
    "customer_name": "Acme%"
  }
}
```

Never return `owner_session_id` to the MCP client.

### Redis key layout

Use chunked storage:

```text
{prefix}:result:{result_id}:meta
{prefix}:result:{result_id}:chunk:000000
{prefix}:result:{result_id}:chunk:000001
{prefix}:session:{session_id}:results
```

Store each chunk as Arrow IPC bytes with compression. Each chunk should contain approximately `RESULTSET_CHUNK_ROWS` rows.

Metadata must include each chunk’s starting row offset and row count so that `list_rows` can load only the chunks intersecting the requested range.

Use Redis pipelines when writing chunks and applying expiration.

Write result sets transactionally enough that readers never see incomplete data:

1. Generate the result ID.
2. Write chunks with a short staging TTL.
3. Write final metadata only after all chunks succeed.
4. Change all keys to their normal expiration.
5. Add the result ID to the session index.
6. On failure, delete all keys already written.

Treat the metadata key as the commit marker. A result does not exist unless valid metadata exists.

---

## 7. Type handling

Preserve database types wherever reasonably possible.

Support at least:

* `None`
* Boolean
* Integer
* Floating point
* `decimal.Decimal`
* String
* Bytes
* `date`
* `datetime`
* `time`
* UUID-like values

Convert pyodbc rows into Arrow record batches based on `cursor.description` and observed values.

For MCP JSON responses:

* Serialize dates and datetimes as ISO 8601 strings.
* Serialize decimals as strings when returning them as row values to avoid precision loss.
* Return `null` for null values.
* Reject columns whose values cannot be represented safely rather than calling `str()` on arbitrary objects without warning.

Keep one canonical column-name mapping and reject duplicate column names at ingestion.

---

## 8. Customer loader

Replace the current paginated `list_customers` tool.

Use this public signature conceptually:

```python
async def list_customers(
    customer_name: str | None = None,
    ctx: Context = ...
) -> ResultSetSummary
```

Remove `limit` and `offset`.

The SQL query should return the same selected columns, but remove `OFFSET` and `FETCH NEXT`.

Continue parameterizing `customer_name`. Use `%` when no filter is supplied.

Fetch rows incrementally with `cursor.fetchmany(RESULTSET_CHUNK_ROWS)` rather than `fetchall()`.

As chunks are fetched:

1. Enforce `RESULTSET_MAX_ROWS`.
2. Track approximate uncompressed and stored byte sizes.
3. Abort and clean up if `RESULTSET_MAX_BYTES` is exceeded.
4. Calculate compact summary statistics.
5. Persist chunks immediately rather than holding the complete result twice in memory.

Set a database query timeout.

Because the operation is read-only:

* Use `autocommit=True`, or explicitly roll back before closing.
* Consider adding `ApplicationIntent=ReadOnly`.
* Explicitly configure encryption and certificate behavior.

Return only a compact result summary:

```json
{
  "result_id": "rs_...",
  "source": "customers",
  "row_count": 12345,
  "column_count": 12,
  "stored_size_bytes": 987654,
  "created_at": "...",
  "expires_at": "...",
  "columns": [
    {
      "name": "adress_nummer",
      "type": "integer",
      "null_count": 0
    }
  ]
}
```

Do not include customer rows or automatic sample values in this response.

Do not silently truncate. When a limit is exceeded, return a clear structured error such as:

```json
{
  "error": "RESULT_SET_TOO_LARGE",
  "message": "The query exceeded RESULTSET_MAX_ROWS and was not stored."
}
```

---

## 9. Session ownership

Every result tool must accept `ctx: Context` and compare:

```text
metadata.owner_session_id == ctx.session_id
```

For a nonexistent, expired, or foreign result ID, return the same generic `RESULT_NOT_FOUND` error. Do not reveal whether a result belongs to another session.

Store a small list or set of result IDs in the FastMCP session state only when useful, for example to implement `list_result_sets`.

Do not trust a caller-supplied session ID.

---

## 10. Generic tool models

Define reusable Pydantic models.

### Filtering

Use a structured filter model, not a text expression:

```json
{
  "combine": "all",
  "clauses": [
    {
      "column": "branche",
      "operator": "eq",
      "value": "Retail%"
    },
    {
      "column": "adress_nummer",
      "operator": "gte",
      "value": 1000
    }
  ]
}
```

Support:

```text
eq
ne
gt
gte
lt
lte
in
not_in
between
is_null
is_not_null
contains
starts_with
ends_with
like
not_like
```

Required wildcard behavior:

* For string `eq` and `ne`, if the supplied value contains unescaped `%` or `_`, apply SQL-LIKE-style matching.
* `%` means zero or more characters.
* `_` means one character.
* `\` escapes a wildcard.
* Also expose explicit `like` and `not_like`, because implicit wildcard behavior should not be the only interface.
* Default string matching to case-insensitive unless `case_sensitive=true`.

Validate operators against column types before executing.

Limits:

* Maximum filter clauses from configuration.
* Maximum 100 values in `in` or `not_in`.
* No regular expressions in the first implementation.
* No arbitrary Python, SQL, or Polars expressions.

### Sorting

Use:

```json
[
  {"column": "name", "direction": "asc", "nulls": "last"}
]
```

Cap sort columns at three.

### Aggregations

Use:

```json
[
  {
    "column": "umsatz",
    "operation": "sum",
    "alias": "total_umsatz"
  }
]
```

Support:

```text
count
count_non_null
count_distinct
sum
avg
min
max
```

`count` may use no column. Reject `sum` and `avg` for non-numeric columns.

---

## 11. Required generic MCP tools

### `get_result_info`

Arguments:

```text
result_id
```

Returns compact metadata, schema, row count, creation time, expiry time, and source. Never returns rows.

### `list_result_sets`

No result ID argument.

Returns active result sets owned by the current session with compact metadata. Remove stale result IDs from the session index lazily.

### `explain_columns`

Arguments:

```text
result_id
columns: optional list
include_examples: false by default
```

Return per-column:

* Name.
* Logical type.
* Null count.
* Non-null count.
* Numeric/date minimum and maximum.
* Numeric sum and average when meaningful.
* String minimum and maximum length.
* Exact distinct count only when it can be computed within configured limits.
* Up to three examples only when explicitly requested.

Do not include examples by default.

### `list_rows`

Arguments:

```text
result_id
columns: optional list
limit: default 50
offset: default 0
sort: optional
```

Cap `limit` using `TOOL_MAX_ROWS`.

Return:

```json
{
  "result_id": "rs_...",
  "offset": 0,
  "limit": 50,
  "returned": 50,
  "total_rows": 1000,
  "has_more": true,
  "next_offset": 50,
  "rows": []
}
```

When no sorting is requested, preserve original source order.

For unsorted requests, load only chunks intersecting the requested offset range.

### `query_rows`

Arguments:

```text
result_id
filters
columns: optional
sort: optional
limit
offset
```

Apply structured filters, sorting, projection, limit, and offset.

Return matching row count when it can be obtained without a second complete scan. Otherwise return `matching_rows: null` and `has_more` based on reading one extra row.

Never return more than the configured maximum.

### `count_rows`

Arguments:

```text
result_id
filters: optional
```

Return the exact number of matching rows without returning row data.

### `distinct_values`

Arguments:

```text
result_id
column
filters: optional
limit: default 50
offset: default 0
include_counts: true
order: count_desc | value_asc | value_desc
```

Return at most the configured maximum number of values.

Return:

* Values.
* Optional counts.
* Number returned.
* Whether more values exist.

Treat null as a separate value only when `include_null=true`.

### `aggregate`

Arguments:

```text
result_id
aggregations
filters: optional
```

Return one row containing the requested aggregates.

Implement averages using `sum` and non-null count so chunked processing remains numerically correct.

### `group_by`

Arguments:

```text
result_id
group_columns
aggregations
filters: optional
sort: optional
limit: default 50
offset: default 0
```

Support one to three grouping columns.

Always support `count`, including group-by requests without additional numeric aggregations.

Enforce `RESULTSET_MAX_GROUPS`. If group cardinality exceeds the limit, return a clear error rather than exhausting memory.

Return `has_more` and `next_offset`.

### `histogram`

Arguments:

```text
result_id
column
bins: default 10, maximum 50
filters: optional
```

Support numeric and date/datetime columns.

Return bin boundaries and counts. Ignore nulls but report `null_count`.

### `delete_result_set`

Arguments:

```text
result_id
```

Delete metadata, all chunks, and the session-index entry.

Deletion must be idempotent.

Return only whether an owned active result was deleted.

---

## 12. Query engine design

The generic tools must use a shared `QueryEngine`, not duplicate filter logic in each tool.

Responsibilities:

* Load and validate result metadata.
* Resolve requested columns.
* Translate structured filters into Polars expressions.
* Apply wildcard matching safely.
* Normalize result values for JSON.
* Process chunks incrementally where possible.
* Combine partial aggregations correctly.
* Enforce row, grouping, sorting, and cardinality limits.
* Produce consistent errors.

Do not interpolate column names into SQL. All operations happen against Arrow/Polars data loaded from Redis.

For operations that require all matching data to be sorted globally, either:

1. Load the needed columns from all chunks and enforce a configurable working-memory limit, or
2. Return `OPERATION_TOO_LARGE`.

Do not risk an uncontrolled container OOM merely to satisfy a sort.

---

## 13. Result expiry and cleanup

Use Redis TTL as the primary cleanup mechanism.

Each result set has:

```text
idle TTL
absolute maximum lifetime
```

Recommended defaults:

```text
idle TTL: 1 hour
absolute maximum lifetime: 24 hours
```

On successful access:

1. Update `last_accessed_at`.
2. Extend Redis expiry by the idle TTL.
3. Never extend beyond `absolute_expires_at`.

Apply the same absolute expiry to metadata and every chunk.

Partial or orphaned chunks must also have TTLs, so cleanup does not depend on a background worker.

Add explicit deletion through `delete_result_set`.

Maintain the session result index with its own TTL. Stale entries are removed lazily by `list_result_sets`.

Optionally add a low-frequency cleanup task during the server lifespan that:

* Uses Redis `SCAN`, never `KEYS`.
* Cleans stale session-index entries.
* Removes metadata whose chunks are irrecoverably incomplete.
* Stops cleanly during server shutdown.

The service must remain correct even when that cleanup task never runs. Native TTL is the correctness mechanism; the sweeper is housekeeping only.

When Redis evicts or expires a result between two calls, return `RESULT_NOT_FOUND` cleanly.

---

## 14. Error model

Create stable machine-readable errors:

```text
INVALID_ARGUMENT
RESULT_NOT_FOUND
RESULT_SET_TOO_LARGE
UNKNOWN_COLUMN
INVALID_OPERATOR_FOR_COLUMN
INVALID_AGGREGATION
TOO_MANY_GROUPS
OPERATION_TOO_LARGE
DATABASE_UNAVAILABLE
DATABASE_QUERY_TIMEOUT
REDIS_UNAVAILABLE
INTERNAL_ERROR
```

Tool errors should include:

```json
{
  "error": "UNKNOWN_COLUMN",
  "message": "Column 'foo' does not exist.",
  "details": {
    "available_columns": ["bar", "baz"]
  }
}
```

Do not include stack traces, connection strings, SQL credentials, Redis URLs containing passwords, or raw internal exceptions in MCP responses.

Log full exceptions server-side with a request ID, but redact secrets.

---

## 15. SQL Server connection improvements

Refactor `build_connection()` into `database.py`.

Requirements:

* Consume the application settings object.
* Explicitly set connection timeout and query timeout.
* Use SQL authentication in the documented Docker example.
* Preserve optional trusted authentication, but document that Linux containers require a working Kerberos setup.
* Explicitly include encryption behavior.
* Use a fixed database and fixed query; do not accept table or SQL identifiers from MCP arguments.
* Close cursor and connection deterministically.
* Do not keep a global pyodbc connection.
* Do not retry arbitrary SQL queries indefinitely.

A small bounded retry may be added only for transient connection establishment errors.

---

## 16. Observability

Add structured logging for:

* Tool name.
* MCP request ID.
* Source name.
* Row count.
* Fetch duration.
* Serialization duration.
* Redis write duration.
* Query-engine duration.
* Result size.
* Error code.

Do not log returned rows.

Add `/health` and optionally `/ready`:

* `/health`: process and Redis connectivity.
* `/ready`: Redis plus a lightweight configuration sanity check.
* Avoid querying the customer table in routine probes.

---

## 17. Tests

Use FastMCP’s in-memory client for most MCP-level tests. FastMCP officially supports passing the server directly to a client for deterministic in-process testing.

### Unit tests

Cover:

* Result ID generation.
* Metadata serialization.
* Arrow serialization round trips.
* Decimal and datetime preservation.
* Every filter operator.
* `%`, `_`, and escaped wildcard semantics.
* Null filtering.
* Case-sensitive and insensitive matching.
* Projection.
* Offset and limit.
* Numeric aggregation.
* Average across multiple chunks.
* Group-by across multiple chunks.
* Distinct values and counts.
* Histogram boundaries.
* Unknown columns.
* Invalid type/operator combinations.
* Max-group enforcement.
* JSON normalization.

### Result-store integration tests

Use a real disposable Redis container and verify:

* Chunk writes and reads.
* Atomic publication.
* Failed-write cleanup.
* TTL assignment.
* Sliding idle expiry.
* Absolute maximum expiry.
* Session ownership.
* Cross-session denial.
* Explicit deletion.
* Expired metadata.
* Missing chunks.
* Stale session-index cleanup.

### MCP tests

Using two separate FastMCP clients/sessions, verify:

1. Client A creates a result.
2. Client A can inspect it.
3. Client B receives `RESULT_NOT_FOUND`.
4. `list_customers` returns metadata but no rows.
5. `list_rows` respects caps.
6. Generic tools share consistent schemas and error responses.
7. Deleting a result makes subsequent calls fail cleanly.

### Database tests

Do not require a production SQL Server for normal unit tests.

Abstract row fetching behind a `CustomerRepository` or loader interface and provide fake record batches.

Add optional SQL Server integration tests gated by environment variables.

### Docker test

Add a test procedure that runs:

```text
docker compose -f docker/docker-compose.yml up --build
```

Then verifies:

* Redis becomes healthy.
* MCP becomes healthy.
* The MCP endpoint is available.
* Redis data survives an MCP-container restart.
* Expired result sets disappear.

---

## 18. Documentation

Write a README containing:

* Architecture overview.
* Repository layout.
* Required environment variables.
* SQL authentication example.
* Kerberos caveat for trusted authentication.
* Docker Compose commands.
* MCP endpoint URL.
* Result-set lifecycle.
* Tool descriptions and small request/response examples.
* Configurable safety limits.
* Cleanup behavior.
* Troubleshooting for ODBC Driver 18 and Redis.
* Warning that result sets may contain sensitive customer data.
* Warning not to expose the unauthenticated MCP endpoint publicly.

Add `.env.example` with placeholders only.

Do not commit `.env`, Redis data, database credentials, Arrow dumps, or generated result data.

---

## 19. Implementation order

Complete the work in these phases:

### Phase 1: foundation

* Create project packaging.
* Create settings.
* Install ODBC dependencies.
* Create Compose services.
* Add Redis and health checks.
* Confirm a minimal MCP server starts.

### Phase 2: storage

* Implement serialization.
* Implement result metadata.
* Implement chunked Redis storage.
* Implement ownership and TTL.
* Add result-store tests.

### Phase 3: customer ingestion

* Refactor SQL access.
* Stream all matching customers.
* Enforce complete-or-fail safety limits.
* Return only result metadata and statistics.

### Phase 4: generic tools

Implement in this order:

1. `get_result_info`
2. `list_result_sets`
3. `explain_columns`
4. `list_rows`
5. `count_rows`
6. `query_rows`
7. `distinct_values`
8. `aggregate`
9. `group_by`
10. `histogram`
11. `delete_result_set`

### Phase 5: hardening

* Structured error responses.
* Logging.
* Memory/work limits.
* Missing-chunk handling.
* Cleanup task.
* Docker restart tests.
* Documentation.

---

## 20. Acceptance criteria

The implementation is complete only when all of the following are true:

1. `docker compose` starts both services successfully.
2. Redis data persists across MCP-container restarts.
3. FastMCP uses a Redis-backed `session_state_store`.
4. `list_customers` has no pagination parameters.
5. `list_customers` fetches and stores the complete matching result or fails without retaining a partial result.
6. `list_customers` returns no customer rows.
7. Every result receives an opaque string ID.
8. Result IDs are scoped to the creating MCP session.
9. Another session cannot access a result even when it knows the ID.
10. Generic tools do not contain customer-specific column names.
11. No generic tool executes arbitrary SQL or evaluates arbitrary expressions.
12. Row-returning tools enforce strict output limits.
13. Wildcard, null, comparison, grouping, sum, and average behavior are covered by tests.
14. Expired result sets are removed by Redis TTL.
15. Explicit result deletion works.
16. Partial writes do not produce readable result sets.
17. Oversized result sets fail explicitly and are not silently truncated.
18. Database and Redis credentials never appear in logs or MCP error responses.
19. The source is modular enough that a future tool can materialize a different SQL query through the same `ResultSetStore`.
20. All tests and Docker health checks pass.
