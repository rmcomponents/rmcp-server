# AlphaPlan MCP Server

A Redis-backed generic result-set MCP (Model Context Protocol) server that fetches complete database result sets without returning all rows to the model. It stores result sets in Redis under opaque identifiers and exposes generic tools for exploring and aggregating the data safely.

## Architecture Overview

The server is built using FastMCP and Python 3.12. It uses:
- **Redis**: For FastMCP session-state persistence and application-owned result-set storage.
- **PyArrow**: For typed, portable result-set serialization.
- **Polars**: For safe filtering, grouping, sorting, and aggregation without constructing arbitrary SQL.
- **pyodbc**: For connecting to SQL Server (run asynchronously via `asyncio.to_thread`).

Result sets are stored in chunks in Redis with metadata. Clients receive an opaque result ID and can use generic tools to query, aggregate, or list rows within the stored result set, strictly scoped to the session that created it.

## Repository Layout

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

## Setup

### Prerequisites
- Docker and Docker Compose
- SQL Server database access (for the customer tools)

### Environment Variables
Copy `.env.example` to `.env` in the repository root and fill in the values:

```env
MCP_HOST=0.0.0.0
MCP_PORT=9000

REDIS_URL=redis://redis:6379/0
REDIS_KEY_PREFIX=alphaplan-mcp

# Result Set Limits
RESULTSET_CHUNK_ROWS=5000
RESULTSET_IDLE_TTL_SECONDS=3600
RESULTSET_MAX_TTL_SECONDS=86400
RESULTSET_MAX_ROWS=250000
RESULTSET_MAX_BYTES=268435456
RESULTSET_MAX_GROUPS=10000

# Tool Limits
TOOL_MAX_ROWS=200
TOOL_MAX_DISTINCT_VALUES=200
TOOL_MAX_FILTER_CLAUSES=20
TOOL_MAX_GROUP_COLUMNS=3
TOOL_MAX_AGGREGATIONS=10

# SQL Server Configuration
SQLSERVER_HOST=
SQLSERVER_DATABASE=ApiPreise
SQLSERVER_USER=
SQLSERVER_PASSWORD=
SQLSERVER_DRIVER=ODBC Driver 18 for SQL Server
SQLSERVER_TRUST_CERTIFICATE=yes
SQLSERVER_QUERY_TIMEOUT_SECONDS=120
```

### SQL Authentication Example
The Docker setup uses SQL authentication by default. Ensure `SQLSERVER_USER` and `SQLSERVER_PASSWORD` are set in your `.env` file.

### Kerberos Caveat
Trusted authentication (`Trusted_Connection=yes`) is supported but requires a working Kerberos setup in Linux containers, which is out of scope for the default Docker configuration.

## Running the Server

To start the server and Redis using Docker Compose:

```bash
docker compose -f docker/docker-compose.yml up --build
```

The MCP endpoint will be available at `http://localhost:9000`.

## Running Tests

The project uses `pytest` and `pytest-asyncio` for testing. Unit tests mock Redis using `fakeredis`, while integration tests may require a running Redis instance.

To run the tests, navigate to the `src` directory and execute:

```bash
# If using uv
uv run pytest

# Or using standard pytest within your virtual environment
pytest
```

To run only the unit tests:

```bash
pytest tests/unit
```

To run only the integration tests (ensure Redis is running if they don't use fakeredis):

```bash
pytest tests/integration
```

## Result-Set Lifecycle

1. A tool like `list_customers` fetches data from the database and stores it in Redis as chunked Arrow IPC bytes.
2. The client receives a compact summary and an opaque `result_id` (e.g., `rs_...`).
3. The client uses generic tools (`list_rows`, `query_rows`, `aggregate`, etc.) with the `result_id` to inspect the data.
4. Result sets have an idle TTL (default 1 hour) and an absolute maximum lifetime (default 24 hours). Accessing a result extends its idle TTL.
5. Result sets are scoped to the MCP session that created them. Other sessions cannot access them.
6. Explicit deletion is available via the `delete_result_set` tool.

## Configurable Safety Limits

The server enforces strict limits to prevent memory exhaustion and unbounded responses:
- `RESULTSET_MAX_ROWS`: Maximum rows allowed in a single result set.
- `RESULTSET_MAX_BYTES`: Maximum uncompressed size of a result set.
- `TOOL_MAX_ROWS`: Maximum rows returned by row-returning tools.
- `RESULTSET_MAX_GROUPS`: Maximum unique groups allowed in `group_by` operations.

## Cleanup Behavior

Redis TTL is the primary cleanup mechanism. A low-frequency background sweeper may run to clean up stale session indexes, but the service remains correct even if the sweeper never runs.

## Troubleshooting

### ODBC Driver 18
The Dockerfile installs Microsoft ODBC Driver 18. If you encounter certificate errors, ensure `SQLSERVER_TRUST_CERTIFICATE=yes` is set in your `.env` file.

### Redis
If Redis fails to start, ensure the `redis-data` volume is not corrupted. You can remove the volume with `docker volume rm <project>_redis-data` (note: this deletes persisted data).

## Security Warnings

- **Sensitive Data**: Result sets may contain sensitive customer data. Ensure access to the MCP endpoint and Redis is restricted.
- **Unauthenticated Endpoint**: The MCP endpoint is unauthenticated by default. Do not expose it publicly without an authentication layer.

