# AlphaPlan MCP Server

A Redis-backed generic **result-set** MCP (Model Context Protocol) server. Ingestion
tools fetch *complete* result sets from SQL Server or the Mouser API, store them in
Redis, and return only a compact summary plus an opaque `result_id` — never the rows
themselves. Clients then explore the stored data through generic tools (filter,
paginate, aggregate, join, histogram) that run on demand.

## Core contract

> **Never put full result-set rows into the model context.** Ingestion tools return only a
> summary + `result_id`; row data is always fetched on demand through the generic
> result-set tools.

## Features

- **Streaming ingestion** from SQL Server (customers, parts, part stats, sales prices)
  and the Mouser part-search API, persisted as chunked, LZ4-compressed Arrow IPC in Redis.
- **18 MCP tools** — 5 ingestion + 13 generic result-set tools (incl. `join`).
- **Typed, provenance-aware result sets** — derived sets (from `store_result`) carry
  column origins, structured lineage, and generated descriptions. See
  [`src/result_set_spec.md`](src/result_set_spec.md).
- **Safe querying** — filtering/aggregation use the Polars DSL on stored data, never
  arbitrary SQL; strict row/byte/group/column limits everywhere.
- **TTL lifecycle** — idle TTL extended on access, absolute max lifetime, plus orphan
  chunk cleanup.

## Tech stack

| Concern | Technology |
|---|---|
| MCP framework | FastMCP (>=3,<4), transport = HTTP (streamable) |
| Language | Python >=3.12 |
| Result-set storage | Redis (>=5.0), chunked LZ4 Arrow IPC |
| Typed serialization | PyArrow (>=15) |
| Query engine | Polars (>=0.20) |
| SQL Server access | pyodbc (>=5.3), async via `asyncio.to_thread` |
| External API | Mouser part search over httpx, Redis-cached |
| Validation/conf | pydantic (>=2), pydantic-settings |
| Tests | pytest + pytest-asyncio + fakeredis |

## Quick start (Docker)

```bash
docker compose -f docker/docker-compose.yml up --build
```

- Host port **9001** → container **9000**; Redis 7.2 sidecar (`docker/redis.conf`).
- Env from `docker/.env`; health-gated `depends_on`.
- Health check: `GET /health` → `{"status":"healthy","redis":"ok"}` or 503.

## Configuration

All settings live in `src/alphaplan_mcp/config.py` (`Settings`, pydantic-settings) and are
read from `.env` in the CWD (Docker Compose feeds from `docker/.env`).

| Variable | Default | Purpose |
|---|---|---|
| `DEBUG` | `false` | Enables request logging (dev only) |
| `MCP_HOST` | `0.0.0.0` | Bind host |
| `MCP_PORT` | `9000` | HTTP port |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection URL |
| `REDIS_KEY_PREFIX` | `alphaplan-mcp` | Prefix for all app keys |
| `RESULTSET_CHUNK_ROWS` | `5000` | Rows per stored chunk |
| `RESULTSET_IDLE_TTL_SECONDS` | `3600` | Idle TTL, extended on access |
| `RESULTSET_MAX_TTL_SECONDS` | `86400` | Absolute max lifetime |
| `RESULTSET_MAX_ROWS` | `250000` | Max rows per result set |
| `RESULTSET_MAX_BYTES` | `268435456` | Max uncompressed size per set |
| `RESULTSET_MAX_GROUPS` | `10000` | Max groups in `group_by` |
| `TOOL_MAX_ROWS` | `200` | Cap for row-returning tools |
| `TOOL_MAX_DISTINCT_VALUES` | `200` | Cap for `distinct_values` |
| `TOOL_MAX_FILTER_CLAUSES` | `20` | Cap for `FilterGroup.clauses` |
| `TOOL_MAX_GROUP_COLUMNS` | `3` | Cap for `group_by` columns |
| `TOOL_MAX_AGGREGATIONS` | `10` | Cap for aggregation list |
| `SQLSERVER_HOST` / `_DATABASE` (`ALPHAPLAN_EchtDaten`) / `_USER` / `_PASSWORD` / `_DRIVER` (`ODBC Driver 18 for SQL Server`) / `_TRUST_CERTIFICATE` (`yes`) / `_QUERY_TIMEOUT_SECONDS` (`120`) | — | SQL Server connectivity (ReadOnly, `Encrypt=yes`, autocommit) |
| `MOUSER_API_KEY` | — | Mouser API key (required for `search_mouser_artikel`) |
| `MOUSER_API_BASE_URL` | `https://api.mouser.com/api/v1` | Mouser endpoint |
| `MOUSER_TIMEOUT_SECONDS` | `15.0` | httpx timeout |
| `MOUSER_CACHE_TTL_HOURS` | `168.0` | Default cache TTL (7 days) |
| `MOUSER_RATE_LIMIT_RETRY_SECONDS` | `60.0` | 429 → single retry delay |

## MCP tools

### Ingestion tools (return a `ResultSetSummary`, never rows)

| Tool | Source |
|---|---|
| `list_adressen(such_name)` | SQL Server `Adressen` (LIKE on `SuchName`) + payment/shipping terms, currency, price levels, delivery address |
| `list_artikel(positive_filter_term, negative_filter_term=None, match_code, sample_n, include_archived)` | SQL Server `Artikel` (+ supplier + warehouses) |
| `list_artikel_statistik(artikel_nummer, jahr, einkauf_verkauf, limit)` | SQL Server `StatistikenArtikel` join |
| `list_beleg_positionen(artikel_nummer, beleg_typ, date_from, date_to, include_archived, adress_nummer, open_only, include_text_lines)` | SQL Server `BelegePositionen`/`Belege` (+ counterparty); columns depend on the document type |
| `get_verkaufspreise(artikel_nummern, adress_nummer, output_format)` | AlphaPlan `VerkaufsPreise` (customer price list, or `SENTINEL_WEBSHOP` / `SENTINEL_LUMINOVO`) |
| `search_mouser_artikel(artikel_nummern, caching, cache_ttl_override_hours, output_format, retry_on_rate_limit)` | Mouser API, Redis-cached |

### Generic result-set tools (all require a `result_id`)

`get_result_info`, `explain_columns`, `describe_columns`, `list_rows`, `count_rows`,
`query_rows`, `distinct_values`, `aggregate`, `group_by`, `histogram`, `join`,
`delete_result_set`, `normalize_preview`.

`aggregate`/`group_by`/`distinct_values`/`histogram`/`list_rows`/`query_rows` accept
`store_result=true` to persist the computed table as a new, chainable result set with a
fresh `result_id`. Detailed semantics, column naming, provenance, and the aggregation
vocabulary are specified in [`src/result_set_spec.md`](src/result_set_spec.md).

## Development

The dev environment is a **uv** venv rooted at `src/`:

```bash
cd src
uv sync                    # install from src/pyproject.toml + uv.lock
uv run python -m alphaplan_mcp   # run server (needs Redis + .env + reachable SQL Server)
```

### Tests

```bash
cd src
uv run pytest ../tests/unit                  # all unit tests (use fakeredis, no Redis needed)
uv run pytest ../tests/unit/test_query_engine.py   # single module
```

Unit tests mock Redis with `fakeredis`; `tests/integration/` is currently empty.

## Architecture

```
MCP client
   │  tool call (e.g. list_adressen, list_artikel)
   ▼
tools/*.py  ──►  database.py fetch_*_stream()     # pyodbc, streaming pa.RecordBatch
   ▼
ResultSetStore.store_result_stream()  ──►  Redis chunk keys + meta + session index
   ▼
Redis keys:  {prefix}:result:{rs_id}:chunk:000000   (LZ4 Arrow IPC, EX = max_ttl)
             {prefix}:result:{rs_id}:meta           (ResultSetMetadata JSON)
             {prefix}:session:{session}:results     (SET of that session's rs_ ids)
   ▼
client receives ResultSetSummary { result_id, row_count, columns, expires_at, … }
   ▼  (later calls)
tools/result_sets.py  ──►  QueryEngine (Polars over the stored table)
   ▼
filtered/aggregated rows or a new derived result set
```

## Repository layout

```text
.
├── pyproject.toml                # root packaging file (used by the Docker build)
├── README.md                     # this file (canonical user-facing docs)
├── src/
│   ├── pyproject.toml            # dev/uv project (pytest etc.)
│   ├── uv.lock
│   ├── README.md                 # pointer to the root README
│   ├── result_set_spec.md        # design spec: provenance & column semantics
│   ├── main.py                   # LEGACY standalone server (do not extend)
│   └── alphaplan_mcp/
│       ├── __main__.py / server.py / config.py / database.py
│       ├── result_store.py / serialization.py / query_engine.py
│       ├── models.py / errors.py / utils.py
│       ├── tools/  (customers, parts, prices, mouser, result_sets)
│       └── apis/mouser/  (api, cache, parts_extraction)
├── tests/ (unit/ + integration/)
├── docker/ (Dockerfile, docker-compose.yml, redis.conf)
└── AGENTS.md                     # guidance for coding agents
```

## Security

- The MCP endpoint is **unauthenticated by default** and serves potentially sensitive
  customer, pricing, and part statistics — do not expose it publicly without an auth layer.
- DB connections are `ReadOnly` + `Encrypt=yes`; user input never forms SQL text (Polars
  DSL on materialized data, parameterized `?` DB predicates).
- Restrict access to Redis; never log or return secrets or connection strings.
