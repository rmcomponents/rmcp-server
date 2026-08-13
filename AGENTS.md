# AGENTS.md — AlphaPlan MCP Server

Guidance for coding agents working in this repository. Read this before making changes.

---

## 1. Project Overview

AlphaPlan MCP is a **Redis-backed generic result-set MCP (Model Context Protocol) server**.
It exposes tools that fetch *complete* result sets from SQL Server or the Mouser API, then
stores them in Redis instead of returning all rows to the model. Clients receive an opaque
`result_id` (e.g. `rs_...`) plus a compact summary, and then use **generic tools** to
filter, paginate, count, aggregate, group, and histogram the stored data.

### Core contract

> **Never put full result-set rows into the model context.** Ingestion tools return only a
> summary + `result_id`; row data is always fetched on demand through the generic
> result-set tools, strictly scoped to the MCP session that created it.

### Tech stack

| Concern | Technology |
|---|---|
| MCP framework | FastMCP (`>=3,<4`), transport = HTTP (streamable) |
| Language | Python `>=3.12` |
| Result-set storage | Redis (`>=5.0`), chunked LZ4-compressed Arrow IPC |
| Typed serialization | PyArrow (`>=15`) |
| Query engine | Polars (`>=0.20`) — filtering/grouping/aggregation, **no arbitrary SQL** |
| SQL Server access | pyodbc (`>=5.3`), run asynchronously via `asyncio.to_thread`/executor |
| External API | Mouser part search over httpx, Redis-cached |
| Validation/conf | pydantic (`>=2`), pydantic-settings |
| Tests | pytest + pytest-asyncio + fakeredis |

---

## 2. Main Entry Points

| # | Entry point | Command | Notes |
|---|---|---|---|
| 1 | **Console script** | `alphaplan-mcp` | Declared in `[project.scripts]` of both `pyproject.toml` files → `alphaplan_mcp.__main__:main` → `alphaplan_mcp.server:main()` |
| 2 | **Module run** | `python -m alphaplan_mcp` | Same path as above; this is the Docker `CMD` |
| 3 | **App assembly / wiring** | `src/alphaplan_mcp/server.py` | Builds the `FastMCP("AlphaPlanMCP")` instance, lifespan, Redis client, `ResultSetStore`, Mouser cache, cleanup loop, registers all 17 tools, adds the `/health` route and DEBUG middleware |
| 4 | **Config load** | `src/alphaplan_mcp/config.py` | `settings = Settings()` (pydantic-settings, env file `.env`) — imported everywhere; this is the single source of configuration truth |
| 5 | **Docker deployment** | `docker compose -f docker/docker-compose.yml up --build` | Host port **9001** → container **9000**; Redis 7.2 sidecar (`docker/redis.conf`); env from `docker/.env`; health-gated `depends_on` |
| 6 | **Legacy entry (do not extend)** | `src/main.py` | Pre-refactor standalone server with a single `list_customers` tool and its own pyodbc connection building. Not packaged, not registered in `server.py`. Left in tree; treat as reference only |
| 7 | **Tests** | pytest, rootdir = repo root | 319 unit tests in `tests/unit`. Venv lives at `src/.venv`. See §8 for the exact invocation that is known to work |
| 8 | **Health check** | `GET /health` | Custom route in `server.py`; returns `{"status": "healthy", "redis": "ok"}` or 503 when Redis is unreachable |

The canonical boot path is:

```
alphaplan-mcp            (console script, from pyproject [project.scripts])
  └─ alphaplan_mcp/__main__.py   → calls server.main()
       └─ alphaplan_mcp/server.py → main() → mcp.run(transport="http", host=settings.MCP_HOST, port=settings.MCP_PORT)
```

---

## 3. Repository Layout

Actual structure (this is **current**; `src/README.md` is a pointer to the root `README.md`).

```text
.
├── AGENTS.md                     # this file
├── README.md                     # canonical user-facing docs (root pyproject readme)
├── pyproject.toml                # ROOT packaging file, used by the Docker build
├── src/
│   ├── pyproject.toml            # DEV project file (uv) — see §3 note below
│   ├── uv.lock                   # uv lockfile for the dev environment
│   ├── .venv/                    # local Python 3.12 virtualenv (uv-managed)
│   ├── README.md                 # pointer to the root README.md
│   ├── result_set_spec.md        # Design spec: provenance & column semantics (store_result chaining contract)
│   ├── main.py                   # LEGACY standalone MCP server (reference only)
│   ├── pytest-tests-unit.out     # captured pytest output artifact
│   └── alphaplan_mcp/
│       ├── __init__.py
│       ├── __main__.py           # entry: from .server import main; main()
│       ├── server.py             # FastMCP app, lifespan, tool registration, /health
│       ├── config.py             # Settings (pydantic-settings); env-driven
│       ├── database.py           # pyodbc connections + streaming Arrow-batch fetchers
│       ├── result_store.py       # ResultSetStore: chunked Redis storage, TTLs, session index
│       ├── serialization.py      # Arrow IPC (LZ4) <-> bytes; metadata JSON <-> bytes
│       ├── query_engine.py       # QueryEngine: Polars filter/sort/agg over stored sets
│       ├── models.py             # Pydantic models (FilterGroup, SortSpec, AggregationSpec, …)
│       ├── errors.py             # ToolError codes + safe error wrapping
│       ├── utils.py              # single normalize_mpn() (Mouser + join)
│       ├── tools/
│       │   ├── __init__.py
│       │   ├── customers.py      # list_customers
│       │   ├── parts.py          # list_parts, list_part_stats
│       │   ├── documents.py      # list_line_items
│       │   ├── webshop.py        # get_webshop_prices
│       │   ├── mouser.py         # search_mouser_parts
│       │   └── result_sets.py    # 11 generic result-set tools (incl. join)
│       └── apis/
│           └── mouser/
│               ├── api.py                # httpx Mouser client (search_by_part_number)
│               ├── cache.py              # RedisMouserCache, NullMouserCache, default_cache
│               ├── parts_extraction.py   # extract_purchasing_parts(raw_response)
│               └── example-response.json # sample Mouser payload (for tests/dev)
├── tests/
│   ├── conftest.py               # pytest_asyncio plugin + fake_redis fixture (fakeredis)
│   ├── unit/                     # 10 test modules, 319 tests (mock Redis via fakeredis)
│   └── integration/              # EMPTY — no integration tests yet
├── docker/
│   ├── Dockerfile                # python:3.12-slim + ODBC Driver 18; runs `python -m alphaplan_mcp`
│   ├── docker-compose.yml        # mcp + redis services
│   ├── redis.conf                # Redis config (persistence enabled)
│   └── out                       # a FILE (build log), not a directory
├── agent/                        # EMPTY directory
├── .gitignore                    # ignores .env, __pycache__, .venv, .aider*, redis data, …
├── .aider.chat.history.md        # aider artifacts (gitignored, not source)
├── .aider.input.history
└── .aider.tags.cache.v4/
```

### ⚠️ Dual `pyproject.toml` layout

This is the single most confusing thing in the repo:

- **Root `pyproject.toml`** — the *packaging* metadata. The Docker build (`docker/Dockerfile`)
  copies it into the image, plus the root `README.md` (the project references
  `readme = "README.md"`). Deps in root do
  **not** include pytest/fakeredis (they are in `[project.optional-dependencies] test`).
- **`src/pyproject.toml`** — the *dev/uv* project. It lists `pytest`, `pytest-asyncio`,
  `fakeredis` as top-level deps and is what `src/uv.lock` + `src/.venv` were built from.

Consequence: when you add a dependency, decide which file(s) need it. For tests you'll want it
in the src/dev file (or the root `test` extra); for the runtime image, root. When you change tool
versions, keep both in sync where they overlap.

---

## 4. Architecture & Data Flow

### 4.1 Ingestion → storage → generic querying

```
MCP client
   │  tool call (e.g. list_customers, list_parts, list_line_items)
   ▼
tools/*.py  ──►  database.py fetch_*_stream()     # pyodbc, streaming pa.RecordBatch
   │               (blocking ODBC in run_in_executor, batches pushed through asyncio.Queue)
   │               (list_line_items: also accumulates NettoPreisStueckEUR for its price_summary)
   ▼
result_store.ResultSetStore.store_result_stream() # enforces row/byte limits, incremental stats
   │                 writes chunk keys + meta key + session index via Redis pipeline
   ▼
Redis keys:  {prefix}:result:{rs_id}:chunk:000000   (LZ4 Arrow IPC, EX = max_ttl)
             {prefix}:result:{rs_id}:meta           (ResultSetMetadata JSON)
             {prefix}:session:{session}:results    (SET of the session's rs_ ids)
   │
   ▼
client receives ResultSetSummary { result_id, row_count, column_count, columns, expires_at, … }
   │
   ▼ (later calls)
tools/result_sets.py  ──►  QueryEngine (Polars over stored table loaded from Redis chunks)
   │                        filters/sorts/aggregates WITHOUT SQL; page slices load only needed chunks
   ▼
response rows / derived result set
```

### 4.2 Result-set lifecycle

1. Ingestion tool streams data into Redis as chunks; metadata holds stats, provenance, TTLs.
2. Every set has an **idle TTL** (`RESULTSET_IDLE_TTL_SECONDS`, default 1 h) extended on access
   (`ResultSetStore.update_access`, bounded by the absolute max) and an **absolute max lifetime**
   (`RESULTSET_MAX_TTL_SECONDS`, default 24 h). The reported `expires_at` reflects the **actual**
   current expiry — i.e. `now + idle_ttl`, re-armed & persisted on every access — not the static
   absolute cap. `absolute_expires_at` is the immutable upper bound kept for `update_access`; old
   stored metadata without `expires_at` backfills it to `absolute_expires_at` on load.
3. Sets are keyed by `result_id`, not by session. There are **no ownership checks anywhere**:
   any `result_id` can be read, joined, or deleted regardless of which session created it. The
   `owner_session_id` metadata field and the `{prefix}:session:{session}:results` index are retained
   for bookkeeping/listing only; they are never enforced as an access control boundary.
4. Explicite deletion via `delete_result_set`; TTL expiry is the primary cleanup.
5. A background `cleanup_loop` (every 300 s, started in lifespan) scans for orphan chunks and
   removes them (`run_cleanup` → `_cleanup_orphans`).

### 4.3 `store_result` chaining & provenance

Most generic tools accept `store_result: bool = False`. When `true`, the computed table is
persisted as a **new** result set (fresh `rs_` id) and a `ResultSetSummary` is returned instead
of rows. Every derived set records `ResultProvenance` (`op`, `parent`, `parameters`) plus
column `origin`/`source_column`. Derived sets are first-class tables and can be chained
recursively. **Read `src/result_set_spec.md`** — it is the authoritative spec for column naming,
alias validation, type/description inheritance and folding `<source>` fields into provenance.

### 4.4 Mouser flow

`search_mouser_parts` → normalize MPNs (`utils.normalize_mpn`: NFKC, dash-fold, collapse
whitespace, uppercase, strip non-`[A-Z0-9\-_.]`), dedupe order-preserving → per normalized MPN, parallel cached lookup via
`RedisMouserCache` (`{prefix}:mouser:part:{normalized}`, JSON envelope `{stored_at,payload}`
with Redis `EX`) else httpx `search_by_part_number` → `extract_purchasing_parts` → cache →
optional 429 retry (60 s) → JSON or CSV (+`sha256_base64`). Cache misses degrade to a miss,
never a tool failure.

---

## 5. Module Guide

| Module | Responsibility | Key symbols |
|---|---|---|
| `__main__.py` | Entry shim | `main()` from `.server` |
| `server.py` | FastMCP app; lifespan; tool registration; `/health`; DEBUG `LoggingMiddleware` | `mcp`, `lifespan`, `cleanup_loop`, `health_check`, `main()` |
| `config.py` | Single source of config truth | `Settings`, `settings` |
| `database.py` | SQL Server via pyodbc; column-description dictionaries; async stream generators | `build_connection`, `fetch_customers_stream`, `fetch_parts_stream`, `fetch_part_stats_stream`, `fetch_line_items_stream`, `fetch_webshop_prices_stream`, `_*_COLUMN_DESCRIPTIONS` |
| `result_store.py` | Chunked Redis persistence, limits, TTLs, session index, orphan cleanup, incremental stats | `ResultSetStore`, `_IncrementalStats` |
| `serialization.py` | Encode/decode tables & metadata | `serialize_table` (LZ4 IPC), `deserialize_table`, `serialize_metadata`, `deserialize_metadata` |
| `query_engine.py` | Polars engine over stored sets: filters, sort, projection, pagination, CSV/gzip encodings, `store_result`, `join` | `QueryEngine`, `_build_filter_expr`, `_apply_sort`, `_build_agg_exprs`, `_validate_aliases`, `_df_to_rows`, `_format_paginated_result`, `join` |
| `models.py` | Shared Pydantic arg/result types | `FilterClause`, `FilterGroup`, `SortSpec`, `AggregationSpec`, `ColumnInfo`, `ResultProvenance` (+`parents`), `ResultSetMetadata`, `ResultSetSummary`, `ColumnOrigin`, `JoinColumnPair`, `NormalizeOptions`, `JoinReport`, `JoinResultSummary` |
| `errors.py` | Stable, machine-readable error surface; never leaks internals | `ToolError` (+`to_dict`), `tool_error`, `wrap_redis_error`, `wrap_db_error` |
| `utils.py` | Single shared normalizer (used by Mouser and `join`) | `normalize_mpn` |
| `tools/customers.py` | Customer ingestion tool | `list_customers` |
| `tools/parts.py` | Parts / part-stats tools | `list_parts`, `list_part_stats` |
| `tools/documents.py` | Document line-item tool (configurable by document type) | `list_line_items`, `DOCUMENT_TYPE_BELEG_TYP`, `_price_summary` |
| `tools/webshop.py` | Webshop price tool | `get_webshop_prices` |
| `tools/mouser.py` | Mouser search tool | `search_mouser_parts` |
| `tools/result_sets.py` | Generic result-set tools (thin wrappers over `QueryEngine`) | `get_result_info`, `explain_columns`, `describe_columns`, `list_rows`, `count_rows`, `query_rows`, `distinct_values`, `aggregate`, `group_by`, `histogram`, `join`, `delete_result_set`, `normalize_preview` |
| `apis/mouser/api.py` | Mouser REST client | `search_by_part_number` |
| `apis/mouser/cache.py` | Cache backend & selection point | `RedisMouserCache`, `NullMouserCache`, `default_cache`, `build_cache_key`, `CachedEntry`, `cache_metadata` |
| `apis/mouser/parts_extraction.py` | Shape raw Mouser payload → purchasing parts | `extract_purchasing_parts` |

---

## 6. MCP Tool Inventory

All tools are registered in `server.py` via `mcp.tool()`.

### Ingestion tools (return a `ResultSetSummary`, never rows)

| Tool | Source | Safety-relevant behavior |
|---|---|---|
| `list_customers(customer_name)` | SQL Server `Adressen` (LIKE on `SuchName`) | streams, stores, returns summary |
| `list_parts(positive_filter_term, negative_filter_term="TCC%", match_code, sample_n)` | SQL Server `Artikel` (+ supplier + warehouses) | `sample_n` ⇒ random sample of up to n rows **drawn from the filtered result** (post-filter, via `ORDER BY NEWID()` on the filtered subquery). **Default `negative_filter_term='TCC%'` silently excludes all `TCC` articles (tens of thousands); when the default applies the returned summary includes a `warning` field.** Pass `negative_filter_term=None` for the full master |
| `list_part_stats(mpn, year, sales_purchasing, limit)` | SQL Server `StatistikenArtikel` join | `year` **mandatory**; `limit` returns rows inline instead of storing |
| `list_line_items(artikel_nummer, document_type, date_from, date_to)` | SQL Server `BelegePositionen`/`Belege`/`Waehrungen`/`Adressen` | One document type per call via `document_type` code → `BelegTyp`: `AN`=2 (Angebot), `AB`=3 (Auftrag), `AL`=4 (Lieferschein Ausgang), `AR`=5 (Rechnung Ausgang), `AA`=8 (Lieferantenanfrage), `EB`=9 (Bestellung — purchasing), `ER`=10 (Rechnung Eingang); inclusive date range `>=`/`<=` on `B.Datum`; joins the counterparty (`KundeOderLieferant` + classification/branch) and delivery dates; uneven `document_type` → `INVALID_ARGUMENT` with `available_document_types`; returns summary **plus** `price_summary` (`min`/`max`/`average`/`median`/`count` of `NettoPreisStueckEUR`) |
| `get_webshop_prices(mpns, output_format)` | SQL Server `WebshopPreise` | ≤25 MPNs; CSV (or JSON) per-MPN price range; raises `ToolError` on bad input |
| `search_mouser_parts(mpns, caching, cache_ttl_override_hours, output_format, retry_on_rate_limit)` | Mouser API (httpx) | ≤25 MPNs, parallel, per-MPN cache, per-MPN error isolation |

### Generic result-set tools (all require `result_id`)

| Tool | Purpose | Notable params |
|---|---|---|
| `get_result_info` | Metadata + column stats, safe field set (no owner id) | `result_id` |
| `explain_columns` | Per-column stats, optional `examples` | `columns`, `include_examples` |
| `describe_columns` | Per-column distribution bundle (mean, stddev, min, percentiles, max, skewness, count_negative, counts); whole set or per group | `columns`, `percentiles` (default `[0.05,.25,.5,.75,.95]`, linear), `filters`, `group_columns` (≤3) |
| `list_rows` | Paginate raw rows; sorting/projection applied per call | `columns`, `sort`, `limit`, `offset`, `format` (`json`/`csv`), `encoding` (`plain`/`gzip+base64`), `round_to`, `stable_sort`, `store_result`, `result_label` |
| `count_rows` | Row count w/ optional filters | `filters` |
| `query_rows` | Filter + sort + project + paginate | `filters`, `columns`, `sort`, `store_result`, `result_label`, `round_to` |
| `distinct_values` | Value frequencies for a column | `column`, `include_counts`, `order`, `include_null`, `store_result`, `result_label` |
| `aggregate` | Single-row aggregation over whole (filtered) set | `aggregations`, `store_result`, `result_label` |
| `group_by` | Per-group aggregation | `group_columns` (≤3), `aggregations`, `sort`, `store_result`, `result_label` |
| `histogram` | Binned histogram (numeric/date) or top-N (string) | `column`, `bins` (≤50), `store_result`, `result_label` |
| `join` | Join two result sets; only cross-`result_id` operation | `left_result_id`, `right_result_id`, `on` (shared name or `{left,right}` pairs), `how` (`left` default), `columns_left`/`columns_right`, `suffix` (`_r`), `normalize_keys` (default on: single fixed `normalize_mpn` pipeline — trim/collapse whitespace, NFKC, dash-fold, uppercase, strip), `validate` (`many_to_one` default; **uniqueness is scoped to participating keys only** — a duplicate absent from the other side cannot fan out and never blocks; it is surfaced in `data_quality`), `right_duplicates` (`error`\|`first`\|`report_only`; default `error`; `first` keeps the lowest-ordinal right row per normalized key and preserves sums, `report_only` proceeds and records a metadata note), `indicator` (`_join_match`), `result_label`, `dry_run` (`false` default; returns a `JoinDryRunReport` with no `result_id`, still enforcing `validate`/size guard). Returns `JoinResultSummary` (structural report only — no measures; use `aggregate`/`group_by` filtered on the indicator to quantify unmatched rows). Report now includes `deduplicated_rows` and a `data_quality` block (`non_participating_collisions`, `sample`). On failure, `CARDINALITY_VIOLATION` carries `keys: [{normalized, count, raw, side, caused_by_normalization, steps_applied}]` in its details. Raises `CARDINALITY_VIOLATION`, `JOIN_KEY_TYPE_MISMATCH`, `COLUMN_NAME_COLLISION`, `RESULT_SET_TOO_LARGE` (pre-materialization estimate) |
| `delete_result_set` | Explicit deletion | `result_id` |
| `normalize_preview` | Preview how strings are normalized (join/config pipeline) | `values` (≤100); returns `[{raw, normalized, steps: [...]}]` per input — makes key-collision causes directly observable |

**Pagination contract** (`list_rows`, `query_rows`, `group_by`, `distinct_values`): the response is
terminal whenever a page returns zero rows — `has_more` is `false` and `next_offset` is `null`.
In particular `limit=0` and an offset past the end both terminate; a client driving the loop via
`while has_more: offset = next_offset` can never be stuck. `has_more` requires both
`returned > 0` **and** `offset + limit < total_rows`.

The `aggregations` vocabulary (both `aggregate` and `group_by`) covers (see `models.AggregationSpec`
and `result_set_spec.md` §9.3): `count`, `count_non_null`, `count_distinct` (alias `nunique`), `sum`, `avg`
(`mean`), `min`, `max`, `median`, `percentile` (`q`, `method` default `linear`), `mode`,
`trimmed_mean` (`trim`), `stddev`/`variance` (`ddof`), `iqr`, `mad`, `skewness`, `kurtosis`,
`coefficient_of_variation`, `count_null`, `count_zero`/`count_negative`/`count_positive`,
`weighted_avg` (`weight_column`), and `ratio` (`denominator_column`). Statistical ops drop nulls;
pair with `count_null` when the drop matters. An invalid operation raises `INVALID_AGGREGATION`
with `details.available_operations` listing the vocabulary (mirrors `UNKNOWN_COLUMN` →
`available_columns`).

---

## 7. Configuration Reference

Everything is read from `src/alphaplan_mcp/config.py` (`Settings`, pydantic-settings).
Env file: `.env` in the CWD; Docker Compose feeds it from `docker/.env`.

| Variable | Default | Purpose |
|---|---|---|
| `DEBUG` | `false` | Enables `LoggingMiddleware` with payloads (dev only) |
| `MCP_HOST` | `0.0.0.0` | Bind host |
| `MCP_PORT` | `9000` | HTTP port |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection URL |
| `REDIS_KEY_PREFIX` | `alphaplan-mcp` | Prefix for **all** app keys (result sets + mouser cache) |
| `RESULTSET_CHUNK_ROWS` | `5000` | Rows per stored chunk |
| `RESULTSET_IDLE_TTL_SECONDS` | `3600` | Idle TTL, extended on access; drives the reported `expires_at` |
| `RESULTSET_MAX_TTL_SECONDS` | `86400` | Absolute max lifetime; the immutable cap (`absolute_expires_at`) |
| `RESULTSET_MAX_ROWS` | `250000` | Max rows per result set |
| `RESULTSET_MAX_BYTES` | `268435456` | Max uncompressed size per result set |
| `RESULTSET_MAX_GROUPS` | `10000` | Max groups in `group_by` |
| `TOOL_MAX_ROWS` | `200` | Cap for row-returning tools |
| `TOOL_MAX_DISTINCT_VALUES` | `200` | Cap for `distinct_values` (and string histogram bins) |
| `TOOL_MAX_FILTER_CLAUSES` | `20` | Cap for `FilterGroup.clauses` |
| `TOOL_MAX_GROUP_COLUMNS` | `3` | Cap for `group_by` columns |
| `TOOL_MAX_AGGREGATIONS` | `10` | Cap for aggregation list |
| `SQLSERVER_HOST` / `_DATABASE` (`ApiPreise`) / `_USER` / `_PASSWORD` / `_DRIVER` (`ODBC Driver 18 for SQL Server`) / `_TRUST_CERTIFICATE` (`yes`) / `_QUERY_TIMEOUT_SECONDS` (`120`) | — | SQL Server connectivity; connection enforces `ReadOnly`, `Encrypt=yes`, autocommit |
| `MOUSER_API_KEY` | — | Mouser API key (required for `search_mouser_parts`) |
| `MOUSER_API_BASE_URL` | `https://api.mouser.com/api/v1` | Mouser endpoint |
| `MOUSER_TIMEOUT_SECONDS` | `15.0` | httpx timeout |
| `MOUSER_CACHE_TTL_HOURS` | `168.0` | Default cache TTL (7 days) |
| `MOUSER_RATE_LIMIT_RETRY_SECONDS` | `60.0` | 429 → single retry delay |

Similar settings: `tools/mouser.py::MAX_MPNS_PER_CALL = 25`, hard-coded `MAX_IN_VALUES = 100`
in `query_engine.py`, `histogram` caps `bins` at 50.

---

## 8. Development Workflow

The dev environment is a **uv** venv rooted at `src/`:

```bash
cd src
uv sync              # install from src/pyproject.toml + uv.lock
uv run python -m alphaplan_mcp        # run server (needs Redis + .env + a reachable SQL Server)
uv run python -m uvicorn ...          # (not the normal path; server.main() runs its own HTTP server)
```

### Running tests

Rootdir is the **repo root**; test modules live in `tests/` *outside* `src/`, while the package
is installed into `src/.venv`. The invocation known to work (matches `src/pytest-tests-unit.out`,
which shows `../tests/unit/...`) is:

```bash
cd src
uv run pytest ../tests/unit          # all unit tests (319)
uv run pytest ../tests/unit/test_query_engine.py   # single module
```

Notes:
- Conventions: `@pytest.mark.asyncio` on each async test; `pytest_asyncio` **strict** mode
  (set by plugin defaults — there is no `[tool.pytest.ini_options]` anywhere); `fake_redis`
  fixture (`fakeredis.FakeAsyncRedis`) provided by `tests/conftest.py`.
- Unit tests mock Redis with fakeredis — no live Redis needed. `tests/integration/` is empty.
- `tests/unit` modules: `test_query_engine`, `test_result_store`, `test_serialization`,
  `test_mouser_api`, `test_mouser_cache`, `test_search_mouser_parts`, `test_part_stats`,
  `test_list_line_items`, `test_webshop_prices`, `test_join`.

### Running locally (dev)

```bash
docker compose -f docker/docker-compose.yml up --build   # full stack (port 9001→9000)
# or, against a local Redis + your .env:
uv run python -m alphaplan_mcp
```

---

## 9. Conventions & Patterns

1. **Errors** — Use `ToolError` with a stable `code` (see `errors.py`; codes such as
   `RESULT_NOT_FOUND`, `RESULT_SET_TOO_LARGE`, `TOO_MANY_GROUPS`, `UNKNOWN_COLUMN`,
   `INVALID_ARGUMENT`, `MOUSER_TIMEOUT`, …). Never leak stack traces, SQL, Redis internals,
   or connection strings. Each result-set tool wraps calls in `try/except ToolError` and returns
   `e.to_dict()` (JSON-able) rather than raising. During ingestion tools, the same pattern applies.
   **Inconsistency to be aware of:** `webshop.py` and `parts.list_part_stats` **raise**
   `ToolError` instead of returning `to_dict()`; keep behavior matching the surrounding tool when
   editing one of them.
2. **Session ownership** — There are **no ownership checks anywhere**; every `result_id` access goes
   through `QueryEngine._ensure_owned`, which only verifies existence and refreshes the idle TTL.
   Keep this invariant for any new result-set tool.
3. **Safety limits** — Enforce `settings.TOOL_*`/`RESULTSET_*` caps before doing work
   (row count, bytes, groups, filter clauses, `in`-list length, bins). Full-table loads are
   refused when `uncompressed_size_bytes > RESULTSET_MAX_BYTES`.
4. **Column descriptions** — Database fetchers attach human-readable descriptions as PyArrow
   field metadata (`b"description"`); the store reads them into `ColumnInfo.description`.
   `src/result_set_spec.md` defines the rules for inheriting/deriving descriptions and types on
   `store_result`. Preserve `type`/`description` fidelity through projections (avoid
   `large_string` leakage).
5. **Streaming** — The DB fetchers all follow one pattern: blocking pyodbc work in
   `asyncio.get_running_loop().run_in_executor(None, _fetch)`, batches pushed to an
   `asyncio.Queue`, `None` as end/error sentinel; the async generator awaits the executor task at
   the end to propagate exceptions. Duplicate this for any new fetcher.
6. **Redis writes** — Batch multi-key operations into a single `pipe = redis.pipeline()`
   transactionally (chunk + meta + session index). Use `EX` TTLs everywhere.
7. **CSV exports** — Tools returning CSV produce `sha256_base64 = Base64(SHA-256(UTF-8 CSV))`,
   LF line endings, final newline, and tell the consumer to verify before use.
8. **`store_result`** — Supports chaining; assigns real column names (never `value`/`col0`),
   validates aliases (`^[A-Za-z_][A-Za-z0-9_]*$`, no collision), records `ResultProvenance`
   (note: `join` emits plural `parents`; the rest emit singular `parent`), and returns a
   `ResultSetSummary`, not rows. See `src/result_set_spec.md`.
9. **Config** — Never hardcode environment-specific values in modules; add a field to
   `Settings` (mirroring existing naming), update §7 of this file, and update whichever
   `pyproject.toml` needs the dependency.
10. **Docstrings** — Tool functions have elaborate parameter/behavior docstrings; FastMCP
    surfaces these to the model. Keep them accurate and detailed when touching tools.

---

## 10. Known Quirks & Stale Docs

- **Dual `pyproject.toml`** — root is packaging/Docker; `src/` is the uv dev environment. Keep
  deps in sync where they overlap (§3).
- **`src/README.md` is a pointer** to the root `README.md` and holds no layout/doc content of its
  own; the root `README.md` is the canonical user-facing documentation. The Dockerfile previously
  worked around the missing root README by copying `src/README.md` — this is no longer needed.
- **`src/main.py` is legacy** — duplicated `list_customers` logic with env-var parsing; do not
  extend. Prefer the packaged entry points.
- **Inconsistent error style** — `webshop.py` / `list_part_stats` raise `ToolError`; most other
  tools return `to_dict()`.
- **`default_cache`** in `apis/mouser/cache.py` starts as `NullMouserCache()` and is replaced in
  `server.lifespan` with `RedisMouserCache(app.redis)`. Tools importing `default_cache` must read
  it at call time, not at import time.
- **`agent/` is empty**; `tests/integration/` is empty; `docker/out` is a build-log **file**.
- **`prefix` in keys contains a colon** (default `alphaplan-mcp`); `_cleanup_orphans` extracts
  result ids by stripping the *known* prefix length, not by splitting on `:` — keep this in mind
  if you change the prefix format.
- `.aider.*` files at the repo root are tooling artifacts (gitignored); don't reference them.
- Staging TTL of **300 s** is used for chunks while a stream ingests; final TTLs are promoted to
  `max_ttl` at the end. Empty streams still produce a valid (empty) result set.

---

## 11. Security Notes

- The MCP endpoint is **unauthenticated by default** and serves potentially sensitive customer,
  pricing, and part statistics. Do not expose it publicly without an auth layer.
- Result sets and the Mouser cache live in Redis; restrict access to Redis.
- DB connections are `ReadOnly` + `Encrypt=yes`; user input never forms SQL text — filters and
  aggregations are constrained to the Polars DSL on materialized data, and DB predicates use
  parameterized `?` placeholders with LIKE values as parameters.
- Never log or return secrets (API keys, DB passwords), stack traces, or raw error internals.

---

## 12. How to Extend

**Add a new MCP tool**
1. Add Pydantic arg/result models to `models.py` (or reuse existing ones).
2. If it queries SQL Server, add a `fetch_*_stream` to `database.py` with a column-description
   dict, following the streaming pattern (§9.5).
3. Implement the tool in the appropriate `tools/*.py` module (thin wrapper over `QueryEngine` or
   store) with a detailed docstring and `ToolError` handling matching that file's style.
4. Register it in `server.py` via `mcp.tool()`.
5. Add unit tests under `tests/unit/` and run §8.
6. If new behavior settings are needed, extend `Settings` in `config.py` and update §7 here.

**Add a new external API namespace** — mirror `apis/mouser/`: an `api.py` (httpx client raising
`ToolError`), a `cache.py` (Redis-backed, best-effort), and a `tools/*.py` wrapper.

**Keep this file in sync** — any change to entry points, tool registrations, settings, the
repo layout, or test commands must be reflected in AGENTS.md.