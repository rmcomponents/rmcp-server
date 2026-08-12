# Spec: Provenance & column semantics for stored result sets

**Audience:** the coding agent implementing this in the Alphaplan MCP server.
**Status:** backend provenance plumbing is prepared; this document defines the behavior to wire through per tool.
**Scope:** all tools that can persist output as a new result set (`store_result`), plus `get_result_info` / `explain_columns`.

---

## 0. Core principle

A stored result set is **always a table of named, typed columns**, and *every* tool that accepts a `result_id` must accept a derived one and behave identically. So the output of an aggregation is not a special "report object" — it is a first-class table that `query_rows`, `count_rows`, `group_by`, `aggregate`, `distinct_values`, `histogram`, `list_rows`, and `explain_columns` can run on recursively.

Design everything below so that **chaining is transparent**: a derived set must be indistinguishable from a base set in how downstream tools consume it. The only difference is richer metadata.

---

## 1. The `store_result` flag (all applicable tools)

- Add `store_result: bool = false` to `group_by`, `aggregate`, `distinct_values`, and `histogram` (matching the existing `query_rows` / `list_rows` behavior). **Do not** introduce separate "store" tools — one flag per tool keeps the surface uniform.
- When `store_result = false`: current behavior, unchanged (return the computed rows/summary inline as today).
- When `store_result = true`:
  - Persist the computed table as a new result set with a fresh `result_id` (same Redis lifecycle/TTL as `list_parts`).
  - Return the **`get_result_info`-style summary of the new set**, not the rows. Rows are fetched on demand via `list_rows` / `query_rows`. This preserves the "no rows into the model context" contract.
- Add an optional `result_label: str | null = null` so the caller can name the set (free-text; store verbatim in metadata; never used as a key).

---

## 2. Column naming — per tool

**Rule for all tools:** never emit generic names (`value`, `col0`, `key`). Generic names break the chaining mental model and collide on re-aggregation. Every column name must be a real, meaningful identifier that a downstream filter can reference by name.

### `group_by`
- Emit the `group_columns` first, each keeping its **original name and original type** (inherited from the parent — see §3).
- Then one column per aggregation, named **exactly by its `alias`** (alias becomes the column name).
- Validate `alias` at request time: must match `^[A-Za-z_][A-Za-z0-9_]*$`, must not contain whitespace/dots/quotes (these break the filter DSL), and must not collide with a `group_column` name or another alias. On violation, **reject with a clear error** — never silently rename.
- Resulting shape example: columns `HauptLieferant` (passthrough) + `anzahl` (aggregate). This is already the row shape the tool returns today; persisting it should be a straight materialization.

### `aggregate` (scalar aggregation over the whole set)
- Produce a **single-row table**; columns = the aliases. Same alias validation as `group_by`.
- Keep it a one-row table even when it is a single scalar — consistency matters more than compactness, and it stays chainable.

### `distinct_values`
- Name the value column **after the source column** (e.g. `HauptLieferant`), **not** `value`. This makes a `distinct_values` set look like a small `group_by` set and lets downstream tools filter it by the same real column name.
- The count column is `count`.
- Without `include_counts`: emit only the single value column.
- With `include_null`: represent the null bucket as a **real `null`** in the value column (not the string `"null"`).

### `histogram`
Two modes, both fully materialized and filterable:
- **Numeric / date:** columns `bin_index` (int64), `bin_lower`, `bin_upper` (source numeric/date type), `bin_label` (string, e.g. `"10–20"`), `count` (int64).
- **String top-N:** effectively `distinct_values` with a cap. Value column named after the source column, `count`, plus an `is_other` (bool) flag column. The trailing "other" bucket is a row with `is_other = true` and a `null` category value — **mark it explicitly** rather than smuggling it in as if it were a real category, so the table stays both complete and correctly filterable.

---

## 3. Type & description fidelity (there is a real gap here today)

Observed current behavior: a stored `query_rows` set came back with `type: "large_string"` and `description: null` — i.e. it **lost the clean `string` type and the column descriptions** ("Main supplier name", etc.) from the parent. Fix this as part of the work.

Rules when materializing:
- **Passthrough columns** (group keys, projected/carried-over columns) must **inherit `type` and `description` from the parent set**. A projected `string` column stays `string` (not `large_string`); its description carries over verbatim.
- **Computed columns** must be typed from the operation, not from a default:
  - `count` → `int64`
  - `avg` / `mean` → `float64`
  - `sum` → numeric matching the source column's numeric family (int→int64 or int64→numeric as appropriate; float→float64)
  - `min` / `max` → the **source column's type**
  - `bin_index` → `int64`; `bin_lower` / `bin_upper` → source column type; `bin_label` → `string`; `is_other` → `bool`
- If a source type genuinely cannot be preserved (e.g. an engine-level widening), record the original type in provenance (§4) so `get_result_info` can still report the semantic type.

---

## 4. Provenance to persist

Store enough that a derived set is **self-describing** without the caller re-deriving context.

Set-level metadata:
- `parent_result_id: str` — the set this was computed from.
- `source` as a **structured object**, not just the string `"group_by"`. Example:
  ```json
  {
    "op": "group_by",
    "parent": "rs_e26e5de8-...",
    "group_columns": ["HauptLieferant"],
    "aggregations": [{"operation": "count", "alias": "anzahl", "column": null}],
    "filters": { "clauses": [], "combine": "all" }
  }
  ```
  (Include `filters` whenever the op applied a filter, so the lineage is exact.)
- `result_label` (from §1) if provided.

Per-column metadata (extend whatever `explain_columns` / `get_result_info` already returns):
- `origin: "group_key" | "aggregate" | "passthrough" | "derived"`
- For `aggregate`-origin columns: `operation` (e.g. `"count"`) and `source_column` (e.g. `"ArtikelNummer"`, or `null` for count-of-rows).
- For `passthrough` / `group_key` columns: `source_column` on the parent (for description inheritance).

---

## 5. `get_result_info` / `explain_columns` behavior on derived sets

### 5.1 Statistics — no special-casing
Column statistics (`min`, `max`, `avg`, `sum`, `min_length`, `max_length`, `distinct_count`, `examples`) should just compute over the column's contents regardless of where the column came from. `max` of a `count` column and length-stats on a string group key are meaningful as-is.

Two statistics are directly useful and worth surfacing/validating:
- On a `group_by` set, `sum` of the `count` column should equal the parent set's row count — a cheap **invariant check**.
- `max` of the `count` column = largest group size.

### 5.2 Generate descriptions from provenance
Do **not** leave derived-column `description` null. Generate it from provenance:
- Aggregate column `anzahl`: `"group_by aggregation: count per group"`.
- Group-key column `HauptLieferant`: `"grouping key; from parts.HauptLieferant (Main supplier name)"` — the parenthetical inherited from the parent's description (§3).
- Histogram `count`: `"number of rows in this bin"`; `bin_label`: `"human-readable bin range"`; `is_other`: `"true = aggregated remainder bucket, category value is null"`.

### 5.3 Expose the grain
An aggregated set's semantics aren't visible from row shape alone, so add set-level fields:
- `grain: "one row per (HauptLieferant)"` (list the group columns; for `aggregate`, `grain: "single summary row"`; for `distinct_values`, `"one row per distinct HauptLieferant"`).
- Make explicit that for aggregated sets `row_count` = number of groups/bins, not underlying source rows. Optionally also carry `source_row_count` for clarity.

---

## 6. Worked examples (target return schemas)

### 6.1 `group_by(..., store_result=true)` → returned summary
```json
{
  "result_id": "rs_new-...",
  "parent_result_id": "rs_e26e5de8-...",
  "result_label": null,
  "source": {
    "op": "group_by",
    "parent": "rs_e26e5de8-...",
    "group_columns": ["HauptLieferant"],
    "aggregations": [{"operation": "count", "alias": "anzahl", "column": null}],
    "filters": {"clauses": [], "combine": "all"}
  },
  "grain": "one row per (HauptLieferant)",
  "row_count": 16,
  "source_row_count": 18231,
  "column_count": 2,
  "created_at": "...",
  "expires_at": "...",
  "columns": [
    {
      "name": "HauptLieferant",
      "type": "string",
      "origin": "group_key",
      "source_column": "HauptLieferant",
      "description": "grouping key; from parts.HauptLieferant (Main supplier name)",
      "null_count": 0,
      "non_null_count": 16
    },
    {
      "name": "anzahl",
      "type": "int64",
      "origin": "aggregate",
      "operation": "count",
      "source_column": null,
      "description": "group_by aggregation: count per group",
      "min": 2,
      "max": 10995,
      "sum": 18231
    }
  ]
}
```

### 6.2 `aggregate(..., store_result=true)` → single-row set
```json
{
  "result_id": "rs_new-...",
  "parent_result_id": "rs_e26e5de8-...",
  "source": {"op": "aggregate", "parent": "rs_e26e5de8-...",
             "aggregations": [
               {"operation": "count", "alias": "n", "column": null},
               {"operation": "avg", "alias": "avg_vpe", "column": "VPE"}]},
  "grain": "single summary row",
  "row_count": 1,
  "column_count": 2,
  "columns": [
    {"name": "n", "type": "int64", "origin": "aggregate",
     "operation": "count", "source_column": null,
     "description": "aggregate: total row count"},
    {"name": "avg_vpe", "type": "float64", "origin": "aggregate",
     "operation": "avg", "source_column": "VPE",
     "description": "aggregate: mean of parts.VPE (Sales unit)"}
  ]
}
```

### 6.3 `distinct_values("HauptLieferant", include_counts=true, store_result=true)`
Columns: `HauptLieferant` (string, `origin: "derived"`, description `"distinct values of parts.HauptLieferant (Main supplier name)"`) and `count` (int64, description `"row count per distinct value"`). `grain: "one row per distinct HauptLieferant"`.

---

## 7. Validation & edge cases

- **Alias validation** (`group_by`, `aggregate`): regex `^[A-Za-z_][A-Za-z0-9_]*$`, no collisions with group columns or each other → reject with an actionable error naming the offending alias.
- **Empty result**: still create the set, `row_count: 0`, columns/schema intact so downstream tools don't crash.
- **Nulls in group keys**: preserve as real `null` group values; `include_null` semantics consistent between `group_by` and `distinct_values`.
- **`is_other` row** (histogram string mode): exactly one such row max; its category value is `null`; downstream filters on the category column must not accidentally match it.
- **Chaining guarantee**: run at least one downstream op on every derived-set type in tests (e.g. `query_rows` filter on a `group_by` set, `group_by` on a `distinct_values` set) to prove first-class behavior.
- **Provenance depth**: `parent_result_id` chains; do not flatten. `get_result_info` reports the immediate parent; full lineage can be walked via repeated calls.

---

## 8. Acceptance criteria

1. `store_result` present and functional on `group_by`, `aggregate`, `distinct_values`, `histogram`; returns a summary (no rows), with a fresh `result_id`.
2. Column names follow §2 exactly; no generic names; alias collisions rejected.
3. Passthrough columns inherit parent `type` and `description`; the `large_string` / `description: null` regression from `query_rows` store is fixed for passthrough columns.
4. Computed columns typed per §3.
5. Each column carries `origin` (+ `operation` / `source_column` where applicable); each set carries `parent_result_id`, structured `source`, and `grain`.
6. `get_result_info` generates non-null descriptions for derived columns and reports `grain` + `row_count` semantics (groups vs source rows).
7. Every derived-set type is provably consumable by every `result_id`-accepting tool (chaining tests green).
8. Invariant: on a `group_by` count set, `sum(count_column) == parent.row_count`.

---

## 9. Tool Reference

### 9.1 Filter Operators
Filters are defined using `FilterGroup` and `FilterClause`. The allowed `operator` values and their expected `value` shapes are:
- `eq`: Single value. Equality check. (Supports `%` and `_` wildcards for strings, acting like `like`).
- `ne`: Single value. Inequality check.
- `gt`, `gte`, `lt`, `lte`: Single value. Numeric/date comparisons.
- `in`: Array of values. Matches if the column value is in the list.
- `not_in`: Array of values. Matches if the column value is not in the list.
- `between`: Array of exactly two values. Matches if `value >= low` and `value <= high`.
- `like`: String with `%` (any sequence) and `_` (single character) wildcards.
- `not_like`: Negated `like`.
- `regex`: String regular expression. Matches rows whose value matches the pattern.
- `not_regex`: Negated `regex`.
- `matches_charset`: String set of allowed characters. Matches values composed only of those
  characters (e.g. `matches_charset: "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_."` flags keys that are
  clean under the normalization allow-list).
- `not_matches_charset`: Negated `matches_charset`. A cheap sweep for dirty keys — any value
  containing a character outside the allowed set (e.g. a stray comma in an `ArtikelNummer`).
- `contains`: String substring match.
- `starts_with`: String prefix match.
- `ends_with`: String suffix match.
- `is_null`: No `value` field required. Matches null values.
- `is_not_null`: No `value` field required. Matches non-null values.

### 9.2 `case_sensitive` Default
String comparisons (e.g., `eq`, `like`, `contains`, `regex`, `matches_charset`) are
case-insensitive by default (`case_sensitive: false`). Set `case_sensitive: true` in the
`FilterClause` for case-sensitive matching.

### 9.3 Aggregation Operations
For `aggregate` and `group_by`, the `operation` field in `AggregationSpec` must be one of:
- `sum`: Requires `column`.
- `avg`: Requires `column`. Alias: `mean`.
- `median`: Requires `column`. Exactly the 50th percentile.
- `percentile`: Requires `column` and `q` (a quantile in `[0,1]`). Optional `method`
  (`linear`* | `lower` | `higher` | `nearest` | `midpoint`) selects interpolation — the default
  of **`linear`** matches numpy/pandas. `median` is unambiguous regardless of method.
- `mode`: Requires `column`. Most frequent value (first occurrence on ties).
- `trimmed_mean`: Requires `column` and `trim` (fraction per end, `0 <= trim < 0.5`); trims by
  linear-quantile bounds then averages.
- `stddev`: Requires `column`. Optional `ddof` (1* = sample, 0 = population).
- `variance`: Requires `column`. Optional `ddof` (1* = sample, 0 = population).
- `iqr`: Requires `column`. `q(0.75) - q(0.25)` (linear).
- `mad`: Requires `column`. Median absolute deviation (`median(|x - median(x)|)`) — robust spread.
- `skewness`: Requires `column`.
- `kurtosis`: Requires `column` (Fisher).
- `coefficient_of_variation`: Requires `column`. `stddev / mean`.
- `weighted_avg`: Requires `column` (value) and `weight_column`. `Σ(value·weight) / Σ(weight)`.
- `ratio`: Requires `column` (numerator) and `denominator_column`. `sum(column)/sum(denominator_column)`.
  Use `sum(A)/sum(B)`, never an average of per-row ratios, to compute e.g. margin percentage.
- `count`: Does not accept a `column` (use `column: null`). Counts the number of rows.
- `count_non_null`: Requires `column`. Counts non-null values.
- `count_distinct`: Requires `column`. Counts distinct values. Alias: `nunique`.
- `count_null`: Requires `column`. Counts null values.
- `count_zero` / `count_negative` / `count_positive`: Require `column`. Count rows in each bucket
  (nulls excluded). `count_negative` isolates credits/corrections without a separate filtered query.
- `min`: Requires `column`.
- `max`: Requires `column`.

All statistical operations **drop nulls** before computing; if you need to know how many were
dropped, pair them with an explicit `count_null` aggregation. An invalid or unknown `operation`
raises `INVALID_AGGREGATION` whose `details.available_operations` lists the valid vocabulary.

### 9.4 Enum Values for `distinct_values` and `histogram`
- `distinct_values` `order` parameter accepts: `count_desc` (default), `count_asc`, `value_asc`, `value_desc`.
- `histogram` on string columns returns the top-N values by count plus an `other_count` bucket. The response `bins` count will be N+1. For numeric columns, it returns evenly spaced bins.

### 9.5 `store_result` Semantics
When `store_result=true` is passed to `list_rows`, `query_rows`, `group_by`, `distinct_values`, or `histogram`, the **full** filtered/grouped result set is persisted as a new result set. The `limit` and `offset` parameters only affect the rows returned in the immediate response, not the persisted set.

### 9.6 Hard Limits and Lifecycle
- Result sets are capped at 250,000 rows (`RESULTSET_MAX_ROWS`). Exceeding this raises `RESULT_SET_TOO_LARGE`.
- Result sets have a TTL (default ~24h, configurable via `RESULTSET_IDLE_TTL_SECONDS` and `RESULTSET_MAX_TTL_SECONDS`). The `expires_at` field indicates the absolute expiration time.

### 9.7 Error Catalog
- `UNKNOWN_COLUMN`: The requested column does not exist. Returns `available_columns` in the details.
- `INVALID_ARGUMENT`: The request parameters are invalid (e.g., bad operator, too many clauses).
- `RESULT_NOT_FOUND`: The `result_id` does not exist or has expired.
- `RESULT_SET_TOO_LARGE`: The operation exceeded the row or byte limit.
- `TOO_MANY_GROUPS`: `group_by` cardinality exceeded the limit.

### 9.8 Analytics Operate on the Full Parent Set
`count_rows`, `aggregate`, `group_by`, `distinct_values`, and `histogram` apply their `filters` directly to the full parent result set. You do not need to materialize a subset first to compute metrics. `result_label` is purely organizational and does not affect data.

### 9.8b `describe_columns`
`describe_columns(result_id, columns, percentiles, filters, group_columns)` returns a per-column
distribution bundle (count, count_null, mean, stddev, min, percentiles, max, skewness,
count_negative) over the whole set or per group. It is the statistical companion to
`explain_columns` (which covers the schema side). Prefer it to `avg` alone when a distribution
may be skewed — the mean alone is misleading on the revenue columns (mean ≈ 4.3× the median).

### 9.9 Join key diagnostics (recommended defaults)
- `validate` uniqueness is **scoped to participating keys only**: a duplicate normalized key that is absent from the other side cannot fan out, so it never blocks — it is surfaced in `data_quality.non_participating_collisions` (with a `sample`). A participating duplicate still raises `CARDINALITY_VIOLATION`. This lets dirty master data be reported without turning the guard into a blocker.
- `right_duplicates="first"` keeps the lowest-ordinal right row per normalized key (row-count and sums preserved) and records `deduplicated_rows` in both the report and provenance — a safe alternative to disabling validation.
- The report's `normalization.collisions` and `data_quality.sample` entries carry `{normalized, raw, side, caused_by_normalization, steps_applied}`; use `normalize_preview(values)` to confirm which normalization step produced a collision, and `dry_run=true` to obtain the full report without materializing a result set.

## 10. Tool: `list_parts`

- `sample_n` (optional int): If provided, returns a random sample of up to `sample_n` rows, drawn **after** all other filters (`positive_filter_term`, `negative_filter_term`, `match_code`) are applied, i.e. a sample of the filtered population. If omitted, all matching rows are returned.
- `negative_filter_term` defaults to `'TCC%'`, which **silently excludes all `TCC%` articles (tens of thousands of rows)**. When the default is applied, the returned summary carries a prominent `warning` field. Pass `negative_filter_term=None` for the full article master.

## 11. Tool: `search_mouser_parts`

- `mpns` (list of str, required): 1–5 manufacturer part numbers. Each MPN is looked up independently, in parallel, and cached individually in Redis. Duplicates (after normalization) are queried only once.
- `caching` (bool, default `true`): when `false`, the cache is neither read nor written.
- `cache_ttl_override_hours` (float, default 168, must be positive).
- Returns `{"results": {<normalized MPN>: {"part_count", "parts", "metadata"}}}`. Per-MPN lookup failures (e.g. `MOUSER_TIMEOUT`) are returned as an error object in that MPN's entry only — they do not fail the whole call. Call-level validation errors (`INVALID_MPNS`, `TOO_MANY_MPNS`, `INVALID_CACHE_TTL`) are returned as a top-level error object.
- `metadata.source` is `"mouser_api"` or `"cache"` per MPN; `metadata.cached` / `cache_age_hours` reflect the per-MPN cache state.
- **Mouser API quotas: max 30 requests per minute and 1000 requests per day.** Only cache misses consume quota. Keep caching enabled and batch up to 5 MPNs per call to conserve the daily quota.

