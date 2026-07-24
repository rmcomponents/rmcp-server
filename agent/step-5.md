# Step 5: Implement Provenance Persistence

## Objective
Persist set-level and per-column provenance so derived sets are self-describing.

## Changes
1. **`src/alphaplan_mcp/query_engine.py`**:
   - **Set-level metadata**:
     - Set `parent_result_id` (or include in `source` object).
     - Construct the structured `source` object (e.g., `{"op": "group_by", "parent": "...", "group_columns": [...], "aggregations": [...], "filters": {...}}`).
     - Set `result_label` if provided.
     - Set `grain` (e.g., `"one row per (HauptLieferant)"`).
     - Set `source_row_count` from parent metadata.
   - **Per-column metadata**:
     - Set `origin` (`group_key`, `aggregate`, `passthrough`, `derived`).
     - For `aggregate` columns: set `operation` and `source_column`.
     - For `passthrough` / `group_key` columns: set `source_column`.
   - Update `_store_result_from_df` or create a new helper to accept these provenance details and attach them to the `ColumnInfo` objects before storing.

## Files to modify
- `src/alphaplan_mcp/query_engine.py`
- `src/alphaplan_mcp/result_store.py` (if needed to pass through new metadata fields)
