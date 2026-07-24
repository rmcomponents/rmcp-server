# Step 6: Update `get_result_info` and `explain_columns` Behavior

## Objective
Ensure `get_result_info` and `explain_columns` generate descriptions from provenance and expose `grain` and `source_row_count`.

## Changes
1. **`src/alphaplan_mcp/query_engine.py`**:
   - In `get_result_info` and `explain_columns`:
     - If a derived column's `description` is null, generate it from provenance (e.g., `"group_by aggregation: count per group"`).
     - Include `grain` and `source_row_count` in the returned metadata.
     - Ensure statistics (`min`, `max`, `sum`, etc.) compute over the column's contents regardless of origin.

## Files to modify
- `src/alphaplan_mcp/query_engine.py`
