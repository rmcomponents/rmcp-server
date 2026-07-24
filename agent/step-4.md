# Step 4: Implement Type and Description Inheritance

## Objective
Ensure passthrough columns inherit type and description from the parent set, and computed columns are typed correctly. Fix the `large_string` regression.

## Changes
1. **`src/alphaplan_mcp/query_engine.py`**:
   - When materializing a derived set, fetch the parent metadata using `self.store.get_metadata(parent_result_id)`.
   - **Passthrough columns** (group keys, projected columns):
     - Inherit `type` and `description` from the parent `ColumnInfo`.
     - Ensure `string` stays `string` (not `large_string`).
   - **Computed columns**:
     - `count` -> `int64`
     - `avg` / `mean` -> `float64`
     - `sum` -> numeric matching source
     - `min` / `max` -> source column type
     - `bin_index` -> `int64`; `bin_lower` / `bin_upper` -> source type; `bin_label` -> `string`; `is_other` -> `bool`
   - Update the `ColumnInfo` objects for the new result set accordingly before storing.

## Files to modify
- `src/alphaplan_mcp/query_engine.py`
