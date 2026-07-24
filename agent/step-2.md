# Step 2: Add `store_result` and `result_label` to QueryEngine Methods

## Objective
Add the `store_result` and `result_label` parameters to the `group_by`, `aggregate`, `distinct_values`, and `histogram` methods in `QueryEngine`. When `store_result` is true, persist the computed table and return a `ResultSetSummary`.

## Changes
1. **`src/alphaplan_mcp/query_engine.py`**:
   - Add `store_result: bool = False` and `result_label: Optional[str] = None` to the signatures of `group_by`, `aggregate`, `distinct_values`, and `histogram`.
   - In each method, when `store_result` is true:
     - Convert the computed Polars DataFrame to a PyArrow table.
     - Store it using `self.store.store_result` (or a new helper method that also handles provenance/column metadata).
     - Return the `ResultSetSummary` object instead of the inline dictionary response.
   - When `store_result` is false, retain current behavior.

## Files to modify
- `src/alphaplan_mcp/query_engine.py`
