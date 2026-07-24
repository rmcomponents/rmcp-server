# Step 8: Add and Update Unit Tests

## Objective
Add unit tests to cover the new functionality and ensure chaining works transparently.

## Changes
1. **`tests/unit/test_query_engine.py`**:
   - Add tests for `store_result` on `group_by`, `aggregate`, `distinct_values`, `histogram`.
   - Add tests for alias validation (invalid characters, collisions).
   - Add tests for column naming rules.
   - Add tests for type and description inheritance.
   - Add tests for provenance persistence (set-level and per-column).
   - Add tests for `get_result_info` behavior on derived sets (descriptions, `grain`, `source_row_count`).
   - Add chaining tests (e.g., `query_rows` filter on a `group_by` set, `group_by` on a `distinct_values` set).

## Files to modify
- `tests/unit/test_query_engine.py`
