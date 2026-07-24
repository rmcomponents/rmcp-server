# Step 3: Implement Column Naming and Alias Validation

## Objective
Enforce strict column naming rules and alias validation in `QueryEngine` to prevent generic names and collisions.

## Changes
1. **`src/alphaplan_mcp/query_engine.py`**:
   - **Alias Validation** (`group_by`, `aggregate`):
     - Validate aliases against regex `^[A-Za-z_][A-Za-z0-9_]*$`.
     - Reject if alias contains whitespace/dots/quotes.
     - Reject if alias collides with a group column name or another alias.
     - Raise `ToolError` with a clear message on violation.
   - **Column Naming**:
     - `group_by`: Group columns keep original name. Aggregation columns named by `alias`.
     - `aggregate`: Columns named by `alias`.
     - `distinct_values`: Value column named after the source column. Count column named `count`.
     - `histogram` (numeric/date): Columns `bin_index`, `bin_lower`, `bin_upper`, `bin_label`, `count`.
     - `histogram` (string top-N): Value column named after source, `count`, `is_other` (bool).

## Files to modify
- `src/alphaplan_mcp/query_engine.py`
