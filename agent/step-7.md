# Step 7: Update MCP Tool Signatures

## Objective
Expose the new `store_result` and `result_label` parameters in the MCP tool definitions.

## Changes
1. **`src/alphaplan_mcp/tools/result_sets.py`**:
   - Add `store_result: bool = False` and `result_label: Optional[str] = None` to the signatures of `group_by`, `aggregate`, `distinct_values`, and `histogram` tool functions.
   - Pass these parameters to the corresponding `QueryEngine` methods.

## Files to modify
- `src/alphaplan_mcp/tools/result_sets.py`
