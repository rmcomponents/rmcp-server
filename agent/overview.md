# Overview: Derived Result Sets and Provenance Persistence

## Objective
This series of steps implements support for derived result sets (e.g., aggregations, group-bys, histograms) that can be stored in the result store and chained transparently. It introduces
strict provenance tracking, column naming rules, and type inheritance to ensure derived sets are self-describing and robust.

## Approach
1. **Step 1: Update Pydantic Models** - Extend `ResultSetMetadata`, `ResultSetSummary`, and `ColumnInfo` in `models.py` to support new provenance fields (`source`, `grain`,
`source_row_count`, `result_label`).
2. **Step 2: Add `store_result` to QueryEngine** - Add `store_result` and `result_label` parameters to `group_by`, `aggregate`, `distinct_values`, and `histogram` in `query_engine.py` to
allow persisting computed tables.
3. **Step 3: Column Naming and Alias Validation** - Enforce strict alias validation and column naming rules in `query_engine.py` to prevent generic names and collisions.
4. **Step 4: Type and Description Inheritance** - Ensure passthrough columns inherit type and description from the parent set, and computed columns are typed correctly in
`query_engine.py`.
5. **Step 5: Provenance Persistence** - Persist set-level and per-column provenance in `query_engine.py` so derived sets are self-describing.
6. **Step 6: Update `get_result_info` and `explain_columns`** - Update these methods in `query_engine.py` to generate descriptions from provenance and expose `grain` and
`source_row_count`.
7. **Step 7: Update MCP Tool Signatures** - Expose the new `store_result` and `result_label` parameters in the MCP tool definitions in `tools/result_sets.py`.
8. **Step 8: Add and Update Unit Tests** - Add unit tests in `tests/unit/test_query_engine.py` to cover the new functionality, alias validation, provenance persistence, and chaining.
