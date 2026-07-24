# Step 1: Update Pydantic Models for Provenance and Metadata

## Objective
Extend the Pydantic models in `src/alphaplan_mcp/models.py` to support the new provenance fields, column origins, and structured source objects as defined in `agent.md`.

## Changes
1. **`ResultSetMetadata` and `ResultSetSummary`**:
   - Add `source: Optional[Dict[str, Any]] = None` to hold the structured source object (e.g., `{"op": "group_by", "parent": "...", "group_columns": [...], ...}`).
   - Add `grain: Optional[str] = None` to describe the grain of the result set (e.g., `"one row per (HauptLieferant)"`).
   - Add `source_row_count: Optional[int] = None` to track the row count of the parent set.
   - Add `result_label: Optional[str] = None` to store the caller-provided label.

2. **`ColumnInfo`**:
   - Ensure `origin: ColumnOrigin` is present (already exists).
   - Ensure `operation: Optional[str] = None` is present (already exists).
   - Ensure `source_column: Optional[str] = None` is present (already exists).

## Files to modify
- `src/alphaplan_mcp/models.py`
