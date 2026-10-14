import datetime as dt

import polars as pl
import pytest

from alphaplan_mcp.errors import ToolError
from alphaplan_mcp.models import FilterClause, FilterGroup
from alphaplan_mcp.query_engine import _build_filter_expr

DF = pl.DataFrame({
    "d": [dt.date(2025, 1, 1), dt.date(2025, 6, 1), None],
    "ts": [dt.datetime(2025, 1, 1, 10), dt.datetime(2025, 6, 1), None],
})


def _count(column, operator, value):
    group = FilterGroup(clauses=[FilterClause(column=column, operator=operator, value=value)])
    return DF.filter(_build_filter_expr(group, dict(DF.schema))).height


@pytest.mark.parametrize("operator,value,expected", [
    ("eq", "2025-06-01", 1),
    ("gte", "2025-03-01", 1),
    ("lt", "2025-06-01", 1),
    ("between", ["2025-01-01", "2025-06-01"], 2),
    ("in", ["2025-01-01", "2024-01-01"], 1),
    ("eq", "2025-06-01T15:00:00", 1),  # time part ignored on a date column
])
def test_iso_strings_filter_date_columns(operator, value, expected):
    assert _count("d", operator, value) == expected


@pytest.mark.parametrize("operator,value,expected", [
    ("gte", "2025-01-01", 2),             # date = midnight
    ("gt", "2025-01-01T10:00:00", 1),
    ("eq", "2025-06-01", 1),
    ("lt", "2025-06-01T00:00:00Z", 1),    # timezone suffix dropped
])
def test_iso_strings_filter_datetime_columns(operator, value, expected):
    assert _count("ts", operator, value) == expected


def test_unparseable_date_is_type_mismatch():
    with pytest.raises(ToolError) as exc_info:
        _count("d", "eq", "01.06.2025")
    assert exc_info.value.code == "TYPE_MISMATCH"
