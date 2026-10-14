import re

from alphaplan_mcp.database import _CUSTOMER_COLUMN_DESCRIPTIONS, _CUSTOMER_QUERY

_CONDITION_COLUMNS = [
    "zahlungsbedingung", "waehrung", "preisstufe_standard", "preisstufe_vorrangig",
    "versandbedingung", "versandart", "lieferadresse", "lieferadressen_anzahl",
]


def test_customer_query_selects_and_describes_condition_columns():
    selected = set(re.findall(r"\bAS (\w+)", _CUSTOMER_QUERY)) | {"lieferadresse"}
    for name in _CONDITION_COLUMNS:
        assert name in selected, name
        assert _CUSTOMER_COLUMN_DESCRIPTIONS.get(name), name


def test_customer_query_every_column_described():
    for name in set(re.findall(r"\bAS (\w+)", _CUSTOMER_QUERY)):
        assert name in _CUSTOMER_COLUMN_DESCRIPTIONS, name


def test_customer_query_joins_are_outer_and_parameterized():
    assert _CUSTOMER_QUERY.count("?") == 1
    for table in ("ZahlungsBedingungen", "Waehrungen", "VersandBedingungen", "VersandArten"):
        assert re.search(rf"LEFT JOIN ALPHAPLAN_EchtDaten\.dbo\.{table}\b", _CUSTOMER_QUERY), table
    assert "OUTER APPLY" in _CUSTOMER_QUERY
