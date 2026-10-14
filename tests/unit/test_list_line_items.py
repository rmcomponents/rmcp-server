import pytest
import pyarrow as pa
from unittest.mock import patch, MagicMock
from alphaplan_mcp.tools.documents import list_beleg_positionen, _price_summary
from alphaplan_mcp.errors import ToolError

@pytest.mark.asyncio
async def test_list_line_items_missing_params():
    with pytest.raises(ToolError):
        await list_beleg_positionen(artikel_nummer="", beleg_typ="AR", date_from="2025-01-01", date_to="2025-12-31")
    with pytest.raises(ToolError):
        await list_beleg_positionen(artikel_nummer="A123", beleg_typ="AR", date_from=None, date_to="2025-12-31")
    with pytest.raises(ToolError):
        await list_beleg_positionen(artikel_nummer="A123", beleg_typ="AR", date_from="2025-01-01", date_to=None)
    with pytest.raises(ToolError):
        await list_beleg_positionen(artikel_nummer="A123", beleg_typ="", date_from="2025-01-01", date_to="2025-12-31")

@pytest.mark.asyncio
async def test_list_line_items_invalid_document_type():
    with pytest.raises(ToolError) as exc_info:
        await list_beleg_positionen(artikel_nummer="A123", beleg_typ="XX", date_from="2025-01-01", date_to="2025-12-31")
    assert exc_info.value.code == "invalid_parameters"
    assert "available_document_types" in exc_info.value.details
    assert "EB" in exc_info.value.details["available_document_types"]

def _batch(rows):
    return pa.RecordBatch.from_pylist(rows)

_ROW_BASE = {
    "ArtikelNummer": "A123", "BelegNummer": "ER1", "KundenNummer": 4711,
    "KundeOderLieferant": "Supplier A",
    "KundeKlassifizierung": "S", "KundeBranche": "Industrial", "Menge": 100,
    "MengeOffen": 0, "NettoPreisStueckEUR": 0.05, "BelegDatum": "2025-01-14 15:06:44.000",
    "LieferDatum": "2025-02-01 00:00:00.000", "LieferDatumBestaetigt": None,
    "OriginalDatum": "2025-01-20 00:00:00.000", "BelegKursFaktor": 0.006,
    "BelegWaehrung": "JPY", "IstArchiviert": 0,
}

@pytest.mark.asyncio
async def test_list_line_items_store_result_with_summary():
    row1 = dict(_ROW_BASE, BelegNummer="ER1", NettoPreisStueckEUR=0.03, IstArchiviert=1)
    row2 = dict(_ROW_BASE, BelegNummer="ER2", NettoPreisStueckEUR=0.05, IstArchiviert=0)
    rows = [row1, row2]

    async def mock_stream(*args, **kwargs):
        yield _batch(rows)

    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = MagicMock(store_result_stream=None)

    async def consume(session_id, provenance, batches, **kwargs):
        captured["provenance"] = provenance
        async for _ in batches:
            pass
        return {"result_id": "rs_123"}
    captured = {}
    mock_store.store_result_stream = consume
    mock_ctx.fastmcp.result_store = mock_store

    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        result = await list_beleg_positionen(
            artikel_nummer="A123",
            beleg_typ="er",
            date_from="2025-01-01",
            date_to="2025-12-31",
            ctx=mock_ctx,
        )
    assert isinstance(result, dict)
    assert result["result_id"] == "rs_123"
    assert captured["provenance"].op == "list_beleg_positionen"
    assert captured["provenance"].parameters["beleg_typ_id"] == 10
    ps = result["price_summary"]
    assert ps["min"] == 0.03
    assert ps["max"] == 0.05
    assert ps["average"] == 0.04
    assert ps["median"] == 0.04
    assert ps["count"] == 2
    assert result["archived_summary"] == {"archived": 1, "non_archived": 1}

@pytest.mark.asyncio
async def test_list_line_items_resolves_eb_to_9():
    async def mock_stream(*args, **kwargs):
        captured["beleg_typ_id"] = kwargs.get("beleg_typ_id")
        return
        yield  # pragma: no cover

    captured = {}
    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = MagicMock(store_result_stream=None)

    async def consume(session_id, provenance, batches, **kwargs):
        async for _ in batches:
            pass
        return {"result_id": "rs_eb"}
    mock_store.store_result_stream = consume
    mock_ctx.fastmcp.result_store = mock_store

    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        result = await list_beleg_positionen(
            artikel_nummer="A123",
            beleg_typ="EB",
            date_from="2025-01-01",
            date_to="2025-12-31",
            ctx=mock_ctx,
        )
    assert result["result_id"] == "rs_eb"
    assert captured["beleg_typ_id"] == 9


@pytest.mark.parametrize("doc_type,expected_beleg_typ", [
    ("AA", 1),
    ("EA", 8),
])
@pytest.mark.asyncio
async def test_list_line_items_aa_ea_beleg_typ(doc_type, expected_beleg_typ):
    async def mock_stream(*args, **kwargs):
        captured["beleg_typ_id"] = kwargs.get("beleg_typ_id")
        return
        yield  # pragma: no cover

    captured = {}
    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = MagicMock(store_result_stream=None)

    async def consume(session_id, provenance, batches, **kwargs):
        async for _ in batches:
            pass
        return {"result_id": f"rs_{doc_type}"}
    mock_store.store_result_stream = consume
    mock_ctx.fastmcp.result_store = mock_store

    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        result = await list_beleg_positionen(
            artikel_nummer="0402B102J500CT",
            beleg_typ=doc_type,
            date_from="2025-01-01",
            date_to="2025-12-31",
            ctx=mock_ctx,
        )
    assert result["result_id"] == f"rs_{doc_type}"
    assert captured["beleg_typ_id"] == expected_beleg_typ


def _run(captured):
    async def mock_stream(*args, **kwargs):
        captured["include_archived"] = kwargs.get("include_archived")
        return
        yield  # pragma: no cover

    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = MagicMock(store_result_stream=None)

    async def consume(session_id, provenance, batches, **kwargs):
        async for _ in batches:
            pass
        return {"result_id": "rs_arch"}
    mock_store.store_result_stream = consume
    mock_ctx.fastmcp.result_store = mock_store
    return mock_ctx, mock_stream


@pytest.mark.parametrize("doc_type,expected", [
    ("AB", False),
    ("ab", False),
    ("AN", False),
    ("AR", False),
    ("EB", False),
    ("ER", False),
    ("AL", False),
    ("AA", False),
    ("EA", False),
])
@pytest.mark.asyncio
async def test_list_line_items_include_archived_default(doc_type, expected):
    captured = {}
    mock_ctx, mock_stream = _run(captured)
    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        await list_beleg_positionen(
            artikel_nummer="A123",
            beleg_typ=doc_type,
            date_from="2025-01-01",
            date_to="2025-12-31",
            ctx=mock_ctx,
        )
    assert captured["include_archived"] is expected


@pytest.mark.asyncio
async def test_list_line_items_include_archived_explicit_override():
    captured = {}
    mock_ctx, mock_stream = _run(captured)
    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        await list_beleg_positionen(
            artikel_nummer="A123",
            beleg_typ="AR",
            date_from="2025-01-01",
            date_to="2025-12-31",
            include_archived=True,
            ctx=mock_ctx,
        )
    assert captured["include_archived"] is True

    captured2 = {}
    mock_ctx2, mock_stream2 = _run(captured2)
    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream2):
        await list_beleg_positionen(
            artikel_nummer="A123",
            beleg_typ="AB",
            date_from="2025-01-01",
            date_to="2025-12-31",
            include_archived=False,
            ctx=mock_ctx2,
        )
    assert captured2["include_archived"] is False

@pytest.mark.asyncio
async def test_price_summary_median_even_and_odd():
    assert _price_summary([3, 1, 2])["median"] == 2
    assert _price_summary([4, 1, 3, 2])["median"] == 2.5
    assert _price_summary([10]) == {"min": 10, "max": 10, "average": 10, "median": 10, "count": 1}

@pytest.mark.asyncio
async def test_list_line_items_empty_skips_nulls():
    batch = _batch([
        dict(_ROW_BASE, BelegNummer="ER1", NettoPreisStueckEUR=None, LieferDatum=None,
             LieferDatumBestaetigt=None, OriginalDatum=None, IstArchiviert=0),
    ])

    async def mock_stream(*args, **kwargs):
        yield batch

    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"
    mock_store = MagicMock(store_result_stream=None)

    async def consume(session_id, provenance, batches, **kwargs):
        async for _ in batches:
            pass
        return {"result_id": "rs_empty"}
    mock_store.store_result_stream = consume
    mock_ctx.fastmcp.result_store = mock_store

    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        result = await list_beleg_positionen(
            artikel_nummer="A123",
            beleg_typ="AR",
            date_from="2025-01-01",
            date_to="2025-12-31",
            ctx=mock_ctx,
        )
    assert result["price_summary"] == {"min": None, "max": None, "average": None, "median": None, "count": 0}
    assert result["archived_summary"] == {"archived": 0, "non_archived": 1}


# --- filters: customer backlog, open lines, optional dates --------------------------------

def _capture_run():
    captured = {}

    async def mock_stream(*args, **kwargs):
        captured.update(kwargs)
        return
        yield  # pragma: no cover

    mock_ctx = MagicMock()
    mock_ctx.session_id = "test_session"

    async def consume(session_id, provenance, batches, **kwargs):
        captured["provenance"] = provenance
        async for _ in batches:
            pass
        return {"result_id": "rs_x"}
    mock_ctx.fastmcp.result_store.store_result_stream = consume
    return captured, mock_ctx, mock_stream


@pytest.mark.asyncio
async def test_list_line_items_customer_backlog_without_article_or_dates():
    captured, mock_ctx, mock_stream = _capture_run()
    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        result = await list_beleg_positionen(beleg_typ="ab", adress_nummer=" D010187 ",
                                       open_only=True, ctx=mock_ctx)
    assert result["result_id"] == "rs_x"
    assert captured["beleg_typ_id"] == 3
    assert captured["artikel_nummer"] is None
    assert captured["adress_nummer"] == "D010187"
    assert captured["open_only"] is True
    assert captured["include_archived"] is False
    assert captured["date_from"] is None and captured["date_to"] is None
    assert captured["provenance"].parameters["adress_nummer"] == "D010187"
    assert captured["provenance"].parameters["open_only"] is True


@pytest.mark.asyncio
async def test_list_line_items_requires_article_or_customer():
    with pytest.raises(ToolError) as exc_info:
        await list_beleg_positionen(beleg_typ="AB", date_from="2025-01-01", date_to="2025-12-31")
    assert exc_info.value.code == "invalid_parameters"


@pytest.mark.asyncio
async def test_list_line_items_dates_both_or_neither():
    with pytest.raises(ToolError):
        await list_beleg_positionen(artikel_nummer="A1", beleg_typ="AB", date_from="2025-01-01",
                              open_only=True)


@pytest.mark.asyncio
async def test_list_line_items_dates_required_without_open_only():
    with pytest.raises(ToolError):
        await list_beleg_positionen(adress_nummer="D010187", beleg_typ="AB")


@pytest.mark.parametrize("doc_type", ["AN", "AL", "AR", "EA", "ER"])
@pytest.mark.asyncio
async def test_list_line_items_open_only_rejected_for_end_documents(doc_type):
    with pytest.raises(ToolError) as exc_info:
        await list_beleg_positionen(adress_nummer="D010187", beleg_typ=doc_type, open_only=True)
    assert "open_only" in exc_info.value.message


@pytest.mark.parametrize("doc_type", ["AA", "AB", "EB"])
@pytest.mark.asyncio
async def test_list_line_items_open_only_allowed(doc_type):
    captured, mock_ctx, mock_stream = _capture_run()
    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        await list_beleg_positionen(artikel_nummer="A1", beleg_typ=doc_type, open_only=True,
                              ctx=mock_ctx)
    assert captured["open_only"] is True


# --- per-BelegTyp column registry ---------------------------------------------------------

from alphaplan_mcp.database import (
    LINE_ITEM_COLUMNS,
    LineItemColumn,
    _LINE_ITEM_JOINS,
    build_line_items_query,
    fetch_line_items_stream,
    line_item_columns_for,
)
from alphaplan_mcp.tools.documents import DOCUMENT_TYPE_BELEG_TYP

_COMMON_COLUMNS = [
    "ArtikelNummer", "BelegNummer", "PositionsTyp", "KundenNummer", "KundeOderLieferant",
    "KundeKlassifizierung", "KundeBranche", "Menge", "MengeOffen", "NettoPreisStueckEUR",
    "NettoPreisStueck", "NettoPreisPro1000", "Preiseinheit", "BelegDatum", "IstArchiviert",
    "BelegKursFaktor", "BelegWaehrung", "Titel", "Kommission", "ZusatzText1", "ZusatzText2",
    "ArtikelBezeichnung",
]
_AB_ONLY = ["KDWunsch", "IhrAuftrag", "IhrAuftragPos", "KundenArtNr", "Bestand", "Status", "AuftragsTyp"]
_EB_ONLY = ["RequestedDate", "BestaetigtZum"]


def _names(beleg_typ_id):
    return [c.name for c in line_item_columns_for(beleg_typ_id)]


@pytest.mark.parametrize("beleg_typ_id", sorted(DOCUMENT_TYPE_BELEG_TYP.values()))
def test_line_item_registry_common_columns_for_every_type(beleg_typ_id):
    names = _names(beleg_typ_id)
    assert [n for n in names if n in _COMMON_COLUMNS] == _COMMON_COLUMNS
    # price_summary / archived_summary in tools/documents.py depend on these
    assert "NettoPreisStueckEUR" in names and "IstArchiviert" in names


def test_line_item_registry_ab_columns():
    names = _names(3)
    assert all(n in names for n in _AB_ONLY)
    assert "OriginalDatum" not in names  # replaced by KDWunsch
    assert not any(n in names for n in _EB_ONLY)


def test_line_item_registry_eb_columns():
    names = _names(9)
    assert all(n in names for n in _EB_ONLY)
    for replaced in ("LieferDatum", "LieferDatumBestaetigt", "OriginalDatum"):
        assert replaced not in names
    assert not any(n in names for n in _AB_ONLY)


@pytest.mark.parametrize("beleg_typ_id", [1, 2, 4, 5, 8, 10])
def test_line_item_registry_other_types_keep_generic_dates(beleg_typ_id):
    names = _names(beleg_typ_id)
    assert {"LieferDatum", "LieferDatumBestaetigt", "OriginalDatum"} <= set(names)
    assert not any(n in names for n in _AB_ONLY + _EB_ONLY)


def test_line_item_registry_names_unique_and_joins_known():
    names = [c.name for c in LINE_ITEM_COLUMNS]
    assert len(names) == len(set(names))
    for col in LINE_ITEM_COLUMNS:
        assert set(col.joins) <= set(_LINE_ITEM_JOINS), col.name


def test_line_item_registry_ea_title_description():
    titel = next(c for c in LINE_ITEM_COLUMNS if c.name == "Titel")
    assert "HolyStone" in titel.description_for(8)
    assert "HolyStone" not in titel.description_for(9)


def test_build_query_joins_only_when_needed():
    ab = build_line_items_query(3, artikel_nummer="A1", date_from="2025-01-01", date_to="2025-12-31")
    assert "dbo.Lager LG ON LG.LagerID = BP.AbbuchungsLagerID" in ab.sql
    assert "dbo.Artikel ART" in ab.sql
    assert ab.sql.count("dbo.Artikel ART") == 1  # shared by Status and WarenGruppe
    eb = build_line_items_query(9, artikel_nummer="A1", date_from="2025-01-01", date_to="2025-12-31")
    assert "dbo.Lager LG" not in eb.sql  # stock is AB only
    # article group for every type; WarenGruppen depends on Artikel, so it must come after it
    assert eb.sql.index("dbo.Artikel ART") < eb.sql.index("dbo.WarenGruppen WG")


def test_build_query_custom_registry_type_specific_and_overrides():
    columns = (
        LineItemColumn("A", "BP.ArtikelNummer", "a"),
        LineItemColumn("X", "X.Foo", "x", beleg_typen=frozenset({3}), joins=("x", "x")),
        LineItemColumn("T", "B.BelegTitel", "t", type_descriptions={8: "t8"}),
    )
    joins = {"x": "LEFT JOIN Foo X ON X.ID = BP.ID"}
    ab = build_line_items_query(3, artikel_nummer="A1", date_from="d1", date_to="d2",
                                columns=columns, joins=joins)
    assert "X.Foo AS X" in ab.sql and ab.sql.count("LEFT JOIN Foo X") == 1
    ea = build_line_items_query(8, artikel_nummer="A1", date_from="d1", date_to="d2",
                                columns=columns, joins=joins)
    assert "X.Foo" not in ea.sql and "LEFT JOIN Foo" not in ea.sql
    assert [c.name for c in ea.columns] == ["A", "T"]
    assert ea.columns[1].description_for(8) == "t8"


def test_build_query_params_article_and_dates():
    q = build_line_items_query(5, artikel_nummer="A1", date_from="d1", date_to="d2")
    assert q.sql.count("?") == len(q.params) == 10
    assert q.params == [5, "A1"] + ["d1", "d2"] * 4
    assert "B.IstArchiviert = 0" in q.sql
    assert "BP.PositionsTyp = 0" not in q.sql  # text lines included by default
    assert "NochZuUebernehmen > 0" not in q.sql
    assert "A.AdressNummer = ?" not in q.sql


def test_build_query_params_customer_open_no_dates():
    q = build_line_items_query(3, adress_nummer="D010187", open_only=True, include_archived=True)
    assert q.sql.count("?") == len(q.params) == 2
    assert q.params == [3, "D010187"]
    assert "A.AdressNummer = ?" in q.sql
    assert "BP.NochZuUebernehmen > 0" in q.sql
    assert "B.IstArchiviert = 0" not in q.sql
    assert "BP.LieferDatum >= ?" not in q.sql
    assert "BP.ArtikelNummer = ?" not in q.sql


def test_build_query_params_article_and_customer():
    q = build_line_items_query(3, artikel_nummer="A1", adress_nummer="D1", date_from="d1", date_to="d2")
    assert q.params == [3, "A1", "D1"] + ["d1", "d2"] * 4
    assert q.sql.count("?") == len(q.params)


def test_build_query_rejects_half_open_date_range():
    with pytest.raises(ValueError):
        build_line_items_query(3, artikel_nummer="A1", date_from="d1")


def test_price_per_1000_uses_price_unit():
    col = next(c for c in LINE_ITEM_COLUMNS if c.name == "NettoPreisPro1000")
    assert "BP.PENettoPreis * BP.PEsProBE" in col.sql
    assert "BP.NettoPreisEinzel * 1000" in col.sql  # fallback without a price unit


# --- fetcher: schema, transforms, descriptions ---------------------------------------------

import datetime as _dt


class _FakeCursor:
    def __init__(self, description, rows):
        self.description = description
        self._rows = list(rows)
        self.executed = None

    def execute(self, sql, params):
        self.executed = (sql, params)

    def fetchmany(self, n):
        out, self._rows = self._rows[:n], self._rows[n:]
        return out

    def close(self):
        pass


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        pass


@pytest.mark.asyncio
async def test_fetch_line_items_stream_transforms_and_stable_schema(monkeypatch):
    from alphaplan_mcp import database
    q = build_line_items_query(8, artikel_nummer="A1", date_from="d1", date_to="d2")
    pytypes = {"IstArchiviert": bool, "PositionsTyp": int, "BelegDatum": _dt.date, "LieferDatum": _dt.date,
               "LieferDatumBestaetigt": _dt.date, "OriginalDatum": _dt.date}
    numeric = {"Menge", "MengeOffen", "NettoPreisStueckEUR", "NettoPreisStueck",
               "NettoPreisPro1000", "BelegKursFaktor"}
    description = [(c.name, pytypes.get(c.name, float if c.name in numeric else str))
                   for c in q.columns]
    rtf = r"{\rtf1\ansi\ansicpg1252{\fonttbl{\f0 Arial;}}\pard R\'fcckstand\par}"
    row_rtf = tuple(rtf if c.name == "ZusatzText1" else None for c in q.columns)
    row_null = tuple(None for _ in q.columns)
    cursor = _FakeCursor(description, [row_rtf, row_null, row_null])
    monkeypatch.setattr(database, "build_connection", lambda: _FakeConn(cursor))
    monkeypatch.setattr(database.settings, "RESULTSET_CHUNK_ROWS", 1)

    batches = [b async for b in fetch_line_items_stream(beleg_typ_id=8, artikel_nummer="A1",
                                                        date_from="d1", date_to="d2")]
    assert len(batches) == 3
    # the all-null batches keep the declared types, so chunks concatenate on load
    assert len({b.schema for b in batches}) == 1
    table = pa.concat_tables([pa.Table.from_batches([b]) for b in batches])
    assert table.schema.field("ZusatzText1").type == pa.string()
    assert table.column("ZusatzText1").to_pylist() == ["Rückstand", None, None]
    meta = table.schema.field("Titel").metadata
    assert b"HolyStone" in meta[b"description"]
    assert cursor.executed[1] == [8, "A1"] + ["d1", "d2"] * 4


# --- text/comment lines ---------------------------------------------------------------------

def test_build_query_text_lines_switch():
    q = build_line_items_query(3, adress_nummer="D1", open_only=True, include_text_lines=False)
    assert "BP.PositionsTyp = 0" in q.sql
    assert q.sql.count("?") == len(q.params) == 2


def test_build_query_without_any_predicate_is_valid_sql():
    q = build_line_items_query(3, include_archived=True)
    assert "WHERE 1 = 1" in q.sql


@pytest.mark.parametrize("kwargs,expected", [({}, True), ({"include_text_lines": False}, False)])
@pytest.mark.asyncio
async def test_list_line_items_include_text_lines_default_and_override(kwargs, expected):
    captured, mock_ctx, mock_stream = _capture_run()
    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        await list_beleg_positionen(adress_nummer="D010187", beleg_typ="AB", open_only=True,
                              ctx=mock_ctx, **kwargs)
    assert captured["include_text_lines"] is expected
    assert captured["provenance"].parameters["include_text_lines"] is expected


@pytest.mark.asyncio
async def test_price_summary_ignores_text_lines():
    rows = [
        dict(_ROW_BASE, PositionsTyp=0, NettoPreisStueckEUR=0.04),
        dict(_ROW_BASE, PositionsTyp=2, ArtikelNummer="", NettoPreisStueckEUR=0.0),
        dict(_ROW_BASE, PositionsTyp=0, ArtikelNummer="", NettoPreisStueckEUR=0.0),
        dict(_ROW_BASE, PositionsTyp=0, NettoPreisStueckEUR=0.06),
    ]

    async def mock_stream(*args, **kwargs):
        yield _batch(rows)

    _, mock_ctx, _ = _capture_run()
    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        result = await list_beleg_positionen(artikel_nummer="A123", beleg_typ="AB",
                                       date_from="2025-01-01", date_to="2025-12-31", ctx=mock_ctx)
    assert result["price_summary"]["count"] == 2
    assert result["price_summary"]["min"] == 0.04
    assert result["archived_summary"] == {"archived": 0, "non_archived": 4}


# --- dates: date-only output, whole-day range filter ---------------------------------------

_DATE_COLUMNS = {"BelegDatum", "LieferDatum", "LieferDatumBestaetigt", "OriginalDatum",
                 "RequestedDate", "BestaetigtZum", "KDWunsch"}


def test_all_line_item_date_columns_are_dates():
    for col in LINE_ITEM_COLUMNS:
        if col.name in _DATE_COLUMNS:
            assert "CAST(" in col.sql and "AS date)" in col.sql, col.name
            assert "'19010101'" in col.sql, col.name  # 1900 placeholder -> NULL


def test_date_range_includes_whole_last_day():
    q = build_line_items_query(9, artikel_nummer="A1", date_from="2025-12-01", date_to="2025-12-31")
    assert "B.Datum >= CAST(? AS date) AND B.Datum < DATEADD(day, 1, CAST(? AS date))" in q.sql
    assert "<= ?" not in q.sql
    assert q.sql.count("?") == len(q.params) == 10


# --- review fixes: date validation, non-part articles, empty results --------------------------

@pytest.mark.parametrize("date_from,date_to", [
    ("01.01.2026", "2026-12-31"),
    ("2026-01-01", "31.12.2026"),
    ("2026-13-01", "2026-12-31"),
    ("20260101", "20261231"),
])
@pytest.mark.asyncio
async def test_list_line_items_rejects_non_iso_dates(date_from, date_to):
    with pytest.raises(ToolError) as exc_info:
        await list_beleg_positionen(artikel_nummer="A1", beleg_typ="AB",
                                    date_from=date_from, date_to=date_to)
    assert exc_info.value.code == "invalid_parameters"
    assert "YYYY-MM-DD" in exc_info.value.message


@pytest.mark.asyncio
async def test_list_line_items_rejects_reversed_range():
    with pytest.raises(ToolError) as exc_info:
        await list_beleg_positionen(artikel_nummer="A1", beleg_typ="AB",
                                    date_from="2026-12-31", date_to="2026-01-01")
    assert "after" in exc_info.value.message


@pytest.mark.asyncio
async def test_list_line_items_single_day_range_allowed():
    captured, mock_ctx, mock_stream = _capture_run()
    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        await list_beleg_positionen(artikel_nummer="A1", beleg_typ="EB", date_from=" 2025-12-01",
                                    date_to="2025-12-01", ctx=mock_ctx)
    assert captured["date_from"] == captured["date_to"] == "2025-12-01"


@pytest.mark.asyncio
async def test_price_summary_ignores_non_part_article_groups():
    rows = [
        dict(_ROW_BASE, PositionsTyp=0, WarenGruppenNummer="TY_CB standard", NettoPreisStueckEUR=0.085),
        dict(_ROW_BASE, PositionsTyp=0, ArtikelNummer="Frachtkosten / Freight charges",
             WarenGruppenNummer="RMC_Kosten Transport", NettoPreisStueckEUR=25.0),
        dict(_ROW_BASE, PositionsTyp=0, WarenGruppenNummer=None, NettoPreisStueckEUR=0.095),
    ]

    async def mock_stream(*args, **kwargs):
        yield _batch(rows)

    _, mock_ctx, _ = _capture_run()
    with patch('alphaplan_mcp.tools.documents.fetch_line_items_stream', side_effect=mock_stream):
        result = await list_beleg_positionen(artikel_nummer="A123", beleg_typ="EB",
                                             date_from="2026-01-01", date_to="2026-12-31", ctx=mock_ctx)
    assert result["price_summary"]["count"] == 2
    assert result["price_summary"]["max"] == 0.095


@pytest.mark.asyncio
async def test_fetch_line_items_stream_empty_result_keeps_schema(monkeypatch):
    from alphaplan_mcp import database
    q = build_line_items_query(9, artikel_nummer="A1", date_from="d1", date_to="d1")
    pytypes = {"IstArchiviert": bool, "PositionsTyp": int}
    description = [(c.name, pytypes.get(c.name, str)) for c in q.columns]
    cursor = _FakeCursor(description, [])
    monkeypatch.setattr(database, "build_connection", lambda: _FakeConn(cursor))

    batches = [b async for b in fetch_line_items_stream(beleg_typ_id=9, artikel_nummer="A1",
                                                        date_from="d1", date_to="d1")]
    assert len(batches) == 1 and batches[0].num_rows == 0
    assert batches[0].schema.names == [c.name for c in q.columns]


def test_text_columns_are_trimmed_and_nulled():
    for name in ("ArtikelNummer", "KundenNummer", "KundeOderLieferant", "KundeKlassifizierung",
                 "KundeBranche", "BelegWaehrung", "Titel", "Kommission", "WarenGruppe"):
        col = next(c for c in LINE_ITEM_COLUMNS if c.name == name)
        assert col.sql.startswith("NULLIF(LTRIM(RTRIM("), name


def test_eur_per_1000_column():
    col = next(c for c in LINE_ITEM_COLUMNS if c.name == "NettoPreisPro1000EUR")
    assert "B.KursFaktor / NULLIF(B.KursDivisor, 0)" in col.sql
    assert col.beleg_typen is None
