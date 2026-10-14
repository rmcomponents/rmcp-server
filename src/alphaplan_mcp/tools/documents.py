"""Document line-item tool."""

import logging
import re
from datetime import date
from typing import Any, Dict, List, Optional, Union

from fastmcp import Context

from ..database import fetch_line_items_stream
from ..errors import ToolError
from ..models import ResultSetSummary, ResultProvenance
from ..result_store import ResultSetStore

logger = logging.getLogger(__name__)

# BelegTyp codes (two-letter document type -> numeric BelegTyp in the Belege table).
DOCUMENT_TYPE_BELEG_TYP = {
    "AN": 2,    # Angebot / Quote (Angebotsbelege)
    "AB": 3,    # Auftrag / Order (Auftragsbelege)
    "AL": 4,    # Lieferscheine Ausgang (delivery notes out)
    "AR": 5,    # Rechnung Ausgang (invoices out)
    "AA": 1,    # Verkaufsanfragen (sales inquiries)
    "EA": 8,    # Lieferantenanfragen (requests to suppliers)
    "EB": 9,    # Bestellungen bei unseren Lieferanten (purchase orders) - purchasing prices
    "ER": 10,   # gebuchte Warenzugaenge / Rechnung Eingang (goods receipts / invoices in)
}

_DOCUMENT_TYPE_HINTS = {
    "AN": "Quote/Angebot: offered prices, not binding.",
    "AB": "Order/Auftrag: confirmed customer orders (outgoing).",
    "AL": "Delivery note out (Lieferschein Ausgang).",
    "AR": "Invoice out (Rechnung Ausgang).",
    "AA": "Sales inquiry (Verkaufsanfrage).",
    "EA": "Request for quote to suppliers (Lieferantenanfrage).",
    "EB": "Purchase order to suppliers (Bestellung). Use for purchasing prices.",
    "ER": "Goods receipt / invoice in (Warenzugang / Rechnung Eingang).",
}


# Article groups of internal non-part items (freight, packaging, services, catalogues ...).
_NON_PART_GROUP_PREFIX = "RMC_"
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Document types whose open quantity (NochZuUebernehmen) is meaningful; for end documents it
# never decreases, so `open_only` would return everything.
_OPEN_QUANTITY_DOCUMENT_TYPES = ("AA", "AB", "EB")


async def list_beleg_positionen(
    artikel_nummer: Optional[str] = None,
    beleg_typ: str = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    include_archived: bool = False,
    adress_nummer: Optional[str] = None,
    open_only: bool = False,
    include_text_lines: bool = True,
    ctx: Context = None,
) -> Union[Dict[str, Any], ResultSetSummary]:
    """
    Fetch document line items of one document type - for one part number, for one customer /
    supplier, or both - store them as a result set and return a summary plus a quick price
    summary. No rows are returned to the model.

    Typical uses:
    - price/history of one part: `artikel_nummer` + `beleg_typ` + `date_from`/`date_to`.
    - customer backlog (all open order lines of a customer, no part list needed):
      `beleg_typ='AB'`, `adress_nummer='D010187'`, `open_only=true` (dates optional).

    Parameters:
    - `artikel_nummer`: Optional. The exact article/part number as stored in AlphaPlan (`ArtikelNummer`,
      e.g. 'UMK325AB7106KMHL').
      At least one of `artikel_nummer` / `adress_nummer` is required.
    - `beleg_typ`: Mandatory. The two-letter document type code. Pick the one that matches
      the documents you need:
      - `AN` (2) - Angebot/Quote: quotations/offers, prices are not binding.
      - `AB` (3) - Auftrag/Order: confirmed customer orders (outgoing).
      - `AL` (4) - Lieferschein Ausgang: outgoing delivery notes.
      - `AR` (5) - Rechnung Ausgang: outgoing invoices.
      - `AA` (1) - Verkaufsanfrage: sales inquiries (requests from customers for quotes).
      - `EA` (8) - Lieferantenanfrage: requests for quotes sent to suppliers.
      - `EB` (9) - Bestellung: purchase orders placed with suppliers. **Use this one for
        purchasing prices.**
      - `ER` (10) - Warenzugang / Rechnung Eingang: posted goods receipts / incoming invoices.
    - `date_from` / `date_to`: Inclusive date range in whole days, ISO format 'YYYY-MM-DD'
      (e.g. '2025-01-01' / '2025-12-31'; everything dated 2025-12-31 is included). Give both
      or neither; `date_from` must not be after `date_to`. Required
      unless `open_only` is true. A row matches when ANY of the line's four date fields falls
      into the range: the document date, the delivery date, the confirmed delivery date, or the
      original/requested delivery date.
    - `adress_nummer`: Optional. Exact address number of the counterparty (customer for sales
      documents, supplier for EA/EB/ER), e.g. 'D010187' - the `KundenNummer` column of the result,
      `adress_nummer` in `list_adressen`.
    - `open_only`: Optional, default false. Only lines with an open quantity
      (`MengeOffen` > 0). Only allowed for `AA`, `AB` and `EB` - for the other types the open
      quantity is not maintained. Combined with the default `include_archived=false`: open
      lines on archived documents exist (old orders archived with a remaining quantity) and are
      only included with `include_archived=true`.
    - `include_archived`: Optional, default false for every document type. Archived documents
      (`B.IstArchiviert = 1`) are excluded unless this is true. Order processing archives
      fulfilled orders, so pass true when you need the full order history.
    - `include_text_lines`: Optional, default true. Whether text/comment lines
      (`PositionsTyp` != 0) are included. They have no article number (so an `artikel_nummer`
      filter never matches them), quantity, open quantity and price 0, and their text in
      `ArtikelBezeichnung`. Pass false to get article lines only.

    Columns present for every document type:
    - `ArtikelNummer`, `BelegNummer`, `PositionsTyp` (0 = article line, 2 = text line),
      `ArtikelBezeichnung` (line description / comment text, plain text), `WarenGruppe` /
      `WarenGruppenNummer` (article group; 'RMC_' groups are internal non-parts such as
      freight, packaging or services),
    - the counterparty: `KundenNummer`, `KundeOderLieferant`, `KundeKlassifizierung`,
      `KundeBranche` (customer for outgoing types, supplier for `EA`/`EB`/`ER`),
    - `Menge` (quantity) and `MengeOffen` (open quantity, `NochZuUebernehmen`),
    - prices: `NettoPreisStueckEUR` (per piece, converted to EUR), `NettoPreisStueck` (per piece,
      document currency), `NettoPreisPro1000` (per 1000 pieces, document currency, computed
      from the line's price unit - use this one for exact purchase-price comparisons),
      `NettoPreisPro1000EUR` (per 1000 pieces in EUR - best for comparing across currencies),
      `Preiseinheit` ('/1000', rarely '/100', 'x' = per piece), `BelegWaehrung` and
      `BelegKursFaktor` (document currency x factor = EUR),
    - `BelegDatum`, `IstArchiviert` (1 = archived),
    - `Titel` (BelegTitel; meaning depends on the type: EA to HolyStone/Walsin = end customer,
      AN = end customer, EB = order type, AB = e.g. 'EDI' or empty), `Kommission`,
    - `ZusatzText1`, `ZusatzText2`: line texts as plain text (RTF removed). Free text without
      a fixed format; for CCTC supplier lines ZusatzText1 holds the landed cost
      ('Landed Cost Price: 13,47 $ / 1000'); for AN, ZusatzText1 is the quoted lead time and
      ZusatzText2 the quote status (e.g. 'nominiert', 'zu teuer', 'kein Feedback'). Other rows
      may hold unrelated notes or garbage.
    Text columns are trimmed; empty texts are returned as null.
    Date columns by type. All dates are plain dates without a time of day ('YYYY-MM-DD'), and
    AlphaPlan's 1900-01-01 "no date" placeholder is returned as null. 7777-01-01 and 9999-xx-xx
    are deliberate placeholders used by the team and are passed through unchanged - do not
    read them as real dates:
    - `EB`: `RequestedDate` (requested delivery date) and `BestaetigtZum` (date confirmed by
      the supplier, as in F10; often empty).
    - `AB`: `LieferDatum`, `LieferDatumBestaetigt` and `KDWunsch` (customer's requested date).
    - all other types: `LieferDatum`, `LieferDatumBestaetigt`, `OriginalDatum`.
    Additional `AB` columns (customer backlog matching): `IhrAuftrag` (customer PO number),
    `IhrAuftragPos` (customer PO line), `KundenArtNr` (customer part number / CPN), `Bestand`
    (current stock of the issuing warehouse), `Status` (article supply status rot/gelb/grün),
    `AuftragsTyp` (Rahmen / Lieferplan / ForeCast / ...). `IhrAuftrag` + `IhrAuftragPos` is
    the matching key for real orders; forecast lines (AuftragsTyp 'ForeCast') all carry
    'Forecast' / 'FC' there and must be matched via `KundenArtNr` + `KDWunsch` instead.
    Use `get_result_info` / `explain_columns` on the returned `result_id` for the exact set
    and per-column descriptions.

    The response additionally contains a `price_summary` with the `min`, `max`, `average`,
    and `median` of `NettoPreisStueckEUR` across all fetched part lines, plus the `count` of
    price values. Left out: text lines and lines without an article number (price always 0)
    and internal non-part articles ('RMC_' article groups: freight, packaging, services ...).
    `archived_summary` counts archived (`IstArchiviert = 1`) and non-archived
    (`IstArchiviert = 0`) line items across the fetched rows. When no line items match, the
    stored result set is empty (it keeps its columns) and all `price_summary` /
    `archived_summary` values are null or zero.
    """
    artikel_nummer = artikel_nummer.strip() if artikel_nummer and artikel_nummer.strip() else None
    adress_nummer = adress_nummer.strip() if adress_nummer and adress_nummer.strip() else None
    date_from = date_from or None
    date_to = date_to or None

    if not beleg_typ:
        raise ToolError(
            code="invalid_parameters",
            message="The 'beleg_typ' parameter is mandatory. Choose one of: "
                    + ", ".join(DOCUMENT_TYPE_BELEG_TYP),
        )
    beleg_typ = beleg_typ.strip().upper()
    beleg_typ_id = DOCUMENT_TYPE_BELEG_TYP.get(beleg_typ)
    if beleg_typ_id is None:
        raise ToolError(
            code="invalid_parameters",
            message=(
                f"Unknown beleg_typ '{beleg_typ}'. Choose one of: "
                + ", ".join(f"{code} ({hint})" for code, hint in _DOCUMENT_TYPE_HINTS.items())
            ),
            details={"available_document_types": _DOCUMENT_TYPE_HINTS},
        )
    if artikel_nummer is None and adress_nummer is None:
        raise ToolError(
            code="invalid_parameters",
            message="Give 'artikel_nummer', 'adress_nummer', or both.",
        )
    if (date_from is None) != (date_to is None):
        raise ToolError(
            code="invalid_parameters",
            message="Give both 'date_from' and 'date_to', or neither.",
        )
    if open_only and beleg_typ not in _OPEN_QUANTITY_DOCUMENT_TYPES:
        raise ToolError(
            code="invalid_parameters",
            message=(
                f"'open_only' is only supported for document types "
                f"{', '.join(_OPEN_QUANTITY_DOCUMENT_TYPES)}; for {beleg_typ} the open "
                "quantity is not maintained."
            ),
        )
    if date_from is None and not open_only:
        raise ToolError(
            code="invalid_parameters",
            message="The 'date_from' and 'date_to' parameters are mandatory unless 'open_only' is true.",
        )
    if date_from is not None:
        parsed_from = _parse_iso_date("date_from", date_from)
        parsed_to = _parse_iso_date("date_to", date_to)
        if parsed_from > parsed_to:
            raise ToolError(
                code="invalid_parameters",
                message=f"'date_from' ({date_from}) is after 'date_to' ({date_to}).",
            )
        date_from, date_to = parsed_from.isoformat(), parsed_to.isoformat()
    include_archived = bool(include_archived)
    include_text_lines = bool(include_text_lines)

    store: ResultSetStore = ctx.fastmcp.result_store

    logger.info(
        "list_beleg_positionen called with artikel_nummer=%s, adress_nummer=%s, beleg_typ=%s, "
        "date_from=%s, date_to=%s, include_archived=%s, open_only=%s, include_text_lines=%s",
        artikel_nummer, adress_nummer, beleg_typ, date_from, date_to, include_archived, open_only,
        include_text_lines,
    )

    prices: List[float] = []
    archived_count = 0
    non_archived_count = 0

    async def _teed_batches():
        nonlocal archived_count, non_archived_count
        async for batch in fetch_line_items_stream(
            beleg_typ_id=beleg_typ_id,
            artikel_nummer=artikel_nummer,
            adress_nummer=adress_nummer,
            date_from=date_from,
            date_to=date_to,
            include_archived=include_archived,
            open_only=open_only,
            include_text_lines=include_text_lines,
        ):
            # Only real parts count: text lines and empty lines carry 0.0, and internal non-part
            # articles (freight, packaging, services ... in 'RMC_' article groups) distort it.
            price_values = batch.column("NettoPreisStueckEUR").to_pylist()
            articles = batch.column("ArtikelNummer").to_pylist()
            line_types = _column_or(batch, "PositionsTyp", 0)
            groups = _column_or(batch, "WarenGruppenNummer", None)
            prices.extend(
                value for value, article, line_type, group
                in zip(price_values, articles, line_types, groups)
                if value is not None and line_type == 0 and article and article.strip()
                and not (group or "").startswith(_NON_PART_GROUP_PREFIX)
            )
            archived_values = batch.column("IstArchiviert").to_pylist()
            archived_count += sum(1 for v in archived_values if v == 1)
            non_archived_count += sum(1 for v in archived_values if v == 0)
            yield batch

    try:
        provenance = ResultProvenance(
            op="list_beleg_positionen",
            parameters={
                "artikel_nummer": artikel_nummer,
                "adress_nummer": adress_nummer,
                "beleg_typ": beleg_typ,
                "beleg_typ_id": beleg_typ_id,
                "date_from": date_from,
                "date_to": date_to,
                "include_archived": include_archived,
                "open_only": open_only,
                "include_text_lines": include_text_lines,
            }
        )
        summary = await store.store_result_stream(
            session_id=ctx.session_id,
            provenance=provenance,
            batches=_teed_batches(),
        )
        out = summary.model_dump() if isinstance(summary, ResultSetSummary) else dict(summary)
        out["price_summary"] = _price_summary(prices)
        out["archived_summary"] = {
            "archived": archived_count,
            "non_archived": non_archived_count,
        }
        return out
    except ToolError as e:
        logger.error("ToolError in list_beleg_positionen: %s", e.message)
        return e.to_dict()


def _parse_iso_date(name: str, value: str) -> date:
    """Strict 'YYYY-MM-DD' -> date, else an invalid_parameters error (never reaches SQL Server)."""
    text = value.strip() if isinstance(value, str) else ""
    try:
        if not _ISO_DATE.match(text):
            raise ValueError
        return date.fromisoformat(text)
    except ValueError:
        raise ToolError(
            code="invalid_parameters",
            message=f"'{name}' must be an ISO date 'YYYY-MM-DD' (e.g. '2026-01-31'), got '{value}'.",
        )


def _column_or(batch, name: str, default: Any) -> List[Any]:
    """Values of a batch column, or `default` per row when the column is absent."""
    if name in batch.schema.names:
        return batch.column(name).to_pylist()
    return [default] * batch.num_rows


def _price_summary(prices: List[float]) -> Dict[str, Any]:
    """Compute min/max/average/median of the unit EUR prices (empty-safe)."""
    count = len(prices)
    if count == 0:
        return {"min": None, "max": None, "average": None, "median": None, "count": 0}
    ordered = sorted(prices)
    min_price = ordered[0]
    max_price = ordered[-1]
    average = sum(ordered) / count
    mid = count // 2
    if count % 2 == 1:
        median = ordered[mid]
    else:
        median = (ordered[mid - 1] + ordered[mid]) / 2.0
    return {
        "min": min_price,
        "max": max_price,
        "average": average,
        "median": median,
        "count": count,
    }
