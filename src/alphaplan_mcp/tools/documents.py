"""Document line-item tool."""

import logging
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


# Document types whose open quantity (NochZuUebernehmen) is meaningful; for end documents it
# never decreases, so `nur_offen` would return everything.
_OPEN_QUANTITY_DOCUMENT_TYPES = ("AA", "AB", "EB")


async def list_line_items(
    artikel_nummer: Optional[str] = None,
    document_type: str = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    include_archived: bool = False,
    kunden_nummer: Optional[str] = None,
    nur_offen: bool = False,
    include_text_lines: bool = True,
    ctx: Context = None,
) -> Union[Dict[str, Any], ResultSetSummary]:
    """
    Fetch document line items of one document type - for one part number, for one customer /
    supplier, or both - store them as a result set and return a summary plus a quick price
    summary. No rows are returned to the model.

    Typical uses:
    - price/history of one part: `artikel_nummer` + `document_type` + `date_from`/`date_to`.
    - customer backlog (all open order lines of a customer, no part list needed):
      `document_type='AB'`, `kunden_nummer='D010187'`, `nur_offen=true` (dates optional).

    Parameters:
    - `artikel_nummer`: Optional. The exact article/part number (e.g. 'UMK325AB7106KMHL').
      At least one of `artikel_nummer` / `kunden_nummer` is required.
    - `document_type`: Mandatory. The two-letter document type code. Pick the one that matches
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
    - `date_from` / `date_to`: Inclusive date range in whole days (e.g. '2025-01-01' /
      '2025-12-31'; everything dated 2025-12-31 is included). Give both or neither. Required
      unless `nur_offen` is true. A row matches when ANY of the line's four date fields falls
      into the range: the document date, the delivery date, the confirmed delivery date, or the
      original/requested delivery date.
    - `kunden_nummer`: Optional. Exact address number of the counterparty (customer for sales
      documents, supplier for EA/EB/ER), e.g. 'D010187'. Use `list_customers` to look it up.
    - `nur_offen`: Optional, default false. Only lines with an open quantity
      (`MengeOffen` > 0). Only allowed for `AA`, `AB` and `EB` - for the other types the open
      quantity is not maintained.
    - `include_archived`: Optional, default false for every document type. Archived documents
      (`B.IstArchiviert = 1`) are excluded unless this is true. Order processing archives
      fulfilled orders, so pass true when you need the full order history.
    - `include_text_lines`: Optional, default true. Whether text/comment lines
      (`PositionsTyp` != 0) are included. They have no article number (so an `artikel_nummer`
      filter never matches them), quantity, open quantity and price 0, and their text in
      `ArtikelBezeichnung`. Pass false to get article lines only.

    Columns present for every document type:
    - `ArtikelNummer`, `BelegNummer`, `PositionsTyp` (0 = article line, 2 = text line),
      `ArtikelBezeichnung` (line description / comment text, plain text),
    - the counterparty: `KundenNummer`, `KundeOderLieferant`, `KundeKlassifizierung`,
      `KundeBranche` (customer for outgoing types, supplier for `EA`/`EB`/`ER`),
    - `Menge` (quantity) and `MengeOffen` (open quantity, `NochZuUebernehmen`),
    - prices: `NettoPreisStueckEUR` (per piece, converted to EUR), `NettoPreisStueck` (per piece,
      document currency), `NettoPreisPro1000` (per 1000 pieces, document currency, computed
      from the line's price unit - use this one for exact purchase-price comparisons),
      `Preiseinheit` (e.g. '/1000'), `BelegWaehrung` and `BelegKursFaktor`,
    - `BelegDatum`, `IstArchiviert` (1 = archived),
    - `Titel` (BelegTitel; for EA to HolyStone/Walsin the end customer), `Kommission`,
    - `ZusatzText1`, `ZusatzText2`: line texts as plain text (RTF removed). Free text without
      a fixed format; for CCTC supplier lines ZusatzText1 holds the landed cost
      ('Landed Cost Price: 13,47 $ / 1000'). Other rows may hold unrelated notes or garbage.
    Date columns by type. All dates are plain dates without a time of day ('YYYY-MM-DD'), and
    AlphaPlan's 1900-01-01 "no date" placeholder is returned as null:
    - `EB`: `RequestedDate` (requested delivery date) and `BestaetigtZum` (date confirmed by
      the supplier, as in F10; often empty).
    - `AB`: `LieferDatum`, `LieferDatumBestaetigt` and `KDWunsch` (customer's requested date).
    - all other types: `LieferDatum`, `LieferDatumBestaetigt`, `OriginalDatum`.
    Additional `AB` columns (customer backlog matching): `IhrAuftrag` (customer PO number),
    `IhrAuftragPos` (customer PO line), `KundenArtNr` (customer part number / CPN), `Bestand`
    (current stock of the issuing warehouse), `Status` (article supply status rot/gelb/grün),
    `AuftragsTyp` (Rahmen / Lieferplan / ...).
    Use `get_result_info` / `explain_columns` on the returned `result_id` for the exact set
    and per-column descriptions.

    The response additionally contains a `price_summary` with the `min`, `max`, `average`,
    and `median` of `NettoPreisStueckEUR` across all fetched article lines with an article
    number (text lines and empty lines are left out because their price is always 0), plus the
    `count` of
    price values, and an `archived_summary` with the count of archived (`IstArchiviert = 1`)
    and non-archived (`IstArchiviert = 0`) line items across the fetched rows. When no line
    items match, the stored result set is empty and all `price_summary` / `archived_summary`
    values are null or zero.
    """
    artikel_nummer = artikel_nummer.strip() if artikel_nummer and artikel_nummer.strip() else None
    kunden_nummer = kunden_nummer.strip() if kunden_nummer and kunden_nummer.strip() else None
    date_from = date_from or None
    date_to = date_to or None

    if not document_type:
        raise ToolError(
            code="invalid_parameters",
            message="The 'document_type' parameter is mandatory. Choose one of: "
                    + ", ".join(DOCUMENT_TYPE_BELEG_TYP),
        )
    document_type = document_type.strip().upper()
    beleg_typ = DOCUMENT_TYPE_BELEG_TYP.get(document_type)
    if beleg_typ is None:
        raise ToolError(
            code="invalid_parameters",
            message=(
                f"Unknown document_type '{document_type}'. Choose one of: "
                + ", ".join(f"{code} ({hint})" for code, hint in _DOCUMENT_TYPE_HINTS.items())
            ),
            details={"available_document_types": _DOCUMENT_TYPE_HINTS},
        )
    if artikel_nummer is None and kunden_nummer is None:
        raise ToolError(
            code="invalid_parameters",
            message="Give 'artikel_nummer', 'kunden_nummer', or both.",
        )
    if (date_from is None) != (date_to is None):
        raise ToolError(
            code="invalid_parameters",
            message="Give both 'date_from' and 'date_to', or neither.",
        )
    if nur_offen and document_type not in _OPEN_QUANTITY_DOCUMENT_TYPES:
        raise ToolError(
            code="invalid_parameters",
            message=(
                f"'nur_offen' is only supported for document types "
                f"{', '.join(_OPEN_QUANTITY_DOCUMENT_TYPES)}; for {document_type} the open "
                "quantity is not maintained."
            ),
        )
    if date_from is None and not nur_offen:
        raise ToolError(
            code="invalid_parameters",
            message="The 'date_from' and 'date_to' parameters are mandatory unless 'nur_offen' is true.",
        )
    include_archived = bool(include_archived)
    include_text_lines = bool(include_text_lines)

    store: ResultSetStore = ctx.fastmcp.result_store

    logger.info(
        "list_line_items called with artikel_nummer=%s, kunden_nummer=%s, document_type=%s, "
        "date_from=%s, date_to=%s, include_archived=%s, nur_offen=%s, include_text_lines=%s",
        artikel_nummer, kunden_nummer, document_type, date_from, date_to, include_archived, nur_offen,
        include_text_lines,
    )

    prices: List[float] = []
    archived_count = 0
    non_archived_count = 0

    async def _teed_batches():
        nonlocal archived_count, non_archived_count
        async for batch in fetch_line_items_stream(
            beleg_typ=beleg_typ,
            artikel_nummer=artikel_nummer,
            kunden_nummer=kunden_nummer,
            date_from=date_from,
            date_to=date_to,
            include_archived=include_archived,
            nur_offen=nur_offen,
            include_text_lines=include_text_lines,
        ):
            # Only real article lines count: text lines and empty placeholder lines carry 0.0.
            price_values = batch.column("NettoPreisStueckEUR").to_pylist()
            articles = batch.column("ArtikelNummer").to_pylist()
            if "PositionsTyp" in batch.schema.names:
                line_types = batch.column("PositionsTyp").to_pylist()
            else:
                line_types = [0] * batch.num_rows
            prices.extend(
                value for value, article, line_type in zip(price_values, articles, line_types)
                if value is not None and line_type == 0 and article and article.strip()
            )
            archived_values = batch.column("IstArchiviert").to_pylist()
            archived_count += sum(1 for v in archived_values if v == 1)
            non_archived_count += sum(1 for v in archived_values if v == 0)
            yield batch

    try:
        provenance = ResultProvenance(
            op="list_line_items",
            parameters={
                "artikel_nummer": artikel_nummer,
                "kunden_nummer": kunden_nummer,
                "document_type": document_type,
                "beleg_typ": beleg_typ,
                "date_from": date_from,
                "date_to": date_to,
                "include_archived": include_archived,
                "nur_offen": nur_offen,
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
        logger.error("ToolError in list_line_items: %s", e.message)
        return e.to_dict()


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
