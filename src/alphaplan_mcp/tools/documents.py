"""Document line-item tool."""

import logging
from typing import Any, Dict, List, Union

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


async def list_line_items(
    artikel_nummer: str,
    document_type: str,
    date_from: str,
    date_to: str,
    include_archived: bool = None,
    ctx: Context = None,
) -> Union[Dict[str, Any], ResultSetSummary]:
    """
    Fetch all line items for a single part number within a date range, from documents of one type.

    Store the rows as a result set and return a summary plus a quick price summary.
    No rows are returned to the model.

    Parameters:
    - `artikel_nummer`: Mandatory. The exact article/part number (e.g. 'UMK325AB7106KMHL').
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
    - `date_from`: Mandatory. Inclusive start of the date range (e.g. '2025-01-01').
    - `date_to`: Mandatory. Inclusive end of the date range (e.g. '2025-12-31').
    - `include_archived`: Optional. Whether documents that have been archived
      (`B.IstArchiviert = 1`) should be included. When omitted (None) it defaults to `true` for
      `AB` (Auftrag/Order) documents and `false` for all other types. Order processing archives
      fulfilled orders; some orders may effectively be "closed" by archiving, so include archived
      orders when you need the full history. Pass `false` to see only active (non-archived)
      documents, or `true` to force including archived ones for any type.

    A row is returned when ANY of the line item's four date fields falls within the inclusive
    range `[date_from, date_to]`: the document date, the delivery date, the confirmed delivery
    date, or the original/requested delivery date. Each row is one line item and carries:
    - the counterparty (`KundeOderLieferant`) plus its number (`KundenNummer`) and its
      classification and industry sector
      (customer for outgoing document types, supplier for `EA`/`EB`/`ER`),
    - the net unit price converted to EUR (`NettoPreisStueckEUR`); the document's original
      currency and exchange-rate factor are kept in `BelegWaehrung` / `BelegKursFaktor`,
    - `Menge` - the quantity (VEAnzahl) and `MengeOffen` - the open/remaining quantity still to
      be taken over (`NochZuUebernehmen`),
    - `BelegDatum` - date of the document (`B.Datum`); also one of the four date fields the
      range filter matches on,
    - `LieferDatum` - the delivery date; for `EB` orders this is the requested delivery date,
    - `LieferDatumBestaetigt` - the confirmed delivery date; only populated for `EB`, often empty,
    - `OriginalDatum` - the original/requested delivery date (`AnlieferungsDatumWunsch`),
    - `IstArchiviert` - whether the document is archived (1 = archived, 0 = not archived).

    The result set is stored in Redis and managed identically to `list_customers`. The response
    additionally contains a `price_summary` with the `min`, `max`, `average`, and `median` of
    `NettoPreisStueckEUR` across all fetched line items, plus the `count` of price values, and an
    `archived_summary` with the count of archived (`IstArchiviert = 1`) and non-archived
    (`IstArchiviert = 0`) line items across the fetched rows. When no line items match, the stored
    result set is empty and all `price_summary` / `archived_summary` values are null or zero.
    """
    if not artikel_nummer or not artikel_nummer.strip():
        raise ToolError(
            code="invalid_parameters",
            message="The 'artikel_nummer' parameter is mandatory and must not be empty.",
        )
    if not date_from or not date_to:
        raise ToolError(
            code="invalid_parameters",
            message="The 'date_from' and 'date_to' parameters are mandatory.",
        )
    if not document_type:
        raise ToolError(
            code="invalid_parameters",
            message="The 'document_type' parameter is mandatory. Choose one of: "
                    + ", ".join(DOCUMENT_TYPE_BELEG_TYP),
        )
    beleg_typ = DOCUMENT_TYPE_BELEG_TYP.get(document_type.upper())
    if beleg_typ is None:
        raise ToolError(
            code="invalid_parameters",
            message=(
                f"Unknown document_type '{document_type}'. Choose one of: "
                + ", ".join(f"{code} ({hint})" for code, hint in _DOCUMENT_TYPE_HINTS.items())
            ),
            details={"available_document_types": _DOCUMENT_TYPE_HINTS},
        )

    if include_archived is None:
        include_archived = document_type.upper() == "AB"

    store: ResultSetStore = ctx.fastmcp.result_store

    logger.info(
        "list_line_items called with artikel_nummer=%s, document_type=%s, date_from=%s, date_to=%s, include_archived=%s",
        artikel_nummer, document_type, date_from, date_to, include_archived
    )

    prices: List[float] = []
    archived_count = 0
    non_archived_count = 0

    async def _teed_batches():
        nonlocal archived_count, non_archived_count
        async for batch in fetch_line_items_stream(
            artikel_nummer=artikel_nummer,
            beleg_typ=beleg_typ,
            date_from=date_from,
            date_to=date_to,
            include_archived=include_archived,
        ):
            price_values = batch.column("NettoPreisStueckEUR").to_pylist()
            prices.extend(value for value in price_values if value is not None)
            archived_values = batch.column("IstArchiviert").to_pylist()
            archived_count += sum(1 for v in archived_values if v == 1)
            non_archived_count += sum(1 for v in archived_values if v == 0)
            yield batch

    try:
        provenance = ResultProvenance(
            op="list_line_items",
            parameters={
                "artikel_nummer": artikel_nummer,
                "document_type": document_type.upper(),
                "beleg_typ": beleg_typ,
                "date_from": date_from,
                "date_to": date_to,
                "include_archived": include_archived,
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
