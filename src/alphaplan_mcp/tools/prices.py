"""Sales-price tool (AlphaPlan `VerkaufsPreise`, one price list per address)."""

import asyncio
import base64
import csv
import hashlib
import io
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastmcp import Context

from ..config import settings
from ..database import fetch_sales_prices
from ..errors import ToolError, tool_error, wrap_db_error

logger = logging.getLogger(__name__)

MAX_MPNS_PER_CALL = 25

SENTINEL_WEBSHOP = "SENTINEL_WEBSHOP"
SENTINEL_LUMINOVO = "SENTINEL_LUMINOVO"


@dataclass(frozen=True)
class _PriceList:
    adress_nummer: str
    # Mirrors the price-sync agent: the Luminovo platform list uses the article's sales
    # packing unit (K_VPE) as its single breakpoint instead of AnzahlBEs.
    breakpoint_from_vpe: bool = False


def _resolve_price_list(adress_nummer: str) -> _PriceList:
    """Sentinel -> configured address (the real numbers live only in Settings)."""
    if adress_nummer == SENTINEL_WEBSHOP:
        return _PriceList(settings.VERKAUFSPREISE_WEBSHOP_ADRESS_NUMMER)
    if adress_nummer == SENTINEL_LUMINOVO:
        return _PriceList(settings.VERKAUFSPREISE_LUMINOVO_ADRESS_NUMMER, breakpoint_from_vpe=True)
    return _PriceList(adress_nummer)


def _breakpoint(row: Dict[str, Any], from_vpe: bool) -> Optional[int]:
    """Quantity breakpoint in pieces; None when the row has no usable breakpoint."""
    vpe = row.get("vpe")
    vpe = int(vpe) if vpe is not None and vpe > 0 else None
    if from_vpe:
        return vpe
    anzahl = row.get("anzahl_bes")
    # AnzahlBEs is in pieces (unit '/1000': Anzahl in thousands); 0 and 1 both mean "from the
    # first piece" (the usual customer-specific price without a quantity tier)
    return max(int(round(anzahl or 0)), 1)


def _date_part(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


async def get_verkaufspreise(
    artikel_nummern: List[str],
    adress_nummer: str,
    output_format: str = "csv",
    ctx: Context = None,
) -> Any:
    """
    Retrieve AlphaPlan sales prices (`VerkaufsPreise`) of one address's price list for up to
    25 article numbers (MPNs).

    Parameters:
    - `artikel_nummern`: List of 1-25 exact article numbers (`ArtikelNummer`, e.g.
      ["CRCW040210K0FKED", "RC0603FR-0710KL"]). Surrounding whitespace is stripped; matching
      is exact, no wildcards.
    - `adress_nummer`: Whose price list to read. Either a customer's `AdressNummer` (as in
      `list_adressen.adress_nummer`, e.g. 'D010187') for the prices agreed **specifically with
      that customer**, or one of the sentinels:
      - `SENTINEL_WEBSHOP`: the webshop price list (our public online-shop prices, tiered by
        quantity).
      - `SENTINEL_LUMINOVO`: the price list published on the Luminovo platform (one price per
        part; the breakpoint is the article's sales packing unit `K_VPE`).
      The sentinels are resolved by the server; pass them literally, never a resolved number.
      An unknown address returns error `UNKNOWN_ADDRESS`.
    - `output_format`: "csv" (default) or "json".

    Prices: `unit_price` = `VerkaufsPreise.Netto / 1000` (AlphaPlan stores net prices per 1000
    pieces), net, per piece, in the price row's `currency`. Breakpoints are in pieces
    (`AnzahlBEs`); customer-specific prices usually have a single tier from 1 piece
    (breakpoint 1; a stored 0 is reported as 1).
    Only prices valid today are returned: customer price lists keep expired prices
    (`GueltigVon`/`GueltigBis`), and those are ignored, as are rows with a non-positive price.
    If two valid rows overlap at the same breakpoint, the one valid from the latest date wins.

    Not included: the general price-level prices (`PreisStufe`, the bulk of AlphaPlan's sales
    prices, assigned via `list_adressen.preisstufe_standard`). A customer without
    customer-specific prices for an MPN gets 'no sales prices found' even if a price-level
    price exists.

    Returns for "csv" a dict with:
    - `csv`: columns
      MPN, breakpoint_min, breakpoint_max, unit_price_min, unit_price_max, currency, updated_at, error
      (`currency` = ISO code(s) of the MPN's price rows, ';'-joined if mixed; `updated_at` =
      date of the most recent price change of that MPN (`PreisGeaendertAm`), 'YYYY-MM-DD')
    - `sha256_base64`: Base64(SHA-256(exact UTF-8 bytes of the preceding CSV)), excluding the
      checksum line. Preserve LF line endings and the final newline exactly. Verify before
      using the data.
    For "json": `{"adress_nummer": <as requested>, "results": [...]}` with the same fields per
    MPN plus `tiers`: [{breakpoint, unit_price, currency}] ascending by breakpoint.

    One row per MPN in input order (duplicates once). An MPN without prices in this price list
    still gets a row with empty price columns and `error` = 'no sales prices found'.
    """
    try:
        if not artikel_nummern:
            raise tool_error("INVALID_MPNS", f"Provide between 1 and {MAX_MPNS_PER_CALL} MPNs.")
        if len(artikel_nummern) > MAX_MPNS_PER_CALL:
            raise tool_error(
                "TOO_MANY_MPNS",
                f"At most {MAX_MPNS_PER_CALL} MPNs can be queried in a single call.",
                {"max_mpns": MAX_MPNS_PER_CALL, "received": len(artikel_nummern)},
            )
        if any(not mpn or not mpn.strip() for mpn in artikel_nummern):
            raise tool_error("INVALID_MPNS", "MPN list must not contain empty values.")
        if not adress_nummer or not adress_nummer.strip():
            raise tool_error("INVALID_ARGUMENT", "adress_nummer is required (an AdressNummer or a sentinel).")
        if output_format not in ("csv", "json"):
            raise tool_error("INVALID_FORMAT", "output_format must be 'csv' or 'json'")

        requested_address = adress_nummer.strip()
        price_list = _resolve_price_list(requested_address)
        mpn_order = list(dict.fromkeys(mpn.strip() for mpn in artikel_nummern))
        logger.info("get_verkaufspreise called for adress_nummer=%s, artikel_nummern=%s",
                    requested_address, mpn_order)

        try:
            rows = await asyncio.to_thread(fetch_sales_prices, price_list.adress_nummer, mpn_order)
        except ToolError:
            raise
        except Exception as exc:
            raise wrap_db_error("get_verkaufspreise", exc)
        if rows is None:
            raise tool_error("UNKNOWN_ADDRESS", f"No address '{requested_address}' found.",
                             {"adress_nummer": requested_address})

        tiers: Dict[str, List[Dict[str, Any]]] = {mpn: [] for mpn in mpn_order}
        updated: Dict[str, Optional[date]] = {mpn: None for mpn in mpn_order}
        for row in rows:
            mpn = row["mpn"]
            if mpn not in tiers:
                continue
            breakpoint = _breakpoint(row, price_list.breakpoint_from_vpe)
            if breakpoint is None:
                logger.warning("get_verkaufspreise: no usable breakpoint for %s (VPE=%s)", mpn, row.get("vpe"))
                continue
            if any(t["breakpoint"] == breakpoint for t in tiers[mpn]):
                continue  # rows come newest-valid first per breakpoint; keep that one
            tiers[mpn].append({
                "breakpoint": breakpoint,
                "unit_price": round(float(row["netto_pro_1000"]) / 1000, 8),
                "currency": row["currency"],
            })
            changed = _date_part(row.get("updated_at"))
            if changed is not None and (updated[mpn] is None or changed > updated[mpn]):
                updated[mpn] = changed

        results = []
        for mpn in mpn_order:
            mpn_tiers = sorted(tiers[mpn], key=lambda t: t["breakpoint"])
            if not mpn_tiers:
                results.append({
                    "MPN": mpn, "breakpoint_min": None, "breakpoint_max": None,
                    "unit_price_min": None, "unit_price_max": None, "currency": None,
                    "updated_at": None, "error": "no sales prices found", "tiers": [],
                })
                continue
            breakpoints = [t["breakpoint"] for t in mpn_tiers]
            prices = [t["unit_price"] for t in mpn_tiers]
            currencies = list(dict.fromkeys(t["currency"] for t in mpn_tiers))
            results.append({
                "MPN": mpn,
                "breakpoint_min": min(breakpoints),
                "breakpoint_max": max(breakpoints),
                "unit_price_min": min(prices),
                "unit_price_max": max(prices),
                "currency": ";".join(currencies),
                "updated_at": updated[mpn].isoformat() if updated[mpn] else None,
                "error": None,
                "tiers": mpn_tiers,
            })

        if output_format == "json":
            return {"adress_nummer": requested_address, "results": results}
        csv_data = _prices_to_csv(results)
        return {
            "csv": csv_data,
            "sha256_base64": base64.b64encode(
                hashlib.sha256(csv_data.encode("utf-8")).digest()
            ).decode("ascii"),
        }
    except ToolError as e:
        logger.error("ToolError in get_verkaufspreise: %s", e.message)
        return e.to_dict()


_CSV_COLUMNS = ["MPN", "breakpoint_min", "breakpoint_max", "unit_price_min", "unit_price_max",
                "currency", "updated_at", "error"]


def _prices_to_csv(results: List[Dict[str, Any]]) -> str:
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(_CSV_COLUMNS)
    for row in results:
        writer.writerow(["" if row[c] is None else row[c] for c in _CSV_COLUMNS])
    return output.getvalue()
