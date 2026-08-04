"""Webshop price checking tool."""

import base64
import csv
import hashlib
import io
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastmcp import Context

from ..database import fetch_webshop_prices_stream
from ..errors import ToolError

logger = logging.getLogger(__name__)


async def get_webshop_prices(
    mpns: List[str],
    output_format: str = "csv",
    ctx: Context = None,
) -> Any:
    """
    Retrieve webshop prices for up to 25 manufacturer part numbers as CSV.

    Parameters:
    - `mpns`: List of 1-25 exact manufacturer part numbers
      (e.g., ["CRCW040210K0FKED", "RC0603FR-0710KL"]).
    - `output_format`: Output format, either "csv" or "json". Defaults to "csv".

    Returns a dict with:
    - `csv`: a CSV string with columns:
      MPN, breakpoint_min, breakpoint_max, unit_price_min, unit_price_max, updated_at, error
    - `sha256_base64`: Base64(SHA-256(exact UTF-8 bytes of the preceding CSV)),
      excluding the checksum line. Preserve LF line endings and the final newline
      exactly. Verify before using the data.

    One row per MPN is returned in the same order as input.
    If no prices are found, the CSV contains only the header line.
    """
    if not mpns:
        raise ToolError("INVALID_MPNS", "At least one MPN is required. Provide 1 to 25 MPNs.")
    if len(mpns) > 25:
        raise ToolError("TOO_MANY_MPNS", "At most 25 MPNs can be queried in a single call.")
    if any(not mpn or not mpn.strip() for mpn in mpns):
        raise ToolError("INVALID_MPNS", "MPN list must not contain empty values.")
    if output_format not in ("csv", "json"):
        raise ToolError("INVALID_FORMAT", "output_format must be 'csv' or 'json'")

    normalized_mpns = [mpn.strip() for mpn in mpns]

    if ctx:
        logger.info("get_webshop_prices called for mpns=%s", normalized_mpns)

    mpn_order = list(dict.fromkeys(normalized_mpns))
    fetched_at = datetime.now(timezone.utc).isoformat()

    try:
        aggregates = {
            mpn: {"breakpoints": [], "unit_prices": []}
            for mpn in mpn_order
        }

        async for batch in fetch_webshop_prices_stream(mpns=mpn_order):
            for row in batch.to_pylist():
                mpn = str(row.get("mpn", ""))
                if mpn not in aggregates:
                    continue
                bp = row.get("quantity_breakpoint")
                up = row.get("unit_price")
                if bp is not None:
                    aggregates[mpn]["breakpoints"].append(
                        float(bp) if not isinstance(bp, (int, float)) else bp
                    )
                if up is not None:
                    aggregates[mpn]["unit_prices"].append(
                        float(up) if not isinstance(up, (int, float)) else up
                    )

        rows = []
        for mpn in mpn_order:
            agg = aggregates[mpn]
            if not agg["breakpoints"] and not agg["unit_prices"]:
                continue
            rows.append({
                "MPN": mpn,
                "breakpoint_min": min(agg["breakpoints"]) if agg["breakpoints"] else None,
                "breakpoint_max": max(agg["breakpoints"]) if agg["breakpoints"] else None,
                "unit_price_min": min(agg["unit_prices"]) if agg["unit_prices"] else None,
                "unit_price_max": max(agg["unit_prices"]) if agg["unit_prices"] else None,
                "updated_at": fetched_at,
                "error": None,
            })

        if output_format == "json":
            return {"results": rows}
        csv_data = _webshop_rows_to_csv(rows)
        return {
            "csv": csv_data,
            "sha256_base64": base64.b64encode(
                hashlib.sha256(csv_data.encode("utf-8")).digest()
            ).decode("ascii"),
        }
    except ToolError:
        raise
    except Exception as e:
        logger.exception("get_webshop_prices failed for mpns=%s", normalized_mpns)
        raise ToolError("INTERNAL_ERROR", f"An unexpected error occurred: {e}")

def _webshop_rows_to_csv(rows: List[Dict[str, Any]]) -> str:
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([
        "MPN", "breakpoint_min", "breakpoint_max",
        "unit_price_min", "unit_price_max", "updated_at", "error",
    ])
    for row in rows:
        writer.writerow([
            row["MPN"],
            row["breakpoint_min"] if row["breakpoint_min"] is not None else "",
            row["breakpoint_max"] if row["breakpoint_max"] is not None else "",
            row["unit_price_min"] if row["unit_price_min"] is not None else "",
            row["unit_price_max"] if row["unit_price_max"] is not None else "",
            row["updated_at"] or "",
            row.get("error") or "",
        ])
    return output.getvalue()
