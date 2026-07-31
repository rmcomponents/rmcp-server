"""Webshop price checking tool."""

import csv
import io
import logging
from typing import List, Optional

from fastmcp import Context

from ..database import fetch_webshop_prices_stream
from ..errors import ToolError

logger = logging.getLogger(__name__)


async def get_webshop_prices(
    mpns: List[str],
    ctx: Context = None,
) -> str:
    """
    Retrieve webshop prices for up to 10 manufacturer part numbers as CSV.

    Parameters:
    - `mpns`: List of 1-10 exact manufacturer part numbers
      (e.g., ["CRCW040210K0FKED", "RC0603FR-0710KL"]).

    Returns a CSV string with the following columns:
    - `mpn`: Manufacturer part number.
    - `quantity_breakpoint`: Minimum quantity for this price tier.
    - `unit_price`: Unit price at the given quantity breakpoint.
    - `updated_at`: Timestamp when this price was last updated.

    Rows are ordered by `mpn`, then `quantity_breakpoint` ascending.
    If no prices are found, the CSV contains only the header line.
    """
    if not mpns:
        raise ToolError("INVALID_MPNS", "At least one MPN is required. Provide 1 to 10 MPNs.")
    if len(mpns) > 10:
        raise ToolError("TOO_MANY_MPNS", "At most 10 MPNs can be queried in a single call.")
    if any(not mpn or not mpn.strip() for mpn in mpns):
        raise ToolError("INVALID_MPNS", "MPN list must not contain empty values.")

    normalized_mpns = [mpn.strip() for mpn in mpns]

    if ctx:
        logger.info("get_webshop_prices called for mpns=%s", normalized_mpns)

    try:
        output = io.StringIO()
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(["mpn", "quantity_breakpoint", "unit_price", "updated_at"])

        async for batch in fetch_webshop_prices_stream(mpns=normalized_mpns):
            for row in batch.to_pylist():
                writer.writerow([
                    row.get("mpn", ""),
                    row.get("quantity_breakpoint", ""),
                    row.get("unit_price", ""),
                    row.get("updated_at", ""),
                ])

        return output.getvalue()
    except ToolError:
        raise
    except Exception as e:
        logger.exception("get_webshop_prices failed for mpns=%s", normalized_mpns)
        raise ToolError("INTERNAL_ERROR", f"An unexpected error occurred: {e}")
