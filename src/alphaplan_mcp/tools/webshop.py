"""Webshop price checking tool."""

import csv
import io
import logging
from typing import Optional

from fastmcp import Context

from ..database import fetch_webshop_prices_stream
from ..errors import ToolError

logger = logging.getLogger(__name__)


async def get_webshop_prices(
    mpn: str,
    ctx: Context = None,
) -> str:
    """
    Retrieve webshop prices for an exact manufacturer part number as CSV.

    Parameters:
    - `mpn`: Exact manufacturer part number to look up (e.g., "CRCW040210K0FKED").

    Returns a CSV string with the following columns:
    - `mpn`: Manufacturer part number.
    - `quantity_breakpoint`: Minimum quantity for this price tier.
    - `unit_price`: Unit price at the given quantity breakpoint.
    - `updated_at`: Timestamp when this price was last updated.

    Rows are ordered by `quantity_breakpoint` ascending. If no prices are found,
    the CSV contains only the header line.
    """
    if not mpn or not mpn.strip():
        raise ToolError("INVALID_MPN", "The 'mpn' parameter is mandatory.")

    if ctx:
        logger.info("get_webshop_prices called for mpn=%s", mpn)

    try:
        output = io.StringIO()
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(["mpn", "quantity_breakpoint", "unit_price", "updated_at"])

        async for batch in fetch_webshop_prices_stream(mpn=mpn.strip()):
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
        logger.exception("get_webshop_prices failed for mpn=%s", mpn)
        raise ToolError("INTERNAL_ERROR", f"An unexpected error occurred: {e}")
