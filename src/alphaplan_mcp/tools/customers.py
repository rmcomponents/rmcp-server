# Implements the list_adressen MCP tool.
# Fetches complete customer result sets from SQL Server incrementally
# and stores them in Redis, returning only a compact summary.
"""Customer ingestion tool."""

import logging
from typing import Any, Dict, Optional, Union

from fastmcp import Context

from ..database import fetch_customers_stream
from ..errors import ToolError
from ..models import ResultSetSummary, ResultProvenance
from ..result_store import ResultSetStore

logger = logging.getLogger(__name__)

async def list_adressen(
    such_name: Optional[str] = None,
    ctx: Context = None,
) -> Union[ResultSetSummary, Dict[str, Any]]:
    """
    Fetch all matching addresses (customers and suppliers - AlphaPlan keeps both in `Adressen`),
    store them as a result set, and return a summary. No rows are returned to the model.
    
    The `such_name` parameter matches the internal search name `SuchName` (returned as
    `name_intern`, e.g. 'Bosch - Murrhardt'), not the full company name (`name` = the main address's name
    lines `Anschriften.Name1`-`Name3` joined, e.g. 'Robert Bosch GmbH'; may include a division or
    location such as 'Siemens AG Smart Infrastructure'). It supports SQL LIKE wildcards:
    - Use `%` to match any sequence of characters (e.g., `%Bosch%`).
    - Use `_` to match a single character.

    Besides the names, classification and responsible employees, each row carries the address's
    master-data conditions (AlphaPlan customer master, tabs "Zahlung" / "Versand"):
    `zahlungsbedingung` (payment terms text), `waehrung` (currency code), `preisstufe_standard`
    and `preisstufe_vorrangig` (standard / priority price level), `versandbedingung` (shipping
    terms / Incoterm text, e.g. 'DAP delivery address Incoterms 2020'), `versandart` (shipping
    method), `lieferadresse` (one-line delivery address if it
    differs from the main address, else null) and `lieferadressen_anzahl` (how many differing
    delivery addresses exist; >1 means `lieferadresse` shows only the standard / first one).
    These are the values stored on the address itself.

    Notes:
    - Every active address matching `such_name` is returned, customers and suppliers alike
      (e.g. 70029 CCTC, 70016 Mouser). For supplier rows the condition columns are the
      supplier's terms (what we pay / how they ship to us).
    - The payment and shipping term texts are in the address's language (German or English),
      e.g. '30. des Folgemonats netto' vs. 'until 30th of the following month net' - filter on
      both or use a pattern.
    - `lieferadresse` is assembled from the stored address; the country is missing when it is
      not maintained there.
    - Text columns are trimmed; empty texts are returned as null.
    """
    store: ResultSetStore = ctx.fastmcp.result_store

    logger.info("list_adressen called with such_name=%s", such_name)

    try:
        provenance = ResultProvenance(
            op="list_adressen",
            parameters={"such_name": such_name}
        )
        summary = await store.store_result_stream(
            session_id=ctx.session_id,
            provenance=provenance,
            batches=fetch_customers_stream(such_name),
        )
        return summary
    except ToolError as e:
        logger.error("ToolError in list_adressen: %s", e.message)
        return e.to_dict()
