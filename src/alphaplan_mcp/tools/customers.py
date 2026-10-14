# Implements the list_customers MCP tool.
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

async def list_customers(
    customer_name: Optional[str] = None,
    ctx: Context = None,
) -> Union[ResultSetSummary, Dict[str, Any]]:
    """
    Fetch all matching customers, store as a result set, and return a summary.
    No rows are returned to the model.
    
    The `customer_name` parameter supports SQL LIKE wildcards:
    - Use `%` to match any sequence of characters (e.g., `%GmbH%`).
    - Use `_` to match a single character.

    Besides name, classification and responsible employees, each row carries the customer's
    master-data conditions (AlphaPlan customer master, tabs "Zahlung" / "Versand"):
    `zahlungsbedingung` (payment terms text), `waehrung` (currency code), `preisstufe_standard`
    and `preisstufe_vorrangig` (standard / priority price level), `versandbedingung` (shipping
    terms / Incoterm text, e.g. 'DAP delivery address Incoterms 2020'), `versandart` (shipping
    method), `lieferadresse` (one-line delivery address if it
    differs from the main address, else null) and `lieferadressen_anzahl` (how many differing
    delivery addresses exist; >1 means `lieferadresse` shows only the standard / first one).
    These are the values stored on the customer itself.
    """
    store: ResultSetStore = ctx.fastmcp.result_store

    logger.info("list_customers called with customer_name=%s", customer_name)

    try:
        provenance = ResultProvenance(
            op="list_customers",
            parameters={"customer_name": customer_name}
        )
        summary = await store.store_result_stream(
            session_id=ctx.session_id,
            provenance=provenance,
            batches=fetch_customers_stream(customer_name),
        )
        return summary
    except ToolError as e:
        logger.error("ToolError in list_customers: %s", e.message)
        return e.to_dict()
