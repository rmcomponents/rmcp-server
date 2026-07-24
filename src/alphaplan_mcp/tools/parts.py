# Implements the list_parts MCP tool.
# Fetches complete parts result sets from SQL Server incrementally
# and stores them in Redis, returning only a compact summary.
"""Parts ingestion tool."""

import logging
from typing import Any, Dict, Optional, Union

from fastmcp import Context

from ..database import fetch_parts_stream
from ..errors import ToolError
from ..models import ResultSetSummary, ResultProvenance
from ..result_store import ResultSetStore

logger = logging.getLogger(__name__)

async def list_parts(
    positive_filter_term: Optional[str] = None,
    negative_filter_term: Optional[str] = "TCC%",
    match_code: Optional[str] = None,
    ctx: Context = None,
) -> Union[ResultSetSummary, Dict[str, Any]]:
    """
    Fetch all matching parts, store as a result set, and return a summary.
    No rows are returned to the model.
    
    Parameters:
    - `positive_filter_term`: Optional. If provided, only fetch parts where `ArtikelNummer` matches this term. Supports `%` as a wildcard (e.g., `ABC%`).
    - `negative_filter_term`: Optional, defaults to 'TCC%'. Excludes parts where `ArtikelNummer` matches this term. Supports `%` as a wildcard.
    - `match_code`: Optional. If provided, filters parts where `MatchCode` contains this string (LIKE '%match_code%').
    
    Filters can be combined. The result set is stored in Redis and managed identically to `list_customers`.
    """
    store: ResultSetStore = ctx.fastmcp.result_store

    logger.info(
        "list_parts called with positive_filter_term=%s, negative_filter_term=%s, match_code=%s",
        positive_filter_term, negative_filter_term, match_code
    )

    try:
        provenance = ResultProvenance(
            op="list_parts",
            parameters={
                "positive_filter_term": positive_filter_term,
                "negative_filter_term": negative_filter_term,
                "match_code": match_code
            }
        )
        summary = await store.store_result_stream(
            session_id=ctx.session_id,
            provenance=provenance,
            batches=fetch_parts_stream(
                positive_filter_term=positive_filter_term,
                negative_filter_term=negative_filter_term,
                match_code=match_code
            ),
        )
        return summary
    except ToolError as e:
        logger.error("ToolError in list_parts: %s", e.message)
        return e.to_dict()
