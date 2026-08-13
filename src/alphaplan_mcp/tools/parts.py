# Implements the list_parts MCP tool.
# Fetches complete parts result sets from SQL Server incrementally
# and stores them in Redis, returning only a compact summary.
"""Parts ingestion tool."""

import logging
from typing import Any, Dict, List, Optional, Union

from fastmcp import Context

from ..database import fetch_parts_stream, fetch_part_stats_stream
from ..errors import ToolError
from ..models import ResultSetSummary, ResultProvenance
from ..result_store import ResultSetStore

logger = logging.getLogger(__name__)

async def list_parts(
    positive_filter_term: Optional[str] = None,
    negative_filter_term: Optional[str] = "TCC%",
    match_code: Optional[str] = None,
    sample_n: Optional[int] = None,
    ctx: Context = None,
) -> Union[ResultSetSummary, Dict[str, Any]]:
    """
    Fetch all matching parts, store as a result set, and return a summary.
    No rows are returned to the model.
    
    Parameters:
    - `positive_filter_term`: Optional. If provided, only fetch parts where `ArtikelNummer` matches this term. Supports `%` as a wildcard (e.g., `ABC%`).
    - `negative_filter_term`: Optional, defaults to 'TCC%'. Excludes parts where `ArtikelNummer` matches this term. Supports `%` as a wildcard. **The default excludes all `TCC%` articles (tens of thousands of rows).** Pass `None` to include them — unless you specifically want the truncated catalog, you very likely do. When the default is applied, the response includes a prominent `warning` noting the exclusion.
    - `match_code`: Optional. If provided, filters parts where `MatchCode` contains this string (LIKE '%match_code%').
    - `sample_n`: Optional. If provided, returns a random sample of up to `sample_n` rows, drawn
      **after** all other filters (positive/negative/match_code) are applied, so it is a sample of
      the filtered population. If omitted, all matching rows are returned.
    
    Filters can be combined. The result set is stored in Redis and managed identically to `list_customers`.

    Note on the default exclusion: with `negative_filter_term` left at its default `'TCC%'`, the catalog is silently truncated to non-`TCC` articles. This is a large, non-obvious cut. Pass `negative_filter_term=None` to fetch the full article master. When the default filter is applied, the returned summary includes a `warning` field stating so.
    """
    store: ResultSetStore = ctx.fastmcp.result_store

    logger.info(
        "list_parts called with positive_filter_term=%s, negative_filter_term=%s, match_code=%s, sample_n=%s",
        positive_filter_term, negative_filter_term, match_code, sample_n
    )

    try:
        provenance = ResultProvenance(
            op="list_parts",
            parameters={
                "positive_filter_term": positive_filter_term,
                "negative_filter_term": negative_filter_term,
                "match_code": match_code,
                "sample_n": sample_n
            }
        )
        summary = await store.store_result_stream(
            session_id=ctx.session_id,
            provenance=provenance,
            batches=fetch_parts_stream(
                positive_filter_term=positive_filter_term,
                negative_filter_term=negative_filter_term,
                match_code=match_code,
                sample_n=sample_n
            ),
        )
        # Rec 4: surface the silently-truncating default prominently instead of hiding it.
        default_exclusion = negative_filter_term is not None and negative_filter_term == "TCC%"
        if default_exclusion:
            out = summary.model_dump() if isinstance(summary, ResultSetSummary) else dict(summary)
            out["warning"] = (
                "The default negative_filter_term 'TCC%' was applied: all ArtikelNummer starting "
                "with TCC (tens of thousands of articles) are EXCLUDED from this result set. "
                "If you intended the full article master, re-run with negative_filter_term=None."
            )
            return out
        return summary
    except ToolError as e:
        logger.error("ToolError in list_parts: %s", e.message)
        return e.to_dict()


async def list_part_stats(
    mpn: Optional[str] = None,
    year: int = None,
    sales_purchasing: Optional[str] = None,
    limit: Optional[int] = None,
    ctx: Context = None,
) -> Union[ResultSetSummary, Dict[str, Any], List[Dict[str, Any]]]:
    """
    Fetch part statistics, either returning a limited number of rows directly or storing as a result set.
    
    Parameters:
    - `mpn`: Optional. If provided, filters parts where `ArtikelNummer` matches this term. Supports `%` as a wildcard.
    - `year`: Mandatory. The year to fetch statistics for (e.g., 2025).
    - `sales_purchasing`: Optional. 'V' for Sales (Verkauf), 'E' for Purchasing (Einkauf). If omitted, returns both.
    - `limit`: Optional. If provided, returns up to this many rows directly in the response. If omitted, stores the full result set in Redis and returns a summary.
    """
    if year is None:
        raise ToolError(code="invalid_parameters", message="The 'year' parameter is mandatory.")

    logger.info(
        "list_part_stats called with mpn=%s, year=%s, sales_purchasing=%s, limit=%s",
        mpn, year, sales_purchasing, limit
    )

    if limit is not None:
        rows = []
        async for batch in fetch_part_stats_stream(mpn=mpn, year=year, sales_purchasing=sales_purchasing, limit=limit):
            rows.extend(batch.to_pylist())
            if len(rows) >= limit:
                return rows[:limit]
        return rows
    else:
        store: ResultSetStore = ctx.fastmcp.result_store
        provenance = ResultProvenance(
            op="list_part_stats",
            parameters={
                "mpn": mpn,
                "year": year,
                "sales_purchasing": sales_purchasing
            }
        )
        summary = await store.store_result_stream(
            session_id=ctx.session_id,
            provenance=provenance,
            batches=fetch_part_stats_stream(mpn=mpn, year=year, sales_purchasing=sales_purchasing),
        )
        return summary
