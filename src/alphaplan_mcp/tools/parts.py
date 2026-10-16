# Implements the list_artikel MCP tool.
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

async def list_artikel(
    positive_filter_term: Optional[str] = None,
    negative_filter_term: Optional[str] = None,
    match_code: Optional[str] = None,
    sample_n: Optional[int] = None,
    include_archived: Optional[bool] = False,
    ctx: Context = None,
) -> Union[ResultSetSummary, Dict[str, Any]]:
    """
    Fetch all matching parts, store as a result set, and return a summary.
    No rows are returned to the model.
    
    Parameters:
    - `positive_filter_term`: Optional. If provided, only fetch parts where `ArtikelNummer` matches this term. Supports `%` as a wildcard (e.g., `ABC%`).
    - `negative_filter_term`: Optional, defaults to `None` (no exclusion). If provided, excludes parts where `ArtikelNummer` matches this term. Supports `%` as a wildcard. `'TCC%'` excludes the newly created `TCC` articles (about 92 % of the article master); use it only for whole-catalog statistics. **Never combine it with targeted searches** (`positive_filter_term` / `match_code`): it silently drops matching TCC articles (e.g. CCTC parts in crossing and second-source searches).
    - `match_code`: Optional. If provided, filters parts where `MatchCode` contains this string (LIKE '%match_code%').
    - `sample_n`: Optional. If provided, returns a random sample of up to `sample_n` rows, drawn
      **after** all other filters (positive/negative/match_code) are applied, so it is a sample of
      the filtered population. If omitted, all matching rows are returned.
    - `include_archived`: Optional, defaults to `false`. When `false` (the default), archived
      articles (`IstArchiviert = 1`) are excluded from the result set. Pass `true` to include
      archived articles as well. This cut applies by default, so the returned row count is
      lower than the raw article-master figure in `alphaplan://stammdaten`.
    
    Filters can be combined. The result set is stored in Redis and managed identically to `list_adressen`.

    Column semantics worth knowing:
    - Text columns are trimmed; empty strings are returned as null.
    - `Status` is the supplier's ability-to-supply indicator (source `Feld02`). It is mostly one of
      `rot` (red - the supplier cannot supply the part), `gelb` (yellow - limited availability), or
      `grün` (green - no supply problem; with umlaut, a filter on `gruen` matches nothing). Other
      values occasionally appear because of data-quality issues (e.g. `Active`, `NRND`), so do not
      assume the vocabulary is exhaustive.
    - `Bestand` is the **current** on-hand quantity in the main warehouse `RM-Lager`
      (LagerOrteID 2, summed over all charges), 0 when there is no stock row. It is a snapshot
      at fetch time, not a historic value. Other warehouses (Muster-, Sperr-, Webshop-,
      Strategielager) are not included.
    """
    store: ResultSetStore = ctx.fastmcp.result_store

    logger.info(
        "list_artikel called with positive_filter_term=%s, negative_filter_term=%s, match_code=%s, sample_n=%s, include_archived=%s",
        positive_filter_term, negative_filter_term, match_code, sample_n, include_archived
    )

    try:
        provenance = ResultProvenance(
            op="list_artikel",
            parameters={
                "positive_filter_term": positive_filter_term,
                "negative_filter_term": negative_filter_term,
                "match_code": match_code,
                "sample_n": sample_n,
                "include_archived": include_archived
            }
        )
        summary = await store.store_result_stream(
            session_id=ctx.session_id,
            provenance=provenance,
            batches=fetch_parts_stream(
                positive_filter_term=positive_filter_term,
                negative_filter_term=negative_filter_term,
                match_code=match_code,
                sample_n=sample_n,
                include_archived=include_archived
            ),
        )
        return summary
    except ToolError as e:
        logger.error("ToolError in list_artikel: %s", e.message)
        return e.to_dict()


async def list_artikel_statistik(
    artikel_nummer: Optional[str] = None,
    jahr: int = None,
    einkauf_verkauf: Optional[str] = None,
    limit: Optional[int] = None,
    ctx: Context = None,
) -> Union[ResultSetSummary, Dict[str, Any], List[Dict[str, Any]]]:
    """
    Fetch part statistics, either returning a limited number of rows directly or storing as a result set.
    
    Parameters:
    - `artikel_nummer`: Optional. If provided, filters parts where `ArtikelNummer` matches this term. Supports `%` as a wildcard.
    - `jahr`: Mandatory. The year to fetch statistics for (e.g., 2025).
    - `einkauf_verkauf`: Optional. 'V' for Sales (Verkauf), 'E' for Purchasing (Einkauf). If omitted, returns both.
    - `limit`: Optional. If provided, returns up to this many rows directly in the response. If omitted, stores the full result set in Redis and returns a summary.
    """
    if jahr is None:
        raise ToolError(code="invalid_parameters", message="The 'jahr' parameter is mandatory.")

    logger.info(
        "list_artikel_statistik called with artikel_nummer=%s, jahr=%s, einkauf_verkauf=%s, limit=%s",
        artikel_nummer, jahr, einkauf_verkauf, limit
    )

    if limit is not None:
        rows = []
        async for batch in fetch_part_stats_stream(mpn=artikel_nummer, year=jahr, sales_purchasing=einkauf_verkauf, limit=limit):
            rows.extend(batch.to_pylist())
            if len(rows) >= limit:
                return rows[:limit]
        return rows
    else:
        store: ResultSetStore = ctx.fastmcp.result_store
        provenance = ResultProvenance(
            op="list_artikel_statistik",
            parameters={
                "artikel_nummer": artikel_nummer,
                "jahr": jahr,
                "einkauf_verkauf": einkauf_verkauf
            }
        )
        summary = await store.store_result_stream(
            session_id=ctx.session_id,
            provenance=provenance,
            batches=fetch_part_stats_stream(mpn=artikel_nummer, year=jahr, sales_purchasing=einkauf_verkauf),
        )
        return summary
