"""Mouser part search tool."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict

from fastmcp import Context

from ..apis.mouser.api import search_by_part_number
from ..apis.mouser.cache import build_cache_key, cache_metadata, default_cache
from ..apis.mouser.parts_extraction import extract_purchasing_parts
from ..config import settings
from ..errors import ToolError, tool_error
from ..utils import normalize_mpn

logger = logging.getLogger(__name__)

MAX_MPNS_PER_CALL = 5


async def search_mouser_parts(
    mpns: list[str],
    caching: bool = True,
    cache_ttl_override_hours: float = settings.MOUSER_CACHE_TTL_HOURS,
    ctx: Context = None,
) -> Dict[str, Any]:
    """
    Search Mouser for up to 5 manufacturer part numbers in a single call.

    Parameters:
    - `mpns`: List of 1-5 exact manufacturer part numbers. Each MPN is looked up
      independently, in parallel, and cached individually in Redis.
    - `caching`: Whether to use caching (default True). When False, cache is neither read nor written.
    - `cache_ttl_override_hours`: Cache TTL in hours (default 168, must be positive).
    - `ctx`: FastMCP context (injected automatically).

    Returns a dict with a single key `results`: a map of normalized MPN -> result, where each
    result has `part_count`, `parts` (from `extract_purchasing_parts`), and `metadata`
    (`caching_enabled`, `cached`, `cache_age_hours`, `cache_ttl_hours`, `source` ("mouser_api"
    or "cache"), `fetched_at`). A failed lookup for one MPN does not fail the call; that MPN's
    entry contains an error object instead. Duplicate MPNs (after normalization) are queried once.

    Mouser API quotas: max 30 requests/minute and 1000 requests/day. Cache hits do not consume
    quota. Keep caching enabled and batch up to 5 MPNs per call to stay within the daily quota.
    """
    try:
        if not mpns:
            raise tool_error("INVALID_MPNS", f"Provide between 1 and {MAX_MPNS_PER_CALL} MPNs")
        if len(mpns) > MAX_MPNS_PER_CALL:
            raise tool_error(
                "TOO_MANY_MPNS",
                f"At most {MAX_MPNS_PER_CALL} MPNs are allowed per call",
                {"max_mpns": MAX_MPNS_PER_CALL, "received": len(mpns)},
            )
        if cache_ttl_override_hours <= 0:
            raise tool_error("INVALID_CACHE_TTL", "cache_ttl_override_hours must be positive")

        ttl_seconds = cache_ttl_override_hours * 3600
        fetched_at = datetime.now(timezone.utc).isoformat()

        # Normalize and dedupe (order-preserving); empty entries become per-MPN errors.
        results: Dict[str, Any] = {}
        to_fetch: list[str] = []
        for raw_mpn in mpns:
            normalized = normalize_mpn(raw_mpn)
            if not normalized:
                results[raw_mpn] = tool_error("INVALID_MPN", "MPN must not be empty").to_dict()
            elif normalized not in to_fetch:
                to_fetch.append(normalized)

        async def fetch_one(mpn: str) -> Dict[str, Any]:
            key = build_cache_key(mpn)
            entry = None
            try:
                if caching:
                    entry = await default_cache.get(key)
                if entry is not None:
                    parts = entry.payload
                    source = "cache"
                else:
                    raw = await search_by_part_number(mpn)
                    parts = extract_purchasing_parts(raw)
                    if caching:
                        await default_cache.set(key, parts, ttl_seconds)
                    source = "mouser_api"
                return {
                    "part_count": len(parts),
                    "parts": parts,
                    "metadata": {
                        **cache_metadata(
                            caching_enabled=caching,
                            ttl_hours=cache_ttl_override_hours,
                            entry=entry,
                        ),
                        "source": source,
                        "fetched_at": fetched_at,
                    },
                }
            except ToolError as e:
                logger.error("ToolError searching MPN %s: %s", mpn, e.message)
                return e.to_dict()

        fetched = await asyncio.gather(*(fetch_one(m) for m in to_fetch))
        for mpn, result in zip(to_fetch, fetched):
            results[mpn] = result

        return {"results": results}
    
    except ToolError as e:
        logger.error("ToolError in search_mouser_parts: %s", e.message)
        return e.to_dict()
