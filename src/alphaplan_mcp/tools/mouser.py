"""Mouser part search tool."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict

from fastmcp import Context

from ..apis.mouser.api import search_by_part_number
from ..apis.mouser.cache import build_cache_key, cache_metadata, default_cache
from ..apis.mouser.parts_extraction import extract_purchasing_parts
from ..config import settings
from ..errors import ToolError, tool_error

logger = logging.getLogger(__name__)


async def search_mouser_parts(
    mpn: str,
    caching: bool = True,
    cache_ttl_override_hours: float = settings.MOUSER_CACHE_TTL_HOURS,
    ctx: Context = None,
) -> Dict[str, Any]:
    """
    Search Mouser for a single manufacturer part number.
    
    Parameters:
    - `mpn`: Exact manufacturer part number to search (single part).
    - `caching`: Whether to use caching (default True). When False, cache is neither read nor written.
    - `cache_ttl_override_hours`: Cache TTL in hours (default 168, must be positive).
    - `ctx`: FastMCP context (injected automatically).
    
    Returns a dict with:
    - `query`: The original MPN query.
    - `part_count`: Number of parts found.
    - `parts`: List of extracted purchasing parts (from `extract_purchasing_parts`).
    - `metadata`: Cache and source information:
        - `caching_enabled`: Whether caching was enabled.
        - `cached`: Whether the result came from cache.
        - `cache_age_hours`: Age of cached entry (null if not cached).
        - `cache_ttl_hours`: Cache TTL used.
        - `source`: "mouser_api" or "cache".
        - `fetched_at`: ISO-8601 UTC timestamp of the request.
    
    Results are cached in Redis (persistent across restarts, key expiry handled by Redis TTL).
    """
    try:
        mpn = mpn.strip()
        if not mpn:
            raise tool_error("INVALID_MPN", "MPN must not be empty")
        
        if cache_ttl_override_hours <= 0:
            raise tool_error("INVALID_CACHE_TTL", "cache_ttl_override_hours must be positive")
        
        key = build_cache_key(mpn)
        ttl_seconds = cache_ttl_override_hours * 3600
        
        entry = None
        parts = None
        source = None
        
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
            "query": {"mpn": mpn},
            "part_count": len(parts),
            "parts": parts,
            "metadata": {
                **cache_metadata(
                    caching_enabled=caching,
                    ttl_hours=cache_ttl_override_hours,
                    entry=entry,
                ),
                "source": source,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            },
        }
    
    except ToolError as e:
        logger.error("ToolError in search_mouser_parts: %s", e.message)
        return e.to_dict()
