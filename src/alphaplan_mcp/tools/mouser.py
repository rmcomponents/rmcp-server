"""Mouser part search tool."""
from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import io
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastmcp import Context

from ..apis.mouser.api import search_by_part_number
from ..apis.mouser.cache import build_cache_key, cache_metadata, default_cache
from ..apis.mouser.parts_extraction import extract_purchasing_parts
from ..config import settings
from ..errors import ToolError, tool_error
from ..utils import normalize_mpn

logger = logging.getLogger(__name__)

MAX_MPNS_PER_CALL = 25



def _date_part(timestamp: Optional[str]) -> str:
    """ISO timestamp -> 'YYYY-MM-DD' for the CSV `updated_at` column ('' when missing/unparseable)."""
    if not timestamp:
        return ""
    try:
        return datetime.fromisoformat(timestamp).date().isoformat()
    except ValueError:
        return ""

async def search_mouser_artikel(
    artikel_nummern: list[str],
    caching: bool = True,
    cache_ttl_override_hours: float = settings.MOUSER_CACHE_TTL_HOURS,
    output_format: str = "json",
    retry_on_rate_limit: bool = True,
    ctx: Context = None,
) -> Any:
    """
    Search Mouser for up to 25 manufacturer part numbers in a single call.

    Parameters:
    - `artikel_nummern`: List of 1-25 exact manufacturer part numbers. Each MPN is looked up
      independently, in parallel, and cached individually in Redis.
    - `caching`: Whether to use caching (default True). When False, cache is neither read nor written.
    - `cache_ttl_override_hours`: Cache TTL in hours (default 168, must be positive).
    - `output_format`: Output format, either "csv" or "json". Defaults to "json".
    - `retry_on_rate_limit`: If True, a 429 triggers one retry after 60 seconds. Defaults to True.
    - `ctx`: FastMCP context (injected automatically).

    Returns a dict with a single key `results`: a map of request string -> result, where each
    result has `requested_mpn` (the exact input string), `part_count`, `parts` (from
    `extract_purchasing_parts`), and `metadata` (`caching_enabled`, `cached`,
    `cache_age_hours`, `cache_ttl_hours`, `source` ("mouser_api" or "cache"), `fetched_at`).
    A failed lookup for one MPN does not fail the call; that MPN's entry contains an error
    object instead. Duplicate MPNs (after normalization) are queried once, but each distinct
    request string appears as a separate key in `results` pointing to the same part data.

    Response keys are the original request strings with leading/trailing whitespace removed;
    internal spaces, case, and punctuation are preserved. Normalization (uppercase, remove all
    whitespace, strip characters that are not alphanumeric, '-', '_', or '.') is used only for
    deduplication and cache keys.

    Mouser API quotas: max 30 requests/minute and 1000 requests/day. Cache hits do not consume
    quota. Keep caching enabled and batch up to 25 MPNs per call to stay within the daily quota.

    If output_format is "csv", returns a dict with keys:
    - `csv`: the CSV string with columns:
      MPN, breakpoint_min, breakpoint_max, unit_price_min, unit_price_max, updated_at, error
      (`updated_at` = date of the cached or fresh Mouser data, 'YYYY-MM-DD'; `error` is the
      lookup error, or 'no parts found' / 'no price breaks' when Mouser has no part or no
      prices for that MPN)
    - `sha256_base64`: Base64(SHA-256(exact UTF-8 bytes of the preceding CSV)),
      excluding the checksum line. Preserve LF line endings and the final newline
      exactly. ***Verify before using the data***.
    """
    try:
        if not artikel_nummern:
            raise tool_error("INVALID_MPNS", f"Provide between 1 and {MAX_MPNS_PER_CALL} MPNs")
        if len(artikel_nummern) > MAX_MPNS_PER_CALL:
            raise tool_error(
                "TOO_MANY_MPNS",
                f"At most {MAX_MPNS_PER_CALL} MPNs are allowed per call",
                {"max_mpns": MAX_MPNS_PER_CALL, "received": len(artikel_nummern)},
            )
        if cache_ttl_override_hours <= 0:
            raise tool_error("INVALID_CACHE_TTL", "cache_ttl_override_hours must be positive")
        if output_format not in ("csv", "json"):
            raise tool_error("INVALID_FORMAT", "output_format must be 'csv' or 'json'")

        ttl_seconds = cache_ttl_override_hours * 3600
        fetched_at = datetime.now(timezone.utc).isoformat()

        def _mouser_results_to_csv(results: dict) -> str:
            output = io.StringIO()
            writer = csv.writer(output, lineterminator="\n")
            writer.writerow([
                "MPN", "breakpoint_min", "breakpoint_max",
                "unit_price_min", "unit_price_max", "updated_at", "error",
            ])

            for mpn, result in results.items():
                if "error" in result:
                    writer.writerow([mpn, "", "", "", "", "", result.get("message", "")])
                    continue

                parts = result.get("parts") or []
                quantity_breaks = []
                unit_prices = []

                for part in parts:
                    for price_break in part.get("price_breaks") or []:
                        q = price_break.get("quantity")
                        p = price_break.get("unit_price")
                        if q is not None:
                            quantity_breaks.append(q)
                        if p is not None:
                            unit_prices.append(p)

                metadata = result.get("metadata") or {}
                if metadata.get("source") == "cache":
                    updated_at = _date_part(metadata.get("cached_at"))
                else:
                    updated_at = _date_part(metadata.get("fetched_at"))

                if not parts:
                    row_error = "no parts found"
                elif not unit_prices:
                    row_error = "no price breaks"
                else:
                    row_error = ""
                writer.writerow([
                    mpn,
                    min(quantity_breaks) if quantity_breaks else "",
                    max(quantity_breaks) if quantity_breaks else "",
                    min(unit_prices) if unit_prices else "",
                    max(unit_prices) if unit_prices else "",
                    updated_at,
                    row_error,
                ])

            return output.getvalue()

        # Normalize and dedupe (order-preserving); empty entries become per-MPN errors.
        # Response keys are the original request strings with leading/trailing whitespace
        # removed, while normalization is used only for deduplication and cache keys.
        results: Dict[str, Any] = {}
        normalized_to_requests: Dict[str, list[str]] = {}
        for raw_mpn in artikel_nummern:
            normalized = normalize_mpn(raw_mpn)
            if not normalized:
                results[raw_mpn] = {
                    **tool_error("INVALID_MPN", "MPN must not be empty").to_dict(),
                    "requested_mpn": raw_mpn,
                }
            else:
                normalized_to_requests.setdefault(normalized, []).append(raw_mpn)

        async def fetch_one(mpn: str) -> Dict[str, Any]:
            key = build_cache_key(mpn)
            entry = None
            cached_at = None
            try:
                if caching:
                    entry = await default_cache.get(key)
                if entry is not None:
                    parts = entry.payload
                    source = "cache"
                    cached_at = entry.stored_at.isoformat()
                else:
                    raw = await search_by_part_number(
                        mpn,
                        retry_on_rate_limit=retry_on_rate_limit,
                    )
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
                        "cached_at": cached_at,
                    },
                }
            except ToolError as e:
                logger.error("ToolError searching MPN %s: %s", mpn, e.message)
                return e.to_dict()

        fetched = await asyncio.gather(*(fetch_one(m) for m in normalized_to_requests))
        for normalized, result in zip(normalized_to_requests, fetched):
            for raw_mpn in normalized_to_requests[normalized]:
                request_key = raw_mpn.strip()
                # If multiple request strings strip to the same key, the first one wins.
                if request_key not in results:
                    results[request_key] = {
                        **result,
                        "requested_mpn": raw_mpn,
                    }

        if output_format == "csv":
            csv_data = _mouser_results_to_csv(results)
            return {
                "csv": csv_data,
                "sha256_base64": base64.b64encode(
                    hashlib.sha256(csv_data.encode("utf-8")).digest()
                ).decode("ascii"),
            }
        return {"results": results}
    
    except ToolError as e:
        logger.error("ToolError in search_mouser_artikel: %s", e.message)
        return e.to_dict()
