from __future__ import annotations

from typing import Any

import httpx

from ...config import settings
from ...errors import tool_error

PART_SEARCH_OPTIONS = "Exact"


async def search_by_part_number(
    mpn: str,
    *,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    """
    Search Mouser by exact part number.
    
    Returns the raw parsed JSON dict from the API.
    Raises ToolError on transport/HTTP/API errors.
    """
    mpn = mpn.strip()
    if not mpn:
        raise tool_error("INVALID_MPN", "MPN must not be empty")
    
    if not settings.MOUSER_API_KEY:
        raise tool_error("MOUSER_NOT_CONFIGURED", "Mouser API key is not configured")
    
    async with httpx.AsyncClient() if client is None else client as c:
        try:
            response = await c.post(
                f"{settings.MOUSER_API_BASE_URL}/search/partnumber",
                params={"apiKey": settings.MOUSER_API_KEY},
                json={
                    "SearchByPartRequest": {
                        "mouserPartNumber": mpn,
                        "partSearchOptions": PART_SEARCH_OPTIONS,
                    }
                },
                headers={
                    "accept": "application/json",
                    "Content-Type": "application/json",
                },
                timeout=settings.MOUSER_TIMEOUT_SECONDS,
            )
        except httpx.TimeoutException:
            raise tool_error("MOUSER_TIMEOUT", "Mouser API request timed out")
        except httpx.HTTPError as exc:
            raise tool_error("MOUSER_HTTP_ERROR", f"Mouser HTTP error: {exc}")
    
    # Handle HTTP errors
    if response.status_code >= 400:
        if response.status_code == 401 or response.status_code == 403:
            raise tool_error("MOUSER_AUTH_ERROR", "Mouser authentication failed")
        if response.status_code == 429:
            raise tool_error("MOUSER_RATE_LIMIT", "Mouser API rate limit exceeded")
        
        details = {"status_code": response.status_code}
        raise tool_error("MOUSER_API_ERROR", "Mouser API error", details)
    
    # Parse response
    try:
        data = response.json()
    except (ValueError, TypeError):
        raise tool_error("MOUSER_BAD_RESPONSE", "Mouser API returned invalid JSON")
    
    if not isinstance(data, dict):
        raise tool_error("MOUSER_BAD_RESPONSE", "Mouser API response is not a JSON object")
    
    # Check for API-level errors
    errors = data.get("Errors")
    if errors and isinstance(errors, list) and len(errors) > 0:
        raise tool_error("MOUSER_API_ERROR", "Mouser API returned errors", {"errors": errors})
    
    return data
