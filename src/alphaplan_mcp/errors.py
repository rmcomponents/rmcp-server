"""Application error codes and helpers."""

from typing import Any, Dict, Optional

def tool_error(
    code: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return a structured error dict for MCP tool responses."""
    response: Dict[str, Any] = {
        "error": code,
        "message": message,
    }
    if details:
        response["details"] = details
    return response
