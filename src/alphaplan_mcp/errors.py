"""Application error codes and helpers."""

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class ToolError(Exception):
    """An error meant to be returned to the MCP client."""
    def __init__(self, code: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        self.code = code
        self.message = message
        self.details = details
        super().__init__(self.message)

    def to_dict(self) -> Dict[str, Any]:
        resp = {"error": self.code, "message": self.message}
        if self.details:
            resp["details"] = self.details
        return resp


def tool_error(code: str, message: str, details: Optional[Dict[str, Any]] = None) -> ToolError:
    """Return a ToolError instance (should be raised)."""
    return ToolError(code, message, details)


def wrap_redis_error(func_name: str, exc: Exception) -> ToolError:
    """Convert a Redis exception into a REDIS_UNAVAILABLE ToolError."""
    logger.error("Redis error in %s: %s", func_name, exc)
    return ToolError("REDIS_UNAVAILABLE", "Redis operation failed. Please try again later.")


def wrap_db_error(func_name: str, exc: Exception) -> ToolError:
    """Convert a database exception into a safe ToolError."""
    logger.error("Database error in %s: %s", func_name, exc)
    if "timeout" in str(exc).lower() or "timed out" in str(exc).lower():
        return ToolError("DATABASE_QUERY_TIMEOUT", "The database query timed out.")
    return ToolError("DATABASE_UNAVAILABLE", "Database operation failed. Please try again later.")
