# Shared document backend for the AlphaPlan schema documentation.
#
# These functions are the single loading/listing implementation. They are
# exposed through the existing FastMCP resource API (resources/list,
# resources/read) AND through the compatibility tools in server.py
# (list_documentation / read_documentation / search_documentation). Nothing here is tool
# or resource specific: FastMCP's own resource handlers already delegate to
# the same public backend (server.list_resources / server.read_resource), so
# listing, reading, filtering and version resolution stay consistent between
# the two surfaces. No duplicated file-reading or storage logic exists.
from typing import Any

from fastmcp.exceptions import NotFoundError
from fastmcp.server.server import FastMCP

from ..errors import ToolError

ERROR_RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"


async def list_document_metadata(server: FastMCP) -> list[dict[str, str]]:
    """Return lightweight metadata for every resource useful to an LLM.

    Same source of truth as ``resources/list``. Each entry carries the URI,
    title/name, description and MIME type so a model can decide what to read
    without loading content.
    """
    resources = await server.list_resources()
    return [
        {
            "uri": str(resource.uri),
            "name": resource.name,
            "description": resource.description or "",
            "mime_type": resource.mime_type,
        }
        for resource in resources
    ]


def _contents_to_text(contents: list[Any]) -> str:
    """Flatten a ResourceResult's contents to a single text surface.

    Binary items are turned into a short placeholder rather than discarded so
    the caller always learns about every content item.
    """
    parts: list[str] = []
    for item in contents:
        content = getattr(item, "content", None)
        if isinstance(content, str):
            parts.append(content)
        elif isinstance(content, bytes):
            mime = getattr(item, "mime_type", None) or "application/octet-stream"
            parts.append(f"[binary content ({mime}), {len(content)} bytes]")
        else:
            parts.append(str(content))
    return "\n".join(parts)


async def load_document(server: FastMCP, uri: str) -> str:
    """Resolve and load a document by resource URI.

    Delegates to the same backend that ``resources/read`` uses, so the
    document must be visible through the standard resource implementation;
    an unavailable or unknown URI is surfaced as a ToolError rather than
    becoming reachable here.
    """
    try:
        result = await server.read_resource(uri)
    except NotFoundError as e:
        raise ToolError(
            ERROR_RESOURCE_NOT_FOUND,
            f"Unbekannte Resource: {uri!r}",
            details={"uri": uri},
        ) from e
    return _contents_to_text(result.contents)


def _excerpt(text: str, query: str, limit: int = 400) -> str:
    """Build a compact excerpt windowed around the first case-insensitive hit."""
    lower = text.lower()
    query_lower = query.lower()
    idx = lower.find(query_lower)
    if idx == -1:
        return text[:limit]
    start = max(0, idx - limit // 2)
    end = min(len(text), start + limit)
    prefix = "…" if start > 0 else ""
    suffix = "…" if end < len(text) else ""
    return prefix + text[start:end].replace("\n", " ").strip() + suffix


async def search_document_content(
    server: FastMCP,
    query: str,
    *,
    limit: int = 5,
    snippet_chars: int = 400,
) -> list[dict[str, str]]:
    """Search the textual resources and return the most relevant documents.

    This is a model-friendly discovery operation over the same resources as
    ``resources/list`` / ``resources/read`` — it is a convenience, not a
    replacement resource interface. It scans metadata and content, ranks by
    substring hits (title matches weight higher), and returns compact results
    (URI, title, excerpt, relevance) so the model can decide whether to call
    read_documentation(uri).
    """
    query_lower = query.strip().lower()
    if not query_lower:
        return []
    resources = await server.list_resources()
    matching: list[tuple[int, dict[str, str]]] = []
    for resource in resources:
        uri = str(resource.uri)
        name = resource.name or uri
        try:
            text = await load_document(server, uri)
        except ToolError:
            continue
        title_matches = name.lower().count(query_lower)
        body_matches = text.lower().count(query_lower)
        total = title_matches + body_matches
        if total == 0:
            continue
        excerpt = _excerpt(text, query_lower, snippet_chars)
        matching.append(
            (
                total * 10 + (1 if title_matches else 0),
                {
                    "uri": uri,
                    "title": name,
                    "excerpt": excerpt,
                    "relevance": "title+body" if title_matches else "body",
                },
            )
        )
    matching.sort(key=lambda item: item[0], reverse=True)
    return [item[1] for item in matching[:limit]]