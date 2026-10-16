# Constructs the FastMCP application with a Redis-backed session state store
# and application result-set client.
# Manages server lifespan, health checks, and cleanup tasks.
import asyncio
import logging
from contextlib import asynccontextmanager
from fastmcp import FastMCP
from fastmcp.server.middleware.logging import LoggingMiddleware
from redis.asyncio import Redis
from .alphaplan_resources import register_alphaplan_resources
from .apis.mouser.cache import RedisMouserCache
from .config import settings
from .middleware import StartupTimeMiddleware
from .result_store import ResultSetStore
from .tools import (
    customers,
    document_access,
    documents,
    mouser as mouser_tool,
    parts,
    prices,
    result_sets,
)

logger = logging.getLogger(__name__)

async def cleanup_loop(app, interval_seconds=300):
    while True:
        try:
            await app.result_store.run_cleanup()
        except Exception as e:
            logger.warning("Cleanup task failed: %s", e)
        await asyncio.sleep(interval_seconds)


@asynccontextmanager
async def lifespan(app):
    app.redis = Redis.from_url(settings.REDIS_URL, decode_responses=False)
    mouser_tool.default_cache = RedisMouserCache(app.redis)
    app.result_store = ResultSetStore(
        redis=app.redis,
        prefix=settings.REDIS_KEY_PREFIX,
        chunk_rows=settings.RESULTSET_CHUNK_ROWS,
        idle_ttl=settings.RESULTSET_IDLE_TTL_SECONDS,
        max_ttl=settings.RESULTSET_MAX_TTL_SECONDS,
        max_rows=settings.RESULTSET_MAX_ROWS,
        max_bytes=settings.RESULTSET_MAX_BYTES,
    )
    cleanup_task = asyncio.create_task(cleanup_loop(app))
    try:
        await app.redis.ping()
        logger.info("Connected to Redis")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise
    yield
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass
    await app.redis.close()
    logger.info("Redis connection closed")

mcp = FastMCP("AlphaPlanMCP", lifespan=lifespan)

# Stamp every tool-call result's `_meta` with the server startup epoch so
# clients can detect a mid-session restart and re-load the tool set / reset
# the connection (see `SERVER_RESTART_EPOCH_KEY`).
mcp.add_middleware(StartupTimeMiddleware())

if settings.DEBUG:
    logging.basicConfig(level=logging.DEBUG)
    mcp.add_middleware(
        LoggingMiddleware(
            include_payloads=True,
            max_payload_length=10_000,
        )
    )

# Register tools
mcp.tool()(customers.list_adressen)
mcp.tool()(mouser_tool.search_mouser_artikel)
mcp.tool()(parts.list_artikel)
mcp.tool()(parts.list_artikel_statistik)
mcp.tool()(documents.list_beleg_positionen)
mcp.tool()(prices.get_verkaufspreise)

mcp.tool()(result_sets.get_result_info)
mcp.tool()(result_sets.explain_columns)
mcp.tool()(result_sets.describe_columns)
mcp.tool()(result_sets.list_rows)
mcp.tool()(result_sets.count_rows)
mcp.tool()(result_sets.query_rows)
mcp.tool()(result_sets.distinct_values)
mcp.tool()(result_sets.aggregate)
mcp.tool()(result_sets.group_by)
mcp.tool()(result_sets.histogram)
mcp.tool()(result_sets.join)
mcp.tool()(result_sets.delete_result_set)
mcp.tool()(result_sets.normalize_preview)

register_alphaplan_resources(mcp)

# ---------------------------------------------------------------------------
# Document compatibility tools.
#
# These are deliberately redundant with the standard MCP resource API. The
# canonical representation stays `resources/list` / `resources/read` (see
# register_alphaplan_resources above). The tools below exist only for MCP
# clients that connect successfully but do not make ordinary text resources
# available to the model (e.g. some Claude chat / custom connectors). They are
# thin adapters over the same shared backend as the resource handlers, so no
# document-loading or storage logic is duplicated.
# ---------------------------------------------------------------------------


@mcp.tool()
async def list_documentation():
    """List the AlphaPlan schema documents that are useful to an LLM.

    Compatibility wrapper around this server's MCP resources. Equivalent
    content is exposed through the standard MCP resource API (resources/list);
    this tool exists for MCP clients that do not expose resources directly to
    the model.

    Returns lightweight metadata per document: URI, name/title, description
    and MIME type. Use read_documentation(uri) to load a document's content.
    """
    return await document_access.list_document_metadata(mcp)


@mcp.tool()
async def read_documentation(uri: str):
    """Read the text content of a document by its resource URI.

    Compatibility wrapper around this server's MCP resources. Equivalent
    content is exposed through the standard MCP resource API (resources/read);
    this tool exists for MCP clients that do not expose resources directly to
    the model.

    The URI must identify a document reachable through the standard resource
    implementation (e.g. one the list_documentation tool returned). Unknown or
    unavailable URIs raise RESOURCE_NOT_FOUND.
    """
    return await document_access.load_document(mcp, uri)


@mcp.tool()
async def search_documentation(query: str):
    """Search the available textual documents and return the most relevant.

    This is a convenience/discovery operation over this server's MCP resources
    rather than a replacement resource interface. Equivalent content is exposed
    through the standard MCP resource API (resources/list / resources/read);
    this tool exists for MCP clients that do not expose resources directly to
    the model.

    Scans document metadata and content for the query and returns
    `{"query": ..., "results": [...]}`; each result is compact (resource URI,
    title, relevant excerpt, relevance) so the model can decide whether to call
    read_documentation(uri). No match returns `"results": []`.
    """
    # An object, not a bare list: an empty list would reach the client as no output at all.
    results = await document_access.search_document_content(mcp, query)
    return {"query": query, "results": results}

@mcp.custom_route("/health", methods=["GET"])
async def health_check(request):
    redis = request.app.redis
    try:
        await redis.ping()
        return {"status": "healthy", "redis": "ok"}
    except Exception as e:
        return {"status": "unhealthy", "redis": str(e)}, 503

def main():
    mcp.run(transport="http", host=settings.MCP_HOST, port=settings.MCP_PORT)
