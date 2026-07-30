# Constructs the FastMCP application with a Redis-backed session state store
# and application result-set client.
# Manages server lifespan, health checks, and cleanup tasks.
import asyncio
import logging
from contextlib import asynccontextmanager
from fastmcp import FastMCP
from redis.asyncio import Redis
from .config import settings
from .result_store import ResultSetStore
from .tools import customers, parts, result_sets

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

# Register tools
mcp.tool()(customers.list_customers)
mcp.tool()(parts.list_parts)
mcp.tool()(parts.list_part_stats)

mcp.tool()(result_sets.get_result_info)
mcp.tool()(result_sets.explain_columns)
mcp.tool()(result_sets.list_rows)
mcp.tool()(result_sets.count_rows)
mcp.tool()(result_sets.query_rows)
mcp.tool()(result_sets.distinct_values)
mcp.tool()(result_sets.aggregate)
mcp.tool()(result_sets.group_by)
mcp.tool()(result_sets.histogram)
mcp.tool()(result_sets.delete_result_set)

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
