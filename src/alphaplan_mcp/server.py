# Constructs the FastMCP application with a Redis-backed session state store
# and application result-set client.
# Manages server lifespan, health checks, and cleanup tasks.
import asyncio
import logging
from contextlib import asynccontextmanager
from fastmcp import FastMCP
from py_key_value_aio.redis import RedisStore
from redis.asyncio import Redis
from .config import settings
from .result_store import ResultSetStore
from .tools import customers, result_sets

logger = logging.getLogger(__name__)

store = RedisStore(url=settings.REDIS_URL)

async def cleanup_loop(app, interval_seconds=300):
    while True:
        try:
            await app.state.result_store.run_cleanup()
        except Exception as e:
            logger.warning("Cleanup task failed: %s", e)
        await asyncio.sleep(interval_seconds)


@asynccontextmanager
async def lifespan(app):
    app.state.redis = Redis.from_url(settings.REDIS_URL, decode_responses=False)
    app.state.result_store = ResultSetStore(app.state.redis, settings)
    cleanup_task = asyncio.create_task(cleanup_loop(app))
    try:
        await app.state.redis.ping()
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
    await app.state.redis.close()
    logger.info("Redis connection closed")

mcp = FastMCP("AlphaPlanMCP", lifespan=lifespan, session_state_store=store)

@mcp.custom_route("/health", methods=["GET"])
async def health_check(request):
    redis = request.app.state.redis
    try:
        await redis.ping()
        return {"status": "healthy", "redis": "ok"}
    except Exception as e:
        return {"status": "unhealthy", "redis": str(e)}, 503

def main():
    mcp.run(transport="http", host=settings.MCP_HOST, port=settings.MCP_PORT)
