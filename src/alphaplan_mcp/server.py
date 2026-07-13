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

@asynccontextmanager
async def lifespan(app):
    app.state.redis = Redis.from_url(settings.REDIS_URL, decode_responses=False)
    app.state.result_store = ResultSetStore(app.state.redis, settings)
    try:
        await app.state.redis.ping()
        logger.info("Connected to Redis")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise
    yield
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
