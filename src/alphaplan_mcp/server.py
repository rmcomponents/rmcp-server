import logging
from contextlib import asynccontextmanager
from fastmcp import FastMCP
from redis.asyncio import Redis
from .config import settings

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app):
    app.state.redis = Redis.from_url(settings.REDIS_URL, decode_responses=False)
    try:
        await app.state.redis.ping()
        logger.info("Connected to Redis")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise
    yield
    await app.state.redis.close()
    logger.info("Redis connection closed")

mcp = FastMCP("AlphaPlanMCP", lifespan=lifespan)

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
