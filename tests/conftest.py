import pytest
import pytest_asyncio

pytest_plugins = ["pytest_asyncio"]


@pytest_asyncio.fixture
async def mcp_server():
    from fastmcp import FastMCP
    from alphaplan_mcp.alphaplan_resources import register_alphaplan_resources

    mcp = FastMCP("TestResources")
    register_alphaplan_resources(mcp)
    yield mcp


@pytest_asyncio.fixture
async def fake_redis():
    fakeredis = pytest.importorskip("fakeredis")
    from fakeredis import FakeAsyncRedis

    r = FakeAsyncRedis(decode_responses=False)
    yield r
    await r.aclose()
