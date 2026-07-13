import pytest

pytest_plugins = ["pytest_asyncio"]


@pytest.fixture
async def fake_redis():
    from fakeredis import FakeRedis

    r = FakeRedis(decode_responses=False)
    yield r
    await r.aclose()
