"""Unit tests for StartupTimeMiddleware (server startup epoch stamping)."""

import pytest
from fastmcp.server.middleware.middleware import MiddlewareContext
from fastmcp.tools.base import ToolResult

from alphaplan_mcp.middleware import (
    SERVER_RESTART_EPOCH_KEY,
    StartupTimeMiddleware,
)


async def _run(middleware, result):
    async def call_next(context):
        return result

    return await middleware.on_call_tool(
        MiddlewareContext(message=None), call_next
    )


def _tool_result(meta=None):
    return ToolResult(content="ok", meta=meta)


@pytest.mark.asyncio
async def test_stamps_startup_epoch():
    mw = StartupTimeMiddleware(startup_time=1_700_000_000.9)
    out = await _run(mw, _tool_result())
    assert out.meta[SERVER_RESTART_EPOCH_KEY] == 1_700_000_000


@pytest.mark.asyncio
async def test_preserves_existing_meta():
    mw = StartupTimeMiddleware(startup_time=123.0)
    out = await _run(mw, _tool_result(meta={"client_note": "keep-me"}))
    assert out.meta["client_note"] == "keep-me"
    assert out.meta[SERVER_RESTART_EPOCH_KEY] == 123


@pytest.mark.asyncio
async def test_epoch_is_stable_across_calls():
    mw = StartupTimeMiddleware(startup_time=42.0)
    first = await _run(mw, _tool_result())
    second = await _run(mw, _tool_result())
    assert first.meta[SERVER_RESTART_EPOCH_KEY] == 42
    assert second.meta[SERVER_RESTART_EPOCH_KEY] == 42


@pytest.mark.asyncio
async def test_epoch_reaches_wire_meta():
    mw = StartupTimeMiddleware(startup_time=7.0)
    out = await _run(mw, _tool_result())
    wire = out.to_mcp_result()
    assert wire.meta == {SERVER_RESTART_EPOCH_KEY: 7}


@pytest.mark.asyncio
async def test_default_startup_time_is_current_epoch():
    import time

    mw = StartupTimeMiddleware()
    before = int(time.time())
    out = await _run(mw, _tool_result())
    assert out.meta[SERVER_RESTART_EPOCH_KEY] in (before, before + 1)