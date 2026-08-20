# FastMCP middleware that stamps every tool-call result with the server's
# startup epoch.
#
# The value carried by `_meta` (response metadata) under each tool result is a
# process-launch timestamp. It never changes while the server is running, so a
# client that observes a different value later in the same session knows the
# server restarted in the meantime and should re-load the tool set / reset the
# connection before issuing further calls.
import time

from fastmcp.server.middleware.middleware import CallNext, Middleware, MiddlewareContext

#: `_meta` key stamped on every tool-call result. The value is the server's
#: startup time as a UNIX timestamp. A change over the course of a session
#: means the server restarted: clients should re-load the tool set / reset the
#: connection before continuing.
SERVER_RESTART_EPOCH_KEY = "server_restart_epoch"


class StartupTimeMiddleware(Middleware):
    """Inject the server startup time into the ``_meta`` of every tool result.

    The epoch is captured once when the middleware is constructed, which for a
    single-process FastMCP server happens at process start. It stays constant
    for the lifetime of the process, so a client that sees the value change
    mid-session can infer that the server restarted and react accordingly
    (re-load the tool set / reset the connection).
    """

    def __init__(self, startup_time: float | None = None):
        self.started_at = int(
            startup_time if startup_time is not None else time.time()
        )

    async def on_call_tool(
        self,
        context: MiddlewareContext,
        call_next: CallNext,
    ):
        result = await call_next(context)
        meta = getattr(result, "meta", None)
        if meta is not None or hasattr(result, "meta"):
            result.meta = {
                **(meta or {}),
                SERVER_RESTART_EPOCH_KEY: self.started_at,
            }
        return result