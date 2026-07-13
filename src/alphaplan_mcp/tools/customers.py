"""Customer ingestion tool."""

import logging
from typing import Optional

from fastmcp import Context

from ..database import fetch_customers_stream
from ..models import ResultSetSummary
from ..result_store import ResultSetStore

logger = logging.getLogger(__name__)

async def list_customers(
    customer_name: Optional[str] = None,
    ctx: Context = None,
) -> ResultSetSummary:
    """
    Fetch all matching customers, store as a result set, and return a summary.
    No rows are returned to the model.
    """
    app = ctx.request_context.app
    store: ResultSetStore = app.state.result_store
    session_id = ctx.session_id

    logger.info("list_customers called with customer_name=%s", customer_name)

    summary = await store.store_result_stream(
        session_id=session_id,
        source="customers",
        source_parameters={"customer_name": customer_name},
        batches=fetch_customers_stream(customer_name),
    )
    return summary
