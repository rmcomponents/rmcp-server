# Manages SQL Server connections via pyodbc.
# Consumes application settings, enforces timeouts,
# and runs blocking calls asynchronously.
"""SQL Server connection and customer data fetching."""

import logging
from typing import AsyncGenerator, List, Optional, Tuple

import pyarrow as pa
import pyodbc

from .config import settings

logger = logging.getLogger(__name__)

# Fixed query – no user‑supplied table or column names.
_CUSTOMER_QUERY = """
    SELECT
        AdressNummer AS adress_nummer,
        SuchName AS name_intern,
        SuchName AS name,
        Branche AS branche,
        K_KundenTypNeu AS kunden_typ,
        KundenTyp AS finale_klassifizierung,
        K_Vertriebskanal AS vertriebskanal,
        Kategorie AS potential_zu_kategorie,
        Beruf AS umsatz_level_vorjahr,
        K_VKID AS sachbearbeiter_innendiest,
        K_VKAD AS sachbearbeiter_aussendienst,
        K_AB AS sachbearbeiter_auftragsbearbeitung
    FROM
        ALPHAPLAN_EchtDaten.dbo.Adressen
    WHERE
        IstArchiviert = 0
        AND AdressenID > 0
        AND SuchName LIKE ?
    ORDER BY AdressNummer
"""

_CUSTOMER_COLUMN_DESCRIPTIONS = {
    "adress_nummer": "Unique identifier for the customer address.",
    "name_intern": "(Company Internal) customer name",
    "name": "customer name",
    "branche": "Industry sector of the customer.",
    "kunden_typ": "?",
    "vertriebskanal": "?",
    "potential_zu_kategorie": "?",
    "umsatz_level_vorjahr": "?",
    "sachbearbeiter_innendienst": "Unique name abbreviation of the employee responsible for internal sales",
    "sachbearbeiter_aussendienst": "Unique name abbreviation of the employee responsible for field sales",
    "sachbearbeiter_auftragsbearbeitung": "Unique name abbreviation of the employee responsible for order processing"
}


def build_connection() -> pyodbc.Connection:
    """Create a new pyodbc connection using application settings."""
    conn_str = (
        f"DRIVER={{{settings.SQLSERVER_DRIVER}}};"
        f"SERVER={settings.SQLSERVER_HOST};"
        f"DATABASE={settings.SQLSERVER_DATABASE};"
        f"UID={settings.SQLSERVER_USER};"
        f"PWD={settings.SQLSERVER_PASSWORD};"
        f"TrustServerCertificate={settings.SQLSERVER_TRUST_CERTIFICATE};"
        f"ApplicationIntent=ReadOnly;"
        f"Encrypt=yes;"
        f"Connection Timeout={settings.SQLSERVER_QUERY_TIMEOUT_SECONDS};"
    )
    return pyodbc.connect(conn_str, autocommit=True, timeout=settings.SQLSERVER_QUERY_TIMEOUT_SECONDS)


async def fetch_customers_stream(
    customer_name: Optional[str] = None,
) -> AsyncGenerator[pa.RecordBatch, None]:
    """
    Asynchronously yield customer rows as Arrow record batches.

    Runs the blocking ODBC calls inside a thread.
    """
    import asyncio
    from asyncio import Queue

    queue: Queue[Optional[pa.RecordBatch]] = asyncio.Queue()

    def _fetch():
        conn = None
        cursor = None
        try:
            conn = build_connection()
            cursor = conn.cursor()
            pattern = customer_name if customer_name else "%"
            cursor.execute(_CUSTOMER_QUERY, (pattern,))

            columns = [col[0] for col in cursor.description]
            while True:
                rows = cursor.fetchmany(settings.RESULTSET_CHUNK_ROWS)
                if not rows:
                    break
                dicts = [dict(zip(columns, row)) for row in rows]
                batch = pa.RecordBatch.from_pylist(dicts)
                # Attach column descriptions to the schema metadata
                new_fields = []
                for field in batch.schema:
                    desc = _CUSTOMER_COLUMN_DESCRIPTIONS.get(field.name)
                    if desc:
                        metadata = {**field.metadata, b"description": desc.encode("utf-8")} if field.metadata else {b"description": desc.encode("utf-8")}
                        new_fields.append(field.with_metadata(metadata))
                    else:
                        new_fields.append(field)
                new_schema = pa.schema(new_fields)
                columns = [batch.column(i) for i in range(batch.num_columns)]
                batch = pa.RecordBatch.from_arrays(columns, schema=new_schema)
                queue.put_nowait(batch)
        except Exception as e:
            logger.exception("Error fetching customers")
            queue.put_nowait(None)  # signal error
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        queue.put_nowait(None)  # signal end

    import asyncio
    loop = asyncio.get_running_loop()
    # Run the blocking function in a thread
    task = loop.run_in_executor(None, _fetch)

    # Yield batches as they become available
    while True:
        batch = await queue.get()
        if batch is None:
            break
        yield batch
    await task  # propagate any exception
