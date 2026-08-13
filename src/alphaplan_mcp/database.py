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

_PART_COLUMN_DESCRIPTIONS = {
    "ArtikelNummer": "Unique article/part number.",
    "RollengroesseZoll": "Roll size in inches.",
    "Beschreibung": "Combined description (short text + long text).",
    "MatchCode": "Search match code for the part.",
    "Status": "Supplier ability-to-supply indicator (Feld02). Mostly 'rot' (red = cannot supply), 'gelb' (yellow = limited), or 'gruen' (green = no problem). Other values are possible due to data-quality issues.",
    "VPE": "Sales unit (Verpackungseinheit).",
    "HauptLieferant": "Main supplier name.",
    "NachfolgeArtikel": "Successor article number (Feld03).",
    "Wiederbeschaffungszeit_Tage": "Replenishment time in days.",
    "WarenGruppe": "Product group description.",
    "WarenGruppenNummer": "Product group number."
}

_PART_STATS_COLUMN_DESCRIPTIONS = {
    "ArtikelNummer": "Unique article/part number.",
    "EinkaufVerkauf": "Sales or Purchasing indicator ('V' = Verkauf/Sales, 'E' = Einkauf/Purchasing).",
    "Jahr": "Year of the statistics.",
    "KW": "Calendar week (Kalenderwoche).",
    "Netto": "Net amount.",
    "Rohertrag": "Gross profit (Roh).",
    "Menge": "Quantity (BEEanzahl)."
}

_WEBSHOP_PRICE_COLUMN_DESCRIPTIONS = {
    "mpn": "Manufacturer part number.",
    "quantity_breakpoint": "Minimum quantity for this price tier.",
    "unit_price": "Unit price at the given quantity breakpoint.",
    "updated_at": "Timestamp when this price was last updated.",
}

_LINE_ITEM_COLUMN_DESCRIPTIONS = {
    "ArtikelNummer": "Unique article/part number of the line item.",
    "BelegNummer": "Document number (prefix reflects the document type, e.g. 'ER250025').",
    "KundenNummer": "Counterparty address number (AdressNummer).",
    "KundeOderLieferant": "Counterparty name (customer for outgoing documents, supplier for purchasing documents).",
    "KundeKlassifizierung": "Counterparty classification code (KundenTyp).",
    "KundeBranche": "Counterparty industry sector (Branche).",
    "Menge": "Quantity (VEAnzahl / packaging-unit count).",
    "MengeOffen": "Open/remaining quantity still to be taken over (NochZuUebernehmen).",
    "NettoPreisStueckEUR": "Net unit price per piece converted to EUR.",
    "BelegDatum": "Date of the document (B.Datum). No longer used as the date-range filter.",
    "LieferDatum": "Delivery date. For EB (purchasing orders) this is the requested delivery date.",
    "LieferDatumBestaetigt": "Confirmed delivery date. Populated only for EB orders; often empty.",
    "OriginalDatum": "Original/requested delivery date (AnlieferungsDatumWunsch).",
    "BelegKursFaktor": "Exchange-rate factor of the document (KursFaktor / KursDivisor).",
    "BelegWaehrung": "ISO currency code of the document (e.g. JPY).",
    "IstArchiviert": "Whether the document is archived (1 = archived, 0 = not archived).",
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


async def fetch_webshop_prices_stream(
    mpns: List[str],
) -> AsyncGenerator[pa.RecordBatch, None]:
    """
    Asynchronously yield webshop price rows for multiple MPNs as Arrow record batches.
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
            placeholders = ",".join("?" for _ in mpns)
            query = f"""
                SELECT mpn, quantity_breakpoint, unit_price, updated_at
                FROM [ApiPreise].[dbo].[WebshopPreise]
                WHERE mpn IN ({placeholders})
                ORDER BY mpn, quantity_breakpoint ASC
            """
            cursor.execute(query, mpns)

            columns = [col[0] for col in cursor.description]
            while True:
                rows = cursor.fetchmany(settings.RESULTSET_CHUNK_ROWS)
                if not rows:
                    break
                dicts = [dict(zip(columns, row)) for row in rows]
                batch = pa.RecordBatch.from_pylist(dicts)
                # Attach column descriptions
                new_fields = []
                for field in batch.schema:
                    desc = _WEBSHOP_PRICE_COLUMN_DESCRIPTIONS.get(field.name)
                    if desc:
                        metadata = {**field.metadata, b"description": desc.encode("utf-8")} if field.metadata else {b"description": desc.encode("utf-8")}
                        new_fields.append(field.with_metadata(metadata))
                    else:
                        new_fields.append(field)
                new_schema = pa.schema(new_fields)
                cols = [batch.column(i) for i in range(batch.num_columns)]
                batch = pa.RecordBatch.from_arrays(cols, schema=new_schema)
                queue.put_nowait(batch)
        except Exception:
            logger.exception("Error fetching webshop prices")
            queue.put_nowait(None)
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        queue.put_nowait(None)

    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(None, _fetch)

    while True:
        batch = await queue.get()
        if batch is None:
            break
        yield batch
    await task


async def fetch_line_items_stream(
    artikel_nummer: str,
    beleg_typ: int,
    date_from: str,
    date_to: str,
    include_archived: bool = False,
) -> AsyncGenerator[pa.RecordBatch, None]:
    """
    Asynchronously yield line items for a single part number within a date range.

    Rows come from documents of a single BelegTyp (resolved from the tool's `document_type`
    code, e.g. 'AR' -> 5, 'EB' -> 9, 'ER' -> 10). The counterparty (customer or supplier),
    the net unit EUR price (`NettoPreisStueckEUR`), the open quantity (`MengeOffen`), and the
    delivery dates are joined in SQL.

    A row is returned when ANY of the three BP date fields falls inside the inclusive bounds
    (>= `date_from` and <= `date_to`): `BP.LieferDatum` (LieferDatum),
    `BP.AnlieferungsDatumBestaetigt` (LieferDatumBestaetigt), or
    `BP.AnlieferungsDatumWunsch` (OriginalDatum). The document date `B.Datum` (BelegDatum) is
    still returned but no longer filters rows.

    When `include_archived` is True, documents that are archived (`B.IstArchiviert = 1`) are
    included; otherwise they are excluded.
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
            archived_predicate = "" if include_archived else "B.IstArchiviert = 0 AND "
            query = f"""
                SELECT
                    BP.ArtikelNummer,
                    B.BelegNummer,
                    A.AdressNummer AS KundenNummer,
                    A.SuchName AS KundeOderLieferant,
                    A.KundenTyp AS KundeKlassifizierung,
                    A.Branche AS KundeBranche,
                    BP.VEAnzahl AS Menge,
                    BP.NochZuUebernehmen AS MengeOffen,
                    ROUND(BP.NettoPreisEinzelX * B.KursFaktor / NULLIF(B.KursDivisor, 0), 5) AS NettoPreisStueckEUR,
                    B.Datum AS BelegDatum,
                    B.IstArchiviert AS IstArchiviert,
                    BP.LieferDatum AS LieferDatum,
                    BP.AnlieferungsDatumBestaetigt AS LieferDatumBestaetigt,
                    BP.AnlieferungsDatumWunsch AS OriginalDatum,
                    ROUND(B.KursFaktor / NULLIF(B.KursDivisor, 0), 5) AS BelegKursFaktor,
                    W.WaehrungsKuerzel AS BelegWaehrung
                FROM ALPHAPLAN_EchtDaten.dbo.BelegePositionen BP
                    JOIN ALPHAPLAN_EchtDaten.dbo.Belege B
                        ON BP.BelegeID = B.BelegeID AND B.BelegTyp = ?
                    JOIN ALPHAPLAN_EchtDaten.dbo.Waehrungen W
                        ON W.WaehrungenID = B.WaehrungenID
                    JOIN ALPHAPLAN_EchtDaten.dbo.Adressen A
                        ON A.AdressenID = B.AuftragsAdressenID
                WHERE {archived_predicate}BP.ArtikelNummer = ?
                    AND (
                        (BP.LieferDatum >= ? AND BP.LieferDatum <= ?)
                     OR (BP.AnlieferungsDatumBestaetigt >= ? AND BP.AnlieferungsDatumBestaetigt <= ?)
                     OR (BP.AnlieferungsDatumWunsch >= ? AND BP.AnlieferungsDatumWunsch <= ?)
                    )
                ORDER BY BP.ArtikelNummer, B.Datum ASC
            """
            params = [beleg_typ, artikel_nummer,
                      date_from, date_to,
                      date_from, date_to,
                      date_from, date_to]
            cursor.execute(query, params)

            columns = [col[0] for col in cursor.description]
            while True:
                rows = cursor.fetchmany(settings.RESULTSET_CHUNK_ROWS)
                if not rows:
                    break
                dicts = [dict(zip(columns, row)) for row in rows]
                batch = pa.RecordBatch.from_pylist(dicts)
                new_fields = []
                for field in batch.schema:
                    desc = _LINE_ITEM_COLUMN_DESCRIPTIONS.get(field.name)
                    if desc:
                        metadata = {**field.metadata, b"description": desc.encode("utf-8")} if field.metadata else {b"description": desc.encode("utf-8")}
                        new_fields.append(field.with_metadata(metadata))
                    else:
                        new_fields.append(field)
                new_schema = pa.schema(new_fields)
                cols = [batch.column(i) for i in range(batch.num_columns)]
                batch = pa.RecordBatch.from_arrays(cols, schema=new_schema)
                queue.put_nowait(batch)
        except Exception:
            logger.exception("Error fetching purchasing prices")
            queue.put_nowait(None)
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        queue.put_nowait(None)

    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(None, _fetch)

    while True:
        batch = await queue.get()
        if batch is None:
            break
        yield batch
    await task


async def fetch_part_stats_stream(
    mpn: Optional[str] = None,
    year: int = None,
    sales_purchasing: Optional[str] = None,
    limit: Optional[int] = None,
) -> AsyncGenerator[pa.RecordBatch, None]:
    """
    Asynchronously yield part statistics rows as Arrow record batches.
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
            
            query = """
                SELECT
                    A.ArtikelNummer,
                    EinkaufVerkauf,
                    Jahr,
                    KW,
                    Netto,
                    Roh AS Rohertrag,
                    BEEanzahl AS Menge
                FROM [ALPHAPLAN_EchtDaten].[dbo].[StatistikenArtikel]
                JOIN dbo.Artikel A ON A.ArtikelID = StatistikenArtikel.ArtikelID
                WHERE A.ArtikelID > 0
            """
            params = []
            
            if year is not None:
                query += " AND Jahr = ?"
                params.append(year)
            
            if sales_purchasing == 'E':
                query += " AND EinkaufVerkauf = 'E'"
            elif sales_purchasing == 'V':
                query += " AND EinkaufVerkauf = 'V'"
            else:
                query += " AND (EinkaufVerkauf = 'E' OR EinkaufVerkauf = 'V')"
                
            if mpn:
                query += " AND A.ArtikelNummer LIKE ?"
                params.append(mpn)
                
            query += " ORDER BY ArtikelNummer, KW"
            
            cursor.execute(query, params)

            columns = [col[0] for col in cursor.description]
            rows_fetched = 0
            while True:
                rows = cursor.fetchmany(settings.RESULTSET_CHUNK_ROWS)
                if not rows:
                    break
                dicts = [dict(zip(columns, row)) for row in rows]
                batch = pa.RecordBatch.from_pylist(dicts)
                # Attach column descriptions to the schema metadata
                new_fields = []
                for field in batch.schema:
                    desc = _PART_STATS_COLUMN_DESCRIPTIONS.get(field.name)
                    if desc:
                        metadata = {**field.metadata, b"description": desc.encode("utf-8")} if field.metadata else {b"description": desc.encode("utf-8")}
                        new_fields.append(field.with_metadata(metadata))
                    else:
                        new_fields.append(field)
                new_schema = pa.schema(new_fields)
                cols = [batch.column(i) for i in range(batch.num_columns)]
                batch = pa.RecordBatch.from_arrays(cols, schema=new_schema)
                queue.put_nowait(batch)
                
                if limit is not None:
                    rows_fetched += len(rows)
                    if rows_fetched >= limit:
                        break
        except Exception as e:
            logger.exception("Error fetching part stats")
            queue.put_nowait(None)
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        queue.put_nowait(None)

    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(None, _fetch)

    while True:
        batch = await queue.get()
        if batch is None:
            break
        yield batch
    await task


async def fetch_parts_stream(
    positive_filter_term: Optional[str] = None,
    negative_filter_term: Optional[str] = "TCC%",
    match_code: Optional[str] = None,
    sample_n: Optional[int] = None,
) -> AsyncGenerator[pa.RecordBatch, None]:
    """
    Asynchronously yield part rows as Arrow record batches.
    
    Supports optional positive/negative filtering on ArtikelNummer and match code search.
    When `sample_n` is set, the sample is taken *after* all other filters are applied, returning
    up to `sample_n` random rows from the filtered population.
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
            
            columns_sql = """
                SELECT 
                    A.ArtikelNummer, 
                    A.Groesse AS RollengroesseZoll,
                    A.Bezeichnung + ' ' + A.LangText AS Beschreibung,
                    A.MatchCode AS MatchCode,
                    A.Feld02 AS [Status],
                    A.K_VPE AS VPE,
                    L.SuchName AS HauptLieferant,
                    A.Feld03 AS NachfolgeArtikel,
                    A.Wiederbeschaffungszeit AS Wiederbeschaffungszeit_Tage,
                    WG.Bezeichnung AS WarenGruppe,
                    WG.WarenGruppenNummer AS WarenGruppenNummer
                FROM ALPHAPLAN_EchtDaten.dbo.Artikel A WITH (NOLOCK)
                JOIN ALPHAPLAN_EchtDaten.dbo.Adressen L WITH (NOLOCK) ON L.AdressenID = A.HauptLieferantenAdressenID
                JOIN ALPHAPLAN_EchtDaten.dbo.WarenGruppen WG WITH (NOLOCK) ON WG.WarenGruppenID = A.WarenGruppenID
            """

            # Build the WHERE clauses (equivalent to `inner_where` below). We do not use
            # TABLESAMPLE: it samples the physical table *before* filters, i.e. from the wrong
            # population. Instead we filter first (inner subquery), then take up to sample_n random
            # rows from the filtered result via `ORDER BY NEWID()`.
            params = []
            where = (
                "WHERE A.IstArchiviert = 0 AND A.ArtikelID > 0"
                " AND WG.Bezeichnung IS NOT NULL AND LTRIM(RTRIM(WG.Bezeichnung)) <> ''"
            )

            if positive_filter_term:
                where += " AND A.ArtikelNummer LIKE ?"
                params.append(positive_filter_term)

            if negative_filter_term:
                where += " AND A.ArtikelNummer NOT LIKE ?"
                params.append(negative_filter_term)

            if match_code:
                where += " AND A.MatchCode LIKE ?"
                params.append(f"%{match_code}%")

            if sample_n is not None:
                inner = f"{columns_sql}\n{where}"
                query = f"SELECT TOP (?) * FROM (\n{inner}\n) t ORDER BY NEWID()"
                params.insert(0, sample_n)
            else:
                query = f"{columns_sql}\n{where}\nORDER BY A.ArtikelNummer ASC"

            cursor.execute(query, params)

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
                    desc = _PART_COLUMN_DESCRIPTIONS.get(field.name)
                    if desc:
                        metadata = {**field.metadata, b"description": desc.encode("utf-8")} if field.metadata else {b"description": desc.encode("utf-8")}
                        new_fields.append(field.with_metadata(metadata))
                    else:
                        new_fields.append(field)
                new_schema = pa.schema(new_fields)
                cols = [batch.column(i) for i in range(batch.num_columns)]
                batch = pa.RecordBatch.from_arrays(cols, schema=new_schema)
                queue.put_nowait(batch)
        except Exception as e:
            logger.exception("Error fetching parts")
            queue.put_nowait(None)  # signal error
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        queue.put_nowait(None)  # signal end

    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(None, _fetch)

    while True:
        batch = await queue.get()
        if batch is None:
            break
        yield batch
    await task  # propagate any exception
