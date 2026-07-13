#!/usr/bin/env python3

import os
from typing import Optional

import pyodbc
from dotenv import load_dotenv
from fastmcp import FastMCP


def build_connection() -> pyodbc.Connection:
    load_dotenv()

    server = os.getenv("SQLSERVER_HOST")
    database = os.getenv("SQLSERVER_DATABASE", "ApiPreise")
    username = os.getenv("SQLSERVER_USER")
    password = os.getenv("SQLSERVER_PASSWORD")
    driver = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
    trust_cert = os.getenv("SQLSERVER_TRUST_CERTIFICATE", "yes")

    if not server:
        raise RuntimeError("SQLSERVER_HOST is missing in .env")

    if username and password:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate={trust_cert};"
        )
    else:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate={trust_cert};"
        )

    conn = pyodbc.connect(conn_str)
    conn.autocommit = False
    return conn


mcp = FastMCP("AlphaPlanMCP")


@mcp.tool()
def list_customers(customer_name: Optional[str] = None, limit: Optional[int] = 1000, offset: Optional[int] = 0) -> list[dict]:
    """
    Search for customers in the database.
    Provide a customer_name (supports SQL LIKE wildcards, e.g., 'Acme%') to filter.
    Use limit and offset for large queries. Default limit is 1000 (highest possible value).

    If no name is provided, returns all non-archived customers.
    """
    if not customer_name:
        customer_name = "%"

    # cap 
    limit = min(1000, limit)

    conn = build_connection()
    cursor = conn.cursor()

    try:
        query = """
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
            OFFSET ? ROWS
            FETCH NEXT ? ROWS ONLY;
        """
        cursor.execute(query, customer_name, offset, limit)

        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()

        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def main():
    mcp.run(transport="http", port=9000)


if __name__ == "__main__":
    main()
