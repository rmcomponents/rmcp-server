#!/usr/bin/env python3

import argparse
import os
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

import pyodbc
from dotenv import load_dotenv
from openpyxl import load_workbook


SHEET_NAME = None  # None = active sheet
HEADER_ROW = 1



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


