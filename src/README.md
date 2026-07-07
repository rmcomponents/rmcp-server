# SQL Server MCP Server

This project provides a simple Model Context Protocol (MCP) server using `fastmcp` that connects to a SQL Server database.

## Setup

1. Ensure you have Python 3.10+ installed.
2. Install dependencies using `uv`:
   ```bash
   uv sync
   ```
3. Create a `.env` file in the `src` directory based on `.env.example` and fill in your SQL Server credentials.

## Running the Server

To start the MCP server, run:

