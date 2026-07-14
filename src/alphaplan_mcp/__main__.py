# Entry point for the AlphaPlan MCP server.
# Starts the FastMCP application based on the server lifespan and configuration.
from .server import main

if __name__ == "__main__":
    main()
