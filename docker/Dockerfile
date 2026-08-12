FROM python:3.12-slim

# Install ODBC Driver 18 for SQL Server and unixODBC
RUN apt-get update && apt-get install -y curl gnupg apt-transport-https \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1) Create a minimal package skeleton so pip can install dependencies
#    without the real source being present yet.
RUN mkdir -p alphaplan_mcp && echo '__version__ = "0.0.0"' > alphaplan_mcp/__init__.py

COPY pyproject.toml ./
COPY README.md ./README.md
RUN pip install --no-cache-dir .

# 2) Overwrite the skeleton with the real application source
COPY src/alphaplan_mcp ./alphaplan_mcp

# 3) Re‑install (dependencies are already cached, only the package is rebuilt)
RUN pip install --no-cache-dir .

# Run as non-root user
RUN useradd -m appuser
USER appuser

EXPOSE 9000

CMD ["python", "-m", "alphaplan_mcp"]
