FROM python:3.12-slim

# Install ODBC Driver 18 for SQL Server and unixODBC
RUN apt-get update && apt-get install -y curl gnupg apt-transport-https \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project files
COPY src/pyproject.toml src/README.md ./src/
COPY src/alphaplan_mcp ./src/alphaplan_mcp

# Install the project
RUN pip install --no-cache-dir ./src

# Run as non-root user
RUN useradd -m appuser
USER appuser

EXPOSE 9000

CMD ["python", "-m", "alphaplan_mcp"]
