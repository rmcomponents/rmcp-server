# Defines the typed Settings class using pydantic-settings.
# Loads and validates configuration at startup, including Redis,
# result-set limits, and SQL Server settings.
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    DEBUG: bool = False

    MCP_HOST: str = "0.0.0.0"
    MCP_PORT: int = 9000

    REDIS_URL: str = "redis://redis:6379/0"
    REDIS_KEY_PREFIX: str = "alphaplan-mcp"

    RESULTSET_CHUNK_ROWS: int = 5000
    RESULTSET_IDLE_TTL_SECONDS: int = 3600
    RESULTSET_MAX_TTL_SECONDS: int = 86400
    RESULTSET_MAX_ROWS: int = 250000
    RESULTSET_MAX_BYTES: int = 268435456
    RESULTSET_MAX_GROUPS: int = 10000

    TOOL_MAX_ROWS: int = 200
    TOOL_MAX_DISTINCT_VALUES: int = 200
    TOOL_MAX_FILTER_CLAUSES: int = 20
    TOOL_MAX_GROUP_COLUMNS: int = 3
    TOOL_MAX_AGGREGATIONS: int = 10

    SQLSERVER_HOST: str = ""
    SQLSERVER_DATABASE: str = "ApiPreise"
    SQLSERVER_USER: str = ""
    SQLSERVER_PASSWORD: str = ""
    SQLSERVER_DRIVER: str = "ODBC Driver 18 for SQL Server"
    SQLSERVER_TRUST_CERTIFICATE: str = "yes"
    SQLSERVER_QUERY_TIMEOUT_SECONDS: int = 120

    MOUSER_API_KEY: str = ""
    MOUSER_API_BASE_URL: str = "https://api.mouser.com/api/v1"
    MOUSER_TIMEOUT_SECONDS: float = 15.0
    MOUSER_CACHE_TTL_HOURS: float = 168.0  # 7 days

settings = Settings()
