"""
Connection configuration loaded from connections.toml.

The config file stores connection parameters as structured data —
no environment variables. Each section is a named connection with
a dialect and backend-specific parameters.
"""

import sys
from pathlib import Path
from urllib.parse import quote_plus

if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore[no-redef]

CONFIG_FILENAME = "connections.toml"
SEARCH_PATHS = [
    Path.cwd(),
    Path.home() / ".config" / "blobapi",
    Path.home(),
]


def find_config() -> Path:
    """Locate connections.toml by searching standard paths."""
    for d in SEARCH_PATHS:
        p = d / CONFIG_FILENAME
        if p.exists():
            return p
    raise FileNotFoundError(
        f"No {CONFIG_FILENAME} found. Searched: {[str(d) for d in SEARCH_PATHS]}.\n"
        f"Copy connections.toml.example to connections.toml and fill in credentials."
    )


def load_config(path: Path | None = None) -> dict:
    """Load and return the full config dict."""
    if path is None:
        path = find_config()
    with open(path, "rb") as f:
        return tomllib.load(f)


def _sqlite_url(params: dict) -> str:
    db = params.get("database", "blobapi.db")
    return f"sqlite:///{db}"


def _duckdb_url(params: dict) -> str:
    db = params.get("database", "blobapi.duckdb")
    return f"duckdb:///{db}"


def _postgresql_url(params: dict) -> str:
    user = params.get("username", "")
    password = params.get("password", "")
    host = params.get("host", "localhost")
    port = params.get("port", 5432)
    db = params.get("database", "blobapi")

    # Build base URL — omit password if using cert auth
    if password:
        base = f"postgresql+psycopg://{user}:{quote_plus(password)}@{host}:{port}/{db}"
    else:
        base = f"postgresql+psycopg://{user}@{host}:{port}/{db}"

    # SSL cert parameters
    ssl_params = {}
    for key in ("sslmode", "sslcert", "sslkey", "sslrootcert", "gssencmode"):
        val = params.get(key)
        if val is not None:
            # Expand ~ in cert paths
            if key.startswith("ssl") and key != "sslmode":
                val = str(Path(val).expanduser())
            ssl_params[key] = val

    if ssl_params:
        qs = "&".join(f"{k}={quote_plus(str(v))}" for k, v in ssl_params.items())
        return f"{base}?{qs}"
    return base


def _sqlserver_url(params: dict) -> str:
    user = params.get("username", "")
    password = quote_plus(params.get("password", ""))
    host = params.get("host", "localhost")
    port = params.get("port", 1433)
    db = params.get("database", "blobapi")
    driver = quote_plus(params.get("driver", "ODBC Driver 18 for SQL Server"))
    trust = params.get("trust_server_certificate", False)

    conn_str = (
        f"DRIVER={{{params.get('driver', 'ODBC Driver 18 for SQL Server')}}};"
        f"SERVER={host},{port};"
        f"DATABASE={db};"
        f"UID={user};"
        f"PWD={params.get('password', '')};"
    )
    if trust:
        conn_str += "TrustServerCertificate=yes;"

    return f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}"


_URL_BUILDERS = {
    "sqlite": _sqlite_url,
    "duckdb": _duckdb_url,
    "postgresql": _postgresql_url,
    "pg": _postgresql_url,
    "sqlserver": _sqlserver_url,
    "mssql": _sqlserver_url,
}


def connection_url(connection_name: str = "default", config: dict | None = None) -> str:
    """
    Build a SQLAlchemy connection URL from a named connection in the config.

    Falls back to 'default' if the named connection is not found.
    """
    if config is None:
        config = load_config()

    params = config.get(connection_name)
    if params is None:
        raise KeyError(
            f"Connection '{connection_name}' not found in {CONFIG_FILENAME}. "
            f"Available: {list(config.keys())}"
        )

    dialect = params.get("dialect", connection_name)
    builder = _URL_BUILDERS.get(dialect)
    if builder is None:
        raise ValueError(
            f"Unknown dialect '{dialect}'. Supported: {list(_URL_BUILDERS.keys())}"
        )

    return builder(params)


def create_engine(connection_name: str = "default", config: dict | None = None, **kwargs):
    """
    Create a SQLAlchemy engine from a named connection.

    Handles dialect-specific engine options (e.g. suppressing the SQL Server
    2025 unrecognized version warning).
    """
    import warnings

    import sqlalchemy

    url = connection_url(connection_name, config)

    if config is None:
        config = load_config()
    dialect = config[connection_name].get("dialect", connection_name)

    if dialect in ("sqlserver", "mssql"):
        # SQL Server 2025 triggers "Unrecognized server version info" warning
        # from SQLAlchemy — safe to suppress globally since it fires on every
        # connection, not just engine creation.
        warnings.filterwarnings("ignore", message="Unrecognized server version")
        return sqlalchemy.create_engine(url, **kwargs)

    return sqlalchemy.create_engine(url, **kwargs)


def list_connections(config: dict | None = None) -> list[str]:
    """Return all connection names from the config."""
    if config is None:
        config = load_config()
    return list(config.keys())
