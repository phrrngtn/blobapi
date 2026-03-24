"""One-call DuckDB setup for blobapi: extensions + macros + adapter table.

Usage:
    import duckdb
    from blobapi.duckdb_setup import setup

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    setup(con)
    # Now you can call:
    #   SELECT * FROM llm_adapt('intent_to_constraints', json_object('intent', '...'));
    #   SELECT * FROM edgar_tickers();

Idempotent — safe to call multiple times on the same connection.
"""
import pathlib

_HERE = pathlib.Path(__file__).parent
_ROOT = _HERE.parent  # blobapi repo root


def setup(con, *, load_adapters: bool = True, pg_dsn: str | None = None):
    """Initialize DuckDB with all blobapi dependencies.

    Loads extensions, registers macros, creates adapter tables, and
    optionally loads adapter definitions from YAML.

    Args:
        con: duckdb.DuckDBPyConnection
        load_adapters: if True, load adapter YAML files into llm_adapter table
        pg_dsn: if set, attach PG and load http_adapt macros.
                Format: "host=/tmp dbname=rule4_test"
    """
    # ── Load dependency extensions ────────────────────────────────
    # Each setup() is idempotent — safe to call multiple times.
    try:
        import blobhttp_duckdb
        blobhttp_duckdb.setup(con)
    except ImportError:
        pass  # blobhttp not installed — HTTP macros won't be available

    try:
        import blobtemplates_duckdb
        _load_if_not_loaded(con, blobtemplates_duckdb.extension_path())
    except ImportError:
        pass  # blobtemplates not installed — template rendering won't be available

    try:
        import blobfilters_duckdb
        _load_if_not_loaded(con, blobfilters_duckdb.extension_path())
    except ImportError:
        pass

    try:
        import blobboxes_duckdb
        _load_if_not_loaded(con, blobboxes_duckdb.extension_path())
    except ImportError:
        pass

    # ── LLM adapter table + macros ────────────────────────────────
    adapter_sql = _ROOT / "sql" / "create_llm_adapter.sql"
    if adapter_sql.exists():
        con.execute(adapter_sql.read_text())

    # Load llm_adapt macro (from blobhttp's bundled SQL or repo)
    for path in [
        # Prefer bundled SQL from the blobhttp package
        _try_blobhttp_sql("llm_adapt.sql"),
        _try_blobhttp_sql("llm_complete.sql"),
        # Fallback to repo paths
        _ROOT.parent / "blobhttp" / "sql" / "llm_adapt.sql",
        _ROOT.parent / "blobhttp" / "sql" / "llm_complete.sql",
    ]:
        if path and path.exists():
            try:
                con.execute(path.read_text())
            except Exception:
                pass  # dependency not loaded yet

    # ── Load adapter definitions from YAML ────────────────────────
    if load_adapters:
        load_adapter_sql = _ROOT / "sql" / "load_llm_adapters.sql"
        if load_adapter_sql.exists():
            try:
                con.execute(load_adapter_sql.read_text())
            except Exception:
                pass  # bt_yaml_to_json not available

    # ── PostgreSQL + HTTP adapter macros ──────────────────────────
    if pg_dsn:
        try:
            con.execute("INSTALL postgres; LOAD postgres;")
            con.execute(f"ATTACH '{pg_dsn}' AS pg (TYPE POSTGRES)")
        except Exception:
            pass  # already attached or PG not available

        http_adapt_sql = _ROOT / "sql" / "http_adapt.sql"
        if http_adapt_sql.exists():
            try:
                con.execute(http_adapt_sql.read_text())
            except Exception:
                pass


def _load_if_not_loaded(con, path: str):
    """Load a DuckDB extension, ignoring if already loaded."""
    try:
        con.execute(f"LOAD '{path}'")
    except Exception:
        pass  # already loaded or incompatible


def _try_blobhttp_sql(filename: str):
    """Try to find a SQL file in the blobhttp package's bundled sql dir."""
    try:
        import blobhttp_duckdb
        sql_dir = pathlib.Path(blobhttp_duckdb.__file__).parent / "sql"
        path = sql_dir / filename
        if path.exists():
            return path
    except ImportError:
        pass
    return None
