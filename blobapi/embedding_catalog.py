"""Metadata-driven embedding collection query builder.

A registry (domain.embedding_catalog) declares embedding collections:
which table, what columns are filterable, and how to filter them.
The query builder reads the catalog and generates SQLAlchemy queries
from constraint dicts — no collection-specific Python code needed.

Usage:
    engine = create_engine("postgresql+psycopg2:///rule4_test", ...)
    catalog = EmbeddingCatalog(engine)

    # Discover available collections
    catalog.list_collections()
    # → [{'name': 'disasters', 'description': '...', 'filter_columns': {...}}, ...]

    # Query with constraints from LLM intent expansion
    results = catalog.query('disasters', {
        'peril_type': ['Earthquake', 'Tsunami'],
        'country': ['Japan'],
        'event_date': {'gte': '2011-01-01', 'lte': '2011-12-31'},
    })
"""
import json
from typing import Any

from sqlalchemy import (
    Column, String, Text, Integer, Float, JSON,
    MetaData, Table, create_engine, select, text as sa_text,
    and_, or_,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session


# ── Catalog model ──────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class EmbeddingCollectionDef(Base):
    """Registry of embedding collections.

    Each row declares an embedding table, its key/label/embedding columns,
    and which columns can be filtered (with filter type per column).

    Filter types:
        "in"       — equality / IN list (peril_type, country)
        "range"    — gte/lte range (event_date, magnitude)
        "contains" — substring match on a text column (hierarchical_path)
    """
    __tablename__ = "embedding_catalog"
    __table_args__ = {"schema": "domain"}

    collection_name: Mapped[str] = mapped_column(String(200), primary_key=True)
    table_schema: Mapped[str] = mapped_column(String(100), default="domain")
    table_name: Mapped[str] = mapped_column(String(200))
    key_column: Mapped[str] = mapped_column(String(200))
    label_column: Mapped[str] = mapped_column(String(200))
    embedding_column: Mapped[str] = mapped_column(String(200), default="embedding")
    filter_columns: Mapped[dict] = mapped_column(
        JSON,
        doc='{"column_name": "filter_type", ...} where filter_type is "in", "range", or "contains"',
    )
    column_types: Mapped[dict | None] = mapped_column(
        JSON, nullable=True,
        doc='{"column_name": "sqlalchemy_type", ...} e.g. {"event_date": "String(20)", "deaths": "Integer"}. '
            'Used to generate the embedding table via SQLAlchemy.',
    )
    embedding_template: Mapped[str | None] = mapped_column(
        Text, nullable=True,
        doc='Inja/Jinja2 template for generating the text to embed. '
            'Columns are available as {{ column_name }}. '
            'e.g. "{{ peril_type }} > {{ country }} > {{ event_date }} > {{ event_name }}"',
    )
    model_name: Mapped[str | None] = mapped_column(
        String(100), nullable=True, default="nomic",
        doc="Embedding model alias (as registered with be_load_model)",
    )
    batch_size: Mapped[int | None] = mapped_column(
        Integer, nullable=True, default=1000,
        doc="Number of rows to embed and write per batch",
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True)


# ── Generic query builder ─────────────────────────────────────────

class EmbeddingCatalog:
    """Metadata-driven query builder for embedding collections.

    Reads the catalog from PG, reflects the target tables, and builds
    parameterized SQLAlchemy queries from constraint dicts.
    """

    def __init__(self, engine):
        self.engine = engine
        self._metadata = MetaData()
        self._table_cache = {}

    def list_collections(self) -> list[dict]:
        """List all registered embedding collections."""
        with Session(self.engine) as session:
            defs = session.query(EmbeddingCollectionDef).all()
            return [
                {
                    "name": d.collection_name,
                    "table": f"{d.table_schema}.{d.table_name}",
                    "key": d.key_column,
                    "label": d.label_column,
                    "filter_columns": d.filter_columns,
                    "description": d.description,
                }
                for d in defs
            ]

    def _get_collection_def(self, session, name: str) -> EmbeddingCollectionDef:
        defn = session.get(EmbeddingCollectionDef, name)
        if not defn:
            available = [d.collection_name for d in
                         session.query(EmbeddingCollectionDef).all()]
            raise ValueError(
                f"Unknown collection '{name}'. Available: {available}"
            )
        return defn

    def _reflect_table(self, schema: str, table_name: str) -> Table:
        """Reflect a PG table lazily, caching the result."""
        key = f"{schema}.{table_name}"
        if key not in self._table_cache:
            self._table_cache[key] = Table(
                table_name, self._metadata,
                schema=schema, autoload_with=self.engine,
            )
        return self._table_cache[key]

    def build_query(self, collection_name: str, constraints: dict):
        """Build a SQLAlchemy SELECT from a constraint dict.

        Args:
            collection_name: registered collection (e.g., 'disasters')
            constraints: dict mapping column names to filter values.
                For "in" columns:      list of values, e.g. ['Japan', 'China']
                For "range" columns:   dict with 'gte' and/or 'lte' keys
                For "contains" columns: list of substrings (ORed)

        Returns:
            SQLAlchemy Select object (not yet executed).
            Only includes the key, label, filter columns, and embedding.
        """
        with Session(self.engine) as session:
            defn = self._get_collection_def(session, collection_name)

        table = self._reflect_table(defn.table_schema, defn.table_name)
        filter_spec = defn.filter_columns or {}

        # Build SELECT with key, label, all filter columns, and embedding
        columns = [
            table.c[defn.key_column],
            table.c[defn.label_column],
        ]
        for col_name in filter_spec:
            if col_name not in (defn.key_column, defn.label_column):
                columns.append(table.c[col_name])
        columns.append(table.c[defn.embedding_column])

        query = select(*columns)

        # Apply constraints
        for col_name, filter_type in filter_spec.items():
            if col_name not in constraints:
                continue

            col = table.c[col_name]
            value = constraints[col_name]

            if filter_type == "in":
                if isinstance(value, list):
                    query = query.where(col.in_(value))
                else:
                    query = query.where(col == value)

            elif filter_type == "range":
                if isinstance(value, dict):
                    if "gte" in value:
                        query = query.where(col >= value["gte"])
                    if "lte" in value:
                        query = query.where(col <= value["lte"])
                else:
                    query = query.where(col == value)

            elif filter_type == "contains":
                if isinstance(value, list):
                    query = query.where(
                        or_(*[col.contains(v) for v in value])
                    )
                else:
                    query = query.where(col.contains(value))

        return query

    def query(self, collection_name: str, constraints: dict,
              limit: int | None = None) -> list[dict]:
        """Execute a filtered query and return results as dicts.

        Same as build_query() but executes and returns rows.
        Does NOT include the embedding vector (too large for display).
        """
        q = self.build_query(collection_name, constraints)
        if limit:
            q = q.limit(limit)

        with Session(self.engine) as session:
            rows = session.execute(q).all()
            # Return all columns except embedding
            col_names = [c.key for c in q.selected_columns
                         if c.key != 'embedding']
            return [
                {name: getattr(row, name, None) for name in col_names}
                for row in rows
            ]

    def query_count(self, collection_name: str, constraints: dict) -> int:
        """Count matching rows without fetching embeddings."""
        from sqlalchemy import func
        q = self.build_query(collection_name, constraints)
        # Replace columns with count(*)
        count_q = select(func.count()).select_from(q.subquery())
        with Session(self.engine) as session:
            return session.execute(count_q).scalar()


# ── Table generation from metadata ────────────────────────────────

# Map from string type names (stored in JSON) to SQLAlchemy types
_TYPE_MAP = {
    "String": lambda args: String(int(args)) if args else String(200),
    "Text": lambda _: Text,
    "Integer": lambda _: Integer,
    "Float": lambda _: Float,
    "REAL[]": lambda _: ARRAY(Float),
}


def _parse_type_spec(spec: str):
    """Parse a type spec like 'String(200)' or 'Integer' into a SA type."""
    if "(" in spec:
        name, args = spec.split("(", 1)
        args = args.rstrip(")")
    else:
        name, args = spec, None

    factory = _TYPE_MAP.get(name)
    if not factory:
        return String(200)  # safe default
    result = factory(args)
    return result if not callable(result) else result


def generate_embedding_table(engine, collection_name: str):
    """Generate (CREATE IF NOT EXISTS) the embedding table from catalog metadata.

    Reads column_types from the catalog entry and creates a table with:
    - All declared columns (with their types)
    - key_column as PRIMARY KEY
    - embedding column as REAL[]

    Returns the reflected SQLAlchemy Table object.
    """
    with Session(engine) as session:
        defn = session.get(EmbeddingCollectionDef, collection_name)
        if not defn:
            raise ValueError(f"Unknown collection: {collection_name}")

        schema = defn.table_schema
        table_name = defn.table_name
        key_col = defn.key_column
        label_col = defn.label_column
        emb_col = defn.embedding_column
        col_types = defn.column_types or {}

    # Build column list
    from sqlalchemy import ARRAY as SA_ARRAY
    from sqlalchemy.dialects.postgresql import REAL as PG_REAL

    columns = []

    # Key column — always first, always primary key
    key_type = _parse_type_spec(col_types.get(key_col, "Text"))
    columns.append(Column(key_col, key_type, primary_key=True))

    # Label column (if different from key)
    if label_col != key_col:
        label_type = _parse_type_spec(col_types.get(label_col, "Text"))
        columns.append(Column(label_col, label_type))

    # All other declared columns
    for col_name, type_spec in col_types.items():
        if col_name in (key_col, label_col, emb_col):
            continue
        columns.append(Column(col_name, _parse_type_spec(type_spec)))

    # Embedding column — always last
    columns.append(Column(emb_col, SA_ARRAY(Float)))

    metadata = MetaData()
    table = Table(table_name, metadata, *columns, schema=schema)
    metadata.create_all(engine, checkfirst=True)

    return table


# ── Generic collection population ─────────────────────────────────

def populate_collection(engine, duck, collection_name: str,
                        source_table: str = "source_data",
                        pg_conn=None):
    """Embed and persist a collection from a DuckDB temp table.

    Architecture: DuckDB computes (template rendering + embedding),
    Python orchestrates batches, PG persists via psycopg2.

    The caller is responsible for:
    1. Staging data into a DuckDB temp table (any name, default 'source_data')
    2. Ensuring columns match the catalog's column_types keys
    3. Loading the embedding model (be_load_model)
    4. Providing a psycopg2 connection for direct PG writes

    This function:
    1. Reads the template, columns, batch_size from the catalog
    2. Renders the template per row via bt_template_render
    3. Embeds via be_embed
    4. Writes to PG via psycopg2 (not the scanner)
    5. Logs progress with timestamps

    Args:
        engine: SQLAlchemy engine (for catalog reads)
        duck: duckdb connection (with extensions loaded, model loaded)
        collection_name: registered collection name
        source_table: name of DuckDB temp table with source data
        pg_conn: psycopg2 connection for writes (if None, uses engine)
    """
    import logging
    log = logging.getLogger(__name__)

    # Read catalog metadata
    with Session(engine) as session:
        defn = session.get(EmbeddingCollectionDef, collection_name)
        if not defn:
            raise ValueError(f"Unknown collection: {collection_name}")

    template = defn.embedding_template
    col_types = defn.column_types or {}
    batch_size = defn.batch_size or 1000
    model = defn.model_name or "nomic"
    schema = defn.table_schema
    table_name = defn.table_name
    emb_col = defn.embedding_column
    data_cols = [c for c in col_types if c != emb_col]

    if not template:
        raise ValueError(f"No embedding_template for collection '{collection_name}'")

    log.info(f"Populating '{collection_name}': template='{template}', "
             f"model={model}, batch_size={batch_size}")

    # Store template in DuckDB variable for clean SQL
    duck.execute("SET VARIABLE embed_template = ?", [template])

    # Verify source table has required columns
    source_cols = [r[0] for r in duck.execute(
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{source_table}'"
    ).fetchall()]
    missing = [c for c in data_cols if c not in source_cols]
    if missing:
        raise ValueError(f"Source table '{source_table}' missing columns: {missing}")

    # Add row numbers for batching
    duck.execute(f"""
        CREATE OR REPLACE TEMP TABLE _populate_numbered AS
        SELECT *, ROW_NUMBER() OVER () AS _rn
        FROM {source_table}
    """)
    n_total = duck.execute("SELECT count(*) FROM _populate_numbered").fetchone()[0]
    log.info(f"  {n_total} rows to embed")

    # Sample template rendering
    sample = duck.execute(f"""
        SELECT bt_template_render(getvariable('embed_template'),
            json_object({', '.join(f"'{c}', {c}" for c in data_cols)})
        ) FROM _populate_numbered LIMIT 1
    """).fetchone()[0]
    log.info(f"  Sample: {sample}")

    # Build the json_object expression for template rendering
    json_obj_expr = "json_object(" + ", ".join(f"'{c}', {c}" for c in data_cols) + ")"

    # Manage PG connection
    own_pg = pg_conn is None
    if own_pg:
        import psycopg2
        pg_conn = psycopg2.connect(dbname="rule4_test", host="/tmp")

    pg_cur = pg_conn.cursor()
    pg_cur.execute(f"DELETE FROM {schema}.{table_name}")
    pg_conn.commit()
    log.info(f"  Cleared {schema}.{table_name}")

    # Build INSERT statement
    all_cols = data_cols + [emb_col]
    placeholders = ", ".join(["%s"] * len(all_cols))
    insert_sql = (f"INSERT INTO {schema}.{table_name} "
                  f"({', '.join(all_cols)}) VALUES ({placeholders})")

    # Batch loop
    import time
    n_batches = (n_total + batch_size - 1) // batch_size
    total_inserted = 0
    total_errors = 0
    t_start = time.perf_counter()

    for batch_idx in range(n_batches):
        rn_start = batch_idx * batch_size + 1
        rn_end = rn_start + batch_size - 1
        t_batch = time.perf_counter()

        try:
            # Compute embeddings in DuckDB
            select_cols = ", ".join(data_cols)
            rows = duck.execute(f"""
                SELECT {select_cols},
                       be_embed('{model}',
                           bt_template_render(getvariable('embed_template'),
                               {json_obj_expr}
                           )
                       ) AS {emb_col}
                FROM _populate_numbered
                WHERE _rn BETWEEN {rn_start} AND {rn_end}
            """).fetchall()

            # Write to PG
            for row in rows:
                *cols, embedding = row
                emb_str = "{" + ",".join(str(v) for v in embedding) + "}"
                pg_cur.execute(insert_sql, (*cols, emb_str))
            pg_conn.commit()

            batch_elapsed = time.perf_counter() - t_batch
            total_inserted += len(rows)
            overall_elapsed = time.perf_counter() - t_start
            rate = total_inserted / overall_elapsed if overall_elapsed > 0 else 0
            eta = (n_total - total_inserted) / rate if rate > 0 else 0

            if (batch_idx + 1) % 10 == 0 or batch_idx == 0 or batch_idx == n_batches - 1:
                log.info(
                    f"  batch {batch_idx+1}/{n_batches}: {len(rows)} in {batch_elapsed:.1f}s | "
                    f"total {total_inserted}/{n_total} ({100*total_inserted/n_total:.0f}%) | "
                    f"{rate:.0f}/sec | ETA {eta:.0f}s"
                )

        except Exception as e:
            total_errors += 1
            log.error(f"  batch {batch_idx+1}/{n_batches} FAILED: {e}")
            pg_conn.rollback()

    total_elapsed = time.perf_counter() - t_start
    log.info(f"  Complete: {total_inserted} embedded, {total_errors} errors, "
             f"{total_elapsed:.0f}s ({total_inserted/total_elapsed:.0f}/sec)")

    # Cleanup
    duck.execute("DROP TABLE IF EXISTS _populate_numbered")
    if own_pg:
        pg_cur.close()
        pg_conn.close()

    return total_inserted


# ── Index management ─────────────────────────────────────────────

def ensure_indexes(engine, collection_name: str):
    """Add indexes to an existing embedding table based on catalog metadata.

    Reads filter_columns from the catalog and creates appropriate indexes:
        "in"       → B-tree index
        "range"    → B-tree index
        "contains" → GIN trigram index (requires pg_trgm extension)

    Idempotent — uses CREATE INDEX IF NOT EXISTS. Safe to call on tables
    that already have some or all indexes.

    Does NOT create indexes on the embedding column — that requires
    pgvector (HNSW/IVFFlat) and should be done explicitly when the
    collection is large enough to warrant it.
    """
    with Session(engine) as session:
        defn = session.get(EmbeddingCollectionDef, collection_name)
        if not defn:
            raise ValueError(f"Unknown collection: {collection_name}")

        schema = defn.table_schema
        table_name = defn.table_name
        filter_spec = defn.filter_columns or {}
        qualified = f"{schema}.{table_name}"

    with engine.connect() as conn:
        for col_name, filter_type in filter_spec.items():
            idx_name = f"ix_{table_name}_{col_name}"

            if filter_type in ("in", "range"):
                conn.execute(sa_text(
                    f'CREATE INDEX IF NOT EXISTS {idx_name} '
                    f'ON {qualified} ({col_name})'
                ))

            elif filter_type == "contains":
                # GIN trigram index for substring search
                # Requires: CREATE EXTENSION IF NOT EXISTS pg_trgm;
                try:
                    conn.execute(sa_text(
                        'CREATE EXTENSION IF NOT EXISTS pg_trgm'
                    ))
                    conn.execute(sa_text(
                        f'CREATE INDEX IF NOT EXISTS {idx_name} '
                        f'ON {qualified} USING gin ({col_name} gin_trgm_ops)'
                    ))
                except Exception:
                    # Fall back to B-tree if pg_trgm not available
                    conn.execute(sa_text(
                        f'CREATE INDEX IF NOT EXISTS {idx_name} '
                        f'ON {qualified} ({col_name})'
                    ))

        conn.commit()


def ensure_indexes_all(engine):
    """Add indexes for ALL registered embedding collections.

    Idempotent — safe to run repeatedly.
    """
    with Session(engine) as session:
        collections = session.query(EmbeddingCollectionDef).all()
        names = [c.collection_name for c in collections]

    for name in names:
        try:
            ensure_indexes(engine, name)
        except Exception as e:
            # Table may not exist yet — skip silently
            pass


# ── Registration helpers ──────────────────────────────────────────

def register_collection(engine, *,
                        collection_name: str,
                        table_schema: str = "domain",
                        table_name: str,
                        key_column: str,
                        label_column: str,
                        embedding_column: str = "embedding",
                        filter_columns: dict,
                        column_types: dict | None = None,
                        embedding_template: str | None = None,
                        description: str | None = None,
                        create_table: bool = False):
    """Register an embedding collection in the catalog.

    Args:
        collection_name: unique name for this collection
        table_schema: PG schema (default 'domain')
        table_name: target table name
        key_column: primary key column
        label_column: human-readable label column
        embedding_column: column holding the embedding vector
        filter_columns: {"col": "in|range|contains"} for query building
        column_types: {"col": "String(200)|Integer|Float|Text"} for table generation
        embedding_template: inja template for generating text to embed
        description: human-readable description
        create_table: if True, generate the table from column_types
    """
    EmbeddingCollectionDef.__table__.create(engine, checkfirst=True)

    with Session(engine) as session:
        session.merge(EmbeddingCollectionDef(
            collection_name=collection_name,
            table_schema=table_schema,
            table_name=table_name,
            key_column=key_column,
            label_column=label_column,
            embedding_column=embedding_column,
            filter_columns=filter_columns,
            column_types=column_types,
            embedding_template=embedding_template,
            description=description,
        ))
        session.commit()

    if create_table and column_types:
        generate_embedding_table(engine, collection_name)


# ── Demo ──────────────────────────────────────────────────────────

def main():
    engine = create_engine(
        "postgresql+psycopg2:///rule4_test",
        connect_args={"host": "/tmp"},
    )

    # Register the disasters collection (existing table)
    register_collection(
        engine,
        collection_name="disasters",
        table_name="disaster_event_embedding",
        key_column="hierarchical_path",
        label_column="event_name",
        filter_columns={
            "peril_type": "in",
            "country": "in",
            "event_date": "range",
            "hierarchical_path": "contains",
        },
        column_types={
            "hierarchical_path": "Text",
            "event_name": "Text",
            "peril_type": "String(100)",
            "country": "String(200)",
            "event_date": "String(20)",
        },
        embedding_template="{{ peril_type }} > {{ country }} > {{ event_date }} > {{ event_name }}",
        description="Enriched natural disaster events with hierarchical paths "
                    "(earthquakes, hurricanes, floods, wildfires, etc.)",
    )
    print("Registered 'disasters' collection")

    # Register a hypothetical companies collection — demonstrates table generation
    register_collection(
        engine,
        collection_name="companies",
        table_name="company_embedding",
        key_column="cik",
        label_column="company_name",
        filter_columns={
            "sic": "in",
            "sic_description": "contains",
            "state": "in",
            "filer_category": "in",
        },
        column_types={
            "cik": "String(10)",
            "company_name": "Text",
            "ticker": "String(10)",
            "sic": "String(10)",
            "sic_description": "String(200)",
            "state": "String(10)",
            "filer_category": "String(100)",
        },
        embedding_template="{{ sic_description }} > {{ state }} > {{ company_name }} ({{ ticker }})",
        description="SEC-registered public companies with SIC classification",
        create_table=True,  # generates the table from column_types
    )
    print("Registered 'companies' collection (table generated)\n")

    catalog = EmbeddingCatalog(engine)

    # List collections
    for c in catalog.list_collections():
        print(f"  {c['name']}: {c['table']}")
        print(f"    filters: {c['filter_columns']}")
        print(f"    {c['description']}\n")

    # Test queries
    queries = [
        ("Japanese earthquake/tsunami 2011", {
            "peril_type": ["Earthquake", "Tsunami"],
            "country": ["Japan"],
            "event_date": {"gte": "2011-01-01", "lte": "2011-12-31"},
        }),
        ("US hurricanes 2005", {
            "peril_type": ["Tropical Cyclone"],
            "country": ["United States"],
            "event_date": {"gte": "2005-01-01", "lte": "2005-12-31"},
        }),
        ("California wildfires (any year)", {
            "peril_type": ["Wildfire"],
            "hierarchical_path": ["California"],
        }),
        ("All floods in Bangladesh", {
            "peril_type": ["Flood"],
            "country": ["Bangladesh"],
        }),
        ("Everything (no constraints)", {}),
    ]

    for desc, constraints in queries:
        count = catalog.query_count("disasters", constraints)
        results = catalog.query("disasters", constraints, limit=5)
        print(f"  {desc}: {count} events")
        for r in results:
            print(f"    {r.get('event_name', '?'):<45} "
                  f"{r.get('peril_type', '?'):>18} "
                  f"{r.get('country') or '':>15} "
                  f"{r.get('event_date') or '':>10}")
        if count > 5:
            print(f"    ... ({count - 5} more)")
        print()


if __name__ == "__main__":
    main()
