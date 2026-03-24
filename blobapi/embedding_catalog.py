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


# ── Registration helpers ──────────────────────────────────────────

def register_collection(engine, *,
                        collection_name: str,
                        table_schema: str = "domain",
                        table_name: str,
                        key_column: str,
                        label_column: str,
                        embedding_column: str = "embedding",
                        filter_columns: dict,
                        description: str | None = None):
    """Register an embedding collection in the catalog."""
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
            description=description,
        ))
        session.commit()


# ── Demo ──────────────────────────────────────────────────────────

def main():
    engine = create_engine(
        "postgresql+psycopg2:///rule4_test",
        connect_args={"host": "/tmp"},
    )

    # Register the disasters collection
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
        description="Enriched natural disaster events with hierarchical paths "
                    "(earthquakes, hurricanes, floods, wildfires, etc.)",
    )
    print("Registered 'disasters' collection\n")

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
