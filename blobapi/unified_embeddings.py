"""Unified embedding table: one table for all collections.

Alternative physical layout to per-collection tables. Both contain
the same logical data; the catalog describes the schema either way.

Schema:
    domain.embedding_unified (
        collection   VARCHAR     -- 'disasters', 'geonames', 'companies'
        key          TEXT        -- entity primary key
        label        TEXT        -- human-readable name
        model        VARCHAR     -- 'nomic-embed-text-v1.5'
        properties   JSONB       -- typed columns, varies per collection
        embedding    REAL[]      -- vector (pgvector if installed, else REAL[])
        PRIMARY KEY (collection, key, model)
    )

Usage:
    from blobapi.unified_embeddings import UnifiedEmbeddings

    ue = UnifiedEmbeddings(engine)
    ue.create_table()

    # Populate from an existing per-collection table
    ue.import_collection(engine, 'disasters')

    # Query within a collection (same as per-collection, but one table)
    results = ue.query('disasters', {'peril_type': ['Earthquake']})

    # Cross-domain search: "what domain does 'Mercury' belong to?"
    results = ue.search_all_collections(query_embedding, limit=5)

    # Best match per collection
    results = ue.search_per_collection(query_embedding, limit_per=3)
"""
import json
import logging
from typing import Any

from sqlalchemy import (
    Column, String, Text, Float, Index, PrimaryKeyConstraint,
    create_engine, select, text as sa_text, func,
    or_,
)
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

from blobapi.embedding_catalog import EmbeddingCollectionDef

log = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class UnifiedEmbeddingRow(Base):
    """One row per (collection, key, model) triple.

    properties is JSONB containing all the typed columns from the
    per-collection table. The catalog's filter_columns describes
    which keys exist and how to filter them.
    """
    __tablename__ = "embedding_unified"
    __table_args__ = (
        PrimaryKeyConstraint("collection", "key", "model"),
        Index("ix_unified_collection", "collection"),
        Index("ix_unified_label", "label"),
        Index("ix_unified_props", "properties", postgresql_using="gin"),
        {"schema": "domain"},
    )

    collection: Mapped[str] = mapped_column(String(200))
    key: Mapped[str] = mapped_column(Text)
    label: Mapped[str] = mapped_column(Text)
    model: Mapped[str] = mapped_column(String(100))
    properties: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    embedding = Column(ARRAY(Float))


class UnifiedEmbeddings:
    """Query and manage the unified embedding table."""

    def __init__(self, engine):
        self.engine = engine

    def create_table(self):
        """Create the unified table if it doesn't exist."""
        UnifiedEmbeddingRow.__table__.create(self.engine, checkfirst=True)

    def import_collection(self, collection_name: str, model_name: str = "nomic"):
        """Import a per-collection embedding table into the unified table.

        Reads the catalog to find the per-collection table, extracts
        the key, label, and all other columns as JSONB properties.

        Idempotent — deletes existing rows for this collection+model first.
        """
        with Session(self.engine) as session:
            defn = session.get(EmbeddingCollectionDef, collection_name)
            if not defn:
                raise ValueError(f"Unknown collection: {collection_name}")

        schema = defn.table_schema
        table_name = defn.table_name
        key_col = defn.key_column
        label_col = defn.label_column
        emb_col = defn.embedding_column
        col_types = defn.column_types or {}

        # Build the properties JSON from all non-key, non-label, non-embedding columns
        prop_cols = [c for c in col_types if c not in (key_col, label_col, emb_col)]

        if prop_cols:
            json_build = "jsonb_build_object(" + ", ".join(
                f"'{c}', {c}" for c in prop_cols
            ) + ")"
        else:
            json_build = "'{}'::jsonb"

        sql = f"""
            DELETE FROM domain.embedding_unified
            WHERE collection = :collection AND model = :model;

            INSERT INTO domain.embedding_unified (collection, key, label, model, properties, embedding)
            SELECT :collection,
                   {key_col}::TEXT,
                   {label_col}::TEXT,
                   :model,
                   {json_build},
                   {emb_col}
            FROM {schema}.{table_name}
        """

        with self.engine.connect() as conn:
            conn.execute(sa_text(sql.split(";")[0].strip() + ";"),
                         {"collection": collection_name, "model": model_name})
            conn.execute(sa_text(sql.split(";")[1].strip()),
                         {"collection": collection_name, "model": model_name})
            conn.commit()

        with self.engine.connect() as conn:
            n = conn.execute(sa_text(
                "SELECT count(*) FROM domain.embedding_unified "
                "WHERE collection = :c AND model = :m"
            ), {"c": collection_name, "m": model_name}).scalar()

        log.info(f"Imported {n} rows into unified table for '{collection_name}'")
        return n

    def import_all_collections(self):
        """Import all registered collections into the unified table."""
        with Session(self.engine) as session:
            collections = session.query(EmbeddingCollectionDef).all()

        total = 0
        for defn in collections:
            try:
                n = self.import_collection(
                    defn.collection_name,
                    defn.model_name or "nomic",
                )
                total += n
            except Exception as e:
                log.error(f"Failed to import '{defn.collection_name}': {e}")

        log.info(f"Unified table: {total} total rows across {len(collections)} collections")
        return total

    def query(self, collection_name: str, constraints: dict | None = None,
              limit: int = 10) -> list[dict]:
        """Query within a single collection using JSONB property filters.

        Args:
            collection_name: which collection to search
            constraints: {"property_name": value_or_list} — applied to properties JSONB
            limit: max rows to return
        """
        # Read filter types from catalog
        with Session(self.engine) as session:
            defn = session.get(EmbeddingCollectionDef, collection_name)
            filter_spec = defn.filter_columns if defn else {}

        query = select(
            UnifiedEmbeddingRow.key,
            UnifiedEmbeddingRow.label,
            UnifiedEmbeddingRow.properties,
        ).where(
            UnifiedEmbeddingRow.collection == collection_name
        )

        if constraints:
            for col_name, value in constraints.items():
                if col_name not in (filter_spec or {}):
                    continue

                filter_type = filter_spec[col_name]
                prop_ref = UnifiedEmbeddingRow.properties[col_name].astext

                if filter_type == "in":
                    if isinstance(value, list):
                        query = query.where(prop_ref.in_(value))
                    else:
                        query = query.where(prop_ref == str(value))

                elif filter_type == "range":
                    if isinstance(value, dict):
                        if "gte" in value:
                            query = query.where(prop_ref >= value["gte"])
                        if "lte" in value:
                            query = query.where(prop_ref <= value["lte"])

                elif filter_type == "contains":
                    if isinstance(value, list):
                        query = query.where(
                            or_(*[prop_ref.contains(v) for v in value])
                        )
                    else:
                        query = query.where(prop_ref.contains(value))

        query = query.limit(limit)

        with Session(self.engine) as session:
            rows = session.execute(query).all()
            return [
                {"key": r.key, "label": r.label, **(r.properties or {})}
                for r in rows
            ]

    def search_all_collections(self, query_embedding_sql: str, limit: int = 10):
        """Cross-domain nearest-neighbor search (brute force).

        Returns the closest matches across ALL collections.
        query_embedding_sql is a SQL expression that produces the embedding
        (e.g., "be_embed('nomic', 'earthquake japan')").

        Must be called via DuckDB or raw PG with the embedding as a parameter.
        """
        sql = f"""
            SELECT collection, key, label, properties,
                   1 - (embedding <=> :qvec) AS similarity
            FROM domain.embedding_unified
            ORDER BY embedding <=> :qvec
            LIMIT :limit
        """
        return sql  # Return the SQL — caller executes with their connection

    def search_per_collection(self, limit_per: int = 3):
        """SQL for best match per collection (DISTINCT ON pattern).

        Returns the SQL template — caller provides :qvec parameter.
        """
        sql = f"""
            SELECT DISTINCT ON (collection)
                   collection, key, label, properties,
                   1 - (embedding <=> :qvec) AS similarity
            FROM domain.embedding_unified
            ORDER BY collection, embedding <=> :qvec
        """
        return sql

    def stats(self) -> list[dict]:
        """Row counts per collection."""
        with Session(self.engine) as session:
            rows = session.execute(sa_text("""
                SELECT collection, model, count(*) AS n
                FROM domain.embedding_unified
                GROUP BY collection, model
                ORDER BY n DESC
            """)).fetchall()
            return [{"collection": r[0], "model": r[1], "n": r[2]} for r in rows]


# ── Demo ──────────────────────────────────────────────────────────

def main():
    import time

    engine = create_engine(
        "postgresql+psycopg2:///rule4_test",
        connect_args={"host": "/tmp"},
    )

    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S", level=logging.INFO)

    ue = UnifiedEmbeddings(engine)
    ue.create_table()
    log.info("Unified table created")

    # Import all collections
    t0 = time.perf_counter()
    total = ue.import_all_collections()
    elapsed = time.perf_counter() - t0
    log.info(f"Imported {total} rows in {elapsed:.1f}s")

    # Stats
    for s in ue.stats():
        log.info(f"  {s['collection']:<20} {s['n']:>7} rows  model={s['model']}")

    # Query within a collection
    log.info("\nQuery: disasters, Japan earthquakes 2011")
    results = ue.query("disasters", {
        "peril_type": ["Earthquake", "Tsunami"],
        "country": ["Japan"],
        "event_date": {"gte": "2011-01-01", "lte": "2011-12-31"},
    })
    for r in results:
        log.info(f"  {r['label']:<50} {r.get('peril_type', ''):>18} {r.get('event_date', '')}")

    log.info("\nQuery: companies, SIC 6331 (Fire, Marine & Casualty)")
    results = ue.query("companies", {"sic": ["6331"]}, limit=5)
    for r in results:
        log.info(f"  {r['label']:<50} {r.get('ticker', ''):>8} {r.get('state', '')}")

    log.info("\nQuery: geonames, Japan cities")
    results = ue.query("geonames", {"country_name": ["Japan"]}, limit=5)
    for r in results:
        log.info(f"  {r['label']:<30} pop={r.get('population', ''):>10}")

    # Cross-domain search SQL (for reference — needs pgvector to execute)
    log.info("\nCross-domain search SQL:")
    log.info(ue.search_per_collection())


if __name__ == "__main__":
    main()
