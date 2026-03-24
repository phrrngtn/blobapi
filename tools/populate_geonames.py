"""Populate GeoNames embedding collection from PG gazetteer data.

Usage:
    populate_geonames.py [options]
    populate_geonames.py -h | --help

Options:
    --pg-dsn DSN          PG connection string [default: host=/tmp dbname=rule4_test]
    --batch-size N        Rows per batch [default: 1000]
    --skip-indexes        Don't create indexes after populating
    --skip-blobfilter     Don't rebuild the blobfilter after populating
    -h --help             Show this help

Architecture:
    DuckDB: stage data from PG (fast read via scanner), compute embeddings
    Python: orchestrate batches via populate_collection()
    PG: persist via psycopg2 (fast write, not via scanner)
"""
import os
import sys
import time
import glob
import logging
import pathlib

from docopt import docopt

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
    stream=sys.stderr,
)
log = logging.getLogger(__name__)


def main():
    args = docopt(__doc__)
    pg_dsn = args["--pg-dsn"]
    batch_size = int(args["--batch-size"])

    import duckdb
    import psycopg2
    from sqlalchemy import create_engine
    from blobapi.embedding_catalog import (
        register_collection, populate_collection, ensure_indexes,
    )

    engine = create_engine(f"postgresql+psycopg2:///{pg_dsn.split('dbname=')[1].split()[0]}",
                           connect_args={"host": pg_dsn.split("host=")[1].split()[0]})

    # ── Register collection in catalog ────────────────────────────
    log.info("Registering GeoNames collection...")
    register_collection(
        engine,
        collection_name="geonames",
        table_name="geonames_embedding",
        key_column="geonameid",
        label_column="place_name",
        filter_columns={
            "country_name": "in",
            "country_code": "in",
            "admin1_name": "in",
            "feature_code": "in",
            "continent": "in",
            "full_path": "contains",
            "population": "range",
        },
        column_types={
            "geonameid": "Integer",
            "place_name": "Text",
            "place_ascii": "Text",
            "country_name": "String(200)",
            "country_code": "String(3)",
            "continent": "String(20)",
            "admin1_name": "String(200)",
            "admin2_name": "String(200)",
            "feature_code": "String(10)",
            "population": "Integer",
            "latitude": "Float",
            "longitude": "Float",
            "full_path": "Text",
        },
        embedding_template="{{ full_path }}",
        description="GeoNames places (131K) with hierarchical paths",
        create_table=True,
    )

    # ── Setup DuckDB ──────────────────────────────────────────────
    log.info("Loading extensions...")

    # Suppress llama.cpp Metal diagnostics during model load
    stderr_fd = os.dup(2)
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, 2)

    import blobembed_duckdb, blobfilters_duckdb

    MODEL = glob.glob(str(
        pathlib.Path.home() / ".cache/huggingface/hub/"
        "models--nomic-ai--nomic-embed-text-v1.5-GGUF"
        "/snapshots/*/nomic-embed-text-v1.5.Q8_0.gguf"
    ))[0]

    duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    duck.execute(f"LOAD '{blobembed_duckdb.extension_path()}'")
    duck.execute(f"LOAD '{blobfilters_duckdb.extension_path()}'")
    duck.execute("INSTALL postgres; LOAD postgres;")
    duck.execute(f"ATTACH '{pg_dsn}' AS pg (TYPE POSTGRES)")
    duck.execute(f"SELECT be_load_model('nomic', '{MODEL}')")

    os.dup2(stderr_fd, 2)
    os.close(devnull)
    os.close(stderr_fd)
    log.info("  Model loaded")

    # ── Stage source data ─────────────────────────────────────────
    log.info("Staging places from PG into DuckDB...")
    t0 = time.perf_counter()
    duck.execute("""
        CREATE TEMP TABLE source_data AS
        SELECT geonameid, place_name, place_ascii, country_name, country_code,
               continent, admin1_name, admin2_name, feature_code,
               population, latitude, longitude, full_path
        FROM pg.gazetteer.geonames_place
    """)
    n = duck.execute("SELECT count(*) FROM source_data").fetchone()[0]
    log.info(f"  {n} places staged in {time.perf_counter()-t0:.1f}s")

    # ── Populate via generic pipeline ─────────────────────────────
    pg_conn = psycopg2.connect(dbname="rule4_test", host="/tmp")
    n_inserted = populate_collection(engine, duck, "geonames", pg_conn=pg_conn)

    # ── Indexes ───────────────────────────────────────────────────
    if not args["--skip-indexes"]:
        log.info("Creating indexes...")
        ensure_indexes(engine, "geonames")
        log.info("  Done")

    # ── Blobfilter ────────────────────────────────────────────────
    if not args["--skip-blobfilter"]:
        log.info("Building place name blobfilter...")
        existing = duck.execute(
            "SELECT domain_name FROM pg.domain.enumeration "
            "WHERE domain_name = 'geonames_places'"
        ).fetchall()
        if not existing:
            duck.execute("""INSERT INTO pg.domain.enumeration
                (domain_name, domain_label, source, member_count)
                VALUES ('geonames_places', 'GeoNames places', 'geonames', 0)""")

        duck.execute(
            "DELETE FROM pg.domain.member WHERE domain_name = 'geonames_places'"
        )
        duck.execute("""
            INSERT INTO pg.domain.member (domain_name, label)
            SELECT 'geonames_places', place_name
            FROM pg.domain.geonames_embedding
            UNION
            SELECT 'geonames_places', place_ascii
            FROM pg.domain.geonames_embedding
            WHERE place_ascii IS NOT NULL AND place_ascii != place_name
        """)

        result = duck.execute("""
            WITH M AS (
                SELECT json_group_array(label) AS mj
                FROM pg.domain.member WHERE domain_name = 'geonames_places'
            )
            SELECT bf_to_base64(bf_build_json_normalized(mj)) AS fb64,
                   bf_cardinality(bf_build_json_normalized(mj)) AS card
            FROM M
        """).fetchone()
        duck.execute("""UPDATE pg.domain.enumeration
            SET filter_b64 = ?, member_count = ?, updated_at = NOW()
            WHERE domain_name = 'geonames_places'""",
            [result[0], result[1]])
        log.info(f"  Blobfilter: {result[1]} place names")

    pg_conn.close()
    duck.close()
    log.info("Done!")


if __name__ == "__main__":
    main()
