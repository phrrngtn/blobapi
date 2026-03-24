"""Populate GeoNames embedding collection.

Architecture:
- DuckDB: stage data from PG (fast read via scanner), compute embeddings
- Python: orchestrate batches, fetch results from DuckDB
- PG: persist via psycopg2 COPY (fast write, not via scanner)
"""
import os
import sys
import time
import glob
import logging
import io

sys.path.insert(0, "/Users/paulharrington/checkouts/blobapi")

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
    stream=sys.stderr,
)
log = logging.getLogger(__name__)

import duckdb
import psycopg2
from sqlalchemy import create_engine
from blobapi.embedding_catalog import register_collection, ensure_indexes

BATCH_SIZE = 1000

# ── Register collection ──────────────────────────────────────────

log.info("Registering GeoNames collection...")
engine = create_engine("postgresql+psycopg2:///rule4_test", connect_args={"host": "/tmp"})

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

# ── Setup DuckDB (computation) ───────────────────────────────────

log.info("Loading extensions...")
stderr_fd = os.dup(2)
devnull = os.open(os.devnull, os.O_WRONLY)
os.dup2(devnull, 2)

import blobembed_duckdb, blobfilters_duckdb

MODEL = glob.glob(
    "/Users/paulharrington/.cache/huggingface/hub/models--nomic-ai--nomic-embed-text-v1.5-GGUF"
    "/snapshots/*/nomic-embed-text-v1.5.Q8_0.gguf"
)[0]

duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
duck.execute(f"LOAD '{blobembed_duckdb.extension_path()}'")
duck.execute(f"LOAD '{blobfilters_duckdb.extension_path()}'")
duck.execute("INSTALL postgres; LOAD postgres;")
duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES)")
duck.execute(f"SELECT be_load_model('nomic', '{MODEL}')")

os.dup2(stderr_fd, 2)
os.close(devnull)
os.close(stderr_fd)
log.info("  Model loaded")

# ── Stage source data locally (fast read from PG) ────────────────

log.info("Staging places from PG into DuckDB...")
t0 = time.perf_counter()
duck.execute("""
    CREATE TEMP TABLE places AS
    SELECT geonameid, place_name, place_ascii, country_name, country_code,
           continent, admin1_name, admin2_name, feature_code,
           population, latitude, longitude, full_path,
           ROW_NUMBER() OVER (ORDER BY geonameid) AS rn
    FROM pg.gazetteer.geonames_place
""")
n_total = duck.execute("SELECT count(*) FROM places").fetchone()[0]
log.info(f"  {n_total} places staged in {time.perf_counter()-t0:.1f}s")

# ── Setup PG connection for direct writes ────────────────────────

pg_conn = psycopg2.connect(dbname="rule4_test", host="/tmp")
pg_cur = pg_conn.cursor()
pg_cur.execute("DELETE FROM domain.geonames_embedding")
pg_conn.commit()
log.info("  Cleared target table")

# ── Embed in DuckDB, write to PG via psycopg2 ───────────────────

n_batches = (n_total + BATCH_SIZE - 1) // BATCH_SIZE
total_inserted = 0
total_errors = 0
t_start = time.perf_counter()

for batch_idx in range(n_batches):
    rn_start = batch_idx * BATCH_SIZE + 1
    rn_end = rn_start + BATCH_SIZE - 1
    t_batch = time.perf_counter()

    try:
        # Compute embeddings in DuckDB
        rows = duck.execute(f"""
            SELECT geonameid, place_name, place_ascii, country_name, country_code,
                   continent, admin1_name, admin2_name, feature_code,
                   population, latitude, longitude, full_path,
                   be_embed('nomic', full_path) AS embedding
            FROM places
            WHERE rn BETWEEN {rn_start} AND {rn_end}
        """).fetchall()

        # Write to PG via psycopg2
        for row in rows:
            *cols, embedding = row
            # Convert embedding list to PG array literal
            emb_str = "{" + ",".join(str(v) for v in embedding) + "}"
            pg_cur.execute(
                """INSERT INTO domain.geonames_embedding
                   (geonameid, place_name, place_ascii, country_name, country_code,
                    continent, admin1_name, admin2_name, feature_code,
                    population, latitude, longitude, full_path, embedding)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (*cols, emb_str)
            )
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

# ── Summary ──────────────────────────────────────────────────────

total_elapsed = time.perf_counter() - t_start
pg_cur.execute("SELECT count(*) FROM domain.geonames_embedding")
n_in_pg = pg_cur.fetchone()[0]
log.info(f"Complete: {n_in_pg} places in PG, {total_errors} errors, {total_elapsed:.0f}s ({n_in_pg/total_elapsed:.0f}/sec)")

# ── Indexes ──────────────────────────────────────────────────────

log.info("Creating indexes...")
ensure_indexes(engine, "geonames")
log.info("  Done")

# ── Blobfilter ───────────────────────────────────────────────────

log.info("Building place name blobfilter...")
existing = duck.execute(
    "SELECT domain_name FROM pg.domain.enumeration WHERE domain_name = 'geonames_places'"
).fetchall()
if not existing:
    duck.execute("""INSERT INTO pg.domain.enumeration
        (domain_name, domain_label, source, member_count)
        VALUES ('geonames_places', 'GeoNames places', 'geonames', 0)""")

duck.execute("DELETE FROM pg.domain.member WHERE domain_name = 'geonames_places'")
duck.execute("""
    INSERT INTO pg.domain.member (domain_name, label)
    SELECT 'geonames_places', place_name FROM pg.domain.geonames_embedding
    UNION
    SELECT 'geonames_places', place_ascii FROM pg.domain.geonames_embedding
    WHERE place_ascii IS NOT NULL AND place_ascii != place_name
""")

result = duck.execute("""
    WITH M AS (
        SELECT json_group_array(label) AS mj
        FROM pg.domain.member WHERE domain_name = 'geonames_places'
    )
    SELECT bf_to_base64(bf_build_json_normalized(mj)) AS fb64,
           bf_cardinality(bf_build_json_normalized(mj)) AS card FROM M
""").fetchone()
duck.execute("""UPDATE pg.domain.enumeration
    SET filter_b64 = ?, member_count = ?, updated_at = NOW()
    WHERE domain_name = 'geonames_places'""", [result[0], result[1]])
log.info(f"  Blobfilter: {result[1]} place names")

pg_cur.close()
pg_conn.close()
duck.close()
log.info("Done!")
