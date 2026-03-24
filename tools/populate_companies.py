"""Populate company embedding collection from SEC EDGAR bulk ZIP.

Usage:
    populate_companies.py [options]
    populate_companies.py -h | --help

Options:
    --zip PATH            Path to submissions.zip [default: /tmp/submissions.zip]
    --pg-dsn DSN          PG connection string [default: host=/tmp dbname=rule4_test]
    --batch-size N        Rows per batch [default: 1000]
    --skip-indexes        Don't create indexes after populating
    --skip-blobfilter     Don't rebuild the blobfilter after populating
    -h --help             Show this help

Prerequisites:
    Download the bulk ZIP first:
        curl -o /tmp/submissions.zip \\
            "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip" \\
            -H "User-Agent: your-app/0.1 you@example.com"

Architecture:
    Python: parse ZIP, stage in DuckDB
    DuckDB: render template + embed (computation)
    PG: persist via psycopg2 (fast write)
"""
import json
import zipfile
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
    zip_path = args["--zip"]
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

    # ── Register collection ───────────────────────────────────────
    log.info("Registering companies collection...")
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
        embedding_template=(
            "{{ sic }} {{ sic_description }} > {{ state }} > "
            "{{ company_name }} ({{ ticker }}) [CIK {{ cik }}]"
        ),
        description="SEC-registered public companies with SIC classification",
        create_table=True,
    )

    # ── Parse ZIP ─────────────────────────────────────────────────
    log.info(f"Parsing {zip_path}...")
    t0 = time.perf_counter()
    companies = []
    skipped = 0
    with zipfile.ZipFile(zip_path, "r") as zf:
        entries = [n for n in zf.namelist()
                   if n.startswith("CIK") and n.endswith(".json")]
        log.info(f"  {len(entries)} JSON entries in ZIP")
        for i, name in enumerate(entries):
            if i > 0 and i % 100000 == 0:
                log.info(f"  parsed {i}/{len(entries)}, {len(companies)} kept")
            try:
                data = json.loads(zf.read(name))
                sic = data.get("sic", "")
                if not sic or sic == "0000":
                    skipped += 1
                    continue
                cik = str(data.get("cik", "")).zfill(10)
                tickers = data.get("tickers", [])
                companies.append((
                    cik,
                    data.get("name", "")[:500],
                    (tickers[0] if tickers else "")[:10],
                    sic[:10],
                    (data.get("sicDescription", "") or "")[:200],
                    (data.get("stateOfIncorporation", "") or "")[:10],
                    (data.get("category", "") or "")[:100],
                ))
            except Exception:
                skipped += 1

    log.info(f"  {len(companies)} companies, {skipped} skipped, "
             f"{time.perf_counter()-t0:.1f}s")

    # ── Setup DuckDB ──────────────────────────────────────────────
    log.info("Loading extensions...")
    stderr_fd = os.dup(2)
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, 2)

    import blobembed_duckdb, blobfilters_duckdb, blobtemplates_duckdb

    MODEL = glob.glob(str(
        pathlib.Path.home() / ".cache/huggingface/hub/"
        "models--nomic-ai--nomic-embed-text-v1.5-GGUF"
        "/snapshots/*/nomic-embed-text-v1.5.Q8_0.gguf"
    ))[0]

    duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    duck.execute(f"LOAD '{blobembed_duckdb.extension_path()}'")
    duck.execute(f"LOAD '{blobfilters_duckdb.extension_path()}'")
    duck.execute(f"LOAD '{blobtemplates_duckdb.extension_path()}'")
    duck.execute("INSTALL postgres; LOAD postgres;")
    duck.execute(f"ATTACH '{pg_dsn}' AS pg (TYPE POSTGRES)")
    duck.execute(f"SELECT be_load_model('nomic', '{MODEL}')")

    os.dup2(stderr_fd, 2)
    os.close(devnull)
    os.close(stderr_fd)
    log.info("  Model loaded")

    # ── Stage into DuckDB ─────────────────────────────────────────
    log.info(f"Staging {len(companies)} companies...")
    duck.execute("""CREATE TEMP TABLE source_data (
        cik VARCHAR, company_name VARCHAR, ticker VARCHAR,
        sic VARCHAR, sic_description VARCHAR, state VARCHAR,
        filer_category VARCHAR)""")
    duck.executemany("INSERT INTO source_data VALUES (?,?,?,?,?,?,?)", companies)

    # ── Populate via generic pipeline ─────────────────────────────
    pg_conn = psycopg2.connect(dbname="rule4_test", host="/tmp")
    n_inserted = populate_collection(engine, duck, "companies", pg_conn=pg_conn)

    # ── Indexes ───────────────────────────────────────────────────
    if not args["--skip-indexes"]:
        log.info("Creating indexes...")
        ensure_indexes(engine, "companies")
        log.info("  Done")

    # ── Blobfilter ────────────────────────────────────────────────
    if not args["--skip-blobfilter"]:
        log.info("Rebuilding company name blobfilter...")
        result = duck.execute("""
            WITH M AS (
                SELECT json_group_array(label) AS mj FROM (
                    SELECT company_name AS label
                    FROM pg.domain.company_embedding
                    UNION
                    SELECT ticker FROM pg.domain.company_embedding
                    WHERE ticker != ''
                )
            )
            SELECT bf_to_base64(bf_build_json_normalized(mj)) AS fb64,
                   bf_cardinality(bf_build_json_normalized(mj)) AS card
            FROM M
        """).fetchone()
        duck.execute("""UPDATE pg.domain.enumeration
            SET filter_b64 = ?, member_count = ?, updated_at = NOW()
            WHERE domain_name = 'public_company_names'""",
            [result[0], result[1]])
        log.info(f"  Filter: {result[1]} members")

    pg_conn.close()
    duck.close()
    log.info("Done!")


if __name__ == "__main__":
    main()
