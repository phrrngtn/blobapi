"""Populate the companies embedding collection from EDGAR.

Run: cd ~/checkouts/blobapi && uv run python experiments/populate_companies.py

Steps:
1. Fetch all company tickers from SEC EDGAR (bulk JSON, ~10K companies)
2. For each, look up SIC code and details via submissions API (rate-limited)
3. Generate embedding text from the catalog's embedding_template
4. Embed with nomic-embed-text-v1.5 via blobembed
5. Insert into domain.company_embedding (auto-generated table)
6. Build blobfilter for the collection

Estimated time: ~30 minutes (10K companies × 0.1s SEC rate limit + embedding)
"""
import json
import time
import urllib.request
import glob

import duckdb
from sqlalchemy import create_engine, text as sa_text
from sqlalchemy.orm import Session

from blobapi.embedding_catalog import EmbeddingCatalog, register_collection

UA = "blobboxes-domain-builder/0.1 phrrngtn@panix.com"
BLOBEMBED_EXT = None
BLOBFILTERS_EXT = None

# Try package imports first, fall back to build paths
try:
    import blobembed_duckdb
    BLOBEMBED_EXT = blobembed_duckdb.extension_path()
except ImportError:
    import pathlib
    p = pathlib.Path.home() / "checkouts/blobembed/build/duckdb/blobembed.duckdb_extension"
    if p.exists():
        BLOBEMBED_EXT = str(p)

try:
    import blobfilters_duckdb
    BLOBFILTERS_EXT = blobfilters_duckdb.extension_path()
except ImportError:
    import pathlib
    p = pathlib.Path.home() / "checkouts/blobfilters/build/duckdb/blobfilters.duckdb_extension"
    if p.exists():
        BLOBFILTERS_EXT = str(p)

MODEL_PATHS = glob.glob(
    str(pathlib.Path.home() / ".cache/huggingface/hub/models--nomic-ai--nomic-embed-text-v1.5-GGUF/snapshots/*/nomic-embed-text-v1.5.Q8_0.gguf")
)
MODEL = MODEL_PATHS[0] if MODEL_PATHS else None


def fetch_sec(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def main():
    engine = create_engine("postgresql+psycopg2:///rule4_test", connect_args={"host": "/tmp"})

    # Ensure collection is registered with template
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
        create_table=True,
    )

    # Step 1: Fetch all tickers
    print("Fetching company tickers from SEC EDGAR...")
    tickers_data = fetch_sec("https://www.sec.gov/files/company_tickers.json")
    companies = list(tickers_data.values())
    print(f"  {len(companies)} companies")

    # Step 2: Fetch details for each company (SIC, state, category)
    # This is rate-limited to 10/s by SEC policy
    print("\nFetching company details (this takes ~20 minutes)...")
    details = []
    errors = 0
    for i, co in enumerate(companies):
        cik = str(co["cik_str"]).zfill(10)
        if i > 0 and i % 100 == 0:
            print(f"  {i}/{len(companies)} ({errors} errors)")

        try:
            data = fetch_sec(f"https://data.sec.gov/submissions/CIK{cik}.json")
            details.append({
                "cik": cik,
                "company_name": data.get("name", co["title"]),
                "ticker": co["ticker"],
                "sic": data.get("sic", ""),
                "sic_description": data.get("sicDescription", ""),
                "state": data.get("stateOfIncorporation", ""),
                "filer_category": data.get("category", ""),
            })
        except Exception as e:
            errors += 1
            # Use what we have from the tickers file
            details.append({
                "cik": cik,
                "company_name": co["title"],
                "ticker": co["ticker"],
                "sic": "",
                "sic_description": "",
                "state": "",
                "filer_category": "",
            })

        time.sleep(0.1)  # 10 req/sec

    print(f"  Done: {len(details)} companies, {errors} errors")

    # Step 3: Generate embedding text from template and embed
    if not BLOBEMBED_EXT or not MODEL:
        print("\nWARNING: blobembed not available, inserting without embeddings")
        # Insert into PG without embeddings
        with engine.connect() as conn:
            conn.execute(sa_text("DELETE FROM domain.company_embedding"))
            for d in details:
                conn.execute(sa_text("""
                    INSERT INTO domain.company_embedding
                    (cik, company_name, ticker, sic, sic_description, state, filer_category)
                    VALUES (:cik, :company_name, :ticker, :sic, :sic_description, :state, :filer_category)
                    ON CONFLICT (cik) DO UPDATE SET
                        company_name = EXCLUDED.company_name,
                        ticker = EXCLUDED.ticker,
                        sic = EXCLUDED.sic
                """), d)
            conn.commit()
        print(f"  Inserted {len(details)} companies (no embeddings)")
        return

    print(f"\nGenerating embeddings with nomic ({len(details)} companies)...")
    duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    duck.execute(f"LOAD '{BLOBEMBED_EXT}'")
    if BLOBFILTERS_EXT:
        duck.execute(f"LOAD '{BLOBFILTERS_EXT}'")
    duck.execute("INSTALL postgres; LOAD postgres;")
    duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES)")
    duck.execute(f"SELECT be_load_model('nomic', '{MODEL}')")

    # Load details into DuckDB temp table
    duck.execute("CREATE TEMPORARY TABLE companies (cik VARCHAR, company_name VARCHAR, ticker VARCHAR, sic VARCHAR, sic_description VARCHAR, state VARCHAR, filer_category VARCHAR)")
    duck.executemany("INSERT INTO companies VALUES (?, ?, ?, ?, ?, ?, ?)",
                     [(d["cik"], d["company_name"], d["ticker"], d["sic"],
                       d["sic_description"], d["state"], d["filer_category"])
                      for d in details])

    # Generate embeddings using the template pattern
    t0 = time.perf_counter()
    duck.execute("""
        DELETE FROM pg.domain.company_embedding;
        INSERT INTO pg.domain.company_embedding
        SELECT cik, company_name, ticker, sic, sic_description, state, filer_category,
               be_embed('nomic',
                   COALESCE(sic_description, '') || ' > ' ||
                   COALESCE(state, '') || ' > ' ||
                   company_name || ' (' || COALESCE(ticker, '') || ')'
               ) AS embedding
        FROM companies
    """)
    t1 = time.perf_counter()
    n = duck.execute("SELECT count(*) FROM pg.domain.company_embedding").fetchone()[0]
    print(f"  Embedded and inserted {n} companies in {t1-t0:.0f}s ({n/(t1-t0):.0f}/sec)")

    # Build blobfilter for company names
    if BLOBFILTERS_EXT:
        print("\nBuilding company name blobfilter...")
        result = duck.execute("""
            WITH MEMBERS AS (
                SELECT json_group_array(label) AS mj
                FROM (
                    SELECT company_name AS label FROM pg.domain.company_embedding
                    UNION
                    SELECT ticker FROM pg.domain.company_embedding WHERE ticker IS NOT NULL
                )
            )
            SELECT bf_to_base64(bf_build_json_normalized(mj)) AS fb64,
                   bf_cardinality(bf_build_json_normalized(mj)) AS card
            FROM MEMBERS
        """).fetchone()
        duck.execute("""
            UPDATE pg.domain.enumeration
            SET filter_b64 = ?, member_count = ?, updated_at = NOW()
            WHERE domain_name = 'public_company_names'
        """, [result[0], result[1]])
        print(f"  Filter rebuilt: {result[1]} members")

    duck.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
