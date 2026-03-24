"""Populate company embeddings from SEC EDGAR bulk ZIP.

Reads the embedding_template from the catalog, renders it with
bt_template_render per row, embeds with nomic. Batched with
progress logging.
"""
import json
import zipfile
import time
import glob
import logging
import sys

sys.path.insert(0, "/Users/paulharrington/checkouts/blobapi")
import duckdb

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
    stream=sys.stderr,
)
log = logging.getLogger(__name__)

BATCH_SIZE = 1000

# ── Parse ZIP ────────────────────────────────────────────────────

log.info("Parsing bulk ZIP (filtering to companies with SIC codes)...")
t0 = time.perf_counter()
companies = []
skipped = 0
with zipfile.ZipFile("/tmp/submissions.zip", "r") as zf:
    entries = [n for n in zf.namelist() if n.startswith("CIK") and n.endswith(".json")]
    log.info(f"  {len(entries)} JSON entries in ZIP")
    for i, name in enumerate(entries):
        if i > 0 and i % 100000 == 0:
            log.info(f"  parsed {i}/{len(entries)} entries, {len(companies)} kept")
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

elapsed = time.perf_counter() - t0
log.info(f"  {len(companies)} companies with SIC, {skipped} skipped, {elapsed:.1f}s")

# ── Setup DuckDB + extensions ────────────────────────────────────

log.info("Loading extensions...")
import blobembed_duckdb, blobfilters_duckdb, blobtemplates_duckdb

MODEL = glob.glob(
    "/Users/paulharrington/.cache/huggingface/hub/models--nomic-ai--nomic-embed-text-v1.5-GGUF"
    "/snapshots/*/nomic-embed-text-v1.5.Q8_0.gguf"
)[0]

duck = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
duck.execute(f"LOAD '{blobembed_duckdb.extension_path()}'")
duck.execute(f"LOAD '{blobfilters_duckdb.extension_path()}'")
duck.execute(f"LOAD '{blobtemplates_duckdb.extension_path()}'")
duck.execute("INSTALL postgres; LOAD postgres;")
duck.execute("ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES)")
duck.execute(f"SELECT be_load_model('nomic', '{MODEL}')")
log.info("  Model loaded, PG attached")

# ── Read embedding template from catalog ─────────────────────────

template_row = duck.execute("""
    SELECT embedding_template
    FROM pg.domain.embedding_catalog
    WHERE collection_name = 'companies'
""").fetchone()

if not template_row or not template_row[0]:
    log.error("No embedding_template found for 'companies' collection in catalog")
    sys.exit(1)

template = template_row[0]
log.info(f"  Template: {template}")

# ── Verify template renders correctly with a sample ──────────────

duck.execute("""CREATE TEMP TABLE sample_test (
    cik VARCHAR, company_name VARCHAR, ticker VARCHAR,
    sic VARCHAR, sic_description VARCHAR, state VARCHAR,
    filer_category VARCHAR)""")
duck.execute("INSERT INTO sample_test VALUES (?,?,?,?,?,?,?)", list(companies[0]))

# Store template in a DuckDB variable to avoid quote escaping issues
duck.execute("SET VARIABLE embed_template = ?", [template])

sample_text = duck.execute("""
    SELECT bt_template_render(getvariable('embed_template'),
        json_object(
            'cik', cik, 'company_name', company_name, 'ticker', ticker,
            'sic', sic, 'sic_description', sic_description,
            'state', state, 'filer_category', filer_category
        )
    ) FROM sample_test
""").fetchone()[0]
log.info(f"  Sample: {sample_text}")
duck.execute("DROP TABLE sample_test")

# ── Clear target table ───────────────────────────────────────────

duck.execute("DELETE FROM pg.domain.company_embedding")
log.info("  Cleared company_embedding table")

# ── Embed and insert in batches ──────────────────────────────────

n_batches = (len(companies) + BATCH_SIZE - 1) // BATCH_SIZE
total_inserted = 0
total_errors = 0
t_start = time.perf_counter()

for batch_idx in range(n_batches):
    batch = companies[batch_idx * BATCH_SIZE : (batch_idx + 1) * BATCH_SIZE]
    t_batch = time.perf_counter()

    try:
        duck.execute("DROP TABLE IF EXISTS batch")
        duck.execute("""CREATE TEMP TABLE batch (
            cik VARCHAR, company_name VARCHAR, ticker VARCHAR,
            sic VARCHAR, sic_description VARCHAR, state VARCHAR,
            filer_category VARCHAR)""")
        duck.executemany("INSERT INTO batch VALUES (?,?,?,?,?,?,?)", batch)

        # Render the catalog template per row, then embed
        duck.execute("""
            INSERT INTO pg.domain.company_embedding
                (cik, company_name, ticker, sic, sic_description, state,
                 filer_category, embedding)
            SELECT cik, company_name, ticker, sic, sic_description, state,
                   filer_category,
                   be_embed('nomic',
                       bt_template_render(getvariable('embed_template'),
                           json_object(
                               'cik', cik,
                               'company_name', company_name,
                               'ticker', ticker,
                               'sic', sic,
                               'sic_description', sic_description,
                               'state', state,
                               'filer_category', filer_category
                           )
                       )
                   ) AS embedding
            FROM batch
            WHERE length(company_name) > 0
        """)

        batch_elapsed = time.perf_counter() - t_batch
        total_inserted += len(batch)
        overall_elapsed = time.perf_counter() - t_start
        rate = total_inserted / overall_elapsed if overall_elapsed > 0 else 0
        eta = (len(companies) - total_inserted) / rate if rate > 0 else 0

        log.info(
            f"  batch {batch_idx+1}/{n_batches}: {len(batch)} in {batch_elapsed:.1f}s | "
            f"total {total_inserted}/{len(companies)} ({100*total_inserted/len(companies):.0f}%) | "
            f"{rate:.0f}/sec | ETA {eta:.0f}s"
        )

    except Exception as e:
        total_errors += 1
        log.error(f"  batch {batch_idx+1}/{n_batches} FAILED: {e}")
        for row in batch[:3]:
            log.error(f"    sample: cik={row[0]} name={row[1][:30]} ticker={row[2]} sic={row[3]}")

# ── Summary ──────────────────────────────────────────────────────

total_elapsed = time.perf_counter() - t_start
n_in_pg = duck.execute("SELECT count(*) FROM pg.domain.company_embedding").fetchone()[0]
log.info(f"Embedding complete: {n_in_pg} in PG, {total_errors} errors, {total_elapsed:.0f}s")

# Sample: show rendered embedding text for a few insurance companies
log.info("Insurance/reinsurance (SIC 63xx) — rendered embedding text:")
for r in duck.execute("""
    SELECT bt_template_render(getvariable('embed_template'),
        json_object('cik', cik, 'company_name', company_name, 'ticker', ticker,
                    'sic', sic, 'sic_description', sic_description,
                    'state', state, 'filer_category', filer_category)
    ) AS text
    FROM pg.domain.company_embedding
    WHERE sic LIKE '63%'
    ORDER BY company_name LIMIT 10
""").fetchall():
    log.info(f"  {r[0]}")

# ── Rebuild blobfilter ───────────────────────────────────────────

log.info("Rebuilding company name blobfilter...")
result = duck.execute("""
    WITH M AS (
        SELECT json_group_array(label) AS mj FROM (
            SELECT company_name AS label FROM pg.domain.company_embedding
            UNION
            SELECT ticker FROM pg.domain.company_embedding WHERE ticker != ''
        )
    )
    SELECT bf_to_base64(bf_build_json_normalized(mj)) AS fb64,
           bf_cardinality(bf_build_json_normalized(mj)) AS card
    FROM M
""").fetchone()
duck.execute("""
    UPDATE pg.domain.enumeration
    SET filter_b64 = ?, member_count = ?, updated_at = NOW()
    WHERE domain_name = 'public_company_names'
""", [result[0], result[1]])
log.info(f"  Filter rebuilt: {result[1]} members")

duck.close()
log.info("Done!")
