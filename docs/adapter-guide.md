# Adapter Guide: Reifying API Calls as SQL

## What is an Adapter?

An adapter declares an API endpoint as data — URL template, headers, rate limit,
response reshaping — so it can be called from SQL without imperative code.

Three adapter types, from simplest to most complex:

| Type | Table | When to use | Example |
|------|-------|-------------|---------|
| **HttpAdapter** | `domain.http_adapter` | Simple REST data-fetch (JSON API → rows) | EDGAR company tickers, Wikidata SPARQL |
| **LlmAdapter** | `llm_adapter` (session) | LLM-backed function with prompt template | Domain inference, property lookup |
| **ApiAdapter** | `api_adapter` | Full OpenAPI-derived adapter with call/response JMESPath | Weather API, pricing scrape |

## How to Add an HTTP Adapter

### 1. Define the adapter in Python

```python
from blobapi.experiments.http_adapter import HttpAdapter

adapter = HttpAdapter(
    name="my_api_endpoint",
    description="What this endpoint returns",
    method="get",
    url_template="https://api.example.com/v1/data/{{ item_id }}.json",
    default_headers={"User-Agent": "myapp/0.1", "Accept": "application/json"},
    rate_limit_profile="10/s",
    response_jmespath="results[].{id: id, name: name, value: value}",
    source="example:api",
)
```

### 2. Register it in PostgreSQL

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

engine = create_engine("postgresql+psycopg2:///rule4_test",
                       connect_args={"host": "/tmp"})
HttpAdapter.__table__.create(engine, checkfirst=True)

with Session(engine) as session:
    session.merge(adapter)
    session.commit()
```

### 3. Use it from DuckDB

```sql
-- Load the adapter definition
ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES);

-- Read the adapter config
SELECT url_template, default_headers, rate_limit_profile,
       response_jmespath
FROM pg.domain.http_adapter
WHERE name = 'my_api_endpoint';

-- Set rate limits from the adapter config
SET VARIABLE bh_http_config = MAP {
    'https://api.example.com/': '{"rate_limit": "10/s"}'
};

-- Call the endpoint
SELECT (bh_http_get(
    bt_template_render(url_template, json_object('item_id', '42')),
    headers := from_json(default_headers, 'MAP(VARCHAR, VARCHAR)')
)).response_body
FROM pg.domain.http_adapter
WHERE name = 'my_api_endpoint';
```

## Key Fields

| Field | Purpose | Example |
|-------|---------|---------|
| `name` | Unique identifier, used in SQL calls | `edgar_submission` |
| `url_template` | Inja/Jinja2 template with `{{ param }}` placeholders | `https://data.sec.gov/submissions/CIK{{ cik }}.json` |
| `default_headers` | JSON object, merged with per-call headers | `{"User-Agent": "...", "Accept": "application/json"}` |
| `default_params` | JSON object of query parameters | `{"format": "json"}` |
| `rate_limit_profile` | GCRA rate string for bh_http_config | `"10/s"`, `"100/m"`, `"5/s"` |
| `response_jmespath` | JMESPath expression to reshape response body into rows | `results[].{id: id, name: name}` |
| `source` | Provenance tag | `sec:edgar`, `wikidata:sparql` |

## Rate Limiting

The `rate_limit_profile` field stores the rate limit as a string (`"10/s"`,
`"100/m"`). This gets injected into the `bh_http_config` variable:

```sql
-- Build config from adapter registry
SET VARIABLE bh_http_config = (
    SELECT MAP_FROM_ENTRIES(
        LIST({key: url_template, value: json_object('rate_limit', rate_limit_profile)})
    )
    FROM pg.domain.http_adapter
    WHERE rate_limit_profile IS NOT NULL
);
```

The blobhttp extension uses GCRA (Generic Cell Rate Algorithm) to enforce
these limits per-host, with optional global limits across all hosts.

## Comparison: Python vs SQL

See `experiments/edgar_domains.py` (imperative) alongside
`experiments/edgar_domains.sql` (declarative) in the blobfilters repo
for a side-by-side comparison of the same EDGAR workflow in both styles.

## DuckDB Macros

After registering adapters in PG, source `sql/http_adapt.sql` to get
convenience macros:

```sql
ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES);
.read sql/http_adapt.sql

-- All public companies
SELECT * FROM edgar_tickers() LIMIT 10;

-- Company details by CIK
SELECT * FROM edgar_company('0000913144');  -- RenaissanceRe

-- Wikidata SPARQL
SELECT * FROM wikidata_query('
    SELECT ?item ?label WHERE {
      ?item wdt:P31 wd:Q3624078 .
      ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
    }
') LIMIT 10;
```

These macros read their URL templates, headers, and rate limits from
the `domain.http_adapter` table — the adapter registry is the single
source of truth. Adding a new adapter to PG makes it available to SQL
without writing new macro code.

### Inside a Macro

Here's the full source for `edgar_company` — it's just SQL:

```sql
CREATE OR REPLACE MACRO edgar_company(cik_padded) AS TABLE (
    SELECT doc->>'cik'                  AS cik,
           doc->>'name'                 AS name,
           doc->>'sic'                  AS sic,
           doc->>'sicDescription'       AS sic_description,
           doc->>'stateOfIncorporation' AS state,
           doc->>'fiscalYearEnd'        AS fiscal_year_end,
           doc->>'category'             AS filer_category,
           doc->'tickers'               AS tickers_json,
           doc->'exchanges'             AS exchanges_json
    FROM (
        SELECT (bh_http_get(
            REPLACE(a.url_template, '{{ cik_padded }}', cik_padded),
            headers := a.default_headers::VARCHAR
        )).response_body::JSON AS doc
        FROM pg.domain.http_adapter AS a
        WHERE a.name = 'edgar_submission'
    )
);
```

The macro looks up its own URL template and headers from the adapter
registry — the HTTP mechanics (rate limiting, retries, timeouts) are
handled by the `bh_http_get` infrastructure, not by the macro.

**Important:** Calling `edgar_company` in a loop (one CIK at a time)
is a performance anti-pattern. Each call makes a separate HTTP request
to the SEC API. For bulk lookups, use the `edgar_tickers()` macro to
get all 10,000+ companies in a single request, or use a data-driven
pattern where DuckDB drives the HTTP calls in parallel:

```sql
-- ANTI-PATTERN: one HTTP call per row, serial
SELECT * FROM edgar_company('0000913144');
SELECT * FROM edgar_company('0001095073');
SELECT * FROM edgar_company('0000082811');

-- BETTER: data-driven, parallel HTTP calls via LATERAL join.
-- bh_http_get processes up to 10 concurrent requests per chunk.
-- Note: SEC EDGAR has no batch/multi-CIK endpoint, so each CIK
-- is a separate HTTP request.  For a handful of CIKs this is fine;
-- for thousands, use the bulk ZIP instead.
SELECT c.*
FROM (VALUES ('0000913144'), ('0001095073'), ('0000082811')) AS v(cik),
     LATERAL edgar_company(v.cik) AS c;
```

### The Right Pattern: Poll → Store → Query Locally

Reference data like company tickers changes slowly.  The adapter
macros are for **refresh** (poll remote → store local), not for
ad-hoc queries.  Never pull 10,000 rows over HTTP just to filter.

```sql
-- ── REFRESH: poll bulk endpoint into a TTST ──────────────────
-- Run daily/weekly.  sys_from tracks when we observed this state.
INSERT INTO edgar_company_history (cik, ticker, name, sic, sys_from)
SELECT cik, ticker, name, NULL AS sic, NOW() AS sys_from
FROM edgar_tickers();

-- ── QUERY: find companies added since last week ──────────────
-- The TTST lets you compare snapshots at two points in time.
WITH CURRENT_SNAPSHOT AS (
    SELECT DISTINCT ON (cik) cik, ticker, name
    FROM edgar_company_history
    WHERE sys_from <= NOW()
    ORDER BY cik, sys_from DESC
),
PREVIOUS_SNAPSHOT AS (
    SELECT DISTINCT ON (cik) cik, ticker, name
    FROM edgar_company_history
    WHERE sys_from <= NOW() - INTERVAL '7 days'
    ORDER BY cik, sys_from DESC
)
SELECT c.cik, c.ticker, c.name
FROM CURRENT_SNAPSHOT AS c
LEFT JOIN PREVIOUS_SNAPSHOT AS p USING (cik)
WHERE p.cik IS NULL;  -- new companies not in last week's snapshot

-- ── QUERY: reinsurance companies (local, instant) ────────────
SELECT * FROM edgar_company_history
WHERE sic = '6331'
  AND sys_to IS NULL;  -- current rows only
```

For full company details (SIC codes, state, etc.), SEC provides a
1.5GB nightly bulk ZIP at
`https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip`
containing all submissions data.  Poll that into the TTST rather than
making 10,000 individual API calls.

### Available Macros

| Macro | Returns | Example |
|-------|---------|---------|
| `edgar_tickers()` | ~10K rows: cik, ticker, name | `SELECT * FROM edgar_tickers()` |
| `edgar_company(cik)` | 1 row: name, SIC, tickers, state | `SELECT * FROM edgar_company('0000320193')` |
| `wikidata_query(sparql)` | N rows: label, alt_label, item_uri | `SELECT * FROM wikidata_query('...')` |

## Links

- [[Resolution Sieve Architecture]] — adapters feed the domain sieve
- [[Data As Control Plane]] — adapters are data, not code
- `blobhttp/sql/http_verbs.sql` — bh_http_get/post macro definitions
- `blobhttp/sql/http_config.sql` — config MAP documentation
- `blobapi/sql/create_llm_adapter.sql` — LLM adapter DDL (similar pattern)
