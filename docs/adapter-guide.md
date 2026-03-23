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

-- BETTER: data-driven, parallel HTTP calls via LATERAL join
-- bh_http_get processes up to 10 concurrent requests per chunk
SELECT t.ticker, c.*
FROM (VALUES ('0000913144'), ('0001095073'), ('0000082811')) AS v(cik),
     LATERAL edgar_company(v.cik) AS c;

-- BEST: poll the bulk endpoint once into a local TTST, query locally
-- The HTTP call is a REFRESH operation, not a query:
--   INSERT INTO edgar_company_snapshot (cik, ticker, name, refreshed_at)
--   SELECT cik, ticker, name, NOW() FROM edgar_tickers();
-- Then query the local table:
--   SELECT * FROM edgar_company_snapshot WHERE name LIKE '%RE %';
```

Reference data like company tickers changes slowly — poll it daily or
weekly into a local transaction-time system table (TTST), then query
the local copy. Never pull 10,000 rows over HTTP just to filter by
LIKE. The adapter macros are for **refresh**, not for ad-hoc queries
against remote data.

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
