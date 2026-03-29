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

## OAuth Authentication (Google APIs)

Google Drive, Sheets, and Docs APIs require OAuth2 bearer tokens. The
adapter definitions in `adapters/google.yaml` declare `{{ access_token }}`
and `{{ quota_project }}` template variables in their headers, but the
reify macros in `sql/google_adapt.sql` handle token injection automatically
via blobhttp's OpenBao integration.

### Architecture

```
┌─────────────────────┐
│  google.yaml        │  7 adapter definitions (URL templates, JMESPath, rate limits)
└─────────┬───────────┘
          │
┌─────────▼───────────┐
│  google_adapt.sql   │  DuckDB table macros: google_drive_list(), google_sheet_tabs(), etc.
│                     │  Only need document IDs — no token params
└─────────┬───────────┘
          │ bh_http_get() with headers := MAP{'X-Goog-User-Project': ...}
          │
┌─────────▼───────────┐
│  blobhttp           │  Sees vault_path in bh_http_config for googleapis.com
│                     │  Fetches access_token from OpenBao, injects Authorization header
│                     │  5-minute in-process cache
└─────────┬───────────┘
          │
┌─────────▼───────────┐
│  OpenBao            │  secret/blobapi/google_token  → {access_token, expires_at}
│  (KV v2)            │  secret/blobapi/google        → {client_id, client_secret, refresh_token}
└─────────────────────┘
```

### Setup (one-time)

**1. Create a GCP project and OAuth client**

- Create a project (e.g., `meplex-integration`) at console.cloud.google.com
- Enable the Drive API and Sheets API
- Create an OAuth consent screen (External, Testing mode)
- Add your email as a test user
- Create an OAuth client ID (Desktop app type)
- Download the client secret JSON

**2. Authorize and store credentials in OpenBao**

Run the one-time authorization flow (opens a browser for consent):

```bash
uv run python tools/google_oauth_setup.py   # or equivalent
```

This stores `client_id`, `client_secret`, `refresh_token`, and `token_uri`
in OpenBao at `secret/blobapi/google`.

**3. Refresh the access token**

Google access tokens expire after 1 hour. Before using the macros,
refresh the token:

```bash
uv run python tools/google_token_refresh.py
```

This reads the OAuth credentials from `secret/blobapi/google`, exchanges
the refresh token for a fresh access token, and writes it to
`secret/blobapi/google_token`. blobhttp caches vault secrets for 5
minutes, so the token must be refreshed before it expires. Run this on
a timer (cron, launchd) every 45 minutes for unattended use.

### SQL Usage

```sql
-- Load extensions and macros
LOAD 'bhttp';
LOAD 'blobtemplates';
.read sql/create_http_adapter.sql
.read sql/load_http_adapters.sql
.read sql/google_adapt.sql

-- Configure vault-backed auth (once per session)
SET VARIABLE bh_http_config = google_init();

-- List files in a Drive folder
SELECT * FROM google_drive_list('1C01bJDPMZfChCJhgUd11W9eF3HVqXjYT');

-- Get metadata for a file
SELECT * FROM google_drive_metadata('1C01bJDPMZfChCJhgUd11W9eF3HVqXjYT');

-- List tabs in a spreadsheet
SELECT * FROM google_sheet_tabs('13uaR6lb314XJ7ytIcFAqcjXfeVezstQIpDHBlN9fMuA');

-- Read cell values
SELECT * FROM google_sheet_values('13uaR6lb...', 'Sheet1!A1:Z50');

-- Compose: folder → spreadsheet tabs in one query
SELECT f.name AS file_name, t.*
FROM google_drive_list('1C01bJDPMZfChCJhgUd11W9eF3HVqXjYT') AS f,
     LATERAL google_sheet_tabs(f.id) AS t
WHERE f.mime_type = 'application/vnd.google-apps.spreadsheet';
```

### Explicit token (without vault)

For ad-hoc use without OpenBao, set the bearer token directly:

```sql
SET VARIABLE bh_http_config = bh_http_config_set_bearer(
    'https://www.googleapis.com/', 'ya29.a0...');
SET VARIABLE bh_http_config = bh_http_config_set_bearer(
    'https://sheets.googleapis.com/', 'ya29.a0...');
SET VARIABLE google_project = 'meplex-integration';

-- Same macros work — blobhttp uses the bearer token from config
SELECT * FROM google_drive_list('1C01bJD...');
```

### Why not a service account?

OpenBao's GCP secrets engine can auto-mint tokens for **service accounts**
with zero external refresh logic — vault holds the SA key and generates
tokens on demand. This is the fully automatic path.

However, service accounts can only access files explicitly shared with the
SA email (e.g., `blobhttp@meplex-integration.iam.gserviceaccount.com`).
They cannot see files shared with your personal Google account unless you
re-share each one. For personal Drive folders this is friction; for
production pipelines with dedicated shared drives it's the right answer.

| Approach | Token refresh | File access | Best for |
|----------|--------------|-------------|----------|
| User OAuth + `google_token_refresh.py` | External (cron/manual) | Your personal Drive | Development, ad-hoc |
| Service account + GCP secrets engine | Fully automatic (vault) | Only explicitly shared files | Production pipelines |

### Why not extension-level auto-refresh?

An earlier design proposed `token_endpoint` / `token_auth_type` config
fields in blobhttp itself. This was rejected because:

- OpenBao already handles credential minting, rotation, and TTL
- Automatic refresh is hidden state that violates the extension's
  side-effect-free principle
- Supporting OAuth2 + SPNEGO + arbitrary token formats is scope creep

See `blobhttp/docs/TODO-enterprise.md` for the full rationale.

## Links

- [[Resolution Sieve Architecture]] — adapters feed the domain sieve
- [[Data As Control Plane]] — adapters are data, not code
- `blobhttp/sql/http_verbs.sql` — bh_http_get/post macro definitions
- `blobhttp/sql/http_config.sql` — config MAP documentation
- `blobapi/sql/create_llm_adapter.sql` — LLM adapter DDL (similar pattern)
- `blobapi/sql/google_adapt.sql` — Google reify macros
- `blobapi/adapters/google.yaml` — Google adapter definitions
