# Services and Dependencies

This document describes the external services that blobapi depends on for
LLM-backed reified functions, pricing scrapes, and cost accounting.

## Architecture

```
DuckDB (client)
  │
  │  llm_adapt('physical_properties', params)
  │    → renders inja prompt template (blobtemplates: bt_template_render)
  │    → validates response against JSON Schema (bhttp: jsoncons)
  │    → reshapes response via JMESPath (bhttp: _llm_adapt_raw)
  │
  │  POST /v1/chat/completions  (OpenAI-compatible)
  ▼
Bifrost  (localhost:8080, Docker)
  │
  │  Translates to provider-native protocol
  │  Holds API keys internally
  ▼
Anthropic API  (or any Bifrost-supported provider)
```

Bifrost is an open-source LLM gateway that exposes a single OpenAI-compatible
endpoint and routes to 20+ providers. DuckDB never contains vendor-specific
logic or API keys — Bifrost handles both.

## Services

### 1. Bifrost (LLM Gateway)

**What**: Translates OpenAI-compatible `/v1/chat/completions` requests to
Anthropic (and other providers). Holds API keys so they never appear in SQL.

**Port**: 8080

**Start**:

```bash
docker run -d --name bifrost -p 8080:8080 maximhq/bifrost
```

**Configure the Anthropic provider** (one-time, persisted in Bifrost's
internal SQLite DB at `/app/data/config.db`):

```bash
# Read the API key from OpenBao (see below)
ANTHROPIC_KEY=$(curl -s \
  -H "X-Vault-Token: dev-blobapi-token" \
  'http://127.0.0.1:8200/v1/secret/data/blobapi/anthropic' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['data']['api_key'])")

# Register with Bifrost
curl -X POST http://localhost:8080/api/v1/keys \
  -H "Content-Type: application/json" \
  -d "{\"provider\": \"anthropic\", \"api_key\": \"$ANTHROPIC_KEY\"}"
```

**Verify**:

```bash
# Should list anthropic/claude-haiku-4-5-20251001, etc.
curl -s http://localhost:8080/v1/models | python3 -m json.tool

# Quick smoke test
curl -s http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"anthropic/claude-haiku-4-5-20251001",
       "messages":[{"role":"user","content":"Say hello in 3 words"}],
       "max_tokens":20}'
```

### 2. OpenBao (Secrets)

**What**: Stores API keys for external services. blobhttp reads secrets at
request time when `vault_path` is set in `bh_http_config`. For the LLM demo
via Bifrost, OpenBao is **not strictly required** (Bifrost holds the key),
but it is needed for direct API calls (weather, geocoding, etc.).

**Port**: 8200

**Start** (dev mode):

```bash
brew services start openbao
# Or manually:
bao server -dev -dev-root-token-id=dev-blobapi-token \
    -dev-listen-address=127.0.0.1:8200
```

**Secrets stored** (under `secret/blobapi/`):

| Path                       | Keys      | Used by          |
|----------------------------|-----------|------------------|
| `secret/blobapi/anthropic` | `api_key` | Bifrost setup    |
| `secret/blobapi/visualcrossing` | `api_key` | Weather adapter |
| `secret/blobapi/openmeteo` | `api_key` | Weather adapter  |
| `secret/blobapi/geocodio`  | `api_key` | Geocoding        |
| `secret/blobapi/weatherbit`| `api_key` | Weather adapter  |

**Write a secret** (example):

```bash
curl -X POST \
  -H "X-Vault-Token: dev-blobapi-token" \
  -H "Content-Type: application/json" \
  'http://127.0.0.1:8200/v1/secret/data/blobapi/anthropic' \
  -d '{"data": {"api_key": "sk-ant-..."}}'
```

### 3. DuckDB

**Version**: 1.x with `-unsigned` flag (extensions are locally built, not
signed by the DuckDB extension registry).

**Extensions required**:

| Extension       | Source                  | Provides                                        |
|-----------------|-------------------------|-------------------------------------------------|
| `bhttp`         | `../blobhttp/build/release/extension/bhttp/bhttp.duckdb_extension` | HTTP client, `_llm_adapt_raw`, `_llm_complete_raw`, rate limiting, Vault integration |
| `blobtemplates` | `../blobtemplates/build/duckdb/blobtemplates.duckdb_extension`       | `bt_template_render` (inja), `bt_yaml_to_json`, `bt_jmespath` |

> **blobhttp dependency**: blobapi has a hard dependency on blobhttp for all
> HTTP and LLM functionality. For proxy (mitmproxy) and secret management
> (OpenBao) setup, see the
> [blobhttp dev-setup guide](https://github.com/phrrngtn/blobhttp/blob/main/docs/dev-setup.md).

### 4. Jina Reader (Web Scraping)

**What**: Converts any URL to clean markdown by rendering JavaScript,
stripping navigation/boilerplate, and returning just the content. Used by the
pricing scraper to extract tables from provider pricing pages.

**URL pattern**: `https://r.jina.ai/{target_url}`

**No API key required** for basic use (20 requests/minute free tier).

**Key headers**:

| Header | Purpose | Example |
|---|---|---|
| `X-Target-Selector` | CSS selector — extract only matching elements | `table` |
| `X-Remove-Selector` | CSS selector — strip before extraction | `nav, footer, .sidebar` |

**Why Jina**: Provider pricing pages are typically JS-rendered SPAs.
Plain HTTP fetch returns a JavaScript bundle, not rendered content.
Jina handles the rendering and returns compact markdown — the Anthropic
pricing page goes from 218k tokens (raw HTML) to ~1,500 tokens
(tables only via `X-Target-Selector: table`).

**Usage from SQL** (via blobhttp):

```sql
SELECT bh_http_get(
    'https://r.jina.ai/https://platform.claude.com/docs/en/about-claude/pricing',
    headers := MAP {
        'X-Target-Selector': 'table',
        'X-Remove-Selector': 'nav, footer, .sidebar'
    }
).response_body;
```

**Usage from Python** (via httpx):

```python
resp = httpx.get(
    "https://r.jina.ai/https://platform.claude.com/docs/en/about-claude/pricing",
    headers={"X-Target-Selector": "table"},
)
markdown = resp.text  # Clean markdown tables, ~4.7k chars
```

**Jina config per provider** is stored in `adapters/providers.yaml` under
the `jina` key:

```yaml
- provider: anthropic
  jina:
    target_selector: table
    remove_selector: nav, footer, .sidebar
```

## Running the Demo

From the `blobapi` directory:

```bash
duckdb -unsigned -init sql/llm_demo.sql
```

This will:
1. Load both extensions
2. Source bhttp SQL macros (`llm_complete`, `llm_adapt`, HTTP config helpers)
3. Create the `llm_adapter` table and load adapter definitions from YAML
4. Query Claude for boiling point, melting point, and density of water,
   ethanol, and mercury
5. Return a schema-validated JSON result with `_meta` (token counts, latency)
   and `data` (list of `{substance, metric, value, unit_of_measure}`)

### Expected output

```
substance  metric          value     unit_of_measure
water      boiling point   373.15    K
water      melting point   273.15    K
water      density at 25°C 997       kg/m³
ethanol    boiling point   351.65    K
ethanol    melting point   159.05    K
ethanol    density at 25°C 789       kg/m³
mercury    boiling point   629.88    K
mercury    melting point   234.43    K
mercury    density at 25°C 13546     kg/m³
```

### Overriding defaults

```sql
-- Use a different model
SET VARIABLE llm_model = 'anthropic/claude-sonnet-4-6';

-- Use a different endpoint
SET VARIABLE llm_endpoint = 'http://my-gateway:8080/v1/chat/completions';

-- Per-call override (model, max_tokens, etc.)
SELECT * FROM llm_adapt('physical_properties',
    json_object('substances', ['water'],
                'metrics', ['boiling point'],
                'model', 'anthropic/claude-sonnet-4-6'));
```

## File Layout (blobapi)

```
adapters/
  physical_properties.yaml   -- LLM adapter: material properties lookup
  domain_inference.yaml      -- LLM adapter: column domain classification
  weather.yaml               -- HTTP adapter: weather API (JMESPath-based)
sql/
  create_llm_adapter.sql     -- DDL for llm_adapter table
  load_llm_adapters.sql      -- INSERT from YAML via bt_yaml_to_json
  llm_demo.sql               -- End-to-end runnable demo
docs/
  llm-setup.md               -- This file
```

## How It Works

### Adapter table

Each row in `llm_adapter` is a reified function definition:

| Column              | Purpose                                              |
|---------------------|------------------------------------------------------|
| `name`              | Adapter name, used as key in `llm_adapt()` calls     |
| `prompt_template`   | Inja/Jinja2 template, rendered with caller params    |
| `output_schema`     | JSON Schema (Draft 2020-12) for response validation  |
| `response_jmespath` | JMESPath expression to reshape validated JSON        |
| `max_tokens`        | Max tokens for the LLM completion                    |

### Call flow

```
llm_adapt('physical_properties', json_object(...))
  │
  ├─ Look up adapter row from llm_adapter table
  ├─ Render prompt: bt_template_render(prompt_template, params)
  ├─ Merge session defaults (endpoint, model, bh_http_config)
  ├─ Merge caller overrides (any key in params JSON)
  │
  └─ _llm_adapt_raw(config_json)   [C++ scalar function]
       ├─ POST to gateway (Bifrost)
       ├─ Continuation loop if finish_reason == "length"
       ├─ JSON Schema validation (jsoncons)
       ├─ Retry with error feedback on validation failure
       └─ JMESPath reshape → return JSON list-of-dicts
```

### Why Bifrost (not direct Anthropic calls)?

- **No API keys in SQL**: Bifrost holds keys; DuckDB sends unauthenticated
  localhost requests.
- **Provider abstraction**: Switch models/providers by changing the model
  string (e.g., `anthropic/claude-haiku-4-5-20251001` → `google/gemini-2.5-flash`).
  No SQL changes needed.
- **Observability**: Bifrost logs all requests with latency, token counts,
  and provider response headers.
- **Key rotation**: Update the key in Bifrost once, not in every DuckDB
  session.

### Why OpenBao (not config files)?

- API keys are never written to disk in plaintext.
- blobhttp's Vault integration (`vault_path` in `bh_http_config`) fetches
  secrets at request time with 5-minute caching.
- Dev mode uses a fixed token (`dev-blobapi-token`); production would use
  AppRole or Kubernetes auth.
- For Bifrost-routed LLM calls, OpenBao is only needed during Bifrost's
  initial API key setup — not at query time.

## LLM Pricing Pipeline

### Overview

Three approaches to populating the pricing TTST, in order of fidelity:

1. **Bifrost bootstrap** (`bootstrap-pricing`) — fast, comprehensive, but
   static per Docker image. Pricing is compiled into `governance_model_pricing`
   in Bifrost's SQLite DB. Updated only when Bifrost is rebuilt.

2. **Jina Reader + LLM (Python)** (`scrape-pricing`) — fetches live pricing
   pages, extracts tables, passes to LLM for structured extraction. Updates
   the high-ceremony TTST directly via SQLAlchemy.

3. **Jina Reader + LLM (pure SQL)** (`scrape_pricing_init.sql`) — same
   pipeline but entirely in DuckDB. Useful for ad-hoc queries and for
   operational DuckDB instances that don't have Python.

### Jina Reader

[Jina Reader](https://jina.ai/reader/) converts any URL to clean markdown.
Prepend `https://r.jina.ai/` to any URL — no API key needed (20 RPM free).

Key headers for our use case:

| Header | Purpose | Example |
|---|---|---|
| `X-Target-Selector` | CSS selector — extract only matching elements | `table` |
| `X-Remove-Selector` | CSS selector — strip before extraction | `nav, footer, .sidebar` |

Using `X-Target-Selector: table` reduces the Anthropic pricing page from
37,005 chars (218k tokens) to 4,759 chars (~1,500 tokens). This makes LLM
extraction cheap enough to run from Haiku (~$0.001 per scrape).

Jina config is stored per-provider in `adapters/providers.yaml` under the
`jina` key.

### Temporal tracking

Prices are tracked via `sys_from` / `sys_to` (transaction-time):

- **Bootstrap**: `sys_from` = Docker image creation timestamp (upper bound
  on when Bifrost's compiled pricing was set).
- **Scrape**: `sys_from` = scrape timestamp (upper bound on when the
  provider actually changed the price).
- **Current row**: `sys_to IS NULL`.
- **Idempotent**: re-running skips unchanged rows.

The business key is `model` (provider-prefixed, e.g. `anthropic/claude-opus-4-6`).
Only one current row per model. A price change closes the old row and opens
a new one.

### Pricing sources per provider

| Provider | Bifrost | Pricing page | Models API | Notes |
|---|---|---|---|---|
| Anthropic | Yes (14 models) | platform.claude.com | /v1/models (no pricing) | No programmatic pricing API |
| OpenAI | Yes (159 models) | openai.com/api/pricing | /v1/models (no pricing) | |
| Google Gemini | Yes (51 models) | ai.google.dev/pricing | /v1/models | |
| Mistral | Yes (51 models) | mistral.ai | /v1/models | |
| Others | Yes (2,566 total) | Varies | Varies | 88 providers in Bifrost |

### Cost accounting

Every `llm_adapt()` call returns `_meta` with token counts:

```sql
-- Cost of a query
SELECT
    (prompt_tokens * p.input_per_mtok
     + completion_tokens * p.output_per_mtok) / 1e6 AS cost_usd
FROM _meta
JOIN llm_pricing AS p ON 'anthropic/' || _meta.model = p.model;
```

Anthropic has no programmatic usage/billing API. The `_meta` from our own
calls is the usage ledger. `metadata.user_id` (opaque string in the request
body) is the only attribution mechanism — no project ID header exists.
Workspaces with separate API keys are the intended project-level separation.

### Cache/batch multipliers

Standard Anthropic multipliers (applied only for `provider = 'anthropic'`;
other providers get NULL for these columns):

| Tier | Multiplier | Relative to |
|---|---|---|
| 5-minute cache write | 1.25x | base input |
| 1-hour cache write | 2.0x | base input |
| Cache read (hit) | 0.1x | base input |
| Batch input | 0.5x | base input |
| Batch output | 0.5x | base output |

### Model ID normalization

LLM extraction from pricing pages produces marketing-style names
("Claude Opus 4.6" → `anthropic/claude-opus-4.6`) while the API uses
dashes (`claude-opus-4-6`). The `model_id` module normalizes:

- Dots to dashes in version numbers (`4.6` → `4-6`)
- Matches against known canonical IDs from the TTST
- Prefix matching for dated variants (`claude-opus-4-6` →
  `claude-opus-4-6-20250514` if the undated form isn't canonical)

This runs automatically in the Python scraper. The canonical form is
whatever the provider's API returns (and what Bifrost registers).

### Operational DuckDB: MERGE from high-ceremony DB

An operational DuckDB instance that runs real queries can pull current
pricing from the high-ceremony TTST via ODBC:

```sql
-- Load the ODBC extension and create local pricing table
LOAD 'blobodbc';
.read sql/create_llm_pricing.sql
.read sql/merge_pricing_from_ttst.sql

-- Pull current prices from PostgreSQL
SELECT * FROM merge_pricing_from_ttst('my_pg_dsn');
```

The TTST is append-only, so the MERGE is safe to run at any time. It
fetches all rows where `sys_to IS NULL` (current prices) and upserts
them into the local `llm_pricing` table. The local table is a simple
(non-temporal) snapshot used for cost accounting joins.

This separation means:
- **Python scraper** writes to the high-ceremony DB (source of truth)
- **Operational DuckDB** reads from it via ODBC (local cache)
- No coordination needed — pull whenever you want fresh prices
