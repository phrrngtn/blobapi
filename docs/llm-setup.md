# LLM Demo Setup

This document describes the services and configuration needed to run the
`physical_properties` demo (and other LLM-backed reified functions) from
DuckDB via the blobhttp extension.

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
