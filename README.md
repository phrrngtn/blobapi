# blobapi

> **Disclaimer**: This repository is almost entirely AI-generated, under close human supervision. Everything here is experimental. If any of the experiments prove particularly successful, we may re-implement them in a more designed and "joined up" manner.

blobapi is a member of the [BLOB extension family](https://github.com/phrrngtn/rule4/blob/main/BLOB_EXTENSIONS.md) — four C/C++ libraries (blobtemplates, blobboxes, blobfilters, blobodbc) that share a common pattern of core C API + SQLite/DuckDB/Python wrappers.

## What this is

blobapi makes web APIs look like tables.

Many data sources that *should* be local tables — employee directories, weather observations, address lookups, currency rates, LLM model pricing — are only accessible through web service interfaces. Each service has its own URL scheme, authentication method, parameter format, and response shape. blobapi unifies them so that a SQL query can join a local database table against a web API result as naturally as joining two local tables.

The key idea is that **metadata, not code**, should describe how to talk to each service. Two JMESPath expressions per API operation — one to construct the request, one to reshape the response — are stored as data in the database alongside the OpenAPI spec they describe. The SQL that executes them is generic and source-agnostic.

## Two kinds of adapters

### HTTP adapters (JMESPath)

For REST APIs with stable schemas. Two expressions per operation:
- **call_jmespath**: context object → `{url, params}` for the HTTP call
- **response_jmespath**: response body → common fact schema

Different services that return the same kind of data get different adapters but produce identical output schemas. A weather query against Visual Crossing and one against Open-Meteo both produce `{date, temperature, unit_of_measure}`.

### LLM adapters (inja prompt templates)

For unstructured or semi-structured data extraction. Each adapter is a reified function:
- **prompt_template**: Inja/Jinja2 template rendered with caller params
- **output_schema**: JSON Schema for response validation (jsoncons Draft 2020-12)
- **response_jmespath**: reshape validated JSON into the desired structure

The `physical_properties` adapter asks Claude for material properties; the `llm_model_cost` adapter extracts pricing from scraped web pages. The SQL that drives them is the same `llm_adapt()` macro.

## LLM pricing pipeline

A complete system for tracking LLM model costs across providers:

```
                    ┌─────────────────┐
                    │  Provider URLs  │  adapters/providers.yaml
                    │  (pricing pages,│  → llm_provider table (TTST)
                    │   Jina selectors)│
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
     ┌────────────┐  ┌────────────┐  ┌────────────┐
     │  Bifrost   │  │ Jina Reader│  │ Jina Reader│
     │ /v1/models │  │  + LLM     │  │  + LLM     │
     │ (bootstrap)│  │ (Python)   │  │ (pure SQL) │
     └─────┬──────┘  └─────┬──────┘  └─────┬──────┘
           │               │               │
           └───────────────┼───────────────┘
                           ▼
              ┌──────────────────────┐
              │ llm_model_price_     │  TTST on PG/SQL Server
              │ history              │  sys_from/sys_to tracks
              │ (high-ceremony DB)   │  when prices changed
              └──────────┬───────────┘
                         │  ODBC query
                         ▼
              ┌──────────────────────┐
              │ Operational DuckDB   │  Local copy, MERGE
              │ llm_pricing table    │  from high-ceremony DB
              └──────────────────────┘
```

**Three data sources** (use whichever is available):

1. **Bifrost `/v1/models`** — static pricing compiled into the Docker image. Fast, comprehensive (88 providers, 2,566 models), but only updated when Bifrost is rebuilt.
2. **Jina Reader + LLM (Python)** — `scrape-pricing` command fetches provider pricing pages via [Jina Reader](https://jina.ai/reader/), extracts tables with `X-Target-Selector: table`, and passes the clean markdown to an LLM for structured extraction. Updates the TTST directly via SQLAlchemy.
3. **Jina Reader + LLM (pure SQL)** — same pipeline but entirely in DuckDB: `bh_http_get` → Jina → `llm_adapt` → structured table.

**Cost accounting**: every `llm_adapt()` call returns `_meta` with `prompt_tokens`, `completion_tokens`, `model`, and `elapsed_seconds`. Join against `llm_pricing` to compute per-query cost in USD.

## Architecture

### Catalog pipeline

The OpenAPI specs from [APIs.guru](https://github.com/APIs-guru/openapi-directory) are loaded into a relational catalog via a log-oriented pipeline:

1. **Dulwich** walks the git tree (or commit log for history)
2. **DuckDB + blobtemplates** converts YAML to JSON in parallel via `yaml_to_json()` (rapidyaml at C speed — 4,138 specs in ~8 seconds)
3. **Python** shreds the JSON into normalized TTST tables (specs, paths, operations, parameters, responses, schemas)
4. **SQLAlchemy** bulk-writes to the target database (SQLite, PostgreSQL, SQL Server, DuckDB)

Git is the store of record. `sync` rebuilds the catalog from HEAD; `sync --full` walks the entire commit history for temporal reconstruction.

### TTST (Transaction-Time State Tables)

Every table carries `sys_from` / `sys_to` columns. Each row records when it was current in the database. For specs loaded from git, `sys_from` is the commit timestamp. For pricing data, `sys_from` is the observation time (upper bound on when the price actually changed).

The temporal upsert pattern: compare incoming data against the current row (`sys_to IS NULL`). If unchanged, skip. If different, close the old row (`sys_to = now`) and insert a new one. This makes all operations idempotent and append-only.

### Services

| Service | Purpose | Port | Required for |
|---|---|---|---|
| **Bifrost** | LLM gateway (OpenAI-compatible → Anthropic/etc.) | 8080 | LLM calls, pricing bootstrap |
| **OpenBao** | Secret storage (Vault-compatible) | 8200 | Direct API calls (weather, geocoding) |
| **Jina Reader** | HTML → clean markdown (JS rendering, table extraction) | — | Pricing scrapes (external service) |

See [docs/llm-setup.md](docs/llm-setup.md) for detailed setup instructions.

### Secret management

API keys are stored in [OpenBao](https://openbao.org) (open-source Vault fork) and retrieved via `bh_http_get` in SQL. For Bifrost-routed LLM calls, OpenBao is only needed during Bifrost's initial API key setup — not at query time.

### Custom JMESPath functions

Four functions added to the jsoncons JMESPath engine in blobtemplates, available in DuckDB, SQLite, and Python:

| Function | Purpose |
|---|---|
| `zip_arrays(obj)` | `{a:[1,2], b:[3,4]}` → `[{a:1,b:3}, {a:2,b:4}]` |
| `unzip_arrays(arr)` | Inverse of zip_arrays |
| `to_entries(obj)` | `{k:v, ...}` → `[{key:k, value:v}, ...]` |
| `from_entries(arr)` | Inverse of to_entries |

## Usage

```bash
# Database setup
uv run python main.py init                        # Create tables
uv run python main.py sync                        # Load specs from git HEAD
uv run python main.py sync --full                 # Full git history TTST
uv run python main.py catalog                     # Fast metadata-only
uv run python main.py adapters                    # Load HTTP adapter configs
uv run python main.py connections                 # List configured databases

# LLM pricing
uv run python main.py bootstrap-pricing           # Seed from Bifrost + providers.yaml
uv run python main.py scrape-pricing              # Scrape all providers
uv run python main.py scrape-pricing anthropic    # Scrape one provider

# DuckDB demos (from blobapi directory)
duckdb -unsigned -init sql/llm_demo.sql           # Material properties
duckdb -unsigned -init sql/scrape_pricing_init.sql # Price scraping (pure SQL)
```

## SQLAlchemy models

All models target SQLite, DuckDB, PostgreSQL, and SQL Server.

### High-ceremony database (PG/SQL Server)

| Model | Table | Purpose |
|---|---|---|
| `ApiRegistry` | `api_registry` | Source registries (APIs.guru, etc.) |
| `ApiSpec` | `api_spec` | One row per API identity |
| `ApiPath` | `api_path` | URL paths within a spec |
| `ApiOperation` | `api_operation` | HTTP methods on paths |
| `ApiParameter` | `api_parameter` | Query/path/header params |
| `ApiResponse` | `api_response` | Status codes and schemas |
| `ApiSchema` | `api_schema` | Reusable component schemas |
| `GitSpecStaging` | `git_spec_staging` | Fast metadata from git tree walk |
| `ApiAdapter` | `api_adapter` | JMESPath HTTP adapters (TTST) |
| `LlmProvider` | `llm_provider` | Provider reference data (TTST) |
| `LlmModelPriceHistory` | `llm_model_price_history` | Pricing snapshots (TTST) |

### Session-scoped (DuckDB/SQLite)

| Model | Table | Purpose |
|---|---|---|
| `LlmAdapter` | `llm_adapter` | Inja prompt templates for `llm_adapt()` |
| `LlmPricing` | `llm_pricing` | Per-model token costs for cost accounting |

## Project layout

```
blobapi/
├── blobapi/
│   ├── models.py              # SQLAlchemy models (TTST, multi-dialect)
│   ├── bootstrap_pricing.py   # Bifrost bootstrap + provider YAML loading
│   ├── scrape_pricing.py      # Jina Reader + LLM pricing scraper
│   ├── git_scraper.py         # Dulwich + DuckDB sync pipeline
│   ├── loader.py              # Spec shredding into relational tables
│   ├── config.py              # Connection management from connections.toml
│   ├── schema_fingerprint.py  # Table-like response classification
│   └── scraper.py             # HTTP-based scraper (APIs.guru fallback)
├── adapters/
│   ├── providers.yaml         # Provider slugs, URLs, Jina selectors
│   ├── physical_properties.yaml # LLM adapter: material property lookup
│   ├── domain_inference.yaml  # LLM adapter: column domain classification
│   ├── llm_model_cost.yaml    # LLM adapter: pricing page extraction
│   ├── llm_pricing.yaml       # Static pricing reference (Anthropic)
│   └── weather.yaml           # HTTP adapter: weather APIs
├── sql/
│   ├── llm_demo.sql           # Physical properties demo (init file)
│   ├── scrape_pricing_init.sql # Pure-SQL pricing scrape demo
│   ├── scrape_pricing.sql     # scrape_pricing() DuckDB macro
│   ├── llm_model_cost.sql     # llm_model_cost() macro (reads Bifrost)
│   ├── create_llm_adapter.sql # DDL for llm_adapter table
│   ├── create_llm_pricing.sql # DDL for llm_pricing table
│   ├── load_llm_adapters.sql  # INSERT adapters from YAML
│   ├── load_llm_pricing.sql   # INSERT pricing from static YAML
│   └── load_llm_pricing_from_bifrost.sql # INSERT pricing from Bifrost
├── docs/
│   ├── llm-setup.md           # Service setup (Bifrost, OpenBao, DuckDB)
│   └── jmespath-vs-sql.md     # JMESPath/SQL expressiveness analysis
├── specs/                     # Manually curated OpenAPI specs
├── openapi-directory/         # Git submodule (APIs.guru)
├── main.py                    # CLI entry point (docopt)
├── connections.toml.example   # Database connection template
└── pyproject.toml
```
