# blobapi

> **Disclaimer**: This repository is almost entirely AI-generated, under close human supervision. Everything here is experimental. If any of the experiments prove particularly successful, we may re-implement them in a more designed and "joined up" manner.

blobapi is a member of the [BLOB extension family](https://github.com/phrrngtn/rule4/blob/main/BLOB_EXTENSIONS.md) — four C/C++ libraries (blobtemplates, blobboxes, blobfilters, blobodbc) that share a common pattern of core C API + SQLite/DuckDB/Python wrappers.

## What this is

blobapi makes web APIs look like tables.

Many data sources that *should* be local tables — employee directories, weather observations, address lookups, currency rates — are only accessible through web service interfaces. Each service has its own URL scheme, authentication method, parameter format, and response shape. blobapi unifies them so that a SQL query can join a local database table against a web API result as naturally as joining two local tables.

The key idea is that **metadata, not code**, should describe how to talk to each service. Two JMESPath expressions per API operation — one to construct the request, one to reshape the response — are stored as data in the database alongside the OpenAPI spec they describe. The SQL that executes them is generic and source-agnostic.

## Why it works this way

### Scalar function composition

The entire pipeline — vault secret retrieval, URL construction, HTTP call, response normalization — composes as scalar functions in a CTE chain:

```sql
WITH CREDS AS (
    SELECT json_extract(
        (http_get('http://vault:8200/v1/secret/data/blobapi/geocodio',
            headers := MAP {'X-Vault-Token': token}
        )).response_body, '$.data.data') AS secret
),
LOCATION AS (
    SELECT (http_get(
        json_extract_string(secret, '$.base_url') || '/geocode',
        params := json_object('q', '02458', 'api_key',
                              json_extract_string(secret, '$.api_key'))
    )).response_body AS body FROM CREDS
)
SELECT
    json_extract_string(body, '$.results[0].formatted_address') AS address,
    json_extract(body, '$.results[0].location.lat') AS lat
FROM LOCATION;
```

No stored procedures, no application code, no ORM. Each CTE is one step: get the credential, make the call, extract the result. Because `http_get` is a scalar function, it composes with `json_extract`, `jmespath_search`, and every other expression in the SELECT list.

### Metadata-driven adapters

Each API operation has an adapter — two JMESPath expressions stored in `api_adapter`:

- **call_jmespath**: takes a context object `{base_url, api_key, lat, lng, ...}` and produces `{url, params}` for the HTTP call
- **response_jmespath**: takes the response body and produces a common fact schema (e.g., `[{date, temperature, unit_of_measure}, ...]`)

Different services that return the same kind of data get different adapters but produce identical output schemas. A weather query against Visual Crossing and one against Open-Meteo both produce `{date, temperature, unit_of_measure}` — the SQL that consumes them doesn't know or care which backend provided the data.

This is the same principle as extended properties in SQL Server: metadata *about* a schema object, stored alongside it, that tells tooling how to interpret or interact with it.

### Why not code-generate the queries?

Because the queries are already generic. The adapter JMESPath expressions are the only per-service configuration, and they're data. Adding a new weather provider means adding a YAML stanza in `adapters/`, not writing a new function or class. The SQL that drives the pipeline reads the adapter from the database and applies it — one query handles any number of backends.

## Architecture

### Catalog pipeline

The OpenAPI specs from [APIs.guru](https://github.com/APIs-guru/openapi-directory) are loaded into a relational catalog via a log-oriented pipeline:

1. **Dulwich** walks the git tree (or commit log for history)
2. **DuckDB + blobtemplates** converts YAML to JSON in parallel via `yaml_to_json()` (rapidyaml at C speed — 4,138 specs in ~8 seconds)
3. **Python** shreds the JSON into normalized TTST tables (specs, paths, operations, parameters, responses, schemas)
4. **SQLAlchemy** bulk-writes to the target database (SQLite, PostgreSQL, SQL Server, DuckDB)

Git is the store of record. `sync` rebuilds the catalog from HEAD; `sync --full` walks the entire commit history for temporal reconstruction.

### TTST (Transaction-Time State Tables)

Every table carries `sys_from` / `sys_to` columns. Each row records when it was current in the database. For specs loaded from git, `sys_from` is the commit timestamp. This enables point-in-time queries: "what did the GitHub API look like on 2024-01-15?" or "when did this endpoint first appear?"

The adapter table is also temporal — when an API changes its response shape and the JMESPath expression is updated, the old version is closed and the new one inserted. A time-aware join gives you the correct adapter for any historical spec version.

### Secret management

API keys are stored in [OpenBao](https://openbao.org) (open-source Vault fork) and retrieved via `http_get` in SQL. The scoped `http_config` mechanism in blobhttp can inject bearer tokens automatically for URL prefixes, so authenticated API calls look identical to unauthenticated ones.

### Custom JMESPath functions

Four functions added to the jsoncons JMESPath engine in blobtemplates, available in DuckDB, SQLite, and Python:

| Function | Purpose |
|---|---|
| `zip_arrays(obj)` | `{a:[1,2], b:[3,4]}` → `[{a:1,b:3}, {a:2,b:4}]` |
| `unzip_arrays(arr)` | Inverse of zip_arrays |
| `to_entries(obj)` | `{k:v, ...}` → `[{key:k, value:v}, ...]` |
| `from_entries(arr)` | Inverse of to_entries |

`zip_arrays` is the critical one: many APIs return columnar/parallel-array responses (Open-Meteo, charting endpoints) to save bandwidth. This function transposes them to the row-oriented format that SQL expects, enabling a uniform response JMESPath across both row-oriented and columnar backends.

## Usage

```bash
uv run python main.py init                  # Create tables
uv run python main.py sync                  # Load specs from git HEAD
uv run python main.py catalog               # Fast metadata-only (no YAML parse)
uv run python main.py adapters              # Load adapter configs from YAML
uv run python main.py connections           # List configured databases
```

## Project layout

```
blobapi/
├── blobapi/
│   ├── models.py              # SQLAlchemy models (TTST, multi-dialect)
│   ├── git_scraper.py         # Dulwich + DuckDB sync pipeline
│   ├── loader.py              # Spec shredding into relational tables
│   ├── config.py              # Connection management from connections.toml
│   ├── schema_fingerprint.py  # Table-like response classification
│   └── scraper.py             # HTTP-based scraper (APIs.guru fallback)
├── adapters/
│   └── weather.yaml           # JMESPath adapters for weather APIs
├── specs/
│   └── open-meteo-archive.yaml # Manually curated OpenAPI specs
├── openapi-directory/          # Git submodule (APIs.guru)
├── main.py                     # CLI entry point
├── connections.toml.example    # Database connection template
└── pyproject.toml
```
