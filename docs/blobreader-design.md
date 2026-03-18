# Browser Table Extraction

> This design has moved to blobboxes where it belongs — the browser is
> just another document source alongside PDF, and blobboxes owns the
> bounding box abstraction.
>
> See: [blobboxes/docs/browser-table-extraction.md](https://github.com/phrrngtn/blobboxes/blob/main/docs/browser-table-extraction.md)
>
> What follows is the original design for reference.

## What it is

An HTTP service (like Jina Reader, but on-prem) that:

1. Fetches a URL (via blobhttp: rate-limited, proxied, authenticated)
2. Renders it (headless Chromium via Playwright)
3. Extracts text bounding boxes (TreeWalker + getClientRects in CDP
   isolated world)
4. Optionally classifies bboxes against domain filters (roaring-wasm)
5. Returns structured table candidates as JSON

```
GET /read?url=https://mistral.ai/pricing
    &click=button:has-text('API pricing')
    &domains=price_per_mtok,model_identifier

→ {
    "url": "https://mistral.ai/pricing",
    "bboxes": 463,
    "tables": [
      {
        "confidence": 0.92,
        "columns": ["model_identifier", "price_per_mtok", "price_per_mtok"],
        "headers": ["Model", "Input (/M tokens)", "Output (/M tokens)"],
        "rows": [
          {"model_identifier": "Mistral Large 3",
           "Input (/M tokens)": 0.5,
           "Output (/M tokens)": 1.5},
          ...
        ],
        "region": {"x": 153, "y": 458, "w": 1200, "h": 400}
      }
    ]
  }
```

## Why not just use Jina Reader?

| | Jina Reader | blobreader |
|---|---|---|
| Hosting | External SaaS | On-prem (Docker) |
| Rate limits | 20 RPM free | Your hardware |
| Auth/proxy | None | blobhttp (Vault, mTLS, SPNEGO) |
| Output | Markdown text | Structured table candidates with bboxes |
| Table detection | `X-Target-Selector: table` (CSS only) | Domain-aware schema matching (roaring bitmaps) |
| JS interaction | None (renders once) | Click tabs, scroll, wait for content |
| Domain filtering | None | Roaring bitmap probing against known schemas |
| PDF/Excel | No | Future: blobboxes for PDF, openpyxl for Excel |

Jina is great for the simple case (static HTML tables → markdown).
blobreader handles the hard cases: JS-rendered pages, tabbed content,
domain-aware extraction, authenticated sources.

## Architecture

```
Client (DuckDB / Python / curl)
  │
  │  GET /read?url=...&domains=...
  │
  ▼
blobreader (FastAPI / Docker)
  │
  ├── blobhttp (fetch)
  │     Rate limiting, Vault auth, proxy, mTLS
  │
  ├── Playwright (render)
  │     Headless Chromium, tab clicks, wait conditions
  │
  ├── CDP isolated world (extract)
  │     TreeWalker + getClientRects → bboxes
  │     MutationObserver for dynamic content
  │
  ├── roaring-wasm (classify)
  │     Domain filters deserialized from request or cache
  │     Token hashing + bitmap probing per bbox
  │
  └── Table recovery (assemble)
        Spatial clustering of classified bboxes
        Grid detection (aligned x/y coordinates)
        Column/row structure inference
        → JSON table candidates
```

## API

### `GET /read`

Fetch and extract tables from a URL.

| Param | Type | Description |
|---|---|---|
| `url` | string | URL to fetch and render |
| `click` | string | CSS selector to click before extraction (optional) |
| `wait` | string | CSS selector to wait for (optional) |
| `wait_ms` | int | Extra wait time in ms (default: 3000) |
| `domains` | string | Comma-separated domain names for filtering (optional) |
| `format` | string | `bboxes` (raw), `tables` (structured), `markdown` (Jina-like) |
| `viewport` | string | `WxH` viewport size (default: `1440x900`) |

### `GET /domains`

List available domain filters (loaded from database or YAML).

### `POST /classify`

Classify pre-extracted bboxes against domain filters (for when the
client does its own rendering).

### `GET /health`

Health check.

## How it connects to SQL

From DuckDB, blobreader looks like another `bh_http_get` call:

```sql
-- Fetch structured tables from a pricing page
WITH PAGE AS (
    SELECT bh_http_get(
        'http://localhost:9090/read',
        params := json_object(
            'url', 'https://mistral.ai/pricing',
            'click', 'button:has-text(''API pricing'')',
            'format', 'tables',
            'domains', 'model_identifier,price_per_mtok')
    ).response_body AS resp
),
TABLES AS (
    SELECT unnest(from_json(
        json_extract(CAST(resp AS JSON), '$.tables'),
        '["json"]'
    )) AS t
    FROM PAGE
)
SELECT
    t->>'$.headers' AS headers,
    json_array_length(t->'$.rows') AS row_count,
    t->>'$.confidence' AS confidence
FROM TABLES;
```

The result is a table candidate that can be unnested and joined —
the data has made the round trip from table → rendered page → table.

## Docker deployment

```dockerfile
FROM python:3.14-slim
RUN pip install fastapi uvicorn playwright roaring-wasm
RUN playwright install chromium
COPY blobreader/ /app/
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9090"]
```

```bash
docker run -d --name blobreader -p 9090:9090 blobreader
```

Sits alongside Bifrost (8080) and OpenBao (8200) in the local
service stack.

## What makes it a blob* extension

blobreader follows the blob family pattern:

- **Core logic in C/WASM**: roaring bitmap operations (via roaring-wasm)
  and spatial analysis (future: via blobboxes compiled to WASM)
- **Multiple wrappers**: HTTP API (primary), Python library, potentially
  DuckDB table function
- **Composable with siblings**: uses blobhttp for fetching, blobfilters
  for classification, blobboxes for spatial analysis, blobtemplates for
  JMESPath reshaping of results
- **Data-driven configuration**: domain filters, click selectors, and
  wait conditions come from the database (providers.yaml / llm_provider
  table), not hardcoded

## The journey of a price

To illustrate the full round trip:

1. **Origin**: Someone at Anthropic types `$5 / MTok` into a CMS
2. **Rendering**: Next.js RSC serializes it, React hydrates it,
   CSS positions it at (461, 644) in a card grid
3. **blobreader**: Chromium renders the page, TreeWalker finds the
   text node, getClientRects returns the bbox, FNV-1a hashes the
   tokens, roaring bitmap matches `price_per_mtok` domain
4. **Table recovery**: Spatial clustering groups it with "Mistral
   Large 3" (above, same x-column) and "Output (/M tokens) $1.5"
   (below, same card)
5. **SQL**: `SELECT * FROM blobreader_tables WHERE domain = 'price_per_mtok'`
6. **JOIN**: The price participates in a cost accounting query
   alongside `_meta.prompt_tokens` from an LLM call

The data is home.

## Cross-references

- **blobapi/bbox_extract.py**: working Playwright prototype of the
  TreeWalker + getClientRects extraction
- **blobapi/browser_extract.py**: headless browser + LLM extraction
  (current pricing scraper implementation)
- **blobapi/docs/schema-driven-table-extraction.md**: the broader
  vision for variable-resolution schema matching
- **blobfilters/docs/browser-domain-matching.md**: roaring-wasm
  domain classification design
- **blobhttp/docs/dev-setup.md**: proxy, auth, rate limiting setup
