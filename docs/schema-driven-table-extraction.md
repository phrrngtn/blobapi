# Schema-Driven Table Extraction

Finding tables in documents when you know what you're looking for but not
exactly where it is or what format it's in.

## Problem

Pricing data, financial tables, technical specifications, and other
structured data are published in HTML pages, PDFs, Excel workbooks, and
Word documents. The data we want is tabular, but:

- It may not be in `<table>` elements (CSS grid, `<div>` layouts, Shadow DOM)
- It may be one of many tables on a page (nav tables, layout tables, etc.)
- The column names may differ from our schema ("Price per MTok" vs "input_per_mtok")
- The document format varies (HTML, PDF, XLSX, DOCX)
- The precision of what we know about the target varies from "it's a table
  with numbers" to "it matches this exact JSON Schema"

The current pricing scraper pipeline (Jina + `X-Target-Selector: table` +
LLM extraction) works when the data is in HTML tables. It fails for
Shadow DOM, JS-rendered layouts, and non-HTML documents.

## Idea: Variable Resolution Schema Matching

Instead of hardcoding extraction logic per document type, define **what
we're looking for** at whatever level of precision we have, and let the
system find candidate regions that match.

### Resolution levels

From loosest to most precise:

1. **Column hints**: a bag of plausible column names and rough data types
   ```yaml
   columns:
     - name_hints: [model, model_id, model_name]
       type: string
     - name_hints: [input, input_price, input_cost, prompt]
       type: number
     - name_hints: [output, output_price, output_cost, completion]
       type: number
   ```

2. **Typed schema**: column names + types + constraints
   ```yaml
   columns:
     - name: model_id
       type: string
       pattern: "^[a-z]"
     - name: input_per_mtok
       type: number
       minimum: 0
       maximum: 1000
   ```

3. **JSON Schema**: full Draft 2020-12 schema with validation
   (this is what `llm_model_cost.yaml` already uses for the output)

4. **Domain-annotated schema**: columns carry semantic domain labels
   from the blobfilters domain registry (e.g., `domain: model_identifier`,
   `domain: price_per_mtok`). The domain label connects to world knowledge
   about what values look like, what units are expected, and what
   functional dependencies exist.

### How it connects to the blob* extensions

| Extension | Role in table extraction |
|---|---|
| **blobrange** | Find candidate rectangular regions in a document. Score each region against the schema at whatever resolution is available. Return ranked candidates with confidence scores. |
| **blobboxes** | Spatial layout analysis for PDFs — bounding boxes, reading order, cell boundaries. Feeds candidate regions to blobrange. |
| **blobfilters** | Domain registry — semantic type labels (`model_identifier`, `price_per_mtok`, `currency_code`). Enables matching by meaning, not just by column name string similarity. |
| **blobtemplates** | JMESPath reshaping of extracted data into the target schema. The same `response_jmespath` pattern used in LLM adapters. |
| **blobhttp** | Fetch documents (Jina Reader for HTML, direct download for PDF/XLSX). |
| **blobapi** | Catalog of known schemas (what we're looking for) and adapters (how to reshape what we find). |

### Extraction pipeline

```
Document (HTML / PDF / XLSX / DOCX)
  │
  ├─ blobboxes: spatial analysis → candidate regions
  │  (bounding boxes, cell boundaries, reading order)
  │
  ├─ blobrange: schema matching → ranked candidates
  │  (score each region against the target schema)
  │  (resolution 1: column name fuzzy match + type check)
  │  (resolution 2: value range/pattern validation)
  │  (resolution 3: full JSON Schema validation)
  │  (resolution 4: domain-aware semantic matching)
  │
  ├─ blobtemplates: reshape → target schema
  │  (JMESPath or column mapping)
  │
  └─ LLM (optional): disambiguate → final extraction
     (only needed when structural matching is ambiguous)
     (the LLM sees only the candidate region, not the whole doc)
```

The key insight is that the LLM is the **last resort**, not the first
step. Structural matching (blobrange + blobboxes) handles the common
cases cheaply. The LLM handles the ambiguous cases — but it sees only
a small candidate region, not the entire 218k-token document.

### Current state (pricing scraper)

The pricing scraper is an early instance of this pattern:

| Step | Current implementation | Future (schema-driven) |
|---|---|---|
| Document fetch | Jina Reader (`bh_http_get` + `r.jina.ai/`) | Same, plus PDF/XLSX readers |
| Region selection | `X-Target-Selector: table` (CSS) | blobrange schema matching |
| Extraction | LLM parses entire page | LLM sees only matched region |
| Validation | jsoncons JSON Schema | Same |
| Reshaping | JMESPath | Same |

### Why this matters for the broken providers

The 4 providers where Jina + table selector fails (Mistral, DeepSeek,
xAI, Cohere) all have pricing data that isn't in `<table>` elements.
With schema-driven extraction:

- **Resolution 1** (column hints) would find the pricing data in `<div>`
  grids, definition lists, or inline text by recognizing patterns like
  "model name followed by dollar amounts"
- **blobrange** would score these non-table regions against the pricing
  schema and surface them as candidates
- The **LLM** would only need to parse a small, already-identified region

This is a general solution — the same machinery works for extracting
financial tables from SEC filings (PDF), product specs from datasheets
(XLSX), or API pricing from any web page regardless of how it's rendered.

## Working prototype: bbox_extract.py

A working implementation of step 1 (spatial data extraction from web pages)
exists in `blobapi/bbox_extract.py`. It uses:

- **Playwright** (headless Chromium) to render JS-heavy pages
- **CDP isolated worlds** (`Page.createIsolatedWorld`) to inject extraction
  JS that the page cannot detect or interfere with — same mechanism as
  Chrome extension content scripts and Tampermonkey userscripts
- **TreeWalker** to visit every visible text node in the DOM
- **Range.getClientRects()** to get the bounding rectangle without
  triggering layout reflow

Each text node yields: bounding rect (x, y, w, h), text content, font
metrics (family, size, weight, color), and DOM context (tag, class).

Example output for the Mistral pricing page after clicking "API pricing":

```
y≈460:  "Mistral Large 3" (h3, 24px)  |  "Mistral Small 4" (h3, 24px)
y≈580:  "Input (/M tokens)" (p, 16px) |  "$0.15" (p, 16px)
y≈640:  "Output (/M tokens)"          |  "$0.6"
y≈680:  "Output (/M tokens)" | "$1.5"
```

The spatial clustering reveals the CSS grid layout — model names in h3
at 24px, prices in p at 16px, aligned in columns by x-coordinate. A
table detection algorithm would find these grid-aligned clusters and
match them against the pricing schema.

### Connection to MutationObserver

For dynamic pages where content changes after initial render (tab clicks,
infinite scroll, lazy loading), a MutationObserver watches for new text
nodes and extracts their bounding boxes incrementally. Tested on Mistral's
pricing page: 352 initial bboxes + 447 mutations after clicking the "API
pricing" tab, with sub-millisecond timestamps.

The observer + bbox extraction JS is ~60 lines and is universal — the
same script works in both controller environments:

| | Playwright (Python) | Qt WebEngine (C++/Python) |
|---|---|---|
| Inject JS | `page.evaluate(js)` | `page.runJavaScript(js, worldId)` |
| Isolated world | CDP `Page.createIsolatedWorld` | `worldId` param (1-256) |
| Persistent injection | `page.add_init_script()` | `QWebEngineScript` with `DocumentReady` |
| Pre-paint injection | Unreliable (Next.js replaces context) | `DocumentCreation` injection point (reliable) |
| JS → Python callback | `page.expose_function()` or `Runtime.addBinding` | `QWebChannel` + `setWebChannel(channel, worldId)` |
| Python → JS | `page.evaluate()` | `runJavaScript()` |

Qt WebEngine is better suited for pre-paint observation because
`QWebEngineScript` with `DocumentCreation` injection and a specific
`worldId` survives framework hydration — the isolated world persists
even when React/Next.js replaces the main world's execution context.

The `QWebChannel` bridge is also richer than Playwright's
`expose_function` — it's full bidirectional RPC where Python objects
are directly callable from JS and vice versa. The MutationObserver
calls `bboxReceiver.onMutation(data)` and it arrives as a Qt signal.

### Headless rendering caveat

Qt WebEngine's `offscreen` QPA platform does not initialize the GPU
compositor properly — pages render as empty (just footer/nav). This
means PySide6 cannot be used for headless scraping (CI, cron jobs).

For headless use, Playwright's headless Chromium is the right choice —
it has a purpose-built compositor that works without a display.

PySide6 is the right choice for **interactive** use cases: CTPs in
Excel, development tools, visual debugging. A working demo is in
`blobapi/pyside6_bbox_demo.py`.

The **extraction JS is identical** in both controllers — it's the
TreeWalker + Range.getClientRects() script. Only the injection
mechanism and callback bridge differ.

### Browser-side domain matching via WASM

The bbox extraction JS can be combined with roaring bitmap domain
classifiers running in WASM — the
[roaring-wasm](https://github.com/SalvatorePreviti/roaring-wasm)
npm package uses the same portable serialization format as CRoaring
(which blobfilters wraps). Bitmaps serialized in DuckDB via
`bf_roaring_serialize(bitmap, 'portable')` deserialize directly in
the browser via `RoaringBitmap32.deserialize("portable", bytes)`.

This enables real-time domain classification of text bboxes in the
browser: for each bbox, tokenize the text, build a roaring bitmap of
token hashes, and probe against domain filters sent from the database.
Performance is ~10,000 probes in under 10ms (WASM is near-native).

The "browsing while hunting" use case: a PySide6 CTP connected to
Excel sends the domain filters derived from the active workbook's
named ranges into the browser's isolated world. As the user browses,
matching tables are highlighted automatically.

Full design: see
[blobfilters/docs/browser-domain-matching.md](https://github.com/phrrngtn/blobfilters/blob/main/docs/browser-domain-matching.md).

### Relation to domain inference

The `domain_inference` LLM adapter (already in blobapi) classifies columns
by semantic domain. This feeds directly into resolution level 4: if we
know that a column's domain is `price_per_mtok`, we can match it against
candidate regions even when the column header says "Cost" or "Rate" or
uses a completely different language.

The domain registry in blobfilters (via Rule4 extended properties) stores
these labels persistently. The `domain_inference` adapter discovers them;
blobrange uses them for matching; the circle closes.
